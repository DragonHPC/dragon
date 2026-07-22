"""03 — Human-in-the-Loop (HITL) Approval Gates.

**Prerequisites:** Read ``02_multi_agent_dag.py`` first.

**What's new:**

* **Approval filters** — ``approval_filter`` on ``AgentConfig`` selectively
  gates tool calls for human review before execution
* **Three-way operator decisions** — approve, reject (with reason), or
  provide feedback (agent retries incorporating the feedback)
* **HITL TCP bridge** — orchestrator auto-starts a TCP server; a standalone
  CLI client connects from another terminal to review requests
* **HITL audit log** — all decisions auto-saved to JSONL for compliance

Architecture (same DAG as 02, with HITL gates added)::

    planner_agent ──► runner_agent ──► analyzer_agent ──┐
       [HITL gate]     [HITL gate]                      ├──► reporter_agent
         │                │                             │
         └────────────────┴─────────────────────────────┘

    Terminal 2:  python -m dragon.ai.agent.hitl --tcp HOST:PORT

HITL gates:
  * planner_agent  → ``propose_experiment`` requires approval
  * runner_agent   → ``launch_experiment`` requires approval

Usage::

    # Terminal 1:
    dragon 03_hitl_approval.py

    # Terminal 2 (HITL approval client):
    python -m dragon.ai.agent.hitl --tcp HOST:PORT
"""

import asyncio
from typing import Any

import dragon
import multiprocessing as mp

from dragon.ai.agent.core import create_sub_agent
from dragon.ai.agent.config import (
    AgentConfig,
    OrchestratorConfig,
    Pipeline,
    PipelineNode,
)
from dragon.ai.agent.tools import ToolRegistry
from dragon.ai.agent.orchestrator import DAGOrchestrator
from dragon.infrastructure.policy import Policy
from dragon.native.event import Event
from dragon.native.machine import Node, System
from dragon.native.process import Process
from dragon.native.queue import Queue
from dragon.workflows.batch import Batch

from dragon.ai.inference import (
    BatchingConfig,
    HardwareConfig,
    InferenceConfig,
    ModelConfig,
    Inference,
)

from tools import (
    propose_experiment,
    launch_experiment,
    check_progress,
    collect_results,
    analyze_convergence,
    format_results_table,
)
from tools.runner import cleanup_experiment_state


# ===========================================================================
# User-configurable constants
# ===========================================================================

MODEL_NAME = "/path/to/your/model"
HF_TOKEN = ""


# ===========================================================================
# Inference Pipeline Configuration
# ===========================================================================

INFERENCE_CONFIG = InferenceConfig(
    model=ModelConfig(
        model_name=MODEL_NAME,
        hf_token=HF_TOKEN,
        tp_size=2,
        max_tokens=8192,
        max_model_len=32768,
    ),
    hardware=HardwareConfig(
        num_nodes=1,
        num_gpus=2,
        num_inf_workers_per_cpu=1,
    ),
    batching=BatchingConfig(
        batch_wait_seconds=0.1,
        max_batch_size=32,
    ),
)


# ===========================================================================
# Tool registries
# ===========================================================================

planner_registry = ToolRegistry()
planner_registry.register(propose_experiment)

runner_registry = ToolRegistry()
runner_registry.register(launch_experiment)
runner_registry.register(check_progress)
runner_registry.register(collect_results)

analyzer_registry = ToolRegistry()
analyzer_registry.register(analyze_convergence)

reporter_registry = ToolRegistry()
reporter_registry.register(format_results_table)


# ===========================================================================
# DAG pipeline
# ===========================================================================

pipeline = Pipeline(nodes=[
    PipelineNode(
        agent_id="planner_agent",
        task_description=(
            "You are a scientific experiment planner.  The user wants to study "
            "Monte Carlo convergence for estimating π.\n\n"
            "Propose an experiment plan by calling propose_experiment with:\n"
            "  - description, sample_sizes, convergence_target, methodology\n\n"
            "A human operator will review your plan.  If they provide "
            "feedback, revise and resubmit.  Once approved, report verbatim."
        ),
        depends_on=[],
    ),
    PipelineNode(
        agent_id="runner_agent",
        task_description=(
            "You manage parallel Monte Carlo simulations on an HPC cluster.\n\n"
            "Tools:\n"
            "  1. launch_experiment(sample_sizes, seeds) — launches ALL "
            "simulations in parallel.  Operator must approve.\n"
            "  2. check_progress() — shows done/running/pending status.\n"
            "  3. collect_results() — call ONLY when all_done=true.\n\n"
            "STRICT workflow:\n"
            "  1. Call launch_experiment.\n"
            "  2. Call check_progress until all_done=true.\n"
            "  3. Call collect_results — REQUIRED before final answer.\n"
            "  4. Report collect_results output verbatim."
        ),
        depends_on=["planner_agent"],
    ),
    PipelineNode(
        agent_id="analyzer_agent",
        task_description=(
            "Call analyze_convergence with a list of dicts (keys: "
            "'n_samples', 'absolute_error').  Report all metrics verbatim."
        ),
        depends_on=["runner_agent"],
    ),
    PipelineNode(
        agent_id="reporter_agent",
        task_description=(
            "Write a structured report.\n\n"
            "STRICT workflow:\n"
            "  1. Call format_results_table to get a Markdown table.\n"
            "  2. COPY-PASTE the actual table into your final answer.\n"
            "  3. Include sections: Plan, Results Table, Parallel Execution,\n"
            "     Convergence Analysis, Quality Assessment, Recommendations.\n\n"
            "Never invent data — use only data from upstream agents."
        ),
        depends_on=["planner_agent", "runner_agent", "analyzer_agent"],
    ),
])


# ===========================================================================
# Helpers
# ===========================================================================

def _make_agent_kwargs(agent_id, name, role, registry, inference_queue,
                       approval_filter=None,
                       max_tool_call_iterations=20):
    """Build the kwargs dict for create_sub_agent().

    NEW in this example: ``approval_filter`` — a callable that returns True
    for tool calls that need human approval.  The dispatcher pauses the
    agentic loop and sends the request to the HITL bridge.
    """
    return {
        "config": AgentConfig(
            agent_id=agent_id, name=name, role=role,
            inference_queue=inference_queue,
            approval_filter=approval_filter,
            max_tool_call_iterations=max_tool_call_iterations,
        ),
        "tool_registry": registry,
        "shutdown_event": Event(),
        "reply_queue": Queue(),
    }


def _start_agents(specs, policies):
    procs = []
    for spec, policy in zip(specs, policies):
        p = Process(target=create_sub_agent, kwargs=spec, policy=policy)
        p.start()
        procs.append(p)
    queues = {}
    for spec in specs:
        aid = spec["config"].agent_id
        queues[aid] = spec["reply_queue"].get()
        print(f"[startup] Agent '{aid}' ready.", flush=True)
    return procs, queues


# ===========================================================================
# Main
# ===========================================================================

async def main():
    input_queue = Queue()

    print("[startup] Initializing inference pipeline...", flush=True)

    inference_pipeline = None
    try:
        inference_pipeline = Inference(INFERENCE_CONFIG, input_queue)
        inference_pipeline.initialize()
    except Exception as exc:
        import traceback
        print(f"\n[FATAL] Inference pipeline failed to initialize: {exc}", flush=True)
        traceback.print_exc()
        if inference_pipeline is not None:
            inference_pipeline.destroy()
        return
    print("[startup] Inference pipeline ready.\n", flush=True)

    my_alloc = System()
    node_list = my_alloc.nodes
    compute_host = (
        Node(node_list[1]).hostname if len(node_list) > 1
        else Node(node_list[0]).hostname
    )
    compute_policy = Policy(
        placement=Policy.Placement.HOST_NAME,
        host_name=compute_host,
    )

    procs, agent_specs = [], []
    try:
        agent_specs = [
            _make_agent_kwargs(
                "planner_agent", "Experiment Planner",
                "You are an experiment planner for Monte Carlo convergence "
                "studies on an HPC cluster.  Propose plans via "
                "propose_experiment.  Revise on feedback.  Simulations will "
                "run IN PARALLEL via Dragon ProcessGroup.",
                planner_registry, input_queue,
                # ← NEW: gate propose_experiment for human review
                approval_filter=lambda n, a: n == "propose_experiment",
                max_tool_call_iterations=60,
            ),
            _make_agent_kwargs(
                "runner_agent", "Parallel Simulation Runner",
                "You manage parallel Monte Carlo simulations.\n"
                "You MUST call all three tools in order: "
                "launch_experiment → check_progress → collect_results.\n"
                "NEVER give a final answer without calling collect_results first.",
                runner_registry, input_queue,
                # ← NEW: gate launch_experiment for human review
                approval_filter=lambda n, a: n == "launch_experiment",
                max_tool_call_iterations=60,
            ),
            _make_agent_kwargs(
                "analyzer_agent", "Convergence Analyzer",
                "Analyse convergence.  Call analyze_convergence with "
                "n_samples + absolute_error dicts.  Report verbatim.",
                analyzer_registry, input_queue,
                # No approval filter — analyzer runs unattended
            ),
            _make_agent_kwargs(
                "reporter_agent", "Report Writer",
                "Write a structured report with Markdown tables.  "
                "Include parallel execution details (which node ran what).  "
                "Always include tool output verbatim — "
                "never use placeholder variables.",
                reporter_registry, input_queue,
                # No approval filter — reporter runs unattended
            ),
        ]
        policies = [None, compute_policy, None, None]

        procs, queues = _start_agents(agent_specs, policies)
        for spec in agent_specs:
            spec["config"].input_queue = queues[spec["config"].agent_id]

        orchestrator = DAGOrchestrator(
            config=OrchestratorConfig(
                agents=[s["config"] for s in agent_specs],
                poll_interval=0.5,
                poll_timeout=14400.0,
            ),
            pipeline=pipeline,
        )

        user_input = (
            "Design and run a Monte Carlo convergence experiment for π.  "
            "I want to review the plan first, then approve the parallel "
            "launch.  Monitor progress and report which nodes ran what."
        )

        batch = Batch()
        try:
            print("=" * 60, flush=True)
            print("Dragon AI — 03 HITL Approval Gates", flush=True)
            print("=" * 60, flush=True)
            print(f"Request: {user_input}\n", flush=True)

            # ← NEW: Print HITL client connection instructions
            if orchestrator.hitl_address:
                h, p = orchestrator.hitl_address
                print(
                    f"HITL approval client — start in another terminal:\n"
                    f"  python -m dragon.ai.agent.hitl --tcp {h}:{p}\n"
                    f"  Options:\n"
                    f"    --jsonl PATH        Custom JSONL path (default: auto-generated)\n"
                    f"    --report PATH       Custom .txt report path\n"
                    f"    --no-color          Disable ANSI colours\n",
                    flush=True,
                )

            result = orchestrator.run(
                user_input=user_input,
                batch=batch,
            )

            print("\n" + "=" * 60, flush=True)
            print("FINAL RESULT", flush=True)
            print("=" * 60, flush=True)
            print(result, flush=True)

        except Exception as exc:
            import traceback
            print(f"\n[error] Pipeline failed: {exc}", flush=True)
            traceback.print_exc()
        finally:
            orchestrator.destroy()
            batch.join()

    except Exception as exc:
        import traceback
        print(f"\n[error] Fatal: {exc}", flush=True)
        traceback.print_exc()
    finally:
        cleanup_experiment_state()
        for spec in agent_specs:
            try:
                spec["shutdown_event"].set()
            except Exception:
                pass
        for p in procs:
            try:
                p.join()
            except Exception:
                pass
        print("\n[teardown] All agents stopped.", flush=True)
        try:
            inference_pipeline.destroy()
        except Exception:
            pass
        print("[teardown] Inference pipeline stopped.", flush=True)


if __name__ == "__main__":
    mp.set_start_method("dragon")
    asyncio.run(main())
