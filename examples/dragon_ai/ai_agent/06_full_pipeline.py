"""06 — Full Pipeline: DAG + HITL + Memory + MCP + Tracing.

**Prerequisites:** Read ``01``–``05`` first.

**What's new:**

* **Tracing / observability** — ``tracing=True`` on ``OrchestratorConfig``
  writes a per-run JSONL trace with every LLM call, tool call,
  approval/rejection, memory operation, and DAG state transition
* **Trace viewer CLI** — inspect the trace interactively after a run
* **Everything combined** — this is the production-ready reference
  combining all features from the previous examples:

  - Multi-agent DAG + function node       (from 02)
  - HITL approval gates                   (from 03)
  - Memory strategies + summarizer LLM    (from 04)
  - MCP tool server integration           (from 05)

Architecture::

    Node 0 GPUs [0,1] → Main InferenceWorker (tp_size=2)
    Node 1 GPU  [0]   → Summarizer Worker    (tp_size=1)

    planner_agent ──► runner_agent ──► analyzer_agent ──┐
       [HITL]           [HITL]           [memory]       ├──► reporter_agent ──► save_report(fn)
     SLIDING_WINDOW   SUMMARIZE                         │       [MCP: jupyter]
         └──────────────┴───────────────────────────────┘

    tracing → ./traces/<run-id>.jsonl

Usage::

    # Without MCP:
    dragon 06_full_pipeline.py

    # With MCP:
    echo '<token>' > token.txt
    dragon 06_full_pipeline.py <mcp_url>

    # HITL approval client (in another terminal):
    python -m dragon.ai.agent.hitl --tcp HOST:PORT

    # Trace viewer (in another terminal):
    python -m dragon.ai.agent.observability --tcp HOST:PORT -i
"""

import asyncio
import json
import os
import sys
import time
from typing import Any

import dragon
import multiprocessing as mp

from dragon.ai.agent.core import create_sub_agent
from dragon.ai.agent.config import (
    AgentConfig,
    MCPServerConfig,
    MemoryConfig,
    MemoryStrategy,
    OrchestratorConfig,
    Pipeline,
    PipelineNode,
    TaskResult,
    TaskStatus,
    DISPATCH_ID_KEY,
    RESULT_KEY,
    STATUS_KEY,
)
from dragon.ai.agent.tools import ToolRegistry
from dragon.ai.agent.orchestrator import DAGOrchestrator
from dragon.data.ddict import DDict
from dragon.infrastructure.policy import Policy
from dragon.native.event import Event
from dragon.native.machine import Node, System
from dragon.native.process import Process
from dragon.native.queue import Queue
from dragon.workflows.batch import Batch

from dragon.ai.inference.config import (
    BatchingConfig,
    HardwareConfig,
    InferenceConfig,
    ModelConfig,
)
from dragon.ai.inference.inference_utils import Inference

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
SUMMARIZER_MODEL_NAME = "/path/to/your/summarizer/model"


# ===========================================================================
# Inference — Main model (reasoning + tool-calling)
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
# Inference — Summarizer (separate GPU on node 1)
# ===========================================================================

SUMMARIZER_CONFIG = InferenceConfig(
    model=ModelConfig(
        model_name=SUMMARIZER_MODEL_NAME,
        hf_token=HF_TOKEN,
        tp_size=1,
        max_tokens=2048,
        max_model_len=8192,
    ),
    hardware=HardwareConfig(
        num_nodes=1,
        num_gpus=1,
        node_offset=1,
        num_inf_workers_per_cpu=1,
    ),
    batching=BatchingConfig(
        batch_wait_seconds=0.05,
        max_batch_size=8,
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
# Function node — persists the final report to disk + DDict
# ===========================================================================

REPORT_DIR = os.environ.get("DRAGON_REPORT_DIR", os.getcwd())


def save_report(*upstreams: TaskResult) -> TaskResult:
    """Post-processing function node — saves the reporter's output to disk.

    Runs as a plain Python function inside Dragon Batch (no LLM, no agent
    queue).  Receives upstream TaskResult tokens, attaches to the shared
    DDict, reads the reporter's result, and writes it to disk.

    :param upstreams: One or more TaskResult tokens from upstream nodes.
        The first upstream carries ``task_id`` and ``serialized_ddict``.
    :return: TaskResult with status DONE for the orchestrator to collect.
    """
    upstream = upstreams[0]
    task_id = upstream.task_id
    serialized_ddict = upstream.serialized_ddict

    print(f"\n[save_report] Function node started (task_id={task_id[:8]}...)",
          flush=True)

    ddict = DDict.attach(serialized_ddict)
    try:
        # Read the reporter agent's result from DDict
        reporter_dispatch_key = DISPATCH_ID_KEY.format(
            task_id=task_id, agent_id="reporter_agent"
        )
        reporter_dispatch_id = ddict[reporter_dispatch_key]
        reporter_result_key = RESULT_KEY.format(
            task_id=task_id,
            agent_id="reporter_agent",
            dispatch_id=reporter_dispatch_id,
        )
        reporter_result = ddict[reporter_result_key]

        if isinstance(reporter_result, dict):
            report_text = reporter_result.get("response", str(reporter_result))
        else:
            report_text = str(reporter_result)

        # Write Markdown file
        md_path = os.path.join(REPORT_DIR, "monte_carlo_report.md")
        with open(md_path, "w") as f:
            f.write(report_text)
        print(f"[save_report] Written: {md_path}", flush=True)

        # Write JSON artifact with metadata
        json_path = os.path.join(REPORT_DIR, "monte_carlo_report.json")
        artifact = {
            "task_id": task_id,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "report": report_text,
            "source_agent": "reporter_agent",
            "output_files": [md_path],
        }
        with open(json_path, "w") as f:
            json.dump(artifact, f, indent=2)
        print(f"[save_report] Written: {json_path}", flush=True)

        # Write own result to DDict so the orchestrator can collect it
        own_dispatch_id = f"fn-save-report-{task_id[:8]}"
        dispatch_id_key = DISPATCH_ID_KEY.format(
            task_id=task_id, agent_id="save_report"
        )
        result_key = RESULT_KEY.format(
            task_id=task_id, agent_id="save_report", dispatch_id=own_dispatch_id
        )
        status_key = STATUS_KEY.format(
            task_id=task_id, agent_id="save_report", dispatch_id=own_dispatch_id
        )
        result_payload = {
            "response": (
                f"Report saved to:\n"
                f"  - {md_path}\n"
                f"  - {json_path}"
            )
        }
        ddict[dispatch_id_key] = own_dispatch_id
        ddict[result_key] = result_payload
        ddict[status_key] = TaskStatus.DONE
    finally:
        ddict.detach()

    print("[save_report] Function node complete.\n", flush=True)

    return TaskResult(
        task_id=task_id,
        agent_id="save_report",
        status=TaskStatus.DONE,
        serialized_ddict=serialized_ddict,
    )


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
            "Write a structured report with the experiment results.\n\n"
            "STRICT workflow — follow every step IN ORDER:\n\n"
            "  STEP 1.  Call format_results_table to get a Markdown table.\n\n"
            "  STEP 2.  Check your available tools.  If you have tools that\n"
            "           can manage Jupyter kernels and notebook files, do\n"
            "           BOTH sub-steps:\n"
            "             a. Create a Jupyter kernel.\n"
            "             b. Create a notebook file named\n"
            "                'monte_carlo_convergence_report' with the\n"
            "                COMPLETE report as code_content.\n"
            "           If no such tools are available, skip Step 2.\n\n"
            "  STEP 3.  Give your final answer with the SAME complete\n"
            "           report text.  COPY-PASTE the actual table.\n\n"
            "Never invent data — use only data from upstream agents."
        ),
        depends_on=["planner_agent", "runner_agent", "analyzer_agent"],
    ),
    # ← Function node: runs save_report() after reporter_agent finishes
    PipelineNode(
        agent_id="save_report",
        fn=save_report,
        task_description="Persist final report to DDict.",
        depends_on=["reporter_agent"],
    ),
])


# ===========================================================================
# Helpers
# ===========================================================================

def _make_agent_kwargs(agent_id, name, role, registry, inference_queue,
                       approval_filter=None,
                       max_tool_call_iterations=20,
                       memory=None,
                       summarizer_inference_queue=None,
                       mcp_servers=None):
    """Build kwargs for create_sub_agent() — all features combined."""
    return {
        "config": AgentConfig(
            agent_id=agent_id, name=name, role=role,
            inference_queue=inference_queue,
            summarizer_inference_queue=summarizer_inference_queue,
            approval_filter=approval_filter,
            max_tool_call_iterations=max_tool_call_iterations,
            memory=memory,
        ),
        "tool_registry": registry,
        "mcp_servers": mcp_servers,
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
    # -------------------------------------------------------------------
    # 1. Main inference pipeline
    # -------------------------------------------------------------------
    input_queue = Queue()

    print("[startup] Initializing main inference pipeline...", flush=True)

    inference_pipeline = None
    try:
        inference_pipeline = Inference(INFERENCE_CONFIG, input_queue)
        inference_pipeline.initialize()
    except Exception as exc:
        import traceback
        print(f"\n[FATAL] Main inference pipeline failed: {exc}", flush=True)
        traceback.print_exc()
        if inference_pipeline is not None:
            inference_pipeline.destroy()
        return
    print("[startup] Main inference pipeline ready.\n", flush=True)

    # -------------------------------------------------------------------
    # 2. Summarizer inference pipeline
    # -------------------------------------------------------------------
    summarizer_input_queue = Queue()

    print("[startup] Initializing summarizer inference pipeline...", flush=True)

    summarizer_pipeline = None
    try:
        summarizer_pipeline = Inference(SUMMARIZER_CONFIG, summarizer_input_queue)
        summarizer_pipeline.initialize()
    except Exception as exc:
        import traceback
        print(f"\n[FATAL] Summarizer pipeline failed: {exc}", flush=True)
        traceback.print_exc()
        inference_pipeline.destroy()
        if summarizer_pipeline is not None:
            summarizer_pipeline.destroy()
        return
    print("[startup] Summarizer inference pipeline ready.\n", flush=True)

    # -------------------------------------------------------------------
    # 3. MCP server (optional)
    # -------------------------------------------------------------------
    mcp_url = sys.argv[1] if len(sys.argv) >= 2 else None

    TOKEN_FILE = os.path.join(os.getcwd(), "token.txt")
    mcp_token = None
    if mcp_url:
        try:
            with open(TOKEN_FILE, "rb") as f:
                raw = f.read()
            mcp_token = raw.decode("utf-8").strip().strip("\ufeff")
            print(f"[startup] MCP token loaded from {TOKEN_FILE}", flush=True)
        except FileNotFoundError:
            print(f"[startup] WARNING: {TOKEN_FILE} not found.", flush=True)

    reporter_mcp_servers = None
    if mcp_url:
        reporter_mcp_servers = [
            MCPServerConfig(url=mcp_url, alias="jupyter", token=mcp_token),
        ]
        print(f"[startup] Reporter MCP: {mcp_url}", flush=True)
    else:
        print("[startup] No MCP URL — reporter uses local tools only.", flush=True)

    # -------------------------------------------------------------------
    # 4. Node placement
    # -------------------------------------------------------------------
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
                "studies on an HPC cluster.",
                planner_registry, input_queue,
                approval_filter=lambda n, a: n == "propose_experiment",
                max_tool_call_iterations=60,
                memory=MemoryConfig(
                    strategy=MemoryStrategy.SLIDING_WINDOW,
                    max_kept_turns=3,
                ),
            ),
            _make_agent_kwargs(
                "runner_agent", "Parallel Simulation Runner",
                "You manage parallel Monte Carlo simulations.\n"
                "You MUST call all three tools in order: "
                "launch_experiment → check_progress → collect_results.\n"
                "NEVER give a final answer without calling collect_results first.",
                runner_registry, input_queue,
                approval_filter=lambda n, a: n == "launch_experiment",
                max_tool_call_iterations=60,
                memory=MemoryConfig(
                    strategy=MemoryStrategy.SUMMARIZE,
                    max_kept_turns=1,
                    summarize_after_turns=2,
                ),
                summarizer_inference_queue=summarizer_input_queue,
            ),
            _make_agent_kwargs(
                "analyzer_agent", "Convergence Analyzer",
                "Analyse convergence.  Call analyze_convergence with "
                "n_samples + absolute_error dicts.  Report verbatim.",
                analyzer_registry, input_queue,
            ),
            _make_agent_kwargs(
                "reporter_agent", "Report Writer",
                "Write a structured report with Markdown tables.  "
                "If you have Jupyter tools, also create a notebook.  "
                "Always include tool output verbatim.",
                reporter_registry, input_queue,
                mcp_servers=reporter_mcp_servers,
            ),
        ]
        policies = [None, compute_policy, None, None]

        procs, queues = _start_agents(agent_specs, policies)
        for spec in agent_specs:
            spec["config"].input_queue = queues[spec["config"].agent_id]

        # ← NEW: tracing=True enables the JSONL trace log
        orchestrator = DAGOrchestrator(
            config=OrchestratorConfig(
                agents=[s["config"] for s in agent_specs],
                poll_interval=0.5,
                poll_timeout=14400.0,
                tracing=True,          # ← writes traces/<run-id>.jsonl
            ),
            pipeline=pipeline,
        )

        user_input = (
            "Design and run a Monte Carlo convergence experiment for π.  "
            "I want to review the plan first, then approve the parallel "
            "launch.  Monitor progress and report which nodes ran what."
            + (
                "  Save the final report to a Jupyter notebook on the cluster."
                if mcp_url else ""
            )
        )

        batch = Batch()
        try:
            print("=" * 60, flush=True)
            print("Dragon AI — 06 Full Pipeline (all features)", flush=True)
            print("=" * 60, flush=True)
            print(f"Request: {user_input}\n", flush=True)

            # Feature summary
            print("Active features:", flush=True)
            print("  DAG .............. 4 agents + save_report fn node", flush=True)
            print("  HITL ............. planner (propose_experiment), "
                  "runner (launch_experiment)", flush=True)
            print("  Memory ........... planner=SLIDING_WINDOW, "
                  "runner=SUMMARIZE", flush=True)
            print(f"  MCP .............. {'reporter → ' + mcp_url if mcp_url else 'disabled'}", flush=True)
            print("  Tracing .......... enabled → ./traces/", flush=True)
            print(flush=True)

            # HITL instructions
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

            # Trace viewer instructions
            if orchestrator.trace_address:
                th, tp = orchestrator.trace_address
                print(
                    f"Trace viewer — start in another terminal:\n"
                    f"  python -m dragon.ai.agent.observability --tcp {th}:{tp} -i\n"
                    f"  Options:\n"
                    f"    -i, --interactive   Curses TUI (recommended)\n"
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
            batch.destroy()

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
        print("[teardown] Main inference pipeline stopped.", flush=True)
        try:
            summarizer_pipeline.destroy()
        except Exception:
            pass
        print("[teardown] Summarizer inference pipeline stopped.", flush=True)


if __name__ == "__main__":
    mp.set_start_method("dragon")
    asyncio.run(main())
