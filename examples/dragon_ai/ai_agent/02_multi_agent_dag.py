"""02 — Multi-Agent DAG + Function Nodes + Tool Registration Styles.

**Prerequisites:** Read ``01_single_agent.py`` first.

**What's new:**

* **Multi-agent DAG** — four agents wired as a directed acyclic graph
  (planner → runner → analyzer → reporter)
* **Function node** — ``save_report`` runs as plain Python in Dragon Batch
  (no LLM), demonstrating ``PipelineNode(fn=...)``
* **All three tool registration styles:**

  1. ``registry.register(fn)`` — direct callable (sync)
  2. ``@registry.tool`` decorator — inline async tool
  3. ``@registry.tool`` decorator — inline sync tool

* **Async tools** — ``analyze_convergence`` is an async function
* **:param: annotations** — auto-extracted into JSON schemas for the LLM

Architecture::

    planner_agent ──► runner_agent ──► analyzer_agent ──┐
         │                │                             ├──► reporter_agent
         └────────────────┴─────────────────────────────┘         │
                                                          save_report (fn)

Usage::

    dragon 02_multi_agent_dag.py
"""

import asyncio
import json
import math
import os
import time
from typing import Any

import dragon
import multiprocessing as mp

from dragon.ai.agent.core import create_sub_agent
from dragon.ai.agent.config import (
    AgentConfig,
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

from dragon.ai.inference import (
    BatchingConfig,
    HardwareConfig,
    InferenceConfig,
    ModelConfig,
    Inference,
)

# ---------------------------------------------------------------------------
# Style 1: Import existing sync tools, register via registry.register(fn)
# ---------------------------------------------------------------------------
from tools import propose_experiment
from tools.runner import (
    launch_experiment,
    check_progress,
    collect_results,
    cleanup_experiment_state,
)


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
# Tool registries — demonstrating all registration styles
# ===========================================================================

# --- Style 1: registry.register(fn) — sync tools from tools/ package ------
planner_registry = ToolRegistry()
planner_registry.register(propose_experiment)

runner_registry = ToolRegistry()
runner_registry.register(launch_experiment)
runner_registry.register(check_progress)
runner_registry.register(collect_results)

# --- Style 2: @registry.tool — inline async tool --------------------------
# The decorator detects async callables automatically.  :param: annotations
# are extracted into the JSON schema the LLM receives.
analyzer_registry = ToolRegistry()


@analyzer_registry.tool
async def analyze_convergence(results: list) -> dict:
    """Analyse Monte Carlo convergence from experiment results.

    Computes convergence rate (slope of log-log fit) and determines
    whether the experiment meets the theoretical 1/sqrt(n) rate.

    :param results: List of dicts with keys 'n_samples' and 'absolute_error'.
    """
    await asyncio.sleep(0.01)  # simulate async I/O

    if not results or len(results) < 2:
        return {"error": "Need at least 2 data points."}

    sorted_r = sorted(results, key=lambda r: r["n_samples"])
    errors = [r["absolute_error"] for r in sorted_r]
    samples = [r["n_samples"] for r in sorted_r]

    log_n = [math.log10(n) for n in samples]
    log_e = [math.log10(max(e, 1e-12)) for e in errors]
    slope = (log_e[-1] - log_e[0]) / (log_n[-1] - log_n[0])

    return {
        "convergence_rate": round(slope, 4),
        "expected_rate": -0.5,
        "best_error": min(errors),
        "worst_error": max(errors),
        "n_experiments": len(results),
        "meets_theory": abs(slope - (-0.5)) < 0.15,
    }


# --- Style 3: @registry.tool — inline sync tool ---------------------------
reporter_registry = ToolRegistry()


@reporter_registry.tool
def format_results_table(
    results: list,
    convergence_info: dict,
    plan_summary: str,
) -> dict:
    """Format experiment results into a Markdown table for the final report.

    :param results: List of result dicts from collect_results.
    :param convergence_info: Dict from analyze_convergence.
    :param plan_summary: One-line summary of the experiment plan.
    """
    lines = [
        f"## Monte Carlo π Estimation — {plan_summary}",
        "",
        "| Samples | π Estimate | Abs Error | Wall Time (s) | Node |",
        "|--------:|-----------:|----------:|--------------:|------|",
    ]
    for r in sorted(results, key=lambda x: x.get("n_samples", 0)):
        lines.append(
            f"| {r.get('n_samples', '?'):>7,} "
            f"| {r.get('pi_estimate', 0):.6f} "
            f"| {r.get('absolute_error', 0):.6f} "
            f"| {r.get('wall_time_s', 0):>13.3f} "
            f"| {r.get('hostname', '?')} |"
        )
    conv = convergence_info or {}
    lines += [
        "",
        f"**Convergence rate:** {conv.get('convergence_rate', 'N/A')} "
        f"(expected ≈ {conv.get('expected_rate', -0.5)})",
        f"**Meets theoretical rate:** {conv.get('meets_theory', 'N/A')}",
    ]
    return {"markdown_table": "\n".join(lines)}


# ===========================================================================
# Function node: save_report
#
# A PipelineNode with fn= runs as a plain Python function in Dragon Batch.
# No LLM, no agent queue.  Receives TaskResult tokens from upstream nodes,
# reads from DDict, writes files, returns a TaskResult.
# ===========================================================================

REPORT_DIR = os.environ.get("DRAGON_REPORT_DIR", os.getcwd())


def save_report(*upstreams: TaskResult) -> TaskResult:
    """Write the reporter's output to disk as Markdown and JSON."""
    upstream = upstreams[0]
    task_id = upstream.task_id
    serialized_ddict = upstream.serialized_ddict

    print(f"\n[save_report] Function node started (task_id={task_id[:8]}...)",
          flush=True)

    ddict = DDict.attach(serialized_ddict)
    try:
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

        report_text = (
            reporter_result.get("response", str(reporter_result))
            if isinstance(reporter_result, dict)
            else str(reporter_result)
        )

        md_path = os.path.join(REPORT_DIR, "monte_carlo_report.md")
        with open(md_path, "w") as f:
            f.write(report_text)
        print(f"[save_report] Written: {md_path}", flush=True)

        json_path = os.path.join(REPORT_DIR, "monte_carlo_report.json")
        artifact = {
            "task_id": task_id,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "report": report_text,
            "source_agent": "reporter_agent",
        }
        with open(json_path, "w") as f:
            json.dump(artifact, f, indent=2)
        print(f"[save_report] Written: {json_path}", flush=True)

        own_dispatch_id = f"fn-save-report-{task_id[:8]}"
        ddict[DISPATCH_ID_KEY.format(task_id=task_id, agent_id="save_report")] = own_dispatch_id
        ddict[RESULT_KEY.format(task_id=task_id, agent_id="save_report", dispatch_id=own_dispatch_id)] = {
            "response": f"Report saved to:\n  - {md_path}\n  - {json_path}"
        }
        ddict[STATUS_KEY.format(task_id=task_id, agent_id="save_report", dispatch_id=own_dispatch_id)] = TaskStatus.DONE
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
            "Report the approved plan verbatim as your final answer."
        ),
        depends_on=[],
    ),
    PipelineNode(
        agent_id="runner_agent",
        task_description=(
            "You manage parallel Monte Carlo simulations on an HPC cluster.\n\n"
            "Tools:\n"
            "  1. launch_experiment(sample_sizes, seeds) — launches ALL "
            "simulations in parallel.\n"
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
    # Function node — no LLM, runs as plain Python
    PipelineNode(
        agent_id="save_report",
        fn=save_report,
        depends_on=["reporter_agent"],
    ),
])


# ===========================================================================
# Helpers
# ===========================================================================

def _make_agent_kwargs(agent_id, name, role, registry, inference_queue,
                       max_tool_call_iterations=20):
    return {
        "config": AgentConfig(
            agent_id=agent_id, name=name, role=role,
            inference_queue=inference_queue,
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
                "studies on an HPC cluster.",
                planner_registry, input_queue,
            ),
            _make_agent_kwargs(
                "runner_agent", "Parallel Simulation Runner",
                "You manage parallel Monte Carlo simulations.\n"
                "You MUST call all three tools in order: "
                "launch_experiment → check_progress → collect_results.\n"
                "NEVER give a final answer without calling collect_results first.",
                runner_registry, input_queue,
                max_tool_call_iterations=60,
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
                "Always include tool output verbatim — "
                "never use placeholder variables.",
                reporter_registry, input_queue,
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
            "Design and run a Monte Carlo convergence experiment for π "
            "and save the report to disk."
        )

        batch = Batch()
        try:
            print("=" * 60, flush=True)
            print("Dragon AI — 02 Multi-Agent DAG + Function Node", flush=True)
            print("=" * 60, flush=True)
            print(f"Request: {user_input}\n", flush=True)

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
