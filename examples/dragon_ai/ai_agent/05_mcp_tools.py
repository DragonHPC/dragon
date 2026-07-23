"""05 — MCP Server Integration.

**Prerequisites:** Read ``02_multi_agent_dag.py`` first.

**What's new:**

* **MCPServerConfig** — connect an agent to a remote MCP tool server
* **Scoped tool names** — MCP tools appear as ``alias__tool_name``
  (e.g. ``jupyter__create_notebook``) to avoid collisions with local tools
* **Mixed local + MCP tools** — the reporter agent has both local tools
  (``format_results_table``) and remote MCP tools (Jupyter operations)
* **Auto-discovery** — MCP tool schemas are fetched at connect time and
  merged into the LLM's tool list
* **Token from file** — bearer token read from ``token.txt``, never on CLI
* **Graceful fallback** — if no MCP URL is provided, reporter uses
  local tools only

Architecture::

    planner_agent ──► runner_agent ──► analyzer_agent ──┐
         │                │                             ├──► reporter_agent
         └────────────────┴─────────────────────────────┘      │
                                                         MCP: jupyter__*

Usage::

    # Without MCP (local tools only):
    dragon 05_mcp_tools.py

    # With MCP (Jupyter notebook creation):
    echo '<your-token>' > token.txt
    dragon 05_mcp_tools.py <mcp_server_url>
"""

import asyncio
import os
import sys
from typing import Any

import dragon
import multiprocessing as mp

from dragon.ai.agent.core import create_sub_agent
from dragon.ai.agent.config import (
    AgentConfig,
    MCPServerConfig,       # ← NEW
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
])


# ===========================================================================
# Helpers
# ===========================================================================

def _make_agent_kwargs(agent_id, name, role, registry, inference_queue,
                       mcp_servers=None,
                       max_tool_call_iterations=20):
    """Build kwargs for create_sub_agent().

    NEW in this example: ``mcp_servers`` — list of MCPServerConfig objects.
    The agent connects to each MCP server in its own process at startup.
    """
    return {
        "config": AgentConfig(
            agent_id=agent_id, name=name, role=role,
            inference_queue=inference_queue,
            max_tool_call_iterations=max_tool_call_iterations,
        ),
        "tool_registry": registry,
        "mcp_servers": mcp_servers,    # ← NEW: passed to create_sub_agent
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

    # -------------------------------------------------------------------
    # ← NEW: MCP server setup for the reporter agent
    #
    # Pass the MCP URL as a command-line argument:
    #   dragon 05_mcp_tools.py <mcp_url>
    #
    # The bearer token is read from token.txt in CWD.
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
            print(
                f"[startup] WARNING: {TOKEN_FILE} not found — "
                "MCP connection will have no auth token.",
                flush=True,
            )

    reporter_mcp_servers = None
    if mcp_url:
        reporter_mcp_servers = [
            MCPServerConfig(url=mcp_url, alias="jupyter", token=mcp_token),
        ]
        print(f"[startup] Reporter will connect to MCP: {mcp_url}", flush=True)
    else:
        print(
            "[startup] No MCP URL provided — reporter uses local tools only.\n"
            "  To enable MCP, run:\n"
            "    echo '<token>' > token.txt\n"
            "    dragon 05_mcp_tools.py <mcp_url>",
            flush=True,
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
                "If you have Jupyter tools, also create a notebook.  "
                "Always include tool output verbatim.",
                reporter_registry, input_queue,
                # ← NEW: attach MCP servers to this agent
                mcp_servers=reporter_mcp_servers,
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
            "Monitor progress and report which nodes ran what."
            + (
                "  Save the final report to a Jupyter notebook on the cluster."
                if mcp_url else ""
            )
        )

        batch = Batch()
        try:
            print("=" * 60, flush=True)
            print("Dragon AI — 05 MCP Tools Integration", flush=True)
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
