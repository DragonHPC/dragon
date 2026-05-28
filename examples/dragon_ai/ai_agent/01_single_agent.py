"""01 — Single Agent with Tool Registration.

**Prerequisites:** None — this is the starting point.

**What you'll learn:**

* How to stand up a single Dragon agent with an LLM backend
* How to register a sync tool using ``registry.register(fn)``
* Minimal configuration: ``AgentConfig``, ``ToolRegistry``, ``InferenceConfig``
* The agent lifecycle: create → listen → process → shutdown

Architecture::

    Main Process
      └── Inference.initialize()         ← vLLM engine on 1 GPU
            └── CPUWorker
                  └── InferenceWorker

    planner_agent (Dragon Process)
      └── receives task via Dragon Queue
      └── calls propose_experiment tool
      └── returns result

This is the simplest possible Dragon agent setup.  One agent, one tool,
one inference pipeline.

Usage::

    dragon 01_single_agent.py
"""

import asyncio

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
from dragon.native.event import Event
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

# --- Tool implementation ---------------------------------------------------
from tools import propose_experiment


# ===========================================================================
# User-configurable constants
# ===========================================================================

MODEL_NAME = "/path/to/your/model"       # any vLLM-compatible checkpoint
HF_TOKEN = ""                            # set if the model is gated                              # set if the model is gated


# ===========================================================================
# Inference Pipeline Configuration
#
# Minimal setup: 1 node, 1 GPU, 1 inference worker.
# ===========================================================================

INFERENCE_CONFIG = InferenceConfig(
    model=ModelConfig(
        model_name=MODEL_NAME,
        hf_token=HF_TOKEN,
        tp_size=1,                    # single GPU
        max_tokens=8192,
        max_model_len=32768,
    ),
    hardware=HardwareConfig(
        num_nodes=1,
        num_gpus=1,
        num_inf_workers_per_cpu=1,
    ),
    batching=BatchingConfig(
        batch_wait_seconds=0.1,
        max_batch_size=32,
    ),
)


# ===========================================================================
# Tool registry
#
# register() accepts any callable — sync or async.  It auto-wraps it in
# a FunctionTool, deriving name, description, and parameter schemas from
# the function's __name__, __doc__, and type annotations.
# ===========================================================================

registry = ToolRegistry()
registry.register(propose_experiment)


# ===========================================================================
# Pipeline — single node, single agent
# ===========================================================================

pipeline = Pipeline(nodes=[
    PipelineNode(
        agent_id="planner_agent",
        task_description=(
            "You are a scientific experiment planner.  The user wants to "
            "study Monte Carlo convergence for estimating π.\n\n"
            "Propose an experiment plan by calling propose_experiment with:\n"
            "  - description, sample_sizes, convergence_target, methodology\n\n"
            "Report the approved plan verbatim as your final answer."
        ),
        depends_on=[],
    ),
])


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

    procs, agent_specs = [], []
    try:
        # Create the single agent
        agent_spec = {
            "config": AgentConfig(
                agent_id="planner_agent",
                name="Experiment Planner",
                role=(
                    "You are an experiment planner for Monte Carlo "
                    "convergence studies.  Propose plans via "
                    "propose_experiment."
                ),
                inference_queue=input_queue,
            ),
            "tool_registry": registry,
            "shutdown_event": Event(),
            "reply_queue": Queue(),
        }
        agent_specs = [agent_spec]

        # Launch agent as a Dragon Process
        p = Process(target=create_sub_agent, kwargs=agent_spec)
        p.start()
        procs.append(p)

        # Wait for agent to publish its input queue
        agent_input_queue = agent_spec["reply_queue"].get()
        agent_spec["config"].input_queue = agent_input_queue
        print("[startup] Agent 'planner_agent' ready.", flush=True)

        # Create orchestrator (even for a single agent, it manages
        # the DDict, Batch dispatch, and result collection)
        orchestrator = DAGOrchestrator(
            config=OrchestratorConfig(
                agents=[agent_spec["config"]],
                poll_interval=0.5,
                poll_timeout=120.0,
            ),
            pipeline=pipeline,
        )

        user_input = (
            "Propose a Monte Carlo experiment to estimate π using sample "
            "sizes of 1000, 10000, and 100000."
        )

        batch = Batch()
        try:
            print("=" * 60, flush=True)
            print("Dragon AI — 01 Single Agent", flush=True)
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
            batch.destroy()

    except Exception as exc:
        import traceback
        print(f"\n[error] Fatal: {exc}", flush=True)
        traceback.print_exc()
    finally:
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
        print("\n[teardown] Agent stopped.", flush=True)
        try:
            inference_pipeline.destroy()
        except Exception:
            pass
        print("[teardown] Inference pipeline stopped.", flush=True)


if __name__ == "__main__":
    mp.set_start_method("dragon")
    asyncio.run(main())
