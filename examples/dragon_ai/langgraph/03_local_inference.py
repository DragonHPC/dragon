"""LLM Inference example: agents use Dragon Inference for local LLM calls.

Demonstrates:
- Dragon Inference pipeline (vLLM) running on GPU nodes
- ``get_llm(queue)`` — the per-host accessor that builds a DragonQueueLLMProxy
  inside the agent host, on demand, cached per inference pipeline
- Async agent nodes that ``await get_llm(...).chat(...)`` on the host event loop
- Agents share one inference pipeline (one model, many agents)
- No external API calls — inference is local on-cluster

Why ``get_llm`` (and not a proxy passed in): the proxy holds a loop-bound
response pool, so it must be built *inside* the host process and used on the
host's event loop.  You pass the inference *queue handle* into your node (via a
closure) and call ``get_llm(queue)`` inside it; the proxy is created there, once,
and reused.  See the two-model variant at the bottom for different models on the
same host.

Run (requires GPU nodes)
---
    dragon examples/dragon_ai/langgraph/03_local_inference.py

Architecture
------------
    Node 0: AgentHost (one event loop) + CPU workers
    Node 1: vLLM inference workers (GPUs)

    ┌─────────────────────┐           ┌──────────────────────┐
    │ AgentHost (Node 0)  │           │ Inference (Node 1)   │
    │  ├─ researcher ─────┼──Queue──▶ │  ├─ vLLM worker GPU0 │
    │  └─ writer    ◀─────┼──Queue──  │  └─ vLLM worker GPU1 │
    │   (async; share one │           │    (tp_size=2)       │
    │    get_llm proxy)   │           │                      │
    └─────────────────────┘           └──────────────────────┘
"""

from __future__ import annotations

import asyncio
import dragon
import multiprocessing as mp
import operator
from functools import partial
from typing import Annotated, Any, TypedDict

from langgraph.graph import END, START, StateGraph

from dragon.ai.inference.config import (
    BatchingConfig,
    HardwareConfig,
    InferenceConfig,
    ModelConfig,
)
from dragon.ai.inference.inference_utils import Inference
from dragon.ai.langgraph import DragonExecutor, get_llm
from dragon.native.queue import Queue


# =============================================================================
# 1. Graph state
# =============================================================================

class State(TypedDict):
    messages: Annotated[list[str], operator.add]
    query: str
    research: str
    report: str


# =============================================================================
# 2. Agent node functions — async, and they pull the LLM with get_llm(queue)
#    inside the node (so the proxy is built in the host, on the host loop).
#    The inference queue handle is injected per node via functools.partial in
#    main(); a Dragon Queue handle is picklable and safe to close over.
# =============================================================================

async def researcher(state: State, *, input_queue: Any) -> dict:
    """Research agent — queries the local LLM for information."""
    query = state.get("query", "unknown topic")

    resp = await get_llm(input_queue).chat([
        {"role": "system", "content": "You are a research assistant. Provide concise findings."},
        {"role": "user", "content": f"Research the following topic: {query}"},
    ])

    print(f"  [researcher] got response ({len(resp)} chars)")
    return {"messages": [resp], "research": resp}


async def writer(state: State, *, input_queue: Any) -> dict:
    """Writer agent — drafts a report from research findings."""
    research = state.get("research", "")

    resp = await get_llm(input_queue).chat([
        {"role": "system", "content": "You are a technical writer. Write clear, concise reports."},
        {"role": "user", "content": f"Write a brief report based on these findings:\n{research}"},
    ])

    print(f"  [writer] drafted report ({len(resp)} chars)")
    return {"messages": [resp], "report": resp}


async def summarizer(state: State, *, input_queue: Any) -> dict:
    """Summarizer agent — produces a one-paragraph summary."""
    report = state.get("report", "")

    resp = await get_llm(input_queue).chat([
        {"role": "system", "content": "Summarize in one paragraph."},
        {"role": "user", "content": f"Summarize this report:\n{report}"},
    ])

    print("  [summarizer] summary ready")
    return {"messages": [resp]}


# =============================================================================
# 3. Build and run
# =============================================================================

def main() -> None:
    # -- Configure inference pipeline -----------------------------------------
    config = InferenceConfig(
        model=ModelConfig(
            model_name="meta-llama/Llama-3.1-8B-Instruct",
            hf_token="<YOUR_HF_TOKEN>",  # Set via env: HF_TOKEN
            tp_size=2,                    # 2 GPUs for tensor parallelism
            max_tokens=512,
            max_model_len=4096,
        ),
        hardware=HardwareConfig(
            num_nodes=1,    # 1 node for inference
            num_gpus=2,     # 2 GPUs on that node
        ),
        batching=BatchingConfig(
            enabled=True,
            batch_wait_seconds=0.05,
            max_batch_size=32,
        ),
    )

    # -- Launch inference pipeline and grab its input queue -------------------
    input_queue = Queue(maxsize=256)
    inference = Inference(config=config, input_queue=input_queue)
    inference.initialize()
    print("Inference pipeline ready")

    # -- Launch agent host and build graph ------------------------------------
    # Inject the inference QUEUE HANDLE into each node via functools.partial.
    # The node builds/reuses the proxy itself via get_llm(input_queue), inside
    # the host, on the host's event loop.
    try:
        with DragonExecutor() as executor:
            executor.launch_host(agents={
                "researcher": partial(researcher, input_queue=input_queue),
                "writer": partial(writer, input_queue=input_queue),
                "summarizer": partial(summarizer, input_queue=input_queue),
            })

            builder = StateGraph(State)
            builder.add_node("researcher", executor.node("researcher"))
            builder.add_node("writer", executor.node("writer"))
            builder.add_node("summarizer", executor.node("summarizer"))

            builder.add_edge(START, "researcher")
            builder.add_edge("researcher", "writer")
            builder.add_edge("writer", "summarizer")
            builder.add_edge("summarizer", END)

            graph = builder.compile()

            # -- Run with the async driver, so the async nodes run on the host's
            #    shared event loop (and their get_llm proxy is reused). -------
            print("\n=== Running LangGraph with Dragon Inference ===\n")
            result = asyncio.run(graph.ainvoke(
                {
                    "messages": [],
                    "query": "advances in quantum error correction 2025",
                    "research": "",
                    "report": "",
                },
            ))

            print(f"\n=== Final Report ===\n{result['report']}")
    finally:
        # -- Cleanup inference pipeline ---------------------------------------
        # destroy() ends the workers, joins the process group and closes the
        # input queue. Without it the vLLM workers keep the runtime alive.
        inference.destroy()

    print("\nDone.")


# -----------------------------------------------------------------------------
# Two models on one host?  Just use two inference queues — each get_llm(q)
# returns that pipeline's own cached proxy inside the host:
#
#     async def researcher(state, *, llama_q):
#         return {"research": await get_llm(llama_q).chat([...])}
#
#     async def writer(state, *, mistral_q):
#         return {"report": await get_llm(mistral_q).chat([...])}
#
#     executor.launch_host(agents={
#         "researcher": partial(researcher, llama_q=llama_q),
#         "writer": partial(writer, mistral_q=mistral_q),
#     })
# -----------------------------------------------------------------------------


if __name__ == "__main__":
    mp.set_start_method("dragon", force=True)
    main()
