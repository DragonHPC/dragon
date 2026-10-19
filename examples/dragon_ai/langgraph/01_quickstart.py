"""01 - Quickstart: run LangGraph agents as Dragon processes.

The new Dragon ability
----------------------
``launch_host(...)`` spawns one persistent Dragon **AgentHost process**, and
each agent you register runs inside it — a sync (``def``) node on the host's
thread pool, an async (``async def``) node on the host's event loop.  What
changes is *where* your agents run: instead of the coordinator's own Python
process, they run in a separate Dragon process on the runtime.  You wrap each
agent with ``executor.node(name)`` to dispatch to that host; the graph
definition itself is unchanged.  Because each ``launch_host`` call is its own
process, you can place hosts on different cluster nodes by calling it once per
node (see 02).

Run
---
    dragon examples/dragon_ai/langgraph/01_quickstart.py
"""

from __future__ import annotations

import dragon
import multiprocessing as mp
import operator
from typing import Annotated, TypedDict

from langgraph.graph import END, START, StateGraph

from dragon.ai.langgraph import DragonExecutor
from dragon.native.process import current as current_process


class State(TypedDict):
    messages: Annotated[list[str], operator.add]
    research: str
    report: str


# =============================================================================
# 2. Node functions - ordinary agent bodies (swap in real LLM/tool calls)
# =============================================================================

def researcher(state: State) -> dict:
    """Simulate a research agent. Replace with real LLM calls."""
    query = state["messages"][-1] if state["messages"] else "unknown"
    findings = f"Research findings on '{query}': key insight A, B, C."
    print(f"  [researcher] puid={current_process().puid} query='{query}'")
    return {"messages": [findings], "research": findings}


def writer(state: State) -> dict:
    """Simulate a writing agent that produces a report from research."""
    research = state.get("research", "")
    report = f"Report: Based on research — {research}"
    print(f"  [writer] puid={current_process().puid} drafting report")
    return {"messages": [report], "report": report}


# =============================================================================
# 3. Build and run
# =============================================================================

def main() -> None:
    print(f"[orchestrator] main process puid={current_process().puid}")
    # DragonExecutor manages the AgentHost processes. A few optional knobs
    # control concurrency and memory — shown here at their DEFAULTS, so this
    # runs identically whether or not you pass them:
    #   max_concurrent_tasks (default 64) — max agent tasks in flight across the
    #       WHOLE system at once (global backpressure). Raise for more
    #       parallelism/pipelining; lower to use less memory. Everything internal
    #       (channel capacity, host queue sizes) is derived from this one number.
    #   num_shards (default 1) — parallel completion lanes on the coordinator.
    #       Raise ONLY if a single watcher thread can't keep up with a very high
    #       completion rate of small results.
    # You normally set only max_concurrent_tasks (or nothing). See the
    # "Sizing cheat-sheet" in doc/devguide/langgraph.rst and the README.
    with DragonExecutor(
        max_concurrent_tasks=64,
        num_shards=1,
    ) as executor:
        # launch_host spawns ONE AgentHost process; both agents run inside it.
        # max_threads bounds how many *sync* (def) node bodies run concurrently
        # inside this host (they run on a thread pool). Async (async def) nodes
        # run on the host's shared event loop and are NOT bounded by this.
        # Default: max(len(agents) * 4, 8). The agents print a different puid —
        # they live in the host process, not in this orchestrator process.
        executor.launch_host(
            agents={
                "researcher": researcher,
                "writer": writer,
            },
            max_threads=8,
        )

        # Build the graph - executor.node(...) is the only Dragon-specific call.
        builder = StateGraph(State)
        builder.add_node("researcher", executor.node("researcher"))
        builder.add_node("writer", executor.node("writer"))
        builder.add_edge(START, "researcher")
        builder.add_edge("researcher", "writer")
        builder.add_edge("writer", END)

        graph = builder.compile()

        print("\n=== Running graph ===")
        result = graph.invoke(
            {"messages": ["quantum computing advances 2025"], "research": "", "report": ""},
        )
        print(f"\n=== Final report ===\n{result['report']}")


if __name__ == "__main__":
    mp.set_start_method("dragon", force=True)
    main()
