"""02 - Multi-node placement: pin agents to specific cluster nodes.

The new Dragon ability
----------------------
A ``Policy`` places each AgentHost on a chosen node.  Call ``launch_host``
once per node, each with its own ``Policy``, and your agents run on different
machines in the allocation - co-located with the data or GPUs they need.
Plain LangGraph has no concept of node placement; this is a Dragon runtime
capability surfaced through the integration.

Run (requires 2+ nodes)
---
    dragon examples/dragon_ai/langgraph/02_multinode_placement.py
"""

from __future__ import annotations

import dragon
import multiprocessing as mp
import operator
from typing import Annotated, TypedDict

from langgraph.graph import END, START, StateGraph

from dragon import (
    Policy,
    System
)

from dragon.ai.langgraph import DragonExecutor
from dragon.native.machine import Node
from dragon.native.process import current as current_process


# =============================================================================
# 1. Graph state
# =============================================================================

class State(TypedDict):
    messages: Annotated[list[str], operator.add]
    research: str
    analysis: str
    report: str


# =============================================================================
# 2. Node functions
# =============================================================================

def researcher(state: State) -> dict:
    """Research agent — runs on Node 0."""
    me = current_process()
    query = state["messages"][-1] if state["messages"] else "unknown"
    findings = f"Deep research on '{query}': discovered patterns X, Y, Z."
    print(f"  [researcher] puid={me.puid} node={me.node} findings ready")
    return {"messages": [findings], "research": findings}


def analyzer(state: State) -> dict:
    """Analysis agent — runs on Node 1, co-located with GPU resources."""
    me = current_process()
    research = state.get("research", "")
    analysis = f"Statistical analysis of: {research} → significance p<0.01"
    print(f"  [analyzer] puid={me.puid} node={me.node} analysis complete")
    return {"messages": [analysis], "analysis": analysis}


def writer(state: State) -> dict:
    """Writer agent — runs on Node 1 alongside analyzer (shared resources)."""
    me = current_process()
    research = state.get("research", "")
    analysis = state.get("analysis", "")
    report = f"Final Report:\n  Research: {research}\n  Analysis: {analysis}"
    print(f"  [writer] puid={me.puid} node={me.node} report drafted")
    return {"messages": [report], "report": report}


# =============================================================================
# 3. Build and run with multi-node placement
# =============================================================================

def main() -> None:
    me = current_process()
    print(f"[orchestrator] main process puid={me.puid} node={me.node}")

    # Discover cluster topology
    system = System()
    node_list = system.nodes
    assert len(node_list) >= 2, "This example requires at least 2 cluster nodes"

    node0_hostname = Node(node_list[0]).hostname
    node1_hostname = Node(node_list[1]).hostname

    print(f"Cluster: node0={node0_hostname}, node1={node1_hostname}")

    # Policies for placement
    policy_node0 = Policy(
        placement=Policy.Placement.HOST_NAME,
        host_name=node0_hostname,
    )
    policy_node1 = Policy(
        placement=Policy.Placement.HOST_NAME,
        host_name=node1_hostname,
    )

    with DragonExecutor() as executor:
        # Node 0: researcher agent
        host0 = executor.launch_host(
            agents={"researcher": researcher},
            policy=policy_node0,
        )
        # Node 1: analyzer + writer (share one host process on the same node)
        host1 = executor.launch_host(
            agents={"analyzer": analyzer, "writer": writer},
            policy=policy_node1,
        )

        # Alternatively, launch_hosts() does the same thing in one call:
        #
        #   hosts = executor.launch_hosts(
        #       hosts=[
        #           {"researcher": researcher},
        #           {"analyzer": analyzer, "writer": writer},
        #       ],
        #       policies=[policy_node0, policy_node1],
        #   )
        #   host0, host1 = hosts

        print(f"  host0 puid={host0.puid}, host1 puid={host1.puid}")

        # Build graph
        builder = StateGraph(State)
        builder.add_node("researcher", executor.node("researcher"))
        builder.add_node("analyzer", executor.node("analyzer"))
        builder.add_node("writer", executor.node("writer"))

        builder.add_edge(START, "researcher")
        builder.add_edge("researcher", "analyzer")
        builder.add_edge("analyzer", "writer")
        builder.add_edge("writer", END)

        graph = builder.compile()

        # Run
        print("\n=== Running multi-node graph ===")
        result = graph.invoke(
            {"messages": ["climate modeling at scale"], "research": "", "analysis": "", "report": ""},
        )
        print(f"\n=== Result ===\n{result['report']}")


if __name__ == "__main__":
    mp.set_start_method("dragon", force=True)
    main()
