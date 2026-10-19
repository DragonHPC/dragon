"""
04 - Real-time supervision: watch and abort live experiments.
=============================================================================
The new Dragon ability
----------------------
A LangGraph node is normally an opaque, blocking call: the framework has no
handle to the computation inside it, so it cannot observe a half-finished
experiment or stop it.  Dragon gives the agent first-class handles - a
``Process`` per experiment, a streaming ``Queue``, and a shared ``DDict`` -
so the agent can supervise and intervene *while the experiment runs*.

This self-driving lab (planner -> supervise -> analyze -> loop) launches each
experiment as its own Dragon ``Process``.  Experiments STREAM progress over a
``Queue``; the supervisor consumes those events live and, the instant a run
diverges, sets an abort flag in a shared ``DDict``.  The experiment sees the
flag and stops within one reporting interval - its remaining compute is
RECLAIMED instead of wasted.  That mid-flight steering is impossible in plain
LangGraph; it is an architectural capability, not a tunable.

Notes
-----
* ``launch_host`` runs its agents in one AgentHost process; call it once per
  node (each with its own ``Policy``) to spread agents across the cluster.
  The experiments it launches are independent Dragon ``Process``es regardless.
* The abort path uses Dragon runtime primitives (Process / Queue / DDict);
  the integration's role is letting you drive them from an idiomatic
  LangGraph graph.

Run
---
    dragon examples/dragon_ai/langgraph/04_realtime_supervision.py

For a true multi-node run, uncomment the Policy blocks in main().
=============================================================================
"""

from __future__ import annotations

import dragon
import multiprocessing as mp
import operator
import time
from functools import partial
from typing import Annotated, Any, TypedDict

from langgraph.graph import END, START, StateGraph

# -- The Dragon + LangGraph integration (the only new import) -----------------
from dragon.ai.langgraph import DragonExecutor

# -- Dragon primitives the agents use as first-class handles ------------------
from dragon import DDict
from dragon.native.process import Process
from dragon.native.queue import Queue

# -- Multi-node placement (uncomment on a real allocation) --------------------
# from dragon import Policy, System
# from dragon.native.machine import Node


# =============================================================================
# Lab configuration  (identical to the "before")
# =============================================================================

N_CANDIDATES = 4
MAX_ITERATIONS = 2
STABLE_TEMP = 1.2
DIVERGE_THRESHOLD = 50.0
TOTAL_STEPS = 20
STEP_SECONDS = 0.03
REPORT_EVERY = 2            # stream a progress event every N steps
DDICT_MEM = 256 * 1024**2  # 256 MiB managed heap


# =============================================================================
# Graph state  (identical to the "before", plus reclaimed_steps)
# =============================================================================

class LabState(TypedDict):
    iteration: int
    candidates: list[dict]
    outcomes: list[dict]
    reclaimed_steps: Annotated[int, operator.add]
    abort_latency_ms: list[float]
    done: bool


# =============================================================================
# DDict attach helper (attach-once-and-cache inside long-lived host threads).
# =============================================================================

_DDICT_CACHE: dict[bytes, Any] = {}


def _get_ddict(serialized: bytes) -> Any:
    d = _DDICT_CACHE.get(serialized)
    if d is None:
        d = DDict.attach(serialized)
        _DDICT_CACHE[serialized] = d
    return d


# =============================================================================
# THE EXPERIMENT — now a standalone Dragon Process.
# It STREAMS progress as it runs and OBEYS a mid-flight abort flag.
# (Same solver math as the "before"; the contract is what's different.)
# =============================================================================

def _experiment_process(exp_id: str, params: dict, ddict_ser: bytes,
                        progress_q: Any) -> None:
    ddict = _get_ddict(ddict_ser)
    ddict[f"control:{exp_id}"] = "run"

    temp = params["temperature"]
    x = 0.1
    for step in range(1, TOTAL_STEPS + 1):
        x = x * (2.71828 ** ((temp - STABLE_TEMP) * 0.25))
        time.sleep(STEP_SECONDS)

        if step % REPORT_EVERY == 0 or step == TOTAL_STEPS:
            progress_q.put({"id": exp_id, "step": step, "metric": float(x),
                            "status": "running"})
            # Cooperative cancellation: stop if the supervisor said so.
            if ddict.get(f"control:{exp_id}") == "abort":
                progress_q.put({"id": exp_id, "step": step, "metric": float(x),
                                "status": "aborted"})
                return

    score = -abs(x - 1.0)
    progress_q.put({"id": exp_id, "step": TOTAL_STEPS, "metric": float(x),
                    "status": "done", "score": float(score)})


# =============================================================================
# Agents (graph nodes)
# =============================================================================

def planner_node(state: LabState) -> dict:
    """Identical proposal logic to the 'before'."""
    it = state.get("iteration", 0)
    temps = [0.8, 1.0, 1.6, 2.0] if it == 0 else [0.6, 0.9, 1.1, 1.8]
    candidates = [
        {"id": f"exp-{it}-{i}", "temperature": t} for i, t in enumerate(temps)
    ]
    print(f"\n[planner] round {it}: proposing {len(candidates)} experiments "
          f"(temps={temps})")
    return {"candidates": candidates, "iteration": it}


def supervise_node(state: LabState, *, ddict_ser: bytes) -> dict:
    """Launch experiments as Dragon Processes and WATCH them live.

    This is what 'before' cannot do: each experiment is a real handle, progress
    streams over a Queue, and a diverging run is aborted mid-flight by setting
    its DDict flag. Remaining steps are reclaimed instead of wasted.
    """
    ddict = _get_ddict(ddict_ser)
    candidates = state["candidates"]
    progress_q = Queue()

    # --- launch each experiment as its own Dragon Process --------------------
    procs: dict[str, Any] = {}
    for c in candidates:
        p = Process(target=_experiment_process,
                    args=(c["id"], c, ddict_ser, progress_q))
        p.start()
        procs[c["id"]] = p
    print(f"[supervise] launched {len(procs)} experiments as Dragon Processes "
          f"— watching live")

    # --- consume the live event stream and intervene -------------------------
    outcomes: list[dict] = []
    reclaimed = 0
    abort_latencies: list[float] = []
    finished: set[str] = set()
    abort_t0: dict[str, float] = {}

    while len(finished) < len(procs):
        ev = progress_q.get()                # blocks until the next live event
        eid, status = ev["id"], ev["status"]

        if status == "running":
            # The "decision": is this running experiment diverging right now?
            if eid not in abort_t0 and ev["metric"] > DIVERGE_THRESHOLD:
                abort_t0[eid] = time.perf_counter()
                ddict[f"control:{eid}"] = "abort"     # <-- mid-flight kill
                print(f"  ! ABORT {eid} at step {ev['step']} "
                      f"(metric={ev['metric']:.1f}) — reclaiming its compute")

        elif status == "aborted":
            latency = (time.perf_counter() - abort_t0.get(eid, time.perf_counter())) * 1e3
            abort_latencies.append(latency)
            reclaimed += (TOTAL_STEPS - ev["step"])
            outcomes.append({"id": eid, "final_metric": ev["metric"],
                             "score": -abs(ev["metric"] - 1.0), "aborted": True})
            finished.add(eid)
            print(f"  + {eid} stopped at step {ev['step']} "
                  f"({latency:.0f} ms after abort signal)")

        elif status == "done":
            outcomes.append({"id": eid, "final_metric": ev["metric"],
                             "score": ev["score"], "aborted": False})
            finished.add(eid)
            print(f"  + {eid} finished score={ev['score']:.3f}")

    for p in procs.values():
        p.join()

    return {"outcomes": outcomes, "reclaimed_steps": reclaimed,
            "abort_latency_ms": abort_latencies}


def analyze_node(state: LabState) -> dict:
    """Identical decision logic to the 'before'."""
    it = state["iteration"]
    best = max(state["outcomes"], key=lambda o: o["score"])
    converged = best["score"] > -0.2
    done = converged or (it + 1) >= MAX_ITERATIONS
    print(f"[analyze] round {it}: best={best['id']} "
          f"score={best['score']:.3f} -> {'DONE' if done else 'refine'}")
    return {"iteration": it + 1, "done": done}


def route(state: LabState) -> str:
    return END if state["done"] else "planner"


# =============================================================================
# Build and run — same graph shape; only the executor + DDict are new.
# =============================================================================

def main() -> None:
    # -- shared DDict the agents and experiments use --------------------------
    ddict = DDict(managers_per_node=1, n_nodes=1, total_mem=DDICT_MEM)
    ddict_ser = ddict.serialize()

    # -- multi-node placement (uncomment on a real allocation) ----------------
    # system = System()
    # policy = Policy(placement=Policy.Placement.HOST_NAME,
    #                 host_name=Node(system.nodes[0]).hostname)

    with DragonExecutor() as executor:
        # One AgentHost process holds these agents. To spread agents across
        # nodes, call launch_host once per node with its own Policy. The
        # experiments each agent launches are already separate Dragon Processes
        # regardless (see supervise_node).
        executor.launch_host(agents={
            "planner": planner_node,
            "supervise": partial(supervise_node, ddict_ser=ddict_ser),
            "analyze": analyze_node,
        })  # , policy=policy)

        # -- the graph is IDENTICAL in shape to the plain-LangGraph version ---
        builder = StateGraph(LabState)
        builder.add_node("planner", executor.node("planner"))
        builder.add_node("supervise", executor.node("supervise"))
        builder.add_node("analyze", executor.node("analyze"))

        builder.add_edge(START, "planner")
        builder.add_edge("planner", "supervise")
        builder.add_edge("supervise", "analyze")
        builder.add_conditional_edges("analyze", route,
                                      {"planner": "planner", END: END})

        graph = builder.compile()

        t0 = time.perf_counter()
        final = graph.invoke(
            {"iteration": 0, "candidates": [], "outcomes": [],
             "reclaimed_steps": 0, "abort_latency_ms": [], "done": False},
            config={"recursion_limit": 50},
        )
        elapsed = time.perf_counter() - t0

    ddict.destroy()

    avg_latency = (sum(final["abort_latency_ms"]) / len(final["abort_latency_ms"])
                   if final["abort_latency_ms"] else 0.0)
    print("\n" + "=" * 60)
    print("AFTER (Dragon + LangGraph) summary")
    print("=" * 60)
    print(f"  wall-time            : {elapsed:6.1f} s")
    print(f"  reclaimed solver steps: {final['reclaimed_steps']}  "
          f"(diverging runs killed mid-flight instead of wasted)")
    print(f"  avg abort latency    : {avg_latency:.0f} ms "
          f"(divergence detected -> experiment stopped)")
    print("  why: experiments are Dragon Processes streaming over a Queue; the")
    print("       supervisor sets a DDict abort flag the instant a run diverges.")
    print("       Same LangGraph graph — only the executor changed.")


if __name__ == "__main__":
    mp.set_start_method("dragon", force=True)
    main()
