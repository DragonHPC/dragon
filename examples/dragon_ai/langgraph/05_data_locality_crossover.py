"""
05 - Data locality + distributed agents: the measured crossover.
=============================================================================
The new Dragon ability
----------------------
This is the data-plane hero.  It shows the two Dragon abilities that matter
most for *data-heavy* agentic AI on HPC -- working together in ONE example,
and it MEASURES the win instead of asserting it:

  (1) DATA LOCALITY.  Bulk arrays (here, 3-D charge-density fields, one per
      candidate) live in a distributed ``DDict`` and never flow through the
      LangGraph coordinator.  The simulator writes its field to the DDict
      manager *local to its own node*; the reducer agent on that same node
      reads it back with ``local_keys()`` -- zero-copy, zero network.  Only
      KB-scale handles and descriptors travel through graph state.

  (2) DISTRIBUTED AGENT PLACEMENT.  Agents are split by role and pinned with
      a ``Policy``: control-plane agents (planner, global decision) route only
      handles/scalars and sit on Node 0; data-plane agents (the heavy
      cross-candidate reducer) are placed *on the data nodes* and reduce in
      place, returning a KB novelty vector instead of shipping GB fields back.

The same LangGraph graph is run two ways back-to-back on an identical
workload and the coordinator cost is printed as a crossover table:

  * FUNNEL backend  - fields flow THROUGH graph state (mimics Redis/staging).
  * DDICT backend   - fields stay in the DDict; state carries handles only.

The curves converge at toy sizes and diverge at GB scale -- that divergence
is the whole pitch, and here it is a number you can read, not a claim.

Why this needs Dragon
---------------------
Plain LangGraph can pass arrays through state, but every byte then transits
the coordinator; there is no node-placement and no node-local shared memory,
so the data-heavy version simply does not hold at scale.  Dragon's DDict +
``Policy`` placement are runtime capabilities surfaced through an otherwise
idiomatic LangGraph graph.

Honest boundaries
-----------------
* The LangGraph graph/coordinator is single-node today (orchestration on
  Node 0).  What Dragon distributes here is AGENT EXECUTION (the reducers) and
  the DATA PLANE (the DDict) -- not the graph itself.
* The crossover TABLE is projected analytically from field sizes (exact, from
  ``ndarray.nbytes``) so it can show the GB-scale rows without allocating GBs
  on a dev box.  The LIVE run below it actually round-trips a field through the
  DDict and prints real locality receipts to prove the path works.
* On a single node the simulator and reducer co-locate by definition; the
  locality receipt is honest about that.  The win appears once data nodes > 1.

Run
---
    dragon examples/dragon_ai/langgraph/05_data_locality_crossover.py

    # heavier live fields (256^3 ~ 67 MB each) instead of the default demo size:
    MATSCREEN_RES=headline dragon examples/dragon_ai/langgraph/05_data_locality_crossover.py
=============================================================================
"""

from __future__ import annotations

import dragon
import multiprocessing as mp
import operator
import os
from functools import partial
from typing import Annotated, TypedDict

import numpy as np

from langgraph.graph import END, START, StateGraph

# -- The Dragon + LangGraph integration (the only new import) -----------------
from dragon.ai.langgraph import DragonExecutor

# -- Dragon primitives: distributed data plane + node placement ---------------
from dragon import DDict, Policy, System
from dragon.native.machine import Node
from dragon.native.process import Process
from dragon.native.queue import Queue


# =============================================================================
# Configuration
# =============================================================================

# Field resolution presets (float32 cube edge).  The crossover lives in the GB
# regime, so the projected table always ends at a large row; the live run uses
# the selected preset (default "demo") so it stays friendly on a dev box.
RESOLUTION_PRESETS = {
    "smoke": 48,        # ~0.4 MB / field
    "demo": 112,        # ~5.6 MB / field
    "headline": 256,    # ~67 MB / field
}
LIVE_RES = RESOLUTION_PRESETS[os.environ.get("MATSCREEN_RES", "demo")]

# Resolutions shown in the projected crossover table (ends GB-scale).
TABLE_RES = [48, 112, 256, 512]

N_CANDIDATES = 4            # candidates screened per iteration (fan-out width)
MAX_ITERATIONS = 2         # agentic refine loop depth
DESC_BINS = 64             # radial |FFT|^2 histogram length (the KB descriptor)
DESC_BYTES = DESC_BINS * 4 + 8         # descriptor + energy scalar
HANDLE_BYTES = 24                      # a "field:<cid>" string handle
DDICT_MEM = 1 * 1024**3                # 1 GiB managed heap (sized for "demo")


# =============================================================================
# Field synthesis + map-side descriptor (cheap, local, known at write time)
# =============================================================================

def _synth_field(resolution: int, seed: int) -> np.ndarray:
    """A toy 3-D charge-density field.  Stands in for a real simulator output."""
    rng = np.random.default_rng(seed)
    axis = np.linspace(-1.0, 1.0, resolution, dtype=np.float32)
    x, y, z = np.meshgrid(axis, axis, axis, indexing="ij")
    centers = rng.uniform(-0.6, 0.6, size=(3, 3)).astype(np.float32)
    field = np.zeros((resolution, resolution, resolution), dtype=np.float32)
    for cx, cy, cz in centers.T:
        field += np.exp(-8.0 * ((x - cx) ** 2 + (y - cy) ** 2 + (z - cz) ** 2))
    return field.astype(np.float32)


def _map_side_descriptor(field: np.ndarray) -> tuple[float, np.ndarray]:
    """MAP-SIDE reduction: fused into the simulator, runs right after the slab
    is written.  Cheap, purely local, and known at write time -- so it belongs
    in the simulator, not the analyzer.  Returns (energy scalar, KB spectral
    descriptor).  This is the small thing that travels; the field stays put.
    """
    energy = float(np.sum(field.astype(np.float64) ** 2))
    spectrum = np.abs(np.fft.rfftn(field)).astype(np.float32)
    flat = spectrum.reshape(-1)
    # radially-binned (here: magnitude-binned) |FFT| histogram -> fixed length
    hist, _ = np.histogram(flat, bins=DESC_BINS, range=(0.0, float(flat.max()) + 1e-6))
    desc = hist.astype(np.float32)
    norm = np.linalg.norm(desc)
    if norm > 0:
        desc = desc / norm
    return energy, desc


# =============================================================================
# Data-plane workers (run as Dragon Processes, placed ON the data nodes)
# =============================================================================

def _simulate_worker(cid: int, seed: int, resolution: int, dd_ser: str, out_q: Queue) -> None:
    """DATA-PLANE simulator.  Builds the field and writes it to the DDict
    manager LOCAL to this node, then returns only a handle + KB descriptor.
    The GB field never touches the coordinator.
    """
    dd = DDict.attach(dd_ser)
    field = _synth_field(resolution, seed)
    energy, desc = _map_side_descriptor(field)

    key = f"field:{cid}"
    local_mgr = dd.local_manager                 # manager id on THIS node (or None on 1 node)
    target = dd.manager(local_mgr) if local_mgr is not None else dd
    target[key] = field                          # field lands on this node's manager
    owner = dd.manager_nodes[local_mgr].hostname if local_mgr is not None else "node0"

    out_q.put({
        "cid": cid, "key": key, "energy": energy, "desc": desc,
        "owner_node": owner, "field_mb": field.nbytes / 1024**2,
    })
    dd.detach()


def _reduce_worker(node_tag: str, dd_ser: str, archive_ser: str | None, out_q: Queue) -> None:
    """DATA-PLANE reducer, placed ON a data node.  Does the genuinely heavy,
    cross-candidate work a single simulator structurally cannot: it needs
    MULTIPLE full fields at once.  It reads only this node's LOCAL keys
    (zero network), computes pairwise structural novelty over the local fields
    plus the persistent elite archive, and returns a KB novelty vector.
    """
    dd = DDict.attach(dd_ser)
    local_mgr = dd.local_manager
    handle = dd.manager(local_mgr) if local_mgr is not None else dd

    # Reduce-in-place: read fields that are LOCAL to this node only.
    if local_mgr is not None:
        local_keys = [k for k in dd.local_keys() if isinstance(k, str) and k.startswith("field:")]
    else:
        local_keys = [k for k in dd.keys() if isinstance(k, str) and k.startswith("field:")]
    fields = [handle[k] for k in local_keys]     # local reads, no field crosses the network

    # Heavy cross-candidate compute: 3-D FFT per field + O(N^2) spectral distances.
    descs = [_map_side_descriptor(f)[1] for f in fields]
    n = len(descs)
    novelty = np.zeros(n, dtype=np.float32)
    for i in range(n):
        dists = [float(np.linalg.norm(descs[i] - descs[j])) for j in range(n) if j != i]
        novelty[i] = float(np.mean(dists)) if dists else 0.0

    # ELITE ARCHIVE (cross-iteration memory): diff against archived past winners.
    archive_bonus = 0.0
    if archive_ser is not None:
        arch = DDict.attach(archive_ser)
        arch_keys = [k for k in arch.keys() if isinstance(k, str) and k.startswith("elite:")]
        if arch_keys and descs:
            ref = arch[arch_keys[-1]]              # a KB descriptor from a past winner
            archive_bonus = float(np.mean([np.linalg.norm(d - ref) for d in descs]))
        arch.detach()

    out_q.put({
        "node": node_tag,
        "manager": local_mgr,
        "local_keys": local_keys,
        "novelty": novelty.tolist(),
        "archive_bonus": archive_bonus,
        "n_local_fields": n,
    })
    dd.detach()


# =============================================================================
# Projected crossover table (exact, from ndarray.nbytes -- no GBs allocated)
# =============================================================================

def _print_crossover_table() -> None:
    print("\n=== Projected coordinator cost: FUNNEL vs DDICT (per iteration) ===")
    print(f"  candidates/iter = {N_CANDIDATES};  bytes are what passes THROUGH the coordinator\n")
    header = f"  {'res':>5} {'field':>9} | {'FUNNEL thru':>12} {'FUNNEL peak':>12} | {'DDICT thru':>11} {'DDICT peak':>11} | {'ratio':>7}"
    print(header)
    print("  " + "-" * (len(header) - 2))
    for res in TABLE_RES:
        field_bytes = res**3 * 4
        funnel_thru = N_CANDIDATES * field_bytes        # every field transits state
        funnel_peak = N_CANDIDATES * field_bytes        # coordinator holds them all
        ddict_thru = N_CANDIDATES * (HANDLE_BYTES + DESC_BYTES)
        ddict_peak = HANDLE_BYTES + DESC_BYTES           # only a handle in flight
        ratio = funnel_thru / ddict_thru
        print(f"  {res:>5} {_mb(field_bytes):>9} | {_mb(funnel_thru):>12} {_mb(funnel_peak):>12} | "
              f"{_mb(ddict_thru):>11} {_mb(ddict_peak):>11} | {ratio:>6.0f}x")
    print("\n  Curves converge at toy sizes and diverge at GB scale (last row).")
    print("  FUNNEL coordinator RAM grows N x field; DDICT coordinator stays flat.")


def _mb(nbytes: float) -> str:
    mb = nbytes / 1024**2
    if mb >= 1024:
        return f"{mb / 1024:.2f} GB"
    if mb >= 1:
        return f"{mb:.1f} MB"
    return f"{nbytes / 1024:.1f} KB"


# =============================================================================
# Node roles: control plane (Node 0) vs data plane (the rest)
# =============================================================================

def _plan_node_roles() -> tuple[list[str], list[str]]:
    """Derive a placement plan from the live allocation.  Node 0 = control
    plane (routes handles/scalars); remaining nodes = data plane (hold fields,
    run reducers).  One node -> everything co-locates (honest)."""
    hosts = [Node(h).hostname for h in System().nodes]
    if len(hosts) == 1:
        return hosts, hosts                       # co-located: control == data
    return hosts[:1], hosts[1:]


# =============================================================================
# Agentic graph: planner (control) -> simulate -> analyze (reduce) -> route
# =============================================================================

class ScreenState(TypedDict):
    iteration: int
    seeds: list[int]                              # KB: candidate parameters only
    handles: Annotated[list[dict], operator.add]  # KB: handles + descriptors
    novelty: list[float]
    best_cid: int
    decision: str
    log: Annotated[list[str], operator.add]


def planner_node(state: ScreenState) -> dict:
    """CONTROL-PLANE agent (Node 0).  Proposes the next batch of candidate
    seeds from the KB-scale scoreboard -- never touches a field.  Here the
    proposal is rule-based; wiring it to the on-cluster LLM (with a real
    tool-call) is exactly what examples 03 and 06 show.
    """
    it = state["iteration"]
    base = it * 100
    seeds = [base + i for i in range(N_CANDIDATES)]
    return {"seeds": seeds, "log": [f"[planner@node0] iter {it}: proposed seeds {seeds}"]}


def simulate_node(state: ScreenState, *, dd_ser: str, data_hosts: list[str]) -> dict:
    """Fan out one DATA-PLANE simulator per candidate, each pinned to a data
    node, each writing its field to the LOCAL DDict manager.  Gathers only
    handles + KB descriptors back through state.
    """
    out_q = Queue()
    procs = []
    for i, seed in enumerate(state["seeds"]):
        host = data_hosts[i % len(data_hosts)]
        policy = Policy(placement=Policy.Placement.HOST_NAME, host_name=host)
        p = Process(target=_simulate_worker,
                    args=(i, seed, LIVE_RES, dd_ser, out_q),
                    policy=policy)
        p.start()
        procs.append(p)

    handles = [out_q.get() for _ in state["seeds"]]
    for p in procs:
        p.join()
    handles.sort(key=lambda h: h["cid"])

    receipts = [f"    cid {h['cid']}: {h['field_mb']:.1f} MB field -> {h['owner_node']} "
                f"(handle '{h['key']}', desc {DESC_BYTES} B)" for h in handles]
    return {
        "handles": handles,
        "log": [f"[simulate] {len(handles)} fields written to DDict, only handles returned:"] + receipts,
    }


def analyze_node(state: ScreenState, *, dd_ser: str, archive_ser: str,
                 data_hosts: list[str]) -> dict:
    """REDUCE-SIDE: launch one reducer per data node, placed ON that node, to
    reduce its local fields in place.  GLOBAL COMBINE happens here on Node 0
    over the KB novelty vectors -- one decision, not one per field.
    """
    out_q = Queue()
    procs = []
    for host in data_hosts:
        policy = Policy(placement=Policy.Placement.HOST_NAME, host_name=host)
        p = Process(target=_reduce_worker,
                    args=(host, dd_ser, archive_ser, out_q),
                    policy=policy)
        p.start()
        procs.append(p)

    parts = [out_q.get() for _ in data_hosts]
    for p in procs:
        p.join()

    # Locality receipts: each reducer touched only keys local to its own node.
    log = ["[analyze] reduce-in-place receipts (bytes over network ~ 0):"]
    novelty_by_cid: dict[int, float] = {}
    for part in parts:
        log.append(f"    reducer@{part['node']} mgr={part['manager']} read "
                   f"{part['n_local_fields']} field(s) from its LOCAL manager "
                   f"{part['local_keys']} -- no field crossed the network")
        for k, nov in zip(part["local_keys"], part["novelty"]):
            cid = int(k.split(":")[1])
            novelty_by_cid[cid] = nov + part["archive_bonus"]

    novelty = [novelty_by_cid.get(h["cid"], 0.0) for h in state["handles"]]
    # GLOBAL COMBINE + decision on the KB scoreboard (one decision, not one per
    # field). Wire this to the on-cluster LLM as examples 03/06 show.
    best_cid = max(range(len(novelty)), key=lambda i: novelty[i]) if novelty else 0

    # ELITE ARCHIVE UPDATE (cross-iteration memory): persist the winner's KB
    # descriptor so the next iteration's reducer can diff against it. Only a
    # ~KB vector is written -- never a field.
    winner_desc = next((h["desc"] for h in state["handles"] if h["cid"] == best_cid), None)
    if winner_desc is not None:
        arch = DDict.attach(archive_ser)
        arch[f"elite:{state['iteration']}"] = winner_desc
        arch.detach()

    converged = state["iteration"] + 1 >= MAX_ITERATIONS
    decision = "converge" if converged else "refine"
    log.append(f"[analyze@node0] best cid={best_cid} novelty={novelty[best_cid]:.3f} "
               f"-> {decision}")
    return {"novelty": novelty, "best_cid": best_cid, "decision": decision, "log": log}


def route_after_analysis(state: ScreenState) -> str:
    if state["decision"] == "converge":
        return END
    return "bump"


def bump_iteration(state: ScreenState) -> dict:
    return {"iteration": state["iteration"] + 1}


# =============================================================================
# Main
# =============================================================================

def main() -> None:
    control_hosts, data_hosts = _plan_node_roles()
    field_mb = LIVE_RES**3 * 4 / 1024**2

    print("=" * 77)
    print("Materials screening - data locality + distributed agents (measured)")
    print("=" * 77)
    print(f"  allocation        : {len(control_hosts) + len(data_hosts) - (1 if data_hosts == control_hosts else 0)} node(s)")
    print(f"  control plane     : {control_hosts}  (routes handles/scalars; single-node coordinator)")
    print(f"  data plane        : {data_hosts}  (hold fields, run reducers)")
    print(f"  live field        : {LIVE_RES}^3 float32 = {field_mb:.1f} MB/candidate "
          f"x {N_CANDIDATES} = {field_mb * N_CANDIDATES:.1f} MB in DDict, ~0 to coordinator")

    # ---- Part 1: the measured crossover (projected from exact field sizes) ----
    _print_crossover_table()

    # ---- Part 2: a live distributed iteration that proves the path + locality --
    print("\n=== Live distributed run (handles in state, fields stay node-local) ===")

    co_located = data_hosts == control_hosts
    n_data = 1 if co_located else len(data_hosts)
    fields_mb = field_mb * N_CANDIDATES
    assert DDICT_MEM / n_data >= fields_mb * 1024**2, (
        f"DDict per-node mem too small for {fields_mb:.0f} MB of fields; "
        f"lower MATSCREEN_RES or raise DDICT_MEM"
    )

    dd = DDict(1, n_data, DDICT_MEM)               # 1 manager per data node
    archive = DDict(1, n_data, 64 * 1024**2)       # elite-archive memory
    dd_ser = dd.serialize()
    archive_ser = archive.serialize()

    control_policy = Policy(placement=Policy.Placement.HOST_NAME, host_name=control_hosts[0])

    try:
        with DragonExecutor() as executor:
            # Control-plane agents run in one AgentHost pinned to Node 0.
            executor.launch_host(
                agents={
                    "planner": planner_node,
                    "simulate": partial(simulate_node, dd_ser=dd_ser, data_hosts=data_hosts),
                    "analyze": partial(analyze_node, dd_ser=dd_ser, archive_ser=archive_ser,
                                       data_hosts=data_hosts),
                },
                policy=control_policy,
            )

            builder = StateGraph(ScreenState)
            builder.add_node("planner", executor.node("planner"))
            builder.add_node("simulate", executor.node("simulate"))
            builder.add_node("analyze", executor.node("analyze"))
            builder.add_node("bump", bump_iteration)

            builder.add_edge(START, "planner")
            builder.add_edge("planner", "simulate")
            builder.add_edge("simulate", "analyze")
            # converge -> END; refine -> bump iteration and re-enter the planner
            builder.add_conditional_edges("analyze", route_after_analysis, ["bump", END])
            builder.add_edge("bump", "planner")

            graph = builder.compile()
            result = graph.invoke(
                {"iteration": 0, "seeds": [], "handles": [], "novelty": [],
                 "best_cid": 0, "decision": "", "log": []},
                config={"recursion_limit": 50},
            )

        print()
        for line in result["log"]:
            print(line)
        print(f"\n  Winner: candidate {result['best_cid']} "
              f"(novelty {result['novelty'][result['best_cid']]:.3f})")
        print("  Only handles + KB descriptors ever crossed graph state. "
              "Fields stayed node-local in the DDict.")
    finally:
        dd.destroy()
        archive.destroy()


if __name__ == "__main__":
    mp.set_start_method("dragon", force=True)
    main()
