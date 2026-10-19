"""
06 - Self-driving lab: the full agentic-AI-on-HPC workflow.
=============================================================================
A SELF-DRIVING LAB on HPC -- the capstone, built on **Dragon + LangGraph**.

This combines the abilities from earlier examples into one workflow: an LLM
planner served on-cluster (03) proposes experiments, the data-plane discipline
from the crossover example (05) keeps bulk fields out of the coordinator, and a
supervisor watches runs live and aborts diverging ones mid-flight (04).
Experiments must be watched *while they run*, and a diverging run must be killed
*immediately* so its compute can be reclaimed for promising candidates.

This example shows all three pillars of why Dragon is powerful for agentic
AI on HPC, in ONE workflow:

  (A) LOCAL INFERENCE.  A planner agent calls an LLM served *on the cluster*
      by Dragon Inference (vLLM behind a queue -- no HTTP, no external API)
      to propose the next batch of experiments.  [Optional; rule-based
      fallback so the demo runs without GPUs.]

  (B) SCIENTIFIC WORKFLOW.  Each experiment is a small sinusoidal solver
      submitted as a Dragon **Batch** task, scheduled across the allocation.
      It writes both its live progress and its bulk output into a user-managed
      DDict (zero-copy, never through the coordinator).

  (C) REAL-TIME ACTION -- the headline.  Experiments STREAM progress into the
      DDict as they run.  A supervisor agent polls those DDict entries live
      and, the instant a run diverges, sets its abort flag in the DDict.  The
      experiment sees the flag and stops within one reporting interval.
      No waiting for completion.  No reading data after the fact.  The agent
      steers the lab in real time.

Why pure LangGraph cannot do this
---------------------------------
LangGraph nodes are opaque function calls: a node blocks until it returns,
and the framework has NO handle to the computation running inside it.  You
cannot observe a half-finished experiment, and you cannot stop it.  Dragon
gives the agent first-class handles -- Batch tasks and a shared DDict -- so the
agent can supervise and intervene mid-flight.  That is
an architectural capability, not a feature you can bolt on.

The flow
-----------------------------------------
This is a self-driving optimization campaign: an AI loop repeatedly (1) proposes
a batch of experiments, (2) runs them in parallel, (3) watches them live and
kills the bad ones early, and (4) inspects the survivors, keeps the best, and
decides whether to stop or iterate with better guesses.

It is an ordinary LangGraph ``StateGraph`` with three nodes wired into a cycle::

    START -> planner -> supervisor -> analyzer --(refine?)--> planner
                                          |
                                       (converged?) --> END

  * planner    -- proposes N candidate experiments (each is one parameter).
  * supervisor -- runs those N experiments and babysits them LIVE.
  * analyzer   -- ranks the finished ones, updates the "best so far", and takes
                  the conditional edge: refine (loop) or converged (END).

Three Dragon pieces sit underneath the graph:

  * DragonExecutor -- launches the AgentHost processes and runs each agent
                      inside one (this integration).  planner + analyzer run in
                      one host process; the supervisor runs in a second host
                      process on another node.
  * Batch          -- a separate Dragon scheduler that runs the experiment tasks
                      across the allocation.
  * DDict          -- a shared distributed dictionary; the "blackboard" that
                      every party reads and writes.

Placement: planner + analyzer share one host process (head node); the supervisor
runs in a second host process (compute node); Batch schedules the experiments
wherever there is room.

One round in detail -- what the supervisor actually does::

    receive the candidate list from the planner
      |
      +- for each candidate: submit it to Batch as a task  --> Batch runs the
      |    (submission returns immediately; tasks run async)    experiments
      |
      +- enter a POLL LOOP (every POLL_INTERVAL seconds):
            for each still-running experiment:
               read  progress:{id}  from the DDict   <-- experiments write here
               |
               +- if it looks like it is diverging:
               |     write  control:{id} = "abort"   --> experiment reads this
               |                                          flag and stops itself
               |
               +- if the Batch task finished:
                     record its outcome; drop it from the pending set
      |
      +- when all experiments are done/aborted -> return outcomes to the analyzer

The key mechanism is the DDict used as a two-way message board:

  * experiments WRITE their live progress   -> ``progress:{id}``
  * the supervisor READS that progress and, if a run goes bad, WRITES an abort
    flag                                     -> ``control:{id}``
  * the experiment READS its own abort flag each step and, if set, STOPS itself
    and reports "aborted".

That is the whole "real-time supervision" idea: nobody waits for an experiment
to finish before deciding it is a dud -- a diverging run is stopped mid-flight,
freeing its compute for the next round.

Run
---
    dragon examples/dragon_ai/langgraph/06_self_driving_lab.py            # rule-based judge
    USE_LLM_JUDGE=1 dragon examples/dragon_ai/langgraph/06_self_driving_lab.py   # + local LLM

Agents are placed across two hosts via ``Policy``: the control plane (planner +
analyzer) on the head node, the supervisor on a compute node. On a single-node
allocation both policies resolve to the same node and the demo still runs.
=============================================================================
"""

from __future__ import annotations

import dragon
import multiprocessing as mp
import operator
import os
import time
from functools import partial
from typing import Annotated, Any, TypedDict

import numpy as np

# -- Standard LangGraph imports -----------------------------------------------
from langgraph.graph import END, START, StateGraph

# -- The Dragon + LangGraph integration ---------------------------------------
from dragon.ai.langgraph import DragonExecutor

# -- Dragon Batch (schedules the experiment tasks across the allocation) -------
from dragon import Batch
from dragon.workflows.batch import TaskNotReadyError

# -- Dragon Inference (on-cluster vLLM behind a queue) ------------------------
from dragon.ai.inference.config import (
    BatchingConfig,
    HardwareConfig,
    InferenceConfig,
    ModelConfig,
)
from dragon.ai.inference.inference_utils import Inference
from dragon.ai.inference.llm_proxy import DragonQueueLLMProxy
from dragon.native.queue import Queue

# -- Dragon placement ---------------------------------------------------------
from dragon import (
    Policy,
    System
)
from dragon.native.machine import Node
from dragon.native.process import current as current_process

# =============================================================================
# Lab configuration
# =============================================================================

N_CANDIDATES = 6           # experiments launched per round
MAX_ITERATIONS = 3         # outer optimization rounds
TARGET_FREQ = 1.0          # frequency the campaign is optimizing toward
STABLE_FREQ = 1.5          # experiments with freq above this resonate & diverge
DIVERGE_THRESHOLD = 50.0   # supervisor aborts a run once its amplitude exceeds this
TOTAL_STEPS = 60           # iterations per experiment
REPORT_EVERY = 5           # write a progress event every N steps
POLL_INTERVAL = 0.2        # how often the supervisor polls DDict progress (s)
DDICT_MEM = 1 * 1024**3    # 1 GiB managed heap (waveforms are small)
USE_LLM_JUDGE = os.environ.get("USE_LLM_JUDGE") == "1"


# =============================================================================
# Graph state -- only lightweight handles cross the graph, never bulk data.
# =============================================================================

class LabState(TypedDict):
    iteration: int
    candidates: list[dict]                              # params proposed this round
    outcomes: list[dict]                               # per-experiment summaries
    best_score: float
    best_params: dict
    history: Annotated[list[float], operator.add]
    aborted_count: int                                # interventions this round
    done: bool


# =============================================================================
# DDict attach helper (attach-once-and-cache inside long-lived host threads).
# =============================================================================

_DDICT_CACHE: dict[bytes, Any] = {}


def _get_ddict(serialized: bytes) -> Any:
    d = _DDICT_CACHE.get(serialized)
    if d is None:
        from dragon import DDict
        d = DDict.attach(serialized)
        _DDICT_CACHE[serialized] = d
    return d


# =============================================================================
# Placement helper -- resolve the caller's host process into a readable label.
# ``current().node`` is only a numeric index (0..#nodes-1); ``h_uid`` maps to
# the actual hostname via ``Node``, so we print both to make placement obvious.
# =============================================================================

def _where() -> str:
    me = current_process()
    hostname = Node(me.h_uid).hostname
    return f"puid={me.puid} node={me.node} host={hostname}"


# =============================================================================
# THE EXPERIMENT  (runs as a Dragon Batch task, scheduled onto the allocation).
#
# A tiny sinusoidal solver: value(step) = amp * sin(freq * step).  A stable run
# is a clean sine wave (amp stays 1); a run with freq > STABLE_FREQ resonates
# and its amplitude blows up.  It is iterative, STREAMS its progress into the
# DDict, and is INTERRUPTIBLE: it checks its abort flag (also in the DDict)
# every reporting interval.  Replace the body with a real PDE/MD/CFD step; the
# DDict streaming + abort contract is identical.
# =============================================================================

def _run_experiment(exp_id: str, params: dict, ddict_ser: bytes,
                    total_steps: int) -> dict:
    """One sinusoidal experiment, run as a Dragon Batch task. Streams progress
    into the DDict and obeys a mid-flight abort flag read from the DDict."""
    print(f"  [experiment {exp_id}] {_where()} started")
    ddict = _get_ddict(ddict_ser)
    ddict[f"control:{exp_id}"] = "run"

    freq = params["freq"]
    unstable = freq > STABLE_FREQ
    waveform = np.zeros(total_steps, dtype=np.float32)

    amp = 1.0
    for step in range(1, total_steps + 1):
        if unstable:
            amp *= 1.15                          # resonance blow-up
        waveform[step - 1] = amp * float(np.sin(freq * step))
        time.sleep(0.02)                         # stand-in for real solver flops

        if step % REPORT_EVERY == 0 or step == total_steps:
            # Stream a live progress event INTO the DDict.
            ddict[f"progress:{exp_id}"] = {"id": exp_id, "step": step,
                                           "metric": amp, "status": "running"}
            # Read our abort flag FROM the DDict.
            if ddict.get(f"control:{exp_id}") == "abort":
                ddict[f"progress:{exp_id}"] = {"id": exp_id, "step": step,
                                               "metric": amp, "status": "aborted"}
                return {"id": exp_id, "status": "aborted"}

    # --- completed: store the full waveform (bulk) and score vs target freq --
    ddict[f"wave:{exp_id}"] = waveform            # zero-copy bulk artifact
    score = -abs(freq - TARGET_FREQ)              # closeness to the target frequency
    ddict[f"progress:{exp_id}"] = {"id": exp_id, "step": total_steps,
                                   "metric": amp, "status": "done",
                                   "score": float(score),
                                   "wave_key": f"wave:{exp_id}"}
    return {"id": exp_id, "status": "done", "score": float(score)}


# =============================================================================
# Divergence judgment -- the "decision" the supervisor makes in real time.
# Rule-based by default; optional local-LLM judge to show inference-in-the-loop.
# =============================================================================

def judge_divergence(event: dict, llm: Any | None) -> bool:
    """Return True if this *running* experiment should be aborted now."""
    metric = event["metric"]
    if not np.isfinite(metric) or metric > DIVERGE_THRESHOLD:
        if llm is not None:
            # Inference-in-the-loop: let a cluster-served LLM make the call.
            # A real prompt would include recent metric history / physics context.
            import asyncio
            verdict = asyncio.run(llm.chat([
                {"role": "system", "content": "You are an experiment supervisor."},
                {"role": "user", "content":
                    f"Experiment {event['id']} metric={metric:.2f} at step "
                    f"{event['step']}. Diverging? Answer ABORT or CONTINUE."},
            ]))
            return "ABORT" in str(verdict).upper()
        return True            # rule-based: clear divergence
    return False


# =============================================================================
# Agent 1 -- PLANNER (optionally LLM-driven, on cluster-local inference)
# =============================================================================

def planner_node(state: LabState, *, llm: Any | None = None) -> dict:
    """Propose the next batch of experiments (one frequency each). Deliberately
    seeds some unstable (freq > STABLE_FREQ) candidates so aborts are visible."""
    it = state.get("iteration", 0)
    rng = np.random.default_rng(seed=100 + it)

    if it == 0:
        center, spread = TARGET_FREQ, 0.8       # straddle the stability edge
    else:
        center = state["best_params"]["freq"]
        spread = 0.5 / (it + 1)

    # (Optional) ask a cluster-served LLM to nudge the search -- here we just
    # show the call site; the numeric proposal below is the executable default.
    if llm is not None:
        import asyncio
        _ = asyncio.run(llm.chat([
            {"role": "system", "content": "You are an experiment design AI."},
            {"role": "user", "content":
                f"Round {it}; best params {state.get('best_params', {})}. "
                "Suggest exploration spread."},
        ]))

    candidates = [{
        "id": f"it{it}-e{i}",
        "freq": float(center + rng.normal(0, spread)),
    } for i in range(N_CANDIDATES)]

    print(f"[planner] {_where()} round {it}: "
          f"proposed {len(candidates)} experiments")
    return {"iteration": it, "candidates": candidates}


# =============================================================================
# Agent 2 -- LAB SUPERVISOR: submit to Batch, MONITOR LIVE, and INTERVENE.
#
# This is the real-time core.  It submits each experiment as a Dragon Batch
# task, then polls the DDict for the progress each task streams into it.  The
# moment a progress entry shows divergence, it flips that experiment's abort
# flag in the DDict -- reclaiming the slot without waiting for the run to finish.
# =============================================================================

def supervisor_node(state: LabState, *, ddict_ser: bytes, batch: Any,
                    llm: Any | None = None) -> dict:
    """Submit each experiment as a Dragon Batch task, then supervise LIVE by
    polling the DDict. The instant a run diverges, flip its abort flag in the
    DDict; the task sees it and self-terminates within one reporting interval."""
    ddict = _get_ddict(ddict_ser)
    candidates = state["candidates"]

    # -- Submit every experiment as an independent Batch task (non-blocking) --
    # Batch schedules these across the allocation; submission returns at once.
    tasks: dict[str, Any] = {}
    for c in candidates:
        eid = c["id"]
        ddict[f"control:{eid}"] = "run"
        ddict[f"progress:{eid}"] = {"id": eid, "step": 0, "metric": 0.0,
                                    "status": "running"}
        tasks[eid] = batch.function(_run_experiment, eid, c, ddict_ser, TOTAL_STEPS)
    print(f"[supervisor] {_where()} submitted "
          f"{len(tasks)} Batch experiments; supervising via DDict...")

    # -- Poll the DDict for live progress and intervene in real time ---------
    pending = set(tasks)
    aborting: set[str] = set()
    outcomes: list[dict] = []
    while pending:
        time.sleep(POLL_INTERVAL)
        for eid in list(pending):
            prog = ddict.get(f"progress:{eid}")

            # (1) live intervention while the task is still running
            if prog and prog["status"] == "running" and eid not in aborting:
                if judge_divergence(prog, llm):
                    ddict[f"control:{eid}"] = "abort"     # mid-flight, via DDict
                    aborting.add(eid)
                    print(f"[supervisor]  ! ABORT {eid} at step {prog['step']} "
                          f"(metric={prog['metric']:.1f} diverging) -> reclaiming slot")

            # (2) has the Batch task finished? (authoritative completion signal)
            try:
                tasks[eid].get(block=False)               # non-blocking reap
            except TaskNotReadyError:
                continue                                  # still running
            except Exception:                             # worker raised
                outcomes.append({"id": eid, "status": "error", "score": None})
                pending.discard(eid)
                continue

            # (3) task done -- read its final outcome from the DDict
            final = ddict.get(f"progress:{eid}") or {"status": "done"}
            if final["status"] == "aborted":
                outcomes.append({"id": eid, "status": "aborted", "score": None})
            else:
                outcomes.append({"id": eid, "status": "done",
                                 "score": final.get("score"),
                                 "wave_key": final.get("wave_key")})
                print(f"[supervisor]  + DONE  {eid} score={final.get('score'):.4f}")
            pending.discard(eid)

    batch.fence()   # ensure this round's Batch DAG is fully drained
    n_done = sum(1 for o in outcomes if o["status"] == "done")
    print(f"[supervisor] round complete: {n_done} finished, "
          f"{len(aborting)} aborted early")
    return {"outcomes": outcomes, "aborted_count": len(aborting)}


# =============================================================================
# Agent 3 -- ANALYZER: rank survivors (zero-copy reads), update incumbent,
# decide convergence, and free spent artifacts.
# =============================================================================

def analyzer_node(state: LabState, *, ddict_ser: bytes) -> dict:
    ddict = _get_ddict(ddict_ser)

    survivors = [o for o in state["outcomes"] if o["status"] == "done"]
    if not survivors:
        # Everything diverged -- widen the search next round.
        print(f"[analyzer] {_where()} no survivors this round")
        it = state["iteration"]
        return {"iteration": it + 1, "history": [state.get("best_score", -1e9)],
                "done": it + 1 >= MAX_ITERATIONS}

    # Rank by the figure of merit; the winner needs its full waveform read back.
    survivors.sort(key=lambda o: o["score"], reverse=True)
    champ = survivors[0]
    champ_wave = ddict[champ["wave_key"]]              # zero-copy bulk read
    rms = float(np.sqrt(np.mean(champ_wave ** 2)))     # a "needs whole array" metric
    best_params = next(c for c in state["candidates"] if c["id"] == champ["id"])

    prev_best = state.get("best_score", float(-np.inf))
    improved = champ["score"] - prev_best

    # User-managed cleanup: keep the champion, free the rest.
    for o in survivors[1:]:
        if o["wave_key"] in ddict:
            del ddict[o["wave_key"]]

    it = state["iteration"]
    converged = (it + 1 >= MAX_ITERATIONS) or (it > 0 and abs(improved) < 1e-3)
    print(f"[analyzer] {_where()} round {it}: "
          f"champion={champ['id']} score={champ['score']:.4f} "
          f"(wave rms={rms:.3f}) -> {'CONVERGED' if converged else 'refine'}")

    return {
        "best_score": max(prev_best, champ["score"]),
        "best_params": best_params,
        "history": [champ["score"]],
        "iteration": it + 1,
        "done": converged,
    }


# =============================================================================
# Routing
# =============================================================================

def route_after_analysis(state: LabState) -> str:
    return END if state["done"] else "planner"


# =============================================================================
# Optional: build a cluster-local LLM proxy (Dragon Inference / vLLM).
# Returns (llm_proxy, inference_handle). The handle is kept alive for the
# campaign and destroyed in main()'s finally block; it is None when the
# rule-based judge is used.
# =============================================================================

def _maybe_build_llm() -> tuple[Any | None, Any | None]:
    if not USE_LLM_JUDGE:
        return None, None

    # Configure a small on-cluster vLLM pipeline. Adjust the model and GPU
    # counts to match your allocation; set HF_TOKEN in the environment for
    # gated models (e.g. Llama).
    config = InferenceConfig(
        model=ModelConfig(
            model_name="meta-llama/Llama-3.1-8B-Instruct",
            hf_token=os.environ.get("HF_TOKEN", ""),
            tp_size=1,               # GPUs per inference worker (tensor parallel)
            max_tokens=64,           # the judge only needs ABORT / CONTINUE
            max_model_len=4096,
        ),
        hardware=HardwareConfig(
            num_nodes=1,             # inference on one node
            num_gpus=1,              # one GPU on that node
        ),
        batching=BatchingConfig(
            enabled=True,
            batch_wait_seconds=0.05,
            max_batch_size=32,
        ),
    )

    # Launch the pipeline and hand the agents a queue-backed proxy.
    input_queue = Queue(maxsize=256)
    inference = Inference(config=config, input_queue=input_queue)
    inference.initialize()
    print("[setup] on-cluster vLLM inference pipeline ready")

    llm = DragonQueueLLMProxy(input_queue, max_concurrent_requests=16)
    return llm, inference


# =============================================================================
# Main
# =============================================================================

def main() -> None:
    from dragon import DDict

    print(f"[orchestrator] {_where()}")

    ddict = DDict(managers_per_node=1, n_nodes=1, total_mem=DDICT_MEM)
    ddict_ser = ddict.serialize()

    llm, inference = _maybe_build_llm()

    # A Batch runtime schedules the experiment tasks across the allocation. The
    # supervisor submits into it; the experiments stream progress via the DDict.
    # It is created with managed_lifecycle=True because the supervisor runs in a
    # *different* process (its agent host) and receives its own Batch client via
    # cloudpickle -- that copy is torn down with the host, so we shut the shared
    # runtime down explicitly from here with a forced grace period.
    batch = Batch(managed_lifecycle=True)
    print(f"[setup] {batch.topology()}")

    # -- Discover the allocation and build one Policy per host ----------------
    # Two hosts are placed on two nodes: the control plane (planner + analyzer)
    # on the head node, and the supervisor -- which submits and monitors the
    # experiment tasks -- on a compute node. If the allocation has only one
    # node, both policies resolve to it (the demo still runs).
    nodes = System().nodes
    head_hostname = Node(nodes[0]).hostname
    compute_hostname = Node(nodes[1 % len(nodes)]).hostname
    head_policy = Policy(placement=Policy.Placement.HOST_NAME,
                         host_name=head_hostname)
    compute_policy = Policy(placement=Policy.Placement.HOST_NAME,
                            host_name=compute_hostname)
    print(f"[setup] head node={head_hostname}, compute node={compute_hostname}")

    try:
        with DragonExecutor() as executor:
            # Host 0 (head node): control-plane agents -- planning and ranking.
            executor.launch_host(
                agents={
                    "planner": partial(planner_node, llm=llm),
                    "analyzer": partial(analyzer_node, ddict_ser=ddict_ser),
                },
                policy=head_policy,
            )
            # Host 1 (compute node): the supervisor, which submits the experiment
            # tasks to Batch and polls the DDict live. It needs extra threads to
            # run its monitoring loop alongside the submitted work.
            executor.launch_host(
                agents={
                    "supervisor": partial(supervisor_node, ddict_ser=ddict_ser,
                                          batch=batch, llm=llm),
                },
                max_threads=8,
                policy=compute_policy,
            )

            # Equivalent one-call form via launch_hosts(): pass a list of agent
            # dicts and a matching list of policies (applied in order). Note that
            # max_threads here would apply to every host, so the two-call form
            # above is used when hosts need different max_threads.
            #
            #   executor.launch_hosts(
            #       hosts=[
            #           {"planner": partial(planner_node, llm=llm),
            #            "analyzer": partial(analyzer_node, ddict_ser=ddict_ser)},
            #           {"supervisor": partial(supervisor_node, ddict_ser=ddict_ser,
            #                                  batch=batch, llm=llm)},
            #       ],
            #       policies=[head_policy, compute_policy],
            #   )

            builder = StateGraph(LabState)
            builder.add_node("planner", executor.node("planner"))
            builder.add_node("supervisor", executor.node("supervisor"))
            builder.add_node("analyzer", executor.node("analyzer"))

            builder.add_edge(START, "planner")
            builder.add_edge("planner", "supervisor")
            builder.add_edge("supervisor", "analyzer")
            builder.add_conditional_edges("analyzer", route_after_analysis,
                                          ["planner", END])

            graph = builder.compile()

            config = {"recursion_limit": 100}
            initial = {"iteration": 0, "outcomes": [], "best_score": float(-np.inf),
                       "history": [], "aborted_count": 0, "done": False}

            print("\n=== Self-driving lab (Dragon + LangGraph) ===")
            final = graph.invoke(initial, config)

            print("\n=== Lab campaign complete ===")
            print(f"  rounds run    : {final['iteration']}")
            print(f"  best params   : {final.get('best_params')}")
            print(f"  best score    : {final['best_score']:.4f}")
            print(f"  score history : {[round(h, 4) for h in final['history']]}")
    finally:
        # Force the shared Batch runtime down after a short grace period: the
        # supervisor's cloudpickled Batch client lived in the (now stopped) agent
        # host and can never detach on its own, so we don't wait on it forever.
        batch.destroy(force_timeout=30)
        if inference is not None:
            inference.destroy()
        ddict.destroy()


if __name__ == "__main__":
    mp.set_start_method("dragon", force=True)
    main()
