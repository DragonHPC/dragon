"""Eight graph nodes: learn from MPI runs without moving their mesh through the graph."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import time
import uuid
from functools import partial
from typing import Any, TypedDict

import dragon
import multiprocessing as mp

from dragon.ai.langgraph import DragonExecutor, get_llm
from dragon.data.ddict import DDict
from dragon.infrastructure.policy import Policy
from dragon.native.machine import Node
from dragon.workflows.batch import Batch
from langgraph.graph import END, START, StateGraph

import settings
from execution import clients, steering, worker
from execution.launch import Resources
from llm import prompts, tools
from science import sampling, solver

DEFAULT_BRIEF = (f"Sweep object speed and size; use at most {settings.DEFAULT_BUDGET} runs and "
                 "target ten percent held-out block error.")


class Campaign(TypedDict):
    """Small decisions and messages. Jobs, model weights and mesh stay elsewhere."""


    brief: dict[str, Any] # the brief settings for the campaign
    wave: int # the current wave number in the campaign
    sims_run: int # the number of simulations that have been run so far
    selected_inputs: list[dict[str, Any]] # the inputs selected for the current wave
    recent_runs: list[str] # the IDs of the most recent simulation runs
    steer_region: list[list[float]] # the region of the parameter space being steered
    model_info: dict[str, Any] | None # information about the current surrogate model
    pending: bool # any solver, reader, reducer, cleanup, or fit still outstanding on the worker
    report: dict[str, Any] # the report generated from the most recent wave
    published: dict[str, Any] # the published results from the campaign


# ---- printing helpers used by the nodes and the summary --------------------

def print_promotion(model, brief, history):
    """One boxed block per promoted model, so the fit result stands out in the log."""
    error, target = model["validate_error"], brief["target_fraction"]
    met = error <= target and model["n_total"] >= settings.MIN_POINTS_TO_STOP
    bar = "=" * 72
    print(f"\n{bar}\n[train] SURROGATE PROMOTED  {model['version']}  (fit submitted in wave {model['launch_wave']})\n"
          f"  labelled runs     {model['n_train']} train + {model['n_validate']} held-out = {model['n_total']} total\n"
          f"  held-out error    {error:.4f} of the per-rank block cap ({solver.MAX_BLOCKS})  "
          f"= off by ~{error * solver.MAX_BLOCKS:.0f} blocks on average; labels are 8-rank totals, so this can exceed 1\n"
          f"  target            error <= {target:.4f} and >= {settings.MIN_POINTS_TO_STOP} labelled runs  "
          f"-> {'MET' if met else 'not met'}\n"
          f"  error history     {[round(e, 3) for e in history]}\n{bar}\n", flush=True)


def stop_reason(state):
    """Why no more waves may launch. Valid only once launch_allowed is False."""
    budget = f"{state['sims_run']}/{state['brief']['sim_budget']}"
    if sampling.target_met(state):
        model = state["model_info"]
        reason = (f"target met (held-out error {model['validate_error']:.4f} <= "
                  f"{state['brief']['target_fraction']:.4f} on {model['n_total']} labelled runs)")
        if state["sims_run"] >= state["brief"]["sim_budget"]:
            reason += f", budget also spent ({budget})"
        return reason
    if state["sims_run"] >= state["brief"]["sim_budget"]:
        return f"budget spent ({budget})"
    return f"wave limit ({state['wave']}/{settings.MAX_WAVES})"


# ---- the eight graph nodes, in graph order ---------------------------------

async def brief_node(state, *, brief_text, inference_queue=None):
    """Turn the request into bounded settings; no LLM uses the defaults."""
    brief = settings.default_brief()
    source = "defaults"
    # If an inference queue is provided, attempt to get settings from the LLM. Otherwise, use defaults.
    if inference_queue is not None:
        start = time.perf_counter()
        try:
            # get_llm: create a DragonQueueLLMProxy from host agent process if one does not already exist for the given inference queue.
            # If the host agent process already has a DragonQueueLLMProxy for the given inference queue, it will be reused.
            text = await get_llm(inference_queue).chat(  # Dragon: the LLM is vLLM inside this allocation; no API key, no data leaves the cluster
                messages=[{"role": "system", "content": prompts.BRIEF_SYSTEM},
                          {"role": "user", "content": json.dumps({"request": brief_text, "defaults": brief})}],
                json_schema=prompts.BRIEF_SCHEMA)
            brief = settings.validate_brief(json.loads(text))
            source = f"llm inside the allocation, {time.perf_counter() - start:.1f}s"
        except Exception as exc:
            print(f"[brief] using defaults: {exc}", flush=True)
    print(f"[brief] settings={brief} source={source}", flush=True)
    return {"brief": brief}


def propose_node(state, *, ddict_ser, seed):
    """
    Random until a model exists, then where the ensemble members disagree.

    If no model exists, randomly select 4 inputs.
    If model exists, randomly select 400 candidates and pick 3 of them where the
    ensemble members disagree. Pick 1 at random from the remaining candidates.

    This function implements a simple active learning strategy for selecting the
    next set of inputs to simulate.

    """
    wave = state["wave"] + 1
    budget_left = max(0, state["brief"]["sim_budget"] - state["sims_run"])
    model = clients.active_surrogate(ddict_ser)  # Dragon: the promoted ensemble (all three members in one dict) from the campaign DDict, cached on this host until its version changes
    # choose candidate inputs for the next simulation wave
    chosen = sampling.next_dials(model, state["brief"]["bounds"], state["steer_region"],
                                 budget_left, seed + wave)
    print(f"[propose] wave={wave} selected={len(chosen)} "
          f"model={model['version'] if model else 'random'} "
          f"region={'steered by analyze' if state['steer_region'] else 'full bounds'}", flush=True)
    return {"wave": wave,
            "selected_inputs": [{"id": f"w{wave}-p{i}", "dials": d} for i, d in enumerate(chosen)]}


def simulate_node(state, *, connection, bin_path, hostnames):
    """
    Submit placed MPI jobs and return immediately, keeping handles on this host.

    Each selected inputs by the propose node are submitted as separate MPI jobs to the available hosts.
    Each MPI job has 8 MPI ranks and is placed on a specific host according to the round-robin scheduling policy.
    """
    runtime = worker.runtime(**connection)
    if state["selected_inputs"]:
        print(f"\n{'=' * 20} launch wave {state['wave']} (not a barrier) {'=' * 20}", flush=True)
        print(f"[simulate] still running from earlier waves: {runtime.in_flight()}", flush=True)
    for index, item in enumerate(state["selected_inputs"]):
        runtime.check_submission(item["id"])
        host = hostnames[(state["sims_run"] + index) % len(hostnames)]  # round-robin over the whole allocation
        template = clients.simulation_template(item, host, bin_path, runtime.mesh_ser, runtime.campaign)  # Dragon: ProcessTemplate + Policy pins the job; env vars point the C hook at the mesh DDict
        task = runtime.batch.job([(solver.RANKS, template)])  # Dragon: 8 MPI ranks on that one machine; returns at once, so the next wave can launch
        runtime.add_run(item, host, state["wave"], task)
    return {"sims_run": state["sims_run"] + len(state["selected_inputs"]),
            "selected_inputs": [], "pending": runtime.pending}


def watch_node(state, *, connection):
    """Look at running solvers for a short window and stop the bad ones.

    Every run here is still computing. The reader this node launches opens a
    mid-run snapshot, read-only, so the rules have numbers to judge. That
    summary is for steering only; it is never a training label.

    Returns once WATCH_UNTIL_READY runs have something to look at, or after
    MAX_MONITOR_PASSES. The others keep running into the next wave.
    """
    runtime = worker.runtime(**connection)
    for _ in range(steering.MAX_MONITOR_PASSES):
        running = runtime.running()
        ready = 0
        for run_id, run in running:
            steering.poll_summary(runtime, run_id, run)              # Dragon: the placed reader's ~10 numbers, read back from the campaign DDict
            status, _ = run.work.poll(f"solver {run_id}", node="watch")  # Dragon: Task.get(block=False) answers "done yet?" without waiting on the job
            if status != "pending":
                ready += 1
                continue
            snap = steering.read_live(runtime, run_id, run)          # Dragon: DDict reads of what the C hook wrote while its ranks still compute
            if steering.needs_reader(run, snap):
                runtime.start_live_reader(run_id, run, snap)        # Dragon: Batch.process pinned to run.host; the ~200 MB field never moves
            reason = steering.stop_reason(run, state)
            if reason:
                run.work.request_cancel(f"{run_id}: {reason}")      # Dragon: Task.cancel stops all 8 ranks mid-run; termination is confirmed in collect
            ready += run.summary is not None
        if runtime.pending:
            time.sleep(steering.MONITOR_POLL)                        # a pause, not a barrier: fits and later waves keep running
        if not running or ready >= min(settings.WATCH_UNTIL_READY, len(running)):  # enough to act on; the rest keep running
            break
    return {"pending": runtime.pending}


def collect_node(state, *, connection):
    """Turn finished solvers into labels. One non-blocking step per run per visit.

    A run moves solver -> reduce -> cleanup (only if the reducer failed). Each
    visit checks every run and advances it at most one stage:
      solver finished   -> place the final reducer on the run's machine
      reducer finished  -> its one number becomes a training label; the mesh is freed
      reducer failed    -> place a cleanup; forget the run only once it confirms
    Nothing here waits. A run that is not ready is picked up next visit, so
    one slow solver never holds the wave. Training is on the cumulative label
    set, not on a wave: see sampling.training_due.
    """
    runtime = worker.runtime(**connection)
    recent = []
    for run_id, run in list(runtime.runs.items()):
        if run.stage == "solver":
            runtime.finish_solver(run_id, run, state["wave"])       # Dragon: Batch.process pinned to run.host reads the final snapshot in place
        elif run.stage == "reduce":
            if runtime.finish_reducer(run_id, run):                 # Dragon: the reducer's small summary crosses the DDict and becomes a label
                recent.append(run_id)
        elif run.stage == "cleanup":
            runtime.finish_cleanup(run_id, run)                     # Dragon: nothing is forgotten until the placed cleanup confirms the mesh is gone
    return {"recent_runs": recent, "pending": runtime.pending}


async def analyze_node(state, *, ddict_ser, campaign="", inference_queue=None):
    """Use bounded LLM tools on small summaries; evidence stays in the log."""
    if inference_queue is None or not state["recent_runs"]:
        return {"steer_region": []}
    if not sampling.launch_allowed(state):
        print("[analyze] skipped: no more launches, so a steered region would not be used", flush=True)
        return {"steer_region": []}
    # ReAct pattern: the LLM analyzes recent runs and decides on the next steering actions
    result = await tools.analyze(get_llm(inference_queue), clients.handle(ddict_ser), campaign,  # Dragon: LLM inside the allocation; tools read small summaries from the DDict
                                 state["brief"]["bounds"], state["recent_runs"])
    return {"steer_region": result["steer_region"]}


def train_node(state, *, connection, seed):
    """Promote finished fits, then start another frozen fit when enough data arrived."""
    runtime = worker.runtime(**connection)
    promoted = runtime.finish_training()                            # Dragon: collect the three Batch.function results; promote into the campaign DDict
    if promoted:
        print_promotion(promoted, state["brief"], runtime.error_history)
    model_info = promoted or state["model_info"]
    updated = {**state, "model_info": model_info}
    if runtime.training is None and sampling.training_due(
            updated, len(runtime.campaign_ddict[settings.K_TRAIN]), runtime.last_fit_count):
        runtime.start_training(seed, state["wave"])                 # Dragon: three Python fits as Batch tasks, wherever there is room, while the next wave launches
    out = {"model_info": model_info, "pending": runtime.pending}
    draining = not sampling.launch_allowed(updated)
    if draining and not runtime.drain_announced:
        runtime.drain_announced = True
        print(f"\n{'=' * 20} no more launches: {stop_reason(updated)} {'=' * 20}\n"
              f"[drain] still running: {runtime.in_flight()}\n"
              f"[drain] the graph now cycles watch -> collect -> analyze -> train until all of it has ended, then publishes",
              flush=True)
    runtime.check_drain(draining)
    if next_step({**updated, **out}) == "publish":
        out["report"] = runtime.finish()                            # Dragon: the worker's Batch client joins; its counts cross to the thinker as plain data
    return out


def next_step(state):
    """The one routing decision, taken after every train visit.

    1. May another wave launch?  -> "propose"
       Yes while all three hold: runs submitted < budget, wave < MAX_WAVES, and
       the promoted model has not met the error target. This is checked FIRST
       and does not look at what is still running: earlier waves keep computing
       while the new one launches. That is the in-flight overlap.

    2. Only once launching has stopped: is anything still outstanding?  -> "watch"
       `pending` covers every solver, live reader, final reducer, cleanup, and
       model fit the worker has not seen finish. This is the end-of-campaign
       drain, not a per-wave barrier; it skips propose and simulate and cycles
       watch -> collect -> analyze -> train until pending is False.

    3. Otherwise  -> "publish"

    Reads only the small graph state, never the worker's records, so the same
    state always gives the same answer. The rules live in science.sampling.
    """
    if sampling.launch_allowed(state):
        return "propose"
    return "watch" if state["pending"] else "publish"


def publish_node(state, *, ddict_ser, out_dir):
    """Save measured model facts after the worker confirms completion."""
    if state["pending"] or not state["report"].get("worker_joined"):
        raise clients.UnsafeTermination("publication requires a finished worker report")
    try:
        model = clients.handle(ddict_ser)[settings.K_MODEL]        # Dragon: DDict read on the thinker; the worker wrote it from another machine
        report = {**state["report"], "warm_reuses": clients.warm_reuse_count(ddict_ser)}
        if model is None:
            print("[publish] no valid model to save", flush=True)
            return {"published": {}, "report": report}
        from science.surrogate import predict, save_surrogate

        facts = {key: model[key] for key in (
            "version", "launch_wave", "dataset_ids", "n_train", "n_validate", "n_total", "validate_error")}
        card = "miniAMR synthetic refinement footprint surrogate\n" + json.dumps(facts, indent=2, allow_nan=False)
        card += "\nHeld-out block error as a fraction of the block cap; not physical solution error or proven scaling."
        paths = save_surrogate(out_dir, model, card)
        queries = sampling.random_dials(settings.DEMAND_QUERIES, state["brief"]["bounds"], 99)
        start = time.perf_counter()
        predict(model, queries)
        seconds = time.perf_counter() - start
        print(f"[publish] saved model={model['version']} {sampling.fit_counts(model['n_total'])} "
              f"held-out block error={model['validate_error']:.4f} of the per-rank block cap", flush=True)
        return {"published": {"model": str(paths[0]), "card": str(paths[1]), "seconds": seconds,
                              "queries": len(queries)}, "report": report}
    finally:
        clients.detach_all(ddict_ser)


def build_graph(executor):
    graph = StateGraph(Campaign)
    for name in ("brief", "propose", "simulate", "watch", "collect", "analyze", "train", "publish"):
        graph.add_node(name, executor.node(name))
    for source, target in ((START, "brief"), ("brief", "propose"), ("propose", "simulate"),
                           ("simulate", "watch"), ("watch", "collect"),
                           ("collect", "analyze"), ("analyze", "train"), ("publish", END)):
        graph.add_edge(source, target)
    graph.add_conditional_edges("train", next_step, {"propose": "propose", "watch": "watch", "publish": "publish"})
    return graph.compile()


def new_campaign():
    return Campaign(brief=settings.default_brief(), wave=0, sims_run=0, selected_inputs=[],
                    recent_runs=[], steer_region=[], model_info=None, pending=False, report={}, published={})


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--llm", default=None)
    parser.add_argument("--gpus", type=int, default=1)
    parser.add_argument("--max-tokens", type=int, default=512)
    parser.add_argument("--temperature", type=float, default=0.1)
    parser.add_argument("--brief", default=DEFAULT_BRIEF)
    parser.add_argument("--bin", default=solver.MINIAMR_BIN)
    parser.add_argument("--campaign-id", default="")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--out", default=os.path.join(os.path.dirname(__file__), "artifacts"))
    return parser.parse_args()


async def main(args):
    campaign_id = args.campaign_id or uuid.uuid4().hex[:8]
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,32}", campaign_id):
        raise ValueError("campaign ID must be 1-32 ASCII letters, digits, underscores or hyphens")
    hosts = list(dict.fromkeys(clients.allocation_hostnames()))  # Dragon: the machines the scheduler gave us, no $SLURM_NODELIST parsing
    if len(hosts) < 2 or settings.JOB_NODES != 1:
        raise RuntimeError("this example requires two distinct allocation hosts and one-node MPI jobs")
    with Resources() as resources:
        state_ddict = resources.state_ddict = DDict(1, len(hosts), settings.DDICT_MEM_PER_NODE * len(hosts))  # Dragon: one DDict both AgentHosts read and write; small objects only
        mesh_ddict = resources.mesh_ddict = DDict(1, len(hosts), settings.MESH_MEM_PER_NODE * len(hosts))  # Dragon: one manager per machine; each rank's field stays on its own
        for key, value in ((settings.K_TRAIN, []), (settings.K_MODEL, None), (settings.K_VERSION, "")):
            state_ddict[key] = value
        state_ser, mesh_ser = state_ddict.serialize(), mesh_ddict.serialize()  # Dragon: descriptors are what other processes attach with
        batch = resources.batch = Batch(managed_lifecycle=True)  # Dragon: one runtime for MPI jobs, placed processes and Python tasks; this handle owns destroy
        # Dragon: Sending this to the worker host registers a second Batch client there.
        connection = dict(batch=batch, ddict_ser=state_ser, mesh_ser=mesh_ser, campaign=campaign_id)
        if args.llm:
            resources.start_inference(args)  # Dragon: vLLM as Dragon processes in the allocation, reached through one Queue
            print(f"[startup] LLM {args.llm} running as Dragon processes on "
                  f"{sorted(resources.inference.cpu_and_device_proc_by_hostname)}; no endpoint outside this allocation", flush=True)
        resources.safe = False
        with DragonExecutor() as executor:
            thinkers = {
                "brief": partial(brief_node, brief_text=args.brief, inference_queue=resources.queue),
                "propose": partial(propose_node, ddict_ser=state_ser, seed=args.seed),
                "analyze": partial(analyze_node, ddict_ser=state_ser,
                                   campaign=campaign_id, inference_queue=resources.queue),
                "publish": partial(publish_node, ddict_ser=state_ser, out_dir=args.out),
            }
            workers = {
                "simulate": partial(simulate_node, connection=connection, bin_path=args.bin, hostnames=hosts),
                "watch": partial(watch_node, connection=connection),
                "collect": partial(collect_node, connection=connection),
                "train": partial(train_node, connection=connection, seed=args.seed),
            }
            policies = [Policy(placement=Policy.Placement.HOST_NAME, host_name=host) for host in hosts[:2]]  # Dragon: a request for a machine, not a guarantee
            placed_hosts = executor.launch_hosts([thinkers, workers], policies=policies)  # Dragon: one long-lived process per dict; your node functions run inside it
            print(f"[startup] campaign={campaign_id} seed={args.seed}; eight graph nodes, two AgentHosts", flush=True)
            print(f"[startup] allocation: {hosts}; MPI jobs round-robin over all of them", flush=True)
            for label, process, expected in zip(("thinker", "worker"), placed_hosts, hosts[:2]):
                actual = Node(process.h_uid).hostname  # Dragon: where it actually landed; checked, not assumed
                print(f"[startup] {label}: {actual} puid={process.puid}", flush=True)
                if actual != expected:
                    raise RuntimeError(f"{label} placement {actual} != requested {expected}")
            final = await build_graph(executor).ainvoke(new_campaign(), config={  # Dragon: plain LangGraph ainvoke; each node call is shipped to its AgentHost
                # Bounded: every wave plus every drain pass, eight nodes each.
                "recursion_limit": (settings.MAX_WAVES + settings.DRAIN_WAVES + 2) * 8})
        if final["pending"] or not final["report"].get("worker_joined"):
            raise clients.UnsafeTermination("missing final worker acknowledgement")
        resources.safe = True
        report = final["report"]
        print(f"[summary] stopped: {stop_reason(final)}; runs={final['sims_run']} invalid={report['invalid_runs']} "
              f"confirmed cancellations={report['runs_killed']} training points={report['n_training_points']} "
              f"held-out error history={[round(e, 3) for e in report['error_history']]}", flush=True)
        print(f"[summary] mesh bytes read where they were written={report['mesh_bytes_read']} "
              f"(same_node={report['same_node_reads']} other_node={report['cross_node_reads']}); "
              f"bytes crossed to the campaign DDict={report['summary_bytes_crossed']}", flush=True)
        return final


if __name__ == "__main__":
    mp.set_start_method("dragon", force=True)  # an import above may already have set a context
    asyncio.run(main(parse_args()))
