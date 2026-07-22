"""Runner tools — launch / monitor / collect parallel simulations."""

from __future__ import annotations

import json
from typing import Any

from dragon.data.ddict import DDict
from dragon.infrastructure.policy import Policy
from dragon.native.machine import Node, System
from dragon.native.process import ProcessTemplate
from dragon.native.process_group import ProcessGroup

from .worker import monte_carlo_worker

# ---------------------------------------------------------------------------
# Experiment state — shared between the three runner tools.
# Lives in the runner_agent's process address space.
# ---------------------------------------------------------------------------

_experiment_state: dict[str, Any] = {
    "pg": None,
    "exp_ddict": None,
    "sample_sizes": [],
    "launched": False,
    "nodes_used": [],
}


def launch_experiment(sample_sizes: list, seeds: list) -> dict:
    """Launch all Monte Carlo simulations in parallel using Dragon ProcessGroup.

    Creates a ProcessGroup with one process per sample size, distributed
    round-robin across compute nodes.  Each worker writes its progress to
    a shared DDict so you can monitor status via ``check_progress``.

    A human operator must approve this call before it executes.

    :param sample_sizes: List of sample sizes (e.g. [100, 1000, 10000]).
    :param seeds: List of random seeds, one per sample size.
    :returns: Dict with launch status, process count, node assignments.
    """
    global _experiment_state

    if _experiment_state["launched"]:
        return {
            "error": "Experiment already launched. "
                     "Call check_progress or collect_results.",
            "sample_sizes": _experiment_state["sample_sizes"],
        }

    if len(seeds) != len(sample_sizes):
        return {
            "error": f"seeds length ({len(seeds)}) must match "
                     f"sample_sizes length ({len(sample_sizes)})."
        }

    # --- Discover cluster topology ---
    my_alloc = System()
    node_list = my_alloc.nodes
    nnodes = my_alloc.nnodes
    hostnames = [Node(nid).hostname for nid in node_list]

    print(f"[runner] Cluster: {nnodes} node(s) -- {hostnames}", flush=True)

    # --- Create a dedicated DDict for experiment progress tracking ---
    managers = min(nnodes, len(sample_sizes))
    exp_ddict = DDict(managers_per_node=1, n_nodes=managers)
    serialized = exp_ddict.serialize()

    for n in sample_sizes:
        exp_ddict[f"experiment:sample:{n}:status"] = "pending"

    # --- Build ProcessGroup with per-node placement ---
    pg = ProcessGroup(restart=False)

    node_assignments: list[dict] = []
    for i, (n_samples, seed) in enumerate(zip(sample_sizes, seeds)):
        target_host = hostnames[i % nnodes]
        local_policy = Policy(
            placement=Policy.Placement.HOST_NAME,
            host_name=target_host,
        )
        pg.add_process(
            nproc=1,
            template=ProcessTemplate(
                target=monte_carlo_worker,
                args=(n_samples, seed, serialized),
                policy=local_policy,
            ),
        )
        node_assignments.append({
            "n_samples": n_samples,
            "seed": seed,
            "node": target_host,
        })

    # --- Launch! ---
    pg.init()
    pg.start()

    _experiment_state = {
        "pg": pg,
        "exp_ddict": exp_ddict,
        "sample_sizes": list(sample_sizes),
        "launched": True,
        "nodes_used": list(set(h["node"] for h in node_assignments)),
    }

    result = {
        "status": "launched",
        "n_processes": len(sample_sizes),
        "sample_sizes": sample_sizes,
        "node_assignments": node_assignments,
        "nodes_used": _experiment_state["nodes_used"],
    }
    print(
        f"[runner] launch_experiment -> {len(sample_sizes)} processes "
        f"launched across {len(_experiment_state['nodes_used'])} node(s)",
        flush=True,
    )
    return result


def check_progress() -> dict:
    """Check the progress of the running experiment.

    Reads the experiment DDict to report which simulations are
    pending, running, done, or in error.

    :returns: Dict with counts, all_done flag, and per-sample details.
    """
    global _experiment_state

    if not _experiment_state["launched"]:
        return {
            "error": "No experiment launched yet. "
                     "Call launch_experiment first."
        }

    exp_ddict = _experiment_state["exp_ddict"]
    sample_sizes = _experiment_state["sample_sizes"]

    details: list[dict] = []
    counts = {"done": 0, "running": 0, "pending": 0, "error": 0}

    for n in sample_sizes:
        prefix = f"experiment:sample:{n}"
        status = str(exp_ddict[f"{prefix}:status"])
        entry: dict[str, Any] = {"n_samples": n, "status": status}

        try:
            entry["hostname"] = str(exp_ddict[f"{prefix}:hostname"])
        except (KeyError, Exception):
            entry["hostname"] = "—"

        # Note: result data (pi_estimate, absolute_error, wall_time_s)
        # is intentionally NOT included here.  Exposing results in
        # check_progress causes the LLM to skip collect_results because
        # it thinks it already has the data.  Use collect_results to
        # retrieve final results.

        counts[status] = counts.get(status, 0) + 1
        details.append(entry)

    total = len(sample_sizes)
    all_done = counts["done"] + counts["error"] == total

    result = {
        "total": total,
        **counts,
        "all_done": all_done,
        "details": details,
    }

    status_line = (
        f"done={counts['done']}/{total}, "
        f"running={counts['running']}, "
        f"pending={counts['pending']}, "
        f"error={counts['error']}"
    )
    print(f"[runner] check_progress -> {status_line}", flush=True)
    return result


def collect_results() -> dict:
    """Collect final results after all simulations complete.

    Joins the ProcessGroup, reads all results from DDict, cleans up.
    Only call after check_progress reports all_done=true.

    :returns: Dict with status and list of result dicts.
    """
    global _experiment_state

    if not _experiment_state["launched"]:
        return {"error": "No experiment launched yet."}

    pg = _experiment_state["pg"]
    exp_ddict = _experiment_state["exp_ddict"]
    sample_sizes = _experiment_state["sample_sizes"]

    try:
        pg.join(timeout=14400)  # 4 hour max
    except Exception as exc:
        print(f"[runner] collect_results join error: {exc}", flush=True)

    results: list[dict] = []
    for n in sample_sizes:
        prefix = f"experiment:sample:{n}"
        status = str(exp_ddict[f"{prefix}:status"])
        if status == "done":
            try:
                result_str = str(exp_ddict[f"{prefix}:result"])
                results.append(json.loads(result_str))
            except Exception as exc:
                results.append({
                    "n_samples": n, "status": "error",
                    "error": f"Failed to read result: {exc}",
                })
        else:
            results.append({
                "n_samples": n, "status": status,
                "error": f"Worker did not complete (status={status}).",
            })

    try:
        pg.close()
    except Exception as exc:
        print(f"[runner] WARNING: ProcessGroup close failed: {exc}", flush=True)
    try:
        exp_ddict.destroy()
    except Exception as exc:
        print(f"[runner] WARNING: experiment DDict destroy failed: {exc}", flush=True)

    _experiment_state.update({
        "pg": None, "exp_ddict": None, "sample_sizes": [],
        "launched": False, "nodes_used": [],
    })

    print(f"[runner] collect_results -> {len(results)} results collected", flush=True)
    return {
        "status": "complete",
        "n_results": len(results),
        "results": results,
    }


def cleanup_experiment_state():
    """Safety-net cleanup called from main() finally block."""
    if _experiment_state.get("pg"):
        try:
            _experiment_state["pg"].stop()
            _experiment_state["pg"].close()
        except Exception as exc:
            print(f"[runner] WARNING: cleanup ProcessGroup failed: {exc}", flush=True)
    if _experiment_state.get("exp_ddict"):
        try:
            _experiment_state["exp_ddict"].destroy()
        except Exception as exc:
            print(f"[runner] WARNING: cleanup experiment DDict failed: {exc}", flush=True)
