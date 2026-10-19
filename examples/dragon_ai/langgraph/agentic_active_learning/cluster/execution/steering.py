"""Reading a running job, and the fixed rules for stopping one.

The watch loop itself lives in campaign.watch_node so the Dragon calls it makes
are visible from the graph. This module holds what that loop calls: how to read
the live data, when to send a reader, and which rules name a run to stop.
"""

import time

import settings
from data import mesh
from execution.clients import exit_ok
from science import solver
from science.sampling import target_met

MAX_MONITOR_PASSES = 8   # passes per visit; the wave is not waited out
MONITOR_POLL = 1.0       # seconds between passes, so watching does not spin


def assess(latest, summary=None):
    """Pure rules: inspect numbers, return a reason, never touch a task."""
    if summary and summary.get("interior_finite") is False:
        return "nonfinite interior"
    if not latest:
        return ""
    if latest["checksum_finite"] is False:
        return "nonfinite checksum"
    cap = int(0.9 * solver.MAX_BLOCKS)
    if latest["max_local_blocks"] >= cap:  # miniAMR would abort at the cap anyway
        return f"per-rank blocks {latest['max_local_blocks']} >= {cap} at t{latest['timestep']}"
    return ""


def poll_summary(worker, run_id, run):
    """If the run's live reader has returned, take its summary and print it.

    Pops campaign DDict summary/<c>/<run>/t<ts>; the key is gone after this.
    The dict becomes run.summary, which assess() checks for interior_finite.
    """
    reader = run.reader
    if reader is None:
        return
    status, value = reader.work.poll(f"summary {run_id}", timeout=settings.TASK_TIMEOUT, node="watch")
    if status == "pending":
        return
    # Dragon: the placed reader wrote ~10 numbers here from another machine; pop them as a plain dict.
    summary = worker.campaign_ddict.pop(reader.key, None)
    if (status != "done" or not exit_ok(value) or not isinstance(summary, dict) or
            summary.get("run") != run_id or summary.get("timestep") != reader.timestep):
        summary = {"interior_finite": None, "valid": False,
                   "reason": f"missing/invalid summary; task={status} result={value}"}
    run.summary, run.reader = summary, None
    worker.progress = True
    print(f"[watch] {run_id} t{reader.timestep} snapshot: valid={summary.get('valid', False)} "
          f"ranks={summary.get('ranks_read', 0)}/{solver.RANKS} read {summary.get('bytes_read', 0)} bytes "
          f"on {summary.get('host', 'unknown')} in {summary.get('read_seconds', 0.0):.2f}s; "
          f"{summary.get('summary_bytes', 0)} bytes crossed back reason={summary.get('reason', '')}", flush=True)


def log_missing(run, message):
    if message not in run.missing_logs:
        run.missing_logs.add(message)
        print(f"[watch] {message}", flush=True)


def read_live(worker, run_id, run):
    """Refresh run.latest from what the solver has published. Returns the newest complete snapshot.

    Reads two routed mesh DDict keys, both written by rank 0:
    live/<c>/<run>/progress and live/<c>/<run>/t<ts>. Never a mesh/ key.
    """
    try:
        # Dragon: rank 0 wrote this 8-byte progress record mid-run; we read it while the ranks still compute.
        latest, snap = mesh.read_progress(worker.mesh_ddict, worker.campaign, run_id)
    except ValueError as exc:
        log_missing(run, f"{run_id}: invalid progress: {exc}")
        latest = snap = 0
    if not latest and not snap:
        log_missing(run, f"{run_id}: no published progress yet; field health unknown")
    # The newest record, or the last scheduled one if the newest step has none.
    for ts in [t for t in (latest, latest - latest % solver.LIVE_FREQ) if t > 0]:
        row = mesh.read_telemetry(worker.mesh_ddict, worker.campaign, run_id, ts)
        if row is not None:
            if run.latest is None or row["timestep"] > run.latest["timestep"]:
                print(f"[watch] {run_id} t{row['timestep']} live: blocks/rank={row['max_local_blocks']} "
                      f"global={row['global_blocks']} wall={row['wall_seconds']:.1f}s; "
                      f"read from the mesh DDict while {solver.RANKS} ranks compute", flush=True)
            run.latest = row
            break
        log_missing(run, f"{run_id} t{ts}: missing/invalid telemetry")
    return snap


def needs_reader(run, snap):
    """One live inspection per run; the final timestep belongs to the reducer."""
    return (0 < snap < solver.NUM_TSTEPS and run.reader is None and
            run.summary is None and run.work.cancel_at is None)


def stop_reason(run, state):
    """The fixed rules plus the two campaign-level ones. Empty string means keep going."""
    reason = assess(run.latest, run.summary)
    if time.monotonic() - run.work.started >= settings.RUN_TIMEOUT:
        reason = "submission timeout (includes queue time)"
    if target_met(state):
        reason = "campaign target met"
    return reason
