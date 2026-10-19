"""Placed summary and cleanup processes; all returned data is small.

Three functions, each run as its own process on the machine that holds the
run's mesh (worker.py places them with Policy(HOST_NAME, run.host)). Each
attaches to the DDicts it needs, does one job, detaches, and exits; nothing
is returned through Batch except the exit code. Results travel as small dicts
written to the campaign DDict.

  summarize_snapshot   mid-run, read-only.  reads mesh/  writes summary/<c>/<run>/t<ts>
  finalize_run         after the solver.     reads mesh/+progress  writes desc/<c>/<run>
                                             deletes every mesh/ and live/ key of the run
  free_only            if finalize_run failed. deletes the same keys, writes nothing
"""

import pickle
import socket
import time

from dragon.data.ddict import DDict

from data import mesh
from science import solver


def _timed_summary(local_ddict, campaign, run_id, ts):
    """Summarize one snapshot and record what the read cost and what will cross back."""
    start = time.perf_counter()
    summary = mesh.summarize_snapshot(local_ddict, campaign, run_id, ts, solver.RANKS)
    summary["read_seconds"] = round(time.perf_counter() - start, 3)
    summary["host"] = socket.gethostname()
    summary["summary_bytes"] = len(pickle.dumps(summary))  # what leaves this machine, vs bytes_read which stays
    return summary


def summarize_snapshot(run_id, campaign, ts, state_ser, mesh_ser, result_key):
    """Read one snapshot without deleting it.

    Reads:  mesh/<c>/<run>/t<ts>/r*/{meta,field} through the local manager
    Writes: campaign DDict[result_key], result_key = summary/<c>/<run>/t<ts>
    The worker pops that key in the next watch visit; it is never read twice.
    """
    # Mid-run inspection: the solver may still be writing later timesteps.
    # Dragon: this process was placed on the data's machine; attach() joins the DDict it already holds.
    mesh_ddict = DDict.attach(mesh_ser)
    campaign_ddict = None
    try:
        campaign_ddict = DDict.attach(state_ser)
        summary = _timed_summary(mesh.local_view(mesh_ddict), campaign, run_id, ts)
        campaign_ddict[result_key] = summary
    finally:
        try:
            if campaign_ddict is not None:
                campaign_ddict.detach()
        finally:
            mesh_ddict.detach()


def finalize_run(run_id, campaign, state_ser, mesh_ser, failure=""):
    """Register final data after the solver and live reader are terminal.

    Reads:   live/<c>/<run>/progress (which ts is the last complete snapshot),
             then mesh/<c>/<run>/t<that ts>/r*/{meta,field} locally
    Writes:  campaign DDict desc/<c>/<run> = summary dict, forced valid=False
             when `failure` is set or the last ts is not NUM_TSTEPS
    Deletes: every mesh/<c>/<run>/* and live/<c>/<run>/* key, so the run's
             hundreds of MB are gone the moment its one number is recorded
    """
    # Dragon: this process was placed on the data's machine; attach() joins the DDict it already holds.
    mesh_ddict = DDict.attach(mesh_ser)
    campaign_ddict = None
    try:
        campaign_ddict = DDict.attach(state_ser)
        routed_ddict = mesh.raw_bytes_view(mesh_ddict)
        local_ddict = mesh.local_view(mesh_ddict)
        try:
            _, ts = mesh.read_progress(routed_ddict, campaign, run_id)
        except ValueError as exc:
            ts = 0
            failure = failure or str(exc)
        summary = _timed_summary(local_ddict, campaign, run_id, ts)
        if failure or ts != solver.NUM_TSTEPS:
            summary.update(valid=False, quantity=None,
                           reason=failure or f"final timestep {ts} != planned {solver.NUM_TSTEPS}")
        mesh.free_run(local_ddict, campaign, run_id, solver.RANKS)
        _free_live(routed_ddict, campaign, run_id)
        # Dragon: one small dict crosses to the campaign DDict; the ~200 MB field stays and is freed here.
        campaign_ddict[f"desc/{mesh.ns(campaign)}{run_id}"] = summary
    finally:
        try:
            if campaign_ddict is not None:
                campaign_ddict.detach()
        finally:
            mesh_ddict.detach()


def _free_live(routed_ddict, campaign, run_id):
    """Delete the run's telemetry and progress keys; these were routed, not local."""
    keys = [f"live/{mesh.ns(campaign)}{run_id}/t{t}" for t in range(1, solver.NUM_TSTEPS + 1)]
    keys.append(f"live/{mesh.ns(campaign)}{run_id}/progress")
    for key in keys:
        try:
            del routed_ddict[key]
        except KeyError:
            pass


def free_only(run_id, campaign, mesh_ser):
    """Delete a terminal run only after all its readers have finished.

    Deletes the same mesh/ and live/ keys as finalize_run; reads and writes
    nothing else. Used only when finalize_run did not confirm completion.
    """
    # Last resort: the final reducer failed, so its data was never freed.
    # Dragon: this process was placed on the data's machine; attach() joins the DDict it already holds.
    mesh_ddict = DDict.attach(mesh_ser)
    try:
        mesh.free_run(mesh.local_view(mesh_ddict), campaign, run_id, solver.RANKS)
        _free_live(mesh.raw_bytes_view(mesh_ddict), campaign, run_id)
    finally:
        mesh_ddict.detach()
