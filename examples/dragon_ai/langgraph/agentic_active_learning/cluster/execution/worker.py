"""The one owner of every running job, reader, and fit on the worker host.

One serial graph uses each worker. Batch tasks run concurrently, but graph
nodes never touch these records at the same time. Nothing here is ever put
into a graph message; the graph gets small facts back, not handles.
"""

from __future__ import annotations

import copy
import math
import socket
import time
import uuid
from dataclasses import dataclass, field
from typing import Literal

import settings
from data import mesh, reduce_worker
from dragon.infrastructure.policy import Policy
from dragon.native.process import ProcessTemplate
from execution import clients
from execution.clients import TrackedTask, UnsafeTermination, exit_ok
from science import solver
from science.sampling import fit_counts

_workers = {}


@dataclass
class LiveReader:
    work: TrackedTask
    key: str
    timestep: int


@dataclass
class Run:
    dials: list[float]
    host: str
    launch_wave: int
    work: TrackedTask
    stage: Literal["solver", "reduce", "cleanup"] = "solver"
    reader: LiveReader | None = None
    summary: dict | None = None
    latest: dict | None = None
    missing_logs: set[str] = field(default_factory=set)
    failure: str = ""


@dataclass
class TrainingRound:
    version: str
    snapshot_key: str
    wave: int
    count: int
    tasks: dict[int, TrackedTask] = field(default_factory=dict)
    results: dict[int, dict] = field(default_factory=dict)


def runtime(*, batch, ddict_ser, mesh_ser, campaign):
    """One runtime per worker process, so the Batch handle registers one client."""
    key = (campaign, ddict_ser, mesh_ser)
    if key not in _workers:
        _workers[key] = WorkerRuntime(batch, ddict_ser, mesh_ser, campaign)
    return _workers[key]


class WorkerRuntime:
    """One owner for runs, one current fit, and one Batch client on this host."""

    def __init__(self, batch, ddict_ser, mesh_ser, campaign):
        self.batch = batch
        self.ddict_ser, self.mesh_ser, self.campaign = ddict_ser, mesh_ser, campaign
        # Dragon: attached once, here; both hosts then read and write one campaign state without a database.
        self.campaign_ddict = clients.handle(ddict_ser)
        # Dragon: the C side wrote raw bytes; this view reads them back without a Python pickle layer.
        self.mesh_ddict = mesh.raw_bytes_view(clients.handle(mesh_ser))
        self.runs: dict[str, Run] = {}
        self.training: TrainingRound | None = None
        self.last_fit_count = self.drain_rounds = 0
        self.progress = self.closed = self.drain_announced = False
        self.metrics = {name: 0 for name in (
            "invalid_runs", "runs_killed", "mesh_bytes_read", "summary_bytes_crossed",
            "same_node_reads", "cross_node_reads")}
        self.error_history = []
        self.report = None

    @property
    def pending(self):
        return bool(self.runs) or self.training is not None

    def running(self):
        """Runs whose solver has not yet been seen to finish."""
        return [(run_id, run) for run_id, run in self.runs.items() if run.stage == "solver"]

    def in_flight(self):
        """What is still running on the cluster right now, as one printable line."""
        stages = {"solver": 0, "reduce": 0, "cleanup": 0}
        readers = 0
        for run in self.runs.values():
            stages[run.stage] += 1
            readers += run.reader is not None
        fits = len(self.training.tasks) if self.training else 0
        return (f"solvers={stages['solver']} live_readers={readers} reducers={stages['reduce']} "
                f"cleanups={stages['cleanup']} fits={fits}")

    def check_submission(self, run_id=None):
        if self.closed:
            raise RuntimeError("worker client already joined; no more submissions")
        if run_id is not None and run_id in self.runs:
            raise ValueError(f"duplicate active run: {run_id}")

    def add_run(self, item, host, wave, task):
        self.check_submission(item["id"])
        self.runs[item["id"]] = Run(list(item["dials"]), host, wave, TrackedTask(task))
        self.progress = True
        print(f"[simulate] {item['id']}: submitted {solver.RANKS} ranks on {host}", flush=True)

    def start_live_reader(self, run_id, run, timestep):
        """One placed reader per run, on the machine that holds the snapshot.

        The reader writes its result to campaign DDict summary/<c>/<run>/t<ts>;
        steering.poll_summary pops it. Nothing here reads the mesh DDict.
        """
        key = f"summary/{self.campaign}/{run_id}/t{timestep}"
        # Dragon: Batch.process + Policy sends this function to run.host, next to the data it reads.
        task = self.batch.process(ProcessTemplate(
            target=reduce_worker.summarize_snapshot,
            args=(run_id, self.campaign, timestep, self.ddict_ser, self.mesh_ser, key),
            policy=Policy(placement=Policy.Placement.HOST_NAME, host_name=run.host)))  # the field never leaves its machine
        run.reader = LiveReader(TrackedTask(task), key, timestep)
        print(f"[watch] {run_id} t{timestep}: snapshot summary submitted on {run.host}", flush=True)

    def finish_solver(self, run_id, run, wave):
        """Once the solver is terminal, place the final reducer on the same machine."""
        status, value = run.work.poll(f"solver {run_id}")
        if status == "pending" or run.reader is not None:  # never reduce while a live reader still holds the data
            return
        if status == "done" and exit_ok(value, solver.RANKS):
            run.failure = "cancellation accepted; result excluded" if run.work.cancel_accepted else ""
        else:
            run.failure = f"solver {status}: {value}"
        reason = run.work.cancel_reason
        # Dragon: Batch.process + Policy sends this function to run.host, next to the data it reads.
        task = self.batch.process(ProcessTemplate(
            target=reduce_worker.finalize_run,
            args=(run_id, self.campaign, self.ddict_ser, self.mesh_ser, run.failure),
            policy=Policy(placement=Policy.Placement.HOST_NAME, host_name=run.host)))  # reads the last snapshot where it was written
        run.stage, run.work = "reduce", TrackedTask(task)
        self.metrics["runs_killed"] += int(status == "cancelled")  # counted only once the task is terminal
        self.progress = True
        print(f"[collect] {run_id}: solver terminal={status}; launch_wave={run.launch_wave} "
              f"collect_wave={wave} cancel_reason={reason}", flush=True)

    def finish_reducer(self, run_id, run):
        """Once the reducer is terminal, record the label or start a cleanup. True if recorded.

        Reads campaign DDict desc/<c>/<run> (the reducer wrote it), then writes
        it back with `dials` added and `failure` applied. That rewritten dict is
        the permanent record the analyze tools read.
        """
        status, value = run.work.poll(f"reduce {run_id}", timeout=settings.TASK_TIMEOUT)
        if status == "pending":
            return False
        key = f"desc/{self.campaign}/{run_id}"
        try:
            summary = read_final_summary(self.campaign_ddict, key, run_id, status, value)
        except (KeyError, ValueError) as exc:
            summary = {"run": run_id, "valid": False, "reason": str(exc)}
            # Dragon: Batch.process + Policy sends this function to run.host, next to the data it reads.
            task = self.batch.process(ProcessTemplate(
                target=reduce_worker.free_only,
                args=(run_id, self.campaign, self.mesh_ser),
                policy=Policy(placement=Policy.Placement.HOST_NAME, host_name=run.host)))  # a failed reducer may have left mesh data behind
            run.stage, run.work = "cleanup", TrackedTask(task)
        else:
            del self.runs[run_id]
        if run.failure:
            summary.update(valid=False, quantity=None, reason=run.failure)
        summary["dials"] = run.dials
        self.campaign_ddict[key] = summary
        self._record_result(run_id, run, summary)
        self.progress = True
        return True

    def finish_cleanup(self, run_id, run):
        """Once cleanup is acknowledged, forget the run. Anything else is unsafe."""
        status, value = run.work.poll(f"cleanup {run_id}", timeout=settings.TASK_TIMEOUT)
        if status == "pending":
            return
        if status != "done" or not exit_ok(value):
            raise UnsafeTermination(f"cleanup {run_id} not acknowledged: {status} {value}")
        del self.runs[run_id]
        self.progress = True
        print(f"[collect] {run_id}: cleanup completed", flush=True)

    def _record_result(self, run_id, run, summary):
        """Count the run; if valid, append {run, dials, quantity} to campaign DDict training_set."""
        local = summary.get("host") == run.host
        self.metrics["mesh_bytes_read"] += summary.get("bytes_read", 0)
        self.metrics["summary_bytes_crossed"] += summary.get("summary_bytes", 0)
        self.metrics["same_node_reads"] += int(local)
        self.metrics["cross_node_reads"] += int(not local and bool(summary.get("host")))
        if summary.get("valid"):
            self.campaign_ddict[settings.K_TRAIN] = self.campaign_ddict[settings.K_TRAIN] + [
                {"run": run_id, "dials": run.dials, "quantity": summary["quantity"]}]
        else:
            self.metrics["invalid_runs"] += 1
        outcome = "valid" if summary.get("valid") else f"invalid: {summary.get('reason', 'unknown')}"
        print(f"[collect] {run_id}: {outcome}; ranks={summary.get('ranks_read', 0)}/{solver.RANKS} "
              f"read {summary.get('bytes_read', 0)} bytes on {summary.get('host', 'unknown')} "
              f"in {summary.get('read_seconds', 0.0):.2f}s same_node={local}; "
              f"{summary.get('summary_bytes', 0)} bytes crossed to the campaign DDict", flush=True)

    def start_training(self, seed, wave):
        """Freeze the current labels under train/<version> and submit one fit per member.

        Reads campaign DDict training_set; writes train/<version> (a deep copy).
        The three fits receive the list by value, so later labels cannot leak in.
        """
        from science.surrogate import ENSEMBLE_SIZE

        self.check_submission()
        if self.training is not None:
            raise RuntimeError("a training round is already active")
        pairs = copy.deepcopy(self.campaign_ddict[settings.K_TRAIN])  # frozen here, so later arrivals cannot change this fit
        version = f"v{len(pairs)}-{uuid.uuid4().hex[:10]}"
        fit = TrainingRound(version, f"train/{version}", wave, len(pairs))
        self.campaign_ddict[fit.snapshot_key] = pairs
        self.training = fit
        for member in range(ENSEMBLE_SIZE):
            # Dragon: a plain Python function becomes a cluster task; Batch picks the machine.
            task = self.batch.function(fit_member, pairs, seed, member, version, wave)  # runs wherever Batch has room
            fit.tasks[member] = TrackedTask(task, process=False)
            print(f"[train] model={version} member={member} launch_wave={wave} "
                  f"{fit_counts(fit.count)} submitted", flush=True)
        self.last_fit_count = fit.count
        self.progress = True

    def finish_training(self):
        """Collect the three member results; once all are in, promote.

        Reads train/<version> back to score the ensemble on its held-out tail.
        Writes campaign DDict surrogate (the whole ensemble) and
        surrogate_version; the thinker sees the new version on its next
        propose. Deletes train/<version>.
        """
        fit = self.training
        if fit is None:
            return None
        for member, work in list(fit.tasks.items()):
            status, result = work.poll(f"fit {fit.version} member {member}")
            if status == "pending":
                if time.monotonic() - work.started > settings.RUN_TIMEOUT:
                    raise UnsafeTermination("training wait exceeded; task termination unknown")
                continue
            if status != "done":
                raise RuntimeError(f"training {fit.version} member {member} failed: {result}")
            if (not isinstance(result, dict) or result.get("member") != member or
                    result.get("model_version") != fit.version or
                    result.get("launch_wave") != fit.wave or result.get("n_total") != fit.count):
                raise ValueError("training snapshot identity mismatch")  # all three members must share one frozen dataset
            fit.results[member] = result
            del fit.tasks[member]
            self.progress = True
        if fit.tasks:
            return None
        from science.surrogate import assemble

        pairs = self.campaign_ddict[fit.snapshot_key]
        if len(pairs) != fit.count:
            raise ValueError("training snapshot count changed")
        model = assemble(pairs, list(fit.results.values()))
        model.update(version=fit.version, launch_wave=fit.wave)
        # Dragon: writing here publishes the model to the thinker host on its next propose.
        self.campaign_ddict[settings.K_MODEL] = model
        self.campaign_ddict[settings.K_VERSION] = fit.version
        del self.campaign_ddict[fit.snapshot_key]
        self.training = None
        self.error_history.append(model["validate_error"])
        return {key: model[key] for key in
                ("version", "launch_wave", "validate_error", "n_train", "n_validate", "n_total")}

    def check_drain(self, draining):
        """Only a pass that made no progress while draining counts against the budget."""
        if draining and self.pending and not self.progress:
            self.drain_rounds += 1
        self.progress = False
        if self.pending and self.drain_rounds >= settings.DRAIN_WAVES:
            raise UnsafeTermination("bounded drain exhausted")

    def finish(self):
        """Export the small report only after all jobs, readers, fits and cleanup end."""
        if self.pending:
            raise UnsafeTermination("worker work remains at publication")
        if not self.closed:
            report = {**self.metrics, "n_training_points": len(self.campaign_ddict[settings.K_TRAIN]),
                      "error_history": list(self.error_history)}
            # Dragon: join() waits for this client's outstanding tasks and detaches it; the launcher still owns destroy.
            self.batch.join(timeout=settings.CANCEL_TIMEOUT)
            clients.detach_all(self.ddict_ser, self.mesh_ser)
            self.report = {**report, "worker_joined": True}
            self.closed = True
        return copy.deepcopy(self.report)


def read_final_summary(campaign_ddict, key, run_id, status, value):
    """A run becomes a label only if every one of these holds.

    Reads campaign DDict desc/<c>/<run>. Raises when the reducer process did
    not exit cleanly, the key is missing or names another run, or a summary
    that claims valid=True is internally inconsistent. A summary that says
    valid=False (cancelled, wrong final ts) passes through unchanged.
    """
    if status != "done" or not exit_ok(value):
        raise ValueError(f"final reducer {status}: {value}")
    summary = campaign_ddict[key]
    if not isinstance(summary, dict) or summary.get("run") != run_id:
        raise ValueError("missing or mismatched final summary")
    if summary.get("valid") and (not summary.get("complete") or
            summary.get("ranks_read") != solver.RANKS or
            summary.get("timestep") != solver.NUM_TSTEPS or
            summary.get("interior_finite") is not True or
            type(summary.get("quantity")) not in (int, float) or
            not math.isfinite(summary["quantity"])):
        raise ValueError("invalid final summary data")
    return summary


def fit_member(pairs, seed, member, version, wave):
    """What Batch runs for one ensemble member, on whichever machine is free."""
    from science.surrogate import train_member

    label = (f"model={version} member={member} launch_wave={wave} "
             f"host={socket.gethostname()} {fit_counts(len(pairs))}")
    print(f"[train] {label} started", flush=True)
    result = train_member(pairs, seed, member)
    print(f"[train] {label} completed", flush=True)
    return {**result, "model_version": version, "launch_wave": wave, "n_total": len(pairs)}
