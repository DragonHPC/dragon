"""How a node reaches Dragon: attached stores, task polling, and placement.

Every helper here is process-local. A DDict client attached in one AgentHost
is not visible in another; each process attaches its own on first use.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

from dragon.data.ddict import DDict
from dragon.infrastructure.policy import Policy
from dragon.native.machine import Node, System
from dragon.native.process import ProcessTemplate
from dragon.workflows.batch import TaskNotReadyError, TaskCancelledError

import settings
from science import solver

_attached = {}
_warm = {}


class UnsafeTermination(RuntimeError):
    """A task may still access the stores; do not destroy them."""


def allocation_hostnames():
    # Dragon: System().nodes is the allocation the scheduler handed us; no parsing of $SLURM_NODELIST.
    return [Node(h_uid).hostname for h_uid in System().nodes]


def handle(serialized):
    """This process's DDict client for one descriptor: attached on first use, a dict lookup after."""
    if serialized not in _attached:
        # Dragon: DDict.attach runs once per process per DDict; a serialized descriptor is all it needs.
        _attached[serialized] = DDict.attach(serialized)
    return _attached[serialized]


def detach_all(*serialized):
    """Release this process's cached connections after its last use."""
    for descriptor in serialized:
        ddict = _attached.get(descriptor)
        if ddict is not None:
            ddict.detach()
            del _attached[descriptor]
        _warm.pop(descriptor, None)


def active_surrogate(serialized):
    """The promoted ensemble: one dict holding every member's weights, re-read only when its version changes."""
    ddict = handle(serialized)
    version = ddict[settings.K_VERSION]
    if not version:
        return None
    cached = _warm.setdefault(serialized, {"version": "", "model": None, "reuses": 0})
    if version == cached["version"]:
        cached["reuses"] += 1
    else:
        cached.update(version=version, model=ddict[settings.K_MODEL])
    return cached["model"]


def warm_reuse_count(serialized):
    return _warm.get(serialized, {}).get("reuses", 0)


def _group_helper_exited_late(exc):
    """The one teardown error that still means every rank is down.

    Batch ends a cancelled job with ProcessGroup.close(), which only returns
    after every rank is joined, then gives the group manager helper 5 s to
    exit. If the helper is late this RuntimeError replaces the cancellation.
    """
    text = str(exc)
    return (isinstance(exc, RuntimeError) and "ProcessGroup manager is not in state" in text
            and "DEAD" in text)


def poll_task(task, *, process=False):
    """Read Batch's result; a process-management failure is not a confirmed exit."""
    try:
        # Dragon: block=False makes "is it done?" a cheap question, so one loop can watch many jobs.
        return "done", task.get(block=False)
    except TaskNotReadyError:
        return "pending", None
    except TaskCancelledError as exc:
        # Batch may translate any cached exception after an accepted cancellation.
        # Do not mistake an underlying process teardown error for a clean stop.
        original = getattr(task, "exception", exc)
        if not process or isinstance(original, TaskCancelledError):
            return "cancelled", str(exc)
        if getattr(task, "_result_cached", False) and _group_helper_exited_late(original):
            return "cancelled", f"{exc}; group manager exited late, ranks were already down"
        raise UnsafeTermination(f"process cancellation unconfirmed: {original}") from exc
    except Exception as exc:
        # Function exceptions are results. Job failures normally return exit codes;
        # a raised job exception may instead come from starting/stopping processes.
        if process or not getattr(task, "_result_cached", False):
            raise UnsafeTermination(f"task termination unknown: {exc}") from exc
        return "failed", str(exc)


def exit_ok(result, ranks=1):
    """Every rank must have returned exit code zero, and nothing else."""
    values = result if isinstance(result, list) else [result]
    return len(values) == ranks and all(type(code) is int and code == 0 for code in values)


def simulation_template(item, host, bin_path, mesh_ser, campaign):
    """The miniAMR command line plus the env vars its C hook reads."""
    # Dragon: one ProcessTemplate describes the binary, its args, and which machine gets it.
    template = ProcessTemplate(
        target=bin_path, args=tuple(solver.build_argv(item["dials"])),
        policy=Policy(placement=Policy.Placement.HOST_NAME, host_name=host))  # all 8 ranks on one machine, so their mesh lands on one manager
    template.env = {
        "MINIAMR_DDICT": mesh_ser.decode() if isinstance(mesh_ser, bytes) else str(mesh_ser),
        "MINIAMR_RUN": item["id"], "MINIAMR_CAMPAIGN": campaign,
        "MINIAMR_LIVE_FREQ": str(solver.LIVE_FREQ),
        "MINIAMR_SNAP_FREQ": str(solver.SNAP_FREQ),
        "MINIAMR_SNAP_KEEP": str(solver.SNAP_KEEP),
    }
    template.stdout = ProcessTemplate.DEVNULL
    return template


# Task records are never serialized into a graph message.
@dataclass
class TrackedTask:
    task: Any
    started: float = field(default_factory=time.monotonic)
    process: bool = True
    cancel_at: float | None = None
    cancel_accepted: bool = False
    cancel_reason: str = ""

    def request_cancel(self, reason, node="watch"):
        if self.cancel_at is not None:
            return
        self.cancel_at, self.cancel_reason = time.monotonic(), reason
        error = ""
        try:
            # Dragon: cancel() reaches into a running MPI job; True means accepted, not yet stopped.
            self.cancel_accepted = self.task.cancel(timeout=settings.CANCEL_TIMEOUT) is True
        except Exception as exc:
            error = str(exc)
        print(f"[{node}] cancel requested: {reason}; accepted={self.cancel_accepted} error={error}", flush=True)

    def poll(self, label, *, timeout=None, node="collect"):
        status, value = poll_task(self.task, process=self.process)
        if status == "pending":
            if timeout is not None and time.monotonic() - self.started >= timeout:
                self.request_cancel(f"{label} timeout", node)
            if self.cancel_at is not None and time.monotonic() - self.cancel_at > settings.CANCEL_TIMEOUT:
                raise UnsafeTermination(f"{label} termination unknown after cancel request")
        return status, value
