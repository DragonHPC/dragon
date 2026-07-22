"""Graph-based distributed scheduling for functions, executables, and parallel applications"""

import copy
import heapq
import json
import logging
import math
import os
import queue
import random
import subprocess
import sys
import threading
import time
import traceback
from collections import OrderedDict, deque
from contextlib import contextmanager
import warnings
import yaml
import cloudpickle

from collections import namedtuple
from collections.abc import Callable
from dataclasses import dataclass
from ...data.ddict.ddict import DDict
from ...dlogging.util import setup_BE_logging, DragonLoggingServices as dls
from ... import fli
from ...globalservices.process import query as process_query, ProcessError
from ...infrastructure.process_desc import ProcessDescriptor
from ...infrastructure.facts import PMIBackend
from ...infrastructure.policy import Policy
from ...native.machine import current, Node, System
from ...native.pool import Pool
from ...native.process_group import ProcessGroup, DragonUserCodeError
from ...native.process import ProcessTemplate, Process, Popen
from ...native.queue import Queue
from ...telemetry.telemetry import Telemetry as dt
from ...utils import ExceptionalThread
from enum import Enum, IntEnum
from .facts import (
    client_completion_queue_maxsize,
    client_task_batch_linger_sec,
    client_task_batch_maxsize,
    default_timeout,
    default_progress_timeout,
    default_block_size,
    default_results_ddict_managers_per_pool,
    manager_work_queue_max_batch_size,
    manager_work_queue_maxsize,
    return_queue_maxsize,
)
from functools import singledispatchmethod
from pathlib import Path
from .proxy import ProxyObj
from queue import Queue as LocalQueue
from typing import Any, Iterable, Optional, TYPE_CHECKING
from uuid import uuid1

_FUNCTION_TASK_OUTPUT_ITEM_COUNT = 2


def _next_pow_of_2(x: int) -> int:
    """
    Get the next power of 2 greater than or equal to a given value.
    """
    if x == 0:
        return 1
    else:
        return 1 << (x - 1).bit_length()


def _resolve_results_ddict_managers_per_pool(
    requested: Optional[int], workers_per_node: int
) -> tuple[int, Optional[str]]:
    """
    Resolve and clamp the results-DDict managers-per-pool knob.

    ``requested is None`` selects one results-DDict manager per worker (the
    historical default). Any other value is clamped to ``[1, workers_per_node]``.

    :param requested: User-requested number of results-DDict managers per worker
        pool, or ``None`` to use one manager per worker.
    :type requested: Optional[int]
    :param workers_per_node: Number of batch workers per node (the per-pool worker count).
    :type workers_per_node: int

    :return: A ``(value, warning)`` tuple where ``value`` is the clamped
        managers-per-pool count and ``warning`` is an optional message describing
        any clamping that occurred (``None`` when no clamping was needed).
    :rtype: tuple[int, Optional[str]]
    """
    if requested is None:
        requested_value = workers_per_node
    else:
        requested_value = int(requested)

    clamped = max(1, min(requested_value, workers_per_node))

    warning = None
    if clamped != requested_value:
        warning = (
            f"results_ddict_managers_per_pool={requested_value} is out of range "
            f"[1, {workers_per_node}]; clamping to {clamped}"
        )

    return clamped, warning


@contextmanager
def _with_ddict_manager(ddict: DDict, manager_idx: int) -> Iterable[DDict]:
    previous_manager = ddict._chosen_manager
    ddict._chosen_manager = manager_idx
    try:
        yield ddict
    finally:
        ddict._chosen_manager = previous_manager


def _drain_client_task_batch(
    first_item: Any,
    request_q: LocalQueue,
    item_type: type,
    linger_sec: float = client_task_batch_linger_sec,
    max_batch_size: int = client_task_batch_maxsize,
) -> tuple[list[Any], Optional[Any]]:
    """
    Collect a bounded batch of same-type client submissions from a local queue.

    Batch eagerly drains already-queued items, then waits briefly for follow-on
    submissions from a tight producer loop so the client request worker does not
    collapse into one-task compile batches.

    :param first_item: The item already dequeued by the caller.
    :type first_item: Any
    :param request_q: Local client request queue to drain.
    :type request_q: LocalQueue
    :param item_type: Queue item type that should be coalesced into the batch.
    :type item_type: type
    :param linger_sec: Maximum extra time to wait for follow-on submissions once
        the immediate queue drain is empty.
    :type linger_sec: float
    :param max_batch_size: Upper bound on the number of queued submissions to batch.
    :type max_batch_size: int

    :return: A ``(batch, deferred_item)`` tuple. ``deferred_item`` is the first
        non-batchable queue item encountered, if any.
    :rtype: tuple[list[Any], Optional[Any]]
    """
    batch = [first_item]
    deferred_item = None

    def _consume(item: Any) -> bool:
        nonlocal deferred_item
        if isinstance(item, item_type):
            batch.append(item)
            return True

        deferred_item = item
        return False

    while len(batch) < max_batch_size:
        try:
            next_item = request_q.get_nowait()
        except queue.Empty:
            break

        if not _consume(next_item):
            return batch, deferred_item

    if deferred_item is not None or len(batch) >= max_batch_size or linger_sec <= 0:
        return batch, deferred_item

    while len(batch) < max_batch_size and deferred_item is None:
        try:
            next_item = request_q.get(timeout=linger_sec)
        except queue.Empty:
            break

        if not _consume(next_item):
            break

        while len(batch) < max_batch_size:
            try:
                next_item = request_q.get_nowait()
            except queue.Empty:
                break

            if not _consume(next_item):
                return batch, deferred_item

    return batch, deferred_item


def _setup_logging(context: Optional[str] = None) -> logging.Logger:
    """
    Set up a batch-service logger.

    :param context: Optional suffix used to distinguish manager/client log files.
    :type context: str

    :return: Configured logger for the batch service.
    :rtype: logging.Logger
    """
    if context is None:
        file_name = f"{dls.BATCH}_{current().hostname}.log"
    else:
        file_name = f"{dls.BATCH}_{context}_{current().hostname}.log"

    setup_BE_logging(service=dls.BATCH, fname=file_name)

    if context is None:
        log = logging.getLogger(dls.BATCH)
    else:
        log = logging.getLogger(dls.BATCH).getChild(context)

    return log


def _get_traceback() -> str:
    """
    Gets the current traceback.

    :return: Returns the traceback.
    :rtype: str
    """
    return traceback.format_exc().replace("\\n", "\n").replace("\n", "\n> ")


def _sanitize_log_component(value: str) -> str:
    safe_value = "".join(ch if ch.isalnum() or ch in ("-", "_", ".") else "_" for ch in value).strip("._")
    return safe_value or "task"


def _build_log_stem(
    task_kind: str,
    target_label: Optional[str],
    task_name: Optional[str],
    tuid: str,
) -> str:
    parts = []
    if target_label:
        parts.append(_sanitize_log_component(target_label))
    if task_name:
        sanitized_name = _sanitize_log_component(task_name)
        if not target_label or sanitized_name != _sanitize_log_component(target_label):
            parts.append(sanitized_name)
    if not parts:
        parts.append(_sanitize_log_component(task_kind))
    parts.append(_sanitize_log_component(tuid))
    return "--".join(parts)


def _normalize_output_path(path: Optional[str | Path]) -> Optional[str]:
    if path is None:
        return None
    if isinstance(path, Path):
        path = str(path)
    if not isinstance(path, str):
        raise TypeError(f"output path must be a str or Path, got {type(path)!r}")
    if not path:
        raise ValueError("output path must be non-empty")
    return path


def _ensure_output_parent(path: str) -> None:
    Path(path).expanduser().parent.mkdir(parents=True, exist_ok=True)


def _describe_target(target: Any) -> str:
    if callable(target):
        return getattr(target, "__name__", target.__class__.__name__)

    return Path(str(target)).stem or Path(str(target)).name or "task"


def _describe_process_template_target(template: ProcessTemplate) -> str:
    if getattr(template, "is_python", False):
        orig_target, _, _ = template.get_original_python_parameters()
        return _describe_target(orig_target)

    return _describe_target(template.target)


def _get_rank_label() -> Optional[str]:
    for env_name in (
        "PMI_RANK",
        "PMIX_RANK",
        "OMPI_COMM_WORLD_RANK",
        "SLURM_PROCID",
        "MV2_COMM_WORLD_RANK",
    ):
        rank = os.getenv(env_name)
        if rank is not None:
            return rank

    return None


def _build_job_log_prefix() -> str:
    prefix_parts = [f"host={current().hostname}"]
    rank = _get_rank_label()
    if rank is not None:
        prefix_parts.append(f"rank={rank}")
    prefix_parts.append(f"pid={os.getpid()}")
    return f"[{' '.join(prefix_parts)}] "


class _PrefixedLineWriter:
    def __init__(self, wrapped_stream, prefix: str) -> None:
        self._wrapped_stream = wrapped_stream
        self._prefix = prefix
        self._buffer = ""
        self.encoding = getattr(wrapped_stream, "encoding", "utf-8")

    def write(self, data: str) -> int:
        if not data:
            return 0

        self._buffer += data
        while "\n" in self._buffer:
            line, self._buffer = self._buffer.split("\n", 1)
            self._wrapped_stream.write(f"{self._prefix}{line}\n")
        return len(data)

    def flush(self) -> None:
        if self._buffer:
            self._wrapped_stream.write(f"{self._prefix}{self._buffer}")
            self._buffer = ""
        self._wrapped_stream.flush()

    def writable(self) -> bool:
        return True

    def isatty(self) -> bool:
        return getattr(self._wrapped_stream, "isatty", lambda: False)()

    def fileno(self) -> int:
        return self._wrapped_stream.fileno()


def _run_python_with_prefixed_logs(target: Callable, args: tuple, kwargs: dict) -> None:
    prefix = _build_job_log_prefix()
    save_stdout = sys.stdout
    save_stderr = sys.stderr
    sys.stdout = _PrefixedLineWriter(save_stdout, prefix)
    sys.stderr = _PrefixedLineWriter(save_stderr, prefix)

    try:
        target(*args, **kwargs)
    finally:
        try:
            sys.stdout.flush()
        finally:
            try:
                sys.stderr.flush()
            finally:
                sys.stdout = save_stdout
                sys.stderr = save_stderr


def _unwrap_prefixed_python_process_payload(
    target: Callable,
    args: tuple,
    kwargs: dict,
) -> Optional[tuple[Callable, tuple, dict]]:
    """Return the underlying user payload for prefixed Python job wrappers.

    Multi-rank Python jobs wrap the original callable as
    ``_run_python_with_prefixed_logs(user_target, user_args, user_kwargs)`` so
    stdout/stderr can be rank-prefixed. Argument dependency scanning and
    substitution must operate on the nested ``user_args`` payload rather than
    the wrapper's top-level argument tuple.
    """
    if target is not _run_python_with_prefixed_logs:
        return None

    if kwargs:
        return None

    if len(args) != 3:
        return None

    wrapped_target, wrapped_args, wrapped_kwargs = args
    if not callable(wrapped_target):
        return None
    if not isinstance(wrapped_args, tuple):
        return None
    if not isinstance(wrapped_kwargs, dict):
        return None

    return wrapped_target, wrapped_args, wrapped_kwargs


def _forward_subprocess_stream(pipe, writer: _PrefixedLineWriter) -> None:
    try:
        for line in pipe:
            writer.write(line)
    finally:
        writer.flush()
        pipe.close()


def _run_binary_with_prefixed_logs(target: str, args: tuple, cwd: str, env: Optional[dict]) -> None:
    prefix = _build_job_log_prefix()
    stdout_writer = _PrefixedLineWriter(sys.stdout, prefix)
    stderr_writer = _PrefixedLineWriter(sys.stderr, prefix)
    argv = [str(target)]
    if args is not None:
        argv.extend(str(arg) for arg in args)

    proc = subprocess.Popen(
        argv,
        cwd=cwd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )
    stdout_thread = threading.Thread(target=_forward_subprocess_stream, args=(proc.stdout, stdout_writer), daemon=True)
    stderr_thread = threading.Thread(target=_forward_subprocess_stream, args=(proc.stderr, stderr_writer), daemon=True)
    stdout_thread.start()
    stderr_thread.start()
    return_code = proc.wait()
    stdout_thread.join()
    stderr_thread.join()

    if return_code != 0:
        raise SystemExit(return_code)


def _compress_hostnames(hostnames: list) -> str:
    """Return a compressed hostlist string (e.g. 'node[001-003]') for *hostnames*.

    Requires the ``python-hostlist`` package.  Falls back to a comma-separated
    list if the package is not available.
    """
    try:
        import hostlist as _hostlist

        return _hostlist.collect_hostlist(hostnames)
    except ImportError:
        return ", ".join(hostnames)


CompiledResultWrapper = namedtuple(
    "CompiledResultWrapper",
    ["tuid", "result_dict"],
)
ManagerException = namedtuple("ManagerException", ["tuid", "exception", "traceback", "err_message"])
DepSat = namedtuple(
    "DepSat",
    [
        "source_tuid",
        "source_manager_idx",
        "source_result_ddict_idx",
        "tuid",
        "compiled_tuid",
        "arg_dep_updates",
        "cancel_reason",
    ],
)
DepSatRequest = namedtuple(
    "DepSatRequest",
    ["upstream_tuid", "tuid", "compiled_tuid", "arg_dep_updates", "reply_q", "client_id", "is_raw"],
)
ArgDepUpdate = namedtuple("ArgDepUpdate", ["template_idx", "arg_idx"])
LogicalDependency = namedtuple("LogicalDependency", ["upstream_tuid", "arg_dep_updates", "origin", "is_raw"])
DependencyNotificationRoute = namedtuple(
    "DependencyNotificationRoute", ["queue", "downstream_tuid", "arg_dep_updates", "is_raw"]
)
DependencyRequestRoute = namedtuple("DependencyRequestRoute", ["queue", "upstream_tuid", "arg_dep_updates", "is_raw"])
TaskComplete = namedtuple("TaskComplete", ["tuid", "compiled_tuid", "raised"])
CompletedTaskInfo = namedtuple("CompletedTaskInfo", ["manager_idx", "result_ddict_idx", "raised"])
CompletionNotification = TaskComplete
CompletedTuids = namedtuple("CompletedTuids", ["tuids"])
StartedTaskInfo = namedtuple("StartedTaskInfo", ["control_q", "reserved_cores"])
RegisterClient = namedtuple("RegisterClient", ["ret_q", "client_id"])
UnregisterClient = namedtuple("UnregisterClient", ["client_id", "reply_q", "clean_detach"], defaults=[None, False])
DestroyCalled = namedtuple("DestroyCalled", ["force_timeout"], defaults=[None])
FenceRequest = namedtuple("FenceRequest", ["client_id", "reply_q"])
FenceComplete = namedtuple("FenceComplete", ["client_id"])
ClientFenceRequest = namedtuple("ClientFenceRequest", ["reply_q"])
ClientFlushRequest = namedtuple("ClientFlushRequest", ["reply_q"])
ClientStopRequest = namedtuple("ClientStopRequest", ["reply_q"])
ClientCancelRequest = namedtuple("ClientCancelRequest", ["task", "reply_q"])
CancelRequest = namedtuple("CancelRequest", ["client_id", "tuid", "manager_idx", "reply_q"])
CancelResponse = namedtuple("CancelResponse", ["tuid", "cancelled"])
CancelJob = namedtuple("CancelJob", [])
DataAccess = namedtuple("DataAccess", ["access_type", "kvs", "keys"])
FrontierInfo = namedtuple("FrontierInfo", ["task_list", "access_type", "write_before_read"])
SubnodeAllocRequest = namedtuple(
    "SubnodeAllocRequest", ["manager_idx", "hostnames", "client_ids", "reply_q", "priority_weight"]
)
SubnodeAllocResponse = namedtuple("SubnodeAllocResponse", ["manager_idx"])
SubnodeFreeRequest = namedtuple("SubnodeFreeRequest", ["manager_idx", "client_work_counts", "task_tuids"])
PendingAllocInfo = namedtuple("PendingAllocInfo", ["hostnames", "num_nodes", "client_ids", "priority_weight"])
ClearFenceState = namedtuple("ClearFenceState", ["client_id"])
MultiNodeJobComplete = namedtuple("MultiNodeJobComplete", ["tuid", "compiled_tuid", "node_hostnames"])


class AccessType(Enum):
    READ = 0
    WRITE = 1


class DependencyOrigin(Enum):
    ARGUMENT = 0
    DATA_ACCESS = 1


class TaskCancellationReason(Enum):
    NONE = 0
    USER_REQUESTED = 1
    UPSTREAM_RAW_FAILURE = 2


SCHEDULER_MANAGER_IDX = -1


class BatchError(Exception):
    """
    Base exception class for Batch.
    """

    def __init__(self, message):
        """Initialize the base class for Batch exceptions"""
        super().__init__(message)
        self.message = message

    def __str__(self):
        """Return the message for this exception."""
        return f"{self.message}"


class SubmitAfterCloseError(BatchError):
    """
    Deprecated compatibility exception for submitting work through a detached
    Batch client handle.

    Historically this was raised after :py:meth:`Batch.close`. Now
    :py:meth:`Batch.close` is deprecated and is a no-op, so this exception is
    raised when work is submitted after :py:meth:`Batch.join`,
    :py:meth:`Batch.destroy`, or :py:meth:`Batch.terminate` has detached the
    client from the Batch instance shared by clients.
    """

    def __init__(self, message):
        """Initialize the submit-after-close subclass for Batch exceptions"""
        super().__init__(message)
        self.message = message

    def __str__(self):
        """Return the message for this exception."""
        return f"{self.message}"


class TaskNotReadyError(BatchError):
    """
    Exception raised by :py:meth:`Task.get` when ``block=False`` and the task
    result is not yet available.
    """

    def __init__(self, message):
        """Initialize the task-not-ready subclass for Batch exceptions"""
        super().__init__(message)
        self.message = message

    def __str__(self):
        """Return the message for this exception."""
        return f"{self.message}"


class TaskCancelledError(BatchError):
    """Exception raised when a task is cancelled before producing a result."""

    def __init__(self, message):
        super().__init__(message)
        self.message = message

    def __str__(self):
        return f"{self.message}"


class _FunctionTaskCancelled(Exception):
    """Internal exception injected into function worker threads on cancellation."""


class _FunctionTaskTimedOut(TimeoutError):
    """Internal exception injected into function worker threads on timeout."""


def _cancel_requested(control_q: Queue) -> bool:
    try:
        control_msg = control_q.get_nowait()
    except queue.Empty:
        return False

    return isinstance(control_msg, CancelJob)


class ReadAfterWriteDependencyError(BatchError):
    """Exception raised when a read-after-write dependency completed with failure."""

    def __init__(self, message):
        super().__init__(message)
        self.message = message

    def __str__(self):
        return f"{self.message}"


class AsyncType(IntEnum):
    RESULT = 0
    STDOUT = 1
    STDERR = 2


class TaskType(IntEnum):
    FUNC = 0
    PROC = 1
    JOB = 2


class ValueType(Enum):
    INVALID = 0


def _str_to_task_type(input_string: str) -> TaskType:
    """
    Converts a string reprsenting the task type to a TaskType value.

    :param input_string: The string to be converted.
    :type input_str: str

    :return: Returns the TaskType value.
    :rtype: TaskType
    """
    if not isinstance(input_string, str):
        raise RuntimeError("task type must be a string")

    string = input_string.lower()
    if string == "function":
        return TaskType.FUNC
    elif string == "process":
        return TaskType.PROC
    elif string == "job":
        return TaskType.JOB
    else:
        raise RuntimeError("invalid task type--valid types are: function, process, job")


# TODO: Replacing Task with TaskCore on the managers improved throughput, but it's still
# not where it should be. Could the remaining perf loss be coming from an increased amount
# of data returning to the clients?
class TaskCore:
    def __init__(self, tuid: Optional[str], client_id: int, name: str, timeout: float) -> None:
        """
        Initializes a the core of the task, which is a lean representation of the task that
        is sent to the managers.

        :param tuid: Optional task ID assigned by the owner creating this task core.
            Client-created tasks pass in their base tuid; scheduler-created compiled
            tasks assign their internal tuid separately.
        :type tuid: Optional[str]
        :param client_id: The unique of of the client that created this task.
        :type client_id: int
        :param name: The user-supplied name for this task.
        :type name: str
        :param timeout: A timeout for the task.
        :type timeout: float

        :return: Returns None.
        :rtype: None
        """
        self.client_id = client_id
        self.name = name
        self.timeout = timeout
        self.tuid = tuid
        self.manager_idx: Optional[int] = None
        self.result_ddict_idx: Optional[int] = None
        self.compiled_tuid = None
        self.cached_queue = None

        # Runtime routing state populated by Manager.compile() for the current
        # compiled batch only. These routes tell an upstream task where to send
        # direct DepSat notifications when both ends of the dependency are part
        # of the same compiled task.
        self.downstream_routes: list[DependencyNotificationRoute] = []
        self.upstream_routes: list[DependencyRequestRoute] = []

        # Durable logical dependency state recorded when dependencies are
        # declared. This survives until compile-time, where it is used to
        # rebuild the live dependency DAG and to build the runtime queue
        # routing above. Multiplicity is preserved because a downstream task
        # can depend on the same upstream task more than once for different
        # reasons.
        self.dependencies: list[LogicalDependency] = []
        self.num_dep_sat = 0
        self.num_dep_tot = 0
        # HEFTY metadata is assigned at compile time and then reused both for
        # partitioning and for ready-task launch order on subnode managers.
        self.weight = 0
        self.heft_topo_order: int | float = math.inf
        self.is_cancelled = TaskCancellationReason.NONE
        self.cancel_source_tuid: Optional[str] = None
        self.cancel_source_manager_idx: Optional[int] = None
        self.cancel_source_result_ddict_idx: Optional[int] = None

    def __repr__(self) -> str:
        """
        Get the ID string for this task.

        :return: Returns the ID string for this task.
        :rtype: str
        """
        return self._get_id_str()

    def _get_id_str(self) -> str:
        """
        Get an id string for this task indicating its client, tuid, and any associated compiled tuid.
        """
        return (
            f"name={self.name}, client_id={self.client_id}, tuid={self.tuid}, "
            f"manager_idx={self.manager_idx}, "
            f"result_ddict_idx={self.result_ddict_idx}, "
            f"compiled_tuid={self.compiled_tuid}"
        )

    def _notify_dep_tasks(self, result) -> None:
        """
        Notifies all tasks that depend on this task of its completion.

        :return: Returns None.
        :rtype: None
        """
        if len(self.downstream_routes) > 0:
            compiled_tuid = self.compiled_tuid
            task_failed = isinstance(result, BaseException)

            for route in self.downstream_routes:
                arg_dep_updates = route.arg_dep_updates if route.arg_dep_updates is not None else []
                cancel_reason = TaskCancellationReason.NONE
                if route.is_raw and task_failed:
                    cancel_reason = TaskCancellationReason.UPSTREAM_RAW_FAILURE
                dep_sat = DepSat(
                    self.tuid,
                    self.manager_idx,
                    self.result_ddict_idx,
                    route.downstream_tuid,
                    compiled_tuid,
                    arg_dep_updates,
                    cancel_reason,
                )
                route.queue.put(dep_sat)


class Task:
    def __init__(
        self,
        task_core: TaskCore,
        batch: "Batch",
        reads: Optional[list] = None,
        writes: Optional[list] = None,
        compiled: bool = False,
    ) -> None:
        """
        Initializes a new task.

        :param task_core: The core parts of the task, allowing us to send leaner objects to the managers.
        :type task_core: :py:class:`TaskCore`
        :param batch: The batch to which this task belongs.
        :type batch: :py:class:`Batch`
        :param reads: A list of ``Read`` objects created by calling :py:meth:`Batch.read`.
        :type reads: Optional[list]
        :param writes: A list of ``Write`` objects created by calling :py:meth:`Batch.write`.
        :type writes: Optional[list]
        :param compiled: A flag indicating if this task is compiled.
        :type compiled: bool

        :return: Returns None.
        :rtype: None
        """
        if batch is not None:
            batch.log.debug(
                f"initializing task with core={task_core}, reads={reads}, writes={writes}, compiled={compiled}"
            )

        self.core = task_core
        # non-core attributes are only needed by client code (i.e., can be deleted
        # when sending a work chunk to a manager)
        self._compiled_task = None
        self._batch = batch
        self._results_ddict = batch.results_ddict if batch is not None else None
        self.accesses = {}
        self.subnode_work_chunks = []  # Work chunks for subnode managers
        self.mnj_work_chunk = None  # Single scheduler-owned Work chunk for multi-node jobs
        self._ready = False
        self.exception = None
        self.traceback = None
        self.num_subtasks = 0
        self._result = None
        self._result_cached = False
        self._result_cleared = False
        self._result_unavailable_message = None
        self._result_fetch_lock = threading.Lock()
        self._stdout = None
        self._stderr = None
        self._task_kind = None
        self._target_label = None

        if reads is not None:
            for read in reads:
                self._access(read)

        if writes is not None:
            for write in writes:
                self._access(write)

    def get_manager_idx(self, block: bool = True, timeout: float = default_timeout) -> int:
        if self.core.manager_idx is not None:
            return self.core.manager_idx

        if self._batch is None:
            raise RuntimeError(f"task has no batch reference: {self.core._get_id_str()}")

        if not block:
            raise TaskNotReadyError(f"manager idx not yet available: {self.core._get_id_str()}")

        self._batch._flush_client_request_worker(timeout=timeout)

        if self.core.manager_idx is None:
            raise RuntimeError(f"manager idx not set after compile: {self.core._get_id_str()}")

        return self.core.manager_idx

    def _depends_on(
        self,
        task: "Task",
        arg_dep_update: Optional[list[ArgDepUpdate]] = None,
        origin: DependencyOrigin = DependencyOrigin.DATA_ACCESS,
        is_raw: bool = False,
    ) -> None:
        """
        Sets a dependency for this task, which will be blocked until the completion of ``task``.

        :param task: The task that this one will be dependent on.
        :param origin: Metadata describing why this dependency exists.
        :type origin: DependencyOrigin

        :return: Returns None.
        :rtype: None
        """
        self.core.num_dep_tot += 1

        # Record the logical dependency on the downstream task. Compile-time
        # code later turns this durable record into live dependency edges and
        # manager-to-manager routing state.
        self.core.dependencies.append(LogicalDependency(task.core.tuid, arg_dep_update, origin, is_raw))

    def _handle_arg_passing_deps(self, list_of_arg_lists: Optional[list]) -> None:
        """
        Set up argument-passing dependencies for this task.

        :param list_of_arg_lists: A list of argument lists to be checked for Task args.
        :type list_of_arg_lists: list

        :return: Returns None.
        :rtype: None
        """
        arg_dep_updates = {}

        for template_idx, args in enumerate(list_of_arg_lists):
            if args is None:
                continue

            # get the index for each argument that's a Task (representing its output in this case)
            # and add a key-value pair to the arg_dep_updates dict, key=arg, value=list of arg dep
            # update tuples
            for idx, arg in enumerate(args):
                if isinstance(arg, Task):
                    try:
                        arg_dep_updates[arg].append(ArgDepUpdate(template_idx, idx))
                    except KeyError:
                        arg_dep_updates[arg] = [ArgDepUpdate(template_idx, idx)]

        # for each argument and its associated list of indexes, add a dependency from the task
        # associated with the argument to this task, and associate the list of output indexes
        # with this dependency
        for arg, arg_dep_update_list in arg_dep_updates.items():
            self._depends_on(
                arg,
                arg_dep_update=arg_dep_update_list,
                origin=DependencyOrigin.ARGUMENT,
                is_raw=True,
            )

    def _access(self, data_access: DataAccess) -> None:
        """
        Indicates that this task will access (read or write) a kvs at the specified keys.

        :param data_access: Specifies the access type, kvs, and keys used for one or more data accesses

        :return: Returns None.
        :rtype: None
        """
        access_type = data_access.access_type
        kvs = data_access.kvs
        keys = data_access.keys

        for key in keys:
            # if we haven't seen this access before, or if the previous access was a read,
            # then update the accesses dict. the check for previous read accesses is necessary
            # because (1) this access could be a write, and (2) if there are multiple accesses
            # to the same key, and at least one of them is a write, then this access type should
            # be a write for the purpose of inferring dependencies
            access_key = (id(kvs), key)
            if access_key not in self.accesses or self.accesses[access_key][1] == AccessType.READ:
                self.accesses[access_key] = (self, access_type)

    def get(self, block: bool = True, timeout: float = default_timeout) -> None:
        """
        Wait for this Task to complete and return its result.

        ``get()`` never prints task stdout or stderr. When the owning Batch was
        created with ``task_logs=True``, task stdout and stderr are redirected to
        files; use :py:meth:`Task.stdout_path`, :py:meth:`Task.stderr_path`,
        :py:meth:`Task.get_stdout`, :py:meth:`Task.get_stderr`, or
        :py:meth:`Task.log_paths` to inspect the resolved task logs. When task
        logging is disabled (the default), task stdout and stderr are not
        captured to files unless an explicit ``stdout``/``stderr`` path was given;
        otherwise they are forwarded to the client console like any other Dragon
        process. Task results remain in Batch's internal
        results DDict until ``get()`` fetches them or
        :py:meth:`Batch.clear_results` discards them. Once fetched here, the
        result is cached on this client-side task handle for subsequent calls.
        In unmanaged mode, fetch any result you need before the last client
        calls :py:meth:`Batch.join`, because that shutdown destroys the shared
        results DDict.

        :param block: If True (the default), block until the result is available or *timeout* is
            exceeded. If False, return immediately if the result is available, otherwise raise
            :py:exc:`TaskNotReadyError`.
        :type block: bool
        :param timeout: The timeout for waiting. Defaults to 1e9. Ignored when *block* is False.
        :type timeout: float

        :raises TimeoutError: If the specified timeout is exceeded.
        :raises :py:exc:`TaskNotReadyError`: If *block* is False and the result is not yet available.

        :return: Returns the result of the task.
        :rtype: Any
        """
        self._ensure_result_cached(block=block, timeout=timeout)
        return self._return_or_raise_cached_result()

    def _get_result_tuple(
        self, block: bool = True, timeout: float = default_timeout
    ) -> tuple[Any, Optional[str], bool]:
        if self._result_cached:
            return self._result, self.traceback, self.exception is not None or self.traceback is not None

        if self._result_cleared:
            raise RuntimeError(f"result was cleared before retrieval: {self.core._get_id_str()}")

        if self._result_unavailable_message is not None:
            raise RuntimeError(f"{self._result_unavailable_message}: {self.core._get_id_str()}")

        if self._results_ddict is None:
            raise RuntimeError(f"task has no results_ddict reference: {self.core._get_id_str()}")

        self.get_manager_idx(block=block, timeout=timeout)

        if self.core.result_ddict_idx is None:
            raise RuntimeError(f"task result location not assigned by compiler: {self.core._get_id_str()}")

        # Serialize the shared results DDict client connector. get() and the log
        # accessors can run from multiple threads and across different tasks of
        # the same Batch; the DDict client assumes single-threaded use (and
        # _with_ddict_manager mutates shared state on it), so concurrent indexing
        # would desync its return channel. This lock covers only the brief
        # request/response below -- not the blocking wait in get_manager_idx() --
        # so fetches for independent tasks still proceed concurrently.
        with self._batch._results_ddict_lock:
            with _with_ddict_manager(self._results_ddict, self.core.result_ddict_idx) as results_ddict:
                if not block and self.core.tuid not in results_ddict:
                    raise TaskNotReadyError(f"result not yet available: {self.core._get_id_str()}")

                try:
                    return results_ddict[self.core.tuid]
                except KeyError:
                    raise RuntimeError(f"no return value found for task: {self.core._get_id_str()}")

    def _ensure_result_cached(self, block: bool = True, timeout: float = default_timeout) -> None:
        if self._result_cached:
            return

        # get() and get_stdout()/get_stderr() can run on the same task from
        # different threads. Serialize the fetch so only one thread touches the
        # shared results DDict connector; the other observes the cached result.
        with self._result_fetch_lock:
            if self._result_cached:
                return

            result, tb, raised = self._get_result_tuple(block=block, timeout=timeout)
            self._cache_result_tuple(result, tb, raised)

            if self._batch is not None:
                self._batch._mark_task_log_complete(self.uid)

    def _cache_result_tuple(self, result: Any, tb: Optional[str], raised: bool) -> None:
        self._result = result
        self.traceback = tb
        self.exception = result if raised and isinstance(result, BaseException) else None
        self._result_cached = True
        self._result_cleared = False

    def _return_or_raise_cached_result(self) -> Any:
        if self.exception is not None:
            if self.core.is_cancelled == TaskCancellationReason.USER_REQUESTED:
                raise TaskCancelledError(f"task {self.core.tuid} was cancelled") from None
            raise self.exception

        if self.traceback is not None and self.exception is None:
            raise RuntimeError(
                f"task with {self.core._get_id_str()} failed with the following traceback:\n{self.traceback}"
            )

        return self._result

    def cancel(self, timeout: float = default_timeout) -> bool:
        """Best-effort cancellation request for this task.

        Returns ``True`` when the scheduler accepted the cancellation and the
        task will complete with ``TaskCancelledError``. Returns ``False`` when
        the task cannot be cancelled or already completed.
        """
        cancelled = self._batch._cancel_task(self, timeout=timeout)
        if cancelled and self.core.is_cancelled == TaskCancellationReason.NONE:
            self.core.is_cancelled = TaskCancellationReason.USER_REQUESTED
        return cancelled

    def log_paths(self) -> dict[str, Optional[str]]:
        """Return the resolved stdout/stderr log file paths for this task.

        The returned dictionary contains ``stdout`` and ``stderr`` keys whose
        values are the shared-filesystem paths configured or generated for this
        task. A value is ``None`` when no path was configured, which happens for
        tasks submitted without an explicit ``stdout``/``stderr`` on a client
        created with ``task_logs=False`` (the default), since their output is
        forwarded to the client console instead of a file.
        """
        return {"stdout": self._stdout, "stderr": self._stderr}

    @property
    def stdout_path(self) -> Optional[str]:
        """Return the resolved stdout log path for this task, if any."""

        return self._stdout

    @property
    def stderr_path(self) -> Optional[str]:
        """Return the resolved stderr log path for this task, if any."""

        return self._stderr

    def get_stdout(
        self,
        block: bool = True,
        timeout: float = default_timeout,
        encoding: str = "utf-8",
    ) -> Optional[str]:
        """Read this task's stdout log.

        By default, waits for the task to complete before reading the final log
        file. When *block* is ``False``, raises :py:exc:`TaskNotReadyError` if
        the task has not completed yet.
        """

        return self._get_log_contents("stdout", block=block, timeout=timeout, encoding=encoding)

    def get_stderr(
        self,
        block: bool = True,
        timeout: float = default_timeout,
        encoding: str = "utf-8",
    ) -> Optional[str]:
        """Read this task's stderr log.

        By default, waits for the task to complete before reading the final log
        file. When *block* is ``False``, raises :py:exc:`TaskNotReadyError` if
        the task has not completed yet.
        """

        return self._get_log_contents("stderr", block=block, timeout=timeout, encoding=encoding)

    def _get_log_contents(
        self,
        stream_name: str,
        block: bool = True,
        timeout: float = default_timeout,
        encoding: str = "utf-8",
    ) -> Optional[str]:
        self._ensure_result_cached(block=block, timeout=timeout)

        path = self._stdout if stream_name == "stdout" else self._stderr
        if path is None:
            return None

        return Path(path).read_text(encoding=encoding)

    def __getstate__(self) -> dict:
        state = self.__dict__.copy()
        # DDict objects cannot be deserialized in the same process that created
        # them (process-local channel restriction). The scheduler runs as a thread
        # in the client process, so strip _results_ddict before pickling. The
        # original client-side task object is never pickled and retains the ref.
        state.pop("_batch", None)
        state.pop("_results_ddict", None)
        state.pop("_result_fetch_lock", None)
        return state

    def __setstate__(self, state) -> None:
        self.__dict__.update(state)
        self._results_ddict = None
        if not hasattr(self, "_result_fetch_lock"):
            self._result_fetch_lock = threading.Lock()
        if not hasattr(self, "_result_cached"):
            self._result_cached = False
        if not hasattr(self, "_result_cleared"):
            self._result_cleared = False
        if not hasattr(self, "_result_unavailable_message"):
            self._result_unavailable_message = None

    @property
    def uid(self):
        """
        Provides the unique ID for this task.
        """
        return self.core.tuid

    @property
    def weight(self):
        """
        Provides the HEFTY weight for this task.
        """
        return self.core.weight

    @weight.setter
    def weight(self, value) -> None:
        self.core.weight = value


class FunctionCore(TaskCore):
    def __init__(
        self,
        tuid: str,
        client_id: int,
        name: str,
        timeout: float,
        target: Callable,
        args: tuple = (),
        kwargs: dict = {},
        stdout: Optional[str] = None,
        stderr: Optional[str] = None,
    ) -> None:
        """
        Initialize the core part of a function.

        :param client_id: The ID of the client that created this function task.
        :type client_id: int
        :param name: The name of the function task.
        :type name: str
        :param timeout: The timeout for the function task.
        :type timeout: float
        :param target: The function to be run.
        :type target: Callable
        :param args: Positional arguments for the function.
        :type args: tuple
        :param kwargs: Keyword arguments for the function.
        :type kwargs: dict

        :return: Returns None.
        :rtype: None
        """
        super().__init__(tuid, client_id, name, timeout)

        self._serialized_func = cloudpickle.dumps(target)
        self.args = args
        self.kwargs = kwargs
        self.stdout = stdout
        self.stderr = stderr

    def _load_func(self) -> Callable:
        return cloudpickle.loads(self._serialized_func)

    def _func_wrapper(self, output: list) -> None:
        """
        Run the user's function with stdout/stderr redirected to the resolved
        log files, then append the function result or exception to ``output``.

        This wrapper is run in a thread so the caller can enforce the
        user-specified timeout.

        :param output: A list containing output from the function (necessary because the
        function is run in a thread).
        :type output: list

        :return: Returns None.
        :rtype: None
        """
        save_stdout = sys.stdout
        save_stderr = sys.stderr
        stdout_file = None
        stderr_file = None
        combined_file = None

        result = None
        exc = None

        try:
            if self.stdout is not None and self.stderr is not None and self.stdout == self.stderr:
                _ensure_output_parent(self.stdout)
                combined_file = open(self.stdout, "a", encoding="utf-8", buffering=1)
                sys.stdout = combined_file
                sys.stderr = combined_file
            else:
                if self.stdout is not None:
                    _ensure_output_parent(self.stdout)
                    stdout_file = open(self.stdout, "a", encoding="utf-8", buffering=1)
                    sys.stdout = stdout_file
                if self.stderr is not None:
                    _ensure_output_parent(self.stderr)
                    stderr_file = open(self.stderr, "a", encoding="utf-8", buffering=1)
                    sys.stderr = stderr_file
            result = self._load_func()(*self.args, **self.kwargs)
        except Exception as err:
            exc = err
        finally:
            for handle in (combined_file, stdout_file, stderr_file):
                if handle is not None:
                    try:
                        handle.flush()
                    except Exception:
                        pass
            sys.stdout = save_stdout
            sys.stderr = save_stderr
            for handle in (combined_file, stdout_file, stderr_file):
                if handle is not None:
                    handle.close()

        output.extend([result, exc])

    def run(self, control_q: Queue) -> Any:
        """
        Runs the function associated with a function task.

        :return: Returns the return value of the function associated with the task.
        :rtype: Any
        """
        output = []
        worker = ExceptionalThread(target=self._func_wrapper, args=(output,))
        timeout_deadline = None if self.timeout >= default_timeout else time.monotonic() + self.timeout

        worker.start()

        while worker.is_alive():
            worker.join(timeout=0.1)
            if not worker.is_alive():
                break

            try:
                control_msg = control_q.get_nowait()
            except queue.Empty:
                control_msg = None

            if isinstance(control_msg, CancelJob):
                worker.kill_by_exception(_FunctionTaskCancelled)

            if timeout_deadline is not None and time.monotonic() >= timeout_deadline:
                worker.kill_by_exception(_FunctionTaskTimedOut)
                timeout_deadline = None

        worker.join()

        if len(output) != _FUNCTION_TASK_OUTPUT_ITEM_COUNT:
            raise RuntimeError(f"function worker exited without producing output: {self._get_id_str()}")

        result, exc = output
        if isinstance(exc, _FunctionTaskCancelled):
            raise TaskCancelledError(f"task was cancelled before completing: {self._get_id_str()}")
        if isinstance(exc, _FunctionTaskTimedOut):
            raise TimeoutError(f"task timed out before completing: {self._get_id_str()}")
        if exc is not None:
            raise exc

        return result


class Function(Task):
    def __init__(
        self,
        batch,
        # function args
        target: Callable,
        args: tuple = (),
        kwargs: dict = {},
        # task args
        reads: Optional[list] = None,
        writes: Optional[list] = None,
        name: Optional[str] = None,
        timeout: float = default_timeout,
        stdout: Optional[str | Path] = None,
        stderr: Optional[str | Path] = None,
    ) -> None:
        """
        Creates a new function task. Arguments for the function that are of type :py:class:`Task`
        will create a dependency for this task on the output of the task specified by the
        argument. Further, the output of the specified task will be passed in place of the
        :py:class:`Task` argument when the function executes.

        :param batch: The batch in which this function task will execute.
        :param func: The function to associate with the object.
        :param *args: The arguments for the function.
        :param reads: A list of ``Read`` objects created by calling :py:meth:`Batch.read`.
        :type reads: Optional[list]
        :param writes: A list of ``Write`` objects created by calling :py:meth:`Batch.write`.
        :type writes: Optional[list]
        :param name: A human-readable name for the task.
        :type name: Optional[str]

        :return: Returns None.
        :rtype: None
        """
        tuid = batch._next_tuid()
        resolved_stdout, resolved_stderr = batch._resolve_task_output_paths(
            tuid=tuid,
            task_kind="function",
            target_label=_describe_target(target),
            name=name,
            stdout=stdout,
            stderr=stderr,
        )

        # replace Task argument with None, since the actual value will be filled in
        # by the manager after the dependency is satisfied
        sanitized_args = tuple(None if isinstance(arg, Task) else arg for arg in args)
        super().__init__(
            FunctionCore(
                tuid,
                batch.client_id,
                name,
                timeout,
                target,
                sanitized_args,
                kwargs,
                stdout=resolved_stdout,
                stderr=resolved_stderr,
            ),
            batch,
            reads=reads,
            writes=writes,
        )
        self._stdout = resolved_stdout
        self._stderr = resolved_stderr
        self._task_kind = "function"
        self._target_label = _describe_target(target)
        if batch._task_logs_enabled:
            batch._append_log_manifest_entry(
                tuid=tuid,
                task_kind=self._task_kind,
                target_label=self._target_label,
                task_name=name,
                stdout_path=resolved_stdout,
                stderr_path=resolved_stderr,
            )
        # Always track the task so unmanaged-shutdown result invalidation can
        # reach it, even when task logging (and thus the manifest) is disabled.
        batch._register_task_for_log_metadata(self)
        self._handle_arg_passing_deps([args])


class JobCore(TaskCore):
    def __init__(
        self,
        tuid: str,
        client_id: int,
        name: str,
        timeout: float,
        process_templates: list[ProcessTemplate],
        pmi: Optional[PMIBackend] = PMIBackend.CRAY,
    ) -> None:
        """
        Description.

        :param client_id: The ID of the client that created this job task.
        :type client_id: int
        :param name: The user-specified name of the job task.
        :type name: str
        :param timeout: The timeout for this job task.
        :type timeout: float
        :param process_templates: A list of pairs of the form (nprocs, process_template), where
        nprocs indicates the number of processes to create for this job using the corresponding
        ProcessTemplate.
        :type process_templates: list
        :param pmi: The PMI backend to use for launching MPI jobs. Defaults to ``PMIBackend.CRAY``. Set to ``PMIBackend.PMIX`` for systems using PMIx, or ``None`` to disable PMI.
        :type pmi: PMIBackend

        :raises RuntimeError: If there is an issue while setting up the dependency graph

        :return: Returns None.
        :rtype: None
        """
        super().__init__(tuid, client_id, name, timeout)

        self.num_procs = None
        self.process_templates = process_templates
        self.hostname_list = None
        self.is_parallel = True
        self.pmi = pmi

    def run(self, control_q: Queue) -> Any:
        """
        Runs the job associated with a job task.

        A non-zero exit is reported as the exit code, not raised: process and
        job tasks return their child exit code(s) as the task result. A single
        process returns a scalar exit code; a multi-rank job returns a list of
        exit codes, one per launched process.

        :raises RuntimeError: If path to the target is invalid.

        :return: The child exit code (int) for a single process, or a list of
            exit codes for a multi-rank job.
        :rtype: int | list[int]
        """
        if self.is_parallel:
            pmi = self.pmi
        else:
            pmi = None

        # ignore_error_on_exit=True so a non-zero process exit is reported back
        # as an exit code via inactive_puids rather than the Manager raising a
        # DragonUserCodeError. Batch's process()/job() contract is to return
        # exit codes (including non-zero) as the task result so callers and
        # downstream argument dependencies can inspect them.
        grp = ProcessGroup(restart=False, ignore_error_on_exit=True, pmi=pmi, walltime=self.timeout)

        if self.hostname_list is None:
            raise RuntimeError(f"no hostname list available for job: {self._get_id_str()}")
        if len(self.hostname_list) != self.num_procs:
            raise RuntimeError(
                f"invalid hostname list for job: expected {self.num_procs} entries, got {len(self.hostname_list)}"
            )

        # Build one add_process call per (template, hostname) pair, using the
        # nproc replica count instead of one call per rank. This keeps the
        # number of templates sent to GS at O(num_templates × num_nodes) rather
        # than O(num_procs).
        hostname_idx = 0
        for nprocs_this_template, template in self.process_templates:
            template_hostnames = self.hostname_list[hostname_idx : hostname_idx + nprocs_this_template]
            if len(template_hostnames) != nprocs_this_template:
                raise RuntimeError(
                    f"invalid hostname list for template: expected {nprocs_this_template} entries, got {len(template_hostnames)}"
                )
            hostname_idx += nprocs_this_template

            # Count how many ranks land on each node for this template. Nodes
            # do not need to receive the same number of ranks.
            counts: dict[str, int] = {}
            for h in template_hostnames:
                counts[h] = counts.get(h, 0) + 1

            base_policy = template.policy if template.policy is not None else Policy()
            requested_host_name = base_policy.placement == Policy.Placement.HOST_NAME
            requested_host_id = base_policy.placement == Policy.Placement.HOST_ID

            if requested_host_name and not base_policy.host_name:
                raise RuntimeError(
                    "invalid process template policy: placement=HOST_NAME requires a non-empty host_name"
                )
            if requested_host_id and base_policy.host_id == -1:
                raise RuntimeError("invalid process template policy: placement=HOST_ID requires a valid host_id")

            for hostname, count in counts.items():
                t = copy.copy(template)
                explicit_host_requested = requested_host_name
                explicit_host_id_requested = requested_host_id

                if explicit_host_requested or explicit_host_id_requested:
                    t.policy = copy.copy(base_policy)
                else:
                    runtime_policy = copy.copy(base_policy)
                    runtime_policy.placement = Policy.Placement.HOST_NAME
                    runtime_policy.host_name = hostname
                    runtime_policy.host_id = -1
                    t.policy = runtime_policy
                grp.add_process(nproc=count, template=t)

        cancelled = False

        try:
            # TODO: is it possible to avoid global services in the single local process case?
            grp.init()
            grp.start()

            while True:
                if _cancel_requested(control_q):
                    cancelled = True
                    grp._stop_no_decorator()
                    break

                try:
                    grp.join(timeout=0.1)
                    break
                except TimeoutError:
                    continue

            if cancelled:
                raise TaskCancelledError(f"task was cancelled before completing: {self._get_id_str()}")

            # get puids and exit codes
            puids = []
            exit_codes = []
            for puid, exit_code in grp.inactive_puids:
                puids.append(puid)
                exit_codes.append(exit_code)

            # Preserve process() ergonomics by returning a scalar exit code for
            # the single-process case, even though jobs internally track one
            # exit code per launched process.
            if len(exit_codes) == 1:
                exit_codes = exit_codes[0]

            return exit_codes
        except DragonUserCodeError:
            # Cancellation stops a running ProcessGroup by signalling its
            # workers, which ProcessGroup reports back as DragonUserCodeError.
            # Translate that shutdown path to TaskCancelledError so callers see
            # the Batch-level cancellation contract instead of a process-group
            # implementation detail.
            if cancelled or _cancel_requested(control_q):
                cancelled = True
                raise TaskCancelledError(f"task was cancelled before completing: {self._get_id_str()}") from None
            raise
        finally:
            try:
                if cancelled:
                    grp._close_no_decorator()
                else:
                    grp.close()
            except DragonUserCodeError:
                if not (cancelled or _cancel_requested(control_q)):
                    raise


class Job(Task):
    def __init__(
        self,
        batch,
        process_templates: list[ProcessTemplate],
        reads: Optional[list] = None,
        writes: Optional[list] = None,
        name: Optional[str] = None,
        timeout: float = default_timeout,
        pmi: Optional[PMIBackend] = PMIBackend.CRAY,
        stdout: Optional[str | Path] = None,
        stderr: Optional[str | Path] = None,
    ) -> None:
        """
        Creates a new job task. Arguments for a process passed using :py:attr:`ProcessTemplate.args`
        that are of type :py:class:`Task` will create a dependency for this task on the output of the
        task specified by the argument. Further, the output of the specified task will be
        passed in place of the :py:class:`Task` argument when the job executes.

        :param batch: The batch in which this function task will execute.
        :type batch: :py:class:`Batch`
        :param process_templates: List of pairs of the form (nprocs, process_template), where nprocs is the number
        of processes to create using the specified template.
        :type process_templates: list
        :param reads: A list of ``Read`` objects created by calling :py:meth:`Batch.read`.
        :type reads: Optional[list]
        :param writes: A list of ``Write`` objects created by calling :py:meth:`Batch.write`.
        :type writes: Optional[list]
        :param name: A human-readable name for the task.
        :type name: Optional[str]
        :param pmi: The PMI backend to use for launching MPI jobs. Defaults to ``PMIBackend.CRAY``. Set to ``PMIBackend.PMIX`` for systems using PMIx, or ``None`` to disable PMI.
        :type pmi: PMIBackend

        :return: Returns None.
        :rtype: None
        """
        if len(process_templates) == 0:
            raise RuntimeError("need at least one process template")

        tuid = batch._next_tuid()
        task_kind = "process" if len(process_templates) == 1 and process_templates[0][0] == 1 else "job"
        target_labels = {_describe_process_template_target(template) for _, template in process_templates}
        target_label = next(iter(target_labels)) if len(target_labels) == 1 else "mixed"
        resolved_stdout, resolved_stderr = batch._resolve_task_output_paths(
            tuid=tuid,
            task_kind=task_kind,
            target_label=target_label,
            name=name,
            stdout=stdout,
            stderr=stderr,
        )

        total_procs = 0
        list_of_arg_lists = []
        resolved_templates = []

        # TODO: handle kwargs for arg-passing deps (probably changes elsewhere too)
        for nprocs_this_template, template in process_templates:
            template = batch._resolve_process_template_outputs(
                template,
                resolved_stdout,
                resolved_stderr,
            )
            total_procs += nprocs_this_template

            if getattr(template, "is_python", False):
                # For Python-callable templates the user-visible args (including
                # any Task handles) are serialized into `argdata` by
                # ProcessTemplate.__init__; `template.args` only holds the
                # subprocess CLI flags (["-c", "..."]) and never contains Task
                # instances. We must extract the original user args from
                # `argdata` so that _handle_arg_passing_deps can register
                # dependencies, and then write back a sanitized argdata with
                # Task placeholders replaced by None. `template.args` (CLI
                # flags) must be left untouched.
                orig_target, orig_args, orig_kwargs = template.get_original_python_parameters()
                wrapped_payload = _unwrap_prefixed_python_process_payload(orig_target, orig_args, orig_kwargs)

                if wrapped_payload is not None:
                    wrapped_target, wrapped_args, wrapped_kwargs = wrapped_payload
                    list_of_arg_lists.append(tuple(wrapped_args))
                    sanitized_wrapped_args = tuple(None if isinstance(a, Task) else a for a in wrapped_args)
                    template.argdata = cloudpickle.dumps(
                        (orig_target, (wrapped_target, sanitized_wrapped_args, wrapped_kwargs), orig_kwargs)
                    )
                else:
                    list_of_arg_lists.append(tuple(orig_args))
                    sanitized_orig_args = tuple(None if isinstance(a, Task) else a for a in orig_args)
                    template.argdata = cloudpickle.dumps((orig_target, sanitized_orig_args, orig_kwargs))
            else:
                # Binary templates: user args are directly in template.args.
                list_of_arg_lists.append(template.args)
                sanitized_args = tuple(None if isinstance(arg, Task) else arg for arg in template.args)
                template.args = sanitized_args

            resolved_templates.append((nprocs_this_template, template))

        super().__init__(
            JobCore(tuid, batch.client_id, name, timeout, resolved_templates, pmi=pmi),
            batch,
            reads=reads,
            writes=writes,
        )
        metadata_stdout, metadata_stderr = batch._summarize_template_log_paths(resolved_templates)
        self._stdout = metadata_stdout
        self._stderr = metadata_stderr
        self._task_kind = task_kind
        self._target_label = target_label
        if batch._task_logs_enabled:
            batch._append_log_manifest_entry(
                tuid=tuid,
                task_kind=task_kind,
                target_label=target_label,
                task_name=name,
                stdout_path=metadata_stdout,
                stderr_path=metadata_stderr,
            )
        # Always track the task so unmanaged-shutdown result invalidation can
        # reach it, even when task logging (and thus the manifest) is disabled.
        batch._register_task_for_log_metadata(self)

        self.core.num_procs = total_procs
        self._handle_arg_passing_deps(list_of_arg_lists)


class Work:
    def __init__(
        self,
        task_set: set,
        client_id: int,
        compiled_tuid: str,
        manager_q: Optional[Queue] = None,
        manager_idx: Optional[int] = None,
    ) -> None:
        """
        Initialize a new work object for a program.

        :param task_set: Set containing all subtasks for this compiled task.
        :type task_set: set
        :param client_id: The id for the batch client creating this work object.
        :type client_id: int
        :param compiled_tuid: The tuid of the compiled task for this work object.
        :type tuid: str
        :param manager_q: Optional queue to send this work to (destination manager queue).
        :type manager_q: Optional[Queue]
        :param manager_idx: Optional logical destination manager index used by the scheduler to route this work.
            Scheduler-owned work uses ``-1``.
        :type manager_idx: Optional[int]

        :return: Returns None.
        :rtype: None
        """
        self._client_id = client_id
        self.manager_q = manager_q
        self.manager_idx = manager_idx

        # divide tasks in this partition into a list of tasks with no dependencies,
        # and a dictionary mapping tuids to tasks

        self._ready_task_cores = {}
        self._blocked_task_cores = {}
        self._compiled_result_wrapper = CompiledResultWrapper(compiled_tuid, {})

        for task_core in task_set:
            if task_core.num_dep_tot == 0:
                self._ready_task_cores[task_core.tuid] = task_core
            else:
                self._blocked_task_cores[task_core.tuid] = task_core


def _is_multi_node_job(task_core: TaskCore, physical_cores_per_node: int) -> bool:
    num_procs = getattr(task_core, "num_procs", 0) or 0
    return isinstance(task_core, JobCore) and num_procs > physical_cores_per_node


def _task_heft_priority_key(task_core: TaskCore) -> tuple[int, int | float, str]:
    """Return the deterministic HEFTY priority key shared by placement and launch ordering."""
    task_tuid = task_core.tuid if task_core.tuid is not None else ""
    return (-task_core.weight, task_core.heft_topo_order, task_tuid)


def _bootstrap_manager(work_q: Queue) -> None:
    """
    Receive manager object from origin and start the manager's work loop.

    :param work_q: Queue on which the manager is received.
    :type work_q: multiprocessing.Queue

    :return: Returns None.
    :rtype: None
    """
    bootstrap_timeout = 30
    manager = work_q.get(bootstrap_timeout)
    manager._run()


def _get_result_tuple(results_ddict: DDict, tuid: str, result_ddict_idx: int) -> tuple:
    with _with_ddict_manager(results_ddict, result_ddict_idx) as manager_results_ddict:
        return manager_results_ddict[tuid]


def _build_cancelled_error(task_core: TaskCore, results_ddict: DDict) -> BatchError:
    if task_core.is_cancelled == TaskCancellationReason.UPSTREAM_RAW_FAILURE:
        source_tuid = task_core.cancel_source_tuid if task_core.cancel_source_tuid is not None else "<unknown>"
        dep_error = ReadAfterWriteDependencyError(
            f"task with {task_core._get_id_str()} could not run because it has a read-after-write dependency "
            f"on upstream task tuid={source_tuid}, and that upstream task failed or was cancelled."
        )

        if task_core.cancel_source_tuid is not None and task_core.cancel_source_result_ddict_idx is not None:
            upstream_result, _, upstream_raised = _get_result_tuple(
                results_ddict,
                task_core.cancel_source_tuid,
                task_core.cancel_source_result_ddict_idx,
            )
            if upstream_raised and isinstance(upstream_result, BaseException):
                raise dep_error from upstream_result

        return dep_error

    return TaskCancelledError(f"task was cancelled before completing: {task_core._get_id_str()}")


def _do_task_impl(task_core: TaskCore, results_ddict: DDict, control_q: Queue) -> TaskComplete:
    """
    Start a specified task and write results directly to the distributed dict.

    :param task_core: The core of the task to start.
    :type task_core: TaskCore
    :param results_ddict: Distributed dict to store results keyed by tuid.
    :type results_ddict: DDict

    :return: Returns task completion metadata for the finished task.
    :rtype: TaskComplete
    """
    tb = None
    raised = False

    try:
        if task_core.is_cancelled != TaskCancellationReason.NONE:
            raise _build_cancelled_error(task_core, results_ddict)
        result = task_core.run(control_q=control_q)
    except Exception as e:
        result = e
        tb = _get_traceback()
        raised = True

    task_core._notify_dep_tasks(result)

    # Write results directly to the task's assigned result ddict manager.
    if task_core.result_ddict_idx is None:
        raise RuntimeError(f"task result_ddict_idx not assigned: {task_core._get_id_str()}")

    with _with_ddict_manager(results_ddict, task_core.result_ddict_idx) as local_results_ddict:
        local_results_ddict[task_core.tuid] = (result, tb, raised)

    return TaskComplete(task_core.tuid, task_core.compiled_tuid, raised)


def _do_task(task_and_args) -> TaskComplete:
    """
    Run one task on a worker.

    Host placement is resolved before dispatch. A JobCore that reaches this
    wrapper without ``hostname_list`` set indicates a manager-side routing bug,
    not something the worker should recover from.
    """
    task_core, args = task_and_args
    results_ddict, control_q = args

    if isinstance(task_core, JobCore) and task_core.hostname_list is None:
        raise RuntimeError(f"job task reached worker without assigned hostnames: {task_core._get_id_str()}")

    tuid_compiled_tuid = _do_task_impl(task_core, results_ddict, control_q=control_q)

    return tuid_compiled_tuid


class Manager:
    def __init__(
        self,
        idx: int,
        num_workers: int,
        work_q: Queue,
        ret_q: Queue,
        results_ddict: DDict,
        pool_node_huids: Optional[list[int]] = None,
        physical_cores_per_node: int = 1,
        disable_telem: bool = False,
        is_scheduler: bool = False,
        all_node_hostnames: Optional[list[str]] = None,
        subnode_manager_qs: Optional[list] = None,
        managed_lifecycle: bool = False,
    ) -> None:
        """
        Initialize a manager.

        :param idx: This manager's logical index. The scheduler uses ``-1`` and
            subnode managers use ``0..n-1``.
        :type idx: int
        :param num_workers: Number of workers for this manager.
        :type num_workers: int
        :param work_q: Queue used by this manager to receive work and completion updates.
        :type work_q: multiprocessing.Queue
        :param ret_q: Qeueue used by this manager to return work to the origin.
        :type ret_q: multiprocessing.Queue
        :param results_ddict: Distributed dict to store task results keyed by tuid.
        :type results_ddict: DDict
        :param pool_node_huids: List of Dragon node h_uids whose cores make up this manager's
            worker pool.  Workers are pinned to these nodes via placement policy.  When
            ``None`` the manager falls back to local placement as a defensive default,
            but normal Batch bringup always provides explicit pool nodes.
        :type pool_node_huids: Optional[list[int]]
        :param physical_cores_per_node: Number of physical CPU cores per node (hyperthreads // 2).
            Used to determine how many workers to launch on each pool node.
        :type physical_cores_per_node: int
        :param disable_telem: Disables telemetry for the managers.
        :type disable_telem: bool
        :param is_scheduler: If True this is the top-level scheduler,
            which handles node allocation, multi-node jobs, and fence coordination.
        :type is_scheduler: bool
        :param all_node_hostnames: Hostnames of all nodes in the Batch allocation.
            Only used when ``is_scheduler=True``.
        :type all_node_hostnames: Optional[list[str]]
        :param subnode_manager_qs: Work queues for logical subnode managers ``0..n-1``.
            Only used when ``is_scheduler=True``.
        :type subnode_manager_qs: Optional[list]

        :return: Returns None.
        :rtype: None
        """
        self.num_workers = num_workers
        self.idx = idx
        self.pool = None
        self.work = None
        self.work_q = work_q
        self.ret_q = {}
        self.results_ddict = results_ddict
        self.cached_queues = []
        self.client_ctr = 0
        self.work_backlog = {}
        self.unexpected_dep_sat = {}
        self.active_clients = set()
        # Per-client mapping of completed subtask tuids to the manager idx that
        # owns their result in results_ddict plus whether the task completed by
        # raising. Used only to answer DepSatRequest queries that arrive after
        # completion without needing another ddict read.
        self._completed_tuids: dict[int, dict[str, CompletedTaskInfo]] = {}
        # Mapping from upstream_tuid -> list of
        # (reply_q, downstream_tuid, downstream_compiled_tuid, arg_dep_updates, is_raw)
        # where reply_q is the queue to notify when upstream_tuid completes.
        self._dep_request_reply_map = {}
        # Number of in-flight Work chunks per client (client_id -> count). Incremented
        # when a Work chunk is accepted by this manager, decremented when it completes.
        self._pending_task_counts = {}
        # Detached clients whose manager-side state should be reaped once all
        # in-flight work and runtime bookkeeping for that client drains.
        self._clients_pending_reap: set[int] = set()
        self._pool_launch_args_list = []
        self._compiled_task_list = []
        self._queued_task_count = 0
        self._async_queue = None
        self.managed_lifecycle = managed_lifecycle
        self.destroy_called = False
        self._force_destroy_deadline: Optional[float] = None
        self.dbg_start_time = None
        self.dbg_update_start = False
        self.dispatch = None
        self.pool_node_huids = pool_node_huids if pool_node_huids is not None else []
        self.physical_cores_per_node = physical_cores_per_node

        # telemetry stuff
        self.dragon_telem = None
        self.disable_telem = disable_telem

        if not disable_telem:
            self.num_running_tasks = 0
            self.num_completed_tasks = 0

        # scheduler (manager 0)
        # ---------------------
        self.is_scheduler = is_scheduler
        # Pool of hostnames available for node allocation (multi-node jobs and
        # subnode manager pool locking). Populated only on the scheduler.
        self._available_nodes: list[str] = sorted(all_node_hostnames) if all_node_hostnames else []
        # Allocation handle counter: incremented for each request_nodes() call.
        self._alloc_handle_ctr: int = 0
        # Maps active allocation handles to their allocated hostname lists.
        # {handle: [hostname, ...]}
        self._alloc_handles: dict[int, list[str]] = {}
        # Pending (deferred) allocation requests that cannot yet be satisfied.
        # {handle: PendingAllocInfo}
        self._pending_alloc_requests: dict[int, "PendingAllocInfo"] = {}
        # Allocation completions produced by free_nodes(); drained each loop iteration.
        # [(handle, [hostname, ...]), ...]
        self._newly_satisfied_allocs: list = []
        # Deferred subnode alloc replies, keyed by allocation handle.
        # {handle: (manager_idx, reply_q)}
        self._subnode_alloc_reply_qs: dict[int, tuple] = {}
        # Maps subnode manager index to its current allocation handle.
        # {manager_idx: handle}
        self._subnode_alloc_handle: dict[int, int] = {}
        # Deferred MNJ submissions waiting for a node allocation.
        # {handle: (task_core, client_id)}
        self._pending_mnj_tasks: dict[int, tuple] = {}
        # Per-client count of subnode Work outstanding from scheduler routing
        # until subnode manager allocation release. This spans the entire lifecycle
        # where fence() must wait for the client to reach quiescence.
        # {client_id: int}
        self._subnode_outstanding: dict[int, int] = {}
        # Per-client count of in-flight multi-node jobs.
        # {client_id: int}
        self._mnj_pending_counts: dict[int, int] = {}
        # Fence requests pending on manager 0. Stored as {client_id: reply_q}.
        self._scheduler_fence_requests: dict[int, Queue] = {}
        # Work queues for subnode managers 1-n (set on scheduler only).
        self._subnode_manager_qs: list = list(subnode_manager_qs) if subnode_manager_qs else []
        # Map tuid -> hostname_list for running multi-node jobs (to return nodes on completion).
        self._mnj_running: dict[str, list[str]] = {}
        # Map tuid -> client_id for running multi-node jobs (to check fences on completion).
        self._mnj_client_ids: dict[str, int] = {}
        # Control queues and reserved-core bookkeeping for tasks that have been
        # submitted to workers and can still observe cancellation.
        self._running_task_state: dict[str, StartedTaskInfo] = {}

        # subnode manager state
        # ---------------------
        # True while this subnode manager holds a node allocation from manager 0.
        self._subnode_has_alloc: bool = False
        # True while a SubnodeAllocRequest is in flight (sent but not yet replied to).
        self._subnode_alloc_pending: bool = False
        # Work chunks covered by the current pending allocation request.
        self._pending_work_chunks: list = []
        # Work chunks that arrived while a subnode allocation request was already in flight.
        self._queued_work_chunks: list = []
        # Number of Work chunks accepted per client during the current allocation
        # phase. This mirrors the scheduler-side _subnode_outstanding unit so
        # fence() accounting can be decremented consistently when the allocation
        # is released.
        self._alloc_phase_client_work_counts: dict[int, int] = {}
        # Task tuids that were accepted for execution during the current allocation phase.
        self._alloc_phase_tuids: set[str] = set()
        # Ready subnode tasks for the current allocation phase, ordered by HEFT priority.
        self._subnode_ready_heap: list[tuple[int, int | float, str, TaskCore]] = []
        # Number of node-local physical cores still available for active
        # subnode work on this manager. This is the shared capacity limiter
        # for both pool-launched tasks and manager-owned direct function
        # threads, so the total active work on the node never exceeds the
        # physical core budget.
        self._subnode_available_cores: int = physical_cores_per_node

    def _request_runtime_shutdown(self, force_timeout: Optional[float] = None) -> None:
        if not self.destroy_called:
            self.log.debug("destroy called")
            self.destroy_called = True

        if force_timeout is not None:
            deadline = time.monotonic() + force_timeout
            if self._force_destroy_deadline is None or deadline < self._force_destroy_deadline:
                self._force_destroy_deadline = deadline

    def _should_exit_runtime(self) -> bool:
        if not self.destroy_called:
            return False

        if self._force_destroy_deadline is not None and time.monotonic() >= self._force_destroy_deadline:
            return True

        return len(self.active_clients) == 0 and len(self.work_backlog) == 0

    def _reap_client_state(self, client_id: int) -> None:
        self.ret_q.pop(client_id, None)
        self._completed_tuids.pop(client_id, None)
        self._pending_task_counts.pop(client_id, None)
        self._clients_pending_reap.discard(client_id)

        if self.is_scheduler:
            self._subnode_outstanding.pop(client_id, None)
            self._mnj_pending_counts.pop(client_id, None)
            self._scheduler_fence_requests.pop(client_id, None)

    def _can_reap_client_state(self, client_id: int) -> bool:
        if client_id in self.active_clients:
            return False

        if self._pending_task_counts.get(client_id, 0) != 0:
            return False

        if self.is_scheduler:
            if self._subnode_outstanding.get(client_id, 0) != 0:
                return False
            if self._mnj_pending_counts.get(client_id, 0) != 0:
                return False
            if client_id in self._scheduler_fence_requests:
                return False

        return True

    def _maybe_reap_client_state(self, client_id: int) -> None:
        if client_id in self._clients_pending_reap and self._can_reap_client_state(client_id):
            self._reap_client_state(client_id)

    def __setstate__(self, state) -> None:
        """
        The manager is sent over a queue to each process in the ProcessGroup, so we create
        the worker pool and do other setup for the manager here.

        :param state: The manager state that's set when it's initially create by the client.

        :return: Returns None.
        :rtype: None
        """
        self.__dict__.update(state)

        # Pool hostnames: used by subnode managers to send hostname-based alloc requests.
        self._pool_hostnames: list[str] = (
            [Node(h).hostname for h in self.pool_node_huids] if self.pool_node_huids else []
        )

        policy_list = []

        if self.is_scheduler:
            # Scheduler manager (manager 0) runs on the client node.  Create
            # one worker per requested scheduler_workers, all on the local node.
            local_hostname = current().hostname
            my_alloc = System()
            node = Node(my_alloc.nodes[0])
            num_gpus = node.num_gpus
            for _ in range(self.num_workers):
                if num_gpus > 0:
                    device_idx = random.randint(0, num_gpus - 1)
                else:
                    device_idx = []
                policy_list.append(
                    Policy(
                        placement=Policy.Placement.HOST_NAME,
                        host_name=local_hostname,
                        gpu_affinity=[device_idx],
                    )
                )
        elif self.pool_node_huids:
            # Pin each worker to one of the designated pool nodes, distributing
            # physical_cores_per_node workers across each node.
            for h_uid in self.pool_node_huids:
                node = Node(h_uid)
                hostname = node.hostname
                num_gpus = node.num_gpus
                for _ in range(self.physical_cores_per_node):
                    if num_gpus > 0:
                        device_idx = random.randint(0, num_gpus - 1)
                    else:
                        device_idx = []
                    policy_list.append(
                        Policy(
                            placement=Policy.Placement.HOST_NAME,
                            host_name=hostname,
                            gpu_affinity=[device_idx],
                        )
                    )
        else:
            # Defensive fallback for non-Batch callers that construct a Manager
            # without explicit pool nodes.
            my_alloc = System()
            node = Node(my_alloc.nodes[0])
            num_gpus = node.num_gpus
            for _ in range(self.num_workers):
                if num_gpus > 0:
                    device_idx = random.randint(0, num_gpus - 1)
                else:
                    device_idx = []
                policy_list.append(Policy(gpu_affinity=[device_idx]))

        self.pool = Pool(policy=policy_list, processes_per_policy=1)
        self._async_queue = LocalQueue()

        self.dbg_start_time = time.time()

        self.log = _setup_logging("manager")
        if self.pool_node_huids:
            pool_hosts = _compress_hostnames([Node(h).hostname for h in self.pool_node_huids])
        else:
            pool_hosts = current().hostname
        self.log.debug(f"manager {self.idx} starting pool: {self.num_workers} worker(s) on {pool_hosts}")

        if not self.disable_telem:
            self.dragon_telem = dt()

    ###############################################
    # Scheduler (manager 0) node allocation helpers
    ###############################################

    def _is_multi_node_job(self, task_core: TaskCore) -> bool:
        return _is_multi_node_job(task_core, self.physical_cores_per_node)

    def _sort_available_nodes(self) -> None:
        """Keep the free-node list in lexicographic order."""
        self._available_nodes.sort()

    def _claim_requested_nodes(self, hostnames: list[str]) -> Optional[list[str]]:
        """Claim a specific set of nodes if they are all currently free."""
        if not all(hostname in self._available_nodes for hostname in hostnames):
            return None

        for hostname in hostnames:
            self._available_nodes.remove(hostname)

        return hostnames

    def _claim_required_and_contiguous_nodes(self, hostnames: list[str], num_nodes: int) -> Optional[list[str]]:
        """Claim required nodes plus enough additional free nodes to reach ``num_nodes``."""
        if num_nodes < len(hostnames):
            raise RuntimeError(
                f"invalid node allocation request: need at least {len(hostnames)} node(s) for explicit hostnames"
            )
        if not all(hostname in self._available_nodes for hostname in hostnames):
            return None

        remaining_available = [hostname for hostname in self._available_nodes if hostname not in hostnames]
        extra_nodes_needed = num_nodes - len(hostnames)
        if len(remaining_available) < extra_nodes_needed:
            return None

        allocated = sorted(hostnames) + remaining_available[:extra_nodes_needed]
        for hostname in allocated:
            self._available_nodes.remove(hostname)

        return allocated

    def _claim_contiguous_nodes(self, num_nodes: int) -> Optional[list[str]]:
        """Claim the first contiguous slice from the free-node list.

        Invariant: ``self._available_nodes`` is kept in lexicographic order.
        It is initialized sorted, resorted in :py:meth:`free_nodes` after
        nodes are returned, and the other mutation paths here only remove
        elements or take ordered slices, which preserve that ordering.
        """
        if len(self._available_nodes) < num_nodes:
            return None

        allocated = self._available_nodes[:num_nodes]
        self._available_nodes = self._available_nodes[num_nodes:]
        return allocated

    def _build_contiguous_hostname_list(self, hostnames: list[str], num_procs: int) -> list[str]:
        """Pack ranks contiguously onto nodes, allowing the final node to be partial."""
        if len(hostnames) == 0:
            raise RuntimeError("cannot assign ranks without allocated hostnames")

        hostname_list = []
        remaining = num_procs
        ranks_per_node = self.physical_cores_per_node

        for hostname in hostnames:
            nranks = min(ranks_per_node, remaining)
            hostname_list.extend([hostname] * nranks)
            remaining -= nranks
            if remaining == 0:
                break

        if remaining != 0:
            raise RuntimeError(
                f"insufficient node allocation for job: need {num_procs} ranks, only placed {num_procs - remaining}"
            )

        return hostname_list

    def _get_subnode_alloc_request_metadata(self, work_chunks: list[Work]) -> tuple[set[int], int]:
        """
        Return the client set and max task weight for a fixed work snapshot.

        The scheduler only needs a scalar priority for subnode allocation
        requests, but the tie-break policy should match the ready-task launch
        order used once the allocation is granted.
        """
        client_ids = {work._client_id for work in work_chunks}
        task_cores = (
            task_core
            for work in work_chunks
            for task_dict in (work._ready_task_cores, work._blocked_task_cores)
            for task_core in task_dict.values()
        )
        best_task_core = min(task_cores, key=_task_heft_priority_key, default=None)

        priority_weight = best_task_core.weight if best_task_core is not None else 0
        return client_ids, priority_weight

    def _send_subnode_alloc_request(self) -> None:
        """Send one allocation request for the current snapshot of pending subnode work."""
        if self._subnode_has_alloc or self._subnode_alloc_pending or len(self._pending_work_chunks) == 0:
            return

        scheduler_q = self._subnode_manager_qs[0] if self._subnode_manager_qs else None
        if scheduler_q is None:
            return

        client_ids, priority_weight = self._get_subnode_alloc_request_metadata(self._pending_work_chunks)
        scheduler_q.put(
            SubnodeAllocRequest(
                self.idx,
                self._pool_hostnames,
                client_ids,
                self.work_q,
                priority_weight,
            )
        )
        self._subnode_alloc_pending = True
        self.log.debug(f"subnode manager {self.idx}: SubnodeAllocRequest sent with priority_weight={priority_weight}")

    def _pending_alloc_sort_key(self, handle: int) -> tuple[int, int]:
        """Sort deferred alloc requests by descending priority weight, then FIFO."""
        request = self._pending_alloc_requests[handle]
        priority = request.priority_weight
        return (-priority, handle)

    def _get_job_requested_hostnames(self, task_core: "TaskCore") -> Optional[list[str]]:
        """Return the distinct explicit hostnames requested by a job's templates, if any."""
        if not isinstance(task_core, JobCore):
            return None

        requested_hostnames: list[str] = []
        for _, template in task_core.process_templates:
            policy = template.policy
            if policy is None or policy.placement != Policy.Placement.HOST_NAME:
                continue

            host_name = policy.host_name
            if not host_name:
                raise RuntimeError(
                    "invalid process template policy: placement=HOST_NAME requires a non-empty host_name"
                )

            if host_name not in requested_hostnames:
                requested_hostnames.append(host_name)

        if not requested_hostnames:
            return None

        return sorted(requested_hostnames)

    def _validate_job_requested_hostnames(self, hostnames: list[str]) -> None:
        """Reject hostname requests that cannot be satisfied by this Batch allocation."""
        if any(hostname not in self._all_node_hostnames for hostname in hostnames):
            invalid = [hostname for hostname in hostnames if hostname not in self._all_node_hostnames]
            invalid_str = ", ".join(sorted(invalid))
            raise RuntimeError(f"job requested hostnames outside this Batch allocation: {invalid_str}")

    def _submit_mnj_task(self, task_core: "TaskCore", client_id: int, hostnames: list[str]) -> None:
        """Queue a multi-node job after its nodes have been allocated."""
        task_core.hostname_list = self._build_contiguous_hostname_list(hostnames, task_core.num_procs)
        self._mnj_running[task_core.tuid] = hostnames
        self._mnj_client_ids[task_core.tuid] = client_id
        cancel_q = self._get_queue_for_task(task_core)
        self._register_running_task(task_core, cancel_q)
        self._queue_task_for_launch(task_core, cancel_q)
        self._queued_task_count += 1

    def _build_alloc_request(
        self,
        num_nodes: int = 0,
        hostnames: Optional[list[str]] = None,
        client_ids: Optional[set[int]] = None,
        priority_weight: int = 0,
    ) -> PendingAllocInfo:
        """Normalize a node-allocation request into the shared internal form."""
        frozen_client_ids = frozenset(client_ids) if client_ids else frozenset()
        if hostnames is not None:
            # An explicit hostname set is a hard constraint, so the allocation
            # must cover at least those nodes; honor a larger num_nodes request
            # by adding extra free nodes on top of the required hosts.
            return PendingAllocInfo(
                hostnames=sorted(hostnames),
                num_nodes=max(num_nodes, len(hostnames)),
                client_ids=frozen_client_ids,
                priority_weight=priority_weight,
            )

        return PendingAllocInfo(
            hostnames=None,
            num_nodes=num_nodes,
            client_ids=frozen_client_ids,
            priority_weight=priority_weight,
        )

    def _try_allocate_request(self, request: PendingAllocInfo) -> Optional[list[str]]:
        """Attempt to satisfy a normalized allocation request from the free-node pool."""
        if request.hostnames is not None:
            if request.num_nodes == len(request.hostnames):
                return self._claim_requested_nodes(request.hostnames)

            return self._claim_required_and_contiguous_nodes(request.hostnames, request.num_nodes)

        return self._claim_contiguous_nodes(request.num_nodes)

    def _record_allocation(self, handle: int, hostnames: list[str], deferred: bool = False) -> None:
        """Record a satisfied allocation and optionally queue it for deferred processing."""
        self._alloc_handles[handle] = hostnames
        if deferred:
            self._newly_satisfied_allocs.append((handle, hostnames))

    def request_nodes(
        self,
        num_nodes: int = 0,
        hostnames: Optional[list[str]] = None,
        client_ids: Optional[set[int]] = None,
        priority_weight: int = 0,
    ) -> int:
        """
        Request an allocation of nodes, returning an integer handle immediately.
        If *hostnames* is given, those specific nodes are always included in the
        allocation and additional nodes are chosen from the front of
        ``_available_nodes`` when *num_nodes* exceeds ``len(hostnames)``.
        Otherwise *num_nodes* arbitrary nodes are requested from the front of
        ``_available_nodes``. If the requested nodes are currently available they
        are removed from ``_available_nodes`` and stored in
        ``_alloc_handles[handle]``. If not, the request is queued in
        ``_pending_alloc_requests`` and ``free_nodes`` will satisfy it later,
        appending the result to ``_newly_satisfied_allocs``.

        :param num_nodes: Number of nodes for a count-based request, or the total
            node count when *hostnames* must be included.
        :type num_nodes: int
        :param hostnames: Specific hostnames for a hostname-based request.
        :type hostnames: Optional[list[str]]
        :param client_ids: Clients whose work is covered by this allocation request.
        :type client_ids: Optional[set[int]]
        :param priority_weight: Priority weight used to order deferred requests.
        :type priority_weight: int

        :return: Allocation handle.
        :rtype: int
        """
        handle = self._alloc_handle_ctr
        self._alloc_handle_ctr += 1

        request = self._build_alloc_request(
            num_nodes=num_nodes,
            hostnames=hostnames,
            client_ids=client_ids,
            priority_weight=priority_weight,
        )
        allocated = self._try_allocate_request(request)
        if allocated is not None:
            self._record_allocation(handle, allocated)
        else:
            self._pending_alloc_requests[handle] = request

        return handle

    def free_nodes(self, hostnames: list[str]) -> None:
        """
        Return previously-allocated nodes to the available pool and satisfy any
        pending allocation requests that can now be served. Satisfied requests are
        appended to ``_newly_satisfied_allocs``; the main loop processes them via
        ``_process_alloc_completions``.

        :param hostnames: Hostnames to return.
        :type hostnames: list[str]
        """
        self._available_nodes.extend(hostnames)
        self._sort_available_nodes()

        satisfied = []
        for handle in sorted(self._pending_alloc_requests, key=self._pending_alloc_sort_key):
            request = self._pending_alloc_requests[handle]
            allocated = self._try_allocate_request(request)
            if allocated is not None:
                self._record_allocation(handle, allocated, deferred=True)
                satisfied.append(handle)

        for handle in satisfied:
            del self._pending_alloc_requests[handle]

    def _try_satisfy_scheduler_fence(self, client_id: int) -> None:
        """
        Check whether the fence for *client_id* can be satisfied on manager 0.
        A scheduler fence is complete when:
          (1) All multi-node jobs for the client have completed.
          (2) No subnode Work is outstanding (routed but not yet released by a
              subnode manager's allocation).

        If all conditions hold and a fence is pending, send ``FenceComplete``
        to the client's return queue.
        """
        if client_id not in self._scheduler_fence_requests:
            return

        mnj_done = self._mnj_pending_counts.get(client_id, 0) == 0
        subnode_outstanding = self._subnode_outstanding.get(client_id, 0) == 0

        if mnj_done and subnode_outstanding:
            reply_q = self._scheduler_fence_requests.pop(client_id)

            # Fence boundary: clear per-client completion/dependency state on all managers
            # before acknowledging fence completion to the client.
            for subnode_q in self._subnode_manager_qs:
                subnode_q.put(ClearFenceState(client_id))

            self._completed_tuids.pop(client_id, None)

            reply_q.put(FenceComplete(client_id))
            self.log.debug(f"scheduler fence complete for client {client_id}")

    def _process_alloc_completions(self) -> None:
        """
        Drain ``_newly_satisfied_allocs`` and act on each completed allocation.
        Called each iteration of the scheduler's ``_run`` loop after draining
        the work queue.

        - For subnode alloc handles: send ``SubnodeAllocResponse`` to the waiting
          subnode manager.
        - For MNJ alloc handles: submit the deferred task to the worker pool.
        """
        if not self._newly_satisfied_allocs:
            return

        for handle, hostnames in self._newly_satisfied_allocs:
            if handle in self._subnode_alloc_reply_qs:
                manager_idx, reply_q = self._subnode_alloc_reply_qs.pop(handle)
                self._subnode_alloc_handle[manager_idx] = handle
                reply_q.put(SubnodeAllocResponse(manager_idx))
                self.log.debug(f"scheduler: deferred SubnodeAllocResponse sent to manager {manager_idx}")
            elif handle in self._pending_mnj_tasks:
                task_core, client_id = self._pending_mnj_tasks.pop(handle)
                self._submit_mnj_task(task_core, client_id, hostnames)
                self.log.debug(f"scheduler: deferred MNJ submitted tuid={task_core.tuid}")

        self._newly_satisfied_allocs = []

    def _handle_subnode_alloc_request(self, req: SubnodeAllocRequest) -> None:
        """
        Handle a ``SubnodeAllocRequest`` from a subnode manager.  Calls
        ``request_nodes`` with the manager's pool hostnames.  If the allocation
        is immediately satisfied, sends ``SubnodeAllocResponse`` right away.
        Otherwise the reply is deferred to ``_process_alloc_completions``.

        Work accounting for these client_ids already began when the scheduler
        routed subnode Work chunks to this manager; no additional accounting
        is needed at allocation request time.

        :param req: The allocation request.
        :type req: SubnodeAllocRequest
        """
        manager_idx = req.manager_idx
        self.log.debug(f"scheduler: SubnodeAllocRequest from manager {manager_idx}, clients={req.client_ids}")

        handle = self.request_nodes(
            hostnames=req.hostnames,
            client_ids=req.client_ids,
            priority_weight=req.priority_weight,
        )
        if handle in self._alloc_handles:
            # Immediately satisfied: reply now.
            self._subnode_alloc_handle[manager_idx] = handle
            req.reply_q.put(SubnodeAllocResponse(manager_idx))
            self.log.debug(f"scheduler: immediate SubnodeAllocResponse sent to manager {manager_idx}")
        else:
            # Deferred: store reply info keyed by handle.
            self._subnode_alloc_reply_qs[handle] = (manager_idx, req.reply_q)

    def _handle_subnode_free_request(self, req: SubnodeFreeRequest) -> None:
        """
        Handle a ``SubnodeFreeRequest`` from a subnode manager.  Looks up the
        current allocation handle for this manager, returns the associated nodes
        to the available pool (potentially satisfying other pending requests),
        and decrements outstanding work count for each client that was active
        during this allocation phase.

        :param req: The free request.
        :type req: SubnodeFreeRequest
        """
        manager_idx = req.manager_idx
        client_work_counts = req.client_work_counts
        task_tuids = req.task_tuids
        self.log.debug(
            f"scheduler: SubnodeFreeRequest from manager {manager_idx}, clients={client_work_counts}, num tasks={len(task_tuids)}"
        )

        handle = self._subnode_alloc_handle.pop(manager_idx, None)
        if handle is not None:
            hostnames = self._alloc_handles.pop(handle, [])
            self.free_nodes(hostnames)

        # Decrement the outstanding work counter for each client whose work
        # was covered by this allocation. This completes the Work lifecycle
        # that began when the scheduler routed those chunks.
        for client_id, work_count in client_work_counts.items():
            new_count = self._subnode_outstanding.get(client_id, 0) - work_count
            if new_count <= 0:
                self._subnode_outstanding.pop(client_id, None)
            else:
                self._subnode_outstanding[client_id] = new_count
            self._try_satisfy_scheduler_fence(client_id)
            self._maybe_reap_client_state(client_id)

    def _handle_scheduler_fence_request(self, fence_request: FenceRequest) -> None:
        """
        Handle a fence request on the scheduler (manager 0).

        A scheduler fence completes only after manager 0 has observed all
        of its client-scoped scheduler states reach quiescence: no pending
        multi-node jobs, no scheduler-routed subnode Work chunks awaiting
        completion, and no outstanding subnode allocations for the client.

        :param fence_request: Contains the client_id and the queue to reply on.
        :type fence_request: FenceRequest
        """
        client_id = fence_request.client_id
        reply_q = fence_request.reply_q
        self.log.debug(f"scheduler: received fence request for client {client_id}")

        self._scheduler_fence_requests[client_id] = reply_q
        self._try_satisfy_scheduler_fence(client_id)

    def _handle_mnj_complete(self, mnj: MultiNodeJobComplete) -> None:
        """
        Handle a ``MultiNodeJobComplete`` notification (manager 0 only).
        Returns the allocated nodes and checks for pending fences.

        :param mnj: Completion sentinel for a multi-node job.
        :type mnj: MultiNodeJobComplete
        """
        tuid = mnj.tuid
        compiled_tuid = mnj.compiled_tuid
        hostnames = mnj.node_hostnames

        self.log.debug(f"scheduler: multi-node job complete tuid={tuid}, freeing {len(hostnames)} node(s)")

        # Return nodes to the available pool.
        self.free_nodes(hostnames)
        self._mnj_running.pop(tuid, None)

        # Update in-flight count for the owning client.
        # The Work object for this compiled task was already removed from
        # work_backlog by _handle_results; we need the client_id from there.
        # We stored it in _mnj_client_ids during _handle_mnj_task.
        client_id = self._mnj_client_ids.pop(tuid, None)
        if client_id is not None:
            new_count = self._mnj_pending_counts.get(client_id, 1) - 1
            if new_count <= 0:
                self._mnj_pending_counts.pop(client_id, None)
            else:
                self._mnj_pending_counts[client_id] = new_count
            self._try_satisfy_scheduler_fence(client_id)
            self._maybe_reap_client_state(client_id)

    def _request_mnj_alloc(self, task_core: "TaskCore", client_id: int) -> None:
        """
        Request a node allocation for a ready multi-node job and, if immediately
        granted, submit the job to the worker pool.  If deferred, store the job
        in ``_pending_mnj_tasks`` to be submitted by ``_process_alloc_completions``.

        Jobs with explicit ``HOST_NAME`` placement in their process-template
        policies treat those hostnames as hard allocation constraints. Batch
        validates that the requested hosts belong to its managed allocation and
        reserves those nodes before launch, adding extra nodes only if the job
        needs more capacity than the explicit host set provides.

        ``_mnj_pending_counts`` is incremented immediately so fence checks
        correctly see the job as in-flight even when the allocation is deferred.

        :param task_core: JobCore for the multi-node job.
        :param client_id: Client that owns this task.
        """
        # Track in-flight MNJ count immediately for fence purposes.
        self._mnj_pending_counts[client_id] = self._mnj_pending_counts.get(client_id, 0) + 1

        requested_hostnames = self._get_job_requested_hostnames(task_core)
        if requested_hostnames is not None:
            self._validate_job_requested_hostnames(requested_hostnames)
            handle = self.request_nodes(
                hostnames=requested_hostnames,
                client_ids={client_id},
                priority_weight=task_core.weight,
            )
        else:
            num_nodes = math.ceil(task_core.num_procs / self.physical_cores_per_node)
            handle = self.request_nodes(num_nodes=num_nodes, client_ids={client_id}, priority_weight=task_core.weight)

        if handle in self._alloc_handles:
            # Immediately satisfied: submit to pool now.
            hostnames = self._alloc_handles[handle]
            self._submit_mnj_task(task_core, client_id, hostnames)
            self.log.debug(f"scheduler: immediate MNJ submission tuid={task_core.tuid}")
        else:
            # Deferred: wait for nodes to become available.
            self._pending_mnj_tasks[handle] = (task_core, client_id)
            self.log.debug(f"scheduler: deferred MNJ tuid={task_core.tuid}, handle={handle}")

    def _handle_subnode_alloc_response(self, resp: SubnodeAllocResponse) -> None:
        """
        Handle a ``SubnodeAllocResponse`` on a subnode manager.  Records that the
        allocation has been granted and processes all work chunks that were queued
        while waiting for the allocation.

        :param resp: The allocation response from manager 0.
        :type resp: SubnodeAllocResponse
        """
        self._subnode_has_alloc = True
        self._subnode_alloc_pending = False
        if self._queued_work_chunks:
            # Work that arrived while the allocation request was in flight must
            # be processed under this grant as well. Leaving it parked until a
            # later allocation phase can deadlock when currently blocked work is
            # waiting on one of those queued tasks to run.
            self._pending_work_chunks.extend(self._queued_work_chunks)
            self._queued_work_chunks = []
        self.log.debug(
            f"subnode manager {self.idx}: allocation granted, processing {len(self._pending_work_chunks)} queued chunk(s)"
        )
        pending = self._pending_work_chunks
        self._pending_work_chunks = []
        for work in pending:
            self._handle_compiled_task(work)

    def _mark_task_cancelled(self, task_core: TaskCore) -> bool:
        if task_core.is_cancelled == TaskCancellationReason.NONE:
            task_core.is_cancelled = TaskCancellationReason.USER_REQUESTED
        return True

    def _find_waiting_task_in_work_chunks(self, tuid: str, work_chunks: list[Work]) -> Optional[TaskCore]:
        for work in work_chunks:
            task_core = work._blocked_task_cores.get(tuid)
            if task_core is not None:
                return task_core

            task_core = work._ready_task_cores.get(tuid)
            if task_core is not None:
                return task_core

        return None

    def _task_is_queued_for_launch(self, tuid: str) -> bool:
        if any(task_core.tuid == tuid for task_core, _ in self._pool_launch_args_list):
            return True

        return False

    def _task_is_queued_in_ready_heap(self, tuid: str) -> bool:
        return any(task_core.tuid == tuid for _, _, _, task_core in self._subnode_ready_heap)

    def _handle_cancel_request(self, req: CancelRequest) -> None:
        tuid = req.tuid

        if self.is_scheduler and req.manager_idx not in (None, SCHEDULER_MANAGER_IDX):
            if req.manager_idx < 0 or req.manager_idx >= len(self._subnode_manager_qs):
                req.reply_q.put(CancelResponse(tuid, False))
                return

            # Forward subnode-owned cancellation to the manager that holds the
            # pre-launch state. Sending it from the scheduler preserves queue
            # ordering relative to the preceding Work dispatch.
            self._subnode_manager_qs[req.manager_idx].put(req)
            return

        # Completed tasks cannot be cancelled.
        completed_task_info = self._completed_tuids.get(req.client_id, {})
        if tuid in completed_task_info:
            req.reply_q.put(CancelResponse(tuid, False))
            return

        # Ready/blocked tasks are still cancellable only while they are waiting
        # in manager-owned state: either parked in a subnode allocation queue,
        # waiting in a Work backlog entry, queued for worker launch, staged in
        # the subnode ready heap, or waiting for MNJ node allocation.
        task_core = self._find_waiting_task_in_work_chunks(tuid, self._pending_work_chunks)
        if task_core is not None:
            req.reply_q.put(CancelResponse(tuid, self._mark_task_cancelled(task_core)))
            return

        task_core = self._find_waiting_task_in_work_chunks(tuid, self._queued_work_chunks)
        if task_core is not None:
            req.reply_q.put(CancelResponse(tuid, self._mark_task_cancelled(task_core)))
            return

        queued_for_launch = self._task_is_queued_for_launch(tuid)
        pending_mnj_alloc = any(task_core.tuid == tuid for task_core, _ in self._pending_mnj_tasks.values())
        queued_in_ready_heap = self._task_is_queued_in_ready_heap(tuid)

        for work in self.work_backlog.values():
            task_core = work._blocked_task_cores.get(tuid)
            if task_core is not None:
                req.reply_q.put(CancelResponse(tuid, self._mark_task_cancelled(task_core)))
                return

            task_core = work._ready_task_cores.get(tuid)
            if task_core is not None and (pending_mnj_alloc or queued_for_launch or queued_in_ready_heap):
                req.reply_q.put(CancelResponse(tuid, self._mark_task_cancelled(task_core)))
                return

        # Tasks already submitted to workers are cancelled via their per-task
        # control queue. If the task completes first, queue recycling drains
        # any stale cancel request before the queue is reused.
        started_task = self._running_task_state.get(tuid)
        if started_task is not None:
            started_task.control_q.put(CancelJob())
            req.reply_q.put(CancelResponse(tuid, True))
            return

        # Everything else has already started and is best-effort only.
        req.reply_q.put(CancelResponse(tuid, False))

    def _is_active_client(self, client_id: int) -> None:
        """
        Checks if the client is active, and puts an exception on the return queue if it is not.

        :param client_id: The id for the client.
        :type client_id: int

        :raises RuntimeError: If the client is not active.

        :return: Returns None.
        :rtype: None
        """
        if client_id not in self.active_clients:
            raise SubmitAfterCloseError("new task received after batch server closed")

    def _get_queue_for_task(self, task_core: TaskCore) -> Queue:
        """
        Get a queue for a task that needs a way for the manager to send messages
        directly to it.

        :param task_core: The core of the task that needs a queue for direct communication.
        :type task_core: TaskCore

        :return: Returns the queue.
        :rtype: Queue
        """
        if task_core.cached_queue is None:
            task_core.cached_queue = self._borrow_cached_queue()

        return task_core.cached_queue

    def _borrow_cached_queue(self) -> Queue:
        """Borrow a manager-local queue, reusing a drained queue when possible."""
        try:
            return self.cached_queues.pop()
        except IndexError:
            return Queue(
                block_size=default_block_size,
                policy=Policy(
                    placement=Policy.Placement.HOST_NAME,
                    host_name=current().hostname,
                ),
            )

    def _subnode_cores_for_task(self, task_core: TaskCore) -> int:
        if isinstance(task_core, JobCore):
            return task_core.num_procs

        return 1

    def _subnode_has_capacity_for_task(self, task_core: TaskCore) -> bool:
        """Return True when the shared node-local core budget can admit this task."""
        return self._subnode_cores_for_task(task_core) <= self._subnode_available_cores

    def _register_running_task(self, task_core: TaskCore, control_q: Queue, reserved_cores: int = 0) -> None:
        self._running_task_state[task_core.tuid] = StartedTaskInfo(control_q, reserved_cores)
        if reserved_cores > 0:
            self._subnode_available_cores -= reserved_cores

    def _release_running_task(self, tuid: str) -> None:
        started_task = self._running_task_state.pop(tuid, None)
        if started_task is not None and started_task.reserved_cores > 0:
            self._subnode_available_cores += started_task.reserved_cores

    def _recycle_queue(self, queue_to_recycle: Queue) -> None:
        """Drain and cache a queue for reuse by a later task."""
        try:
            while True:
                queue_to_recycle.get_nowait()
        except queue.Empty:
            pass

        self.cached_queues.append(queue_to_recycle)

    def _setup_subnode_task(self, task_core: TaskCore) -> None:
        """Queue a subnode-owned task with a per-task control queue.

        All non-multi-node work runs through this path, including node-local
        jobs and single-process ``process()`` tasks.
        """
        reserved_cores = self._subnode_cores_for_task(task_core)
        control_q = self._get_queue_for_task(task_core)
        self._register_running_task(task_core, control_q, reserved_cores=reserved_cores)

        if isinstance(task_core, JobCore):
            task_core.hostname_list = [current().hostname] * reserved_cores

        self._queue_task_for_launch(task_core, control_q)
        self._queued_task_count += 1

    def _queue_task_for_launch(self, task_core: TaskCore, control_q: Queue) -> None:
        """Append task launch args to the worker pool.

        Core budget was already reserved by ``_register_running_task``, so this
        simply queues the task for dispatch via map_async.
        """
        task_and_args = (task_core, (self.results_ddict, control_q))
        self._pool_launch_args_list.append(task_and_args)

    def _prepare_task_for_launch(self, task_core: TaskCore) -> None:
        """
        Prepare a ready task for launch on this manager.

        Scheduler-owned multi-node jobs are routed into the allocation path.
        Subnode-owned tasks go through local setup, core reservation, and then
        backend-specific launch queueing.

        :param task_core: The core of the task being prepared for launch.
        :type task_core: TaskCore

        :return: Returns None.
        :rtype: None
        """
        if self.is_scheduler:
            # Only multi-node jobs should reach the scheduler.
            if not self._is_multi_node_job(task_core):
                raise RuntimeError(f"scheduler received non-multi-node task unexpectedly: {task_core._get_id_str()}")

            self._request_mnj_alloc(task_core, task_core.client_id)
        else:
            self._setup_subnode_task(task_core)
            self._queued_task_count += 1

    def _enqueue_ready_task(self, task_core: TaskCore) -> None:
        """
        Queue a ready task for launch on this manager.

        Subnode managers stage ready work in a HEFT-ordered heap so each
        allocation phase prepares the highest-priority tasks first.
        """
        if self.is_scheduler:
            self._prepare_task_for_launch(task_core)
        else:
            self._queue_subnode_ready_task(task_core)

    def _queue_subnode_ready_task(self, task_core: TaskCore) -> None:
        """Insert a ready subnode task into the current allocation phase heap."""
        heapq.heappush(self._subnode_ready_heap, (*_task_heft_priority_key(task_core), task_core))

    def _drain_subnode_ready_tasks(self) -> None:
        """Materialize the current phase's HEFT order into launch-ready queues."""
        # Tasks that exceed the current local core budget stay in the heap for
        # the next allocation phase, so we preserve the existing HEFT order
        # without oversubscribing the node.
        deferred_tasks = []

        while self._subnode_ready_heap:
            item = heapq.heappop(self._subnode_ready_heap)
            task_core = item[-1]

            if self._subnode_has_capacity_for_task(task_core):
                self._prepare_task_for_launch(task_core)
            else:
                deferred_tasks.append(item)

        for item in deferred_tasks:
            heapq.heappush(self._subnode_ready_heap, item)

    def _handle_manager_exception(
        self,
        e: Exception,
        err_msg: str,
        ret_q: Optional[Queue] = None,
        tuid: Optional[str] = None,
    ) -> None:
        """
        Handle a general "manager exception" unrelated to any task.

        :param e: The exception to be returned to the requesting client.
        :type e: Exception
        :param err_msg: The error message to be returned to the requesting client.
        :type err_msg: str

        :return: Returns None.
        :rtype: None
        """
        self.log.debug(f"error associated with task {tuid} (exception below): {err_msg}\n\n{e}")

    def _handle_compiled_task(self, work: Work) -> None:
        """
        Handle a request to run a compiled task in this manager's worker pool.

        :param work: The work chunk for the compiled task handled by this manager.
        :type work: Work

        :return: Returns None.
        :rtype: None
        """
        self.log.debug(
            f"received work from client {work._client_id}: {len(work._ready_task_cores)} ready tasks, {len(work._blocked_task_cores)} blocked tasks"
        )

        # Work that was already submitted before a client closes must still be
        # accepted and completed, so only reject truly unknown clients.
        if work._client_id not in self.ret_q:
            raise RuntimeError(f"received work for unknown client_id={work._client_id}")

        if not self.is_scheduler:
            if len(work._ready_task_cores) == 0 and len(work._blocked_task_cores) == 0:
                return

            # Subnode managers must hold a node allocation before processing work.
            # Once a request is in flight, its work snapshot is fixed; later arrivals
            # wait for the next allocation phase.
            if not self._subnode_has_alloc:
                if self._subnode_alloc_pending:
                    self._queued_work_chunks.append(work)
                else:
                    self._pending_work_chunks.append(work)
                    self._send_subnode_alloc_request()
                return

        # count this Work chunk as in-flight on this manager; subnode managers use
        # this for allocation-phase bookkeeping and state cleanup.
        client_id = work._client_id
        self._pending_task_counts[client_id] = self._pending_task_counts.get(client_id, 0) + 1
        self._alloc_phase_client_work_counts[client_id] = self._alloc_phase_client_work_counts.get(client_id, 0) + 1
        self._alloc_phase_tuids.update(task_core.tuid for task_core in work._ready_task_cores.values())
        self._alloc_phase_tuids.update(task_core.tuid for task_core in work._blocked_task_cores.values())

        compiled_tuid = work._compiled_result_wrapper.tuid
        self.work_backlog[compiled_tuid] = work

        for _, task_core in work._ready_task_cores.items():
            self._enqueue_ready_task(task_core)

        for _, task_core in work._blocked_task_cores.items():
            # send DepSatRequest messages to the managers that own each upstream dependency
            for upstream_route in task_core.upstream_routes:
                upstream_tuid = upstream_route.upstream_tuid
                arg_dep_updates = upstream_route.arg_dep_updates if upstream_route.arg_dep_updates is not None else []

                # reply queue for DepSat is this manager's work_q so DepSat messages
                # arrive as regular work-queue items and are handled by _handle_dep_sat
                dep_sat_req = DepSatRequest(
                    upstream_tuid,
                    task_core.tuid,
                    task_core.compiled_tuid,
                    arg_dep_updates,
                    self.work_q,
                    work._client_id,
                    upstream_route.is_raw,
                )

                upstream_route.queue.put(dep_sat_req)

        self._compiled_task_list.append((work._client_id, compiled_tuid))

        # A DepSat message for a task in this compiled task can arrive before the Work chunk
        # for the compiled task itself does. When that happens, `_handle_dep_sat` cannot find
        # the compiled_tuid in `work_backlog` and stashes the message in `unexpected_dep_sat`.
        # Now that we've added the work to the backlog, drain any stashed messages so those
        # dependencies are correctly accounted for.
        try:
            # if there aren't any unexpected DepSat messages waiting to be processed,
            # this will throw an exception
            dep_sat_list = self.unexpected_dep_sat[compiled_tuid]

            for dep_sat in dep_sat_list:
                self._handle_dep_sat(dep_sat)

            del self.unexpected_dep_sat[compiled_tuid]
        except:
            pass

    def _update_task_args(
        self,
        task_core: TaskCore,
        arg_dep_updates: list[ArgDepUpdate],
        source_tuid: Optional[str] = None,
        source_result_ddict_idx: Optional[int] = None,
    ) -> None:
        """
        Update task args using dep_sat.arg_dep_updates. ArgDepUpdate contains only
        indices; the actual value is fetched from results_ddict using source_tuid.
        """
        if len(arg_dep_updates) == 0:
            return

        # fetch the result value from the distributed dict
        if source_tuid is not None:
            if source_result_ddict_idx is None:
                raise RuntimeError(f"missing result ddict index for upstream task {source_tuid}")
            try:
                result_tuple = _get_result_tuple(self.results_ddict, source_tuid, source_result_ddict_idx)
                new_arg = result_tuple[0]
            except Exception:
                raise
        else:
            return

        if isinstance(task_core, FunctionCore):
            args_list = list(task_core.args)

            for template_idx, arg_idx in arg_dep_updates:
                args_list[arg_idx] = new_arg

            task_core.args = tuple(args_list)
        else:
            # JobCore: map template_idx -> list of (new_arg, arg_idx)
            template_idx_to_new_args = {}

            for template_idx, arg_idx in arg_dep_updates:
                if template_idx not in template_idx_to_new_args:
                    template_idx_to_new_args[template_idx] = []

                template_idx_to_new_args[template_idx].append((new_arg, arg_idx))

            for template_idx, nprocs_and_template in enumerate(task_core.process_templates):
                _, process_template = nprocs_and_template
                new_args_list = template_idx_to_new_args.get(template_idx, [])

                # If the process template represents a Python callable, the
                # serialized payload is stored in `argdata`. Updating
                # `process_template.args` alone is insufficient because
                # `Process.from_template` for Python targets uses
                # `get_original_python_parameters()` (which reads `argdata`) to
                # reconstruct the callable and its arguments. Replace the
                # relevant arguments in the original payload and reserialize
                # back into `argdata` so workers receive the updated values.
                if getattr(process_template, "is_python", False):
                    orig_target, orig_args, orig_kwargs = process_template.get_original_python_parameters()
                    wrapped_payload = _unwrap_prefixed_python_process_payload(orig_target, orig_args, orig_kwargs)

                    if wrapped_payload is not None:
                        wrapped_target, wrapped_args, wrapped_kwargs = wrapped_payload
                        args_list = list(wrapped_args)
                        for new_arg_val, arg_idx in new_args_list:
                            args_list[arg_idx] = new_arg_val

                        process_template.argdata = cloudpickle.dumps(
                            (orig_target, (wrapped_target, tuple(args_list), wrapped_kwargs), orig_kwargs)
                        )
                    else:
                        args_list = list(orig_args)
                        for new_arg_val, arg_idx in new_args_list:
                            args_list[arg_idx] = new_arg_val

                        # For Python-callable templates, `process_template.args` holds
                        # the subprocess CLI flags (["-c", "..."]) and must NOT be
                        # modified. Only `argdata` (the cloudpickled user payload) needs
                        # to be updated with the resolved dependency values.
                        process_template.argdata = cloudpickle.dumps((orig_target, tuple(args_list), orig_kwargs))
                else:
                    args_list = list(process_template.args)

                    for new_arg_val, arg_idx in new_args_list:
                        args_list[arg_idx] = new_arg_val

                    process_template.args = tuple(args_list)

    def _handle_dep_sat(self, dep_sat: DepSat) -> None:
        """
        Handle a "dependency satisfied" message either by updating the number of satisfied
        dependencies for the task specified by ``dep_sat.tuid`` and ``dep_sat.compiled_tuid``,
        or adding ``dep_sat`` to a list of unexpected messages for this work item.

        :param dep_sat: Contains the ``tuid`` and ``compiled_tuid`` for the satisfied dependency.
        :type dep_sat: DepSat

        :return: Returns None.
        :rtype: None
        """
        source_tuid = dep_sat.source_tuid
        source_manager_idx = dep_sat.source_manager_idx
        source_result_ddict_idx = dep_sat.source_result_ddict_idx
        tuid = dep_sat.tuid
        compiled_tuid = dep_sat.compiled_tuid
        cancel_reason = dep_sat.cancel_reason

        try:
            # if the try succeeds, we have a compiled task that we are already working on
            work = self.work_backlog[compiled_tuid]

            # update the number of satisfied dependencies for this individual task
            # (which is part of a larger compiled task)
            task_core = work._blocked_task_cores[tuid]

            self._update_task_args(task_core, dep_sat.arg_dep_updates, source_tuid, source_result_ddict_idx)
            if cancel_reason == TaskCancellationReason.UPSTREAM_RAW_FAILURE:
                if task_core.is_cancelled == TaskCancellationReason.NONE:
                    task_core.is_cancelled = cancel_reason
                    task_core.cancel_source_tuid = source_tuid
                    task_core.cancel_source_manager_idx = source_manager_idx
                    task_core.cancel_source_result_ddict_idx = source_result_ddict_idx
            task_core.num_dep_sat += 1

            self.log.debug(
                f"received update for task with {task_core._get_id_str()} about a satisfied dependency: satisfied={task_core.num_dep_sat}, total={task_core.num_dep_tot}"
            )
        except Exception:
            # this manager received a "dependency satisfied" message for a compiled task
            # before receiving the task, so add dep_sat to a list of unexpected messages
            self.log.debug(
                f"received unexpected update for task with {compiled_tuid=} and {tuid=} about a satisfied dependency"
            )

            try:
                dep_sat_list = self.unexpected_dep_sat[compiled_tuid]
                dep_sat_list.append(dep_sat)
            except Exception:
                self.unexpected_dep_sat[compiled_tuid] = [dep_sat]
            return

        # Once every dependency is satisfied, move the task into the ready set
        # and queue it for launch using the manager's normal ready-task path.
        if task_core.num_dep_sat == task_core.num_dep_tot:
            self._enqueue_ready_task(task_core)

            work._ready_task_cores[tuid] = task_core
            del work._blocked_task_cores[tuid]

            self.log.debug(f"number of remaining tasks to be started={len(work._blocked_task_cores)}")

    def _handle_dep_sat_request(self, dep_sat_request: DepSatRequest):
        try:
            upstream_tuid = dep_sat_request.upstream_tuid
            reply_q = dep_sat_request.reply_q
            downstream_tuid = dep_sat_request.tuid
            downstream_compiled_tuid = dep_sat_request.compiled_tuid
            arg_dep_updates = dep_sat_request.arg_dep_updates
            client_id = dep_sat_request.client_id
            is_raw = dep_sat_request.is_raw

            completed_task_info = self._completed_tuids.get(client_id, {})
            if upstream_tuid in completed_task_info:
                completion_info = completed_task_info[upstream_tuid]
                # The upstream task is already complete; reply immediately.
                self.log.debug(
                    f"DepSatRequest: upstream {upstream_tuid} already complete, "
                    f"replying immediately for downstream {downstream_tuid} ({downstream_compiled_tuid})"
                )
                cancel_reason = TaskCancellationReason.NONE
                if is_raw and completion_info.raised:
                    cancel_reason = TaskCancellationReason.UPSTREAM_RAW_FAILURE
                dep_sat = DepSat(
                    upstream_tuid,
                    completion_info.manager_idx,
                    completion_info.result_ddict_idx,
                    downstream_tuid,
                    downstream_compiled_tuid,
                    arg_dep_updates,
                    cancel_reason,
                )
                reply_q.put(dep_sat)
                return
            else:
                # Upstream task is not yet complete; record the request so we
                # can notify the requester when it completes.
                self.log.debug(
                    f"DepSatRequest: upstream {upstream_tuid} pending, "
                    f"registered downstream {downstream_tuid} ({downstream_compiled_tuid})"
                )
                entry = (reply_q, downstream_tuid, downstream_compiled_tuid, arg_dep_updates, is_raw)
                try:
                    self._dep_request_reply_map[upstream_tuid].append(entry)
                except Exception:
                    self._dep_request_reply_map[upstream_tuid] = [entry]
                return
        except Exception as e:
            self._handle_manager_exception(e, "failed to handle DepSatRequest")

    def _handle_register_client(self, register_client: RegisterClient) -> None:
        """
        Register a new client with this batch instance and set the client as "active".

        :param register_client: Contains the client ID and return queue for the client.
        :type register_client: RegisterClient

        :return: Returns None.
        :rtype: None
        """
        ret_q = register_client.ret_q

        if register_client.client_id is None:
            client_id = self.client_ctr
            self.client_ctr += 1
            ret_q.put(client_id)
        else:
            client_id = register_client.client_id

        self.log.debug(f"received a registration request")

        if client_id in self.active_clients:
            # each manager should receive this message only once per cloned
            # batch instance, so return an exception back to the client
            e = RuntimeError("cannot register client more than once")
            me = ManagerException(None, e, _get_traceback(), f"client {client_id} is already registered")
            ret_q.put(me)
        elif self.destroy_called:
            e = RuntimeError("cannot register client after destroy has been requested")
            me = ManagerException(None, e, _get_traceback(), "batch runtime is shutting down")
            ret_q.put(me)
        else:
            self.log.debug(f"registering client {client_id}")
            self.active_clients.add(client_id)
            self.ret_q[client_id] = ret_q

    def _handle_unregister_client(self, unregister_client: UnregisterClient) -> None:
        """
        Unregister a client with this batch instance and set the client as "inactive".

        :param unregister_client: Contains the client ID for the client.
        :type unregister_client: UnregisterClient

        :return: Returns None.
        :rtype: None
        """
        client_id = unregister_client.client_id
        last_client = False

        if client_id in self.active_clients:
            self.log.debug(f"unregistering client {client_id}")
            self.active_clients.remove(client_id)

        if unregister_client.clean_detach:
            self._reap_client_state(client_id)
        else:
            self._clients_pending_reap.add(client_id)
            self._maybe_reap_client_state(client_id)

        if len(self.active_clients) == 0:
            last_client = True
            if not self.managed_lifecycle:
                self._request_runtime_shutdown()

        if self.is_scheduler and unregister_client.reply_q is not None:
            unregister_client.reply_q.put(last_client)

    def _handle_destroy_called(self, destroy_called: DestroyCalled) -> None:
        """
        Sets a flag indicating that destroy has been called by some client.

        :param destroy_called: An empty namedtuple that simply helps dispatch the correct method.
        :type destroy_called: DestroyCalled

        :return: Returns None.
        :rtype: None
        """
        self._request_runtime_shutdown(destroy_called.force_timeout)

    @singledispatchmethod
    def _handle_request(self, item):
        e = RuntimeError("invalid item received on the work queue")
        self._handle_manager_exception(e, f"manager received {item}")

    @_handle_request.register
    def _(self, work: Work) -> None:
        try:
            if self.is_scheduler and work.manager_idx not in (None, SCHEDULER_MANAGER_IDX):
                if work.manager_idx < 0 or work.manager_idx >= len(self._subnode_manager_qs):
                    raise RuntimeError(f"invalid destination manager_idx for work: {work.manager_idx}")

                # Account subnode Work at scheduler routing time so fence()
                # cannot complete until allocation is released.
                client_id = work._client_id
                self._subnode_outstanding[client_id] = self._subnode_outstanding.get(client_id, 0) + 1
                self._subnode_manager_qs[work.manager_idx].put(work)
            else:
                self._handle_compiled_task(work)
        except Exception as e:
            self._handle_manager_exception(
                e,
                f"manager got exception when handling compiled-task request from client={work._client_id}, uid={work._compiled_result_wrapper.tuid}",
                self.ret_q[work._client_id],
                work._compiled_result_wrapper.tuid,
            )

    @_handle_request.register
    def _(self, task: Task) -> None:
        try:
            if self.is_scheduler:
                raise RuntimeError(
                    "scheduler received unexpected raw Task; client worker should dispatch compiled Work"
                )
            else:
                self.log.debug("subnode manager received unexpected raw Task; ignoring")
        except Exception as e:
            self._handle_manager_exception(e, "manager got exception when handling new task")

    @_handle_request.register
    def _(self, dep_sat: DepSat) -> None:
        try:
            self._handle_dep_sat(dep_sat)
        except Exception as e:
            compiled_tuid = dep_sat.compiled_tuid
            work = self.work_backlog[compiled_tuid]

            self._handle_manager_exception(
                e,
                "manager got exception when handling dependency-satisfied message",
                self.ret_q[work._client_id],
                compiled_tuid,
            )

    @_handle_request.register
    def _(self, dep_sat_request: DepSatRequest) -> None:
        try:
            self._handle_dep_sat_request(dep_sat_request)
        except Exception as e:
            # best-effort: try to find related compiled task for error reporting
            try:
                compiled_tuid = dep_sat_request.compiled_tuid
                work = self.work_backlog.get(compiled_tuid, None)
                ret_q = self.ret_q[work._client_id] if work is not None else None
            except Exception:
                compiled_tuid = None
                ret_q = None

            self._handle_manager_exception(
                e,
                "manager got exception when handling DepSatRequest",
                ret_q,
                compiled_tuid,
            )

    @_handle_request.register
    def _(self, register_client: RegisterClient) -> None:
        try:
            self._handle_register_client(register_client)
        except Exception as e:
            self._handle_manager_exception(
                e,
                "manager got exception when handling register-client request",
                register_client.ret_q,
            )

    @_handle_request.register
    def _(self, unregister_client: UnregisterClient) -> None:
        try:
            self._handle_unregister_client(unregister_client)
        except Exception as e:
            self._handle_manager_exception(
                e,
                "manager got exception when handling unregister-client request",
                self.ret_q[unregister_client.client_id],
            )

    @_handle_request.register
    def _(self, destroy_called: DestroyCalled) -> None:
        try:
            self._handle_destroy_called(destroy_called)
        except Exception as e:
            self._handle_manager_exception(e, "manager got exception when handling destroy-called message")

    @_handle_request.register
    def _(self, fence_request: FenceRequest) -> None:
        try:
            if self.is_scheduler:
                self._handle_scheduler_fence_request(fence_request)
            else:
                self.log.debug("subnode manager received unexpected FenceRequest; ignoring")
        except Exception as e:
            self._handle_manager_exception(e, "manager got exception when handling fence request")

    @_handle_request.register
    def _(self, req: CancelRequest) -> None:
        try:
            self._handle_cancel_request(req)
        except Exception as e:
            self._handle_manager_exception(e, "manager got exception when handling cancel request")

    @_handle_request.register
    def _(self, req: SubnodeAllocRequest) -> None:
        try:
            if self.is_scheduler:
                self._handle_subnode_alloc_request(req)
            else:
                self.log.debug("subnode manager received unexpected SubnodeAllocRequest; ignoring")
        except Exception as e:
            self._handle_manager_exception(e, "manager got exception when handling SubnodeAllocRequest")

    @_handle_request.register
    def _(self, resp: SubnodeAllocResponse) -> None:
        try:
            if not self.is_scheduler:
                self._handle_subnode_alloc_response(resp)
            else:
                self.log.debug("scheduler received unexpected SubnodeAllocResponse; ignoring")
        except Exception as e:
            self._handle_manager_exception(e, "manager got exception when handling SubnodeAllocResponse")

    @_handle_request.register
    def _(self, req: SubnodeFreeRequest) -> None:
        try:
            if self.is_scheduler:
                self._handle_subnode_free_request(req)
            else:
                self.log.debug("subnode manager received unexpected SubnodeFreeRequest; ignoring")
        except Exception as e:
            self._handle_manager_exception(e, "manager got exception when handling SubnodeFreeRequest")

    @_handle_request.register
    def _(self, mnj: MultiNodeJobComplete) -> None:
        try:
            if self.is_scheduler:
                self._handle_mnj_complete(mnj)
            else:
                self.log.debug("subnode manager received unexpected MultiNodeJobComplete; ignoring")
        except Exception as e:
            self._handle_manager_exception(e, "manager got exception when handling MultiNodeJobComplete")

    @_handle_request.register
    def _(self, req: ClearFenceState) -> None:
        try:
            # These per-client caches are fence-epoch state. Once the client has
            # observed the fence completion, later fences should start from a
            # clean view of completed tasks and in-flight work.
            self._completed_tuids.pop(req.client_id, None)
            self._pending_task_counts.pop(req.client_id, None)
        except Exception as e:
            self._handle_manager_exception(e, "manager got exception when handling ClearFenceState")

    def _add_to_async_queue(self, completion_batch: list[CompletionNotification]) -> None:
        """Add a completion batch to the async queue."""
        self._async_queue.put(completion_batch)

    def _launch_tasks(self) -> None:
        """
        Launch all queued tasks using map_async.

        :return: Returns None.
        :rtype: None
        """
        if not self.is_scheduler:
            # The pool API still consumes a flat list, so subnode managers drain
            # the current allocation phase's HEFTY-ordered heap immediately
            # before dispatch.
            self._drain_subnode_ready_tasks()

        if len(self._pool_launch_args_list) == 0:
            return

        num_tasks = len(self._pool_launch_args_list)
        # chunk_size=1 ensures each task is dispatched to a worker individually, so completed
        # tasks are reported back (via _add_to_async_queue) as soon as possible. A larger
        # chunk_size would bundle multiple tasks onto one worker and execute them sequentially,
        # delaying completion notifications and therefore holding up any tasks that depend on
        # the ones in that chunk. The per-task dispatch overhead is negligible compared to the
        # cost of the user-defined work (functions, processes, MPI jobs).
        chunk_size = 1

        self.log.debug(f"starting {num_tasks} tasks")
        self.pool.map_async(
            _do_task,
            self._pool_launch_args_list,
            chunk_size,
            self._add_to_async_queue,
        )

        if not self.disable_telem:
            self.num_running_tasks += self._queued_task_count
            self.dragon_telem.add_data("num_running_tasks", self.num_running_tasks)

    def _handle_results(self, tuid_compiled_tuid_list: list[CompletionNotification]) -> None:
        """
        Process completion notifications returned from workers.
        Each notification contains (tuid, compiled_tuid).
        Results have already been written to the distributed dict by the workers.

        :param tuid_compiled_tuid_list: List of (tuid, compiled_tuid) tuples from completed tasks.
        :type tuid_compiled_tuid_list: list[CompletionNotification]

        :return: Returns None.
        :rtype: None
        """
        num_tasks = 0
        completed_tuids_by_client: dict[int, list[str]] = {}

        for completion in tuid_compiled_tuid_list:
            num_tasks += 1

            tuid = completion.tuid
            compiled_tuid = completion.compiled_tuid
            self.log.debug(f"individual task complete: tuid={tuid}, compiled_tuid={compiled_tuid}")

            work = self.work_backlog[compiled_tuid]
            task_core = work._ready_task_cores[tuid]
            del work._ready_task_cores[tuid]
            work_client_id = work._client_id
            completed_tuids_by_client.setdefault(work_client_id, []).append(tuid)

            # On the scheduler, a completed multi-node job means we can return the allocated
            # nodes to the available pool and check pending fences.
            if self.is_scheduler and self._is_multi_node_job(task_core):
                self._handle_mnj_complete(MultiNodeJobComplete(tuid, compiled_tuid, self._mnj_running.get(tuid, [])))

            self._release_running_task(tuid)

            if len(work._ready_task_cores) == 0 and len(work._blocked_task_cores) == 0:
                self.log.debug(f"compiled task complete: {compiled_tuid=}")

                # notify client that the compiled task is complete
                del self.work_backlog[compiled_tuid]

                # Decrement this manager's per-client in-flight Work chunk counter.
                new_count = self._pending_task_counts.get(work_client_id, 1) - 1
                if new_count == 0:
                    del self._pending_task_counts[work_client_id]
                else:
                    self._pending_task_counts[work_client_id] = new_count
                self._maybe_reap_client_state(work_client_id)

            # task_core.cached_queue is the per-task control queue used for
            # cancellation. Recycle it after completion so later tasks can
            # reuse the queue object without inheriting stale cancel messages.
            if task_core.cached_queue is not None:
                self._recycle_queue(task_core.cached_queue)

            # Record completion before replying to any late DepSatRequest so
            # those request handlers can safely reply immediately.
            if work_client_id not in self._completed_tuids:
                self._completed_tuids[work_client_id] = {}
            self._completed_tuids[work_client_id][tuid] = CompletedTaskInfo(
                task_core.manager_idx,
                task_core.result_ddict_idx,
                completion.raised,
            )

            # notify any other managers that had requested DepSat for this tuid
            try:
                reply_list = self._dep_request_reply_map.pop(tuid)
                for reply_q, downstream_tuid, downstream_compiled_tuid, arg_dep_updates, is_raw in reply_list:
                    try:
                        cancel_reason = TaskCancellationReason.NONE
                        if is_raw and completion.raised:
                            cancel_reason = TaskCancellationReason.UPSTREAM_RAW_FAILURE
                        ds = DepSat(
                            tuid,
                            task_core.manager_idx,
                            task_core.result_ddict_idx,
                            downstream_tuid,
                            downstream_compiled_tuid,
                            arg_dep_updates,
                            cancel_reason,
                        )
                        reply_q.put(ds)
                    except Exception:
                        self.log.debug(f"failed to send DepSat for {tuid=} to waiting manager")
            except Exception:
                pass

        if not self.disable_telem:
            self.num_running_tasks -= num_tasks
            self.num_completed_tasks += num_tasks

            self.dragon_telem.add_data("num_running_tasks", self.num_running_tasks)
            self.dragon_telem.add_data("num_completed_tasks", self.num_completed_tasks)

        for client_id, tuids in completed_tuids_by_client.items():
            ret_q = self.ret_q.get(client_id)
            if ret_q is not None:
                ret_q.put(CompletedTuids(tuids))

    def _log_task_debug_info(self, task_core: TaskCore) -> None:
        """
        Print information about a task to help debug hangs

        :param task_core: The core of the task whose information is being logged.
        :type task_core: TaskCore

        :return: Returns None.
        :rtype: None
        """
        self.log.debug(f"+     {task_core._get_id_str()}")
        if isinstance(task_core, JobCore):
            if task_core.hostname_list is not None:
                hosts_str = _compress_hostnames(task_core.hostname_list)
                self.log.debug(f"+     --> {len(task_core.hostname_list)} hosts assigned: {hosts_str}")
            else:
                self.log.debug("+     --> hostname assignment pending")
        if task_core.num_dep_tot > 0:
            self.log.debug(f"+     --> deps: {task_core.num_dep_sat}/{task_core.num_dep_tot} satisfied")

    def _dump_debug_state(self) -> None:
        """
        Log information about pending tasks to help debug hangs.

        :return: Returns None.
        :rtype: None
        """
        if self.dbg_update_start:
            self.dbg_start_time = time.time()
            self.dbg_update_start = False

        current_time = time.time()
        if current_time - self.dbg_start_time < default_progress_timeout:
            return

        self.dbg_update_start = True

        divider = "+ --------------------------------------------------------------------------+"
        no_tasks = "|     no outstanding tasks"
        self.log.debug(f"dumping batch state to debug potential hang...")
        self.log.debug(divider)
        self.log.debug(f"| active clients={self.active_clients}")
        self.log.debug(divider)
        self.log.debug(f"| size of backlog={len(self.work_backlog)}")
        self.log.debug(divider)
        self.log.debug(f"| destroy called={self.destroy_called}")
        self.log.debug(divider)
        self.log.debug(f"| pending DepSatRequests={len(self._dep_request_reply_map)}")

        if len(self.work_backlog) > 0:
            for _, item in self.work_backlog.items():
                if isinstance(item, Task):
                    task_core = item
                    self.log.debug(divider)
                    self.log.debug(f"| non-compiled task with tuid={task_core.tuid}")
                    self._log_task_debug_info(task_core)
                else:
                    work = item
                    compiled_tuid = work._compiled_result_wrapper.tuid

                    if len(work._ready_task_cores) > 0:
                        self.log.debug(divider)
                        self.log.debug(f"| ready tasks for compiled_tuid={compiled_tuid}")
                        for _, task_core in work._ready_task_cores.items():
                            self._log_task_debug_info(task_core)

                    if len(work._blocked_task_cores) > 0:
                        self.log.debug(divider)
                        self.log.debug(f"| blocked tasks for compiled_tuid={compiled_tuid}")
                        for _, task_core in work._blocked_task_cores.items():
                            self._log_task_debug_info(task_core)
        else:
            self.log.debug(divider)
            self.log.debug(no_tasks)

        self.log.debug(divider)

    def _run(self) -> None:
        """
        Runs the manager's main work loop.

        :return: Returns None.
        :rtype: None
        """
        # Subnode managers (idx > 0) use _subnode_has_alloc (an instance attribute)
        # to track whether they currently hold a node allocation from manager 0.
        subnode_has_work = False  # True if we queued at least one task this phase

        while True:
            # pull items off of the work queue until we either (1) drain the queue, or
            # (2) hit our batch size limit
            try:
                current_batch_size = 0
                while current_batch_size <= manager_work_queue_max_batch_size:
                    item = self.work_q.get_nowait()
                    self._handle_request(item)
                    current_batch_size += 1
                    self.dbg_update_start = True
            except queue.Empty:
                pass
            except Exception as e:
                self._handle_manager_exception(e, "failed to get item from work queue")

            if self.is_scheduler:
                try:
                    self._process_alloc_completions()
                except Exception as e:
                    self._handle_manager_exception(e, "failed to process allocation completions")

            # start any queued tasks in the worker pool (subnode managers must hold
            # an allocation before launching; tasks arriving before then are queued
            # in _pending_work_chunks by _handle_compiled_task). Subnode managers
            # stage ready tasks in _subnode_ready_heap until _launch_tasks drains
            # them into launch queues, so both containers mean the current
            # allocation phase has performed work and must later release it.
            if self._pool_launch_args_list or (not self.is_scheduler and self._subnode_ready_heap):
                subnode_has_work = True
            try:
                if self.is_scheduler or self._subnode_has_alloc:
                    self._launch_tasks()
            except Exception as e:
                for client, compiled_tuid in self._compiled_task_list:
                    self._handle_manager_exception(
                        e,
                        "failed to send tasks to the worker pool",
                        self.ret_q[client],
                        compiled_tuid,
                    )
            finally:
                self._pool_launch_args_list = []
                self._compiled_task_list = []
                self._queued_task_count = 0

            # process all queued results from previous calls to map_async
            try:
                while True:
                    item = self._async_queue.get_nowait()
                    self._handle_results(item)
                    self.dbg_update_start = True
            except queue.Empty:
                pass
            except Exception as e:
                self._handle_manager_exception(e, "failed to get item from async results queue")

            # Subnode managers: once all in-flight work is done and there are no
            # more ready tasks, release the node allocation back to manager 0.
            if (
                not self.is_scheduler
                and self._subnode_has_alloc
                and subnode_has_work
                and not self._has_ready_work()
                and len(self.work_backlog) == 0
            ):
                try:
                    scheduler_q = self._subnode_manager_qs[0] if self._subnode_manager_qs else None
                    if scheduler_q is not None:
                        phase_client_work_counts = self._alloc_phase_client_work_counts
                        phase_tuids = self._alloc_phase_tuids
                        self._alloc_phase_client_work_counts = {}
                        self._alloc_phase_tuids = set()
                        scheduler_q.put(SubnodeFreeRequest(self.idx, phase_client_work_counts, phase_tuids))
                        self._subnode_has_alloc = False
                        self._subnode_ready_heap.clear()
                        subnode_has_work = False
                        self.log.debug(
                            f"subnode manager {self.idx}: node allocation released, clients={phase_client_work_counts}, tasks={len(phase_tuids)}"
                        )
                        if self._queued_work_chunks:
                            self._pending_work_chunks = self._queued_work_chunks
                            self._queued_work_chunks = []
                            self._send_subnode_alloc_request()
                except Exception as e:
                    self._handle_manager_exception(e, "failed to release subnode allocation")

            if self._should_exit_runtime():
                break

            try:
                # if default_progress_timeout (=10) seconds have passed with no progress, dump
                # the current state of the batch service to help with debugging hangs
                self._dump_debug_state()
            except Exception as e:
                self._handle_manager_exception(e, f"failed to dump current state")

        if not self.disable_telem:
            try:
                self.dragon_telem.shutdown()
            except Exception as e:
                self._handle_manager_exception(e, f"failed to shut down telemetry")

        try:
            self.log.debug(f"manager shutting down pool")
            self.pool.close()
            self.pool.join()
        except Exception as e:
            self._handle_manager_exception(e, f"failed to join worker pool")

        for q in self.cached_queues:
            try:
                q.close()
            except Exception as e:
                self._handle_manager_exception(e, f"failed to close cached queue")

    def _has_ready_work(self) -> bool:
        """Return True if there are any tasks with ready task cores in the work backlog."""
        for _, work in self.work_backlog.items():
            if len(work._ready_task_cores) > 0:
                return True
        return False


class ClientCompiler:
    def __init__(
        self,
        num_managers: int,
        manager_qs: list[Queue],
        physical_cores_per_node: int,
        results_ddict_managers_per_pool: int,
        log: logging.Logger,
    ) -> None:
        self.num_managers = num_managers
        self.manager_qs = list(manager_qs)
        self.physical_cores_per_node = physical_cores_per_node
        self.results_ddict_managers_per_pool = results_ddict_managers_per_pool
        self.log = log
        self._compiled_task_ctr = 0
        self.dep_frontier: dict = {}
        self.tuid_to_manager_q: dict[str, Queue] = {}
        # Per-subnode-manager round-robin slot counter for result ddict assignment
        # Maps manager_idx -> next slot to use for this manager's workers
        self._result_slot_counter: dict[int, int] = {}

    def reset(self) -> None:
        """Clear compile-time state that is carried across batches for one client."""
        self.dep_frontier = {}
        self.tuid_to_manager_q = {}
        # Note: we keep result_slot_counter across batches for stable affinity

    def _result_ddict_idx_for_task(self, manager_idx: int) -> int:
        """
        Map a task's manager and an auto-incremented slot to its result ddict manager index.

        Each worker pool owns ``results_ddict_managers_per_pool`` consecutive
        result-ddict manager shards. Scheduler tasks always use ddict manager 0.
        Subnode manager ``i`` tasks use ddict managers
        ``i*R .. i*R + R - 1`` where ``R = results_ddict_managers_per_pool``.

        This means the scheduler shares ddict manager 0 with subnode manager 0's
        slot-0 shard on node 0, avoiding a reserved extra shard slot on
        every node.
        Round-robin slot assignment ensures equal load distribution across a pool's shards.
        """
        if manager_idx == SCHEDULER_MANAGER_IDX:
            return 0

        managers_per_pool = self.results_ddict_managers_per_pool

        # For subnode managers, use round-robin slot assignment within this pool's shards
        if manager_idx not in self._result_slot_counter:
            self._result_slot_counter[manager_idx] = 0

        slot = self._result_slot_counter[manager_idx]
        self._result_slot_counter[manager_idx] = (slot + 1) % managers_per_pool

        return manager_idx * managers_per_pool + slot

    def _subnode_manager_idx_for_partition(self, part_idx: int, subnode_offset: int, num_subnode_managers: int) -> int:
        """Map a partition index to a concrete subnode manager, honoring the batch offset."""
        return (subnode_offset + part_idx) % num_subnode_managers

    def _manager_idx_for_queue(self, manager_q) -> Optional[int]:
        """Resolve a manager queue back to its stable manager index, if known."""
        for queue_idx, queue in enumerate(self.manager_qs):
            if queue is manager_q or queue == manager_q:
                return SCHEDULER_MANAGER_IDX if queue_idx == 0 else queue_idx - 1

        return None

    def _task_compute_cost(self, task_core: TaskCore) -> int:
        """Return the coarse HEFT compute-cost estimate used for placement and weighting."""
        if isinstance(task_core, FunctionCore):
            return 1

        if isinstance(task_core, JobCore):
            if getattr(task_core, "is_parallel", True):
                num_procs = getattr(task_core, "num_procs", 0) or 1
                return 100 * num_procs

            return 1

        return 1

    def assign_heft_weights(self, tasks_to_compile: list["Task"]) -> None:
        """
        Assign HEFTY weights using client submission order.

        Task tuids are allocated monotonically when tasks are created, and the
        client request worker batches them without reordering. That means the
        compile list is already in deterministic topological order for current-
        batch dependencies, so the reverse pass can compute critical-path
        weights directly without a separate graph walk.
        """
        if not tasks_to_compile:
            return

        successors_by_task: dict[TaskCore, set[TaskCore]] = {}
        current_batch_task_core_index: dict[str, TaskCore] = {}
        topological_order = [task.core for task in tasks_to_compile]

        for task_core in topological_order:
            successors_by_task[task_core] = set()
            current_batch_task_core_index[task_core.tuid] = task_core

        for task_core in topological_order:
            for dep_record in task_core.dependencies:
                upstream_tuid = dep_record.upstream_tuid
                upstream_task_core = current_batch_task_core_index.get(upstream_tuid)
                if upstream_task_core is not None:
                    successors_by_task[upstream_task_core].add(task_core)
                elif upstream_tuid not in self.tuid_to_manager_q:
                    self.log.debug(
                        f"failed to add HEFT edge for upstream_tuid={upstream_tuid} downstream_tuid={task_core.tuid}: upstream node not found"
                    )

        for order_idx, task_core in enumerate(topological_order):
            task_core.heft_topo_order = order_idx

        for task_core in reversed(topological_order):
            successor_weights = [successor.weight for successor in successors_by_task[task_core]]
            task_core.weight = self._task_compute_cost(task_core) + (max(successor_weights) if successor_weights else 0)

    def hefty_partition(self, tasks_to_compile: list["Task"], subnode_offset: int = 0) -> list[set["TaskCore"]]:
        # Partition only the work that can run on subnode managers. Multi-node
        # jobs stay on the scheduler path and are handled separately later in
        # compile(), so they must not consume subnode partition slots here.
        num_subnode_managers = self.num_managers - 1
        if num_subnode_managers <= 0:
            raise RuntimeError("Batch requires at least one subnode manager")

        subnode_task_cores = [
            task.core for task in tasks_to_compile if not _is_multi_node_job(task.core, self.physical_cores_per_node)
        ]

        if not subnode_task_cores:
            return []

        # Use at most one partition per available subnode manager, but avoid
        # creating more partitions than tasks. Empty trailing partitions are
        # removed later once placement finishes.
        num_partitions = min(len(subnode_task_cores), num_subnode_managers)
        partitions = [set() for _ in range(num_partitions)]

        # Track the running compute load assigned to each partition. HEFT-style
        # placement here is intentionally approximate: we do not simulate exact
        # worker timelines, we just keep a cheap scalar load estimate that can
        # balance critical-path work across managers.
        partition_loads = [0 for _ in range(num_partitions)]

        # Remember where tasks from this compile batch were placed so children
        # can prefer the same manager and avoid unnecessary result-lookup hops.
        placed_partitions: dict[str, int] = {}

        # Multi-node jobs in the current compile batch are scheduler-owned. We
        # treat them as upstream parents on the scheduler when computing the
        # communication penalty for subnode tasks that depend on them.
        current_batch_mnj_tuids = {
            task.core.tuid for task in tasks_to_compile if _is_multi_node_job(task.core, self.physical_cores_per_node)
        }

        # Place higher-priority tasks first so that critical-path work gets the
        # first choice of managers. _task_heft_priority_key already bakes in the
        # reverse-pass HEFT weight plus deterministic tie-break information.
        ordered_task_cores = sorted(
            subnode_task_cores,
            key=_task_heft_priority_key,
        )

        def _manager_idx_for_parent(upstream_tuid: str) -> Optional[int]:
            # Prefer placements decided in this compile pass, because those are
            # the most accurate and let same-batch parent/child pairs cluster on
            # one manager when possible.
            placed_partition = placed_partitions.get(upstream_tuid)
            if placed_partition is not None:
                return self._subnode_manager_idx_for_partition(
                    placed_partition, subnode_offset, num_subnode_managers=num_subnode_managers
                )

            # Current-batch multi-node jobs are started by the scheduler, not a subnode
            # manager, so treat them as coming from the scheduler.
            if upstream_tuid in current_batch_mnj_tuids:
                return SCHEDULER_MANAGER_IDX

            # Otherwise fall back to the remembered placement from a prior batch
            # compile, if one exists.
            manager_q = self.tuid_to_manager_q.get(upstream_tuid)
            if manager_q is None:
                return None

            return self._manager_idx_for_queue(manager_q)

        for task_core in ordered_task_cores:
            # The partition score combines three ideas:
            # 1. current estimated load on that partition,
            # 2. compute cost of the task itself,
            # 3. a simple communication penalty for upstream argument producers
            #    that live on other managers.
            #
            # This is intentionally lightweight. We want something that respects
            # critical-path order and preserves locality without turning compile()
            # into a full scheduling simulation.
            # Only argument dependencies need result locality here. Pure data
            # access dependencies affect execution ordering, but they do not
            # require the downstream task to fetch an upstream Python result
            # tuple for argument substitution.
            arg_parent_tuids = {
                dep_record.upstream_tuid
                for dep_record in task_core.dependencies
                if dep_record.origin == DependencyOrigin.ARGUMENT
            }
            best_partition = None
            best_key = None
            base_cost = self._task_compute_cost(task_core)

            for part_idx in range(num_partitions):
                manager_idx = self._subnode_manager_idx_for_partition(
                    part_idx, subnode_offset, num_subnode_managers=num_subnode_managers
                )
                comm_penalty = 0
                candidate_node_idx = 0 if manager_idx == SCHEDULER_MANAGER_IDX else manager_idx

                # Penalize managers that would force this task to consume
                # upstream argument values from elsewhere. The penalty is coarse
                # by design: a simple per-parent increment is enough to bias
                # placement toward node-local argument-passing chains without
                # overfitting to uncertain runtime costs.
                if arg_parent_tuids:
                    for upstream_tuid in arg_parent_tuids:
                        parent_manager_idx = _manager_idx_for_parent(upstream_tuid)
                        if parent_manager_idx is not None:
                            parent_node_idx = 0 if parent_manager_idx == SCHEDULER_MANAGER_IDX else parent_manager_idx
                            if parent_node_idx != candidate_node_idx:
                                comm_penalty += 1

                load = partition_loads[part_idx]
                # Compare candidate partitions lexicographically so we first
                # minimize estimated finish cost, then communication, then raw
                # load, and finally use partition index as a deterministic tie
                # breaker.
                candidate_key = (load + base_cost + comm_penalty, comm_penalty, load, part_idx)
                if best_key is None or candidate_key < best_key:
                    best_partition = part_idx
                    best_key = candidate_key

            if best_partition is None:
                raise RuntimeError(f"failed to place subnode task {task_core.tuid}")

            # Commit the winning placement and update the partition's running
            # load so later tasks see the new balance state.
            partitions[best_partition].add(task_core)
            partition_loads[best_partition] += base_cost
            placed_partitions[task_core.tuid] = best_partition

            self.log.debug(
                f"placed subnode task tuid={task_core.tuid} on manager={self._subnode_manager_idx_for_partition(best_partition, subnode_offset, num_subnode_managers=num_subnode_managers)} "
                f"load={partition_loads[best_partition]} score={best_key[0]}"
            )

        # Drop any unused partitions at the tail so callers only see the
        # manager groups that actually received work.
        while partitions and not partitions[-1]:
            partitions.pop()

        self.log.debug(
            "HEFTY partition loads: "
            + ", ".join(
                f"manager {self._subnode_manager_idx_for_partition(part_idx, subnode_offset, num_subnode_managers=num_subnode_managers)}={partition_loads[part_idx]}"
                for part_idx in range(len(partitions))
            )
        )

        return partitions

    def compile(self, tasks_to_compile: list["Task"], client_id: int, name: Optional[str] = None) -> "Task":
        if not isinstance(tasks_to_compile, list):
            raise RuntimeError(f"tasks_to_compile must be a list of tasks: {tasks_to_compile=}")
        else:
            num_tasks = len(tasks_to_compile)
            if num_tasks == 0:
                raise RuntimeError("cannot compile an empty list of tasks")
            elif not isinstance(tasks_to_compile[0], Task):
                raise RuntimeError(f"tasks_to_compile must be a list of tasks: {tasks_to_compile[0]=}")

        compiled_task_tuid = f"compiled-{client_id}-{self._compiled_task_ctr}"
        self._compiled_task_ctr += 1
        compiled_task_core = TaskCore(compiled_task_tuid, client_id, name, None)
        compiled_task = Task(compiled_task_core, None, compiled=True)
        current_batch_task_cores: set[TaskCore] = set()

        for task in tasks_to_compile:
            compiled_task.num_subtasks += 1
            task.core.compiled_tuid = compiled_task.core.tuid
            task._compiled_task = compiled_task
            current_batch_task_cores.add(task.core)

            for kvs_and_key, task_and_access_type in task.accesses.items():
                task_obj, access_type = task_and_access_type

                if kvs_and_key not in self.dep_frontier:
                    if access_type == AccessType.WRITE:
                        write_before_read = task_obj
                    else:
                        write_before_read = None

                    self.dep_frontier[kvs_and_key] = FrontierInfo([task_obj], access_type, write_before_read)
                    continue

                frontier_info = self.dep_frontier[kvs_and_key]
                prev_task_list, prev_access_type, write_before_read = frontier_info

                if prev_access_type == AccessType.READ:
                    if access_type == AccessType.READ:
                        if write_before_read is not None:
                            task_obj._depends_on(
                                task=write_before_read,
                                origin=DependencyOrigin.DATA_ACCESS,
                                is_raw=True,
                            )

                        prev_task_list.append(task_obj)
                    elif access_type == AccessType.WRITE:
                        for prev_read in prev_task_list:
                            task_obj._depends_on(
                                task=prev_read,
                                origin=DependencyOrigin.DATA_ACCESS,
                                is_raw=False,
                            )

                        self.dep_frontier[kvs_and_key] = FrontierInfo([task_obj], access_type, task_obj)
                else:
                    if prev_access_type != AccessType.WRITE:
                        raise RuntimeError("invalid access mode")

                    if access_type == AccessType.READ:
                        task_obj._depends_on(
                            task=prev_task_list[0],
                            origin=DependencyOrigin.DATA_ACCESS,
                            is_raw=True,
                        )
                        self.dep_frontier[kvs_and_key] = FrontierInfo([task_obj], access_type, write_before_read)
                    else:
                        task_obj._depends_on(
                            task=prev_task_list[0],
                            origin=DependencyOrigin.DATA_ACCESS,
                            is_raw=False,
                        )
                        self.dep_frontier[kvs_and_key] = FrontierInfo([task_obj], access_type, task_obj)

        self.assign_heft_weights(tasks_to_compile)
        num_subnode_managers = self.num_managers - 1
        if num_subnode_managers <= 0:
            raise RuntimeError("Batch requires at least one subnode manager")

        # Batch startup always provisions one scheduler plus one subnode manager
        # per requested node. The compiler relies on that invariant so every
        # non-multi-node task can be placed onto a concrete subnode manager.
        subnode_offset = random.randrange(num_subnode_managers)

        self.log.debug("partitioning the dependency graph")
        subnode_partitions = self.hefty_partition(tasks_to_compile, subnode_offset=subnode_offset)
        mnj_partition: set["TaskCore"] = {
            task.core for task in tasks_to_compile if _is_multi_node_job(task.core, self.physical_cores_per_node)
        }

        self.log.debug(
            f"compiled_tuid={compiled_task.core.tuid}: {len(tasks_to_compile)} task(s) "
            f"-> {len(subnode_partitions)} subnode partition(s)"
            + (f", {len(mnj_partition)} multi-node job(s)" if mnj_partition else "")
        )

        scheduler_q = self.manager_qs[0]

        for part_idx, task_set in enumerate(subnode_partitions):
            subnode_idx = self._subnode_manager_idx_for_partition(part_idx, subnode_offset, num_subnode_managers)
            manager_q = self.manager_qs[subnode_idx + 1]
            for task_core in task_set:
                task_core.manager_idx = subnode_idx
                task_core.result_ddict_idx = self._result_ddict_idx_for_task(subnode_idx)
                self.tuid_to_manager_q[task_core.tuid] = manager_q

        for task_core in mnj_partition:
            task_core.manager_idx = SCHEDULER_MANAGER_IDX
            task_core.result_ddict_idx = self._result_ddict_idx_for_task(SCHEDULER_MANAGER_IDX)
            self.tuid_to_manager_q[task_core.tuid] = scheduler_q

        current_batch_task_core_index = {task_core.tuid: task_core for task_core in current_batch_task_cores}
        for task_core in current_batch_task_cores:
            task_core.upstream_routes = []
            task_core.downstream_routes = []

        for task in tasks_to_compile:
            task_core = task.core
            downstream_q = self.tuid_to_manager_q.get(task_core.tuid)
            if downstream_q is None:
                raise RuntimeError(f"failed to find manager queue for task {task_core.tuid}")

            for dep_record in task_core.dependencies:
                upstream_tuid = dep_record.upstream_tuid
                arg_dep_updates = dep_record.arg_dep_updates
                upstream_q = self.tuid_to_manager_q.get(upstream_tuid)
                if upstream_q is None:
                    raise RuntimeError(
                        f"failed to find manager queue for upstream task {upstream_tuid} while compiling dependency for task {task_core.tuid}"
                    )

                upstream_task_core = current_batch_task_core_index.get(upstream_tuid)
                is_raw = dep_record.is_raw
                if upstream_task_core is not None:
                    upstream_task_core.downstream_routes.append(
                        DependencyNotificationRoute(downstream_q, task_core.tuid, arg_dep_updates, is_raw)
                    )
                else:
                    task_core.upstream_routes.append(
                        DependencyRequestRoute(upstream_q, upstream_tuid, arg_dep_updates, is_raw)
                    )

        compiled_task.subnode_work_chunks = []
        for part_idx, task_set in enumerate(subnode_partitions):
            subnode_idx = self._subnode_manager_idx_for_partition(part_idx, subnode_offset, num_subnode_managers)
            manager_q = self.manager_qs[subnode_idx + 1]
            work = Work(
                task_set,
                client_id,
                compiled_task.core.tuid,
                manager_q=manager_q,
                manager_idx=subnode_idx,
            )
            compiled_task.subnode_work_chunks.append(work)

        compiled_task.mnj_work_chunk = None
        if mnj_partition:
            compiled_task.mnj_work_chunk = Work(
                mnj_partition,
                client_id,
                compiled_task.core.tuid,
                manager_q=scheduler_q,
                manager_idx=SCHEDULER_MANAGER_IDX,
            )

        return compiled_task


def _get_timeout_val(timeout_dict: Optional[dict]) -> float:
    """
    Get a timeout value in seconds from a dict that specifies the timeout in terms of
    days, hours, minutes, and seconds.

    :param timeout_dict: A dict specifying the timeout for a task.
    :type timeout_dict: Optional[dict]

    :return: Returns the timeout value in seconds.
    :rtype: float
    """
    if timeout_dict is None:
        return default_timeout
    else:
        day = timeout_dict["day"]
        hour = timeout_dict["hour"]
        min = timeout_dict["min"]
        sec = timeout_dict["sec"]
        return (day * 24.0 * 60.0 * 60.0) + (hour * 60.0 * 60.0) + (min * 60.0) + sec


# TODO: Optimizations ideas for _resolve_val
# 1. Resolve whatever we can at PTD import-time to help amortize costs across multiple calls
#    to the task wrapper
# 2. Mark wrapper call-time update-tuples to indicate if the updated value must be resolved
#    to minimize the numer of calls to _resolve_val
def _resolve_val(val: Any) -> Any:
    """
    Evaluate a function (represented as a FuncDesc), recursively applying ``_resolve_val`` to
    arguments of the function, and return the computed value.

    :param val: The value to resolve (assuming it's a FuncDesc).
    :type val: Any

    :return: Returns the computed value.
    :rtype: Any
    """
    if isinstance(val, FuncDesc):
        # recursively resolve all values in the list before moving on
        for item_idx, item in enumerate(val):
            val[item_idx] = _resolve_val(item)

        # apply func
        func = val[0]
        real_val = func(*val[1:])
    else:
        real_val = val

    return real_val


def _init_task_deps(read_or_write: dict, access_type: AccessType, batch: "Batch") -> list:
    """
    Set dependencies for a task given a list of keys or files to be accessed.

    :param read_or_write: A dict whose keys are "ddict" and "keys" if it represents accesses
    to a distributed dict, or "files" if it represents accesses to a list of files.
    :type read_or_write: dict
    :param acess_type: The type of accesses, either READ or WRITE.
    :type access_type: AccessType
    :param task: The task performing the accesses.
    :type task: Task

    :return: Returns None.
    :rtype: None
    """
    try:
        comm_obj = read_or_write["ddict"]
        keys_or_files = "keys"
    except KeyError:
        comm_obj = None
        keys_or_files = "files"

    accesses = []

    for key_or_file in read_or_write[keys_or_files]:
        real_key_or_file = _resolve_val(key_or_file)

        if access_type == AccessType.READ:
            read = batch.read(comm_obj, real_key_or_file)
            accesses.append(read)
        else:
            write = batch.write(comm_obj, real_key_or_file)
            accesses.append(write)

    return accesses


class FuncDesc:
    def __init__(self, func: Callable, args_list: list) -> None:
        """
        Initialize a function descriptor, which is used to reprsent a function in the
        PTD that we extract from a task group's PTD file.

        :param func: The function itself.
        :type func: Callable
        :param args_list: The list of arguments for the function.
        :type args_list: list

        :return: Returns None.
        :rtype: None
        """
        self._func_desc = [func]
        self._func_desc.extend(args_list)

    def __getitem__(self, idx: int) -> Any:
        """
        Get an item from ``_func_desc``.

        :param idx: The index of the item.
        :type idx: int

        :return: Returns the item.
        :rtype: Any
        """
        return self._func_desc[idx]

    def __setitem__(self, idx: int, val: Any) -> None:
        """
        Set an item in ``_func_desc``.

        :param idx: The index in the list item to be set.
        :type idx: int
        :param val: The value used to set the list item.
        :type val: Any

        :return: Returns None.
        :rtype: None
        """
        self._func_desc[idx] = val

    def get_list(self) -> list:
        """
        Get the ``_func_desc`` list.

        :return: Returns the ``_func_desc`` list that describes the function and its arguments.
        :rtype: list
        """
        return self._func_desc


class MakeTask:
    """
    Note on terminology: we use the terms "process" and "resolve" in a number of places
    in this class and in helper functions for this class. In general, processing a value
    refers to creating update-tuples at import-time. Update-tuples are applied at both
    import-time and call-time. Resolving a value refers to evaluating a function defined
    in the PTD file to get a specific value. Values are currently only resolved at call-time,
    but it would make sense to resolve anything we can at import-time to avoid the cost
    at call-time.
    """

    def __init__(
        self,
        batch: "Batch",
        ptd: dict,
        real_import_args: tuple,
        real_import_kwargs: dict,
    ) -> None:
        """
        Initialize a MakeTask object. MakeTask objects are callable and return either (1) a new task
        described by the parameterized task descriptor (``ptd``), or (2) a proxy object for the task
        (reprsented as a ProxyObj). In the second case, calling the MakeTask object is supposed to
        simulate calling a function that runs a task described by ``ptd`` (the same task that's returned
        in the first case). The proxy object represents the return value from the task. Consequently,
        the proxy object defines (almost) all of its dunder methods so that a batch fence is called if
        output from the task needs to be accessed, and then runs the dunder method with the proxy object
        replaced by the actual return value.

        :param batch: The batch that will run the generated task.
        :type batch: Batch
        :param ptd: The Parameterized Task Descriptor used to describe the task.
        :type ptd: dict
        :param real_import_args: The actual import-time positional arguments obtained from the call
        to ``import_func``.
        :type real_import_args: tuple
        :param real_import_kwargs: The actual import-time keyword arguments obtained from the call
        to ``import_func``..
        :type real_import_kwargs: dict

        :return: Returns None.
        :rtype: None
        """
        self._batch = batch
        self._ptd = ptd

        try:
            self._exes = ptd["executables"]
        except:
            raise RuntimeError("must specify one or more executables")

        try:
            self._reads = ptd["reads"]
        except:
            self._reads = []

        try:
            self._writes = ptd["writes"]
        except:
            self._writes = []

        try:
            import_args = ptd["import_args"]
        except:
            import_args = []

        try:
            import_kwargs = ptd["import_kwargs"]
        except:
            import_kwargs = {}

        try:
            calltime_args = ptd["args"]
        except:
            calltime_args = []

        try:
            calltime_kwargs = ptd["kwargs"]
        except:
            calltime_kwargs = {}

        # handle import args/kwargs
        self._arg_update_tuples = []
        self._ptd_arg_set = set(import_args)
        self._arg_idxs = {}

        for idx, arg in enumerate(import_args):
            self._arg_idxs[arg] = idx

        self._kwarg_update_tuples = []
        self._ptd_kwarg_set = set(import_kwargs.values())
        self._kwarg_keys = {}

        for key, arg in import_kwargs.items():
            self._kwarg_keys[arg] = key

        self._preprocess_val = True

        self._make_update_tuples_for_exes(self._exes)
        self._make_update_tuples_for_deps(self._reads, self._writes)
        self._make_update_tuples_for_task_attrs()
        # note that we only swap in the real args once for import args/kwargs, but
        # we do it once per task wrapper call for task args/kwargs
        self._swap_in_real_args(real_import_args, real_import_kwargs)

        ptd_import_arg_set = self._ptd_arg_set.union(self._ptd_kwarg_set)

        # handle calltime args/kwargs
        self._arg_update_tuples = []
        self._ptd_arg_set = set(calltime_args)
        self._arg_idxs = {}

        for idx, arg in enumerate(calltime_args):
            self._arg_idxs[arg] = idx

        self._kwarg_update_tuples = []
        self._ptd_kwarg_set = set(calltime_kwargs.values())
        self._kwarg_keys = {}

        for key, arg in calltime_kwargs.items():
            self._kwarg_keys[arg] = key

        self._preprocess_val = False

        import_and_calltime_intersection = self._ptd_arg_set.union(self._ptd_kwarg_set).intersection(ptd_import_arg_set)

        if len(import_and_calltime_intersection) > 0:
            raise RuntimeError(
                f"non-empty intersection between import-time and call-time arg/kwarg names: {import_and_calltime_intersection}"
            )

        self._make_update_tuples_for_exes(self._exes)
        self._make_update_tuples_for_deps(self._reads, self._writes)
        self._make_update_tuples_for_task_attrs()

    def _process_val(
        self,
        val: Any,
        iterable_to_update: Iterable,
        key_or_idx: Any,
    ) -> None:
        """
        Process ``iterable_to_update`` and generate "update-tuples" that can be applied to
        the iterable, either at import-time or call-time. Update-tuples are of the form
        (iterable_to_update, key_or_idx_for_iterable, key_or_idx_for_real_args), and the
        update-tuple is applied like so:

        iterable_to_update[key_or_idx_for_iterable] = real_args_or_kwargs[key_or_idx_for_real_args]

        :param val: The value to (potentially) be updated.
        :type val: Any
        :param iterable_to_update: The iterable containing ``val``.
        :type iterable_to_update: Iterable
        :param key_or_idx: The key or index for ``val`` in ``iterable_to_update``.
        :type key_or_idx: Any

        :return: Returns None.
        :rtype: None
        """
        if self._preprocess_val and isinstance(val, dict):
            # replace the dict with a list because its amendable to our update-tuple approach
            func, args_list = val.popitem()
            func_desc = FuncDesc(func, args_list)
            val = iterable_to_update[key_or_idx] = func_desc

        if isinstance(val, FuncDesc):
            func_desc = val
            for fn_or_arg_idx, fn_or_arg in enumerate(func_desc.get_list()):
                self._process_val(
                    fn_or_arg,
                    func_desc,
                    fn_or_arg_idx,
                )
        elif isinstance(val, str):
            if val in self._ptd_arg_set:
                # add update-tuple
                idx = self._arg_idxs[val]
                self._arg_update_tuples.append((iterable_to_update, key_or_idx, idx))
            elif val in self._ptd_kwarg_set:
                # add update-tuple
                key = self._kwarg_keys[val]
                self._kwarg_update_tuples.append((iterable_to_update, key_or_idx, key))

    def _make_update_tuples_for_exes(self, exe_list: list) -> None:
        """
        Make "update tuples" that allow us to quickly swap in the runtime argument values,
        replacing the template strings that represent the arguments in the PTD file.

        :param exe_list: The list of strings representing executables to make update-tuples for.
        :type exe_list: list

        :return: Returns None.
        :rtype: None
        """
        for exe in exe_list:
            # key=target
            val = exe["target"]
            self._process_val(val, exe, "target")

            # key=args
            try:
                user_fn_args = exe["args"]
                for val_idx, val in enumerate(user_fn_args):
                    self._process_val(val, user_fn_args, val_idx)
            except KeyError:
                pass

            # key=kwargs
            try:
                user_fn_kwargs = exe["kwargs"]
                for key, val in user_fn_kwargs.items():
                    self._process_val(val, user_fn_kwargs, key)
            except KeyError:
                pass

            # key=nprocs
            try:
                val = exe["nprocs"]
                self._process_val(val, exe, "nprocs")
            except KeyError:
                pass

            # key=cwd
            try:
                val = exe["cwd"]
                self._process_val(val, exe, "cwd")
            except KeyError:
                pass

            # key=env
            try:
                val = exe["env"]
                self._process_val(val, exe, "env")
            except KeyError:
                pass

    def _process_deps(self, dep_list: list) -> None:
        """
        Processes dicts of strings (reprsenting read and write accesses) to determine which
        strings need to be replaced with real arguments, and generates update-tuples to handle
        the replacing.

        :param dep_list: A list of dicts of strings representing read and write accesses.
        :type dep_list: list

        :raises RuntimeError: If any of the read_or_write dicts are invalid (i.e., don't
        have the expected keys).

        :return: Returns None.
        :rtype: None
        """
        for read_or_write in dep_list:
            try:
                val = read_or_write["ddict"]
                self._process_val(val, read_or_write, "ddict")

                key_list = read_or_write["keys"]
                for key_idx, val in enumerate(key_list):
                    self._process_val(val, key_list, key_idx)

                return
            except KeyError:
                pass

            try:
                file_list = read_or_write["files"]
                for file_idx, val in enumerate(file_list):
                    self._process_val(val, file_list, file_idx)
            except KeyError:
                raise RuntimeError(f"invalid read or write specified: {read_or_write}")

    def _make_update_tuples_for_deps(self, reads: list, writes: list) -> None:
        """
        Make "update tuples" that allow us to quickly swap in the runtime argument values for
        read and write accesses, replacing the template strings that represent the arguments
        in the PTD file.

        :param reads: List of strings representing read accesses.
        :type reads: list
        :param writes: List of strings representing write accesses.
        :type writes: list

        :return: Returns None.
        :rtype: None
        """
        self._process_deps(reads)
        self._process_deps(writes)

    def _make_update_tuples_for_task_attrs(self) -> None:
        """
        Make "update tuples" that allow us to quickly swap in the runtime argument values for
        task attributes, replacing the template strings that represent the arguments in the PTD
        file. Possible task attributes are the task's type, name, timeout value,
        and explicit stdout/stderr log paths.

        :return: Returns None.
        :rtype: None
        """
        # key=type
        try:
            val = self._ptd["type"]
            self._process_val(val, self._ptd, "type")
        except KeyError:
            raise RuntimeError("must specify a type for tasks")

        # key=name
        try:
            val = self._ptd["name"]
            self._process_val(val, self._ptd, "name")
        except KeyError:
            pass

        # key=timeout
        try:
            timeout_dict = self._ptd["timeout"]
            for val_idx, val in enumerate(timeout_dict):
                self._process_val(val, timeout_dict, val_idx)
        except KeyError:
            pass

        # key=stdout
        try:
            val = self._ptd["stdout"]
            self._process_val(val, self._ptd, "stdout")
        except KeyError:
            pass

        # key=stderr
        try:
            val = self._ptd["stderr"]
            self._process_val(val, self._ptd, "stderr")
        except KeyError:
            pass

    def _swap_in_real_args(self, real_args: tuple, real_kwargs: dict) -> None:
        """
        Apply all stored update-tuples to the PTD dict to swap in real values for arguments
        arguments, obtained from the calls to ``import_func`` (import-time arguments) and
        ``__call__`` (call-time arguments).

        :param real_args: The real call-time positional arguments that are used to update the PTD dict.
        :type real_args: tuple
        :param real_kwargs: The real call-time keyword arguments that are used to update the PTD dict.
        :type real_kwargs: dict

        :return: Returns None.
        :rtype: None
        """
        for update_tuple in self._arg_update_tuples:
            iterable_to_update, key_or_idx, arg_idx = update_tuple
            iterable_to_update[key_or_idx] = real_args[arg_idx]

        for update_tuple in self._kwarg_update_tuples:
            iterable_to_update, key_or_idx, kwarg_key = update_tuple
            iterable_to_update[key_or_idx] = real_kwargs[kwarg_key]

    def __call__(self, *args, **kwargs) -> Task:
        """
        Retun the task generated by ``_make_task``.

        :return: Returns the generated task.
        :rtype: Task
        """
        return self._make_task(*args, **kwargs)

    def _make_task(self, *real_calltime_args, **real_calltime_kwargs) -> Task:
        """
        Generates a new task based on the PTD for this instance of MakeTask and the real
        call-time arguments. The outline of how the task is generated is as follows: (1)
        the PTD dict is updated by swapping in the real call-time arguments; (2) the PTD
        dict is processed and values reprsented as FuncDesc objects are resolved; (3) the
        task is created using values from the PTD and the task creation functions (:py:meth:`function`,
        :py:meth:`process`, and :py:meth:`job`); (4) task dependencies are initialized using values from
        the PTD and the task's read/write dependency functions; (5) if background batching
        has been disabled, we just return the task; otherwise, a proxy object for the task
        is created and returned. The proxy object acts as a proxy for the return value of
        the task.

        :param real_calltime_args: The actual positional arguments passed in to __call__.
        :param real_calltime_kwargs: The actual keyword arguments passed in to __call__.

        :raises RuntimeError: If there's an invalid or missing target, or an invallid value for
        ``nprocs``, ``cwd``, or ``env``.

        :return: Returns either the generated task, or a proxy object for the task.
        :rtype: Task | ProxyObj
        """
        self._swap_in_real_args(real_calltime_args, real_calltime_kwargs)

        # get task attributes: type, name, timeout

        try:
            task_type = _resolve_val(self._ptd["type"])
            task_type = _str_to_task_type(task_type)
        except:
            raise RuntimeError("failed to resolve target for task")

        try:
            task_name = _resolve_val(self._ptd["name"])
        except:
            task_name = None

        try:
            timeout_dict = _resolve_val(self._ptd["timeout"])
        except:
            timeout_dict = None

        try:
            task_stdout = _resolve_val(self._ptd["stdout"])
        except KeyError:
            task_stdout = None

        try:
            task_stderr = _resolve_val(self._ptd["stderr"])
        except KeyError:
            task_stderr = None

        # set up ProcessTemplate list

        pt_list = []

        for exe in self._exes:
            try:
                target = _resolve_val(exe["target"])
                if task_type == TaskType.FUNC:
                    if not isinstance(target, Callable):
                        raise RuntimeError(f"function target must be a callable: {target=}")
                else:
                    if not isinstance(target, str) and not isinstance(target, Path):
                        raise RuntimeError(f"process or job target must be a str or Path: {target=}")
            except KeyError:
                raise RuntimeError("failed to resolve target for task")

            try:
                nprocs = _resolve_val(exe["nprocs"])
                if not isinstance(nprocs, int):
                    raise RuntimeError(f"nprocs must be an int: {nprocs=}")
            except KeyError:
                nprocs = 1

            try:
                args = list(exe["args"])
                for arg_idx, arg in enumerate(args):
                    args[arg_idx] = _resolve_val(arg)
            except KeyError:
                args = ()

            try:
                kwargs = dict(exe["kwargs"])
                for key, arg in kwargs.items():
                    kwargs[key] = _resolve_val(arg)
            except KeyError:
                kwargs = {}

            try:
                cwd = _resolve_val(exe["cwd"])
                if not isinstance(cwd, str) and not isinstance(cwd, Path):
                    raise RuntimeError(f"cwd must be a str or Path: {cwd=}")
            except KeyError:
                cwd = None

            try:
                env = _resolve_val(exe["env"])
                if not isinstance(env, dict):
                    raise RuntimeError(f"env must be a dict: {env=}")
            except KeyError:
                env = None

            try:
                stdout = _resolve_val(exe["stdout"])
                if not isinstance(stdout, str) and not isinstance(stdout, Path):
                    raise RuntimeError(f"stdout must be a str or Path: {stdout=}")
            except KeyError:
                stdout = None

            try:
                stderr = _resolve_val(exe["stderr"])
                if stderr != Popen.STDOUT and not isinstance(stderr, str) and not isinstance(stderr, Path):
                    raise RuntimeError(f"stderr must be a str, Path, or Popen.STDOUT: {stderr=}")
            except KeyError:
                stderr = None

            if task_type != TaskType.FUNC:
                pt = ProcessTemplate(
                    target=target, args=args, kwargs=kwargs, cwd=cwd, env=env, stdout=stdout, stderr=stderr
                )
                pt_list.append((nprocs, pt))

        # set up reads and writes
        reads = []
        writes = []

        for read in self._reads:
            new_reads = _init_task_deps(read, AccessType.READ, self._batch)
            reads.extend(new_reads)

        for write in self._writes:
            new_writes = _init_task_deps(write, AccessType.WRITE, self._batch)
            writes.extend(new_writes)

        # create Task from task_type and pt_list
        task_options = self._batch.options(
            reads=reads,
            writes=writes,
            name=task_name,
            timeout=_get_timeout_val(timeout_dict),
            stdout=task_stdout,
            stderr=task_stderr,
        )

        if task_type == TaskType.FUNC:
            task = task_options.function(target, *args, **kwargs)
        elif task_type == TaskType.PROC:
            _, pt = pt_list[0]
            task = task_options.process(process_template=pt)
        elif task_type == TaskType.JOB:
            task = task_options.job(process_templates=pt_list)
        else:
            raise RuntimeError(f"invalid task type: {task_type}\n==> valid types: function, process, job")

        return task


class BatchTopology:
    """Describes the placement of managers and worker pools across nodes.

    Returned by :py:meth:`Batch.topology`.

    The scheduler runs separately on the client host. Each requested
    node contributes one worker pool and one colocated subnode manager. All
    physical cores on every pool node are available as workers; no core is
    reserved for the manager process.

    Hostnames in the string representation are compressed into Slurm-style
    bracket notation (e.g. ``node[001-004]``) when the ``python-hostlist``
    package is installed (``pip install python-hostlist``).  Without it,
    hostnames are listed verbatim, separated by commas.
    """

    def __init__(
        self,
        total_nodes: int,
        scheduler_hostname: str,
        manager_hostnames: list[str],
        pool_hostnames: list[list[str]],
        workers_per_pool: list[int],
    ) -> None:
        self.total_nodes = total_nodes
        """Total number of nodes used by this Batch instance."""
        self.scheduler_hostname = scheduler_hostname
        """Hostname where the dedicated scheduler runs."""
        self.manager_hostnames = manager_hostnames
        """Hostname of each subnode manager's colocated pool node."""
        self.pool_hostnames = pool_hostnames
        """List of hostname-lists, one inner list per worker pool."""
        self.workers_per_pool = workers_per_pool
        """Per-pool worker counts (physical cores x nodes in that pool)."""
        self.total_managers = 1 + len(manager_hostnames)
        """Total number of managers, including the dedicated scheduler."""

    def __str__(self) -> str:
        lines = ["Batch Topology:"]
        lines.append(f"  Total nodes  : {self.total_nodes}")
        lines.append(
            f"  Managers     : {self.total_managers} total (1 scheduler + {len(self.manager_hostnames)} subnode)"
        )
        lines.append(f"  Scheduler    : {self.scheduler_hostname}")
        lines.append(f"  Worker pools : {len(self.pool_hostnames)} pool(s) (1 dedicated subnode manager per pool)")
        for i, (hostnames, wpp, mgr_host) in enumerate(
            zip(self.pool_hostnames, self.workers_per_pool, self.manager_hostnames)
        ):
            compressed = _compress_hostnames(hostnames)
            lines.append(f"    Pool {i} ({len(hostnames)} node(s), {wpp} worker(s)): {compressed}  [mgr: {mgr_host}]")
        return "\n".join(lines)

    def __repr__(self) -> str:
        total_workers = sum(self.workers_per_pool)
        return (
            f"BatchTopology(total_nodes={self.total_nodes}, "
            f"total_managers={self.total_managers}, "
            f"num_pools={len(self.pool_hostnames)}, "
            f"total_workers={total_workers})"
        )


@dataclass(frozen=True)
class BatchTaskOptions:
    """Execution metadata for a Batch task submission.

    Use :py:meth:`Batch.options` to construct one of these indirectly and then
    call ``.function(...)``, ``.process(...)``, or ``.job(...)`` on the returned
    proxy so Batch metadata is kept separate from user payload arguments.
    """

    reads: Optional[list] = None
    writes: Optional[list] = None
    name: Optional[str] = None
    timeout: float = default_timeout
    stdout: Optional[str | Path] = None
    stderr: Optional[str | Path] = None
    pmi: PMIBackend = PMIBackend.CRAY


class BatchOptionsProxy:
    """Bound submission helper returned by :py:meth:`Batch.options`."""

    def __init__(self, batch: "Batch", task_options: BatchTaskOptions) -> None:
        self._batch = batch
        self._task_options = task_options

    def function(self, target: Callable, *args, **kwargs) -> Function:
        """Submit a function task using the bound Batch task options."""
        return self._batch._submit_function(target, args, kwargs, self._task_options)

    def process(self, process_template: ProcessTemplate) -> Job:
        """Submit a process task using the bound Batch task options."""
        return self._batch._submit_process(process_template, self._task_options)

    def job(self, process_templates: list) -> Job:
        """Submit a job task using the bound Batch task options."""
        return self._batch._submit_job(process_templates, self._task_options)


# TODO: need to be able to pickle and unpickle Batch objects and send them
# to managers and workers to enable recursive function calls
class Batch:
    """Graph-based distributed scheduling for functions, executables, and parallel applications"""

    def __init__(
        self,
        num_nodes: Optional[int] = None,
        pool_nodes: Optional[int] = None,
        disable_telem: bool = False,
        scheduler_workers: Optional[int] = None,
        results_ddict_mem: Optional[int] = None,
        results_ddict_managers_per_pool: Optional[int] = default_results_ddict_managers_per_pool,
        managed_lifecycle: bool = False,
        stdout: Optional[str | Path] = None,
        stderr: Optional[str | Path] = None,
        task_logs: bool = False,
    ) -> None:
        """
        Create a Batch instance for orchestrating functions, executables, and parallel applications
        with data dependencies with a directed acyclic graph (DAG).

        :param num_nodes: Number of nodes to use for this Batch instance.  Defaults to all nodes
            in the allocation.  Values larger than the allocation are silently clamped.
        :type num_nodes: Optional[int]
        :param pool_nodes: Reserved for future worker-pool grouping support. The current
            implementation overrides this to ``1`` so each requested node gets its own
            subnode manager and worker pool.
        :type pool_nodes: Optional[int]
        :param disable_telem: Indicates if telemetry should be disabled for this Batch instance. Defaults to False.
        :type disable_telem: bool
        :param scheduler_workers: Number of workers in the scheduler (manager 0)'s local
            worker pool. Defaults to the total number of nodes in the allocation (one worker per
            node). Increase this to allow more concurrent multi-node jobs.
        :type scheduler_workers: Optional[int]
        :param results_ddict_mem: Total memory in bytes to allocate for the Batch-owned results
            DDict. When omitted, Batch allocates one gibibyte per requested node.
        :type results_ddict_mem: Optional[int]
        :param results_ddict_managers_per_pool: Number of results-DDict manager shards to
            provision per worker pool (one pool per requested node). Must be between ``1`` and
            the number of batch workers per node; values outside that range are clamped (and the
            clamping is logged). When omitted, Batch uses ``default_results_ddict_managers_per_pool``
            shards per pool (clamped to the worker count). Pass ``None`` explicitly to request one
            results-DDict manager per worker.
        :type results_ddict_managers_per_pool: Optional[int]
        :param managed_lifecycle: Controls the lifetime of the Batch instance
            shared by clients. When ``False`` (the default), that Batch
            instance shuts down automatically after the last client detaches.
            When ``True``, it stays alive until some client calls
            :py:meth:`Batch.destroy`.
        :type managed_lifecycle: bool
        :param stdout: Default stdout log file path for tasks created by this client. When omitted
            and ``task_logs=True``, Batch creates per-task stdout files under
            ``runinfo/<batch-run-id>/client-<id>/task_logs/<task-kind>``. When omitted and
            ``task_logs=False`` (the default), task stdout is not captured to a file and is
            forwarded to the client console instead.
        :type stdout: Optional[str | Path]
        :param stderr: Default stderr log file path for tasks created by this client. When omitted
            and ``task_logs=True``, Batch creates per-task stderr files under
            ``runinfo/<batch-run-id>/client-<id>/task_logs/<task-kind>``. When omitted and
            ``task_logs=False`` (the default), task stderr is not captured to a file and is
            forwarded to the client console instead.
        :type stderr: Optional[str | Path]
        :param task_logs: When ``True``, Batch captures each task's stdout/stderr to per-task files
            under ``runinfo/<batch-run-id>/client-<id>/task_logs`` and maintains a ``manifest.jsonl``
            index, enabling the experimental log-discovery helpers (:py:meth:`Batch.log_dir`,
            :py:meth:`Batch.log_manifest_path`, :py:meth:`Batch.iter_log_records`,
            :py:meth:`Batch.find_logs`, and :py:meth:`Batch.read_logs`). When ``False``
            (the default), no ``runinfo`` directory or manifest is created and task output is only
            written to a file when an explicit ``stdout``/``stderr`` path is supplied; otherwise it
            is forwarded to the client console. These helper APIs may evolve as the logging
            interface matures. The log-discovery helpers raise ``RuntimeError`` while task
            logging is disabled.
        :type task_logs: bool

        .. highlight:: python
        .. code-block:: python

            # Generate the powers of a matrix and write them to disk
            from dragon.workflows.batch import Batch
            from pathlib import Path

            import numpy as np

            def gpu_matmul(m, base_dir, i):
                # do GPU work with matrix m and data from {base_dir}/file_{i}
                return matrix

            # A base directory, and files in it, will be used for communication of results
            batch = Batch()
            base_dir = Path("/some/path/to/base_dir")

            # Knowledge of reads and writes to files is used by Batch to infer data dependencies
            # and automatically parallelize tasks
            get_read = lambda i: batch.read(base_dir, Path(f"file_{i}"))
            get_write = lambda i: batch.write(base_dir, Path(f"file_{i+1}"))

            a = np.array([j for j in range(100)])
            m = np.vander(a)

            # Submit tasks — Batch dispatches them to workers in the background
            tasks = [batch.options(reads=[get_read(i)], writes=[get_write(i)], timeout=30)
                         .function(gpu_matmul, m, base_dir, i)
                     for i in range(1000)]

            # Retrieve results — .get() waits for each task to complete if needed
            for task in tasks:
                try:
                    print(f"result={task.get()}")
                except Exception as e:
                    print(f"gpu_matmul failed with the following exception: {e}")

            batch.join()


        :return: Returns None.
        :rtype: None
        """
        if results_ddict_mem is not None and results_ddict_mem <= 0:
            raise ValueError("results_ddict_mem must be a positive integer number of bytes")

        # ------------------------------------------------------------------ #
        # Node discovery and placement decisions                             #
        # ------------------------------------------------------------------ #

        alloc = System()
        all_node_objs = alloc._node_objs
        total_available = len(all_node_objs)

        # Clamp requested node count to what is actually available.
        if num_nodes is None or num_nodes > total_available:
            num_nodes = total_available
        # Batch always keeps at least one requested node in play, which in turn
        # guarantees one subnode manager alongside the dedicated scheduler.
        num_nodes = max(1, num_nodes)

        node_objs = all_node_objs[:num_nodes]

        # Physical cores per node: Dragon cpu_count() reports hyperthreads across
        # all nodes; dividing each node's logical CPU count by 2 gives physical cores.
        cpus_per_node = max(1, node_objs[0].num_cpus // 2)

        # pool_nodes is intentionally forced to one node per pool for now.
        # The current topology promises one dedicated subnode manager per
        # requested node, plus a separate scheduler colocated with the Batch
        # allocation rather than the creating client.
        # Keeping the parameter but overriding it avoids exposing partially
        # implemented multi-node pool semantics through the public API.
        if pool_nodes is None:
            pool_nodes = 1
        pool_nodes = 1

        # Each pool node contributes cpus_per_node workers; no core reservation.
        workers_per_node_in_pool = cpus_per_node

        # The scheduler is a dedicated extra manager process that runs on the
        # first Batch node. Each requested node gets its own one-node worker
        # pool and subnode manager.
        num_managers = num_nodes + 1
        pool_node_huids_list: list[list[int]] = [[]]
        pool_node_huids_list.extend([[n.h_uid] for n in node_objs])
        subnode_manager_node_objs = [Node(pool[0]) for pool in pool_node_huids_list[1:]]
        scheduler_node_obj = node_objs[0]
        self._scheduler_hostname = scheduler_node_obj.hostname
        self._scheduler_node_huid = scheduler_node_obj.h_uid

        # Publish node topology for use by _setup_managers and topology().
        self._node_objs = node_objs
        self._subnode_manager_node_objs = subnode_manager_node_objs
        self._pool_node_huids_list = pool_node_huids_list
        self._cpus_per_node = cpus_per_node
        self._workers_per_node_in_pool = workers_per_node_in_pool

        # Resolve the number of results-DDict manager shards per worker pool. Default
        # is ``default_results_ddict_managers_per_pool``; the value is clamped to
        # [1, workers_per_node] and any clamping is logged once self.log is available
        # (see below). Passing None requests one shard per worker.
        self._results_ddict_managers_per_pool, self._results_ddict_managers_per_pool_warning = (
            _resolve_results_ddict_managers_per_pool(results_ddict_managers_per_pool, workers_per_node_in_pool)
        )

        # One worker pool per requested node.
        self.num_workers = workers_per_node_in_pool
        self.num_managers = num_managers

        self.managed_lifecycle = managed_lifecycle
        self._deserialized_handle = False
        self._run_id = str(uuid1())
        self._task_logs_enabled = task_logs
        self._default_stdout = _normalize_output_path(stdout)
        self._default_stderr = _normalize_output_path(stderr)
        self._client_run_dir: Optional[Path] = None
        self._log_manifest_path: Optional[Path] = None
        self._log_manifest_lock = threading.Lock()

        # All node hostnames, used by the scheduler for node allocation.
        self._all_node_hostnames: list[str] = [n.hostname for n in node_objs]

        # Scheduler workers: number of workers in manager 0's pool. Defaults to
        # total node count (one worker per node for concurrent multi-node jobs).
        self._scheduler_workers: int = scheduler_workers if scheduler_workers is not None else num_nodes

        # Work submission queue; bound to manager 0's queue in _setup_managers().
        self.work_q = None

        # Every Batch handle registers through the same client path. Runtime
        # shutdown is then controlled by the shared lifecycle mode: either the
        # last client detaches (default) or some client explicitly calls
        # destroy() when managed_lifecycle=True.
        self.client_id: Optional[int] = None
        self.manager_qs = []
        self._serialized_manager_qs = []
        self.grp = None
        self._serialized_grp = None
        self._serialized_grp_mgr_puid = None

        # These flags are local to this client handle.
        self.closed = False
        self.destroyed = False
        self.terminated = False

        self.disable_telem = disable_telem

        # Create a distributed dict to store task results keyed by tuid.
        # Use ``self._results_ddict_managers_per_pool`` shards per pool on every
        # node. The scheduler shares ddict manager 0 with subnode manager 0's
        # slot-0 shard on node 0 rather than reserving a dedicated extra shard.
        one_gig = 1024**3
        results_ddict_total_mem = results_ddict_mem if results_ddict_mem is not None else (num_nodes * one_gig)

        self.results_ddict = DDict(
            n_nodes=num_nodes,
            managers_per_node=self._results_ddict_managers_per_pool,
            total_mem=results_ddict_total_mem,
            wait_for_keys=True,
            working_set_size=2,
            name=str(uuid1()),
        )

        self._set_unique_attrs()
        # NOTE: self.log is set in _set_unique_attrs
        if self._results_ddict_managers_per_pool_warning is not None:
            self.log.warning(self._results_ddict_managers_per_pool_warning)
        self.log.debug(str(self.topology()))

        self._setup_managers()
        self._init_client_compiler()
        self._register()
        self._initialize_client_run_dir()
        self._start_client_request_worker()

    def __del__(self) -> None:
        """
        Best-effort detach this client handle during garbage collection.

        :return: Returns None.
        :rtype: None
        """
        if getattr(self, "destroyed", False) or getattr(self, "terminated", False):
            return

        if getattr(self, "closed", True):
            return

        if not self.closed:
            try:
                self._stop_client_request_worker(timeout=1.0)
            except Exception:
                pass

            try:
                self._detach_group_client()
            except Exception:
                pass

            try:
                self._unregister(timeout=1.0, clean_detach=False)
            except Exception:
                pass

        ret_q = getattr(self, "ret_q", None)
        if ret_q is not None:
            try:
                ret_q.close()
            except Exception:
                pass

    def __setstate__(self, state) -> None:
        """
        Set state for a new client handle from a serialized Batch.

        Deserialized copies preserve the lifecycle mode of the Batch instance
        shared by clients, carry enough shared Batch-instance state to wait for
        shutdown, and register as fresh clients with the existing managers.

        :return: Returns None.
        :rtype: None
        """

        serialized_results_ddict = state.pop("_serialized_results_ddict", None)

        # a lot of the state is generic and can just be set using the origin __dict__
        self.__dict__.update(state)
        self.results_ddict = None

        self.destroyed = False
        self._deserialized_handle = True
        self.grp = None

        # Local-only sync/log state is stripped during __getstate__. Restore safe
        # defaults up front so the runtime-loss finalizer (which flushes the log
        # manifest) can run even when we bail out before _set_unique_attrs() and
        # _initialize_client_run_dir() recreate them on the success path.
        self._log_manifest_lock = threading.Lock()
        self._log_manifest_path = None
        self._task_log_metadata = OrderedDict()
        self._task_log_tasks = {}

        try:
            serialized_grp_mgr_puid = state.get("_serialized_grp_mgr_puid", None)
            if serialized_grp_mgr_puid is not None and not self._is_runtime_group_manager_active(
                serialized_grp_mgr_puid
            ):
                raise RuntimeError("serialized batch runtime is no longer active")

            # Rebuild manager_qs from serialized descriptors
            serialized_qs = state.pop("_serialized_manager_qs", [])
            self.manager_qs = [Queue.attach(desc) for desc in serialized_qs]
            self.work_q = self.manager_qs[0] if self.manager_qs else None

            # update attributes that are unique to this batch instance
            self._set_unique_attrs()
            self._init_client_compiler()

            # Only register if we have queues to register with
            if self.manager_qs:
                self._register()
                self._initialize_client_run_dir()
                self._start_client_request_worker()

            if serialized_results_ddict is not None:
                self.results_ddict = DDict.attach(serialized_results_ddict)
        except Exception as exc:
            if not self._is_runtime_unavailable_exception(exc):
                raise

            self._finalize_local_runtime_loss()
            raise RuntimeError("cannot attach to batch runtime after it has been destroyed") from exc

    def __getstate__(self) -> dict:
        """
        Serialize only the remote Batch handle state.

        Local-only synchronization objects that cannot be safely shared between
        client handles are excluded. Deserialized copies preserve the runtime's
        lifecycle mode, keep the serialized ProcessGroup client needed to wait
        for shutdown, and re-register as new clients.
        """
        state = self.__dict__.copy()

        # Strip unpicklable local state
        for key in (
            "ret_q",
            "cq",
            "_completion_overflow",
            "_client_request_q",
            "_client_request_thread",
            "_client_request_worker_exc",
            "client_compiler",
            "log",
            "grp",
            "work_q",
            "_client_run_dir",
            "_log_manifest_path",
            "_log_manifest_lock",
            "_results_ddict_lock",
            "_task_log_metadata",
            "_task_log_tasks",
            "results_ddict",
        ):
            state.pop(key, None)

        if state.get("_serialized_grp", None) is None and getattr(self, "grp", None) is not None:
            state["_serialized_grp"] = cloudpickle.dumps(self.grp)

        if state.get("_serialized_grp_mgr_puid", None) is None and getattr(self, "grp", None) is not None:
            state["_serialized_grp_mgr_puid"] = getattr(self.grp, "_mgr_p_uid", None)

        results_ddict = getattr(self, "results_ddict", None)
        if results_ddict is not None:
            state["_serialized_results_ddict"] = results_ddict.serialize()

        # Serialize manager queue descriptors for reattachment
        manager_qs = getattr(self, "manager_qs", [])
        if manager_qs:
            state["_serialized_manager_qs"] = [q.serialize() for q in manager_qs]
        else:
            state["_serialized_manager_qs"] = list(getattr(self, "_serialized_manager_qs", []))
        state.pop("manager_qs", None)

        state["client_id"] = None
        state["closed"] = False
        state["destroyed"] = False
        state["terminated"] = False
        state["_deserialized_handle"] = True

        return state

    def _setup_managers(self) -> None:
        """
        Creates the managers for a batch.

        The scheduler queue lives at ``manager_qs[0]``. Logical task
        ``manager_idx`` values use ``-1`` for scheduler-owned work and
        ``0..n-1`` for subnode managers. This should only be called by the
        handle that owns runtime bringup.

        :return: Returns None.
        :rtype: None
        """
        self.log.debug(f"setting up managers")

        # We need manager_qs[0] to exist so that subnode managers can reference
        # it when sending SubnodeAllocRequest messages. The scheduler queue and
        # process are placed on the first Batch allocation node so the runtime
        # does not depend on the creating client's host.
        scheduler_hostname = self._scheduler_hostname
        scheduler_q = Queue(
            maxsize=manager_work_queue_maxsize,
            block_size=default_block_size,
            policy=Policy(
                placement=Policy.Placement.HOST_NAME,
                host_name=scheduler_hostname,
            ),
        )

        # First pass: allocate work queues for logical subnode managers 0..n-1
        subnode_managers = []
        subnode_manager_qs: list[Queue] = []

        self.grp = ProcessGroup(restart=False)

        for queue_idx in range(1, self.num_managers):
            manager_idx = queue_idx - 1
            pool_node_huids = self._pool_node_huids_list[queue_idx]
            manager_hostname = Node(pool_node_huids[0]).hostname
            manager_node_policy = Policy(placement=Policy.Placement.HOST_NAME, host_name=manager_hostname)

            manager_q = Queue(
                maxsize=manager_work_queue_maxsize,
                block_size=default_block_size,
                policy=Policy(
                    placement=Policy.Placement.HOST_NAME,
                    host_name=manager_hostname,
                ),
            )

            num_workers_this_manager = len(pool_node_huids) * self._workers_per_node_in_pool

            subnode_manager = Manager(
                manager_idx,
                num_workers_this_manager,
                manager_q,
                self.ret_q,
                self.results_ddict,
                pool_node_huids=pool_node_huids,
                physical_cores_per_node=self._workers_per_node_in_pool,
                disable_telem=self.disable_telem,
                is_scheduler=False,
                managed_lifecycle=self.managed_lifecycle,
                # Pass the scheduler queue so the subnode manager can send
                # SubnodeAllocRequest / SubnodeFreeRequest messages.
                subnode_manager_qs=[scheduler_q],
            )
            subnode_managers.append(subnode_manager)
            subnode_manager_qs.append(manager_q)

            self.grp.add_process(
                nproc=1,
                template=ProcessTemplate(
                    target=_bootstrap_manager,
                    args=(manager_q,),
                    policy=manager_node_policy,
                ),
            )

        # Build the scheduler (scheduler queue is manager_qs[0])
        # ------------------------------------------------------
        # Publish manager_qs: index 0 = scheduler queue, indices 1-n = subnode queues.
        manager_qs = [scheduler_q] + subnode_manager_qs

        scheduler_manager = Manager(
            SCHEDULER_MANAGER_IDX,
            self._scheduler_workers,
            scheduler_q,
            self.ret_q,
            self.results_ddict,
            pool_node_huids=[self._scheduler_node_huid],
            physical_cores_per_node=self._workers_per_node_in_pool,
            disable_telem=self.disable_telem,
            is_scheduler=True,
            managed_lifecycle=self.managed_lifecycle,
            all_node_hostnames=self._all_node_hostnames,
            subnode_manager_qs=subnode_manager_qs,
        )
        self.manager_qs = manager_qs
        self.work_q = scheduler_q

        # Start the ProcessGroup for all managers (including scheduler)
        # -------------------------------------------------------------
        # Add the scheduler as a process placed on the first Batch node so the
        # scheduler runs independently of the creating client process.
        scheduler_policy = Policy(placement=Policy.Placement.HOST_NAME, host_name=scheduler_hostname)
        self.grp.add_process(
            nproc=1,
            template=ProcessTemplate(
                target=_bootstrap_manager,
                args=(scheduler_q,),
                policy=scheduler_policy,
            ),
        )

        self.log.debug(f"initializing and starting the process group of managers")
        self.grp.init()
        self.grp.start()

        # Bootstrap each subnode manager by sending it to its own work queue.
        for subnode_mgr, mgr_q in zip(subnode_managers, subnode_manager_qs):
            mgr_q.put(subnode_mgr)

            if self.log.isEnabledFor(logging.DEBUG):
                pool_node_huids = self._pool_node_huids_list[subnode_mgr.idx + 1]
                mgr_host = Node(pool_node_huids[0]).hostname
                pool_hosts = _compress_hostnames([Node(h).hostname for h in pool_node_huids])
                self.log.debug(
                    f"  subnode manager {subnode_mgr.idx}: process on {mgr_host}, "
                    f"pool={pool_hosts} ({subnode_mgr.num_workers} workers)"
                )

        # Bootstrap the scheduler by sending it to its scheduler queue.
        scheduler_q.put(scheduler_manager)
        self.log.debug(f"scheduler: {self._scheduler_workers} worker(s) on {scheduler_hostname} (process)")

    def _set_unique_attrs(self) -> None:
        """
        Set manager attributes that are unique to this client (most are generic and apply to all clients).
        """
        self.closed = False
        self._task_ctr = 0
        self.client_id = None
        # Pin the return queue to the node where this client is running so that
        # managers sending results back incur only a single hop.
        client_hostname = current().hostname
        self.ret_q = Queue(
            maxsize=return_queue_maxsize,
            block_size=default_block_size,
            policy=Policy(
                placement=Policy.Placement.HOST_NAME,
                host_name=client_hostname,
            ),
        )
        self.cq = LocalQueue(maxsize=client_completion_queue_maxsize)
        # Completions that arrive while ``cq`` is full (e.g. a client that never
        # calls poll()). Buffered here so the Dragon return queue keeps draining;
        # only the client request worker thread touches this buffer.
        self._completion_overflow = deque()
        self._client_request_q = LocalQueue()
        self._client_request_thread = None
        self._client_request_worker_exc = None
        self.client_compiler = None
        self.log = _setup_logging("client")
        # Serializes access to the shared results DDict client connector across
        # threads/tasks (see Task._get_result_tuple). The DDict client assumes
        # single-threaded use, so concurrent fetches must not overlap on it.
        self._results_ddict_lock = threading.Lock()
        self._log_manifest_lock = threading.Lock()
        self._task_log_metadata = OrderedDict()
        self._task_log_tasks = {}

    def _initialize_client_run_dir(self) -> None:
        if not self._task_logs_enabled:
            # Task logging disabled: do not create a runinfo directory or manifest.
            self._client_run_dir = None
            self._log_manifest_path = None
            return

        if self.client_id is None:
            raise RuntimeError("cannot initialize client run dir before client registration")

        client_run_dir = Path.cwd() / "runinfo" / self._run_id / f"client-{self.client_id}" / "task_logs"
        client_run_dir.mkdir(parents=True, exist_ok=True)
        self._client_run_dir = client_run_dir
        self._log_manifest_path = client_run_dir / "manifest.jsonl"

    def _resolve_task_output_paths(
        self,
        tuid: str,
        task_kind: str,
        target_label: Optional[str],
        name: Optional[str],
        stdout: Optional[str | Path],
        stderr: Optional[str | Path],
    ) -> tuple[Optional[str], Optional[str]]:
        resolved_stdout = _normalize_output_path(stdout) if stdout is not None else self._default_stdout
        resolved_stderr = _normalize_output_path(stderr) if stderr is not None else self._default_stderr

        if not self._task_logs_enabled:
            # Task logging disabled: only honor explicit (or default) paths and
            # leave the rest unset so task output inherits and is forwarded to
            # the client console. Never auto-generate per-task log files.
            if resolved_stdout is not None:
                _ensure_output_parent(resolved_stdout)
            if resolved_stderr is not None:
                _ensure_output_parent(resolved_stderr)
            return resolved_stdout, resolved_stderr

        if self._client_run_dir is None:
            raise RuntimeError("client run dir is not initialized")

        stem = _build_log_stem(task_kind, target_label, name, tuid)
        task_dir = self._client_run_dir / task_kind

        if resolved_stdout is None:
            resolved_stdout = str(task_dir / f"{stem}.out")
        if resolved_stderr is None:
            resolved_stderr = str(task_dir / f"{stem}.err")

        _ensure_output_parent(resolved_stdout)
        _ensure_output_parent(resolved_stderr)
        return resolved_stdout, resolved_stderr

    def _append_log_manifest_entry(
        self,
        tuid: str,
        task_kind: str,
        target_label: Optional[str],
        task_name: Optional[str],
        stdout_path: Optional[str],
        stderr_path: Optional[str],
    ) -> None:
        if self._log_manifest_path is None or self.client_id is None:
            raise RuntimeError("log manifest path is not initialized")

        entry = {
            "client_id": self.client_id,
            "tuid": tuid,
            "task_kind": task_kind,
            "target": target_label,
            "name": task_name,
            "stdout": stdout_path,
            "stderr": stderr_path,
            "log_dir": str(self._client_run_dir),
            "submit_host": current().hostname,
            "host": None,
            "hostnames": None,
            "submitted_at_ns": time.time_ns(),
            "completed_at_ns": None,
        }

        with self._log_manifest_lock:
            self._task_log_metadata[tuid] = entry

    def _write_log_manifest_locked(self) -> None:
        if self._log_manifest_path is None:
            raise RuntimeError("log manifest path is not initialized")

        with self._log_manifest_path.open("w", encoding="utf-8") as manifest_file:
            for entry in self._task_log_metadata.values():
                manifest_file.write(json.dumps(entry, sort_keys=True) + "\n")

    def _load_log_manifest_locked(self) -> None:
        if self._log_manifest_path is None or not self._log_manifest_path.exists():
            return

        if self._task_log_metadata:
            return

        for line in self._log_manifest_path.read_text(encoding="utf-8").splitlines():
            if not line:
                continue
            entry = json.loads(line)
            self._task_log_metadata[entry["tuid"]] = entry

    def _register_task_for_log_metadata(self, task: Task) -> None:
        self._task_log_tasks[task.uid] = task

    def _refresh_task_log_metadata_locked(self, tuid: Optional[str] = None) -> None:
        tuids = [tuid] if tuid is not None else list(self._task_log_metadata.keys())

        for current_tuid in tuids:
            entry = self._task_log_metadata.get(current_tuid)
            task = self._task_log_tasks.get(current_tuid)
            if entry is None or task is None:
                continue

            host, hostnames = self._resolve_task_host_metadata(task)
            if host is not None and entry.get("host") != host:
                entry["host"] = host
                entry["hostnames"] = hostnames

    def _mark_task_log_complete_locked(self, tuid: str, completed_at_ns: Optional[int] = None) -> None:
        entry = self._task_log_metadata.get(tuid)
        task = self._task_log_tasks.get(tuid)
        if entry is None or task is None:
            return

        host, hostnames = self._resolve_task_host_metadata(task)
        if host is not None and entry.get("host") != host:
            entry["host"] = host
            entry["hostnames"] = hostnames

        if entry.get("completed_at_ns") is None:
            entry["completed_at_ns"] = time.time_ns() if completed_at_ns is None else completed_at_ns

    def _resolve_task_host_metadata(self, task: Task) -> tuple[Optional[str], Optional[list[str]]]:
        hostnames: Optional[list[str]] = None

        if isinstance(task.core, JobCore) and task.core.hostname_list is not None:
            hostnames = list(task.core.hostname_list)
        elif task.core.manager_idx is not None:
            if task.core.manager_idx == SCHEDULER_MANAGER_IDX:
                hostnames = [self._scheduler_hostname]
            elif 0 <= task.core.manager_idx < len(self._subnode_manager_node_objs):
                hostnames = [self._subnode_manager_node_objs[task.core.manager_idx].hostname]

        if not hostnames:
            return None, None

        return _compress_hostnames(hostnames), hostnames

    def _refresh_task_log_metadata(self, tuid: Optional[str] = None) -> None:
        with self._log_manifest_lock:
            self._refresh_task_log_metadata_locked(tuid=tuid)

    def _mark_task_log_complete(self, tuid: str, completed_at_ns: Optional[int] = None) -> None:
        if not self._task_logs_enabled:
            return
        with self._log_manifest_lock:
            self._mark_task_log_complete_locked(tuid, completed_at_ns=completed_at_ns)

    def _flush_log_manifest(self) -> None:
        with self._log_manifest_lock:
            if self._log_manifest_path is None:
                return

            self._refresh_task_log_metadata_locked()
            self._write_log_manifest_locked()

    def log_dir(self) -> Path:
        """Return the per-client directory for the experimental log-discovery layer.

        This is the root of the current client's Batch logging area, typically
        ``runinfo/<batch-run-id>/client-<id>/task_logs``.

        :raises RuntimeError: If task logging is disabled (``Batch(task_logs=False)``).
        """
        if not self._task_logs_enabled:
            raise RuntimeError("task logging is disabled; construct Batch(task_logs=True) to enable it")
        if self._client_run_dir is None:
            raise RuntimeError("client run dir is not initialized")

        return self._client_run_dir

    def log_manifest_path(self) -> Path:
        """Return the path to the per-client JSONL manifest of task log files.

        The manifest is the index used by the experimental log-discovery helpers.
        Each record stores
        the task id, task kind, task/target names, log paths, and any host or
        completion metadata known to this client.

        :raises RuntimeError: If task logging is disabled (``Batch(task_logs=False)``).
        """
        if not self._task_logs_enabled:
            raise RuntimeError("task logging is disabled; construct Batch(task_logs=True) to enable it")
        if self._log_manifest_path is None:
            raise RuntimeError("log manifest path is not initialized")

        return self._log_manifest_path

    def iter_log_records(self) -> list[dict[str, Any]]:
        """Return the current client's task-log records from the manifest.

        This is the primary Python helper in the experimental log-discovery
        layer for inspecting all known task-log
        metadata without reading the JSONL file manually.

        :raises RuntimeError: If task logging is disabled (``Batch(task_logs=False)``).
        """
        if not self._task_logs_enabled:
            raise RuntimeError("task logging is disabled; construct Batch(task_logs=True) to enable it")
        self._refresh_task_log_metadata()
        with self._log_manifest_lock:
            return [dict(entry) for entry in self._task_log_metadata.values()]

    def find_logs(self, tuid: str) -> Optional[dict[str, Any]]:
        """Return the manifest record for *tuid*, or ``None`` if it is unknown.

        Use this experimental helper to locate a task's stdout/stderr files and any resolved
        host metadata by task id.

        :raises RuntimeError: If task logging is disabled (``Batch(task_logs=False)``).
        """
        if not self._task_logs_enabled:
            raise RuntimeError("task logging is disabled; construct Batch(task_logs=True) to enable it")
        self._refresh_task_log_metadata(tuid)
        with self._log_manifest_lock:
            entry = self._task_log_metadata.get(tuid)
            return dict(entry) if entry is not None else None

    def read_logs(self, tuid: str, log_type: Optional[str] = None, encoding: str = "utf-8") -> dict[str, Optional[str]]:
        """Read stdout/stderr log contents for *tuid* from the shared filesystem.

        This is part of Batch's experimental log-discovery layer.

        When *log_type* is ``"stdout"`` or ``"stderr"``, only that log file is read.
        Otherwise both log files are read.

        The return value is always a dictionary with ``stdout`` and ``stderr``
        keys. The selected log content is returned as a string, and any
        unrequested or unavailable side is returned as ``None``.

        :raises RuntimeError: If task logging is disabled (``Batch(task_logs=False)``).
        """
        if not self._task_logs_enabled:
            raise RuntimeError("task logging is disabled; construct Batch(task_logs=True) to enable it")
        entry = self.find_logs(tuid)
        if entry is None:
            raise KeyError(f"unknown task log record: {tuid}")

        if log_type is not None and log_type not in {"stdout", "stderr"}:
            raise ValueError("log_type must be one of: stdout, stderr, or None")

        streams = (log_type,) if log_type is not None else ("stdout", "stderr")
        result: dict[str, Optional[str]] = {"stdout": None, "stderr": None}

        for stream_name in streams:
            path = entry.get(stream_name)
            if path is not None:
                result[stream_name] = Path(path).read_text(encoding=encoding)

        return result

    def _resolve_process_template_outputs(
        self,
        template: ProcessTemplate,
        resolved_stdout: str,
        resolved_stderr: str,
    ) -> ProcessTemplate:
        cloned = copy.copy(template)

        template_stdout = _normalize_output_path(cloned.stdout) if isinstance(cloned.stdout, (str, Path)) else None
        if template_stdout is None:
            cloned.stdout = resolved_stdout
        else:
            cloned.stdout = template_stdout

        if cloned.stderr == Popen.STDOUT:
            cloned.stderr = Popen.STDOUT
        else:
            template_stderr = _normalize_output_path(cloned.stderr) if isinstance(cloned.stderr, (str, Path)) else None
            if template_stderr is None:
                cloned.stderr = resolved_stderr
            else:
                cloned.stderr = template_stderr

        if isinstance(cloned.stdout, str):
            _ensure_output_parent(cloned.stdout)
        if isinstance(cloned.stderr, str):
            _ensure_output_parent(cloned.stderr)

        return cloned

    def _summarize_template_log_paths(
        self,
        resolved_templates: list,
    ) -> tuple[Optional[str], Optional[str]]:
        stdout_paths = set()
        stderr_paths = set()

        for _, template in resolved_templates:
            if isinstance(template.stdout, str):
                stdout_paths.add(template.stdout)

            if template.stderr == Popen.STDOUT:
                if isinstance(template.stdout, str):
                    stderr_paths.add(template.stdout)
            elif isinstance(template.stderr, str):
                stderr_paths.add(template.stderr)

        summary_stdout = next(iter(stdout_paths)) if len(stdout_paths) == 1 else None
        summary_stderr = next(iter(stderr_paths)) if len(stderr_paths) == 1 else None
        return summary_stdout, summary_stderr

    def _init_client_compiler(self) -> None:
        if not self.manager_qs:
            return

        self.client_compiler = ClientCompiler(
            num_managers=self.num_managers,
            manager_qs=self.manager_qs,
            physical_cores_per_node=self._workers_per_node_in_pool,
            results_ddict_managers_per_pool=self._results_ddict_managers_per_pool,
            log=self.log,
        )

    def _detach_group_client(self) -> None:
        grp = getattr(self, "grp", None)
        if grp is None:
            return

        self._serialized_grp_mgr_puid = getattr(grp, "_mgr_p_uid", self._serialized_grp_mgr_puid)
        self._serialized_grp = cloudpickle.dumps(grp)
        with grp:
            pass
        self.grp = None

    def _ensure_group_client_attached(self) -> None:
        if self.grp is not None or self._serialized_grp is None:
            return

        self.grp = cloudpickle.loads(self._serialized_grp)

    def _stash_manager_queue_descriptors(self) -> None:
        manager_qs = getattr(self, "manager_qs", None)
        if manager_qs:
            self._serialized_manager_qs = [q.serialize() for q in manager_qs]

    def _ensure_manager_queues_attached(self) -> None:
        if getattr(self, "manager_qs", None):
            return

        serialized_qs = list(getattr(self, "_serialized_manager_qs", []))
        if not serialized_qs:
            return

        self.manager_qs = [Queue.attach(desc) for desc in serialized_qs]
        self.work_q = self.manager_qs[0] if self.manager_qs else None

    def _runtime_group_manager_alive(self) -> Optional[bool]:
        """Best-effort liveness check of the ProcessGroup manager backing this runtime."""
        mgr_puid = getattr(self, "_serialized_grp_mgr_puid", None)
        if mgr_puid is None:
            grp = getattr(self, "grp", None)
            mgr_puid = getattr(grp, "_mgr_p_uid", None)
        if mgr_puid is None:
            return None
        try:
            return self._is_runtime_group_manager_active(mgr_puid)
        except Exception:
            return None

    def _wait_for_runtime_shutdown(self, timeout: float = default_timeout) -> None:
        grp = getattr(self, "grp", None)
        if grp is None:
            self._ensure_group_client_attached()
            grp = self.grp

        if grp is None:
            return

        try:
            grp.join(timeout=timeout)
        except TimeoutError:
            raise RuntimeError("timed out waiting for batch runtime shutdown")

    def _close_local_client_queues(self, close_manager_qs: bool) -> None:
        ret_q = getattr(self, "ret_q", None)
        if ret_q is not None:
            try:
                ret_q.close()
            except Exception:
                pass
            self.ret_q = None

        if close_manager_qs:
            self._stash_manager_queue_descriptors()

            for manager_q in getattr(self, "manager_qs", []):
                try:
                    manager_q.close()
                except Exception:
                    pass

            self.manager_qs = []
            self.work_q = None

    def _finalize_runtime_shutdown(self) -> None:
        grp = getattr(self, "grp", None)
        if grp is None:
            try:
                self._ensure_group_client_attached()
                grp = self.grp
            except Exception:
                grp = None

        if grp is not None:
            try:
                grp.close()
            except Exception:
                pass

        results_ddict = getattr(self, "results_ddict", None)
        if results_ddict is not None:
            try:
                results_ddict.destroy()
            except Exception:
                pass

        for manager_q in getattr(self, "manager_qs", []):
            try:
                manager_q.close()
            except Exception:
                pass

        ret_q = getattr(self, "ret_q", None)
        if ret_q is not None:
            try:
                ret_q.close()
            except Exception:
                pass

        self._mark_unfetched_results_unavailable(
            "task result is unavailable because Batch.join() shut down the unmanaged runtime; "
            "fetch results before join() or use Batch(managed_lifecycle=True) and destroy() after retrieval"
        )

        self.grp = None
        self.manager_qs = []
        self.work_q = None
        self.ret_q = None
        self.results_ddict = None
        self._serialized_grp = None
        self._serialized_grp_mgr_puid = None
        self._serialized_manager_qs = []

        self._release_task_references()
        self.destroyed = True

    def _finalize_local_runtime_loss(self) -> None:
        grp = getattr(self, "grp", None)
        if grp is not None:
            try:
                grp.close()
            except Exception:
                pass
            self.grp = None

        results_ddict = getattr(self, "results_ddict", None)
        if results_ddict is not None:
            # The client-shared Batch instance (and its results DDict) is already gone, so we
            # must only release this client's local handle. ``destroy()`` would
            # send a blocking DDDestroy to an orchestrator that no longer
            # exists; ``detach()`` releases local resources without that
            # round-trip (and is a no-op for a non-creator handle).
            try:
                results_ddict.detach()
            except Exception:
                pass

        for manager_q in getattr(self, "manager_qs", []):
            try:
                manager_q.close()
            except Exception:
                pass

        ret_q = getattr(self, "ret_q", None)
        if ret_q is not None:
            try:
                ret_q.close()
            except Exception:
                pass

        self._mark_unfetched_results_unavailable(
            "task result is unavailable because the Batch runtime was already shut down"
        )

        self.grp = None
        self.manager_qs = []
        self.work_q = None
        self.ret_q = None
        self.results_ddict = None
        self._serialized_grp = None
        self._serialized_grp_mgr_puid = None
        self._serialized_manager_qs = []

        self._release_task_references()
        self.closed = True
        self.destroyed = True

    def _is_runtime_destroyed_exception(self, exc: BaseException) -> bool:
        current_exc = exc

        while current_exc is not None:
            if isinstance(current_exc, fli.DragonFLIObjectDestroyed):
                return True

            current_exc = current_exc.__cause__ or current_exc.__context__

        return False

    def _is_runtime_group_manager_active(self, manager_puid: int) -> bool:
        try:
            desc = process_query(manager_puid)
        except ProcessError:
            return False
        except Exception:
            return False

        return desc.state is ProcessDescriptor.State.ACTIVE

    def _is_runtime_unavailable_exception(self, exc: BaseException) -> bool:
        if self._is_runtime_destroyed_exception(exc):
            return True

        current_exc = exc

        while current_exc is not None:
            if isinstance(current_exc, RuntimeError):
                msg = str(current_exc)
                if "Failed to attach to ProcessGroup" in msg:
                    return True
                if "ProcessGroup manager is not in state" in msg:
                    return True
                if "serialized batch runtime is no longer active" in msg:
                    return True

            current_exc = current_exc.__cause__ or current_exc.__context__

        return False

    def _release_task_references(self) -> None:
        self._flush_log_manifest()
        task_log_tasks = getattr(self, "_task_log_tasks", None)
        if task_log_tasks is not None:
            task_log_tasks.clear()

    def _mark_unfetched_results_unavailable(self, message: str) -> None:
        for task in getattr(self, "_task_log_tasks", {}).values():
            task._results_ddict = None
            if not task._result_cached and not task._result_cleared:
                task._result_unavailable_message = message

    def _start_client_request_worker(self) -> None:
        if self._client_request_thread is not None:
            return

        self._client_request_thread = threading.Thread(
            target=self._run_client_request_worker,
            name=f"batch-client-{self.client_id}-requests",
            daemon=True,
        )
        self._client_request_thread.start()

    def _raise_client_request_worker_error(self) -> None:
        if self._client_request_worker_exc is not None:
            raise RuntimeError("client request worker failed") from self._client_request_worker_exc

    def _drain_client_return_queue(self) -> None:
        ret_q = self.ret_q
        if ret_q is None:
            return

        # Make room from any previously buffered overflow before pulling more.
        self._flush_completion_overflow()

        while True:
            try:
                item = ret_q.get_nowait()
            except queue.Empty:
                return

            if isinstance(item, CompletedTuids):
                completed_at_ns = time.time_ns()
                for tuid in item.tuids:
                    self._mark_task_log_complete(tuid, completed_at_ns=completed_at_ns)
                    self._enqueue_completion(tuid)
                continue

            if isinstance(item, ManagerException):
                raise RuntimeError(item.err_message) from item.exception

            raise RuntimeError(f"unexpected item received on client return queue: {item!r}")

    def _flush_completion_overflow(self) -> None:
        """Move buffered completions into the bounded completion queue in FIFO order.

        Only the client request worker thread mutates ``_completion_overflow``,
        so no extra locking is needed; ``cq`` is the thread-safe handoff to
        :py:meth:`poll`.
        """
        while self._completion_overflow:
            try:
                self.cq.put_nowait(self._completion_overflow[0])
            except queue.Full:
                return
            self._completion_overflow.popleft()

    def _enqueue_completion(self, tuid: str) -> None:
        """Publish one completed tuid for :py:meth:`poll`.

        The completion queue is bounded so it cannot grow without limit when a
        client never calls ``poll()``. Once it fills, further completions are
        buffered in ``_completion_overflow`` (preserving FIFO order) until
        ``poll()`` frees space.
        """
        if self._completion_overflow:
            # Preserve FIFO ordering: new completions must queue behind items
            # already waiting in the overflow buffer.
            self._completion_overflow.append(tuid)
            return

        try:
            self.cq.put_nowait(tuid)
        except queue.Full:
            self._completion_overflow.append(tuid)

    def _run_client_request_worker(self) -> None:
        deferred_item = None

        while True:
            item = None
            try:
                # Once the handle has been finalized (runtime gone or shut
                # down), ``ret_q`` is dropped out from under us. Exit cleanly
                # instead of dereferencing a now-``None`` return queue.
                if self.closed or self.destroyed or self.ret_q is None:
                    return

                self._drain_client_return_queue()

                if deferred_item is not None:
                    item = deferred_item
                    deferred_item = None
                else:
                    try:
                        item = self._client_request_q.get(timeout=0.05)
                    except queue.Empty:
                        continue

                if isinstance(item, Task):
                    task_batch, deferred_item = _drain_client_task_batch(
                        item,
                        self._client_request_q,
                        Task,
                    )

                    self._compile_and_dispatch_client_tasks(task_batch)
                    continue

                if isinstance(item, ClientStopRequest):
                    item.reply_q.put(None)
                    return

                if isinstance(item, ClientFlushRequest):
                    item.reply_q.put(None)
                    continue

                if isinstance(item, ClientFenceRequest):
                    fence_reply_q = Queue()
                    self.work_q.put(FenceRequest(self.client_id, fence_reply_q))
                    fence_complete = fence_reply_q.get()

                    if not isinstance(fence_complete, FenceComplete) or fence_complete.client_id != self.client_id:
                        raise RuntimeError(
                            f"expected FenceComplete for client_id={self.client_id}, got {fence_complete!r}"
                        )

                    self._reset_client_compile_state()
                    item.reply_q.put(fence_complete)
                    continue

                if isinstance(item, ClientCancelRequest):
                    self.work_q.put(
                        CancelRequest(self.client_id, item.task.core.tuid, item.task.core.manager_idx, item.reply_q)
                    )
                    continue

                self.work_q.put(item)
            except Exception as e:
                self._client_request_worker_exc = e
                if isinstance(item, (ClientFenceRequest, ClientFlushRequest, ClientStopRequest, ClientCancelRequest)):
                    item.reply_q.put(e)
                return

    def _wait_for_client_request(self, request_type, timeout: float = default_timeout) -> object:
        self._raise_client_request_worker_error()
        reply_q = LocalQueue()
        self._client_request_q.put(request_type(reply_q))
        item = reply_q.get(timeout=timeout)
        if isinstance(item, Exception):
            raise RuntimeError("client request worker failed") from item
        self._raise_client_request_worker_error()
        return item

    def _flush_client_request_worker(self, timeout: float = default_timeout) -> None:
        self._wait_for_client_request(ClientFlushRequest, timeout=timeout)

    def poll(self, timeout: float = default_timeout) -> Optional[str]:
        """Wait up to ``timeout`` seconds for the next completed task tuid."""
        self._raise_client_request_worker_error()

        try:
            item = self.cq.get(timeout=timeout)
        except queue.Empty:
            self._raise_client_request_worker_error()
            return None

        return item

    def _stop_client_request_worker(self, timeout: float = default_timeout) -> None:
        thread = self._client_request_thread
        if thread is None:
            return

        self._wait_for_client_request(ClientStopRequest, timeout=timeout)
        thread.join(timeout=timeout)
        self._client_request_thread = None

    def _enqueue_client_request(self, item) -> None:
        self._raise_client_request_worker_error()
        self._client_request_q.put(item)

    def _wait_for_client_completion(self, timeout: float = default_timeout) -> None:
        if self.client_id is None:
            raise RuntimeError("client is not registered with this batch")

        if self.closed:
            return

        if self._client_request_thread is None:
            raise RuntimeError("client request worker is not running")

        self._flush_client_request_worker(timeout=timeout)

        self._wait_for_client_request(ClientFenceRequest, timeout=timeout)

    def _detach_client(self, timeout: float = default_timeout) -> bool:
        if self.closed:
            return False

        runtime_destroyed = False

        try:
            self._wait_for_client_completion(timeout=timeout)
        except Exception as exc:
            if not self._is_runtime_unavailable_exception(exc):
                raise
            runtime_destroyed = True

        try:
            self._stop_client_request_worker(timeout=timeout)
        except Exception as exc:
            if not self._is_runtime_unavailable_exception(exc):
                raise
            runtime_destroyed = True
        finally:
            self._client_request_thread = None

        if runtime_destroyed:
            self._finalize_local_runtime_loss()
            return False

        try:
            return self._unregister(timeout=timeout, clean_detach=True)
        except Exception as exc:
            if not self._is_runtime_unavailable_exception(exc):
                raise
            self._finalize_local_runtime_loss()
            return False

    def _cancel_task(self, task: Task, timeout: float = default_timeout) -> bool:
        """Send a cancellation request and wait for the scheduler's response."""
        self._raise_client_request_worker_error()
        reply_q = Queue(block_size=default_block_size)

        try:
            self._enqueue_client_request(ClientCancelRequest(task, reply_q))
            item = reply_q.get(timeout=timeout)
            if isinstance(item, Exception):
                raise RuntimeError("client request worker failed") from item
            self._raise_client_request_worker_error()

            if not isinstance(item, CancelResponse) or item.tuid != task.core.tuid:
                raise RuntimeError(f"expected CancelResponse for tuid={task.core.tuid}, got {item!r}")

            return item.cancelled
        finally:
            try:
                reply_q.close()
            except Exception:
                pass

    def _compile_and_dispatch_client_tasks(self, tasks_to_compile: list["Task"]) -> None:
        if not tasks_to_compile:
            return

        if self.client_compiler is None:
            raise RuntimeError("client compiler not initialized")

        compiled_task = self.client_compiler.compile(tasks_to_compile, self.client_id)

        for work in compiled_task.subnode_work_chunks:
            self.work_q.put(work)

        if compiled_task.mnj_work_chunk is not None:
            self.work_q.put(compiled_task.mnj_work_chunk)

    def _reset_client_compile_state(self) -> None:
        if self.client_compiler is not None:
            self.client_compiler.reset()

    def _next_tuid(self) -> str:
        """Return the next client-local base task tuid."""
        task_seq = self._task_ctr
        self._task_ctr += 1
        return f"{self.client_id}-{task_seq}"

    def _register(self) -> None:
        """
        Registers a new client with the managers, i.e., adds the client id to the list of
        active clients and associates the client id with a return queue.

        :return: Returns None.
        :rtype: None
        """
        # get client_id from the primary manager
        self.log.debug(f"registering a new client")
        register_client = RegisterClient(self.ret_q, None)
        primary_manager = 0
        try:
            self.manager_qs[primary_manager].put(register_client)
        except Exception as exc:
            if self._is_runtime_destroyed_exception(exc):
                raise RuntimeError("cannot attach to batch runtime after it has been destroyed") from exc
            raise
        # Registration happens before the client request worker exists, so
        # manager-side bootstrap failures still have to come back directly on
        # the return queue rather than through the normal worker-exception path.
        item = self.ret_q.get()
        if isinstance(item, ManagerException):
            raise RuntimeError(item.err_message) from item.exception
        if not isinstance(item, int):
            raise RuntimeError(f"expected client_id from scheduler, got {item!r}")

        self.client_id = item
        self.log.debug(f"received new client_id={self.client_id}")

        # use the client_id to register with the remaining managers
        register_client = RegisterClient(self.ret_q, self.client_id)
        for manager_q in self.manager_qs[1:]:
            try:
                manager_q.put(register_client)
            except Exception as exc:
                if self._is_runtime_destroyed_exception(exc):
                    raise RuntimeError("cannot attach to batch runtime after it has been destroyed") from exc
                raise

    def _unregister(self, timeout: float = default_timeout, clean_detach: bool = False) -> bool:
        """
        Unregisters a client with the managers, i.e., removes the client id to the list of
        active clients.

        :return: Returns None.
        :rtype: None
        """
        if self.client_id is None:
            self.closed = True
            return False

        reply_q = Queue(block_size=default_block_size)
        last_client = False

        try:
            unregister_client = UnregisterClient(self.client_id, reply_q, clean_detach)
            for manager_q in self.manager_qs:
                manager_q.put(unregister_client)

            try:
                item = reply_q.get(timeout=timeout)
            except queue.Empty:
                item = False

            if isinstance(item, Exception):
                raise RuntimeError("client unregister failed") from item

            last_client = bool(item)
        finally:
            try:
                reply_q.close()
            except Exception:
                pass

        self.closed = True
        return last_client

    def read(self, obj, *channels) -> DataAccess:
        """
        Indicates READ accesses of a specified set of channels on a communication object. These
        accesses are not yet associated with a given task.

        :param obj: The communication object being accessed.
        :param *channels: A tuple of channels on the communcation object that will be read from.

        :return: Returns an descriptor for the data access that can be passed to (in a list) when creating a new task.
        :rtype: DataAccess
        """
        return DataAccess(AccessType.READ, obj, channels)

    def write(self, obj, *channels) -> DataAccess:
        """
        Indicates WRITE accesses of a specified set of channels on a communication object. These
        accesses are not yet associated with a given task.

        :param obj: The communication object being accessed.
        :param *channels: A tuple of channels on the communcation object that will be writtent o.

        :return: Returns an descriptor for the data access that can be passed to (in a list) when creating a new task.
        :rtype: DataAccess
        """
        return DataAccess(AccessType.WRITE, obj, channels)

    def fence(self, timeout: float = default_timeout) -> None:
        """
        Wait for all tasks submitted by this client to complete. Tasks submitted after
        the :class:`FenceRequest` is enqueued will be handled after the fence finishes.
        The client-side compile worker clears per-client compile state after the
        scheduler acknowledges the fence.

        :param timeout: Timeout in seconds for each blocking operation. Defaults to 1e9.
        :type timeout: float

        :return: Returns None.
        :rtype: None
        """
        self._wait_for_client_request(ClientFenceRequest, timeout=timeout)

    def close(self) -> None:
        """
        Deprecated no-op retained for API compatibility.

        Client detachment is now handled by :py:meth:`Batch.join`, which flushes
        pending local submissions, waits for this client's work to complete, and
        unregisters the client from the Batch instance shared by clients.

        :return: Returns None.
        :rtype: None

        .. deprecated::
           ``Batch.close()`` no longer changes Batch state. Use
           :py:meth:`Batch.join` when a client is done submitting work.
        """
        warnings.warn(
            "Batch.close() is deprecated and is now a no-op; use Batch.join() instead.",
            DeprecationWarning,
            stacklevel=2,
        )

    def join(self, timeout: float = default_timeout) -> None:
        """
        Wait for the completion of all operations started by this client, then
        detach this client from the Batch instance shared by clients.

        After ``join()`` returns, this handle can no longer submit additional
        work. In the default unmanaged mode, the client-shared Batch instance
        shuts down automatically after the last client detaches. In managed
        mode, any client may later call :py:meth:`Batch.destroy`, even after
        that handle has already joined. In unmanaged mode, fetch any task
        results you need before calling ``join()`` on the last attached client,
        because automatic shutdown destroys the shared results DDict. Use
        ``managed_lifecycle=True`` when results must remain retrievable after a
        handle has joined.

        :param float: A timeout value for waiting on batch completion. Defaults to 1e9.
        :type timeout: float

        :return: Returns None.
        :rtype: None
        """
        if self.destroyed or self.terminated or self.closed:
            return

        # If the client-shared Batch instance has already been torn down (for example another
        # client called destroy()), the scheduler can never answer the detach
        # handshake (flush/fence/unregister). Detect that up front via the
        # non-attaching manager-liveness probe and finalize local cleanup
        # instead of blocking on replies that will never arrive.
        if self._runtime_group_manager_alive() is False:
            self._finalize_local_runtime_loss()
            return

        last_client = self._detach_client(timeout=timeout)

        if self.managed_lifecycle:
            self._detach_group_client()
            self._close_local_client_queues(close_manager_qs=True)
            self._release_task_references()
            return

        if last_client:
            self._wait_for_runtime_shutdown(timeout=timeout)
            self._finalize_runtime_shutdown()
            return

        self._detach_group_client()
        self._close_local_client_queues(close_manager_qs=True)
        self._release_task_references()

    def destroy(self, timeout: float = default_timeout, force_timeout: Optional[float] = None) -> None:
        """
        Gracefully destroy the Batch instance shared by clients.

        Managed-lifecycle runtimes are shut down after all currently attached
        clients detach. The calling client is detached first if needed. When
        ``force_timeout`` is provided, managers shut down after that many
        seconds even if other clients remain attached.

        Calling ``destroy()`` on an unmanaged runtime raises ``RuntimeError``;
        the default mode shuts down automatically after the last client joins.

        :param timeout: Timeout in seconds for waiting on manager shutdown.
        :type timeout: float
        :param force_timeout: Optional grace period in seconds before forcing
            runtime shutdown even if other clients remain attached.
        :type force_timeout: Optional[float]

        :return: Returns None.
        :rtype: None
        """
        if self.destroyed or self.terminated:
            return

        if not self.managed_lifecycle:
            raise RuntimeError(
                "Batch.destroy() is only supported when managed_lifecycle=True; "
                "default Batch runtimes shut down automatically when the last client detaches"
            )

        # If another client already tore the runtime down, the detach/shutdown
        # handshake (and the wait for managers to exit) would block on a dead
        # scheduler. Detect that up front via the non-attaching manager-liveness
        # probe and finalize local cleanup instead, mirroring join().
        if self._runtime_group_manager_alive() is False:
            self._finalize_local_runtime_loss()
            return

        self._ensure_manager_queues_attached()

        if not self.closed:
            self._detach_client(timeout=timeout)

        destroy_called = DestroyCalled(force_timeout)
        manager_qs = getattr(self, "manager_qs", [])
        for manager_q in manager_qs:
            manager_q.put(destroy_called)

        self._wait_for_runtime_shutdown(timeout=timeout)
        self._finalize_runtime_shutdown()

    def terminate(self) -> None:
        """
        Force the termination of a Batch instance.

        :return: Returns None.
        :rtype: None
        """
        # sanity checks
        if self.terminated:
            return

        try:
            self._stop_client_request_worker()
        except Exception:
            pass

        grp = getattr(self, "grp", None)
        if grp is not None:
            grp.stop()
            grp.close()

        self.terminated = True

    def clear_results(self) -> None:
        """
        Wait for all outstanding tasks to complete then clear the results dict for
        this batch. This can be used to free up memory after tasks have completed
        and their results have been retrieved. Any task result that has not yet
        been fetched with :py:meth:`Task.get` becomes unavailable after this
        call.

        :return: Returns None.
        :rtype: None
        """
        self.fence()
        self.results_ddict.clear()

        for task in getattr(self, "_task_log_tasks", {}).values():
            if not task._result_cached:
                task._result_cleared = True

    def topology(self) -> BatchTopology:
        """
        Return a :py:class:`BatchTopology` describing the node placement of managers and
        worker pools in this Batch instance.

        The returned object reports:

        * the total number of nodes used,
        * the hostname where the dedicated scheduler runs,
        * the hostname of the pool node where each subnode manager process runs, and
        * for each worker pool, the hostnames of the nodes that make up that pool.

        Each requested node gets its own worker pool and its own subnode manager. The scheduler
        manager is an extra process colocated with the first Batch node and is not counted as one
        of the worker pools. All physical cores on every pool node are available as workers; no
        core is reserved for the manager.

        Example::

            batch = Batch(num_nodes=8)
            print(batch.topology())
            # Batch Topology:
            #   Total nodes  : 8
            #   Managers     : 9 total (1 scheduler + 8 subnode)
            #   Scheduler    : hotlum0001
            #   Worker pools : 8 pool(s) (1 dedicated subnode manager per pool)
            #     Pool 0 (1 node(s), 32 worker(s)): hotlum0001  [mgr: hotlum0001]
            #     Pool 1 (1 node(s), 32 worker(s)): hotlum0002  [mgr: hotlum0002]
            #     Pool 2 (1 node(s), 32 worker(s)): hotlum0003  [mgr: hotlum0003]
            #     ...

        .. tip::

            Install ``python-hostlist`` (``pip install python-hostlist``) to have
            hostnames in the output compressed into Slurm-style bracket notation,
            e.g. ``hotlum[0001-0008]`` instead of a long comma-separated list.
            This is especially helpful on large allocations.

        :return: A :py:class:`BatchTopology` object describing the placement.
        :rtype: BatchTopology
        """
        manager_hostnames = [n.hostname for n in self._subnode_manager_node_objs]

        pool_hostnames = [
            [Node(h_uid).hostname for h_uid in pool_huids] for pool_huids in self._pool_node_huids_list[1:]
        ]

        workers_per_pool_list = [
            len(huids) * self._workers_per_node_in_pool for huids in self._pool_node_huids_list[1:]
        ]

        return BatchTopology(
            total_nodes=len(self._node_objs),
            scheduler_hostname=self._scheduler_hostname,
            manager_hostnames=manager_hostnames,
            pool_hostnames=pool_hostnames,
            workers_per_pool=workers_per_pool_list,
        )

    def options(
        self,
        reads: Optional[list] = None,
        writes: Optional[list] = None,
        name: Optional[str] = None,
        timeout: float = default_timeout,
        stdout: Optional[str | Path] = None,
        stderr: Optional[str | Path] = None,
        pmi: PMIBackend = PMIBackend.CRAY,
    ) -> BatchOptionsProxy:
        """Return a submission proxy with Batch metadata bound ahead of time.

        This is the preferred API for task metadata. It cleanly separates Batch
        execution options from user payload arguments, following the same broad
        idea as Ray's ``.options(...)`` API.

        For process and job submissions, ``stdout`` and ``stderr`` supplied via
        :py:meth:`Batch.options` act as task-level defaults. If an individual
        :py:class:`ProcessTemplate` already specifies ``stdout`` or ``stderr``,
        the template setting takes precedence.
        """
        return BatchOptionsProxy(
            self,
            BatchTaskOptions(
                reads=reads,
                writes=writes,
                name=name,
                timeout=timeout,
                stdout=stdout,
                stderr=stderr,
                pmi=pmi,
            ),
        )

    def _warn_direct_submission_options(self, method_name: str, task_options: BatchTaskOptions) -> None:
        direct_options_used = self._direct_submission_option_names(task_options)

        if direct_options_used:
            warnings.warn(
                self._deprecated_direct_submission_options_message(method_name, direct_options_used),
                DeprecationWarning,
                stacklevel=3,
            )

    def _direct_submission_option_names(self, task_options: BatchTaskOptions) -> list[str]:
        option_names = []
        if task_options.reads is not None:
            option_names.append("reads")
        if task_options.writes is not None:
            option_names.append("writes")
        if task_options.name is not None:
            option_names.append("name")
        if task_options.timeout != default_timeout:
            option_names.append("timeout")
        if task_options.stdout is not None:
            option_names.append("stdout")
        if task_options.stderr is not None:
            option_names.append("stderr")
        if task_options.pmi != PMIBackend.CRAY:
            option_names.append("pmi")
        return option_names

    def _deprecated_direct_submission_options_message(self, method_name: str, option_names: list[str]) -> str:
        if method_name == "function":
            replacement = "Batch.options(...).function(fn, *args, **kwargs)"
        elif method_name == "process":
            replacement = "Batch.options(...).process(process_template)"
        elif method_name == "job":
            replacement = "Batch.options(...).job(process_templates)"
        else:
            replacement = f"Batch.options(...).{method_name}(...)"

        option_list = ", ".join(option_names)
        return (
            f"Passing Batch metadata directly to Batch.{method_name}() is deprecated. "
            f"Move these option(s) into Batch.options(...): {option_list}. "
            f"Use {replacement} instead."
        )

    def _submit_function(
        self,
        target: Callable,
        args: tuple,
        kwargs: dict,
        task_options: BatchTaskOptions,
    ) -> Function:
        if self.closed or self.destroyed or self.terminated:
            raise SubmitAfterCloseError("cannot submit work after this Batch client has joined or terminated")

        task = Function(
            self,
            target,
            args,
            kwargs,
            reads=task_options.reads,
            writes=task_options.writes,
            name=task_options.name,
            timeout=task_options.timeout,
            stdout=task_options.stdout,
            stderr=task_options.stderr,
        )
        self._enqueue_client_request(task)
        return task

    def _submit_process(self, process_template: ProcessTemplate, task_options: BatchTaskOptions) -> Job:
        if self.closed or self.destroyed or self.terminated:
            raise SubmitAfterCloseError("cannot submit work after this Batch client has joined or terminated")

        job = self._submit_job([(1, process_template)], task_options)
        job.core.is_parallel = False
        return job

    def _submit_job(self, process_templates: list, task_options: BatchTaskOptions) -> Job:
        if self.closed or self.destroyed or self.terminated:
            raise SubmitAfterCloseError("cannot submit work after this Batch client has joined or terminated")

        task = Job(
            self,
            process_templates,
            reads=task_options.reads,
            writes=task_options.writes,
            name=task_options.name,
            timeout=task_options.timeout,
            pmi=task_options.pmi,
            stdout=task_options.stdout,
            stderr=task_options.stderr,
        )
        self._enqueue_client_request(task)
        return task

    def function(
        self,
        # function args (except kwargs)
        target: Callable,
        *args,
        # task args
        reads: Optional[list] = None,
        writes: Optional[list] = None,
        name: Optional[str] = None,
        timeout: float = default_timeout,
        stdout: Optional[str | Path] = None,
        stderr: Optional[str | Path] = None,
        **kwargs,
    ) -> Function:
        """
        Creates a new function task. Arguments for the function that are of type :py:class:`Task`
        will create a dependency for this task on the output of the task specified by the
        argument. Further, the output of the specified task will be passed in place of the
        :py:class:`Task` argument when the function executes.

        .. deprecated::
           Passing Batch metadata such as ``reads``, ``writes``, ``name``,
           ``timeout``, ``stdout``, or ``stderr`` directly to
           :py:meth:`Batch.function` is deprecated. Use
           :py:meth:`Batch.options` to separate Batch metadata from user
           function arguments.

        :param func: The function to associate with the object.
        :param *args: The arguments for the function.
        :param reads: A list of ``Read`` objects created by calling :py:meth:`Batch.read`.
        :type reads: Optional[list]
        :param writes: A list of ``Write`` objects created by calling :py:meth:`Batch.write`.
        :type writes: Optional[list]
        :param name: A human-readable name for the task.
        :type name: Optional[str]
        :param stdout: Optional stdout log file path for this task. When omitted, Batch uses the
            client default. If the client was created with ``task_logs=True`` and no default is set,
            Batch auto-generates a per-task file in the client run directory; otherwise the task's
            stdout is forwarded to the client console.
            ``stdout`` is a reserved Batch submission keyword and is not passed
            through to the user function's ``**kwargs``.
        :type stdout: Optional[str | Path]
        :param stderr: Optional stderr log file path for this task. When omitted, Batch uses the
            client default. If the client was created with ``task_logs=True`` and no default is set,
            Batch auto-generates a per-task file in the client run directory; otherwise the task's
            stderr is forwarded to the client console.
            ``stderr`` is a reserved Batch submission keyword and is not passed
            through to the user function's ``**kwargs``.
        :type stderr: Optional[str | Path]

        :raises :py:exc:`SubmitAfterCloseError`: If this client handle has already
            been detached from the Batch runtime by :py:meth:`Batch.join`,
            :py:meth:`Batch.destroy`, or :py:meth:`Batch.terminate`.

        :return: The new function task.
        :rtype: Function
        """
        task_options = BatchTaskOptions(
            reads=reads,
            writes=writes,
            name=name,
            timeout=timeout,
            stdout=stdout,
            stderr=stderr,
        )
        self._warn_direct_submission_options("function", task_options)
        return self._submit_function(target, args, kwargs, task_options)

    def process(
        self,
        process_template: ProcessTemplate,
        reads: Optional[list] = None,
        writes: Optional[list] = None,
        name: Optional[str] = None,
        timeout: float = default_timeout,
        stdout: Optional[str | Path] = None,
        stderr: Optional[str | Path] = None,
    ) -> Job:
        """
        Creates a new process task. Arguments for a process passed using :py:attr:`ProcessTemplate.args`
        that are of type :py:class:`Task` will create a dependency for this task on the output of the
        task specified by the argument. Further, the output of the specified task will be
        passed in place of the :py:class:`Task` argument when the process executes.

        .. deprecated::
           Passing Batch metadata such as ``reads``, ``writes``, ``name``,
           ``timeout``, ``stdout``, or ``stderr`` directly to
           :py:meth:`Batch.process` is deprecated. Use
           :py:meth:`Batch.options(...).process(process_template)`` instead.

        :param process_template: A Dragon :py:class:`ProcessTemplate` to describe the process to be run.
        :type process_template: ProcessTemplate
        :param reads: A list of ``Read`` objects created by calling :py:meth:`Batch.read`.
        :type reads: Optional[list]
        :param writes: A list of ``Write`` objects created by calling :py:meth:`Batch.write`.
        :type writes: Optional[list]
        :param name: A human-readable name for the task.
        :type name: Optional[str]
        :param stdout: Optional stdout log file path for this task. When omitted, Batch uses the
            client default. If the client was created with ``task_logs=True`` and no default is set,
            Batch auto-generates a per-task file in the client run directory; otherwise the task's
            stdout is forwarded to the client console.
        :type stdout: Optional[str | Path]
        :param stderr: Optional stderr log file path for this task. When omitted, Batch uses the
            client default. If the client was created with ``task_logs=True`` and no default is set,
            Batch auto-generates a per-task file in the client run directory; otherwise the task's
            stderr is forwarded to the client console.
        :type stderr: Optional[str | Path]

        :raises :py:exc:`SubmitAfterCloseError`: If this client handle has already
            been detached from the Batch runtime by :py:meth:`Batch.join`,
            :py:meth:`Batch.destroy`, or :py:meth:`Batch.terminate`.

        :return: The new process task.
        :rtype: Job
        """
        task_options = BatchTaskOptions(
            reads=reads,
            writes=writes,
            name=name,
            timeout=timeout,
            stdout=stdout,
            stderr=stderr,
        )
        self._warn_direct_submission_options("process", task_options)
        return self._submit_process(process_template, task_options)

    def job(
        self,
        process_templates: list,
        reads: Optional[list] = None,
        writes: Optional[list] = None,
        name: Optional[str] = None,
        timeout: float = default_timeout,
        pmi: PMIBackend = PMIBackend.CRAY,
        stdout: Optional[str | Path] = None,
        stderr: Optional[str | Path] = None,
    ) -> Job:
        """
        Creates a new job task. Arguments for a process passed using :py:attr:`ProcessTemplate.args`
        that are of type :py:class:`Task` will create a dependency for this task on the output of the
        task specified by the argument. Further, the output of the specified task will be
        passed in place of the :py:class:`Task` argument when the job executes.

        .. deprecated::
           Passing Batch metadata such as ``reads``, ``writes``, ``name``,
           ``timeout``, ``pmi``, ``stdout``, or ``stderr`` directly to
           :py:meth:`Batch.job` is deprecated. Use
           :py:meth:`Batch.options(...).job(process_templates)`` instead.

        If any ``process_template`` includes an explicit host-name policy
        (``Policy(placement=Policy.Placement.HOST_NAME, host_name=...)``), Batch
        preserves that placement request. For multi-node jobs, Batch also treats
        those hostnames as allocation constraints and reserves those specific
        nodes before launch. Leave ``host_name`` unset if you want Batch to
        choose job placement automatically.

        :param process_templates: A list of pairs of the form (num_procs, process_template), where
        ``process_template`` is template for ``num_procs`` processes in the job. The process template
        is based on Dragon's :py:class:`ProcessTemplate`.
        :type process_templates: list
        :param reads: A list of ``Read`` objects created by calling :py:meth:`Batch.read`.
        :type reads: Optional[list]
        :param writes: A list of ``Write`` objects created by calling :py:meth:`Batch.write`.
        :type writes: Optional[list]
        :param name: A human-readable name for the task.
        :type name: Optional[str]
        :param pmi: The PMI backend to use for launching MPI jobs. Defaults to ``PMIBackend.CRAY``.
            Set to ``PMIBackend.PMIX`` for systems using PMIx, or ``None`` to disable PMI.
        :type pmi: PMIBackend
        :param stdout: Optional stdout log file path for this task. Multi-rank jobs share one stdout
            file across all ranks. When omitted, Batch uses the client default. If the client was
            created with ``task_logs=True`` and no default is set, Batch auto-generates a per-task
            file in the client run directory; otherwise the job's stdout is forwarded to the client
            console.
        :type stdout: Optional[str | Path]
        :param stderr: Optional stderr log file path for this task. Multi-rank jobs share one stderr
            file across all ranks. When omitted, Batch uses the client default. If the client was
            created with ``task_logs=True`` and no default is set, Batch auto-generates a per-task
            file in the client run directory; otherwise the job's stderr is forwarded to the client
            console.
        :type stderr: Optional[str | Path]

        :raises :py:exc:`SubmitAfterCloseError`: If this client handle has already been detached from the Batch runtime by :py:meth:`Batch.join`, :py:meth:`Batch.destroy`, or :py:meth:`Batch.terminate`.

        :return: The new job task.
        :rtype: Job
        """
        task_options = BatchTaskOptions(
            reads=reads,
            writes=writes,
            name=name,
            timeout=timeout,
            stdout=stdout,
            stderr=stderr,
            pmi=pmi,
        )
        self._warn_direct_submission_options("job", task_options)
        return self._submit_job(process_templates, task_options)

    def import_func(self, ptd_file: str, *real_import_args, **real_import_kwargs) -> MakeTask:
        """
        Loads the PTD dict and creates a :py:class:`MakeTask` object for the parameterized task group
        specified by the PTD file and import arguments (``real_import_args`` and ``real_import_kwargs``).
        The group of tasks is parameterized by the arguments passed to the :py:class:`MakeTask` object's
        :py:meth:`MakeTask.__call__` method, with a different task created for each unique collection of arguments.
        The name of this function comes from the fact that the :py:meth:`MakeTask.__call__` method of
        the :py:class:`MakeTask` object is meant to "look and feel" like calling the task and getting a
        return value without blocking, i.e., writing a serial program that runs locally, even though
        the tasks are lazily executed in parallel by the remote batch workers.

        :param ptd_file: Specifies the parameterized task group.
        :type ptd_file: str
        :param x: Positional arguments to replace the identifiers listed under the "import_args"
        key in the PTD file.
        :param x: Keyword arguments to replace the key/value identifiers specified under the
        "import_args" key in the PTD file.

        :return: Returns a :py:class:`MakeTask` object representing the parameterized group of tasks
        specified by the PTD file and import arguments.
        :rtype: MakeTask
        """
        with open(ptd_file) as file:
            ptd = yaml.safe_load(file)

        return MakeTask(
            self,
            ptd,
            real_import_args,
            real_import_kwargs,
        )
