"""Graph-based distributed scheduling for functions, executables, and parallel applications"""

import io
import logging
import networkx as nx
import queue
import random
import sys
import threading
import time
import traceback
import yaml

from collections import namedtuple
from collections.abc import Callable
from ...data.ddict.ddict import DDict
from ...dlogging.util import setup_BE_logging, DragonLoggingServices as dls
from ...infrastructure.facts import PMIBackend
from ...infrastructure.policy import Policy
from ...native.machine import current, Node, System
from ...native.pool import Pool
from ...native.process_group import ProcessGroup
from ...native.process import ProcessTemplate, Process, Popen
from ...native.queue import Queue
from ...telemetry.telemetry import Telemetry as dt
from enum import Enum, IntEnum
from .facts import (
    default_timeout,
    default_progress_timeout,
    default_block_size,
    manager_work_queue_max_batch_size,
    manager_work_queue_maxsize,
    primary_client_id,
    return_queue_maxsize,
)
from functools import singledispatchmethod
from pathlib import Path
from .proxy import ProxyObj
from queue import Empty, Queue as LocalQueue
from typing import Any, Iterable, Optional, TYPE_CHECKING
from uuid import uuid1

if TYPE_CHECKING:
    from ...fli import FLInterface


def _next_pow_of_2(x: int) -> int:
    """
    Get the next power of 2 greater than or equal to a given value.
    """
    if x == 0:
        return 1
    else:
        return 1 << (x - 1).bit_length()


def _setup_logging(context: str = None) -> logging.Logger:
    """
    Set up logging for a manager.
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


ResultWrapper = namedtuple(
    "ResultWrapper",
    ["compiled_tuid", "tuid", "result", "traceback", "stdout", "stderr", "raised"],
)
CompiledResultWrapper = namedtuple(
    "CompiledResultWrapper",
    ["tuid", "result_dict", "stdout_dict", "stderr_dict"],
)
AsyncWrapper = namedtuple("AsyncWrapper", ["async_result", "async_stdout", "async_stderr"])
ManagerException = namedtuple("ManagerException", ["tuid", "exception", "traceback", "err_message"])
DepSat = namedtuple("DepSat", ["source_tuid", "tuid", "compiled_tuid", "arg_dep_updates"])
DepSatRequest = namedtuple(
    "DepSatRequest",
    ["upstream_tuid", "tuid", "compiled_tuid", "arg_dep_updates", "reply_q", "client_id"],
)
ArgDepUpdate = namedtuple("ArgDepUpdate", ["template_idx", "arg_idx"])
HostInfo = namedtuple("HostInfo", ["tuid", "hostname"])
FollowerDone = namedtuple("FollowerDone", ["tuid", "compiled_tuid"])
CompletionNotification = tuple[str, str] | FollowerDone
RegisterClient = namedtuple("RegisterClient", ["ret_q", "client_id"])
UnregisterClient = namedtuple("UnregisterClient", ["client_id"])
JoinCalled = namedtuple("JoinCalled", [])
FenceRequest = namedtuple("FenceRequest", ["client_id", "reply_q", "done_event"])
ManagerFenceRequest = namedtuple("ManagerFenceRequest", ["client_id", "reply_q"])
FenceComplete = namedtuple("FenceComplete", ["client_id"])
DataAccess = namedtuple("DataAccess", ["access_type", "kvs", "keys"])
FrontierInfo = namedtuple("FrontierInfo", ["task_list", "access_type", "write_before_read"])


class AccessType(Enum):
    READ = 0
    WRITE = 1


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
    Exception for submitting work to a batch instance after it's been closed.
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
    def __init__(self, client_id: int, name: str, timeout: float) -> None:
        """
        Initializes a the core of the task, which is a lean representation of the task that
        is sent to the managers.

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
        self.tuid = str(uuid1())
        self.compiled_tuid = self.tuid
        self.cached_queue = None
        self.downstream_queues = []
        self.downstream_tuids = []
        self.upstream_queues = []
        self.upstream_tuids = []
        # for cross-compiled dependencies, keep parallel list of arg dep updates
        # corresponding to entries in `upstream_tuids` / `upstream_queues`
        self.upstream_arg_dep_updates = []
        self.downstream_arg_dep_updates = []
        self.num_dep_sat = 0
        self.num_dep_tot = 0

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
        return f"name={self.name}, client_id={self.client_id}, tuid={self.tuid}, compiled_tuid={self.compiled_tuid}"

    def _notify_dep_tasks(self, result) -> None:
        """
        Notifies all tasks that depend on this task of its completion.

        :return: Returns None.
        :rtype: None
        """
        if len(self.downstream_queues) > 0:
            compiled_tuid = self.compiled_tuid

            for q, tuid, arg_dep_update_list in zip(
                self.downstream_queues, self.downstream_tuids, self.downstream_arg_dep_updates
            ):
                arg_dep_updates = arg_dep_update_list if arg_dep_update_list is not None else []
                dep_sat = DepSat(self.tuid, tuid, compiled_tuid, arg_dep_updates)
                q.put(dep_sat)


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
        batch.log.debug(f"initializing task with core={task_core}, reads={reads}, writes={writes}, compiled={compiled}")

        self.core = task_core
        # non-core attributes are only needed by client code (i.e., can be deleted
        # when sending a work chunk to a manager)
        self._compiled_task = None
        self._batch = batch
        self.accesses = {}
        self.work_chunks = []
        self._manager_offset = None
        self._started = False
        self._ready = False
        self.exception = None
        self.traceback = None
        self.num_subtasks = 0
        self._result = None
        self._stdout = None
        self._stderr = None

        if compiled:
            self.dep_dag = nx.DiGraph()
        else:
            self.dep_dag = None

        if reads is not None:
            for read in reads:
                self._access(read)

        if writes is not None:
            for write in writes:
                self._access(write)

        # individual tasks should be thought of as singleton compiled tasks
        if compiled:
            self._compiled_task = self

    def _depends_on(
        self,
        task: "Task",
        compiled_task: "Task",
        arg_dep_update: Optional[list[ArgDepUpdate]] = None,
    ) -> None:
        """
        Sets a dependency for this task, which will be blocked until the completion of ``task``.

        :param task: The task that this one will be dependent on.
        :param compiled_task: The compiled task (sequence of tasks with possible dependencies) that
        these tasks and dependency belong to.

        :return: Returns None.
        :rtype: None
        """
        self.core.num_dep_tot += 1

        # Two cases based on whether the upstream task belongs to the same compiled task as self:
        #
        # 1. Same compiled_tuid: both tasks were submitted together in one `compile()` call.
        #    The compiled task's DAG is partitioned across managers, so the two tasks may end up
        #    on different managers. The upstream task_core tracks intra-compiled dependencies via
        #    `downstream_tuids` / `downstream_arg_dep_updates`, and calls `_notify_dep_tasks` (which puts a
        #    DepSat directly on the downstream task's manager queue) when the upstream one completes.
        #
        # 2. Different compiled_tuid: the upstream task belongs to a different (already-dispatched)
        #    compiled task, possibly on a different manager. Self records the upstream tuid in
        #    `upstream_tuids` and sends a DepSatRequest to the upstream manager when its Work chunk
        #    is dispatched; the upstream manager replies with a DepSat when that task finishes.

        if task.core.compiled_tuid == self.core.compiled_tuid:
            task.core.downstream_tuids.append(self.core.tuid)
            # `downstream_arg_dep_updates` is a parallel list to `downstream_tuids`, so we always append
            # to keep them in sync — even when there's no arg-passing dep (None means "no update
            # needed", and `_notify_dep_tasks` handles None by passing an empty list).
            task.core.downstream_arg_dep_updates.append(arg_dep_update)

            # TODO: this if-statement prevents arg-passing deps from being represented in
            # networkx dependency dag (this only really impacts visualization of the dag,
            # since DepSat messages are based on the downstream_tuids list in the task_core)
            if compiled_task is not None:
                if compiled_task.dep_dag is None:
                    compiled_task.dep_dag = nx.DiGraph()

                compiled_task.dep_dag.add_edge(task.core, self.core)
        else:
            self.core.upstream_tuids.append(task.core.tuid)
            # record arg_dep_update (may be None) for this upstream tuid so we can
            # create DepSatRequest messages to upstream managers containing the
            # arg-update indices (the actual values will be fetched from results_ddict)
            self.core.upstream_arg_dep_updates.append(arg_dep_update)

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
            self._depends_on(arg, arg._compiled_task, arg_dep_update_list)

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

    def _start(self) -> None:
        """
        Start this task by sending its work chunks to the managers. Currently, a task can only
        be started once and cannot be restarted after it completes. If a task is a subtask of
        a compiled task, then ``start`` must be called for the compiled task and not the subtask.

        :return: Returns None.
        :rtype: None
        """
        compiled_task = self._compiled_task

        # this is a program with multiple work chunks, so round-robin them
        # across managers
        manager_qs = compiled_task._batch.manager_qs
        num_managers = len(manager_qs)
        idx = compiled_task._manager_offset

        for work in compiled_task.work_chunks:
            manager_q = manager_qs[idx]
            manager_q.put(work)
            idx = (idx + 1) % num_managers

        compiled_task._started = True

    def _run(self, timeout: float = default_timeout) -> Any:
        """
        Starts a task and waits for it to complete. Currently, a task can only
        be started once and cannot be restarted after it completes.

        :param float timeout: The timeout for waiting. Defaults to 1e9.

        :raises TimeoutError: If the specified timeout is exceeded.
        :raises Exception: If this Task raised an exception while running. The exception raised by the
            task is propagated back to the host that started the task so it can be raised here.

        :return: Returns the result of the operation being waited on.
        :rtype: Any
        """
        self._start()
        return self.get(timeout)

    # TODO: add other options for checking completions, and take inspiration from Pool
    def get(self, block: bool = True, timeout: float = default_timeout) -> None:
        """
        Wait for this Task to complete. This function returns the task's result and prints
        any stdout/stderr output.

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
        if not block:
            if self.core.tuid not in self._batch.results_ddict:
                raise TaskNotReadyError(f"result not yet available: {self.core._get_id_str()}")

        try:
            # we're using wait_for_keys in the ddict, so this will wait for the
            # result to be ready
            result, tb, raised, stdout, stderr = self._batch.results_ddict[self.core.tuid]
        except KeyError:
            raise RuntimeError(f"no return value found for task: {self.core._get_id_str()}")

        # Print stdout and stderr if available
        if stdout:
            print(stdout)
        if stderr:
            print(stderr, file=sys.stderr)

        if raised:
            message = f"task with {self.core._get_id_str()} failed with the following traceback:\n{tb}"
            e = type(result)
            raise e(message)
        else:
            return result

    @property
    def uid(self):
        """
        Provides the unique ID for this task.
        """
        return self.core.tuid

    def dump_dag(self, file_name: str) -> None:
        """
        Dump a PNG image of the dependency DAG associated with a compiled program.

        :param file_name: Name for the new PNG file.

        :return: Returns None.
        :rtype: None
        """
        if self.dep_dag is None:
            raise RuntimeError(
                "no dependency DAG found for this task (only tasks with Kernighan-Lin scheduling have dumpable DAGs)"
            )

        pydot_graph = nx.drawing.nx_pydot.to_pydot(self.dep_dag)
        pydot_graph.write_png(f"{file_name}")


class FunctionCore(TaskCore):
    def __init__(
        self,
        client_id: int,
        name: str,
        timeout: float,
        target: Callable,
        args: tuple = (),
        kwargs: dict = {},
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
        super().__init__(client_id, name, timeout)

        self.func = target
        self.args = args
        self.kwargs = kwargs

    def _func_wrapper(self, output: list) -> None:
        """
        The wrapper for the user's function that captures stdout and stderr, and appends
        output values to the ``output`` list. This wrapper is run in a thread to handle
        the user-specified timeout.

        :param output: A list containing output from the function (necessary because the
        function is run in a thread).
        :type output: list

        :return: Returns None.
        :rtype: None
        """
        save_stdout = sys.stdout
        stdout = sys.stdout = io.StringIO()

        save_stderr = sys.stderr
        stderr = sys.stderr = io.StringIO()

        try:
            result = self.func(*self.args, **self.kwargs)
        finally:
            sys.stdout = save_stdout
            sys.stderr = save_stderr

        output.extend([result, stdout, stderr])

    def run(self) -> Any:
        """
        Runs the function associated with a function task.

        :return: Returns the return value of the function associated with the task.
        :rtype: Any
        """
        output = []

        if self.timeout >= default_timeout:
            # Fast path: no finite timeout requested, so skip thread creation and
            # call the wrapper directly. Thread spawn/join overhead (~50-100µs) would
            # otherwise dominate execution time for fast functions.
            self._func_wrapper(output)
        else:
            t = threading.Thread(target=self._func_wrapper, args=(output,))
            t.start()
            t.join(timeout=self.timeout)

        result, stdout, stderr = output
        return result, stdout.getvalue(), stderr.getvalue()


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
        # replace Task argument with None, since the actual value will be filled in
        # by the manager after the dependency is satisfied
        sanitized_args = tuple(None if isinstance(arg, Task) else arg for arg in args)
        super().__init__(
            FunctionCore(batch.client_id, name, timeout, target, sanitized_args, kwargs),
            batch,
            reads=reads,
            writes=writes,
        )
        self._handle_arg_passing_deps([args])


class JobCore(TaskCore):
    def __init__(
        self,
        client_id: int,
        name: str,
        timeout: float,
        process_templates: list[ProcessTemplate],
        pmi: PMIBackend = PMIBackend.CRAY,
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
        :param pmi: The PMI backend to use for launching MPI jobs. Defaults to ``PMIBackend.CRAY``.
            Set to ``PMIBackend.PMIX`` for systems using PMIx, or ``None`` to disable PMI.
        :type pmi: PMIBackend

        :raises RuntimeError: If there is an issue while setting up the dependency graph

        :return: Returns None.
        :rtype: None
        """
        super().__init__(client_id, name, timeout)

        self.num_procs = None
        self.process_templates = process_templates
        self.hostname_list = None
        self.is_parallel = True
        self.pmi = pmi

    def run(self) -> None:
        """
        Runs the job associated with a job task.

        :raises RuntimeError: If path to the target is invalid.

        :return: Returns None.
        :rtype: None
        """
        if self.is_parallel:
            pmi = self.pmi
        else:
            pmi = None

        grp = ProcessGroup(restart=False, pmi=pmi, walltime=self.timeout)

        proc_count = 0
        template_idx = 0

        for hostname in self.hostname_list:
            nprocs_this_template, template = self.process_templates[template_idx]
            base_policy = template.policy if template.policy is not None else Policy()
            template.policy = Policy.merge(
                base_policy, Policy(placement=Policy.Placement.HOST_NAME, host_name=hostname)
            )

            template.stdout = Popen.PIPE

            # TODO: support access to stdin/stdout/stderr
            grp.add_process(nproc=1, template=template)

            proc_count += 1
            if proc_count == nprocs_this_template:
                template_idx += 1
                proc_count = 0

        # TODO: is it possible to avoid global services in the single local process case?
        grp.init()
        grp.start()
        grp.join()

        # get puids and exit codes
        puids = []
        exit_codes = []
        proc_resources = []
        stdout = ""  # captures output from all batch jobs
        stderr = ""

        for puid, exit_code in grp.inactive_puids:
            puids.append(puid)
            exit_codes.append(exit_code)
            proc_resources.append(Process(None, ident=puid))

        for proc_idx in range(self.num_procs):
            conn_stdout = proc_resources[proc_idx].stdout_conn
            conn_stderr = proc_resources[proc_idx].stderr_conn

            if conn_stdout is not None:
                try:
                    while True:
                        stdout += conn_stdout.recv()
                except EOFError:
                    pass

            if conn_stderr is not None:
                try:
                    while True:
                        stderr += conn_stderr.recv()
                except EOFError:
                    pass

        grp.close()

        # TODO: Is this the right thing to do? Since we implement Process as a Job with a
        # single process, it makes things more intuitive for the user. The downside is that
        # it might lead to surprising results if the user creates a Job with a single process.
        if len(exit_codes) == 1:
            exit_codes = exit_codes[0]

        return exit_codes, stdout, stderr


class Job(Task):
    def __init__(
        self,
        batch,
        process_templates: list[ProcessTemplate],
        reads: Optional[list] = None,
        writes: Optional[list] = None,
        name: Optional[str] = None,
        timeout: float = default_timeout,
        pmi: PMIBackend = PMIBackend.CRAY,
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
        :param pmi: The PMI backend to use for launching MPI jobs. Defaults to ``PMIBackend.CRAY``.
            Set to ``PMIBackend.PMIX`` for systems using PMIx, or ``None`` to disable PMI.
        :type pmi: PMIBackend

        :return: Returns None.
        :rtype: None
        """
        if len(process_templates) == 0:
            raise RuntimeError("need at least one process template")

        total_procs = 0
        list_of_arg_lists = []

        # TODO: handle kwargs for arg-passing deps (probably changes elsewhere too)
        for nprocs_this_template, template in process_templates:
            total_procs += nprocs_this_template
            list_of_arg_lists.append(template.args)

            # replace Task argument with None, since the actual value will be filled in
            # by the manager after the dependency is satisfied
            sanitized_args = tuple(None if isinstance(arg, Task) else arg for arg in template.args)
            template.args = sanitized_args

        super().__init__(
            JobCore(batch.client_id, name, timeout, process_templates, pmi=pmi),
            batch,
            reads=reads,
            writes=writes,
        )

        self.core.num_procs = total_procs
        self._handle_arg_passing_deps(list_of_arg_lists)


class Work:
    def __init__(self, task_set: set, client_id: int, compiled_tuid: str) -> None:
        """
        Initialize a new work object for a program.

        :param task_set: Set containing all subtasks for this compiled task.
        :type task_set: set
        :param client_id: The id for the batch client creating this work object.
        :type client_id: int
        :param compiled_tuid: The tuid of the compiled task for this work object.
        :type tuid: str

        :return: Returns None.
        :rtype: None
        """
        self._client_id = client_id

        # divide tasks in this partition into a list of tasks with no dependencies,
        # and a dictionary mapping tuids to tasks

        self._ready_task_cores = {}
        self._blocked_task_cores = {}
        self._compiled_result_wrapper = CompiledResultWrapper(compiled_tuid, {}, {}, {})

        for task_core in task_set:
            if task_core.num_dep_tot == 0:
                self._ready_task_cores[task_core.tuid] = task_core
            else:
                self._blocked_task_cores[task_core.tuid] = task_core

    def _num_blocked_tasks(self) -> int:
        """
        Get the number of blocked tasks for this work item.

        :return: Returns the number of blocked tasks.
        :rtype: int
        """
        return len(self._blocked_task_cores)

    def _num_ready_tasks(self) -> int:
        """
        Get the number of tasks that are ready to run (no dependencies) for this work item.

        :return: Returns the number of ready tasks.
        :rtype: int
        """
        return len(self._ready_task_cores)


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


def _do_task_impl(task_core: TaskCore, results_ddict: DDict) -> tuple[str, str]:
    """
    Start a specified task and write results directly to the distributed dict.

    :param task_core: The core of the task to start.
    :type task_core: TaskCore
    :param results_ddict: Distributed dict to store results keyed by tuid.
    :type results_ddict: DDict

    :return: Returns tuple of (tuid, compiled_tuid) as completion notification.
    :rtype: tuple[str, str]
    """
    try:
        result, stdout, stderr = task_core.run()
        raised = False
        tb = None
        stdout_val = stdout.removesuffix("\n")
        stderr_val = stderr.removesuffix("\n")
    except Exception as e:
        result = e
        raised = True
        tb = _get_traceback()
        stdout_val = None
        stderr_val = None

    task_core._notify_dep_tasks(result)

    # Write result directly to the distributed dict
    results_ddict[task_core.tuid] = (result, tb, raised, stdout_val, stderr_val)

    return (task_core.tuid, task_core.compiled_tuid)


def _get_hostname_for_job(
    task_core: TaskCore,
    work_q: Queue,
    job_done_q: Queue,
    results_ddict: DDict,
) -> CompletionNotification:
    """
    This function (1) gets the hostname of the worker and sends it back to the manager,
    which uses it to help set the policy for an MPI job; and (2) it sleeps until the
    MPI job completes, reserving the core used by the worker for an MPI rank. This helps
    prevent oversubscription of nodes when running MPI jobs as tasks.

    :param task_core: The core of the task associated with the MPI job.
    :type task_core: TaskCore
    :param work_q: The work queue for the manager.
    :type work_q: Queue
    :param job_done_q: The queue used to wait for a "job complete" message, as well as the full list of hostnames for the job.
    :type job_done_q: Queue
    :param results_ddict: Distributed dict to store results keyed by tuid.
    :type results_ddict: DDict

    :return: Returns tuple of (tuid, compiled_tuid) if the job is launched, otherwise a
        ``FollowerDone`` sentinel for follower slots.
    :rtype: CompletionNotification
    """
    hostname = current().hostname

    if task_core.num_procs > 1:
        host_info = HostInfo(task_core.tuid, hostname)
        work_q.put(host_info)

        # get either (1) a list of hostnames for the MPI job (only one worker
        # receives this), or (2) a message indicating completion of the MPI job
        # (num_procs - 1 workers receive this)
        # TODO: since this is blocking, make sure we can't end up in a deadlock
        # situation where multiple MPI jobs are waiting for resources, and none
        # can make forward progress
        item = job_done_q.get()
    else:
        # there's only one rank, so just use this local host
        item = [hostname]

    if isinstance(item, list):
        task_core.hostname_list = item
        tuid_compiled_tuid = _do_task_impl(task_core, results_ddict)

        for _ in range(task_core.num_procs - 1):
            # put 0 onto all other "job done" queues just to indicate that the job
            # has completed and we can exit this function
            job_done_q.put(0)

        return tuid_compiled_tuid
    else:
        # Return a sentinel so the manager knows this follower slot is done and
        # can safely recycle the job_done_q once all followers have reported back.
        return FollowerDone(task_core.tuid, task_core.compiled_tuid)


def _do_task(task_and_args) -> CompletionNotification:
    """
    Wrapper around the user's task that is called by the workers.
    Returns either a task completion tuple or a ``FollowerDone`` sentinel.
    """
    task_core, args = task_and_args
    if isinstance(task_core, JobCore):
        # For jobs, args is (work_q, job_done_queue, results_ddict)
        tuid_compiled_tuid = _get_hostname_for_job(task_core, *args)
    else:
        # For regular tasks, args is (results_ddict,)
        results_ddict = args[0]
        tuid_compiled_tuid = _do_task_impl(task_core, results_ddict)

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
    ) -> None:
        """
        Initialize a manager.

        :param idx: This manager's index.
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
            ``None`` the pool is created without hostname placement (legacy behaviour).
        :type pool_node_huids: Optional[list[int]]
        :param physical_cores_per_node: Number of physical CPU cores per node (hyperthreads // 2).
            Used to determine how many workers to launch on each pool node.
        :type physical_cores_per_node: int
        :param disable_telem: Disables telemetry for the managers.
        :type disable_telem: bool

        :return: Returns None.
        :rtype: None
        """
        self.num_workers = num_workers
        self.idx = idx
        self.pool = None
        self.work = None
        self.work_q = work_q
        self.ret_q = {0: ret_q}
        self.results_ddict = results_ddict
        self.cached_queues = []
        self.primary_client_id = 0
        self.client_ctr = 1
        self.work_backlog = {}
        self.unexpected_dep_sat = {}
        self.job_hostname_lists = {}
        self._pending_followers = {}
        self.active_clients = {0}
        # Per-client sets of completed subtask tuids. Used to answer DepSatRequest
        # queries that arrive after completion. Keyed by client_id so each client's
        # set can be cleared independently when a fence completes for that client.
        self._completed_tuids: dict[int, set] = {}
        # Mapping from upstream_tuid -> list of
        # (reply_q, downstream_tuid, downstream_compiled_tuid, downstream_arg_dep_updates)
        # where reply_q is the queue to notify when upstream_tuid completes.
        self._dep_request_reply_map = {}
        # Number of in-flight Work chunks per client (client_id -> count). Incremented
        # when a Work chunk is accepted by this manager, decremented when it completes.
        self._pending_task_counts = {}
        # Fence requests waiting for a client's pending count to reach zero
        # (client_id -> reply_q).
        self._fence_requests = {}
        self._map_args_list = []
        self._compiled_task_list = []
        self._queued_task_count = 0
        self._async_queue = None
        self.join_called = False
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

    def __setstate__(self, state) -> None:
        """
        The manager is sent over a queue to each process in the ProcessGroup, so we create
        the worker pool and do other setup for the manager here.

        :param state: The manager state that's set when it's initially create by the client.

        :return: Returns None.
        :rtype: None
        """
        self.__dict__.update(state)

        policy_list = []

        if self.pool_node_huids:
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
            # Legacy fallback: no hostname placement, use first node for GPU discovery.
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
        try:
            task_core.cached_queue = self.cached_queues.pop()
        except:
            task_core.cached_queue = Queue(block_size=default_block_size)

        return task_core.cached_queue

    def _setup_job(self, task_core: TaskCore) -> None:
        """
        Queue up map args for each rank in the job.

        :param task_core: The core of the task for the job.
        :type task_core: TaskCore

        :return: Returns None.
        :rtype: None
        """
        self.log.debug(f"setting up map args for a job with {task_core._get_id_str()}")
        job_done_queue = self._get_queue_for_task(task_core)

        for _ in range(task_core.num_procs):
            self._map_args_list.append(
                (
                    task_core,
                    (self.work_q, job_done_queue, self.results_ddict),
                )
            )

        self.job_hostname_lists[task_core.tuid] = (
            task_core.num_procs,
            job_done_queue,
            [],
        )

        if task_core.num_procs > 1:
            # Register the pending-follower count now, before any worker is
            # dispatched to the pool. This prevents the race where follower
            # workers return FollowerDone before the leader's ResultWrapper is
            # processed by the manager, which would cause _handle_follower_done
            # to silently drop the sentinel (tuid not yet in _pending_followers).
            self._pending_followers[task_core.tuid] = [task_core.num_procs - 1, job_done_queue]

    def _add_to_map_args_list(self, task_core: TaskCore) -> None:
        """
        Add a map argument for this task, or one argument for each rank if the task is a job.

        :param task_core: The core of the task that is being set up to be run via map_async.
        :type task_core: TaskCore

        :return: Returns None.
        :rtype: None
        """
        if isinstance(task_core, JobCore):
            self._setup_job(task_core)
        else:
            # For regular tasks, include results_ddict as argument
            self._map_args_list.append((task_core, (self.results_ddict,)))

        self._queued_task_count += 1

    def _handle_manager_exception(
        self,
        e: Exception,
        err_msg: str,
        ret_q: Optional[Queue] = None,
        tuid: Optional[str] = None,
    ) -> None:
        """
        Handle a general "manager exception" unrelated to any task.

        :param e: The exception to be returned to the primary client.
        :type e: Exception
        :param err_msg: The error message to be returned to the primary client.
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

        :raises RuntimeError: If the client is not active.

        :return: Returns None.
        :rtype: None
        """
        self.log.debug(
            f"received work from client {work._client_id}: {len(work._ready_task_cores)} ready tasks, {len(work._blocked_task_cores)} blocked tasks"
        )

        # make sure this client is active
        self._is_active_client(work._client_id)

        # count this Work chunk as in-flight for the client so fence() knows when
        # all of the client's work has completed on this manager
        client_id = work._client_id
        self._pending_task_counts[client_id] = self._pending_task_counts.get(client_id, 0) + 1

        compiled_tuid = work._compiled_result_wrapper.tuid
        self.work_backlog[compiled_tuid] = work

        for _, task_core in work._ready_task_cores.items():
            self._add_to_map_args_list(task_core)

        for _, task_core in work._blocked_task_cores.items():
            # send DepSatRequest messages to the managers that own each upstream tuid
            for upstream_tuid, upstream_q, upstream_arg_dep_updates in zip(
                task_core.upstream_tuids, task_core.upstream_queues, task_core.upstream_arg_dep_updates
            ):
                arg_dep_updates = upstream_arg_dep_updates if upstream_arg_dep_updates is not None else []

                # reply queue for DepSat is this manager's work_q so DepSat messages
                # arrive as regular work-queue items and are handled by _handle_dep_sat
                dep_sat_req = DepSatRequest(
                    upstream_tuid,
                    task_core.tuid,
                    task_core.compiled_tuid,
                    arg_dep_updates,
                    self.work_q,
                    work._client_id,
                )

                upstream_q.put(dep_sat_req)

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
        self, task_core: TaskCore, arg_dep_updates: list[ArgDepUpdate], source_tuid: Optional[str] = None
    ) -> None:
        """
        Update task args using dep_sat.arg_dep_updates. ArgDepUpdate contains only
        indices; the actual value is fetched from results_ddict using source_tuid.
        """
        if len(arg_dep_updates) == 0:
            return

        # fetch the result value from the distributed dict
        if source_tuid is not None:
            try:
                result_tuple = self.results_ddict[source_tuid]
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
        tuid = dep_sat.tuid
        compiled_tuid = dep_sat.compiled_tuid

        try:
            # if the try succeeds, we have a compiled task that we are already working on
            work = self.work_backlog[compiled_tuid]

            # update the number of satisfied dependencies for this individual task
            # (which is part of a larger compiled task)
            task_core = work._blocked_task_cores[tuid]
            task_core.num_dep_sat += 1

            self._update_task_args(task_core, dep_sat.arg_dep_updates, source_tuid)

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

        # if all dependencies have been satisfied for this task, launch it and
        # decrement the number of unstarted sub-tasks for this compiled task
        if task_core.num_dep_sat == task_core.num_dep_tot:
            self._add_to_map_args_list(task_core)

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

            if upstream_tuid in self._completed_tuids.get(client_id, set()):
                # The upstream task is already complete; reply immediately.
                self.log.debug(
                    f"DepSatRequest: upstream {upstream_tuid} already complete, "
                    f"replying immediately for downstream {downstream_tuid} ({downstream_compiled_tuid})"
                )
                dep_sat = DepSat(upstream_tuid, downstream_tuid, downstream_compiled_tuid, arg_dep_updates)
                reply_q.put(dep_sat)
                return
            else:
                # Upstream task is not yet complete; record the request so we
                # can notify the requester when it completes.
                self.log.debug(
                    f"DepSatRequest: upstream {upstream_tuid} pending, "
                    f"registered downstream {downstream_tuid} ({downstream_compiled_tuid})"
                )
                entry = (reply_q, downstream_tuid, downstream_compiled_tuid, arg_dep_updates)
                try:
                    self._dep_request_reply_map[upstream_tuid].append(entry)
                except Exception:
                    self._dep_request_reply_map[upstream_tuid] = [entry]
                return
        except Exception as e:
            self._handle_manager_exception(e, "failed to handle DepSatRequest")

    def _handle_follower_done(self, follower_done: FollowerDone) -> None:
        """
        Handle a ``FollowerDone`` sentinel returned by a job follower worker.
        Decrements the pending-follower count for the job and recycles the
        job_done_q only after every follower slot has been released, preventing
        the queue from being handed to a new job while old followers might still
        hold a reference to it.

        :param follower_done: Contains the ``tuid`` of the completed follower's job.
        :type follower_done: FollowerDone

        :return: Returns None.
        :rtype: None
        """
        tuid = follower_done.tuid
        entry = self._pending_followers.get(tuid)
        if entry is not None:
            entry[0] -= 1
            self.log.debug(f"follower done for job tuid={tuid}, {entry[0]} followers pending")
            if entry[0] == 0:
                self.cached_queues.append(entry[1])
                del self._pending_followers[tuid]

    def _handle_host_info(self, host_info: HostInfo) -> None:
        """
        A "host info" message lets the manager know that a specific node is available
        for a job. This function handles the host-info message by adding the hostname
        to a list of hostnames for the job, and sending the list of hostnames to a worker
        if all hostnames have been received.

        :param host_info: Contains an availabele``hostname`` and ``tuid`` for a job.
        :type host_info: HostInfo

        :return: Returns None.
        :rtype: None
        """
        self.log.debug(f"received hostname={host_info.hostname} for task with {host_info.tuid}")

        num_procs, job_done_q, hostname_list = self.job_hostname_lists[host_info.tuid]
        hostname_list.append(host_info.hostname)

        if len(hostname_list) == num_procs:
            self.log.debug(f"sending {num_procs} hostnames to worker to start job")
            job_done_q.put(hostname_list)

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
            # each manager should receive this message only once per (non-primary)
            # batch instance, so return an exception back to the client
            e = RuntimeError("cannot register client more than once")
            me = ManagerException(None, e, _get_traceback(), f"client {client_id} is already registered")
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

        if client_id in self.active_clients:
            self.log.debug(f"unregistering client {client_id}")
            self.active_clients.remove(client_id)

    def _handle_join_called(self, join_called: JoinCalled) -> None:
        """
        Sets a flag indicating that join has been called by the client.

        :param join_called: An empty namedtuple that simply helps dispatch the correct method.
        :type join_called: JoinCalled

        :return: Returns None.
        :rtype: None
        """
        if not self.join_called:
            self.log.debug(f"join called")
            self.join_called = True

    @singledispatchmethod
    def _handle_request(self, item):
        e = RuntimeError("invalid item received on the work queue")
        self._handle_manager_exception(e, f"manager received {item}")

    @_handle_request.register
    def _(self, work: Work) -> None:
        try:
            self._handle_compiled_task(work)
        except Exception as e:
            self._handle_manager_exception(
                e,
                f"manager got exception when handling compiled-task request from client={work._client_id}, uid={work._compiled_result_wrapper.tuid}",
                self.ret_q[work._client_id],
                work._compiled_result_wrapper.tuid,
            )

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
    def _(self, host_info: HostInfo) -> None:
        try:
            self._handle_host_info(host_info)
        except Exception as e:
            compiled_tuid = host_info.tuid
            work = self.work_backlog[compiled_tuid]
            self._handle_manager_exception(
                e,
                "manager got exception when handling host-info message",
                self.ret_q[work._client_id],
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
    def _(self, join_called: JoinCalled) -> None:
        try:
            self._handle_join_called(join_called)
        except Exception as e:
            self._handle_manager_exception(e, "manager got exception when handling join-called message")

    @_handle_request.register
    def _(self, fence_request: ManagerFenceRequest) -> None:
        try:
            self._handle_fence_request(fence_request)
        except Exception as e:
            self._handle_manager_exception(e, "manager got exception when handling fence request")

    def _handle_fence_request(self, fence_request: ManagerFenceRequest) -> None:
        """
        Handle a fence request from a client. If the client has no in-flight Work
        chunks on this manager, reply immediately with FenceComplete; otherwise,
        record the request and reply when the last Work chunk for this client finishes.

        :param fence_request: Contains the client_id and the queue to reply on.
        :type fence_request: ManagerFenceRequest

        :return: Returns None.
        :rtype: None
        """
        client_id = fence_request.client_id
        reply_q = fence_request.reply_q

        if self._pending_task_counts.get(client_id, 0) == 0:
            reply_q.put(FenceComplete(client_id))
        else:
            self._fence_requests[client_id] = reply_q

    def _add_to_async_queue(self, tuid_notification: CompletionNotification) -> None:
        """Add completion notification (tuid) to the async queue."""
        self._async_queue.put(tuid_notification)

    def _launch_tasks(self) -> None:
        """
        Launch all queued tasks using map_async.

        :return: Returns None.
        :rtype: None
        """
        if len(self._map_args_list) == 0:
            return

        num_tasks = len(self._map_args_list)
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
            self._map_args_list,
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

        for tuid_compiled_tuid in tuid_compiled_tuid_list:
            # FollowerDone sentinels are returned by follower workers (all ranks of an
            # MPI job except the one that actually ran the ProcessGroup). Defer recycling
            # the job_done_q until every follower has reported back to avoid the race
            # where a recycled queue is assigned to a new job while old followers still
            # hold a reference to it.
            if isinstance(tuid_compiled_tuid, FollowerDone):
                self._handle_follower_done(tuid_compiled_tuid)
                continue

            num_tasks += 1

            tuid, compiled_tuid = tuid_compiled_tuid
            self.log.debug(f"individual task complete: tuid={tuid}, compiled_tuid={compiled_tuid}")

            work = self.work_backlog[compiled_tuid]
            task_core = work._ready_task_cores[tuid]
            del work._ready_task_cores[tuid]
            work_client_id = work._client_id

            if len(work._ready_task_cores) == 0 and len(work._blocked_task_cores) == 0:
                self.log.debug(f"compiled task complete: {compiled_tuid=}")

                # notify client that the compiled task is complete
                del self.work_backlog[compiled_tuid]

                # decrement in-flight count for this client; if it reaches zero and
                # a fence is pending, clear the per-client completed tuids (the client
                # will clear dep_frontier / tuid_to_manager_q, so no new DepSatRequests
                # will reference these tuids) and send FenceComplete back to the client
                new_count = self._pending_task_counts.get(work_client_id, 1) - 1
                if new_count == 0:
                    del self._pending_task_counts[work_client_id]
                    if work_client_id in self._fence_requests:
                        self._completed_tuids.pop(work_client_id, None)
                        reply_q = self._fence_requests.pop(work_client_id)
                        reply_q.put(FenceComplete(work_client_id))
                else:
                    self._pending_task_counts[work_client_id] = new_count

            # Recycle the queue used for this task. For multi-rank jobs the
            # job_done_q was already registered in _setup_job so _handle_follower_done
            # will recycle it once all followers report back; don't touch it here.
            if task_core.cached_queue is not None:
                num_procs = getattr(task_core, "num_procs", 1) or 1
                if num_procs == 1:
                    self.cached_queues.append(task_core.cached_queue)

            # notify any other managers that had requested DepSat for this tuid
            try:
                reply_list = self._dep_request_reply_map.pop(tuid)
                for reply_q, downstream_tuid, downstream_compiled_tuid, arg_dep_updates in reply_list:
                    try:
                        ds = DepSat(tuid, downstream_tuid, downstream_compiled_tuid, arg_dep_updates)
                        reply_q.put(ds)
                    except Exception:
                        self.log.debug(f"failed to send DepSat for {tuid=} to waiting manager")
            except Exception:
                pass

            # record that this subtask has completed, keyed by client so each
            # client's set can be cleared independently when its fence completes
            if work_client_id not in self._completed_tuids:
                self._completed_tuids[work_client_id] = set()
            self._completed_tuids[work_client_id].add(tuid)

        if not self.disable_telem:
            self.num_running_tasks -= num_tasks
            self.num_completed_tasks += num_tasks

            self.dragon_telem.add_data("num_running_tasks", self.num_running_tasks)
            self.dragon_telem.add_data("num_completed_tasks", self.num_completed_tasks)

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
            num_procs, _, hostname_list = self.job_hostname_lists[task_core.tuid]
            hosts_str = _compress_hostnames(hostname_list)
            self.log.debug(f"+     --> {len(hostname_list)} out of {num_procs} hosts reserved: {hosts_str}")
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
        self.log.debug(f"| join called={self.join_called}")
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

            # start any queued tasks in the worker pool
            try:
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
                self._map_args_list = []
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

            if self.join_called and len(self.active_clients) == 0 and len(self.work_backlog) == 0:
                break

            try:
                # if default_progress_timeout (=10) seconds have passed with no progress, dump
                # the current state of the batch service to help with debugging hangs
                self._dump_debug_state()
            except Exception as e:
                self._handle_manager_exception(e, f"failed to dump current state")

        if not self.disable_telem:
            try:
                self.dragon_telem.finalize()
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
                self._handle_manager_exception(e, f"failed to close cached job_done queue")


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
        file. Possible task attributes are the task's type, name, and timeout value.

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

            if task_type != TaskType.FUNC:
                pt = ProcessTemplate(target=target, args=args, kwargs=kwargs, cwd=cwd, env=env)
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
        if task_type == TaskType.FUNC:
            task = self._batch.function(
                target,
                *args,
                reads=reads,
                writes=writes,
                name=task_name,
                timeout=_get_timeout_val(timeout_dict),
                **kwargs,
            )
        elif task_type == TaskType.PROC:
            _, pt = pt_list[0]
            task = self._batch.process(
                process_template=pt,
                reads=reads,
                writes=writes,
                name=task_name,
                timeout=_get_timeout_val(timeout_dict),
            )
        elif task_type == TaskType.JOB:
            task = self._batch.job(
                process_templates=pt_list,
                reads=reads,
                writes=writes,
                name=task_name,
                timeout=_get_timeout_val(timeout_dict),
            )
        else:
            raise RuntimeError(f"invalid task type: {task_type}\n==> valid types: function, process, job")

        return task


class BatchTopology:
    """Describes the placement of managers and worker pools across nodes.

    Returned by :py:meth:`Batch.topology`.

    Each manager process is co-located on the first node of its worker pool.
    All physical cores on every pool node are available as workers; no core is
    reserved for the manager process.

    Hostnames in the string representation are compressed into Slurm-style
    bracket notation (e.g. ``node[001-004]``) when the ``python-hostlist``
    package is installed (``pip install python-hostlist``).  Without it,
    hostnames are listed verbatim, separated by commas.
    """

    def __init__(
        self,
        total_nodes: int,
        manager_hostnames: list[str],
        pool_hostnames: list[list[str]],
        workers_per_pool: list[int],
    ) -> None:
        self.total_nodes = total_nodes
        """Total number of nodes used by this Batch instance."""
        self.manager_hostnames = manager_hostnames
        """Hostname of the pool node where each manager process is co-located
        (the first node of the respective pool)."""
        self.pool_hostnames = pool_hostnames
        """List of hostname-lists, one inner list per worker pool."""
        self.workers_per_pool = workers_per_pool
        """Per-pool worker counts (physical cores x nodes in that pool)."""

    def __str__(self) -> str:
        lines = ["Batch Topology:"]
        lines.append(f"  Total nodes  : {self.total_nodes}")
        lines.append(f"  Worker pools : {len(self.pool_hostnames)} pool(s) (manager co-located on first pool node)")
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
            f"num_pools={len(self.pool_hostnames)}, "
            f"total_workers={total_workers})"
        )


# TODO: need to be able to pickle and unpickle Batch objects and send them
# to managers and workers to enable recursive function calls
class Batch:
    """Graph-based distributed scheduling for functions, executables, and parallel applications"""

    def __init__(
        self,
        num_nodes: Optional[int] = None,
        pool_nodes: Optional[int] = None,
        disable_telem: bool = False,
    ) -> None:
        """
        Create a Batch instance for orchestrating functions, executables, and parallel applications
        with data dependencies with a directed acyclic graph (DAG).

        :param num_nodes: Number of nodes to use for this Batch instance.  Defaults to all nodes
            in the allocation.  Values larger than the allocation are silently clamped.
        :type num_nodes: Optional[int]
        :param pool_nodes: Number of nodes in each worker pool (one pool per manager).  Defaults
            to 1.  Each pool node contributes ``node.num_cpus // 2`` workers (one per physical
            core).  The manager process for each pool is co-located on the pool's first node
            and does not reserve a core from the pool.
        :type pool_nodes: Optional[int]
        :param disable_telem: Indicates if telemetry should be disabled for this Batch instance. Defaults to False.
        :type disable_telem: bool

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
            tasks = [batch.function(gpu_matmul, m, base_dir, i,
                                    reads=[get_read(i)], writes=[get_write(i)], timeout=30)
                     for i in range(1000)]

            # Retrieve results — .get() waits for each task to complete if needed
            for task in tasks:
                try:
                    print(f"result={task.get()}")
                except Exception as e:
                    print(f"gpu_matmul failed with the following exception: {e}")

            batch.close()
            batch.join()


        :return: Returns None.
        :rtype: None
        """
        # ------------------------------------------------------------------ #
        # Node discovery and placement decisions                             #
        # ------------------------------------------------------------------ #

        alloc = System()
        all_node_objs = alloc._node_objs
        total_available = len(all_node_objs)

        # Clamp requested node count to what is actually available.
        if num_nodes is None or num_nodes > total_available:
            num_nodes = total_available
        num_nodes = max(1, num_nodes)

        node_objs = all_node_objs[:num_nodes]

        # Physical cores per node: Dragon cpu_count() reports hyperthreads across
        # all nodes; dividing each node's logical CPU count by 2 gives physical cores.
        cpus_per_node = max(1, node_objs[0].num_cpus // 2)

        # pool_nodes: how many nodes form one worker pool.
        if pool_nodes is None:
            pool_nodes = 1
        pool_nodes = max(1, pool_nodes)

        # All nodes are pool nodes.  Each manager process is co-located on the
        # first node of its own pool and does not reserve a core from the pool.
        pool_nodes_eff = min(pool_nodes, num_nodes)
        num_managers = max(1, num_nodes // pool_nodes_eff)

        # Distribute any remainder nodes to the first pools so every node is used.
        remainder = num_nodes - num_managers * pool_nodes_eff

        # Each pool node contributes cpus_per_node workers; no core reservation.
        workers_per_node_in_pool = cpus_per_node

        pool_node_huids_list: list[list[int]] = []
        node_offset = 0
        for pm_idx in range(num_managers):
            this_pool_nodes = pool_nodes_eff + (1 if pm_idx < remainder else 0)
            pool_node_huids_list.append([n.h_uid for n in node_objs[node_offset : node_offset + this_pool_nodes]])
            node_offset += this_pool_nodes

        # Each manager runs on the first node of its own pool.
        manager_node_objs = [Node(pool[0]) for pool in pool_node_huids_list]

        # Publish node topology for use by _setup_managers and topology().
        self._node_objs = node_objs
        self._manager_node_objs = manager_node_objs
        self._pool_node_huids_list = pool_node_huids_list
        self._cpus_per_node = cpus_per_node
        self._workers_per_node_in_pool = workers_per_node_in_pool

        # Use the first pool's node count as the representative worker count.
        self.num_workers = workers_per_node_in_pool * (len(pool_node_huids_list[0]) if pool_node_huids_list else 1)
        self.num_managers = num_managers

        self.work_q = LocalQueue(maxsize=manager_work_queue_max_batch_size)

        # this is the primary client and responsible for cleanup
        self.client_id = 0
        self.manager_qs = []
        self.tuid_to_manager_q = {}
        self.grp = None
        self.dep_frontier = {}

        self.stop_event = threading.Event()
        self.work_q_handler_thread = threading.Thread(target=self._handle_work_requests, daemon=True)
        self.work_q_handler_thread.start()

        # these flags are only used by the primary client, which is responsible for bringup and teardown
        self.closed = False
        self.terminated = False
        self.joined = False

        self.disable_telem = disable_telem

        # Create a distributed dict to store task results keyed by tuid.
        # One DDict manager per Batch node, with one gigabyte of memory each.
        one_gig = 1024**3

        self.results_ddict = DDict(
            n_nodes=num_nodes,
            managers_per_node=1,
            total_mem=(num_nodes * one_gig),
            wait_for_keys=True,
            working_set_size=2,
            name=str(uuid1()),
        )

        self._set_unique_attrs()
        # NOTE: self.log is set in _set_unique_attrs
        self.log.debug(str(self.topology()))

        self._setup_managers()

    def __del__(self) -> None:
        """
        Non-primary clients must close the batch if they haven't already done so.

        :return: Returns None.
        :rtype: None
        """
        if not self.closed:
            self._unregister()

        if self.client_id == primary_client_id and not (self.joined or self.terminated):
            self.terminate()

    def __setstate__(self, state) -> None:
        """
        Set state for a new client, including registering the client with the managers.

        :return: Returns None.
        :rtype: None
        """
        # a lot of the state is generic and can just be set using the origin __dict__
        self.__dict__.update(state)

        # update attributes that are unique to this batch instance
        self._set_unique_attrs()

        # register this client with the managers
        self._register()

    def _setup_managers(self) -> None:
        """
        Creates the managers for a batch, along with the process group for them.
        This should only be called by the primary client.

        :return: Returns None.
        :rtype: None
        """
        self.log.debug(f"setting up managers")

        self.grp = ProcessGroup(restart=False)
        managers = []

        for idx in range(self.num_managers):
            pool_node_huids = self._pool_node_huids_list[idx]

            # Manager process is co-located on the first node of its pool.
            manager_hostname = Node(pool_node_huids[0]).hostname
            manager_node_policy = Policy(placement=Policy.Placement.HOST_NAME, host_name=manager_hostname)

            # Place the manager's work queue on the same node as the manager process
            # so that queue operations stay local and avoid cross-node traffic.
            manager_q = Queue(
                maxsize=manager_work_queue_maxsize,
                block_size=default_block_size,
                policy=Policy(
                    placement=Policy.Placement.HOST_NAME,
                    host_name=manager_hostname,
                ),
            )

            # Worker count for this manager is determined by its pool's node count.
            num_workers_this_manager = len(pool_node_huids) * self._workers_per_node_in_pool

            manager = Manager(
                idx,
                num_workers_this_manager,
                manager_q,
                self.ret_q,
                self.results_ddict,
                pool_node_huids=pool_node_huids,
                physical_cores_per_node=self._workers_per_node_in_pool,
                disable_telem=self.disable_telem,
            )
            managers.append(manager)
            self.manager_qs.append(manager.work_q)

            self.grp.add_process(
                nproc=1,
                template=ProcessTemplate(
                    target=_bootstrap_manager,
                    args=(manager.work_q,),
                    policy=manager_node_policy,
                ),
            )

        self.log.debug(f"initializing and starting the process group of managers")
        self.grp.init()
        self.grp.start()

        # send the manager objects to start the bootstrap
        for manager, manager_q in zip(managers, self.manager_qs):
            manager_q.put(manager)

            if self.log.isEnabledFor(logging.DEBUG):
                pool_node_huids = self._pool_node_huids_list[manager.idx]
                mgr_host = Node(pool_node_huids[0]).hostname
                pool_hosts = _compress_hostnames([Node(h).hostname for h in pool_node_huids])

                self.log.debug(
                    f"  manager {manager.idx}: process on {mgr_host}, "
                    f"pool={pool_hosts} ({manager.num_workers} workers)"
                )

    def _set_unique_attrs(self) -> None:
        """
        Set manager attributes that are unique to this client (most are generic and apply to all clients).
        """
        self.closed = False
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
        self.log = _setup_logging("client")

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
        self.manager_qs[primary_manager].put(register_client)
        self.client_id = self.ret_q.get()
        self.log.debug(f"received new client_id={self.client_id}")

        # use the client_id to register with the remaining managers
        register_client = RegisterClient(self.ret_q, self.client_id)
        for manager_q in self.manager_qs[1:]:
            manager_q.put(register_client)

    def _handle_work_requests(self) -> None:
        """
        Get new work requests and compile and start them. This is meant to be run in a background
        thread for each client.

        :return: Returns None.
        :rtype: None
        """
        new_tasks = []
        pending_fence_req = None

        while not self.stop_event.is_set():
            if pending_fence_req is None:
                try:
                    task = self.work_q.get(timeout=5)
                except Empty:
                    continue
            else:
                task = pending_fence_req
                pending_fence_req = None

            if isinstance(task, FenceRequest):
                # Flush any tasks accumulated so far before starting the fence, so
                # that all work submitted before fence() is dispatched to the managers.
                if new_tasks:
                    try:
                        compiled_task = self.compile(new_tasks)
                        compiled_task._start()
                    except Exception as e:
                        self.log.exception(f"failed to compile/start background tasks: {e}")
                        raise
                    new_tasks = []

                # Send FenceRequest messages to all managers and wait for all
                # FenceComplete replies before resuming normal work processing.
                fence_request = task
                manager_fence_request = ManagerFenceRequest(fence_request.client_id, fence_request.reply_q)
                for manager_q in self.manager_qs:
                    manager_q.put(manager_fence_request)

                num_responses = 0
                num_managers = len(self.manager_qs)
                while num_responses < num_managers:
                    try:
                        item = self.ret_q.get(timeout=1)
                        if not isinstance(item, FenceComplete):
                            self.log.debug(f"expected FenceComplete but got {item}")
                            raise RuntimeError(f"expected FenceComplete but got {item}")
                        num_responses += 1
                    except Empty:
                        pass

                # Clear client-side dependency state so the next batch of work
                # starts from a clean slate, then signal fence() that we're done.
                self.dep_frontier.clear()
                self.tuid_to_manager_q.clear()
                fence_request.done_event.set()
                continue

            new_tasks.append(task)

            while True:
                try:
                    task = self.work_q.get_nowait()
                    if isinstance(task, FenceRequest):
                        # Save it and stop draining; handle it on the next outer iteration.
                        pending_fence_req = task
                        break
                    new_tasks.append(task)
                except Empty:
                    break

            try:
                compiled_task = self.compile(new_tasks)
                compiled_task._start()
            except Exception as e:
                self.log.exception(f"failed to compile/start background tasks: {e}")
                raise
            new_tasks = []

    def _unregister(self) -> None:
        """
        Unregisters a client with the managers, i.e., removes the client id to the list of
        active clients.

        :return: Returns None.
        :rtype: None
        """
        unregister_client = UnregisterClient(self.client_id)
        for manager_q in self.manager_qs:
            manager_q.put(unregister_client)

        self.closed = True

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

    def dump_background_dag(self, file_name: str | Path) -> None:
        """
        Compiles all background tasks for this batch and dumps a DAG for the compiled program.

        :param file_name: Filename for the dumped DAG.
        :type file_name: str | Path

        :return: Returns None.
        :rtype: None
        """
        task = self.compile(tasks_to_compile=None)
        task.dump_dag(file_name)

    def fence(self, timeout: float = default_timeout) -> None:
        """
        Wait for all tasks submitted by this client to complete. Tasks submitted after
        the :class:`FenceRequest` is enqueued will be compiled and dispatched after the
        fence finishes. After all managers confirm that the client's work is done, the
        accumulated dependency state (``dep_frontier``, ``tuid_to_manager_q``, and each
        manager's per-client set of completed tuids) is cleared so that the next batch of
        work starts from a clean slate.

        :param timeout: Timeout in seconds for each blocking operation. Defaults to 1e9.
        :type timeout: float

        :return: Returns None.
        :rtype: None
        """
        # Put the FenceRequest on the work queue. The background compiler thread
        # (_handle_work_requests) will flush any pending tasks, coordinate with the
        # managers, clear dep_frontier / tuid_to_manager_q, and set done_event
        # when the fence completes.
        done_event = threading.Event()
        fence_request = FenceRequest(self.client_id, self.ret_q, done_event)
        self.work_q.put(fence_request)
        done_event.wait(timeout=timeout)

    def close(self) -> None:
        """
        Indicates to the Batch service that no more work will be submitted to it. All clients
        must call this function, although it will be called by the ``__del__`` method of Batch
        if not called by the user. This should only be called once per client.

        :return: Returns None.
        :rtype: None
        """
        if self.closed:
            return

        self._unregister()

    def join(self, timeout: float = default_timeout) -> None:
        """
        Wait for the completion of a Batch instance. This function will block until all work
        submitted to the Batch service by all clients is complete, and all clients have called
        ``close``. Only the primary client (i.e., the one that initially created this Batch
        instance) should call ``join``, and it should be called after ``close``. This should
        only be called once by the primary client.

        :param float: A timeout value for waiting on batch completion. Defaults to 1e9.
        :type timeout: float

        :return: Returns None.
        :rtype: None
        """
        # sanity checks
        if not self.client_id == 0:
            raise RuntimeError("only primary client can call join")

        if self.joined or self.terminated:
            return

        # indicate that join has been called so the manager know to begin shutdown
        join_called = JoinCalled()
        for manager_q in self.manager_qs:
            manager_q.put(join_called)

        self.grp.join(timeout=timeout)
        self.grp.close()

        self.stop_event.set()
        self.work_q_handler_thread.join(timeout=timeout)

        # clean up ddict and queues
        self.results_ddict.destroy()
        for manager_q in self.manager_qs:
            manager_q.close()
        self.ret_q.close()

        self.joined = True

    def terminate(self) -> None:
        """
        Force the termination of a Batch instance. This should only be called by the primary
        client (i.e., the one that initially created this Batch instance), and it should only
        be called once. This will be called when the primary Batch object is garbage collected
        if the user has not called ``join`` or ``terminate``.

        :return: Returns None.
        :rtype: None
        """
        # sanity checks
        if self.terminated:
            return

        if self.grp is not None:
            self.grp.stop()
            self.grp.close()

        self.terminated = True

    def clear_results(self) -> None:
        """
        Wait for all outstanding tasks to complete then clear the results dict for
        this batch. This can be used to free up memory after tasks have completed
        and their results have been retrieved.

        :return: Returns None.
        :rtype: None
        """
        self.fence()
        self.results_ddict.clear()

    def topology(self) -> BatchTopology:
        """
        Return a :py:class:`BatchTopology` describing the node placement of managers and
        worker pools in this Batch instance.

        The returned object reports:

        * the total number of nodes used,
        * the hostname of the pool node where each manager process runs (first node of the pool), and
        * for each worker pool, the hostnames of the nodes that make up that pool.

        Each manager process is co-located on the first node of its own worker pool.  All
        physical cores on every pool node are available as workers; no core is reserved for
        the manager.

        Example::

            batch = Batch(num_nodes=8, pool_nodes=2)
            print(batch.topology())
            # Batch Topology:
            #   Total nodes  : 8
            #   Worker pools : 4 pool(s) (manager co-located on first pool node)
            #     Pool 0 (2 node(s), 64 worker(s)): hotlum[0001-0002]  [mgr: hotlum0001]
            #     Pool 1 (2 node(s), 64 worker(s)): hotlum[0003-0004]  [mgr: hotlum0003]
            #     Pool 2 (2 node(s), 64 worker(s)): hotlum[0005-0006]  [mgr: hotlum0005]
            #     Pool 3 (2 node(s), 64 worker(s)): hotlum[0007-0008]  [mgr: hotlum0007]

        .. tip::

            Install ``python-hostlist`` (``pip install python-hostlist``) to have
            hostnames in the output compressed into Slurm-style bracket notation,
            e.g. ``hotlum[0001-0008]`` instead of a long comma-separated list.
            This is especially helpful on large allocations.

        :return: A :py:class:`BatchTopology` object describing the placement.
        :rtype: BatchTopology
        """
        manager_hostnames = [n.hostname for n in self._manager_node_objs]

        pool_hostnames = [[Node(h_uid).hostname for h_uid in pool_huids] for pool_huids in self._pool_node_huids_list]

        workers_per_pool_list = [len(huids) * self._workers_per_node_in_pool for huids in self._pool_node_huids_list]

        return BatchTopology(
            total_nodes=len(self._node_objs),
            manager_hostnames=manager_hostnames,
            pool_hostnames=pool_hostnames,
            workers_per_pool=workers_per_pool_list,
        )

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
        **kwargs,
    ) -> Function:
        """
        Creates a new function task. Arguments for the function that are of type :py:class:`Task`
        will create a dependency for this task on the output of the task specified by the
        argument. Further, the output of the specified task will be passed in place of the
        :py:class:`Task` argument when the function executes.

        :param func: The function to associate with the object.
        :param *args: The arguments for the function.
        :param reads: A list of ``Read`` objects created by calling :py:meth:`Batch.read`.
        :type reads: Optional[list]
        :param writes: A list of ``Write`` objects created by calling :py:meth:`Batch.write`.
        :type writes: Optional[list]
        :param name: A human-readable name for the task.
        :type name: Optional[str]

        :raises :py:exc:`SubmitAfterCloseError`: If the batch has already been closed.

        :return: The new function task.
        :rtype: Function
        """
        if self.closed:
            raise SubmitAfterCloseError("cannot submit work after batch has been closed")

        task = Function(
            self,
            # function args
            target,
            args,
            kwargs,
            # task args
            reads=reads,
            writes=writes,
            name=name,
            timeout=timeout,
        )

        self.work_q.put(task)

        return task

    def process(
        self,
        process_template: ProcessTemplate,
        reads: Optional[list] = None,
        writes: Optional[list] = None,
        name: Optional[str] = None,
        timeout: float = default_timeout,
    ) -> Job:
        """
        Creates a new process task. Arguments for a process passed using :py:attr:`ProcessTemplate.args`
        that are of type :py:class:`Task` will create a dependency for this task on the output of the
        task specified by the argument. Further, the output of the specified task will be
        passed in place of the :py:class:`Task` argument when the process executes.

        :param process_template: A Dragon :py:class:`ProcessTemplate` to describe the process to be run.
        :type process_template: ProcessTemplate
        :param reads: A list of ``Read`` objects created by calling :py:meth:`Batch.read`.
        :type reads: Optional[list]
        :param writes: A list of ``Write`` objects created by calling :py:meth:`Batch.write`.
        :type writes: Optional[list]
        :param name: A human-readable name for the task.
        :type name: Optional[str]

        :raises :py:exc:`SubmitAfterCloseError`: If the batch has already been closed.

        :return: The new process task.
        :rtype: Job
        """
        if self.closed:
            raise SubmitAfterCloseError("cannot submit work after batch has been closed")

        job = self.job(
            [(1, process_template)],
            reads,
            writes,
            name,
            timeout,
        )
        job.core.is_parallel = False

        return job

    def job(
        self,
        process_templates: list,
        reads: Optional[list] = None,
        writes: Optional[list] = None,
        name: Optional[str] = None,
        timeout: float = default_timeout,
        pmi: PMIBackend = PMIBackend.CRAY,
    ) -> Job:
        """
        Creates a new job task. Arguments for a process passed using :py:attr:`ProcessTemplate.args`
        that are of type :py:class:`Task` will create a dependency for this task on the output of the
        task specified by the argument. Further, the output of the specified task will be
        passed in place of the :py:class:`Task` argument when the job executes.

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

        :raises :py:exc:`SubmitAfterCloseError`: If the batch has already been closed.

        :return: The new job task.
        :rtype: Job
        """
        if self.closed:
            raise SubmitAfterCloseError("cannot submit work after batch has been closed")

        task = Job(
            self,
            process_templates=process_templates,
            reads=reads,
            writes=writes,
            name=name,
            timeout=timeout,
            pmi=pmi,
        )

        self.work_q.put(task)

        return task

    def kernighan_lin_partition(self, tasks_to_compile: list[Task], dep_dag) -> list[set[TaskCore]]:
        """
        Partition tasks across managers using the Kernighan-Lin algorithm.

        Managers are treated as the partitions in the KL model. The resulting partition list
        contains one task-core set per manager, and may contain empty sets for managers that
        receive no work.

        :param tasks_to_compile: Tasks in the compiled DAG.
        :type tasks_to_compile: list[Task]
        :param dep_dag: The dependency DAG built by :py:meth:`compile`.

        :return: A list of task-core sets, one per manager partition.
        :rtype: list[set[TaskCore]]
        """
        # partition the dependency graph for the managers, subdiving partitions
        # until there is one partition per manager, or until it doesn't make sense
        # to keep partitioning

        self.log.debug("partitioning the dependency graph")

        if self.num_managers > 1 and len(tasks_to_compile) > 1:
            dep_graph = dep_dag.to_undirected()

            # If all inter-task dependencies are cross-compiled (inter-batch), dep_dag will
            # have no edges. kernighan_lin_bisection requires at least one edge, so fall back
            # to using round-robin across partitions in that case.
            if dep_graph.number_of_edges() > 0:
                partitions = list(nx.community.kernighan_lin_bisection(dep_graph))
            else:
                # No intra-batch edges: distribute tasks round-robin.
                # Cap at len(tasks_to_compile) to avoid creating empty partitions,
                # which would leave stale entries in work_backlog and prevent managers
                # from ever reaching the shutdown condition.
                n = min(len(tasks_to_compile), self.num_managers)
                partitions = [set() for _ in range(n)]
                for i, task in enumerate(tasks_to_compile):
                    partitions[i % n].add(task.core)

            while len(partitions) < self.num_managers:
                try:
                    new_partitions = []
                    made_progress = False
                    for part in partitions:
                        g = dep_graph.subgraph(part)
                        # only bisect partitions with >1 node and at least one edge;
                        # edgeless subgraphs arise when all deps in the partition are
                        # inter-batch and were not captured as edges in dep_dag
                        if len(part) > 1 and g.number_of_edges() > 0:
                            new_partitions.extend(nx.community.kernighan_lin_bisection(g))
                            made_progress = True
                        else:
                            new_partitions.append(part)

                    partitions = new_partitions

                    if not made_progress:
                        break
                except Exception:
                    raise RuntimeError("failure occurred while trying to bisect dependency graph")

            # A bisection step can overshoot num_managers (each bisect() adds 2 at
            # once, so going from num_managers-1 to num_managers+1 is possible).
            # Redistribute round-robin so each manager gets at most one Work chunk
            # for this compiled_tuid; sending two to the same manager would
            # overwrite work_backlog and permanently block tasks.
            if len(partitions) > self.num_managers:
                merged = [set() for _ in range(self.num_managers)]
                for i, part in enumerate(partitions):
                    merged[i % self.num_managers] |= part
                partitions = merged
        else:
            task_core_set = {task.core for task in tasks_to_compile}
            partitions = [task_core_set]

        return partitions

    def compile(
        self,
        tasks_to_compile: list[Task],
        name: Optional[str] = None,
    ) -> Task:
        """
        Generate a single compiled task from a list of tasks. :py:class:`Batch` calls this method
        internally to batch and dispatch tasks submitted via :py:meth:`function`,
        :py:meth:`process`, and :py:meth:`job`. The ordering of ``tasks_to_compile`` is treated as
        a valid sequential execution order; dependencies are inferred from data accesses to produce
        a DAG that maximizes parallelism.

        :param tasks_to_compile: List of tasks to compile.
        :type tasks_to_compile: list

        :raises RuntimeError: If there is an issue while setting up the dependency graph

        :return: The compiled task.
        :rtype: Task
        """
        # check that tasks_to_compile is a non-empty list of tasks
        if not isinstance(tasks_to_compile, list):
            raise RuntimeError(f"tasks_to_compile must be a list of tasks: {tasks_to_compile=}")
        else:
            num_tasks = len(tasks_to_compile)
            if num_tasks == 0:
                raise RuntimeError("cannot compile an empty list of tasks")
            elif not isinstance(tasks_to_compile[0], Task):
                raise RuntimeError(f"tasks_to_compile must be a list of tasks: {tasks_to_compile[0]=}")

        compiled_task_core = TaskCore(self.client_id, name, None)
        compiled_task = Task(compiled_task_core, self, compiled=True)

        for task in tasks_to_compile:
            # NOTE: this assumes all subtasks of a compiled task are unique
            compiled_task.dep_dag.add_node(task.core)

            compiled_task.num_subtasks += 1

            task.core.compiled_tuid = compiled_task.core.tuid
            task._compiled_task = compiled_task

            # compare each task's accesses to a access frontier to determine inter-task dependencies

            for kvs_and_key, task_and_access_type in task.accesses.items():
                task, access_type = task_and_access_type

                # if key isn't in dep_frontier, then there are no previous accesses to
                # this key, so we don't need to add any dependencies
                if kvs_and_key not in self.dep_frontier:
                    if access_type == AccessType.WRITE:
                        write_before_read = task
                    else:
                        write_before_read = None

                    self.dep_frontier[kvs_and_key] = FrontierInfo([task], access_type, write_before_read)
                    continue

                frontier_info = self.dep_frontier[kvs_and_key]
                prev_task_list, prev_access_type, write_before_read = frontier_info

                if prev_access_type == AccessType.READ:
                    if access_type == AccessType.READ:
                        # prev=READ, curr=READ
                        if write_before_read is not None:
                            task._depends_on(write_before_read, compiled_task)

                        prev_task_list.append(task)
                    elif access_type == AccessType.WRITE:
                        # prev=READ, curr=WRITE
                        for prev_read in prev_task_list:
                            task._depends_on(prev_read, compiled_task)

                        self.dep_frontier[kvs_and_key] = FrontierInfo([task], access_type, task)
                else:
                    if prev_access_type != AccessType.WRITE:
                        raise RuntimeError("invalid access mode")

                    if access_type == AccessType.READ:
                        # prev=WRITE, curr=READ
                        task._depends_on(prev_task_list[0], compiled_task)
                        self.dep_frontier[kvs_and_key] = FrontierInfo([task], access_type, write_before_read)
                    else:
                        # prev=WRITE, curr=WRITE
                        task._depends_on(prev_task_list[0], compiled_task)
                        self.dep_frontier[kvs_and_key] = FrontierInfo([task], access_type, task)

        # partition the dependency graph for the managers
        self.log.debug("partitioning the dependency graph")
        partitions = self.kernighan_lin_partition(tasks_to_compile, compiled_task.dep_dag)
        self.log.debug(
            f"compiled_tuid={compiled_task.core.tuid}: {len(tasks_to_compile)} task(s) "
            f"-> {len(partitions)} partition(s): " + ", ".join(f"{len(p)} task(s)" for p in partitions)
        )

        # establish mapping between tasks and managers/queues

        self.log.debug("setting up the mapping between tasks and managers")

        compiled_task._manager_offset = random.randrange(self.num_managers)

        for base_idx, task_set in enumerate(partitions):
            idx = (base_idx + compiled_task._manager_offset) % self.num_managers
            manager_q = self.manager_qs[idx]

            for task_core in task_set:
                self.tuid_to_manager_q[task_core.tuid] = manager_q

        # now that we know the mapping between tasks and managers/queues, we can
        # update the list of dependent tuids to make it a list of dependent queues

        self.log.debug("setting up dependency queues")

        for task_set in partitions:
            for task_core in task_set:
                for downstream_tuid in task_core.downstream_tuids:
                    q = self.tuid_to_manager_q[downstream_tuid]
                    task_core.downstream_queues.append(q)

                for upstream_tuid in task_core.upstream_tuids:
                    q = self.tuid_to_manager_q.get(upstream_tuid, None)
                    if q is None:
                        raise RuntimeError(
                            f"failed to find manager queue for upstream task {upstream_tuid} while compiling inter-compiled dependency"
                        )
                    task_core.upstream_queues.append(q)

        # create a work item for each manager
        for task_set in partitions:
            work = Work(
                task_set,
                self.client_id,
                compiled_task.core.tuid,
            )
            compiled_task.work_chunks.append(work)

        return compiled_task

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
