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
from ...native.machine import cpu_count, current, Node, System
from ...native.pool import Pool
from ...native.process_group import ProcessGroup
from ...native.process import ProcessTemplate, Process, Popen
from ...native.queue import Queue
from ...telemetry.telemetry import Telemetry as dt
from enum import Enum, IntEnum
from .facts import (
    default_workers_per_manager,
    default_timeout,
    default_progress_timeout,
    manager_work_queue_max_batch_size,
    manager_work_queue_maxsize,
    primary_client_id,
    return_queue_maxsize,
)
from functools import singledispatchmethod
from pathlib import Path
from .proxy import ProxyObj
from queue import Queue as LocalQueue
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
    # TODO: change TA to something else
    if context is None:
        file_name = f"BATCH_{current().hostname}.log"
    else:
        file_name = f"BATCH_{context}_{current().hostname}.log"

    setup_BE_logging(service=dls.TA, fname=file_name)
    log = logging.getLogger(__name__)
    return log


def _get_traceback() -> str:
    """
    Gets the current traceback.

    :return: Returns the traceback.
    :rtype: str
    """
    return traceback.format_exc().replace("\\n", "\n").replace("\n", "\n> ")


ResultWrapper = namedtuple(
    "ResultWrapper",
    ["compiled_tuid", "tuid", "result", "traceback", "stdout", "stderr", "raised"],
)
CompiledResultWrapper = namedtuple(
    "CompiledResultWrapper",
    ["tuid", "result_dict", "stdout_dict", "stderr_dict"],
)
AsyncWrapper = namedtuple(
    "AsyncWrapper", ["async_result", "async_stdout", "async_stderr"]
)
ManagerException = namedtuple(
    "ManagerException", ["tuid", "exception", "traceback", "err_message"]
)
DepSat = namedtuple("DepSat", ["tuid", "compiled_tuid", "input_tuples"])
DepOutputIdxs = namedtuple("DepOutputIdxs", ["result_idx", "stdout_idx", "stderr_idx"])
HostInfo = namedtuple("HostInfo", ["tuid", "hostname"])
RegisterClient = namedtuple("RegisterClient", ["ret_q", "client_id"])
UnregisterClient = namedtuple("UnregisterClient", ["client_id"])
JoinCalled = namedtuple("JoinCalled", [])
DataAccess = namedtuple("DataAccess", ["access_type", "comm_object", "channels"])


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
        self.dep_queues = []
        self.dep_tuids = []
        self.dep_output_idxs = []
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

    def _notify_dep_tasks(self, result, stdout, stderr) -> None:
        """
        Notifies all tasks that depend on this task of its completion.

        :return: Returns None.
        :rtype: None
        """
        if len(self.dep_queues) > 0:
            compiled_tuid = self.compiled_tuid

            for q, tuid, output_idxs in zip(
                self.dep_queues, self.dep_tuids, self.dep_output_idxs
            ):
                input_tuples = []

                if output_idxs is not None:
                    result_templ_arg_idx = output_idxs[AsyncType.RESULT]
                    if result_templ_arg_idx is not None:
                        template_idx, arg_idx = result_templ_arg_idx
                        input_tuples.append((result, template_idx, arg_idx))

                    stdout_templ_arg_idx = output_idxs[AsyncType.STDOUT]
                    if stdout_templ_arg_idx is not None:
                        template_idx, arg_idx = stdout_templ_arg_idx
                        input_tuples.append((stdout, template_idx, arg_idx))

                    stderr_templ_arg_idx = output_idxs[AsyncType.STDERR]
                    if stderr_templ_arg_idx is not None:
                        template_idx, arg_idx = stderr_templ_arg_idx
                        input_tuples.append((stderr, template_idx, arg_idx))

                dep_sat = DepSat(tuid, compiled_tuid, input_tuples)
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
        :type task_core: TaskCore
        :param batch: The batch to which this task belongs.
        :type batch: Batch
        :param reads: A list of ``Read`` objects created by calling ``Batch.read``.
        :type reads: Optional[list]
        :param writes: A list of ``Write`` objects created by calling ``Batch.write``.
        :type writes: Optional[list]
        :param compiled: A flag indicating if this task is compiled.
        :type compiled: bool

        :return: Returns None.
        :rtype: None
        """
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
        # TODO: get rid of ret_q_handler now that the Task object has direct access
        # to the Batch object
        self.ret_q_handler = batch.ret_q_handler
        self.num_managers_complete = 0
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

    def __del__(self) -> None:
        """
        Clean up this task, and guarantee that all background tasks are complete before
        the garbage collector free the task's memory.

        :return: Returns None.
        :rtype: None
        """
        try:
            # TODO: we should check if this task is in _background_tasks before calling fence()
            self._batch.fence()
            del self.ret_q_handler._async_dicts[self.core.tuid]
        except:
            pass

    def _depends_on(
        self, task, compiled_task, output_idxs: Optional[dict] = None
    ) -> None:
        """
        Sets a dependency for this task, which will be blocked until the completion of ``task``.

        :param task: The task that this one will be dependent on.
        :param compiled_task: The compiled task (sequence of tasks with possible dependencies) that
        these tasks and dependency belong to.

        :return: Returns None.
        :rtype: None
        """
        task.core.dep_tuids.append(self.core.tuid)
        task.core.dep_output_idxs.append(output_idxs)

        self.core.num_dep_tot += 1

        # TODO: this if-statement prevents arg-passing deps from being represented in
        # networkx dependency dag (this only really impacts visualization of the dag,
        # since DepSat messages are based on the dep_tuids list in the task_core)
        if compiled_task is not None:
            if compiled_task.dep_dag is None:
                compiled_task.dep_dag = nx.DiGraph()

            compiled_task.dep_dag.add_edge(task.core, self.core)

    def _handle_arg_passing_deps(self, list_of_arg_lists: Optional[list]) -> None:
        """
        Set up argument-passing dependencies for this task.

        :param list_of_arg_lists: A list of argument lists to be checked for AsyncDict args.
        :type list_of_arg_lists: list

        :return: Returns None.
        :rtype: None
        """
        arg_deps = {}

        for template_idx, args in enumerate(list_of_arg_lists):
            if args is None:
                continue

            # get the index for each argument that's an AsyncDict (representing another task's output)
            # and add a key-value pair to the arg_deps dict, key=AsyncDict, value=list of output indexes
            # and the index is stored in the list slot determined by its output type
            for idx, arg in enumerate(args):
                if isinstance(arg, AsyncDict):
                    if arg not in arg_deps:
                        arg_deps[arg] = [None, None, None]

                    arg_deps[arg][arg._async_type] = (template_idx, idx)

        # for each argument and its associated list of indexes, add a dependency from the (sub)task
        # associated with the AsyncDict to this task, and associate the list of output indexes with
        # this dependency
        for arg, output_idxs in arg_deps.items():
            self._depends_on(arg._subtask, arg._compiled_task, output_idxs)

    def _access(self, data_access: DataAccess) -> None:
        """
        Indicates that this task will access (read or write) a communication object on the specified channels.

        :param data_access: Specifies the access type, communication object, and channels used for one or more data accesses

        :return: Returns None.
        :rtype: None
        """
        access_type = data_access.access_type
        comm_object = data_access.comm_object
        channels = data_access.channels

        if comm_object not in self.accesses:
            self.accesses[comm_object] = {}

        accesses_this_dict = self.accesses[comm_object]

        for channel in channels:
            try:
                # if the current access type is READ and this new one is WRITE,
                # then update the access type to WRITE
                _, current_access_type = accesses_this_dict[channel]
                if (
                    current_access_type == AccessType.READ
                    and access_type == AccessType.WRITE
                ):
                    accesses_this_dict[channel] = (self, access_type)
            except:
                # no access type has been set yet, so we do that now
                accesses_this_dict[channel] = (self, access_type)

    def read(self, obj, *channels) -> None:
        """
        Indicates READ accesses of a specified set of channels on a communication object, and
        associates these accesses with this task. Associating READ accesses with a task allows
        the Batch service to infer dependencies between subtasks in a compiled task, but has
        no effect on individual (non-compiled) tasks.

        :param obj: The communication object being accessed.
        :param *channels: A tuple of channels on the communcation object that will be read from.

        :return: Returns None.
        :rtype: None
        """
        data_access = DataAccess(AccessType.READ, obj, channels)
        self._access(data_access)

    def write(self, obj, *channels) -> None:
        """
        Indicates WRITE accesses of a specified set of channels on a communication object, and
        associates these accesses with this task. Associating WRITE accesses with a task allows
        the Batch service to infer dependencies between subtasks in a compiled task, but has
        no effect on individual (non-compiled) tasks.

        :param obj: The communication object being accessed.
        :param *channels: A tuple of channels on the communcation object that will be written to.

        :return: Returns None.
        :rtype: None
        """
        data_access = DataAccess(AccessType.WRITE, obj, channels)
        self._access(data_access)

    def start(self) -> None:
        """
        Start this task by sending its work chunks to the managers. Currently, a task can only
        be started once and cannot be restarted after it completes. If a task is a subtask of
        a compiled task, then ``start`` must be called for the compiled task and not the subtask.

        :return: Returns None.
        :rtype: None
        """
        if self._compiled_task is None:
            self._batch.compile([self])

        compiled_task = self._compiled_task

        # this is a program with multiple work chunks, so round-robin them
        # across managers
        manager_qs = compiled_task.ret_q_handler._manager_qs
        num_managers = len(manager_qs)
        idx = compiled_task._manager_offset

        for work in compiled_task.work_chunks:
            manager_q = manager_qs[idx]
            manager_q.put(work)
            idx = (idx + 1) % num_managers

        # create AsyncDicts to hold the returned output from the task
        async_dicts = compiled_task.ret_q_handler._async_dicts
        async_dicts[compiled_task.core.compiled_tuid] = AsyncWrapper(
            AsyncDict(
                compiled_task._batch, AsyncType.RESULT, compiled_task=compiled_task
            ),
            AsyncDict(
                compiled_task._batch, AsyncType.STDOUT, compiled_task=compiled_task
            ),
            AsyncDict(
                compiled_task._batch, AsyncType.STDERR, compiled_task=compiled_task
            ),
        )

        compiled_task._started = True

    # TODO: add other options for checking completions, and take inspiration from Pool
    def wait(self, timeout: float = default_timeout) -> None:
        """
        Wait for this Task to complete. This can only be called after ``start`` has been called.
        This function does not return the task's result; instead, use ``get`` for that purpose.

        :param timeout: The timeout for waiting. Defaults to 1e9.

        :raises TimeoutError: If the specified timeout is exceeded.

        :return: Returns None.
        :rtype: None
        """
        start = time.time()
        compiled_task = self._compiled_task
        num_work_chunks = len(compiled_task.work_chunks)

        while (
            compiled_task.num_managers_complete < num_work_chunks
            and compiled_task.exception is None
        ):
            time_so_far = time.time() - start
            try:
                compiled_task.ret_q_handler._handle_next_ret(timeout - time_so_far)
            except Exception as e:
                time_so_far = time.time() - start
                if time_so_far >= timeout:
                    raise TimeoutError("timed out while waiting for result") from e
                else:
                    pass

        if compiled_task.exception is not None:
            message = f"compiled task with {compiled_task.core._get_id_str()} failed with the following traceback\n{compiled_task.traceback}"
            e = type(compiled_task.exception)
            raise e(message)

        compiled_task._ready = True

    def run(self, timeout: float = default_timeout) -> Any:
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
        self.start()
        return self.wait(timeout)

    def get(self):
        """
        Performs a batch fence to guarantee the completion of all tasks batched in the background,
        and then gets the result for this task.

        :return: Returns the result of this task.
        :rtype: Any
        """
        self._batch.fence()
        return self.result.get()

    @property
    def uid(self):
        """
        Provides the unique ID for this task.
        """
        return self.core.tuid

    @property
    def result(self):
        """
        Handle for the task's result. This should not be accessed until the task is started.
        ``result`` only applies to individual tasks and compiled tasks with a single subtask.
        The handle has type AsyncDict.
        """
        if self.num_subtasks > 1:
            raise RuntimeError(
                "result does not apply to compiled tasks with more than one subtask"
            )

        if self._result is None:
            self._result = AsyncDict(
                self._batch, AsyncType.RESULT, compiled_task=None, subtask=self
            )

        return self._result

    @property
    def stdout(self):
        """
        Handle for the task's stdout. This should not be accessed until the task is started.
        ``stdout`` only applies to individual tasks and compiled tasks with a single subtask.
        The handle has type AsyncDict.
        """
        if self.num_subtasks > 1:
            raise RuntimeError(
                "stdout does not apply to compiled tasks with more than one subtask"
            )

        if self._stdout is None:
            self._stdout = AsyncDict(
                self._batch, AsyncType.STDOUT, compiled_task=None, subtask=self
            )

        return self._stdout

    @property
    def stderr(self):
        """
        Handle for the task's stderr. This should not be accessed until the task is started.
        ``stderr`` only applies to individual tasks and compiled tasks with a single subtask.
        The handle has type AsyncDict.
        """
        if self.num_subtasks > 1:
            raise RuntimeError(
                "stderr does not apply to compiled tasks with more than one subtask"
            )

        if self._stderr is None:
            self._stderr = AsyncDict(
                self._batch, AsyncType.STDERR, compiled_task=None, subtask=self
            )

        return self._stderr

    def dump_dag(self, file_name: str) -> None:
        """
        Dump a PNG image of the dependency DAG associated with a compiled program.

        :param file_name: Name for the new PNG file.

        :return: Returns None.
        :rtype: None
        """
        pydot_graph = nx.drawing.nx_pydot.to_pydot(self.dep_dag)
        pydot_graph.write_png(f"{file_name}")


class AsyncDict:
    def __init__(
        self,
        batch: "Batch",
        async_type: AsyncType,
        compiled_task: Optional[Task] = None,
        subtask: Optional[Task] = None,
    ) -> None:
        """
        Initialize an AsyncDict, which is used to store and look up output from subtasks of a
        compiled task. A single AsyncDict stores a specific type of output (return value, stdout,
        or stderr) for all subtasks of a given compiled task, but copies of that AsyncDict are
        also used to fetch output for a given subtask (these are obtained by calling Task.result,
        Task.stdout, or Task.stderr).

        :param batch: The batch for the compiled task.
        :type batch: Batch
        :param async_type: The type of output handled by this AsyncDict, i.e., return values, stdout, or stderr.
        :type async_type: AsyncType
        :param compiled_task: The compiled task that generates the output.
        :type compiled_task: Optional[Task]
        :param subtask: A subtask of the compiled task.
        :type subtask: Optional[Task]

        :raises RuntimeError: If there is an issue while setting up the dependency graph

        :return: Returns None.
        :rtype: None
        """
        self._async_dicts = batch.ret_q_handler._async_dicts
        self._compiled_task = compiled_task
        self._subtask = subtask
        self._async_type = async_type
        self._dict = {}

    # avoid sending the whole AsyncDict to managers
    # TODO: might need to revisit this when we add support for deeply-nested arg-passing deps
    def __getstate__(self) -> AsyncType:
        """
        Since the user can pass task output to other tasks, we need to be careful
        not to send the whole AsyncDict to the managers. Consequently, we make this
        function trivial and just return the async type of the AsyncDict, although
        this may have to be updated in the future.

        :return: Returns the async type of the AsyncDict.
        :rtype: AsyncType
        """
        return self._async_type

    def __setstate__(self, async_type: AsyncType) -> None:
        """
        Since ``__getstate__`` just returns the async type, all we have to do here
        is set that attribute.

        :param async_type: The async type for this AsyncDict, i.e., return value, stdout, or stderr.
        :type async_type: AsyncType

        :raises RuntimeError: If there is an issue while setting up the dependency graph

        :return: Returns None.
        :rtype: None
        """
        self._async_type = async_type

    def get(self, timeout: float = default_timeout) -> Any:
        """
        Get the result, stdout, or stderr for a task.

        :param timeout: A timeout for waiting on the task to complete.

        :return: Returns output for a given task. The output can be the result returned
        from the task, or its stdout or stderr.
        :rtype: Any
        """
        if self._compiled_task is None:
            async_result, async_stdout, async_stderr = self._async_dicts[
                self._subtask.core.compiled_tuid
            ]
            # TODO: use match statement once we deprecate support for python 3.9
            """
            match self._async_type:
                case AsyncType.RESULT:
                    self._dict = async_result._dict
                    self._compiled_task = async_result._compiled_task
                case AsyncType.STDOUT:
                    self._dict = async_stdout
                    self._compiled_task = async_stdout._compiled_task
                case AsyncType.STDERR:
                    self._dict = async_stderr
                    self._compiled_task = async_stdout._compiled_task
                case _:
                    raise RuntimeError(f"invalid async type: {self._async_type=}")
            """
            if self._async_type == AsyncType.RESULT:
                self._dict = async_result._dict
                self._compiled_task = async_result._compiled_task
            elif self._async_type == AsyncType.STDOUT:
                self._dict = async_stdout._dict
                self._compiled_task = async_stdout._compiled_task
            elif self._async_type == AsyncType.STDERR:
                self._dict = async_stderr._dict
                self._compiled_task = async_stderr._compiled_task
            else:
                raise RuntimeError(f"invalid async type: {self._async_type=}")

        if self._subtask.core.tuid not in self._dict:
            # TODO: it would be better to wait for just the subtask, rather than
            # the whole compiled task
            self._compiled_task.wait(timeout)

        if self._subtask.exception is not None:
            message = f"task with {self._subtask.core._get_id_str()} failed with the following traceback\n{self._subtask.traceback}"
            e = type(self._subtask.exception)
            raise e(message)

        try:
            val = self._dict[self._subtask.core.tuid]
        except KeyError:
            if self._async_type != AsyncType.RESULT:
                # we don't add entries to _dict for empty output strings in Manager._handle_results,
                # so in that case we can just manually set val here
                val = ""
            else:
                raise RuntimeError(
                    f"no return value found for task: {self._subtask.core._get_id_str()}"
                )

        if isinstance(val, tuple):
            # this is a (result, tb, raised) tuple, so check if there was an exception
            result, tb, raised = val
            if raised:
                message = f"task with {self._subtask.core._get_id_str()} failed with the following traceback:\n{tb}"
                e = type(result)
                raise e(message)
            else:
                return result
        else:
            return val


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
        Creates a new function task. Arguments for the function that are of type ``AsyncDict``
        will create a dependency for this task on the output specified by the ``AsyncDict``.
        Further, the output specified by the ``AsyncDict`` will be passed in place of the
        ``AsyncDict`` when the function executes.

        :param batch: The batch in which this function task will execute.
        :param func: The function to associate with the object.
        :param *args: The arguments for the function.
        :param reads: A list of ``Read`` objects created by calling ``Batch.read``.
        :type reads: Optional[list]
        :param writes: A list of ``Write`` objects created by calling ``Batch.write``.
        :type writes: Optional[list]
        :param name: A human-readable name for the task.
        :type name: Optional[str]

        :return: Returns None.
        :rtype: None
        """
        super().__init__(
            FunctionCore(batch.client_id, name, timeout, target, args, kwargs),
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

        :raises RuntimeError: If there is an issue while setting up the dependency graph

        :return: Returns None.
        :rtype: None
        """
        super().__init__(client_id, name, timeout)

        self.num_procs = None
        self.process_templates = process_templates
        self.hostname_list = None
        self.is_parallel = True

    def run(self) -> None:
        """
        Runs the job associated with a job task.

        :raises RuntimeError: If path to the target is invalid.

        :return: Returns None.
        :rtype: None
        """
        if self.is_parallel:
            pmi = PMIBackend.CRAY
        else:
            pmi = None

        grp = ProcessGroup(restart=False, pmi=pmi, walltime=self.timeout)
        proc_count = 0
        template_idx = 0

        for hostname in self.hostname_list:
            nprocs_this_template, template = self.process_templates[template_idx]
            template.policy = Policy(
                placement=Policy.Placement.HOST_NAME, host_name=hostname
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

        for puid, exit_code in grp.inactive_puids:
            puids.append(puid)
            exit_codes.append(exit_code)
            proc_resources.append(Process(None, ident=puid))

        for proc_idx in range(self.num_procs):
            conn_stdout = proc_resources[proc_idx].stdout_conn
            conn_stderr = proc_resources[proc_idx].stderr_conn

            stdout = ""
            stderr = ""

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
    ) -> None:
        """
        Creates a new job task. Arguments for a process passed using ``ProcessTemplate.args``
        that are of type ``AsyncDict`` will create a dependency for this task on the output
        specified by the ``AsyncDict``. Further, the output specified by the ``AsyncDict``
        will be passed in place of the ``AsyncDict`` when the job executes.

        :param batch: The batch in which this function task will execute.
        :type batch: Batch
        :param process_templates: List of pairs of the form (nprocs, process_template), where nprocs is the number
        of processes to create using the specified template.
        :type process_templates: list
        :param reads: A list of ``Read`` objects created by calling ``Batch.read``.
        :type reads: Optional[list]
        :param writes: A list of ``Write`` objects created by calling ``Batch.write``.
        :type writes: Optional[list]
        :param name: A human-readable name for the task.
        :type name: Optional[str]

        :return: Returns None.
        :rtype: None
        """
        if len(process_templates) == 0:
            raise RuntimeError("need at least one process template")

        super().__init__(
            JobCore(batch.client_id, name, timeout, process_templates),
            batch,
            reads=reads,
            writes=writes,
        )

        total_procs = 0
        list_of_arg_lists = []

        # TODO: handle kwargs for arg-passing deps (probably changes elsewhere too)
        for nprocs_this_template, template in process_templates:
            total_procs += nprocs_this_template
            list_of_arg_lists.append(template.args)

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


def _do_task_impl(task_core: TaskCore) -> ResultWrapper:
    """
    Start a specified task in this manager's pool.

    :param task: The task to start.
    :type task: Task

    :return: Returns the task after it completes.
    :rtype: Task
    """
    try:
        result, stdout, stderr = task_core.run()
        rw = ResultWrapper(
            task_core.compiled_tuid,
            task_core.tuid,
            result,
            None,
            stdout.removesuffix("\n"),
            stderr.removesuffix("\n"),
            False,
        )
    except Exception as e:
        result = e
        # TODO: can we do better than this for stdout/stderr, i.e., get partial output?
        stdout = stderr = None

        rw = ResultWrapper(
            task_core.compiled_tuid,
            task_core.tuid,
            result,
            _get_traceback(),
            stdout,
            stderr,
            True,
        )

    task_core._notify_dep_tasks(result, stdout, stderr)

    return rw


def _get_hostname_for_job(
    task_core: TaskCore,
    work_q: Queue,
    job_done_q: Queue,
) -> Optional[ResultWrapper]:
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

    :return: Returns a ``ResultWrapper`` for the task if the job is launched, otherwise None.
    :rtype: Optional[ResultWrapper]
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
        rw = _do_task_impl(task_core)

        for _ in range(task_core.num_procs - 1):
            # put 0 onto all other "job done" queues just to indicate that the job
            # has completed and we can exit this function
            job_done_q.put(0)

        return rw
    else:
        return None


def _do_task(task_and_args) -> Task:
    """
    Wrapper around the user's task that is called by the workers.
    """
    task_core, args = task_and_args
    if isinstance(task_core, JobCore):
        rw = _get_hostname_for_job(task_core, *args)
    else:
        rw = _do_task_impl(task_core)

    return rw


class Manager:
    def __init__(
        self,
        idx: int,
        num_workers: int,
        work_q: Queue,
        ret_q: Queue,
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
        self.cached_queues = []
        self.primary_client_id = 0
        self.client_ctr = 1
        self.work_backlog = {}
        self.unexpected_dep_sat = {}
        self.job_hostname_lists = {}
        self.active_clients = {0}
        self._map_args_list = []
        self._compiled_task_list = []
        self._queued_task_count = 0
        self._async_queue = None
        self.join_called = False
        self.dbg_start_time = None
        self.dbg_update_start = False
        self.dispatch = None

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

        my_alloc = System()
        node = Node(my_alloc.nodes[0])

        policy_list = []

        num_gpus = node.num_gpus
        for _ in range(self.num_workers):
            if num_gpus > 0:
                # TODO: It would be better to round-robin device indexes on each node, but since
                # we're not specifying hosts for the workers, we don't know which nodes will be
                # used or how many workers will be on a given node.
                device_idx = random.randint(0, num_gpus - 1)
            else:
                device_idx = []
            policy_list.append(Policy(gpu_affinity=[device_idx]))

        self.pool = Pool(policy=policy_list, processes_per_policy=1)
        self._async_queue = LocalQueue()

        self.dbg_start_time = time.time()

        self.log = _setup_logging("manager")
        self.log.debug("creating pool for manager")

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
            task_core.cached_queue = Queue()

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
                    (self.work_q, job_done_queue),
                )
            )

        self.job_hostname_lists[task_core.tuid] = (
            task_core.num_procs,
            job_done_queue,
            [],
        )

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
            self._map_args_list.append((task_core, ()))

        self._queued_task_count += 1

    def _handle_manager_exception(
        self,
        e: Exception,
        err_msg: str,
        ret_q: Optional[Queue] = None,
        tuid: Optional[str] = None,
    ) -> None:
        """
        Return a general "manager exception" unrelated to any task.

        :param e: The exception to be returned to the primary client.
        :type e: Exception
        :param err_msg: The error message to be returned to the primary client.
        :type err_msg: str

        :return: Returns None.
        :rtype: None
        """
        self.log.debug(f"{err_msg}: {e}")
        me = ManagerException(tuid, e, _get_traceback(), err_msg)
        # if this exception isn't associated with any client/ret_q, just send it to the primary client
        if ret_q is None:
            ret_q = self.ret_q[self.primary_client_id]

        ret_q.put(me)

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

        compiled_tuid = work._compiled_result_wrapper.tuid
        self.work_backlog[compiled_tuid] = work

        for _, task_core in work._ready_task_cores.items():
            self._add_to_map_args_list(task_core)

        self._compiled_task_list.append((work._client_id, compiled_tuid))

        try:
            # if there aren't any unexpected DepSat messages waiting to be processed,
            # this will throw an exception
            dep_sat_list = self.unexpected_dep_sat[compiled_tuid]

            for dep_sat in dep_sat_list:
                self._handle_dep_sat(dep_sat)

            del self.unexpected_dep_sat[compiled_tuid]
        except:
            pass

    def _update_task_args(self, task_core: TaskCore, input_tuples: list) -> None:
        """
        Update task args using dep_sat.input_tuples of the form (new arg, template idx, argument idx).
        """
        if len(input_tuples) > 0:
            if isinstance(task_core, FunctionCore):
                args_list = list(task_core.args)

                # update the current arguments with the new ones
                for new_arg, _, arg_idx in input_tuples:
                    args_list[arg_idx] = new_arg

                task_core.args = tuple(args_list)
            else:
                # this is a JobCore, so we need to handle all process templates
                template_idx_to_new_args = {}

                # create a dict that maps a template index to a list of (new arg, arg idx) pairs
                for argval_templidx_argidx in input_tuples:
                    new_arg, template_idx, arg_idx = argval_templidx_argidx

                    if template_idx not in template_idx_to_new_args:
                        template_idx_to_new_args[template_idx] = []

                    template_idx_to_new_args[template_idx].append((new_arg, arg_idx))

                # for each template, update the current arguments with the new ones
                for template_idx, nprocs_and_template in enumerate(
                    task_core.process_templates
                ):
                    _, process_template = nprocs_and_template
                    new_args_list = template_idx_to_new_args[template_idx]
                    args_list = list(process_template.args)

                    for new_arg, arg_idx in new_args_list:
                        args_list[arg_idx] = new_arg

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
        tuid = dep_sat.tuid
        compiled_tuid = dep_sat.compiled_tuid

        try:
            # if the try succeeds, we have a compiled task that we are already working on
            work = self.work_backlog[compiled_tuid]

            # update the number of satisfied dependencies for this individual task
            # (which is part of a larger compiled task)
            task_core = work._blocked_task_cores[tuid]
            task_core.num_dep_sat += 1

            self._update_task_args(task_core, dep_sat.input_tuples)

            self.log.debug(
                f"received update for task with {task_core._get_id_str()} about a satisfied dependency: satisfied={task_core.num_dep_sat}, total={task_core.num_dep_tot}"
            )
        except:
            # this manager received a "dependency satisfied" message for a compiled task
            # before receiving the task, so add dep_sat to a list of unexpected messages
            self.log.debug(
                f"received unexpected update for task with {compiled_tuid=} and {tuid=} about a satisfied dependency"
            )

            try:
                dep_sat_list = self.unexpected_dep_sat[compiled_tuid]
                dep_sat_list.append(dep_sat)
            except:
                self.unexpected_dep_sat[compiled_tuid] = [dep_sat]
            return

        # if all dependencies have been satisfied for this task, launch it and
        # decrement the number of unstarted sub-tasks for this compiled task
        if task_core.num_dep_sat == task_core.num_dep_tot:
            self._add_to_map_args_list(task_core)

            work._ready_task_cores[tuid] = task_core
            del work._blocked_task_cores[tuid]

            self.log.debug(
                f"number of remaining tasks to be started={len(work._blocked_task_cores)}"
            )

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
        self.log.debug(
            f"received hostname={host_info.hostname} for task with {host_info.tuid}"
        )

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
            me = ManagerException(
                None, e, _get_traceback(), f"client {client_id} is already registered"
            )
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
            self._handle_manager_exception(
                e, "manager got exception when handling join-called message"
            )

    def _add_to_async_queue(self, result_wrapper: Optional[ResultWrapper]):
        self._async_queue.put(result_wrapper)

    def _launch_tasks(self) -> None:
        """
        Launch all queued tasks using map_async.

        :return: Returns None.
        :rtype: None
        """
        if len(self._map_args_list) == 0:
            return

        num_tasks = len(self._map_args_list)
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

    def _handle_results(self, result_wrappers: list[ResultWrapper]) -> None:
        """
        Process async results returned from map_async to check for task completions and
        handle any required cleanup.

        :param result_wrapper: The result to be processed.
        :type result_wrapper: ResultWrapper

        :return: Returns None.
        :rtype: None
        """
        for rw in result_wrappers:
            # if the result wrapper is None, this result is for a job sub-task that returned
            # None (only the primary sub-task that launched the job returns a real result)
            if rw is None:
                continue

            self.log.debug(f"individual task complete: result wrapper={rw}")
            compiled_tuid, tuid, result, tb, stdout, stderr, raised = rw

            work = self.work_backlog[compiled_tuid]
            task_core = work._ready_task_cores[tuid]
            del work._ready_task_cores[tuid]

            crw = work._compiled_result_wrapper
            crw.result_dict[tuid] = (result, tb, raised)
            if stdout != "":
                crw.stdout_dict[tuid] = stdout
            if stderr != "":
                crw.stderr_dict[tuid] = stderr

            if len(work._ready_task_cores) == 0 and len(work._blocked_task_cores) == 0:
                self.log.debug(f"compiled task complete: {compiled_tuid=}, {tuid=}")

                # notify client that the compiled task is complete
                self.ret_q[work._client_id].put(crw)
                del self.work_backlog[compiled_tuid]

            # recycle the queue used for this task if there is one
            if task_core.cached_queue is not None:
                self.cached_queues.append(task_core.cached_queue)

        if not self.disable_telem:
            num_tasks = len(result_wrappers)
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
            self.log.debug(
                f"+     --> {len(hostname_list)} out of {num_procs} hosts reserved: {hostname_list}"
            )

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
                        self.log.debug(
                            f"| ready tasks for compiled_tuid={compiled_tuid}"
                        )
                        for _, task_core in work._ready_task_cores.items():
                            self._log_task_debug_info(task_core)

                    if len(work._blocked_task_cores) > 0:
                        self.log.debug(divider)
                        self.log.debug(
                            f"| blocked tasks for compiled_tuid={compiled_tuid}"
                        )
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
                self._handle_manager_exception(
                    e, "failed to get item from async results queue"
                )

            if (
                self.join_called
                and len(self.active_clients) == 0
                and len(self.work_backlog) == 0
            ):
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


class ReturnQueueHandler:
    def __init__(self, manager_queues: list[Queue]) -> None:
        """
        Initializes a return queue for a batch (shared by all managers).

        :return: Returns None.
        :rtype: None
        """
        self._manager_qs = manager_queues
        self.ret_q = Queue(maxsize=return_queue_maxsize)
        self._async_dicts = {}
        self.manager_exception = None
        self.manager_traceback = None

    def _handle_next_ret(self, timeout: Optional[float] = None) -> None:
        """
        Handles the next message received on the return queue, either a return value,
        an exception, or a special "end of batch" value used to indicate that the
        manager has completed all its work.

        :param timeout: A timeout value for the get operation on the return queue. Defaults to None.
        :type timeout: float

        :return: Returns None.
        :rtype: None
        """
        try:
            ret = self.ret_q.get(timeout=timeout)
        except Exception as e:
            raise RuntimeError("failed to get update from managers")

        if not isinstance(ret, ManagerException):
            async_result, async_stdout, async_stderr = self._async_dicts[ret.tuid]
            task = async_result._compiled_task

            async_result._dict.update(ret.result_dict)
            async_stdout._dict.update(ret.stdout_dict)
            async_stderr._dict.update(ret.stderr_dict)

            for msg in ret.stdout_dict.values():
                if msg is not None:
                    print(msg)

            for msg in ret.stderr_dict.values():
                if msg is not None:
                    print(msg, file=sys.stderr)

            task.num_managers_complete += 1
        else:
            if ret.tuid is not None:
                # this exception is associated with trying to execute a work chunk,
                # so mark the compiled task as failed
                async_result, async_stdout, async_stderr = self._async_dicts[ret.tuid]
                task = async_result._compiled_task
                task.exception = ret.exception
                task.traceback = ret.traceback
            else:
                # we can assume here that this is an exception unrelated to a specific task,
                # so we set the general manager_exception
                self.manager_exception = ret.exception
                self.manager_traceback = ret.traceback


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


def _init_task_deps(read_or_write: dict, access_type: AccessType, task: Task) -> None:
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

    for key_or_file in read_or_write[keys_or_files]:
        real_key_or_file = _resolve_val(key_or_file)

        if access_type == AccessType.READ:
            task.read(comm_obj, real_key_or_file)
        else:
            task.write(comm_obj, real_key_or_file)


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

        import_and_calltime_intersection = self._ptd_arg_set.union(
            self._ptd_kwarg_set
        ).intersection(ptd_import_arg_set)

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
        task is created using values from the PTD and the task creation functions (``function``,
        ``process``, and ``job``); (4) task dependencies are initialized using values from
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
                        raise RuntimeError(
                            f"function target must be a callable: {target=}"
                        )
                else:
                    if not isinstance(target, str) and not isinstance(target, Path):
                        raise RuntimeError(
                            f"process or job target must be a str or Path: {target=}"
                        )
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
                pt = ProcessTemplate(
                    target=target, args=args, kwargs=kwargs, cwd=cwd, env=env
                )
                pt_list.append((nprocs, pt))

        # create Task from task_type and pt_list
        if task_type == TaskType.FUNC:
            task = self._batch.function(
                target,
                *args,
                name=task_name,
                timeout=_get_timeout_val(timeout_dict),
                **kwargs,
            )
        elif task_type == TaskType.PROC:
            _, pt = pt_list[0]
            task = self._batch.process(
                process_template=pt,
                name=task_name,
                timeout=_get_timeout_val(timeout_dict),
            )
        elif task_type == TaskType.JOB:
            task = self._batch.job(
                process_templates=pt_list,
                name=task_name,
                timeout=_get_timeout_val(timeout_dict),
            )
        else:
            raise RuntimeError(
                f"invalid task type: {task_type}\n==> valid types: function, process, job"
            )

        # set up reads and writes
        for read in self._reads:
            _init_task_deps(read, AccessType.READ, task)

        for write in self._writes:
            _init_task_deps(write, AccessType.WRITE, task)

        # queue up tasks in the background so they can be run in a batch later
        if not self._batch._disable_background_batching:
            self._batch._background_tasks.append(task)

        return task


class BatchDDict(DDict):
    def __init__(self, batch: "Batch", *args, **kwargs) -> None:
        """
        Initialize a batch ddict object. This object adds a little logic on top of a normal
        ddict to guarantee that previous (according to client program order) updates to the
        ddict are complete before the client accesses the ddict. This is only necessary when
        background batching is enabled.

        :param batch: The batch whose tasks will update the ddict.
        :type batch: Batch

        :return: Returns None.
        :rtype: None
        """
        self._batch = batch
        super().__init__(*args, **kwargs)

    def __getstate__(self) -> dict:
        """
        Performs a fence operation (if called by client code), and returns the ddict's state.

        :return: Returns the ddict's state.
        :rtype: tuples
        """
        # call fence() here to avoid scenarios where, e.g., a ddict is passed to another process
        # after the user queues tasks that will update the ddict once they (lazily) execute, and
        # that process attempts to access keys that it expects to be there based on program order
        if self._batch is not None:
            self._batch.fence()
        return super().__getstate__()

    def __setstate__(self, state: tuple) -> None:
        """
        Sets the ddict's state, and sets ``_batch`` to None. The managers don't currently need the
        batch object, and performing fences in manager code is unnecessary.

        :param state: Used to set the ddict's state.
        :type state: tuple

        :return: Returns None.
        :rtype: None
        """
        self._batch = None
        super().__setstate__(state)

    def _send(self, *args, **kwargs) -> None:
        """
        Wraps the ddict's ``_send`` function and performs a fence before calling it. This guarantees
        that interactions with the ddict will be consistent with the user's expectations based on
        client program order.

        :param msglist: First argument to ``ddict._send``.
        :type msglist: list
        :param connection: Second argument to ``ddict._send``.
        :type connection: FLInterface

        :return: Returns None.
        :rtype: None
        """
        if self._batch is not None:
            self._batch.fence()
        super()._send(*args, **kwargs)


# TODO: need to handle more dunder methods
class BatchFile:
    def __init__(self, batch, *args, **kwargs) -> None:
        """
        Initialize a batch file object. This object adds a little logic on top of a normal
        file to guarantee that previous (according to client program order) updates to the
        file are complete before the client accesses the file. This is only necessary when
        background batching is enabled.

        :param batch: The batch whose tasks will update the ddict.
        :type batch: Batch

        :return: Returns None.
        :rtype: None
        """
        self._batch = batch
        self._file = open(*args, **kwargs)

    def __getstate__(self) -> Any:
        """
        Performs a fence before returning the the file's state.

        :return: Returns the file's state.
        :rtype: Any
        """
        # call fence() here to avoid scenarios where, e.g., a file is passed to another process
        # after the user queues tasks that will update the ddict once they (lazily) execute, and
        # that process attempts to access data that it expects to be there based on program order
        if self._batch is not None:
            self._batch.fence()
        return self._file.__getstate__()

    def __setstate__(self, state: Any) -> None:
        """
        Sets the file's state, and sets ``_batch`` to None. The managers don't currently need the
        batch object, and performing fences in manager code is unnecessary.

        :param state: Used to set the file's state.
        :type state: Any

        :return: Returns None.
        :rtype: None
        """
        self._batch = None
        self._file.__setstate__(state)

    def __getattr__(self, name: Any) -> Any:
        """
        Calls the file's ``__getattr__`` method.

        :param name: Argument to be passed to the file's ``__getattr__`` method.
        :type name: Any

        :return: Returns the value obtained from calling the file's ``__getattr__`` method.
        :rtype: Any
        """
        return self._file.__getattr__(name)

    def __enter__(self) -> "BatchFile":
        """
        Simply returns this object.

        :return: Returns this object.
        :rtype: BatchFile
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        Closes the file.

        :param exc_type: Unused.
        :param exc_val: Unused.
        :param exc_tb: Unused.

        :return: Returns None.
        :rtype: None
        """
        self._file.close()

    def read(self, *args, **kwargs) -> Any:
        """
        Performs a batch fence before reading from the file.

        :return: Returns the data read from the file.
        :rtype: Any
        """
        if self._batch is not None:
            self._batch.fence()
        return self._file.read(*args, **kwargs)

    def read1(self, *args, **kwargs) -> Any:
        """
        Performs a batch fence before reading from the file.

        :return: Returns the data read from the file.
        :rtype: Any
        """
        if self._batch is not None:
            self._batch.fence()
        return self._file.read1(*args, **kwargs)

    def readline(self, *args, **kwargs) -> Any:
        """
        Performs a batch fence before reading from the file.

        :return: Returns the data read from the file.
        :rtype: Any
        """
        if self._batch is not None:
            self._batch.fence()
        return self._file.readline(*args, **kwargs)

    def readlines(self, *args, **kwargs) -> Any:
        """
        Performs a batch fence before reading from the file.

        :return: Returns the data read from the file.
        :rtype: Any
        """
        if self._batch is not None:
            self._batch.fence()
        return self._file.readlines(*args, **kwargs)

    def readall(self, *args, **kwargs) -> Any:
        """
        Performs a batch fence before reading from the file.

        :return: Returns the data read from the file.
        :rtype: Any
        """
        if self._batch is not None:
            self._batch.fence()
        return self._file.readall(*args, **kwargs)

    def readinto(self, *args, **kwargs) -> Any:
        """
        Performs a batch fence before reading from the file.

        :return: Returns the data read from the file.
        :rtype: Any
        """
        if self._batch is not None:
            self._batch.fence()
        return self._file.readinto(*args, **kwargs)

    def readinto1(self, *args, **kwargs) -> Any:
        """
        Performs a batch fence before reading from the file.

        :return: Returns the data read from the file.
        :rtype: Any
        """
        if self._batch is not None:
            self._batch.fence()
        return self._file.readinto1(*args, **kwargs)

    def write(self, *args, **kwargs) -> None:
        """
        Performs a batch fence before writing to the file.

        :return: Returns None.
        :rtype: None
        """
        if self._batch is not None:
            self._batch.fence()
        self._file.write(*args, **kwargs)

    def writelines(self, *args, **kwargs) -> None:
        """
        Performs a batch fence before writing to the file.

        :return: Returns None.
        :rtype: None
        """
        if self._batch is not None:
            self._batch.fence()
        self._file.writelines(*args, **kwargs)


# TODO: need to be able to pickle and unpickle Batch objects and send them
# to managers and workers to enable recursive function calls
class Batch:
    def __init__(
        self,
        num_workers: int = 0,
        disable_telem: bool = False,
        disable_background_batching: bool = False,
    ) -> None:
        """
        Initializes a batch.

        :param num_workers: Number of workers for this batch. Defaults to multiprocessing.cpu_count().
        :type num_workers: int
        :param disable_telem: Indicates if telemetry should be disabled for this Batch instance. Defaults to False.
        :type disable_telem: bool

        :return: Returns None.
        :rtype: None
        """
        if (
            not isinstance(num_workers, int)
            or num_workers <= 0
            or num_workers > cpu_count()
        ):
            num_workers = cpu_count()

        self.num_workers = num_workers
        self.num_managers = min(
            num_workers,
            _next_pow_of_2(max(1, num_workers // default_workers_per_manager)),
        )
        self._background_tasks = []
        self._disable_background_batching = disable_background_batching

        # this is the primary client and responsible for cleanup
        self.client_id = 0
        self.manager_qs = []
        self.grp = None

        # these flags are only used by the primary client, which is responsible for bringup and teardown
        self.closed = False
        self.terminated = False
        self.joined = False

        self.disable_telem = disable_telem

        self._set_unique_attrs(self.manager_qs)
        # NOTE: self.log is set in _set_unique_addrs
        self.log.debug(
            f"creating new Batch object with {num_workers} workers and {self.num_managers} managers"
        )

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
        self._set_unique_attrs(self.manager_qs)

        # register this client with the managers
        self._register()

    def _setup_managers(self) -> None:
        """
        Creates the managers for a batch, along with the process group for them.
        This should only be called by the primary client.

        :return: Returns None.
        :rtype: None
        """
        # create the managers

        self.log.debug(f"setting up managers")

        self.grp = ProcessGroup(restart=False)

        num_workers_per_manager_floor = self.num_workers // self.num_managers
        extra_worker_cutoff = self.num_workers % self.num_managers

        managers = []

        for idx in range(self.num_managers):
            num_workers_per_manager = num_workers_per_manager_floor
            if idx < extra_worker_cutoff:
                num_workers_per_manager += 1

            manager = Manager(
                idx,
                num_workers_per_manager,
                Queue(maxsize=manager_work_queue_maxsize),
                self.ret_q_handler.ret_q,
                disable_telem=self.disable_telem,
            )
            managers.append(manager)
            self.manager_qs.append(manager.work_q)

            self.grp.add_process(
                nproc=1,
                template=ProcessTemplate(
                    target=_bootstrap_manager, args=(manager.work_q,)
                ),
            )

        self.log.debug(f"initializing and starting the process group of managers")
        self.grp.init()
        self.grp.start()

        # send the manager objects to start the bootstrap
        for manager, manager_q in zip(managers, self.manager_qs):
            manager_q.put(manager)

    def _set_unique_attrs(self, manager_qs: list[Queue]) -> None:
        """
        Set manager attributes that are unique to this client (most are generic and apply to all clients).
        """
        self.closed = False
        # each manager needs its own return queue
        self.ret_q_handler = ReturnQueueHandler(manager_qs)
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
        register_client = RegisterClient(self.ret_q_handler.ret_q, None)
        primary_manager = 0
        self.manager_qs[primary_manager].put(register_client)
        self.client_id = self.ret_q_handler.ret_q.get()
        self.log.debug(f"received new client_id={self.client_id}")

        # use the client_id to register with the remaining managers
        register_client = RegisterClient(self.ret_q_handler.ret_q, self.client_id)
        for manager_q in self.manager_qs[1:]:
            manager_q.put(register_client)

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

    def _add_to_compiled_task(self, subtask: Task, compiled_task: Task) -> None:
        """
        Add a new task to a program.

        :param task: The task to be added.
        :type task: Task
        :param compiled_task: The compiled task to be extended.
        :type compiled_task: Task

        :return: Returns None.
        :rtype: None
        """
        if subtask._started:
            raise ("cannot compile a task that has already been started")

        # append task's access list to program's access list
        for ddict, accesses_this_ddict_for_subtask in subtask.accesses.items():
            if ddict not in compiled_task.accesses:
                compiled_task.accesses[ddict] = {}

            accesses_this_ddict_for_prog = compiled_task.accesses[ddict]

            for key, access_for_subtask in accesses_this_ddict_for_subtask.items():
                if key not in accesses_this_ddict_for_prog:
                    accesses_this_ddict_for_prog[key] = []

                accesses_this_ddict_for_prog[key].append(access_for_subtask)

        # NOTE: this assumes all subtasks of a compiled task are unique
        compiled_task.dep_dag.add_node(subtask.core)

        compiled_task.num_subtasks += 1

        subtask.core.compiled_tuid = compiled_task.core.tuid
        subtask._compiled_task = compiled_task

    def read(self, obj, *channels) -> DataAccess:
        """
        Indicates READ accesses of a specified set of channels on a communication object. These
        accesses are not yet associated with a given task.

        :param obj: The communication object being accessed.
        :param *channels: A tuple of channels on the communcation object that will be read from.

        :return: Returns an descriptor for the data access that can be passed to (in a list) when creating a new task.
        :rtype: DataAccess
        """
        return DataAccess(AccessType.READ, obj, *channels)

    def write(self, obj, *channels) -> DataAccess:
        """
        Indicates WRITE accesses of a specified set of channels on a communication object. These
        accesses are not yet associated with a given task.

        :param obj: The communication object being accessed.
        :param *channels: A tuple of channels on the communcation object that will be writtent o.

        :return: Returns an descriptor for the data access that can be passed to (in a list) when creating a new task.
        :rtype: DataAccess
        """
        return DataAccess(AccessType.WRITE, obj, *channels)

    def fence(self) -> None:
        """
        Compiles and runs all background tasks for this batch. Once a fence completes,
        the results of all background tasks will be ready and locally available.

        :return: Returns None.
        :rtype: None
        """
        if len(self._background_tasks) > 0:
            background_tasks = self._background_tasks
            # reset _background_tasks before running the compiled task to avoid scenarios
            # where run() recursively calls fence() (i.e., when pickling a ddict)
            self._background_tasks = []
            self.compile(tasks_to_compile=background_tasks).run()

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

        if self.ret_q_handler.manager_exception is not None:
            raise RuntimeError(
                "manager raised a general exception (not connected to a particular task)"
            ) from self.ret_q_handler.manager_exception

        # indicate that join has been called so the manager know to begin shutdown
        join_called = JoinCalled()
        for manager_q in self.manager_qs:
            manager_q.put(join_called)

        self.grp.join(timeout=timeout)
        self.grp.close()

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
        Creates a new function task. Arguments for the function that are of type ``AsyncDict``
        will create a dependency for this task on the output specified by the ``AsyncDict``.
        Further, the output specified by the ``AsyncDict`` will be passed in place of the
        ``AsyncDict`` when the job executes.

        :param func: The function to associate with the object.
        :param *args: The arguments for the function.
        :param reads: A list of ``Read`` objects created by calling ``Batch.read``.
        :type reads: Optional[list]
        :param writes: A list of ``Write`` objects created by calling ``Batch.write``.
        :type writes: Optional[list]
        :param name: A human-readable name for the task.
        :type name: Optional[str]

        :return: The new function task.
        :rtype: Function
        """
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
        Creates a new process task. Arguments for the process passed using ``ProcessTemplate.args``
        that are of type ``AsyncDict`` will create a dependency for this task on the output
        specified by the ``AsyncDict``. Further, the output specified by the ``AsyncDict``
        will be passed in place of the ``AsyncDict`` when the process executes.

        :param process_template: A Dragon ``ProcessTemplate`` to describe the process to be run.
        :type process_template: ProcessTemplate
        :param reads: A list of ``Read`` objects created by calling ``Batch.read``.
        :type reads: Optional[list]
        :param writes: A list of ``Write`` objects created by calling ``Batch.write``.
        :type writes: Optional[list]
        :param name: A human-readable name for the task.
        :type name: Optional[str]

        :return: The new process task.
        :rtype: Job
        """
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
    ) -> Job:
        """
        Creates a new job task. Arguments for a process passed using ``ProcessTemplate.args``
        that are of type ``AsyncDict`` will create a dependency for this task on the output
        specified by the ``AsyncDict``. Further, the output specified by the ``AsyncDict``
        will be passed in place of the ``AsyncDict`` when the job executes.

        :param process_templates: A list of pairs of the form (num_procs, process_template), where
        ``process_template`` is template for ``num_procs`` processes in the job. The process template
        is based on Dragon's ProcessTemplate.
        :type process_templates: list
        :param reads: A list of ``Read`` objects created by calling ``Batch.read``.
        :type reads: Optional[list]
        :param writes: A list of ``Write`` objects created by calling ``Batch.write``.
        :type writes: Optional[list]
        :param name: A human-readable name for the task.
        :type name: Optional[str]

        :return: The new job task.
        :rtype: Job
        """
        task = Job(
            self,
            process_templates=process_templates,
            reads=reads,
            writes=writes,
            name=name,
            timeout=timeout,
        )
        return task

    def compile(
        self,
        tasks_to_compile: list[Task],
        name: Optional[str] = None,
    ) -> Task:
        """
        Generate a single, compiled task from a list of tasks. After a list of tasks has been
        compiled, the individual subtasks of the compiled task can no longer be started or waited
        on separately--``start``, ``wait``, and ``run`` should all be called via the compiled task.

        :param tasks_to_compile: List of tasks to compile.
        :type tasks_to_compile: list

        :raises RuntimeError: If there is an issue while setting up the dependency graph

        :return: The compiled task.
        :rtype: Task
        """
        # check that tasks_to_compile is a non-empty list of tasks
        if not isinstance(tasks_to_compile, list):
            raise RuntimeError(
                f"tasks_to_compile must be a list of tasks: {tasks_to_compile=}"
            )
        else:
            num_tasks = len(tasks_to_compile)
            if num_tasks == 0:
                raise RuntimeError("cannot compile an empty list of tasks")
            elif not isinstance(tasks_to_compile[0], Task):
                raise RuntimeError(
                    f"tasks_to_compile must be a list of tasks: {tasks_to_compile[0]=}"
                )

        task_core = TaskCore(self.client_id, name, None)
        compiled_task = Task(task_core, self, compiled=True)

        # TODO: add a check to make sure a task isn't a subtask for two different progs
        for task in tasks_to_compile:
            self._add_to_compiled_task(task, compiled_task)

        # determine dependecies from accesses
        for ddict, accesses_this_ddict in compiled_task.accesses.items():
            for key, access_list in accesses_this_ddict.items():
                if len(access_list) > 1:
                    # process access list and add dependencies as necessary

                    tmp_read_list = []
                    prev_task, prev_access_type = access_list[0]

                    if prev_access_type == AccessType.READ:
                        write_before_read = None
                    else:
                        write_before_read = prev_task

                    for task, access_type in access_list[1:]:
                        if prev_access_type == AccessType.READ:
                            if access_type == AccessType.READ:
                                # prev=READ, curr=READ
                                tmp_read_list.append(task)
                                if write_before_read is not None:
                                    task._depends_on(write_before_read, compiled_task)
                            elif access_type == AccessType.WRITE:
                                # prev=READ, curr=WRITE
                                for prev_read in tmp_read_list:
                                    task._depends_on(prev_read, compiled_task)
                                tmp_read_list = []
                                write_before_read = task
                        else:
                            if prev_access_type != AccessType.WRITE:
                                raise RuntimeError("invalid access mode")

                            if access_type == AccessType.READ:
                                # prev=WRITE, curr=READ
                                tmp_read_list.append(task)
                                task._depends_on(prev_task, compiled_task)
                            else:
                                # prev=WRITE, curr=WRITE
                                task._depends_on(prev_task, compiled_task)

                        prev_task = task
                        prev_access_type = access_type

        # partition the dependency graph for the managers, subdiving partitions
        # until there is one partition per manager, or until it doesn't make sense
        # to keep partitioning

        self.log.debug("partitioning the dependency graph")

        if self.num_managers > 1 and len(tasks_to_compile) > 1:
            dep_graph = compiled_task.dep_dag.to_undirected()
            partitions = list(nx.community.kernighan_lin_bisection(dep_graph))

            while len(partitions) < self.num_managers and len(partitions[0]) > 1:
                try:
                    new_partitions = []
                    for part in partitions:
                        g = dep_graph.subgraph(part)
                        tmp_partitions = list(nx.community.kernighan_lin_bisection(g))
                        new_partitions.extend(tmp_partitions)

                    partitions = new_partitions
                except:
                    raise RuntimeError(
                        "failure occurred while trying to bisect dependency graph"
                    )
        else:
            task_core_set = {task.core for task in tasks_to_compile}
            partitions = [task_core_set]

        tuid_to_queue = {}

        # establish mapping between tasks and managers/queues

        self.log.debug("setting up the mapping between tasks and managers")

        compiled_task._manager_offset = random.randrange(self.num_managers)

        for base_idx, task_set in enumerate(partitions):
            idx = (base_idx + compiled_task._manager_offset) % self.num_managers
            manager_q = self.manager_qs[idx]

            for task_core in task_set:
                tuid_to_queue[task_core.tuid] = manager_q

        # now that we know the mapping between tasks and managers/queues, we can
        # update the list of dependent tuids to make it a list of dependent queues

        self.log.debug("setting up dependency queues")

        for task_set in partitions:
            for task_core in task_set:
                for dep_tuid in task_core.dep_tuids:
                    q = tuid_to_queue[dep_tuid]
                    task_core.dep_queues.append(q)

        # create a work item for each manager
        for task_set in partitions:
            work = Work(
                task_set,
                self.client_id,
                compiled_task.core.tuid,
            )
            compiled_task.work_chunks.append(work)

        return compiled_task

    def import_func(
        self, ptd_file: str, *real_import_args, **real_import_kwargs
    ) -> MakeTask:
        """
        Loads the PTD dict and creates a ``MakeTask`` object for the parameterized task group
        specified by the PTD file and import arguments (``real_import_args`` and ``real_import_kwargs``).
        The group of tasks is parameterized by the arguments passed to the ``MakeTask``object's
        ``__call__`` method, with a different task created for each unique collection of arguments.
        The name of this function comes from the fact that the ``__call__`` method of the``MakeTask``
        object is meant to "look and feel" like calling the task and getting a return value without
        blocking, i.e., writing a serial program that runs locally, even though the tasks are lazily
        executed in parallel by the remote batch workers.

        :param ptd_file: Specifies the parameterized task group.
        :type ptd_file: str
        :param x: Positional arguments to replace the identifiers listed under the "import_args"
        key in the PTD file.
        :param x: Keyword arguments to replace the key/value identifiers specified under the
        "import_args" key in the PTD file.

        :return: Returns a MakeTask object representing the parameterized group of tasks specified by
        the PTD file and import arguments.
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

    def ddict(self, *args, **kwargs) -> BatchDDict:
        """
        Creates a batch ddict.

        :return: Returns the batch ddict.
        :rtype: BatchDDict
        """
        return BatchDDict(self, *args, **kwargs)

    def open(self, *args, **kwargs) -> BatchFile:
        """
        Creates a batch file.

        :return: Returns the batch file.
        :rtype: BatchFile
        """
        # need a fence here, because it's possible that queued tasks are supposed to
        # create the file that we're about to open
        self.fence()
        return BatchFile(self, *args, **kwargs)
