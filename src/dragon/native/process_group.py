"""
This module broadly defines the ProcessGroup API. It is based on a
multiple client-single server model. The user API acts as the client,
and a separate Manager process acts as the server that contains all state
for a given ProcessGroup.

At any point after a ProcessGroup has been initialized via `ProcessGroup.init()`,
the object may be pickled and delivered to a new process in order for that
new process to query the ProcessGroup state.

Critical features of the ProcessGroup client API are exposed via the `restart` and
`ignore_error_on_exit` kwargs set during creation of a ProcessGroup object.
These tell the Manager to restart any processes (`restart == True`) in the event
of their exit. If `ignore_error_on_exit == True`, the Manager will restart these
processes no matter their exit status. If `ignore_error_on_exit == False`, the Manager
instead will log a backtrace and send an exception to the Client. This exception is
always raised in a Client background thread. If any call to the client API is made,
the exception is raised in the main user thread.

Experientially speaking, the combination of `(restart=True, igonore_error_on_exit=True)`
gives behavior similar the `multiprocessing.Pool()` while
`(restart=False, ignore_error_on_exit=False)` is more consistent with execution
of something like an MPI application, where a user would want to see an error raised
if any process were to experience a non-zero exit.

In order to communicate state of the processes, the Client API provides several query
functions exposing which processes have been started, which have exited, what any
process exit codes were, and what the current state of the ProcessGroup is.

Generally, the user will use the Client API to tell the Manager what processes to
start, to start them, query status, and how the processes should be run and
stopped. This API is exposed in the `ProcessGroup` class.

Though contained in this module, it is not intended for the user to make use of the
`Manager` class. Rather calls to the Client API will make changes to a given
`Manager` object as it appropriate to complete the API request.

A typical workflow using ProcessGroup may look like:

    .. highlight:: python
    .. code-block:: python

        from dragon.native.process_group import ProcessGroup
        from dragon.native.process import ProcessTemplate

        # define a function that will service as a worker function template
        def hello_world():
            from dragon.infrastructuture.parameters import this_process
            print(f'hello from process {this_process.my_puid}!')


        # create a ProcessGroup object
        pg = ProcessGroup()

        # Define a template
        template = dragon.native.process.ProcessTemplate(target=hello_world)

        # Tell ProcessGroup to ultimately create 4 instances of the hello world template
        pg.add_process(nproc=4, template=template)

        # Initialize the Manager. This should only be called once by a single client.
        pg.init()

        # Start the worker processes
        pg.start()

        # Join on the workers
        pg.join()  #  If your worker functions won't exit on their own, use pg.stop() to transmit
                   #  interrupt/termination signals

        # Close the Manager and all other resources used to facilitate management of this ProcessGroup object.
        # Though, this particular object can be re-used, pg.init() must be called first.
        pg.close()
"""

import threading
import queue
import logging
import traceback
import signal
import socket
import enum
import cloudpickle
import time
from dataclasses import dataclass, asdict, field
from typing import List, Tuple
from abc import ABC, abstractmethod
from functools import wraps
import os
from enum import Enum

from ..globalservices.process import (
    query as process_query,
    join as process_join,
    multi_join,
    get_multi_join_failure_puids,
    get_multi_join_success_puids,
    get_create_message_with_argdata,
    get_create_message,
)
from ..globalservices.group import (
    GroupError,
    create as group_create,
    cleanup_pmix_resources,
    kill as group_kill,
    create_add_to as group_create_add_to,
)

from ..channels import Channel
from ..dlogging import util as dlog
from ..dlogging.util import setup_BE_logging, DragonLoggingServices as dls
from ..dlogging.logger import DragonLoggingError
from ..utils import b64decode, b64encode
from ..infrastructure.connection import Pipe, Connection
from ..infrastructure.group_desc import GroupDescriptor
from ..infrastructure.process_desc import ProcessDescriptor
from ..infrastructure import util as dutil
from ..infrastructure.policy import Policy
from ..infrastructure.parameters import this_process
from ..infrastructure.facts import PMIBackend
from ..infrastructure import messages as dmsg
from ..native.machine import System
from ..rc import DragonError

import os
import sys
from ..native.machine import System, Node

from .queue import Queue
from .process import Process, ProcessTemplate, Popen


_DEFAULT_STOP_PATIENCE = 5.0  # seconds between successive signals to the group
_LOG = None


def get_logs(name):
    global _LOG
    log = _LOG.getChild(name)
    return log.debug, log.info


class DragonProcessGroupError(DragonLoggingError):
    pass


class DragonProcessGroupException(DragonProcessGroupError):
    pass


class DragonProcessGroupAlreadyInitialized(DragonProcessGroupError):
    pass


class DragonProcessGroupIdleError(DragonProcessGroupError):
    pass


class DragonProcessGroupStartError(DragonProcessGroupError):
    pass


class DragonProcessGroupRunningError(DragonProcessGroupError):
    pass


class DragonUserCodeError(DragonProcessGroupError):
    pass


class DragonProcessGroupJoinError(DragonProcessGroupError):
    pass


class DragonProcessGroupSignalError(DragonProcessGroupError):
    pass


@dataclass
class RankInfo:
    """Utility class for accessing process rank metadata from environment variables."""

    def __init__(self):
        self._my_local_rank = None
        self._my_rank = None
        self._my_node_rank = None
        self._my_local_world_size = None
        self._my_world_size = None
        self._master_addr = None
        self._master_port = None

    @property
    def master_addr(self):
        if self._master_addr is None:
            self._master_addr = os.environ["MASTER_ADDR"]
        return self._master_addr

    @property
    def master_port(self):
        if self._master_port is None:
            self._master_port = os.environ["MASTER_PORT"]
        return self._master_port

    @property
    def my_local_rank(self):
        if self._my_local_rank is None:
            self._my_local_rank = int(os.environ["DRAGON_PG_LOCAL_RANK"])
        return self._my_local_rank

    @property
    def my_rank(self):
        if self._my_rank is None:
            self._my_rank = int(os.environ["DRAGON_PG_RANK"])
        return self._my_rank

    @property
    def my_node_rank(self):
        if self._my_node_rank is None:
            self._my_node_rank = int(os.environ["DRAGON_PG_GROUP_RANK"])
        return self._my_node_rank

    @property
    def my_local_world_size(self):
        if self._my_local_world_size is None:
            self._my_local_world_size = int(os.environ["DRAGON_PG_LOCAL_WORLD_SIZE"])
        return self._my_local_world_size

    @property
    def my_world_size(self):
        if self._my_world_size is None:
            self._my_world_size = int(os.environ["DRAGON_PG_WORLD_SIZE"])
        return self._my_world_size


@enum.unique
class PGSignals(enum.IntEnum):
    """Signals passed to and from Manager state runner to communicate flow of ProcessGroup state

    :param SUCCESS: requested signal succeeded
    :param RAISE_EXCEPTION: raise the exception included in the payload
    :param ADD_TEMPLATES: add the given list of ProcessTemplates
    :param READY_TO_START: ProcessTemplates have been given to the Manager
    :param START: start all processes/workers
    :param INVALID_REQUEST: requested signal not allowed for current state
    :param CLEAN_IDLE: processes have all exited within the allowed time
    :param WALLTIME_EXPIRED: the allowed walltime has expired
    :param PROCESSES_EXITED: one or more process exited unexpectedly and we weren't told to ignore that
    :param START_JOIN_THREAD: start the joiner thread waiting for state change on the group from GS
    :param READY_TO_RUN: we're in a steady state for running
    :param SIGNAL: send a Linux signal to processes
    :param JOIN: Wait for all the processes to complete
    :param JOIN_TIMEOUT: a timeout occured on join
    :param STATE: query the current state
    :param PUIDS: request state runner to return worker puids
    :param JOIN_POKE: query the status of the worker processes in order to maintain state awareness
    :param JOIN_FINAL: joiner thread is down and we need to scan join requests again for completion
    :param STOP: bring down processes but leave ProcessGroup object still usable
    :param CLOSE: imply stop and then instruct manager to shutdown
    :param STOP_MAINTAINING: start joiner thread with join_all=True and cease restarting of worker processes if it was being done


    """

    SUCCESS = enum.auto()  # requested signal succeeded
    RAISE_EXCEPTION = enum.auto()  # raise the exception included in the payload
    ADD_TEMPLATES = enum.auto()  # add the given list of ProcessTemplates
    READY_TO_START = enum.auto()  # ProcessTemplates have been given to the Manager
    START = enum.auto()  # start all processes/workers
    INVALID_REQUEST = enum.auto()  # requested signal not allowed for current state
    CLEAN_IDLE = enum.auto()  # processes have all exited within the allowed time
    WALLTIME_EXPIRED = enum.auto()  # the allowed walltime has expired
    PROCESSES_EXITED = enum.auto()  # one or more process exited unexpectedly and we weren't told to ignore that
    START_JOIN_THREAD = enum.auto()  # start the joiner thread waiting for state change on the group from GS
    READY_TO_RUN = enum.auto()  # we're in a steady state for running
    SIGNAL = enum.auto()  # send a Linux signal to processes
    JOIN = enum.auto()  # Wait for all the processes to complete
    JOIN_TIMEOUT = enum.auto()  # a timeout occured on join
    STATE = enum.auto()  # query the current state
    PUIDS = enum.auto()  # request state runner to return worker puids
    JOIN_POKE = enum.auto()  # query the status of the worker processes in order to maintain state awareness
    JOIN_FINAL = enum.auto()  # joiner thread is down and we need to scan join requests again for completion
    STOP = enum.auto()  # bring down processes but leave ProcessGroup object still usable
    CLOSE = enum.auto()  # imply stop and then instruct manager to shutdown
    STOP_MAINTAINING = (
        enum.auto()
    )  # start joiner thread with join_all=True and cease restarting of worker processes if it was being done

    @classmethod
    def from_str(cls, input_str):
        # Assume it's the int representation first:
        try:
            return cls(int(input_str))
        except KeyError as e:
            raise RuntimeError(f"messed up: {e}")

        # Try the class and enum next
        try:
            sig = input_str.replace(cls.__name__ + ".", "")
            return cls[sig]
        except KeyError as e:
            raise RuntimeError(f"Invalid string representation of PGSignals class: {e}")


class BaseState(ABC):
    pass


@dataclass
class PGProperties:
    """Defines how the ProcessGroup will behave

    :param restart: tells Manager to restart processes indefinitely as they exit, defaults to False
    :type restart: bool
    :param ignore_error_on_exit: determines if ProcessGroup will raise an exception if a worker is not successfullt executed, defaults to False
    :type ignore_error_on_exit: bool
    :param pmi: PMI backend to use for launching MPI applications, defaults to None
    :type pmi: str, Options given in dragon.facts.PMIBackend
    :param walltime: Time to allow ProcessGroup to execute after `start` is called, defaults to None
    :type walltime: float
    :param policy: Use policy objects to define how worker processes should be placed, default to None
    :type policy: Policy
    :param critical: whether to use the Dragon runtime restart capabilities. Unused. Defaults to False
    :type critical: bool
    :param name: Name for idenitifying ProcessGroup. Unused. Defaults to None
    :type name: str
    """

    restart: bool = False
    ignore_error_on_exit: bool = False
    pmi: PMIBackend = None
    walltime: float = None
    policy: Policy = None
    critical: bool = False
    name: str = None
    lock: threading.Lock = None

    def from_dict(self, the_dict) -> None:
        self.restart = the_dict["restart"]
        self.ignore_error_on_exit = the_dict["ignore_error_on_exit"]
        self.pmi = the_dict["pmi"]
        self.walltime = the_dict["walltime"]
        try:
            self.policy = Policy.from_sdict(the_dict["policy"])
        except Exception:
            self.policy = None
        try:
            self.pmi = PMIBackend.from_str(the_dict["pmi"])
        except Exception:
            self.pmi = None
        self.critical = the_dict["critical"]
        self.name = the_dict["name"]


@dataclass(frozen=True)
class PGState:
    """
    ProcessGroup State that is the main means of communicating state changes

    :param state: state of object
    :type state: BaseState
    :param g_uid: group uid. Used to identify the group of processes in the Global Services API
    :type g_uid: int
    :param group_descr: contains description of the group
    :type group_descr: GroupDescriptor
    :param p_templates: process templates. Many processes be paired to a given template
    :type p_templates: list[tuple[int, ProcessTemplate]]
    :param p_templates_expanded: process templates expanded such that 1 template defines 1 process
    :type p_templates_expanded: list[ProcessTemplate]
    :param critical: Whether processes should be treated as critical and trigger a dragon runtime restart. Unused.
    :type critical: bool
    :param joiner_thread: Background thread the Manager uses to query execution status of processes via the GS API
    :type joiner_thread: threading.Thread
    :param walltime_event: Event that marks whether passed walltime supersedes the max set by the user.
    :type walltime_event: threading.Event
    :param pending_replies: queue holding replies for outstanding join requests
    :type pending_replies: queue.Queue
    :param exq: queue created by client that the Manager drops worker exceptions into
    :type exq: Queue
    :param pmix_ddict: Serialized descriptor of ddict used for PMIx KVS
    :type pmix_ddict: str
    """

    state: BaseState
    g_uid: int = None
    group_descr: GroupDescriptor = None
    p_templates: list[tuple] = None  # (nprocs, ProcessTemplate) as given by a client
    p_templates_expanded: list = None  # one template per process
    critical: bool = False
    joiner_thread: threading.Thread = None
    walltime_event: threading.Event = None
    pending_replies: queue.Queue = None
    exq: Queue = None
    pmix_ddict: str = None


@dataclass(frozen=True)
class PGSignalMessage:
    """Messages passed within Manager to change states and communicate success/failure

    :param signal: signal describing requestied action or reply
    :type signal: PGSignals
    :param p_uid: requesting puid
    :type p_uid: int
    :param desired_state: state to immediately transition to
    :type desired_state: PGState
    :param payload: any additional information needed to passed to desired states that doesn't fit into message
    :type payload: dict
    :param tag: tag uniquely identifying this message
    :type tag: int
    :param skip_reply: whether to send a message back to the client
    :type skip_reply: bool
    :param close: whether to close the state runner thread and ultimately the Manager
    :type close: bool
    """

    signal: PGSignals
    p_uid: int = None
    desired_state: PGState = None
    payload: dict = None
    tag: int = None
    skip_reply: bool = False
    close: bool = False


@dataclass
class PGProcessHistory:
    """History of all worker processes associated with a given ProcessGroup

    :param active_nprocs: Number of workers currently executing or have exited and haven't yet been archived
    :type active_nprocs: int
    :param active_processes: puids and exit code (puid, ecode) of workers currently executing or have exited and haven't yet been archived
    :type active_processes: list[tuple(int, int)]
    :param active_processes_inv_map: dictionary mapping puid (key) to index in the active_processes list to enable faster lookup
    :type active_processes_inv_map: dict
    :param inactive_nprocs: Number of workers who have exited
    :type inactive_nprocs: int
    :param inactive_procs: puids and exit code (puid, ecode) of workers who have exited and have yet been archived
    :param lock: lock surrounding access to the processes' lists
    :type lock: threading.Lock
    :param archived: whether active processes have been archived. Can help limit some unnecessary list traversals
    :type archived: bool
    """

    active_nprocs: int = 0
    active_processes: list = field(
        default_factory=list
    )  # tuple of p_uid and exit code (matching the current g_uid state)
    active_processes_inv_map: dict = field(default_factory=dict)  # map of p_uid to index in active_processes
    inactive_nprocs: int = 0
    inactive_processes: list = field(default_factory=list)  # tuple of p_uid and exit code (from all previous g_uids)
    guids: list = field(default_factory=list)
    lock: threading.Lock = None
    archived: bool = False

    def init_active_processes(self, p_uids: List) -> None:
        """Used to initialize the processes for the current group being managed with the given p_uids

        :param p_uids: puids that have been started
        :type p_uids: List[int]
        """

        log = logging.getLogger("pg_init_active_processes")
        log.debug("Replace and archive processes")
        self.active_nprocs = len(p_uids)
        self.active_processes = [(p_uid, None) for p_uid in p_uids]
        self.active_processes_inv_map = {tu[0]: i for i, tu in enumerate(self.active_processes)}

    def replace_and_archive_processes(self, new_puids: List[int], old_puids_idx: List[Tuple[int, int]]) -> None:
        """Archive exited processes and update the active processes' lists and inverse dictionary map

        :param new_puids: puids to place in the active_processes attribute
        :type new_puids: List[int]
        :param old_puids_idx: indices of processes that need to be removed from active_processes list
        :type old_puids_idx: List[Tuple[int, int]]
        """
        log = logging.getLogger("pg_replace_and_archive")
        log.debug("Replace and archive processes")
        assert len(new_puids) == len(old_puids_idx)
        self.inactive_nprocs += len(old_puids_idx)

        for (old_puid, idx), new_puid in zip(old_puids_idx, new_puids):
            self.inactive_processes.append(self.active_processes[idx])
            self.active_processes[idx] = (new_puid, None)
            del self.active_processes_inv_map[old_puid]
            self.active_processes_inv_map[new_puid] = idx

    def archive_active(self) -> None:
        """Move exited processes from active_processes list to inactive_processes"""
        self.inactive_nprocs += self.active_nprocs
        self.active_nprocs = 0

        log = logging.getLogger("pg_archive")
        log.debug("active processes in archive: %s", self.active_processes)
        self.inactive_processes += self.active_processes
        log.debug("inactive processes in archive: %s", self.inactive_processes)
        self.active_processes = []
        self.active_processes_inv_map = {}
        self.archived = True

    def get_running_p_uids(self) -> List:
        """Get a list of p_uids that are in the active set and are currently running

        :returns: puids that are currently running
        :rtype: {List}
        """

        return [p_uid for p_uid, exitc in self.active_processes if exitc is None]

    def get_exited_procs(self) -> List[Tuple]:
        """Get a list of (p_uid, exit_code) for processes in the active set that have exited

        :returns: puids that have exited and their exit code (puid, ecode)
        :rtype: {List[Tuple[int, int]]}
        """

        return [(p_uid, exitc) for p_uid, exitc in self.active_processes if exitc is not None]

    def get_nonzero_exited_procs(self) -> List[Tuple]:
        """Get a list of (p_uid, exit_code) for process that have exited with a non-zero exit

        :returns: puids that have exited with non-zero exit codes (puid, ecode)
        :rtype: {List[Tuple[int, int]]}
        """

        return [(p_uid, exitc) for p_uid, exitc in self.active_processes if exitc not in [None, 0]]

    def get_archived_procs(self) -> List[Tuple]:
        """Get a list of (p_uid, exit_code) for processes in the active set that have exited

        :returns: puids that have been previously archived
        :rtype: {List[Tuple[int, int]]}
        """

        return self.inactive_processes

    def update_active_processes(self, puid_ecodes: List[Tuple[int, int]]) -> None:
        """Given a list of tuples made up of puids and exit codes, update our active processes list

        :param puid_ecodes: puids and their exit codes (puid, ecode)
        :type puid_ecodes: List[Tuple[int, int]]
        """

        for p_uid, exitc in puid_ecodes:
            try:
                idx = self.active_processes_inv_map[p_uid]
                self.active_processes[idx] = (p_uid, exitc)

            # Something has already moved this to the archive
            except KeyError:
                pass

    def add_guid(self, guid: int) -> None:
        """Add a guid to the history once it's created"""

        self.guids.append(guid)

    def get_guids(self) -> List:
        """Return a list of all guids for groups this manager has overseen execution of"""

        return self.guids

    def __str__(self) -> str:
        return f"ProcessGroup History running procs (count={self.active_nprocs}) = [{list(self.get_running_p_uids())}]"


def _generate_process_create_messages(templates: List[ProcessTemplate], props: PGProperties):
    messages = {}
    for i, tup in enumerate(templates):
        t = tup[1]

        if t.is_python:
            messages[i] = get_create_message_with_argdata(
                t.target,
                t.cwd,
                t.args,
                t.env,
                t.argdata,
                pmi=props.pmi,
                stdin=t.stdin,
                stdout=t.stdout,
                stderr=t.stderr,
                policy=t.policy,
                options=t.options,
            )
        else:
            messages[i] = get_create_message(
                t.target,
                t.cwd,
                t.args,
                t.env,
                pmi=props.pmi,
                stdin=t.stdin,
                stdout=t.stdout,
                stderr=t.stderr,
                policy=t.policy,
                options=t.options,
            )
    return messages


class State(Enum):
    pass


class BaseState(ABC):
    """This class declares methods that all concrete State classes should implement."""

    allowed_sigs: list[int] = None
    query_sigs = {PGSignals.STATE, PGSignals.PUIDS}

    @abstractmethod
    def run(
        self, signal_msg: PGSignalMessage, pstate: PGState, cur_procs: PGProcessHistory, props: PGProperties
    ) -> PGSignalMessage:
        """Execute the run function defined by the parent state"""
        pass

    @property
    @abstractmethod
    def state(self):
        """Return the appropriate value from the State enum"""

    def __repr__(self):
        return f"{self.__class__.__name__}()"

    def __str__(self):
        return f"{self.__class__.__name__}"


class Error(BaseState):
    """Error state class."""

    @property
    def state(self):
        return State.ERROR

    def run(
        self, signal_msg: PGSignalMessage, pstate: PGState, cur_procs: PGProcessHistory, props: PGProperties
    ) -> PGSignalMessage:
        pass


class Idle(BaseState):
    """This state brings down existing processes and does nothing otherwise. This state blocks until process are all
    down.

    Optional message payload:
        :param patience: number of seconds to wait between successive Linux signals
        :type patience: float

    Raises DragonProcessGroupIdleError in case of error.
    """

    @property
    def state(self):
        return State.IDLE

    allowed_sigs = [
        PGSignals.READY_TO_START,
        PGSignals.START,
        PGSignals.CLEAN_IDLE,
        PGSignals.WALLTIME_EXPIRED,
        PGSignals.PROCESSES_EXITED,
        PGSignals.JOIN,
        PGSignals.STOP,
        PGSignals.CLOSE,
    ]

    def run(
        self, signal_msg: PGSignalMessage, pstate: PGState, cur_procs: PGProcessHistory, props: PGProperties
    ) -> PGSignalMessage:
        fdebug, finfo = get_logs("State=Idle")

        try:
            patience = signal_msg.payload["patience"]
        except Exception:
            patience = _DEFAULT_STOP_PATIENCE

        with cur_procs.lock:
            running_procs = cur_procs.get_running_p_uids()
        if len(running_procs) > 0:
            # phase one is to issue a SIGINT
            try:
                fdebug("sending SIGINT")
                group_kill(pstate.g_uid, sig=signal.SIGINT, hide_stderr=True)
            except (AttributeError, RuntimeError, GroupError):
                pass

            try:
                success, puid_stat = multi_join(
                    running_procs, join_all=True, timeout=patience, return_on_bad_exit=False
                )

                if success:
                    with cur_procs.lock:
                        cur_procs.update_active_processes(success)
                else:
                    # phase two is to issue SIGTERM after giving it some time
                    try:
                        fdebug("sending SIGTERM")
                        group_kill(pstate.g_uid, sig=signal.SIGTERM, hide_stderr=True)
                    except (AttributeError, RuntimeError, GroupError):
                        pass
                    success, puid_stat = multi_join(
                        running_procs, join_all=True, timeout=patience, return_on_bad_exit=False
                    )

                    if success:
                        with cur_procs.lock:
                            cur_procs.update_active_processes(success)
                    else:
                        # phase three is to issue SIGKILL after giving it some time
                        try:
                            fdebug("sending SIGKILL")
                            group_kill(self.state.guid, sig=signal.SIGKILL, hide_stderr=True)
                        except (AttributeError, RuntimeError, GroupError):
                            pass
                        success, puid_stat = multi_join(
                            running_procs, join_all=True, timeout=None, return_on_bad_exit=False
                        )

                        with cur_procs.lock:
                            cur_procs.update_active_processes(success)

                # group is done for so archive it
                cur_procs.archive_active()

            except Exception:
                raise DragonProcessGroupIdleError(DragonError.FAILURE, "Unable to completely end processes")

        if signal_msg.signal == PGSignals.START:
            fdebug("Requesting a Start state")
            desired_state = PGState(
                state=Start(),
                p_templates=pstate.p_templates,
                exq=signal_msg.payload["exq"],
                pmix_ddict=signal_msg.payload["pmix_sdd"],
            )
            msg = PGSignalMessage(signal=PGSignals.READY_TO_START, desired_state=desired_state)
        elif (signal_msg.signal == PGSignals.STOP or signal_msg.signal == PGSignals.CLOSE) and pstate.g_uid is not None:
            desired_state = PGState(
                state=Join(),
                g_uid=pstate.g_uid,
                group_descr=pstate.group_descr,
                p_templates=pstate.p_templates,
                p_templates_expanded=pstate.p_templates_expanded,
                critical=pstate.critical,
                joiner_thread=pstate.joiner_thread,
                walltime_event=pstate.walltime_event,
                pending_replies=pstate.pending_replies,
            )

            closeit = signal_msg.close
            if signal_msg.signal == PGSignals.CLOSE:
                closeit = True

            msg = PGSignalMessage(
                signal=PGSignals.JOIN_FINAL, desired_state=desired_state, tag=signal_msg.tag, close=closeit
            )
        else:
            fdebug("Requesting an Idle state")

            if signal_msg.signal == PGSignals.CLOSE:
                closeit = True
                desired_state = PGState(state=Idle(), p_templates=pstate.p_templates)
            else:
                closeit = signal_msg.close
                desired_state = PGState(state=Idle(), p_templates=pstate.p_templates)
            msg = PGSignalMessage(signal=PGSignals.SUCCESS, desired_state=desired_state, close=closeit)
        return msg


class Start(BaseState):
    """Start a new GS process group and return the new PGState

    Required message payload:
        None
    """

    allowed_sigs = [PGSignals.READY_TO_START]

    @property
    def state(self):
        return State.START

    def run(
        self, signal_msg: PGSignalMessage, pstate: PGState, cur_procs: PGProcessHistory, props: PGProperties
    ) -> PGSignalMessage:
        fdebug, finfo = get_logs("State=Start")

        messages = _generate_process_create_messages(pstate.p_templates, props)

        expanded_templates = []
        for i, (replicas, create_template) in enumerate(pstate.p_templates):
            expanded_templates.extend(replicas * [create_template])

        # Create a ddict for PMIx servers if PMIx backend was requested.
        try:
            fdebug("Sending group create request with pmix ddict: %s", pstate.pmix_ddict)
            group_descr = group_create(
                [(replicas, messages[i].serialize()) for i, (replicas, _) in enumerate(pstate.p_templates)],
                props.policy,
                pmix_ddict=pstate.pmix_ddict,
            )

        except Exception:
            raise DragonProcessGroupStartError(DragonError.FAILURE, "Failed to create GS group")

        # TODO to add Maintain path
        fdebug("start msg: %s", signal_msg)
        desired_state = PGState(
            state=MakeJoiner(),
            g_uid=group_descr.g_uid,
            group_descr=group_descr,
            p_templates=pstate.p_templates,
            p_templates_expanded=expanded_templates,
            critical=group_descr.resilient,
            walltime_event=threading.Event(),
            pending_replies=queue.Queue(),
            exq=pstate.exq,
        )

        with cur_procs.lock:
            cur_procs.init_active_processes([descr.uid for lst in group_descr.sets for descr in lst])
            cur_procs.add_guid(group_descr.g_uid)

        return PGSignalMessage(signal=PGSignals.START_JOIN_THREAD, desired_state=desired_state)


class MakeJoiner(BaseState):
    """Start a thread whose job is simply to join on the group

    Required message payload:
        None
    """

    allowed_sigs = [PGSignals.START_JOIN_THREAD]

    @property
    def state(self):
        return State.MAKE_JOINER

    def run(
        self, signal_msg: PGSignalMessage, pstate: PGState, cur_procs: PGProcessHistory, props: PGProperties
    ) -> PGSignalMessage:
        fdebug, finfo = get_logs("State=MakeJoiner")

        th = threading.Thread(
            target=self._join_runner, args=(pstate, cur_procs, props, pstate.walltime_event), daemon=False
        )
        th.start()

        desired_state = PGState(
            state=Running(),
            g_uid=pstate.g_uid,
            group_descr=pstate.group_descr,
            p_templates=pstate.p_templates,
            p_templates_expanded=pstate.p_templates_expanded,
            critical=pstate.critical,
            joiner_thread=th,
            walltime_event=pstate.walltime_event,
            pending_replies=pstate.pending_replies,
            exq=pstate.exq,
        )

        return PGSignalMessage(signal=PGSignals.READY_TO_RUN, desired_state=desired_state)

    @staticmethod
    def _join_runner(pstate: PGState, cur_procs: PGProcessHistory, props: PGProperties, event: threading.Event):
        """The join runner does just that. It may wait for any process to exit, all processes to exit, or
        a walltime to expire. Nothing is returned. Another action is taken to discover the outcome.
        If the walltime has expired, that event is set.
        """
        fdebug, finfo = get_logs("State=MakeJoiner._join_runner")
        if pstate.g_uid is None:
            raise DragonProcessGroupRunningError(DragonError.FAILURE, "No GS group defined to enter Running state")

        fdebug("Started join runner thread")

        try:
            if props.ignore_error_on_exit is True:
                return_on_bad_exit = False
            else:
                return_on_bad_exit = True

            with cur_procs.lock:
                active_puids = cur_procs.get_running_p_uids()

            if active_puids:
                keep_going = True
                with props.lock:
                    cur_restart_flg = props.restart
                while keep_going:
                    if cur_restart_flg is True:
                        join_all = False
                    else:
                        join_all = True

                    success, puid_stat = multi_join(
                        active_puids, join_all=join_all, timeout=props.walltime, return_on_bad_exit=return_on_bad_exit
                    )
                    with props.lock:
                        orig_restart = props.restart
                    if cur_restart_flg == orig_restart:
                        keep_going = False
                    else:
                        cur_restart_flg = orig_restart

                fdebug(
                    "Joiner thread observed a multi_join return (join_all=%s, return_on_bad_exit=%s)",
                    join_all,
                    return_on_bad_exit,
                )

                failed_exits, timeout_f = get_multi_join_failure_puids(puid_stat)
                clean_exits, timeout_s = get_multi_join_success_puids(puid_stat)

                with cur_procs.lock:
                    cur_procs.update_active_processes(failed_exits)
                    cur_procs.update_active_processes(clean_exits)

            # If we are supposed to respond to errors, check the exit codes
            if not props.ignore_error_on_exit and len(failed_exits) > 0:
                exit_codes = [failed_exit[1] for failed_exit in failed_exits]
                fdebug("putting exception in queue for %s", failed_exits)
                pstate.exq.put(
                    DragonUserCodeError(
                        DragonError.USER_CODE_ERROR,
                        f"Error(s) in user-provided code resulted in exit codes: {exit_codes}",
                    )
                )

        except Exception:
            raise DragonProcessGroupRunningError(DragonError.FAILURE, "Failed joining on group with GS")

        if active_puids:
            if (timeout_f or timeout_s) and props.walltime is not None:
                event.set()


class Running(BaseState):
    """Verify the set of processes are still healthy

    Optional message payload:
        :param patience: seconds to wait for the joining thread
        :type patience: float
    """

    allowed_sigs = [
        PGSignals.READY_TO_RUN,
        PGSignals.JOIN,
        PGSignals.JOIN_POKE,
        PGSignals.JOIN_FINAL,
        PGSignals.STOP,
        PGSignals.CLOSE,
        PGSignals.STOP_MAINTAINING,
    ]

    @property
    def state(self):
        return State.RUNNING

    def run(
        self, signal_msg: PGSignalMessage, pstate: PGState, cur_procs: PGProcessHistory, props: PGProperties
    ) -> PGSignalMessage:
        fdebug, finfo = get_logs("State=Running")

        if signal_msg.signal == PGSignals.STOP or signal_msg.signal == PGSignals.CLOSE:
            fdebug("signal = [STOP, CLOSE]")
            closeit = False
            if signal_msg.signal == PGSignals.CLOSE:
                closeit = True
            desired_state = PGState(
                state=Idle(),
                g_uid=pstate.g_uid,
                group_descr=pstate.group_descr,
                p_templates=pstate.p_templates,
                p_templates_expanded=pstate.p_templates_expanded,
                critical=pstate.critical,
                joiner_thread=pstate.joiner_thread,
                walltime_event=pstate.walltime_event,
                pending_replies=pstate.pending_replies,
            )
            return PGSignalMessage(
                signal=signal_msg.signal,
                p_uid=signal_msg.p_uid,
                desired_state=desired_state,
                payload=signal_msg.payload,
                tag=signal_msg.tag,
                close=closeit,
            )

        # a few notes:
        # not that we inject skip_reply here. This is done because this is the state that transitions to
        # processing joins. Joins do not create replies in the normal fashion (due to timeout handling)
        # instead they are queue up into a pending_replies queue. Setting skip_reply tells the state
        # manager not to bother creating a reply to a join request because that is handled separately.
        #
        # We handle join replies by passing along a payload with a "replies" field. This points to queue
        # that holds the join replies
        if signal_msg.signal == PGSignals.JOIN or signal_msg.signal == PGSignals.JOIN_POKE:
            fdebug("signal = [JOIN, JOIN_POKE]")
            desired_state = PGState(
                state=Join(),
                g_uid=pstate.g_uid,
                group_descr=pstate.group_descr,
                p_templates=pstate.p_templates,
                p_templates_expanded=pstate.p_templates_expanded,
                critical=pstate.critical,
                joiner_thread=pstate.joiner_thread,
                walltime_event=pstate.walltime_event,
                pending_replies=pstate.pending_replies,
                exq=pstate.exq,
            )

            if pstate.joiner_thread.is_alive():
                fdebug("joiner_thread is alive")
                return PGSignalMessage(
                    signal=signal_msg.signal,
                    p_uid=signal_msg.p_uid,
                    desired_state=desired_state,
                    payload=signal_msg.payload,
                    skip_reply=True,
                    tag=signal_msg.tag,
                )
            else:
                fdebug("joiner_thread is dead")
                if pstate.g_uid is None:
                    return PGSignalMessage(
                        signal=PGSignals.JOIN_FINAL,
                        p_uid=signal_msg.p_uid,
                        desired_state=desired_state,
                        payload=signal_msg.payload,
                        skip_reply=False,
                        tag=signal_msg.tag,
                    )
                else:
                    return PGSignalMessage(
                        signal=PGSignals.JOIN_FINAL,
                        p_uid=signal_msg.p_uid,
                        desired_state=desired_state,
                        payload=signal_msg.payload,
                        skip_reply=True,
                        tag=signal_msg.tag,
                    )

        # TODO: Colin, get sign off on this from someone else
        # When stop_restart is called change the behavior of the join runner.
        # Specifically, we need to act as if it had entered Running state so
        # the join runner should now be run with join_all=True. This allows us
        # to then call join() on the group and the Running state not
        # accidentally think that all the process are down since the join runner
        # is gone. The easiest way to get the ordering right seemed to be to add
        # another signal so that Running would get called and allow us to
        # restart the join runner.
        if signal_msg.signal == PGSignals.STOP_MAINTAINING:
            fdebug("signal = STOP_MAINTAINING")
            with props.lock:
                props.restart = False

            # transition to start the joiner thread
            desired_state = PGState(
                state=MakeJoiner(),
                g_uid=pstate.g_uid,
                group_descr=pstate.group_descr,
                p_templates=pstate.p_templates,
                p_templates_expanded=pstate.p_templates_expanded,
                critical=pstate.critical,
                joiner_thread=pstate.joiner_thread,
                walltime_event=pstate.walltime_event,
                pending_replies=pstate.pending_replies,
            )

            return PGSignalMessage(
                signal=PGSignals.START_JOIN_THREAD,
                desired_state=desired_state,
                skip_reply=signal_msg.skip_reply,
                payload=signal_msg.payload,
                close=signal_msg.close,
            )

        if pstate.joiner_thread.is_alive():
            return PGSignalMessage(signal=PGSignals.SUCCESS, desired_state=pstate, skip_reply=signal_msg.skip_reply)

        with props.lock:
            cur_restart = props.restart
        if not cur_restart:
            fdebug("Running: restart = False")
            desired_state = PGState(
                state=Idle(),
                g_uid=pstate.g_uid,
                group_descr=pstate.group_descr,
                p_templates=pstate.p_templates,
                p_templates_expanded=pstate.p_templates_expanded,
                critical=pstate.critical,
                joiner_thread=pstate.joiner_thread,
                walltime_event=pstate.walltime_event,
                pending_replies=pstate.pending_replies,
            )

            if pstate.walltime_event.is_set():
                return PGSignalMessage(
                    signal=PGSignals.WALLTIME_EXPIRED,
                    desired_state=desired_state,
                    skip_reply=signal_msg.skip_reply,
                    payload=signal_msg.payload,
                    close=signal_msg.close,
                )

            non_zero = False
            with cur_procs.lock:
                exits = [exitc for _, exitc in cur_procs.active_processes]
            for exitc in exits:
                if exitc is not None and exitc > 0:
                    non_zero = True
            if non_zero:
                msg = PGSignalMessage(
                    signal=PGSignals.PROCESSES_EXITED,
                    desired_state=desired_state,
                    skip_reply=signal_msg.skip_reply,
                    payload=signal_msg.payload,
                    close=signal_msg.close,
                )
            else:
                msg = PGSignalMessage(
                    signal=PGSignals.CLEAN_IDLE,
                    desired_state=desired_state,
                    skip_reply=signal_msg.skip_reply,
                    payload=signal_msg.payload,
                    close=signal_msg.close,
                )

        else:
            fdebug("Maintaining: restart = True")
            # determine which processes have gone missing
            templates = []
            # policies = []
            gone_procs = []
            with cur_procs.lock:
                for idx, item in enumerate(cur_procs.active_processes):
                    p_uid, exitc = item
                    if exitc != None:
                        fdebug("Found process exited that must be restarted: p_uid=%i, idx=%i", p_uid, idx)
                        templates.append((1, pstate.p_templates_expanded[idx]))
                        gone_procs.append((p_uid, idx))

            messages = _generate_process_create_messages(templates, props)

            group_desc = group_create_add_to(
                pstate.g_uid, [(tup[0], messages[i].serialize()) for i, tup in enumerate(templates)], props.policy
            )

            with cur_procs.lock:
                # update cur_procs with the new process(es) while archiving the old one(s)
                all_puids = [descr.uid for lst in group_desc.sets for descr in lst]
                # there is certainly a better way to get all of the new_puids
                # currently use set differencing with all active and inactive puids
                new_puids = list(
                    set(all_puids)
                    .difference([puid for puid, _ in cur_procs.active_processes])
                    .difference([puid for puid, _ in cur_procs.inactive_processes])
                )
                cur_procs.replace_and_archive_processes(new_puids, gone_procs)

            # transition to start the joiner thread
            desired_state = PGState(
                state=MakeJoiner(),
                g_uid=pstate.g_uid,
                group_descr=pstate.group_descr,
                p_templates=pstate.p_templates,
                p_templates_expanded=pstate.p_templates_expanded,
                critical=pstate.critical,
                joiner_thread=pstate.joiner_thread,
                walltime_event=pstate.walltime_event,
                pending_replies=pstate.pending_replies,
            )

            return PGSignalMessage(
                signal=PGSignals.START_JOIN_THREAD,
                desired_state=desired_state,
                skip_reply=signal_msg.skip_reply,
                payload=signal_msg.payload,
                close=signal_msg.close,
            )

        return msg


class Join(BaseState):
    """Process and pending join operations

    Optional message payload:
        :param patience: seconds to wait for the joining thread
        :type patience: float

    """

    allowed_sigs = [PGSignals.JOIN, PGSignals.JOIN_POKE, PGSignals.JOIN_FINAL]

    @property
    def state(self):
        return State.JOIN

    def run(
        self, signal_msg: PGSignalMessage, pstate: PGState, cur_procs: PGProcessHistory, props: PGProperties
    ) -> PGSignalMessage:
        fdebug, finfo = get_logs("State=Join")

        q = pstate.pending_replies

        # append new request to the joinq
        if signal_msg.signal == PGSignals.JOIN:
            requester = signal_msg.p_uid
            timeout = None
            deadline = time.clock_gettime(time.CLOCK_MONOTONIC) + 1.0e9
            try:
                timeout = signal_msg.payload["timeout"]
                deadline = time.clock_gettime(time.CLOCK_MONOTONIC) + timeout
            except Exception:
                pass
            try:
                fdebug(
                    "Adding in new client join request: %s, %s, %s, %s", requester, timeout, deadline, signal_msg.tag
                )
                q.put((requester, timeout, deadline, signal_msg.tag))
            except Exception:
                raise DragonProcessGroupJoinError(DragonError.FAILURE, "Join pending queue not set or is full!")

        # try to progress each join and respond to any timeouts
        payload = None
        put_back = []

        while not q.empty():
            if payload is None:
                payload = []
            requester, timeout, deadline, tag = q.get()

            fdebug("Inspecting join request: %s, %s, %s, %s", requester, timeout, deadline, tag)
            if pstate.joiner_thread.is_alive():
                if timeout is None:
                    put_back.append((requester, timeout, deadline, tag))
                else:
                    now = time.clock_gettime(time.CLOCK_MONOTONIC)
                    if now > deadline:
                        fdebug("Client join timed out: %s, %s, %s, %s", requester, timeout, deadline, tag)
                        payload.append((requester, PGSignalMessage(signal=PGSignals.JOIN_TIMEOUT, tag=tag)))
                    else:
                        put_back.append((requester, timeout, deadline, tag))
            else:
                fdebug("Client join completed: %s, %s, %s, %s", requester, timeout, deadline, tag)
                payload.append((requester, PGSignalMessage(signal=PGSignals.SUCCESS, tag=tag)))

        for item in put_back:
            fdebug("Put back join request: %s, %s, %s, %s", item[0], item[1], item[2], item[3])
            q.put(item)

        fdebug("Joins to message: %s", payload)

        desired_state = PGState(
            state=Running(),
            g_uid=pstate.g_uid,
            group_descr=pstate.group_descr,
            p_templates=pstate.p_templates,
            p_templates_expanded=pstate.p_templates_expanded,
            critical=pstate.critical,
            joiner_thread=pstate.joiner_thread,
            walltime_event=pstate.walltime_event,
            pending_replies=pstate.pending_replies,
        )

        return PGSignalMessage(
            signal=PGSignals.READY_TO_RUN,
            desired_state=desired_state,
            payload={"replies": payload},
            skip_reply=signal_msg.skip_reply,
            tag=signal_msg.tag,
            close=signal_msg.close,
        )


class AddTemplates(BaseState):
    """Modify the PGState with the given ProcessTemplate list

    Required message payload:
        :param p_templates: List of nproc ProcessTemplate objects for create worker processes with
        :type p_template: list[(int, ProcessTemplate)]
    """

    allowed_sigs = [PGSignals.ADD_TEMPLATES]

    @property
    def state(self):
        return State.ADD_TEMPLATES

    def run(
        self, signal_msg: PGSignalMessage, pstate: PGState, cur_procs: PGProcessHistory, props: PGProperties
    ) -> PGSignalMessage:
        fdebug, finfo = get_logs("State=AddTemplates")

        desired_state = PGState(state=Idle(), p_templates=signal_msg.payload["p_templates"])

        return PGSignalMessage(signal=PGSignals.READY_TO_START, desired_state=desired_state)


class State(Enum):
    ERROR = str(Error())
    IDLE = str(Idle())
    START = str(Start())
    MAKE_JOINER = str(MakeJoiner())
    RUNNING = str(Running())
    JOIN = str(Join())
    ADD_TEMPLATES = str(AddTemplates())


class Benign(ABC):
    """Base class for ProcessGroup operations to don't change ProcessGroup State, eg: queries and signals"""

    allowed_sigs: list[int] = None

    @abstractmethod
    def run(
        self, signal_msg: PGSignalMessage, pstate: PGState, cur_procs: PGProcessHistory, props: PGProperties
    ) -> PGSignalMessage:
        """Execute the run function defined by the parent state"""
        pass

    def __repr__(self):
        return f"{self.__class__.__name__}()"

    def __str__(self):
        return f"{self.__class__.__name__}"


class Signal(Benign):
    """Send a Linux OS signal to the ProcessGroup worker processes

    Required message payload:
        :param signal: signal to transmit to worker processes
        :type signal: signal.Signals
        :param hide_stderr: whether to hide stderr. Useful for limiting noise when sending SIGINT
        :type hide_stderr: bool
    """

    allowed_sigs = [PGSignals.SIGNAL]

    def run(
        self, signal_msg: PGSignalMessage, pstate: PGState, cur_procs: PGProcessHistory, props: PGProperties
    ) -> PGSignalMessage:
        fdebug, finfo = get_logs("Benign=Signal")

        if pstate.g_uid is None:
            raise DragonProcessGroupSignalError(
                DragonError.FAILURE, "cannot send signal to a ProcessGroup before it is started"
            )

        msg = PGSignalMessage(signal=PGSignals.SUCCESS)
        try:
            fdebug(
                "group_kill on g_uid=%s with sig=%s (hide stderr=%s)",
                pstate.g_uid,
                signal_msg.payload["signal"],
                signal_msg.payload["hide_stderr"],
            )
            group_kill(pstate.g_uid, sig=signal_msg.payload["signal"], hide_stderr=signal_msg.payload["hide_stderr"])
        except Exception as ex:
            tb = traceback.format_exc()
            msg = PGSignalMessage(signal=PGSignals.RAISE_EXCEPTION, payload={"exception": ex, "traceback": tb})
        return msg


class Query(Benign):
    """Query a given aspect of the ProcessGroup State

    Required message payload:
        None for state query
        :param active: ask for currently executing puids
        :type active: bool
        :param inactive: ask for exited puids and their exit codes
        :type active: bool

    """

    allowed_sigs = [PGSignals.STATE, PGSignals.PUIDS]

    def run(
        self, signal_msg: PGSignalMessage, pstate: PGState, cur_procs: PGProcessHistory, props: PGProperties
    ) -> PGSignalMessage:
        log = logging.getLogger("query eval")
        log.debug("doing eval of %s", signal_msg)

        reply = None

        # Handle state query
        if signal_msg.signal is PGSignals.STATE:
            log.debug("forming a reply for state")
            cur_state = str(pstate.state)
            reply = PGSignalMessage(signal=PGSignals.SUCCESS, payload={"current_state": cur_state})
            log.debug("have reply %s", reply)

        elif signal_msg.signal is PGSignals.PUIDS:
            log.debug("forming a reply for puids query")
            active_puids = inactive_puids = None
            if signal_msg.payload["active"]:
                with cur_procs.lock:
                    active_puids = cur_procs.get_running_p_uids()
            elif signal_msg.payload["inactive"]:
                log.debug("getting the exited processes. state = %s", pstate.state)
                with cur_procs.lock:
                    inactive_puids = cur_procs.get_archived_procs() + cur_procs.get_exited_procs()
            log.debug("returning collection: active = %s | inactive = %s", active_puids, inactive_puids)
            reply = PGSignalMessage(
                signal=PGSignals.SUCCESS, payload={"active": active_puids, "inactive": inactive_puids}
            )

        return reply


class Manager:
    """This class defines the server for ProcessGroup. It's job is to accept requests for manipulating the state
    of the ProcessGroup and for serving up status information.
    """

    _QPATIENCE = 0.3
    _LOCALQPATIENCE = 0.01
    _DTBL = {}  # dispatch router, keyed by type of message -- for messages into state runner

    def __init__(self, inq: Queue = None, exq: Queue = None, name: str = None):
        self._inq = inq
        self._exq = exq
        self._puid = this_process.my_puid
        self._props = None

        self._clients = dict()
        self._tag = 0

        self._state_inq = queue.Queue()
        self._state_outq = queue.Queue()
        self._sig_id = 0

        self._the_state = None
        self._cur_procs = None

        self._walltime_expired = threading.Event()
        self._join_thread = None

        self.pmix_dd = None
        self.pmix_sdd = None
        self.my_guids = []

        # setup logging with deferred f-string eval support
        pgname = ""
        if name is not None:
            pgname = f"_{name}_"
        fname = f"{dls.PG}_{socket.gethostname()}_manager_{str(self._puid)}{pgname}.log"
        setup_BE_logging(service=dls.PG, fname=fname)
        global _LOG
        _LOG = logging.getLogger(dls.PG).getChild("Manager")

        self._stop_serving = threading.Event()
        self._abnormal_termination = False

    def _send_pending_replies(self, payload):
        fdebug, finfo = get_logs("_send_pending_replies")

        try:
            pending = payload["replies"]
            fdebug("pending join msg = %s", pending)
            for item in pending:
                fdebug("sending out pending join msg to %s", item)
                self._state_outq.put(item)

            payload["replies"] = None
        except Exception:
            return

    @staticmethod
    def _validate_signal(state: BaseState, signal_msg: PGSignalMessage) -> Tuple[bool, PGSignalMessage]:
        """Determine if message from client/Manager main thread is valid

        :param state: Current ProcessGroup state
        :type state: BaseState
        :param signal_msg: Message containing the signal for requested state change and any related payload values
        :type signal_msg: PGSignalMessage
        :returns: Tuple with first value as True if message is value. If False, second arg is a error message to return to main thread
        :rtype: {Tuple[bool, PGSignalMessage]}
        """
        fdebug, finfo = get_logs("_validate_signal")

        if signal_msg.signal in state.allowed_sigs:
            return (True, None)
        else:
            reply_msg = PGSignalMessage(signal=PGSignals.INVALID_REQUEST, payload={"current_state": str(state)})
            return (False, reply_msg)

    def _state_runner(self):
        """Target function of Manager background thread for managing ProcessGroup state transitions

        :raises: DragonProcessGroupError
        """
        fdebug, finfo = get_logs("_state_runner")

        self._the_state = PGState(state=AddTemplates())  # note, qstate is immutable and is only replaced in this method
        self._cur_procs = PGProcessHistory(
            lock=threading.Lock()
        )  # process history is mutable and can be updated by state runs

        finfo("Up and running (p_uid %s) in %s with %s", self._puid, self._the_state.state, self._cur_procs)

        while not self._stop_serving.is_set():
            finfo("Status check with: %s in state %s", self._cur_procs, self._the_state.state)
            try:
                client_msg = self._state_inq.get(timeout=self._QPATIENCE)
            except queue.Empty:
                if self._the_state.state.state == State.RUNNING:
                    fdebug("Faking JOIN_POKE client message for join progressing")
                    client_msg = PGSignalMessage(PGSignals.JOIN_POKE, p_uid=self._puid)
                else:
                    continue

            valid, reply = self._validate_signal(self._the_state.state, client_msg)

            if valid:
                fdebug("Valid message from %s with signal %s", client_msg.p_uid, client_msg.signal)
                try:
                    fdebug("Running: %s", self._the_state.state)
                    desired_state_msg = self._the_state.state.run(
                        signal_msg=client_msg, pstate=self._the_state, cur_procs=self._cur_procs, props=self._props
                    )
                    self._send_pending_replies(desired_state_msg.payload)
                    fdebug(
                        "Completed (return signal %s): %s close=%s",
                        desired_state_msg.signal,
                        self._the_state.state,
                        desired_state_msg.close,
                    )

                    while not isinstance(
                        self._the_state.state, type(desired_state_msg.desired_state.state)
                    ) and not isinstance(self._the_state.state, Error):
                        valid, _ = self._validate_signal(desired_state_msg.desired_state.state, desired_state_msg)
                        if not valid:
                            raise DragonProcessGroupError(
                                DragonError.FAILURE, "Critical ProcessGroup state change error"
                            )

                        fdebug(
                            "Auto-transition from %s to %s",
                            self._the_state.state,
                            desired_state_msg.desired_state.state,
                        )
                        self._the_state = desired_state_msg.desired_state
                        desired_state_msg = self._the_state.state.run(
                            signal_msg=desired_state_msg,
                            pstate=self._the_state,
                            cur_procs=self._cur_procs,
                            props=self._props,
                        )
                        self._send_pending_replies(desired_state_msg.payload)
                        fdebug(
                            "Completed (return signal %s): %s close=%s",
                            desired_state_msg.signal,
                            self._the_state.state,
                            desired_state_msg.close,
                        )

                    reply = PGSignalMessage(
                        signal=PGSignals.SUCCESS,
                        payload={"current_state": str(self._the_state.state)},
                        p_uid=self._puid,
                        tag=client_msg.tag,
                    )
                except Exception as ex:
                    self._the_state = PGState(state=Error())
                    tb = traceback.format_exc()
                    fdebug("Encountered exception!\n%s\n%s", ex, tb)
                    reply = PGSignalMessage(
                        signal=PGSignals.RAISE_EXCEPTION,
                        p_uid=self._puid,
                        tag=client_msg.tag,
                        payload={"exception": ex, "traceback": tb},
                    )

            else:
                fdebug("Invalid message from %s with signal %s", client_msg.p_uid, client_msg.signal)

            # if our steady state says we're done, set the event
            if desired_state_msg.close or self._the_state.state.state == State.ERROR:
                fdebug("Last state message indicates we should close")
                self._stop_serving.set()

            if self._reply_needed(client_msg, desired_state_msg) or not valid:
                fdebug("sending reply to %s", client_msg.p_uid)
                self._state_outq.put((client_msg.p_uid, reply))
            else:
                fdebug(
                    "did not put reply %s for puid %s in queue (puid %s | skip %s)",
                    client_msg,
                    client_msg.p_uid,
                    self._puid,
                    desired_state_msg.skip_reply,
                )

        finfo("Shutting down (p_uid=%s) in %s with %s", self._puid, self._the_state.state, self._cur_procs)

    def _reply_needed(self, client_msg, desired_state_msg):
        """Decide if a reply is necessary to send to the client"""
        if client_msg.p_uid != self._puid:
            if not desired_state_msg.skip_reply:
                return True
        return False

    def run(self):
        """Run the manager services until shutdown"""
        fdebug, finfo = get_logs("run")

        th = threading.Thread(target=self._state_runner, daemon=False)
        th.start()

        try:
            while not self._stop_serving.is_set():
                try:
                    fdebug("Waiting for client message")
                    ser_msg = self._inq.get(timeout=self._QPATIENCE)
                    fdebug("Received client message")
                except queue.Empty:
                    self._process_resp()
                    continue

                try:
                    msg = dmsg.parse(ser_msg)
                except Exception as ex:
                    self._stop_serving.set()
                    self._abnormal_termination = True
                    fdebug("There was an exception parsing the message:\n%s", ex)
                    continue

                if type(msg) in self._DTBL:
                    self._DTBL[type(msg)][0](self, msg=msg, p_uid=msg.p_uid)
                    fdebug("Finished processing: %s", msg)
                else:
                    self._stop_serving.set()
                    self._abnormal_termination = True
                    fdebug("The message %s is not a valid message!", msg)
                    continue

                self._process_resp()

        except Exception as ex:
            tb = traceback.format_exc()
            fdebug("There was an exception in manager:\n%s\n Traceback:\n%s", ex, tb)

        # note: the state runner thread actually sets the stop_serving event since we really needed to know
        #  that it's safe from its perspective
        th.join()

        # Close the pmix DDict if one was created:
        if self.pmix_dd is not None:
            for guid in self._cur_procs.get_guids():
                fdebug("Cleaning up PMIx resources for guid %s", guid)
                cleanup_pmix_resources(guid)
                fdebug("PMIx resources for guid %s destroyed", guid)

            fdebug("Destroying PMIx dictionary")
            self.pmix_dd.destroy()
            fdebug('PMIx dictionary destroyed"')

        fdebug("detaching from dragon handler")
        dlog.detach_from_dragon_handler(dls.PG)
        fdebug("detached from dragon handler")

    def _tag_inc(self):
        tag = self._tag
        self._tag += 1
        return tag

    def _send_response(self, p_uid, msg: dmsg.PGClientResponse):
        """Send response to requesting client

        :param p_uid: puid of requestions client
        :type p_uid: int
        :param msg: message containing appropriate response to client request
        :type msg: dmsg.PGClientResponse
        """
        fdebug, finfo = get_logs("_send_response")

        try:
            fdebug("Generating reply message to %s: error=%s, ex=%s, payload=%s", p_uid, msg.error, msg.ex, msg.payload)
            if msg.error is not None:
                msg.error = str(int(msg.error))
            if msg.ex is not None:
                msg.ex = str(msg.ex)
            if msg.payload is not None:
                msg.payload = b64encode(cloudpickle.dumps(msg.payload))
            self._clients[p_uid].send(msg.serialize())
        except Exception as ex:
            fdebug("Failed to send response to client:\n%s", ex)

    def _process_resp(self):
        fdebug, finfo = get_logs("_process_resp")

        while True:
            try:
                v = self._state_outq.get(timeout=self._LOCALQPATIENCE)
                p_uid, reply = v
            except queue.Empty:
                return

            fdebug("Preparing reply for p_uid=%s reply=%s", p_uid, reply)
            msg = dmsg.PGClientResponse(self._tag_inc(), error=reply.signal, payload=reply.payload, src_tag=reply.tag)
            self._send_response(p_uid, msg)

    def _derive_nnodes_for_pmix_ddict(self, nprocs):
        """Determine number of nodes to host the PMIx dictionary on"""

        # Get the number of nodes in our runtime
        n_nodes = len(System().nodes)

        # If the number of procs >= nnodes, place one manager on each node
        if n_nodes <= nprocs:
            return n_nodes
        # If it's less, just use nprocs
        else:
            return nprocs

    @dutil.route(dmsg.PGRegisterClient, _DTBL)
    def register_client(self, msg: dmsg.PGRegisterClient, p_uid):
        fdebug, finfo = get_logs("register_client")

        # Note, reregistration is allowed
        fdebug("Processing client registration from p_uid=%s", p_uid)
        try:
            # no ref counting. receiver handles that
            resp_chan = Channel.attach(b64decode(msg.resp_cd))
            self._clients[p_uid] = Connection(outbound_initializer=resp_chan)
            self._clients[p_uid].open()
        except Exception as ex:
            fdebug("Failed to attach to client response Connection (cuid=%s):\n%s", msg.resp_cd, ex)

        msg = dmsg.PGClientResponse(self._tag_inc(), error=PGSignals.SUCCESS, src_tag=msg.tag)
        self._send_response(p_uid, msg)

    @dutil.route(dmsg.PGUnregisterClient, _DTBL)
    def unregister_client(self, msg: dmsg.PGUnregisterClient, p_uid):
        fdebug, finfo = get_logs("unregister_client")

        fdebug("Processing client unregistration from p_uid=%s", p_uid)
        msg = dmsg.PGClientResponse(self._tag_inc(), error=PGSignals.SUCCESS, src_tag=msg.tag)
        self._send_response(p_uid, msg)

        try:
            self._clients[p_uid].close()
            del self._clients[p_uid]
        except Exception as ex:
            fdebug("Failed to detach from client response Connection:\n%s", msg.resp_cd, ex)

    @dutil.route(dmsg.PGSetProperties, _DTBL)
    def set_properties(self, msg: dmsg.PGSetProperties, p_uid):
        fdebug, finfo = get_logs("set_properties")

        fdebug("Processing properties assignment from p_uid=%s", p_uid)
        error = PGSignals.SUCCESS
        ex = None

        # ProcessGroup is immutable for now
        if self._props is None:
            self._props = PGProperties(lock=threading.Lock())
            self._props.from_dict(msg.props)
        else:
            error = PGSignals.RAISE_EXCEPTION
            ex = DragonProcessGroupAlreadyInitialized("Cannot update ProcessGroup already initialized")

        msg = dmsg.PGClientResponse(self._tag_inc(), error=error, ex=ex, src_tag=msg.tag)
        self._send_response(p_uid, msg)

    @dutil.route(dmsg.PGStopRestart, _DTBL)
    def stop_restart(self, msg: dmsg.PGStopRestart, p_uid):
        fdebug, finfo = get_logs("stop_restart")

        fdebug("Processing stop restart from p_uid=%s", msg.p_uid)

        with self._props.lock:
            cur_restart = self._props.restart
        if cur_restart is True:
            msg = PGSignalMessage(signal=PGSignals.STOP_MAINTAINING, p_uid=p_uid, tag=msg.tag)
            self._state_inq.put(msg)
        else:
            msg = dmsg.PGClientResponse(self._tag_inc(), src_tag=msg.tag)
            self._send_response(p_uid, msg)

    @dutil.route(dmsg.PGAddProcessTemplates, _DTBL)
    def add_processes(self, msg: dmsg.PGAddProcessTemplates, p_uid):
        """worth noting for now that this overwrites the templates. leaving the opportunity to later allow live adds"""
        fdebug, finfo = get_logs("add_processes")

        fdebug("Processing process initialization from p_uid %s", p_uid)

        # decode the templates into (nproc, ProcessTemplate)
        p_templates = []
        try:
            for item in msg.templates:
                p_templates.append((item[0], ProcessTemplate.from_sdict(item[1])))
        except Exception as ex:
            msg = dmsg.PGClientResponse(self._tag_inc(), error=PGSignals.RAISE_EXCEPTION, ex=ex, src_tag=msg.tag)
            self._send_response(p_uid, msg)

        # Take a look at the messages, if PMIx is requested, create a ddict. Note: we should
        # only create 1 ddict per manager. Ideally this code is only called once. Otherwise,
        # errors are likely.
        try:
            fdebug("Checking whether we need to make a PMIx ddict. Props: %s", self._props)
            if PMIBackend.from_str(self._props.pmi) == PMIBackend.PMIX:
                from ..data.ddict import DDict

                expanded_templates = []
                for i, (replicas, create_template) in enumerate(p_templates):
                    expanded_templates.extend(replicas * [create_template])

                fdebug("Creating PMIx DDict for %s procs", len(expanded_templates))
                n_nodes = self._derive_nnodes_for_pmix_ddict(len(expanded_templates))
                fdebug("Will run PMIx DDict on %s nodes", n_nodes)
                self.pmix_dd = DDict(
                    managers_per_node=1,
                    n_nodes=n_nodes,
                    total_mem=n_nodes * 128 * 1024 * 1024,
                    wait_for_keys=True,
                    working_set_size=2,
                )
                fdebug("Successfully created PMIx DDict")

                self.pmix_sdd = self.pmix_dd.serialize()
        # In case props.pmi is None
        except ValueError as e:
            fdebug("Value error with PMIx DDict creation: %s", e)
            pass
        except Exception as e:
            fdebug("Other exception: %s", e)
            raise

        msg = PGSignalMessage(
            signal=PGSignals.ADD_TEMPLATES,
            p_uid=p_uid,
            payload={"p_templates": p_templates, "exq": self._exq},
            tag=msg.tag,
        )
        self._state_inq.put(msg)

    @dutil.route(dmsg.PGStart, _DTBL)
    def start(self, msg: dmsg.PGStart, p_uid):
        fdebug, finfo = get_logs("start")

        fdebug("Processing process start from p_uid=%s", p_uid)
        msg = PGSignalMessage(
            signal=PGSignals.START, p_uid=p_uid, payload={"exq": self._exq, "pmix_sdd": self.pmix_sdd}, tag=msg.tag
        )
        fdebug("Putting start signal msg in queue: %s", msg)
        self._state_inq.put(msg)

    @dutil.route(dmsg.PGJoin, _DTBL)
    def join(self, msg: dmsg.PGJoin, p_uid):
        fdebug, finfo = get_logs("join")

        fdebug("Processing join from p_uid=%s with timeout=%s", p_uid, msg.timeout)

        msg = PGSignalMessage(signal=PGSignals.JOIN, p_uid=p_uid, payload={"timeout": msg.timeout}, tag=msg.tag)
        self._state_inq.put(msg)

    @dutil.route(dmsg.PGSignal, _DTBL)
    def signal(self, msg: dmsg.PGSignal, p_uid):
        fdebug, finfo = get_logs("signal")

        fdebug("Processing signal from p_uid=%s (sig=%s)", p_uid, msg.sig)

        # done in this thread as it is non-blocking
        op = Signal()
        try:
            fdebug("Running: %s", op)
            outcome = op.run(
                signal_msg=PGSignalMessage(
                    signal=PGSignals.SIGNAL, payload={"signal": msg.sig, "hide_stderr": msg.hide_stderr}
                ),
                pstate=self._the_state,
                cur_procs=self._cur_procs,
                props=self._props,
            )
            fdebug("Completed: %s", op)
            msg = dmsg.PGClientResponse(self._tag_inc(), error=outcome.signal, payload=outcome.payload, src_tag=msg.tag)
        except Exception as ex:
            msg = dmsg.PGClientResponse(self._tag_inc(), error=PGSignals.RAISE_EXCEPTION, ex=ex, src_tag=msg.tag)

        self._send_response(p_uid, msg)

    @dutil.route(dmsg.PGState, _DTBL)
    def state(self, msg: dmsg.PGState, p_uid):
        fdebug, finfo = get_logs("state")
        fdebug("Processing status query from p_uid=%s", p_uid)

        concrete_states = {State.IDLE, State.RUNNING, State.ERROR}

        op = Query()

        keep_going = True
        while keep_going:
            outcome = op.run(
                signal_msg=PGSignalMessage(signal=PGSignals.STATE),
                pstate=self._the_state,
                cur_procs=self._cur_procs,
                props=self._props,
            )

            if State(outcome.payload["current_state"]) in concrete_states:
                keep_going = False

        fdebug("Completed: %s with results %s", op, outcome)
        msg = dmsg.PGClientResponse(self._tag_inc(), payload=outcome.payload, src_tag=msg.tag)
        self._send_response(p_uid, msg)
        fdebug("sent reponse...")

    @dutil.route(dmsg.PGPuids, _DTBL)
    def get_puids(self, msg: dmsg.PGState, p_uid):
        fdebug, finfo = get_logs("get_puids")
        fdebug("Processing puids query from p_uid=%s", p_uid)

        op = Query()
        outcome = op.run(
            signal_msg=PGSignalMessage(
                signal=PGSignals.PUIDS, payload={"active": msg.active, "inactive": msg.inactive}
            ),
            pstate=self._the_state,
            cur_procs=self._cur_procs,
            props=self._props,
        )
        fdebug("Completed: %s", op)

        msg = dmsg.PGClientResponse(self._tag_inc(), payload=outcome.payload, src_tag=msg.tag)
        self._send_response(p_uid, msg)

    @dutil.route(dmsg.PGStop, _DTBL)
    def stop(self, msg: dmsg.PGStop, p_uid):
        fdebug, finfo = get_logs("stop")

        fdebug("Processing stop from p_uid=%s with patience=%s", p_uid, msg.patience)

        msg = PGSignalMessage(signal=PGSignals.STOP, p_uid=p_uid, payload={"patience": msg.patience}, tag=msg.tag)
        self._state_inq.put(msg)

    @dutil.route(dmsg.PGClose, _DTBL)
    def close(self, msg: dmsg.PGStop, p_uid):
        fdebug, finfo = get_logs("close")

        fdebug("Processing close from p_uid=%s with patience=%s", p_uid, msg.patience)

        msg = PGSignalMessage(signal=PGSignals.CLOSE, p_uid=p_uid, payload={"patience": msg.patience}, tag=msg.tag)
        self._state_inq.put(msg)


def _run_manager(inq: Queue, exq: Queue, name: str):
    mgr = Manager(inq, exq, name)
    mgr.run()


@dataclass(frozen=True)
class TrainingGroupConfig:
    nprocs: int
    ppn: int
    port: int


class ProcessGroup:
    """Object providing API to manage group of Dragon Processes via Dragon Global Services

    This is really a state machine of the associated processes. A typical workflow would resemble:

    .. highlight:: python
    .. code-block:: python

        from dragon.native.process_group import ProcessGroup
        from dragon.native.process import ProcessTemplate

        def hello_world():
            from dragon.infrastructuture.parameters import this_process
            print(f'hello from process {this_process.my_puid}!')

        pg = ProcessGroup()

        template = dragon.native.process.ProcessTemplate(target=hello_world)
        pg.add_process(nproc=4, template=template)

        pg.init()
        pg.start()
        pg.join()  #  If your worker functions won't exit on their own, use pg.stop() to transmit
                   #  interrupt/termination signals
        pg.close()
    """

    _EXCEPTION_Q_PATIENCE = 0.1

    def __init__(
        self,
        restart: bool = False,
        ignore_error_on_exit: bool = False,
        pmi: str = None,
        walltime: float = None,
        policy: Policy = None,
        critical: bool = False,
        name: str = None,
    ):
        """Instantiate a number of managed processes.


        :param restart: if True, restart worker processes that exit unexpectedly and suppress any errors from them, defaults to False
        :type restart: bool, optional
        :param ignore_error_on_exit: If True, ignore worker processe errors as they exit, defaults to False
        :type ignore_error_on_exit: bool, optional
        :param pmi: PMI backend to use for launching MPI applications, defaults to None
        :type pmi: str, Options given in dragon.infrastructure.facts.PMIBackend
        :param walltime: Time in seconds until the processes in the group get killed after they start, defaults to None
        :type walltime: float, optional
        :param policy: determines the placement of the processes, defaults to None
        :type policy: Policy, optional
        :param critical: whether failure of a worker should initiate restart of runtime. Currently unused, defaults to False
        :type critical: bool, optional
        :param name: identification name given to process group, defaults to None
        :type name: str, optional
        """

        # If PMI was requested, check that it's a supported version
        if pmi is not None:
            try:
                _ = PMIBackend.from_str(pmi)
            except ValueError as e:
                raise RuntimeError("Unsupported PMI backend requested") from e

        # TODO: nhill - I don't think a thread lock is necessary on the PGProperties for the client. If it is, I'll
        # need to add a __setstate__ and __getstate__ to ensure it remains picklable.
        self._props = PGProperties(restart, ignore_error_on_exit, pmi, walltime, policy, critical, name)
        self._local_templates = []
        self._registered = False
        self._rank_info = RankInfo()

    def _start_manager(self, inq: Queue, exq: Queue, name: str = "", policy: Policy = None):
        proc = Process(target=_run_manager, args=(inq, exq, name), policy=None)
        proc.start()
        self._mgr_p_uid = proc.puid
        self._update_mgr()

    def _update_mgr(self, expectation: ProcessDescriptor.State = ProcessDescriptor.State.ACTIVE):
        self._mgr = process_query(self._mgr_p_uid)
        if self._mgr.state is not expectation:
            raise RuntimeError(f"ProcessGroup manager is not in state {expectation}")

    @property
    def _mgr_alive(self):
        if self._mgr.state is ProcessDescriptor.State.ACTIVE:
            return True
        return False

    @property
    def _mgr_state(self):
        """Query the manager state"""
        self._mgr = process_query(self._mgr_p_uid)
        return self._mgr.state

    def __setstate__(self, state):
        self._mgrq, self._mgr_p_uid, self._exq = state
        try:
            self._update_mgr()
            self._register()
        except Exception as ex:
            raise RuntimeError("Failed to attach to ProcessGroup") from ex

    def __getstate__(self) -> object:
        try:
            self._mgrq.size()
        except Exception as ex:
            raise ValueError("Cannot pickle ProcessGroup before init() method is called") from ex
        return (self._mgrq, self._mgr_p_uid, self._exq)

    def __enter__(self):
        return self

    def __exit__(self, type, value, tb):
        self._unregister()
        self._cleanup()

    def _tag_inc(self):
        tag = self._tag
        self._tag += 1
        return tag

    def _send_msg(self, msg, ex_str, timeout=None):
        # TODO: should use a lock here in case someone is trying to use the object from multiple threads
        # lock here would do it since it synchronizes req/resp. anything left what a timeout and can be trashed

        try:
            self._mgrq.put(msg.serialize())

            resp = None
            if timeout is None:
                keepgoing = True

                # We need this loop in case a former timeout response appears, which does not match with our
                # expected response
                while keepgoing:
                    resp = self._conn_me.recv()
                    chk_resp = dmsg.parse(resp)
                    if (chk_resp is not None) and (chk_resp.src_tag == msg.tag):
                        keepgoing = False
                    elif PGSignals(int(chk_resp.error)) == PGSignals.INVALID_REQUEST or chk_resp.error is None:
                        keepgoing = False

            else:
                keepgoing = True
                while keepgoing:
                    if self._conn_me.poll(timeout=timeout):
                        ser_resp = self._conn_me.recv()
                        chk_resp = dmsg.parse(ser_resp)
                        # be sure it's the message we expect
                        if (chk_resp is not None) and (chk_resp.src_tag == msg.tag):
                            resp = ser_resp
                            keepgoing = False
                    else:
                        keepgoing = False
                if resp is None:
                    raise TimeoutError()

            msg = dmsg.parse(resp)

            if msg.error is not None:
                msg.error = PGSignals(int(msg.error))
            else:
                msg.error = PGSignals.SUCCESS

            if msg.payload is not None:
                msg.payload = cloudpickle.loads(b64decode(msg.payload))
        except Exception as ex:
            if isinstance(ex, TimeoutError):
                raise TimeoutError()
            raise RuntimeError(f"Failed to parse manager response:\n{ex}")

        if msg.error != PGSignals.SUCCESS:
            try:
                tb = msg.payload["traceback"]
            except KeyError:
                tb = None
            raise DragonProcessGroupError(msg.error, f"{ex_str}:\n{msg.ex}\n{msg.payload}\n{tb}")

        return msg

    def _register(self):
        self._exq_lock = threading.Lock()
        self._exq_shutdown = threading.Event()
        self._ex_raised = False

        self._tag = 0
        self._conn_me, self._conn_mgr = Pipe(duplex=False)
        self._conn_me.open()
        self._conn_mgr.open()
        mgr_cd = b64encode(self._conn_mgr.outbound_chan.serialize())
        msg = dmsg.PGRegisterClient(self._tag_inc(), this_process.my_puid, mgr_cd)

        msg = self._send_msg(msg, "Failed to register with manager")
        self._registered = True

    def _unregister(self):
        if self._registered:
            msg = dmsg.PGUnregisterClient(self._tag_inc(), this_process.my_puid)

            msg = self._send_msg(msg, "Failed to unregister with manager")
            self._conn_me.close()

            self._registered = False

    @staticmethod
    def _query_exception(raised: bool, exception_q: Queue, tlock: threading.Lock):
        """Grab the exception from the queue shared with Manager and replace it back into the Queue for other clients"""

        if raised:
            with tlock:
                ex = exception_q.get()
                exception_q.put(ex)
            return ex
        return None

    def _check_exception(f):
        """Decorator for checking exception status around methods that could raise an error"""

        def _manage_exception(obj):
            ex = obj._query_exception(obj._ex_raised, obj._exq, obj._exq_lock)
            if ex is not None:
                raise ex

        @wraps(f)
        def check_exception(obj, *args, **kwargs):
            _manage_exception(obj)
            x = f(obj, *args, **kwargs)
            _manage_exception(obj)
            return x

        return check_exception

    def add_process(self, nproc: int, template: ProcessTemplate) -> None:
        """Add processes to the ProcessGroup.

        :param nproc: number of Dragon processes to start that follow the provided template
        :type nproc: int
        :param template: single template processes, i.e. unstarted process objects
        :type template: dragon.native.process.ProcessTemplate
        """

        self._local_templates.append((nproc, template))

    def init(self) -> None:
        """Initialize the ProcessGroupState and Manager."""
        self._mgrq = Queue()
        self._exq = Queue()
        self._start_manager(self._mgrq, self._exq, self._props.name)
        self._register()

        msg = dmsg.PGSetProperties(self._tag_inc(), this_process.my_puid, asdict(self._props))

        msg = self._send_msg(msg, "Failed to send new properties to Manager")

        # make the templates into a simple list that can be JSON encoded via Dragon messaging
        msg = []
        for nproc, tmpl in self._local_templates:
            msg.append([nproc, tmpl.sdesc])

        msg = dmsg.PGAddProcessTemplates(self._tag_inc(), this_process.my_puid, msg)

        msg = self._send_msg(msg, "Failed to send process template list to Manager")

    def start(self) -> None:
        """Starts all processes according to the template. If `restart ==
        False`, transition to 'Running', otherwise transition to 'Maintain'.
        """

        if not self._mgr_alive:
            raise DragonProcessGroupRunningError(
                DragonError.INVALID_OPERATION, "Cannot execute a ProcessGroup without Manager initialization"
            )

        msg = dmsg.PGStart(self._tag_inc(), this_process.my_puid)
        msg = self._send_msg(msg, "Failed to tell Manager to start processes")

        # If we were told to register exceptions, set a background join thread
        if not self._props.ignore_error_on_exit:
            self.exq_th = threading.Thread(target=self._exq_monitor, daemon=False)
            self.exq_th.start()

    def _exq_monitor(self) -> None:
        """Sits on a join and parses the exception queue"""

        # Check the exception queue
        while not self._exq_shutdown.is_set():
            if self._exq.poll(timeout=self._EXCEPTION_Q_PATIENCE):
                with self._exq_lock:
                    ex = self._exq.get()
                    self._exq.put(ex)

                if isinstance(ex, Exception):
                    # Exit Loop to indicate to Manager that
                    # there was a failure in user-provided code.
                    # Setting this is necessary so when requested,
                    # the ProcessGroup surfaces the exception.
                    self._ex_raised = True
                    return

    def _cleanup(self):
        try:
            self._exq_shutdown.set()
            self._exq_th.join()
        except AttributeError:
            pass

    def _join_no_decorator(self, timeout: float = None):
        """Wait for all processes to complete and the group to transition to
        Idle state. If the group status is 'Maintain', transition to 'Running' first

        Raises TimeoutError, if the timeout occurred.

        :param timeout: Timeout in seconds, optional defaults to None
        :type timeout: float
        """
        if not self._mgr_alive:
            raise DragonProcessGroupRunningError(
                DragonError.INVALID_OPERATION, "Cannot excecute a ProcessGroup without Manager initialization"
            )

        tag = self._tag_inc()
        msg = dmsg.PGJoin(tag, this_process.my_puid, timeout)
        msg = self._send_msg(msg, "Failed to join with processes", timeout=timeout)

    @_check_exception
    def join(self, timeout: float = None):
        """Wait for all processes to complete and the group to transition to
        Idle state. If the group status is 'Maintain', transition to 'Running' first

        Raises TimeoutError, if the timeout occurred.

        :param timeout: Timeout in seconds, optional defaults to None
        :type timeout: float
        """

        self._join_no_decorator(timeout=timeout)

    @_check_exception
    def send_signal(self, sig: signal.Signals, hide_stderr: bool = False) -> None:
        """Send the given Linux signal to all processes in the process group

        :param sig: Linux signal to send to processes
        :type signal.Signals: example is signal.SIGINT
        """
        if not self._mgr_alive:
            raise DragonProcessGroupRunningError(
                DragonError.INVALID_OPERATION, "Cannot excecute a ProcessGroup without Manager initialization"
            )

        msg = dmsg.PGSignal(self._tag_inc(), this_process.my_puid, sig, hide_stderr)
        msg = self._send_msg(msg, "Failed to signal processes")

    @_check_exception
    def terminate(self, hide_stderr: bool = False) -> None:
        """Send signal.SIGTERM to all processes and optionally maintain exit codes"""
        if not self._mgr_alive:
            raise DragonProcessGroupRunningError(
                DragonError.INVALID_OPERATION, "Cannot excecute a ProcessGroup without Manager initialization"
            )

        cur_restart = self._props.restart
        if cur_restart:
            self.stop_restart()

        msg = dmsg.PGSignal(self._tag_inc(), this_process.my_puid, signal.SIGTERM, hide_stderr)
        msg = self._send_msg(msg, "Failed to signal processes with SIGTERM")

    def _kill_no_decorator(self, hide_stderr: bool = False) -> None:
        """Send signal.SIGKILL to all processes and optionally maintain exit codes"""
        if not self._mgr_alive:
            raise DragonProcessGroupRunningError(
                DragonError.INVALID_OPERATION, "Cannot excecute a ProcessGroup without Manager initialization"
            )

        cur_restart = self._props.restart
        if cur_restart:
            self.stop_restart()

        msg = dmsg.PGSignal(self._tag_inc(), this_process.my_puid, signal.SIGKILL, hide_stderr)
        msg = self._send_msg(msg, "Failed to signal processes with SIGKILL")

    @_check_exception
    def kill(self, hide_stderr: bool = False) -> None:
        """Send signal.SIGKILL to all processes and optionally maintain exit codes"""
        self._kill_no_decorator(hide_stderr)

    def _stop_no_decorator(self, patience: float = 5.0) -> None:
        """Forcibly terminate all workers by sending signal.SIGINT, then signal.SIGTERM, then signal.SIGKILL, with
        `patience` seconds between them waiting for all processes to exit. The ProcessGroup will transition to `Stop`.
        This also removes the group from the manager process and marks the end of the group life-cycle.

        :param patience: Number of seconds to wait between successive signals are sent to bring down processes
        :type float: defaults to 5 seconds
        """
        if not self._mgr_alive:
            raise DragonProcessGroupRunningError(
                DragonError.INVALID_OPERATION, "Cannot excecute a ProcessGroup without Manager initialization"
            )
        cur_restart = self._props.restart
        if cur_restart:
            self.stop_restart()

        msg = dmsg.PGStop(self._tag_inc(), this_process.my_puid, patience)
        msg = self._send_msg(msg, "Failed to stop processes")

    @_check_exception
    def stop(self, patience: float = 5.0) -> None:
        """Forcibly terminate all workers by sending signal.SIGINT, then signal.SIGTERM, then signal.SIGKILL, with
        `patience` seconds between them waiting for all processes to exit. The ProcessGroup will transition to `Stop`.
        This also removes the group from the manager process and marks the end of the group life-cycle.

        :param patience: Number of seconds to wait between successive signals are sent to bring down processes
        :type float: defaults to 5 seconds
        """

        self._stop_no_decorator(patience=patience)

    def _close_no_decorator(self, patience: float = 5.0) -> None:
        """Ensure the underlying process group is down, use stop methodology if not, and instruct the manager
        to exit
        """
        if not self._mgr_alive:
            raise DragonProcessGroupRunningError(
                DragonError.INVALID_OPERATION, "Cannot excecute a ProcessGroup without Manager initialization"
            )

        cur_restart = self._props.restart
        if cur_restart:
            self.stop_restart()

        msg = dmsg.PGClose(self._tag_inc(), this_process.my_puid, patience)

        # Do this with a timeout. If it returns, check on the manager. If there was a previous error
        # state, we may not have a manager to communciate with
        close_done = False
        while not close_done:
            try:
                msg = self._send_msg(msg, "Failed to close the ProcessGroup", timeout=1)
                close_done = True
            except TimeoutError:
                try:
                    self._update_mgr(expectation=ProcessDescriptor.State.ACTIVE)
                except RuntimeError:
                    close_done = True

        self._cleanup()
        self._registered = False  # we cannot unregister from a manager who is gone!

        # check that the manager has gone down
        process_join(self._mgr_p_uid, timeout=patience)
        self._update_mgr(expectation=ProcessDescriptor.State.DEAD)

        try:
            self._conn_me.close()
            self._conn_mgr.close()
        except Exception:
            pass

    @_check_exception
    def close(self, patience: float = 5.0) -> None:
        """Ensure the underlying process group is down, use stop methodology if not, and instruct the manager
        to exit

        :param patience: time to wait for group to come down, including the Manager, defaults to 5.0
        :type patience: float, optional
        """
        self._close_no_decorator(patience=patience)

    @_check_exception
    def stop_restart(self) -> None:
        """Tell the Manager to cease restarting of workers, if ProcessGroup was initialized with restart == True

        :raises: DragonProcessGroupRunningError
        """
        if not self._mgr_alive:
            raise DragonProcessGroupRunningError(
                DragonError.INVALID_OPERATION, "Cannot excecute a ProcessGroup without Manager initialization"
            )

        msg = dmsg.PGStopRestart(self._tag_inc(), this_process.my_puid)
        msg = self._send_msg(msg, "Failed to stop restart")

    @property
    @_check_exception
    def puids(self) -> list[int]:
        """Return the currently executiing puids of processes contained in this group.

        :return: a list of puids
        :rtype: list[int]
        """
        if not self._mgr_alive:
            raise DragonProcessGroupRunningError(
                DragonError.INVALID_OPERATION, "Cannot excecute a ProcessGroup without Manager initialization"
            )

        msg = dmsg.PGPuids(self._tag_inc(), this_process.my_puid, active=True, inactive=False)
        reply = self._send_msg(msg, "Failed to query active puids of the process group")
        return reply.payload["active"]

    @property
    def inactive_puids(self) -> List[Tuple[int, int]]:
        """Return the group's puids and their exit codes that have exited

        :returns: a list of tuples (puid, exit_code)
        :rtype: List[Tuple[int, int]]
        """

        if not self._mgr_alive:
            raise DragonProcessGroupRunningError(
                DragonError.INVALID_OPERATION, "Cannot excecute a ProcessGroup without Manager initialization"
            )

        msg = dmsg.PGPuids(self._tag_inc(), this_process.my_puid, active=False, inactive=True)
        reply = self._send_msg(msg, "Failed to query inactive puids of the process group")
        return reply.payload["inactive"]

    @property
    @_check_exception
    def exit_status(self) -> List[Tuple[int, int]]:
        """Return the group's puids and their exit codes that have exited

        :returns: a list of tuples (puid, exit_code)
        :rtype: List[Tuple[int, int]]
        """
        return self.inactive_puids

    @property
    @_check_exception
    def in_error_state(self) -> bool:
        """Whether a worker has raised an exception

        :returns: True if exception occurred. False otherwise.
        :rtype: {bool}
        """
        try:
            return self._ex_raised
        except AttributeError:
            return False

    @property
    @_check_exception
    def _state(self) -> str:
        """Get the current status of the process group handled by this instance.

        :returns: current status of the group
        :rtype: str
        """
        if not self._mgr_alive:
            raise DragonProcessGroupRunningError(
                DragonError.INVALID_OPERATION, "Cannot excecute a ProcessGroup without Manager initialization"
            )

        msg = dmsg.PGState(self._tag_inc(), this_process.my_puid)
        reply = self._send_msg(msg, "Failed to query state of the process group")
        return State(reply.payload["current_state"])

    def make_ai_training_env(
        self,
        rank: int,
        local_rank: int,
        node_rank: int,
        master_addr: str,
        master_port: str,
        world_size: int,
        local_world_size: int,
    ) -> dict:
        env = dict(os.environ).copy()
        env["MASTER_ADDR"] = master_addr
        env["MASTER_PORT"] = master_port
        env["DRAGON_PG_RANK"] = str(rank)
        env["DRAGON_PG_LOCAL_RANK"] = str(local_rank)
        env["DRAGON_PG_WORLD_SIZE"] = str(world_size)
        env["DRAGON_PG_LOCAL_WORLD_SIZE"] = str(local_world_size)
        env["DRAGON_PG_GROUP_RANK"] = str(node_rank)

        return env

    @classmethod
    def configure_training_group(
        cls,
        *,
        training_fn,
        training_args: tuple = None,
        training_kwargs: dict = None,
        ppn: int = None,
        nprocs: int = None,
        hide_stderr: bool = False,
        port: int = 29500,
        policies: list = None,
    ) -> "ProcessGroup":
        """
        Configure and return a ProcessGroup suitable for distributed training jobs. This helper sets up environment variables and process templates necessary for a training job (like PyTorch DDP) over multiple nodes using NCCL or similar backends.
        Users can specify the group in two ways by either specifying processes per node, total processes or by providing a list of policies.

        :param training_fn: The target function to run on each distributed process.
        :type training_fn: callable
        :param training_args: Positional arguments to pass to training_fn, defaults to None
        :type training_args: tuple, optional
        :param training_kwargs: Keyword arguments to pass to training_fn, defaults to None
        :type training_kwargs: dict, optional
        :param ppn: Number of processes to run per node. Required if policies is not provided, defaults to None
        :type ppn: int, optional
        :param nprocs: Total number of processes. Required if policies is not provided. Ignored if policies is a list, defaults to None
        :type nprocs: int, optional
        :param hide_stderr: If True, suppress standard error from the launched processes, defaults to False
        :type hide_stderr: bool, optional
        :param port: Master port for NCCL backend communication, defaults to 29500
        :type port: int, optional
        :param policies: List of Policy objects or a single Policy.
        :type policies: list or Policy, optional
        :return: A configured and initialized process group for distributed training
        :rtype: ProcessGroup

        """

        if not ((ppn and nprocs) or policies):
            raise ValueError("Must provide both ppn and nprocs, or a list of policies.")

        if isinstance(policies, list):
            if not policies:
                raise ValueError("Policies list cannot be empty.")
            nprocs = len(policies)

        try:
            my_alloc = System()
            node_list = my_alloc.nodes
        except Exception as e:
            print(f"Exception while querying system: {e}", flush=True)
            raise

        nnodes = len(node_list)

        if policies is None:
            if ppn <= 0 or nprocs <= 0:
                raise ValueError("Both ppn and nprocs must be > 0.")

            if nnodes == 0:
                raise RuntimeError("No nodes found in allocation.")

            if ppn * nnodes < nprocs:
                raise RuntimeError(f"Cannot allocate {nprocs} processes on {nnodes} nodes with only {ppn} ppn.")

            policy_list = [
                Policy(placement=Policy.Placement.HOST_NAME, host_name=Node(node).hostname)
                for node in node_list
                for _ in range(ppn)
            ][:nprocs]

        elif isinstance(policies, Policy):
            if nprocs <= 0:
                raise ValueError("nprocs must be > 0 when using a single Policy.")
            policy_list = [policies] * nprocs
        elif isinstance(policies, list):
            if len(policies) != nprocs:
                nprocs = len(policies)
            policy_list = policies
        else:
            raise TypeError("policies must be None, a single Policy, or a list of Policy objects.")

        pg = ProcessGroup(restart=False, pmi=None)
        pg._nccl_config = TrainingGroupConfig(nprocs=nprocs, ppn=ppn, port=port)
        master_node = policy_list[0].host_name
        master_port = str(port)
        stderr = Popen.DEVNULL if hide_stderr else None

        try:
            for rank in range(nprocs):
                node_rank = rank // ppn
                local_rank = rank % ppn
                policy = policy_list[rank]

                env = pg.make_ai_training_env(
                    rank=rank,
                    local_rank=local_rank,
                    node_rank=node_rank,
                    master_addr=master_node,
                    master_port=master_port,
                    world_size=nprocs,
                    local_world_size=ppn,
                )

                template = ProcessTemplate(
                    target=training_fn,
                    args=training_args,
                    kwargs=training_kwargs,
                    env=env,
                    policy=policy,
                    stderr=stderr,
                )

                pg.add_process(nproc=1, template=template)

        except Exception as e:
            print(f"Exception during ProcessTemplate setup: {e}", flush=True)
            raise

        return pg

    @property
    def nccl_config(self):
        return getattr(self, "_nccl_config", None)
