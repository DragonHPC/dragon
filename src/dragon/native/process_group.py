"""The Dragon native class for managing the life-cycle of a group of Dragon processes.

The intent is for the class to be agnostic about what the processes are doing,
it only maintains their lifecycle.

This file implements a client API class and a Manager process handling all
groups on the node. The manager holds a list of GroupContext classes and gets
signals from the client classes using a queue. The group of processes undergoes
state transitions depending on the signals the client send to the manager.

Due to transparency constraints the manager cannot send information back to the client.
Instead we are using shared state

The underlying state machine looks like this:

.. image:: ./images/dragon_worker_pool.svg
    :scale: 75%
"""


import logging
import time
import enum
import signal
from queue import Empty

from abc import ABC, abstractmethod

import dragon

from .process import Process, TemplateProcess
from .queue import Queue
from .value import Value
from .lock import Lock
from .machine import current as current_node

from ..globalservices.process import multi_join, kill as process_kill, get_create_message, get_create_message_with_argdata, query as process_query
from ..globalservices.group import create, kill as group_kill, add_to, create_add_to, remove_from, destroy
from ..infrastructure.policy import Policy

LOG = logging.getLogger(__name__)

# exit code returned by cython for sigterm
# we also mod by 256 for unsigned char repr
CYTHON_SIGTERM_ECODE = -15


class DragonProcessGroupError(Exception):
    """Exceptions raised by the Dragon Pool Workers implementation"""

    pass


class DragonProcessGroupAbnormalExit(DragonProcessGroupError):
    """Exception raised by the Dragon Pool Workers implementation"""

    pass


# TODO: Extend API to control distribution over multiple nodes.


@enum.unique
class PGSignals(enum.IntEnum):
    ERROR = enum.auto()  # for testing purposes
    NEW = enum.auto()  # Start manager but no workers
    START = enum.auto()  # start all processes/workers
    JOIN = enum.auto()  # Wait for all the processes to complete
    SHUTDOWN = enum.auto()  # stop all processes/workers via SIGTERM
    KILL = enum.auto()  # forcefully stop all processes/workers via SIGKILL
    STOP = enum.auto()  # kill all Dragon processes and exit the manager


class BaseState(ABC):
    """This class declares methods that all concrete State classes should
    implement and also provides a backreference to the Context object,
    associated with the State. This backreference can be used by States to
    transition the Context to another State.
    It also defines common methods and data structures to all states.
    """

    forbidden: list[int] = None

    @property
    def context(self):
        """Link back to it's own context.

        :return: The group context holding this state
        :rtype: GroupContext
        """

        return self._context

    @context.setter
    def context(self, new_context) -> None:
        self._context = new_context

    @abstractmethod
    def run(self, prior_state, signal: PGSignals, sig_id: int) -> None:
        """This method runs the state it is a part of on the context."""
        pass

    def __str__(self):
        return self.__class__.__name__


# concrete states of the process group


class Error(BaseState):
    """This is the fallback state if an issue with the group occurs.
    The state of the processes is undefined here.
    """

    forbidden: list = [s for s in PGSignals if s not in [PGSignals.KILL, PGSignals.STOP]]

    def run(self, prior_state: BaseState, signal: PGSignals, sig_id: int) -> None:

        try:
            LOG.error(f"Process Group {self.context} is in error state.")
        except Exception:
            pass

        time.sleep(self.context.update_interval_sec * 10)


class Idle(BaseState):
    """This state kills existing processes and does nothing otherwise."""

    forbidden = [PGSignals.SHUTDOWN, PGSignals.KILL]

    def run(self, prior_state: BaseState, signal: PGSignals, sig_id: int) -> None:
        """The idle state just does nothing except making sure all processes are gone."""

        if self.context.guid:
            group_kill(self.context.guid)
            self.context.guid = self.context._group_descr = None

        self._start_time = None


class Maintain(BaseState):
    """This state starts missing processes and restarts processes that are not
    alive anymore.

    :raises DragonProcessGroupError: If one of the processes could not be (re)started.
    """

    forbidden = [PGSignals.NEW, PGSignals.START]

    def run(self, prior_state: BaseState, signal: PGSignals, sig_id: int) -> None:

        nretries = self.context.num_restart_retries

        if not self.context.guid:
            self.context._start_group_once()

        guid = self.context.guid

        group_descr = None
        for i, lst in enumerate(self.context._group_descr.sets):
            for descr in lst:
                puid = descr.uid
                # check the status of this process by asking GS
                proc_desc = process_query(puid)
                if proc_desc.state == descr.desc.State.ACTIVE:
                    continue

                # remove process from group
                group_descr = remove_from(guid, [puid])

                # create a new one
                msg = self.context.messages[self.context.puid_to_message_map[puid]]
                nrestarts = 0

                while nrestarts < nretries:
                    try:
                        group_descr = create_add_to(guid, [(1, msg.serialize())], self.context.policy)
                        # update the puid_to_message_map dict with the newly added process
                        puids = [descr.uid for lst in group_descr.sets for descr in lst]
                        for new_puid in puids:
                            if new_puid not in self.context.puid_to_message_map:
                                self.context.puid_to_message_map[new_puid] = self.context.puid_to_message_map[puid]
                                # since we added only one process, we can safely assume that we found it
                                break
                        break
                    except Exception as e:
                        nrestarts += 1

                if nrestarts == nretries:
                    raise DragonProcessGroupError(f"Unable to start process {i} using message {msg}.")

        # we need to update the group descriptor after all the above additions/removals
        if group_descr is not None:
            self.context._group_descr = group_descr

class Running(BaseState):

    # user needs to wait for group to become IDLE
    forbidden = [
        s for s in PGSignals if s not in [PGSignals.ERROR, PGSignals.KILL, PGSignals.SHUTDOWN, PGSignals.JOIN]
    ]

    def run(self, prior_state: BaseState, pgsignal: PGSignals, sig_id: int) -> None:

        # this is more complicated as it needs to be, because we're using
        # process objects and multi_join wants puids.

        timeout = self.context.update_interval_sec
        ignore_err = self.context.ignore_error_on_exit

        if prior_state == Idle:  # if we started with restart == False from Idle
            self.context._start_group_once()
            self.context.update_status(sig_id)

        if pgsignal == PGSignals.SHUTDOWN:  # have processes exit
            group_kill(self.context.guid, sig=signal.SIGTERM)

        # collect puids
        puids = self.context.puids

        # join on them
        ready = multi_join(puids, join_all=True, timeout=timeout)

        if ready[0] != None:  # no timeout

            # catch bad ecodes
            if not ignore_err:
                for puid, ecode in ready[0]:
                    if ecode not in {0, CYTHON_SIGTERM_ECODE, CYTHON_SIGTERM_ECODE % 256}:
                        LOG.debug(f"Bad exit code {ecode} for puid {puid} in ProcessGroup")
                        raise DragonProcessGroupAbnormalExit("Some processes in group exited abnormally !")

            # move on to Idle
            prior_state = self.context.transition_to(Idle)
            self.context.run(prior_state, pgsignal, sig_id)
            self.context.update_status(None)

class Stop(BaseState):
    """Stops all processes of the group and removes the group from the Manager. The
    group cannot be restarted anymore.
    """

    forbidden = [s for s in PGSignals]  # end of line

    def run(self, prior_state: BaseState, signal: PGSignals, sig_id: int) -> None:

        if self.context.guid != None:
            destroy(self.context.guid)
        self.context.guid = self.context._group_descr = None

# end concrete state classes

class GroupContext:
    """The Context defines the group interface for the manager and the client.
    In particular, it handles signals and state changes. It maintains a
    reference to an instance of a State subclass, which represents the current
    state of the group of processes.
    """

    _state: BaseState = None

    update_interval_sec: float = 0.5
    num_kill_retries: int = 2
    num_restart_retries: int = 2

    # why have more than 1 target status ?
    # because JOIN transitions to Join, and then auto-transitions to Idle.
    # so the target state for the manager is Join, but the client has to
    # wait for Signal completion on Join and Idle to not introduce a race condition.
    # The same is true for SHUTDOWN, which is JOIN with a SIGTERM.

    target_states: dict = {
        PGSignals.ERROR: [Stop],
        PGSignals.NEW: [Idle],
        PGSignals.START: [Maintain],
        PGSignals.JOIN: [Running, Idle],
        PGSignals.SHUTDOWN: [Running, Idle],
        PGSignals.KILL: [Idle],
        PGSignals.STOP: [Stop],
    }

    forbidden: dict = {
        str(Error()): Error.forbidden,
        str(Idle()): Idle.forbidden,
        str(Maintain()): Maintain.forbidden,
        str(Running()): Running.forbidden,
        str(Stop()): Stop.forbidden,
    }

    def __init__(
        self,
        templates: list[tuple],
        nproc: int,
        restart: bool,
        ignore_error_on_exit: bool,
        pmi_enabled: bool,  # MPI jobs
        walltime: float,
        policy: Policy,
    ) -> None:
        """This class represents a group of processes and exposes an interface
        to the Manager to handle signals and state transitions.

        :param templates: a list of tuples where each tuple contains a replication factor `n` and a Dragon TemplateProcess object specifing the properties of the process to start. The processes can hold the same or different attributes in any way and will be numbered in order.
        :type templates: list[tuple(int, dragon.native.process.TemplateProcess),]
        :param nproc: total number of processes that belong to the ProcessGroup
        :type nproc: int
        :param restart: wether to restart processes that exited prematurely, defaults to True
        :type restart: bool
        :param ignore_error_on_exit: whether to ignore errors when the group exists, defaults to False
        :type ignore_error_on_exit: bool
        :param pmi_enabled: wether to instruct Dragon to enable MPI support, defaults to False
        :type pmi_enabled: bool
        :param walltime: time in seconds to run processes before killing them.
        :type walltime: float
        :param policy: determines the placement of the group resources
        :type policy: dragon.infrastructure.policy.Policy
        """

        self.nproc = nproc
        self.templates = templates # list of tuples
        self.messages = {} # keys are the indices of tuples in self.templates

        # use a dict to make restarting easy and order-safe
        self.puid_to_message_map = {} # keys are the puids, values are the keys in self.messages{}

        for i, tup in enumerate(templates):
            t = tup[1]
            if t.is_python:
                self.messages[i] = get_create_message_with_argdata(t.target, t.cwd, t.args, t.env, t.argdata, pmi_required=pmi_enabled, stdin=t.stdin, stdout=t.stdout, stderr=t.stderr)
            else:
                self.messages[i] = get_create_message(t.target, t.cwd, t.args, t.env, pmi_required=pmi_enabled, stdin=t.stdin, stdout=t.stdout, stderr=t.stderr)

        self.guid = None
        self._group_descr = None

        self.restart = restart
        self.ignore_error_on_exit = ignore_error_on_exit
        self.pmi_enabled = pmi_enabled
        self.walltime = walltime
        if policy:
            self.policy = policy
        else:
            self.policy = Policy()

        self._start_time = 0

        self._state = None

        # count signals so we can report completion
        self._signal_counter = Value("i", value=0)
        self._signal_counter_lock = Lock()

        self._status_queue = Queue()
        self._status_queue.put((-1, -1, -1))

        self.transition_to(Idle)
        self.update_status(None)

    def __del__(self):

        try:
            self._status_queue.put(True)
        except Exception:
            pass

    def handle_signal(self, signal: PGSignals) -> tuple:
        """This method takes a signal, checks if the signal is allowed, given
        the current state, and returns the new state.
        If the walltime is up, it overwrites the signal with kill.

        :param signal: The signal to consider
        :type signal: Signal
        :return: target state
        :rtype: BaseState
        :raises RuntimeError: raised, if the transition is not allowed
        """

        if signal in self._state.forbidden:
            LOG.error(f"Signal not accepted {signal} for state {self._state} !")
            return

        if self._walltime_expired():
            signal = PGSignals.KILL  # goodbye

        if signal:
            next_state = self.target_states[signal][0]
        else:
            next_state = self.current_state

        return next_state, self.current_state

    def transition_to(self, new_state: BaseState) -> BaseState:
        """Transition the context to a new state. This does NOT run
        the new state or updates the status.

        :param new_state: state to transition to
        :type new_state: BaseState
        :return: previous state
        :rtype: BaseState
        """

        prior_state = self._state

        self._state = new_state
        self._state.context = self

        return prior_state

    @property
    def current_state(self) -> BaseState:
        """Return the current state object.

        :return: the state
        :rtype: BaseState
        """

        return self._state

    def run(self, prior_state: BaseState, signal: PGSignals, sig_id: int):
        """Execute the current (!) state in self._state.

        :param prior_state: prior state
        :type prior_state: BaseState
        """

        try:
            self._state.run(self._state, prior_state, signal, sig_id)  # Why ?
        except Exception as e:  # automagically transition to Error if something goes wrong
            LOG.error(f"Exception in transition {prior_state} -> {self._state}: {e} ")
            failed_state = self._state
            self.transition_to(Error)
            self.update_status(None)
            self.run(self._state, None, sig_id)

    def get_next_sig_id(self) -> int:
        """Obtain the next unique signal ID for this group context

        :return: next signal id
        :rtype: int
        """

        # we need to communicate if a signal completed, so we need an ID for it.

        with self._signal_counter_lock:
            sig_id = self._signal_counter.value
            self._signal_counter.value = 1 + self._signal_counter.value

        return sig_id

    @property
    def status(self) -> str:
        """The current status of the Group.

        :return: state name
        :rtype: str
        """

        item = self._status_queue.get(timeout=None)
        self._status_queue.put(item)
        state_name, _, __ = item

        return state_name

    @property
    def puids(self) -> list[int or None]:
        """The puids maintained by the GroupContext.

        :return: A list of puids if the process is started or None if not.
        :rtype: list[int or None]
        """

        item = self._status_queue.get(timeout=None)
        self._status_queue.put(item)
        _, puids, __ = item

        return puids

    @property
    def last_completed_signal(self) -> int:
        """Return the ID of the last successfully completed signal.

        :return: the signal id
        :rtype: int
        """

        item = self._status_queue.get(timeout=None)
        self._status_queue.put(item)
        _, __, compl_sig_id = item

        return compl_sig_id

    def _start_group_once(self):
        if not self._start_time:
            self._start_time = time.monotonic()

        group_descr = create([ (tup[0], self.messages[i].serialize()) for i, tup in enumerate(self.templates) ], self.policy)

        self._group_descr = group_descr
        self.guid = group_descr.g_uid

        # construct the puid_to_message_map dict now that the processes are created
        puids = [descr.uid for lst in group_descr.sets for descr in lst]
        puid_idx = 0
        for i, tup in enumerate(self.templates):
            for _ in range(tup[0]):
                self.puid_to_message_map[puids[puid_idx]] = i
                puid_idx += 1

    def _walltime_expired(self) -> bool:
        """Check if the walltime has expired

        :return: True if the walltime has expired, False otherwise
        :rtype: bool
        """

        if self.walltime and self._start_time > 0:
            expired = time.monotonic() - self._start_time >= self.walltime
        else:
            expired = False

        return expired

    # this is a hack using a queue to communicate the status. We would want to
    # use a few Array objects and a single lock to share the puids and status
    # string.  I need to report the last completed signal ID here, so that the
    # corresponding client can be be sure his request has been completed.

    def update_status(self, compl_sig_id: int):
        """update the global status of this context.

        :param compl_sig_id: signal just completed. If None, reuse the last value from the queue.
        :type compl_sig_id: int
        """

        _, __, last_sig_id = self._status_queue.get(timeout=None)

        if compl_sig_id == None:  # this transition was automatic
            compl_sig_id = last_sig_id

        state_name = str(self._state.__name__)

        if self._group_descr:
            puids = [descr.uid for lst in self._group_descr.sets for descr in lst]
        else:
            puids = [None for _ in range(self.nproc)]

        item = (state_name, puids, compl_sig_id)
        self._status_queue.put(item)


class Manager:
    def __init__(self):
        """The Manager class holds a process that handles the life-cycle of all
        process groups on this node.
        We handle the group using a group_context class that holds all necessary
        methods and attributes shared by manager and clients.
        """

        # I am not yet using a single node-wide process here, because I need to
        # discover it and its communication queue, but I cannot ask Dragon for that
        # named queue yet.

        node = current_node()

        # missing shared memory implementation, so I am using a Queue
        self.group_context = None

        ident = f"_DPoolQ1-{node.h_uid}-{id(self)}"
        self.queue = Queue()  # ident=ident) # need a named queue here

        ident = f"_DPoolMan-{node.h_uid}-{id(self)}"
        self._proc = Process(
            self._signal_handler,
            (self.queue,),
            # ident=ident,
        )
        self._proc.start()

    def __del__(self):

        # Kill processes and remove context
        try:
            self.queue.put((PGSignals.STOP, None))
        except Exception:
            pass

        # wait for manager to exit
        try:
            self._proc.join(timeout=self.update_interval_sec)
        except Exception:
            pass

        # make manager exit
        try:
            self._proc.kill()  # please go away
        except Exception:
            pass

    def _signal_handler(self, queue: Queue):
        """Get a new signal from the queue, obtain the new state, transition the
        context, run the new context, update the status.
        If the queue is empty, run the current context again.
        """

        signal = None

        while signal != PGSignals.STOP:

            try:
                signal, payload, sig_id = queue.get(timeout=0)
            except Empty:
                signal = payload = sig_id = None  # no change

            if signal == PGSignals.NEW and payload:  # new context !
                self.group_context = payload

            # this looks overengineered, but I need to cover Join :
            # * Join.run depends on the prior state (Idle or Maintain), need to save it
            # * transitioning is not followed by a run for Join->Idle
            # * update_status has to be run multiple times by a Join
            # * we update the status without running the state in Join
            if self.group_context:
                new_state, prior_state = self.group_context.handle_signal(signal)
                self.group_context.transition_to(new_state)
                self.group_context.run(prior_state, signal, sig_id)
                self.group_context.update_status(sig_id)

                time.sleep(self.group_context.update_interval_sec)  # do not spin too hot

        self.group_context = None


class ProcessGroup:
    """Robustly manage the lifecycle of a group of Dragon processes using Dragon Global Services.

    This is really a state machine of the underlying processes. We should always
    be able to "ask" the manager process for the state of the group and send it a
    signal to make a state transition.
    """

    def __init__(
        self,
        restart: bool = True,
        ignore_error_on_exit: bool = False,
        pmi_enabled: bool = False,
        walltime: float = None,
        policy: Policy = None
    ):
        """Instantiate a number of Dragon processes.

        :param restart: if True, restart worker processes that exit unexpectedly and suppress any errors from them, defaults to True.
        :type restart: bool
        :param ignore_error_on_exit: If to ignore errors coming from processes when they exit from Join state.
        :type ignore_error_on_exit: bool
        :param flags: optional flags that affect the handling of a worker process.
        :type flags: Worker.Flags
        :param pmi_enabled: Instruct the runtime to setup the environment so that the binary can use MPI for inter-process communication.
        :type pmi_enabled: Bool
        :param walltime: Time in seconds until the processes in the group get killed
        :type walltime: float
        :param policy: determines the placement of the processes
        :type policy: dragon.infrastructure.policy.Policy
        """

        self.templates = [] # this will be a list of tuples that will be sent to the GSGroup API
        self.nproc = 0
        self.restart = restart
        self.ignore_error_on_exit = ignore_error_on_exit
        self.pmi_enabled = pmi_enabled
        self.walltime = walltime
        self.policy = policy
        self._group_context = None

    def add_process(self, nproc: int, template: TemplateProcess) -> None:
        """Add processes to the ProcessGroup.

        :param template: single template processes, i.e. unstarted process objects
        :type template: dragon.native.process.TemplateProcess
        :param nproc: number of Dragon processes to start that follow the provided template
        :type nproc: int
        """

        # if add_process is called after the ProcessGroup is initialized, then we raise
        if self._group_context:
            raise DragonProcessGroupError("You cannot call add_process() to already initialized ProcessGroup. Please use ProcessGroup.create_add_to() instead to add more processes.")

        self.templates.append((nproc, template))
        self.nproc += nproc

    def init(self) -> None:
        """Initialize the GroupContext and Manager."""

        self._group_context = GroupContext(self.templates, self.nproc, self.restart, self.ignore_error_on_exit, self.pmi_enabled, self.walltime, self.policy)
        self._manager = Manager()
        self._send_signal(PGSignals.NEW)

    def start(self) -> None:
        """Starts up all processes according to the templates. If `restart ==
        False`, transition to 'Running', otherwise transition to 'Maintain'.
        """

        if not self.restart:
            self._send_signal(PGSignals.JOIN)
        else:
            self._send_signal(PGSignals.START)

    def join(self, timeout: float = None) -> None:
        """Wait for all processes to complete and the group to transition to
        Idle state. If the group status is 'Maintain', transition to 'Running'.

        Raises TimeoutError, if the timeout occurred.

        :param timeout: Timeout in seconds, optional defaults to None
        :type timeout: float
        :return: True if the timeout occured, False otherwise
        :retype: bool
        """

        start = time.monotonic()
        if self.status == str(Maintain()):
            self._send_signal(PGSignals.JOIN)
        stop = time.monotonic()

        if timeout == None:
            timeout = 100000000

        timeout = max(0, timeout - (stop - start))

        return self._wait_for_status(str(Idle()), timeout=timeout)

    def kill(self, signal: signal.Signals = signal.SIGKILL) -> None:
        """Send a signal to each process of the process group.

        The signals SIGKILL and SIGTERM have the following side effects:

        * If the signal is SIGKILL, the group will transition to 'Idle'. It can then be reused.
        * If the group status is 'Maintain', SIGTERM will transition it to 'Running'.
        * If the group status is 'Error', SIGTERM will raise a `DragonProcessGroupError`.

        :param signal: the signal to send, defaults to signal.SIGKILL
        :type signal: signal.Signals, optional
        """

        if signal == signal.SIGTERM:
            self._send_signal(PGSignals.SHUTDOWN)
        elif signal == signal.SIGKILL:
            self._send_signal(PGSignals.KILL)
        else:
            for puid in self.puids:
                process_kill(puid, sig=signal)

    def stop(self) -> None:
        """Forcibly terminate all workers by sending `SIGKILL` from any state,
        transition to `Stop`. This also removes the group from the manager process
        and marks the end of the group life-cycle.
        """

        self._send_signal(PGSignals.STOP)

    @property
    def puids(self) -> list[int]:
        """Return the puids of the processes contained in this group.

        :return: a list of puids
        :rtype: list[int or None]
        """

        return self._group_context.puids

    @property
    def status(self) -> str:
        """Get the current status of the process group handled by this instance.

        :returns: current status of the group
        :rtype: str
        """

        return self._group_context.status

    # Private interface

    def _send_signal(self, signal: PGSignals) -> bool:
        """Send the signal to the manager and wait for the response.
        The method guarantees completion of the signal by the manager,
        nothing more. I.e. the processes may have been started, but not
        actually executed any useful code yet.
        In case of sending IDLE, include the group context as well.
        """

        status = self.status

        if signal in self._group_context.forbidden[status]:
            raise DragonProcessGroupError(f"Signal {str(signal)} is not a valid transition from {status}")

        if signal == PGSignals.NEW:
            payload = self._group_context
        else:
            payload = None

        sig_id = self._group_context.get_next_sig_id()

        self._manager.queue.put((signal, payload, sig_id))

        while self._group_context.last_completed_signal < sig_id:
            if self.status == str(Error()):
                if signal not in [PGSignals.KILL, PGSignals.STOP]:
                    if self._group_context.last_completed_signal == sig_id - 1:
                        raise DragonProcessGroupError(
                            f"Signal {str(signal)} was not successful. Group in ERROR state."
                        )
                    else:
                        raise DragonProcessGroupError(
                            f"Signal {str(signal)} cannot be completed. Group in ERROR state"
                        )

            time.sleep(self._group_context.update_interval_sec)

    def _wait_for_status(self, status: str, timeout: float = None) -> None:

        sleep_time = self._group_context.update_interval_sec

        start = time.monotonic()

        while not self._group_context.status == status:

            dt = timeout - time.monotonic() + start

            if dt <= 0:
                raise TimeoutError(f"Timeout waiting for status {status}")

            time.sleep(min(sleep_time, dt))
