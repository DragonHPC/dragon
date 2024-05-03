import logging
import time
import enum
import signal
import threading
from time import sleep
from typing import List, Tuple
from abc import ABC, abstractmethod

from .value import Value
from .queue import Queue
from .array import Array
from .lock import Lock

from ..globalservices.process import (
    multi_join,
    get_create_message,
    get_create_message_with_argdata,
    query as process_query,
    get_multi_join_failure_puids,
    get_multi_join_success_puids
)
from ..globalservices.group import (
    create,
    create_add_to,
    remove_from,
    destroy,
    kill as group_kill,
    GroupError)
from ..globalservices.policy_eval import PolicyEvaluator 
from ..infrastructure.policy import Policy
from ..channels import Channel
from ..infrastructure.connection import Connection

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


class DragonCriticalProcessFailure(Exception):
    """Exception raised by the Dragon Pool Workers implementation"""

    pass


@enum.unique
class PGSignals(enum.IntEnum):
    ERROR = enum.auto()  # for testing purposes
    CRITICAL_FAILURE = enum.auto()  # for raising exceptions in case of failed processes
    NEW = enum.auto()  # Start manager but no workers
    START = enum.auto()  # start all processes/workers
    JOIN = enum.auto()  # Wait for all the processes to complete
    JOIN_SAVE = enum.auto()  # Wait for all the processes to complete and save puids at completion
    SHUTDOWN = enum.auto()  # stop all processes/workers via SIGTERM
    SHUTDOWN_SAVE = enum.auto()  # stop all processes/workers via SIGTERM and save puids at completion
    KILL = enum.auto()  # forcefully stop all processes/workers via SIGKILL
    KILL_SAVE = enum.auto()  # forcefully stop all processes/workers via SIGKILL and save puids at completion
    STOP = enum.auto()  # kill all Dragon processes and exit the manager
    STOP_SAVE = enum.auto()  # kill all Dragon processes and exit the manager, but cache inactive puids first
    EXIT_LOOP = enum.auto()  # tell state runner to exit its loop
    RAISE_EXCEPTION = enum.auto()  # tell state runner to return the group state status
    GROUP_STARTED = enum.auto()  # let processgroup loop know the group has been started in maintain
    GROUP_KILLED = enum.auto()  # let processgroup loop know the group has been destroyed from shutdown
    RETURN_TO_IDLE = enum.auto()  # let processgroup loop know all group procs are gone and state is back to Idle
    REQ_PUIDS = enum.auto()  # request state runner populate the puids array with active puids
    REQ_PUIDS_RESPONSE = enum.auto()  # let process group know the puids array is up-to-date
    REQ_INACTIVE_PUIDS = enum.auto()  # request state runner populate the inactive puids array with inactive puids
    REQ_INACTIVE_PUIDS_RESPONSE = enum.auto()  # let process group know the inactive puids array is up-to-date


class BaseState(ABC):
    """This class declares methods that all concrete State classes should
    implement and also provides a backreference to the Context object,
    associated with the State. This backreference can be used by States to
    transition the Context to another State.
    It also defines common methods and data structures to all states.
    """

    forbidden: list[int] = None
    gs_req_lock = threading.Lock()

    @property
    def state(self):
        """Link back to its our parent state object.
        :return: The group state holding this state
        :rtype: GroupContext
        """

        return self._state

    @state.setter
    def state(self, new_state) -> None:
        self._state = new_state

    @abstractmethod
    def run(self, prior_state, signal: PGSignals, sig_id: int) -> None:
        """Execute the run function defined by the parent state"""
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
            LOG.error(f"Process Group {self.state} is in error state.")
        except Exception:
            pass


class CriticalFailure(BaseState):
    """This state is triggered when an individual process marked as critical fails
    In this state, we kill all processes in the Group and raise an exception
    that gets triggered in the Monitor state, so the user sees an exception
    raised in their application"""

    forbidden: list = [s for s in PGSignals if s not in [PGSignals.KILL, PGSignals.STOP]]

    def run(self, prior_state: BaseState, signal: PGSignals, sig_id: int) -> None:

        if self.state.guid:
            group_kill(self.state.guid)
            self.state.guid = self.state._group_descr = None


class Idle(BaseState):
    """This state kills existing processes and does nothing otherwise."""

    forbidden = [PGSignals.KILL]

    def run(self, prior_state: BaseState, signal: PGSignals, sig_id: int) -> None:
        """The idle state just does nothing except making sure all processes are gone."""

        # Before doing anything make sure any threads are joined on:
        try:
            prior_state._maintainer_quit.set()
        except AttributeError:
            pass

        try:
            prior_state._maintainer_thread.join()
        except (AttributeError, RuntimeError):
            pass

        # Now clean up anything else
        if self.state.guid:
            try:
                group_kill(self.state.guid)
            except (AttributeError, RuntimeError):
                pass

            try:
                prior_state._multi_join_runner_thread.join()
            except (AttributeError, RuntimeError):
                pass

            # See if there are any hanging processes on our end
            non_exit_puids = [puid for puid in self.state.local_puids if puid != 0]
            if len(non_exit_puids) > 0:

                # Honestly, this should be uncessesary work for GS. We should figure out a better way
                proc_statuses = []
                for puid in non_exit_puids:

                    # check the status of this process by asking GS
                    not_exited = True
                    while not_exited:
                        proc_desc = process_query(puid)
                        if proc_desc.ecode:
                            not_exited = False
                            proc_statuses.append((puid, proc_desc.ecode))
                        else:
                            # don't overwhelm GS with requests
                            sleep(0.01)
                self.state._update_inactive_puids(proc_statuses)

            # Drop the global services state
            self.state.guid = self.state._group_descr = None

        self._start_time = None


def complete_exit(puid_codes: List[Tuple[int, int]],
                  state: BaseState,
                  pgsignal: PGSignals,
                  sig_id: int,
                  conn_out: Connection):
    """Register exiting of all processes in the ProcessGroup

    One of the last steps before ending execution of a ProcessGroup. It updates
    puids, both active and inactive, zeros out the Group ID/descriptor, and
    transitions the state to Idle

    :param puid_codes: List of the last tuples of puids and exit code as reported by Global Services
    :type puid_codes: List[Tuple[int, int]]
    :param state: The current state the ProcessGroup is in
    :type state: BaseState
    :param pgsignal: The signal we are told to update based on. From the user.
    :type pgsignal: PGSignals
    :param sig_id: signal that tells the user code we have completed the requested state
    :type sig_id: int
    :param conn_out: Connection to use to transmit a RETURN_TO_IDLE state
    :type conn_out: Connection
    """

    # catch bad ecodes
    ignore_err = state.ignore_error_on_exit
    if (pgsignal == PGSignals.STOP):
        ignore_err = True

    # Since this is running in its own thread, it needs to update the
    # status if needed
    raise_error = False
    if not ignore_err:
        for puid, ecode in puid_codes:
            if ecode not in {0, CYTHON_SIGTERM_ECODE, CYTHON_SIGTERM_ECODE % 256}:
                LOG.debug(f"Bad exit code {ecode} for puid {puid} in ProcessGroup")
                error_to_raise = DragonProcessGroupAbnormalExit(f"Process {puid} exited abnormally with {ecode}")
                raise_error = True

    # Update the exit code
    state._update_inactive_puids(puid_codes)

    if state._save_puids:
        state._update_puids_array(PGSignals.REQ_PUIDS)
        state._update_puids_array(PGSignals.REQ_INACTIVE_PUIDS)

    # If we ended up here, we've joined on all members of the group and that particular descriptor
    # and guid is now dead. Querying on it can deadlock gs_request.
    state.guid = state._group_descr = None

    # raise exception to parent thread
    if raise_error:
        state.man_out.send((PGSignals.RAISE_EXCEPTION, error_to_raise))
    # Else move on to error and exit this state
    else:
        # Tell anyone waiting for we're done
        conn_out.send((PGSignals.RETURN_TO_IDLE, sig_id))
        prior_state = state.transition_to(Idle)
        state.run(prior_state, pgsignal, sig_id)
        state.update_status(None)


class Maintain(BaseState):
    """This state starts missing processes and restarts processes that are not
    alive anymore.
    :raises DragonProcessGroupError: If one of the processes could not be (re)started.
    """

    forbidden = [PGSignals.NEW, PGSignals.START]
    _maintainer_thread = None
    _maintainer_quit = None

    def run(self, prior_state: BaseState, pgsignal: PGSignals, sig_id: int) -> None:

        nretries = self.state.num_restart_retries

        if self.state._group_descr is None:
            self.state._start_group_once()
            self.state.man_out.send((PGSignals.GROUP_STARTED, sig_id))
            self.state.update_status(sig_id)

        def _restart_processes(puids: List[int],
                               guid: int,
                               quitter_event: threading.Event) -> bool:
            """Remove a list of puids from the GS ProcessGroup, restart a corresponding number, and define a new GroupDescriptor

            :param puids: List of puids to remove from Global Services' monitoring (because they've exited)
            :type puids: List[int]
            :param guid: The group descriptor guid these processes belong to
            :type guid: int
            :param quitter_event: threading event that will tell parent thread to exit this function early.
            :type quitter_event: threading.Event
            :returns: Whether the calling fucnction should break from its while loop due to the quitter_event being set
            :rtype: bool
            """
            group_descr = None
            break_loop = False

            for puid, _ in puids:
                # Before submitting a GS request, check that we haven't been told to quit
                if quitter_event.is_set():
                    break_loop = True
                    break
                try:
                    group_descr = remove_from(guid, [puid])
                # Group may have been destroyed and we just haven't gotten back to the top of our loop yet
                except (GroupError, TypeError):
                    if quitter_event.is_set():
                        break_loop = True
                        break

                # create a new one
                msg = self.state.messages[self.state.puid_to_message_map[puid]]
                nrestarts = 0

                while nrestarts < nretries:
                    try:
                        # Before submitting a GS request, check that we haven't been told to quit
                        if quitter_event.is_set():
                            break_loop = True
                            break
                        group_descr = create_add_to(guid, [(1, msg.serialize())], self.state.policy)
                        # update the puid_to_message_map dict with the newly added process
                        puids = [descr.uid for lst in group_descr.sets for descr in lst]
                        for new_puid in puids:
                            if new_puid not in self.state.puid_to_message_map:
                                self.state.puid_to_message_map[new_puid] = self.state.puid_to_message_map[
                                    puid
                                ]
                                # since we added only one process, we can safely assume that we found it
                                break
                        break
                    except Exception:
                        nrestarts += 1

                # we need to update the group descriptor after all the above additions/removals
                if group_descr is not None:
                    guid = group_descr.g_uid
                    self.state._group_descr = group_descr
                    self.state.guid = group_descr.g_uid
                    self.state._update_active_puids([descr.uid for lst in group_descr.sets for descr in lst])
                if break_loop:
                    break

                if nrestarts == nretries:
                    self.state.man_out.send((PGSignals.RAISE_EXCEPTION,
                                             DragonProcessGroupError(f"Unable to start process using message {msg}.")))

            return break_loop

        def _maintain_runner(sig_id: int,
                             quitter_event: threading.Event):
            """Maintain thread function that monitors processes and restarts them as they exit until told to stop

            :param sig_id: Signal that tells used to tell us user has control of its main thread again
            :type sig_id: int
            :param quitter_event: Event telling us to exit and allow transition to a new state
            :type quitter_event: threading.Event
            """

            break_loop = False
            # Wait until we've updated the status to start our maintain loop
            while self.state.last_completed_signal != sig_id:
                sleep(0.01)

            # Enter a loop monitoring the processes in our group and restart if they go down
            while not break_loop:

                # Get the latest guid just in case it's been changed
                guid = self.state.guid

                # The event checking may seem excessive but we want to avoid entering a
                # GS request via multi_join if at all possible.
                if quitter_event.is_set():
                    break
                puids = [descr.uid for lst in self.state._group_descr.sets for descr in lst]
                if puids:
                    ready = multi_join(puids, join_all=False, timeout=0.3, return_on_bad_exit=True)
                # Make sure we weren't told to exit by the user.
                if quitter_event.is_set():
                    break

                if ready[0]:
                    self.state._update_inactive_puids(ready[0])

                # Check if there were any non-zero exits
                bad_exits, _ = get_multi_join_failure_puids(ready[1])
                clean_exits, _ = get_multi_join_success_puids(ready[1])

                # If we returned from the join via a bad exit, restart those processes
                if bad_exits:
                    break_loop = _restart_processes(bad_exits, guid, quitter_event)
                # Otherwise, we can exit since we have no bad exits and all processes have exited and we  (ready[0] condition)
                elif clean_exits:
                    break_loop = _restart_processes(clean_exits, guid, quitter_event)

        # Start a thread that runs a loop over joins and restarts for our Pool implementation.
        # Setting this loop in a thread allows us to return control to the thread listening for
        # state changes from the manager
        self._maintainer_quit = threading.Event()
        self._maintainer_thread = threading.Thread(name="maintainer thread",
                                                   target=_maintain_runner,
                                                   args=(sig_id, self._maintainer_quit),
                                                   daemon=False)
        self._maintainer_thread.start()


class Running(BaseState):
    """State for running ProcessGroup with no restart, or to handle group kills from earlier states"""

    # user needs to wait for group to become IDLE
    forbidden = [
        s for s in PGSignals if s not in [PGSignals.ERROR,
                                          PGSignals.KILL, PGSignals.KILL_SAVE,
                                          PGSignals.SHUTDOWN, PGSignals.SHUTDOWN_SAVE,
                                          PGSignals.JOIN, PGSignals.JOIN_SAVE]
    ]

    _multi_join_runner_thread = None

    def run(self, prior_state: BaseState, pgsignal: PGSignals, sig_id: int) -> None:

        # In case the maintainer thread is in action, make sure it's gone before we try doing
        # anything else. This limits stress on global services and thread contention
        try:
            prior_state._maintainer_quit.set()
        except AttributeError:
            pass

        try:
            prior_state._maintainer_thread.join()
        except (AttributeError, RuntimeError):
            pass

        # If we ended up here, the user may is requesting a shutdown rather than for us to run thing.
        # I'm unsure why this is needed inside Running, but I'm following the precedent given me
        if pgsignal == PGSignals.SHUTDOWN and self.state.guid is not None:  # have processes exit
            group_kill(self.state.guid, sig=signal.SIGTERM)
            self.state.man_out.send((PGSignals.GROUP_KILLED, sig_id))

            # NOTE: don't set guid to None here. Later transitions will do that when appropriate.

        # Make sure we join on everything
        if not self._multi_join_runner_thread:
            # this is more complicated as it needs to be, because we're using
            # process objects and multi_join wants puids.
            if prior_state == Idle:  # if we started with restart == False from Idle
                self.state._start_group_once()
                self.state.man_out.send((PGSignals.GROUP_STARTED, sig_id))
                self.state.update_status(sig_id)

            # Set up a thread to run the blocking multi_join on
            if not self._multi_join_runner_thread:

                def _multi_join_runner(puids: List[int],
                                       critical: bool,
                                       pgsignal: PGSignals,
                                       sig_id: int):
                    """Function run by Running thread. Joins on a list of puids and exits when they've exited

                    :param puids: List of puids to monitor state of
                    :type puids: List[int]
                    :param critical: Whether the loss of any invididual puid should be treated as critical via raising an error
                    :type critical: bool
                    :param pgsignal: The signal the user provided to bring us to this state
                    :type pgsignal: PGSignals
                    :param sig_id: The signal int that we were given upon transition to this state
                    :type sig_id: int
                    """

                    ready = multi_join(puids, join_all=True, timeout=None, return_on_bad_exit=critical)
                    if ready[0] is not None and not self.state.critical.value:  # no timeout
                        complete_exit(ready[0], self.state, pgsignal, sig_id, self.state.man_out)

                    # if we were told to treat each process in the group as a critical process, we want to
                    # give the user info about process-by-process proc status.
                    elif self.state.critical.value:

                        # If multi-join told us everyone is done, let's get out of here:
                        if ready[0]:
                            complete_exit(ready[0], self.state, pgsignal, sig_id, self.state.man_out)
                        else:
                            bad_puids, _ = get_multi_join_failure_puids(ready[1])
                            if bad_puids:
                                prior_state = self.state.transition_to(CriticalFailure)
                                self.state.run(prior_state, pgsignal, sig_id)
                                self.state.update_status(None)

                self._multi_join_runner_thread = threading.Thread(name="multi_join runner thread",
                                                                  target=_multi_join_runner,
                                                                  args=(self.state.local_puids, self.state.critical.value, pgsignal, sig_id),
                                                                  daemon=False)
                self._multi_join_runner_thread.start()

        # If we didn't go into the above section, we still need to transition to Idle. The
        # logic in the above if block will handle the transition otherwise.
        else:
            self.state.man_out.send((PGSignals.RETURN_TO_IDLE, sig_id))
            prior_state = self.state.transition_to(Idle)
            self.state.run(prior_state, pgsignal, sig_id)
            self.state.update_status(None)


class Stop(BaseState):
    """Stops all processes of the group and removes the group from the Manager. The
    group cannot be restarted anymore.
    """

    forbidden = [s for s in PGSignals]  # end of line

    def run(self, prior_state: BaseState, signal: PGSignals, sig_id: int) -> None:

        # Make sure we exit the maintain thread if it's up
        try:
            prior_state._maintainer_quit.set()
        except AttributeError:
            pass

        if self.state.guid is not None:
            resp = destroy(self.state.guid)
            ready = [(desc.desc.p_uid, desc.desc.ecode) for lst in resp.sets for desc in lst]
            complete_exit(ready, self.state, signal, sig_id, self.state.man_out)

        if self.state._save_puids:
            self.state._update_puids_array(PGSignals.REQ_PUIDS)
            self.state._update_puids_array(PGSignals.REQ_INACTIVE_PUIDS)

        # And join on that thread
        try:
            prior_state._maintainer_thread.join()
        except (AttributeError, RuntimeError):
            pass

        # And join on that thread
        try:
            prior_state._maintainer_thread.join()
        except (AttributeError, RuntimeError):
            pass

        self.state.guid = self.state._group_descr = None

# end concrete state classes


class ProcessGroupState:
    """The Context defines the group interface for the manager and the client.
    In particular, it handles signals and state changes. It maintains a
    reference to an instance of a State subclass, which represents the current
    state of the group of processes.
    """

    _state: BaseState = None

    update_interval_sec: float = 0.5
    num_kill_retries: int = 2
    num_restart_retries: int = 10000
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
        str(CriticalFailure()): CriticalFailure.forbidden
    }

    avail_states: dict = {
            str(Error()): 0,
            str(Idle()): 1,
            str(Maintain()): 2,
            str(Running()): 3,
            str(Stop()): 4,
            str(CriticalFailure()): 5
    }
    states_keys = list(avail_states.keys())
    states_vals = list(avail_states.values())

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

        :param templates: a list of tuples where each tuple contains a replication factor `n` and a Dragon ProcessTemplate object specifing the properties of the process to start. The processes can hold the same or different attributes in any way and will be numbered in order.
        :type templates: list[tuple(int, dragon.native.process.ProcessTemplate),]
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
        self.templates = templates  # list of tuples
        self.messages = {}  # keys are the indices of tuples in self.templates

        # use a dict to make restarting easy and order-safe
        self.puid_to_message_map = {}  # keys are the puids, values are the keys in self.messages{}

        for i, tup in enumerate(templates):
            t = tup[1]
            if t.is_python:
                self.messages[i] = get_create_message_with_argdata(
                    t.target,
                    t.cwd,
                    t.args,
                    t.env,
                    t.argdata,
                    pmi_required=pmi_enabled,
                    stdin=t.stdin,
                    stdout=t.stdout,
                    stderr=t.stderr,
                    policy=t.policy,
                )
            else:
                self.messages[i] = get_create_message(
                    t.target,
                    t.cwd,
                    t.args,
                    t.env,
                    pmi_required=pmi_enabled,
                    stdin=t.stdin,
                    stdout=t.stdout,
                    stderr=t.stderr,
                    policy=t.policy,
                )

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

        # (State enum, join status, puid list, puid exited, exit code if exited, last_completed_signal)
        self._status_lock = Lock()
        self._status = (Value('i', value=-1),
                        Array('l', [-1] * self.nproc),
                        Value('i', value=-1))
        self._puids_initd = False
        self.local_puids = []

        # I need this to be able to grow to n > self.nproc, so I have to employ this hack
        self._save_puids = False
        self.local_inactives = []
        self._inactive_puids = Queue()
        self._inactive_puids.put([(0, 0)])

        self.critical = Value("b", value=False)

        self.transition_to(Idle)
        self.update_status(None)

    def start_state_runner(self,
                           man_ch_in_sdesc: str,
                           man_ch_out_sdesc: str):
        """Start the ProcessGroupState thread that listens for messages from Manager

        :param man_ch_sdesc_in: Channel for receiving messages from Manager
        :type man_ch_sdesc_in: str
        :param man_ch_sdesc_out: Channel for sending messages to Manager
        :type man_ch_sdesc_out: str
        """

        self._state_runner_thread = threading.Thread(name="GroupContext State Runner Thread",
                                                     target=self._state_runner,
                                                     args=(man_ch_in_sdesc, man_ch_out_sdesc),
                                                     daemon=False)
        self._state_runner_thread.start()

    def stop_state_runner(self):
        """Join on the ProcessGroupState runner thread"""

        self._state_runner_thread.join()

    def handle_signal(self, signal: PGSignals) -> Tuple[BaseState, BaseState, PGSignals]:
        """This method takes a signal, checks if the signal is allowed, given
        the current state, and returns the new state.

        :param signal: The signal to consider
        :type signal: PGSignals
        :returns: target state, current state, modified signal
        :rtype: {Tuple[BaseState, BaseState, PGSignals]}
        """

        # This messages is to tell us to PGSignals.STOP but to save the inactives before exiting.
        # So update that attribute and follow the logic for STOP
        if signal in [PGSignals.STOP_SAVE, PGSignals.JOIN_SAVE, PGSignals.KILL_SAVE, PGSignals.SHUTDOWN_SAVE]:
            self._save_puids = True
            if signal == PGSignals.STOP_SAVE:
                signal = PGSignals.STOP
            elif signal == PGSignals.JOIN_SAVE:
                signal = PGSignals.JOIN
            elif signal == PGSignals.KILL_SAVE:
                signal = PGSignals.KILL
            elif signal == PGSignals.SHUTDOWN_SAVE:
                signal = PGSignals.SHUTDOWN

        if signal in self._state.forbidden:
            LOG.error(f"Signal not accepted {signal} for state {self._state} !")
            return

        if signal:
            next_state = self.target_states[signal][0]
        else:
            next_state = self.current_state

        return next_state, self.current_state, signal

    def transition_to(self, new_state: BaseState) -> BaseState:
        """Transition the state to a new state. This does NOT run
        the new state or updates the status.

        :param new_state: state to transition to
        :type new_state: BaseState
        :return: previous state
        :rtype: BaseState
        """

        prior_state = self._state

        # If we're in maintain mode, we need tell the maintainer thread to stop
        if self.status == str(Maintain()):
            new_state._maintainer_quit = self._state._maintainer_quit

        # Proceed with the transition
        self._state = new_state
        self._state.state = self

        return prior_state

    @property
    def current_state(self) -> BaseState:
        """Return the current state object.

        :return: the state
        :rtype: BaseState
        """

        return self._state

    @property
    def prior_state(self) -> BaseState:
        """Return the current state object.

        :return: the state
        :rtype: BaseState
        """

        return self._prior_state

    def run(self, prior_state: BaseState, signal: PGSignals, sig_id: int):
        """Execute the current (!) state in self._state.

        :param prior_state: prior state
        :type prior_state: BaseState
        """

        self._state.run(self._state, prior_state, signal, sig_id)

    def transition_to_error(self, prior_state, sig_id, e):
        LOG.error(f"Exception in transition {prior_state} -> {self._state}: {e} ")
        self.transition_to(Error)
        self.update_status(None)
        self.run(self._state, None, sig_id)

    def _get_response(self, input_signal: PGSignals) -> PGSignals:
        """Return a signal analogously matching the input signal

        :param input_signal: The signal we're requesting the corresponding signal to
        :type input_signal: PGSignals
        :returns: signal corresponding to the input
        :rtype: {PGSignals}
        """

        if input_signal == PGSignals.REQ_PUIDS:
            return PGSignals.REQ_PUIDS_RESPONSE
        elif input_signal == PGSignals.REQ_INACTIVE_PUIDS:
            return PGSignals.REQ_INACTIVE_PUIDS_RESPONSE

    def _state_runner(self, man_ch_sdesc_in: str, man_ch_sdesc_out: str):
        """Function run by ProcessGroupState thread to respond to Manager requests

        :param man_ch_sdesc_in: Channel for receiving messages from Manager
        :type man_ch_sdesc_in: str
        :param man_ch_sdesc_out: Channel for sending messages to Manager
        :type man_ch_sdesc_out: str
        """

        man_ch_in = Channel.attach(man_ch_sdesc_in)
        man_ch_out = Channel.attach(man_ch_sdesc_out)

        man_inout = Connection(inbound_initializer=man_ch_in,
                               outbound_initializer=man_ch_out)
        self.man_out = Connection(outbound_initializer=man_ch_in)

        running = True
        while running:

            # Provide a way to handle a user requested timeout
            if self.walltime and self._start_time > 0:
                timeout = self.walltime - (time.monotonic() - self._start_time)
                if man_inout.poll(timeout=timeout):
                    signal, sig_id = man_inout.recv()
                else:
                    signal = PGSignals.KILL  # goodbye
                    sig_id = self.last_completed_signal

                    # Make sure we don't go crazy in a loop
                    self._start_time = 0
            # Otherwise, just do a better performing blocking recv.
            else:
                signal, sig_id = man_inout.recv()

            # The Manager may have gotten the message. If not, we need to forward it
            if signal in [PGSignals.GROUP_STARTED, PGSignals.GROUP_KILLED, PGSignals.RETURN_TO_IDLE]:
                man_inout.send((signal, sig_id))
                continue

            if signal in [PGSignals.REQ_PUIDS, PGSignals.REQ_INACTIVE_PUIDS]:
                # Make sure the puids array is up-to-date with our local info
                self._update_puids_array(signal)
                man_inout.send((self._get_response(signal), sig_id))
                continue

            if signal in [PGSignals.STOP, PGSignals.STOP_SAVE]:
                running = False
            if signal == PGSignals.EXIT_LOOP:
                break
            # The thread in Running hit an exception
            elif signal == PGSignals.RAISE_EXCEPTION:
                self.transition_to_error(self.current_state, None, sig_id)
                continue

            try:
                new_state, prior_state, signal = self.handle_signal(signal)
                self.transition_to(new_state)
                self.run(prior_state, signal, sig_id)
                self.update_status(sig_id)
            except Exception as e:  # automagically transition to Error if something goes wrong
                self.transition_to_error(prior_state, sig_id, e)
                break

        # Tell the manager we're out
        man_inout.send((None, None))

    def get_next_sig_id(self) -> int:
        """Obtain the next unique signal ID for this group state

        :return: next signal id
        :rtype: int
        """

        # we need to communicate if a signal completed, so we need an ID for it.

        sig_id = self._signal_counter.value
        self._signal_counter.value = 1 + self._signal_counter.value

        return sig_id

    @property
    def status(self) -> str:
        """The current status of the Group.

        :return: state name
        :rtype: str
        """
        with self._status_lock:
            state_val, _, __ = self._status
            val = state_val.value

            if val >= 0:
                return self.states_keys[self.states_vals.index(val)]
            else:
                return str(None)

    @property
    def puids(self) -> List[int]:
        """The puids currently active and maintained by the GroupContext.

        :return: A list of puids. If puids haven't started, an empty list is returned
        :rtype: List[int]
        """
        with self._status_lock:
            _, puids, __, = self._status
            val = [puid for puid in puids if puid != 0]
            return val

    @property
    def active_puids(self) -> List[int]:
        """The puids currently active and maintained by the GroupContext.

        :return: A list of puids if the process is started or None if not.
        :rtype: List[int]
        """

        return self.puids

    @property
    def inactive_puids(self) -> List[Tuple[int, int]]:
        """puids that have exited processes

        :return: A list of puids if the process is started or None if not.
        :rtype: list[int or None]
        """

        i_puids = self._inactive_puids.get(timeout=None)
        self._inactive_puids.put(i_puids)
        return i_puids

    @property
    def last_completed_signal(self) -> int:
        """Return the ID of the last successfully completed signal.

        :return: the signal id
        :rtype: int
        """
        with self._status_lock:
            _, __, compl_sig_id = self._status
            sig_id = compl_sig_id.value

            return sig_id

    def _update_puids_array(self, signal: PGSignals):
        """Make sure the puids array matches in locally cached one, accessibly by ProcessGroup object

        :param signal: signal asking for Active or Inactive PUIDS
        :type signal: PGSignals
        """

        if signal == PGSignals.REQ_PUIDS:
            with self._status_lock:
                _, puids, __, = self._status
                for idx, local_puid in enumerate(self.local_puids):
                    puids[idx] = local_puid
        elif signal == PGSignals.REQ_INACTIVE_PUIDS:
            _ = self._inactive_puids.get(timeout=None)
            self._inactive_puids.put(self.local_inactives)

    def _start_group_once(self):
        """Start all processes in the ProcessGroup. Only done once"""

        if not self._start_time:
            self._start_time = time.monotonic()

        # piecemeal merging of policies. 
        # first merge group policy and global policy with group policy > global policy.
        # this merged policy is saved and is what will be used if in the maintain state if we need to restart processes
        if self.policy is not None:
            self.policy = PolicyEvaluator.merge(Policy.global_policy(), self.policy)
        else:
            self.policy = Policy.global_policy() 

        # then go through process policies and merge process policies with group+global policy with process policy > group+global policy.
        policy_list = []
        for i, tup in enumerate(self.templates):
            # we check if we the template has a policy
            if self.messages[i].policy is None:
                policy_list.extend([self.policy] * tup[0])
            else:
                # if the template process got a policy then we give it higher priority, than the policy of the process group.  
                merged_policy = PolicyEvaluator.merge(self.policy, self.messages[i].policy)
                policy_list.extend([merged_policy] * tup[0])

        group_descr = create(
            [(tup[0], self.messages[i].serialize()) for i, tup in enumerate(self.templates)], policy_list 
        )

        self._group_descr = group_descr
        self.guid = group_descr.g_uid

        # construct the puid_to_message_map dict now that the processes are created
        puids = [descr.uid for lst in group_descr.sets for descr in lst]
        puid_idx = 0
        for i, tup in enumerate(self.templates):
            for _ in range(tup[0]):
                self.puid_to_message_map[puids[puid_idx]] = i
                puid_idx += 1

    def _update_inactive_puids(self, exit_statuses: List[Tuple[int, int]]):
        """Update the exit status of all processes inside of the state

        Takes results from GlobalServices' multi-join to update our internal state
        :param exit_statuses: puids paired with their exit codes
        :type exit_statuses: List[Tuple[int, int]]
        """
        self.local_inactives += exit_statuses

        # Remove these dead puids from the active list
        for l_puid, _ in exit_statuses:
            try:
                s_idx = [i for i, puid in enumerate(self.local_puids) if puid == l_puid]
                # It's possible we already removed this puid earlier
                if len(s_idx) == 1:
                    self.local_puids[s_idx[0]] = 0
            except (IndexError, AssertionError):
                raise

    def _update_active_puids(self, puids: List[int]):
        """Update the list of active puids

        Takes results from GlobalServices' multi-join to update our internal state
        :param exit_statuses: puids paired with their exit codes
        :type exit_statuses: List[Tuple[int, int]]
        """

        # find the current index:
        for l_puid in puids:
            if l_puid not in self.local_puids:
                try:
                    idx = [idx for idx, o_puid in enumerate(self.local_puids) if o_puid == 0][0]
                    self.local_puids[idx] = l_puid
                except Exception:
                    pass

    # I need to report the last completed signal ID here, so that the
    # corresponding client can be be sure his request has been completed.
    def update_status(self, compl_sig_id: int):
        """update the global status of this state.

        :param compl_sig_id: signal just completed. If None, reuse the last value from the queue.
        :type compl_sig_id: int
        """
        with self._status_lock:
            state_name, last_puids, last_sig_id = self._status
            state_name.value = self.avail_states[str(self._state.__name__)]

            if compl_sig_id is None:  # this transition was automatic
                compl_sig_id = last_sig_id.value
            else:
                last_sig_id.value = compl_sig_id

            if self._group_descr and not self._puids_initd:
                self.local_puids = [descr.uid for lst in self._group_descr.sets for descr in lst]
                self._puids_initd = True

    def _make_critical(self):

        self.critical.value = True
