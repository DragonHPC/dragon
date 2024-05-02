"""The Dragon native class for managing the life-cycle of a group of Dragon processes.

The intent is for the class to be agnostic about what the processes are doing,
it only maintains their lifecycle.

This file implements a client API class and a Manager process handling all
groups on the node. The manager holds a list of ProcessGroupState classes and gets
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
import signal
import threading
from time import sleep
from typing import List, Tuple

from .process import Process, ProcessTemplate
from .group_state import (
        ProcessGroupState,
        PGSignals,
        DragonProcessGroupError,
        Maintain,
        Idle,
        Error
)

from ..channels import Channel
from ..infrastructure.facts import default_pool_muid_from_index
from ..globalservices.process import kill as process_kill
from ..globalservices.channel import create
from ..infrastructure.parameters import this_process, Policy
from ..infrastructure.connection import Connection, ConnectionOptions
from ..utils import B64

LOG = logging.getLogger(__name__)


# TODO: Extend API to control distribution over multiple nodes.

class Manager:
    def __init__(self,
                 pg_ch_in_sdesc,
                 pg_ch_out_sdesc):
        """The Manager class holds a process that handles the life-cycle of all
        process groups on this node.
        We handle the group using a ProcessGroupState class that holds all necessary
        methods and attributes shared by manager and clients.
        """

        # missing shared memory implementation, so I am using a Queue
        self.group_state = None

        # Create some channels for talking between Manager threads and then the ProcessGroupState
        # state thread
        self._pg_ch_in_sdesc = pg_ch_in_sdesc
        self._pg_ch_out_sdesc = pg_ch_out_sdesc
        self._gc_ch_in = create(m_uid=default_pool_muid_from_index(this_process.index))
        self._gc_ch_out = create(m_uid=default_pool_muid_from_index(this_process.index))
        self._state_runner_out = create(m_uid=default_pool_muid_from_index(this_process.index))

        # create a pipe to make sure I can

        signal_args = (self._pg_ch_in_sdesc, self._pg_ch_out_sdesc,
                       self._gc_ch_in.sdesc, self._gc_ch_out.sdesc,
                       self._state_runner_out.sdesc)
        self._proc = Process(target=self._signal_handler,
                             args=signal_args)
        self._proc.start()

    def __del__(self):

        try:
            self._proc.join(timeout=self.update_interval_sec)
        except Exception:
            pass

        # make manager exit
        try:
            self._proc.kill()  # please go away
        except Exception:
            pass

    def kill(self):
        """Kill the manager process"""

        self._proc.kill()

    def join(self, timeout=None):
        """Join on the manager process

        :param timeout: how long to wait before returning. If none, block. defaults to None
        :type timeout: float, optional
        """
        try:
            if self.is_alive:
                self._proc.join(timeout=timeout)
        except AttributeError:
            pass

    @property
    def is_alive(self) -> bool:
        """Whether the manager process is still alive

        :returns: True if alive. False otherwise
        :rtype: {bool}
        """
        try:
            return self._proc.is_alive
        except Exception:
            return False

    def _listen_for_pgsignals(self,
                              pg_sdesc_in: str,
                              pg_sdesc_out: str,
                              gc_sdesc_in: str,
                              gc_sdesc_out: str):
        """Function executed as thread for listening to ProcessGroup requests from the user

        :param pg_sdesc_in: Channel used for receiving messages from user
        :type pg_sdesc_in: str
        :param pg_sdesc_out: Channel used for sending message back to user
        :type pg_sdesc_out: str
        :param gc_sdesc_in: Channel for receiving messages from thread managing ProcessGroup state
        :type gc_sdesc_in: str
        :param gc_sdesc_out: Channel for sending messages to thread managing ProcessGroup state
        :type gc_sdesc_out: str
        """
        signal = None
        exit_loop = False

        pg_ch_in = Channel.attach(pg_sdesc_in)
        pg_ch_out = Channel.attach(pg_sdesc_out)
        pg_inout = Connection(inbound_initializer=pg_ch_in,
                              outbound_initializer=pg_ch_out)

        gc_ch_in = Channel.attach(gc_sdesc_in)
        gc_ch_out = Channel.attach(gc_sdesc_out)
        gc_inout = Connection(inbound_initializer=gc_ch_in,
                              outbound_initializer=gc_ch_out)

        response_signals = [PGSignals.GROUP_STARTED,
                            PGSignals.GROUP_KILLED,
                            PGSignals.RETURN_TO_IDLE,
                            PGSignals.REQ_PUIDS_RESPONSE,
                            PGSignals.REQ_INACTIVE_PUIDS_RESPONSE]

        while not exit_loop:

            try:
                signal, payload, sig_id = pg_inout.recv()
            except Exception:
                signal = payload = sig_id = None  # no change

            if signal == PGSignals.NEW and payload:  # new state object !
                self.group_state = payload

            if signal == PGSignals.EXIT_LOOP:
                exit_loop = True
            elif signal in response_signals:
                pg_inout.send(signal)
                continue

            # this looks overengineered, but I need to cover Join :
            # * Join.run depends on the prior state (Idle or Maintain), need to save it
            # * transitioning is not followed by a run for Join->Idle
            # * update_status has to be run multiple times by a Join
            # * we update the status without running the state in Join
            if self.group_state:
                gc_inout.send((signal, sig_id))

        pg_inout.send(None)
        gc_inout.send((None, None))

        try:
            pg_inout.ghost_close()
        except Exception:
            pass

        try:
            gc_inout.close()
        except Exception:
            pass

        try:
            self._gc_ch_in.destroy()
        except Exception:
            pass

        try:
            self._gc_ch_out.destroy()
        except Exception:
            pass

    def _monitor_group_state(self,
                             gc_sdesc_in: str,
                             pg_sdesc_out: str,
                             state_sdesc_out: str):
        """Function run as thread monitoring the ProcessGroup state via the ProcessGroupState management thread

        :param gc_sdesc_in: Channel for receiving messages from the Manager's listener thread
        :type gc_sdesc_in: str
        :param pg_sdesc_out: Channel for sending mesages to the user's ProcessGroup object
        :type pg_sdesc_out: str
        :param state_sdesc_out: Channel for sending signals to the ProcessGroupState's state runner thread
        :type state_sdesc_out: str
        """

        gc_ch_in = Channel.attach(gc_sdesc_in)
        gc_in = Connection(inbound_initializer=gc_ch_in)

        pg_ch_out = Channel.attach(pg_sdesc_out)
        pg_out = Connection(outbound_initializer=pg_ch_out)

        state_ch_in = Channel.attach(state_sdesc_out)
        state_out = Connection(outbound_initializer=state_ch_in)

        response_signals = [PGSignals.GROUP_STARTED,
                            PGSignals.GROUP_KILLED,
                            PGSignals.RETURN_TO_IDLE,
                            PGSignals.REQ_PUIDS_RESPONSE,
                            PGSignals.REQ_INACTIVE_PUIDS_RESPONSE]

        # Wait until we know we have a state before I try referencing it
        while not self.group_state:
            sleep(0.001)

        self.runner_thread = self.group_state.start_state_runner(state_sdesc_out, gc_sdesc_in)

        running = True
        while running:

            signal, sig_id = gc_in.recv()
            if signal is None and sig_id is None:
                running = False
                break
            elif signal in response_signals:
                pg_out.send((signal, None, None))
                continue

            state_out.send((signal, sig_id))

        # Let the processgroup thread now we're done
        pg_out.send((PGSignals.EXIT_LOOP, None, None))
        self.group_state.stop_state_runner()
        try:
            gc_in.ghost_close()
        except Exception:
            pass

        try:
            pg_out.ghost_close()
        except Exception:
            pass

        try:
            state_out.close()
        except Exception:
            pass

        try:
            self._state_runner_out.destroy()
        except Exception:
            pass

    def _signal_handler(self,
                        pg_ch_in_sdesc: str, pg_ch_out_sdesc: str,
                        gc_ch_in_sdesc: str, gc_ch_out_sdesc: str,
                        state_runner_out_sdesc: str):
        """Function that is the 'Manager' and launched as a new Process to execute the ProcessGroup

        This function is only responsible for launching the listener and monitor threads to manage
        ProcessGroup requests from the user and monitoring the state of the ProcessGroupState
        object. It returns when the thread have finished their work.

        :param pg_sdesc_in: Channel used for receiving messages from user
        :type pg_sdesc_in: str
        :param pg_sdesc_out: Channel used for sending message back to user
        :type pg_sdesc_out: str
        :param gc_sdesc_in: Channel for receiving messages from thread managing ProcessGroup state
        :type gc_sdesc_in: str
        :param gc_sdesc_out: Channel for sending messages to thread managing ProcessGroup state
        :type gc_sdesc_out: str
        :param state_sdesc_out: Channel for sending signals to the ProcessGroupState's state runner thread
        :type state_sdesc_out: str
        """

        # Create one thread that listens for messages from the user via
        # the ProcessGroup user API. Create another thread that monitors
        # execution of jobs in the ProcessGroup via it's ProcessGroupState object
        pgroup_listener_thread = threading.Thread(name="ProcessGroup listener",
                                                  target=self._listen_for_pgsignals,
                                                  args=(pg_ch_in_sdesc, pg_ch_out_sdesc, gc_ch_in_sdesc, gc_ch_out_sdesc),
                                                  daemon=False)

        gstate_monitor_thread = threading.Thread(name="ProcessGroupState monitorer",
                                                 target=self._monitor_group_state,
                                                 args=(gc_ch_out_sdesc, pg_ch_in_sdesc, state_runner_out_sdesc),
                                                 daemon=False)

        pgroup_listener_thread.start()
        gstate_monitor_thread.start()

        pgroup_listener_thread.join()
        gstate_monitor_thread.join()


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
        policy: Policy = None,
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

        self.templates = []  # this will be a list of tuples that will be sent to the GSGroup API
        self.nproc = 0
        self.restart = restart
        self.ignore_error_on_exit = ignore_error_on_exit
        self.pmi_enabled = pmi_enabled
        self.walltime = walltime
        self.policy = policy
        self._group_state = None

        # We may catch exit of the state runner in a few places. We set this true
        # to make sure we don't get hung in places
        self._state_exit_signaled = False

        self._pg_ch_in = create(m_uid=default_pool_muid_from_index(this_process.index))
        self._pg_ch_out = create(m_uid=default_pool_muid_from_index(this_process.index))
        self._conn_options = ConnectionOptions(creation_policy=ConnectionOptions.CreationPolicy.PRE_CREATED)
        self._pg_inout = Connection(inbound_initializer=self._pg_ch_in.sdesc,
                                    outbound_initializer=self._pg_ch_out.sdesc,
                                    options=self._conn_options)

    def __del__(self):

        try:
            self._pg_inout.send((PGSignals.EXIT_LOOP, None, None))
        except Exception:
            pass

        try:
            self._manager.join(timeout=1)
        except Exception:
            self._manager.kill()
        finally:
            del self._manager

        try:
            self._pg_inout.close()
        except Exception:
            pass

        try:
            self._pg_ch_in.destroy()
        except Exception:
            pass

        try:
            self._pg_ch_out.destroy()
        except Exception:
            pass

    def add_process(self, nproc: int, template: ProcessTemplate) -> None:
        """Add processes to the ProcessGroup.

        :param template: single template processes, i.e. unstarted process objects
        :type template: dragon.native.process.ProcessTemplate
        :param nproc: number of Dragon processes to start that follow the provided template
        :type nproc: int
        """

        # if add_process is called after the ProcessGroup is initialized, then we raise
        if self._group_state:
            # TODO: Consider adding ProcessGroup.create_add_to() to allow users to add more template processes after init has been called
            raise DragonProcessGroupError("You cannot call add_process() to already initialized ProcessGroup.")

        self.templates.append((nproc, template))
        self.nproc += nproc

    def init(self) -> None:
        """Initialize the ProcessGroupState and Manager."""

        self._group_state = ProcessGroupState(
            self.templates,
            self.nproc,
            self.restart,
            self.ignore_error_on_exit,
            self.pmi_enabled,
            self.walltime,
            self.policy
        )

        self._manager = Manager(self._pg_ch_out.sdesc, self._pg_ch_in.sdesc)
        self._send_signal(PGSignals.NEW)

    def start(self) -> None:
        """Starts up all processes according to the templates. If `restart ==
        False`, transition to 'Running', otherwise transition to 'Maintain'.
        """

        # This needs to be done before we signal starting of the procs so the group
        # state gets marked as critical if we're running inside a with block

        if not self.restart:
            self._send_signal(PGSignals.JOIN)
        else:
            self._send_signal(PGSignals.START)

    def join(self, timeout: float = None, save_puids: bool = False) -> None:
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
            if save_puids:
                self._send_signal(PGSignals.JOIN_SAVE)
            else:
                self._send_signal(PGSignals.JOIN)
        stop = time.monotonic()

        if timeout is None:
            _ = self._pg_inout.recv()
        else:
            try:
                timeout = max(0, timeout - (stop - start))
                if self._pg_inout.poll(timeout=timeout):
                    _ = self._pg_inout.recv()
            except TimeoutError:
                raise TimeoutError("Timeout waiting for status Idle")

        # This is here just to make sure tests pass till they get updated. This is as close to a non-op as
        # possible at this point in the code
        if timeout is None:
            timeout = 100000000
        timeout = max(0, timeout - (stop - start))
        self._wait_for_status(str(Idle()), timeout=timeout)

    def kill(self, signal: signal.Signals = signal.SIGKILL, save_puids: bool = False) -> None:
        """Send a signal to each process of the process group.

        The signals SIGKILL and SIGTERM have the following side effects:

        * If the signal is SIGKILL, the group will transition to 'Idle'. It can then be reused.
        * If the group status is 'Maintain', SIGTERM will transition it to 'Running'.
        * If the group status is 'Error', SIGTERM will raise a `DragonProcessGroupError`.

        :param signal: the signal to send, defaults to signal.SIGKILL
        :type signal: signal.Signals, optional
        """

        if signal == signal.SIGTERM:
            if save_puids:
                self._send_signal(PGSignals.SHUTDOWN_SAVE)
            else:
                self._send_signal(PGSignals.SHUTDOWN)
        elif signal == signal.SIGKILL:
            if save_puids:
                self._send_signal(PGSignals.KILL_SAVE)
            else:
                self._send_signal(PGSignals.KILL)
        else:
            for puid in self.puids:
                process_kill(puid, sig=signal)

    def stop(self, save_puids=False) -> None:
        """Forcibly terminate all workers by sending `SIGKILL` from any state,
        transition to `Stop`. This also removes the group from the manager process
        and marks the end of the group life-cycle.
        """
        if save_puids:
            self._send_signal(PGSignals.STOP_SAVE)
        else:
            self._send_signal(PGSignals.STOP)

    @property
    def puids(self) -> list[int]:
        """Return the puids of the processes contained in this group.

        :return: a list of puids
        :rtype: list[int]
        """
        # Send a request to process group to make sure this is up-to-date
        if self._manager.is_alive:
            self._send_signal(PGSignals.REQ_PUIDS)

        return self._group_state.puids

    @property
    def inactive_puids(self) -> List[Tuple[int, int]]:
        """Return the group's puids and their exit codes that have exited

        :returns: a list of tuples (puid,  exit_code)
        :rtype: List[Tuple[int, int]]
        """
        # Send a request to process group to make sure this is up-to-date
        if self._manager.is_alive:
            self._send_signal(PGSignals.REQ_INACTIVE_PUIDS)

        return self._group_state.inactive_puids

    @property
    def exit_status(self) -> List[Tuple[int, int]]:
        """Return the group's puids and their exit codes that have exited

        :returns: a list of tuples (puid,  exit_code)
        :rtype: List[Tuple[int, int]]
        """

        return self.inactive_puids

    def _get_status(self):

        self._pg_inout.send((signal, None, None))

    @property
    def status(self) -> str:
        """Get the current status of the process group handled by this instance.

        :returns: current status of the group
        :rtype: str
        """
        stat = self._group_state.status

        return stat

    # Private interface
    def _send_signal(self, signal: PGSignals) -> bool:
        """Send the signal to the manager and wait for the response.
        The method guarantees completion of the signal by the manager,
        nothing more. I.e. the processes may have been started, but not
        actually executed any useful code yet.
        In case of sending IDLE, include the group state as well.
        """
        status = self.status

        if signal in self._group_state.forbidden[status] and signal not in [PGSignals.REQ_PUIDS, PGSignals.REQ_INACTIVE_PUIDS]:
            raise DragonProcessGroupError(f"Signal {str(signal)} is not a valid transition from {status}")

        if signal == PGSignals.NEW:
            payload = self._group_state
        else:
            payload = None

        sig_id = self._group_state.get_next_sig_id()
        self._pg_inout.send((signal, payload, sig_id))

        # The following are all messages that block on a receipt message
        if signal == PGSignals.START or (signal == PGSignals.JOIN and self.status == str(Idle())):
            msg = self._pg_inout.recv()

        elif signal in [PGSignals.SHUTDOWN, PGSignals.SHUTDOWN_SAVE]:
            msg = self._pg_inout.recv()

        elif signal in [PGSignals.REQ_PUIDS, PGSignals.REQ_INACTIVE_PUIDS]:
            break_loop = False
            while not break_loop:
                msg = self._pg_inout.recv()
                if msg in [PGSignals.REQ_PUIDS_RESPONSE, PGSignals.REQ_INACTIVE_PUIDS_RESPONSE, None]:
                    break_loop = True
                    if msg is None:
                        # None means the state manager has begun its exit and isn't going to return requests
                        self._state_exit_signaled = True
            return

        # Before entering the loop, make sure we haven't already been told of the state runner's exit:
        if self._state_exit_signaled:
            return

        # Otherwise, enter the loop here
        while self._group_state.last_completed_signal < sig_id:
            if self.status == str(Error()):
                if signal not in [PGSignals.KILL, PGSignals.KILL_SAVE, PGSignals.STOP, PGSignals.STOP_SAVE]:
                    if self._group_state.last_completed_signal == sig_id - 1:
                        raise DragonProcessGroupError(
                            f"Signal {str(signal)} was not successful. Group in ERROR state."
                        )
                    else:
                        raise DragonProcessGroupError(
                            f"Signal {str(signal)} cannot be completed. Group in ERROR state"
                        )

            # If the ProcessGroupState has exited because all processes have finished, the Manager will
            # have told me
            try:
                if self._pg_inout.poll():
                    msg = self._pg_inout.recv()
                    if msg is None:
                        # We end up here because the GUID has been killed
                        break
            except Exception:
                pass

            # Don't overwhelm the state object while we wait for the request to complete
            sleep(0.001)

    def _wait_for_status(self, status: str, timeout: float = 0.0) -> None:

        start = time.monotonic()
        while not self.status == status:
            dt = timeout - time.monotonic() + start

            if dt <= 0:
                raise TimeoutError(f"Timeout waiting for status {status}")
            sleep(0.1)
