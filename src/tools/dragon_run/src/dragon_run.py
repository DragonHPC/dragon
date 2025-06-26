import errno
import logging
import selectors
import socket
import sys
import threading

from queue import Queue, Empty
from multiprocessing import Event
from multiprocessing.synchronize import Event as EventClass
from typing import Dict, Optional, BinaryIO, cast
from dataclasses import dataclass, field

from . import facts
from .local_executor import local_executor
from .remote_executor import RemoteExecutor, FERemoteExecutor, BERemoteExecutor
from .exceptions import DragonRunBaseException
from . import messages
from . import util


logger = logging.getLogger(__name__)


@dataclass
class PendingRequest:
    tag: int
    expected_responses: int
    received_responses: int = 0
    responses: Dict[str, int] = field(default_factory=dict)
    event: EventClass = field(default_factory=Event)


class DragonRunFE:
    """
    The frontend DragonRun (drun) process, communicates over SSH to its children's stdin/stdout files.

       drun
        /\
    drbe  drbe

    """

    _DTBL = {}

    def __init__(self, children, fanout):
        self.children = children
        self.fanout = fanout

        self.msg_q: Queue = Queue()
        self.shutdown_event: EventClass = Event()
        self.main_thread: Optional[threading.Thread] = None

        self.remote_executor = None
        if self.children:
            self.remote_executor = FERemoteExecutor(
                hostnames=self.children, drun_q=self.msg_q, fanout=self.fanout
            )

        self.local_executor_thread: Optional[threading.Thread] = None
        self.pending: dict[int, PendingRequest] = dict()  # key = tag, value = Event

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def start(self):
        logger.info("++DragonRunFE.start")
        try:
            if self.remote_executor:
                self.remote_executor.connect()
            self.main_thread = threading.Thread(name="DragonRunFE MainThread", target=self.run)
            self.main_thread.start()
        finally:
            logger.info("--DragonRunFE.start")

    def stop(self):
        if self.main_thread:
            self.shutdown_event.set()
            self.main_thread.join()
            if self.remote_executor:
                self.remote_executor.disconnect()

    def run_user_app(self, command, env=None, cwd=None, exec_on_fe=False):
        logger.info("++run_user_app command=%s", command)

        tag: int = util.next_tag()
        expected_responses = 0

        assert any([self.remote_executor, exec_on_fe])

        num_ranks = len(self.children) if self.children else 0
        if exec_on_fe:
            num_ranks += 1

        logger.info("run_user_app - 1")
        if self.remote_executor:
            logger.info("run_user_app - 2")
            expected_responses += len(self.children)
            msg = messages.RunUserApp(tag=tag, command=command, num_ranks=num_ranks, env=env, cwd=cwd)
            logger.info("run_user_app msg=%s", msg)
            self.remote_executor.handle_msg(msg)

        logger.info("run_user_app - 3")
        if exec_on_fe:
            expected_responses += 1
            self.local_executor_thread = threading.Thread(
                name=f"LocalExecutor {command}",
                target=local_executor,
                args=(command, env, cwd, tag, 0, num_ranks, self.msg_q),  # Always rank 0
            )
            self.local_executor_thread.start()

        logger.info("run_user_app - 4")
        pending_req = PendingRequest(
            tag=tag,
            expected_responses=expected_responses,
        )
        self.pending[tag] = pending_req
        logger.info("run_user_app - 5")
        pending_req.event.wait()

    @util.route(messages.AbnormalRuntimeExit, _DTBL)
    def handleAbnormalRuntimeExit(self, msg: messages.AbnormalRuntimeExit):
        pass

    @util.route(messages.FwdOutput, _DTBL)
    def handleFwdOutput(self, msg: messages.FwdOutput):
        out_h = {
            messages.FwdOutput.FDNum.STDOUT.value: sys.stdout,
            messages.FwdOutput.FDNum.STDERR.value: sys.stderr,
        }[msg.fd_num]

        lines = str(msg.data).splitlines()
        for line in lines:
            print(f"{msg.fd_num} ({msg.hostname}) {line}", file=out_h, flush=True)

    @util.route(messages.UserAppExit, _DTBL)
    def handleUserAppExit(self, msg: messages.UserAppExit):
        try:
            logger.debug(
                "++handle_user_exit - tag=%d, ref=%d, hostname=%s, exit_code=%d",
                msg.tag,
                msg.ref,
                msg.hostname,
                msg.exit_code,
            )

            ref = msg.ref
            assert ref in self.pending
            pending = self.pending[ref]
            pending.received_responses += 1
            pending.responses[msg.hostname] = msg.exit_code
            if pending.received_responses >= pending.expected_responses:
                pending.event.set()
        finally:
            pass

    def run(self):
        logger.info("++run")

        try:
            if self.remote_executor:
                self.remote_executor.connect()

            while not self.shutdown_event.is_set():
                logger.info("run -- loop")
                try:
                    msg = self.msg_q.get(timeout=facts.QUEUE_GET_TIMEOUT)
                    logger.debug("main_thread_msg_proc msg=%s", msg)
                    if type(msg) in DragonRunFE._DTBL:
                        self._DTBL[type(msg)][0](self, msg=msg)
                    else:
                        logger.error("unable to dispatch message %s", msg)
                except Empty:
                    pass
        finally:
            if self.remote_executor:
                self.remote_executor.disconnect()
            logger.info("--run")


class DragonRunBE:
    """
    The backend drbe process communicates with both its parent and child processes. Messages sent to/from
    its parent are sent over sys.stdout/sys.stdin. Messages sent to its children are sent over SSH, to its
    children's stdin/stdout.

         /
       drbe
        /\
     ch1  ch2
    """

    _DTBL = {}  # dispatch router for messages being sent to our local_executor

    def __init__(self, my_rank):
        self.my_rank = my_rank
        self.my_hostname = socket.gethostname()

        self.shutdown_event: EventClass = Event()

        self.send_msg_q: Queue = Queue()
        self.recv_msg_q: Queue = Queue()

        self.send_msg_thread: Optional[threading.Thread] = None
        self.recv_msg_thread: Optional[threading.Thread] = None

        self.remote_executor: Optional[RemoteExecutor] = None
        self.local_executor_thread: Optional[threading.Thread] = None

    def start(self):

        assert self.send_msg_thread == None
        assert self.recv_msg_thread == None

        self.shutdown_event.clear()
        self.send_msg_thread = threading.Thread(
            name="be_send_msg_to_stdout_proc", target=self.be_send_msg_to_stdout_proc, args=[]
        )
        self.send_msg_thread.start()

        self.recv_msg_thread = threading.Thread(
            name="be_recv_msg_from_stdin_proc", target=self.be_recv_msg_from_stdin_proc, args=[]
        )
        self.recv_msg_thread.start()

    def stop(self):
        if self.recv_msg_thread:
            logger.debug("Joining recv_msg_thread")
            self.recv_msg_thread.join()
        if self.send_msg_thread:
            logger.debug("Joining send_msg_thread")
            self.send_msg_thread.join()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def forward_to_parent(self, msg):
        if self.send_msg_q:
            self.send_msg_q.put(msg)

    def be_send_msg_to_stdout_proc(self, timeout=facts.QUEUE_GET_TIMEOUT):
        logger.debug("++be_send_msg_to_stdout_proc")

        assert self.shutdown_event
        assert self.send_msg_q

        try:
            while not self.shutdown_event.is_set() or not self.send_msg_q.empty():
                try:
                    msg = self.send_msg_q.get(timeout=timeout)

                    logger.debug("sending msg=%s", msg)

                    sys.stdout.buffer.write(msg.uncompressed_serialize())
                    sys.stdout.buffer.write(b"\n")
                    sys.stdout.buffer.flush()
                except Empty:
                    pass
                except Exception as exc:
                    logger.error("Unhandled Exception getting msg from send_msg_q %s", exc)
                    raise exc
        finally:
            logger.debug("--be_send_msg_to_stdout_proc")

    def be_stdin_select_msg_handler(self, fileh, recv_msg_q: Queue):
        try:
            line = fileh.readline()
            if line:
                logger.debug("recv_msg line=%s", line)
                msg = messages.parse(line.decode("utf-8").strip())
                logger.debug("recv_msg msg=%s", msg)
                recv_msg_q.put(msg)
        except socket.error as err:
            if isinstance(err.args, tuple):
                if err.args[0] == errno.EPIPE:
                    # remote peer disconnected
                    logger.debug("Detected remote disconnect")
                else:
                    logger.error("Unhandled socket error %s", err.args)
            else:
                logger.error("unhandled socket exception %s", err)
        except Exception as exc:
            logger.error("unhandled exception reading msg from stdin. %s", exc)
            raise exc

    def be_recv_msg_from_stdin_proc(self, timeout=0):
        logger.debug("++be_recv_msg_from_stdin_proc")
        try:
            selector = selectors.DefaultSelector()
            selector.register(sys.stdin.buffer, selectors.EVENT_READ, self.be_stdin_select_msg_handler)

            # Event loop
            while not self.shutdown_event.is_set():
                events = selector.select(timeout=timeout)
                for key, _ in events:
                    inf: BinaryIO = cast(BinaryIO, key.fileobj)
                    if not inf.closed:
                        callback = key.data
                        callback(key.fileobj, self.recv_msg_q)
                    else:
                        logger.debug("recv_msg_proc recv file obj is closed")
                        self.shutdown_event.set()
        finally:
            logger.debug("--be_recv_msg_from_stdin_proc")

    @util.route(messages.HelloSynAck, _DTBL)
    def handleSynAck(self, msg: messages.HelloSynAck):
        logger.debug("received SynAck, sending Ack")
        return messages.HelloAck(util.next_tag())

    @util.route(messages.CreateTree, _DTBL)
    def handleCreateTree(self, msg: messages.CreateTree):
        if msg.children:
            # TODO: Add exception handling about BERemoteExecutor. RemoteProcessError
            self.remote_executor = BERemoteExecutor(
                self.my_rank, msg.children, msg.fanout, self.send_msg_q
            )
            self.remote_executor.connect()

        return messages.BackendUp(util.next_tag(), self.my_hostname)

    @util.route(messages.RunUserApp, _DTBL)
    def handleRunUserApp(self, msg: messages.RunUserApp):
        logger.debug("spawning local_executor user_command=%s", msg.command)

        if self.remote_executor:
            self.remote_executor.handle_msg(msg)

        self.local_executor_thread = threading.Thread(
            name=f"LocalExecutor {msg.tag}:{msg.command}",
            target=local_executor,
            args=(msg.command, msg.env, msg.cwd, msg.tag, self.my_rank, msg.num_ranks, self.recv_msg_q),
        )
        self.local_executor_thread.start()

    @util.route(messages.FwdOutput, _DTBL)
    def handleFwdOutput(self, msg: messages.FwdOutput):
        logger.debug("Forwarding output %s", msg)
        self.forward_to_parent(msg)

    @util.route(messages.Ping, _DTBL)
    def handlePing(self, msg: messages.Ping):
        logger.debug("++handlePing")
        try:
            self.forward_to_parent(
                messages.PingResponse(
                    util.next_tag(),
                    msg.tag,
                    messages.PingResponse.Errors.OK.value,
                    self.my_hostname,
                )
            )
        except Exception as ex:
            raise

    @util.route(messages.UserAppExit, _DTBL)
    def handleUserAppExit(self, msg: messages.UserAppExit):
        logger.debug("++Received UserAppExit")

        if self.local_executor_thread:
            self.local_executor_thread.join()
            self.local_executor_thread = None

        self.forward_to_parent(msg)
        logger.debug("--Received UserAppExit")

    @util.route(messages.DestroyTree, _DTBL)
    def handleDestroyTree(self, msg: messages.DestroyTree):
        logger.debug("handleDestroyTree")

        if self.remote_executor:
            self.remote_executor.disconnect()

        logger.debug("handleDestroyTree - shutdown_event.set")
        self.shutdown_event.set()
        self.forward_to_parent(messages.TreeDestroyed(util.next_tag()))

    @util.route(messages.TreeDestroyed, _DTBL)
    def handleTreeDestroyed(self, msg: messages.TreeDestroyed):
        logger.debug("handleTreeDestroyed")

        logger.debug("handleTreeDestroyed - shutdown_event.set")
        self.shutdown_event.set()

        self.forward_to_parent(messages.TreeDestroyed(util.next_tag()))

    def run(self):
        logger.debug("++run_backend")

        assert self.recv_msg_q
        assert self.send_msg_q

        self.forward_to_parent(messages.HelloSyn(util.next_tag()))

        while not self.shutdown_event.is_set():
            try:
                msg = self.recv_msg_q.get(timeout=facts.QUEUE_GET_TIMEOUT)
                logger.debug("run_backend - dispatching %s", msg)
                if type(msg) in DragonRunBE._DTBL:
                    resp_msg = self._DTBL[type(msg)][0](self, msg=msg)
                    if resp_msg is not None:
                        self.forward_to_parent(resp_msg)
                else:
                    logger.error("unable to dispatch message %s", msg)
            except Empty:
                pass
        logger.debug("--run_backend")
