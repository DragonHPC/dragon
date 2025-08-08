import threading

from queue import Queue, Empty
from .exceptions import RemoteProcessError

from . import facts
from . import messages
from . import util
from .host import RemoteHost

import logging

logger = logging.getLogger(__name__)


DRAGON_RUN_BE = "drbe"


class BackendConnector:
    """
    Multiple instances of the BackendConnector are created by the
    RemoteExecutor, one for each RemoteHost in the current nodes fanout tree.
    The BackendConnector class has the following responsibilities:

    1. Establish a connection with a given remote host in order to run an
       instance of the Dragon Run Back End (DRBE) process.

    2. Once the remote DRBE process has been started, establish and maintain
       the communication protocol with the remote DRBE process, including
       initial processing of messages received from the remote DRBE.

    3. Pass messages received from the remote DRBE process to the
       RemoteExecutor.
    """

    _DTBL = {}  # dispatch router, keyed by type of shepherd message

    def __init__(self, host, child_tree, fanout, remote_executor_q):
        self.host: RemoteHost = host
        self.hostname = host.hostname
        self.child_tree = child_tree
        self.fanout = fanout
        self.msg_q: Queue = Queue()
        self.remote_executor_q: Queue = remote_executor_q
        self.shutdown_event: threading.Event = threading.Event()
        self.host_recv_thread = None

    @util.route(messages.FwdOutput, _DTBL)
    def handleFwdOutput(self, msg: messages.FwdOutput):
        logger.debug("++host=%s handleFwdOutput", self.hostname)
        self.remote_executor_q.put(msg)

    @util.route(messages.RunUserApp, _DTBL)
    def handleRunUserApp(self, msg: messages.RunUserApp):
        logger.debug("%s handleRunUserApp - Sending RunUserApp", self.hostname)
        self.host.send_message(msg)

    @util.route(messages.Ping, _DTBL)
    def handlePing(self, msg: messages.Ping):
        logger.debug("%s handlePing", self.hostname)
        self.host.send_message(msg)

    @util.route(messages.PingResponse, _DTBL)
    def handlePingResponse(self, msg: messages.PingResponse):
        logger.debug("%s handlePingResponse err=%d", self.hostname, msg.err.value) # type: ignore
        self.remote_executor_q.put(msg)

    @util.route(messages.UserAppExit, _DTBL)
    def handleUserAppExit(self, msg: messages.UserAppExit):
        logger.debug("++host=%s handleUserAppExit", self.hostname)
        self.remote_executor_q.put(msg)

    @util.route(messages.DestroyTree, _DTBL)
    def handleDestroyTree(self, msg: messages.DestroyTree):
        logger.debug("%s handleDestroyTree", self.hostname)
        self.host.send_message(msg)

    @util.route(messages.TreeDestroyed, _DTBL)
    def handleTreeDestroyed(self, msg: messages.TreeDestroyed):
        logger.debug("++host=%s handleTreeDestroyed", self.hostname)
        self.shutdown_event.set()
        self.remote_executor_q.put(msg)

    def abnormalExit(self, ex):
        logger.debug("++host=%s abnormalExit", self.hostname)
        msg = messages.AbnormalRuntimeExit(util.next_tag(), hostname=self.hostname, data=str(ex))
        self.shutdown_event.set()
        self.remote_executor_q.put(msg)

    def host_recv_proc(self):
        try:
            logger.debug("++host=%s host_recv_thread", self.hostname)
            while not self.shutdown_event.is_set():
                try:
                    msg = self.host.recv_message()
                    if not msg:
                        break

                    logger.debug("host=%s host_recv_proc msg=%s", self.hostname, msg)
                    self.msg_q.put(msg)
                except Empty:
                    pass
        except RemoteProcessError as rpe:
            logger.info("RemoteProcessError %s ExitCode=%d", rpe.args, rpe.exit_code)
            raise
        except Exception as ex:
            logger.error("host=%s host_recv_thread Unhandled Exception: %s", self.hostname, ex)
        finally:
            logger.debug("--host=%s host_recv_thread", self.hostname)

    def connect(self):
        try:
            self.host.connect()
            self.host.execute_command(DRAGON_RUN_BE)

            # There may be text that the SSH server sends that we need to "get past"
            while True:
                logger.debug("host=%s connect - waiting for Syn", self.hostname)
                msg = self.host.recv_message([messages.HelloSyn.tcv()])
                logger.debug("host=%s connect - recvd %s", self.hostname, msg)
                break

            logger.debug("host=%s connect - sending SynAck", self.hostname)
            self.host.send_message(messages.HelloSynAck(0))

            logger.debug("host=%s connect - waiting for Ack", self.hostname)
            msg = self.host.recv_message([messages.HelloAck.tcv()])
            logger.debug("host=%s connect - recvd %s", self.hostname, msg)

            logger.debug("host=%s connect - sending CreateTree", self.hostname)
            self.host.send_message(
                messages.CreateTree(
                    util.next_tag(),
                    children=self.child_tree,
                    fanout=self.fanout
                )
            )

            logger.debug("%s connect - waiting for BackendUp", self.hostname)
            msg = self.host.recv_message([messages.BackendUp.tcv()])
            logger.debug("host=%s connect - recvd %s", self.hostname, msg)

            # Forward the BackendUp message to the remote_executor
            self.remote_executor_q.put(msg)

            self.host_recv_thread = threading.Thread(
                name="host_recv_proc",
                target=self.host_recv_proc
            )
            self.host_recv_thread.start()

        except RemoteProcessError as rpe:
            logger.error("Remote Process Error %s, exit_code=%d", rpe, rpe.exit_code)
            raise
        except Exception as ex:
            logger.error("Connect Unhandled exception: %s %s", type(ex), ex)

    def disconnect(self):
        logger.debug("++host=%s disconnect", self.hostname)
        if self.host_recv_thread:
            self.host_recv_thread.join()
        self.host.disconnect()
        logger.debug("host=%s disconnect", self.hostname)

    def run(self):
        try:
            logger.debug("++host=%s run", self.hostname)
            self.connect()
            while not self.shutdown_event.is_set():
                try:
                    msg = self.msg_q.get(timeout=facts.QUEUE_GET_TIMEOUT)
                    logger.debug("host=%s run msg=%s", self.hostname, msg)
                    if type(msg) in BackendConnector._DTBL:
                        self._DTBL[type(msg)][0](self, msg=msg)
                    else:
                        logger.error("unable to dispatch message %s", msg)
                except Empty:
                    pass
            return 0
        except RemoteProcessError as rpe:
            logger.debug("host=%s run RemoteProcessError: %s", self.hostname, rpe)
            self.abnormalExit(rpe)
        except Exception as ex:
            logger.debug("host=%s run Unhandled Exception: %s", self.hostname, ex)
            self.abnormalExit(ex)
            return 1
        finally:
            self.disconnect()
            logger.debug("--host=%s run", self.hostname)
