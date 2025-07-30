#! /usr/bin/env python3

import sys
import threading

from . import messages
from . import util

from .host import RemoteHost, SSHHost
from .backend_connector import BackendConnector
from .exceptions import RemoteProcessError
from . import facts

import concurrent.futures
from queue import Queue, Empty
from typing import List, Optional

import logging
import time

logger = logging.getLogger(__name__)


class RemoteExecutor:
    """
    The RemoteExecutor class is responsible for establishing and managing
    connections with multiple Dragon Run Back End (DRBE) processes, in order
    to build the Dragon Run fanout.

    The RemoteExecutor class creates multiple instances of the BackendConnector,
    one for each RemoteHost in the current nodes fanout tree.
    """

    _DTBL = {}  # dispatch router, keyed by type of shepherd message

    def __init__(self, my_rank, hosts, fanout=facts.DEFAULT_FANOUT):
        self.hosts: List[str] = hosts
        self.n_hosts: int = len(hosts)
        self.fanout: int = fanout
        self.my_rank: int = my_rank
        self.msg_q: Queue = Queue()
        self.num_children: int = min(self.n_hosts, self.fanout)
        self.shutdown_event: threading.Event = threading.Event()
        self.recv_msg_thread: Optional[threading.Thread] = None
        self.num_trees_up: int = 0
        self.num_trees_destroyed: int = 0
        self.backend_tree_destroyed: threading.Event = threading.Event()
        self.ping_response_map = {}
        self.futures = {}

    def get_child_tree(self, id):
        children = range(self.fanout * (id + 1), min(self.n_hosts, self.fanout * (id + 1) + self.fanout))
        child_nodes = [self.hosts[i] for i in children]
        for node in children:
            child_nodes.extend(self.get_child_tree(node))
        return child_nodes

    def handle_msg(self, msg: messages.InfraMsg):
        """
        Add the message to our msg_q for processing by the main_thread_msg_proc.
        """
        assert self.msg_q
        self.msg_q.put(msg)

    def main_thread_msg_proc(self):
        logger.debug("++main_thread_msg_proc")
        try:
            while not self.shutdown_event.is_set():
                try:
                    msg = self.msg_q.get(timeout=facts.QUEUE_GET_TIMEOUT)
                    logger.debug("main_thread_msg_proc msg=%s", msg)
                    if type(msg) in RemoteExecutor._DTBL:
                        self._DTBL[type(msg)][0](self, msg=msg)
                    else:
                        logger.error("unable to dispatch message %s", msg)
                except Empty:
                    pass
        except Exception as exc:
            logger.error("main_thread_msg_proc - unhandled exception: %s", exc)
        finally:
            logger.debug("--main_thread_msg_proc")

    def _handleAbnormalRuntimeExit(self, msg: messages.AbnormalRuntimeExit):
        pass

    @util.route(messages.AbnormalRuntimeExit, _DTBL)
    def handleAbnormalRuntimeExit(self, msg: messages.AbnormalRuntimeExit):
        logger.debug("handleAbnormalRuntimeExit err=%s", msg.err)
        self._handleAbnormalRuntimeExit(msg)

    @util.route(messages.BackendUp, _DTBL)
    def handleBackendUp(self, msg: messages.BackendUp):
        logger.debug("handleBackendUp hostname=%s", msg.hostname)
        self.num_trees_up += 1

    @util.route(messages.RunUserApp, _DTBL)
    def handleRunUserApp(self, msg: messages.RunUserApp):
        logger.debug("handleRunUserApp command=%s", msg.command)
        for _, backend_connector in self.futures.values():
            backend_connector.msg_q.put(msg)

    def _handleFwdOutput(self, msg: messages.FwdOutput):
        pass

    @util.route(messages.FwdOutput, _DTBL)
    def handleFwdOutput(self, msg: messages.FwdOutput):
        logger.debug("handleFwdOutput hostname=%s data=%s", msg.hostname, msg.data)
        self._handleFwdOutput(msg)

    @util.route(messages.PingResponse, _DTBL)
    def handlePingResponse(self, msg: messages.PingResponse):
        logger.debug("handlePingResponse hostname=%s err=%d", msg.hostname, msg.err.value)  # type: ignore
        self.ping_response_map[msg.hostname] = (msg.err.value, time.monotonic)  # type: ignore

    def _handleUserAppExit(self, msg: messages.UserAppExit):
        pass

    @util.route(messages.UserAppExit, _DTBL)
    def handleUserAppExit(self, msg: messages.UserAppExit):
        logger.debug(
            "handleUserAppExit hostname=%s exit_code=%d",
            msg.hostname,
            msg.exit_code,
        )

        self._handleUserAppExit(msg)

    @util.route(messages.DestroyTree, _DTBL)
    def handleDestroyTree(self, msg: messages.DestroyTree):
        logger.debug("handleDestroyTree")
        for _, backend_connector in self.futures.values():
            backend_connector.msg_q.put(msg)

    @util.route(messages.TreeDestroyed, _DTBL)
    def handleTreeDestroyed(self, msg: messages.TreeDestroyed):
        logger.debug("handleTreeDestroyed")

        self.num_trees_destroyed += 1
        if self.num_trees_destroyed >= self.num_children:
            logger.debug("setting backend_tree_destroyed")
            self.backend_tree_destroyed.set()

    def connect(self):
        try:
            logger.debug("++connect")
            if not self.num_children:
                logger.debug("connect -- no children")
                return

            logger.debug("connect - starting main_thread_msg_proc")
            self.main_thread_msg_thread = threading.Thread(
                name="main_thread_msg_proc", target=self.main_thread_msg_proc, args=()
            )
            self.main_thread_msg_thread.start()

            logger.debug("connect - before ThreadPoolExecutor")
            self.executor = concurrent.futures.ThreadPoolExecutor(
                max_workers=self.num_children, thread_name_prefix="remote_executor_pool"
            )

            logger.debug("connect - submitting launch_backends")
            for i in range(self.num_children):
                child_rank: int = self.my_rank * self.fanout + i + 1
                backend_connector = BackendConnector(
                    host=SSHHost(
                        hostname=self.hosts[i],
                        rank=child_rank,
                    ),
                    child_tree=self.get_child_tree(i),
                    fanout=self.fanout,
                    remote_executor_q=self.msg_q,
                )

                self.futures[self.executor.submit(backend_connector.run)] = (
                    i,
                    backend_connector,
                )

            self.pool_watcher_thread = threading.Thread(name="pool_watcher", target=self.pool_watcher)
            self.pool_watcher_thread.start()
        except Exception as ex:
            logger.debug("connect - unhandled exception: %s", ex, exc_info=True)
            raise
        finally:
            logger.debug("--connect")

    def disconnect(self):
        logger.debug("++disconnect")

        logger.debug("Destroying Backend Tree")
        self.msg_q.put(messages.DestroyTree(util.next_tag()))

        if self.num_trees_up:
            logger.debug("Waiting for Backends to be down")
            self.backend_tree_destroyed.wait()

        logger.debug("setting the shutdown_event")
        self.shutdown_event.set()

        logger.debug("joining executor")
        self.executor.shutdown()
        logger.debug("joining pool_watcher_thread")
        self.pool_watcher_thread.join()
        logger.debug("joining main_thread_msg_thread")
        self.main_thread_msg_thread.join()

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    def pool_watcher(self):
        try:
            logger.debug("++pool_watcher")
            completed = False
            while not completed:
                try:
                    for future in concurrent.futures.as_completed(self.futures, 2):
                        i, _ = self.futures[future]
                        if i:
                            try:
                                logger.debug("pool_watcher - calling result")
                                result = future.result()
                                logger.debug("pool_watcher - result = %s", result)
                            except Exception as exc:
                                logger.debug("pool_watcher - Unhandled Exception: %d %s", i, exc)
                    completed = True
                except TimeoutError:
                    logger.debug("sending Pings")

                    for _, backend_connector in self.futures.values():
                        backend_connector.msg_q.put(messages.Ping(util.next_tag()))
        finally:
            logger.debug("--pool_watcher")


class FERemoteExecutor(RemoteExecutor):
    def __init__(self, hostnames: List[str], drun_q: Queue, fanout: int = facts.DEFAULT_FANOUT):
        super().__init__(0, hostnames, fanout)
        self.drun_q = drun_q

    def _handleAbnormalRuntimeExit(self, msg: messages.AbnormalRuntimeExit):
        logger.debug("__handleAbnormalRuntimeExit hostname=%s err=%s", msg.hostname, msg.err)
        print(msg.data)
        self.drun_q.put(msg)

    def _handleFwdOutput(self, msg: messages.FwdOutput):
        logger.debug("_handleFwdOutput hostname=%s data=%s", msg.hostname, msg.data)
        self.drun_q.put(msg)

    def _handleUserAppExit(self, msg: messages.UserAppExit):
        logger.debug("_handleUserAppExit hostname=%s exitcode=%s", msg.hostname, msg.exit_code)
        self.drun_q.put(msg)


class BERemoteExecutor(RemoteExecutor):
    def __init__(self, my_rank, hostnames, fanout, drbe_q):
        super().__init__(my_rank, hostnames, fanout)
        self.drbe_q = drbe_q

    def _handleAbnormalRuntimeExit(self, msg: messages.AbnormalRuntimeExit):
        logger.debug("__handleAbnormalRuntimeExit hostname=%s err=%s", msg.hostname, msg.err)
        self.drbe_q.put(msg)

    def _handleFwdOutput(self, msg: messages.FwdOutput):
        logger.debug("_handleFwdOutput hostname=%s data=%s", msg.hostname, msg.data)
        self.drbe_q.put(msg)

    def _handleUserAppExit(self, msg: messages.UserAppExit):
        logger.debug("_handleUserAppExit hostname=%s exitcode=%s", msg.hostname, msg.exit_code)
        self.drbe_q.put(msg)
