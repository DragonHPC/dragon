"""The Distributed Dictionary is a performant and distributed key-value store
that is available to applications and workflows written for the Dragon ecosystem.

This is Dragon's specialized implementation based on the Dragon file-like interface
which relies on Dragon Channels. The Distributed Dictionary is to work like a standard
Python dictionary except that the data that it holds may span multiple nodes and be
larger than any one node can hold.

The orchestrator's role is to coordinate any activity over the entire distributed
dictionary. For instance it coordinates creation and teardown of the distributed
dictionary. It also provides information to clients that are attaching to the
dictionary.

"""

import logging
import traceback
import socket
import cloudpickle
import random
import string
import os
import sys

from ...utils import b64decode, b64encode
from ...globalservices import channel
from ...infrastructure import facts
from ...infrastructure import parameters
from ...infrastructure import util as dutil
from ...infrastructure import messages as dmsg
from ...channels import Channel
from ...native.process import ProcessTemplate
from ...native.process_group import ProcessGroup
from ...native.machine import Node, System
from ...infrastructure.policy import Policy
from .manager import manager_proc
from ... import fli
from ...rc import DragonError
from ...dlogging.util import setup_BE_logging, DragonLoggingServices as dls

fname = f"{dls.DD}_{socket.gethostname()}_orchestrator_{str(parameters.this_process.my_puid)}.log"
setup_BE_logging(service=dls.DD, fname=fname)
log = logging.getLogger(str(dls.DD))


class Orchestrator:

    _DTBL = {}  # dispatch router, keyed by type of message

    def __init__(self, managers_per_node, n_nodes, total_mem, trace):
        try:
            # Use this connection for the managers to respond to the orchestrator
            # so clients sending to main channel are ignored until registration of
            # managers is complete.
            # This must be done first to do the SH handshake so the SH_RETURN
            # channel can safely be used here.
            self._return_strm = Channel.make_process_local()
            self._return_connector = fli.FLInterface(main_ch=self._return_strm, use_buffered_protocol=True)
            self._serialized_return_connector = b64encode(self._return_connector.serialize())

            self._stream_channel = Channel.make_process_local()

            self._default_muid = facts.default_pool_muid_from_index(parameters.this_process.index)

            self._main_channel_desc = channel.create(self._default_muid)
            self._main_channel = Channel.attach(self._main_channel_desc.sdesc)

            self._main_connector = fli.FLInterface(main_ch=self._main_channel, use_buffered_protocol=True)
            log.debug(f"here is the main connector:{b64encode(self._main_connector.serialize())}")
            self._serialized_main_connector = b64encode(self._main_connector.serialize())
            self._trace = trace
            # Bootstrapping code. This is read by the client that created the dictionary.
            print(self._serialized_main_connector, flush=True)

            self._serving = True
            self._abnormal_termination = False
            self._manager_pool_created = False
            self._received_destroy = False
            self._clients = {}
            self._next_client_id = 0
            self._next_main_manager_id = 0
            self._timeout = None
            self._drained_nodes = (
                {}
            )  # nodes that exist in previous node list but not in current node. Key as node_id value as a list of manager ids
            self._mark_drained_manager = set()
            self._pending_destroy_requests = []

            # The managers are a ProcessGroup. May have to record it differently here.
            self._managers_per_node = managers_per_node
            self._num_managers = n_nodes * self._managers_per_node
            self.mpool_size = total_mem // self._num_managers
            self._manager_nodes = [None for _ in range(self._num_managers)]
            self._manager_connections = [None for _ in range(self._num_managers)]
            self._serialized_manager_flis = ["" for _ in range(self._num_managers)]
            self._serialized_manager_return_flis = [None for _ in range(self._num_managers)]
            self._serialized_manager_pool = [None for _ in range(self._num_managers)]

            self._tag = 0
        except Exception as ex:
            tb = traceback.format_exc()
            log.debug("There was an exception in the Orchestrator: %s\n Traceback: %s", ex, tb)

    def __setstate__(self, fname):
        # read managers' serialized pool descriptors from file
        with open(fname, "r") as f:
            (self._serialized_manager_pool, self._manager_nodes) = cloudpickle.loads(b64decode(f.read()))

    def __getstate__(self):
        # write the map of managers' serialized memory pool descriptors to the file
        # self._pickle_new = "/tmp/ddict_orc_" + self._name
        with open(self._pickle_new, "w") as f:
            f.write(b64encode(cloudpickle.dumps((self._serialized_manager_pool, self._manager_nodes))))
        return fname

    def run(self):

        # bring up all managers first. Use SH_RETURN as message channel for response messages.
        self._num_managers_created = 0
        self._serving_connector = self._main_connector

        # start serving the request sent to main channel
        while self._serving:
            try:
                with self._serving_connector.recvh(timeout=self._timeout) as recvh:
                    ser_msg, _ = recvh.recv_bytes()
                # We close the receive handle right away for best performance -
                # it allows a sender to re-use its stream channel right away.
                msg = dmsg.parse(ser_msg)
                if self._trace:
                    log.info("About to process %s", msg)
                if type(msg) in self._DTBL:
                    self._DTBL[type(msg)][0](self, msg=msg)
                else:
                    self._serving = False
                    self._abnormal_termination = True
                    log.debug("The message %s was received and is not understood!", msg)
                    raise RuntimeError(f"The message {msg} was received and is not understood!")

            except Exception as ex:
                self._serving = False
                self._abnormal_termination = False
                tb = traceback.format_exc()
                log.debug("There was an exception in the Orchestrator: %s\n Traceback: %s", ex, tb)
                raise RuntimeError(f"There was an exception in the Orchestrator: {ex} \n Traceback: {tb}")

        if self._manager_pool_created:
            self._free_resources()
        else:
            self._cleanup()
        log.info("Exiting orchestrator.")

    def _cleanup(self):
        self._main_connector.destroy()
        channel.destroy(self._main_channel_desc.c_uid)
        self._stream_channel.destroy_process_local()
        self._return_connector.destroy()
        self._return_strm.destroy_process_local()

    def _free_resources(self):
        self._manager_pool.join()
        self._manager_pool.close()
        log.info("Stopped manager pool")

        for i in range(self._num_managers):
            try:
                # During failure in dictionary creation, the orchestrator might not have all managers' connections.
                self._manager_connections[i].detach()
            except Exception as ex:
                log.debug("Failed to detach manager %s connection. %s", i, ex)

        self._cleanup()

        # send destroy response if the orchestrator received destroy request
        while len(self._pending_destroy_requests) > 0:
            req_msg = self._pending_destroy_requests.pop()
            resp_msg = dmsg.DDDestroyResponse(self._tag_inc(), ref=req_msg.tag, err=DragonError.SUCCESS)
            connection = fli.FLInterface.attach(b64decode(req_msg.respFLI))
            self._send_msg(resp_msg, connection)
            connection.detach()

        # remove old pickle file in the end
        try:
            if self._restart and self._pickle_old != self._pickle_new:
                os.remove(self._pickle_old)
        except Exception as e:
            log.debug("Caught error while removing old pickle file. %s", e)

        # remove new pickle file in the end if not allow restart
        try:
            if not self._allow_restart:
                os.remove(self._pickle_new)
        except Exception as e:
            log.debug("Caught error while removing new pickle file. %s", e)

    def _send_msg(self, resp_msg, connection):
        try:
            if connection.is_buffered:
                strm = None
            else:
                strm = self._stream_channel

            with connection.sendh(stream_channel=strm, timeout=self._timeout) as sendh:
                sendh.send_bytes(resp_msg.serialize(), timeout=self._timeout)
                if self._trace:
                    log.info("Sent message %s", resp_msg)

        except Exception as e:
            tb = traceback.format_exc()
            log.debug("There is an exception in orchestrator Exception %s\n Traceback: %s\n.", e, tb)

    def _tag_inc(self):
        tag = self._tag
        self._tag += 1
        return tag

    def _clientid_inc(self):
        clientid = self._next_client_id
        self._next_client_id += 1
        return clientid

    def _get_next_main_manager_id(self):
        manager_id = self._next_main_manager_id
        self._next_main_manager_id = (self._next_main_manager_id + 1) % self._num_managers
        return manager_id

    def _return_create_failure(self, ec: DragonError, e: str):
        resp_msg = dmsg.DDCreateResponse(self._tag_inc(), ref=self._create_req_msg_tag, err=ec, errInfo=e)
        connection = fli.FLInterface.attach(b64decode(self._create_req_msg_respFLI))
        self._send_msg(resp_msg, connection)
        connection.detach()

    @dutil.route(dmsg.DDEmptyManagers, _DTBL)
    def empty_managers(self, msg: dmsg.DDEmptyManagers) -> None:
        managers = []
        for _, manager_ids in self._drained_nodes.items():
            managers.extend(manager_ids)

        resp_msg = dmsg.DDEmptyManagersResponse(
            tag=self._tag_inc(), ref=msg.tag, err=DragonError.SUCCESS, errInfo="", managers=managers
        )
        connection = fli.FLInterface.attach(b64decode(msg.respFLI))
        self._send_msg(resp_msg, connection)
        connection.detach()

    @dutil.route(dmsg.DDRegisterClient, _DTBL)
    def register_client(self, msg: dmsg.DDRegisterClient) -> None:
        resp_msg = dmsg.DDRegisterClientResponse(
            tag=self._tag_inc(),
            ref=msg.tag,
            err=DragonError.SUCCESS,
            clientID=self._clientid_inc(),
            numManagers=self._num_managers,
            timeout=self._timeout,
        )
        connection = fli.FLInterface.attach(b64decode(msg.respFLI))
        self._send_msg(resp_msg, connection)
        connection.detach()

    @dutil.route(dmsg.DDCreate, _DTBL)
    def create(self, msg: dmsg.DDCreate) -> None:
        self._err_code = DragonError.SUCCESS
        self._err_str = ""
        self._create_req_msg_tag = msg.tag
        self._create_req_msg_respFLI = msg.respFLI
        args = cloudpickle.loads(b64decode(msg.args))

        (
            self._working_set_size,
            self._wait_for_keys,
            self._wait_for_writers,
            self._policy,
            self._persist_freq,
            self._name,
            self._timeout,
            self._restart,
        ) = args

        # the dictionary is restarted with previous manager pool
        # open the file and read the map with manager id as keys
        # and serialized manager memory pool descriptor as values
        # The serialized pool descriptors are sent along with
        # ProcessGroup create because managers need those to create
        # main FLI, managers then register to orchestrator with
        # their main FLIs, return FLIs, and serialized pool descriptor.
        if self._restart:
            # client must provide name of the file to read managers' serialized pool descriptor.
            # return failure if the name is not provided
            if not self._name:
                err_str = "Name was not specified, cannot restart dictionary."
                log.debug(err_str)
                self._return_create_failure(DragonError.FAILURE, err_str)
                self._serving = False
                return
            try:
                # unpickle orchestrator
                self._pickle_old = "/tmp/ddict_orc_" + self._name
                self.__setstate__(self._pickle_old)
            except Exception as ex:
                log.debug("Exception ocurred while calling __setstate__: %s", ex)
                self._return_create_failure(DragonError.FAILURE, str(ex))
                self._serving = False
                return

            # check that number of manager matches
            try:
                assert (
                    len(self._serialized_manager_pool) == self._num_managers
                ), f"DDict number of managers mismatch, expect {self._num_managers}, got {len(self._serialized_manager_pool)}"
            except AssertionError as e:
                log.debug("Assertion failed in orchestrator for number of managers: %s", e)
                self._return_create_failure(DragonError.FAILURE, str(e))
                self._serving = False
                return

        # generate a random name if name was not provided
        if not self._name:  # TODO: name - time and date
            random.seed()
            self._name = "".join(random.choices(string.ascii_lowercase + string.digits, k=10))
            args = (
                self._working_set_size,
                self._wait_for_keys,
                self._wait_for_writers,
                self._policy,
                self._persist_freq,
                self._name,
                self._timeout,
                self._restart,
            )
        self._pickle_new = "/tmp/ddict_orc_" + self._name

        # create managers
        self._manager_pool = ProcessGroup(restart=False)

        # If policies is given AND we are restarting, we don't use the policy given from the user.
        if self._restart:
            log.debug("restart without policy")
            node_alloc = System()
            current_nodes = set(node_alloc.nodes)  # current node list
            same_nodes = set()  # nodes that exist in both current nodes list and previous node list
            for manager_id in range(self._num_managers):
                ser_pool_descr = self._serialized_manager_pool[manager_id]
                node = cloudpickle.loads(b64decode(self._manager_nodes[manager_id]))
                node_id = node.h_uid
                # If node is not in current node allocation, keep the manager ID to create manager on new node later.
                if node_id not in current_nodes:
                    if node_id not in self._drained_nodes:
                        self._drained_nodes[node_id] = []
                    self._drained_nodes[node_id].append(manager_id)
                else:  # node exists in previous allocation - restart manager on the node
                    same_nodes.add(node_id)
                    policy = Policy(placement=Policy.Placement.HOST_NAME, host_name=node.hostname)
                    template = ProcessTemplate(
                        manager_proc,
                        args=(
                            self.mpool_size,
                            self._serialized_return_connector,
                            self._serialized_main_connector,
                            self._trace,
                            args,
                            manager_id,
                            ser_pool_descr,
                        ),
                        policy=policy,
                    )
                    self._manager_pool.add_process(nproc=1, template=template)

            # Create manager on new nodes
            new_nodes = current_nodes - same_nodes
            log.debug("current_nodes: %s", current_nodes)
            log.debug("same_nodes: %s", same_nodes)
            log.debug("self._drained_nodes: %s", self._drained_nodes)
            log.debug("new_nodes: %s", new_nodes)

            if len(new_nodes) < len(self._drained_nodes):
                err_info = "Number of new nodes is less than number of drained nodes."
                self._return_create_failure(DragonError.INVALID_ARGUMENT, err_info)
                self._serving = False
                return

            for _, manager_ids in self._drained_nodes.items():
                node_id = new_nodes.pop()
                for manager_id in manager_ids:
                    node = Node(node_id)
                    policy = Policy(placement=Policy.Placement.HOST_NAME, host_name=node.hostname)
                    template = ProcessTemplate(
                        manager_proc,
                        args=(
                            self.mpool_size,
                            self._serialized_return_connector,
                            self._serialized_main_connector,
                            self._trace,
                            args,
                            manager_id,
                            None,
                        ),
                        policy=policy,
                    )
                    self._manager_pool.add_process(nproc=1, template=template)
        else:
            if isinstance(self._policy, list):
                log.debug("policy_list: %s", self._policy)
                self._manager_pool = ProcessGroup(restart=False)
                manager_id = 0
                for policy in self._policy:
                    # If policy is a list, managers per node is the same as managers per policy
                    for _ in range(self._managers_per_node):
                        template = ProcessTemplate(
                            manager_proc,
                            args=(
                                self.mpool_size,
                                self._serialized_return_connector,
                                self._serialized_main_connector,
                                self._trace,
                                args,
                                manager_id,
                                None,
                            ),
                            policy=policy,
                        )
                        self._manager_pool.add_process(nproc=1, template=template)
                        manager_id += 1
            else:
                log.debug("policy: %s", self._policy)
                for manager_id in range(self._num_managers):
                    template = ProcessTemplate(
                        manager_proc,
                        args=(
                            self.mpool_size,
                            self._serialized_return_connector,
                            self._serialized_main_connector,
                            self._trace,
                            args,
                            manager_id,
                            None,
                        ),
                        policy=self._policy,
                    )
                    self._manager_pool.add_process(nproc=1, template=template)

        self._manager_pool.init()
        self._manager_pool.start()
        self._manager_pool_created = True
        self._serving_connector = self._return_connector

    @dutil.route(dmsg.DDRegisterManager, _DTBL)
    def register_manager(self, msg: dmsg.DDRegisterManager) -> None:
        try:

            self._num_managers_created += 1
            if msg.err != DragonError.SUCCESS:
                self._err_code = msg.err
                self._err_str = msg.errInfo

            if msg.err == DragonError.SUCCESS:
                self._manager_nodes[msg.managerID] = b64encode(cloudpickle.dumps(Node(msg.hostID)))
                self._manager_connections[msg.managerID] = fli.FLInterface.attach(b64decode(msg.mainFLI))
                self._serialized_manager_flis[msg.managerID] = msg.mainFLI
                self._serialized_manager_pool[msg.managerID] = msg.poolSdesc

            self._serialized_manager_return_flis[msg.managerID] = (msg.respFLI, msg.tag)

            if self._num_managers_created == self._num_managers:
                # write manager's serialized pool descriptor to file everytime
                self.__getstate__()

                # send response message to all managers
                for i, (m, tag) in enumerate(self._serialized_manager_return_flis):
                    resp_msg = dmsg.DDRegisterManagerResponse(
                        self._tag_inc(),
                        ref=tag,
                        err=self._err_code,
                        errInfo=self._err_str,
                        managers=self._serialized_manager_flis,
                        managerNodes=self._manager_nodes,
                    )
                    connection = fli.FLInterface.attach(b64decode(m))
                    self._send_msg(resp_msg, connection)
                    connection.detach()
                # send DD create response to client
                resp_msg = dmsg.DDCreateResponse(
                    self._tag_inc(), ref=self._create_req_msg_tag, err=self._err_code, errInfo=self._err_str
                )
                connection = fli.FLInterface.attach(b64decode(self._create_req_msg_respFLI))
                self._send_msg(resp_msg, connection)
                connection.detach()

                if self._err_code == DragonError.SUCCESS:
                    # dictionary created - switch to main connector to serve other requests
                    self._serving_connector = self._main_connector
                else:
                    # failed to create dictionary - stop serving and free all resources
                    self._serving = False
        except Exception as ex:
            tb = traceback.format_exc()
            log.debug("There was an exception while registering managers: %s\n Traceback: %s", ex, tb)
            raise RuntimeError(f"There was an exception while registering managers: {ex} \n Traceback: {tb}")

    @dutil.route(dmsg.DDRandomManager, _DTBL)
    def get_random_manager(self, msg: dmsg.DDRandomManager) -> None:
        managerFLI = self._serialized_manager_flis[self._get_next_main_manager_id()]
        resp_msg = dmsg.DDRandomManagerResponse(
            self._tag_inc(), ref=msg.tag, err=DragonError.SUCCESS, manager=managerFLI
        )
        connection = fli.FLInterface.attach(b64decode(msg.respFLI))
        self._send_msg(resp_msg, connection)
        connection.detach()

    @dutil.route(dmsg.DDDestroy, _DTBL)
    def destroy_dict(self, msg: dmsg.DDDestroy) -> None:
        self._pending_destroy_requests.append(msg)
        if self._received_destroy:
            return
        self._received_destroy = True
        self._serving_connector = self._return_connector
        self._allow_restart = msg.allowRestart
        self._destroyed_managers = 0
        self._destroy_resp_ref = msg.tag
        for m in self._manager_connections:
            req_msg = dmsg.DDDestroyManager(
                tag=self._tag_inc(), allowRestart=self._allow_restart, respFLI=self._serialized_return_connector
            )
            self._send_msg(req_msg, m)

    @dutil.route(dmsg.DDDestroyManagerResponse, _DTBL)
    def destroy_manager_response(self, msg: dmsg.DDDestroyManagerResponse) -> None:
        if msg.err != DragonError.SUCCESS:
            raise Exception(f"{msg.err} Failed to destroy manager!")
        self._destroyed_managers += 1
        if self._destroyed_managers == self._num_managers:
            self._serving = False

    @dutil.route(dmsg.DDGetManagers, _DTBL)
    def get_managers(self, msg: dmsg.DDGetManagers) -> None:
        empty_managers = [False] * self._num_managers
        for manager_id in self._drained_nodes.values():
            empty_managers[manager_id] = True

        for manager_id in self._mark_drained_manager:
            empty_managers[manager_id] = True

        resp_msg = dmsg.DDGetManagersResponse(
            self._tag_inc(),
            ref=msg.tag,
            err=DragonError.SUCCESS,
            errInfo="",
            emptyManagers=empty_managers,
            managers=self._serialized_manager_flis,
        )
        connection = fli.FLInterface.attach(b64decode(msg.respFLI))
        self._send_msg(resp_msg, connection)
        connection.detach()

    @dutil.route(dmsg.DDMarkDrainedManagers, _DTBL)
    def mark_drained_manager(self, msg: dmsg.DDMarkDrainedManagers) -> None:
        for id in msg.managerIDs:
            self._mark_drained_manager.add(id)
        resp_msg = dmsg.DDMarkDrainedManagersResponse(self._tag_inc(), ref=msg.tag, err=DragonError.SUCCESS, errInfo="")
        connection = fli.FLInterface.attach(b64decode(msg.respFLI))
        self._send_msg(resp_msg, connection)
        connection.detach()

    @dutil.route(dmsg.DDUnmarkDrainedManagers, _DTBL)
    def unmark_drained_manager(self, msg: dmsg.DDUnmarkDrainedManagers) -> None:
        for id in msg.managerIDs:
            if id in self._mark_drained_manager:
                self._mark_drained_manager.remove(id)
        resp_msg = dmsg.DDUnmarkDrainedManagersResponse(
            self._tag_inc(), ref=msg.tag, err=DragonError.SUCCESS, errInfo=""
        )
        connection = fli.FLInterface.attach(b64decode(msg.respFLI))
        self._send_msg(resp_msg, connection)
        connection.detach()

    @dutil.route(dmsg.DDGetMetaData, _DTBL)
    def get_meta_data(self, msg: dmsg.DDGetMetaData) -> None:
        resp_msg = dmsg.DDGetMetaDataResponse(
            self._tag_inc(),
            ref=msg.tag,
            err=DragonError.SUCCESS,
            errInfo="",
            serializedDdict=self._serialized_main_connector,
            numManagers=self._num_managers,
        )
        connection = fli.FLInterface.attach(b64decode(msg.respFLI))
        self._send_msg(resp_msg, connection)
        connection.detach()

    @dutil.route(dmsg.DDManagerNodes, _DTBL)
    def manager_nodes(self, msg: dmsg.DDManagerNodes) -> None:
        manager_huids = [cloudpickle.loads(b64decode(node)).h_uid for node in self._manager_nodes]
        resp_msg = dmsg.DDManagerNodesResponse(
            self._tag_inc(), ref=msg.tag, err=DragonError.SUCCESS, errInfo="", huids=manager_huids
        )
        connection = fli.FLInterface.attach(b64decode(msg.respFLI))
        self._send_msg(resp_msg, connection)
        connection.detach()


def start(managers_per_node: int, n_nodes: int, total_mem: int, trace: bool):
    try:
        log.debug("Initing Orchestrator")
        orc = Orchestrator(managers_per_node, n_nodes, total_mem, trace)
        orc.run()
    except Exception as ex:
        tb = traceback.format_exc()
        log.debug("There was an exception initing the orchestrator: %s\n Traceback: %s", ex, tb)
