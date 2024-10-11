""" The Distributed Dictionary is a performant and distributed key-value store
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

from ...utils import b64decode, b64encode
from ...globalservices import channel
from ...infrastructure import facts
from ...infrastructure import parameters
from ...infrastructure import util as dutil
from ...infrastructure import messages as dmsg
from ...channels import Channel
from ...native.process import ProcessTemplate
from ...native.process_group import ProcessGroup
from ...native.machine import Node
from ...infrastructure.policy import Policy
from .manager import manager_proc
from ... import fli
from ...rc import DragonError
from ...dlogging.util import setup_BE_logging, DragonLoggingServices as dls, deferred_flog

fname = f'{dls.DD}_{socket.gethostname()}_orchestrator_{str(parameters.this_process.my_puid)}.log'
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
            return_channel = Channel.make_process_local()
            self._return_connector = fli.FLInterface(main_ch=return_channel, use_buffered_protocol=True)
            self._serialized_return_connector = b64encode(self._return_connector.serialize())

            self._stream_channel = Channel.make_process_local()

            self._default_muid = facts.default_pool_muid_from_index(parameters.this_process.index)

            self._main_channel_desc = channel.create(self._default_muid)
            self._main_channel = Channel.attach(self._main_channel_desc.sdesc)

            self._main_connector = fli.FLInterface(main_ch=self._main_channel, use_buffered_protocol=True)
            self._serialized_main_connector = b64encode(self._main_connector.serialize())
            self._trace = trace
            # Bootstrapping code. This is read by the client that created the dictionary.
            print(self._serialized_main_connector, flush=True)

            self._serving = True
            self._abnormal_termination = False
            self._clients = {}
            self._next_client_id = 0
            self._next_main_manager_id = 0
            self._timeout = None

            # The managers are a ProcessGroup. May have to record it differently here.
            self._managers_per_node = managers_per_node
            self._num_managers = (n_nodes * self._managers_per_node)
            self.mpool_size = total_mem // self._num_managers
            self._manager_connections = []
            self._serialized_manager_flis = []
            self._serialized_manager_return_flis = []
            self._manager_nodes = []

            self._tag = 0
        except Exception as ex:
            tb = traceback.format_exc()
            log.debug(f'There was an exception in the Orchestrator: {ex} \n Traceback: {tb}')

    def run(self):

        # bring up all managers first. Use SH_RETURN as message channel for response messages.
        self._num_managers_created = 0
        self._serving_connector = self._main_connector

        # start serving the request sent to main channel
        while self._serving:
            try:
                with self._serving_connector.recvh(timeout=self._timeout) as recvh:
                    ser_msg, hint = recvh.recv_bytes()
                    msg = dmsg.parse(ser_msg)
                    if self._trace:
                        log.info(f'About to process {msg}')
                    if type(msg) in self._DTBL:
                        self._DTBL[type(msg)][0](self, msg=msg)
                    else:
                        self._serving = False
                        self._abnormal_termination = True
                        log.debug(f'The message {msg} was received and is not understood!')
                        raise RuntimeError(f'The message {msg} was received and is not understood!')

            except Exception as ex:
                self._serving = False
                self._abnormal_termination = False
                tb = traceback.format_exc()
                log.debug(f'There was an exception in the Orchestrator: {ex} \n Traceback: {tb}')
                raise RuntimeError(f'There was an exception in the Orchestrator: {ex} \n Traceback: {tb}')

        self._free_resources()
        log.info(f'Exiting orchestrator.')

    def _free_resources(self):
        self._manager_pool.join()
        self._manager_pool.close()
        log.info('Stopped manager pool')

        for i in range(self._num_managers):
            self._manager_connections[i].detach()

        self._main_connector.destroy()
        channel.destroy(self._main_channel_desc.c_uid)

        resp_msg = dmsg.DDDestroyResponse(self._tag_inc(), ref=self._destroying_tag, err=DragonError.SUCCESS)
        connection = fli.FLInterface.attach(b64decode(self._destroying_client_fli))
        self._send_msg(resp_msg, connection)
        connection.detach()

    def _send_msg(self, resp_msg, connection):
        try:
            if connection.is_buffered:
                strm = None
            else:
                strm = self._stream_channel

            with connection.sendh(stream_channel=strm, timeout=self._timeout) as sendh:
                sendh.send_bytes(resp_msg.serialize(), timeout=self._timeout)
                if self._trace:
                    log.info(f'Sent message {resp_msg}')

        except Exception as e:
            tb = traceback.format_exc()
            log.debug(f'There is an exception in orchestrator Exception {e}\n Traceback: {tb}\n.')

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

    @dutil.route(dmsg.DDRegisterClient, _DTBL)
    def register_client(self, msg: dmsg.DDRegisterClient) -> None:
        resp_msg = dmsg.DDRegisterClientResponse(tag=self._tag_inc(), ref=msg.tag, err=DragonError.SUCCESS, clientID=self._clientid_inc(), numManagers=self._num_managers, timeout=self._timeout)
        connection = fli.FLInterface.attach(b64decode(msg.respFLI))
        self._send_msg(resp_msg, connection)
        connection.detach()

    @dutil.route(dmsg.DDCreate, _DTBL)
    def create(self, msg: dmsg.DDCreate) -> None:
        self._create_req_msg_tag = msg.tag
        self._create_req_msg_respFLI = msg.respFLI
        args = cloudpickle.loads(b64decode(msg.args))
        self._working_set_size, self._wait_for_keys, self._wait_for_writers, self._policy, self._persist_freq, self._persist_base_name, self._timeout = args
        if isinstance(self._policy, list):
            log.debug('policy_list: '.format(self._policy))
            self._manager_pool = ProcessGroup(restart=False)
            for policy in self._policy:
                template = ProcessTemplate(manager_proc, args=(self.mpool_size, self._serialized_return_connector, self._serialized_main_connector, self._trace, args), policy=policy)
                self._manager_pool.add_process(nproc=self._managers_per_node, template=template)
        else:
            log.debug('policy: {}'.format(self._policy))
            template = ProcessTemplate(manager_proc, args=(self.mpool_size, self._serialized_return_connector, self._serialized_main_connector, self._trace, args))
            self._manager_pool = ProcessGroup(restart=False, policy=self._policy)
            self._manager_pool.add_process(nproc=self._num_managers, template=template)
        self._manager_pool.init()
        self._manager_pool.start()
        self._serving_connector = self._return_connector

    @dutil.route(dmsg.DDRegisterManager, _DTBL)
    def register_manager(self, msg: dmsg.DDRegisterManager) -> None:
        try:
            self._manager_connections.append(fli.FLInterface.attach(b64decode(msg.mainFLI)))
            self._serialized_manager_flis.append(msg.mainFLI)
            self._serialized_manager_return_flis.append((msg.respFLI, msg.tag))
            self._manager_nodes.append(b64encode(cloudpickle.dumps(Node(msg.hostID))))
            self._num_managers_created += 1
            if self._num_managers_created == self._num_managers:
                # send response message to all managers
                for i, (m, tag) in enumerate(self._serialized_manager_return_flis):
                    resp_msg = dmsg.DDRegisterManagerResponse(self._tag_inc(), ref=tag, err=DragonError.SUCCESS, managerID=i, managers=self._serialized_manager_flis, managerNodes=self._manager_nodes)
                    connection = fli.FLInterface.attach(b64decode(m))
                    self._send_msg(resp_msg, connection)
                    connection.detach()
                # send DD create response to client
                resp_msg = dmsg.DDCreateResponse(self._tag_inc(), ref=self._create_req_msg_tag, err=DragonError.SUCCESS)
                connection = fli.FLInterface.attach(b64decode(self._create_req_msg_respFLI))
                self._send_msg(resp_msg, connection)
                connection.detach()
                # dictionary created - switch to main connector to serve other requests
                self._serving_connector = self._main_connector
        except Exception as ex:
            tb = traceback.format_exc()
            log.debug(f'There was an exception while registering managers: {ex} \n Traceback: {tb}')
            raise RuntimeError(f'There was an exception while registering managers: {ex} \n Traceback: {tb}')

    @dutil.route(dmsg.DDGetRandomManager, _DTBL)
    def get_random_manager(self, msg: dmsg.DDGetRandomManager) -> None:
        managerFLI = self._serialized_manager_flis[self._get_next_main_manager_id()]
        resp_msg = dmsg.DDGetRandomManagerResponse(self._tag_inc(), ref=msg.tag, err=DragonError.SUCCESS, manager=managerFLI)
        connection = fli.FLInterface.attach(b64decode(msg.respFLI))
        self._send_msg(resp_msg, connection)
        connection.detach()

    @dutil.route(dmsg.DDDestroy, _DTBL)
    def destroy_dict(self, msg: dmsg.DDDestroy) -> None:
        self._serving_connector = self._return_connector
        self._destroyed_managers = 0
        self._destroy_resp_ref = msg.tag
        for m in self._manager_connections:
            req_msg = dmsg.DDDestroyManager(tag=self._tag_inc(), respFLI=self._serialized_return_connector)
            self._send_msg(req_msg, m)

        self._destroying_client_fli = msg.respFLI
        self._destroying_tag = msg.tag

    @dutil.route(dmsg.DDDestroyManagerResponse, _DTBL)
    def destroy_manager_response(self, msg: dmsg.DDDestroyManagerResponse) -> None:
        if msg.err != DragonError.SUCCESS:
            raise Exception(f'{msg.err} Failed to destroy manager!')
        self._destroyed_managers += 1
        if self._destroyed_managers == self._num_managers:
            self._serving = False

def start(managers_per_node: int, n_nodes: int, total_mem: int, trace: bool):
    try:
        log.debug('Initing Orchestrator')
        orc = Orchestrator(managers_per_node, n_nodes, total_mem, trace)
        orc.run()
    except Exception as ex:
        tb = traceback.format_exc()
        log.debug(f'There was an exception initing the orchestrator: {ex} \n Traceback: {tb}')