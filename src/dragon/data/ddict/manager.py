"""
The Distributed Dictionary is a performant and distributed key-value store
that is available to applications and workflows written for the Dragon ecosystem.

This is Dragon's specialized implementation based on the Dragon file-like interface
which relies on Dragon Channels. The Distributed Dictionary is to work like a standard
Python dictionary except that the data that it holds may span multiple nodes and be
larger than any one node can hold.

The manager manages on shard of a distributed database. Selection of the manager is based
on the hashed value of the key being used for the insertion of key/value pairs within
the distributed dictionary.

"""

import os
import sys
import logging
import traceback
import time
import socket
import os

from ...utils import b64decode, b64encode, set_local_kv
from ... import managed_memory as dmem
from ...globalservices import channel
from ...globalservices import pool
from ...infrastructure import facts
from ...infrastructure import parameters
from ...infrastructure import util as dutil
from ...infrastructure import messages as dmsg
from ...infrastructure.channel_desc import ChannelOptions
from ...localservices.options import ChannelOptions as LSChannelOptions
from ...channels import Channel
from ... import fli
from ...rc import DragonError
from .ddict import KEY_HINT, VALUE_HINT, DDictManagerFull
from ...dlogging.util import setup_BE_logging, DragonLoggingServices as dls

log = None
NUM_STREAM_CHANNELS = 20
MAX_NUM_CLIENTS = 100000

# This is used to make it possible to still use the managed memory pool
# when it is getting fully utilized to be able to respond to requests
# by rejecting additional puts.
RESERVED_POOL_SPACE = 1024**2

class Manager:

    _DTBL = {}  # dispatch router, keyed by type of message

    def __init__(self, pool_size: int, serialized_return_orc, serialized_main_orc, args):
        self._working_set_size, self._wait_for_keys, self._wait_for_writers, self._policy, self._persist_freq, self._persist_base_name, self._timeout = args
        self._puid = parameters.this_process.my_puid
        fname = f'{dls.DD}_{socket.gethostname()}_manager_{str(self._puid)}.log'
        global log
        if log == None:
            setup_BE_logging(service=dls.DD, fname=fname)
            log = logging.getLogger(str(dls.DD))

        # create manager's return_connector (SHReturn)
        self._return_channel = Channel.make_process_local()
        self._return_connector = fli.FLInterface(main_ch=self._return_channel, use_buffered_protocol=True)
        self._serialized_return_connector = b64encode(self._return_connector.serialize())

        # create memory pool
        _user = os.environ.get('USER', str(os.getuid()))
        self._pool_name = f'{facts.DEFAULT_DICT_POOL_NAME_BASE}{os.getpid()}_{_user}'
        self._pool_desc = pool.create(pool_size, user_name=self._pool_name)
        self._pool = dmem.MemoryPool.attach(self._pool_desc._sdesc)

        # We create these two channels in the default pool to isolate them
        # from the manager pool which could fill up. This is to make the
        # receiving of messages a bit more resistant to the manager pool
        # filling up.
        self._fli_main_channel = Channel.make_process_local()
        self._fli_manager_channel = Channel.make_process_local()

        # stream channels are created in the pool so when data arrives
        # at the manager it is already copied into the manager's pool.
        self._stream_channels_descs = []
        self._stream_channels = []

        sh_channel_options = LSChannelOptions(capacity=10)
        gs_channel_options = ChannelOptions(ref_count=True, local_opts=sh_channel_options)

        for i in range(NUM_STREAM_CHANNELS):
            desc = channel.create(self._pool.muid, options=gs_channel_options)
            self._stream_channels_descs.append(desc)
            stream_channel = Channel.attach(desc.sdesc)
            self._stream_channels.append(stream_channel)

        self._main_connector = fli.FLInterface(main_ch=self._fli_main_channel, manager_ch=self._fli_manager_channel,
                                                pool=self._pool, stream_channels=self._stream_channels)
        self._serialized_main_connector = b64encode(self._main_connector.serialize())

        self._client_connections_map = {}
        self._buffered_client_connections_map = {}
        self._kvs = {}
        self._key_map = {}
        self.iterators = {}
        self._serving = False
        self._tag = 0
        self._iter_id = 0
        self._abnormal_termination = False
        self._managers = []
        self._local_client_id = 0

        # send the manager information to local service and orchestrator to register manager
        self._serialized_main_orc = serialized_main_orc
        self._register_with_local_service()
        self._serialized_return_orc = serialized_return_orc
        self._register_with_orchestrator(serialized_return_orc)

    def _free_resources(self):

        try:
            # destroy all client maps
            for i in self._buffered_client_connections_map:
                self._buffered_client_connections_map[i].detach()
            del self._buffered_client_connections_map
            for i in self._client_connections_map:
                self._client_connections_map[i].detach()
            del self._client_connections_map

            self._main_connector.destroy()
            self._fli_main_channel.destroy()
            self._fli_manager_channel.destroy()

            for i in range(NUM_STREAM_CHANNELS):
                channel.destroy(self._stream_channels_descs[i].c_uid)
            pool.destroy(self._pool_desc.m_uid)

        except Exception as ex:
            tb = traceback.format_exc()
            log.debug(f'manager {self._puid} failed to destroy resources. Exception: {ex}\n Traceback: {tb}\n')

    def _register_with_local_service(self):
        log.debug(f'manager is sending set_local_kv with {self._serialized_main_orc=}')
        set_local_kv(key=self._serialized_main_orc, value=self._serialized_main_connector)

    def _tag_inc(self):
        tag = self._tag
        self._tag += 1
        return tag

    def _iter_inc(self):
        iter_id = self._iter_id
        self._iter_id += 1
        return iter_id

    def _send_msg(self, msg, connection, clientID=None):
        try:
            with connection.sendh(timeout=self._timeout) as sendh:
                sendh.send_bytes(msg.serialize(), timeout=self._timeout)
        except Exception as ex:
            tb = traceback.format_exc()
            log.debug(f'There was an exception in the manager _send_msg: {ex} \n Traceback: {tb}')
            raise RuntimeError(f'There was an exception in the manager _send_msg: {ex} \n Traceback: {tb}')

    def _recv_msg(self):
        with self._return_connector.recvh() as recvh:
            # Wait patiently here since it might be a big dictionary and is
            # just starting.
            resp_ser_msg, hint = recvh.recv_bytes()

        return dmsg.parse(resp_ser_msg)

    def _send_dmsg_and_value(self, resp_msg, connection, key_mem, transfer_ownership=False, clientID=None):
        with connection.sendh(use_main_as_stream_channel=True, timeout=self._timeout) as sendh:
            sendh.send_bytes(resp_msg.serialize(), timeout=self._timeout)

            if resp_msg.err == DragonError.SUCCESS:
                val_list = self._kvs[key_mem]
                if transfer_ownership:
                    self._kvs[key_mem] = []
                for val in val_list:
                    sendh.send_mem(val, transfer_ownership=transfer_ownership, arg=VALUE_HINT, timeout=self._timeout)
                if transfer_ownership:
                    del self._kvs[key_mem]
                    del self._key_map[key_mem]
                    key_mem.free()

    def _register_with_orchestrator(self, serialized_return_orc):
        msg = dmsg.DDRegisterManager(self._tag_inc(), self._serialized_main_connector, self._serialized_return_connector)
        connection = fli.FLInterface.attach(b64decode(serialized_return_orc))
        self._send_msg(msg, connection)
        connection.detach()
        resp_msg = self._recv_msg()
        if resp_msg.err != DragonError.SUCCESS:
            raise Exception(f'Failed to register manager with orchester. Return code: {resp_msg.err}')
        self._manager_id = resp_msg.managerID
        self._managers = resp_msg.managers
        self._client_id_offset = self._manager_id * MAX_NUM_CLIENTS
        self._global_client_id = self._client_id_offset
        self._serving = True

    def _move_to_pool(self, client_mem):
        if client_mem.pool.muid != self._pool.muid:
            # we need to move it - if no room don't wait.
            try:
                new_mem = client_mem.copy(self._pool, timeout=0)
            except dmem.DragonMemoryError:
                raise DDictManagerFull(DragonError.MEMORY_POOL_FULL, "Could not move data to manager pool.")
            finally:
                client_mem.free()

            return new_mem

        return client_mem

    def _contains_put_key(self, client_key_mem):
        if client_key_mem in self._kvs:
            key_mem = self._key_map[client_key_mem]
            client_key_mem.free()
            ec = DragonError.SUCCESS
        else:
            key_mem = self._move_to_pool(client_key_mem)
            self._key_map[key_mem] = key_mem
            ec = DragonError.KEY_NOT_FOUND

        return ec, key_mem

    def _contains_and_free_msg_key(self, msg_key_mem):
        if msg_key_mem in self._kvs:
            ec = DragonError.SUCCESS
            key_mem = self._key_map[msg_key_mem]
        else:
            key_mem = None
            ec = DragonError.KEY_NOT_FOUND

        # We must always free the messages key memory
        msg_key_mem.free()

        return ec, key_mem

    def _register_client(self, client_id, respFLI, bufferedRespFLI):
        self._client_connections_map[client_id] = fli.FLInterface.attach(b64decode(respFLI))
        self._buffered_client_connections_map[client_id] = fli.FLInterface.attach(b64decode(bufferedRespFLI))

    def _get_next_client_id(self):
        client_id = self._global_client_id
        self._local_client_id = (self._local_client_id + 1) % MAX_NUM_CLIENTS
        self._global_client_id = self._client_id_offset + self._local_client_id
        return client_id

    def dump_state(self):
        print(f'\nmanager {self._puid} has {len(self._kvs)} keys stored with it.\n', file=sys.stderr, flush=True)

    def run(self):
        try:
            while self._serving:
                with self._main_connector.recvh() as recvh:
                    ser_msg, hint = recvh.recv_bytes()
                    msg = dmsg.parse(ser_msg)
                    if type(msg) in self._DTBL:
                        self._DTBL[type(msg)][0](self, msg=msg, recvh=recvh)
                    else:
                        self._serving = False
                        self._abnormal_termination = True
                        log.debug(f'The message {msg} is not a valid message!')
        except Exception as ex:
            tb = traceback.format_exc()
            log.debug(f'There was an exception in manager:\n{ex}\n Traceback:\n{tb}')

        log.info(f'Manager {self._manager_id} preparing to exit')
        log.info(f'Pool utilization percent is {self._pool.utilization}')
        log.info(f'Number of keys stored is {len(self._kvs)}')
        log.info(f'Free space is {self._pool.free_space}')
        log.info(f'The total size of the pool managed by this manager was {self._pool.size}')

        try:
            self._free_resources()
        except Exception as ex:
            tb = traceback.format_exc()
            log.debug(f'There was an exception in the manager while free resources:\n{ex}\n Traeback:\n{tb}')


    @dutil.route(dmsg.DDRegisterClient, _DTBL)
    def register_client(self, msg: dmsg.DDRegisterClient, recvh):
        try:
            client_id = self._get_next_client_id()
            self._register_client(client_id=client_id, respFLI=msg.respFLI, bufferedRespFLI=msg.bufferedRespFLI)
            resp_msg = dmsg.DDRegisterClientResponse(self._tag_inc(), ref=msg.tag, err=DragonError.SUCCESS, clientID=client_id, numManagers=len(self._managers))
            self._send_msg(resp_msg, self._buffered_client_connections_map[client_id], client_id)
        except Exception as ex:
            tb = traceback.format_exc()
            log.debug(f'There was an exception in the register_client to manager {self._puid=} for client {self._global_client_id}: {ex}\n Traceback: {tb}\n {msg.respFLI=}')

    @dutil.route(dmsg.DDConnectToManager, _DTBL)
    def connect_to_manager(self, msg: dmsg.DDConnectToManager, recvh):
        try:
            resp_msg = dmsg.DDConnectToManagerResponse(self._tag_inc(), ref=msg.tag, err=DragonError.SUCCESS, manager=self._managers[msg.managerID])
            self._send_msg(resp_msg, self._buffered_client_connections_map[msg.clientID], msg.clientID)
        except Exception as ex:
            tb = traceback.format_exc()
            log.debug(f'There was an exception in request manager {msg.managerID} from manager {self._puid=} for client {msg.clientID}: {ex} \n Traceback: {tb}\n {msg.respFLI=}')
            raise RuntimeError(f'There was an exception in request manager {msg.managerID} from manager {self._puid=} for client {msg.clientID}: {ex} \n Traceback: {tb}\n {msg.respFLI=}')

    @dutil.route(dmsg.DDRegisterClientID, _DTBL)
    def register_clientID(self, msg: dmsg.DDRegisterClientID, recvh):
        try:
            self._register_client(client_id=msg.clientID, respFLI=msg.respFLI, bufferedRespFLI=msg.bufferedRespFLI)
            resp_msg = dmsg.DDRegisterClientIDResponse(self._tag_inc(), ref=msg.tag, err=DragonError.SUCCESS)
            self._send_msg(resp_msg, self._buffered_client_connections_map[msg.clientID], msg.clientID)
        except Exception as ex:
            tb = traceback.format_exc()
            log.debug(f'There was an exception in the register_clientID to manager {self._puid=} for client {msg.clientID}: {ex} \n Traceback: {tb}\n {msg.respFLI=}')
            raise RuntimeError(f'There was an exception in the register_clientID to manager {self._puid=} for client {msg.clientID}: {ex} \n Traceback: {tb}\n {msg.respFLI=}')

    @dutil.route(dmsg.DDDestroyManager, _DTBL)
    def destroy_manager(self, msg: dmsg.DDDestroyManager, recvh):
        try:
            self._serving = False
            set_local_kv(key=self._serialized_main_orc, value='')
            resp_msg = dmsg.DDDestroyManagerResponse(self._tag_inc(), ref=msg._tag, err=DragonError.SUCCESS)
            connection = fli.FLInterface.attach(b64decode(self._serialized_return_orc))
            self._send_msg(resp_msg, connection)
            connection.detach()
        except Exception as ex:
            tb = traceback.format_exc()
            log.debug(f'There was an exception while destroying manager {self._puid=}: {ex} \n Traceback: {tb}\n')

    @dutil.route(dmsg.DDPut, _DTBL)
    def put(self, msg: dmsg.DDPut, recvh):
        ec = DragonError.MEMORY_POOL_FULL # It will be reset below.
        key_mem = None
        client_key_mem = None
        val_list = []

        try:
            try:
                if self._pool.utilization >= 90.0 or self._pool.free_space < RESERVED_POOL_SPACE:
                    raise DDictManagerFull(DragonError.MEMORY_POOL_FULL, f'DDict Manager {self._manager_id}: Pool reserve limit exceeded.')

                # There is likely room for the key/value pair.
                client_key_mem, hint = recvh.recv_mem(timeout=self._timeout)

                assert hint == KEY_HINT

                try:
                    while True:
                        val_mem, hint = recvh.recv_mem(timeout=self._timeout)
                        val_mem = self._move_to_pool(val_mem)
                        assert hint == VALUE_HINT
                        val_list.append(val_mem)
                except EOFError:
                    pass

                key_moved = True # it is moved on the next call.
                ec, key_mem = self._contains_put_key(client_key_mem)
                client_key_mem = None # reset because it may be freed.
                # the underlying memory in the pool needs to be cleaned up if we put the same key-value pair into the dictionary

                if ec == DragonError.SUCCESS:
                    old_vals = self._kvs[key_mem] # free old value memory
                    self._kvs[key_mem] = [] # just in case of error while freeing
                    while len(old_vals) > 0:
                        try:
                            val = old_vals.pop()
                            val.free()
                        except Exception as ex:
                            log.info(f'There was an error while freeing value being replaced. {ex}')

                self._kvs[key_mem] = val_list
                ec = DragonError.SUCCESS

            except Exception as ex:
                log.info(f'Manager {self._manager_id} with PUID={self._puid} could not process put request. {ex}')
                # Depending on where we got to, these two free's may fail, that's OK.
                try:
                    key_mem.free()
                except:
                    pass
                try:
                    client_key_mem.free()
                except:
                    pass

                while len(val_list) > 0:
                    val = val_list.pop()
                    try:
                        val.free()
                    except:
                        pass

                if not recvh.stream_received:
                    try:
                        while True:
                            mem, hint = recvh.recv_mem(timeout=self._timeout)
                            mem.free()
                    except EOFError:
                        pass
                    except Exception as ex:
                        tb = traceback.format_exc()
                        log.debug(f'Caught exception while discarding rest of stream: {ex}\n {tb}')

                ec = DragonError.MEMORY_POOL_FULL

            resp_msg = dmsg.DDPutResponse(self._tag_inc(), ref=msg.tag, err=ec)

            self._send_msg(resp_msg, self._buffered_client_connections_map[msg.clientID], msg.clientID)

        except Exception as ex:
            tb = traceback.format_exc()
            log.debug(f'There was an unexpected exception in put in the manager, {self._puid=}, {msg.clientID=}: {ex} \n Traceback: {tb}')
            raise RuntimeError(f'There was an unexpected exception in put in manager, {self._puid=}, {msg.clientID=}: {ex} \n Traceback: {tb}')

    @dutil.route(dmsg.DDGet, _DTBL)
    def get(self, msg: dmsg.DDGet, recvh):
        key_mem, hint = recvh.recv_mem(timeout=self._timeout)
        assert hint == KEY_HINT
        # the underlying memory in the pool needs to be cleanup
        ec, key_mem = self._contains_and_free_msg_key(key_mem)
        resp_msg = dmsg.DDGetResponse(self._tag_inc(), ref=msg.tag, err=ec)
        self._send_dmsg_and_value(resp_msg, self._client_connections_map[msg.clientID], key_mem=key_mem, transfer_ownership=False, clientID=msg.clientID)

    @dutil.route(dmsg.DDPop, _DTBL)
    def pop(self, msg: dmsg.DDPop, recvh):
        key_mem, hint = recvh.recv_mem(timeout=self._timeout)
        assert hint == KEY_HINT
        ec, key_mem = self._contains_and_free_msg_key(key_mem)
        resp_msg = dmsg.DDPopResponse(self._tag_inc(), ref=msg.tag, err=ec)
        self._send_dmsg_and_value(resp_msg, self._client_connections_map[msg.clientID], key_mem=key_mem, transfer_ownership=True)

    @dutil.route(dmsg.DDContains, _DTBL)
    def contains(self, msg: dmsg.DDContains, recvh):
        key_mem, hint = recvh.recv_mem(timeout=self._timeout)
        assert hint == KEY_HINT
        ec, key_mem = self._contains_and_free_msg_key(key_mem)
        resp_msg = dmsg.DDContainsResponse(self._tag_inc(), ref=msg.tag, err=ec)
        self._send_msg(resp_msg, self._buffered_client_connections_map[msg.clientID], msg.clientID)

    @dutil.route(dmsg.DDGetLength, _DTBL)
    def get_length(self, msg: dmsg.DDGetLength, recvh):
        try:
            resp_msg = dmsg.DDGetLengthResponse(self._tag_inc(), ref=msg.tag, err=DragonError.SUCCESS, length=len(self._kvs))
            self._send_msg(resp_msg, self._buffered_client_connections_map[msg.clientID], msg.clientID)
        except Exception as ex:
            tb = traceback.format_exc()
            log.debug(f'There was an exception in get_length from manager {self._puid}: {ex} \n Traeback: \n {tb}')
            raise RuntimeError(f'There was an exception in get_length from manager {self._puid}: {ex} \n Traeback: \n {tb}')

    @dutil.route(dmsg.DDClear, _DTBL)
    def clear(self, msg: dmsg.DDClear, recvh):
        # The underlying memory in the pool needs to be cleaned up
        # in addition to the key/value maps.
        for key in self._kvs:
                val_list = self._kvs[key]
                self._kvs[key] = []
                while len(val_list) > 0:
                    val = val_list.pop()
                    try:
                        val.free()
                    except:
                        pass

        for key_mem in self._key_map:
            try:
                key_mem.free()
            except:
                pass

        self._kvs.clear()
        self._key_map.clear()
        resp_msg = dmsg.DDClearResponse(self._tag_inc(), ref=msg.tag, err=DragonError.SUCCESS)
        self._send_msg(resp_msg, self._buffered_client_connections_map[msg.clientID], msg.clientID)

    @dutil.route(dmsg.DDGetIterator, _DTBL)
    def get_iterator(self, msg:dmsg.DDGetIterator, recvh):
        iter_id = self._iter_inc()
        self.iterators[iter_id] = iter(self._kvs)
        resp_msg = dmsg.DDGetIteratorResponse(self._tag_inc(), ref=msg.tag, err=DragonError.SUCCESS, iterID=iter_id)
        self._send_msg(resp_msg, self._buffered_client_connections_map[msg.clientID], msg.clientID)

    @dutil.route(dmsg.DDIteratorNext, _DTBL)
    def iterate_next(self, msg:dmsg.DDIteratorNext, recvh):
        with self._client_connections_map[msg.clientID].sendh(use_main_as_stream_channel=True, timeout=self._timeout) as sendh:
            try:
                key = next(self.iterators[msg.iterID])
                resp_msg = dmsg.DDIteratorNextResponse(self._tag_inc(), ref=msg.tag, err=DragonError.SUCCESS)
                sendh.send_bytes(resp_msg.serialize(), timeout=self._timeout)
                sendh.send_mem(key, transfer_ownership=False, arg=KEY_HINT, timeout=self._timeout)
            except StopIteration:
                resp_msg = dmsg.DDIteratorNextResponse(self._tag_inc(), ref=msg.tag, err=DragonError.NO_MORE_KEYS)
                del self.iterators[msg.iterID]
                sendh.send_bytes(resp_msg.serialize(), timeout=self._timeout)

    @dutil.route(dmsg.DDKeys, _DTBL)
    def keys(self, msg: dmsg.DDKeys, recvh):
        with self._client_connections_map[msg.clientID].sendh(use_main_as_stream_channel=True, timeout=self._timeout) as sendh:
            resp_msg = dmsg.DDKeysResponse(self._tag_inc(), ref=msg.tag, err=DragonError.SUCCESS)
            sendh.send_bytes(resp_msg.serialize(), timeout=self._timeout)
            for k in self._kvs.keys():
                sendh.send_mem(k, transfer_ownership=False, arg=KEY_HINT, timeout=self._timeout)

    @dutil.route(dmsg.DDDeregisterClient, _DTBL)
    def deregister_client(self, msg: dmsg.DDDeregisterClient, recvh):
        try:
            self._client_connections_map[msg.clientID].detach()
            del self._client_connections_map[msg.clientID]

            resp_msg = dmsg.DDDeregisterClientResponse(self._tag_inc(), ref=msg.tag, err=DragonError.SUCCESS)
            self._send_msg(resp_msg, self._buffered_client_connections_map[msg.clientID], msg.clientID)

            self._buffered_client_connections_map[msg.clientID].detach()
            del self._buffered_client_connections_map[msg.clientID]

        except Exception as ex:
            tb = traceback.format_exc()
            log.debug(f'There was an exception while deregistering client in manager. Exception: {ex}\n Traceback: {tb}')
            raise RuntimeError(f'There was an exception while deregistering client in manager. Exception: {ex}\n Traceback: {tb}')

def manager_proc(pool_size: int, serialized_return_orc, serialized_main_orc, args):
    try:
        manager = Manager(pool_size, serialized_return_orc, serialized_main_orc, args)
        manager.run()
        log.debug('Manager is exiting....')
    except Exception as ex:
        tb = traceback.format_exc()
        log.debug(f'There was an exception initing the manager: {ex}\n Traceback: {tb}')
        raise RuntimeError(f'There was an exception initing the manager: {ex}\n Traceback: {tb}')