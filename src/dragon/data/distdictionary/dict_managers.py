"""
Dragon dictionary managers
"""
import os
import sys
import enum
import pickle
import logging


from ... import managed_memory
from ...globalservices import channel
from ...globalservices import pool
from ...infrastructure import facts
from ...infrastructure import parameters
from ...channels import Channel, Message, Many2ManyWritingChannelFile, Many2ManyReadingChannelFile

LOG = logging.getLogger(__name__)

# Define the byteorder as the native system byteorder
_BYTEORDER = sys.byteorder

@enum.unique
class DictOp(enum.Enum):
    """Action to be performed on the dictionary item"""
    INIT_CLIENT = enum.auto()
    CLOSE_CLIENT = enum.auto()
    STOP_DICT = enum.auto()
    SET_ITEM = enum.auto()
    GET_ITEM = enum.auto()
    DEL_ITEM = enum.auto()
    POP_ITEM = enum.auto()
    CONT_ITEM = enum.auto()
    GET_LEN = enum.auto()
    GET_KEYS = enum.auto()
    GET_VALS = enum.auto()
    GET_ITEMS = enum.auto()

@enum.unique
class ReturnCode(enum.Enum):
    """Code returned by the dictionary manager"""
    SUCCESS_RET_VAL = enum.auto()
    SUCCESS_RET_TRUE = enum.auto()
    SUCCESS_RET_FALSE = enum.auto()
    FAILED = enum.auto()

def send_message(client_chnl, manager_pool_mem, ret_code, value=None):
    """Send message to the client channel from the manager process.

    :param client_chnl: Serialized channel descriptor of the client to attach to
    :type client_chnl: bytes-like object
    :param manager_pool_mem: Memory pool for the writing adapter
    :type manager_pool_mem: MemoryPool object
    :param ret_code: Return value of the exectued operation to the client
    :type ret_code: int
    :param value: Value fetched from the dictionary and to be returned to the client
    :type value: bytes-like object
    """
    try:
        ret_adapter = Many2ManyWritingChannelFile(client_chnl, buffer_pool=manager_pool_mem)
        ret_adapter.open()
        ret_header = ret_code.value.to_bytes(4, byteorder=_BYTEORDER, signed=True)
        ret_adapter.write(ret_header)

        if value:
            value_size = sys.getsizeof(value)
            ret_adapter.write(value_size.to_bytes(4, byteorder=_BYTEORDER, signed=True))
            ret_adapter.write(value)
    except Exception as e:
        raise ConnectionError(f"Could not complete send bytes operation to the client channel: {e}")
    finally:
        ret_adapter.close()

def manager_proc(pool_size: int, mowner_chnl_name: str) -> None:
    """Manager process creates the associated channel and pool.
    Communicates its information to the manager owner process.
    Waits for the client messages on the created manager channel.

    :param pool_size: Size of the manager pool
    :type pool_size: int
    :param mowner_chnl_name: channel name that acts an identifier(c_uid) to the channel
    :type mowner_chnl_name: string
    """
    # Collect the puid of the manager process
    manager_puid = parameters.this_process.my_puid

    # Allocate pool for the manager process
    manager_pool_name = f'{facts.DEFAULT_DICT_POOL_NAME_BASE}{manager_puid}'
    manager_pool_desc = pool.create(pool_size, user_name=manager_pool_name)
    manager_pool_mem = managed_memory.MemoryPool.attach(manager_pool_desc._sdesc)
    LOG.debug(f"Dictionary manager process with puid:{parameters.this_process.my_puid} \
              created on node-index:{parameters.this_process.index} with pool-name: {manager_pool_name}.")

    # Create the manager channel that receives the requests from the client
    manager_req_chnl = channel.create(manager_pool_desc.m_uid)
    manager_chnl_desc = channel.query(manager_req_chnl.name)
    manager_chnl = Channel.attach(manager_chnl_desc.sdesc)

    # Information of the manager process
    manager_info = {'p_uid': manager_puid,
                    'chnl_desc': manager_chnl_desc,
                    'pool_desc': manager_pool_desc}

    # Send the manager information to the owner channel
    mowner_chnl_desc = channel.query(mowner_chnl_name)
    mowner_chnl = Channel.attach(mowner_chnl_desc.sdesc)
    with Many2ManyWritingChannelFile(mowner_chnl, buffer_pool=manager_pool_mem) as adapter:
        pickle.dump(manager_info, file=adapter, protocol=5)
    mowner_chnl.detach()

    # Manager dictionary that stores key and memory descriptor of the value
    manager_kv = dict()
    manager_client_map = dict()

    LOG.debug(f'{manager_pool_mem} ; {manager_pool_mem.muid=}')
    LOG.debug(f'The pool free space is {manager_pool_mem.free_space} and utilization is {manager_pool_mem.utilization} percent.')

    serving = True
    while serving:
        try:
            read_adapter = Many2ManyReadingChannelFile(manager_chnl)
            read_adapter.open()
            key_info = pickle.load(read_adapter)
            value = None
            ret_code = ReturnCode.SUCCESS_RET_VAL

            # Identify the client channel from the manager-client channel map
            if key_info['dict_op'] == DictOp.INIT_CLIENT:
                try:
                    client_chnl = Channel.attach(key_info['client_chnl_desc'].sdesc)
                    manager_client_map[key_info['client_puid']] = client_chnl
                    LOG.debug(f"Client with puid:{key_info['client_puid']} initialized by manager process:{parameters.this_process.my_puid} \
                                on node-index:{parameters.this_process.index} with pool-name: {manager_pool_name}.")
                except Exception as e:
                        raise ConnectionError(f"Could not find client channel desc: {e}")
                ret_code = ReturnCode.SUCCESS_RET_TRUE
                send_message(client_chnl, manager_pool_mem, ret_code)
                continue
            elif key_info['dict_op'] == DictOp.CLOSE_CLIENT:
                client_puid = key_info['client_puid']
                if manager_client_map.get(client_puid):
                    try:
                        client_chnl = manager_client_map[client_puid]
                        ret_code = ReturnCode.SUCCESS_RET_TRUE
                        send_message(client_chnl, manager_pool_mem, ret_code)
                        client_chnl.detach()
                        LOG.debug(f"Detached the manager process with puid:{parameters.this_process.my_puid} from client channel {client_chnl}.")
                    except Exception as e:
                        # Client channel must have already deleted
                        pass
                    del manager_client_map[client_puid]
                    continue
            else:
                try:
                    client_chnl = manager_client_map[key_info['client_puid']]
                except Exception as e:
                    raise ConnectionError(f"Could not find client channel desc: {e}")

            if manager_client_map.get(key_info['client_puid']):
                client_chnl = manager_client_map[key_info['client_puid']]
            else:
                try:
                    client_chnl = Channel.attach(key_info['client_chnl_desc'].sdesc)
                    manager_client_map['client_puid'] = client_chnl
                except Exception as e:
                    raise ConnectionError(f"Could not find client channel desc: {e}")

            if key_info['dict_op'] == DictOp.GET_LEN:
                manager_len = len(manager_kv)
                length = pickle.dumps(manager_len)
                send_message(client_chnl, manager_pool_mem, ret_code, value=length)
            elif key_info['dict_op'] == DictOp.SET_ITEM:
                try:
                    mem_alloc_obj, offset = read_adapter.get_remaining_data()
                    mem_alloc_clone_obj = mem_alloc_obj.clone(offset)
                    manager_kv[key_info['key']] = mem_alloc_clone_obj
                    ret_code = ReturnCode.SUCCESS_RET_TRUE
                    send_message(client_chnl, manager_pool_mem, ret_code)
                except Exception as e:
                    raise ConnectionError(f"Could not complete SET {key_info['key']} operation: {e}")
            elif key_info['dict_op'] == DictOp.GET_ITEM:
                if manager_kv.get(key_info['key']):
                    mem_alloc_obj = manager_kv[key_info['key']]
                    msg = Message.create_from_mem(mem_alloc_obj)
                    value = msg.tobytes()
                    send_message(client_chnl, manager_pool_mem, ret_code, value=value)
                else:
                    ret_code = ReturnCode.FAILED
                    send_message(client_chnl, manager_pool_mem, ret_code)
            elif key_info['dict_op'] == DictOp.DEL_ITEM:
                if manager_kv.get(key_info['key']):
                    del manager_kv[key_info['key']]
                    ret_code = ReturnCode.SUCCESS_RET_TRUE
                else:
                    ret_code = ReturnCode.SUCCESS_RET_FALSE
                send_message(client_chnl, manager_pool_mem, ret_code)
            elif key_info['dict_op'] == DictOp.POP_ITEM:
                if manager_kv.get(key_info['key']):
                    mem_alloc_obj = manager_kv[key_info['key']]
                    msg = Message.create_from_mem(mem_alloc_obj)
                    value = msg.tobytes()
                    del manager_kv[key_info['key']]
                    send_message(client_chnl, manager_pool_mem, ret_code, value=value)
                else:
                    ret_code = ReturnCode.SUCCESS_RET_FALSE
                    send_message(client_chnl, manager_pool_mem, ret_code)
            elif key_info['dict_op'] == DictOp.CONT_ITEM:
                if manager_kv.get(key_info['key']):
                    ret_code = ReturnCode.SUCCESS_RET_TRUE
                else:
                    ret_code = ReturnCode.SUCCESS_RET_FALSE
                send_message(client_chnl, manager_pool_mem, ret_code)
            elif key_info['dict_op'] == DictOp.GET_KEYS:
                value = pickle.dumps(list(manager_kv.keys()))
                send_message(client_chnl, manager_pool_mem, ret_code, value=value)
            elif key_info['dict_op'] == DictOp.GET_VALS:
                values = []
                for value in manager_kv.values():
                    msg = Message.create_from_mem(value)
                    values.append(pickle.loads(msg.tobytes()))
                value = pickle.dumps(values)
                send_message(client_chnl, manager_pool_mem, ret_code, value=value)
            elif key_info['dict_op'] == DictOp.GET_ITEMS:
                items = []
                for key, value in manager_kv.items():
                    msg = Message.create_from_mem(value)
                    items.append((key, pickle.loads(msg.tobytes())))
                value = pickle.dumps(items)
                send_message(client_chnl, manager_pool_mem, ret_code, value=value)
            elif key_info['dict_op'] == DictOp.STOP_DICT:
                serving = False
            else:
                continue
        except Exception as e:
            raise ConnectionError(f"Could not complete receive bytes operation on the manager channel: {e}")
        finally:
            read_adapter.close()

    manager_pool_mem.destroy()