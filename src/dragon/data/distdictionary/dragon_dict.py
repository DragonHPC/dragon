"""
Dragon dictionary
"""

import os
import uuid
import pickle
import logging
import multiprocessing as mp

from . import distributed_dict
from .dict_managers import DictOp, ReturnCode, _BYTEORDER
from ...infrastructure import parameters
from ...globalservices import channel
from ...infrastructure import facts
from ... import managed_memory
from ...channels import Channel, Many2ManyWritingChannelFile, Many2ManyReadingChannelFile
from ...utils import B64

LOG = logging.getLogger(__name__)

class DragonDict:
    """
    A dictionary like object created over Dragon channels
    """

    def __init__(self, managers_per_node: int, n_nodes: int, total_mem: int) -> None:
        """Instantiate a distributed dictionary object that acts as a (key,value) store

        :param managers_per_node: Number of manager processes to be created on each node.
        :type managers_per_node: int
        :param n_nodes: Number of nodes specified for creating the manager processes.
        :type n_nodes: int
        :param total_mem: Total memory for the allocated pools of all the manager processes.
        This parameter along with managers_per_node, n_nodes helps to decide the pool size of each manager.
        This parameter expects to be specified in GB.
        :type total_mem: int
        """
        self._dist_dict = distributed_dict.DistributedDict(
            managers_per_node=managers_per_node,
            n_nodes=n_nodes,
            total_mem=total_mem)
        self._dist_dict.start()
        self._client_map = dict()
        self._reset()

    def __setstate__(self, state):
        self._dist_dict = state['_dist_dict']
        self._client_map = state['_client_map']
        self._reset()

    def __getstate__(self):
        dragon_dict = dict()
        dragon_dict['_dist_dict'] = self._dist_dict
        dragon_dict['_client_map'] = self._client_map
        return dragon_dict

    def _send_and_read_message_from_all_managers(self, key_info, ret_val=0):
        """Message will be sent to all the dictionary managers.
        Collect the results from the managers to return the cumulative result.

        :param key_info: Information about the operation to the manager
        :type key_info: dict
        :param ret_val: Cumulative value collected from the managers
        :type ret_val: int/list based on the caller. Defaults to zero.
        """
        manager_processes = self._dist_dict.manager_processes

        # Prepare the dictionary message and send over to the manager
        message = DragonDictMessage(key_info, buffer_pool=self._buffer_pool,
                                    client_chnl=self.client_chnl)
        for i in range(0, len(manager_processes)):
            manager_chnl_desc = manager_processes[i]['chnl_desc']
            message.send_message(manager_chnl_desc=manager_chnl_desc)

        manager_count = 0
        n_managers = (self._dist_dict.managers_per_node * self._dist_dict.nodes)
        for _ in range(n_managers):
            # Wait on the client channel to receive the message from the manager
            value = message.read_message()
            if value and not isinstance(value, bool):
                ret_val += value
        return ret_val

    def _reset(self):
        """Collect the client information from the client map to return the response
        from the dictionary operation. If the client is accessing the dictionary for
        first time, add it's information to the client map. Each manager will hold
        the client information and return the information to the client channel.
        """
        self.default_muid = facts.default_pool_muid_from_index(parameters.this_process.index)
        self._buffer_pool = managed_memory.MemoryPool.attach(B64.str_to_bytes(parameters.this_process.default_pd))
        self.client_puid = parameters.this_process.my_puid

        if self._client_map.get(self.client_puid):
            client_chnl = self._client_map[self.client_puid]
            self.client_chnl_create = client_chnl[0]
            self.client_chnl_desc = client_chnl[1]
        else:
            self.client_chnl_uuid = str(uuid.uuid4())
            self.client_chnl_create = channel.create(self.default_muid, user_name="client-"+self.client_chnl_uuid)
            self.client_chnl_desc = channel.query(self.client_chnl_create.name)
            self.client_chnl = Channel.attach(self.client_chnl_desc.sdesc)
            self._client_map[self.client_puid] = (self.client_chnl_create, self.client_chnl_desc)

            # Construct the information needed for serving the request
            key_info = {'dict_op': DictOp.INIT_CLIENT,
                        'client_puid': self.client_puid,
                        'client_chnl_desc': self.client_chnl_desc}
            self._send_and_read_message_from_all_managers(key_info)

    def close(self):
        """Close the client. This typically implies that client does not want to access
        the dictionary no more. The dragon dictionary will erase the client information
        across it and all the managers. Closing typically implies to the clients, while
        stop call implies closing the entire dragon dictionary.
        """
        # Construct the information needed for closing the client request
        key_info = {'dict_op': DictOp.CLOSE_CLIENT,
                    'client_puid': self.client_puid}

        self._send_and_read_message_from_all_managers(key_info)

        # Remove the client entry from the dictionary
        del self._client_map[self.client_puid]

        # Destroy the client channel
        self.client_chnl.detach()
        channel.destroy(self.client_chnl_desc.c_uid)

    def stop(self):
        """Stop the dragon dictionary. Destroys all the manager processes, associated
        channels and memory pools. Implies no further access to the dictionary object.
        """
        self._dist_dict.stop()

    def __len__(self):
        # Construct the information needed for serving the request
        key_info = {'dict_op': DictOp.GET_LEN,
                    'client_puid': self.client_puid}
        length = self._send_and_read_message_from_all_managers(key_info)
        return length

    def length(self):
        """Length of the dictionary. Collects the number of all the key,value
        pairs across all the managers and returns the cumulative result.
        """
        len = self.__len__()
        return len

    def __getitem__(self, key):
        """GET item in the dictionary. The request will be routed to the
        corresponding manager in the dictionary based on the provided key.

        :param key: Information about the GET operation to the manager
        :type key: Any hashable structure
        :return: Value for the stored key in the dictionary
        :rtype: Any object that can be serialized
        """
        # Retrieve the manager channel descriptor for the request
        manager_chnl_desc = self._dist_dict.retrieve_channel_desc(key)

        # Construct the information needed for serving the request
        key_info = {'key': key, 'dict_op': DictOp.GET_ITEM,
                    'client_puid': self.client_puid}

        # Prepare the dictionary message and send over to the manager
        message = DragonDictMessage(key_info, buffer_pool=self._buffer_pool,
                                    manager_chnl_desc=manager_chnl_desc,
                                    client_chnl=self.client_chnl)
        message.send_message()

        # Wait on the client channel to receive the message from the manager
        client_val = message.read_message()
        LOG.debug(f"GET ITEM: Retrieved {key} successfully from the dictionary.")
        return client_val

    def __setitem__(self, key, value):
        """SET item in the dictionary. The request will be routed to the
        corresponding manager in the dictionary based on the provided key.

        :param key: Information about the SET operation to the manager
        :type key: Any hashable structure
        :param value: Value to be stored inside the manager
        :type value: Any structure that can be serialized
        """
        # Retrieve the manager channel descriptor for the request
        manager_chnl_desc = self._dist_dict.retrieve_channel_desc(key)

        # Construct the information needed for serving the request
        key_info = {'key': key,
                    'dict_op': DictOp.SET_ITEM,
                    'client_puid': self.client_puid}

        # Prepare the dictionary message and send over to the manager
        message = DragonDictMessage(key_info, value=value,
                                    buffer_pool=self._buffer_pool,
                                    manager_chnl_desc=manager_chnl_desc,
                                    client_chnl=self.client_chnl)
        message.send_message()
        message.read_message()
        LOG.debug(f"SET ITEM: Stored {key} successfully in the dictionary.")

    def __delitem__(self, key):
        """DEL item in the dictionary. The request will be routed to the
        corresponding manager in the dictionary based on the provided key.

        :param key: Information about the DEL operation to the manager
        :type key: Any hashable structure
        """
        # Retrieve the manager channel descriptor for the request
        manager_chnl_desc = self._dist_dict.retrieve_channel_desc(key)

        # Construct the information needed for serving the request
        key_info = {'key': key,
                    'dict_op': DictOp.DEL_ITEM,
                    'client_puid': self.client_puid}

        # Prepare the dictionary message and send over to the manager
        message = DragonDictMessage(key_info, buffer_pool=self._buffer_pool,
                                    manager_chnl_desc=manager_chnl_desc,
                                    client_chnl=self.client_chnl)
        message.send_message()
        if(message.read_message()):
            LOG.debug(f"Deleted key: {key} successfully from the dictionary.")
        else:
            LOG.debug(f"Key: {key} does not exist. Delete {key} is not successful.")

    def __iter__(self):
        """
        Return an iterator to the list of keys in the dictionary
        """
        return iter(self.keys())

    def get(self, key: object) -> object:
        """GET item from the dictionary. The request will be routed to the
        corresponding manager in the dictionary based on the provided key.

        :param key: Information about the GET operation to the manager
        :type key: Any hashable structure
        :return: Value for the stored key in the dictionary
        :rtype: Any object that can be serialized
        """
        return self[key]

    def keys(self) -> list:
        """Retrieve the keys from the dictionary. Collects all the keys
        across the managers and returns the result as a list.
        :return: List of all the keys in the dictionary
        :rtype: list
        """
        op_info = {'dict_op': DictOp.GET_KEYS,
                   'client_puid': self.client_puid}
        keys = self._send_and_read_message_from_all_managers(op_info, [])
        return keys

    def values(self) -> list:
        """Retrieve the values from the dictionary. Collects all the values
        across the managers and returns the result as a list.
        :return: List of all the values in the dictionary
        :rtype: list
        """
        op_info = {'dict_op': DictOp.GET_VALS,
                   'client_puid': self.client_puid}
        values = self._send_and_read_message_from_all_managers(op_info, [])
        return values

    def items(self) -> list[tuple]:
        """Retrieve the items from the dictionary. Collects all the keys,values
        across the managers and returns the result as a list, with each key,value
        pair as a tuple.
        :return: List of all the key, value pairs as tuples in the dictionary
        :rtype: list
        """
        op_info = {'dict_op': DictOp.GET_ITEMS,
                   'client_puid': self.client_puid}
        items = self._send_and_read_message_from_all_managers(op_info, [])
        return items

    def __contains__(self, key):
        """Check if the key is present in the dictionary. The request will be
        routed to the corresponding manager based on the provided key.

        :param key: Information about the key to the manager
        :type key: Any hashable structure
        """
        # Retrieve the manager channel descriptor for the request
        manager_chnl_desc = self._dist_dict.retrieve_channel_desc(key)

        # Construct the information needed for checking the key
        key_info = {'key': key, 'dict_op': DictOp.CONT_ITEM,
                    'client_puid': self.client_puid}

        # Prepare the dictionary message and send over to the manager
        message = DragonDictMessage(key_info, buffer_pool=self._buffer_pool,
                                    manager_chnl_desc=manager_chnl_desc,
                                    client_chnl=self.client_chnl)
        message.send_message()

        # Wait on the client channel to receive the message from the manager
        ret_val = message.read_message()
        if not ret_val:
            LOG.debug(f"Failed: Dictionary does not contain the key: {key}.")
        return ret_val

    def pop(self, key: object) -> object:
        """Pop an item in the dictionary. The request will be routed to the
        corresponding manager in the dictionary based on the provided key. The
        key will be deleted, collecting the value from it.

        :param key: Information about the key to be popped
        :type new_key: Any hashable structure
        :return: Value of the removed key from the dictionary
        :rtype: Any object that can be serialized
        """
        # Retrieve the manager channel descriptor for the request
        manager_chnl_desc = self._dist_dict.retrieve_channel_desc(key)

        # Construct the information needed for serving the request
        key_info = {'key': key,
                    'dict_op': DictOp.POP_ITEM,
                    'client_puid': self.client_puid}

        # Prepare the dictionary message and send over to the manager
        message = DragonDictMessage(key_info, buffer_pool=self._buffer_pool,
                                    manager_chnl_desc=manager_chnl_desc,
                                    client_chnl=self.client_chnl)
        message.send_message()
        ret_val = message.read_message()
        if not ret_val:
            LOG.debug(f"Key: {key} does not exist. Pop {key} is not successful.")
            raise KeyError(f"on request for {key}")
        else:
            LOG.debug(f"Popped key: {key} successfully from the dictionary.")
            return ret_val

    def rename(self, old_key: object, new_key: object) -> None:
        """Rename an item in the dictionary. The request will be routed to the
        corresponding manager in the dictionary based on the provided key. The
        old key will be deleted, collecting the value from it. A new set operation
        will be executed on the dictionary with a new key and collected value.

        :param old_key: Information about the key to be replaced
        :type old_key: Any hashable structure
        :param new_key: Information about the new key to be added
        :type new_key: Any hashable structure
        :return: None
        :rtype None
        """
        ret_val = self.pop(old_key)
        if ret_val:
            self.__setitem__(new_key, ret_val)
        else:
            LOG.debug(f"Key: {old_key} does not exist. Rename {old_key} is not successful.")


class DragonDictMessage():
    """
    A message object created to carry the information over the Dragon channel.
    Contains the information about the operation and the (key, value) to be stored
    in the dragon dictionary.
    :param key_info: Information about the operation to be performed on the manager.
    :type key_info: dict
    :param value: Value to be stored inside the dictionary
    :type value: Any structure that can be serialized
    :param buffer_pool: Memory pool that will be used while transferring the data
    information to the remote node.
    :type buffer_pool: Dragon Memory Pool
    :param manager_chnl_desc: Manager channel descriptor that is used to be send the message
    :type manager_chnl_desc: Dragon Channel Descriptor
    :param client_chnl: Client channel used to receive the response
    :type client_chnl: Dragon channel object
    """

    def __init__(self, key_info, value=None, buffer_pool=None,
                 manager_chnl_desc=None, client_chnl=None):
        self.key_info = key_info
        self.value = value

        if buffer_pool:
            self.buffer_pool = buffer_pool
        else:
            self.buffer_pool = None

        if manager_chnl_desc:
            self.manager_chnl = Channel.attach(manager_chnl_desc.sdesc)
            self.write_adapter = Many2ManyWritingChannelFile(self.manager_chnl, buffer_pool=self.buffer_pool)
        else:
            self.manager_chnl = None
            self.write_adapter = None

        if client_chnl:
            self.read_adapter = Many2ManyReadingChannelFile(client_chnl)
        else:
            self.read_adapter = None

    def send_message(self, manager_chnl_desc: object = None) -> None:
        """Send the message to the dictionary manager over it's corresponding channel.
        Use the memory pool if the channel is on the remote node.
        :param manager_chnl_desc: Manager channel descriptor that is used to be send the message
        :type manager_chnl_desc: Dragon channel descriptor
        """
        if manager_chnl_desc:
            self.manager_chnl = Channel.attach(manager_chnl_desc.sdesc)
            self.write_adapter = Many2ManyWritingChannelFile(self.manager_chnl, buffer_pool=self.buffer_pool)

        try:
            self.write_adapter.open()
            pickle.dump(self.key_info, file=self.write_adapter, protocol=5)
            if self.value is not None:
                pickle.dump(self.value, file=self.write_adapter, protocol=5)
        except Exception as e:
            raise ConnectionError(f"Could not complete send operation: {e}")
        finally:
            self.write_adapter.close()

        if manager_chnl_desc:
            self.manager_chnl.detach()

    def read_message(self, client_chnl: object = None) -> None:
        """Read the message from the dictionary manager over the client channel.
        :param client_chnl: Client channel that is used to read the message
        :type client_chnl: Dragon channel object
        :return: Value retrieved from the dictionary for the requested message
        :rtype: Boolean or any value that is serializable
        """
        value = None

        if client_chnl:
            self.read_adapter = Many2ManyReadingChannelFile(self.client_chnl)

        try:
            self.read_adapter.open()
            buf = bytearray(4)
            self.read_adapter.readinto(memoryview(buf))
            ret_val = ReturnCode(int.from_bytes(buf, byteorder=_BYTEORDER, signed=True))

            if (ret_val == ReturnCode.FAILED):
                raise KeyError(f"on request for {self.key_info['key']}")
            elif (ret_val == ReturnCode.SUCCESS_RET_VAL):
                self.read_adapter.readinto(memoryview(buf))
                value_size = int.from_bytes(buf, byteorder=_BYTEORDER, signed=True)
                buf = bytearray(value_size)
                self.read_adapter.readinto(memoryview(buf))
                value = pickle.loads(buf)
            elif (ret_val == ReturnCode.SUCCESS_RET_TRUE):
                value = True
            elif (ret_val == ReturnCode.SUCCESS_RET_FALSE):
                value = False
        except EOFError:
            raise EOFError
        except Exception as e:
            raise ConnectionError(f"Could not complete receive operation: {e}")
        finally:
            self.read_adapter.close()

        return value
