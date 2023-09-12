#!/usr/bin/env python3

import os
import sys
import string
import random
import dragon
import unittest
import multiprocessing as mp

from dragon.data.distdictionary.dragon_dict import DragonDict

def do_client_ops(_dict, key, value):
    """Each client will do the operation of adding the sample key,value pair
    into the dictionary and again delete the same pair.

    :param _dict: dragon distributed dictionary
    :type _dict: dragon dictionary object
    :param key: Information about the SET operation to the manager
    :type key: Any hashable structure
    :param value: Value to be stored inside the manager
    :type value: Any structure that can be serialized
    """
    _dict[key] = value
    del _dict[key]
    # Close the client
    _dict.close()

def generate_keys():
    keys = list()
    # Generate the list of 100 keys to add to the dictionary
    dict_size = 100
    letters = string.ascii_letters

    for _ in range(dict_size):
        # each key is 20 characters long and characters can be repeated
        key = ''.join(random.choice(letters) for i in range(20))
        keys.append(key)
    return keys

def do_set_ops(_keys, _dict, value_size):
    """Each client will generate the value of given size, and perform
    the SET operation to all the keys of the dictionary

    :param _keys: list of all keys to be added to the dictionary
    :type _keys: list
    :param _dict: dragon distributed dictionary
    :type _dict: dragon dictionary object
    :param value_size: size of the value in bytes added to each dictionary key
    :type value_size: int
    """
    num_keys = len(_keys)
    letters = string.ascii_letters
    value = ''.join(random.choice(letters) for i in range(value_size))

    # Add each key to the dictionary
    for key in _keys:
        _dict[key] = value
    # Close the client
    _dict.close()

def do_del_ops(_keys, _dict):
    """Each client will delete the given list of keys in the dictionary
    
    :param _keys: list of all keys to be deleted from the dictionary
    :type _keys: list
    :param _dict: dragon distributed dictionary
    :type _dict: dragon dictionary object
    """
    for key in _keys:
        del _dict[key]
    # Close the client
    _dict.close()

def do_get_ops(_keys, _dict):
    """Each client will retrieve the values for the given list of keys in the dictionary
    
    :param _keys: list of all keys to be fetched from the dictionary
    :type _keys: list
    :param _dict: dragon distributed dictionary
    :type _dict: dragon dictionary object
    """
    for key in _keys:
        value = _dict[key]
    # Close the client
    _dict.close()


class TestDragonDictSingleNode(unittest.TestCase):
    @classmethod
    def setUpClass(self) -> None:
        # Create a dragon dictionary on a single node with multiple manager processes
        self.managers_per_node = 2 # 2 Managers per node
        self.num_nodes = 1 # Single Node
        self.total_mem_size = 2*(1024*1024*1024) # 2 GB total size
        self.DD = DragonDict(self.managers_per_node, self.num_nodes, self.total_mem_size)

    @classmethod
    def tearDownClass(self) -> None:
        self.DD.stop()

    def test_dict_params(self):
        # Verify the dictionary params
        num_nodes = self.DD._dist_dict.nodes
        managers_per_node = self.DD._dist_dict.managers_per_node
        num_managers = num_nodes * managers_per_node
        self.assertEqual(num_nodes, 1, "Number of dictionary nodes are incorrect")
        self.assertEqual(managers_per_node, 2, "Number of managers per node are incorrect")
        self.assertEqual(num_managers, 2, "Number of dictionary managers are incorrect")

    def test_set_and_get(self):
        key = "Who are you Dragon?"
        value = "A Fire Breathing Monster!"
        self.DD[key] = value
        # Compare if the fetched values are correct
        self.assertEqual(self.DD[key], value, "Retrieved value from the dict is incorrect")

    def test_setup_and_close_client(self):
        # Create a client process and pass the dictionary
        client_proc = mp.Process(target=do_client_ops, args=(self.DD, "Dragon", "Dictionary"))
        client_proc.start()
        client_proc.join()

    def test_set_ops(self):
        keys = generate_keys()
        # Create client procs that does dictionary operations
        num_clients = 8
        value_size = 64
        procs = []
        for i in range(num_clients):
            client_proc = mp.Process(target=do_set_ops, args=(keys, self.DD, value_size))
            client_proc.start()
            procs.append(client_proc)

        for i in range(len(procs)):
            procs[i].join()
            procs[i].kill()

    def test_dictionary_length(self):
        num_kv_pairs = 100
        # Assign key value pairs to the dictionary
        for i in range(0, num_kv_pairs):
            key = "Hello" + "." + str(i)
            value = "Dragon" + "." + str(i)
            self.DD[key] = value

        # Calculate the length of the dictionary
        dict_length = len(self.DD)
        # Verify that length of dictionary is equal to number of kv pairs
        self.assertEqual(dict_length, num_kv_pairs, "Calculated length of the dictionary is incorrect")

    def test_set_and_del_item(self):
        key = "Hello"
        value = "Dictionary"
        self.DD[key] = value
        # Delete operation should be successful
        del self.DD[key]

    def test_del_item_with_no_key(self):
        # Delete operation should be successful, without key present also
        # Log the information that the key is not present.
        letters = string.ascii_letters
        key = ''.join(random.choice(letters) for i in range(10))
        del self.DD[key]

    def test_del_ops(self):
        keys = list()
        num_kv_pairs = 100
        # Collect the set of keys for the dictionary
        for i in range(0, num_kv_pairs):
            key = "Hello" + "." + str(i)
            keys.append(key)

        # Two client processes delete keys in parallel
        # This might conduct extra deletes with keys deleted by other proc before
        num_clients = 2
        procs = []
        for i in range(num_clients):
            client_proc = mp.Process(target=do_del_ops, args=(keys, self.DD))
            client_proc.start()
            procs.append(client_proc)

        for i in range(len(procs)):
            procs[i].join()
            procs[i].kill()

    def test_keys(self):
        # Collect the keys from the dictionary
        keys = self.DD.keys()
        num_keys = len(keys)
        dict_length = len(self.DD)
        # Verify that length of dictionary is equal to number of kv pairs
        self.assertEqual(dict_length, num_keys, "Calculated length of the dictionary is incorrect")

    def test_get_ops(self):
        keys = self.DD.keys()
        # Create client procs that does dictionary operations
        num_clients = 8
        procs = []
        for i in range(num_clients):
            client_proc = mp.Process(target=do_get_ops, args=(keys, self.DD))
            client_proc.start()
            procs.append(client_proc)

        for i in range(len(procs)):
            procs[i].join()
            procs[i].kill()

    def test_existing_key(self):
        key = "Testing"
        value = "Key"
        self.DD[key] = value
        key_found = False
        # Verify if the key is present in the dictionary
        if key in self.DD:
            key_found = True
        self.assertEqual(key_found, True, "Key has not found in the dictionary")

    def test_not_existing_key(self):
        key = "Test"
        key_found = False
        # Verify if the key is present in the dictionary
        if key in self.DD:
            key_found = True
        self.assertEqual(key_found, False, "Key found in the dictionary")

    def test_pop_item(self):
        key = "Pop"
        value = "Item"
        self.DD[key] = value
        # Collect the value by popping the key
        pop_value = self.DD.pop(key)
        # Verify that the pop operation is correct
        self.assertEqual(value, pop_value, "Pop value is not returned correctly")

    def test_existing_value(self):
        key = "Testing"
        value = "Value"
        self.DD[key] = value
        value_found = False
        # Verify if the value is existing in the dictionary
        if value in self.DD.values():
            value_found = True
        self.assertEqual(value_found, True, "Value has not found in the dictionary")

    def test_not_existing_value(self):
        value = "Dummy Value"
        value_found = False
        # Verify if the value is existing in the dictionary
        if value in self.DD.values():
            value_found = True
        self.assertEqual(value_found, False, "Value found in the dictionary")

    def test_items(self):
        items = self.DD.items()
        for (key, value) in items:
            ret_value = self.DD[key]
            # Verify the value retrieved from the items with the specific key
            self.assertEqual(value, ret_value, "Value collected from the items is incorrect")

    def test_rename(self):
        key = "Hello"
        value = "Dictionary"
        self.DD[key] = value
        # Rename the above key
        new_key = "Renamed"
        self.DD.rename(key, new_key)
        ret_value = self.DD[new_key]
        # Verify that the key is properly renamed
        self.assertEqual(value, ret_value, "Rename key is not successful")


if __name__ == "__main__":
    mp.set_start_method("dragon")

    # Disable the non-deterministic behavior of the python hashing algorithm behavior
    hashseed = os.getenv('PYTHONHASHSEED')
    if not hashseed:
        os.environ['PYTHONHASHSEED'] = '0'
        os.execv(sys.executable, [sys.executable] + sys.argv)
    unittest.main()
