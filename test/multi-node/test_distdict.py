#!/usr/bin/env python3

import unittest

import dragon.infrastructure.messages as dmsg
import dragon.channels as dch
from dragon.utils import b64encode, b64decode
from dragon.data.ddict.ddict import DDict
from dragon.globalservices.node import get_list
import multiprocessing as mp
from dragon.rc import DragonError

def register_and_detach(d):
    d.detach()

def set_ops(d, client_id):
    key1 = 'hello' + str(client_id)
    d[key1] = 'world' + str(client_id)
    d.detach()

def get_ops(d, client_id):
    key1 = 'hello' + str(client_id)
    assert d[key1] == 'world' + str(client_id)
    d.detach()

def del_ops(d, client_id):
    key1 = 'hello' + str(client_id)
    del d[key1]
    d.detach()

def contains_ops(d, client_id):
    key1 = 'hello' + str(client_id)
    assert key1 in d
    d.detach()

class TestDDict(unittest.TestCase):
    @classmethod
    def setUpClass(self) -> None:
        # Create a dragon dictionary on a single node with multiple manager processes
        self._managers_per_node = 1 # 1 Managers per node
        self._num_nodes = len(get_list()) # Collect the total number of nodes
        self._total_mem_size = self._num_nodes*(1024*1024*1024) # 1 GB for each node
        self._num_clients = self._num_nodes * 2 # 2 clients per node

    def test_local_channel(self):
        ch = dch.Channel.make_process_local()
        ch.detach()

    def test_infra_message(self):
        msg = dmsg.GSHalted(42)
        ser = msg.serialize()
        newmsg = dmsg.parse(ser)
        self.assertIsInstance(newmsg, dmsg.GSHalted)
        newser = 'eJyrVoovSVayUjA21lFQKklMBzItawE+xQWS'
        from_str = dmsg.parse(newser)
        self.assertIsInstance(from_str, dmsg.GSHalted)
        newser = 'eJyrVoovSVayUjA21lFQKklMBzItawE+xQWS\n'
        from_str = dmsg.parse(newser)
        self.assertIsInstance(from_str, dmsg.GSHalted)
        newline = b'\n\n\n\n'
        encoded = b64encode(newline)
        decoded = b64decode(encoded)
        self.assertEqual(newline, decoded)
        newline = '\n\n\n\n'
        encoded = b64encode(newline.encode('utf-8'))
        decoded = b64decode(encoded)
        self.assertEqual(newline, decoded.decode('utf-8'))

    def test_capnp_message (self):
        msg = dmsg.DDRegisterClient(42, "HelloWorld", "Dragon")
        ser = msg.serialize()

        newmsg = dmsg.parse(ser)
        self.assertIsInstance(newmsg, dmsg.DDRegisterClient)

    def test_ddict_client_response_message(self):
        msg = dmsg.DDRegisterClientResponse(42, 43, DragonError.SUCCESS, 0, 2, 'this is dragon error info')
        ser = msg.serialize()
        newmsg = dmsg.parse(ser)
        self.assertIsInstance(newmsg, dmsg.DDRegisterClientResponse)

    def test_bringup_teardown(self):
        d = DDict(self._managers_per_node, self._num_nodes, self._total_mem_size)
        d.destroy()

    def test_detach_client(self):
        d = DDict(self._managers_per_node, self._num_nodes, self._total_mem_size)
        procs = []
        for i in range(self._num_clients):
            client_proc = mp.Process(target=register_and_detach, kwargs={"d": d})
            client_proc.start()
            procs.append(client_proc)

        for i in range(self._num_clients):
            procs[i].join()

        for i in range(self._num_clients):
            procs[i].terminate()

        d.destroy()

    def test_set(self):
        d = DDict(self._managers_per_node, self._num_nodes, self._total_mem_size)
        procs = []
        for i in range(self._num_clients):
            client_proc = mp.Process(target=set_ops, args=(d, i))
            client_proc.start()
            procs.append(client_proc)

        for i in range(self._num_clients):
            procs[i].join()

        for i in range(self._num_clients):
            procs[i].terminate()

        for i in range(self._num_clients):
            self.assertTrue('hello' + str(i) in d)

        d.destroy()

    def test_get(self):
        d = DDict(self._managers_per_node, self._num_nodes, self._total_mem_size)
        procs = []

        # put a bunch of key-value pairs to dictionary
        for i in range(self._num_clients):
            client_proc = mp.Process(target=set_ops, args=(d, i))
            client_proc.start()
            procs.append(client_proc)

        for i in range(self._num_clients):
            procs[i].join()

        for i in range(self._num_clients):
            procs[i].terminate()

        procs = []
        # get key-value pairs from dictionary
        for i in range(self._num_clients):
            client_proc = mp.Process(target=get_ops, args=(d, i))
            client_proc.start()
            procs.append(client_proc)

        for i in range(self._num_clients):
            procs[i].join()

        for i in range(self._num_clients):
            procs[i].terminate()

        d.destroy()

    def test_pop(self):
        d = DDict(self._managers_per_node, self._num_nodes, self._total_mem_size)
        procs = []

        # put a bunch of key-value pairs to dictionary
        for i in range(self._num_clients):
            client_proc = mp.Process(target=set_ops, args=(d, i))
            client_proc.start()
            procs.append(client_proc)

        for i in range(self._num_clients):
            procs[i].join()

        for i in range(self._num_clients):
            procs[i].terminate()

        procs = []
        # delete key-value pairs from dictionary
        for i in range(self._num_clients):
            client_proc = mp.Process(target=del_ops, args=(d, i))
            client_proc.start()
            procs.append(client_proc)

        for i in range(self._num_clients):
            procs[i].join()

        for i in range(self._num_clients):
            procs[i].terminate()

        for i in range(self._num_clients):
            self.assertFalse('hello' + str(i) in d)

        d.destroy()

    def test_contains_key(self):
        d = DDict(self._managers_per_node, self._num_nodes, self._total_mem_size)
        procs = []

        # put a bunch of key-value pairs to dictionary
        for i in range(self._num_clients):
            client_proc = mp.Process(target=set_ops, args=(d, i))
            client_proc.start()
            procs.append(client_proc)

        for i in range(self._num_clients):
            procs[i].join()

        # test contains key
        procs = []
        for i in range(self._num_clients):
            client_proc = mp.Process(target=contains_ops, args=(d, i))
            client_proc.start()
            procs.append(client_proc)

        for i in range(self._num_clients):
            procs[i].join()

        for i in range(self._num_clients):
            procs[i].terminate()

        d.destroy()

    def test_len(self):
        d = DDict(self._managers_per_node, self._num_nodes, self._total_mem_size)
        self.assertEqual(len(d), 0)
        procs = []

        # put a bunch of key-value pairs to dictionary
        for i in range(self._num_clients):
            client_proc = mp.Process(target=set_ops, args=(d, i))
            client_proc.start()
            procs.append(client_proc)

        for i in range(self._num_clients):
            procs[i].join()

        self.assertEqual(len(d), self._num_clients)
        d.destroy()

    def test_clear(self):
        d = DDict(self._managers_per_node, self._num_nodes, self._total_mem_size)
        procs = []

        # put a bunch of key-value pairs to dictionary
        for i in range(self._num_clients):
            client_proc = mp.Process(target=set_ops, args=(d, i))
            client_proc.start()
            procs.append(client_proc)

        for i in range(self._num_clients):
            procs[i].join()

        self.assertEqual(len(d), self._num_clients)
        d.clear()
        self.assertEqual(len(d), 0)
        d.destroy()


    @unittest.skip('Not yet implemented')
    def test_iter(self):
        d = DDict(self._managers_per_node, self._num_nodes, self._total_mem_size)
        procs = []

        # put a bunch of key-value pairs to dictionary
        for i in range(self._num_clients):
            client_proc = mp.Process(target=set_ops, args=(d, i))
            client_proc.start()
            procs.append(client_proc)

        for i in range(self._num_clients):
            procs[i].join()

        for key in d:
            num = key[5:]
            self.assertEqual(d[key], 'world'+num)
        d.destroy()

    def test_keys(self):
        d = DDict(self._managers_per_node, self._num_nodes, self._total_mem_size)
        procs = []

        # put a bunch of key-value pairs to dictionary
        for i in range(self._num_clients):
            client_proc = mp.Process(target=set_ops, args=(d, i))
            client_proc.start()
            procs.append(client_proc)

        for i in range(self._num_clients):
            procs[i].join()

        ddict_keys = d.keys()
        for key in ddict_keys:
            num = key[5:]
            self.assertEqual(d[key], 'world'+num)

        d.destroy()


if __name__ == "__main__":
    unittest.main()
