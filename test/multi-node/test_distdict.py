#!/usr/bin/env python3

import unittest
import socket
import cloudpickle

import dragon.infrastructure.messages as dmsg
import dragon.channels as dch
from dragon.utils import b64encode, b64decode
from dragon.data.ddict import DDict
from dragon.globalservices.node import get_list
import multiprocessing as mp
from dragon.rc import DragonError
from dragon.data.ddict import DDictManagerFull
from dragon.native.queue import Queue
from dragon.native.machine import System, Node
from dragon.native.process import Process
from dragon.infrastructure.policy import Policy

def fillit(d):
    i = 0
    key = "abc"
    try:
        while True:
            d[key] = key
            i+=1
            key += "abc"*i
    except DDictManagerFull:
        pass

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

def check_local_manager(d, q, counter):
    q.put((d.local_manager(), d.main_manager(), d.manager_nodes(), counter))

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
        manager_nodes = b64encode(cloudpickle.dumps([Node(ident=socket.gethostname()) for _ in range(2)]))
        msg = dmsg.DDRegisterClientResponse(42, 43, DragonError.SUCCESS, 0, 2, 3, manager_nodes, 10, 'this is dragon error info')
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

    def test_attach_ddict(self):
        d = DDict(self._managers_per_node, self._num_nodes, self._total_mem_size)
        d['hello'] = 'world'
        d_serialized = d.serialize()
        new_d = DDict.attach(d_serialized)
        self.assertEqual(new_d['hello'], 'world')
        d.detach()
        new_d.destroy()

    def test_fill(self):
        d = DDict(self._managers_per_node, self._num_nodes, self._total_mem_size)
        procs = []
        for i in range(self._num_clients):
            client_proc = mp.Process(target=fillit, args=(d,))
            client_proc.start()
            procs.append(client_proc)
        for i in range(self._num_clients):
            procs[i].join()
        d.destroy()

    def test_placement(self):

        my_alloc = System()
        num_nodes_to_use = int(my_alloc.nnodes/2)
        node_list = my_alloc.nodes

        dict_nodes = node_list[:num_nodes_to_use]
        dict_policies = [Policy(placement=Policy.Placement.HOST_ID, host_id=huid) for huid in dict_nodes]
        d = DDict(None,  None, self._total_mem_size, managers_per_policy=self._managers_per_node, policy=dict_policies)
        procs = []
        q = Queue()
        proc_policies = [Policy(placement=Policy.Placement.HOST_ID, host_id=huid) for huid in node_list]

        for i, policy in enumerate(proc_policies):
            client_proc = Process(target=check_local_manager, args=(d, q, i), policy=policy)
            client_proc.start()
            procs.append(client_proc)

        for i in range(len(procs)):
            local_manager, main_manager, manager_nodes, counter = q.get()
            self.assertIn(main_manager, list(range(self._managers_per_node*len(dict_policies))))
            self.assertSetEqual(set(dict_nodes), set([node.h_uid for node in manager_nodes]))
            if counter < num_nodes_to_use:
                self.assertIsNotNone(local_manager)
            else:
                self.assertIsNone(local_manager)

        for i in range(len(procs)):
            procs[i].join()
        d.destroy()

if __name__ == "__main__":
    unittest.main()
