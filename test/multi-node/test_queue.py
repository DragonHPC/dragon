"""This file contains Dragon multi-node acceptance tests for the
`multiprocessing.Queue` object.  The test scales with the total number of CPUs
reported by the allocation, i.e. it becomes tougher on larger allocations.
The test is run with `dragon test_queue.py -f -v`
"""

import unittest
import random

import dragon
import multiprocessing as mp
from dragon.native.machine import System, Node
from dragon.native.process import Process
from dragon.infrastructure.policy import Policy
import dragon.native.queue as native

max_msg_size = 1024  # limit to 1KB


def _joiner(q, item):
    q.put(item)
    q.join()


class TestQueueMultiNode(unittest.TestCase):
    @classmethod
    def _writer(self, id, q, nitems, msg_list, qver):
        for i in range(nitems):
            msg_idx = id * nitems + i
            q.put((msg_idx, msg_list[i]))
            qver.put((msg_idx, msg_list[i]))
        q.close()
        qver.close()

    @classmethod
    def _reader(self, id, q, nitems):
        for i in range(nitems):
            q.get()

        q.close()

    def test_multi_producer_consumer(self):
        """Start 2 processes per cpu and have each of them send 16 messages to
        each other. Message sizes rotate between 1 byte and 1 Mbyte"""

        num_readers = num_writers = max(2, mp.cpu_count() // 32)

        msg_list = {}  # key is the msg identifier, value is the payload
        nitems = 42  # Seems like the right value.

        for i in range(nitems):
            msg_list[i] = random.randbytes(max_msg_size)

        q = mp.Queue()
        q_ver = mp.Queue(maxsize=nitems * num_writers)  # used to verify that all messages are gotten from the queue

        writers = []
        for i in range(num_writers):
            p = mp.Process(target=self._writer, args=(i, q, nitems, msg_list, q_ver))
            p.start()
            writers.append(p)

        readers = []
        reader_qlist = []
        for i in range(num_readers):
            p = mp.Process(target=self._reader, args=(i, q, nitems))
            p.start()
            readers.append(p)

        for p in writers:
            p.join()

        for p in readers:
            p.join()

        self.assertTrue(q_ver.qsize() == nitems * num_writers)

        while not q_ver.empty():
            _id, payload = q_ver.get()
            if _id in msg_list:
                del msg_list[_id]

        self.assertEqual(len(msg_list), 0)
        self.assertTrue(q_ver.empty())

        self.assertTrue(q.empty())

        q.close()
        q_ver.close()

    def test_policy(self):
        my_alloc = System()
        node_list = my_alloc.nodes
        qlist = []
        channel_nodes = set()

        for node_id in node_list:
            node = Node(node_id)
            policy = Policy(placement=Policy.Placement.HOST_NAME, host_name=node.hostname)
            nQueue = native.Queue(policy=policy)
            channel_nodes.add(nQueue.node)
            qlist.append(nQueue)

        self.assertEqual(len(channel_nodes), len(node_list))
        print(f"Created Queues on {len(channel_nodes)} nodes ... ", flush=True, end="")

        qlist[0].put("Hello World")

        for i in range(3):
            for from_idx in range(len(node_list)):
                to_idx = (from_idx + 1) % len(node_list)
                qlist[to_idx].put(qlist[from_idx].get())

        text = qlist[0].get()

        self.assertEqual(
            text, "Hello World", "The string 'Hello World' did not match after we were done passing it around."
        )

    def test_policy_context(self):
        my_alloc = System()
        node_list = my_alloc.nodes
        qlist = []
        channel_nodes = set()

        for node_id in node_list:
            node = Node(node_id)
            with Policy(placement=Policy.Placement.HOST_NAME, host_name=node.hostname) as policy:
                nQueue = native.Queue()
            channel_nodes.add(nQueue.node)
            qlist.append(nQueue)

        self.assertEqual(len(channel_nodes), len(node_list))
        print(f"Created Queues on {len(channel_nodes)} nodes ... ", flush=True, end="")

        qlist[0].put("Hello World")

        for i in range(3):
            for from_idx in range(len(node_list)):
                to_idx = (from_idx + 1) % len(node_list)
                qlist[to_idx].put(qlist[from_idx].get())

        text = qlist[0].get()

        self.assertEqual(
            text, "Hello World", "The string 'Hello World' did not match after we were done passing it around."
        )

    def test_joinable(self):
        """Test joinability of a multi-node queue"""

        q = mp.JoinableQueue()
        num_processes = max(2, mp.cpu_count() // 8)

        self.assertTrue(q._joinable == True)

        items = [f"JQueue-item-{i}" for i in range(num_processes)]

        processes = []
        for item in items:
            p = mp.Process(target=_joiner, args=(q, item))
            p.start()
            processes.append(p)

        for _ in range(num_processes):
            item = q.get()
            self.assertTrue(item in items)

        for p in processes:  # make sure they are all blocked
            p.join(timeout=0)
            self.assertTrue(p.exitcode == None)

        for p in processes:  # unblock workers
            q.task_done()

        for p in processes:
            p.join(timeout=None)
            self.assertTrue(p.exitcode == 0)


if __name__ == "__main__":
    mp.set_start_method("dragon", force=True)
    unittest.main()
