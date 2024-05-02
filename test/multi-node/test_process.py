""" This file contains Dragon multi-node acceptance tests for the
`multiprocessing.Process` object. The test scales with the total number of CPUs
reported by the allocation, i.e. it becomes tougher on larger allocations.

The test is run with `dragon test_process.py -f -v`
"""

import unittest
import time
import socket
import os 

import dragon
import multiprocessing as mp
from dragon.globalservices.process import query, multi_join, this_process
from dragon.native.machine import cpu_count, current, System, Node
from dragon.native.process import Process 
from dragon.infrastructure.policy import Policy 

def inception(nnew: int, q: mp.Queue, ev1: mp.Event, ev2: mp.Event, sem: mp.Semaphore) -> None:

    ev1.wait(timeout=None)

    for _ in range(nnew):

        if not sem.acquire(timeout=0.1):  # try for a while
            break  # stop spawning

        p = mp.Process(target=inception, args=(nnew, q, ev1, ev2, sem))
        p.start()
        q.put(p.sentinel)

    ev2.wait(timeout=None)

def placement_gpu_info(sleep_time, q, vendor=None):
    hostname = socket.gethostname()
    if vendor is not None:
        if vendor == 'Nvidia':
            visible_devices=int(os.getenv("CUDA_VISIBLE_DEVICES"))
        elif vendor == 'AMD':
            visible_devices=int(os.getenv("ROCR_VISIBLE_DEVICES"))
    else: 
        visible_devices=None
        # this sleep is important until Process Group holds a history of puids
    q.put((hostname, visible_devices,))

class TestProcessMultiNode(unittest.TestCase):
    def test_inception(self) -> None:
        """Have processes spawn `nnew` processes, until `nchildren` have been
        spawned."""

        nchildren = max(2, mp.cpu_count())
        nnew = 4

        q = mp.Queue(maxsize=nchildren)
        sem = mp.Semaphore(value=nchildren)
        ev1 = mp.Event()
        ev2 = mp.Event()

        processes = []
        for _ in range(nnew):
            sem.acquire()
            p = mp.Process(target=inception, args=(nnew, q, ev1, ev2, sem))
            p.start()
            q.put(p.sentinel)
            processes.append(p)

        ev1.set()

        puids = []
        while not len(puids) == nchildren:
            child_puid = q.get(timeout=None)
            puids.append(child_puid)

        parents = puids + [this_process.my_puid]

        for puid in puids:
            descr = query(puid)
            self.assertTrue(descr.p_uid == puid)
            self.assertTrue(descr.p_p_uid in parents)

        ev2.set()

        mp.connection.wait(puids, timeout=None)

        start = time.monotonic()
        ready = multi_join(puids, timeout=None, join_all=True)  # should exit "immediately"
        stop = time.monotonic()

        # self.assertTrue(2 > (stop - start))  # performance of GS

        for puid, ecode in ready[0]:
            self.assertTrue(ecode == 0)
            self.assertTrue(puid in puids)

        for p in processes:
            self.assertTrue(p.exitcode == 0)
    
    def test_policy(self) -> None:
        my_alloc = System()
        node_list = my_alloc.nodes
        node = Node(node_list[-1])
        q = mp.Queue()
        cwd = os.getcwd()
        if node.gpu_vendor is None:
            args = (5,q,)
            policy = Policy(placement=Policy.Placement.HOST_NAME,host_name=node.hostname)
        else:
            args = (5,q,node.gpu_vendor,)
            policy = Policy(placement=Policy.Placement.HOST_NAME, host_name=node.hostname, device=Policy.Device.GPU, gpu_affinity=[node.gpus[-1]])
        #using native process to take template
        proc = Process(target=placement_gpu_info, args=args, policy=policy)         
        proc.start()
        hostname, gpu_affinity = q.get()
        self.assertEqual(hostname, node.hostname)
        if node.gpu_vendor is not None:
            self.assertEqual(gpu_affinity, node.gpus[-1])

if __name__ == "__main__":
    mp.set_start_method("dragon")
    unittest.main()
