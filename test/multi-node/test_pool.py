"""This file contains Dragon multi-node acceptance tests for the
`multiprocessing.Pool` object.  The test scales with the total number of CPUs
reported by the allocation, i.e. it becomes tougher on larger allocations.

The test is run with `dragon test_pool.py -f -v`
"""

import unittest
import time
import itertools
import socket
import os

import dragon
import multiprocessing as mp
from multiprocessing import TimeoutError

import numpy as np
from dragon.native.machine import cpu_count, System, Node
from dragon.native.pool import Pool
from dragon.infrastructure.policy import Policy

TIMEOUT1, TIMEOUT2, TIMEOUT3 = 0.82, 0.35, 1.4
TIMEOUT_DELTA_TOL = 1.0


def sqr(x, wait=0.0):
    time.sleep(wait)
    return x * x


def mul(x, y):
    return x * y


def raise_large_valuerror(wait):
    time.sleep(wait)
    raise ValueError("x" * 1024**2)


def identity(x):
    return x


def placement_info(vendor):
    hostname = socket.gethostname()
    pid = os.getpid()
    cpus_allowed_list = -1
    with open(f"/proc/{pid}/status") as f:
        for _, line in enumerate(f):
            split_line = line.split(":")
            if split_line[0] == "Cpus_allowed_list":
                cpus_allowed_list = split_line[1].strip("\n").strip("\t")
                break
    visible_devices = None
    if vendor == "Nvidia":
        visible_devices = os.getenv("CUDA_VISIBLE_DEVICES")
    elif vendor == "AMD":
        visible_devices = os.getenv("ROCR_VISIBLE_DEVICES")

    return (
        hostname,
        cpus_allowed_list,
        visible_devices,
    )


class TestPoolMultiNode(unittest.TestCase):
    """These are multi-node versions of some of the Multiprocessing Pool unit
    tests."""

    def setUp(self):
        ncpu = max(2, mp.cpu_count() // 8)
        self.pool = mp.Pool(ncpu)
        # self.pool = mp.Pool(4)
        while not self.pool._start_barrier_passed.is_set():
            time.sleep(0.25)

    def tearDown(self):
        # self.pool.terminate()
        self.pool.close()
        self.pool.join()
        self.pool = None

    def test_apply(self):
        papply = self.pool.apply
        self.assertEqual(papply(sqr, (5,)), sqr(5))
        self.assertEqual(papply(sqr, (), {"x": 3}), sqr(x=3))

    def test_map(self):
        pmap = self.pool.map
        self.assertEqual(pmap(sqr, list(range(10))), list(map(sqr, list(range(10)))))
        self.assertEqual(pmap(sqr, list(range(100)), chunksize=20), list(map(sqr, list(range(100)))))

    def test_starmap(self):
        psmap = self.pool.starmap
        tuples = list(zip(range(10), range(9, -1, -1)))
        self.assertEqual(psmap(mul, tuples), list(itertools.starmap(mul, tuples)))
        tuples = list(zip(range(100), range(99, -1, -1)))
        self.assertEqual(psmap(mul, tuples, chunksize=20), list(itertools.starmap(mul, tuples)))

    def test_starmap_async(self):
        tuples = list(zip(range(100), range(99, -1, -1)))
        test = self.pool.starmap_async(mul, tuples)
        ref = itertools.starmap(mul, tuples)
        self.assertEqual(test.get(), list(ref))

    def test_map_async(self):
        self.assertEqual(self.pool.map_async(sqr, list(range(10))).get(), list(map(sqr, list(range(10)))))

    def test_map_async_callbacks(self):
        call_args = []
        self.pool.map_async(int, ["1"], callback=call_args.append, error_callback=call_args.append).wait()
        self.assertEqual(1, len(call_args))
        self.assertEqual([1], call_args[0])
        self.pool.map_async(int, ["a"], callback=call_args.append, error_callback=call_args.append).wait()
        self.assertEqual(2, len(call_args))
        self.assertIsInstance(call_args[1], ValueError)

    def test_async(self):
        res = self.pool.apply_async(
            sqr,
            (
                7,
                TIMEOUT1,
            ),
        )
        start = time.monotonic()
        self.assertEqual(res.get(), 49)
        stop = time.monotonic()

        elap = stop - start
        self.assertGreaterEqual(elap, TIMEOUT1)
        self.assertLess(elap, (TIMEOUT1 + TIMEOUT_DELTA_TOL))

    def test_async_timeout(self):
        res = self.pool.apply_async(sqr, (6, TIMEOUT2 + 1.0))
        start = time.monotonic()
        try:
            res.get(timeout=TIMEOUT2)
            self.assertTrue(False)
        except TimeoutError:
            pass
        stop = time.monotonic()
        self.assertAlmostEqual(stop - start, TIMEOUT2, 1)

    def test_imap(self):
        it = self.pool.imap(sqr, list(range(10)))
        self.assertEqual(list(it), list(map(sqr, list(range(10)))))

        it = self.pool.imap(sqr, list(range(10)))
        for i in range(10):
            self.assertEqual(next(it), i * i)
        self.assertRaises(StopIteration, it.__next__)

        it = self.pool.imap(sqr, list(range(1000)), chunksize=100)
        for i in range(1000):
            self.assertEqual(next(it), i * i)
        self.assertRaises(StopIteration, it.__next__)

    def test_imap_unordered(self):
        it = self.pool.imap_unordered(sqr, list(range(10)))
        self.assertEqual(sorted(it), list(map(sqr, list(range(10)))))

        it = self.pool.imap_unordered(sqr, list(range(1000)), chunksize=100)
        self.assertEqual(sorted(it), list(map(sqr, list(range(1000)))))

    def test_hostname_node_restriction(self):
        my_alloc = System()
        num_procs_per_node = 2
        num_nodes_to_use = int(my_alloc.nnodes / 2)
        node_list = my_alloc.nodes
        num_procs = num_nodes_to_use * num_procs_per_node
        gpu_vendor = None
        acceptable_hostnames = []

        # create a process group that runs on a subset of nodes

        policy_list = []
        for node_num in range(num_nodes_to_use):
            node_name = Node(node_list[node_num]).hostname
            policy_list.append(Policy(placement=Policy.Placement.HOST_NAME, host_name=node_name))
            acceptable_hostnames.append(node_name)

        p = Pool(policy=policy_list, processes_per_policy=num_procs_per_node)
        res = p.map_async(placement_info, [gpu_vendor] * num_procs)

        self.assertEqual(len(res.get()), num_procs)
        for result in res.get():
            hostname, _, _ = result
            # check that proc is on a node it was meant to land on
            self.assertIn(
                hostname, acceptable_hostnames, msg=f"Got hostname {hostname} which is not in {acceptable_hostnames}"
            )
        p.close()
        p.join()


class TestPoolScalingMultiNode(unittest.TestCase):
    def test_strong_scalability(self) -> None:
        """Trivial process throughput/scalability benchmark:
        https://parsl.readthedocs.io/en/stable/userguide/performance.html
        Strong Scaling
        """

        N = 500

        maxcpus = np.flip(np.logspace(np.log10(5), np.log10(max(2, mp.cpu_count() // 8)), num=8, dtype=int))

        params = [0.01 for _ in range(N)]

        for cpus in maxcpus:
            start = time.monotonic()
            with mp.Pool(int(cpus)) as pool:
                pool.map(time.sleep, params)
            pool.join()
            stop = time.monotonic()
            print(f"{N=} {cpus=} {stop-start=} {(stop-start)*cpus/max(maxcpus)=}", flush=True)

    def test_weak_scalability(self) -> None:
        """Trivial process throughput/scalability benchmark:
        https://parsl.readthedocs.io/en/stable/userguide/performance.html
        Weak Scaling
        """

        maxcpus = np.flip(list(set(np.logspace(0, np.log10(max(2, mp.cpu_count() // 8)), num=8, dtype=int))))

        for cpus in maxcpus:
            N = int(10 * cpus)
            params = [0.01 for _ in range(N)]
            start = time.monotonic()
            with mp.Pool(int(cpus)) as pool:
                pool.map(time.sleep, params)
            pool.join()
            stop = time.monotonic()
            print(f"{N=} {int(cpus)=} {stop-start=}", flush=True)


if __name__ == "__main__":
    mp.set_start_method("dragon")
    unittest.main()
