""" This file contains Dragon multi-node acceptance tests for the
`multiprocessing.Pool` object.  The test scales with the total number of CPUs
reported by the allocation, i.e. it becomes tougher on larger allocations.

The test is run with `dragon test_pool.py -f -v`
"""

import unittest
import time
import itertools

import dragon
import multiprocessing as mp
from multiprocessing import TimeoutError

import numpy as np

TIMEOUT1, TIMEOUT2, TIMEOUT3 = 0.82, 0.35, 1.4

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


class TestPoolMultiNode(unittest.TestCase):
    """These are multi-node versions of some of the Multiprocessing Pool unit
    tests."""

    @classmethod
    def setUpClass(cls):
        ncpu = max(2, mp.cpu_count() // 8)
        cls.pool = mp.Pool(ncpu)

    @classmethod
    def tearDownClass(cls):
        cls.pool.terminate()
        cls.pool.join()
        cls.pool = None

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
        self.assertEqual(self.pool.starmap_async(mul, tuples).get(), list(itertools.starmap(mul, tuples)))

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
        self.assertAlmostEqual(stop - start, TIMEOUT1, 1)

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


class TestPoolScalingMultiNode(unittest.TestCase):
    def test_strong_scalability(self) -> None:
        """Trivial process throughput/scalability benchmark:
        https://parsl.readthedocs.io/en/stable/userguide/performance.html
        Strong Scaling
        """

        N = 5000

        maxcpus = np.flip(np.logspace(0, np.log10(max(2, mp.cpu_count() // 8)), num=16))

        params = [1 for _ in range(N)]

        for cpus in maxcpus:
            start = time.monotonic()
            with mp.Pool(int(cpus)) as pool:
                pool.map(time.sleep, params)
            stop = time.monotonic()
            print(f"{N=} {int(cpus)=} {stop-start=} {(stop-start)*cpus/max(maxcpus)=}", flush=True)

    def test_strong_scalability(self) -> None:
        """Trivial process throughput/scalability benchmark:
        https://parsl.readthedocs.io/en/stable/userguide/performance.html
        Weak Scaling
        """

        maxcpus = np.flip(np.logspace(0, np.log10(max(2, mp.cpu_count() // 8)), num=16))

        for cpus in maxcpus:
            N = int(10 * cpus)
            params = [1 for _ in range(N)]
            start = time.monotonic()
            with mp.Pool(int(cpus)) as pool:
                pool.map(time.sleep, params)
            stop = time.monotonic()
            print(f"{N=} {int(cpus)=} {stop-start=}", flush=True)


if __name__ == "__main__":
    mp.set_start_method("dragon")
    unittest.main()
