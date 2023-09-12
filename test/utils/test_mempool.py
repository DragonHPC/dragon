#!/usr/bin/env python3

import time
import unittest
import multiprocessing as mp
import time
from dragon.managed_memory import MemoryPool, MemoryAlloc, DragonPoolError, DragonPoolAllocationNotAvailable
import os

SMALL_TIMEOUT_VAL = 0.2
BIGGER_TIMEOUT_VAL = 1.2

def worker_attach(q=None, pool_ser=None, mem_ser=None):
    if pool_ser is not None and q is not None:
        mpool = MemoryPool.attach(pool_ser)
        mem = mpool.alloc(512)
        memview = mem.get_memview()
        memview[0:5] = b"Hello"
        q.put(mem.serialize())
        mpool.detach()
    elif mem_ser is not None:
        mem = MemoryAlloc.attach(mem_ser)
        memview = mem.get_memview()
        memview[0:1] = b"Y"
    else:
        raise RuntimeError("Invalid arguments to worker_attach")

def worker_blocking_alloc(q, pool_ser, pid):
    mpool = MemoryPool.attach(pool_ser)
    mem = mpool.alloc_blocking(3000*pid+100)
    memview = mem.get_memview()
    memview[0:5] = b"Hello"
    q.put(mem.serialize())
    mpool.detach()

def worker_blocking_alloc_timeout(pool_ser, TIMEOUT_VAL):
    mpool = MemoryPool.attach(pool_ser)
    try:
        mem = mpool.alloc_blocking(3100,timeout=TIMEOUT_VAL)
        rc = 1
    except TimeoutError as ex:
        rc = 0
    except DragonPoolError:
        rc = 2
    finally:
        mpool.detach()

    exit(rc)

def worker_attach_via_pickle(q, mpool):
    mem = mpool.alloc(512)
    memview = mem.get_memview()
    memview[:5] = b"Howdy"
    q.put(mem.serialize())
    mpool.detach()

class MemPoolTest(unittest.TestCase):
    mpool = None

    @classmethod
    def setUpClass(cls):
        # Create a 32kb memory pool ahead of time for all tests
        cls.mpool = MemoryPool(32768, "mpool_test", 1, None)

    @classmethod
    def tearDownClass(cls):
        cls.mpool.destroy()

    def test_exhaust_pool(self):
        allocations = []
        try:
            while True:
                mem = self.mpool.alloc(1)
                allocations.append(mem)
        except DragonPoolAllocationNotAvailable as ex:
            self.assertTrue("DRAGON_DYNHEAP_REQUESTED_SIZE_NOT_AVAILABLE" in str(ex))

        for mem in allocations:
            mem.free()

    def test_memory_serialize_attach(self):
        mem1 = self.mpool.alloc(512)
        mem_ser = mem1.serialize()

        mem2 = MemoryAlloc.attach(mem_ser)

        memview1 = mem1.get_memview()
        memview2 = mem2.get_memview()

        memview1[0:5] = b"Hello"
        self.assertEqual(memview1[0:5], memview2[0:5])

        memview2[0:1] = b"Y"
        self.assertEqual(memview1[0:5], memview2[0:5])
        mem1.free()

    def test_check_allocations(self):
        mem1 = self.mpool.alloc(512)
        mem2 = self.mpool.alloc(512)

        allocs = self.mpool.get_allocations()
        self.assertEqual(2, allocs.num_allocs)

        for i in range(allocs.num_allocs):
            self.assertEqual(0, allocs.alloc_type(i).value)
            self.assertTrue(self.mpool.allocation_exists(allocs.alloc_type(i), allocs.alloc_id(i)))

        mem1.free()
        mem2.free()

    def test_multiprocess_pool(self):
        pool_ser = self.mpool.serialize()

        q = mp.Queue()
        proc = mp.Process(target=worker_attach, args=(q, pool_ser, None))
        proc.start()
        proc.join()

        mem_ser = q.get()
        mem = MemoryAlloc.attach(mem_ser)
        memview = mem.get_memview()
        self.assertEqual(b"Hello", memview[0:5])
        mem.free()

    def test_multiprocess_pool_via_pickle(self):
        q = mp.Queue()
        proc = mp.Process(target=worker_attach_via_pickle, args=(q, self.mpool))
        proc.start()
        proc.join()

        mem_ser = q.get()
        mem = MemoryAlloc.attach(mem_ser)
        memview = mem.get_memview()
        self.assertEqual(b"Howdy", memview[:5])
        mem.free()

    def test_multiprocess_alloc(self):
        msg = b"Hello"
        mem = self.mpool.alloc(512)
        memview = mem.get_memview()
        memview[0:5] = msg
        mem_ser = mem.serialize()

        proc = mp.Process(target=worker_attach, args=(None, None, mem_ser))
        proc.start()
        proc.join()
        self.assertEqual(b"Yello", memview[0:5])
        mem.free()

    def test_multiprocess_blocking_alloc(self):
        pool_ser = self.mpool.serialize()
        # Getting a big allocation so subsequent allocs will block.
        mem = self.mpool.alloc(32000)
        proc_list = []
        q = mp.Queue()
        for k in range(10):
            proc = mp.Process(target=worker_blocking_alloc, args=(q, pool_ser, k))
            proc_list.append(proc)
            proc.start()

        time.sleep(1)
        mem.free()

        for k in range(10):
            mem_ser = q.get()
            mem = MemoryAlloc.attach(mem_ser)
            memview = mem.get_memview()
            self.assertEqual(b"Hello", memview[0:5])
            mem.free()

        for k in range(10):
            proc_list[k].join()

    def test_small_blocking_timeout(self):
        pool_ser = self.mpool.serialize()
        # Getting a big allocation so subsequent allocs will block.
        mem = self.mpool.alloc(32000)

        proc = mp.Process(target=worker_blocking_alloc_timeout, args=(pool_ser,SMALL_TIMEOUT_VAL))
        start = time.monotonic()
        proc.start()
        proc.join()
        delta = time.monotonic() - start

        rc = proc.exitcode

        mem.free()

        self.assertEqual(rc, 0, "Blocking Alloc process did not timeout.")

        self.assertGreater(delta, SMALL_TIMEOUT_VAL, 'time lapsed should be bigger than timeout.')
        self.assertLessEqual(delta, 1+SMALL_TIMEOUT_VAL, "blocking allocation timeout took too long.")

    def test_bigger_blocking_timeout(self):
        pool_ser = self.mpool.serialize()
        # Getting a big allocation so subsequent allocs will block.
        mem = self.mpool.alloc(32000)

        proc = mp.Process(target=worker_blocking_alloc_timeout, args=(pool_ser,BIGGER_TIMEOUT_VAL))
        start = time.monotonic()
        proc.start()
        proc.join()
        delta = time.monotonic() - start

        rc = proc.exitcode

        mem.free()

        self.assertEqual(rc, 0, "Blocking Alloc process did not timeout.")

        self.assertGreater(delta, BIGGER_TIMEOUT_VAL, 'time lapsed should be bigger than timeout.')
        self.assertLessEqual(delta, 1+BIGGER_TIMEOUT_VAL, "blocking allocation timeout took too long.")

    def test_zero_blocking_timeout(self):
        pool_ser = self.mpool.serialize()
        # Getting a big allocation so subsequent allocs will block.
        mem = self.mpool.alloc(32000)

        proc = mp.Process(target=worker_blocking_alloc_timeout, args=(pool_ser,0))
        start = time.monotonic()
        proc.start()
        proc.join()
        delta = time.monotonic() - start

        rc = proc.exitcode

        mem.free()

        self.assertEqual(rc, 2, "Blocking Alloc process timed out with zero timeout.")

        self.assertGreater(delta, 0, 'There should have been a near immediate return.')
        self.assertLessEqual(delta, 1, "blocking allocation timeout took too long.")


    def test_big_allocation(self):
        mem_bytes = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES')
        big_size = int(mem_bytes * 0.55)
        big_pool = MemoryPool(big_size, "big_pool_test", 2)
        mem = big_pool.alloc(big_size)
        memview = mem.get_memview()
        memview[big_size-1] = 42
        self.assertEqual(memview[big_size-1], 42)
        mem.free()
        big_pool.destroy()

if __name__ == '__main__':
    mp.set_start_method('spawn', force=True)
    unittest.main(verbosity=2)
