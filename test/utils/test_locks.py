#!/usr/bin/env python3

import time
import unittest
import mmap
import platform
import multiprocessing as mp
from multiprocessing import shared_memory as shm
from multiprocessing import Array
from parameterized import parameterized
from parameterized import parameterized_class
from dragon.locks import DragonLock, Type
from enum import Enum


class Mem(Enum):
    MMAP = 0
    SHM = 1


# Driver class that contains all shared tests for FIFO vs Greedy and SharedMem vs MMAP
@parameterized_class(
    [
        {"lock_type": Type.FIFO, "mem_type": Mem.MMAP},
        {"lock_type": Type.FIFO, "mem_type": Mem.SHM},
        {"lock_type": Type.FIFOLITE, "mem_type": Mem.MMAP},
        {"lock_type": Type.FIFOLITE, "mem_type": Mem.SHM},
        {"lock_type": Type.GREEDY, "mem_type": Mem.MMAP},
        {"lock_type": Type.GREEDY, "mem_type": Mem.SHM},
    ]
)
class LockTest(unittest.TestCase):

    def setUp(self) -> None:
        self.memsize = DragonLock.size(self.lock_type)
        self.memsize = int(4096.0 * ((self.memsize / 4096.0) + 1.0))
        if self.mem_type == Mem.MMAP:
            self.memobj = mmap.mmap(-1, self.memsize)
        elif self.mem_type == Mem.SHM:
            self.shmemobj = shm.SharedMemory(create=True, size=self.memsize)
            self.memobj = self.shmemobj.buf
        else:
            raise RuntimeError("Invalid memory type")

        self.hdl = None

    def tearDown(self) -> None:
        if self.hdl:
            # If locked, nothing happens
            # If unlocked, guarantees we can do unlock and avoid undefined behavior
            self.hdl.try_lock()
            self.hdl.unlock()
            self.hdl.destroy()
            del self.hdl

        if self.mem_type == Mem.MMAP:
            self.memobj.close()
            del self.memobj

        elif self.mem_type == Mem.SHM:
            self.shmemobj.close()
            self.shmemobj.unlink()
            del self.shmemobj

    # Count atomically, make sure the locks work as expected
    @staticmethod
    def atomic_count(lockmem, iters, tdata) -> None:
        hdl = DragonLock.attach(lockmem)

        for i in range(iters):
            hdl.lock()
            tdata[0] += 1
            hdl.unlock()

        hdl.detach()

    # Used to benchmark lock/unlock performance across multiple processes
    def bench_ops(self, lockmem, warmup, iters, tdata) -> None:
        hdl = DragonLock.attach(lockmem)

        for i in range(warmup + iters):
            hdl.lock()

            if i == warmup:
                start_time = time.monotonic_ns()

            hdl.unlock()

        end_time = time.monotonic_ns()
        total = 1.0 * (end_time - start_time) * 1e-9
        ops = (1.0 * iters) / total

        # Multiprocessing Arrays are thread safe according to docs
        tdata[0] += total
        tdata[1] += ops

        hdl.detach()

    def test_init_single(self) -> None:
        self.hdl = DragonLock.init(self.lock_type, self.memobj)
        self.hdl.lock()
        self.hdl.unlock()

    @unittest.skipIf(platform.system() == "Darwin", "Test not applicable on macOS")
    @parameterized.expand([["2Thread", 2], ["4Thread", 4]])
    @unittest.skipIf(platform.system() == "Darwin", "Test not applicable on macOS")
    def test_lock_count(self, name: str, maxprocs: int) -> None:
        self.hdl = DragonLock.init(self.lock_type, self.memobj)
        self.hdl.lock()

        iters = 1000
        procs = []

        tdata = Array("i", range(1))

        for i in range(maxprocs):
            proc = mp.Process(target=self.atomic_count, args=(self.memobj, iters, tdata))
            proc.start()
            procs.append(proc)

        self.hdl.unlock()

        for proc in procs:
            proc.join()

        self.assertEqual(maxprocs * iters, tdata[0])

    @unittest.skipIf(platform.system() == "Darwin", "Test not applicable on macOS")
    @parameterized.expand([["1Thread", 1], ["2Thread", 2], ["4Thread", 4]])
    @unittest.skipIf(platform.system() == "Darwin", "Test not applicable on macOS")
    def test_multi_lock(self, name: str, maxprocs: int) -> None:
        self.hdl = DragonLock.init(self.lock_type, self.memobj)
        self.hdl.lock()

        warmup = 1000
        iters = 10000
        tdata = Array("d", range(2))
        procs = []

        for i in range(maxprocs):
            proc = mp.Process(target=self.bench_ops, args=(self.memobj, warmup, iters, tdata))
            proc.start()
            procs.append(proc)

        self.hdl.unlock()

        for proc in procs:
            proc.join()

        avg_time = tdata[0] / maxprocs
        avg_ops = tdata[1] / maxprocs
        # print(f"Average time/ops: {avg_time} ({avg_ops} ops/s)")

    # TODO: This currently fails on FIFO locks
    def test_try_lock(self):
        self.hdl = DragonLock.init(self.lock_type, self.memobj)
        self.hdl.lock()
        expected = 0
        res = self.hdl.try_lock()
        self.assertEqual(expected, res)
        self.hdl.unlock()
        expected = 1
        res = self.hdl.try_lock()
        self.assertEqual(expected, res)
        self.hdl.unlock()

    def worker_try_lock(self, lockmem, procid):
        hdl = DragonLock.attach(lockmem)
        res = hdl.try_lock()
        hdl.detach()
        if res != 0:
            raise RuntimeError(f"Process {procid} got lock when it should not have")

    @unittest.skipIf(platform.system() == "Darwin", "Test not applicable on macOS")
    @parameterized.expand([["1Thread", 1], ["2Thread", 2], ["4Thread", 4]])
    @unittest.skipIf(platform.system() == "Darwin", "Test not applicable on macOS")
    def test_multi_try_lock(self, name: str, maxprocs: int) -> None:
        self.hdl = DragonLock.init(self.lock_type, self.memobj)
        self.hdl.lock()

        procs = []
        for i in range(maxprocs):
            proc = mp.Process(target=self.worker_try_lock, args=(self.memobj, i))
            proc.start()
            procs.append(proc)

        time.sleep(1)

        for proc in procs:
            proc.join()

        self.hdl.unlock()

    @unittest.skip("Skipping until double unlock behavior is well defined")
    def test_fail_double_unlock(self):
        self.hdl = DragonLock.init(self.lock_type, self.memobj)
        self.hdl.lock()
        self.hdl.unlock()
        # Should throw an exception on attempting to double unlock
        self.assertRaises(self.hdl.unlock())
        # Delete the handle object so tearDown() doesn't cause runtime errors
        del self.hdl


if __name__ == "__main__":
    unittest.main()
