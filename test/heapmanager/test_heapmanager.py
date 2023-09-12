#!/usr/bin/env python3

import unittest
import mmap
from multiprocessing import shared_memory as shm
from parameterized import parameterized

from dragon.heapmanager import Heap
from dragon.locks import Type


# @MCB: Handy for debugging failing tests since we might run into segfaults and that will crash
#       the whole unittest runner.  Uncomment the line in __main__ to use.
def trace(frame, event, arg):
    print("%s, %s:%d" % (event, frame.f_code.co_filename, frame.f_lineno))
    return trace

# @MCB: Intended util function for comapring stringified heap dumps against known-good dump files.
#       Currently does nothing but spam stdout.
def check_heap_dump(expected, actual):
    expected_lines = expected.split('\n')
    actual_lines = actual.split('\n')
    for idx in range(len(expected_lines)):
        print(expected_lines[idx])
        print(actual_lines[idx])


class MallocTest():

    # Test basic init and verify stat values for a few different heap sizes
    @parameterized.expand([
        ["128mb", 27, 5, 2048],
        ["1gb", 30, 12, 4096],
        ["4gb", 32, 12, 4096]
    ])
    def test_init_and_stats(self, name: str, max_pwr: int, min_pwr: int, alignment: int):
        # Destroy the handle we got from setUp so we can re-init to test different sizes
        self.heap_hdl.destroy()
        self.heap_hdl = Heap.init(
            max_pwr, min_pwr, alignment, Type.FIFO, self.memobj)

        expct_num_freelists = max_pwr - min_pwr + 1
        self.assertEqual(1 << min_pwr, self.heap_hdl.segment_size,
                         "Handle: Incorrect segment size")
        self.assertEqual(1 << (expct_num_freelists - 1), self.heap_hdl.num_segments,
                         "Handle: Incorrect segment count")
        self.assertEqual(expct_num_freelists, self.heap_hdl.num_freelists,
                         "Handle: Incorrect number of free lists")
        self.assertEqual(0, self.heap_hdl.recovery_needed,
                         "Handle: Unexpected needed recovery")
        self.assertEqual(1, self.heap_hdl.init_handle,
                         "Handle: Expected init handle to be set")

        for i in range(self.heap_hdl.num_freelists):
            self.assertNotEqual(None, self.heap_hdl.free_lists(i),
                                "Handle: Free list {} shouldn't be empty but is".format(i))

        stats = self.heap_hdl.get_stats()
        self.assertEqual(1 << max_pwr, stats.total_size,
                         "Stats: Incorrect heap size")
        self.assertEqual(1 << min_pwr, stats.segment_size,
                         "Stats: Incorrect segment size")
        self.assertEqual(0, stats.utilization_pct,
                         "Stats: Incorrect expected utilization")

    # Verify we can stick more than one handle onto an initialized heap
    # @MCB TODO: Parameterize this to attach/detach random numbers of extra handles
    def test_attach_detach(self):
        heap_hdl2 = Heap.attach(self.memobj)

        self.assertEqual(self.heap_hdl.num_segments, heap_hdl2.num_segments,
                         "Number of segments don't match")
        self.assertEqual(self.heap_hdl.num_freelists, heap_hdl2.num_freelists,
                         "Number of free lists don't match")
        self.assertEqual(self.heap_hdl.exclusive_access, heap_hdl2.exclusive_access,
                         "Exclusive access pointers should be the same")
        for i in range(self.heap_hdl.num_freelists):
            self.assertEqual(self.heap_hdl.free_lists(i), heap_hdl2.free_lists(i),
                             "Free lists being empty doesn't match up")

        heap_hdl2.detach()

    # Do a basic allocation on the heap, verify we can acutally write into it, validate stats are correct
    def test_dragon_malloc(self):
        tmp_obj = self.heap_hdl.malloc(16)

        # Assert that we can actually write into our malloc'd space
        tmp_obj[0:16] = b'abcdefghijklmnop'
        self.assertEqual(b'abcdefghijklmnop', tmp_obj.tobytes())

        # Assert stats are correct based on a 4gb heap
        stats = self.heap_hdl.get_stats()

        self.assertTrue(stats.utilization_pct < 0.000096,
                        "Utilization percent greater than 0.000096, expected about 0.000095")
        self.assertTrue(stats.utilization_pct > 0.000094,
                        "Utilization percent lower than 0.000094, expected about 0.000095")
        self.assertEqual(21, stats.num_block_sizes,
                         "Incorrect number of block sizes after malloc")

        for i in range(stats.num_block_sizes - 1):
            self.assertEqual(1, stats.free_block_count(i),
                             "Expected number of free blocks to be 1, but got {}".format(stats.free_block_count(i)))

        self.assertEqual(0, stats.free_block_count(stats.num_block_sizes - 1),
                         "Expected final free block size to have no free blocks, but has {}".format(stats.free_block_count(stats.num_block_sizes - 1)))


    # Allocate, verify we can write, verify we can free, verify stats are correct
    def test_dragon_free(self):
        tmp_obj = self.heap_hdl.malloc(16)

        # Assert that we can actually write into our malloc'd space
        tmp_obj[0:16] = b'abcdefghijklmnop'
        self.assertEqual(b'abcdefghijklmnop', tmp_obj.tobytes())

        # Free it
        self.heap_hdl.free(tmp_obj)

        stats = self.heap_hdl.get_stats()

        self.assertEqual(0.0, stats.utilization_pct,
                         "Utilization percent not 0")

        for i in range(stats.num_block_sizes - 1):
            self.assertEqual(0, stats.free_block_count(i),
                             "Expected number of free blocks to be 0, but got {}".format(stats.free_block_count(i)))

        self.assertEqual(1, stats.free_block_count(stats.num_block_sizes - 1),
                         "Expected final free block size to have 1 free block, but got {}".format(stats.free_block_count(stats.num_block_sizes - 1)))

    # Intent of this test is to overwrite and corrupt allocations.  Because python wraps memoryview pointers into actual objects,
    #    we can't actually test this, since the returned memoryview objects know their own bounds.  Prior testing with raw pointers
    #    verified this worked.  Implemented an internal cython method to continue testing that corruption process?
    @unittest.skip("Skipping corrupt for now due to python bounds checking")
    def test_dragon_corrupt(self):
        mem1 = self.heap_hdl.malloc(32)
        mem2 = self.heap_hdl.malloc(5000)

        for i in range(5100):
            mem1[i] = b'e'

        # Don't catch exception here, should free correctly
        self.heap_hdl.free(mem1)

        # Should catch a recovery exception since we corrupted mem2
        with self.assertRaises(RuntimeError):
            self.heap_hdl.free(mem2)

        self.heap_hdl.recover()
        self.heap_hdl.free(mem2)

        stats = self.heap_hdl.get_stats()
        self.assertEqual(0.0, stats.utilization_pct,
                         "Utilization percent after recovery and free should be 0")

        for i in range(stats.num_block_sizes - 1):
            self.assertEqual(0, stats.free_block_count(i),
                             "Expected number of free blocks to be 0, but got {}".format(stats.free_block_count(i)))

        self.assertEqual(1, stats.free_block_count(stats.num_block_sizes - 1),
                         "Expected final free block size to have 1 free block, but got {}".format(stats.free_block_count(stats.num_block_sizes - 1)))


    # Make a bunch of garbage allocations, assert that all sizes that should fit into a 4GB heap are valid and
    #      all that aren't are invalid (throw exceptions)
    def test_random_malloc(self):
        good_values = [1382, 250915223, 1916, 2121290834, 449, 177372358, 2115, 360690951, 3524, 286665256,
                       191, 1241134, 3778, 29086950, 1927, 36346056, 703, 9029225, 3507, 84239849, 3085,
                       4864365, 81, 2227456, 2455, 2991, 40179276, 3798, 35426764, 832, 41696064,
                       3579, 3584099, 1329, 2473419, 2783, 314163, 1926, 207293, 1339, 20486641, 452,
                       710, 25659, 3314, 1828, 812, 957, 3262, 807,
                       3862, 4096]

        bad_values = [810373453, 1546034581, 79469219, 65647013,
                      2911824890, 3223138597, 3072378300, 31477418]

        valid_allocs = 0
        num_tests = len(good_values)
        allocs = [None] * num_tests

        # Perform mallocs with good values
        for i in range(num_tests):
            mem = self.heap_hdl.malloc(good_values[i])
            allocs[valid_allocs] = mem
            valid_allocs += 1

        # Perform mallocs with bad values, expect and catch raised exceptions
        for i in range(len(bad_values)):
            with self.assertRaises(RuntimeError):
                self.heap_hdl.malloc(bad_values[i])

        self.assertEqual(52, valid_allocs, "Expected 52 valid allocations")

        # Free valid allocations
        for i in range(valid_allocs):
            self.heap_hdl.free(allocs[i])


class MMAPTest(MallocTest, unittest.TestCase):

    def setUp(self) -> None:
        self.max_pwr = 32
        self.min_pwr = 12
        self.alignment = 4096
        self.memobj = mmap.mmap(-1, Heap.size(self.max_pwr, self.min_pwr, self.alignment, Type.FIFO))
        self.heap_hdl = Heap.init(
            self.max_pwr, self.min_pwr, self.alignment, Type.FIFO, self.memobj)

    def tearDown(self) -> None:
        if self.heap_hdl:
            self.heap_hdl.destroy()

        self.memobj.close()
        del self.memobj


class SharedMemTest(MallocTest, unittest.TestCase):

    def setUp(self) -> None:
        self.max_pwr = 32
        self.min_pwr = 12
        self.alignment = 4096
        self.shmemobj = shm.SharedMemory(create=True, size=Heap.size(self.max_pwr, self.min_pwr, self.alignment, Type.FIFO))
        self.memobj = self.shmemobj.buf
        self.heap_hdl = Heap.init(
            self.max_pwr, self.min_pwr, self.alignment, Type.FIFO, self.memobj)

    def tearDown(self) -> None:
        if self.heap_hdl:
            self.heap_hdl.destroy()

        self.shmemobj.close()
        self.shmemobj.unlink()
        del self.shmemobj


if __name__ == '__main__':
    unittest.main(verbosity=2)
    # sys.settrace(trace)
