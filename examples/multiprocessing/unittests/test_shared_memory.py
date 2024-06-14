"""Test multiprocessing Shared_Memory and Heap
"""
import unittest
import unittest.mock
import os
import pickle
import signal
import subprocess
import sys
import time
import warnings
import random
import gc

import test.support
from test.support import hashlib_helper

import dragon  # DRAGON import before multiprocessing

import multiprocessing

try:
    from multiprocessing import shared_memory

    HAS_SHMEM = True
except ImportError:
    HAS_SHMEM = False

from common import (
    BaseTestCase,
    ProcessesMixin,
    setUpModule,
    tearDownModule,
)


@unittest.skip("DRAGON: not implemented")
@unittest.skipUnless(HAS_SHMEM, "requires multiprocessing.shared_memory")
@hashlib_helper.requires_hashdigest("md5")
class WithProcessesTestSharedMemory(BaseTestCase, ProcessesMixin, unittest.TestCase):

    # DRAGON ALLOWED_TYPES = ('processes',)

    @staticmethod
    def _attach_existing_shmem_then_write(shmem_name_or_obj, binary_data):
        if isinstance(shmem_name_or_obj, str):
            local_sms = shared_memory.SharedMemory(shmem_name_or_obj)
        else:
            local_sms = shmem_name_or_obj
        local_sms.buf[: len(binary_data)] = binary_data
        local_sms.close()

    def test_shared_memory_basics(self):
        sms = shared_memory.SharedMemory("test01_tsmb", create=True, size=512)
        self.addCleanup(sms.unlink)

        # Verify attributes are readable.
        self.assertEqual(sms.name, "test01_tsmb")
        self.assertGreaterEqual(sms.size, 512)
        self.assertGreaterEqual(len(sms.buf), sms.size)

        # Modify contents of shared memory segment through memoryview.
        sms.buf[0] = 42
        self.assertEqual(sms.buf[0], 42)

        # Attach to existing shared memory segment.
        also_sms = shared_memory.SharedMemory("test01_tsmb")
        self.assertEqual(also_sms.buf[0], 42)
        also_sms.close()

        # Attach to existing shared memory segment but specify a new size.
        same_sms = shared_memory.SharedMemory("test01_tsmb", size=20 * sms.size)
        self.assertLess(same_sms.size, 20 * sms.size)  # Size was ignored.
        same_sms.close()

        # Creating Shared Memory Segment with -ve size
        with self.assertRaises(ValueError):
            shared_memory.SharedMemory(create=True, size=-2)

        # Attaching Shared Memory Segment without a name
        with self.assertRaises(ValueError):
            shared_memory.SharedMemory(create=False)

        # Test if shared memory segment is created properly,
        # when _make_filename returns an existing shared memory segment name
        with unittest.mock.patch("multiprocessing.shared_memory._make_filename") as mock_make_filename:

            NAME_PREFIX = shared_memory._SHM_NAME_PREFIX
            names = ["test01_fn", "test02_fn"]
            # Prepend NAME_PREFIX which can be '/psm_' or 'wnsm_', necessary
            # because some POSIX compliant systems require name to start with /
            names = [NAME_PREFIX + name for name in names]

            mock_make_filename.side_effect = names
            shm1 = shared_memory.SharedMemory(create=True, size=1)
            self.addCleanup(shm1.unlink)
            self.assertEqual(shm1._name, names[0])

            mock_make_filename.side_effect = names
            shm2 = shared_memory.SharedMemory(create=True, size=1)
            self.addCleanup(shm2.unlink)
            self.assertEqual(shm2._name, names[1])

        if shared_memory._USE_POSIX:
            # Posix Shared Memory can only be unlinked once.  Here we
            # test an implementation detail that is not observed across
            # all supported platforms (since WindowsNamedSharedMemory
            # manages unlinking on its own and unlink() does nothing).
            # True release of shared memory segment does not necessarily
            # happen until process exits, depending on the OS platform.
            with self.assertRaises(FileNotFoundError):
                sms_uno = shared_memory.SharedMemory("test01_dblunlink", create=True, size=5000)

                try:
                    self.assertGreaterEqual(sms_uno.size, 5000)

                    sms_duo = shared_memory.SharedMemory("test01_dblunlink")
                    sms_duo.unlink()  # First shm_unlink() call.
                    sms_duo.close()
                    sms_uno.close()

                finally:
                    sms_uno.unlink()  # A second shm_unlink() call is bad.

        with self.assertRaises(FileExistsError):
            # Attempting to create a new shared memory segment with a
            # name that is already in use triggers an exception.
            there_can_only_be_one_sms = shared_memory.SharedMemory("test01_tsmb", create=True, size=512)

        if shared_memory._USE_POSIX:
            # Requesting creation of a shared memory segment with the option
            # to attach to an existing segment, if that name is currently in
            # use, should not trigger an exception.
            # Note:  Using a smaller size could possibly cause truncation of
            # the existing segment but is OS platform dependent.  In the
            # case of MacOS/darwin, requesting a smaller size is disallowed.
            class OptionalAttachSharedMemory(shared_memory.SharedMemory):
                _flags = os.O_CREAT | os.O_RDWR

            ok_if_exists_sms = OptionalAttachSharedMemory("test01_tsmb")
            self.assertEqual(ok_if_exists_sms.size, sms.size)
            ok_if_exists_sms.close()

        # Attempting to attach to an existing shared memory segment when
        # no segment exists with the supplied name triggers an exception.
        with self.assertRaises(FileNotFoundError):
            nonexisting_sms = shared_memory.SharedMemory("test01_notthere")
            nonexisting_sms.unlink()  # Error should occur on prior line.

        sms.close()

        # Test creating a shared memory segment with negative size
        with self.assertRaises(ValueError):
            sms_invalid = shared_memory.SharedMemory(create=True, size=-1)

        # Test creating a shared memory segment with size 0
        with self.assertRaises(ValueError):
            sms_invalid = shared_memory.SharedMemory(create=True, size=0)

        # Test creating a shared memory segment without size argument
        with self.assertRaises(ValueError):
            sms_invalid = shared_memory.SharedMemory(create=True)

    def test_shared_memory_across_processes(self):
        # bpo-40135: don't define shared memory block's name in case of
        # the failure when we run multiprocessing tests in parallel.
        sms = shared_memory.SharedMemory(create=True, size=512)
        self.addCleanup(sms.unlink)

        # Verify remote attachment to existing block by name is working.
        p = self.Process(target=self._attach_existing_shmem_then_write, args=(sms.name, b"howdy"))
        p.daemon = True
        p.start()
        p.join()
        self.assertEqual(bytes(sms.buf[:5]), b"howdy")

        # Verify pickling of SharedMemory instance also works.
        p = self.Process(target=self._attach_existing_shmem_then_write, args=(sms, b"HELLO"))
        p.daemon = True
        p.start()
        p.join()
        self.assertEqual(bytes(sms.buf[:5]), b"HELLO")

        sms.close()

    @unittest.skipIf(os.name != "posix", "not feasible in non-posix platforms")
    def test_shared_memory_SharedMemoryServer_ignores_sigint(self):
        # bpo-36368: protect SharedMemoryManager server process from
        # KeyboardInterrupt signals.
        smm = multiprocessing.managers.SharedMemoryManager()
        smm.start()

        # make sure the manager works properly at the beginning
        sl = smm.ShareableList(range(10))

        # the manager's server should ignore KeyboardInterrupt signals, and
        # maintain its connection with the current process, and success when
        # asked to deliver memory segments.
        os.kill(smm._process.pid, signal.SIGINT)

        sl2 = smm.ShareableList(range(10))

        # test that the custom signal handler registered in the Manager does
        # not affect signal handling in the parent process.
        with self.assertRaises(KeyboardInterrupt):
            os.kill(os.getpid(), signal.SIGINT)

        smm.shutdown()

    @unittest.skipIf(os.name != "posix", "resource_tracker is posix only")
    def test_shared_memory_SharedMemoryManager_reuses_resource_tracker(self):
        # bpo-36867: test that a SharedMemoryManager uses the
        # same resource_tracker process as its parent.
        cmd = """if 1:
            from multiprocessing.managers import SharedMemoryManager


            smm = SharedMemoryManager()
            smm.start()
            sl = smm.ShareableList(range(10))
            smm.shutdown()
        """
        rc, out, err = test.support.script_helper.assert_python_ok("-c", cmd)

        # Before bpo-36867 was fixed, a SharedMemoryManager not using the same
        # resource_tracker process as its parent would make the parent's
        # tracker complain about sl being leaked even though smm.shutdown()
        # properly released sl.
        self.assertFalse(err)

    def test_shared_memory_SharedMemoryManager_basics(self):
        smm1 = multiprocessing.managers.SharedMemoryManager()
        with self.assertRaises(ValueError):
            smm1.SharedMemory(size=9)  # Fails if SharedMemoryServer not started
        smm1.start()
        lol = [smm1.ShareableList(range(i)) for i in range(5, 10)]
        lom = [smm1.SharedMemory(size=j) for j in range(32, 128, 16)]
        doppleganger_list0 = shared_memory.ShareableList(name=lol[0].shm.name)
        self.assertEqual(len(doppleganger_list0), 5)
        doppleganger_shm0 = shared_memory.SharedMemory(name=lom[0].name)
        self.assertGreaterEqual(len(doppleganger_shm0.buf), 32)
        held_name = lom[0].name
        smm1.shutdown()
        if sys.platform != "win32":
            # Calls to unlink() have no effect on Windows platform; shared
            # memory will only be released once final process exits.
            with self.assertRaises(FileNotFoundError):
                # No longer there to be attached to again.
                absent_shm = shared_memory.SharedMemory(name=held_name)

        with multiprocessing.managers.SharedMemoryManager() as smm2:
            sl = smm2.ShareableList("howdy")
            shm = smm2.SharedMemory(size=128)
            held_name = sl.shm.name
        if sys.platform != "win32":
            with self.assertRaises(FileNotFoundError):
                # No longer there to be attached to again.
                absent_sl = shared_memory.ShareableList(name=held_name)

    def test_shared_memory_ShareableList_basics(self):
        sl = shared_memory.ShareableList(["howdy", b"HoWdY", -273.154, 100, None, True, 42])
        self.addCleanup(sl.shm.unlink)

        # Verify attributes are readable.
        self.assertEqual(sl.format, "8s8sdqxxxxxx?xxxxxxxx?q")

        # Exercise len().
        self.assertEqual(len(sl), 7)

        # Exercise index().
        with warnings.catch_warnings():
            # Suppress BytesWarning when comparing against b'HoWdY'.
            warnings.simplefilter("ignore")
            with self.assertRaises(ValueError):
                sl.index("100")
            self.assertEqual(sl.index(100), 3)

        # Exercise retrieving individual values.
        self.assertEqual(sl[0], "howdy")
        self.assertEqual(sl[-2], True)

        # Exercise iterability.
        self.assertEqual(tuple(sl), ("howdy", b"HoWdY", -273.154, 100, None, True, 42))

        # Exercise modifying individual values.
        sl[3] = 42
        self.assertEqual(sl[3], 42)
        sl[4] = "some"  # Change type at a given position.
        self.assertEqual(sl[4], "some")
        self.assertEqual(sl.format, "8s8sdq8sxxxxxxx?q")
        with self.assertRaisesRegex(ValueError, "exceeds available storage"):
            sl[4] = "far too many"
        self.assertEqual(sl[4], "some")
        sl[0] = "encodés"  # Exactly 8 bytes of UTF-8 data
        self.assertEqual(sl[0], "encodés")
        self.assertEqual(sl[1], b"HoWdY")  # no spillage
        with self.assertRaisesRegex(ValueError, "exceeds available storage"):
            sl[0] = "encodées"  # Exactly 9 bytes of UTF-8 data
        self.assertEqual(sl[1], b"HoWdY")
        with self.assertRaisesRegex(ValueError, "exceeds available storage"):
            sl[1] = b"123456789"
        self.assertEqual(sl[1], b"HoWdY")

        # Exercise count().
        with warnings.catch_warnings():
            # Suppress BytesWarning when comparing against b'HoWdY'.
            warnings.simplefilter("ignore")
            self.assertEqual(sl.count(42), 2)
            self.assertEqual(sl.count(b"HoWdY"), 1)
            self.assertEqual(sl.count(b"adios"), 0)

        # Exercise creating a duplicate.
        sl_copy = shared_memory.ShareableList(sl, name="test03_duplicate")
        try:
            self.assertNotEqual(sl.shm.name, sl_copy.shm.name)
            self.assertEqual("test03_duplicate", sl_copy.shm.name)
            self.assertEqual(list(sl), list(sl_copy))
            self.assertEqual(sl.format, sl_copy.format)
            sl_copy[-1] = 77
            self.assertEqual(sl_copy[-1], 77)
            self.assertNotEqual(sl[-1], 77)
            sl_copy.shm.close()
        finally:
            sl_copy.shm.unlink()

        # Obtain a second handle on the same ShareableList.
        sl_tethered = shared_memory.ShareableList(name=sl.shm.name)
        self.assertEqual(sl.shm.name, sl_tethered.shm.name)
        sl_tethered[-1] = 880
        self.assertEqual(sl[-1], 880)
        sl_tethered.shm.close()

        sl.shm.close()

        # Exercise creating an empty ShareableList.
        empty_sl = shared_memory.ShareableList()
        try:
            self.assertEqual(len(empty_sl), 0)
            self.assertEqual(empty_sl.format, "")
            self.assertEqual(empty_sl.count("any"), 0)
            with self.assertRaises(ValueError):
                empty_sl.index(None)
            empty_sl.shm.close()
        finally:
            empty_sl.shm.unlink()

    def test_shared_memory_ShareableList_pickling(self):
        sl = shared_memory.ShareableList(range(10))
        self.addCleanup(sl.shm.unlink)

        serialized_sl = pickle.dumps(sl)
        deserialized_sl = pickle.loads(serialized_sl)
        self.assertTrue(isinstance(deserialized_sl, shared_memory.ShareableList))
        self.assertTrue(deserialized_sl[-1], 9)
        self.assertFalse(sl is deserialized_sl)
        deserialized_sl[4] = "changed"
        self.assertEqual(sl[4], "changed")

        # Verify data is not being put into the pickled representation.
        name = "a" * len(sl.shm.name)
        larger_sl = shared_memory.ShareableList(range(400))
        self.addCleanup(larger_sl.shm.unlink)
        serialized_larger_sl = pickle.dumps(larger_sl)
        self.assertTrue(len(serialized_sl) == len(serialized_larger_sl))
        larger_sl.shm.close()

        deserialized_sl.shm.close()
        sl.shm.close()

    def test_shared_memory_cleaned_after_process_termination(self):
        cmd = """if 1:
            import os, time, sys
            from multiprocessing import shared_memory

            # Create a shared_memory segment, and send the segment name
            sm = shared_memory.SharedMemory(create=True, size=10)
            sys.stdout.write(sm.name + '\\n')
            sys.stdout.flush()
            time.sleep(100)
        """
        with subprocess.Popen(
            [sys.executable, "-E", "-c", cmd], stdout=subprocess.PIPE, stderr=subprocess.PIPE
        ) as p:
            name = p.stdout.readline().strip().decode()

            # killing abruptly processes holding reference to a shared memory
            # segment should not leak the given memory segment.
            p.terminate()
            p.wait()

            deadline = time.monotonic() + support.LONG_TIMEOUT
            t = 0.1
            while time.monotonic() < deadline:
                time.sleep(t)
                t = min(t * 2, 5)
                try:
                    smm = shared_memory.SharedMemory(name, create=False)
                except FileNotFoundError:
                    break
            else:
                raise AssertionError(
                    "A SharedMemory segment was leaked after" " a process was abruptly terminated."
                )

            if os.name == "posix":
                # A warning was emitted by the subprocess' own
                # resource_tracker (on Windows, shared memory segments
                # are released automatically by the OS).
                err = p.stderr.read().decode()
                self.assertIn(
                    "resource_tracker: There appear to be 1 leaked "
                    "shared_memory objects to clean up at shutdown",
                    err,
                )


@unittest.skip("bug filed PE-40902")
class WithProcessesTestHeap(BaseTestCase, ProcessesMixin, unittest.TestCase):

    # DRAGON ALLOWED_TYPES = ('processes',)

    def setUp(self):
        super().setUp()
        # Make pristine heap for these tests
        self.old_heap = multiprocessing.heap.BufferWrapper._heap
        multiprocessing.heap.BufferWrapper._heap = multiprocessing.heap.Heap()

    def tearDown(self):
        multiprocessing.heap.BufferWrapper._heap = self.old_heap
        super().tearDown()

    def test_heap(self):
        iterations = 5000
        maxblocks = 50
        blocks = []

        # get the heap object
        heap = multiprocessing.heap.BufferWrapper._heap
        heap._DISCARD_FREE_SPACE_LARGER_THAN = 0

        # create and destroy lots of blocks of different sizes
        for i in range(iterations):
            size = int(random.lognormvariate(0, 1) * 1000)
            b = multiprocessing.heap.BufferWrapper(size)
            blocks.append(b)
            if len(blocks) > maxblocks:
                i = random.randrange(maxblocks)
                del blocks[i]
            del b

        # verify the state of the heap
        with heap._lock:
            all = []
            free = 0
            occupied = 0
            for L in list(heap._len_to_seq.values()):
                # count all free blocks in arenas
                for arena, start, stop in L:
                    all.append((heap._arenas.index(arena), start, stop, stop - start, "free"))
                    free += stop - start
            for arena, arena_blocks in heap._allocated_blocks.items():
                # count all allocated blocks in arenas
                for start, stop in arena_blocks:
                    all.append((heap._arenas.index(arena), start, stop, stop - start, "occupied"))
                    occupied += stop - start

            self.assertEqual(free + occupied, sum(arena.size for arena in heap._arenas))

            all.sort()

            for i in range(len(all) - 1):
                (arena, start, stop) = all[i][:3]
                (narena, nstart, nstop) = all[i + 1][:3]
                if arena != narena:
                    # Two different arenas
                    self.assertEqual(stop, heap._arenas[arena].size)  # last block
                    self.assertEqual(nstart, 0)  # first block
                else:
                    # Same arena: two adjacent blocks
                    self.assertEqual(stop, nstart)

        # test free'ing all blocks
        random.shuffle(blocks)
        while blocks:
            blocks.pop()

        self.assertEqual(heap._n_frees, heap._n_mallocs)
        self.assertEqual(len(heap._pending_free_blocks), 0)
        self.assertEqual(len(heap._arenas), 0)
        self.assertEqual(len(heap._allocated_blocks), 0, heap._allocated_blocks)
        self.assertEqual(len(heap._len_to_seq), 0)

    def test_free_from_gc(self):
        # Check that freeing of blocks by the garbage collector doesn't deadlock
        # (issue #12352).
        # Make sure the GC is enabled, and set lower collection thresholds to
        # make collections more frequent (and increase the probability of
        # deadlock).
        if not gc.isenabled():
            gc.enable()
            self.addCleanup(gc.disable)
        thresholds = gc.get_threshold()
        self.addCleanup(gc.set_threshold, *thresholds)
        gc.set_threshold(10)

        # perform numerous block allocations, with cyclic references to make
        # sure objects are collected asynchronously by the gc
        for i in range(5000):
            a = multiprocessing.heap.BufferWrapper(1)
            b = multiprocessing.heap.BufferWrapper(1)
            # circular references
            a.buddy = b
            b.buddy = a


# DRAGON
if __name__ == "__main__":
    setUpModule()
    unittest.main()
    tearDownModule()
