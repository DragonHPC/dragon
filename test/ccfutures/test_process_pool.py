import dragon
import multiprocessing as mp
import os
import sys
import threading
import time
import unittest
from concurrent import futures
from concurrent.futures.process import BrokenProcessPool

from test import support
from test.support import hashlib_helper

from executor import ExecutorTest, mul
from util import (
    ProcessPoolForkMixin,
    ProcessPoolForkserverMixin,
    ProcessPoolSpawnMixin,
    create_executor_tests,
    setup_module,
)


class EventfulGCObj:
    def __init__(self, mgr):
        self.event = mgr.Event()

    def __del__(self):
        self.event.set()


@unittest.skip("This class does not work without file descriptors")
class ProcessPoolExecutorTest(ExecutorTest):
    @unittest.skipUnless(sys.platform == "win32", "Windows-only process limit")
    def test_max_workers_too_large(self):
        with self.assertRaisesRegex(ValueError, "max_workers must be <= 61"):
            futures.ProcessPoolExecutor(max_workers=62)

    @unittest.skip("Test hangs with Dragon runtime; related to file descriptors")
    def test_killed_child(self):
        # When a child process is abruptly terminated, the whole pool gets
        # "broken".
        print("Test killed child", flush=True)
        futures = [self.executor.submit(time.sleep, 3)]
        # Get one of the processes, and terminate (kill) it
        p = next(iter(self.executor._processes.values()))
        p.terminate()
        for fut in futures:
            self.assertRaises(BrokenProcessPool, fut.result)
        # Submitting other jobs fails as well.
        self.assertRaises(BrokenProcessPool, self.executor.submit, pow, 2, 8)

    @unittest.skip("A process in the process pool was terminated abruptly while the future was running or pending.")
    def test_map_chunksize(self):
        print("Map Chunksize", flush=True)

        def bad_map():
            list(self.executor.map(pow, range(40), range(40), chunksize=-1))

        ref = list(map(pow, range(40), range(40)))
        self.assertEqual(list(self.executor.map(pow, range(40), range(40), chunksize=6)), ref)
        self.assertEqual(list(self.executor.map(pow, range(40), range(40), chunksize=50)), ref)
        self.assertEqual(list(self.executor.map(pow, range(40), range(40), chunksize=40)), ref)
        self.assertRaises(ValueError, bad_map)

    @classmethod
    def _test_traceback(cls):
        raise RuntimeError(123)  # some comment

    @unittest.skip("Runtime error")
    def test_traceback(self):
        # We want ensure that the traceback from the child process is
        # contained in the traceback raised in the main process.
        future = self.executor.submit(self._test_traceback)
        with self.assertRaises(Exception) as cm:
            future.result()

        exc = cm.exception
        self.assertIs(type(exc), RuntimeError)
        self.assertEqual(exc.args, (123,))
        cause = exc.__cause__
        self.assertIs(type(cause), futures.process._RemoteTraceback)
        self.assertIn("raise RuntimeError(123) # some comment", cause.tb)

        with support.captured_stderr() as f1:
            try:
                raise exc
            except RuntimeError:
                sys.excepthook(*sys.exc_info())
        self.assertIn("raise RuntimeError(123) # some comment", f1.getvalue())

    # @hashlib_helper.requires_hashdigest("md5")
    @unittest.skip("Error due to lack of manager support")
    def test_resources_gced_in_workers(self):
        print("Test resources gced in workers", flush=True)
        # Ensure that argument for a job are correctly gc-ed after the job
        # is finished
        mgr = self.get_context().Manager()
        obj = EventfulGCObj(mgr)
        future = self.executor.submit(id, obj)
        future.result()

        self.assertTrue(obj.event.wait(timeout=1))

        # explicitly destroy the object to ensure that EventfulGCObj.__del__()
        # is called while manager is still running.
        support.gc_collect()
        obj = None
        support.gc_collect()

        mgr.shutdown()
        mgr.join()

    @unittest.skip("Root receives unexpected message")
    def test_saturation(self):
        print("Test saturation", flush=True)
        executor = self.executor
        mp_context = self.get_context()
        sem = mp_context.Semaphore(0)
        job_count = 15 * executor._max_workers
        for _ in range(job_count):
            executor.submit(sem.acquire)
        self.assertEqual(len(executor._processes), executor._max_workers)
        for _ in range(job_count):
            sem.release()

    @unittest.skip("Value error - cannot find context for method; channel breaks")
    # @support.requires_gil_enabled("gh-117344: test is flaky without the GIL")
    def test_idle_process_reuse_one(self):
        print("Test idle process reuse one", flush=True)
        executor = self.executor
        assert executor._max_workers >= 4
        if self.get_context().get_start_method(allow_none=False) == "fork":
            raise unittest.SkipTest("Incompatible with the fork start method.")
        executor.submit(mul, 21, 2).result()
        executor.submit(mul, 6, 7).result()
        executor.submit(mul, 3, 14).result()
        self.assertEqual(len(executor._processes), 1)

    @unittest.skip("Value error - cannot find context for method; channel breaks")
    def test_idle_process_reuse_multiple(self):
        print("Test idle process reuse multiple", flush=True)
        executor = self.executor
        assert executor._max_workers <= 5
        if self.get_context().get_start_method(allow_none=False) == "fork":
            raise unittest.SkipTest("Incompatible with the fork start method.")
        executor.submit(mul, 12, 7).result()
        executor.submit(mul, 33, 25)
        executor.submit(mul, 25, 26).result()
        executor.submit(mul, 18, 29)
        executor.submit(mul, 1, 2).result()
        executor.submit(mul, 0, 9)
        self.assertLessEqual(len(executor._processes), 3)
        executor.shutdown()

    @unittest.skip("A process in the process pool was terminated abruptly while the future was running or pending.")
    def test_max_tasks_per_child(self):
        print("Test max tasks per child", flush=True)
        context = self.get_context()
        if context.get_start_method(allow_none=False) == "fork":
            with self.assertRaises(ValueError):
                self.executor_type(1, mp_context=context, max_tasks_per_child=3)
            return
        # not using self.executor as we need to control construction.
        # arguably this could go in another class w/o that mixin.
        executor = self.executor_type(1, mp_context=context, max_tasks_per_child=3)
        f1 = executor.submit(os.getpid)
        original_pid = f1.result()
        # The worker pid remains the same as the worker could be reused
        f2 = executor.submit(os.getpid)
        self.assertEqual(f2.result(), original_pid)
        self.assertEqual(len(executor._processes), 1)
        f3 = executor.submit(os.getpid)
        self.assertEqual(f3.result(), original_pid)

        # A new worker is spawned, with a statistically different pid,
        # while the previous was reaped.
        f4 = executor.submit(os.getpid)
        new_pid = f4.result()
        self.assertNotEqual(original_pid, new_pid)
        self.assertEqual(len(executor._processes), 1)

        executor.shutdown()

    @unittest.skip("A process in the process pool was terminated abruptly while the future was running or pending.")
    def test_max_tasks_per_child_defaults_to_spawn_context(self):
        print("Test max tasks per child defaults to spawn context", flush=True)
        # not using self.executor as we need to control construction.
        # arguably this could go in another class w/o that mixin.
        executor = self.executor_type(1, max_tasks_per_child=3)
        self.assertEqual(executor._mp_context.get_start_method(), "spawn")

    @unittest.skip("A process in the process pool was terminated abruptly while the future was running or pending.")
    def test_max_tasks_early_shutdown(self):
        print("Test max tasks early shutdown")
        context = self.get_context()
        if context.get_start_method(allow_none=False) == "fork":
            raise unittest.SkipTest("Incompatible with the fork start method.")
        # not using self.executor as we need to control construction.
        # arguably this could go in another class w/o that mixin.
        executor = self.executor_type(3, mp_context=context, max_tasks_per_child=1)
        futures = []
        for i in range(6):
            futures.append(executor.submit(mul, i, i))
        executor.shutdown()
        for i, future in enumerate(futures):
            self.assertEqual(future.result(), mul(i, i))

    @unittest.skip("Dragon does not support file descriptors")
    def test_python_finalization_error(self):
        # gh-109047: Catch RuntimeError on thread creation
        # during Python finalization.

        context = self.get_context()

        # gh-109047: Mock the threading.start_joinable_thread() function to inject
        # RuntimeError: simulate the error raised during Python finalization.
        # Block the second creation: create _ExecutorManagerThread, but block
        # QueueFeederThread.
        orig_start_new_thread = threading._start_joinable_thread
        nthread = 0

        def mock_start_new_thread(func, *args, **kwargs):
            nonlocal nthread
            if nthread >= 1:
                raise RuntimeError("can't create new thread at " "interpreter shutdown")
            nthread += 1
            return orig_start_new_thread(func, *args, **kwargs)

        with support.swap_attr(threading, "_start_joinable_thread", mock_start_new_thread):
            executor = self.executor_type(max_workers=2, mp_context=context)
            with executor:
                with self.assertRaises(BrokenProcessPool):
                    list(executor.map(mul, [(2, 3)] * 10))
            executor.shutdown()


create_executor_tests(
    globals(),
    ProcessPoolExecutorTest,
    executor_mixins=(ProcessPoolForkMixin, ProcessPoolForkserverMixin, ProcessPoolSpawnMixin),
)


def setUpModule():
    setup_module()


if __name__ == "__main__":
    mp.set_start_method("dragon")
    unittest.main()
