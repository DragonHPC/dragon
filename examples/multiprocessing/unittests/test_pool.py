"""Test multiprocessing.Pool()
"""
import sys
import time
import weakref
import gc
import itertools

import unittest
import test.support
import test.support.script_helper
from test.support import hashlib_helper
from test import support

import dragon  # DRAGON import before multiprocessing

import multiprocessing
from multiprocessing import util
from multiprocessing.managers import RemoteError

from common import (
    BaseTestCase,
    ProcessesMixin,
    ManagerMixin,
    ThreadsMixin,
    TimingWrapper,
    DELTA,
    TIMEOUT1,
    TIMEOUT2,
    setUpModule,
    tearDownModule,
)

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


class CountedObject(object):
    n_instances = 0

    def __new__(cls):
        cls.n_instances += 1
        return object.__new__(cls)

    def __del__(self):
        type(self).n_instances -= 1


class SayWhenError(ValueError):
    pass


def exception_throwing_generator(total, when):
    if when == -1:
        raise SayWhenError("Somebody said when")
    for i in range(total):
        if i == when:
            raise SayWhenError("Somebody said when")
        yield i



class WithProcessesTestPool(BaseTestCase, ProcessesMixin, unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.pool = cls.Pool(4)

    @classmethod
    def tearDownClass(cls):
        cls.pool.terminate()
        cls.pool.join()
        cls.pool = None
        super().tearDownClass()

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
        call_args = self.manager.list() if self.TYPE == "manager" else []
        self.pool.map_async(int, ["1"], callback=call_args.append, error_callback=call_args.append).wait()
        self.assertEqual(1, len(call_args))
        self.assertEqual([1], call_args[0])
        self.pool.map_async(int, ["a"], callback=call_args.append, error_callback=call_args.append).wait()
        self.assertEqual(2, len(call_args))
        self.assertIsInstance(call_args[1], ValueError)

    @unittest.skip("bug filed PE-40898. Fails with native pool. Need to handle failure of put differently.")
    def test_map_unplicklable(self):
        # Issue #19425 -- failure to pickle should not cause a hang
        if self.TYPE == "threads":
            self.skipTest("test not appropriate for {}".format(self.TYPE))

        class A(object):
            def __reduce__(self):
                raise RuntimeError("cannot pickle")

        with self.assertRaises(RuntimeError):
            self.pool.map(sqr, [A()] * 10)

    def test_map_chunksize(self):
        try:
            self.pool.map_async(sqr, [], chunksize=1).get(timeout=TIMEOUT1)
        except multiprocessing.TimeoutError:
            self.fail("pool.map_async with chunksize stalled on null list")

    def test_map_handle_iterable_exception(self):
        if self.TYPE == "manager":
            self.skipTest("test not appropriate for {}".format(self.TYPE))

        # SayWhenError seen at the very first of the iterable
        with self.assertRaises(SayWhenError):
            self.pool.map(sqr, exception_throwing_generator(1, -1), 1)
        # again, make sure it's reentrant
        with self.assertRaises(SayWhenError):
            self.pool.map(sqr, exception_throwing_generator(1, -1), 1)

        with self.assertRaises(SayWhenError):
            self.pool.map(sqr, exception_throwing_generator(10, 3), 1)

        class SpecialIterable:
            def __iter__(self):
                return self

            def __next__(self):
                raise SayWhenError

            def __len__(self):
                return 1

        with self.assertRaises(SayWhenError):
            self.pool.map(sqr, SpecialIterable(), 1)
        with self.assertRaises(SayWhenError):
            self.pool.map(sqr, SpecialIterable(), 1)

    def test_async(self):
        res = self.pool.apply_async(
            sqr,
            (
                7,
                TIMEOUT1,
            ),
        )
        get = TimingWrapper(res.get)
        self.assertEqual(get(), 49)
        self.assertTimingAlmostEqual(get.elapsed, TIMEOUT1)

    def test_async_timeout(self):
        res = self.pool.apply_async(sqr, (6, TIMEOUT2 + 1.0))
        get = TimingWrapper(res.get)
        self.assertRaises(multiprocessing.TimeoutError, get, timeout=TIMEOUT2)
        self.assertTimingAlmostEqual(get.elapsed, TIMEOUT2)

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

    def test_imap_handle_iterable_exception(self):
        if self.TYPE == "manager":
            self.skipTest("test not appropriate for {}".format(self.TYPE))

        # SayWhenError seen at the very first of the iterable
        it = self.pool.imap(sqr, exception_throwing_generator(1, -1), 1)
        self.assertRaises(SayWhenError, it.__next__)
        # again, make sure it's reentrant
        it = self.pool.imap(sqr, exception_throwing_generator(1, -1), 1)
        self.assertRaises(SayWhenError, it.__next__)

        it = self.pool.imap(sqr, exception_throwing_generator(10, 3), 1)
        for i in range(3):
            self.assertEqual(next(it), i * i)
        self.assertRaises(SayWhenError, it.__next__)

        # SayWhenError seen at start of problematic chunk's results
        it = self.pool.imap(sqr, exception_throwing_generator(20, 7), 2)
        for i in range(6):
            self.assertEqual(next(it), i * i)
        self.assertRaises(SayWhenError, it.__next__)
        it = self.pool.imap(sqr, exception_throwing_generator(20, 7), 4)
        for i in range(4):
            self.assertEqual(next(it), i * i)
        self.assertRaises(SayWhenError, it.__next__)

    def test_imap_unordered(self):
        it = self.pool.imap_unordered(sqr, list(range(10)))
        self.assertEqual(sorted(it), list(map(sqr, list(range(10)))))

        it = self.pool.imap_unordered(sqr, list(range(1000)), chunksize=100)
        self.assertEqual(sorted(it), list(map(sqr, list(range(1000)))))

    def test_imap_unordered_handle_iterable_exception(self):
        if self.TYPE == "manager":
            self.skipTest("test not appropriate for {}".format(self.TYPE))

        # SayWhenError seen at the very first of the iterable
        it = self.pool.imap_unordered(sqr, exception_throwing_generator(1, -1), 1)
        self.assertRaises(SayWhenError, it.__next__)
        # again, make sure it's reentrant
        it = self.pool.imap_unordered(sqr, exception_throwing_generator(1, -1), 1)
        self.assertRaises(SayWhenError, it.__next__)

        it = self.pool.imap_unordered(sqr, exception_throwing_generator(10, 3), 1)
        expected_values = list(map(sqr, list(range(10))))
        with self.assertRaises(SayWhenError):
            # imap_unordered makes it difficult to anticipate the SayWhenError
            for i in range(10):
                value = next(it)
                self.assertIn(value, expected_values)
                expected_values.remove(value)

        it = self.pool.imap_unordered(sqr, exception_throwing_generator(20, 7), 2)
        expected_values = list(map(sqr, list(range(20))))
        with self.assertRaises(SayWhenError):
            for i in range(20):
                value = next(it)
                self.assertIn(value, expected_values)
                expected_values.remove(value)


    def test_make_pool(self):
        expected_error = RemoteError if self.TYPE == "manager" else ValueError

        self.assertRaises(expected_error, self.Pool, -1)
        self.assertRaises(expected_error, self.Pool, 0)

        if self.TYPE != "manager":
            p = self.Pool(3)
            try:
                self.assertEqual(3, len(p._pool))
            finally:
                p.close()
                p.join()

    def test_terminate(self):
        result = self.pool.map_async(time.sleep, [0.1 for i in range(10000)], chunksize=1)
        self.pool.terminate()
        join = TimingWrapper(self.pool.join)
        join()
        # Sanity check the pool didn't wait for all tasks to finish
        self.assertLess(join.elapsed, 2.0)

    def test_empty_iterable(self):
        # See Issue 12157
        p = self.Pool(1)

        self.assertEqual(p.map(sqr, []), [])
        self.assertEqual(list(p.imap(sqr, [])), [])
        self.assertEqual(list(p.imap_unordered(sqr, [])), [])
        self.assertEqual(p.map_async(sqr, []).get(), [])

        p.close()
        p.join()

    def test_context(self):
        if self.TYPE == "processes":
            L = list(range(10))
            expected = [sqr(i) for i in L]
            with self.Pool(2) as p:
                r = p.map_async(sqr, L)
                self.assertEqual(r.get(), expected)
            p.join()
            self.assertRaises(ValueError, p.map_async, sqr, L)

    @classmethod
    def _test_traceback(cls):
        raise RuntimeError(123)  # some comment

    @unittest.skip("bug filed PE-40898")
    def test_traceback(self):
        # We want ensure that the traceback from the child process is
        # contained in the traceback raised in the main process.
        if self.TYPE == "processes":
            with self.Pool(1) as p:
                try:
                    p.apply(self._test_traceback)
                except Exception as e:
                    exc = e
                else:
                    self.fail("expected RuntimeError")
            p.join()
            self.assertIs(type(exc), RuntimeError)
            self.assertEqual(exc.args, (123,))
            cause = exc.__cause__
            self.assertIs(type(cause), multiprocessing.pool.RemoteTraceback)
            self.assertIn("raise RuntimeError(123) # some comment", cause.tb)

            with test.support.captured_stderr() as f1:
                try:
                    raise exc
                except RuntimeError:
                    sys.excepthook(*sys.exc_info())
            self.assertIn("raise RuntimeError(123) # some comment", f1.getvalue())
            # _helper_reraises_exception should not make the error
            # a remote exception
            with self.Pool(1) as p:
                try:
                    p.map(sqr, exception_throwing_generator(1, -1), 1)
                except Exception as e:
                    exc = e
                else:
                    self.fail("expected SayWhenError")
                self.assertIs(type(exc), SayWhenError)
                self.assertIs(exc.__cause__, None)
            p.join()

    @classmethod
    def _test_wrapped_exception(cls):
        raise RuntimeError("foo")

    @unittest.skip("bug filed CIRRUS-1473.")
    def test_wrapped_exception(self):
        # Issue #20980: Should not wrap exception when using thread pool
        with self.Pool(1) as p:
            with self.assertRaises(RuntimeError):
                p.apply(self._test_wrapped_exception)
        p.join()

    def test_map_no_failfast(self):
        # Issue #23992: the fail-fast behaviour when an exception is raised
        # during map() would make Pool.join() deadlock, because a worker
        # process would fill the result queue (after the result handler thread
        # terminated, hence not draining it anymore).

        t_start = time.monotonic()

        with self.assertRaises(ValueError):
            with self.Pool(2) as p:
                try:
                    p.map(raise_large_valuerror, [0, 1])
                finally:
                    time.sleep(0.5)
                    p.close()
                    p.join()

        # check that we indeed waited for all jobs
        self.assertGreater(time.monotonic() - t_start, 0.9)

    @unittest.skip("bug filed CIRRUS-1473.")
    def test_release_task_refs(self):
        # Issue #29861: task arguments and results should not be kept
        # alive after we are done with them.
        objs = [CountedObject() for i in range(10)]
        refs = [weakref.ref(o) for o in objs]
        self.pool.map(identity, objs)

        del objs
        gc.collect()  # For PyPy or other GCs.
        time.sleep(DELTA)  # let threaded cleanup code run
        self.assertEqual(set(wr() for wr in refs), {None})
        # With a process pool, copies of the objects are returned, check
        # they were released too.
        self.assertEqual(CountedObject.n_instances, 0)

    def test_enter(self):
        if self.TYPE == "manager":
            self.skipTest("test not applicable to manager")

        pool = self.Pool(1)
        with pool:
            pass
            # call pool.terminate()
        # pool is no longer running

        with self.assertRaises(ValueError):
            # bpo-35477: pool.__enter__() fails if the pool is not running
            with pool:
                pass
        pool.join()

    @unittest.skip("bug filed CIRRUS-1473")
    def test_resource_warning(self):
        if self.TYPE == "manager":
            self.skipTest("test not applicable to manager")

        pool = self.Pool(1)
        pool.terminate()
        pool.join()

        # force state to RUN to emit ResourceWarning in __del__()
        pool._state = multiprocessing.pool.RUN

        with support.check_warnings(("unclosed running multiprocessing pool", ResourceWarning)):
            pool = None
            support.gc_collect()


def raising():
    raise KeyError("key")


def unpickleable_result():
    return lambda: 42


class WithProcessesTestPoolWorkerErrors(BaseTestCase, ProcessesMixin, unittest.TestCase):

    # DRAGON ALLOWED_TYPES = ('processes', )

    def test_async_error_callback(self):
        p = multiprocessing.Pool(2)

        scratchpad = [None]

        def errback(exc):
            scratchpad[0] = exc

        res = p.apply_async(raising, error_callback=errback)
        self.assertRaises(KeyError, res.get)
        self.assertTrue(scratchpad[0])
        self.assertIsInstance(scratchpad[0], KeyError)

        p.close()
        p.join()

    @unittest.skip(f"DRAGON: Bug filed PE-41882")
    def test_unpickleable_result(self):
        from multiprocessing.pool import MaybeEncodingError

        p = multiprocessing.Pool(2)

        # Make sure we don't lose pool processes because of encoding errors.
        for iteration in range(20):

            scratchpad = [None]

            def errback(exc):
                scratchpad[0] = exc

            res = p.apply_async(unpickleable_result, error_callback=errback)
            self.assertRaises(MaybeEncodingError, res.get)
            wrapped = scratchpad[0]
            self.assertTrue(wrapped)
            self.assertIsInstance(scratchpad[0], MaybeEncodingError)
            self.assertIsNotNone(wrapped.exc)
            self.assertIsNotNone(wrapped.value)

        p.close()
        p.join()


class WithProcessesTestPoolWorkerLifetime(BaseTestCase, ProcessesMixin, unittest.TestCase):

    # DRAGON ALLOWED_TYPES = ('processes', )

    def test_pool_worker_lifetime(self):
        p = multiprocessing.Pool(3, maxtasksperchild=10)
        self.assertEqual(3, len(p._pool))
        origworkerpids = [w.pid for w in p._pool]
        # Run many tasks so each worker gets replaced (hopefully)
        results = []
        for i in range(100):
            results.append(p.apply_async(sqr, (i,)))
        # Fetch the results and verify we got the right answers,
        # also ensuring all the tasks have completed.
        for (j, res) in enumerate(results):
            self.assertEqual(res.get(), sqr(j))
        # Refill the pool
        p._repopulate_pool()
        # Wait until all workers are alive
        # (countdown * DELTA = 5 seconds max startup process time)
        countdown = 50
        while countdown and not all(w.is_alive() for w in p._pool):
            countdown -= 1
            time.sleep(DELTA)
        finalworkerpids = [w.pid for w in p._pool]
        # All pids should be assigned.  See issue #7805.
        self.assertNotIn(None, origworkerpids)
        self.assertNotIn(None, finalworkerpids)
        # Finally, check that the worker pids have changed
        self.assertNotEqual(sorted(origworkerpids), sorted(finalworkerpids))
        p.close()
        p.join()

    def test_pool_worker_lifetime_early_close(self):
        # Issue #10332: closing a pool whose workers have limited lifetimes
        # before all the tasks completed would make join() hang.
        p = multiprocessing.Pool(3, maxtasksperchild=1)
        results = []
        for i in range(6):
            results.append(p.apply_async(sqr, (i, 0.3)))
        p.close()
        p.join()
        # check the results
        for (j, res) in enumerate(results):
            self.assertEqual(res.get(), sqr(j))

    def test_worker_finalization_via_atexit_handler_of_multiprocessing(self):
        # tests cases against bpo-38744 and bpo-39360
        cmd = """if 1:
            from multiprocessing import Pool
            problem = None
            class A:
                def __init__(self):
                    self.pool = Pool(processes=1)
            def test():
                global problem
                problem = A()
                problem.pool.map(float, tuple(range(10)))
            if __name__ == "__main__":
                test()
        """
        rc, out, err = test.support.script_helper.assert_python_ok("-c", cmd)
        self.assertEqual(rc, 0)


@unittest.skip("DRAGON: Manager not implemented")
@hashlib_helper.requires_hashdigest("md5")
class WithManagerTestPool(BaseTestCase, ManagerMixin, unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.pool = cls.Pool(4)

    @classmethod
    def tearDownClass(cls):
        cls.pool.terminate()
        cls.pool.join()
        cls.pool = None
        super().tearDownClass()

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
        call_args = self.manager.list() if self.TYPE == "manager" else []
        self.pool.map_async(int, ["1"], callback=call_args.append, error_callback=call_args.append).wait()
        self.assertEqual(1, len(call_args))
        self.assertEqual([1], call_args[0])
        self.pool.map_async(int, ["a"], callback=call_args.append, error_callback=call_args.append).wait()
        self.assertEqual(2, len(call_args))
        self.assertIsInstance(call_args[1], ValueError)

    def test_map_unplicklable(self):
        # Issue #19425 -- failure to pickle should not cause a hang
        if self.TYPE == "threads":
            self.skipTest("test not appropriate for {}".format(self.TYPE))

        class A(object):
            def __reduce__(self):
                raise RuntimeError("cannot pickle")

        with self.assertRaises(RuntimeError):
            self.pool.map(sqr, [A()] * 10)

    def test_map_chunksize(self):
        try:
            self.pool.map_async(sqr, [], chunksize=1).get(timeout=TIMEOUT1)
        except multiprocessing.TimeoutError:
            self.fail("pool.map_async with chunksize stalled on null list")

    def test_map_handle_iterable_exception(self):
        if self.TYPE == "manager":
            self.skipTest("test not appropriate for {}".format(self.TYPE))

        # SayWhenError seen at the very first of the iterable
        with self.assertRaises(SayWhenError):
            self.pool.map(sqr, exception_throwing_generator(1, -1), 1)
        # again, make sure it's reentrant
        with self.assertRaises(SayWhenError):
            self.pool.map(sqr, exception_throwing_generator(1, -1), 1)

        with self.assertRaises(SayWhenError):
            self.pool.map(sqr, exception_throwing_generator(10, 3), 1)

        class SpecialIterable:
            def __iter__(self):
                return self

            def __next__(self):
                raise SayWhenError

            def __len__(self):
                return 1

        with self.assertRaises(SayWhenError):
            self.pool.map(sqr, SpecialIterable(), 1)
        with self.assertRaises(SayWhenError):
            self.pool.map(sqr, SpecialIterable(), 1)

    def test_async(self):
        res = self.pool.apply_async(
            sqr,
            (
                7,
                TIMEOUT1,
            ),
        )
        get = TimingWrapper(res.get)
        self.assertEqual(get(), 49)
        self.assertTimingAlmostEqual(get.elapsed, TIMEOUT1)

    def test_async_timeout(self):
        res = self.pool.apply_async(sqr, (6, TIMEOUT2 + 1.0))
        get = TimingWrapper(res.get)
        self.assertRaises(multiprocessing.TimeoutError, get, timeout=TIMEOUT2)
        self.assertTimingAlmostEqual(get.elapsed, TIMEOUT2)

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

    def test_imap_handle_iterable_exception(self):
        if self.TYPE == "manager":
            self.skipTest("test not appropriate for {}".format(self.TYPE))

        # SayWhenError seen at the very first of the iterable
        it = self.pool.imap(sqr, exception_throwing_generator(1, -1), 1)
        self.assertRaises(SayWhenError, it.__next__)
        # again, make sure it's reentrant
        it = self.pool.imap(sqr, exception_throwing_generator(1, -1), 1)
        self.assertRaises(SayWhenError, it.__next__)

        it = self.pool.imap(sqr, exception_throwing_generator(10, 3), 1)
        for i in range(3):
            self.assertEqual(next(it), i * i)
        self.assertRaises(SayWhenError, it.__next__)

        # SayWhenError seen at start of problematic chunk's results
        it = self.pool.imap(sqr, exception_throwing_generator(20, 7), 2)
        for i in range(6):
            self.assertEqual(next(it), i * i)
        self.assertRaises(SayWhenError, it.__next__)
        it = self.pool.imap(sqr, exception_throwing_generator(20, 7), 4)
        for i in range(4):
            self.assertEqual(next(it), i * i)
        self.assertRaises(SayWhenError, it.__next__)

    def test_imap_unordered(self):
        it = self.pool.imap_unordered(sqr, list(range(10)))
        self.assertEqual(sorted(it), list(map(sqr, list(range(10)))))

        it = self.pool.imap_unordered(sqr, list(range(1000)), chunksize=100)
        self.assertEqual(sorted(it), list(map(sqr, list(range(1000)))))

    def test_imap_unordered_handle_iterable_exception(self):
        if self.TYPE == "manager":
            self.skipTest("test not appropriate for {}".format(self.TYPE))

        # SayWhenError seen at the very first of the iterable
        it = self.pool.imap_unordered(sqr, exception_throwing_generator(1, -1), 1)
        self.assertRaises(SayWhenError, it.__next__)
        # again, make sure it's reentrant
        it = self.pool.imap_unordered(sqr, exception_throwing_generator(1, -1), 1)
        self.assertRaises(SayWhenError, it.__next__)

        it = self.pool.imap_unordered(sqr, exception_throwing_generator(10, 3), 1)
        expected_values = list(map(sqr, list(range(10))))
        with self.assertRaises(SayWhenError):
            # imap_unordered makes it difficult to anticipate the SayWhenError
            for i in range(10):
                value = next(it)
                self.assertIn(value, expected_values)
                expected_values.remove(value)

        it = self.pool.imap_unordered(sqr, exception_throwing_generator(20, 7), 2)
        expected_values = list(map(sqr, list(range(20))))
        with self.assertRaises(SayWhenError):
            for i in range(20):
                value = next(it)
                self.assertIn(value, expected_values)
                expected_values.remove(value)

    def test_make_pool(self):
        expected_error = RemoteError if self.TYPE == "manager" else ValueError

        self.assertRaises(expected_error, self.Pool, -1)
        self.assertRaises(expected_error, self.Pool, 0)

        if self.TYPE != "manager":
            p = self.Pool(3)
            try:
                self.assertEqual(3, len(p._pool))
            finally:
                p.close()
                p.join()

    def test_terminate(self):
        result = self.pool.map_async(time.sleep, [0.1 for i in range(10000)], chunksize=1)
        self.pool.terminate()
        join = TimingWrapper(self.pool.join)
        join()
        # Sanity check the pool didn't wait for all tasks to finish
        self.assertLess(join.elapsed, 2.0)

    def test_empty_iterable(self):
        # See Issue 12157
        p = self.Pool(1)

        self.assertEqual(p.map(sqr, []), [])
        self.assertEqual(list(p.imap(sqr, [])), [])
        self.assertEqual(list(p.imap_unordered(sqr, [])), [])
        self.assertEqual(p.map_async(sqr, []).get(), [])

        p.close()
        p.join()

    def test_context(self):
        if self.TYPE == "processes":
            L = list(range(10))
            expected = [sqr(i) for i in L]
            with self.Pool(2) as p:
                r = p.map_async(sqr, L)
                self.assertEqual(r.get(), expected)
            p.join()
            self.assertRaises(ValueError, p.map_async, sqr, L)

    @classmethod
    def _test_traceback(cls):
        raise RuntimeError(123)  # some comment

    def test_traceback(self):
        # We want ensure that the traceback from the child process is
        # contained in the traceback raised in the main process.
        if self.TYPE == "processes":
            with self.Pool(1) as p:
                try:
                    p.apply(self._test_traceback)
                except Exception as e:
                    exc = e
                else:
                    self.fail("expected RuntimeError")
            p.join()
            self.assertIs(type(exc), RuntimeError)
            self.assertEqual(exc.args, (123,))
            cause = exc.__cause__
            self.assertIs(type(cause), multiprocessing.pool.RemoteTraceback)
            self.assertIn("raise RuntimeError(123) # some comment", cause.tb)

            with test.support.captured_stderr() as f1:
                try:
                    raise exc
                except RuntimeError:
                    sys.excepthook(*sys.exc_info())
            self.assertIn("raise RuntimeError(123) # some comment", f1.getvalue())
            # _helper_reraises_exception should not make the error
            # a remote exception
            with self.Pool(1) as p:
                try:
                    p.map(sqr, exception_throwing_generator(1, -1), 1)
                except Exception as e:
                    exc = e
                else:
                    self.fail("expected SayWhenError")
                self.assertIs(type(exc), SayWhenError)
                self.assertIs(exc.__cause__, None)
            p.join()

    @classmethod
    def _test_wrapped_exception(cls):
        raise RuntimeError("foo")

    def test_wrapped_exception(self):
        # Issue #20980: Should not wrap exception when using thread pool
        with self.Pool(1) as p:
            with self.assertRaises(RuntimeError):
                p.apply(self._test_wrapped_exception)
        p.join()

    def test_map_no_failfast(self):
        # Issue #23992: the fail-fast behaviour when an exception is raised
        # during map() would make Pool.join() deadlock, because a worker
        # process would fill the result queue (after the result handler thread
        # terminated, hence not draining it anymore).

        t_start = time.monotonic()

        with self.assertRaises(ValueError):
            with self.Pool(2) as p:
                try:
                    p.map(raise_large_valuerror, [0, 1])
                finally:
                    time.sleep(0.5)
                    p.close()
                    p.join()

        # check that we indeed waited for all jobs
        self.assertGreater(time.monotonic() - t_start, 0.9)

    def test_release_task_refs(self):
        # Issue #29861: task arguments and results should not be kept
        # alive after we are done with them.
        objs = [CountedObject() for i in range(10)]
        refs = [weakref.ref(o) for o in objs]
        self.pool.map(identity, objs)

        del objs
        gc.collect()  # For PyPy or other GCs.
        time.sleep(DELTA)  # let threaded cleanup code run
        self.assertEqual(set(wr() for wr in refs), {None})
        # With a process pool, copies of the objects are returned, check
        # they were released too.
        self.assertEqual(CountedObject.n_instances, 0)

    def test_enter(self):
        if self.TYPE == "manager":
            self.skipTest("test not applicable to manager")

        pool = self.Pool(1)
        with pool:
            pass
            # call pool.terminate()
        # pool is no longer running

        with self.assertRaises(ValueError):
            # bpo-35477: pool.__enter__() fails if the pool is not running
            with pool:
                pass
        pool.join()

    def test_resource_warning(self):
        if self.TYPE == "manager":
            self.skipTest("test not applicable to manager")

        pool = self.Pool(1)
        pool.terminate()
        pool.join()

        # force state to RUN to emit ResourceWarning in __del__()
        pool._state = multiprocessing.pool.RUN

        with support.check_warnings(("unclosed running multiprocessing pool", ResourceWarning)):
            pool = None
            support.gc_collect()


@unittest.skip("DRAGON: Threads not implemented")
class WithThreadsTestPool(BaseTestCase, ThreadsMixin, unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.pool = cls.Pool(4)

    @classmethod
    def tearDownClass(cls):
        cls.pool.terminate()
        cls.pool.join()
        cls.pool = None
        super().tearDownClass()

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
        call_args = self.manager.list() if self.TYPE == "manager" else []
        self.pool.map_async(int, ["1"], callback=call_args.append, error_callback=call_args.append).wait()
        self.assertEqual(1, len(call_args))
        self.assertEqual([1], call_args[0])
        self.pool.map_async(int, ["a"], callback=call_args.append, error_callback=call_args.append).wait()
        self.assertEqual(2, len(call_args))
        self.assertIsInstance(call_args[1], ValueError)

    def test_map_unplicklable(self):
        # Issue #19425 -- failure to pickle should not cause a hang
        if self.TYPE == "threads":
            self.skipTest("test not appropriate for {}".format(self.TYPE))

        class A(object):
            def __reduce__(self):
                raise RuntimeError("cannot pickle")

        with self.assertRaises(RuntimeError):
            self.pool.map(sqr, [A()] * 10)

    def test_map_chunksize(self):
        try:
            self.pool.map_async(sqr, [], chunksize=1).get(timeout=TIMEOUT1)
        except multiprocessing.TimeoutError:
            self.fail("pool.map_async with chunksize stalled on null list")

    def test_map_handle_iterable_exception(self):
        if self.TYPE == "manager":
            self.skipTest("test not appropriate for {}".format(self.TYPE))

        # SayWhenError seen at the very first of the iterable
        with self.assertRaises(SayWhenError):
            self.pool.map(sqr, exception_throwing_generator(1, -1), 1)
        # again, make sure it's reentrant
        with self.assertRaises(SayWhenError):
            self.pool.map(sqr, exception_throwing_generator(1, -1), 1)

        with self.assertRaises(SayWhenError):
            self.pool.map(sqr, exception_throwing_generator(10, 3), 1)

        class SpecialIterable:
            def __iter__(self):
                return self

            def __next__(self):
                raise SayWhenError

            def __len__(self):
                return 1

        with self.assertRaises(SayWhenError):
            self.pool.map(sqr, SpecialIterable(), 1)
        with self.assertRaises(SayWhenError):
            self.pool.map(sqr, SpecialIterable(), 1)

    def test_async(self):
        res = self.pool.apply_async(
            sqr,
            (
                7,
                TIMEOUT1,
            ),
        )
        get = TimingWrapper(res.get)
        self.assertEqual(get(), 49)
        self.assertTimingAlmostEqual(get.elapsed, TIMEOUT1)

    def test_async_timeout(self):
        res = self.pool.apply_async(sqr, (6, TIMEOUT2 + 1.0))
        get = TimingWrapper(res.get)
        self.assertRaises(multiprocessing.TimeoutError, get, timeout=TIMEOUT2)
        self.assertTimingAlmostEqual(get.elapsed, TIMEOUT2)

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

    def test_imap_handle_iterable_exception(self):
        if self.TYPE == "manager":
            self.skipTest("test not appropriate for {}".format(self.TYPE))

        # SayWhenError seen at the very first of the iterable
        it = self.pool.imap(sqr, exception_throwing_generator(1, -1), 1)
        self.assertRaises(SayWhenError, it.__next__)
        # again, make sure it's reentrant
        it = self.pool.imap(sqr, exception_throwing_generator(1, -1), 1)
        self.assertRaises(SayWhenError, it.__next__)

        it = self.pool.imap(sqr, exception_throwing_generator(10, 3), 1)
        for i in range(3):
            self.assertEqual(next(it), i * i)
        self.assertRaises(SayWhenError, it.__next__)

        # SayWhenError seen at start of problematic chunk's results
        it = self.pool.imap(sqr, exception_throwing_generator(20, 7), 2)
        for i in range(6):
            self.assertEqual(next(it), i * i)
        self.assertRaises(SayWhenError, it.__next__)
        it = self.pool.imap(sqr, exception_throwing_generator(20, 7), 4)
        for i in range(4):
            self.assertEqual(next(it), i * i)
        self.assertRaises(SayWhenError, it.__next__)

    def test_imap_unordered(self):
        it = self.pool.imap_unordered(sqr, list(range(10)))
        self.assertEqual(sorted(it), list(map(sqr, list(range(10)))))

        it = self.pool.imap_unordered(sqr, list(range(1000)), chunksize=100)
        self.assertEqual(sorted(it), list(map(sqr, list(range(1000)))))

    def test_imap_unordered_handle_iterable_exception(self):
        if self.TYPE == "manager":
            self.skipTest("test not appropriate for {}".format(self.TYPE))

        # SayWhenError seen at the very first of the iterable
        it = self.pool.imap_unordered(sqr, exception_throwing_generator(1, -1), 1)
        self.assertRaises(SayWhenError, it.__next__)
        # again, make sure it's reentrant
        it = self.pool.imap_unordered(sqr, exception_throwing_generator(1, -1), 1)
        self.assertRaises(SayWhenError, it.__next__)

        it = self.pool.imap_unordered(sqr, exception_throwing_generator(10, 3), 1)
        expected_values = list(map(sqr, list(range(10))))
        with self.assertRaises(SayWhenError):
            # imap_unordered makes it difficult to anticipate the SayWhenError
            for i in range(10):
                value = next(it)
                self.assertIn(value, expected_values)
                expected_values.remove(value)

        it = self.pool.imap_unordered(sqr, exception_throwing_generator(20, 7), 2)
        expected_values = list(map(sqr, list(range(20))))
        with self.assertRaises(SayWhenError):
            for i in range(20):
                value = next(it)
                self.assertIn(value, expected_values)
                expected_values.remove(value)

    def test_make_pool(self):
        expected_error = RemoteError if self.TYPE == "manager" else ValueError

        self.assertRaises(expected_error, self.Pool, -1)
        self.assertRaises(expected_error, self.Pool, 0)

        if self.TYPE != "manager":
            p = self.Pool(3)
            try:
                self.assertEqual(3, len(p._pool))
            finally:
                p.close()
                p.join()

    def test_terminate(self):
        result = self.pool.map_async(time.sleep, [0.1 for i in range(10000)], chunksize=1)
        self.pool.terminate()
        join = TimingWrapper(self.pool.join)
        join()
        # Sanity check the pool didn't wait for all tasks to finish
        self.assertLess(join.elapsed, 2.0)

    def test_empty_iterable(self):
        # See Issue 12157
        p = self.Pool(1)

        self.assertEqual(p.map(sqr, []), [])
        self.assertEqual(list(p.imap(sqr, [])), [])
        self.assertEqual(list(p.imap_unordered(sqr, [])), [])
        self.assertEqual(p.map_async(sqr, []).get(), [])

        p.close()
        p.join()

    def test_context(self):
        if self.TYPE == "processes":
            L = list(range(10))
            expected = [sqr(i) for i in L]
            with self.Pool(2) as p:
                r = p.map_async(sqr, L)
                self.assertEqual(r.get(), expected)
            p.join()
            self.assertRaises(ValueError, p.map_async, sqr, L)

    @classmethod
    def _test_traceback(cls):
        raise RuntimeError(123)  # some comment

    def test_traceback(self):
        # We want ensure that the traceback from the child process is
        # contained in the traceback raised in the main process.
        if self.TYPE == "processes":
            with self.Pool(1) as p:
                try:
                    p.apply(self._test_traceback)
                except Exception as e:
                    exc = e
                else:
                    self.fail("expected RuntimeError")
            p.join()
            self.assertIs(type(exc), RuntimeError)
            self.assertEqual(exc.args, (123,))
            cause = exc.__cause__
            self.assertIs(type(cause), multiprocessing.pool.RemoteTraceback)
            self.assertIn("raise RuntimeError(123) # some comment", cause.tb)

            with test.support.captured_stderr() as f1:
                try:
                    raise exc
                except RuntimeError:
                    sys.excepthook(*sys.exc_info())
            self.assertIn("raise RuntimeError(123) # some comment", f1.getvalue())
            # _helper_reraises_exception should not make the error
            # a remote exception
            with self.Pool(1) as p:
                try:
                    p.map(sqr, exception_throwing_generator(1, -1), 1)
                except Exception as e:
                    exc = e
                else:
                    self.fail("expected SayWhenError")
                self.assertIs(type(exc), SayWhenError)
                self.assertIs(exc.__cause__, None)
            p.join()

    @classmethod
    def _test_wrapped_exception(cls):
        raise RuntimeError("foo")

    def test_wrapped_exception(self):
        # Issue #20980: Should not wrap exception when using thread pool
        with self.Pool(1) as p:
            with self.assertRaises(RuntimeError):
                p.apply(self._test_wrapped_exception)
        p.join()

    def test_map_no_failfast(self):
        # Issue #23992: the fail-fast behaviour when an exception is raised
        # during map() would make Pool.join() deadlock, because a worker
        # process would fill the result queue (after the result handler thread
        # terminated, hence not draining it anymore).

        t_start = time.monotonic()

        with self.assertRaises(ValueError):
            with self.Pool(2) as p:
                try:
                    p.map(raise_large_valuerror, [0, 1])
                finally:
                    time.sleep(0.5)
                    p.close()
                    p.join()

        # check that we indeed waited for all jobs
        self.assertGreater(time.monotonic() - t_start, 0.9)

    def test_release_task_refs(self):
        # Issue #29861: task arguments and results should not be kept
        # alive after we are done with them.
        objs = [CountedObject() for i in range(10)]
        refs = [weakref.ref(o) for o in objs]
        self.pool.map(identity, objs)

        del objs
        gc.collect()  # For PyPy or other GCs.
        time.sleep(DELTA)  # let threaded cleanup code run
        self.assertEqual(set(wr() for wr in refs), {None})
        # With a process pool, copies of the objects are returned, check
        # they were released too.
        self.assertEqual(CountedObject.n_instances, 0)

    def test_enter(self):
        if self.TYPE == "manager":
            self.skipTest("test not applicable to manager")

        pool = self.Pool(1)
        with pool:
            pass
            # call pool.terminate()
        # pool is no longer running

        with self.assertRaises(ValueError):
            # bpo-35477: pool.__enter__() fails if the pool is not running
            with pool:
                pass
        pool.join()

    def test_resource_warning(self):
        if self.TYPE == "manager":
            self.skipTest("test not applicable to manager")

        pool = self.Pool(1)
        pool.terminate()
        pool.join()

        # force state to RUN to emit ResourceWarning in __del__()
        pool._state = multiprocessing.pool.RUN

        with support.check_warnings(("unclosed running multiprocessing pool", ResourceWarning)):
            pool = None
            support.gc_collect()

# DRAGON
if __name__ == "__main__":
    setUpModule()
    unittest.main()
    tearDownModule()
