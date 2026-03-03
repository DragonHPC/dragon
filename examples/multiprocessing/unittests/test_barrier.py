""" Test mulitprocessing barrier
"""
import unittest
import time
import os
import struct
import weakref

from test.support import hashlib_helper

import threading

import dragon  # DRAGON import before multiprocessing

import multiprocessing

from common import (
    BaseTestCase,
    ProcessesMixin,
    ManagerMixin,
    ThreadsMixin,
    setUpModule,
    tearDownModule,
    close_queue,
)

#
# Tests for Barrier - adapted from tests in test/lock_tests.py
#

# Many of the tests for threading.Barrier use a list as an atomic
# counter: a value is appended to increment the counter, and the
# length of the list gives the value.  We use the class DummyList
# for the same purpose.


class _DummyList(object):
    def __init__(self):
        wrapper = multiprocessing.heap.BufferWrapper(struct.calcsize("i"))
        lock = multiprocessing.Lock()
        self.__setstate__((wrapper, lock))
        self._lengthbuf[0] = 0

    def __setstate__(self, state):
        (self._wrapper, self._lock) = state
        self._lengthbuf = self._wrapper.create_memoryview().cast("i")

    def __getstate__(self):
        return (self._wrapper, self._lock)

    def append(self, _):
        with self._lock:
            self._lengthbuf[0] += 1

    def __len__(self):
        with self._lock:
            return self._lengthbuf[0]


def _wait():
    # A crude wait/yield function not relying on synchronization primitives.
    time.sleep(0.01)


class Bunch(object):
    """
    A bunch of threads.
    """

    def __init__(self, namespace, f, args, n, wait_before_exit=False):
        """
        Construct a bunch of `n` threads running the same function `f`.
        If `wait_before_exit` is True, the threads won't terminate until
        do_finish() is called.
        """
        self.f = f
        self.args = args
        self.n = n
        self.started = namespace.DummyList()
        self.finished = namespace.DummyList()
        self._can_exit = namespace.Event()
        if not wait_before_exit:
            self._can_exit.set()

        threads = []
        for i in range(n):
            p = namespace.Process(target=self.task)
            p.daemon = True
            p.start()
            threads.append(p)

        def finalize(threads):
            for p in threads:
                p.join()

        self._finalizer = weakref.finalize(self, finalize, threads)

    def task(self):
        pid = os.getpid()
        self.started.append(pid)
        try:
            self.f(*self.args)
        finally:
            self.finished.append(pid)
            self._can_exit.wait(30)
            assert self._can_exit.is_set()
    def wait_for_started(self):
        while len(self.started) < self.n:
            _wait()

    def wait_for_finished(self):
        while len(self.finished) < self.n:
           _wait()

    def do_finish(self):
        self._can_exit.set()

    def close(self):
        self._finalizer()


class AppendTrue(object):
    def __init__(self, obj):
        self.obj = obj

    def __call__(self):
        self.obj.append(True)

class WithProcessesTestBarrier(BaseTestCase, ProcessesMixin, unittest.TestCase):
    """
    Tests for Barrier objects.
    """

    N = 5
    defaultTimeout = 30.0  # XXX Slow Windows buildbots need generous timeout

    def setUp(self):
        self.barrier = self.Barrier(self.N, timeout=self.defaultTimeout)

    def tearDown(self):
        self.barrier.abort()
        self.barrier = None

    def DummyList(self):
        if self.TYPE == "threads":
            return []
        elif self.TYPE == "manager":
            return self.manager.list()
        else:
            return _DummyList()

    def run_threads(self, f, args):
        b = Bunch(self, f, args, self.N - 1)
        try:
            f(*args)
            b.wait_for_finished()
        finally:
            b.close()

    @classmethod
    def multipass(cls, barrier, results, n):
        m = barrier.parties
        assert m == cls.N
        for i in range(n):
            results[0].append(True)
            assert len(results[1]) == i * m
            barrier.wait()
            results[1].append(True)
            assert len(results[0]) == (i + 1) * m
            barrier.wait()
        try:
            assert barrier.n_waiting == 0
        except NotImplementedError:
            pass
        assert not barrier.broken

    def test_barrier(self, passes=1):
        """
        Test that a barrier is passed in lockstep
        """
        results = [self.DummyList(), self.DummyList()]
        self.run_threads(self.multipass, (self.barrier, results, passes))

    def test_barrier_10(self):
        """
        Test that a barrier works for 10 consecutive runs
        """
        return self.test_barrier(10)

    @classmethod
    def _test_wait_return_f(cls, barrier, queue):
        res = barrier.wait()
        queue.put(res)

    def test_wait_return(self):
        """
        test the return value from barrier.wait
        """
        queue = self.Queue()
        self.run_threads(self._test_wait_return_f, (self.barrier, queue))
        results = [queue.get() for i in range(self.N)]
        self.assertEqual(results.count(0), 1)
        close_queue(queue)

    @classmethod
    def _test_action_f(cls, barrier, results):
        barrier.wait()
        if len(results) != 1:
            raise RuntimeError

    def test_action(self):
        """
        Test the 'action' callback
        """
        results = self.DummyList()
        barrier = self.Barrier(self.N, action=AppendTrue(results))
        self.run_threads(self._test_action_f, (barrier, results))
        self.assertEqual(len(results), 1)

    @classmethod
    def _test_abort_f(cls, barrier, results1, results2):
        try:
            i = barrier.wait()
            if i == cls.N // 2:
                raise RuntimeError
            barrier.wait()
            results1.append(True)
        except threading.BrokenBarrierError:
            results2.append(True)
        except RuntimeError:
            barrier.abort()

    def test_abort(self):
        """
        Test that an abort will put the barrier in a broken state
        """
        results1 = self.DummyList()
        results2 = self.DummyList()
        self.run_threads(self._test_abort_f, (self.barrier, results1, results2))
        self.assertEqual(len(results1), 0)
        self.assertEqual(len(results2), self.N - 1)
        self.assertTrue(self.barrier.broken)

    @classmethod
    def _test_reset_f(cls, barrier, results1, results2, results3):
        i = barrier.wait()
        if i == cls.N // 2:
            # Wait until the other threads are all in the barrier.
            while barrier.n_waiting < cls.N - 1:
                time.sleep(0.001)
            
            barrier.reset()
        else:
            try:
                barrier.wait()
                results1.append(True)
            except threading.BrokenBarrierError:
                results2.append(True)
        # Now, pass the barrier again
        barrier.wait()
        results3.append(True)

    def test_reset(self):
        """
        Test that a 'reset' on a barrier frees the waiting threads
        """
        results1 = self.DummyList()
        results2 = self.DummyList()
        results3 = self.DummyList()
        self.run_threads(self._test_reset_f, (self.barrier, results1, results2, results3))
        self.assertEqual(len(results1), 0)
        self.assertEqual(len(results2), self.N - 1)
        self.assertEqual(len(results3), self.N)

    @classmethod
    def _test_abort_and_reset_f(cls, barrier, barrier2, results1, results2, results3):
        try:
            i = barrier.wait()
            if i == cls.N // 2:
                raise RuntimeError
            barrier.wait()
            results1.append(True)
        except threading.BrokenBarrierError:
            results2.append(True)
        except RuntimeError:
            barrier.abort()
        # Synchronize and reset the barrier.  Must synchronize first so
        # that everyone has left it when we reset, and after so that no
        # one enters it before the reset.
        if barrier2.wait() == cls.N // 2:
            barrier.reset()
        barrier2.wait()
        barrier.wait()
        results3.append(True)

    def test_abort_and_reset(self):
        """
        Test that a barrier can be reset after being broken.
        """
        results1 = self.DummyList()
        results2 = self.DummyList()
        results3 = self.DummyList()
        barrier2 = self.Barrier(self.N)

        self.run_threads(self._test_abort_and_reset_f, (self.barrier, barrier2, results1, results2, results3))
        self.assertEqual(len(results1), 0)
        self.assertEqual(len(results2), self.N - 1)
        self.assertEqual(len(results3), self.N)

    @classmethod
    def _test_timeout_f(cls, barrier, results):
        i = barrier.wait()
        if i == cls.N // 2:
            # One thread is late!
            time.sleep(1.0)
        try:
            barrier.wait(0.5)
        except threading.BrokenBarrierError:
            results.append(True)

    def test_timeout(self):
        """
        Test wait(timeout)
        """
        results = self.DummyList()
        self.run_threads(self._test_timeout_f, (self.barrier, results))
        self.assertEqual(len(results), self.barrier.parties)

    @classmethod
    def _test_default_timeout_f(cls, barrier, results):
        i = barrier.wait(cls.defaultTimeout)
        if i == cls.N // 2:
            # One thread is later than the default timeout
            time.sleep(1.0)
        try:
            barrier.wait()
        except threading.BrokenBarrierError:
            results.append(True)

    def test_default_timeout(self):
        """
        Test the barrier's default timeout
        """
        barrier = self.Barrier(self.N, timeout=0.5)
        results = self.DummyList()
        self.run_threads(self._test_default_timeout_f, (barrier, results))
        self.assertEqual(len(results), barrier.parties)

    def test_single_thread(self):
        b = self.Barrier(1)
        b.wait()
        b.wait()

    @classmethod
    def _test_thousand_f(cls, barrier, passes, conn, lock):
        for i in range(passes):
            barrier.wait()
            with lock:
                conn.send(i)

    def test_thousand(self):
        if self.TYPE == "manager":
            self.skipTest("test not appropriate for {}".format(self.TYPE))
        passes = 1000
        lock = self.Lock()
        conn, child_conn = self.Pipe(False)
        for j in range(self.N):
            p = self.Process(target=self._test_thousand_f, args=(self.barrier, passes, child_conn, lock))
            p.start()
            self.addCleanup(p.join)

        for i in range(passes):
            for j in range(self.N):
                self.assertEqual(conn.recv(), i)


@unittest.skip("DRAGON: Manager not implemented")
@hashlib_helper.requires_hashdigest("md5")
class WithManagerTestBarrier(BaseTestCase, ManagerMixin, unittest.TestCase):
    """
    Tests for Barrier objects.
    """

    N = 5
    defaultTimeout = 30.0  # XXX Slow Windows buildbots need generous timeout

    def setUp(self):
        self.barrier = self.Barrier(self.N, timeout=self.defaultTimeout)

    def tearDown(self):
        self.barrier.abort()
        self.barrier = None

    def DummyList(self):
        if self.TYPE == "threads":
            return []
        elif self.TYPE == "manager":
            return self.manager.list()
        else:
            return _DummyList()

    def run_threads(self, f, args):
        b = Bunch(self, f, args, self.N - 1)
        try:
            f(*args)
            b.wait_for_finished()
        finally:
            b.close()

    @classmethod
    def multipass(cls, barrier, results, n):
        m = barrier.parties
        assert m == cls.N
        for i in range(n):
            results[0].append(True)
            assert len(results[1]) == i * m
            barrier.wait()
            results[1].append(True)
            assert len(results[0]) == (i + 1) * m
            barrier.wait()
        try:
            assert barrier.n_waiting == 0
        except NotImplementedError:
            pass
        assert not barrier.broken

    def test_barrier(self, passes=1):
        """
        Test that a barrier is passed in lockstep
        """
        results = [self.DummyList(), self.DummyList()]
        self.run_threads(self.multipass, (self.barrier, results, passes))

    def test_barrier_10(self):
        """
        Test that a barrier works for 10 consecutive runs
        """
        return self.test_barrier(10)

    @classmethod
    def _test_wait_return_f(cls, barrier, queue):
        res = barrier.wait()
        queue.put(res)

    def test_wait_return(self):
        """
        test the return value from barrier.wait
        """
        queue = self.Queue()
        self.run_threads(self._test_wait_return_f, (self.barrier, queue))
        results = [queue.get() for i in range(self.N)]
        self.assertEqual(results.count(0), 1)
        close_queue(queue)

    @classmethod
    def _test_action_f(cls, barrier, results):
        barrier.wait()
        if len(results) != 1:
            raise RuntimeError

    def test_action(self):
        """
        Test the 'action' callback
        """
        results = self.DummyList()
        barrier = self.Barrier(self.N, action=AppendTrue(results))
        self.run_threads(self._test_action_f, (barrier, results))
        self.assertEqual(len(results), 1)

    @classmethod
    def _test_abort_f(cls, barrier, results1, results2):
        try:
            i = barrier.wait()
            if i == cls.N // 2:
                raise RuntimeError
            barrier.wait()
            results1.append(True)
        except threading.BrokenBarrierError:
            results2.append(True)
        except RuntimeError:
            barrier.abort()

    def test_abort(self):
        """
        Test that an abort will put the barrier in a broken state
        """
        results1 = self.DummyList()
        results2 = self.DummyList()
        self.run_threads(self._test_abort_f, (self.barrier, results1, results2))
        self.assertEqual(len(results1), 0)
        self.assertEqual(len(results2), self.N - 1)
        self.assertTrue(self.barrier.broken)

    @classmethod
    def _test_reset_f(cls, barrier, results1, results2, results3):
        i = barrier.wait()
        if i == cls.N // 2:
            # Wait until the other threads are all in the barrier.
            while barrier.n_waiting < cls.N - 1:
                time.sleep(0.001)
            barrier.reset()
        else:
            try:
                barrier.wait()
                results1.append(True)
            except threading.BrokenBarrierError:
                results2.append(True)
        # Now, pass the barrier again
        barrier.wait()
        results3.append(True)

    def test_reset(self):
        """
        Test that a 'reset' on a barrier frees the waiting threads
        """
        results1 = self.DummyList()
        results2 = self.DummyList()
        results3 = self.DummyList()
        self.run_threads(self._test_reset_f, (self.barrier, results1, results2, results3))
        self.assertEqual(len(results1), 0)
        self.assertEqual(len(results2), self.N - 1)
        self.assertEqual(len(results3), self.N)

    @classmethod
    def _test_abort_and_reset_f(cls, barrier, barrier2, results1, results2, results3):
        try:
            i = barrier.wait()
            if i == cls.N // 2:
                raise RuntimeError
            barrier.wait()
            results1.append(True)
        except threading.BrokenBarrierError:
            results2.append(True)
        except RuntimeError:
            barrier.abort()
        # Synchronize and reset the barrier.  Must synchronize first so
        # that everyone has left it when we reset, and after so that no
        # one enters it before the reset.
        if barrier2.wait() == cls.N // 2:
            barrier.reset()
        barrier2.wait()
        barrier.wait()
        results3.append(True)

    def test_abort_and_reset(self):
        """
        Test that a barrier can be reset after being broken.
        """
        results1 = self.DummyList()
        results2 = self.DummyList()
        results3 = self.DummyList()
        barrier2 = self.Barrier(self.N)

        self.run_threads(self._test_abort_and_reset_f, (self.barrier, barrier2, results1, results2, results3))
        self.assertEqual(len(results1), 0)
        self.assertEqual(len(results2), self.N - 1)
        self.assertEqual(len(results3), self.N)

    @classmethod
    def _test_timeout_f(cls, barrier, results):
        i = barrier.wait()
        if i == cls.N // 2:
            # One thread is late!
            time.sleep(1.0)
        try:
            barrier.wait(0.5)
        except threading.BrokenBarrierError:
            results.append(True)

    def test_timeout(self):
        """
        Test wait(timeout)
        """
        results = self.DummyList()
        self.run_threads(self._test_timeout_f, (self.barrier, results))
        self.assertEqual(len(results), self.barrier.parties)

    @classmethod
    def _test_default_timeout_f(cls, barrier, results):
        i = barrier.wait(cls.defaultTimeout)
        if i == cls.N // 2:
            # One thread is later than the default timeout
            time.sleep(1.0)
        try:
            barrier.wait()
        except threading.BrokenBarrierError:
            results.append(True)

    def test_default_timeout(self):
        """
        Test the barrier's default timeout
        """
        barrier = self.Barrier(self.N, timeout=0.5)
        results = self.DummyList()
        self.run_threads(self._test_default_timeout_f, (barrier, results))
        self.assertEqual(len(results), barrier.parties)

    def test_single_thread(self):
        b = self.Barrier(1)
        b.wait()
        b.wait()

    @classmethod
    def _test_thousand_f(cls, barrier, passes, conn, lock):
        for i in range(passes):
            barrier.wait()
            with lock:
                conn.send(i)

    def test_thousand(self):
        if self.TYPE == "manager":
            self.skipTest("test not appropriate for {}".format(self.TYPE))
        passes = 1000
        lock = self.Lock()
        conn, child_conn = self.Pipe(False)
        for j in range(self.N):
            p = self.Process(target=self._test_thousand_f, args=(self.barrier, passes, child_conn, lock))
            p.start()
            self.addCleanup(p.join)

        for i in range(passes):
            for j in range(self.N):
                self.assertEqual(conn.recv(), i)


@unittest.skip("DRAGON: Threads not implemented")
class WithThreadsTestBarrier(BaseTestCase, ThreadsMixin, unittest.TestCase):
    """
    Tests for Barrier objects.
    """

    N = 5
    defaultTimeout = 30.0  # XXX Slow Windows buildbots need generous timeout

    def setUp(self):
        self.barrier = self.Barrier(self.N, timeout=self.defaultTimeout)

    def tearDown(self):
        self.barrier.abort()
        self.barrier = None

    def DummyList(self):
        if self.TYPE == "threads":
            return []
        elif self.TYPE == "manager":
            return self.manager.list()
        else:
            return _DummyList()

    def run_threads(self, f, args):
        b = Bunch(self, f, args, self.N - 1)
        try:
            f(*args)
            b.wait_for_finished()
        finally:
            b.close()

    @classmethod
    def multipass(cls, barrier, results, n):
        m = barrier.parties
        assert m == cls.N
        for i in range(n):
            results[0].append(True)
            assert len(results[1]) == i * m
            barrier.wait()
            results[1].append(True)
            assert len(results[0]) == (i + 1) * m
            barrier.wait()
        try:
            assert barrier.n_waiting == 0
        except NotImplementedError:
            pass
        assert not barrier.broken

    def test_barrier(self, passes=1):
        """
        Test that a barrier is passed in lockstep
        """
        results = [self.DummyList(), self.DummyList()]
        self.run_threads(self.multipass, (self.barrier, results, passes))

    def test_barrier_10(self):
        """
        Test that a barrier works for 10 consecutive runs
        """
        return self.test_barrier(10)

    @classmethod
    def _test_wait_return_f(cls, barrier, queue):
        res = barrier.wait()
        queue.put(res)

    def test_wait_return(self):
        """
        test the return value from barrier.wait
        """
        queue = self.Queue()
        self.run_threads(self._test_wait_return_f, (self.barrier, queue))
        results = [queue.get() for i in range(self.N)]
        self.assertEqual(results.count(0), 1)
        close_queue(queue)

    @classmethod
    def _test_action_f(cls, barrier, results):
        barrier.wait()
        if len(results) != 1:
            raise RuntimeError

    def test_action(self):
        """
        Test the 'action' callback
        """
        results = self.DummyList()
        barrier = self.Barrier(self.N, action=AppendTrue(results))
        self.run_threads(self._test_action_f, (barrier, results))
        self.assertEqual(len(results), 1)

    @classmethod
    def _test_abort_f(cls, barrier, results1, results2):
        try:
            i = barrier.wait()
            if i == cls.N // 2:
                raise RuntimeError
            barrier.wait()
            results1.append(True)
        except threading.BrokenBarrierError:
            results2.append(True)
        except RuntimeError:
            barrier.abort()

    def test_abort(self):
        """
        Test that an abort will put the barrier in a broken state
        """
        results1 = self.DummyList()
        results2 = self.DummyList()
        self.run_threads(self._test_abort_f, (self.barrier, results1, results2))
        self.assertEqual(len(results1), 0)
        self.assertEqual(len(results2), self.N - 1)
        self.assertTrue(self.barrier.broken)

    @classmethod
    def _test_reset_f(cls, barrier, results1, results2, results3):
        i = barrier.wait()
        if i == cls.N // 2:
            # Wait until the other threads are all in the barrier.
            while barrier.n_waiting < cls.N - 1:
                time.sleep(0.001)
            barrier.reset()
        else:
            try:
                barrier.wait()
                results1.append(True)
            except threading.BrokenBarrierError:
                results2.append(True)
        # Now, pass the barrier again
        barrier.wait()
        results3.append(True)

    def test_reset(self):
        """
        Test that a 'reset' on a barrier frees the waiting threads
        """
        results1 = self.DummyList()
        results2 = self.DummyList()
        results3 = self.DummyList()
        self.run_threads(self._test_reset_f, (self.barrier, results1, results2, results3))
        self.assertEqual(len(results1), 0)
        self.assertEqual(len(results2), self.N - 1)
        self.assertEqual(len(results3), self.N)

    @classmethod
    def _test_abort_and_reset_f(cls, barrier, barrier2, results1, results2, results3):
        try:
            i = barrier.wait()
            if i == cls.N // 2:
                raise RuntimeError
            barrier.wait()
            results1.append(True)
        except threading.BrokenBarrierError:
            results2.append(True)
        except RuntimeError:
            barrier.abort()
        # Synchronize and reset the barrier.  Must synchronize first so
        # that everyone has left it when we reset, and after so that no
        # one enters it before the reset.
        if barrier2.wait() == cls.N // 2:
            barrier.reset()
        barrier2.wait()
        barrier.wait()
        results3.append(True)

    def test_abort_and_reset(self):
        """
        Test that a barrier can be reset after being broken.
        """
        results1 = self.DummyList()
        results2 = self.DummyList()
        results3 = self.DummyList()
        barrier2 = self.Barrier(self.N)

        self.run_threads(self._test_abort_and_reset_f, (self.barrier, barrier2, results1, results2, results3))
        self.assertEqual(len(results1), 0)
        self.assertEqual(len(results2), self.N - 1)
        self.assertEqual(len(results3), self.N)

    @classmethod
    def _test_timeout_f(cls, barrier, results):
        i = barrier.wait()
        if i == cls.N // 2:
            # One thread is late!
            time.sleep(1.0)
        try:
            barrier.wait(0.5)
        except threading.BrokenBarrierError:
            results.append(True)

    def test_timeout(self):
        """
        Test wait(timeout)
        """
        results = self.DummyList()
        self.run_threads(self._test_timeout_f, (self.barrier, results))
        self.assertEqual(len(results), self.barrier.parties)

    @classmethod
    def _test_default_timeout_f(cls, barrier, results):
        i = barrier.wait(cls.defaultTimeout)
        if i == cls.N // 2:
            # One thread is later than the default timeout
            time.sleep(1.0)
        try:
            barrier.wait()
        except threading.BrokenBarrierError:
            results.append(True)

    def test_default_timeout(self):
        """
        Test the barrier's default timeout
        """
        barrier = self.Barrier(self.N, timeout=0.5)
        results = self.DummyList()
        self.run_threads(self._test_default_timeout_f, (barrier, results))
        self.assertEqual(len(results), barrier.parties)

    def test_single_thread(self):
        b = self.Barrier(1)
        b.wait()
        b.wait()

    @classmethod
    def _test_thousand_f(cls, barrier, passes, conn, lock):
        for i in range(passes):
            barrier.wait()
            with lock:
                conn.send(i)

    def test_thousand(self):
        if self.TYPE == "manager":
            self.skipTest("test not appropriate for {}".format(self.TYPE))
        passes = 1000
        lock = self.Lock()
        conn, child_conn = self.Pipe(False)
        for j in range(self.N):
            p = self.Process(target=self._test_thousand_f, args=(self.barrier, passes, child_conn, lock))
            p.start()
            self.addCleanup(p.join)

        for i in range(passes):
            for j in range(self.N):
                self.assertEqual(conn.recv(), i)


# DRAGON
if __name__ == "__main__":
    setUpModule()
    unittest.main()
    tearDownModule()
