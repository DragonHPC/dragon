""" Test mulitprocessing.Condition
"""
import unittest
import time
import sys
import os
import signal

from test import support

import threading

import dragon  # DRAGON import before multiprocessing

import multiprocessing

try:
    from multiprocessing.sharedctypes import Value, copy

    HAS_SHAREDCTYPES = True
except ImportError:
    HAS_SHAREDCTYPES = False

from common import (
    BaseTestCase,
    ProcessesMixin,
    ManagerMixin,
    ThreadsMixin,
    TimingWrapper,
    setUpModule,
    tearDownModule,
    get_value,
    join_process,
    DELTA,
    TIMEOUT1,
)


class WithProcessesTestCondition(BaseTestCase, ProcessesMixin, unittest.TestCase):
    @classmethod
    def f(cls, cond, sleeping, woken, timeout=None):
        cond.acquire()
        sleeping.release()
        cond.wait(timeout)
        woken.release()
        cond.release()

    def assertReachesEventually(self, func, value):
        for i in range(10):
            try:
                if func() == value:
                    break
            except NotImplementedError:
                break
            time.sleep(DELTA)
        time.sleep(DELTA)
        self.assertReturnsIfImplemented(value, func)

    def check_invariant(self, cond):
        # this is only supposed to succeed when there are no sleepers
        if self.TYPE == "processes":
            try:
                sleepers = cond._sleeping_count.get_value() - cond._woken_count.get_value()
                self.assertEqual(sleepers, 0)
                self.assertEqual(cond._wait_semaphore.get_value(), 0)
            except NotImplementedError:
                pass

    def test_notify(self):
        cond = self.Condition()
        sleeping = self.Semaphore(0)
        woken = self.Semaphore(0)

        p = self.Process(target=self.f, args=(cond, sleeping, woken))
        p.daemon = True
        p.start()
        self.addCleanup(p.join)

        p = threading.Thread(target=self.f, args=(cond, sleeping, woken))
        p.daemon = True
        p.start()
        self.addCleanup(p.join)

        # wait for both children to start sleeping
        sleeping.acquire()
        sleeping.acquire()

        # check no process/thread has woken up
        time.sleep(DELTA)
        self.assertReturnsIfImplemented(0, get_value, woken)

        # wake up one process/thread
        cond.acquire()
        cond.notify()
        cond.release()

        # check one process/thread has woken up
        time.sleep(DELTA)
        self.assertReachesEventually(lambda: get_value(woken), 1)
        # self.assertReturnsIfImplemented(1, get_value, woken) # DRAGON: test not deterministic

        # wake up another
        cond.acquire()
        cond.notify()
        cond.release()

        # check other has woken up
        time.sleep(DELTA)
        self.assertReachesEventually(lambda: get_value(woken), 2)
        # self.assertReturnsIfImplemented(2, get_value, woken) # DRAGON: test not deterministic

        # check state is not mucked up
        self.check_invariant(cond)
        p.join()

    def test_notify_all(self):
        cond = self.Condition()
        sleeping = self.Semaphore(0)
        woken = self.Semaphore(0)

        # start some threads/processes which will timeout
        for i in range(3):
            p = self.Process(target=self.f, args=(cond, sleeping, woken, TIMEOUT1))
            p.daemon = True
            p.start()
            self.addCleanup(p.join)

            t = threading.Thread(target=self.f, args=(cond, sleeping, woken, TIMEOUT1))
            t.daemon = True
            t.start()
            self.addCleanup(t.join)

        # wait for them all to sleep
        for i in range(6):
            sleeping.acquire()

        # check they have all timed out
        for i in range(6):
            woken.acquire()
        self.assertReturnsIfImplemented(0, get_value, woken)

        # check state is not mucked up
        self.check_invariant(cond)

        # start some more threads/processes
        for i in range(3):
            p = self.Process(target=self.f, args=(cond, sleeping, woken))
            p.daemon = True
            p.start()
            self.addCleanup(p.join)

            t = threading.Thread(target=self.f, args=(cond, sleeping, woken))
            t.daemon = True
            t.start()
            self.addCleanup(t.join)

        # wait for them to all sleep
        for i in range(6):
            sleeping.acquire()

        # check no process/thread has woken up
        time.sleep(DELTA)
        self.assertReturnsIfImplemented(0, get_value, woken)

        # wake them all up
        cond.acquire()
        cond.notify_all()
        cond.release()

        # check they have all woken
        self.assertReachesEventually(lambda: get_value(woken), 6)

        # check state is not mucked up
        self.check_invariant(cond)

    def test_notify_n(self):
        cond = self.Condition()
        sleeping = self.Semaphore(0)
        woken = self.Semaphore(0)

        # start some threads/processes
        for i in range(3):
            p = self.Process(target=self.f, args=(cond, sleeping, woken))
            p.daemon = True
            p.start()
            self.addCleanup(p.join)

            t = threading.Thread(target=self.f, args=(cond, sleeping, woken))
            t.daemon = True
            t.start()
            self.addCleanup(t.join)

        # wait for them to all sleep
        for i in range(6):
            sleeping.acquire()

        # check no process/thread has woken up
        time.sleep(DELTA)
        self.assertReturnsIfImplemented(0, get_value, woken)

        # wake some of them up
        cond.acquire()
        cond.notify(n=2)
        cond.release()

        # check 2 have woken
        self.assertReachesEventually(lambda: get_value(woken), 2)

        # wake the rest of them
        cond.acquire()
        cond.notify(n=4)
        cond.release()

        self.assertReachesEventually(lambda: get_value(woken), 6)

        # doesn't do anything more
        cond.acquire()
        cond.notify(n=3)
        cond.release()

        self.assertReturnsIfImplemented(6, get_value, woken)

        # check state is not mucked up
        self.check_invariant(cond)

    def test_timeout(self):
        cond = self.Condition()
        wait = TimingWrapper(cond.wait)
        cond.acquire()
        res = wait(TIMEOUT1)
        cond.release()
        self.assertEqual(res, False)
        self.assertTimingAlmostEqual(wait.elapsed, TIMEOUT1)

    @classmethod
    def _test_waitfor_f(cls, cond, state):
        with cond:
            state.value = 0
            cond.notify()
            result = cond.wait_for(lambda: state.value == 4)
            if not result or state.value != 4:
                sys.exit(1)

    @unittest.skip("DRAGON: sharedctypes & Value not implemented")
    @unittest.skipUnless(HAS_SHAREDCTYPES, "needs sharedctypes")
    def test_waitfor(self):
        # based on test in test/lock_tests.py
        cond = self.Condition()
        state = self.Value("i", -1)

        p = self.Process(target=self._test_waitfor_f, args=(cond, state))
        p.daemon = True
        p.start()

        with cond:
            result = cond.wait_for(lambda: state.value == 0)
            self.assertTrue(result)
            self.assertEqual(state.value, 0)

        for i in range(4):
            time.sleep(0.01)
            with cond:
                state.value += 1
                cond.notify()

        join_process(p)
        self.assertEqual(p.exitcode, 0)

    @classmethod
    def _test_waitfor_timeout_f(cls, cond, state, success, sem):
        sem.release()
        with cond:
            expected = 0.1
            dt = time.monotonic()
            result = cond.wait_for(lambda: state.value == 4, timeout=expected)
            dt = time.monotonic() - dt
            # borrow logic in assertTimeout() from test/lock_tests.py
            if not result and expected * 0.6 < dt < expected * 10.0:
                success.value = True

    @unittest.skip("DRAGON: sharedctypes & Value not implemented")
    @unittest.skipUnless(HAS_SHAREDCTYPES, "needs sharedctypes")
    def test_waitfor_timeout(self):
        # based on test in test/lock_tests.py
        cond = self.Condition()
        state = self.Value("i", 0)
        success = self.Value("i", False)
        sem = self.Semaphore(0)

        p = self.Process(target=self._test_waitfor_timeout_f, args=(cond, state, success, sem))
        p.daemon = True
        p.start()
        self.assertTrue(sem.acquire(timeout=support.LONG_TIMEOUT))

        # Only increment 3 times, so state == 4 is never reached.
        for i in range(3):
            time.sleep(0.01)
            with cond:
                state.value += 1
                cond.notify()

        join_process(p)
        self.assertTrue(success.value)

    @classmethod
    def _test_wait_result(cls, c, pid):
        with c:
            c.notify()
        time.sleep(1)
        if pid is not None:
            os.kill(pid, signal.SIGINT)

    def test_wait_result(self):
        if isinstance(self, ProcessesMixin) and sys.platform != "win32":
            pid = os.getpid()
        else:
            pid = None

        c = self.Condition()
        with c:
            self.assertFalse(c.wait(0))
            self.assertFalse(c.wait(0.1))

            p = self.Process(target=self._test_wait_result, args=(c, pid))
            p.start()

            self.assertTrue(c.wait(60))
            if pid is not None:
                self.assertRaises(KeyboardInterrupt, c.wait, 60)

            p.join()


@unittest.skip("DRAGON: Manager not implemented")
class WithManagerTestCondition(BaseTestCase, ManagerMixin, unittest.TestCase):
    @classmethod
    def f(cls, cond, sleeping, woken, timeout=None):
        cond.acquire()
        sleeping.release()
        cond.wait(timeout)
        woken.release()
        cond.release()

    def assertReachesEventually(self, func, value):
        for i in range(10):
            try:
                if func() == value:
                    break
            except NotImplementedError:
                break
            time.sleep(DELTA)
        time.sleep(DELTA)
        self.assertReturnsIfImplemented(value, func)

    def check_invariant(self, cond):
        # this is only supposed to succeed when there are no sleepers
        if self.TYPE == "processes":
            try:
                sleepers = cond._sleeping_count.get_value() - cond._woken_count.get_value()
                self.assertEqual(sleepers, 0)
                self.assertEqual(cond._wait_semaphore.get_value(), 0)
            except NotImplementedError:
                pass

    def test_notify(self):
        cond = self.Condition()
        sleeping = self.Semaphore(0)
        woken = self.Semaphore(0)

        p = self.Process(target=self.f, args=(cond, sleeping, woken))
        p.daemon = True
        p.start()
        self.addCleanup(p.join)

        p = threading.Thread(target=self.f, args=(cond, sleeping, woken))
        p.daemon = True
        p.start()
        self.addCleanup(p.join)

        # wait for both children to start sleeping
        sleeping.acquire()
        sleeping.acquire()

        # check no process/thread has woken up
        time.sleep(DELTA)
        self.assertReturnsIfImplemented(0, get_value, woken)

        # wake up one process/thread
        cond.acquire()
        cond.notify()
        cond.release()

        # check one process/thread has woken up
        time.sleep(DELTA)
        self.assertReturnsIfImplemented(1, get_value, woken)

        # wake up another
        cond.acquire()
        cond.notify()
        cond.release()

        # check other has woken up
        time.sleep(DELTA)
        self.assertReturnsIfImplemented(2, get_value, woken)

        # check state is not mucked up
        self.check_invariant(cond)
        p.join()

    def test_notify_all(self):
        cond = self.Condition()
        sleeping = self.Semaphore(0)
        woken = self.Semaphore(0)

        # start some threads/processes which will timeout
        for i in range(3):
            p = self.Process(target=self.f, args=(cond, sleeping, woken, TIMEOUT1))
            p.daemon = True
            p.start()
            self.addCleanup(p.join)

            t = threading.Thread(target=self.f, args=(cond, sleeping, woken, TIMEOUT1))
            t.daemon = True
            t.start()
            self.addCleanup(t.join)

        # wait for them all to sleep
        for i in range(6):
            sleeping.acquire()

        # check they have all timed out
        for i in range(6):
            woken.acquire()
        self.assertReturnsIfImplemented(0, get_value, woken)

        # check state is not mucked up
        self.check_invariant(cond)

        # start some more threads/processes
        for i in range(3):
            p = self.Process(target=self.f, args=(cond, sleeping, woken))
            p.daemon = True
            p.start()
            self.addCleanup(p.join)

            t = threading.Thread(target=self.f, args=(cond, sleeping, woken))
            t.daemon = True
            t.start()
            self.addCleanup(t.join)

        # wait for them to all sleep
        for i in range(6):
            sleeping.acquire()

        # check no process/thread has woken up
        time.sleep(DELTA)
        self.assertReturnsIfImplemented(0, get_value, woken)

        # wake them all up
        cond.acquire()
        cond.notify_all()
        cond.release()

        # check they have all woken
        self.assertReachesEventually(lambda: get_value(woken), 6)

        # check state is not mucked up
        self.check_invariant(cond)

    def test_notify_n(self):
        cond = self.Condition()
        sleeping = self.Semaphore(0)
        woken = self.Semaphore(0)

        # start some threads/processes
        for i in range(3):
            p = self.Process(target=self.f, args=(cond, sleeping, woken))
            p.daemon = True
            p.start()
            self.addCleanup(p.join)

            t = threading.Thread(target=self.f, args=(cond, sleeping, woken))
            t.daemon = True
            t.start()
            self.addCleanup(t.join)

        # wait for them to all sleep
        for i in range(6):
            sleeping.acquire()

        # check no process/thread has woken up
        time.sleep(DELTA)
        self.assertReturnsIfImplemented(0, get_value, woken)

        # wake some of them up
        cond.acquire()
        cond.notify(n=2)
        cond.release()

        # check 2 have woken
        self.assertReachesEventually(lambda: get_value(woken), 2)

        # wake the rest of them
        cond.acquire()
        cond.notify(n=4)
        cond.release()

        self.assertReachesEventually(lambda: get_value(woken), 6)

        # doesn't do anything more
        cond.acquire()
        cond.notify(n=3)
        cond.release()

        self.assertReturnsIfImplemented(6, get_value, woken)

        # check state is not mucked up
        self.check_invariant(cond)

    def test_timeout(self):
        cond = self.Condition()
        wait = TimingWrapper(cond.wait)
        cond.acquire()
        res = wait(TIMEOUT1)
        cond.release()
        self.assertEqual(res, False)
        self.assertTimingAlmostEqual(wait.elapsed, TIMEOUT1)

    @classmethod
    def _test_waitfor_f(cls, cond, state):
        with cond:
            state.value = 0
            cond.notify()
            result = cond.wait_for(lambda: state.value == 4)
            if not result or state.value != 4:
                sys.exit(1)

    @unittest.skip("DRAGON: hangs")
    @unittest.skipUnless(HAS_SHAREDCTYPES, "needs sharedctypes")
    def test_waitfor(self):
        # based on test in test/lock_tests.py
        cond = self.Condition()
        state = self.Value("i", -1)

        p = self.Process(target=self._test_waitfor_f, args=(cond, state))
        p.daemon = True
        p.start()

        with cond:
            result = cond.wait_for(lambda: state.value == 0)
            self.assertTrue(result)
            self.assertEqual(state.value, 0)

        for i in range(4):
            time.sleep(0.01)
            with cond:
                state.value += 1
                cond.notify()

        join_process(p)
        self.assertEqual(p.exitcode, 0)

    @classmethod
    def _test_waitfor_timeout_f(cls, cond, state, success, sem):
        sem.release()
        with cond:
            expected = 0.1
            dt = time.monotonic()
            result = cond.wait_for(lambda: state.value == 4, timeout=expected)
            dt = time.monotonic() - dt
            # borrow logic in assertTimeout() from test/lock_tests.py
            if not result and expected * 0.6 < dt < expected * 10.0:
                success.value = True

    @unittest.skip("DRAGON: hangs")
    @unittest.skipUnless(HAS_SHAREDCTYPES, "needs sharedctypes")
    def test_waitfor_timeout(self):
        # based on test in test/lock_tests.py
        cond = self.Condition()
        state = self.Value("i", 0)
        success = self.Value("i", False)
        sem = self.Semaphore(0)

        p = self.Process(target=self._test_waitfor_timeout_f, args=(cond, state, success, sem))
        p.daemon = True
        p.start()
        self.assertTrue(sem.acquire(timeout=support.LONG_TIMEOUT))

        # Only increment 3 times, so state == 4 is never reached.
        for i in range(3):
            time.sleep(0.01)
            with cond:
                state.value += 1
                cond.notify()

        join_process(p)
        self.assertTrue(success.value)

    @classmethod
    def _test_wait_result(cls, c, pid):
        with c:
            c.notify()
        time.sleep(1)
        if pid is not None:
            os.kill(pid, signal.SIGINT)

    def test_wait_result(self):
        if isinstance(self, ProcessesMixin) and sys.platform != "win32":
            pid = os.getpid()
        else:
            pid = None

        c = self.Condition()
        with c:
            self.assertFalse(c.wait(0))
            self.assertFalse(c.wait(0.1))

            p = self.Process(target=self._test_wait_result, args=(c, pid))
            p.start()

            self.assertTrue(c.wait(60))
            if pid is not None:
                self.assertRaises(KeyboardInterrupt, c.wait, 60)

            p.join()


@unittest.skip("DRAGON: Threads not implemented")
class WithThreadsTestCondition(BaseTestCase, ThreadsMixin, unittest.TestCase):
    @classmethod
    def f(cls, cond, sleeping, woken, timeout=None):
        cond.acquire()
        sleeping.release()
        cond.wait(timeout)
        woken.release()
        cond.release()

    def assertReachesEventually(self, func, value):
        for i in range(10):
            try:
                if func() == value:
                    break
            except NotImplementedError:
                break
            time.sleep(DELTA)
        time.sleep(DELTA)
        self.assertReturnsIfImplemented(value, func)

    def check_invariant(self, cond):
        # this is only supposed to succeed when there are no sleepers
        if self.TYPE == "processes":
            try:
                sleepers = cond._sleeping_count.get_value() - cond._woken_count.get_value()
                self.assertEqual(sleepers, 0)
                self.assertEqual(cond._wait_semaphore.get_value(), 0)
            except NotImplementedError:
                pass

    def test_notify(self):
        cond = self.Condition()
        sleeping = self.Semaphore(0)
        woken = self.Semaphore(0)

        p = self.Process(target=self.f, args=(cond, sleeping, woken))
        p.daemon = True
        p.start()
        self.addCleanup(p.join)

        p = threading.Thread(target=self.f, args=(cond, sleeping, woken))
        p.daemon = True
        p.start()
        self.addCleanup(p.join)

        # wait for both children to start sleeping
        sleeping.acquire()
        sleeping.acquire()

        # check no process/thread has woken up
        time.sleep(DELTA)
        self.assertReturnsIfImplemented(0, get_value, woken)

        # wake up one process/thread
        cond.acquire()
        cond.notify()
        cond.release()

        # check one process/thread has woken up
        time.sleep(DELTA)
        self.assertReturnsIfImplemented(1, get_value, woken)

        # wake up another
        cond.acquire()
        cond.notify()
        cond.release()

        # check other has woken up
        time.sleep(DELTA)
        self.assertReturnsIfImplemented(2, get_value, woken)

        # check state is not mucked up
        self.check_invariant(cond)
        p.join()

    def test_notify_all(self):
        cond = self.Condition()
        sleeping = self.Semaphore(0)
        woken = self.Semaphore(0)

        # start some threads/processes which will timeout
        for i in range(3):
            p = self.Process(target=self.f, args=(cond, sleeping, woken, TIMEOUT1))
            p.daemon = True
            p.start()
            self.addCleanup(p.join)

            t = threading.Thread(target=self.f, args=(cond, sleeping, woken, TIMEOUT1))
            t.daemon = True
            t.start()
            self.addCleanup(t.join)

        # wait for them all to sleep
        for i in range(6):
            sleeping.acquire()

        # check they have all timed out
        for i in range(6):
            woken.acquire()
        self.assertReturnsIfImplemented(0, get_value, woken)

        # check state is not mucked up
        self.check_invariant(cond)

        # start some more threads/processes
        for i in range(3):
            p = self.Process(target=self.f, args=(cond, sleeping, woken))
            p.daemon = True
            p.start()
            self.addCleanup(p.join)

            t = threading.Thread(target=self.f, args=(cond, sleeping, woken))
            t.daemon = True
            t.start()
            self.addCleanup(t.join)

        # wait for them to all sleep
        for i in range(6):
            sleeping.acquire()

        # check no process/thread has woken up
        time.sleep(DELTA)
        self.assertReturnsIfImplemented(0, get_value, woken)

        # wake them all up
        cond.acquire()
        cond.notify_all()
        cond.release()

        # check they have all woken
        self.assertReachesEventually(lambda: get_value(woken), 6)

        # check state is not mucked up
        self.check_invariant(cond)

    def test_notify_n(self):
        cond = self.Condition()
        sleeping = self.Semaphore(0)
        woken = self.Semaphore(0)

        # start some threads/processes
        for i in range(3):
            p = self.Process(target=self.f, args=(cond, sleeping, woken))
            p.daemon = True
            p.start()
            self.addCleanup(p.join)

            t = threading.Thread(target=self.f, args=(cond, sleeping, woken))
            t.daemon = True
            t.start()
            self.addCleanup(t.join)

        # wait for them to all sleep
        for i in range(6):
            sleeping.acquire()

        # check no process/thread has woken up
        time.sleep(DELTA)
        self.assertReturnsIfImplemented(0, get_value, woken)

        # wake some of them up
        cond.acquire()
        cond.notify(n=2)
        cond.release()

        # check 2 have woken
        self.assertReachesEventually(lambda: get_value(woken), 2)

        # wake the rest of them
        cond.acquire()
        cond.notify(n=4)
        cond.release()

        self.assertReachesEventually(lambda: get_value(woken), 6)

        # doesn't do anything more
        cond.acquire()
        cond.notify(n=3)
        cond.release()

        self.assertReturnsIfImplemented(6, get_value, woken)

        # check state is not mucked up
        self.check_invariant(cond)

    def test_timeout(self):
        cond = self.Condition()
        wait = TimingWrapper(cond.wait)
        cond.acquire()
        res = wait(TIMEOUT1)
        cond.release()
        self.assertEqual(res, False)
        self.assertTimingAlmostEqual(wait.elapsed, TIMEOUT1)

    @classmethod
    def _test_waitfor_f(cls, cond, state):
        with cond:
            state.value = 0
            cond.notify()
            result = cond.wait_for(lambda: state.value == 4)
            if not result or state.value != 4:
                sys.exit(1)

    @unittest.skip("DRAGON: hangs")
    @unittest.skipUnless(HAS_SHAREDCTYPES, "needs sharedctypes")
    def test_waitfor(self):
        # based on test in test/lock_tests.py
        cond = self.Condition()
        state = self.Value("i", -1)

        p = self.Process(target=self._test_waitfor_f, args=(cond, state))
        p.daemon = True
        p.start()

        with cond:
            result = cond.wait_for(lambda: state.value == 0)
            self.assertTrue(result)
            self.assertEqual(state.value, 0)

        for i in range(4):
            time.sleep(0.01)
            with cond:
                state.value += 1
                cond.notify()

        join_process(p)
        self.assertEqual(p.exitcode, 0)

    @classmethod
    def _test_waitfor_timeout_f(cls, cond, state, success, sem):
        sem.release()
        with cond:
            expected = 0.1
            dt = time.monotonic()
            result = cond.wait_for(lambda: state.value == 4, timeout=expected)
            dt = time.monotonic() - dt
            # borrow logic in assertTimeout() from test/lock_tests.py
            if not result and expected * 0.6 < dt < expected * 10.0:
                success.value = True

    @unittest.skip("DRAGON: hangs")
    @unittest.skipUnless(HAS_SHAREDCTYPES, "needs sharedctypes")
    def test_waitfor_timeout(self):
        # based on test in test/lock_tests.py
        cond = self.Condition()
        state = self.Value("i", 0)
        success = self.Value("i", False)
        sem = self.Semaphore(0)

        p = self.Process(target=self._test_waitfor_timeout_f, args=(cond, state, success, sem))
        p.daemon = True
        p.start()
        self.assertTrue(sem.acquire(timeout=support.LONG_TIMEOUT))

        # Only increment 3 times, so state == 4 is never reached.
        for i in range(3):
            time.sleep(0.01)
            with cond:
                state.value += 1
                cond.notify()

        join_process(p)
        self.assertTrue(success.value)

    @classmethod
    def _test_wait_result(cls, c, pid):
        with c:
            c.notify()
        time.sleep(1)
        if pid is not None:
            os.kill(pid, signal.SIGINT)

    def test_wait_result(self):
        if isinstance(self, ProcessesMixin) and sys.platform != "win32":
            pid = os.getpid()
        else:
            pid = None

        c = self.Condition()
        with c:
            self.assertFalse(c.wait(0))
            self.assertFalse(c.wait(0.1))

            p = self.Process(target=self._test_wait_result, args=(c, pid))
            p.start()

            self.assertTrue(c.wait(60))
            if pid is not None:
                self.assertRaises(KeyboardInterrupt, c.wait, 60)

            p.join()


# DRAGON
if __name__ == "__main__":
    setUpModule()
    unittest.main()
    tearDownModule()
