""" Test multiprocessing lock
"""
import unittest
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
)

#
#
#


class WithProcessesTestLock(BaseTestCase, ProcessesMixin, unittest.TestCase):
    def test_lock(self):
        lock = self.Lock()
        self.assertEqual(lock.acquire(), True)
        self.assertEqual(lock.acquire(False), False)
        self.assertEqual(lock.release(), None)
        self.assertRaises((ValueError, threading.ThreadError), lock.release)

    def test_rlock(self):
        lock = self.RLock()
        self.assertEqual(lock.acquire(), True)
        self.assertEqual(lock.acquire(), True)
        self.assertEqual(lock.acquire(), True)
        self.assertEqual(lock.release(), None)
        self.assertEqual(lock.release(), None)
        self.assertEqual(lock.release(), None)
        self.assertRaises((AssertionError, RuntimeError), lock.release)

    def test_lock_context(self):
        with self.Lock():
            pass


@unittest.skip("DRAGON: Manager not implemented")
@hashlib_helper.requires_hashdigest("md5")
class WithManagerTestLock(BaseTestCase, ManagerMixin, unittest.TestCase):
    def test_lock(self):
        lock = self.Lock()
        self.assertEqual(lock.acquire(), True)
        self.assertEqual(lock.acquire(False), False)
        self.assertEqual(lock.release(), None)
        self.assertRaises((ValueError, threading.ThreadError), lock.release)

    def test_rlock(self):
        lock = self.RLock()
        self.assertEqual(lock.acquire(), True)
        self.assertEqual(lock.acquire(), True)
        self.assertEqual(lock.acquire(), True)
        self.assertEqual(lock.release(), None)
        self.assertEqual(lock.release(), None)
        self.assertEqual(lock.release(), None)
        self.assertRaises((AssertionError, RuntimeError), lock.release)

    def test_lock_context(self):
        with self.Lock():
            pass


@unittest.skip("DRAGON: Threads not implemented")
class WithThreadsTestLock(BaseTestCase, ThreadsMixin, unittest.TestCase):
    def test_lock(self):
        lock = self.Lock()
        self.assertEqual(lock.acquire(), True)
        self.assertEqual(lock.acquire(False), False)
        self.assertEqual(lock.release(), None)
        self.assertRaises((ValueError, threading.ThreadError), lock.release)

    def test_rlock(self):
        lock = self.RLock()
        self.assertEqual(lock.acquire(), True)
        self.assertEqual(lock.acquire(), True)
        self.assertEqual(lock.acquire(), True)
        self.assertEqual(lock.release(), None)
        self.assertEqual(lock.release(), None)
        self.assertEqual(lock.release(), None)
        self.assertRaises((AssertionError, RuntimeError), lock.release)

    def test_lock_context(self):
        with self.Lock():
            pass


# DRAGON
if __name__ == "__main__":
    setUpModule()
    unittest.main()
    tearDownModule()
