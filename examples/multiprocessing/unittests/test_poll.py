"""Test multiprocessing.Process()
"""
import os
import time
import signal

import unittest


import dragon  # DRAGON import before multiprocessing

import multiprocessing
from multiprocessing import util
from multiprocessing.connection import wait

from common import (
    BaseTestCase,
    ProcessesMixin,
    ThreadsMixin,
    setUpModule,
    tearDownModule,
)


class WithProcessesTestPoll(BaseTestCase, ProcessesMixin, unittest.TestCase):

    # DRAGON ALLOWED_TYPES = ('processes', 'threads')

    def test_empty_string(self):
        a, b = self.Pipe()
        self.assertEqual(a.poll(), False)
        b.send_bytes(b"")
        self.assertEqual(a.poll(), True)
        self.assertEqual(a.poll(), True)

    @classmethod
    def _child_strings(cls, conn, strings):
        for s in strings:
            time.sleep(0.1)
            conn.send_bytes(s)
        conn.close()

    def test_strings(self):
        strings = (b"hello", b"", b"a", b"b", b"", b"bye", b"", b"lop")
        a, b = self.Pipe()
        p = self.Process(target=self._child_strings, args=(b, strings))
        p.start()

        for s in strings:
            for i in range(200):
                if a.poll(0.01):
                    break
            x = a.recv_bytes()
            self.assertEqual(s, x)

        p.join()

    @classmethod
    def _child_boundaries(cls, r):
        # Polling may "pull" a message in to the child process, but we
        # don't want it to pull only part of a message, as that would
        # corrupt the pipe for any other processes which might later
        # read from it.
        r.poll(5)

    def test_boundaries(self):
        r, w = self.Pipe(False)
        p = self.Process(target=self._child_boundaries, args=(r,))
        p.start()
        time.sleep(2)
        L = [b"first", b"second"]
        for obj in L:
            w.send_bytes(obj)
        w.close()
        p.join()
        self.assertIn(r.recv_bytes(), L)

    @classmethod
    def _child_dont_merge(cls, b):
        b.send_bytes(b"a")
        b.send_bytes(b"b")
        b.send_bytes(b"cd")

    def test_dont_merge(self):
        a, b = self.Pipe()
        self.assertEqual(a.poll(0.0), False)
        self.assertEqual(a.poll(0.1), False)

        p = self.Process(target=self._child_dont_merge, args=(b,))
        p.start()

        self.assertEqual(a.recv_bytes(), b"a")
        self.assertEqual(a.poll(1.0), True)
        self.assertEqual(a.poll(1.0), True)
        self.assertEqual(a.recv_bytes(), b"b")
        self.assertEqual(a.poll(1.0), True)
        self.assertEqual(a.poll(1.0), True)
        self.assertEqual(a.poll(0.0), True)
        self.assertEqual(a.recv_bytes(), b"cd")

        p.join()


class WithProcessesTestPollEintr(BaseTestCase, ProcessesMixin, unittest.TestCase):

    # DRAGON ALLOWED_TYPES = ('processes',)

    @classmethod
    def _killer(cls, pid):
        time.sleep(0.1)
        os.kill(pid, signal.SIGUSR1)

    @unittest.skipUnless(hasattr(signal, "SIGUSR1"), "requires SIGUSR1")
    def test_poll_eintr(self):
        got_signal = [False]

        def record(*args):
            got_signal[0] = True

        pid = os.getpid()
        oldhandler = signal.signal(signal.SIGUSR1, record)
        try:
            killer = self.Process(target=self._killer, args=(pid,))
            killer.start()
            try:
                p = self.Process(target=time.sleep, args=(2,))
                p.start()
                p.join()
            finally:
                killer.join()
            self.assertTrue(got_signal[0])
            self.assertEqual(p.exitcode, 0)
        finally:
            signal.signal(signal.SIGUSR1, oldhandler)


@unittest.skip("DRAGON: Threads not implemented")
class WithThreadsTestPoll(BaseTestCase, ThreadsMixin, unittest.TestCase):

    # DRAGON ALLOWED_TYPES = ('processes', 'threads')

    def test_empty_string(self):
        a, b = self.Pipe()
        self.assertEqual(a.poll(), False)
        b.send_bytes(b"")
        self.assertEqual(a.poll(), True)
        self.assertEqual(a.poll(), True)

    @classmethod
    def _child_strings(cls, conn, strings):
        for s in strings:
            time.sleep(0.1)
            conn.send_bytes(s)
        conn.close()

    def test_strings(self):
        strings = (b"hello", b"", b"a", b"b", b"", b"bye", b"", b"lop")
        a, b = self.Pipe()
        p = self.Process(target=self._child_strings, args=(b, strings))
        p.start()

        for s in strings:
            for i in range(200):
                if a.poll(0.01):
                    break
            x = a.recv_bytes()
            self.assertEqual(s, x)

        p.join()

    @classmethod
    def _child_boundaries(cls, r):
        # Polling may "pull" a message in to the child process, but we
        # don't want it to pull only part of a message, as that would
        # corrupt the pipe for any other processes which might later
        # read from it.
        r.poll(5)

    def test_boundaries(self):
        r, w = self.Pipe(False)
        p = self.Process(target=self._child_boundaries, args=(r,))
        p.start()
        time.sleep(2)
        L = [b"first", b"second"]
        for obj in L:
            w.send_bytes(obj)
        w.close()
        p.join()
        self.assertIn(r.recv_bytes(), L)

    @classmethod
    def _child_dont_merge(cls, b):
        b.send_bytes(b"a")
        b.send_bytes(b"b")
        b.send_bytes(b"cd")

    def test_dont_merge(self):
        a, b = self.Pipe()
        self.assertEqual(a.poll(0.0), False)
        self.assertEqual(a.poll(0.1), False)

        p = self.Process(target=self._child_dont_merge, args=(b,))
        p.start()

        self.assertEqual(a.recv_bytes(), b"a")
        self.assertEqual(a.poll(1.0), True)
        self.assertEqual(a.poll(1.0), True)
        self.assertEqual(a.recv_bytes(), b"b")
        self.assertEqual(a.poll(1.0), True)
        self.assertEqual(a.poll(1.0), True)
        self.assertEqual(a.poll(0.0), True)
        self.assertEqual(a.recv_bytes(), b"cd")

        p.join()


# DRAGON
if __name__ == "__main__":
    setUpModule()
    unittest.main()
    tearDownModule()
