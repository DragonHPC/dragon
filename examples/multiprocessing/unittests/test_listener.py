"""Test multiprocessing connection, listener, client, pickling
"""
import unittest
import time
import socket

from test.support import hashlib_helper
from test.support import socket_helper

import dragon  # DRAGON import before multiprocessing

import multiprocessing
from multiprocessing import util

try:
    from multiprocessing import reduction

    HAS_REDUCTION = reduction.HAVE_SEND_HANDLE
except ImportError:
    HAS_REDUCTION = False

from common import (
    BaseTestCase,
    ProcessesMixin,
    ThreadsMixin,
    latin,
    setUpModule,
    tearDownModule,
)


#
#
#

SENTINEL = latin("")


@unittest.skip(f"DRAGON: not implemented")
class WithProcessesTestListener(BaseTestCase, ProcessesMixin, unittest.TestCase):

    # DRAGON ALLOWED_TYPES = ('processes',)

    def test_multiple_bind(self):
        for family in self.connection.families:
            l = self.connection.Listener(family=family)
            self.addCleanup(l.close)
            self.assertRaises(OSError, self.connection.Listener, l.address, family)

    def test_context(self):
        with self.connection.Listener() as l:
            with self.connection.Client(l.address) as c:
                with l.accept() as d:
                    c.send(1729)
                    self.assertEqual(d.recv(), 1729)

        if self.TYPE == "processes":
            self.assertRaises(OSError, l.accept)

    @unittest.skipUnless(util.abstract_sockets_supported, "test needs abstract socket support")
    def test_abstract_socket(self):
        with self.connection.Listener("\0something") as listener:
            with self.connection.Client(listener.address) as client:
                with listener.accept() as d:
                    client.send(1729)
                    self.assertEqual(d.recv(), 1729)

        if self.TYPE == "processes":
            self.assertRaises(OSError, listener.accept)


@unittest.skip(f"DRAGON: not implemented")
class WithProcessesTestListenerClient(BaseTestCase, ProcessesMixin, unittest.TestCase):

    # DRAGON ALLOWED_TYPES = ('processes', 'threads')

    @classmethod
    def _test(cls, address):
        conn = cls.connection.Client(address)
        conn.send("hello")
        conn.close()

    def test_listener_client(self):
        for family in self.connection.families:
            l = self.connection.Listener(family=family)
            p = self.Process(target=self._test, args=(l.address,))
            p.daemon = True
            p.start()
            conn = l.accept()
            self.assertEqual(conn.recv(), "hello")
            p.join()
            l.close()

    def test_issue14725(self):
        l = self.connection.Listener()
        p = self.Process(target=self._test, args=(l.address,))
        p.daemon = True
        p.start()
        time.sleep(1)
        # On Windows the client process should by now have connected,
        # written data and closed the pipe handle by now.  This causes
        # ConnectNamdedPipe() to fail with ERROR_NO_DATA.  See Issue
        # 14725.
        conn = l.accept()
        self.assertEqual(conn.recv(), "hello")
        conn.close()
        p.join()
        l.close()

    def test_issue16955(self):
        for fam in self.connection.families:
            l = self.connection.Listener(family=fam)
            c = self.connection.Client(l.address)
            a = l.accept()
            a.send_bytes(b"hello")
            self.assertTrue(c.poll(1))
            a.close()
            c.close()
            l.close()


@unittest.skip("DRAGON: Threads not implemented")
class WithThreadsTestListenerClient(BaseTestCase, ThreadsMixin, unittest.TestCase):

    # DRAGON ALLOWED_TYPES = ('processes', 'threads')

    @classmethod
    def _test(cls, address):
        conn = cls.connection.Client(address)
        conn.send("hello")
        conn.close()

    def test_listener_client(self):
        for family in self.connection.families:
            l = self.connection.Listener(family=family)
            p = self.Process(target=self._test, args=(l.address,))
            p.daemon = True
            p.start()
            conn = l.accept()
            self.assertEqual(conn.recv(), "hello")
            p.join()
            l.close()

    def test_issue14725(self):
        l = self.connection.Listener()
        p = self.Process(target=self._test, args=(l.address,))
        p.daemon = True
        p.start()
        time.sleep(1)
        # On Windows the client process should by now have connected,
        # written data and closed the pipe handle by now.  This causes
        # ConnectNamdedPipe() to fail with ERROR_NO_DATA.  See Issue
        # 14725.
        conn = l.accept()
        self.assertEqual(conn.recv(), "hello")
        conn.close()
        p.join()
        l.close()

    def test_issue16955(self):
        for fam in self.connection.families:
            l = self.connection.Listener(family=fam)
            c = self.connection.Client(l.address)
            a = l.accept()
            a.send_bytes(b"hello")
            self.assertTrue(c.poll(1))
            a.close()
            c.close()
            l.close()


# DRAGON
if __name__ == "__main__":
    setUpModule()
    unittest.main()
    tearDownModule()
