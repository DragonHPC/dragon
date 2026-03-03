"""Test multiprocessing connection, listener, client, pickling
"""
import unittest
import sys
import time
import array
import os
import errno
import socket

try:
    import msvcrt
except ImportError:
    msvcrt = None

import test.support
from test.support import hashlib_helper
from test.support import socket_helper

import dragon  # DRAGON import before multiprocessing

import multiprocessing

try:
    from multiprocessing import reduction

    HAS_REDUCTION = reduction.HAVE_SEND_HANDLE
except ImportError:
    HAS_REDUCTION = False

from common import (
    BaseTestCase,
    ProcessesMixin,
    ThreadsMixin,
    TimingWrapper,
    TIMEOUT1,
    MAXFD,
    latin,
    setUpModule,
    tearDownModule,
)

#
#
#

SENTINEL = latin("")


class WithProcessesTestConnection(BaseTestCase, ProcessesMixin, unittest.TestCase):

    # DRAGON ALLOWED_TYPES = ('processes', 'threads')

    @classmethod
    def _echo(cls, conn):
        for msg in iter(conn.recv_bytes, SENTINEL):
            conn.send_bytes(msg)
        conn.close()

    @unittest.skip("bug filed PE-40894")
    def test_connection(self):
        conn, child_conn = self.Pipe()

        p = self.Process(target=self._echo, args=(child_conn,))
        p.daemon = True
        p.start()

        seq = [1, 2.25, None]
        msg = latin("hello world")
        longmsg = msg * 10
        arr = array.array("i", list(range(4)))

        if self.TYPE == "processes":
            self.assertEqual(type(conn.fileno()), int)

        self.assertEqual(conn.send(seq), None)
        self.assertEqual(conn.recv(), seq)

        self.assertEqual(conn.send_bytes(msg), None)
        self.assertEqual(conn.recv_bytes(), msg)

        if self.TYPE == "processes":
            buffer = array.array("i", [0] * 10)
            expected = list(arr) + [0] * (10 - len(arr))
            self.assertEqual(conn.send_bytes(arr), None)
            self.assertEqual(conn.recv_bytes_into(buffer), len(arr) * buffer.itemsize)
            self.assertEqual(list(buffer), expected)

            buffer = array.array("i", [0] * 10)
            expected = [0] * 3 + list(arr) + [0] * (10 - 3 - len(arr))
            self.assertEqual(conn.send_bytes(arr), None)
            self.assertEqual(conn.recv_bytes_into(buffer, 3 * buffer.itemsize), len(arr) * buffer.itemsize)
            self.assertEqual(list(buffer), expected)

            buffer = bytearray(latin(" " * 40))
            self.assertEqual(conn.send_bytes(longmsg), None)
            try:
                res = conn.recv_bytes_into(buffer)
            except multiprocessing.BufferTooShort as e:
                self.assertEqual(e.args, (longmsg,))
            else:
                self.fail("expected BufferTooShort, got %s" % res)

        poll = TimingWrapper(conn.poll)

        self.assertEqual(poll(), False)
        self.assertTimingAlmostEqual(poll.elapsed, 0)

        self.assertEqual(poll(-1), False)
        self.assertTimingAlmostEqual(poll.elapsed, 0)

        self.assertEqual(poll(TIMEOUT1), False)
        self.assertTimingAlmostEqual(poll.elapsed, TIMEOUT1)

        conn.send(None)
        time.sleep(0.1)

        self.assertEqual(poll(TIMEOUT1), True)
        self.assertTimingAlmostEqual(poll.elapsed, 0)

        self.assertEqual(conn.recv(), None)

        really_big_msg = latin("X") * (1024 * 1024 * 16)  # 16Mb
        conn.send_bytes(really_big_msg)
        self.assertEqual(conn.recv_bytes(), really_big_msg)

        conn.send_bytes(SENTINEL)  # tell child to quit
        child_conn.close()

        if self.TYPE == "processes":
            self.assertEqual(conn.readable, True)
            self.assertEqual(conn.writable, True)
            self.assertRaises(EOFError, conn.recv)
            self.assertRaises(EOFError, conn.recv_bytes)

        p.join()

    def test_duplex_false(self):
        reader, writer = self.Pipe(duplex=False)
        self.assertEqual(writer.send(1), None)
        self.assertEqual(reader.recv(), 1)
        if self.TYPE == "processes":
            self.assertEqual(reader.readable, True)
            self.assertEqual(reader.writable, False)
            self.assertEqual(writer.readable, False)
            self.assertEqual(writer.writable, True)
            self.assertRaises(OSError, reader.send, 2)
            self.assertRaises(OSError, writer.recv)
            self.assertRaises(OSError, writer.poll)

    def test_spawn_close(self):
        # We test that a pipe connection can be closed by parent
        # process immediately after child is spawned.  On Windows this
        # would have sometimes failed on old versions because
        # child_conn would be closed before the child got a chance to
        # duplicate it.
        conn, child_conn = self.Pipe()

        p = self.Process(target=self._echo, args=(child_conn,))
        p.daemon = True
        p.start()
        child_conn.close()  # this might complete before child initializes

        msg = latin("hello")
        conn.send_bytes(msg)
        self.assertEqual(conn.recv_bytes(), msg)

        conn.send_bytes(SENTINEL)
        conn.close()
        p.join()

    def test_sendbytes(self):
        if self.TYPE != "processes":
            self.skipTest("test not appropriate for {}".format(self.TYPE))

        msg = latin("abcdefghijklmnopqrstuvwxyz")
        a, b = self.Pipe()

        a.send_bytes(msg)
        self.assertEqual(b.recv_bytes(), msg)

        a.send_bytes(msg, 5)
        self.assertEqual(b.recv_bytes(), msg[5:])

        a.send_bytes(msg, 7, 8)
        self.assertEqual(b.recv_bytes(), msg[7 : 7 + 8])

        a.send_bytes(msg, 26)
        self.assertEqual(b.recv_bytes(), latin(""))

        a.send_bytes(msg, 26, 0)
        self.assertEqual(b.recv_bytes(), latin(""))

        self.assertRaises(ValueError, a.send_bytes, msg, 27)

        self.assertRaises(ValueError, a.send_bytes, msg, 22, 5)

        self.assertRaises(ValueError, a.send_bytes, msg, 26, 1)

        self.assertRaises(ValueError, a.send_bytes, msg, -1)

        self.assertRaises(ValueError, a.send_bytes, msg, 4, -1)

    @classmethod
    def _is_fd_assigned(cls, fd):
        try:
            os.fstat(fd)
        except OSError as e:
            if e.errno == errno.EBADF:
                return False
            raise
        else:
            return True

    @classmethod
    def _writefd(cls, conn, data, create_dummy_fds=False):
        if create_dummy_fds:
            for i in range(0, 256):
                if not cls._is_fd_assigned(i):
                    os.dup2(conn.fileno(), i)
        fd = reduction.recv_handle(conn)
        if msvcrt:
            fd = msvcrt.open_osfhandle(fd, os.O_WRONLY)
        os.write(fd, data)
        os.close(fd)

    @unittest.skip("bug filed PE-40895")
    @unittest.skipUnless(HAS_REDUCTION, "test needs multiprocessing.reduction")
    def test_fd_transfer(self):
        if self.TYPE != "processes":
            self.skipTest("only makes sense with processes")
        conn, child_conn = self.Pipe(duplex=True)

        p = self.Process(target=self._writefd, args=(child_conn, b"foo"))
        p.daemon = True
        p.start()
        self.addCleanup(test.support.unlink, test.support.TESTFN)
        with open(test.support.TESTFN, "wb") as f:
            fd = f.fileno()
            if msvcrt:
                fd = msvcrt.get_osfhandle(fd)
            reduction.send_handle(conn, fd, p.pid)
        p.join()
        with open(test.support.TESTFN, "rb") as f:
            self.assertEqual(f.read(), b"foo")

    @unittest.skip("bug filed PE-40896")
    @unittest.skipUnless(HAS_REDUCTION, "test needs multiprocessing.reduction")
    @unittest.skipIf(sys.platform == "win32", "test semantics don't make sense on Windows")
    @unittest.skipIf(MAXFD <= 256, "largest assignable fd number is too small")
    @unittest.skipUnless(hasattr(os, "dup2"), "test needs os.dup2()")
    def test_large_fd_transfer(self):
        # With fd > 256 (issue #11657)
        if self.TYPE != "processes":
            self.skipTest("only makes sense with processes")
        conn, child_conn = self.Pipe(duplex=True)

        p = self.Process(target=self._writefd, args=(child_conn, b"bar", True))
        p.daemon = True
        p.start()
        self.addCleanup(test.support.unlink, test.support.TESTFN)
        with open(test.support.TESTFN, "wb") as f:
            fd = f.fileno()
            for newfd in range(256, MAXFD):
                if not self._is_fd_assigned(newfd):
                    break
            else:
                self.fail("could not find an unassigned large file descriptor")
            os.dup2(fd, newfd)
            try:
                reduction.send_handle(conn, newfd, p.pid)
            finally:
                os.close(newfd)
        p.join()
        with open(test.support.TESTFN, "rb") as f:
            self.assertEqual(f.read(), b"bar")

    @classmethod
    def _send_data_without_fd(self, conn):
        os.write(conn.fileno(), b"\0")

    @unittest.skip("bug filed PE-40896")
    @unittest.skipUnless(HAS_REDUCTION, "test needs multiprocessing.reduction")
    @unittest.skipIf(sys.platform == "win32", "doesn't make sense on Windows")
    def test_missing_fd_transfer(self):
        # Check that exception is raised when received data is not
        # accompanied by a file descriptor in ancillary data.
        if self.TYPE != "processes":
            self.skipTest("only makes sense with processes")
        conn, child_conn = self.Pipe(duplex=True)

        p = self.Process(target=self._send_data_without_fd, args=(child_conn,))
        p.daemon = True
        p.start()
        self.assertRaises(RuntimeError, reduction.recv_handle, conn)
        p.join()

    def test_context(self):
        a, b = self.Pipe()

        with a, b:
            a.send(1729)
            self.assertEqual(b.recv(), 1729)
            if self.TYPE == "processes":
                self.assertFalse(a.closed)
                self.assertFalse(b.closed)

        if self.TYPE == "processes":
            self.assertTrue(a.closed)
            self.assertTrue(b.closed)
            self.assertRaises(OSError, a.recv)
            self.assertRaises(OSError, b.recv)

#
# Test of sending connection and socket objects between processes
#


@unittest.skipUnless(HAS_REDUCTION, "test needs multiprocessing.reduction")
@hashlib_helper.requires_hashdigest("md5")
class WithProcessesTestPicklingConnections(BaseTestCase, ProcessesMixin, unittest.TestCase):

    ALLOWED_TYPES = ("processes",)

    @classmethod
    def tearDownClass(cls):
        from multiprocessing import resource_sharer

        resource_sharer.stop(timeout=test.support.LONG_TIMEOUT)

    @classmethod
    def _listener(cls, conn, families):
        for fam in families:
            l = cls.connection.Listener(family=fam)
            conn.send(l.address)
            new_conn = l.accept()
            conn.send(new_conn)
            new_conn.close()
            l.close()

        l = socket.create_server((socket_helper.HOST, 0))
        conn.send(l.getsockname())
        new_conn, addr = l.accept()
        conn.send(new_conn)
        new_conn.close()
        l.close()

        conn.recv()

    @classmethod
    def _remote(cls, conn):
        for (address, msg) in iter(conn.recv, None):
            client = cls.connection.Client(address)
            client.send(msg.upper())
            client.close()

        address, msg = conn.recv()
        client = socket.socket()
        client.connect(address)
        client.sendall(msg.upper())
        client.close()

        conn.close()

    @unittest.skip(f"DRAGON: Broken")
    def test_pickling(self):
        families = self.connection.families

        lconn, lconn0 = self.Pipe()
        lp = self.Process(target=self._listener, args=(lconn0, families))
        lp.daemon = True
        lp.start()
        lconn0.close()

        rconn, rconn0 = self.Pipe()
        rp = self.Process(target=self._remote, args=(rconn0,))
        rp.daemon = True
        rp.start()
        rconn0.close()

        for fam in families:
            msg = ("This connection uses family %s" % fam).encode("ascii")
            address = lconn.recv()
            rconn.send((address, msg))
            new_conn = lconn.recv()
            self.assertEqual(new_conn.recv(), msg.upper())

        rconn.send(None)

        msg = latin("This connection uses a normal socket")
        address = lconn.recv()
        rconn.send((address, msg))
        new_conn = lconn.recv()
        buf = []
        while True:
            s = new_conn.recv(100)
            if not s:
                break
            buf.append(s)
        buf = b"".join(buf)
        self.assertEqual(buf, msg.upper())
        new_conn.close()

        lconn.send(None)

        rconn.close()
        lconn.close()

        lp.join()
        rp.join()

    @classmethod
    def child_access(cls, conn):
        w = conn.recv()
        w.send("all is well")
        w.close()

        r = conn.recv()
        msg = r.recv()
        conn.send(msg * 2)

        conn.close()

    def test_access(self):
        # On Windows, if we do not specify a destination pid when
        # using DupHandle then we need to be careful to use the
        # correct access flags for DuplicateHandle(), or else
        # DupHandle.detach() will raise PermissionError.  For example,
        # for a read only pipe handle we should use
        # access=FILE_GENERIC_READ.  (Unfortunately
        # DUPLICATE_SAME_ACCESS does not work.)
        conn, child_conn = self.Pipe()
        p = self.Process(target=self.child_access, args=(child_conn,))
        p.daemon = True
        p.start()
        child_conn.close()

        r, w = self.Pipe(duplex=False)
        conn.send(w)
        w.close()
        self.assertEqual(r.recv(), "all is well")
        r.close()

        r, w = self.Pipe(duplex=False)
        conn.send(r)
        r.close()
        w.send("foobar")
        w.close()
        self.assertEqual(conn.recv(), "foobar" * 2)

        p.join()


@unittest.skip("DRAGON: Threads not implemented")
class WithThreadsTestConnection(BaseTestCase, ThreadsMixin, unittest.TestCase):

    # DRAGON ALLOWED_TYPES = ('processes', 'threads')

    @classmethod
    def _echo(cls, conn):
        for msg in iter(conn.recv_bytes, SENTINEL):
            conn.send_bytes(msg)
        conn.close()

    def test_connection(self):
        conn, child_conn = self.Pipe()

        p = self.Process(target=self._echo, args=(child_conn,))
        p.daemon = True
        p.start()

        seq = [1, 2.25, None]
        msg = latin("hello world")
        longmsg = msg * 10
        arr = array.array("i", list(range(4)))

        if self.TYPE == "processes":
            self.assertEqual(type(conn.fileno()), int)

        self.assertEqual(conn.send(seq), None)
        self.assertEqual(conn.recv(), seq)

        self.assertEqual(conn.send_bytes(msg), None)
        self.assertEqual(conn.recv_bytes(), msg)

        if self.TYPE == "processes":
            buffer = array.array("i", [0] * 10)
            expected = list(arr) + [0] * (10 - len(arr))
            self.assertEqual(conn.send_bytes(arr), None)
            self.assertEqual(conn.recv_bytes_into(buffer), len(arr) * buffer.itemsize)
            self.assertEqual(list(buffer), expected)

            buffer = array.array("i", [0] * 10)
            expected = [0] * 3 + list(arr) + [0] * (10 - 3 - len(arr))
            self.assertEqual(conn.send_bytes(arr), None)
            self.assertEqual(conn.recv_bytes_into(buffer, 3 * buffer.itemsize), len(arr) * buffer.itemsize)
            self.assertEqual(list(buffer), expected)

            buffer = bytearray(latin(" " * 40))
            self.assertEqual(conn.send_bytes(longmsg), None)
            try:
                res = conn.recv_bytes_into(buffer)
            except multiprocessing.BufferTooShort as e:
                self.assertEqual(e.args, (longmsg,))
            else:
                self.fail("expected BufferTooShort, got %s" % res)

        poll = TimingWrapper(conn.poll)

        self.assertEqual(poll(), False)
        self.assertTimingAlmostEqual(poll.elapsed, 0)

        self.assertEqual(poll(-1), False)
        self.assertTimingAlmostEqual(poll.elapsed, 0)

        self.assertEqual(poll(TIMEOUT1), False)
        self.assertTimingAlmostEqual(poll.elapsed, TIMEOUT1)

        conn.send(None)
        time.sleep(0.1)

        self.assertEqual(poll(TIMEOUT1), True)
        self.assertTimingAlmostEqual(poll.elapsed, 0)

        self.assertEqual(conn.recv(), None)

        really_big_msg = latin("X") * (1024 * 1024 * 16)  # 16Mb
        conn.send_bytes(really_big_msg)
        self.assertEqual(conn.recv_bytes(), really_big_msg)

        conn.send_bytes(SENTINEL)  # tell child to quit
        child_conn.close()

        if self.TYPE == "processes":
            self.assertEqual(conn.readable, True)
            self.assertEqual(conn.writable, True)
            self.assertRaises(EOFError, conn.recv)
            self.assertRaises(EOFError, conn.recv_bytes)

        p.join()

    def test_duplex_false(self):
        reader, writer = self.Pipe(duplex=False)
        self.assertEqual(writer.send(1), None)
        self.assertEqual(reader.recv(), 1)
        if self.TYPE == "processes":
            self.assertEqual(reader.readable, True)
            self.assertEqual(reader.writable, False)
            self.assertEqual(writer.readable, False)
            self.assertEqual(writer.writable, True)
            self.assertRaises(OSError, reader.send, 2)
            self.assertRaises(OSError, writer.recv)
            self.assertRaises(OSError, writer.poll)

    def test_spawn_close(self):
        # We test that a pipe connection can be closed by parent
        # process immediately after child is spawned.  On Windows this
        # would have sometimes failed on old versions because
        # child_conn would be closed before the child got a chance to
        # duplicate it.
        conn, child_conn = self.Pipe()

        p = self.Process(target=self._echo, args=(child_conn,))
        p.daemon = True
        p.start()
        child_conn.close()  # this might complete before child initializes

        msg = latin("hello")
        conn.send_bytes(msg)
        self.assertEqual(conn.recv_bytes(), msg)

        conn.send_bytes(SENTINEL)
        conn.close()
        p.join()

    def test_sendbytes(self):
        if self.TYPE != "processes":
            self.skipTest("test not appropriate for {}".format(self.TYPE))

        msg = latin("abcdefghijklmnopqrstuvwxyz")
        a, b = self.Pipe()

        a.send_bytes(msg)
        self.assertEqual(b.recv_bytes(), msg)

        a.send_bytes(msg, 5)
        self.assertEqual(b.recv_bytes(), msg[5:])

        a.send_bytes(msg, 7, 8)
        self.assertEqual(b.recv_bytes(), msg[7 : 7 + 8])

        a.send_bytes(msg, 26)
        self.assertEqual(b.recv_bytes(), latin(""))

        a.send_bytes(msg, 26, 0)
        self.assertEqual(b.recv_bytes(), latin(""))

        self.assertRaises(ValueError, a.send_bytes, msg, 27)

        self.assertRaises(ValueError, a.send_bytes, msg, 22, 5)

        self.assertRaises(ValueError, a.send_bytes, msg, 26, 1)

        self.assertRaises(ValueError, a.send_bytes, msg, -1)

        self.assertRaises(ValueError, a.send_bytes, msg, 4, -1)

    @classmethod
    def _is_fd_assigned(cls, fd):
        try:
            os.fstat(fd)
        except OSError as e:
            if e.errno == errno.EBADF:
                return False
            raise
        else:
            return True

    @classmethod
    def _writefd(cls, conn, data, create_dummy_fds=False):
        if create_dummy_fds:
            for i in range(0, 256):
                if not cls._is_fd_assigned(i):
                    os.dup2(conn.fileno(), i)
        fd = reduction.recv_handle(conn)
        if msvcrt:
            fd = msvcrt.open_osfhandle(fd, os.O_WRONLY)
        os.write(fd, data)
        os.close(fd)

    @unittest.skipUnless(HAS_REDUCTION, "test needs multiprocessing.reduction")
    def test_fd_transfer(self):
        if self.TYPE != "processes":
            self.skipTest("only makes sense with processes")
        conn, child_conn = self.Pipe(duplex=True)

        p = self.Process(target=self._writefd, args=(child_conn, b"foo"))
        p.daemon = True
        p.start()
        self.addCleanup(test.support.unlink, test.support.TESTFN)
        with open(test.support.TESTFN, "wb") as f:
            fd = f.fileno()
            if msvcrt:
                fd = msvcrt.get_osfhandle(fd)
            reduction.send_handle(conn, fd, p.pid)
        p.join()
        with open(test.support.TESTFN, "rb") as f:
            self.assertEqual(f.read(), b"foo")

    @unittest.skipUnless(HAS_REDUCTION, "test needs multiprocessing.reduction")
    @unittest.skipIf(sys.platform == "win32", "test semantics don't make sense on Windows")
    @unittest.skipIf(MAXFD <= 256, "largest assignable fd number is too small")
    @unittest.skipUnless(hasattr(os, "dup2"), "test needs os.dup2()")
    def test_large_fd_transfer(self):
        # With fd > 256 (issue #11657)
        if self.TYPE != "processes":
            self.skipTest("only makes sense with processes")
        conn, child_conn = self.Pipe(duplex=True)

        p = self.Process(target=self._writefd, args=(child_conn, b"bar", True))
        p.daemon = True
        p.start()
        self.addCleanup(test.support.unlink, test.support.TESTFN)
        with open(test.support.TESTFN, "wb") as f:
            fd = f.fileno()
            for newfd in range(256, MAXFD):
                if not self._is_fd_assigned(newfd):
                    break
            else:
                self.fail("could not find an unassigned large file descriptor")
            os.dup2(fd, newfd)
            try:
                reduction.send_handle(conn, newfd, p.pid)
            finally:
                os.close(newfd)
        p.join()
        with open(test.support.TESTFN, "rb") as f:
            self.assertEqual(f.read(), b"bar")

    @classmethod
    def _send_data_without_fd(self, conn):
        os.write(conn.fileno(), b"\0")

    @unittest.skipUnless(HAS_REDUCTION, "test needs multiprocessing.reduction")
    @unittest.skipIf(sys.platform == "win32", "doesn't make sense on Windows")
    def test_missing_fd_transfer(self):
        # Check that exception is raised when received data is not
        # accompanied by a file descriptor in ancillary data.
        if self.TYPE != "processes":
            self.skipTest("only makes sense with processes")
        conn, child_conn = self.Pipe(duplex=True)

        p = self.Process(target=self._send_data_without_fd, args=(child_conn,))
        p.daemon = True
        p.start()
        self.assertRaises(RuntimeError, reduction.recv_handle, conn)
        p.join()

    def test_context(self):
        a, b = self.Pipe()

        with a, b:
            a.send(1729)
            self.assertEqual(b.recv(), 1729)
            if self.TYPE == "processes":
                self.assertFalse(a.closed)
                self.assertFalse(b.closed)

        if self.TYPE == "processes":
            self.assertTrue(a.closed)
            self.assertTrue(b.closed)
            self.assertRaises(OSError, a.recv)
            self.assertRaises(OSError, b.recv)


# DRAGON
if __name__ == "__main__":
    setUpModule()
    unittest.main()
    tearDownModule()
