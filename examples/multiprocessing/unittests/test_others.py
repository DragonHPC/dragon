"""Various unittests not associated with a MixIn.
"""
import unittest
import unittest.mock
import os
import sys
import time
import random
import socket
import io
import subprocess
import signal
import warnings
import weakref
import errno
import gc

import queue as pyqueue

import unittest
import unittest.mock
import test.support
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
    join_process,
    setUpModule,
    tearDownModule,
    WIN32,
    PRELOAD,
)

if os.name == "posix":
    from multiprocessing import resource_tracker

    def _resource_unlink(name, rtype):
        resource_tracker._CLEANUP_FUNCS[rtype](name)


#
# Test to verify handle verification, see issue 3321
#


class TestInvalidHandle(unittest.TestCase):
    @unittest.skipIf(WIN32, "skipped on Windows")
    def test_invalid_handles(self):
        conn = multiprocessing.connection.Connection(44977608)
        # check that poll() doesn't crash
        try:
            conn.poll()
        except (ValueError, OSError):
            pass
        finally:
            # Hack private attribute _handle to avoid printing an error
            # in conn.__del__
            conn._handle = None
        self.assertRaises((ValueError, OSError), multiprocessing.connection.Connection, -1)


@hashlib_helper.requires_hashdigest("md5")
class OtherTest(unittest.TestCase):
    # TODO: add more tests for deliver/answer challenge.
    def test_deliver_challenge_auth_failure(self):
        class _FakeConnection(object):
            def recv_bytes(self, size):
                return b"something bogus"

            def send_bytes(self, data):
                pass

        self.assertRaises(
            multiprocessing.AuthenticationError,
            multiprocessing.connection.deliver_challenge,
            _FakeConnection(),
            b"abc",
        )

    def test_answer_challenge_auth_failure(self):
        class _FakeConnection(object):
            def __init__(self):
                self.count = 0

            def recv_bytes(self, size):
                self.count += 1
                if self.count == 1:
                    return multiprocessing.connection.CHALLENGE
                elif self.count == 2:
                    return b"something bogus"
                return b""

            def send_bytes(self, data):
                pass

        self.assertRaises(
            multiprocessing.AuthenticationError,
            multiprocessing.connection.answer_challenge,
            _FakeConnection(),
            b"abc",
        )


#
# Test Manager.start()/Pool.__init__() initializer feature - see issue 5585
#


def initializer(ns):
    ns.test += 1


@hashlib_helper.requires_hashdigest("md5")
@unittest.skip("CIRRUS-1473 DragonPool nor DragonPatchedPool pass this.")
class TestInitializers(unittest.TestCase):
    def setUp(self):
        self.mgr = multiprocessing.Manager()
        self.ns = self.mgr.Namespace()
        self.ns.test = 0

    def tearDown(self):
        self.mgr.shutdown()
        self.mgr.join()

    def test_manager_initializer(self):
        m = multiprocessing.managers.SyncManager()
        self.assertRaises(TypeError, m.start, 1)
        m.start(initializer, (self.ns,))
        self.assertEqual(self.ns.test, 1)
        m.shutdown()
        m.join()

    def test_pool_initializer(self):
        self.assertRaises(TypeError, multiprocessing.Pool, initializer=1)
        p = multiprocessing.Pool(1, initializer, (self.ns,))
        p.close()
        p.join()
        self.assertEqual(self.ns.test, 1)


#
# Issue 5155, 5313, 5331: Test process in processes
# Verifies os.close(sys.stdin.fileno) vs. sys.stdin.close() behavior
#


def _this_sub_process(q):
    try:
        item = q.get(block=False)
    except pyqueue.Empty:
        pass


def _test_process():
    queue = multiprocessing.Queue()
    subProc = multiprocessing.Process(target=_this_sub_process, args=(queue,))
    subProc.daemon = True
    subProc.start()
    subProc.join()


def _afunc(x):
    return x * x


def pool_in_process():
    pool = multiprocessing.Pool(processes=4)
    x = pool.map(_afunc, [1, 2, 3, 4, 5, 6, 7])
    pool.close()
    pool.join()


class _file_like(object):
    def __init__(self, delegate):
        self._delegate = delegate
        self._pid = None

    @property
    def cache(self):
        pid = os.getpid()
        # There are no race conditions since fork keeps only the running thread
        if pid != self._pid:
            self._pid = pid
            self._cache = []
        return self._cache

    def write(self, data):
        self.cache.append(data)

    def flush(self):
        self._delegate.write("".join(self.cache))
        self._cache = []


class TestStdinBadfiledescriptor(unittest.TestCase):
    def test_queue_in_process(self):
        proc = multiprocessing.Process(target=_test_process)
        proc.start()
        proc.join()

    @unittest.skip("bug filed CIRRUS-1473.")
    def test_pool_in_process(self):
        p = multiprocessing.Process(target=pool_in_process)
        p.start()
        p.join()

    def test_flushing(self):
        sio = io.StringIO()
        flike = _file_like(sio)
        flike.write("foo")
        proc = multiprocessing.Process(target=lambda: flike.flush())
        flike.flush()
        assert sio.getvalue() == "foo"


class TestWait(unittest.TestCase):
    @classmethod
    def _child_test_wait(cls, w, slow):
        # DRAGON os.getpid() is not applicable in dragon, as it doesn't generalize to multinode.
        p = multiprocessing.current_process()  # DRAGON
        for i in range(10):
            if slow:
                time.sleep(random.random() * 0.1)
            # w.send((i, os.getpid())) DRAGON
            w.send((i, p.pid))  # DRAGON
        w.close()

    def test_wait(self, slow=False):
        from multiprocessing.connection import wait

        readers = []
        procs = []
        messages = []

        for i in range(4):
            r, w = multiprocessing.Pipe(duplex=False)
            p = multiprocessing.Process(target=self._child_test_wait, args=(w, slow))
            p.daemon = True
            p.start()
            w.close()
            readers.append(r)
            procs.append(p)
            self.addCleanup(p.join)

        while readers:
            for r in wait(readers):
                try:
                    msg = r.recv()
                except EOFError:
                    readers.remove(r)
                    r.close()
                else:
                    messages.append(msg)

        messages.sort()
        expected = sorted((i, p.pid) for i in range(10) for p in procs)
        self.assertEqual(messages, expected)

    @classmethod
    def _child_test_wait_socket(cls, address, slow):
        s = socket.socket()
        s.connect(address)
        for i in range(10):
            if slow:
                time.sleep(random.random() * 0.1)
            s.sendall(("%s\n" % i).encode("ascii"))
        s.close()

    def test_wait_socket(self, slow=False):
        from multiprocessing.connection import wait

        l = socket.create_server((socket_helper.HOST, 0))
        addr = l.getsockname()
        readers = []
        procs = []
        dic = {}

        for i in range(4):
            p = multiprocessing.Process(target=self._child_test_wait_socket, args=(addr, slow))
            p.daemon = True
            p.start()
            procs.append(p)
            self.addCleanup(p.join)

        for i in range(4):
            r, _ = l.accept()
            readers.append(r)
            dic[r] = []
        l.close()

        while readers:
            for r in wait(readers):
                msg = r.recv(32)
                if not msg:
                    readers.remove(r)
                    r.close()
                else:
                    dic[r].append(msg)

        expected = "".join("%s\n" % i for i in range(10)).encode("ascii")
        for v in dic.values():
            self.assertEqual(b"".join(v), expected)

    def test_wait_slow(self):
        self.test_wait(True)

    def test_wait_socket_slow(self):
        self.test_wait_socket(True)

    def test_wait_timeout(self):
        from multiprocessing.connection import wait

        expected = 5
        a, b = multiprocessing.Pipe()

        start = time.monotonic()
        res = wait([a, b], expected)
        delta = time.monotonic() - start

        self.assertEqual(res, [])
        self.assertLess(delta, expected * 2)
        self.assertGreater(delta, expected * 0.5)

        b.send(None)

        start = time.monotonic()
        res = wait([a, b], 20)
        delta = time.monotonic() - start

        self.assertEqual(res, [a])
        self.assertLess(delta, 0.4)

    @classmethod
    def signal_and_sleep(cls, sem, period):
        sem.release()
        time.sleep(period)

    @unittest.skip("bug filed PE-40903")
    def test_wait_integer(self):
        from multiprocessing.connection import wait

        expected = 3
        sorted_ = lambda l: sorted(l, key=lambda x: id(x))
        sem = multiprocessing.Semaphore(0)
        a, b = multiprocessing.Pipe()
        p = multiprocessing.Process(target=self.signal_and_sleep, args=(sem, expected))

        p.start()
        self.assertIsInstance(p.sentinel, int)
        self.assertTrue(sem.acquire(timeout=20))

        start = time.monotonic()
        res = wait([a, p.sentinel, b], expected + 20)
        delta = time.monotonic() - start

        self.assertEqual(res, [p.sentinel])
        self.assertLess(delta, expected + 2)
        self.assertGreater(delta, expected - 2)

        a.send(None)

        start = time.monotonic()
        res = wait([a, p.sentinel, b], 20)
        delta = time.monotonic() - start

        self.assertEqual(sorted_(res), sorted_([p.sentinel, b]))
        self.assertLess(delta, 0.4)

        b.send(None)

        start = time.monotonic()
        res = wait([a, p.sentinel, b], 20)
        delta = time.monotonic() - start

        self.assertEqual(sorted_(res), sorted_([a, p.sentinel, b]))
        self.assertLess(delta, 0.4)

        p.terminate()
        p.join()

    def test_neg_timeout(self):
        from multiprocessing.connection import wait

        a, b = multiprocessing.Pipe()
        t = time.monotonic()
        res = wait([a], timeout=-1)
        t = time.monotonic() - t
        self.assertEqual(res, [])
        self.assertLess(t, 1)
        a.close()
        b.close()


#
# Issue 14151: Test invalid family on invalid environment
#

@unittest.skip(f"DRAGON: Not Implemented")
class TestInvalidFamily(unittest.TestCase):
    @unittest.skipIf(WIN32, "skipped on Windows")
    def test_invalid_family(self):
        with self.assertRaises(ValueError):
            multiprocessing.connection.Listener(r"\\.\test")

    @unittest.skipUnless(WIN32, "skipped on non-Windows platforms")
    def test_invalid_family_win32(self):
        with self.assertRaises(ValueError):
            multiprocessing.connection.Listener("/var/test.pipe")


#
# Issue 12098: check sys.flags of child matches that for parent
#


class TestFlags(unittest.TestCase):
    @classmethod
    def run_in_grandchild(cls, conn):
        conn.send(tuple(sys.flags))

    @classmethod
    def run_in_child(cls):
        import json

        r, w = multiprocessing.Pipe(duplex=False)
        p = multiprocessing.Process(target=cls.run_in_grandchild, args=(w,))
        p.start()
        grandchild_flags = r.recv()
        p.join()
        r.close()
        w.close()
        flags = (tuple(sys.flags), grandchild_flags)
        print(json.dumps(flags))

    def test_flags(self):
        import json

        # start child process using unusual flags
        prog = "from test._test_multiprocessing import TestFlags; " + "TestFlags.run_in_child()"
        data = subprocess.check_output([sys.executable, "-E", "-S", "-O", "-c", prog])
        child_flags, grandchild_flags = json.loads(data.decode("ascii"))
        self.assertEqual(child_flags, grandchild_flags)


#
# Test interaction with socket timeouts - see Issue #6056
#

@unittest.skip(f"Dragon: Not Implemented")
class TestTimeouts(unittest.TestCase):
    @classmethod
    def _test_timeout(cls, child, address):
        time.sleep(1)
        child.send(123)
        child.close()
        conn = multiprocessing.connection.Client(address)
        conn.send(456)
        conn.close()

    def test_timeout(self):
        old_timeout = socket.getdefaulttimeout()
        try:
            socket.setdefaulttimeout(0.1)
            parent, child = multiprocessing.Pipe(duplex=True)
            l = multiprocessing.connection.Listener(family="AF_INET")
            p = multiprocessing.Process(target=self._test_timeout, args=(child, l.address))
            p.start()
            child.close()
            self.assertEqual(parent.recv(), 123)
            parent.close()
            conn = l.accept()
            self.assertEqual(conn.recv(), 456)
            conn.close()
            l.close()
            join_process(p)
        finally:
            socket.setdefaulttimeout(old_timeout)


#
# Test what happens with no "if __name__ == '__main__'"
#


class TestNoForkBomb(unittest.TestCase):
    @unittest.skip("bug filed PE-40903")
    def test_noforkbomb(self):
        sm = multiprocessing.get_start_method()
        name = os.path.join(os.path.dirname(__file__), "mp_fork_bomb.py")
        if sm != "fork":
            rc, out, err = test.support.script_helper.assert_python_failure(name, sm)
            self.assertEqual(out, b"")
            self.assertIn(b"RuntimeError", err)
        else:
            rc, out, err = test.support.script_helper.assert_python_ok(name, sm)
            self.assertEqual(out.rstrip(), b"123")
            self.assertEqual(err, b"")


#
# Issue #17555: ForkAwareThreadLock
#


class TestForkAwareThreadLock(unittest.TestCase):
    # We recursively start processes.  Issue #17555 meant that the
    # after fork registry would get duplicate entries for the same
    # lock.  The size of the registry at generation n was ~2**n.

    @classmethod
    def child(cls, n, conn):
        if n > 1:
            p = multiprocessing.Process(target=cls.child, args=(n - 1, conn))
            p.start()
            conn.close()
            join_process(p)
        else:
            conn.send(len(util._afterfork_registry))
        conn.close()

    def test_lock(self):
        r, w = multiprocessing.Pipe(False)
        l = util.ForkAwareThreadLock()
        old_size = len(util._afterfork_registry)
        p = multiprocessing.Process(target=self.child, args=(5, w))
        p.start()
        w.close()
        new_size = r.recv()
        join_process(p)
        self.assertLessEqual(new_size, old_size)


#
# Check that non-forked child processes do not inherit unneeded fds/handles
#


class TestCloseFds(unittest.TestCase):
    def get_high_socket_fd(self):
        if WIN32:
            # The child process will not have any socket handles, so
            # calling socket.fromfd() should produce WSAENOTSOCK even
            # if there is a handle of the same number.
            return socket.socket().detach()
        else:
            # We want to produce a socket with an fd high enough that a
            # freshly created child process will not have any fds as high.
            fd = socket.socket().detach()
            to_close = []
            while fd < 50:
                to_close.append(fd)
                fd = os.dup(fd)
            for x in to_close:
                os.close(x)
            return fd

    def close(self, fd):
        if WIN32:
            socket.socket(socket.AF_INET, socket.SOCK_STREAM, fileno=fd).close()
        else:
            os.close(fd)

    @classmethod
    def _test_closefds(cls, conn, fd):
        try:
            s = socket.fromfd(fd, socket.AF_INET, socket.SOCK_STREAM)
        except Exception as e:
            conn.send(e)
        else:
            s.close()
            conn.send(None)

    def test_closefd(self):
        if not HAS_REDUCTION:
            raise unittest.SkipTest("requires fd pickling")

        reader, writer = multiprocessing.Pipe()
        fd = self.get_high_socket_fd()
        try:
            p = multiprocessing.Process(target=self._test_closefds, args=(writer, fd))
            p.start()
            writer.close()
            e = reader.recv()
            join_process(p)
        finally:
            self.close(fd)
            writer.close()
            reader.close()

        if multiprocessing.get_start_method() == "fork":
            self.assertIs(e, None)
        else:
            WSAENOTSOCK = 10038
            self.assertIsInstance(e, OSError)
            self.assertTrue(e.errno == errno.EBADF or e.winerror == WSAENOTSOCK, e)


#
# Issue #17097: EINTR should be ignored by recv(), send(), accept() etc
#


class TestIgnoreEINTR(unittest.TestCase):

    # Sending CONN_MAX_SIZE bytes into a multiprocessing pipe must block
    CONN_MAX_SIZE = max(test.support.PIPE_MAX_SIZE, test.support.SOCK_MAX_SIZE)

    @classmethod
    def _test_ignore(cls, conn):
        def handler(signum, frame):
            pass

        signal.signal(signal.SIGUSR1, handler)
        conn.send("ready")
        x = conn.recv()
        conn.send(x)
        conn.send_bytes(b"x" * cls.CONN_MAX_SIZE)

    @unittest.skip("DRAGON: not implemented")
    @unittest.skipUnless(hasattr(signal, "SIGUSR1"), "requires SIGUSR1")
    def test_ignore(self):
        conn, child_conn = multiprocessing.Pipe()
        try:
            p = multiprocessing.Process(target=self._test_ignore, args=(child_conn,))
            p.daemon = True
            p.start()
            child_conn.close()
            self.assertEqual(conn.recv(), "ready")
            time.sleep(0.1)
            os.kill(p.pid, signal.SIGUSR1)
            time.sleep(0.1)
            conn.send(1234)
            self.assertEqual(conn.recv(), 1234)
            time.sleep(0.1)
            os.kill(p.pid, signal.SIGUSR1)
            self.assertEqual(conn.recv_bytes(), b"x" * self.CONN_MAX_SIZE)
            time.sleep(0.1)
            p.join()
        finally:
            conn.close()

    @classmethod
    def _test_ignore_listener(cls, conn):
        def handler(signum, frame):
            pass

        signal.signal(signal.SIGUSR1, handler)
        with multiprocessing.connection.Listener() as l:
            conn.send(l.address)
            a = l.accept()
            a.send("welcome")

    @unittest.skip("bug filed PE-40903")
    @unittest.skipUnless(hasattr(signal, "SIGUSR1"), "requires SIGUSR1")
    def test_ignore_listener(self):
        conn, child_conn = multiprocessing.Pipe()
        try:
            p = multiprocessing.Process(target=self._test_ignore_listener, args=(child_conn,))
            p.daemon = True
            p.start()
            child_conn.close()
            address = conn.recv()
            time.sleep(0.1)
            os.kill(p.pid, signal.SIGUSR1)
            time.sleep(0.1)
            client = multiprocessing.connection.Client(address)
            self.assertEqual(client.recv(), "welcome")
            p.join()
        finally:
            conn.close()


class TestStartMethod(unittest.TestCase):
    @classmethod
    def _check_context(cls, conn):
        conn.send(multiprocessing.get_start_method())

    def check_context(self, ctx):
        r, w = ctx.Pipe(duplex=False)
        p = ctx.Process(target=self._check_context, args=(w,))
        p.start()
        w.close()
        child_method = r.recv()
        r.close()
        p.join()
        self.assertEqual(child_method, ctx.get_start_method())

    @unittest.skip("bug filed PE-40903")
    def test_context(self):
        for method in ("fork", "spawn", "forkserver"):
            try:
                ctx = multiprocessing.get_context(method)
            except ValueError:
                continue
            self.assertEqual(ctx.get_start_method(), method)
            self.assertIs(ctx.get_context(), ctx)
            self.assertRaises(ValueError, ctx.set_start_method, "spawn")
            self.assertRaises(ValueError, ctx.set_start_method, None)
            self.check_context(ctx)

    @unittest.skip("bug filed PE-40903")
    def test_set_get(self):
        multiprocessing.set_forkserver_preload(PRELOAD)
        count = 0
        old_method = multiprocessing.get_start_method()
        try:
            for method in ("fork", "spawn", "forkserver"):
                try:
                    multiprocessing.set_start_method(method, force=True)
                except ValueError:
                    continue
                self.assertEqual(multiprocessing.get_start_method(), method)
                ctx = multiprocessing.get_context()
                self.assertEqual(ctx.get_start_method(), method)
                self.assertTrue(type(ctx).__name__.lower().startswith(method))
                self.assertTrue(ctx.Process.__name__.lower().startswith(method))
                self.check_context(multiprocessing)
                count += 1
        finally:
            multiprocessing.set_start_method(old_method, force=True)
        self.assertGreaterEqual(count, 1)

    def test_get_all(self):
        methods = multiprocessing.get_all_start_methods()
        if sys.platform == "win32":
            self.assertEqual(methods, ["spawn"])
        else:
            # DRAGON Need to modify this, because we have another start method
            self.assertTrue("dragon" in methods)  # DRAGON
            methods.remove("dragon")  # DRAGON
            self.assertTrue(
                methods == ["fork", "spawn"]
                or methods == ["spawn", "fork"]
                or methods == ["fork", "spawn", "forkserver"]
                or methods == ["spawn", "fork", "forkserver"]
            )

    def test_preload_resources(self):
        if multiprocessing.get_start_method() != "forkserver":
            self.skipTest("test only relevant for 'forkserver' method")
        name = os.path.join(os.path.dirname(__file__), "mp_preload.py")
        rc, out, err = test.support.script_helper.assert_python_ok(name)
        out = out.decode()
        err = err.decode()
        if out.rstrip() != "ok" or err != "":
            print(out)
            print(err)
            self.fail("failed spawning forkserver or grandchild")

@unittest.skip(f"DRAGON: Not Implemented PE-45417")
@unittest.skipIf(sys.platform == "win32", "test semantics don't make sense on Windows")
class TestResourceTracker(unittest.TestCase):
    def test_resource_tracker(self):
        #
        # Check that killing process does not leak named semaphores
        #
        cmd = """if 1:
            import time, os, tempfile
            import multiprocessing as mp
            from multiprocessing import resource_tracker
            from multiprocessing.shared_memory import SharedMemory

            mp.set_start_method("spawn")
            rand = tempfile._RandomNameSequence()


            def create_and_register_resource(rtype):
                if rtype == "semaphore":
                    lock = mp.Lock()
                    return lock, lock._semlock.name
                elif rtype == "shared_memory":
                    sm = SharedMemory(create=True, size=10)
                    return sm, sm._name
                else:
                    raise ValueError(
                        "Resource type {{}} not understood".format(rtype))


            resource1, rname1 = create_and_register_resource("{rtype}")
            resource2, rname2 = create_and_register_resource("{rtype}")

            os.write({w}, rname1.encode("ascii") + b"\\n")
            os.write({w}, rname2.encode("ascii") + b"\\n")

            time.sleep(10)
        """
        for rtype in resource_tracker._CLEANUP_FUNCS:
            with self.subTest(rtype=rtype):
                if rtype == "noop":
                    # Artefact resource type used by the resource_tracker
                    continue
                r, w = os.pipe()
                p = subprocess.Popen(
                    [sys.executable, "-E", "-c", cmd.format(w=w, rtype=rtype)],
                    pass_fds=[w],
                    stderr=subprocess.PIPE,
                )
                os.close(w)
                with open(r, "rb", closefd=True) as f:
                    name1 = f.readline().rstrip().decode("ascii")
                    name2 = f.readline().rstrip().decode("ascii")
                _resource_unlink(name1, rtype)
                p.terminate()
                p.wait()

                deadline = time.monotonic() + test.support.LONG_TIMEOUT
                while time.monotonic() < deadline:
                    time.sleep(0.5)
                    try:
                        _resource_unlink(name2, rtype)
                    except OSError as e:
                        # docs say it should be ENOENT, but OSX seems to give
                        # EINVAL
                        self.assertIn(e.errno, (errno.ENOENT, errno.EINVAL))
                        break
                else:
                    raise AssertionError(
                        f"A {rtype} resource was leaked after a process was " f"abruptly terminated."
                    )
                err = p.stderr.read().decode("utf-8")
                p.stderr.close()
                expected = "resource_tracker: There appear to be 2 leaked {} " "objects".format(rtype)
                self.assertRegex(err, expected)
                self.assertRegex(err, r"resource_tracker: %r: \[Errno" % name1)

    def check_resource_tracker_death(self, signum, should_die):
        # bpo-31310: if the semaphore tracker process has died, it should
        # be restarted implicitly.
        from multiprocessing.resource_tracker import _resource_tracker

        pid = _resource_tracker._pid
        if pid is not None:
            os.kill(pid, signal.SIGKILL)
            test.support.wait_process(pid, exitcode=-signal.SIGKILL)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            _resource_tracker.ensure_running()
        pid = _resource_tracker._pid

        os.kill(pid, signum)
        time.sleep(1.0)  # give it time to die

        ctx = multiprocessing.get_context("spawn")
        with warnings.catch_warnings(record=True) as all_warn:
            warnings.simplefilter("always")
            sem = ctx.Semaphore()
            sem.acquire()
            sem.release()
            wr = weakref.ref(sem)
            # ensure `sem` gets collected, which triggers communication with
            # the semaphore tracker
            del sem
            gc.collect()
            self.assertIsNone(wr())
            if should_die:
                self.assertEqual(len(all_warn), 1)
                the_warn = all_warn[0]
                self.assertTrue(issubclass(the_warn.category, UserWarning))
                self.assertTrue("resource_tracker: process died" in str(the_warn.message))
            else:
                self.assertEqual(len(all_warn), 0)

    def test_resource_tracker_sigint(self):
        # Catchable signal (ignored by semaphore tracker)
        self.check_resource_tracker_death(signal.SIGINT, False)

    def test_resource_tracker_sigterm(self):
        # Catchable signal (ignored by semaphore tracker)
        self.check_resource_tracker_death(signal.SIGTERM, False)

    def test_resource_tracker_sigkill(self):
        # Uncatchable signal.
        self.check_resource_tracker_death(signal.SIGKILL, True)

    @staticmethod
    def _is_resource_tracker_reused(conn, pid):
        from multiprocessing.resource_tracker import _resource_tracker

        _resource_tracker.ensure_running()
        # The pid should be None in the child process, expect for the fork
        # context. It should not be a new value.
        reused = _resource_tracker._pid in (None, pid)
        reused &= _resource_tracker._check_alive()
        conn.send(reused)

    @unittest.skip("bug filed PE-40903")
    def test_resource_tracker_reused(self):
        from multiprocessing.resource_tracker import _resource_tracker

        _resource_tracker.ensure_running()
        pid = _resource_tracker._pid

        r, w = multiprocessing.Pipe(duplex=False)
        p = multiprocessing.Process(target=self._is_resource_tracker_reused, args=(w, pid))
        p.start()
        is_resource_tracker_reused = r.recv()

        # Clean up
        p.join()
        w.close()
        r.close()

        self.assertTrue(is_resource_tracker_reused)


class TestSimpleQueue(unittest.TestCase):
    @classmethod
    def _test_empty(cls, queue, child_can_start, parent_can_continue):
        child_can_start.wait()
        # issue 30301, could fail under spawn and forkserver
        try:
            queue.put(queue.empty())
            queue.put(queue.empty())
        finally:
            parent_can_continue.set()

    def test_empty(self):
        queue = multiprocessing.SimpleQueue()
        child_can_start = multiprocessing.Event()
        parent_can_continue = multiprocessing.Event()

        proc = multiprocessing.Process(
            target=self._test_empty, args=(queue, child_can_start, parent_can_continue)
        )
        proc.daemon = True
        proc.start()

        self.assertTrue(queue.empty())

        child_can_start.set()
        parent_can_continue.wait()

        self.assertFalse(queue.empty())
        self.assertEqual(queue.get(), True)
        self.assertEqual(queue.get(), False)
        self.assertTrue(queue.empty())

        proc.join()

    def test_close(self):
        queue = multiprocessing.SimpleQueue()
        queue.close()
        # closing a queue twice should not fail
        queue.close()

    # Test specific to CPython since it tests private attributes
    @test.support.cpython_only
    def test_closed(self):
        queue = multiprocessing.SimpleQueue()
        queue.close()
        self.assertTrue(queue._reader.closed)
        self.assertTrue(queue._writer.closed)


class TestPoolNotLeakOnFailure(unittest.TestCase):
    @unittest.skip("CIRRUS-1473 DRAGON: Fails with native pool.")
    def test_release_unused_processes(self):
        # Issue #19675: During pool creation, if we can't create a process,
        # don't leak already created ones.
        will_fail_in = 3
        forked_processes = []

        class FailingForkProcess:
            def __init__(self, **kwargs):
                self.name = "Fake Process"
                self.exitcode = None
                self.state = None
                forked_processes.append(self)

            def start(self):
                nonlocal will_fail_in
                if will_fail_in <= 0:
                    raise OSError("Manually induced OSError")
                will_fail_in -= 1
                self.state = "started"

            def terminate(self):
                self.state = "stopping"

            def join(self):
                if self.state == "stopping":
                    self.state = "stopped"

            def is_alive(self):
                return self.state == "started" or self.state == "stopping"

        with self.assertRaisesRegex(OSError, "Manually induced OSError"):
            p = multiprocessing.pool.Pool(5, context=unittest.mock.MagicMock(Process=FailingForkProcess))
            p.close()
            p.join()
        self.assertFalse(any(process.is_alive() for process in forked_processes))

    @unittest.skip("DRAGON: doesn't have managers yet.")
    @hashlib_helper.requires_hashdigest("md5")
    class TestSyncManagerTypes(unittest.TestCase):
        """Test all the types which can be shared between a parent and a
        child process by using a manager which acts as an intermediary
        between them.

        In the following unit-tests the base type is created in the parent
        process, the @classmethod represents the worker process and the
        shared object is readable and editable between the two.

        # The child.
        @classmethod
        def _test_list(cls, obj):
            assert obj[0] == 5
            assert obj.append(6)

        # The parent.
        def test_list(self):
            o = self.manager.list()
            o.append(5)
            self.run_worker(self._test_list, o)
            assert o[1] == 6
        """

        # DRAGON: commented out so file works. Comment back in, when we have managers
        # manager_class = multiprocessing.managers.SyncManager

        def setUp(self):
            self.manager = self.manager_class()
            self.manager.start()
            self.proc = None

        def tearDown(self):
            if self.proc is not None and self.proc.is_alive():
                self.proc.terminate()
                self.proc.join()
            self.manager.shutdown()
            self.manager = None
            self.proc = None

        @classmethod
        def setUpClass(cls):
            test.support.reap_children()

        tearDownClass = setUpClass

        def wait_proc_exit(self):
            # Only the manager process should be returned by active_children()
            # but this can take a bit on slow machines, so wait a few seconds
            # if there are other children too (see #17395).
            join_process(self.proc)
            start_time = time.monotonic()
            t = 0.01
            while len(multiprocessing.active_children()) > 1:
                time.sleep(t)
                t *= 2
                dt = time.monotonic() - start_time
                if dt >= 5.0:
                    test.support.environment_altered = True
                    test.support.print_warning(
                        f"multiprocessing.Manager still has "
                        f"{multiprocessing.active_children()} "
                        f"active children after {dt} seconds"
                    )
                    break

        def run_worker(self, worker, obj):
            self.proc = multiprocessing.Process(target=worker, args=(obj,))
            self.proc.daemon = True
            self.proc.start()
            self.wait_proc_exit()
            self.assertEqual(self.proc.exitcode, 0)

        @classmethod
        def _test_event(cls, obj):
            assert obj.is_set()
            obj.wait()
            obj.clear()
            obj.wait(0.001)

        def test_event(self):
            o = self.manager.Event()
            o.set()
            self.run_worker(self._test_event, o)
            assert not o.is_set()
            o.wait(0.001)

        @classmethod
        def _test_lock(cls, obj):
            obj.acquire()

        def test_lock(self, lname="Lock"):
            o = getattr(self.manager, lname)()
            self.run_worker(self._test_lock, o)
            o.release()
            self.assertRaises(RuntimeError, o.release)  # already released

        @classmethod
        def _test_rlock(cls, obj):
            obj.acquire()
            obj.release()

        def test_rlock(self, lname="Lock"):
            o = getattr(self.manager, lname)()
            self.run_worker(self._test_rlock, o)

        @classmethod
        def _test_semaphore(cls, obj):
            obj.acquire()

        def test_semaphore(self, sname="Semaphore"):
            o = getattr(self.manager, sname)()
            self.run_worker(self._test_semaphore, o)
            o.release()

        def test_bounded_semaphore(self):
            self.test_semaphore(sname="BoundedSemaphore")

        @classmethod
        def _test_condition(cls, obj):
            obj.acquire()
            obj.release()

        def test_condition(self):
            o = self.manager.Condition()
            self.run_worker(self._test_condition, o)

        @classmethod
        def _test_barrier(cls, obj):
            assert obj.parties == 5
            obj.reset()

        def test_barrier(self):
            o = self.manager.Barrier(5)
            self.run_worker(self._test_barrier, o)

        @classmethod
        def _test_pool(cls, obj):
            # TODO: fix https://bugs.python.org/issue35919
            with obj:
                pass

        def test_pool(self):
            o = self.manager.Pool(processes=4)
            self.run_worker(self._test_pool, o)

        @classmethod
        def _test_queue(cls, obj):
            assert obj.qsize() == 2
            assert obj.full()
            assert not obj.empty()
            assert obj.get() == 5
            assert not obj.empty()
            assert obj.get() == 6
            assert obj.empty()

        def test_queue(self, qname="Queue"):
            o = getattr(self.manager, qname)(2)
            o.put(5)
            o.put(6)
            self.run_worker(self._test_queue, o)
            assert o.empty()
            assert not o.full()

        def test_joinable_queue(self):
            self.test_queue("JoinableQueue")

        @classmethod
        def _test_list(cls, obj):
            assert obj[0] == 5
            assert obj.count(5) == 1
            assert obj.index(5) == 0
            obj.sort()
            obj.reverse()
            for x in obj:
                pass
            assert len(obj) == 1
            assert obj.pop(0) == 5

        def test_list(self):
            o = self.manager.list()
            o.append(5)
            self.run_worker(self._test_list, o)
            assert not o
            self.assertEqual(len(o), 0)

        @classmethod
        def _test_dict(cls, obj):
            assert len(obj) == 1
            assert obj["foo"] == 5
            assert obj.get("foo") == 5
            assert list(obj.items()) == [("foo", 5)]
            assert list(obj.keys()) == ["foo"]
            assert list(obj.values()) == [5]
            assert obj.copy() == {"foo": 5}
            assert obj.popitem() == ("foo", 5)

        def test_dict(self):
            o = self.manager.dict()
            o["foo"] = 5
            self.run_worker(self._test_dict, o)
            assert not o
            self.assertEqual(len(o), 0)

        @classmethod
        def _test_value(cls, obj):
            assert obj.value == 1
            assert obj.get() == 1
            obj.set(2)

        def test_value(self):
            o = self.manager.Value("i", 1)
            self.run_worker(self._test_value, o)
            self.assertEqual(o.value, 2)
            self.assertEqual(o.get(), 2)

        @classmethod
        def _test_array(cls, obj):
            assert obj[0] == 0
            assert obj[1] == 1
            assert len(obj) == 2
            assert list(obj) == [0, 1]

        def test_array(self):
            o = self.manager.Array("i", [0, 1])
            self.run_worker(self._test_array, o)

        @classmethod
        def _test_namespace(cls, obj):
            assert obj.x == 0
            assert obj.y == 1

        def test_namespace(self):
            o = self.manager.Namespace()
            o.x = 0
            o.y = 1
            self.run_worker(self._test_namespace, o)


class MiscTestCase(unittest.TestCase):
    def test__all__(self):
        # Just make sure names in blacklist are excluded
        try: 
            test.support.check__all__(
                self, multiprocessing, extra=multiprocessing.__all__, not_exported=["SUBDEBUG", "SUBWARNING"]
            )
        except TypeError:
            #kwargs prior to Python 3.10
            test.support.check__all__(
                self, multiprocessing, extra=multiprocessing.__all__, blacklist=["SUBDEBUG", "SUBWARNING"]
            )


#

# DRAGON
if __name__ == "__main__":
    setUpModule()
    unittest.main()
    tearDownModule()
