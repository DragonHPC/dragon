"""Test multiprocessing.Process()
"""
import os
import sys
import time
import weakref
import signal
import io
import gc

import unittest
import test.support

try:
    from test.support.os_helper import fd_count
    from test.support.os_helper import TESTFN
    from test.support.os_helper import unlink
except ImportError:
    #location prior to Python 3.10
    from test.support import fd_count
    from test.support import TESTFN
    from test.support import unlink


import threading

import dragon  # DRAGON import before multiprocessing

import multiprocessing

from common import (
    BaseTestCase,
    ProcessesMixin,
    ThreadsMixin,
    TimingWrapper,
    DELTA,
    join_process,
    close_queue,
    wait_for_handle,
    setUpModule,
    tearDownModule,
)

#
#
#


class DummyCallable:
    """Just a Dummy"""

    def __call__(self, q, c):
        assert isinstance(c, DummyCallable)
        q.put(5)


class WithProcessesTestProcess(BaseTestCase, ProcessesMixin, unittest.TestCase):
    """Test multiprocessing.Process()"""

    # DRAGON ALLOWED_TYPES = ('processes', 'threads')

    def test_current(self):
        if self.TYPE == "threads":
            self.skipTest("test not appropriate for {}".format(self.TYPE))

        current = self.current_process()
        authkey = current.authkey

        self.assertTrue(current.is_alive())
        self.assertTrue(not current.daemon)
        self.assertIsInstance(authkey, bytes)
        self.assertTrue(len(authkey) > 0)
        self.assertEqual(current.ident, os.getpid())
        self.assertEqual(current.exitcode, None)

    def test_daemon_argument(self):
        if self.TYPE == "threads":
            self.skipTest("test not appropriate for {}".format(self.TYPE))

        # By default uses the current process's daemon flag.
        proc0 = self.Process(target=self._test)
        self.assertEqual(proc0.daemon, self.current_process().daemon)
        proc1 = self.Process(target=self._test, daemon=True)
        self.assertTrue(proc1.daemon)
        proc2 = self.Process(target=self._test, daemon=False)
        self.assertFalse(proc2.daemon)

    @classmethod
    def _test(cls, q, *args, **kwds):
        current = cls.current_process()
        q.put(args)
        q.put(kwds)
        q.put(current.name)
        if cls.TYPE != "threads":
            q.put(bytes(current.authkey))
            q.put(current.pid)

    def test_parent_process_attributes(self):
        if self.TYPE == "threads":
            self.skipTest("test not appropriate for {}".format(self.TYPE))

        self.assertIsNone(self.parent_process())

        rconn, wconn = self.Pipe(duplex=False)
        p = self.Process(target=self._test_send_parent_process, args=(wconn,))
        p.start()
        p.join()
        parent_pid, parent_name = rconn.recv()
        self.assertEqual(parent_pid, self.current_process().pid)
        self.assertEqual(parent_pid, os.getpid())
        self.assertEqual(parent_name, self.current_process().name)

    @classmethod
    def _test_send_parent_process(cls, wconn):
        from multiprocessing.process import parent_process

        wconn.send([parent_process().pid, parent_process().name])

    def test_parent_process(self):
        if self.TYPE == "threads":
            self.skipTest("test not appropriate for {}".format(self.TYPE))

        # Launch a child process. Make it launch a grandchild process. Kill the
        # child process and make sure that the grandchild notices the death of
        # its parent (a.k.a the child process).
        rconn, wconn = self.Pipe(duplex=False)
        p = self.Process(target=self._test_create_grandchild_process, args=(wconn,))
        p.start()

        if not rconn.poll(timeout=test.support.LONG_TIMEOUT):
            raise AssertionError("Could not communicate with child process")
        parent_process_status = rconn.recv()
        self.assertEqual(parent_process_status, "alive")

        p.terminate()
        p.join()

        if not rconn.poll(timeout=test.support.LONG_TIMEOUT):
            raise AssertionError("Could not communicate with child process")
        parent_process_status = rconn.recv()
        self.assertEqual(parent_process_status, "not alive")

    @classmethod
    def _test_create_grandchild_process(cls, wconn):
        p = cls.Process(target=cls._test_report_parent_status, args=(wconn,))
        p.start()
        time.sleep(300)

    @classmethod
    def _test_report_parent_status(cls, wconn):
        from multiprocessing.process import parent_process

        wconn.send("alive" if parent_process().is_alive() else "not alive")
        parent_process().join(timeout=test.support.SHORT_TIMEOUT)
        wconn.send("alive" if parent_process().is_alive() else "not alive")

    def test_process(self):
        q = self.Queue(1)
        e = self.Event()
        args = (q, 1, 2)
        kwargs = {"hello": 23, "bye": 2.54}
        name = "SomeProcess"
        p = self.Process(target=self._test, args=args, kwargs=kwargs, name=name)
        p.daemon = True
        current = self.current_process()

        if self.TYPE != "threads":
            self.assertEqual(p.authkey, current.authkey)
        self.assertEqual(p.is_alive(), False)
        self.assertEqual(p.daemon, True)
        self.assertNotIn(p, self.active_children())
        self.assertTrue(type(self.active_children()) is list)
        self.assertEqual(p.exitcode, None)

        p.start()

        self.assertEqual(p.exitcode, None)
        self.assertEqual(p.is_alive(), True)
        self.assertIn(p, self.active_children())

        self.assertEqual(q.get(), args[1:])
        self.assertEqual(q.get(), kwargs)
        self.assertEqual(q.get(), p.name)
        if self.TYPE != "threads":
            self.assertEqual(q.get(), current.authkey)
            self.assertEqual(q.get(), p.pid)

        p.join()

        self.assertEqual(p.exitcode, 0)
        self.assertEqual(p.is_alive(), False)
        self.assertNotIn(p, self.active_children())
        close_queue(q)

    @unittest.skipUnless(threading._HAVE_THREAD_NATIVE_ID, "needs native_id")
    def test_process_mainthread_native_id(self):
        if self.TYPE == "threads":
            self.skipTest("test not appropriate for {}".format(self.TYPE))

        current_mainthread_native_id = threading.main_thread().native_id

        q = self.Queue(1)
        p = self.Process(target=self._test_process_mainthread_native_id, args=(q,))
        p.start()

        child_mainthread_native_id = q.get()
        p.join()
        close_queue(q)

        self.assertNotEqual(current_mainthread_native_id, child_mainthread_native_id)

    @classmethod
    def _test_process_mainthread_native_id(cls, q):
        mainthread_native_id = threading.main_thread().native_id
        q.put(mainthread_native_id)

    @classmethod
    def _sleep_some(cls):
        time.sleep(100)

    @classmethod
    def _test_sleep(cls, delay):
        time.sleep(delay)

    def _kill_process(self, meth):
        if self.TYPE == "threads":
            self.skipTest("test not appropriate for {}".format(self.TYPE))

        p = self.Process(target=self._sleep_some)
        p.daemon = True
        p.start()

        self.assertEqual(p.is_alive(), True)
        self.assertIn(p, self.active_children())
        self.assertEqual(p.exitcode, None)

        join = TimingWrapper(p.join)

        self.assertEqual(join(0), None)
        self.assertTimingAlmostEqual(join.elapsed, 0.0)
        self.assertEqual(p.is_alive(), True)

        self.assertEqual(join(-1), None)
        self.assertTimingAlmostEqual(join.elapsed, 0.0)
        self.assertEqual(p.is_alive(), True)

        # XXX maybe terminating too soon causes the problems on Gentoo...
        time.sleep(1)

        meth(p)

        if hasattr(signal, "alarm"):
            # On the Gentoo buildbot waitpid() often seems to block forever.
            # We use alarm() to interrupt it if it blocks for too long.
            def handler(*args):
                raise RuntimeError("join took too long: %s" % p)

            old_handler = signal.signal(signal.SIGALRM, handler)
            try:
                signal.alarm(10)
                self.assertEqual(join(), None)
            finally:
                signal.alarm(0)
                signal.signal(signal.SIGALRM, old_handler)
        else:
            self.assertEqual(join(), None)

        self.assertTimingAlmostEqual(join.elapsed, 0.0)

        self.assertEqual(p.is_alive(), False)
        self.assertNotIn(p, self.active_children())

        p.join()

        return p.exitcode

    def test_terminate(self):
        exitcode = self._kill_process(multiprocessing.Process.terminate)
        if os.name != "nt":
            self.assertEqual(exitcode, -signal.SIGTERM)

    def test_kill(self):
        exitcode = self._kill_process(multiprocessing.Process.kill)
        if os.name != "nt":
            self.assertEqual(exitcode, -signal.SIGKILL)

    def test_cpu_count(self):
        try:
            cpus = multiprocessing.cpu_count()
        except NotImplementedError:
            cpus = 1
        self.assertTrue(type(cpus) is int)
        self.assertTrue(cpus >= 1)

    def test_active_children(self):
        self.assertEqual(type(self.active_children()), list)

        p = self.Process(target=time.sleep, args=(DELTA,))
        self.assertNotIn(p, self.active_children())

        p.daemon = True
        p.start()
        self.assertIn(p, self.active_children())

        p.join()
        self.assertNotIn(p, self.active_children())

    @classmethod
    def _test_recursion(cls, wconn, id):
        wconn.send(id)
        if len(id) < 2:
            for i in range(2):
                p = cls.Process(target=cls._test_recursion, args=(wconn, id + [i]))
                p.start()
                p.join()

    @unittest.skip("bug filed PE-40917")
    def test_recursion(self):
        rconn, wconn = self.Pipe(duplex=False)
        self._test_recursion(wconn, [])

        time.sleep(DELTA)
        result = []
        while rconn.poll():
            result.append(rconn.recv())

        expected = [[], [0], [0, 0], [0, 1], [1], [1, 0], [1, 1]]
        self.assertEqual(result, expected)

    @classmethod
    def _test_sentinel(cls, event):
        event.wait(10.0)

    def test_sentinel(self):
        if self.TYPE == "threads":
            self.skipTest("test not appropriate for {}".format(self.TYPE))
        event = self.Event()
        p = self.Process(target=self._test_sentinel, args=(event,))
        with self.assertRaises(ValueError):
            p.sentinel
        p.start()
        self.addCleanup(p.join)
        sentinel = p.sentinel
        self.assertIsInstance(sentinel, int)
        self.assertFalse(wait_for_handle(sentinel, timeout=0.0))
        event.set()
        p.join()
        self.assertTrue(wait_for_handle(sentinel, timeout=1))

    @classmethod
    def _test_close(cls, rc=0, q=None):
        if q is not None:
            q.get()
        sys.exit(rc)

    def test_close(self):
        if self.TYPE == "threads":
            self.skipTest("test not appropriate for {}".format(self.TYPE))
        q = self.Queue()
        p = self.Process(target=self._test_close, kwargs={"q": q})
        p.daemon = True
        p.start()
        self.assertEqual(p.is_alive(), True)
        # Child is still alive, cannot close
        with self.assertRaises(ValueError):
            p.close()

        q.put(None)
        p.join()
        self.assertEqual(p.is_alive(), False)
        self.assertEqual(p.exitcode, 0)
        p.close()
        with self.assertRaises(ValueError):
            p.is_alive()
        with self.assertRaises(ValueError):
            p.join()
        with self.assertRaises(ValueError):
            p.terminate()
        p.close()

        wr = weakref.ref(p)
        del p
        gc.collect()
        self.assertIs(wr(), None)

        close_queue(q)

    def test_many_processes(self):
        if self.TYPE == "threads":
            self.skipTest("test not appropriate for {}".format(self.TYPE))

        sm = multiprocessing.get_start_method()
        N = 5 if sm == "spawn" else 100

        # Try to overwhelm the forkserver loop with events
        procs = [self.Process(target=self._test_sleep, args=(0.01,)) for i in range(N)]
        for p in procs:
            p.start()
        for p in procs:
            join_process(p)
        for p in procs:
            self.assertEqual(p.exitcode, 0)

        procs = [self.Process(target=self._sleep_some) for i in range(N)]
        for p in procs:
            p.start()
        time.sleep(0.001)  # let the children start...
        for p in procs:
            p.terminate()
        for p in procs:
            join_process(p)
        if os.name != "nt":
            exitcodes = [-signal.SIGTERM]
            if sys.platform == "darwin":
                # bpo-31510: On macOS, killing a freshly started process with
                # SIGTERM sometimes kills the process with SIGKILL.
                exitcodes.append(-signal.SIGKILL)
            for p in procs:
                self.assertIn(p.exitcode, exitcodes)

    def test_lose_target_ref(self):
        c = DummyCallable()
        wr = weakref.ref(c)
        q = self.Queue()
        p = self.Process(target=c, args=(q, c))
        del c
        p.start()
        p.join()
        gc.collect()  # For PyPy or other GCs.
        self.assertIs(wr(), None)
        self.assertEqual(q.get(), 5)
        close_queue(q)

    @classmethod
    def _test_child_fd_inflation(self, evt, q):
        q.put(fd_count())
        evt.wait()

    def test_child_fd_inflation(self):
        # Number of fds in child processes should not grow with the
        # number of running children.
        if self.TYPE == "threads":
            self.skipTest("test not appropriate for {}".format(self.TYPE))

        sm = multiprocessing.get_start_method()
        if sm == "fork":
            # The fork method by design inherits all fds from the parent,
            # trying to go against it is a lost battle
            self.skipTest("test not appropriate for {}".format(sm))

        N = 5
        evt = self.Event()
        q = self.Queue()

        procs = [self.Process(target=self._test_child_fd_inflation, args=(evt, q)) for i in range(N)]
        for p in procs:
            p.start()

        try:
            fd_counts = [q.get() for i in range(N)]
            self.assertEqual(len(set(fd_counts)), 1, fd_counts)

        finally:
            evt.set()
            for p in procs:
                p.join()
            close_queue(q)

    @classmethod
    def _test_wait_for_threads(self, evt):
        def func1():
            time.sleep(0.5)
            evt.set()

        def func2():
            time.sleep(20)
            evt.clear()

        threading.Thread(target=func1).start()
        threading.Thread(target=func2, daemon=True).start()

    def test_wait_for_threads(self):
        # A child process should wait for non-daemonic threads to end
        # before exiting
        if self.TYPE == "threads":
            self.skipTest("test not appropriate for {}".format(self.TYPE))

        evt = self.Event()
        proc = self.Process(target=self._test_wait_for_threads, args=(evt,))
        proc.start()
        proc.join()
        self.assertTrue(evt.is_set())

    @classmethod
    def _test_error_on_stdio_flush(self, evt, break_std_streams={}):
        for stream_name, action in break_std_streams.items():
            if action == "close":
                stream = io.StringIO()
                stream.close()
            else:
                assert action == "remove"
                stream = None
            setattr(sys, stream_name, None)
        evt.set()

    def test_error_on_stdio_flush_1(self):
        # Check that Process works with broken standard streams
        streams = [io.StringIO(), None]
        streams[0].close()
        for stream_name in ("stdout", "stderr"):
            for stream in streams:
                old_stream = getattr(sys, stream_name)
                setattr(sys, stream_name, stream)
                try:
                    evt = self.Event()
                    proc = self.Process(target=self._test_error_on_stdio_flush, args=(evt,))
                    proc.start()
                    proc.join()
                    self.assertTrue(evt.is_set())
                    self.assertEqual(proc.exitcode, 0)
                finally:
                    setattr(sys, stream_name, old_stream)

    def test_error_on_stdio_flush_2(self):
        # Same as test_error_on_stdio_flush_1(), but standard streams are
        # broken by the child process
        for stream_name in ("stdout", "stderr"):
            for action in ("close", "remove"):
                old_stream = getattr(sys, stream_name)
                try:
                    evt = self.Event()
                    proc = self.Process(
                        target=self._test_error_on_stdio_flush, args=(evt, {stream_name: action})
                    )
                    proc.start()
                    proc.join()
                    self.assertTrue(evt.is_set())
                    self.assertEqual(proc.exitcode, 0)
                finally:
                    setattr(sys, stream_name, old_stream)

    @classmethod
    def _sleep_and_set_event(self, evt, delay=0.0):
        time.sleep(delay)
        evt.set()

    def check_forkserver_death(self, signum):
        # bpo-31308: if the forkserver process has died, we should still
        # be able to create and run new Process instances (the forkserver
        # is implicitly restarted).
        if self.TYPE == "threads":
            self.skipTest("test not appropriate for {}".format(self.TYPE))
        sm = multiprocessing.get_start_method()
        # if sm != "forkserver":
        #     # The fork method by design inherits all fds from the parent,
        #     # trying to go against it is a lost battle
        #     self.skipTest("test not appropriate for {}".format(sm))

        from multiprocessing.forkserver import _forkserver

        _forkserver.ensure_running()

        # First process sleeps 500 ms
        delay = 0.5

        evt = self.Event()
        proc = self.Process(target=self._sleep_and_set_event, args=(evt, delay))
        proc.start()

        pid = _forkserver._forkserver_pid
        os.kill(pid, signum)
        # give time to the fork server to die and time to proc to complete
        time.sleep(delay * 2.0)

        evt2 = self.Event()
        proc2 = self.Process(target=self._sleep_and_set_event, args=(evt2,))
        proc2.start()
        proc2.join()
        self.assertTrue(evt2.is_set())
        self.assertEqual(proc2.exitcode, 0)

        proc.join()
        self.assertTrue(evt.is_set())
        self.assertIn(proc.exitcode, (0, 255))

    @unittest.skip("DRAGON: Semlock not implemented")
    def test_forkserver_sigint(self):
        # Catchable signal
        self.check_forkserver_death(signal.SIGINT)

    @unittest.skip("DRAGON: Semlock not implemented")
    def test_forkserver_sigkill(self):
        # Uncatchable signal
        if os.name != "nt":
            self.check_forkserver_death(signal.SIGKILL)


class _UpperCaser(multiprocessing.Process):
    def __init__(self):
        multiprocessing.Process.__init__(self)
        self.child_conn, self.parent_conn = multiprocessing.Pipe()

    def run(self):
        self.parent_conn.close()
        for s in iter(self.child_conn.recv, None):
            self.child_conn.send(s.upper())
        self.child_conn.close()

    def submit(self, s):
        assert type(s) is str
        self.parent_conn.send(s)
        return self.parent_conn.recv()

    def stop(self):
        self.parent_conn.send(None)
        self.parent_conn.close()
        self.child_conn.close()


class TestSubclassingProcess(BaseTestCase, ProcessesMixin, unittest.TestCase):

    # DRAGON ALLOWED_TYPES = ("processes",)

    def test_subclassing(self):
        uppercaser = _UpperCaser()
        uppercaser.daemon = True
        uppercaser.start()
        self.assertEqual(uppercaser.submit("hello"), "HELLO")
        self.assertEqual(uppercaser.submit("world"), "WORLD")
        uppercaser.stop()
        uppercaser.join()

    def test_stderr_flush(self):
        # sys.stderr is flushed at process shutdown (issue #13812)
        if self.TYPE == "threads":
            self.skipTest("test not appropriate for {}".format(self.TYPE))

        testfn = TESTFN
        self.addCleanup(unlink, testfn)
        proc = self.Process(target=self._test_stderr_flush, args=(testfn,))
        proc.start()
        proc.join()
        with open(testfn, "r") as f:
            err = f.read()
            # The whole traceback was printed
            self.assertIn("ZeroDivisionError", err)
            # DRAGON     self.assertIn("test_multiprocessing.py", err)
            self.assertIn("test_process.py", err)
            self.assertIn("1 / 0  # MARKER", err)

    @classmethod
    def _test_stderr_flush(cls, testfn):
        fd = os.open(testfn, os.O_WRONLY | os.O_CREAT | os.O_EXCL)
        sys.stderr = open(fd, "w", closefd=False)
        1 / 0  # MARKER

    @classmethod
    def _test_sys_exit(cls, reason, testfn):
        fd = os.open(testfn, os.O_WRONLY | os.O_CREAT | os.O_EXCL)
        sys.stderr = open(fd, "w", closefd=False)
        sys.exit(reason)

    def test_sys_exit(self):
        # See Issue 13854
        if self.TYPE == "threads":
            self.skipTest("test not appropriate for {}".format(self.TYPE))

        testfn = TESTFN
        self.addCleanup(unlink, testfn)

        for reason in (
            [1, 2, 3],
            "ignore this",
        ):
            p = self.Process(target=self._test_sys_exit, args=(reason, testfn))
            p.daemon = True
            p.start()
            join_process(p)
            self.assertEqual(p.exitcode, 1)

            with open(testfn, "r") as f:
                content = f.read()
            self.assertEqual(content.rstrip(), str(reason))

            os.unlink(testfn)

        cases = [
            ((True,), 1),
            ((False,), 0),
            ((8,), 8),
            ((None,), 0),
            ((), 0),
        ]

        for args, expected in cases:
            with self.subTest(args=args):
                p = self.Process(target=sys.exit, args=args)
                p.daemon = True
                p.start()
                join_process(p)
                self.assertEqual(p.exitcode, expected)


class WithThreadsTestProcess(BaseTestCase, ThreadsMixin, unittest.TestCase):
    """Test multiprocessing.Process()"""

    # DRAGON ALLOWED_TYPES = ('processes', 'threads')

    def test_current(self):
        if self.TYPE == "threads":
            self.skipTest("test not appropriate for {}".format(self.TYPE))

        current = self.current_process()
        authkey = current.authkey

        self.assertTrue(current.is_alive())
        self.assertTrue(not current.daemon)
        self.assertIsInstance(authkey, bytes)
        self.assertTrue(len(authkey) > 0)
        self.assertEqual(current.ident, os.getpid())
        self.assertEqual(current.exitcode, None)

    def test_daemon_argument(self):
        if self.TYPE == "threads":
            self.skipTest("test not appropriate for {}".format(self.TYPE))

        # By default uses the current process's daemon flag.
        proc0 = self.Process(target=self._test)
        self.assertEqual(proc0.daemon, self.current_process().daemon)
        proc1 = self.Process(target=self._test, daemon=True)
        self.assertTrue(proc1.daemon)
        proc2 = self.Process(target=self._test, daemon=False)
        self.assertFalse(proc2.daemon)

    @classmethod
    def _test(cls, q, *args, **kwds):
        current = cls.current_process()
        q.put(args)
        q.put(kwds)
        q.put(current.name)
        if cls.TYPE != "threads":
            q.put(bytes(current.authkey))
            q.put(current.pid)

    def test_parent_process_attributes(self):
        if self.TYPE == "threads":
            self.skipTest("test not appropriate for {}".format(self.TYPE))

        self.assertIsNone(self.parent_process())

        rconn, wconn = self.Pipe(duplex=False)
        p = self.Process(target=self._test_send_parent_process, args=(wconn,))
        p.start()
        p.join()
        parent_pid, parent_name = rconn.recv()
        self.assertEqual(parent_pid, self.current_process().pid)
        self.assertEqual(parent_pid, os.getpid())
        self.assertEqual(parent_name, self.current_process().name)

    @classmethod
    def _test_send_parent_process(cls, wconn):
        from multiprocessing.process import parent_process

        wconn.send([parent_process().pid, parent_process().name])

    def test_parent_process(self):
        if self.TYPE == "threads":
            self.skipTest("test not appropriate for {}".format(self.TYPE))

        # Launch a child process. Make it launch a grandchild process. Kill the
        # child process and make sure that the grandchild notices the death of
        # its parent (a.k.a the child process).
        rconn, wconn = self.Pipe(duplex=False)
        p = self.Process(target=self._test_create_grandchild_process, args=(wconn,))
        p.start()

        if not rconn.poll(timeout=test.support.LONG_TIMEOUT):
            raise AssertionError("Could not communicate with child process")
        parent_process_status = rconn.recv()
        self.assertEqual(parent_process_status, "alive")

        p.terminate()
        p.join()

        if not rconn.poll(timeout=test.support.LONG_TIMEOUT):
            raise AssertionError("Could not communicate with child process")
        parent_process_status = rconn.recv()
        self.assertEqual(parent_process_status, "not alive")

    @classmethod
    def _test_create_grandchild_process(cls, wconn):
        p = cls.Process(target=cls._test_report_parent_status, args=(wconn,))
        p.start()
        time.sleep(300)

    @classmethod
    def _test_report_parent_status(cls, wconn):
        from multiprocessing.process import parent_process

        wconn.send("alive" if parent_process().is_alive() else "not alive")
        parent_process().join(timeout=test.support.SHORT_TIMEOUT)
        wconn.send("alive" if parent_process().is_alive() else "not alive")

    @unittest.skip("bug filed PE-40908")
    def test_process(self):
        q = self.Queue(1)
        e = self.Event()
        args = (q, 1, 2)
        kwargs = {"hello": 23, "bye": 2.54}
        name = "SomeProcess"
        p = self.Process(target=self._test, args=args, kwargs=kwargs, name=name)
        p.daemon = True
        current = self.current_process()

        if self.TYPE != "threads":
            self.assertEqual(p.authkey, current.authkey)
        self.assertEqual(p.is_alive(), False)
        self.assertEqual(p.daemon, True)
        self.assertNotIn(p, self.active_children())
        self.assertTrue(type(self.active_children()) is list)
        self.assertEqual(p.exitcode, None)

        p.start()

        self.assertEqual(p.exitcode, None)
        self.assertEqual(p.is_alive(), True)
        self.assertIn(p, self.active_children())

        self.assertEqual(q.get(), args[1:])
        self.assertEqual(q.get(), kwargs)
        self.assertEqual(q.get(), p.name)
        if self.TYPE != "threads":
            self.assertEqual(q.get(), current.authkey)
            self.assertEqual(q.get(), p.pid)

        p.join()

        self.assertEqual(p.exitcode, 0)
        self.assertEqual(p.is_alive(), False)
        self.assertNotIn(p, self.active_children())
        close_queue(q)

    @unittest.skipUnless(threading._HAVE_THREAD_NATIVE_ID, "needs native_id")
    def test_process_mainthread_native_id(self):
        if self.TYPE == "threads":
            self.skipTest("test not appropriate for {}".format(self.TYPE))

        current_mainthread_native_id = threading.main_thread().native_id

        q = self.Queue(1)
        p = self.Process(target=self._test_process_mainthread_native_id, args=(q,))
        p.start()

        child_mainthread_native_id = q.get()
        p.join()
        close_queue(q)

        self.assertNotEqual(current_mainthread_native_id, child_mainthread_native_id)

    @classmethod
    def _test_process_mainthread_native_id(cls, q):
        mainthread_native_id = threading.main_thread().native_id
        q.put(mainthread_native_id)

    @classmethod
    def _sleep_some(cls):
        time.sleep(100)

    @classmethod
    def _test_sleep(cls, delay):
        time.sleep(delay)

    def _kill_process(self, meth):
        if self.TYPE == "threads":
            self.skipTest("test not appropriate for {}".format(self.TYPE))

        p = self.Process(target=self._sleep_some)
        p.daemon = True
        p.start()

        self.assertEqual(p.is_alive(), True)
        self.assertIn(p, self.active_children())
        self.assertEqual(p.exitcode, None)

        join = TimingWrapper(p.join)

        self.assertEqual(join(0), None)
        self.assertTimingAlmostEqual(join.elapsed, 0.0)
        self.assertEqual(p.is_alive(), True)

        self.assertEqual(join(-1), None)
        self.assertTimingAlmostEqual(join.elapsed, 0.0)
        self.assertEqual(p.is_alive(), True)

        # XXX maybe terminating too soon causes the problems on Gentoo...
        time.sleep(1)

        meth(p)

        if hasattr(signal, "alarm"):
            # On the Gentoo buildbot waitpid() often seems to block forever.
            # We use alarm() to interrupt it if it blocks for too long.
            def handler(*args):
                raise RuntimeError("join took too long: %s" % p)

            old_handler = signal.signal(signal.SIGALRM, handler)
            try:
                signal.alarm(10)
                self.assertEqual(join(), None)
            finally:
                signal.alarm(0)
                signal.signal(signal.SIGALRM, old_handler)
        else:
            self.assertEqual(join(), None)

        self.assertTimingAlmostEqual(join.elapsed, 0.0)

        self.assertEqual(p.is_alive(), False)
        self.assertNotIn(p, self.active_children())

        p.join()

        return p.exitcode

    def test_terminate(self):
        exitcode = self._kill_process(multiprocessing.Process.terminate)
        if os.name != "nt":
            self.assertEqual(exitcode, -signal.SIGTERM)

    def test_kill(self):
        exitcode = self._kill_process(multiprocessing.Process.kill)
        if os.name != "nt":
            self.assertEqual(exitcode, -signal.SIGKILL)

    def test_cpu_count(self):
        try:
            cpus = multiprocessing.cpu_count()
        except NotImplementedError:
            cpus = 1
        self.assertTrue(type(cpus) is int)
        self.assertTrue(cpus >= 1)

    def test_active_children(self):
        if self.TYPE == "threads":
            self.skipTest("test not appropriate for {}".format(self.TYPE))

        self.assertEqual(type(self.active_children()), list)

        p = self.Process(target=time.sleep, args=(DELTA,))
        self.assertNotIn(p, self.active_children())

        p.daemon = True
        p.start()
        self.assertIn(p, self.active_children())

        p.join()
        self.assertNotIn(p, self.active_children())

    @classmethod
    def _test_recursion(cls, wconn, id):
        wconn.send(id)
        if len(id) < 2:
            for i in range(2):
                p = cls.Process(target=cls._test_recursion, args=(wconn, id + [i]))
                p.start()
                p.join()

    @unittest.skip("bug filed PE-40908")
    def test_recursion(self):
        rconn, wconn = self.Pipe(duplex=False)
        self._test_recursion(wconn, [])

        time.sleep(DELTA)
        result = []
        while rconn.poll():
            result.append(rconn.recv())

        expected = [[], [0], [0, 0], [0, 1], [1], [1, 0], [1, 1]]
        self.assertEqual(result, expected)

    @classmethod
    def _test_sentinel(cls, event):
        event.wait(10.0)

    def test_sentinel(self):
        if self.TYPE == "threads":
            self.skipTest("test not appropriate for {}".format(self.TYPE))
        event = self.Event()
        p = self.Process(target=self._test_sentinel, args=(event,))
        with self.assertRaises(ValueError):
            p.sentinel
        p.start()
        self.addCleanup(p.join)
        sentinel = p.sentinel
        self.assertIsInstance(sentinel, int)
        self.assertFalse(wait_for_handle(sentinel, timeout=0.0))
        event.set()
        p.join()
        self.assertTrue(wait_for_handle(sentinel, timeout=1))

    @classmethod
    def _test_close(cls, rc=0, q=None):
        if q is not None:
            q.get()
        sys.exit(rc)

    def test_close(self):
        if self.TYPE == "threads":
            self.skipTest("test not appropriate for {}".format(self.TYPE))
        q = self.Queue()
        p = self.Process(target=self._test_close, kwargs={"q": q})
        p.daemon = True
        p.start()
        self.assertEqual(p.is_alive(), True)
        # Child is still alive, cannot close
        with self.assertRaises(ValueError):
            p.close()

        q.put(None)
        p.join()
        self.assertEqual(p.is_alive(), False)
        self.assertEqual(p.exitcode, 0)
        p.close()
        with self.assertRaises(ValueError):
            p.is_alive()
        with self.assertRaises(ValueError):
            p.join()
        with self.assertRaises(ValueError):
            p.terminate()
        p.close()

        wr = weakref.ref(p)
        del p
        gc.collect()
        self.assertIs(wr(), None)

        close_queue(q)

    def test_many_processes(self):
        if self.TYPE == "threads":
            self.skipTest("test not appropriate for {}".format(self.TYPE))

        sm = multiprocessing.get_start_method()
        N = 5 if sm == "spawn" else 100

        # Try to overwhelm the forkserver loop with events
        procs = [self.Process(target=self._test_sleep, args=(0.01,)) for i in range(N)]
        for p in procs:
            p.start()
        for p in procs:
            join_process(p)
        for p in procs:
            self.assertEqual(p.exitcode, 0)

        procs = [self.Process(target=self._sleep_some) for i in range(N)]
        for p in procs:
            p.start()
        time.sleep(0.001)  # let the children start...
        for p in procs:
            p.terminate()
        for p in procs:
            join_process(p)
        if os.name != "nt":
            exitcodes = [-signal.SIGTERM]
            if sys.platform == "darwin":
                # bpo-31510: On macOS, killing a freshly started process with
                # SIGTERM sometimes kills the process with SIGKILL.
                exitcodes.append(-signal.SIGKILL)
            for p in procs:
                self.assertIn(p.exitcode, exitcodes)

    @unittest.skip("bug filed PE-40908")
    def test_lose_target_ref(self):
        c = DummyCallable()
        wr = weakref.ref(c)
        q = self.Queue()
        p = self.Process(target=c, args=(q, c))
        del c
        p.start()
        p.join()
        gc.collect()  # For PyPy or other GCs.
        self.assertIs(wr(), None)
        self.assertEqual(q.get(), 5)
        close_queue(q)

    @classmethod
    def _test_child_fd_inflation(self, evt, q):
        q.put(fd_count())
        evt.wait()

    @unittest.skip("DRAGON: Semlock not implemented")
    def test_child_fd_inflation(self):
        # Number of fds in child processes should not grow with the
        # number of running children.
        if self.TYPE == "threads":
            self.skipTest("test not appropriate for {}".format(self.TYPE))

        sm = multiprocessing.get_start_method()
        if sm == "fork":
            # The fork method by design inherits all fds from the parent,
            # trying to go against it is a lost battle
            self.skipTest("test not appropriate for {}".format(sm))

        N = 5
        evt = self.Event()
        q = self.Queue()

        procs = [self.Process(target=self._test_child_fd_inflation, args=(evt, q)) for i in range(N)]
        for p in procs:
            p.start()

        try:
            fd_counts = [q.get() for i in range(N)]
            self.assertEqual(len(set(fd_counts)), 1, fd_counts)

        finally:
            evt.set()
            for p in procs:
                p.join()
            close_queue(q)

    @classmethod
    def _test_wait_for_threads(self, evt):
        def func1():
            time.sleep(0.5)
            evt.set()

        def func2():
            time.sleep(20)
            evt.clear()

        threading.Thread(target=func1).start()
        threading.Thread(target=func2, daemon=True).start()

    def test_wait_for_threads(self):
        # A child process should wait for non-daemonic threads to end
        # before exiting
        if self.TYPE == "threads":
            self.skipTest("test not appropriate for {}".format(self.TYPE))

        evt = self.Event()
        proc = self.Process(target=self._test_wait_for_threads, args=(evt,))
        proc.start()
        proc.join()
        self.assertTrue(evt.is_set())

    @classmethod
    def _test_error_on_stdio_flush(self, evt, break_std_streams={}):
        for stream_name, action in break_std_streams.items():
            if action == "close":
                stream = io.StringIO()
                stream.close()
            else:
                assert action == "remove"
                stream = None
            setattr(sys, stream_name, None)
        evt.set()

    @unittest.skip("bug filed PE-40908")
    def test_error_on_stdio_flush_1(self):
        # Check that Process works with broken standard streams
        streams = [io.StringIO(), None]
        streams[0].close()
        for stream_name in ("stdout", "stderr"):
            for stream in streams:
                old_stream = getattr(sys, stream_name)
                setattr(sys, stream_name, stream)
                try:
                    evt = self.Event()
                    proc = self.Process(target=self._test_error_on_stdio_flush, args=(evt,))
                    proc.start()
                    proc.join()
                    self.assertTrue(evt.is_set())
                    self.assertEqual(proc.exitcode, 0)
                finally:
                    setattr(sys, stream_name, old_stream)

    @unittest.skip("bug filed PE-40908")
    def test_error_on_stdio_flush_2(self):
        # Same as test_error_on_stdio_flush_1(), but standard streams are
        # broken by the child process
        for stream_name in ("stdout", "stderr"):
            for action in ("close", "remove"):
                old_stream = getattr(sys, stream_name)
                try:
                    evt = self.Event()
                    proc = self.Process(
                        target=self._test_error_on_stdio_flush, args=(evt, {stream_name: action})
                    )
                    proc.start()
                    proc.join()
                    self.assertTrue(evt.is_set())
                    self.assertEqual(proc.exitcode, 0)
                finally:
                    setattr(sys, stream_name, old_stream)

    @classmethod
    def _sleep_and_set_event(self, evt, delay=0.0):
        time.sleep(delay)
        evt.set()

    def check_forkserver_death(self, signum):
        # bpo-31308: if the forkserver process has died, we should still
        # be able to create and run new Process instances (the forkserver
        # is implicitly restarted).
        if self.TYPE == "threads":
            self.skipTest("test not appropriate for {}".format(self.TYPE))
        sm = multiprocessing.get_start_method()
        if sm != "forkserver":
            # The fork method by design inherits all fds from the parent,
            # trying to go against it is a lost battle
            self.skipTest("test not appropriate for {}".format(sm))

        from multiprocessing.forkserver import _forkserver

        _forkserver.ensure_running()

        # First process sleeps 500 ms
        delay = 0.5

        evt = self.Event()
        proc = self.Process(target=self._sleep_and_set_event, args=(evt, delay))
        proc.start()

        pid = _forkserver._forkserver_pid
        os.kill(pid, signum)
        # give time to the fork server to die and time to proc to complete
        time.sleep(delay * 2.0)

        evt2 = self.Event()
        proc2 = self.Process(target=self._sleep_and_set_event, args=(evt2,))
        proc2.start()
        proc2.join()
        self.assertTrue(evt2.is_set())
        self.assertEqual(proc2.exitcode, 0)

        proc.join()
        self.assertTrue(evt.is_set())
        self.assertIn(proc.exitcode, (0, 255))

    def test_forkserver_sigint(self):
        # Catchable signal
        self.check_forkserver_death(signal.SIGINT)

    def test_forkserver_sigkill(self):
        # Uncatchable signal
        if os.name != "nt":
            self.check_forkserver_death(signal.SIGKILL)


# DRAGON
if __name__ == "__main__":
    setUpModule()
    unittest.main()
    tearDownModule()
