import unittest
import os
import time
import queue as pyqueue


import test.support
from test.support import hashlib_helper
from test import support

import dragon  # DRAGON import before multiprocessing

import multiprocessing

from common import (
    BaseTestCase,
    ProcessesMixin,
    ManagerMixin,
    ThreadsMixin,
    TimingWrapper,
    setUpModule,
    tearDownModule,
    close_queue,
    DELTA,
    TIMEOUT1,
    TIMEOUT2,
    TIMEOUT3,
)

#
#
#


def queue_empty(q):
    if hasattr(q, "empty"):
        return q.empty()
    else:
        return q.qsize() == 0


def queue_full(q, maxsize):
    if hasattr(q, "full"):
        return q.full()
    else:
        return q.qsize() == maxsize


class WithProcessesTestQueue(BaseTestCase, ProcessesMixin, unittest.TestCase):
    @classmethod
    def _test_put(cls, queue, child_can_start, parent_can_continue):
        child_can_start.wait()
        for i in range(6):
            queue.get()
        parent_can_continue.set()

    def test_put(self):
        MAXSIZE = 6
        queue = self.Queue(maxsize=MAXSIZE)
        child_can_start = self.Event()
        parent_can_continue = self.Event()

        proc = self.Process(target=self._test_put, args=(queue, child_can_start, parent_can_continue))
        proc.daemon = True
        proc.start()

        self.assertEqual(queue_empty(queue), True)
        self.assertEqual(queue_full(queue, MAXSIZE), False)

        queue.put(1)
        queue.put(2, True)
        queue.put(3, True, None)
        queue.put(4, False)
        queue.put(5, False, None)
        queue.put_nowait(6)

        # the values may be in buffer but not yet in pipe so sleep a bit
        time.sleep(DELTA)

        self.assertEqual(queue_empty(queue), False)
        self.assertEqual(queue_full(queue, MAXSIZE), True)

        put = TimingWrapper(queue.put)
        put_nowait = TimingWrapper(queue.put_nowait)

        self.assertRaises(pyqueue.Full, put, 7, False)
        self.assertTimingAlmostEqual(put.elapsed, 0)

        self.assertRaises(pyqueue.Full, put, 7, False, None)
        self.assertTimingAlmostEqual(put.elapsed, 0)

        self.assertRaises(pyqueue.Full, put_nowait, 7)
        self.assertTimingAlmostEqual(put_nowait.elapsed, 0)

        self.assertRaises(pyqueue.Full, put, 7, True, TIMEOUT1)
        self.assertTimingAlmostEqual(put.elapsed, TIMEOUT1)

        self.assertRaises(pyqueue.Full, put, 7, False, TIMEOUT2)
        self.assertTimingAlmostEqual(put.elapsed, 0)

        self.assertRaises(pyqueue.Full, put, 7, True, timeout=TIMEOUT3)
        self.assertTimingAlmostEqual(put.elapsed, TIMEOUT3)

        child_can_start.set()
        parent_can_continue.wait()

        self.assertEqual(queue_empty(queue), True)
        self.assertEqual(queue_full(queue, MAXSIZE), False)

        proc.join()
        close_queue(queue)

    @classmethod
    def _test_get(cls, queue, child_can_start, parent_can_continue):
        child_can_start.wait()
        queue.put(1)
        queue.put(2)
        queue.put(3)
        queue.put(4)
        queue.put(5)
        parent_can_continue.set()

    def test_get(self):
        queue = self.Queue()
        child_can_start = self.Event()
        parent_can_continue = self.Event()

        proc = self.Process(target=self._test_get, args=(queue, child_can_start, parent_can_continue))
        proc.daemon = True
        proc.start()

        self.assertEqual(queue_empty(queue), True)

        child_can_start.set()
        parent_can_continue.wait()

        time.sleep(DELTA)
        self.assertEqual(queue_empty(queue), False)

        self.assertEqual(queue.get(), 1)
        self.assertEqual(queue.get(True, None), 2)
        self.assertEqual(queue.get(True), 3)
        self.assertEqual(queue.get(timeout=1), 4)
        self.assertEqual(queue.get_nowait(), 5)

        self.assertEqual(queue_empty(queue), True)

        get = TimingWrapper(queue.get)
        get_nowait = TimingWrapper(queue.get_nowait)

        self.assertRaises(pyqueue.Empty, get, False)
        self.assertTimingAlmostEqual(get.elapsed, 0)

        self.assertRaises(pyqueue.Empty, get, False, None)
        self.assertTimingAlmostEqual(get.elapsed, 0)

        self.assertRaises(pyqueue.Empty, get_nowait)
        self.assertTimingAlmostEqual(get_nowait.elapsed, 0)

        self.assertRaises(pyqueue.Empty, get, True, TIMEOUT1)
        self.assertTimingAlmostEqual(get.elapsed, TIMEOUT1)

        self.assertRaises(pyqueue.Empty, get, False, TIMEOUT2)
        self.assertTimingAlmostEqual(get.elapsed, 0)

        self.assertRaises(pyqueue.Empty, get, timeout=TIMEOUT3)
        self.assertTimingAlmostEqual(get.elapsed, TIMEOUT3)

        proc.join()
        close_queue(queue)

    @classmethod
    def _test_fork(cls, queue):
        for i in range(10, 20):
            queue.put(i)
        # note that at this point the items may only be buffered, so the
        # process cannot shutdown until the feeder thread has finished
        # pushing items onto the pipe.

    def test_fork(self):
        # Old versions of Queue would fail to create a new feeder
        # thread for a forked process if the original process had its
        # own feeder thread.  This test checks that this no longer
        # happens.

        queue = self.Queue()

        # put items on queue so that main process starts a feeder thread
        for i in range(10):
            queue.put(i)

        # wait to make sure thread starts before we fork a new process
        time.sleep(DELTA)

        # fork process
        p = self.Process(target=self._test_fork, args=(queue,))
        p.daemon = True
        p.start()

        # check that all expected items are in the queue
        for i in range(20):
            self.assertEqual(queue.get(), i)
        self.assertRaises(pyqueue.Empty, queue.get, False)

        p.join()
        close_queue(queue)

    def test_qsize(self):
        q = self.Queue()
        try:
            self.assertEqual(q.qsize(), 0)
        except NotImplementedError:
            self.skipTest("qsize method not implemented")
        q.put(1)
        self.assertEqual(q.qsize(), 1)
        q.put(5)
        self.assertEqual(q.qsize(), 2)
        q.get()
        self.assertEqual(q.qsize(), 1)
        q.get()
        self.assertEqual(q.qsize(), 0)
        close_queue(q)

    @classmethod
    def _test_task_done(cls, q):
        for obj in iter(q.get, None):
            time.sleep(DELTA)
            q.task_done()

    def test_task_done(self):
        queue = self.JoinableQueue()

        workers = [self.Process(target=self._test_task_done, args=(queue,)) for i in range(4)]

        for p in workers:
            p.daemon = True
            p.start()

        for i in range(10):
            queue.put(i)

        queue.join()

        for p in workers:
            queue.put(None)

        for p in workers:
            p.join()

        close_queue(queue)

    def test_no_import_lock_contention(self):
        with test.support.temp_cwd():
            module_name = "imported_by_an_imported_module"
            with open(module_name + ".py", "w") as f:
                f.write(
                    """if 1:
                    import multiprocessing

                    q = multiprocessing.Queue()
                    q.put('knock knock')
                    q.get(timeout=3)
                    q.close()
                    del q
                """
                )

            with test.support.DirsOnSysPath(os.getcwd()):
                try:
                    __import__(module_name)
                except pyqueue.Empty:
                    self.fail("Probable regression on import lock contention;" " see Issue #22853")

    def test_timeout(self):
        q = multiprocessing.Queue()
        start = time.monotonic()
        self.assertRaises(pyqueue.Empty, q.get, True, 0.200)
        delta = time.monotonic() - start
        # bpo-30317: Tolerate a delta of 100 ms because of the bad clock
        # resolution on Windows (usually 15.6 ms). x86 Windows7 3.x once
        # failed because the delta was only 135.8 ms.
        self.assertGreaterEqual(delta, 0.100)
        close_queue(q)

    def test_queue_feeder_donot_stop_onexc(self):
        # bpo-30414: verify feeder handles exceptions correctly
        if self.TYPE != "processes":
            self.skipTest("test not appropriate for {}".format(self.TYPE))

        class NotSerializable(object):
            def __reduce__(self):
                raise AttributeError

        with test.support.captured_stderr():
            q = self.Queue()
            q.put(NotSerializable())
            q.put(True)
            self.assertTrue(q.get(timeout=support.SHORT_TIMEOUT))
            close_queue(q)

        with test.support.captured_stderr():
            # bpo-33078: verify that the queue size is correctly handled
            # on errors.
            q = self.Queue(maxsize=1)
            q.put(NotSerializable())
            q.put(True)
            try:
                self.assertEqual(q.qsize(), 1)
            except NotImplementedError:
                # qsize is not available on all platform as it
                # relies on sem_getvalue
                pass
            self.assertTrue(q.get(timeout=support.SHORT_TIMEOUT))

            # Check that the size of the queue is correct
            self.assertTrue(q.empty())
            close_queue(q)

    def test_queue_feeder_on_queue_feeder_error(self):
        # bpo-30006: verify feeder handles exceptions using the
        # _on_queue_feeder_error hook.
        if self.TYPE != "processes":
            self.skipTest("test not appropriate for {}".format(self.TYPE))

        class NotSerializable(object):
            """Mock unserializable object"""

            def __init__(self):
                self.reduce_was_called = False
                self.on_queue_feeder_error_was_called = False

            def __reduce__(self):
                self.reduce_was_called = True
                raise AttributeError

        class SafeQueue(multiprocessing.queues.Queue):
            """Queue with overloaded _on_queue_feeder_error hook"""

            @staticmethod
            def _on_queue_feeder_error(e, obj):
                if isinstance(e, AttributeError) and isinstance(obj, NotSerializable):
                    obj.on_queue_feeder_error_was_called = True

        not_serializable_obj = NotSerializable()
        # The captured_stderr reduces the noise in the test report
        with test.support.captured_stderr():
            q = SafeQueue(ctx=multiprocessing.get_context())
            q.put(not_serializable_obj)

            # Verify that q is still functioning correctly
            q.put(True)
            self.assertTrue(q.get(timeout=support.SHORT_TIMEOUT))

        # Assert that the serialization and the hook have been called correctly
        self.assertTrue(not_serializable_obj.reduce_was_called)
        self.assertTrue(not_serializable_obj.on_queue_feeder_error_was_called)

    def test_closed_queue_put_get_exceptions(self):
        for q in multiprocessing.Queue(), multiprocessing.JoinableQueue():
            q.close()
            with self.assertRaisesRegex(ValueError, "is closed"):
                q.put("foo")
            with self.assertRaisesRegex(ValueError, "is closed"):
                q.get()


@unittest.skip("DRAGON: Manager not implemented")
@hashlib_helper.requires_hashdigest("md5")
class WithManagerTestQueue(BaseTestCase, ManagerMixin, unittest.TestCase):
    @classmethod
    def _test_put(cls, queue, child_can_start, parent_can_continue):
        child_can_start.wait()
        for i in range(6):
            queue.get()
        parent_can_continue.set()

    def test_put(self):
        MAXSIZE = 6
        queue = self.Queue(maxsize=MAXSIZE)
        child_can_start = self.Event()
        parent_can_continue = self.Event()

        proc = self.Process(target=self._test_put, args=(queue, child_can_start, parent_can_continue))
        proc.daemon = True
        proc.start()

        self.assertEqual(queue_empty(queue), True)
        self.assertEqual(queue_full(queue, MAXSIZE), False)

        queue.put(1)
        queue.put(2, True)
        queue.put(3, True, None)
        queue.put(4, False)
        queue.put(5, False, None)
        queue.put_nowait(6)

        # the values may be in buffer but not yet in pipe so sleep a bit
        time.sleep(DELTA)

        self.assertEqual(queue_empty(queue), False)
        self.assertEqual(queue_full(queue, MAXSIZE), True)

        put = TimingWrapper(queue.put)
        put_nowait = TimingWrapper(queue.put_nowait)

        self.assertRaises(pyqueue.Full, put, 7, False)
        self.assertTimingAlmostEqual(put.elapsed, 0)

        self.assertRaises(pyqueue.Full, put, 7, False, None)
        self.assertTimingAlmostEqual(put.elapsed, 0)

        self.assertRaises(pyqueue.Full, put_nowait, 7)
        self.assertTimingAlmostEqual(put_nowait.elapsed, 0)

        self.assertRaises(pyqueue.Full, put, 7, True, TIMEOUT1)
        self.assertTimingAlmostEqual(put.elapsed, TIMEOUT1)

        self.assertRaises(pyqueue.Full, put, 7, False, TIMEOUT2)
        self.assertTimingAlmostEqual(put.elapsed, 0)

        self.assertRaises(pyqueue.Full, put, 7, True, timeout=TIMEOUT3)
        self.assertTimingAlmostEqual(put.elapsed, TIMEOUT3)

        child_can_start.set()
        parent_can_continue.wait()

        self.assertEqual(queue_empty(queue), True)
        self.assertEqual(queue_full(queue, MAXSIZE), False)

        proc.join()
        close_queue(queue)

    @classmethod
    def _test_get(cls, queue, child_can_start, parent_can_continue):
        child_can_start.wait()
        # queue.put(1)
        queue.put(2)
        queue.put(3)
        queue.put(4)
        queue.put(5)
        parent_can_continue.set()

    def test_get(self):
        queue = self.Queue()
        child_can_start = self.Event()
        parent_can_continue = self.Event()

        proc = self.Process(target=self._test_get, args=(queue, child_can_start, parent_can_continue))
        proc.daemon = True
        proc.start()

        self.assertEqual(queue_empty(queue), True)

        child_can_start.set()
        parent_can_continue.wait()

        time.sleep(DELTA)
        self.assertEqual(queue_empty(queue), False)

        # Hangs unexpectedly, remove for now
        # self.assertEqual(queue.get(), 1)
        self.assertEqual(queue.get(True, None), 2)
        self.assertEqual(queue.get(True), 3)
        self.assertEqual(queue.get(timeout=1), 4)
        self.assertEqual(queue.get_nowait(), 5)

        self.assertEqual(queue_empty(queue), True)

        get = TimingWrapper(queue.get)
        get_nowait = TimingWrapper(queue.get_nowait)

        self.assertRaises(pyqueue.Empty, get, False)
        self.assertTimingAlmostEqual(get.elapsed, 0)

        self.assertRaises(pyqueue.Empty, get, False, None)
        self.assertTimingAlmostEqual(get.elapsed, 0)

        self.assertRaises(pyqueue.Empty, get_nowait)
        self.assertTimingAlmostEqual(get_nowait.elapsed, 0)

        self.assertRaises(pyqueue.Empty, get, True, TIMEOUT1)
        self.assertTimingAlmostEqual(get.elapsed, TIMEOUT1)

        self.assertRaises(pyqueue.Empty, get, False, TIMEOUT2)
        self.assertTimingAlmostEqual(get.elapsed, 0)

        self.assertRaises(pyqueue.Empty, get, timeout=TIMEOUT3)
        self.assertTimingAlmostEqual(get.elapsed, TIMEOUT3)

        proc.join()
        close_queue(queue)

    @classmethod
    def _test_fork(cls, queue):
        for i in range(10, 20):
            queue.put(i)
        # note that at this point the items may only be buffered, so the
        # process cannot shutdown until the feeder thread has finished
        # pushing items onto the pipe.

    def test_fork(self):
        # Old versions of Queue would fail to create a new feeder
        # thread for a forked process if the original process had its
        # own feeder thread.  This test checks that this no longer
        # happens.

        queue = self.Queue()

        # put items on queue so that main process starts a feeder thread
        for i in range(10):
            queue.put(i)

        # wait to make sure thread starts before we fork a new process
        time.sleep(DELTA)

        # fork process
        p = self.Process(target=self._test_fork, args=(queue,))
        p.daemon = True
        p.start()

        # check that all expected items are in the queue
        for i in range(20):
            self.assertEqual(queue.get(), i)
        self.assertRaises(pyqueue.Empty, queue.get, False)

        p.join()
        close_queue(queue)

    def test_qsize(self):
        q = self.Queue()
        try:
            self.assertEqual(q.qsize(), 0)
        except NotImplementedError:
            self.skipTest("qsize method not implemented")
        q.put(1)
        self.assertEqual(q.qsize(), 1)
        q.put(5)
        self.assertEqual(q.qsize(), 2)
        q.get()
        self.assertEqual(q.qsize(), 1)
        q.get()
        self.assertEqual(q.qsize(), 0)
        close_queue(q)

    @classmethod
    def _test_task_done(cls, q):
        for obj in iter(q.get, None):
            time.sleep(DELTA)
            q.task_done()

    def test_task_done(self):
        queue = self.JoinableQueue()

        workers = [self.Process(target=self._test_task_done, args=(queue,)) for i in range(4)]

        for p in workers:
            p.daemon = True
            p.start()

        for i in range(10):
            queue.put(i)

        queue.join()

        for p in workers:
            queue.put(None)

        for p in workers:
            p.join()
        close_queue(queue)

    def test_no_import_lock_contention(self):
        with test.support.temp_cwd():
            module_name = "imported_by_an_imported_module"
            with open(module_name + ".py", "w") as f:
                f.write(
                    """if 1:
                    import multiprocessing

                    q = multiprocessing.Queue()
                    q.put('knock knock')
                    q.get(timeout=3)
                    q.close()
                    del q
                """
                )

            with test.support.DirsOnSysPath(os.getcwd()):
                try:
                    __import__(module_name)
                except pyqueue.Empty:
                    self.fail("Probable regression on import lock contention;" " see Issue #22853")

    def test_timeout(self):
        q = multiprocessing.Queue()
        start = time.monotonic()
        self.assertRaises(pyqueue.Empty, q.get, True, 0.200)
        delta = time.monotonic() - start
        # bpo-30317: Tolerate a delta of 100 ms because of the bad clock
        # resolution on Windows (usually 15.6 ms). x86 Windows7 3.x once
        # failed because the delta was only 135.8 ms.
        self.assertGreaterEqual(delta, 0.100)
        close_queue(q)

    def test_queue_feeder_donot_stop_onexc(self):
        # bpo-30414: verify feeder handles exceptions correctly
        if self.TYPE != "processes":
            self.skipTest("test not appropriate for {}".format(self.TYPE))

        class NotSerializable(object):
            def __reduce__(self):
                raise AttributeError

        with test.support.captured_stderr():
            q = self.Queue()
            q.put(NotSerializable())
            q.put(True)
            self.assertTrue(q.get(timeout=support.SHORT_TIMEOUT))
            close_queue(q)

        with test.support.captured_stderr():
            # bpo-33078: verify that the queue size is correctly handled
            # on errors.
            q = self.Queue(maxsize=1)
            q.put(NotSerializable())
            q.put(True)
            try:
                self.assertEqual(q.qsize(), 1)
            except NotImplementedError:
                # qsize is not available on all platform as it
                # relies on sem_getvalue
                pass
            self.assertTrue(q.get(timeout=support.SHORT_TIMEOUT))
            # Check that the size of the queue is correct
            self.assertTrue(q.empty())
            close_queue(q)

    def test_queue_feeder_on_queue_feeder_error(self):
        # bpo-30006: verify feeder handles exceptions using the
        # _on_queue_feeder_error hook.
        if self.TYPE != "processes":
            self.skipTest("test not appropriate for {}".format(self.TYPE))

        class NotSerializable(object):
            """Mock unserializable object"""

            def __init__(self):
                self.reduce_was_called = False
                self.on_queue_feeder_error_was_called = False

            def __reduce__(self):
                self.reduce_was_called = True
                raise AttributeError

        class SafeQueue(multiprocessing.queues.Queue):
            """Queue with overloaded _on_queue_feeder_error hook"""

            @staticmethod
            def _on_queue_feeder_error(e, obj):
                if isinstance(e, AttributeError) and isinstance(obj, NotSerializable):
                    obj.on_queue_feeder_error_was_called = True

        not_serializable_obj = NotSerializable()
        # The captured_stderr reduces the noise in the test report
        with test.support.captured_stderr():
            q = SafeQueue(ctx=multiprocessing.get_context())
            q.put(not_serializable_obj)

            # Verify that q is still functioning correctly
            q.put(True)
            self.assertTrue(q.get(timeout=support.SHORT_TIMEOUT))

        # Assert that the serialization and the hook have been called correctly
        self.assertTrue(not_serializable_obj.reduce_was_called)
        self.assertTrue(not_serializable_obj.on_queue_feeder_error_was_called)

    def test_closed_queue_put_get_exceptions(self):
        for q in multiprocessing.Queue(), multiprocessing.JoinableQueue():
            q.close()
            with self.assertRaisesRegex(ValueError, "is closed"):
                q.put("foo")
            with self.assertRaisesRegex(ValueError, "is closed"):
                q.get()


@unittest.skip("DRAGON: Threads not implemented")
class WithThreadsTestQueue(BaseTestCase, ThreadsMixin, unittest.TestCase):
    @classmethod
    def _test_put(cls, queue, child_can_start, parent_can_continue):
        child_can_start.wait()
        for i in range(6):
            queue.get()
        parent_can_continue.set()

    def test_put(self):
        MAXSIZE = 6
        queue = self.Queue(maxsize=MAXSIZE)
        child_can_start = self.Event()
        parent_can_continue = self.Event()

        proc = self.Process(target=self._test_put, args=(queue, child_can_start, parent_can_continue))
        proc.daemon = True
        proc.start()

        self.assertEqual(queue_empty(queue), True)
        self.assertEqual(queue_full(queue, MAXSIZE), False)

        queue.put(1)
        queue.put(2, True)
        queue.put(3, True, None)
        queue.put(4, False)
        queue.put(5, False, None)
        queue.put_nowait(6)

        # the values may be in buffer but not yet in pipe so sleep a bit
        time.sleep(DELTA)

        self.assertEqual(queue_empty(queue), False)
        self.assertEqual(queue_full(queue, MAXSIZE), True)

        put = TimingWrapper(queue.put)
        put_nowait = TimingWrapper(queue.put_nowait)

        self.assertRaises(pyqueue.Full, put, 7, False)
        self.assertTimingAlmostEqual(put.elapsed, 0)

        self.assertRaises(pyqueue.Full, put, 7, False, None)
        self.assertTimingAlmostEqual(put.elapsed, 0)

        self.assertRaises(pyqueue.Full, put_nowait, 7)
        self.assertTimingAlmostEqual(put_nowait.elapsed, 0)

        self.assertRaises(pyqueue.Full, put, 7, True, TIMEOUT1)
        self.assertTimingAlmostEqual(put.elapsed, TIMEOUT1)

        self.assertRaises(pyqueue.Full, put, 7, False, TIMEOUT2)
        self.assertTimingAlmostEqual(put.elapsed, 0)

        self.assertRaises(pyqueue.Full, put, 7, True, timeout=TIMEOUT3)
        self.assertTimingAlmostEqual(put.elapsed, TIMEOUT3)

        child_can_start.set()
        parent_can_continue.wait()

        self.assertEqual(queue_empty(queue), True)
        self.assertEqual(queue_full(queue, MAXSIZE), False)

        proc.join()
        close_queue(queue)

    @classmethod
    def _test_get(cls, queue, child_can_start, parent_can_continue):
        child_can_start.wait()
        # queue.put(1)
        queue.put(2)
        queue.put(3)
        queue.put(4)
        queue.put(5)
        parent_can_continue.set()

    def test_get(self):
        queue = self.Queue()
        child_can_start = self.Event()
        parent_can_continue = self.Event()

        proc = self.Process(target=self._test_get, args=(queue, child_can_start, parent_can_continue))
        proc.daemon = True
        proc.start()

        self.assertEqual(queue_empty(queue), True)

        child_can_start.set()
        parent_can_continue.wait()

        time.sleep(DELTA)
        self.assertEqual(queue_empty(queue), False)

        # Hangs unexpectedly, remove for now
        # self.assertEqual(queue.get(), 1)
        self.assertEqual(queue.get(True, None), 2)
        self.assertEqual(queue.get(True), 3)
        self.assertEqual(queue.get(timeout=1), 4)
        self.assertEqual(queue.get_nowait(), 5)

        self.assertEqual(queue_empty(queue), True)

        get = TimingWrapper(queue.get)
        get_nowait = TimingWrapper(queue.get_nowait)

        self.assertRaises(pyqueue.Empty, get, False)
        self.assertTimingAlmostEqual(get.elapsed, 0)

        self.assertRaises(pyqueue.Empty, get, False, None)
        self.assertTimingAlmostEqual(get.elapsed, 0)

        self.assertRaises(pyqueue.Empty, get_nowait)
        self.assertTimingAlmostEqual(get_nowait.elapsed, 0)

        self.assertRaises(pyqueue.Empty, get, True, TIMEOUT1)
        self.assertTimingAlmostEqual(get.elapsed, TIMEOUT1)

        self.assertRaises(pyqueue.Empty, get, False, TIMEOUT2)
        self.assertTimingAlmostEqual(get.elapsed, 0)

        self.assertRaises(pyqueue.Empty, get, timeout=TIMEOUT3)
        self.assertTimingAlmostEqual(get.elapsed, TIMEOUT3)

        proc.join()
        close_queue(queue)

    @classmethod
    def _test_fork(cls, queue):
        for i in range(10, 20):
            queue.put(i)
        # note that at this point the items may only be buffered, so the
        # process cannot shutdown until the feeder thread has finished
        # pushing items onto the pipe.

    def test_fork(self):
        # Old versions of Queue would fail to create a new feeder
        # thread for a forked process if the original process had its
        # own feeder thread.  This test checks that this no longer
        # happens.

        queue = self.Queue()

        # put items on queue so that main process starts a feeder thread
        for i in range(10):
            queue.put(i)

        # wait to make sure thread starts before we fork a new process
        time.sleep(DELTA)

        # fork process
        p = self.Process(target=self._test_fork, args=(queue,))
        p.daemon = True
        p.start()

        # check that all expected items are in the queue
        for i in range(20):
            self.assertEqual(queue.get(), i)
        self.assertRaises(pyqueue.Empty, queue.get, False)

        p.join()
        close_queue(queue)

    def test_qsize(self):
        q = self.Queue()
        try:
            self.assertEqual(q.qsize(), 0)
        except NotImplementedError:
            self.skipTest("qsize method not implemented")
        q.put(1)
        self.assertEqual(q.qsize(), 1)
        q.put(5)
        self.assertEqual(q.qsize(), 2)
        q.get()
        self.assertEqual(q.qsize(), 1)
        q.get()
        self.assertEqual(q.qsize(), 0)
        close_queue(q)

    @classmethod
    def _test_task_done(cls, q):
        for obj in iter(q.get, None):
            time.sleep(DELTA)
            q.task_done()

    def test_task_done(self):
        queue = self.JoinableQueue()

        workers = [self.Process(target=self._test_task_done, args=(queue,)) for i in range(4)]

        for p in workers:
            p.daemon = True
            p.start()

        for i in range(10):
            queue.put(i)

        queue.join()

        for p in workers:
            queue.put(None)

        for p in workers:
            p.join()
        close_queue(queue)

    def test_no_import_lock_contention(self):
        with test.support.temp_cwd():
            module_name = "imported_by_an_imported_module"
            with open(module_name + ".py", "w") as f:
                f.write(
                    """if 1:
                    import multiprocessing

                    q = multiprocessing.Queue()
                    q.put('knock knock')
                    q.get(timeout=3)
                    q.close()
                    del q
                """
                )

            with test.support.DirsOnSysPath(os.getcwd()):
                try:
                    __import__(module_name)
                except pyqueue.Empty:
                    self.fail("Probable regression on import lock contention;" " see Issue #22853")

    def test_timeout(self):
        q = multiprocessing.Queue()
        start = time.monotonic()
        self.assertRaises(pyqueue.Empty, q.get, True, 0.200)
        delta = time.monotonic() - start
        # bpo-30317: Tolerate a delta of 100 ms because of the bad clock
        # resolution on Windows (usually 15.6 ms). x86 Windows7 3.x once
        # failed because the delta was only 135.8 ms.
        self.assertGreaterEqual(delta, 0.100)
        close_queue(q)

    def test_queue_feeder_donot_stop_onexc(self):
        # bpo-30414: verify feeder handles exceptions correctly
        if self.TYPE != "processes":
            self.skipTest("test not appropriate for {}".format(self.TYPE))

        class NotSerializable(object):
            def __reduce__(self):
                raise AttributeError

        with test.support.captured_stderr():
            q = self.Queue()
            q.put(NotSerializable())
            q.put(True)
            self.assertTrue(q.get(timeout=support.SHORT_TIMEOUT))
            close_queue(q)

        with test.support.captured_stderr():
            # bpo-33078: verify that the queue size is correctly handled
            # on errors.
            q = self.Queue(maxsize=1)
            q.put(NotSerializable())
            q.put(True)
            try:
                self.assertEqual(q.qsize(), 1)
            except NotImplementedError:
                # qsize is not available on all platform as it
                # relies on sem_getvalue
                pass
            self.assertTrue(q.get(timeout=support.SHORT_TIMEOUT))
            # Check that the size of the queue is correct
            self.assertTrue(q.empty())
            close_queue(q)

    def test_queue_feeder_on_queue_feeder_error(self):
        # bpo-30006: verify feeder handles exceptions using the
        # _on_queue_feeder_error hook.
        if self.TYPE != "processes":
            self.skipTest("test not appropriate for {}".format(self.TYPE))

        class NotSerializable(object):
            """Mock unserializable object"""

            def __init__(self):
                self.reduce_was_called = False
                self.on_queue_feeder_error_was_called = False

            def __reduce__(self):
                self.reduce_was_called = True
                raise AttributeError

        class SafeQueue(multiprocessing.queues.Queue):
            """Queue with overloaded _on_queue_feeder_error hook"""

            @staticmethod
            def _on_queue_feeder_error(e, obj):
                if isinstance(e, AttributeError) and isinstance(obj, NotSerializable):
                    obj.on_queue_feeder_error_was_called = True

        not_serializable_obj = NotSerializable()
        # The captured_stderr reduces the noise in the test report
        with test.support.captured_stderr():
            q = SafeQueue(ctx=multiprocessing.get_context())
            q.put(not_serializable_obj)

            # Verify that q is still functioning correctly
            q.put(True)
            self.assertTrue(q.get(timeout=support.SHORT_TIMEOUT))

        # Assert that the serialization and the hook have been called correctly
        self.assertTrue(not_serializable_obj.reduce_was_called)
        self.assertTrue(not_serializable_obj.on_queue_feeder_error_was_called)

    def test_closed_queue_put_get_exceptions(self):
        for q in multiprocessing.Queue(), multiprocessing.JoinableQueue():
            q.close()
            with self.assertRaisesRegex(ValueError, "is closed"):
                q.put("foo")
            with self.assertRaisesRegex(ValueError, "is closed"):
                q.get()


# DRAGON
if __name__ == "__main__":
    setUpModule()
    unittest.main()
    tearDownModule()
