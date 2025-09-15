import unittest
from threading import Thread
import time
import pickle
import sys

import dragon
import dragon.native.event
from dragon.globalservices.process import (
    join,
    create,
    multi_join,
)
from dragon.infrastructure.process_desc import ProcessOptions
import dragon.utils as du

TIMEOUT_DELTA_TOL = 1.0


def wait_event_with_timeout(args):
    bytes = du.B64.str_to_bytes(args)
    event, timeout = pickle.loads(bytes)
    wait_val = event.wait(timeout=timeout)
    assert wait_val == True, "The process wait did not poll a message"


def set_event(event):
    bytes = du.B64.str_to_bytes(event)
    event = pickle.loads(bytes)
    event.set()
    assert event.is_set() == True, "The event is not set"


class TestEvent(unittest.TestCase):
    @staticmethod
    def set_event(event):
        event.set()
        assert event.is_set() == True, "The event is not set"

    @staticmethod
    def wait_event_with_timeout(event, timeout):
        wait_val = event.wait(timeout=timeout)
        assert wait_val == True, "The thread wait did not poll a message"

    @staticmethod
    def thread_forloop_wait_event_with_timeout(event, timeout):
        time.sleep(0.01)
        wait_val = event.wait(timeout=timeout)
        assert wait_val == True, "The thread wait did not poll a message"

    def test_process_event_none_timeout(self):
        """Requirement 1.1 for processes: If the method is called with timeout=None,
        the calling process blocks until another process calls set() on the same event class.
        The calling process then unblocks and the class return True.
        """
        event = dragon.native.event.Event()
        cmd = sys.executable
        wdir = "."
        options = ProcessOptions(make_inf_channels=True)
        env_str = du.B64.bytes_to_str(pickle.dumps([event, None], protocol=5))
        event.set()
        process_1 = create(
            cmd,
            wdir,
            [__file__, "wait_event_with_timeout", env_str],
            None,
            options=options,
        )
        env_str_2 = du.B64.bytes_to_str(pickle.dumps(event, protocol=5))
        process_2 = create(
            cmd,
            wdir,
            [__file__, "set_event", env_str_2],
            None,
            options=options,
        )
        join(process_1.p_uid, timeout=None)
        join(process_2.p_uid, timeout=None)
        self.assertTrue(event.wait(), "The event is not set by the second process.")

    def test_thread_event_none_timeout(self):
        """Requirement 1.1 for threads."""
        event = dragon.native.event.Event()
        t1 = Thread(target=self.wait_event_with_timeout, args=(event, None))
        t2 = Thread(target=self.set_event, args=(event,))
        event.set()
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        self.assertTrue(event.wait(), "The event is not set by the second process.")

    def test_process_event_postive_timeout(self):
        """Requirement 1.2 for processes: If the method is called with timeout > 0 and no other process has called set, the calling process blocks until
        timeout number of seconds have elapsed,
        The class then returns False.
        """
        event = dragon.native.event.Event()
        def_timeout = 0.9
        start = time.monotonic()
        event_wait = event.wait(def_timeout)
        elap = time.monotonic() - start
        self.assertGreaterEqual(elap, def_timeout)
        self.assertLess(elap, (def_timeout + TIMEOUT_DELTA_TOL))
        # This proves the event was not set and wait returns False as a result.
        self.assertFalse(event_wait, "The event is set")
        self.assertFalse(event.is_set(), "The event is set")

    def test_process_wait_event_with_timeout(self):
        """Requirement 1.3 for processes: The calling process does not block on a timeout of 0, but returns False immediately if set() has not been called before.
        It returns True immediately, if set() has been called before by any process.
        """
        event = dragon.native.event.Event()
        start = time.monotonic()
        wait_val = event.wait(timeout=0)
        elap = time.monotonic() - start
        self.assertGreaterEqual(elap, 0)
        self.assertLess(elap, TIMEOUT_DELTA_TOL)
        self.assertFalse(wait_val, "The event is set")
        cmd = sys.executable
        wdir = "."
        options = ProcessOptions(make_inf_channels=True)
        env_str = du.B64.bytes_to_str(pickle.dumps(event, protocol=5))
        the_descr = create(
            cmd,
            wdir,
            [__file__, "set_event", env_str],
            None,
            options=options,
        )
        join(the_descr.p_uid)
        self.assertEqual(event.is_set(), event.wait(), "The event is not set")

    def test_thread_wait_event_with_timeout(self):
        """Requirement 1.3 for threads."""
        event = dragon.native.event.Event()
        start = time.monotonic()
        wait_val = event.wait(timeout=0)
        elap = time.monotonic() - start
        self.assertGreaterEqual(elap, 0)
        self.assertLess(elap, TIMEOUT_DELTA_TOL)
        self.assertFalse(wait_val, "The event is set")
        t1 = Thread(target=self.set_event, args=(event,))
        t1.start()
        t1.join()
        self.assertEqual(event.is_set(), event.wait(), "The event is not set")

    def test_process_event_negative_wait_timeout(self):
        """Requirement 1.4 for process: If the method is called with timeout<0 ,
        it has to behave as if it was called with timeout=0.
        """
        event = dragon.native.event.Event()
        start = time.monotonic()
        wait_val = event.wait(timeout=-1)
        stop = time.monotonic()
        elap = time.monotonic() - start
        self.assertGreaterEqual(elap, 0)
        self.assertLess(elap, TIMEOUT_DELTA_TOL)
        self.assertFalse(wait_val, "The event is set")

    def test_process_multiple_sets(self):
        """Requirement 1.5 for processes: If the method is called after set()
        has been called by any process, it always returns True immediately.
        """
        event = dragon.native.event.Event()
        event.set()
        self.assertTrue(event.is_set(), "The event was not set")
        wait_val = event.wait()
        self.assertTrue(wait_val, "The event was not set")

    def test_process_multiple_wait_set(self):
        """Requirement 2.1 for processes: If the method is called, all processes
        blocking in a wait() call return True.
        """
        event = dragon.native.event.Event()
        cmd = sys.executable
        wdir = "."
        options = ProcessOptions(make_inf_channels=True)
        env_str = du.B64.bytes_to_str(pickle.dumps([event, None]))
        puids = []
        for _ in range(5):
            the_descr = create(
                cmd,
                wdir,
                [__file__, "wait_event_with_timeout", env_str],
                None,
                options=options,
            )
            puids.append(the_descr.p_uid)

        event.set()

        ready = multi_join(puids, timeout=10, join_all=True)

        for puid, ret_code in ready[0]:
            self.assertTrue(puid in puids)
            self.assertTrue(ret_code == 0)

    def test_thread_multiple_wait_set(self):
        """Requirement 2.1 for threads."""

        event = dragon.native.event.Event()

        threads = []
        for _ in range(5):
            thread = Thread(target=self.thread_forloop_wait_event_with_timeout, args=(event, None))
            threads.append(thread)
            thread.start()

        event.set()

        for thread in threads:
            join_val = thread.join()
            self.assertIsNone(join_val)

    def test_process_varied_timeouts(self):
        """Requirement 2.2 for processes: After the method was called, all
        processes calling wait() in the future return True immediately,
        regardless if the value of timeout was timeout=None, timeout<0,
        timeout=0 or timeout>0.
        """
        event = dragon.native.event.Event()
        cmd = sys.executable
        wdir = "."
        options = ProcessOptions(make_inf_channels=True)
        env_str = du.B64.bytes_to_str(pickle.dumps(event, protocol=5))
        puids = []
        timeouts = [-1, 1, 0, None]
        for timeout in timeouts:
            env_str = du.B64.bytes_to_str(pickle.dumps([event, timeout], protocol=5))
            the_descr = create(
                cmd,
                wdir,
                [__file__, "wait_event_with_timeout", env_str],
                None,
                options=options,
            )
            puids.append(the_descr.p_uid)
        event.set()
        ready = multi_join(puids, timeout=None, join_all=True)

        for puid, ret_code in ready[0]:
            self.assertTrue(puid in puids)
            self.assertTrue(ret_code == 0)

    def test_thread_varied_timeouts(self):
        """Requirement 2.2 for threads."""
        event = dragon.native.event.Event()
        threads = []
        timeouts = [None, -1, 0, 0]
        for timeout in timeouts:
            thread = Thread(target=self.thread_forloop_wait_event_with_timeout, args=(event, timeout))
            threads.append(thread)
            thread.start()
        event.set()
        for thread in threads:
            join_val = thread.join()
            self.assertIsNone(join_val, "The thread did not join properly.")
        self.assertTrue(event.is_set(), "The event is not registering as set by the main thread.")

    def test_set(self):
        """Requirement 3.1: The method must return True only if set() was called
        before on the same Event by the same process or any other process.
        """

        event = dragon.native.event.Event()
        event.set()
        self.assertTrue(event.is_set())

    def test_set_false(self):
        """Requirement 3.2: The method must return False only if set() was not
        called before on the same Event by the same or any other process.
        """

        event = dragon.native.event.Event()
        self.assertFalse(event.is_set())

    def test_set_clear(self):
        """Requirement 4.1: If the method is called after a process has call
        set(), the result is that the event class behaves as if set() has not
        been called.
        """

        event = dragon.native.event.Event()

        event.set()
        self.assertTrue(event.is_set())

        event.clear()
        self.assertFalse(event.is_set())

    def test_clear_no_set(self):
        """Requirement 4.2: If the method is called and set() has not been
        called before by any processes, it is a no-op."""

        event = dragon.native.event.Event()
        event.clear()
        self.assertFalse(event.is_set())


if __name__ == "__main__":

    if len(sys.argv) > 1 and sys.argv[1] == "set_event":
        set_event(sys.argv[2])
    elif len(sys.argv) > 1 and sys.argv[1] == "wait_event_with_timeout":
        wait_event_with_timeout(sys.argv[2])
    else:
        unittest.main()
