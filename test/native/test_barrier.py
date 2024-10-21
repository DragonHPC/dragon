import os
import unittest
import time
import pickle
import sys

import dragon

from dragon.native.process import current
from dragon.native.queue import Queue
from dragon.native.barrier import Barrier, BrokenBarrierError

from dragon.globalservices.process import (
    join,
    create,
    multi_join,
)
from dragon.infrastructure.process_desc import ProcessOptions
import dragon.utils as du


def timeout_wait_barrier(args):
    bytes = du.B64.str_to_bytes(args)
    barrier = pickle.loads(bytes)[0]
    try:
        barrier.wait(timeout=2)
    except BrokenBarrierError:
        pass


def wait_barrier_forever(args):
    bytes = du.B64.str_to_bytes(args)
    barrier = pickle.loads(bytes)[0]
    barrier.wait(timeout=None)


def wait_with_broken_barrier_helper(args):
    bytes = du.B64.str_to_bytes(args)
    barrier = pickle.loads(bytes)[0]
    try:
        barrier.wait(timeout=None)
    except BrokenBarrierError:
        pass


def reset_barrier(args):
    bytes = du.B64.str_to_bytes(args)
    barrier = pickle.loads(bytes)[0]
    barrier.reset()


def abort_barrier(args):
    bytes = du.B64.str_to_bytes(args)
    barrier = pickle.loads(bytes)[0]
    barrier.abort()


def barrier_wait_helper(args):
    bytes = du.B64.str_to_bytes(args)
    barrier, queue = pickle.loads(bytes)
    queue.put(barrier.wait())


def finite_barrier_wait(args):
    bytes = du.B64.str_to_bytes(args)
    barrier, queue = pickle.loads(bytes)
    try:
        barrier.wait(timeout=1)
    except BrokenBarrierError:
        queue.put(True)


def barrier_wait_and_put(args):
    bytes = du.B64.str_to_bytes(args)
    barrier, queue = pickle.loads(bytes)
    try:
        barrier.wait()
        queue.put(True)
    except BrokenBarrierError:
        queue.put(False)


def my_action():
    puid = current()
    with open("puid_file.txt", "a") as f:
        f.writelines(str(puid))


class TestBarrier(unittest.TestCase):
    def test_requirement_1_1(self):
        """Requirement 1.1: If the method is called with parties < 2, it has to
        raise a ValueError.
        """
        self.assertRaises(ValueError, lambda: Barrier(parties=0))
        self.assertRaises(ValueError, lambda: Barrier(parties=-1))

    def test_requirement_1_2(self):
        """Requirement 1.2: If the method is called with action not being either
        callable or None, it has to raise a ValueError
        """
        self.assertRaises(ValueError, lambda: Barrier(action=10))

    def test_requirement_1_3(self):
        """Requirement 1.3: If the method is called with timeout <0, it has to
        raise a ValueError.
        """
        self.assertRaises(ValueError, lambda: Barrier(timeout=-0.1))

    def test_requirement_2_0(self):
        """If the wait is called with a timeout, it takes precedent of the one
        that was supplied to __init__
        """
        def_timeout = 1
        barrier = Barrier(timeout=def_timeout, parties=2)
        start = time.monotonic()
        try:
            barrier.wait(timeout=0)
        except BrokenBarrierError:
            pass
        elap = time.monotonic() - start
        self.assertGreaterEqual(elap, 0)
        self.assertLess(elap, def_timeout) 

    def test_requirement_2_1(self):
        """If the wait is called with timeout <0, it has to assume raise a ValueError."""
        self.assertRaises(ValueError, lambda: Barrier().wait(timeout=-0.1))

    def test_requirement_2_2(self):
        """
        If wait is called by less than parties processes within timeout seconds
        from the first process calling the method, all processes who have called
        it and will call it until reset() was called, have to raise a
        BrokenBarrierError immediately.  Note also Req 2.4.
        """
        barrier = Barrier(parties=2)
        cmd = sys.executable
        wdir = "."
        options = ProcessOptions(make_inf_channels=True)
        env_str = du.B64.bytes_to_str(
            pickle.dumps(
                [
                    barrier,
                ],
                protocol=5,
            )
        )
        process_1 = create(
            cmd,
            wdir,
            [__file__, "timeout_wait_barrier", env_str],
            None,
            options=options,
        )
        self.assertRaises(BrokenBarrierError, lambda: barrier.wait(timeout=0))
        join(process_1.p_uid)

    def test_requirement_2_3_a(self):
        """
        If wait is called with timeout=None, the calling process has to block
        until the method has been called by parties processes, or abort() or
        reset() has been called by any other process.
        """
        barrier = Barrier(parties=2)
        cmd = sys.executable
        wdir = "."
        options = ProcessOptions(make_inf_channels=True)
        env_str = du.B64.bytes_to_str(
            pickle.dumps(
                [
                    barrier,
                ],
                protocol=5,
            )
        )
        process_1 = create(
            cmd,
            wdir,
            [__file__, "wait_barrier_forever", env_str],
            None,
            options=options,
        )
        process_2 = create(
            cmd,
            wdir,
            [__file__, "wait_barrier_forever", env_str],
            None,
            options=options,
        )
        multi_join([process_1.p_uid, process_2.p_uid])

    def test_requirement_2_3_b(self):
        """
        If wait is called with timeout=None, the calling process has to block
        until the method has been called by parties processes, or abort() or
        reset() has been called by any other process.
        """
        barrier = Barrier()
        cmd = sys.executable
        wdir = "."
        options = ProcessOptions(make_inf_channels=True)
        env_str = du.B64.bytes_to_str(
            pickle.dumps(
                [
                    barrier,
                ],
                protocol=5,
            )
        )
        process_1 = create(
            cmd,
            wdir,
            [__file__, "wait_with_broken_barrier_helper", env_str],
            None,
            options=options,
        )
        process_2 = create(
            cmd,
            wdir,
            [__file__, "reset_barrier", env_str],
            None,
            options=options,
        )
        multi_join([process_1.p_uid, process_2.p_uid])

    def test_requirement_2_3_c(self):
        """
        If wait is called with timeout=None, the calling process has to block
        until the method has been called by parties processes, or abort() or
        reset() has been called by any other process.
        """
        barrier = Barrier()
        cmd = sys.executable
        wdir = "."
        options = ProcessOptions(make_inf_channels=True)
        env_str = du.B64.bytes_to_str(
            pickle.dumps(
                [
                    barrier,
                ],
                protocol=5,
            )
        )
        process_1 = create(
            cmd,
            wdir,
            [__file__, "wait_with_broken_barrier_helper", env_str],
            None,
            options=options,
        )
        process_2 = create(
            cmd,
            wdir,
            [__file__, "abort_barrier", env_str],
            None,
            options=options,
        )
        multi_join([process_1.p_uid, process_2.p_uid])

    def test_requirement_2_4(self):
        """
        If wait() is called after abort() has been called by any process, raise
        a BrokenBarrierError until reset() is called.
        """
        barrier = Barrier()
        cmd = sys.executable
        wdir = "."
        options = ProcessOptions(make_inf_channels=True)
        env_str = du.B64.bytes_to_str(
            pickle.dumps(
                [
                    barrier,
                ],
                protocol=5,
            )
        )
        process_1 = create(
            cmd,
            wdir,
            [__file__, "wait_with_broken_barrier_helper", env_str],
            None,
            options=options,
        )
        process_2 = create(
            cmd,
            wdir,
            [__file__, "abort_barrier", env_str],
            None,
            options=options,
        )
        multi_join([process_1.p_uid, process_2.p_uid])
        self.assertRaises(BrokenBarrierError, lambda: barrier.wait())
        self.assertIsNone(barrier.reset(), "The barrier did not reset.")

    @unittest.skipIf(
        os.path.dirname(os.path.realpath(__file__)) != os.getcwd(),
        f"Pickling callables works only if test is executed from the file dir.",
    )
    def test_requirement_2_5(self):
        """
        If wait has been called parties times by different processes/threads
        within timeout seconds, one process/thread waiting in the method has to
        call action if it is a callable. If action is None, do nothing.
        """

        if os.path.exists("puid_file.txt"):
            os.remove("puid_file.txt")

        barrier = Barrier(parties=2, action=my_action)

        cmd = sys.executable
        wdir = "."
        options = ProcessOptions(make_inf_channels=True)
        env_str = du.B64.bytes_to_str(
            pickle.dumps(
                [
                    barrier,
                ],
            )
        )
        process_1 = create(
            cmd,
            wdir,
            [__file__, "wait_barrier_forever", env_str],
            None,
            options=options,
        )
        process_2 = create(
            cmd,
            wdir,
            [__file__, "wait_barrier_forever", env_str],
            None,
            options=options,
        )

        multi_join([process_1.p_uid, process_2.p_uid])

        puid_val = None
        with open("puid_file.txt", "r") as f:
            lines = f.readlines()
            puid_val = int(lines[0])
        self.assertTrue(puid_val in [process_1.p_uid, process_2.p_uid])

        if os.path.exists("puid_file.txt"):
            os.remove("puid_file.txt")
        os.chdir("..")

    def test_requirement_2_6(self):
        """
        If wait has been called parties times within timeout seconds,
        it has to return a unique integer on every process in range 0<=i<parties.
        """
        barrier = Barrier(action=None, parties=5, timeout=None)
        queue = Queue(joinable=False)
        cmd = sys.executable
        wdir = "."
        options = ProcessOptions(make_inf_channels=True)
        env_str = du.B64.bytes_to_str(pickle.dumps([barrier, queue], protocol=5))
        puids = []
        for _ in range(5):
            process_1 = create(
                cmd,
                wdir,
                [__file__, "barrier_wait_helper", env_str],
                None,
                options=options,
            )
            puids.append(process_1.p_uid)
        multi_join(puids)
        queue_vals = []
        for _ in range(5):
            queue_vals.append(queue.get())
        queue_vals = sorted(queue_vals)
        self.assertListEqual(
            [0, 1, 2, 3, 4], queue_vals, "The queue did not receive unique integers between 0 and 5"
        )

    def test_requirement_3_1(self):
        """
        If the reset has been called while processes are waiting in wait(), they
        have to raise a BrokenBarrierError immediately.
        """
        barrier = Barrier()
        queue = Queue(joinable=False)
        cmd = sys.executable
        wdir = "."
        options = ProcessOptions(make_inf_channels=True)
        env_str = du.B64.bytes_to_str(pickle.dumps([barrier, queue], protocol=5))
        process_1 = create(
            cmd,
            wdir,
            [__file__, "finite_barrier_wait", env_str],
            None,
            options=options,
        )
        process_2 = create(
            cmd,
            wdir,
            [__file__, "reset_barrier", env_str],
            None,
            options=options,
        )
        self.assertTrue(queue.get(), "The BrokenBarrierException has not occurred.")
        multi_join([process_1.p_uid, process_2.p_uid])

    def test_requirement_3_2(self):
        """
        After the reset has been called, processes calling wait() will not raise
        a BrokenBarrierError anymore. (Duplicate to 2.2 & 2.4)
        """
        barrier = Barrier(parties=5, timeout=None)
        cmd = sys.executable
        wdir = "."
        options = ProcessOptions(make_inf_channels=True)
        env_str = du.B64.bytes_to_str(
            pickle.dumps(
                [
                    barrier,
                ]
            )
        )
        process_1 = create(
            cmd,
            wdir,
            [__file__, "wait_with_broken_barrier_helper", env_str],
            None,
            options=options,
        )
        process_2 = create(
            cmd,
            wdir,
            [__file__, "abort_barrier", env_str],
            None,
            options=options,
        )
        multi_join([process_1.p_uid, process_2.p_uid])

        barrier.reset()

        queue = Queue(joinable=False)

        env_str = du.B64.bytes_to_str(pickle.dumps([barrier, queue]))
        process_4 = create(
            cmd,
            wdir,
            [__file__, "barrier_wait_and_put", env_str],
            None,
            options=options,
        )
        process_5 = create(
            cmd,
            wdir,
            [__file__, "barrier_wait_and_put", env_str],
            None,
            options=options,
        )
        multi_join([process_4.p_uid, process_5.p_uid])

        self.assertTrue(queue.get(), "The first barrier wait did not complete successfully.")
        self.assertTrue(queue.get(), "The second barrier wait did not complete successfully.")

    def test_requirements_5_6_7(self):
        """
        The class needs to implement an attribute Barrier.parties : int that is
        > 0 and holds the parties attribute from the __init__ method.

        The class needs to implement an attribute Barrier.n_waiting : int that
        is >= 0 and the number of waiting processes/threads.

        The class needs to implement an attribute Barrier.broken : int
        """
        barrier = Barrier()
        self.assertIsInstance(barrier.parties, int, "The parties are not an int.")
        self.assertIsInstance(barrier.n_waiting, int, "The number of waiting threads are not an int.")
        self.assertIsInstance(barrier.broken, int, "The broken barrier error is not returning an int")


if __name__ == "__main__":

    if len(sys.argv) > 1 and sys.argv[1] == "wait_barrier_forever":
        wait_barrier_forever(sys.argv[2])
    elif len(sys.argv) > 1 and sys.argv[1] == "barrier_wait_helper":
        barrier_wait_helper(sys.argv[2])
    elif len(sys.argv) > 1 and sys.argv[1] == "timeout_wait_barrier":
        timeout_wait_barrier(sys.argv[2])
    elif len(sys.argv) > 1 and sys.argv[1] == "finite_barrier_wait":
        finite_barrier_wait(sys.argv[2])
    elif len(sys.argv) > 1 and sys.argv[1] == "barrier_wait_and_put":
        finite_barrier_wait(sys.argv[2])
    elif len(sys.argv) > 1 and sys.argv[1] == "reset_barrier":
        reset_barrier(sys.argv[2])
    elif len(sys.argv) > 1 and sys.argv[1] == "abort_barrier":
        abort_barrier(sys.argv[2])
    elif len(sys.argv) > 1 and sys.argv[1] == "wait_with_broken_barrier_helper":
        wait_with_broken_barrier_helper(sys.argv[2])
    else:
        unittest.main()
