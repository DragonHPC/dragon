""" This file contains Dragon multi-node acceptance tests for the
`dragon.native.Barrier` object. The tests scale with the total number of CPUs
reported by the allocation, i.e. they become tougher on larger allocations.

The tests are run with `dragon test_barrier.py -f -v`
"""

import os
import unittest
import time
import pickle
import sys

import dragon

from dragon.native.process import current
from dragon.native.queue import Queue
from dragon.native.barrier import Barrier, BrokenBarrierError
from dragon.native.machine import cpu_count

from dragon.globalservices.process import (
    join,
    create,
    multi_join,
)
from dragon.infrastructure.process_desc import ProcessOptions
from dragon.utils import B64


def wait_and_timeout_gracefully(args):
    bytes = B64.str_to_bytes(args)
    barrier = pickle.loads(bytes)[0]
    try:
        start = time.monotonic()
        barrier.wait(timeout=2)
    except BrokenBarrierError:
        pass
    stop = time.monotonic()
    if abs(stop - start - 2) > 0.2:
        raise AssertionError


def wait_forever(args):
    try:
        bytes = B64.str_to_bytes(args)
        barrier = pickle.loads(bytes)[0]
        barrier.wait(timeout=None)
    except Exception as ex:
        print(f"There was an exception {ex}")


def wait_forever_and_timeout_gracefully(args):
    bytes = B64.str_to_bytes(args)
    barrier = pickle.loads(bytes)[0]
    try:
        barrier.wait(timeout=None)
    except BrokenBarrierError:
        pass


def reset_barrier(args):
    bytes = B64.str_to_bytes(args)
    arg_list = pickle.loads(bytes)
    barrier = arg_list[0]
    num_parties = arg_list[1]
    while barrier.n_waiting < num_parties:
        time.sleep(0.1)
    barrier.reset()


def abort_barrier(args):
    bytes = B64.str_to_bytes(args)
    arg_list = pickle.loads(bytes)
    barrier = arg_list[0]
    num_parties = arg_list[1]
    while barrier.n_waiting < num_parties:
        time.sleep(0.1)
    barrier.abort()


def wait_and_put_result(args):
    bytes = B64.str_to_bytes(args)
    barrier, queue = pickle.loads(bytes)
    queue.put(barrier.wait())


def wait_and_put_when_broken(args):
    bytes = B64.str_to_bytes(args)
    barrier, queue = pickle.loads(bytes)
    try:
        barrier.wait(timeout=None)
    except BrokenBarrierError:
        queue.put(True)


class TestBarrierMultiNode(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.ncpu = max(2, int(cpu_count() / 4))  # control the number of CPU for all tests
        # print(f'The tests were run with {cls.ncpu} parties for the Barriers.')

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
        arg = B64.bytes_to_str(
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
            [__file__, "wait_and_timeout_gracefully", arg],
            None,
            options=options,
        )
        join(process_1.p_uid)

        self.assertRaises(BrokenBarrierError, lambda: barrier.wait(timeout=0))

    def test_requirement_2_3_a(self):
        """
        If wait is called with timeout=None, the calling process has to block
        until the method has been called by parties processes, or abort() or
        reset() has been called by any other process.
        """

        barrier = Barrier(parties=self.ncpu)

        cmd = sys.executable
        wdir = "."
        options = ProcessOptions(make_inf_channels=True)

        arg = B64.bytes_to_str(
            pickle.dumps(
                [
                    barrier,
                ],
                protocol=5,
            )
        )

        puids = []
        for _ in range(self.ncpu):
            descr = create(
                cmd,
                wdir,
                [__file__, "wait_forever", arg],
                None,
                options=options,
            )
            puids.append(descr.p_uid)

        ready = multi_join(puids, join_all=True)

        for puid, ecode in ready[0]:
            self.assertTrue(ecode == 0)
            self.assertTrue(puid in puids)

    def test_requirement_2_3_b(self):
        """
        If wait is called with timeout=None, the calling process has to block
        until the method has been called by parties processes, or abort() or
        reset() has been called by any other process.
        """

        barrier = Barrier(parties=self.ncpu)

        cmd = sys.executable
        wdir = "."
        options = ProcessOptions(make_inf_channels=True)
        arg = B64.bytes_to_str(
            pickle.dumps(
                [
                    barrier,
                ],
                protocol=5,
            )
        )

        puids = []
        for _ in range(self.ncpu - 1):
            descr = create(
                cmd,
                wdir,
                [__file__, "wait_forever_and_timeout_gracefully", arg],
                None,
                options=options,
            )
            puids.append(descr.p_uid)

        arg2 = B64.bytes_to_str(
            pickle.dumps(
                [barrier, self.ncpu - 1],
                protocol=5,
            )
        )

        descr = create(
            cmd,
            wdir,
            [__file__, "reset_barrier", arg2],
            None,
            options=options,
        )
        puids.append(descr.p_uid)

        ready = multi_join(puids, join_all=True)

        for puid, ecode in ready[0]:
            self.assertTrue(ecode == 0)
            self.assertTrue(puid in puids)

    def test_requirement_2_3_c(self):
        """
        If wait is called with timeout=None, the calling process has to block
        until the method has been called by parties processes, or abort() or
        reset() has been called by any other process.
        """

        barrier = Barrier(parties=self.ncpu)

        cmd = sys.executable
        wdir = "."
        options = ProcessOptions(make_inf_channels=True)
        arg = B64.bytes_to_str(
            pickle.dumps(
                [
                    barrier,
                ],
                protocol=5,
            )
        )
        puids = []
        for _ in range(self.ncpu - 1):
            descr = create(
                cmd,
                wdir,
                [__file__, "wait_forever_and_timeout_gracefully", arg],
                None,
                options=options,
            )
            puids.append(descr.p_uid)

        arg2 = B64.bytes_to_str(
            pickle.dumps(
                [barrier, self.ncpu - 1],
                protocol=5,
            )
        )

        descr = create(
            cmd,
            wdir,
            [__file__, "abort_barrier", arg2],
            None,
            options=options,
        )
        puids.append(descr.p_uid)

        ready = multi_join(puids, join_all=True)

        for puid, ecode in ready[0]:
            self.assertTrue(ecode == 0)
            self.assertTrue(puid in puids)

    def test_requirement_2_4(self):
        """
        If wait() is called after abort() has been called by any process, raise
        a BrokenBarrierError until reset() is called.
        """

        barrier = Barrier(parties=self.ncpu)

        barrier.abort()
        self.assertRaises(BrokenBarrierError, lambda: barrier.wait(timeout=None))

        barrier.reset()

        # check that we are fully functional again
        cmd = sys.executable
        wdir = "."
        options = ProcessOptions(make_inf_channels=True)
        arg = B64.bytes_to_str(
            pickle.dumps(
                [
                    barrier,
                ],
                protocol=5,
            )
        )

        puids = []
        for _ in range(self.ncpu):
            descr = create(
                cmd,
                wdir,
                [__file__, "wait_forever", arg],
                None,
                options=options,
            )
            puids.append(descr.p_uid)

        ready = multi_join(puids, join_all=True)

        for puid, ecode in ready[0]:
            self.assertTrue(ecode == 0)
            self.assertTrue(puid in puids)

    def test_requirement_2_6(self):
        """
        If wait has been called parties times within timeout seconds,
        it has to return a unique integer on every process in range 0<=i<parties.
        """

        barrier = Barrier(action=None, parties=self.ncpu, timeout=None)

        queue = Queue(joinable=False)

        cmd = sys.executable
        wdir = "."
        options = ProcessOptions(make_inf_channels=True)
        arg = B64.bytes_to_str(pickle.dumps([barrier, queue], protocol=5))

        puids = []
        for _ in range(self.ncpu):
            process_1 = create(
                cmd,
                wdir,
                [__file__, "wait_and_put_result", arg],
                None,
                options=options,
            )
            puids.append(process_1.p_uid)

        queue_vals = []
        for _ in range(self.ncpu):
            queue_vals.append(queue.get())

        multi_join(puids, join_all=True)

        queue_vals = sorted(queue_vals)
        self.assertListEqual(list(range(self.ncpu)), queue_vals)

    def test_requirement_3_1(self):
        """
        If the reset has been called while processes are waiting in wait(), they
        have to raise a BrokenBarrierError immediately.
        """
        barrier = Barrier(parties=self.ncpu)

        queue = Queue(joinable=False)

        cmd = sys.executable
        wdir = "."
        options = ProcessOptions(make_inf_channels=True)
        arg = B64.bytes_to_str(pickle.dumps([barrier, queue], protocol=5))

        puids = []
        for _ in range(self.ncpu - 1):
            process_1 = create(
                cmd,
                wdir,
                [__file__, "wait_and_put_when_broken", arg],
                None,
                options=options,
            )
            puids.append(process_1.p_uid)

        arg2 = B64.bytes_to_str(
            pickle.dumps(
                [barrier, self.ncpu - 1],
                protocol=5,
            )
        )

        descr = create(
            cmd,
            wdir,
            [__file__, "reset_barrier", arg2],
            None,
            options=options,
        )

        for _ in range(self.ncpu - 1):  # hangs here ...
            self.assertTrue(queue.get())

        ready = multi_join(puids, join_all=True)

        for puid, ecode in ready[0]:
            self.assertTrue(ecode == 0)
            self.assertTrue(puid in puids)


if __name__ == "__main__":

    if len(sys.argv) > 1 and sys.argv[1] == "wait_forever":
        wait_forever(sys.argv[2])
    elif len(sys.argv) > 1 and sys.argv[1] == "wait_and_put_result":
        wait_and_put_result(sys.argv[2])
    elif len(sys.argv) > 1 and sys.argv[1] == "wait_and_timeout_gracefully":
        wait_and_timeout_gracefully(sys.argv[2])
    elif len(sys.argv) > 1 and sys.argv[1] == "wait_and_put_when_broken":
        wait_and_put_when_broken(sys.argv[2])
    elif len(sys.argv) > 1 and sys.argv[1] == "reset_barrier":
        reset_barrier(sys.argv[2])
    elif len(sys.argv) > 1 and sys.argv[1] == "abort_barrier":
        abort_barrier(sys.argv[2])
    elif len(sys.argv) > 1 and sys.argv[1] == "wait_forever_and_timeout_gracefully":
        wait_forever_and_timeout_gracefully(sys.argv[2])
    else:
        unittest.main()
