#!/usr/bin/env python3

import unittest
import time
import pickle
import sys
import os

import dragon
import dragon.globalservices.channel
from dragon.globalservices.process import (
    multi_join,
    create,
    get_list,
    join,
)
from dragon.infrastructure.process_desc import ProcessOptions
from dragon.native.semaphore import Semaphore
import dragon.utils as du

TIMEOUT_DELTA_TOL = 1.0


def _waiter(env_str):

    # unpack the semaphore
    bytes = du.B64.str_to_bytes(env_str)
    sem = pickle.loads(bytes)

    # acquire it
    sem.acquire(blocking=True, timeout=None)


class TestSemaphore(unittest.TestCase):
    def setUp(self):
        self.num_proc = 4

    def test_basic_lifecycle(self):

        sem = Semaphore(value=1)

        # init correct ?
        self.assertTrue(sem._bounded == False)
        self.assertTrue(sem._channel.capacity == 1)
        self.assertTrue(1 == sem.get_value())

        # simple acquire correct ?
        ret_val = sem.acquire()
        self.assertTrue(ret_val == True)
        self.assertTrue(0 == sem.get_value())
        self.assertTrue(sem.acquire(False) == False)  # cannot acquire below 0

        # blocking correct ?
        start = time.monotonic()
        ret_val = sem.acquire(blocking=False, timeout=None)
        elap = time.monotonic() - start
        self.assertTrue(ret_val == False)
        self.assertGreaterEqual(elap, 0)
        self.assertLess(elap, TIMEOUT_DELTA_TOL)

        # timeout correct ?
        start = time.monotonic()
        ret_val = sem.acquire(blocking=True, timeout=0.05)
        elap = time.monotonic() - start
        self.assertTrue(ret_val == False)
        self.assertGreaterEqual(elap, 0.05)
        self.assertLess(elap, (0.05 + TIMEOUT_DELTA_TOL))

        # can release ?
        sem.release(n=1)
        ret_val = sem.acquire(blocking=True, timeout=None)
        self.assertTrue(ret_val == True)
        ret_val = sem.acquire(blocking=False, timeout=None)
        self.assertTrue(ret_val == False)

        # can release many at once ?
        sem.release(n=16)
        for _ in range(16):
            ret_val = sem.acquire(blocking=True, timeout=None)
            self.assertTrue(ret_val == True)

        ret_val = sem.acquire(blocking=False, timeout=None)
        self.assertTrue(ret_val == False)

        # get_value correct ?
        value = sem.get_value()
        self.assertTrue(value == 0)

        sem.release(n=16)
        value = sem.get_value()
        self.assertTrue(value == 16)

    def test_serialization(self):

        value = 14041981
        sem = Semaphore(value=value)
        # pickle correct ?
        data = pickle.dumps(sem, protocol=5)
        sem2 = pickle.loads(data)

        self.assertTrue(sem2._channel.cuid == sem._channel.cuid)
        self.assertTrue(sem.get_value() == value)
        self.assertTrue(sem2.get_value() == value)

    def test_many_processes_lifecycle(self):

        sem = Semaphore(value=0)

        cmd = sys.executable
        wdir = "."
        options = ProcessOptions(make_inf_channels=True)
        env_str = du.B64.bytes_to_str(pickle.dumps(sem, protocol=5))

        puids = []
        for _ in range(0, self.num_proc):
            the_descr = create(
                cmd,
                wdir,
                [__file__, "waiter", env_str],
                None,
                options=options,
            )
            puids.append(the_descr.p_uid)

        live_puids = get_list()

        for puid in puids:
            self.assertTrue(puid in live_puids)  # all should be blocked

        sem.release()  # release one process

        object_tuples, proc_status = multi_join(puids, timeout=None)
        ready_puids = [i[0] for i in object_tuples]
        self.assertTrue(len(ready_puids) == 1)  # one ! should have returned

        value = sem.get_value()  # semaphore value should 0
        self.assertTrue(value == 0)

        puids.remove(ready_puids[0])

        live_puids = get_list()
        for puid in puids:
            self.assertTrue(puid in live_puids)  # all should be blocked

        # release them all
        sem.release(n=self.num_proc - 1)

        for puid in puids:  # should not hang
            join(puid)

    def test_is_bounded(self):

        bsem = Semaphore(value=21071985, bounded=True)

        self.assertTrue(bsem._bounded == True)
        self.assertRaises(ValueError, bsem.release)


if __name__ == "__main__":

    if len(sys.argv) > 1 and sys.argv[1] == "waiter":
        _waiter(sys.argv[2])
    else:
        unittest.main()
