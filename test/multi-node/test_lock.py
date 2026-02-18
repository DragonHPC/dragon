"""This file contains Dragon multi-node acceptance tests for the
`dragon.native.Lock` object.  The test scales with the total number of CPUs
reported by the allocation, i.e. it becomes tougher on larger allocations.

The test is run with `dragon test_lock.py -f -v`
"""

import unittest
import pickle
import sys
import os

import dragon
from dragon.globalservices.process import (
    multi_join,
    create,
    get_list,
    join,
)
from dragon.infrastructure.process_desc import ProcessOptions
from dragon.infrastructure.parameters import this_process
from dragon.native.lock import Lock
from dragon.native.machine import cpu_count
import dragon.utils as du


def _burner(env_str):
    num_acq = 128

    # unpack the lock from the environment string
    bytes = du.B64.str_to_bytes(env_str)
    lock = pickle.loads(bytes)

    for _ in range(num_acq):
        success = lock.acquire(block=True, timeout=None)
        lock.release()  # burn it !
        assert success is True, "Could not acquire lock"


def _releaser(env_str):
    # unpack the lock from the environment string
    bytes = du.B64.str_to_bytes(env_str)
    rlock = pickle.loads(bytes)

    # try to release it
    try:
        rlock.release()
    except AssertionError:
        pass
    else:
        assert False, "Release should not have been successful"


def _acquirer(env_str):
    # unpack the lock from the environment string
    bytes = du.B64.str_to_bytes(env_str)
    lock = pickle.loads(bytes)

    lock.acquire(block=True, timeout=None)
    assert lock.is_mine(), "is_mine() broken"


class TestLockMultiNode(unittest.TestCase):

    @unittest.skipIf(bool(os.environ.get("DRAGON_PROXY_ENABLED")), "Fails in proxy mode")
    def test_lock_basic(self):
        """Test that processes actually wait on the lock and can be released
        by any other process.
        """

        nprocs = max(2, cpu_count() // 8)  # all sleeping in a lock

        # get a lock
        lock = Lock(recursive=False)
        self.assertTrue(lock.acquire())  # head process owns the lock now
        self.assertFalse(lock.acquire(timeout=0))  # cannot acquire again

        # start some processes
        cmd = sys.executable
        wdir = "."
        options = ProcessOptions(make_inf_channels=True)
        env_str = du.B64.bytes_to_str(pickle.dumps(lock, protocol=5))

        puids = []
        for _ in range(nprocs):
            the_descr = create(
                cmd,
                wdir,
                [__file__, "_acquirer", env_str],
                None,
                options=options,
            )
            puids.append(the_descr.p_uid)

        # all should be waiting, we still own the lock
        live_puids = get_list()
        live_puids.remove(this_process.my_puid)

        for puid in puids:
            self.assertTrue(puid in live_puids)

        # release one by one
        for _ in puids:
            lock.release()
            ready = multi_join(live_puids, timeout=None, join_all=False)
            self.assertTrue(len(ready[0]) == 1)  # only one has exited !
            ecode = ready[0][0][1]
            self.assertTrue(ecode == 0)
            puid = ready[0][0][0]
            live_puids.remove(puid)

        # cause a value error
        lock.release()  # last puid left the Lock in a locked state
        self.assertRaises(ValueError, lock.release)

    def test_recursive(self):
        """Test that recursive locks can be acquired multiple times and
        need to be released multiple times by the same process
        """

        # get a recursive lock
        rlock = Lock(recursive=True)

        # acquire it 25 times
        for _ in range(25):
            self.assertTrue(rlock.acquire())
        self.assertTrue(rlock._accesscount == 25)

        # start a processes that tries to release it
        cmd = sys.executable
        wdir = "."
        options = ProcessOptions(make_inf_channels=True)
        env_str = du.B64.bytes_to_str(pickle.dumps(rlock, protocol=5))

        the_descr = create(
            cmd,
            wdir,
            [__file__, "_releaser", env_str],
            None,
            options=options,
        )
        puid = the_descr.p_uid

        # wait until it has tried, counter must still be 25
        ready = multi_join([puid], timeout=None, join_all=True)

        # release 25 times
        for _ in range(25):
            rlock.release()  # cannot raise an exception
        self.assertTrue(rlock._accesscount == 0)

        # counter is now 0
        self.assertRaises(AssertionError, rlock.release)

    def test_rlock_ownership(self):
        """Test that only the calling thread owns the lock."""

        # get a recursive lock
        rlock = Lock(recursive=True)

        # acquire it
        self.assertTrue(rlock.acquire())
        self.assertTrue(rlock.is_mine())

        # still mine after pickle ?
        bytes = pickle.dumps(rlock)
        rlock2 = pickle.loads(bytes)
        self.assertTrue(rlock2.is_mine())

        # release it
        rlock.release()
        self.assertFalse(rlock.is_mine())

        # still not mine after pickle ?
        bytes = pickle.dumps(rlock)
        rlock3 = pickle.loads(bytes)
        self.assertFalse(rlock3.is_mine())

        # start a processes that acquires the lock and leaves
        cmd = sys.executable
        wdir = "."
        options = ProcessOptions(make_inf_channels=True)
        env_str = du.B64.bytes_to_str(pickle.dumps(rlock, protocol=5))

        the_descr = create(
            cmd,
            wdir,
            [__file__, "_acquirer", env_str],
            None,
            options=options,
        )
        puid = the_descr.p_uid

        ready = join(puid, timeout=None)
        self.assertTrue(ready == 0)  # process has exited

        # should not be mine and I cannot release it
        self.assertFalse(rlock.is_mine())
        self.assertRaises(AssertionError, rlock.release)

    def test_lock_contention(self):
        """Test lock correctness under contention."""

        # get a recursive lock
        lock = Lock(recursive=False)

        nprocs = max(2, cpu_count() // 8)

        # create env string
        cmd = sys.executable
        wdir = "."
        options = ProcessOptions(make_inf_channels=True)
        env_str = du.B64.bytes_to_str(pickle.dumps(lock, protocol=5))

        # start processes
        puids = []
        for _ in range(nprocs):
            the_descr = create(
                cmd,
                wdir,
                [__file__, "_burner", env_str],
                None,
                options=options,
            )
            puids.append(the_descr.p_uid)

        # let them finish
        ready = multi_join(puids, timeout=None, join_all=True)

        # check they all have exited without error
        for puid, exit_code in ready[0]:
            self.assertTrue(exit_code == 0)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "_acquirer":
        _acquirer(sys.argv[2])
    elif len(sys.argv) > 1 and sys.argv[1] == "_releaser":
        _releaser(sys.argv[2])
    elif len(sys.argv) > 1 and sys.argv[1] == "_burner":
        _burner(sys.argv[2])
    else:
        unittest.main()
