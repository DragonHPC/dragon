from time import sleep
import unittest
import pickle
import os

import dragon
import multiprocessing as mp


class TestCondition(unittest.TestCase):
    def setUp(self):
        self.assertEqual(mp.get_start_method(), "dragon")

    @staticmethod
    def cond_wait(cond: mp.Condition, queue: mp.Queue, item: object) -> None:
        queue.put(item)

        assert cond.acquire(timeout=None)
        cond.wait(timeout=None)
        cond.release()

        queue.put(item)

    # Test cases
    def test_basic_condition_test(self):
        """
        Basic condition test that checks that acquire, wait, and release work.
        """
        condition = mp.Condition()
        result = condition.acquire()
        self.assertTrue(result)
        condition.release()

    def test_process_multiple_notify(self):
        """
        Condition test that passes a condition to multiple processes. Notify is called
        by the parent and the first process exits.
        The other processes continue to run until is notify_all called on them.
        """

        cond = mp.Condition()
        queue = mp.Queue()
        processes = []

        nproc = 4

        # start a bunch of processes
        for i in range(nproc):
            proc = mp.Process(target=self.cond_wait, args=(cond, queue, i))
            proc.start()
            processes.append(proc)
            j = queue.get(timeout=10)  # make sure process is actually running

        # notify one of them
        cond.acquire()
        cond.notify()
        cond.release()

        # check that one of them exited
        i = queue.get(timeout=10)
        processes[i].join(timeout=None)
        self.assertTrue(processes[i].exitcode == 0)

        # notify them all and check they all joined
        cond.acquire()
        cond.notify_all()
        cond.release()

        for _ in range(nproc - 1):
            i = queue.get(timeout=10)
            processes[i].join(timeout=None)
            self.assertTrue(processes[i].exitcode == 0)

    def test_lock_hack_for_condition(self):
        """Check that we have correctly faked a _semlock object in _lock"""
        cond = mp.Condition()

        self.assertTrue(cond.acquire == cond._lock.acquire)
        self.assertTrue(cond.release == cond._lock.release)

        success = cond.acquire()

        self.assertTrue(cond._lock._semlock._is_mine())
        self.assertTrue(1 == cond._lock._semlock._count())

        bytes = pickle.dumps(cond)
        cond2 = pickle.loads(bytes)
        self.assertTrue(cond2._lock._semlock._is_mine())


if __name__ == "__main__":

    mp.set_start_method("dragon", force=True)
    unittest.main()
