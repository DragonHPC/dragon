#!/usr/bin/env python3
import unittest

import dragon
import multiprocessing as mp


def setUpModule():
    mp.set_start_method("dragon", force=True)


class TestDragonLocks(unittest.TestCase):
    def setUp(self):
        self.assertEqual(mp.get_start_method(), "dragon")

    def test_semlock_hack(self):

        rlock = mp.RLock()

        success = rlock.acquire()
        self.assertTrue(success)

        self.assertTrue(rlock._semlock._is_mine())
        self.assertTrue(1 == rlock._semlock._count())

        for i in range(13):
            self.assertTrue(rlock.acquire())
            self.assertTrue(i + 2 == rlock._semlock._count())


if __name__ == "__main__":
    unittest.main()
