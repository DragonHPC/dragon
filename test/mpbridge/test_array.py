#!/usr/bin/env python3

import dragon
import unittest
from dragon.native.lock import Lock
from dragon.mpbridge.sharedctypes import Array
import ctypes
import multiprocessing as mp


class TestArray(unittest.TestCase):
    def test_requirement_1_1(self,):
        """tests that the attributes get_obj and get_lock are not available"""
        self.assertFalse(hasattr(Array("c",range(10),lock=False,),"get_lock",))
        self.assertFalse(hasattr(Array("c",range(10),lock=False,),"get_obj",))

    def test_requirement_1_2(self,):
        """tests that the rlock is set properly"""
        array = Array("c", range(10), lock=True,)
        lock = array.get_lock()
        self.assertIsInstance(lock, type(Lock(recursive=True)),)
        #tests recursive attributes
        self.assertTrue(hasattr(lock,"_recursive",))
        self.assertTrue(hasattr(lock,"is_recursive",))


if __name__ == "__main__":
    unittest.main()
