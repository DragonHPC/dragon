"""Test multiprocessing Array()
"""
import unittest
import array

try:
    from ctypes import c_int
except ImportError:
    Structure = object
    c_int = c_double = c_longlong = None

import dragon  # DRAGON import before multiprocessing

import multiprocessing
from multiprocessing import util

from common import (
    BaseTestCase,
    ProcessesMixin,
    setUpModule,
    tearDownModule,
)


class WithProcessesTestArray(BaseTestCase, ProcessesMixin, unittest.TestCase):

    # DRAGON ALLOWED_TYPES = ('processes',)

    @classmethod
    def f(cls, seq):
        for i in range(1, len(seq)):
            seq[i] += seq[i - 1]

    @unittest.skipIf(c_int is None, "requires _ctypes")
    def test_array(self, raw=False):
        seq = [680, 626, 934, 821, 150, 233, 548, 982, 714, 831]
        if raw:
            arr = self.RawArray("i", seq)
        else:
            arr = self.Array("i", seq)

        self.assertEqual(len(arr), len(seq))
        self.assertEqual(arr[3], seq[3])
        self.assertEqual(list(arr[2:7]), list(seq[2:7]))

        arr[4:8] = seq[4:8] = array.array("i", [1, 2, 3, 4])

        self.assertEqual(list(arr[:]), seq)

        self.f(seq)

        p = self.Process(target=self.f, args=(arr,))
        p.daemon = True
        p.start()
        p.join()

        self.assertEqual(list(arr[:]), seq)

    @unittest.skipIf(c_int is None, "requires _ctypes")
    def test_array_from_size(self):
        size = 10
        # Test for zeroing (see issue #11675).
        # The repetition below strengthens the test by increasing the chances
        # of previously allocated non-zero memory being used for the new array
        # on the 2nd and 3rd loops.
        for _ in range(3):
            arr = self.Array("i", size)
            self.assertEqual(len(arr), size)
            self.assertEqual(list(arr), [0] * size)
            arr[:] = range(10)
            self.assertEqual(list(arr), list(range(10)))
            del arr

    @unittest.skipIf(c_int is None, "requires _ctypes")
    def test_rawarray(self):
        self.test_array(raw=True)

    @unittest.skipIf(c_int is None, "requires _ctypes")
    def test_getobj_getlock_obj(self):
        arr1 = self.Array("i", list(range(10)))
        lock1 = arr1.get_lock()
        obj1 = arr1.get_obj()

        arr2 = self.Array("i", list(range(10)), lock=None)
        lock2 = arr2.get_lock()
        obj2 = arr2.get_obj()

        lock = self.Lock()
        arr3 = self.Array("i", list(range(10)), lock=lock)
        lock3 = arr3.get_lock()
        obj3 = arr3.get_obj()
        self.assertEqual(lock, lock3)

        arr4 = self.Array("i", range(10), lock=False)
        self.assertFalse(hasattr(arr4, "get_lock"))
        self.assertFalse(hasattr(arr4, "get_obj"))
        self.assertRaises(AttributeError, self.Array, "i", range(10), lock="notalock")

        arr5 = self.RawArray("i", range(10))
        self.assertFalse(hasattr(arr5, "get_lock"))
        self.assertFalse(hasattr(arr5, "get_obj"))


# DRAGON
if __name__ == "__main__":
    setUpModule()
    unittest.main()
    tearDownModule()
