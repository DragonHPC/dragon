"""Test multiprocessing Shared_Memory, Array, Value and Shared_ctypes
"""
import unittest

import dragon  # DRAGON import before multiprocessing

import multiprocessing
from multiprocessing import util

try:
    from multiprocessing.sharedctypes import Value, copy

    HAS_SHAREDCTYPES = True
except ImportError:
    HAS_SHAREDCTYPES = False

from common import (
    BaseTestCase,
    ProcessesMixin,
    latin,
    setUpModule,
    tearDownModule,
)


class WithProcessesTestValue(BaseTestCase, ProcessesMixin, unittest.TestCase):

    # DRAGON ALLOWED_TYPES = ('processes',)

    codes_values = [
        ("i", 4343, 24234),
        ("d", 3.625, -4.25),
        ("h", -232, 234),
        ("q", 2**33, 2**34),
        ("c", latin("x"), latin("y")),
    ]

    def setUp(self):
        if not HAS_SHAREDCTYPES:
            self.skipTest("requires multiprocessing.sharedctypes")

    @classmethod
    def _test(cls, values):
        for sv, cv in zip(values, cls.codes_values):
            sv.value = cv[2]

    def test_value(self, raw=False):
        if raw:
            values = [self.RawValue(code, value) for code, value, _ in self.codes_values]
        else:
            values = [self.Value(code, value) for code, value, _ in self.codes_values]

        for sv, cv in zip(values, self.codes_values):
            self.assertEqual(sv.value, cv[1])

        proc = self.Process(target=self._test, args=(values,))
        proc.daemon = True
        proc.start()
        proc.join()

        for sv, cv in zip(values, self.codes_values):
            self.assertEqual(sv.value, cv[2])

    def test_rawvalue(self):
        self.test_value(raw=True)

    def test_getobj_getlock(self):
        val1 = self.Value("i", 5)
        lock1 = val1.get_lock()
        obj1 = val1.get_obj()

        val2 = self.Value("i", 5, lock=None)
        lock2 = val2.get_lock()
        obj2 = val2.get_obj()

        lock = self.Lock()
        val3 = self.Value("i", 5, lock=lock)
        lock3 = val3.get_lock()
        obj3 = val3.get_obj()
        self.assertEqual(lock, lock3)

        arr4 = self.Value("i", 5, lock=False)
        self.assertFalse(hasattr(arr4, "get_lock"))
        self.assertFalse(hasattr(arr4, "get_obj"))

        self.assertRaises(AttributeError, self.Value, "i", 5, lock="navalue")

        arr5 = self.RawValue("i", 5)
        self.assertFalse(hasattr(arr5, "get_lock"))
        self.assertFalse(hasattr(arr5, "get_obj"))


# DRAGON
if __name__ == "__main__":
    setUpModule()
    unittest.main()
    tearDownModule()