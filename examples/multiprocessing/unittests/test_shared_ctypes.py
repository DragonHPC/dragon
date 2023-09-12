"""Test multiprocessing Shared_Memory, Array, Value and Shared_ctypes
"""
import unittest

try:
    from ctypes import Structure, c_int, c_double, c_longlong
except ImportError:
    Structure = object
    c_int = c_double = c_longlong = None

import dragon  # DRAGON import before multiprocessing

import multiprocessing

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


class _Foo(Structure):
    _fields_ = [
        ("x", c_int),
        ("y", c_double),
        (
            "z",
            c_longlong,
        ),
    ]

@unittest.skip("DRAGON: Not Implemented")
class WithProcessesTestSharedCTypes(BaseTestCase, ProcessesMixin, unittest.TestCase):

    # DRAGON ALLOWED_TYPES = ('processes',)

    def setUp(self):
        if not HAS_SHAREDCTYPES:
            self.skipTest("requires multiprocessing.sharedctypes")

    @classmethod
    def _double(cls, x, y, z, foo, arr, string):
        x.value *= 2
        y.value *= 2
        z.value *= 2
        foo.x *= 2
        foo.y *= 2
        string.value *= 2
        for i in range(len(arr)):
            arr[i] *= 2

    @unittest.skip("bug filed PE-40919")
    def test_sharedctypes(self, lock=False):
        x = Value("i", 7, lock=lock)
        y = Value(c_double, 1.0 / 3.0, lock=lock)
        z = Value(c_longlong, 2**33, lock=lock)
        foo = Value(_Foo, 3, 2, lock=lock)
        arr = self.Array("d", list(range(10)), lock=lock)
        string = self.Array("c", 20, lock=lock)
        string.value = latin("hello")

        p = self.Process(target=self._double, args=(x, y, z, foo, arr, string))
        p.daemon = True
        p.start()
        p.join()

        self.assertEqual(x.value, 14)
        self.assertAlmostEqual(y.value, 2.0 / 3.0)
        self.assertEqual(z.value, 2**34)
        self.assertEqual(foo.x, 6)
        self.assertAlmostEqual(foo.y, 4.0)
        for i in range(10):
            self.assertAlmostEqual(arr[i], i * 2)
        self.assertEqual(string.value, latin("hellohello"))

    def test_synchronize(self):
        self.test_sharedctypes(lock=True)

    def test_copy(self):
        foo = _Foo(2, 5.0, 2**33)
        bar = copy(foo)
        foo.x = 0
        foo.y = 0
        foo.z = 0
        self.assertEqual(bar.x, 2)
        self.assertAlmostEqual(bar.y, 5.0)
        self.assertEqual(bar.z, 2**33)


# DRAGON
if __name__ == "__main__":
    setUpModule()
    unittest.main()
    tearDownModule()
