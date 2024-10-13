#!/usr/bin/env python3

import dragon
import unittest
from dragon.native.lock import (
    Lock,
)
from dragon.mpbridge.sharedctypes import (
    Value,
)
import ctypes
from ctypes import Structure, c_double
from multiprocessing import Process


def modify(value):
    value.value **= 2


class Point(Structure):
    _fields_ = [("x", c_double), ("y", c_double)]


class TestValue(unittest.TestCase):
    def test_requirement_1_1(
        self,
    ):
        """tests that the attributes get_obj and get_lock are not available"""
        self.assertFalse(
            hasattr(
                Value(
                    "c",
                    lock=False,
                ),
                "get_lock",
            )
        )
        self.assertFalse(
            hasattr(
                Value(
                    "c",
                    lock=False,
                ),
                "get_obj",
            )
        )

    def test_requirement_1_2(
        self,
    ):
        """tests that the rlock is set properly"""
        val = Value(
            "c",
            lock=True,
        )
        self.assertIsInstance(
            val.get_lock(),
            type(Lock(recursive=True)),
        )

    def test_structure(self):
        """test that Value can handle Structure"""
        v = Value(Point, (10, 10))
        self.assertEqual(v.value.x, 10, "Assignment to x in Point did not happen properly")
        self.assertEqual(v.value.y, 10, "Assignment to y in Point did not happen properly")
        v.value.x = 100
        v.value.y = 100
        self.assertEqual(v.value.x, 100, "Assignment to x in Point did not happen properly")
        self.assertEqual(v.value.y, 100, "Assignment to y in Point did not happen properly")

    def proc_test_float_power(self):
        value = Value(c_double, 1.0 / 3.0)
        p = Process(target=modify, args=(value,))
        p.start()
        p.join()
        self.assertEqual((1.0 / 3.0) ** 2, value.value)


if __name__ == "__main__":
    unittest.main()
