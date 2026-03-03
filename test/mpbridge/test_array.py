#!/usr/bin/env python3

import dragon
import unittest
from dragon.native.lock import Lock
from multiprocessing import Process
from dragon.mpbridge.sharedctypes import Array
from ctypes import Structure, c_double


class Point(Structure):
    _fields_ = [("x", c_double), ("y", c_double)]


def modify(A):
    for a in A:
        a.x **= 2
        a.y **= 2


class TestArray(unittest.TestCase):
    def test_requirement_1_1(
        self,
    ):
        """tests that the attributes get_obj and get_lock are not available"""
        self.assertFalse(
            hasattr(
                Array(
                    "c",
                    range(10),
                    lock=False,
                ),
                "get_lock",
            )
        )
        self.assertFalse(
            hasattr(
                Array(
                    "c",
                    range(10),
                    lock=False,
                ),
                "get_obj",
            )
        )

    def test_requirement_1_2(
        self,
    ):
        """tests that the rlock is set properly"""
        array = Array(
            "c",
            range(10),
            lock=True,
        )
        lock = array.get_lock()
        self.assertIsInstance(
            lock,
            type(Lock(recursive=True)),
        )
        # tests recursive attributes
        self.assertTrue(
            hasattr(
                lock,
                "_recursive",
            )
        )
        self.assertTrue(
            hasattr(
                lock,
                "is_recursive",
            )
        )

    def test_structure(
        self,
    ):
        """tests that Structure works in array"""
        lock = Lock()

        A = Array(Point, [(1.875, -6.25), (-5.75, 2.0), (2.375, 9.5)], lock=lock)

        p = Process(target=modify, args=(A,))
        p.start()
        p.join()

        self.assertEqual(
            [(a.x, a.y) for a in A],
            [(3.515625, 39.0625), (33.0625, 4.0), (5.640625, 90.25)],
            "Array assignment does not work for structure",
        )


if __name__ == "__main__":
    unittest.main()
