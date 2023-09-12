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


if __name__ == "__main__":
    unittest.main()
