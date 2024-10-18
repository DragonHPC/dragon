#!/usr/bin/env python3

import unittest
import pickle
import sys
import ctypes
import random
import string

import dragon
from dragon.globalservices.process import create, multi_join
from dragon.infrastructure.process_desc import ProcessOptions
from dragon.native.value import Value, _SUPPORTED_TYPES
from dragon.native.queue import Queue
import dragon.utils as du
from ctypes import Structure, c_double


class Point(Structure):
    _fields_ = [("x", c_double), ("y", c_double)]


def create_value_assignment(type):

    if type is ctypes.c_char:
        return [b"0", b"1", bytes(str(random.choice(string.ascii_letters)).encode("utf-8"))]
    elif type is ctypes.c_wchar:
        return ["0", "1", str(random.choice(string.ascii_letters))]
    elif type in [ctypes.c_double, ctypes.c_float]:
        return [
            0.0,
            1.0,
            random.uniform(sys.float_info.min, sys.float_info.max),
            sys.float_info.min,
            sys.float_info.max,
        ]
    else:
        return [0, 1, random.randint(-sys.maxsize - 1, sys.maxsize), -sys.maxsize - 1, sys.maxsize]


def test_value(args):

    bytes = du.B64.str_to_bytes(args)
    value, parent_queue, child_queue, values_assignment = pickle.loads(bytes)

    # For every item in values_assignment
    for val in values_assignment:
        # set value.value to val
        value.value = val
        # put True into child queue
        child_queue.put(True)
        # wait on the parent queue and exit cleanly
        assert parent_queue.get(timeout=None)


class TestValue(unittest.TestCase):
    def test_requirement_1_1(self):
        """typedef_or_type type checking"""
        self.assertRaises(AttributeError, Value, (0))

    def test_requirement_1_2(self):
        """typedef_or_type type checking"""
        self.assertRaises(AttributeError, Value, (str("v")))

    def test_requirement_1_3(self):
        """typedef_or_type type checking"""
        self.assertRaises(AttributeError, Value, ())

    def test_structure(self):
        """test that Value can handle Structure"""
        v = Value(Point, (10, 10))
        self.assertEqual(v.value.x, 10, "Assignment to x in Point did not happen properly")
        self.assertEqual(v.value.y, 10, "Assignment to y in Point did not happen properly")
        v.value.x = 100
        v.value.y = 100
        self.assertEqual(v.value.x, 100, "Assignment to x in Point did not happen properly")
        self.assertEqual(v.value.y, 100, "Assignment to y in Point did not happen properly")

    def test_float_power(self):
        float_value = Value(c_double, 1.0 / 3.0)
        float_value.value **= 2
        self.assertEqual(
            (1.0 / 3.0) ** 2, float_value.value, "There is an issue with raising floats to the power of 2"
        )

    def test_ping_pong(self):
        """queue ping pong between 2 processes that tests value assignment in
        parent and child processes"""

        for type in _SUPPORTED_TYPES:
            # Create a list of values to be tested values_assignment: bytes, char, int, and float
            values_assignment = create_value_assignment(type)

            # Create a Value v of type type
            value = Value(typecode_or_type=type)

            # Start a process, hand over v, a queue q and the list of value to be tested values_assignment
            cmd = sys.executable
            wdir = "."
            options = ProcessOptions(make_inf_channels=True)
            # naming the queues based on which process puts true in the queue
            parent_queue, child_queue = Queue(joinable=False), Queue(joinable=False)
            puids = []
            env_str = du.B64.bytes_to_str(
                pickle.dumps([value, parent_queue, child_queue, values_assignment], protocol=5)
            )
            proc = create(
                cmd,
                wdir,
                [__file__, "test_value", env_str],
                None,
                options=options,
            )

            # For every item in the list of values to be tested
            for val in values_assignment:

                # wait on the child queue _ = q.get(timeout=None)
                self.assertTrue(child_queue.get(timeout=None))

                # assert that value.value is equal to the current item
                self.assertEqual(value.value, val)

                # put True into the parent queue q.put(True)
                parent_queue.put(True)

            puids.append(proc.p_uid)

        # join on the puid of the process
        ready = multi_join(puids)

        # check the exit code of the process from ready = multi_join(puids) from ready[0][0]
        self.assertEqual(ready[0][0][1], 0)


if __name__ == "__main__":

    if len(sys.argv) > 1 and sys.argv[1] == "test_value":
        test_value(sys.argv[2])
    else:
        unittest.main()
