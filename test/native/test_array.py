#!/usr/bin/env python3

import unittest
import pickle
import sys
import ctypes
import random
import string

from dragon.globalservices.process import create, multi_join
from dragon.infrastructure.process_desc import ProcessOptions
from dragon.native.array import Array, _SUPPORTED_TYPES
from dragon.native.queue import Queue
from dragon.native.process import Process
from dragon.native.lock import Lock
import dragon.utils as du
from ctypes import Structure, c_double


class Point(Structure):
    _fields_ = [("x", c_double), ("y", c_double)]


def create_array_assignment(type):

    if type is ctypes.c_char:
        return [b"0", b"1", bytes(str(random.choice(string.ascii_letters)).encode("utf-8"))]
    elif type is ctypes.c_wchar:
        # wchar can take up to 4 bytes on most machines. We need to test at least up to 4 charcters
        return [
            "0",
            "1",
            str(random.choice(string.ascii_letters)),
            "".join(str(random.choice(string.ascii_letters)) for i in range(2)),
            "".join(str(random.choice(string.ascii_letters)) for i in range(3)),
            "".join(str(random.choice(string.ascii_letters)) for i in range(4)),
        ]
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


def test_array(args):
    bytes = du.B64.str_to_bytes(args)
    array, array_assignment, parent_queue, child_queue = pickle.loads(bytes)

    # set array.size_or_initializer to array_assignment
    array[:] = array_assignment
    # put True into queue
    child_queue.put(True)
    # wait on the queue and exit cleanly
    assert parent_queue.get(timeout=None)


def square(A):
    """Square a Point structure"""

    for a in A:
        a.x **= 2
        a.y **= 2


class TestArray(unittest.TestCase):
    def test_typedef_or_type(self):
        """typedef_or_type type checking"""
        self.assertRaises(AttributeError, lambda: Array(typecode_or_type=0, size_or_initializer=0))
        self.assertRaises(AttributeError, lambda: Array(typecode_or_type="v", size_or_initializer=0))
        self.assertRaises(AttributeError, lambda: Array(typecode_or_type=(), size_or_initializer=0))

    def test_array_assignment(self):
        """check array assignment"""
        # Do test for input list
        self.assertEqual(Array("i", [1, 2, 3])[:], [1, 2, 3])

        # Do test for iterator
        arr = Array("i", 10)
        arr[:] = range(10)
        self.assertEqual(list(arr), list(range(10)))

    def test_structure(self):
        """tests that Structure works in array"""
        lock = Lock()

        A = Array(Point, [(1.875, -6.25), (-5.75, 2.0), (2.375, 9.5)], lock=lock)

        for a in A:
            a.x **= 2
            a.y **= 2

        self.assertEqual(
            [(a.x, a.y) for a in A],
            [(3.515625, 39.0625), (33.0625, 4.0), (5.640625, 90.25)],
            "Array assignment does not work for structure",
        )

    def test_pickled_structure(self):
        """tests that Structure works in array when pickled"""
        lock = Lock()

        A = Array(Point, [(1.875, -6.25), (-5.75, 2.0), (2.375, 9.5)], lock=lock)

        p = Process(target=square, args=(A,))
        p.daemon = True
        p.start()
        p.join()

        self.assertEqual(
            [(a.x, a.y) for a in A],
            [(3.515625, 39.0625), (33.0625, 4.0), (5.640625, 90.25)],
            "Array assignment does not work for structure",
        )

    def test_char_value_raw(self):
        """Tests that char type returns a bytes array and only char does"""

        x = Array("c", 5)
        x.value = b"hello"
        y = Array("i", range(10))

        self.assertEqual(x.value, b"hello")
        self.assertEqual(x.raw, b"hello")

        with self.assertRaises(AttributeError):
            y.value

    def test_ping_pong(self):
        """queue ping pong between 2 processes that tests array assignment in
        parent and child processes"""

        for type in _SUPPORTED_TYPES:
            # Create a list of values to be tested array_assignment: bytes, char, int, and float
            array_assignment = create_array_assignment(type)

            # Create a array of type
            array = Array(typecode_or_type=type, size_or_initializer=len(array_assignment))

            # Start a process, hand over v, a queue q and the array to be tested array_assignment
            cmd = sys.executable
            wdir = "."
            options = ProcessOptions(make_inf_channels=True)
            parent_queue, child_queue = Queue(joinable=False), Queue(joinable=False)
            puids = []
            env_str = du.B64.bytes_to_str(
                pickle.dumps([array, array_assignment, parent_queue, child_queue], protocol=5)
            )
            proc = create(
                cmd,
                wdir,
                [__file__, "test_array", env_str],
                None,
                options=options,
            )

            # wait on the queue _ = q.get(timeout=None)
            self.assertTrue(child_queue.get(timeout=None))

            # check each element in the array to make sure that the array assignment is correct
            self.assertEqual(list(array[:]), array_assignment)

            for i in range(len(array)):
                self.assertEqual(array[i], array_assignment[i])

            # put True into the queue q.put(True)
            parent_queue.put(True)

            puids.append(proc.p_uid)

        # join on the puid of the process
        ready = multi_join(puids)

        # check the exit code of the process from ready = multi_join(puids) from ready[0][0]
        self.assertEqual(ready[0][0][1], 0)


if __name__ == "__main__":

    if len(sys.argv) > 1 and sys.argv[1] == "test_array":
        test_array(sys.argv[2])
    else:
        unittest.main()
