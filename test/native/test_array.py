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
from dragon.native.array import Array, _TYPECODE_TO_TYPE, _SUPPORTED_TYPES
from dragon.native.queue import Queue
import dragon.utils as du


def create_array_assignment(type):

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


def test_array(args):

    bytes = du.B64.str_to_bytes(args)
    array, array_assignment, parent_queue, child_queue = pickle.loads(bytes)

    # set array.size_or_initializer to array_assignment
    array[:] = array_assignment
    # put True into queue
    child_queue.put(True)
    # wait on the queue and exit cleanly
    assert parent_queue.get(timeout=None)


class TestArray(unittest.TestCase):
    def test_typedef_or_type(self):
        """typedef_or_type type checking"""
        self.assertRaises(AttributeError, lambda: Array(typecode_or_type=0, size_or_initializer=0))
        self.assertRaises(AttributeError, lambda: Array(typecode_or_type="v", size_or_initializer=0))
        self.assertRaises(AttributeError, lambda: Array(typecode_or_type=(), size_or_initializer=0))

    def test_array_assignment(self):
        """check array assignment"""
        self.assertEqual(Array("i", [1, 2, 3])[:], [1, 2, 3])
        arr = Array("i", 10)
        arr[:] = range(10)
        self.assertEqual(list(arr), list(range(10)))

    def test_ping_pong(self):
        """queue ping pong between 2 processes that tests array assignment in
        parent and child processes"""

        for type in _SUPPORTED_TYPES:

            # Create a list of values to be tested array_assignment: bytes, char, int, and float
            array_assignment = create_array_assignment(type)

            # Create a array of type
            array = Array(typecode_or_type=type, size_or_initializer=[])

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

            #put True into the queue q.put(True)
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
