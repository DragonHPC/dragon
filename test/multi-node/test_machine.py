"""This file contains Dragon multi-node acceptance tests for the
`dragon.native.machine` module. The tests scales with the total number of CPUs
reported by the allocation, i.e. it becomes tougher on larger allocations.

The test is run with `dragon test_machine.py -f -v`
"""

import unittest
import os

import dragon
from dragon.native.machine import cpu_count
import multiprocessing as mp


class TestMachineMultiNode(unittest.TestCase):
    """We need to test `cpu_count, or all other tests could be bogus."""

    def test_on_multi_node(self):

        ncpu_native = max(2, cpu_count())
        ncpu_mp = max(2, mp.cpu_count())
        ncpu_posix = int(os.sysconf("SC_NPROCESSORS_ONLN"))

        self.assertTrue(ncpu_mp > ncpu_posix, "You're not using a multi-node allocation.")
        self.assertTrue(ncpu_native > ncpu_posix, "You're not using a multi-node allocation.")


if __name__ == "__main__":
    mp.set_start_method("dragon")
    unittest.main()
