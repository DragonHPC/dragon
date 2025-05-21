#!/usr/bin/env python3

import shim_dragon_paths
import unittest
from utils.test_basic_mempool import (
    MemPoolCreateTest,
    MemoryPoolAllocTest,
    MemoryPoolAllocNoSetupTests,
    MemoryPoolAttachTests,
)

from utils.test_mempool import MemPoolTest
from utils.test_pyheap import PHeapTest
from utils.test_logging import LoggingTest, TestLogHandler
from utils.test_logging import TestLoggingSubprocesses

# TEMPORARY HACK: somehow the use of parameterized in these tests
# seem to be interfering with unittests's discovery on import.
# This brutal method solves it for the time being but:
# TODO: reorganize the tests overall
with open("utils/test_locks.py") as fh:
    exec(fh.read())

if __name__ == "__main__":
    unittest.main()
