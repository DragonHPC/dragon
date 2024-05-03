#!/usr/bin/env python3
import shim_dragon_paths
import unittest

from mpbridge.test_pipe_with_process import SendReceiveTest
from mpbridge.test_mpbridge_context_wait import TestDragonContextWait
from mpbridge.test_mpbridge_basic import TestStartMethod
from mpbridge.test_queue import TestQueue
from mpbridge.test_condition import TestCondition
from mpbridge.test_lock import TestDragonLocks
from mpbridge.test_process import TestMPBridgeProcess
from mpbridge.test_pool import TestMPBridgePool
from mpbridge.test_api import TestMultiprocessingAPI, TestMultiprocessingInternalPatching
# from mpbridge.test_barrier import TestBarrier


def setUpModule():
    import dragon
    import multiprocessing
    multiprocessing.set_start_method('dragon')


if __name__ == "__main__":
    unittest.main()
