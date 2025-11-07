#!/usr/bin/env python3
import shim_dragon_paths
import unittest
import multiprocessing as mp

from native.test_process_group import TestDragonNativeProcessGroup
from native.test_process import TestDragonNativeProcess
from native.test_semaphore import TestSemaphore
from native.test_machine import TestMachineSingle
from native.test_value import TestValue
from native.test_array import TestArray
from native.test_barrier import TestBarrier
from native.test_event import TestEvent
from native.test_lock import TestLock
from native.test_queue import TestQueue
from native.test_redirection import TestIORedirection
from native.test_ddict import TestDDict
from native.test_pool import TestDragonNativePool
from native.test_ddict_c_driver import TestDDictC
from native.test_ddict_cpp_driver import TestDDictCPP
from native.test_barrier_cpp_driver import TestBarrierCPP
from native.test_semaphore_cpp_driver import TestSemaphoreCPP
from native.test_queue_cpp_driver import TestQueueCPP


if __name__ == "__main__":
    mp.set_start_method("dragon")
    unittest.main()
