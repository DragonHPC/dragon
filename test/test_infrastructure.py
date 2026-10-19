#!/usr/bin/env python3
import shim_dragon_paths
import unittest
from infrastructure.env_parameter_tests import LaunchParameterTest
from infrastructure.newline_stream_wrapper_test import NewlineStreamWrapperTest
from infrastructure.test_dragon_config import DragonConfigTest
from infrastructure.test_dragon_version import DragonVersionTester
from infrastructure.test_gpu_desc import TestGpuDesc, TestAccelerator
from infrastructure.test_numa import TestNUMA, TestNUMALoader, TestNodeDescriptorNUMA
from infrastructure.test_gpu_stubs import (
    TestStubbedCuda,
    TestStubbedHip,
    TestStubbedZe,
    TestNoAccelerators,
    TestCudaRuntimeLibLookup,
)

if __name__ == "__main__":
    unittest.main()
