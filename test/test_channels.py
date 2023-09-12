#!/usr/bin/env python3

import shim_dragon_paths
import multiprocessing as mp
import unittest
from channels_subtests.test_basic_channels import ChannelCreateTest, ChannelTests

if __name__ == '__main__':
    mp.set_start_method("spawn", force=True)
    unittest.main()
