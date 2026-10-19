#!/usr/bin/env python3
import shim_dragon_paths
import unittest
from globalservices.test_refcounting import TestGSRefcounting
from globalservices.single_internal import SingleInternal
from globalservices.single_process_msg import SingleProcMsgChannels
from globalservices.process_api import SingleProcAPIChannels
from globalservices.group_api import GSGroupAPI

if __name__ == "__main__":
    unittest.main()
