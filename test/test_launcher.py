#!/usr/bin/env python3
import shim_dragon_paths
import unittest
import logging

from launcher.test_launch_options import LaunchSelectionTest, LaunchOptionsTest
from launcher.test_network_config import FileNetworkConfigTest, SlurmNetworkConfigTest, PbsPalsNetworkConfigTest
from launcher.test_network_config import SSHNetworkConfigTest
from launcher.test_signal_handling import SigIntTest
from launcher.test_frontend_bringup import FrontendBringUpTeardownTest
from launcher.test_backend_bringup import BackendBringUpTeardownTest
from launcher.test_resilient_restart import FrontendRestartTest


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    unittest.main()
