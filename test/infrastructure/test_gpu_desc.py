#!/usr/bin/env python3

import unittest

import dragon.globalservices.policy_eval as dgpol
from dragon.infrastructure.gpu_desc import AcceleratorDescriptor, AccVendor, AccEnvStr
from dragon.infrastructure.policy import Policy, GS_DEFAULT_POLICY
from dragon.infrastructure.node_desc import NodeDescriptor

class TestGpuDesc(unittest.TestCase):

    def setUp(self):
        self.nvidia = AcceleratorDescriptor(vendor = AccVendor.NVIDIA,
                                            device_list = [0,1,2],
                                            env_str = AccEnvStr.NVIDIA)
        self.amd = AcceleratorDescriptor(vendor = AccVendor.AMD,
                                            device_list = [0,1,2],
                                            env_str = AccEnvStr.AMD)
        self.node = NodeDescriptor()
        self.node.host_name = "mock_node"
        self.node.host_id = 1
        self.node.h_uid = 1
        self.node.num_cpus = 8

    def test_nvidia(self):
        self.node.accelerators = self.nvidia
        p = Policy(gpu_affinity=[1]) # Make sure we get the proper device other than 0
        eval = dgpol.PolicyEvaluator([self.node], GS_DEFAULT_POLICY)
        layout = eval.evaluate([p])
        self.assertEqual(layout.accelerator_env, self.nvidia.env_str)
        self.assertEqual(layout.gpu_core, [1])

    def test_amd(self):
        self.node.accelerators = self.amd
        p = Policy(gpu_affinity=[1]) # Make sure we get the proper device other than 0
        eval = dgpol.PolicyEvaluator([self.node], GS_DEFAULT_POLICY)
        layout = eval.evaluate([p])
        self.assertEqual(layout.accelerator_env, self.amd.env_str)
        self.assertEqual(layout.gpu_core, [1])
    