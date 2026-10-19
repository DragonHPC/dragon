#!/usr/bin/env python3

import unittest
from unittest.mock import patch

from dragon.accelerators import Accelerator
from dragon.rc import DragonError

import dragon.globalservices.policy_eval as dgpol
from dragon import Policy
from dragon.infrastructure.gpu_desc import (
    AcceleratorDescriptor,
    AccEnvStr,
    AccVendor,
    apply_gpu_affinity,
    find_accelerators,
)
from dragon.infrastructure.node_desc import NodeDescriptor

# dragonGPUBackend_t enum value from src/lib/gpu/gpu_c_api.h (not exposed to Python)
DRAGON_GPU_BACKEND_UNDETECTED = 0
DRAGON_GPU_BACKEND_CUDA = 1
DRAGON_GPU_BACKEND_HIP = 2
DRAGON_GPU_BACKEND_ZE = 3
DRAGON_GPU_BACKEND_END = 4


class TestGpuDesc(unittest.TestCase):
    def test_affinity_masks(self):
        cases = [
            (AccEnvStr.NVIDIA, {AccEnvStr.NVIDIA: "3,6"}, [0, 1], "3,6"),
            (AccEnvStr.NVIDIA, {AccEnvStr.NVIDIA: "3,6"}, [1], "6"),
            (AccEnvStr.NVIDIA, {AccEnvStr.NVIDIA: "GPU-first,GPU-second"}, [1], "GPU-second"),
            (AccEnvStr.AMD, {AccEnvStr.AMD: "3,6"}, [1], "6"),
            (AccEnvStr.AMD, {AccEnvStr.HIP: "3,6"}, [1], "6"),
            (AccEnvStr.AMD, {AccEnvStr.AMD: "3,6", AccEnvStr.HIP: "1"}, [0], "6"),
            (AccEnvStr.INTEL, {AccEnvStr.INTEL: "1.0,1.1,4.1"}, [1.1], "4.1"),
            (AccEnvStr.INTEL, {AccEnvStr.INTEL: "3,6", "ZE_FLAT_DEVICE_HIERARCHY": "FLAT"}, [1], "6"),
            (AccEnvStr.INTEL, {AccEnvStr.INTEL: "3,6"}, [1.0, 1.1], "6.0,6.1"),
            (AccEnvStr.NVIDIA, {}, [0, 1], "0,1"),
        ]
        for env_str, inherited_env, devices, expected in cases:
            with self.subTest(env_str=env_str, inherited_env=inherited_env, devices=devices):
                env = dict(inherited_env)
                apply_gpu_affinity(env, devices, env_str, inherited_env)
                self.assertEqual(env[env_str], expected)
                if env_str == AccEnvStr.AMD:
                    self.assertNotIn(AccEnvStr.HIP, env)

    def setUp(self):
        self.nvidia = AcceleratorDescriptor(vendor=AccVendor.NVIDIA, device_list=[0, 1, 2], env_str=AccEnvStr.NVIDIA)
        self.amd = AcceleratorDescriptor(vendor=AccVendor.AMD, device_list=[0, 1, 2], env_str=AccEnvStr.AMD)
        self.intel = AcceleratorDescriptor(vendor=AccVendor.INTEL, device_list=[0, 1, 2], env_str=AccEnvStr.INTEL)
        self.node = NodeDescriptor()
        self.node.host_name = "mock_node"
        self.node.host_id = 1
        self.node.h_uid = 1
        self.node.num_cpus = 8

    def test_nvidia(self):
        self.node.accelerators = self.nvidia
        p = Policy(gpu_affinity=[1])  # Make sure we get the proper device other than 0
        eval = dgpol.PolicyEvaluator([self.node], Policy.global_policy())
        layout = eval.evaluate([p])
        self.assertEqual(layout[0].accelerator_env, self.nvidia.env_str)
        self.assertEqual(layout[0].gpu_core, [1])

    def test_amd(self):
        self.node.accelerators = self.amd
        p = Policy(gpu_affinity=[1])  # Make sure we get the proper device other than 0
        eval = dgpol.PolicyEvaluator([self.node], Policy.global_policy())
        layout = eval.evaluate([p])
        self.assertEqual(layout[0].accelerator_env, self.amd.env_str)
        self.assertEqual(layout[0].gpu_core, [1])

    def test_intel(self):
        self.node.accelerators = self.intel
        p = Policy(gpu_affinity=[1])  # Make sure we get the proper device other than 0
        eval = dgpol.PolicyEvaluator([self.node], Policy.global_policy())
        layout = eval.evaluate([p])
        self.assertEqual(layout[0].accelerator_env, self.intel.env_str)
        self.assertEqual(layout[0].gpu_core, [1])


class TestAccelerator(unittest.TestCase):
    @patch.object(Accelerator, "_get_gpu_handle", return_value=(DragonError.SUCCESS, 0))
    @patch.object(Accelerator, "_setup_gpu", return_value=(DragonError.SUCCESS, 0))
    @patch.object(Accelerator, "_get_backend_type", return_value=(DragonError.SUCCESS, DRAGON_GPU_BACKEND_CUDA))
    @patch.object(Accelerator, "_get_device_list", return_value=(DragonError.SUCCESS, []))
    @patch.object(Accelerator, "_get_device_count", return_value=0)
    def test_nvidia_libs_no_devices(
        self, mock_device_count, mock_device_list, mock_backend_type, mock_setup_gpu, mock_gpu_handle
    ):
        acc = Accelerator()

        self.assertEqual(acc.get_vendor(), AccVendor.NVIDIA)
        self.assertEqual(acc.get_device_count(), 0)

        found = find_accelerators()
        self.assertEqual(
            found, AcceleratorDescriptor(vendor=AccVendor.NVIDIA, device_list=[], env_str=AccEnvStr.NVIDIA)
        )

    @patch.object(Accelerator, "_get_gpu_handle", return_value=(DragonError.SUCCESS, 0))
    @patch.object(Accelerator, "_setup_gpu", return_value=(DragonError.SUCCESS, 0))
    @patch.object(Accelerator, "_get_backend_type", return_value=(DragonError.SUCCESS, DRAGON_GPU_BACKEND_CUDA))
    @patch.object(Accelerator, "_get_device_list", return_value=(DragonError.SUCCESS, [0, 1, 2, 3]))
    @patch.object(Accelerator, "_get_device_count", return_value=4)
    @patch.object(
        Accelerator, "_get_uuid", side_effect=lambda handle, devID: (DragonError.SUCCESS, f"uuid-{devID}".encode())
    )
    def test_nvidia_libs_with_devices(
        self, mock_get_uuid, mock_device_count, mock_device_list, mock_backend_type, mock_setup_gpu, mock_gpu_handle
    ):
        acc = Accelerator()

        self.assertEqual(acc.get_vendor(), AccVendor.NVIDIA)
        self.assertEqual(acc.get_device_count(), 4)
        self.assertEqual(acc.get_device_uuid(0), f"uuid-0")
        self.assertEqual(acc.get_device_uuid(1), f"uuid-1")
        self.assertEqual(acc.get_device_uuid(2), f"uuid-2")
        self.assertEqual(acc.get_device_uuid(3), f"uuid-3")
        self.assertEqual(acc.get_device_list(), [0, 1, 2, 3])

        found = find_accelerators()
        self.assertEqual(
            found, AcceleratorDescriptor(vendor=AccVendor.NVIDIA, device_list=[0, 1, 2, 3], env_str=AccEnvStr.NVIDIA)
        )

    @patch.object(Accelerator, "_get_gpu_handle", return_value=(DragonError.SUCCESS, 0))
    @patch.object(Accelerator, "_setup_gpu", return_value=(DragonError.SUCCESS, 0))
    @patch.object(Accelerator, "_get_backend_type", return_value=(DragonError.SUCCESS, DRAGON_GPU_BACKEND_HIP))
    @patch.object(Accelerator, "_get_device_list", return_value=(DragonError.SUCCESS, []))
    @patch.object(Accelerator, "_get_device_count", return_value=0)
    def test_amd_libs_no_devices(
        self, mock_device_count, mock_device_list, mock_backend_type, mock_setup_gpu, mock_gpu_handle
    ):
        acc = Accelerator()

        self.assertEqual(acc.get_vendor(), AccVendor.AMD)
        self.assertEqual(acc.get_device_count(), 0)

        found = find_accelerators()
        self.assertEqual(found, AcceleratorDescriptor(vendor=AccVendor.AMD, device_list=[], env_str=AccEnvStr.AMD))

    @patch.object(Accelerator, "_get_gpu_handle", return_value=(DragonError.SUCCESS, 0))
    @patch.object(Accelerator, "_setup_gpu", return_value=(DragonError.SUCCESS, 0))
    @patch.object(Accelerator, "_get_backend_type", return_value=(DragonError.SUCCESS, DRAGON_GPU_BACKEND_HIP))
    @patch.object(Accelerator, "_get_device_list", return_value=(DragonError.SUCCESS, [0, 1, 2, 3]))
    @patch.object(Accelerator, "_get_device_count", return_value=4)
    @patch.object(
        Accelerator, "_get_uuid", side_effect=lambda handle, devID: (DragonError.SUCCESS, f"uuid-{devID}".encode())
    )
    def test_amd_libs_with_devices(
        self, mock_get_uuid, mock_device_count, mock_device_list, mock_backend_type, mock_setup_gpu, mock_gpu_handle
    ):
        acc = Accelerator()

        self.assertEqual(acc.get_device_uuid(2), f"uuid-2")
        self.assertEqual(acc.get_device_uuid(3), f"uuid-3")
        self.assertEqual(acc.get_device_list(), [0, 1, 2, 3])

        found = find_accelerators()
        self.assertEqual(
            found, AcceleratorDescriptor(vendor=AccVendor.AMD, device_list=[0, 1, 2, 3], env_str=AccEnvStr.AMD)
        )

    @patch.object(Accelerator, "_get_gpu_handle", return_value=(DragonError.SUCCESS, 0))
    @patch.object(Accelerator, "_setup_gpu", return_value=(DragonError.SUCCESS, 0))
    @patch.object(Accelerator, "_get_backend_type", return_value=(DragonError.SUCCESS, DRAGON_GPU_BACKEND_ZE))
    @patch.object(Accelerator, "_get_device_list", return_value=(DragonError.SUCCESS, []))
    @patch.object(Accelerator, "_get_device_count", return_value=0)
    def test_intel_libs_no_devices(
        self, mock_device_count, mock_device_list, mock_backend_type, mock_setup_gpu, mock_gpu_handle
    ):
        acc = Accelerator()

        self.assertEqual(acc.get_vendor(), AccVendor.INTEL)
        self.assertEqual(acc.get_device_count(), 0)

        found = find_accelerators()
        self.assertEqual(found, AcceleratorDescriptor(vendor=AccVendor.INTEL, device_list=[], env_str=AccEnvStr.INTEL))

    @patch.object(Accelerator, "_get_gpu_handle", return_value=(DragonError.SUCCESS, 0))
    @patch.object(Accelerator, "_setup_gpu", return_value=(DragonError.SUCCESS, 0))
    @patch.object(Accelerator, "_get_backend_type", return_value=(DragonError.SUCCESS, DRAGON_GPU_BACKEND_ZE))
    @patch.object(Accelerator, "_get_device_list", return_value=(DragonError.SUCCESS, [0, 1, 2, 3]))
    @patch.object(Accelerator, "_get_device_count", return_value=4)
    @patch.object(
        Accelerator, "_get_uuid", side_effect=lambda handle, devID: (DragonError.SUCCESS, f"uuid-{devID}".encode())
    )
    def test_intel_libs_with_devices(
        self, mock_get_uuid, mock_device_count, mock_device_list, mock_backend_type, mock_setup_gpu, mock_gpu_handle
    ):
        acc = Accelerator()

        self.assertEqual(acc.get_vendor(), AccVendor.INTEL)
        self.assertEqual(acc.get_device_count(), 4)
        self.assertEqual(acc.get_device_uuid(0), f"uuid-0")
        self.assertEqual(acc.get_device_uuid(1), f"uuid-1")
        self.assertEqual(acc.get_device_uuid(2), f"uuid-2")
        self.assertEqual(acc.get_device_uuid(3), f"uuid-3")
        self.assertEqual(acc.get_device_list(), [0, 1, 2, 3])

        found = find_accelerators()
        self.assertEqual(
            found, AcceleratorDescriptor(vendor=AccVendor.INTEL, device_list=[0, 1, 2, 3], env_str=AccEnvStr.INTEL)
        )
