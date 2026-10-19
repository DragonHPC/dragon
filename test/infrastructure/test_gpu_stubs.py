#!/usr/bin/env python3
"""Exercise Dragon's GPU detection against stubbed vendor runtimes.

These tests run the real C++ CUDA/HIP/Level Zero backends and the Cython
``Accelerator`` wrapper, but the vendor runtime libraries they dlopen are stubs
built by :mod:`gpu_stub_builder`. That makes it possible to test single-GPU and
multi-GPU node behavior in a build container with no GPUs present.
"""

import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

import gpu_stub_builder as stubs  # noqa: E402
from dragon.accelerators import AccVendor  # noqa: E402
from dragon.infrastructure.gpu_desc import AccEnvStr  # noqa: E402

_CHILD = _HERE / "_gpu_stub_child.py"


def _requires(backend):
    reason = None
    if not stubs.backend_is_compiled_in(backend):
        reason = f"libdragon was not built with the {backend} backend"
    else:
        try:
            stubs.build_stub(backend)
        except stubs.StubUnavailable as ex:
            reason = str(ex)
    return unittest.skipIf(reason is not None, reason or "")


class StubbedAcceleratorTestCase(unittest.TestCase):
    """Base class that runs the detection probe against staged stubs."""

    def probe(self, backends, scenario="discovery", **stub_vars):
        result = self.probe_raw(backends, scenario=scenario, **stub_vars)
        self.assertNotIn("error", result, result.get("traceback", ""))
        return result

    def probe_raw(self, backends, scenario="discovery", **stub_vars):
        with tempfile.TemporaryDirectory(prefix="dragon_gpu_stub_") as tmpdir:
            stub_dir = stubs.stage_stubs(Path(tmpdir) / "libs", backends)
            env = stubs.stub_environment(stub_dir, **stub_vars)
            out_path = Path(tmpdir) / "result.json"

            proc = subprocess.run(
                [sys.executable, str(_CHILD), str(out_path), scenario],
                env=env,
                capture_output=True,
                text=True,
                timeout=30,
            )

            self.assertEqual(proc.returncode, 0, f"probe failed\n{proc.stdout}\n{proc.stderr}")
            self.assertTrue(
                out_path.exists(),
                f"probe produced no result (rc={proc.returncode})\n{proc.stdout}\n{proc.stderr}",
            )
            result = json.loads(out_path.read_text())

        return result

    def assertDeviceList(self, actual_list, expected_list):
        self.assertEqual(len(actual_list), len(expected_list))
        for actual, expected in zip(actual_list, expected_list):
            if isinstance(expected, float):
                self.assertAlmostEqual(actual, expected, places=6)
            else:
                self.assertEqual(actual, expected)

    def assertDevices(self, result, vendor, device_list, uuid_count=None):
        self.assertEqual(result["vendor"], int(vendor))
        self.assertDeviceList(result["device_list"], device_list)
        self.assertEqual(result["device_count"], len(device_list) if uuid_count is None else uuid_count)
        self.assertEqual(result["uuids"], [stubs.expected_uuid(i) for i in range(result["device_count"])])

        # find_accelerators() must report the same vendor/device_list Dragon's
        # Accelerator wrapper detected, so downstream policy code sees a
        # consistent AcceleratorDescriptor.
        descriptor = result["descriptor"]
        self.assertIsNotNone(descriptor)
        self.assertEqual(descriptor["vendor"], int(vendor))
        self.assertDeviceList(descriptor["device_list"], device_list)
        expected_env_str = {
            AccVendor.NVIDIA: AccEnvStr.NVIDIA,
            AccVendor.AMD: AccEnvStr.AMD,
            AccVendor.INTEL: AccEnvStr.INTEL,
        }[vendor]
        self.assertEqual(descriptor["env_str"], expected_env_str)


@_requires("cuda")
class TestStubbedCuda(StubbedAcceleratorTestCase):
    def test_pool_lifetime(self):
        result = self.probe(["cuda"], scenario="pool_lifetime", DRAGON_STUB_CUDA_DEVICE_COUNT=2)
        self.assertDevices(result, AccVendor.NVIDIA, [0, 1])

    def test_multi_gpu_node(self):
        result = self.probe(["cuda"], DRAGON_STUB_CUDA_DEVICE_COUNT=4)
        self.assertDevices(result, AccVendor.NVIDIA, [0, 1, 2, 3])

    def test_single_gpu_node(self):
        result = self.probe(["cuda"], DRAGON_STUB_CUDA_DEVICE_COUNT=1)
        self.assertDevices(result, AccVendor.NVIDIA, [0])

    def test_large_gpu_node(self):
        result = self.probe(["cuda"], DRAGON_STUB_CUDA_DEVICE_COUNT=8)
        self.assertDevices(result, AccVendor.NVIDIA, list(range(8)))

    def test_visible_devices(self):
        # CUDA_VISIBLE_DEVICES selects and renumbers visible devices
        # consecutively from 0, regardless of the underlying physical IDs.
        result = self.probe(["cuda"], DRAGON_STUB_CUDA_DEVICE_COUNT=8, CUDA_VISIBLE_DEVICES="0, 3, 4, 6")
        self.assertDevices(result, AccVendor.NVIDIA, [0, 1, 2, 3])

    def test_runtime_present_but_no_devices(self):
        # Vendor libraries installed but no hardware: no usable backend is
        # detected, so vendor reports as UNKNOWN.
        result = self.probe(["cuda"], DRAGON_STUB_CUDA_DEVICE_COUNT=0)
        self.assertEqual(result["vendor"], int(AccVendor.UNKNOWN))
        self.assertEqual(result["device_count"], 0)
        self.assertEqual(result.get("device_list", []), [])
        self.assertIsNone(result["descriptor"])


@_requires("hip")
class TestStubbedHip(StubbedAcceleratorTestCase):
    def test_pool_lifetime(self):
        result = self.probe(["hip"], scenario="pool_lifetime", DRAGON_STUB_HIP_DEVICE_COUNT=2)
        self.assertDevices(result, AccVendor.AMD, [0, 1])

    def test_multi_gpu_node(self):
        result = self.probe(["hip"], DRAGON_STUB_HIP_DEVICE_COUNT=4)
        self.assertDevices(result, AccVendor.AMD, [0, 1, 2, 3])

    def test_single_gpu_node(self):
        result = self.probe(["hip"], DRAGON_STUB_HIP_DEVICE_COUNT=1)
        self.assertDevices(result, AccVendor.AMD, [0])

    def test_large_gpu_node(self):
        result = self.probe(["hip"], DRAGON_STUB_HIP_DEVICE_COUNT=8)
        self.assertDevices(result, AccVendor.AMD, list(range(8)))

    def test_visible_devices(self):
        # HIP_VISIBLE_DEVICES selects and renumbers visible devices
        # consecutively from 0, regardless of the underlying physical IDs.
        result = self.probe(["hip"], DRAGON_STUB_HIP_DEVICE_COUNT=8, HIP_VISIBLE_DEVICES="0, 3, 4, 6")
        self.assertDevices(result, AccVendor.AMD, [0, 1, 2, 3])

    def test_runtime_present_but_no_devices(self):
        # Vendor libraries installed but no hardware: no usable backend is
        # detected, so vendor reports as UNKNOWN.
        result = self.probe(["hip"], DRAGON_STUB_HIP_DEVICE_COUNT=0)
        self.assertEqual(result["vendor"], int(AccVendor.UNKNOWN))
        self.assertEqual(result["device_count"], 0)
        self.assertEqual(result.get("device_list", []), [])
        self.assertIsNone(result["descriptor"])


@_requires("ze")
class TestStubbedZe(StubbedAcceleratorTestCase):
    # HIP is probed before Level Zero, so stage a HIP stub that fails to
    # initialize to keep any system ROCm runtime from winning the search.
    BACKENDS = ["hip", "ze"]

    def probe(self, backends=None, **stub_vars):
        stub_vars.setdefault("DRAGON_STUB_HIP_DEVICE_COUNT", -1)
        return super().probe(backends or self.BACKENDS, **stub_vars)

    def test_multi_gpu_node(self):
        result = self.probe(DRAGON_STUB_ZE_DEVICE_COUNT=4, DRAGON_STUB_ZE_SUBDEVICE_COUNT=0)
        self.assertDevices(result, AccVendor.INTEL, [0, 1, 2, 3])

    def test_copy_roundtrip(self):
        result = self.probe(scenario="copy", DRAGON_STUB_ZE_DEVICE_COUNT=2, DRAGON_STUB_ZE_SUBDEVICE_COUNT=2)
        self.assertEqual(result["copied_data"], list(range(64)))

    def test_single_gpu_node(self):
        result = self.probe(DRAGON_STUB_ZE_DEVICE_COUNT=1, DRAGON_STUB_ZE_SUBDEVICE_COUNT=0)
        self.assertDevices(result, AccVendor.INTEL, [0])

    def test_multi_gpu_node_with_tiles(self):
        result = self.probe(DRAGON_STUB_ZE_DEVICE_COUNT=2, DRAGON_STUB_ZE_SUBDEVICE_COUNT=2)
        self.assertDevices(result, AccVendor.INTEL, [0.0, 0.1, 1.0, 1.1], uuid_count=2)

    def test_single_gpu_node_with_tiles(self):
        result = self.probe(DRAGON_STUB_ZE_DEVICE_COUNT=1, DRAGON_STUB_ZE_SUBDEVICE_COUNT=4)
        self.assertDevices(result, AccVendor.INTEL, [0.0, 0.1, 0.2, 0.3], uuid_count=1)

    def test_flat_hierarchy(self):
        result = self.probe(DRAGON_STUB_ZE_DEVICE_COUNT=2, DRAGON_STUB_ZE_SUBDEVICE_COUNT=0)
        self.assertDevices(result, AccVendor.INTEL, [0, 1], uuid_count=2)

    def test_composite_hierarchy_with_composite_affinity_mask(self):
        # ZE_AFFINITY_MASK under a COMPOSITE hierarchy: the stub emulates a
        # driver that has already filtered devices down to the masked
        # (device, subdevice) pairs, renumbering majors consecutively.
        result = self.probe(
            ZE_AFFINITY_MASK="0.0,1.0,1.1,4.1",
            ZE_FLAT_DEVICE_HIERARCHY="COMPOSITE",
        )
        self.assertDevices(result, AccVendor.INTEL, [0.0, 1.0, 1.1, 2.1], uuid_count=3)

    def test_flat_hierarchy_affinity_mask(self):
        # ZE_FLAT_DEVICE_HIERARCHY=FLAT treats subdevices as independent
        # top-level devices; the mask selects flat tile indices and
        # renumbers them consecutively with no fractional subdevice IDs.
        result = self.probe(
            ZE_AFFINITY_MASK="0,3,4,5",
            ZE_FLAT_DEVICE_HIERARCHY="FLAT",
        )
        self.assertDevices(result, AccVendor.INTEL, [0, 1, 2, 3])

    def test_runtime_present_but_no_devices(self):
        # Vendor libraries installed but no hardware: no usable backend is
        # detected, so vendor reports as UNKNOWN.
        result = self.probe(DRAGON_STUB_ZE_DEVICE_COUNT=0, DRAGON_STUB_ZE_SUBDEVICE_COUNT=0)
        self.assertEqual(result["vendor"], int(AccVendor.UNKNOWN))
        self.assertEqual(result["device_count"], 0)
        self.assertEqual(result.get("device_list", []), [])
        self.assertIsNone(result["descriptor"])


class TestNoAccelerators(StubbedAcceleratorTestCase):
    def test_no_vendor_runtimes(self):
        stub_vars = {
            "DRAGON_STUB_CUDA_DEVICE_COUNT": -1,
            "DRAGON_STUB_HIP_DEVICE_COUNT": -1,
            "DRAGON_STUB_ZE_DEVICE_COUNT": 0,
        }
        available = [b for b in ("cuda", "hip", "ze") if stubs.backend_is_compiled_in(b)]
        try:
            result = self.probe(available, **stub_vars)
        except stubs.StubUnavailable as ex:
            self.skipTest(str(ex))

        self.assertEqual(result["vendor"], int(AccVendor.UNKNOWN))
        self.assertEqual(result["device_count"], 0)
        self.assertEqual(result.get("device_list", []), [])
        self.assertIsNone(result["descriptor"])


@_requires("cuda")
class TestCudaRuntimeLibLookup(unittest.TestCase):
    """Check the order _open_gpu_runtime_lib searches for the CUDA runtime library.

    Copies of the CUDA stub are placed in an LD_LIBRARY_PATH directory, a
    cuda_runtime_lib directory and an absolute-path directory, and the test
    asserts which copy the child process actually mapped.
    """

    # A name no real CUDA install provides, so a system runtime can't satisfy the lookup.
    LIBNAME = "libcudart_dragon_test.so.99"
    DEFAULT = "libcudart.so"

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix="dragon_gpu_libname_")
        self.root = Path(self._tmp.name).resolve()
        self.bin_dir = self.root / "bin"
        self.ld_dir = self.root / "ld"
        self.lib_dir = self.root / "runtime_lib"
        self.abs_dir = self.root / "abs"
        for directory in (self.bin_dir, self.ld_dir, self.lib_dir, self.abs_dir):
            directory.mkdir()
        self.stub = stubs.build_stub("cuda")

    def tearDown(self):
        self._tmp.cleanup()

    def place(self, directory, name):
        dest = directory / name
        shutil.copy2(self.stub, dest)
        return dest

    def loaded_cudart(self, libname="", lib_dir=""):
        stubs._write_dragon_config_shim(self.bin_dir, {"cuda_runtime_lib": lib_dir, "cuda_runtime_libname": libname})

        env = dict(os.environ)
        env["PATH"] = f"{self.bin_dir}:{env.get('PATH', '')}"
        env["LD_LIBRARY_PATH"] = f"{self.ld_dir}:{env.get('LD_LIBRARY_PATH', '')}"
        env["DRAGON_STUB_CUDA_DEVICE_COUNT"] = "1"

        out_path = self.root / "result.json"
        proc = subprocess.run(
            [sys.executable, str(_CHILD), str(out_path)], env=env, capture_output=True, text=True, timeout=30
        )
        self.assertEqual(proc.returncode, 0, f"probe failed\n{proc.stdout}\n{proc.stderr}")
        result = json.loads(out_path.read_text())
        self.assertNotIn("error", result, result.get("traceback", ""))
        self.assertEqual(result["vendor"], int(AccVendor.NVIDIA))

        return [lib for lib in result["loaded_libs"] if Path(lib).name.startswith("libcudart")]

    def test_full_path_libname(self):
        libname = self.place(self.abs_dir, self.LIBNAME)
        self.place(self.ld_dir, self.LIBNAME)
        self.place(self.lib_dir, self.LIBNAME)
        self.place(self.lib_dir, self.DEFAULT)

        loaded = self.loaded_cudart(libname=str(libname), lib_dir=str(self.lib_dir))
        self.assertEqual(loaded, [str(libname)])

    def test_missing_full_path_libname_uses_library_path(self):
        expected = self.place(self.ld_dir, self.LIBNAME)
        self.place(self.lib_dir, self.LIBNAME)

        loaded = self.loaded_cudart(libname=str(self.root / "missing" / self.LIBNAME), lib_dir=str(self.lib_dir))
        self.assertEqual(loaded, [str(expected)])

    def test_missing_full_path_libname_uses_runtime_lib_dir(self):
        expected = self.place(self.lib_dir, self.LIBNAME)

        loaded = self.loaded_cudart(libname=str(self.root / "missing" / self.LIBNAME), lib_dir=str(self.lib_dir))
        self.assertEqual(loaded, [str(expected)])

    def test_bare_libname_prefers_library_path(self):
        expected = self.place(self.ld_dir, self.LIBNAME)
        self.place(self.lib_dir, self.LIBNAME)

        loaded = self.loaded_cudart(libname=self.LIBNAME, lib_dir=str(self.lib_dir))
        self.assertEqual(loaded, [str(expected)])

    def test_bare_libname_in_runtime_lib_dir(self):
        expected = self.place(self.lib_dir, self.LIBNAME)

        loaded = self.loaded_cudart(libname=self.LIBNAME, lib_dir=str(self.lib_dir))
        self.assertEqual(loaded, [str(expected)])

    def test_unresolved_libname_falls_back_to_default_in_runtime_lib_dir(self):
        expected = self.place(self.lib_dir, self.DEFAULT)
        self.place(self.ld_dir, self.DEFAULT)

        loaded = self.loaded_cudart(libname=self.LIBNAME, lib_dir=str(self.lib_dir))
        self.assertEqual(loaded, [str(expected)])

    def test_unresolved_libname_falls_back_to_default_on_library_path(self):
        expected = self.place(self.ld_dir, self.DEFAULT)

        loaded = self.loaded_cudart(libname=str(self.root / "missing" / self.LIBNAME), lib_dir=str(self.lib_dir))
        self.assertEqual(loaded, [str(expected)])

    def test_default_in_runtime_lib_dir(self):
        expected = self.place(self.lib_dir, self.DEFAULT)
        self.place(self.ld_dir, self.DEFAULT)

        loaded = self.loaded_cudart(lib_dir=str(self.lib_dir))
        self.assertEqual(loaded, [str(expected)])

    def test_default_on_library_path(self):
        expected = self.place(self.ld_dir, self.DEFAULT)

        loaded = self.loaded_cudart()
        self.assertEqual(loaded, [str(expected)])


if __name__ == "__main__":
    unittest.main()
