import ctypes
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import dragon.numa as numa
from dragon.infrastructure.facts import DRAGON_LIB_SO
from dragon.infrastructure.gpu_desc import AcceleratorDescriptor
from dragon.infrastructure.node_desc import NodeDescriptor
from dragon.numa import DragonNUMA
from dragon.rc import DragonError


class TestNUMA(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        try:
            DragonNUMA()
        except RuntimeError as ex:
            raise unittest.SkipTest(str(ex))

    def test_node_count_matches_sysfs(self):
        nodes = list(Path("/sys/devices/system/node").glob("node[0-9]*"))
        if not nodes:
            self.skipTest("NUMA topology is not exposed in sysfs")
        self.assertEqual(DragonNUMA.get_numa_node_count(), len(nodes))

    def test_cpu_mapping_matches_sysfs(self):
        nodes = list(Path("/sys/devices/system/node").glob("node[0-9]*"))
        if not nodes:
            self.skipTest("NUMA topology is not exposed in sysfs")
        for node in nodes:
            nid = int(node.name[4:])
            expected = sorted(int(cpu.name[3:]) for cpu in node.glob("cpu[0-9]*"))
            with self.subTest(node=nid):
                self.assertEqual(DragonNUMA.get_numa_node_to_cpus(nid), expected)

    def test_invalid_node(self):
        with self.assertRaisesRegex(RuntimeError, "Failed to get CPUs"):
            DragonNUMA.get_numa_node_to_cpus(-1)

    def test_native_nodemask(self):
        library = ctypes.CDLL(numa.__file__)
        library.dragon_numa_allocate_nodemask.argtypes = [ctypes.POINTER(ctypes.c_void_p)]
        library.dragon_numa_bitmask_setbit.argtypes = [ctypes.c_void_p, ctypes.c_uint]
        library.dragon_numa_bitmask_setall.argtypes = [ctypes.c_void_p]
        library.dragon_numa_bitmask_isbitset.argtypes = [ctypes.c_void_p, ctypes.c_uint, ctypes.POINTER(ctypes.c_int)]
        library.dragon_numa_free_nodemask.argtypes = [ctypes.c_void_p]
        mask = ctypes.c_void_p()
        self.assertEqual(library.dragon_numa_allocate_nodemask(ctypes.byref(mask)), DragonError.SUCCESS)
        try:
            self.assertEqual(library.dragon_numa_bitmask_setbit(mask, 0), DragonError.SUCCESS)
            bit = ctypes.c_int()
            self.assertEqual(library.dragon_numa_bitmask_isbitset(mask, 0, ctypes.byref(bit)), DragonError.SUCCESS)
            self.assertEqual(bit.value, 1)
            self.assertEqual(library.dragon_numa_bitmask_setall(mask), DragonError.SUCCESS)
            for nid in range(DragonNUMA.get_numa_node_count()):
                self.assertEqual(
                    library.dragon_numa_bitmask_isbitset(mask, nid, ctypes.byref(bit)), DragonError.SUCCESS
                )
                self.assertEqual(bit.value, 1)
        finally:
            self.assertEqual(library.dragon_numa_free_nodemask(mask), DragonError.SUCCESS)

    def test_native_null_outputs(self):
        library = ctypes.CDLL(numa.__file__)
        library.dragon_numa_allocate_cpumask.argtypes = [ctypes.POINTER(ctypes.c_void_p)]
        library.dragon_numa_allocate_nodemask.argtypes = [ctypes.POINTER(ctypes.c_void_p)]
        library.dragon_numa_bitmask_isbitset.argtypes = [ctypes.c_void_p, ctypes.c_uint, ctypes.POINTER(ctypes.c_int)]
        library.dragon_numa_free_cpumask.argtypes = [ctypes.c_void_p]
        self.assertEqual(library.dragon_numa_allocate_cpumask(None), DragonError.INVALID_ARGUMENT)
        self.assertEqual(library.dragon_numa_allocate_nodemask(None), DragonError.INVALID_ARGUMENT)
        mask = ctypes.c_void_p()
        self.assertEqual(library.dragon_numa_allocate_cpumask(ctypes.byref(mask)), DragonError.SUCCESS)
        try:
            self.assertEqual(library.dragon_numa_bitmask_isbitset(mask, 0, None), DragonError.INVALID_ARGUMENT)
        finally:
            self.assertEqual(library.dragon_numa_free_cpumask(mask), DragonError.SUCCESS)

    def test_optional_free_symbols(self):
        for symbols in (("numa_free_cpumask_p",), ("numa_free_nodemask_p",), ()):
            with self.subTest(symbols=symbols):
                code = f"""
import ctypes
import dragon.numa as numa
from dragon.numa import DragonNUMA
DragonNUMA()
library = ctypes.CDLL(numa.__file__)
library.dragon_numa_allocate_cpumask.argtypes = [ctypes.POINTER(ctypes.c_void_p)]
library.dragon_numa_free_cpumask.argtypes = [ctypes.c_void_p]
callback_type = ctypes.CFUNCTYPE(None, ctypes.c_void_p)
real_free = callback_type(ctypes.c_void_p.in_dll(library, 'numa_bitmask_free_p').value)
freed = []
def record_free(mask):
    freed.append(mask)
    real_free(mask)
callback = callback_type(record_free)
address = ctypes.cast(callback, ctypes.c_void_p).value
for symbol in ('numa_free_cpumask_p', 'numa_free_nodemask_p'):
    ctypes.c_void_p.in_dll(library, symbol).value = address if symbol in {symbols!r} else None
ctypes.c_void_p.in_dll(library, 'numa_bitmask_free_p').value = address
mask = ctypes.c_void_p()
assert library.dragon_numa_allocate_cpumask(ctypes.byref(mask)) == 0
assert library.dragon_numa_free_cpumask(mask) == 0
assert len(freed) == 1, 'CPU mask was not freed exactly once'
"""
                result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=30)
                self.assertEqual(result.returncode, 0, result.stderr)

    def test_cpu_mask_freed_on_query_error(self):
        code = """
import ctypes
import sys
import dragon.numa as numa
from dragon.infrastructure import facts
from dragon.numa import DragonNUMA
DragonNUMA()
if len(sys.argv) > 1:
    facts.DRAGON_LIB_SO = sys.argv[1]
    shadow_library = ctypes.CDLL(facts.DRAGON_LIB_SO)
    assert ctypes.c_void_p.in_dll(shadow_library, 'numa_bitmask_free_p').value is None
library = ctypes.CDLL(numa.__file__)
callback_type = ctypes.CFUNCTYPE(None, ctypes.c_void_p)
real_free = callback_type(ctypes.c_void_p.in_dll(library, 'numa_bitmask_free_p').value)
freed = []
def record_free(mask):
    freed.append(mask)
    real_free(mask)
callback = callback_type(record_free)
address = ctypes.cast(callback, ctypes.c_void_p).value
ctypes.c_void_p.in_dll(library, 'numa_free_cpumask_p').value = address
ctypes.c_void_p.in_dll(library, 'numa_free_nodemask_p').value = address
try:
    DragonNUMA.get_numa_node_to_cpus(-1)
except RuntimeError:
    pass
else:
    raise AssertionError('Invalid node was accepted')
assert len(freed) == 1, 'CPU mask was not freed after a failed query'
"""
        with tempfile.TemporaryDirectory(prefix="dragon_numa_duplicate_") as directory:
            duplicate = Path(directory) / "libdragon.so"
            shutil.copy2(DRAGON_LIB_SO, duplicate)
            for arguments in ([], [str(duplicate)]):
                with self.subTest(duplicate_library=bool(arguments)):
                    result = subprocess.run(
                        [sys.executable, "-c", code, *arguments], capture_output=True, text=True, timeout=30
                    )
                    self.assertEqual(result.returncode, 0, result.stderr)


class TestNUMALoader(unittest.TestCase):
    def test_unusable_libraries(self):
        compiler = shutil.which(os.environ.get("CC", "cc"))
        if compiler is None:
            self.skipTest("A C compiler is needed for NUMA loader tests")
        symbols = (
            "numa_allocate_nodemask",
            "numa_bitmask_setall",
            "numa_bitmask_setbit",
            "numa_interleave_memory",
            "numa_bitmask_free",
            "numa_sched_getaffinity",
            "numa_sched_setaffinity",
            "numa_num_configured_nodes",
            "numa_node_to_cpus",
            "numa_bitmask_isbitset",
            "numa_allocate_cpumask",
        )
        unavailable = "int numa_available(void) { return -1; }\n"
        unavailable += "\n".join(f"void {symbol}(void) {{}}" for symbol in symbols)
        cases = (
            ("missing_available", "int unrelated(void) { return 0; }", DragonError.NUMA_NOT_FOUND),
            ("missing_symbols", "int numa_available(void) { return 0; }", DragonError.NUMA_NOT_FOUND),
            ("unavailable", unavailable, DragonError.NUMA_NOT_AVAILABLE),
        )
        for name, source, expected in cases:
            with self.subTest(case=name), tempfile.TemporaryDirectory(prefix="dragon_numa_") as directory:
                library_path = Path(directory) / "libnuma.so.1"
                build = subprocess.run(
                    [compiler, "-shared", "-fPIC", "-x", "c", "-", "-o", str(library_path)],
                    input=source,
                    capture_output=True,
                    text=True,
                    timeout=30,
                )
                self.assertEqual(build.returncode, 0, build.stderr)
                env = dict(os.environ)
                library_path_variable = "DYLD_LIBRARY_PATH" if sys.platform == "darwin" else "LD_LIBRARY_PATH"
                env[library_path_variable] = directory + os.pathsep + env.get(library_path_variable, "")
                code = f"""
import ctypes
import os
import dragon.numa as numa
from dragon.managed_memory import MemoryPool
from dragon.numa import DragonNUMA
library = ctypes.CDLL(numa.__file__)
actual = library.dragon_numa_available()
assert actual == {int(expected)}, f'Expected {int(expected)}, got {{actual}}'
for operation in (DragonNUMA, DragonNUMA.get_numa_node_count, lambda: DragonNUMA.get_numa_node_to_cpus(0)):
    try:
        operation()
    except RuntimeError:
        pass
    else:
        raise AssertionError('Unusable NUMA library was accepted')
pool = MemoryPool(1024 * 1024, 'numa_fallback_' + str(os.getpid()), os.getpid())
pool.destroy()
"""
                result = subprocess.run(
                    [sys.executable, "-c", code], env=env, capture_output=True, text=True, timeout=30
                )
                self.assertEqual(result.returncode, 0, result.stderr)


class TestNodeDescriptorNUMA(unittest.TestCase):
    def test_numa_json_roundtrip(self):
        cpus = {0: [0, 1], 1: [2, 3]}
        node = NodeDescriptor(num_numas=2, numa_cpus=cpus)
        self.assertEqual(node.num_numas, 2)
        self.assertEqual(node.numa_cpus, cpus)
        restored = NodeDescriptor.from_sdict(json.loads(json.dumps(node.get_sdict())))
        self.assertEqual(restored.num_numas, 2)
        self.assertEqual(restored.numa_cpus, cpus)

    def test_legacy_descriptor_defaults(self):
        serialized = NodeDescriptor().get_sdict()
        serialized.pop("num_numas")
        serialized.pop("numa_cpus")
        node = NodeDescriptor.from_sdict(serialized)
        self.assertEqual(node.num_numas, 0)
        self.assertEqual(node.numa_cpus, {})

    def test_local_numa_discovery(self):
        with (
            patch("dragon.infrastructure.node_desc.DragonNUMA.get_numa_node_count", return_value=2),
            patch("dragon.infrastructure.node_desc.DragonNUMA.get_numa_node_to_cpus", side_effect=[[0, 1], [2, 3]]),
        ):
            node = NodeDescriptor.get_localservices_node_conf(
                host_id=1, ls_cd="test", cpu_devices=[0, 1, 2, 3], accelerators=AcceleratorDescriptor()
            )
        self.assertEqual(node.num_numas, 2)
        self.assertEqual(node.numa_cpus, {0: [0, 1], 1: [2, 3]})

    def test_numa_unavailable(self):
        with patch(
            "dragon.infrastructure.node_desc.DragonNUMA.get_numa_node_count", side_effect=RuntimeError("no NUMA")
        ):
            node = NodeDescriptor.get_localservices_node_conf(
                host_id=1, ls_cd="test", cpu_devices=[0, 1], accelerators=AcceleratorDescriptor()
            )
        self.assertEqual(node.num_numas, 0)
        self.assertEqual(node.numa_cpus, {})


if __name__ == "__main__":
    unittest.main()
