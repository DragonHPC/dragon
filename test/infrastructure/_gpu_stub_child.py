"""Child process helper: probe Dragon's accelerator detection and dump JSON.

Run as ``python3 _gpu_stub_child.py <output.json>``. The results are written to
a file because the Cython accelerator layer prints diagnostics on stdout.
"""

import json
import os
import sys
import traceback
from pathlib import Path

_TEST_ROOT = Path(__file__).resolve().parent.parent
for _path in (str(_TEST_ROOT), str(_TEST_ROOT.parent / "src")):
    if _path not in sys.path:
        sys.path.insert(0, _path)

_GPU_RUNTIME_PREFIXES = ("libcudart", "libamdhip64", "libze_intel_gpu")


def _loaded_gpu_runtimes():
    """Paths of the GPU runtime libraries mapped into this process (Linux only)."""

    try:
        with open("/proc/self/maps") as maps:
            paths = {fields[5].strip() for fields in (line.split(None, 5) for line in maps) if len(fields) == 6}
    except OSError:
        return []
    return sorted(p for p in paths if os.path.basename(p).startswith(_GPU_RUNTIME_PREFIXES))


def probe():
    from dragon.accelerators import Accelerator, AccVendor
    from dragon.infrastructure.gpu_desc import find_accelerators

    acc = Accelerator()
    vendor = acc.get_vendor()
    result = {
        "vendor": int(vendor),
        "vendor_name": AccVendor(vendor).name,
        "device_count": acc.get_device_count(),
        "uuids": [acc.get_device_uuid(devID) for devID in range(acc.get_device_count())],
        "loaded_libs": _loaded_gpu_runtimes(),
    }

    # Accelerator never assigns _device_list when no backend is detected
    try:
        result["device_list"] = list(acc.get_device_list())
    except BaseException as ex:
        result["device_list_error"] = f"{type(ex).__name__}: {ex}"

    descriptor = find_accelerators()
    result["descriptor"] = (
        None
        if descriptor is None
        else {
            "vendor": int(descriptor.vendor),
            "device_list": list(descriptor.device_list),
            "env_str": descriptor.env_str,
        }
    )

    return result


def probe_pool_lifetime():
    import os
    from dragon.accelerators import Accelerator
    from dragon.managed_memory import MemoryPool

    pools = [
        MemoryPool(1024 * 1024, f"gpu_lifetime_{os.getpid()}_{idx}", os.getpid() + idx, gpu_memory=True)
        for idx in range(2)
    ]
    pools[0].destroy()
    allocation = pools[1].alloc(64)
    allocation.free()
    pools[1].destroy()
    result = probe()
    pool = MemoryPool(1024 * 1024, f"gpu_lifetime_{os.getpid()}_new", os.getpid() + 2, gpu_memory=True)
    pool.destroy()
    return result


def probe_copy():
    import os
    from dragon.managed_memory import MemoryPool

    host_pool = MemoryPool(1024 * 1024, f"gpu_copy_host_{os.getpid()}", os.getpid())
    device_pool = MemoryPool(1024 * 1024, f"gpu_copy_device_{os.getpid()}", os.getpid() + 1, gpu_memory=True)
    source = host_pool.alloc(64)
    payload = bytes(range(64))
    source.get_memview()[:] = payload
    device = source.copy(device_pool)
    destination = device.copy(host_pool)
    result = probe()
    result["copied_data"] = list(destination.get_memview())
    destination.free()
    device.free()
    source.free()
    device_pool.destroy()
    host_pool.destroy()
    return result


def main():
    out_path = sys.argv[1]
    try:
        scenarios = {"discovery": probe, "pool_lifetime": probe_pool_lifetime, "copy": probe_copy}
        result = scenarios[sys.argv[2] if len(sys.argv) > 2 else "discovery"]()
    except BaseException as ex:
        result = {"error": f"{type(ex).__name__}: {ex}", "traceback": traceback.format_exc()}

    with open(out_path, "w") as out_file:
        json.dump(result, out_file)


if __name__ == "__main__":
    main()
