"""Compile and stage fake CUDA/HIP/Level Zero runtime libraries.

Dragon's GPU backends dlopen the vendor runtime libraries and resolve every
entry point with dlsym, so a shared library that exports the same symbols is
enough to exercise the real C++ detection code on a node with no GPUs. The
stubs are compiled against the vendor headers Dragon itself was configured
with, so struct layouts and versioned symbol aliases match exactly.
"""

import os
import re
import shlex
import shutil
import stat
import subprocess
from pathlib import Path

_TEST_DIR = Path(__file__).resolve().parent
_SRC_DIR = _TEST_DIR / "gpu_stubs"
_CACHE_DIR = Path(os.environ.get("DRAGON_GPU_STUB_CACHE", str(_SRC_DIR / "_build")))

STUB_SPECS = {
    "cuda": {
        "source": "stub_cuda.cpp",
        "libname": "libcudart.so",
        "probe": ["cuda_runtime.h"],
        "define": "HAVE_CUDA_INCLUDE",
        "flags": [],
        "default_dirs": ["/usr/local/cuda/include", "/usr/include/cuda", "/usr/include"],
    },
    "hip": {
        "source": "stub_hip.cpp",
        "probe": ["hip/hip_runtime_api.h"],
        "libname": "libamdhip64.so",
        "define": "HAVE_HIP_INCLUDE",
        "flags": ["-D__HIP_PLATFORM_AMD__"],
        "default_dirs": ["/opt/rocm/include", "/usr/include"],
    },
    "ze": {
        "source": "stub_ze.cpp",
        "libname": "libze_intel_gpu.so.1",
        "probe": ["ze_api.h"],
        "define": "HAVE_ZE_INCLUDE",
        "flags": [],
        "default_dirs": ["/usr/include/level_zero", "/usr/local/include/level_zero", "/usr/include"],
    },
}


class StubUnavailable(Exception):
    """Raised when a stub cannot be built for this environment."""


def _dragon_base_dir():
    try:
        from dragon.infrastructure.facts import DRAGON_BASE_DIR

        return Path(DRAGON_BASE_DIR)
    except Exception:
        return None


def _config_makefile():
    base_dir = _dragon_base_dir()
    if base_dir is not None:
        makefile = base_dir / ".dragon-config.mk"
        if makefile.exists():
            return makefile

    makefile = _TEST_DIR / ".." / ".." / "src" / "dragon" / ".dragon-config.mk"
    return makefile.resolve() if makefile.exists() else None


def _makefile_text():
    makefile = _config_makefile()
    return makefile.read_text() if makefile is not None else ""


def backend_is_compiled_in(backend):
    """True if libdragon was configured with this GPU backend."""

    return f"-D{STUB_SPECS[backend]['define']}" in _makefile_text()


def _candidate_include_dirs():
    dirs = []

    for path in re.findall(r"-I(\S+)", _makefile_text()):
        dirs.append(path)

    try:
        from dragon.infrastructure.config import dragon_config

        config = dragon_config() or {}
    except Exception:
        config = {}

    for key, value in config.items():
        if key.endswith("_include") and isinstance(value, str):
            dirs.extend(value.split(":"))

    return dirs


def _find_include_dir(backend, spec):
    override = os.environ.get(f"DRAGON_GPU_STUB_{backend.upper()}_INCLUDE")
    candidates = (override.split(":") if override else []) + _candidate_include_dirs() + spec["default_dirs"]

    for candidate in candidates:
        candidate = candidate.strip()
        if candidate and all((Path(candidate) / header).exists() for header in spec["probe"]):
            return candidate
    return None


def build_stub(backend):
    """Compile the stub runtime library for ``backend`` and return its path."""

    spec = STUB_SPECS[backend]
    source = _SRC_DIR / spec["source"]
    include_dir = _find_include_dir(backend, spec)
    if include_dir is None:
        raise StubUnavailable(f"could not locate {spec['probe'][0]} for the {backend} stub")

    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    output = _CACHE_DIR / spec["libname"]
    if output.exists() and output.stat().st_mtime >= source.stat().st_mtime:
        return output

    cxx = os.environ.get("CXX", "g++")
    if shutil.which(cxx) is None:
        raise StubUnavailable(f"no C++ compiler ({cxx}) available to build the {backend} stub")

    cmd = [
        cxx,
        "-shared",
        "-fPIC",
        "-O0",
        "-g",
        f"-I{include_dir}",
        *spec["flags"],
        f"-Wl,-soname,{spec['libname']}",
        "-o",
        str(output),
        str(source),
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise StubUnavailable(f"failed to build the {backend} stub:\n{' '.join(cmd)}\n{result.stderr}")

    return output


def _write_dragon_config_shim(stub_dir, config=None):
    """Point Dragon's CUDA loader at the stub directory.

    ``_open_cuda_lib`` asks ``dragon-config`` where libcudart.so lives, so the
    shim both finds the CUDA stub and keeps a real system libcudart.so from
    being picked up when testing the other backends. ``config`` overrides the
    value returned for each ``dragon-config --get`` key.
    """

    if config is None:
        config = {"cuda_runtime_lib": str(stub_dir), "cuda_runtime_libname": ""}

    real = shutil.which("dragon-config") or ""
    shim = Path(stub_dir) / "dragon-config"
    cases = "".join(f"  *--get={key}) echo {shlex.quote(str(value))} ;;\n" for key, value in config.items())
    shim.write_text(
        "#!/bin/sh\n"
        'case "$*" in\n'
        f"{cases}"
        f"  *) [ -x '{real}' ] && exec '{real}' \"$@\" || exit 0 ;;\n"
        "esac\n"
    )
    shim.chmod(shim.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    return shim


def stage_stubs(stub_dir, backends):
    """Populate ``stub_dir`` with stub libraries for the given backends."""

    stub_dir = Path(stub_dir)
    stub_dir.mkdir(parents=True, exist_ok=True)

    for backend in backends:
        shutil.copy2(build_stub(backend), stub_dir / STUB_SPECS[backend]["libname"])

    _write_dragon_config_shim(stub_dir)
    return stub_dir


def stub_environment(stub_dir, base_env=None, **stub_vars):
    """Build the environment a child process needs to see the stubs."""

    env = dict(os.environ if base_env is None else base_env)
    stub_dir = str(stub_dir)

    for var, path_entry in (("LD_LIBRARY_PATH", stub_dir), ("PATH", stub_dir)):
        existing = env.get(var)
        env[var] = f"{path_entry}:{existing}" if existing else path_entry

    for key, value in stub_vars.items():
        env[key] = str(value)

    return env


def expected_uuid(device_id, subdevice_id=0):
    """The UUID string Dragon should produce for a stub device."""

    raw = bytes((device_id * 0x10 + subdevice_id + i) & 0xFF for i in range(16))
    hexed = raw.hex()
    return f"GPU-{hexed[0:8]}-{hexed[8:12]}-{hexed[12:16]}-{hexed[16:20]}-{hexed[20:32]}"
