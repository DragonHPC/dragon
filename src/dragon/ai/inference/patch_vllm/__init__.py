"""Dragon compatibility plugin for vLLM multiprocessing internals.

vLLM starts worker and engine-core processes with assumptions that do not hold
inside a Dragon runtime.  This plugin monkey-patches the small set of vLLM
startup functions that need Dragon-aware behavior:

* engine startup waits use ``multiprocessing.connection.wait`` in a way that is
    compatible with Dragon-managed sentinel handles,
* worker process startup and readiness handling tolerate Dragon channel cleanup,
    parent-death notifications, and peer response handles,
* open-port selection is seeded by GPU device ID so co-located vLLM instances do
    not all probe the same port first, and
* the multiprocessing-context helper returns the active Dragon context.

The package is shipped inside ``dragonhpc`` and its patch functions are
registered as vLLM general plugins through the ``vllm.general_plugins`` entry
point group declared by ``dragonhpc``.  Because those entry points are loaded
for *any* vLLM in the environment, every patch first checks
:func:`_in_dragon_inference_runtime` and is a no-op unless vLLM is running
inside the Dragon inference service.  The concrete patch implementation is then
selected based on the installed vLLM version.
"""

import os

from packaging import version
import vllm

VLLM_VERSION = version.parse(vllm.__version__)


def _in_dragon_inference_runtime():
    """Return ``True`` when vLLM is running inside the Dragon inference service.

    ``LLMInferenceEngine.initialize()`` sets ``_DRAGON_DEVICE_OFFSET`` in the
    environment before constructing ``LLM()``, which is what triggers vLLM to
    load its general plugins.  When that variable is absent, vLLM is being used
    outside the Dragon inference service and the Dragon patches must not be
    applied so that normal vLLM behavior is preserved.

    :returns: ``True`` if the Dragon inference service environment marker is
        present, ``False`` otherwise.
    :rtype: bool
    """
    return "_DRAGON_DEVICE_OFFSET" in os.environ


def patch_engine_utils():
    """Patch vLLM engine startup waiting for Dragon compatibility.

    vLLM's engine utility function is replaced with the Dragon-specific
    implementation matching the installed vLLM version.  The original function
    is stored on the vLLM module as ``_original_wait_for_engine_startup`` for
    debugging or future restoration.  No-op outside the Dragon inference
    service.
    """
    if not _in_dragon_inference_runtime():
        return

    import vllm.v1.engine.utils as utils_module

    if VLLM_VERSION >= version.parse("0.15.0"):
        from .dragon_engine_utils_v015 import dragon_wait_for_engine_startup
    else:  # 0.12.x
        from .dragon_engine_utils import dragon_wait_for_engine_startup

    # Store original for potential restoration
    utils_module._original_wait_for_engine_startup = utils_module.wait_for_engine_startup

    # Apply Dragon version
    utils_module.wait_for_engine_startup = dragon_wait_for_engine_startup


# Port range for seeded random search.
_PORT_RANGE_START = 30000
_PORT_RANGE_END = 60000

# Filled by patch_get_open_port() with the original vLLM function.
_original_get_open_port = None


def _device_based_get_open_port():
    """Return a free port using a worker-seeded random starting point.

    ``LLMInferenceEngine.initialize()`` sets the environment variable
    ``_DRAGON_DEVICE_OFFSET`` to the first GPU device ID before creating
    the ``LLM()`` instance.  This patched ``get_open_port()`` uses the
    device index as a seed for a random starting port within a large range,
    then scans sequentially until a free port is found.
    Because each worker has a different seed, they start scanning from
    widely separated points in the range, making collisions extremely
    unlikely.
    Falls back to the original ``get_open_port()`` if the env var is not
    set, for example when vLLM is used outside Dragon.

    :returns: Available TCP port number.
    :rtype: int
    :raises RuntimeError: If no free port can be found in the configured range
        or no original vLLM fallback is available.
    """
    import os
    import random
    import socket

    offset = os.environ.get("_DRAGON_DEVICE_OFFSET")
    if offset is not None:
        rng = random.Random(int(offset))
        port_range_size = _PORT_RANGE_END - _PORT_RANGE_START
        start = rng.randint(0, port_range_size - 1)
        for i in range(port_range_size):
            port = _PORT_RANGE_START + (start + i) % port_range_size
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                sock.bind(("", port))
                sock.close()
                return port
            except OSError:
                sock.close()
        raise RuntimeError(
            f"Could not find a free port for device offset {offset} " f"after scanning {port_range_size} ports"
        )

    # Fallback: call the original vLLM implementation
    if _original_get_open_port is not None:
        return _original_get_open_port()
    raise RuntimeError("_DRAGON_DEVICE_OFFSET not set and original get_open_port unavailable")


def patch_get_open_port():
    """Replace vLLM's ``get_open_port`` with a device-index-based version.

    Patches the function in both its canonical module
    (``vllm.utils.network_utils``) and in the multiproc executor module
    where it has already been imported as a local name.  No-op outside the
    Dragon inference service.
    """
    if not _in_dragon_inference_runtime():
        return

    global _original_get_open_port
    import importlib

    # Patch canonical location and save original for fallback
    try:
        net_utils = importlib.import_module("vllm.utils.network_utils")
        _original_get_open_port = net_utils.get_open_port
        net_utils.get_open_port = _device_based_get_open_port
    except (ImportError, AttributeError):
        pass

    # Patch the re-export in vllm.utils (for older import paths)
    try:
        vllm_utils = importlib.import_module("vllm.utils")
        if hasattr(vllm_utils, "get_open_port"):
            vllm_utils.get_open_port = _device_based_get_open_port
    except (ImportError, AttributeError):
        pass

    # Patch the already-imported local reference in the executor module
    try:
        mpe = importlib.import_module("vllm.v1.executor.multiproc_executor")
        if hasattr(mpe, "get_open_port"):
            mpe.get_open_port = _device_based_get_open_port
    except (ImportError, AttributeError):
        pass


def _dragon_get_mp_context(*args, **kwargs):
    """Return the active Dragon multiprocessing context.

    vLLM's ``get_mp_context`` requests a *named* context (``spawn`` or
    ``fork``).  Inside the Dragon runtime the start method is already selected,
    so any named request must be ignored in favor of the active context
    returned by ``multiprocessing.get_context()`` with no arguments.

    :returns: The active multiprocessing context.
    :rtype: multiprocessing.context.BaseContext
    """
    import multiprocessing

    return multiprocessing.get_context()


def patch_mp_context():
    """Force vLLM to inherit the active Dragon multiprocessing context.

    vLLM's ``get_mp_context`` is replaced wherever it is exposed so that
    vLLM inherits the Dragon multiprocessing context selected by the runtime.

    The function is patched in its canonical module
    (``vllm.utils.system_utils`` for vLLM >= 0.12, ``vllm.utils`` for earlier
    releases) and in the multiproc executor module where it has already been
    imported as a local name.  The originals are stored as
    ``_original_get_mp_context`` on each patched module for inspection or
    future restoration.  No-op outside the Dragon inference service.
    """
    if not _in_dragon_inference_runtime():
        return

    import importlib

    # ``get_mp_context`` has lived in different modules across vLLM releases.
    # ``vllm.utils.system_utils`` is canonical for >= 0.12; ``vllm.utils`` is
    # both the older home (utils/__init__.py) and a re-export.
    for module_name in ("vllm.utils.system_utils", "vllm.utils"):
        try:
            mod = importlib.import_module(module_name)
        except ImportError:
            continue
        if hasattr(mod, "get_mp_context"):
            if not hasattr(mod, "_original_get_mp_context"):
                mod._original_get_mp_context = mod.get_mp_context
            mod.get_mp_context = _dragon_get_mp_context

    # Patch the already-imported local reference in the executor module, which
    # binds ``get_mp_context`` at import time and calls it when spawning workers.
    try:
        mpe = importlib.import_module("vllm.v1.executor.multiproc_executor")
        if hasattr(mpe, "get_mp_context"):
            if not hasattr(mpe, "_original_get_mp_context"):
                mpe._original_get_mp_context = mpe.get_mp_context
            mpe.get_mp_context = _dragon_get_mp_context
    except (ImportError, AttributeError):
        pass


def patch_multiproc_executor():
    """Patch vLLM's multiprocess worker startup path.

    The selected Dragon implementation replaces ``WorkerProc.worker_main`` and
    ``WorkerProc.wait_for_ready``.  The originals are kept on the class as
    ``_original_worker_main`` and ``_original_wait_for_ready`` for inspection or
    future restoration.  No-op outside the Dragon inference service.
    """
    if not _in_dragon_inference_runtime():
        return

    from vllm.v1.executor.multiproc_executor import WorkerProc

    if VLLM_VERSION >= version.parse("0.15.0"):
        from .dragon_executor_v015 import dragon_worker_main
        from .dragon_executor_v015 import dragon_wait_for_ready
    else:  # 0.12.x
        from .dragon_executor import dragon_worker_main
        from .dragon_executor import dragon_wait_for_ready

    # Store original function for potential restoration
    WorkerProc._original_worker_main = WorkerProc.worker_main
    WorkerProc._original_wait_for_ready = WorkerProc.wait_for_ready

    # Apply the patch
    WorkerProc.worker_main = dragon_worker_main
    WorkerProc.wait_for_ready = dragon_wait_for_ready

    # Also patch get_open_port and get_mp_context now that the executor module
    # is imported and has bound both as local names.
    patch_get_open_port()
    patch_mp_context()


def _apply_subprocess_patches():
    """Apply all Dragon patches needed inside a vLLM subprocess.

    Called at the start of engine core and worker subprocesses to ensure
    Dragon-compatible behavior.  Safe to call multiple times; patches check
    for existing originals before re-applying.
    """
    patch_get_open_port()
    patch_mp_context()
    patch_multiproc_executor()


def patch_engine_core():
    """Patch vLLM's V1 engine core subprocess startup for AsyncLLM compatibility.

    The V1 engine (used by AsyncLLM) spawns ``EngineCoreProc`` as a subprocess
    via ``run_engine_core``.  This patch wraps that function to apply all
    necessary Dragon patches inside the subprocess before the engine core
    creates its executor and workers.

    Without this patch, AsyncLLM fails because the subprocess doesn't have
    Dragon-compatible multiprocessing patches applied.

    No-op outside the Dragon inference service.
    """
    if not _in_dragon_inference_runtime():
        return

    import importlib

    try:
        core_module = importlib.import_module("vllm.v1.engine.core")
    except ImportError:
        # V1 engine not available in this vLLM version
        return

    if not hasattr(core_module, "run_engine_core"):
        return

    # Already patched
    if hasattr(core_module, "_original_run_engine_core"):
        return

    original_run_engine_core = core_module.run_engine_core

    def dragon_run_engine_core(*args, **kwargs):
        """Dragon-wrapped engine core subprocess entry point.

        Applies all necessary patches before running the original
        ``run_engine_core`` to ensure Dragon-compatible multiprocessing.
        """
        # Apply patches at subprocess startup
        _apply_subprocess_patches()

        # Run the original engine core
        return original_run_engine_core(*args, **kwargs)

    # Store original and apply patch
    core_module._original_run_engine_core = original_run_engine_core
    core_module.run_engine_core = dragon_run_engine_core
