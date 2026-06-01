from packaging import version
import vllm

VLLM_VERSION = version.parse(vllm.__version__)


def patch_engine_utils():
    """Patch engine utils for Dragon compatibility."""
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
    set (e.g. when vLLM is used outside Dragon).
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
    where it has already been imported as a local name.
    """
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


def patch_multiproc_executor():
    """Apply the monkey patch to vllm.v1.engine.utils"""

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

    # Also patch get_open_port now that the executor module is imported
    patch_get_open_port()
