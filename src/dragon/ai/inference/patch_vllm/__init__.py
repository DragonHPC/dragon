def patch_engine_utils():
    """Patch engine utils for Dragon compatibility."""
    import vllm.v1.engine.utils as utils_module
    from .dragon_engine_utils import dragon_wait_for_engine_startup
    
    # Store original for potential restoration
    utils_module._original_wait_for_engine_startup = utils_module.wait_for_engine_startup
    
    # Apply Dragon version
    utils_module.wait_for_engine_startup = dragon_wait_for_engine_startup

def patch_multiproc_executor():
    """Apply the monkey patch to vllm.v1.engine.utils"""

    from vllm.v1.executor.multiproc_executor import WorkerProc
    
    from .dragon_executor import dragon_worker_main
    from .dragon_executor import dragon_wait_for_ready

    # Store original function for potential restoration
    WorkerProc._original_worker_main = WorkerProc.worker_main
    WorkerProc._original_wait_for_ready = WorkerProc.wait_for_ready

    # Apply the patch
    WorkerProc.worker_main = dragon_worker_main
    WorkerProc.wait_for_ready = dragon_wait_for_ready