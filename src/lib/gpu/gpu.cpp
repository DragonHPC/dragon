#include "gpu.hpp"

bool dragon_gpu_debug = false;
FILE *dragon_gpu_log = nullptr;

/**
 * @brief Set up a GPU backend and obtain a handle to it.
 *
 * @param backend_type IN constant indicating which vendor backend to use
 * @param gpuh OUT handle to the GPU
 *
 * @return An error code for the operation. DRAGON_SUCCESS upon success.
 */

dragonError_t
dragon_gpu_setup(dragonGPUBackend_t backend_type, dragonGPUHandle_t *gpuh)
{
    gpuh->lock.acquire();

    // set up debugging log file

    auto tmp_envstr = getenv("_DRAGON_GPU_DEBUG");
    if (tmp_envstr != nullptr) {
        dragon_gpu_debug = bool(atoi(tmp_envstr));
    }

    if (dragon_gpu_debug) {
        char dbg_filename[256];
        char my_hostname[128];
        gethostname(my_hostname, 128);
        sprintf(dbg_filename, "dragon_gpu.%s.%d.log", my_hostname, getpid());
        dragon_gpu_log = fopen(dbg_filename, "w");
        if (dragon_gpu_log == nullptr) {
            append_err_return(DRAGON_FAILURE, "failed to open Dragon GPU debug log");
        }
    }

    // set up gpu library

    switch (backend_type) {
#ifdef HAVE_CUDA_INCLUDE
        case DRAGON_GPU_BACKEND_CUDA: {
            auto libhandle = dragon_gpu_open_cuda_lib();
            if (libhandle) {
                dragon_gpu_setup_cuda(libhandle, gpuh);
            } else {
                append_err_return(DRAGON_FAILURE, "failed to dlopen CUDA backend library");
            }
            break;
        }
#endif // HAVE_CUDA_INCLUDE
#ifdef HAVE_HIP_INCLUDE
        case DRAGON_GPU_BACKEND_HIP: {
            auto libhandle = dragon_gpu_open_hip_lib();
            if (libhandle) {
                dragon_gpu_setup_hip(libhandle, gpuh);
            } else {
                append_err_return(DRAGON_FAILURE, "failed to dlopen HIP backend library");
            }
            break;
        }
#endif // HAVE_HIP_INCLUDE
#ifdef HAVE_ZE_INCLUDE
        case DRAGON_GPU_BACKEND_ZE: {
            auto libhandle = dragon_gpu_open_ze_lib();
            if (libhandle) {
                dragon_gpu_setup_ze(libhandle, gpuh);
            } else {
                append_err_return(DRAGON_FAILURE, "failed to dlopen ZE backend library");
            }
            break;
        }
#endif // HAVE_ZE_INCLUDE
        default: {
            append_err_return(DRAGON_FAILURE, "invalid GPU backend type");
        }
    }

    if (gpuh->dgpu != nullptr) {
        gpuh->lock.release();
        no_err_return(DRAGON_SUCCESS);
    } else {
        gpuh->lock.release();
        err_return(DRAGON_FAILURE, "no GPU detected");
    }
}

/**
 * @brief Clean up resources for a GPU backend.
 *
 * @param gpuh IN handle to the GPU
 *
 * @return An error code for the operation. DRAGON_SUCCESS upon success.
 */

dragonError_t
dragon_gpu_cleanup(dragonGPUHandle_t *gpuh)
{
    gpuh->lock.acquire();
    gpuh->dgpu.reset();
    gpuh->lock.release();
    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Allocate memory on the device specified by @ref gpuh.
 *
 * @param gpuh IN handle to the GPU
 * @param addr OUT pointer to the base of the memory allocation
 * @param size IN size of the memory allocation
 *
 * @return An error code for the operation. DRAGON_SUCCESS upon success.
 */

dragonError_t
dragon_gpu_mem_alloc(dragonGPUHandle_t *gpuh, void **addr, size_t size)
{
    gpuh->lock.acquire();
    auto derr = gpuh->dgpu->mem_alloc(addr, size);
    gpuh->lock.release();
    return derr;
}

/**
 * @brief Free memory on the device specified by @ref gpuh.
 *
 * @param gpuh IN handle to the GPU
 * @param addr IN pointer to the base of the memory to be freed
 *
 * @return An error code for the operation. DRAGON_SUCCESS upon success.
 */

dragonError_t
dragon_gpu_mem_free(dragonGPUHandle_t *gpuh, void *addr)
{
    gpuh->lock.acquire();
    auto derr = gpuh->dgpu->mem_free(addr);
    gpuh->lock.release();
    return derr;
}

/**
 * @brief Get an IPC handle for a memory allocation that can be shared with other processes.
 *
 * @param gpuh IN handle to the GPU
 * @param addr IN pointer to the base of the allocation for the IPC handle
 * @param ipc_handle OUT the IPC handle
 *
 * @return An error code for the operation. DRAGON_SUCCESS upon success.
 */

dragonError_t
dragon_gpu_get_ipc_handle(dragonGPUHandle_t *gpuh, void *addr, dragonIPCHandle_t *ipc_handle)
{
    gpuh->lock.acquire();
    auto derr = gpuh->dgpu->get_ipc_handle(addr, ipc_handle->data);
    gpuh->lock.release();
    return derr;
}

/**
 * @brief Free an IPC handle.
 *
 * The process that calls @ref dragon_gpu_get_ipc_handle must call this function
 * to clean up the IPC handle.
 *
 * @param gpuh IN handle to the GPU
 * @param ipc_handle IN the IPC handle
 *
 * @return An error code for the operation. DRAGON_SUCCESS upon success.
 */

dragonError_t
dragon_gpu_free_ipc_handle(dragonGPUHandle_t *gpuh, dragonIPCHandle_t *ipc_handle)
{
    gpuh->lock.acquire();
    auto derr = gpuh->dgpu->free_ipc_handle(ipc_handle->data);
    gpuh->lock.release();
    return derr;
}

/**
 * @brief Use an IPC handle to attach to an inter-process memory allocation.
 *
 * A process that receives an IPC handle can use this function to attach to an
 * inter-process memory allocation. Once attached, this process can access the
 * allocation using a local virtual address segment.
 *
 * @param gpuh IN handle to the GPU
 * @param ipc_handle IN the IPC handle
 * @param addr OUT pointer to the base of the allocation for the IPC handle
 *
 * @return An error code for the operation. DRAGON_SUCCESS upon success.
 */

dragonError_t
dragon_gpu_attach(dragonGPUHandle_t *gpuh, dragonIPCHandle_t *ipc_handle, void **addr)
{
    gpuh->lock.acquire();
    auto derr = gpuh->dgpu->attach(ipc_handle->data, addr);
    gpuh->lock.release();
    return derr;
}

/**
 * @brief Detach from an inter-process memory allocation.
 *
 * Once a process detaches from an inter-process memory allocation, it will
 * lose access to the allocation, but the allocation will not be freed. The
 * process that originally called @ref dragon_gpu_mem_alloc must free the
 * memory.
 *
 * @param gpuh IN handle to the GPU
 * @param addr IN pointer to the base of the allocation to detach from
 *
 * @return An error code for the operation. DRAGON_SUCCESS upon success.
 */

dragonError_t
dragon_gpu_detach(dragonGPUHandle_t *gpuh, void *addr)
{
    gpuh->lock.acquire();
    auto derr = gpuh->dgpu->detach(addr);
    gpuh->lock.release();
    return derr;
}

/**
 * @brief Copy data between device buffers, or between device and host buffers.
 *
 * The direction of the memory copy is determined by the @ref memcpy_type parameter.
 * The direction can be either device-to-device (DRAGON_GPU_D2D), device-to-host (DRAGON_GPU_D2H)
 * or host-to-device (DRAGON_GPU_H2D).
 *
 * @param gpuh IN handle to the GPU
 * @param dst_addr INOUT the destination buffer
 * @param src_addr IN the source buffer
 * @param size IN size in bytes of the data to be copied
 * @param memcpy_type IN direction of the memory copy 
 *
 * @return An error code for the operation. DRAGON_SUCCESS upon success.
 */

dragonError_t
dragon_gpu_copy(dragonGPUHandle_t *gpuh, void *dst_addr, const void *src_addr, size_t size, dragonGPUMemcpyType_t memcpy_type)
{
    gpuh->lock.acquire();
    auto derr = gpuh->dgpu->copy(dst_addr, src_addr, size, memcpy_type);
    gpuh->lock.release();
    return derr;
}

/**
 * @brief Update the values stored in a buffer.
 *
 * @param gpuh IN handle to the GPU
 * @param addr INOUT the buffer to be updated
 * @param val IN value between 0 and 255 to use for each byte in the buffer 
 * @param size IN size in bytes of the data to updated
 *
 * @return An error code for the operation. DRAGON_SUCCESS upon success.
 */

dragonError_t
dragon_gpu_memset(dragonGPUHandle_t *gpuh, void *addr, int val, size_t size)
{
    gpuh->lock.acquire();
    auto derr = gpuh->dgpu->memset(addr, val, size);
    gpuh->lock.release();
    return derr;
}

/**
 * @brief Get an error string corresponding to a return code.
 *
 * @param gpuh IN handle to the GPU
 * @param event IN C string describing the operation
 * @param rc IN the return code
 * @param errstr INOUT C string to store the description of the return code
 * @param strlen IN maximum number of bytes that can be stored in @ref errstr
 */

// TODO: this function needs work (including the backend implementations for it)
void
dragon_gpu_get_errstr(dragonGPUHandle_t *gpuh, const char *event, int rc, char *errstr, int strlen)
{
    gpuh->lock.acquire();
    strncpy(errstr, gpuh->dgpu->get_errstr(event, rc).c_str(), strlen);
    gpuh->lock.release();
}

