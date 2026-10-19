#include "gpu.hpp"
#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#ifdef __cplusplus
extern "C" {
#endif

bool dragon_gpu_debug = false;
FILE* dragon_gpu_log = nullptr;
void* ldragon_gpu_handle = nullptr;

static std::string
_get_dragon_config_value(const std::string& config_key)
{
    const std::string command = "dragon-config --get='" + config_key + "'";
    FILE* pipe = popen(command.c_str(), "r");
    std::string result;
    if (pipe != nullptr) {
        char buffer[128];
        while (fgets(buffer, sizeof(buffer), pipe) != nullptr)
            result += buffer;
        pclose(pipe);
    }

    // Trim leading and trailing whitespace from the result.
    const auto first_char = result.find_first_not_of(" \t\n\r");
    if (first_char == std::string::npos)
        return "";
    result = result.substr(first_char, result.find_last_not_of(" \t\n\r") - first_char + 1);

    // Remove surrounding single quotes if present.
    if (result.size() >= 2 && result.front() == '\'' && result.back() == '\'')
        result = result.substr(1, result.size() - 2);

    return result;
}

static void*
_try_dlopen(const std::string& lib)
{
    void* libhandle = dlopen(lib.c_str(), RTLD_LAZY | RTLD_GLOBAL);
    if (libhandle == nullptr && dragon_gpu_debug) {
        const char* error = dlerror();
        fprintf(dragon_gpu_log, "failed to dlopen %s: %s\n", lib.c_str(),
                error != nullptr ? error : "unknown error");
        fflush(dragon_gpu_log);
    }
    return libhandle;
}

static void*
_try_dlopen_in_dir(const std::string& lib_dir, const std::string& lib)
{
    if (lib_dir.empty())
        return nullptr;

    std::string lib_path = lib_dir;
    // Allow the runtime lib option to point directly at the library file.
    if (lib_dir.find(lib) == std::string::npos) {
        if (lib_path.back() != '/')
            lib_path += '/';
        lib_path += lib;
    }
    return _try_dlopen(lib_path);
}

void*
_open_gpu_runtime_lib(const char* config_key, const char* libname)
{
    // e.g. "cuda_runtime_lib" -> "cuda_runtime_libname"
    const std::string name = _get_dragon_config_value(std::string(config_key) + "name");
    const std::string lib_dir = _get_dragon_config_value(config_key);
    void* libhandle = nullptr;

    if (!name.empty()) {
        libhandle = _try_dlopen(name);

        // For a full path, retry with just its filename via the LD cache and library path env vars.
        const std::string base_name = name.substr(name.find_last_of('/') + 1);
        if (libhandle == nullptr && base_name != name)
            libhandle = _try_dlopen(base_name);

        if (libhandle == nullptr)
            libhandle = _try_dlopen_in_dir(lib_dir, base_name);
    }

    // Default library: try within lib_dir, then the standard library paths.
    if (libhandle == nullptr)
        libhandle = _try_dlopen_in_dir(lib_dir, libname);
    if (libhandle == nullptr)
        libhandle = _try_dlopen(libname);

    return libhandle;
}

/**
 * @brief Get the type of GPU backend avialable on this node
 *
 * @param backend_type OUT constant indicating which vendor backend to use
 *
 * @return An error code for the operation. DRAGON_SUCCESS upon success.
 */

dragonError_t
dragon_gpu_detect_backend(dragonGPUHandle_t* gpuh)
{

    // Set the default value:
    gpuh->backend_type = DRAGON_GPU_BACKEND_UNDETECTED;

    // Loop through the other options and see if we pick anything up.
    for (int i = 0; i < DRAGON_GPU_BACKEND_END; i++) {

        dragonGPUBackend_t l_backend_type = static_cast<dragonGPUBackend_t>(i);
        if (dragon_gpu_debug) {
            fprintf(dragon_gpu_log, "checking for GPU backend %d\n", (int)l_backend_type);
            fflush(dragon_gpu_log);
        }

        switch (l_backend_type) {
            case DRAGON_GPU_BACKEND_CUDA: {
#ifdef HAVE_CUDA_INCLUDE
                auto libhandle = _open_cuda_lib();

                // if libhandle is non-null, see if we can detect devices
                if (libhandle) {
                    _setup_cuda(libhandle, gpuh);
                    // if dgpu is null we didn't detect any CUDA devices. So we may just
                    // be on a system with CUDA libraries installed but no actual devices.
                    if (gpuh->dgpu != nullptr) {
                        gpuh->backend_type = DRAGON_GPU_BACKEND_CUDA;
                    }
                }
#endif // HAVE_CUDA_INCLUDE
                break;
            }
            case DRAGON_GPU_BACKEND_HIP: {
#ifdef HAVE_HIP_INCLUDE
                auto libhandle = _open_hip_lib();
                if (libhandle) {
                    _setup_hip(libhandle, gpuh);
                    // if dgpu is null we didn't detect any AMD devices. So we may just
                    // be on a system with HIP libraries installed but no actual devices.
                    if (gpuh->dgpu != nullptr) {
                        gpuh->backend_type = DRAGON_GPU_BACKEND_HIP;
                    }
                }
#endif // HAVE_HIP_INCLUDE
                break;
            }
            case DRAGON_GPU_BACKEND_ZE: {
#ifdef HAVE_ZE_INCLUDE
                auto libhandle = _open_ze_lib();
                if (libhandle) {
                    _setup_ze(libhandle, gpuh);
                    // if dgpu is null we didn't detect any ZE devices. So we may just
                    // be on a system with ZE libraries installed but no actual devices.
                    if (gpuh->dgpu != nullptr) {
                        gpuh->backend_type = DRAGON_GPU_BACKEND_ZE;
                    }
                }
#endif // HAVE_ZE_INCLUDE
                break;
            }
            case DRAGON_GPU_BACKEND_UNDETECTED:
            case DRAGON_GPU_BACKEND_END: {
                break;
            }
        }
        if (gpuh->dgpu != nullptr) {
            if (dragon_gpu_debug) {
                fprintf(dragon_gpu_log, "detected GPU backend %d with %d devices \n", (int)gpuh->backend_type,
                        gpuh->dgpu->get_device_count());
                fflush(dragon_gpu_log);
            }
            no_err_return(DRAGON_SUCCESS);
        }
    }

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_gpu_get_handle(void** dragon_gpu_handle)
{
    if (ldragon_gpu_handle == nullptr) {
        ldragon_gpu_handle = new dragonGPUHandle_t();
    }
    if (ldragon_gpu_handle == nullptr) {
        append_err_return(DRAGON_GPU_ERROR, "failed to allocate dragonGPUHandle_t");
    }

    *dragon_gpu_handle = ldragon_gpu_handle;

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_gpu_get_backend_type(void* dragon_gpu_handle, dragonGPUBackend_t* backend_type)
{
    if (dragon_gpu_handle == nullptr) {
        append_err_return(DRAGON_INVALID_ARGUMENT,
                          "dragon_gpu_handle is null when trying to get backend type.");
    }
    dragonGPUHandle_t* gpuh;
    gpuh = static_cast<dragonGPUHandle_t*>(dragon_gpu_handle);
    if (gpuh->backend_type < DRAGON_GPU_BACKEND_UNDETECTED && gpuh->backend_type > DRAGON_GPU_BACKEND_END) {
        append_err_return(DRAGON_GPU_ERROR,
                          "GPU backend has a wholly undefined type. Call dragon_gpu_setup first.");
    }
    *backend_type = gpuh->backend_type;
    no_err_return(DRAGON_SUCCESS);
}

/**
 * @brief Set up a GPU backend and obtain a handle to it.
 *
 * @param backend_type IN constant indicating which vendor backend to use
 * @param gpuh OUT handle to the GPU
 *
 * @return An error code for the operation. DRAGON_SUCCESS upon success.
 */

dragonError_t
dragon_gpu_setup(void** dragon_gpu_handle)
{

    // Make sure we have a handle to the GPU. If not, try allocating one.
    dragonError_t derr;
    if (*dragon_gpu_handle == nullptr) {
        derr = dragon_gpu_get_handle(dragon_gpu_handle);
        if (derr != DRAGON_SUCCESS)
            append_err_return(derr, "failed to get dragon_gpu_handle");
    }

    dragonGPUHandle_t* gpuh = static_cast<dragonGPUHandle_t*>(*dragon_gpu_handle);

    // If we already have a GPU backend, just return success.
    if (gpuh->dgpu != nullptr) {
        no_err_return(DRAGON_SUCCESS);
    }

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
        snprintf(dbg_filename, 256, "dragon_gpu.%s.%d.log", my_hostname, getpid());
        dragon_gpu_log = fopen(dbg_filename, "w");
        if (dragon_gpu_log == nullptr) {
            append_err_return(DRAGON_FAILURE, "failed to open Dragon GPU debug log");
        }
    }

    // set up gpu library
    dragon_gpu_detect_backend(gpuh);

    if (dragon_gpu_debug) {
        fprintf(dragon_gpu_log, "exiting dragon_gpu_setup\n");
        fflush(dragon_gpu_log);
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
dragon_gpu_cleanup(void** dragon_gpu_handle)
{
    if (*dragon_gpu_handle == nullptr) {
        append_err_return(DRAGON_INVALID_ARGUMENT, "dragon_gpu_handle is null when trying to clean up.");
    }
    dragonGPUHandle_t* gpuh;
    gpuh = static_cast<dragonGPUHandle_t*>(*dragon_gpu_handle);
    if (dragon_gpu_debug) {
        fprintf(dragon_gpu_log, "dragon_gpu_cleanup called\n");
        fflush(dragon_gpu_log);
    }

    gpuh->lock.acquire();
    gpuh->dgpu.reset();
    gpuh->lock.release();
    if (ldragon_gpu_handle == gpuh) {
        ldragon_gpu_handle = nullptr;
    }
    delete gpuh;
    *dragon_gpu_handle = nullptr;
    if (dragon_gpu_debug) {
        if (dragon_gpu_log != nullptr) {
            fclose(dragon_gpu_log);
            dragon_gpu_log = nullptr;
        }
    }
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
dragon_gpu_mem_alloc(void* dragon_gpu_handle, void** addr, size_t size)
{
    if (dragon_gpu_debug) {
        fprintf(dragon_gpu_log, "dragon_gpu_mem_alloc called\n");
        fflush(dragon_gpu_log);
    }
    dragonGPUHandle_t* gpuh;
    gpuh = static_cast<dragonGPUHandle_t*>(dragon_gpu_handle);
    if (dragon_gpu_debug) {
        fprintf(dragon_gpu_log, "dragon_gpu_mem_alloc called\n");
        fflush(dragon_gpu_log);
    }
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
dragon_gpu_mem_free(void* dragon_gpu_handle, void* addr)
{
    if (dragon_gpu_debug) {
        fprintf(dragon_gpu_log, "dragon_gpu_mem_free called\n");
        fflush(dragon_gpu_log);
    }
    dragonGPUHandle_t* gpuh;
    gpuh = static_cast<dragonGPUHandle_t*>(dragon_gpu_handle);
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
dragon_gpu_get_ipc_handle(void* dragon_gpu_handle, void* addr, void** dragon_ipc_handle)
{
    if (dragon_gpu_debug) {
        fprintf(dragon_gpu_log, "dragon_gpu_get_ipc_handle called\n");
        fflush(dragon_gpu_log);
    }
    dragonGPUHandle_t* gpuh;
    gpuh = static_cast<dragonGPUHandle_t*>(dragon_gpu_handle);
    gpuh->lock.acquire();
    auto derr = gpuh->dgpu->get_ipc_handle(addr, dragon_ipc_handle);
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
dragon_gpu_free_ipc_handle(void* dragon_gpu_handle, void** dragon_ipc_handle)
{
    if (dragon_gpu_debug) {
        fprintf(dragon_gpu_log, "dragon_gpu_free_ipc_handle called\n");
        fflush(dragon_gpu_log);
    }
    dragonGPUHandle_t* gpuh;
    gpuh = static_cast<dragonGPUHandle_t*>(dragon_gpu_handle);
    gpuh->lock.acquire();
    auto derr = gpuh->dgpu->free_ipc_handle(dragon_ipc_handle);
    free(*dragon_ipc_handle);
    *dragon_ipc_handle = nullptr;
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
dragon_gpu_attach(void* dragon_gpu_handle, void* dragon_ipc_handle, void** addr)
{
    if (dragon_gpu_debug) {
        fprintf(dragon_gpu_log, "dragon_gpu_attach called\n");
        fflush(dragon_gpu_log);
    }
    dragonGPUHandle_t* gpuh;
    gpuh = static_cast<dragonGPUHandle_t*>(dragon_gpu_handle);
    gpuh->lock.acquire();
    auto derr = gpuh->dgpu->attach(dragon_ipc_handle, addr);
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
dragon_gpu_detach(void* dragon_gpu_handle, void* addr)
{
    if (dragon_gpu_debug) {
        fprintf(dragon_gpu_log, "dragon_gpu_detach called\n");
        fflush(dragon_gpu_log);
    }
    if (addr == nullptr) {
        if (dragon_gpu_debug) {
            fprintf(dragon_gpu_log, "dragon_gpu_detach called on null pointer address\n");
            fflush(dragon_gpu_log);
        }
        no_err_return(DRAGON_SUCCESS);
    }
    dragonGPUHandle_t* gpuh;
    gpuh = static_cast<dragonGPUHandle_t*>(dragon_gpu_handle);
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
dragon_gpu_copy(void* dragon_gpu_handle, void* dst_addr, const void* src_addr, size_t size,
                dragonGPUMemcpyType_t memcpy_type)
{
    dragonGPUHandle_t* gpuh;
    gpuh = static_cast<dragonGPUHandle_t*>(dragon_gpu_handle);
    if (dragon_gpu_debug) {
        fprintf(dragon_gpu_log, "dragon_gpu_copy called with cpy %d\n", (int)memcpy_type);
        fflush(dragon_gpu_log);
    }
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
dragon_gpu_memset(void* dragon_gpu_handle, void* addr, int val, size_t size)
{
    if (dragon_gpu_debug) {
        fprintf(dragon_gpu_log, "dragon_gpu_memset called\n");
        fflush(dragon_gpu_log);
    }
    dragonGPUHandle_t* gpuh;
    gpuh = static_cast<dragonGPUHandle_t*>(dragon_gpu_handle);
    gpuh->lock.acquire();
    auto derr = gpuh->dgpu->memset(addr, val, size);
    gpuh->lock.release();
    return derr;
}

dragonError_t
dragon_gpu_host_register(void* dragon_gpu_handle, void* addr, size_t size)
{
    if (dragon_gpu_debug) {
        fprintf(dragon_gpu_log, "dragon_gpu_host_register called\n");
        fflush(dragon_gpu_log);
    }
    dragonGPUHandle_t* gpuh;
    gpuh = static_cast<dragonGPUHandle_t*>(dragon_gpu_handle);
    gpuh->lock.acquire();
    auto derr = gpuh->dgpu->host_register(addr, size);
    gpuh->lock.release();
    return derr;
}

dragonError_t
dragon_gpu_host_unregister(void* dragon_gpu_handle, void* addr)
{
    if (dragon_gpu_debug) {
        fprintf(dragon_gpu_log, "dragon_gpu_host_unregister called\n");
        fflush(dragon_gpu_log);
    }
    dragonGPUHandle_t* gpuh;
    gpuh = static_cast<dragonGPUHandle_t*>(dragon_gpu_handle);
    gpuh->lock.acquire();
    auto derr = gpuh->dgpu->host_unregister(addr);
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
dragon_gpu_get_errstr(void* dragon_gpu_handle, const char* event, int rc, char* errstr, int strlen)
{
    dragonGPUHandle_t* gpuh;
    gpuh = static_cast<dragonGPUHandle_t*>(dragon_gpu_handle);
    gpuh->lock.acquire();
    strncpy(errstr, gpuh->dgpu->get_errstr(event, rc).c_str(), strlen);
    gpuh->lock.release();
}

size_t
dragon_gpu_get_ipc_handle_size(void* dragon_gpu_handle)
{
    dragonGPUHandle_t* gpuh;
    gpuh = static_cast<dragonGPUHandle_t*>(dragon_gpu_handle);
    return gpuh->dgpu->get_ipc_handle_size();
}

int
dragon_gpu_get_device_count(void* dragon_gpu_handle)
{

    dragonGPUHandle_t* gpuh;
    gpuh = static_cast<dragonGPUHandle_t*>(dragon_gpu_handle);

    if (gpuh->dgpu == nullptr) {
        return 0;
    }
    return gpuh->dgpu->get_device_count();
}

dragonError_t
dragon_gpu_get_device_list(void* dragon_gpu_handle, int** int_device_list, double** double_device_list,
                           int* num_devices)
{
    dragonGPUHandle_t* gpuh;
    gpuh = static_cast<dragonGPUHandle_t*>(dragon_gpu_handle);

    *int_device_list = nullptr;
    *double_device_list = nullptr;

    if (gpuh->dgpu == nullptr) {
        append_err_return(DRAGON_GPU_ERROR, "dragon_gpu_get_device_list called before dragon_gpu_setup");
    }

    // devices are plain integer indices unless a backend needs fractional sub-device IDs (e.g. MIG
    // partitions), in which case double_device_list would be filled instead and int_device_list left NULL
    dragonError_t derr = gpuh->dgpu->get_device_list(int_device_list, double_device_list, num_devices);
    if (derr != DRAGON_SUCCESS && derr != DRAGON_GPU_NO_DEVICES) {
        append_err_return(derr, "failed to get device list");
    }

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_gpu_set_device(void* dragon_gpu_handle, int deviceID)
{
    dragonGPUHandle_t* gpuh;
    gpuh = static_cast<dragonGPUHandle_t*>(dragon_gpu_handle);

    if (gpuh->dgpu == nullptr) {
        append_err_return(DRAGON_GPU_ERROR, "dragon_gpu_set_device called before dragon_gpu_setup");
    }
    return gpuh->dgpu->set_device(deviceID);
}

dragonError_t
dragon_gpu_get_uuid(void* dragon_gpu_handle, int devID, char** uuid)
{
    dragonGPUHandle_t* gpuh;
    gpuh = static_cast<dragonGPUHandle_t*>(dragon_gpu_handle);

    if (gpuh->dgpu == nullptr) {
        append_err_return(DRAGON_GPU_ERROR, "dragon_gpu_get_uuid called before dragon_gpu_setup");
    }
    return gpuh->dgpu->get_uuid(devID, uuid);
}

#ifdef __cplusplus
} // extern "C"
#endif
