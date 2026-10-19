#ifndef HAVE_DRAGON_GPU_C_API_H
#define HAVE_DRAGON_GPU_C_API_H

#include "../err.h"
#include "dragon/return_codes.h"
#include "dragon/shared_lock.h"
#include "dragon/utils.h"
#include <assert.h>
#include <dlfcn.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#ifdef __cplusplus
extern "C" {
#endif

extern bool dragon_gpu_debug;
extern FILE* dragon_gpu_log;

typedef enum dragonGPUBackend_t {
    DRAGON_GPU_BACKEND_UNDETECTED = 0,
    DRAGON_GPU_BACKEND_CUDA,
    DRAGON_GPU_BACKEND_HIP,
    DRAGON_GPU_BACKEND_ZE,
    DRAGON_GPU_BACKEND_END
} dragonGPUBackend_t;

typedef enum dragonGPUMemcpyType_t {
    DRAGON_GPU_D2D = 0,
    DRAGON_GPU_D2H,
    DRAGON_GPU_H2D
} dragonGPUMemcpyType_t;

dragonError_t
dragon_gpu_get_handle(void** dragon_gpu_handle);

dragonError_t
dragon_gpu_setup(void** dragon_gpu_handle);

dragonError_t
dragon_gpu_get_backend_type(void* dragon_gpu_handle, dragonGPUBackend_t* backend_type);

dragonError_t
dragon_gpu_cleanup(void** dragon_gpu_handle);

dragonError_t
_setup_cuda(void* libhandle, void* dragon_gpu_handle);

dragonError_t
_setup_hip(void* libhandle, void* dragon_gpu_handle);

dragonError_t
_setup_ze(void* libhandle, void* dragon_gpu_handle);

void*
_open_cuda_lib();

void*
_open_gpu_runtime_lib(const char* config_key, const char* libname);

void*
_open_hip_lib();

void*
_open_ze_lib();

dragonError_t
_resolve_cuda_symbols(void* libhandle);

dragonError_t
_resolve_hip_symbols(void* libhandle);

dragonError_t
_resolve_ze_symbols(void* libhandle);

dragonError_t
dragon_gpu_mem_alloc(void* dragon_gpu_handle, void** addr, size_t size);

dragonError_t
dragon_gpu_mem_free(void* dragon_gpu_handle, void* addr);

dragonError_t
dragon_gpu_get_ipc_handle(void* dragon_gpu_handle, void* addr, void** dragon_ipc_handle);

size_t
dragon_gpu_get_ipc_handle_size(void* dragon_gpu_handle);

dragonError_t
dragon_gpu_free_ipc_handle(void* dragon_gpu_handle, void** dragon_ipc_handle);

dragonError_t
dragon_gpu_attach(void* dragon_gpu_handle, void* dragon_ipc_handle, void** addr);

dragonError_t
dragon_gpu_detach(void* dragon_gpu_handle, void* addr);

dragonError_t
dragon_gpu_copy(void* dragon_gpu_handle, void* dst_addr, const void* src_addr, size_t size,
                dragonGPUMemcpyType_t memcpy_type);

dragonError_t
dragon_gpu_memset(void* dragon_gpu_handle, void* addr, int val, size_t num_bytes);

dragonError_t
dragon_gpu_host_register(void* dragon_gpu_handle, void* addr, size_t size);

dragonError_t
dragon_gpu_host_unregister(void* dragon_gpu_handle, void* addr);

void
dragon_gpu_get_errstr(void* dragon_gpu_handle, const char* event, int rc, char* errstr, int strlen);

int
dragon_gpu_get_device_count(void* dragon_gpu_handle);

dragonError_t
dragon_gpu_get_device_list(void* dragon_gpu_handle, int** int_device_list, double** double_device_list,
                           int* num_devices);

dragonError_t
dragon_gpu_set_device(void* dragon_gpu_handle, int deviceID);

dragonError_t
dragon_gpu_get_uuid(void* dragon_gpu_handle, int devID, char** uuid);

#ifdef __cplusplus
} // extern "C"
#endif

#endif // HAVE_DRAGON_GPU_C_API_H
