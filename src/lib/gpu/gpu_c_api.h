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
extern FILE *dragon_gpu_log;

typedef enum dragonGPUBackend_t {
    DRAGON_GPU_BACKEND_CUDA = 0,
    DRAGON_GPU_BACKEND_HIP,
    DRAGON_GPU_BACKEND_ZE
}dragonGPUBackend_t;

typedef enum dragonGPUMemcpyType_t {
    DRAGON_GPU_D2D = 0,
    DRAGON_GPU_D2H,
    DRAGON_GPU_H2D
}dragonGPUMemcpyType_t;

dragonError_t
dragon_gpu_setup(dragonGPUBackend_t gpu_backend_type, int deviceID, void **dragon_gpu_handle);

dragonError_t
dragon_gpu_cleanup(void **dragon_gpu_handle);

dragonError_t
dragon_gpu_setup_cuda(void *libhandle, int deviceID, void *dragon_gpu_handle);

dragonError_t
dragon_gpu_setup_hip(void *libhandle, void *dragon_gpu_handle);

dragonError_t
dragon_gpu_setup_ze(void *libhandle, void *dragon_gpu_handle);

void *
dragon_gpu_open_cuda_lib();

dragonError_t
dragon_gpu_resolve_cuda_symbols(void *libhandle);

void *
dragon_gpu_open_hip_lib();

dragonError_t
dragon_gpu_resolve_hip_symbols(void *libhandle);

void *
dragon_gpu_open_ze_lib();

dragonError_t
dragon_gpu_resolve_ze_symbols(void *libhandle);

dragonError_t
dragon_gpu_mem_alloc(void *dragon_gpu_handle, void **addr, size_t size);

dragonError_t
dragon_gpu_mem_free(void *dragon_gpu_handle, void *addr);

dragonError_t
dragon_gpu_get_ipc_handle(void *dragon_gpu_handle, void *addr, void **dragon_ipc_handle);

size_t
dragon_gpu_get_ipc_handle_size(void *dragon_gpu_handle);

dragonError_t
dragon_gpu_free_ipc_handle(void *dragon_gpu_handle, void **dragon_ipc_handle);

dragonError_t
dragon_gpu_attach(void *dragon_gpu_handle, void *dragon_ipc_handle, void **addr);

dragonError_t
dragon_gpu_detach(void *dragon_gpu_handle, void *addr);

dragonError_t
dragon_gpu_copy(void *dragon_gpu_handle, void *dst_addr, const void *src_addr, size_t size, dragonGPUMemcpyType_t memcpy_type);

dragonError_t
dragon_gpu_memset(void *dragon_gpu_handle, void *addr, int val, size_t num_bytes);

void
dragon_gpu_get_errstr(void *dragon_gpu_handle, const char *event, int rc, char *errstr, int strlen);

#ifdef __cplusplus
} // extern "C"
#endif

#endif // HAVE_DRAGON_GPU_C_API_H
