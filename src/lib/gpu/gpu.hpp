#ifndef HAVE_DRAGON_GPU_HPP
#define HAVE_DRAGON_GPU_HPP

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif // _GNU_SOURCE

#include "../err.h"
#include "../shared_lock.hpp"
#include "dragon/return_codes.h"
#include "dragon/shared_lock.h"
#include "dragon/utils.h"

#include <assert.h>
#include <dlfcn.h>
#include <memory>
#include <stdexcept>
#include <stdio.h>
#include <string>
#include <string.h>
#include <unistd.h>
#include <unordered_map>
#include <vector>

extern bool dragon_gpu_debug;
extern FILE *dragon_gpu_log;

enum dragonGPUBackend_t {
    DRAGON_GPU_BACKEND_CUDA = 0,
    DRAGON_GPU_BACKEND_HIP,
    DRAGON_GPU_BACKEND_ZE
};

enum dragonGPUMemcpyType_t {
    DRAGON_GPU_D2D = 0,
    DRAGON_GPU_D2H,
    DRAGON_GPU_H2D
};

class dragonGPU {
protected:

    dragonGPUBackend_t backend_type;
    int device_idx;

public:

    virtual dragonError_t mem_alloc(void **addr, size_t size) = 0;
    virtual dragonError_t mem_free(void *addr) = 0;
    virtual dragonError_t get_ipc_handle(void *addr, std::vector<uint8_t>& ipc_handle) = 0;
    virtual dragonError_t free_ipc_handle(std::vector<uint8_t>& ipc_handle) = 0;
    virtual dragonError_t attach(std::vector<uint8_t>& ipc_handle, void **addr) = 0;
    virtual dragonError_t detach(void *addr) = 0;
    virtual dragonError_t copy(void *dst_addr, const void *src_addr, size_t size, dragonGPUMemcpyType_t memcpy_type) = 0;
    virtual dragonError_t memset(void *addr, int val, size_t num_bytes) = 0;
    virtual std::string get_errstr(const char *event, int rc) = 0;
};

struct dragonGPUHandle_t {
    std::shared_ptr<dragonGPU> dgpu;
    dragonLock lock;
};

struct dragonIPCHandle_t {
    std::vector<uint8_t> data;
};

// extern "C" decls for setup functions

#ifdef __cplusplus
extern "C" {
#endif

dragonError_t
dragon_gpu_setup(dragonGPUBackend_t gpu_backend_type, dragonGPUHandle_t *handle);

dragonError_t
dragon_gpu_cleanup(dragonGPUHandle_t *handle);

dragonError_t
dragon_gpu_setup_cuda(void *libhandle, dragonGPUHandle_t *handle);

dragonError_t
dragon_gpu_setup_hip(void *libhandle, dragonGPUHandle_t *handle);

dragonError_t
dragon_gpu_setup_ze(void *libhandle, dragonGPUHandle_t *handle);

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
dragon_gpu_mem_alloc(dragonGPUHandle_t *handle, void **addr, size_t size);

dragonError_t
dragon_gpu_mem_free(dragonGPUHandle_t *handle, void *addr);

dragonError_t
dragon_gpu_get_ipc_handle(dragonGPUHandle_t *handle, void *addr, dragonIPCHandle_t *ipc_handle);

dragonError_t
dragon_gpu_free_ipc_handle(dragonGPUHandle_t *handle, dragonIPCHandle_t *ipc_handle);

dragonError_t
dragon_gpu_attach(dragonGPUHandle_t *handle, dragonIPCHandle_t *ipc_handle, void **addr);

dragonError_t
dragon_gpu_detach(dragonGPUHandle_t *handle, void *addr);

dragonError_t
dragon_gpu_copy(dragonGPUHandle_t *handle, void *dst_addr, const void *src_addr, size_t size, dragonGPUMemcpyType_t memcpy_type);

dragonError_t
dragon_gpu_memset(dragonGPUHandle_t *handle, void *addr, int val, size_t num_bytes);

void
dragon_gpu_get_errstr(dragonGPUHandle_t *handle, const char *event, int rc, char *errstr, int strlen);

#ifdef __cplusplus
} // extern "C"
#endif

#endif // HAVE_DRAGON_GPU_HPP

