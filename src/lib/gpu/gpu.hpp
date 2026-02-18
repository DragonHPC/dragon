#ifndef HAVE_DRAGON_GPU_HPP
#define HAVE_DRAGON_GPU_HPP

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif // _GNU_SOURCE

#include "gpu_c_api.h"
#include "../shared_lock.hpp"
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

class dragonGPU {
protected:

    dragonGPUBackend_t backend_type;
    //TODO: Fix. Use device idx to specify which device handle to get. likely dragon_gpu_setup
    int device_idx;

public:

    virtual dragonError_t mem_alloc(void **addr, size_t size) = 0;
    virtual dragonError_t mem_free(void *addr) = 0;
    virtual dragonError_t get_ipc_handle(void *addr, void** ipc_handle) = 0;
    virtual size_t get_ipc_handle_size() = 0;
    virtual dragonError_t free_ipc_handle(void** ipc_handle) = 0;
    virtual dragonError_t attach(void* ipc_handle, void **addr) = 0;
    virtual dragonError_t detach(void *addr) = 0;
    virtual dragonError_t copy(void *dst_addr, const void *src_addr, size_t size, dragonGPUMemcpyType_t memcpy_type) = 0;
    virtual dragonError_t memset(void *addr, int val, size_t num_bytes) = 0;
    virtual std::string get_errstr(const char *event, int rc) = 0;
    virtual dragonError_t host_register(void *addr, size_t size) = 0;
    virtual dragonError_t host_unregister(void *addr) = 0;
};

struct dragonGPUHandle_t {
    std::shared_ptr<dragonGPU> dgpu;
    dragonLock lock;
};

// extern "C" decls for setup functions


#endif // HAVE_DRAGON_GPU_HPP

