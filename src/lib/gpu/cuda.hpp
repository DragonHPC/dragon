#ifndef HAVE_DRAGON_GPU_CUDA_HPP
#define HAVE_DRAGON_GPU_CUDA_HPP

#include "gpu.hpp"
#include "cuda_runtime.h"
#include <cstdlib>
#include <iostream>
#include <cstdio>
#include <string>

class dragonGPU_cuda final : public dragonGPU {
public:

    static constexpr const char *libname{"libcudart.so"};

    dragonGPU_cuda(int deviceID);

    dragonError_t
    mem_alloc(void **addr, size_t size) override;

    dragonError_t
    mem_free(void *addr) override;

    dragonError_t
    get_ipc_handle(void *addr, void **ipc_handle) override;

    size_t
    get_ipc_handle_size() override;

    dragonError_t
    free_ipc_handle(void **ipc_handle) override;

    dragonError_t
    attach(void *ipc_handle, void **addr) override;

    dragonError_t
    detach(void *addr) override;

    dragonError_t
    copy(void *dst_addr, const void *src_addr, size_t size, dragonGPUMemcpyType_t memcpy_type) override;

    dragonError_t
    memset(void *addr, int val, size_t num_bytes) override;

    std::string
    get_errstr(const char *event, int cuda_rc) override;
};

#endif // HAVE_DRAGON_GPU_CUDA_HPP

