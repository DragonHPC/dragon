#ifndef HAVE_DRAGON_GPU_HIP_HPP
#define HAVE_DRAGON_GPU_HIP_HPP

#define __HIP_PLATFORM_AMD__

#include "gpu.hpp"
#include "hip/hip_runtime.h"
#include <cstdlib>
#include <iostream>
#include <cstdio>
#include <string>

class dragonGPU_hip final : public dragonGPU {
public:

    static constexpr const char *libname{"libamdhip64.so"};

    dragonGPU_hip();

    dragonError_t
    set_device(int device_id) override;

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

    dragonError_t
    host_register(void *addr, size_t size) override;

    dragonError_t
    host_unregister(void *addr) override;

    std::string
    get_errstr(const char *event, int hip_rc) override;

    int
    get_device_count() override;

    dragonError_t
    get_device_list(int **int_device_list, double **double_device_list, int *num_devices) override;

    dragonError_t
    uuid_hex_to_str(char *uuid_hex, char **uuid_str) override;

    dragonError_t
    get_uuid(int device_id, char **uuid_str) override;

};

#endif // HAVE_DRAGON_GPU_HIP_HPP

