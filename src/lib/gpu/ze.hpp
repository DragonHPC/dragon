#ifndef HAVE_DRAGON_GPU_ZE_HPP
#define HAVE_DRAGON_GPU_ZE_HPP

#include "gpu.hpp"
#include "ze_api.h"

// forward declarations

class dragonGPU_ze;

// class definitions

class Tile {
private:

    ze_device_handle_t subdevice;
    ze_command_list_handle_t command_list;
    ze_command_list_handle_t immediate_command_list;
    ze_command_queue_handle_t command_queue;
    ze_fence_handle_t fence;
    uint32_t copyq_group_ord;
    uint32_t local_mem_idx;
    uint32_t local_mem_count;
    dragonGPU_ze *gpu;

    dragonError_t
    init_ordinals();

    dragonError_t
    create_command_objects();

public:

    Tile(dragonGPU_ze *gpu, ze_device_handle_t subdevice);

    dragonError_t
    destroy_command_objects();

    ze_command_list_handle_t
    get_command_list()
    {
        return this->command_list;
    }

    ze_command_list_handle_t
    get_immediate_command_list()
    {
        return this->immediate_command_list;
    }

    ze_command_queue_handle_t
    get_command_queue()
    {
        return this->command_queue;
    }

    ze_fence_handle_t
    get_fence()
    {
        return this->fence;
    }

    uint32_t
    get_local_mem_idx()
    {
        // round-robin over local memory ordinals
        auto save_idx = this->local_mem_idx;
        this->local_mem_idx = (this->local_mem_idx + 1) % this->local_mem_count;
        return save_idx;
    }
};

class dragonGPU_ze final : public dragonGPU {
private:

    ze_driver_handle_t driver;
    std::vector<ze_device_handle_t> device;
    std::vector<ze_device_handle_t> subdevice;
    std::vector<uint32_t> subdevice_count;
    std::vector<std::shared_ptr<Tile>> tile;
    ze_event_pool_handle_t event_pool;
    ze_event_handle_t event;

    dragonError_t
    create_context();

public:

    static constexpr const char *libname{"libze_intel_gpu.so.1"};
    ze_context_handle_t context;

    dragonGPU_ze();
    ~dragonGPU_ze();

    dragonError_t
    mem_alloc(void **addr, size_t size) override;

    dragonError_t
    mem_free(void *addr) override;

    dragonError_t
    get_ipc_handle(void *addr, std::vector<uint8_t>& ipc_handle) override;

    dragonError_t
    free_ipc_handle(std::vector<uint8_t>& ipc_handle) override;

    dragonError_t
    attach(std::vector<uint8_t>& ipc_handle, void **addr) override;

    dragonError_t
    detach(void *addr) override;

    dragonError_t
    copy(void *dst_addr, const void *src_addr, size_t size, dragonGPUMemcpyType_t memcpy_type) override;

    dragonError_t
    memset(void *addr, int val, size_t num_bytes) override;

    std::string
    get_errstr(const char *event, int ze_rc) override;
};

#endif // HAVE_DRAGON_GPU_ZE_HPP

