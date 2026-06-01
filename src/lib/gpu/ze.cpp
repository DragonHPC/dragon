#ifdef HAVE_ZE_INCLUDE

#include "ze.hpp"

// init ze backend

ze_result_t (*fn_zeInit)(ze_init_flag_t flags);
ze_result_t (*fn_zeDriverGet)(uint32_t *count, ze_driver_handle_t *drivers);
ze_result_t (*fn_zeDeviceGet)(ze_driver_handle_t driver, uint32_t *count, ze_device_handle_t *devices);
ze_result_t (*fn_zeDeviceGetSubDevices)(ze_device_handle_t device, uint32_t *count, ze_device_handle_t *subdevices);
ze_result_t (*fn_zeContextCreate)(ze_driver_handle_t driver, ze_context_desc_t *desc, ze_context_handle_t *context);
ze_result_t (*fn_zeContextDestroy)(ze_context_handle_t context);
ze_result_t (*fn_zeContextSystemBarrier)(ze_context_handle_t context, ze_device_handle_t device);
ze_result_t (*fn_zeMemAllocDevice)(ze_context_handle_t context, const ze_device_mem_alloc_desc_t *device_desc, size_t size, size_t alignment, ze_device_handle_t device, void **addr);
ze_result_t (*fn_zeMemFree)(ze_context_handle_t context, void *addr);
ze_result_t (*fn_zeMemGetIpcHandle)(ze_context_handle_t context, void *addr, ze_ipc_mem_handle_t *ipc_handle);
ze_result_t (*fn_zeMemPutIpcHandle)(ze_context_handle_t context, ze_ipc_mem_handle_t ipc_handle);
ze_result_t (*fn_zeMemOpenIpcHandle)(ze_context_handle_t context, ze_device_handle_t device, ze_ipc_mem_handle_t ipc_handle, ze_ipc_memory_flags_t flags, void **addr);
ze_result_t (*fn_zeMemCloseIpcHandle)(ze_context_handle_t ipc_handle, void *addr);
ze_result_t (*fn_zeDeviceGetCommandQueueGroupProperties)(ze_device_handle_t subdevice, uint32_t *group_count, ze_command_queue_group_properties_t *cmdq_group_properties);
ze_result_t (*fn_zeDeviceGetMemoryProperties)(ze_device_handle_t subdevice, uint32_t *local_mem_count, ze_device_memory_properties_t *mem_properties);
ze_result_t (*fn_zeCommandListCreate)(ze_context_handle_t context, ze_device_handle_t subdevice, ze_command_list_desc_t *cmdl_desc, ze_command_list_handle_t *command_list);
ze_result_t (*fn_zeCommandListCreateImmediate)(ze_context_handle_t context, ze_device_handle_t subdevice, ze_command_queue_desc_t *cmdq_desc, ze_command_list_handle_t *command_list);
ze_result_t (*fn_zeCommandListDestroy)(ze_command_list_handle_t command_list);
ze_result_t (*fn_zeCommandQueueCreate)(ze_context_handle_t context, ze_device_handle_t subdevice, ze_command_queue_desc_t *cmdq_desc, ze_command_queue_handle_t *command_queue);
ze_result_t (*fn_zeCommandQueueDestroy)(ze_command_queue_handle_t command_queue);
ze_result_t (*fn_zeCommandQueueExecuteCommandLists)(ze_command_queue_handle_t command_queue, uint32_t num_lists, ze_command_list_handle_t *command_lists, ze_fence_handle_t fence);
ze_result_t (*fn_zeCommandQueueSynchronize)(ze_command_queue_handle_t command_queue, uint64_t timeout);
ze_result_t (*fn_zeCommandListAppendMemoryCopy)(ze_command_list_handle_t command_list, void *dst_addr, const void *src_addr, size_t size, ze_event_handle_t signal, uint32_t num_events, ze_event_handle_t *wait_events);
ze_result_t (*fn_zeCommandListAppendMemoryFill)(ze_command_list_handle_t command_list, void *addr, const void *pattern, size_t pattern_size, size_t size, ze_event_handle_t signal_event, uint32_t num_wait_events, ze_event_handle_t *wait_events);
ze_result_t (*fn_zeCommandListAppendBarrier)(ze_command_list_handle_t command_list, ze_event_handle_t signal_event, uint32_t num_wait_events, ze_event_handle_t *wait_events);
ze_result_t (*fn_zeCommandListClose)(ze_command_list_handle_t command_list);
ze_result_t (*fn_zeCommandListReset)(ze_command_list_handle_t command_list);
ze_result_t (*fn_zeFenceCreate)(ze_command_queue_handle_t command_queue, ze_fence_desc_t *fence_desc, ze_fence_handle_t *fence);
ze_result_t (*fn_zeFenceDestroy)(ze_fence_handle_t fence);
ze_result_t (*fn_zeFenceHostSynchronize)(ze_fence_handle_t fence, uint64_t timeout);
ze_result_t (*fn_zeFenceReset)(ze_fence_handle_t fence);
ze_result_t (*fn_zeEventPoolCreate)(ze_context_handle_t context, ze_event_pool_desc_t *event_pool_desc, uint32_t num_devices, ze_device_handle_t *devices, ze_event_pool_handle_t *event_pool);
ze_result_t (*fn_zeEventPoolDestroy)(ze_event_pool_handle_t event_pool);
ze_result_t (*fn_zeEventCreate)(ze_event_pool_handle_t event_pool, ze_event_desc_t *event_desc, ze_event_handle_t *event);
ze_result_t (*fn_zeEventDestroy)(ze_event_handle_t event);
ze_result_t (*fn_zeEventHostSynchronize)(ze_event_handle_t event, uint64_t timeout);
ze_result_t (*fn_zeEventHostReset)(ze_event_handle_t event);
ze_result_t (*fn_zeDriverGetLastErrorDescription)(ze_driver_handle_t driver, const char **last_err);

void *
dragon_gpu_open_ze_lib()
{
    return dlopen(dragonGPU_ze::libname, RTLD_LAZY | RTLD_GLOBAL);
}

dragonError_t
dragon_gpu_resolve_ze_symbols(void *libhandle)
{
    fn_zeInit = (ze_result_t (*)(ze_init_flag_t)) dlsym(libhandle, "zeInit");
    assert(fn_zeInit != nullptr);

    fn_zeDriverGet = (ze_result_t (*)(uint32_t *, ze_driver_handle_t *)) dlsym(libhandle, "zeDriverGet");
    assert(fn_zeDriverGet != nullptr);

    fn_zeDeviceGet = (ze_result_t (*)(ze_driver_handle_t, uint32_t *, ze_device_handle_t *)) dlsym(libhandle, "zeDeviceGet");
    assert(fn_zeDeviceGet != nullptr);

    fn_zeDeviceGetSubDevices = (ze_result_t (*)(ze_device_handle_t, uint32_t *, ze_device_handle_t *)) dlsym(libhandle, "zeDeviceGetSubDevices");
    assert(fn_zeDeviceGetSubDevices != nullptr);
    
    fn_zeContextCreate = (ze_result_t (*)(ze_driver_handle_t, ze_context_desc_t *, ze_context_handle_t *)) dlsym(libhandle, "zeContextCreate");
    assert(fn_zeContextCreate != nullptr);

    fn_zeContextDestroy = (ze_result_t (*)(ze_context_handle_t)) dlsym(libhandle, "zeContextDestroy");
    assert(fn_zeContextCreate != nullptr);

    // TODO: This function doesn't seem to exist in the libze_intel_gpu
    //       library on pinoak, even though it appears in online docs.
    //fn_zeContextSystemBarrier = (ze_result_t (*)(ze_context_handle_t, ze_device_handle_t)) dlsym(libhandle, "zeContextSystemBarrier");
    //assert(fn_zeContextSystemBarrier != nullptr);

    fn_zeMemAllocDevice = (ze_result_t (*)(ze_context_handle_t, const ze_device_mem_alloc_desc_t *, size_t, size_t, ze_device_handle_t, void **)) dlsym(libhandle, "zeMemAllocDevice");
    assert(fn_zeMemAllocDevice != nullptr);

    fn_zeMemFree = (ze_result_t (*)(ze_context_handle_t, void *)) dlsym(libhandle, "zeMemFree");
    assert(fn_zeMemFree != nullptr);

    fn_zeMemGetIpcHandle = (ze_result_t (*)(ze_context_handle_t, void *, ze_ipc_mem_handle_t *)) dlsym(libhandle, "zeMemGetIpcHandle");
    assert(fn_zeMemGetIpcHandle != nullptr);

    fn_zeMemPutIpcHandle = (ze_result_t (*)(ze_context_handle_t, ze_ipc_mem_handle_t)) dlsym(libhandle, "zeMemPutIpcHandle");
    // zeMemPutIpcHandle is new with 1.10, so it might be null
    //assert(fn_zeMemPutIpcHandle != nullptr);

    fn_zeMemOpenIpcHandle = (ze_result_t (*)(ze_context_handle_t, ze_device_handle_t, ze_ipc_mem_handle_t, ze_ipc_memory_flags_t, void **)) dlsym(libhandle, "zeMemOpenIpcHandle");
    assert(fn_zeMemOpenIpcHandle != nullptr);

    fn_zeMemCloseIpcHandle = (ze_result_t (*)(ze_context_handle_t, void *)) dlsym(libhandle, "zeMemCloseIpcHandle");
    assert(fn_zeMemCloseIpcHandle != nullptr);

    fn_zeDeviceGetCommandQueueGroupProperties = (ze_result_t (*)(ze_device_handle_t, uint32_t *, ze_command_queue_group_properties_t *)) dlsym(libhandle, "zeDeviceGetCommandQueueGroupProperties");
    assert(fn_zeDeviceGetCommandQueueGroupProperties != nullptr);

    fn_zeDeviceGetMemoryProperties = (ze_result_t (*)(ze_device_handle_t, uint32_t *, ze_device_memory_properties_t *)) dlsym(libhandle, "zeDeviceGetMemoryProperties");
    assert(fn_zeDeviceGetMemoryProperties != nullptr);

    fn_zeCommandListCreate = (ze_result_t (*)(ze_context_handle_t, ze_device_handle_t, ze_command_list_desc_t *, ze_command_list_handle_t *)) dlsym(libhandle, "zeCommandListCreate");
    assert(fn_zeCommandListCreate != nullptr);

    fn_zeCommandListCreateImmediate = (ze_result_t (*)(ze_context_handle_t, ze_device_handle_t, ze_command_queue_desc_t *, ze_command_list_handle_t *)) dlsym(libhandle, "zeCommandListCreateImmediate");
    assert(fn_zeCommandListCreateImmediate != nullptr);

    fn_zeCommandListDestroy = (ze_result_t (*)(ze_command_list_handle_t)) dlsym(libhandle, "zeCommandListDestroy");
    assert(fn_zeCommandListDestroy != nullptr);

    fn_zeCommandListAppendMemoryCopy = (ze_result_t (*)(ze_command_list_handle_t, void *, const void *, size_t, ze_event_handle_t, uint32_t, ze_event_handle_t *)) dlsym(libhandle, "zeCommandListAppendMemoryCopy");
    assert(fn_zeCommandListAppendMemoryCopy != nullptr);

    fn_zeCommandListAppendMemoryFill = (ze_result_t (*)(ze_command_list_handle_t, void *, const void *, size_t, size_t, ze_event_handle_t, uint32_t, ze_event_handle_t *)) dlsym(libhandle, "zeCommandListAppendMemoryFill");
    assert(fn_zeCommandListAppendMemoryFill != nullptr);

    fn_zeCommandListAppendBarrier = (ze_result_t (*)(ze_command_list_handle_t, ze_event_handle_t, uint32_t, ze_event_handle_t *)) dlsym(libhandle, "zeCommandListAppendBarrier");
    assert(fn_zeCommandListAppendBarrier != nullptr);

    fn_zeCommandListClose = (ze_result_t (*)(ze_command_list_handle_t)) dlsym(libhandle, "zeCommandListClose");
    assert(fn_zeCommandListClose != nullptr);

    fn_zeCommandListReset = (ze_result_t (*)(ze_command_list_handle_t)) dlsym(libhandle, "zeCommandListReset");
    assert(fn_zeCommandListReset != nullptr);

    fn_zeCommandQueueCreate = (ze_result_t (*)(ze_context_handle_t, ze_device_handle_t, ze_command_queue_desc_t *, ze_command_queue_handle_t *)) dlsym(libhandle, "zeCommandQueueCreate");
    assert(fn_zeCommandQueueCreate != nullptr);

    fn_zeCommandQueueDestroy = (ze_result_t (*)(ze_command_queue_handle_t)) dlsym(libhandle, "zeCommandQueueDestroy");
    assert(fn_zeCommandQueueDestroy != nullptr);

    fn_zeCommandQueueExecuteCommandLists = (ze_result_t (*)(ze_command_queue_handle_t, uint32_t, ze_command_list_handle_t *, ze_fence_handle_t)) dlsym(libhandle, "zeCommandQueueExecuteCommandLists");
    assert(fn_zeCommandQueueExecuteCommandLists != nullptr);

    fn_zeCommandQueueSynchronize = (ze_result_t (*)(ze_command_queue_handle_t, uint64_t)) dlsym(libhandle, "zeCommandQueueSynchronize");
    assert(fn_zeCommandQueueSynchronize != nullptr);

    fn_zeFenceCreate = (ze_result_t (*)(ze_command_queue_handle_t, ze_fence_desc_t *, ze_fence_handle_t *)) dlsym(libhandle, "zeFenceCreate");
    assert(fn_zeFenceCreate != nullptr);

    fn_zeFenceDestroy = (ze_result_t (*)(ze_fence_handle_t)) dlsym(libhandle, "zeFenceDestroy");
    assert(fn_zeFenceDestroy != nullptr);

    fn_zeFenceHostSynchronize = (ze_result_t (*)(ze_fence_handle_t, uint64_t)) dlsym(libhandle, "zeFenceHostSynchronize");
    assert(fn_zeFenceHostSynchronize != nullptr);

    fn_zeFenceReset = (ze_result_t (*)(ze_fence_handle_t)) dlsym(libhandle, "zeFenceReset");
    assert(fn_zeFenceReset != nullptr);

    fn_zeEventPoolCreate = (ze_result_t (*)(ze_context_handle_t, ze_event_pool_desc_t *, uint32_t, ze_device_handle_t *, ze_event_pool_handle_t *)) dlsym(libhandle, "zeEventPoolCreate");
    assert(fn_zeEventPoolCreate != nullptr);

    fn_zeEventPoolDestroy = (ze_result_t (*)(ze_event_pool_handle_t)) dlsym(libhandle, "zeEventPoolDestroy");
    assert(fn_zeEventPoolDestroy != nullptr);

    fn_zeEventCreate = (ze_result_t (*)(ze_event_pool_handle_t, ze_event_desc_t *, ze_event_handle_t *)) dlsym(libhandle, "zeEventCreate");
    assert(fn_zeEventCreate != nullptr);
    
    fn_zeEventDestroy = (ze_result_t (*)(ze_event_handle_t)) dlsym(libhandle, "zeEventDestroy");
    assert(fn_zeEventDestroy != nullptr);
    
    fn_zeEventHostSynchronize = (ze_result_t (*)(ze_event_handle_t, uint64_t)) dlsym(libhandle, "zeEventHostSynchronize");
    assert(fn_zeEventHostSynchronize != nullptr);
    
    fn_zeEventHostReset = (ze_result_t (*)(ze_event_handle_t)) dlsym(libhandle, "zeEventHostReset");
    assert(fn_zeEventHostReset != nullptr);

    // TODO: This function doesn't seem to exist in the libze_intel_gpu
    //       library on pinoak, even though it appears in online docs.
    //fn_zeDriverGetLastErrorDescription = (ze_result_t (*)(ze_driver_handle_t, const char **)) dlsym(libhandle, "zeDriverGetLastErrorDescription");
    //assert(fn_zeDriverGetLastErrorDescription != nullptr);

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_gpu_setup_ze(void *libhandle, dragonGPUHandle_t *gpuh)
{
    dragon_gpu_resolve_ze_symbols(libhandle);

    try {
        gpuh->dgpu = std::make_shared<dragonGPU_ze>();
    } catch (std::exception& e) {
        append_err_return(DRAGON_FAILURE, e.what());
    }

    no_err_return(DRAGON_SUCCESS);
}

// member function definitions

Tile::Tile(dragonGPU_ze *gpu, ze_device_handle_t subdevice)
{
    this->gpu           = gpu;
    this->subdevice     = subdevice;
    this->local_mem_idx = 0u;

    // need to find the group ordinal for a command queue with copy
    // functionality before we can create the command list/queue.
    // also need to init stuff for selecting local memory ordinals
    auto dragon_rc = this->init_ordinals();
    if (dragon_rc != DRAGON_SUCCESS) {
        throw std::runtime_error("failed to initialize ordinals");
    }

    dragon_rc = this->create_command_objects();
    if (dragon_rc != DRAGON_SUCCESS) {
        throw std::runtime_error("failed to create command objects");
    }
}

dragonError_t
Tile::init_ordinals()
{
    // get all the command queue groups

    auto cmdq_group_count = 0u;

    auto ze_rc = fn_zeDeviceGetCommandQueueGroupProperties(this->subdevice, &cmdq_group_count, nullptr);
    if (ze_rc != ZE_RESULT_SUCCESS) {
        append_err_return(DRAGON_FAILURE, this->gpu->get_errstr("failed to get command queue group properties", ze_rc).c_str());
    }

    std::vector<ze_command_queue_group_properties_t> cmdq_group_properties;
    cmdq_group_properties.resize(cmdq_group_count);

    for (auto i = 0u; i < cmdq_group_count; ++i) {
        cmdq_group_properties[i].stype = ZE_STRUCTURE_TYPE_COMMAND_QUEUE_GROUP_PROPERTIES;
        cmdq_group_properties[i].pNext = nullptr;
    }

    ze_rc = fn_zeDeviceGetCommandQueueGroupProperties(this->subdevice, &cmdq_group_count, &cmdq_group_properties[0]);
    if (ze_rc != ZE_RESULT_SUCCESS) {
        append_err_return(DRAGON_FAILURE, this->gpu->get_errstr("failed to get command queue group properties", ze_rc).c_str());
    }

    // find a command queue group that supports copying
    this->copyq_group_ord = cmdq_group_count;
    for (auto i = 0u; i < cmdq_group_count; ++i) {
        if (cmdq_group_properties[i].flags & ZE_COMMAND_QUEUE_GROUP_PROPERTY_FLAG_COPY) {
            this->copyq_group_ord = i;
            break;
        }
    }

    if (this->copyq_group_ord == cmdq_group_count) {
        append_err_return(DRAGON_FAILURE, this->gpu->get_errstr("failed to find a command queue group that supports copying", ze_rc).c_str());
    }

    // the local memory ordinal needs to be less than the count
    // returned from zeDeviceGetMemoryProperties. we will use
    // round-robin to select such an ordinal

    this->local_mem_count = 0u;

    ze_rc = fn_zeDeviceGetMemoryProperties(this->subdevice, &this->local_mem_count, nullptr);
    if (ze_rc != ZE_RESULT_SUCCESS) {
        append_err_return(DRAGON_FAILURE, this->gpu->get_errstr("failed to get memory properties", ze_rc).c_str());
    }

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
Tile::create_command_objects()
{
    // NOTE: some of these objects will be superfluous, but we're
    // hanging on to them just in case they come in handy later

    auto ze_rc = ZE_RESULT_SUCCESS;

    // create command queue

    ze_command_queue_desc_t cmdq_desc{
        ZE_STRUCTURE_TYPE_COMMAND_QUEUE_DESC,
        nullptr,
        this->copyq_group_ord,
        0, // index
        0, // flags
        ZE_COMMAND_QUEUE_MODE_DEFAULT,
        ZE_COMMAND_QUEUE_PRIORITY_NORMAL
    };

    ze_rc = fn_zeCommandQueueCreate(
        this->gpu->context,
        this->subdevice,
        &cmdq_desc,
        &this->command_queue
    );
    if (ze_rc != ZE_RESULT_SUCCESS) {
        append_err_return(DRAGON_FAILURE, this->gpu->get_errstr("failed to create command queue", ze_rc).c_str());
    }

    // create command list

    ze_command_list_desc_t cmdl_desc{
        ZE_STRUCTURE_TYPE_COMMAND_LIST_DESC,
        nullptr,
        this->copyq_group_ord,
        0 // flags
    };

    ze_rc = fn_zeCommandListCreate(
        this->gpu->context,
        this->subdevice,
        &cmdl_desc,
        &this->command_list
    );
    if (ze_rc != ZE_RESULT_SUCCESS) {
        append_err_return(DRAGON_FAILURE, this->gpu->get_errstr("failed to create command list", ze_rc).c_str());
    }

    ze_rc = fn_zeCommandListCreateImmediate(
        this->gpu->context,
        this->subdevice,
        &cmdq_desc,
        &this->immediate_command_list
    );
    if (ze_rc != ZE_RESULT_SUCCESS) {
        append_err_return(DRAGON_FAILURE, this->gpu->get_errstr("failed to create immediate command list", ze_rc).c_str());
    }

    // create fence

    ze_fence_desc_t fence_desc = {
        ZE_STRUCTURE_TYPE_FENCE_DESC,
        nullptr,
        0 // flags
    };

    ze_rc = fn_zeFenceCreate(this->command_queue, &fence_desc, &this->fence);
    if (ze_rc != ZE_RESULT_SUCCESS) {
        append_err_return(DRAGON_FAILURE, this->gpu->get_errstr("failed to create fence", ze_rc).c_str());
    }

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
Tile::destroy_command_objects()
{
    auto ze_rc = ZE_RESULT_SUCCESS;

    // destroy command list

    ze_rc = fn_zeCommandListDestroy(this->command_list);
    if (ze_rc != ZE_RESULT_SUCCESS) {
        append_err_return(DRAGON_FAILURE, this->gpu->get_errstr("failed to destroy command list", ze_rc).c_str());
    }

    ze_rc = fn_zeCommandListDestroy(this->immediate_command_list);
    if (ze_rc != ZE_RESULT_SUCCESS) {
        append_err_return(DRAGON_FAILURE, this->gpu->get_errstr("failed to destroy command list", ze_rc).c_str());
    }

    // destroy command queue

    ze_rc = fn_zeCommandQueueSynchronize(this->command_queue, dragon_sec_to_nsec(30));
    if (ze_rc != ZE_RESULT_SUCCESS) {
        append_err_return(DRAGON_FAILURE, this->gpu->get_errstr("failed to synchronize command queue", ze_rc).c_str());
    }

    ze_rc = fn_zeCommandQueueDestroy(this->command_queue);
    if (ze_rc != ZE_RESULT_SUCCESS) {
        append_err_return(DRAGON_FAILURE, this->gpu->get_errstr("failed to destroy command queue", ze_rc).c_str());
    }

    // destroy fence

    ze_rc = fn_zeFenceDestroy(this->fence);
    if (ze_rc != ZE_RESULT_SUCCESS) {
        append_err_return(DRAGON_FAILURE, this->gpu->get_errstr("failed to destroy fence", ze_rc).c_str());
    }

    no_err_return(DRAGON_SUCCESS);
}

dragonGPU_ze::dragonGPU_ze()
{
    this->backend_type = DRAGON_GPU_BACKEND_ZE;

    auto init_flag = (ze_init_flag_t) 0;
    auto ze_rc = fn_zeInit(init_flag);
    if (ze_rc != ZE_RESULT_SUCCESS) {
        throw std::runtime_error("failed to initialize ZE library");
    }

    // a driver corresponds to a collection of physical devices in the
    // system accessed by the same Level-Zero driver, so we assume there's
    // only one driver for now
    auto driver_count = 1u;

    ze_rc = fn_zeDriverGet(&driver_count, &this->driver);
    if (ze_rc != ZE_RESULT_SUCCESS || driver_count == 0u) {
        throw std::runtime_error("failed to get driver");
    }

    // get devices

    uint32_t device_count = 0;

    ze_rc = fn_zeDeviceGet(this->driver, &device_count, nullptr);
    if (ze_rc != ZE_RESULT_SUCCESS || device_count == 0u) {
        throw std::runtime_error("failed to get device count");
    }

    this->device.resize(device_count);

    ze_rc = fn_zeDeviceGet(this->driver, &device_count, &this->device[0]);
    if (ze_rc != ZE_RESULT_SUCCESS) {
        throw std::runtime_error("failed to get devices");
    }

    // get subdevices

    this->subdevice_count.resize(device_count, 0u);
    auto tile_count = 0u;

    for (auto i = 0u; i < device_count; ++i) {
        auto& device = this->device[i];

        ze_rc = fn_zeDeviceGetSubDevices(device, &this->subdevice_count[i], nullptr);
        if (ze_rc != ZE_RESULT_SUCCESS) {
            throw std::runtime_error("failed to get get subdevice count");
        }

        tile_count += this->subdevice_count[i];
    }

    this->subdevice.resize(tile_count);
    tile_count = 0u;

    for (auto i = 0u; i < device_count; ++i) {
        auto& device = this->device[i];

        ze_rc = fn_zeDeviceGetSubDevices(device, &this->subdevice_count[i], &this->subdevice[tile_count]);
        if (ze_rc != ZE_RESULT_SUCCESS) {
            throw std::runtime_error("failed to get get subdevices");
        }

        tile_count += this->subdevice_count[i];
    }

    // TODO: select nearest device
    this->device_idx = 0;

    // create a context before initializing the subdevices
    auto derr = this->create_context();
    if (derr != DRAGON_SUCCESS) {
        throw std::runtime_error("failed to create context");
    }

    // initialize the subdevices (depends on creating context first)
    for (auto i = 0u; i < tile_count; ++i) {
        auto tile = std::make_shared<Tile>(this, this->subdevice[i]);
        this->tile.push_back(tile);
    }

    // create event

    ze_event_pool_desc_t event_pool_desc = {
        .stype = ZE_STRUCTURE_TYPE_EVENT_POOL_DESC,
        .pNext = nullptr,
        .flags = ZE_EVENT_POOL_FLAG_HOST_VISIBLE,
        .count = 1u
    };

    ze_rc = fn_zeEventPoolCreate(this->context, &event_pool_desc, 1, &this->subdevice[this->device_idx], &this->event_pool);
    if (ze_rc != ZE_RESULT_SUCCESS) {
        throw std::runtime_error("failed to create event pool");
    }

    ze_event_desc_t event_desc = { 
        .stype  = ZE_STRUCTURE_TYPE_EVENT_DESC,
        .pNext  = nullptr,
        .index  = 0,
        .signal = ZE_EVENT_SCOPE_FLAG_HOST,
        .wait   = ZE_EVENT_SCOPE_FLAG_HOST
    };

    ze_rc = fn_zeEventCreate(this->event_pool, &event_desc, &this->event);
    if (ze_rc != ZE_RESULT_SUCCESS) {
        throw std::runtime_error("failed to create event");
    }

}

dragonGPU_ze::~dragonGPU_ze()
{
    auto ze_rc = ZE_RESULT_SUCCESS;

    // destroy event pool

    ze_rc = fn_zeEventDestroy(this->event);
    if (ze_rc != ZE_RESULT_SUCCESS) {
        // call get_errstr just to log the error (assuming debugging is enabled)
        this->get_errstr("failed to destroy event", ze_rc).c_str();
    }

    ze_rc = fn_zeEventPoolDestroy(this->event_pool);
    if (ze_rc != ZE_RESULT_SUCCESS) {
        // call get_errstr just to log the error (assuming debugging is enabled)
        this->get_errstr("failed to destroy event pool", ze_rc).c_str();
    }

    // destroy tiles

    for (auto& tile: this->tile) {
        tile->destroy_command_objects();
    }

    // destroy context

    ze_rc = fn_zeContextDestroy(this->context);
    if (ze_rc != ZE_RESULT_SUCCESS) {
        // call get_errstr just to log the error (assuming debugging is enabled)
        this->get_errstr("failed to destroy context", ze_rc).c_str();
    }
}

dragonError_t
dragonGPU_ze::create_context()
{
    ze_context_desc_t ctx_desc{
        ZE_STRUCTURE_TYPE_CONTEXT_DESC,
        nullptr,
        0
    };

    auto ze_rc = fn_zeContextCreate(this->driver, &ctx_desc, &this->context);
    if (ze_rc != ZE_RESULT_SUCCESS) {
        append_err_return(DRAGON_FAILURE, this->get_errstr("failed to create a GPU context", ze_rc).c_str());
    }

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragonGPU_ze::mem_alloc(void **addr, size_t size)
{
    auto my_tile = this->tile[this->device_idx];

    ze_device_mem_alloc_desc_t mem_alloc_desc = {
        ZE_STRUCTURE_TYPE_DEVICE_MEM_ALLOC_DESC,
        nullptr,
        0, // flags
        my_tile->get_local_mem_idx() // ordinal
    };

    // TODO: is this a reasonable choice for alignment?
    auto alignment = 64ul;

    auto ze_rc = fn_zeMemAllocDevice(
        this->context,
        &mem_alloc_desc,
        size,
        alignment,
        this->subdevice[this->device_idx],
        addr
    );
    if (ze_rc != ZE_RESULT_SUCCESS) {
        append_err_return(DRAGON_FAILURE, this->get_errstr("failed to allocate device memory", ze_rc).c_str());
    }

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragonGPU_ze::mem_free(void *addr)
{
    auto ze_rc = fn_zeMemFree(this->context, addr);
    if (ze_rc != ZE_RESULT_SUCCESS) {
        append_err_return(DRAGON_FAILURE, this->get_errstr("failed to free device memory", ze_rc).c_str());
    }

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragonGPU_ze::get_ipc_handle(void *addr, std::vector<uint8_t>& ipc_handle_out)
{
    ze_ipc_mem_handle_t ipc_handle;

    auto ze_rc = fn_zeMemGetIpcHandle(this->context, addr, &ipc_handle);
    if (ze_rc != ZE_RESULT_SUCCESS) {
        append_err_return(DRAGON_FAILURE, this->get_errstr("failed to get IPC handle", ze_rc).c_str());
    }

    ipc_handle_out.resize(sizeof(ze_ipc_mem_handle_t));
    memcpy(&ipc_handle_out[0], &ipc_handle, ipc_handle_out.size());

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragonGPU_ze::free_ipc_handle(std::vector<uint8_t>& ipc_handle_in)
{
    if (fn_zeMemPutIpcHandle != nullptr) {
        ze_ipc_mem_handle_t ipc_handle;
        memcpy(&ipc_handle, &ipc_handle_in[0], sizeof(ze_ipc_mem_handle_t));

        auto ze_rc = fn_zeMemPutIpcHandle(this->context, ipc_handle);
        if (ze_rc != ZE_RESULT_SUCCESS) {
            append_err_return(DRAGON_FAILURE, this->get_errstr("failed to put IPC handle", ze_rc).c_str());
        }
    }

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragonGPU_ze::attach(std::vector<uint8_t>& ipc_handle_in, void **addr)
{
    ze_ipc_mem_handle_t ipc_handle;
    memcpy(&ipc_handle, &ipc_handle_in[0], sizeof(ze_ipc_mem_handle_t));

    auto ze_rc = fn_zeMemOpenIpcHandle(this->context, this->subdevice[this->device_idx], ipc_handle, 0, addr);
    if (ze_rc != ZE_RESULT_SUCCESS) {
        append_err_return(DRAGON_FAILURE, this->get_errstr("failed to open IPC handle", ze_rc).c_str());
    }

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragonGPU_ze::detach(void *addr)
{
    auto ze_rc = fn_zeMemCloseIpcHandle(this->context, addr);
    if (ze_rc != ZE_RESULT_SUCCESS) {
        append_err_return(DRAGON_FAILURE, this->get_errstr("failed to close IPC handle", ze_rc).c_str());
    }

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragonGPU_ze::copy(void *dst_addr, const void *src_addr, size_t size, dragonGPUMemcpyType_t memcpy_type)
{
    auto my_tile         = this->tile[this->device_idx];
    auto my_imm_cmd_list = my_tile->get_immediate_command_list();

    // silence compiler warning
    // (we don't need memcpy_type in this derived class)
    (void)memcpy_type;

    // append memcpy

    auto ze_rc =
        fn_zeCommandListAppendMemoryCopy(
            my_imm_cmd_list,
            dst_addr, src_addr, size,
            this->event, 0, nullptr
        );
    if (ze_rc != ZE_RESULT_SUCCESS) {
        append_err_return(DRAGON_FAILURE, this->get_errstr("failed to append memory copy", ze_rc).c_str());
    }

    // synchronize with event

    ze_rc = fn_zeEventHostSynchronize(this->event, dragon_sec_to_nsec(30));
    if (ze_rc != ZE_RESULT_SUCCESS) {
        append_err_return(DRAGON_FAILURE, this->get_errstr("failed to synchronize with event", ze_rc).c_str());
    }

    ze_rc = fn_zeEventHostReset(this->event);
    if (ze_rc != ZE_RESULT_SUCCESS) {
        append_err_return(DRAGON_FAILURE, this->get_errstr("failed to reset event", ze_rc).c_str());
    }

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragonGPU_ze::memset(void *addr, int val, size_t num_bytes)
{
    auto my_tile         = this->tile[this->device_idx];
    auto my_imm_cmd_list = my_tile->get_immediate_command_list();
    auto val_size        = 1ul; // always use 1 byte for the pattern size

    // append memory fill

    auto ze_rc =
        fn_zeCommandListAppendMemoryFill(
            my_imm_cmd_list,
            addr, (void *) &val, val_size, num_bytes,
            this->event, 0, nullptr
        );
    if (ze_rc != ZE_RESULT_SUCCESS) {
        append_err_return(DRAGON_FAILURE, this->get_errstr("failed to append memory fill", ze_rc).c_str());
    }

    // synchronize with event

    ze_rc = fn_zeEventHostSynchronize(this->event, dragon_sec_to_nsec(30));
    if (ze_rc != ZE_RESULT_SUCCESS) {
        append_err_return(DRAGON_FAILURE, this->get_errstr("failed to synchronize with event", ze_rc).c_str());
    }

    ze_rc = fn_zeEventHostReset(this->event);
    if (ze_rc != ZE_RESULT_SUCCESS) {
        append_err_return(DRAGON_FAILURE, this->get_errstr("failed to reset event", ze_rc).c_str());
    }

    no_err_return(DRAGON_SUCCESS);
}

std::string
dragonGPU_ze::get_errstr(const char *event, int ze_rc)
{
    const char *last_err = nullptr;
    // TODO: this function doesn't appear to be in the libze_intel_gpu.so
    //       library on pinoak
    //fn_zeDriverGetLastErrorDescription(this->driver, &last_err);

    auto log_str = 
          std::string(event)
        + std::string(": rc=") + std::to_string(ze_rc)
        + (last_err ? std::string(", ") + std::string(last_err) : std::string(""));

    if (dragon_gpu_debug) {
        fprintf(dragon_gpu_log, "%s\n", log_str.c_str());
        fflush(dragon_gpu_log);
    }

    return log_str;
}

#endif // HAVE_ZE_INCLUDE

