/*
 * Stub implementation of the Level Zero symbols that Dragon's GPU backend
 * resolves with dlsym. Built against the real ze_api.h so struct layouts match
 * what Dragon compiles against, but it never touches a device.
 *
 * Runtime behavior is controlled by environment variables:
 *   DRAGON_STUB_ZE_DEVICE_COUNT     number of fake root devices (0 => init failure)
 *   DRAGON_STUB_ZE_SUBDEVICE_COUNT  subdevices (tiles) per root device, 0 for flat
 *   ZE_AFFINITY_MASK / ZE_FLAT_DEVICE_HIERARCHY  when set, overrides the
 *     layout above to emulate a real driver that has already filtered
 *     devices down to the masked entries, renumbering them consecutively:
 *     - COMPOSITE (default) with a mask containing '.' groups entries by
 *       major (device) ordinal, exposing each as a genuine subdevice.
 *     - FLAT with a plain integer mask selects tiles from the flattened
 *       device x subdevice list, exposing each as its own top-level device.
 */

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include <ze_api.h>

namespace
{

// Fake opaque handles. Root devices and subdevices are encoded so that
// zeDeviceGetProperties can recover the (device, subdevice) pair.
constexpr uintptr_t STUB_DRIVER = 0xD0000001ul;
constexpr uintptr_t STUB_CONTEXT = 0xC0000001ul;
constexpr uintptr_t STUB_ROOT_BASE = 0x1000ul;
constexpr uintptr_t STUB_SUB_BASE = 0x100000ul;
constexpr uintptr_t STUB_SUB_STRIDE = 0x100ul;
constexpr uintptr_t STUB_OBJECT = 0xB0000001ul;

int
stub_device_count()
{
    const char* env = getenv("DRAGON_STUB_ZE_DEVICE_COUNT");
    return env == nullptr ? 0 : atoi(env);
}

int
stub_subdevice_count()
{
    const char* env = getenv("DRAGON_STUB_ZE_SUBDEVICE_COUNT");
    return env == nullptr ? 0 : atoi(env);
}

// Per-device lists of subdevice IDs. An empty inner list means that device
// is flat (no subdevices exposed, matching real hardware with no tiles).
struct DeviceLayout {
    std::vector<std::vector<int>> subdevice_ids;
};

const DeviceLayout&
stub_layout()
{
    static const DeviceLayout layout = [] {
        DeviceLayout result;

        const char* mask_env = getenv("ZE_AFFINITY_MASK");
        const char* hierarchy_env = getenv("ZE_FLAT_DEVICE_HIERARCHY");
        bool composite = hierarchy_env == nullptr || strcmp(hierarchy_env, "COMPOSITE") == 0;

        if (mask_env != nullptr && composite && strchr(mask_env, '.') != nullptr) {
            // Emulate a real driver that has filtered devices down to the
            // masked (device, subdevice) pairs, renumbering majors
            // consecutively in the order they're first seen.
            std::vector<int> seen_majors;
            std::string mask(mask_env);
            size_t pos = 0;
            while (pos <= mask.size()) {
                size_t comma = mask.find(',', pos);
                std::string token = mask.substr(pos, comma == std::string::npos ? std::string::npos : comma - pos);
                pos = comma == std::string::npos ? mask.size() + 1 : comma + 1;
                if (token.empty()) {
                    continue;
                }

                size_t dot = token.find('.');
                int major = atoi(token.substr(0, dot).c_str());
                int minor = dot == std::string::npos ? 0 : atoi(token.substr(dot + 1).c_str());

                auto it = std::find(seen_majors.begin(), seen_majors.end(), major);
                size_t device_idx;
                if (it == seen_majors.end()) {
                    device_idx = seen_majors.size();
                    seen_majors.push_back(major);
                    result.subdevice_ids.emplace_back();
                } else {
                    device_idx = static_cast<size_t>(std::distance(seen_majors.begin(), it));
                }

                result.subdevice_ids[device_idx].push_back(minor);
            }

            return result;
        }

        if (mask_env != nullptr && strchr(mask_env, '.') == nullptr) {
            // ZE_FLAT_DEVICE_HIERARCHY=FLAT: every physical tile is its own
            // top-level device; the mask selects flat tile indices and
            // renumbers them consecutively, each with no subdevices.
            std::string mask(mask_env);
            size_t pos = 0;
            while (pos <= mask.size()) {
                size_t comma = mask.find(',', pos);
                std::string token = mask.substr(pos, comma == std::string::npos ? std::string::npos : comma - pos);
                pos = comma == std::string::npos ? mask.size() + 1 : comma + 1;
                if (token.empty()) {
                    continue;
                }

                result.subdevice_ids.emplace_back();
            }

            return result;
        }

        const int device_count = stub_device_count();
        const int subdevice_count = stub_subdevice_count();
        for (int i = 0; i < device_count; ++i) {
            std::vector<int> subs;
            for (int j = 0; j < subdevice_count; ++j) {
                subs.push_back(j);
            }
            result.subdevice_ids.push_back(std::move(subs));
        }

        return result;
    }();

    return layout;
}

template<typename T>
T
stub_handle(uintptr_t value)
{
    return reinterpret_cast<T>(value);
}

uintptr_t
stub_value(void* handle)
{
    return reinterpret_cast<uintptr_t>(handle);
}

void
stub_fill_uuid(uint8_t* id, int device_id, int subdevice_id)
{
    for (int i = 0; i < ZE_MAX_DEVICE_UUID_SIZE; ++i) {
        id[i] = static_cast<uint8_t>((device_id * 0x10 + subdevice_id + i) & 0xFF);
    }
}

} // namespace

extern "C" {

ze_result_t
zeInit(ze_init_flags_t flags)
{
    (void)flags;
    return stub_layout().subdevice_ids.empty() ? ZE_RESULT_ERROR_UNINITIALIZED : ZE_RESULT_SUCCESS;
}

ze_result_t
zeDriverGet(uint32_t* count, ze_driver_handle_t* drivers)
{
    if (drivers == nullptr) {
        *count = 1u;
        return ZE_RESULT_SUCCESS;
    }

    if (*count == 0u) {
        return ZE_RESULT_ERROR_INVALID_SIZE;
    }

    drivers[0] = stub_handle<ze_driver_handle_t>(STUB_DRIVER);
    *count = 1u;
    return ZE_RESULT_SUCCESS;
}

ze_result_t
zeDeviceGet(ze_driver_handle_t driver, uint32_t* count, ze_device_handle_t* devices)
{
    (void)driver;

    const uint32_t total = static_cast<uint32_t>(stub_layout().subdevice_ids.size());
    if (devices == nullptr) {
        *count = total;
        return ZE_RESULT_SUCCESS;
    }

    const uint32_t n = *count < total ? *count : total;
    for (uint32_t i = 0u; i < n; ++i) {
        devices[i] = stub_handle<ze_device_handle_t>(STUB_ROOT_BASE + i);
    }
    *count = n;
    return ZE_RESULT_SUCCESS;
}

ze_result_t
zeDeviceGetSubDevices(ze_device_handle_t device, uint32_t* count, ze_device_handle_t* subdevices)
{
    const uintptr_t value = stub_value(device);
    if (value < STUB_ROOT_BASE || value >= STUB_SUB_BASE) {
        return ZE_RESULT_ERROR_INVALID_ARGUMENT;
    }

    const uint32_t device_id = static_cast<uint32_t>(value - STUB_ROOT_BASE);
    const auto& layout = stub_layout();
    if (device_id >= layout.subdevice_ids.size()) {
        return ZE_RESULT_ERROR_INVALID_ARGUMENT;
    }
    const uint32_t total = static_cast<uint32_t>(layout.subdevice_ids[device_id].size());

    if (subdevices == nullptr) {
        *count = total;
        return ZE_RESULT_SUCCESS;
    }

    const uint32_t n = *count < total ? *count : total;
    for (uint32_t i = 0u; i < n; ++i) {
        subdevices[i] = stub_handle<ze_device_handle_t>(STUB_SUB_BASE + device_id * STUB_SUB_STRIDE + i);
    }
    *count = n;
    return ZE_RESULT_SUCCESS;
}

ze_result_t
zeDeviceGetProperties(ze_device_handle_t device, ze_device_properties_t* device_properties)
{
    const uintptr_t value = stub_value(device);

    int device_id = 0;
    int subdevice_id = 0;
    bool is_subdevice = false;

    if (value >= STUB_SUB_BASE) {
        const uintptr_t offset = value - STUB_SUB_BASE;
        device_id = static_cast<int>(offset / STUB_SUB_STRIDE);
        const int local_index = static_cast<int>(offset % STUB_SUB_STRIDE);
        const auto& layout = stub_layout();
        if (device_id < 0 || static_cast<size_t>(device_id) >= layout.subdevice_ids.size() || local_index < 0 ||
            static_cast<size_t>(local_index) >= layout.subdevice_ids[device_id].size()) {
            return ZE_RESULT_ERROR_INVALID_ARGUMENT;
        }
        subdevice_id = layout.subdevice_ids[device_id][local_index];
        is_subdevice = true;
    } else if (value >= STUB_ROOT_BASE) {
        device_id = static_cast<int>(value - STUB_ROOT_BASE);
    } else {
        return ZE_RESULT_ERROR_INVALID_ARGUMENT;
    }

    const ze_structure_type_t stype = device_properties->stype;
    void* pnext = device_properties->pNext;
    memset(device_properties, 0, sizeof(*device_properties));
    device_properties->stype = stype;
    device_properties->pNext = pnext;

    device_properties->type = ZE_DEVICE_TYPE_GPU;
    device_properties->vendorId = 0x8086;
    device_properties->deviceId = static_cast<uint32_t>(device_id);
    device_properties->flags = is_subdevice ? ZE_DEVICE_PROPERTY_FLAG_SUBDEVICE : 0;
    device_properties->subdeviceId = static_cast<uint32_t>(subdevice_id);
    stub_fill_uuid(device_properties->uuid.id, device_id, subdevice_id);
    snprintf(device_properties->name, ZE_MAX_DEVICE_NAME, "Dragon Stub ZE Device %d.%d", device_id,
             subdevice_id);

    return ZE_RESULT_SUCCESS;
}

ze_result_t
zeDeviceGetCommandQueueGroupProperties(ze_device_handle_t device, uint32_t* group_count,
                                       ze_command_queue_group_properties_t* properties)
{
    (void)device;

    if (properties == nullptr) {
        *group_count = 1u;
        return ZE_RESULT_SUCCESS;
    }

    if (*group_count == 0u) {
        return ZE_RESULT_ERROR_INVALID_SIZE;
    }

    properties[0].stype = ZE_STRUCTURE_TYPE_COMMAND_QUEUE_GROUP_PROPERTIES;
    properties[0].pNext = nullptr;
    properties[0].flags =
      ZE_COMMAND_QUEUE_GROUP_PROPERTY_FLAG_COPY | ZE_COMMAND_QUEUE_GROUP_PROPERTY_FLAG_COMPUTE;
    properties[0].maxMemoryFillPatternSize = 16u;
    properties[0].numQueues = 1u;
    *group_count = 1u;

    return ZE_RESULT_SUCCESS;
}

ze_result_t
zeDeviceGetMemoryProperties(ze_device_handle_t device, uint32_t* local_mem_count,
                            ze_device_memory_properties_t* mem_properties)
{
    (void)device;

    if (mem_properties == nullptr) {
        *local_mem_count = 1u;
        return ZE_RESULT_SUCCESS;
    }

    if (*local_mem_count == 0u) {
        return ZE_RESULT_ERROR_INVALID_SIZE;
    }

    mem_properties[0].stype = ZE_STRUCTURE_TYPE_DEVICE_MEMORY_PROPERTIES;
    mem_properties[0].pNext = nullptr;
    mem_properties[0].totalSize = 1024ull * 1024ull * 1024ull;
    *local_mem_count = 1u;

    return ZE_RESULT_SUCCESS;
}

ze_result_t
zeContextCreate(ze_driver_handle_t driver, const ze_context_desc_t* desc, ze_context_handle_t* context)
{
    (void)driver;
    (void)desc;
    *context = stub_handle<ze_context_handle_t>(STUB_CONTEXT);
    return ZE_RESULT_SUCCESS;
}

ze_result_t
zeContextDestroy(ze_context_handle_t context)
{
    (void)context;
    return ZE_RESULT_SUCCESS;
}

ze_result_t
zeMemAllocDevice(ze_context_handle_t context, const ze_device_mem_alloc_desc_t* device_desc, size_t size,
                 size_t alignment, ze_device_handle_t device, void** addr)
{
    (void)context;
    (void)device_desc;
    (void)alignment;
    (void)device;
    *addr = malloc(size);
    return *addr == nullptr ? ZE_RESULT_ERROR_OUT_OF_DEVICE_MEMORY : ZE_RESULT_SUCCESS;
}

ze_result_t
zeMemFree(ze_context_handle_t context, void* addr)
{
    (void)context;
    free(addr);
    return ZE_RESULT_SUCCESS;
}

ze_result_t
zeMemGetIpcHandle(ze_context_handle_t context, const void* addr, ze_ipc_mem_handle_t* ipc_handle)
{
    (void)context;
    memset(ipc_handle, 0, sizeof(*ipc_handle));
    memcpy(ipc_handle, &addr, sizeof(addr));
    return ZE_RESULT_SUCCESS;
}

ze_result_t
zeMemPutIpcHandle(ze_context_handle_t context, ze_ipc_mem_handle_t ipc_handle)
{
    (void)context;
    (void)ipc_handle;
    return ZE_RESULT_SUCCESS;
}

ze_result_t
zeMemOpenIpcHandle(ze_context_handle_t context, ze_device_handle_t device, ze_ipc_mem_handle_t ipc_handle,
                   ze_ipc_memory_flags_t flags, void** addr)
{
    (void)context;
    (void)device;
    (void)flags;
    memcpy(addr, &ipc_handle, sizeof(*addr));
    return ZE_RESULT_SUCCESS;
}

ze_result_t
zeMemCloseIpcHandle(ze_context_handle_t context, const void* addr)
{
    (void)context;
    (void)addr;
    return ZE_RESULT_SUCCESS;
}

ze_result_t
zeCommandListCreate(ze_context_handle_t context, ze_device_handle_t device,
                    const ze_command_list_desc_t* desc, ze_command_list_handle_t* command_list)
{
    (void)context;
    (void)device;
    (void)desc;
    *command_list = stub_handle<ze_command_list_handle_t>(STUB_OBJECT);
    return ZE_RESULT_SUCCESS;
}

ze_result_t
zeCommandListCreateImmediate(ze_context_handle_t context, ze_device_handle_t device,
                             const ze_command_queue_desc_t* desc, ze_command_list_handle_t* command_list)
{
    (void)context;
    (void)device;
    (void)desc;
    *command_list = stub_handle<ze_command_list_handle_t>(STUB_OBJECT);
    return ZE_RESULT_SUCCESS;
}

ze_result_t
zeCommandListDestroy(ze_command_list_handle_t command_list)
{
    (void)command_list;
    return ZE_RESULT_SUCCESS;
}

ze_result_t
zeCommandListAppendMemoryCopy(ze_command_list_handle_t command_list, void* dst_addr, const void* src_addr,
                              size_t size, ze_event_handle_t signal, uint32_t num_events,
                              ze_event_handle_t* wait_events)
{
    (void)command_list;
    (void)signal;
    (void)num_events;
    (void)wait_events;
    memcpy(dst_addr, src_addr, size);
    return ZE_RESULT_SUCCESS;
}

ze_result_t
zeCommandListAppendMemoryFill(ze_command_list_handle_t command_list, void* addr, const void* pattern,
                              size_t pattern_size, size_t size, ze_event_handle_t signal_event,
                              uint32_t num_wait_events, ze_event_handle_t* wait_events)
{
    (void)command_list;
    (void)signal_event;
    (void)num_wait_events;
    (void)wait_events;

    if (pattern_size == 0u) {
        return ZE_RESULT_ERROR_INVALID_SIZE;
    }
    memset(addr, *static_cast<const unsigned char*>(pattern), size);
    return ZE_RESULT_SUCCESS;
}

ze_result_t
zeCommandListAppendBarrier(ze_command_list_handle_t command_list, ze_event_handle_t signal_event,
                           uint32_t num_wait_events, ze_event_handle_t* wait_events)
{
    (void)command_list;
    (void)signal_event;
    (void)num_wait_events;
    (void)wait_events;
    return ZE_RESULT_SUCCESS;
}

ze_result_t
zeCommandListClose(ze_command_list_handle_t command_list)
{
    (void)command_list;
    return ZE_RESULT_SUCCESS;
}

ze_result_t
zeCommandListReset(ze_command_list_handle_t command_list)
{
    (void)command_list;
    return ZE_RESULT_SUCCESS;
}

ze_result_t
zeCommandQueueCreate(ze_context_handle_t context, ze_device_handle_t device,
                     const ze_command_queue_desc_t* desc, ze_command_queue_handle_t* command_queue)
{
    (void)context;
    (void)device;
    (void)desc;
    *command_queue = stub_handle<ze_command_queue_handle_t>(STUB_OBJECT);
    return ZE_RESULT_SUCCESS;
}

ze_result_t
zeCommandQueueDestroy(ze_command_queue_handle_t command_queue)
{
    (void)command_queue;
    return ZE_RESULT_SUCCESS;
}

ze_result_t
zeCommandQueueExecuteCommandLists(ze_command_queue_handle_t command_queue, uint32_t num_lists,
                                  ze_command_list_handle_t* command_lists, ze_fence_handle_t fence)
{
    (void)command_queue;
    (void)num_lists;
    (void)command_lists;
    (void)fence;
    return ZE_RESULT_SUCCESS;
}

ze_result_t
zeCommandQueueSynchronize(ze_command_queue_handle_t command_queue, uint64_t timeout)
{
    (void)command_queue;
    (void)timeout;
    return ZE_RESULT_SUCCESS;
}

ze_result_t
zeFenceCreate(ze_command_queue_handle_t command_queue, const ze_fence_desc_t* fence_desc,
              ze_fence_handle_t* fence)
{
    (void)command_queue;
    (void)fence_desc;
    *fence = stub_handle<ze_fence_handle_t>(STUB_OBJECT);
    return ZE_RESULT_SUCCESS;
}

ze_result_t
zeFenceDestroy(ze_fence_handle_t fence)
{
    (void)fence;
    return ZE_RESULT_SUCCESS;
}

ze_result_t
zeFenceHostSynchronize(ze_fence_handle_t fence, uint64_t timeout)
{
    (void)fence;
    (void)timeout;
    return ZE_RESULT_SUCCESS;
}

ze_result_t
zeFenceReset(ze_fence_handle_t fence)
{
    (void)fence;
    return ZE_RESULT_SUCCESS;
}

ze_result_t
zeEventPoolCreate(ze_context_handle_t context, const ze_event_pool_desc_t* event_pool_desc,
                  uint32_t num_devices, ze_device_handle_t* devices, ze_event_pool_handle_t* event_pool)
{
    (void)context;
    (void)event_pool_desc;
    (void)num_devices;
    (void)devices;
    *event_pool = stub_handle<ze_event_pool_handle_t>(STUB_OBJECT);
    return ZE_RESULT_SUCCESS;
}

ze_result_t
zeEventPoolDestroy(ze_event_pool_handle_t event_pool)
{
    (void)event_pool;
    return ZE_RESULT_SUCCESS;
}

ze_result_t
zeEventCreate(ze_event_pool_handle_t event_pool, const ze_event_desc_t* event_desc, ze_event_handle_t* event)
{
    (void)event_pool;
    (void)event_desc;
    *event = stub_handle<ze_event_handle_t>(STUB_OBJECT);
    return ZE_RESULT_SUCCESS;
}

ze_result_t
zeEventDestroy(ze_event_handle_t event)
{
    (void)event;
    return ZE_RESULT_SUCCESS;
}

ze_result_t
zeEventHostSynchronize(ze_event_handle_t event, uint64_t timeout)
{
    (void)timeout;
    return event == stub_handle<ze_event_handle_t>(STUB_OBJECT)
        ? ZE_RESULT_SUCCESS : ZE_RESULT_ERROR_INVALID_NULL_HANDLE;
}

ze_result_t
zeEventHostReset(ze_event_handle_t event)
{
    return event == stub_handle<ze_event_handle_t>(STUB_OBJECT)
        ? ZE_RESULT_SUCCESS : ZE_RESULT_ERROR_INVALID_NULL_HANDLE;
}

} // extern "C"
