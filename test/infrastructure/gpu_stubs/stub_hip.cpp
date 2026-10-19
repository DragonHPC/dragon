/*
 * Stub implementation of the HIP runtime symbols that Dragon's GPU backend
 * resolves with dlsym. Built against the real HIP headers so the versioned
 * hipGetDeviceProperties alias and hipDeviceProp_t layout match Dragon's.
 *
 * Runtime behavior is controlled by environment variables:
 *   DRAGON_STUB_HIP_DEVICE_COUNT  number of fake devices (<0 => init failure)
 *   HIP_VISIBLE_DEVICES           comma-separated list; its entry count
 *     becomes the effective device count, mirroring how the real HIP
 *     runtime renumbers visible devices consecutively from 0.
 */

#ifndef __HIP_PLATFORM_AMD__
#define __HIP_PLATFORM_AMD__
#endif

#include <cstdio>
#include <cstdlib>
#include <cstring>

#include <hip/hip_runtime_api.h>

namespace
{

int
stub_device_count()
{
    const char* env = getenv("DRAGON_STUB_HIP_DEVICE_COUNT");
    return env == nullptr ? 0 : atoi(env);
}

int
stub_visible_count()
{
    const char* env = getenv("HIP_VISIBLE_DEVICES");
    if (env == nullptr) {
        return -1;
    }

    int count = 1;
    for (const char* c = env; *c != '\0'; ++c) {
        if (*c == ',') {
            ++count;
        }
    }
    return count;
}

int
stub_effective_count()
{
    const int visible = stub_visible_count();
    return visible >= 0 ? visible : stub_device_count();
}

void
stub_fill_uuid(char* bytes, int device_id)
{
    for (int i = 0; i < 16; ++i) {
        bytes[i] = static_cast<char>((device_id * 0x10 + i) & 0xFF);
    }
}

bool
stub_valid_device(int device_id)
{
    const int count = stub_effective_count();
    return device_id >= 0 && device_id < count;
}

} // namespace

extern "C" {

hipError_t
hipGetDeviceCount(int* count)
{
    const int n = stub_effective_count();
    if (n < 0) {
        return hipErrorNoDevice;
    }
    *count = n;
    return hipSuccess;
}

hipError_t
hipGetDevice(int* device)
{
    *device = 0;
    return hipSuccess;
}

hipError_t
hipSetDevice(int device_id)
{
    return stub_valid_device(device_id) ? hipSuccess : hipErrorInvalidDevice;
}

hipError_t
hipSetDeviceFlags(unsigned int flags)
{
    (void)flags;
    return hipSuccess;
}

hipError_t
hipDeviceSynchronize(void)
{
    return hipSuccess;
}

hipError_t
hipMalloc(void** addr, size_t size)
{
    *addr = malloc(size);
    return *addr == nullptr ? hipErrorOutOfMemory : hipSuccess;
}

hipError_t
hipFree(void* addr)
{
    free(addr);
    return hipSuccess;
}

hipError_t
hipMemcpy(void* dst_addr, const void* src_addr, size_t size, hipMemcpyKind kind)
{
    (void)kind;
    memcpy(dst_addr, src_addr, size);
    return hipSuccess;
}

hipError_t
hipMemset(void* addr, int val, size_t num_bytes)
{
    memset(addr, val, num_bytes);
    return hipSuccess;
}

hipError_t
hipHostRegister(void* ptr, size_t size, unsigned int flags)
{
    (void)ptr;
    (void)size;
    (void)flags;
    return hipSuccess;
}

hipError_t
hipHostUnregister(void* ptr)
{
    (void)ptr;
    return hipSuccess;
}

hipError_t
hipIpcGetMemHandle(hipIpcMemHandle_t* ipc_handle, void* addr)
{
    memset(ipc_handle, 0, sizeof(*ipc_handle));
    memcpy(ipc_handle, &addr, sizeof(addr));
    return hipSuccess;
}

hipError_t
hipIpcOpenMemHandle(void** addr, hipIpcMemHandle_t ipc_handle, unsigned int flags)
{
    (void)flags;
    memcpy(addr, &ipc_handle, sizeof(*addr));
    return hipSuccess;
}

hipError_t
hipIpcCloseMemHandle(void* addr)
{
    (void)addr;
    return hipSuccess;
}

const char*
hipGetErrorString(hipError_t hip_rc)
{
    return hip_rc == hipSuccess ? "stub success" : "stub hip error";
}

// hipGetDeviceProperties/hipDeviceProp_t are versioned macro aliases; letting
// them expand here produces exactly the symbol Dragon resolves.
hipError_t
hipGetDeviceProperties(hipDeviceProp_t* prop, int device)
{
    if (!stub_valid_device(device)) {
        return hipErrorInvalidDevice;
    }

    memset(prop, 0, sizeof(*prop));
    snprintf(prop->name, sizeof(prop->name), "Dragon Stub HIP Device %d", device);
    stub_fill_uuid(prop->uuid.bytes, device);
    prop->totalGlobalMem = 1024ul * 1024ul * 1024ul;

    return hipSuccess;
}

} // extern "C"
