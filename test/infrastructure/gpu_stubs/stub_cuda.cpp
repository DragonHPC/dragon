/*
 * Stub implementation of the CUDA runtime symbols that Dragon's GPU backend
 * resolves with dlsym. Built against the real CUDA headers so that struct
 * layouts (notably cudaDeviceProp) match what Dragon compiles against, but it
 * never touches a device.
 *
 * Runtime behavior is controlled by environment variables:
 *   DRAGON_STUB_CUDA_DEVICE_COUNT  number of fake devices (<0 => init failure)
 *   CUDA_VISIBLE_DEVICES           comma-separated list; its entry count
 *     becomes the effective device count, mirroring how the real CUDA
 *     runtime renumbers visible devices consecutively from 0.
 */

#include <cstdio>
#include <cstdlib>
#include <cstring>

#include <cuda_runtime.h>

// CUDA 12 headers alias this to cudaGetDeviceProperties_v2, but Dragon resolves
// the unversioned name, so the stub must export it literally.
#ifdef cudaGetDeviceProperties
#undef cudaGetDeviceProperties
#endif

namespace
{

int
stub_device_count()
{
    const char* env = getenv("DRAGON_STUB_CUDA_DEVICE_COUNT");
    return env == nullptr ? 0 : atoi(env);
}

int
stub_visible_count()
{
    const char* env = getenv("CUDA_VISIBLE_DEVICES");
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

cudaError_t
cudaGetDeviceCount(int* count)
{
    const int n = stub_effective_count();
    if (n < 0) {
        return cudaErrorNoDevice;
    }
    *count = n;
    return cudaSuccess;
}

cudaError_t
cudaGetDevice(int* device)
{
    *device = 0;
    return cudaSuccess;
}

cudaError_t
cudaSetDevice(int device_id)
{
    return stub_valid_device(device_id) ? cudaSuccess : cudaErrorInvalidDevice;
}

cudaError_t
cudaSetDeviceFlags(unsigned int flags)
{
    (void)flags;
    return cudaSuccess;
}

cudaError_t
cudaDeviceSynchronize(void)
{
    return cudaSuccess;
}

cudaError_t
cudaMalloc(void** addr, size_t size)
{
    *addr = malloc(size);
    return *addr == nullptr ? cudaErrorMemoryAllocation : cudaSuccess;
}

cudaError_t
cudaFree(void* addr)
{
    free(addr);
    return cudaSuccess;
}

cudaError_t
cudaMemcpy(void* dst_addr, const void* src_addr, size_t size, cudaMemcpyKind kind)
{
    (void)kind;
    memcpy(dst_addr, src_addr, size);
    return cudaSuccess;
}

cudaError_t
cudaMemset(void* addr, int val, size_t num_bytes)
{
    memset(addr, val, num_bytes);
    return cudaSuccess;
}

cudaError_t
cudaHostRegister(void* ptr, size_t size, unsigned int flags)
{
    (void)ptr;
    (void)size;
    (void)flags;
    return cudaSuccess;
}

cudaError_t
cudaHostUnregister(void* ptr)
{
    (void)ptr;
    return cudaSuccess;
}

cudaError_t
cudaIpcGetMemHandle(cudaIpcMemHandle_t* ipc_handle, void* addr)
{
    memset(ipc_handle, 0, sizeof(*ipc_handle));
    memcpy(ipc_handle, &addr, sizeof(addr));
    return cudaSuccess;
}

cudaError_t
cudaIpcOpenMemHandle(void** addr, cudaIpcMemHandle_t ipc_handle, unsigned int flags)
{
    (void)flags;
    memcpy(addr, &ipc_handle, sizeof(*addr));
    return cudaSuccess;
}

cudaError_t
cudaIpcCloseMemHandle(void* addr)
{
    (void)addr;
    return cudaSuccess;
}

const char*
cudaGetErrorString(cudaError_t cuda_rc)
{
    return cuda_rc == cudaSuccess ? "stub success" : "stub cuda error";
}

cudaError_t
cudaGetDeviceProperties(cudaDeviceProp* prop, int device)
{
    if (!stub_valid_device(device)) {
        return cudaErrorInvalidDevice;
    }

    memset(prop, 0, sizeof(*prop));
    snprintf(prop->name, sizeof(prop->name), "Dragon Stub CUDA Device %d", device);
    stub_fill_uuid(prop->uuid.bytes, device);
    prop->major = 8;
    prop->minor = 0;
    prop->totalGlobalMem = 1024ul * 1024ul * 1024ul;

    return cudaSuccess;
}

} // extern "C"
