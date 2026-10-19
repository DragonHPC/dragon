#ifdef HAVE_HIP_INCLUDE

#include "hip.hpp"

// init hip backend

// hipGetDeviceProperties is a versioned macro alias (e.g. hipGetDevicePropertiesR0600);
// stringify after macro expansion so dlsym resolves the real exported symbol.
#define _DRAGON_STRINGIFY(x) #x
#define _DRAGON_TOSTRING(x) _DRAGON_STRINGIFY(x)

hipError_t (*fn_hipMalloc)(void** addr, size_t size);
hipError_t (*fn_hipFree)(void* addr);
hipError_t (*fn_hipMemcpy)(void* dst_addr, const void* src_addr, size_t size, hipMemcpyKind kind);
hipError_t (*fn_hipMemset)(void* addr, int val, size_t num_bytes);
hipError_t (*fn_hipSetDeviceFlags)(unsigned int flags);
hipError_t (*fn_hipSetDevice)(int deviceID);
hipError_t (*fn_hipDeviceSynchronize)(void);
hipError_t (*fn_hipIpcGetMemHandle)(hipIpcMemHandle_t* ipc_handle, void* addr);
hipError_t (*fn_hipIpcOpenMemHandle)(void** addr, hipIpcMemHandle_t ipc_handle, unsigned int flags);
hipError_t (*fn_hipIpcCloseMemHandle)(void* addr);
const char* (*fn_hipGetErrorString)(hipError_t hip_rc);
hipError_t (*fn_hipHostRegister)(void* ptr, size_t size, unsigned int flags);
hipError_t (*fn_hipHostUnregister)(void* ptr);
hipError_t (*fn_hipGetDevice)(int* device);
hipError_t (*fn_hipGetDeviceCount)(int* count);
hipError_t (*fn_hipGetDeviceProperties)(hipDeviceProp_t* prop, int device);

static int symbols_loaded = 0;

void*
_open_hip_lib()
{
    return _open_gpu_runtime_lib("hip_runtime_lib", dragonGPU_hip::libname);
}

dragonError_t
dragon_gpu_resolve_hip_symbols(void* libhandle)
{

    if (symbols_loaded)
        no_err_return(DRAGON_SUCCESS);

    if (libhandle == nullptr)
        err_return(DRAGON_GPU_BE_NOT_FOUND, "failed to dlopen HIP backend library");

    fn_hipMalloc = (hipError_t (*)(void**, size_t))dlsym(libhandle, "hipMalloc");
    if (fn_hipMalloc == nullptr)
        err_return(DRAGON_GPU_BE_NOT_FOUND, "failed to resolve hipMalloc symbol");

    fn_hipFree = (hipError_t (*)(void*))dlsym(libhandle, "hipFree");
    if (fn_hipFree == nullptr)
        err_return(DRAGON_GPU_BE_NOT_FOUND, "failed to resolve hipFree symbol");

    fn_hipMemcpy = (hipError_t (*)(void*, const void*, size_t, hipMemcpyKind))dlsym(libhandle, "hipMemcpy");
    if (fn_hipMemcpy == nullptr)
        err_return(DRAGON_GPU_BE_NOT_FOUND, "failed to resolve hipMemcpy symbol");

    fn_hipMemset = (hipError_t (*)(void*, int, size_t))dlsym(libhandle, "hipMemset");
    if (fn_hipMemset == nullptr)
        err_return(DRAGON_GPU_BE_NOT_FOUND, "failed to resolve hipMemset symbol");

    fn_hipSetDeviceFlags = (hipError_t (*)(unsigned int))dlsym(libhandle, "hipSetDeviceFlags");
    if (fn_hipSetDeviceFlags == nullptr)
        err_return(DRAGON_GPU_BE_NOT_FOUND, "failed to resolve hipSetDeviceFlags symbol");

    fn_hipSetDevice = (hipError_t (*)(int))dlsym(libhandle, "hipSetDevice");
    if (fn_hipSetDevice == nullptr)
        err_return(DRAGON_GPU_BE_NOT_FOUND, "failed to resolve hipSetDevice symbol");

    fn_hipGetDevice = (hipError_t (*)(int*))dlsym(libhandle, "hipGetDevice");
    if (fn_hipGetDevice == nullptr)
        err_return(DRAGON_GPU_BE_NOT_FOUND, "failed to resolve hipGetDevice symbol");

    fn_hipDeviceSynchronize = (hipError_t (*)(void))dlsym(libhandle, "hipDeviceSynchronize");
    if (fn_hipDeviceSynchronize == nullptr)
        err_return(DRAGON_GPU_BE_NOT_FOUND, "failed to resolve hipDeviceSynchronize symbol");

    fn_hipIpcGetMemHandle = (hipError_t (*)(hipIpcMemHandle_t*, void*))dlsym(libhandle, "hipIpcGetMemHandle");
    if (fn_hipIpcGetMemHandle == nullptr)
        err_return(DRAGON_GPU_BE_NOT_FOUND, "failed to resolve hipIpcGetMemHandle symbol");

    fn_hipIpcOpenMemHandle =
      (hipError_t (*)(void**, hipIpcMemHandle_t, unsigned int))dlsym(libhandle, "hipIpcOpenMemHandle");
    if (fn_hipIpcOpenMemHandle == nullptr)
        err_return(DRAGON_GPU_BE_NOT_FOUND, "failed to resolve hipIpcOpenMemHandle symbol");

    fn_hipIpcCloseMemHandle = (hipError_t (*)(void*))dlsym(libhandle, "hipIpcCloseMemHandle");
    if (fn_hipIpcCloseMemHandle == nullptr)
        err_return(DRAGON_GPU_BE_NOT_FOUND, "failed to resolve hipIpcCloseMemHandle symbol");

    fn_hipGetErrorString = (const char* (*)(hipError_t))dlsym(libhandle, "hipGetErrorString");
    if (fn_hipGetErrorString == nullptr)
        err_return(DRAGON_GPU_BE_NOT_FOUND, "failed to resolve hipGetErrorString symbol");

    fn_hipGetDeviceCount = (hipError_t (*)(int*))dlsym(libhandle, "hipGetDeviceCount");
    if (fn_hipGetDeviceCount == nullptr)
        err_return(DRAGON_GPU_BE_NOT_FOUND, "failed to resolve hipGetDeviceCount symbol");

    fn_hipHostRegister = (hipError_t (*)(void*, size_t, unsigned int))dlsym(libhandle, "hipHostRegister");
    if (fn_hipHostRegister == nullptr)
        err_return(DRAGON_GPU_BE_NOT_FOUND, "failed to resolve hipHostRegister symbol");

    fn_hipHostUnregister = (hipError_t (*)(void*))dlsym(libhandle, "hipHostUnregister");
    if (fn_hipHostUnregister == nullptr)
        err_return(DRAGON_GPU_BE_NOT_FOUND, "failed to resolve hipHostUnregister symbol");

    fn_hipGetDeviceProperties =
      (hipError_t (*)(hipDeviceProp_t*, int))dlsym(libhandle, _DRAGON_TOSTRING(hipGetDeviceProperties));
    if (fn_hipGetDeviceProperties == nullptr)
        err_return(DRAGON_GPU_BE_NOT_FOUND, "failed to resolve hipGetDeviceProperties symbol");

    symbols_loaded = 1;
    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
_setup_hip(void* libhandle, void* dragon_gpu_handle)
{
    dragonGPUHandle_t* gpuh;
    gpuh = (dragonGPUHandle_t*)dragon_gpu_handle;
    dragonError_t derr = dragon_gpu_resolve_hip_symbols(libhandle);
    if (derr != DRAGON_SUCCESS)
        append_err_return(derr, "failed to resolve HIP symbols");

    try {
        gpuh->dgpu = std::make_shared<dragonGPU_hip>();
    } catch (std::exception& e) {
        append_err_return(DRAGON_GPU_ERROR, e.what());
    }

    no_err_return(DRAGON_SUCCESS);
}

// member function definitions
dragonGPU_hip::dragonGPU_hip()
{
    this->device_idx = -1; // initial value

    // Try getting the number of devices
    auto hip_rc = fn_hipGetDeviceCount(&this->num_devices);
    if (hip_rc != hipSuccess) {
        auto errstr = this->get_errstr("failed to get hip device count", hip_rc);
        throw std::runtime_error(errstr.c_str());
    }

    // HIP libraries installed but no devices found: treat this the same as the
    // backend not being present so detection can fall through to other backends.
    if (this->num_devices == 0) {
        throw std::runtime_error("no HIP devices available");
    }
}

dragonError_t
dragonGPU_hip::set_device(int device_id)
{
    auto flags = hipDeviceScheduleBlockingSync;

    auto hip_rc = fn_hipSetDevice(device_id);
    if (hip_rc != hipSuccess) {
        append_err_return(DRAGON_GPU_ERROR, this->get_errstr("failed to set device id", hip_rc).c_str());
    }

    hip_rc = fn_hipSetDeviceFlags(flags);
    if (hip_rc != hipSuccess) {
        append_err_return(DRAGON_GPU_ERROR, this->get_errstr("failed to set device flags", hip_rc).c_str());
    }

    this->device_idx = device_id;
    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragonGPU_hip::mem_alloc(void** addr, size_t size)
{
    auto hip_rc = fn_hipMalloc(addr, size);
    if (hip_rc != hipSuccess) {
        append_err_return(DRAGON_GPU_ERROR,
                          this->get_errstr("failed to allocate device memory", hip_rc).c_str());
    }

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragonGPU_hip::mem_free(void* addr)
{
    auto hip_rc = fn_hipFree(addr);
    if (hip_rc != hipSuccess) {
        append_err_return(DRAGON_GPU_ERROR, this->get_errstr("failed to free device memory", hip_rc).c_str());
    }
    addr = nullptr;
    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragonGPU_hip::get_ipc_handle(void* addr, void** ipc_handle_out)
{
    size_t ipc_handle_size = sizeof(hipIpcMemHandle_t);
    *ipc_handle_out = malloc(ipc_handle_size);
    if (*ipc_handle_out == nullptr) {
        append_err_return(DRAGON_INTERNAL_MALLOC_FAIL, "failed to allocate IPC handle");
    }

    hipIpcMemHandle_t* ipc_handle = static_cast<hipIpcMemHandle_t*>(*ipc_handle_out);

    auto hip_rc = fn_hipIpcGetMemHandle(ipc_handle, addr);
    if (hip_rc != hipSuccess) {
        append_err_return(DRAGON_GPU_ERROR, this->get_errstr("failed to get IPC handle", hip_rc).c_str());
    }

    no_err_return(DRAGON_SUCCESS);
}

size_t
dragonGPU_hip::get_ipc_handle_size()
{
    return sizeof(hipIpcMemHandle_t);
}

dragonError_t
dragonGPU_hip::free_ipc_handle(void** ipc_handle)
{
    // this is a no-op for hip
    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragonGPU_hip::attach(void* ipc_handle_in, void** addr)
{
    hipIpcMemHandle_t ipc_handle;
    size_t ipc_handle_size = sizeof(hipIpcMemHandle_t);
    memcpy(&ipc_handle, ipc_handle_in, ipc_handle_size);

    auto flags = hipIpcMemLazyEnablePeerAccess;

    auto hip_rc = fn_hipIpcOpenMemHandle(addr, ipc_handle, flags);
    if (hip_rc != hipSuccess) {
        append_err_return(DRAGON_GPU_ERROR, this->get_errstr("failed to open IPC handle", hip_rc).c_str());
    }

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragonGPU_hip::detach(void* addr)
{
    auto hip_rc = fn_hipIpcCloseMemHandle(addr);
    if (hip_rc != hipSuccess) {
        append_err_return(DRAGON_GPU_ERROR, this->get_errstr("failed to close IPC handle", hip_rc).c_str());
    }

    no_err_return(DRAGON_SUCCESS);
}

static hipMemcpyKind
dragon_to_hip_memcpy_kind(dragonGPUMemcpyType_t memcpy_type)
{
    switch (memcpy_type) {
        case DRAGON_GPU_D2D: {
            return hipMemcpyDeviceToDevice;
        }
        case DRAGON_GPU_D2H: {
            return hipMemcpyDeviceToHost;
        }
        case DRAGON_GPU_H2D: {
            return hipMemcpyHostToDevice;
        }
        default: {
            assert("invalid memcpy type");
        }
    }

    return hipMemcpyDefault;
}

dragonError_t
dragonGPU_hip::copy(void* dst_addr, const void* src_addr, size_t size, dragonGPUMemcpyType_t memcpy_type)
{
    auto hip_rc = fn_hipMemcpy(dst_addr, src_addr, size, dragon_to_hip_memcpy_kind(memcpy_type));
    if (hip_rc != hipSuccess) {
        append_err_return(DRAGON_GPU_ERROR, this->get_errstr("failed to copy memory", hip_rc).c_str());
    }

    hip_rc = fn_hipDeviceSynchronize();
    if (hip_rc != hipSuccess) {
        append_err_return(DRAGON_GPU_ERROR, this->get_errstr("failed to synchronize device", hip_rc).c_str());
    }

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragonGPU_hip::memset(void* addr, int val, size_t num_bytes)
{
    auto hip_rc = fn_hipMemset(addr, val, num_bytes);
    if (hip_rc != hipSuccess) {
        append_err_return(DRAGON_GPU_ERROR, this->get_errstr("failed to set memory", hip_rc).c_str());
    }

    hip_rc = fn_hipDeviceSynchronize();
    if (hip_rc != hipSuccess) {
        append_err_return(DRAGON_GPU_ERROR, this->get_errstr("failed to synchronize device", hip_rc).c_str());
    }

    no_err_return(DRAGON_SUCCESS);
}

std::string
dragonGPU_hip::get_errstr(const char* event, int hip_rc)
{
    auto errstr = fn_hipGetErrorString((hipError_t)hip_rc);

    auto log_str = std::string(event) + std::string(": rc=") + std::to_string(hip_rc) +
                   (errstr ? std::string(", ") + std::string(errstr) : std::string(""));

    if (dragon_gpu_debug) {
        fprintf(dragon_gpu_log, "%s\n", log_str.c_str());
        fflush(dragon_gpu_log);
    }

    return log_str;
}

dragonError_t
dragonGPU_hip::host_register(void* addr, size_t size)
{
    auto hip_rc = fn_hipHostRegister(addr, size, hipHostRegisterPortable);
    if (hip_rc != hipSuccess) {
        append_err_return(DRAGON_GPU_ERROR,
                          this->get_errstr("failed to register host memory", hip_rc).c_str());
    }
    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragonGPU_hip::host_unregister(void* addr)
{
    auto hip_rc = fn_hipHostUnregister(addr);
    if (hip_rc != hipSuccess) {
        append_err_return(DRAGON_GPU_ERROR,
                          this->get_errstr("failed to unregister host memory", hip_rc).c_str());
    }
    no_err_return(DRAGON_SUCCESS);
}

int
dragonGPU_hip::get_device_count()
{
    return this->num_devices;
}

dragonError_t
dragonGPU_hip::get_device_list(int** int_device_list, double** double_device_list, int* num_devices)
{
    *int_device_list = nullptr;
    *double_device_list = nullptr;

    if (this->num_devices == 0) {
        append_err_return(DRAGON_GPU_ERROR, "no HIP devices available");
    }

    int count = this->num_devices;

    int* list_ptr = static_cast<int*>(malloc(count * sizeof(int)));
    if (list_ptr == nullptr) {
        append_err_return(DRAGON_INTERNAL_MALLOC_FAIL, "failed to allocate device list");
    }

    for (int i = 0; i < count; i++) {
        list_ptr[i] = i;
    }

    *int_device_list = list_ptr;
    *num_devices = count;

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragonGPU_hip::uuid_hex_to_str(char* uuid_hex, char** uuid_str)
{

    constexpr int UUID_STR_LEN = 40; // GPU- + 8-4-4-4-12 hex digits
    *uuid_str = static_cast<char*>(malloc(UUID_STR_LEN + 1));
    if (*uuid_str == nullptr)
        append_err_return(DRAGON_INTERNAL_MALLOC_FAIL, "failed to allocate UUID string");

    int written =
      snprintf(*uuid_str, UUID_STR_LEN + 1,
               "GPU-"
               "%02x%02x%02x%02x-"
               "%02x%02x-"
               "%02x%02x-"
               "%02x%02x-"
               "%02x%02x%02x%02x%02x%02x",
               static_cast<unsigned char>(uuid_hex[0]), static_cast<unsigned char>(uuid_hex[1]),
               static_cast<unsigned char>(uuid_hex[2]), static_cast<unsigned char>(uuid_hex[3]),
               static_cast<unsigned char>(uuid_hex[4]), static_cast<unsigned char>(uuid_hex[5]),
               static_cast<unsigned char>(uuid_hex[6]), static_cast<unsigned char>(uuid_hex[7]),
               static_cast<unsigned char>(uuid_hex[8]), static_cast<unsigned char>(uuid_hex[9]),
               static_cast<unsigned char>(uuid_hex[10]), static_cast<unsigned char>(uuid_hex[11]),
               static_cast<unsigned char>(uuid_hex[12]), static_cast<unsigned char>(uuid_hex[13]),
               static_cast<unsigned char>(uuid_hex[14]), static_cast<unsigned char>(uuid_hex[15]));

    if (written != UUID_STR_LEN) {
        free(*uuid_str);
        *uuid_str = nullptr;
        append_err_return(DRAGON_GPU_ERROR, "failed to format UUID string");
    }

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragonGPU_hip::get_uuid(int device_id, char** uuid)
{

    if (uuid == nullptr) {
        err_return(DRAGON_INVALID_ARGUMENT, "uuid output pointer must be non-null");
    }

    hipDeviceProp_t prop;
    auto hip_rc = fn_hipGetDeviceProperties(&prop, device_id);
    if (hip_rc != hipSuccess)
        append_err_return(DRAGON_GPU_ERROR,
                          this->get_errstr("failed to query device properties", hip_rc).c_str());

    dragonError_t derr = this->uuid_hex_to_str(prop.uuid.bytes, uuid);
    if (derr != DRAGON_SUCCESS) {
        append_err_return(derr, "failed to convert UUID to string");
    }

    no_err_return(DRAGON_SUCCESS);
}

#endif // HAVE_HIP_INCLUDE
