#ifdef HAVE_CUDA_INCLUDE

#include "cuda.hpp"

// init cuda backend

cudaError_t (*fn_cudaMalloc)(void** addr, size_t size);
cudaError_t (*fn_cudaFree)(void* addr);
cudaError_t (*fn_cudaMemcpy)(void* dst_addr, const void* src_addr, size_t size, cudaMemcpyKind kind);
cudaError_t (*fn_cudaMemset)(void* addr, int val, size_t num_bytes);
cudaError_t (*fn_cudaSetDeviceFlags)(unsigned int flags);
cudaError_t (*fn_cudaSetDevice)(int deviceID);
cudaError_t (*fn_cudaDeviceSynchronize)(void);
cudaError_t (*fn_cudaIpcGetMemHandle)(cudaIpcMemHandle_t* ipc_handle, void* addr);
cudaError_t (*fn_cudaIpcOpenMemHandle)(void** addr, cudaIpcMemHandle_t ipc_handle, unsigned int flags);
cudaError_t (*fn_cudaIpcCloseMemHandle)(void* addr);
const char* (*fn_cudaGetErrorString)(cudaError_t cuda_rc);
cudaError_t (*fn_cudaHostRegister)(void* ptr, size_t size, unsigned int flags);
cudaError_t (*fn_cudaHostUnregister)(void* ptr);
cudaError_t (*fn_cudaGetDevice)(int* device);
cudaError_t (*fn_cudaGetDeviceCount)(int* count);
cudaError_t (*fn_cudaGetDeviceProperties)(cudaDeviceProp* prop, int device);

static int symbols_loaded = 0;

void*
_open_cuda_lib()
{
    return _open_gpu_runtime_lib("cuda_runtime_lib", dragonGPU_cuda::libname);
}

dragonError_t
_resolve_cuda_symbols(void* libhandle)
{

    if (symbols_loaded)
        no_err_return(DRAGON_SUCCESS);

    if (libhandle == nullptr)
        err_return(DRAGON_GPU_BE_NOT_FOUND, "failed to dlopen CUDA backend library");

    fn_cudaMalloc = (cudaError_t (*)(void**, size_t))dlsym(libhandle, "cudaMalloc");
    if (fn_cudaMalloc == nullptr)
        err_return(DRAGON_GPU_BE_NOT_FOUND, "failed to resolve cudaMalloc symbol");

    fn_cudaFree = (cudaError_t (*)(void*))dlsym(libhandle, "cudaFree");
    if (fn_cudaFree == nullptr)
        err_return(DRAGON_GPU_BE_NOT_FOUND, "failed to resolve cudaFree symbol");

    fn_cudaMemcpy =
      (cudaError_t (*)(void*, const void*, size_t, cudaMemcpyKind))dlsym(libhandle, "cudaMemcpy");
    if (fn_cudaMemcpy == nullptr)
        err_return(DRAGON_GPU_BE_NOT_FOUND, "failed to resolve cudaMemcpy symbol");

    fn_cudaMemset = (cudaError_t (*)(void*, int, size_t))dlsym(libhandle, "cudaMemset");
    if (fn_cudaMemset == nullptr)
        err_return(DRAGON_GPU_BE_NOT_FOUND, "failed to resolve cudaMemset symbol");

    fn_cudaSetDeviceFlags = (cudaError_t (*)(unsigned int))dlsym(libhandle, "cudaSetDeviceFlags");
    if (fn_cudaSetDeviceFlags == nullptr)
        err_return(DRAGON_GPU_BE_NOT_FOUND, "failed to resolve cudaSetDeviceFlags symbol");

    fn_cudaSetDevice = (cudaError_t (*)(int))dlsym(libhandle, "cudaSetDevice");
    if (fn_cudaSetDevice == nullptr)
        err_return(DRAGON_GPU_BE_NOT_FOUND, "failed to resolve cudaSetDevice symbol");

    fn_cudaGetDevice = (cudaError_t (*)(int*))dlsym(libhandle, "cudaGetDevice");
    if (fn_cudaGetDevice == nullptr)
        err_return(DRAGON_GPU_BE_NOT_FOUND, "failed to resolve cudaGetDevice symbol");

    fn_cudaDeviceSynchronize = (cudaError_t (*)(void))dlsym(libhandle, "cudaDeviceSynchronize");
    if (fn_cudaDeviceSynchronize == nullptr)
        err_return(DRAGON_GPU_BE_NOT_FOUND, "failed to resolve cudaDeviceSynchronize symbol");

    fn_cudaIpcGetMemHandle =
      (cudaError_t (*)(cudaIpcMemHandle_t*, void*))dlsym(libhandle, "cudaIpcGetMemHandle");
    if (fn_cudaIpcGetMemHandle == nullptr)
        err_return(DRAGON_GPU_BE_NOT_FOUND, "failed to resolve cudaIpcGetMemHandle symbol");

    fn_cudaIpcOpenMemHandle =
      (cudaError_t (*)(void**, cudaIpcMemHandle_t, unsigned int))dlsym(libhandle, "cudaIpcOpenMemHandle");
    if (fn_cudaIpcOpenMemHandle == nullptr)
        err_return(DRAGON_GPU_BE_NOT_FOUND, "failed to resolve cudaIpcOpenMemHandle symbol");

    fn_cudaIpcCloseMemHandle = (cudaError_t (*)(void*))dlsym(libhandle, "cudaIpcCloseMemHandle");
    if (fn_cudaIpcCloseMemHandle == nullptr)
        err_return(DRAGON_GPU_BE_NOT_FOUND, "failed to resolve cudaIpcCloseMemHandle symbol");

    fn_cudaGetErrorString = (const char* (*)(cudaError_t))dlsym(libhandle, "cudaGetErrorString");
    if (fn_cudaGetErrorString == nullptr)
        err_return(DRAGON_GPU_BE_NOT_FOUND, "failed to resolve cudaGetErrorString symbol");

    fn_cudaGetDevice = (cudaError_t (*)(int*))dlsym(libhandle, "cudaGetDevice");
    if (fn_cudaGetDevice == nullptr)
        err_return(DRAGON_GPU_BE_NOT_FOUND, "failed to resolve cudaGetDeviceCount symbol");

    fn_cudaGetDeviceCount = (cudaError_t (*)(int*))dlsym(libhandle, "cudaGetDeviceCount");
    if (fn_cudaGetDeviceCount == nullptr)
        err_return(DRAGON_GPU_BE_NOT_FOUND, "failed to resolve cudaGetDeviceCount symbol");

    fn_cudaHostRegister = (cudaError_t (*)(void*, size_t, unsigned int))dlsym(libhandle, "cudaHostRegister");
    if (fn_cudaHostRegister == nullptr)
        err_return(DRAGON_GPU_BE_NOT_FOUND, "failed to resolve cudaHostRegister symbol");

    fn_cudaHostUnregister = (cudaError_t (*)(void*))dlsym(libhandle, "cudaHostUnregister");
    if (fn_cudaHostUnregister == nullptr)
        err_return(DRAGON_GPU_BE_NOT_FOUND, "failed to resolve cudaHostUnregister symbol");

    fn_cudaGetDeviceProperties =
      (cudaError_t (*)(cudaDeviceProp*, int))dlsym(libhandle, "cudaGetDeviceProperties");
    if (fn_cudaGetDeviceProperties == nullptr)
        err_return(DRAGON_GPU_BE_NOT_FOUND, "failed to resolve cudaGetDeviceProperties symbol");

    symbols_loaded = 1;
    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
_setup_cuda(void* libhandle, void* dragon_gpu_handle)
{
    dragonGPUHandle_t* gpuh;
    gpuh = (dragonGPUHandle_t*)dragon_gpu_handle;
    dragonError_t derr = _resolve_cuda_symbols(libhandle);
    if (derr != DRAGON_SUCCESS)
        append_err_return(derr, "failed to resolve CUDA symbols");

    try {
        gpuh->dgpu = std::make_shared<dragonGPU_cuda>();
    } catch (std::exception& e) {
        gpuh->dgpu = nullptr;
        append_err_return(DRAGON_GPU_ERROR, e.what());
    }

    no_err_return(DRAGON_SUCCESS);
}

// member function definitions
dragonGPU_cuda::dragonGPU_cuda()
{
    this->device_idx = -1; // initial value

    // Try getting the number of devices
    auto cuda_rc = fn_cudaGetDeviceCount(&this->num_devices);
    if (cuda_rc != cudaSuccess) {
        auto errstr = this->get_errstr("failed to get cuda device count", cuda_rc);
        throw std::runtime_error(errstr.c_str());
    }

    // CUDA libraries installed but no devices found: treat this the same as the
    // backend not being present so detection can fall through to other backends.
    if (this->num_devices == 0) {
        throw std::runtime_error("no CUDA devices available");
    }
}

dragonError_t
dragonGPU_cuda::set_device(int device_id)
{
    auto flags = cudaDeviceScheduleBlockingSync;

    auto cuda_rc = fn_cudaSetDevice(device_id);
    if (cuda_rc != cudaSuccess) {
        append_err_return(DRAGON_GPU_ERROR, this->get_errstr("failed to set device id", cuda_rc).c_str());
    }

    cuda_rc = fn_cudaSetDeviceFlags(flags);
    if (cuda_rc != cudaSuccess) {
        append_err_return(DRAGON_GPU_ERROR, this->get_errstr("failed to set device flags", cuda_rc).c_str());
    }

    this->device_idx = device_id;
    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragonGPU_cuda::mem_alloc(void** addr, size_t size)
{
    auto cuda_rc = fn_cudaMalloc(addr, size);
    if (cuda_rc != cudaSuccess) {
        append_err_return(DRAGON_GPU_ERROR,
                          this->get_errstr("failed to allocate device memory", cuda_rc).c_str());
    }

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragonGPU_cuda::mem_free(void* addr)
{
    auto cuda_rc = fn_cudaFree(addr);
    if (cuda_rc != cudaSuccess) {
        append_err_return(DRAGON_GPU_ERROR,
                          this->get_errstr("failed to free device memory", cuda_rc).c_str());
    }
    addr = nullptr;
    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragonGPU_cuda::get_ipc_handle(void* addr, void** ipc_handle_out)
{
    size_t ipc_handle_size = sizeof(cudaIpcMemHandle_t);
    *ipc_handle_out = malloc(ipc_handle_size);
    if (*ipc_handle_out == nullptr) {
        append_err_return(DRAGON_INTERNAL_MALLOC_FAIL, "failed to allocate IPC handle");
    }

    cudaIpcMemHandle_t* ipc_handle = static_cast<cudaIpcMemHandle_t*>(*ipc_handle_out);

    auto cuda_rc = fn_cudaIpcGetMemHandle(ipc_handle, addr);
    if (cuda_rc != cudaSuccess) {
        append_err_return(DRAGON_GPU_ERROR, this->get_errstr("failed to get IPC handle", cuda_rc).c_str());
    }

    //*ipc_handle_out = static_cast<void *>(&ipc_handle);

    no_err_return(DRAGON_SUCCESS);
}

size_t
dragonGPU_cuda::get_ipc_handle_size()
{
    return sizeof(cudaIpcMemHandle_t);
}

dragonError_t
dragonGPU_cuda::free_ipc_handle(void** ipc_handle)
{
    // this is a no-op for cuda
    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragonGPU_cuda::attach(void* ipc_handle_in, void** addr)
{
    cudaIpcMemHandle_t ipc_handle;
    size_t ipc_handle_size = sizeof(cudaIpcMemHandle_t);
    memcpy(&ipc_handle, ipc_handle_in, ipc_handle_size);

    auto flags = cudaIpcMemLazyEnablePeerAccess;

    auto cuda_rc = fn_cudaIpcOpenMemHandle(addr, ipc_handle, flags);
    if (cuda_rc != cudaSuccess) {
        append_err_return(DRAGON_GPU_ERROR, this->get_errstr("failed to open IPC handle", cuda_rc).c_str());
    }

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragonGPU_cuda::detach(void* addr)
{
    auto cuda_rc = fn_cudaIpcCloseMemHandle(addr);
    if (cuda_rc != cudaSuccess) {
        append_err_return(DRAGON_GPU_ERROR, this->get_errstr("failed to close IPC handle", cuda_rc).c_str());
    }

    no_err_return(DRAGON_SUCCESS);
}

static cudaMemcpyKind
dragon_to_cuda_memcpy_kind(dragonGPUMemcpyType_t memcpy_type)
{
    switch (memcpy_type) {
        case DRAGON_GPU_D2D: {
            return cudaMemcpyDeviceToDevice;
        }
        case DRAGON_GPU_D2H: {
            return cudaMemcpyDeviceToHost;
        }
        case DRAGON_GPU_H2D: {
            return cudaMemcpyHostToDevice;
        }
        default: {
            assert("invalid memcpy type");
        }
    }

    return cudaMemcpyDefault;
}

dragonError_t
dragonGPU_cuda::copy(void* dst_addr, const void* src_addr, size_t size, dragonGPUMemcpyType_t memcpy_type)
{
    auto cuda_rc = fn_cudaMemcpy(dst_addr, src_addr, size, dragon_to_cuda_memcpy_kind(memcpy_type));
    if (cuda_rc != cudaSuccess) {
        append_err_return(DRAGON_GPU_ERROR, this->get_errstr("failed to copy memory", cuda_rc).c_str());
    }

    cuda_rc = fn_cudaDeviceSynchronize();
    if (cuda_rc != cudaSuccess) {
        append_err_return(DRAGON_GPU_ERROR,
                          this->get_errstr("failed to synchronize device", cuda_rc).c_str());
    }

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragonGPU_cuda::memset(void* addr, int val, size_t num_bytes)
{
    auto cuda_rc = fn_cudaMemset(addr, val, num_bytes);
    if (cuda_rc != cudaSuccess) {
        append_err_return(DRAGON_GPU_ERROR, this->get_errstr("failed to set memory", cuda_rc).c_str());
    }

    cuda_rc = fn_cudaDeviceSynchronize();
    if (cuda_rc != cudaSuccess) {
        append_err_return(DRAGON_GPU_ERROR,
                          this->get_errstr("failed to synchronize device", cuda_rc).c_str());
    }

    no_err_return(DRAGON_SUCCESS);
}

std::string
dragonGPU_cuda::get_errstr(const char* event, int cuda_rc)
{
    auto errstr = fn_cudaGetErrorString((cudaError_t)cuda_rc);

    auto log_str = std::string(event) + std::string(": rc=") + std::to_string(cuda_rc) +
                   (errstr ? std::string(", ") + std::string(errstr) : std::string(""));

    if (dragon_gpu_debug) {
        fprintf(dragon_gpu_log, "%s\n", log_str.c_str());
        fflush(dragon_gpu_log);
    }

    return log_str;
}

dragonError_t
dragonGPU_cuda::host_register(void* addr, size_t size)
{
    auto cuda_rc = fn_cudaHostRegister(addr, size, cudaHostRegisterPortable);
    if (cuda_rc != cudaSuccess) {
        append_err_return(DRAGON_GPU_ERROR,
                          this->get_errstr("failed to register host memory", cuda_rc).c_str());
    }
    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragonGPU_cuda::host_unregister(void* addr)
{
    auto cuda_rc = fn_cudaHostUnregister(addr);
    if (cuda_rc != cudaSuccess) {
        append_err_return(DRAGON_GPU_ERROR,
                          this->get_errstr("failed to unregister host memory", cuda_rc).c_str());
    }
    no_err_return(DRAGON_SUCCESS);
}

int
dragonGPU_cuda::get_device_count()
{
    return this->num_devices;
}

dragonError_t
dragonGPU_cuda::get_device_list(int** int_device_list, double** double_device_list, int* num_devices)
{
    *int_device_list = nullptr;
    *double_device_list = nullptr;

    if (this->num_devices == 0) {
        append_err_return(DRAGON_GPU_ERROR, "no CUDA devices available");
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
dragonGPU_cuda::uuid_hex_to_str(char* uuid_hex, char** uuid_str)
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
dragonGPU_cuda::get_uuid(int device_id, char** uuid)
{

    if (uuid == nullptr) {
        err_return(DRAGON_INVALID_ARGUMENT, "uuid output pointer must be non-null");
    }

    cudaDeviceProp prop;
    auto cuda_rc = fn_cudaGetDeviceProperties(&prop, device_id);
    if (cuda_rc != cudaSuccess)
        append_err_return(DRAGON_GPU_ERROR,
                          this->get_errstr("failed to query device properties", cuda_rc).c_str());

    dragonError_t derr = this->uuid_hex_to_str(prop.uuid.bytes, uuid);
    if (derr != DRAGON_SUCCESS) {
        append_err_return(derr, "failed to convert UUID to string");
    }

    no_err_return(DRAGON_SUCCESS);
}

#endif // HAVE_CUDA_INCLUDE
