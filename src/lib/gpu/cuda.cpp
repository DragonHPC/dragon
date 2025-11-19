#ifdef HAVE_CUDA_INCLUDE

#include "cuda.hpp"


// init cuda backend

cudaError_t (*fn_cudaMalloc)(void **addr, size_t size);
cudaError_t (*fn_cudaFree)(void *addr);
cudaError_t (*fn_cudaMemcpy)(void* dst_addr, const void* src_addr, size_t size, cudaMemcpyKind kind);
cudaError_t (*fn_cudaMemset)(void *addr, int val, size_t num_bytes);
cudaError_t (*fn_cudaSetDeviceFlags)(unsigned int flags);
cudaError_t (*fn_cudaSetDevice)(int deviceID);
cudaError_t (*fn_cudaDeviceSynchronize)(void);
cudaError_t (*fn_cudaIpcGetMemHandle)(cudaIpcMemHandle_t *ipc_handle, void *addr);
cudaError_t (*fn_cudaIpcOpenMemHandle)(void **addr, cudaIpcMemHandle_t ipc_handle, unsigned int flags);
cudaError_t (*fn_cudaIpcCloseMemHandle)(void *addr);
const char* (*fn_cudaGetErrorString)(cudaError_t cuda_rc);

void *
dragon_gpu_open_cuda_lib()
{
    FILE* pipe = popen("dragon-config --get='cuda_runtime_lib'", "r");
    if (!pipe) {
        std::cerr << "Error opening pipe." << std::endl;
    }
    char buffer[128];
    std::string result = "";
    while (fgets(buffer, sizeof(buffer), pipe) != nullptr) {
        result += buffer;
    }
    pclose(pipe);

    std::string lib_path;
    size_t first_quote = result.find('\'');
    size_t last_quote = result.find_last_of('\'');
    if (first_quote != std::string::npos && last_quote != std::string::npos) {
        // Extract the substring between the quotes
        std::string parsed_string = result.substr(first_quote + 1, last_quote - first_quote - 1);

        // Print the result
        lib_path = parsed_string + '/' + dragonGPU_cuda::libname;
    }
    else
        lib_path = dragonGPU_cuda::libname;

    return dlopen(lib_path.c_str(), RTLD_LAZY | RTLD_GLOBAL);
}

dragonError_t
dragon_gpu_resolve_cuda_symbols(void *libhandle)
{
    fn_cudaMalloc = (cudaError_t (*)(void **, size_t)) dlsym(libhandle, "cudaMalloc");
    assert(fn_cudaMalloc != nullptr);

    fn_cudaFree = (cudaError_t (*)(void *)) dlsym(libhandle, "cudaFree");
    assert(fn_cudaFree != nullptr);

    fn_cudaMemcpy = (cudaError_t (*)(void *, const void *, size_t, cudaMemcpyKind)) dlsym(libhandle, "cudaMemcpy");
    assert(fn_cudaMemcpy != nullptr);

    fn_cudaMemset = (cudaError_t (*)(void *, int, size_t)) dlsym(libhandle, "cudaMemset");
    assert(fn_cudaMemset != nullptr);

    fn_cudaSetDeviceFlags = (cudaError_t (*)(unsigned int)) dlsym(libhandle, "cudaSetDeviceFlags");
    assert(fn_cudaSetDeviceFlags != nullptr);

    fn_cudaSetDevice = (cudaError_t (*)(int)) dlsym(libhandle, "cudaSetDevice");
    assert(fn_cudaSetDevice != nullptr);

    fn_cudaDeviceSynchronize = (cudaError_t (*)(void)) dlsym(libhandle, "cudaDeviceSynchronize");
    assert(fn_cudaDeviceSynchronize != nullptr);

    fn_cudaIpcGetMemHandle = (cudaError_t (*)(cudaIpcMemHandle_t *, void *)) dlsym(libhandle, "cudaIpcGetMemHandle");
    assert(fn_cudaIpcGetMemHandle != nullptr);

    fn_cudaIpcOpenMemHandle = (cudaError_t (*)(void **, cudaIpcMemHandle_t, unsigned int)) dlsym(libhandle, "cudaIpcOpenMemHandle");
    assert(fn_cudaIpcOpenMemHandle != nullptr);

    fn_cudaIpcCloseMemHandle = (cudaError_t (*)(void *)) dlsym(libhandle, "cudaIpcCloseMemHandle");
    assert(fn_cudaIpcCloseMemHandle != nullptr);

    fn_cudaGetErrorString = (const char* (*)(cudaError_t)) dlsym(libhandle, "cudaGetErrorString");
    assert(fn_cudaGetErrorString != nullptr);

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_gpu_setup_cuda(void *libhandle, int deviceID, void *dragon_gpu_handle)
{
    dragonGPUHandle_t *gpuh;
    gpuh = (dragonGPUHandle_t *)dragon_gpu_handle;
    dragon_gpu_resolve_cuda_symbols(libhandle);

    try {
        gpuh->dgpu = std::make_shared<dragonGPU_cuda>(deviceID);
    } catch (std::exception& e) {
        append_err_return(DRAGON_FAILURE, e.what());
    }

    no_err_return(DRAGON_SUCCESS);
}

// member function definitions

dragonGPU_cuda::dragonGPU_cuda(int deviceID)
{
    this->backend_type = DRAGON_GPU_BACKEND_CUDA;

    auto flags = cudaDeviceScheduleBlockingSync;

    auto cuda_rc = fn_cudaSetDevice(deviceID);
    if (cuda_rc != cudaSuccess) {
        auto errstr = this->get_errstr("failed to set device id", cuda_rc);
        throw std::runtime_error(errstr.c_str());
    }
    cuda_rc = fn_cudaSetDeviceFlags(flags);
    if (cuda_rc != cudaSuccess) {
        auto errstr = this->get_errstr("failed to set device flags", cuda_rc);
        throw std::runtime_error(errstr.c_str());
    }
}

dragonError_t
dragonGPU_cuda::mem_alloc(void **addr, size_t size)
{
    auto cuda_rc = fn_cudaMalloc(addr, size);
    if (cuda_rc != cudaSuccess) {
        append_err_return(DRAGON_FAILURE, this->get_errstr("failed to allocate device memory", cuda_rc).c_str());
    }

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragonGPU_cuda::mem_free(void *addr)
{
    auto cuda_rc = fn_cudaFree(addr);
    if (cuda_rc != cudaSuccess) {
        append_err_return(DRAGON_FAILURE, this->get_errstr("failed to free device memory", cuda_rc).c_str());
    }
    addr = nullptr;
    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragonGPU_cuda::get_ipc_handle(void *addr, void **ipc_handle_out)
{
    size_t ipc_handle_size = sizeof(cudaIpcMemHandle_t);
    *ipc_handle_out = malloc(ipc_handle_size);
    if (*ipc_handle_out == nullptr) {
        append_err_return(DRAGON_INTERNAL_MALLOC_FAIL, "failed to allocate IPC handle");
    }

    cudaIpcMemHandle_t *ipc_handle = static_cast<cudaIpcMemHandle_t *>(*ipc_handle_out);

    auto cuda_rc = fn_cudaIpcGetMemHandle(ipc_handle, addr);
    if (cuda_rc != cudaSuccess) {
        append_err_return(DRAGON_FAILURE, this->get_errstr("failed to get IPC handle", cuda_rc).c_str());
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
dragonGPU_cuda::free_ipc_handle(void **ipc_handle)
{
    // this is a no-op for cuda
    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragonGPU_cuda::attach(void* ipc_handle_in, void **addr)
{
    cudaIpcMemHandle_t ipc_handle;
    size_t ipc_handle_size = sizeof(cudaIpcMemHandle_t);
    memcpy(&ipc_handle, ipc_handle_in, ipc_handle_size);

    auto flags = cudaIpcMemLazyEnablePeerAccess;

    auto cuda_rc = fn_cudaIpcOpenMemHandle(addr, ipc_handle, flags);
    if (cuda_rc != cudaSuccess) {
        append_err_return(DRAGON_FAILURE, this->get_errstr("failed to open IPC handle", cuda_rc).c_str());
    }

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragonGPU_cuda::detach(void *addr)
{
    auto cuda_rc = fn_cudaIpcCloseMemHandle(addr);
    if (cuda_rc != cudaSuccess) {
        append_err_return(DRAGON_FAILURE, this->get_errstr("failed to close IPC handle", cuda_rc).c_str());
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
dragonGPU_cuda::copy(void *dst_addr, const void *src_addr, size_t size, dragonGPUMemcpyType_t memcpy_type)
{
    auto cuda_rc = fn_cudaMemcpy(dst_addr, src_addr, size, dragon_to_cuda_memcpy_kind(memcpy_type));
    if (cuda_rc != cudaSuccess) {
        append_err_return(DRAGON_FAILURE, this->get_errstr("failed to copy memory", cuda_rc).c_str());
    }

    cuda_rc = fn_cudaDeviceSynchronize();
    if (cuda_rc != cudaSuccess) {
        append_err_return(DRAGON_FAILURE, this->get_errstr("failed to synchronize device", cuda_rc).c_str());
    }

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragonGPU_cuda::memset(void *addr, int val, size_t num_bytes)
{
    auto cuda_rc = fn_cudaMemset(addr, val, num_bytes);
    if (cuda_rc != cudaSuccess) {
        append_err_return(DRAGON_FAILURE, this->get_errstr("failed to set memory", cuda_rc).c_str());
    }

    cuda_rc = fn_cudaDeviceSynchronize();
    if (cuda_rc != cudaSuccess) {
        append_err_return(DRAGON_FAILURE, this->get_errstr("failed to synchronize device", cuda_rc).c_str());
    }

    no_err_return(DRAGON_SUCCESS);
}

std::string
dragonGPU_cuda::get_errstr(const char *event, int cuda_rc)
{
    auto errstr = fn_cudaGetErrorString((cudaError_t) cuda_rc);

    auto log_str =
          std::string(event)
        + std::string(": rc=") + std::to_string(cuda_rc)
        + (errstr ? std::string(", ") + std::string(errstr) : std::string(""));

    if (dragon_gpu_debug) {
        fprintf(dragon_gpu_log, "%s\n", log_str.c_str());
        fflush(dragon_gpu_log);
    }

    return log_str;
}

#endif // HAVE_CUDA_INCLUDE

