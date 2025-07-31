#ifdef HAVE_HIP_INCLUDE

#include "hip.hpp"

// init hip backend

hipError_t (*fn_hipMalloc)(void **addr, size_t size);
hipError_t (*fn_hipFree)(void *addr);
hipError_t (*fn_hipMemcpy)(void* dst_addr, const void* src_addr, size_t size, hipMemcpyKind kind);
hipError_t (*fn_hipMemset)(void *addr, int val, size_t num_bytes);
hipError_t (*fn_hipSetDeviceFlags)(unsigned int flags);
hipError_t (*fn_hipDeviceSynchronize)(void);
hipError_t (*fn_hipIpcGetMemHandle)(hipIpcMemHandle_t *ipc_handle, void *addr);
hipError_t (*fn_hipIpcOpenMemHandle)(void **addr, hipIpcMemHandle_t ipc_handle, unsigned int flags);
hipError_t (*fn_hipIpcCloseMemHandle)(void *addr);
const char* (*fn_hipGetErrorString)(hipError_t hip_rc);

void *
dragon_gpu_open_hip_lib()
{
    return dlopen(dragonGPU_hip::libname, RTLD_LAZY | RTLD_GLOBAL);
}

dragonError_t
dragon_gpu_resolve_hip_symbols(void *libhandle)
{
    fn_hipMalloc = (hipError_t (*)(void **, size_t)) dlsym(libhandle, "hipMalloc");
    assert(fn_hipMalloc != nullptr);

    fn_hipFree = (hipError_t (*)(void *)) dlsym(libhandle, "hipFree");
    assert(fn_hipFree != nullptr);

    fn_hipMemcpy = (hipError_t (*)(void *, const void *, size_t, hipMemcpyKind)) dlsym(libhandle, "hipMalloc");
    assert(fn_hipMemcpy != nullptr);

    fn_hipMemset = (hipError_t (*)(void *, int, size_t)) dlsym(libhandle, "hipMemset");
    assert(fn_hipMemset != nullptr);

    fn_hipSetDeviceFlags = (hipError_t (*)(unsigned int)) dlsym(libhandle, "hipSetDeviceFlags");
    assert(fn_hipSetDeviceFlags != nullptr);

    fn_hipDeviceSynchronize = (hipError_t (*)(void)) dlsym(libhandle, "hipDeviceSynchronize");
    assert(fn_hipDeviceSynchronize != nullptr);

    fn_hipIpcGetMemHandle = (hipError_t (*)(hipIpcMemHandle_t *, void *)) dlsym(libhandle, "hipIpcGetMemHandle");
    assert(fn_hipIpcGetMemHandle != nullptr);

    fn_hipIpcOpenMemHandle = (hipError_t (*)(void **, hipIpcMemHandle_t, unsigned int)) dlsym(libhandle, "hipIpcOpenMemHandle");
    assert(fn_hipIpcOpenMemHandle != nullptr);

    fn_hipIpcCloseMemHandle = (hipError_t (*)(void *)) dlsym(libhandle, "hipIpcCloseMemHandle");
    assert(fn_hipIpcCloseMemHandle != nullptr);

    fn_hipGetErrorString = (const char* (*)(hipError_t)) dlsym(libhandle, "hipGetErrorString");
    assert(fn_hipGetErrorString != nullptr);

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_gpu_setup_hip(void *libhandle, dragonGPUHandle_t *gpuh)
{
    dragon_gpu_resolve_hip_symbols(libhandle);

    try {
        gpuh->dgpu = std::make_shared<dragonGPU_hip>();
    } catch (std::exception& e) {
        append_err_return(DRAGON_FAILURE, e.what());
    }

    no_err_return(DRAGON_SUCCESS);
}

// member function definitions

dragonGPU_hip::dragonGPU_hip()
{
    this->backend_type = DRAGON_GPU_BACKEND_CUDA;

    auto flags = hipDeviceScheduleBlockingSync;

    auto hip_rc = fn_hipSetDeviceFlags(flags);
    if (hip_rc != hipSuccess) {
        auto errstr = this->get_errstr("failed to set device flags", hip_rc);
        throw std::runtime_error(errstr.c_str());
    }
}

dragonError_t
dragonGPU_hip::mem_alloc(void **addr, size_t size)
{
    auto hip_rc = fn_hipMalloc(addr, size);
    if (hip_rc != hipSuccess) {
        append_err_return(DRAGON_FAILURE, this->get_errstr("failed to allocate device memory", hip_rc).c_str());
    }

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragonGPU_hip::mem_free(void *addr)
{
    auto hip_rc = fn_hipFree(addr);
    if (hip_rc != hipSuccess) {
        append_err_return(DRAGON_FAILURE, this->get_errstr("failed to free device memory", hip_rc).c_str());
    }

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragonGPU_hip::get_ipc_handle(void *addr, std::vector<uint8_t>& ipc_handle_out)
{
    hipIpcMemHandle_t ipc_handle;

    auto hip_rc = fn_hipIpcGetMemHandle(&ipc_handle, addr);
    if (hip_rc != hipSuccess) {
        append_err_return(DRAGON_FAILURE, this->get_errstr("failed to get IPC handle", hip_rc).c_str());
    }

    ipc_handle_out.resize(sizeof(hipIpcMemHandle_t));
    memcpy(&ipc_handle_out[0], &ipc_handle, ipc_handle_out.size());

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragonGPU_hip::free_ipc_handle(std::vector<uint8_t>& ipc_handle)
{
    // this is a no-op for hip
    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragonGPU_hip::attach(std::vector<uint8_t>& ipc_handle_in, void **addr)
{
    hipIpcMemHandle_t ipc_handle;
    memcpy(&ipc_handle, &ipc_handle_in[0], sizeof(hipIpcMemHandle_t));

    auto flags = hipIpcMemLazyEnablePeerAccess;

    auto hip_rc = fn_hipIpcOpenMemHandle(addr, ipc_handle, flags);
    if (hip_rc != hipSuccess) {
        append_err_return(DRAGON_FAILURE, this->get_errstr("failed to open IPC handle", hip_rc).c_str());
    }

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragonGPU_hip::detach(void *addr)
{
    auto hip_rc = fn_hipIpcCloseMemHandle(addr);
    if (hip_rc != hipSuccess) {
        append_err_return(DRAGON_FAILURE, this->get_errstr("failed to close IPC handle", hip_rc).c_str());
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
dragonGPU_hip::copy(void *dst_addr, const void *src_addr, size_t size, dragonGPUMemcpyType_t memcpy_type)
{
    auto hip_rc = fn_hipMemcpy(dst_addr, src_addr, size, dragon_to_hip_memcpy_kind(memcpy_type));
    if (hip_rc != hipSuccess) {
        append_err_return(DRAGON_FAILURE, this->get_errstr("failed to copy memory", hip_rc).c_str());
    }

    hip_rc = fn_hipDeviceSynchronize();
    if (hip_rc != hipSuccess) {
        append_err_return(DRAGON_FAILURE, this->get_errstr("failed to synchronize device", hip_rc).c_str());
    }

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragonGPU_hip::memset(void *addr, int val, size_t num_bytes)
{
    auto hip_rc = fn_hipMemset(addr, val, num_bytes);
    if (hip_rc != hipSuccess) {
        append_err_return(DRAGON_FAILURE, this->get_errstr("failed to allocate device memory", hip_rc).c_str());
    }

    hip_rc = fn_hipDeviceSynchronize();
    if (hip_rc != hipSuccess) {
        append_err_return(DRAGON_FAILURE, this->get_errstr("failed to synchronize device", hip_rc).c_str());
    }

    no_err_return(DRAGON_SUCCESS);
}

std::string
dragonGPU_hip::get_errstr(const char *event, int hip_rc)
{
    auto errstr = fn_hipGetErrorString((hipError_t) hip_rc);

    auto log_str = 
          std::string(event)
        + std::string(": rc=") + std::to_string(hip_rc)
        + (errstr ? std::string(", ") + std::string(errstr) : std::string(""));

    if (dragon_gpu_debug) {
        fprintf(dragon_gpu_log, "%s", log_str.c_str());
        fflush(dragon_gpu_log);
    }

    return log_str;
}

#endif // HAVE_HIP_INCLUDE

