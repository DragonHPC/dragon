from dragon.dtypes_inc cimport *
from cpython.long cimport PyLong_AsVoidPtr
from libc.stdlib cimport free
from libc.stdint cimport uintptr_t
import enum

class AccVendor(enum.IntEnum):
    NVIDIA = enum.auto()
    AMD = enum.auto()
    INTEL = enum.auto()
    UNKNOWN = enum.auto()

class AccEnvStr:
    NVIDIA = "CUDA_VISIBLE_DEVICES"
    AMD = "ROCR_VISIBLE_DEVICES"
    HIP = "HIP_VISIBLE_DEVICES"
    INTEL = "ZE_AFFINITY_MASK"


class Accelerator:

    def __init__(self):

        cdef:
            dragonError_t derr
            dragonGPUBackend_t backend
            object dragon_gpu_handle
            int num_devices
            int devID

        # Initialize some things:
        self._num_devices = 0
        self._device_list = []
        self._device_uuids = []
        self._uuid_by_device = {}
        self._vendor = AccVendor.UNKNOWN

        # Figure out if we have anything going on
        derr, dragon_gpu_handle = self._get_gpu_handle()
        if derr != DRAGON_SUCCESS:
            raise RuntimeError("Failed to get GPU handle")

        # Make sure we've initialized the GPU system
        derr, dragon_gpu_handle = self._setup_gpu(dragon_gpu_handle)
        if derr != DRAGON_SUCCESS:
            # Confirm we weren't able to detect a backend
            derr, backend = self._get_backend_type(dragon_gpu_handle)
            if derr != DRAGON_SUCCESS:
                raise RuntimeError("Failed to get GPU backend type")
            if backend == DRAGON_GPU_BACKEND_UNDETECTED:
                return

        # Get the vendor
        derr, backend = self._get_backend_type(dragon_gpu_handle)
        if derr != DRAGON_SUCCESS:
            raise RuntimeError("Failed to get GPU backend type")
        if backend == DRAGON_GPU_BACKEND_CUDA:
            self._vendor = AccVendor.NVIDIA
        elif backend == DRAGON_GPU_BACKEND_HIP:
            self._vendor = AccVendor.AMD
        elif backend == DRAGON_GPU_BACKEND_ZE:
            self._vendor = AccVendor.INTEL

        # Get device count and UUIDs
        self._num_devices = self._get_device_count(dragon_gpu_handle)
        for devID in range(self._num_devices):
            derr, uuid_hex = self._get_uuid(dragon_gpu_handle, devID)
            if derr != DRAGON_SUCCESS:
                raise RuntimeError(f"Failed to query uuid of GPU device {devID}")
            self._device_uuids.append((<bytes>uuid_hex).decode("utf-8"))
            self._uuid_by_device[devID] = self._device_uuids[-1]

        derr, self._device_list = self._get_device_list(dragon_gpu_handle)
        if derr != DRAGON_SUCCESS:
            raise RuntimeError("Failed to get GPU device list")


    def get_device_count(self) -> int:
        return self._num_devices

    def get_device_uuid(self, device_id: int) -> str:
        if self._num_devices == 0:
            return ""
        if device_id < 0 or device_id >= self._num_devices:
            raise ValueError(f"Invalid device ID {device_id}. Must be between 0 and {self._num_devices - 1}")
        return self._uuid_by_device[device_id]

    def get_vendor(self) -> AccVendor:
        return self._vendor

    def get_vendor_env_str(self) -> str:
        if self._vendor == AccVendor.NVIDIA:
            return AccEnvStr.NVIDIA
        elif self._vendor == AccVendor.AMD:
            return AccEnvStr.AMD
        elif self._vendor == AccVendor.INTEL:
            return AccEnvStr.INTEL
        else:
            return None

    def get_device_list(self) -> list:
        return self._device_list

    def _get_gpu_handle(self):
        cdef void *dragon_gpu_handle
        cdef dragonError_t derr
        derr = dragon_gpu_get_handle(&dragon_gpu_handle)
        if derr != DRAGON_SUCCESS:
            return derr, None
        return derr, <uintptr_t>dragon_gpu_handle

    def _get_device_list(self, handle):
        cdef:
            dragonError_t derr
            int *int_device_list = NULL
            double *double_device_list = NULL
            int num_devices = 0
        cdef void *dragon_gpu_handle = PyLong_AsVoidPtr(handle)
        derr = dragon_gpu_get_device_list(dragon_gpu_handle, &int_device_list, &double_device_list, &num_devices)
        if derr != DRAGON_SUCCESS:
            return derr, []
        if int_device_list != NULL:
            py_device_list = [int_device_list[i] for i in range(num_devices)]
            free(int_device_list)
        else:
            py_device_list = [double_device_list[i] for i in range(num_devices)]
            free(double_device_list)
        return derr, py_device_list

    def _setup_gpu(self, handle):
        cdef dragonError_t derr
        cdef void *dragon_gpu_handle = PyLong_AsVoidPtr(handle)
        derr = dragon_gpu_setup(&dragon_gpu_handle)
        if derr != DRAGON_SUCCESS:
            return derr, <uintptr_t>dragon_gpu_handle
        return derr, <uintptr_t>dragon_gpu_handle

    def _get_backend_type(self, handle):
        cdef dragonError_t derr
        cdef dragonGPUBackend_t backend
        cdef void *dragon_gpu_handle = PyLong_AsVoidPtr(handle)
        derr = dragon_gpu_get_backend_type(dragon_gpu_handle, &backend)
        if derr != DRAGON_SUCCESS:
            return derr, None
        return derr, backend

    def _get_device_count(self, handle):
        cdef void *dragon_gpu_handle = PyLong_AsVoidPtr(handle)
        return dragon_gpu_get_device_count(dragon_gpu_handle)

    def _get_uuid(self, handle, int devID):
        cdef dragonError_t derr
        cdef char *uuid_hex
        cdef void *dragon_gpu_handle = PyLong_AsVoidPtr(handle)
        derr = dragon_gpu_get_uuid(dragon_gpu_handle, devID, &uuid_hex)
        if derr != DRAGON_SUCCESS:
            return derr, None
        uuid_bytes = (<bytes>uuid_hex)
        free(uuid_hex)
        return derr, uuid_bytes

