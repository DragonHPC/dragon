from dragon.managed_memory import (
    MemoryPool,
    MemoryAlloc,
)
from dragon.infrastructure.policy import Policy
from dragon.native.process_group import ProcessGroup
from dragon.native.process import ProcessTemplate, Process
from dragon.native.queue import Queue
from dragon.native.event import Event
from dragon.native.machine import current
from dragon.telemetry import Telemetry

import cupy as cp
import numpy as np
import queue
import os
import time

from abc import ABC, abstractmethod


class DataMover(ABC):
    """Abstract base class for data movers. Subclasses should implement the _move_data method, the reconstruct method, and the construct_input_output_queues method."""

    def __init__(
        self,
        stop_event: Event,
        device_pool: MemoryPool,
    ):
        self.device_pool = device_pool
        self.stop_event = stop_event
        self.input_queue = None
        self.output_queue = None
        self.construct_input_output_queues()

    def run(self):
        """Main loop for the data mover process"""
        while not self.stop_event.is_set():
            try:
                data = self.input_queue.get(timeout=1)
                moved_repr = self._move_data(data)
                self.output_queue.put(moved_repr)
            except (TimeoutError, queue.Empty):
                continue

    def get_input_output_queues(self):
        """Returns the input and output queues"""
        return self.input_queue, self.output_queue

    @abstractmethod
    def _move_data(self, host_data):
        """Moves data from host to device. Should be implemented by subclasses. Takes whatever is returned by the get method of the input queue and returns whatever should be put into the output queue."""
        pass

    @staticmethod
    @abstractmethod
    def reconstruct(self, device_ser_mem_desc, metadata):
        """Reconstructs the data from the serialized memory descriptor on the GPU. Should be implemented by subclasses. Takes the serialized memory descriptor and any metadata needed to reconstruct the data (e.g., shape, dtype) and returns the reconstructed object."""
        pass

    @abstractmethod
    def construct_input_output_queues(self):
        """Constructs the input and output queues. Should be implemented by subclasses."""
        pass

    @property
    def name(self):
        return self.__class__.__name__

    @staticmethod
    def free_alloc(ser_mem_desc):
        """Frees the memory allocation on the GPU given the serialized memory descriptor. This should be called after the CuPy array is no longer needed."""
        mem_alloc = MemoryAlloc.attach(ser_mem_desc)
        mem_alloc.free()


class NumPyInputQueue(Queue):
    """Input queue for NumPyDataMover. Expects numpy arrays to be put into the queue. Puts a tuple of (metadata, numpy array) into the queue, where metadata is a tuple of (shape, dtype)."""

    def put(self, item, *args, **kwargs):
        if not isinstance(item, np.ndarray):
            raise TypeError("Only numpy arrays are supported for putting into a NumPyDataMover Queue")
        shape = item.shape
        dtype = item.dtype
        metadata = (shape, dtype)
        super().put((metadata, item), *args, **kwargs)


class OutputQueue(Queue):
    """Output queue for DataMovers. Expects tuples of (metadata, serialized memory descriptor) to be put into the queue. The get method reconstructs the CuPy or NumPy array using the reconstruct method defined as part of the data mover class.

    :param reconstruct: The reconstruct method from the data mover class.
    """

    def __init__(self, reconstruct, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.reconstruct = reconstruct

    def __getstate__(self):
        state = super().__getstate__()
        return state + (self.reconstruct,)

    def __setstate__(self, state):
        self.reconstruct = state[-1]
        old_state = state[:-1]
        super().__setstate__(old_state)

    def get(self, *args, **kwargs):
        metadata, ser_mem_desc = super().get(*args, **kwargs)
        return self.reconstruct(ser_mem_desc, metadata)


class NumPyDataMover(DataMover):
    """Data mover for moving numpy arrays from host to GPU and reconstructing them as CuPy arrays on the GPU.

    :param pool_size: The size of the host memory pool to use for staging the numpy arrays before copying to the GPU.
    """

    def __init__(self, pool_size, *args, **kwargs):

        uid = 1044 + os.getpid()  # what should this be in general?
        self._mpool = MemoryPool(pool_size, "numpy_data_mover_host_mpool_" + str(os.getpid()), uid)
        super().__init__(*args, **kwargs)

    def _move_data(self, data):
        """Moves data from host to device. Expects data to be a tuple of (metadata, numpy array), where metadata is a tuple of (shape, dtype). Returns a tuple of (metadata, GPU serialized memory descriptor)."""

        metadata, host_data = data
        host_mem_alloc = self._mpool.alloc_blocking(host_data.nbytes)
        host_mem_view = host_mem_alloc.get_memview()
        host_mem_view[:] = memoryview(host_data).tobytes()
        # this does the actual host->device copy. this will block until a gpu memory buffer is free since timeout is set to none.
        device_mem_alloc = host_mem_alloc.copy(self.device_pool)

        device_ser_mem_desc = device_mem_alloc.serialize()
        host_mem_alloc.free()  # free the host memory allocation

        return (metadata, device_ser_mem_desc)

    @staticmethod
    def reconstruct(device_ser_mem_desc, metadata):
        """Reconstructs the CuPy array from the serialized memory descriptor on the GPU. Expects metadata to be a tuple of (shape, dtype). Returns the reconstructed CuPy array and the serialized memory descriptor for freeing later."""

        shape, np_dtype = metadata
        device_mem_alloc = MemoryAlloc.attach(device_ser_mem_desc)
        # need to get device ptr
        intptr = device_mem_alloc.get_int_ptr()
        # from intptr_t this should reconstruct the cupy array object
        mem = cp.cuda.UnownedMemory(ptr=intptr, owner=None, size=device_mem_alloc.size)
        memptr = cp.cuda.MemoryPointer(mem, 0)
        cupy_array = cp.ndarray(shape=shape, dtype=cp.dtype(np_dtype), memptr=memptr)

        return cupy_array, device_ser_mem_desc

    @staticmethod
    def free_alloc(ser_mem_desc):
        """Frees the memory allocation on the GPU given the serialized memory descriptor. This should be called after the CuPy array is no longer needed."""
        mem_alloc = MemoryAlloc.attach(ser_mem_desc)
        mem_alloc.free()

    def construct_input_output_queues(self):
        """Constructs the special input and output queues."""
        self.input_queue = NumPyInputQueue()
        self.output_queue = OutputQueue(self.reconstruct)

    def cleanup(self):
        """Cleans up the host memory pool that is used to stage the data for memcpying to the GPU."""
        self._mpool.destroy()


class NumPyOptimizedInputQueue(Queue):
    """Input queue for NumPyOptimizedDataMover. Expects numpy arrays to be put into the queue. Allocates memory from a memory pool and copies the numpy array into that memory. Puts a tuple of (metadata, serialized memory descriptor) into the queue, where metadata is a tuple of (shape, dtype).

    :param mpool: The host memory pool to use for allocating memory for the numpy arrays. The descriptor to the allocation is what is put into the queue.
    """

    def __init__(self, mpool, *args, **kwargs):
        self._mpool = mpool
        super().__init__(*args, **kwargs)

    def __getstate__(self):
        state = super().__getstate__()
        new_state = state + (self._mpool.serialize(),)
        return new_state

    def __setstate__(self, state):
        mpool = MemoryPool.attach(state[-1])
        self._mpool = mpool
        old_state = state[:-1]
        super().__setstate__(old_state)

    def put(self, item, *args, **kwargs):
        if not isinstance(item, np.ndarray):
            raise TypeError("Only numpy arrays are supported for putting into a CuPyDataMover Queue")
        shape = item.shape
        dtype = item.dtype
        metadata = (shape, dtype)
        host_mem_alloc = self._mpool.alloc_blocking(item.nbytes)
        host_mem_view = host_mem_alloc.get_memview()
        host_mem_view[:] = memoryview(item).tobytes()
        ser_host_mem_desc = host_mem_alloc.serialize()
        super().put((metadata, ser_host_mem_desc), *args, **kwargs)


class NumPyOptimizedDataMover(NumPyDataMover):
    """Data mover for moving numpy arrays from host to GPU and reconstructing them as CuPy arrays on the GPU. This version uses a pre-allocated memory pool on the host to stage the data before copying to the GPU. Compared to CuPyDataMover, this version avoids an extra memcpy onto the process heap."""

    def _move_data(self, data):
        """Moves data from host to device. Expects data to be a tuple of (metadata, serialized host memory descriptor), where metadata is a tuple of (shape, dtype). Returns a tuple of (metadata, GPU serialized memory descriptor)."""
        metadata, ser_host_mem_desc = data
        host_mem_alloc = MemoryAlloc.attach(ser_host_mem_desc)
        # this does the actual host->device copy. this will block until a gpu memory buffer is free since timeout is set to none.
        device_mem_alloc = host_mem_alloc.copy(self.device_pool)
        device_ser_mem_desc = device_mem_alloc.serialize()
        host_mem_alloc.free()  # free the host memory allocation

        return (metadata, device_ser_mem_desc)

    def construct_input_output_queues(self):
        """Constructs the special input and output queues."""
        super().construct_input_output_queues()
        self.input_queue = NumPyOptimizedInputQueue(self._mpool)


class CuPyOptimizedDataMover(DataMover):
    """Data mover for moving arrays from GPU to host and reconstructing them as numpy arrays on the host. This version uses relies on the data residing in the buffer that it was originally allocated in. This works for operations like in-place FFTs on complex data-types. Compared to CuPyDataMover, this version avoids an extra memcpy by the kernel process."""

    def __init__(self, pool_size, *args, **kwargs):
        uid = 1045 + os.getpid()
        self._mpool = MemoryPool(pool_size, "cupy_optimized_offload_data_mover_host_mpool_" + str(os.getpid()), uid)
        super().__init__(*args, **kwargs)

    def _move_data(self, data):
        """Moves data from GPU to host. Expects data to be a tuple of (metadata, numpy array), where metadata is a tuple of (shape, dtype). Returns a tuple of (metadata, GPU serialized memory descriptor)."""

        metadata, device_ser_mem_desc = data
        device_mem_alloc = MemoryAlloc.attach(device_ser_mem_desc)
        host_mem_alloc = device_mem_alloc.copy(self._mpool)

        host_ser_mem_desc = host_mem_alloc.serialize()
        device_mem_alloc.free()  # free the host memory allocation

        return (metadata, host_ser_mem_desc)

    @staticmethod
    def reconstruct(host_ser_mem_desc, metadata):
        """Reconstructs the NumPy array from the serialized memory descriptor on the CPU. Expects metadata to be a tuple of (shape, dtype). Returns the reconstructed NumPy array frees the associated memory allocation."""

        shape, dtype = metadata
        host_mem_alloc = MemoryAlloc.attach(host_ser_mem_desc)
        # need to get device ptr
        data_memview = host_mem_alloc.get_memview()
        # from intptr_t this should reconstruct the cupy array object
        numpy_memview_array = np.asarray(data_memview).view(dtype=dtype).reshape(shape)

        # Create a copy of the array
        numpy_array = numpy_memview_array.copy()

        # should have a copy so can now free the host mem alloc
        host_mem_alloc.free()

        return numpy_array

    def construct_input_output_queues(self):
        """Constructs the special input and output queues. A regular queue can be utilized as the serialized GPU memory descriptor is all that is given to the data mover."""
        self.input_queue = Queue()
        self.output_queue = OutputQueue(self.reconstruct)

    def cleanup(self):
        """Cleans up the host memory pool that is used to stage the data for memcpying from the GPU."""
        self._mpool.destroy()


class CuPyInputQueue(Queue):
    """Input queue for moving CuPy arrays from the GPU to the host."""

    def __init__(self, mpool, *args, **kwargs):
        self._mpool = mpool
        super().__init__(*args, **kwargs)

    def __getstate__(self):
        state = super().__getstate__()
        new_state = state + (self._mpool.serialize(),)
        return new_state

    def __setstate__(self, state):
        mpool = MemoryPool.attach(state[-1])
        self._mpool = mpool
        old_state = state[:-1]
        super().__setstate__(old_state)

    def put(self, item, *args, **kwargs):
        """Takes a CuPy array and copies into into the GPU backed managed memory pool to be copied off the device. Puts a tuple of (metadata, serialized memory descriptor) into the queue, where metadata is a tuple of (shape, dtype).

        :param item: The CuPy array to copy.
        :type item: cp.ndarray
        :raises TypeError: If the item is not a CuPy array.
        """
        if not isinstance(item, cp.ndarray):
            raise TypeError("Only cupy arrays are supported for putting into a CuPyInputQueue")
        shape = item.shape
        dtype = item.dtype
        metadata = (shape, dtype)
        device_mem_alloc = self._mpool.alloc_blocking(item.nbytes)
        intptr = device_mem_alloc.get_int_ptr()
        cp.cuda.runtime.memcpy(intptr, item.data.ptr, item.nbytes, cp.cuda.runtime.memcpyDeviceToDevice)
        ser_device_mem_desc = device_mem_alloc.serialize()
        super().put((metadata, ser_device_mem_desc), *args, **kwargs)


class CuPyDataMover(DataMover):
    """Data mover for moving arrays GPU to host and reconstructing them as CuPy arrays on the host."""

    def __init__(self, pool_size, *args, **kwargs):
        uid = 1046 + os.getpid()
        self._mpool = MemoryPool(pool_size, "cupy_offload_data_mover_host_mpool_" + str(os.getpid()), uid)
        super().__init__(*args, **kwargs)

    def _move_data(self, data):
        """Moves data from the GPU to the host.

        :param data: The data to move. Expects a tuple of (metadata, serialized gpu memory descriptor), where metadata is a tuple of (shape, dtype).
        :type data: tuple
        :return: The metadata and serialized memory descriptor for the host.
        :rtype: tuple
        """
        metadata, device_ser_mem_desc = data
        device_mem_alloc = MemoryAlloc.attach(device_ser_mem_desc)
        host_mem_alloc = device_mem_alloc.copy(self._mpool)

        host_ser_mem_desc = host_mem_alloc.serialize()
        device_mem_alloc.free()  # free the host memory allocation

        return (metadata, host_ser_mem_desc)

    @staticmethod
    def reconstruct(host_ser_mem_desc, metadata):
        """Reconstructs the NumPy array from the serialized memory descriptor on the host. Expects metadata to be a tuple of (shape, dtype). Returns the reconstructed NumPy array and the serialized memory descriptor for freeing later.

        :param host_ser_mem_desc: The serialized memory descriptor for the host.
        :param metadata: The metadata for the array, including shape and dtype.
        :return: The reconstructed NumPy array.
        :rtype: tuple
        """

        shape, dtype = metadata
        host_mem_alloc = MemoryAlloc.attach(host_ser_mem_desc)
        # need to get device ptr
        data_memview = host_mem_alloc.get_memview()
        # from intptr_t this should reconstruct the cupy array object
        numpy_memview_array = np.asarray(data_memview).view(dtype=dtype).reshape(shape)

        # Create a copy of the array
        numpy_array = numpy_memview_array.copy()

        # should have a copy so can now free the host mem alloc
        host_mem_alloc.free()

        return numpy_array

    def construct_input_output_queues(self):
        """Constructs the special input and output queues."""
        self.input_queue = CuPyInputQueue(self.device_pool)
        self.output_queue = OutputQueue(self.reconstruct)

    def cleanup(self):
        """Cleans up the host memory pool that is used to stage the data for memcpying from the GPU."""
        self._mpool.destroy()


class DataMovers:
    """Manages multiple data mover processes. Initializes the data mover class, starts and stops the processes, and provides access to the input and output queues. The manager process initializes the data mover class and starts the specified number of worker processes. It also manages the lifecycle of the GPU-backed managed memory pools, which is especially important when using CUDA as the process that instantiates the memory pool must also destroy it.

    Example usage:

        .. highlight:: python
        .. code-block:: python

            import dragon
            import multiprocessing as mp
            from dragon.workflows.data_mover import (
                DataMovers,
                CuPyDataMover,
                NumPyDataMover,
            )
            import cupy as cp
            import numpy as np
            import time


            def cupy_user_kernel_noop(data):
                return data


            def cupy_general_kernel_proc(data_queue, stop_event, output_data_queue):
                kernel_stream = cp.cuda.Stream(non_blocking=True, ptds=True)
                with kernel_stream:
                    while not stop_event.is_set():
                        try:
                            x, serialized_descriptor = data_queue.get(timeout=1)
                        except (TimeoutError, queue.Empty):
                            continue
                        xp = cupy_user_kernel_noop(x)
                        kernel_stream.synchronize()
                        NumPyDataMover.free_alloc(serialized_descriptor)
                        output_data_queue.put(xp)
                        kernel_stream.synchronize()

            def producer(args):
                data_size, data_type = args
                data = np.random.rand(data_size).astype(data_type)
                producer_queue.put(data)


            def processed_consumer(stop_event, processed_data_queue, num_items):
                item_counter = 0
                while item_counter < num_items:
                    try:
                        data = processed_data_queue.get(timeout=1)
                        item_counter += 1
                    except (TimeoutError, queue.Empty):
                        continue
                stop_event.set()


            def main():

                num_movers = 2 # 2 on and 2 off for a total of 4
                num_producers = 4
                num_items = 100
                data_size = 1024 * 1024
                data_type = np.float32

                movers_on = DataMovers(
                    data_mover=NumPyDataMover,
                    data_mover_args={"pool_size": 1024**3},
                    num_workers=num_movers,
                )
                movers_off = DataMovers(
                    data_mover=CuPyDataMover,
                    data_mover_args={"pool_size": 1024**3},
                    num_workers=num_movers,
                )
                movers_on.start()
                movers_off.start()
                input_queue, output_descriptor_queue = movers_on.get_queues()
                input_descriptor_queue, output_queue = movers_off.get_queues()

                pool = mp.Pool(num_producers)

                kernel_proc = mp.Process(
                    target=cupy_general_kernel_proc,
                    args=(
                        output_descriptor_queue,
                        stop_event,
                        input_descriptor_queue,
                    ),
                )
                kernel_proc.start()

                processed_consumer_proc = mp.Process(
                    target=processed_consumer,
                    args=(
                        stop_event,
                        output_queue,
                        num_items,
                    ),
                )
                processed_consumer_proc.start()

                producer_args = (data_size, data_type)
                result = pool.map_async(producer, [producer_args] * num_items)
                processed_consumer_proc.join()
                total_time = end_time - start_time
                pool.close()
                pool.join()
                movers_on.stop()
                movers_off.stop()
                kernel_proc.join()



            if __name__ == "__main__":
                main()


    :param data_mover: The data mover class to use. Must be a subclass of DataMover.
    :type data_mover: DataMover
    :param data_mover_args: The arguments to pass to the data mover class.
    :type data_mover_args: dict
    :param num_workers: The number of worker processes to start. Ignored if policies is specified.
    :type num_workers: int
    :param device_pool_size: The size of the GPU memory pool to allocate for the data mover. If None, no GPU memory pool is allocated and the data mover will not have a dedicated GPU memory pool.
    :type device_pool_size: int or None
    :param manager_policy: The policy to use for the manager process. If None, the manager process will be placed on the current host.
    :type manager_policy: Policy or None
    :param policies: A list of policies to use for the worker processes. If specified, num_workers is ignored and num_workers_per_policy is used instead.
    :type policies: list of Policy or None
    :param num_workers_per_policy: The number of worker processes to start for each policy in policies. Ignored if policies is None.
    :type num_workers_per_policy: int
    :param tuning_collection_delay: If specified, the data mover will add pool utilization information every tuning_collection_delay seconds to the telemetry database. If None, no tuning information is printed.
    :type tuning_collection_delay: float or None
    """

    def __init__(
        self,
        data_mover: DataMover,
        data_mover_args: dict = {},
        num_workers: int = 1,
        device_pool_size: int = 1024 * 1024 * 1024,
        manager_policy: Policy = None,
        policies: list = None,
        num_workers_per_policy: int = 1,
        tuning_collection_delay: float = None,
    ):
        self._stop_event = Event()
        assert (policies is None) or (num_workers is None), "Cannot specify both policies and num_workers"
        if manager_policy is None:
            self._manager_policy = Policy(placement=Policy.Placement.HOST_NAME, host_name=current().hostname)
        else:
            self._manager_policy = manager_policy
            assert isinstance(self._manager_policy, Policy), "manager_policy must be a Policy"

        self._policies = policies
        self._num_workers_per_policy = num_workers_per_policy
        self._data_mover_class = data_mover
        self._data_mover_args = data_mover_args
        self._num_workers = num_workers
        self._device_pool_size = device_pool_size
        self._manager_queue = Queue()
        self._tuning_collection_delay = tuning_collection_delay
        self._manager_proc = Process(target=self._manager, policy=manager_policy)

    def _manager(self):
        uid = 1043 + os.getpid()  # what should this be in general?
        if self._device_pool_size is not None:
            self._device_pool = MemoryPool(
                self._device_pool_size,
                "data_movers_gpu_mpool_" + f"{self._data_mover_class.__name__}" + str(os.getpid()),
                uid,
                gpu_memory=True,
            )
        else:
            self._device_pool = None
        self._data_mover = self._data_mover_class(
            stop_event=self._stop_event, device_pool=self._device_pool, **self._data_mover_args
        )
        self._process_group = ProcessGroup()
        if self._policies is not None:
            assert self._num_workers is None, "Cannot specify both num_workers and policies"
            for policy in self._policies:
                proc_template = ProcessTemplate(
                    target=self._data_mover.run,
                    policy=policy,
                )
                self._process_group.add_process(
                    nproc=self._num_workers_per_policy,
                    template=proc_template,
                )
        else:
            proc_template = ProcessTemplate(
                target=self._data_mover.run,
                policy=Policy(placement=Policy.Placement.HOST_NAME, host_name=current().hostname),
            )
            self._process_group.add_process(
                nproc=self._num_workers,
                template=proc_template,
            )
        self._process_group.init()
        # this needs to be moved out of this process so it can be communicated back to the parent
        self.input_queue, self.output_queue = self._data_mover.get_input_output_queues()
        self._manager_queue.put((self.input_queue, self.output_queue))
        # Wait until stop event is set
        self._process_group.start()
        # tuning prints. will switch to use telemetry later
        if self._tuning_collection_delay is not None:
            dt = Telemetry()
            device_list = os.getenv("CUDA_VISIBLE_DEVICES")
            device_list = device_list.split(",")
            device = device_list[0]
            while not self._stop_event.is_set():
                time.sleep(self._tuning_collection_delay)
                if self._device_pool is not None:
                    dt.add_data(
                        f"{self._data_mover.name}_shared_device_pool_utilization",
                        self._device_pool.utilization,
                        tagk="gpu",
                        tagv=device,
                    )
                dt.add_data(
                    f"{self._data_mover.name}_host_mpool", self._data_mover._mpool.utilization, tagk="gpu", tagv=device
                )
        else:
            self._stop_event.wait()

        # Clean up
        self._process_group.join()
        self._process_group.close()
        self._data_mover.cleanup()
        if self._device_pool is not None:
            self._device_pool.destroy()

    def get_queues(self):
        """Returns the input and output queues."""
        if not self._manager_proc.is_alive:
            raise RuntimeError("Data movers are not running. Please start them before getting the queues.")
        return self._manager_queue.get()

    def start(self):
        """Start the data movers."""
        self._manager_proc.start()

    def stop(self):
        """Stop the data movers and cleans up memory pools."""
        self._stop_event.set()
        self._manager_proc.join()
