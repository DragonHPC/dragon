#!/usr/bin/env python3
"""
Unit tests for Dragon GPU Data Mover classes.

These tests are designed to run on a single node with GPU support within
the Dragon runtime environment. They test the functionality of data movers
that transfer numpy arrays to GPU memory and reconstruct them as CuPy arrays.
"""

import unittest
import multiprocessing as mp
import numpy as np
import cupy as cp
import time
import tempfile
import os
import sys
import queue
from unittest.mock import Mock, patch, MagicMock

# Dragon imports
import dragon
from dragon.managed_memory import MemoryPool, MemoryAlloc
from dragon.infrastructure.policy import Policy
from dragon.native.process_group import ProcessGroup
from dragon.native.process import ProcessTemplate, Process
from dragon.native.queue import Queue
from dragon.native.event import Event
from dragon.native.machine import current

# Import the data mover classes
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../src"))
from dragon.workflows.data_mover import (
    DataMover,
    NumPyInputQueue,
    NumPyOptimizedInputQueue,
    CuPyInputQueue,
    OutputQueue,
    NumPyDataMover,
    NumPyOptimizedDataMover,
    CuPyDataMover,
    CuPyOptimizedDataMover,
    DataMovers,
)


def numpy_to_cupy_dtype(np_dtype):
    """
    Convert a numpy dtype to the corresponding cupy.dtype.
    """
    try:
        npd = np.dtype(np_dtype)
    except Exception as e:
        raise TypeError(f"cannot interpret {np_dtype!r} as a numpy dtype: {e}")

    try:
        return cp.dtype(npd)
    except Exception:
        # fallback for common numeric types
        kind = npd.kind
        itemsize = npd.itemsize
        fallback_map = {
            ("b", 1): cp.bool_,
            ("i", 1): cp.int8,
            ("i", 2): cp.int16,
            ("i", 4): cp.int32,
            ("i", 8): cp.int64,
            ("u", 1): cp.uint8,
            ("u", 2): cp.uint16,
            ("u", 4): cp.uint32,
            ("u", 8): cp.uint64,
            ("f", 2): getattr(cp, "float16"),
            ("f", 4): cp.float32,
            ("f", 8): cp.float64,
            ("c", 8): cp.complex64,
            ("c", 16): cp.complex128,
        }
        key = (kind, itemsize)
        if key in fallback_map:
            return cp.dtype(fallback_map[key])
        raise TypeError(f"NumPy dtype {npd!r} cannot be converted to a CuPy dtype")


class TestNumPyInputQueue(unittest.TestCase):
    """Test cases for NumPyInputQueue class."""

    def setUp(self):
        """Set up test fixtures."""
        self.queue = NumPyInputQueue()

    def tearDown(self):
        """Clean up after tests."""
        self.queue.close()

    def test_put_numpy_array(self):
        """Test putting a numpy array into the queue."""
        # Create test data
        data = np.random.rand(10, 10).astype(np.float32)

        # Put data into queue
        self.queue.put(data)

        # Retrieve data
        metadata, array = self.queue.get()
        shape, dtype = metadata

        # Verify metadata
        self.assertEqual(shape, data.shape)
        self.assertEqual(dtype, data.dtype)

        # Verify array data
        np.testing.assert_array_equal(array, data)

    def test_put_non_numpy_array_raises_error(self):
        """Test that putting non-numpy data raises TypeError."""
        with self.assertRaises(TypeError):
            self.queue.put([1, 2, 3])

        with self.assertRaises(TypeError):
            self.queue.put("not an array")

        with self.assertRaises(TypeError):
            self.queue.put(42)

    def test_put_different_dtypes(self):
        """Test putting arrays with different data types."""
        dtypes = [np.float32, np.float64, np.int32, np.int64, np.complex64]

        for dtype in dtypes:
            with self.subTest(dtype=dtype):
                data = np.random.rand(5, 5).astype(dtype)
                self.queue.put(data)
                metadata, array = self.queue.get()
                shape, retrieved_dtype = metadata
                self.assertEqual(shape, data.shape)
                self.assertEqual(retrieved_dtype, data.dtype)
                np.testing.assert_array_equal(array, data)
                self.queue.put(data)

                metadata, array = self.queue.get()
                shape, retrieved_dtype = metadata

                self.assertEqual(retrieved_dtype, dtype)
                np.testing.assert_array_equal(array, data)


class TestOutputQueue(unittest.TestCase):
    """Test cases for OutputQueue class."""

    def setUp(self):
        """Set up test fixtures."""
        self.reconstruct_func = Mock(return_value="reconstructed_data")
        self.queue = OutputQueue(self.reconstruct_func)

    def tearDown(self):
        """Clean up after tests."""
        self.queue.close()

    def test_get_calls_reconstruct(self):
        """Test that get method calls the reconstruct function."""
        metadata = (np.float32, (10, 10))
        ser_mem_desc = "mock_descriptor"

        # Put test data
        self.queue.put((metadata, ser_mem_desc))

        # Get data (should call reconstruct)
        result = self.queue.get()

        # Verify reconstruct was called with correct arguments
        self.reconstruct_func.assert_called_once_with(ser_mem_desc, metadata)
        self.assertEqual(result, "reconstructed_data")

    def test_serialization_preserves_reconstruct_function(self):
        """Test that serialization/deserialization preserves reconstruct function."""
        # Get state
        state = self.queue.__getstate__()

        # Create new queue and set state
        new_queue = OutputQueue(lambda x, y: None)
        new_queue.__setstate__(state)

        # Verify reconstruct function is preserved
        self.assertEqual(new_queue.reconstruct, self.reconstruct_func)


class TestNumPyDataMover(unittest.TestCase):
    """Test cases for NumPyDataMover class (host->GPU)."""

    def setUp(self):
        """Set up test fixtures."""
        # Create mock objects for testing
        self.mock_stop_event = Mock()
        self.mock_stop_event.is_set.return_value = False

        # Create a device memory pool for testing
        self.device_pool_size = 1024 * 1024  # 1MB for testing
        self.device_pool = MemoryPool(self.device_pool_size, "test_device_pool", 1045, gpu_memory=True)

        # Create data mover instance
        self.host_pool_size = 1024 * 1024  # 1MB for testing
        self.data_mover = NumPyDataMover(
            pool_size=self.host_pool_size, stop_event=self.mock_stop_event, device_pool=self.device_pool
        )

    def tearDown(self):
        """Clean up after tests."""
        self.data_mover.cleanup()
        self.device_pool.destroy()

    def test_initialization(self):
        """Test that NumPyDataMover initializes correctly."""
        self.assertIsNotNone(self.data_mover._mpool)
        self.assertIsNotNone(self.data_mover.input_queue)
        self.assertIsNotNone(self.data_mover.output_queue)
        self.assertIsInstance(self.data_mover.input_queue, NumPyInputQueue)
        self.assertIsInstance(self.data_mover.output_queue, OutputQueue)

    def test_move_data_small_array(self):
        """Test moving a small numpy array to GPU."""
        # Create test data
        data = np.random.rand(10, 10).astype(np.float32)
        metadata = (data.shape, data.dtype)
        input_data = (metadata, data)

        # Move data
        result_metadata, ser_mem_desc = self.data_mover._move_data(input_data)

        # Verify metadata is preserved
        self.assertEqual(result_metadata, metadata)
        self.assertIsNotNone(ser_mem_desc)

        # Clean up the device memory allocation
        device_mem_alloc = MemoryAlloc.attach(ser_mem_desc)
        device_mem_alloc.free()

    def test_reconstruct_static_method(self):
        """Test the static reconstruct method."""
        # Create test data and move it to device
        data = np.random.rand(5, 5).astype(np.float32)
        metadata = (data.shape, data.dtype)
        input_data = (metadata, data)

        # Move data to get serialized descriptor
        result_metadata, ser_mem_desc = self.data_mover._move_data(input_data)

        # Reconstruct the CuPy array
        cupy_array, returned_descriptor = NumPyDataMover.reconstruct(ser_mem_desc, metadata)

        # Verify the reconstructed array
        self.assertIsInstance(cupy_array, cp.ndarray)
        self.assertEqual(cupy_array.shape, data.shape)
        self.assertEqual(cupy_array.dtype, numpy_to_cupy_dtype(data.dtype))

        # Verify data content (convert back to CPU for comparison)
        np.testing.assert_array_almost_equal(cupy_array.get(), data)

        # Clean up
        NumPyDataMover.free_alloc(returned_descriptor)

    def test_free_alloc_static_method(self):
        """Test the static free_alloc method."""
        # Create and move test data
        data = np.random.rand(3, 3).astype(np.float32)
        metadata = (data.shape, data.dtype)
        input_data = (metadata, data)

        result_metadata, ser_mem_desc = self.data_mover._move_data(input_data)

        # Free the allocation (should not raise exception)
        NumPyDataMover.free_alloc(ser_mem_desc)

    def test_end_to_end_workflow(self):
        """Test the complete workflow from numpy to CuPy."""
        # Create test data
        original_data = np.random.rand(8, 8).astype(np.float64)

        # Put data into input queue
        self.data_mover.input_queue.put(original_data)

        # Get data from input queue (simulates what _move_data receives)
        input_data = self.data_mover.input_queue.get()

        # Move data to device
        moved_data = self.data_mover._move_data(input_data)

        # Put moved data into output queue
        self.data_mover.output_queue.put(moved_data)

        # Get reconstructed data from output queue
        cupy_array = self.data_mover.output_queue.get()

        # Verify the result
        self.assertIsInstance(cupy_array, tuple)  # Returns (array, descriptor)
        reconstructed_array, descriptor = cupy_array
        self.assertIsInstance(reconstructed_array, cp.ndarray)

        # Verify data integrity
        np.testing.assert_array_almost_equal(reconstructed_array.get(), original_data)

        # Clean up
        NumPyDataMover.free_alloc(descriptor)


class TestNumPyOptimizedInputQueue(unittest.TestCase):
    """Test cases for NumPyOptimizedInputQueue class."""

    def setUp(self):
        """Set up test fixtures."""
        self.pool_size = 1024 * 1024  # 1MB
        self.mpool = MemoryPool(self.pool_size, "test_opt_pool", 1046)
        self.queue = NumPyOptimizedInputQueue(self.mpool)

    def tearDown(self):
        """Clean up after tests."""
        self.queue.close()
        self.mpool.destroy()

    def test_put_allocates_memory(self):
        """Test that put method allocates memory from the pool."""
        data = np.random.rand(10, 10).astype(np.float32)

        # Put data into queue
        self.queue.put(data)

        # Get data from queue
        metadata, ser_mem_desc = self.queue.get()
        shape, dtype = metadata

        # Verify metadata
        self.assertEqual(shape, data.shape)
        self.assertEqual(dtype, data.dtype)
        self.assertIsNotNone(ser_mem_desc)

        # Verify we can attach to the memory allocation
        mem_alloc = MemoryAlloc.attach(ser_mem_desc)
        self.assertEqual(mem_alloc.size, data.nbytes)

        # Clean up
        mem_alloc.free()

    def test_serialization_preserves_mpool(self):
        """Test that serialization preserves the memory pool."""
        # Get state
        state = self.queue.__getstate__()

        # Create new queue and set state
        new_queue = NumPyOptimizedInputQueue(None)
        new_queue.__setstate__(state)

        # Verify memory pool is restored (by checking we can use it)
        data = np.random.rand(5, 5).astype(np.float32)
        new_queue.put(data)

        metadata, ser_mem_desc = new_queue.get()
        mem_alloc = MemoryAlloc.attach(ser_mem_desc)
        mem_alloc.free()

        new_queue.close()


class TestNumPyOptimizedDataMover(unittest.TestCase):
    """Test cases for NumPyOptimizedDataMover class."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_stop_event = Mock()
        self.mock_stop_event.is_set.return_value = False

        # Create device memory pool
        self.device_pool_size = 1024 * 1024  # 1MB
        self.device_pool = MemoryPool(self.device_pool_size, "test_opt_device_pool", 1047, gpu_memory=True)

        # Create optimized data mover
        self.host_pool_size = 1024 * 1024  # 1MB
        self.data_mover = NumPyOptimizedDataMover(
            pool_size=self.host_pool_size, stop_event=self.mock_stop_event, device_pool=self.device_pool
        )

    def tearDown(self):
        """Clean up after tests."""
        self.data_mover.cleanup()
        self.device_pool.destroy()

    def test_uses_optimized_input_queue(self):
        """Test that optimized data mover uses NumPyOptimizedInputQueue."""
        self.assertIsInstance(self.data_mover.input_queue, NumPyOptimizedInputQueue)

    def test_move_data_with_serialized_descriptor(self):
        """Test moving data using serialized memory descriptor."""
        # Create test data
        data = np.random.rand(6, 6).astype(np.float32)

        # Simulate what the optimized input queue produces
        host_mem_alloc = self.data_mover._mpool.alloc(data.nbytes)
        host_mem_view = host_mem_alloc.get_memview()
        host_mem_view[:] = memoryview(data).tobytes()
        ser_host_mem_desc = host_mem_alloc.serialize()

        metadata = (data.shape, data.dtype)
        input_data = (metadata, ser_host_mem_desc)

        # Move data
        result_metadata, ser_device_mem_desc = self.data_mover._move_data(input_data)

        # Verify metadata is preserved
        self.assertEqual(result_metadata, metadata)
        self.assertIsNotNone(ser_device_mem_desc)

        # Clean up device memory
        device_mem_alloc = MemoryAlloc.attach(ser_device_mem_desc)
        device_mem_alloc.free()

    def test_end_to_end_optimized_workflow(self):
        """Test the complete optimized workflow."""
        # Create test data
        original_data = np.random.rand(7, 7).astype(np.float32)

        # Put data into optimized input queue
        self.data_mover.input_queue.put(original_data)

        # Get data from input queue
        input_data = self.data_mover.input_queue.get()

        # Move data to device
        moved_data = self.data_mover._move_data(input_data)

        # Reconstruct on device
        metadata, ser_device_mem_desc = moved_data
        cupy_array, descriptor = NumPyDataMover.reconstruct(ser_device_mem_desc, metadata)

        # Verify result
        self.assertIsInstance(cupy_array, cp.ndarray)
        np.testing.assert_array_almost_equal(cupy_array.get(), original_data)

        # Clean up
        NumPyDataMover.free_alloc(descriptor)


class TestCuPyInputQueue(unittest.TestCase):
    """Test cases for CuPyInputQueue class."""

    def setUp(self):
        """Set up test fixtures."""
        # CuPyInputQueue requires a memory pool for GPU memory
        self.pool_size = 1024 * 1024  # 1MB
        self.device_pool = MemoryPool(self.pool_size, "test_cupy_queue_pool", 1048, gpu_memory=True)
        self.queue = CuPyInputQueue(self.device_pool)

    def tearDown(self):
        """Clean up after tests."""
        self.queue.close()
        self.device_pool.destroy()

    def test_put_cupy_array(self):
        """Test putting a CuPy array into the queue."""
        # Create test data on GPU
        data = cp.random.rand(10, 10).astype(cp.float32)

        # Put data into queue
        self.queue.put(data)

        # Retrieve data
        metadata, ser_mem_desc = self.queue.get()
        shape, dtype = metadata

        # Verify metadata
        self.assertEqual(shape, data.shape)
        self.assertEqual(dtype, data.dtype)
        self.assertIsNotNone(ser_mem_desc)

        # Clean up device memory
        device_mem_alloc = MemoryAlloc.attach(ser_mem_desc)
        device_mem_alloc.free()

    def test_put_non_cupy_array_raises_error(self):
        """Test that putting non-CuPy data raises TypeError."""
        with self.assertRaises(TypeError):
            self.queue.put(np.array([1, 2, 3]))

        with self.assertRaises(TypeError):
            self.queue.put("not an array")

        with self.assertRaises(TypeError):
            self.queue.put(42)


class TestCuPyDataMover(unittest.TestCase):
    """Test cases for CuPyDataMover class (GPU->host)."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_stop_event = Mock()
        self.mock_stop_event.is_set.return_value = False

        # Create a device memory pool for testing
        self.device_pool_size = 1024 * 1024  # 1MB for testing
        self.device_pool = MemoryPool(self.device_pool_size, "test_cupy_mover_device_pool", 1049, gpu_memory=True)

        # Create data mover instance
        self.host_pool_size = 1024 * 1024  # 1MB for testing
        self.data_mover = CuPyDataMover(
            pool_size=self.host_pool_size, stop_event=self.mock_stop_event, device_pool=self.device_pool
        )

    def tearDown(self):
        """Clean up after tests."""
        self.data_mover.cleanup()
        self.device_pool.destroy()

    def test_initialization(self):
        """Test that CuPyDataMover initializes correctly."""
        self.assertIsNotNone(self.data_mover._mpool)
        self.assertIsNotNone(self.data_mover.input_queue)
        self.assertIsNotNone(self.data_mover.output_queue)
        self.assertIsInstance(self.data_mover.input_queue, CuPyInputQueue)
        self.assertIsInstance(self.data_mover.output_queue, OutputQueue)

    def test_move_data_small_array(self):
        """Test moving a small CuPy array from GPU to host."""
        # Create test data on GPU
        cupy_data = cp.random.rand(10, 10).astype(cp.float32)

        # Allocate GPU memory and copy data
        device_mem_alloc = self.device_pool.alloc(cupy_data.nbytes)
        intptr = device_mem_alloc.get_int_ptr()
        cp.cuda.runtime.memcpy(intptr, cupy_data.data.ptr, cupy_data.nbytes, cp.cuda.runtime.memcpyDeviceToDevice)
        ser_device_mem_desc = device_mem_alloc.serialize()

        metadata = (cupy_data.shape, cupy_data.dtype)
        input_data = (metadata, ser_device_mem_desc)

        # Move data
        result_metadata, ser_mem_desc = self.data_mover._move_data(input_data)

        # Verify metadata is preserved
        self.assertEqual(result_metadata, metadata)
        self.assertIsNotNone(ser_mem_desc)

    def test_reconstruct_static_method(self):
        """Test the static reconstruct method."""
        # Create test data and move it to host
        cupy_data = cp.random.rand(5, 5).astype(cp.float32)

        # Allocate GPU memory and copy data
        device_mem_alloc = self.device_pool.alloc(cupy_data.nbytes)
        intptr = device_mem_alloc.get_int_ptr()
        cp.cuda.runtime.memcpy(intptr, cupy_data.data.ptr, cupy_data.nbytes, cp.cuda.runtime.memcpyDeviceToDevice)
        ser_device_mem_desc = device_mem_alloc.serialize()

        metadata = (cupy_data.shape, cupy_data.dtype)
        input_data = (metadata, ser_device_mem_desc)

        # Move data to get host serialized descriptor
        result_metadata, ser_host_mem_desc = self.data_mover._move_data(input_data)

        # Reconstruct the NumPy array
        numpy_array = CuPyDataMover.reconstruct(ser_host_mem_desc, metadata)

        # Verify the reconstructed array
        self.assertIsInstance(numpy_array, np.ndarray)
        self.assertEqual(numpy_array.shape, cupy_data.shape)
        self.assertEqual(numpy_array.dtype, cupy_data.dtype.type().dtype)

        # Verify data content
        np.testing.assert_array_almost_equal(numpy_array, cupy_data.get())


class TestCuPyOptimizedDataMover(unittest.TestCase):
    """Test cases for CuPyOptimizedDataMover class (GPU->host optimized)."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_stop_event = Mock()
        self.mock_stop_event.is_set.return_value = False

        # Create a device memory pool for testing
        self.device_pool_size = 1024 * 1024  # 1MB for testing
        self.device_pool = MemoryPool(self.device_pool_size, "test_cupy_opt_device_pool", 1050, gpu_memory=True)

        # Create optimized data mover
        self.host_pool_size = 1024 * 1024  # 1MB for testing
        self.data_mover = CuPyOptimizedDataMover(
            pool_size=self.host_pool_size, stop_event=self.mock_stop_event, device_pool=self.device_pool
        )

    def tearDown(self):
        """Clean up after tests."""
        self.data_mover.cleanup()
        self.device_pool.destroy()

    def test_initialization(self):
        """Test that CuPyOptimizedDataMover initializes correctly."""
        self.assertIsNotNone(self.data_mover._mpool)
        self.assertIsNotNone(self.data_mover.input_queue)
        self.assertIsNotNone(self.data_mover.output_queue)
        self.assertIsInstance(self.data_mover.input_queue, Queue)  # Regular queue
        self.assertIsInstance(self.data_mover.output_queue, OutputQueue)

    def test_move_data_with_serialized_descriptor(self):
        """Test moving data using serialized GPU memory descriptor."""
        # Create test data on GPU
        cupy_data = cp.random.rand(6, 6).astype(cp.float32)

        # Allocate GPU memory and copy data
        device_mem_alloc = self.device_pool.alloc(cupy_data.nbytes)
        intptr = device_mem_alloc.get_int_ptr()
        cp.cuda.runtime.memcpy(intptr, cupy_data.data.ptr, cupy_data.nbytes, cp.cuda.runtime.memcpyDeviceToDevice)
        ser_device_mem_desc = device_mem_alloc.serialize()

        metadata = (cupy_data.shape, cupy_data.dtype)
        input_data = (metadata, ser_device_mem_desc)

        # Move data
        result_metadata, ser_host_mem_desc = self.data_mover._move_data(input_data)

        # Verify metadata is preserved
        self.assertEqual(result_metadata, metadata)
        self.assertIsNotNone(ser_host_mem_desc)

    def test_reconstruct_static_method(self):
        """Test the static reconstruct method."""
        # Create test data and move it to host
        cupy_data = cp.random.rand(4, 4).astype(cp.float32)

        # Allocate GPU memory and copy data
        device_mem_alloc = self.device_pool.alloc(cupy_data.nbytes)
        intptr = device_mem_alloc.get_int_ptr()
        cp.cuda.runtime.memcpy(intptr, cupy_data.data.ptr, cupy_data.nbytes, cp.cuda.runtime.memcpyDeviceToDevice)
        ser_device_mem_desc = device_mem_alloc.serialize()

        metadata = (cupy_data.shape, cupy_data.dtype)
        input_data = (metadata, ser_device_mem_desc)

        # Move data to get host serialized descriptor
        result_metadata, ser_host_mem_desc = self.data_mover._move_data(input_data)

        # Reconstruct the NumPy array
        numpy_array = CuPyOptimizedDataMover.reconstruct(ser_host_mem_desc, metadata)

        # Verify the reconstructed array
        self.assertIsInstance(numpy_array, np.ndarray)
        self.assertEqual(numpy_array.shape, cupy_data.shape)
        self.assertEqual(numpy_array.dtype, cupy_data.dtype.type().dtype)

        # Verify data content
        np.testing.assert_array_almost_equal(numpy_array, cupy_data.get())


class TestDataMovers(unittest.TestCase):
    """Test cases for DataMovers manager class."""

    def setUp(self):
        """Set up test fixtures."""
        # Use smaller pools for testing
        self.device_pool_size = 1024 * 1024  # 1MB
        self.host_pool_size = 512 * 1024  # 512KB

        # Create test policy
        self.test_policy = Policy(placement=Policy.Placement.HOST_NAME, host_name=current().hostname)

    def test_initialization_with_single_worker(self):
        """Test DataMovers initialization with single worker."""
        data_movers = DataMovers(
            data_mover=NumPyDataMover,
            data_mover_args={"pool_size": self.host_pool_size},
            num_workers=1,
            device_pool_size=self.device_pool_size,
            manager_policy=self.test_policy,
        )

        self.assertIsNotNone(data_movers._stop_event)
        self.assertIsNotNone(data_movers._manager_queue)
        self.assertEqual(data_movers._num_workers, 1)
        self.assertEqual(data_movers._device_pool_size, self.device_pool_size)

    def test_initialization_with_policies(self):
        """Test DataMovers initialization with custom policies."""
        policies = [self.test_policy]

        data_movers = DataMovers(
            data_mover=NumPyDataMover,
            data_mover_args={"pool_size": self.host_pool_size},
            policies=policies,
            num_workers_per_policy=1,
            device_pool_size=self.device_pool_size,
            manager_policy=self.test_policy,
            num_workers=None,
        )

        self.assertEqual(data_movers._policies, policies)
        self.assertEqual(data_movers._num_workers_per_policy, 1)
        self.assertIsNone(data_movers._num_workers)

    def test_invalid_initialization_raises_assertion(self):
        """Test that invalid initialization parameters raise assertions."""
        with self.assertRaises(AssertionError):
            # Cannot specify both policies and num_workers
            DataMovers(
                data_mover=NumPyDataMover,
                policies=[self.test_policy],
                num_workers=2,
                device_pool_size=self.device_pool_size,
            )

    @unittest.skip(
        "Skipping momentarily. Need to work on getting the patching to work correctly and make sense for this test."
    )
    @patch("dragon.workflows.data_mover.Process")
    @patch("dragon.native.event.Event")
    def test_manager_creates_process_group(self, mock_process, mock_event):
        """Test that manager creates and initializes process group."""
        # Setup mocks
        mock_event.is_set.return_value = False

        # Create DataMovers instance
        data_movers = DataMovers(
            data_mover=NumPyDataMover,
            data_mover_args={"pool_size": self.host_pool_size},
            num_workers=1,
            device_pool_size=self.device_pool_size,
            manager_policy=self.test_policy,
        )

        # Start the manager (this will run in a separate process)
        data_movers.start()

        call_args = mock_process.call_args  # Check call arguments
        print(f"{call_args=}", flush=True)

        mock_event.is_set.return_value = True
        call_args[1]

        # Stop the manager
        data_movers.stop()


class TestIntegration(unittest.TestCase):
    """Integration tests for the complete data mover system."""

    def setUp(self):
        """Set up integration test fixtures."""
        # Use small pools for testing
        self.device_pool_size = 2 * 1024 * 1024  # 2MB
        self.host_pool_size = 1024 * 1024  # 1MB

        # Create test policy
        self.test_policy = Policy(placement=Policy.Placement.HOST_NAME, host_name=current().hostname)

    def tearDown(self):
        """Clean up integration test fixtures."""
        pass

    def test_data_mover_workflow_with_mock_processes(self):
        """Test data mover workflow with mocked process execution."""

        # Create a simple mock data mover for testing
        class MockDataMover(DataMover):
            def __init__(self, *args, **kwargs):
                self.device_pool = kwargs.get("device_pool")
                super().__init__(*args, **kwargs)

            def _move_data(self, data):
                # Simple mock implementation
                metadata, host_data = data
                return (metadata, f"mock_device_descriptor_{id(host_data)}")

            @staticmethod
            def reconstruct(device_ser_mem_desc, metadata):
                shape, dtype = metadata
                # Return mock CuPy array (use numpy for testing)
                mock_array = np.random.rand(*shape).astype(dtype)
                return mock_array, device_ser_mem_desc

            def construct_input_output_queues(self):
                self.input_queue = NumPyInputQueue()
                self.output_queue = OutputQueue(self.reconstruct)

            def cleanup(self):
                pass

        # Test the mock data mover
        mock_stop_event = Mock()
        mock_stop_event.is_set.return_value = False

        device_pool = MemoryPool(self.device_pool_size, "integration_test_pool", 1048, gpu_memory=True)

        try:
            data_mover = MockDataMover(stop_event=mock_stop_event, device_pool=device_pool)

            # Test putting data through the system
            test_data = np.random.rand(5, 5).astype(np.float32)
            data_mover.input_queue.put(test_data)

            # Simulate the run loop (single iteration)
            input_data = data_mover.input_queue.get()
            moved_data = data_mover._move_data(input_data)
            data_mover.output_queue.put(moved_data)

            # Get the result
            result = data_mover.output_queue.get()

            # Verify we got a numpy array back (mock CuPy)
            self.assertIsInstance(result, tuple)
            array, descriptor = result
            self.assertIsInstance(array, np.ndarray)
            self.assertEqual(array.shape, test_data.shape)
            self.assertEqual(array.dtype, test_data.dtype)

            data_mover.cleanup()

        finally:
            device_pool.destroy()

    def test_complete_data_pipeline_workflow(self):
        """Test complete workflow: NumPy -> GPU -> processing -> host as in benchmark."""
        # Create test data
        original_data = np.random.rand(8, 8).astype(np.complex128)

        # Create device memory pools
        device_pool = MemoryPool(self.device_pool_size, "pipeline_device_pool", 1051, gpu_memory=True)

        try:
            # Setup data movers like in benchmark
            mock_stop_event = Mock()
            mock_stop_event.is_set.return_value = False

            # NumPy -> GPU data mover (like movers_on in benchmark)
            numpy_to_gpu_mover = NumPyDataMover(
                pool_size=self.host_pool_size, stop_event=mock_stop_event, device_pool=device_pool
            )

            # GPU -> NumPy data mover (like movers_off in benchmark)
            gpu_to_numpy_mover = CuPyDataMover(
                pool_size=self.host_pool_size, stop_event=mock_stop_event, device_pool=device_pool
            )

            # Step 1: Move numpy data to GPU
            numpy_to_gpu_mover.input_queue.put(original_data)
            host_to_gpu_data = numpy_to_gpu_mover.input_queue.get()
            gpu_metadata, gpu_ser_mem_desc = numpy_to_gpu_mover._move_data(host_to_gpu_data)

            # Step 2: Reconstruct as CuPy array on GPU
            cupy_array, gpu_descriptor = NumPyDataMover.reconstruct(gpu_ser_mem_desc, gpu_metadata)
            self.assertIsInstance(cupy_array, cp.ndarray)

            # Step 3: Simulate GPU processing (like cupy_user_kernel in benchmark)
            # Simple operation: conjugate (simulates FFT processing)
            processed_cupy = cp.conj(cupy_array)

            # Step 4: Move processed data back to host
            # Put processed CuPy array into GPU->host mover input
            gpu_to_numpy_mover.input_queue.put(processed_cupy)
            gpu_to_host_data = gpu_to_numpy_mover.input_queue.get()
            host_metadata, host_ser_mem_desc = gpu_to_numpy_mover._move_data(gpu_to_host_data)

            # Step 5: Reconstruct as NumPy array on host
            final_numpy_array = CuPyDataMover.reconstruct(host_ser_mem_desc, host_metadata)

            # Verify complete pipeline
            self.assertIsInstance(final_numpy_array, np.ndarray)
            self.assertEqual(final_numpy_array.shape, original_data.shape)
            self.assertEqual(final_numpy_array.dtype, original_data.dtype)

            # Verify processing was applied correctly
            expected_result = np.conj(original_data)
            np.testing.assert_array_almost_equal(final_numpy_array, expected_result)

            # Clean up
            NumPyDataMover.free_alloc(gpu_descriptor)
            numpy_to_gpu_mover.cleanup()
            gpu_to_numpy_mover.cleanup()

        finally:
            device_pool.destroy()


if __name__ == "__main__":
    # Run the tests
    unittest.main(verbosity=2)
