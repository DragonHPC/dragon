"""
Test driver for SerializableNDArray and related Serializable classes.
This test program works in concert with cpp_serializables.cpp to provide tests
that run partly in Python and partly in C++ compiled code.
"""

import os
import unittest
import time
import dragon
from dragon import System, DDict
from dragon.native.process import Popen
from dragon.native.machine import Node
from dragon.infrastructure.facts import DRAGON_LIB_DIR
from dragon.utils import X2DMatrixPickler, XScalarPickler, XPickler, XStringPickler, XByteBufferPickler
import multiprocessing as mp
import pathlib
import numpy as np

test_dir = pathlib.Path(__file__).resolve().parent
os.system(f"cd {test_dir}; make --silent")

ENV = dict(os.environ)
ENV["LD_LIBRARY_PATH"] = str(DRAGON_LIB_DIR) + ":" + str(ENV.get("LD_LIBRARY_PATH", ""))
ENV["DYLD_FALLBACK_LIBRARY_PATH"] = str(DRAGON_LIB_DIR) + ":" + str(ENV.get("DYLD_FALLBACK_LIBRARY_PATH", ""))


class TestSerializables(unittest.TestCase):

    def tearDown(self):
        for p in pathlib.Path(".").glob("*.ddict"):
            os.remove(p)

    def _make_queues(self):
        """
        Create the queue used to send to C++ and the one used to receive from it. Two
        queues are needed so a get on this side cannot consume our own message.
        """
        to_cpp = dragon.native.queue.Queue(maxsize=10, pickler=XPickler())
        from_cpp = dragon.native.queue.Queue(maxsize=10, pickler=XPickler())

        return to_cpp, from_cpp

    def _start_cpp(self, test_name, to_cpp, from_cpp, ser_ddict="unused"):
        return Popen(
            executable=str(test_dir / "cpp_serializables"),
            args=[to_cpp.serialize(), from_cpp.serialize(), ser_ddict, test_name],
            env=ENV,
        )

    def _round_trip(self, test_name, value):
        """
        Put value on a Queue, let the C++ test validate it and reply, and return the reply.
        """
        to_cpp, from_cpp = self._make_queues()
        to_cpp.put(value)

        proc = self._start_cpp(test_name, to_cpp, from_cpp)

        reply = from_cpp.get()

        proc.wait()
        self.assertEqual(proc.returncode, 0, f"{test_name}: CPP process exited with non-zero exit code")

        to_cpp.destroy()
        from_cpp.destroy()

        return reply

    def _ndarray_round_trip(self, test_name, data):
        """
        Put an XNDArray on a Queue. The C++ test validates it, doubles every element in
        place, and sends back the last row as an NDArray of its own.
        """
        # The XNDArray is a view of data, so the expected values must be captured up front.
        expected = data * 2
        expected_reply = data[-1] * 2
        dtype = data.dtype

        ddict = DDict(2, 1, 3000000)
        ser_ddict = ddict.serialize()

        to_cpp, from_cpp = self._make_queues()
        xndarray = dragon.utils.XNDArray(data, ser_ddict)
        to_cpp.put(xndarray)

        proc = self._start_cpp(test_name, to_cpp, from_cpp, ser_ddict)

        reply = from_cpp.get()

        proc.wait()
        self.assertEqual(proc.returncode, 0, f"{test_name}: CPP process exited with non-zero exit code")

        xndarray.refresh()
        self.assertTrue(np.array_equal(xndarray, expected), f"{test_name}: {xndarray} is not {expected}")

        self.assertIsInstance(reply, dragon.utils.XNDArray)
        self.assertEqual(reply.dtype, dtype)
        self.assertTrue(np.array_equal(reply, expected_reply), f"{test_name}: {reply} is not {expected_reply}")

        reply.destroy()
        xndarray.destroy()
        to_cpp.destroy()
        from_cpp.destroy()
        ddict.destroy()

    def test_xndarray_through_queue(self):
        """
        Test passing an XNDArray object through a Queue from Python to C++ code.
        The C++ process receives the XNDArray from the queue and validates its data.
        """
        exe = "cpp_serializables"

        # Create a DDict to store the ndarray data
        ddict = DDict(2, 1, 3000000, trace=True)
        ser_ddict = ddict.serialize()

        # One Queue to send to C++ and another to receive from it
        to_cpp, from_cpp = self._make_queues()

        # Create a 2D numpy array with known data
        data = np.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]])

        # Create an XNDArray from the data
        # The XNDArray will be passed through the queue
        xndarray = dragon.utils.XNDArray(data, ser_ddict)

        # Put the XNDArray into the queue
        to_cpp.put(xndarray)

        # Spawn the C++ process to receive and validate
        proc = Popen(
            executable=str(test_dir / exe),
            args=[to_cpp.serialize(), from_cpp.serialize(), ser_ddict, "test_xndarray_through_queue"],
            env=ENV
        )

        # While writing code like this loop would be a terrible idea in general, it tests
        # refresh in Python and sync in C++.
        count = 0
        while xndarray[0,2] != 8.0 and count < 1000:
            time.sleep(1)
            xndarray.refresh()
            count += 1

        self.assertEqual(xndarray[0,2], 8.0)
        xndarray[0,0] = 2.0
        xndarray.sync()
        proc.wait()

        copy = from_cpp.get()
        self.assertTrue(np.array_equal(copy, [4, 5, 6]))
        self.assertIsInstance(copy, dragon.utils.XNDArray)

        copy.destroy()
        xndarray.destroy()

        to_cpp.destroy()
        from_cpp.destroy()
        ddict.destroy()

        self.assertEqual(proc.returncode, 0, "CPP process exited with non-zero exit code")

    def test_string_through_queue(self):
        """
        Test passing a str to C++ as a SerializableString and getting one back.
        """
        reply = self._round_trip("test_string_through_queue", "Hello from Python")
        self.assertEqual(reply, "Hello from C++")

    def test_int_through_queue(self):
        """
        Test passing an int to C++ as a SerializableInt and getting one back.
        """
        reply = self._round_trip("test_int_through_queue", 42)
        self.assertEqual(reply, 84)

    def test_double_through_queue(self):
        """
        Test passing a float to C++ as a SerializableDouble and getting one back.
        """
        reply = self._round_trip("test_double_through_queue", 3.5)
        self.assertEqual(reply, 7.0)

    def test_bytes_through_queue(self):
        """
        Test passing bytes to C++ as a SerializableByteBuffer and getting bytes back.
        """
        reply = self._round_trip("test_bytes_through_queue", b"bytes from Python")
        self.assertEqual(reply, b"bytes from C++")

    def test_int_vector_through_queue(self):
        """
        Test passing a 1D int array to C++ as a SerializableIntVector and getting one back.
        """
        data = np.array([1, 2, 3, 4], dtype=np.int32)
        reply = self._round_trip("test_int_vector_through_queue", data)
        self.assertTrue(np.array_equal(reply, data * 2), f"{reply} is not {data * 2}")

    def test_double_vector_through_queue(self):
        """
        Test passing a 1D float array to C++ as a SerializableDoubleVector and getting one back.
        """
        data = np.array([1.5, 2.5, 3.5, 4.5])
        reply = self._round_trip("test_double_vector_through_queue", data)
        self.assertTrue(np.array_equal(reply, data * 2), f"{reply} is not {data * 2}")

    def test_int_matrix_through_queue(self):
        """
        Test passing a 2D int array to C++ as a Serializable2DIntMatrix and getting one back.
        """
        data = np.array([[1, 2, 3], [4, 5, 6]], dtype=np.int32)
        reply = self._round_trip("test_int_matrix_through_queue", data)
        self.assertTrue(np.array_equal(reply, data * 2), f"{reply} is not {data * 2}")

    def test_double_matrix_through_queue(self):
        """
        Test passing a 2D float array to C++ as a Serializable2DDoubleMatrix and getting one back.
        """
        data = np.array([[1.5, 2.5, 3.5], [4.5, 5.5, 6.5]])
        reply = self._round_trip("test_double_matrix_through_queue", data)
        self.assertTrue(np.array_equal(reply, data * 2), f"{reply} is not {data * 2}")

    def test_float_ndarray_through_queue(self):
        """
        Test a float32 XNDArray against the C++ SerializableFloatNDArray.
        """
        data = np.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]], dtype=np.float32)
        self._ndarray_round_trip("test_float_ndarray_through_queue", data)

    def test_int_ndarray_through_queue(self):
        """
        Test an int32 XNDArray against the C++ SerializableIntNDArray.
        """
        data = np.array([[1, 2, 3], [4, 5, 6]], dtype=np.int32)
        self._ndarray_round_trip("test_int_ndarray_through_queue", data)

    def test_long_ndarray_through_queue(self):
        """
        Test an int64 XNDArray against the C++ SerializableLongNDArray.
        """
        data = np.array([[1, 2, 3], [4, 5, 6]], dtype=np.int64)
        self._ndarray_round_trip("test_long_ndarray_through_queue", data)

    def test_queue_through_queue(self):
        """
        Test passing a native Queue to C++ as a SerializableQueue and getting one back.
        The C++ code attaches to the Queue it is given and puts a value on it.
        """
        inner = dragon.native.queue.Queue(maxsize=10, pickler=XPickler())

        reply = self._round_trip("test_queue_through_queue", inner)

        self.assertEqual(reply.serialize(), inner.serialize())
        self.assertEqual(reply.get(), "Hello from the inner queue")

        inner.destroy()

    def test_ddict_through_queue(self):
        """
        Test passing a DDict to C++ as a SerializableDDict and getting one back.
        The C++ code attaches to the DDict it is given and stores a key/value pair in it.
        """
        inner = DDict(2, 1, 3000000)

        reply = self._round_trip("test_ddict_through_queue", inner)

        self.assertEqual(reply.serialize(), inner.serialize())
        self.assertEqual(reply["hello"], 42)

        inner.destroy()

    def test_barrier_through_queue(self):
        """
        Test passing a Barrier to C++ as a SerializableBarrier and getting one back.
        Both sides wait on the barrier, so C++ is still running when the reply is read.
        """
        inner = dragon.native.barrier.Barrier(parties=2)

        to_cpp, from_cpp = self._make_queues()
        to_cpp.put(inner)

        proc = self._start_cpp("test_barrier_through_queue", to_cpp, from_cpp)

        reply = from_cpp.get()
        self.assertEqual(reply.serialize(), inner.serialize())
        self.assertEqual(reply.parties, 2)

        inner.wait()

        proc.wait()
        self.assertEqual(proc.returncode, 0, "test_barrier_through_queue: CPP process exited with non-zero exit code")

        to_cpp.destroy()
        from_cpp.destroy()

    def test_semaphore_through_queue(self):
        """
        Test passing a Semaphore to C++ as a SerializableSemaphore and getting one back.
        The C++ code releases the semaphore it is given.
        """
        inner = dragon.native.semaphore.Semaphore(value=0)

        reply = self._round_trip("test_semaphore_through_queue", inner)

        self.assertEqual(reply.serialize(), inner.serialize())
        self.assertTrue(reply.acquire(timeout=10))

    def test_semaphore_through_queue(self):
        """
        Test passing a Semaphore to C++ as a SerializableSemaphore and getting one back.
        The C++ code releases the semaphore it is given.
        """
        inner = dragon.native.semaphore.Semaphore(value=0)

        reply = self._round_trip("test_semaphore_through_queue", inner)

        self.assertEqual(reply.serialize(), inner.serialize())
        self.assertTrue(reply.acquire(timeout=10))


if __name__ == "__main__":
    mp.set_start_method("dragon", force=True)
    unittest.main()
