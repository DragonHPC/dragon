import os
import unittest
import dragon
from dragon.native.process import Popen
from dragon.native.queue import Queue
from dragon.infrastructure.facts import DRAGON_LIB_DIR
from dragon.channels import Channel
import multiprocessing as mp
import pathlib
import numpy as np
import ctypes
import sys

test_dir = pathlib.Path(__file__).resolve().parent
os.system(f"cd {test_dir}; make --silent")

ENV = dict(os.environ)
ENV["LD_LIBRARY_PATH"] = str(DRAGON_LIB_DIR) + ":" + str(ENV.get("LD_LIBRARY_PATH", ""))
ENV["DYLD_FALLBACK_LIBRARY_PATH"] = str(DRAGON_LIB_DIR) + ":" + str(ENV.get("DYLD_FALLBACK_LIBRARY_PATH", ""))

def _put_and_join(q, item):
    q.put(item)
    q.join(timeout=None)


def _multiple_puts_and_join(q, num_puts, item):
    for _ in range(num_puts):
        q.put(item)
    q.join(timeout=None)


def _task_done_and_join(q):
    q.task_done()
    q.join()


class numPy2dPickler:

    def __init__(self, shape: tuple, data_type: np.dtype, chunk_size=0):
        self._shape = shape
        self._data_type = data_type
        self._chunk_size = chunk_size

    def dump(self, nparr, file) -> None:

        # write the dimension of the array
        num_bytes = ctypes.sizeof(ctypes.c_size_t)
        nrow = nparr.shape[0]
        ncol = nparr.shape[1]
        bytes_nrow = nrow.to_bytes(num_bytes, byteorder=sys.byteorder)
        bytes_ncol = ncol.to_bytes(num_bytes, byteorder=sys.byteorder)
        file.write(bytes_nrow)
        file.write(bytes_ncol)

        # write array
        mv = memoryview(nparr)
        bobj = mv.tobytes()
        # print(f"Dumping {bobj=}", file=sys.stderr, flush=True)
        if self._chunk_size == 0:
            chunk_size = len(bobj)
        else:
            chunk_size = self._chunk_size

        for i in range(0, len(bobj), chunk_size):
            file.write(bobj[i : i + chunk_size])

    def load(self, file):

        obj = None

        # read the dimension of the array
        num_bytes = ctypes.sizeof(ctypes.c_size_t)
        nrow = file.read(num_bytes)
        ncol = file.read(num_bytes)

        try:
            while True:
                data = file.read(self._chunk_size)
                if obj is None:
                    # convert bytes to bytearray
                    view = memoryview(data)
                    obj = bytearray(view)
                else:
                    obj.extend(data)
        except EOFError:
            pass

        ret_arr = np.frombuffer(obj, dtype=self._data_type).reshape(self._shape)

        return ret_arr


class TestQueueCPP(unittest.TestCase):
    def test_attach_detach(self):
        exe = "cpp_queue"
        q = Queue()
        ser_q = q.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_q, "test_attach_detach"], env=ENV)
        proc.wait()
        self.assertEqual(proc.returncode, 0, "CPP client exited with non-zero exit code")
        q.destroy()

    def test_attach_with_pool(self):
        exe = "cpp_queue"
        q = Queue()
        ser_q = q.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_q, "test_attach_with_pool"], env=ENV)
        proc.wait()
        self.assertEqual(proc.returncode, 0, "CPP client exited with non-zero exit code")
        q.destroy()

    def test_serialize(self):
        exe = "cpp_queue"
        q = Queue()
        ser_q = q.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_q, "test_serialize"], env=ENV)
        proc.wait()
        self.assertEqual(proc.returncode, 0, "CPP client exited with non-zero exit code")
        q.destroy()

    def test_single_put(self):
        exe = "cpp_queue"
        q = Queue()
        ser_q = q.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_q, "test_single_put"], env=ENV)
        proc.wait()
        self.assertEqual(proc.returncode, 0, "CPP client exited with non-zero exit code")
        q.destroy()

    def test_multiple_puts(self):
        exe = "cpp_queue"
        q = Queue()
        ser_q = q.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_q, "test_multiple_puts"], env=ENV)
        proc.wait()
        self.assertEqual(proc.returncode, 0, "CPP client exited with non-zero exit code")
        q.destroy()

    def test_put_in_full_queue(self):
        exe = "cpp_queue"
        q = Queue(maxsize=5)
        for _ in range(5):
            q.put(25)
        ser_q = q.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_q, "test_put_in_full_queue"], env=ENV)
        proc.wait()
        self.assertEqual(proc.returncode, 0, "CPP client exited with non-zero exit code")
        q.destroy()

    def test_put_with_arg(self):
        exe = "cpp_queue"
        q = Queue()
        ser_q = q.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_q, "test_put_with_arg"], env=ENV)
        proc.wait()
        self.assertEqual(proc.returncode, 0, "CPP client exited with non-zero exit code")
        q.destroy()

    def test_get_nowait(self):
        exe = "cpp_queue"
        q = Queue()
        ser_q = q.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_q, "test_get_nowait"], env=ENV)
        proc.wait()
        self.assertEqual(proc.returncode, 0, "CPP client exited with non-zero exit code")
        q.destroy()

    def test_get_from_empty_queue(self):
        exe = "cpp_queue"
        q = Queue()
        ser_q = q.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_q, "test_get_from_empty_queue"], env=ENV)
        proc.wait()
        self.assertEqual(proc.returncode, 0, "CPP client exited with non-zero exit code")
        q.destroy()

    def test_get_from_emptied_queue(self):
        exe = "cpp_queue"
        q = Queue()
        ser_q = q.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_q, "test_get_from_emptied_queue"], env=ENV)
        proc.wait()
        self.assertEqual(proc.returncode, 0, "CPP client exited with non-zero exit code")
        q.destroy()

    def test_poll_empty_queue(self):
        exe = "cpp_queue"
        q = Queue()
        ser_q = q.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_q, "test_poll_empty_queue"], env=ENV)
        proc.wait()
        self.assertEqual(proc.returncode, 0, "CPP client exited with non-zero exit code")
        q.destroy()

    def test_poll(self):
        exe = "cpp_queue"
        q = Queue()
        ser_q = q.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_q, "test_poll"], env=ENV)
        proc.wait()
        self.assertEqual(proc.returncode, 0, "CPP client exited with non-zero exit code")
        q.destroy()

    def test_full(self):
        exe = "cpp_queue"
        q = Queue()
        ser_q = q.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_q, "test_full"], env=ENV)
        proc.wait()
        self.assertEqual(proc.returncode, 0, "CPP client exited with non-zero exit code")
        q.destroy()

    def test_full_from_filled_queue(self):
        exe = "cpp_queue"
        maxsize = 5
        q = Queue(maxsize=maxsize)
        ser_q = q.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_q, "test_full_from_filled_queue", maxsize], env=ENV)
        proc.wait()
        self.assertEqual(proc.returncode, 0, "CPP client exited with non-zero exit code")
        q.destroy()

    def test_empty(self):
        exe = "cpp_queue"
        q = Queue()
        ser_q = q.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_q, "test_empty"], env=ENV)
        proc.wait()
        self.assertEqual(proc.returncode, 0, "CPP client exited with non-zero exit code")
        q.destroy()

    def test_size(self):
        exe = "cpp_queue"
        q = Queue()
        ser_q = q.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_q, "test_size"], env=ENV)
        proc.wait()
        self.assertEqual(proc.returncode, 0, "CPP client exited with non-zero exit code")
        q.destroy()

    def test_task_done(self):
        exe = "cpp_queue"
        q = Queue(joinable=True)

        proc = mp.Process(target=_put_and_join, args=(q, "hello"))
        proc.start()

        proc.join(timeout=0)  # porcess is blocked here
        self.assertEqual(proc.exitcode, None)

        ser_q = q.serialize()
        cpp_proc = Popen(executable=str(test_dir / exe), args=[ser_q, "test_task_done"], env=ENV)
        cpp_proc.wait()  # cpp client unblock the process
        self.assertEqual(cpp_proc.returncode, 0, "CPP client exited with non-zero exit code")

        proc.join(timeout=None)
        self.assertEqual(proc.exitcode, 0)

        q.destroy()

    def test_multiple_task_done(self):
        exe = "cpp_queue"
        q = Queue(joinable=True)
        num_puts = 10
        proc = mp.Process(target=_multiple_puts_and_join, args=(q, num_puts, "hello"))
        proc.start()
        proc.join(timeout=0)  # porcess is blocked here
        self.assertEqual(proc.exitcode, None)

        ser_q = q.serialize()
        cpp_proc = Popen(executable=str(test_dir / exe), args=[ser_q, "test_multiple_task_done", num_puts], env=ENV)
        cpp_proc.wait()  # cpp client unblock the process
        self.assertEqual(cpp_proc.returncode, 0, "CPP client exited with non-zero exit code")

        proc.join(timeout=None)
        self.assertEqual(proc.exitcode, 0)

        q.destroy()

    def test_join(self):
        """Add a task to queue and mark it done from python client, and join from CPP client."""
        exe = "cpp_queue"
        q = Queue(joinable=True)

        q.put("hello")

        ser_q = q.serialize()
        cpp_proc = Popen(executable=str(test_dir / exe), args=[ser_q, "test_join"], env=ENV)
        cpp_proc.wait(timeout=0)
        self.assertEqual(cpp_proc.returncode, None, "CPP client should return None exit code here")

        q.task_done()  # unblock cpp client

        cpp_proc.wait(timeout=None)
        self.assertEqual(cpp_proc.returncode, 0, "CPP client exited with non-zero exit code")
        q.destroy()

    def test_new_task(self):
        exe = "cpp_queue"
        q = Queue(joinable=True)

        proc = mp.Process(target=_task_done_and_join, args=(q,))
        proc.start()
        proc.join(timeout=0)  # porcess is blocked here
        self.assertEqual(proc.exitcode, None)

        ser_q = q.serialize()
        cpp_proc = Popen(executable=str(test_dir / exe), args=[ser_q, "test_new_task"], env=ENV)
        cpp_proc.wait(timeout=None)  # unblock the process
        self.assertEqual(cpp_proc.returncode, 0, "CPP client exited with non-zero exit code")

        proc.join(timeout=None)  # porcess should exit
        self.assertEqual(proc.exitcode, 0)
        q.destroy()

    def test_multiple_new_tasks(self):
        """Add a number of tasks to queue from CPP client, mark task done from python API, and join queue from CPP client."""
        exe = "cpp_queue"
        q = Queue(joinable=True)
        num_puts = 10

        ser_q = q.serialize()
        cpp_proc = Popen(executable=str(test_dir / exe), args=[ser_q, "test_multiple_new_tasks", num_puts], env=ENV)
        cpp_proc.wait(timeout=0)
        self.assertEqual(cpp_proc.returncode, None, "CPP client should return None exit code here")

        # unblock cpp client
        for i in range(num_puts):
            q.task_done()

        cpp_proc.wait(timeout=None)
        self.assertEqual(cpp_proc.returncode, 0, "CPP client exited with non-zero exit code")
        q.destroy()

    def test_new_tasks_and_task_done_and_join(self):
        """Add and mark tasks done and join from CPP."""
        exe = "cpp_queue"
        q = Queue(joinable=True)
        ser_q = q.serialize()
        cpp_proc = Popen(executable=str(test_dir / exe), args=[ser_q, "test_new_tasks_and_task_done_and_join"], env=ENV)
        cpp_proc.wait()
        self.assertEqual(cpp_proc.returncode, 0, "CPP client exited with non-zero exit code")
        q.destroy()

    def test_custom_pickler_dump(self):
        exe = "cpp_queue"
        q = Queue(buffered=False, pickler=numPy2dPickler((2, 3), np.double), num_streams=1)
        arr = [[0.12, 0.31, 3.4], [4.579, 5.98, 6.54]]
        value = np.array(arr)
        q.put(value)
        ser_q = q.serialize()
        cpp_proc = Popen(executable=str(test_dir / exe), args=[ser_q, "test_custom_pickler_dump"], env=ENV)
        cpp_proc.wait()
        self.assertEqual(cpp_proc.returncode, 0, "CPP client exited with non-zero exit code")
        q.destroy()

    def test_custom_pickler_load(self):
        exe = "cpp_queue"
        q = Queue(buffered=False, pickler=numPy2dPickler((2, 3), np.double), num_streams=1)
        arr = [[0.12, 0.31, 3.4], [4.579, 5.98, 6.54]]
        ser_q = q.serialize()
        cpp_proc = Popen(executable=str(test_dir / exe), args=[ser_q, "test_custom_pickler_load"], env=ENV)
        cpp_proc.wait()
        self.assertEqual(cpp_proc.returncode, 0, "CPP client exited with non-zero exit code")
        recv_arr = q.get()
        self.assertTrue(np.array_equal(arr, recv_arr))
        q.destroy()

    def test_2d_vector_put(self):
        exe = "cpp_queue"
        q = Queue(buffered=False, num_streams=1)
        ser_q = q.serialize()
        cpp_proc = Popen(executable=str(test_dir / exe), args=[ser_q, "test_2d_vector_put"], env=ENV)
        cpp_proc.wait()
        self.assertEqual(cpp_proc.returncode, 0, "CPP client exited with non-zero exit code")
        q.destroy()


if __name__ == "__main__":
    mp.set_start_method("dragon")
    unittest.main()
