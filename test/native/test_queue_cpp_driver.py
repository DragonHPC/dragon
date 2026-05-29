import os
import unittest
import dragon
from dragon.native.process import Popen
from dragon.native.queue import Queue
from dragon.infrastructure.facts import DRAGON_LIB_DIR
from dragon.channels import Channel
from dragon.utils import XNumPy2DPickler, XScalarPickler, XStringPickler
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

    def test_custom_matrix_pickler_dump(self):
        exe = "cpp_queue"
        q = Queue(buffered=False, pickler=XNumPy2DPickler(np.float64), num_streams=1)
        arr = [[0.12, 0.31, 3.4], [4.579, 5.98, 6.54]]
        value = np.array(arr)
        q.put(value)
        ser_q = q.serialize()
        cpp_proc = Popen(executable=str(test_dir / exe), args=[ser_q, "test_custom_matrix_pickler_dump"], env=ENV)
        cpp_proc.wait()
        self.assertEqual(cpp_proc.returncode, 0, "CPP client exited with non-zero exit code")
        q.destroy()

    def test_custom_matrix_pickler_load(self):
        exe = "cpp_queue"
        q = Queue(buffered=False, pickler=XNumPy2DPickler(np.float64), num_streams=1)
        arr = [[0.12, 0.31, 3.4], [4.579, 5.98, 6.54]]
        ser_q = q.serialize()
        cpp_proc = Popen(executable=str(test_dir / exe), args=[ser_q, "test_custom_matrix_pickler_load"], env=ENV)
        cpp_proc.wait()
        self.assertEqual(cpp_proc.returncode, 0, "CPP client exited with non-zero exit code")
        recv_arr = q.get()
        self.assertTrue(np.array_equal(arr, recv_arr))
        q.destroy()

    def test_custom_int_pickler_dump(self):
        exe = "cpp_queue"
        q = Queue(buffered=False, pickler=XScalarPickler(np.int32), num_streams=1)
        q.put(42)
        ser_q = q.serialize()
        cpp_proc = Popen(executable=str(test_dir / exe), args=[ser_q, "test_custom_int_pickler_dump"], env=ENV)
        cpp_proc.wait()
        self.assertEqual(cpp_proc.returncode, 0, "CPP client exited with non-zero exit code")
        q.destroy()

    def test_custom_int_pickler_load(self):
        exe = "cpp_queue"
        q = Queue(buffered=False, pickler=XScalarPickler(np.int32), num_streams=1)
        ser_q = q.serialize()
        cpp_proc = Popen(executable=str(test_dir / exe), args=[ser_q, "test_custom_int_pickler_load"], env=ENV)
        cpp_proc.wait()
        self.assertEqual(cpp_proc.returncode, 0, "CPP client exited with non-zero exit code")
        x = q.get()
        self.assertTrue(x, 42)
        q.destroy()

    def test_custom_double_pickler_dump(self):
        exe = "cpp_queue"
        q = Queue(buffered=False, pickler=XScalarPickler(np.float64), num_streams=1)
        q.put(42.0)
        ser_q = q.serialize()
        cpp_proc = Popen(executable=str(test_dir / exe), args=[ser_q, "test_custom_double_pickler_dump"], env=ENV)
        cpp_proc.wait()
        self.assertEqual(cpp_proc.returncode, 0, "CPP client exited with non-zero exit code")
        q.destroy()

    def test_custom_double_pickler_load(self):
        exe = "cpp_queue"
        q = Queue(buffered=False, pickler=XScalarPickler(np.float64), num_streams=1)
        ser_q = q.serialize()
        cpp_proc = Popen(executable=str(test_dir / exe), args=[ser_q, "test_custom_double_pickler_load"], env=ENV)
        cpp_proc.wait()
        self.assertEqual(cpp_proc.returncode, 0, "CPP client exited with non-zero exit code")
        x = q.get()
        self.assertTrue(x, 42.0)
        q.destroy()

    def test_custom_str_pickler_dump(self):
        exe = "cpp_queue"
        q = Queue(buffered=False, pickler=XStringPickler(), num_streams=1)
        q.put("hello world")
        ser_q = q.serialize()
        cpp_proc = Popen(executable=str(test_dir / exe), args=[ser_q, "test_custom_str_pickler_dump"], env=ENV)
        cpp_proc.wait()
        self.assertEqual(cpp_proc.returncode, 0, "CPP client exited with non-zero exit code")
        q.destroy()

    def test_custom_str_pickler_load(self):
        exe = "cpp_queue"
        q = Queue(buffered=False, pickler=XStringPickler(), num_streams=1)
        ser_q = q.serialize()
        cpp_proc = Popen(executable=str(test_dir / exe), args=[ser_q, "test_custom_str_pickler_load"], env=ENV)
        cpp_proc.wait()
        self.assertEqual(cpp_proc.returncode, 0, "CPP client exited with non-zero exit code")
        x = q.get()
        self.assertTrue(x, "hello world")
        q.destroy()

    def test_2d_vector_put(self):
        exe = "cpp_queue"
        q = Queue(buffered=False, num_streams=1)
        ser_q = q.serialize()
        cpp_proc = Popen(executable=str(test_dir / exe), args=[ser_q, "test_2d_vector_put"], env=ENV)
        cpp_proc.wait()
        self.assertEqual(cpp_proc.returncode, 0, "CPP client exited with non-zero exit code")
        q.destroy()

    def test_custom_matrix_dumps_loads(self):
        arr = [[0.12, 0.31, 3.4], [4.579, 5.98, 6.54]]
        value = np.array(arr)
        pickler = XNumPy2DPickler(np.float64)
        value_bytes = pickler.dumps(value)
        arr2 = pickler.loads(value_bytes)
        self.assertTrue(np.array_equal(arr, arr2))

if __name__ == "__main__":
    mp.set_start_method("dragon")
    unittest.main()
