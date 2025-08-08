import os
import unittest
import dragon
from dragon.native.process import Popen
from dragon.data.ddict.ddict import DDict
from dragon.native.machine import System, Node
from dragon.infrastructure.policy import Policy
from dragon.infrastructure.facts import DRAGON_LIB_DIR
from dragon.utils import b64encode
import multiprocessing as mp
import cloudpickle
import pathlib
import numpy as np
import sys
import math
import ctypes

test_dir = pathlib.Path(__file__).resolve().parent
os.system(f"cd {test_dir}; make --silent")

ENV = dict(os.environ)
ENV["LD_LIBRARY_PATH"] = str(DRAGON_LIB_DIR) + ":" + str(ENV.get("LD_LIBRARY_PATH", ""))


class numPy2dValuePickler:

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


class intKeyPickler:

    def __init__(self, num_bytes=ctypes.sizeof(ctypes.c_int)):
        self._num_bytes = num_bytes

    def dumps(self, val: int) -> bytearray:
        return val.to_bytes(self._num_bytes, byteorder=sys.byteorder)

    def loads(self, val: bytearray) -> int:
        return int.from_bytes(val, byteorder=sys.byteorder)


class TestDDictCPP(unittest.TestCase):
    def test_serialize(self):
        exe = "cpp_ddict"
        ddict = DDict(2, 1, 3000000)
        ser_ddict = ddict.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_serialize"], env=ENV)
        proc.wait()
        ddict.destroy()
        self.assertEqual(proc.returncode, 0, "CPP client exited with non-zero exit code")

    def test_attach_detach(self):
        exe = "cpp_ddict"
        ddict = DDict(2, 1, 3000000)
        ser_ddict = ddict.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_attach_detach"], env=ENV)
        proc.wait()
        ddict.destroy()
        self.assertEqual(proc.returncode, 0, "CPP client exited with non-zero exit code")

    def test_length(self):
        exe = "cpp_ddict"
        ddict = DDict(2, 1, 3000000)
        ddict["hello"] = "world"
        ddict["dragon"] = "runtime"
        ddict["Miska"] = "smart"
        ser_ddict = ddict.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_length"], env=ENV)
        proc.wait()
        ddict.destroy()
        self.assertEqual(proc.returncode, 0, "CPP client exited with non-zero exit code")

    def test_clear(self):
        exe = "cpp_ddict"
        ddict = DDict(2, 1, 3000000, trace=True)
        ser_ddict = ddict.serialize()
        ddict["hello"] = "world"
        ddict["dragon"] = "runtime"
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_clear"], env=ENV)
        proc.wait()
        ddict.destroy()
        self.assertEqual(proc.returncode, 0, "CPP client exited with non-zero exit code")

    def test_put_and_get(self):
        exe = "cpp_ddict"
        ddict = DDict(2, 1, 3000000)
        ser_ddict = ddict.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_put_and_get"], env=ENV)
        proc.wait()
        ddict.destroy()
        self.assertEqual(proc.returncode, 0, "CPP client exited with non-zero exit code")

    def test_pput(self):
        exe = "cpp_ddict"
        ddict = DDict(2, 1, 3000000, trace=True)
        ser_ddict = ddict.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_pput"], env=ENV)
        proc.wait()
        ddict.destroy()
        self.assertEqual(proc.returncode, 0, "CPP client exited with non-zero exit code")

    def test_contains_existing_key(self):
        exe = "cpp_ddict"
        ddict = DDict(2, 1, 3000000, trace=True)
        ser_ddict = ddict.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_contains_existing_key"], env=ENV)
        proc.wait()
        ddict.destroy()
        self.assertEqual(proc.returncode, 0, "CPP client exited with non-zero exit code")

    def test_contains_non_existing_key(self):
        exe = "cpp_ddict"
        ddict = DDict(2, 1, 3000000, trace=True)
        ser_ddict = ddict.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_contains_non_existing_key"], env=ENV)
        proc.wait()
        ddict.destroy()
        self.assertEqual(proc.returncode, 0, "CPP client exited with non-zero exit code")

    def test_erase_existing_key(self):
        exe = "cpp_ddict"
        ddict = DDict(2, 1, 3000000, trace=True)
        ser_ddict = ddict.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_erase_existing_key"], env=ENV)
        proc.wait()
        ddict.destroy()
        self.assertEqual(proc.returncode, 0, "CPP client exited with non-zero exit code")

    def test_erase_non_existing_key(self):
        exe = "cpp_ddict"
        ddict = DDict(2, 1, 3000000, trace=True)
        ser_ddict = ddict.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_erase_non_existing_key"], env=ENV)
        proc.wait()
        ddict.destroy()
        self.assertEqual(proc.returncode, 0, "CPP client exited with non-zero exit code")

    def test_keys(self):
        exe = "cpp_ddict"
        ddict = DDict(2, 1, 3000000)
        ser_ddict = ddict.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_keys"], env=ENV)
        proc.wait()
        ddict.destroy()
        self.assertEqual(proc.returncode, 0, "CPP client exited with non-zero exit code")

    def test_checkpoint(self):
        exe = "cpp_ddict"
        ddict = DDict(2, 1, 3000000, wait_for_keys=True, working_set_size=2, trace=True)
        ser_ddict = ddict.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_checkpoint"], env=ENV)
        proc.wait()
        ddict.destroy()
        self.assertEqual(proc.returncode, 0, "CPP client exited with non-zero exit code")

    def test_rollback(self):
        exe = "cpp_ddict"
        ddict = DDict(2, 1, 3000000, wait_for_keys=True, working_set_size=2, trace=True)
        ser_ddict = ddict.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_rollback"], env=ENV)
        proc.wait()
        ddict.destroy()
        self.assertEqual(proc.returncode, 0, "CPP client exited with non-zero exit code")

    def test_sync_to_newest_checkpoint(self):
        exe = "cpp_ddict"
        ddict = DDict(2, 1, 3000000, wait_for_keys=True, working_set_size=2, trace=True)
        ddict.checkpoint()
        ddict.pput("hello", "world")
        ddict.checkpoint()
        ddict.pput("hello0", "world0")
        ser_ddict = ddict.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_sync_to_newest_checkpoint"], env=ENV)
        proc.wait()
        ddict.destroy()
        self.assertEqual(proc.returncode, 0, "CPP client exited with non-zero exit code")

    def test_checkpoint_id(self):
        exe = "cpp_ddict"
        ddict = DDict(2, 1, 3000000, wait_for_keys=True, working_set_size=2, trace=True)
        ser_ddict = ddict.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_checkpoint_id"], env=ENV)
        proc.wait()
        ddict.destroy()
        self.assertEqual(proc.returncode, 0, "CPP client exited with non-zero exit code")

    def test_local_manager(self):
        exe = "cpp_ddict"
        ddict = DDict(2, 1, 3000000, trace=True)
        local_manager = ddict.local_manager
        ser_ddict = ddict.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_local_manager", local_manager], env=ENV)
        proc.wait()
        ddict.destroy()
        self.assertEqual(proc.returncode, 0, "CPP client exited with non-zero exit code")

    def test_main_manager(self):
        exe = "cpp_ddict"
        ddict = DDict(2, 1, 3000000, trace=True)
        main_manager = ddict.main_manager
        ser_ddict = ddict.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_main_manager", main_manager], env=ENV)
        proc.wait()
        ddict.destroy()
        self.assertEqual(proc.returncode, 0, "CPP client exited with non-zero exit code")

    def test_custom_manager(self):
        exe = "cpp_ddict"
        ddict = DDict(2, 1, 3000000)
        ser_ddict = ddict.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_custom_manager"], env=ENV)
        proc.wait()
        ddict.destroy()
        self.assertEqual(proc.returncode, 0, "CPP client exited with non-zero exit code")

    def test_empty_managers(self):
        exe = "cpp_ddict"
        ddict = DDict(2, 1, 3000000)
        ser_ddict = ddict.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_empty_managers"], env=ENV)
        proc.wait()
        ddict.destroy()
        self.assertEqual(proc.returncode, 0, "CPP client exited with non-zero exit code")

    def test_local_managers(self):
        exe = "cpp_ddict"
        ddict = DDict(2, 1, 3000000)
        ser_ddict = ddict.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_local_managers"], env=ENV)
        proc.wait()
        ddict.destroy()
        self.assertEqual(proc.returncode, 0, "CPP client exited with non-zero exit code")

    def test_local_keys(self):
        exe = "cpp_ddict"
        ddict = DDict(2, 1, 3000000)
        INT_NUM_BYTES = ctypes.sizeof(ctypes.c_int)
        with ddict.pickler(intKeyPickler(INT_NUM_BYTES), None) as type_dd:
            type_dd[1] = 2
            type_dd[0] = 1

        ser_ddict = ddict.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_local_keys"], env=ENV)
        proc.wait()
        ddict.destroy()
        self.assertEqual(proc.returncode, 0, "CPP client exited with non-zero exit code")

    def test_synchronize(self):
        exe = "cpp_ddict"
        ddict1 = DDict(2, 1, 3000000, trace=True)
        ddict2 = DDict(2, 1, 3000000, trace=True)
        ddict3 = DDict(2, 1, 3000000, trace=True)

        ddict1["hello"] = "world"
        ddict2["hello2"] = "world"
        ddict3["hello3"] = "world"
        ddict2._mark_as_drained(0)
        ddict2._mark_as_drained(1)
        ddict3._mark_as_drained(0)
        ddict3._mark_as_drained(1)

        ser_ddict = ddict1.serialize()
        proc = Popen(
            executable=str(test_dir / exe),
            args=[ser_ddict, "test_synchronize", 3, ddict1.serialize(), ddict2.serialize(), ddict3.serialize()],
            env=ENV,
        )

        proc.wait()
        self.assertEqual(proc.returncode, 0, "C client exited with non-zero exit code")

        self.assertTrue("hello" in ddict2)
        self.assertTrue("hello" in ddict3)
        self.assertTrue("hello2" not in ddict2)
        self.assertTrue("hello3" not in ddict3)

        ddict1.destroy()
        ddict2.destroy()
        ddict3.destroy()

    def test_clone(self):
        exe = "cpp_ddict"
        d = DDict(2, 1, 3000000, trace=True)
        d_clone = DDict(2, 1, 3000000, trace=True)
        d_clone_1 = DDict(2, 1, 3000000, trace=True)

        d["dragon"] = "runtime"
        d["hello"] = "world"
        d[100] = 1000
        d["ddict"] = "test"
        d_clone["test_key"] = "test_val"
        d_clone_1["test_key_1"] = "test_val_1"

        ser_ddict = d.serialize()
        proc = Popen(
            executable=str(test_dir / exe),
            args=[ser_ddict, "test_clone", 2, d_clone.serialize(), d_clone_1.serialize()],
            env=ENV,
        )

        proc.wait()
        self.assertEqual(proc.returncode, 0, "CPP client exited with non-zero exit code")

        self.assertEqual(d_clone["dragon"], "runtime")
        self.assertEqual(d_clone["hello"], "world")
        self.assertEqual(d_clone[100], 1000)
        self.assertEqual(d_clone["ddict"], "test")
        self.assertFalse("test_key" in d_clone)

        self.assertEqual(d_clone_1["dragon"], "runtime")
        self.assertEqual(d_clone_1["hello"], "world")
        self.assertEqual(d_clone_1[100], 1000)
        self.assertEqual(d_clone_1["ddict"], "test")
        self.assertFalse("test_key_1" in d_clone_1)

        d.destroy()
        d_clone.destroy()
        d_clone_1.destroy()

    def test_write_np_arr(self):
        """
        Write an array (2-D vector) through C++ ddict API and read from Python ddict client as a 2D NumPy array.
        """
        exe = "cpp_ddict"
        ddict = DDict(2, 1, 3000000)
        ser_ddict = ddict.serialize()

        INT_NUM_BYTES = ctypes.sizeof(ctypes.c_int)  # number of bytes for a C++ integer, typically 4, depending on system and compiler

        # Write the numpy array through C++ client API
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_write_np_arr"], env=ENV)
        proc.wait()
        self.assertEqual(proc.returncode, 0, "CPP client exited with non-zero exit code")

        # Read the numpy array written by C++ client
        key = 32
        arr = [[1.5, 2.5, 3.5], [4.5, 5.5, 6.5]]
        value = np.array(arr)
        with ddict.pickler(intKeyPickler(INT_NUM_BYTES), numPy2dValuePickler((2, 3), np.double)) as type_dd:
            data = type_dd[key]
            print(f"NumPy array written from C++ client: \n{data}", flush=True)
            self.assertTrue(np.array_equal(data, value))

        ddict.destroy()

    def test_read_np_arr(self):
        """
        Write a 2D NumPy array from Python client using customized pickler and read it as a 2D vector through C++ client API.
        """
        exe = "cpp_ddict"
        ddict = DDict(2, 1, 3000000)
        ser_ddict = ddict.serialize()

        INT_NUM_BYTES = ctypes.sizeof(ctypes.c_int)  # number of bytes for a C++ integer, typically 4, depending on system and compiler

        # Write numpy through python ddict client API and read from C++ client
        with ddict.pickler(intKeyPickler(INT_NUM_BYTES), numPy2dValuePickler((2, 3), np.double)) as type_dd:
            key = 2048
            arr = [[0.12, 0.31, 3.4], [4.579, 5.98, 6.54]]
            value = np.array(arr)
            type_dd[key] = value
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_read_np_arr"], env=ENV)
        proc.wait()
        self.assertEqual(proc.returncode, 0, "CPP client exited with non-zero exit code")

        ddict.destroy()

    def test_keys_read_from_py(self):
        """
        Write few keys through C++ API and read keys from Python client API using user-defined pickler.
        """
        exe = "cpp_ddict"
        ddict = DDict(2, 1, 3000000)
        ser_ddict = ddict.serialize()

        INT_NUM_BYTES = ctypes.sizeof(ctypes.c_int)  # number of bytes for a C++ integer, typically 4, depending on system and compiler

        # Write the array through C++ client API
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_keys_read_from_py"], env=ENV)
        proc.wait()
        self.assertEqual(proc.returncode, 0, "CPP client exited with non-zero exit code")

        # Get list of integer keys written by C++ client and Python client
        expected_keys = {32, 1024, 9876, 2048}
        with ddict.pickler(intKeyPickler(INT_NUM_BYTES), None) as type_dd:
            type_dd[32] = 0
            keys = type_dd.keys()
            received_keys = set(keys)
            self.assertEqual(expected_keys, received_keys)

        ddict.destroy()


if __name__ == "__main__":
    mp.set_start_method("dragon")
    unittest.main()
