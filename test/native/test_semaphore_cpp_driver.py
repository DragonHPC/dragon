import os
import unittest
import pathlib
import dragon
import multiprocessing as mp
from dragon.native.process import Popen
from dragon.infrastructure.facts import DRAGON_LIB_DIR
from dragon.native.semaphore import Semaphore

test_dir = pathlib.Path(__file__).resolve().parent
os.system(f"cd {test_dir}; make --silent")

ENV = dict(os.environ)
ENV["LD_LIBRARY_PATH"] = str(DRAGON_LIB_DIR) + ":" + str(ENV.get("LD_LIBRARY_PATH", ""))

class TestSemaphoreCPP(unittest.TestCase):

    def test_attach_detach(self):
        exe = "cpp_semaphore"
        semaphore = Semaphore()
        ser_sem = semaphore.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_sem, "test_attach_detach"], env=ENV)
        proc.wait()
        self.assertEqual(proc.returncode, 0, "CPP client exited with non-zero exit code")

    def test_attach_by_ch_descr(self):
        exe = "cpp_semaphore"
        semaphore = Semaphore()
        ser_sem = semaphore.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_sem, "test_attach_by_ch_descr"], env=ENV)
        proc.wait()
        self.assertEqual(proc.returncode, 0, "CPP client exited with non-zero exit code")

    def test_serialize(self):
        exe = "cpp_semaphore"
        semaphore = Semaphore(value=1)
        semaphore.acquire()
        ser_sem = semaphore.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_sem, "test_serialize"], env=ENV)
        proc.wait()
        self.assertEqual(proc.returncode, 0, "CPP client exited with non-zero exit code")

    def test_acquire(self):
        exe = "cpp_semaphore"
        semaphore = Semaphore()
        ser_sem = semaphore.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_sem, "test_acquire"], env=ENV)
        proc.wait()
        self.assertEqual(proc.returncode, 0, "CPP client exited with non-zero exit code")
        semaphore.release()

    def test_release(self):
        exe = "cpp_semaphore"
        semaphore = Semaphore()
        semaphore.acquire()
        ser_sem = semaphore.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_sem, "test_release"], env=ENV)
        proc.wait()
        self.assertEqual(proc.returncode, 0, "CPP client exited with non-zero exit code")

    def test_get_value(self):
        exe = "cpp_semaphore"
        value = 10
        semaphore = Semaphore(value=value)
        ser_sem = semaphore.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_sem, "test_get_value", value], env=ENV)
        proc.wait()
        self.assertEqual(proc.returncode, 0, "CPP client exited with non-zero exit code")

    def test_release_group(self):
        exe = "cpp_semaphore"
        value = 10
        semaphore = Semaphore(value=value)
        for _ in range(value):
            semaphore.acquire()
        ser_sem = semaphore.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_sem, "test_release_group"], env=ENV)
        proc.wait()
        self.assertEqual(proc.returncode, 0, "CPP client exited with non-zero exit code")

    def test_bounded(self):
        exe = "cpp_semaphore"
        value = 10
        semaphore = Semaphore(value=value, bounded=True)
        ser_sem = semaphore.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_sem, "test_bounded"], env=ENV)
        proc.wait()
        self.assertEqual(proc.returncode, 0, "CPP client exited with non-zero exit code")

    def test_invalid_acquire(self):
        exe = "cpp_semaphore"
        semaphore = Semaphore(value=1)
        semaphore.acquire()
        ser_sem = semaphore.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_sem, "test_invalid_acquire"], env=ENV)
        proc.wait()
        self.assertEqual(proc.returncode, 0, "CPP client exited with non-zero exit code")


if __name__ == "__main__":
    mp.set_start_method("dragon")
    unittest.main()