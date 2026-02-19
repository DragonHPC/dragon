import os
import unittest
import pathlib
import dragon
import multiprocessing as mp
from dragon.native.process import Popen
from dragon.infrastructure.facts import DRAGON_LIB_DIR
from dragon.native.barrier import Barrier

test_dir = pathlib.Path(__file__).resolve().parent
os.system(f"cd {test_dir}; make --silent")

ENV = dict(os.environ)
ENV["LD_LIBRARY_PATH"] = str(DRAGON_LIB_DIR) + ":" + str(ENV.get("LD_LIBRARY_PATH", ""))
ENV["DYLD_FALLBACK_LIBRARY_PATH"] = str(DRAGON_LIB_DIR) + ":" + str(ENV.get("DYLD_FALLBACK_LIBRARY_PATH", ""))

def wait_func(b1):
    b1.wait()


class TestBarrierCPP(unittest.TestCase):

    def test_attach_detach(self):
        exe = "cpp_barrier"
        barrier = Barrier()
        ser_barrier = barrier.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_barrier, "test_attach_detach"], env=ENV)
        proc.wait()
        self.assertEqual(proc.returncode, 0, "CPP client exited with non-zero exit code")

    def test_attach_by_ch_descr(self):
        exe = "cpp_barrier"
        barrier = Barrier()
        ser_barrier = barrier.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_barrier, "test_attach_by_ch_descr"], env=ENV)
        proc.wait()
        self.assertEqual(proc.returncode, 0, "CPP client exited with non-zero exit code")

    def test_serialize(self):
        exe = "cpp_barrier"
        barrier = Barrier()
        ser_barrier = barrier.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_barrier, "test_serialize"], env=ENV)
        proc.wait()
        self.assertEqual(proc.returncode, 0, "CPP client exited with non-zero exit code")

    def test_get_parties(self):
        exe = "cpp_barrier"
        num_parties = 8
        barrier = Barrier(parties=num_parties)
        ser_barrier = barrier.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_barrier, "test_get_parties", num_parties], env=ENV)
        proc.wait()
        self.assertEqual(proc.returncode, 0, "CPP client exited with non-zero exit code")

    def test_single_wait(self):
        exe = "cpp_barrier"
        barrier = Barrier(parties=1)
        ser_barrier = barrier.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_barrier, "test_single_wait"], env=ENV)
        proc.wait()
        self.assertEqual(proc.returncode, 0, "CPP client exited with non-zero exit code")

    def test_multiple_wait(self):
        exe = "cpp_barrier"
        n_parties = 4
        barrier = Barrier(parties=n_parties)
        ser_barrier = barrier.serialize()

        procs = []

        for i in range(n_parties-1):
            proc = Popen(executable=str(test_dir / exe), args=[ser_barrier, "test_single_wait"], env=ENV)
            procs.append(proc)

        for proc in procs:
            proc.wait(timeout=0)
            self.assertEqual(proc.returncode, None, "CPP client should not exit here.")

        last_proc = Popen(executable=str(test_dir / exe), args=[ser_barrier, "test_single_wait"], env=ENV)
        for proc in procs:
            proc.wait()
            self.assertEqual(proc.returncode, 0, "CPP client exited with non-zero exit code")

        last_proc.wait()
        self.assertEqual(last_proc.returncode, 0, "CPP client exited with non-zero exit code")

    def test_wait_timeout(self):
        exe = "cpp_barrier"
        barrier = Barrier(parties=2)
        ser_barrier = barrier.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_barrier, "test_wait_timeout"], env=ENV)
        proc.wait()
        self.assertEqual(proc.returncode, 0, "CPP client exited with non-zero exit code")

    def test_n_waiting(self):
        exe = "cpp_barrier"
        n_parties = 4
        b1 = Barrier(parties=n_parties)
        ser_barrier = b1.serialize()
        procs = []
        for _ in range(n_parties-1):
            proc = Popen(executable=str(test_dir / exe), args=[ser_barrier, "test_single_wait"], env=ENV)
            procs.append(proc)

        for proc in procs:
            proc.wait(timeout=0)
            self.assertEqual(proc.returncode, None, "CPP client should not exit here.")

        last_proc = Popen(executable=str(test_dir / exe), args=[ser_barrier, "test_n_waiting", n_parties-1], env=ENV)
        last_proc.wait()
        self.assertEqual(last_proc.returncode, 0, "CPP client exited with non-zero exit code")

        for proc in procs:
            proc.wait()
            self.assertEqual(proc.returncode, 0, "CPP client exited with non-zero exit code")

    def test_n_waiting_py(self):
        """
        Wait from python clients and get number of waiting processes from CPP client.
        """
        exe = "cpp_barrier"
        n_parties = 4
        b1 = Barrier(parties=n_parties)
        ser_barrier = b1.serialize()
        procs = []
        for _ in range(n_parties-1):
            proc = mp.Process(target=wait_func, args=(b1,))
            proc.start()
            procs.append(proc)

        # To avoid the CPP procs from going too fast, even before python clients finish attaching to the barrier
        import time
        time.sleep(5)

        for proc in procs:
            proc.join(timeout=0)
            self.assertEqual(None, proc.exitcode)

        last_proc = Popen(executable=str(test_dir / exe), args=[ser_barrier, "test_n_waiting", n_parties-1], env=ENV)
        last_proc.wait()
        self.assertEqual(last_proc.returncode, 0, "CPP client exited with non-zero exit code")

        for proc in procs:
            proc.join()
            self.assertEqual(0, proc.exitcode)

    def test_abort(self):
        exe = "cpp_barrier"
        barrier = Barrier(parties=2)
        ser_barrier = barrier.serialize()
        proc_wait = Popen(executable=str(test_dir / exe), args=[ser_barrier, "test_abort_reset_wait_proc"], env=ENV)
        proc = Popen(executable=str(test_dir / exe), args=[ser_barrier, "test_abort"], env=ENV)
        proc_wait.wait()
        proc.wait()
        self.assertEqual(proc_wait.returncode, 0, "CPP client exited with non-zero exit code")
        self.assertEqual(proc.returncode, 0, "CPP client exited with non-zero exit code")

    def test_reset(self):
        exe = "cpp_barrier"
        barrier = Barrier(parties=2)
        ser_barrier = barrier.serialize()
        proc_wait = Popen(executable=str(test_dir / exe), args=[ser_barrier, "test_abort_reset_wait_proc"], env=ENV)
        import time
        time.sleep(3)
        proc = Popen(executable=str(test_dir / exe), args=[ser_barrier, "test_reset"], env=ENV)
        proc.wait()
        proc_wait.wait()
        self.assertEqual(proc_wait.returncode, 0, "CPP client exited with non-zero exit code")
        self.assertEqual(proc.returncode, 0, "CPP client exited with non-zero exit code")

    def test_broken(self):
        exe = "cpp_barrier"
        barrier = Barrier(parties=2)
        ser_barrier = barrier.serialize()
        proc_wait = Popen(executable=str(test_dir / exe), args=[ser_barrier, "test_abort_reset_wait_proc"], env=ENV)
        proc = Popen(executable=str(test_dir / exe), args=[ser_barrier, "test_broken"], env=ENV)
        proc_wait.wait()
        proc.wait()
        self.assertEqual(proc_wait.returncode, 0, "CPP client exited with non-zero exit code")
        self.assertEqual(proc.returncode, 0, "CPP client exited with non-zero exit code")

    def test_abort_reset(self):
        exe = "cpp_barrier"
        n_parties = 2
        barrier = Barrier(parties=n_parties)
        ser_barrier = barrier.serialize()
        procs = []
        for _ in range(n_parties - 1):
            proc_wait = Popen(executable=str(test_dir / exe), args=[ser_barrier, "test_abort_reset_wait_proc"], env=ENV)
            procs.append(proc_wait)
        import time
        time.sleep(5)
        proc = Popen(executable=str(test_dir / exe), args=[ser_barrier, "test_abort_reset", n_parties-1], env=ENV)
        for proc_wait in procs:
            proc_wait.wait()
            self.assertEqual(proc_wait.returncode, 0, "CPP client exited with non-zero exit code")

        proc.wait()
        self.assertEqual(proc.returncode, 0, "CPP client exited with non-zero exit code")

        procs = []
        for _ in range(n_parties):
            proc = Popen(executable=str(test_dir / exe), args=[ser_barrier, "test_single_wait"], env=ENV)
            procs.append(proc)

        for proc in procs:
            proc.wait()
            self.assertEqual(proc.returncode, 0, "CPP client exited with non-zero exit code")

    def test_wait_with_action(self):

        pfile = pathlib.Path("./cpp_barrier_test_action.txt")
        if pfile.exists():
            os.remove(pfile)

        exe = "cpp_barrier"
        n_parties = 4
        barrier = Barrier(parties=n_parties)
        ser_barrier = barrier.serialize()
        procs = []
        for _ in range(n_parties):
            proc = Popen(executable=str(test_dir / exe), args=[ser_barrier, "test_wait_with_action"], env=ENV)
            procs.append(proc)

        for proc in procs:
            proc.wait()
            self.assertEqual(proc.returncode, 0, "CPP client exited with non-zero exit code")

        # check file existence
        pfile = pathlib.Path("./cpp_barrier_test_action.txt")
        self.assertTrue(pfile.exists())
        # check file content
        with open(pfile, 'r', encoding='utf-8') as file:
            content_string = file.read()
            self.assertEqual(content_string, "ActionInvoked!")

        os.remove(pfile)


if __name__ == "__main__":
    mp.set_start_method("dragon")
    unittest.main()
