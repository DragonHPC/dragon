import unittest
import sys
import time
import os

from contextlib import redirect_stdout
from io import StringIO

from dragon.native.queue import Queue
from dragon.native.process import Process, ProcessError, current, ProcessTemplate
from dragon.infrastructure.parameters import this_process
from dragon.infrastructure.process_desc import ProcessOptions
from dragon.infrastructure.policy import Policy
from dragon.native.array import Array
from dragon.native.value import Value



def simple_mod_2x(v, A):
    v.value *= 2
    for idx, _ in enumerate(A):
        A[idx] *= 2


def exception_raiser():
    raise RuntimeError("Intentional Exception")


class TestDragonNativeProcess(unittest.TestCase):
    def test_basic(self):
        exe = "sleep"
        args = ("10000",)

        p = Process(exe, args, ident="Apple")  # creates a new process
        p.start()
        self.assertTrue(p.is_alive)
        self.assertTrue(p.ident == "Apple")

        p2 = Process(None, None, ident="Apple")
        self.assertTrue(p.name == p2.name)
        self.assertTrue(p.puid == p2.puid)

        pp = p.parent()
        self.assertTrue(pp.puid == this_process.my_puid)
        me = current()
        self.assertTrue(me.puid == this_process.my_puid)

        p.kill()

        p2.join(timeout=None)
        self.assertTrue(p.is_alive == False)
        self.assertTrue(p2.is_alive == False)

        self.assertRaises(AttributeError, Process, "gobbledygook")

    def test_err_handling(self):
        p = Process(target=exception_raiser, args=(), stderr=Process.PIPE)
        p.start()
        msg = ""
        try:
            while True:
                txt = p.stderr_conn.recv()
                msg += txt
        except EOFError:
            pass

        p.stderr_conn.close()
        p.join()
        self.assertIn("RuntimeError: Intentional Exception", msg)

    def test_subclassing(self):
        class UserProcess(Process):
            pass

        self.assertRaises(NotImplementedError, UserProcess, "sleep")

    @classmethod
    def putter(cls, putter_q, getter_q):
        putter_q.put(True)
        _ = getter_q.get()

    @classmethod
    def raiser(cls):
        raise Exception("Bad Function - ignore")

    @classmethod
    def exiter(cls):
        sys.exit(42)

    @classmethod
    def sleeper(cls):
        time.sleep(1000000)

    def test_basic_python(self):
        putter_q = Queue()
        getter_q = Queue()

        pyproc = Process(self.putter, args=(putter_q, getter_q), ident="Pear")
        pyproc.start()

        item = putter_q.get()
        self.assertTrue(item == True)
        self.assertTrue(pyproc.is_alive == True)

        getter_q.put(True)

        pyproc.join()

        self.assertTrue(pyproc.is_alive == False)
        self.assertTrue(pyproc.returncode == 0)

    def test_exception_handling(self):
        pyproc = Process(self.raiser)
        pyproc.start()
        pyproc.join()
        self.assertTrue(pyproc.returncode == 1)

    def test_exit_handling(self):
        pyproc = Process(self.exiter)
        pyproc.start()
        pyproc.join()
        self.assertTrue(pyproc.returncode == 42)

    def test_kill(self):
        pyproc = Process(self.sleeper)
        pyproc.start()
        self.assertTrue(pyproc.is_alive == True)
        pyproc.kill()
        pyproc.join(timeout=None)

        pyproc = Process(self.exiter)
        pyproc.start()
        pyproc.join()
        self.assertTrue(pyproc.returncode == 42)

    def test_templating_basic(self):
        exe = "sleep"
        args = ("10000",)

        templ = ProcessTemplate(exe, args)

        p = Process.from_template(templ, ident="Banana")  # creates a new process

        p.start()
        self.assertTrue(p.is_alive)
        self.assertTrue(p.ident == "Banana")
        p.kill()
        p.join()

    def test_templating_no_args(self):
        exe = "pwd"
        cwd = os.getcwd()

        templ = ProcessTemplate(exe, stdout=ProcessTemplate.PIPE, stderr=ProcessTemplate.DEVNULL)

        p = Process.from_template(templ)
        p.start()

        # Make sure everything completes in a sensible fashion
        msg = ""
        try:
            while True:
                txt = p.stdout_conn.recv()
                msg += txt
        except EOFError:
            pass
        p.join()

        self.assertEqual(p.returncode, 0)
        self.assertEqual(msg.strip(), cwd)

    def test_template_python_exe_with_infra(self):
        exe = sys.executable
        args = ["-c", "import dragon; import multiprocessing as mp; mp.set_start_method('dragon'); q = mp.Queue()"]

        templ = ProcessTemplate(exe, args, options=ProcessOptions(make_inf_channels=True))
        p = Process.from_template(templ)

        p.start()
        p.join()
        self.assertEqual(p.returncode, 0)

    def test_policy(self):
        ref_val = 0.5
        ref_arr = [0.1, 0.2, 0.3]
        v = Value("d", ref_val)
        A = Array("d", ref_arr)
        policy = Policy(distribution=Policy.Distribution.DEFAULT, cpu_affinity=5)
        p = Process(target=simple_mod_2x, args=(v, A), policy=policy)
        self.assertRaises(ProcessError, p.start)

    def test_value_array(self):
        ref_val = 0.5
        ref_arr = [0.1, 0.2, 0.3]
        v = Value("d", ref_val)
        A = Array("d", ref_arr)

        p = Process(target=simple_mod_2x, args=(v, A))
        p.start()
        p.join()

        self.assertEqual(v.value, ref_val * 2)
        self.assertEqual([a for a in A], [x * 2 for x in ref_arr])

    def test_templating_python(self):
        putter_q = Queue()
        getter_q = Queue()

        templ = ProcessTemplate(self.putter, args=(putter_q, getter_q))
        func, args, kwargs = templ.get_original_python_parameters()
        self.assertTrue(callable(func))
        self.assertIsInstance(args[0], Queue)
        self.assertIsInstance(args, tuple)
        self.assertTrue(kwargs == {})

        self.assertTrue(templ.is_python)

        pyproc = Process.from_template(templ, ident="Pineapple")

        pyproc.start()

        item = putter_q.get()
        self.assertTrue(item == True)
        self.assertTrue(pyproc.is_alive == True)

        getter_q.put(True)

        pyproc.join()

        self.assertTrue(pyproc.is_alive == False)
        self.assertTrue(pyproc.returncode == 0)

    def test_proc_cwd(self):
        def test_cwd(temp_cwd):
            temp_cwd.value = os.getcwd().encode("utf-8")

        initial_cwd = os.getcwd()
        os.chdir("..")
        head_cwd = os.getcwd()

        # use a mutable object
        temp_cwd = Array("c", 256)
        temp_cwd.value = os.getcwd().encode("utf-8")

        p = Process(target=test_cwd, args=(temp_cwd,))
        p.start()
        p.join()

        self.assertEqual(head_cwd, temp_cwd.value.decode().rstrip("\x00"))

        # re-set temp_cwd
        temp_cwd.value = os.getcwd().encode("utf-8")

        templ = ProcessTemplate(target=test_cwd, args=(temp_cwd,))
        p1 = Process.from_template(templ)
        p1.start()
        p1.join()

        self.assertEqual(head_cwd, temp_cwd.value.decode().rstrip("\x00"))

        # bring back the cwd to its original value
        os.chdir(initial_cwd)


if __name__ == "__main__":
    unittest.main()
