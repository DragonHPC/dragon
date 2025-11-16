import os
import unittest

from dragon.native.process import ProcessTemplate
from dragon.workflows.batch import Batch, SubmitAfterCloseError
from dragon.native.machine import System, Node
from pathlib import Path
from user_functions import (
    cider_vs_juice,
    darn,
    hi,
    check_exit_code,
    check_gpu_affinity,
    foo_result,
    foo_stdout,
    foo_stderr,
    get_fib_sequence,
    print_it,
    print_several,
)


# TODO: add test where a batch function gets a handle to the batch instance
# and runs a new batch functions


class TestArgDeps(unittest.TestCase):

    def setUp(self):
        self.batch = Batch(disable_background_batching=True)
        return super().setUp()

    def tearDown(self):
        self.batch.close()
        self.batch.join()
        return super().tearDown()

    def test_result_dep(self):
        task_foo = self.batch.function(foo_result, hi)
        task_print = self.batch.function(print_it, 42, task_foo.result, 1729)

        self.batch.compile([task_foo, task_print]).run()
        stdout = task_print.stdout.get()
        self.assertEqual(stdout.strip("\n"), hi)

    def test_stdout_dep(self):
        task_foo = self.batch.function(foo_stdout, hi)
        task_print = self.batch.function(print_it, 42, task_foo.stdout, 1729)

        self.batch.compile([task_foo, task_print]).run()
        stdout = task_print.stdout.get()
        self.assertEqual(stdout.strip("\n"), hi)

    def test_stderr_dep(self):
        task_foo = self.batch.function(foo_stderr, hi)
        task_print = self.batch.function(print_it, 42, task_foo.stderr, 1729)

        self.batch.compile([task_foo, task_print]).run()
        stdout = task_print.stdout.get()
        self.assertEqual(stdout.strip("\n"), hi)

    def test_result_stdout_stderr_dep(self):
        task_foo_result = self.batch.function(foo_result, hi)
        task_foo_stdout = self.batch.function(foo_stdout, darn)
        task_foo_stderr = self.batch.function(foo_stderr, cider_vs_juice)
        task_print = self.batch.function(
            print_several,
            task_foo_result.result,
            task_foo_stdout.stdout,
            task_foo_stderr.stderr,
        )

        self.batch.compile(
            [task_foo_result, task_foo_stdout, task_foo_stderr, task_print]
        ).run()
        stdout = task_print.stdout.get()

        self.assertTrue(hi in stdout)
        self.assertTrue(darn in stdout)
        self.assertTrue(cider_vs_juice in stdout)

    def test_process_result_dep(self):
        task_pwd = self.batch.process(ProcessTemplate(target="pwd", args=()))
        task_check = self.batch.function(check_exit_code, -1, -2, task_pwd.result)

        self.batch.compile([task_pwd, task_check]).run()
        self.assertTrue(task_check.result.get())

    def test_process_stdout_dep(self):
        task_pwd = self.batch.process(ProcessTemplate(target="pwd", args=()))
        task_print = self.batch.function(print_it, 42, task_pwd.stdout, 1729)

        self.batch.compile([task_pwd, task_print]).run()
        stdout = task_print.stdout.get().strip("\n")
        self.assertEqual(stdout.strip("\n"), os.getcwd())

    @unittest.skip("hanging")
    def test_mpi_job_stdout_dep(self):
        target = "./mpi_job_stdout"
        self.assertTrue(Path(target).exists())

        mpi_job_tempate = ProcessTemplate(target=target, args=())
        task_mpi_job = self.batch.job([(4, mpi_job_tempate)])
        task_print = self.batch.function(print_it, 42, task_mpi_job.stdout, 1729)

        self.batch.compile([task_mpi_job, task_print]).run()
        stdout = task_print.stdout.get()
        self.assertTrue(hi in stdout)


class TestBatchLifecycle(unittest.TestCase):

    def setUp(self):
        self.batch = Batch(disable_background_batching=True)
        return super().setUp()

    def tearDown(self):
        self.batch.close()
        self.batch.join()
        return super().tearDown()

    def test_submit_after_close(self):
        self.batch.close()
        with self.assertRaises(SubmitAfterCloseError) as context:
            self.batch.function(foo_result).run()


class TestBatchFibonacci(unittest.TestCase):

    def setUp(self):
        self.batch = Batch(disable_background_batching=True)
        return super().setUp()

    def tearDown(self):
        self.batch.close()
        self.batch.join()
        return super().tearDown()

    def test_fib_with_ddict(self):
        fib_seq = get_fib_sequence(self.batch, use_ddict=True)

        a = 0
        b = 1

        for val in fib_seq:
            self.assertEqual(val, a + b)
            a = b
            b = val

    def test_fib_with_fs(self):
        fib_seq = get_fib_sequence(self.batch, use_ddict=False)

        a = 0
        b = 1

        for val in fib_seq:
            self.assertEqual(val, a + b)
            a = b
            b = val


class TestGPUAffinity(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.ngpus = 0
        cls.system = System()
        for node in cls.system.nodes:
            cls.ngpus += Node(node).num_gpus

    def setUp(self):
        if not TestGPUAffinity.ngpus:
            self.skipTest("No GPUs detected in the system")

        self.batch = Batch(disable_background_batching=True)
        return super().setUp()

    def tearDown(self):
        self.batch.close()
        self.batch.join()
        return super().tearDown()

    def test_gpu_affinity_sort_of(self):
        # GPU affinity for workers is currently being set randomly, so we can't
        # properly test things, but we can sort of test them by checking that
        # the affinity is at least set to *something*
        task_check = self.batch.function(check_gpu_affinity)
        task_check.run()
        self.assertTrue(task_check.result.get())


if __name__ == "__main__":
    unittest.main(verbosity=2)