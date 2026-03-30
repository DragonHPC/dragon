import os
import unittest

from dragon.native.process import ProcessTemplate
from dragon.workflows.batch import Batch, SubmitAfterCloseError
from dragon.native.machine import System, Node
from pathlib import Path
from user_functions import (
    hi,
    check_exit_code,
    check_gpu_affinity,
    foo_3_1,
    foo_3_2,
    foo_3_3,
    foo_5_1,
    foo_5_3,
    foo_5_5,
    get_ddict,
    get_fib_sequence,
    get_prime,
    next_idx,
    supersingular_primes,
    update_dict,
    ITERATED_DEP_MODULUS,
    iterated_return_value,
    iterated_sum_values,
    iterated_write_value,
)


# TODO: add test where a batch function gets a handle to the batch instance
# and runs a new batch functions


class TestArgDeps(unittest.TestCase):

    def setUp(self):
        self.batch = Batch()
        return super().setUp()

    def tearDown(self):
        self.batch.close()
        self.batch.join()
        return super().tearDown()

    def test_dep_3_1_3_2(self):
        val1 = self.batch.function(foo_3_1, hi, None, None)
        val2 = self.batch.function(foo_3_2, 42, val1, 1729)

        self.assertEqual(val2.get(), hi)

    def test_dep_3_1_3_3(self):
        val1 = self.batch.function(foo_3_1, hi, None, None)
        val2 = self.batch.function(foo_3_3, 42, 1729, val1)

        self.assertEqual(val2.get(), hi)

    def test_dep_3_2_5_1(self):
        val1 = self.batch.function(foo_3_2, None, hi, None)
        val2 = self.batch.function(foo_5_1, val1, 42, None, 1729, None)

        self.assertEqual(val2.get(), hi)

    def test_dep_3_3_5_3(self):
        val1 = self.batch.function(foo_3_3, None, None, hi)
        val2 = self.batch.function(foo_5_3, 42, None, val1, 1729, None)

        self.assertEqual(val2.get(), hi)

    def test_dep_5_1_5_5(self):
        val1 = self.batch.function(foo_5_1, hi, None, None, None, None)
        val2 = self.batch.function(foo_5_5, 42, None, 1729, None, val1)

        self.assertEqual(val2.get(), hi)

    def test_process_result_dep(self):
        task_pwd = self.batch.process(ProcessTemplate(target="pwd", args=()))
        task_check = self.batch.function(check_exit_code, -1, -2, task_pwd)

        self.assertTrue(task_check.get())


class TestBatchLifecycle(unittest.TestCase):

    def setUp(self):
        self.batch = Batch()
        return super().setUp()

    def tearDown(self):
        self.batch.close()
        self.batch.join()
        return super().tearDown()

    def test_submit_after_close(self):
        self.batch.close()
        with self.assertRaises(SubmitAfterCloseError) as context:
            val = self.batch.function(foo_3_1, 42, 0, 0)
            val.get()


class TestBatchFibonacci(unittest.TestCase):

    def setUp(self):
        self.batch = Batch()
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

        self.batch = Batch()
        return super().setUp()

    def tearDown(self):
        self.batch.close()
        self.batch.join()
        return super().tearDown()

    def test_gpu_affinity_sort_of(self):
        # GPU affinity for workers is currently being set randomly, so we can't
        # properly test things, but we can sort of test them by checking that
        # the affinity is at least set to *something*
        check = self.batch.function(check_gpu_affinity)
        self.assertTrue(check.get())


class TestIteratedInterCompiledDeps(unittest.TestCase):

    def setUp(self):
        self.batch = Batch()
        return super().setUp()

    def tearDown(self):
        self.batch.close()
        self.batch.join()
        return super().tearDown()

    def _make_function_task(self, target, *args, reads=None, writes=None, name=None, timeout=None):
        if timeout is None:
            return self.batch.function(target, *args, reads=reads, writes=writes, name=name)

        return self.batch.function(target, *args, reads=reads, writes=writes, name=name, timeout=timeout)

    def _expected_iteration_sum(self, previous_sum: int) -> int:
        return_values_total = sum(iterated_return_value(previous_sum, offset) for offset in range(10))
        write_values_total = sum((previous_sum + offset + 11) % ITERATED_DEP_MODULUS for offset in range(100))
        return return_values_total + write_values_total

    def test_iterated_return_and_ddict_sum(self):
        shared_ddict = get_ddict(self.batch, 128)
        previous_sum = 0

        for iteration in range(100):
            return_tasks = []
            writer_keys = []

            for offset in range(10):
                task = self._make_function_task(iterated_return_value, previous_sum, offset)
                return_tasks.append(task)

            for offset in range(100):
                key = f"iter_{iteration}_offset_{offset}"
                task = self._make_function_task(
                    iterated_write_value,
                    shared_ddict,
                    key,
                    previous_sum,
                    offset,
                    writes=[self.batch.write(shared_ddict, key)],
                )
                writer_keys.append(key)

            read_deps = [self.batch.read(shared_ddict, key) for key in writer_keys]
            sum_task = self._make_function_task(
                iterated_sum_values,
                shared_ddict,
                tuple(writer_keys),
                *return_tasks,
                reads=read_deps,
            )

            expected_sum = self._expected_iteration_sum(previous_sum)
            observed_sum = sum_task.get()
            self.assertEqual(
                observed_sum,
                expected_sum,
                msg=f"unexpected sum for iteration {iteration} with previous_sum={previous_sum}",
            )
            previous_sum = observed_sum


class TestParameterizedTaskDescriptor(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.nnodes = System().nnodes

    def setUp(self):
        if TestParameterizedTaskDescriptor.nnodes == 1:
            self.skipTest("Not enough nodes detected in the system")

        self.batch = Batch()
        return super().setUp()

    def tearDown(self):
        self.batch.close()
        self.batch.join()
        return super().tearDown()

    def test_function_ptd(self):
        get_prime_wrapper = self.batch.import_func("function.yml", get_prime)

        returned_primes = []
        num_primes = len(supersingular_primes)

        for i in range(num_primes):
            val = get_prime_wrapper(i)
            returned_primes.append(val)

        for i, val in enumerate(returned_primes):
            returned_primes[i] = val.get()

        self.assertEqual(sum(returned_primes), sum(supersingular_primes))

    def test_function_w_deps_ptd(self):
        num_iters = 4
        num_items = 8

        update_ddict = self.batch.import_func("function_w_deps.yml", update_dict, next_idx)

        # init dict and ddict
        the_ddict = get_ddict(self.batch, num_iters)
        the_dict = {}
        num_primes = len(supersingular_primes)

        for i in range(num_items):
            the_ddict[i] = supersingular_primes[i % num_primes]
            the_dict[i] = the_ddict[i]

        # compute using tasks
        handles = []
        for _ in range(num_iters):
            for i in range(num_items):
                handle = update_ddict(the_ddict, i, num_items)
                handles.append(handle)

        # compute locally
        for _ in range(num_iters):
            for i in range(num_items):
                update_dict(the_dict, i, num_items)

        for handle in handles:
            handle.get()

        # assert the computed values are the same
        for i in range(num_items):
            self.assertEqual(the_dict[i], the_ddict[i])

    def test_process_w_deps_ptd(self):
        def foo(x: float):
            return x / 2.0

        new_env = dict(os.environ)
        new_env["ABCDEF"] = "0"

        proc = self.batch.import_func(
            "process_w_deps.yml",
            "my_dumb_proc",
            base_dir=".",
            env=new_env,
            update_input=foo,
        )

        val_in = 0.0
        arg_in = 3.14

        with open(f"process_w_deps_data_0", "w") as file_in:
            file_in.write(f"{val_in}")

        num_iter = 8
        return_codes = []
        for i in range(num_iter):
            rc = proc(
                arg_in,
                input_file=f"process_w_deps_data_{i}",
                output_file=f"process_w_deps_data_{i + 1}",
            )

            return_codes.append(rc)

        for rc in return_codes:
            rc.get()

        with open(f"process_w_deps_data_{num_iter}", "r") as file_out:
            val_out = float(file_out.read())

        self.assertAlmostEqual(val_out, num_iter * arg_in / 2.0, delta=0.01)

    def test_process_w_deps_and_timeout_ptd(self):
        def foo(x: float):
            return x / 2.0

        new_env = dict(os.environ)
        new_env["ABCDEF"] = "0"
        new_env["TIMEOUT_SET"] = "1"

        proc = self.batch.import_func(
            "process_w_deps_and_timeout.yml",
            "my_dumb_proc",
            base_dir=".",
            env=new_env,
            update_input=foo,
        )

        val_in = 0.0
        arg_in = 3.14

        with open(f"process_w_deps_data_in", "w") as file_in:
            file_in.write(f"{val_in}")

        rc = proc(
            arg_in,
            input_file="process_w_deps_data_in",
            output_file="process_w_deps_data_out",
        ).get()
        self.assertNotEqual(rc, 0)

    @unittest.skip("issue with starting jobs")
    def test_job_w_deps_ptd(self):
        # TODO: need to update mpi_job.c to make it consistent with this test
        def foo(x: float):
            return x / 2.0

        new_env = dict(os.environ)
        new_env["ABCDEF"] = "0"

        proc = self.batch.import_func(
            "job_w_deps.yml",
            "my_mpi_job",
            base_dir=".",
            env=new_env,
            update_input=foo,
        )

        val_in = 0
        arg_in = 3.14

        with open(f"job_w_deps_data_0", "w") as file_in:
            file_in.write(f"{val_in}")

        num_iter = 8
        return_codes = []

        for i in range(num_iter):
            rc = proc(
                arg_in,
                input_file=f"job_w_deps_data_{i}",
                output_file=f"job_w_deps_data_{i + 1}",
            )

            return_codes.append(rc)

        for rc in return_codes:
            rc.get()

        with open(f"job_w_deps_data_{num_iter}", "r") as file_out:
            val_out = file_out.read()

        self.assertEqual(val_out, num_iter * arg_in / 2.0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
