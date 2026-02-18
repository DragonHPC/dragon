import os
import unittest

from dragon.workflows.batch import Batch
from dragon.native.machine import System

from user_functions import (
    supersingular_primes,
    get_ddict,
    get_prime,
    next_idx,
    update_dict,
)


# TODO: add test where a batch function gets a handle to the batch instance
# and runs a new batch functions


class TestParameterizedTaskDescriptor(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.nnodes = System().nnodes
        print(f"{cls.nnodes=}")

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

        update_ddict = self.batch.import_func(
            "function_w_deps.yml", update_dict, next_idx
        )

        # init dict and ddict
        the_ddict = get_ddict(self.batch, num_iters)
        the_dict = {}
        num_primes = len(supersingular_primes)

        for i in range(num_items):
            the_ddict[i] = supersingular_primes[i % num_primes]
            the_dict[i] = the_ddict[i]

        # compute using tasks
        for _ in range(num_iters):
            for i in range(num_items):
                update_ddict(the_ddict, i, num_items)

        # compute locally
        for _ in range(num_iters):
            for i in range(num_items):
                update_dict(the_dict, i, num_items)

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

        with self.batch.open(f"process_w_deps_data_0", "w") as file_in:
            file_in.write(f"{val_in}")

        num_iter = 8
        for i in range(num_iter):
            proc(
                arg_in,
                input_file=f"process_w_deps_data_{i}",
                output_file=f"process_w_deps_data_{i + 1}",
            )

        with self.batch.open(f"process_w_deps_data_{num_iter}", "r") as file_out:
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

        with self.batch.open(f"process_w_deps_data_in", "w") as file_in:
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

        with self.batch.open(f"job_w_deps_data_0", "w") as file_in:
            file_in.write(f"{val_in}")

        num_iter = 8
        for i in range(num_iter):
            proc(
                arg_in,
                input_file=f"job_w_deps_data_{i}",
                output_file=f"job_w_deps_data_{i + 1}",
            )

        with self.batch.open(f"job_w_deps_data_{num_iter}", "r") as file_out:
            val_out = file_out.read()

        self.assertEqual(val_out, num_iter * arg_in / 2.0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
