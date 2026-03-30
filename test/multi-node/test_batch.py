import os
import random
import sys
import unittest

# Make batch_utils importable for this process and all worker processes spawned by Batch
_HERE = os.path.dirname(os.path.abspath(__file__)) or os.getcwd()
_BATCH_UTILS = os.path.join(_HERE, "batch_utils")
sys.path.insert(0, _BATCH_UTILS)
_existing_pythonpath = os.environ.get("PYTHONPATH", "")
os.environ["PYTHONPATH"] = f"{_BATCH_UTILS}:{_existing_pythonpath}" if _existing_pythonpath else _BATCH_UTILS

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
    mpi_f,
    mpi_job_func,
    next_idx,
    supersingular_primes,
    update_dict,
    ITERATED_DEP_MODULUS,
    iterated_return_value,
    iterated_sum_values,
    iterated_write_value,
)


def _yml(name):
    return os.path.join(_BATCH_UTILS, name)


# TODO: add test where a batch function gets a handle to the batch instance
# and runs a new batch functions


class TestArgDeps(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.batch = Batch()

    @classmethod
    def tearDownClass(cls):
        cls.batch.close()
        cls.batch.join()

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

    def test_submit_after_close(self):
        batch = Batch()
        batch.close()
        with self.assertRaises(SubmitAfterCloseError) as context:
            val = batch.function(foo_3_1, 42, 0, 0)
            val.get()
        batch.join()


class TestBatchFibonacci(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.batch = Batch()

    @classmethod
    def tearDownClass(cls):
        cls.batch.close()
        cls.batch.join()

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
        ngpus = 0
        system = System()
        for node in system.nodes:
            ngpus += Node(node).num_gpus
        if not ngpus:
            raise unittest.SkipTest("No GPUs detected in the system")
        cls.batch = Batch()

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, "batch"):
            cls.batch.close()
            cls.batch.join()

    def test_gpu_affinity_sort_of(self):
        # GPU affinity for workers is currently being set randomly, so we can't
        # properly test things, but we can sort of test them by checking that
        # the affinity is at least set to *something*
        check = self.batch.function(check_gpu_affinity)
        self.assertTrue(check.get())


class TestIteratedInterCompiledDeps(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.batch = Batch()

    @classmethod
    def tearDownClass(cls):
        cls.batch.close()
        cls.batch.join()

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
        if System().nnodes == 1:
            raise unittest.SkipTest("Not enough nodes detected in the system")
        cls.batch = Batch()

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, "batch"):
            cls.batch.close()
            cls.batch.join()

    def test_function_ptd(self):
        get_prime_wrapper = self.batch.import_func(_yml("function.yml"), get_prime)

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

        update_ddict = self.batch.import_func(_yml("function_w_deps.yml"), update_dict, next_idx)

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
            _yml("process_w_deps.yml"),
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
            _yml("process_w_deps_and_timeout.yml"),
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

    def test_job_w_deps_ptd(self):
        # TODO: need to update my_mpi_job.c to make it consistent with this test
        def foo(x: float):
            return x / 2.0

        new_env = dict(os.environ)
        new_env["ABCDEF"] = "0"

        proc = self.batch.import_func(
            _yml("job_w_deps.yml"),
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
            val_out = float(file_out.read())

        self.assertAlmostEqual(val_out, num_iter * arg_in / 2.0, delta=0.01)


class TestBatchFence(unittest.TestCase):
    """Tests for Batch.fence() and Batch.clear_results()."""

    @classmethod
    def setUpClass(cls):
        cls.batch = Batch()

    @classmethod
    def tearDownClass(cls):
        cls.batch.close()
        cls.batch.join()

    def test_fence_completes(self):
        """fence() should complete within a reasonable timeout after tasks are submitted."""
        tasks = [self.batch.function(foo_3_1, i, 0, 0) for i in range(10)]
        self.batch.fence(timeout=5)
        # all results should already be available since fence() waited for completion
        for i, task in enumerate(tasks):
            self.assertEqual(task.get(), i)

    def test_fence_completes_with_dep_chain(self):
        """fence() should wait for dependent tasks as well."""
        val1 = self.batch.function(foo_3_1, 42, None, None)
        val2 = self.batch.function(foo_3_2, 0, val1, 0)
        self.batch.fence(timeout=5)
        self.assertEqual(val2.get(), 42)

    def test_fence_clears_state(self):
        """After fence(), dep_frontier and tuid_to_manager_q should be empty."""
        for i in range(5):
            self.batch.function(foo_3_1, i, 0, 0)
        self.batch.fence(timeout=5)
        self.assertEqual(len(self.batch.dep_frontier), 0)
        self.assertEqual(len(self.batch.tuid_to_manager_q), 0)

    def test_tasks_can_be_submitted_after_fence(self):
        """Tasks submitted after a fence should complete normally."""
        val1 = self.batch.function(foo_3_1, 1, 0, 0)
        self.batch.fence(timeout=5)
        val2 = self.batch.function(foo_3_1, 2, 0, 0)
        self.assertEqual(val2.get(), 2)

    def test_clear_results_empties_ddict(self):
        """clear_results() should wait for tasks to finish, then clear the results ddict."""
        tasks = [self.batch.function(foo_3_1, i, 0, 0) for i in range(5)]
        # retrieve all results so the ddict entries exist, then clear
        for i, task in enumerate(tasks):
            self.assertEqual(task.get(), i)
        self.batch.clear_results()
        # after clear_results() the ddict should have no entries
        self.assertEqual(len(self.batch.results_ddict), 0)


def _mpi4py_available() -> bool:
    try:
        import mpi4py

        return True
    except ImportError:
        return False


class TestMPIEnsemble(unittest.TestCase):
    """Functional test based on examples/workflows/batch/mpi_ensemble.py.
    Submits a randomised ensemble of MPI jobs and verifies all exit codes are 0.
    Requires mpi4py and cray-mpich-abi to be available; if not, the test
    passes trivially with an explanatory message."""

    @classmethod
    def setUpClass(cls):
        cls.batch = Batch()

    @classmethod
    def tearDownClass(cls):
        cls.batch.close()
        cls.batch.join()

    def test_mpi_ensemble(self):
        if not _mpi4py_available():
            print(
                "mpi4py is not available (try: module load cray-mpich-abi); "
                "TestMPIEnsemble.test_mpi_ensemble trivially passed.",
                flush=True,
            )
            return

        njobs = 32
        sleepsecs = 2
        maxranks = 32

        alljobs = []
        for i in range(njobs):
            nranks = random.randint(2, maxranks)
            template = ProcessTemplate(target=mpi_f, args=(i, sleepsecs))
            alljobs.append(self.batch.job(process_templates=[(nranks, template)]))

        for i, job in enumerate(alljobs):
            exit_codes = job.get()
            if isinstance(exit_codes, list):
                self.assertTrue(
                    all(ec == 0 for ec in exit_codes),
                    msg=f"Job {i} had non-zero exit codes: {exit_codes}",
                )
            else:
                self.assertEqual(exit_codes, 0, msg=f"Job {i} failed with exit code {exit_codes}")


class TestRapidMPIJobSubmission(unittest.TestCase):
    """Regression test for the race condition where job_done_q's were recycled
    after the job leader completed but before all follower workers had finished.
    Rapidly submitting many multi-rank jobs forces queue recycling to happen in
    quick succession, reliably triggering the bug if it is present."""

    @classmethod
    def setUpClass(cls):
        cls.batch = Batch()

    @classmethod
    def tearDownClass(cls):
        cls.batch.close()
        cls.batch.join()

    def test_rapid_mpi_jobs_no_queue_recycle_race(self):
        """Submit 50 jobs of 4 ranks each without waiting between submissions,
        then verify every job completes with all-zero exit codes.
        """
        n_jobs = 50
        n_ranks = 4
        template = ProcessTemplate(target=mpi_job_func, args=())

        job_handles = []
        for _ in range(n_jobs):
            job_handles.append(self.batch.job(process_templates=[(n_ranks, template)], pmi=None))

        for i, handle in enumerate(job_handles):
            exit_codes = handle.get()
            # For multi-rank jobs get() returns a list of per-rank exit codes.
            if isinstance(exit_codes, list):
                self.assertTrue(
                    all(ec == 0 for ec in exit_codes),
                    msg=f"Job {i} had a non-zero exit code: {exit_codes}",
                )
            else:
                self.assertEqual(exit_codes, 0, msg=f"Job {i} failed with exit code {exit_codes}")


if __name__ == "__main__":
    unittest.main(verbosity=2)
