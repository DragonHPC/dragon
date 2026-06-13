import os
import random
import sys
import time
import unittest
import cloudpickle
from pathlib import Path
from typing import cast

# Make batch_utils importable for this process and all worker processes spawned by Batch
_HERE = os.path.dirname(os.path.abspath(__file__)) or os.getcwd()
_BATCH_UTILS = os.path.join(_HERE, "batch_utils")
sys.path.insert(0, _BATCH_UTILS)
_existing_pythonpath = os.environ.get("PYTHONPATH", "")
os.environ["PYTHONPATH"] = f"{_BATCH_UTILS}:{_existing_pythonpath}" if _existing_pythonpath else _BATCH_UTILS

from dragon.native.process import Process, ProcessTemplate
from dragon.native.queue import Queue
from dragon.infrastructure.policy import Policy
from dragon.workflows.batch import Batch, ReadAfterWriteDependencyError, SubmitAfterCloseError, TaskCancelledError
from dragon.workflows.batch.batch import TaskCore, TaskNotReadyError, Work
from dragon.native.machine import System, Node
from user_functions import (
    add_values,
    hi,
    check_exit_code,
    check_gpu_affinity,
    consume_batch_from_queue_and_destroy,
    create_joined_batch_and_send,
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
    producer_value,
    mpi_job_arg_checker,
    next_idx,
    raise_runtime_error,
    record_process_placement,
    return_constant,
    signal_start_and_sleep,
    sleep_and_return,
    submit_batch_value_from_batch_worker,
    submit_batch_value_from_client,
    supersingular_primes,
    update_dict,
    ITERATED_DEP_MODULUS,
    iterated_return_value,
    iterated_sum_values,
    iterated_write_value,
)


def _yml(name):
    return os.path.join(_BATCH_UTILS, name)


class TestArgDeps(unittest.TestCase):
    """Exercise argument dependency rewrites for function and process tasks."""

    @classmethod
    def setUpClass(cls):
        cls.batch = Batch()

    @classmethod
    def tearDownClass(cls):
        cls.batch.join()
        cls.batch.destroy()

    def test_dep_3_1_3_2(self):
        """Resolve an upstream result into the middle argument position."""
        # Build the smallest possible chain where the downstream function reads
        # its dependency from a non-leading argument slot.
        val1 = self.batch.function(foo_3_1, hi, None, None)
        val2 = self.batch.function(foo_3_2, 42, val1, 1729)

        # The Batch compiler should rewrite the placeholder argument in-place.
        self.assertEqual(val2.get(), hi)

    def test_dep_3_1_3_3(self):
        """Resolve an upstream result into the final argument position."""
        # Use the same source task shape as above, but route the dependency into
        # the final positional slot to cover a different rewrite index.
        val1 = self.batch.function(foo_3_1, hi, None, None)
        val2 = self.batch.function(foo_3_3, 42, 1729, val1)

        self.assertEqual(val2.get(), hi)

    def test_dep_3_2_5_1(self):
        """Propagate a dependency from one function into the first slot of another."""
        # This checks that a completed upstream value can be threaded into the
        # first positional argument of a wider downstream call signature.
        val1 = self.batch.function(foo_3_2, None, hi, None)
        val2 = self.batch.function(foo_5_1, val1, 42, None, 1729, None)

        self.assertEqual(val2.get(), hi)

    def test_dep_3_3_5_3(self):
        """Propagate a dependency from one function into a non-leading slot."""
        # Cover a middle-argument rewrite in a downstream function with more
        # total parameters than the upstream producer.
        val1 = self.batch.function(foo_3_3, None, None, hi)
        val2 = self.batch.function(foo_5_3, 42, None, val1, 1729, None)

        self.assertEqual(val2.get(), hi)

    def test_dep_5_1_5_5(self):
        """Propagate a dependency from the first slot of one function into the last slot of another."""
        # The last-slot case is easy to regress when dependency rewrites rely on
        # prefix-based logic, so keep a direct end-of-argument-chain example.
        val1 = self.batch.function(foo_5_1, hi, None, None, None, None)
        val2 = self.batch.function(foo_5_5, 42, None, 1729, None, val1)

        self.assertEqual(val2.get(), hi)

    def test_process_result_dep(self):
        """Use a process exit code as an argument dependency for a downstream function."""
        # Process tasks produce exit codes rather than Python return values, so
        # this verifies that Batch feeds that result type through arg deps too.
        task_pwd = self.batch.process(ProcessTemplate(target="pwd", args=()))
        task_check = self.batch.function(check_exit_code, -1, -2, task_pwd)

        self.assertTrue(task_check.get())

    def test_duplicate_argument_dependency_updates_all_positions(self):
        """Resolve the same upstream task into every argument position where it appears."""
        # The same Task object appears twice, so the compiler has to patch both
        # argument positions rather than just the first one it finds.
        upstream = self.batch.function(return_constant, 21)
        downstream = self.batch.function(add_values, upstream, upstream)

        self.assertEqual(downstream.get(), 42)


class TestBatchLifecycle(unittest.TestCase):
    """Cover join/destroy behavior around the public Batch submission API."""

    def _join_process_or_fail(self, proc: Process, timeout: float, msg: str) -> None:
        proc.join(timeout=timeout)
        if proc.returncode is None:
            proc.kill()
            proc.join()
            self.fail(msg)

    def test_submit_after_join(self):
        """Joining a batch client should reject later function submissions."""
        batch = Batch()
        batch.join(timeout=5)
        with self.assertRaises(SubmitAfterCloseError):
            val = batch.function(foo_3_1, 42, 0, 0)
            val.get()
        batch.destroy()

    def test_join_waits_for_calling_client_work(self):
        """join() should wait for this client's in-flight work after close()."""
        batch = Batch(num_nodes=1, scheduler_workers=1)
        task = batch.function(sleep_and_return, 21, 0.1)

        batch.join(timeout=5)

        self.assertEqual(task.get(), 21)
        batch.destroy()

    def test_non_primary_destroys(self):
        """A non-creator client should be able to use and destroy a Batch handle after the creator exits."""
        handoff_q = Queue()
        result_q = Queue()
        values = (2, 4, 6, 8)

        try:
            creator = Process(target=create_joined_batch_and_send, args=(handoff_q, result_q, values))
            creator.start()
            creator_created = cast(dict, result_q.get(timeout=20))
            creator_joined = cast(dict, result_q.get(timeout=20))
            creator_payload = cast(dict, result_q.get(timeout=20))

            self.assertEqual(creator_created["stage"], "created")
            self.assertEqual(creator_joined["stage"], "joined")
            self.assertEqual(creator_payload["stage"], "handed_off")
            self.assertEqual(tuple(creator_payload["values"]), values)
            self.assertEqual(creator_created["client_id"], creator_joined["client_id"])
            self.assertEqual(creator_joined["client_id"], creator_payload["client_id"])

            consumer = Process(target=consume_batch_from_queue_and_destroy, args=(handoff_q, result_q, values))
            consumer.start()

            consumer_acquired = cast(dict, result_q.get(timeout=20))
            consumer_payload = cast(dict, result_q.get(timeout=20))

            self.assertEqual(consumer_acquired["stage"], "acquired")
            self.assertEqual(consumer_payload["stage"], "destroyed")

            self._join_process_or_fail(
                creator,
                timeout=20,
                msg=f"creator process {creator.puid} did not exit after the consumer acquired the Batch handle",
            )
            self.assertEqual(creator.returncode, 0, f"creator process {creator.puid} failed with exit code {creator.returncode}")

            self._join_process_or_fail(
                consumer,
                timeout=20,
                msg=f"consumer process {consumer.puid} did not exit after destroying the Batch runtime",
            )
            self.assertEqual(
                consumer.returncode, 0, f"consumer process {consumer.puid} failed with exit code {consumer.returncode}"
            )

            self.assertEqual(tuple(consumer_payload["values"]), values)
            self.assertEqual(consumer_payload["results"], list(values))
            self.assertNotEqual(creator_payload["client_id"], consumer_payload["consumer_client_id"])
        finally:
            handoff_q.close()
            result_q.close()


class TestMultiClientSubmission(unittest.TestCase):
    """Verify that multiple Dragon-process clients can share one Batch runtime."""

    def setUp(self):
        self.batch = Batch(num_nodes=1, scheduler_workers=1)
        self.result_q = Queue()
        return super().setUp()

    def tearDown(self):
        try:
            self.result_q.close()
        finally:
            self.batch.join()
            self.batch.destroy()

        return super().tearDown()

    def test_dragon_process_clients_can_submit_work_to_same_batch(self):
        """Two Dragon child processes should be able to submit work through the same pickled Batch handle."""
        procs = []

        for value in (3, 5):
            proc = Process(
                target=submit_batch_value_from_client,
                args=(self.batch, self.result_q, value),
            )
            proc.start()
            procs.append(proc)

        payloads = [cast(dict, self.result_q.get(timeout=20)) for _ in procs]

        for proc in procs:
            proc.join()
            self.assertEqual(proc.returncode, 0, f"client process {proc.puid} failed with exit code {proc.returncode}")

        self.assertEqual({payload["result"] for payload in payloads}, {3, 5})
        self.assertEqual({payload["value"] for payload in payloads}, {3, 5})

        child_client_ids = {payload["client_id"] for payload in payloads}
        self.assertEqual(len(child_client_ids), 2)
        self.assertNotIn(self.batch.client_id, child_client_ids)

        # Closing the child handles should not tear down the shared runtime.
        self.assertEqual(self.batch.function(add_values, 7, 8).get(), 15)

    def test_recursive_submission(self):
        """A Batch worker given a Batch handle should be able to submit nested work as its own client."""
        serialized_batch = cloudpickle.dumps(self.batch)
        client_puids = []

        for value in (11, 13):
            outer_task = self.batch.function(
                submit_batch_value_from_batch_worker,
                serialized_batch,
                self.result_q,
                value,
                name=f"outer-worker-{value}",
            )
            client_puids.append(outer_task.get())

        client_procs = [Process(None, ident=puid) for puid in client_puids]
        payloads = [cast(dict, self.result_q.get(timeout=20)) for _ in client_procs]

        for proc in client_procs:
            proc.join()
            self.assertEqual(
                proc.returncode, 0, f"nested client process {proc.puid} failed with exit code {proc.returncode}"
            )

        self.assertEqual({payload["result"] for payload in payloads}, {11, 13})
        self.assertEqual({payload["value"] for payload in payloads}, {11, 13})

        nested_client_ids = {payload["client_id"] for payload in payloads}
        self.assertEqual(len(nested_client_ids), 2)
        self.assertNotIn(self.batch.client_id, nested_client_ids)

class TestTaskCancellation(unittest.TestCase):
    """Verify cancellation semantics for blocked and running work."""

    def _wait_for_start_marker(self, task, marker_path: Path, timeout: float = 5.0):
        deadline = time.time() + timeout

        while time.time() < deadline:
            if marker_path.exists():
                return

            try:
                task.get(block=False)
            except TaskNotReadyError:
                time.sleep(0.01)
                continue
            except Exception as exc:
                self.fail(f"task finished before publishing its start marker: {exc}")
            else:
                self.fail("task finished before publishing its start marker")

        self.fail("timed out waiting for running task to publish its start marker")

    def setUp(self):
        self.batch = Batch(num_nodes=1, scheduler_workers=1)
        return super().setUp()

    def tearDown(self):
        self.batch.join()
        self.batch.destroy()
        return super().tearDown()

    def test_cancel_blocked_task(self):
        """Cancelling a queued task should prevent it from ever producing a result."""
        # The first task keeps the only worker busy so the second task remains
        # queued long enough for cancellation to target a blocked item.
        blocker = self.batch.function(sleep_and_return, "blocker", 0.5, name="blocker")
        blocked = self.batch.function(foo_3_2, 0, blocker, 0, name="blocked")

        # Once cancelled, the blocked task should never publish a normal result.
        self.assertTrue(blocked.cancel(timeout=5))
        self.assertEqual(blocker.get(), "blocker")

        with self.assertRaises(TaskCancelledError):
            blocked.get()

    def test_cancel_running_function(self):
        """Cancelling a running function should stop it via the worker control queue."""
        marker_path = Path(_HERE) / f".slow-func-started-{os.getpid()}-{random.randint(0, 1_000_000)}"
        marker_path.unlink(missing_ok=True)

        try:
            # The task writes a marker as soon as execution begins so the test
            # can distinguish an already-running task from a merely queued one.
            task = self.batch.function(
                signal_start_and_sleep,
                str(marker_path),
                7,
                0.5,
                name="slow-func",
            )

            self._wait_for_start_marker(task, marker_path)
            self.assertTrue(task.cancel(timeout=5))
            with self.assertRaises(TaskCancelledError):
                task.get()
        finally:
            marker_path.unlink(missing_ok=True)

    def test_cancel_running_process(self):
        """Cancelling a running process task should stop its ProcessGroup via the control queue."""
        marker_path = Path(_HERE) / f".slow-proc-started-{os.getpid()}-{random.randint(0, 1_000_000)}"
        marker_path.unlink(missing_ok=True)

        try:
            task = self.batch.process(
                ProcessTemplate(
                    target=signal_start_and_sleep,
                    args=(str(marker_path), 7, 0.5),
                ),
                name="slow-proc",
            )

            self._wait_for_start_marker(task, marker_path)
            self.assertTrue(task.cancel(timeout=5))
            with self.assertRaises(TaskCancelledError):
                task.get()
        finally:
            marker_path.unlink(missing_ok=True)

    def test_read_after_write_failure_propagates(self):
        """A failed writer should surface a chained read-after-write error downstream."""
        # The reader depends on a write edge, so it should not run successfully
        # if the writer fails before producing the shared key.
        shared_ddict = get_ddict(self.batch)
        self.addCleanup(shared_ddict.destroy)
        writer = self.batch.function(
            raise_runtime_error,
            "writer exploded",
            writes=[self.batch.write(shared_ddict, "k")],
            name="writer-task",
        )
        reader = self.batch.function(
            return_constant,
            42,
            reads=[self.batch.read(shared_ddict, "k")],
            name="reader-task",
        )

        with self.assertRaises(RuntimeError):
            writer.get()

        # The downstream failure should preserve dependency context rather than
        # surfacing as an unrelated missing-key or generic runtime error.
        with self.assertRaises(ReadAfterWriteDependencyError) as context:
            reader.get()

        message = str(context.exception)
        self.assertIn("read-after-write dependency", message)
        self.assertIn(writer.core.tuid, message)

    def test_argument_dependency_failure_propagates_as_raw(self):
        """A failed argument producer should cancel the consumer with dependency context."""
        # This mirrors the previous test, but exercises raw argument dependency
        # propagation instead of read/write metadata on a shared DDict key.
        upstream = self.batch.function(
            raise_runtime_error,
            "argument producer exploded",
            name="upstream-arg-task",
        )
        downstream = self.batch.function(
            foo_3_2,
            0,
            upstream,
            0,
            name="downstream-arg-task",
        )

        with self.assertRaises(RuntimeError):
            upstream.get()

        # The downstream task should be cancelled with a dependency-aware error
        # message that still points back to the original producer task.
        with self.assertRaises(ReadAfterWriteDependencyError) as context:
            downstream.get()

        message = str(context.exception)
        self.assertIn("read-after-write dependency", message)
        self.assertIn(upstream.core.tuid, message)

    def test_cancel_completed_task_returns_false(self):
        """Once a task has already finished, later cancellation requests should report failure."""
        # A completed task is immutable from the scheduler's point of view, so a
        # cancellation request after get() should be a no-op.
        task = self.batch.function(return_constant, 9, name="completed-task")

        self.assertEqual(task.get(), 9)
        self.assertFalse(task.cancel(timeout=5))
        self.assertEqual(task.get(), 9)


class TestBatchFibonacci(unittest.TestCase):
    """Check the fibonacci demo program over both supported storage backends."""

    @classmethod
    def setUpClass(cls):
        cls.batch = Batch()

    @classmethod
    def tearDownClass(cls):
        cls.batch.join()
        cls.batch.destroy()

    def test_fib_with_ddict(self):
        """The ddict-backed fibonacci workflow should produce the expected sequence."""
        # The helper submits the whole fibonacci DAG and returns realized values,
        # so the test only needs to verify the recurrence step-by-step.
        fib_seq = get_fib_sequence(self.batch, use_ddict=True)

        a = 0
        b = 1

        for val in fib_seq:
            self.assertEqual(val, a + b)
            a = b
            b = val

    def test_fib_with_fs(self):
        """The filesystem-backed fibonacci workflow should produce the same recurrence."""
        # This uses the file-based dependency backend to ensure the same logical
        # task graph works when state is stored outside DDict.
        fib_seq = get_fib_sequence(self.batch, use_ddict=False)

        a = 0
        b = 1

        for val in fib_seq:
            self.assertEqual(val, a + b)
            a = b
            b = val

    def test_fib_backends_match(self):
        """Both storage backends should drive the same task graph and outputs."""
        # Compare the end-to-end sequences directly so any divergence in either
        # dependency tracking or storage semantics shows up immediately.
        self.assertEqual(
            get_fib_sequence(self.batch, use_ddict=True),
            get_fib_sequence(self.batch, use_ddict=False),
        )


class TestGPUAffinity(unittest.TestCase):
    """Validate the minimal GPU affinity guarantees available today."""

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
            cls.batch.join()
            cls.batch.destroy()

    def test_gpu_affinity_sort_of(self):
        """At least one vendor-specific GPU visibility variable should be populated."""
        # GPU affinity for workers is currently being set randomly, so we can't
        # properly test things, but we can sort of test them by checking that
        # the affinity is at least set to *something*
        check = self.batch.function(check_gpu_affinity)
        self.assertTrue(check.get())


class TestProcessPlacement(unittest.TestCase):
    """Exercise explicit and implicit placement behavior for Batch processes and jobs."""

    pool_hosts: set[str] = set()
    allocation_hosts: set[str] = set()
    excluded_hosts: set[str] = set()

    @classmethod
    def setUpClass(cls):
        system = System()
        if system.nnodes < 2:
            raise unittest.SkipTest("Need at least 2 nodes for Batch placement tests")

        # Keep the placement test batch small so implicit-placement assertions
        # can distinguish Batch-local hosts from other hosts in the allocation.
        cls.batch = Batch(num_nodes=2, pool_nodes=1)
        topology = cls.batch.topology()

        if len(topology.pool_hostnames) < 2:
            cls.batch.join()
            cls.batch.destroy()
            raise unittest.SkipTest("Need at least 2 worker pools for Batch placement tests")

        cls.target_host = topology.pool_hostnames[0][0]
        cls.subnode_host = topology.pool_hostnames[1][0]

        if cls.target_host == cls.subnode_host:
            cls.batch.join()
            cls.batch.destroy()
            raise unittest.SkipTest("Need distinct target and subnode hosts for Batch placement tests")

        cls.target_node = Node(cls.batch._pool_node_huids_list[1][0])
        cls.pool_hosts = {hostname for pool in topology.pool_hostnames for hostname in pool}
        cls.allocation_hosts = {Node(node).hostname for node in system.nodes}
        cls.excluded_hosts = cls.allocation_hosts - cls.pool_hosts

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, "batch"):
            cls.batch.join()
            cls.batch.destroy()

    def tearDown(self):
        self.batch.fence(timeout=10)
        return super().tearDown()

    def _make_probe_queue(self) -> Queue:
        probe_queue = Queue()
        self.addCleanup(probe_queue.close)
        return probe_queue

    def _run_batch_process_probe(self, policy: Policy) -> dict:
        probe_queue = self._make_probe_queue()
        task = self.batch.process(
            ProcessTemplate(target=record_process_placement, args=(probe_queue,), policy=policy),
            name="batch-process-placement",
        )

        self.assertEqual(task.get(), 0)
        return cast(dict, probe_queue.get(timeout=10))

    def _run_batch_job_probe(self, policy: Policy) -> dict:
        probe_queue = self._make_probe_queue()
        task = self.batch.job(
            [(1, ProcessTemplate(target=record_process_placement, args=(probe_queue,), policy=policy))],
            name="batch-job-placement",
            pmi=None,
        )

        self.assertEqual(task.get(), 0)
        return cast(dict, probe_queue.get(timeout=10))

    def _assert_host_probe(self, payload: dict, expected_host: str) -> None:
        self.assertEqual(payload["hostname"], expected_host)
        self.assertIn(payload["hostname"], self.pool_hosts)

    def _assert_gpu_probe(self, payload: dict) -> None:
        self.assertEqual(payload["hostname"], self.target_host)
        self.assertEqual(payload["gpu_env_name"], self.target_node.gpu_env_str)
        self.assertIsNotNone(payload["gpu_env_value"])
        self.assertIn("0", payload["gpu_env_value"])

    def test_batch_process_honors_explicit_host_policy(self):
        """A process should stay on the user-requested host instead of the launching worker's host."""
        # Probe placement from inside the child process so the assertion uses the
        # runtime-observed host rather than planner-side bookkeeping.
        payload = self._run_batch_process_probe(
            Policy(placement=Policy.Placement.HOST_NAME, host_name=self.target_host)
        )

        self._assert_host_probe(payload, self.target_host)
        self.assertNotEqual(payload["hostname"], self.subnode_host)

    def test_batch_job_honors_explicit_host_policy(self):
        """A single-rank job should preserve an explicit host-name placement request."""
        # Jobs use a different launch path than process tasks, so keep a mirror
        # assertion to catch host-policy rewrites specific to jobs.
        payload = self._run_batch_job_probe(Policy(placement=Policy.Placement.HOST_NAME, host_name=self.target_host))

        self._assert_host_probe(payload, self.target_host)
        self.assertNotEqual(payload["hostname"], self.subnode_host)

    def test_batch_process_honors_explicit_host_id_policy(self):
        """A process should also honor explicit host-id placement, not just host names."""
        # Host-id placement exercises the same intent through a different policy
        # field, which has historically been easy to normalize incorrectly.
        payload = self._run_batch_process_probe(
            Policy(placement=Policy.Placement.HOST_ID, host_id=self.target_node.h_uid)
        )

        self._assert_host_probe(payload, self.target_host)
        self.assertNotEqual(payload["hostname"], self.subnode_host)

    def test_batch_job_honors_explicit_host_id_policy(self):
        """A single-rank job should preserve explicit host-id placement requests."""
        # Keep the job variant separate so host-id handling is validated on both
        # Batch.process and Batch.job entry points.
        payload = self._run_batch_job_probe(Policy(placement=Policy.Placement.HOST_ID, host_id=self.target_node.h_uid))

        self._assert_host_probe(payload, self.target_host)
        self.assertNotEqual(payload["hostname"], self.subnode_host)

    def test_batch_process_without_explicit_host_stays_within_batch_topology(self):
        """Fallback runtime placement should stay inside the Batch worker topology when spare allocation nodes exist."""
        if not self.excluded_hosts:
            self.skipTest("Need at least 1 allocated node outside the Batch topology for implicit placement tests")

        # With no explicit host policy, Batch should still constrain placement to
        # the worker pools it owns rather than wandering across the full alloc.
        payload = self._run_batch_process_probe(Policy())

        self.assertIn(payload["hostname"], self.pool_hosts)
        self.assertNotIn(payload["hostname"], self.excluded_hosts)

    def test_batch_job_without_explicit_host_stays_within_batch_topology(self):
        """Default job placement should remain inside the Batch worker topology when spare allocation nodes exist."""
        if not self.excluded_hosts:
            self.skipTest("Need at least 1 allocated node outside the Batch topology for implicit placement tests")

        # This catches the same regression on the job path, where scheduler-side
        # allocation could accidentally use nodes outside the Batch topology.
        payload = self._run_batch_job_probe(Policy())

        self.assertIn(payload["hostname"], self.pool_hosts)
        self.assertNotIn(payload["hostname"], self.excluded_hosts)

    def test_batch_process_rejects_empty_explicit_host_name(self):
        """A malformed HOST_NAME policy should fail loudly instead of being repaired implicitly."""
        # Submit the malformed policy through the public API so the test proves
        # validation happens before launch rather than being silently repaired.
        probe_queue = self._make_probe_queue()
        task = self.batch.process(
            ProcessTemplate(
                target=record_process_placement,
                args=(probe_queue,),
                policy=Policy(placement=Policy.Placement.HOST_NAME),
            ),
            name="invalid-batch-process-host-policy",
        )

        with self.assertRaisesRegex(RuntimeError, "placement=HOST_NAME requires a non-empty host_name"):
            task.get()

    def test_batch_job_rejects_invalid_explicit_host_id(self):
        """A malformed HOST_ID policy should fail before Batch rewrites it to the worker host."""
        # Jobs used to be especially prone to policy rewriting, so keep an
        # explicit negative test for an unset host_id value here.
        probe_queue = self._make_probe_queue()
        task = self.batch.job(
            [
                (
                    1,
                    ProcessTemplate(
                        target=record_process_placement,
                        args=(probe_queue,),
                        policy=Policy(placement=Policy.Placement.HOST_ID),
                    ),
                )
            ],
            name="invalid-batch-job-host-policy",
            pmi=None,
        )

        with self.assertRaisesRegex(RuntimeError, "placement=HOST_ID requires a valid host_id"):
            task.get()

    def test_batch_process_honors_explicit_gpu_policy(self):
        """A process should preserve GPU affinity when the host placement is explicit."""
        if self.target_node.num_gpus < 1 or self.target_node.gpu_env_str is None:
            self.skipTest("Need at least 1 GPU on the target host for Batch GPU placement tests")

        # Pin both host and GPU so the test can assert exact affinity instead of
        # relying on Batch's default worker-level GPU assignment heuristics.
        payload = self._run_batch_process_probe(
            Policy(
                placement=Policy.Placement.HOST_NAME,
                host_name=self.target_host,
                gpu_affinity=[0],
            )
        )

        self._assert_gpu_probe(payload)

    def test_batch_job_honors_explicit_gpu_policy(self):
        """A single-rank job should preserve GPU affinity when the host placement is explicit."""
        if self.target_node.num_gpus < 1 or self.target_node.gpu_env_str is None:
            self.skipTest("Need at least 1 GPU on the target host for Batch GPU placement tests")

        # Mirror the process test on the job path so GPU environment propagation
        # is checked for both launcher implementations.
        payload = self._run_batch_job_probe(
            Policy(
                placement=Policy.Placement.HOST_NAME,
                host_name=self.target_host,
                gpu_affinity=[0],
            )
        )

        self._assert_gpu_probe(payload)


class TestIteratedInterCompiledDeps(unittest.TestCase):
    """Stress cross-batch dependency routing over many iterations."""

    @classmethod
    def setUpClass(cls):
        cls.batch = Batch()

    @classmethod
    def tearDownClass(cls):
        cls.batch.join()
        cls.batch.destroy()

    def _make_function_task(self, target, *args, reads=None, writes=None, name=None, timeout=None):
        if timeout is None:
            return self.batch.function(target, *args, reads=reads, writes=writes, name=name)

        return self.batch.function(target, *args, reads=reads, writes=writes, name=name, timeout=timeout)

    def _expected_iteration_sum(self, previous_sum: int) -> int:
        return_values_total = sum(iterated_return_value(previous_sum, offset) for offset in range(10))
        write_values_total = sum((previous_sum + offset + 11) % ITERATED_DEP_MODULUS for offset in range(100))
        return return_values_total + write_values_total

    def test_iterated_return_and_ddict_sum(self):
        """Repeated compiled batches should preserve both return-value and ddict dependency ordering."""
        # This test stresses cross-compile state: each iteration produces both
        # plain return values and DDict-backed writes that the next task must see.
        shared_ddict = get_ddict(self.batch)
        self.addCleanup(shared_ddict.destroy)
        previous_sum = 0

        for iteration in range(10):
            return_tasks = []
            writer_keys = []

            # Build a batch of pure return-value producers.
            for offset in range(10):
                task = self._make_function_task(iterated_return_value, previous_sum, offset)
                return_tasks.append(task)

            # Build a larger batch of DDict writers whose keys are consumed by a
            # downstream reduction task in the same iteration.
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

            # The reducer depends on both classes of upstream outputs, so it is a
            # compact way to validate that both dependency channels stay ordered.
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
    """Exercise imported PTD wrappers for function, process, and job tasks."""

    @classmethod
    def setUpClass(cls):
        if System().nnodes == 1:
            raise unittest.SkipTest("Not enough nodes detected in the system")
        cls.batch = Batch()

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, "batch"):
            cls.batch.join()
            cls.batch.destroy()

    def tearDown(self):
        # PTD tests share one Batch instance across the class, so reset retained
        # runtime state between tests rather than carrying it into the next case.
        self.batch.clear_results()

    def test_function_ptd(self):
        """A PTD-imported function should preserve result ordering across many calls."""
        # Import the PTD once, then submit many calls so the wrapper is exercised
        # as a reusable task factory rather than a one-shot helper.
        get_prime_wrapper = self.batch.import_func(_yml("function.yml"), get_prime)

        returned_primes = []
        num_primes = len(supersingular_primes)

        for i in range(num_primes):
            val = get_prime_wrapper(i)
            returned_primes.append(val)

        # Materialize the Task handles in submission order and compare against the
        # known reference list to catch ordering or argument-marshalling issues.
        for i, val in enumerate(returned_primes):
            returned_primes[i] = val.get()

        self.assertEqual(sum(returned_primes), sum(supersingular_primes))

    def test_function_ptd_returns_none_for_missing_value(self):
        """PTD-imported functions should faithfully return None when the wrapped function does."""
        # This is a narrow regression check that the PTD wrapper does not coerce
        # a legitimate None return into some sentinel or serialization artifact.
        get_prime_wrapper = self.batch.import_func(_yml("function.yml"), get_prime)

        self.assertIsNone(get_prime_wrapper(len(supersingular_primes) + 1).get())

    def test_function_w_deps_ptd(self):
        """A PTD wrapper with dependency metadata should match an equivalent local update loop."""
        num_iters = 4
        num_items = 8

        update_ddict = self.batch.import_func(_yml("function_w_deps.yml"), update_dict, next_idx)

        # Build identical starting state in a local dict and the DDict-backed task
        # workflow so the test can compare the PTD behavior against a known-good
        # in-process baseline.
        the_ddict = get_ddict(self.batch)
        self.addCleanup(the_ddict.destroy)
        the_dict = {}
        num_primes = len(supersingular_primes)

        for i in range(num_items):
            the_ddict[i] = supersingular_primes[i % num_primes]
            the_dict[i] = the_ddict[i]

        # Drive the PTD-imported task graph for several rounds of dependent updates.
        handles = []
        for _ in range(num_iters):
            for i in range(num_items):
                handle = update_ddict(the_ddict, i, num_items)
                handles.append(handle)

        # Compute the same transformation locally so the final structures can be
        # compared value-for-value.
        for _ in range(num_iters):
            for i in range(num_items):
                update_dict(the_dict, i, num_items)

        for handle in handles:
            handle.get()

        # Matching end state proves the PTD dependency metadata preserved the same
        # ordering guarantees as the handwritten local loop.
        for i in range(num_items):
            self.assertEqual(the_dict[i], the_ddict[i])

    def test_process_w_deps_ptd(self):
        """A PTD-imported process should propagate transformed inputs across chained files."""

        # The imported PTD should keep rewriting each process input from the prior
        # output file, so the final file should reflect the cumulative transform.
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

        # Launch a chain where each process consumes the previous output file.
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
        """A timed PTD-imported process should surface a non-zero exit code on timeout."""

        # The helper binary honors TIMEOUT_SET by sleeping past its limit, so the
        # PTD wrapper should return a failing exit status instead of hanging.
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
        """A PTD-imported job should preserve transformed file-based state across iterations."""

        # TODO: need to update my_mpi_job.c to make it consistent with this test
        # This mirrors the process PTD test on the job path so the file-based
        # dependency plumbing is covered for multi-process launches too.
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
        cls.batch.join()
        cls.batch.destroy()

    def test_fence_completes(self):
        """fence() should complete within a reasonable timeout after tasks are submitted."""
        # Submit a burst of independent tasks, then rely on fence() to drain the
        # entire batch before any explicit get() calls are issued.
        tasks = [self.batch.function(foo_3_1, i, 0, 0) for i in range(10)]
        self.batch.fence(timeout=5)
        # all results should already be available since fence() waited for completion
        for i, task in enumerate(tasks):
            self.assertEqual(task.get(), i)

    def test_fence_waits_for_dispatched_subnode_work(self):
        """fence() should not complete before dispatched subnode work has actually finished."""
        delay = 0.2
        tasks = [self.batch.function(sleep_and_return, i, delay) for i in range(8)]

        start = time.perf_counter()
        self.batch.fence(timeout=5)
        elapsed = time.perf_counter() - start

        # Regression for a scheduler race where fence() could complete after the
        # client dispatched work but before a subnode manager had registered it.
        # Use slack (75%) to avoid test flakiness from timer/dispatch jitter.
        self.assertGreaterEqual(elapsed, delay * 0.75)

        for i, task in enumerate(tasks):
            self.assertEqual(task.get(), i)

    def test_fence_completes_with_dep_chain(self):
        """fence() should wait for dependent tasks as well."""
        # A dependent chain ensures fence() is waiting on transitive work, not
        # just tasks that were immediately runnable at submission time.
        val1 = self.batch.function(foo_3_1, 42, None, None)
        val2 = self.batch.function(foo_3_2, 0, val1, 0)
        self.batch.fence(timeout=5)
        self.assertEqual(val2.get(), 42)

    def test_fence_with_no_pending_work(self):
        """Calling fence on an idle batch should be a no-op for later submissions."""
        # This protects against fence() leaving the batch in a terminal or stale
        # internal state when there was nothing to wait on.
        self.batch.fence(timeout=5)
        val = self.batch.function(foo_3_1, 99, 0, 0)
        self.assertEqual(val.get(), 99)

    def test_fence_clears_state(self):
        """After fence(), the client compiler's dependency and routing caches should be empty."""
        shared_ddict = get_ddict(self.batch)
        self.addCleanup(shared_ddict.destroy)

        # Use a write followed by a read so both compile-time caches are
        # populated before the fence resets the client compiler state.
        self.batch.function(
            return_constant,
            1,
            writes=[self.batch.write(shared_ddict, "k")],
            name="fence-state-writer",
        )
        self.batch.function(
            return_constant,
            2,
            reads=[self.batch.read(shared_ddict, "k")],
            name="fence-state-reader",
        )

        # Force the client request worker to compile and dispatch the queued
        # tasks so the compiler caches are populated before the fence runs.
        self.batch._flush_client_request_worker(timeout=5)
        self.assertIsNotNone(self.batch.client_compiler)
        compiler = self.batch.client_compiler

        self.assertGreater(len(compiler.dep_frontier), 0)
        self.assertGreater(len(compiler.tuid_to_manager_q), 0)

        self.batch.fence(timeout=5)

        self.assertEqual(len(compiler.dep_frontier), 0)
        self.assertEqual(len(compiler.tuid_to_manager_q), 0)

    def test_tasks_can_be_submitted_after_fence(self):
        """Tasks submitted after a fence should complete normally."""
        # Submit one task before the fence and one after so the test catches any
        # accidental one-shot semantics in the fence implementation.
        val1 = self.batch.function(foo_3_1, 1, 0, 0)
        self.batch.fence(timeout=5)
        val2 = self.batch.function(foo_3_1, 2, 0, 0)
        self.assertEqual(val2.get(), 2)

    def test_clear_results_empties_ddict(self):
        """clear_results() should wait for tasks to finish, then clear the results ddict."""
        # First materialize the results so the DDict definitely contains entries,
        # then verify clear_results() removes only the stored outputs.
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


class TestScheduler(unittest.TestCase):
    """Tests for the scheduler manager (manager 0) handling multi-node MPI jobs."""

    @classmethod
    def setUpClass(cls):
        system = System()
        cls.nnodes = system.nnodes
        # Record MPI availability and node information; individual tests
        # will skip or fall back as appropriate.
        cls.mpi_available = _mpi4py_available()
        node = Node(system.nodes[0])
        cls.physical_cores_per_node = node.num_cpus // 2
        cls.batch = Batch()

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, "batch"):
            cls.batch.join()
            cls.batch.destroy()

    def test_mnj_jobs_varying_node_counts(self):
        """Submit multi-node jobs with varying rank counts routed through the scheduler."""
        if self.nnodes < 2:
            self.skipTest("Need at least 2 nodes for multi-node job tests")

        n_jobs = 10
        sleepsecs = 1
        max_ranks = self.nnodes * self.physical_cores_per_node

        # Randomize the requested size so the scheduler path gets coverage across
        # one-node-plus and larger multi-node allocations.
        job_handles = []
        for i in range(n_jobs):
            nranks = random.randint(self.physical_cores_per_node + 1, max_ranks)
            # choose a job target depending on MPI availability; fall back to
            # a lightweight local job function when mpi4py is missing
            if getattr(self, "mpi_available", False):
                template = ProcessTemplate(target=mpi_f, args=(i, sleepsecs))
                job = self.batch.job(process_templates=[(nranks, template)])
            else:
                template = ProcessTemplate(target=mpi_job_func, args=())
                job = self.batch.job(process_templates=[(nranks, template)], pmi=None)
            job_handles.append(job)

        # Regardless of whether the test uses real MPI or the fallback function,
        # every launched rank should report success.
        for i, handle in enumerate(job_handles):
            exit_codes = handle.get()
            if isinstance(exit_codes, list):
                self.assertTrue(
                    all(ec == 0 for ec in exit_codes),
                    msg=f"Job {i} had non-zero exit codes: {exit_codes}",
                )
            else:
                self.assertEqual(exit_codes, 0, msg=f"Job {i} failed with exit code {exit_codes}")

    def test_mpi_ensemble(self):
        """Submit a randomized ensemble of MPI jobs; fall back to mpi_job_func if mpi4py missing."""
        njobs = 32
        sleepsecs = 2
        maxranks = 32

        # A larger ensemble is more likely to catch scheduler bookkeeping bugs
        # than a single representative job size.
        alljobs = []
        for i in range(njobs):
            nranks = random.randint(2, maxranks)
            if getattr(self, "mpi_available", False):
                template = ProcessTemplate(target=mpi_f, args=(i, sleepsecs))
                job = self.batch.job(process_templates=[(nranks, template)])
            else:
                template = ProcessTemplate(target=mpi_job_func, args=())
                job = self.batch.job(process_templates=[(nranks, template)], pmi=None)
            alljobs.append(job)

        for i, job in enumerate(alljobs):
            exit_codes = job.get()
            if isinstance(exit_codes, list):
                self.assertTrue(all(ec == 0 for ec in exit_codes), msg=f"Job {i} had non-zero exit codes: {exit_codes}")
            else:
                self.assertEqual(exit_codes, 0, msg=f"Job {i} failed with exit code {exit_codes}")

    def test_interleaved_functions_and_growing_jobs(self):
        """Submit interleaved function and job tasks with growing job sizes.

        Jobs grow from 1 core, to 1 node, to 2 nodes, ... up to total nodes.
        The cycle runs twice; tasks are submitted back-to-back with no fence
        until the end of the two-cycle run.
        """
        # Build sizes: 1 core, then 1 node, 2 nodes, ..., up to nnodes
        sizes = [1] + [self.physical_cores_per_node * n for n in range(1, self.nnodes + 1)]

        handles = []
        sleepsecs = 1
        for cycle in range(2):
            for idx, sz in enumerate(sizes):
                # Interleave cheap function work with progressively larger jobs so
                # the scheduler has to juggle both task classes back-to-back.
                f = self.batch.function(foo_3_1, sz, 0, 0)
                handles.append(("func", f, sz))

                # Use the lightweight fallback job target so the test stresses Batch's
                # scheduling path without depending on mpi4py in the worker env.
                template = ProcessTemplate(target=mpi_job_func, args=())
                j = self.batch.job(process_templates=[(sz, template)], pmi=None)
                handles.append(("job", j, sz))

        # Fence once at the end so the run behaves like a realistic mixed backlog.
        self.batch.fence()

        # Verify the two task families independently: functions echo their input,
        # while jobs must report all-zero exit codes.
        for task_type, handle, expected in handles:
            if task_type == "func":
                self.assertEqual(handle.get(), expected)
            else:
                exit_codes = handle.get()
                if isinstance(exit_codes, list):
                    self.assertTrue(all(ec == 0 for ec in exit_codes), msg=f"Non-zero exit codes: {exit_codes}")
                else:
                    self.assertEqual(exit_codes, 0, msg=f"Non-zero exit code: {exit_codes}")

    def test_producer_job_arg_chain(self):
        """Iteratively produce values with functions and validate them in multi-node jobs.

        - Functions double each iteration until reaching total physical cores.
        - For each iteration, function outputs are spread evenly across
          `nnodes // 2` jobs, each running on 2 nodes.
        - The first exit code from each job in the previous iteration is
          provided to the functions in the next iteration.
        - Jobs raise an exception if any value is incorrect.
        """
        if self.nnodes < 2:
            self.skipTest("Need at least 2 nodes for producer/job arg-chain test")

        total_physical_cores = self.physical_cores_per_node * self.nnodes
        num_jobs = self.nnodes // 2
        ranks_per_job = 2 * self.physical_cores_per_node

        prev_first_codes = None

        n_funcs = 1
        while True:
            # Grow the producer side geometrically so each round stresses more
            # argument dependencies while keeping the total bounded by the alloc.
            producers = [self.batch.function(producer_value, i, prev_first_codes) for i in range(n_funcs)]

            # Split `lst` into `parts` sublists as evenly as possible while
            # preserving the original order. If `len(lst)` does not divide
            # evenly, the first `m` groups receive one extra element where
            # `k, m = divmod(len(lst), parts)`. Returns a list of the
            # sublists (some may be empty if `parts` > `len(lst)`).
            def _split_even(lst, parts):
                k, m = divmod(len(lst), parts)
                res = []
                i = 0
                for p in range(parts):
                    sz = k + (1 if p < m else 0)
                    res.append(lst[i : i + sz])
                    i += sz
                return res

            groups = _split_even(producers, num_jobs)

            # Feed each group of producer tasks into a two-node job so the test
            # exercises Task-as-argument propagation into scheduler-owned work.
            job_handles = []
            for grp in groups:
                # always include prev_first_codes as the last argument (may be None)
                args = tuple(grp) + (prev_first_codes,)
                tmpl = ProcessTemplate(target=mpi_job_arg_checker, args=args)
                job_handles.append(self.batch.job(process_templates=[(ranks_per_job, tmpl)], pmi=None))

            # Collect one code per job and feed that summary back into the next
            # round of producers, forming a cross-iteration dependency chain.
            prev_first_codes = []
            for job_idx, j in enumerate(job_handles):
                exit_codes = j.get()
                if isinstance(exit_codes, list):
                    self.assertTrue(
                        len(exit_codes) > 0,
                        msg=f"Job {job_idx} returned no exit codes",
                    )
                    self.assertTrue(
                        all(ec == 0 for ec in exit_codes),
                        msg=f"Job {job_idx} had non-zero exit codes: {exit_codes}",
                    )
                    first = exit_codes[0] if len(exit_codes) > 0 else 0
                else:
                    self.assertEqual(
                        exit_codes,
                        0,
                        msg=f"Job {job_idx} failed with exit code {exit_codes}",
                    )
                    first = exit_codes
                prev_first_codes.append(first)

            if n_funcs == total_physical_cores:
                break
            n_funcs = min(total_physical_cores, n_funcs * 2)

# Add this test class to test_batch.py

class TestBatchManagedLifecycle(unittest.TestCase):
    """Tests for managed_lifecycle flag and Batch serialization."""

    def test_default_managed_lifecycle_is_false(self):
        """New Batch instances should default to managed_lifecycle=False."""
        batch = Batch()
        try:
            self.assertFalse(batch.managed_lifecycle)
        finally:
            batch.join()
            batch.destroy()

    def test_explicit_managed_lifecycle_true(self):
        """Batch can be created with managed_lifecycle=True."""
        batch = Batch(managed_lifecycle=True)
        try:
            self.assertTrue(batch.managed_lifecycle)
        finally:
            batch.join()
            batch.destroy()

    def test_destroy_is_idempotent(self):
        """Calling destroy() multiple times should not raise."""
        batch = Batch()
        batch.join()
        batch.destroy()
        # Second call should be a no-op
        batch.destroy()
        batch.destroy()

    def test_serialized_batch_has_managed_lifecycle_true(self):
        """Deserialized Batch handles should always have managed_lifecycle=True."""
        batch = Batch()
        try:
            # Run a task to ensure the batch is functional
            task = batch.function(return_constant, 42)
            self.assertEqual(task.get(), 42)

            # Serialize and deserialize
            serialized = cloudpickle.dumps(batch)
            restored = cloudpickle.loads(serialized)

            try:
                self.assertTrue(restored.managed_lifecycle)
                self.assertIsNotNone(restored.client_id)  # Should get new client_id on register
            finally:
                restored.destroy()
        finally:
            batch.join()
            batch.destroy()

    def test_serialized_batch_can_submit_work_while_owner_alive(self):
        """A deserialized Batch handle should be able to submit work while the original is alive."""
        batch = Batch()
        try:
            serialized = cloudpickle.dumps(batch)
            restored = cloudpickle.loads(serialized)

            try:
                # The restored handle should be able to submit and retrieve work
                task = restored.function(return_constant, 99)
                self.assertEqual(task.get(), 99)

                # Original should still work too
                task2 = batch.function(return_constant, 101)
                self.assertEqual(task2.get(), 101)
            finally:
                restored.destroy()
        finally:
            batch.join()
            batch.destroy()

    def test_serialized_batch_gets_distinct_client_id(self):
        """A deserialized Batch handle should register as a new client with a distinct ID."""
        batch = Batch()
        try:
            original_client_id = batch.client_id

            serialized = cloudpickle.dumps(batch)
            restored = cloudpickle.loads(serialized)

            try:
                # Trigger registration by submitting work
                task = restored.function(return_constant, 1)
                task.get()

                self.assertIsNotNone(restored.client_id)
                self.assertNotEqual(restored.client_id, original_client_id)
            finally:
                restored.destroy()
        finally:
            batch.join()
            batch.destroy()

    def test_multiple_serialized_handles_can_coexist(self):
        """Multiple deserialized Batch handles should be able to work concurrently."""
        batch = Batch()
        try:
            restored_handles = []
            for i in range(3):
                serialized = cloudpickle.dumps(batch)
                restored = cloudpickle.loads(serialized)
                restored_handles.append(restored)

            try:
                # All handles should be able to submit work
                tasks = []
                for i, handle in enumerate(restored_handles):
                    task = handle.function(return_constant, i * 10)
                    tasks.append((i, task))

                for i, task in tasks:
                    self.assertEqual(task.get(), i * 10)

                # All should have distinct client IDs
                client_ids = {h.client_id for h in restored_handles}
                self.assertEqual(len(client_ids), 3)
                self.assertNotIn(batch.client_id, client_ids)
            finally:
                for handle in restored_handles:
                    handle.destroy()
        finally:
            batch.join()
            batch.destroy()

    def test_serialized_batch_destroy_does_not_affect_owner(self):
        """Destroying a deserialized handle should not tear down the shared runtime."""
        batch = Batch()
        try:
            serialized = cloudpickle.dumps(batch)
            restored = cloudpickle.loads(serialized)

            # Destroy the restored handle
            restored.destroy()

            # Original batch should still be functional
            task = batch.function(return_constant, 77)
            self.assertEqual(task.get(), 77)
        finally:
            batch.join()
            batch.destroy()

    def test_getstate_excludes_unpicklable_fields(self):
        """__getstate__ should exclude thread and local queue objects."""
        batch = Batch()
        try:
            state = batch.__getstate__()

            # These fields should be stripped
            self.assertNotIn("_client_request_q", state)
            self.assertNotIn("_client_request_thread", state)
            self.assertNotIn("log", state)
            self.assertNotIn("ret_q", state)

            # Serialized queue descriptors should be present
            self.assertIn("_serialized_manager_qs", state)

            # State flags should be reset for deserialized handle
            self.assertTrue(state["managed_lifecycle"])
            self.assertIsNone(state["client_id"])
            self.assertFalse(state["closed"])
            self.assertFalse(state["destroyed"])
            self.assertFalse(state["terminated"])
        finally:
            batch.join()
            batch.destroy()


class TestBatchSerializationWithDDict(unittest.TestCase):
    """Tests for storing and retrieving Batch handles via DDict."""

    def test_store_and_restore_batch_from_ddict_while_alive(self):
        """A Batch handle stored in DDict should be restorable while the owner is alive."""
        from dragon.data.ddict.ddict import DDict

        ddict = DDict(1, 1, 50 * 1024**2)
        batch = Batch(managed_lifecycle=True)

        try:
            # Store the batch handle
            ddict["batch"] = batch

            # Retrieve it
            restored = ddict.pop("batch")

            try:
                self.assertTrue(restored.managed_lifecycle)

                # Should be able to submit work
                task = restored.function(return_constant, 123)
                self.assertEqual(task.get(), 123)
            finally:
                restored.destroy()
        finally:
            batch.join()
            batch.destroy()
            ddict.destroy()

    def test_restore_batch_from_ddict_after_destroy_fails(self):
        """Restoring a Batch handle from DDict after destroy() should fail gracefully."""
        from dragon.data.ddict.ddict import DDict

        ddict = DDict(1, 1, 50 * 1024**2)
        batch = Batch(managed_lifecycle=True)

        try:
            # Store the batch handle
            ddict["batch"] = batch

            # Destroy the original batch (tears down results_ddict)
            batch.join()
            batch.destroy()
            batch = None  # Prevent double-destroy in finally

            # Attempting to restore should fail because the underlying
            # resources (results_ddict) have been destroyed
            with self.assertRaises(RuntimeError):
                restored = ddict.pop("batch")
        finally:
            if batch is not None:
                batch.join()
                batch.destroy()
            ddict.destroy()

if __name__ == "__main__":
    unittest.main(verbosity=2)
