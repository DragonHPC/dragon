"""
Unit tests for the CPU worker utilities module.

Tests the CPUWorker class.

These tests use real Dragon multiprocessing primitives and infrastructure.
"""

import dragon
import multiprocessing as mp
from unittest import TestCase, main
from unittest.mock import patch, MagicMock

from dragon.infrastructure.policy import Policy
from dragon.native.process_group import ProcessGroup
from dragon.native.process import ProcessTemplate, Popen
from dragon.ai.inference.cpu_worker_utils import CPUWorker
from dragon.ai.inference.config import (
    ModelConfig,
    BatchingConfig,
    GuardrailsConfig,
    DynamicWorkerConfig,
)


class MockTelemetry:
    """Simple mock for dragon telemetry (external dependency for metrics)."""

    pass


class TestCPUWorkerInit(TestCase):
    """Test cases for CPUWorker initialization."""

    @classmethod
    def setUpClass(cls):
        """Set up Dragon multiprocessing."""
        try:
            mp.set_start_method("dragon", force=True)
        except RuntimeError:
            pass  # Already set

    def setUp(self):
        """Set up test fixtures."""
        self.input_queue = mp.Queue()
        self.model_config = ModelConfig(
            model_name="test-model",
            hf_token="test-token",
            tp_size=2,
            dtype="bfloat16",
            max_tokens=100,
            top_k=50,
            top_p=0.95,
            padding_side="left",
            truncation_side="left",
        )
        self.batching_config = BatchingConfig(
            enabled=True,
            batch_type="dynamic",
            batch_wait_seconds=0.1,
            max_batch_size=60,
        )
        self.guardrails_config = GuardrailsConfig(
            enabled=True,
            prompt_guard_model="meta-llama/Prompt-Guard-86M",
            prompt_guard_sensitivity=0.5,
        )
        self.dynamic_worker_config = DynamicWorkerConfig(
            enabled=True,
            min_active_workers_per_cpu=1,
            spin_down_threshold_seconds=3600,
            spin_up_threshold_seconds=3,
            spin_up_prompt_threshold=5,
        )
        self.end_event = mp.Event()
        self.cpu_barrier = mp.Barrier(2)
        self.dt = MockTelemetry()

    def test_init_basic(self):
        """Test basic initialization of CPUWorker."""
        worker = CPUWorker(
            input_queue=self.input_queue,
            model_config=self.model_config,
            batching_config=self.batching_config,
            guardrails_config=self.guardrails_config,
            dynamic_worker_config=self.dynamic_worker_config,
            num_inf_workers_per_cpu=4,
            end_event=self.end_event,
            cpu_barrier=self.cpu_barrier,
            dt=self.dt,
        )

        # Verify config objects are stored
        self.assertEqual(worker.model_config, self.model_config)
        self.assertEqual(worker.batching_config, self.batching_config)
        self.assertEqual(worker.guardrails_config, self.guardrails_config)
        self.assertEqual(worker.dynamic_worker_config, self.dynamic_worker_config)

        # Verify extracted model values
        self.assertEqual(worker.model_name, "test-model")
        self.assertEqual(worker.dtype, "bfloat16")
        self.assertEqual(worker.hf_token, "test-token")
        self.assertEqual(worker.tp_size, 2)
        self.assertEqual(worker.kv_cache_max_tokens, 100)
        self.assertEqual(worker.max_new_tokens, 100)
        self.assertEqual(worker.padding_side, "left")
        self.assertEqual(worker.truncation_side, "left")
        self.assertEqual(worker.top_k, 50)
        self.assertEqual(worker.top_p, 0.95)

        # Verify batching values
        self.assertTrue(worker.batch_toggle)
        self.assertEqual(worker.batch_wait_time, 0.1)
        self.assertEqual(worker.batch_limit_max, 60)
        self.assertEqual(worker.batch_type, "dynamic")

        # Verify guardrails values
        self.assertTrue(worker.prompt_guard_toggle)
        self.assertEqual(worker.prompt_guard_sensitivity, 0.5)
        self.assertEqual(worker.prompt_guard_model, "meta-llama/Prompt-Guard-86M")

        # Verify dynamic worker values
        self.assertTrue(worker.dynamic_inf_wrkr_toggle)
        self.assertEqual(worker.spin_down_threshold, 3600)
        self.assertEqual(worker.min_active_inf_workers_per_cpu_head, 1)
        self.assertEqual(worker.spin_up_threshold_seconds, 3)
        self.assertEqual(worker.spin_up_prompt_threshold, 5)

        # Verify other attributes
        self.assertEqual(worker.num_inf_workers_for_each_cpu_head, 4)
        self.assertEqual(worker.input_queue, self.input_queue)
        self.assertEqual(worker.end_event, self.end_event)
        self.assertEqual(worker.cpu_barrier, self.cpu_barrier)
        self.assertEqual(worker.dt, self.dt)

    def test_preprocessing_needed_guardrails_enabled(self):
        """Test preprocessing_needed is True when guardrails enabled."""
        worker = CPUWorker(
            input_queue=self.input_queue,
            model_config=self.model_config,
            batching_config=BatchingConfig(enabled=False),
            guardrails_config=GuardrailsConfig(enabled=True),
            dynamic_worker_config=self.dynamic_worker_config,
            num_inf_workers_per_cpu=4,
            end_event=self.end_event,
            cpu_barrier=self.cpu_barrier,
            dt=self.dt,
        )

        self.assertTrue(worker.preprocessing_needed)

    def test_preprocessing_needed_dynamic_batching(self):
        """Test preprocessing_needed is True when dynamic batching enabled."""
        worker = CPUWorker(
            input_queue=self.input_queue,
            model_config=self.model_config,
            batching_config=BatchingConfig(enabled=True, batch_type="dynamic"),
            guardrails_config=GuardrailsConfig(enabled=False),
            dynamic_worker_config=self.dynamic_worker_config,
            num_inf_workers_per_cpu=4,
            end_event=self.end_event,
            cpu_barrier=self.cpu_barrier,
            dt=self.dt,
        )

        self.assertTrue(worker.preprocessing_needed)

    def test_preprocessing_needed_prebatch(self):
        """Test preprocessing_needed is False when using pre-batch without guardrails."""
        worker = CPUWorker(
            input_queue=self.input_queue,
            model_config=self.model_config,
            batching_config=BatchingConfig(enabled=True, batch_type="pre-batch"),
            guardrails_config=GuardrailsConfig(enabled=False),
            dynamic_worker_config=self.dynamic_worker_config,
            num_inf_workers_per_cpu=4,
            end_event=self.end_event,
            cpu_barrier=self.cpu_barrier,
            dt=self.dt,
        )

        self.assertFalse(worker.preprocessing_needed)

    def test_preprocessing_needed_all_disabled(self):
        """Test preprocessing_needed is False when both features disabled."""
        worker = CPUWorker(
            input_queue=self.input_queue,
            model_config=self.model_config,
            batching_config=BatchingConfig(enabled=False),
            guardrails_config=GuardrailsConfig(enabled=False),
            dynamic_worker_config=self.dynamic_worker_config,
            num_inf_workers_per_cpu=4,
            end_event=self.end_event,
            cpu_barrier=self.cpu_barrier,
            dt=self.dt,
        )

        self.assertFalse(worker.preprocessing_needed)


class TestCPUWorkerCreateInfWorker(TestCase):
    """Test cases for create_inf_worker method."""

    @classmethod
    def setUpClass(cls):
        """Set up Dragon multiprocessing."""
        try:
            mp.set_start_method("dragon", force=True)
        except RuntimeError:
            pass  # Already set

    def setUp(self):
        """Set up test fixtures."""
        self.input_queue = mp.Queue()
        self.model_config = ModelConfig(
            model_name="test-model",
            hf_token="test-token",
            tp_size=1,
        )
        self.batching_config = BatchingConfig(enabled=True, batch_type="dynamic")
        self.guardrails_config = GuardrailsConfig(enabled=True)
        self.dynamic_worker_config = DynamicWorkerConfig(enabled=True)
        self.end_event = mp.Event()
        self.cpu_barrier = mp.Barrier(2)
        self.dt = MockTelemetry()

    def test_create_inf_worker_with_preprocessing(self):
        """Test create_inf_worker creates correct process group with preprocessing."""
        worker = CPUWorker(
            input_queue=self.input_queue,
            model_config=self.model_config,
            batching_config=self.batching_config,
            guardrails_config=self.guardrails_config,
            dynamic_worker_config=self.dynamic_worker_config,
            num_inf_workers_per_cpu=4,
            end_event=self.end_event,
            cpu_barrier=self.cpu_barrier,
            dt=self.dt,
        )

        # Verify worker configuration
        self.assertTrue(worker.preprocessing_needed)
        self.assertEqual(worker.tp_size, 1)

    def test_create_inf_worker_without_preprocessing(self):
        """Test create_inf_worker setup without preprocessing."""
        # Disable preprocessing (no guardrails and pre-batch mode)
        worker = CPUWorker(
            input_queue=self.input_queue,
            model_config=self.model_config,
            batching_config=BatchingConfig(enabled=True, batch_type="pre-batch"),
            guardrails_config=GuardrailsConfig(enabled=False),
            dynamic_worker_config=self.dynamic_worker_config,
            num_inf_workers_per_cpu=4,
            end_event=self.end_event,
            cpu_barrier=self.cpu_barrier,
            dt=self.dt,
        )

        # Verify preprocessing is not needed
        self.assertFalse(worker.preprocessing_needed)


class TestCPUWorkerDestroy(TestCase):
    """Test cases for destroy method."""

    @classmethod
    def setUpClass(cls):
        """Set up Dragon multiprocessing."""
        try:
            mp.set_start_method("dragon", force=True)
        except RuntimeError:
            pass  # Already set

    def setUp(self):
        """Set up test fixtures."""
        self.input_queue = mp.Queue()
        self.model_config = ModelConfig(
            model_name="test-model",
            hf_token="test-token",
            tp_size=1,
        )
        self.batching_config = BatchingConfig(enabled=False)
        self.guardrails_config = GuardrailsConfig(enabled=False)
        self.dynamic_worker_config = DynamicWorkerConfig(enabled=False)
        self.end_event = mp.Event()
        self.cpu_barrier = mp.Barrier(2)
        self.dt = MockTelemetry()

    def test_destroy_cleans_up_workers(self):
        """Test destroy properly cleans up all inference workers."""
        worker = CPUWorker(
            input_queue=self.input_queue,
            model_config=self.model_config,
            batching_config=self.batching_config,
            guardrails_config=self.guardrails_config,
            dynamic_worker_config=self.dynamic_worker_config,
            num_inf_workers_per_cpu=4,
            end_event=self.end_event,
            cpu_barrier=self.cpu_barrier,
            dt=self.dt,
        )

        policy = Policy()
        pg_1 = ProcessGroup(restart=False, policy=policy)
        pg_2 = ProcessGroup(restart=False, policy=policy)
        ev_1 = mp.Event()
        ev_2 = mp.Event()

        # Add dummy processes that exit immediately
        def dummy_process():
            pass

        pg_1.add_process(nproc=1, template=ProcessTemplate(target=dummy_process))
        pg_2.add_process(nproc=1, template=ProcessTemplate(target=dummy_process))

        # Initialize and start process groups
        pg_1.init()
        pg_1.start()
        pg_2.init()
        pg_2.start()

        my_inf_workers = [
            (ev_1, pg_1, 1),
            (ev_2, pg_2, 2),
        ]

        # Test destroy method - should properly clean up all workers
        worker.destroy(my_inf_workers)


class TestCPUWorkerDynamicInfWorkers(TestCase):
    """Test cases for dynamic_inf_workers method."""

    @classmethod
    def setUpClass(cls):
        """Set up Dragon multiprocessing."""
        try:
            mp.set_start_method("dragon", force=True)
        except RuntimeError:
            pass  # Already set

    def setUp(self):
        """Set up test fixtures."""
        self.input_queue = mp.Queue()
        self.model_config = ModelConfig(
            model_name="test-model",
            hf_token="test-token",
            tp_size=1,
        )
        self.batching_config = BatchingConfig(enabled=True, batch_type="dynamic")
        self.guardrails_config = GuardrailsConfig(enabled=True)
        self.dynamic_worker_config = DynamicWorkerConfig(
            enabled=True,
            min_active_workers_per_cpu=1,
            spin_down_threshold_seconds=3600,
            spin_up_threshold_seconds=3,
            spin_up_prompt_threshold=5,
        )
        self.end_event = mp.Event()
        self.cpu_barrier = mp.Barrier(2)
        self.dt = MockTelemetry()

    def test_dynamic_inf_workers_removes_spun_down(self):
        """Test dynamic_inf_workers removes workers that have spun down."""
        worker = CPUWorker(
            input_queue=self.input_queue,
            model_config=self.model_config,
            batching_config=self.batching_config,
            guardrails_config=self.guardrails_config,
            dynamic_worker_config=self.dynamic_worker_config,
            num_inf_workers_per_cpu=4,
            end_event=self.end_event,
            cpu_barrier=self.cpu_barrier,
            dt=self.dt,
        )

        # Create inference workers - one active, one spun down
        policy = Policy()

        # Add dummy processes that exit immediately
        def dummy_process():
            pass

        ev_active = mp.Event()  # Not set = active
        pg_active = ProcessGroup(restart=False, policy=policy)
        pg_active.add_process(nproc=1, template=ProcessTemplate(target=dummy_process))
        pg_active.init()
        pg_active.start()

        ev_down = mp.Event()
        ev_down.set()  # Set = spun down
        pg_down = ProcessGroup(restart=False, policy=policy)
        pg_down.add_process(nproc=1, template=ProcessTemplate(target=dummy_process))
        pg_down.init()
        pg_down.start()

        my_inf_workers = [
            (ev_active, pg_active, 1),
            (ev_down, pg_down, 2),
        ]

        mock_manager_q = mp.Queue()

        updated_workers, prompt_count = worker.dynamic_inf_workers(
            my_inf_workers=my_inf_workers,
            cpu_worker_pid=1234,
            inf_wrkr_input_queue=mp.Queue(),
            inf_wrkr_manager_q=mock_manager_q,
            num_input_prompts_since_last_idle=1,
            idle_time_seconds=10,  # More than spin_up_threshold
        )

        # Verify only active worker remains
        self.assertEqual(len(updated_workers), 1)
        self.assertEqual(updated_workers[0][2], 1)  # inf_wrkr_id = 1

        # Clean up process groups manually to avoid Dragon manager errors
        # The spun-down worker has already been cleaned up by dynamic_inf_workers
        for ev, pg, inf_wrkr_id in updated_workers:
            try:
                pg.join()
                pg.stop()
                pg.close()
            except Exception:
                pass  # Ignore cleanup errors in test environment

    def test_dynamic_inf_workers_resets_prompt_count(self):
        """Test dynamic_inf_workers resets prompt count after idle threshold."""
        worker = CPUWorker(
            input_queue=self.input_queue,
            model_config=self.model_config,
            batching_config=self.batching_config,
            guardrails_config=self.guardrails_config,
            dynamic_worker_config=self.dynamic_worker_config,
            num_inf_workers_per_cpu=4,
            end_event=self.end_event,
            cpu_barrier=self.cpu_barrier,
            dt=self.dt,
        )

        my_inf_workers = []
        mock_manager_q = mp.Queue()  # Empty queue

        # Test with idle_time > spin_up_threshold (should reset count)
        _, prompt_count = worker.dynamic_inf_workers(
            my_inf_workers=my_inf_workers,
            cpu_worker_pid=1234,
            inf_wrkr_input_queue=mp.Queue(),
            inf_wrkr_manager_q=mock_manager_q,
            num_input_prompts_since_last_idle=10,
            idle_time_seconds=10,  # More than spin_up_threshold_seconds (3)
        )

        # Prompt count should be reset to 1
        self.assertEqual(prompt_count, 1)


class TestCPUWorkerStartInfWorkers(TestCase):
    """Test cases for start_inf_workers method."""

    @classmethod
    def setUpClass(cls):
        """Set up Dragon multiprocessing."""
        try:
            mp.set_start_method("dragon", force=True)
        except RuntimeError:
            pass  # Already set

    def setUp(self):
        """Set up test fixtures."""
        self.input_queue = mp.Queue()
        self.model_config = ModelConfig(
            model_name="test-model",
            hf_token="test-token",
            tp_size=1,
        )
        self.batching_config = BatchingConfig(enabled=False, batch_type="dynamic")
        self.guardrails_config = GuardrailsConfig(enabled=False)
        self.dynamic_worker_config = DynamicWorkerConfig(enabled=False)
        self.end_event = mp.Event()
        self.cpu_barrier = mp.Barrier(2)
        self.dt = MockTelemetry()

    @patch.object(CPUWorker, "create_inf_worker")
    def test_start_inf_workers_creates_workers(self, mock_create_inf):
        """Test start_inf_workers creates the correct number of workers."""

        # Use threading to prevent barrier hang
        def mock_create_worker_impl(*args, **kwargs):
            """Mock that returns a minimal ProcessGroup and participates in barrier."""
            # Create a minimal real ProcessGroup
            policy = Policy()
            mock_pg = ProcessGroup(restart=False, policy=policy)

            def dummy_process():
                pass

            mock_pg.add_process(nproc=1, template=ProcessTemplate(target=dummy_process))
            mock_pg.init()
            mock_pg.start()

            # Participate in the barrier (simulating what real workers would do)
            # The barrier is passed in args[5] (inf_wrkr_barrier)
            barrier = args[5]

            def wait_on_barrier():
                try:
                    barrier.wait()
                except:
                    pass

            # Start a thread to wait on the barrier (simulating worker process)
            import threading

            t = threading.Thread(target=wait_on_barrier)
            t.daemon = True
            t.start()

            return mock_pg

        mock_create_inf.side_effect = mock_create_worker_impl

        worker = CPUWorker(
            input_queue=self.input_queue,
            model_config=self.model_config,
            batching_config=self.batching_config,
            guardrails_config=self.guardrails_config,
            dynamic_worker_config=self.dynamic_worker_config,
            num_inf_workers_per_cpu=4,
            end_event=self.end_event,
            cpu_barrier=self.cpu_barrier,
            dt=self.dt,
        )

        # start_inf_workers uses these attributes (initialized in initialize(), not __init__)
        worker.log = MagicMock()
        worker.llm_proc_end_events = []
        worker.inf_wrkr_output_queues = []

        inf_wrkr_config = [
            ([0], "29500"),
            ([1], "29501"),
        ]

        # With preprocessing disabled, barrier count should be: 1 + len(inf_wrkr_config) = 3
        # Expected: 1 (main thread) + 2 (one per worker)

        my_inf_workers = worker.start_inf_workers(
            my_inf_workers=[],
            cpu_worker_pid=1234,
            inf_wrkr_input_queue=mp.Queue(),
            inf_wrkr_manager_q=mp.Queue(),
            hostname="test-host",
            inf_wrkr_config=inf_wrkr_config,
        )

        # Verify create_inf_worker was called for each config
        self.assertEqual(mock_create_inf.call_count, 2)

        # Verify workers were added to list
        self.assertEqual(len(my_inf_workers), 2)

        # Verify structure of worker tuples
        for worker_tuple in my_inf_workers:
            self.assertEqual(len(worker_tuple), 3)  # (event, pg, id)
            self.assertIsInstance(worker_tuple[0], type(mp.Event()))  # event
            self.assertIsInstance(worker_tuple[1], ProcessGroup)  # pg
            self.assertIsInstance(worker_tuple[2], int)  # id

        # Clean up all process groups properly
        worker.destroy(my_inf_workers)


if __name__ == "__main__":
    main()
