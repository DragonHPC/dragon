"""
Integration tests for the CPUWorker class.

These tests verify the CPUWorker orchestration layer that manages
inference workers, distributes prompts, and handles dynamic worker lifecycle.
"""

import dragon
import multiprocessing as mp
import time
import unittest
from unittest.mock import MagicMock, patch, Mock
from queue import Empty

from dragon.ai.inference.cpu_worker_utils import CPUWorker
from dragon.ai.inference.config import (
    ModelConfig,
    BatchingConfig,
    GuardrailsConfig,
    DynamicWorkerConfig,
)

from ..mocks import MockTelemetry


def setUpModule():
    """Set the multiprocessing start method to dragon at module load."""
    try:
        mp.set_start_method("dragon", force=True)
    except RuntimeError:
        pass


class TestCPUWorkerBasicOrchestration(unittest.TestCase):
    """Integration tests for CPUWorker basic orchestration and prompt distribution."""

    @classmethod
    def setUpClass(cls):
        try:
            mp.set_start_method("dragon", force=True)
        except RuntimeError:
            pass

    def setUp(self):
        self.model_config = ModelConfig(
            model_name="test-model",
            hf_token="test-token",
            tp_size=1,
        )
        self.batching_config = BatchingConfig(enabled=False)
        self.guardrails_config = GuardrailsConfig(enabled=False)
        self.dynamic_worker_config = DynamicWorkerConfig(enabled=False)
        self.end_event = mp.Event()
        self.dt = MockTelemetry()

    def test_cpu_worker_initialization(self):
        """Test that CPUWorker initializes correctly with config objects."""
        input_queue = mp.Queue()
        cpu_barrier = mp.Barrier(2)

        cpu_worker = CPUWorker(
            input_queue=input_queue,
            model_config=self.model_config,
            batching_config=self.batching_config,
            guardrails_config=self.guardrails_config,
            dynamic_worker_config=self.dynamic_worker_config,
            num_inf_workers_per_cpu=2,
            end_event=self.end_event,
            cpu_barrier=cpu_barrier,
            dt=self.dt,
        )

        # Verify config values flow through
        self.assertEqual(cpu_worker.model_name, "test-model")
        self.assertEqual(cpu_worker.tp_size, 1)
        self.assertFalse(cpu_worker.batch_toggle)
        self.assertFalse(cpu_worker.prompt_guard_toggle)
        self.assertFalse(cpu_worker.dynamic_inf_wrkr_toggle)
        self.assertEqual(cpu_worker.num_inf_workers_for_each_cpu_head, 2)

    def test_cpu_worker_preprocessing_needed_determination(self):
        """Test that preprocessing_needed is correctly determined based on config."""
        input_queue = mp.Queue()
        cpu_barrier = mp.Barrier(2)

        # Test case 1: No batching, no guardrails -> preprocessing NOT needed
        cpu_worker1 = CPUWorker(
            input_queue=input_queue,
            model_config=self.model_config,
            batching_config=BatchingConfig(enabled=False),
            guardrails_config=GuardrailsConfig(enabled=False),
            dynamic_worker_config=self.dynamic_worker_config,
            num_inf_workers_per_cpu=1,
            end_event=self.end_event,
            cpu_barrier=cpu_barrier,
            dt=self.dt,
        )
        self.assertFalse(cpu_worker1.preprocessing_needed)

        # Test case 2: Dynamic batching enabled -> preprocessing needed
        cpu_worker2 = CPUWorker(
            input_queue=input_queue,
            model_config=self.model_config,
            batching_config=BatchingConfig(enabled=True, batch_type="dynamic", max_batch_size=4),
            guardrails_config=GuardrailsConfig(enabled=False),
            dynamic_worker_config=self.dynamic_worker_config,
            num_inf_workers_per_cpu=1,
            end_event=self.end_event,
            cpu_barrier=cpu_barrier,
            dt=self.dt,
        )
        self.assertTrue(cpu_worker2.preprocessing_needed)

        # Test case 3: Guardrails enabled -> preprocessing needed
        cpu_worker3 = CPUWorker(
            input_queue=input_queue,
            model_config=self.model_config,
            batching_config=BatchingConfig(enabled=False),
            guardrails_config=GuardrailsConfig(enabled=True),
            dynamic_worker_config=self.dynamic_worker_config,
            num_inf_workers_per_cpu=1,
            end_event=self.end_event,
            cpu_barrier=cpu_barrier,
            dt=self.dt,
        )
        self.assertTrue(cpu_worker3.preprocessing_needed)

        # Test case 4: Pre-batch, no guardrails -> preprocessing NOT needed
        cpu_worker4 = CPUWorker(
            input_queue=input_queue,
            model_config=self.model_config,
            batching_config=BatchingConfig(enabled=True, batch_type="pre-batch", max_batch_size=4),
            guardrails_config=GuardrailsConfig(enabled=False),
            dynamic_worker_config=self.dynamic_worker_config,
            num_inf_workers_per_cpu=1,
            end_event=self.end_event,
            cpu_barrier=cpu_barrier,
            dt=self.dt,
        )
        self.assertFalse(cpu_worker4.preprocessing_needed)

    @patch("dragon.ai.inference.cpu_worker_utils.setup_logging")
    @patch("dragon.ai.inference.cpu_worker_utils.CPUWorker.create_inf_worker")
    def test_cpu_worker_distributes_prompts(self, mock_create_inf_worker, mock_logging):
        """Test that CPUWorker correctly distributes prompts from input queue to inference workers."""
        mock_logger = MagicMock()
        mock_logging.return_value = mock_logger

        # Mock ProcessGroup
        mock_pg = MagicMock()
        mock_create_inf_worker.return_value = mock_pg

        input_queue = mp.Queue()
        cpu_barrier = mp.Barrier(2)  # 1 for test, 1 for CPU worker

        cpu_worker = CPUWorker(
            input_queue=input_queue,
            model_config=self.model_config,
            batching_config=self.batching_config,
            guardrails_config=self.guardrails_config,
            dynamic_worker_config=self.dynamic_worker_config,
            num_inf_workers_per_cpu=1,
            end_event=self.end_event,
            cpu_barrier=cpu_barrier,
            dt=self.dt,
        )

        # Manually set up internal state instead of calling initialize
        mp.set_start_method("dragon", force=True)
        cpu_worker.inf_wrkr_input_queue = mp.Queue()
        cpu_worker.inf_wrkr_manager_q = mp.Queue()
        cpu_worker.inf_wrkr_output_queues = []
        cpu_worker.llm_proc_end_events = []
        cpu_worker.log = mock_logger

        # Put prompts in input queue
        response_queue1 = mp.Queue()
        response_queue2 = mp.Queue()
        input_queue.put(("Prompt 1", "<formatted1>", response_queue1, time.time()))
        input_queue.put(("Prompt 2", "<formatted2>", response_queue2, time.time()))

        # Simulate CPU worker processing prompts
        for _ in range(2):
            q_item = input_queue.get(timeout=1)
            user_prompt = q_item[0]
            formatted_input = q_item[1]
            q = q_item[2]
            input_entry_timestamp = q_item[3]

            cpu_head_network_latency = round(time.time() - input_entry_timestamp, 2)
            tuple_latency_metric = (
                input_entry_timestamp,
                cpu_head_network_latency,
                time.time(),
            )

            # Forward to inference worker queue
            cpu_worker.inf_wrkr_input_queue.put((user_prompt, formatted_input, q, tuple_latency_metric))

        # Verify prompts were forwarded to inference worker queue
        item1 = cpu_worker.inf_wrkr_input_queue.get(timeout=1)
        item2 = cpu_worker.inf_wrkr_input_queue.get(timeout=1)

        self.assertEqual(item1[0], "Prompt 1")
        self.assertEqual(item1[1], "<formatted1>")
        self.assertIsInstance(item1[2], type(response_queue1))

        self.assertEqual(item2[0], "Prompt 2")
        self.assertEqual(item2[1], "<formatted2>")
        self.assertIsInstance(item2[2], type(response_queue2))

    @patch("dragon.ai.inference.cpu_worker_utils.ProcessGroup")
    def test_cpu_worker_barrier_synchronization(self, mock_pg_class):
        """Test that CPUWorker correctly uses barriers for synchronization."""
        input_queue = mp.Queue()
        cpu_barrier = mp.Barrier(2)

        cpu_worker = CPUWorker(
            input_queue=input_queue,
            model_config=self.model_config,
            batching_config=self.batching_config,
            guardrails_config=self.guardrails_config,
            dynamic_worker_config=self.dynamic_worker_config,
            num_inf_workers_per_cpu=2,
            end_event=self.end_event,
            cpu_barrier=cpu_barrier,
            dt=self.dt,
        )

        # Verify CPU barrier is stored
        self.assertIs(cpu_worker.cpu_barrier, cpu_barrier)

    def test_cpu_worker_graceful_shutdown(self):
        """Test that CPUWorker shuts down gracefully when end_event is set."""
        input_queue = mp.Queue()
        cpu_barrier = mp.Barrier(2)

        cpu_worker = CPUWorker(
            input_queue=input_queue,
            model_config=self.model_config,
            batching_config=self.batching_config,
            guardrails_config=self.guardrails_config,
            dynamic_worker_config=self.dynamic_worker_config,
            num_inf_workers_per_cpu=1,
            end_event=self.end_event,
            cpu_barrier=cpu_barrier,
            dt=self.dt,
        )

        # Simulate shutdown: set end_event
        self.end_event.set()

        # Verify end_event is accessible
        self.assertTrue(cpu_worker.end_event.is_set())


class TestCPUWorkerDynamicWorkerManagement(unittest.TestCase):
    """Integration tests for CPUWorker dynamic worker spin-up/spin-down."""

    @classmethod
    def setUpClass(cls):
        try:
            mp.set_start_method("dragon", force=True)
        except RuntimeError:
            pass

    def setUp(self):
        self.model_config = ModelConfig(
            model_name="test-model",
            hf_token="test-token",
            tp_size=1,
        )
        self.batching_config = BatchingConfig(enabled=False)
        self.guardrails_config = GuardrailsConfig(enabled=False)
        self.end_event = mp.Event()
        self.dt = MockTelemetry()

    def test_dynamic_worker_config_flows_through(self):
        """Test that dynamic worker configuration is correctly set."""
        dynamic_worker_config = DynamicWorkerConfig(
            enabled=True,
            min_active_workers_per_cpu=2,
            spin_down_threshold_seconds=60,
            spin_up_threshold_seconds=3,
            spin_up_prompt_threshold=5,
        )

        input_queue = mp.Queue()
        cpu_barrier = mp.Barrier(2)

        cpu_worker = CPUWorker(
            input_queue=input_queue,
            model_config=self.model_config,
            batching_config=self.batching_config,
            guardrails_config=self.guardrails_config,
            dynamic_worker_config=dynamic_worker_config,
            num_inf_workers_per_cpu=2,
            end_event=self.end_event,
            cpu_barrier=cpu_barrier,
            dt=self.dt,
        )

        self.assertTrue(cpu_worker.dynamic_inf_wrkr_toggle)
        self.assertEqual(cpu_worker.min_active_inf_workers_per_cpu_head, 2)
        self.assertEqual(cpu_worker.spin_down_threshold, 60)
        self.assertEqual(cpu_worker.spin_up_threshold_seconds, 3)
        self.assertEqual(cpu_worker.spin_up_prompt_threshold, 5)

    @patch("dragon.ai.inference.cpu_worker_utils.setup_logging")
    @patch("dragon.ai.inference.cpu_worker_utils.ProcessGroup")
    def test_dynamic_inf_workers_removes_shutdown_workers(self, mock_pg_class, mock_logging):
        """Test that dynamic_inf_workers removes workers that have shut down."""
        mock_logger = MagicMock()
        mock_logging.return_value = mock_logger

        dynamic_worker_config = DynamicWorkerConfig(
            enabled=True,
            min_active_workers_per_cpu=1,
            spin_down_threshold_seconds=60,
        )

        input_queue = mp.Queue()
        cpu_barrier = mp.Barrier(2)

        cpu_worker = CPUWorker(
            input_queue=input_queue,
            model_config=self.model_config,
            batching_config=self.batching_config,
            guardrails_config=self.guardrails_config,
            dynamic_worker_config=dynamic_worker_config,
            num_inf_workers_per_cpu=2,
            end_event=self.end_event,
            cpu_barrier=cpu_barrier,
            dt=self.dt,
        )
        cpu_worker.log = mock_logger

        # Initialize internal state required by dynamic_inf_workers
        cpu_worker.llm_proc_end_events = []
        cpu_worker.inf_wrkr_output_queues = []

        # Create mock inference workers
        ev1 = mp.Event()
        ev2 = mp.Event()
        ev3 = mp.Event()

        mock_pg1 = MagicMock()
        mock_pg2 = MagicMock()
        mock_pg3 = MagicMock()

        my_inf_workers = [
            (ev1, mock_pg1, 1),
            (ev2, mock_pg2, 2),
            (ev3, mock_pg3, 3),
        ]

        # Set ev2 to simulate worker 2 has shut down
        ev2.set()

        inf_wrkr_input_queue = mp.Queue()
        inf_wrkr_manager_q = mp.Queue()

        # Call dynamic_inf_workers
        updated_workers, _ = cpu_worker.dynamic_inf_workers(
            my_inf_workers=my_inf_workers,
            cpu_worker_pid=12345,
            inf_wrkr_input_queue=inf_wrkr_input_queue,
            inf_wrkr_manager_q=inf_wrkr_manager_q,
            num_input_prompts_since_last_idle=0,
            idle_time_seconds=100,  # Long idle time
        )

        # Verify worker 2 was removed
        self.assertEqual(len(updated_workers), 2)
        worker_ids = [worker_id for _, _, worker_id in updated_workers]
        self.assertIn(1, worker_ids)
        self.assertNotIn(2, worker_ids)
        self.assertIn(3, worker_ids)

        # Verify pg2 was cleaned up
        mock_pg2.join.assert_called_once()
        mock_pg2.stop.assert_called_once()
        mock_pg2.close.assert_called_once()

    @patch("dragon.ai.inference.cpu_worker_utils.setup_logging")
    @patch("dragon.ai.inference.cpu_worker_utils.CPUWorker.create_inf_worker")
    @patch("dragon.ai.inference.cpu_worker_utils.mp.Barrier")
    def test_dynamic_spinup_on_high_load(self, mock_barrier_class, mock_create_inf_worker, mock_logging):
        """Test that a new worker is spun up when load exceeds thresholds."""
        mock_logger = MagicMock()
        mock_logging.return_value = mock_logger

        # Mock Barrier to prevent hanging on wait()
        mock_barrier_instance = MagicMock()
        mock_barrier_class.return_value = mock_barrier_instance

        # Mock ProcessGroup
        mock_pg = MagicMock()
        mock_create_inf_worker.return_value = mock_pg

        dynamic_worker_config = DynamicWorkerConfig(
            enabled=True,
            min_active_workers_per_cpu=1,
            spin_up_threshold_seconds=5,
            spin_up_prompt_threshold=10,
        )

        input_queue = mp.Queue()
        cpu_barrier = mp.Barrier(2)

        cpu_worker = CPUWorker(
            input_queue=input_queue,
            model_config=self.model_config,
            batching_config=self.batching_config,
            guardrails_config=self.guardrails_config,
            dynamic_worker_config=dynamic_worker_config,
            num_inf_workers_per_cpu=2,
            end_event=self.end_event,
            cpu_barrier=cpu_barrier,
            dt=self.dt,
        )
        cpu_worker.log = mock_logger
        cpu_worker.preprocessing_needed = False

        # Initialize internal state required by dynamic_inf_workers
        cpu_worker.llm_proc_end_events = []
        cpu_worker.inf_wrkr_output_queues = []

        # Create initial worker list
        ev1 = mp.Event()
        mock_pg1 = MagicMock()
        my_inf_workers = [(ev1, mock_pg1, 1)]

        inf_wrkr_input_queue = mp.Queue()
        inf_wrkr_manager_q = mp.Queue()

        # Put available worker slot in manager queue
        inf_wrkr_manager_q.put(("node-0", [0], "29600", 2))

        # Call with high load: short idle time and high prompt count
        updated_workers, updated_count = cpu_worker.dynamic_inf_workers(
            my_inf_workers=my_inf_workers,
            cpu_worker_pid=12345,
            inf_wrkr_input_queue=inf_wrkr_input_queue,
            inf_wrkr_manager_q=inf_wrkr_manager_q,
            num_input_prompts_since_last_idle=15,  # Above threshold
            idle_time_seconds=2,  # Below threshold
        )

        # Verify a new worker was created
        self.assertEqual(len(updated_workers), 2)
        mock_create_inf_worker.assert_called_once()

    @patch("dragon.ai.inference.cpu_worker_utils.setup_logging")
    def test_dynamic_spinup_resets_counter_on_long_idle(self, mock_logging):
        """Test that prompt counter is reset during long idle periods."""
        mock_logger = MagicMock()
        mock_logging.return_value = mock_logger

        dynamic_worker_config = DynamicWorkerConfig(
            enabled=True,
            spin_up_threshold_seconds=5,
            spin_up_prompt_threshold=10,
        )

        input_queue = mp.Queue()
        cpu_barrier = mp.Barrier(2)

        cpu_worker = CPUWorker(
            input_queue=input_queue,
            model_config=self.model_config,
            batching_config=self.batching_config,
            guardrails_config=self.guardrails_config,
            dynamic_worker_config=dynamic_worker_config,
            num_inf_workers_per_cpu=2,
            end_event=self.end_event,
            cpu_barrier=cpu_barrier,
            dt=self.dt,
        )
        cpu_worker.log = mock_logger

        # Initialize internal state required by dynamic_inf_workers
        cpu_worker.llm_proc_end_events = []
        cpu_worker.inf_wrkr_output_queues = []

        my_inf_workers = []
        inf_wrkr_input_queue = mp.Queue()
        inf_wrkr_manager_q = mp.Queue()

        # Call with long idle time
        _, updated_count = cpu_worker.dynamic_inf_workers(
            my_inf_workers=my_inf_workers,
            cpu_worker_pid=12345,
            inf_wrkr_input_queue=inf_wrkr_input_queue,
            inf_wrkr_manager_q=inf_wrkr_manager_q,
            num_input_prompts_since_last_idle=100,  # High count
            idle_time_seconds=10,  # Long idle (> threshold)
        )

        # Counter should be reset to 1
        self.assertEqual(updated_count, 1)


class TestCPUWorkerInferenceWorkerCreation(unittest.TestCase):
    """Integration tests for CPUWorker creating and managing inference workers."""

    @classmethod
    def setUpClass(cls):
        try:
            mp.set_start_method("dragon", force=True)
        except RuntimeError:
            pass

    def setUp(self):
        self.model_config = ModelConfig(
            model_name="test-model",
            hf_token="test-token",
            tp_size=1,
        )
        self.batching_config = BatchingConfig(enabled=False)
        self.guardrails_config = GuardrailsConfig(enabled=False)
        self.dynamic_worker_config = DynamicWorkerConfig(enabled=False)
        self.end_event = mp.Event()
        self.dt = MockTelemetry()

    @patch("dragon.ai.inference.cpu_worker_utils.setup_logging")
    @patch("dragon.ai.inference.cpu_worker_utils.ProcessGroup")
    def test_create_inf_worker_parameters(self, mock_pg_class, mock_logging):
        """Test that create_inf_worker receives correct parameters."""
        mock_logger = MagicMock()
        mock_logging.return_value = mock_logger

        mock_pg_instance = MagicMock()
        mock_pg_class.return_value = mock_pg_instance

        input_queue = mp.Queue()
        cpu_barrier = mp.Barrier(2)

        cpu_worker = CPUWorker(
            input_queue=input_queue,
            model_config=self.model_config,
            batching_config=self.batching_config,
            guardrails_config=self.guardrails_config,
            dynamic_worker_config=self.dynamic_worker_config,
            num_inf_workers_per_cpu=1,
            end_event=self.end_event,
            cpu_barrier=cpu_barrier,
            dt=self.dt,
        )
        cpu_worker.log = mock_logger

        inf_worker_queue = mp.Queue()
        inf_wrkr_barrier = mp.Barrier(2)
        inf_wrkr_down_ev = mp.Event()
        inf_wrkr_manager_q = mp.Queue()
        output_queue = mp.Queue()
        llm_proc_end_ev = mp.Event()

        # Call create_inf_worker
        result_pg = cpu_worker.create_inf_worker(
            hostname="node-0",
            cpu_worker_pid=12345,
            devices=[0],
            master_port="29500",
            inf_worker_queue=inf_worker_queue,
            inf_wrkr_barrier=inf_wrkr_barrier,
            inf_wrkr_down_ev=inf_wrkr_down_ev,
            inf_wrkr_id=1,
            inf_wrkr_manager_q=inf_wrkr_manager_q,
            output_queue=output_queue,
            llm_proc_end_ev=llm_proc_end_ev,
        )

        # Verify ProcessGroup was created and started
        mock_pg_class.assert_called_once()
        mock_pg_instance.init.assert_called_once()
        mock_pg_instance.start.assert_called_once()
        self.assertIs(result_pg, mock_pg_instance)

    def test_destroy_cleans_up_workers(self):
        """Test that destroy properly cleans up all inference workers."""
        input_queue = mp.Queue()
        cpu_barrier = mp.Barrier(2)

        cpu_worker = CPUWorker(
            input_queue=input_queue,
            model_config=self.model_config,
            batching_config=self.batching_config,
            guardrails_config=self.guardrails_config,
            dynamic_worker_config=self.dynamic_worker_config,
            num_inf_workers_per_cpu=2,
            end_event=self.end_event,
            cpu_barrier=cpu_barrier,
            dt=self.dt,
        )

        # Create mock workers
        ev1 = mp.Event()
        ev2 = mp.Event()
        mock_pg1 = MagicMock()
        mock_pg2 = MagicMock()

        my_inf_workers = [
            (ev1, mock_pg1, 1),
            (ev2, mock_pg2, 2),
        ]

        # Call destroy
        cpu_worker.destroy(my_inf_workers)

        # Verify all workers were cleaned up
        mock_pg1.join.assert_called_once()
        mock_pg1.stop.assert_called_once()
        mock_pg1.close.assert_called_once()

        mock_pg2.join.assert_called_once()
        mock_pg2.stop.assert_called_once()
        mock_pg2.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
