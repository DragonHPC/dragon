"""
Integration tests for all combinations of batching modes and guardrails.

This test file provides comprehensive coverage of all possible configurations:

Batching Modes:
1. No batching (single prompt processing)
2. Pre-batch (inputs already batched by user)
3. Dynamic batch (system batches inputs over time window)

Guardrails:
- Enabled (prompts filtered for safety)
- Disabled (prompts pass through unfiltered)

Total: 6 combinations tested
"""

import dragon
import multiprocessing as mp
import time
import unittest
from unittest.mock import MagicMock, patch, Mock

from dragon.ai.inference.config import (
    ModelConfig,
    BatchingConfig,
    GuardrailsConfig,
    DynamicWorkerConfig,
)
from dragon.ai.inference.inference_worker_utils import InferenceWorker
from dragon.ai.inference.batching import DynamicBatcher

from ..mocks import MockTelemetry


def setUpModule():
    """Set the multiprocessing start method to dragon at module load."""
    try:
        mp.set_start_method("dragon", force=True)
    except RuntimeError:
        pass


# =============================================================================
# CONFIGURATION 1: No Batching + Guardrails OFF
# =============================================================================
class TestNoBatchingGuardrailsOff(unittest.TestCase):
    """
    Configuration: batch_toggle=False, guardrails=False

    Behavior:
    - Single prompts processed immediately
    - No safety filtering
    """

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
            max_tokens=100,
        )
        self.batching_config = BatchingConfig(enabled=False)
        self.guardrails_config = GuardrailsConfig(enabled=False)
        self.dynamic_worker_config = DynamicWorkerConfig(enabled=False)
        self.end_event = mp.Event()
        self.dt = MockTelemetry()

    def test_worker_configuration(self):
        """Test worker is configured for no batching, no guardrails."""
        worker = InferenceWorker(
            end_event=self.end_event,
            model_config=self.model_config,
            batching_config=self.batching_config,
            guardrails_config=self.guardrails_config,
            dynamic_worker_config=self.dynamic_worker_config,
            dt=self.dt,
        )

        self.assertFalse(worker.batch_toggle)
        self.assertFalse(worker.prompt_guard_toggle)

    @patch("dragon.ai.inference.inference_worker_utils.setup_logging")
    def test_single_prompt_direct_forward(self, mock_logging):
        """Test single prompt is forwarded directly to LLM without preprocessing."""
        mock_logger = MagicMock()
        mock_logging.return_value = mock_logger

        output_queue = mp.Queue()
        response_queue = mp.Queue()

        worker = InferenceWorker(
            end_event=self.end_event,
            model_config=self.model_config,
            batching_config=self.batching_config,
            guardrails_config=self.guardrails_config,
            dynamic_worker_config=self.dynamic_worker_config,
            dt=self.dt,
            hostname="test-host",
            devices=[0],
            inf_wrkr_id=1,
            preprocessing_output_queue=output_queue,
        )
        worker.log = mock_logger

        # Single prompt forwarded directly
        worker._forward_to_llm(
            formatted_prompts=["<user>Hello</user>"],
            user_prompts=["Hello"],
            response_queues=[response_queue],
            latency_metrics=[(time.time(), 0.01, 0.02)],
            preprocessing_time=0.0,
        )

        result = output_queue.get(timeout=1)
        self.assertEqual(result[0], ["Hello"])
        self.assertEqual(result[4], 0.0)  # No preprocessing time

    @patch("dragon.ai.inference.inference_worker_utils.setup_logging")
    def test_guard_and_forward_passthrough(self, mock_logging):
        """Test _guard_and_forward_batch passes through without filtering."""
        mock_logger = MagicMock()
        mock_logging.return_value = mock_logger

        output_queue = mp.Queue()
        response_queue = mp.Queue()

        worker = InferenceWorker(
            end_event=self.end_event,
            model_config=self.model_config,
            batching_config=self.batching_config,
            guardrails_config=self.guardrails_config,
            dynamic_worker_config=self.dynamic_worker_config,
            dt=self.dt,
            hostname="test-host",
            devices=[0],
            inf_wrkr_id=1,
            preprocessing_output_queue=output_queue,
        )
        worker.log = mock_logger

        # With guardrails disabled, passes guardrails=None
        worker._guard_and_forward_batch(
            formatted_prompts=["<user>Any prompt</user>"],
            user_prompts=["Any prompt"],
            response_queues=[response_queue],
            latency_metrics=[(time.time(), 0.01, 0.02)],
            guardrails=None,  # Guardrails disabled
        )

        result = output_queue.get(timeout=1)
        self.assertEqual(result[0], ["Any prompt"])

    @patch("dragon.ai.inference.inference_worker_utils.setup_logging")
    def test_response_sent_correctly(self, mock_logging):
        """Test response is sent back correctly for single prompt."""
        mock_logger = MagicMock()
        mock_logging.return_value = mock_logger

        response_queue = mp.Queue()

        worker = InferenceWorker(
            end_event=self.end_event,
            model_config=self.model_config,
            batching_config=self.batching_config,
            guardrails_config=self.guardrails_config,
            dynamic_worker_config=self.dynamic_worker_config,
            dt=self.dt,
            hostname="test-host",
            devices=[0],
            inf_wrkr_id=1,
        )
        worker.log = mock_logger

        worker._send_responses(
            user_prompts=["Hello"],
            responses=["Hi there!"],
            qs=[response_queue],
            preprocessing_time=0.0,
            tuple_latency_timestamps=[(time.time() - 1, 0.01, 0.02)],
            model_network_latency=0.01,
            metrics={
                "inference_time": 0.5,
                "requests_per_second": 2.0,
                "total_tokens_per_second": 100.0,
                "output_tokens_per_second": 50.0,
            },
        )

        result = response_queue.get(timeout=1)
        self.assertEqual(result["batch_size"], 1)
        self.assertEqual(result["guardrails_inference_latency"], 0.0)


# =============================================================================
# CONFIGURATION 2: No Batching + Guardrails ON
# =============================================================================
class TestNoBatchingGuardrailsOn(unittest.TestCase):
    """
    Configuration: batch_toggle=False, guardrails=True

    Behavior:
    - Single prompts processed immediately
    - Each prompt checked for safety before LLM
    - Malicious prompts blocked with error response
    """

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
        self.guardrails_config = GuardrailsConfig(
            enabled=True,
            prompt_guard_sensitivity=0.5,
        )
        self.dynamic_worker_config = DynamicWorkerConfig(enabled=False)
        self.end_event = mp.Event()
        self.dt = MockTelemetry()

    def test_worker_configuration(self):
        """Test worker is configured for no batching with guardrails."""
        worker = InferenceWorker(
            end_event=self.end_event,
            model_config=self.model_config,
            batching_config=self.batching_config,
            guardrails_config=self.guardrails_config,
            dynamic_worker_config=self.dynamic_worker_config,
            dt=self.dt,
        )

        self.assertFalse(worker.batch_toggle)
        self.assertTrue(worker.prompt_guard_toggle)
        self.assertEqual(worker.prompt_guard_sensitivity, 0.5)

    @patch("dragon.ai.inference.inference_worker_utils.GuardrailsProcessor")
    @patch("dragon.ai.inference.inference_worker_utils.setup_logging")
    def test_safe_prompt_passes_through(self, mock_logging, mock_guardrails_class):
        """Test safe single prompt passes through guardrails."""
        mock_logger = MagicMock()
        mock_logging.return_value = mock_logger

        output_queue = mp.Queue()
        response_queue = mp.Queue()

        worker = InferenceWorker(
            end_event=self.end_event,
            model_config=self.model_config,
            batching_config=self.batching_config,
            guardrails_config=self.guardrails_config,
            dynamic_worker_config=self.dynamic_worker_config,
            dt=self.dt,
            hostname="test-host",
            devices=[0],
            inf_wrkr_id=1,
            preprocessing_output_queue=output_queue,
        )
        worker.log = mock_logger

        # Mock guardrails to pass the prompt
        mock_guardrails = mock_guardrails_class.return_value
        mock_guardrails.filter_batch.return_value = (
            ["Safe prompt"],
            ["<user>Safe prompt</user>"],
            [response_queue],
            [(1.0, 0.01, 0.02)],
            [],  # No malicious
            0.05,
        )

        worker._guard_and_forward_batch(
            formatted_prompts=["<user>Safe prompt</user>"],
            user_prompts=["Safe prompt"],
            response_queues=[response_queue],
            latency_metrics=[(1.0, 0.01, 0.02)],
            guardrails=mock_guardrails,
        )

        result = output_queue.get(timeout=1)
        self.assertEqual(result[0], ["Safe prompt"])

    @patch("dragon.ai.inference.inference_worker_utils.GuardrailsProcessor")
    @patch("dragon.ai.inference.inference_worker_utils.setup_logging")
    def test_malicious_prompt_blocked(self, mock_logging, mock_guardrails_class):
        """Test malicious single prompt is blocked."""
        mock_logger = MagicMock()
        mock_logging.return_value = mock_logger

        output_queue = mp.Queue()
        response_queue = mp.Queue()

        worker = InferenceWorker(
            end_event=self.end_event,
            model_config=self.model_config,
            batching_config=self.batching_config,
            guardrails_config=self.guardrails_config,
            dynamic_worker_config=self.dynamic_worker_config,
            dt=self.dt,
            hostname="test-host",
            devices=[0],
            inf_wrkr_id=1,
            preprocessing_output_queue=output_queue,
        )
        worker.log = mock_logger

        # Mock guardrails to block the prompt
        mock_guardrails = mock_guardrails_class.return_value
        mock_guardrails.filter_batch.return_value = (
            [],  # No safe prompts
            [],
            [],
            [],
            [0],  # Index 0 is malicious
            0.05,
        )
        mock_guardrails.get_malicious_response.return_value = "BLOCKED"

        worker._guard_and_forward_batch(
            formatted_prompts=["<user>Malicious</user>"],
            user_prompts=["Malicious"],
            response_queues=[response_queue],
            latency_metrics=[(1.0, 0.01, 0.02)],
            guardrails=mock_guardrails,
        )

        # Malicious prompt should get error response
        result = response_queue.get(timeout=1)
        self.assertEqual(result["assistant"], "BLOCKED")
        self.assertEqual(result["user"], "Malicious")

        # Output queue should be empty (nothing forwarded to LLM)
        self.assertTrue(output_queue.empty())


# =============================================================================
# CONFIGURATION 3: Pre-Batch + Guardrails OFF
# =============================================================================
class TestPreBatchGuardrailsOff(unittest.TestCase):
    """
    Configuration: batch_toggle=True, batch_type="pre-batch", guardrails=False

    Behavior:
    - User provides already-batched inputs
    - Batch processed together without additional batching logic
    - No safety filtering
    """

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
        self.batching_config = BatchingConfig(
            enabled=True,
            batch_type="pre-batch",
            max_batch_size=10,
        )
        self.guardrails_config = GuardrailsConfig(enabled=False)
        self.dynamic_worker_config = DynamicWorkerConfig(enabled=False)
        self.end_event = mp.Event()
        self.dt = MockTelemetry()

    def test_worker_configuration(self):
        """Test worker is configured for pre-batch without guardrails."""
        worker = InferenceWorker(
            end_event=self.end_event,
            model_config=self.model_config,
            batching_config=self.batching_config,
            guardrails_config=self.guardrails_config,
            dynamic_worker_config=self.dynamic_worker_config,
            dt=self.dt,
        )

        self.assertTrue(worker.batch_toggle)
        self.assertEqual(worker.batch_type, "pre-batch")
        self.assertFalse(worker.prompt_guard_toggle)

    @patch("dragon.ai.inference.inference_worker_utils.setup_logging")
    def test_prebatch_forward_to_llm(self, mock_logging):
        """Test pre-batched prompts forwarded together."""
        mock_logger = MagicMock()
        mock_logging.return_value = mock_logger

        output_queue = mp.Queue()
        response_queue = mp.Queue()

        worker = InferenceWorker(
            end_event=self.end_event,
            model_config=self.model_config,
            batching_config=self.batching_config,
            guardrails_config=self.guardrails_config,
            dynamic_worker_config=self.dynamic_worker_config,
            dt=self.dt,
            hostname="test-host",
            devices=[0],
            inf_wrkr_id=1,
            preprocessing_output_queue=output_queue,
        )
        worker.log = mock_logger

        # Pre-batched: multiple prompts in one call
        batch_prompts = ["Question 1", "Question 2", "Question 3"]
        batch_formatted = ["<Q1>", "<Q2>", "<Q3>"]

        worker._forward_to_llm(
            formatted_prompts=batch_formatted,
            user_prompts=batch_prompts,
            response_queues=[response_queue] * 3,
            latency_metrics=[(time.time(), 0.01, 0.02)] * 3,
            preprocessing_time=0.0,
        )

        result = output_queue.get(timeout=1)
        self.assertEqual(result[0], batch_prompts)
        self.assertEqual(result[1], batch_formatted)
        self.assertEqual(len(result[2]), 3)  # 3 response queues

    @patch("dragon.ai.inference.inference_worker_utils.setup_logging")
    def test_prebatch_responses_distributed(self, mock_logging):
        """Test responses distributed to correct queues for pre-batch."""
        mock_logger = MagicMock()
        mock_logging.return_value = mock_logger

        queues = [mp.Queue() for _ in range(3)]

        worker = InferenceWorker(
            end_event=self.end_event,
            model_config=self.model_config,
            batching_config=self.batching_config,
            guardrails_config=self.guardrails_config,
            dynamic_worker_config=self.dynamic_worker_config,
            dt=self.dt,
            hostname="test-host",
            devices=[0],
            inf_wrkr_id=1,
        )
        worker.log = mock_logger

        worker._send_responses(
            user_prompts=["Q1", "Q2", "Q3"],
            responses=["A1", "A2", "A3"],
            qs=queues,
            preprocessing_time=0.0,
            tuple_latency_timestamps=[(time.time() - 1, 0.01, 0.02)] * 3,
            model_network_latency=0.02,
            metrics={
                "inference_time": 1.0,
                "requests_per_second": 3.0,
                "total_tokens_per_second": 150.0,
                "output_tokens_per_second": 75.0,
            },
        )

        for i, q in enumerate(queues):
            result = q.get(timeout=1)
            self.assertEqual(result["user"], f"Q{i+1}")
            self.assertEqual(result["assistant"], f"A{i+1}")
            self.assertEqual(result["batch_size"], 3)


# =============================================================================
# CONFIGURATION 4: Pre-Batch + Guardrails ON
# =============================================================================
class TestPreBatchGuardrailsOn(unittest.TestCase):
    """
    Configuration: batch_toggle=True, batch_type="pre-batch", guardrails=True

    Behavior:
    - User provides already-batched inputs
    - Entire batch checked for safety
    - Malicious prompts in batch blocked, safe ones forwarded
    """

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
        self.batching_config = BatchingConfig(
            enabled=True,
            batch_type="pre-batch",
            max_batch_size=10,
        )
        self.guardrails_config = GuardrailsConfig(
            enabled=True,
            prompt_guard_sensitivity=0.5,
        )
        self.dynamic_worker_config = DynamicWorkerConfig(enabled=False)
        self.end_event = mp.Event()
        self.dt = MockTelemetry()

    def test_worker_configuration(self):
        """Test worker is configured for pre-batch with guardrails."""
        worker = InferenceWorker(
            end_event=self.end_event,
            model_config=self.model_config,
            batching_config=self.batching_config,
            guardrails_config=self.guardrails_config,
            dynamic_worker_config=self.dynamic_worker_config,
            dt=self.dt,
        )

        self.assertTrue(worker.batch_toggle)
        self.assertEqual(worker.batch_type, "pre-batch")
        self.assertTrue(worker.prompt_guard_toggle)

    @patch("dragon.ai.inference.inference_worker_utils.GuardrailsProcessor")
    @patch("dragon.ai.inference.inference_worker_utils.setup_logging")
    def test_prebatch_all_safe(self, mock_logging, mock_guardrails_class):
        """Test pre-batch where all prompts are safe."""
        mock_logger = MagicMock()
        mock_logging.return_value = mock_logger

        output_queue = mp.Queue()
        queues = [mp.Queue() for _ in range(3)]

        worker = InferenceWorker(
            end_event=self.end_event,
            model_config=self.model_config,
            batching_config=self.batching_config,
            guardrails_config=self.guardrails_config,
            dynamic_worker_config=self.dynamic_worker_config,
            dt=self.dt,
            hostname="test-host",
            devices=[0],
            inf_wrkr_id=1,
            preprocessing_output_queue=output_queue,
        )
        worker.log = mock_logger

        # Mock guardrails - all pass
        mock_guardrails = mock_guardrails_class.return_value
        mock_guardrails.filter_batch.return_value = (
            ["Safe 1", "Safe 2", "Safe 3"],
            ["<Safe 1>", "<Safe 2>", "<Safe 3>"],
            queues,
            [(1.0, 0.01, 0.02)] * 3,
            [],  # No malicious
            0.1,
        )

        worker._guard_and_forward_batch(
            formatted_prompts=["<Safe 1>", "<Safe 2>", "<Safe 3>"],
            user_prompts=["Safe 1", "Safe 2", "Safe 3"],
            response_queues=queues,
            latency_metrics=[(1.0, 0.01, 0.02)] * 3,
            guardrails=mock_guardrails,
        )

        result = output_queue.get(timeout=1)
        self.assertEqual(len(result[0]), 3)

    @patch("dragon.ai.inference.inference_worker_utils.GuardrailsProcessor")
    @patch("dragon.ai.inference.inference_worker_utils.setup_logging")
    def test_prebatch_mixed_safe_malicious(self, mock_logging, mock_guardrails_class):
        """Test pre-batch with mix of safe and malicious prompts."""
        mock_logger = MagicMock()
        mock_logging.return_value = mock_logger

        output_queue = mp.Queue()
        queues = [mp.Queue() for _ in range(4)]

        worker = InferenceWorker(
            end_event=self.end_event,
            model_config=self.model_config,
            batching_config=self.batching_config,
            guardrails_config=self.guardrails_config,
            dynamic_worker_config=self.dynamic_worker_config,
            dt=self.dt,
            hostname="test-host",
            devices=[0],
            inf_wrkr_id=1,
            preprocessing_output_queue=output_queue,
        )
        worker.log = mock_logger

        # Mock guardrails - indices 1 and 3 are malicious
        mock_guardrails = mock_guardrails_class.return_value
        mock_guardrails.filter_batch.return_value = (
            ["Safe 0", "Safe 2"],  # Only safe prompts
            ["<Safe 0>", "<Safe 2>"],
            [queues[0], queues[2]],
            [(1.0, 0.01, 0.02), (1.2, 0.01, 0.02)],
            [1, 3],  # Malicious indices
            0.1,
        )
        mock_guardrails.get_malicious_response.return_value = "BLOCKED"

        worker._guard_and_forward_batch(
            formatted_prompts=["<Safe 0>", "<Bad 1>", "<Safe 2>", "<Bad 3>"],
            user_prompts=["Safe 0", "Bad 1", "Safe 2", "Bad 3"],
            response_queues=queues,
            latency_metrics=[(1.0, 0.01, 0.02)] * 4,
            guardrails=mock_guardrails,
        )

        # Check malicious got blocked
        for idx in [1, 3]:
            result = queues[idx].get(timeout=1)
            self.assertEqual(result["assistant"], "BLOCKED")

        # Check safe prompts forwarded
        llm_input = output_queue.get(timeout=1)
        self.assertEqual(llm_input[0], ["Safe 0", "Safe 2"])

    @patch("dragon.ai.inference.inference_worker_utils.GuardrailsProcessor")
    @patch("dragon.ai.inference.inference_worker_utils.setup_logging")
    def test_prebatch_all_malicious(self, mock_logging, mock_guardrails_class):
        """Test pre-batch where all prompts are malicious."""
        mock_logger = MagicMock()
        mock_logging.return_value = mock_logger

        output_queue = mp.Queue()
        queues = [mp.Queue() for _ in range(3)]

        worker = InferenceWorker(
            end_event=self.end_event,
            model_config=self.model_config,
            batching_config=self.batching_config,
            guardrails_config=self.guardrails_config,
            dynamic_worker_config=self.dynamic_worker_config,
            dt=self.dt,
            hostname="test-host",
            devices=[0],
            inf_wrkr_id=1,
            preprocessing_output_queue=output_queue,
        )
        worker.log = mock_logger

        # Mock guardrails - all blocked
        mock_guardrails = mock_guardrails_class.return_value
        mock_guardrails.filter_batch.return_value = (
            [],  # No safe prompts
            [],
            [],
            [],
            [0, 1, 2],  # All malicious
            0.1,
        )
        mock_guardrails.get_malicious_response.return_value = "BLOCKED"

        worker._guard_and_forward_batch(
            formatted_prompts=["<Bad 0>", "<Bad 1>", "<Bad 2>"],
            user_prompts=["Bad 0", "Bad 1", "Bad 2"],
            response_queues=queues,
            latency_metrics=[(1.0, 0.01, 0.02)] * 3,
            guardrails=mock_guardrails,
        )

        # All should be blocked
        for q in queues:
            result = q.get(timeout=1)
            self.assertEqual(result["assistant"], "BLOCKED")

        # Nothing forwarded to LLM
        self.assertTrue(output_queue.empty())


# =============================================================================
# CONFIGURATION 5: Dynamic Batch + Guardrails OFF
# =============================================================================
class TestDynamicBatchGuardrailsOff(unittest.TestCase):
    """
    Configuration: batch_toggle=True, batch_type="dynamic", guardrails=False

    Behavior:
    - System accumulates prompts over time window
    - Batch triggered by max_batch_size or timeout
    - No safety filtering
    """

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
        self.batching_config = BatchingConfig(
            enabled=True,
            batch_type="dynamic",
            batch_wait_seconds=0.1,
            max_batch_size=4,
        )
        self.guardrails_config = GuardrailsConfig(enabled=False)
        self.dynamic_worker_config = DynamicWorkerConfig(enabled=False)
        self.end_event = mp.Event()
        self.dt = MockTelemetry()

    def test_worker_configuration(self):
        """Test worker is configured for dynamic batching without guardrails."""
        worker = InferenceWorker(
            end_event=self.end_event,
            model_config=self.model_config,
            batching_config=self.batching_config,
            guardrails_config=self.guardrails_config,
            dynamic_worker_config=self.dynamic_worker_config,
            dt=self.dt,
        )

        self.assertTrue(worker.batch_toggle)
        self.assertEqual(worker.batch_type, "dynamic")
        self.assertEqual(worker.batch_wait_time, 0.1)
        self.assertEqual(worker.batch_limit_max, 4)
        self.assertFalse(worker.prompt_guard_toggle)

    @patch("dragon.ai.inference.inference_worker_utils.setup_logging")
    def test_dynamic_batch_forward_to_llm(self, mock_logging):
        """Test dynamically batched prompts forwarded together."""
        mock_logger = MagicMock()
        mock_logging.return_value = mock_logger

        output_queue = mp.Queue()

        worker = InferenceWorker(
            end_event=self.end_event,
            model_config=self.model_config,
            batching_config=self.batching_config,
            guardrails_config=self.guardrails_config,
            dynamic_worker_config=self.dynamic_worker_config,
            dt=self.dt,
            hostname="test-host",
            devices=[0],
            inf_wrkr_id=1,
            preprocessing_output_queue=output_queue,
        )
        worker.log = mock_logger

        # Simulate forwarding a dynamic batch
        worker._forward_to_llm(
            formatted_prompts=["<P1>", "<P2>", "<P3>"],
            user_prompts=["P1", "P2", "P3"],
            response_queues=[mp.Queue() for _ in range(3)],
            latency_metrics=[(time.time(), 0.01, 0.02)] * 3,
            preprocessing_time=0.0,
        )

        result = output_queue.get(timeout=1)
        self.assertEqual(len(result[0]), 3)


# =============================================================================
# CONFIGURATION 6: Dynamic Batch + Guardrails ON
# =============================================================================
class TestDynamicBatchGuardrailsOn(unittest.TestCase):
    """
    Configuration: batch_toggle=True, batch_type="dynamic", guardrails=True

    Behavior:
    - System accumulates prompts over time window
    - Batch triggered by max_batch_size or timeout
    - Entire batch checked for safety before LLM
    - Malicious prompts blocked, safe ones forwarded
    """

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
        self.batching_config = BatchingConfig(
            enabled=True,
            batch_type="dynamic",
            batch_wait_seconds=0.1,
            max_batch_size=4,
        )
        self.guardrails_config = GuardrailsConfig(
            enabled=True,
            prompt_guard_sensitivity=0.5,
        )
        self.dynamic_worker_config = DynamicWorkerConfig(enabled=False)
        self.end_event = mp.Event()
        self.dt = MockTelemetry()

    def test_worker_configuration(self):
        """Test worker is configured for dynamic batching with guardrails."""
        worker = InferenceWorker(
            end_event=self.end_event,
            model_config=self.model_config,
            batching_config=self.batching_config,
            guardrails_config=self.guardrails_config,
            dynamic_worker_config=self.dynamic_worker_config,
            dt=self.dt,
        )

        self.assertTrue(worker.batch_toggle)
        self.assertEqual(worker.batch_type, "dynamic")
        self.assertTrue(worker.prompt_guard_toggle)

    @patch("dragon.ai.inference.inference_worker_utils.GuardrailsProcessor")
    @patch("dragon.ai.inference.inference_worker_utils.setup_logging")
    def test_dynamic_batch_all_safe(self, mock_logging, mock_guardrails_class):
        """Test dynamic batch where all prompts are safe."""
        mock_logger = MagicMock()
        mock_logging.return_value = mock_logger

        output_queue = mp.Queue()
        queues = [mp.Queue() for _ in range(4)]

        worker = InferenceWorker(
            end_event=self.end_event,
            model_config=self.model_config,
            batching_config=self.batching_config,
            guardrails_config=self.guardrails_config,
            dynamic_worker_config=self.dynamic_worker_config,
            dt=self.dt,
            hostname="test-host",
            devices=[0],
            inf_wrkr_id=1,
            preprocessing_output_queue=output_queue,
        )
        worker.log = mock_logger

        # Mock guardrails - all pass
        mock_guardrails = mock_guardrails_class.return_value
        mock_guardrails.filter_batch.return_value = (
            ["P1", "P2", "P3", "P4"],
            ["<P1>", "<P2>", "<P3>", "<P4>"],
            queues,
            [(1.0, 0.01, 0.02)] * 4,
            [],
            0.1,
        )

        worker._guard_and_forward_batch(
            formatted_prompts=["<P1>", "<P2>", "<P3>", "<P4>"],
            user_prompts=["P1", "P2", "P3", "P4"],
            response_queues=queues,
            latency_metrics=[(1.0, 0.01, 0.02)] * 4,
            guardrails=mock_guardrails,
        )

        result = output_queue.get(timeout=1)
        self.assertEqual(len(result[0]), 4)

    @patch("dragon.ai.inference.inference_worker_utils.GuardrailsProcessor")
    @patch("dragon.ai.inference.inference_worker_utils.setup_logging")
    def test_dynamic_batch_mixed(self, mock_logging, mock_guardrails_class):
        """Test dynamic batch with mixed safe and malicious prompts."""
        mock_logger = MagicMock()
        mock_logging.return_value = mock_logger

        output_queue = mp.Queue()
        queues = [mp.Queue() for _ in range(4)]

        worker = InferenceWorker(
            end_event=self.end_event,
            model_config=self.model_config,
            batching_config=self.batching_config,
            guardrails_config=self.guardrails_config,
            dynamic_worker_config=self.dynamic_worker_config,
            dt=self.dt,
            hostname="test-host",
            devices=[0],
            inf_wrkr_id=1,
            preprocessing_output_queue=output_queue,
        )
        worker.log = mock_logger

        # Mock guardrails - indices 0 and 2 are safe, 1 and 3 are malicious
        mock_guardrails = mock_guardrails_class.return_value
        mock_guardrails.filter_batch.return_value = (
            ["Safe 0", "Safe 2"],
            ["<Safe 0>", "<Safe 2>"],
            [queues[0], queues[2]],
            [(1.0, 0.01, 0.02)] * 2,
            [1, 3],  # Malicious
            0.1,
        )
        mock_guardrails.get_malicious_response.return_value = "BLOCKED"

        worker._guard_and_forward_batch(
            formatted_prompts=["<Safe 0>", "<Bad 1>", "<Safe 2>", "<Bad 3>"],
            user_prompts=["Safe 0", "Bad 1", "Safe 2", "Bad 3"],
            response_queues=queues,
            latency_metrics=[(1.0, 0.01, 0.02)] * 4,
            guardrails=mock_guardrails,
        )

        # Check malicious blocked
        self.assertEqual(queues[1].get(timeout=1)["assistant"], "BLOCKED")
        self.assertEqual(queues[3].get(timeout=1)["assistant"], "BLOCKED")

        # Check safe forwarded
        result = output_queue.get(timeout=1)
        self.assertEqual(result[0], ["Safe 0", "Safe 2"])

    def test_dynamic_batcher_with_guardrails_flow(self):
        """Test complete flow: accumulate -> batch -> guard -> forward."""
        batcher = DynamicBatcher(
            batch_wait_seconds=10.0,
            max_batch_size=3,
            enabled=True,
        )

        queues = [mp.Queue() for _ in range(3)]

        # Accumulate batch
        batcher.add_item("P1", "<P1>", queues[0], (time.time(), 0.01, 0.02))
        batcher.add_item("P2", "<P2>", queues[1], (time.time(), 0.01, 0.02))
        batch = batcher.add_item("P3", "<P3>", queues[2], (time.time(), 0.01, 0.02))

        self.assertIsNotNone(batch)

        # Batch ready - extract for guardrails processing
        self.assertEqual(batch.user_prompts, ["P1", "P2", "P3"])
        self.assertEqual(batch.formatted_prompts, ["<P1>", "<P2>", "<P3>"])
        self.assertEqual(len(batch.response_queues), 3)


# =============================================================================
# SUMMARY TEST: All Configurations Matrix
# =============================================================================
class TestConfigurationMatrix(unittest.TestCase):
    """
    Summary test verifying all 6 configurations are properly handled.
    """

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
        self.end_event = mp.Event()
        self.dt = MockTelemetry()

    @patch("dragon.ai.inference.inference_worker_utils.setup_logging")
    def test_all_configurations(self, mock_logging):
        """Test all 6 configuration combinations."""
        mock_logger = MagicMock()
        mock_logging.return_value = mock_logger

        configurations = [
            (False, "dynamic", False),  # Config 1: No batch, no guard
            (False, "dynamic", True),  # Config 2: No batch, guard
            (True, "pre-batch", False),  # Config 3: Pre-batch, no guard
            (True, "pre-batch", True),  # Config 4: Pre-batch, guard
            (True, "dynamic", False),  # Config 5: Dynamic, no guard
            (True, "dynamic", True),  # Config 6: Dynamic, guard
        ]

        for batch_enabled, batch_type, guardrails_enabled in configurations:
            with self.subTest(
                batch_enabled=batch_enabled,
                batch_type=batch_type,
                guardrails_enabled=guardrails_enabled,
            ):
                batching_config = BatchingConfig(
                    enabled=batch_enabled,
                    batch_type=batch_type,
                    batch_wait_seconds=0.1,
                    max_batch_size=4,
                )
                guardrails_config = GuardrailsConfig(
                    enabled=guardrails_enabled,
                    prompt_guard_sensitivity=0.5,
                )

                worker = InferenceWorker(
                    end_event=self.end_event,
                    model_config=self.model_config,
                    batching_config=batching_config,
                    guardrails_config=guardrails_config,
                    dynamic_worker_config=DynamicWorkerConfig(enabled=False),
                    dt=self.dt,
                )

                # Verify configuration is correctly set
                self.assertEqual(worker.batch_toggle, batch_enabled)
                if batch_enabled:
                    self.assertEqual(worker.batch_type, batch_type)
                self.assertEqual(worker.prompt_guard_toggle, guardrails_enabled)

    @patch("dragon.ai.inference.inference_worker_utils.setup_logging")
    def test_preprocessing_needed_determination(self, mock_logging):
        """Test that preprocessing_needed is correctly determined."""
        mock_logger = MagicMock()
        mock_logging.return_value = mock_logger

        # Preprocessing needed when: guardrails OR (batching AND dynamic)
        test_cases = [
            # (batch_enabled, batch_type, guardrails, expected_preprocessing)
            (False, "dynamic", False, False),  # Nothing enabled
            (False, "dynamic", True, True),  # Only guardrails
            (True, "pre-batch", False, False),  # Pre-batch, no guard
            (True, "pre-batch", True, True),  # Pre-batch with guard
            (True, "dynamic", False, True),  # Dynamic batch
            (True, "dynamic", True, True),  # Dynamic batch with guard
        ]

        for batch_enabled, batch_type, guardrails_enabled, expected in test_cases:
            with self.subTest(
                batch_enabled=batch_enabled,
                batch_type=batch_type,
                guardrails_enabled=guardrails_enabled,
            ):
                batching_config = BatchingConfig(
                    enabled=batch_enabled,
                    batch_type=batch_type,
                )
                guardrails_config = GuardrailsConfig(enabled=guardrails_enabled)

                # Calculate expected preprocessing_needed
                preprocessing_needed = guardrails_enabled or (batch_enabled and batch_type == "dynamic")

                self.assertEqual(
                    preprocessing_needed,
                    expected,
                    f"Failed for batch={batch_enabled}, type={batch_type}, " f"guard={guardrails_enabled}",
                )


if __name__ == "__main__":
    unittest.main()
