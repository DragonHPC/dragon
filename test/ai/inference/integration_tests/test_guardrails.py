"""
Integration tests for the guardrails module with inference worker integration.

These tests verify the interaction between GuardrailsProcessor and the
InferenceWorker preprocessing module.
"""

import dragon
import multiprocessing as mp
import time
import unittest
from unittest.mock import MagicMock, patch

from dragon.ai.inference.config import (
    ModelConfig,
    BatchingConfig,
    GuardrailsConfig,
    DynamicWorkerConfig,
)
from dragon.ai.inference.inference_worker_utils import InferenceWorker

from ..mocks import MockTelemetry


class TestGuardrailsWithInferenceWorker(unittest.TestCase):
    """Integration tests for guardrails filtering with InferenceWorker."""

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
        self.batching_config = BatchingConfig(enabled=True, batch_type="dynamic")
        self.guardrails_config = GuardrailsConfig(
            enabled=True,
            prompt_guard_model="meta-llama/Prompt-Guard-86M",
            prompt_guard_sensitivity=0.5,
        )
        self.dynamic_worker_config = DynamicWorkerConfig(enabled=False)
        self.end_event = mp.Event()
        self.dt = MockTelemetry()

    @patch("dragon.ai.inference.inference_worker_utils.GuardrailsProcessor")
    @patch("dragon.ai.inference.inference_worker_utils.setup_logging")
    def test_guardrails_filters_malicious_prompts(self, mock_logging, mock_guardrails_class):
        """Test that guardrails correctly filters malicious prompts."""
        mock_logger = MagicMock()
        mock_logging.return_value = mock_logger

        # Create queues for responses
        q_safe_1 = mp.Queue()
        q_safe_2 = mp.Queue()
        q_malicious = mp.Queue()

        worker = InferenceWorker(
            end_event=self.end_event,
            model_config=self.model_config,
            batching_config=self.batching_config,
            guardrails_config=self.guardrails_config,
            dynamic_worker_config=self.dynamic_worker_config,
            dt=self.dt,
            hostname="test-host",
            devices=[0],
            head_cpu_pid=1234,
            inf_wrkr_id=1,
        )
        worker.log = mock_logger

        # Configure mock guardrails to mark second prompt as malicious
        mock_guardrails = mock_guardrails_class.return_value
        mock_guardrails.filter_batch.return_value = (
            ["Safe 1", "Safe 2"],  # safe_user_prompts
            ["<user>Safe 1</user>", "<user>Safe 2</user>"],  # safe_formatted_prompts
            [q_safe_1, q_safe_2],  # safe_response_queues
            [(1.0, 0.01, 0.02), (1.2, 0.01, 0.02)],  # safe_latency_metrics
            [1],  # malicious_indices (middle prompt)
            0.05,  # preprocessing_time
        )
        mock_guardrails.get_malicious_response.return_value = "Your input has been categorized as malicious."

        # Input batch with 3 prompts (middle one is malicious)
        formatted_prompts = [
            "<user>Safe 1</user>",
            "<user>Malicious</user>",
            "<user>Safe 2</user>",
        ]
        user_prompts = ["Safe 1", "Malicious", "Safe 2"]
        response_queues = [q_safe_1, q_malicious, q_safe_2]
        latency_metrics = [
            (1.0, 0.01, 0.02),
            (1.1, 0.01, 0.02),
            (1.2, 0.01, 0.02),
        ]

        # Filter the batch
        (
            safe_formatted,
            safe_user,
            safe_queues,
            safe_metrics,
            preprocessing_time,
        ) = worker.filter_with_guardrails(
            formatted_prompts=formatted_prompts,
            user_prompts=user_prompts,
            response_queues=response_queues,
            latency_metrics=latency_metrics,
            guardrails=mock_guardrails,
        )

        # Verify safe prompts are returned
        self.assertEqual(safe_user, ["Safe 1", "Safe 2"])
        self.assertEqual(safe_formatted, ["<user>Safe 1</user>", "<user>Safe 2</user>"])
        self.assertEqual(len(safe_queues), 2)
        self.assertEqual(preprocessing_time, 0.05)

        # Verify malicious prompt received error response
        malicious_response = q_malicious.get(timeout=1)
        self.assertEqual(malicious_response["user"], "Malicious")
        self.assertIn("malicious", malicious_response["assistant"].lower())

    @patch("dragon.ai.inference.inference_worker_utils.setup_logging")
    def test_guardrails_disabled_passes_all(self, mock_logging):
        """Test that disabled guardrails passes all prompts through."""
        mock_logger = MagicMock()
        mock_logging.return_value = mock_logger

        disabled_guardrails = GuardrailsConfig(enabled=False)

        worker = InferenceWorker(
            end_event=self.end_event,
            model_config=self.model_config,
            batching_config=self.batching_config,
            guardrails_config=disabled_guardrails,
            dynamic_worker_config=self.dynamic_worker_config,
            dt=self.dt,
        )
        worker.log = mock_logger

        queues = [mp.Queue() for _ in range(3)]
        formatted_prompts = ["<P1>", "<P2>", "<P3>"]
        user_prompts = ["P1", "P2", "P3"]
        latency_metrics = [(1.0, 0.01, 0.02)] * 3

        (
            safe_formatted,
            safe_user,
            safe_queues,
            safe_metrics,
            preprocessing_time,
        ) = worker.filter_with_guardrails(
            formatted_prompts=formatted_prompts,
            user_prompts=user_prompts,
            response_queues=queues,
            latency_metrics=latency_metrics,
            guardrails=None,
        )

        # All prompts should pass through
        self.assertEqual(safe_user, user_prompts)
        self.assertEqual(safe_formatted, formatted_prompts)
        self.assertEqual(len(safe_queues), 3)
        self.assertEqual(preprocessing_time, 0)


class TestGuardrailsWithBatching(unittest.TestCase):
    """Integration tests combining guardrails with batching."""

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
            max_batch_size=5,
        )
        self.guardrails_config = GuardrailsConfig(
            enabled=True,
            prompt_guard_sensitivity=0.5,
        )
        self.dynamic_worker_config = DynamicWorkerConfig(enabled=False)
        self.end_event = mp.Event()
        self.dt = MockTelemetry()

    @patch("dragon.ai.inference.inference_worker_utils.GuardrailsProcessor")
    @patch("dragon.ai.inference.inference_worker_utils.setup_logging")
    def test_guard_and_forward_batch_integration(self, mock_logging, mock_guardrails_class):
        """Test _guard_and_forward_batch with both guardrails and batching."""
        mock_logger = MagicMock()
        mock_logging.return_value = mock_logger

        output_queue = mp.Queue()
        q_safe = mp.Queue()
        q_malicious = mp.Queue()

        worker = InferenceWorker(
            end_event=self.end_event,
            model_config=self.model_config,
            batching_config=self.batching_config,
            guardrails_config=self.guardrails_config,
            dynamic_worker_config=self.dynamic_worker_config,
            dt=self.dt,
            hostname="test-host",
            devices=[0],
            head_cpu_pid=1234,
            inf_wrkr_id=1,
            preprocessing_output_queue=output_queue,
        )
        worker.log = mock_logger

        # Configure mock guardrails
        mock_guardrails = mock_guardrails_class.return_value
        mock_guardrails.filter_batch.return_value = (
            ["Safe prompt"],  # safe_user_prompts
            ["<user>Safe prompt</user>"],  # safe_formatted_prompts
            [q_safe],  # safe_response_queues
            [(1.0, 0.01, 0.02)],  # safe_latency_metrics
            [1],  # malicious_indices
            0.03,  # preprocessing_time
        )
        mock_guardrails.get_malicious_response.return_value = "Blocked"

        # Call _guard_and_forward_batch
        worker._guard_and_forward_batch(
            formatted_prompts=["<user>Safe prompt</user>", "<user>Bad</user>"],
            user_prompts=["Safe prompt", "Bad"],
            response_queues=[q_safe, q_malicious],
            latency_metrics=[(1.0, 0.01, 0.02), (1.1, 0.01, 0.02)],
            guardrails=mock_guardrails,
        )

        # Verify malicious response was sent
        malicious_result = q_malicious.get(timeout=1)
        self.assertEqual(malicious_result["assistant"], "Blocked")

        # Verify safe prompts were forwarded to LLM
        llm_input = output_queue.get(timeout=1)
        self.assertEqual(llm_input[0], ["Safe prompt"])
        self.assertEqual(llm_input[1], ["<user>Safe prompt</user>"])


class TestMaliciousResponseHandling(unittest.TestCase):
    """Test malicious response handling and telemetry."""

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
        self.batching_config = BatchingConfig(enabled=True)
        self.guardrails_config = GuardrailsConfig(enabled=True)
        self.dynamic_worker_config = DynamicWorkerConfig(enabled=False)
        self.end_event = mp.Event()
        self.dt = MockTelemetry()

    def test_malicious_response_telemetry(self):
        """Test that malicious responses record correct telemetry."""
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

        response_queue = mp.Queue()
        latency_metric = (100.0, 0.05, 0.03)

        worker._send_malicious_response(
            user_prompt="Bad prompt",
            response_queue=response_queue,
            latency_metric=latency_metric,
            preprocessing_time=0.02,
            error_response="Blocked",
        )

        # Verify response
        result = response_queue.get(timeout=1)
        self.assertEqual(result["user"], "Bad prompt")
        self.assertEqual(result["assistant"], "Blocked")
        self.assertEqual(result["model_inference_latency"], 0)
        self.assertEqual(result["model_network_latency"], 0)

        # Verify telemetry
        self.assertIn(("guardrails_inference_latency", 0.02), self.dt.add_data_calls)
        self.assertIn(("model_inference_latency", 0), self.dt.add_data_calls)

    def test_malicious_response_structure(self):
        """Test malicious response has all required fields."""
        worker = InferenceWorker(
            end_event=self.end_event,
            model_config=self.model_config,
            batching_config=self.batching_config,
            guardrails_config=self.guardrails_config,
            dynamic_worker_config=self.dynamic_worker_config,
            dt=self.dt,
            hostname="node-0",
            devices=[0, 1],
            inf_wrkr_id=5,
        )

        response_queue = mp.Queue()

        worker._send_malicious_response(
            user_prompt="Test",
            response_queue=response_queue,
            latency_metric=(time.time(), 0.01, 0.02),
            preprocessing_time=0.05,
            error_response="Error message",
        )

        result = response_queue.get(timeout=1)

        # Check all required fields
        required_fields = [
            "hostname",
            "inf_worker_id",
            "devices",
            "batch_size",
            "cpu_head_network_latency",
            "guardrails_inference_latency",
            "guardrails_network_latency",
            "model_inference_latency",
            "model_network_latency",
            "end_to_end_latency",
            "requests_per_second",
            "total_tokens_per_second",
            "total_output_tokens_per_second",
            "user",
            "assistant",
        ]

        for field in required_fields:
            self.assertIn(field, result, f"Missing field: {field}")

        self.assertEqual(result["hostname"], "node-0")
        self.assertEqual(result["inf_worker_id"], 5)
        self.assertEqual(result["devices"], [0, 1])


if __name__ == "__main__":
    unittest.main()
