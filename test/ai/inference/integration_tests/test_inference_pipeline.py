"""
Integration tests for the inference worker pipeline.

These tests verify the interaction between InferenceWorker and other
components (queues, telemetry, response routing).
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
from dragon.ai.inference.batching import DynamicBatcher

from ..mocks import MockTelemetry


class TestInferenceWorkerResponseRouting(unittest.TestCase):
    """Integration tests for InferenceWorker routing responses to queues
    with telemetry recording.
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
            dtype="bfloat16",
            max_tokens=100,
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

    @patch("dragon.ai.inference.inference_worker_utils.setup_logging")
    def test_single_response_routed_to_queue(self, mock_logging):
        """Test that a single response is correctly routed to the caller's
        queue with all metadata and telemetry recorded.
        """
        mock_logger = MagicMock()
        mock_logging.return_value = mock_logger

        response_queue = mp.Queue()

        worker = InferenceWorker(
            end_event=self.end_event,
            model_config=self.model_config,
            batching_config=BatchingConfig(enabled=False),
            guardrails_config=self.guardrails_config,
            dynamic_worker_config=self.dynamic_worker_config,
            dt=self.dt,
            hostname="node-0",
            devices=[0],
            inf_wrkr_id=1,
        )
        worker.log = mock_logger

        worker._send_responses(
            user_prompts=["Hello, world!"],
            responses=["Hello! How can I help you today?"],
            qs=[response_queue],
            preprocessing_time=0.0,
            tuple_latency_timestamps=[(time.time() - 1.0, 0.01, 0.02)],
            model_network_latency=0.05,
            metrics={
                "inference_time": 0.5,
                "requests_per_second": 2.0,
                "total_tokens_per_second": 100.0,
                "output_tokens_per_second": 50.0,
            },
        )

        # Verify response reached queue
        result = response_queue.get(timeout=2)
        self.assertEqual(result["user"], "Hello, world!")
        self.assertEqual(result["assistant"], "Hello! How can I help you today?")
        self.assertEqual(result["batch_size"], 1)
        self.assertEqual(result["model_inference_latency"], 0.5)

        # Verify telemetry recorded
        keys_recorded = [call[0] for call in self.dt.add_data_calls]
        self.assertIn("model_inference_latency", keys_recorded)

    @patch("dragon.ai.inference.inference_worker_utils.setup_logging")
    def test_batch_responses_routed_to_individual_queues(self, mock_logging):
        """Test that batch responses are correctly routed to each caller's
        individual queue.
        """
        mock_logger = MagicMock()
        mock_logging.return_value = mock_logger

        # Create separate queues for each caller
        alice_queue = mp.Queue()
        bob_queue = mp.Queue()
        carol_queue = mp.Queue()

        worker = InferenceWorker(
            end_event=self.end_event,
            model_config=self.model_config,
            batching_config=self.batching_config,
            guardrails_config=self.guardrails_config,
            dynamic_worker_config=self.dynamic_worker_config,
            dt=self.dt,
            hostname="node-0",
            devices=[0, 1],
            inf_wrkr_id=2,
        )
        worker.log = mock_logger

        base_time = time.time() - 1.0
        worker._send_responses(
            user_prompts=["Alice's question", "Bob's question", "Carol's question"],
            responses=["Alice's answer", "Bob's answer", "Carol's answer"],
            qs=[alice_queue, bob_queue, carol_queue],
            preprocessing_time=0.05,
            tuple_latency_timestamps=[
                (base_time, 0.01, 0.02),
                (base_time + 0.01, 0.01, 0.02),
                (base_time + 0.02, 0.01, 0.02),
            ],
            model_network_latency=0.03,
            metrics={
                "inference_time": 1.0,
                "requests_per_second": 3.0,
                "total_tokens_per_second": 150.0,
                "output_tokens_per_second": 75.0,
            },
        )

        # Verify each caller got their response
        alice_result = alice_queue.get(timeout=2)
        bob_result = bob_queue.get(timeout=2)
        carol_result = carol_queue.get(timeout=2)

        self.assertEqual(alice_result["user"], "Alice's question")
        self.assertEqual(alice_result["assistant"], "Alice's answer")

        self.assertEqual(bob_result["user"], "Bob's question")
        self.assertEqual(bob_result["assistant"], "Bob's answer")

        self.assertEqual(carol_result["user"], "Carol's question")
        self.assertEqual(carol_result["assistant"], "Carol's answer")

        # All should have same batch metadata
        for result in [alice_result, bob_result, carol_result]:
            self.assertEqual(result["batch_size"], 3)
            self.assertEqual(result["devices"], [0, 1])


class TestInferenceWorkerTelemetryIntegration(unittest.TestCase):
    """Integration tests for InferenceWorker telemetry recording during
    response processing.
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
            tp_size=2,
        )
        self.batching_config = BatchingConfig(enabled=True)
        self.guardrails_config = GuardrailsConfig(enabled=True)
        self.dynamic_worker_config = DynamicWorkerConfig(enabled=False)
        self.end_event = mp.Event()
        self.dt = MockTelemetry()

    @patch("dragon.ai.inference.inference_worker_utils.setup_logging")
    def test_all_latency_metrics_recorded(self, mock_logging):
        """Test that all latency metrics are recorded to telemetry during response processing."""
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
            hostname="gpu-node-01",
            devices=[0, 1],
            inf_wrkr_id=5,
        )
        worker.log = mock_logger

        worker._send_responses(
            user_prompts=["Test prompt"],
            responses=["Test response"],
            qs=[response_queue],
            preprocessing_time=0.1,
            tuple_latency_timestamps=[(time.time() - 2.0, 0.05, 0.03)],
            model_network_latency=0.02,
            metrics={
                "inference_time": 1.5,
                "requests_per_second": 4.0,
                "total_tokens_per_second": 200.0,
                "output_tokens_per_second": 100.0,
            },
        )

        # Verify all telemetry keys recorded
        keys_recorded = [call[0] for call in self.dt.add_data_calls]
        self.assertIn("cpu_head_network_latency", keys_recorded)
        self.assertIn("guardrails_inference_latency", keys_recorded)
        self.assertIn("guardrails_network_latency", keys_recorded)
        self.assertIn("model_inference_latency", keys_recorded)
        self.assertIn("model_network_latency", keys_recorded)
        self.assertIn("end_to_end_latency", keys_recorded)
        self.assertIn("requests_per_second", keys_recorded)
        self.assertIn("total_tokens_per_second", keys_recorded)

    @patch("dragon.ai.inference.inference_worker_utils.setup_logging")
    def test_response_includes_all_required_fields(self, mock_logging):
        """Test that response dict includes all fields needed by downstream consumers (web app, metrics collectors)."""
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
            hostname="gpu-node-01",
            devices=[0, 1],
            inf_wrkr_id=5,
        )
        worker.log = mock_logger

        worker._send_responses(
            user_prompts=["Test prompt"],
            responses=["Test response"],
            qs=[response_queue],
            preprocessing_time=0.1,
            tuple_latency_timestamps=[(time.time() - 2.0, 0.05, 0.03)],
            model_network_latency=0.02,
            metrics={
                "inference_time": 1.5,
                "requests_per_second": 4.0,
                "total_tokens_per_second": 200.0,
                "output_tokens_per_second": 100.0,
            },
        )

        result = response_queue.get(timeout=2)

        # Verify all required fields present
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
            self.assertIn(field, result, f"Missing required field: {field}")

        # Verify field values
        self.assertEqual(result["hostname"], "gpu-node-01")
        self.assertEqual(result["inf_worker_id"], 5)
        self.assertEqual(result["devices"], [0, 1])
        self.assertEqual(result["user"], "Test prompt")
        self.assertEqual(result["assistant"], "Test response")


class TestInferenceWorkerWithBatcherPipeline(unittest.TestCase):
    """Integration tests for InferenceWorker with DynamicBatcher in the preprocessing pipeline."""

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
        self.guardrails_config = GuardrailsConfig(enabled=False)
        self.dynamic_worker_config = DynamicWorkerConfig(enabled=False)
        self.end_event = mp.Event()
        self.dt = MockTelemetry()

    @patch("dragon.ai.inference.inference_worker_utils.setup_logging")
    def test_batcher_batch_forwarded_through_worker(self, mock_logging):
        """Test that batches formed by DynamicBatcher are correctly forwarded through InferenceWorker to the LLM queue."""
        mock_logger = MagicMock()
        mock_logging.return_value = mock_logger

        batching_config = BatchingConfig(
            enabled=True,
            batch_type="dynamic",
            batch_wait_seconds=10.0,
            max_batch_size=3,
        )

        llm_input_queue = mp.Queue()

        worker = InferenceWorker(
            end_event=self.end_event,
            model_config=self.model_config,
            batching_config=batching_config,
            guardrails_config=self.guardrails_config,
            dynamic_worker_config=self.dynamic_worker_config,
            dt=self.dt,
            hostname="test-host",
            devices=[0],
            inf_wrkr_id=1,
            preprocessing_output_queue=llm_input_queue,
        )
        worker.log = mock_logger

        # Create batcher with same config
        batcher = DynamicBatcher(
            batch_wait_seconds=10.0,
            max_batch_size=3,
            enabled=True,
        )

        # Collect prompts into batch
        response_queues = [mp.Queue() for _ in range(3)]
        batch = None
        for i in range(3):
            batch = batcher.add_item(
                user_prompt=f"Prompt{i}",
                formatted_prompt=f"<Prompt{i}>",
                response_queue=response_queues[i],
                latency_metrics=(time.time(), 0.01, 0.02),
            )

        # Batch should be formed
        self.assertIsNotNone(batch)
        self.assertEqual(batch.size, 3)

        # Forward through worker
        worker._forward_to_llm(
            formatted_prompts=batch.formatted_prompts,
            user_prompts=batch.user_prompts,
            response_queues=batch.response_queues,
            latency_metrics=batch.latency_metrics,
            preprocessing_time=0.05,
        )

        # Verify data reached LLM queue
        llm_data = llm_input_queue.get(timeout=1)
        self.assertEqual(llm_data[0], ["Prompt0", "Prompt1", "Prompt2"])
        self.assertEqual(llm_data[1], ["<Prompt0>", "<Prompt1>", "<Prompt2>"])
        self.assertEqual(len(llm_data[2]), 3)  # response_queues


class TestInferenceWorkerErrorHandling(unittest.TestCase):
    """Integration tests for error handling in the inference pipeline."""

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

    @patch("dragon.ai.inference.inference_worker_utils.setup_logging")
    def test_error_response_routed_to_caller(self, mock_logging):
        """Test that error responses are correctly routed to the caller's
        queue so they know inference failed.
        """
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

        # Simulate error response
        worker._send_responses(
            user_prompts=["Failed prompt"],
            responses=["LLM inference failed. Please try again later."],
            qs=[response_queue],
            preprocessing_time=0.0,
            tuple_latency_timestamps=[(time.time(), 0.01, 0.02)],
            model_network_latency=0.0,
            metrics={
                "inference_time": 0.0,
                "requests_per_second": 0.0,
                "total_tokens_per_second": 0.0,
                "output_tokens_per_second": 0.0,
            },
        )

        # Verify error response reached caller
        result = response_queue.get(timeout=2)
        self.assertIn("failed", result["assistant"].lower())
        self.assertEqual(result["model_inference_latency"], 0.0)
        self.assertEqual(result["user"], "Failed prompt")


if __name__ == "__main__":
    unittest.main()
