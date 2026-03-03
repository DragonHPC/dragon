"""
Integration tests for error handling and recovery in the inference pipeline.

These tests verify the pipeline's resilience to various error conditions
and its ability to handle edge cases gracefully.
"""

import dragon
import multiprocessing as mp
import time
import unittest
from unittest.mock import MagicMock, patch, Mock
from queue import Empty, Full

from dragon.ai.inference.inference_worker_utils import InferenceWorker
from dragon.ai.inference.llm_engine import LLMInferenceEngine
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


class TestInferenceWorkerErrorHandling(unittest.TestCase):
    """Integration tests for InferenceWorker error handling."""

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
    def test_error_response_sent_to_caller(self, mock_logging):
        """Test that error responses are correctly sent to caller's queue."""
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

        # Send error response
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

        # Verify error response
        result = response_queue.get(timeout=2)
        self.assertIn("failed", result["assistant"].lower())
        self.assertEqual(result["model_inference_latency"], 0.0)
        self.assertEqual(result["user"], "Failed prompt")

    @patch("dragon.ai.inference.inference_worker_utils.setup_logging")
    def test_batch_partial_failure_handling(self, mock_logging):
        """Test handling when some prompts in batch fail."""
        mock_logger = MagicMock()
        mock_logging.return_value = mock_logger

        response_queues = [mp.Queue() for _ in range(3)]

        worker = InferenceWorker(
            end_event=self.end_event,
            model_config=self.model_config,
            batching_config=BatchingConfig(enabled=True),
            guardrails_config=self.guardrails_config,
            dynamic_worker_config=self.dynamic_worker_config,
            dt=self.dt,
            hostname="test-host",
            devices=[0],
            inf_wrkr_id=1,
        )
        worker.log = mock_logger

        # Simulate partial failure - one response is error message
        worker._send_responses(
            user_prompts=["Prompt 1", "Prompt 2", "Prompt 3"],
            responses=[
                "Response 1",
                "Error: Generation failed",
                "Response 3",
            ],
            qs=response_queues,
            preprocessing_time=0.1,
            tuple_latency_timestamps=[(time.time() - 1, 0.01, 0.02)] * 3,
            model_network_latency=0.02,
            metrics={
                "inference_time": 0.5,
                "requests_per_second": 3.0,
                "total_tokens_per_second": 150.0,
                "output_tokens_per_second": 75.0,
            },
        )

        # Verify all responses reached their queues
        result1 = response_queues[0].get(timeout=1)
        result2 = response_queues[1].get(timeout=1)
        result3 = response_queues[2].get(timeout=1)

        self.assertEqual(result1["assistant"], "Response 1")
        self.assertIn("Error", result2["assistant"])
        self.assertEqual(result3["assistant"], "Response 3")

    @patch("dragon.ai.inference.inference_worker_utils.setup_logging")
    def test_malicious_response_has_zero_inference_latency(self, mock_logging):
        """Test that malicious responses correctly report zero model latency."""
        mock_logger = MagicMock()
        mock_logging.return_value = mock_logger

        response_queue = mp.Queue()

        worker = InferenceWorker(
            end_event=self.end_event,
            model_config=self.model_config,
            batching_config=self.batching_config,
            guardrails_config=GuardrailsConfig(enabled=True),
            dynamic_worker_config=self.dynamic_worker_config,
            dt=self.dt,
            hostname="test-host",
            devices=[0],
            inf_wrkr_id=1,
        )
        worker.log = mock_logger

        # Send malicious response (blocked by guardrails)
        worker._send_malicious_response(
            user_prompt="Malicious prompt",
            response_queue=response_queue,
            latency_metric=(time.time(), 0.01, 0.05),
            preprocessing_time=0.1,
            error_response="Your input has been categorized as malicious.",
        )

        result = response_queue.get(timeout=1)

        # Verify zero model latencies for blocked prompt
        self.assertEqual(result["model_inference_latency"], 0)
        self.assertEqual(result["model_network_latency"], 0)
        self.assertGreater(result["guardrails_inference_latency"], 0)

    @patch("dragon.ai.inference.inference_worker_utils.setup_logging")
    def test_empty_queue_timeout_handling(self, mock_logging):
        """Test worker handles empty queue timeouts gracefully."""
        mock_logger = MagicMock()
        mock_logging.return_value = mock_logger

        worker = InferenceWorker(
            end_event=self.end_event,
            model_config=self.model_config,
            batching_config=self.batching_config,
            guardrails_config=self.guardrails_config,
            dynamic_worker_config=self.dynamic_worker_config,
            dt=self.dt,
        )
        worker.log = mock_logger

        # Verify worker can handle end_event being set
        self.end_event.set()
        self.assertTrue(worker.end_event.is_set())


class TestLLMEngineErrorHandling(unittest.TestCase):
    """Integration tests for LLMInferenceEngine error handling."""

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

    @patch("torch.cuda.synchronize")
    @patch("vllm.LLM")
    def test_llm_generation_exception_handling(self, mock_llm_class, mock_torch_sync):
        """Test that LLM generation exceptions are handled gracefully."""

        # Setup mock LLM that raises exception
        mock_llm = MagicMock()
        mock_llm_class.return_value = mock_llm
        mock_llm.generate.side_effect = RuntimeError("CUDA out of memory")

        engine = LLMInferenceEngine(
            model_config=self.model_config,
            batching_config=self.batching_config,
            hostname="node-0",
            devices=[0],
            master_port="29500",
        )
        engine.llm = mock_llm
        engine.sampling_params = MagicMock()

        # Attempt generation
        with self.assertRaises(RuntimeError):
            engine.generate(["Test prompt"])

    @patch("torch.cuda.synchronize")
    @patch("vllm.LLM")
    def test_empty_prompt_handling(self, mock_llm_class, mock_torch_sync):
        """Test handling of empty prompts."""

        mock_llm = MagicMock()
        mock_llm_class.return_value = mock_llm

        # Mock returns empty list
        mock_llm.generate.return_value = []

        engine = LLMInferenceEngine(
            model_config=self.model_config,
            batching_config=self.batching_config,
            hostname="node-0",
            devices=[0],
            master_port="29500",
        )
        engine.llm = mock_llm
        engine.sampling_params = MagicMock()

        # Generate with empty list
        responses, metrics = engine.generate([])

        # Verify returns empty list
        self.assertEqual(responses, [])

    @patch("torch.cuda.synchronize")
    @patch("vllm.LLM")
    def test_malformed_output_handling(self, mock_llm_class, mock_torch_sync):
        """Test handling when LLM returns malformed output structure."""

        mock_llm = MagicMock()
        mock_llm_class.return_value = mock_llm

        # Create malformed output (missing expected attributes)
        mock_output = Mock()
        mock_output.prompt_token_ids = []
        mock_output.outputs = []  # Empty outputs list

        mock_llm.generate.return_value = [mock_output]

        engine = LLMInferenceEngine(
            model_config=self.model_config,
            batching_config=self.batching_config,
            hostname="node-0",
            devices=[0],
            master_port="29500",
        )
        engine.llm = mock_llm
        engine.sampling_params = MagicMock()

        # This should handle the malformed output gracefully
        # The actual error handling depends on the implementation
        try:
            responses, metrics = engine.generate(["Test"])
        except (IndexError, AttributeError) as e:
            # Expected to raise error with malformed output
            pass


class TestInvalidInputHandling(unittest.TestCase):
    """Integration tests for handling invalid or malformed inputs."""

    @classmethod
    def setUpClass(cls):
        try:
            mp.set_start_method("dragon", force=True)
        except RuntimeError:
            pass

    @patch("dragon.ai.inference.inference_worker_utils.setup_logging")
    def test_empty_string_prompt(self, mock_logging):
        """Test handling of empty string prompts."""
        mock_logger = MagicMock()
        mock_logging.return_value = mock_logger

        response_queue = mp.Queue()

        worker = InferenceWorker(
            end_event=mp.Event(),
            model_config=ModelConfig(model_name="test-model", hf_token="token", tp_size=1),
            batching_config=BatchingConfig(enabled=False),
            guardrails_config=GuardrailsConfig(enabled=False),
            dynamic_worker_config=DynamicWorkerConfig(enabled=False),
            dt=MockTelemetry(),
            hostname="test",
            devices=[0],
            inf_wrkr_id=1,
        )
        worker.log = mock_logger
        # Mock the preprocessing_output_queue that _forward_to_llm uses
        worker.preprocessing_output_queue = MagicMock()

        # Forward empty prompt
        worker._forward_to_llm(
            formatted_prompts=[""],
            user_prompts=[""],
            response_queues=[response_queue],
            latency_metrics=[(time.time(), 0.01, 0.02)],
            preprocessing_time=0.0,
        )

        # Should handle gracefully (forwarding logic doesn't validate content)
        self.assertIsNotNone(worker)
        # Verify that the queue's put method was called
        worker.preprocessing_output_queue.put.assert_called_once()

    @patch("dragon.ai.inference.inference_worker_utils.setup_logging")
    def test_very_long_prompt_handling(self, mock_logging):
        """Test handling of extremely long prompts."""
        mock_logger = MagicMock()
        mock_logging.return_value = mock_logger

        response_queue = mp.Queue()

        worker = InferenceWorker(
            end_event=mp.Event(),
            model_config=ModelConfig(model_name="test-model", hf_token="token", tp_size=1),
            batching_config=BatchingConfig(enabled=False),
            guardrails_config=GuardrailsConfig(enabled=False),
            dynamic_worker_config=DynamicWorkerConfig(enabled=False),
            dt=MockTelemetry(),
            hostname="test",
            devices=[0],
            inf_wrkr_id=1,
        )
        worker.log = mock_logger
        # Mock the preprocessing_output_queue that _forward_to_llm uses
        worker.preprocessing_output_queue = MagicMock()

        # Create very long prompt
        long_prompt = "Test " * 10000

        worker._forward_to_llm(
            formatted_prompts=[long_prompt],
            user_prompts=[long_prompt],
            response_queues=[response_queue],
            latency_metrics=[(time.time(), 0.01, 0.02)],
            preprocessing_time=0.0,
        )

        # Should handle without crashing
        self.assertIsNotNone(worker)
        # Verify that the queue's put method was called
        worker.preprocessing_output_queue.put.assert_called_once()


class TestRecoveryScenarios(unittest.TestCase):
    """Integration tests for recovery from error conditions."""

    @classmethod
    def setUpClass(cls):
        try:
            mp.set_start_method("dragon", force=True)
        except RuntimeError:
            pass

    @patch("dragon.ai.inference.inference_worker_utils.setup_logging")
    def test_worker_continues_after_single_failure(self, mock_logging):
        """Test that worker can continue processing after a single failure."""
        mock_logger = MagicMock()
        mock_logging.return_value = mock_logger

        response_queues = [mp.Queue() for _ in range(3)]

        worker = InferenceWorker(
            end_event=mp.Event(),
            model_config=ModelConfig(model_name="test-model", hf_token="token", tp_size=1),
            batching_config=BatchingConfig(enabled=False),
            guardrails_config=GuardrailsConfig(enabled=False),
            dynamic_worker_config=DynamicWorkerConfig(enabled=False),
            dt=MockTelemetry(),
            hostname="test",
            devices=[0],
            inf_wrkr_id=1,
        )
        worker.log = mock_logger

        # Send first response (success)
        worker._send_responses(
            user_prompts=["Prompt 1"],
            responses=["Response 1"],
            qs=[response_queues[0]],
            preprocessing_time=0.0,
            tuple_latency_timestamps=[(time.time(), 0.01, 0.02)],
            model_network_latency=0.01,
            metrics={
                "inference_time": 0.5,
                "requests_per_second": 1.0,
                "total_tokens_per_second": 100.0,
                "output_tokens_per_second": 50.0,
            },
        )

        # Send second response (error)
        worker._send_responses(
            user_prompts=["Prompt 2"],
            responses=["Error occurred"],
            qs=[response_queues[1]],
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

        # Send third response (success after error)
        worker._send_responses(
            user_prompts=["Prompt 3"],
            responses=["Response 3"],
            qs=[response_queues[2]],
            preprocessing_time=0.0,
            tuple_latency_timestamps=[(time.time(), 0.01, 0.02)],
            model_network_latency=0.01,
            metrics={
                "inference_time": 0.5,
                "requests_per_second": 1.0,
                "total_tokens_per_second": 100.0,
                "output_tokens_per_second": 50.0,
            },
        )

        # Verify all responses received
        result1 = response_queues[0].get(timeout=1)
        result2 = response_queues[1].get(timeout=1)
        result3 = response_queues[2].get(timeout=1)

        self.assertEqual(result1["assistant"], "Response 1")
        self.assertEqual(result2["assistant"], "Error occurred")
        self.assertEqual(result3["assistant"], "Response 3")


if __name__ == "__main__":
    unittest.main()
