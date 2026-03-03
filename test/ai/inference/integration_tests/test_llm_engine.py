"""
Integration tests for the LLM inference engine module.

These tests verify the interaction between LLMInferenceEngine and other
components (queues, InferenceWorker) in the inference pipeline.
"""

import dragon
import multiprocessing as mp
import unittest
from unittest.mock import MagicMock, patch, Mock
import time

from dragon.ai.inference.llm_engine import LLMInferenceEngine
from dragon.ai.inference.config import (
    ModelConfig,
    BatchingConfig,
    GuardrailsConfig,
    DynamicWorkerConfig,
)
from dragon.ai.inference.inference_worker_utils import InferenceWorker

from ..mocks import MockTelemetry


class TestLLMEngineWithQueuePipeline(unittest.TestCase):
    """Integration tests for LLMInferenceEngine receiving batches from queues
    and sending responses back through response queues.
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
        self.batching_config = BatchingConfig(enabled=True, max_batch_size=4)

    @patch("vllm.LLM")
    def test_engine_processes_batch_from_queue(self, mock_llm_class):
        """Test that LLMInferenceEngine can process a batch received from a queue
        and return responses with metrics.
        """

        # Setup mock LLM
        mock_llm = MagicMock()
        mock_llm_class.return_value = mock_llm

        # Create mock outputs for batch of 3
        outputs = []
        for i in range(3):
            mock_output = Mock()
            mock_output.prompt_token_ids = list(range(10))
            mock_inner = Mock()
            mock_inner.text = f"Response to prompt {i+1}"
            mock_inner.token_ids = list(range(15))
            mock_output.outputs = [mock_inner]
            outputs.append(mock_output)
        mock_llm.generate.return_value = outputs

        engine = LLMInferenceEngine(
            model_config=self.model_config,
            batching_config=self.batching_config,
            hostname="gpu-node",
            devices=[0],
            master_port="29500",
        )
        engine.llm = mock_llm
        engine.sampling_params = MagicMock()

        # Simulate batch arriving from preprocessing queue
        llm_input_queue = mp.Queue()
        response_queues = [mp.Queue() for _ in range(3)]

        batch_data = (
            ["User prompt 1", "User prompt 2", "User prompt 3"],  # user_prompts
            ["<P1>", "<P2>", "<P3>"],  # formatted_prompts
            response_queues,  # response_queues
            [(time.time(), 0.01, 0.02)] * 3,  # latency_metrics
            0.05,  # preprocessing_time
        )
        llm_input_queue.put(batch_data)

        # Engine retrieves batch from queue
        received_batch = llm_input_queue.get(timeout=1)
        user_prompts, formatted_prompts, resp_queues, latency_metrics, preproc_time = received_batch

        # Engine generates responses
        responses, metrics = engine.generate(formatted_prompts)

        # Verify responses generated
        self.assertEqual(len(responses), 3)
        self.assertEqual(responses[0], "Response to prompt 1")

        # Simulate sending responses back through response queues
        for i, (response, resp_queue) in enumerate(zip(responses, resp_queues)):
            result = {
                "user": user_prompts[i],
                "assistant": response,
                "model_inference_latency": metrics["inference_time"],
                "total_tokens": metrics["total_tokens"],
            }
            resp_queue.put(result)

        # Verify callers receive their responses
        for i, resp_queue in enumerate(response_queues):
            result = resp_queue.get(timeout=1)
            self.assertEqual(result["user"], f"User prompt {i+1}")
            self.assertIn("Response to prompt", result["assistant"])

    @patch("vllm.LLM")
    def test_engine_metrics_flow_to_telemetry(self, mock_llm_class):
        """Test that metrics from LLMInferenceEngine flow correctly to telemetry system."""

        mock_llm = MagicMock()
        mock_llm_class.return_value = mock_llm

        mock_output = Mock()
        mock_output.prompt_token_ids = list(range(20))
        mock_inner = Mock()
        mock_inner.text = "Generated response"
        mock_inner.token_ids = list(range(30))
        mock_output.outputs = [mock_inner]
        mock_llm.generate.return_value = [mock_output]

        engine = LLMInferenceEngine(
            model_config=self.model_config,
            batching_config=self.batching_config,
            hostname="gpu-node",
            devices=[0],
            master_port="29500",
        )
        engine.llm = mock_llm
        engine.sampling_params = MagicMock()

        # Generate and get metrics
        responses, metrics = engine.generate(["Test prompt"])

        # Simulate telemetry recording (as would happen in actual pipeline)
        telemetry = MockTelemetry()
        telemetry.add_data("model_inference_latency", metrics["inference_time"])
        telemetry.add_data("total_tokens_per_second", metrics["total_tokens_per_second"])
        telemetry.add_data("requests_per_second", metrics["requests_per_second"])

        # Verify telemetry received correct metrics
        self.assertIn("model_inference_latency", telemetry.data)
        self.assertIn("total_tokens_per_second", telemetry.data)
        self.assertEqual(
            telemetry.data["total_tokens_per_second"],
            metrics["total_tokens_per_second"],
        )


class TestLLMEngineInferenceWorkerIntegration(unittest.TestCase):
    """Integration tests for LLMInferenceEngine working with InferenceWorker
    in the full inference pipeline.
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
            max_batch_size=4,
        )
        self.guardrails_config = GuardrailsConfig(enabled=False)
        self.dynamic_worker_config = DynamicWorkerConfig(enabled=False)
        self.end_event = mp.Event()
        self.dt = MockTelemetry()

    @patch("dragon.ai.inference.inference_worker_utils.setup_logging")
    @patch("vllm.LLM")
    def test_worker_forwards_to_engine_queue(self, mock_llm_class, mock_logging):
        """Test full pipeline: InferenceWorker forwards batch to queue,
        LLMInferenceEngine processes and returns responses.
        """
        mock_logger = MagicMock()
        mock_logging.return_value = mock_logger

        # Setup mock LLM
        mock_llm = MagicMock()
        mock_llm_class.return_value = mock_llm

        outputs = []
        for i in range(2):
            mock_output = Mock()
            mock_output.prompt_token_ids = list(range(8))
            mock_inner = Mock()
            mock_inner.text = f"LLM Response {i+1}"
            mock_inner.token_ids = list(range(12))
            mock_output.outputs = [mock_inner]
            outputs.append(mock_output)
        mock_llm.generate.return_value = outputs

        # Create the shared queue between InferenceWorker and LLMEngine
        llm_input_queue = mp.Queue()

        # Create InferenceWorker
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
            preprocessing_output_queue=llm_input_queue,
        )
        worker.log = mock_logger

        # Create LLMInferenceEngine
        engine = LLMInferenceEngine(
            model_config=self.model_config,
            batching_config=self.batching_config,
            hostname="test-host",
            devices=[0],
            master_port="29500",
        )
        engine.llm = mock_llm
        engine.sampling_params = MagicMock()

        # Response queues for callers
        caller1_queue = mp.Queue()
        caller2_queue = mp.Queue()

        # InferenceWorker forwards batch to LLM queue
        worker._forward_to_llm(
            formatted_prompts=["<Prompt1>", "<Prompt2>"],
            user_prompts=["Prompt1", "Prompt2"],
            response_queues=[caller1_queue, caller2_queue],
            latency_metrics=[(time.time(), 0.01, 0.02), (time.time(), 0.01, 0.02)],
            preprocessing_time=0.05,
        )

        # LLMEngine receives from queue
        batch_data = llm_input_queue.get(timeout=1)
        (
            user_prompts,
            formatted_prompts,
            response_queues,
            latency_metrics,
            preproc_time,
            timestamp,
        ) = batch_data

        # LLMEngine generates responses
        responses, metrics = engine.generate(formatted_prompts)

        self.assertEqual(len(responses), 2)
        self.assertEqual(responses[0], "LLM Response 1")
        self.assertEqual(responses[1], "LLM Response 2")

        # Send responses back to callers
        for user_prompt, response, resp_queue in zip(user_prompts, responses, response_queues):
            resp_queue.put({"user": user_prompt, "assistant": response})

        # Verify callers receive correct responses
        result1 = caller1_queue.get(timeout=1)
        result2 = caller2_queue.get(timeout=1)

        self.assertEqual(result1["user"], "Prompt1")
        self.assertEqual(result1["assistant"], "LLM Response 1")
        self.assertEqual(result2["user"], "Prompt2")
        self.assertEqual(result2["assistant"], "LLM Response 2")

    @patch("dragon.ai.inference.inference_worker_utils.setup_logging")
    @patch("vllm.LLM")
    def test_multiple_batches_processed_sequentially(self, mock_llm_class, mock_logging):
        """
        Test that multiple batches are processed sequentially through the pipeline.
        """
        mock_logger = MagicMock()
        mock_logging.return_value = mock_logger

        mock_llm = MagicMock()
        mock_llm_class.return_value = mock_llm

        # LLM will be called twice with different batches
        call_count = [0]

        def mock_generate(prompts, sampling_params=None, use_tqdm=False):
            call_count[0] += 1
            outputs = []
            for i, _ in enumerate(prompts):
                mock_output = Mock()
                mock_output.prompt_token_ids = list(range(5))
                mock_inner = Mock()
                mock_inner.text = f"Batch{call_count[0]}_Response{i+1}"
                mock_inner.token_ids = list(range(10))
                mock_output.outputs = [mock_inner]
                outputs.append(mock_output)
            return outputs

        mock_llm.generate = mock_generate

        llm_input_queue = mp.Queue()

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
            preprocessing_output_queue=llm_input_queue,
        )
        worker.log = mock_logger

        engine = LLMInferenceEngine(
            model_config=self.model_config,
            batching_config=self.batching_config,
            hostname="test-host",
            devices=[0],
            master_port="29500",
        )
        engine.llm = mock_llm
        engine.sampling_params = MagicMock()

        # Forward two batches
        batch1_queues = [mp.Queue(), mp.Queue()]
        batch2_queues = [mp.Queue(), mp.Queue()]

        worker._forward_to_llm(
            formatted_prompts=["<B1P1>", "<B1P2>"],
            user_prompts=["B1P1", "B1P2"],
            response_queues=batch1_queues,
            latency_metrics=[(time.time(), 0.01, 0.02)] * 2,
            preprocessing_time=0.03,
        )
        worker._forward_to_llm(
            formatted_prompts=["<B2P1>", "<B2P2>"],
            user_prompts=["B2P1", "B2P2"],
            response_queues=batch2_queues,
            latency_metrics=[(time.time(), 0.01, 0.02)] * 2,
            preprocessing_time=0.03,
        )

        # Process batch 1
        batch1_data = llm_input_queue.get(timeout=1)
        responses1, _ = engine.generate(batch1_data[1])

        # Process batch 2
        batch2_data = llm_input_queue.get(timeout=1)
        responses2, _ = engine.generate(batch2_data[1])

        # Verify batches processed correctly
        self.assertEqual(responses1, ["Batch1_Response1", "Batch1_Response2"])
        self.assertEqual(responses2, ["Batch2_Response1", "Batch2_Response2"])
        self.assertEqual(call_count[0], 2)


class TestEngineResponseQueuePreservation(unittest.TestCase):
    """Test that response queues are correctly preserved through the entire
    LLMEngine integration so responses reach the original callers.
    """

    @classmethod
    def setUpClass(cls):
        try:
            mp.set_start_method("dragon", force=True)
        except RuntimeError:
            pass

    @patch("vllm.LLM")
    def test_response_queues_correctly_mapped(self, mock_llm_class):
        """
        Test that each response is sent to the correct caller's queue.
        """

        mock_llm = MagicMock()
        mock_llm_class.return_value = mock_llm

        # Setup outputs in specific order
        outputs = []
        for text in ["Answer for Alice", "Answer for Bob", "Answer for Carol"]:
            mock_output = Mock()
            mock_output.prompt_token_ids = list(range(5))
            mock_inner = Mock()
            mock_inner.text = text
            mock_inner.token_ids = list(range(10))
            mock_output.outputs = [mock_inner]
            outputs.append(mock_output)
        mock_llm.generate.return_value = outputs

        model_config = ModelConfig(
            model_name="test-model",
            hf_token="test-token",
            tp_size=1,
        )
        batching_config = BatchingConfig(enabled=True)

        engine = LLMInferenceEngine(
            model_config=model_config,
            batching_config=batching_config,
            hostname="node",
            devices=[0],
            master_port="29500",
        )
        engine.llm = mock_llm
        engine.sampling_params = MagicMock()

        # Create distinct queues for each caller
        alice_queue = mp.Queue()
        bob_queue = mp.Queue()
        carol_queue = mp.Queue()

        user_prompts = ["Alice question", "Bob question", "Carol question"]
        formatted_prompts = ["<Alice>", "<Bob>", "<Carol>"]
        response_queues = [alice_queue, bob_queue, carol_queue]

        # Generate responses
        responses, metrics = engine.generate(formatted_prompts)

        # Dispatch responses to correct queues
        for user_prompt, response, resp_queue in zip(user_prompts, responses, response_queues):
            resp_queue.put({"user": user_prompt, "assistant": response})

        # Verify each caller gets their own response
        alice_result = alice_queue.get(timeout=1)
        bob_result = bob_queue.get(timeout=1)
        carol_result = carol_queue.get(timeout=1)

        self.assertEqual(alice_result["user"], "Alice question")
        self.assertEqual(alice_result["assistant"], "Answer for Alice")

        self.assertEqual(bob_result["user"], "Bob question")
        self.assertEqual(bob_result["assistant"], "Answer for Bob")

        self.assertEqual(carol_result["user"], "Carol question")
        self.assertEqual(carol_result["assistant"], "Answer for Carol")


if __name__ == "__main__":
    unittest.main()
