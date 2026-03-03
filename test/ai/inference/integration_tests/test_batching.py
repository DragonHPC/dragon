"""
Integration tests for the batching module with inference worker integration.

These tests verify the interaction between DynamicBatcher and the
InferenceWorker preprocessing module working together as a pipeline.
"""

import dragon
import multiprocessing as mp
import time
import unittest
from unittest.mock import MagicMock, patch

from dragon.ai.inference.batching import DynamicBatcher
from dragon.ai.inference.config import (
    ModelConfig,
    BatchingConfig,
    GuardrailsConfig,
    DynamicWorkerConfig,
)
from dragon.ai.inference.inference_worker_utils import InferenceWorker

from ..mocks import MockTelemetry


class TestBatcherToInferenceWorkerPipeline(unittest.TestCase):
    """
    Integration tests for the complete pipeline:
    DynamicBatcher collects items → forms batch → InferenceWorker processes and forwards to LLM.
    """

    @classmethod
    def setUpClass(cls):
        """Set up Dragon multiprocessing."""
        try:
            mp.set_start_method("dragon", force=True)
        except RuntimeError:
            pass

    def setUp(self):
        """Set up test fixtures for each test method."""
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
    def test_batcher_collects_and_worker_forwards_batch(self, mock_logging):
        """
        Test the full pipeline: DynamicBatcher collects prompts until max_batch_size,
        then InferenceWorker forwards the complete batch to LLM queue.
        """
        mock_logger = MagicMock()
        mock_logging.return_value = mock_logger

        # Create the batcher with max_batch_size=3
        batcher = DynamicBatcher(
            batch_wait_seconds=10.0,
            max_batch_size=3,
            enabled=True,
        )

        # Create output queue (simulates LLM input queue)
        llm_input_queue = mp.Queue()

        # Create InferenceWorker
        batching_config = BatchingConfig(
            enabled=True,
            batch_type="dynamic",
            batch_wait_seconds=10.0,
            max_batch_size=3,
        )
        worker = InferenceWorker(
            end_event=self.end_event,
            model_config=self.model_config,
            batching_config=batching_config,
            guardrails_config=self.guardrails_config,
            dynamic_worker_config=self.dynamic_worker_config,
            dt=self.dt,
            hostname="test-host",
            devices=[0],
            head_cpu_pid=1234,
            inf_wrkr_id=1,
            preprocessing_output_queue=llm_input_queue,
        )
        worker.log = mock_logger

        # Create response queues for each prompt
        response_queues = [mp.Queue() for _ in range(3)]

        # Simulate incoming prompts being added to batcher
        batch = None
        prompts = [
            ("Hello", "<user>Hello</user>"),
            ("World", "<user>World</user>"),
            ("Test", "<user>Test</user>"),
        ]

        for i, (user_prompt, formatted_prompt) in enumerate(prompts):
            batch = batcher.add_item(
                user_prompt=user_prompt,
                formatted_prompt=formatted_prompt,
                response_queue=response_queues[i],
                latency_metrics=(time.time(), 0.01, 0.02),
            )

        # Batch should be formed after 3rd item (max_batch_size reached)
        self.assertIsNotNone(batch)
        self.assertEqual(batch.size, 3)

        # Now InferenceWorker forwards the batch to LLM
        worker._forward_to_llm(
            formatted_prompts=batch.formatted_prompts,
            user_prompts=batch.user_prompts,
            response_queues=batch.response_queues,
            latency_metrics=batch.latency_metrics,
            preprocessing_time=0.05,
        )

        # Verify LLM received the complete batch
        result = llm_input_queue.get(timeout=1)
        self.assertEqual(result[0], ["Hello", "World", "Test"])
        self.assertEqual(result[1], ["<user>Hello</user>", "<user>World</user>", "<user>Test</user>"])
        self.assertEqual(len(result[2]), 3)  # 3 response queues
        self.assertEqual(result[4], 0.05)  # preprocessing_time

    @patch("dragon.ai.inference.inference_worker_utils.setup_logging")
    def test_batcher_timeout_triggers_partial_batch_forwarding(self, mock_logging):
        """
        Test that when batch timeout expires, a partial batch is forwarded
        to InferenceWorker even if max_batch_size is not reached.
        """
        mock_logger = MagicMock()
        mock_logging.return_value = mock_logger

        # Create batcher with short timeout and large max_batch_size
        batch_timeout = 0.1
        batcher = DynamicBatcher(
            batch_wait_seconds=batch_timeout,
            max_batch_size=100,
            enabled=True,
        )

        llm_input_queue = mp.Queue()

        batching_config = BatchingConfig(
            enabled=True,
            batch_type="dynamic",
            batch_wait_seconds=batch_timeout,
            max_batch_size=100,
        )
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

        response_queue = mp.Queue()

        # Add only 2 items (less than max_batch_size)
        batcher.add_item("Prompt1", "<Prompt1>", response_queue, (time.time(), 0.01, 0.02))
        batcher.add_item("Prompt2", "<Prompt2>", response_queue, (time.time(), 0.01, 0.02))

        # Wait for timeout
        time.sleep(batch_timeout * 1.5)

        # Timeout should have expired
        self.assertTrue(batcher.should_check_batch())

        # Flush the partial batch
        batch = batcher.flush_batch()
        self.assertIsNotNone(batch)
        self.assertEqual(batch.size, 2)

        # Forward partial batch to LLM
        worker._forward_to_llm(
            formatted_prompts=batch.formatted_prompts,
            user_prompts=batch.user_prompts,
            response_queues=batch.response_queues,
            latency_metrics=batch.latency_metrics,
            preprocessing_time=0.03,
        )

        # Verify LLM received the partial batch
        result = llm_input_queue.get(timeout=1)
        self.assertEqual(result[0], ["Prompt1", "Prompt2"])
        self.assertEqual(len(result[2]), 2)

    @patch("dragon.ai.inference.inference_worker_utils.setup_logging")
    def test_multiple_batches_processed_sequentially(self, mock_logging):
        """
        Test that multiple batches are formed and forwarded sequentially
        through the pipeline.
        """
        mock_logger = MagicMock()
        mock_logging.return_value = mock_logger

        batcher = DynamicBatcher(
            batch_wait_seconds=10.0,
            max_batch_size=2,
            enabled=True,
        )

        llm_input_queue = mp.Queue()

        batching_config = BatchingConfig(
            enabled=True,
            batch_type="dynamic",
            batch_wait_seconds=10.0,
            max_batch_size=2,
        )
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

        response_queues = [mp.Queue() for _ in range(4)]
        batches_forwarded = []

        # Add 4 prompts - should create 2 batches of size 2
        for i in range(4):
            batch = batcher.add_item(
                user_prompt=f"Prompt{i}",
                formatted_prompt=f"<Prompt{i}>",
                response_queue=response_queues[i],
                latency_metrics=(time.time(), 0.01, 0.02),
            )
            if batch:
                # Forward each batch as it's formed
                worker._forward_to_llm(
                    formatted_prompts=batch.formatted_prompts,
                    user_prompts=batch.user_prompts,
                    response_queues=batch.response_queues,
                    latency_metrics=batch.latency_metrics,
                    preprocessing_time=0.02,
                )
                batches_forwarded.append(batch)

        # Should have formed 2 batches
        self.assertEqual(len(batches_forwarded), 2)

        # Verify first batch
        result1 = llm_input_queue.get(timeout=1)
        self.assertEqual(result1[0], ["Prompt0", "Prompt1"])

        # Verify second batch
        result2 = llm_input_queue.get(timeout=1)
        self.assertEqual(result2[0], ["Prompt2", "Prompt3"])

    @patch("dragon.ai.inference.inference_worker_utils.setup_logging")
    def test_response_queues_preserved_through_pipeline(self, mock_logging):
        """
        Test that response queues from batcher are correctly preserved
        through InferenceWorker so responses can be sent back to original callers.
        """
        mock_logger = MagicMock()
        mock_logging.return_value = mock_logger

        batcher = DynamicBatcher(
            batch_wait_seconds=10.0,
            max_batch_size=2,
            enabled=True,
        )

        llm_input_queue = mp.Queue()

        batching_config = BatchingConfig(enabled=True, batch_type="dynamic", max_batch_size=2)
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

        # Create distinct response queues for each caller
        caller1_queue = mp.Queue()
        caller2_queue = mp.Queue()

        batcher.add_item("Q1", "<Q1>", caller1_queue, (time.time(), 0.01, 0.02))
        batch = batcher.add_item("Q2", "<Q2>", caller2_queue, (time.time(), 0.01, 0.02))

        # Forward batch
        worker._forward_to_llm(
            formatted_prompts=batch.formatted_prompts,
            user_prompts=batch.user_prompts,
            response_queues=batch.response_queues,
            latency_metrics=batch.latency_metrics,
            preprocessing_time=0.01,
        )

        # Get forwarded data and verify response queues are intact
        result = llm_input_queue.get(timeout=1)
        forwarded_queues = result[2]

        # Simulate LLM sending responses back through the preserved queues
        forwarded_queues[0].put({"response": "Answer1"})
        forwarded_queues[1].put({"response": "Answer2"})

        # Verify original callers receive their responses
        self.assertEqual(caller1_queue.get(timeout=1), {"response": "Answer1"})
        self.assertEqual(caller2_queue.get(timeout=1), {"response": "Answer2"})


class TestBatchingModeIntegration(unittest.TestCase):
    """
    Integration tests for different batching modes (dynamic vs pre-batch)
    with InferenceWorker.
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
        self.guardrails_config = GuardrailsConfig(enabled=False)
        self.dynamic_worker_config = DynamicWorkerConfig(enabled=False)
        self.end_event = mp.Event()
        self.dt = MockTelemetry()

    @patch("dragon.ai.inference.inference_worker_utils.setup_logging")
    def test_dynamic_batching_mode_workflow(self, mock_logging):
        """
        Test complete workflow with dynamic batching:
        Worker uses internal batcher to collect items before forwarding.
        """
        mock_logger = MagicMock()
        mock_logging.return_value = mock_logger

        llm_input_queue = mp.Queue()

        batching_config = BatchingConfig(
            enabled=True,
            batch_type="dynamic",
            batch_wait_seconds=0.1,
            max_batch_size=3,
        )
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

        # Verify worker is configured for dynamic batching
        self.assertTrue(worker.batch_toggle)
        self.assertEqual(worker.batch_type, "dynamic")

        # Verify worker has batcher configured with correct parameters
        self.assertEqual(worker.batch_limit_max, 3)
        self.assertEqual(worker.batch_wait_time, 0.1)

    @patch("dragon.ai.inference.inference_worker_utils.setup_logging")
    def test_prebatch_mode_workflow(self, mock_logging):
        """
        Test workflow with pre-batch mode: batches arrive pre-formed,
        worker forwards them directly without additional batching.
        """
        mock_logger = MagicMock()
        mock_logging.return_value = mock_logger

        llm_input_queue = mp.Queue()

        batching_config = BatchingConfig(
            enabled=True,
            batch_type="pre-batch",
            max_batch_size=10,
        )
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

        # Verify worker is configured for pre-batch mode
        self.assertTrue(worker.batch_toggle)
        self.assertEqual(worker.batch_type, "pre-batch")

        # Forward a pre-formed batch directly
        response_queues = [mp.Queue() for _ in range(3)]
        worker._forward_to_llm(
            formatted_prompts=["<P1>", "<P2>", "<P3>"],
            user_prompts=["P1", "P2", "P3"],
            response_queues=response_queues,
            latency_metrics=[(time.time(), 0.01, 0.02)] * 3,
            preprocessing_time=0.0,
        )

        result = llm_input_queue.get(timeout=1)
        self.assertEqual(result[0], ["P1", "P2", "P3"])

    @patch("dragon.ai.inference.inference_worker_utils.setup_logging")
    def test_batching_disabled_immediate_forwarding(self, mock_logging):
        """
        Test that with batching disabled, items are forwarded immediately
        without any batching delay.
        """
        mock_logger = MagicMock()
        mock_logging.return_value = mock_logger

        llm_input_queue = mp.Queue()

        batching_config = BatchingConfig(enabled=False)
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

        # Verify batching is disabled
        self.assertFalse(worker.batch_toggle)

        # Single item should be forwarded immediately as batch of 1
        response_queue = mp.Queue()
        worker._forward_to_llm(
            formatted_prompts=["<SinglePrompt>"],
            user_prompts=["SinglePrompt"],
            response_queues=[response_queue],
            latency_metrics=[(time.time(), 0.01, 0.02)],
            preprocessing_time=0.0,
        )

        result = llm_input_queue.get(timeout=1)
        self.assertEqual(result[0], ["SinglePrompt"])
        self.assertEqual(len(result[2]), 1)


if __name__ == "__main__":
    unittest.main()
