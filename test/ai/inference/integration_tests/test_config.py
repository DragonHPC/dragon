"""
Integration tests for the configuration system.

These tests verify configuration integration with pipeline components
(InferenceWorker, LLMInferenceEngine, Inference).

Run with: dragon python -m unittest tests/integration_tests/test_config.py
"""

import dragon
import multiprocessing as mp
import unittest
from unittest.mock import MagicMock, patch

from dragon.ai.inference.config import (
    InferenceConfig,
    HardwareConfig,
    ModelConfig,
    BatchingConfig,
    GuardrailsConfig,
    DynamicWorkerConfig,
)

from ..mocks import MockNode, MockTelemetry


class TestConfigWithInferenceWorker(unittest.TestCase):
    """Integration tests for config objects being used by InferenceWorker.
    Tests that configuration flows correctly through component initialization.
    """

    @classmethod
    def setUpClass(cls):
        try:
            mp.set_start_method("dragon", force=True)
        except RuntimeError:
            pass

    def test_config_flows_to_inference_worker(self):
        """Test that all config values correctly flow to InferenceWorker."""
        from dragon.ai.inference.inference_worker_utils import InferenceWorker

        model_config = ModelConfig(
            model_name="test-model",
            hf_token="test-token",
            tp_size=2,
            dtype="float16",
            max_tokens=200,
            top_k=40,
            top_p=0.8,
        )
        batching_config = BatchingConfig(
            enabled=True,
            batch_type="dynamic",
            batch_wait_seconds=0.2,
            max_batch_size=16,
        )
        guardrails_config = GuardrailsConfig(
            enabled=True,
            prompt_guard_sensitivity=0.6,
        )
        dynamic_worker_config = DynamicWorkerConfig(
            enabled=True,
            min_active_workers_per_cpu=2,
            spin_down_threshold_seconds=120,
        )

        dt = MagicMock()
        end_event = mp.Event()

        worker = InferenceWorker(
            end_event=end_event,
            model_config=model_config,
            batching_config=batching_config,
            guardrails_config=guardrails_config,
            dynamic_worker_config=dynamic_worker_config,
            dt=dt,
        )

        # Verify model config values flow through
        self.assertEqual(worker.model_name, "test-model")
        self.assertEqual(worker.dtype, "float16")
        self.assertEqual(worker.tp_size, 2)
        self.assertEqual(worker.max_new_tokens, 200)
        self.assertEqual(worker.top_k, 40)
        self.assertEqual(worker.top_p, 0.8)

        # Verify batching config values flow through
        self.assertTrue(worker.batch_toggle)
        self.assertEqual(worker.batch_type, "dynamic")
        self.assertEqual(worker.batch_wait_time, 0.2)
        self.assertEqual(worker.batch_limit_max, 16)

        # Verify guardrails config values flow through
        self.assertTrue(worker.prompt_guard_toggle)
        self.assertEqual(worker.prompt_guard_sensitivity, 0.6)

        # Verify dynamic worker config values flow through
        self.assertTrue(worker.dynamic_inf_wrkr_toggle)
        self.assertEqual(worker.spin_down_threshold, 120)

    @patch("dragon.ai.inference.inference_worker_utils.setup_logging")
    def test_worker_uses_config_for_response_routing(self, mock_logging):
        """Test that InferenceWorker uses config to correctly route responses
        through queues with proper telemetry.
        """
        from dragon.ai.inference.inference_worker_utils import InferenceWorker
        import time

        mock_logger = MagicMock()
        mock_logging.return_value = mock_logger

        model_config = ModelConfig(
            model_name="test-model",
            hf_token="test-token",
            tp_size=2,
        )
        batching_config = BatchingConfig(enabled=True, max_batch_size=4)
        guardrails_config = GuardrailsConfig(enabled=True)
        dynamic_worker_config = DynamicWorkerConfig(enabled=False)

        dt = MockTelemetry()
        end_event = mp.Event()
        response_queue = mp.Queue()

        worker = InferenceWorker(
            end_event=end_event,
            model_config=model_config,
            batching_config=batching_config,
            guardrails_config=guardrails_config,
            dynamic_worker_config=dynamic_worker_config,
            dt=dt,
            hostname="gpu-node-01",
            devices=[0, 1],
            inf_wrkr_id=3,
        )
        worker.log = mock_logger

        # Send response through worker
        worker._send_responses(
            user_prompts=["Test prompt"],
            responses=["Test response"],
            qs=[response_queue],
            preprocessing_time=0.1,
            tuple_latency_timestamps=[(time.time() - 1.0, 0.05, 0.03)],
            model_network_latency=0.02,
            metrics={
                "inference_time": 1.0,
                "requests_per_second": 1.0,
                "total_tokens_per_second": 100.0,
                "output_tokens_per_second": 50.0,
            },
        )

        # Verify response reflects config-driven worker state
        result = response_queue.get(timeout=1)
        self.assertEqual(result["hostname"], "gpu-node-01")
        self.assertEqual(result["devices"], [0, 1])
        self.assertEqual(result["inf_worker_id"], 3)

        # Verify telemetry was recorded
        keys_recorded = [call[0] for call in dt.add_data_calls]
        self.assertIn("model_inference_latency", keys_recorded)


class TestConfigWithLLMEngine(unittest.TestCase):
    """Integration tests for config objects being used by LLMInferenceEngine."""

    @classmethod
    def setUpClass(cls):
        try:
            mp.set_start_method("dragon", force=True)
        except RuntimeError:
            pass

    def test_config_flows_to_llm_engine(self):
        """Test that config values correctly flow to LLMInferenceEngine."""
        from dragon.ai.inference.llm_engine import LLMInferenceEngine

        model_config = ModelConfig(
            model_name="meta-llama/Llama-3.2-1B-Instruct",
            hf_token="test-token",
            tp_size=2,
            dtype="bfloat16",
            max_tokens=150,
            top_k=50,
            top_p=0.95,
        )
        batching_config = BatchingConfig(
            enabled=True,
            max_batch_size=32,
        )

        engine = LLMInferenceEngine(
            model_config=model_config,
            batching_config=batching_config,
            hostname="node-0",
            devices=[0, 1],
            master_port="29500",
        )

        # Verify config is accessible
        self.assertEqual(engine.model_config.model_name, "meta-llama/Llama-3.2-1B-Instruct")
        self.assertEqual(engine.model_config.tp_size, 2)
        self.assertEqual(engine.model_config.dtype, "bfloat16")
        self.assertEqual(engine.batching_config.max_batch_size, 32)

    @patch("torch.cuda.synchronize")
    @patch("vllm.LLM")
    def test_engine_uses_config_for_batch_processing(self, mock_llm_class, mock_torch_sync):
        """Test that LLMInferenceEngine uses batching config when processing
        and returns responses to queues.
        """
        from dragon.ai.inference.llm_engine import LLMInferenceEngine
        from unittest.mock import Mock

        # Setup mock LLM
        mock_llm = MagicMock()
        mock_llm_class.return_value = mock_llm

        outputs = []
        for i in range(3):
            mock_output = Mock()
            mock_output.prompt_token_ids = list(range(10))
            mock_inner = Mock()
            mock_inner.text = f"Response {i+1}"
            mock_inner.token_ids = list(range(15))
            mock_output.outputs = [mock_inner]
            outputs.append(mock_output)
        mock_llm.generate.return_value = outputs

        model_config = ModelConfig(
            model_name="test-model",
            hf_token="test-token",
            tp_size=1,
        )
        batching_config = BatchingConfig(enabled=True, max_batch_size=4)

        engine = LLMInferenceEngine(
            model_config=model_config,
            batching_config=batching_config,
            hostname="node-0",
            devices=[0],
            master_port="29500",
        )
        engine.llm = mock_llm
        engine.sampling_params = MagicMock()

        # Process batch
        response_queues = [mp.Queue() for _ in range(3)]
        responses, metrics = engine.generate(["<P1>", "<P2>", "<P3>"])

        # Send to queues
        for resp, queue in zip(responses, response_queues):
            queue.put({"assistant": resp})

        # Verify responses reached queues
        for i, queue in enumerate(response_queues):
            result = queue.get(timeout=1)
            self.assertEqual(result["assistant"], f"Response {i+1}")


class TestConfigWithInference(unittest.TestCase):
    """Integration tests for config objects being used by Inference orchestrator."""

    @classmethod
    def setUpClass(cls):
        try:
            mp.set_start_method("dragon", force=True)
        except RuntimeError:
            pass

    @patch("dragon.ai.inference.inference_utils.chat_template_formatter")
    @patch("dragon.ai.inference.inference_utils.System")
    @patch("dragon.ai.inference.inference_utils.Node")
    @patch("dragon.ai.inference.inference_utils.Telemetry")
    def test_config_flows_to_dragon_inference(self, mock_telemetry, mock_node_class, mock_system, mock_formatter):
        """Test that config flows through Inference and reaches input queue."""
        from dragon.ai.inference.inference_utils import Inference

        mock_system_instance = MagicMock()
        mock_system_instance.nodes = [0]
        mock_system.return_value = mock_system_instance

        mock_node = MockNode("node-0", num_gpus=4)
        mock_node_class.return_value = mock_node

        mock_formatter.return_value = "<formatted>"

        config = InferenceConfig(
            hardware=HardwareConfig(num_nodes=1, num_gpus=4),
            model=ModelConfig(
                model_name="test-model",
                hf_token="token",
                tp_size=2,
                system_prompt=["You are helpful."],
            ),
            batching=BatchingConfig(enabled=True, batch_type="dynamic", max_batch_size=16),
            guardrails=GuardrailsConfig(enabled=True, prompt_guard_sensitivity=0.7),
            dynamic_worker=DynamicWorkerConfig(enabled=True),
            flask_secret_key="secret",
        )

        input_queue = mp.Queue()

        dragon_inference = Inference(
            config=config,
            num_nodes=1,
            offset=0,
            input_queue=input_queue,
        )

        # Verify config values are accessible
        self.assertEqual(dragon_inference.model_name, "test-model")
        self.assertEqual(dragon_inference.tp_size, 2)
        self.assertTrue(dragon_inference.batch_toggle)
        self.assertEqual(dragon_inference.batch_limit_max, 16)

        # Test query puts data on queue
        response_queue = mp.Queue()
        dragon_inference.query(("Hello!", response_queue))

        # Verify data reached input queue
        item = input_queue.get(timeout=1)
        self.assertEqual(item[0], "Hello!")
        self.assertEqual(item[1], "<formatted>")

    @patch("dragon.ai.inference.inference_utils.System")
    @patch("dragon.ai.inference.inference_utils.Node")
    @patch("dragon.ai.inference.inference_utils.Telemetry")
    def test_config_creates_cpu_worker_kwargs(self, mock_telemetry, mock_node_class, mock_system):
        """Test that config is correctly packaged into cpu_worker_kwargs."""
        from dragon.ai.inference.inference_utils import Inference

        mock_system_instance = MagicMock()
        mock_system_instance.nodes = [0]
        mock_system.return_value = mock_system_instance

        mock_node = MockNode("node-0", num_gpus=4)
        mock_node_class.return_value = mock_node

        config = InferenceConfig(
            hardware=HardwareConfig(num_nodes=1, num_gpus=4, num_inf_workers_per_cpu=2),
            model=ModelConfig(model_name="test", hf_token="token", tp_size=2),
            batching=BatchingConfig(enabled=True),
            guardrails=GuardrailsConfig(enabled=True),
            dynamic_worker=DynamicWorkerConfig(enabled=True),
            flask_secret_key="secret",
        )

        input_queue = mp.Queue()

        dragon_inference = Inference(
            config=config,
            num_nodes=1,
            offset=0,
            input_queue=input_queue,
        )

        # Verify cpu_worker_kwargs contains config objects
        kwargs = dragon_inference.cpu_worker_kwargs
        self.assertEqual(kwargs["model_config"], config.model)
        self.assertEqual(kwargs["batching_config"], config.batching)
        self.assertEqual(kwargs["guardrails_config"], config.guardrails)
        self.assertEqual(kwargs["dynamic_worker_config"], config.dynamic_worker)
        self.assertEqual(kwargs["num_inf_workers_per_cpu"], 2)


if __name__ == "__main__":
    unittest.main()
