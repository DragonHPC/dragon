"""
End-to-end integration tests for the complete inference pipeline.

These tests verify the full pipeline from Inference → CPUWorker → InferenceWorker
→ LLMEngine and back to the caller.
"""

import dragon
import multiprocessing as mp
import time
import unittest
from unittest.mock import MagicMock, patch, Mock

from dragon.ai.inference.inference_utils import Inference
from dragon.ai.inference.config import (
    InferenceConfig,
    HardwareConfig,
    ModelConfig,
    BatchingConfig,
    GuardrailsConfig,
    DynamicWorkerConfig,
)

from ..mocks import MockNode


def setUpModule():
    """Set the multiprocessing start method to dragon at module load."""
    try:
        mp.set_start_method("dragon", force=True)
    except RuntimeError:
        pass


class TestEndToEndSinglePrompt(unittest.TestCase):
    """End-to-end integration tests for single prompt flow through the pipeline."""

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
    def test_query_creates_data_on_input_queue(self, mock_telemetry, mock_node_class, mock_system, mock_formatter):
        """Test that Inference.query() puts properly formatted data on input queue."""
        mock_system_instance = MagicMock()
        mock_system_instance.nodes = [0]
        mock_system.return_value = mock_system_instance

        mock_node = MockNode("node-0", num_gpus=4)
        mock_node_class.return_value = mock_node

        mock_formatter.return_value = "<formatted_prompt>"

        config = InferenceConfig(
            hardware=HardwareConfig(num_nodes=1, num_gpus=1),
            model=ModelConfig(
                model_name="test-model",
                hf_token="token",
                tp_size=1,
                system_prompt=["You are helpful."],
            ),
            batching=BatchingConfig(enabled=False),
            guardrails=GuardrailsConfig(enabled=False),
            dynamic_worker=DynamicWorkerConfig(enabled=False),
            flask_secret_key="secret",
        )

        input_queue = mp.Queue()

        dragon_inference = Inference(
            config=config,
            num_nodes=1,
            offset=0,
            input_queue=input_queue,
        )

        # Submit query
        response_queue = mp.Queue()
        user_prompt = "What is 2+2?"
        dragon_inference.query((user_prompt, response_queue))

        # Verify data structure on input queue
        item = input_queue.get(timeout=1)
        self.assertEqual(item[0], user_prompt)
        self.assertEqual(item[1], "<formatted_prompt>")
        # Queue objects are serialized/deserialized through mp.Queue
        self.assertIsInstance(item[2], type(response_queue))
        self.assertIsInstance(item[3], float)  # timestamp

        # Verify response queue is preserved
        test_response = {"test": "data"}
        item[2].put(test_response)
        received = response_queue.get(timeout=1)
        self.assertEqual(received, test_response)

    @patch("dragon.ai.inference.inference_utils.chat_template_formatter")
    @patch("dragon.ai.inference.inference_utils.System")
    @patch("dragon.ai.inference.inference_utils.Node")
    @patch("dragon.ai.inference.inference_utils.Telemetry")
    def test_inference_creates_cpu_workers(self, mock_telemetry, mock_node_class, mock_system, mock_formatter):
        """Test that Inference correctly initializes CPU worker infrastructure."""
        mock_system_instance = MagicMock()
        mock_system_instance.nodes = [0]
        mock_system.return_value = mock_system_instance

        mock_node = MockNode("node-0", num_gpus=4)
        mock_node_class.return_value = mock_node

        config = InferenceConfig(
            hardware=HardwareConfig(num_nodes=1, num_gpus=4, num_inf_workers_per_cpu=2),
            model=ModelConfig(model_name="test-model", hf_token="token", tp_size=2),
            batching=BatchingConfig(enabled=False),
            guardrails=GuardrailsConfig(enabled=False),
            dynamic_worker=DynamicWorkerConfig(enabled=False),
            flask_secret_key="secret",
        )

        input_queue = mp.Queue()

        dragon_inference = Inference(
            config=config,
            num_nodes=1,
            offset=0,
            input_queue=input_queue,
        )

        # Verify CPU worker kwargs are created with correct structure
        self.assertIn("input_queue", dragon_inference.cpu_worker_kwargs)
        self.assertIn("model_config", dragon_inference.cpu_worker_kwargs)
        self.assertIn("batching_config", dragon_inference.cpu_worker_kwargs)
        self.assertIn("guardrails_config", dragon_inference.cpu_worker_kwargs)
        self.assertIn("end_event", dragon_inference.cpu_worker_kwargs)

        # Verify shared resources
        self.assertIsNotNone(dragon_inference.end_event)
        self.assertTrue(hasattr(dragon_inference.end_event, "is_set"))
        self.assertIsNotNone(dragon_inference.cpu_barrier)
        self.assertTrue(hasattr(dragon_inference.cpu_barrier, "wait"))

    @patch("dragon.ai.inference.inference_utils.chat_template_formatter")
    @patch("dragon.ai.inference.inference_utils.System")
    @patch("dragon.ai.inference.inference_utils.Node")
    @patch("dragon.ai.inference.inference_utils.Telemetry")
    def test_multiple_queries_preserve_order(self, mock_telemetry, mock_node_class, mock_system, mock_formatter):
        """Test that multiple queries maintain order through the input queue."""
        mock_system_instance = MagicMock()
        mock_system_instance.nodes = [0]
        mock_system.return_value = mock_system_instance

        mock_node = MockNode("node-0", num_gpus=4)
        mock_node_class.return_value = mock_node

        call_count = [0]

        def format_side_effect(system, user, history, model):
            call_count[0] += 1
            return f"<formatted_{call_count[0]}>"

        mock_formatter.side_effect = format_side_effect

        config = InferenceConfig(
            hardware=HardwareConfig(num_nodes=1, num_gpus=1),
            model=ModelConfig(model_name="test", hf_token="token", tp_size=1),
            batching=BatchingConfig(enabled=False),
            guardrails=GuardrailsConfig(enabled=False),
            dynamic_worker=DynamicWorkerConfig(enabled=False),
            flask_secret_key="secret",
        )

        input_queue = mp.Queue()

        dragon_inference = Inference(
            config=config,
            num_nodes=1,
            offset=0,
            input_queue=input_queue,
        )

        # Submit multiple queries with different response queues
        queues = [mp.Queue() for _ in range(5)]
        prompts = [f"Prompt {i}" for i in range(5)]

        for prompt, queue in zip(prompts, queues):
            dragon_inference.query((prompt, queue))

        # Verify all queries in correct order
        for i in range(5):
            item = input_queue.get(timeout=1)
            self.assertEqual(item[0], f"Prompt {i}")
            self.assertEqual(item[1], f"<formatted_{i+1}>")
            # Queue objects are serialized/deserialized through mp.Queue
            self.assertIsInstance(item[2], type(queues[i]))


class TestEndToEndBatchedPrompts(unittest.TestCase):
    """End-to-end integration tests for batched prompt flow."""

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
    def test_prebatch_query_formats_all_prompts(self, mock_telemetry, mock_node_class, mock_system, mock_formatter):
        """Test that pre-batched queries format all prompts correctly."""
        mock_system_instance = MagicMock()
        mock_system_instance.nodes = [0]
        mock_system.return_value = mock_system_instance

        mock_node = MockNode("node-0", num_gpus=4)
        mock_node_class.return_value = mock_node

        def format_side_effect(system, user, history, model):
            return f"<formatted:{user}>"

        mock_formatter.side_effect = format_side_effect

        config = InferenceConfig(
            hardware=HardwareConfig(num_nodes=1, num_gpus=1),
            model=ModelConfig(model_name="test", hf_token="token", tp_size=1),
            batching=BatchingConfig(enabled=True, batch_type="pre-batch", max_batch_size=10),
            guardrails=GuardrailsConfig(enabled=False),
            dynamic_worker=DynamicWorkerConfig(enabled=False),
            flask_secret_key="secret",
        )

        input_queue = mp.Queue()

        dragon_inference = Inference(
            config=config,
            num_nodes=1,
            offset=0,
            input_queue=input_queue,
        )

        response_queue = mp.Queue()
        batch = ["Question A", "Question B", "Question C"]
        dragon_inference.query((batch, response_queue))

        # Verify batch data on queue
        item = input_queue.get(timeout=1)
        self.assertEqual(item[0], batch)
        self.assertEqual(
            item[1],
            [
                "<formatted:Question A>",
                "<formatted:Question B>",
                "<formatted:Question C>",
            ],
        )

    @patch("dragon.ai.inference.inference_utils.chat_template_formatter")
    @patch("dragon.ai.inference.inference_utils.System")
    @patch("dragon.ai.inference.inference_utils.Node")
    @patch("dragon.ai.inference.inference_utils.Telemetry")
    def test_dynamic_batching_config_propagates(self, mock_telemetry, mock_node_class, mock_system, mock_formatter):
        """Test that dynamic batching configuration propagates to workers."""
        mock_system_instance = MagicMock()
        mock_system_instance.nodes = [0]
        mock_system.return_value = mock_system_instance

        mock_node = MockNode("node-0", num_gpus=2)
        mock_node_class.return_value = mock_node

        config = InferenceConfig(
            hardware=HardwareConfig(num_nodes=1, num_gpus=2),
            model=ModelConfig(model_name="test", hf_token="token", tp_size=1),
            batching=BatchingConfig(
                enabled=True,
                batch_type="dynamic",
                batch_wait_seconds=0.5,
                max_batch_size=8,
            ),
            guardrails=GuardrailsConfig(enabled=False),
            dynamic_worker=DynamicWorkerConfig(enabled=False),
            flask_secret_key="secret",
        )

        input_queue = mp.Queue()

        dragon_inference = Inference(
            config=config,
            num_nodes=1,
            offset=0,
            input_queue=input_queue,
        )

        # Verify batching config flows to cpu_worker_kwargs
        batching_config = dragon_inference.cpu_worker_kwargs["batching_config"]
        self.assertTrue(batching_config.enabled)
        self.assertEqual(batching_config.batch_type, "dynamic")
        self.assertEqual(batching_config.batch_wait_seconds, 0.5)
        self.assertEqual(batching_config.max_batch_size, 8)


class TestEndToEndWithGuardrails(unittest.TestCase):
    """End-to-end integration tests with guardrails enabled."""

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
    def test_guardrails_config_propagates_to_workers(
        self, mock_telemetry, mock_node_class, mock_system, mock_formatter
    ):
        """Test that guardrails configuration propagates through the pipeline."""
        mock_system_instance = MagicMock()
        mock_system_instance.nodes = [0]
        mock_system.return_value = mock_system_instance

        mock_node = MockNode("node-0", num_gpus=2)
        mock_node_class.return_value = mock_node

        config = InferenceConfig(
            hardware=HardwareConfig(num_nodes=1, num_gpus=2),
            model=ModelConfig(model_name="test", hf_token="token", tp_size=1),
            batching=BatchingConfig(enabled=False),
            guardrails=GuardrailsConfig(
                enabled=True,
                prompt_guard_model="meta-llama/Prompt-Guard-86M",
                prompt_guard_sensitivity=0.75,
            ),
            dynamic_worker=DynamicWorkerConfig(enabled=False),
            flask_secret_key="secret",
        )

        input_queue = mp.Queue()

        dragon_inference = Inference(
            config=config,
            num_nodes=1,
            offset=0,
            input_queue=input_queue,
        )

        # Verify guardrails config flows to cpu_worker_kwargs
        guardrails_config = dragon_inference.cpu_worker_kwargs["guardrails_config"]
        self.assertTrue(guardrails_config.enabled)
        self.assertEqual(guardrails_config.prompt_guard_model, "meta-llama/Prompt-Guard-86M")
        self.assertEqual(guardrails_config.prompt_guard_sensitivity, 0.75)


class TestEndToEndMultiNode(unittest.TestCase):
    """End-to-end integration tests for multi-node configuration."""

    @classmethod
    def setUpClass(cls):
        try:
            mp.set_start_method("dragon", force=True)
        except RuntimeError:
            pass

    @patch("dragon.ai.inference.inference_utils.System")
    @patch("dragon.ai.inference.inference_utils.Node")
    @patch("dragon.ai.inference.inference_utils.Telemetry")
    def test_multi_node_worker_structure(self, mock_telemetry, mock_node_class, mock_system):
        """Test that multi-node configuration creates correct worker structure."""
        mock_system_instance = MagicMock()
        mock_system_instance.nodes = [0, 1, 2]
        mock_system.return_value = mock_system_instance

        nodes = [MockNode(f"node-{i}", num_gpus=4) for i in range(3)]

        def node_side_effect(node_id):
            return nodes[node_id]

        mock_node_class.side_effect = node_side_effect

        config = InferenceConfig(
            hardware=HardwareConfig(num_nodes=3, num_gpus=4, num_inf_workers_per_cpu=2),
            model=ModelConfig(model_name="test", hf_token="token", tp_size=2),
            batching=BatchingConfig(enabled=False),
            guardrails=GuardrailsConfig(enabled=False),
            dynamic_worker=DynamicWorkerConfig(enabled=False),
            flask_secret_key="secret",
        )

        input_queue = mp.Queue()

        dragon_inference = Inference(
            config=config,
            num_nodes=3,
            offset=0,
            input_queue=input_queue,
        )

        # Verify worker structure across nodes
        self.assertEqual(len(dragon_inference.nodes), 3)
        self.assertEqual(len(dragon_inference.cpu_and_device_proc_by_hostname), 3)

        # Verify each node has workers
        for hostname in dragon_inference.cpu_and_device_proc_by_hostname:
            self.assertIn(hostname, ["node-0", "node-1", "node-2"])

    @patch("dragon.ai.inference.inference_utils.System")
    @patch("dragon.ai.inference.inference_utils.Node")
    @patch("dragon.ai.inference.inference_utils.Telemetry")
    def test_node_offset_selection(self, mock_telemetry, mock_node_class, mock_system):
        """Test that node offset correctly selects subset of nodes."""
        mock_system_instance = MagicMock()
        mock_system_instance.nodes = [0, 1, 2, 3, 4]
        mock_system.return_value = mock_system_instance

        nodes = [MockNode(f"node-{i}", num_gpus=4) for i in range(5)]

        def node_side_effect(node_id):
            return nodes[node_id]

        mock_node_class.side_effect = node_side_effect

        config = InferenceConfig(
            hardware=HardwareConfig(num_nodes=2, num_gpus=4),
            model=ModelConfig(model_name="test", hf_token="token", tp_size=1),
            batching=BatchingConfig(enabled=False),
            guardrails=GuardrailsConfig(enabled=False),
            dynamic_worker=DynamicWorkerConfig(enabled=False),
            flask_secret_key="secret",
        )

        input_queue = mp.Queue()

        # Use offset=2 to start from node-2
        dragon_inference = Inference(
            config=config,
            num_nodes=2,
            offset=2,
            input_queue=input_queue,
        )

        # Verify only nodes 2 and 3 are selected
        self.assertEqual(len(dragon_inference.nodes), 2)
        hostnames = [hostname for hostname, _ in dragon_inference.nodes.keys()]
        self.assertIn("node-2", hostnames)
        self.assertIn("node-3", hostnames)
        self.assertNotIn("node-0", hostnames)
        self.assertNotIn("node-1", hostnames)


class TestEndToEndPortManagement(unittest.TestCase):
    """End-to-end integration tests for port allocation."""

    @classmethod
    def setUpClass(cls):
        try:
            mp.set_start_method("dragon", force=True)
        except RuntimeError:
            pass

    @patch("dragon.ai.inference.inference_utils.socket.socket")
    @patch("dragon.ai.inference.inference_utils.System")
    @patch("dragon.ai.inference.inference_utils.Node")
    @patch("dragon.ai.inference.inference_utils.Telemetry")
    def test_unique_ports_allocated_across_workers(self, mock_telemetry, mock_node_class, mock_system, mock_socket):
        """Test that each inference worker gets a unique port."""
        mock_system_instance = MagicMock()
        mock_system_instance.nodes = [0]
        mock_system.return_value = mock_system_instance

        mock_node = MockNode("node-0", num_gpus=8)
        mock_node_class.return_value = mock_node

        mock_sock_instance = MagicMock()
        mock_socket.return_value = mock_sock_instance

        config = InferenceConfig(
            hardware=HardwareConfig(num_nodes=1, num_gpus=8, num_inf_workers_per_cpu=4),
            model=ModelConfig(model_name="test", hf_token="token", tp_size=2),
            batching=BatchingConfig(enabled=False),
            guardrails=GuardrailsConfig(enabled=False),
            dynamic_worker=DynamicWorkerConfig(enabled=False),
            flask_secret_key="secret",
        )

        input_queue = mp.Queue()

        dragon_inference = Inference(
            config=config,
            num_nodes=1,
            offset=0,
            input_queue=input_queue,
        )

        # Collect all ports from worker configuration
        all_ports = []
        for (
            hostname,
            cpu_workers,
        ) in dragon_inference.cpu_and_device_proc_by_hostname.items():
            for cpu_wrkr_id, inf_wrkr_configs in cpu_workers.items():
                for devices, port in inf_wrkr_configs:
                    all_ports.append(port)

        # Verify all ports are unique
        self.assertEqual(len(all_ports), len(set(all_ports)))

        # Verify all ports are tracked
        for port in all_ports:
            self.assertIn(port, dragon_inference.in_use_ports)


if __name__ == "__main__":
    unittest.main()
