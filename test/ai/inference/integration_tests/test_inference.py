"""
Integration tests for the Dragon Inference orchestration layer.

These tests verify the Inference class interactions with queues,
InferenceWorkers, and the overall inference pipeline.
"""

import dragon
import multiprocessing as mp
import time
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
from dragon.ai.inference.inference_utils import Inference

from ..mocks import MockNode


class TestInferenceQueryPipeline(unittest.TestCase):
    """Integration tests for Inference query method putting data
    on the input queue for downstream workers.
    """

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
    def test_query_puts_formatted_data_on_queue(self, mock_telemetry, mock_node_class, mock_system, mock_formatter):
        """Test that query() formats prompt and puts complete data tuple
        on the input queue for InferenceWorkers to consume.
        """
        mock_system_instance = MagicMock()
        mock_system_instance.nodes = [0]
        mock_system.return_value = mock_system_instance

        mock_node = MockNode("node-0", num_gpus=4)
        mock_node_class.return_value = mock_node

        mock_formatter.return_value = "<system>You are helpful.</system><user>Hello!</user>"

        config = InferenceConfig(
            hardware=HardwareConfig(num_nodes=1, num_gpus=4),
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
        dragon_inference.query(("Hello!", response_queue))

        # Verify complete data tuple on input queue
        item = input_queue.get(timeout=1)
        self.assertEqual(item[0], "Hello!")  # user_prompt
        self.assertEqual(item[1], "<system>You are helpful.</system><user>Hello!</user>")  # formatted
        self.assertIsInstance(item[2], type(response_queue))
        self.assertIsInstance(item[3], float)  # start_time

    @patch("dragon.ai.inference.inference_utils.chat_template_formatter")
    @patch("dragon.ai.inference.inference_utils.System")
    @patch("dragon.ai.inference.inference_utils.Node")
    @patch("dragon.ai.inference.inference_utils.Telemetry")
    def test_query_prebatch_formats_all_prompts(self, mock_telemetry, mock_node_class, mock_system, mock_formatter):
        """Test that pre-batch query formats each prompt in the batch
        and preserves them together for downstream processing.
        """
        mock_system_instance = MagicMock()
        mock_system_instance.nodes = [0]
        mock_system.return_value = mock_system_instance

        mock_node = MockNode("node-0", num_gpus=4)
        mock_node_class.return_value = mock_node

        def format_side_effect(system, user, history, model):
            return f"<formatted:{user}>"

        mock_formatter.side_effect = format_side_effect

        config = InferenceConfig(
            hardware=HardwareConfig(num_nodes=1, num_gpus=4),
            model=ModelConfig(model_name="test", hf_token="token", tp_size=1),
            batching=BatchingConfig(enabled=True, batch_type="pre-batch"),
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
        batch = ["Question 1", "Question 2", "Question 3"]
        dragon_inference.query((batch, response_queue))

        # Verify batch data on queue
        item = input_queue.get(timeout=1)
        self.assertEqual(item[0], batch)
        self.assertEqual(
            item[1],
            [
                "<formatted:Question 1>",
                "<formatted:Question 2>",
                "<formatted:Question 3>",
            ],
        )

    @patch("dragon.ai.inference.inference_utils.chat_template_formatter")
    @patch("dragon.ai.inference.inference_utils.System")
    @patch("dragon.ai.inference.inference_utils.Node")
    @patch("dragon.ai.inference.inference_utils.Telemetry")
    def test_multiple_queries_queued_in_order(self, mock_telemetry, mock_node_class, mock_system, mock_formatter):
        """Test that multiple queries are queued in order and can be
        consumed sequentially by workers.
        """
        mock_system_instance = MagicMock()
        mock_system_instance.nodes = [0]
        mock_system.return_value = mock_system_instance

        mock_node = MockNode("node-0", num_gpus=4)
        mock_node_class.return_value = mock_node

        call_count = [0]

        def format_side_effect(system, user, history, model):
            call_count[0] += 1
            return f"<{call_count[0]}:{user}>"

        mock_formatter.side_effect = format_side_effect

        config = InferenceConfig(
            hardware=HardwareConfig(num_nodes=1, num_gpus=4),
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

        # Submit multiple queries
        queues = [mp.Queue() for _ in range(3)]
        dragon_inference.query(("First", queues[0]))
        dragon_inference.query(("Second", queues[1]))
        dragon_inference.query(("Third", queues[2]))

        # Verify order preserved
        item1 = input_queue.get(timeout=1)
        item2 = input_queue.get(timeout=1)
        item3 = input_queue.get(timeout=1)

        self.assertEqual(item1[0], "First")
        self.assertEqual(item2[0], "Second")
        self.assertEqual(item3[0], "Third")


class TestInferenceCPUWorkerIntegration(unittest.TestCase):
    """Integration tests for Inference creating CPU worker configurations
    that will be used to spawn actual workers.
    """

    @classmethod
    def setUpClass(cls):
        try:
            mp.set_start_method("dragon", force=True)
        except RuntimeError:
            pass

    @patch("dragon.ai.inference.inference_utils.System")
    @patch("dragon.ai.inference.inference_utils.Node")
    @patch("dragon.ai.inference.inference_utils.Telemetry")
    def test_cpu_worker_kwargs_include_shared_queue(self, mock_telemetry, mock_node_class, mock_system):
        """Test that cpu_worker_kwargs includes the shared input queue
        so workers can consume from it.
        """
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

        # Verify cpu_worker_kwargs has shared resources
        kwargs = dragon_inference.cpu_worker_kwargs
        self.assertIs(kwargs["input_queue"], input_queue)
        self.assertEqual(kwargs["model_config"], config.model)
        self.assertEqual(kwargs["batching_config"], config.batching)
        self.assertEqual(kwargs["guardrails_config"], config.guardrails)
        self.assertEqual(kwargs["num_inf_workers_per_cpu"], 2)

        # Verify shared synchronization primitives exist
        self.assertIn("end_event", kwargs)
        self.assertIn("cpu_barrier", kwargs)
        self.assertIn("dt", kwargs)

    @patch("dragon.ai.inference.inference_utils.System")
    @patch("dragon.ai.inference.inference_utils.Node")
    @patch("dragon.ai.inference.inference_utils.Telemetry")
    def test_worker_structure_per_node(self, mock_telemetry, mock_node_class, mock_system):
        """Test that cpu_and_device_proc_by_hostname creates correct
        worker structure for each node.
        """
        mock_system_instance = MagicMock()
        mock_system_instance.nodes = [0]
        mock_system.return_value = mock_system_instance

        mock_node = MockNode("node-0", num_gpus=4)
        mock_node_class.return_value = mock_node

        config = InferenceConfig(
            hardware=HardwareConfig(num_nodes=1, num_gpus=4, num_inf_workers_per_cpu=2),
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

        # Verify worker structure
        self.assertIn("node-0", dragon_inference.cpu_and_device_proc_by_hostname)
        node_config = dragon_inference.cpu_and_device_proc_by_hostname["node-0"]

        # Each CPU worker entry should have (devices, port) tuples
        for cpu_wrkr_id, inf_wrkr_configs in node_config.items():
            self.assertIsInstance(inf_wrkr_configs, list)
            for devices, port in inf_wrkr_configs:
                self.assertIsInstance(devices, list)
                self.assertEqual(len(devices), 2)  # tp_size=2
                self.assertIsInstance(port, str)


class TestInferencePortManagement(unittest.TestCase):
    """Integration tests for Inference port allocation to prevent
    conflicts between inference workers.
    """

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
    def test_unique_ports_for_workers(self, mock_telemetry, mock_node_class, mock_system, mock_socket):
        """Test that get_master_port allocates unique ports and tracks
        them to prevent conflicts.
        """
        mock_system_instance = MagicMock()
        mock_system_instance.nodes = [0]
        mock_system.return_value = mock_system_instance

        mock_node = MockNode("node-0", num_gpus=4)
        mock_node_class.return_value = mock_node

        mock_sock_instance = MagicMock()
        mock_socket.return_value = mock_sock_instance

        config = InferenceConfig(
            hardware=HardwareConfig(num_nodes=1, num_gpus=4),
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

        # Track ports allocated during initialization (4 workers with tp_size=1)
        initial_port_count = len(dragon_inference.in_use_ports)

        # Allocate multiple ports
        ports = []
        for _ in range(5):
            port = dragon_inference.get_master_port()
            ports.append(port)

        # Verify all ports are unique
        self.assertEqual(len(ports), len(set(ports)))

        # Verify total ports tracked (initial + 5 new)
        self.assertEqual(len(dragon_inference.in_use_ports), initial_port_count + 5)


class TestInferenceNodeAllocation(unittest.TestCase):
    """Integration tests for Inference node selection and allocation."""

    @classmethod
    def setUpClass(cls):
        try:
            mp.set_start_method("dragon", force=True)
        except RuntimeError:
            pass

    @patch("dragon.ai.inference.inference_utils.System")
    @patch("dragon.ai.inference.inference_utils.Node")
    @patch("dragon.ai.inference.inference_utils.Telemetry")
    def test_node_subset_selection(self, mock_telemetry, mock_node_class, mock_system):
        """Test that Inference correctly selects a subset of available nodes."""
        mock_system_instance = MagicMock()
        mock_system_instance.nodes = [0, 1, 2, 3]
        mock_system.return_value = mock_system_instance

        nodes = [MockNode(f"node-{i}", num_gpus=4) for i in range(4)]

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

        dragon_inference = Inference(
            config=config,
            num_nodes=2,
            offset=0,
            input_queue=input_queue,
        )

        # Should only use 2 of 4 nodes (nodes is a dict with (hostname, node) keys)
        self.assertEqual(len(dragon_inference.nodes), 2)

    @patch("dragon.ai.inference.inference_utils.System")
    @patch("dragon.ai.inference.inference_utils.Node")
    @patch("dragon.ai.inference.inference_utils.Telemetry")
    def test_node_offset_selection(self, mock_telemetry, mock_node_class, mock_system):
        """Test that offset correctly shifts which nodes are selected."""
        mock_system_instance = MagicMock()
        mock_system_instance.nodes = [0, 1, 2, 3]
        mock_system.return_value = mock_system_instance

        nodes = [MockNode(f"node-{i}", num_gpus=4) for i in range(4)]

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

        dragon_inference = Inference(
            config=config,
            num_nodes=2,
            offset=2,  # Start from node 2
            input_queue=input_queue,
        )

        # Should use nodes 2 and 3 (nodes is a dict with (hostname, node) keys)
        self.assertEqual(len(dragon_inference.nodes), 2)
        hostnames = [hostname for hostname, node in dragon_inference.nodes.keys()]
        self.assertIn("node-2", hostnames)
        self.assertIn("node-3", hostnames)


if __name__ == "__main__":
    unittest.main()
