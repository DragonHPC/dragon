"""
Unit tests for the Dragon inference utilities module.

Tests the Inference class.
"""

import dragon
import multiprocessing as mp
from unittest import TestCase, main
from unittest.mock import patch

from dragon.native.machine import System, Node
from dragon.ai.inference.inference_utils import Inference
from dragon.ai.inference.config import (
    InferenceConfig,
    HardwareConfig,
    ModelConfig,
    BatchingConfig,
    GuardrailsConfig,
    DynamicWorkerConfig,
)


class MockTelemetry:
    """Mock for dragon telemetry."""

    def add_data(self, key, value):
        pass


class MockCPUWorker:
    """Mock CPUWorker to avoid actual worker logic in tests."""

    def __init__(self, *args, **kwargs):
        pass

    def initialize(self, *args, **kwargs):
        pass


def create_test_config():
    """Create a test configuration for unit tests."""
    return InferenceConfig(
        hardware=HardwareConfig(
            num_nodes=-1,
            num_gpus=-1,
            num_inf_workers_per_cpu=4,
            node_offset=0,
        ),
        model=ModelConfig(
            model_name="test-model",
            hf_token="test-token",
            tp_size=1,
            dtype="bfloat16",
            max_tokens=100,
        ),
        batching=BatchingConfig(
            enabled=True,
            batch_type="dynamic",
            batch_wait_seconds=0.1,
            max_batch_size=60,
        ),
        guardrails=GuardrailsConfig(
            enabled=True,
            prompt_guard_model="meta-llama/Prompt-Guard-86M",
            prompt_guard_sensitivity=0.5,
        ),
        dynamic_worker=DynamicWorkerConfig(
            enabled=True,
            min_active_workers_per_cpu=1,
            spin_down_threshold_seconds=3600,
            spin_up_threshold_seconds=3,
            spin_up_prompt_threshold=5,
        ),
        flask_secret_key="test-secret-key",
    )


@patch("dragon.ai.inference.inference_utils.Telemetry")
@patch("dragon.ai.inference.inference_utils.mp.Process")
@patch("dragon.ai.inference.inference_utils.CPUWorker")
class TestInferenceUtils(TestCase):
    """Unit tests for Inference class."""

    @classmethod
    def setUpClass(cls):
        """Set up Dragon multiprocessing."""
        try:
            mp.set_start_method("dragon", force=True)
        except RuntimeError:
            pass  # Already set

    def test_infra_args_validator(self, mock_cpu_wrkr, mock_mp_proc, mock_dt):
        """Test validation of infrastructure arguments using real Dragon System/Node."""
        # Mock dragon-telemetry and CPU worker (external dependencies)
        mock_dt.return_value = MockTelemetry()
        mock_cpu_wrkr.return_value = MockCPUWorker()

        # Get real system info
        system = System()
        num_available_nodes = len(system.nodes)

        # Validate config file:
        input_queue = mp.Queue()

        # Test num_nodes = 0 (should raise ValueError)
        config = create_test_config()
        with self.assertRaises(ValueError) as context:
            # Inference(config, num_nodes, offset, input_queue)
            Inference(config, 0, 0, input_queue)
        self.assertIn("num_nodes must be >= 1", str(context.exception))

        # Test num_nodes > available (should raise ValueError)
        config = create_test_config()
        with self.assertRaises(ValueError) as context:
            Inference(config, num_available_nodes + 100, 0, input_queue)
        self.assertIn("only", str(context.exception).lower())

    def test_maybe_subset_nodes_gpus(self, mock_cpu_wrkr, mock_mp_proc, mock_dt):
        """Test node/GPU subsetting using real Dragon System/Node."""
        # Mock dragon-telemetry and CPU worker (external dependencies)
        mock_dt.return_value = MockTelemetry()
        mock_cpu_wrkr.return_value = MockCPUWorker()

        # Get real system info
        system = System()
        num_available_nodes = len(system.nodes)

        # Skip test if no nodes available
        if num_available_nodes < 1:
            self.skipTest("No Dragon nodes available for testing")

        # Get GPU count from first node
        first_node = Node(system.nodes[0])
        num_gpus_per_node = first_node.num_gpus

        # Validate subset of nodes - use num_nodes=1
        input_queue = mp.Queue()
        config = create_test_config()
        # New signature: nference(config, num_nodes, offset, input_queue)
        pipeline = Inference(config, 1, 0, input_queue)
        nodes = pipeline.nodes
        self.assertEqual(len(nodes.keys()), 1)  # Assert number of nodes is 1
        for (hostname, node), gpus in nodes.items():
            self.assertEqual(len(gpus), num_gpus_per_node)  # Assert all GPUs on node

        # Validate subset of gpus - num_gpus is set via config
        if num_gpus_per_node >= 4:
            input_queue = mp.Queue()
            config = create_test_config()
            config.hardware.num_gpus = 4
            # Use -1 for num_nodes to use all nodes
            pipeline = Inference(config, -1, 0, input_queue)
            nodes = pipeline.nodes
            self.assertEqual(len(nodes.keys()), num_available_nodes)  # Assert all nodes
            for (hostname, node), gpus in nodes.items():
                self.assertEqual(len(gpus), 4)  # Assert subset of GPUs


if __name__ == "__main__":
    main()
