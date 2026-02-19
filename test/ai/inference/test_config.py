"""
Unit tests for the configuration module.

Tests the InferenceConfig dataclasses and their validation logic.

These tests use Dragon multiprocessing primitives.
"""

import dragon
import multiprocessing as mp
from unittest import TestCase, main

from dragon.ai.inference.config import (
    HardwareConfig,
    ModelConfig,
    BatchingConfig,
    GuardrailsConfig,
    DynamicWorkerConfig,
    InferenceConfig,
)


class NodeData:
    """A simple test data class representing node properties for validation testing."""

    def __init__(self, num_gpus=8, gpu_vendor="NVIDIA"):
        self.num_gpus = num_gpus
        self.gpu_vendor = gpu_vendor


class TestHardwareConfig(TestCase):
    """Test cases for HardwareConfig dataclass."""

    @classmethod
    def setUpClass(cls):
        """Set up Dragon multiprocessing."""
        try:
            mp.set_start_method("dragon", force=True)
        except RuntimeError:
            pass  # Already set

    def setUp(self):
        """Create mock nodes for testing."""
        self.mock_node_1 = NodeData(num_gpus=8, gpu_vendor="NVIDIA")
        self.mock_node_2 = NodeData(num_gpus=8, gpu_vendor="NVIDIA")
        self.all_nodes = {"node1": self.mock_node_1, "node2": self.mock_node_2}

    def test_default_values(self):
        """Test default configuration values."""
        config = HardwareConfig()
        self.assertEqual(config.num_nodes, -1)
        self.assertEqual(config.num_gpus, -1)
        self.assertEqual(config.num_inf_workers_per_cpu, 4)
        self.assertEqual(config.node_offset, 0)

    def test_validate_num_nodes_valid(self):
        """Test validation passes for valid num_nodes."""
        config = HardwareConfig(num_nodes=1)
        # Should not raise
        config.validate(self.all_nodes)

        config = HardwareConfig(num_nodes=2)
        config.validate(self.all_nodes)

    def test_validate_num_nodes_auto_detect(self):
        """Test validation passes for auto-detect (-1)."""
        config = HardwareConfig(num_nodes=-1)
        # Should not raise
        config.validate(self.all_nodes)

    def test_validate_num_nodes_zero(self):
        """Test validation fails for num_nodes = 0."""
        config = HardwareConfig(num_nodes=0)
        with self.assertRaises(ValueError) as context:
            config.validate(self.all_nodes)
        self.assertIn("num_nodes must be >= 1", str(context.exception))

    def test_validate_num_nodes_exceeds_available(self):
        """Test validation fails when num_nodes exceeds available nodes."""
        config = HardwareConfig(num_nodes=10)
        with self.assertRaises(ValueError) as context:
            config.validate(self.all_nodes)
        self.assertIn("only 2 available", str(context.exception))

    def test_validate_num_gpus_valid(self):
        """Test validation passes for valid num_gpus."""
        config = HardwareConfig(num_gpus=4)
        config.validate(self.all_nodes)

    def test_validate_num_gpus_exceeds_available(self):
        """Test validation fails when num_gpus exceeds available GPUs."""
        config = HardwareConfig(num_gpus=16)
        with self.assertRaises(ValueError) as context:
            config.validate(self.all_nodes)
        self.assertIn("only have available", str(context.exception))

    def test_validate_num_gpus_zero(self):
        """Test validation fails for num_gpus = 0."""
        config = HardwareConfig(num_gpus=0)
        with self.assertRaises(ValueError) as context:
            config.validate(self.all_nodes)
        self.assertIn("at least specify 1 GPU", str(context.exception))

    def test_validate_inf_workers_per_cpu_invalid(self):
        """Test validation fails for invalid num_inf_workers_per_cpu."""
        config = HardwareConfig(num_inf_workers_per_cpu=0)
        with self.assertRaises(ValueError) as context:
            config.validate(self.all_nodes)
        self.assertIn("num_inf_workers_per_cpu must be >= 1", str(context.exception))


class TestModelConfig(TestCase):
    """Test cases for ModelConfig dataclass."""

    @classmethod
    def setUpClass(cls):
        """Set up Dragon multiprocessing."""
        try:
            mp.set_start_method("dragon", force=True)
        except RuntimeError:
            pass  # Already set

    def test_default_values(self):
        """Test default configuration values."""
        config = ModelConfig(model_name="test-model", hf_token="test-token", tp_size=1)
        self.assertEqual(config.dtype, "bfloat16")
        self.assertEqual(config.max_tokens, 100)
        self.assertEqual(config.padding_side, "left")
        self.assertEqual(config.truncation_side, "left")
        self.assertEqual(config.top_k, 50)
        self.assertEqual(config.top_p, 0.95)

    def test_validate_tp_size_valid(self):
        """Test validation passes for valid tp_size."""
        config = ModelConfig(model_name="test-model", hf_token="test-token", tp_size=4)
        # Should not raise for 8 GPUs per node
        config.validate(gpus_per_node=8)

    def test_validate_tp_size_exceeds_gpus(self):
        """Test validation fails when tp_size exceeds available GPUs."""
        config = ModelConfig(model_name="test-model", hf_token="test-token", tp_size=8)
        with self.assertRaises(ValueError) as context:
            config.validate(gpus_per_node=4)
        self.assertIn("cannot exceed", str(context.exception))

    def test_validate_tp_size_zero(self):
        """Test validation fails for tp_size = 0."""
        config = ModelConfig(model_name="test-model", hf_token="test-token", tp_size=0)
        with self.assertRaises(ValueError) as context:
            config.validate(gpus_per_node=8)
        self.assertIn("tp_size must be >= 1", str(context.exception))

    def test_validate_max_tokens_invalid(self):
        """Test validation fails for invalid max_tokens."""
        config = ModelConfig(model_name="test-model", hf_token="test-token", tp_size=1, max_tokens=0)
        with self.assertRaises(ValueError) as context:
            config.validate(gpus_per_node=8)
        self.assertIn("max_tokens must be > 0", str(context.exception))

    def test_validate_top_p_out_of_range(self):
        """Test validation fails for top_p out of [0, 1] range."""
        config = ModelConfig(model_name="test-model", hf_token="test-token", tp_size=1, top_p=1.5)
        with self.assertRaises(ValueError) as context:
            config.validate(gpus_per_node=8)
        self.assertIn("top_p must be in [0, 1]", str(context.exception))


class TestBatchingConfig(TestCase):
    """Test cases for BatchingConfig dataclass."""

    @classmethod
    def setUpClass(cls):
        """Set up Dragon multiprocessing."""
        try:
            mp.set_start_method("dragon", force=True)
        except RuntimeError:
            pass  # Already set

    def test_default_values(self):
        """Test default configuration values."""
        config = BatchingConfig()
        self.assertTrue(config.enabled)
        self.assertEqual(config.batch_type, "dynamic")
        self.assertEqual(config.batch_wait_seconds, 0.1)
        self.assertEqual(config.max_batch_size, 60)

    def test_validate_valid_dynamic_batch(self):
        """Test validation passes for valid dynamic batch config."""
        config = BatchingConfig(enabled=True, batch_type="dynamic")
        config.validate()

    def test_validate_valid_prebatch(self):
        """Test validation passes for valid pre-batch config."""
        config = BatchingConfig(enabled=True, batch_type="pre-batch")
        config.validate()

    def test_validate_invalid_batch_type(self):
        """Test validation fails for invalid batch_type."""
        config = BatchingConfig(batch_type="invalid")
        with self.assertRaises(ValueError) as context:
            config.validate()
        self.assertIn("batch_type must be 'dynamic' or 'pre-batch'", str(context.exception))

    def test_validate_batch_wait_seconds_invalid(self):
        """Test validation fails for invalid batch_wait_seconds."""
        config = BatchingConfig(batch_wait_seconds=0)
        with self.assertRaises(ValueError) as context:
            config.validate()
        self.assertIn("batch_wait_seconds must be > 0", str(context.exception))

    def test_validate_max_batch_size_invalid(self):
        """Test validation fails for invalid max_batch_size."""
        config = BatchingConfig(max_batch_size=0)
        with self.assertRaises(ValueError) as context:
            config.validate()
        self.assertIn("max_batch_size must be >= 1", str(context.exception))


class TestGuardrailsConfig(TestCase):
    """Test cases for GuardrailsConfig dataclass."""

    @classmethod
    def setUpClass(cls):
        """Set up Dragon multiprocessing."""
        try:
            mp.set_start_method("dragon", force=True)
        except RuntimeError:
            pass  # Already set

    def test_default_values(self):
        """Test default configuration values."""
        config = GuardrailsConfig()
        self.assertTrue(config.enabled)
        self.assertEqual(config.prompt_guard_model, "meta-llama/Prompt-Guard-86M")
        self.assertEqual(config.prompt_guard_sensitivity, 0.5)

    def test_validate_sensitivity_valid(self):
        """Test validation passes for valid sensitivity."""
        config = GuardrailsConfig(prompt_guard_sensitivity=0.0)
        config.validate()

        config = GuardrailsConfig(prompt_guard_sensitivity=1.0)
        config.validate()

        config = GuardrailsConfig(prompt_guard_sensitivity=0.5)
        config.validate()

    def test_validate_sensitivity_out_of_range(self):
        """Test validation fails for sensitivity out of [0, 1] range."""
        config = GuardrailsConfig(prompt_guard_sensitivity=1.5)
        with self.assertRaises(ValueError) as context:
            config.validate()
        self.assertIn("prompt_guard_sensitivity must be in [0, 1]", str(context.exception))

        config = GuardrailsConfig(prompt_guard_sensitivity=-0.1)
        with self.assertRaises(ValueError) as context:
            config.validate()
        self.assertIn("prompt_guard_sensitivity must be in [0, 1]", str(context.exception))


class TestDynamicWorkerConfig(TestCase):
    """Test cases for DynamicWorkerConfig dataclass."""

    @classmethod
    def setUpClass(cls):
        """Set up Dragon multiprocessing."""
        try:
            mp.set_start_method("dragon", force=True)
        except RuntimeError:
            pass  # Already set

    def test_default_values(self):
        """Test default configuration values."""
        config = DynamicWorkerConfig()
        self.assertTrue(config.enabled)
        self.assertEqual(config.min_active_workers_per_cpu, 1)
        self.assertEqual(config.spin_down_threshold_seconds, 3600)
        self.assertEqual(config.spin_up_threshold_seconds, 3)
        self.assertEqual(config.spin_up_prompt_threshold, 5)

    def test_validate_valid_config(self):
        """Test validation passes for valid config."""
        config = DynamicWorkerConfig()
        config.validate()

    def test_validate_min_active_workers_invalid(self):
        """Test validation fails for invalid min_active_workers_per_cpu."""
        config = DynamicWorkerConfig(min_active_workers_per_cpu=0)
        with self.assertRaises(ValueError) as context:
            config.validate()
        self.assertIn("min_active_workers_per_cpu must be >= 1", str(context.exception))

    def test_validate_spin_down_threshold_invalid(self):
        """Test validation fails for invalid spin_down_threshold_seconds."""
        config = DynamicWorkerConfig(spin_down_threshold_seconds=0)
        with self.assertRaises(ValueError) as context:
            config.validate()
        self.assertIn("spin_down_threshold_seconds must be >= 1", str(context.exception))

    def test_validate_spin_up_threshold_invalid(self):
        """Test validation fails for invalid spin_up_threshold_seconds."""
        config = DynamicWorkerConfig(spin_up_threshold_seconds=0)
        with self.assertRaises(ValueError) as context:
            config.validate()
        self.assertIn("spin_up_threshold_seconds must be >= 1", str(context.exception))

    def test_validate_spin_up_prompt_threshold_invalid(self):
        """Test validation fails for invalid spin_up_prompt_threshold."""
        config = DynamicWorkerConfig(spin_up_prompt_threshold=0)
        with self.assertRaises(ValueError) as context:
            config.validate()
        self.assertIn("spin_up_prompt_threshold must be >= 1", str(context.exception))


class TestInferenceConfig(TestCase):
    """Test cases for InferenceConfig dataclass."""

    @classmethod
    def setUpClass(cls):
        """Set up Dragon multiprocessing."""
        try:
            mp.set_start_method("dragon", force=True)
        except RuntimeError:
            pass  # Already set

    def test_from_dict_valid(self):
        """Test creating InferenceConfig from valid dictionary."""
        config_dict = {
            "required": {
                "model_name": "test-model",
                "hf_token": "test-token",
                "tp_size": 2,
                "flask_secret_key": "test-secret",
            },
            "hardware": {
                "num_nodes": 2,
                "num_gpus": 4,
                "num_inf_wrkrs_per_cpu": 2,
            },
            "llm": {
                "dtype": "float16",
                "max_tokens": 200,
                "padding_side": "right",
                "truncation_side": "right",
                "top_k": 40,
                "top_p": 0.9,
                "system_prompt": ["Be helpful"],
            },
            "input_batching": {
                "toggle_on": True,
                "type": "dynamic",
                "input_batch_wait_seconds": 0.2,
                "max_batch_limit": 30,
            },
            "guardrails": {
                "toggle_on": True,
                "prompt_guard_model": "meta-llama/Prompt-Guard-86M",
                "prompt_guard_sensitivity": 0.7,
            },
            "dynamic_inf_wrkr": {
                "toggle_on": True,
                "min_active_inf_wrkrs_per_cpu": 2,
                "spin_down_threshold_seconds": 1800,
                "spin_up_threshold_seconds": 5,
                "spin_up_prompt_threshold": 10,
            },
        }

        config = InferenceConfig.from_dict(config_dict)

        # Verify hardware config
        self.assertEqual(config.hardware.num_nodes, 2)
        self.assertEqual(config.hardware.num_gpus, 4)
        self.assertEqual(config.hardware.num_inf_workers_per_cpu, 2)

        # Verify model config
        self.assertEqual(config.model.model_name, "test-model")
        self.assertEqual(config.model.hf_token, "test-token")
        self.assertEqual(config.model.tp_size, 2)
        self.assertEqual(config.model.dtype, "float16")
        self.assertEqual(config.model.max_tokens, 200)

        # Verify batching config
        self.assertTrue(config.batching.enabled)
        self.assertEqual(config.batching.batch_type, "dynamic")
        self.assertEqual(config.batching.batch_wait_seconds, 0.2)
        self.assertEqual(config.batching.max_batch_size, 30)

        # Verify guardrails config
        self.assertTrue(config.guardrails.enabled)
        self.assertEqual(config.guardrails.prompt_guard_sensitivity, 0.7)

        # Verify dynamic worker config
        self.assertTrue(config.dynamic_worker.enabled)
        self.assertEqual(config.dynamic_worker.min_active_workers_per_cpu, 2)
        self.assertEqual(config.dynamic_worker.spin_down_threshold_seconds, 1800)

    def test_from_dict_unexpected_top_level_key(self):
        """Test that unexpected top-level keys raise ValueError."""
        config_dict = {
            "required": {
                "model_name": "test-model",
                "hf_token": "test-token",
                "tp_size": 1,
                "flask_secret_key": "test-secret",
            },
            "unexpected_key": {"some": "value"},
        }

        with self.assertRaises(ValueError) as context:
            InferenceConfig.from_dict(config_dict)
        self.assertIn("Unexpected keys", str(context.exception))

    def test_from_dict_unexpected_section_key(self):
        """Test that unexpected keys within sections raise ValueError."""
        config_dict = {
            "required": {
                "model_name": "test-model",
                "hf_token": "test-token",
                "tp_size": 1,
                "flask_secret_key": "test-secret",
                "unexpected_key": "value",
            },
        }

        with self.assertRaises(ValueError) as context:
            InferenceConfig.from_dict(config_dict)
        self.assertIn("Unexpected keys in 'required' section", str(context.exception))

    def test_from_dict_with_defaults(self):
        """Test that default values are used when sections are missing."""
        config_dict = {
            "required": {
                "model_name": "test-model",
                "hf_token": "test-token",
                "tp_size": 1,
                "flask_secret_key": "test-secret",
            },
        }

        config = InferenceConfig.from_dict(config_dict)

        # Verify defaults are applied
        self.assertEqual(config.hardware.num_nodes, -1)
        self.assertEqual(config.hardware.num_gpus, -1)
        self.assertTrue(config.batching.enabled)
        self.assertTrue(config.guardrails.enabled)

    def test_validate_all(self):
        """Test validate_all method."""
        mock_node = NodeData(num_gpus=8, gpu_vendor="NVIDIA")
        all_nodes = {"node1": mock_node}

        config = InferenceConfig(
            hardware=HardwareConfig(num_nodes=1, num_gpus=4),
            model=ModelConfig(model_name="test", hf_token="token", tp_size=2),
            batching=BatchingConfig(),
            guardrails=GuardrailsConfig(),
            dynamic_worker=DynamicWorkerConfig(),
            flask_secret_key="secret",
        )

        # Should not raise
        config.validate_all(all_nodes)


if __name__ == "__main__":
    main()
