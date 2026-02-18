"""
Unit tests for the LLM engine module.

Tests the LLMInferenceEngine class.

These tests use Dragon multiprocessing primitives.
"""

import dragon
import multiprocessing as mp
from unittest import TestCase, main

from dragon.ai.inference.llm_engine import LLMInferenceEngine
from dragon.ai.inference.config import ModelConfig, BatchingConfig


class TestLLMInferenceEngine(TestCase):
    """Test cases for LLMInferenceEngine class."""

    @classmethod
    def setUpClass(cls):
        """Set up Dragon multiprocessing."""
        try:
            mp.set_start_method("dragon", force=True)
        except RuntimeError:
            pass  # Already set

    def setUp(self):
        """Set up test fixtures."""
        self.model_config = ModelConfig(
            model_name="test-model",
            hf_token="test-token",
            tp_size=2,
            dtype="bfloat16",
            max_tokens=100,
            top_k=50,
            top_p=0.95,
        )
        self.batching_config = BatchingConfig(
            enabled=True,
            batch_type="dynamic",
            max_batch_size=60,
        )

    def test_init(self):
        """Test LLMInferenceEngine initialization."""
        engine = LLMInferenceEngine(
            model_config=self.model_config,
            batching_config=self.batching_config,
            hostname="test-host",
            devices=[0, 1],
            master_port="29500",
        )

        self.assertEqual(engine.model_config, self.model_config)
        self.assertEqual(engine.batching_config, self.batching_config)
        self.assertEqual(engine.hostname, "test-host")
        self.assertEqual(engine.devices, [0, 1])
        self.assertEqual(engine.master_port, "29500")
        self.assertIsNone(engine.llm)
        self.assertIsNone(engine.sampling_params)

    def test_generate_without_initialization(self):
        """Test that generate raises error if not initialized."""
        engine = LLMInferenceEngine(
            model_config=self.model_config,
            batching_config=self.batching_config,
            hostname="test-host",
            devices=[0, 1],
            master_port="29500",
        )

        with self.assertRaises(RuntimeError) as context:
            engine.generate(["Test prompt"])

        self.assertIn("not initialized", str(context.exception))

    def test_calculate_metrics(self):
        """Test _calculate_metrics method."""
        engine = LLMInferenceEngine(
            model_config=self.model_config,
            batching_config=self.batching_config,
            hostname="test-host",
            devices=[0, 1],
            master_port="29500",
        )

        # Create simple output objects to simulate vLLM outputs
        class MockOutputObj:
            def __init__(self, token_ids):
                self.token_ids = token_ids

        class MockOutput:
            def __init__(self, prompt_token_ids, output_token_ids_list):
                self.prompt_token_ids = prompt_token_ids
                self.outputs = [MockOutputObj(ids) for ids in output_token_ids_list]

        outputs = [
            MockOutput([1, 2, 3, 4, 5], [[6, 7, 8]]),  # 5 prompt, 3 output
            MockOutput([1, 2, 3], [[4, 5, 6, 7]]),  # 3 prompt, 4 output
        ]
        inference_time = 1.0  # 1 second

        metrics = engine._calculate_metrics(outputs, inference_time)

        # Total prompt tokens: 5 + 3 = 8
        # Total output tokens: 3 + 4 = 7
        # Total tokens: 8 + 7 = 15
        self.assertEqual(metrics["inference_time"], 1.0)
        self.assertEqual(metrics["requests_per_second"], 2.0)  # 2 requests / 1 sec
        self.assertEqual(metrics["total_tokens_per_second"], 15.0)  # 15 tokens / 1 sec
        self.assertEqual(metrics["output_tokens_per_second"], 7.0)  # 7 output / 1 sec
        self.assertEqual(metrics["total_prompt_tokens"], 8)
        self.assertEqual(metrics["total_output_tokens"], 7)
        self.assertEqual(metrics["total_tokens"], 15)

    def test_calculate_metrics_empty_outputs(self):
        """Test _calculate_metrics with empty outputs."""
        engine = LLMInferenceEngine(
            model_config=self.model_config,
            batching_config=self.batching_config,
            hostname="test-host",
            devices=[0, 1],
            master_port="29500",
        )

        class MockOutput:
            def __init__(self):
                self.prompt_token_ids = None
                self.outputs = []

        outputs = [MockOutput()]
        inference_time = 0.5

        metrics = engine._calculate_metrics(outputs, inference_time)

        self.assertEqual(metrics["total_prompt_tokens"], 0)
        self.assertEqual(metrics["total_output_tokens"], 0)
        self.assertEqual(metrics["total_tokens"], 0)

    def test_calculate_metrics_single_output(self):
        """Test _calculate_metrics with single output."""
        engine = LLMInferenceEngine(
            model_config=self.model_config,
            batching_config=self.batching_config,
            hostname="test-host",
            devices=[0, 1],
            master_port="29500",
        )

        class MockOutputObj:
            def __init__(self, token_ids):
                self.token_ids = token_ids

        class MockOutput:
            def __init__(self, prompt_token_ids, output_token_ids_list):
                self.prompt_token_ids = prompt_token_ids
                self.outputs = [MockOutputObj(ids) for ids in output_token_ids_list]

        outputs = [MockOutput([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], [[11, 12, 13, 14, 15]])]
        inference_time = 0.25  # 0.25 seconds

        metrics = engine._calculate_metrics(outputs, inference_time)

        self.assertEqual(metrics["total_prompt_tokens"], 10)
        self.assertEqual(metrics["total_output_tokens"], 5)
        self.assertEqual(metrics["total_tokens"], 15)
        self.assertEqual(metrics["requests_per_second"], 4.0)  # 1 / 0.25
        self.assertEqual(metrics["total_tokens_per_second"], 60.0)  # 15 / 0.25

    def test_shutdown_without_initialization(self):
        """Test shutdown when LLM was never initialized."""
        engine = LLMInferenceEngine(
            model_config=self.model_config,
            batching_config=self.batching_config,
            hostname="test-host",
            devices=[0, 1],
            master_port="29500",
        )

        # Should not raise any error
        engine.shutdown()


class TestLLMInferenceEngineConfig(TestCase):
    """Test cases for LLMInferenceEngine configuration handling."""

    @classmethod
    def setUpClass(cls):
        """Set up Dragon multiprocessing."""
        try:
            mp.set_start_method("dragon", force=True)
        except RuntimeError:
            pass  # Already set

    def test_config_values_stored(self):
        """Test that config values are properly stored."""
        model_config = ModelConfig(
            model_name="custom-model",
            hf_token="custom-token",
            tp_size=4,
            dtype="float16",
            max_tokens=200,
            top_k=40,
            top_p=0.9,
        )
        batching_config = BatchingConfig(
            enabled=True,
            max_batch_size=30,
        )

        engine = LLMInferenceEngine(
            model_config=model_config,
            batching_config=batching_config,
            hostname="node1",
            devices=[0, 1, 2, 3],
            master_port="30000",
        )

        self.assertEqual(engine.model_config.model_name, "custom-model")
        self.assertEqual(engine.model_config.tp_size, 4)
        self.assertEqual(engine.model_config.dtype, "float16")
        self.assertEqual(engine.model_config.max_tokens, 200)
        self.assertEqual(engine.batching_config.max_batch_size, 30)
        self.assertEqual(len(engine.devices), 4)

    def test_different_device_configs(self):
        """Test engine with different device configurations."""
        model_config = ModelConfig(
            model_name="test-model",
            hf_token="test-token",
            tp_size=1,
        )
        batching_config = BatchingConfig()

        # Single GPU
        engine_single = LLMInferenceEngine(
            model_config=model_config,
            batching_config=batching_config,
            hostname="node1",
            devices=[0],
            master_port="29500",
        )
        self.assertEqual(len(engine_single.devices), 1)

        # Multiple GPUs
        engine_multi = LLMInferenceEngine(
            model_config=model_config,
            batching_config=batching_config,
            hostname="node1",
            devices=[0, 1, 2, 3, 4, 5, 6, 7],
            master_port="29501",
        )
        self.assertEqual(len(engine_multi.devices), 8)


class TestLLMInferenceEngineMetrics(TestCase):
    """Test cases for LLM metrics calculation edge cases."""

    @classmethod
    def setUpClass(cls):
        """Set up Dragon multiprocessing."""
        try:
            mp.set_start_method("dragon", force=True)
        except RuntimeError:
            pass  # Already set

    def setUp(self):
        """Set up test fixtures."""
        self.model_config = ModelConfig(
            model_name="test-model",
            hf_token="test-token",
            tp_size=1,
        )
        self.batching_config = BatchingConfig()
        self.engine = LLMInferenceEngine(
            model_config=self.model_config,
            batching_config=self.batching_config,
            hostname="test-host",
            devices=[0],
            master_port="29500",
        )

    def test_metrics_rounding(self):
        """Test that metrics are properly rounded."""

        class MockOutputObj:
            def __init__(self, token_ids):
                self.token_ids = token_ids

        class MockOutput:
            def __init__(self, prompt_token_ids, output_token_ids_list):
                self.prompt_token_ids = prompt_token_ids
                self.outputs = [MockOutputObj(ids) for ids in output_token_ids_list]

        outputs = [MockOutput([1, 2, 3], [[4, 5, 6, 7]])]
        inference_time = 0.333333  # Repeating decimal

        metrics = self.engine._calculate_metrics(outputs, inference_time)

        # Check rounding to 2 decimal places
        self.assertEqual(metrics["inference_time"], 0.33)
        self.assertEqual(metrics["requests_per_second"], 3.0)  # 1/0.333333 ≈ 3.0

    def test_metrics_large_batch(self):
        """Test metrics calculation with large batch."""

        class MockOutputObj:
            def __init__(self, token_ids):
                self.token_ids = token_ids

        class MockOutput:
            def __init__(self, prompt_token_ids, output_token_ids_list):
                self.prompt_token_ids = prompt_token_ids
                self.outputs = [MockOutputObj(ids) for ids in output_token_ids_list]

        outputs = []
        for i in range(100):
            outputs.append(MockOutput(list(range(50)), [list(range(20))]))

        inference_time = 10.0  # 10 seconds

        metrics = self.engine._calculate_metrics(outputs, inference_time)

        # 100 requests * 50 prompt tokens = 5000 prompt tokens
        # 100 requests * 20 output tokens = 2000 output tokens
        self.assertEqual(metrics["total_prompt_tokens"], 5000)
        self.assertEqual(metrics["total_output_tokens"], 2000)
        self.assertEqual(metrics["total_tokens"], 7000)
        self.assertEqual(metrics["requests_per_second"], 10.0)  # 100/10
        self.assertEqual(metrics["total_tokens_per_second"], 700.0)  # 7000/10
        self.assertEqual(metrics["output_tokens_per_second"], 200.0)  # 2000/10

    def test_metrics_multiple_outputs_per_request(self):
        """Test metrics with multiple output sequences per request (beam search)."""

        class MockOutputObj:
            def __init__(self, token_ids):
                self.token_ids = token_ids

        class MockOutput:
            def __init__(self, prompt_token_ids, output_token_ids_list):
                self.prompt_token_ids = prompt_token_ids
                self.outputs = [MockOutputObj(ids) for ids in output_token_ids_list]

        # Multiple output sequences (like beam search): 3 tokens + 2 tokens
        outputs = [MockOutput([1, 2, 3, 4, 5], [[6, 7, 8], [9, 10]])]
        inference_time = 1.0

        metrics = self.engine._calculate_metrics(outputs, inference_time)

        # Output tokens: 3 + 2 = 5
        self.assertEqual(metrics["total_output_tokens"], 5)
        self.assertEqual(metrics["total_tokens"], 10)


if __name__ == "__main__":
    main()
