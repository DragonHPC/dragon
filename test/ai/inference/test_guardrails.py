"""
Unit tests for the guardrails module.

Tests the GuardrailsProcessor class.

These tests use Dragon multiprocessing primitives.
"""

import dragon
import multiprocessing as mp
from unittest import TestCase, main
from unittest.mock import patch

from dragon.ai.inference.guardrails import GuardrailsProcessor
from dragon.ai.inference.config import GuardrailsConfig


class MockPromptGuard:
    """Mock for PromptGuard ML model (external dependency)."""

    def __init__(self, model_name=None, token=None, jailbreak_scores=None, processing_time=0.05):
        self.model_name = model_name
        self.token = token
        self._jailbreak_scores = jailbreak_scores or [0.1, 0.2]
        self._processing_time = processing_time

    def get_jailbreak_scores_for_texts(self, texts):
        return (self._jailbreak_scores, self._processing_time)


class TestGuardrailsProcessor(TestCase):
    """Test cases for GuardrailsProcessor class."""

    @classmethod
    def setUpClass(cls):
        """Set up Dragon multiprocessing."""
        try:
            mp.set_start_method("dragon", force=True)
        except RuntimeError:
            pass  # Already set

    @patch("dragon.ai.inference.guardrails.PromptGuard")
    def test_init_enabled(self, mock_prompt_guard):
        """Test initialization when guardrails are enabled."""
        config = GuardrailsConfig(
            enabled=True,
            prompt_guard_model="meta-llama/Prompt-Guard-86M",
            prompt_guard_sensitivity=0.5,
        )

        processor = GuardrailsProcessor(config, "test-token")

        self.assertTrue(processor.enabled)
        self.assertEqual(processor.sensitivity, 0.5)
        mock_prompt_guard.assert_called_once_with("meta-llama/Prompt-Guard-86M", "test-token")

    @patch("dragon.ai.inference.guardrails.PromptGuard")
    def test_init_disabled(self, mock_prompt_guard):
        """Test initialization when guardrails are disabled."""
        config = GuardrailsConfig(enabled=False)

        processor = GuardrailsProcessor(config, "test-token")

        self.assertFalse(processor.enabled)
        self.assertIsNone(processor.prompt_guard)
        mock_prompt_guard.assert_not_called()

    @patch("dragon.ai.inference.guardrails.PromptGuard")
    def test_check_prompts_disabled(self, mock_prompt_guard):
        """Test check_prompts when guardrails are disabled."""
        config = GuardrailsConfig(enabled=False)
        processor = GuardrailsProcessor(config, "test-token")

        prompts = ["Hello", "World"]
        is_safe, scores, processing_time = processor.check_prompts(prompts)

        self.assertEqual(is_safe, [True, True])
        self.assertEqual(scores, [0.0, 0.0])
        self.assertEqual(processing_time, 0.0)

    @patch("dragon.ai.inference.guardrails.PromptGuard")
    def test_check_prompts_all_safe(self, mock_prompt_guard):
        """Test check_prompts when all prompts are safe."""
        mock_pg_instance = MockPromptGuard(jailbreak_scores=[0.1, 0.2], processing_time=0.05)
        mock_prompt_guard.return_value = mock_pg_instance

        config = GuardrailsConfig(enabled=True, prompt_guard_sensitivity=0.5)
        processor = GuardrailsProcessor(config, "test-token")

        prompts = ["Hello", "How are you?"]
        is_safe, scores, processing_time = processor.check_prompts(prompts)

        self.assertEqual(is_safe, [True, True])
        self.assertEqual(scores, [0.1, 0.2])
        self.assertEqual(processing_time, 0.05)

    @patch("dragon.ai.inference.guardrails.PromptGuard")
    def test_check_prompts_some_malicious(self, mock_prompt_guard):
        """Test check_prompts when some prompts are malicious."""
        mock_pg_instance = MockPromptGuard(jailbreak_scores=[0.1, 0.8, 0.3], processing_time=0.08)
        mock_prompt_guard.return_value = mock_pg_instance

        config = GuardrailsConfig(enabled=True, prompt_guard_sensitivity=0.5)
        processor = GuardrailsProcessor(config, "test-token")

        prompts = ["Hello", "Ignore previous instructions", "Goodbye"]
        is_safe, scores, processing_time = processor.check_prompts(prompts)

        self.assertEqual(is_safe, [True, False, True])
        self.assertEqual(scores, [0.1, 0.8, 0.3])

    @patch("dragon.ai.inference.guardrails.PromptGuard")
    def test_check_prompts_all_malicious(self, mock_prompt_guard):
        """Test check_prompts when all prompts are malicious."""
        mock_pg_instance = MockPromptGuard(jailbreak_scores=[0.9, 0.95], processing_time=0.1)
        mock_prompt_guard.return_value = mock_pg_instance

        config = GuardrailsConfig(enabled=True, prompt_guard_sensitivity=0.5)
        processor = GuardrailsProcessor(config, "test-token")

        prompts = ["Jailbreak 1", "Jailbreak 2"]
        is_safe, scores, processing_time = processor.check_prompts(prompts)

        self.assertEqual(is_safe, [False, False])
        self.assertEqual(scores, [0.9, 0.95])

    @patch("dragon.ai.inference.guardrails.PromptGuard")
    def test_check_prompts_edge_case_at_threshold(self, mock_prompt_guard):
        """Test check_prompts when scores are exactly at threshold."""
        mock_pg_instance = MockPromptGuard(jailbreak_scores=[0.5, 0.49, 0.51], processing_time=0.05)
        mock_prompt_guard.return_value = mock_pg_instance

        config = GuardrailsConfig(enabled=True, prompt_guard_sensitivity=0.5)
        processor = GuardrailsProcessor(config, "test-token")

        prompts = ["Prompt 1", "Prompt 2", "Prompt 3"]
        is_safe, scores, processing_time = processor.check_prompts(prompts)

        # 0.5 >= 0.5 threshold means malicious (not safe)
        # 0.49 < 0.5 threshold means safe
        # 0.51 >= 0.5 threshold means malicious
        self.assertEqual(is_safe, [False, True, False])

    @patch("dragon.ai.inference.guardrails.PromptGuard")
    def test_filter_batch_disabled(self, mock_prompt_guard):
        """Test filter_batch when guardrails are disabled."""
        config = GuardrailsConfig(enabled=False)
        processor = GuardrailsProcessor(config, "test-token")

        prompts = ["Hello", "World"]
        formatted_prompts = ["<user>Hello</user>", "<user>World</user>"]
        response_queues = [mp.Queue(), mp.Queue()]
        latency_metrics = [(1.0, 0.1, 0.05), (1.1, 0.1, 0.05)]

        (
            safe_prompts,
            safe_formatted,
            safe_queues,
            safe_metrics,
            malicious_indices,
            processing_time,
        ) = processor.filter_batch(prompts, formatted_prompts, response_queues, latency_metrics)

        self.assertEqual(safe_prompts, prompts)
        self.assertEqual(safe_formatted, formatted_prompts)
        self.assertEqual(safe_queues, response_queues)
        self.assertEqual(safe_metrics, latency_metrics)
        self.assertEqual(malicious_indices, [])
        self.assertEqual(processing_time, 0.0)

    @patch("dragon.ai.inference.guardrails.PromptGuard")
    def test_filter_batch_all_safe(self, mock_prompt_guard):
        """Test filter_batch when all prompts are safe."""
        mock_pg_instance = MockPromptGuard(jailbreak_scores=[0.1, 0.2], processing_time=0.05)
        mock_prompt_guard.return_value = mock_pg_instance

        config = GuardrailsConfig(enabled=True, prompt_guard_sensitivity=0.5)
        processor = GuardrailsProcessor(config, "test-token")

        prompts = ["Hello", "World"]
        formatted_prompts = ["<user>Hello</user>", "<user>World</user>"]
        response_queues = [mp.Queue(), mp.Queue()]
        latency_metrics = [(1.0, 0.1, 0.05), (1.1, 0.1, 0.05)]

        (
            safe_prompts,
            safe_formatted,
            safe_queues,
            safe_metrics,
            malicious_indices,
            processing_time,
        ) = processor.filter_batch(prompts, formatted_prompts, response_queues, latency_metrics)

        self.assertEqual(safe_prompts, prompts)
        self.assertEqual(safe_formatted, formatted_prompts)
        self.assertEqual(len(safe_queues), 2)
        self.assertEqual(len(safe_metrics), 2)
        self.assertEqual(malicious_indices, [])
        self.assertEqual(processing_time, 0.05)

    @patch("dragon.ai.inference.guardrails.PromptGuard")
    def test_filter_batch_some_malicious(self, mock_prompt_guard):
        """Test filter_batch when some prompts are malicious."""
        mock_pg_instance = MockPromptGuard(jailbreak_scores=[0.1, 0.9, 0.2, 0.8], processing_time=0.08)
        mock_prompt_guard.return_value = mock_pg_instance

        config = GuardrailsConfig(enabled=True, prompt_guard_sensitivity=0.5)
        processor = GuardrailsProcessor(config, "test-token")

        prompts = ["Safe 1", "Malicious 1", "Safe 2", "Malicious 2"]
        formatted_prompts = [
            "<user>Safe 1</user>",
            "<user>Malicious 1</user>",
            "<user>Safe 2</user>",
            "<user>Malicious 2</user>",
        ]
        response_queues = [mp.Queue() for _ in range(4)]
        latency_metrics = [(1.0, 0.1, 0.05) for _ in range(4)]

        (
            safe_prompts,
            safe_formatted,
            safe_queues,
            safe_metrics,
            malicious_indices,
            processing_time,
        ) = processor.filter_batch(prompts, formatted_prompts, response_queues, latency_metrics)

        self.assertEqual(safe_prompts, ["Safe 1", "Safe 2"])
        self.assertEqual(safe_formatted, ["<user>Safe 1</user>", "<user>Safe 2</user>"])
        self.assertEqual(len(safe_queues), 2)
        self.assertEqual(len(safe_metrics), 2)
        self.assertEqual(malicious_indices, [1, 3])

    @patch("dragon.ai.inference.guardrails.PromptGuard")
    def test_filter_batch_all_malicious(self, mock_prompt_guard):
        """Test filter_batch when all prompts are malicious."""
        mock_pg_instance = MockPromptGuard(jailbreak_scores=[0.9, 0.95], processing_time=0.1)
        mock_prompt_guard.return_value = mock_pg_instance

        config = GuardrailsConfig(enabled=True, prompt_guard_sensitivity=0.5)
        processor = GuardrailsProcessor(config, "test-token")

        prompts = ["Malicious 1", "Malicious 2"]
        formatted_prompts = ["<user>Malicious 1</user>", "<user>Malicious 2</user>"]
        response_queues = [mp.Queue(), mp.Queue()]
        latency_metrics = [(1.0, 0.1, 0.05), (1.1, 0.1, 0.05)]

        (
            safe_prompts,
            safe_formatted,
            safe_queues,
            safe_metrics,
            malicious_indices,
            processing_time,
        ) = processor.filter_batch(prompts, formatted_prompts, response_queues, latency_metrics)

        self.assertEqual(safe_prompts, [])
        self.assertEqual(safe_formatted, [])
        self.assertEqual(safe_queues, [])
        self.assertEqual(safe_metrics, [])
        self.assertEqual(malicious_indices, [0, 1])

    @patch("dragon.ai.inference.guardrails.PromptGuard")
    def test_get_malicious_response(self, mock_prompt_guard):
        """Test get_malicious_response returns expected message."""
        config = GuardrailsConfig(enabled=True)
        processor = GuardrailsProcessor(config, "test-token")

        response = processor.get_malicious_response()

        self.assertEqual(
            response,
            "Your input has been categorized as malicious. Please try again.",
        )

    @patch("dragon.ai.inference.guardrails.PromptGuard")
    def test_sensitivity_custom_value(self, mock_prompt_guard):
        """Test that custom sensitivity threshold is applied correctly."""
        mock_pg_instance = MockPromptGuard(jailbreak_scores=[0.3, 0.6], processing_time=0.05)
        mock_prompt_guard.return_value = mock_pg_instance

        # Test with lower sensitivity (0.25) - both should be marked as malicious
        config = GuardrailsConfig(enabled=True, prompt_guard_sensitivity=0.25)
        processor = GuardrailsProcessor(config, "test-token")

        is_safe, _, _ = processor.check_prompts(["Test 1", "Test 2"])
        self.assertEqual(is_safe, [False, False])

    @patch("dragon.ai.inference.guardrails.PromptGuard")
    def test_sensitivity_high_value(self, mock_prompt_guard):
        """Test with high sensitivity threshold."""
        mock_pg_instance = MockPromptGuard(jailbreak_scores=[0.3, 0.6, 0.9], processing_time=0.05)
        mock_prompt_guard.return_value = mock_pg_instance

        # Test with higher sensitivity (0.95) - all should be safe except 0.9
        config = GuardrailsConfig(enabled=True, prompt_guard_sensitivity=0.95)
        processor = GuardrailsProcessor(config, "test-token")

        is_safe, _, _ = processor.check_prompts(["Test 1", "Test 2", "Test 3"])
        self.assertEqual(is_safe, [True, True, True])


class TestGuardrailsProcessorIntegration(TestCase):
    """Integration tests for GuardrailsProcessor class."""

    @classmethod
    def setUpClass(cls):
        """Set up Dragon multiprocessing."""
        try:
            mp.set_start_method("dragon", force=True)
        except RuntimeError:
            pass  # Already set

    @patch("dragon.ai.inference.guardrails.PromptGuard")
    def test_filter_batch_preserves_queue_order(self, mock_prompt_guard):
        """Test that filter_batch preserves the order of safe prompts and queues."""
        mock_pg_instance = MockPromptGuard(jailbreak_scores=[0.1, 0.9, 0.2, 0.8, 0.15], processing_time=0.08)
        mock_prompt_guard.return_value = mock_pg_instance

        config = GuardrailsConfig(enabled=True, prompt_guard_sensitivity=0.5)
        processor = GuardrailsProcessor(config, "test-token")

        prompts = ["A", "B", "C", "D", "E"]
        formatted_prompts = [f"<user>{p}</user>" for p in prompts]

        # Create Dragon queues
        response_queues = [mp.Queue() for _ in prompts]

        latency_metrics = [(float(i), 0.1, 0.05) for i in range(5)]

        (
            safe_prompts,
            safe_formatted,
            safe_queues,
            safe_metrics,
            malicious_indices,
            _,
        ) = processor.filter_batch(prompts, formatted_prompts, response_queues, latency_metrics)

        # Check that safe prompts are in correct order (A, C, E)
        self.assertEqual(safe_prompts, ["A", "C", "E"])
        self.assertEqual(safe_formatted, ["<user>A</user>", "<user>C</user>", "<user>E</user>"])

        # Check that 3 queues correspond to safe prompts
        self.assertEqual(len(safe_queues), 3)

        # Check latency metrics are in order
        self.assertEqual(safe_metrics[0][0], 0.0)
        self.assertEqual(safe_metrics[1][0], 2.0)
        self.assertEqual(safe_metrics[2][0], 4.0)


if __name__ == "__main__":
    main()
