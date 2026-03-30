"""
Shared test utilities and base classes for integration tests.

This module provides common utilities for integration testing the Dragon
inference pipeline using unittest.
"""

import dragon
import multiprocessing as mp
import time
import unittest
from typing import Dict, Any
from unittest.mock import MagicMock, patch

from dragon.ai.inference.config import (
    ModelConfig,
    BatchingConfig,
    GuardrailsConfig,
    DynamicWorkerConfig,
    InferenceConfig,
    HardwareConfig,
)

from ..mocks import (
    MockTelemetry,
    MockNode,
    create_test_prompt,
    create_preprocessed_prompt,
)


def setUpModule():
    """Set the multiprocessing start method to dragon at module load."""
    try:
        mp.set_start_method("dragon", force=True)
    except RuntimeError:
        # Start method may already be set
        pass


class IntegrationTestCase(unittest.TestCase):
    """Base class for integration tests with common setup and utilities."""

    @classmethod
    def setUpClass(cls):
        """Set up Dragon multiprocessing at class level."""
        try:
            mp.set_start_method("dragon", force=True)
        except RuntimeError:
            pass

    def get_mock_telemetry(self) -> MockTelemetry:
        """Provide a mock telemetry instance for tests."""
        return MockTelemetry()

    def get_model_config(self) -> ModelConfig:
        """Provide a standard model configuration for integration tests."""
        return ModelConfig(
            model_name="meta-llama/Llama-3.2-1B-Instruct",
            hf_token="test-token",
            tp_size=1,
            dtype="bfloat16",
            max_tokens=100,
            top_k=50,
            top_p=0.95,
            padding_side="left",
            truncation_side="left",
            system_prompt=["You are a helpful assistant."],
        )

    def get_batching_config_enabled(self) -> BatchingConfig:
        """Provide a batching configuration with dynamic batching enabled."""
        return BatchingConfig(
            enabled=True,
            batch_type="dynamic",
            batch_wait_seconds=0.1,
            max_batch_size=4,
        )

    def get_batching_config_disabled(self) -> BatchingConfig:
        """Provide a batching configuration with batching disabled."""
        return BatchingConfig(enabled=False)

    def get_batching_config_prebatch(self) -> BatchingConfig:
        """Provide a batching configuration for pre-batched inputs."""
        return BatchingConfig(
            enabled=True,
            batch_type="pre-batch",
            batch_wait_seconds=0.1,
            max_batch_size=4,
        )

    def get_guardrails_config_enabled(self) -> GuardrailsConfig:
        """Provide a guardrails configuration with safety filtering enabled."""
        return GuardrailsConfig(
            enabled=True,
            prompt_guard_model="meta-llama/Prompt-Guard-86M",
            prompt_guard_sensitivity=0.5,
        )

    def get_guardrails_config_disabled(self) -> GuardrailsConfig:
        """Provide a guardrails configuration with safety filtering disabled."""
        return GuardrailsConfig(enabled=False)

    def get_dynamic_worker_config_enabled(self) -> DynamicWorkerConfig:
        """Provide a dynamic worker configuration with spin-up/down enabled."""
        return DynamicWorkerConfig(
            enabled=True,
            min_active_workers_per_cpu=1,
            spin_down_threshold_seconds=60,
            spin_up_threshold_seconds=3,
            spin_up_prompt_threshold=5,
        )

    def get_dynamic_worker_config_disabled(self) -> DynamicWorkerConfig:
        """Provide a dynamic worker configuration with spin-up/down disabled."""
        return DynamicWorkerConfig(enabled=False)

    def get_hardware_config(self) -> HardwareConfig:
        """Provide a standard hardware configuration for tests."""
        return HardwareConfig(
            num_nodes=-1,
            num_gpus=1,
            num_inf_workers_per_cpu=1,
            node_offset=0,
        )

    def get_mock_nodes(self) -> Dict[str, MockNode]:
        """Provide mock cluster nodes for testing."""
        return {
            "node-0": MockNode("node-0", num_gpus=4),
            "node-1": MockNode("node-1", num_gpus=4),
        }

    def get_end_event(self) -> mp.Event:
        """Provide a multiprocessing event for shutdown signaling."""
        return mp.Event()

    def get_response_queue(self) -> mp.Queue:
        """Provide a multiprocessing queue for receiving responses."""
        return mp.Queue()

    def get_input_queue(self) -> mp.Queue:
        """Provide a multiprocessing queue for sending inputs."""
        return mp.Queue()

    def get_barrier_single(self) -> mp.Barrier:
        """Provide a barrier with single party for simple tests."""
        return mp.Barrier(1)
