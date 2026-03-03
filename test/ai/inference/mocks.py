"""
Shared mock classes and test helpers for Dragon inference tests.

This module provides reusable mock objects that simulate Dragon infrastructure
components without requiring the actual cluster or telemetry systems.
"""

import time
from typing import Any, Dict


class MockTelemetry:
    """Mock Dragon telemetry for testing without real telemetry infrastructure.

    This mock captures all telemetry data points for inspection in tests,
    allowing verification of metrics collection without actual telemetry setup.

    Example:
        telemetry = MockTelemetry()
        some_function_under_test(telemetry)
        telemetry.assert_any_call("latency", 0.5)
    """

    def __init__(self):
        self.data: Dict[str, Any] = {}
        self.add_data_calls: list = []

    def add_data(self, key: str, value: Any) -> None:
        """Record a telemetry data point."""
        self.data[key] = value
        self.add_data_calls.append((key, value))

    def get_data(self, key: str) -> Any:
        """Retrieve recorded telemetry data."""
        return self.data.get(key)

    def assert_any_call(self, key: str, value: Any) -> None:
        """Check if add_data was called with specific key-value pair."""
        if (key, value) not in self.add_data_calls:
            raise AssertionError(f"add_data was not called with ({key!r}, {value!r})")

    def reset(self) -> None:
        """Clear all recorded telemetry data."""
        self.data = {}
        self.add_data_calls = []


class MockNode:
    """Mock Dragon Node for testing without real cluster infrastructure.

    Simulates a compute node with GPUs for testing node allocation,
    worker distribution, and hardware configuration logic.

    Attributes:
        hostname: The node's hostname identifier.
        num_gpus: Number of GPUs available on this node.
        gpu_vendor: The GPU vendor (e.g., "NVIDIA", "AMD").
        gpus: List of GPU device indices.

    Example:
        node = MockNode("gpu-node-0", num_gpus=4, gpu_vendor="NVIDIA")
        assert len(node.gpus) == 4
    """

    def __init__(self, hostname: str, num_gpus: int = 4, gpu_vendor: str = "NVIDIA"):
        self.hostname = hostname
        self.num_gpus = num_gpus
        self.gpu_vendor = gpu_vendor
        self.gpus = list(range(num_gpus))


def create_test_prompt(
    user_prompt: str,
    response_queue,
    system_prompt: str = "You are a helpful assistant.",
) -> tuple:
    """Create a test prompt tuple in the expected format.

    This helper creates prompts in the format expected by the CPU worker
    and preprocessing stages of the inference pipeline.

    Args:
        user_prompt: The user's input prompt text.
        response_queue: A multiprocessing queue for receiving the response.
        system_prompt: Optional system prompt for context.

    Returns:
        Tuple of (user_prompt, formatted_prompt, response_queue, start_time)
    """
    formatted_prompt = (
        f"<|system|>\n{system_prompt}\n<|user|>\n{user_prompt}\n<|assistant|>\n"
    )
    return (user_prompt, formatted_prompt, response_queue, time.time())


def create_preprocessed_prompt(
    user_prompt: str,
    response_queue,
    system_prompt: str = "You are a helpful assistant.",
) -> tuple:
    """Create a preprocessed prompt tuple for LLM module input.

    This helper creates prompts in the format expected by the LLM engine
    after preprocessing (guardrails, batching) has been applied.

    Args:
        user_prompt: The user's input prompt text.
        response_queue: A multiprocessing queue for receiving the response.
        system_prompt: Optional system prompt for context.

    Returns:
        Tuple of (user_prompt, formatted_prompt, response_queue, latency_metrics)
        where latency_metrics = (entry_time, cpu_latency, guard_latency)
    """
    formatted_prompt = (
        f"<|system|>\n{system_prompt}\n<|user|>\n{user_prompt}\n<|assistant|>\n"
    )
    latency_metrics = (time.time(), 0.01, 0.02)
    return (user_prompt, formatted_prompt, response_queue, latency_metrics)
