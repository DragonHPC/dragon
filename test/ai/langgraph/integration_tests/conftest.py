"""Shared test setup for LangGraph integration tests.

This module sets up Dragon multiprocessing.  Shared helpers are importable
by test modules.
"""

import dragon  # noqa: F401 — activates Dragon runtime
import multiprocessing as mp

try:
    mp.set_start_method("dragon", force=True)
except RuntimeError:
    pass

import cloudpickle
from typing import Any, Callable, Optional
from unittest import TestCase

from dragon.ai.langgraph.constants import (
    ENV_STATUS, ENV_PAYLOAD, STATUS_DONE, STATUS_ERROR, pack_envelope,
)


# ---------------------------------------------------------------------------
# Fake agent functions for testing
# ---------------------------------------------------------------------------

def echo_agent(state: dict) -> dict:
    """Simple agent that echoes input messages with a prefix."""
    messages = state.get("messages", [])
    last_msg = messages[-1] if messages else "empty"
    return {"messages": [f"echo: {last_msg}"]}


def failing_agent(state: dict) -> dict:
    """Agent that always raises an exception."""
    raise ValueError("Intentional test failure")


def slow_agent(state: dict) -> dict:
    """Agent that simulates slow processing."""
    import time
    time.sleep(0.5)
    return {"messages": ["slow result"]}


def stateful_agent(state: dict) -> dict:
    """Agent that transforms state."""
    research = state.get("research", "")
    return {
        "messages": [f"analyzed: {research}"],
        "analysis": f"Analysis of: {research}",
    }


def counter_agent(state: dict) -> dict:
    """Agent that counts invocations via state."""
    count = state.get("count", 0) + 1
    return {"count": count, "messages": [f"count: {count}"]}


# ---------------------------------------------------------------------------
# Envelope helpers
# ---------------------------------------------------------------------------

def create_done_envelope(task_id: str, result: Any) -> bytes:
    """Create a serialized success completion envelope."""
    return pack_envelope(task_id, cloudpickle.dumps({
        ENV_STATUS: STATUS_DONE,
        ENV_PAYLOAD: result,
    }))


def create_error_envelope(task_id: str, exc: Exception) -> bytes:
    """Create a serialized error completion envelope."""
    return pack_envelope(task_id, cloudpickle.dumps({
        ENV_STATUS: STATUS_ERROR,
        ENV_PAYLOAD: exc,
    }))


# ---------------------------------------------------------------------------
# Base test case
# ---------------------------------------------------------------------------

class LangGraphIntegrationTestCase(TestCase):
    """Base test case for LangGraph integration tests.

    Provides common setup/teardown and helper methods.
    """

    def setUp(self):
        """Set up test fixtures."""
        import uuid
        self.task_id = str(uuid.uuid4())

    def tearDown(self):
        """Clean up test fixtures."""
        pass

    def assertFutureResult(self, future, expected, timeout: float = 5.0):
        """Assert that a future resolves to the expected result."""
        result = future.result(timeout=timeout)
        self.assertEqual(result, expected)

    def assertFutureRaises(self, future, exc_type, timeout: float = 5.0):
        """Assert that a future raises the expected exception type."""
        with self.assertRaises(exc_type):
            future.result(timeout=timeout)
