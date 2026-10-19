"""Tests for the agent exception hierarchy."""

import dragon  # noqa: F401 — activates Dragon runtime
import multiprocessing as mp

import warnings
from unittest import TestCase, main

from dragon.ai.agent.utils.errors import (
    AgentError,
    ToolExecutionError,
    AgentLoopError,
    HITLBridgeError,
    CompletionSignalError,
    AgentObservabilityWarning,
)


class TestAgentError(TestCase):
    """Verify the base AgentError exception."""

    def test_base_exception(self):
        """AgentError stores a message and inherits from Exception."""
        err = AgentError("something broke")
        self.assertEqual(str(err), "something broke")
        self.assertIsInstance(err, Exception)


class TestToolExecutionError(TestCase):
    """Verify ToolExecutionError captures tool name, args, and original exception."""

    def test_fields(self):
        """tool_name, tool_args, and original exception are stored."""
        orig = ValueError("bad input")
        err = ToolExecutionError("my_tool", {"arg": "val"}, orig)
        self.assertEqual(err.tool_name, "my_tool")
        self.assertEqual(err.tool_args, {"arg": "val"})
        self.assertIs(err.original, orig)

    def test_str_includes_tool_name(self):
        """String representation includes tool name and original error type."""
        err = ToolExecutionError("search", {}, RuntimeError("timeout"))
        self.assertIn("search", str(err))
        self.assertIn("RuntimeError", str(err))

    def test_inherits_agent_error(self):
        """ToolExecutionError is a subclass of AgentError."""
        err = ToolExecutionError("t", {}, Exception())
        self.assertIsInstance(err, AgentError)


class TestAgentLoopError(TestCase):
    """Verify AgentLoopError for max-iteration breaches."""

    def test_message(self):
        """Message stored and inherits AgentError."""
        err = AgentLoopError("Max iterations exceeded")
        self.assertIn("Max iterations", str(err))
        self.assertIsInstance(err, AgentError)


class TestHITLBridgeError(TestCase):
    """Verify HITLBridgeError for bridge-layer failures."""

    def test_message(self):
        """Message stored and inherits AgentError."""
        err = HITLBridgeError("Queue read failed")
        self.assertIn("Queue read failed", str(err))
        self.assertIsInstance(err, AgentError)


class TestCompletionSignalError(TestCase):
    """Verify CompletionSignalError for event.set() failures."""

    def test_message(self):
        """Message stored and inherits AgentError."""
        err = CompletionSignalError("event.set() failed")
        self.assertIsInstance(err, AgentError)


class TestAgentObservabilityWarning(TestCase):
    """Verify the observability warning can be caught or promoted to error."""

    def test_is_user_warning(self):
        """AgentObservabilityWarning is a subclass of UserWarning."""
        self.assertTrue(issubclass(AgentObservabilityWarning, UserWarning))

    def test_can_be_promoted_to_error(self):
        """Can be promoted to an error via warnings.filterwarnings."""
        with warnings.catch_warnings():
            warnings.filterwarnings("error", category=AgentObservabilityWarning)
            with self.assertRaises(AgentObservabilityWarning):
                warnings.warn("test", AgentObservabilityWarning)


if __name__ == "__main__":
    mp.set_start_method("dragon")
    main()
