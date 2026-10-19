"""Integration tests — task message serialization for AgentHost dispatch.

Tests that task messages serialize and deserialize correctly through
cloudpickle, verifying the dispatch path from DragonAgentNode to AgentHost.

Covers:
- Task message structure validation
- State serialization round-trip
- Done channel handle serialization
- Complex state types (TypedDict-like, nested dicts, lists)

Run with:  dragon python -m unittest test.ai.langgraph.integration_tests.test_task_dispatch -v
"""

import dragon  # noqa: F401 — activates Dragon runtime
import multiprocessing as mp


import cloudpickle
from unittest import TestCase, main
from unittest.mock import MagicMock

# Works both as a module (unittest discovery) and as a direct script
# (``dragon test_task_dispatch.py``), where this file's directory is on sys.path.
try:
    from .conftest import echo_agent, stateful_agent
except ImportError:
    from conftest import echo_agent, stateful_agent


# ========================================================================
# Task Message Structure
# ========================================================================

class TestTaskMessageStructure(TestCase):
    """Verify task message structure and required fields."""

    def test_message_contains_required_fields(self):
        """Task message carries task_id, node_name, state, serialized_done_queue."""
        msg = {
            "task_id": "task_123",
            "node_name": "researcher",
            "state": {"messages": ["test"]},
            "serialized_done_queue": b"sdesc",
        }

        self.assertIn("task_id", msg)
        self.assertIn("node_name", msg)
        self.assertIn("state", msg)
        self.assertIn("serialized_done_queue", msg)

    def test_state_is_raw_object(self):
        """state rides as a raw object; the Queue serializes it on put()."""
        state = {"messages": ["hello"]}
        msg = {
            "task_id": "task_001",
            "node_name": "test",
            "state": state,
            "serialized_done_queue": b"mock_queue",
        }

        # The Queue cloudpickles the whole message internally, so the state is
        # passed through as-is (no manual dumps()).
        self.assertIs(msg["state"], state)
        self.assertEqual(cloudpickle.loads(cloudpickle.dumps(msg))["state"], state)

    def test_state_deserializes_correctly(self):
        """Serialized state can be deserialized back to original."""
        state = {"messages": ["hello"], "count": 42}
        state_bytes = cloudpickle.dumps(state)
        deserialized = cloudpickle.loads(state_bytes)

        self.assertEqual(deserialized, state)


# ========================================================================
# State Serialization Round-Trip
# ========================================================================

class TestStateSerializationRoundTrip(TestCase):
    """Verify various state types serialize and deserialize correctly."""

    def test_simple_messages_state(self):
        """Simple messages state round-trips correctly."""
        state = {"messages": ["user message", "assistant reply"]}
        state_bytes = cloudpickle.dumps(state)
        restored = cloudpickle.loads(state_bytes)

        self.assertEqual(restored["messages"], state["messages"])

    def test_typed_dict_state(self):
        """TypedDict-like state round-trips correctly."""
        state = {
            "messages": ["initial"],
            "research": "findings",
            "report": "final report",
            "iteration": 1,
        }
        state_bytes = cloudpickle.dumps(state)
        restored = cloudpickle.loads(state_bytes)

        self.assertEqual(restored, state)

    def test_nested_state(self):
        """Deeply nested state round-trips correctly."""
        state = {
            "messages": [
                {"role": "user", "content": "hello"},
                {"role": "assistant", "content": "hi", "tool_calls": [
                    {"id": "tc1", "function": {"name": "search", "arguments": "{}"}}
                ]},
            ],
            "metadata": {
                "run_id": "run_123",
                "config": {"temperature": 0.7},
            },
        }
        state_bytes = cloudpickle.dumps(state)
        restored = cloudpickle.loads(state_bytes)

        self.assertEqual(restored, state)
        self.assertEqual(
            restored["messages"][1]["tool_calls"][0]["function"]["name"],
            "search"
        )

    def test_empty_state(self):
        """Empty state round-trips correctly."""
        state = {}
        state_bytes = cloudpickle.dumps(state)
        restored = cloudpickle.loads(state_bytes)

        self.assertEqual(restored, {})

    def test_state_with_none_values(self):
        """State with None values round-trips correctly."""
        state = {"messages": None, "research": None, "report": "has value"}
        state_bytes = cloudpickle.dumps(state)
        restored = cloudpickle.loads(state_bytes)

        self.assertIsNone(restored["messages"])
        self.assertEqual(restored["report"], "has value")

    def test_state_with_lists(self):
        """State with list values round-trips correctly."""
        state = {
            "messages": ["a", "b", "c"],
            "tools_used": ["search", "calculator", "browser"],
            "results": [{"id": 1}, {"id": 2}],
        }
        state_bytes = cloudpickle.dumps(state)
        restored = cloudpickle.loads(state_bytes)

        self.assertEqual(len(restored["messages"]), 3)
        self.assertEqual(len(restored["tools_used"]), 3)

    def test_state_with_numbers(self):
        """State with numeric values round-trips correctly."""
        state = {
            "messages": [],
            "count": 42,
            "score": 3.14,
            "large_num": 10 ** 20,
        }
        state_bytes = cloudpickle.dumps(state)
        restored = cloudpickle.loads(state_bytes)

        self.assertEqual(restored["count"], 42)
        self.assertAlmostEqual(restored["score"], 3.14)
        self.assertEqual(restored["large_num"], 10 ** 20)

    def test_state_with_booleans(self):
        """State with boolean values round-trips correctly."""
        state = {
            "messages": [],
            "is_complete": True,
            "has_error": False,
        }
        state_bytes = cloudpickle.dumps(state)
        restored = cloudpickle.loads(state_bytes)

        self.assertTrue(restored["is_complete"])
        self.assertFalse(restored["has_error"])


# ========================================================================
# Agent Function Invocation
# ========================================================================

class TestAgentFunctionInvocation(TestCase):
    """Verify agent functions can be invoked with deserialized state."""

    def test_echo_agent_with_serialized_state(self):
        """echo_agent works with state that went through serialization."""
        state = {"messages": ["test input"]}
        state_bytes = cloudpickle.dumps(state)
        restored_state = cloudpickle.loads(state_bytes)

        result = echo_agent(restored_state)

        self.assertIn("echo:", result["messages"][0])

    def test_stateful_agent_with_serialized_state(self):
        """stateful_agent works with state that went through serialization."""
        state = {"messages": [], "research": "quantum computing advances"}
        state_bytes = cloudpickle.dumps(state)
        restored_state = cloudpickle.loads(state_bytes)

        result = stateful_agent(restored_state)

        self.assertIn("quantum computing", result["analysis"])


# ========================================================================
# Task ID Handling
# ========================================================================

class TestTaskIdHandling(TestCase):
    """Verify task_id handling in messages."""

    def test_uuid_task_id(self):
        """UUID task_id is preserved in message."""
        import uuid
        task_id = str(uuid.uuid4())
        msg = {
            "task_id": task_id,
            "node_name": "test",
            "state": {},
            "serialized_done_queue": b"",
        }

        self.assertEqual(msg["task_id"], task_id)
        # Verify it's a valid UUID
        uuid.UUID(msg["task_id"])

    def test_custom_task_id(self):
        """Custom task_id format is preserved."""
        task_id = "custom_task_2024_001"
        msg = {
            "task_id": task_id,
            "node_name": "test",
            "state": {},
            "serialized_done_queue": b"",
        }

        self.assertEqual(msg["task_id"], task_id)


# ========================================================================
# Node Name Handling
# ========================================================================

class TestNodeNameHandling(TestCase):
    """Verify node_name handling in messages."""

    def test_simple_node_name(self):
        """Simple node name is preserved."""
        msg = {
            "task_id": "t1",
            "node_name": "researcher",
            "state": {},
            "serialized_done_queue": b"",
        }

        self.assertEqual(msg["node_name"], "researcher")

    def test_node_name_with_underscores(self):
        """Node name with underscores is preserved."""
        msg = {
            "task_id": "t1",
            "node_name": "data_processor_v2",
            "state": {},
            "serialized_done_queue": b"",
        }

        self.assertEqual(msg["node_name"], "data_processor_v2")


# ========================================================================
# Full Message Round-Trip
# ========================================================================

class TestFullMessageRoundTrip(TestCase):
    """Verify complete task message can be serialized and restored."""

    def test_message_serialization(self):
        """Complete message can be serialized and deserialized."""
        state = {"messages": ["hello"], "count": 1}

        msg = {
            "task_id": "task_full_001",
            "node_name": "processor",
            "state": state,
            "serialized_done_queue": b"sdesc",
        }

        # Simulate what happens in the host: the Queue round-trips the message,
        # so the host reads state back as a live object.
        restored_state = cloudpickle.loads(cloudpickle.dumps(msg))["state"]
        # Note: done_q would be attached from serialized_done_queue in real usage

        self.assertEqual(restored_state["messages"], ["hello"])
        self.assertEqual(restored_state["count"], 1)

    def test_message_can_be_put_on_queue(self):
        """Message dict can be serialized for queue transport."""
        state = {"messages": ["test"]}
        msg = {
            "task_id": "task_queue_001",
            "node_name": "test_node",
            "state": state,
            "serialized_done_queue": b"mock_queue_bytes",
        }

        # Simulate queue put/get (queue uses pickle internally)
        msg_bytes = cloudpickle.dumps(msg)
        restored_msg = cloudpickle.loads(msg_bytes)

        self.assertEqual(restored_msg["task_id"], "task_queue_001")
        self.assertEqual(restored_msg["node_name"], "test_node")

        # Verify nested state
        self.assertEqual(restored_msg["state"]["messages"], ["test"])


if __name__ == "__main__":
    main()
