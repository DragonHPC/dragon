"""Integration tests — envelope serialization and transport round-trip.

Tests that completion envelopes serialize and deserialize correctly
through cloudpickle, which is the transport layer used between
AgentHost processes and the DragonWatcher.

Covers:
- Success envelope round-trip (simple dict results)
- Error envelope round-trip (exception objects)
- Complex nested state round-trip
- Edge cases (empty state, None values, large payloads)

Run with:  dragon python -m unittest test.ai.langgraph.integration_tests.test_envelope_roundtrip -v
"""

import dragon  # noqa: F401 — activates Dragon runtime
import multiprocessing as mp


from unittest import TestCase, main

from dragon.ai.langgraph.constants import (
    ENV_STATUS, ENV_PAYLOAD, STATUS_DONE, STATUS_ERROR, EnvelopePickler,
)
from dragon.ai.langgraph.host import (
    _build_done_envelope,
    _build_error_envelope,
)


class _FakeFile:
    """Stands in for the Queue's pickle adapter: records writes, replays reads."""

    def __init__(self, data: bytes = b"") -> None:
        self.data = data
        self.writes: list[bytes] = []

    def write(self, chunk: bytes) -> None:
        self.writes.append(chunk)

    def read(self, size: int = -1) -> bytes:
        return self.data


def _decode_envelope(envelope: tuple) -> dict:
    """Round-trip an envelope through the real codec into one flat dict.

    The Queue delegates serialization wholly to ``EnvelopePickler``, so driving
    it here is what makes this a transport round-trip rather than a bare
    cloudpickle test.  The ``task_id`` travels in the frame header rather than
    the pickled body, so it is merged back in for convenient assertions.
    """
    pickler = EnvelopePickler()
    out = _FakeFile()
    pickler.dump(envelope, out)
    task_id, body, exc = pickler.load(_FakeFile(b"".join(out.writes)))
    if exc is not None:
        raise exc
    return {"task_id": task_id, **body}


# ========================================================================
# Success Envelope Round-Trip
# ========================================================================

class TestDoneEnvelopeRoundTrip(TestCase):
    """Verify done envelope serializes and deserializes correctly."""

    def test_simple_dict_result(self):
        """Simple dict result survives round-trip."""
        result = {"messages": ["hello"], "count": 42}
        data = _build_done_envelope("task_001", result)
        envelope = _decode_envelope(data)

        self.assertEqual(envelope["task_id"], "task_001")
        self.assertEqual(envelope[ENV_STATUS], STATUS_DONE)
        self.assertEqual(envelope[ENV_PAYLOAD], result)

    def test_nested_dict_result(self):
        """Nested dict result survives round-trip."""
        result = {
            "messages": ["msg1", "msg2"],
            "data": {
                "nested": {
                    "deep": [1, 2, 3],
                },
                "flag": True,
            },
            "count": 100,
        }
        data = _build_done_envelope("task_002", result)
        envelope = _decode_envelope(data)

        self.assertEqual(envelope[ENV_PAYLOAD], result)
        self.assertEqual(envelope[ENV_PAYLOAD]["data"]["nested"]["deep"], [1, 2, 3])

    def test_empty_dict_result(self):
        """Empty dict result survives round-trip."""
        result = {}
        data = _build_done_envelope("task_003", result)
        envelope = _decode_envelope(data)

        self.assertEqual(envelope[ENV_PAYLOAD], {})

    def test_none_values_in_result(self):
        """Dict with None values survives round-trip."""
        result = {"messages": None, "data": None}
        data = _build_done_envelope("task_004", result)
        envelope = _decode_envelope(data)

        self.assertIsNone(envelope[ENV_PAYLOAD]["messages"])
        self.assertIsNone(envelope[ENV_PAYLOAD]["data"])

    def test_list_result(self):
        """List result (not dict) survives round-trip."""
        result = ["item1", "item2", {"nested": True}]
        data = _build_done_envelope("task_005", result)
        envelope = _decode_envelope(data)

        self.assertEqual(envelope[ENV_PAYLOAD], result)

    def test_large_result(self):
        """Large result survives round-trip."""
        result = {
            "messages": [f"message_{i}" for i in range(1000)],
            "data": list(range(10000)),
        }
        data = _build_done_envelope("task_006", result)
        envelope = _decode_envelope(data)

        self.assertEqual(len(envelope[ENV_PAYLOAD]["messages"]), 1000)
        self.assertEqual(len(envelope[ENV_PAYLOAD]["data"]), 10000)


# ========================================================================
# Error Envelope Round-Trip
# ========================================================================

class TestErrorEnvelopeRoundTrip(TestCase):
    """Verify error envelope serializes and deserializes correctly."""

    def test_value_error(self):
        """ValueError survives round-trip."""
        exc = ValueError("test value error")
        data = _build_error_envelope("task_err_001", exc)
        envelope = _decode_envelope(data)

        self.assertEqual(envelope["task_id"], "task_err_001")
        self.assertEqual(envelope[ENV_STATUS], STATUS_ERROR)
        self.assertIsInstance(envelope[ENV_PAYLOAD], ValueError)
        self.assertIn("test value error", str(envelope[ENV_PAYLOAD]))

    def test_runtime_error(self):
        """RuntimeError survives round-trip."""
        exc = RuntimeError("runtime failure")
        data = _build_error_envelope("task_err_002", exc)
        envelope = _decode_envelope(data)

        self.assertIsInstance(envelope[ENV_PAYLOAD], RuntimeError)

    def test_key_error(self):
        """KeyError survives round-trip."""
        exc = KeyError("missing_key")
        data = _build_error_envelope("task_err_003", exc)
        envelope = _decode_envelope(data)

        self.assertIsInstance(envelope[ENV_PAYLOAD], KeyError)

    def test_exception_with_args(self):
        """Exception with multiple args survives round-trip."""
        exc = Exception("error", 42, {"detail": "info"})
        data = _build_error_envelope("task_err_004", exc)
        envelope = _decode_envelope(data)

        self.assertIsInstance(envelope[ENV_PAYLOAD], Exception)

    def test_custom_exception(self):
        """Custom exception class survives round-trip."""
        class CustomAgentError(Exception):
            def __init__(self, agent_id: str, message: str):
                self.agent_id = agent_id
                super().__init__(agent_id, message)

        exc = CustomAgentError("agent_1", "custom failure")
        data = _build_error_envelope("task_err_005", exc)
        envelope = _decode_envelope(data)

        self.assertIsInstance(envelope[ENV_PAYLOAD], CustomAgentError)
        self.assertEqual(envelope[ENV_PAYLOAD].agent_id, "agent_1")


# ========================================================================
# Task ID Variations
# ========================================================================

class TestTaskIdVariations(TestCase):
    """Verify envelopes handle various task_id formats."""

    def test_uuid_task_id(self):
        """UUID-formatted task_id survives round-trip."""
        import uuid
        task_id = str(uuid.uuid4())
        data = _build_done_envelope(task_id, {})
        envelope = _decode_envelope(data)

        self.assertEqual(envelope["task_id"], task_id)

    def test_short_task_id(self):
        """Short task_id survives round-trip."""
        data = _build_done_envelope("t1", {"result": True})
        envelope = _decode_envelope(data)

        self.assertEqual(envelope["task_id"], "t1")

    def test_long_task_id(self):
        """Long task_id survives round-trip."""
        task_id = "task_" + "x" * 1000
        data = _build_done_envelope(task_id, {})
        envelope = _decode_envelope(data)

        self.assertEqual(envelope["task_id"], task_id)

    def test_special_chars_task_id(self):
        """Task_id with special characters survives round-trip."""
        task_id = "task/with:special-chars_and.dots"
        data = _build_done_envelope(task_id, {})
        envelope = _decode_envelope(data)

        self.assertEqual(envelope["task_id"], task_id)


# ========================================================================
# State Types for LangGraph
# ========================================================================

class TestLangGraphStateTypes(TestCase):
    """Verify typical LangGraph state structures round-trip correctly."""

    def test_messages_state(self):
        """Typical messages-based state round-trips correctly."""
        state = {
            "messages": [
                {"role": "user", "content": "Hello"},
                {"role": "assistant", "content": "Hi there!"},
            ],
        }
        data = _build_done_envelope("task_lg_001", state)
        envelope = _decode_envelope(data)

        self.assertEqual(envelope[ENV_PAYLOAD], state)
        self.assertEqual(len(envelope[ENV_PAYLOAD]["messages"]), 2)

    def test_typed_dict_like_state(self):
        """State with multiple typed fields round-trips correctly."""
        state = {
            "messages": ["initial message"],
            "research": "Research findings about topic X",
            "report": "Final report: ...",
            "iteration": 3,
            "complete": True,
        }
        data = _build_done_envelope("task_lg_002", state)
        envelope = _decode_envelope(data)

        self.assertEqual(envelope[ENV_PAYLOAD]["research"], "Research findings about topic X")
        self.assertEqual(envelope[ENV_PAYLOAD]["iteration"], 3)
        self.assertTrue(envelope[ENV_PAYLOAD]["complete"])

    def test_annotated_list_field(self):
        """State with list fields (like Annotated[list, operator.add]) round-trips."""
        state = {
            "messages": ["msg1", "msg2", "msg3"],
            "tools_used": ["search", "calculator"],
        }
        data = _build_done_envelope("task_lg_003", state)
        envelope = _decode_envelope(data)

        self.assertEqual(envelope[ENV_PAYLOAD]["messages"], ["msg1", "msg2", "msg3"])


if __name__ == "__main__":
    main()
