"""Tests for Message, DispatchHeader, and DragonQueueProtocol."""

import dragon
import multiprocessing as mp

import queue as _queue
import uuid
from unittest import TestCase, IsolatedAsyncioTestCase, main
from unittest.mock import MagicMock, patch

from dragon.ai.agent.communication.message import Message
from dragon.ai.agent.config.dispatch import DispatchHeader


# ========================================================================
# Message
# ========================================================================

class TestMessage(TestCase):
    """Verify Message dataclass construction and auto-generated fields."""

    def test_construction(self):
        """Required fields are stored; recipient_serialized_queue defaults to empty."""
        header = DispatchHeader(task="Do X", serialized_ddict="sd")
        msg = Message(
            task_id="t1",
            sender_id="orchestrator",
            recipient_id="agent1",
            header=header,
        )
        self.assertEqual(msg.task_id, "t1")
        self.assertEqual(msg.sender_id, "orchestrator")
        self.assertEqual(msg.recipient_id, "agent1")
        self.assertEqual(msg.header.task, "Do X")
        self.assertEqual(msg.recipient_serialized_queue, "")

    def test_auto_id(self):
        """Message.id is auto-generated as a valid UUID."""
        msg = Message(task_id="t", sender_id="s", recipient_id="r")
        uuid.UUID(msg.id)  # validates it's a valid UUID

    def test_default_header_is_none(self):
        """Header defaults to None when not provided."""
        msg = Message(task_id="t", sender_id="s", recipient_id="r")
        self.assertIsNone(msg.header)


# ========================================================================
# DispatchHeader
# ========================================================================

class TestDispatchHeader(TestCase):
    """Verify DispatchHeader construction and auto-generated dispatch_id."""

    def test_construction(self):
        """Required fields stored; optional fields default to None/empty/False."""
        h = DispatchHeader(task="Do research", serialized_ddict="ser-ddict")
        self.assertEqual(h.task, "Do research")
        self.assertEqual(h.serialized_ddict, "ser-ddict")
        self.assertIsNone(h.completion_event)
        self.assertEqual(h.upstream_agent_ids, [])
        self.assertFalse(h.tracing)

    def test_auto_dispatch_id(self):
        """dispatch_id is auto-generated as a valid UUID."""
        h = DispatchHeader(task="t", serialized_ddict="d")
        uuid.UUID(h.dispatch_id)  # validates UUID

    def test_custom_dispatch_id(self):
        """Explicit dispatch_id overrides auto-generation."""
        h = DispatchHeader(task="t", serialized_ddict="d", dispatch_id="custom-id")
        self.assertEqual(h.dispatch_id, "custom-id")


# ========================================================================
# DragonQueueProtocol (mocked Queue)
# ========================================================================

class TestDragonQueueProtocol(IsolatedAsyncioTestCase):
    """Verify DragonQueueProtocol receive, send, and destroy with a mocked Queue."""

    async def test_receive_returns_message(self):
        """receive() returns a Message when the queue has one."""
        mock_queue = MagicMock()
        msg = Message(task_id="t", sender_id="s", recipient_id="r")
        mock_queue.get.return_value = msg
        mock_queue.serialize.return_value = "ser"

        from dragon.ai.agent.communication.dragon_comm import DragonQueueProtocol
        proto = DragonQueueProtocol(queue=mock_queue)
        received = await proto.receive(timeout=1.0)
        self.assertEqual(received.task_id, "t")

    async def test_receive_timeout_returns_none(self):
        """receive() returns None when the queue is empty (timeout)."""
        mock_queue = MagicMock()
        mock_queue.get.side_effect = _queue.Empty()
        mock_queue.serialize.return_value = "ser"

        from dragon.ai.agent.communication.dragon_comm import DragonQueueProtocol
        proto = DragonQueueProtocol(queue=mock_queue)
        received = await proto.receive(timeout=0.01)
        self.assertIsNone(received)

    async def test_receive_dict_coerced_to_message(self):
        """Raw dict from the queue is coerced into a Message object."""
        mock_queue = MagicMock()
        mock_queue.get.return_value = {
            "task_id": "t", "sender_id": "s", "recipient_id": "r",
            "header": {"task": "X"},
        }
        mock_queue.serialize.return_value = "ser"

        from dragon.ai.agent.communication.dragon_comm import DragonQueueProtocol
        proto = DragonQueueProtocol(queue=mock_queue)
        received = await proto.receive(timeout=1.0)
        self.assertIsInstance(received, Message)
        self.assertEqual(received.task_id, "t")

    def test_send(self):
        """send() puts the Message onto the underlying queue."""
        mock_queue = MagicMock()
        mock_queue.serialize.return_value = "ser"

        from dragon.ai.agent.communication.dragon_comm import DragonQueueProtocol
        proto = DragonQueueProtocol(queue=mock_queue)
        msg = Message(task_id="t", sender_id="s", recipient_id="r")
        proto.send(msg)
        mock_queue.put.assert_called_once_with(msg)

    def test_destroy(self):
        """destroy() calls destroy on the underlying queue."""
        mock_queue = MagicMock()
        mock_queue.serialize.return_value = "ser"

        from dragon.ai.agent.communication.dragon_comm import DragonQueueProtocol
        proto = DragonQueueProtocol(queue=mock_queue)
        proto.destroy()
        mock_queue.destroy.assert_called_once()


# ========================================================================
# DragonQueueProtocol — edge cases
# ========================================================================

class TestDragonQueueProtocolEdgeCases(IsolatedAsyncioTestCase):
    """Verify DragonQueueProtocol edge cases."""

    def test_serialized_queue_property(self):
        """serialized_queue is set from queue.serialize() at construction."""
        mock_queue = MagicMock()
        mock_queue.serialize.return_value = "serialized_handle_123"

        from dragon.ai.agent.communication.dragon_comm import DragonQueueProtocol
        proto = DragonQueueProtocol(queue=mock_queue)
        self.assertEqual(proto.serialized_queue, "serialized_handle_123")

    async def test_receive_none_timeout_blocks(self):
        """receive(timeout=None) calls get without timeout argument."""
        mock_queue = MagicMock()
        msg = Message(task_id="t", sender_id="s", recipient_id="r")
        mock_queue.get.return_value = msg
        mock_queue.serialize.return_value = "ser"

        from dragon.ai.agent.communication.dragon_comm import DragonQueueProtocol
        proto = DragonQueueProtocol(queue=mock_queue)
        received = await proto.receive(timeout=None)
        self.assertIsNotNone(received)


# ========================================================================
# DispatchHeader — additional field tests
# ========================================================================

class TestDispatchHeaderEdgeCases(TestCase):
    """Verify DispatchHeader optional fields and non-default values."""

    def test_tracing_enabled(self):
        """tracing=True is correctly stored."""
        h = DispatchHeader(task="t", serialized_ddict="d", tracing=True)
        self.assertTrue(h.tracing)

    def test_upstream_agent_ids(self):
        """upstream_agent_ids stores the provided list."""
        h = DispatchHeader(
            task="t", serialized_ddict="d",
            upstream_agent_ids=["agent_a", "agent_b"],
        )
        self.assertEqual(h.upstream_agent_ids, ["agent_a", "agent_b"])

    def test_completion_event_stored(self):
        """completion_event is stored when provided."""
        evt = MagicMock()
        h = DispatchHeader(
            task="t", serialized_ddict="d", completion_event=evt,
        )
        self.assertIs(h.completion_event, evt)


# ========================================================================
# Message — additional edge case tests
# ========================================================================

class TestMessageEdgeCases(TestCase):
    """Verify Message edge cases."""

    def test_custom_id(self):
        """Custom id overrides auto-generation."""
        msg = Message(
            task_id="t", sender_id="s", recipient_id="r", id="my-custom-id",
        )
        self.assertEqual(msg.id, "my-custom-id")

    def test_recipient_serialized_queue_stored(self):
        """recipient_serialized_queue is stored when provided."""
        msg = Message(
            task_id="t", sender_id="s", recipient_id="r",
            recipient_serialized_queue="ser-queue",
        )
        self.assertEqual(msg.recipient_serialized_queue, "ser-queue")

    def test_complex_header(self):
        """Header with DispatchHeader fields is stored correctly."""
        header = DispatchHeader(
            task="Analyze",
            serialized_ddict="sd",
            upstream_agent_ids=["a", "b"],
        )
        msg = Message(task_id="t", sender_id="s", recipient_id="r", header=header)
        self.assertEqual(msg.header.task, "Analyze")
        self.assertEqual(msg.header.upstream_agent_ids, ["a", "b"])


if __name__ == "__main__":
    mp.set_start_method("dragon")
    main()
