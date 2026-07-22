"""Integration tests — Dragon Queue pickle round-trip for Message / DispatchHeader.

Standard ``pickle.dumps/loads`` can succeed in unit tests but fail when objects
are serialized through a real Dragon Queue (different buffer mechanics, managed
memory, block sizes).  These tests prove end-to-end fidelity.

Run with:  dragon python -m unittest test.ai.agent.integration_tests.test_queue_pickle_roundtrip -v
"""

import dragon  # noqa: F401 — activates Dragon runtime
import multiprocessing as mp

import uuid
from unittest import TestCase, main

from dragon.native.queue import Queue

from dragon.ai.agent.communication.message import Message
from dragon.ai.agent.config import DispatchHeader


class TestMessageQueueRoundTrip(TestCase):
    """Put a Message onto a Dragon Queue, get it back, verify every field."""

    def setUp(self):
        self.queue = Queue()

    def tearDown(self):
        self.queue.destroy()

    def test_plain_message(self):
        """Minimal Message survives put/get on Dragon Queue."""
        msg = Message(
            task_id="task-1",
            sender_id="orchestrator",
            recipient_id="agent-a",
        )
        self.queue.put(msg)
        restored = self.queue.get(timeout=5)
        self.assertEqual(restored.task_id, "task-1")
        self.assertEqual(restored.sender_id, "orchestrator")
        self.assertEqual(restored.recipient_id, "agent-a")
        self.assertIsNone(restored.header)
        self.assertIsInstance(restored.id, str)

    def test_message_with_dispatch_header(self):
        """Message with DispatchHeader survives round-trip."""
        header = DispatchHeader(
            task="Analyze dataset",
            serialized_ddict="ddict-handle-abc123",
            upstream_agent_ids=["planner", "runner"],
        )
        msg = Message(
            task_id="task-2",
            sender_id="orchestrator",
            recipient_id="analyzer",
            header=header,
            recipient_serialized_queue="queue-handle-xyz",
        )
        self.queue.put(msg)
        restored = self.queue.get(timeout=5)

        self.assertEqual(restored.task_id, "task-2")
        self.assertEqual(restored.header.task, "Analyze dataset")
        self.assertEqual(restored.header.serialized_ddict, "ddict-handle-abc123")
        self.assertEqual(restored.header.upstream_agent_ids, ["planner", "runner"])
        self.assertEqual(
            restored.recipient_serialized_queue, "queue-handle-xyz"
        )

    def test_message_id_preserved(self):
        """Custom message id is not overwritten during serialization."""
        custom_id = str(uuid.uuid4())
        msg = Message(
            task_id="t", sender_id="s", recipient_id="r", id=custom_id,
        )
        self.queue.put(msg)
        restored = self.queue.get(timeout=5)
        self.assertEqual(restored.id, custom_id)


class TestDispatchHeaderQueueRoundTrip(TestCase):
    """Verify DispatchHeader survives Dragon Queue serialization."""

    def setUp(self):
        self.queue = Queue()

    def tearDown(self):
        self.queue.destroy()

    def test_header_fields_preserved(self):
        """All DispatchHeader fields survive the round-trip."""
        header = DispatchHeader(
            task="Run simulation",
            serialized_ddict="ddict-handle-456",
            dispatch_id="dispatch-999",
            upstream_agent_ids=["planner", "runner"],
            tracing=True,
        )
        self.queue.put(header)
        restored = self.queue.get(timeout=5)

        self.assertEqual(restored.task, "Run simulation")
        self.assertEqual(restored.serialized_ddict, "ddict-handle-456")
        self.assertEqual(restored.dispatch_id, "dispatch-999")
        self.assertEqual(restored.upstream_agent_ids, ["planner", "runner"])
        self.assertTrue(restored.tracing)

    def test_message_wrapping_dispatch_header(self):
        """Message.header containing a DispatchHeader dict round-trips."""
        header = DispatchHeader(
            task="Summarize results",
            serialized_ddict="ddict-handle-789",
        )
        msg = Message(
            task_id="t3",
            sender_id="dispatcher",
            recipient_id="summarizer",
            header=vars(header),
        )
        self.queue.put(msg)
        restored = self.queue.get(timeout=5)

        self.assertEqual(restored.header["task"], "Summarize results")
        self.assertEqual(
            restored.header["serialized_ddict"], "ddict-handle-789"
        )


class TestMultipleMessagesOrdering(TestCase):
    """Verify FIFO ordering when multiple messages pass through a Queue."""

    def setUp(self):
        self.queue = Queue()

    def tearDown(self):
        self.queue.destroy()

    def test_fifo_ordering(self):
        """Messages come back in the order they were put."""
        ids = [str(uuid.uuid4()) for _ in range(10)]
        for msg_id in ids:
            self.queue.put(Message(
                task_id="t", sender_id="s", recipient_id="r", id=msg_id,
            ))
        restored_ids = []
        for _ in range(10):
            m = self.queue.get(timeout=5)
            restored_ids.append(m.id)
        self.assertEqual(restored_ids, ids)


if __name__ == "__main__":
    mp.set_start_method("dragon")
    main()
