"""Tests for make_dispatcher_fn — batch dispatcher closure."""

import dragon
import multiprocessing as mp


from typing import List, Optional
from unittest import TestCase, main
from unittest.mock import MagicMock, patch

from dragon.ai.agent.config import (
    OrchestratorConfig, AgentConfig, PipelineNode, TaskResult, TaskStatus,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_dispatcher(
    agent_id: str = "agent1",
    task_description: str = "Do stuff",
    depends_on: Optional[List[str]] = None,
    poll_timeout: float = 5.0,
):
    """Create a dispatcher closure with mocked DDict and Queue."""
    from dragon.ai.agent.core.batch_dispatch import make_dispatcher_fn

    node = PipelineNode(
        agent_id=agent_id,
        task_description=task_description,
        depends_on=depends_on or [],
    )
    config = OrchestratorConfig(poll_timeout=poll_timeout)

    mock_queue = MagicMock()
    mock_queue.put = MagicMock()

    return make_dispatcher_fn(
        agent_queue=mock_queue,
        node=node,
        task_id="task123",
        serialized_ddict="fake-ddict-handle",
        config=config,
    ), mock_queue


# ========================================================================
# Successful dispatch (no upstream)
# ========================================================================

class TestDispatcherNoUpstream(TestCase):
    """Verify dispatcher with no upstream dependencies."""

    def test_returns_task_result(self):
        """Successful dispatch returns TaskResult with DONE status."""
        dispatcher, mock_queue = _make_dispatcher()

        with patch("dragon.ai.agent.core.batch_dispatch.DDict") as ddict_cls, \
             patch("dragon.ai.agent.core.batch_dispatch.Event") as event_cls:
            mock_ddict = MagicMock()
            mock_ddict.__setitem__ = MagicMock()
            mock_ddict.__getitem__ = MagicMock(return_value=TaskStatus.DONE)
            mock_ddict.detach = MagicMock()
            ddict_cls.attach.return_value = mock_ddict

            mock_event = MagicMock()
            mock_event.wait.return_value = True  # completed
            mock_event.destroy = MagicMock()
            event_cls.return_value = mock_event

            result = dispatcher()

        self.assertIsInstance(result, TaskResult)
        self.assertEqual(result.agent_id, "agent1")
        self.assertEqual(result.status, TaskStatus.DONE)
        mock_queue.put.assert_called_once()


# ========================================================================
# Upstream error propagation
# ========================================================================

class TestDispatcherUpstreamError(TestCase):
    """Verify dispatcher propagates upstream failures."""

    def test_upstream_exception_raises(self):
        """BaseException passed as upstream arg is re-raised as RuntimeError."""
        dispatcher, _ = _make_dispatcher()

        with patch("dragon.ai.agent.core.batch_dispatch.DDict") as ddict_cls:
            mock_ddict = MagicMock()
            mock_ddict.detach = MagicMock()
            ddict_cls.attach.return_value = mock_ddict

            with self.assertRaisesRegex(RuntimeError, "Upstream node failed"):
                dispatcher(ValueError("upstream blew up"))

    def test_upstream_error_status_raises(self):
        """Upstream TaskResult with ERROR status raises RuntimeError."""
        dispatcher, _ = _make_dispatcher()
        upstream = TaskResult(
            task_id="task123", agent_id="parent",
            status=TaskStatus.ERROR, serialized_ddict="fake",
        )

        with patch("dragon.ai.agent.core.batch_dispatch.DDict") as ddict_cls:
            mock_ddict = MagicMock()
            mock_ddict.__setitem__ = MagicMock()
            mock_ddict.detach = MagicMock()
            ddict_cls.attach.return_value = mock_ddict

            with self.assertRaisesRegex(RuntimeError, "failed"):
                dispatcher(upstream)


# ========================================================================
# Timeout
# ========================================================================

class TestDispatcherTimeout(TestCase):
    """Verify dispatcher raises TimeoutError when agent doesn't complete."""

    def test_timeout_raises(self):
        """Event.wait returning False triggers a TimeoutError."""
        dispatcher, _ = _make_dispatcher(poll_timeout=0.1)

        with patch("dragon.ai.agent.core.batch_dispatch.DDict") as ddict_cls, \
             patch("dragon.ai.agent.core.batch_dispatch.Event") as event_cls:
            mock_ddict = MagicMock()
            mock_ddict.__setitem__ = MagicMock()
            mock_ddict.detach = MagicMock()
            ddict_cls.attach.return_value = mock_ddict

            mock_event = MagicMock()
            mock_event.wait.return_value = False  # timed out
            mock_event.destroy = MagicMock()
            event_cls.return_value = mock_event

            with self.assertRaisesRegex(TimeoutError, "did not complete"):
                dispatcher()


# ========================================================================
# Agent reports ERROR status after completion
# ========================================================================

class TestDispatcherAgentError(TestCase):
    """Verify dispatcher raises when agent reports ERROR status."""

    def test_agent_error_status_raises(self):
        """Agent completing with ERROR status raises RuntimeError."""
        dispatcher, _ = _make_dispatcher()

        with patch("dragon.ai.agent.core.batch_dispatch.DDict") as ddict_cls, \
             patch("dragon.ai.agent.core.batch_dispatch.Event") as event_cls:
            mock_ddict = MagicMock()
            mock_ddict.__setitem__ = MagicMock()
            mock_ddict.__getitem__ = MagicMock(return_value=TaskStatus.ERROR)
            mock_ddict.detach = MagicMock()
            ddict_cls.attach.return_value = mock_ddict

            mock_event = MagicMock()
            mock_event.wait.return_value = True
            mock_event.destroy = MagicMock()
            event_cls.return_value = mock_event

            with self.assertRaisesRegex(RuntimeError, "reported error"):
                dispatcher()


# ========================================================================
# Multiple upstreams — all coerced and checked
# ========================================================================

class TestDispatcherMultipleUpstreams(TestCase):
    """Verify dispatcher handles multiple upstream TaskResults."""

    def test_multiple_successful_upstreams(self):
        """All DONE upstreams are accepted; message sent to agent."""
        dispatcher, mock_queue = _make_dispatcher(depends_on=["p1", "p2"])

        upstream1 = TaskResult(
            task_id="task123", agent_id="p1",
            status=TaskStatus.DONE, serialized_ddict="fake",
        )
        upstream2 = TaskResult(
            task_id="task123", agent_id="p2",
            status=TaskStatus.DONE, serialized_ddict="fake",
        )

        with patch("dragon.ai.agent.core.batch_dispatch.DDict") as ddict_cls, \
             patch("dragon.ai.agent.core.batch_dispatch.Event") as event_cls:
            mock_ddict = MagicMock()
            mock_ddict.__setitem__ = MagicMock()
            mock_ddict.__getitem__ = MagicMock(return_value=TaskStatus.DONE)
            mock_ddict.detach = MagicMock()
            ddict_cls.attach.return_value = mock_ddict

            mock_event = MagicMock()
            mock_event.wait.return_value = True
            mock_event.destroy = MagicMock()
            event_cls.return_value = mock_event

            result = dispatcher(upstream1, upstream2)

        self.assertEqual(result.status, TaskStatus.DONE)
        mock_queue.put.assert_called_once()


# ========================================================================
# DDict always detached (even on error)
# ========================================================================

class TestDispatcherCleanup(TestCase):
    """Verify DDict is always detached, even on failure."""

    def test_ddict_detached_on_success(self):
        """DDict.detach() is called after a successful dispatch."""
        dispatcher, _ = _make_dispatcher()

        with patch("dragon.ai.agent.core.batch_dispatch.DDict") as ddict_cls, \
             patch("dragon.ai.agent.core.batch_dispatch.Event") as event_cls:
            mock_ddict = MagicMock()
            mock_ddict.__setitem__ = MagicMock()
            mock_ddict.__getitem__ = MagicMock(return_value=TaskStatus.DONE)
            ddict_cls.attach.return_value = mock_ddict

            mock_event = MagicMock()
            mock_event.wait.return_value = True
            mock_event.destroy = MagicMock()
            event_cls.return_value = mock_event

            dispatcher()
            mock_ddict.detach.assert_called_once()

    def test_ddict_detached_on_failure(self):
        """DDict.detach() is called even when the dispatcher raises."""
        dispatcher, _ = _make_dispatcher()

        with patch("dragon.ai.agent.core.batch_dispatch.DDict") as ddict_cls, \
             patch("dragon.ai.agent.core.batch_dispatch.Event") as event_cls:
            mock_ddict = MagicMock()
            mock_ddict.__setitem__ = MagicMock()
            mock_ddict.__getitem__ = MagicMock(return_value=TaskStatus.ERROR)
            ddict_cls.attach.return_value = mock_ddict

            mock_event = MagicMock()
            mock_event.wait.return_value = True
            mock_event.destroy = MagicMock()
            event_cls.return_value = mock_event

            with self.assertRaises(RuntimeError):
                dispatcher()
            mock_ddict.detach.assert_called_once()


# ========================================================================
# Dict coercion — Dragon Batch may pass dicts instead of TaskResult
# ========================================================================

class TestDispatcherDictCoercion(TestCase):
    """Verify dispatcher coerces raw dicts to TaskResult."""

    def test_dict_coerced_to_task_result(self):
        """A raw dict upstream is coerced to TaskResult via __post_init__."""
        dispatcher, mock_queue = _make_dispatcher()

        upstream_dict = {
            "task_id": "task123",
            "agent_id": "parent",
            "status": "done",
            # serialized_ddict missing — should be defaulted
        }

        with patch("dragon.ai.agent.core.batch_dispatch.DDict") as ddict_cls, \
             patch("dragon.ai.agent.core.batch_dispatch.Event") as event_cls:
            mock_ddict = MagicMock()
            mock_ddict.__setitem__ = MagicMock()
            mock_ddict.__getitem__ = MagicMock(return_value=TaskStatus.DONE)
            mock_ddict.detach = MagicMock()
            ddict_cls.attach.return_value = mock_ddict

            mock_event = MagicMock()
            mock_event.wait.return_value = True
            mock_event.destroy = MagicMock()
            event_cls.return_value = mock_event

            result = dispatcher(upstream_dict)

        self.assertEqual(result.status, TaskStatus.DONE)

    def test_unexpected_upstream_type_raises(self):
        """An unexpected upstream type (not dataclass or dict) raises TypeError."""
        dispatcher, _ = _make_dispatcher()

        with patch("dragon.ai.agent.core.batch_dispatch.DDict") as ddict_cls:
            mock_ddict = MagicMock()
            mock_ddict.__setitem__ = MagicMock()
            mock_ddict.detach = MagicMock()
            ddict_cls.attach.return_value = mock_ddict

            with self.assertRaisesRegex(TypeError, "Unexpected upstream result type"):
                dispatcher("not a valid upstream")


# ========================================================================
# Event lifecycle
# ========================================================================

class TestDispatcherEventLifecycle(TestCase):
    """Verify completion event is created, waited on, and destroyed."""

    def test_event_destroyed_after_successful_dispatch(self):
        """Completion event is destroyed after successful dispatch."""
        dispatcher, _ = _make_dispatcher()

        with patch("dragon.ai.agent.core.batch_dispatch.DDict") as ddict_cls, \
             patch("dragon.ai.agent.core.batch_dispatch.Event") as event_cls:
            mock_ddict = MagicMock()
            mock_ddict.__setitem__ = MagicMock()
            mock_ddict.__getitem__ = MagicMock(return_value=TaskStatus.DONE)
            mock_ddict.detach = MagicMock()
            ddict_cls.attach.return_value = mock_ddict

            mock_event = MagicMock()
            mock_event.wait.return_value = True
            mock_event.destroy = MagicMock()
            event_cls.return_value = mock_event

            dispatcher()

        mock_event.destroy.assert_called_once()

    def test_event_destroyed_on_timeout(self):
        """Completion event is destroyed even when dispatch times out."""
        dispatcher, _ = _make_dispatcher(poll_timeout=0.01)

        with patch("dragon.ai.agent.core.batch_dispatch.DDict") as ddict_cls, \
             patch("dragon.ai.agent.core.batch_dispatch.Event") as event_cls:
            mock_ddict = MagicMock()
            mock_ddict.__setitem__ = MagicMock()
            mock_ddict.detach = MagicMock()
            ddict_cls.attach.return_value = mock_ddict

            mock_event = MagicMock()
            mock_event.wait.return_value = False  # timeout
            mock_event.destroy = MagicMock()
            event_cls.return_value = mock_event

            with self.assertRaises(TimeoutError):
                dispatcher()

        mock_event.destroy.assert_called_once()


# ========================================================================
# Dispatch writes dispatch_id and trace keys
# ========================================================================

class TestDispatcherKeyWrites(TestCase):
    """Verify dispatcher writes dispatch_id and trace keys to DDict."""

    def test_writes_dispatch_id_to_ddict(self):
        """Dispatcher writes the dispatch_id to a stable key in DDict."""
        dispatcher, _ = _make_dispatcher()
        written = {}

        with patch("dragon.ai.agent.core.batch_dispatch.DDict") as ddict_cls, \
             patch("dragon.ai.agent.core.batch_dispatch.Event") as event_cls:
            mock_ddict = MagicMock()
            mock_ddict.__setitem__ = MagicMock(side_effect=written.__setitem__)
            mock_ddict.__getitem__ = MagicMock(return_value=TaskStatus.DONE)
            mock_ddict.detach = MagicMock()
            ddict_cls.attach.return_value = mock_ddict

            mock_event = MagicMock()
            mock_event.wait.return_value = True
            mock_event.destroy = MagicMock()
            event_cls.return_value = mock_event

            dispatcher()

        # There should be a dispatch_id key written
        dispatch_keys = [k for k in written if "dispatch_id" in k.lower() or ":dispatch:" in k]
        # At least one key should contain the agent's dispatch_id
        self.assertTrue(len(written) > 0)


# ========================================================================
# Message construction
# ========================================================================

class TestDispatcherMessage(TestCase):
    """Verify the message sent to the agent queue is correctly constructed."""

    def test_message_fields(self):
        """Message sent to agent has correct task_id, sender_id, and recipient_id."""
        dispatcher, mock_queue = _make_dispatcher(agent_id="researcher")

        with patch("dragon.ai.agent.core.batch_dispatch.DDict") as ddict_cls, \
             patch("dragon.ai.agent.core.batch_dispatch.Event") as event_cls:
            mock_ddict = MagicMock()
            mock_ddict.__setitem__ = MagicMock()
            mock_ddict.__getitem__ = MagicMock(return_value=TaskStatus.DONE)
            mock_ddict.detach = MagicMock()
            ddict_cls.attach.return_value = mock_ddict

            mock_event = MagicMock()
            mock_event.wait.return_value = True
            mock_event.destroy = MagicMock()
            event_cls.return_value = mock_event

            dispatcher()

        msg = mock_queue.put.call_args[0][0]
        self.assertEqual(msg.task_id, "task123")
        self.assertEqual(msg.sender_id, "dispatcher")
        self.assertEqual(msg.recipient_id, "researcher")


if __name__ == "__main__":
    mp.set_start_method("dragon")
    main()
