"""Tests for DragonAgentNode — LangGraph node callable that dispatches to Dragon."""

import dragon
import multiprocessing as mp

import asyncio
import concurrent.futures
from unittest import TestCase, IsolatedAsyncioTestCase, main
from unittest.mock import MagicMock, patch, AsyncMock

import cloudpickle

from dragon.ai.langgraph.node import DragonAgentNode


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_mock_watcher() -> MagicMock:
    """Create a mock DragonWatcher with submit_task and submit_task_async."""
    watcher = MagicMock()
    return watcher


def _make_node(
    node_name: str = "test_node",
    event_timeout: float | None = None,
) -> tuple[DragonAgentNode, MagicMock]:
    """Create a DragonAgentNode with a mock watcher."""
    watcher = _make_mock_watcher()
    serialized_input_queue = b"mock_queue_bytes"
    node = DragonAgentNode(
        node_name=node_name,
        serialized_input_queue=serialized_input_queue,
        watcher=watcher,
        event_timeout=event_timeout,
    )
    return node, watcher


# ========================================================================
# DragonAgentNode Construction
# ========================================================================

class TestDragonAgentNodeConstruction(TestCase):
    """Verify DragonAgentNode construction and attribute storage."""

    def test_stores_node_name(self):
        """Node stores the provided node_name."""
        node, _ = _make_node(node_name="researcher")
        self.assertEqual(node.node_name, "researcher")

    def test_stores_input_queue_bytes(self):
        """Node stores the serialized input queue handle."""
        node, _ = _make_node()
        self.assertEqual(node._serialized_input_queue, b"mock_queue_bytes")

    def test_stores_watcher(self):
        """Node stores the provided watcher reference."""
        node, watcher = _make_node()
        self.assertIs(node._watcher, watcher)

    def test_default_event_timeout_none(self):
        """Default event_timeout is None (no limit)."""
        node, _ = _make_node()
        self.assertIsNone(node._event_timeout)

    def test_custom_event_timeout(self):
        """Custom event_timeout is stored."""
        node, _ = _make_node(event_timeout=30.0)
        self.assertEqual(node._event_timeout, 30.0)


# ========================================================================
# DragonAgentNode.__call__ (sync path)
# ========================================================================

class TestDragonAgentNodeCall(TestCase):
    """Verify DragonAgentNode.__call__ dispatches via watcher and blocks on result."""

    def test_calls_watcher_submit_task(self):
        """__call__ invokes watcher.submit_task with correct arguments."""
        node, watcher = _make_node(node_name="writer")
        future = concurrent.futures.Future()
        future.set_result({"output": "done"})
        watcher.submit_task.return_value = future

        state = {"messages": ["hello"]}
        result = node(state)

        watcher.submit_task.assert_called_once()
        call_kwargs = watcher.submit_task.call_args.kwargs
        self.assertEqual(call_kwargs["node_name"], "writer")
        self.assertEqual(call_kwargs["state"], state)
        self.assertEqual(call_kwargs["serialized_input_queue"], b"mock_queue_bytes")
        self.assertEqual(result, {"output": "done"})

    def test_passes_timeout_to_future_result(self):
        """__call__ applies event_timeout when waiting on the Future."""
        node, watcher = _make_node(event_timeout=15.0)
        future = MagicMock()
        future.result.return_value = {}
        watcher.submit_task.return_value = future

        node({})

        future.result.assert_called_once_with(timeout=15.0)

    def test_returns_future_result(self):
        """__call__ returns the result from the resolved Future."""
        node, watcher = _make_node()
        expected_result = {"messages": ["output"], "data": 42}
        future = concurrent.futures.Future()
        future.set_result(expected_result)
        watcher.submit_task.return_value = future

        result = node({"messages": ["input"]})

        self.assertEqual(result, expected_result)

    def test_propagates_future_exception(self):
        """__call__ raises if the Future contains an exception."""
        node, watcher = _make_node()
        future = concurrent.futures.Future()
        future.set_exception(ValueError("agent failed"))
        watcher.submit_task.return_value = future

        with self.assertRaisesRegex(ValueError, "agent failed"):
            node({})


# ========================================================================
# DragonAgentNode.acall (async path)
# ========================================================================

class TestDragonAgentNodeAcall(IsolatedAsyncioTestCase):
    """Verify DragonAgentNode.acall dispatches via watcher asynchronously."""

    async def test_calls_watcher_submit_task_async(self):
        """acall invokes watcher.submit_task_async with correct arguments."""
        node, watcher = _make_node(node_name="analyzer")

        loop = asyncio.get_running_loop()
        async_future = loop.create_future()
        async_future.set_result({"result": "analyzed"})
        watcher.submit_task_async = AsyncMock(return_value=async_future)

        state = {"messages": ["analyze this"]}
        result = await node.acall(state)

        watcher.submit_task_async.assert_called_once()
        call_kwargs = watcher.submit_task_async.call_args.kwargs
        self.assertEqual(call_kwargs["node_name"], "analyzer")
        self.assertEqual(call_kwargs["state"], state)
        self.assertEqual(result, {"result": "analyzed"})

    async def test_returns_awaited_result(self):
        """acall returns the result from the resolved asyncio.Future."""
        node, watcher = _make_node()

        loop = asyncio.get_running_loop()
        async_future = loop.create_future()
        expected = {"messages": ["output"], "answer": "42"}
        async_future.set_result(expected)
        watcher.submit_task_async = AsyncMock(return_value=async_future)

        result = await node.acall({})

        self.assertEqual(result, expected)

    async def test_propagates_async_exception(self):
        """acall raises if the asyncio.Future contains an exception."""
        node, watcher = _make_node()

        loop = asyncio.get_running_loop()
        async_future = loop.create_future()
        async_future.set_exception(RuntimeError("async failure"))
        watcher.submit_task_async = AsyncMock(return_value=async_future)

        with self.assertRaisesRegex(RuntimeError, "async failure"):
            await node.acall({})

    async def test_respects_event_timeout(self):
        """acall uses asyncio.wait_for with event_timeout."""
        node, watcher = _make_node(event_timeout=0.01)

        loop = asyncio.get_running_loop()
        # Create a future that never resolves
        async_future = loop.create_future()
        watcher.submit_task_async = AsyncMock(return_value=async_future)

        with self.assertRaises(asyncio.TimeoutError):
            await node.acall({})

    async def test_no_timeout_when_none(self):
        """acall awaits directly without timeout when event_timeout is None."""
        node, watcher = _make_node(event_timeout=None)

        loop = asyncio.get_running_loop()
        async_future = loop.create_future()
        async_future.set_result({"ok": True})
        watcher.submit_task_async = AsyncMock(return_value=async_future)

        result = await node.acall({})
        self.assertEqual(result, {"ok": True})


# ========================================================================
# DragonAgentNode with various state types
# ========================================================================

class TestDragonAgentNodeStateTypes(TestCase):
    """Verify DragonAgentNode handles various state types correctly."""

    def test_dict_state(self):
        """Node handles a plain dict state."""
        node, watcher = _make_node()
        future = concurrent.futures.Future()
        future.set_result({"processed": True})
        watcher.submit_task.return_value = future

        state = {"key": "value", "number": 42}
        result = node(state)

        passed_state = watcher.submit_task.call_args.kwargs["state"]
        self.assertEqual(passed_state, state)

    def test_typed_dict_state(self):
        """Node handles a TypedDict-like state."""
        node, watcher = _make_node()
        future = concurrent.futures.Future()
        future.set_result({})
        watcher.submit_task.return_value = future

        state = {"messages": ["hello"], "research": "", "report": ""}
        node(state)

        passed_state = watcher.submit_task.call_args.kwargs["state"]
        self.assertIn("messages", passed_state)

    def test_empty_state(self):
        """Node handles an empty state dict."""
        node, watcher = _make_node()
        future = concurrent.futures.Future()
        future.set_result({"default": "result"})
        watcher.submit_task.return_value = future

        result = node({})

        self.assertEqual(result, {"default": "result"})


if __name__ == "__main__":
    mp.set_start_method("dragon")
    main()
