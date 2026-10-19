"""Tests for agent_host_entry — Dragon AgentHost process entry point."""

import dragon
import asyncio
import multiprocessing as mp
import queue as _queue

from unittest import TestCase, IsolatedAsyncioTestCase, main
from unittest.mock import MagicMock, patch, call

from dragon.ai.langgraph.constants import (
    ENV_STATUS, ENV_PAYLOAD, STATUS_DONE, STATUS_ERROR,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _simple_agent(state: dict) -> dict:
    """Simple test agent that echoes input with a prefix."""
    msg = state.get("messages", [""])[0]
    return {"messages": [f"processed: {msg}"]}


def _failing_agent(state: dict) -> dict:
    """Test agent that always raises an exception."""
    raise ValueError("intentional failure")


def _slow_agent(state: dict) -> dict:
    """Test agent that simulates slow processing."""
    import time
    time.sleep(0.1)
    return {"messages": ["slow result"]}


def _task_msg(node_name: str, state: dict | None = None, task_id: str = "task") -> dict:
    """Build a task message for the host input queue.

    ``serialized_done_queue`` is an opaque queue descriptor; tests that run the
    host patch ``_host.Queue`` so ``attach`` never touches the runtime.
    """
    return {
        "task_id": task_id,
        "node_name": node_name,
        "state": state or {"messages": ["input"]},
        "serialized_done_queue": b"sdesc",
    }


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
    """Flatten an envelope into one dict by running the real codec over it.

    Serialization happens inside ``EnvelopePickler.dump``, so tests round-trip
    through it to see exactly what would land on the wire — including the error
    it substitutes for a body that will not pickle.  The ``task_id`` travels
    beside the pickled body rather than inside it, so it is merged back in for
    convenient assertions.
    """
    from dragon.ai.langgraph.constants import EnvelopePickler

    pickler = EnvelopePickler()
    out = _FakeFile()
    pickler.dump(envelope, out)
    task_id, body, exc = pickler.load(_FakeFile(b"".join(out.writes)))
    if exc is not None:
        raise exc
    return {"task_id": task_id, **body}


def _run_tasks(agents: dict, msgs: list, *, max_threads=None) -> list:
    """Drive agent_host_entry through the given task messages + shutdown.

    The host runs its real async loop (with real thread pools); only the Dragon
    Queues are mocked. Returns the list of decoded completion envelopes in send
    order.
    """
    from dragon.ai.langgraph.host import agent_host_entry

    mock_reply = MagicMock()
    mock_input = MagicMock()
    mock_input.get.side_effect = [*msgs, None]  # tasks, then shutdown sentinel

    sent: list = []
    with patch("dragon.ai.langgraph.host.Queue") as MockQueue, \
         patch(
             "dragon.ai.langgraph.host._send_envelope",
             side_effect=lambda done_q, envelope: sent.append(
                 _decode_envelope(envelope)
             ),
         ):
        MockQueue.return_value = mock_input
        agent_host_entry(agents=agents, reply_queue=mock_reply, max_threads=max_threads)
    return sent


# ========================================================================
# _build_done_envelope
# ========================================================================

class TestBuildDoneEnvelope(TestCase):
    """Verify _build_done_envelope creates correct success envelopes."""

    def test_envelope_structure(self):
        """Envelope contains task_id, status=done, and payload."""
        from dragon.ai.langgraph.host import _build_done_envelope

        result = {"messages": ["hello"]}
        data = _build_done_envelope("task_123", result)
        envelope = _decode_envelope(data)

        self.assertEqual(envelope["task_id"], "task_123")
        self.assertEqual(envelope[ENV_STATUS], STATUS_DONE)
        self.assertEqual(envelope[ENV_PAYLOAD], result)

    def test_serializes_complex_result(self):
        """Envelope correctly serializes complex result dicts."""
        from dragon.ai.langgraph.host import _build_done_envelope

        result = {
            "messages": ["msg1", "msg2"],
            "data": {"nested": [1, 2, 3]},
            "count": 42,
        }
        data = _build_done_envelope("task_456", result)
        envelope = _decode_envelope(data)

        self.assertEqual(envelope[ENV_PAYLOAD], result)


# ========================================================================
# _build_error_envelope
# ========================================================================

class TestBuildErrorEnvelope(TestCase):
    """Verify _build_error_envelope creates correct error envelopes."""

    def test_envelope_structure(self):
        """Error envelope contains task_id, status=error, and exception."""
        from dragon.ai.langgraph.host import _build_error_envelope

        exc = ValueError("test error")
        data = _build_error_envelope("task_789", exc)
        envelope = _decode_envelope(data)

        self.assertEqual(envelope["task_id"], "task_789")
        self.assertEqual(envelope[ENV_STATUS], STATUS_ERROR)
        self.assertIsInstance(envelope[ENV_PAYLOAD], ValueError)
        self.assertIn("test error", str(envelope[ENV_PAYLOAD]))

    def test_fallback_for_unpicklable_exception(self):
        """An exception the coordinator could never unpickle is replaced by a
        RuntimeError on the way out, so the Future still fails with a reason."""
        from dragon.ai.langgraph.host import _build_error_envelope

        # Create an exception that can't be pickled
        class UnpicklableError(Exception):
            def __reduce__(self):
                raise TypeError("Cannot pickle")

        exc = UnpicklableError("unpicklable")
        data = _build_error_envelope("task_abc", exc)
        envelope = _decode_envelope(data)

        self.assertEqual(envelope["task_id"], "task_abc")
        self.assertEqual(envelope[ENV_STATUS], STATUS_ERROR)
        self.assertIsInstance(envelope[ENV_PAYLOAD], RuntimeError)


# ========================================================================
# Unserializable results
# ========================================================================

class _UnpicklableResult:
    """A node return value that cloudpickle cannot serialize."""

    def __reduce__(self):
        raise TypeError("this result cannot be pickled")


class TestUnserializableResult(TestCase):
    """A result that cannot be serialized must still reach the coordinator.

    ``EnvelopePickler.dump`` substitutes an error envelope rather than raising,
    so the failure arrives framed under its own ``task_id`` and fails that
    Future instead of leaving it to hang until ``event_timeout``.
    """

    def test_unserializable_result_becomes_an_error_envelope(self):
        """The task fails with a picklable error naming the real problem."""
        sent = _run_tasks({"a": lambda state: _UnpicklableResult()}, [_task_msg("a")])

        self.assertEqual(len(sent), 1)
        self.assertEqual(sent[0]["task_id"], "task")
        self.assertEqual(sent[0][ENV_STATUS], STATUS_ERROR)
        self.assertIn("pickle", str(sent[0][ENV_PAYLOAD]).lower())

    def test_one_bad_result_does_not_block_the_next_task(self):
        """The host keeps serving after a serialization failure."""
        agents = {
            "bad": lambda state: _UnpicklableResult(),
            "good": lambda state: {"messages": ["fine"]},
        }
        sent = _run_tasks(
            agents,
            [_task_msg("bad", task_id="t1"), _task_msg("good", task_id="t2")],
        )

        by_task = {env["task_id"]: env for env in sent}
        self.assertEqual(by_task["t1"][ENV_STATUS], STATUS_ERROR)
        self.assertEqual(by_task["t2"][ENV_STATUS], STATUS_DONE)
        self.assertEqual(by_task["t2"][ENV_PAYLOAD]["messages"], ["fine"])


# ========================================================================
# Node execution — sync, async, failing, and awaitable results
# ========================================================================

class TestNodeExecution(TestCase):
    """Verify the host runs each node and sends exactly one result envelope."""

    def test_sync_node_sends_done_envelope(self):
        """A plain def node runs (on the thread pool) and sends a DONE envelope."""
        sent = _run_tasks({"a": _simple_agent}, [_task_msg("a")])
        self.assertEqual(len(sent), 1)
        self.assertEqual(sent[0][ENV_STATUS], STATUS_DONE)
        self.assertIn("processed:", sent[0][ENV_PAYLOAD]["messages"][0])

    def test_failing_node_sends_error_envelope(self):
        """A node that raises sends an ERROR envelope carrying the exception."""
        sent = _run_tasks({"boom": _failing_agent}, [_task_msg("boom")])
        self.assertEqual(len(sent), 1)
        self.assertEqual(sent[0][ENV_STATUS], STATUS_ERROR)
        self.assertIsInstance(sent[0][ENV_PAYLOAD], ValueError)

    def test_async_node_sends_done_envelope(self):
        """An async def node is awaited on the loop and sends a DONE envelope."""
        async def _async_agent(state):
            return {"messages": ["processed: async"]}

        sent = _run_tasks({"a": _async_agent}, [_task_msg("a")])
        self.assertEqual(len(sent), 1)
        self.assertEqual(sent[0][ENV_STATUS], STATUS_DONE)
        self.assertEqual(sent[0][ENV_PAYLOAD]["messages"][0], "processed: async")

    def test_non_coroutine_awaitable_sends_error_envelope(self):
        """A non-coroutine awaitable can't be driven and becomes an ERROR rather
        than being pickled back as a fake result."""
        class _Awaitable:
            def __await__(self):
                yield
                return None

        def _agent(state):
            return _Awaitable()

        sent = _run_tasks({"weird": _agent}, [_task_msg("weird")])
        self.assertEqual(len(sent), 1)
        self.assertEqual(sent[0][ENV_STATUS], STATUS_ERROR)
        self.assertIsInstance(sent[0][ENV_PAYLOAD], TypeError)

    def test_unknown_node_sends_error_envelope(self):
        """An unrouteable node_name sends an ERROR envelope, never hangs."""
        sent = _run_tasks({"known": _simple_agent}, [_task_msg("nonexistent")])
        self.assertEqual(len(sent), 1)
        self.assertEqual(sent[0][ENV_STATUS], STATUS_ERROR)
        self.assertIsInstance(sent[0][ENV_PAYLOAD], ValueError)

    def test_one_failure_does_not_affect_peers(self):
        """A failing task and a succeeding task both get their own envelope."""
        sent = _run_tasks(
            {"boom": _failing_agent, "a": _simple_agent},
            [_task_msg("boom", task_id="t1"), _task_msg("a", task_id="t2")],
        )
        by_id = {e["task_id"]: e for e in sent}
        self.assertEqual(by_id["t1"][ENV_STATUS], STATUS_ERROR)
        self.assertEqual(by_id["t2"][ENV_STATUS], STATUS_DONE)

    def test_sync_callable_with_async_call_is_awaited(self):
        """A callable whose ``__call__`` is async is classified sync (offloaded to
        a thread), returns a coroutine, and that coroutine is then awaited."""
        class _AsyncCallable:
            async def __call__(self, state):
                return {"messages": ["async __call__ ok"]}

        sent = _run_tasks({"a": _AsyncCallable()}, [_task_msg("a")])
        self.assertEqual(len(sent), 1)
        self.assertEqual(sent[0][ENV_STATUS], STATUS_DONE)
        self.assertEqual(sent[0][ENV_PAYLOAD]["messages"][0], "async __call__ ok")

    def test_node_returns_non_dict_result_is_carried_inline(self):
        """The host carries whatever the node returns (not just dicts) inline."""
        def _list_agent(state):
            return [1, 2, 3]

        sent = _run_tasks({"a": _list_agent}, [_task_msg("a")])
        self.assertEqual(sent[0][ENV_STATUS], STATUS_DONE)
        self.assertEqual(sent[0][ENV_PAYLOAD], [1, 2, 3])


# ========================================================================
# Host error paths — broken reply queue, failed send
# ========================================================================

class TestHostErrorPaths(TestCase):
    """Verify the host degrades gracefully when transport itself fails."""

    def test_missing_reply_queue_logs_and_does_not_crash(self):
        """If the reply queue can't be reconstructed, no envelope is sent (the
        coordinator falls back to its timeout) and the host still exits cleanly."""
        from dragon.ai.langgraph.host import agent_host_entry

        bad_msg = {
            "task_id": "t",
            "node_name": "a",
            "state": {},
            "serialized_done_queue": b"not-a-valid-descriptor",
        }
        mock_reply = MagicMock()
        mock_input = MagicMock()
        mock_input.get.side_effect = [bad_msg, None]

        sent = []
        with patch("dragon.ai.langgraph.host.Queue") as MockQueue, \
             patch(
                 "dragon.ai.langgraph.host._send_envelope",
                 side_effect=lambda q, envelope: sent.append(envelope),
             ):
            MockQueue.attach.side_effect = RuntimeError("bad descriptor")
            MockQueue.return_value = mock_input
            # The coordinator's Future can now only unblock on its own timeout,
            # so this must be loud rather than a silent drop.
            with self.assertLogs("LANGGRAPH.agent_host", level="ERROR") as logs:
                agent_host_entry(agents={"a": _simple_agent}, reply_queue=mock_reply)

        message = "\n".join(logs.output)
        self.assertIn("no reply queue", message)
        self.assertIn("time out", message)
        self.assertEqual(sent, [])  # no reply queue → nothing to send
        mock_input.destroy.assert_called_once()  # still cleaned up

    def test_send_failure_is_logged_and_does_not_crash(self):
        """A completion-Queue put failure is swallowed; the host still exits."""
        from dragon.ai.langgraph.host import agent_host_entry

        mock_reply = MagicMock()
        mock_input = MagicMock()
        mock_input.get.side_effect = [_task_msg("a"), None]

        with patch("dragon.ai.langgraph.host.Queue") as MockQueue, \
             patch(
                 "dragon.ai.langgraph.host._send_envelope",
                 side_effect=RuntimeError("send boom"),
             ):
            MockQueue.return_value = mock_input
            # Must not raise despite every send failing — but a result that
            # never reached the coordinator is an error, not a shrug.
            with self.assertLogs("LANGGRAPH.agent_host", level="ERROR") as logs:
                agent_host_entry(agents={"a": _simple_agent}, reply_queue=mock_reply)

        message = "\n".join(logs.output)
        self.assertIn("send boom", message)
        self.assertIn("time out", message)
        mock_input.destroy.assert_called_once()


# ========================================================================
# _log_task_exception (done-callback defensive net)
# ========================================================================

class TestLogTaskException(IsolatedAsyncioTestCase):
    """Verify the done-callback logs escaped exceptions and ignores cancels."""

    async def test_logs_when_task_raised(self):
        from dragon.ai.langgraph.host import _log_task_exception

        async def _boom():
            raise ValueError("escaped")

        task = asyncio.ensure_future(_boom())
        with self.assertRaises(ValueError):
            await task
        # Retrieving/​logging the exception must not raise.
        _log_task_exception(task)

    async def test_ignores_cancelled_task(self):
        from dragon.ai.langgraph.host import _log_task_exception

        async def _sleep():
            await asyncio.sleep(10)

        task = asyncio.ensure_future(_sleep())
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            # Cancelled tasks are skipped — must not raise.
            _log_task_exception(task)


# ========================================================================
# Single shared event loop (async scaling + loop-bound resource reuse)
# ========================================================================

class TestSingleLoop(TestCase):
    """Verify async nodes run on one shared loop, not the sync thread pool."""

    def test_async_nodes_share_one_loop(self):
        """Two async node calls run on the SAME event loop — so a loop-bound
        resource created once can be reused across calls."""
        loop_ids: list = []

        async def _capture(state):
            loop_ids.append(id(asyncio.get_running_loop()))
            return {"messages": ["ok"]}

        _run_tasks(
            {"a": _capture},
            [_task_msg("a", task_id="t1"), _task_msg("a", task_id="t2")],
        )
        self.assertEqual(len(loop_ids), 2)
        self.assertEqual(loop_ids[0], loop_ids[1])

    def test_async_nodes_run_concurrently_despite_max_threads_1(self):
        """Async nodes run on the loop, not the sync pool — many run at once
        even when max_threads=1 (which would serialize sync nodes)."""
        running: list = []
        peak = [0]

        async def _agent(state):
            running.append(1)
            peak[0] = max(peak[0], len(running))
            await asyncio.sleep(0.2)
            running.pop()
            return {"messages": ["ok"]}

        sent = _run_tasks(
            {"a": _agent},
            [
                _task_msg("a", task_id="t1"),
                _task_msg("a", task_id="t2"),
                _task_msg("a", task_id="t3"),
            ],
            max_threads=1,
        )
        self.assertEqual(len(sent), 3)
        self.assertGreaterEqual(peak[0], 2)


# ========================================================================
# agent_host_entry — handshake
# ========================================================================

class TestAgentHostEntryHandshake(TestCase):
    """Verify agent_host_entry sends input queue handle and handles shutdown."""

    def test_sends_input_queue_to_reply_queue(self):
        """Host sends its serialized input queue handle on the reply queue."""
        from dragon.ai.langgraph.host import agent_host_entry

        mock_reply_queue = MagicMock()
        mock_input_queue = MagicMock()
        mock_input_queue.serialize.return_value = b"serialized_queue_handle"

        # Simulate shutdown sentinel immediately
        mock_input_queue.get.return_value = None

        with patch("dragon.ai.langgraph.host.Queue") as MockQueue:
            MockQueue.return_value = mock_input_queue
            agent_host_entry(
                agents={"test": _simple_agent},
                reply_queue=mock_reply_queue,
            )

        # Verify reply_queue.put was called with serialized input queue
        mock_reply_queue.put.assert_called_once()
        call_args = mock_reply_queue.put.call_args[0][0]
        self.assertEqual(call_args, b"serialized_queue_handle")

    def test_shutdown_on_none_sentinel(self):
        """Host exits cleanly when receiving None sentinel."""
        from dragon.ai.langgraph.host import agent_host_entry

        mock_reply_queue = MagicMock()
        mock_input_queue = MagicMock()
        mock_input_queue.get.return_value = None  # Immediate shutdown

        with patch("dragon.ai.langgraph.host.Queue") as MockQueue:
            MockQueue.return_value = mock_input_queue
            # Should not raise, should exit cleanly
            agent_host_entry(
                agents={"test": _simple_agent},
                reply_queue=mock_reply_queue,
            )


# ========================================================================
# agent_host_entry — task dispatch
# ========================================================================

class TestAgentHostEntryDispatch(TestCase):
    """Verify agent_host_entry dispatches tasks to correct agents."""

    def test_dispatches_to_correct_agent(self):
        """Task is dispatched to the agent matching node_name."""
        from dragon.ai.langgraph.host import agent_host_entry

        mock_reply_queue = MagicMock()
        mock_input_queue = MagicMock()

        task_msg = {
            "task_id": "task_x",
            "node_name": "agent_a",
            "state": {"messages": ["test"]},
            "serialized_done_queue": b"sdesc",
        }

        # Return task, then shutdown sentinel
        mock_input_queue.get.side_effect = [task_msg, None]

        results = []
        def agent_a(state):
            results.append(state)
            return {"messages": ["result_a"]}

        def agent_b(state):
            results.append("wrong_agent")
            return {"messages": ["result_b"]}

        with patch("dragon.ai.langgraph.host.Queue") as MockQueue, \
             patch("dragon.ai.langgraph.host._send_envelope"):
            MockQueue.return_value = mock_input_queue
            agent_host_entry(
                agents={"agent_a": agent_a, "agent_b": agent_b},
                reply_queue=mock_reply_queue,
            )

        # Verify only agent_a was called
        self.assertEqual(len(results), 1)
        self.assertIsInstance(results[0], dict)
        self.assertNotIn("wrong_agent", results)

    def test_unknown_node_sends_error_envelope(self):
        """Unknown node_name sends an error envelope back to the caller."""
        from dragon.ai.langgraph.host import agent_host_entry

        mock_reply_queue = MagicMock()
        mock_input_queue = MagicMock()

        task_msg = {
            "task_id": "task_unknown",
            "node_name": "nonexistent",
            "state": {},
            "serialized_done_queue": b"sdesc",
        }

        mock_input_queue.get.side_effect = [task_msg, None]

        sent = []
        with patch("dragon.ai.langgraph.host.Queue") as MockQueue, \
             patch(
                 "dragon.ai.langgraph.host._send_envelope",
                 side_effect=lambda q, envelope: sent.append(
                     _decode_envelope(envelope)
                 ),
             ):
            MockQueue.return_value = mock_input_queue
            agent_host_entry(
                agents={"known_agent": _simple_agent},
                reply_queue=mock_reply_queue,
            )

        self.assertEqual(len(sent), 1)
        self.assertEqual(sent[0]["task_id"], "task_unknown")
        self.assertEqual(sent[0][ENV_STATUS], STATUS_ERROR)
        self.assertIsInstance(sent[0][ENV_PAYLOAD], ValueError)


# ========================================================================
# agent_host_entry — max_threads
# ========================================================================

class TestAgentHostEntryMaxThreads(TestCase):
    """Verify the sync-node thread pool is sized by max_threads."""

    def test_default_max_threads(self):
        """Default sync-body pool size is max(len(agents) * 4, 8)."""
        from dragon.ai.langgraph.host import agent_host_entry
        from concurrent.futures import ThreadPoolExecutor as _RealTPE

        mock_reply_queue = MagicMock()
        mock_input_queue = MagicMock()
        mock_input_queue.get.return_value = None

        with patch("dragon.ai.langgraph.host.Queue") as MockQueue, \
             patch(
                 "dragon.ai.langgraph.host.ThreadPoolExecutor",
                 side_effect=_RealTPE,
             ) as MockPool:
            MockQueue.return_value = mock_input_queue
            agent_host_entry(
                agents={"a": _simple_agent, "b": _simple_agent},
                reply_queue=mock_reply_queue,
            )

        body_calls = [
            c for c in MockPool.call_args_list
            if c.kwargs.get("thread_name_prefix") == "agent-body"
        ]
        self.assertEqual(len(body_calls), 1)
        self.assertEqual(body_calls[0].kwargs["max_workers"], 8)

    def test_custom_max_threads(self):
        """Custom max_threads sizes the sync-body pool."""
        from dragon.ai.langgraph.host import agent_host_entry
        from concurrent.futures import ThreadPoolExecutor as _RealTPE

        mock_reply_queue = MagicMock()
        mock_input_queue = MagicMock()
        mock_input_queue.get.return_value = None

        with patch("dragon.ai.langgraph.host.Queue") as MockQueue, \
             patch(
                 "dragon.ai.langgraph.host.ThreadPoolExecutor",
                 side_effect=_RealTPE,
             ) as MockPool:
            MockQueue.return_value = mock_input_queue
            agent_host_entry(
                agents={"a": _simple_agent},
                reply_queue=mock_reply_queue,
                max_threads=16,
            )

        body_calls = [
            c for c in MockPool.call_args_list
            if c.kwargs.get("thread_name_prefix") == "agent-body"
        ]
        self.assertEqual(body_calls[0].kwargs["max_workers"], 16)

    def test_default_max_threads_scales_with_agent_count(self):
        """With more agents the default sync pool grows: len(agents) * 4."""
        from dragon.ai.langgraph.host import agent_host_entry
        from concurrent.futures import ThreadPoolExecutor as _RealTPE

        mock_reply_queue = MagicMock()
        mock_input_queue = MagicMock()
        mock_input_queue.get.return_value = None

        agents = {name: _simple_agent for name in ("a", "b", "c", "d", "e")}
        with patch("dragon.ai.langgraph.host.Queue") as MockQueue, \
             patch(
                 "dragon.ai.langgraph.host.ThreadPoolExecutor",
                 side_effect=_RealTPE,
             ) as MockPool:
            MockQueue.return_value = mock_input_queue
            agent_host_entry(agents=agents, reply_queue=mock_reply_queue)

        body_calls = [
            c for c in MockPool.call_args_list
            if c.kwargs.get("thread_name_prefix") == "agent-body"
        ]
        self.assertEqual(body_calls[0].kwargs["max_workers"], 20)  # 5 * 4


# ========================================================================
# agent_host_entry — resource cleanup
# ========================================================================

class TestAgentHostEntryCleanup(TestCase):
    """Verify the host destroys its input queue when the listen loop exits."""

    def test_input_queue_destroyed_on_clean_exit(self):
        """The host destroys its own input Queue after the shutdown sentinel."""
        from dragon.ai.langgraph.host import agent_host_entry

        mock_reply_queue = MagicMock()
        mock_input_queue = MagicMock()
        mock_input_queue.get.return_value = None  # immediate shutdown

        with patch("dragon.ai.langgraph.host.Queue") as MockQueue:
            MockQueue.return_value = mock_input_queue
            agent_host_entry(
                agents={"a": _simple_agent},
                reply_queue=mock_reply_queue,
            )

        mock_input_queue.destroy.assert_called_once()

    def test_input_queue_destroy_error_is_swallowed(self):
        """A failure destroying the input Queue must not crash host exit."""
        from dragon.ai.langgraph.host import agent_host_entry

        mock_reply_queue = MagicMock()
        mock_input_queue = MagicMock()
        mock_input_queue.get.return_value = None
        mock_input_queue.destroy.side_effect = RuntimeError("destroy boom")

        with patch("dragon.ai.langgraph.host.Queue") as MockQueue:
            MockQueue.return_value = mock_input_queue
            # Must not raise despite the destroy failure — but a Queue left
            # behind is a leaked Dragon resource, so it has to be reported.
            with self.assertLogs("LANGGRAPH.agent_host", level="WARNING") as logs:
                agent_host_entry(
                    agents={"a": _simple_agent},
                    reply_queue=mock_reply_queue,
                )

        self.assertIn("destroy boom", "\n".join(logs.output))


# ========================================================================
# agent_host_entry — task raised after dispatch still drains
# ========================================================================

class TestAgentHostEntryTaskFailureDrains(TestCase):
    """A task that raises inside the pool still lets the host shut down."""

    def test_failing_task_does_not_block_shutdown(self):
        """A raising agent is handled; the host still exits on the sentinel."""
        from dragon.ai.langgraph.host import agent_host_entry

        mock_reply_queue = MagicMock()
        mock_input_queue = MagicMock()

        task_msg = {
            "task_id": "task_fail",
            "node_name": "boom",
            "state": {},
            "serialized_done_queue": b"sdesc",
        }
        mock_input_queue.get.side_effect = [task_msg, None]

        with patch("dragon.ai.langgraph.host.Queue") as MockQueue, \
             patch("dragon.ai.langgraph.host._send_envelope") as mock_send:
            MockQueue.return_value = mock_input_queue
            agent_host_entry(
                agents={"boom": _failing_agent},
                reply_queue=mock_reply_queue,
            )

        # An error envelope was sent for the failing task.
        mock_send.assert_called_once()
        envelope = _decode_envelope(mock_send.call_args[0][1])
        self.assertEqual(envelope[ENV_STATUS], STATUS_ERROR)
        self.assertIsInstance(envelope[ENV_PAYLOAD], ValueError)
        # And the host cleaned up its queue.
        mock_input_queue.destroy.assert_called_once()


# ========================================================================
# agent_host_entry — shutdown drain of in-flight work
# ========================================================================

class TestAgentHostEntryDrain(TestCase):
    """Shutdown drains in-flight tasks; stragglers are cancelled but still
    report back so the coordinator never hangs."""

    def test_in_flight_task_completes_before_exit(self):
        """A task still running when the shutdown sentinel arrives is drained to
        completion (its DONE envelope is sent) before the host exits."""
        started = []

        async def _slow(state):
            started.append(1)
            await asyncio.sleep(0.3)  # still running when the sentinel arrives
            return {"messages": ["done late"]}

        sent = _run_tasks({"a": _slow}, [_task_msg("a")])

        self.assertEqual(started, [1])
        self.assertEqual(len(sent), 1)
        self.assertEqual(sent[0][ENV_STATUS], STATUS_DONE)
        self.assertEqual(sent[0][ENV_PAYLOAD]["messages"][0], "done late")

    def test_straggler_is_cancelled_and_sends_error(self):
        """A task that outlives the drain timeout is cancelled, yet still sends an
        error envelope so the coordinator's Future fails instead of hanging."""
        from dragon.ai.langgraph.host import agent_host_entry

        async def _hang(state):
            await asyncio.sleep(30)  # never finishes within the patched timeout
            return {"messages": ["never"]}

        mock_reply = MagicMock()
        mock_input = MagicMock()
        mock_input.get.side_effect = [_task_msg("a"), None]

        sent = []
        with patch("dragon.ai.langgraph.host.Queue") as MockQueue, \
             patch("dragon.ai.langgraph.host._DRAIN_TIMEOUT", 0.1), \
             patch(
                 "dragon.ai.langgraph.host._send_envelope",
                 side_effect=lambda q, envelope: sent.append(
                     _decode_envelope(envelope)
                 ),
             ):
            MockQueue.return_value = mock_input
            agent_host_entry(agents={"a": _hang}, reply_queue=mock_reply)

        self.assertEqual(len(sent), 1)
        self.assertEqual(sent[0][ENV_STATUS], STATUS_ERROR)
        self.assertIsInstance(sent[0][ENV_PAYLOAD], RuntimeError)
        self.assertIn("shutting down", str(sent[0][ENV_PAYLOAD]))
        # The host still cleaned up its input queue.
        mock_input.destroy.assert_called_once()


# ========================================================================
# agent_host_entry — input queue sizing
# ========================================================================

class TestAgentHostInputQueueSizing(TestCase):
    """Verify the host input Queue is sized to the coordinator's in-flight cap."""

    def test_sizes_queue_to_cap_plus_headroom(self):
        """input_maxsize sizes the Queue to cap + headroom so put() never blocks."""
        from dragon.ai.langgraph.host import (
            agent_host_entry,
            _INPUT_QUEUE_HEADROOM,
        )

        mock_reply = MagicMock()
        mock_input = MagicMock()
        mock_input.get.return_value = None  # immediate shutdown
        with patch("dragon.ai.langgraph.host.Queue") as MockQueue:
            MockQueue.return_value = mock_input
            agent_host_entry(
                agents={"a": _simple_agent},
                reply_queue=mock_reply,
                input_maxsize=200,
            )
        MockQueue.assert_called_once_with(maxsize=200 + _INPUT_QUEUE_HEADROOM)

    def test_defaults_queue_maxsize_when_unset(self):
        """Without input_maxsize the Queue uses the Dragon default capacity."""
        from dragon.ai.langgraph.host import (
            agent_host_entry,
            _DEFAULT_INPUT_QUEUE_MAXSIZE,
        )

        mock_reply = MagicMock()
        mock_input = MagicMock()
        mock_input.get.return_value = None
        with patch("dragon.ai.langgraph.host.Queue") as MockQueue:
            MockQueue.return_value = mock_input
            agent_host_entry(agents={"a": _simple_agent}, reply_queue=mock_reply)
        MockQueue.assert_called_once_with(maxsize=_DEFAULT_INPUT_QUEUE_MAXSIZE)

    def test_input_queue_maxsize_helper(self):
        """The maxsize helper: None → default; else cap + headroom (floored)."""
        from dragon.ai.langgraph.host import (
            _input_queue_maxsize,
            _DEFAULT_INPUT_QUEUE_MAXSIZE,
            _INPUT_QUEUE_HEADROOM,
        )

        self.assertEqual(_input_queue_maxsize(None), _DEFAULT_INPUT_QUEUE_MAXSIZE)
        self.assertEqual(_input_queue_maxsize(64), 64 + _INPUT_QUEUE_HEADROOM)
        # Tiny caps are floored to a usable minimum.
        for tiny in (0, 1, 2, 7):
            self.assertEqual(_input_queue_maxsize(tiny), 16)
        # Just above the floor, headroom applies again.
        self.assertGreaterEqual(_input_queue_maxsize(64), 64)


# ========================================================================
# Completion-Queue attach cache
# ========================================================================

class TestDoneQueueCache(TestCase):
    """Verify the per-descriptor completion Queue is attached once and reused."""

    def setUp(self):
        from dragon.ai.langgraph import host as _host
        _host._done_queues.clear()
        self.addCleanup(_host._done_queues.clear)

    def test_attaches_once_per_descriptor(self):
        """Repeat tasks for one shard reuse the attach; a new shard adds one."""
        from dragon.ai.langgraph.host import _get_done_queue

        with patch("dragon.ai.langgraph.host.Queue") as MockQueue:
            MockQueue.attach.side_effect = [MagicMock(), MagicMock()]
            first = _get_done_queue(b"shard-0")
            again = _get_done_queue(b"shard-0")
            other = _get_done_queue(b"shard-1")

        self.assertIs(again, first)
        self.assertIsNot(other, first)
        self.assertEqual(MockQueue.attach.call_count, 2)

    def test_attaches_with_the_framing_pickler(self):
        """The host must decode with the same codec the watcher encoded with."""
        from dragon.ai.langgraph.constants import EnvelopePickler
        from dragon.ai.langgraph.host import _get_done_queue

        with patch("dragon.ai.langgraph.host.Queue") as MockQueue:
            _get_done_queue(b"shard-0")

        sdesc = MockQueue.attach.call_args[0][0]
        pickler = MockQueue.attach.call_args.kwargs["pickler"]
        self.assertEqual(sdesc, b"shard-0")
        self.assertIsInstance(pickler, EnvelopePickler)

    def test_shutdown_closes_but_never_destroys(self):
        """Destroying an attached Queue would tear down the coordinator's
        channel, so shutdown must only close."""
        from dragon.ai.langgraph.host import _close_done_queues, _get_done_queue

        with patch("dragon.ai.langgraph.host.Queue") as MockQueue:
            attached = MagicMock()
            MockQueue.attach.return_value = attached
            _get_done_queue(b"shard-0")
            _close_done_queues()

        attached.close.assert_called_once()
        attached.destroy.assert_not_called()

    def test_shutdown_survives_a_failing_close(self):
        """One bad close must not strand the rest of the queues in the cache."""
        from dragon.ai.langgraph import host as _host

        with patch("dragon.ai.langgraph.host.Queue") as MockQueue:
            bad, good = MagicMock(), MagicMock()
            bad.close.side_effect = RuntimeError("close boom")
            MockQueue.attach.side_effect = [bad, good]
            _host._get_done_queue(b"shard-0")
            _host._get_done_queue(b"shard-1")
            with self.assertLogs("LANGGRAPH.agent_host", level="WARNING") as logs:
                _host._close_done_queues()

        self.assertIn("close boom", "\n".join(logs.output))
        good.close.assert_called_once()
        self.assertEqual(_host._done_queues, {})


# ========================================================================
# _send_envelope
# ========================================================================

class TestSendEnvelope(TestCase):
    """Verify the bounded put that carries a completion back."""

    def test_puts_the_envelope_with_a_bound(self):
        """The put is time-bounded so a wedged coordinator cannot pin a thread."""
        from dragon.ai.langgraph.host import _send_envelope, _DONE_PUT_TIMEOUT

        done_q = MagicMock()
        envelope = ("t1", {ENV_STATUS: STATUS_DONE, ENV_PAYLOAD: {"a": 1}})
        _send_envelope(done_q, envelope)

        done_q.put.assert_called_once_with(envelope, timeout=_DONE_PUT_TIMEOUT)

    def test_full_queue_raises_so_the_caller_can_log_it(self):
        """A queue still full at the deadline surfaces as a RuntimeError, which
        _try_send logs; the coordinator then falls back to its event_timeout."""
        from dragon.ai.langgraph.host import _send_envelope

        done_q = MagicMock()
        done_q.put.side_effect = _queue.Full()

        with self.assertRaises(RuntimeError) as ctx:
            _send_envelope(done_q, ("t1", {ENV_STATUS: STATUS_DONE}))
        self.assertIn("full", str(ctx.exception))


if __name__ == "__main__":
    mp.set_start_method("dragon")
    main()
