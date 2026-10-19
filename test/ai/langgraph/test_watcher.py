"""Tests for DragonWatcher — single-threaded recv loop resolving task futures."""

import dragon
import multiprocessing as mp

import asyncio
import concurrent.futures
import queue as _queue
import sys
import threading
import time
import types
from contextlib import contextmanager
from types import SimpleNamespace
from unittest import TestCase, IsolatedAsyncioTestCase, main
from unittest.mock import MagicMock, patch, PropertyMock

import cloudpickle

from dragon.ai.langgraph.constants import (
    ENV_STATUS, ENV_PAYLOAD, STATUS_DONE, STATUS_ERROR,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# What EnvelopePickler.load returns for the sentinel stop() puts on the queue.
_STOP_SENTINEL = ("__dragon_watcher_stop__", None, None)


def _make_done_envelope(task_id: str, result: dict) -> tuple:
    """Create what Queue.get returns for a success completion."""
    return task_id, {ENV_STATUS: STATUS_DONE, ENV_PAYLOAD: result}, None


def _make_error_envelope(task_id: str, exc: Exception) -> tuple:
    """Create what Queue.get returns for an error completion."""
    return task_id, {ENV_STATUS: STATUS_ERROR, ENV_PAYLOAD: exc}, None


@contextmanager
def _patched_transport():
    """Patch the Dragon Queue ``DragonWatcher`` builds, so a real watcher can be
    constructed with no runtime.

    Every shard shares one queue mock (``Queue(...)`` returns the same
    ``return_value``).  Its ``get`` returns the stop sentinel so each shard's
    recv thread exits immediately instead of racing the test.

    Yields a namespace of the mocks tests assert against.
    """
    with patch("dragon.ai.langgraph.watcher.Queue") as queue_cls:
        queue_cls.return_value.get.return_value = _STOP_SENTINEL
        yield SimpleNamespace(
            queue_cls=queue_cls,
            queue=queue_cls.return_value,
        )


# ========================================================================
# DragonWatcher Construction
# ========================================================================

class TestDragonWatcherConstruction(TestCase):
    """Verify DragonWatcher construction and parameter validation."""

    def test_default_construction(self):
        """Watcher can be created with default parameters."""
        with _patched_transport():
            from dragon.ai.langgraph.watcher import DragonWatcher
            watcher = DragonWatcher()

            self.assertIsNotNone(watcher)
            self.assertEqual(watcher.max_concurrent_tasks, 64)
            self.assertEqual(watcher.num_shards, 1)

            watcher.stop()

    def test_num_shards_validation(self):
        """num_shards < 1 raises ValueError."""
        from dragon.ai.langgraph.watcher import DragonWatcher
        with self.assertRaisesRegex(ValueError, "num_shards"):
            DragonWatcher(num_shards=0)

    def test_custom_parameters(self):
        """Custom parameters are stored correctly."""
        with _patched_transport():
            from dragon.ai.langgraph.watcher import DragonWatcher
            watcher = DragonWatcher(
                max_concurrent_tasks=256,
                num_shards=4,
            )

            self.assertEqual(watcher.max_concurrent_tasks, 256)
            self.assertEqual(watcher.num_shards, 4)

            watcher.stop()

    def test_creates_shards(self):
        """Watcher creates specified number of shards."""
        with _patched_transport():
            from dragon.ai.langgraph.watcher import DragonWatcher
            watcher = DragonWatcher(num_shards=3)

            self.assertEqual(len(watcher._shards), 3)

            watcher.stop()

    def test_one_queue_per_shard(self):
        """Each shard gets its own completion Queue, sized to the in-flight cap
        and carrying the framing pickler that keeps task_id outside the body."""
        with _patched_transport() as t:
            from dragon.ai.langgraph.watcher import DragonWatcher
            from dragon.ai.langgraph.constants import EnvelopePickler
            watcher = DragonWatcher(num_shards=2)

            self.assertEqual(t.queue_cls.call_count, 2)
            for call in t.queue_cls.call_args_list:
                self.assertTrue(call.kwargs["buffered"])
                self.assertIsInstance(call.kwargs["pickler"], EnvelopePickler)
            # The descriptor handed to hosts comes from the Queue itself.
            for shard in watcher._shards:
                self.assertIs(shard.serialized_queue, t.queue.serialize.return_value)

            watcher.stop()


# ========================================================================
# DragonWatcher._set_outcome
# ========================================================================

class TestDragonWatcherSetOutcome(TestCase):
    """Verify _set_outcome resolves futures correctly."""

    def test_sets_result_on_sync_future(self):
        """_set_outcome sets result on concurrent.futures.Future."""
        with _patched_transport():
            from dragon.ai.langgraph.watcher import DragonWatcher, _PendingTask
            watcher = DragonWatcher()

            future = concurrent.futures.Future()
            pending = _PendingTask(
                future=future,
                task_id="task_1",
                node_name="test",
                loop=None,
            )

            watcher._set_outcome(pending, result={"data": 42})

            self.assertTrue(future.done())
            self.assertEqual(future.result(), {"data": 42})

            watcher.stop()

    def test_sets_exception_on_sync_future(self):
        """_set_outcome sets exception on concurrent.futures.Future."""
        with _patched_transport():
            from dragon.ai.langgraph.watcher import DragonWatcher, _PendingTask
            watcher = DragonWatcher()

            future = concurrent.futures.Future()
            pending = _PendingTask(
                future=future,
                task_id="task_2",
                node_name="test",
                loop=None,
            )

            watcher._set_outcome(pending, exc=ValueError("test error"))

            self.assertTrue(future.done())
            with self.assertRaisesRegex(ValueError, "test error"):
                future.result()

            watcher.stop()


# ========================================================================
# DragonWatcher._apply_outcome
# ========================================================================

class TestDragonWatcherApplyOutcome(TestCase):
    """Verify _apply_outcome handles edge cases."""

    def test_ignores_cancelled_future(self):
        """_apply_outcome ignores cancelled futures."""
        from dragon.ai.langgraph.watcher import DragonWatcher

        future = concurrent.futures.Future()
        future.cancel()

        # Should not raise
        DragonWatcher._apply_outcome(future, {"result": "data"}, None)

    def test_sets_result(self):
        """_apply_outcome sets result on non-cancelled future."""
        from dragon.ai.langgraph.watcher import DragonWatcher

        future = concurrent.futures.Future()
        DragonWatcher._apply_outcome(future, {"result": "data"}, None)

        self.assertEqual(future.result(), {"result": "data"})

    def test_sets_exception(self):
        """_apply_outcome sets exception on non-cancelled future."""
        from dragon.ai.langgraph.watcher import DragonWatcher

        future = concurrent.futures.Future()
        DragonWatcher._apply_outcome(future, None, RuntimeError("fail"))

        with self.assertRaisesRegex(RuntimeError, "fail"):
            future.result()


# ========================================================================
# DragonWatcher._resolve_task
# ========================================================================

class TestDragonWatcherResolveTask(TestCase):
    """Verify _resolve_task handles various envelope statuses."""

    def test_resolves_done_status(self):
        """Done envelope resolves future with payload."""
        with _patched_transport():
            from dragon.ai.langgraph.watcher import DragonWatcher, _PendingTask
            watcher = DragonWatcher()

            future = concurrent.futures.Future()
            pending = _PendingTask(
                future=future,
                task_id="task_done",
                node_name="test",
                loop=None,
            )

            envelope = {
                ENV_STATUS: STATUS_DONE,
                ENV_PAYLOAD: {"messages": ["result"]},
            }

            watcher._resolve_task(pending, envelope)

            self.assertEqual(future.result(), {"messages": ["result"]})

            watcher.stop()

    def test_resolves_error_status(self):
        """Error envelope resolves future with exception."""
        with _patched_transport():
            from dragon.ai.langgraph.watcher import DragonWatcher, _PendingTask
            watcher = DragonWatcher()

            future = concurrent.futures.Future()
            pending = _PendingTask(
                future=future,
                task_id="task_error",
                node_name="test",
                loop=None,
            )

            envelope = {
                ENV_STATUS: STATUS_ERROR,
                ENV_PAYLOAD: ValueError("agent failed"),
            }

            watcher._resolve_task(pending, envelope)

            with self.assertRaisesRegex(ValueError, "agent failed"):
                future.result()

            watcher.stop()

    def test_handles_unknown_status(self):
        """Unknown status resolves future with RuntimeError."""
        with _patched_transport():
            from dragon.ai.langgraph.watcher import DragonWatcher, _PendingTask
            watcher = DragonWatcher()

            future = concurrent.futures.Future()
            pending = _PendingTask(
                future=future,
                task_id="task_unknown",
                node_name="test",
                loop=None,
            )

            envelope = {
                ENV_STATUS: "invalid_status",
                ENV_PAYLOAD: None,
            }

            watcher._resolve_task(pending, envelope)

            with self.assertRaisesRegex(RuntimeError, "unexpected status"):
                future.result()

            watcher.stop()


# ========================================================================
# DragonWatcher.stop
# ========================================================================

class TestDragonWatcherStop(TestCase):
    """Verify stop() terminates watcher threads and frees resources."""

    def test_sets_stopped_flag(self):
        """stop() sets the _stopped flag."""
        with _patched_transport():
            from dragon.ai.langgraph.watcher import DragonWatcher
            watcher = DragonWatcher()

            self.assertFalse(watcher._stopped)
            watcher.stop()
            self.assertTrue(watcher._stopped)

    def test_wakes_recv_loop_with_sentinel(self):
        """stop() puts the stop sentinel so a blocked get() returns."""
        with _patched_transport() as t:
            from dragon.ai.langgraph.watcher import DragonWatcher
            watcher = DragonWatcher()
            watcher.stop()

            sent = [c.args[0] for c in t.queue.put.call_args_list]
            self.assertIn(("__dragon_watcher_stop__", None), sent)

    def test_destroys_queues(self):
        """stop() destroys each shard's completion Queue, which releases its
        channel refcount and detaches the pool the Queue attached."""
        with _patched_transport() as t:
            from dragon.ai.langgraph.watcher import DragonWatcher
            watcher = DragonWatcher()
            watcher.stop()

            t.queue.destroy.assert_called_once()
            self.assertIsNone(watcher._shards[0].queue)

    def test_stop_is_idempotent(self):
        """A second stop() is a no-op, so teardown cannot double-destroy."""
        with _patched_transport() as t:
            from dragon.ai.langgraph.watcher import DragonWatcher
            watcher = DragonWatcher()
            watcher.stop()
            watcher.stop()

            t.queue.destroy.assert_called_once()

    def test_queue_destroy_failure_is_reported_and_stop_finishes(self):
        """An undestroyed completion Queue is a leaked Dragon resource, so the
        failure has to be logged — and it must not abort the rest of teardown."""
        with _patched_transport() as t:
            from dragon.ai.langgraph.watcher import DragonWatcher
            t.queue.destroy.side_effect = RuntimeError("destroy boom")
            watcher = DragonWatcher(num_shards=2)

            with self.assertLogs("LANGGRAPH.watcher", level="WARNING") as logs:
                watcher.stop()

            self.assertIn("destroy boom", "\n".join(logs.output))
            # Both shards were still attempted, and state was cleared.
            self.assertEqual(t.queue.destroy.call_count, 2)
            self.assertTrue(all(s.queue is None for s in watcher._shards))

    def test_stuck_recv_thread_leaves_its_queue_and_says_so(self):
        """Destroying a Queue under a still-blocked get() would crash, so the
        queue is left for the runtime — loudly, since that is a leak."""
        with _patched_transport() as t:
            from dragon.ai.langgraph.watcher import DragonWatcher
            watcher = DragonWatcher()
            shard = watcher._shards[0]
            shard.thread = MagicMock()
            shard.thread.is_alive.return_value = True

            with self.assertLogs("LANGGRAPH.watcher", level="WARNING") as logs:
                watcher.stop()

            self.assertIn("did not stop", "\n".join(logs.output))
            t.queue.destroy.assert_not_called()

    def test_sentinel_failure_is_reported(self):
        """If the wake-up sentinel cannot be sent the recv thread never exits,
        so the failure must not be swallowed silently."""
        with _patched_transport() as t:
            from dragon.ai.langgraph.watcher import DragonWatcher
            watcher = DragonWatcher()
            t.queue.put.side_effect = RuntimeError("sentinel boom")

            with self.assertLogs("LANGGRAPH.watcher", level="WARNING") as logs:
                watcher.stop()

            self.assertIn("sentinel boom", "\n".join(logs.output))
            self.assertTrue(watcher._stopped)


# ========================================================================
# DragonWatcher._release_slot
# ========================================================================

class TestDragonWatcherReleaseSlot(TestCase):
    """Verify _release_slot handles semaphore release correctly."""

    def test_releases_slot(self):
        """_release_slot releases a semaphore slot."""
        from dragon.ai.langgraph.watcher import DragonWatcher, _Shard

        shard = _Shard(index=0)
        shard.slots = threading.BoundedSemaphore(2)
        shard.slots.acquire()  # Take one slot

        DragonWatcher._release_slot(shard)

        # Should be able to acquire 2 slots now
        self.assertTrue(shard.slots.acquire(blocking=False))
        self.assertTrue(shard.slots.acquire(blocking=False))

    def test_ignores_over_release(self):
        """Over-release cannot raise, but it means slot accounting drifted — a
        real bug that would otherwise show up only as lost concurrency."""
        from dragon.ai.langgraph.watcher import DragonWatcher, _Shard

        shard = _Shard(index=0)
        shard.slots = threading.BoundedSemaphore(1)
        # Don't acquire, so release would over-release

        with self.assertLogs("LANGGRAPH.watcher", level="ERROR") as logs:
            DragonWatcher._release_slot(shard)  # must not raise

        self.assertIn("over-released", "\n".join(logs.output))


# ========================================================================
# _PendingTask
# ========================================================================

class TestPendingTask(TestCase):
    """Verify _PendingTask bookkeeping structure."""

    def test_stores_attributes(self):
        """_PendingTask stores all provided attributes."""
        from dragon.ai.langgraph.watcher import _PendingTask

        future = concurrent.futures.Future()
        pending = _PendingTask(
            future=future,
            task_id="task_abc",
            node_name="researcher",
            loop=None,
        )

        self.assertIs(pending.future, future)
        self.assertEqual(pending.task_id, "task_abc")
        self.assertEqual(pending.node_name, "researcher")
        self.assertIsNone(pending.loop)

    def test_stores_loop_for_async(self):
        """_PendingTask stores event loop for async tasks."""
        from dragon.ai.langgraph.watcher import _PendingTask

        mock_loop = MagicMock()
        pending = _PendingTask(
            future=MagicMock(),
            task_id="async_task",
            node_name="writer",
            loop=mock_loop,
        )

        self.assertIs(pending.loop, mock_loop)


# ========================================================================
# _Shard
# ========================================================================

class TestShard(TestCase):
    """Verify _Shard structure initialization."""

    def test_stores_index(self):
        """_Shard stores its index."""
        from dragon.ai.langgraph.watcher import _Shard

        shard = _Shard(index=5)
        self.assertEqual(shard.index, 5)

    def test_default_attributes(self):
        """_Shard initializes with None/empty defaults."""
        from dragon.ai.langgraph.watcher import _Shard

        shard = _Shard(index=0)
        self.assertIsNone(shard.queue)
        self.assertEqual(shard.serialized_queue, b"")
        self.assertIsNone(shard.slots)
        self.assertEqual(shard.pending, {})
        self.assertIsNotNone(shard.lock)
        self.assertIsNone(shard.thread)


# ========================================================================
# DragonWatcher async support
# ========================================================================

class TestDragonWatcherAsyncSupport(IsolatedAsyncioTestCase):
    """Verify DragonWatcher handles async futures correctly."""

    async def test_submit_task_async_returns_asyncio_future(self):
        """submit_task_async returns an asyncio.Future."""
        with _patched_transport():
            from dragon.ai.langgraph.watcher import DragonWatcher
            watcher = DragonWatcher()

            # Mock _register_and_dispatch to avoid actual dispatch; the slot is
            # acquired for real (fast path), so the awaitable acquire is exercised.
            with patch.object(watcher, "_register_and_dispatch"):
                mock_queue_bytes = cloudpickle.dumps(MagicMock())
                future = await watcher.submit_task_async(
                    node_name="test",
                    state={"messages": []},
                    serialized_input_queue=mock_queue_bytes,
                )

                self.assertIsInstance(future, asyncio.Future)

            watcher.stop()

    async def test_set_outcome_with_async_future(self):
        """_set_outcome uses call_soon_threadsafe for async futures."""
        with _patched_transport():
            from dragon.ai.langgraph.watcher import DragonWatcher, _PendingTask
            watcher = DragonWatcher()

            loop = asyncio.get_running_loop()
            future = loop.create_future()

            pending = _PendingTask(
                future=future,
                task_id="async_task",
                node_name="test",
                loop=loop,
            )

            # Set outcome from a thread (simulating watcher thread)
            def set_in_thread():
                watcher._set_outcome(pending, result={"data": "async_result"})

            await asyncio.get_running_loop().run_in_executor(None, set_in_thread)

            # Wait a bit for call_soon_threadsafe to complete
            await asyncio.sleep(0.1)

            self.assertTrue(future.done())
            self.assertEqual(future.result(), {"data": "async_result"})

            watcher.stop()


# ========================================================================
# Shared harness for watcher-internal tests
# ========================================================================

class _WatcherHarness:
    """setUp mixin: patches the Dragon Queue so a real DragonWatcher builds with
    mock queues but *real* semaphores, threads, locks, and pending-tables — the
    parts these tests exercise.

    The shard queue's ``get`` returns the stop sentinel, so each shard's recv
    thread reads it and exits immediately; tests drive resolution/slot-release
    by hand instead of racing a live recv loop.
    """

    def setUp(self):
        super().setUp()
        # Queue(...) builds the shard's completion queue; Queue.attach(...).put()
        # is the dispatch.  One patch covers both.  Individual tests may
        # override .attach.side_effect.
        self.mock_queue_cls = patch("dragon.ai.langgraph.watcher.Queue").start()
        self.mock_queue_cls.return_value.get.return_value = _STOP_SENTINEL
        self.addCleanup(patch.stopall)

    def _make_watcher(self, **kwargs):
        from dragon.ai.langgraph.watcher import DragonWatcher
        return DragonWatcher(**kwargs)


# ========================================================================
# DragonWatcher.submit_task (sync path)
# ========================================================================

class TestDragonWatcherSubmitTaskSync(_WatcherHarness, TestCase):
    """Verify the synchronous submit_task dispatch, slot use, and unwind."""

    def test_returns_future_and_dispatches(self):
        """submit_task returns a Future, registers the task, and puts a msg."""
        watcher = self._make_watcher(max_concurrent_tasks=2)
        shard = watcher._shards[0]

        future = watcher.submit_task("n", {"m": 1}, b"queue_bytes")

        self.assertIsInstance(future, concurrent.futures.Future)
        self.assertEqual(len(shard.pending), 1)
        self.mock_queue_cls.attach.assert_called_once_with(b"queue_bytes")
        self.mock_queue_cls.attach.return_value.put.assert_called_once()

        watcher.stop()

    def test_consumes_a_backpressure_slot(self):
        """Each dispatched task holds one slot from its shard's semaphore."""
        watcher = self._make_watcher(max_concurrent_tasks=2)
        shard = watcher._shards[0]

        watcher.submit_task("n", {}, b"x")

        # 2 slots, 1 now held → exactly one remains.
        self.assertTrue(shard.slots.acquire(blocking=False))
        self.assertFalse(shard.slots.acquire(blocking=False))

        watcher.stop()

    def test_dispatch_failure_unwinds_registration_and_slot(self):
        """If the host dispatch raises, the task is unwound and the Future fails."""
        watcher = self._make_watcher(max_concurrent_tasks=2)
        shard = watcher._shards[0]
        self.mock_queue_cls.attach.side_effect = RuntimeError("dispatch boom")

        future = watcher.submit_task("n", {}, b"x")

        # Future is failed with the dispatch error.
        with self.assertRaisesRegex(RuntimeError, "dispatch boom"):
            future.result(timeout=1.0)
        # Pending entry removed.
        self.assertEqual(shard.pending, {})
        # Slot released back — both slots available again.
        self.assertTrue(shard.slots.acquire(blocking=False))
        self.assertTrue(shard.slots.acquire(blocking=False))

        watcher.stop()

    def test_blocks_at_cap_then_unblocks_on_release(self):
        """A dispatch at the cap blocks the caller until a slot frees."""
        watcher = self._make_watcher(max_concurrent_tasks=1)
        shard = watcher._shards[0]

        # First task takes the only slot.
        watcher.submit_task("n", {}, b"x")
        self.assertFalse(shard.slots.acquire(blocking=False))

        started = threading.Event()
        done = threading.Event()

        def worker():
            started.set()
            watcher.submit_task("n", {}, b"x")
            done.set()

        t = threading.Thread(target=worker)
        t.start()
        self.assertTrue(started.wait(1.0))
        # Second dispatch is blocked waiting for a slot.
        self.assertFalse(done.wait(0.3))

        # Simulate the first task completing — release its slot.
        watcher._release_slot(shard)

        self.assertTrue(done.wait(2.0))
        t.join(timeout=2.0)
        self.assertFalse(t.is_alive())

        watcher.stop()


# ========================================================================
# DragonWatcher backpressure slot acquisition
# ========================================================================

class TestDragonWatcherSlotAcquisition(_WatcherHarness, TestCase):
    """Verify the low-level slot-acquire helpers and throttle warning."""

    def test_blocking_fast_path_returns_immediately(self):
        """_acquire_slot_blocking takes a free slot with no warning."""
        watcher = self._make_watcher(max_concurrent_tasks=2)
        shard = watcher._shards[0]

        watcher._acquire_slot_blocking(shard)

        self.assertFalse(watcher._throttle_warned)  # fast path warns nothing
        self.assertTrue(shard.slots.acquire(blocking=False))  # 1 of 2 left
        watcher.stop()

    def test_warn_throttle_once_warns_exactly_once(self):
        """_warn_throttle_once logs once and latches the flag."""
        watcher = self._make_watcher()
        with patch("dragon.ai.langgraph.watcher.logger") as mock_logger:
            self.assertFalse(watcher._throttle_warned)
            watcher._warn_throttle_once()
            watcher._warn_throttle_once()
            watcher._warn_throttle_once()
            mock_logger.warning.assert_called_once()
        self.assertTrue(watcher._throttle_warned)
        watcher.stop()

    def test_acquire_slot_wait_raises_when_stopped(self):
        """A slot wait aborts with RuntimeError if the watcher is stopped."""
        watcher = self._make_watcher(max_concurrent_tasks=1)
        shard = watcher._shards[0]
        shard.slots.acquire()  # no slots free
        watcher._stopped = True

        with self.assertRaisesRegex(RuntimeError, "stopped"):
            watcher._acquire_slot_wait(shard)

        watcher.stop()


# ========================================================================
# DragonWatcher async backpressure (the await-don't-freeze property)
# ========================================================================

class TestDragonWatcherAsyncBackpressure(_WatcherHarness, IsolatedAsyncioTestCase):
    """Verify the async slot acquire never blocks the event loop."""

    async def test_fast_path_acquires_inline(self):
        """When a slot is free, _acquire_slot_async takes it with no thread hop."""
        watcher = self._make_watcher(max_concurrent_tasks=2)
        shard = watcher._shards[0]
        loop = asyncio.get_running_loop()

        await watcher._acquire_slot_async(shard, loop)

        # Exactly one slot consumed.
        self.assertTrue(shard.slots.acquire(blocking=False))
        self.assertFalse(shard.slots.acquire(blocking=False))
        watcher.stop()

    async def test_await_does_not_block_event_loop(self):
        """At the cap, the async acquire suspends — the loop keeps running —
        and completes once another thread releases a slot."""
        watcher = self._make_watcher(max_concurrent_tasks=1)
        shard = watcher._shards[0]
        # Exhaust the only slot.
        self.assertTrue(shard.slots.acquire(blocking=False))
        loop = asyncio.get_running_loop()

        acq = asyncio.create_task(watcher._acquire_slot_async(shard, loop))

        # The loop must keep servicing other coroutines while acq waits.
        ticks = 0
        for _ in range(3):
            await asyncio.sleep(0.02)
            ticks += 1
        self.assertEqual(ticks, 3)          # loop never froze
        self.assertFalse(acq.done())        # acquire still pending on the slot

        # A completion on another thread frees a slot.
        threading.Thread(target=watcher._release_slot, args=(shard,)).start()

        await asyncio.wait_for(acq, timeout=2.0)
        self.assertTrue(acq.done())
        watcher.stop()


# ========================================================================
# DragonWatcher._handle_envelope
# ========================================================================

class TestDragonWatcherHandleEnvelope(_WatcherHarness, TestCase):
    """Verify envelope handling resolves the Future and frees the slot."""

    def test_resolves_future_and_releases_slot(self):
        """A matching envelope resolves the Future and releases its slot."""
        from dragon.ai.langgraph.watcher import _PendingTask

        watcher = self._make_watcher(max_concurrent_tasks=2)
        shard = watcher._shards[0]
        shard.slots.acquire()  # simulate the slot this task holds
        future = concurrent.futures.Future()
        with shard.lock:
            shard.pending["t1"] = _PendingTask(
                future=future, task_id="t1", node_name="n", loop=None
            )

        envelope = {
            ENV_STATUS: STATUS_DONE,
            ENV_PAYLOAD: {"ok": 1},
        }
        watcher._handle_envelope(shard, "t1", envelope)

        self.assertEqual(future.result(timeout=1.0), {"ok": 1})
        self.assertNotIn("t1", shard.pending)  # popped
        # Slot released — both of the 2 slots are free again.
        self.assertTrue(shard.slots.acquire(blocking=False))
        self.assertTrue(shard.slots.acquire(blocking=False))

        watcher.stop()

    def test_unknown_task_id_is_ignored_without_release(self):
        """An envelope for an unknown task_id is dropped and frees no slot."""
        watcher = self._make_watcher(max_concurrent_tasks=1)
        shard = watcher._shards[0]
        shard.slots.acquire()  # a real in-flight task holds the only slot

        # Envelope for a task this shard never registered.
        watcher._handle_envelope(
            shard,
            "ghost",
            {ENV_STATUS: STATUS_DONE, ENV_PAYLOAD: {}},
        )

        # Nothing resolved, and no slot was spuriously released.
        self.assertEqual(shard.pending, {})
        self.assertFalse(shard.slots.acquire(blocking=False))

        watcher.stop()


# ========================================================================
# DragonWatcher — undecodable completion envelopes
# ========================================================================

class TestDragonWatcherUndecodableEnvelope(_WatcherHarness, TestCase):
    """A completion whose body cannot be unpickled must fail *its own* task.

    The agent already ran and its slot is spent, so silently skipping the
    message would hang that Future forever and leak the slot permanently.
    Framing the task_id outside the pickle is what makes attribution possible.
    """

    def _watcher_with_pending(self, task_id="t1", **kwargs):
        """Build a watcher with one registered task holding one slot."""
        from dragon.ai.langgraph.watcher import _PendingTask

        watcher = self._make_watcher(**kwargs)
        shard = watcher._shards[0]
        # The shard's own thread consumed the sentinel and exited; joining it
        # keeps it from stealing the messages these tests feed in.
        shard.thread.join(timeout=5)

        shard.slots.acquire()  # the slot this in-flight task holds
        future = concurrent.futures.Future()
        with shard.lock:
            shard.pending[task_id] = _PendingTask(
                future=future, task_id=task_id, node_name="writer", loop=None
            )
        return watcher, shard, future

    def test_fails_the_matching_future(self):
        """The task named by the header is failed, not left pending."""
        watcher, shard, future = self._watcher_with_pending()

        watcher._fail_undecodable(shard, "t1", ValueError("no module named x"))

        with self.assertRaises(RuntimeError):
            future.result(timeout=1.0)
        self.assertNotIn("t1", shard.pending)
        watcher.stop()

    def test_error_names_the_real_cause(self):
        """The error blames deserialization, not the agent, and names the node."""
        watcher, shard, future = self._watcher_with_pending()

        watcher._fail_undecodable(shard, "t1", ValueError("no module named x"))

        with self.assertRaises(RuntimeError) as ctx:
            future.result(timeout=1.0)
        message = str(ctx.exception)
        self.assertIn("writer", message)
        self.assertIn("deserialized", message)
        # The original failure is carried through, not swallowed.
        self.assertIn("no module named x", message)
        self.assertIn("ValueError", message)
        watcher.stop()

    def test_releases_the_backpressure_slot(self):
        """The spent slot is returned, so the shard cannot deadlock."""
        watcher, shard, _ = self._watcher_with_pending(max_concurrent_tasks=1)
        self.assertFalse(shard.slots.acquire(blocking=False))  # cap reached

        watcher._fail_undecodable(shard, "t1", ValueError("boom"))

        self.assertTrue(shard.slots.acquire(blocking=False))
        watcher.stop()

    def test_unknown_task_id_releases_no_slot(self):
        """An undecodable body for an unregistered task frees nothing."""
        watcher, shard, future = self._watcher_with_pending(max_concurrent_tasks=1)

        watcher._fail_undecodable(shard, "ghost", ValueError("boom"))

        self.assertFalse(shard.slots.acquire(blocking=False))
        self.assertIn("t1", shard.pending)
        self.assertFalse(future.done())
        watcher.stop()

    def test_recv_loop_attributes_a_corrupt_body(self):
        """End to end: a body the pickler could not decode fails that task and
        the loop keeps running for the next message."""
        watcher, shard, future = self._watcher_with_pending(max_concurrent_tasks=1)
        shard.queue.get.side_effect = [
            ("t1", None, ValueError("no module named x")),
            _STOP_SENTINEL,
        ]

        watcher._recv_loop(shard)  # returns when it reads the sentinel

        with self.assertRaises(RuntimeError):
            future.result(timeout=1.0)
        self.assertTrue(shard.slots.acquire(blocking=False))
        watcher.stop()

    def test_recv_loop_survives_an_unreadable_header(self):
        """A frame too short to parse cannot be attributed, but must not kill
        the loop — later completions still resolve."""
        watcher, shard, future = self._watcher_with_pending()
        shard.queue.get.side_effect = [
            ValueError("completion envelope is too short to be framed"),
            _make_done_envelope("t1", {"ok": 1}),
            _STOP_SENTINEL,
        ]

        watcher._recv_loop(shard)

        self.assertEqual(future.result(timeout=1.0), {"ok": 1})
        watcher.stop()

    def test_stop_sentinel_is_recognized(self):
        """stop() puts a sentinel the recv loop actually recognizes."""
        from dragon.ai.langgraph.watcher import _STOP_TASK_ID, DragonWatcher

        watcher = self._make_watcher()
        shard = watcher._shards[0]
        shard.thread.join(timeout=5)
        shard.queue.put.reset_mock()

        DragonWatcher._wake_recv_loop(shard)
        task_id, body = shard.queue.put.call_args[0][0]

        self.assertEqual(task_id, _STOP_TASK_ID)
        self.assertIsNone(body)

        # And the loop exits on it rather than treating it as a task.
        shard.queue.get.side_effect = [(task_id, None, None)]
        watcher._recv_loop(shard)
        watcher.stop()


# ========================================================================
# DragonWatcher — end to end over the real EnvelopePickler
# ========================================================================

class _Wire:
    """Collects what ``EnvelopePickler.dump`` writes, like the FLI's adapter."""

    def __init__(self) -> None:
        self.chunks: list[bytes] = []

    def write(self, chunk: bytes) -> None:
        self.chunks.append(chunk)


class _Frame:
    """Replays one whole message, like the FLI's read adapter."""

    def __init__(self, data: bytes) -> None:
        self.data = data

    def read(self, size: int = -1) -> bytes:
        return self.data


class _RealCodecQueue:
    """A Queue stand-in that runs the *real* ``EnvelopePickler`` on both sides.

    ``Queue.put``/``Queue.get`` delegate wholly to the pickler, so replaying
    that here is what makes these tests cover the wire codec rather than a
    mock's canned return value.
    """

    def __init__(self, frames=()) -> None:
        from dragon.ai.langgraph.constants import EnvelopePickler
        self._pickler = EnvelopePickler()
        self._frames: list[bytes] = list(frames)

    def put(self, obj, timeout=None) -> None:
        wire = _Wire()
        self._pickler.dump(obj, wire)
        self._frames.append(b"".join(wire.chunks))

    def get(self, timeout=None):
        if not self._frames:
            raise _queue.Empty
        return self._pickler.load(_Frame(self._frames.pop(0)))

    def destroy(self) -> None:
        pass


class _AbsentPayload:
    """Payload class used to fake a result whose class the coordinator lacks."""


_ABSENT_MODULE = "_dragon_langgraph_absent_module"


def _unimportable_body() -> bytes:
    """cloudpickle a payload *by reference* to a module that then disappears.

    This is the real-world shape of the failure: the agent's result class lives
    in a module importable on the host but not in the coordinator process.
    """
    module = types.ModuleType(_ABSENT_MODULE)
    module._AbsentPayload = _AbsentPayload
    original = _AbsentPayload.__module__
    sys.modules[_ABSENT_MODULE] = module
    _AbsentPayload.__module__ = _ABSENT_MODULE
    try:
        return cloudpickle.dumps(
            {ENV_STATUS: STATUS_DONE, ENV_PAYLOAD: _AbsentPayload()}
        )
    finally:
        _AbsentPayload.__module__ = original
        del sys.modules[_ABSENT_MODULE]


class _UnpicklableError(Exception):
    """An exception that cannot survive cloudpickle, like some SDK errors."""

    def __reduce__(self):
        raise TypeError("this exception cannot be pickled")


class TestDragonWatcherRealCodec(_WatcherHarness, TestCase):
    """Drive the recv loop with bytes an AgentHost actually produces.

    The tests above feed the loop pre-decoded tuples, which proves its
    branching but not that the branches are reachable from the wire.  Here the
    host's own envelope builders write through the real ``EnvelopePickler`` and
    the loop reads back through it, so a codec change that breaks attribution
    of an undecodable result fails a test instead of hanging a workflow.
    """

    def _watcher_with_pending(self, task_ids=("t1",), **kwargs):
        """Build a watcher whose shard uses the real codec, with tasks pending."""
        from dragon.ai.langgraph.watcher import _PendingTask

        watcher = self._make_watcher(**kwargs)
        shard = watcher._shards[0]
        # The shard's own thread consumed the mock sentinel and exited; joining
        # it keeps it from stealing the messages these tests feed in.
        shard.thread.join(timeout=5)
        shard.queue = _RealCodecQueue()

        futures = {}
        for task_id in task_ids:
            shard.slots.acquire()  # the slot this in-flight task holds
            futures[task_id] = concurrent.futures.Future()
            with shard.lock:
                shard.pending[task_id] = _PendingTask(
                    future=futures[task_id],
                    task_id=task_id,
                    node_name="writer",
                    loop=None,
                )
        return watcher, shard, futures

    @staticmethod
    def _stop(shard) -> None:
        """Append the real stop sentinel so ``_recv_loop`` returns."""
        from dragon.ai.langgraph.watcher import _STOP_TASK_ID
        shard.queue.put((_STOP_TASK_ID, None))

    def test_host_done_envelope_resolves_the_future(self):
        """A success envelope built by the host resolves through the codec."""
        from dragon.ai.langgraph.host import _build_done_envelope

        watcher, shard, futures = self._watcher_with_pending()
        shard.queue.put(_build_done_envelope("t1", {"messages": ["hi"]}))
        self._stop(shard)

        watcher._recv_loop(shard)

        self.assertEqual(futures["t1"].result(timeout=1.0), {"messages": ["hi"]})
        watcher.stop()

    def test_host_error_envelope_raises_the_original_exception(self):
        """An error envelope carries the agent's own exception, not a stand-in."""
        from dragon.ai.langgraph.host import _build_error_envelope

        watcher, shard, futures = self._watcher_with_pending()
        shard.queue.put(_build_error_envelope("t1", ValueError("agent blew up")))
        self._stop(shard)

        watcher._recv_loop(shard)

        with self.assertRaises(ValueError) as ctx:
            futures["t1"].result(timeout=1.0)
        self.assertIn("agent blew up", str(ctx.exception))
        watcher.stop()

    def test_unpicklable_exception_still_fails_the_future(self):
        """The pickler's fallback keeps an unpicklable exception from stranding
        the Future — the agent's own text survives as a plain RuntimeError."""
        from dragon.ai.langgraph.host import _build_error_envelope

        watcher, shard, futures = self._watcher_with_pending()
        with self.assertLogs("LANGGRAPH.envelope", level="ERROR"):
            shard.queue.put(
                _build_error_envelope("t1", _UnpicklableError("sdk exploded"))
            )
        self._stop(shard)

        watcher._recv_loop(shard)

        with self.assertRaises(RuntimeError) as ctx:
            futures["t1"].result(timeout=1.0)
        self.assertIn("sdk exploded", str(ctx.exception))
        watcher.stop()

    def test_corrupt_body_fails_its_own_future(self):
        """A body the coordinator cannot unpickle fails the task it belongs to,
        rather than raising out of get() with no task_id attached."""
        from dragon.ai.langgraph.constants import pack_envelope

        watcher, shard, futures = self._watcher_with_pending(max_concurrent_tasks=1)
        shard.queue._frames.append(pack_envelope("t1", b"not-a-pickle"))
        self._stop(shard)

        watcher._recv_loop(shard)

        with self.assertRaises(RuntimeError) as ctx:
            futures["t1"].result(timeout=1.0)
        self.assertIn("deserialized", str(ctx.exception))
        # The spent slot came back, so the shard cannot deadlock.
        self.assertTrue(shard.slots.acquire(blocking=False))
        watcher.stop()

    def test_unimportable_payload_fails_its_own_future(self):
        """A result whose class is missing in the coordinator is blamed on its
        own task and names the node, instead of hanging until event_timeout."""
        body = _unimportable_body()
        try:
            cloudpickle.loads(body)
        except Exception:
            pass
        else:
            self.skipTest("cloudpickle inlined the class; cannot fake a lost module")

        from dragon.ai.langgraph.constants import pack_envelope

        watcher, shard, futures = self._watcher_with_pending()
        shard.queue._frames.append(pack_envelope("t1", body))
        self._stop(shard)

        watcher._recv_loop(shard)

        with self.assertRaises(RuntimeError) as ctx:
            futures["t1"].result(timeout=1.0)
        message = str(ctx.exception)
        self.assertIn("writer", message)
        self.assertIn("deserialized", message)
        watcher.stop()

    def test_undecodable_message_does_not_stop_later_tasks(self):
        """One task's decode failure must not take the shard down with it."""
        from dragon.ai.langgraph.constants import pack_envelope
        from dragon.ai.langgraph.host import _build_done_envelope

        watcher, shard, futures = self._watcher_with_pending(task_ids=("t1", "t2"))
        shard.queue._frames.append(pack_envelope("t1", b"not-a-pickle"))
        shard.queue.put(_build_done_envelope("t2", {"messages": ["ok"]}))
        self._stop(shard)

        watcher._recv_loop(shard)

        with self.assertRaises(RuntimeError):
            futures["t1"].result(timeout=1.0)
        self.assertEqual(futures["t2"].result(timeout=1.0), {"messages": ["ok"]})
        watcher.stop()

    def test_unframable_message_leaves_other_tasks_alive(self):
        """A message with no readable task_id cannot be blamed on anyone, but the
        loop keeps running so the next completion still resolves."""
        from dragon.ai.langgraph.host import _build_done_envelope

        watcher, shard, futures = self._watcher_with_pending()
        shard.queue._frames.append(b"\x00")  # truncated framing header
        shard.queue.put(_build_done_envelope("t1", {"messages": ["ok"]}))
        self._stop(shard)

        watcher._recv_loop(shard)

        self.assertEqual(futures["t1"].result(timeout=1.0), {"messages": ["ok"]})
        watcher.stop()


class TestDragonWatcherRealCodecAsync(_WatcherHarness, IsolatedAsyncioTestCase):
    """The same decode-failure attribution, but for an asyncio Future.

    Async futures are resolved from the recv thread via ``call_soon_threadsafe``,
    a different path from the sync one, so the failure has to be covered twice.
    """

    async def test_undecodable_result_fails_an_asyncio_future(self):
        from dragon.ai.langgraph.constants import pack_envelope
        from dragon.ai.langgraph.watcher import _PendingTask, _STOP_TASK_ID

        watcher = self._make_watcher()
        shard = watcher._shards[0]
        shard.thread.join(timeout=5)
        shard.queue = _RealCodecQueue()

        loop = asyncio.get_running_loop()
        future = loop.create_future()
        shard.slots.acquire()
        with shard.lock:
            shard.pending["t1"] = _PendingTask(
                future=future, task_id="t1", node_name="writer", loop=loop
            )

        shard.queue._frames.append(pack_envelope("t1", b"not-a-pickle"))
        shard.queue.put((_STOP_TASK_ID, b""))

        # The recv loop runs on its own thread in production; do the same here so
        # call_soon_threadsafe is genuinely a cross-thread hand-off.
        await loop.run_in_executor(None, watcher._recv_loop, shard)

        with self.assertRaises(RuntimeError) as ctx:
            await asyncio.wait_for(future, timeout=1.0)
        self.assertIn("deserialized", str(ctx.exception))
        watcher.stop()


# ========================================================================
# DragonWatcher — the recv lane must outlive a poisoned message
# ========================================================================

class TestDragonWatcherRecvLoopSurvival(_WatcherHarness, TestCase):
    """A shard's recv thread is the only thing resolving that lane's futures.

    If it dies, every later task on the shard hangs until its own
    ``event_timeout`` and nothing says why — so resolution failures must be
    logged loudly and the loop must keep going.
    """

    def _watcher_with_queue(self, **kwargs):
        watcher = self._make_watcher(**kwargs)
        shard = watcher._shards[0]
        shard.thread.join(timeout=5)
        shard.queue = _RealCodecQueue()
        return watcher, shard

    def _register(self, shard, task_id, loop=None):
        from dragon.ai.langgraph.watcher import _PendingTask

        shard.slots.acquire()
        future = concurrent.futures.Future()
        with shard.lock:
            shard.pending[task_id] = _PendingTask(
                future=future, task_id=task_id, node_name="writer", loop=loop
            )
        return future

    def test_resolution_failure_is_logged_and_the_loop_continues(self):
        """A raising resolve is reported at error level, not swallowed."""
        from dragon.ai.langgraph.host import _build_done_envelope
        from dragon.ai.langgraph.watcher import _STOP_TASK_ID

        watcher, shard = self._watcher_with_queue()
        self._register(shard, "t1")
        good = self._register(shard, "t2")
        shard.queue.put(_build_done_envelope("t1", {"messages": ["boom"]}))
        shard.queue.put(_build_done_envelope("t2", {"messages": ["ok"]}))
        shard.queue.put((_STOP_TASK_ID, b""))

        real = watcher._handle_envelope

        def explode(sh, task_id, envelope):
            if task_id == "t1":
                raise RuntimeError("resolve boom")
            return real(sh, task_id, envelope)

        with patch.object(watcher, "_handle_envelope", side_effect=explode), \
             self.assertLogs("LANGGRAPH.watcher", level="ERROR") as logs:
            watcher._recv_loop(shard)

        message = "\n".join(logs.output)
        self.assertIn("t1", message)
        self.assertIn("resolve boom", message)
        # The lane survived: the next task still resolved.
        self.assertEqual(good.result(timeout=1.0), {"messages": ["ok"]})
        watcher.stop()

    def test_undecodable_handler_failure_does_not_kill_the_lane(self):
        """The decode-failure branch is guarded too, not just the happy one."""
        from dragon.ai.langgraph.constants import pack_envelope
        from dragon.ai.langgraph.host import _build_done_envelope
        from dragon.ai.langgraph.watcher import _STOP_TASK_ID

        watcher, shard = self._watcher_with_queue()
        self._register(shard, "t1")
        good = self._register(shard, "t2")
        shard.queue._frames.append(pack_envelope("t1", b"not-a-pickle"))
        shard.queue.put(_build_done_envelope("t2", {"messages": ["ok"]}))
        shard.queue.put((_STOP_TASK_ID, b""))

        with patch.object(
            watcher, "_fail_undecodable", side_effect=RuntimeError("unwind boom")
        ), self.assertLogs("LANGGRAPH.watcher", level="ERROR"):
            watcher._recv_loop(shard)

        self.assertEqual(good.result(timeout=1.0), {"messages": ["ok"]})
        watcher.stop()

    def test_closed_coordinator_loop_does_not_kill_the_lane(self):
        """The realistic case: graph.ainvoke finished and closed its loop while
        a completion was still in flight. call_soon_threadsafe then raises."""
        from dragon.ai.langgraph.host import _build_done_envelope
        from dragon.ai.langgraph.watcher import _PendingTask, _STOP_TASK_ID

        watcher, shard = self._watcher_with_queue()

        dead_loop = asyncio.new_event_loop()
        dead_loop.close()
        shard.slots.acquire()
        with shard.lock:
            shard.pending["t1"] = _PendingTask(
                future=MagicMock(),
                task_id="t1",
                node_name="writer",
                loop=dead_loop,
            )
        good = self._register(shard, "t2")

        shard.queue.put(_build_done_envelope("t1", {"messages": ["late"]}))
        shard.queue.put(_build_done_envelope("t2", {"messages": ["ok"]}))
        shard.queue.put((_STOP_TASK_ID, b""))

        with self.assertLogs("LANGGRAPH.watcher", level="ERROR") as logs:
            watcher._recv_loop(shard)

        self.assertIn("t1", "\n".join(logs.output))
        self.assertEqual(good.result(timeout=1.0), {"messages": ["ok"]})
        watcher.stop()

    def test_transport_error_is_logged_and_retried(self):
        """A queue-level failure must not spin silently; it logs and backs off."""
        from dragon.ai.langgraph.watcher import _STOP_TASK_ID

        watcher, shard = self._watcher_with_queue()
        shard.queue = MagicMock()
        shard.queue.get.side_effect = [
            RuntimeError("channel went away"),
            (_STOP_TASK_ID, None, None),
        ]

        with self.assertLogs("LANGGRAPH.watcher", level="ERROR") as logs:
            watcher._recv_loop(shard)

        self.assertIn("channel went away", "\n".join(logs.output))
        watcher.stop()

    def test_transport_error_during_stop_exits_quietly(self):
        """Tearing the queue down under a blocked get() is expected, not an
        error worth reporting."""
        watcher, shard = self._watcher_with_queue()
        shard.queue = MagicMock()

        def die_on_stop(timeout=None):
            watcher._stopped = True
            raise RuntimeError("queue destroyed")

        shard.queue.get.side_effect = die_on_stop

        with patch("dragon.ai.langgraph.watcher.logger") as mock_logger:
            watcher._recv_loop(shard)

        mock_logger.error.assert_not_called()
        watcher.stop()


# ========================================================================
# DragonWatcher.stop idempotency
# ========================================================================

class TestDragonWatcherStopIdempotency(_WatcherHarness, TestCase):
    """Verify stop() can be called multiple times safely."""

    def test_stop_twice_is_noop(self):
        """A second stop() returns immediately and does not raise."""
        watcher = self._make_watcher()
        watcher.stop()
        # Second call must be a safe no-op.
        watcher.stop()
        self.assertTrue(watcher._stopped)


if __name__ == "__main__":
    mp.set_start_method("dragon")
    main()
