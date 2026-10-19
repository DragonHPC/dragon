"""DragonWatcher — resolves pending task futures from result envelopes arriving
on one or more shared completion Queues.

Each agent host puts its result envelope on a shared Queue; a dedicated thread
blocks in ``get``, decodes, and resolves the matching Future by ``task_id``. One
lane by default. Raising ``num_shards`` splits the watcher into independent
lanes — each its own {Queue, recv thread, semaphore, pending table} —
partitioned by ``hash(task_id) % num_shards``, so the serial receive → decode →
resolve step parallelizes with no cross-shard locking.

Concurrency limit
-----------------
``max_concurrent_tasks`` (default 64) caps in-flight tasks, enforced by a
per-shard semaphore. Because in-flight tasks are capped and each completion
Queue is sized to that cap plus headroom, a finishing host can never stall on a
full queue even if every in-flight task completes at once. Lifetime task count
is unlimited — only peak concurrency matters, so size the cap to your expected
fan-out.

Bulk data does not belong here. Store tensors, arrays and datasets in a
user-managed Dragon DDict and return only a handle through LangGraph state, so
bulk bytes never funnel through a recv thread.
"""

from __future__ import annotations

import concurrent.futures
import queue as _queue
import threading
import uuid
from typing import Any

from ...native.queue import Queue

from .logging import get_langgraph_logger
from .constants import (
    ENV_PAYLOAD,
    ENV_STATUS,
    STATUS_DONE,
    STATUS_ERROR,
    EnvelopePickler,
)

logger = get_langgraph_logger("watcher")

# Completion-queue block size.  Small inline envelopes fit in a single block;
# larger ones sideload from the default managed-memory pool.
_QUEUE_BLOCK_SIZE = 64 * 1024

# Sentinel task_id used by stop() to wake the blocked recv loop.
_STOP_TASK_ID = "__dragon_watcher_stop__"


class _PendingTask:
    """Bookkeeping for a single in-flight task, keyed by ``task_id``."""

    __slots__ = (
        "future",
        "task_id",
        "node_name",
        "loop",
    )

    def __init__(
        self,
        future: Any,
        task_id: str,
        node_name: str,
        loop: Any = None,
    ) -> None:
        self.future = future
        self.task_id = task_id
        self.node_name = node_name
        # asyncio event loop if this is an async task; None for sync.
        self.loop = loop


class _Shard:
    """One independent completion lane: its own queue, recv thread,
    backpressure semaphore, and pending-task table.

    Sharding the watcher parallelizes the otherwise-serial per-message route
    step (``get`` + decode + Future resolve) across ``num_shards`` threads.
    Tasks are partitioned by ``hash(task_id) % num_shards``, so each shard owns
    a disjoint slice of the task space and no cross-shard locking is needed.
    """

    __slots__ = (
        "index",
        "queue",
        "serialized_queue",
        "slots",
        "pending",
        "lock",
        "thread",
    )

    def __init__(self, index: int) -> None:
        self.index = index
        self.queue: Any = None
        self.serialized_queue: bytes = b""
        self.slots: threading.BoundedSemaphore | None = None
        self.pending: dict[str, _PendingTask] = {}
        self.lock = threading.Lock()
        self.thread: threading.Thread | None = None


class DragonWatcher:
    """Resolves all task futures over one or more shared completion Queues.

    Usage::

        watcher = DragonWatcher()
        future = watcher.submit_task(node_name, state, serialized_input_queue)
        result = future.result()  # blocks until agent finishes
        watcher.stop()

    :param max_concurrent_tasks: Maximum number of concurrent in-flight tasks
        across the whole watcher. When this many tasks are in flight, a further
        dispatch waits for one to complete (backpressure) — the sync path
        blocks its calling thread, the async path awaits without blocking the
        event loop. Split evenly across shards. Defaults to 64.
    :type max_concurrent_tasks: int, optional
    :param num_shards: Number of independent completion lanes (queue + recv
        thread). Each shard routes ~``1/num_shards`` of the tasks,
        parallelizing the per-message decode/resolve work. Since tasks hash to
        shards randomly, the per-shard cap bites before the global one, so peak
        concurrency falls short of ``max_concurrent_tasks``; raise both
        together. Defaults to 1 (single lane, the original behavior).
    :type num_shards: int, optional
    :raises ValueError: If ``num_shards`` is less than 1.
    """

    def __init__(
        self,
        *,
        max_concurrent_tasks: int = 64,
        num_shards: int = 1,
    ) -> None:
        if num_shards < 1:
            raise ValueError("num_shards must be >= 1")

        self._stopped = False
        self._max_concurrent_tasks = max_concurrent_tasks
        self._num_shards = num_shards

        # Per-shard backpressure budget (at least 1 slot each).
        per_shard_ring = max(max_concurrent_tasks // num_shards, 1)
        # Set once, the first time backpressure actually blocks a dispatch, so
        # the concurrency cap being hit is observable instead of a silent stall.
        self._throttle_warned = False

        # Holds a full burst of completions plus room for the stop() sentinel;
        # the semaphore caps in-flight tasks at per_shard_ring, so a finishing
        # host can never block on a full queue.
        capacity = max(per_shard_ring + 8, 16)

        self._shards: list[_Shard] = []
        for s in range(num_shards):
            shard = _Shard(index=s)
            shard.queue = Queue(
                maxsize=capacity,
                block_size=_QUEUE_BLOCK_SIZE,
                buffered=True,
                pickler=EnvelopePickler(),
            )
            # Handed to every host whose tasks land on this shard so it can
            # attach and put result envelopes back into this queue.
            shard.serialized_queue = shard.queue.serialize()
            shard.slots = threading.BoundedSemaphore(per_shard_ring)
            shard.thread = threading.Thread(
                target=self._recv_loop,
                args=(shard,),
                name=f"dragon-watcher-{s}",
                daemon=True,
            )
            shard.thread.start()
            self._shards.append(shard)

    @property
    def max_concurrent_tasks(self) -> int:
        """Maximum number of tasks allowed in flight at once (across shards)."""
        return self._max_concurrent_tasks

    @property
    def num_shards(self) -> int:
        """Number of independent completion lanes."""
        return self._num_shards

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def submit_task(
        self,
        node_name: str,
        state: Any,
        serialized_input_queue: bytes,
    ) -> concurrent.futures.Future:
        """Dispatch a task to the host and return a Future for its result.

        Non-blocking on the result — the task is dispatched immediately and
        the Future is resolved by the watcher thread when the host signals
        completion.  May block the calling thread briefly for backpressure if
        ``max_concurrent_tasks`` tasks are already in flight.  Used by the
        synchronous node path, where blocking the caller's
        ``BackgroundExecutor`` thread is the intended behavior.
        """
        future: concurrent.futures.Future = concurrent.futures.Future()
        task_id = str(uuid.uuid4())
        shard = self._shards[hash(task_id) % self._num_shards]
        self._acquire_slot_blocking(shard)
        self._register_and_dispatch(
            future, task_id, shard, node_name, state, serialized_input_queue, loop=None
        )
        return future

    async def submit_task_async(
        self,
        node_name: str,
        state: Any,
        serialized_input_queue: bytes,
    ) -> "asyncio.Future":
        """Async variant of :meth:`submit_task` — awaitable end to end.

        Returns an ``asyncio.Future`` bound to the *running* event loop.
        Awaiting it suspends the calling coroutine (no thread is parked) and
        the watcher resolves it via ``loop.call_soon_threadsafe`` when the host
        signals completion.  This is what lets ``graph.ainvoke`` run N
        concurrent agents with zero per-node threads.

        Backpressure is **awaited, not blocked**: if the concurrency cap is
        already reached, the wait for a free slot is offloaded to a worker
        thread (see :meth:`_acquire_slot_async`) so the event loop keeps
        turning — resolving in-flight completions, which is exactly what frees
        slots — instead of freezing on a synchronous acquire.
        """
        import asyncio

        loop = asyncio.get_running_loop()
        future: asyncio.Future = loop.create_future()
        task_id = str(uuid.uuid4())
        shard = self._shards[hash(task_id) % self._num_shards]
        await self._acquire_slot_async(shard, loop)
        self._register_and_dispatch(
            future, task_id, shard, node_name, state, serialized_input_queue, loop=loop
        )
        return future

    # ------------------------------------------------------------------
    # Backpressure (slot acquisition)
    # ------------------------------------------------------------------

    def _warn_throttle_once(self) -> None:
        """Emit the concurrency-cap warning the first time backpressure blocks."""
        if not self._throttle_warned:
            self._throttle_warned = True
            logger.warning(
                "[dragon-watcher] concurrency cap reached "
                "(max_concurrent_tasks=%d): further task dispatches are "
                "being throttled until in-flight tasks complete. Raise "
                "max_concurrent_tasks to allow more parallel agents.",
                self._max_concurrent_tasks,
            )

    def _acquire_slot_wait(self, shard: _Shard) -> None:
        """Block until a slot frees on ``shard`` (the non-blocking try failed).

        Runs on the *caller's* thread — a ``BackgroundExecutor`` thread on the
        sync path, or a ``run_in_executor`` worker on the async path — never on
        an event loop.  Wakes as soon as a slot is released; the 1s timeout is
        only the re-check interval for the stop flag.
        """
        while not shard.slots.acquire(timeout=1.0):
            if self._stopped:
                raise RuntimeError("DragonWatcher is stopped")

    def _acquire_slot_blocking(self, shard: _Shard) -> None:
        """Acquire a backpressure slot, blocking the calling thread if needed.

        Sync path only.  Blocking the caller's thread here is correct: the
        synchronous node runs inside a LangGraph ``BackgroundExecutor`` thread
        whose job is precisely to park while the task is in flight.
        """
        if shard.slots.acquire(blocking=False):
            return
        self._warn_throttle_once()
        self._acquire_slot_wait(shard)

    async def _acquire_slot_async(self, shard: _Shard, loop: Any) -> None:
        """Acquire a backpressure slot without blocking the event loop.

        Fast path: if a slot is free right now, take it inline with no thread
        hop.  Slow path (cap reached): offload the blocking wait to a worker
        thread and ``await`` it, so the loop stays responsive — it keeps
        resolving completions, and each completion releases a slot — instead of
        freezing on a synchronous ``acquire``.  The backpressure budget is
        still the shard's single ``BoundedSemaphore``, shared with the sync
        path, so the completion-queue-can't-fill invariant is preserved.
        """
        if shard.slots.acquire(blocking=False):
            return
        self._warn_throttle_once()
        await loop.run_in_executor(None, self._acquire_slot_wait, shard)

    # ------------------------------------------------------------------
    # Registration / dispatch
    # ------------------------------------------------------------------

    def _register_and_dispatch(
        self,
        future: Any,
        task_id: str,
        shard: _Shard,
        node_name: str,
        state: Any,
        serialized_input_queue: bytes,
        *,
        loop: Any,
    ) -> None:
        """Register the pending task, then hand the message to the host.

        The task is registered in the shard's ``pending`` table *before* the
        message is handed to the host, so the completion envelope can never
        arrive (and be dropped as unknown) before the shard knows about the
        task.  The caller must already hold a backpressure slot for ``shard``;
        it is released here if dispatch fails.
        """
        pending = _PendingTask(
            future=future,
            task_id=task_id,
            node_name=node_name,
            loop=loop,
        )
        with shard.lock:
            shard.pending[task_id] = pending

        try:
            input_queue = Queue.attach(serialized_input_queue)
            msg = {
                "task_id": task_id,
                "node_name": node_name,
                # The Queue cloudpickles the whole message on put(), so the
                # state rides as a raw object — no manual dumps() needed.
                "state": state,
                "serialized_done_queue": shard.serialized_queue,
            }
            input_queue.put(msg)
        except Exception as exc:  # noqa: BLE001
            # Dispatch failed — unwind registration and slot, fail the Future.
            logger.error(
                "[dragon-watcher] task %s dispatch to node '%s' failed: %s: %s",
                task_id,
                node_name,
                type(exc).__name__,
                exc,
            )
            with shard.lock:
                shard.pending.pop(task_id, None)
            self._release_slot(shard)
            self._set_outcome(pending, exc=exc)
            return

        logger.debug(
            "[dragon-watcher] task %s dispatched for node '%s' (shard %d)",
            task_id,
            node_name,
            shard.index,
        )

    @staticmethod
    def _release_slot(shard: _Shard) -> None:
        """Release one backpressure slot on a shard, ignoring over-release."""
        try:
            shard.slots.release()
        except ValueError:
            # Over-release means slot accounting drifted; the cap still holds.
            logger.error(
                "[dragon-watcher] shard %d slot over-released; "
                "backpressure accounting is inconsistent.",
                shard.index,
            )

    def stop(self) -> None:
        """Stop all watcher threads and explicitly destroy every completion
        Queue this watcher created, so nothing is left for the runtime to
        reclaim.

        Cleanup is **idempotent** (a second call is a no-op) and **ordered** so
        it cannot race the runtime teardown

        1. Each blocked ``get`` is woken with a stop sentinel and its thread is
           joined *first*, so no thread is still touching a queue when it is
           destroyed.  (The executor has already joined/killed the AgentHost
           processes before calling ``stop()``, so no host is still sending
           either.)
        2. Per shard, ``Queue.destroy()`` releases the refcount and asks Global
           Services to destroy the underlying channel, then detaches the pool it
           attached.  Wrapped best-effort so a single failure can neither abort
           the rest nor crash interpreter exit.
        3. A shard whose recv thread did not stop (pathological — never in
           normal operation) is left for the runtime to reclaim rather than
           destroying a queue out from under a live ``get``.
        """
        if self._stopped:
            return
        self._stopped = True

        # 1. Wake each blocked recv loop and join its thread.
        for shard in self._shards:
            self._wake_recv_loop(shard)
        for shard in self._shards:
            if shard.thread is not None:
                shard.thread.join(timeout=5)

        # 2. Tear down each shard's queue.
        for shard in self._shards:
            # Only destroy if the recv thread actually stopped; destroying a
            # queue under a still-blocked get() would crash.
            thread_stopped = shard.thread is None or not shard.thread.is_alive()
            if thread_stopped and shard.queue is not None:
                try:
                    shard.queue.destroy()
                except Exception as exc:  # noqa: BLE001
                    logger.warning(
                        "[dragon-watcher] shard %d queue destroy failed: %s",
                        shard.index,
                        exc,
                    )
            elif not thread_stopped:
                logger.warning(
                    "[dragon-watcher] shard %d recv thread did not stop; "
                    "leaving its queue for the runtime to reclaim.",
                    shard.index,
                )

            shard.queue = None

    @staticmethod
    def _wake_recv_loop(shard: _Shard) -> None:
        """Put a sentinel envelope so a shard's blocked ``get`` returns."""
        try:
            shard.queue.put((_STOP_TASK_ID, None))
        except Exception as exc:  # noqa: BLE001
            # The recv thread will not wake, so stop() leaks this shard's queue.
            logger.warning(
                "[dragon-watcher] shard %d stop sentinel send failed: %s",
                shard.index,
                exc,
            )

    # ------------------------------------------------------------------
    # Receive loop
    # ------------------------------------------------------------------

    def _recv_loop(self, shard: _Shard) -> None:
        """Per-shard watcher loop — block on this shard's completion queue.

        ``get(timeout=None)`` parks the thread in the kernel (zero CPU when
        idle) and returns immediately when any host puts a completion envelope.
        Each message is a reliably-queued result, so nothing is ever lost or
        coalesced — no polling, sweeping, or periodic timeout.
        """
        import time as _time

        while not self._stopped:
            try:
                task_id, envelope, decode_exc = shard.queue.get(timeout=None)
            except _queue.Empty:
                continue
            except ValueError as exc:
                # Unframable message: no readable task_id, so there is no Future
                # to fail; that task can only unblock via its own timeout.
                logger.error(
                    "[dragon-watcher] shard %d: unreadable envelope header, "
                    "one task will not be resolved: %s",
                    shard.index,
                    exc,
                )
                continue
            except Exception as exc:  # noqa: BLE001
                if self._stopped:
                    break
                logger.error(
                    "[dragon-watcher] recv error (shard %d): %s: %s",
                    shard.index,
                    type(exc).__name__,
                    exc,
                )
                _time.sleep(0.05)
                continue

            if task_id == _STOP_TASK_ID:
                break

            # Resolution runs under a guard: an escaping exception (a closed
            # coordinator loop, an already-resolved Future) would kill this
            # thread and silently strand every later task on the shard.
            try:
                if decode_exc is not None:
                    self._fail_undecodable(shard, task_id, decode_exc)
                else:
                    self._handle_envelope(shard, task_id, envelope)
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "[dragon-watcher] shard %d: resolving task %s raised "
                    "%s: %s. The lane stays open, but that task will only "
                    "unblock via its own timeout.",
                    shard.index,
                    task_id,
                    type(exc).__name__,
                    exc,
                )

    def _fail_undecodable(
        self, shard: _Shard, task_id: str, exc: BaseException
    ) -> None:
        """Fail the task whose result arrived but could not be deserialized.

        The agent itself succeeded; only the coordinator's ``loads`` failed —
        typically because the payload's class is not importable in this
        process.  Resolving the Future here turns what would otherwise be a
        hang (or a timeout blamed on a slow agent) into an accurate error.
        """
        with shard.lock:
            pending = shard.pending.pop(task_id, None)

        if pending is None:
            logger.error(
                "[dragon-watcher] undecodable envelope for unknown task_id=%s: %s",
                task_id,
                exc,
            )
            return

        logger.error(
            "[dragon-watcher] task %s for node '%s': result could not be "
            "deserialized: %s: %s",
            task_id,
            pending.node_name,
            type(exc).__name__,
            exc,
        )
        try:
            self._set_outcome(
                pending,
                exc=RuntimeError(
                    f"Dragon agent for node '{pending.node_name}' finished, but "
                    f"its result could not be deserialized in the coordinator "
                    f"process: {type(exc).__name__}: {exc}"
                ),
            )
        finally:
            self._release_slot(shard)

    def _handle_envelope(self, shard: _Shard, task_id: Any, envelope: dict) -> None:
        """Resolve the Future matching an inbound completion envelope."""
        with shard.lock:
            pending = shard.pending.pop(task_id, None)

        if pending is None:
            # Unknown or already-resolved task — nothing to do.
            logger.debug(
                "[dragon-watcher] envelope for unknown task_id=%s", task_id
            )
            return

        try:
            self._resolve_task(pending, envelope)
        finally:
            self._release_slot(shard)

    # ------------------------------------------------------------------
    # Future resolution
    # ------------------------------------------------------------------

    def _resolve_task(self, pending: _PendingTask, envelope: dict) -> None:
        """Resolve the Future from an inline completion envelope."""
        task_id = pending.task_id
        node_name = pending.node_name
        status = envelope.get(ENV_STATUS)

        try:
            if status == STATUS_DONE:
                self._set_outcome(pending, result=envelope.get(ENV_PAYLOAD))
            elif status == STATUS_ERROR:
                self._set_outcome(pending, exc=envelope.get(ENV_PAYLOAD))
            else:
                self._set_outcome(
                    pending,
                    exc=RuntimeError(
                        f"Dragon worker for node '{node_name}' task {task_id} "
                        f"returned unexpected status '{status}'"
                    ),
                )
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "[dragon-watcher] task %s for node '%s' failed to resolve: %s: %s",
                task_id,
                node_name,
                type(exc).__name__,
                exc,
            )
            self._set_outcome(pending, exc=exc)

    def _set_outcome(
        self,
        pending: _PendingTask,
        *,
        result: Any = None,
        exc: BaseException | None = None,
    ) -> None:
        """Resolve a pending Future, safely across thread boundaries.

        ``concurrent.futures.Future`` is thread-safe and can be set
        directly.  ``asyncio.Future`` must only be touched from its own
        loop thread, so we hop over via ``call_soon_threadsafe``.
        """
        loop = pending.loop
        if loop is not None:
            loop.call_soon_threadsafe(self._apply_outcome, pending.future, result, exc)
        else:
            self._apply_outcome(pending.future, result, exc)

    @staticmethod
    def _apply_outcome(future: Any, result: Any, exc: BaseException | None) -> None:
        """Apply a result/exception to a Future, ignoring cancelled ones."""
        try:
            if future.cancelled():
                return
        except Exception as check_exc:  # noqa: BLE001
            logger.debug(
                "[dragon-watcher] future cancelled-check failed: %s", check_exc
            )
        if exc is not None:
            future.set_exception(exc)
        else:
            future.set_result(result)
