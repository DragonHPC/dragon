"""Dragon AgentHost — one persistent process multiplexing several LangGraph
agent functions on a single asyncio event loop.

How many hosts exist and where they land is the caller's choice — one per node
is typical, but placement is whatever Policy ``launch_host()`` is given. Agents
sharing a host share an address space, so in-process state costs nothing to
reuse: LLM client proxies, connections, caches. The host hands its input Queue
back over ``reply_queue``, then dispatches each incoming task and publishes the
result inline on that task's done-Queue. A ``None`` sentinel stops intake and
drains what is in flight.

Concurrency model
-----------------
**Sync** nodes are offloaded to a bounded ``ThreadPoolExecutor``
(``max_threads``). **Async** nodes run as tasks on the shared loop, so
I/O-bound agents interleave at ~zero threads each.

The usual asyncio caveat therefore applies: an ``async def`` node that blocks
the loop — a synchronous call or a long CPU stretch without ``await`` — stalls
every *other* agent on this host until it returns. Wrap blocking work in
``await asyncio.to_thread(...)``. Plain ``def`` nodes are immune; they have
their own thread.

Result transport
----------------
Exactly one small envelope per task — ``task_id``, ``status``, and the result
or exception — sent inline on the done-Queue for the single DragonWatcher
thread to pick up.

Bulk data deliberately does not travel this way. Tensors, arrays and datasets
belong in a user-managed Dragon DDict passed into the agent functions; return
only a handle through LangGraph state. That keeps the watcher a pure signal
router and the coordinator free of bulk materialization.
"""

from __future__ import annotations

import asyncio
import inspect
import queue as _queue
import threading
import traceback
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable

from ...native.queue import Queue

from .logging import get_langgraph_logger, setup_langgraph_logging
from .llm import _close_all_llms
from .constants import (
    ENV_PAYLOAD,
    ENV_STATUS,
    STATUS_DONE,
    STATUS_ERROR,
    EnvelopePickler,
)

logger = get_langgraph_logger("agent_host")

_SHUTDOWN = None

# Dragon Queue default capacity, used when the host is driven directly (outside
# DragonExecutor) and so is not told the coordinator's in-flight cap.
_DEFAULT_INPUT_QUEUE_MAXSIZE = 100
# A few extra input-queue slots above the in-flight cap so the shutdown sentinel
# (and any scheduling slack) always has room even when the queue is otherwise
# full.  Mirrors the completion-queue headroom on the coordinator side.
_INPUT_QUEUE_HEADROOM = 8

# Size of the dedicated executor for blocking Dragon IPC (the input Queue.get
# and completion-Queue puts), kept separate from the sync-node body pool so a
# burst of long-running sync nodes can never starve task intake or result
# delivery.
_IO_POOL_SIZE = 8

# Seconds to wait for in-flight tasks to finish on shutdown before cancelling
# the stragglers (each cancelled task still emits an error envelope, so the
# coordinator never hangs).
_DRAIN_TIMEOUT = 30.0

# Bound on a completion put, so a wedged coordinator cannot pin an io_pool
# thread forever; on expiry the coordinator falls back to its own timeout.
_DONE_PUT_TIMEOUT = 30.0

# One attached completion Queue per shard descriptor, reused across tasks:
# attaching per task would cost an FLI attach on every completion.
_done_queues: dict[bytes, Any] = {}
_done_queues_lock = threading.Lock()


def _input_queue_maxsize(input_maxsize: int | None) -> int:
    """Number of message slots to give the host's input Queue.

    The coordinator caps *in-flight* tasks at ``max_concurrent_tasks`` (its
    backpressure semaphore).  Worst case, every one of those tasks routes to this
    single host, so the input queue must be able to hold that many messages —
    otherwise the coordinator's blocking ``put()`` would stall on queue capacity,
    a second (uncoordinated) backpressure point that could also block the async
    event loop.  We therefore size the queue to the in-flight cap plus a little
    headroom for the shutdown sentinel, so ``put()`` only ever waits on the
    coordinator's own semaphore, never on the queue.

    :param input_maxsize: The coordinator's ``max_concurrent_tasks`` (the
        in-flight cap). ``None`` falls back to the Dragon Queue default, for
        hosts launched outside :class:`~dragon.ai.langgraph.DragonExecutor`.
    :type input_maxsize: int, optional
    :returns: The ``maxsize`` to pass to the input Queue.
    :rtype: int
    """
    if input_maxsize is None:
        return _DEFAULT_INPUT_QUEUE_MAXSIZE
    return max(input_maxsize + _INPUT_QUEUE_HEADROOM, 16)


def agent_host_entry(
    agents: dict[str, Callable[..., Any]],
    reply_queue: Any,
    *,
    max_threads: int | None = None,
    input_maxsize: int | None = None,
) -> None:
    """Entry point executed inside a Dragon host process.

    :param agents: Mapping of ``node_name`` to ``fn(state) -> dict``. All
        agents in this dict run in this single process. Both plain ``def`` and
        ``async def`` node functions are supported — sync nodes run on a bounded
        thread pool; async nodes run as concurrent tasks on the host's shared
        event loop.
    :type agents: dict[str, Callable]
    :param reply_queue: Dragon Queue for the startup handshake — the host sends
        its input queue handle here so the coordinator can dispatch tasks.
    :type reply_queue: dragon.native.queue.Queue
    :param max_threads: Maximum concurrent **sync** node bodies — the size of
        the thread pool that runs plain ``def`` nodes. ``None`` defaults to
        ``max(len(agents) * 4, 8)``. Async (``async def``) nodes are *not*
        bounded by this; they run as tasks on the host's shared event loop
        (bounded only by the coordinator's ``max_concurrent_tasks``). Defaults
        to None.
    :type max_threads: int, optional
    :param input_maxsize: The coordinator's ``max_concurrent_tasks`` (the
        in-flight cap), used to size this host's input Queue so a dispatched
        ``put()`` never blocks on queue capacity (see :func:`_input_queue_maxsize`).
        ``None`` uses the Dragon Queue default — for hosts driven outside
        ``DragonExecutor``. Defaults to None.
    :type input_maxsize: int, optional
    """
    # Configure Dragon-native three-tier logging for this AgentHost process.
    setup_langgraph_logging(label=next(iter(agents), None))

    # Sized to the coordinator's in-flight cap so its blocking put() never
    # stalls on queue capacity — backpressure stays solely on the semaphore.
    input_queue: Any = Queue(maxsize=_input_queue_maxsize(input_maxsize))
    reply_queue.put(input_queue.serialize())

    if max_threads is None:
        # Heuristic default for the SYNC-node thread pool: ~4 threads per
        # co-located agent (sync I/O-bound nodes mostly wait, so oversubscribe to
        # overlap the waits), floored at 8. len(agents) is only a proxy — if you
        # fan one sync node out widely, pass max_threads explicitly. (Async nodes
        # do not draw from this pool; they run on the shared loop.)
        max_threads = max(len(agents) * 4, 8)

    logger.info(
        "[agent-host] ready — agents=%s, max_threads=%d",
        list(agents.keys()),
        max_threads,
    )

    # Run the host on a single event loop (mirrors SubAgent.listen): async node
    # functions run as concurrent tasks on the loop; sync node functions are
    # offloaded to a bounded thread pool so their blocking work never stalls it.
    try:
        asyncio.run(_host_listen(agents, input_queue, max_threads))
    finally:
        # Every task has drained (see _host_listen); destroy the host's own
        # input queue so no Dragon resource is left for the runtime to reclaim.
        try:
            input_queue.destroy()
        except Exception as exc:  # noqa: BLE001
            logger.warning("[agent-host] input_queue destroy failed: %s", exc)


def _log_task_exception(task: "asyncio.Task") -> None:
    """Done-callback: log any exception that escaped a node task.

    ``_handle`` already turns node failures into error envelopes, so this is a
    defensive net for unexpected escapes — asyncio never propagates a task's
    exception to its peers, and this keeps it from being silently lost.
    """
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        logger.error(
            "[agent-host] unhandled task exception: %s: %s",
            type(exc).__name__, exc,
        )


async def _host_listen(
    agents: dict[str, Callable[..., Any]],
    input_queue: Any,
    max_threads: int,
) -> None:
    """Single-loop dispatch: receive tasks and run each as a concurrent task.

    Async nodes run directly on this loop; sync nodes are offloaded to
    ``body_pool``. Blocking Dragon IPC (the input ``Queue.get`` and the
    completion-Queue puts) runs on a separate small ``io_pool`` so a burst of
    long sync nodes can never starve intake or result delivery. On the ``None``
    sentinel the loop stops accepting work and drains in-flight tasks.
    """
    loop = asyncio.get_running_loop()
    body_pool = ThreadPoolExecutor(
        max_workers=max_threads, thread_name_prefix="agent-body"
    )
    io_pool = ThreadPoolExecutor(
        max_workers=_IO_POOL_SIZE, thread_name_prefix="agent-io"
    )
    inflight: set[asyncio.Task] = set()
    try:
        while True:
            # Offload the blocking get so the loop stays free to run node tasks;
            # the coordinator's None sentinel unblocks it at shutdown.
            msg = await loop.run_in_executor(io_pool, input_queue.get)
            if msg is _SHUTDOWN:
                logger.info("[agent-host] shutdown received, exiting")
                break
            task = asyncio.create_task(
                _handle(agents, msg, loop, body_pool, io_pool)
            )
            inflight.add(task)
            task.add_done_callback(inflight.discard)
            task.add_done_callback(_log_task_exception)
    finally:
        await _drain(inflight)
        # Best-effort: shut down any per-host LLM proxies built via get_llm()
        # while the loop is still running, so their response queues are freed.
        await _close_all_llms()
        _close_done_queues()
        body_pool.shutdown(wait=True)
        io_pool.shutdown(wait=True)


async def _drain(inflight: "set[asyncio.Task]") -> None:
    """Wait for in-flight tasks to finish; cancel stragglers past the timeout.

    A cancelled task still sends a best-effort error envelope (see ``_handle``),
    so the coordinator's Future fails rather than hanging.
    """
    if not inflight:
        return
    logger.info("[agent-host] draining %d in-flight task(s)…", len(inflight))
    _done, pending = await asyncio.wait(inflight, timeout=_DRAIN_TIMEOUT)
    if pending:
        logger.warning(
            "[agent-host] %d task(s) did not finish within %.0fs; cancelling",
            len(pending), _DRAIN_TIMEOUT,
        )
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)


async def _handle(
    agents: dict[str, Callable[..., Any]],
    msg: dict,
    loop: Any,
    body_pool: ThreadPoolExecutor,
    io_pool: ThreadPoolExecutor,
) -> None:
    """Run one node task and publish exactly one result envelope inline.

    Runs as its own ``asyncio.Task``, so any exception here is confined to this
    task and can never crash the loop or other agents. A node failure is caught
    and turned into an error envelope; a result that will not pickle is turned
    into one by :class:`~dragon.ai.langgraph.constants.EnvelopePickler`. The
    only way a result does not reach the coordinator is a broken reply queue or
    a failed put, in which case the coordinator falls back to its
    ``event_timeout``.
    """
    task_id: str = msg["task_id"]
    node_name = msg.get("node_name")
    done_q = None
    try:
        # Reconstruct the reply queue first, so a failure anywhere below can
        # still be reported back to the coordinator.
        done_q = _get_done_queue(msg["serialized_done_queue"])
        fn = agents.get(node_name)
        if fn is None:
            raise ValueError(f"Unknown node: {node_name}")
        # The Queue already deserialized the message, so state is a live object.
        state: Any = msg["state"]
        if inspect.iscoroutinefunction(fn):
            # Async node: run on the shared loop (scales; ~zero threads each).
            result = await fn(state)
        else:
            # Sync node: offload to a worker thread so blocking work is isolated.
            result = await loop.run_in_executor(body_pool, fn, state)
            if inspect.iscoroutine(result):
                # Sync callable whose __call__ is async — drive its coroutine.
                result = await result
        if inspect.isawaitable(result):
            raise TypeError(
                f"Node '{node_name}' returned a non-coroutine awaitable "
                f"({type(result).__name__}), which the host cannot run. Return a "
                f"plain value or an async def node."
            )
        envelope = _build_done_envelope(task_id, result)
        logger.debug("[agent-host:%s] task %s done", node_name, task_id)
    except asyncio.CancelledError:
        # Shutdown drain cancelled us mid-flight — tell the coordinator so its
        # Future fails instead of hanging, then let cancellation propagate.
        await _try_send(
            loop, io_pool, done_q,
            _build_error_envelope(task_id, RuntimeError("agent host shutting down")),
            task_id, node_name,
        )
        raise
    except Exception as exc:  # noqa: BLE001
        logger.error(
            "[agent-host:%s] task %s raised:\n%s",
            node_name, task_id, traceback.format_exc(),
        )
        envelope = _build_error_envelope(task_id, exc)

    await _try_send(loop, io_pool, done_q, envelope, task_id, node_name)


async def _try_send(
    loop: Any,
    io_pool: ThreadPoolExecutor,
    done_q: Any,
    envelope: tuple[str, dict],
    task_id: str,
    node_name: Any,
) -> None:
    """Put a completion envelope on the reply queue (best-effort).

    The Queue put is blocking IPC, so it is offloaded to ``io_pool``. If the
    reply queue is missing or the put fails, we log and give up — the
    coordinator unblocks via its ``event_timeout``.
    """
    if done_q is None:
        logger.error(
            "[agent-host:%s] task %s: no reply queue; coordinator will time out",
            node_name, task_id,
        )
        return
    try:
        await loop.run_in_executor(io_pool, _send_envelope, done_q, envelope)
    except Exception as exc:  # noqa: BLE001
        logger.error(
            "[agent-host:%s] task %s: send failed (%s); coordinator will time out",
            node_name, task_id, exc,
        )


def _build_done_envelope(task_id: str, result: Any) -> tuple[str, dict]:
    """Build a success envelope carrying the result inline.

    The agent's return value rides inline on the done-Queue.  Bulk data
    (tensors, arrays, datasets) should not be returned directly — store it in
    a user-managed DDict and return only a handle/key in the result dict.

    :param task_id: Identifier of the task being completed.
    :type task_id: str
    :param result: The node's return value.
    :returns: The task id and its envelope body.
    :rtype: tuple[str, dict]
    """
    return task_id, {
        ENV_STATUS: STATUS_DONE,
        ENV_PAYLOAD: result,
    }


def _build_error_envelope(task_id: str, exc: BaseException) -> tuple[str, dict]:
    """Build an error envelope carrying the exception inline.

    An exception object that cannot itself be pickled is replaced with a plain
    ``RuntimeError`` by the Queue's
    :class:`~dragon.ai.langgraph.constants.EnvelopePickler`, so the
    coordinator always receives an attributable failure.

    :param task_id: Identifier of the task being completed.
    :type task_id: str
    :param exc: The exception to report.
    :type exc: BaseException
    :returns: The task id and its envelope body.
    :rtype: tuple[str, dict]
    """
    return task_id, {
        ENV_STATUS: STATUS_ERROR,
        ENV_PAYLOAD: exc,
    }


def _get_done_queue(sdesc: bytes) -> Any:
    """Attach (once) to the completion Queue for a shard descriptor.

    :param sdesc: The serialized descriptor of the shard's completion Queue.
    :type sdesc: bytes
    :returns: The attached Queue, shared by every task routed to that shard.
    :rtype: dragon.native.queue.Queue
    """
    done_q = _done_queues.get(sdesc)
    if done_q is not None:
        return done_q
    with _done_queues_lock:
        done_q = _done_queues.get(sdesc)
        if done_q is None:
            done_q = Queue.attach(sdesc, pickler=EnvelopePickler())
            _done_queues[sdesc] = done_q
    return done_q


def _close_done_queues() -> None:
    """Release every attached completion Queue at host shutdown.

    ``close()``, never ``destroy()`` — an attached Queue does not participate in
    the refcount, so destroying it would tear down the coordinator's channel.
    """
    with _done_queues_lock:
        for sdesc, done_q in _done_queues.items():
            try:
                done_q.close()
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "[agent-host] completion queue %r close failed: %s", sdesc, exc
                )
        _done_queues.clear()


def _send_envelope(done_q: Any, envelope: tuple[str, dict]) -> None:
    """Put a completion envelope on the done-Queue."""
    try:
        done_q.put(envelope, timeout=_DONE_PUT_TIMEOUT)
    except _queue.Full as exc:
        raise RuntimeError(
            f"completion queue still full after {_DONE_PUT_TIMEOUT}s"
        ) from exc
