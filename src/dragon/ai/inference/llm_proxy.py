"""LLM Proxy interface and Dragon Queue implementation.

This module defines the transport-agnostic :class:`LLMProxy` interface that
agent code programs against, and provides the :class:`DragonQueueLLMProxy`
concrete implementation backed by Dragon IPC queues.

The interface is intentionally lightweight — no dependency on vLLM, CUDA, or
Dragon ``Policy`` infrastructure — so it can be safely imported in the main
process and in agent processes before any worker spawning.

Contents:
  * :class:`LLMProxy` — abstract chat interface
  * :class:`InferenceRequest` — typed request payload (NamedTuple)
  * :class:`ResponseQueuePool` — reusable pool of minimal Dragon Queues
  * :class:`DragonQueueLLMProxy` — Dragon-Queue-backed implementation
"""

import abc
import asyncio
import logging
import time
from typing import Any, Dict, List, NamedTuple, Optional

log = logging.getLogger(__name__)


class LLMProxy(abc.ABC):
    """Transport-agnostic proxy interface for LLM chat inference.

    Each proxy is a lightweight client-side handle pointing at a shared
    inference pipeline backend.  One pipeline, many proxies.

    Implementations must override :meth:`chat`.
    """

    @abc.abstractmethod
    async def chat(
        self,
        messages: List[Dict[str, Any]],
        tools: Optional[List[Dict[str, Any]]] = None,
        json_schema: Optional[dict] = None,
        continue_final_message: bool = False,
    ) -> str:
        """Send a chat request and return the assistant's response text.

        :param messages: Conversation messages in OpenAI chat format.
        :type messages: list[dict]
        :param tools: Optional tool definitions.
        :type tools: list[dict] | None
        :param json_schema: JSON schema dict for structured output.
            When provided, guided decoding is enabled.
        :type json_schema: dict | None
        :param continue_final_message: Continue last assistant message.
        :type continue_final_message: bool
        :returns: Response text.
        :rtype: str
        """
        ...

    async def shutdown(self) -> None:
        """Release any resources held by this proxy.

        Called once during agent teardown.  The default implementation
        is a no-op; subclasses that hold pooled connections, queues,
        or other heavyweight resources should override this.
        """


# ====================================================================== #
#  Typed request payload                                                   #
# ====================================================================== #

class InferenceRequest(NamedTuple):
    """Typed request sent through the inference input queue.

    Uses :class:`NamedTuple` for zero overhead over a plain tuple —
    same memory layout, same pickle behavior — while providing named
    field access and catching field-ordering bugs at definition time.
    """

    messages: list
    formatted_messages: list
    response_queue: Any
    timestamp: float
    tools: Optional[list] = None
    sampling_override: Optional[dict] = None
    continue_final_message: bool = False


# ====================================================================== #
#  Response-queue pool                                                     #
# ====================================================================== #

class ResponseQueuePool:
    """Bounded pool of reusable, minimal Dragon Queues.

    Lazily allocates up to ``pool_size`` single-slot queues
    (``maxsize=1, block_size=2048``) and hands them out on demand.
    When all queues are in use, :meth:`acquire` awaits until one is
    returned via :meth:`release` — providing natural backpressure
    without an external semaphore.

    Async-safe via :class:`asyncio.Queue`.

    :param pool_size: Maximum number of queues to keep alive.
    :type pool_size: int
    :param block_size: Block size for each pooled queue's backing channel.
    :type block_size: int
    """

    def __init__(self, pool_size: int = 32, block_size: int = 2048) -> None:
        self._pool_size = pool_size
        self._block_size = block_size
        # _idle holds ready-to-use Dragon Queues; filled lazily.
        # Note: asyncio.Queue is thread-safe under Python's GIL (current model).
        # If Python 3.13+ with optional GIL-free mode is adopted, this would
        # need a threading.Lock to synchronize concurrent access across threads.
        self._idle: asyncio.Queue = asyncio.Queue(maxsize=pool_size)
        self._created = 0  # how many queues we have allocated so far

    @property
    def pool_available(self) -> int:
        """Number of idle pooled queues ready for immediate reuse."""
        return self._idle.qsize()

    def _make_queue(self):
        """Create a minimal single-slot Dragon Queue."""
        from ...native.queue import Queue as _DragonQueue

        return _DragonQueue(maxsize=1, block_size=self._block_size)

    async def acquire(self):
        """Return a Dragon Queue from the pool.

        * If an idle queue is available it is returned immediately.
        * If the pool has not yet reached ``pool_size``, a new queue is
          created (off the event loop via ``asyncio.to_thread``).
        * If the pool is exhausted (all queues in use), awaits until
          a queue is returned via :meth:`release`.

        :returns: A Dragon Queue ready for a single get/put cycle.
        :rtype: dragon.native.Queue
        """
        # Fast path: grab an idle queue without blocking.
        if not self._idle.empty():
            return self._idle.get_nowait()

        # Growth path: pool not yet full, create a new queue.
        if self._created < self._pool_size:
            q = await asyncio.to_thread(self._make_queue)
            self._created += 1
            return q

        # Exhausted: await until release() puts one back.
        return await self._idle.get()

    async def release(self, queue) -> None:
        """Return *queue* to the pool for reuse.

        :param queue: The Dragon Queue to release.
        """
        self._idle.put_nowait(queue)

    async def shutdown(self) -> None:
        """Destroy all pooled queues.  Call once during process teardown."""
        while not self._idle.empty():
            q = self._idle.get_nowait()
            try:
                await asyncio.to_thread(q.destroy)
            except Exception:
                pass
        self._created = 0


class DragonQueueLLMProxy(LLMProxy):
    """LLM proxy backed by a Dragon IPC queue.

    Each :meth:`chat` call puts an :class:`InferenceRequest` on
    *input_queue* with a per-call response queue drawn from
    :class:`ResponseQueuePool`, then awaits until the response arrives.

    Concurrency is hard-limited by the pool size: if
    *max_concurrent_requests* calls are already in flight, subsequent
    callers await inside :meth:`ResponseQueuePool.acquire` until a
    response queue is returned — no overflow queues are ever created.

    Designed to be created **per agent process** — each agent owns its
    own proxy and response-queue pool, all pointing at the same shared
    inference pipeline via *input_queue*.

    :param input_queue: Shared request queue consumed by the backend.
    :type input_queue: dragon.native.Queue
    :param max_concurrent_requests: Hard limit on concurrent in-flight
        requests.  Callers beyond this limit await until a slot frees.
        Defaults to ``32``.
    :type max_concurrent_requests: int
    """

    def __init__(self, input_queue, *, max_concurrent_requests: int = 32) -> None:
        self.input_queue = input_queue
        self._response_pool = ResponseQueuePool(pool_size=max_concurrent_requests)

    @property
    def pool_available(self) -> int:
        """Number of idle response queues available for immediate reuse."""
        return self._response_pool.pool_available

    # ------------------------------------------------------------------ #
    #  Internal helpers                                                    #
    # ------------------------------------------------------------------ #

    def _resolve_schema_override(
        self,
        json_schema: Optional[dict],
    ):
        """Return the *json_schema* dict to be sent through the queue.

        The backend will deep-copy its own authoritative ``SamplingParams``
        (which includes ``stop_token_ids``) and attach guided-decoding
        configuration, ensuring a single source of truth.

        Returns ``None`` when *json_schema* is ``None`` (free-form generation).
        """
        return json_schema

    # ------------------------------------------------------------------ #
    #  Public API                                                          #
    # ------------------------------------------------------------------ #

    async def chat(
        self,
        messages: List[Dict[str, Any]],
        tools: Optional[List[Dict[str, Any]]] = None,
        json_schema: Optional[dict] = None,
        continue_final_message: bool = False,
        *,
        sampling_params_override=None,
    ) -> str:
        """Send a chat request via Dragon Queue and return the response.

        :param messages: Conversation messages in OpenAI chat format.
        :type messages: list[dict]
        :param tools: Optional tool definitions.
        :type tools: list[dict] | None
        :param json_schema: JSON schema dict for structured output.
            When provided, guided decoding is enabled.
        :type json_schema: dict | None
        :param continue_final_message: Continue last assistant message.
        :type continue_final_message: bool
        :param sampling_params_override: Explicit ``SamplingParams``
            override.  Takes precedence over *json_schema*.
        :type sampling_params_override: SamplingParams | None
        :returns: Response text.
        :rtype: str
        :raises Exception: Re-raises any exception returned by the backend.
        """
        # Resolve the effective schema override
        effective_override = sampling_params_override
        if effective_override is None:
            effective_override = self._resolve_schema_override(json_schema)

        # Acquire a response queue (blocks if pool exhausted).
        response_queue = await self._response_pool.acquire()

        try:
            request = InferenceRequest(
                messages=messages,
                formatted_messages=messages,
                response_queue=response_queue,
                timestamp=time.time(),
                tools=tools,
                sampling_override=effective_override,
                continue_final_message=continue_final_message,
            )
            await asyncio.to_thread(self.input_queue.put, request)
            result = await asyncio.to_thread(response_queue.get)
        except Exception:
            log.exception("DragonQueueLLMProxy: LLM request failed")
            raise
        finally:
            # Always return the queue to the pool.
            await self._response_pool.release(response_queue)

        # Re-raise if the backend returned an exception object.
        if isinstance(result, Exception):
            raise result

        # The backend may return a dict with an "assistant" key or a plain
        # string.  Normalize to str here so callers never need to check.
        if isinstance(result, dict):
            return result.get("assistant", str(result))
        return str(result)

    async def shutdown(self) -> None:
        """Destroy all pooled response queues."""
        await self._response_pool.shutdown()
