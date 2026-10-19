"""Per-process LLM-proxy accessor for agent node functions.

A ``DragonQueueLLMProxy`` holds a loop-bound ``ResponseQueuePool``, so it must be
built in the process — and on the event loop — that will use it.

Hence an accessor: pass the inference *queue handle* (picklable, not loop-bound)
into your node via its closure, and call :func:`get_llm` inside the node.  The
proxy is built there on first use and cached per queue, so co-located agents on
one model share a response pool while different models stay separate.

This mirrors :class:`~dragon.ai.agent.core.base.DragonAgent`, which builds its
``self.llm`` proxy from ``config.inference_queue`` inside each agent process.
"""

from __future__ import annotations

import threading
from typing import Any

from .logging import get_langgraph_logger

logger = get_langgraph_logger("llm")

# Per-process cache: queue identity -> DragonQueueLLMProxy.  Module globals are
# per-interpreter, so each Dragon host process gets its own cache (and the
# coordinator, which should never call get_llm, gets a separate empty one).
_lock = threading.Lock()
_proxies: dict[Any, Any] = {}


def _make_proxy(inference_queue: Any, **proxy_kwargs: Any) -> Any:
    """Construct a ``DragonQueueLLMProxy`` (import isolated for optional dep).

    The inference package is imported here, lazily, so ``dragon.ai.langgraph``
    has no hard dependency on it for users who never call :func:`get_llm`.
    """
    from ..inference import DragonQueueLLMProxy  # noqa: PLC0415

    return DragonQueueLLMProxy(inference_queue, **proxy_kwargs)


def get_llm(inference_queue: Any, **proxy_kwargs: Any) -> Any:
    """Return the host-process LLM proxy for a Dragon Inference pipeline.

    Builds a :class:`~dragon.ai.inference.DragonQueueLLMProxy` for
    *inference_queue* on first use and caches it **per process**, keyed by the
    queue — so the same pipeline yields the same proxy (a shared response pool)
    and different pipelines yield distinct proxies.

    Call it **inside a node**, so it runs in the host process, and from an
    ``async def`` node so the proxy binds to the host's event loop::

        async def researcher(state):
            resp = await get_llm(input_queue).chat([...])
            return {"research": resp}

    Two models on one host is just two queues::

        async def a(state): return {"x": await get_llm(llama_q).chat([...])}
        async def b(state): return {"y": await get_llm(mistral_q).chat([...])}

    :param inference_queue: The Dragon Queue feeding a Dragon Inference pipeline
        (its ``input_queue``).  Pass the *handle* — not a pre-built proxy, which
        is loop-bound and per-process.
    :type inference_queue: dragon.native.queue.Queue
    :param proxy_kwargs: Forwarded to ``DragonQueueLLMProxy`` on the *first*
        build for this queue (e.g. ``max_concurrent_requests=16``).  Ignored on
        cache hits (the first call wins).
    :returns: A shared, per-process ``DragonQueueLLMProxy`` for *inference_queue*.
    :rtype: dragon.ai.inference.DragonQueueLLMProxy
    :raises ValueError: If *inference_queue* is ``None``.
    """
    if inference_queue is None:
        raise ValueError(
            "get_llm(inference_queue) needs the Dragon Inference pipeline's "
            "input-queue handle; got None. Pass the queue you fed to the "
            "inference service, e.g. get_llm(input_queue)."
        )

    # The serialized descriptor, not id(), so two closures carrying separately
    # unpickled handles to the same pipeline still share one proxy.
    key = inference_queue.serialize()

    # Fast path: already built for this pipeline in this process.
    cached = _proxies.get(key)
    if cached is not None:
        return cached

    # Build-once, even if a sync node (thread pool) and an async node race.
    # Construction is synchronous and cheap (the response pool is empty until
    # first awaited), so holding the lock across it is fine.
    with _lock:
        cached = _proxies.get(key)
        if cached is None:
            cached = _make_proxy(inference_queue, **proxy_kwargs)
            _proxies[key] = cached
            logger.debug("[get_llm] built proxy for a new inference pipeline")
        return cached


async def _close_all_llms() -> None:
    """Shut down every cached proxy's response pool (best-effort, host exit).

    Called from the host's listen loop on shutdown, while the event loop is still
    running, so the pooled response queues are destroyed rather than left for the
    runtime to reclaim.  A failure is reported and then swallowed — process
    teardown proceeds, but the leak is not silent.
    """
    with _lock:
        proxies = list(_proxies.values())
        _proxies.clear()

    for proxy in proxies:
        pool = getattr(proxy, "_response_pool", None)
        if pool is None:
            continue
        try:
            await pool.shutdown()
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "[get_llm] response-pool shutdown failed, leaving its queues "
                "for the runtime to reclaim: %s",
                exc,
            )
