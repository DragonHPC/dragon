"""Integration test — get_llm builds and memoizes a real Dragon Inference proxy.

Exercises the real wiring that the unit tests mock:
- a real ``dragon.native.queue.Queue`` handle,
- the real ``from ..inference import DragonQueueLLMProxy`` import path,
- real ``serialize()``-based memoization,
- real host-exit cleanup (``_close_all_llms``).

It does NOT call ``.chat()`` (that needs a running vLLM pipeline / GPUs); proxy
construction is GPU-free because the response pool is lazy.  Skipped where the
inference stack (vLLM) is not importable.

Run with:  dragon python -m unittest \
    test.ai.langgraph.integration_tests.test_get_llm -v
"""

import dragon  # noqa: F401 — activates Dragon runtime
import asyncio
import multiprocessing as mp

from unittest import TestCase, main, skipUnless

from dragon.native.queue import Queue
from dragon.ai.langgraph import get_llm
from dragon.ai.langgraph import llm as _llm

# Proxy construction needs the inference package (which pulls vLLM). Skip the
# whole module gracefully where it is not installed.
try:
    from dragon.ai.inference import DragonQueueLLMProxy  # noqa: F401
    _HAVE_INFERENCE = True
except Exception:  # noqa: BLE001
    _HAVE_INFERENCE = False


@skipUnless(_HAVE_INFERENCE, "dragon.ai.inference (vLLM) not available")
class TestGetLLMIntegration(TestCase):
    """Verify get_llm against real Dragon Queues and a real proxy."""

    def setUp(self) -> None:
        _llm._proxies.clear()
        self._queues: list = []

    def tearDown(self) -> None:
        # Cleanup: drop cached proxies, then destroy the queues we created.
        asyncio.run(_llm._close_all_llms())
        _llm._proxies.clear()
        for q in self._queues:
            try:
                q.destroy()
            except Exception:  # noqa: BLE001
                pass

    def _new_queue(self) -> Queue:
        q = Queue()
        self._queues.append(q)
        return q

    def test_builds_real_proxy_and_memoizes(self):
        """get_llm builds a real proxy bound to the queue, and reuses it."""
        q = self._new_queue()

        proxy = get_llm(q)
        self.assertIsInstance(proxy, DragonQueueLLMProxy)
        self.assertIs(proxy.input_queue, q)          # bound to the queue we passed

        again = get_llm(q)
        self.assertIs(again, proxy)                  # same pipeline → same proxy

    def test_distinct_queues_get_distinct_proxies(self):
        """Two different pipelines (queues) get their own proxies."""
        qa, qb = self._new_queue(), self._new_queue()

        proxy_a = get_llm(qa)
        proxy_b = get_llm(qb)

        self.assertIsNot(proxy_a, proxy_b)
        self.assertIs(proxy_a.input_queue, qa)
        self.assertIs(proxy_b.input_queue, qb)

    def test_proxy_kwargs_applied_on_first_build(self):
        """max_concurrent_requests is honored on the first build for a queue."""
        q = self._new_queue()

        proxy = get_llm(q, max_concurrent_requests=4)
        # The response pool is sized to the requested concurrency.
        self.assertEqual(proxy._response_pool._pool_size, 4)

    def test_close_all_llms_clears_cache(self):
        """Host-exit cleanup drops every cached proxy."""
        get_llm(self._new_queue())
        get_llm(self._new_queue())
        self.assertEqual(len(_llm._proxies), 2)

        asyncio.run(_llm._close_all_llms())
        self.assertEqual(_llm._proxies, {})


if __name__ == "__main__":
    mp.set_start_method("dragon")
    main()
