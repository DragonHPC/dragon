"""Tests for get_llm — per-host Dragon Inference proxy accessor."""

import dragon
import multiprocessing as mp

from unittest import TestCase, IsolatedAsyncioTestCase, main
from unittest.mock import MagicMock, patch


def _clear_cache() -> None:
    from dragon.ai.langgraph import llm as _llm

    _llm._proxies.clear()


def _queue(serialized: bytes = b"q") -> MagicMock:
    """A mock Dragon native Queue handle with the given serialized descriptor."""
    q = MagicMock()
    q.serialize.return_value = serialized
    return q


# ========================================================================
# get_llm — memoization and building
# ========================================================================


class TestGetLLM(TestCase):
    """Verify get_llm builds one proxy per pipeline, per process."""

    def setUp(self) -> None:
        _clear_cache()

    def tearDown(self) -> None:
        _clear_cache()

    def test_builds_once_per_queue(self):
        """Two calls for the same queue build one proxy and return the same one."""
        from dragon.ai.langgraph import get_llm

        q = _queue(b"pipe-A")
        with patch("dragon.ai.langgraph.llm._make_proxy") as make:
            make.return_value = MagicMock(name="proxyA")
            first = get_llm(q)
            second = get_llm(q)

        make.assert_called_once()
        self.assertIs(first, second)

    def test_different_queues_get_different_proxies(self):
        """Different pipelines (queues) get their own proxies."""
        from dragon.ai.langgraph import get_llm

        qa, qb = _queue(b"pipe-A"), _queue(b"pipe-B")
        proxy_a, proxy_b = MagicMock(name="A"), MagicMock(name="B")
        with patch("dragon.ai.langgraph.llm._make_proxy", side_effect=[proxy_a, proxy_b]) as make:
            got_a = get_llm(qa)
            got_b = get_llm(qb)

        self.assertEqual(make.call_count, 2)
        self.assertIs(got_a, proxy_a)
        self.assertIs(got_b, proxy_b)
        self.assertIsNot(got_a, got_b)

    def test_same_pipeline_via_separate_handles_dedups(self):
        """Two handles that serialize to the same descriptor share one proxy."""
        from dragon.ai.langgraph import get_llm

        q1, q2 = _queue(b"same-pipe"), _queue(b"same-pipe")  # distinct objects
        with patch("dragon.ai.langgraph.llm._make_proxy") as make:
            make.return_value = MagicMock()
            first = get_llm(q1)
            second = get_llm(q2)

        make.assert_called_once()
        self.assertIs(first, second)

    def test_none_raises_valueerror(self):
        """A None queue raises a clear ValueError."""
        from dragon.ai.langgraph import get_llm

        with self.assertRaisesRegex(ValueError, "get_llm"):
            get_llm(None)

    def test_proxy_kwargs_forwarded_on_first_build(self):
        """proxy_kwargs reach DragonQueueLLMProxy on the first build."""
        from dragon.ai.langgraph import get_llm

        q = _queue(b"pipe-A")
        with patch("dragon.ai.langgraph.llm._make_proxy") as make:
            make.return_value = MagicMock()
            get_llm(q, max_concurrent_requests=8)

        make.assert_called_once_with(q, max_concurrent_requests=8)

    def test_keyed_by_serialized_descriptor(self):
        """The cache key is the queue's serialized descriptor."""
        from dragon.ai.langgraph import get_llm, llm

        q = _queue(b"desc")
        with patch("dragon.ai.langgraph.llm._make_proxy") as make:
            make.return_value = MagicMock()
            get_llm(q)

        self.assertIn(b"desc", llm._proxies)

    def test_build_failure_propagates_and_is_not_cached(self):
        """A missing dragon.ai.inference must surface to the node, not be
        swallowed into a None proxy that fails later and elsewhere."""
        from dragon.ai.langgraph import get_llm, llm

        q = _queue(b"pipe-A")
        with patch(
            "dragon.ai.langgraph.llm._make_proxy",
            side_effect=ImportError("no dragon.ai.inference"),
        ):
            with self.assertRaises(ImportError):
                get_llm(q)

        # Nothing half-built was left behind for the next caller to trip on.
        self.assertEqual(llm._proxies, {})

    def test_queue_without_serialize_propagates(self):
        """Passing a proxy (or anything not a Queue handle) fails at the call,
        not with an opaque error deep inside the inference layer."""
        from dragon.ai.langgraph import get_llm

        with self.assertRaises(AttributeError):
            get_llm(object())


# ========================================================================
# _close_all_llms — host-exit cleanup
# ========================================================================


class TestCloseAllLLMs(IsolatedAsyncioTestCase):
    """Verify host-exit cleanup shuts down every cached proxy's pool."""

    def setUp(self) -> None:
        _clear_cache()

    def tearDown(self) -> None:
        _clear_cache()

    async def test_shuts_down_pools_and_clears_cache(self):
        from dragon.ai.langgraph import llm as _llm

        class _Pool:
            def __init__(self):
                self.shut = False

            async def shutdown(self):
                self.shut = True

        proxy = MagicMock()
        proxy._response_pool = _Pool()
        _llm._proxies[b"k"] = proxy

        await _llm._close_all_llms()

        self.assertTrue(proxy._response_pool.shut)
        self.assertEqual(_llm._proxies, {})

    async def test_swallows_pool_shutdown_error(self):
        from dragon.ai.langgraph import llm as _llm

        class _Pool:
            async def shutdown(self):
                raise RuntimeError("shutdown boom")

        proxy = MagicMock()
        proxy._response_pool = _Pool()
        _llm._proxies[b"k"] = proxy

        # Must not raise; cache still cleared.
        with self.assertLogs("LANGGRAPH.llm", level="WARNING") as logs:
            await _llm._close_all_llms()
        # A pool that would not shut down is a leaked Dragon resource, so it has
        # to be visible at default log level rather than debug-only.
        self.assertIn("shutdown boom", "\n".join(logs.output))
        self.assertEqual(_llm._proxies, {})

    async def test_one_bad_pool_does_not_skip_the_rest(self):
        """Every cached proxy gets a shutdown attempt, failures included."""
        from dragon.ai.langgraph import llm as _llm

        class _BadPool:
            async def shutdown(self):
                raise RuntimeError("shutdown boom")

        class _GoodPool:
            def __init__(self):
                self.shut = False

            async def shutdown(self):
                self.shut = True

        bad, good = MagicMock(), MagicMock()
        bad._response_pool = _BadPool()
        good._response_pool = _GoodPool()
        _llm._proxies[b"bad"] = bad
        _llm._proxies[b"good"] = good

        with self.assertLogs("LANGGRAPH.llm", level="WARNING"):
            await _llm._close_all_llms()

        self.assertTrue(good._response_pool.shut)
        self.assertEqual(_llm._proxies, {})

    async def test_proxy_without_a_pool_is_skipped(self):
        """A proxy that never built a response pool has nothing to release."""
        from dragon.ai.langgraph import llm as _llm

        proxy = MagicMock()
        proxy._response_pool = None
        _llm._proxies[b"k"] = proxy

        await _llm._close_all_llms()  # must not raise
        self.assertEqual(_llm._proxies, {})

    async def test_empty_cache_is_noop(self):
        from dragon.ai.langgraph import llm as _llm

        await _llm._close_all_llms()  # must not raise
        self.assertEqual(_llm._proxies, {})


if __name__ == "__main__":
    mp.set_start_method("dragon")
    main()
