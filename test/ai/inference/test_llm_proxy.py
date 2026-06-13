"""
Unit tests for the LLM proxy module (inference/llm_proxy.py).

Tests the LLMProxy interface, InferenceRequest, ResponseQueuePool,
and DragonQueueLLMProxy classes.

These tests use Dragon multiprocessing primitives.
"""

import asyncio
import dragon
import multiprocessing as mp
from unittest import TestCase, main
from unittest.mock import AsyncMock, MagicMock, patch

from dragon.ai.inference.llm_proxy import (
    DragonQueueLLMProxy,
    InferenceRequest,
    LLMProxy,
    ResponseQueuePool,
)


class TestInferenceRequest(TestCase):
    """Test InferenceRequest NamedTuple construction and defaults."""

    @classmethod
    def setUpClass(cls):
        """Set up Dragon multiprocessing."""
        try:
            mp.set_start_method("dragon", force=True)
        except RuntimeError:
            pass

    def test_required_fields(self):
        """Test InferenceRequest with only required fields."""
        req = InferenceRequest(
            messages=[{"role": "user", "content": "hi"}],
            formatted_messages=["<user>hi</user>"],
            response_queue=MagicMock(),
            timestamp=1234567890.0,
        )
        self.assertEqual(req.messages, [{"role": "user", "content": "hi"}])
        self.assertEqual(req.formatted_messages, ["<user>hi</user>"])
        self.assertEqual(req.timestamp, 1234567890.0)
        self.assertIsNone(req.tools)
        self.assertIsNone(req.sampling_override)
        self.assertFalse(req.continue_final_message)

    def test_all_fields(self):
        """Test InferenceRequest with all fields specified."""
        tools = [{"type": "function", "function": {"name": "test"}}]
        schema = {"type": "object"}
        req = InferenceRequest(
            messages=[{"role": "user", "content": "hi"}],
            formatted_messages=["formatted"],
            response_queue=MagicMock(),
            timestamp=100.0,
            tools=tools,
            sampling_override=schema,
            continue_final_message=True,
        )
        self.assertEqual(req.tools, tools)
        self.assertEqual(req.sampling_override, schema)
        self.assertTrue(req.continue_final_message)

    def test_field_ordering(self):
        """Test that positional access matches named access."""
        queue = MagicMock()
        req = InferenceRequest(
            messages=["m"],
            formatted_messages=["f"],
            response_queue=queue,
            timestamp=1.0,
        )
        self.assertEqual(req[0], ["m"])
        self.assertEqual(req[1], ["f"])
        self.assertIs(req[2], queue)
        self.assertEqual(req[3], 1.0)


class TestResponseQueuePool(TestCase):
    """Test ResponseQueuePool acquire/release and lifecycle."""

    @classmethod
    def setUpClass(cls):
        try:
            mp.set_start_method("dragon", force=True)
        except RuntimeError:
            pass

    @patch("dragon.ai.inference.llm_proxy.ResponseQueuePool._make_queue")
    def test_acquire_creates_queue_on_first_call(self, mock_make_queue):
        """Test that acquire lazily creates a queue when pool is empty."""
        mock_queue = MagicMock()
        mock_make_queue.return_value = mock_queue
        pool = ResponseQueuePool(pool_size=4)

        result = asyncio.run(pool.acquire())

        self.assertIs(result, mock_queue)
        mock_make_queue.assert_called_once()
        self.assertEqual(pool._created, 1)

    @patch("dragon.ai.inference.llm_proxy.ResponseQueuePool._make_queue")
    def test_release_makes_queue_reusable(self, mock_make_queue):
        """Test that released queues are returned on next acquire."""
        mock_queue = MagicMock()
        mock_make_queue.return_value = mock_queue
        pool = ResponseQueuePool(pool_size=4)

        async def _test():
            q = await pool.acquire()
            await pool.release(q)
            q2 = await pool.acquire()
            return q, q2

        q1, q2 = asyncio.run(_test())
        self.assertIs(q1, q2)
        # Only created once since it was reused
        mock_make_queue.assert_called_once()

    @patch("dragon.ai.inference.llm_proxy.ResponseQueuePool._make_queue")
    def test_pool_grows_up_to_limit(self, mock_make_queue):
        """Test that pool creates up to pool_size queues."""
        mock_make_queue.side_effect = [MagicMock() for _ in range(3)]
        pool = ResponseQueuePool(pool_size=3)

        async def _test():
            q1 = await pool.acquire()
            q2 = await pool.acquire()
            q3 = await pool.acquire()
            return q1, q2, q3

        q1, q2, q3 = asyncio.run(_test())
        self.assertEqual(pool._created, 3)
        self.assertEqual(mock_make_queue.call_count, 3)

    @patch("dragon.ai.inference.llm_proxy.ResponseQueuePool._make_queue")
    def test_pool_exhaustion_blocks(self, mock_make_queue):
        """Test that acquire blocks when pool is exhausted."""
        mock_make_queue.side_effect = [MagicMock(), MagicMock()]
        pool = ResponseQueuePool(pool_size=2)

        async def _test():
            q1 = await pool.acquire()
            q2 = await pool.acquire()
            # Pool is now exhausted; next acquire should block
            # until we release one
            released = False

            async def release_later():
                nonlocal released
                await asyncio.sleep(0.05)
                await pool.release(q1)
                released = True

            asyncio.ensure_future(release_later())
            q3 = await pool.acquire()
            self.assertTrue(released)
            self.assertIs(q3, q1)

        asyncio.run(_test())

    @patch("dragon.ai.inference.llm_proxy.ResponseQueuePool._make_queue")
    def test_pool_available_property(self, mock_make_queue):
        """Test pool_available reflects idle queue count."""
        mock_make_queue.return_value = MagicMock()
        pool = ResponseQueuePool(pool_size=4)

        async def _test():
            self.assertEqual(pool.pool_available, 0)
            q = await pool.acquire()
            self.assertEqual(pool.pool_available, 0)
            await pool.release(q)
            self.assertEqual(pool.pool_available, 1)

        asyncio.run(_test())

    @patch("dragon.ai.inference.llm_proxy.ResponseQueuePool._make_queue")
    def test_shutdown_destroys_idle_queues(self, mock_make_queue):
        """Test shutdown calls destroy on all idle queues."""
        mock_queue = MagicMock()
        mock_make_queue.return_value = mock_queue
        pool = ResponseQueuePool(pool_size=4)

        async def _test():
            q = await pool.acquire()
            await pool.release(q)
            await pool.shutdown()
            return q

        asyncio.run(_test())
        mock_queue.destroy.assert_called_once()
        self.assertEqual(pool._created, 0)


class TestDragonQueueLLMProxy(TestCase):
    """Test DragonQueueLLMProxy chat method and lifecycle."""

    @classmethod
    def setUpClass(cls):
        try:
            mp.set_start_method("dragon", force=True)
        except RuntimeError:
            pass

    @patch("dragon.ai.inference.llm_proxy.ResponseQueuePool._make_queue")
    def test_chat_puts_request_on_input_queue(self, mock_make_queue):
        """Test that chat() puts an InferenceRequest on input_queue."""
        mock_response_queue = MagicMock()
        mock_response_queue.get.return_value = "Hello!"
        mock_make_queue.return_value = mock_response_queue

        mock_input_queue = MagicMock()
        proxy = DragonQueueLLMProxy(mock_input_queue, max_concurrent_requests=4)

        messages = [{"role": "user", "content": "hi"}]
        result = asyncio.run(proxy.chat(messages))

        self.assertEqual(result, "Hello!")
        mock_input_queue.put.assert_called_once()
        request = mock_input_queue.put.call_args[0][0]
        self.assertIsInstance(request, InferenceRequest)
        self.assertEqual(request.messages, messages)

    @patch("dragon.ai.inference.llm_proxy.ResponseQueuePool._make_queue")
    def test_chat_with_tools_and_schema(self, mock_make_queue):
        """Test chat() passes tools and json_schema correctly."""
        mock_response_queue = MagicMock()
        mock_response_queue.get.return_value = '{"answer": 42}'
        mock_make_queue.return_value = mock_response_queue

        mock_input_queue = MagicMock()
        proxy = DragonQueueLLMProxy(mock_input_queue)

        tools = [{"type": "function", "function": {"name": "calc"}}]
        schema = {"type": "object", "properties": {"answer": {"type": "integer"}}}
        messages = [{"role": "user", "content": "compute"}]

        result = asyncio.run(proxy.chat(messages, tools=tools, json_schema=schema))

        request = mock_input_queue.put.call_args[0][0]
        self.assertEqual(request.tools, tools)
        self.assertEqual(request.sampling_override, schema)

    @patch("dragon.ai.inference.llm_proxy.ResponseQueuePool._make_queue")
    def test_chat_with_continue_final_message(self, mock_make_queue):
        """Test chat() passes continue_final_message flag."""
        mock_response_queue = MagicMock()
        mock_response_queue.get.return_value = "continued"
        mock_make_queue.return_value = mock_response_queue

        mock_input_queue = MagicMock()
        proxy = DragonQueueLLMProxy(mock_input_queue)

        messages = [{"role": "assistant", "content": "I think"}]
        asyncio.run(proxy.chat(messages, continue_final_message=True))

        request = mock_input_queue.put.call_args[0][0]
        self.assertTrue(request.continue_final_message)

    @patch("dragon.ai.inference.llm_proxy.ResponseQueuePool._make_queue")
    def test_chat_normalizes_dict_response(self, mock_make_queue):
        """Test chat() extracts 'assistant' key from dict response."""
        mock_response_queue = MagicMock()
        mock_response_queue.get.return_value = {"assistant": "Hello from dict"}
        mock_make_queue.return_value = mock_response_queue

        mock_input_queue = MagicMock()
        proxy = DragonQueueLLMProxy(mock_input_queue)

        result = asyncio.run(proxy.chat([{"role": "user", "content": "hi"}]))
        self.assertEqual(result, "Hello from dict")

    @patch("dragon.ai.inference.llm_proxy.ResponseQueuePool._make_queue")
    def test_chat_reraises_backend_exception(self, mock_make_queue):
        """Test chat() re-raises exceptions returned by the backend."""
        mock_response_queue = MagicMock()
        mock_response_queue.get.return_value = RuntimeError("GPU OOM")
        mock_make_queue.return_value = mock_response_queue

        mock_input_queue = MagicMock()
        proxy = DragonQueueLLMProxy(mock_input_queue)

        with self.assertRaises(RuntimeError) as ctx:
            asyncio.run(proxy.chat([{"role": "user", "content": "hi"}]))
        self.assertIn("GPU OOM", str(ctx.exception))

    @patch("dragon.ai.inference.llm_proxy.ResponseQueuePool._make_queue")
    def test_chat_releases_queue_on_error(self, mock_make_queue):
        """Test that response queue is released even when put() fails."""
        mock_response_queue = MagicMock()
        mock_make_queue.return_value = mock_response_queue

        mock_input_queue = MagicMock()
        mock_input_queue.put.side_effect = OSError("queue broken")
        proxy = DragonQueueLLMProxy(mock_input_queue, max_concurrent_requests=2)

        with self.assertRaises(OSError):
            asyncio.run(proxy.chat([{"role": "user", "content": "hi"}]))

        # Queue should still be returned to pool
        self.assertEqual(proxy.pool_available, 1)

    @patch("dragon.ai.inference.llm_proxy.ResponseQueuePool._make_queue")
    def test_pool_available_reflects_inflight(self, mock_make_queue):
        """Test pool_available decreases during inflight requests."""
        mock_response_queue = MagicMock()
        mock_response_queue.get.return_value = "ok"
        mock_make_queue.return_value = mock_response_queue

        mock_input_queue = MagicMock()
        proxy = DragonQueueLLMProxy(mock_input_queue, max_concurrent_requests=4)

        # Before any call
        self.assertEqual(proxy.pool_available, 0)

        # After a successful call, queue should be back in pool
        asyncio.run(proxy.chat([{"role": "user", "content": "hi"}]))
        self.assertEqual(proxy.pool_available, 1)


if __name__ == "__main__":
    main()
