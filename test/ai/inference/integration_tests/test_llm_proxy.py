"""
Integration tests for the LLM proxy module.

Tests DragonQueueLLMProxy end-to-end with simulated backend workers
consuming from the input queue and producing responses.
"""

import asyncio
import dragon
import multiprocessing as mp
import threading
import time
import unittest
from unittest.mock import MagicMock, patch

from dragon.ai.inference.llm_proxy import (
    DragonQueueLLMProxy,
    InferenceRequest,
    ResponseQueuePool,
)


class TestLLMProxyWithSimulatedBackend(unittest.TestCase):
    """Integration tests simulating the inference backend consuming requests
    from the input queue and returning responses via response queues.
    """

    @classmethod
    def setUpClass(cls):
        try:
            mp.set_start_method("dragon", force=True)
        except RuntimeError:
            pass

    def _run_backend(self, input_queue, response_fn, num_requests=1):
        """Simulate a backend worker that reads from input_queue and responds.

        Args:
            input_queue: Queue to read InferenceRequests from.
            response_fn: Callable that takes an InferenceRequest and returns
                the response to put on the response queue.
            num_requests: Number of requests to process before stopping.
        """

        def worker():
            for _ in range(num_requests):
                request = input_queue.get()
                response = response_fn(request)
                request.response_queue.put(response)

        t = threading.Thread(target=worker, daemon=True)
        t.start()
        return t

    @patch("dragon.ai.inference.llm_proxy.ResponseQueuePool._make_queue")
    def test_single_request_response_cycle(self, mock_make_queue):
        """Test a complete request→backend→response cycle."""
        mock_response_queue = MagicMock()
        responses = []

        def fake_put(val):
            responses.append(val)
            mock_response_queue.get.return_value = val

        mock_response_queue.put = fake_put
        mock_make_queue.return_value = mock_response_queue

        input_queue = MagicMock()
        captured_requests = []

        def fake_input_put(request):
            captured_requests.append(request)
            # Simulate backend: put response on the response queue
            request.response_queue.put("Backend says hello")

        input_queue.put = fake_input_put

        proxy = DragonQueueLLMProxy(input_queue, max_concurrent_requests=4)
        messages = [{"role": "user", "content": "Hello"}]

        result = asyncio.run(proxy.chat(messages))

        self.assertEqual(result, "Backend says hello")
        self.assertEqual(len(captured_requests), 1)
        self.assertIsInstance(captured_requests[0], InferenceRequest)

    @patch("dragon.ai.inference.llm_proxy.ResponseQueuePool._make_queue")
    def test_concurrent_requests_use_separate_queues(self, mock_make_queue):
        """Test that concurrent requests each get their own response queue."""
        queues_used = []

        def make_queue_factory():
            q = MagicMock()
            q.get.return_value = f"response-{len(queues_used)}"
            queues_used.append(q)
            return q

        mock_make_queue.side_effect = make_queue_factory

        input_queue = MagicMock()
        proxy = DragonQueueLLMProxy(input_queue, max_concurrent_requests=4)

        async def _test():
            tasks = [
                proxy.chat([{"role": "user", "content": f"msg-{i}"}])
                for i in range(3)
            ]
            return await asyncio.gather(*tasks)

        results = asyncio.run(_test())
        self.assertEqual(len(results), 3)
        # Each request should use a different queue (pool grows)
        self.assertEqual(len(queues_used), 3)

    @patch("dragon.ai.inference.llm_proxy.ResponseQueuePool._make_queue")
    def test_backend_exception_propagates_to_caller(self, mock_make_queue):
        """Test that backend exceptions are propagated through the proxy."""
        mock_response_queue = MagicMock()
        mock_response_queue.get.return_value = ValueError("Invalid prompt format")
        mock_make_queue.return_value = mock_response_queue

        input_queue = MagicMock()
        proxy = DragonQueueLLMProxy(input_queue)

        with self.assertRaises(ValueError) as ctx:
            asyncio.run(
                proxy.chat([{"role": "user", "content": "bad prompt"}])
            )
        self.assertIn("Invalid prompt format", str(ctx.exception))

    @patch("dragon.ai.inference.llm_proxy.ResponseQueuePool._make_queue")
    def test_tools_and_schema_reach_backend(self, mock_make_queue):
        """Test that tools and json_schema are preserved in the request."""
        mock_response_queue = MagicMock()
        mock_response_queue.get.return_value = '{"result": "ok"}'
        mock_make_queue.return_value = mock_response_queue

        captured = []
        input_queue = MagicMock()
        input_queue.put = lambda req: captured.append(req)
        # Also need to trigger the get
        mock_response_queue.get.return_value = '{"result": "ok"}'

        proxy = DragonQueueLLMProxy(input_queue)
        tools = [{"type": "function", "function": {"name": "search"}}]
        schema = {"type": "object", "properties": {"result": {"type": "string"}}}

        asyncio.run(
            proxy.chat(
                [{"role": "user", "content": "find"}],
                tools=tools,
                json_schema=schema,
            )
        )

        self.assertEqual(len(captured), 1)
        req = captured[0]
        self.assertEqual(req.tools, tools)
        self.assertEqual(req.sampling_override, schema)

    @patch("dragon.ai.inference.llm_proxy.ResponseQueuePool._make_queue")
    def test_response_queue_returned_to_pool_after_success(self, mock_make_queue):
        """Test pool reclaims response queue after successful request."""
        mock_response_queue = MagicMock()
        mock_response_queue.get.return_value = "ok"
        mock_make_queue.return_value = mock_response_queue

        input_queue = MagicMock()
        proxy = DragonQueueLLMProxy(input_queue, max_concurrent_requests=2)

        asyncio.run(proxy.chat([{"role": "user", "content": "hi"}]))
        self.assertEqual(proxy.pool_available, 1)

        # Second call reuses the same queue (pool doesn't grow)
        asyncio.run(proxy.chat([{"role": "user", "content": "hi again"}]))
        self.assertEqual(proxy.pool_available, 1)
        mock_make_queue.assert_called_once()

    @patch("dragon.ai.inference.llm_proxy.ResponseQueuePool._make_queue")
    def test_dict_response_normalized(self, mock_make_queue):
        """Test dict responses are normalized to extract assistant text."""
        mock_response_queue = MagicMock()
        mock_response_queue.get.return_value = {
            "assistant": "The answer is 42",
            "tokens_used": 10,
        }
        mock_make_queue.return_value = mock_response_queue

        input_queue = MagicMock()
        proxy = DragonQueueLLMProxy(input_queue)

        result = asyncio.run(
            proxy.chat([{"role": "user", "content": "what is the answer?"}])
        )
        self.assertEqual(result, "The answer is 42")

    @patch("dragon.ai.inference.llm_proxy.ResponseQueuePool._make_queue")
    def test_request_timestamp_is_recent(self, mock_make_queue):
        """Test that InferenceRequest timestamp is set to current time."""
        mock_response_queue = MagicMock()
        mock_response_queue.get.return_value = "ok"
        mock_make_queue.return_value = mock_response_queue

        captured = []
        input_queue = MagicMock()
        input_queue.put = lambda req: captured.append(req)

        proxy = DragonQueueLLMProxy(input_queue)
        before = time.time()
        asyncio.run(proxy.chat([{"role": "user", "content": "hi"}]))
        after = time.time()

        self.assertEqual(len(captured), 1)
        self.assertGreaterEqual(captured[0].timestamp, before)
        self.assertLessEqual(captured[0].timestamp, after)


if __name__ == "__main__":
    unittest.main()
