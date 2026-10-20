import asyncio
import logging
import os
import unittest
from unittest.mock import Mock, patch

from dragon.channels import Channel, EventType, discard_gateways, register_gateways_from_env
from dragon.managed_memory import MemoryPool
from dragon.transport.tcp.client import Client
from dragon.transport.tcp.errno import DRAGON_FAILURE
from dragon.transport.tcp.messages import ErrorResponse, EventRequest, EventResponse
from dragon.transport.tcp.server import Server
from dragon.transport.tcp.transport import LOOPBACK_ADDRESS_IPv4, Transport
from dragon.utils import B64


def setUpModule():
    """Disable logging while running these tests."""
    logging.disable()


def tearDownModule():
    """Re-enable logging after running these tests."""
    logging.disable(logging.NOTSET)


class ClientCleanupTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.pool = MemoryPool(16 * 1024 * 1024, f"tcp_client_cleanup_{os.getpid()}", os.getpid())
        self.gateway = Channel(self.pool, 1)
        self.environment = patch.dict(os.environ, {"DRAGON_GW1": str(B64(self.gateway.serialize()))})
        self.environment.start()
        register_gateways_from_env()
        self.transport = Transport(LOOPBACK_ADDRESS_IPv4)
        self.server = Server(self.transport)
        self.client = Client(self.gateway.serialize(), self.transport)
        self.client.open()
        self.tasks = []

    async def asyncTearDown(self):
        for task in self.tasks:
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        self.transport._responses.clear()
        self.client.close()
        discard_gateways()
        self.environment.stop()
        self.gateway.destroy()
        self.pool.destroy()

    async def make_cleanup(self, start_task=True):
        channel = Channel(self.pool, 2)
        channel.notify_on_destroy()
        channel.destroy()
        event = await asyncio.wait_for(self.client.recv(), 2)
        self.assertEqual(event.event_mask, EventType.CHANNEL_CLEANUP)
        self.client.nodes[event.target_hostid] = self.transport.addr
        task = self.client.process(event)
        self.tasks.append(task)
        request, address = await self.transport.read_request()
        future = self.transport._responses[request.seqno]
        if start_task:
            await asyncio.sleep(0)
        self.assertFalse(task.done())
        return task, request, address, future

    async def test_cleanup_finishes_without_response_and_releases_event(self):
        baseline = self.pool.get_allocations().num_allocs
        for _ in range(2):
            task, request, address, future = await self.make_cleanup()
            await self.server.handle_request(request, address)
            self.assertFalse(future.done(), "cleanup does not send a response")
            self.assertFalse(task.done(), "request I/O must finish before retirement")
            request._io_event.set()
            await asyncio.wait_for(asyncio.shield(task), 2)
            self.assertTrue(future.cancelled())
            self.assertFalse(self.transport._responses)
            self.assertEqual(self.pool.get_allocations().num_allocs, baseline)

    async def test_cancelled_cleanup_releases_event_and_response_entry(self):
        baseline = self.pool.get_allocations().num_allocs
        task, request, address, future = await self.make_cleanup()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertTrue(future.cancelled())
        self.assertFalse(self.transport._responses)
        self.assertEqual(self.pool.get_allocations().num_allocs, baseline)
        # The queued request owns a descriptor copy and can still be handled.
        await self.server.handle_request(request, address)
        request._io_event.set()

    async def test_cleanup_error_response_completes_and_releases_event(self):
        # The server only responds to a cleanup request when it fails to handle
        # it. Run the request through Server.process() so the ErrorResponse and
        # the request I/O event are produced in production order.
        baseline = self.pool.get_allocations().num_allocs
        task, request, address, future = await self.make_cleanup()
        key = (request.channel_sd, EventRequest)
        self.server._requests[key].put_nowait((request, address))
        with patch("dragon.transport.tcp.server.poll_channel", side_effect=RuntimeError("cannot poll")):
            self.tasks.append(asyncio.create_task(self.server.process(key)))
            await asyncio.wait_for(asyncio.shield(task), 2)
        response, _ = future.result()
        self.assertIsInstance(response, ErrorResponse)
        self.assertEqual(response.errno, DRAGON_FAILURE)
        self.assertTrue(response._io_event.is_set())
        self.assertFalse(self.transport._responses)
        self.assertEqual(self.pool.get_allocations().num_allocs, baseline)

    async def test_cleanup_cancelled_before_start_releases_event_and_response_entry(self):
        baseline = self.pool.get_allocations().num_allocs
        task, request, address, future = await self.make_cleanup(start_task=False)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertTrue(future.cancelled())
        self.assertFalse(self.transport._responses)
        self.assertEqual(self.pool.get_allocations().num_allocs, baseline)
        await self.server.handle_request(request, address)
        request._io_event.set()

    async def test_cancelled_cleanup_logs_late_error_response(self):
        baseline = self.pool.get_allocations().num_allocs
        task, request, address, future = await self.make_cleanup()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertTrue(future.cancelled())
        self.assertFalse(self.transport._responses)
        self.assertEqual(self.pool.get_allocations().num_allocs, baseline)

        key = (request.channel_sd, EventRequest)
        self.server._requests[key].put_nowait((request, address))
        with (
            patch("dragon.transport.tcp.server.poll_channel", side_effect=RuntimeError("cannot poll")),
            patch("dragon.transport.tcp.transport.LOGGER.warning") as warning,
        ):
            self.tasks.append(asyncio.create_task(self.server.process(key)))
            await asyncio.wait_for(request._io_event.wait(), 2)
        self.assertTrue(any("cannot poll" in str(call) for call in warning.call_args_list))
        self.assertFalse(self.transport._responses)
        self.assertEqual(self.pool.get_allocations().num_allocs, baseline)

    async def test_unroutable_cleanup_completes_with_error_without_blocking(self):
        # Client.run() error-completes and destroys a gateway message when
        # process() fails, e.g., before the node map includes the local host.
        # No native caller waits on a cleanup notification, so completing it
        # must not block on the gateway completion handshake.
        baseline = self.pool.get_allocations().num_allocs
        channel = Channel(self.pool, 2)
        channel.notify_on_destroy()
        channel.destroy()
        event = await asyncio.wait_for(self.client.recv(), 2)
        with self.assertRaises(ValueError):
            self.client.process(event)
        await asyncio.wait_for(asyncio.to_thread(event.error_complete, DRAGON_FAILURE), 2)
        event.destroy()
        self.assertEqual(self.pool.get_allocations().num_allocs, baseline)

    async def test_ordinary_event_still_waits_for_response(self):
        event = Mock(is_event_kind=True, event_mask=EventType.POLLIN)
        request = EventRequest(seqno=None, timeout=2, channel_sd=b"descriptor", mask=EventType.POLLIN)
        future = self.transport.write_request(request, self.transport.addr)
        task = asyncio.create_task(self.client.wait_for_response(future, event, request))
        self.tasks.append(task)
        request._io_event.set()
        await asyncio.sleep(0)
        self.assertFalse(task.done())
        response = EventResponse(request.seqno, 0, EventType.POLLIN)
        self.transport.write_response(response, self.transport.addr)
        await asyncio.wait_for(task, 2)
        event.event_complete.assert_called_once_with(EventType.POLLIN, 0)
        event.destroy.assert_called_once_with()
        self.assertTrue(response._io_event.is_set())
        self.assertFalse(self.transport._responses)


if __name__ == "__main__":
    unittest.main()
