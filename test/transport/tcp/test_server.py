import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock

from dragon.transport.tcp.server import Server
from dragon.transport.tcp import transport
from dragon.transport.tcp import messages

from test_transport import TestMessages


class ServerTestCase(TestMessages, unittest.IsolatedAsyncioTestCase):

    ADDR = transport.Address.from_netloc("127.0.0.1:7575")

    def setUp(self):
        self.transport = transport.StreamTransport(self.ADDR)

    def tearDown(self):
        del self.transport

    async def asyncSetUp(self):
        self.transport.start()
        await self.transport.wait_serving()

    async def asyncTearDown(self):
        try:
            await self.transport.stop()
        except AssertionError:
            pass

    async def test_run_read_send_request(self):
        # Mock Transport.read_request() to read fake request
        req = self.SendRequest()
        addr = transport.Address.from_netloc("127.0.0.1:8888")
        self.transport.read_request = AsyncMock(side_effect=[(req, addr), asyncio.CancelledError])

        # Mock Server._ensure_process_task() to prevent a process task from
        # being created.
        server = Server(self.transport)
        server._ensure_process_task = MagicMock()

        await server.run()

        key = (req.channel_sd, req.sendhid)
        server._ensure_process_task.assert_called_once_with(key)

        _req, _addr = server._requests[key].get_nowait()
        self.assertIs(req, _req)
        self.assertIs(addr, _addr)

    async def test_run_read_recv_request(self):
        # Mock Transport.read_request() to read fake request
        req = self.RecvRequest()
        addr = transport.Address.from_netloc("127.0.0.1:8888")
        self.transport.read_request = AsyncMock(side_effect=[(req, addr), asyncio.CancelledError])

        # Mock Server._ensure_process_task() to prevent a process task from
        # being created.
        server = Server(self.transport)
        server._ensure_process_task = MagicMock()

        await server.run()

        key = (req.channel_sd, messages.RecvRequest)
        server._ensure_process_task.assert_called_once_with(key)

        _req, _addr = server._requests[key].get_nowait()
        self.assertIs(req, _req)
        self.assertIs(addr, _addr)

    @unittest.skip
    def test__ensure_process_task(self):
        raise NotImplementedError

    @unittest.skip
    async def test_process(self):
        raise NotImplementedError

    @unittest.skip
    def test_handle_unsupported_request(self):
        raise NotImplementedError

    @unittest.skip
    def test_handle_send_request(self):
        raise NotImplementedError

    @unittest.skip
    def test_handle_recv_request(self):
        raise NotImplementedError

    @unittest.skip
    def test_handle_event_request(self):
        raise NotImplementedError


if __name__ == "__main__":
    unittest.main()
