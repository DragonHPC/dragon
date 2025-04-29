import asyncio
from functools import partial
import logging
import unittest
from unittest.mock import call, patch
from uuid import uuid4

from dragon.transport.tcp import errno
from dragon.transport.tcp import messages
from dragon.transport.tcp import transport


def setUpModule():
    """Disable logging while running these tests."""
    logging.disable()


def tearDownModule():
    """Re-enable logging after running these tests."""
    logging.disable(logging.NOTSET)


class TestMessages:
    """Mixin that provides messages for testing."""

    @classmethod
    def setUpClass(cls):
        cls.SendRequest = partial(
            messages.SendRequest,
            seqno=None,
            timeout=0.5,
            channel_sd=b"channel desc",
            return_mode=messages.SendReturnMode.WHEN_BUFFERED,
            sendhid=uuid4(),
            payload=b"payload",
            clientid=0,
            hints=0,
        )

        cls.RecvRequest = partial(
            messages.RecvRequest,
            seqno=None,
            timeout=0.5,
            channel_sd=b"channel desc",
        )


class TransportTestCase(TestMessages, unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.transport = transport.Transport(transport.LOOPBACK_ADDRESS_IPv4)
        self.srcaddr = self.transport.addr
        self.destaddr = self.transport.addr

    def tearDown(self):
        del self.transport

    @patch.object(transport.Transport, "_handle_send")
    async def test_request_response(self, handle_send):
        """Request/Response round-trip

        Verifies the `asyncio.Future` returned from `Transport.write_request`
        is being properly tracked and completed when a corresponding `Response`
        is handled.
        """
        # Write request
        req = self.SendRequest()
        fut = self.transport.write_request(req, self.destaddr)

        # Verify that the seuqnce number was set
        self.assertEqual(req.seqno, 1)

        # Check that _handle_send was called properly
        handle_send.assert_called_once_with(req, self.destaddr)

        # Verify that the returned future is being tracked in _responses
        self.assertIn(req.seqno, self.transport._responses)
        self.assertIs(fut, self.transport._responses[req.seqno])

        # Verify that future is not done
        self.assertFalse(fut.done())

        # Handle corresponding response
        resp = messages.SendResponse(req.seqno)
        self.transport._handle_recv(resp, self.destaddr)

        # Verify the future is no longer being tracked in _responses
        self.assertNotIn(req.seqno, self.transport._responses)

        # Verify the future is done
        self.assertTrue(fut.done())

        # Verify future is completed with response
        fut_resp, fut_addr = await fut
        self.assertIs(fut_resp, resp)
        self.assertIs(fut_addr, self.destaddr)

    @patch.object(transport.Transport, "_handle_send")
    async def test_write_request(self, handle_send):
        # Write request
        req = self.SendRequest()
        fut = self.transport.write_request(req, self.destaddr)

        # Verify that the seuqnce number was set
        self.assertEqual(req.seqno, 1)

        # Check that _handle_send was called properly
        handle_send.assert_called_once_with(req, self.destaddr)

        # Verify that the returned future is being tracked in _responses
        self.assertIn(req.seqno, self.transport._responses)
        self.assertIs(fut, self.transport._responses[req.seqno])

        # Verify that future is not done
        self.assertFalse(fut.done())

    @patch.object(transport.Transport, "_handle_send", side_effect=RuntimeError)
    async def test_write_request_handle_send_exception(self, handle_send):
        """Future response is not managed when exception raised in _handle_send()

        Verifies the `Transport._responses` dictionary is properly cleaned
        up (i.e., the corresponding `asyncio.Future` is removed) if
        `Transport._handle_send` raises an exception.
        """
        # Verify RuntimeError is raised when writing request
        self.assertRaises(RuntimeError, self.transport.write_request, self.SendRequest(), self.transport.addr)

        # Verify that _responses is empty
        self.assertFalse(self.transport._responses)

    @patch.object(transport.Transport, "_handle_send")
    async def test_write_response(self, handle_send):
        resp = messages.SendResponse(seqno=42)
        self.transport.write_response(resp, self.srcaddr)
        handle_send.assert_called_once_with(resp, self.srcaddr)

    @patch.object(transport.Transport, "_handle_recv")
    def test_handle_send(self, handle_recv):
        # Transport._handle_send() only supports messages to itself
        self.assertRaises(AssertionError, self.transport._handle_send, None, None)

        for mode in (messages.SendReturnMode.IMMEDIATELY, messages.SendReturnMode.WHEN_BUFFERED):
            handle_recv.reset_mock()
            req = self.SendRequest(seqno=42, return_mode=mode)
            self.transport._handle_send(req, self.transport.addr)
            handle_recv.assert_has_calls(
                [
                    call(messages.SendResponse(req.seqno), self.transport.addr),
                    call(req, self.transport.addr),
                ]
            )

        handle_recv.reset_mock()
        req = self.SendRequest(seqno=42, return_mode=messages.SendReturnMode.WHEN_RECEIVED)
        self.transport._handle_send(req, self.transport.addr)
        handle_recv.assert_called_once_with(req, self.transport.addr)

    @patch.object(transport.Transport, "_handle_send")
    async def test_handle_recv(self, handle_send):
        """Sends DRAGON_NOT_IMPLEMENTED error when unsupported message type is received

        Verifies that a `DRAGON_NOT_IMPLEMENTED` error response is sent when
        an unsupported message type is received.
        """
        # Note that Hello messages are only exchanged during connection
        # handshake and are not a `Request` or `Response` and therefore
        # are not supported by `Transport`.
        msg = messages.Hello(host=self.transport.addr.host, port=8080)
        self.transport._handle_recv(msg, self.transport.addr)

        # Expected response
        resp = messages.ErrorResponse(0, errno.DRAGON_NOT_IMPLEMENTED, f"Unsupported type of message: {type(msg)}")
        handle_send.assert_called_once_with(resp, self.transport.addr)

    async def test_handle_recv_response(self):
        # Initialize transport to expect a response
        msg = messages.SendResponse(seqno=42)
        fut = self.transport._responses[msg.seqno] = asyncio.Future()

        # Receive response message
        self.transport._handle_recv(msg, self.destaddr)

        # Verify the future is no longer being tracked in _responses
        self.assertNotIn(msg.seqno, self.transport._responses)

        # Verify that future is completed
        self.assertTrue(fut.done())
        resp, addr = await fut
        self.assertIs(resp, msg)
        self.assertIs(addr, self.destaddr)

    async def test_handle_recv_unexpected_response(self):
        msg = messages.SendResponse(seqno=42)
        self.assertFalse(self.transport._responses)
        # _handle_recv() must silently fail since there is not a corresponding
        # future.
        self.transport._handle_recv(msg, self.transport.addr)

    async def test_handle_recv_request(self):
        with patch.object(self.transport._requests, "put_nowait") as put_nowait:
            msg = self.SendRequest(seqno=42)
            self.transport._handle_recv(msg, self.transport.addr)
            self.assertEqual(self.transport._last_request[self.transport.addr], msg.seqno)
            put_nowait.assert_called_once_with((msg, self.transport.addr))

    async def test_handle_recv_replayed_requests(self):
        with patch.object(self.transport._requests, "put_nowait") as put_nowait:
            # Send original request
            msg = self.SendRequest(seqno=42)
            self.transport._handle_recv(msg, self.transport.addr)

            # Verify the last request seqno is correct
            self.assertEqual(self.transport._last_request[self.transport.addr], msg.seqno)
            # Verify the request was put on the _requests queue
            put_nowait.assert_called_once_with((msg, self.transport.addr))

            # Reset mock to see evaluate replays
            put_nowait.reset_mock()

            # Replay previous requests
            self.transport._handle_recv(msg, self.transport.addr)
            self.transport._handle_recv(self.SendRequest(seqno=9), self.transport.addr)

            # Verify the last request seqno is unaffected
            self.assertEqual(self.transport._last_request[self.transport.addr], msg.seqno)
            # Verify the replayed requests were NOT put on the _requests queue
            put_nowait.assert_not_called()

    async def test_read_request(self):
        msg = self.SendRequest(seqno=42)
        self.transport._requests.put_nowait((msg, self.transport.addr))

        req, addr = await self.transport.read_request()
        self.assertIs(req, msg)
        self.assertIs(addr, self.transport.addr)

    async def test_read_response(self):
        msg = messages.SendResponse(seqno=42)
        self.transport._responses[msg.seqno] = asyncio.Future()
        # asyncio.get_event_loop().call_soon(fut.set_result, (msg, self.destaddr))
        asyncio.get_event_loop().call_soon(self.transport._handle_recv, msg, self.destaddr)
        resp, addr = await self.transport.read_response(msg.seqno)
        self.assertIs(resp, msg)
        self.assertIs(addr, self.destaddr)
        self.assertNotIn(resp.seqno, self.transport._responses)


if __name__ == "__main__":
    unittest.main()
