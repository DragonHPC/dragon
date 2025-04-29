from contextlib import asynccontextmanager
from ipaddress import ip_address
import math
import unittest
from uuid import uuid4

from dragon.transport.tcp import messages
from dragon.transport.tcp import transport
from dragon.transport.tcp.util import seconds_remaining


class TransportMessageTestCase(unittest.TestCase):

    @unittest.skip
    def test_init_subclass(self):
        raise NotImplementedError


class RequestTestCase(unittest.TestCase):

    def test_deadline_getter(self):
        req = messages.Request(seqno=None, timeout=1.0, channel_sd=None)
        self.assertEqual(req.deadline, req.timestamp + req.timeout)

    def test_deadline_getter_none(self):
        req = messages.Request(seqno=None, timeout=math.inf, channel_sd=None)
        self.assertEqual(req.deadline, math.inf)

    def test_deadline_setter_none(self):
        req = messages.Request(seqno=None, timeout=1.0, channel_sd=None)
        req.deadline = math.inf
        self.assertEqual(req.timeout, math.inf)

    def test_deadline_setter_increment_timeout(self):
        req = messages.Request(seqno=None, timeout=1.0, channel_sd=None)
        ts = req.timestamp  # preserve original timestamp
        req.deadline += 1.0
        # check that original timeout was increased by 1.0s
        self.assertEqual(req.deadline - ts, 2.0)

    def test_timeout_scales_to_zero(self):
        req = messages.Request(seqno=None, timeout=1.0, channel_sd=None)
        timeout, _ = seconds_remaining(req.deadline - 1.1)
        self.assertEqual(timeout, 0)


class TransmittableTestCase(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        if not (hasattr(self, "msg") and hasattr(self, "data")):
            self.skipTest('Missing "msg" and "data" attributes')
        self.assertIsInstance(self.msg, messages.TransportMessage)
        self.assertIsInstance(self.data, bytes)

    async def asyncSetUp(self):
        self.reader, self.writer = await transport.create_pipe_streams()

    @asynccontextmanager
    async def drain_and_close_writer(self):
        try:
            yield self.writer
            await self.writer.drain()
        finally:
            await transport.close_writer(self.writer)

    async def test_read(self):
        async with self.drain_and_close_writer() as w:
            w.write(self.data)
        req = await messages.read_message(self.reader)
        self.assertEqual(req, self.msg)

    async def test_write(self):
        async with self.drain_and_close_writer() as w:
            await messages.write_message(w, self.msg)
        data = await self.reader.read()
        self.assertEqual(data, self.data)


class HelloTestCase(TransmittableTestCase):

    @classmethod
    def setUpClass(cls):
        cls.msg = messages.Hello(ip_address("127.0.0.1"), 8888)
        cls.data = b'\x40\x7f\x00\x00\x01"\xb8'


class Hello6TestCase(TransmittableTestCase):

    @classmethod
    def setUpClass(cls):
        cls.msg = messages.Hello6(ip_address("::1"), 8888)
        cls.data = b'\x60\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01"\xb8'


class SendRequestTestCase(TransmittableTestCase):

    @classmethod
    def setUpClass(cls):
        cls.msg = messages.SendRequest(
            seqno=1,
            timeout=0.5,
            channel_sd=b"channel desc",
            return_mode=messages.SendReturnMode.WHEN_BUFFERED,
            sendhid=uuid4(),
            payload=b"payload",
            hints=0,
            clientid=0,
        )
        cls.data = (
            b"\x01\x00\x00\x00\x00\x00\x00\x00\x01?\x00\x00\x00\x00\x0cchannel desc\x02"
            + cls.msg.sendhid.bytes
            + b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x07payload"
        )


class SendMemoryRequestTestCase(TransmittableTestCase):

    @classmethod
    def setUpClass(cls):
        cls.msg = messages.SendMemoryRequest(
            seqno=1,
            timeout=0.5,
            channel_sd=b"channel desc",
            return_mode=messages.SendReturnMode.WHEN_BUFFERED,
            sendhid=uuid4(),
            payload=b"payload",
            mem_sd=b"memory desc",
            clientid=0,
            hints=0,
        )
        cls.data = (
            b"\x02\x00\x00\x00\x00\x00\x00\x00\x01?\x00\x00\x00\x00\x0cchannel desc\x02"
            + cls.msg.sendhid.bytes
            + b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x07payload\x00\x0bmemory desc"
        )


class RecvRequestTestCase(TransmittableTestCase):

    @classmethod
    def setUpClass(cls):
        cls.msg = messages.RecvRequest(seqno=1, timeout=0.5, channel_sd=b"channel desc")
        cls.data = b"\x03\x00\x00\x00\x00\x00\x00\x00\x01?\x00\x00\x00\x00\x0cchannel desc"


class EventRequestTestCase(TransmittableTestCase):

    @classmethod
    def setUpClass(cls):
        cls.msg = messages.EventRequest(seqno=1, timeout=0.5, channel_sd=b"channel desc", mask=3)
        cls.data = b"\x04\x00\x00\x00\x00\x00\x00\x00\x01?\x00\x00\x00\x00\x0cchannel desc\x00\x03"


class ErrorResponseTestCase(TransmittableTestCase):

    @classmethod
    def setUpClass(cls):
        cls.msg = messages.ErrorResponse(seqno=1, errno=42, text="error message")
        cls.data = b"\xff\x00\x00\x00\x00\x00\x00\x00\x01\x00*\x00\x00\x00\x00\x00\x00\x00\rerror message"


class SendResponseTestCase(TransmittableTestCase):

    @classmethod
    def setUpClass(cls):
        cls.msg = messages.SendResponse(seqno=1)
        cls.data = b"\xfe\x00\x00\x00\x00\x00\x00\x00\x01"


class RecvResponseTestCase(TransmittableTestCase):

    @classmethod
    def setUpClass(cls):
        cls.msg = messages.RecvResponse(seqno=1, payload=b"payload", clientid=0, hints=0)
        cls.data = b"\xfc\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x07payload"


class EventResponseTestCase(TransmittableTestCase):

    @classmethod
    def setUpClass(cls):
        cls.msg = messages.EventResponse(seqno=1, errno=0, result=42)
        cls.data = b"\xfb\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00*"


class MessagesTestCase(unittest.IsolatedAsyncioTestCase):

    # XXX Not sure of a more effective test compared to all the
    # XXX TransmittableTestCase subclasses.

    @unittest.skip
    def test__get_io_annotations(self):
        raise NotImplementedError

    @unittest.skip
    async def test_read_message(self):
        raise NotImplementedError

    @unittest.skip
    async def test_write_message(self):
        raise NotImplementedError


if __name__ == "__main__":
    unittest.main()
