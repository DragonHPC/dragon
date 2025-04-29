from asyncio import StreamReader, StreamWriter
from ipaddress import ip_address
import unittest
from unittest.mock import MagicMock, patch

from dragon.transport.tcp import messages
from dragon.transport.tcp import transport


class AddressIPv4TestCase(unittest.IsolatedAsyncioTestCase):

    IP = ip_address("127.0.0.1")
    NETLOC = ["127.0.0.1", "127.0.0.1"]

    LOOPBACK = transport.LOOPBACK_ADDRESS_IPv4

    Hello = messages.Hello

    def test_loopback(self):
        self.assertEqual(self.LOOPBACK, transport.Address(self.IP, None))

    def test_from_netloc(self):
        addr = transport.Address(self.IP, 8888)
        for s in self.NETLOC:
            self.assertEqual(self.LOOPBACK, transport.Address.from_netloc(s))
            self.assertEqual(addr, transport.Address.from_netloc(f"{s}:8888"))

    def test___str__(self):
        s = self.NETLOC[0]
        self.assertEqual(s, str(self.LOOPBACK))
        self.assertEqual(f"{s}:8888", str(transport.Address(self.IP, 8888)))

    def test_hello(self):
        self.assertRaises(AssertionError, self.LOOPBACK.hello)
        hello = transport.Address(self.IP, 8888).hello()
        self.assertIsInstance(hello, self.Hello)
        self.assertEqual(hello.host, self.IP)
        self.assertEqual(hello.port, 8888)

    def test_from_hello(self):
        addr = transport.Address.from_hello(self.Hello(self.IP, 8888))
        self.assertEqual(addr.host, self.IP)
        self.assertEqual(addr.port, 8888)

    # XXX Note that write_message() and read_message() must be patched from
    # XXX transport since they are imported there.
    @patch.object(transport, "write_message")
    @patch.object(transport, "read_message")
    async def _do_handshake(self, read_message, write_message):
        my_addr = transport.Address(self.IP, 8888)
        peer_addr = transport.Address(self.IP, 9999)
        read_message.return_value = peer_addr.hello()

        reader = MagicMock(spec=StreamReader)
        writer = MagicMock(spec=StreamWriter)

        addr = await my_addr.do_handshake(reader, writer)

        # Verify correct peer address is returned
        self.assertEqual(addr, peer_addr)

        # Verify mocks were called correctly
        write_message.assert_called_once_with(writer, my_addr.hello())
        read_message.assert_awaited_once_with(reader)


class AddressIPv6TestCase(AddressIPv4TestCase):

    IP = ip_address("::1")
    NETLOC = ["[::1]"]

    LOOPBACK = transport.LOOPBACK_ADDRESS_IPv6

    Hello = messages.Hello6


if __name__ == "__main__":
    unittest.main()
