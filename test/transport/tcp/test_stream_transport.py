import asyncio
from datetime import timedelta
import logging
from pathlib import Path
import ssl
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import MagicMock, patch

from dragon.transport.tcp import transport

from test_transport import TestMessages


def setUpModule():
    """Disable logging while running these tests."""
    logging.disable()


def tearDownModule():
    """Re-enable logging after running these tests."""
    logging.disable(logging.NOTSET)


class StreamTransportTestCase(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        addr = transport.Address.from_netloc("127.0.0.1:7575")
        self.transport = transport.StreamTransport(addr)

    def tearDown(self):
        del self.transport

    @unittest.skip
    async def test_close(self):
        raise NotImplementedError

    async def test_wait_serving(self):
        self.assertIsNone(self.transport._server)
        with patch.object(self.transport, "_server", autospec=asyncio.base_events.Server) as server:
            server.is_serving.side_effect = [False, False, True]
            await self.transport.wait_serving()
        self.assertEqual(server.is_serving.call_count, 3)

    @patch.object(transport.StreamTransport, "add_connection")
    async def test_accept_connection(self, add_connection):
        # Transport.accept_connection() completes the address handshake and
        # adds the connection to internal state.
        addr = transport.Address.from_netloc("127.0.0.1:8888")
        reader = MagicMock(spec=asyncio.StreamReader)
        writer = MagicMock(spec=asyncio.StreamWriter)

        with patch.object(transport.Address, "do_handshake", return_value=addr) as do_handshake:
            await self.transport.accept_connection(reader, writer)

        do_handshake.assert_awaited_once_with(reader, writer)
        add_connection.assert_called_once_with(addr, reader, writer)

    @patch.object(transport.StreamTransport, "add_connection")
    @patch("asyncio.open_connection")
    async def test__open_connection(self, open_connection, add_connection):
        # Transport._open_connection() connects to a transport server and
        # initiates the address handshake. Once established, the connection
        # is added to the transport's internal state.
        addr = transport.Address.from_netloc("127.0.0.1:8888")
        reader = MagicMock(spec=asyncio.StreamReader)
        writer = MagicMock(spec=asyncio.StreamWriter)

        open_connection.return_value = (reader, writer)

        with patch.object(transport.Address, "do_handshake", return_value=addr) as do_handshake:
            r, w = await self.transport._open_connection(addr)

        self.assertEqual(open_connection.await_args.args, (str(addr.host), int(addr.port)))
        do_handshake.assert_awaited_once_with(reader, writer)
        add_connection.assert_called_once_with(addr, reader, writer)

    @patch.object(transport.StreamTransport, "_do_recv", new_callable=MagicMock)
    @patch("asyncio.create_task")
    def test_add_connection(self, create_task, _do_recv):
        task = MagicMock(spec=asyncio.Task)
        create_task.return_value = task

        addr = transport.Address.from_netloc("127.0.0.1:8888")
        reader = MagicMock(spec=asyncio.StreamReader)
        writer = MagicMock(spec=asyncio.StreamWriter)
        self.transport.add_connection(addr, reader, writer)

        # Verify writer added to _writers list for addr
        self.assertIn(writer, self.transport._writers[addr])
        # Verify recv task started
        _do_recv.assert_called_once_with(addr, reader)
        create_task.assert_called_once()
        # Verify recv task added to set of recv tasks for addr
        self.assertIn(task, self.transport._recv_tasks[addr])

    @patch.object(transport, "writer_addrs", None)
    @patch.object(transport, "close_writer")
    @patch.object(transport.StreamTransport, "_open_connection")
    async def test_connect(self, open_connection, close_writer):
        def _open_connection(addr):
            # Simulate connection errors when trying to open a new connection
            if open_connection.await_count < 5:
                raise ConnectionRefusedError
            reader = MagicMock(spec=asyncio.StreamReader)
            writer = MagicMock(spec=asyncio.StreamWriter)
            writer.is_closing.return_value = False
            # XXX Skip the handshake!
            # XXX Do not call add_connection() since that will start recv
            # XXX tasks. Instead, just update the _writers list.
            # self.transport.add_connection(addr, reader, writer)
            self.transport._writers[addr].append(writer)
            return reader, writer

        open_connection.side_effect = _open_connection

        # Verify self connections are prevented
        with self.assertRaises(AssertionError):
            await self.transport.connect(self.transport.addr)

        self.addAsyncCleanup(self.transport.close)

        addr = transport.Address.from_netloc("127.0.0.1:8888")

        # Verify new connection opened
        writer = await self.transport.connect(addr)
        open_connection.assert_awaited_with(addr)
        self.assertIn(writer, self.transport._writers[addr])

        # Verify existing connection is returned
        open_connection.reset_mock()
        writer2 = await self.transport.connect(addr)
        open_connection.assert_not_awaited()
        self.assertIs(writer2, writer)

        writer.is_closing.return_value = True
        open_connection.reset_mock()
        writer3 = await self.transport.connect(addr)
        open_connection.assert_awaited_with(addr)
        # Check that closed writer has been removed
        self.assertNotIn(writer, self.transport._writers[addr])
        self.assertIn(writer3, self.transport._writers[addr])
        # XXX Not sure we can speculate about how many recv tasks may be running
        # XXX at this point

    @unittest.skip
    async def _do_recv(self):
        raise NotImplementedError

    @unittest.skip
    def test__handle_send(self):
        raise NotImplementedError

    @unittest.skip
    def test__ensure_send_task(self):
        raise NotImplementedError

    @unittest.skip
    async def test__do_send(self):
        raise NotImplementedError


class StreamTransportConnectionTestCase(TestMessages, unittest.IsolatedAsyncioTestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.addr = transport.Address.from_netloc("127.0.0.1:7575")

    def setUp(self):
        self.transport = transport.StreamTransport(self.addr)

    def tearDown(self):
        del self.transport

    def _assert_transport_empty(self):
        # Verify transport data structures are cleaned up
        self.assertIsNone(self.transport._task)
        self.assertFalse(self.transport._recv_tasks)
        self.assertFalse(self.transport._send_tasks)
        self.assertFalse(self.transport._writers)

    async def _open_client_connection(self):
        opts = self.transport.default_connection_options.new_child(
            self.transport.connection_options[self.transport.addr]
        )
        return await asyncio.open_connection(
            str(self.transport.addr.host),
            int(self.transport.addr.port),
            **opts,
        )

    async def test_accept_connection(self):
        client_req = self.SendRequest(seqno=42)
        client_addr = transport.Address.from_netloc("127.0.0.1:8686")

        self.transport.start()
        await self.transport.wait_serving()
        try:
            reader, writer = await self._open_client_connection()
            try:
                server_addr = await client_addr.do_handshake(reader, writer)
                self.assertEqual(server_addr, self.transport.addr)

                await transport.write_message(writer, client_req)
            finally:
                await transport.close_writer(writer)

            req, addr = await self.transport.read_request()
            self.assertEqual(req, client_req)
            self.assertEqual(addr, client_addr)
        finally:
            await self.transport.stop()

        self._assert_transport_empty()

    async def _start_server(self, accept_connection):
        return await asyncio.start_server(
            accept_connection,
            str(self.transport.addr.host),
            int(self.transport.addr.port),
            **self.transport.server_options,
        )

    async def test_open_connection(self):
        server_req = self.SendRequest(seqno=42)
        server_addr = transport.Address.from_netloc("127.0.0.1:8686")

        async def accept_connection(reader, writer):
            try:
                client_addr = await server_addr.do_handshake(reader, writer)
                self.assertEqual(client_addr, self.transport.addr)

                await transport.write_message(writer, server_req)
            finally:
                await transport.close_writer(writer)

        server = await self._start_server(accept_connection)
        async with server:
            task = asyncio.create_task(server.serve_forever())
            try:
                await self.transport._open_connection(self.transport.addr)
                req, addr = await self.transport.read_request()
                self.assertEqual(req, server_req)
                self.assertEqual(addr, server_addr)
            finally:
                await self.transport.close()
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        self._assert_transport_empty()


try:
    from dragon.transport.x509 import (
        CertificateAuthority,
        dragon_uri,
        generate_server_csr,
        generate_server_self_signed_cert,
        pem_encode,
        pem_encode_pkcs8,
    )
except ImportError:
    pass
else:

    class StreamTransportSSLConnectionTestCase(StreamTransportConnectionTestCase):
        """StreamTransport tests where transport supports basic SSL using
        self-signed certs and no verification.
        """

        @classmethod
        def setUpClass(cls):
            super().setUpClass()
            cls._uri = dragon_uri(0, cls.addr.host, cls.addr.port)
            # Create temporary directory for key, cert, and CA files
            cls._tempdir = TemporaryDirectory()
            cls._workdir = Path(cls._tempdir.name)
            cls._generate_ssl_files()

        @classmethod
        def tearDownClass(cls):
            cls._tempdir.cleanup()

        def setUp(self):
            super().setUp()
            self.transport.server_options.update(ssl=self._create_server_ssl_context())
            self.transport.default_connection_options.update(ssl=self._create_client_ssl_context())

        @classmethod
        def _generate_ssl_files(cls):
            # Generate self-signed certificate; important to use same IP addr as
            # transport will use.
            key, cert = generate_server_self_signed_cert(cls.addr.host, cls._uri, valid_for=timedelta(minutes=5))
            # Write key file
            cls._keyfile = cls._workdir / "server-key.pem"
            with open(cls._keyfile, "wb") as f:
                f.write(pem_encode_pkcs8(key))
            # Write cert file
            cls._certfile = cls._workdir / "server-cert.pem"
            with open(cls._certfile, "wb") as f:
                f.write(pem_encode(cert))

        def _create_server_ssl_context(self):
            """Returns SSL Context for servers."""
            ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
            ctx.load_cert_chain(self._certfile, self._keyfile)
            return ctx

        def _create_client_ssl_context(self):
            """Returns SSL Context for client connections."""
            ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
            # Disable server verification since it's a self signed cert
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            return ctx

    class StreamTransportMutualTLSConnectionTestCase(StreamTransportSSLConnectionTestCase):
        """StreamTransport tests where transport supports mutual-TLS using certs
        issued by trusted CA.
        """

        @classmethod
        def _generate_ssl_files(cls):
            ca = CertificateAuthority.generate()
            key, csr = generate_server_csr(cls.addr.host, cls._uri)
            cert = ca.issue_server_certificate(csr, valid_for=timedelta(minutes=5))
            # Write CA file
            cls._cafile = cls._workdir / "ca.pem"
            with open(cls._cafile, "wb") as f:
                f.write(pem_encode(ca.certificate))
            # Write key file
            cls._keyfile = cls._workdir / "server-key.pem"
            with open(cls._keyfile, "wb") as f:
                f.write(pem_encode_pkcs8(key))
            # Write cert file
            cls._certfile = cls._workdir / "server-cert.pem"
            with open(cls._certfile, "wb") as f:
                f.write(pem_encode(cert))

        def _create_server_ssl_context(self):
            """Returns SSL Context for servers."""
            ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
            ctx.load_cert_chain(self._certfile, self._keyfile)
            ctx.load_verify_locations(self._cafile)
            # Require client verification (e.g., enable privacy without authentication)
            ctx.verify_mode = ssl.CERT_REQUIRED
            return ctx

        def _create_client_ssl_context(self):
            """Returns SSL Context for client connections."""
            ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
            ctx.load_cert_chain(self._certfile, self._keyfile)
            ctx.load_verify_locations(self._cafile)
            assert ctx.verify_mode == ssl.CERT_REQUIRED
            return ctx


if __name__ == "__main__":
    unittest.main()
