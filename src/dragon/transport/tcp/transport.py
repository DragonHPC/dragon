from __future__ import annotations

import asyncio
from collections import ChainMap, defaultdict
from dataclasses import dataclass
from functools import singledispatchmethod
from ipaddress import ip_address, IPv4Address, IPv6Address
from itertools import chain, count
import logging
import os
from typing import IO, Union
from urllib.parse import urlsplit
from warnings import warn
from weakref import WeakSet, WeakValueDictionary

from .errno import *
from .messages import *
from .task import TaskMixin, run_forever
from .util import unget_nowait


LOGGER = logging.getLogger("dragon.transport.tcp.transport")


@dataclass(frozen=True)
class Address:
    """Node address"""

    host: Union[IPv4Address, IPv6Address]
    """Host IP address"""

    port: int = None
    """Port number"""

    @classmethod
    def from_netloc(cls, netloc: str) -> Address:
        """Returns an `Address` from a *netloc* string.

        Uses `urllib.parse.urlsplit` to extract host and port information from
        the given string. The extracted hostname is expected to be a valid IPv4
        or IPv6 address and will be validated.

        :param netloc: String of the form ``host:port``
        :raise ValueError:
            If host and port cannot be extracted from the specified string or
            cannot be coerced into appropriate types, IP address and integer
            respectively.

        :example:
            >>> Address.from_netloc('127.0.0.1')
            >>> Address.from_netloc('[127.0.0.1]')
            >>> Address.from_netloc('[::1]')
            >>> Address.from_netloc('127.0.0.1:8888')
            >>> Address.from_netloc('[127.0.0.1]:8888')
            >>> Address.from_netloc('[::1]:8888')

        .. seealso:: :rfc:`2732` *Format for Literal IPv6 Addresses in URL's*

        """
        if not netloc:
            raise ValueError("A valid hostname or IPv4 or IPv6 address is required.")
        info = urlsplit("tcp://" + str(netloc))
        host = ip_address(info.hostname)
        return cls(host, info.port)

    def __str__(self):
        """Returns netloc string."""
        if isinstance(self.host, IPv6Address):
            host = f"[{self.host}]"
        else:
            host = str(self.host)
        if self.port is None:
            return host
        return f"{host}:{self.port}"

    def hello(self) -> Union[Hello, Hello6]:
        """Returns corresponding Hello message for this address."""
        assert self.port is not None
        cls = Hello6 if isinstance(self.host, IPv6Address) else Hello
        return cls(self.host, self.port)

    @classmethod
    def from_hello(cls, hello: Union[Hello, Hello6]) -> Address:
        """Returns Address from `Hello` message."""
        return cls(ip_address(hello.host), int(hello.port))

    async def do_handshake(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> Address:
        """Exchange Hello messages with peer.

        :param reader: Stream for messages from peer
        :param writer: Stream for messages to peer
        :return: Peer address
        """
        await write_message(writer, self.hello())
        resp = await read_message(reader)
        assert isinstance(resp, Hello)
        return type(self).from_hello(resp)


LOOPBACK_ADDRESS_IPv4 = Address.from_netloc("127.0.0.1")
LOOPBACK_ADDRESS_IPv6 = Address.from_netloc("[::1]")


class Transport:
    """Base class for TCP transports; provides loopback."""

    def __init__(self, addr: Address):
        """
        :param addr: Server address to accept peer connections.
        """
        self.addr = addr
        """Server address to accept peer connections."""
        self._seqno = count(start=1)
        self._responses = {}
        self._last_request = defaultdict(int)
        self._requests = asyncio.Queue()

    def write_request(self, req: Request, addr: Address, /) -> asyncio.Future:
        """Send a `Request`.

        :param req: `Request` message to send
        :param addr: Recipient `Address`
        :return:
            `asyncio.Future` which will be set to (`Response`, `Address`) pair
            when the corresponding response is available.

        """
        LOGGER.debug(f"Writing request to {addr}: {req}")
        # Set seqno before queuing or handling request
        req.seqno = next(self._seqno)
        # Create future that will be completed when the response is received
        fut = self._responses[req.seqno] = asyncio.Future()
        try:
            self._handle_send(req, addr)
        except:
            # Clean up the future response
            try:
                del self._responses[req.seqno]
            except KeyError:
                pass
            raise
        return fut

    def write_response(self, resp: Response, addr: Address, /) -> None:
        """Send a `Response`.

        :param resp: `Response` message to send
        :param addr: Recipient `Address`
        """
        LOGGER.debug(f"Writing response to {addr}: {resp}")
        self._handle_send(resp, addr)

    def _handle_send(self, msg: TransportMessage, addr: Address, /) -> None:
        # Reminder: Subclasses (e.g., StreamTransport) may rely on this
        # implementation to deliver messages to itself. As a result, any
        # additional overhead introduced here may impact subclasses.
        assert addr == self.addr

        # Even though the message is not actually sent anywhere; but rather,
        # passed immediately to the receive handler, it still needs to process
        # the SendResponse for SendRequests with IMMEDIATELY or WHEN_BUFFERED
        # return modes.
        if isinstance(msg, SendRequest) and msg.return_mode in (
            SendReturnMode.IMMEDIATELY,
            SendReturnMode.WHEN_BUFFERED,
        ):
            resp = SendResponse(msg.seqno)
            self._handle_recv(resp, self.addr)

        # Important: Do NOT call msg._io_event.set() here! The message is never
        # actually written to and read from a stream, so Server and Client are
        # responsible for properly setting the I/O event when the message has
        # been handled.  Otherwise the sender may misbehave, e.g., prematurely
        # destroy the corresponding Dragon message!

        # Immediately handle receiving the message.
        self._handle_recv(msg, self.addr)

    @singledispatchmethod
    def _handle_recv(self, msg: TransportMessage, addr: Address, /) -> None:
        """Handle a message received from the specified address."""
        # Default handler immediately sends error response
        LOGGER.error(f"Received unsupported type of message from {addr}: {type(msg)}")
        # An unsupported message most likely does not have a sequence number,
        # so default to seqno=0 in that case, which should NOT be used
        # by any valid transport since sequence numbers start at 1.
        resp = ErrorResponse(
            getattr(msg, "seqno", 0), DRAGON_NOT_IMPLEMENTED, f"Unsupported type of message: {type(msg)}"
        )
        self.write_response(resp, addr)
        # This handler is a dead end; no client or server will ever process
        # this transport message. As a result, we need to ensure the I/O event
        # is set just in case something else is waiting on it.
        msg._io_event.set()

    @_handle_recv.register
    def _(self, resp: Response, addr: Address, /) -> None:
        """Handle a `Response`."""
        LOGGER.debug(f"Received response from {addr}: {resp}")
        try:
            fut = self._responses.pop(resp.seqno)
        except KeyError:
            LOGGER.warning(f"Received unexpected response: {resp}")
            # May include additional text (e.g., ErrorResponse)
            text = getattr(resp, "text", "")
            if text:
                LOGGER.warning(resp.text)
        else:
            fut.set_result((resp, addr))

    @_handle_recv.register
    def _(self, req: Request, addr: Address, /) -> None:
        """ "Handle a `Request."""
        LOGGER.debug(f"Received request from {addr}: {req}")
        # Protect against inadvertant replays by checking the last seqno from
        # each address.
        if req.seqno <= self._last_request[addr]:
            LOGGER.error("Received duplicate request from {addr}: {req}")
            return
        self._last_request[addr] = req.seqno
        # Queue request so it can be read
        self._requests.put_nowait((req, addr))

    async def read_request(self) -> tuple[Request, Address]:
        """:return: Next `Request` received, when available."""
        req, addr = await self._requests.get()
        LOGGER.debug(f"Read request from {addr}: {req}")
        return req, addr

    async def read_response(self, seqno) -> tuple[Response, Address]:
        try:
            fut = self._responses[seqno]
        except KeyError:
            raise ValueError(f"Invalid sequence number: {seqno}")
        resp, addr = await fut
        return resp, addr


class StreamTransport(Transport, TaskMixin):
    """Asynchronous message transport using `asyncio-streams`.

    `StreamTransport` instances must be given a valid `Address` which indicates
    the host and port on which to accept peer connections::

        addr = Address.from_netloc('127.0.0.1:8888')
        transport = StreamTransport(addr)

    Additional server options to be passed to `asyncio.start_server` may be set
    by updating the `StreamTransport.server_options` dictionary. For example,
    to protect the transport listening socket using TLS::

        import ssl

        # Create server SSLContext
        ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        # Disable client verification (e.g., enable privacy without authentication)
        ctx.verify_mode = ssl.CERT_NONE

        transport.server_options.update(ssl=ctx)

    Similarly, default options for all transport client connections, which are
    passed to `asyncio.open_connection` may be set by updating the
    `StreamTransport.default_connection_options` dictionary::

        # Create SSLContext for client connections
        ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        # Disable server verification (e.g., enable privacy without authentication)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

        transport.default_connection_options.update(ssl=ctx)

    Connection options for specific peers may be set by updating the
    `StreamTransport.connection_options` dictionary using the corresponding
    `Address` as the key::

        # Disable SSL for connections to specific peer
        peer_addr = Address.from_netloc('127.0.0.1:9999')

        transport.connection_options[peer_addr].update(ssl=None)

    After desired server and connection options have been configured, the
    transport must be started to run in a separate task::

        # Run transport in a separate task with a specific name
        transport.start(name=f'Transport-{transport.addr}')

        # Wait for the transport to start the server and be accepting
        # connections.
        await transport.wait_serving()

    When running, a `StreamTransport` will automatically accept and open
    connections from/to peers as necessary. See `StreamTransport.connect` to
    explicitly create a connection to a peer; although, this is not typically
    needed. Immediately after a connection is accepted or opened, a lightweight
    handshake is required to identify the client's corresponding server address
    before the connection may be considered
    *established* and messages freely exchanged. For more details, see
    `StreamTransport._do_handshake`.

    Once a connection is established, the transport adds the corresponding
    `asyncio.StreamWriter` to a list of available writers and creates a task to
    run a receive-loop for the corresponding `asyncio.StreamReader`, see
    `StreamTransport._do_recv`. A receive-loop task does not terminate unless
    its `asyncio.StreamReader` is closed or encounters an unrecoverable error.
    When a message is queued to be sent (e.g., `StreamTransport.write_request`,
    `StreamTransport.write_response`), the transport ensures a corresponding
    send-loop task exists or creates one using using the most recent
    `asyncio.StreamWriter` available for the recipient, see
    `StreamTransport._do_send`.

    Messages are not tied to underlying connections and do not need to be sent
    and received on corresponding `asyncio.StreamReader` and
    `asyncio.StreamWriter` pairs.  This enables the transport to mitigate
    intermittent connection failures and provide resilient message delivery.  As
    a result, a `Response` is not guaranteed to be received on the
    `asyncio.StreamReader` corresponding to the `asyncio.StreamWriter` on which
    the `Request` was sent.  Conversely, a `Response` is not guaranteed to be
    sent on the `asyncio.StreamWriter` corresponding to the
    `asyncio.StreamReader` on which the `Request` was received.

    Use `StreamTransport.write_request` to queue a `Request` to be sent to a
    peer. It assigns a sequence number to the `Request` and returns an
    `asyncio.Future`, which will be completed when the corresponding `Response`
    is received. It is the responsibility of the caller to wait for the returned
    `asyncio.Future` instance to be completed in order to actually process the
    `Response`. For example::

        from . import messages

        # Create request
        req = messages.SendRequest(...)

        # Send request to recipient
        fut = transport.write_request(req, peer_addr)

        # Wait for response
        resp, resp_addr = await fut

        # Check that the responder was the request recipient
        assert resp_addr == peer_addr

    Use `StreamTransport.read_request` to get the *next* `Request` to process
    and `StreamTransport.write_response` to send a corresponding `Response`. For
    example::

        # Receive request
        req, addr = await transport.read_request()

        # Process request and create response
        resp = messages.SendResponse(req.seqno)

        # Send response
        transport.write_response(resp, addr)

    To stop the transport task and wait for it to complete::

        await transport.stop()

    """

    IDLE_SEND_TIMEOUT = 60.0

    def __init__(self, addr: Address):
        """
        :param addr: Server address to accept peer connections.
        """
        super().__init__(addr)
        self.server_options = {}
        """Server connection options."""
        self.default_connection_options = ChainMap()
        """Default connection options for all client connections."""
        self.connection_options = defaultdict(dict)
        """Connection options for specific Addresses applied on top of the
        default connection options.
        """
        self._server = None
        self._connection_locks = WeakValueDictionary()
        self._writers = defaultdict(list)
        self._recv_tasks = defaultdict(WeakSet)
        self._mailboxes = defaultdict(asyncio.Queue)
        self._send_tasks = WeakValueDictionary()
        self._oob_connect = False
        self._oob_accept = False

    @run_forever
    async def run(self):
        """Run the transport server.

        Typically ran in a separate `asyncio.Task` via `StreamTransport.start`.
        """
        if self._oob_accept:
            self._server = await asyncio.start_server(
                self.accept_connection, "localhost", int(self.addr.port), **self.server_options
            )
        else:
            self._server = await asyncio.start_server(
                self.accept_connection, str(self.addr.host), int(self.addr.port), **self.server_options
            )
        try:
            async with self._server:
                await self._server.serve_forever()
        finally:
            self._server = None

    async def stop(self) -> None:
        await self.close()
        await super().stop()

    async def close(self) -> None:
        # Cancel and clean up recv tasks
        recv_tasks = list(chain.from_iterable(self._recv_tasks.values()))
        if recv_tasks:
            for t in recv_tasks:
                t.cancel()
            await asyncio.gather(*recv_tasks, return_exceptions=True)
        self._recv_tasks.clear()
        # Cancel and clean up send tasks
        send_tasks = list(self._send_tasks.values())
        if send_tasks:
            for t in send_tasks:
                t.cancel()
            await asyncio.gather(*send_tasks, return_exceptions=True)
        self._send_tasks.clear()
        # Close and clean up writers
        closing_writers = [close_writer(w) for w in chain.from_iterable(self._writers.values())]
        if closing_writers:
            await asyncio.gather(*closing_writers, return_exceptions=True)
        self._writers.clear()

    def is_serving(self):
        return self._server is not None and self._server.is_serving()

    async def wait_serving(self, interval: float = 0.1):
        """Wait for the transport server to be running and serving clients.

        :param interval: Sleep interval between checking server status.

        :example:
            Intended to be called after `StreamTransport.start` to ensure the
            server is running before proceeding::

                transport = StreamTransport(Address.from_netloc('127.0.0.1:8888'))
                transport.start()
                await transport.wait_serving()

        """
        while not self.is_serving():
            await asyncio.sleep(interval)

    async def wait_started(self, *args, **kwds):
        warn("Use wait_serving() instead of wait_started()", DeprecationWarning)
        return self.wait_serving(*args, **kwds)

    async def accept_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Accept a client connection.

        Completes handshake with client before starting the receive-loop with
        `reader` and adding the `writer` to the available pool.

        .. note::
            Called by the transport server when accepting new connections. Not
            intended to be called directly.
        """
        addr = await self.addr.do_handshake(reader, writer)
        # XXX Verifying the connection in this way prevents test code from
        # XXX forging addresses, but also impacts legitimate use cases, e.g.,
        # XXX proxies. The only resolution is to real client authentication,
        # XXX e.g., verify the advertised address is a valid SAN in an X.509
        # XXX certificate.
        ## Verify connection is to the advertised host; port is not applicable
        ## from the server side.
        # host, _ = writer.get_extra_info('peername')
        # assert addr.host == ip_address(host)
        self.add_connection(addr, reader, writer)

    async def _open_connection(self, addr: Address, /) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        """Open a connection to a peer transport server.

        :param addr: Peer address
        :return: `asyncio.StreamReader` and `asyncio.StreamWriter` pair
        """
        opts = self.default_connection_options.new_child(self.connection_options[addr])
        if self._oob_connect or self._oob_accept:
            reader, writer = await asyncio.open_connection("localhost", int(addr.port), **opts)
            await self.addr.do_handshake(reader, writer)
        else:
            reader, writer = await asyncio.open_connection(str(addr.host), int(addr.port), **opts)
            addr = await self.addr.do_handshake(reader, writer)

        # XXX See comment above in accept_connection() on why this is not a
        # XXX tenable workaround for actual server authentication.
        ## Verify connection is to the advertised address
        # host, port = writer.get_extra_info('peername')
        # assert addr == Address(ip_address(host), int(port))
        self.add_connection(addr, reader, writer)
        return reader, writer

    def add_connection(self, addr: Address, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self._writers[addr].append(writer)
        task = asyncio.create_task(self._do_recv(addr, reader), name=f"{type(self).__name__}-Receiver-{addr}")
        self._recv_tasks[addr].add(task)
        LOGGER.debug(f"Added connection to {addr}")

    async def connect(self, addr: Address) -> asyncio.StreamWriter:
        """Returns an open `asyncio.StreamWriter` connected to the specified
        address, opening a new connection if necessary.

        A task will be created to run the receive-loop for the corresponding
        `asyncio.StreamReader` as appropriate.
        """
        # Do not allow self-connections, since messages to oneself are
        # short-circuited and never actually written to a connection.
        assert addr != self.addr
        # XXX Is there a race condition here? If peers attempt to open
        # XXX connections simultaneously, it is not possible to guarantee
        # XXX the resulting StreamWriters are ordered the same at both, i.e.,
        # XXX they may each have different views on which is the "latest". As a
        # XXX result two sockets may still be used.
        try:
            lock = self._connection_locks[addr]
        except KeyError:
            lock = self._connection_locks[addr] = asyncio.Lock()
        async with lock:
            # Search for open writers, opening a new connection as appropriate.
            while True:
                open_writers = [w for w in self._writers[addr] if not w.is_closing()]
                # If there are open writers, then this loop breaks and we only
                # want to keep one, so clearing self._writers[addr] here is
                # fine. Otherwise, if there are no open writers, then clearing
                # self._writers[addr] before waiting for an open connection
                # simply gets rid of closed writers, if any.
                self._writers[addr].clear()
                if open_writers:
                    break
                # Loop after _open_connection() completes to select an open
                # writer instead of choosing the writer returned in case any
                # connections were accepted while waiting.
                try:
                    await self._open_connection(addr)
                except ConnectionRefusedError:
                    LOGGER.exception(f"Failed to open connection to {addr}")
                    continue
            # TODO I now think sorting the open_writers is pointless, remove?
            # Sort open writers to increase the chance that both peers choose
            # to keep the same  attempt to reduce the number of open sockets
            open_writers.sort(key=writer_addrs)
            writer = open_writers.pop()
            self._writers[addr].append(writer)
        # Close remaining open writers.
        await asyncio.gather(*[close_writer(w) for w in open_writers], return_exceptions=True)
        return writer

    @run_forever
    async def _do_recv(self, addr: Address, reader: asyncio.StreamReader):
        while not reader.at_eof():
            try:
                msg = await read_message(reader)
            except asyncio.IncompleteReadError:
                # NOTE: IncompleteReadError.partial has the portion of bytes
                # read; however, since the stream will have gotten EOF there
                # isn't much to do with an incomplete message.
                continue
            LOGGER.debug(f"Received from {addr}: {msg}")
            try:
                self._handle_recv(msg, addr)
            except Exception as e:
                if isinstance(msg, Request):
                    resp = ErrorResponse(msg.seqno, DRAGON_FAILURE, "Uncaught exception handling message")
                    self.write_response(resp, addr)
                raise

    def _handle_send(self, msg: TransportMessage, addr: Address, /) -> None:
        # Short-circuit delivery when sending to myself
        if addr == self.addr:
            super()._handle_send(msg, addr)
            return
        if isinstance(msg, SendRequest) and msg.return_mode == SendReturnMode.IMMEDIATELY:
            resp = SendResponse(msg.seqno)
            self._handle_recv(resp, self.addr)
        # Enqueue outgoing message
        self._mailboxes[addr].put_nowait(msg)
        # Ensure send task is running
        self._ensure_send_task(addr)

    def _ensure_send_task(self, addr: Address):
        # Check if send task is still running
        try:
            task = self._send_tasks[addr]
        except KeyError:
            task = None
        else:
            if task.done():
                # Remove reference to task
                del self._send_tasks[addr]
                task = None
        if task is None:
            # (Re-)Start send task
            LOGGER.debug(f"Starting sender: {type(self).__name__}-Sender-{addr}")
            self._send_tasks[addr] = asyncio.create_task(
                self._do_send(addr), name=f"{type(self).__name__}-Sender-{addr}"
            )

    @run_forever
    async def _do_send(self, addr: Address) -> None:
        writer = await self.connect(addr)
        while True:
            try:
                msg = await asyncio.wait_for(self._mailboxes[addr].get(), self.IDLE_SEND_TIMEOUT)
            except asyncio.TimeoutError:
                if self._mailboxes[addr].empty():
                    LOGGER.debug("Cleaning up idle mailbox")
                    # Idle timeout and still no outgoing messages, shutdown
                    del self._mailboxes[addr]
                    break
                continue

            LOGGER.debug(f"Sending to {addr}: {msg}")
            if isinstance(msg, Request):
                # Normalizes Request timeout based on the current monotonic
                # clock. May scale timeout down to 0 if deadline exceeded, but
                # does not preemptively raise DRAGON_TIMEOUT in order to satisfy
                # "try once" semantics.
                msg.deadline = msg.deadline
            try:
                await write_message(writer, msg)
            except ConnectionError:
                LOGGER.exception(f"Connection error while writing to {addr}: {msg}")
                unget_nowait(self._mailboxes[addr], msg)
                # Close existing writer, then get a new one and retry
                await close_writer(writer)
                writer = await self.connect(addr)
            except:
                LOGGER.exception(f"Uncaught error while writing to {addr}: {msg}")
                unget_nowait(self._mailboxes[addr], msg)
                raise
            else:
                self._mailboxes[addr].task_done()
                if isinstance(msg, SendRequest) and msg.return_mode == SendReturnMode.WHEN_BUFFERED:
                    resp = SendResponse(msg.seqno)
                    self._handle_recv(resp, self.addr)


def writer_addrs(writer):
    addrs = [writer.get_extra_info("sockname"), writer.get_extra_info("peername")]
    addrs.sort()
    return addrs


async def close_writer(writer: asyncio.StreamWriter) -> None:
    """Write EOF and close an `asyncio.StreamWriter`."""
    if writer.is_closing():
        return
    if writer.can_write_eof():
        writer.write_eof()
    writer.close()
    try:
        await writer.wait_closed()
    except NotImplementedError:
        pass


async def create_streams(rfile: Optional[IO], wfile: Optional[IO]) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    """Creates `asyncio.StreamReader` and `asyncio.StreamWriter` instances that
    wrap the specified file-like objects.

    :param rfile: Read file
    :param wfile: Write file

    :example:
        From stdin and stdout::

            import sys

            reader, writer = await create_streams(sys.stdin, sys.stdout)

    """
    if rfile is None and wfile is None:
        raise ValueError("Requires at least one file object")
    loop = asyncio.get_event_loop()
    if rfile is not None:
        reader = asyncio.StreamReader(loop=loop)
        await loop.connect_read_pipe(lambda: asyncio.StreamReaderProtocol(reader, loop=loop), rfile)
    else:
        reader = None
    if wfile is not None:
        w_transport, w_protocol = await loop.connect_write_pipe(
            lambda: asyncio.streams.FlowControlMixin(loop=loop), wfile
        )
        writer = asyncio.StreamWriter(w_transport, w_protocol, reader, loop)
    else:
        writer = None
    return reader, writer


async def create_pipe_streams() -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    """Creates `asyncio.StreamReader` and `asyncio.StreamWriter` instaces from
    an `os.pipe`.

    :example:
        Useful for testing `read_message` and `write_message` round trip::

            from .messages import SendRequest, read_message, write_message

            reader, writer = create_pipe_streams()

            src_req = SendRequest(...)

            await write_message(writer, src_req)
            await close_writer(writer)

            dest_req = await read_message(reader)

            assert src_req == dest_req

    """
    r, w = os.pipe()
    reader, _ = await create_streams(os.fdopen(r, "rb"), None)
    _, writer = await create_streams(None, os.fdopen(w, "wb"))
    return reader, writer


async def create_pipe_connections() -> (
    tuple[tuple[asyncio.StreamReader, asyncio.StreamWriter], tuple[asyncio.StreamReader, asyncio.StreamWriter]]
):
    """Creates a connection pair (i.e., two pair of `asyncio.StreamReader` and
    `asyncio.StreamWriter` instances) from `os.pipe` file descriptors.

    :example:
        To simulate actors *Alice* and *Bob* communicating via
        `StreamTransports` without running transport servers or making actual
        network connections::

            from .messages import SendRequest, SendResponse

            # Create transports for Alice and Bob
            alice = StreamsTransport(Address.from_netloc('127.0.0.1:8888))
            bob = StreamTransport(Address.from_netloc('127.0.0.1:9999'))

            # Create connections
            alice_conn, bob_conn = create_pipe_connections()

            # Manually add connections to one another
            alice.add_connection(bob.addr, *alice_conn)
            bob.add_connection(alice.addr, *bob_conn)

            # Alice sends a request to Bob
            fut = alice.write_request(SendRequest(...), bob.addr)

            req, addr = await bob.read_request()

            # Verify request is from Alice
            assert addr == alice.addr

            # Bob processes the request and sends a response
            bob.write_response(SendResponse(req.seqno), addr)

            # Alice waits for the response
            resp, addr = wait fut

            # Verify the response is from Bob
            assert addr == bob.addr

    """
    r0, w0 = os.pipe()
    r1, w1 = os.pipe()
    connA = await create_streams(os.fdopen(r0, "rb"), os.fdopen(w1, "wb"))
    connB = await create_streams(os.fdopen(r1, "rb"), os.fdopen(w0, "wb"))
    return connA, connB


if __name__ == "__main__":
    import logging
    from random import choice
    from uuid import uuid4
    from .task import cancel_all_tasks

    logging.basicConfig(
        format="%(asctime)-15s %(levelname)-8s %(taskName)-15s %(message)s",
        level=logging.DEBUG,
    )

    def asyncio_task_log_filter(record):
        try:
            task = asyncio.current_task()
        except RuntimeError:
            record.task = None
            record.taskName = None
        else:
            record.task = task
            record.taskName = task.get_name()
        return True

    for h in logging.getLogger().handlers:
        h.addFilter(asyncio_task_log_filter)

    localhost = ip_address("127.0.0.1")

    async def sender(from_addr, to_addr):
        transport = StreamTransport(from_addr)
        transport.start(name=f"Sender-transport-{transport.addr}")
        await transport.wait_serving()
        while True:
            req = SendMemoryRequest(
                None,
                timeout=0.5,
                channel_sd=b"channel desc",
                return_mode=choice(list(SendReturnMode)),
                sendhid=uuid4().bytes,
                payload=b"payload",
                mem_sd=b"memory desc",
            )
            fut = transport.write_request(req, to_addr)
            LOGGER.info(f"Waiting for response")
            resp, resp_addr = await fut
            LOGGER.info(f"Got response")
            if req.return_mode in (SendReturnMode.IMMEDIATELY, SendReturnMode.WHEN_BUFFERED):
                # Check that the response came from ourselves since the return
                # mode is WHEN_BUFFERED.
                assert resp_addr == from_addr
            else:
                assert resp_addr == to_addr
            assert req.seqno == resp.seqno
            await asyncio.sleep(0.1)
            LOGGER.info(f"Send tasks: {len(transport._send_tasks)}")

    async def receiver(addr):
        transport = StreamTransport(addr)
        transport.start(name=f"Receiver-transport-{transport.addr}")
        await transport.wait_serving()
        while True:
            req, from_addr = await transport.read_request()
            if req.return_mode in (SendReturnMode.WHEN_DEPOSITED, SendReturnMode.WHEN_RECEIVED):
                transport.write_response(SendResponse(req.seqno), from_addr)
            LOGGER.info(f"Receiver send tasks: {len(transport._send_tasks)}")

    async def main():
        to_addr = Address(localhost, 8888)
        for port in range(to_addr.port + 1, to_addr.port + 1 + 1):
            asyncio.create_task(sender(Address(localhost, port), to_addr), name=f"Sender-{port}")
        try:
            await receiver(to_addr)
        except KeyboardInterrupt:
            pass
        finally:
            LOGGER.critical("Shutting down...")
            await cancel_all_tasks()

    asyncio.run(main())
