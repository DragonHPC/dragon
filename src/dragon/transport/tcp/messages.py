import asyncio
from dataclasses import dataclass, field
from enum import Enum
from functools import cache
from inspect import isclass
from ipaddress import IPv4Address, IPv6Address
import logging
import math
import time
from typing import Annotated, get_args, get_origin, get_type_hints, Optional
from uuid import UUID

from .io import *
from .util import seconds_remaining


LOGGER = logging.getLogger("dragon.transport.tcp.messages")


uint8 = Annotated[int, StructIO("!B")]
""":class:`Int <int>` object packed into an unsigned 8-bit integer value."""

uint16 = Annotated[int, StructIO("!H")]
""":class:`Int <int>` object packed into an unsigned 16-bit integer value."""

uint64 = Annotated[int, StructIO("!Q")]
""":class:`Int <int>` object packed into an unsigned 64-bit integer value."""

float32 = Annotated[float, StructIO("!f")]
""":class:`Float <float>` object packed into a signed 32-bit floating point value."""

uuid_bytes = Annotated[UUID, UUIDBytesIO()]
""":class:`UUID` object packed into a 16 byte value."""

varbytes_short = Annotated[bytes, VariableBytesIO("!H")]
""":class:`Bytes <bytes>` object packed into at most (2**16 - 1) variable length bytes."""

varbytes = Annotated[bytes, VariableBytesIO("!Q")]
""":class:`Bytes <bytes>` object packed into at most (2**64 - 1) variable length bytes."""

vartext = Annotated[str, VariableTextIO("!Q")]
""":class:`String <str>` object encoded in utf-8 and packed into at most (2**64 - 1) variable length bytes."""

ipv4addr = Annotated[IPv4Address, IPv4AddressIO()]
""":class:`IPv4Address <ipaddress.IPv4Address>` object packed into 4 bytes."""

ipv6addr = Annotated[IPv6Address, IPv6AddressIO()]
""":class:`IPv6Address <ipaddress.IPv6Address>` object packed into 16 bytes."""


# SendReturnMode = Enum('SendReturnMode', 'IMMEDIATELY WHEN_BUFFERED WHEN_DEPOSITED WHEN_RECEIVED')
class SendReturnMode(Enum):
    """Return behavior of `SendRequest` messages."""

    IMMEDIATELY = 1
    """Return as soon as possible.  The source buffer can not be reused safely
    until some other user-coded sychronization ensures the message was received
    by another process. The source buffer can never be reused by the sender when
    transfer-of-ownership is specified on the send.
    """

    WHEN_BUFFERED = 2
    """Return as soon as the source buffer can be reused by the sender, except
    when transfer-of-ownership is specified on the send.
    """

    WHEN_DEPOSITED = 3
    """Return as soon as the message is deposited into the destination channel.
    The source buffer can be reused by the sender except when
    transfer-of-ownership is specified on the send.
    """

    WHEN_RECEIVED = 4
    """Return as soon as the message is received by some process out of the
    target channel.  The source buffer can be reused by the sender except when
    transfer-of-ownership is specified on the send.
    """


send_return_mode = Annotated[SendReturnMode, EnumIO(SendReturnMode)]


@dataclass
class TransportMessage:
    """Base class for messages to be sent or received using a `Transport`.

    .. rubric:: Type ID

    Subclasses must define a unique ``typeid`` parameter in order to be
    transmittable. For example, `Hello` is defined with a ``typeid`` of
    ``\\x40``. The ``typeid`` is the first byte of all transmitted messages and
    informs the receiver about what type of message to read.

    .. rubric:: Serialization

    The `read_message` and `write_message` functions (de)serialize a
    `TransportMessage` according to its ``__annotations__``, which is guaranteed
    to be an ordered mapping, in class declaration order.  The
    `dataclasses.dataclass` decorator makes it convenient to add common methods
    to a class in coordination with its ``__annotations__`` as dataclass fields.
    Transmittable fields must be annotated with an instance of `FixedBytesIO`,
    which provides the ``read`` and ``write`` methods for serializing the
    annotated type from an `asyncio.StreamReader` or to an
    `asyncio.StreamWriter`, respectively.

    :example: To define a transmittable integer field, serialized as an unsigned
    64-bit integer::

            from dataclasses import dataclass
            from typing import Annotated

            from dragon.transport.tcp.io import StructIO

            @dataclass
            class CustomMessage(TransportMessage):
                custom_field: Annotated[int, StructIO('!Q')]

    """

    _types = {}
    """Maps type ID to subclass.

    This dictionary is automatically populated when subclasses are defined with
    the ``typeid`` parameter. Subclasses of `TransportMessage` that do **not**
    define ``typeid``, and hence are not in this mapping, are not transmittable.
    """

    def __init_subclass__(cls, /, *, typeid: Optional[bytes] = None, **kwargs):
        super().__init_subclass__(**kwargs)
        if typeid is not None:
            assert isinstance(typeid, bytes)
            assert len(typeid) == 1
            assert typeid not in cls._types
            cls._typeid = typeid
            cls._types[typeid] = cls

    @property
    def _io_event(self):
        try:
            return self.__io_event
        except AttributeError:
            self.__io_event = asyncio.Event()
        return self.__io_event


@dataclass
class Hello(TransportMessage, typeid=b"\x40"):
    """Handshake messasge to communicate corresponding IPv4 server address."""

    host: ipv4addr
    """Host `ipaddress.IPv4Address`"""

    port: uint16
    """Port number"""


@dataclass
class Hello6(Hello, typeid=b"\x60"):
    """Handshake message to communicate corresponding IPv6 server address."""

    host: ipv6addr
    """Host `ipaddress.IPv6Address`"""


@dataclass
class SequencedTransportMessage(TransportMessage):
    """Base class for messages with sequnce numbers."""

    seqno: uint64
    """Sequence number"""


@dataclass
class Request(SequencedTransportMessage):
    """Base class for requests."""

    # Concerning absolute deadlines versus relative timeout.
    #
    # A GatewayMessage includes a deadline, which is based on the timeout
    # argument passed to ChannelSendH.send() and ChannelRecvH.recv() on the
    # client side. Currently, this is the only information the transport has
    # available concerning how to set the timeout argument to
    # ChannelSendH.send() or ChannelRecvH.recv() on the server side.
    #
    # The transport has two basic options, neither ideal:
    #
    #   1. A Request includes the deadline as-is and the server side computes
    #      the timeout argument as "seconods remaining" before the deadline
    #      expires. This assumes clocks are synced and any skew will impact the
    #      computed timeout.
    #
    #   2. A Request includes the timeout, which is computed on the client side
    #      prior to send. The fidelity of the deadline computed at the server is
    #      impacted by network and queuing (on both the client and server)
    #      latency.
    #
    # So, pick your poison. The current implementation is (2). The baseline
    # Request.timestamp is not sent to the server. Instead servers will evaluate
    # the deadline against their own monotonic clock.

    timestamp: float = field(default_factory=time.monotonic, init=False, repr=False, compare=False)
    """Base timestamp from which to evaluate `timeout`."""

    timeout: float32
    """Request timeout relative to Request.timestamp.

    Use math.inf to indicate the request never times out. This is consistent
    with the sentinel value used in a GatewayMessage which has no deadline.

    .. warning::
        Timeouts are approximate since messages incur network and queuing
        latency during tranmission; however, they should be relatively
        unaffected by clock skew between peers.

    .. note::
        A request provides the same semantics as ChannelSendH.send() and
        ChannelRecvH.recv(). Request timeout impacts `blocking` and `timeout`
        parameters to those functions as follows:

        - abs(Request.timeout) == math.inf  => blocking=True, timeout=None
        - Request.timeout == 0 => blocking=True, timeout=0 (same as blocking=False)
        - Request.timeout > 0  => blocking=True, timeout=Request.timeout

        Recall that blocking=True and timeout<0 will raise an exception and
        therefore isn't a combination the transport needs to worry about when
        interpretting the deadline of a GatewayMessage.

    """

    channel_sd: varbytes_short
    """Serialized target channel descriptor suitable for passing to
    `dragon.channels.Channel.attach`.
    """

    @property
    def deadline(self):
        """Request deadline based on Request.timeout relative to Request.timestamp."""
        return self.timestamp + self.timeout

    @deadline.setter
    def deadline(self, value):
        """Set the Request's timeout and timestamp based on the specified
        deadline and the current monotonic clock.

        .. note::
            `req.deadline = req.deadline` effectively normalizes the existing
            timeout against the current clock.
        """
        self.timeout, self.timestamp = seconds_remaining(value, _inf=math.inf)


@dataclass
class SendRequest(Request, typeid=b"\x01"):
    """Request corresponding to a gateway message with kind *send*."""

    return_mode: send_return_mode
    """Send return mode."""

    sendhid: uuid_bytes
    """The ``sendhid`` attribute on a gateway message; a 16 byte UUID.  Send
    concurrency is limited based on target channel and the send handle ID.
    """

    clientid: uint64
    """The ``clientid`` attribute is a user supplied
    attribute of a message that does not affect the payload.
    """

    hints: uint64
    """The ``hints`` attribute is a user supplied  attribute
    of a message that does not affect the payload.
    """

    payload: varbytes
    """Message payload."""


@dataclass
class SendMemoryRequest(SendRequest, typeid=b"\x02"):
    """Request corresponding to a gateway message with kind
    ``DRAGON_GATEWAY_MESSAGE_SEND`` where a serialized memory descriptor on the
    remote peer for the payload is available.
    """

    mem_sd: varbytes_short
    """Serialized memory descriptor on the remote peer."""


class RecvRequest(Request, typeid=b"\x03"):
    """Request corresponding to a gateway message with kind
    ``DRAGON_GATEWAY_MESSAGE_GET``.
    """

    pass


@dataclass
class EventRequest(Request, typeid=b"\x04"):
    """Request corresponding to a gateway message with kind
    ```DRAGON_GATEWAY_MESSAGE_EVENT``.
    """

    mask: uint16
    """Event mask argument to `dragon.channels.Channel.poll`."""


class Response(SequencedTransportMessage):
    """Base class for responses."""

    pass


@dataclass
class ErrorResponse(Response, typeid=b"\xff"):
    """Response when the server experiences an error processing a `Request`."""

    errno: uint16
    """Dragon error number."""

    text: vartext
    """Error string."""


class SendResponse(Response, typeid=b"\xfe"):
    """Response to a `SendRequest`.

    .. seealso::
        Depending on the `SendReturnMode`, a corresponding `SendResponse` may
        be sent by the client prior to the `SendRequest` actually being sent
        or processed at the target server.
    """

    pass


@dataclass
class RecvResponse(Response, typeid=b"\xfc"):
    """Response to a `RecvRequest`."""

    clientid: uint64
    """The ``clientid`` attribute is a user supplied
    attribute of a message that does not affect the payload.
    """

    hints: uint64
    """The ``hints`` attribute is a user supplied  attribute
    of a message that does not affect the payload.
    """

    payload: varbytes
    """Message payload."""


@dataclass
class EventResponse(Response, typeid=b"\xfb"):
    """Response to an `EventRequest`."""

    errno: uint16
    """Dragon error number."""

    result: uint64
    """Return value from `dragon.channels.Channel.poll`."""


@cache
def _get_io_annotations(cls):
    """Returns dictionary of object (e.g., class) ``__annotations__`` which
    include a `FixedBytesIO` instance.

    :param cls: Object to be inspected for *IO* ``__annotations__``
    """
    D = {}
    for name, hints in get_type_hints(cls, include_extras=True).items():
        origin = get_origin(hints)
        if not isclass(origin):
            continue
        if not issubclass(origin, Annotated):
            continue
        _, *annotations = get_args(hints)
        for obj in annotations:
            if isinstance(obj, FixedBytesIO):
                D[name] = obj
    return D


async def read_message(reader: asyncio.StreamReader) -> TransportMessage:
    """Read a message from the specified stream.

    See `TransportMessage` for more information about how messages are
    serialized based on annotations.

    :param reader: Stream to read
    :return: Message read
    """
    typeid = await reader.readexactly(1)
    try:
        cls = TransportMessage._types[typeid]
    except KeyError:
        raise NotImplementedError(f"Unknown message type identifier: {typeid}")
    args = []
    for name, io in _get_io_annotations(cls).items():
        try:
            item = await io.read(reader)
        except:
            LOGGER.exception(f"Error reading message attribute: {cls.__name__}.{name}")
            raise
        else:
            args.append(item)
    msg = cls(*args)
    # Set the I/O event since the message was read from a stream
    msg._io_event.set()
    return msg


async def write_message(writer: asyncio.StreamWriter, msg: TransportMessage) -> None:
    """Write message to specified stream.

    See `TransportMessage` for more information about how messages are
    deserialized based on annotations.

    :param writer: Stream to write
    :param msg: Message to write
    """
    cls = type(msg)
    try:
        typeid = cls._typeid
    except AttributeError:
        raise NotImplementedError(f"TransportMessage type not writable: {cls}")
    writer.write(typeid)
    for name, io in _get_io_annotations(cls).items():
        try:
            io.write(writer, getattr(msg, name))
        except:
            LOGGER.exception(f"Error writing message attribute: {cls.__name__}.{name}")
            raise
    await writer.drain()
    # Set the I/O event since the message has been written to a stream
    msg._io_event.set()
