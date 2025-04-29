"""Read and write objects from `asyncio-streams`."""

import asyncio
from enum import Enum
from ipaddress import ip_address, IPv4Address, IPv6Address
from struct import Struct
from typing import Any, Callable, Optional, Union
from uuid import UUID, uuid4


class FixedBytesIO:
    """Base class that reads and writes fixed size bytes from/to streams."""

    size: int
    """The number of bytes to read or write."""

    async def read(self, reader: asyncio.StreamReader) -> bytes:
        """Read bytes from stream.

        :param reader: Stream to read
        :return: Exactly `.size` bytes
        :raises: See `asyncio.StreamReader.readexactly` for possible exceptions
        """
        data = await reader.readexactly(self.size)
        return data

    def write(self, writer: asyncio.StreamWriter, data: bytes) -> None:
        """Write bytes to stream.

        :param writer: Stream to write
        :param data: Bytes to write
        :raises AssertionError: If `data` is not `.size` bytes
        """
        assert len(data) == self.size
        writer.write(data)


class CodableIO(FixedBytesIO):
    """Encode and decode fixed-bytes I/O."""

    encode: Callable[..., Any]
    """Encode bytes before writing."""

    decode: Callable[[Any], Any]
    """Decode bytes after reading."""

    async def read(self, reader: asyncio.StreamReader) -> Any:
        """Read decoded object(s) from stream.

        :param reader: Stream to read
        :return: Decoded object
        """
        data = await super().read(reader)
        return self.decode(data)

    def write(self, writer: asyncio.StreamWriter, *args: Any) -> None:
        """Write encoded object(s) to stream.

        :param writer: Stream to write
        :param args: Objects to encode
        """
        data = self.encode(*args)
        super().write(writer, data)


class StructIO(Struct, FixedBytesIO):
    """Unpack and pack `Struct` objects from/to streams.

    :param format: `Struct` format string
    """

    async def read(self, reader: asyncio.StreamReader) -> Any:
        """Read bytes from stream and unpack as `Struct`.

        :param reader: Stream to read
        :return: Unpacked object
        """
        packed_data = await super().read(reader)
        data = self.unpack(packed_data)
        return data if len(data) > 1 else data[0]

    def write(self, writer: asyncio.StreamWriter, *args: Any) -> None:
        """Pack objects as `Struct` and write bytes to stream.

        :param writer: Stream to write
        :param args: Objects to pack
        """
        packed_data = self.pack(*args)
        super().write(writer, packed_data)


class VariableBytesIO(StructIO):
    """Read and write a variable number of bytes from/to streams.

    Uses a simple length-based encoding scheme where the total number of bytes
    to read/write is first read/written from/to a stream. Instances must
    provide the `Struct` format string for encoding/decoding the number of
    bytes.

    :param format: `Struct` format string; restricts the maximum number of bytes which may be read/written
    """

    async def read(self, reader: asyncio.StreamReader) -> bytes:
        """Read variable number of bytes from stream.

        :param reader: Stream to read
        :return: Bytes read
        :raises: See `asyncio.StreamReader.readexactly` for possible exceptions
        """
        num_bytes = await super().read(reader)
        data = await reader.readexactly(num_bytes)
        return data

    def write(self, writer: asyncio.StreamWriter, data: bytes) -> None:
        """Write variable number of bytes to stream.

        :param writer: Stream to write
        :param data: Bytes to write
        """
        super().write(writer, len(data))
        writer.write(data)


class VariableTextIO(CodableIO, VariableBytesIO):
    """Encode and decode variable length text from/to streams.

    :param format: `Struct` format string; restricts the maximum number of bytes which may be read/written before/after decoding/encoding.
    """

    codec = "utf-8"
    """Codec for encoding/decoding."""

    @classmethod
    def encode(cls, s: str) -> bytes:
        """Encode string to bytes."""
        return s.encode(cls.codec)

    @classmethod
    def decode(cls, s: bytes) -> str:
        """Decode bytes as string."""
        return s.decode(cls.codec)


class UUIDBytesIO(CodableIO):
    """Encode and decode UUIDs from/to streams."""

    # UUID is 16 bytes
    size = len(uuid4().bytes)

    @classmethod
    def encode(cls, u: UUID) -> bytes:
        """Encode UUID to bytes."""
        return u.bytes

    @classmethod
    def decode(cls, s: bytes) -> UUID:
        return UUID(bytes=s)


class IPAddressIO(CodableIO):
    """Base class to read and write IP addresses."""

    @staticmethod
    def encode(ip: Union[IPv4Address, IPv6Address]) -> bytes:
        """Encode an `ipaddress.IPv4Address` or `ipaddress.IPv6Address` as *packed* bytes."""
        return ip.packed

    decode = staticmethod(ip_address)
    """Decode bytes as an `ipaddress.IPv4Address` or `ipaddress.IPv6Address`."""


class IPv4AddressIO(IPAddressIO):
    """Read and write `ipaddress.IPv4Address` instances."""

    size = len(ip_address("127.0.0.1").packed)


class IPv6AddressIO(IPAddressIO):
    """Read and write `ipaddress.IPv6Address` instances."""

    size = len(ip_address("::1").packed)


class EnumIO(CodableIO, StructIO):
    """Read and write an `enum.Enum` packed into a minimum-sized integer value
    based on the maximum `enum.Enum` value.

    :param enum_cls: `enum.Enum` subclass to read and write
    :param format: If not specified, the smallest possible `Struct` format is automatically selected.
    """

    @staticmethod
    def encode(e: Enum) -> int:
        """Encode an `enum.Enum` value as an integer."""
        return int(e.value)

    def __init__(self, enum_cls: Enum, format: Optional[str] = None):
        if format is None:
            max_value = max(e.value for e in enum_cls)
            if max_value < 1 << 8:
                format = "!B"
            elif max_value < 1 << 16:
                format = "!H"
            elif max_value < 1 << 32:
                format = "!L"
            elif max_value < 1 << 64:
                format = "!Q"
            else:
                raise ValueError(f"Maximum value of {enum_cls} is out of range")
        super().__init__(format)
        self.decode = enum_cls
        """Decode `int` value as `enum.Enum` instance."""
