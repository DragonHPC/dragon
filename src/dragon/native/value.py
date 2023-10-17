"""The Dragon native value is an ctypes object allocated from shared memory.
The return value is a synchronized wrapper for the object, and the object itself
can be accessed via the value attribute of a Value.

.. code-block:: python

   value = dragon.native.value.Value(typecode_or_type = ctypes.c_char, value = 0)
"""


import sys
import logging
import ctypes
import struct

import dragon
from ..channels import Channel, Message, ChannelError
from ..managed_memory import MemoryPool
import dragon.utils as du
from ..dtypes import WHEN_DEPOSITED
from ..infrastructure.facts import default_pool_muid_from_index
from ..infrastructure.parameters import this_process
from ..infrastructure.channel_desc import ChannelOptions
from ..localservices.options import ChannelOptions as ShepherdChannelOptions
from ..globalservices.channel import create, get_refcnt, release_refcnt

LOGGER = logging.getLogger(__name__)
_DEF_MUID = default_pool_muid_from_index(this_process.index)

_TYPECODE_TO_TYPE = {
    "c": ctypes.c_char,
    "u": ctypes.c_wchar,
    "b": ctypes.c_byte,
    "B": ctypes.c_ubyte,
    "h": ctypes.c_short,
    "H": ctypes.c_ushort,
    "i": ctypes.c_int,
    "I": ctypes.c_uint,
    "l": ctypes.c_long,
    "L": ctypes.c_ulong,
    "q": ctypes.c_longlong,
    "Q": ctypes.c_ulonglong,
    "f": ctypes.c_float,
    "d": ctypes.c_double,
}

_SUPPORTED_TYPES = frozenset(_TYPECODE_TO_TYPE.values())


class Value:
    """This class implements Dragon value resides in the Dragon channel."""

    def __getstate__(self):
        return (self._channel.serialize(), self._type)

    def __setstate__(self, state):
        (serialized_bytes, self._type) = state
        self._channel = dragon.channels.Channel.attach(serialized_bytes)
        self._reset()
        get_refcnt(self._channel.cuid)

    def __repr__(self):
        return f"Dragon Value(type={self._type}, value={self.value}, cuid={self._channel.cuid})"

    def __init__(self, typecode_or_type, value: int = 0, m_uid: int = _DEF_MUID):
        """Initialize a value object.
        :param typecode_or_type: the typecode or type is returned from the dictionary, _TYPECODE_TO_TYPE
        :type typecode_or_type: str or ctypes, required
        :param value: the value for the object
        :type value: int, optional
        :param m_uid: memory pool to create the channel in and message to write value and typecode_or_type in managed memory, defaults to _DEF_MUID
        :type m_uid: int, optional
        """

        if isinstance(typecode_or_type, str):
            if typecode_or_type in _TYPECODE_TO_TYPE.keys():
                typecode_or_type = _TYPECODE_TO_TYPE[typecode_or_type]
            else:
                raise AttributeError(f"typecode not found, has to be one of {list(_TYPECODE_TO_TYPE.keys())}")

        else:
            if typecode_or_type not in _SUPPORTED_TYPES:
                raise AttributeError(f"type not found, has to be one of {_SUPPORTED_TYPES}")

        LOGGER.debug(
            f"Init Dragon Native Value with typecode_or_type={typecode_or_type}, value={value}, m_uid={m_uid}"
        )

        # for repr method
        self._muid = m_uid

        sh_channel_options = ShepherdChannelOptions(capacity=1)
        gs_channel_options = ChannelOptions(ref_count=True, local_opts=sh_channel_options)
        descriptor = create(m_uid, options=gs_channel_options)
        self._channel = Channel.attach(descriptor.sdesc)
        self._reset()

        # set value with type that is multiprocessing Value type
        self._type = typecode_or_type
        valbytes = self._value2valbytes(value)

        # create value in shared memory
        msg = Message.create_alloc(self._mpool, 8)
        mview = msg.bytes_memview()
        mview[: len(valbytes)] = valbytes

        self._sendh.send(msg)

        msg.destroy()

    def __del__(self):
        try:
            self._sendh.close()
            self._recvh.close()
            self._channel.detach()
            release_refcnt(self._channel.cuid)
        except (AttributeError, ChannelError):
            pass

    @property
    def value(self) -> object:
        """Get the current value.

        :return: The current value
        :rtype: object
        """

        # grab value from the channel
        msg = self._recvh.recv()
        mview = msg.bytes_memview()
        value_info = mview[:].tobytes()

        self._sendh.send(msg)
        msg.destroy()
        return self._valbytes2value(value_info)

    @value.setter
    def value(self, val: object) -> None:
        """Set the value

        :param val: the new value
        :type val: object
        :return: None
        :rtype: NoneType
        """

        # grabs value from the channel
        msg = self._recvh.recv()

        # assigns the changed value to bytes string
        valbytes = self._value2valbytes(val)

        # puts value back into memory
        mview = msg.bytes_memview()
        mview[: len(valbytes)] = valbytes

        self._sendh.send(msg)
        msg.destroy()

    # private methods

    def _reset(self) -> None:
        self._recvh = self._channel.recvh()
        self._sendh = self._channel.sendh(return_mode=WHEN_DEPOSITED)
        self._recvh.open()
        self._sendh.open()
        self._mpool = MemoryPool.attach(du.B64.str_to_bytes(this_process.default_pd))

    def _value2valbytes(self, value: object) -> bytes:

        valbytes = b""
        if self._type in [ctypes.c_char, ctypes.c_wchar]:
            # This is for ctypes.c_char as ctypes.c_wchar returns str
            if isinstance(value, (bytes, bytearray)) and self._type is not ctypes.c_wchar:
                valbytes = bytes(value)
            else:
                valbytes = bytes(str(value).encode("utf-8"))
            # if there are more than one bytes in valbytes
            if len(valbytes) > 8:
                raise AttributeError
        # c_float and c_double return float type
        elif self._type in [ctypes.c_float, ctypes.c_double]:
            valbytes = bytes(struct.pack("d", value))
        # int, longlong, and other ctypes return int type
        else:
            valbytes = int(value).to_bytes(8, byteorder=sys.byteorder, signed=True)
        return valbytes

    def _valbytes2value(self, valbytes: bytes) -> object:

        if self._type in [ctypes.c_char, ctypes.c_wchar]:
            value_info = valbytes.decode("utf-8")
            # removes extraneous characters
            value_val = value_info.replace("\x00", "")
            if self._type is ctypes.c_char:
                # c_char returns bytes
                return str(value_val).encode()
            else:
                # c_wchar returns string
                return str(value_val)
        elif self._type in (ctypes.c_float, ctypes.c_double):
            # returns float for the float types
            (float_val,) = struct.unpack("d", valbytes)
            return float(float_val)
        else:
            # returns int for int types
            return int.from_bytes(valbytes, byteorder=sys.byteorder, signed=True)
