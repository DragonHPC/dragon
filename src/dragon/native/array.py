"""The Dragon native array is an ctypes object allocated from shared memory.
The return value is a synchronized wrapper for the object, and the object itself
can be accessed via the array attribute of a Array.

.. code-block:: python

   array = dragon.native.array.Array(typecode_or_type = ctypes.c_char, size_or_initializer = [b"0", b"1"], lock=True)
"""


import sys
import logging
import ctypes
import struct

import dragon
from ..channels import Channel, Message, ChannelError
from ..managed_memory import MemoryPool
import dragon.utils as du
from ..infrastructure.facts import default_pool_muid_from_index
from ..infrastructure.parameters import this_process
from ..infrastructure.channel_desc import ChannelOptions
from ..localservices.options import ChannelOptions as LSChannelOptions
from ..globalservices.channel import create, get_refcnt, release_refcnt
from ..dtypes import WHEN_DEPOSITED
from ..native.lock import Lock

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

class Array:
    """This class implements Dragon array resides in the Dragon channel."""

    def __getstate__(self):
        return (self._channel.serialize(), self._type, self._data_len)

    def __setstate__(self, state):
        (serialized_bytes, self._type, self._data_len) = state
        self._channel = dragon.channels.Channel.attach(serialized_bytes)
        self._reset()
        get_refcnt(self._channel.cuid)

    def __repr__(self):
        return f"{self.__class__.__name__}(typecode_or_type={self._type}, lock={self._lock}, m_uid={self._muid})"

    def __init__(self, typecode_or_type: str or type, size_or_initializer: int or range or list, lock: Lock or bool = True, m_uid: int = _DEF_MUID):
        """Initialize a array object.
        :param typecode_or_type: the typecode or type is returned from the dictionary, _TYPECODE_TO_TYPE
        :type typecode_or_type: str or ctypes, required
        :param size_or_initializer: the array for the object
        :type size_or_initializer: range, int, list, required
        :param lock: dragon.native.lock.Lock, optional
        :type lock: creates lock for synchronization for array
        :param m_uid: memory pool to create the channel in and message to write value and typecode_or_type in managed memory, defaults to _DEF_MUID
        :type m_uid: int, optional
        """

        if isinstance(typecode_or_type, str):
            if typecode_or_type in _TYPECODE_TO_TYPE:
                typecode_or_type = _TYPECODE_TO_TYPE[typecode_or_type]
            else:
                raise AttributeError

        else:
            if typecode_or_type not in _SUPPORTED_TYPES:
                raise AttributeError

        LOGGER.debug(
            f"Init Dragon Native Array with {typecode_or_type=}, {size_or_initializer=}, {lock=}, {m_uid=}"
        )

        # for repr method
        self._muid = m_uid

        sh_channel_options = LSChannelOptions(capacity=1)
        gs_channel_options = ChannelOptions(ref_count=True, local_opts=sh_channel_options)
        descriptor = create(m_uid, options=gs_channel_options)
        self._channel = Channel.attach(descriptor.sdesc)
        self._reset()

        # set array with type that is multiprocessing Array type
        self._type = typecode_or_type

        #remove the assignment to self._size_or_initializer
        if isinstance(size_or_initializer, int):
            size_or_initializer = [0] * size_or_initializer # convert to list

        self._data_len = len(size_or_initializer)

        msg = Message.create_alloc(self._mpool, max(self._data_len * 8, 8))
        mview = msg.bytes_memview()
        for idx, value in enumerate(size_or_initializer):
            start = idx*8
            stop = start + 8
            mview[start:stop] = self._array2bytes(value)

        #use send handle from reset
        self._sendh.send(msg)
        msg.destroy()

        # if lock is False, return the subclass value
        if lock in (True, None):
            lock = Lock(recursive=True)

        if isinstance(lock, bool):
            pass
        elif isinstance(lock, Lock): # don't need full qualifier here
            self._lock = lock
            self.get_lock = self._get_lock
            self.get_obj = self._type
        else:
            raise AttributeError(f"The Lock must be a bool or dragon.native.lock.Lock, but is  {type(lock)}")

    def __del__(self):
        try:
            self._sendh.close()
        except (AttributeError, ChannelError):
            pass
        try:
            self._recvh.close()
        except (AttributeError, ChannelError):
            pass
        try:
            self._channel.detach()
        except (AttributeError, ChannelError):
            pass
        try:
            release_refcnt(self._channel.cuid)
        except (AttributeError, ChannelError):
            pass

    def _reset(self) -> None:
        self._recvh = self._channel.recvh()
        self._sendh = self._channel.sendh(return_mode=WHEN_DEPOSITED)
        self._recvh.open()
        self._sendh.open()
        self._mpool = MemoryPool.attach(du.B64.str_to_bytes(this_process.default_pd))

    def __len__(self):
        return self._data_len

    def __setitem__(self, item, arr):

        #grab bytes from memory pool
        msg = self._recvh.recv()

        #overwrite the values of interest that are changed in the memory pool
        if isinstance(item, slice) or isinstance(arr, int):
            mview = msg.bytes_memview()
            value_info = mview[:].tobytes()
            msg_val = [self._bytes2array(value_info[i:i+8]) for i in range(0, len(value_info), 8)]
            msg_val[item] = arr
            arr = msg_val

        msg.destroy()
        #replace old value in memory with arr
        msg = Message.create_alloc(self._mpool, max(len(arr) * 8, 8))
        mview = msg.bytes_memview()
        for idx, value in enumerate(arr):
            start = idx * 8
            stop = start + 8
            mview[start:stop] = bytes(self._array2bytes(value))

        # puts array back into memory
        self._sendh.send(msg)
        msg.destroy()

    def __getitem__(self, item):

        #grab bytes from memory
        msg = self._recvh.recv()
        mview = msg.bytes_memview()
        value_info = mview[:].tobytes()
        #divide bytes into 8 and convert the message to the original array
        return_array = [self._bytes2array(value_info[i:i+8]) for i in range(0, len(value_info), 8)]
        self._sendh.send(msg)
        msg.destroy()
        return return_array[item]

    def _array2bytes(self, arr: list or bytes or bytearray) -> Message:
        valbytes = b""
        if self._type in [ctypes.c_char, ctypes.c_wchar]:
            # This is for ctypes.c_char as ctypes.c_wchar returns str
            if isinstance(arr, (bytes, bytearray)) and self._type is not ctypes.c_wchar:
                valbytes = bytes(arr)
            else:
                valbytes = bytes(str(arr).encode("utf-8"))
            # if there are more than one bytes in valbytes
            if len(valbytes) > 8:
                raise AttributeError
        # c_float and c_double return float type
        elif self._type in [ctypes.c_float, ctypes.c_double]:
            valbytes = bytes(struct.pack("d", arr))
        # int, longlong, and other ctypes return int type
        else:
            valbytes = int(arr).to_bytes(8, byteorder=sys.byteorder, signed=True)
        return valbytes + bytes(8 - len(valbytes))

    def _bytes2array(self, msg: Message) -> list or bytes or bytearray:
        if self._type in [ctypes.c_char, ctypes.c_wchar]:
            value_info = msg.decode("utf-8")
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
            (float_val,) = struct.unpack("d", msg)
            return float(float_val)
        else:
            # returns int for int types
            return int.from_bytes(msg, byteorder=sys.byteorder, signed=True)

    def acquire(self):
        """ Acquire the internal lock object """
        return self._lock.acquire()

    def release(self):
        """ Release the internal lock object """
        return self._lock.release()

    def _get_lock(self):
        return self._lock

    def __enter__(self):
        self.acquire()

    def __exit__(self, *args):
        self.release()
