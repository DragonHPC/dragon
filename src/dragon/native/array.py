"""The Dragon native array is an ctypes object allocated from shared memory.
The return value is a synchronized wrapper for the object, and the object itself
can be accessed via the array attribute of a Array.

.. code-block:: python

   array = dragon.native.array.Array(typecode_or_type = ctypes.c_char, size_or_initializer = [b"0", b"1"], lock=True)
"""

import logging
import ctypes
from collections.abc import Iterable

import dragon
from ..channels import Channel, Message, ChannelError
from ..managed_memory import MemoryPool
from ..utils import B64
from ..infrastructure.facts import default_pool_muid_from_index
from ..infrastructure.parameters import this_process
from ..infrastructure.channel_desc import ChannelOptions
from ..localservices.options import ChannelOptions as LSChannelOptions
from ..globalservices.channel import create, get_refcnt, release_refcnt
from ..dtypes import WHEN_DEPOSITED
from .lock import Lock
from .value import (
    PseudoStructure,
    Value,
    ValueConversion,
    _get_nbytes,
    _map_structure_to_list,
    _extract_structure_types,
)
from .value import _TYPECODE_TO_TYPE, _SUPPORTED_TYPES, _ALIGNMENT, _TYPE_CONV_VAL


LOGGER = logging.getLogger(__name__)
_DEF_MUID = default_pool_muid_from_index(this_process.index)


_TYPE_CONV_ARR = {}  # dispatch router for manipulating the arrays based on datatype


class Array:
    """This class implements Dragon array resides in the Dragon channel."""

    _lock = None  # This ensures _get_lock is sane during any point of evaluation
    _type = None  # Needed to make __getattr__ behave correctly

    def __init__(
        self,
        typecode_or_type: str or type,
        size_or_initializer: int or range or list,
        lock: Lock or bool = True,
        m_uid: int = _DEF_MUID,
    ):
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
            if typecode_or_type in _TYPECODE_TO_TYPE.keys():
                typecode_or_type = _TYPECODE_TO_TYPE[typecode_or_type]
            else:
                raise AttributeError(f"typecode not found, has to be one of {list(_TYPECODE_TO_TYPE.keys())}")
        elif self._is_structure(typecode_or_type):
            pass
        else:
            if typecode_or_type not in _SUPPORTED_TYPES:
                raise AttributeError(f"type not found, has to be one of {_SUPPORTED_TYPES}")

        # don't lose our ctype
        self._type = typecode_or_type

        # for most types, we pack the size in the first 8 bytes, and then
        # use that for every individual value in the array. For wchar that changes
        # and we use a 4 byte alignment
        if self._type is ctypes.c_wchar:
            self._alignment = 4
        else:
            self._alignment = _ALIGNMENT

        LOGGER.debug("Init Dragon Native Array with %s, %s, %s, %s", typecode_or_type, size_or_initializer, lock, m_uid)

        # Figure out how much size we need in our message
        if isinstance(size_or_initializer, int):
            initial_length = size_or_initializer  # Used for initializing a value for string length
            size_or_initializer = [0] * size_or_initializer  # convert int to list of requested length
            no_input_val = True
        else:
            no_input_val = False
            initial_length = len(size_or_initializer)

        self._data_len = len(size_or_initializer)

        # If we have a c_char, we need to track the length of the joined string since the API allows
        # users to query value and expect a concat-ed string of correct length as return type
        if typecode_or_type is ctypes.c_char:
            self._str_len = Value("i", initial_length)
        else:
            self._str_len = 0

        # Work out a stride per data element.
        stride = 1
        if self._is_structure(typecode_or_type):
            self._var_names, self.subclass_types, self.tuple_type_list = _extract_structure_types(
                self._type._fields_, len(size_or_initializer)
            )
            self._struct0_sizes, self._summed_s0_sizes = self._get_packed_struct_size(
                self.subclass_types, pack_length=True
            )
            self._struct1_sizes, self._summed_s1_sizes = self._get_packed_struct_size(
                self.subclass_types, pack_length=False
            )
        else:
            self.subclass_types, self.tuple_type_list = (None, None)
            self._struct0_sizes, self._summed_s0_sizes = (None, None)
            self._struct1_sizes, self._summed_s1_sizes = (None, None)
            stride = self._alignment

        # Get a msg and a memview for the value inside our pool
        msg = self._get_msg_from_pool(size_or_initializer, stride, m_uid)
        mview = msg.bytes_memview()

        # Pack the array into a Dragon msg via a memoryview object
        pack_length = True

        size_or_initializer = self._process_input(size_or_initializer)

        # Structures have to be handled carefully as they're essentially doubly-nested arrays
        if self._is_structure(typecode_or_type):
            # Go through each element of the array of structures
            for s_idx, value in enumerate(size_or_initializer):

                # Convert the component of one structure instance into a list for each manipulation
                val = _map_structure_to_list(value)

                # Since a structure obviously has multiple values to go through the individual elements in one
                # structure element
                for e_idx, v in enumerate(val):

                    l_mview = self._get_struct_mview_slice(mview, s_idx, e_idx, pack_length)
                    self.item_type = self.tuple_type_list[e_idx]
                    if no_input_val:
                        _TYPE_CONV_VAL[self.item_type][0](
                            mview=l_mview, convert=ValueConversion.OBJ_TO_BYTES, pack_length=pack_length
                        )
                    else:
                        _TYPE_CONV_VAL[self.item_type][0](
                            mview=l_mview, convert=ValueConversion.OBJ_TO_BYTES, val=v, pack_length=pack_length
                        )

                if pack_length:
                    pack_length = False

        # vanilla types are easier
        else:
            for idx, value in enumerate(size_or_initializer):
                # Regular ctypes

                # puts array in shared memory
                l_mview = self._get_mview_slice(mview, idx, pack_length)

                if no_input_val:
                    _TYPE_CONV_VAL[self._type][0](
                        mview=l_mview,
                        convert=ValueConversion.OBJ_TO_BYTES,
                        pack_length=pack_length,
                        alignment=self._alignment,
                    )
                else:
                    _TYPE_CONV_VAL[self._type][0](
                        mview=l_mview,
                        convert=ValueConversion.OBJ_TO_BYTES,
                        val=value,
                        pack_length=pack_length,
                        alignment=self._alignment,
                    )

                if pack_length:
                    pack_length = False

        # use send handle from reset
        self._sendh.send(msg)
        msg.destroy()

        # The strange logic in here flows from requirements outlined in cpython multiprocessing unittests for Value.
        # We respect the absurdity.
        _lock_instance = isinstance(lock, dragon.mpbridge.synchronize.DragonLock) or isinstance(
            lock, dragon.native.lock.Lock
        )
        if lock in [True, None] or _lock_instance:
            if not _lock_instance:
                lock = Lock(recursive=True)
            self.get_lock = self._get_lock
            self.get_obj = self._type
        elif lock is False:
            pass
        else:
            raise AttributeError(f"Invalid type specificied for lock: {lock}: {type(lock)}")

        self._lock = lock

    def __getstate__(self):
        ret = (
            self._channel.serialize(),
            self._type,
            self._data_len,
            self._str_len,
            self._alignment,
            self._lock,
            self._muid,
            self.subclass_types,
            self.tuple_type_list,
            self._struct0_sizes,
            self._summed_s0_sizes,
            self._struct1_sizes,
            self._summed_s1_sizes,
        )
        return ret

    def __setstate__(self, state):
        (
            serialized_bytes,
            self._type,
            self._data_len,
            self._str_len,
            self._alignment,
            self._lock,
            self._muid,
            self.subclass_types,
            self.tuple_type_list,
            self._struct0_sizes,
            self._summed_s0_sizes,
            self._struct1_sizes,
            self._summed_s1_sizes,
        ) = state

        self._channel = dragon.channels.Channel.attach(serialized_bytes)
        self._reset()
        get_refcnt(self._channel.cuid)

    def __repr__(self):
        return f"{self.__class__.__name__}(typecode_or_type={self._type}, lock={self._lock}, m_uid={self._muid})"

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

    def __enter__(self):
        self.acquire()

    def __exit__(self, *args):
        self.release()

    def __len__(self):
        return self._data_len

    def __getattr__(self, name):
        """This allows structure attributes to be accessed as if part of the class"""

        try:
            x = super().__getattr__(name)
            return x
        except AttributeError:
            # Docs require c_char array have a 'value' and 'raw' attribute that returns
            # array as a string
            if self._type is ctypes.c_char and name in ["value", "raw"]:
                # join the list
                x = b"".join(self.__getitem__(slice(0, self._str_len.value)))
                return x

        raise AttributeError(f"{name} is not an attribute of DragonArray")

    def __setattr__(self, name, val):
        """This allows structure attributes to be modded as if they're part of the base class"""

        raise_ex = None
        non_val = None
        try:
            super().__setattr__(name, val)
        except AttributeError as e:
            raise_ex = e

        if self._type is ctypes.c_char and name == "value":
            self.__setitem__(slice(0, len(val)), val)

            # If the user has wholesale changed the string, we need to update the string length
            self._str_len.value = len(val)

        else:
            non_val = True

        if raise_ex and non_val:
            raise raise_ex

    def __setitem__(self, pos, arr):

        # grab mview from memory pool
        msg = self._recvh.recv()
        mview = msg.bytes_memview()

        # Construct a list of indices that need to be updated
        if isinstance(pos, slice):
            try:
                arr_indices = range(pos.stop)[pos]
            # Handle the scenario where stop wasn't defined, eg: arr[:]
            except TypeError:
                arr_indices = range(self._data_len)
        else:
            arr_indices = [pos]

        # Now stick it all into memory
        if arr_indices[0] == 0:
            pack_length = True
        else:
            pack_length = False

        arr = self._process_input(arr)

        for idx, value in enumerate(arr):

            # Handle the special structure case. Blergh.
            if self._is_structure(value):

                # Convert the component of one structure instance into a list for each manipulation
                val = _map_structure_to_list(value)

                # Since a structure obviously has multiple values to go through the individual elements in one
                # structure element
                for e_idx, v in enumerate(val):

                    l_mview = self._get_struct_mview_slice(mview, arr_indices[idx], e_idx, pack_length)
                    self.item_type = self.tuple_type_list[e_idx]

                    _TYPE_CONV_VAL[self.item_type][0](
                        mview=l_mview, convert=ValueConversion.OBJ_TO_BYTES, val=v, pack_length=pack_length
                    )

            # The simple cases
            else:
                # The first 8 bytes are for data length
                l_mview = self._get_mview_slice(mview, arr_indices[idx], pack_length)

                ## note: if type is wchar, we always pack the length into the array for each element, so pack_length
                ## is ignored if type == ctypes.c_wchar. This helps prevent returning empty bytes if the input element
                ## doesn't make use of all 4 bytes of wchar
                _TYPE_CONV_VAL[self._type][0](
                    mview=l_mview,
                    convert=ValueConversion.OBJ_TO_BYTES,
                    val=value,
                    pack_length=pack_length,
                    alignment=self._alignment,
                )

            if pack_length:
                pack_length = False

        # puts array back into memory
        self._sendh.send(msg)
        msg.destroy()

    def __getitem__(self, pos):

        # grab bytes from memory pool
        if isinstance(pos, int) and pos > (self._data_len) - 1:
            raise IndexError("list index out of range")
        msg = self._recvh.recv()

        # divide bytes into self._alignment and convert the message to the original array
        if self._is_structure(self._type):
            target = PseudoStructure(self, offset=pos)

        # non-Structure array is returned from memory
        else:
            mview = msg.bytes_memview()
            arr = self._unpack_array(mview)

            # Now grab the value, send the message back out there and return
            val = arr[pos]

            self._sendh.send(msg)
            msg.destroy()
            return val

        # create the tuple bytes from the array
        self._sendh.send(msg)
        msg.destroy()
        if type(target) is list:
            target = target[0]

        return target

    def _process_input(self, arr):
        """Make sure the input array is configured in such a way to make it fit into our scheme"""

        if isinstance(arr, range):
            arr = list(arr)

        # Make sure we have the the right flavor of iterable
        if not isinstance(arr, Iterable) and not isinstance(arr, (str, bytes)):
            arr = [arr]
        # If we were given a bytes string with c_char, make it a string so we dont' lose info
        elif isinstance(arr, bytes) and len(arr) > 0 and self._type == ctypes.c_char:
            arr = arr.decode("utf-8")
        return arr

    # Public  methods

    def acquire(self):
        """Acquire the internal lock object"""
        return self._lock.acquire()

    def release(self):
        """Release the internal lock object"""
        return self._lock.release()

    # This seems crazy, but it necessary to always force evaluation of value from
    # the memory pool, which only exists for strings implemented by ctypes.c_char
    @property
    def value(self):

        return self.__getattr__("value")

    @property
    def raw(self):

        return self.__getattr__("raw")

    # Private methods
    def _unpack_array(self, mview: memoryview):

        if self._type is not ctypes.c_wchar:
            # Get the data length out of the first 8 bytes:
            nbytes = _get_nbytes(mview[: self._alignment])

            # Construct the array
            arr = [
                _TYPE_CONV_VAL[self._type][0](
                    mview=mview[i : i + self._alignment],
                    convert=ValueConversion.BYTES_TO_OBJ,
                    nbytes=nbytes,
                    alignment=self._alignment,
                )
                for i in range(self._alignment, len(mview), self._alignment)
            ]

        # Handle wchar whose return length can vary from array element to array element
        else:
            arr = [
                _TYPE_CONV_VAL[self._type][0](
                    mview=self._get_mview_slice(mview, i, True),
                    convert=ValueConversion.BYTES_TO_OBJ,
                    alignment=self._alignment,
                )
                for i in range(0, int(len(mview) / (2 * self._alignment)))
            ]

        return arr

    def _reset(self) -> None:
        self._recvh = self._channel.recvh()
        self._sendh = self._channel.sendh(return_mode=WHEN_DEPOSITED)
        self._recvh.open()
        self._sendh.open()
        self._mpool = MemoryPool.attach(B64.str_to_bytes(this_process.default_pd))

    def _is_structure(self, t):
        """Quick check on whether the type is a structure"""

        return isinstance(t, type) and issubclass(t, ctypes.Structure)

    def _get_packed_struct_size(self, types: list, pack_length: bool) -> tuple[list, int]:
        """Compute how much space packing this structure will require"""

        sizes = []
        for t in types:
            if t is ctypes.c_wchar:
                sizes.append(2 * 4)  # 4 bytes for character array and 4 bytes for size
            else:
                sizes.append(_ALIGNMENT + int(pack_length) * _ALIGNMENT)

        return sizes, sum(sizes)

    def _get_struct_mview_indices(self, s_idx: int, e_idx: int, pack_length: bool):
        """compute slice of mview for a specific elements of an array of structs"""

        if s_idx > 0:
            base_offset = self._summed_s0_sizes + (s_idx - 1) * self._summed_s1_sizes
            element_offset = base_offset + sum(self._struct1_sizes[:e_idx])
            start = element_offset
            stop = start + self._struct1_sizes[e_idx]
        else:
            if e_idx > 0:
                start = sum(self._struct0_sizes[:e_idx])
            else:
                start = 0
            stop = start + self._struct0_sizes[e_idx]

        return start, stop

    def _get_struct_mview_slice(self, mview: memoryview, s_idx: int, e_idx: int, pack_length: bool):
        """Get a slice of memoryview for reading/writing an array element with a packing scheme specific to a structure"""

        # compute the offset to the current array element
        start, stop = self._get_struct_mview_indices(s_idx, e_idx, pack_length)

        return mview[start:stop]

    def _get_mview_slice(self, mview: memoryview, index: int, pack_length: bool):
        """Get a slice of memoryview for reading/writing based on type and whether to pack the length"""

        if self._type is not ctypes.c_wchar:
            start = index * self._alignment + int(not pack_length) * self._alignment
            stop = start + self._alignment + int(pack_length) * self._alignment
        elif self._type is ctypes.c_wchar:
            start = index * (2 * self._alignment)
            stop = start + (2 * self._alignment)

        return mview[start:stop]

    def _get_msg_from_pool(self, size_or_initializer: int or range or list, stride: int, m_uid: int = _DEF_MUID):

        self._muid = m_uid

        sh_channel_options = LSChannelOptions(capacity=1)
        gs_channel_options = ChannelOptions(ref_count=True, local_opts=sh_channel_options)
        descriptor = create(m_uid, options=gs_channel_options)
        self._channel = Channel.attach(descriptor.sdesc)
        self._reset()

        # Handling structures is also annoying
        if self._is_structure(self._type):
            creation_size = self._summed_s0_sizes + (len(size_or_initializer) - 1) * self._summed_s1_sizes
        # The general case
        elif self._type is not ctypes.c_wchar:
            creation_size = max(self._data_len * stride + self._alignment, 2 * self._alignment)

        # Finally we have the wchar_t case
        else:
            creation_size = max(2 * (self._data_len * stride) + self._alignment, 2 * self._alignment)

        # Now create the message
        msg = Message.create_alloc(self._mpool, creation_size)
        msg.clear_payload()
        return msg

    def _get_lock(self):
        return self._lock
