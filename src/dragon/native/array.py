"""The Dragon native array is an ctypes object allocated from shared memory.
The return value is a synchronized wrapper for the object, and the object itself
can be accessed via the array attribute of a Array.

.. code-block:: python

   array = dragon.native.array.Array(typecode_or_type = ctypes.c_char, size_or_initializer = [b"0", b"1"], lock=True)
"""

import logging
import ctypes

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
from .value import PseudoStructure, ValueConversion, _get_nbytes, _map_structure_to_list, _extract_structure_types
from .value import _TYPECODE_TO_TYPE, _SUPPORTED_TYPES, _ALIGNMENT, _TYPE_CONV_VAL


LOGGER = logging.getLogger(__name__)
_DEF_MUID = default_pool_muid_from_index(this_process.index)


_TYPE_CONV_ARR = {}  # dispatch router for manipulating the arrays based on datatype


class Array:
    """This class implements Dragon array resides in the Dragon channel."""

    def __getstate__(self):
        ret = (self._channel.serialize(),
               self._type,
               self._data_len,
               self._alignment,
               self.subclass_types, self.tuple_type_list,
               self._struct0_sizes, self._summed_s0_sizes,
               self._struct1_sizes, self._summed_s1_sizes
               )
        return ret

    def __setstate__(self, state):
        (serialized_bytes, self._type, self._data_len, self._alignment,
         self.subclass_types, self.tuple_type_list,
         self._struct0_sizes, self._summed_s0_sizes,
         self._struct1_sizes, self._summed_s1_sizes) = state

        self._channel = dragon.channels.Channel.attach(serialized_bytes)
        self._reset()
        get_refcnt(self._channel.cuid)

    def __repr__(self):
        return (
            f"{self.__class__.__name__}(typecode_or_type={self._type}, lock={self._lock}, m_uid={self._muid})"
        )

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

        LOGGER.debug(
            "Init Dragon Native Array with %s, %s, %s, %s", typecode_or_type, size_or_initializer, lock, m_uid
        )

        # Figure out how much size we need in our message
        no_input_val = False
        if isinstance(size_or_initializer, int):
            size_or_initializer = [0] * size_or_initializer  # convert to list
            no_input_val = True

        # Work out a stride per data element.
        stride = 1
        if self._is_structure(typecode_or_type):
            self.subclass_types, self.tuple_type_list = _extract_structure_types(self._type._fields_, len(size_or_initializer))
            self._struct0_sizes, self._summed_s0_sizes = self._get_packed_struct_size(self.subclass_types, pack_length=True)
            self._struct1_sizes, self._summed_s1_sizes = self._get_packed_struct_size(self.subclass_types, pack_length=False)
        else:
            self.subclass_types, self.tuple_type_list = (None, None)
            self._struct0_sizes, self._summed_s0_sizes = (None, None)
            self._struct1_sizes, self._summed_s1_sizes = (None, None)

            stride = self._alignment

        self._data_len = len(size_or_initializer)

        # Get a msg and a memview for the value inside our pool
        msg = self._get_msg_from_pool(size_or_initializer, stride, m_uid)
        mview = msg.bytes_memview()

        # Pack the array into a Dragon msg via a memoryview object
        pack_length = True

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
                        _TYPE_CONV_VAL[self.item_type][0](mview=l_mview, convert=ValueConversion.OBJ_TO_BYTES, pack_length=pack_length)
                    else:
                        _TYPE_CONV_VAL[self.item_type][0](mview=l_mview, convert=ValueConversion.OBJ_TO_BYTES, val=v, pack_length=pack_length)

                if pack_length:
                    pack_length = False

        # vanilla types are easier
        else:
            for idx, value in enumerate(size_or_initializer):
                # Regular ctypes

                # puts array in shared memory
                l_mview = self._get_mview_slice(mview, idx, pack_length)

                if no_input_val:
                    _TYPE_CONV_VAL[self._type][0](mview=l_mview, convert=ValueConversion.OBJ_TO_BYTES,
                                                  pack_length=pack_length, alignment=self._alignment)
                else:
                    _TYPE_CONV_VAL[self._type][0](mview=l_mview, convert=ValueConversion.OBJ_TO_BYTES,
                                                  val=value, pack_length=pack_length, alignment=self._alignment)

                if pack_length:
                    pack_length = False

        # use send handle from reset
        self._sendh.send(msg)
        msg.destroy()

        # if lock is False, return the subclass value
        if lock in (True, None):
            lock = Lock(recursive=True)

        if isinstance(lock, bool):
            pass
        elif isinstance(lock, Lock):
            self._lock = lock
            self.get_lock = self._get_lock
            self.get_obj = self._type
        else:
            raise AttributeError(f"The Lock must be a bool or dragon.native.lock.Lock, but is  {type(lock)}")

    def acquire(self):
        """Acquire the internal lock object"""
        return self._lock.acquire()

    def release(self):
        """Release the internal lock object"""
        return self._lock.release()

    def _reset(self) -> None:
        self._recvh = self._channel.recvh()
        self._sendh = self._channel.sendh(return_mode=WHEN_DEPOSITED)
        self._recvh.open()
        self._sendh.open()
        self._mpool = MemoryPool.attach(B64.str_to_bytes(this_process.default_pd))

    def __len__(self):
        return self._data_len

    def __setitem__(self, pos, arr):

        # grab bytes from memory pool
        msg = self._recvh.recv()
        mview = msg.bytes_memview()

        # Structure is placed in array
        if self._is_structure(arr):
            stride = ctypes.sizeof(self._type)
            start = pos * stride
            stop = (pos + 1) * stride
            msg.clear_payload(start, stop)
            mview[start:stop] = memoryview(arr).cast("B")

        elif isinstance(arr, tuple):
            stride = ctypes.sizeof(self._type)
            start = pos * stride
            stop = (pos + 1) * stride
            msg.clear_payload(start, stop)
            mview[start:stop] = memoryview(self._type(*arr)).cast("B")

        # array placed in shared memory
        else:
            # overwrite only input values if that's what we've been given
            if isinstance(pos, slice) or isinstance(arr, int) or isinstance(arr, float):
                # Get the original array out of memory
                msg_val = self._unpack_array(mview)

                # Update the values
                msg_val[pos] = arr
                arr = msg_val

            # Now stick it all into memory
            pack_length = True
            for idx, value in enumerate(arr):

                # The first 8 bytes are for data length
                l_mview = self._get_mview_slice(mview, idx, pack_length)
                # puts array in shared memory

                ## note: if type is wchar, we always pack the length into the array for each element, so pack_length
                ## is ignored if type == ctypes.c_wchar. This helps prevent returning empty bytes if the input element
                ## doesn't make use of all 4 bytes of wchar
                _TYPE_CONV_VAL[self._type][0](mview=l_mview, convert=ValueConversion.OBJ_TO_BYTES,
                                              val=value, pack_length=pack_length, alignment=self._alignment)

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
            ## Note: this could be optimized to not return the full array, but I didn't do that here
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

# Private methods
    def _unpack_array(self, mview: memoryview):

        if self._type is not ctypes.c_wchar:
            # Get the data length out of the first 8 bytes:
            nbytes = _get_nbytes(mview[: self._alignment])

            # Construct the array
            arr = [_TYPE_CONV_VAL[self._type][0](mview=mview[i: i + self._alignment],
                                                 convert=ValueConversion.BYTES_TO_OBJ, nbytes=nbytes, alignment=self._alignment)
                   for i in range(self._alignment, len(mview), self._alignment)]

        # Handle wchar whose return length can vary from array element to array element
        else:
            arr = [_TYPE_CONV_VAL[self._type][0](mview=self._get_mview_slice(mview, i, True),
                                                 convert=ValueConversion.BYTES_TO_OBJ, alignment=self._alignment)
                   for i in range(0, int(len(mview) / (2 * self._alignment)))]

        return arr

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
            element_offset = base_offset + sum(self._struct1_sizes[: e_idx])
            start = element_offset
            stop = start + self._struct1_sizes[e_idx]
        else:
            if e_idx > 0:
                start = sum(self._struct0_sizes[: e_idx])
            else:
                start = 0
            stop = start + self._struct0_sizes[e_idx]

        return start, stop

    def _get_struct_mview_slice(self, mview: memoryview, s_idx: int, e_idx: int, pack_length: bool):
        """Get a slice of memoryview for reading/writing an array element with a packing scheme specific to a structure"""

        # compute the offset to the current array element
        start, stop = self._get_struct_mview_indices(s_idx, e_idx, pack_length)

        return mview[start: stop]

    def _get_mview_slice(self, mview: memoryview, index: int, pack_length: bool):
        """Get a slice of memoryview for reading/writing based on type and whether to pack the length"""

        if self._type is not ctypes.c_wchar:
            start = index * self._alignment + int(not pack_length) * self._alignment
            stop = start + self._alignment + int(pack_length) * self._alignment
        elif self._type is ctypes.c_wchar:
            start = index * (2 * self._alignment)
            stop = start + (2 * self._alignment)

        return mview[start: stop]

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
