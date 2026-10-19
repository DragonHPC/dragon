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
import numbers
from enum import Enum

import dragon
from .lock import Lock
from ..channels import Channel, Message, ChannelError
from ..managed_memory import MemoryPool
import dragon.utils as du
from ..dtypes import WHEN_DEPOSITED
from ..infrastructure.facts import default_pool_muid_from_index
from ..infrastructure.util import route
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
_ALIGNMENT = 8

_TYPE_CONV_VAL = {}  # dispatch router for manipulating the values based on datatype


class ValueConversion(Enum):
    BYTES_TO_OBJ = 0
    OBJ_TO_BYTES = 1


def _get_nbytes(mview: memoryview):
    nbytes_in_bytes = mview.tobytes()
    nbytes = int.from_bytes(nbytes_in_bytes, byteorder=sys.byteorder, signed=True)
    return nbytes


def _get_data_bytes(mview: memoryview, alignment: int = _ALIGNMENT, nbytes: int = None) -> bytes:
    """Read the first 8 bytes of the view to get the nbytes to read"""

    if not nbytes:
        nbytes = _get_nbytes(mview[:alignment])
        val_in_bytes = mview[alignment : nbytes + alignment].tobytes()
    else:
        val_in_bytes = mview[:nbytes].tobytes()

    return val_in_bytes


def _set_data_bytes(
    mview: memoryview, val_in_bytes: bytes, alignment: int = _ALIGNMENT, pack_length: bool = True
) -> None:
    """Store the nbytes the value takes up"""

    # Compute the number of bytes
    nbytes = len(val_in_bytes)
    nbytes_in_bytes = int(nbytes).to_bytes(length=alignment, byteorder=sys.byteorder, signed=True)

    # Store the nbytes and the value in the memoryview
    if pack_length:
        mview[:alignment] = nbytes_in_bytes
        mview[alignment : nbytes + alignment] = val_in_bytes
    else:
        mview[:nbytes] = val_in_bytes


def _extract_structure_types(type_fields: list, struct_len: int):
    var_names, subclass_types = [list(x) for x in zip(*type_fields)]
    tuple_type_list = list(subclass_types) * struct_len

    return var_names, subclass_types, tuple_type_list


def _map_structure_to_list(value: object, type_fields: list = None):
    # Get the structure type if given a list of types
    if type_fields is not None:
        var_names, subclass_types, tuple_type_list = _extract_structure_types(type_fields, len(value))

    # create list of types for tuple
    no_tuples = []
    for item in value:
        no_tuples.append(item)

    if type_fields:
        return no_tuples, var_names, subclass_types, tuple_type_list
    else:
        return no_tuples


@route(ctypes.c_char, _TYPE_CONV_VAL)
def _c_char_conversion(
    mview: memoryview,
    convert: ValueConversion,
    val: object = b"0",
    pack_length=True,
    nbytes: int = None,
    alignment: int = _ALIGNMENT,
):
    if convert is ValueConversion.BYTES_TO_OBJ:
        # c_char's are returned as bytes to the user
        return _get_data_bytes(mview, nbytes=nbytes)

    elif convert is ValueConversion.OBJ_TO_BYTES:
        # Get a bytes representation, utf-8 encoded if necessary
        if isinstance(val, bytes):
            valbytes = val
        else:
            # First make sure we weren't given a number via a range init or something
            if isinstance(val, numbers.Number):
                val = str(val)

            # Get a bytes representation, utf-8 encoded if necessary
            try:
                valbytes = bytes(val)
            except TypeError:
                valbytes = bytes(val.encode("utf-8"))

        # Get the data into memory
        _set_data_bytes(mview, valbytes, pack_length=pack_length, alignment=alignment)


@route(ctypes.c_wchar, _TYPE_CONV_VAL)
def _c_wchar_conversion(
    mview: memoryview,
    convert: ValueConversion,
    val: object = "0",
    pack_length=True,
    nbytes: int = None,
    alignment: int = _ALIGNMENT,
):
    # w_char's prototype follows the template of the other conversion functions but ALWAYS
    # ignores the kwargs in favor of always packing the number of bytes for a given
    # value and doing so with a 4 byte alignment, so every 8 bytes is packed with
    # 4 bytes for length and then 4 bytes for characters. This prevents us
    # from appending null characters and returning them to the user since their
    # input string can occupty 1-4 bytes
    pack_length = True
    alignment = 4
    if convert is ValueConversion.BYTES_TO_OBJ:
        # c_wchar's are returned as a str
        val_in_bytes = _get_data_bytes(mview, alignment=alignment)
        val = str(val_in_bytes.decode("utf-8"))
        return val

    elif convert is ValueConversion.OBJ_TO_BYTES:
        # First make sure we weren't given a number via a range init or something
        if isinstance(val, numbers.Number):
            val = str(val)

        # Get a bytes representation, utf-8 encoded if necessary
        if isinstance(val, bytes):
            valbytes = val
        elif isinstance(val, str):
            valbytes = bytes(val.encode("utf-8"))
        else:
            raise AttributeError("ctypes._c_whar requires a str input")

        # Get the data into memory
        _set_data_bytes(mview, valbytes, pack_length=pack_length, alignment=alignment)


@route(ctypes.c_ubyte, _TYPE_CONV_VAL)
@route(ctypes.c_byte, _TYPE_CONV_VAL)
@route(ctypes.c_short, _TYPE_CONV_VAL)
@route(ctypes.c_ushort, _TYPE_CONV_VAL)
@route(ctypes.c_uint, _TYPE_CONV_VAL)
@route(ctypes.c_long, _TYPE_CONV_VAL)
@route(ctypes.c_ulong, _TYPE_CONV_VAL)
@route(ctypes.c_int, _TYPE_CONV_VAL)
def _c_int_conversion(
    mview: memoryview,
    convert: ValueConversion,
    val=0,
    pack_length=True,
    nbytes: int = None,
    alignment: int = _ALIGNMENT,
):
    if convert is ValueConversion.BYTES_TO_OBJ:
        # Get bytes representation of data
        val_in_bytes = _get_data_bytes(mview, nbytes=nbytes)
        # Convert from bytes to integer
        val = int.from_bytes(val_in_bytes, byteorder=sys.byteorder, signed=True)
        return val

    elif convert is ValueConversion.OBJ_TO_BYTES:
        # Get bytes data into memory
        valbytes = int(val).to_bytes(_ALIGNMENT, byteorder=sys.byteorder, signed=True)
        _set_data_bytes(mview, valbytes, pack_length=pack_length, alignment=alignment)


@route(ctypes.c_float, _TYPE_CONV_VAL)
@route(ctypes.c_double, _TYPE_CONV_VAL)
def _c_double_conversion(
    mview: memoryview,
    convert: ValueConversion,
    val=0.0,
    pack_length=True,
    nbytes: int = None,
    alignment: int = _ALIGNMENT,
):
    """Convert a double to bytes and set it in memory or do the inverse and return it"""
    if convert is ValueConversion.BYTES_TO_OBJ:
        val_in_bytes = _get_data_bytes(mview, nbytes=nbytes)
        (float_val,) = struct.unpack("d", val_in_bytes)
        return float(float_val)

    elif convert is ValueConversion.OBJ_TO_BYTES:
        valbytes = bytes(struct.pack("d", val))
        # Get the data into memory
        _set_data_bytes(mview, valbytes, pack_length=pack_length, alignment=alignment)


class PseudoStructure:
    def __init__(self, value_obj, offset=0):
        # stashed a reference to the value obj in dictionary which is useful for referencing value object from value and offset used in the Structure
        self.__dict__["value_obj"] = value_obj
        self.__dict__["offset"] = offset

    def __getattr__(self, name):
        # go and get field in Structure with that name and return it
        msg = self.value_obj._recvh.recv()
        mview = msg.bytes_memview()

        # use name to find offset (this would be the fields)
        idx = [n for n, t in self.value_obj._type._fields_].index(name)
        return_type = self.value_obj._type._fields_[idx][1]

        # size of type for the index
        nbytes = None
        if self.offset > 0:
            pack_length = False
            nbytes = self.value_obj._struct1_sizes[idx]
            l_mview = self.value_obj._get_struct_mview_slice(
                mview=mview, s_idx=self.offset, e_idx=idx, pack_length=pack_length
            )
        else:
            pack_length = True
            alignment_offset = 2 * _ALIGNMENT  # nbytes in first 8 bytes. value in latter 8
            start = idx * alignment_offset + self.offset
            stop = (idx + 1) * alignment_offset + self.offset
            l_mview = mview[start:stop]

        # grab Structure data from memory
        value_info = _TYPE_CONV_VAL[return_type][0](
            mview=l_mview, convert=ValueConversion.BYTES_TO_OBJ, pack_length=pack_length, nbytes=nbytes
        )
        self.value_obj._sendh.send(msg)
        msg.destroy()

        return value_info

    def __setattr__(self, name, value):
        # Get the message and grab the memview
        msg = self.value_obj._recvh.recv()
        mview = msg.bytes_memview()

        # use name to find offset (this would be the fields)
        idx = [n for n, _ in self.value_obj._type._fields_].index(name)

        # size of type for the index
        c_return_type = self.value_obj._type._fields_[idx][1]

        if self.offset > 0:
            pack_length = False
            l_mview = self.value_obj._get_struct_mview_slice(
                mview=mview, s_idx=self.offset, e_idx=idx, pack_length=pack_length
            )
        else:
            pack_length = True
            alignment_offset = 2 * _ALIGNMENT  # nbytes in first 8 bytes. value in latter 8
            start = idx * alignment_offset + self.offset
            stop = (idx + 1) * alignment_offset + self.offset
            l_mview = mview[start:stop]

        _TYPE_CONV_VAL[c_return_type][0](
            l_mview, convert=ValueConversion.OBJ_TO_BYTES, val=value, pack_length=pack_length
        )

        # write Structure to memory
        self.value_obj._sendh.send(msg)
        msg.destroy()


class Value:
    """This class implements Dragon value resides in the Dragon channel."""

    _types = None
    # list of tuple types
    subclass_types = None
    # type of tuple at index
    tuple_type_list = None
    item_type = None
    _var_names = []
    _lock = None

    _alignment = 8  # all values will be 8 byte aligned

    def __getstate__(self):
        return (
            self._channel.serialize(),
            self._type,
            self._var_names,
            self._muid,
            self.subclass_types,
            self.tuple_type_list,
            self._lock,
        )

    def __setstate__(self, state):
        # Get the pickled state variables
        (serialized_bytes, _type, _var_names, _muid, subclass_types, tuple_type_list, _lock) = state

        self._var_names = _var_names
        self._type = _type
        self._muid = _muid
        self.subclass_types = subclass_types
        self.tuple_type_list = tuple_type_list
        self._channel = dragon.channels.Channel.attach(serialized_bytes)
        self._lock = _lock

        self._reset()
        get_refcnt(self._channel.cuid)

    def __repr__(self):
        return f"Dragon Value(type={self._type}, value={self.value}, cuid={self._channel.cuid})"

    def __init__(self, typecode_or_type, value: object = None, lock: Lock or bool = False, m_uid: int = _DEF_MUID):
        """Initialize a value object.
        :param typecode_or_type: the typecode or type is returned from the dictionary, _TYPECODE_TO_TYPE
        :type typecode_or_type: str or ctypes, required
        :param value: the value for the object
        :type value: int, optional
        :param m_uid: memory pool to create the channel in and message to write value and typecode_or_type in managed memory, defaults to _DEF_MUID
        :type m_uid: int, optional
        """
        # First 8 bytes are just for the size of the value. 2nd is for the value, which are all 8 bytes unless
        # we have a structure or tule
        self._alignment = _ALIGNMENT

        # Figure out how much space we need beyond that
        size = 2 * self._alignment
        self._struct_fields = {}
        if isinstance(typecode_or_type, str):
            if typecode_or_type in _TYPECODE_TO_TYPE.keys():
                typecode_or_type = _TYPECODE_TO_TYPE[typecode_or_type]
            else:
                raise AttributeError(f"typecode not found, has to be one of {list(_TYPECODE_TO_TYPE.keys())}")

        # Type checking against Python type to make the structure check not croak and then ctypes.Structure
        elif isinstance(typecode_or_type, type) and issubclass(typecode_or_type, ctypes.Structure):
            # For structure, we need to make sure we have enough space for every entry and its size
            size = 2 * (self._alignment * len(typecode_or_type._fields_))
            self._struct_fields = {attr for attr, _ in typecode_or_type._fields_}
        else:
            if typecode_or_type not in _SUPPORTED_TYPES:
                raise AttributeError(f"type not found, has to be one of {_SUPPORTED_TYPES}")

        # don't lose our ctype
        self._type = typecode_or_type

        LOGGER.debug(f"Init Dragon Native Value with typecode_or_type={typecode_or_type}, value={value}, m_uid={m_uid}")

        # The strange logic in here flows from requirements outlined in cpython multiprocessing unittests for Value. Ignore
        # the strange logic at your own risk.
        _lock_instance = isinstance(lock, dragon.mpbridge.synchronize.DragonLock)
        if lock in [True, None] or _lock_instance:
            if not _lock_instance:
                lock = Lock(recursive=True)
            self.get_obj = self._type
        elif lock is False:
            pass
        else:
            raise AttributeError("Invalid type specificied for lock")
        self._lock = lock

        # If we were given a structure class, cast it to a list

        # Get a msg and a memview for the value inside our pool
        msg = self._get_msg_from_pool(size, value, m_uid)
        mview = msg.bytes_memview()

        # if we were given an initial value, cast it to bytes and set it in memory view
        self._var_names = []
        self.subclass_types = None
        if value is not None:
            if issubclass(typecode_or_type, ctypes.Structure):
                if isinstance(value, tuple):
                    value = list(value)
                value, self._var_names, self.subclass_types, self.tuple_type_list = _map_structure_to_list(
                    value, self._type._fields_
                )

                for idx, val in enumerate(value):
                    item_type = self.tuple_type_list[idx]

                    # Pad by a factor of 2 in order to include length of data
                    start = idx * (2 * self._alignment)
                    stop = start + (2 * self._alignment)
                    _TYPE_CONV_VAL[item_type][0](
                        mview=mview[start:stop], convert=ValueConversion.OBJ_TO_BYTES, val=val, pack_length=True
                    )
            else:
                # write Value to memory
                _TYPE_CONV_VAL[self._type][0](mview=mview, convert=ValueConversion.OBJ_TO_BYTES, val=value)

        # Otherwise, fill the buffer with 0s
        else:
            mview[: ctypes.sizeof(self._type)] = int(0).to_bytes(
                length=ctypes.sizeof(self._type), byteorder=sys.byteorder, signed=False
            )
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

    def __getattr__(self, name):
        """This allows structure attributes to be accessed as if part of the class"""

        if name == "get_lock" and self._lock is not False:
            return self._get_lock

        try:
            x = super().__getattr__(name)
            return x
        except AttributeError:
            if name in self._var_names:
                x = self.value.__getattr__(name)
                return x
            else:
                raise

        raise AttributeError(f"{name} is not an attribute of Dragon Value")

    def __setattr__(self, name, val):
        """This allows structure attributes to be modded as if they're part of the base class"""

        raise_ex = None
        non_val = None
        try:
            super().__setattr__(name, val)
        except AttributeError as e:
            raise_ex = e

        if name in self._var_names:
            self._set_value(val, struct_attr=name)
        else:
            non_val = True

        if raise_ex and non_val:
            raise raise_ex

    def _get_lock(self):
        """Acquire the lock"""
        return self._lock

    def _get_value(self) -> object:
        """A callable method for getting a requested value"""

        # Structure goes into Pseudostructure class and type checking against Python type and ctypes.Structure to make sure typecode_or_type is ctypes.Structure and not None
        if (
            isinstance(self._type, type)
            and issubclass(self._type, ctypes.Structure)
            and self.subclass_types is not None
        ):
            value_info = PseudoStructure(self)

        else:
            # grab mview from the channel
            msg = self._recvh.recv()
            mview = msg.bytes_memview()

            # And now convert the memory to the correct return type
            value_info = _TYPE_CONV_VAL[self._type][0](mview=mview, convert=ValueConversion.BYTES_TO_OBJ)

            self._sendh.send(msg)
            msg.destroy()

        # flattens out value information
        if type(value_info) is list:
            value_info = value_info[0]
        return value_info

    @property
    def value(self) -> object:
        """Get the current value.

        :return: The current value
        :rtype: object
        """

        return self._get_value()

    def _set_structure(self, idx, val, mview):
        """Set an individual element inside of a Structure object

        :param idx: index in ordered list representation of sturcture
        :type idx: int
        :param val: value being stored in the memoryview object
        :type val: object
        :param mview: memoryview representation of dragon message buffer
        :type mview: memoryview
        """

        item_type = self.tuple_type_list[idx]
        start = idx * (2 * self._alignment)
        stop = start + (2 * self._alignment)
        _TYPE_CONV_VAL[item_type][0](
            mview=mview[start:stop], convert=ValueConversion.OBJ_TO_BYTES, val=val, pack_length=True
        )

    def _set_value(self, val: object, struct_attr=None) -> None:
        """A callable method for seting the value"""

        # grabs value from the channel
        msg = self._recvh.recv()

        # Get the memview
        mview = msg.bytes_memview()

        if isinstance(val, ctypes.Structure):
            no_tuples = []
            for item in val:
                no_tuples.append(item)
            val = no_tuples

        # If we are given a structure and weren't given it at __init__, treat it correctly by casting
        # it to a list
        if issubclass(self._type, ctypes.Structure):
            # This is a request to update a single value of the structure
            if struct_attr:
                # Get the index of this particular attribute
                idx = self._var_names.index(struct_attr)
                self._set_structure(idx, val, mview)

            # This is a request to update the full structure. We construct a list of all the values paired with their
            # attribute names and then set them all
            else:
                val, self._var_names, self.subclass_types, self.tuple_type_list = _map_structure_to_list(
                    val, self._type._fields_
                )
                for idx, v in enumerate(val):
                    self._set_structure(idx, v, mview)
        # writes value to memory
        else:
            _TYPE_CONV_VAL[self._type][0](mview=mview, convert=ValueConversion.OBJ_TO_BYTES, val=val)

        self._sendh.send(msg)
        msg.destroy()

    @value.setter
    def value(self, val: object) -> None:
        """Set the value

        :param val: the new value
        :type val: object
        :return: None
        :rtype: NoneType
        """

        self._set_value(val)

    # private methods
    def _get_msg_from_pool(self, size: int, value: object = None, m_uid: int = _DEF_MUID):
        # for repr method
        self._muid = m_uid

        sh_channel_options = ShepherdChannelOptions(capacity=1)
        gs_channel_options = ChannelOptions(ref_count=True, local_opts=sh_channel_options)
        descriptor = create(m_uid, options=gs_channel_options)
        self._channel = Channel.attach(descriptor.sdesc)
        self._reset()

        # create value in shared memory
        mpool = self._channel.get_pool()
        # if the channel isn't local, then fall back on using the default allocation pool
        if not mpool.is_local:
            mpool = self._channel.default_alloc_pool

        msg = Message.create_alloc(mpool, size)
        return msg

    def _reset(self) -> None:
        self._recvh = self._channel.recvh()
        self._sendh = self._channel.sendh(return_mode=WHEN_DEPOSITED)
        self._recvh.open()
        self._sendh.open()
        self._mpool = MemoryPool.attach(du.B64.str_to_bytes(this_process.default_pd))
