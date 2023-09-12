from dragon.dtypes_inc cimport *
from dragon.return_codes cimport *

cpdef host_id():
    return dragon_host_id()

cpdef set_host_id(int new_id):
    dnew_id = <dragonULInt> new_id
    dragon_set_host_id(new_id)

cpdef set_procname(str name):
    estr = name.encode('utf-8')
    cdef dragonError_t err = dragon_set_procname(<char *>estr)
    if err != DRAGON_SUCCESS:
        raise ValueError('Could not set process name')

cdef class B64:
    """ Cython wrapper for Dragon's byte <> string conversion routines. """
    cdef:
        char* _encoded_string
        size_t _length

    def __cinit__(self):
        self._encoded_string = NULL
        self._length = 0

    def __del__(self):
        if self._length > 0:
            free(self._encoded_string)
            self._encoded_string = NULL
            self._length = 0

    def __init__(self, data):
        """Convert a bytes array into a base64 encoded string.

        :param data: The list of bytes to convert.
        :return: A new B64String object containing the base64 encoded string.
        """
        self._encoded_string = dragon_base64_encode(data, len(data), &self._length)

    def __str__(self):
        return self._encoded_string[:self._length].decode('utf-8')

    def _initialize_from_str(self, serialized_str):
        self._length = 0
        cdef char * todataptr = <char*>malloc(len(serialized_str))
        if todataptr == NULL:
            raise ValueError('Could not allocate space for B64 object.')

        data = serialized_str.encode('utf-8')
        size = len(serialized_str)
        cdef char * fromdataptr = data
        memcpy(todataptr, fromdataptr, size)
        self._encoded_string = todataptr
        self._length = size

    def decode(self):
        cdef:
            uint8_t *data
            size_t decoded_length

        data = dragon_base64_decode(self._encoded_string, self._length, &decoded_length)
        val = data[:decoded_length]
        free(data)
        return val

    @classmethod
    def from_str(cls, serialized_str):
        obj = cls.__new__(cls)
        obj._initialize_from_str(serialized_str)
        return obj

    @classmethod
    def bytes_to_str(cls, the_bytes):
        """Converts bytes into a string by base64 encoding it.

        Convenience function to convert bytes objects to base64
        encoded strings.

        :param the_bytes: bytes to get encoded
        :return: string
        """
        return str(cls(the_bytes))

    @classmethod
    def str_to_bytes(cls, the_str):
        """Converts a base64 encoded string to a bytes object.

        Convenience function to unpack strings.

        :param the_str: base64 encoded string.
        :return: original bytes representation.
        """
        return cls.from_str(the_str).decode()