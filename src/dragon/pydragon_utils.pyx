from dragon.dtypes_inc cimport *
from dragon.return_codes cimport *
#import cython

cpdef host_id_from_k8s(str pod_uid):
    """This is used to get a hostid based on the k8s pod uid.
    It is called when a process in one node needs to know
    the host_id of another node.
    It can be called from a node/pod as many times as needed
    in order to translate a pod uid to a dragon hostid.
    For example, a backend pod can query the hostids of the other
    backend pods. For setting/assigning the hostid of the pod itself,
    each pod needs to call set_host_id().
    """
    pod_uuid = pod_uid.encode('utf-8')
    return dragon_host_id_from_k8s_uuid(<char *>pod_uuid)

cpdef host_id():
    return dragon_host_id()

cpdef set_host_id(unsigned long new_id):
    dnew_id = <dragonULInt> new_id
    dragon_set_host_id(new_id)

cpdef get_local_rt_uid():
    return dragon_get_local_rt_uid()

cpdef set_procname(str name):
    estr = name.encode('utf-8')
    cdef dragonError_t err = dragon_set_procname(<char *>estr)
    if err != DRAGON_SUCCESS:
        raise ValueError('Could not set process name')

cdef class B64:
    """ Cython wrapper for Dragon's byte <> string conversion routines. """
    cdef:
        char* _encoded_string

    def __cinit__(self):
        self._encoded_string = NULL

    def __del__(self):
        if self._encoded_string != NULL:
            free(self._encoded_string)
            self._encoded_string = NULL

    def __init__(self, data):
        """Convert a bytes array into a base64 encoded string.
        :param data: The list of bytes to convert.
        :return: A new B64String object containing the base64 encoded string.
        """
        self._encoded_string = dragon_base64_encode(data, len(data))

    def __str__(self):
        return self._encoded_string[:strlen(self._encoded_string)].decode('utf-8')

    def _initialize_from_str(self, serialized_str):
        cdef char * todataptr = <char*>malloc(len(serialized_str)+1)
        if todataptr == NULL:
            raise ValueError('Could not allocate space for B64 object.')

        data = serialized_str.encode('utf-8')
        size = len(serialized_str)+1
        cdef char * fromdataptr = data
        memcpy(todataptr, fromdataptr, size)
        self._encoded_string = todataptr

    def decode(self):
        cdef:
            uint8_t *data
            size_t decoded_length

        data = dragon_base64_decode(self._encoded_string, &decoded_length)
        if data == NULL:
            raise ValueError('Invalid Base64 Value')

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
        data = cls.from_str(the_str).decode()
        if data is None:
            raise ValueError('Could not convert Base64 string to bytes.')

        return data

cpdef b64encode(the_bytes):
    return B64.bytes_to_str(the_bytes)

cpdef b64decode(the_str):
    return B64.str_to_bytes(the_str)

cpdef hash(byte_str:bytes):
    cdef:
        const unsigned char[:] buf = byte_str

    return dragon_hash(<void*>&buf[0], len(byte_str))

cpdef set_local_kv(key, value, timeout=None):
    cdef:
        const unsigned char* val_ptr
        unsigned char* empty_str = ""
        const unsigned char[:] key_str
        const unsigned char[:] val_str

        dragonError_t err
        timespec_t * time_ptr
        timespec_t val_timeout

    if len(key) == 0:
        raise KeyError('Key cannot be empty')

    if timeout is None:
        time_ptr = NULL
    elif isinstance(timeout, int) or isinstance(timeout, float):
        if timeout < 0:
            raise ValueError('Cannot provide timeout < 0 to set_local_kv operation')
        # Anything > 0 means use that as seconds for timeout.
        time_ptr = & val_timeout
        val_timeout.tv_sec =  int(timeout)
        val_timeout.tv_nsec = int((timeout - val_timeout.tv_sec)*1000000000)
    else:
        raise ValueError('make_process_local timeout must be a float or int')

    key_str = str.encode(key)

    if len(value) > 0:
        val_str = str.encode(value)
        val_ptr = &val_str[0]
    else:
        val_ptr = empty_str

    with nogil:
        err = dragon_ls_set_kv(&key_str[0], val_ptr, time_ptr)

    if err != DRAGON_SUCCESS:
        raise RuntimeError(f'Could not set kv pair. EC=({dragon_get_rc_string(err)})\n ERR_MSG={dragon_getlasterrstr()}')

cpdef get_local_kv(key, timeout=None):
    cdef:
        const unsigned char[:] key_str
        char* val_str

        dragonError_t err
        timespec_t * time_ptr
        timespec_t val_timeout

    if timeout is None:
        time_ptr = NULL
    elif isinstance(timeout, int) or isinstance(timeout, float):
        if timeout < 0:
            raise ValueError('Cannot provide timeout < 0 to set_local_kv operation')
        # Anything > 0 means use that as seconds for timeout.
        time_ptr = & val_timeout
        val_timeout.tv_sec =  int(timeout)
        val_timeout.tv_nsec = int((timeout - val_timeout.tv_sec)*1000000000)
    else:
        raise ValueError('make_process_local timeout must be a float or int')

    key_str = str.encode(key)

    with nogil:
        err = dragon_ls_get_kv(&key_str[0], &val_str, time_ptr)

    if err == DRAGON_NOT_FOUND:
        raise KeyError(key)

    if err != DRAGON_SUCCESS:
        raise RuntimeError(f'Could not set kv pair. EC=({dragon_get_rc_string(err)})\n ERR_MSG={dragon_getlasterrstr()}')

    return val_str.decode('utf-8')

cpdef get_hugepage_mount():
    cdef:
        dragonError_t derr
        char *mount_dir
        bytes tmp_bytes

    derr = dragon_get_hugepage_mount(&mount_dir)
    if derr == DRAGON_SUCCESS:
        tmp_bytes = mount_dir
        return tmp_bytes.decode('utf-8')
    else:
        return None
