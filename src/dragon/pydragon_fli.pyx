from dragon.dtypes_inc cimport *
from dragon.channels cimport *
from dragon.managed_memory cimport *
import dragon.dtypes as dtypes
import dragon.infrastructure.parameters as dparms
import dragon.infrastructure.facts as dfacts
import dragon.globalservices.channel as dgchan
from dragon.localservices.options import ChannelOptions

BUF_READ = PyBUF_READ
BUF_WRITE = PyBUF_WRITE
DEFAULT_CLOSE_TIMEOUT = 5
STREAM_CHANNEL_IS_MAIN = 1010

cdef enum:
    C_TRUE = 1
    C_FALSE = 0

cdef timespec_t* _computed_timeout(timeout, timespec_t* time_ptr):

    if timeout is None:
        time_ptr = NULL
    elif isinstance(timeout, int) or isinstance(timeout, float):
        if timeout < 0:
            raise ValueError('Cannot provide timeout < 0.')

        # Anything >= 0 means use that as seconds for timeout.
        time_ptr.tv_sec = int(timeout)
        time_ptr.tv_nsec =  int((timeout - time_ptr.tv_sec)*1000000000)
    else:
        raise ValueError('The timeout value must be a float or int')

    return time_ptr

class DragonFLIError(Exception):

    def __init__(self, lib_err, msg):
        cdef char * errstr = dragon_getlasterrstr()

        self.msg = msg
        self.lib_msg = errstr[:].decode('utf-8')
        lib_err_str = dragon_get_rc_string(lib_err)
        self.lib_err = lib_err_str[:].decode('utf-8')
        free(errstr)

    def __str__(self):
        return f"FLI Exception: {self.msg}\n*** Dragon C-level Traceback: ***\n{self.lib_msg}\n*** End C-level Traceback: ***\nDragon Error Code: {self.lib_err}"

class FLIEOT(DragonFLIError, EOFError):
    pass


cdef class FLISendH:
    """
    Sending handle for FLInterfaces
    """

    cdef:
        dragonFLISendHandleDescr_t _sendh
        dragonFLIDescr_t _adapter
        bool _is_open

    def __init__(self, FLInterface adapter, Channel stream_channel=None, timeout=None, use_main_as_stream_channel=False):
        cdef:
            dragonError_t derr
            dragonChannelDescr_t * c_strm_ch = NULL
            timespec_t timer
            timespec_t* time_ptr

        self._adapter = adapter._adapter
        time_ptr = _computed_timeout(timeout, &timer)

        if stream_channel is not None:
            c_strm_ch = &stream_channel._channel

        if use_main_as_stream_channel:
            c_strm_ch = STREAM_CHANNEL_IS_MAIN_FOR_1_1_CONNECTION

        with nogil:
            derr = dragon_fli_open_send_handle(&self._adapter, &self._sendh, c_strm_ch, time_ptr)

        if derr != DRAGON_SUCCESS:
            raise DragonFLIError(derr, "Could not open send handle stream.")

        self._is_open = True

    def close(self, timeout=None):
        cdef:
            dragonError_t derr
            timespec_t timer
            timespec_t* time_ptr

        if not self._is_open:
            return

        time_ptr = _computed_timeout(timeout, &timer)

        with nogil:
            derr = dragon_fli_close_send_handle(&self._sendh, time_ptr)

        if derr != DRAGON_SUCCESS:
            raise DragonFLIError(derr, "Could not close send handle stream.")

        self._is_open = False

    def __del__(self):
        try:
            self.close(timeout=DEFAULT_CLOSE_TIMEOUT)
        except:
            pass

    def send_bytes(self, bytes data, uint64_t arg=0, bool buffer=False, timeout=None):
        cdef:
            dragonError_t derr
            #uint8_t * c_data
            size_t num_bytes
            timespec_t timer
            timespec_t* time_ptr
            int data_len

        if self._is_open == False:
            raise RuntimeError("Handle not open, cannot send data.")

        time_ptr = _computed_timeout(timeout, &timer)

        cdef const unsigned char[:] c_data = data
        data_len = len(data)
        arg_val = arg

        with nogil:
            derr = dragon_fli_send_bytes(&self._sendh, data_len, <uint8_t *>&c_data[0], arg, buffer, time_ptr)
        if derr != DRAGON_SUCCESS:
            raise DragonFLIError(derr, "Failed to send message over stream channel.")

    def send_mem(self, MemoryAlloc mem, uint64_t arg=0, timeout=None):
        cdef:
            dragonError_t derr
            timespec_t timer
            timespec_t* time_ptr

        if self._is_open == False:
            raise RuntimeError("Handle not open, cannot send data.")

        time_ptr = _computed_timeout(timeout, &timer)
        arg_val = arg

        with nogil:
            derr = dragon_fli_send_mem(&self._sendh, &mem._mem_descr, arg, time_ptr)
        if derr != DRAGON_SUCCESS:
            raise DragonFLIError(derr, "Failed to send memory over stream channel.")

    def create_fd(self, bool buffered=False, size_t chunk_size=0, arg=0, timeout=None):
        """
        Opens a writable file-descriptor and returns it.
        """
        cdef:
            dragonError_t derr
            int fdes
            timespec_t timer
            timespec_t* time_ptr
            uint64_t user_arg

        if self._is_open == False:
            raise RuntimeError("Handle not open, cannot get a file descriptor.")

        time_ptr = _computed_timeout(timeout, &timer)
        user_arg = arg

        with nogil:
            derr = dragon_fli_create_writable_fd(&self._sendh, &fdes, buffered, chunk_size, user_arg, time_ptr)
        if derr != DRAGON_SUCCESS:
            raise DragonFLIError(derr, "Could not open writeable file descriptor.")

        return fdes

    def finalize_fd(self):
        """
        Flushes a file-descriptor and waits until all buffers are written and the
        file descriptor is closed.
        """
        cdef:
            dragonError_t derr

        if self._is_open == False:
            raise RuntimeError("Handle is not open, cannot finalize an fd on a closed send handle.")

        with nogil:
            derr = dragon_fli_finalize_writable_fd(&self._sendh)
        if derr != DRAGON_SUCCESS:
            raise DragonFLIError(derr, "Could not finalize writable file descriptor")



cdef class FLIRecvH:
    """
    Receiving handle for FLInterfaces
    """

    cdef:
        dragonFLIRecvHandleDescr_t _recvh
        dragonFLIDescr_t _adapter
        bool _is_open

    def __init__(self, FLInterface adapter, Channel stream_channel=None, timeout=None, use_main_as_stream_channel=False):
        """
        Open the handle, optionally with a specific stream channel and timeout
        """
        cdef:
            dragonError_t derr
            dragonChannelDescr_t * c_strm_ch = NULL
            timespec_t timer
            timespec_t* time_ptr

        # This seems short, might flesh out more later
        self._adapter = adapter._adapter

        time_ptr = _computed_timeout(timeout, &timer)

        if stream_channel is not None:
            c_strm_ch = &stream_channel._channel

        if use_main_as_stream_channel:
            c_strm_ch = STREAM_CHANNEL_IS_MAIN_FOR_1_1_CONNECTION

        with nogil:
            derr = dragon_fli_open_recv_handle(&self._adapter, &self._recvh, c_strm_ch, time_ptr)
        if derr != DRAGON_SUCCESS:
            raise DragonFLIError(derr, "Could not open receive handle stream")

        self._is_open = True

    def close(self, timeout=None):
        cdef:
            dragonError_t derr
            timespec_t timer
            timespec_t* time_ptr

        if not self._is_open:
            return

        time_ptr = _computed_timeout(timeout, &timer)

        with nogil:
            derr = dragon_fli_close_recv_handle(&self._recvh, time_ptr)
        if derr != DRAGON_SUCCESS:
            raise DragonFLIError(derr, "Could not close receive handle stream")

        self._is_open = False

    def __del__(self):
        try:
            self.close(timeout=DEFAULT_CLOSE_TIMEOUT)
        except:
            pass

    def recv_bytes_into(self, unsigned char[::1] bytes_buffer=None, timeout=None):
        cdef:
            uint64_t arg
            size_t max_bytes
            size_t num_bytes
            timespec_t timer
            timespec_t* time_ptr

        if self._is_open == False:
            raise RuntimeError("Handle is not open, cannot receive")

        time_ptr = _computed_timeout(timeout, &timer)

        max_bytes = len(bytes_buffer)

        # This gets a memoryview slice of the buffer
        cdef unsigned char [:] c_data = bytes_buffer
        # To pass in as a pointer, get the address of the 0th index &c_data[0]
        with nogil:
            derr = dragon_fli_recv_bytes_into(&self._recvh, max_bytes, &num_bytes, &c_data[0], &arg, time_ptr)
        if derr == DRAGON_EOT:
            raise FLIEOT(derr, "End of Transmission")
        if derr != DRAGON_SUCCESS:
            raise DragonFLIError(derr, "Could not receive into bytes buffer")

        # Landing pad should be populated, just return arg
        return arg

    def recv_bytes(self, timeout=None):
        cdef:
            dragonError_t derr
            size_t num_bytes
            size_t max_bytes = 0
            uint8_t * c_data
            uint64_t arg
            timespec_t timer
            timespec_t* time_ptr

        if self._is_open == False:
            raise RuntimeError("Handle is not open, cannot receive")

        time_ptr = _computed_timeout(timeout, &timer)

        # A max_bytes value of 0 means "get everything"
        with nogil:
            derr = dragon_fli_recv_bytes(&self._recvh, max_bytes, &num_bytes, &c_data, &arg, time_ptr)
        if derr == DRAGON_EOT:
            raise FLIEOT(derr, "End of Transmission")
        if derr != DRAGON_SUCCESS:
            raise DragonFLIError(derr, "Error receiving FLI data")

        # Convert to a memoryview
        py_view = PyMemoryView_FromMemory(<char *>c_data, num_bytes, BUF_WRITE)
        # Convert memoryview to bytes
        py_bytes = py_view.tobytes()
        # Release underlying malloc now that we have a copy
        free(c_data)
        # Return data and metadata as a tuple
        return (py_bytes, arg)

    def recv_mem(self, timeout=None):
        cdef:
            dragonError_t derr
            dragonMemoryDescr_t mem
            uint64_t arg
            timespec_t timer
            timespec_t* time_ptr

        if self._is_open == False:
            raise RuntimeError("Handle is not open, cannot receive memory object")

        time_ptr = _computed_timeout(timeout, &timer)

        with nogil:
            derr = dragon_fli_recv_mem(&self._recvh, &mem, &arg, time_ptr)
        if derr == DRAGON_EOT:
            raise FLIEOT(derr, "End of Transmission")
        if derr != DRAGON_SUCCESS:
            raise DragonFLIError(derr, "Error receiving FLI data into memory object")

        mem_obj = MemoryAlloc.cinit(mem)
        return (mem_obj, arg)

    def create_fd(self, timeout=None):
        """
        Creates a readable file-descriptor and returns it.
        """
        cdef:
            dragonError_t derr
            int fdes
            timespec_t timer
            timespec_t* time_ptr

        if self._is_open == False:
            raise RuntimeError("Handle is not open, cannot create a file descriptor on a closed handle.")

        time_ptr = _computed_timeout(timeout, &timer)

        with nogil:
            derr = dragon_fli_create_readable_fd(&self._recvh, &fdes, time_ptr)
        if derr != DRAGON_SUCCESS:
            raise DragonFLIError(derr, "Could not open readable file descriptor")

        return fdes

    def finalize_fd(self):
        """
        Flushes a file-descriptor and waits until all buffers are read and the
        file descriptor is closed.
        """
        cdef:
            dragonError_t derr

        if self._is_open == False:
            raise RuntimeError("Handle is not open, cannot finalize an fd on a closed receive handle.")

        with nogil:
            derr = dragon_fli_finalize_readable_fd(&self._recvh)
        if derr != DRAGON_SUCCESS:
            raise DragonFLIError(derr, "Could not finalize readable file descriptor")




cdef class FLInterface:
    """
    Cython wrapper for the File-Like-Interface
    """

    cdef:
        dragonFLIDescr_t _adapter
        dragonFLISerial_t _serial
        bool _is_serialized
        list stream_channel_list
        MemoryPool pool


    def __getstate__(self):
        return (self.serialize(),self.pool)

    def __setstate__(self, state):
        serial_fli, pool = state
        if not pool.is_local:
            pool = None
        self._attach(serial_fli, pool)


    def _attach(self, ser_bytes, MemoryPool pool=None):
        cdef:
            dragonError_t derr
            dragonFLISerial_t _serial
            dragonMemoryPoolDescr_t * mpool = NULL

        _serial.len = len(ser_bytes)
        cdef const unsigned char[:] cdata = ser_bytes
        _serial.data = <uint8_t *>&cdata[0]
        self._is_serialized = False

        if pool is not None:
            mpool = &pool._pool_hdl
            self.pool = pool
        else:
            self.pool = None

        derr = dragon_fli_attach(&_serial, mpool, &self._adapter)
        if derr != DRAGON_SUCCESS:
            raise DragonFLIError(derr, "Could not attach to FLI adapter")

        return self

    def __del__(self):
        if self._is_serialized:
            dragon_fli_serial_free(&self._serial)

    def __init__(self, Channel main_ch=None, Channel manager_ch=None, MemoryPool pool=None,
                        stream_channels=[], bool use_buffered_protocol=False):

        cdef:
            dragonError_t derr
            dragonChannelDescr_t ** strm_chs = NULL
            dragonChannelDescr_t * c_main_ch = NULL
            dragonChannelDescr_t * c_mgr_ch = NULL
            dragonMemoryPoolDescr_t * c_pool = NULL
            Channel ch # Necessary to cast python objects into cython objects when pulling out stream_channel values

        self._is_serialized = False

        ###
        ### If creating main and manager channels, make sure their capacity is set to the number of stream channels
        ###

        num_stream_channels = len(stream_channels)

        if pool is None and main_ch is None:
            # Get default pool muid and create a main_channel from there
            default_muid = dfacts.default_pool_muid_from_index(dparms.this_process.index)
            ch_options = ChannelOptions(capacity=num_stream_channels)
            main_ch = dgchan.create(default_muid, options=ch_options)

        if pool is None and main_ch is not None:
            # Do nothing, C code handles this
            pass

        # Get pointers to the handles
        # This simplifies the actual C call since the pointers will either be NULL or assigned to the objects handle
        if main_ch is not None:
            c_main_ch = &main_ch._channel

        if manager_ch is not None:
            c_mgr_ch = &manager_ch._channel

        if pool is not None:
            c_pool = &pool._pool_hdl
            self.pool = pool
        else:
            self.pool = None

        if num_stream_channels > 0:
            strm_chs = <dragonChannelDescr_t **>malloc(sizeof(dragonChannelDescr_t*) * num_stream_channels)
            for i in range(num_stream_channels):
                ch = stream_channels[i]
                strm_chs[i] = &ch._channel

        derr = dragon_fli_create(&self._adapter, c_main_ch, c_mgr_ch, c_pool,
                                 num_stream_channels, strm_chs, use_buffered_protocol, NULL)

        if strm_chs != NULL:
            free(strm_chs) # Free our Malloc before error checking to prevent memory leaks
        if derr != DRAGON_SUCCESS:
            raise DragonFLIError(derr, "Failed to create new FLInterface")

    @classmethod
    def create_buffered(cls, Channel main_ch=None, MemoryPool pool=None):
        """
        Helper function to more easily create a simple buffered FLInterface
        Does not require any internal function, it's simply limiting the number of options for the user
        in order to make it more straightforward to make an explicitly buffered FLI
        """
        return cls(main_ch=main_ch, pool=pool, use_buffered_protocol=True)


    def destroy(self):
        cdef dragonError_t derr

        derr = dragon_fli_destroy(&self._adapter)
        if derr != DRAGON_SUCCESS:
            raise DragonFLIError(derr, "Failed to destroy FLInterface")


    def serialize(self):
        cdef dragonError_t derr

        if not self._is_serialized:
            derr = dragon_fli_serialize(&self._adapter, &self._serial)
            if derr != DRAGON_SUCCESS:
                raise DragonFLIError(derr, "Failed to serialize FLInterface")

            self._is_serialized = True

        py_obj = self._serial.data[:self._serial.len]
        return py_obj

    @classmethod
    def attach(cls, serialized_bytes, mem_pool=None):
        # If mem_pool is None, the default node-local memorypool will be used
        empty_fli = cls.__new__(cls)
        return empty_fli._attach(serialized_bytes, mem_pool)

    def detach(self):
        cdef dragonError_t derr

        derr = dragon_fli_detach(&self._adapter)
        if derr != DRAGON_SUCCESS:
            raise DragonFLIError(derr, "Failed to detach from FLI adapter")

    def sendh(self, *args, **kwargs):
        """
        Return a new FLI Send Handle object
        """
        return FLISendH(self, *args, **kwargs)

    def recvh(self, *args, **kwargs):
        """
        Return a new FLI Recv Handle object
        """
        return FLIRecvH(self, *args, **kwargs)
