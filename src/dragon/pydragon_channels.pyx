"""Channels is the base communication mechanism for Dragon. Channels implements a multi-thread/multi-process safe
communication queue using :py:class:`dragon.managed_memory`.
"""
from dragon.dtypes_inc cimport *
from dragon.channels cimport *
from dragon.managed_memory cimport *
from dragon.rc import DragonError
import dragon.dtypes as dtypes
import dragon.infrastructure.parameters as dparms
from dragon.utils import B64
from dragon.dtypes import SPIN_WAIT, IDLE_WAIT, ADAPTIVE_WAIT
import math
import os
import pickle
import threading
from cython.cimports.cpython.ref import PyObject

################################
# Begin Cython definitions
################################

BUF_READ = PyBUF_READ
BUF_WRITE = PyBUF_WRITE

cdef enum:
    C_TRUE = 1
    C_FALSE = 0

cdef struct msg_blocks:
    uint8_t * _current_block
    size_t _current_block_size
    msg_blocks * _next

ctypedef msg_blocks msg_blocks_t

cdef timespec_t* _compute_timeout(timeout, timespec_t* default_val_ptr, timespec_t* time_value):

    cdef:
        timespec_t* time_ptr

    if timeout is None:
        time_ptr = default_val_ptr
        if default_val_ptr != NULL:
            time_value.tv_sec = default_val_ptr.tv_sec
            time_value.tv_nsec = default_val_ptr.tv_nsec
    elif isinstance(timeout, int) or isinstance(timeout, float):
        if timeout < 0:
            raise ValueError('Cannot provide timeout < 0.')

        # Anything > 0 means use that as seconds for timeout.
        time_ptr = time_value
        time_value.tv_sec = int(timeout)
        time_value.tv_nsec =  int((timeout - time_value.tv_sec)*1000000000)
    else:
        raise ValueError('The timeout value must be a float or int')

    return time_ptr

class ChannelError(dtypes.DragonException):
    pass

class ChannelTimeout(ChannelError, TimeoutError):
    pass

class ChannelHandleNotOpenError(ChannelError):
    pass

class ChannelSendError(ChannelError):
    pass

class ChannelSendTimeout(ChannelSendError, TimeoutError):
    pass

class ChannelValueError(ChannelError, ValueError):
    pass

class ChannelFull(ChannelSendError, TimeoutError):
    pass

class ChannelRecvError(ChannelError):
    pass

class ChannelRecvTimeout(ChannelRecvError, TimeoutError):
    pass

class ChannelEmpty(ChannelRecvError, TimeoutError):
    pass

class ChannelBarrierBroken(ChannelError, threading.BrokenBarrierError):
    pass

class ChannelBarrierReady(ChannelError):
    pass

class ChannelRemoteOperationNotSupported(ChannelError):
    pass

class ChannelDestroyed(ChannelError, dtypes.DragonObjectDestroyed, EOFError):
    pass

class ChannelSetError(Exception):
    def __init__(self, msg, lib_err=None, lib_msg=None, lib_err_str=None):
        self._msg = msg
        self._lib_err = lib_err
        cdef char * errstr
        if lib_err is not None and lib_msg is None and lib_err_str is None:
            errstr = dragon_getlasterrstr()
            self._lib_msg = errstr[:].decode('utf-8')
            free(errstr)
            rcstr = dragon_get_rc_string(lib_err)
            self._lib_err_str = rcstr[:].decode('utf-8')
        else:
            self._lib_err = lib_err
            self._lib_msg = lib_msg
            self._lib_err_str = lib_err_str

    def __str__(self):
        if self._lib_err is None:
            return f'ChannelSet Error: {self._msg}'

        return f'ChannelSet Library Error: {self._msg} | Dragon Msg: {self._lib_msg} | Dragon Error Code: {self._lib_err_str}'

    @property
    def lib_err(self):
        return self._lib_err

    def __repr__(self):
        return f'{self.__class__}({self._msg!r}, {self._lib_err!r}, {self._lib_msg!r}, {self._lib_err_str!r})'

class ChannelSetTimeout(ChannelSetError, TimeoutError):
    def __init__(self, msg, lib_err=None, lib_msg=None, lib_err_str=None):
        self._msg = msg
        self._lib_err = lib_err
        cdef char * errstr
        if lib_err is not None and lib_msg is None and lib_err_str is None:
            errstr = dragon_getlasterrstr()
            self._lib_msg = errstr[:].decode('utf-8')
            free(errstr)
            rcstr = dragon_get_rc_string(lib_err)
            self._lib_err_str = rcstr[:].decode('utf-8')
        else:
            self._lib_err = lib_err
            self._lib_msg = lib_msg
            self._lib_err_str = lib_err_str

    def __str__(self):
        if self._lib_err is None:
            return f'ChannelSet Error: {self._msg}'

        return f'ChannelSet Library Error: {self._msg} | Dragon Msg: {self._lib_msg} | Dragon Error Code: {self._lib_err_str}'

    @property
    def lib_err(self):
        return self._lib_err

    def __repr__(self):
        return f'{self.__class__}({self._msg!r}, {self._lib_err!r}, {self._lib_msg!r}, {self._lib_err_str!r})'


def register_gateways_from_env():
    """ TBD """
    cdef:
        dragonError_t err

    err = dragon_channel_register_gateways_from_env()

    if err != DRAGON_SUCCESS:
        raise ChannelError("Could not register gateways from environment.", err)

def discard_gateways():
    """ TBD """
    cdef:
        dragonError_t err

    err = dragon_channel_discard_gateways()
    if err != DRAGON_SUCCESS:
        raise ChannelError("Could not discard gateway definitions.", err)


cdef class Message:
    """Class for manipulating Dragon Channel Messages

    Dragon Messages are the interface for providing or receiving Managed Memory buffers through Channels.  This
    class provides methods for managing life cycle and direct access to memoryviews from Messages.
    """

    cdef dragonMessage_t _msg
    cdef dragonMemoryDescr_t * _mem
    cdef bint _allocated

    def __del__(self):
        # Note: This does not release managed memory allocations, just the local allocation made in create_alloc()
        if self._allocated == 1:
            free(self._mem)

    # @MCB: Should this and create_alloc be combined into a regular __init__()?
    # create_empty is only used by Receive Handle objects
    @staticmethod
    def create_from_mem(MemoryAlloc mem, hints = 0, clientid = 0):
        """
        Create a new Message object with no memory backing

        :return: New Message Object
        """
        cdef:
            dragonError_t derr
            dragonMessageAttr_t attrs

        msg = Message()
        derr = dragon_channel_message_attr_init(&attrs);
        if derr != DRAGON_SUCCESS:
            raise ChannelError("Could not create default message attributes", derr)

        if (not type(hints) is int) or hints < 0:
            raise ChannelError("The hints argument must be a non-negative integer.", DragonError.INVALID_ARGUMENT)

        if (not type(clientid) is int) or clientid < 0:
            raise ChannelError("The clientid argument must be a non-negative integer.", DragonError.INVALID_ARGUMENT)

        attrs.hints = hints
        attrs.clientid = clientid

        derr = dragon_channel_message_init(&msg._msg, &mem._mem_descr, &attrs)
        if derr != DRAGON_SUCCESS:
            raise ChannelError("Could not create empty message", derr)
        msg._allocated = 0

        return msg

    @staticmethod
    def create_empty():
        """
        Create a new Message object with no memory backing

        :return: New Message Object
        """
        cdef dragonError_t derr

        msg = Message()
        derr = dragon_channel_message_init(&msg._msg, NULL, NULL)
        if derr != DRAGON_SUCCESS:
            raise ChannelError("Could not create empty message", derr)
        msg._mem = NULL
        msg._allocated = 0

        return msg

    @staticmethod
    def create_alloc(MemoryPool mpool, size_t nbytes, hints = 0, clientid = 0, timeout=None):
        """
        Allocate memory and a new message object for inserting data into and sending

        :param mpool: The MemoryPool to allocate from
        :param nbytes: Size in bytes of the payload allocation
        :return: New Message object
        """
        cdef:
            dragonError_t derr
            dragonError_t merr
            dragonMemoryDescr_t * mem
            timespec_t alloc_timeout
            timespec_t timer
            dragonMessageAttr_t attrs

        derr = dragon_channel_message_attr_init(&attrs);
        if derr != DRAGON_SUCCESS:
            raise ChannelError("Could not create default message attributes", derr)

        if (not type(hints) is int) or hints < 0:
            raise ChannelError("The hints argument must be a non-negative integer.", DragonError.INVALID_ARGUMENT)

        if (not type(clientid) is int) or clientid < 0:
            raise ChannelError("The clientid argument must be a non-negative integer.", DragonError.INVALID_ARGUMENT)

        attrs.hints = hints
        attrs.clientid = clientid

        time_ptr = _compute_timeout(timeout, NULL, &timer)

        msg = Message()
        # Malloc here to later be cleaned up in destroy()
        mem = <dragonMemoryDescr_t *>malloc(sizeof(dragonMemoryDescr_t))

        with nogil:
            merr = dragon_memory_alloc_blocking(mem, &mpool._pool_hdl, nbytes, time_ptr)
        if merr != DRAGON_SUCCESS:
            raise ChannelError("Could not allocate memory from pool", merr)

        merr = dragon_memory_get_size(mem, &nbytes)
        if merr != DRAGON_SUCCESS:
            raise ChannelError("Could not get size memory descriptor", merr)

        derr = dragon_channel_message_init(&msg._msg, mem, &attrs)
        if derr != DRAGON_SUCCESS:
            raise ChannelError("Could not create message", derr)

        msg._mem = mem
        msg._allocated = 1

        return msg

    def destroy(self, free_mem=True):
        """
        Destroy the underlying message object in the channel, optionally freeing underlying memory

        :param free_mem: Default True.  Determines whether to free the allocated memory.
        :raises: ChannelError if message could not be freed.
        """
        cdef:
            dragonError_t derr
            dragonMemoryDescr_t mem
            bint ifree

        ifree = free_mem

        derr = dragon_channel_message_destroy(&self._msg, ifree)
        if derr != DRAGON_SUCCESS:
            raise ChannelError("Could not destroy message", derr)

        self._allocated = ifree

    def clear_payload(self, start=0, stop=None):
        """
        Clear the underlying payload of the message without
        requiring reconstruction.

        :return: None
        :raises: ChannelError if the message payload could not be cleated.
        """
        cdef:
            dragonError_t err
            dragonMemoryDescr_t mem
            size_t stop_idx

        err = dragon_channel_message_get_mem(&self._msg, &mem)
        if err != DRAGON_SUCCESS:
            raise ChannelError("Could not get memory descriptor from message", err)

        if stop is None:
            # The API will self adjust down to allowable stop.
            stop_idx = 0xffffffffffffffff
        else:
            stop_idx = stop

        err = dragon_memory_clear(&mem, start, stop_idx)
        if err != DRAGON_SUCCESS:
            raise ChannelError("Could not clear the memory", err)


    def bytes_memview(self):
        """
        Get a memoryview into the underlying message payload

        :return: Memoryview object to underlying message payload
        """
        cdef:
            dragonError_t derr
            dragonError_t merr
            char * ptr
            size_t nbytes
            dragonMemoryDescr_t mem

        derr = dragon_channel_message_get_mem(&self._msg, &mem)
        if derr != DRAGON_SUCCESS:
            raise ChannelError("Could not get memory descriptor from message", derr)

        merr = dragon_memory_get_pointer(&mem, <void **>&ptr)
        if merr != DRAGON_SUCCESS:
            raise ChannelError("Could not get pointer from memory descriptor", merr)

        merr = dragon_memory_get_size(&mem, &nbytes)
        if merr != DRAGON_SUCCESS:
            raise ChannelError("Could not get size memory descriptor", merr)

        #return memoryview(<char[:nbytes]>ptr)
        # This allows slicing assignments apparently.
        # ex: memview_obj[0:5] = b"Hello" works from this call, but not the above
        return PyMemoryView_FromMemory(ptr, nbytes, BUF_WRITE)

    def tobytes(self):
        """
        :return: Bytes object of the message payload
        """
        # @MCB: Doesn't feel quite right?
        mview = self.bytes_memview()
        return mview.tobytes()

    @property
    def hints(self):
        cdef:
            dragonError_t derr
            dragonMessageAttr_t attrs

        derr = dragon_channel_message_getattr(&self._msg, &attrs)
        if derr != DRAGON_SUCCESS:
            raise ChannelError("Could not get message attributes", derr)

        return attrs.hints

    @hints.setter
    def hints(self, value):
        cdef:
            dragonError_t derr
            dragonMessageAttr_t attrs

        if (not type(value) is int) or value < 0:
            raise ChannelError("The hints attribute must be a non-negative integer.", DragonError.INVALID_ARGUMENT)

        derr = dragon_channel_message_getattr(&self._msg, &attrs)
        if derr != DRAGON_SUCCESS:
            raise ChannelError("Could not get message attributes", derr)

        attrs.hints = value

        derr = dragon_channel_message_setattr(&self._msg, &attrs)
        if derr != DRAGON_SUCCESS:
            raise ChannelError("Could not set message attributes", derr)

    @property
    def clientid(self):
        cdef:
            dragonError_t derr
            dragonMessageAttr_t attrs

        derr = dragon_channel_message_getattr(&self._msg, &attrs)
        if derr != DRAGON_SUCCESS:
            raise ChannelError("Could not get message attributes", derr)

        return attrs.clientid

    @clientid.setter
    def clientid(self, value):
        cdef:
            dragonError_t derr
            dragonMessageAttr_t attrs

        if (not type(value) is int) or value < 0:
            raise ChannelError("The clientid attribute must be a non-negative integer.", DragonError.INVALID_ARGUMENT)

        derr = dragon_channel_message_getattr(&self._msg, &attrs)
        if derr != DRAGON_SUCCESS:
            raise ChannelError("Could not get message attributes", derr)

        attrs.clientid = value

        derr = dragon_channel_message_setattr(&self._msg, &attrs)
        if derr != DRAGON_SUCCESS:
            raise ChannelError("Could not set message attributes", derr)


cpdef enum OwnershipOnSend:
    copy_on_send
    transfer_ownership_on_send

cpdef enum LockType:
    FIFO = DRAGON_LOCK_FIFO,
    LITE = DRAGON_LOCK_FIFO_LITE,
    GREEDY = DRAGON_LOCK_GREEDY

cpdef enum EventType:
    POLLNOTHING = DRAGON_CHANNEL_POLLNOTHING
    POLLIN = DRAGON_CHANNEL_POLLIN
    POLLOUT = DRAGON_CHANNEL_POLLOUT
    POLLINOUT = DRAGON_CHANNEL_POLLINOUT
    POLLEMPTY = DRAGON_CHANNEL_POLLEMPTY
    POLLFULL = DRAGON_CHANNEL_POLLFULL
    POLLSIZE = DRAGON_CHANNEL_POLLSIZE
    POLLRESET = DRAGON_CHANNEL_POLLRESET
    POLLBARRIER = DRAGON_CHANNEL_POLLBARRIER_WAIT
    POLLBARRIER_ABORT = DRAGON_CHANNEL_POLLBARRIER_ABORT
    POLLBARRIER_RELEASE = DRAGON_CHANNEL_POLLBARRIER_RELEASE
    POLLBARRIER_ISBROKEN = DRAGON_CHANNEL_POLLBARRIER_ISBROKEN
    POLLBARRIER_WAITERS = DRAGON_CHANNEL_POLLBARRIER_WAITERS
    POLLBLOCKED_RECEIVERS = DRAGON_CHANNEL_POLLBLOCKED_RECEIVERS
    SEMAPHORE_P = DRAGON_SEMAPHORE_P
    SEMAPHORE_V = DRAGON_SEMAPHORE_V
    SEMAPHORE_VZ = DRAGON_SEMAPHORE_VZ
    SEMAPHORE_PEEK = DRAGON_SEMAPHORE_PEEK

cpdef enum FlowControl:
    NO_FLOW_CONTROL = DRAGON_CHANNEL_FC_NONE
    RESOURCES_FLOW_CONTROL = DRAGON_CHANNEL_FC_RESOURCES
    MEMORY_FLOW_CONTROL = DRAGON_CHANNEL_FC_MEMORY
    MSGS_FLOW_CONTROL = DRAGON_CHANNEL_FC_MSGS

cpdef enum ChannelFlags:
    NO_FLAGS = DRAGON_CHANNEL_FLAGS_NONE,
    MASQUERADE_AS_REMOTE = DRAGON_CHANNEL_FLAGS_MASQUERADE_AS_REMOTE

cdef class ChannelSendH:
    """
    Sending handle for outgoing channel messages
    """

    USE_CHANNEL_SENDH_DEFAULT = 'USE_CHANNEL_SENDH_DEFAULT'

    copy_on_send = OwnershipOnSend.copy_on_send
    transfer_ownership_on_send = OwnershipOnSend.transfer_ownership_on_send

    cdef:
        dragonChannelSendh_t _sendh
        dragonChannelDescr_t _ch
        Channel _channel
        timespec_t default_val


    def __init__(self, Channel ch, return_mode=dtypes.WHEN_BUFFERED, wait_mode=dtypes.DEFAULT_WAIT_MODE, timeout=None):
        cdef:
            dragonError_t derr
            dragonChannelSendAttr_t attrs

        wait_mode = dparms.cast_wait_mode(wait_mode)
        return_mode = dparms.cast_return_when_mode(return_mode)

        derr = dragon_channel_send_attr_init(&attrs)
        if derr != DRAGON_SUCCESS:
            raise ChannelError("Could not initialize default send attributes.", derr)

        attrs.return_mode = return_mode.value
        attrs.wait_mode = wait_mode.value
        default_val = DRAGON_CHANNEL_BLOCKING_NOTIMEOUT
        _compute_timeout(timeout, &default_val, &attrs.default_timeout)

        derr = dragon_channel_sendh(&ch._channel, &self._sendh, &attrs)
        if derr != DRAGON_SUCCESS:
            raise ChannelError("Could not created channel send handle", derr)

        self._ch._idx = ch._channel._idx  # Preserves consistency with __setstate__
        self._channel = ch

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    # Should we find a reason to pickle a send handle in the future
    # we should just pickle the channel, unpickle it, and then
    # build a new send handle from it. This is needed to correctly
    # set the default allocation pool and the send handle gateway
    # channel should the channel be non-local on the node where it
    # was unpickled. But ideally, there shouldn't be a reason to
    # pickle a send handle.
    def __getstate__(self):
        raise NotImplementedError("Not implemented for send handles.")

    def __setstate__(self, state):
        raise NotImplementedError("Not implemented for send handles.")


    def open(self) -> None:
        cdef:
            dragonError_t serr

        serr = dragon_chsend_open(&self._sendh)
        if serr != DRAGON_SUCCESS:
            raise ChannelError("Could not open send handle", serr)

    def close(self) -> None:
        cdef:
            dragonError_t serr

        serr = dragon_chsend_close(&self._sendh)

        if serr == DRAGON_CHANNEL_SEND_NOT_OPENED:
            raise ChannelHandleNotOpenError("Cannot close handle that is not open", serr)

        if serr != DRAGON_SUCCESS:
            raise ChannelError("Could not close send handle", serr)

    def send(self, msg: Message, dest_msg: Message=None, ownership=copy_on_send, blocking=True, timeout=ChannelSendH.USE_CHANNEL_SENDH_DEFAULT) -> None:
        ''' Send a message through the channel. The default behavior is to use the blocking and timeout
            behavior from the send channel handle. Otherwise, the two parameters can be set as follows.

            blocking == True and timeout == None means to block forever
            blocking == True and timeout > 0 means to block for timeout seconds
            blocking == True and timeout == 0 is a non-blocking send
            blocking == False is a non-blocking send no matter what timeout is
            Raises an exception if timeout < 0 no matter what blocking is.
        '''
        cdef:
            dragonError_t serr
            timespec_t * time_ptr
            timespec_t val_timeout
            timespec_t default_val

        if not blocking:
            # If non-blocking then timeout is set to 0
            time_ptr = &val_timeout
            val_timeout.tv_sec = 0
            val_timeout.tv_nsec = 0
        elif timeout == ChannelSendH.USE_CHANNEL_SENDH_DEFAULT:
            time_ptr = NULL
        else:
            default_val = DRAGON_CHANNEL_BLOCKING_NOTIMEOUT
            time_ptr = _compute_timeout(timeout, &default_val, &val_timeout)

        if ownership == transfer_ownership_on_send:
            with nogil:
                serr = dragon_chsend_send_msg(&self._sendh, &msg._msg, DRAGON_CHANNEL_SEND_TRANSFER_OWNERSHIP, time_ptr)

        elif dest_msg is None:
            with nogil:
                serr = dragon_chsend_send_msg(&self._sendh, &msg._msg, NULL, time_ptr)

        else:
            with nogil:
                serr = dragon_chsend_send_msg(&self._sendh, &msg._msg, dest_msg._mem, time_ptr)

        if serr != DRAGON_SUCCESS:
            if serr == DRAGON_CHANNEL_SEND_NOT_OPENED:
                raise ChannelHandleNotOpenError("Channel handle is not open", serr)

            if serr == DRAGON_TIMEOUT:
                raise ChannelSendTimeout("Timeout on send", serr)

            if serr == DRAGON_CHANNEL_FULL:
                raise ChannelFull("Channel is full", serr)

            if serr != DRAGON_SUCCESS:
                raise ChannelSendError("Could not send message", serr)


    def send_bytes(self, const unsigned char[::1] msg_bytes, msg_len: int=0, blocking=True, timeout=ChannelSendH.USE_CHANNEL_SENDH_DEFAULT) -> None:
        cdef:
            Message msg
            dragonMemoryDescr_t new_descr
            dragonError_t derr

        if msg_len < 1:
            msg_len = len(msg_bytes) # If len is unspecified or invalid, get length of bytes based on what was passed in

        msg = Message.create_alloc(self._channel.default_alloc_pool, msg_len)
        mview = msg.bytes_memview()
        cdef char[::1] dest_mem = mview
        memcpy(&dest_mem[0], &msg_bytes[0], msg_len)
        self.send(msg, ownership=transfer_ownership_on_send, timeout=timeout, blocking=blocking)

cdef class ChannelRecvH:
    """
    Receiving handle for incoming channel messages
    """

    USE_CHANNEL_RECVH_DEFAULT = 'USE_CHANNEL_RECVH_DEFAULT'

    cdef:
        dragonChannelRecvh_t _recvh
        dragonChannelDescr_t _ch


    def __init__(self, Channel ch, notification_type=dtypes.RECV_SYNC_MANUAL, signal=None, wait_mode=dtypes.DEFAULT_WAIT_MODE, timeout=None):
        # TODO:  Likely want to refactor open() functionality into here
        cdef:
            dragonError_t derr
            dragonChannelRecvAttr_t attrs
            timespec_t* time_ptr
            timespec_t default_val


        if not isinstance(notification_type, dtypes.RecvNotifType):
            raise ValueError('The notification Type must be a dragon.dtypes.RecvNotifType value.')

        if signal is not None and not isinstance(signal, int):
            raise ValueError('Signal attribute of receive handle must be an int.')

        wait_mode = dparms.cast_wait_mode(wait_mode)

        derr = dragon_channel_recv_attr_init(&attrs)
        if derr != DRAGON_SUCCESS:
            raise ChannelError("Could not initialize default recv attributes.", derr)

        attrs.default_notif_type = notification_type.value
        if signal is not None:
            attrs.signal = signal
        attrs.wait_mode = wait_mode.value
        default_val = DRAGON_CHANNEL_BLOCKING_NOTIMEOUT
        _compute_timeout(timeout, &default_val, &attrs.default_timeout)

        derr = dragon_channel_recvh(&ch._channel, &self._recvh, &attrs)
        if derr != DRAGON_SUCCESS:
            raise ChannelError("Could not create channel receive handle", derr)

        self._ch._idx = ch._channel._idx  # Preserves consistency with __setstate__

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    # Should we find a reason to pickle a recv handle in the future
    # we should just pickle the channel, unpickle it, and then
    # build a new recv handle from it. This is needed to correctly
    # set the default allocation pool and the recv handle gateway
    # channel should the channel be non-local on the node where it
    # was unpickled. But ideally, there shouldn't be a reason to
    # pickle a recv handle.
    def __getstate__(self):
        raise NotImplementedError("Not implemented for receive handles.")

    def __setstate__(self, state):
        raise NotImplementedError("Not implemented for receive handles.")


    def open(self) -> None:
        cdef:
            dragonError_t rerr

        rerr = dragon_chrecv_open(&self._recvh)
        if rerr != DRAGON_SUCCESS:
            raise ChannelError("Could not open receive handle", rerr)

    def close(self):
        cdef:
            dragonError_t rerr

        rerr = dragon_chrecv_close(&self._recvh)

        if rerr == DRAGON_CHANNEL_RECV_NOT_OPENED:
            raise ChannelHandleNotOpenError("Cannot close handle that is not open", rerr)

        if rerr != DRAGON_SUCCESS:
            raise ChannelError("Could not close receive handle", rerr)

    def recv(self, dest_msg: Message=None, blocking=True, timeout=ChannelRecvH.USE_CHANNEL_RECVH_DEFAULT):
        '''
            Receive a message from the channel. The default behavior is to use the blocking and timeout
            behavior from the receive channel handle. Otherwise, the two parameters can be set as follows.

            blocking == True and timeout == None means to block forever
            blocking == True and timeout > 0 means to block for timeout seconds
            blocking == True and timeout == 0 is a non-blocking receive
            blocking == False is a non-blocking receive no matter what timeout is
            Raises an exception if timeout < 0 no matter what blocking is.
        '''

        cdef:
            dragonError_t rerr
            Message msg
            timespec_t * time_ptr
            timespec_t val_timeout
            timespec_t default_val


        if not blocking:
            # If non-blocking then timeout is set to 0
            time_ptr = & val_timeout
            val_timeout.tv_sec = 0
            val_timeout.tv_nsec = 0
        elif timeout == ChannelRecvH.USE_CHANNEL_RECVH_DEFAULT:
            time_ptr = NULL
        elif timeout == 0:
            default_val = DRAGON_CHANNEL_TRYONCE_TIMEOUT
            time_ptr = &default_val
        else:
            default_val = DRAGON_CHANNEL_BLOCKING_NOTIMEOUT
            time_ptr = _compute_timeout(timeout, &default_val, &val_timeout)

        if dest_msg is None:
            dest_msg = Message.create_empty()

        with nogil:
            rerr = dragon_chrecv_get_msg_blocking(&self._recvh, &dest_msg._msg, time_ptr)

        if rerr != DRAGON_SUCCESS:
            if rerr == DRAGON_OBJECT_DESTROYED:
                # this indicate the channel was destroyed but likely because
                # other end destroyed it.
                raise ChannelDestroyed('Channel was destroyed')

            if rerr == DRAGON_CHANNEL_RECV_NOT_OPENED:
                raise ChannelHandleNotOpenError("Cannot receive with handle that is not open", rerr)

            if rerr == DRAGON_TIMEOUT:
                raise ChannelRecvTimeout("Timeout on receive", rerr)

            if rerr == DRAGON_CHANNEL_EMPTY:
                raise ChannelEmpty("Channel Empty", rerr)

            if rerr != DRAGON_SUCCESS:
                raise ChannelRecvError("Could not get message from channel", rerr)

        return dest_msg


    def recv_bytes(self, blocking=True, timeout=ChannelRecvH.USE_CHANNEL_RECVH_DEFAULT):
        cdef Message msg
        msg = self.recv(blocking=blocking, timeout=timeout)
        py_view = msg.tobytes()
        msg.destroy()

        return py_view

class PollResult:
    def __init__(self, value=None, rc=None):
        self._value = value # just refers to val, no copy
        self._rc = rc

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, val):
        self._value = val

    @property
    def rc(self):
        return self._rc

    @rc.setter
    def rc(self, val):
        self._rc = val

    def __str__(self):
        return f'PollResult({self._value},{self._rc})'

    def __repr__(self):
        return str(self)

cdef class Channel:
    """
    Cython wrapper for channels
    """

    def __cinit__(self):
        self._is_serialized = C_FALSE

    def __getstate__(self):
        return (self.serialize(), self._default_pool.serialize())

    def __setstate__(self, state):
        (serialized_bytes, default_pool_serialized_bytes) = state
        mp = MemoryPool.attach(default_pool_serialized_bytes)
        if not mp.is_local:
            # We have no choice but to pick a different pool as
            # the default pool for the channel.
            # since channel was pickled and sent to another node.
            # The _attach code has logic to look at the channel's
            # pool first, then pick the default if that pool is
            # also not local. We'll let the _attach logic
            # make the choice.
            mp = None
        self._attach(serialized_bytes, mp)


    def __del__(self):
        # TODO: Proper error handling for this?
        if self._is_serialized:
            self._is_serialized = False
            dragon_channel_serial_free(&self._serial)


    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._creator == C_TRUE:
            self.destroy()
        else:
            self.detach()

    def _attach(self, serialized_bytes, mem_pool):

        cdef:
            dragonError_t derr
            Channel ch
            dragonChannelSerial_t _serial


        self._is_remote = C_FALSE
        _serial.len = len(serialized_bytes)
        cdef const unsigned char[:] cdata = serialized_bytes
        _serial.data = <uint8_t *>&cdata[0]
        # Since the serialized descriptor was passed into this object, it did not call serialize,
        # so the _is_serialized is set to false so we don't try to free a resource we don't own
        # when the ch object is deleted.
        self._is_serialized = C_FALSE

        derr = dragon_channel_attach(&_serial, &self._channel)
        if derr != DRAGON_SUCCESS:
            raise ChannelError("Could not attach to Channel", derr)

        derr = dragon_channel_get_attr(&self._channel, &self._attr)
        if derr == DRAGON_CHANNEL_OPERATION_UNSUPPORTED_REMOTELY:
            self._is_remote = C_TRUE

        elif derr != DRAGON_SUCCESS:
            raise ChannelError("Could not get attributes from attached channel", derr)

        if mem_pool is None:
            mem_pool = self.get_pool()
            if mem_pool.is_local:
                self._default_pool = mem_pool
            else:
                # if no pool is provided the channel was not a local channel,
                # then we have to use the local default pool.
                pool = MemoryPool.attach(B64.from_str(dparms.this_process.default_pd).decode())
                self._default_pool = pool
        else:
            if not mem_pool.is_local:
                raise ChannelError("You cannot supply a pool that is not local.")

            self._default_pool = mem_pool

        self._creator = C_FALSE

        return self

    ######## BEGIN USER API #########

    def __init__(self, MemoryPool mem_pool, dragonC_UID_t c_uid, block_size=None, capacity=None, lock_type=None, fc_type=None, flags=None, def_alloc_pool=None, max_spinners=None, max_event_bcasts=None, bool semaphore=False, bool bounded_semaphore=False, int initial_sem_value=0):
        """
        Create a new Channel object, tied to the provided MemoryPool. The c_uid specifies a unique
        identifier name to share with other processes. For another process to use the channel, it needs to be given
        a serialized instance of this channel or it can inherit access to it through the current process using
        this object. Either semaphore or bounded_semaphore may be True, but not both.

        :param mem_pool: Dragon MemoryPool object the channel should use for allocating its resources.
        :param c_uid: A unique identifier for the channel
        :param block_size: The size of each message block. None indicates to use the default. A channel that is to be used to share small messages may see some performance improvement by choosing a corresponding small block size. Messages may be bigger than the given block size. If messages are bigger they are stored separately from the channel but still use a message block when in the channel.
        :param capacity: The number of messages the channel can hold. Optional, defaults to None.
        :param lock_type: The type of lock to be used in locking the channel. Optional, defaults to None
        :param fc_type: Unimplemented. Optional, defaults to None.
        :param flags: ChannelFlags to use. Optional, defaults to None.
        :param semaphore: A boolean value indicating whether this channel is to be used as a semaphore. If True, then capacity will be ignored.
        :param bounded_semaphore: True if this channel will be used as a bounded semephore. if True, capacity is ignored.
        :param initial_sem_value: The initial value of the semaphore. Only used when bounded_semaphore or semaphore is True.
        :return: A new Channel object using c_uid as its channel identifier.
        """

        cdef:
            dragonError_t derr


        self._is_remote = C_FALSE

        if semaphore and bounded_semaphore:
            raise ChannelError("Only one of semaphore and bounded_semaphore may be True.")

        derr = dragon_channel_attr_init(&self._attr)
        if derr != DRAGON_SUCCESS:
            raise ChannelError("Could not initialize Channel Attributes", derr)

        self._attr.semaphore = (semaphore or bounded_semaphore)
        self._attr.bounded = bounded_semaphore
        self._attr.initial_sem_value = initial_sem_value

        if not block_size is None:
            if not isinstance(block_size, int) or block_size <= 0:
                raise ChannelError("block_size must be an integer, greater than zero")
            self._attr.bytes_per_msg_block = block_size

        if not capacity is None:
            if not isinstance(capacity, int) or capacity <= 0:
                raise ChannelError("capacity must be an integer, greater than zero")
            self._attr.capacity = capacity

        if not lock_type is None:
            if not isinstance(lock_type, LockType):
                raise ChannelError("lock_type must be a value of type LockType")
            self._attr.lock_type = lock_type

        if not fc_type is None:
            if not isinstance(fc_type, FlowControl):
                raise ChannelError("fc_type must be a value of type FlowControl")
            self._attr.fc_type = fc_type

        if not flags is None:
            if not (isinstance(flags, int) or isinstance(flags, ChannelFlags)):
                raise ChannelError("flags must be a value of type int or ChannelFlags")
            self._attr.flags = int(flags)

        if not max_spinners is None:
            if not isinstance(max_spinners , int) or max_spinners < 0:
                raise ChannelError("max_spinners must be an integer, greater than or equal to zero")
            self._attr.max_spinners = max_spinners

        if not max_event_bcasts is None:
            if not isinstance(max_event_bcasts, int) or max_event_bcasts < 0:
                raise ChannelError("max_event_bcasts must be an integer, greater than or equal to zero")
            self._attr.max_event_bcasts = max_event_bcasts

        derr = dragon_channel_create(&self._channel, c_uid, &mem_pool._pool_hdl, &self._attr)

        if derr != DRAGON_SUCCESS:
            raise ChannelError("Could not create Channel", derr)

        if def_alloc_pool is not None:
            if def_alloc_pool.is_local:
                self._default_pool = def_alloc_pool
            else:
                raise ChannelError("You must use a local pool as the default allocation pool for a Channel.")
        else:
            self._default_pool = mem_pool

        self._creator = C_TRUE

    @classmethod
    def serialized_uid(cls, ch_ser):
        cdef:
            dragonError_t derr
            dragonULInt uid
            const unsigned char[:] cdata = ch_ser
            dragonChannelSerial_t _ser

        _ser.len = len(ch_ser)
        _ser.data = <uint8_t*>&cdata[0]
        derr = dragon_channel_get_uid(&_ser, &uid)
        if derr != DRAGON_SUCCESS:
            raise ChannelError("Error retrieving data from serialized channel", derr)

        return uid

    @classmethod
    def serialized_pool_uid_fname(cls, ch_ser):
        cdef:
            dragonError_t derr
            dragonULInt uid
            char * fname
            const unsigned char[:] cdata = ch_ser
            dragonChannelSerial_t _ser

        _ser.len = len(ch_ser)
        _ser.data = <uint8_t*>&cdata[0]
        derr = dragon_channel_pool_get_uid_fname(&_ser, &uid, &fname)
        if derr != DRAGON_SUCCESS:
            raise ChannelError("Error retrieving pool data from serialized channel", derr)

        pystring = fname[:].decode('utf-8')
        free(fname)

        return (uid, pystring)

    @classmethod
    def attach(cls, serialized_bytes, MemoryPool mem_pool=None):
        """
        Attach to an existing channel through a serialized channel descriptor. When attaching
        a non-local channel the memory pool for the channel will also be non-local. Since
        the send_bytes method of Channel needs to make memory allocations, a user may choose to
        supply their own memory pool when attaching a non-local channel. Nothing prevents you
        from doing this with a local channel either. If no memory pool is provided and the channel
        being attached is non-local, then the default memory pool on this node will be used instead.

        :param serialized_bytes: Bytes-like object containing a serialized channel descriptor to attach to.
        :param mem_pool: A memory pool to use for allocations on the attached channel, primarily for send_bytes.
        :return: New channel object attached to existing channel.
        :raises: ChannelError if channel could not be attached.
        """
        empty_channel = cls.__new__(cls)

        return empty_channel._attach(serialized_bytes, mem_pool)


    @classmethod
    def make_process_local(cls, MemoryPool pool=None, block_size=None, capacity=None, timeout=None):
        """
        Create a process local channel which is a channel that exists for the
        sole purpose of use by the current process. The channel is unique
        to all nodes and may be shared with other processes but is
        managed with the life-cycle of the current process. When the
        current process, the one calling this function, exits, the
        process local channel it created via this call will also be
        destroyed.

        This is especially useful for processes that need a channel to receive
        requests or need to have a place where responses to requests of
        other processes can be sent. Most likely calls to this function
        will exist inside of some other API.

        :param pool: The pool from which to allocate the channel. If None is
            specified, the default pool will be used on the node.

        :param block_size: The size of the channel blocks to be used in the channel. This
            is a control on how much space is reserved for each slot in the channel.

        :param capacity: The number of blocks to be allocated in the channel. Once the
            capacity is reached on send operations, further send operations will block
            given a non-zero timeout.

        :param timeout: Default is None which means to block without timeout until the
            channel is made. This should not timeout and should be processed quickly. If a
            timeout value is specified, it is the number of seconds to wait which may be a
            float.

        :return: A new channel object.

        :raises: ChannelError if there was an error. Note that the Dragon run-time
            must be running to use this function as it interacts with Local Services on the
            node on which it is called.
        """
        cdef:
            dragonError_t derr
            timespec_t * time_ptr
            timespec_t val_timeout
            dragonChannelDescr_t ch
            dragonChannelSerial_t ser
            uint64_t blockSize
            uint64_t numBlocks
            uint64_t muid

        if timeout is None:
            time_ptr = NULL
        elif isinstance(timeout, int) or isinstance(timeout, float):
            if timeout < 0:
                raise ValueError('Cannot provide timeout < 0 to make_process_local operation')
            # Anything > 0 means use that as seconds for timeout.
            time_ptr = & val_timeout
            val_timeout.tv_sec =  int(timeout)
            val_timeout.tv_nsec = int((timeout - val_timeout.tv_sec)*1000000000)
        else:
            raise ValueError('make_process_local timeout must be a float or int')

        if pool is None:
            pool = MemoryPool.attach_default()

        muid = pool.muid

        if block_size is None:
            blockSize = 0 # default will be used
        else:
            if not type(block_size) is int or block_size < 0:
                raise ChannelError("Block size must be a non-negative integer", DRAGON_INVALID_ARGUMENT)
            blockSize = block_size

        if capacity is None:
            numBlocks = 0 # default will be used
        else:
            if not type(capacity) is int or capacity < 0:
                raise ChannelError("Capacity must be a non-negative integer", DRAGON_INVALID_ARGUMENT)
            numBlocks = capacity

        with nogil:
            derr = dragon_create_process_local_channel(&ch, muid, blockSize, numBlocks, time_ptr)
        if derr != DRAGON_SUCCESS:
            raise ChannelError("Could not create process local channel", derr)

        derr = dragon_channel_serialize(&ch, &ser)
        if derr != DRAGON_SUCCESS:
            raise ChannelError("Could not serialize channel", derr)

        py_obj = ser.data[:ser.len]

        derr = dragon_channel_serial_free(&ser)

        # This inits the rest of the object given the channel descriptor above.
        return Channel.attach(py_obj)


    def destroy_process_local(self, timeout=None):
        """
        Destroy a process local channel which is a channel that exists for the
        sole purpose of use by the current process. The channel being destroyed
        should have been created by calling make_process_local.

        In certain circumstances process local channels may have temporary usefulness.
        When no longer needed, they can be destroyed by a process.

        :param timeout: Default is None which means to block without timeout until the
            channel is made. This should not timeout and should be processed quickly. If a
            timeout value is specified, it is the number of seconds to wait which may be a
            float.

        :return: None

        :raises: ChannelError if there was an error. Note that the Dragon run-time
            must be running to use this function as it interacts with Local Services on the
            node on which it is called.
        """
        cdef:
            dragonError_t derr
            timespec_t * time_ptr
            timespec_t val_timeout

        if timeout is None:
            time_ptr = NULL
        elif isinstance(timeout, int) or isinstance(timeout, float):
            if timeout < 0:
                raise ValueError('Cannot provide timeout < 0 to destroy_process_local operation')
            # Anything > 0 means use that as seconds for timeout.
            time_ptr = & val_timeout
            val_timeout.tv_sec =  int(timeout)
            val_timeout.tv_nsec = int((timeout - val_timeout.tv_sec)*1000000000)
        else:
            raise ValueError('destroy_process_local timeout must be a float or int')

        with nogil:
            derr = dragon_destroy_process_local_channel(&self._channel, time_ptr)
        if derr != DRAGON_SUCCESS:
            raise ChannelError("Could not destroy process local channel", derr)


    def destroy(self):
        """
        Destroys the channel.
        """
        cdef:
            dragonError_t derr

        derr = dragon_channel_destroy(&self._channel)
        if derr == DRAGON_OBJECT_DESTROYED:
            raise ChannelDestroyed('The channel does not exist. It was likely already destroyed or was garbage collected', derr)

        if derr != DRAGON_SUCCESS:
            raise ChannelError("Could not destroy Channel", derr)


    def detach(self, serialize=False):
        """
        Detach from a channel and its underlying memory.
        Optionally also detach from the underlying pool.
        """
        cdef:
            dragonError_t derr

        if serialize:
            # Don't need to use the returned python copy, just make sure we store the serialized data
            self.serialize()

        derr = dragon_channel_detach(&self._channel)
        if derr != DRAGON_SUCCESS:
            raise ChannelError("Could not detach channel", derr)


    def serialize(self):
        """
        Serialize a channel descriptor for use in storage or communication.
        Calling this will, if not already serialized, store the serialized data in self._serial.
        Subsequent calls will return a copy of that data as a python bytes object.
        This allows for holding onto the serialized data during detach for later use.

        :return: Memoryview of the serialized channel descriptor.
        """
        cdef:
            dragonError_t derr

        if not self._is_serialized:
            derr = dragon_channel_serialize(&self._channel, &self._serial)
            if derr != DRAGON_SUCCESS:
                raise ChannelError("Could not serialize channel", derr)

            self._is_serialized = C_TRUE

        py_obj = self._serial.data[:self._serial.len]
        return py_obj


    def sendh(self, return_mode=dtypes.WHEN_BUFFERED, wait_mode=dtypes.DEFAULT_WAIT_MODE, timeout=None):
        """
        Create a Send Handle object and return it unopened.

        :return: New ChannelSendH object.
        """

        return ChannelSendH(self, return_mode=return_mode, wait_mode=wait_mode, timeout=timeout)


    def recvh(self, notification_type=dtypes.RECV_SYNC_MANUAL, signal=None, wait_mode=dtypes.DEFAULT_WAIT_MODE, timeout=None):
        """
        Create a Receive Handle object and return it unopened.

        :return: New ChannelRecvH object.
        """

        return ChannelRecvH(self, notification_type=notification_type, signal=signal, wait_mode=wait_mode, timeout=timeout)


    def get_pool(self):
        """
        Retrieve the pool this Channel belongs to

        :return: MemoryPool object
        """
        cdef:
            dragonError_t derr
            MemoryPool mpool

        mpool = MemoryPool.empty_pool()
        derr = dragon_channel_get_pool(&self._channel, &mpool._pool_hdl)
        if derr != DRAGON_SUCCESS:
            raise ChannelError("Could not retrieve memory pool from channel", derr)

        return mpool

    def poll(self, timeout=None, wait_mode=dtypes.DEFAULT_WAIT_MODE, event_mask=EventType.POLLIN, poll_result=None):
        """Poll on a channel. If a PollResult is provided, the result and return code are provided
        via the PollResult. Otherwise it will raise an exception on an error condition.

        :param timeout: Timeout for the poll operation in seconds. Defaults to None..
        :type timeout: float
        :param wait_mode: How to poll on the channel, defaults to IDLE_WAIT.
        :type wait_mode: dtypes.WaitMode
        :param event_mask: Which event on the channel to poll on, defaults to POLLIN.
        :type event_mask: EventType
        :param poll_result: If there is a value returned by poll, it is set in this reference variable. If a
        PollResult value is provided, then poll will not raise an exception. It will instead return the result
        and the return code via this PollResult.
        :type poll_result: PollResult
        """

        cdef:
            dragonError_t err
            timespec_t * time_ptr
            timespec_t val_timeout
            dragonWaitMode_t wait
            dragonChannelEvent_t event
            dragonULInt result

        if timeout is None:
            time_ptr = NULL
        elif isinstance(timeout, int) or isinstance(timeout, float):
            if timeout < 0:
                raise ValueError('Cannot provide timeout < 0 to poll operation')
            # Anything > 0 means use that as seconds for timeout.
            time_ptr = & val_timeout
            val_timeout.tv_sec =  int(timeout)
            val_timeout.tv_nsec = int((timeout - val_timeout.tv_sec)*1000000000)
        else:
            raise ValueError('poll timeout must be a float or int')


        wait_mode = dparms.cast_wait_mode(wait_mode)

        # The event type value is checked in the C code so is not checked here.

        if event_mask <= 0:
            raise ValueError('event_mask must be a bitwise combination of POLLIN and POLLOUT or, individually, one of the other constants.')

        wait = wait_mode.value
        event = event_mask

        # Initialize it here but set below if successful poll
        if poll_result is not None:
            poll_result.value = 0
            poll_result.rc = DRAGON_TIMEOUT

        with nogil:
            err = dragon_channel_poll(&self._channel, wait, event, time_ptr, &result)

        if err == DRAGON_TIMEOUT or err == DRAGON_BARRIER_WAIT_TRY_AGAIN:
            # Getting try again here means that a 0 timeout was specified and
            # the operation did not work under that condition. It doesn't make
            # much sense to try once here, but if they did, we should treat it
            # like a timeout. The try once option and this return code is really
            # there for the transport agent to try off node poll operations so that's
            # why there are two separate return codes for timeout.
            return False

        if err == DRAGON_BARRIER_BROKEN:
            raise ChannelBarrierBroken('The Channel Barrier must be reset via a DRAGON_CHANNEL_POLLRESET poll operation.', err)

        if err == DRAGON_BARRIER_READY_TO_RELEASE:
            raise ChannelBarrierReady('The Channel Barrier is ready for the DRAGON_CHANNEL_POLLBARRIER_RELEASE poll operation. All waiters are present', err)

        if err == DRAGON_INVALID_VALUE:
            raise ChannelValueError("There was a problem with the value returned.", err)

        if err != DRAGON_SUCCESS:
            raise ChannelError("Unexpected Error while polling channel.", err)

        if poll_result is not None:
            poll_result.value = result
            poll_result.rc = err

        return err == DRAGON_SUCCESS


    @property
    def cuid(self):
        return self._channel._idx


    @property
    def capacity(self):
        if self._is_remote:
            raise ChannelRemoteOperationNotSupported('Cannot get capacity on remote channel.')

        return self._attr.capacity


    @property
    def block_size(self):
        if self._is_remote:
            raise ChannelRemoteOperationNotSupported('Cannot get block size on remote channel.')

        return self._attr.bytes_per_msg_block


    @property
    def fc_type(self):
        if self._is_remote:
            raise ChannelRemoteOperationNotSupported('Cannot get flow control on remote channel.')

        return FlowControl(self._attr.fc_type)


    @property
    def lock_type(self):
        if self._is_remote:
            raise ChannelRemoteOperationNotSupported('Cannot get lock type on remote channel.')

        return LockType(self._attr.lock_type)


    @property
    def num_msgs(self):
        result = PollResult()
        self.poll(event_mask=EventType.POLLSIZE, poll_result=result)
        if result.rc == DRAGON_TIMEOUT:
            raise ChannelTimeout('Timeout retrieving number of messages in channel.', result.rc)

        if result.rc != DRAGON_SUCCESS:
            raise ChannelError('There was an error in retreiving number of messages in channel.', result.rc)

        return result.value

    @property
    def blocked_receivers(self):
        cdef:
            dragonError_t derr
            uint64_t count

        with nogil:
            derr = dragon_channel_blocked_receivers(&self._channel, &count)
        if derr != DRAGON_SUCCESS:
            raise ChannelError("Error getting the blocked receivers", derr)

        return count

    @property
    def broken_barrier(self):
        cdef:
            bool result

        with nogil:
            result = dragon_channel_barrier_is_broken(&self._channel)
        return result

    @property
    def barrier_count(self):
        cdef:
            dragonError_t derr
            uint64_t count

        with nogil:
            derr = dragon_channel_barrier_waiters(&self._channel, &count)
        if derr != DRAGON_SUCCESS:
            raise ChannelError("Error getting the number of barrier waiters", derr)

        return count

    @property
    def default_alloc_pool(self):
        return self._default_pool

    @property
    def is_local(self):
        return dragon_channel_is_local(&self._channel)


cdef class ChannelSet:
    """Cython wrapper for ChannelSets

    Creating a ChannelSet makes it possible to poll across a set of channels.
    A list of channels must be provided to poll across.
    """

    cdef:
        dragonChannelSetDescr_t _channelset
        dragonChannelSetAttrs_t _attr
        list channel_list

    def __del__(self):
        dragon_channelset_destroy(&self._channelset)

    def __init__(self, MemoryPool mem_pool, channel_list, lock_type=None, num_allowed_spin_waiters=None, event_mask=EventType.POLLIN, capture_all_events=True):
        """Create a new ChannelSet object, tied to the provided MemoryPool. A channelset object cannot be used
        by other processes, as it is not serializable.

        :param mem_pool: Dragon MemoryPool object the channelset should use for allocating its resources
        :type mem_pool: MemoryPool
        :param channel_list: List of Channel objects to be included in the set
        :type channel_list: list[Channel]
        :param lock_type: The type of lock to be used in locking the channelset. Defaults to None
        :type lock_type: LockType, optional
        :param num_allowed_spin_waiters: Controls how many spin waiters a ChannelSet can support
        :type num_allowed_spin_waiters: int, optional
        :param event_mask: Which event on the channelset to poll on, defaults to POLLIN
        :type event_mask: EventType, optional
        :param capture_all_events: controls the sync_type (DRAGON_SYNC or DRAGON_NO_SYNC), defaults to True which is DRAGON_SYNC
        :type capture_all_events: boolean, optional
        :return: A new ChannelSet object
        :rtype: ChannelSet
        """
        cdef:
            dragonError_t derr
            int num_channels
            dragonChannelDescr_t** _channel_array # array of channel descriptors
            dragonChannelEvent_t event
            Channel ch

        self.channel_list = channel_list

        derr = dragon_channelset_attr_init(&self._attr)
        if derr != DRAGON_SUCCESS:
            raise ChannelSetError("Could not initialize Channel Set Attributes", derr)

        if not lock_type is None:
            if not isinstance(lock_type, LockType):
                raise ChannelSetError("lock_type must be a value of type LockType for the Channel Set")
            self._attr.lock_type = lock_type

        if not num_allowed_spin_waiters is None:
            if not isinstance(num_allowed_spin_waiters, int) or num_allowed_spin_waiters <= 0:
                raise ChannelSetError("num_allowed_spin_waiters must be an integer, greater than zero")
            self._attr.num_allowed_spin_waiters = num_allowed_spin_waiters

        if capture_all_events:
            self._attr.sync_type = DRAGON_SYNC
        else:
            self._attr.sync_type = DRAGON_NO_SYNC

        # The event type value is checked in the C code so is not checked here.
        event = event_mask

        num_channels = len(channel_list)
        _channel_array = <dragonChannelDescr_t**>malloc(num_channels * sizeof(dragonChannelDescr_t*))
        for i in range(num_channels):
            # could not do channel_list[i]._channel at the same time
            # and we needed to split it to two steps
            ch = channel_list[i]
            _channel_array[i] = &ch._channel

        with nogil:
            derr = dragon_channelset_create(_channel_array, num_channels, event, &mem_pool._pool_hdl, &self._attr, &self._channelset)
        with nogil:
            free(_channel_array)
        if derr != DRAGON_SUCCESS:
            raise ChannelSetError("Could not create Channel Set", derr)

    def poll(self, timeout=None, wait_mode=dtypes.IDLE_WAIT):
        """Poll on a channelset

        :param timeout: Timeout for the poll operation in seconds, defaults to None
        :type timeout: float, optional
        :param wait_mode: How to poll on the channel, defaults to IDLE_WAIT
        :type wait_mode: dtypes.WaitMode, optional
        :return: A tuple with the Channel object and return event
        :rtype: tuple
        """
        cdef:
            dragonError_t err
            timespec_t * time_ptr
            timespec_t val_timeout
            dragonWaitMode_t wait
            dragonChannelSetEventNotification_t * event
            int channel_idx
            short revent

        if timeout is None:
            time_ptr = NULL
        elif isinstance(timeout, int) or isinstance(timeout, float):
            if timeout < 0:
                raise ValueError('Cannot provide timeout < 0 to poll operation')

            # Anything > 0 means use that as seconds for timeout.
            time_ptr = & val_timeout
            val_timeout.tv_sec =  int(timeout)
            val_timeout.tv_nsec = int((timeout - val_timeout.tv_sec)*1000000000)
        else:
            raise ValueError('poll timeout must be a float or int')

        if wait_mode != dtypes.IDLE_WAIT and wait_mode != dtypes.SPIN_WAIT and wait_mode != dtypes.ADAPTIVE_WAIT:
            raise ValueError(f'wait_mode must be IDLE_WAIT, SPIN_WAIT, or ADAPTIVE_WAIT. It was {wait_mode}.')
        wait = wait_mode.value

        with nogil:
            err = dragon_channelset_poll(&self._channelset, wait, time_ptr, NULL, NULL, &event)
        if err == DRAGON_TIMEOUT:
            raise ChannelSetTimeout("Timeout while polling channelset.", err)
        if err != DRAGON_SUCCESS:
            raise ChannelSetError("Unexpected Error while polling channelset.", err)

        channel_idx = event.channel_idx
        revent = event.revent
        free(event)

        # return a tuple with the channel and revent
        return (self.channel_list[channel_idx], revent)


cpdef enum ChannelAdapterMsgTypes:
    EOT = 0,
    PICKLE_PROT_5,
    RAW_BYTES,


cdef:
    uint8_t _ConnMsgHeader_EOT_MSG = 0x20
    uint8_t _ConnMsgHeader_RAW_BYTE_MSG_FIRSTB = 0x40
    size_t _ConnMsgHeader_MAX_RAW_BYTE_LEN = 2 ** 56 - 1
    size_t _ConnMsgHeader_RAW_HDR_SZ = 8
    uint8_t _ConnMsgHeader_PROTOCOL_5_FIRSTB = 0x80
    uint8_t _ConnMsgHeader_PROTOCOL_5_NEXTB = 0x05


cdef _ConnMsgHeader_parse(uint8_t * first_bytes, size_t length):
    """Internal functions to help manage Connection message delimiting and in band signaling.

    The less fancy way of saying what this is doing is that while pickle streams
    know how long they are, we need to send something definite to know a sender
    is done with the underlying channel (like no more objects at all) and also
    need to delimit send_bytes and recv_bytes type usages where you just want
    to send a buffer.

    This supplies a bunch of constants and precalculated magic numbers
    and so on for the use of Peer2PeerReadingChannelFile and Peer2PeerWritingChannelFile.  The
    details have to agree on both sides, of course, which is why they
    all should be here in this class.

    Note that the blocking and delimiting this layer is worried about is actually
    completely distinct from what is going on at the Channels level - that is
    policy set thru the Peer2PeerReadingChannelFile and Peer2PeerWritingChannelFile objects directly.
    """
    cdef:
        size_t decoded_length
        size_t length_byte

    decoded_length = 0
    decoded_length = decoded_length << 8
    length_byte = first_bytes[7]
    decoded_length = decoded_length | length_byte
    decoded_length = decoded_length << 8
    length_byte = first_bytes[6]
    decoded_length = decoded_length | length_byte
    decoded_length = decoded_length << 8
    length_byte = first_bytes[5]
    decoded_length = decoded_length | length_byte
    decoded_length = decoded_length << 8
    length_byte = first_bytes[4]
    decoded_length = decoded_length | length_byte
    decoded_length = decoded_length << 8
    length_byte = first_bytes[3]
    decoded_length = decoded_length | length_byte
    decoded_length = decoded_length << 8
    length_byte = first_bytes[2]
    decoded_length = decoded_length | length_byte
    decoded_length = decoded_length << 8
    length_byte = first_bytes[1]
    decoded_length = decoded_length | length_byte

    if first_bytes[0] == _ConnMsgHeader_EOT_MSG:
        return ChannelAdapterMsgTypes.EOT, None
    elif first_bytes[0] == _ConnMsgHeader_PROTOCOL_5_FIRSTB and first_bytes[1] == _ConnMsgHeader_PROTOCOL_5_NEXTB:
        return ChannelAdapterMsgTypes.PICKLE_PROT_5, None
    elif first_bytes[0] == _ConnMsgHeader_RAW_BYTE_MSG_FIRSTB and length >= _ConnMsgHeader_RAW_HDR_SZ:
        # The following if statement is temporary code. It is needed to allow
        # C/C++ code to send byte encoded CapNProto messages to connections.
        # This code is only needed until we switch over to FLI adapters for
        # infrastructure connections. This is needed because the size of
        # the CapNProto message is not known until it is written.
        if decoded_length == 0xFFFFFFFFFFFFFF:
            decoded_length = length - _ConnMsgHeader_RAW_HDR_SZ

        return ChannelAdapterMsgTypes.RAW_BYTES, decoded_length
    else:
        raise ConnectionError(f'unexpected header: {first_bytes[:8]}')

cdef _ConnMsgHeader_make(size_t msglen, uint8_t * first_bytes):
    # room in the protocol to do different things if we want to be more
    # thrifty in sending lots of messages < 1K in size thru
    # send_bytes and recv_bytes. Quite possibly worth doing.
    # Overhead is minimum 2 bytes for msg < 256 bytes
    # while 4 bytes is good for messages up to 16MiB.
    # 3 byte overhead might oddly enough be a sweet spot
    # for the infrastructure especially if we go to better serialization
    # than json.
    # as implemented here it is 8 bytes.
    cdef:
        uint8_t length_byte
    assert msglen >= 0

    if msglen > _ConnMsgHeader_MAX_RAW_BYTE_LEN:
        raise ConnectionError(f'xfer size exceeds plausible limits of {_ConnMsgHeader_MAX_RAW_BYTE_LEN} bytes')

    first_bytes[0] = _ConnMsgHeader_RAW_BYTE_MSG_FIRSTB
    first_bytes[1] = msglen % 256
    msglen = msglen >> 8
    first_bytes[2] = msglen % 256
    msglen = msglen >> 8
    first_bytes[3] = msglen % 256
    msglen = msglen >> 8
    first_bytes[4] = msglen % 256
    msglen = msglen >> 8
    first_bytes[5] = msglen % 256
    msglen = msglen >> 8
    first_bytes[6] = msglen % 256
    msglen = msglen >> 8
    first_bytes[7] = msglen % 256
    msglen = msglen >> 8

cdef _ConnMsgHeader_raw_hdr_size(size_t msglen):
    assert msglen >= 0
    return _ConnMsgHeader_RAW_HDR_SZ

cdef _ConnMsgHeader_eot():
    return _ConnMsgHeader_EOT_MSG

cdef class Peer2PeerWritingChannelFile:
    """File-like interface for writing any size message efficiently into a Channel

    The basic flow can follow what is in Connection, but we need to follow more closely
    what was done in send_bytes above.  Essential bits are a static small message memory
    buffer we can bounce messages off of.  We will use a normal managed memory allocation
    so we can trim it to the right size and then wrap it into a message.  For large messages
    we will allocate blocks (leverage fast alloc path in MM) and use transfer of ownership.
    These two things combined will lower latency and then allow memcpy() to be used for more
    efficient copy BW.
    """

    # TODO, add docs and tighten things up a bit more. Otherwise looks to be working.


    cdef:
        dragonMemoryDescr_t _small_blk_descr
        dragonMemoryDescr_t _lrg_blk_descr
        dragonMemoryDescr_t _huge_blk_descr
        uint8_t * _current_block
        dragonChannelSendh_t _sendh
        dragonMemoryPoolDescr_t _pool_descr
        size_t _DEFAULT_SMALL_BLK_SIZE
        size_t _DEFAULT_LARGE_BLK_SIZE
        size_t _DEFAULT_HUGE_BLK_SIZE
        size_t _current_block_size
        size_t _cursor
        size_t _next_transmit_size
        timespec_t timer
        timespec_t * time_ptr
        dragonLock_t _tlock
        void * _tlock_mem
        int _opened
        object _options
        dragonWaitMode_t _wait_mode
        dragonChannelSendReturnWhen_t _return_mode

    def __cinit__(self):
        # These were reasonably tuned for P2P latency tests, but more block sizes could
        # be added with finer tuning in more complex situations.
        self._DEFAULT_SMALL_BLK_SIZE = 2 ** 10
        self._DEFAULT_LARGE_BLK_SIZE = 2 ** 17
        self._DEFAULT_HUGE_BLK_SIZE = 2 ** 21
        self._opened = 0

    # @KDL: FixME - the timeout below is arbitrary and is there to detect deadlock
    # for now. We will have better deadlock detection eventually. Wait for 5 minutes
    # for now.
    def __init__(self, Channel channel, *, options=None, timeout=300, wait_mode=dtypes.DEFAULT_WAIT_MODE, return_mode=dtypes.WHEN_BUFFERED):
        cdef:
            dragonError_t derr
            size_t sz
            dragonChannelSendAttr_t sattr

        self.time_ptr = _compute_timeout(timeout, NULL, &self.timer)

        self._options = {
                         "small_blk_size": self._DEFAULT_SMALL_BLK_SIZE,
                         "large_blk_size": self._DEFAULT_LARGE_BLK_SIZE,
                         "huge_blk_size": self._DEFAULT_HUGE_BLK_SIZE,
                         "buffer_pool": None
                        }

        if isinstance(options, dict):
            self._options.update(options)

        if self._options["large_blk_size"] <= self._options["small_blk_size"]:
            raise ValueError("Large block size is smaller than or equal to small block size")
        if self._options["huge_blk_size"] <= self._options["large_blk_size"]:
            raise ValueError("Huge block size is smaller than or equal to large block size")

        if self._options["buffer_pool"] is None:
            channel.get_pool_ptr(&self._pool_descr)
        elif isinstance(self._options["buffer_pool"], MemoryPool):
            mpool = <MemoryPool>self._options["buffer_pool"]
            derr = mpool.get_pool_ptr(&self._pool_descr)
            if derr != DRAGON_SUCCESS:
                raise ChannelError("Could not get Pool descriptor from provided buffer Pool", derr)
        else:
            raise ValueError("Optional buffer pool must be an instance of MemoryPool")

        wait_mode = dparms.cast_wait_mode(wait_mode)
        return_mode = dparms.cast_return_when_mode(return_mode)

        self._wait_mode = wait_mode.value
        self._return_mode = return_mode.value

        sz = dragon_lock_size(DRAGON_LOCK_FIFO)
        self._tlock_mem = <void *>calloc(sz, 1)
        if self._tlock_mem == NULL:
            raise ChannelError("Could not allocate memory for Dragon shared lock")
        derr = dragon_lock_init(&self._tlock, self._tlock_mem, DRAGON_LOCK_FIFO)
        if derr != DRAGON_SUCCESS:
            raise ChannelError("Could not instantiate Dragon shared lock", derr)

        derr = dragon_channel_send_attr_init(&sattr)
        if derr != DRAGON_SUCCESS:
            raise ChannelError('Could not initialize send handle attributes')

        sattr.wait_mode = self._wait_mode
        sattr.return_mode = self._return_mode

        derr = dragon_channel_sendh(&channel._channel, &self._sendh, &sattr)
        if derr != DRAGON_SUCCESS:
            raise ChannelError("Could not created channel send handle", derr)

        derr = dragon_channel_send_attr_destroy(&sattr)
        if derr != DRAGON_SUCCESS:
            raise ChannelError('Could not destroy send handle attributes')

        derr = dragon_chsend_open(&self._sendh)
        if derr != DRAGON_SUCCESS:
            raise ChannelError("Could not open send handle", derr)

    def __del__(self):
        cdef:
            dragonError_t derr

        derr = dragon_chsend_close(&self._sendh)

        if <void *>self._tlock_mem != NULL:
            free(self._tlock_mem)

    def open(self):
        cdef:
            dragonError_t derr
            size_t sz

        with nogil:
            derr = dragon_lock(&self._tlock)
        if derr != DRAGON_SUCCESS:
            raise ChannelError("Could not lock the adapter", derr)

        # Coercion not allowed without the gil
        sz = <size_t>self._options["small_blk_size"]

        with nogil:
            derr = dragon_memory_alloc_blocking(&self._small_blk_descr, &self._pool_descr, sz, self.time_ptr)
        if derr != DRAGON_SUCCESS:
            dragon_unlock(&self._tlock)
            raise ChannelError("Could not allocate small message send buffer", derr)

        self._current_block = NULL
        self._current_block_size = 0
        self._cursor = 0
        self._next_transmit_size = 0
        self._opened = 1

    cdef _send_current_block(self):
        cdef:
            dragonMemoryDescr_t new_descr
            dragonMessage_t msg
            dragonError_t derr
            timespec_t val_timeout
            size_t length

        val_timeout = DRAGON_CHANNEL_BLOCKING_NOTIMEOUT

        if self._cursor <= self._options["small_blk_size"]:
            length = self._cursor
            derr = dragon_memory_descr_clone(&new_descr, &self._small_blk_descr, 0, &self._cursor)
            if derr != DRAGON_SUCCESS:
                raise ChannelSendError("Could not create small message descriptor", derr)

        elif self._cursor <= self._options["large_blk_size"]:
            length = self._cursor
            derr = dragon_memory_descr_clone(&new_descr, &self._lrg_blk_descr, 0, &self._cursor)
            if derr != DRAGON_SUCCESS:
                raise ChannelSendError("Could not create large message descriptor", derr)

        else:
            length = self._cursor
            derr = dragon_memory_descr_clone(&new_descr, &self._huge_blk_descr, 0, &self._cursor)
            if derr != DRAGON_SUCCESS:
                raise ChannelSendError("Could not create huge message descriptor", derr)

        derr = dragon_channel_message_init(&msg, &new_descr, NULL)
        if derr != DRAGON_SUCCESS:
            raise ChannelError("Could not create message from memory descriptor", derr)

        if self._cursor <= self._options["small_blk_size"]:
            with nogil:
                derr = dragon_chsend_send_msg(&self._sendh, &msg, NULL, &val_timeout)
        else:
            with nogil:
                derr = dragon_chsend_send_msg(&self._sendh, &msg, DRAGON_CHANNEL_SEND_TRANSFER_OWNERSHIP, &val_timeout)

        if derr != DRAGON_SUCCESS:
            raise ChannelSendError("Could not send message", derr)

        derr = dragon_channel_message_destroy(&msg, 0)
        if derr != DRAGON_SUCCESS:
            raise ChannelError("Could not destroy message", derr)

        self._next_transmit_size -= self._cursor
        self._current_block = NULL
        self._current_block_size = 0
        self._cursor = 0

    cdef _get_new_block(self):
        cdef:
            dragonError_t derr
            timespec_t alloc_timeout
            size_t sz

        if self._next_transmit_size <= self._options["small_blk_size"]:
            derr = dragon_memory_get_pointer(&self._small_blk_descr, <void **>&self._current_block)
            if derr != DRAGON_SUCCESS:
                raise ChannelError("Could not generate pointer from small payload block", derr)
            self._current_block_size = self._options["small_blk_size"]
        elif self._cursor <= self._options["large_blk_size"]:

            # Coercion/indexing Python object not allowed without the gil
            sz = self._options["large_blk_size"]

            with nogil:
                derr = dragon_memory_alloc_blocking(&self._lrg_blk_descr, &self._pool_descr, sz, self.time_ptr)
            if derr != DRAGON_SUCCESS:
                raise ChannelError("Could not allocate large message send buffer", derr)
            derr = dragon_memory_get_pointer(&self._lrg_blk_descr, <void **>&self._current_block)
            if derr != DRAGON_SUCCESS:
                raise ChannelError("Could not generate pointer from large payload block", derr)
            self._current_block_size = self._options["large_blk_size"]
        else:

            # Coercion/indexing Python object not allowed without the gil
            sz = self._options["huge_blk_size"]

            with nogil:
                derr = dragon_memory_alloc_blocking(&self._huge_blk_descr, &self._pool_descr, sz, self.time_ptr)
            if derr != DRAGON_SUCCESS:
                raise ChannelError("Could not allocate huge message send buffer", derr)
            derr = dragon_memory_get_pointer(&self._huge_blk_descr, <void **>&self._current_block)
            if derr != DRAGON_SUCCESS:
                raise ChannelError("Could not generate pointer from huge payload block", derr)
            self._current_block_size = self._options["huge_blk_size"]

    def write_raw_header(self, size_t msglen):
        cdef size_t hdr_size = _ConnMsgHeader_raw_hdr_size(msglen)
        cdef uint8_t * header = <uint8_t *>malloc(hdr_size)
        _ConnMsgHeader_make(msglen, header)
        self._write(header, hdr_size)
        free(header)

    def write_eot(self):
        cdef uint8_t eot = _ConnMsgHeader_eot()
        self.write(&eot)

    def write(self, buf):
        cdef:
            size_t length
            const unsigned char[:] sbuf

        # for large Numpy/SciPy objects
        if isinstance(buf, pickle.PickleBuffer):
            sbuf = buf.raw()
        else:
            sbuf = buf

        length = len(sbuf)
        if length == 0:
            self._write(NULL, length)
        else:
            self._write(<uint8_t *>&sbuf[0], length)

    cdef _write(self, uint8_t * buf, size_t length):
        cdef:
            size_t remaining
            size_t buf_cursor
            size_t to_write

        if self._opened != 1:
            raise ChannelError("Adapter not opened for writing")

        if length == 0:
            return

        remaining = length

        # if this new message will spill into a new block, let's cut a new block
        # with potentially a more optimal size
        if <void *>self._current_block != NULL:
            if (self._next_transmit_size + remaining) > self._current_block_size:
                self._send_current_block()
                self._next_transmit_size = 0

        self._next_transmit_size += remaining
        buf_cursor = 0
        while remaining > 0:
            if <void *>self._current_block == NULL:
                self._get_new_block()

            to_write = min(remaining, self._current_block_size - self._cursor)
            memcpy(<void *>&self._current_block[self._cursor], <void *>&buf[buf_cursor], to_write)
            remaining -= to_write
            buf_cursor += to_write
            self._cursor += to_write

            if self._cursor == self._current_block_size:
                self._send_current_block()

    def flush(self):
        if self._opened != 1:
            raise ChannelError("Adapter not opened for writing")

        if <void *>self._current_block != NULL:
            self._send_current_block()

    def close(self):
        cdef:
            dragonError_t derr

        if self._opened == 0:
            return

        try:
            self.flush()

            derr = dragon_memory_free(&self._small_blk_descr)
            if derr != DRAGON_SUCCESS:
                raise ChannelError("Could not free small message send buffer", derr)

        finally:
            self._opened = 0
            with nogil:
                derr = dragon_unlock(&self._tlock)
            if derr != DRAGON_SUCCESS:
                raise ChannelError("Could not unlock the adapter", derr)

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_inst, exc_tb):
        self.close()


cdef class Peer2PeerReadingChannelFile:
    """File-like interface for reading any size message efficiently from a Channel.

    This is the dual end to the Peer2PeerWritingChannelFile class above.

    This interface is NOT thread safe.  If multiple threads will drive this, each thread must
    have its own object.
    """

    cdef:
        dragonMessage_t _current_message
        dragonChannelRecvh_t _recvh
        uint8_t * _current_block
        size_t _current_block_size
        size_t _cursor
        object _options
        object _side_buf
        dragonLock_t _tlock
        void * _tlock_mem
        int _opened
        dragonWaitMode_t _wait_mode

    def __init__(self, Channel channel, *, options=None, wait_mode=dtypes.DEFAULT_WAIT_MODE):
        """This object sits read calls on top of a channel object.

        :param channel: a Channel object
        :param options: an options object, tbd.  Strategy on taking blocks from the pool, etc.
        """
        cdef:
            dragonError_t derr
            dragonChannelRecvAttr_t rattr

        self._options = options
        self._current_block = NULL
        self._current_block_size = 0
        self._cursor = 0
        self._side_buf = None  # Ugly hack to allow recv_bytes of something that was sent.

        sz = dragon_lock_size(DRAGON_LOCK_FIFO)
        self._tlock_mem = <void *>calloc(sz, 1)
        if self._tlock_mem == NULL:
            raise ChannelError("Could not allocate memory for Dragon shared lock")
        derr = dragon_lock_init(&self._tlock, self._tlock_mem, DRAGON_LOCK_FIFO)
        if derr != DRAGON_SUCCESS:
            raise ChannelError("Could not instantiate Dragon shared lock", derr)

        self._wait_mode = dparms.cast_wait_mode(wait_mode).value

        derr = dragon_channel_recv_attr_init(&rattr)
        if derr != DRAGON_SUCCESS:
            raise ChannelError('Could not initialize send handle attributes')

        rattr.wait_mode = self._wait_mode

        derr = dragon_channel_recvh(&channel._channel, &self._recvh, &rattr)
        if derr != DRAGON_SUCCESS:
            raise ChannelError("Could not create channel receive handle", derr)

        derr = dragon_channel_recv_attr_destroy(&rattr)
        if derr != DRAGON_SUCCESS:
            raise ChannelError('Could not destroy receive handle attributes')

        derr = dragon_chrecv_open(&self._recvh)
        if derr != DRAGON_SUCCESS:
            raise ChannelError("Could not open receive handle", derr)

        self._opened = 0

    def __del__(self):
        cdef:
            dragonError_t derr

        derr = dragon_chrecv_close(&self._recvh)

        if <void *>self._tlock_mem != NULL:
            free(self._tlock_mem)

    def open(self):
        cdef:
            dragonError_t derr

        with nogil:
            derr = dragon_lock(&self._tlock)
        if derr != DRAGON_SUCCESS:
            raise ChannelError("Could not lock the adapter", derr)

        self._opened = 1
        self._cursor = 0

    def set_side_buf(self, buf):
        assert isinstance(buf, bytearray)
        self._side_buf = buf

    cdef _destroy_current_block(self):
        cdef:
            dragonError_t derr
            bint ifree = True

        if <void *>self._current_block != NULL:
            derr = dragon_channel_message_destroy(&self._current_message, ifree)
            if derr != DRAGON_SUCCESS:
                raise ChannelError("Could not destroy message", derr)

        self._current_block = NULL
        self._current_block_size = 0
        self._cursor = 0

    cdef _get_new_block(self):
        cdef:
            dragonError_t derr
            timespec_t val_timeout
            dragonMemoryDescr_t mem_descr

        val_timeout = DRAGON_CHANNEL_BLOCKING_NOTIMEOUT

        derr = dragon_channel_message_init(&self._current_message, NULL, NULL)
        if derr != DRAGON_SUCCESS:
            raise ChannelError("Could not create empty message", derr)

        with nogil:
            derr = dragon_chrecv_get_msg_blocking(&self._recvh, &self._current_message, &val_timeout)


        if derr == DRAGON_OBJECT_DESTROYED:
            # this indicate the channel was destroyed but likely because
            # other end destroyed it.
            raise ChannelDestroyed('Channel was destroyed')

        if derr != DRAGON_SUCCESS:
            raise ChannelRecvError("Could not get message from channel", derr)

        derr = dragon_channel_message_get_mem(&self._current_message, &mem_descr)
        if derr != DRAGON_SUCCESS:
            raise ChannelError("Could not get memory descriptor from message", derr)

        derr = dragon_memory_get_pointer(&mem_descr, <void **>&self._current_block)
        if derr != DRAGON_SUCCESS:
            raise ChannelError("Could not get pointer from message memory descriptor", derr)

        derr = dragon_memory_get_size(&mem_descr, &self._current_block_size)
        if derr != DRAGON_SUCCESS:
            raise ChannelError("Could not get size from message memory descriptor", derr)

        self._cursor = 0

    def check_header(self):
        if self._opened != 1:
            raise ChannelError("Adapter not opened for reading")

        if <void *>self._current_block == NULL:
            self._get_new_block()

        return _ConnMsgHeader_parse(&self._current_block[self._cursor], self._current_block_size)

    def advance_raw_header(self, size_t msglen):
        self._cursor += _ConnMsgHeader_raw_hdr_size(msglen)
        assert self._cursor <= self._current_block_size

    def read(self, size=-1):
        # FIXME: in the applications we are using this for,
        # do we want the -1 default arg for size at all?
        # Value chosen to match with io objects
        if size is None or size <= 0:
            raise ValueError('invalid size for this adapter')

        return_buf = bytearray(size)
        self.readinto(return_buf, accumulate_sb=False)

        if self._side_buf is not None:
            self._side_buf.extend(return_buf)

        return return_buf

    def close(self):
        cdef:
            dragonError_t derr

        if self._opened == 0:
            return

        try:
            if <void *>self._current_block != NULL:
                self._destroy_current_block()

        finally:
            self._opened = 0
            with nogil:
                derr = dragon_unlock(&self._tlock)
            if derr != DRAGON_SUCCESS:
                raise ChannelError("Could not unlock the adapter", derr)

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_inst, exc_tb):
        self.close()

    def readinto(self, const unsigned char[:] buf, accumulate_sb=True):
        cdef:
            size_t remaining
            size_t buf_cursor
            size_t to_get

        if self._opened != 1:
            raise ChannelError("Adapter not opened for reading")

        size = len(buf)

        buf_cursor = 0
        remaining = size

        while remaining > 0:
            if <void *>self._current_block == NULL:
                self._get_new_block()

            to_get = min(remaining, self._current_block_size - self._cursor)
            memcpy(<void *>&buf[buf_cursor], <void *>&self._current_block[self._cursor], to_get)
            remaining -= to_get
            buf_cursor += to_get
            self._cursor += to_get

            if self._cursor == self._current_block_size:
                self._destroy_current_block()
                self._cursor = 0

        if self._side_buf is not None and accumulate_sb:
            self._side_buf.extend(buf)

        return size

    def readline(self):
        raise NotImplementedError('should not happen')


cdef class Many2ManyReadingChannelFile:
    """Following similar approach with Peer2PeerReadingChannelFile. This is used when
    there are multiple readers.
    """
    cdef:
        dragonMessage_t _current_message
        dragonChannelRecvh_t _recvh
        uint8_t * _cur
        size_t _cur_size, _cursor
        timespec_t * time_ptr
        timespec_t val_timeout, default_val
        dragonWaitMode_t _wait_mode
        dragonLock_t _tlock
        void * _tlock_mem
        bint _opened

    def __init__(self, Channel channel, wait_mode=dtypes.DEFAULT_WAIT_MODE):
        cdef:
            dragonError_t derr
            dragonChannelRecvAttr_t rattr

        self._wait_mode = dparms.cast_wait_mode(wait_mode).value

        sz = dragon_lock_size(DRAGON_LOCK_FIFO)
        self._tlock_mem = <void *>calloc(sz, 1)
        if self._tlock_mem == NULL:
            raise ChannelError("Could not allocate memory for Dragon shared lock")
        derr = dragon_lock_init(&self._tlock, self._tlock_mem, DRAGON_LOCK_FIFO)
        if derr != DRAGON_SUCCESS:
            raise ChannelError("Could not instantiate Dragon shared lock", derr)

        derr = dragon_channel_recv_attr_init(&rattr)
        if derr != DRAGON_SUCCESS:
            raise ChannelError('Could not initialize send handle attributes')

        rattr.wait_mode = self._wait_mode

        derr = dragon_channel_recvh(&channel._channel, &self._recvh, &rattr)
        if derr != DRAGON_SUCCESS:
            raise ChannelError("Could not create channel receive handle", derr)

        derr = dragon_channel_recv_attr_destroy(&rattr)
        if derr != DRAGON_SUCCESS:
            raise ChannelError('Could not destroy receive handle attributes')

        derr = dragon_chrecv_open(&self._recvh)
        if derr != DRAGON_SUCCESS:
            raise ChannelError("Could not open receive handle", derr)

        self._cursor = 0
        self._cur = NULL
        self._cur_size = 0
        self._opened = C_FALSE

    cdef _destroy_msg(self):
        cdef:
            dragonError_t derr
            bint ifree = True

        if <void *>self._cur != NULL:
            derr = dragon_channel_message_destroy(&self._current_message, ifree)
            if derr != DRAGON_SUCCESS:
                raise ChannelError("Could not destroy message", derr)

        self._cur = NULL
        self._cur_size = 0
        self._cursor = 0

    cdef _get_msg(self):
        cdef:
            dragonError_t derr
            dragonMemoryDescr_t mem_descr

        derr = dragon_channel_message_init(&self._current_message, NULL, NULL)
        if derr != DRAGON_SUCCESS:
            raise ChannelError("Could not create empty message", derr)

        with nogil:
            derr = dragon_chrecv_get_msg_blocking(&self._recvh, &self._current_message, self.time_ptr)
        if derr != DRAGON_SUCCESS:
            if derr == DRAGON_OBJECT_DESTROYED:
                # this indicate the channel was destroyed but likely because
                # other end destroyed it.
                raise ChannelDestroyed('Channel was destroyed')

            if derr == DRAGON_CHANNEL_RECV_NOT_OPENED:
                raise ChannelHandleNotOpenError("Cannot receive with handle that is not open", derr)

            if derr == DRAGON_TIMEOUT:
                raise ChannelRecvTimeout("Timeout on receive", derr)

            if derr == DRAGON_CHANNEL_EMPTY:
                raise ChannelEmpty("Channel Empty", derr)

            if derr != DRAGON_SUCCESS:
                raise ChannelRecvError("Could not get message from channel", derr)

        derr = dragon_channel_message_get_mem(&self._current_message, &mem_descr)
        if derr != DRAGON_SUCCESS:
            raise ChannelError("Could not get memory descriptor from message", derr)

        derr = dragon_memory_get_pointer(&mem_descr, <void **>&self._cur)
        if derr != DRAGON_SUCCESS:
            raise ChannelError("Could not get pointer from message memory descriptor", derr)

        derr = dragon_memory_get_size(&mem_descr, &self._cur_size)
        if derr != DRAGON_SUCCESS:
            raise ChannelError("Could not get size from message memory descriptor", derr)

        self._cursor = 0

    def get_remaining_data(self):
        """
        Allocate memory for storing the dict message with (key, value) information.
        This is used to extract the offset, where value is stored in the message.
        Currently this is used for the use case info of the dragon dict.

        :return: A tuple with new memory alloc object and start offset of the value
        """
        cdef:
            dragonError_t derr
            dragonMemoryDescr_t mem_descr
            int offset

        derr = dragon_channel_message_get_mem(&self._current_message, &mem_descr)
        if derr != DRAGON_SUCCESS:
            raise ChannelError("Could not get memory descriptor from message", derr)

        mem_alloc_obj = MemoryAlloc.cinit(mem_descr)
        if not isinstance(mem_alloc_obj, MemoryAlloc):
            raise ChannelError(mem_alloc_obj[0], mem_alloc_obj[1])

        # Store the offset where the value is started and set the cursor
        # to the end of the message (not a neccesity)
        offset = self._cursor
        self._cursor = self._cur_size

        # Avoid destroying the message so that dictionary can fetch the value
        self._cur = NULL

        return (mem_alloc_obj, offset)


    def open(self):
        cdef:
            dragonError_t derr

        with nogil:
            derr = dragon_lock(&self._tlock)
        if derr != DRAGON_SUCCESS:
            raise ChannelError("Could not lock the adapter", derr)

        self._opened = C_TRUE
        self._cursor = 0

    def close(self):
        cdef:
            dragonError_t derr

        if not self._opened:
            return

        try:
            if <void *>self._cur != NULL:
                self._destroy_msg()

        finally:
            self._opened = C_FALSE
            with nogil:
                derr = dragon_unlock(&self._tlock)
            if derr != DRAGON_SUCCESS:
                raise ChannelError("Could not unlock the Many2ManyReading adapter", derr)

    def __del__(self):
        cdef:
            dragonError_t derr

        derr = dragon_chrecv_close(&self._recvh)

        if <void *>self._tlock_mem != NULL:
            free(self._tlock_mem)

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def set_adapter_timeout(self, blocking=True, timeout=None):
        if not blocking:
            # If non-blocking then timeout is set to 0
            self.time_ptr = & self.val_timeout
            self.val_timeout.tv_sec = 0
            self.val_timeout.tv_nsec = 0
        elif timeout == ChannelRecvH.USE_CHANNEL_RECVH_DEFAULT:
            self.time_ptr = NULL
        else:
            self.default_val = DRAGON_CHANNEL_BLOCKING_NOTIMEOUT
            self.time_ptr = _compute_timeout(timeout, &self.default_val, &self.val_timeout)

    def read(self, size=-1):
        if size is None or size <= 0:
            raise ValueError('invalid size for this adapter')

        return_buf = bytearray(size)
        self.readinto(return_buf)
        return return_buf

    def readinto(self, const unsigned char[:] buf):
        cdef:
            size_t to_get

        if not self._opened:
            raise ChannelError("Many2Many adapter not opened for reading")

        size = len(buf)

        if <void *>self._cur == NULL:
            self._get_msg()
        to_get = min(<size_t>size, self._cur_size - self._cursor)
        memcpy(<void *>&buf[0], <void *>&self._cur[self._cursor], to_get)
        self._cursor += to_get

        return size

    def readline(self):
        raise NotImplementedError('should not happen')


cdef class Many2ManyWritingChannelFile:
    """Following similar approach as Peer2PeerWritingChannelFile, with the main difference
    that this includes a many to many scheme of communication. This means, we
    have to write a single message in the channel coming from each writer.
    """
    cdef:
        dragonChannelSendh_t _sendh
        dragonMemoryPoolDescr_t _pool_descr
        timespec_t * time_ptr
        timespec_t val_timeout, default_val, _deadline
        msg_blocks_t * head
        size_t _msg_size
        dragonWaitMode_t _wait_mode
        dragonChannelSendReturnWhen_t _return_mode
        dragonLock_t _tlock
        void * _tlock_mem
        bint _opened, _timeout_needs_updating

    def __init__(self, Channel channel, buffer_pool=None, wait_mode=dtypes.DEFAULT_WAIT_MODE, return_mode=dtypes.WHEN_BUFFERED):
        cdef:
            dragonError_t derr
            size_t sz
            dragonChannelSendAttr_t sattr

        self._wait_mode = dparms.cast_wait_mode(wait_mode).value
        self._return_mode = dparms.cast_return_when_mode(return_mode).value

        if buffer_pool is None:
            channel.get_pool_ptr(&self._pool_descr)
        elif isinstance(buffer_pool, MemoryPool):
            mpool = <MemoryPool>buffer_pool
            derr = mpool.get_pool_ptr(&self._pool_descr)
            if derr != DRAGON_SUCCESS:
                raise ChannelError("Could not get Pool descriptor from provided buffer Pool", derr)
        else:
            raise ValueError("Optional buffer pool must be an instance of MemoryPool")

        sz = dragon_lock_size(DRAGON_LOCK_FIFO)
        self._tlock_mem = <void *>calloc(sz, 1)
        if self._tlock_mem == NULL:
            raise ChannelError("Could not allocate memory for Dragon shared lock")
        derr = dragon_lock_init(&self._tlock, self._tlock_mem, DRAGON_LOCK_FIFO)
        if derr != DRAGON_SUCCESS:
            raise ChannelError("Could not instantiate Dragon shared lock", derr)

        derr = dragon_channel_send_attr_init(&sattr)
        if derr != DRAGON_SUCCESS:
            raise ChannelError('Could not initialize send handle attributes')

        sattr.wait_mode = self._wait_mode
        sattr.return_mode = self._return_mode

        derr = dragon_channel_sendh(&channel._channel, &self._sendh, &sattr)
        if derr != DRAGON_SUCCESS:
            raise ChannelError("Could not create channel send handle", derr)

        derr = dragon_channel_send_attr_destroy(&sattr)
        if derr != DRAGON_SUCCESS:
            raise ChannelError('Could not destroy send handle attributes')

        derr = dragon_chsend_open(&self._sendh)
        if derr != DRAGON_SUCCESS:
            raise ChannelError("Could not open send handle", derr)

        self._msg_size = 0
        self.head = NULL
        self._opened = C_FALSE

    def open(self):
        cdef:
            dragonError_t derr
            size_t sz

        with nogil:
            derr = dragon_lock(&self._tlock)
        if derr != DRAGON_SUCCESS:
            raise ChannelError("Could not lock the Many2ManyWriting adapter", derr)

        self._msg_size = 0
        self.head = NULL
        self._opened = C_TRUE

    def close(self):
        cdef:
            dragonError_t derr

        if not self._opened:
            return

        try:
            if self._msg_size:
                # if we have something to send, send it
                self._send_msg()
        finally:
            # Cleanup things
            self._free_linked_list_of_mallocs()
            self._opened = C_FALSE
            with nogil:
                derr = dragon_unlock(&self._tlock)
            if derr != DRAGON_SUCCESS:
                raise ChannelError("Could not unlock the adapter", derr)

    cdef _free_linked_list_of_mallocs(self):
        cdef:
            msg_blocks_t * next_node = NULL

        while self.head != NULL:
            next_node = self.head
            self.head = self.head._next
            free(next_node._current_block)
            free(next_node)

    def __del__(self):
        cdef:
            dragonError_t derr

        derr = dragon_chsend_close(&self._sendh)

        if <void *>self._tlock_mem != NULL:
            free(self._tlock_mem)

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    cdef _destroy_msg(self, dragonMessage_t msg):
        cdef:
            dragonError_t derr

        derr = dragon_channel_message_destroy(&msg, 0)
        if derr != DRAGON_SUCCESS:
            raise ChannelError("Could not destroy message", derr)
        self._msg_size = 0

    cdef _send_msg(self):
        cdef:
            dragonMemoryDescr_t mem_descr
            dragonMessage_t msg
            dragonError_t derr
            uint8_t * payload_ptr
            size_t _cursor
            msg_blocks_t * temp_node

        # TODO, this needs fixing to adjust for any elapsed time and the return codes invoking the right exception
        with nogil:
            derr = dragon_memory_alloc_blocking(&mem_descr, &self._pool_descr, self._msg_size, &DRAGON_MEMORY_TEMPORARY_TIMEOUT)
        if derr != DRAGON_SUCCESS:
            raise ChannelError("Could not allocate message send buffer", derr)

        derr = dragon_memory_get_pointer(&mem_descr, <void **>&payload_ptr)
        if derr != DRAGON_SUCCESS:
            raise ChannelError("Could not generate pointer from payload", derr)

        # Traverse the linked list of mallocs
        _cursor = self._msg_size
        temp_node = self.head
        while temp_node != NULL:
            to_write = temp_node._current_block_size
            _cursor -= to_write
            memcpy(<void *>&payload_ptr[_cursor], <void *>&temp_node._current_block[0], to_write)
            temp_node = temp_node._next

        derr = dragon_channel_message_init(&msg, &mem_descr, NULL)
        if derr != DRAGON_SUCCESS:
            raise ChannelError("Could not create message from memory descriptor", derr)

        with nogil:
            derr = dragon_chsend_send_msg(&self._sendh, &msg, DRAGON_CHANNEL_SEND_TRANSFER_OWNERSHIP, self.time_ptr)
        if derr != DRAGON_SUCCESS:
            if derr == DRAGON_CHANNEL_SEND_NOT_OPENED:
                self._destroy_msg(msg)
                raise ChannelHandleNotOpenError("Channel handle is not open", derr)

            if derr == DRAGON_TIMEOUT:
                self._destroy_msg(msg)
                raise ChannelSendTimeout("Timeout on send", derr)

            if derr == DRAGON_CHANNEL_FULL:
                self._destroy_msg(msg)
                raise ChannelFull("Channel is full", derr)

            if derr != DRAGON_SUCCESS:
                self._destroy_msg(msg)
                raise ChannelSendError("Could not send message", derr)

        self._destroy_msg(msg)

    cdef _next_malloc(self, uint8_t * buf, size_t sz):
        """Allocate memory of the size of pickle buffer, every time that the write
        method is called. Since we do not know how many times the write() will be
        called, we store the different mallocs in a linked list.
        The sum of these mallocs will be the size of the full final message which
        we'll send in the channel.
        """
        cdef:
            msg_blocks_t * new = NULL

        if self.head == NULL:
            self.head = <msg_blocks_t *>malloc(sizeof(msg_blocks_t))
            self.head._current_block_size = sz
            self.head._next = NULL
            self.head._current_block = <uint8_t *>malloc(sz)
            memcpy(<void *>&self.head._current_block[0], <void *>&buf[0], sz)
        else:
            # Add the new block in the beginning of the linked list
            new = <msg_blocks_t *>malloc(sizeof(msg_blocks_t))
            new._current_block = <uint8_t *>malloc(sz)
            memcpy(<void *>&new._current_block[0], <void *>&buf[0], sz)
            new._current_block_size = sz
            new._next = self.head
            self.head = new

        self._msg_size += sz

    def set_adapter_timeout(self, blocking=True, timeout=None):
        if not blocking:
            # If non-blocking then timeout is set to 0
            self.time_ptr = & self.val_timeout
            self.val_timeout.tv_sec = 0
            self.val_timeout.tv_nsec = 0
            self._timeout_needs_updating = C_FALSE
        elif timeout is None:
            # blocking is True and timeout is None
            self.default_val = DRAGON_CHANNEL_BLOCKING_NOTIMEOUT
            self.time_ptr = _compute_timeout(timeout, &self.default_val, &self.val_timeout)
            self._timeout_needs_updating = C_FALSE
        else:
            # blocking is True and we were given a value for timeout
            self.time_ptr = _compute_timeout(timeout, &self.default_val, &self.val_timeout)
            self._timeout_needs_updating = C_TRUE
            self._deadline.tv_sec = 0
            self._deadline.tv_nsec = 0

    cdef _adjust_timeout(self):
        cdef:
            timespec_t _remaining_time
            dragonError_t err

        if self._deadline.tv_sec == 0 and self._deadline.tv_nsec == 0:
            _remaining_time.tv_sec = 0
            _remaining_time.tv_nsec = 0

            err = dragon_timespec_deadline(self.time_ptr, &self._deadline)
            if err != DRAGON_SUCCESS:
                raise ChannelSendError("Could not send message", err)
        else:
            err = dragon_timespec_remaining(&self._deadline, &_remaining_time)
            if err == DRAGON_TIMEOUT:
                self._msg_size = 0 # makes sure in close() we will not send the so far payload
                raise ChannelSendTimeout("Timeout on send", err)
            elif err == DRAGON_SUCCESS:
                self.time_ptr = & _remaining_time
                err = dragon_timespec_deadline(self.time_ptr, &self._deadline)
                if err != DRAGON_SUCCESS:
                    raise ChannelSendError("Could not send message", err)
            else:
                raise ChannelSendError("Could not send message", err)

    def write(self, buf):
        cdef:
            size_t length
            const unsigned char[:] sbuf

        # adjust the timeout if needed
        if self._timeout_needs_updating:
            self._adjust_timeout()

        # for large Numpy/SciPy objects
        if isinstance(buf, pickle.PickleBuffer):
            sbuf = buf.raw()
        else:
            sbuf = buf

        length = len(sbuf)
        if length == 0:
            self._write(NULL, length)
        else:
            self._write(<uint8_t *>&sbuf[0], length)

    cdef _write(self, uint8_t * buf, size_t length):
        if not self._opened:
            raise ChannelError("Many2ManyWriting adapter not opened for writing")

        if length == 0:
            return

        self._next_malloc(buf, length)


cdef class GatewayMessage:
    """Cython wrapper for Dragon Channel Gateway Messages
    """
    cdef:
        dragonGatewayMessage_t _gmsg

    def __cinit__(self):
        pass

    def destroy(self):
        """
        Destroy the underlying message object for send operations
        """
        cdef:
            dragonError_t err

        err = dragon_channel_gatewaymessage_destroy(&self._gmsg)
        if err == DRAGON_MAP_KEY_NOT_FOUND:
            # Already destroyed, so just return
            return

        if err != DRAGON_SUCCESS:
            raise ChannelError("Could not destroy gateway message", err)

    @property
    def send_payload_message(self):
        cdef:
            dragonMemoryDescr_t mem_descr
            dragonError_t err
            char* ptr
            size_t nbytes

        err = dragon_channel_message_get_mem(&self._gmsg.send_payload_message, &mem_descr)
        if err != DRAGON_SUCCESS:
            raise ChannelError("Could not get memory descriptor from message", err)

        err = dragon_memory_get_pointer(&mem_descr, <void **>&ptr)
        if err != DRAGON_SUCCESS:
            raise ChannelError("Could not get pointer from memory descriptor", err)

        err = dragon_memory_get_size(&mem_descr, &nbytes)
        if err != DRAGON_SUCCESS:
            raise ChannelError("Could not get size memory descriptor", err)

        return PyMemoryView_FromMemory(ptr, nbytes, BUF_READ)

    @property
    def send_payload_message_attr_sendhid(self):
        if self.is_send_kind:
            return self._gmsg.send_payload_message._attr.sendhid[:16]
        else:
            raise ChannelError('Attempt to get the sendhid of a non-send Gateway message')

    @property
    def send_payload_message_attr_clientid(self):
        if self.is_send_kind:
            return self._gmsg.send_payload_message._attr.clientid
        else:
            raise ChannelError('Attempt to get the clientid of a non-send Gateway message')

    @property
    def send_payload_message_attr_hints(self):
        if self.is_send_kind:
            return self._gmsg.send_payload_message._attr.hints
        else:
            raise ChannelError('Attempt to get the hints of a non-send Gateway message')

    @property
    def send_dest_mem_descr_ser(self):
        """
        :return: Memoryview of the destination serialized memory descriptor for sends
        """
        if <void *>self._gmsg.send_dest_mem_descr_ser != NULL:
            return self._gmsg.send_dest_mem_descr_ser[0].data[:self._gmsg.send_dest_mem_descr_ser[0].len]

        return None # not necessary, but explains that's what we want.

    @property
    def get_dest_mem_descr_ser(self):
        """
        :return: Memoryview of the destination serialized memory descriptor for gets
        """
        if <void *>self._gmsg.get_dest_mem_descr_ser != NULL:
            return self._gmsg.get_dest_mem_descr_ser[0].data[:self._gmsg.get_dest_mem_descr_ser[0].len]

        return None # not necessary, but explains that's what we want.

    @property
    def deadline(self):
        if self._gmsg.deadline.tv_sec == DRAGON_CHANNEL_BLOCKING_NOTIMEOUT.tv_sec and self._gmsg.deadline.tv_nsec == DRAGON_CHANNEL_BLOCKING_NOTIMEOUT.tv_nsec:
            return math.inf
        return <float>((1000000000 * self._gmsg.deadline.tv_sec + self._gmsg.deadline.tv_nsec) / 1000000000)

    @property
    def event_mask(self):
        return self._gmsg.event_mask

    @property
    def is_send_return_immediately(self):
        return self.is_send_kind and self._gmsg.send_return_mode == DRAGON_CHANNEL_SEND_RETURN_IMMEDIATELY

    @property
    def is_send_return_when_buffered(self):
        return self.is_send_kind and self._gmsg.send_return_mode == DRAGON_CHANNEL_SEND_RETURN_WHEN_BUFFERED

    @property
    def is_send_return_when_deposited(self):
        return self.is_send_kind and self._gmsg.send_return_mode == DRAGON_CHANNEL_SEND_RETURN_WHEN_DEPOSITED

    @property
    def is_send_return_when_received(self):
        return self.is_send_kind and self._gmsg.send_return_mode == DRAGON_CHANNEL_SEND_RETURN_WHEN_RECEIVED

    @property
    def target_ch_ser(self):
        """
        :return: Memoryview of the serialized target channel descriptor
        """
        return self._gmsg.target_ch_ser.data[:self._gmsg.target_ch_ser.len]

    @property
    def target_hostid(self):
        return <unsigned long int>self._gmsg.target_hostid

    @property
    def is_send_kind(self):
        return self._gmsg.msg_kind == DRAGON_GATEWAY_MESSAGE_SEND

    @property
    def is_get_kind(self):
        return self._gmsg.msg_kind == DRAGON_GATEWAY_MESSAGE_GET

    @property
    def is_event_kind(self):
        return self._gmsg.msg_kind == DRAGON_GATEWAY_MESSAGE_EVENT

    @staticmethod
    cdef GatewayMessage _from_message(Message msg):
        """
        Receives a channel Message and returns a gateway message
        """
        cdef:
            dragonError_t derr
            dragonMemoryDescr_t mem
            dragonGatewayMessageSerial_t gmsg_ser
            uint8_t * ptr
            size_t nbytes

        derr = dragon_channel_message_get_mem(&msg._msg, &mem)
        if derr != DRAGON_SUCCESS:
            raise ChannelError("Could not get memory descriptor from message", derr)

        derr = dragon_memory_get_pointer(&mem, <void **>&ptr)
        if derr != DRAGON_SUCCESS:
            raise ChannelError("Could not generate pointer from payload", derr)

        derr = dragon_memory_get_size(&mem, &nbytes)
        if derr != DRAGON_SUCCESS:
            raise ChannelError("Could not get size memory descriptor", derr)

        gmsg_ser.data = ptr
        gmsg_ser.len = nbytes

        cdef GatewayMessage obj = GatewayMessage.__new__(GatewayMessage)

        derr = dragon_channel_gatewaymessage_attach(&gmsg_ser, &obj._gmsg)
        if derr != DRAGON_SUCCESS:
            raise ChannelError("Could not attach to GatewayMessage", derr)

        return obj

    @staticmethod
    def from_message(Message msg):
        return GatewayMessage._from_message(msg)

    def send_complete(self, int op_err=<int>DRAGON_SUCCESS):
        cdef:
            dragonError_t err

        if (
            self._gmsg.send_return_mode == DRAGON_CHANNEL_SEND_RETURN_IMMEDIATELY
            or self._gmsg.send_return_mode == DRAGON_CHANNEL_SEND_RETURN_WHEN_BUFFERED
            or self._gmsg.send_return_mode == DRAGON_CHANNEL_SEND_RETURN_WHEN_DEPOSITED
        ):
            with nogil:
                err = dragon_channel_gatewaymessage_transport_send_cmplt(&self._gmsg, <dragonError_t>op_err)
            if err != DRAGON_SUCCESS:
                raise ChannelError("Could not complete a send operation from a transport agent", err)

    def get_complete(self, Message msg_recv, int op_err=<int>DRAGON_SUCCESS):
        cdef:
            dragonError_t err
            dragonMessage_t * _msg_recv

        if msg_recv is None:
            _msg_recv = NULL
        else:
            _msg_recv = &msg_recv._msg

        with nogil:
            err = dragon_channel_gatewaymessage_transport_get_cmplt(&self._gmsg, _msg_recv, <dragonError_t>op_err)
        if err != DRAGON_SUCCESS:
            raise ChannelError("Could not complete a get operation from a transport agent", err)

    def event_complete(self, int poll_result, int op_err=<int>DRAGON_SUCCESS):
        cdef:
            dragonError_t err

        with nogil:
            err = dragon_channel_gatewaymessage_transport_event_cmplt(&self._gmsg, poll_result, <dragonError_t>op_err)
        if err != DRAGON_SUCCESS:
            raise ChannelError("Could not complete an event operation from a transport agent", err)

    def complete_error(self, int op_err):
        assert op_err != <int>DRAGON_SUCCESS
        if self.is_send_kind:
            return self.send_complete(op_err)
        if self.is_get_kind:
            return self.get_complete(None, op_err)
        if self.is_event_kind:
            # Note: The specified event mask is ignored
            return self.event_complete(0, op_err)
        raise ValueError('Unsupported kind of message')

    @staticmethod
    def silence_transport_timeouts():
        dragon_gatewaymessage_silence_timeouts()
