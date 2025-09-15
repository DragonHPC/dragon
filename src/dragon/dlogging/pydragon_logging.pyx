from dragon.dtypes_inc cimport *
from dragon.managed_memory cimport *

from libc.string cimport strcpy, strlen
from libcpp cimport bool

import enum
import logging
import random

from dragon.channels import ChannelEmpty
import dragon.infrastructure.facts as dfacts

cdef enum:
    C_TRUE = 1
    C_FALSE = 0

# Translate python logging levels to dragon typedefs
_toDragonLogLevel = {
    logging.CRITICAL : DG_CRITICAL,
    logging.FATAL : DG_CRITICAL,
    logging.ERROR : DG_ERROR,
    logging.WARNING : DG_WARNING,
    logging.INFO : DG_INFO,
    logging.DEBUG : DG_DEBUG,
    logging.NOTSET : DG_DEBUG,
}



cdef timespec_t* _compute_timeout(timeout, timespec_t* default_val_ptr, timespec_t* time_value):

    cdef:
        timespec_t* time_ptr

    if timeout is None:
        time_ptr = default_val_ptr
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


class DragonLoggingError(Exception):

    def __init__(self, lib_err, msg):
        cdef char * errstr = dragon_getlasterrstr()

        self.msg = msg
        self.lib_msg = errstr[:].decode('utf-8')
        lib_err_str = dragon_get_rc_string(lib_err)
        self.lib_err = lib_err_str[:].decode('utf-8')

    def __str__(self):
        msg = self.msg
        if len(self.lib_msg) > 0:
            msg += f"\n*** Additional Info ***\n{self.lib_msg}\n*** End Additional Info ***"
        msg += f"\nDragon Error Code: {self.lib_err}"
        return msg

    @enum.unique
    class Errors(enum.Enum):
        SUCCESS = 0
        FAIL = 1


cdef class DragonLogger:
    """Python interface to C-level Dragon logging infrastructure"""

    cdef:
        dragonLoggingDescr_t _logger
        dragonLoggingSerial_t _serial
        bint _is_serialized

    def __cinit__(self):
        self._is_serialized = C_FALSE

    def __init__(self, MemoryPool mpool, lattrs=None, uid=0, mode=DRAGON_LOGGING_FIRST):
        """Create a DragonLogger instance for send/recv of infrastructure logs

        Args:
            mpool (MemoryPool): memory pool dedicated to logging infrastructure messages

            lattrs (NULL, optional): Logging attributes. *Not implemented*

            uid (default: 0): Unique ID to use for the logger.
                              If default is provided a semi-random value will be used
                              starting at BASE_LOG_CUID (see facts.py)

            mode (dragonLoggingMode, default: FIRST): What mode to run the logger in.
                              DRAGON_LOGGING_LOSSLESS - Block and wait on a full channel until a message can be inserted
                              DRAGON_LOGGING_FIRST - Drop message on a full channel
                              DRAGON_LOGGING_LAST - Remove oldest message to insert newest on a full channel !!! NOT IMPLEMENTED !!!

        Raises:
            RuntimeError: Input memory pool was None

            DragonLoggingError: Logging was not successfully initialized
        """
        cdef:
            dragonError_t derr
            char * errstr
            dragonLoggingAttr_t c_lattr
        # Python layer Logging attributes not yet impemented

        # TODO: Exceptions
        if mpool is None:
            raise RuntimeError("mpool cannot be none")

        # TODO: This should use the node_idx instead of +random()
        if uid == 0:
            uid = dfacts.BASE_LOG_CUID + random.randrange(1, 2**10) # Generate semi-random UID

        derr = dragon_logging_attr_init(&c_lattr)
        if derr != DRAGON_SUCCESS:
            raise DragonLoggingError(derr, "Failed to initialize logging attributes")

        c_lattr.mode = mode

        derr = dragon_logging_init(&mpool._pool_hdl, uid, &c_lattr, &self._logger)
        if derr != DRAGON_SUCCESS:
            raise DragonLoggingError(derr, "Failed to initialize logging object")

    def __del__(self):
        if self._is_serialized:
            dragon_logging_serial_free(&self._serial)
        # Check for attach and automatically call detach as well?

    def serialize(self):
        '''Return a serialized descriptor of the logging channel

        For an already created logger, return its serialized descriptor so
        other services can attach to it

        Returns:
            bytes: serialized descriptor
        '''
        cdef:
            dragonError_t derr

        if not self._is_serialized:
            derr = dragon_logging_serialize(&self._logger, &self._serial)
            if derr != DRAGON_SUCCESS:
                raise DragonLoggingError(derr, "Failed to serialize logger")

            self._is_serialized = C_TRUE

        py_obj = self._serial.data[:self._serial.len]
        return py_obj

    # Internal method used by classmethod attach to do the legwork
    def _attach(self, serialized_bytes, MemoryPool mpool):
        cdef:
             dragonError_t derr
             dragonLoggingSerial_t _serial

        _serial.len = len(serialized_bytes)
        cdef const unsigned char[:] cdata = serialized_bytes
        _serial.data = <uint8_t*>&cdata[0]
        self._is_serialized = C_FALSE

        if mpool is None:
            derr = dragon_logging_attach(&_serial, &self._logger, NULL)
        else:
            derr = dragon_logging_attach(&_serial, &self._logger, &mpool._pool_hdl)
        if derr != DRAGON_SUCCESS:
            raise DragonLoggingError(derr, "Failed to attach to serialized logger data")

        return self

    # This is a class method to return a new object
    @classmethod
    def attach(cls, serialized_bytes: bytes,
               MemoryPool mpool = None) -> DragonLogger:
        """Attach to a logging channel given a serialized descriptor

        :param serialized_bytes: serialized logging channel descriptor
        :type serialized_bytes: bytes
        :param mpool: memory pool to construct messages from. If None, tries
            to use the pool associated with the channel, defaults to None
        :type mpool: MemoryPool, optional
        :return: `DragonLogger` instance attached to given channel backed by option mpool
        :rtype: DragonLogger
        """
        empty_logger = cls.__new__(cls)
        return empty_logger._attach(serialized_bytes, mpool)

    def destroy(self, destroy_pool: bool = True):
        """Destroy the DragonLogger object and its underlying memory pool

        Should only be called when it is known that no other processes or
        services will be writing to it

        :param destroy_pool: whether to destroy memory pool support the logging channel, defaults to True
        :type destroy_pool: bool, optional
        :raises DragonLoggingError: unable to destroy the logging object as requested
        """
        cdef:
            dragonError_t derr
            bool ldestroy

        ldestroy = <bool> destroy_pool
        derr = dragon_logging_destroy(&self._logger, destroy_pool)
        if derr != DRAGON_SUCCESS:
            raise DragonLoggingError(derr, "Unable to destroy logging channel")

    def put(self, msg: str, priority: int=logging.INFO):
        '''Put a logging message into the logging channel queue

        Args:
            msg (str)

            priority (int): logging level with values matching those in Python's logging module (eg: logging.INFO)
        '''
        cdef:
            dragonError_t derr
            dragonLogPriority_t c_priority
            char * c_str

        # Using 'with nogil:' means cython won't allow unsafe use of temp python strings or coercions
        #  so this is a workaround
        py_str = msg.encode('utf-8')
        c_str = <char *>malloc(strlen(py_str)+1)
        strcpy(c_str, py_str)
        c_priority = <dragonLogPriority_t> priority
        with nogil:
            derr = dragon_logging_put(&self._logger, c_priority, c_str)

        free(c_str) # release temp c string

        if derr != DRAGON_SUCCESS:
            raise DragonLoggingError(derr, "Couldn't put message into Logger")

    def get(self, priority=logging.INFO, timeout=None):
        '''Get a message out of logging channel queue of level ``priority``

        Args:
            priority (int): level the return message should at minimum be (eg: logging.INFO)
            timeout (int, float): maximum time in seconds to wait for a message
        '''
        cdef:
            dragonError_t derr
            dragonLogPriority_t c_priority
            timespec_t timer
            timespec_t * time_ptr = NULL
            void * msg_out
            char * msg_str

        if timeout is not None:
            time_ptr = _compute_timeout(timeout, NULL, &timer)

        c_priority = <dragonLogPriority_t> priority
        with nogil:
            derr = dragon_logging_get(&self._logger, c_priority, &msg_out, time_ptr)

        # Return NULL
        if derr == DRAGON_LOGGING_LOW_PRIORITY_MSG:
            return None

        if derr != DRAGON_SUCCESS:
            if derr == DRAGON_CHANNEL_EMPTY or derr == DRAGON_TIMEOUT:
                raise ChannelEmpty("Channel Empty", derr)

            raise DragonLoggingError(derr, "Could not retrieve message")

        msg_str = <char*>msg_out # Convert to char
        msg_str += sizeof(dragonLogPriority_t) # Skip priority value header

        # Turn into a python string
        msg_len = len(msg_str)
        encoded_obj = msg_str[:msg_len]
        free(msg_out)
        return encoded_obj.decode("utf-8")

    def num_logs(self):
        """Get number of logs in queue

        Returns:
            int: number of logs currently in channel queue
        """
        cdef:
            dragonError_t derr
            uint64_t count

        derr = dragon_logging_count(&self._logger, &count)
        if derr != DRAGON_SUCCESS:
            raise DragonLoggingError(derr, "Could not retrieve number of logs")

        return count
