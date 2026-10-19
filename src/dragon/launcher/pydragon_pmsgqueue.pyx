from libc.stdint cimport uint64_t, uint32_t, uint8_t
from libc.stdlib cimport free
cimport cython
import enum

DEFAULT_RESET_TIMEOUT = 300

cdef extern from "<dragon/return_codes.h>":

    ctypedef enum dragonError_t:
        DRAGON_SUCCESS
        DRAGON_PMSGQUEUE_UNSPECIFIED_ERROR
        DRAGON_PMSGQUEUE_SEND_TO_RECVQ
        DRAGON_PMSGQUEUE_RECV_FROM_SENDQ
        DRAGON_PMSGQUEUE_NOT_OPEN
        DRAGON_PMSGQUEUE_EXCEEDS_MAX_MSG_SIZE
        DRAGON_PMSGQUEUE_CANNOT_SEND
        DRAGON_PMSGQUEUE_CANNOT_RECV
        DRAGON_PMSGQUEUE_INVALID_NAME
        DRAGON_PMSGQUEUE_PERMISSION_VIOLATION
        DRAGON_PMSGQUEUE_NO_ERR_MSG_AVAILABLE
        DRAGON_PMSGQUEUE_MQUEUE_CONFIG_ERROR
        DRAGON_PMSGQUEUE_CLOSE_MUST_BE_CALLED_FIRST
        DRAGON_TIMEOUT

cdef extern from "_pmsgqueue.h":

    ctypedef struct dragonPMsgQueueHandle_t:
        pass

    # General ops
    dragonError_t dragon_pmsgqueue_init_handle(dragonPMsgQueueHandle_t * handle)
    dragonError_t dragon_pmsgqueue_reset(dragonPMsgQueueHandle_t* handle, size_t timeout)
    dragonError_t dragon_pmsgqueue_close(dragonPMsgQueueHandle_t * handle, bint destroy)
    dragonError_t dragon_pmsgqueue_get_last_err_msg(dragonPMsgQueueHandle_t * handle, char ** msg)
    dragonError_t dragon_pmsgqueue_get_queue_name(dragonPMsgQueueHandle_t* handle, char** name_ptr)


    # Send ops
    dragonError_t dragon_pmsgqueue_create_sendq(dragonPMsgQueueHandle_t * handle, const char * name)
    dragonError_t dragon_pmsgqueue_send(dragonPMsgQueueHandle_t * handle, const char * msg)

    # Receive ops
    dragonError_t dragon_pmsgqueue_create_recvq(dragonPMsgQueueHandle_t * handle, const char * name)
    dragonError_t dragon_pmsgqueue_recv(dragonPMsgQueueHandle_t* handle, char** msg, size_t timeout)



################################
# Begin Cython definitions
################################
cdef _lib_err_str(dragonPMsgQueueHandle_t * handle):
    cdef:
        char * errstr
        dragonError_t derr

    # This will (if a message is available) call malloc for errstr, and must be released
    derr = dragon_pmsgqueue_get_last_err_msg(handle, &errstr)
    if derr == DRAGON_PMSGQUEUE_NO_ERR_MSG_AVAILABLE:
        ex_msg = "no traceback"
    else:
        ex_msg = errstr[:].decode('utf-8')
        free(errstr)

    return ex_msg


class PMsgQueueException(Exception):
    def __init__(self, msg, lib_msg, lib_err):
        self.message = msg
        self.lib_msg = lib_msg
        self.lib_err = lib_err

    def __str__(self):
        return f"PMsgQueue Exception: {self.message} | Dragon Msg: {self.lib_msg} | Dragon Error Code: {self.lib_err}"

class PMsgQueueOpenException(PMsgQueueException):
    pass

class PMsgQueueSendException(PMsgQueueException):
    pass

class PMsgQueueReceiveException(PMsgQueueException):
    pass

class PMsgQueueConfigException(PMsgQueueException):
    pass

class PMsgQueueTimeoutException(PMsgQueueException):
    pass

class PMsgQueueCloseException(PMsgQueueException):
    pass


cdef class PMsgQueue:

    cdef dragonPMsgQueueHandle_t _handle
    cdef str _qname

    def __init__(self, queue_name, write_intent=False, read_intent=False):
        cdef:
            dragonError_t derr

        self._qname = queue_name

        if write_intent == read_intent:
            raise PMsgQueueOpenException('Must choose either a send or a receive queue', 'no traceback', -1)

        dragon_pmsgqueue_init_handle(&self._handle)

        if write_intent:
            derr = dragon_pmsgqueue_create_sendq(&self._handle, self._qname.encode('utf-8'))
        else:
            derr = dragon_pmsgqueue_create_recvq(&self._handle, self._qname.encode('utf-8'))

        if derr != DRAGON_SUCCESS:
            if derr == DRAGON_PMSGQUEUE_CLOSE_MUST_BE_CALLED_FIRST:
                raise PMsgQueueOpenException('Close Must Be Called First', _lib_err_str(&self._handle), derr)
            elif derr == DRAGON_PMSGQUEUE_INVALID_NAME:
                raise PMsgQueueConfigException('Invalid Queue Name', _lib_err_str(&self._handle), derr)
            elif derr == DRAGON_PMSGQUEUE_MQUEUE_CONFIG_ERROR:
                raise PMsgQueueConfigException('Config Error', _lib_err_str(&self._handle), derr)
            else:
                raise PMsgQueueException('Unexpected Error While Initializing PMsgQueue', _lib_err_str(&self._handle), derr)

    @property
    def queue_name(self):
        return self._qname

    def reset(self, timeout=DEFAULT_RESET_TIMEOUT):
        cdef dragonError_t derr

        derr = dragon_pmsgqueue_reset(&self._handle, timeout)
        if derr != DRAGON_SUCCESS:
            raise PMsgQueueConfigException('Error on reset', _lib_err_str(&self._handle), derr)

    def send(self, msg: str):
        cdef:
            dragonError_t derr

        derr = dragon_pmsgqueue_send(&self._handle, msg.encode('utf-8'))
        if derr != DRAGON_SUCCESS:
            raise PMsgQueueSendException('Error on sending message', _lib_err_str(&self._handle), derr)

    def recv(self, timeout=0):
        cdef:
            dragonError_t derr
            char * ptr

        derr = dragon_pmsgqueue_recv(&self._handle, &ptr, timeout)

        if derr == DRAGON_TIMEOUT:
            raise PMsgQueueTimeoutException('Receive Timeout', _lib_err_str(&self._handle), derr)

        if derr != DRAGON_SUCCESS:
            raise PMsgQueueReceiveException('Receive Error', _lib_err_str(&self._handle), derr)

        py_msg = ptr[:].decode('utf-8')
        free(ptr)

        return py_msg

    def close(self, destroy=False):
        cdef:
            dragonError_t derr
            bint idestroy = destroy

        derr = dragon_pmsgqueue_close(&self._handle, idestroy)
        if derr != DRAGON_SUCCESS:
            raise PMsgQueueCloseException('Could Not Close', _lib_err_str(&self._handle), derr)
