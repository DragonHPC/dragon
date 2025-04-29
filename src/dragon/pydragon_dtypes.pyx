"""These are basic types and classes used throughout the rest of the core layers of Dragon. Users may primarily be
interested in `get_rc_string()` and `getlasterrstr()` for debugging purposes.
"""
from dragon.dtypes_inc cimport *
# changing the line below to 'from dragon.rc import *' caused an internal error
# in Cython once the WaitMode was added below. So we import explicitly since
# all we need is DragonError anyway. The error is very cryptic. It was:
# pydragon_dtypes.c:3589:20: error: lvalue required as left operand of assignment
# and this pointed to this line
# PY_VERSION_HEX = __Pyx_PyInt_As_int(o); if (unlikely((PY_VERSION_HEX == (int)-1) && PyErr_Occurred())) __PYX_ERR(1, 6, __pyx_L2_error)
from dragon.rc import DragonError

cpdef get_rc_string(const dragonError_t rc):
    """Get the string for the given error code from the underlying Dragon library"""
    s = dragon_get_rc_string(rc)
    ds = s[:].decode('utf-8')
    return ds

cpdef getlasterrstr():
    """Get the last error string from the underlying Dragon library"""
    s = dragon_getlasterrstr()
    ds = s[:].decode('utf-8')
    free(s)
    return ds

cdef class WaitMode:
    """A class for excapsultating the wait mode for for objects using the BCast"""

    cdef:
        dragonWaitMode_t _wait_mode

    def __cinit__(self, wait_mode=DRAGON_IDLE_WAIT):
        self._wait_mode = wait_mode

    def __getstate__(self):
        return (self._wait_mode,)

    def __setstate__(self, state):
        (wait_mode,) = state
        self._wait_mode = wait_mode

    def __repr__(self):
        return f'WaitMode({self._wait_mode})'

    @property
    def value(self):
        return self._wait_mode

IDLE_WAIT = WaitMode(DRAGON_IDLE_WAIT)
SPIN_WAIT = WaitMode(DRAGON_SPIN_WAIT)
ADAPTIVE_WAIT = WaitMode(DRAGON_ADAPTIVE_WAIT)
DEFAULT_WAIT_MODE = DRAGON_DEFAULT_WAIT_MODE

cdef class ReturnWhen:
    """A class for encapsulating the return condition for Channels"""

    cdef dragonChannelSendReturnWhen_t _return_when_mode

    def __cinit__(self, return_when=DRAGON_CHANNEL_SEND_RETURN_WHEN_BUFFERED):
        self._return_when_mode = return_when

    def __getstate__(self):
        return (self._return_when_mode,)

    def __setstate__(self, state):
        (self._return_when_mode,) = state

    def __repr__(self):
        return f'ReturnWhen({self._return_when_mode})'

    @property
    def value(self):
        return self._return_when_mode

WHEN_IMMEDIATE = ReturnWhen(DRAGON_CHANNEL_SEND_RETURN_IMMEDIATELY)
WHEN_BUFFERED = ReturnWhen(DRAGON_CHANNEL_SEND_RETURN_WHEN_BUFFERED)
WHEN_DEPOSITED = ReturnWhen(DRAGON_CHANNEL_SEND_RETURN_WHEN_DEPOSITED)
WHEN_RECEIVED = ReturnWhen(DRAGON_CHANNEL_SEND_RETURN_WHEN_RECEIVED)

cdef class RecvNotifType:
    """A class for encapsulating the notification mechanism"""

    cdef dragonChannelRecvNotif_t _notif_type

    def __cinit__(self, notification_type=DRAGON_RECV_SYNC_MANUAL):
        self._notif_type = notification_type

    def __getstate__(self):
        return (self._notif_type,)

    def __setstate__(self, state):
        (self._notif_type,) = state

    def __repr__(self):
        return f'RecvNotifType({self._notif_type})'

    @property
    def value(self):
        return self._notif_type

RECV_SYNC_SIGNAL = RecvNotifType(DRAGON_RECV_SYNC_SIGNAL)
RECV_SYNC_MANUAL = RecvNotifType(DRAGON_RECV_SYNC_MANUAL)

class DragonException(Exception):
    """A base class for Dragon exceptions"""
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
        try:
            if self._lib_err is None:
                return f'Dragon Message: {self._msg}'

            if self._lib_msg is None or self._lib_msg.strip() == '':
                return f'Dragon Message: {self._msg} | Return Code: {self._lib_err_str}'

            return f'Dragon Message: {self._msg} | Return Code: {self._lib_err_str}\n{self._lib_msg}'
        except Exception as ex:
            return f'While converting DragonException to string we got {repr(ex)}'

    def __repr__(self):
        return str(self)

    @property
    def lib_err(self):
        return self._lib_err

    def __repr__(self):
        return f'{self.__class__}({self._msg!r}, {self._lib_err!r}, {self._lib_msg!r}, {self._lib_err_str!r})'

class DragonObjectDestroyed(DragonException):
    pass