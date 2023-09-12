
from dragon.dtypes_inc cimport *
# changing the line below to 'from dragon.rc import *' caused an internal error
# in Cython once the WaitMode was added below. So we import explicitly since
# all we need is DragonError anyway. The error is very cryptic. It was:
# pydragon_dtypes.c:3589:20: error: lvalue required as left operand of assignment
# and this pointed to this line
# PY_VERSION_HEX = __Pyx_PyInt_As_int(o); if (unlikely((PY_VERSION_HEX == (int)-1) && PyErr_Occurred())) __PYX_ERR(1, 6, __pyx_L2_error)
from dragon.rc import DragonError

cpdef get_rc_string(const dragonError_t rc):
    """ TBD """
    s = dragon_get_rc_string(rc)
    ds = s[:].decode('utf-8')
    return ds

cpdef getlasterrstr():
    """ TBD """
    s = dragon_getlasterrstr()
    ds = s[:].decode('utf-8')
    free(s)
    return ds

cdef class WaitMode:
    """ TBD """

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
    """ TBD """

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
    """ TBD """

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