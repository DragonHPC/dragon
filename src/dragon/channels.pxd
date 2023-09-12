from dragon.dtypes_inc cimport *
from dragon.managed_memory cimport *

cdef class Channel:

    cdef:
        dragonChannelDescr_t _channel
        dragonChannelSerial_t _serial
        bint _is_serialized
        bint _is_remote
        dragonChannelAttr_t _attr
        bint _creator
        MemoryPool _default_pool

    cdef inline get_pool_ptr(self, dragonMemoryPoolDescr_t * pool):
        cdef:
            dragonError_t derr

        derr = dragon_channel_get_pool(&self._channel, pool)
        if derr != DRAGON_SUCCESS:
            return (derr, "Could not retrieve memory pool from channel")
