from dragon.dtypes_inc cimport *

cdef class MemoryPool:
    cdef dragonMemoryPoolDescr_t _pool_hdl
    cdef dragonMemoryPoolSerial_t _pool_ser
    cdef dragonMemoryPoolAttr_t _mattr
    cdef bint _serialized

    cdef inline dragonError_t get_pool_ptr(self, dragonMemoryPoolDescr_t * pool):

        return dragon_memory_pool_descr_clone(pool, &self._pool_hdl)

cdef class MemoryAlloc:
    cdef dragonMemoryDescr_t _mem_descr
    cdef size_t _mem_size
    cdef dragonMemorySerial_t _mem_ser
    cdef bint _is_serial
    cdef bint _is_attach

    @staticmethod
    cdef inline cinit(dragonMemoryDescr_t mem_descr):
        """
        Initializes the object and sets the C struct values.
        The error handling has to happen in the caller.

        :return: MemoryAlloc object or error description
        """
        cdef:
            dragonError_t derr

        memobj = MemoryAlloc()
        derr = dragon_memory_descr_clone(&memobj._mem_descr, &mem_descr, 0, NULL)
        if derr != DRAGON_SUCCESS:
            return (derr, "Could not clone memory descriptor")

        derr = dragon_memory_get_size(&memobj._mem_descr, &memobj._mem_size)
        if derr != DRAGON_SUCCESS:
            return (derr, "Could not retrieve memory size")

        memobj._is_serial = 0
        memobj._is_attach = 0
        return memobj

    cdef inline dragonMemoryDescr_t get_descr(self):
        return self._mem_descr
