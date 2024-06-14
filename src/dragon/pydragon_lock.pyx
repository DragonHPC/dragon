from dragon.dtypes_inc cimport *
import cython
from enum import Enum

# @MCB TODO: This should pull the dragonLockType_t enum directly, but cpdef isn't allowed in cdef extern
class Type(Enum):
    FIFO = DRAGON_LOCK_FIFO
    FIFOLITE = DRAGON_LOCK_FIFO_LITE
    GREEDY = DRAGON_LOCK_GREEDY


cdef class DragonLock:
    """
    Cython interface for general dragon lock API
    """

    cdef dragonLock_t _lock

    def _handle_err(self, derr, err_msg):
        raise RuntimeError(err_msg + f" (Dragon Lock Error Code={dragon_get_rc_string(derr)})")

    @staticmethod
    def size(kind):
        return dragon_lock_size(kind.value)

    @staticmethod
    def init(kind, unsigned char[:] memobj):
        cdef dragonError_t derr
        cdef void * ptr = &memobj[0]

        hdl = DragonLock()
        derr = dragon_lock_init(&hdl._lock, ptr, kind.value)
        if derr != DRAGON_SUCCESS:
            hdl._handle_err(derr, f"Could not initialize lock of type {hdl._lock.kind}")

        return hdl

    @staticmethod
    def attach(unsigned char[:] memobj):
        cdef dragonError_t derr
        cdef void * ptr = &memobj[0]

        hdl = DragonLock()
        derr = dragon_lock_attach(&hdl._lock, ptr)
        if derr != DRAGON_SUCCESS:
            hdl._handle_err(derr, f"Could not attach lock")

        return hdl

    def lock(self):
        cdef dragonError_t derr

        derr = dragon_lock(&self._lock)
        if derr != DRAGON_SUCCESS:
            self._handle_err(derr, f"Could not lock kind {self._lock.kind}")

    def try_lock(self):
        cdef dragonError_t derr
        cdef int locked

        derr = dragon_try_lock(&self._lock, &locked)
        if derr != DRAGON_SUCCESS:
            self._handle_err(derr, f"Error trying to catch lock")

        return locked

    def unlock(self):
        cdef dragonError_t derr

        derr = dragon_unlock(&self._lock)
        if derr != DRAGON_SUCCESS:
            self._handle_err(derr, f"Could not unlock kind {self._lock.kind}")

    def detach(self):
        cdef dragonError_t derr

        derr = dragon_lock_detach(&self._lock)
        if derr != DRAGON_SUCCESS:
            self._handle_err(derr, "Could not detach kind {self._lock.kind}")

    def destroy(self):
        cdef dragonError_t derr

        derr = dragon_lock_destroy(&self._lock)
        if derr != DRAGON_SUCCESS:
            self._handle_err(derr, f"Could not destroy kind {self._lock.kind}")


# @MCB TODO: Feels like there might be a better D.R.Y. solution than having to separate classes
cdef class GreedyLock:
    """
    Cython interface for Greedy Shared Dragon Locks
    """

    cdef dragonGreedyLock_t _lock

    def _handle_err(self, derr, err_msg):
        raise RuntimeError(err_msg + f" (Dragon Lock Error Code={dragon_get_rc_string(derr)})")

    @staticmethod
    def size():
        return dragon_lock_size(DRAGON_LOCK_GREEDY)

    @staticmethod
    def init(unsigned char[:] memobj):
        cdef dragonError_t derr
        cdef void * ptr = &memobj[0]

        lock = GreedyLock()

        derr = dragon_greedy_lock_init(&lock._lock, ptr)
        if derr != DRAGON_SUCCESS:
            lock._handle_err(derr, "Could not initialize Greedy Lock")

        return lock

    @staticmethod
    def attach(unsigned char[:] memobj):
        cdef dragonError_t derr
        cdef void * ptr = &memobj[0]

        lock = GreedyLock()

        derr = dragon_greedy_lock_attach(&lock._lock, ptr)
        if derr != DRAGON_SUCCESS:
            lock._handle_err(derr, "Could not attach Greedy Lock")

        return lock

    def lock(self):
        cdef dragonError_t derr

        derr = dragon_greedy_lock(&self._lock)
        if derr != DRAGON_SUCCESS:
            self._handle_err(derr, "Could not lock Greedy Lock")

    def try_lock(self):
        cdef dragonError_t derr
        cdef int locked

        derr = dragon_greedy_try_lock(&self._lock, &locked)
        if derr != DRAGON_SUCCESS:
            self._handle_err(derr, f"Error trying to catch lock")

        return locked

    def unlock(self):
        cdef dragonError_t derr

        derr = dragon_greedy_unlock(&self._lock)
        if derr != DRAGON_SUCCESS:
            self._handle_err(derr, "Could not unlock Greedy Lock")

    def detach(self):
        cdef dragonError_t derr

        derr = dragon_greedy_lock_detach(&self._lock)
        if derr != DRAGON_SUCCESS:
            self._handle_err(derr, "Could not detach Greedy Lock")

    def destroy(self):
        cdef dragonError_t derr

        derr = dragon_greedy_lock_destroy(&self._lock)
        if derr != DRAGON_SUCCESS:
            self._handle_err(derr, "Could not destroy Greedy Lock")


cdef class FIFOLock:
    """
    Cython interface for Greedy Shared Dragon Locks
    """

    cdef dragonFIFOLock_t _lock

    def _handle_err(self, derr, err_msg):
        raise RuntimeError(err_msg + f" (Dragon Shared Lock Error Code={dragon_get_rc_string(derr)})")

    @staticmethod
    def size():
        return dragon_lock_size(DRAGON_LOCK_FIFO)

    @staticmethod
    def init(unsigned char[:] memobj):
        cdef dragonError_t derr
        cdef void * ptr = &memobj[0]

        lock = FIFOLock()

        derr = dragon_fifo_lock_init(&lock._lock, ptr)
        if derr != DRAGON_SUCCESS:
            lock._handle_err(derr, "Could not initialize FIFO Lock")

        return lock


    @staticmethod
    def attach(unsigned char[:] memobj):
        cdef dragonError_t derr
        cdef void * ptr = &memobj[0]

        lock = FIFOLock()

        derr = dragon_fifo_lock_attach(&lock._lock, ptr)
        if derr != DRAGON_SUCCESS:
            lock._handle_err(derr, "Could not attach FIFO Lock")

        return lock

    def lock(self):
        cdef dragonError_t derr

        derr = dragon_fifo_lock(&self._lock)
        if derr != DRAGON_SUCCESS:
            self._handle_err(derr, "Could not lock FIFO Lock")

    def try_lock(self):
        cdef dragonError_t derr
        cdef int locked

        derr = dragon_fifo_try_lock(&self._lock, &locked)
        if derr != DRAGON_SUCCESS:
            self._handle_err(derr, f"Error trying to catch lock")

        return locked

    def unlock(self):
        cdef dragonError_t derr

        derr = dragon_fifo_unlock(&self._lock)
        if derr != DRAGON_SUCCESS:
            self._handle_err(derr, "Could not unlock FIFO Lock")

    def detach(self):
        cdef dragonError_t derr

        derr = dragon_fifo_lock_detach(&self._lock)
        if derr != DRAGON_SUCCESS:
            self._handle_err(derr, "Could not detach FIFO Lock")

    def destroy(self):
        cdef dragonError_t derr

        derr = dragon_fifo_lock_destroy(&self._lock)
        if derr != DRAGON_SUCCESS:
            self._handle_err(derr, "Could not destroy FIFO Lock")
