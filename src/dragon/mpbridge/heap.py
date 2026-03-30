"""Dragon's replacement for the Multiprocessing Heap based on Dragon managed memory.
We effectively back `multiprocessing.heap.Arena` with Dragons managed memory.
This is a single node only implementation.
"""

import os

import multiprocessing.heap

from ..managed_memory import MemoryAlloc, MemoryPool
from ..infrastructure.facts import default_pool_muid_from_index
from ..globalservices.pool import query
from ..infrastructure.parameters import this_process
import dragon.utils as du

_DEF_MUID = default_pool_muid_from_index(this_process.index)


class DragonArena(multiprocessing.heap.Arena):
    """Dragon's version of a file backed Arena uses
    the default managed memory pool to get a buffer.
    A custom `m_uid` can be given as well.
    """

    def __init__(self, size: int, fd: int = -1, _m_uid: int = _DEF_MUID):
        """Initialize the DragonArena.

        :param size: size in bytes
        :type size: int
        :param fd: a dummy file descriptor, defaults to -1
        :type fd: int, optional
        :param _m_uid: memory pool m_uid, defaults to _DEF_MUID
        :type _m_uid: int, optional
        """

        self._closed = True

        self.size = size
        self.fd = fd  # dummy

        if _m_uid == _DEF_MUID:
            sdesc = du.B64.str_to_bytes(os.environ.get("DRAGON_DEFAULT_PD", "-1"))
        else:
            pool_descr = query(_m_uid)
            sdesc = pool_descr.sdesc

        pool = MemoryPool.attach(sdesc)
        self._mem = pool.alloc(size)
        self.buffer = self._mem.get_memview()
        pool.detach()

        self._closed = False

    def __setstate__(self, state):
        self._closed = True

        mem_ser, self.size, self.fd = state
        self._mem = MemoryAlloc.attach(mem_ser)
        self.buffer = self._mem.get_memview()

        self._closed = False

    def __getstate__(self):
        return self._mem.serialize(), self.size, self.fd

    def __del__(self):
        if not self._closed:
            self._closed = True
            try:
                self._mem.detach()
            except Exception as e:
                pass

    def _destroy(self):
        if not self._closed:
            self._closed = True
            try:
                self._mem.destroy()
            except Exception as e:
                pass
