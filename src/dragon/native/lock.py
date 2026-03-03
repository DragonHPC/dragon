"""The Dragon native lock provides basic synchronization functionality between processes and threads.

Importantly, a `Lock()` can be released by any process or thread, while a recursive
`RLock()` needs to be released by the process & thread that called it.

Our implementation also extends the API by a few methods that are internal
in Python Multiprocessing, but are used very frequently, thus appear useful for everyone.
"""

import logging
import os
from sys import byteorder
from threading import current_thread

import dragon

from dragon.channels import Channel, ChannelRecvH, ChannelSendH, ChannelFull, ChannelSendTimeout, ChannelEmpty
from dragon.dtypes import WHEN_DEPOSITED

from ..globalservices.channel import release_refcnt, create, get_refcnt

from ..infrastructure.channel_desc import ChannelOptions
from ..infrastructure.facts import default_pool_muid_from_index
from ..infrastructure.parameters import this_process

LOGGER = logging.getLogger(__name__)

_DEF_MUID = default_pool_muid_from_index(this_process.index)


class Lock:
    """Dragon native Lock implementation based on a channel with a capacity of 1.

    Once a process or thread has acquired a lock, subsequent attempts to acquire it from any process or thread
    will block until it is released; any process may release it.

    Recursive use is supported by means of the 'recursive' parameter.  A recursive lock must be released by
    the process or thread that acquired it. Once a process or thread has acquired a recursive lock, the same
    process or thread may acquire it again without blocking; that process or thread must release it once for
    each time it has been acquired.

    Lock supports the context manager protocol and thus may be used in 'with' statements.
    """

    def _close(self) -> None:
        if not self._closed:
            self._closed = True
            try:
                self._reader.close()
                self._writer.close()
                self._channel.detach()
                release_refcnt(self._cuid)
            except Exception:
                pass  # all of those may fail if the channel has disappeared during shutdown

    def __del__(self):
        self._close()

    def __init__(self, m_uid: int = _DEF_MUID, recursive: bool = False):
        """Initialize the lock object

        :param m_uid: memory pool to create the channel in, defaults to _DEF_MUID
        :type m_uid: int, optional
        :param recursive: if the Lock should be recursive, defaults to False
        :type recursive: bool, optional
        """

        self._closed = True

        self._recursive = recursive

        the_options = ChannelOptions(ref_count=True)
        the_options.local_opts.capacity = 1

        descr = create(m_uid=m_uid, options=the_options)
        self._channel = Channel.attach(descr.sdesc)
        get_refcnt(descr.c_uid)

        self._cuid = descr.c_uid
        self._reset()
        self._closed = False

    def _reset(self):

        self._lmsg = this_process.my_puid.to_bytes(8, byteorder)
        self._reader = ChannelRecvH(self._channel)
        self._writer = ChannelSendH(self._channel, return_mode=WHEN_DEPOSITED)
        self._reader.open()
        self._writer.open()
        self._accesscount = 0

    def __getstate__(self) -> tuple:

        if self._closed:
            raise ValueError(f"Lock {self!r} is closed")

        puid = this_process.my_puid
        tid = current_thread

        return self._channel.serialize(), self._recursive, self._cuid, self._accesscount, puid, tid

    def __setstate__(self, state) -> None:

        self._closed = True

        sdesc, self._recursive, self._cuid, accesscount, puid, tid = state

        get_refcnt(self._cuid)
        self._channel = Channel.attach(sdesc)

        self._reset()

        # if sent to myself, set accesscount correctly
        if this_process.my_puid == puid:
            if current_thread == tid:
                self._accesscount = accesscount

        self._closed = False

    def acquire(self, block: bool = True, timeout: float = None) -> bool:
        """Acquire a lock, blocking or non-blocking.

        If the lock is not recursive:

        With the block argument set to True (the default), the method call will block until the lock is in an
        unlocked state, then set it to locked and return True.

        With the block argument set to False, the method call does not block.  If the lock is currently in a
        locked state, return False; otherwise set the lock to a locked state and return True.  When invoked
        with a positive, floating-point value for timeout, block for at most the number of seconds specified
        by timeout as long as the lock can not be acquired. Invocations with a negative value for timeout are
        equivalent to a timeout of zero. Invocations with a timeout value of None (the default) set the
        timeout period to infinite.  The timeout argument has no practical implications if the block argument
        is set to False and is thus ignored. Returns True if the lock has been acquired or False if the
        timeout period has elapsed.

        If the lock is recursive, repeated calls to a lock already owned by the current process return True
        and increment the recursion level. `Release()` has then be called an equal amount of times by the same
        process to free the lock.

        :param block: should the call block at all, defaults to True
        :type block: bool, optional
        :param timeout: how long to block in seconds, ignored if block=False, defaults to None
        :type timeout: float, optional
        :return: if the lock was acquired
        :rtype: bool
        """

        # Locks are often acquired and released many times, we don't log these to prevent
        # big impacts on performance.
        # LOGGER.debug(f"Acquire Lock {self!r} with blocking={block}, timeout={timeout}")

        if timeout is not None and timeout < 0:
            timeout = 0

        if self._recursive:
            if self._accesscount > 0:
                self._accesscount += 1
                return True

        # NOTE: If we had an rwlock() on the channel, we might just that and make the lock
        # even lighter.

        try:
            self._writer.send_bytes(self._lmsg, timeout=timeout, blocking=block)

            self._accesscount = 1
            return True
        except (ChannelFull, ChannelSendTimeout) as e:
            return False

    def release(self) -> None:
        """Release a lock.

        If the lock is not recursive: This can be called from any process or thread,
        not only the process or thread which originally acquired the lock.

        If the lock is recursive: Decrement the recursion level. If after the decrement the recursion level is
        zero, reset the lock to unlocked (not owned by any process or thread) and if any other processes or
        threads are blocked waiting for the lock to become unlocked, allow exactly one of them to proceed. If
        after the decrement the recursion level is still nonzero, the lock remains locked and owned by the
        calling dragon process and thread.

        :raises ValueError: if the lock has not been acquired before
        :raises AssertionError: if the lock is recursive and not held by the caller
        """

        # Locks are often acquired and released many times, we don't log these to prevent
        # big impacts on performance.
        # LOGGER.debug(f"Release Lock {self!r}")

        if self._recursive:
            assert self._accesscount > 0, "attempt to release recursive lock not owned by process"
            self._accesscount -= 1
            if self._accesscount > 0:
                return

        # this will remove any message, thus unblocking the next process to acquire
        try:
            _ = self._reader.recv_bytes(timeout=0)
            self._accesscount = 0
        except ChannelEmpty as e:
            raise ValueError("Cannot release unlocked lock.")

    # API extention
    # NOTE: These methods are private methods on _Semlock in Python Multiprocessing,
    # but used very frequently internally. They are very useful, so I think we want
    # them for our users and I am pretty sure packages on top of Multiprocessing
    # are using them too.

    def is_recursive(self) -> bool:
        """Return True if this lock is recursive, i.e. can be called multiple times by the
        same thread/process.

        :return: recursiveness of the lock
        :rtype: bool
        """
        return self._recursive

    def is_mine(self) -> bool:
        """Return True if this rlock is owned by the caller process & thread

        :return: ownership of the lock
        :rtype: bool
        """
        return self._accesscount > 0

    def count(self) -> int:
        """How often this lock has been acquired.
        Will return only 0 or 1 for a non-recursive Lock.
        :return: The number of times this lock has been acquired
        :rtype: int
        """
        return self._accesscount

    def __enter__(self):
        self.acquire()

    def __exit__(self, *args):
        self.release()


# Extensions.  TODO, should we plug into the mp.util.Finalize stuff?
