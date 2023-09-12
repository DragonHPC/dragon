"""The Dragon native implementation of the classical semaphore synchronization object.

The API follows the Python Multiprocessing API of Semaphore and extends it with
a `get_value()` method.
"""

import logging
import time
import os

from .lock import Lock
from ..channels import Channel, ChannelRecvTimeout, ChannelEmpty, ChannelRecvH, ChannelSendH
from ..dtypes import WHEN_DEPOSITED
from ..globalservices.channel import get_refcnt, release_refcnt, create
from ..infrastructure.channel_desc import ChannelOptions
from ..infrastructure.parameters import this_process
from ..infrastructure.facts import default_pool_muid_from_index

LOGGER = logging.getLogger(__name__)

_DEF_MUID = default_pool_muid_from_index(this_process.index)


class Semaphore:
    """This class implements Semaphore objects on top of Dragon channels.
    We use a Dragon native Lock to protect a channel with a single entry that
    is used to count the number of acquire and release calls.
    We extend the standard API with a get_value method to ask the
    Semaphore for the current value.
    """

    _BYTEORDER = "big"
    _POLL_FREQ = 0.01

    def __del__(self):
        if not self._closed:
            self._closed = True
            try:
                self._writer.close()
                self._reader.close()
                release_refcnt(self._channel.cuid)
                self._channel.detach()
            except Exception:
                pass

    def __enter__(self):
        self.acquire()

    def __exit__(self, *args):
        self.release()

    def __init__(self, value: int = 1, *, m_uid: int = _DEF_MUID, bounded=False):
        """Create a Semaphore object that can be shared across Dragon processes.

        :param value: initial value for the counter, defaults to 1
        :type value: int, optional
        :param m_uid: dragon managed memory pool id to use, defaults to the standard pool.
        :type m_uid: int, optional
        :param bounded: set if the value of the Semaphore cannot exceed its initial value, defaults to False
        :type bounded: bool, optional
        """

        LOGGER.debug(f"Init Semaphore with value={value}, m_uid={m_uid}, bounded={bounded}")

        self._closed = True

        self._bounded = bounded
        self._initial_value = value

        self._lock = Lock()

        the_options = ChannelOptions(ref_count=True)
        the_options.local_opts.capacity = 1

        descr = create(m_uid=m_uid, options=the_options)

        self._channel_sdesc = descr.sdesc
        self._channel = Channel.attach(descr.sdesc)
        # get_refcnt(self._channel.cuid)

        self._reset()

        self._lmsg = value.to_bytes(8, byteorder=self._BYTEORDER)

        if value > 0:
            self._writer.send_bytes(self._lmsg)

        self._closed = False

    def __getstate__(self) -> tuple:

        if self._closed:
            raise ValueError(f"Semaphore {self!r} is closed and cannot be serialized.")

        sdesc = self._channel.serialize()

        return sdesc, self._bounded, self._initial_value, self._lock

    def __setstate__(self, state) -> None:

        self._closed = True

        ch_sdesc, self._bounded, self._initial_value, self._lock = state

        self._channel = Channel.attach(ch_sdesc)
        get_refcnt(self._channel.cuid)

        self._reset()

        value = self.get_value()
        self._lmsg = value.to_bytes(8, byteorder=self._BYTEORDER)

        self._closed = False

    def acquire(self, blocking: bool = True, timeout: float = None) -> bool:
        """Acquire the Semaphore, decrementing the counter, blocking the other
        processes, if the counter is 0.

        :param blocking: block the process if the Semaphore cannot be acquired immediately, defaults to True
        :type blocking: bool, optional
        :param timeout: timeout for a block in seconds, defaults to None
        :type timeout: float, optional
        """

        LOGGER.debug(f"Acquire Semaphore with blocking={blocking}, timeout={timeout}")

        # We need to protect the channel with a lock to stop other processes from
        # reading or writing into it while we are incrementing the counter.
        # This is ugly and not very performant
        # TODO: use dragon_channel_write_lock instead of a poor mans wait here.
        # That should get rid of the self._lock

        if timeout is None:
            timeout = 1000000000

        if timeout < 0 or not blocking:
            timeout = 0

        sleep_time = min(self._POLL_FREQ, timeout)

        start = time.monotonic()
        stop = start

        success = False
        while (stop - start) <= timeout:

            if not self._lock.acquire(block=False, timeout=None):
                time.sleep(sleep_time)
                stop = time.monotonic()
                continue

            try:
                self._lmsg = self._reader.recv_bytes(timeout=None, blocking=False)
                success = True
                break
            except ChannelEmpty:  # empty channel - keep waiting
                pass

            self._lock.release()

            time.sleep(sleep_time)

            stop = time.monotonic()

        if not success:
            return False

        # still holding the lock here
        value = int.from_bytes(self._lmsg, byteorder=self._BYTEORDER)
        value -= 1

        if value > 0:  # only write back to channel if value is not 0, else leave it empty
            self._lmsg = value.to_bytes(8, self._BYTEORDER)
            self._writer.send_bytes(self._lmsg, timeout=None, blocking=True)

        self._lock.release()

        return True

    def release(self, n: int = 1) -> None:
        """Release the Semaphore, incrementing the internal value by n,
        thus unblocking up to n processes.

        :param n: how much to increment the internal value by, defaults to 1
        :type n: int, optional
        """

        LOGGER.debug(f"Release Semaphore n={n}")

        self._lock.acquire(block=True, timeout=None)

        try:
            self._lmsg = self._reader.recv_bytes(timeout=None, blocking=False)
        except ChannelEmpty:
            value = 0
        else:  # use read value
            value = int.from_bytes(self._lmsg, byteorder=self._BYTEORDER)

        value += n

        if self._bounded:
            if value > self._initial_value:
                self._writer.send_bytes(self._lmsg, timeout=None, blocking=True)
                raise ValueError(
                    f"BoundedSemaphore value of {value} would exceed initial value of {self._initial_value}"
                )

        self._lmsg = value.to_bytes(8, self._BYTEORDER)

        self._writer.send_bytes(self._lmsg, timeout=None, blocking=True)

        self._lock.release()

    def get_value(self) -> int:
        """Get the value of the internal counter without acquiring the Semaphore."""

        self._lock.acquire(block=True, timeout=None)

        try:
            self._lmsg = self._reader.recv_bytes(timeout=None, blocking=False)
        except ChannelEmpty:
            value = 0
        else:
            self._writer.send_bytes(self._lmsg, timeout=None, blocking=False)
            value = int.from_bytes(self._lmsg, byteorder=self._BYTEORDER)

        self._lock.release()

        return value

    def _reset(self) -> None:

        self._reader = ChannelRecvH(self._channel)
        self._writer = ChannelSendH(self._channel, return_mode=WHEN_DEPOSITED)
        self._reader.open()
        self._writer.open()
