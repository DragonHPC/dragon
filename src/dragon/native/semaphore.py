"""The Dragon native implementation of the classical semaphore synchronization object.

The API follows the Python Multiprocessing API of Semaphore and extends it with
a `get_value()` method.
"""

import logging
import time
import os

from .lock import Lock
from ..channels import Channel, ChannelRecvTimeout, ChannelEmpty, ChannelRecvH, ChannelSendH, EventType, PollResult
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

    def __del__(self):
        try:
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

        # Not used beyond initialization, but included for debugging.
        self._bounded = bounded
        self._initial_value = value
        local_opts = {"semaphore":not bounded, "bounded_semaphore":bounded, "initial_sem_value":value}

        the_options = ChannelOptions(ref_count=True, local_opts=local_opts)

        descr = create(m_uid=m_uid, options=the_options)

        self._channel_sdesc = descr.sdesc
        self._channel = Channel.attach(descr.sdesc)
        # get_refcnt(self._channel.cuid)

    def __getstate__(self) -> tuple:

        return (self._channel_sdesc, self._bounded, self._initial_value)

    def __setstate__(self, state) -> None:

        self._channel_sdesc, self._bounded, self._initial_value = state

        self._channel = Channel.attach(self._channel_sdesc)
        get_refcnt(self._channel.cuid)

    def acquire(self, blocking: bool = True, timeout: float = None) -> bool:
        """Acquire the Semaphore, decrementing the counter, blocking other
        processes when the counter is decremented to 0.

        :param blocking: block the process if the Semaphore cannot be acquired immediately, defaults to True
        :type blocking: bool, optional
        :param timeout: timeout for a block in seconds, defaults to None (i.e. no timeout)
        :type timeout: float, optional
        """

        if not blocking:
            timeout = 0

        LOGGER.debug(f"Acquire Semaphore with blocking={blocking}, timeout={timeout}")

        return self._channel.poll(event_mask=EventType.SEMAPHORE_P, timeout=timeout)

    def release(self, n: int = 1) -> None:
        """Release the Semaphore, incrementing the internal value by n,
        thus unblocking up to n processes.

        :param n: how much to increment the internal value by, defaults to 1
        :type n: int, optional
        """

        LOGGER.debug(f"Release Semaphore n={n}")

        for i in range(n):
            self._channel.poll(event_mask=EventType.SEMAPHORE_V)

    def get_value(self) -> int:
        """Get the value of the internal counter without acquiring the Semaphore."""

        result = PollResult()
        self._channel.poll(event_mask=EventType.SEMAPHORE_PEEK, poll_result=result)
        return result.value


