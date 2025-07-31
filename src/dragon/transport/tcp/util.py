from asyncio import Queue, QueueFull
from collections.abc import Generator
from contextlib import contextmanager
import math
import time
from typing import Any, Optional, Union

from ...channels import Channel, ChannelRecvH, ChannelSendH, Message, PollResult
from ...managed_memory import MemoryAlloc, MemoryPool
from ...dtypes import WaitMode, DEFAULT_WAIT_MODE


def unget_nowait(self: Queue, item: Any) -> None:
    """Re-queues an item at the front of a queue, essentially performing the
    opposite of get_nowait().

    Differs from put_nowait() in that the item is put on the front of the queue
    as opposed to the end, and the number of unfinished tasks is not
    incremented.  As a result, do NOT call task_done() for a requeued item.

    Raises QueueFull if the queue is at max size and the item cannot be
    requeued.
    """
    if self.full():
        raise QueueFull
    self._queue.appendleft(item)
    self._wakeup_next(self._getters)


def seconds_remaining(deadline: float, _inf: Optional[float] = None) -> (Optional[float], float):
    """Returns `(N, since)` where `N` is the number of seconds remaining until
    deadline is exceeded, or 0 if the deadline is exceeded, and `since` is the
    baseline timestamp, i.e., `time.monotonic()`.

    `N` is suitable for use as the `timeout` argument to `ChannelSendH.send()`
    and `ChannelRecvH.recv()`, where `blocking=True` (the default). For example,
    when

    - `abs(deadline) == math.inf`, then `N` will be None` and `blocking=True, timeout=None`
    - seconds remaining <= 0, then `N == 0`, which is equivalent to `blocking=False` or `blocking=True, timeout=0`
    - seconds remaining > 0, then `blocking=True, timeout=N`

    .. note::
        When called successively, `N` will decrease to, but not go below, `0`.
        As a result, it supports graceful degradation of the `timeout` argument
        to `ChannelSendH.send()` and `ChannelRecvH.recv()`.

    """
    since = time.monotonic()
    if abs(deadline) == math.inf:
        return _inf, since
    remaining = max(deadline - since, 0.0)
    return remaining, since


def mem_descr_msg(sdesc: bytes, data: bytes, clientid: int, hints: int, deadline: Optional[float] = None) -> Message:
    """Attaches to memory allocation given a serialized memory descriptor and
    writes the specified data. Yields a Dragon message created from the
    corresponding memory allocation.
    """
    # XXX Neither MemoryAlloc.attach() nor Message.create_from_mem() support
    # XXX a timeout or deadline.
    mem = MemoryAlloc.attach(sdesc)
    try:
        if mem.size == 0:
            # This is a zero-byte allocation. This indicates that an allocation
            # should be made from the pool of this zero-byte allocation for the
            # desired size.
            return mem_pool_msg(mem.pool, data, clientid, hints, deadline)

        if mem.size < len(data):
            raise ValueError(f"Memory allocation too small: {mem}")
        v = mem.get_memview()
        v[:] = data
        return Message.create_from_mem(mem, clientid, hints)
    except:
        mem.detach()
        raise


def mem_pool_msg(pool: MemoryPool, data: bytes, clientid: int, hints: int, deadline: Optional[float] = None) -> Message:
    """Creates Dragon message from data in specific pool's memory allocation.

    Deadline is measured relative to time.monotonic().
    """
    # XXX ChannelSendH.send_bytes() does not call Message.create_alloc() with
    # XXX a timeout or deadline; hence it is re-implemented here.
    timeout, _ = seconds_remaining(deadline)
    msg = Message.create_alloc(pool, len(data), clientid=clientid, hints=hints, timeout=timeout)
    try:
        v = msg.bytes_memview()
        v[:] = data
    except:
        msg.destroy(True)
        raise
    return msg


def create_msg(
    data: bytes,
    clientid: int,
    hints: int,
    channel: Optional[Channel] = None,
    deadline: Optional[float] = None,
    sdesc: Optional[bytes] = None,
) -> Message:
    """Creates a Dragon message from data.

    If a channel is specified, the message will be created from a memory
    allocation in the channel's memory pool with the given deadline. The
    deadline is measured relative to time.monotonic().

    If a serialized memory descriptor is specified, channel and deadline
    arguments are ignored, and the message will be created from the
    corresponding memory allocation.
    """
    if sdesc is None:
        assert channel is not None
        return mem_pool_msg(channel.default_alloc_pool, data, clientid, hints, deadline)
    else:
        return mem_descr_msg(sdesc, data, clientid, hints, deadline)


@contextmanager
def attach_channel(sdesc: bytes) -> Generator[Channel, None, None]:
    ch = Channel.attach(sdesc)
    try:
        yield ch
    finally:
        ch.detach()


@contextmanager
def open_handle(h: Union[ChannelRecvH, ChannelSendH]) -> Generator[Union[ChannelRecvH, ChannelSendH], None, None]:
    h.open()
    try:
        yield h
    finally:
        h.close()


def send_msg(
    channel_sdesc: bytes,
    clientid: int,
    hints: int,
    payload: bytes,
    deadline: float,
    mem_sd: Optional[bytes] = None,
    copy_on_send: bool = False,
    wait_mode: WaitMode = DEFAULT_WAIT_MODE,
) -> None:
    # TODO Should we cache attached channels and open handles?
    with attach_channel(channel_sdesc) as ch, open_handle(ch.sendh(wait_mode=wait_mode)) as h:
        msg = create_msg(payload, clientid, hints, ch, deadline, mem_sd)
        timeout, _ = seconds_remaining(deadline)

        h.send(
            msg,
            ownership=h.copy_on_send if copy_on_send else h.transfer_ownership_on_send,
            timeout=timeout,
        )


def recv_msg(channel_sdesc: bytes, deadline: float, wait_mode: WaitMode = DEFAULT_WAIT_MODE) -> Message:
    # TODO Should we cache attached channels and open handles?
    with attach_channel(channel_sdesc) as ch, open_handle(ch.recvh(wait_mode=wait_mode)) as h:
        timeout, _ = seconds_remaining(deadline)
        msg = h.recv(timeout=timeout)
        clientid = msg.clientid
        hints = msg.hints
        py_view = msg.tobytes()
        msg.destroy()
        return clientid, hints, py_view


def poll_channel(channel_sdesc: bytes, event_mask: int, deadline: float) -> (bool, int):
    # TODO Should we cache attached channels and open handles?
    with attach_channel(channel_sdesc) as ch:
        result = PollResult()
        timeout, _ = seconds_remaining(deadline)
        ok = ch.poll(timeout=timeout, event_mask=event_mask, poll_result=result)
        return ok, result
