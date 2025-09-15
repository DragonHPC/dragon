"""The Dragon native queue provides ordered object communication between processes.

This is Dragon's specialized implementations of a Queue, working
in both single and multi node settings. The API is similar to Multiprocessing.Queue
but has a number of extensions and simplifications.

The implementation relies on Dragon GS Client API and the Dragon runtime libraries.
"""

import cloudpickle as cp
import queue
import time
import logging

from ..channels import Channel, ChannelEmpty, ChannelFull
from ..managed_memory import MemoryPool
from ..utils import b64encode
from ..globalservices.channel import create, destroy, release_refcnt, get_refcnt, query
from ..infrastructure.parameters import POLICY_USER, Policy
from ..infrastructure.channel_desc import ChannelOptions
import dragon.fli as fli

LOG = logging.getLogger(__name__)


class QueueError(Exception):
    pass


def _mk_channel(*, pool=None, block_size=None, capacity=100, policy=None):
    if pool is None:
        pool = MemoryPool.attach_default()

    if pool is not None:
        muid = pool.muid
    else:
        muid = None

    the_options = ChannelOptions(ref_count=True)
    the_options.local_opts.block_size = block_size
    the_options.local_opts.capacity = capacity

    try:
        descr = create(m_uid=muid, options=the_options, policy=policy)
        channel = Channel.attach(descr.sdesc)
        return channel
    except Exception as e:
        raise QueueError(f"Could not create queue main channel.\n{e}")


def _mk_sem_channel(*, pool=None, block_size=None, policy=None):
    if pool is None:
        pool = MemoryPool.attach_default()

    if pool is not None:
        muid = pool.muid
    else:
        muid = None

    local_opts = {"semaphore": True, "bounded_semaphore": False, "initial_sem_value": 0}
    the_options = ChannelOptions(ref_count=True, local_opts=local_opts)

    try:
        descr = create(m_uid=muid, options=the_options, policy=policy)
        sem_channel = Channel.attach(descr.sdesc)
        return sem_channel
    except Exception as e:
        raise QueueError(f"Could not create queue semaphore channel.\n{e}")


class Queue:
    """A Dragon native Queue relying on Dragon Channels.

    The interface resembles Python Multiprocessing.Queue. In particular, the
    queue can be initialized as joinable, which enables joining on the completion
    of items. We have simplified the interface a little and changed a few oddities
    like renaming `qsize()` into `size()`.

    The usual `queue.Empty` and `queue.Full` exceptions from
    the Python standard library’s queue module are raised to signal timeouts.

    This implementation is intended to be a simple and high performance
    implementation for use on both single node and multiple nodes.
    """

    def __init__(
        self,
        maxsize: int = 100,
        *,
        pool: object = None,
        block_size: int = 2**16,  # 64 kbytes
        joinable: bool = False,
        buffered: bool = True,
        policy: Policy = None,
        num_streams: int = 0,
        main_channel: object = None,
        mgr_channel: object = None,
        sem_channel: object = None,
        strm_channels: list[object] = None,
        pickler=cp,
    ):
        """Init method

        :param maxsize: sets the upperbound limit on the number of items that can be placed in the queue, defaults to 100.
        While this can be provided, the queue provides blocking calls and if blocking puts are done, more than 100 items
        may be added at a time which will result in blocking the put caller until room is availabe.
        :type maxsize: int, optional
        :param pool: The memory pool to use, defaults to the default pool on the node where the queue is created.
        :type pool: object, optional
        :param block_size: Block size for the underlying main and manager channels, defaults to 64Kb.
        :type block_size: int, optional
        :param joinable: If this queue should be joinable, defaults to False
        :type joinable: bool, optional
        :param buffered: This queue is to be a buffered queue where all data is buffered internally so receivers do only one
        get operation for a complete transmission from a sender.
        :param policy: policy object, defaults to None. If specified with a specific node/host, the policy dictates
        where any internal channels are created. They will be created in the default pool of the specified node.
        :type policy: object, optional
        :param num_streams: The number of stream channels to be created for a managed FLI queue. If greater than zero, then a
        main and manager channel will be automatically created and managed internally if not provided.
        :param main_channel: An externally managed channel. Defaults to None in which case it will be automatically created
        if num_streams is greater than 0. You need a main channel when processes that put values into the queue provide
        their own stream channel during send handle open or when there are internally managed stream channels.
        :type channel: instance of dragon.channel, optional
        :param mgr_channel: An externally managed channel. Defaults to None in which case it will be automatically created if
        num_streams is greater than 0. You need a manager channel when processes that get values from the queue provide
        their own stream channel during receive handle open or when there are internally managed stream channels.
        :type channel: instance of dragon.channel, optional
        :param sem_channel: An externally managed semaphore channel. If provided, it must have been created as a
        semaphore channel. This is only needed if creating a task queue. If provided, then joinable must also be
        True. If joinable is True and not provide, a semaphore channel will be created and managed internally.
        :type channel: instance of dragon.channel, optional
        :param strm_channels: A list of stream channel objects to place in the manager channel. If provided then the num_streams
        value is ignored. If not provided and the number of stream channels is 0 in an unbuffered queue, then a steram channel
        must be provided when sending or receiving. If stream channels is provided to an unbuffered queue, a main channel and
        manager channel will be created and managed internally if not provided.
        :param pickler: A custom pickler may be provided. It must support the dump and load api calls similar
        to cloudpickle. Cloudpickle is the default if none is provided.
        :raises QueueError: If something goes wrong during creation
        :raises ValueError: If a parameter is wrong
        :raises NotImplementedError: If a joinable queue is used with an external channel
        """
        self._closed = True

        # NOTE: when maxsize=0 it is reset to 100 (default)
        # because in the mp documentation, maxsize equal to zero means
        # infinite queue size. We achieve an effective infinite queue
        # size because of blocking puts and gets.
        if maxsize <= 0:
            maxsize = 100

        if pool is None:
            pool = MemoryPool.attach_default()

        self._maxsize = maxsize
        self._pool = pool
        self._block_size = block_size
        self._joinable = joinable
        self._buffered = buffered
        self._policy = policy
        self._num_streams = num_streams
        self._main_channel = main_channel
        self._mgr_channel = mgr_channel
        self._sem_channel = sem_channel
        self._main_channel_internally_managed = False
        self._mgr_channel_internally_managed = False
        self._sem_channel_internally_managed = False
        self._num_managed_strm_channels = 0
        self._strm_channels = [] if strm_channels is None else strm_channels
        self._pickler = pickler
        self._node_index = None  # Don't set it here. That way we won't talk to GS unless asked to.

        # Now detect what needs to be managed internally and if there are conflicts in
        # the combination of provided arguments.

        if buffered:
            # This is a queue that will use one channel and buffer data that is put to it.

            if len(self._strm_channels) > 0:
                raise ValueError("You cannot specify a buffered queue and also provide a list of stream channels.")

            if num_streams != 0:
                raise ValueError(
                    "You cannot specify a buffered queue also specify a non-zero number of stream channels."
                )

            if mgr_channel is not None:
                raise ValueError(
                    "You cannot specify a buffered queue and also a manager channel. A manager channel is not needed when the Queue is buffered."
                )

            if main_channel is None:
                self._main_channel = _mk_channel(
                    pool=self._pool, block_size=block_size, capacity=maxsize, policy=policy
                )
                self._main_channel_internally_managed = True
        else:
            # This is not a buffered Queue. So there will be stream channels either provided on the
            # constructor or when putting and getting values, depending on the configuration.
            if len(self._strm_channels) > 0 or num_streams > 0:
                if num_streams == 0:
                    num_streams = len(self._strm_channels)

                # Then we must have a manager and a main channel if stream channels are managed by the FLI.
                if main_channel is None:
                    self._main_channel = _mk_channel(
                        pool=self._pool, block_size=None, capacity=num_streams, policy=policy
                    )
                    self._main_channel_internally_managed = True

                if mgr_channel is None:
                    self._mgr_channel = _mk_channel(
                        pool=self._pool, block_size=None, capacity=num_streams, policy=policy
                    )
                    self._mgr_channel_internally_managed = True

                while len(self._strm_channels) < num_streams:
                    self._strm_channels.append(
                        _mk_channel(pool=self._pool, block_size=self._block_size, capacity=self._maxsize, policy=policy)
                    )
                    self._num_managed_strm_channels += 1

        if joinable:
            if sem_channel is None:
                self._sem_channel = _mk_sem_channel(pool=self._pool, policy=policy)
                self._sem_channel_internally_managed = True

        self._fli = fli.FLInterface(
            main_ch=self._main_channel,
            manager_ch=self._mgr_channel,
            sem_ch=self._sem_channel,
            pool=self._pool,
            stream_channels=self._strm_channels,
            use_buffered_protocol=self._buffered,
        )

        self._serialized_fli = b64encode(self._fli.serialize())

        LOG.debug("Created queue {self!r}")
        self._closed = False

    def __getstate__(self):

        if self._closed:
            raise ValueError(f"Queue {self!r} is closed")

        pickler = cp.dumps(self._pickler)

        return (
            self._maxsize,
            self._pool,
            self._block_size,
            self._joinable,
            self._buffered,
            self._policy,
            self._num_streams,
            self._main_channel,
            self._mgr_channel,
            self._sem_channel,
            self._main_channel_internally_managed,
            self._mgr_channel_internally_managed,
            self._sem_channel_internally_managed,
            self._num_managed_strm_channels,
            self._strm_channels,
            self._fli,
            self._node_index,
            pickler,
        )

    def __setstate__(self, state):
        self._closed = True
        (
            self._maxsize,
            self._pool,
            self._block_size,
            self._joinable,
            self._buffered,
            self._policy,
            self._num_streams,
            self._main_channel,
            self._mgr_channel,
            self._sem_channel,
            self._main_channel_internally_managed,
            self._mgr_channel_internally_managed,
            self._sem_channel_internally_managed,
            self._num_managed_strm_channels,
            self._strm_channels,
            self._fli,
            self._node_index,
            pickler,
        ) = state

        self._pickler = cp.loads(pickler)

        self._serialized_fli = b64encode(self._fli.serialize())

        if not self._pool.is_local:
            self._pool = MemoryPool.attach_default()

        if self._main_channel_internally_managed:
            get_refcnt(self._main_channel.cuid)

        if self._mgr_channel_internally_managed:
            get_refcnt(self._mgr_channel.cuid)

        if self._sem_channel_internally_managed:
            get_refcnt(self._sem_channel.cuid)

        for i in range(self._num_managed_strm_channels):
            # the last part of the list is the internally managed streams.
            get_refcnt(self._strm_channels[-1 - i].cuid)

        self._closed = False

    def __del__(self):
        self._close()

    def _thread_wait(self, timeout, done_ev, ready):
        if timeout is None:
            timeout = 1000000
        start = time.monotonic()
        delta = min(0.01, timeout)  # TODO: figure out the value for delta
        timed_out = False

        if not done_ev.is_set():
            while not timed_out and not done_ev.is_set():
                done_res = self.poll(delta)
                if not done_ev.is_set():
                    if done_res:
                        ready.append(self)
                        done_ev.set()
                timed_out = (time.monotonic() - start) > timeout
            done_ev.set()  # if not set here, timed out.

    def close(self) -> None:
        """Indicate that no more data will be put on this queue by the current process.
        The refcount of the background channel will be released and the channel might be
        removed, if no other processes are holding it.
        """
        self._close()

    def full(self) -> bool:
        """Return True if the queue is full, False otherwise.

        :raises ValueError: If the queue is closed
        :return: Wether or not the queue is full
        :rtype: bool
        """

        if self._closed:
            raise ValueError(f"Queue {self!r} is closed")

        return self._main_channel.num_msgs == self._main_channel.capacity

    def _poll(self, timeout=None):
        if self._closed:
            raise ValueError(f"Queue {self!r} is closed")

        return self._main_channel.poll(timeout=timeout)

    def empty(self) -> bool:
        """Return True if the queue is empty, False otherwise.
        This might not be reliable

        :raises ValueError: If the queue is closed
        :return: Wether or not the queue is empty
        :rtype: bool
        """

        return not self._poll(0)

    def size(self) -> int:
        """Return the approximate size of the queue. This number may not be reliable.

        :raises ValueError: If the queue is closed
        :return: approximate number of items in the queue
        :rtype: int
        """
        if self._closed:
            raise ValueError(f"Queue {self!r} is closed")

        return self._main_channel.num_msgs

    def get(self, block: bool = True, timeout: float = None, *, stream_channel=None) -> object:
        """Remove and return an item from the queue.

        :param block: Make this call blocking, defaults to True
        :type block: bool, optional
        :param timeout: number of seconds to block, defaults to None
        :type timeout: float, optional
        :raises ValueError: If queue is closed
        :raises queue.Empty: If queue is empty
        :return: The next item in the queue
        :rtype: object
        """
        if self._closed:
            raise ValueError(f"Queue {self!r} is closed")

        if not block:
            timeout = 0

        try:
            payload = None
            with self._fli.recvh(stream_channel=stream_channel, timeout=timeout) as recvh:
                payload = self._pickler.load(file=fli.PickleReadAdapter(recvh=recvh, timeout=timeout))

        except (ChannelEmpty, TimeoutError, EOFError):
            raise queue.Empty

        return payload

    def get_nowait(self, *, stream_channel=None) -> object:
        """Equivalent to get(False).

        :return: The next item in the queue
        :rtype: object
        """
        return self.get(False, stream_channel=stream_channel)

    def put(self, obj, block: bool = True, timeout: float = None, *, stream_channel=None, flush=False) -> None:
        """Puts the serialization of an object onto the queue.
        If the queue is joinable, require one more call to task_done(), for
        join() to unblock.

        :param obj: object to serialize and put
        :param block: Whether to block
        :param timeout: Timeout, if blocking.  None means infinity, default
        :return: None
        """
        if self._closed:
            raise ValueError(f"Queue {self!r} is closed")

        if not block:
            timeout = 0

        try:
            with self._fli.sendh(stream_channel=stream_channel, timeout=timeout, flush=flush) as sendh:
                self._pickler.dump(
                    obj, file=fli.PickleWriteAdapter(sendh=sendh, buffer=self._buffered, timeout=timeout)
                )

        except (ChannelFull, TimeoutError):
            raise queue.Full

        if self._joinable:
            self._fli.new_task(timeout=timeout)

    def put_nowait(self, obj, *, stream_channel=None, flush=False) -> None:
        """Equivalent to put(obj, False).

        :param obj: object to serialize and put
        :type obj: object
        :return: None
        """
        # here we needed to be explicit about which method was being called because
        # subclasses redefine put and may have different arguments. So we make sure
        # we are calling the put in this class here.
        return Queue.put(self, obj, False, stream_channel=stream_channel, flush=flush)

    def task_done(self, timeout: float = None) -> None:
        """Indicate that a formerly enqueued task is complete. Used by queue consumers.
        Only for joinable queues.

        :raises QueueError: If this queue is not joinable or there were no pending tasks.
        :raises ValueError: If called more times than there were items in the queue.
        """

        if not self._joinable:
            raise QueueError("Queue is not joinable.")

        if self._closed:
            raise ValueError(f"Queue {self!r} is closed")

        self._fli.task_done(timeout)

    def join(self, *args, timeout: float = None, **kwargs) -> None:
        """Block until all items in the queue have been gotten and processed.

        If a join() is blocking, it will resume when all items have been processed
        (meaning that a task_done() call was received for every item that had been put() into
        the queue).

        :param timeout: time to wait on the join in sec, optional
        :type timeout: float
        :return: None
        """

        if not self._joinable:
            raise QueueError(f"Queue is not joinable.")

        if self._closed:
            raise ValueError(f"Queue {self!r} is closed")

        self._fli.join(timeout)

    @property
    def closed(self) -> bool:
        """Close the queue so nothing can be put over it.
        :return: None
        """
        return self._closed

    @property
    def node(self) -> int:
        """Return the node index within the current run-time
           where the main channel of this Queue resides.
        :rtype: int
        :return: The integer node index.
        """
        if self._node_index is None:
            cdesc = query(self._main_channel.cuid)  # reaching inside Queue. Only for test purposes
            self._node_index = cdesc.node

        return self._node_index

    # for dragon wait

    def poll(self, timeout: float = 0) -> None:
        """Block if the queue is empty
        :param timeout: time to wait in seconds, defaults to 0
        :type timeout: float, optional
        :return: None
        """
        return self._main_channel.poll(timeout=timeout)

    def destroy(self) -> None:
        """Destroy the queue immediately."""
        self._close()
        try:
            if self._main_channel_internally_managed:
                destroy(self._main_channel.cuid)
        except:
            pass

        try:
            if self._mgr_channel_internally_managed:
                destroy(self._mgr_channel.cuid)
        except:
            pass

        try:
            if self._sem_channel_internally_managed:
                destroy(self._sem_channel.cuid)
        except:
            pass

        for i in range(self._num_managed_strm_channels):
            # the last part of the list is the internally managed streams.
            try:
                destroy(self._strm_channels[-1 - i])
            except:
                pass

    # private method
    def _close(self):
        try:
            if not self._closed:
                self._closed = True

                try:
                    if self._main_channel_internally_managed:
                        release_refcnt(self._main_channel.cuid)
                except:
                    pass

                try:
                    if self._mgr_channel_internally_managed:
                        release_refcnt(self._mgr_channel.cuid)
                except:
                    pass

                try:
                    if self._sem_channel_internally_managed:
                        release_refcnt(self._sem_channel.cuid)
                except:
                    pass

                for i in range(self._num_managed_strm_channels):
                    # the last part of the list is the internally managed streams.
                    try:
                        release_refcnt(self._strm_channels[-1 - i])
                    except:
                        pass

                if self._pool is not None:
                    try:
                        self._pool.detach()
                    except Exception as e:
                        LOG.debug(f"We couldn't detach from queue's buffer pool. {e=}")
        except:
            # An exception here would indicate _closed was not defined. If this is so then we didn't get very far.
            self._closed = True

    def thread_wait(self, timeout: float, done_ev: object, ready: list) -> None:
        """Thread waiter signaling with an ev."""

        return self._thread_wait(timeout, done_ev, ready)

    def serialize(self):
        """
        Return a serialized, base64 encoded, string that may be used by C++
        code to attach to this queue. Any process attaching to this queue
        does not participate in the ref counting of the queue. In other
        words, the lifetime of the queue is managed by the Python process
        that creates it or other Python processes that have handles to it.
        C++ code that wishes to use it can, but the Python process must live/
        wait to exit until the C++ code is done with it.

        :return: A serialized, base64 encoded string that can be used
        to attach to the queue by the C++ Queue implementation.
        :rtype: str
        """

        return self._serialized_fli
