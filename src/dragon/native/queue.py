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

from ..channels import (
    Channel,
    ChannelEmpty,
    ChannelFull,
    ChannelError,
    Many2ManyReadingChannelFile,
    Many2ManyWritingChannelFile,
)
from ..managed_memory import MemoryPool

from ..globalservices.channel import create, destroy, query, release_refcnt, get_refcnt
from ..infrastructure.facts import default_pool_muid_from_index
from ..infrastructure.parameters import this_process, POLICY_USER, Policy
from ..infrastructure.channel_desc import ChannelOptions
import dragon.utils as du

_DEF_MUID = default_pool_muid_from_index(this_process.index)

LOG = logging.getLogger(__name__)


class QueueError(Exception):
    pass


class Queue(object):
    """A Dragon native Queue relying on Dragon Channels.

    The interface resembles Python Multiprocessing.Queues. In particular, the
    queue can be initialized as joinable, which allows to join on the completion
    of a items. We have simplified the interface a little and changed a few oddities
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
        m_uid: int = _DEF_MUID,
        block_size: int = 2**16,  # 64 kbytes
        joinable: bool = False,
        policy: Policy = POLICY_USER,
        _ext_channel: object = None,
    ):
        """Init method

        :param maxsize: sets the upperbound limit on the number of items that can be placed in the queue, defaults to 100
        :type maxsize: int, optional
        :param m_uid: The m_uid of the memory pool to use, defaults to _DEF_MUID
        :type m_uid: int, optional
        :param block_size: Block size for the underlying channel, defaults to 64 kbytes
        :type block_size: int, optional
        :param joinable: If this queue should be joinable, defaults to False
        :type joinable: bool, optional
        :param policy: policy object, defaults to POLICY_USER
        :type policy: object, optional
        :param _ext_channel: non-local externally managed channel for testing purposes only, defaults to None
        :type _ext_channel: instance of dragon.channel, optional
        :raises QueueError: If something goes wrong during creation
        :raises ValueError: If a parameter is wrong
        :raises NotImplementedError: If a joinable queue is used with an external channel
        """

        if not isinstance(policy, Policy):
            raise ValueError("The class of service must be a dragon.infrastructure.parameters.Policy value.")

        self._policy = policy
        self._closed = False
        self._ext_channel = False
        self._buffer_pool = None
        self._read_adapter = None
        self._write_adapter = None
        self._channel = None
        # Helper variable to know when we need to attach/detach to/from an externally managed channel.
        # In the case of an externally managed channel, we need to attach to the channel only when the
        # queue instance is created from unpickling. When the queue instance is created by the user
        # via the constructor, then we do not need to attach/detach.
        self._unpickled_instance = False

        # if we are joinable, create channels to hold a counter and a event
        self._joinable = joinable

        self._read_adapter_event = None
        self._write_adapter_event = None
        self._read_adapter_count = None
        self._write_adapter_count = None

        if _ext_channel is None:  # create the channel internally

            the_options = ChannelOptions(ref_count=True)
            the_options.local_opts.block_size = block_size

            if maxsize > 0:
                the_options.local_opts.capacity = int(maxsize)
            else:
                LOG.debug(f"Maxsize was {maxsize}. Using default queue capacity of 100.")
                maxsize = 100

            try:
                descr = create(m_uid=m_uid, options=the_options)
                self._channel = Channel.attach(descr.sdesc)
            except Exception as e:
                self._closed = True
                raise QueueError(f"Could not create queue\n{e}")

        else:
            self._channel = _ext_channel
            if self._channel.is_local:
                raise ValueError(
                    f"Channel {self._channel} is an externally managed channel and is not considered as remote."
                )
            self._ext_channel = True
            self._buffer_pool = m_uid  # only in this case, m_uid is a MemoryPool object

        if self._joinable:

            if self._ext_channel:
                raise NotImplementedError("Joinable queues do not support testing with external channels")

            the_options.local_opts.capacity = 1
            try:
                cnt_descr = create(m_uid=m_uid, options=the_options)
                self._cnt_channel = Channel.attach(cnt_descr.sdesc)
                ev_descr = create(m_uid=m_uid, options=the_options)
                self._ev_channel = Channel.attach(ev_descr.sdesc)
            except Exception as e:
                self._closed = True
                raise QueueError(f"Could not create queue\n{e}")

            self._reset_task_counter()
            self._reset_event_channel()

            LOG.debug("Created queue {self!r}")

    def __getstate__(self):

        if self._closed:
            raise ValueError(f"Queue {self!r} is closed")

        if self._joinable:
            cnt_chan_cuid = self._cnt_channel.cuid
            ev_chan_cuid = self._ev_channel.cuid
        else:
            cnt_chan_cuid = None
            ev_chan_cuid = None

        return (
            self._channel.serialize(),
            self._channel.cuid,
            self._joinable,
            self._policy,
            self._ext_channel,
            cnt_chan_cuid,
            ev_chan_cuid,
        )

    def __setstate__(self, state):
        (
            ser_ch,
            q_chan_cuid,
            self._joinable,
            self._policy,
            self._ext_channel,
            cnt_chan_cuid,
            ev_chan_cuid,
        ) = state

        self._read_adapter = None
        self._write_adapter = None
        self._read_adapter_event = None
        self._write_adapter_event = None
        self._read_adapter_count = None
        self._write_adapter_count = None
        self._buffer_pool = None
        self._unpickled_instance = False
        self._channel = None
        self._closed = False

        try:
            if self._ext_channel:
                self._unpickled_instance = True
            else:
                get_refcnt(q_chan_cuid)

            self._channel = Channel.attach(ser_ch)

        except ChannelError as e:
            self._closed = True
            raise QueueError(f"Could not deserialize queue\n{e}")

        if self._joinable:  # attach to counter and event channels
            try:
                descr = query(cnt_chan_cuid)  # implicitly increases refcount
                self._cnt_channel = Channel.attach(descr.sdesc)

                descr = query(ev_chan_cuid)
                self._ev_channel = Channel.attach(descr.sdesc)

            except ChannelError as e:
                try:
                    release_refcnt(q_chan_cuid)
                    self._channel.detach()
                except:
                    pass
                self._closed = True
                raise QueueError(f"Could not deserialize joinable queue\n{e}")

        # figure out what should be the pool in which we will allocate the messages to send:
        # if the channel is local, then the pool is the channel pool
        # else the pool is the default pool of the caller
        try:
            if not self._channel.is_local:
                # if it is an externally managed channel, we have already provided a pool
                if not self._ext_channel:
                    self._buffer_pool = MemoryPool.attach(du.B64.str_to_bytes(this_process.default_pd))
        except Exception as e:
            try:
                release_refcnt(q_chan_cuid)
            except:
                pass
            self._closed = True
            raise QueueError(f"Could not deserialize queue\n{e}")

    def __del__(self):
        self._close()

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

        return self._channel.num_msgs == self._channel.capacity

    def empty(self) -> bool:
        """Return True if the queue is empty, False otherwise.
        This might not be reliable

        :raises ValueError: If the queue is closed
        :return: Wether or not the queue is empty
        :rtype: bool
        """

        if self._closed:
            raise ValueError(f"Queue {self!r} is closed")

        return not self._channel.poll(timeout=0)

    def size(self) -> int:
        """Return the approximate size of the queue. This number may not be reliable.

        :raises ValueError: If the queue is closed
        :return: approximate number of items in the queue
        :rtype: int
        """

        return self._size()

    def get(self, block: bool = True, timeout: float = None) -> object:
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

        return self._get(block=block, timeout=timeout)

    def get_nowait(self) -> object:
        """Equivalent to get(False).

        :return: The next item in the queue
        :rtype: object
        """
        return self.get(False)

    def put(self, obj, block: bool = True, timeout: float = None) -> None:
        """Puts the serialization of an object onto the queue.
        If the queue is joinable, require one more call to task_done(), for
        join() to unblock.

        :param obj: object to serialize and put
        :param block: Whether to block
        :param timeout: Timeout, if blocking.  None means infinity, default
        :return: None
        """

        self._put(obj, block=block, timeout=timeout)

    def put_nowait(self, obj) -> None:
        """Equivalent to put(obj, False).

        :param obj: object to serialize and put
        :type obj: object
        :return: None
        """
        return self.put(obj, False)

    def task_done(self) -> None:
        """Indicate that a formerly enqueued task is complete. Used by queue consumers.
        Only for joinable queues.

        :raises QueueError: If this queue is not joinable or there were no pending tasks.
        :raises ValueError: If called more times than there were items in the queue.
        """

        if not self._joinable:
            raise QueueError("Queue is not joinable.")

        if self._closed:
            raise ValueError(f"Queue {self!r} is closed")

        counter = self._decrease_task_counter()

        if counter < 0:
            self._reset_task_counter()
            raise QueueError("All tasks are already done")

        if counter == 0:  # release all processes currently joining the queue
            self._set_event_channel()

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

        self._ev_channel.poll(timeout=timeout)

    @property
    def closed(self) -> bool:
        return self._closed

    # for dragon wait

    def poll(self, timeout: float = 0) -> None:
        """Block if the queue is empty
        :param timeout: time to wait in seconds, defaults to 0
        :type timeout: float, optional
        :return: None
        """
        return self._poll(timeout=timeout)

    def destroy(self) -> None:
        """Remove underlying channel right now."""
        self._close()
        try:
            destroy(self._channel.cuid)
        except:
            pass

    def thread_wait(self, timeout: float, done_ev: object, ready: list) -> None:
        """Thread waiter signaling with an ev."""

        return self._thread_wait(timeout, done_ev, ready)

    # private methods

    def _setup_write_adapter(self):
        self._write_adapter = Many2ManyWritingChannelFile(
            self._channel,
            self._buffer_pool,
            wait_mode=self._policy.wait_mode,
            return_mode=self._policy.return_when,
        )

    def _setup_read_adapter(self):
        self._read_adapter = Many2ManyReadingChannelFile(self._channel, wait_mode=self._policy.wait_mode)

    def _setup_write_adapter_event(self):
        self._write_adapter_event = Many2ManyWritingChannelFile(
            self._ev_channel,
            self._buffer_pool,
            wait_mode=self._policy.wait_mode,
            return_mode=self._policy.return_when,
        )

    def _setup_read_adapter_event(self):
        self._read_adapter_event = Many2ManyReadingChannelFile(self._ev_channel, wait_mode=self._policy.wait_mode)

    def _setup_write_adapter_count(self):
        self._write_adapter_count = Many2ManyWritingChannelFile(
            self._cnt_channel,
            self._buffer_pool,
            wait_mode=self._policy.wait_mode,
            return_mode=self._policy.return_when,
        )

    def _setup_read_adapter_count(self):
        self._read_adapter_count = Many2ManyReadingChannelFile(self._cnt_channel, wait_mode=self._policy.wait_mode)

    def _call_read_adapter(self, adapter, block, timeout):
        try:
            adapter.open()
            adapter.set_adapter_timeout(block, timeout)
            payload = cp.load(adapter)
        except (ChannelEmpty, TimeoutError):
            raise queue.Empty
        except Exception as e:
            raise QueueError(f"Could not get the object from queue.\n{e}")
        finally:
            adapter.close()

        return payload

    def _call_write_adapter(self, obj, adapter, block, timeout):
        try:
            adapter.open()
            adapter.set_adapter_timeout(block, timeout)
            cp.dump(obj, file=adapter, protocol=5)
        except (ChannelFull, TimeoutError):
            raise queue.Full
        except AttributeError:
            # needed for:
            # WithProcessesTestQueue.test_queue_feeder_on_queue_feeder_error
            # and WithProcessesTestQueue.test_queue_feeder_donot_stop_onexc
            raise AttributeError
        except Exception as e:
            raise QueueError(f"Could not put the object to queue.\n{e}")
        finally:
            adapter.close()

    def _get(self, block=True, timeout=None):

        if self._closed:
            raise ValueError(f"Queue {self!r} is closed")

        try:
            if self._read_adapter is None:
                self._setup_read_adapter()
            payload = self._call_read_adapter(self._read_adapter, block, timeout)
        except (ChannelEmpty, TimeoutError):
            raise queue.Empty

        return payload

    def _put(self, obj, block=True, timeout=None):

        if self._closed:
            raise ValueError(f"Queue {self!r} is closed")
        try:
            if self._write_adapter is None:
                self._setup_write_adapter()
            self._call_write_adapter(obj, self._write_adapter, block, timeout)
        except (ChannelFull, TimeoutError):
            raise queue.Full

        if self._joinable:
            counter = self._increase_task_counter()

            if counter == 1:
                self._reset_event_channel()

    def _poll(self, timeout: float = 0):
        return self._channel.poll(timeout=timeout)

    def _close(self):
        if not self._closed:
            self._closed = True

            if not self._ext_channel:
                try:
                    release_refcnt(self._channel.cuid)
                    self._channel.detach()
                except Exception as e:
                    pass  # Could not complete release of refcount or detach.

            elif self._ext_channel and self._unpickled_instance:
                try:
                    self._channel.detach()  # this should be revisited once refcounting fully works as it should not be necessary then
                except Exception as e:
                    pass  # We couldn't detach from externally managed channel.
            if self._joinable:
                try:
                    release_refcnt(self._cnt_channel.cuid)
                    self._cnt_channel.detach()
                    release_refcnt(self._ev_channel.cuid)
                    self._ev_channel.detach()
                except Exception as e:
                    pass  # Joinable queue: there was a problem with releasing channel refcount or while detaching from channel

        if self._buffer_pool is not None:
            try:
                self._buffer_pool.detach()
            except Exception as e:
                LOG.debug(f"We couldn't detach from queue's buffer pool. {e=}")

    def _size(self) -> int:
        if self._closed:
            raise ValueError(f"Queue {self!r} is closed")

        return self._channel.num_msgs

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

    def _reset_event_channel(self) -> None:
        if self._ev_channel.num_msgs > 0:
            if self._read_adapter_event is None:
                self._setup_read_adapter_event()
            _ = self._call_read_adapter(self._read_adapter_event, False, None)

    def _set_event_channel(self) -> None:
        if self._write_adapter_event is None:
            self._setup_write_adapter_event()
        self._call_write_adapter(1, self._write_adapter_event, False, None)

    def _reset_task_counter(self) -> None:
        if self._cnt_channel.num_msgs > 0:
            if self._read_adapter_count is None:
                self._setup_read_adapter_count()
            counter = self._call_read_adapter(self._read_adapter_count, True, None)

        counter = 0

        if self._write_adapter_count is None:
            self._setup_write_adapter_count()
        self._call_write_adapter(counter, self._write_adapter_count, True, None)

    def _increase_task_counter(self) -> int:
        if self._read_adapter_count is None:
            self._setup_read_adapter_count()
        counter = self._call_read_adapter(self._read_adapter_count, True, None)

        counter += 1

        if self._write_adapter_count is None:
            self._setup_write_adapter_count()
        # with self._write_adapter_count as adapter:
        #     adapter.set_adapter_timeout(True, None)
        #     pickle.dump(counter, file=adapter, protocol=5)
        self._call_write_adapter(counter, self._write_adapter_count, True, None)

        return counter

    def _decrease_task_counter(self) -> int:
        if self._read_adapter_count is None:
            self._setup_read_adapter_count()
        counter = self._call_read_adapter(self._read_adapter_count, True, None)

        counter -= 1
        if self._write_adapter_count is None:
            self._setup_write_adapter_count()
        self._call_write_adapter(counter, self._write_adapter_count, True, None)
        return counter
