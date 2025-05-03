"""Dragon's replacement of Multiprocessing Queue objects based on Channels."""

import multiprocessing.queues
import logging
import pickle
import queue

from . import DragonPopen
from ..channels import ChannelError, Many2ManyWritingChannelFile
from dragon.native.queue import Queue as NativeQueue

LOG = logging.getLogger(__name__)


def restrict_public_api(public_api: list) -> callable:
    """ "Decorator that removes public methods of the decorated class
    not in public_api. To do so it changes the namespace the first parent & grandparent
    class. Breaks inheritance.

    :param public_api: contains the names of all valid public attributes and methods
    :type public_api: list
    :return: returns the cleaned class
    :rtype: type
    """

    def ancestor_method_remover(cls: type) -> type:

        parent_class = cls.__bases__[0]

        grandparent_class = parent_class.__bases__[0]

        grandparent_namespace = dict(grandparent_class.__dict__)

        attr = [item for item in grandparent_namespace.keys() if not item.startswith("_")]
        for name in attr:
            if name not in public_api:
                LOG.debug(f"Removing {name} from namespace of {cls.__name__}")
                _ = grandparent_namespace.pop(name)

        _ = grandparent_namespace.pop("__dict__")  # do not include a false __dict__

        new_grandparent_class = type(
            grandparent_class.__name__,
            grandparent_class.__bases__,
            grandparent_namespace,
        )

        parent_namespace = dict(parent_class.__dict__)

        attr = [item for item in parent_namespace.keys() if not item.startswith("_")]
        for name in attr:
            if name not in public_api:
                LOG.debug(f"Removing {name} from namespace of {cls.__name__}")
                _ = parent_namespace.pop(name)

        new_parent_class = type(
            parent_class.__name__,
            (new_grandparent_class,) + parent_class.__bases__[1:],
            parent_namespace,
        )

        cls.__bases__ = (new_parent_class,) + cls.__bases__[1:]

        return cls

    return ancestor_method_remover


class FakeConnectionHandle:
    """A placeholder connection handle for a queue that has been reset"""

    # would like to use types.SimpleNamespace, but this is unhashable.

    pass


class PatchedDragonNativeQueue(NativeQueue):
    """The Dragon native queue including patches for Python Multiprocessing
    The API has to exactly match the Multiprocessing.queues.Queue API.
    """

    def __init__(self, *args, **kwargs):  # need to mimic Multiprocessing API here
        NativeQueue.__init__(self, *args, **kwargs)  # cannot call super() here
        self._reset()

    def __setstate__(self, *args, **kwargs):
        NativeQueue.__setstate__(self, *args, **kwargs)
        self._reset()

    def qsize(self) -> int:
        """Return the approximate size of the queue. This number may not be reliable.

        :raises ValueError: If the queue is closed
        :return: approximate number of items in the queue
        :rtype: int
        """

        return self._size()

    def put(self, obj, block=True, timeout=None) -> None:
        """Puts the serialization of an object onto the queue.
        This version includes patches for the multiprocessing unit tests

        :param obj: object to serialize and put
        :param block: Whether to block
        :param timeout: Timeout, if blocking.  None means infinity, default
        :return: None
        """
        try:
            self._put(obj, block=block, timeout=timeout)
        except AttributeError as e:  # WithProcessesTestQueue.test_queue_feeder_on_queue_feeder_error
            # and WithProcessesTestQueue.test_queue_feeder_donot_stop_onexc
            self._on_queue_feeder_error(AttributeError(e), obj)

    def close(self) -> None:
        """Indicate that no more data will be put on this queue by the current process.
        The refcount of the background channel will be released and the channel might be
        removed, if no other processes are holding it.
        This version includes a patch for the multiprocessing unit tests.
        """

        self._close()

        # TestSimpleQueue.test_closed
        self._writer.closed = True
        self._reader.closed = True

    def join_thread(self) -> None:
        """This method is a no-op for Dragon based multiprocessing and exists
        solely for compatibility with the queues.Queue API.
        """
        pass

    def cancel_join_thread(self):
        """This method is a no-op for Dragon based multiprocessing and exists
        solely for compatibility with the queues.Queue API.
        """
        pass

    def _reset(self):
        """Add a fake connection object below the queue for Multiprocessing"""

        self._writer = FakeConnectionHandle()
        self._writer.close = lambda: None
        self._writer.closed = False
        self._writer.send = self.put

        self._reader = FakeConnectionHandle()
        self._reader.close = lambda: None
        self._reader.closed = False
        self._reader.poll = self._poll
        self._reader.recv = self.get
        self._reader.thread_wait = self._thread_wait

        self._rlock = FakeConnectionHandle()
        self._rlock.acquire = lambda: None

    @staticmethod
    def _on_queue_feeder_error(e, obj):
        """
        Private API hook called when feeding data in the background thread
        raises an exception.  For overriding by concurrent.futures.
        """
        # see WithProcessesTestQueue.test_queue_feeder_on_queue_feeder_error

        import traceback

        traceback.print_exc()


_QUEUE_API = [
    "cancel_join_thread",
    "close",
    "empty",
    "full",
    "get",
    "get_nowait",
    "join_thread",
    "put",
    "put_nowait",
    "qsize",
]


@restrict_public_api(_QUEUE_API)
class DragonQueue(PatchedDragonNativeQueue):
    """A queue co-located on the same node by default as the creating process"""
    def __init__(self, *args, ctx=None, **kwargs):
        super().__init__(*args, joinable=False, **kwargs)


_JOINABLE_QUEUE_API = [
    "cancel_join_thread",
    "close",
    "empty",
    "full",
    "get",
    "get_nowait",
    "join",
    "join_thread",
    "put",
    "put_nowait",
    "qsize",
    "task_done",
]


@restrict_public_api(_JOINABLE_QUEUE_API)
class DragonJoinableQueue(PatchedDragonNativeQueue):
    """A jonable queue co-located on the same node by default as the creating process"""
    def __init__(self, *args, ctx=None, **kwargs):
        super().__init__(*args, joinable=True, **kwargs)


_SIMPLE_QUEUE_API = ["close", "empty", "get", "put"]


@restrict_public_api(_SIMPLE_QUEUE_API)
class DragonSimpleQueue(PatchedDragonNativeQueue):
    """A simplified queue co-located on the same node by default as the creating process"""
    def __init__(self, *args, ctx=None, **kwargs):
        super().__init__(*args, joinable=False, **kwargs)


class BaseImplQueue(multiprocessing.queues.Queue):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # patches internal dragon.infrastructure.connection.Connection
        # to not send EOT when it is closed.
        self._writer.ghost = True

    def _poll(self, timeout=0):  # for dragon wait
        return not self.empty()


class BaseImplJoinableQueue(multiprocessing.queues.JoinableQueue):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # patches internal dragon.infrastructure.connection.Connection
        # to not send EOT when it is closed.
        self._writer.ghost = True

    def _poll(self, timeout=0):  # for dragon wait
        return not self.empty()


class BaseImplSimpleQueue(multiprocessing.queues.SimpleQueue):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # patches internal dragon.infrastructure.connection.Connection
        # to not send EOT when it is closed.
        self._writer.ghost = True

    def _poll(self, timeout=0):  # for dragon wait
        return not self.empty()


def Queue(maxsize=0, *, ctx=None, use_base_impl=True):
    if use_base_impl:
        return BaseImplQueue(maxsize, ctx)
    else:

        if ctx is not None:
            assert DragonPopen.method == ctx._name

        # NOTE: when maxsize=0, local_opts.capacity is set to 100 (default)
        # whereas in the mp documentation, maxsize equal to zero means
        # infinite queue size
        if maxsize <= 0:
            maxsize = 100

        return DragonQueue(maxsize, ctx=ctx)


def JoinableQueue(maxsize=0, *, ctx=None, use_base_impl=True):
    if use_base_impl:
        return BaseImplJoinableQueue(maxsize, ctx)
    else:

        if ctx is not None:
            assert DragonPopen.method == ctx._name

        if maxsize <= 0:
            maxsize = 100

        return DragonJoinableQueue(maxsize, ctx=ctx)


def SimpleQueue(*, ctx=None, use_base_impl=True):
    if use_base_impl:
        return BaseImplSimpleQueue(ctx)
    else:
        if ctx is not None:
            assert DragonPopen.method == ctx._name

        return DragonSimpleQueue(ctx=ctx)
