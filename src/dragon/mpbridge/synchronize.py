"""Dragon's replacements for the synchronization primitives in Multiprocessing.
Except for `Condition`, all components are based on `dragon.native` component
and are implemented over one or more dragon channels.  `Lock` is patched with a
dummy `_SemLock` class, so `Condition` works out of the box.
"""

import multiprocessing.synchronize
from threading import BrokenBarrierError

import dragon.native.lock
import dragon.native.semaphore
import dragon.native.event
import dragon.native.barrier

# Lock


class _SemLock:
    """Fake Semlock to fool Multiprocessing"""

    def __init__(self, parent):
        self._parent = parent

    def _is_mine(self):
        return self._parent.is_mine()

    def _count(self):
        return self._parent._accesscount


class AugmentedDragonNativeLock(dragon.native.lock.Lock):
    """Augment the Dragon Native Lock so it can be used by Python Multiprocessing"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._semlock = _SemLock(self)

    def __setstate__(self, state):
        super().__setstate__(state)
        self._semlock = _SemLock(self)


class DragonLock(AugmentedDragonNativeLock):
    """A lock co-located on the same node by default as the creating process"""
    def __init__(self, *args, ctx, **kwargs):
        super().__init__(*args, **kwargs, recursive=False)


class DragonRLock(AugmentedDragonNativeLock):
    """A recursive lock co-located on the same node by default as the creating process"""
    def __init__(self, *args, ctx, **kwargs):
        super().__init__(*args, **kwargs, recursive=True)


class BaseImplLock(multiprocessing.synchronize.Lock):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class BaseImplRLock(multiprocessing.synchronize.RLock):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


# Condition


class DragonCondition(multiprocessing.synchronize.Condition):
    """A condition co-located on the same node by default as the creating process

    This implementation removes all references to _semlock and the test for assert_spawning.
    The _semlock interface is mapped onto Dragon's native Lock implementation.
    """

    def __getstate__(self):
        return (self._lock, self._sleeping_count, self._woken_count, self._wait_semaphore)

    def __repr__(self):
        try:
            num_waiters = self._sleeping_count.get_value() - self._woken_count.get_value()
        except Exception:
            num_waiters = "unknown"
        return "<%s(%s, %s)>" % (self.__class__.__name__, self._lock, num_waiters)


class BaseImplCondition(multiprocessing.synchronize.Condition):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


# Semaphore
class DragonSemaphore(dragon.native.semaphore.Semaphore):
    """A sempahore co-located on the same node by default as the creating process"""
    def __init__(self, *args, ctx, **kwargs):
        super().__init__(*args, **kwargs, bounded=False)


class DragonBoundedSemaphore(dragon.native.semaphore.Semaphore):
    """A bounded sempahore co-located on the same node by default as the creating process"""
    def __init__(self, *args, ctx, **kwargs):
        super().__init__(*args, **kwargs, bounded=True)


class BaseImplSemaphore(multiprocessing.synchronize.Semaphore):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class BaseImplBoundedSemaphore(multiprocessing.synchronize.BoundedSemaphore):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


# Event
class DragonEvent(dragon.native.event.Event):
    """An event co-located on the same node by default as the creating process"""
    def __init__(self, *args, ctx, **kwargs):
        super().__init__(*args, **kwargs)


class BaseImplEvent(multiprocessing.synchronize.Event):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


# Barrier


class DragonBarrier(dragon.native.barrier.Barrier):
    """A barrier co-located on the same node by default as the creating process"""
    def __init__(self, parties: int, action: callable = None, timeout: float = None, *, ctx=None):
        if timeout is not None:
            if timeout < 0:
                timeout = 0
        super().__init__(parties, action, timeout)

    def wait(self, timeout: float = None):
        if timeout is not None:
            if timeout < 0:
                timeout = 0
        try:
            return super().wait(timeout)
        # multiprocessing uses the threading exception
        except dragon.native.barrier.BrokenBarrierError:
            raise BrokenBarrierError


class BaseImplBarrier(multiprocessing.synchronize.Barrier):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


# Functions


def Lock(ctx=None, use_base_impl=False):
    if use_base_impl:
        return BaseImplLock(ctx=ctx)
    else:
        return DragonLock(ctx=ctx)


def RLock(ctx=None, use_base_impl=False):
    if use_base_impl:
        return BaseImplRLock(ctx=ctx)
    else:
        return DragonRLock(ctx=ctx)


def Condition(lock, ctx=None, use_base_impl=False):
    if use_base_impl:
        return BaseImplCondition(lock, ctx=ctx)
    else:
        return DragonCondition(lock, ctx=ctx)


def Semaphore(value, ctx=None, use_base_impl=False):
    if use_base_impl:
        return BaseImplSemaphore(value, ctx=ctx)
    else:
        return DragonSemaphore(value, ctx=ctx)


def BoundedSemaphore(value, ctx=None, use_base_impl=False):
    if use_base_impl:
        return BaseImplBoundedSemaphore(value, ctx=ctx)
    else:
        return DragonBoundedSemaphore(value, ctx=ctx)


def Event(ctx=None, use_base_impl=False):
    if use_base_impl:
        return BaseImplEvent(ctx=ctx)
    else:
        return DragonEvent(ctx=ctx)


def Barrier(parties, action=None, timeout=None, ctx=None, use_base_impl=False):
    if use_base_impl:
        print("Using base barrier and we should never see this", flush=True)
        return BaseImplBarrier(parties, action=action, timeout=timeout, ctx=ctx)
    else:
        return DragonBarrier(parties, action=action, timeout=timeout, ctx=ctx)
