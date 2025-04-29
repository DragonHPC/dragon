"""Dragon's replacement for Multiprocessing Pool.

By default this uses the dragon native pool and sets
`DRAGON_BASEPOOL="NATIVE"`. The private api for this class is still under
development. To revert to the version based on the `multiprocessing.Pool` class
with a patched terminate_pool method, set `DRAGON_BASEPOOL="PATCHED"` in the
environment.
"""

import multiprocessing.pool
from multiprocessing import get_start_method
from multiprocessing.pool import TERMINATE, IMapIterator, IMapUnorderedIterator
from multiprocessing.util import debug
from threading import current_thread
from typing import Iterable, Any

from ..native.pool import Pool as NativePool
from ..native.pool import job_counter, mapstar, ApplyResult, MapResult
from ..native.process import Process
from ..native.process_group import ProcessGroup
from ..globalservices.process import multi_join


import collections
import itertools
import time
import threading
import signal
import os


def starmapstar(args):
    return list(itertools.starmap(args[0], args[1]))


# XXX Override Pool._terminate_pool() in order to forcibly terminate pool
# XXX processes AFTER the task (and result) handler thread(s) have
# XXX been joined. This avoids the race condition where a process may
# XXX be terminated (via SIGTERM) while holding the task queue's channel OT
# XXX or UT lock, which effectively blocks the task handler when trying
# XXX to send sentinel messages (i.e., `None`) to processes that tell them
# XXX to exit.
# XXX
# XXX NOTE: This code was copied and modified from Python 3.9.10.
def dragon_terminate_pool(
    cls,
    taskqueue,
    inqueue,
    outqueue,
    pool,
    change_notifier,
    worker_handler,
    task_handler,
    result_handler,
    cache,
):
    # this is guaranteed to only be called once
    debug(f"finalizing pool, context: {get_start_method()}")

    # Notify that the worker_handler state has been changed so the
    # _handle_workers loop can be unblocked (and exited) in order to
    # send the finalization sentinel all the workers.
    worker_handler._state = TERMINATE
    change_notifier.put(None)

    task_handler._state = TERMINATE

    debug("helping task handler/workers to finish")
    cls._help_stuff_finish(inqueue, task_handler, len(pool))

    if (not result_handler.is_alive()) and (len(cache) != 0):
        raise AssertionError("Cannot have cache with result_hander not alive")

    result_handler._state = TERMINATE
    change_notifier.put(None)
    outqueue.put(None)  # sentinel

    # We must wait for the worker handler to exit before terminating
    # workers because we don't want workers to be restarted behind our back.
    debug("joining worker handler")
    if current_thread() is not worker_handler:
        worker_handler.join()

    # XXX Moved the loop to foribly terminate pool processes to after task
    # XXX and result handler threads have been joined.

    debug("joining task handler")
    if current_thread() is not task_handler:
        task_handler.join()

    debug("joining result handler")
    if current_thread() is not result_handler:
        result_handler.join()

    # XXX Terminate worker processes which have not already finished and
    # XXX then join on them.
    if pool and hasattr(pool[0], "terminate"):
        # TODO, since we're patching anyway, lets give the workers a chance
        # to clean themselves up before doing the dangerous thing of killing
        # them all
        puids = [p.sentinel for p in pool]

        debug("joining workers")
        object_tuples, proc_status = multi_join(puids, 2.0, join_all=True)
        if not object_tuples:
            debug(f"multi_join not able to join all pool workers")
            debug(f"proc_status = {proc_status}")

        debug("terminating workers")
        for p in pool:
            if p.exitcode is None:
                debug(f"pool worker has not exited. Terminating {p.pid}")
                p.terminate()

        debug("joining pool workers")
        for p in pool:
            if p.is_alive():
                # worker has not yet exited
                debug("cleaning up worker %d" % p.pid)
                p.join()


class DragonPoolPatched(multiprocessing.pool.Pool):  # Dummy
    """Dragon's patched implementation of Multiprocessing Pool."""

    _terminate_pool = classmethod(dragon_terminate_pool)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class WrappedDragonProcess:  # Dummy

    def __init__(self, process, ident):
        self._puid = ident
        if process is None:
            self._process = Process(None, ident=self._puid)

    def start(self) -> None:
        """Start the process represented by the underlying process object."""
        self._process.start()

    def is_alive(self) -> bool:
        """Check if the process is still running

        :return: True if the process is running, False otherwise
        :rtype: bool
        """
        try:
            stat = self._process.is_alive
        except Exception:
            stat = False
        return stat

    def join(self, timeout: float = None) -> int:
        """Wait for the process to finish.

        :param timeout: timeout in seconds, defaults to None
        :type timeout: float, optional
        :return: exit code of the process, None if timeout occurs
        :rtype: int
        :raises: ProcessError
        """
        return self._process.join()

    def terminate(self) -> None:
        """Send SIGTERM signal to the process, terminating it.

        :return: None
        :rtype: NoneType
        """
        self._process.terminate()

    def kill(self) -> None:
        """Send SIGKILL signal to the process, killing it.

        :return: None
        :rtype: NoneType
        """
        self._process.kill()

    @property
    def pid(self):
        """Process puid. Globally unique"""
        return self._puid

    @property
    def name(self) -> str:
        """gets serialized descriptors name for the process

        :return: serialized descriptor name of process
        :rtype: str
        """
        return self._process.name

    @property
    def exitcode(self) -> int:
        """When the process has terminated, return exit code. None otherwise."""
        return self._process.returncode

    @property
    def sentinel(self):
        raise NotImplementedError

    @property
    def authkey(self):
        raise NotImplementedError

    @property
    def daemon(self):
        raise NotImplementedError

    @property
    def close(self):
        raise NotImplementedError


class WrappedResult:
    """Wraps `ApplyResult` and `MapResult` so that correct timeout error can be raised"""

    def __init__(self, result: ApplyResult or MapResult = None):
        """Initializes wrapped result by saving input result

        :param result: returned result from native pool, defaults to None
        :type result: ApplyResult or MapResult, optional
        """
        self._result = result

    def get(self, timeout: float = None) -> Any:
        """Retrieves returned values of work submitted to pool

        :param timeout: timeout for getting result, defaults to None
        :type timeout: float, optional
        :raises multiprocessing.TimeoutError: raised if result is not ready in specified timeout
        :return: value returned by `func(*args, **kwds)`
        :rtype: Any
        """
        try:
            return self._result.get(timeout=timeout)
        except TimeoutError:
            raise multiprocessing.TimeoutError

    def ready(self) -> bool:
        """Checks whether the result is ready.

        :return: returns True if the result is ready or False if it isn't
        :rtype: bool
        """
        return self._result.ready()

    def successful(self) -> bool:
        """Checks if the result is ready and returns whether the function call was successful.

        :raises ValueError: raised if result is not ready
        :return: returns True if function call was successful
        :rtype: bool
        """
        return self._result.successful()

    def wait(self, timeout: float = None) -> None:
        """Waits on event for result to be ready

        :param timeout: timeout indicating how long to wait for result to be ready, defaults to None
        :type timeout: float, optional
        """
        self._result.wait(timeout)


class DragonPool(NativePool):
    """A process pool consisting of a input and output queues and worker processes"""

    def __init__(self, *args, context=None, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def _pool(self):
        puids = self._pg.puids
        pool_procs = []
        # need to wait until all procs are up.
        while None in self._pg.puids:
            time.sleep(0.1)
        # add a wrapped proc that has an interface like what mp is expecting
        for puid in puids:
            pool_procs.append(WrappedDragonProcess(None, ident=puid))
        return pool_procs

    def _repopulate_pool(self):
        # repopulate pool by shutting PG down and then starting new PG
        if self._close_thread is not None:
            raise RuntimeError("Trying to repopulate a pool that was previously closed. This pattern is not supported.")
        if not self._pg._state == "Idle":
            self.terminate()
        if not self._pg_closed:
            self._pg.join(timeout=None)
            self._pg.close(timeout=60)
        del self._pg
        self._pg = ProcessGroup(restart=True, ignore_error_on_exit=True)
        self._pg.add_process(self._processes, self._template)
        self._pg.init()
        self._pg.start()
        self._pg_closed = False
        self._can_join = False

    def apply_async(
        self,
        func: callable,
        args: tuple = (),
        kwds: dict = {},
        callback: callable = None,
        error_callback: callable = None,
    ) -> WrappedResult:
        """Equivalent to calling `func(*args, **kwds)` in a non-blocking way. A result is immediately returned and then updated when `func(*args, **kwds)` has completed.

        :param func: user function to be called by worker function
        :type func: callable
        :param args: input args to func, defaults to ()
        :type args: tuple, optional
        :param kwds: input kwds to func, defaults to {}
        :type kwds: dict, optional
        :param callback: user provided callback function, defaults to None
        :type callback: callable, optional
        :param error_callback: user provided error callback function, defaults to None
        :type error_callback: callable, optional
        :raises ValueError: raised if pool has already been closed or terminated
        :return: A result that has a `get` method to retrieve result of `func(*args, **kwds)`
        :rtype: WrappedResult
        """
        return WrappedResult(
            super().apply_async(func=func, args=args, kwds=kwds, callback=callback, error_callback=error_callback)
        )

    def map_async(
        self,
        func: callable,
        iterable: Iterable,
        chunksize: int = None,
        callback: callable = None,
        error_callback: callable = None,
    ) -> WrappedResult:
        """Apply `func` to each element in `iterable`. The results
        are collected in a list that is returned immediately. It is equivalent to map if the `get` method is called immediately on the returned result.

        :param func: user provided function to call on elements of iterable
        :type func: callable
        :param iterable: input args to func
        :type iterable: iterable
        :param chunksize: size of work elements to be submitted to input work queue, defaults to None
        :type chunksize: int, optional
        :param callback: user provided callback function, defaults to None
        :type callback: callable, optional
        :param error_callback: user provided error callback function, defaults to None
        :type error_callback: callable, optional
        :return: A result that has a `get` method that returns an iterable of the output from applying `func` to each element of iterable.
        :rtype: WrappedResult
        """
        return WrappedResult(super()._map_async(func, iterable, mapstar, chunksize, callback, error_callback))

    def apply(self, func: callable, args: tuple = (), kwds: dict = {}) -> Any:
        """Equivalent to calling `func(*args, **kwds)` in a blocking way. The function returns when `func(*args, **kwds)` has completed and the result has been updated with the output

        :param func: user function to be called by worker
        :type func: callable
        :param args: input args to func, defaults to ()
        :type args: tuple, optional
        :param kwds: input kwds to func, defaults to {}
        :type kwds: dict, optional
        :return: The result of `func(*args, **kwds)`
        :rtype:
        """
        return self.apply_async(func, args, kwds).get()

    def map(self, func: callable, iterable: Iterable, chunksize: int = None) -> Iterable[Any]:
        """Apply `func` to each element in `iterable`, collecting the results
        in a list that is returned.

        :param func: user provided function to call on elements of iterable
        :type func: callable
        :param iterable: input args to func
        :type iterable: Iterable
        :param chunksize: size of work elements to be submitted to input work queue, defaults to None
        :type chunksize: int, optional
        :return: list of results from applying `func` to each element of input iterable
        :rtype: Iterable[Any]
        """
        return self.map_async(func, iterable, chunksize).get()

    def starmap(self, func: callable, iterable: Iterable[Iterable], chunksize: int = None) -> Iterable[Any]:
        """Like `map()` method but the elements of the `iterable` are expected to be iterables as well and will be unpacked as arguments. Hence `func` and (a, b) becomes func(a, b).


        :param func: user provided function to call on elements of iterable
        :type func: callable
        :param iterable: input iterable args to func
        :type iterable: Iterable[Iterable]
        :param chunksize: size of work elements to be submitted to input work queue, defaults to None
        :type chunksize: int, optional
        :return: results of applying `func` to input iterable args
        :rtype: Iterable[Any]
        """
        return WrappedResult(self._map_async(func, iterable, starmapstar, chunksize)).get()

    def starmap_async(
        self,
        func: callable,
        iterable: Iterable[Iterable],
        chunksize: int = None,
        callback: callable = None,
        error_callback: callable = None,
    ) -> WrappedResult:
        """Asynchronous version of `starmap()` method.

        :param func: user provided function to call on elements of iterable
        :type func: callable
        :param iterable: input iterable args to func
        :type iterable: Iterable[Iterable]
        :param chunksize: size of work elements to be submitted to input work queue, defaults to None
        :type chunksize: int, optional
        :param callback: user provided callback function, defaults to None
        :type callback: callable, optional
        :param error_callback: user provided error callback function, defaults to None
        :type error_callback: callable, optional
        :return: A result that has a `get` method that returns an iterable of the output from applying `func` to each element of iterable.
        :rtype: WrappedResult
        """
        return WrappedResult(self._map_async(func, iterable, starmapstar, chunksize, callback, error_callback))

    def __enter__(self):
        self._check_running()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.terminate()

    def imap_unordered(self, func: callable, iterable: Iterable, chunksize: int = 1) -> IMapUnorderedIterator or Any:
        """Like `imap()` method but ordering of results is arbitrary.

        :param func: user provided function to call on elements of iterable
        :type func: callable
        :param iterable: input args to func
        :type iterable: Iterable
        :param chunksize: size of work elements to be submitted to input work queue, defaults to 1
        :type chunksize: int, optional
        :raises ValueError: raised if chunksize is less than one
        :return: results of calling `func` on `iterable` elements
        :rtype: IMapUnorderedIterator or Any
        """
        self._check_running()
        if chunksize == 1:
            result = IMapUnorderedIterator(self)
            # wait to generate more tasks till last task generation thread has finished
            if self._map_launch_thread is not None:
                self._map_launch_thread.join()
            # start thread to put tasks into the input queue
            self._map_launch_thread = threading.Thread(
                target=NativePool._partition_tasks,
                args=(self._inqueue, result, func, iterable, self._end_threading_event, result._set_length),
            )
            self._map_launch_thread.start()
            return result
        else:
            if chunksize < 1:
                raise ValueError(f"Chunksize must be 1+, not {chunksize}")
            task_batches = NativePool._get_tasks(func, iterable, chunksize)
            result = IMapUnorderedIterator(self)
            self._map_launch_thread = threading.Thread(
                target=NativePool._partition_tasks,
                args=(
                    self._inqueue,
                    result,
                    mapstar,
                    task_batches,
                    self._end_threading_event,
                    result._set_length,
                ),
            )
            self._map_launch_thread.start()
            return (item for chunk in result for item in chunk)

    def imap(self, func: callable, iterable: Iterable, chunksize: int = 1) -> IMapIterator or Any:
        """Equivalent of `map()` -- can be MUCH slower than `Pool.map()`.
        Unlike `map()`, the iterable isn't immediately turned into a list.
        Rather, it is iterated over and items are put into the queue one at a
        time if chunksize=1. Unlike the non-imap variants of map, the returned
        object can then be iterated over and individual results will be
        returned as soon as they are available rather than when all work
        submitted via the iterable is done.

        :param func: user provided function to call on elements of iterable
        :type func: callable
        :param iterable: input args to func
        :type iterable: Iterable
        :param chunksize: size of work elements to be submitted to input work queue, defaults to 1
        :type chunksize: int, optional
        :raises ValueError: raised if chunksize is less than one
        :return: results of calling `func` on `iterable` elements
        :rtype: IMapIterator or Any
        """
        self._check_running()
        if chunksize == 1:
            result = IMapIterator(self)
            # wait to generate more tasks till last task generation thread has finished
            if self._map_launch_thread is not None:
                self._map_launch_thread.join()
            # start thread to put tasks into the input queue
            self._map_launch_thread = threading.Thread(
                target=NativePool._partition_tasks,
                args=(self._inqueue, result, func, iterable, self._end_threading_event, result._set_length),
            )
            self._map_launch_thread.start()
            return result
        else:
            if chunksize < 1:
                raise ValueError(f"Chunksize must be 1+, not {chunksize}")
            # generate batches of work based on chunksize
            task_batches = NativePool._get_tasks(func, iterable, chunksize)
            result = IMapIterator(self)
            # start thread that puts batches of work into input queue
            self._map_launch_thread = threading.Thread(
                target=NativePool._partition_tasks,
                args=(
                    self._inqueue,
                    result,
                    mapstar,
                    task_batches,
                    self._end_threading_event,
                    result._set_length,
                ),
            )
            self._map_launch_thread.start()
            return (item for chunk in result for item in chunk)


class BaseImplPool(multiprocessing.pool.Pool):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


def Pool(processes=None, initializer=None, initargs=(), maxtasksperchild=None, context=None, *, use_base_impl=True):
    #    dragon_base_pool = int(os.getenv("DRAGON_BASEPOOL", "0"))
    dragon_base_pool = os.getenv("DRAGON_BASEPOOL", "NATIVE")
    if use_base_impl:
        return BaseImplPool(processes, initializer, initargs, maxtasksperchild, context=context)
    elif dragon_base_pool == "PATCHED":
        return DragonPoolPatched(processes, initializer, initargs, maxtasksperchild, context=context)
    else:
        return DragonPool(processes, initializer, initargs, maxtasksperchild, policy=None)
