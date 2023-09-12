"""The Dragon native pool manages a pool of child processes. 
"""

from __future__ import annotations
import logging
import queue
import socket
import itertools
import traceback
import threading
import types
import signal
from typing import Iterable, Any
import time

import dragon

from .queue import Queue
from .process import TemplateProcess
from .process_group import ProcessGroup
from .event import Event
from .process import current as current_process
from .machine import System

from ..infrastructure.policy import Policy

# helper functions copied from multiprocessing.pool.Pool

job_counter = itertools.count()

POLL_FREQUENCY = 0.2


class RemoteTraceback(Exception):
    def __init__(self, tb):
        self.tb = tb

    def __str__(self):
        return self.tb


class ExceptionWithTraceback:
    def __init__(self, exc, tb):
        tb = traceback.format_exception(type(exc), exc, tb)
        tb = "".join(tb)
        self.exc = exc
        self.tb = '\n"""\n%s"""' % tb

    def __reduce__(self):
        return rebuild_exc, (self.exc, self.tb)


def rebuild_exc(exc, tb):
    exc.__cause__ = RemoteTraceback(tb)
    return exc


class MaybeEncodingError(Exception):
    """Wraps possible unpickleable errors, so they can be
    safely sent through the socket.
    """

    def __init__(self, exc, value):
        self.exc = repr(exc)
        self.value = repr(value)
        super(MaybeEncodingError, self).__init__(self.exc, self.value)

    def __str__(self):
        return "Error sending result: '%s'. Reason: '%s'" % (self.value, self.exc)

    def __repr__(self):
        return "<%s: %s>" % (self.__class__.__name__, self)


def _helper_reraises_exception(ex):
    "Pickle-able helper function for use by _guarded_task_generation."
    raise ex


class ApplyResult:
    """Returned by `apply_async` and has `get` method to retrieve result."""

    def __init__(self, pool: Pool, callback: callable, error_callback: callable):
        """Initializes a result

        :param pool: the pool where work has been submitted
        :type pool: dragon.native.pool.Pool
        :param callback: function called on returned result
        :type callback: callable
        :param error_callback: function called on returned result if an error was raised
        :type error_callback: callable
        """
        self._pool = pool
        self._event = threading.Event()
        self._job = next(job_counter)
        self._cache = pool._cache
        self._callback = callback
        self._error_callback = error_callback
        self._cache[self._job] = self
        self._success = None
        self._value = None

    def ready(self) -> bool:
        """Checks whether the result is ready.

        :return: returns True if the result is ready or False if it isn't
        :rtype: bool
        """
        return self._event.is_set()

    def successful(self) -> bool:
        """Checks if the result is ready and returns whether the function call was successful.

        :raises ValueError: raised if result is not ready
        :return: returns True if function call was successful
        :rtype: bool
        """
        if not self.ready():
            raise ValueError("{0!r} not ready".format(self))
        return self._success

    def wait(self, timeout: float = None) -> None:
        """Waits on event for result to be ready

        :param timeout: timeout indicating how long to wait for result to be ready, defaults to None
        :type timeout: float, optional
        """
        self._event.wait(timeout)

    def get(self, timeout: float = None) -> Any:
        """Retrieves returned values of work submitted to pool

        :param timeout: timeout for getting result, defaults to None
        :type timeout: float, optional
        :raises TimeoutError: raised if result is not ready in specified timeout
        :raises self._value: raises error if returned work was unsuccessful
        :return: value returned by `func(*args, **kwargs)`
        :rtype: Any
        """
        self.wait(timeout)
        if not self.ready():
            raise TimeoutError
        if self._success:
            return self._value
        else:
            raise self._value

    def _set(self, i, obj):
        self._success, self._value = obj
        if self._callback and self._success:
            self._callback(self._value)
        if self._error_callback and not self._success:
            self._error_callback(self._value)
        self._event.set()
        del self._cache[self._job]
        self._pool = None

    __class_getitem__ = classmethod(types.GenericAlias)


AsyncResult = ApplyResult  # create alias -- see #17805

#
# Class whose instances are returned by `Pool.map_async()`
#


class MapResult(ApplyResult):
    def __init__(self, pool: Pool, chunksize: int, length: int, callback: callable, error_callback: callable):
        """Initialization method

        :param pool: the pool where work is submitted
        :type pool: dragon.native.pool.Pool
        :param chunksize: size that work is supposed to be broken up into
        :type chunksize: int
        :param length: number of items in iterable
        :type length: int
        :param callback: function to be called upon return of result
        :type callback: callable
        :param error_callback: function to be called upon return of result if an error was raised.
        :type error_callback: callable
        """

        ApplyResult.__init__(self, pool, callback, error_callback=error_callback)
        self._success = True
        self._value = [None] * length
        self._chunksize = chunksize
        if chunksize <= 0:
            self._number_left = 0
            self._event.set()
            del self._cache[self._job]
        else:
            self._number_left = length // chunksize + bool(length % chunksize)

    def _set(self, i, success_result):
        self._number_left -= 1
        success, result = success_result
        if success and self._success:
            self._value[i * self._chunksize : (i + 1) * self._chunksize] = result
            if self._number_left == 0:
                if self._callback:
                    self._callback(self._value)
                del self._cache[self._job]
                self._event.set()
                self._pool = None
        else:
            if not success and self._success:
                # only store first exception
                self._success = False
                self._value = result
            if self._number_left == 0:
                # only consider the result ready once all jobs are done
                if self._error_callback:
                    self._error_callback(self._value)
                del self._cache[self._job]
                self._event.set()
                self._pool = None


def mapstar(args):
    return list(map(*args))


LOGGER = logging.getLogger(__name__)


class Pool:
    """A Dragon native Pool relying on native Process Group and Queues

    The interface resembles the Python Multiprocessing.Pool interface and focuses on the most generic functionality. Here we directly support the asynchronous API. The synchronous version of these calls are supported by calling get on the objects returned from the asynchronous functions. By using a Dragon native Process Group to coordinate the worker processes this implementation addresses scalability limitations with the patched base implementation of Multiprocessing.Pool.

    At this time, both `terminate` and `close` send a `signal.SIGTERM` that the workers catch. Using `close` guarantees that all work submitted to the pool is finished before the signal is sent while `terminate` sends the signal immediately. The user is expected to call `join` following both of these calls. If `join` is not called, zombie processes may be left.

    """

    def __init__(
        self,
        processes: int = None,
        initializer: callable = None,
        initargs: tuple = (),
        maxtasksperchild: int = None,
        policy: Policy = None,
    ):
        """Init method

        :param processes: number of worker processes, defaults to None
        :type processes: int, optional
        :param initializer: initializer function, defaults to None
        :type initializer: callable, optional
        :param initargs: arguments for initializer function, defaults to ()
        :type initargs: tuple, optional
        :param maxtasksperchild: maximum tasks each worker will perform, defaults to None
        :type maxtasksperchild: int, optional
        :param policy: determines the placement of the processes
        :type policy: dragon.infrastructure.policy.Policy
        :raises ValueError: raised if number of worker processes is less than 1
        """
        myp = current_process()
        LOGGER.debug(f"pool init on node {socket.gethostname()} by process {myp.ident}")
        self._inqueue = Queue()
        self._outqueue = Queue()
        self._end_threading_event = threading.Event()
        self._maxtasksperchild = maxtasksperchild
        self._initializer = initializer
        self._initargs = initargs
        self._can_join = False

        if processes is None:
            my_sys = System()
            self._processes = my_sys.nnodes
            LOGGER.warning(
                f"Number of processes not specified. Setting processes={self._processes}, equal to the number of nodes."
            )
        else:
            self._processes = processes

        if self._processes < 1:
            raise ValueError("Number of processes must be at least 1")

        # starts a process group with nproc workers
        self._template = TemplateProcess(
            self._worker_function,
            args=(self._inqueue, self._outqueue, self._initializer, self._initargs, self._maxtasksperchild),
        )
        self._pg = ProcessGroup(restart=True, ignore_error_on_exit=True)
        self._pg.add_process(self._processes, self._template)
        self._pg.init()
        self._pg.start()

        # dict that holds jobs submitted via apply_async and map_async
        self._cache = {}

        # thread that handles getting results from outqueue and putting in dict
        self._results_handler = threading.Thread(
            target=self._handle_results, args=(self._outqueue, self._cache, self._end_threading_event)
        )
        self._results_handler.start()

        # thread used by map_async to chunk input list
        self._map_launch_thread = None
        # thread used by close to wait for results before sending shutdown signals
        self._close_thread = None

    def _check_running(self):
        status = self._pg.status
        if not (status == "Running" or status == "Maintain"):
            raise ValueError("Pool not running")

    def terminate(self) -> None:
        """This sends the signal.SIGTERM and sets the threading event immediately. Calling this method before blocking on results may lead to some work being undone. If all work should be done before stopping the workers, `close` should be used. If `join` is not called after terminate zombie processes may be left over and interfere with future pool or process group use."""

        LOGGER.debug(f"terminating pool")
        # setting end event to stop all threads
        self._end_threading_event.set()
        # send SIGTERM signal to process group. Worker functions will return on their next pass through the while loop. See CIRRUS-1467 for reasons why sending SIGKILL doesn't work here.
        if not self._pg.status == "Stop":
            self._pg.kill(signal.SIGTERM)
        self._can_join = True

    @staticmethod
    def _close(cache, end_threading_event, pool):
        def work_done(cache):
            return len(cache) == 0

        LOGGER.debug(f"closing pool and setting end events in thread {threading.get_native_id()}")

        # waits until all submitted jobs are done.
        while not work_done(cache):
            if end_threading_event.is_set():
                return

        # send SIGTERM to worker_functions
        if not pool.status == "Stop":
            pool.kill(signal.SIGTERM)
        end_threading_event.set()

    def close(self) -> None:
        """This method starts a thread that waits for all submitted jobs to finish. This thread then sends signal.SIGTERM to the process group and sets the threading event to close the handle_results thread. Waiting and then sending the signals within the thread allows close to return without all work needing to be done.

        :raises ValueError: raised if `close` or `terminate` have been previously called
        """
        if self._can_join:
            raise ValueError("Trying to close pool that has already been closed or terminated")
        # defines and starts thread that waits for jobs to be done before sending shutdown signals
        self._close_thread = threading.Thread(
            target=self._close, args=(self._cache, self._end_threading_event, self._pg)
        )
        self._close_thread.start()
        self._can_join = True

    def join(self) -> None:
        """Waits for all workers to return. The user must have called close or terminate prior to calling join.

        :raises ValueError: raised if `close` or `terminate` were not previously called
        """
        LOGGER.debug(f"joining pool")
        if not self._can_join:
            raise ValueError("Trying to join pool that has not been closed or terminated")
        # waits on close_thread before joining workers
        if self._close_thread is not None:
            self._close_thread.join()
        if self._map_launch_thread is not None:
            self._map_launch_thread.join()

        if not self._pg.status == "Stop":
            self._pg.join()
            self._pg.stop()
        self._results_handler.join()

    @staticmethod
    def _worker_function(inqueue, outqueue, initializer=None, initargs=(), maxtasks=None):
        myp = current_process()

        # handles shutdown signal
        termflag = False

        def handler(signum, frame):
            LOGGER.debug("_worker_function SIGTERM handler saw signal")
            nonlocal termflag
            termflag = True

        signal.signal(signal.SIGTERM, handler)

        if (maxtasks is not None) and not (isinstance(maxtasks, int) and maxtasks >= 1):
            raise AssertionError("Maxtasks {!r} is not valid".format(maxtasks))

        if initializer is not None:
            initializer(*initargs)

        completed_tasks = 0

        while not termflag and (maxtasks is None or (maxtasks and completed_tasks < maxtasks)):
            # get work item
            LOGGER.debug(f"getting work from inqueue on node {socket.gethostname()}")
            try:
                # still need timeout here to check signal occasionally
                task = inqueue.get(timeout=0)
            except queue.Empty:
                time.sleep(POLL_FREQUENCY)
                continue
            except TimeoutError:
                time.sleep(POLL_FREQUENCY)
                continue

            LOGGER.debug(f"{task} task received by {myp.ident} on {socket.gethostname()}")

            job, i, func, args, kwargs = task
            LOGGER.debug(f"job={job}, i={i}, func={func}, args={args}, kwargs={kwargs}")

            # perform work
            try:
                output = (True, func(*args, **kwargs))
            except Exception as e:
                LOGGER.debug(f"\nDragon pool._worker_function threw an exception: \n{e}")
                if func is not _helper_reraises_exception:
                    e = ExceptionWithTraceback(e, e.__traceback__)
                output = (False, e)
            # put result in output queue
            LOGGER.debug(f"putting result in outqueue = {(job, i, output)}")
            try:
                outqueue.put((job, i, output))
            except Exception as e:
                wrapped = MaybeEncodingError(e, output[1])
                LOGGER.debug("Possible encoding error while sending result: %s" % (wrapped))
                outqueue.put((job, i, (False, wrapped)))
            completed_tasks += 1

        LOGGER.debug(f"{myp.ident} returning from worker_function")
        

    @classmethod
    def _handle_results(cls, outqueue, cache, end_event):
        LOGGER.debug(
            f"handle_results on node {socket.gethostname()} with thread id {threading.get_native_id()}"
        )
        while not end_event.is_set():

            # timeout with some frequency so we check if end_event is set
            try:
                task = outqueue.get(timeout=POLL_FREQUENCY)
            except queue.Empty:
                continue
            except TimeoutError:
                continue

            job, i, obj = task

            # place result into dict holding different jobs
            try:
                cache[job]._set(i, obj)
            except KeyError:
                pass

            task = job = obj = None

    def apply_async(
        self,
        func: callable,
        args: tuple = (),
        kwargs: dict = {},
        callback: callable = None,
        error_callback: callable = None,
    ) -> ApplyResult:
        """Equivalent to calling `func(*args, **kwargs)` in a non-blocking way. A result is immediately returned and then updated when `func(*args, **kwargs)` has completed.

        :param func: user function to be called by worker function
        :type func: callable
        :param args: input args to func, defaults to ()
        :type args: tuple, optional
        :param kwargs: input kwargs to func, defaults to {}
        :type kwargs: dict, optional
        :param callback: user provided callback function, defaults to None
        :type callback: callable, optional
        :param error_callback: user provided error callback function, defaults to None
        :type error_callback: callable, optional
        :raises ValueError: raised if pool has already been closed or terminated
        :return: A result that has a `get` method to retrieve result of `func(*args, **kwargs)`
        :rtype: ApplyResult
        """

        if self._can_join:
            raise ValueError("Pool closed or terminated. Cannot add work to a closed pool.")

        result = ApplyResult(self, callback, error_callback)
        self._inqueue.put((result._job, 0, func, args, kwargs))
        return result

    @staticmethod
    def _get_tasks(func, it, size):
        it = iter(it)
        while True:
            x = tuple(itertools.islice(it, size))
            if not x:
                return
            yield (func, x)

    @staticmethod
    def _guarded_task_generation(result_job, func, iterable):
        try:
            i = -1
            for i, x in enumerate(iterable):
                yield (result_job, i, func, (x,), {})
        except Exception as e:
            yield (result_job, i + 1, _helper_reraises_exception, (e,), {})

    @staticmethod
    def _partition_tasks(inqueue, result, mapper, task_batches, end_event, set_length=None):
        # this is here in case the iterable is empty that way the set_length
        # can be computed correctly
        task = None

        try:
            for task in Pool._guarded_task_generation(result._job, mapper, task_batches):
                if end_event.is_set():
                    return
                inqueue.put(task)
            # only used by imap and imap_unordered
            else:
                if set_length:
                    idx = task[1] if task else -1
                    set_length(idx + 1)
        finally:
            task = taskseq = job = None

    def _map_async(self, func, iterable, mapper, chunksize=None, callback=None, error_callback=None):
        if self._can_join:
            raise ValueError("Pool closed or terminated. Cannot map to a closed pool.")
        self._check_running()

        if not hasattr(iterable, "__len__"):
            iterable = list(iterable)

        num_items = len(iterable)

        if num_items == 0:
            chunksize = 0

        if chunksize is None:
            chunksize, remainder = divmod(num_items, self._processes * 4)
            if remainder:
                chunksize += 1

        # Generate the tasks and place result reference in dict
        task_batches = Pool._get_tasks(func, iterable, chunksize)
        result = MapResult(self, chunksize, len(iterable), callback, error_callback=error_callback)

        # wait to generate more tasks till last task generation thread has finished
        if self._map_launch_thread is not None:
            self._map_launch_thread.join()
        # start thread to put tasks into the input queue
        self._map_launch_thread = threading.Thread(
            target=Pool._partition_tasks,
            args=(self._inqueue, result, mapper, task_batches, self._end_threading_event),
        )
        self._map_launch_thread.start()

        return result

    def map_async(
        self,
        func: callable,
        iterable: Iterable,
        chunksize: int = None,
        callback: callable = None,
        error_callback: callable = None,
    ) -> MapResult:
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
        :rtype: MapResult
        """
        return self._map_async(func, iterable, mapstar, chunksize, callback, error_callback)
