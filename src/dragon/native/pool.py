"""The Dragon native pool manages a pool of child processes that can be used to run python callables."""

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

import dragon

from .queue import Queue
from .process import ProcessTemplate
from .process_group import ProcessGroup, DragonProcessGroupError
from .event import Event
from .process import current as current_process
from .machine import System
from .barrier import Barrier
from ..dlogging import util as dlog
from ..dlogging.util import setup_BE_logging, DragonLoggingServices as dls

from ..infrastructure.policy import Policy

# helper functions copied from multiprocessing.pool.Pool

job_counter = itertools.count()

WORKER_POLL_FREQUENCY = 0.2
RESULTS_HANDLER_POLL_FREQUENCY = 0.2


def get_logs(name):
    global _LOG
    log = _LOG.getChild(name)
    return log.debug, log.info


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
        :return: value returned by `func(*args, **kwds)`
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


class Pool:
    """A Dragon native Pool relying on native Process Group and Queues

    The interface resembles the Python Multiprocessing.Pool interface and focuses on the most generic functionality. Here we directly support the asynchronous API. The synchronous version of these calls are supported by calling get on the objects returned from the asynchronous functions. By using a Dragon native Process Group to coordinate the worker processes this implementation addresses scalability limitations with the patched base implementation of Multiprocessing.Pool.

    At this time, `signal.SIGUSR2` is used to signal workers to shutdown. Users should not use or register a handler for this signal if they want to use `close` to shutdown the pool. Using `close` guarantees that all work submitted to the pool is finished before the signal is sent while `terminate` sends the signal immediately. If workers don't immediately exit, `terminate` will send escalating signals (SIGINT, SIGTERM, then SIGKILL) and wait patience amount of time for processes to exit before sending the next signal. The user is expected to call `join` following both of these calls. If `join` is not called, zombie processes may be left and leave the runtime in a corrupted state.

    """

    def __init__(
        self,
        processes: int = None,
        initializer: callable = None,
        initargs: tuple = (),
        maxtasksperchild: int = None,
        *,
        policy: Policy = None,
        processes_per_policy: int = None,
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
        :param policy: determines the placement and resources of processes. If a list of policies is given that the number of processes_per_policy must be specified.
        :type policy: dragon.infrastructure.policy.Policy or list of dragon.infrastructure.policy.Policy
        :param processes_per_policy: determines the number of processes to be placed with a specific policy if a list of policies is provided
        :type processes_per_policy: int
        :raises ValueError: raised if number of worker processes is less than 1
        """
        myp = current_process()

        # setup logging
        fname = f"{dls.PG}_{socket.gethostname()}_pool_main.log"
        setup_BE_logging(service=dls.PG, fname=fname)
        global _LOG
        _LOG = logging.getLogger(dls.PG)
        self.fdebug, self.finfo = get_logs("pool main")

        self.fdebug(f"pool init on node {socket.gethostname()} by process {myp.ident}")
        self._inqueue = Queue()
        self._outqueue = Queue()
        self._end_threading_event = threading.Event()
        self._maxtasksperchild = maxtasksperchild
        self._initializer = initializer
        self._initargs = initargs
        self._can_join = False
        self._pg_closed = False

        if processes is None:
            if processes_per_policy is not None:
                if isinstance(policy, list):
                    self._processes = len(policy) * processes_per_policy
                else:
                    self._processes = processes_per_policy
            else:
                my_sys = System()
                self._processes = my_sys.nnodes
        else:
            if processes_per_policy is not None:
                raise RuntimeError("cannot provide processes_per_policy and a total number of processes")
            if isinstance(policy, list):
                raise RuntimeError(
                    "cannot provide a list of policies and a total number of processes. must provide the number of processes_per_policy"
                )
            self._processes = processes

        if self._processes < 1:
            raise ValueError("Number of processes must be at least 1")

        self._start_barrier = Barrier(parties=self._processes + 1)
        self._start_barrier_passed = Event()
        self._start_barrier_helper = threading.Thread(
            target=self._start_barrier_helper, args=(self._start_barrier, self._start_barrier_passed)
        )
        self._start_barrier_helper.daemon = True
        self._start_barrier_helper.start()

        self._template = ProcessTemplate(
            self._worker_function,
            args=(
                self._inqueue,
                self._outqueue,
                self._start_barrier,
                self._start_barrier_passed,
                self._initializer,
                self._initargs,
                self._maxtasksperchild,
            ),
        )
        if isinstance(policy, list):
            self._pg = ProcessGroup(restart=True, ignore_error_on_exit=True)
            for p in policy:
                # starts a process group with nproc workers
                placed_template = ProcessTemplate(
                    self._worker_function,
                    args=(
                        self._inqueue,
                        self._outqueue,
                        self._start_barrier,
                        self._start_barrier_passed,
                        self._initializer,
                        self._initargs,
                        self._maxtasksperchild,
                    ),
                    policy=p,
                )
                self._pg.add_process(processes_per_policy, placed_template)
        else:
            self._pg = ProcessGroup(restart=True, ignore_error_on_exit=True, policy=policy)
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
        if self._pg_closed:
            raise ValueError("Checking running of a closed pool")
        status = self._pg._state
        if not (status == "Running"):
            raise ValueError(f"Pool not running. ProcessGroup State = {status}")

    def terminate(self, patience: float = 60) -> None:
        """This sets the threading event immediately and sends signal.SIGINT, signal.SIGTERM, and signal.SIGKILL successively with patience time between the sending of each signal until all processes have exited. Calling this method before blocking on results may lead to some work being undone. If all work should be done before stopping the workers, `close` should be used.

        :param patience: timeout to wait for processes to join after each signal, defaults to 60 to prevent indefinite hangs
        :type patience: float, optional
        """

        self.fdebug("terminating pool")
        if self._pg_closed:
            raise ValueError("Closing when ProcessGroup has been closed")
        if self._start_barrier_helper is not None:
            self._start_barrier_helper.join()
        self._check_running()
        self._end_threading_event.set()
        self.fdebug("stop restart")
        self._pg.stop_restart()
        self.fdebug("in stop")
        self._pg.stop(patience=patience)
        self.fdebug("in close")
        self._pg.close(patience=patience)
        self.fdebug("closed")
        self._pg_closed = True
        self._can_join = True

    def kill(self) -> None:
        """This sends the signal.SIGKILL and sets the threading event immediately. This is the most dangerous way to shutdown pool workers and will likely leave the runtime in a corrupted state. Calling this method before blocking on results may lead to some work being undone. If all work should be done before stopping the workers, `close` should be used."""

        self.fdebug("killing pool")
        # setting end event to stop all threads
        self._end_threading_event.set()
        self._pg.stop_restart()
        self._pg.kill()
        self._can_join = True

    @staticmethod
    def _start_barrier_helper(start_barrier: Barrier = None, start_barrier_passed: Event = None):
        """This is started in a separate thread and is meant to set the event when all workers have passed the barrier. We join on this thread in terminate to avoid sending a sigint when worker processes are in the middle of being started.

        :param start_barrier: barrier shared with worker_function, defaults to None
        :type start_barrier: Barrier, optional
        :param start_barrier_passed: Event to prevent restarted workers from trying to wait on barrier, defaults to None
        :type start_barrier_passed: Event, optional
        """
        fdebug, finfo = get_logs("start barrier helper")
        fdebug("waiting for all workers to reach barrier")
        start_barrier.wait()
        fdebug("all workers past barrier")
        start_barrier_passed.set()

    @staticmethod
    def _close(cache, start_barrier_passed, end_threading_event, pool):
        def work_done(cache):
            return len(cache) == 0

        # waits until all submitted jobs are done.
        while not work_done(cache):
            if end_threading_event.is_set():
                return

        # wait for all the initial processes to have come up and registered their signal handler
        start_barrier_passed.wait()

        pool.stop_restart()
        # send signal to shutdown pool workers
        pool.send_signal(signal.SIGUSR2)
        end_threading_event.set()

    def close(self) -> None:
        """This method starts a thread that waits for all submitted jobs to finish. This thread then sends signal.SIGUSR2 to the process group and sets the threading event to close the handle_results thread. Waiting and then sending the signals within the thread allows close to return without all work needing to be done.

        :raises ValueError: raised if `close` or `terminate` have been previously called
        """
        self.fdebug("closing pool")
        if self._can_join:
            raise ValueError("Trying to close pool that has already been closed or terminated")
        if self._pg_closed:
            raise ValueError("Closing when ProcessGroup has been closed")
        # defines and starts thread that waits for work in input queue to be done before sending shutdown signal
        self._close_thread = threading.Thread(
            target=self._close, args=(self._cache, self._start_barrier_passed, self._end_threading_event, self._pg)
        )
        self.fdebug("starting close thread")
        self._close_thread.start()
        self._can_join = True

    def join(self, patience: float = None) -> None:
        """Waits for all workers to return. The user must have called close or terminate prior to calling join. By default this blocks indefinitely for processes to exit. If a patience is given, then once the patience has passed signal.SIGINT, signal.SIGTERM, and signal.SIGKILL will be sent successively with patience time between the sending of each signal. It is recommended that if a patience is set to a value other than None that a user assume the runtime is corrupted after the join completes.

        :param patience: timeout to wait for processes to join after each signal, defaults to None
        :type patience: float, optional
        :raises ValueError: raised if `close` or `terminate` were not previously called
        :raises ValueError: raised if join has already been called and the process group is closed

        """
        self.fdebug("joining pool")
        if not self._can_join:
            raise ValueError("Trying to join pool that has not been closed or terminated")

        # this thread should almost always already be finished but put it here for safety
        if self._start_barrier_helper is not None:
            self.fdebug("joining start_barrier_helper")
            self._start_barrier_helper.join()

        self.fdebug("joining close thread and map thread")
        # waits on close_thread before joining workers
        if self._close_thread is not None:
            self.fdebug("joining close thread")
            self._close_thread.join()
        if self._map_launch_thread is not None:
            self.fdebug("joining map launch thread")
            self._map_launch_thread.join()

        if not self._pg_closed:
            self.fdebug("joining process group")
            try:
                self._pg.join(timeout=patience)
            except TimeoutError:
                self.fdebug("process group join timed out")
                pass

            self.fdebug("closing process group")
            try:
                # This extra step makes sure the infrastructure related to managing the processgroup exits cleanly
                if patience is not None:
                    self._pg.close(patience=patience)
                else:
                    self._pg.close()
            except DragonProcessGroupError:
                pass
            self._pg_closed = True
        self.fdebug("joining on results handler thread")
        self._results_handler.join()

        # destroy the underlying Dragon resources so
        # managed memory is freed.
        self._start_barrier_passed.destroy()
        self._start_barrier.destroy()
        self._inqueue.destroy()
        self._outqueue.destroy()
        del self._start_barrier
        del self._start_barrier_passed
        del self._inqueue
        del self._outqueue
        del self._template

    @staticmethod
    def _worker_function(
        inqueue, outqueue, start_barrier, start_barrier_passed, initializer=None, initargs=(), maxtasks=None
    ):
        try:
            termflag = threading.Event()

            def handler(signum, frame):
                termflag.set()

            signal.signal(signal.SIGUSR2, handler)

            # setup logging
            fname = f"{dls.PG}_{socket.gethostname()}_workers.log"
            setup_BE_logging(service=dls.PG, fname=fname)
            global _LOG
            _LOG = logging.getLogger(dls.PG).getChild("worker")
            fdebug, finfo = get_logs("worker")

            if not start_barrier_passed.is_set():
                start_barrier.wait()

            if (maxtasks is not None) and not (isinstance(maxtasks, int) and maxtasks >= 1):
                raise AssertionError("Maxtasks {!r} is not valid".format(maxtasks))

            if initializer is not None:
                initializer(*initargs)

            completed_tasks = 0

            while not termflag.is_set() and (maxtasks is None or (maxtasks and completed_tasks < maxtasks)):
                # get work item
                try:
                    # still need timeout here to check signal occasionally
                    task = inqueue.get(timeout=WORKER_POLL_FREQUENCY)
                except queue.Empty:
                    continue
                except TimeoutError:
                    continue

                job, i, func, args, kwargs = task

                # perform work
                try:
                    output = (True, func(*args, **kwargs))
                except Exception as e:
                    fdebug(f"\nDragon pool._worker_function threw an exception: \n{e}")
                    if func is not _helper_reraises_exception:
                        e = ExceptionWithTraceback(e, e.__traceback__)
                    output = (False, e)
                # put result in output queue
                try:
                    fdebug("putting output")
                    outqueue.put((job, i, output))
                    fdebug("put output")
                except Exception as e:
                    wrapped = MaybeEncodingError(e, output[1])
                    fdebug(f"Possible encoding error while sending result: {wrapped}")
                    outqueue.put((job, i, (False, wrapped)))
                completed_tasks += 1
        except EOFError:
            pass  # This can happen when barrier is destroyed at end of pool.
        except KeyboardInterrupt:
            pass
        finally:
            dlog.detach_from_dragon_handler(dls.PG)

    @classmethod
    def _handle_results(cls, outqueue, cache, end_event):
        fdebug, finfo = get_logs("results handler")
        fdebug(f"handle_results on node {socket.gethostname()} with thread id {threading.get_native_id()}")
        while not end_event.is_set():
            # timeout with some frequency so we check if end_event is set
            try:
                task = outqueue.get(timeout=RESULTS_HANDLER_POLL_FREQUENCY)
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
        kwds: dict = {},
        callback: callable = None,
        error_callback: callable = None,
    ) -> ApplyResult:
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
        :rtype: ApplyResult
        """

        if self._can_join:
            raise ValueError("Pool closed or terminated. Cannot add work to a closed pool.")

        result = ApplyResult(self, callback, error_callback)
        self._inqueue.put((result._job, 0, func, args, kwds))
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
            target=Pool._partition_tasks, args=(self._inqueue, result, mapper, task_batches, self._end_threading_event)
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
