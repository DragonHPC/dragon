"""Dragon's replacement for the Multiprocessing context object.

Also contains a high performance implementation of `wait()` on Multiprocessing and Dragon
Native objects. A blocking wait on one or more objects with a timeout is a
common task when handling communication. Objects can be of very different nature
(Queue, Process, Pipe), below them is usually a Dragon Channel.  Our
implementation here handles multiple waiters on lists that contain some of the
same objects gracefully and in a high perfomant way. Objects are categorized by
type and threads are spawned for every type. Objects of the same type are
grouped into a multi-join call, so the number of threads spawned is minimized.
The life-cycle of the process handling the wait is independent of the waiter
process, i.e. multiple repeated calls on the same objects by the same or
different processes will create only minimal overhead.

:raises NotImplementedError: If used with win32
"""

import multiprocessing
import multiprocessing.pool
import multiprocessing.queues
import multiprocessing.connection
import multiprocessing.shared_memory
import multiprocessing.synchronize
import multiprocessing.sharedctypes

import threading
import time
import os

import dragon
from .monkeypatching import original_multiprocessing
from .queues import DragonJoinableQueue, DragonQueue
from .shared_memory import DragonSharedMemory, DragonShareableList
from .process import DragonProcess, DragonPopen, PUID
from .connection import DragonListener, DragonClient

from dragon.globalservices.process import multi_join

import dragon.infrastructure.connection


def _sort_and_check_for_ready(object_list):
    """Sort objects by waiting type and check, if any are ready already.
    Addresses Multiprocessing unit test TestWait.test_wait_integer

    :param object_list: object to wait on
    :type object_list: list of PUID, Connection, etc.
    :return: ready objects, puids, connections and others
    :rtype: tuple of lists
    """

    # sort by type
    puids = set()
    conns = set()
    queues = set()
    others = list()

    for obj in object_list:
        if isinstance(obj, PUID):
            puids.add(obj)
        elif isinstance(obj, dragon.infrastructure.connection.Connection):
            conns.add(obj)
        elif isinstance(obj, dragon.infrastructure.connection.CUID):
            conns.add(obj)
        elif isinstance(obj, dragon.mpbridge.queues.DragonQueue):
            queues.add(obj)
        elif isinstance(obj, dragon.mpbridge.queues.DragonJoinableQueue):
            queues.add(obj)
        elif isinstance(obj, dragon.mpbridge.queues.DragonSimpleQueue):
            queues.add(obj)
        elif isinstance(obj, dragon.mpbridge.queues.BaseImplQueue):
            queues.add(obj)
        elif isinstance(obj, dragon.mpbridge.queues.BaseImplJoinableQueue):
            queues.add(obj)
        elif isinstance(obj, dragon.mpbridge.queues.BaseImplSimpleQueue):
            queues.add(obj)
        elif isinstance(obj, dragon.mpbridge.queues.FakeConnectionHandle):
            conns.add(obj)
        else:
            others.append(obj)

    # Check if any are done already
    # I have decided not to use the methods of _WaitService here,
    # but to call into Dragon directly. It reduces complexity, we might not have to spawn the
    # _WaitService class at all and it separates the concern of this function with _WaitService.

    ready_puids = []
    ready_conns = []
    ready_queues = []
    ready_others = []

    if puids:
        object_tuples, proc_status = multi_join(list(puids), 0)
        if object_tuples:
            ready_puids = [PUID(i[0]) for i in object_tuples]

    if conns:
        ready_conns = [conn for conn in list(conns) if conn.poll(0)]

    if queues:
        ready_queues = [q for q in list(queues) if q._poll(0)]

    if others:
        ready_others = original_multiprocessing.connection_wait(others, 0)

    ready_objects = ready_puids + ready_conns + ready_queues + ready_others

    return ready_objects, list(puids), list(conns), list(queues), list(others)


class _WaitService:
    """Provide a thread safe way of waiting on multiple types of objects at
    once.  This is a performance oriented implementation with shared data
    structures and one lock.  Service threads are waiting on the same event as
    their parent, that joins on them.  We never spawn more than one service
    thread per object. We handle Dragon PUIDs, connection, queues and the
    standard multiprocessing objects. If only a single new object is waited on,
    we do not spawn a service thread, but use the incoming thread.

    NOTE: Due to timing/scheduling of threads in the OS, this implementation
    cannot behave exactly like a single threaded version. In particular, when
    waiting on several object types at once, one type will always return first.
    A single threaded version might return a puid and 2 connections together.
    This implementation will return either a puid or 2 connections, but not both
    of them, even if they get ready at virtually the same time.
    `_sort_and_check_for_ready` alleviates the problem for objects that are
    already 'ready', when waited upon.
    """

    THREAD_WAIT = (
        0.1  # [sec] time interval for a thread to check if it needs to stop waiting, used when timeout is None
    )
    GC_INTERVAL = 10  # [sec] how often to look for abandoned waiters/ready_objects from vanished waiters
    # exact value not important as it's not related to timeout

    def __init__(self, timeout):
        self.wait_timeout = timeout  # [sec] time interval for a thread to check if it needs to stop waiting

        self.insert_lock = threading.Lock()  # protect inserting new objects and waiters

        self.events = {}  # event of waiting tid

        self.objects = {}  # list of objects by service thread tid
        self.waiters = {}  # list of waiting tids by service thread tid

        self.ready_objects = {}  # list of ready objects by waiter tid

        self.garbage_truck = threading.Thread(target=self._collect_garbage, daemon=True)
        self.garbage_truck.start()

    def __del__(self):
        """Shut down service threads"""

        for ev in self.events.values():
            ev.set()  # stop every waiter and the garbage truck

    def _collect_garbage(self):
        """Cleanup leftover tids that have never been picked up by a parent."""

        my_pid = os.getpid()
        my_tid = threading.get_native_id()

        with self.insert_lock:
            self.events[my_tid] = threading.Event()

        while True:
            # Wait on the event for 10sec.
            # If the event is set, this breaks immediately
            res = self.events[my_tid].wait(timeout=self.GC_INTERVAL)
            if res:
                break

            self.insert_lock.acquire()
            active_tids = os.listdir(f"/proc/{my_pid}/task")  # Linux only
            waiting_tids = {item for sublist in self.waiters.values() for item in sublist}
            ready_tids = self.ready_objects.keys()

            # find waiter tids that are no longer active, i.e. have disappeared
            bad_tids = []
            for tid in waiting_tids:
                if str(tid) not in active_tids:
                    bad_tids.append(tid)

            # remove bad waiter tids from the data structures. This is a bit tedious.
            for tid in bad_tids:
                for serv_tid in self.waiters.keys():
                    if tid in self.waiters[serv_tid]:
                        self.waiters[serv_tid].remove(tid)
                if tid in ready_tids:
                    del self.ready_objects[tid]
            self.insert_lock.release()

    def _wait_on_connection(self, conn):
        """Paranoid wait for a single connection object. We wake up from time to time
        to check if we are actually still needed.

        :param conns: connection objects to wait on
        :type conns: list of
                     dragon.infrastructure.connection.Connection,
                     dragon.infrastructure.connection.CUID,
                     dragon.mpbridge.queues.DragonQueue,
                     dragon.mpbridge.queues.Faker
        """

        my_tid = threading.get_native_id()

        joined = False
        lonely = False

        while not lonely and not joined:
            joined = conn.poll(self.wait_timeout)

            self.insert_lock.acquire()
            lonely = self.waiters[my_tid] == []
            self.insert_lock.release()

        if joined and not lonely:
            self._handle_wait_return([conn])

        self.insert_lock.acquire()
        del self.objects[my_tid]
        del self.waiters[my_tid]
        self.insert_lock.release()

    def _wait_on_queue(self, queue):
        """Paranoid wait for a single queue object. We wake up from time to time
        to check if we are actually still needed.

        :param queue: queue object to wait on
        :type queue: one of
                     dragon.mpbridge.queues.DragonQueue,
                     dragon.mpbridge.queues.DragonSimpleQueue,
                     dragon.mpbridge.queues.DragonJoinableQueue,
        """

        my_tid = threading.get_native_id()

        joined = False
        lonely = False

        while not lonely and not joined:
            joined = queue._poll(self.wait_timeout)

            self.insert_lock.acquire()
            lonely = self.waiters[my_tid] == []
            self.insert_lock.release()

        if joined and not lonely:
            self._handle_wait_return([queue])

        self.insert_lock.acquire()
        del self.objects[my_tid]
        del self.waiters[my_tid]
        self.insert_lock.release()

    def _wait_on_puids(self, puids):
        """Paranoid wait for a list PUIDs, pids, sentinels using a single thread and Dragon's multi-join.
        We wake up from time to time to check if our parent disappeared, i.e. if we are lonely.

        :param puids: PUID objects to wait on
        :type puids: list of dragon.process.PUID
        """

        my_tid = threading.get_native_id()
        object_tuples = None
        joined = False
        lonely = False

        while not lonely and not joined:
            object_tuples, proc_status = multi_join(puids, self.wait_timeout)

            joined = object_tuples != None
            self.insert_lock.acquire()
            lonely = self.waiters[my_tid] == []
            self.insert_lock.release()

        if joined and not lonely:
            ready_objects = [i[0] for i in object_tuples]
            self._handle_wait_return(ready_objects)

        self.insert_lock.acquire()
        del self.objects[my_tid]
        del self.waiters[my_tid]
        self.insert_lock.release()

    def _wait_on_multiprocessing_objects(self, objs):
        """Paranoid wait for a list of standard multiprocessing objects.
        We wake up from time to time to check if someone else got ready.

        :param objs: mp objects to wait on
        :type objs: list of
                    multiprocessing.Connection,
                    socket.socket,
                    multiprocessing.Process.sentinel
        """

        my_tid = threading.get_native_id()
        joined = False
        lonely = False

        while not lonely and not joined:
            # saved this from monkey patching
            ready_objects = original_multiprocessing.connection_wait(objs, self.wait_timeout)

            joined = ready_objects != []
            self.insert_lock.acquire()
            lonely = self.waiters[my_tid] == []
            self.insert_lock.release()

        if joined and not lonely:
            self._handle_wait_return(ready_objects)

        self.insert_lock.acquire()
        del self.objects[my_tid]
        del self.waiters[my_tid]
        self.insert_lock.release()

    def _handle_wait_return(self, ready_objects):
        """Handle the ready objects returned by a wait call.

        :param ready_objs: list of objects that are "ready"
        :type ready_objs: list connections, puids, multiprocessing objects
        """

        my_tid = threading.get_native_id()
        my_pid = os.getpid()

        self.insert_lock.acquire()

        my_waiters = self.waiters[my_tid]

        # add their objects for pickup and notify
        for tid in my_waiters:
            self.ready_objects[tid] = ready_objects
            self.events[tid].set()

        self.insert_lock.release()

    def wait_for_me(self, puids, conns, queues, others):
        """Initiate a wait on puids, connections and multiprocessing objects.
        We spawn service threads for:
            * every list of new multiprocessing or CUID objects
            * every single new connection object
        We do not spawn service threads, if:
            * an object is already waited on

        :param puids: puid object handles to wait on
        :type puids: list
        :param conns: _description_
        :type conns: _type_
        :param queues: _description_
        :type queues: _type_
        :param others: _description_
        :type others: _type_
        :return: _description_
        :rtype: _type_
        """

        my_tid = threading.get_native_id()
        done_ev = threading.Event()

        # spin until we get the lock and can accept more waits
        self.insert_lock.acquire()

        self.events[my_tid] = done_ev

        # add myself as a waiter to existing threads

        my_objects = set(puids + conns + queues + others)

        for tid, objs in self.objects.items():
            if my_objects.intersection(set(objs)):
                self.waiters[tid].append(my_tid)

        # spawn threads for new objects not already waited on
        cur_objects = [obj for lst in self.objects.values() for obj in lst]  # flatten dict of lists

        new_puids = list(set(puids) - set(cur_objects))
        new_conns = list(set(conns) - set(cur_objects))
        new_queues = list(set(queues) - set(cur_objects))
        new_others = list(set(others) - set(cur_objects))

        if new_puids:
            t = threading.Thread(target=self._wait_on_puids, args=(new_puids,), daemon=True)
            t.start()
            self.waiters[t.native_id] = [my_tid]
            self.objects[t.native_id] = new_puids

        # TODO: multi_join for everyone, will break thread hoarding test

        if new_conns:
            for obj in new_conns:  # PE-43870 need to replace with multi-join on channels
                t = threading.Thread(target=self._wait_on_connection, args=(obj,), daemon=True)
                t.start()
                self.waiters[t.native_id] = [my_tid]
                self.objects[t.native_id] = [obj]

        if new_queues:
            for obj in new_queues:  # PE-43870 need to replace with multi-join on channels
                t = threading.Thread(target=self._wait_on_queue, args=(obj,), daemon=True)
                t.start()
                self.waiters[t.native_id] = [my_tid]
                self.objects[t.native_id] = [obj]

        if new_others:
            t = threading.Thread(target=self._wait_on_multiprocessing_objects, args=(new_others,), daemon=True)
            t.start()
            self.waiters[t.native_id] = [my_tid]
            self.objects[t.native_id] = new_others

        self.insert_lock.release()

        return done_ev

    def pickup_and_leave(self, my_objects, pickup):
        """Get the objects ready after the wait for this process.
        Clean internal data structures, even if we timed out.

        :param my_objects: list of objects that were waited on
        :param pickup: bool, that is false we only timed out and pickup ready objects.
        :return: list of objects ready after wait
        """

        my_tid = threading.get_native_id()
        my_ready_objects = []

        self.insert_lock.acquire()

        del self.events[my_tid]

        # remove myself from the waiters dict
        for tid, waiters in self.waiters.items():
            if my_tid in waiters:
                self.waiters[tid].remove(my_tid)

        if pickup:
            all_ready_objects = set(self.ready_objects.pop(my_tid))
            my_ready_objects = list(all_ready_objects.intersection(my_objects))

        self.insert_lock.release()

        return my_ready_objects


_wait_service = None

# these global variables store the original multiprocessing
# functions before monkeypatching

_original_cpu_count = None
_original_active_children = None
_original_current_process = None
_original_parent_process = None
_original_get_logger = None
_original_log_to_stderr = None

_original_Pipe = None
_original_Queue = None
_original_JoinableQueue = None
_original_Value = None
_original_RawValue = None
_original_Array = None
_original_RawArray = None
_original_Client = None
_original_SharedMemory = None
_original_ShareableList = None


class DragonContext(multiprocessing.context.BaseContext):
    """This class patches the :py:mod:`dragon.native` modules into Python multiprocessing using the
    :py:mod:`dragon.mpbridge` modules, when the start method is set to `dragon` and Dragon is imported. Processes,
    whether started through :py:meth:`~dragon.mpbridge.context.DragonContext.Process` or
    :py:meth:`~dragon.mpbridge.context.DragonContext.Pool`, are placed in a round-robin fashion across nodes. This
    behavior can be changed by instead using the lower level :py:mod:`dragon.native` equivalents.

    Example usage:

    .. highlight:: python
    .. code-block:: python

        import dragon  # <<-- import before multiprocessing
        import multiprocessing as mp

        def f(item):
            print(f"I got {item}", flush=True)

        if __name__ == "__main__":
            mp.set_start_method("dragon")  # <<-- set the start method to "dragon"

            with mp.Pool(5) as p:
                p.map(f, [0, 1, 2])
    """

    Process = DragonProcess

    _name = DragonPopen.method

    # these allow us to enable parts of the API original multiprocessing API for debugging
    USE_MPFUNCTIONS = False  # switches all functions under context
    USE_MPQUEUE = False
    USE_MPJQUEUE = False
    USE_MPSQUEUE = False
    USE_MPPOOL = False
    USE_MPPIPE = False
    USE_MPMANAGER = True
    USE_MPLOCK = False
    USE_MPRLOCK = False
    USE_MPCONDITION = False
    USE_MPSEMAPHORE = False
    USE_MPBOUNDSEMAPHORE = False
    USE_MPEVENT = False
    USE_MPBARRIER = False
    USE_MPVALUE = False
    USE_MPRAWVALUE = False
    USE_MPARRAY = False
    USE_MPRAWARRAY = False
    USE_MPSHAREDMEMORY = False
    USE_MPSHAREABLELIST = False
    USE_MPLISTENER = False
    USE_MPCLIENT = False

    def cpu_count(self):
        """Returns the total number of logical CPUs across all nodes. See
        :external+python:py:func:`multiprocessing.cpu_count` for additional information.

        Example usage:

        .. highlight:: python
        .. code-block:: python

            import dragon
            from multiprocessing import cpu_count, set_start_method

            if __name__ == "__main__":
                set_start_method("dragon")
                print(f"There are {cpu_count()} logical CPUs across all nodes", flush=True)

        :return: total number of logical CPUs across all nodes
        :rtype: int
        """
        if multiprocessing.get_start_method() == self._name:
            from ..native.machine import cpu_count

            if self.USE_MPFUNCTIONS:
                return _original_cpu_count()
            else:
                return cpu_count()
        else:
            return multiprocessing.get_context().cpu_count()

    def freeze_support(self):
        """Present to allow CPython unit tests to pass with Dragon. Check whether this is a fake forked process in a
        frozen executable. If so then run code specified by commandline and exit.
        """
        if multiprocessing.get_start_method() == self._name:
            raise NotImplementedError(
                f"Multiprocessing.freeze_support() is not implemented in Dragon, as it is Windows only."
            )
        else:
            return multiprocessing.get_context().freeze_support()

    def log_to_stderr(self, level=None):
        """Turn on logging and add a handler which prints to stderr. See
        :external+python:py:func:`multiprocessing.log_to_stderr` for additional information.

        :param level: the logging level
        :type level: int (e.g., `logging.DEBUG`)
        """

        if multiprocessing.get_start_method() == self._name:
            from .util import log_to_stderr

            if self.USE_MPFUNCTIONS:
                return log_to_stderr(level=level, original=_original_log_to_stderr, use_base_impl=True)
            else:
                return log_to_stderr(level=level, original=_original_log_to_stderr, use_base_impl=False)
        else:
            return multiprocessing.get_context().log_to_stderr(level=level)

    def get_logger(self):
        """Return package logger -- if it does not already exist then it is created. See
        :external+python:py:func:`multiprocessing.get_logger` for additional information.

        :return: the logger used by `multiprocessing`
        :rtype: :external+python:py:class:`logging.Logger`
        """
        if multiprocessing.get_start_method() == self._name:
            from .util import get_logger

            if self.USE_MPFUNCTIONS:
                return get_logger(original=_original_get_logger, use_base_impl=True)
            else:
                return get_logger(original=_original_get_logger, use_base_impl=False)
        else:
            return multiprocessing.get_context().get_logger()

    @staticmethod
    def wait(object_list, timeout=None):
        """Implements a wait on a list of various Dragon-specific and multiprocessing objects. See
        :external+python:py:func:`multiprocessing.connection.wait` for additional information.

        The following types can be waited upon:

            * :py:class:`dragon.mpbridge.process.PUID`,
            * :py:class:`dragon.infrastructure.connection.Connection`,
            * :py:class:`dragon.mpbridge.queue.DragonQueue`,
            * standard multiprocessing objects: sockets, mp.connection, mp.sentinel

        Example usage:

        .. highlight:: python
        .. code-block:: python

            import dragon
            from multiprocessing import Process, Queue, set_start_method
            from multiprocessing.connection import wait
            import time

            def f(q):
                time.sleep(5)
                q.put("Hello!")

            if __name__ == "__main__":
                set_start_method("dragon")

                q = Queue()
                p = Process(target=f, args=(q,))
                p.start()
                waitlist = [q, p.sentinel]
                wait(waitlist)   # wait for data to be in the Queue and the process to complete

        :param object_list: list of objects to wait on
        :type object_list: supported types
        :param timeout: time to wait before return, defaults to None
        :type timeout: float, optional
        :return: returns a list of objects that are "ready"
        :rtype: list of supported types
        """

        start = time.monotonic()

        if timeout is None:
            timeout = 1000000

        # check if we're already done
        ready_objects, puids, conns, queues, others = _sort_and_check_for_ready(object_list)
        if ready_objects or timeout <= 0:
            return ready_objects  # that was easy

        # use threading
        global _wait_service
        if _wait_service is None:
            _wait_service = _WaitService(timeout)

        _wait_service.insert_lock.acquire()
        if timeout == 1000000:
            _wait_service.wait_timeout = _wait_service.THREAD_WAIT  # we need a value for the polling timeout
        else:
            # Multiply by 4 because he have previously divided by 4 the value of _wait_service.wait_timeout.
            # Setting the value of _wait_service.wait_timeout to a quarter of the minimum value among the
            # the timeouts we've seen so far is kind of random, but works good enough.
            # See PE-44688 for more info.
            _wait_service.wait_timeout = min(_wait_service.wait_timeout * 4, timeout) / 4
        _wait_service.insert_lock.release()

        done_ev = _wait_service.wait_for_me(puids, conns, queues, others)

        end = time.monotonic()

        if timeout:
            exact_timeout = max(0, timeout - (end - start))
        else:
            exact_timeout = None

        # don't wait longer than we have to
        pickup = done_ev.wait(timeout=exact_timeout)

        ready_objects = _wait_service.pickup_and_leave(object_list, pickup)

        return ready_objects

    def Manager(self):
        """Not implemented"""
        if multiprocessing.get_start_method() == self._name:
            from .managers import Manager

            m = Manager(ctx=self.get_context(), use_base_impl=self.USE_MPMANAGER)
        else:
            m = multiprocessing.get_context().Manager()

        m.start()

        return m

    def Pipe(self, duplex=True):
        """Return a pair of :py:class:`~dragon.infrastructure.connection.Connection` objects. Note that Dragon
        does not use true Linux file descriptors. See
        :external+python:py:class:`multiprocessing.connection.Connection` for additional information.

        Example usage:

        .. highlight:: python
        .. code-block:: python

            import dragon
            from multiprocessing import Process, Pipe, set_start_method

            def f(c):
                d = c.recv()
                d = f"{d} to you!"
                c.send(d)
                c.close()

            if __name__ == "__main__":
                set_start_method("dragon")

                mine, theirs = Pipe()
                p = Process(target=f, args=(thiers, ))
                p.start()
                mine.send("hello")
                d = mine.recv()
                print(d, flush=True)
                mine.close()
                p.join()

        :param duplex: True or False if bi-directional communication is desired
        :type duplex: bool, optional
        :return: returns a pair of (conn1, conn2). If `duplex` is True (default) the pipe is bidirectional. If `duplex` is False, `conn1` can only be used for receiving and `conn2` only for sending.
        :rtype: (:py:class:`~dragon.infrastructure.connection.Connection`, :py:class:`~dragon.infrastructure.connection.Connection`)
        """
        if multiprocessing.get_start_method() == self._name:
            from .connection import Pipe

            return Pipe(duplex, original=_original_Pipe, use_base_impl=self.USE_MPPIPE)
        else:
            return _original_Pipe(duplex)

    def Lock(self, *, ctx=None):
        """A non-recursive lock object: a close analog of :external+python:py:class:`threading.Lock`. See
        :external+python:py:class:`multiprocessing.Lock` for additional information.

        Example usage:

        .. highlight:: python
        .. code-block:: python

            import dragon
            from multiprocessing import Process, Lock, set_start_method
            import time

            def f(name, l):
                with l:
                    print(f"{name} has the lock!", flush=True)
                    time.sleep(2)  # operate on some shared resource

            if __name__ == "__main__":
                set_start_method("dragon")

                l = Lock()
                p = Process(target=f, args=("Worker", l))
                p.start()
                f("Manager", l)
                p.join()

        :return: non-recursive lock object
        :rtype: :py:class:`~dragon.mpbridge.synchronize.DragonLock`
        """
        if multiprocessing.get_start_method() == self._name:
            from .synchronize import Lock

            return Lock(ctx=self.get_context(), use_base_impl=self.USE_MPLOCK)
        else:
            if ctx:
                return multiprocessing.get_context().Lock(ctx=ctx)
            else:
                return multiprocessing.get_context().Lock()

    def RLock(self, *, ctx=None):
        """A recursive lock object: a close analog of :external+python:py:class:`threading.RLock`.See
        :external+python:py:class:`multiprocessing.RLock` for additional information.

        Example usage:

        .. highlight:: python
        .. code-block:: python

            import dragon
            from multiprocessing import Process, RLock, set_start_method
            import time

            def f(name, l):
                # recursively acquired and released by this process only
                with l:
                    with l:
                        print(f"{name} has the lock!", flush=True)
                        time.sleep(2)  # operate on some shared resource

            if __name__ == "__main__":
                set_start_method("dragon")

                l = Lock()
                p = Process(target=f, args=("Worker", l))
                p.start()
                f("Manager", l)
                p.join()

        :return: recursive lock object
        :rtype: :py:class:`~dragon.mpbridge.synchronize.DragonRLock`
        """
        if multiprocessing.get_start_method() == self._name:
            from .synchronize import RLock

            return RLock(ctx=self.get_context(), use_base_impl=self.USE_MPRLOCK)
        else:
            if ctx:
                return multiprocessing.get_context().RLock(ctx=ctx)
            else:
                return multiprocessing.get_context().RLock()

    def Condition(self, *, ctx=None, lock=None):
        """A condition variable: a close analog of :external+python:py:class:`threading.Condition`. See
        :external+python:py:class:`multiprocessing.Condition` for additional information.

        Example usage:

        .. highlight:: python
        .. code-block:: python

            import dragon
            from multiprocessing import Process, Condition, set_start_method
            import time

            def f(cond, n):
                cond.acquire()
                cond.wait()
                print(f"Worker {n} woke up", flush=True)
                cond.release()

            if __name__ == "__main__":
                set_start_method("dragon")

                cond = Condition()
                ps = []
                for i in range(3):
                    p = Process(target=f, args=(cond, i,))
                    p.start()
                    ps.append(p)

                time.sleep(5)  # should really use a Barrier, but still give workers time to acquire

                cond.acquire()
                cond.notify(n=1)
                cond.release()

                cond.acquire()
                cond.notify_all()
                cond.release()

                for p in ps:
                    p.join()

        :param lock: lock to use with the Condition, otherwise an :py:class:`~dragon.mpbridge.synchronize.DragonRLock` is created and used
        :type object_list: None, RLock, Lock, optional
        :return: condition variable
        :rtype: :py:class:`~dragon.mpbridge.synchronize.DragonCondition`
        """
        if multiprocessing.get_start_method() == self._name:
            from .synchronize import Condition

            return Condition(lock, ctx=self.get_context(), use_base_impl=self.USE_MPCONDITION)
        else:
            if ctx:
                return multiprocessing.get_context().Condition(lock, ctx=ctx)
            else:
                return multiprocessing.get_context().Condition(lock, ctx=self.get_context())

    def Semaphore(self, value=1):
        """A semaphore object: a close analog of :external+python:py:class:`threading.Semaphore`. See
        :external+python:py:class:`multiprocessing.Semaphore` for additional information.

        Example usage:

        .. highlight:: python
        .. code-block:: python

            import dragon
            from multiprocessing import Process, Semaphore, set_start_method
            import time

            def f(sem):
                sem.acquire()
                if n == 2:
                    time.sleep(8)
                    sem.release()

            if __name__ == "__main__":
                set_start_method("dragon")

                # use a Semaphore to hold a process back from proceeding until others have passed through a code block
                sem = Semaphore(value=3)
                ps = []
                for _ in range(3):
                    p = Process(target=f, args=(sem,))
                    p.start()
                    ps.append(p)

                time.sleep(5)  # should really use a Barrier, but still give workers time to acquire

                sem.acquire()  # blocks until worker 2 calls release

                for p in ps:
                    p.join()

        :param value: initial value for the internal counter, defaults to 1
        :type value: int, optional
        :return: semaphore
        :rtype: :py:class:`~dragon.mpbridge.synchronize.DragonSemaphore`
        """
        if multiprocessing.get_start_method() == self._name:
            from .synchronize import Semaphore

            return Semaphore(value, ctx=self.get_context(), use_base_impl=self.USE_MPSEMAPHORE)
        else:
            return multiprocessing.get_context().Semaphore(value, ctx=self.get_context())

    def BoundedSemaphore(self, value=1):
        """A bounded semaphore object: a close analog of :external+python:py:class:`threading.BoundedSemaphore`. See
        :external+python:py:class:`multiprocessing.BoundedSemaphore` for additional information.

        :param value: initial value for the internal counter, defaults to 1
        :type value: int, optional
        :return: semaphore
        :rtype: :py:class:`~dragon.mpbridge.synchronize.DragonBoundedSemaphore`
        """
        if multiprocessing.get_start_method() == self._name:
            from .synchronize import BoundedSemaphore

            return BoundedSemaphore(value, ctx=self.get_context(), use_base_impl=self.USE_MPBOUNDSEMAPHORE)
        else:
            return multiprocessing.get_context().BoundedSemaphore(value, ctx=self.get_context())

    def Event(self, *, ctx=None):
        """An event object: a close analog of :external+python:py:class:`threading.Event`. See
        :external+python:py:class:`multiprocessing.Event` for additional information.

        Example usage:

        .. highlight:: python
        .. code-block:: python

            import dragon
            from multiprocessing import Process, Event, set_start_method
            import time

            def f(ev):
                while not ev.is_set():
                    time.sleep(1)  # or do other work

            if __name__ == "__main__":
                set_start_method("dragon")

                ev = Event()
                ps = []
                for _ in range(3):
                    p = Process(target=f, args=(ev,))
                    p.start()
                    ps.append(p)

                time.sleep(5)  # or do some work

                ev.set()  # alert the workers

                for p in ps:
                    p.join()

        :return: event
        :rtype: :py:class:`~dragon.mpbridge.synchronize.DragonEvent`
        """
        if multiprocessing.get_start_method() == self._name:
            from .synchronize import Event

            return Event(ctx=self.get_context(), use_base_impl=self.USE_MPEVENT)
        else:
            if ctx:
                return multiprocessing.get_context().Event(ctx=ctx)
            else:
                return multiprocessing.get_context().Event()

    def Barrier(self, parties, *, ctx=None, action=None, timeout=None):
        """A barrier object: a close analog of :external+python:py:class:`threading.Barrier`. See
        :external+python:py:class:`multiprocessing.Barrier` for additional information.

        Example usage:

        .. highlight:: python
        .. code-block:: python

            import dragon
            from multiprocessing import Process, Barrier, set_start_method

            def f(bar):
                bar.wait()

            if __name__ == "__main__":
                set_start_method("dragon")

                bar = Barrier(4)
                ps = []
                for _ in range(3):
                    p = Process(target=f, args=(bar,))
                    p.start()
                    ps.append(p)

                bar.wait()  # blocks until all workers and this process are in the barrier

                for p in ps:
                    p.join()

        :param parties: number of parties participating in the barrier
        :type value: int
        :param action: callable executed by one of the processes when they are released
        :type value: Callable, optional
        :param timeout: default timeout to use if none is specified to :py:meth:`~dragon.mpbridge.synchronize.Barrier.wait`
        :type timeout: float, None, optional
        :return: barrier
        :rtype: :py:class:`~dragon.mpbridge.synchronize.DragonBarrier`
        """
        if multiprocessing.get_start_method() == self._name:
            from .synchronize import Barrier

            return Barrier(parties, action, timeout, ctx=self.get_context(), use_base_impl=self.USE_MPBARRIER)
        else:
            if ctx:
                return multiprocessing.get_context().Barrier(parties, action=action, timeout=timeout, ctx=ctx)
            else:
                return multiprocessing.get_context().Barrier(
                    parties, action=action, timeout=timeout, ctx=self.get_context()
                )

    def Queue(self, maxsize=0):
        """A shared FIFO-style queue. Unlike the base implementation, this class is implemented using
        :py:class:`dragon.channels` and provides more flexibility in terms of the size of items without
        requiring a helper thread. See :external+python:py:class:`multiprocessing.Queue` for additional information.

        Example usage:

        .. highlight:: python
        .. code-block:: python

            import dragon
            from multiprocessing import Process, Queue, set_start_method

            def f(inq, outq):
                v = inq.get()
                v += 1
                outq.put(v)

            if __name__ == "__main__":
                set_start_method("dragon")

                workq = Queue()
                resq = Queue()
                p = Process(target=f, args=(workq, resq,))
                p.start()

                workq.put(1)
                res = resq.get()
                print(f"Put in 1 and got back {res}", flush=True)
                p.join()

        :param maxsize: maximum number of entries that can reside at once, default of 0 implies a value of 100
        :type value: int, optional
        :return: queue
        :rtype: :py:class:`~dragon.mpbridge.queues.DragonQueue`
        """
        if multiprocessing.get_start_method() == self._name:
            from .queues import Queue

            if self.USE_MPQUEUE:
                q = Queue(maxsize=maxsize, ctx=self.get_context(), use_base_impl=True)
                # patches internal dragon.infrastructure.connection.Connection
                # to not send EOT when it is closed.
                # q._writer.ghost = True
                return q
            else:
                return Queue(maxsize=maxsize, ctx=self.get_context(), use_base_impl=False)
        else:
            return multiprocessing.get_context().Queue(maxsize)

    def JoinableQueue(self, *, ctx=None, maxsize=0):
        """A subclass of :py:class:`~dragon.mpbridge.queues.Queue` that additionally has
        :py:meth:`~dragon.mpbridge.queues.JoinableQueue.task_done` and
        :py:meth:`~dragon.mpbridge.queues.JoinableQueue.join` methods. See
        :external+python:py:class:`multiprocessing.JoinableQueue` for additional information.

        Example usage:

        .. highlight:: python
        .. code-block:: python

            import dragon
            from multiprocessing import Process, JoinableQueue, set_start_method
            import queue

            def f(q):
                while True:
                    try:
                        task = q.get(timeout=1.0)
                    except queue.Empty:
                        break

                    print(f"got task {task}", flush=True)
                    q.task_done()

            if __name__ == "__main__":
                set_start_method("dragon")

                workq = JoinableQueue()
                p = Process(target=f, args=(workq,))
                p.start()

                for i in range(10):
                    workq.put(i)
                workq.join()

                p.join()

        :param maxsize: maximum number of entries that can reside at once, default of 0 implies a value of 100
        :type value: int, optional
        :return: queue
        :rtype: :py:class:`~dragon.mpbridge.queues.DragonJoinableQueue`
        """
        if multiprocessing.get_start_method() == self._name:
            from .queues import JoinableQueue

            if self.USE_MPJQUEUE:
                q = JoinableQueue(maxsize=maxsize, ctx=self.get_context(), use_base_impl=True)
                # patches internal dragon.infrastructure.connection.Connection
                # to not send EOT when it is closed.
                # q._writer.ghost = True
                return q
            else:
                if ctx:
                    return JoinableQueue(maxsize=maxsize, ctx=ctx, use_base_impl=False)
                else:
                    return JoinableQueue(maxsize=maxsize, ctx=self.get_context(), use_base_impl=False)
        else:
            return multiprocessing.get_context().JoinableQueue(maxsize)

    def SimpleQueue(self, *, ctx=None):
        """A :py:class:`~dragon.mpbridge.queues.Queue` object with fewer methods to match the API of
        :external+python:py:class:`multiprocessing.SimpleQueue`.

        Example usage:

        .. highlight:: python
        .. code-block:: python

            import dragon
            from multiprocessing import Process, SimpleQueue, set_start_method

            def f(q):
                task = q.get()
                print(f"got task {task}", flush=True)

            if __name__ == "__main__":
                set_start_method("dragon")

                workq = SimpleQueue()
                p = Process(target=f, args=(workq,))
                p.start()

                workq.put(1)

                p.join()

        :return: queue
        :rtype: :py:class:`~dragon.mpbridge.queues.DragonSimpleQueue`
        """
        if multiprocessing.get_start_method() == self._name:
            from .queues import SimpleQueue, Queue

            if self.USE_MPSQUEUE:
                q = SimpleQueue(ctx=self.get_context(), use_base_impl=True)
                # patches internal dragon.infrastructure.connection.Connection
                # to not send EOT when it is closed.
                # q._writer.ghost = True
                return q
            else:
                # TODO, PE-41041 we need a Dragon-native SimpleQueue (wrapped Pipe), but for now we can try
                # return DragonSimpleQueue(ctx=self.get_context(), use_base_impl=False)
                if ctx:
                    return SimpleQueue(ctx=ctx, use_base_impl=False)
                else:
                    return SimpleQueue(ctx=self.get_context(), use_base_impl=False)
        else:
            return multiprocessing.get_context().SimpleQueue()

    def Pool(self, processes=None, initializer=None, initargs=(), maxtasksperchild=None):
        """A :py:class:`~dragon.mpbridge.pool.Pool` object that consists of a pool of worker processes to which
        jobs can be submitted.
        See :external+python:py:class:`multiprocessing.pool.Pool` for additional information.

        Example usage:

        .. highlight:: python
        .. code-block:: python

            import dragon
            import multiprocessing as mp

            def f(item):
                print(f"I got {item}", flush=True)

            if __name__ == "__main__":
                mp.set_start_method("dragon")

                with mp.Pool(5) as p:
                    p.map(f, [0, 1, 2])

        :param processes: the number of workers to use, defaults to :py:attr:`dragon.native.machine.System.nnodes`
        :type value: int, optional
        :param initializer: function to call in each worker at startup
        :type value: Callable, optional
        :param initargs: arguments to pass to `initializer`
        :type value: tuple, optional
        :param maxtasksperchild: if not `None`, the maximum number of tasks a worker processes before it is restarted
        :type value: int, optional
        :return: pool
        :rtype: :py:class:`~dragon.mpbridge.pool.DragonPool`
        """
        if multiprocessing.get_start_method() == self._name:
            from .pool import Pool

            return Pool(
                processes,
                initializer,
                initargs,
                maxtasksperchild,
                context=self.get_context(),
                use_base_impl=self.USE_MPPOOL,
            )
        else:
            return multiprocessing.get_context().Pool(
                processes=processes, initializer=initializer, initargs=initargs, maxtasksperchild=maxtasksperchild
            )

    def RawValue(self, typecode_or_type, *args):
        """Return a ctypes object implemented with :py:class:`dragon.channels`. See
        :external+python:py:func:`multiprocessing.sharedctypes.RawValue` for additional information.

        Example usage:

        .. highlight:: python
        .. code-block:: python

            import dragon
            from multiprocessing import Process, RawValue, set_start_method

            def f(v):
                print(f"I see {v.value}", flush=True)

            if __name__ == "__main__":
                set_start_method("dragon")

                value = RawValue("i", 42)
                p = Process(target=f, args=(value,))
                p.start()

                p.join()

        :param typecode_or_type: either a ctypes type or a one character typecode
        :type typecode_or_type: see :external+python:py:mod:`multiprocessing.sharedctypes`
        :return: raw value
        :rtype: :py:class:`~dragon.mpbridge.sharedctypes.DragonRawValue`
        """
        if multiprocessing.get_start_method() == self._name:
            from .sharedctypes import RawValue

            return RawValue(typecode_or_type, *args, original=_original_RawValue, use_base_impl=self.USE_MPVALUE)
        else:
            return _original_RawValue(typecode_or_type, *args)

    def RawArray(self, typecode_or_type, size_or_initializer):
        """Return a ctypes array implemented with :py:class:`dragon.channels`. See
        :external+python:py:func:`multiprocessing.sharedctypes.RawArray` for additional information.

        Example usage:

        .. highlight:: python
        .. code-block:: python

            import dragon
            from multiprocessing import Process, RawArray, set_start_method

            def f(a):
                print(f"I see {a[:]}", flush=True)

            if __name__ == "__main__":
                set_start_method("dragon")

                arr = RawArray("i", 11 * [42])
                p = Process(target=f, args=(arr,))
                p.start()

                p.join()

        :param typecode_or_type: either a ctypes type or a one character typecode
        :type value: see :external+python:py:mod:`multiprocessing.sharedctypes`
        :param size_or_initializer: either an integer length or sequence of values to initialize with
        :type size_or_initializer: int or sequence
        :return: raw array
        :rtype: :py:class:`~dragon.mpbridge.sharedctypes.DragonRawArray`
        """
        if multiprocessing.get_start_method() == self._name:
            from .sharedctypes import RawArray

            return RawArray(
                typecode_or_type, size_or_initializer, original=_original_RawArray, use_base_impl=self.USE_MPARRAY
            )
        else:
            return _original_RawArray(typecode_or_type, size_or_initializer)

    def Value(self, typecode_or_type, *args, lock=True, ctx=None):
        """The same as :py:class:`~dragon.mpbridge.sharedctypes.RawValue` except that depending on the value of `lock`
        a synchronizing wrapper may be returned. See
        :external+python:py:func:`multiprocessing.sharedctypes.Value` for additional information.

        Example usage:

        .. highlight:: python
        .. code-block:: python

            import dragon
            from multiprocessing import Process, Value, set_start_method

            def f(v):
                print(f"I see {v.value}", flush=True)
                v.value += 1

            if __name__ == "__main__":
                set_start_method("dragon")

                value = Value("i", 42)
                p = Process(target=f, args=(value,))
                p.start()

                p.join()
                print(f"I now see {value.value}", flush=True)

        :param typecode_or_type: either a ctypes type or a one character typecode
        :type value: see :external+python:py:mod:`multiprocessing.sharedctypes`
        :param lock: a lock object or a flag to create a lock
        :type lock: a :py:class:`~dragon.mpbridge.synchronize.Lock`, bool, optional
        :return: value
        :rtype: :py:class:`~dragon.mpbridge.sharedctypes.DragonValue`
        """
        if multiprocessing.get_start_method() == self._name:
            from .sharedctypes import Value

            return Value(
                typecode_or_type, *args, lock=lock, ctx=ctx, original=_original_Value, use_base_impl=self.USE_MPRAWVALUE
            )
        else:
            return _original_Value(typecode_or_type, *args, lock=lock, ctx=ctx)

    def Array(self, typecode_or_type, size_or_initializer, *, lock=True, ctx=None):
        """The same as :py:class:`~dragon.mpbridge.sharedctypes.RawArray` except that depending on the value of `lock`
        a synchronizing wrapper may be returned. See
        :external+python:py:func:`multiprocessing.sharedctypes.Array` for additional information.

        Example usage:

        .. highlight:: python
        .. code-block:: python

            import dragon
            from multiprocessing import Process, Array, set_start_method

            def f(a):
                with a.get_lock():
                    print(f"I see {a[:]}", flush=True)
                    a[0] = 99

            if __name__ == "__main__":
                set_start_method("dragon")

                arr = Array("i", 11 * [42])
                p = Process(target=f, args=(arr,))
                p.start()

                p.join()
                with arr.get_lock():
                    print(f"I see {arr[:]}", flush=True)

        :param typecode_or_type: either a ctypes type or a one character typecode
        :type value: see :external+python:py:mod:`multiprocessing.sharedctypes`
        :param size_or_initializer: either an integer length or sequence of values to initialize with
        :type size_or_initializer: int or sequence
        :param lock: a lock object or a flag to create a lock
        :type lock: a :py:class:`~dragon.mpbridge.synchronize.Lock`, bool, optional
        :return: array
        :rtype: :py:class:`~dragon.mpbridge.sharedctypes.DragonArray`
        """
        if multiprocessing.get_start_method() == self._name:
            from .sharedctypes import Array

            return Array(
                typecode_or_type,
                size_or_initializer,
                lock=lock,
                ctx=ctx,
                original=_original_Array,
                use_base_impl=self.USE_MPRAWARRAY,
            )

        else:
            return _original_Array(typecode_or_type, size_or_initializer, lock=lock, ctx=ctx)

    def Listener(self, address=None, family=None, backlog=1, authkey=None):
        """Not implemented"""
        if multiprocessing.get_start_method() == self._name:
            from .connection import Listener

            return Listener(
                address=address, family=family, backlog=backlog, authkey=authkey, use_base_impl=self.USE_MPLISTENER
            )

        else:
            return multiprocessing.get_context().Listener(
                address=address, family=family, backlog=backlog, authkey=authkey
            )

    def Client(self, address, family=None, authkey=None):
        """Not implemented"""
        if multiprocessing.get_start_method() == self._name:
            from .connection import Client

            return Client(
                address, family=family, authkey=authkey, original=_original_Client, use_base_impl=self.USE_MPCLIENT
            )
        else:
            return _original_Client(address, family=family, authkey=authkey)

    def _apply_patches(self):
        """Additional monkeypatching.
        Unfortunately multiprocessing doesn't use the context for the whole API internally, so
        we have to force it to use our Dragon versions.
        Here we overwrite some of the internal multiprocessing classes explicitly with our own.
        Note that the corresponding files have to be explicitly imported at the top for this to work.
        """

        # save original multiprocessing functions
        # (and classes if their not in context)
        global _original_cpu_count
        global _original_active_children
        global _original_current_process
        global _original_parent_process
        global _original_get_logger
        global _original_log_to_stderr

        global _original_Pipe
        global _original_Value
        global _original_RawValue
        global _original_Array
        global _original_RawArray
        global _original_Client
        global _original_SharedMemory
        global _original_ShareableList
        global _original_Queue
        global _original_JoinableQueue

        if _original_cpu_count is None:
            _original_cpu_count = multiprocessing.cpu_count

        if _original_active_children is None:
            _original_active_children = multiprocessing.active_children

        if _original_current_process is None:
            _original_current_process = multiprocessing.current_process

        if _original_parent_process is None:
            _original_parent_process = multiprocessing.parent_process

        if _original_get_logger is None:
            _original_get_logger = multiprocessing.get_logger

        if _original_log_to_stderr is None:
            _original_log_to_stderr = multiprocessing.log_to_stderr

        if _original_Queue is None:
            _original_Queue = multiprocessing.queues.Queue

        if _original_JoinableQueue is None:
            _original_JoinableQueue = multiprocessing.queues.JoinableQueue

        if _original_Pipe is None:
            _original_Pipe = multiprocessing.connection.Pipe

        if _original_RawArray is None:
            _original_RawArray = multiprocessing.sharedctypes.RawArray

        if _original_Array is None:
            _original_Array = multiprocessing.sharedctypes.Array

        if _original_RawValue is None:
            _original_RawValue = multiprocessing.sharedctypes.RawValue

        if _original_Value is None:
            _original_Value = multiprocessing.sharedctypes.Value

        if _original_Client is None:
            _original_Client = multiprocessing.connection.Client

        if _original_SharedMemory is None:
            _original_SharedMemory = multiprocessing.shared_memory.SharedMemory

        if _original_ShareableList is None:
            _original_ShareableList = multiprocessing.shared_memory.ShareableList

    # this method is called by BaseContext.get_context() before
    # returning the actual context for the given startmethod
    def _check_available(self):
        pass
