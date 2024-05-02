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

    THREAD_WAIT = 0.1  # [sec] time interval for a thread to check if it needs to stop waiting, used when timeout is None
    GC_INTERVAL = 10  # [sec] how often to look for abandoned waiters/ready_objects from vanished waiters
    # exact value not important as it's not related to timeout

    def __init__(self, timeout):

        self.wait_timeout = timeout # [sec] time interval for a thread to check if it needs to stop waiting

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
            t = threading.Thread(
                target=self._wait_on_multiprocessing_objects, args=(new_others,), daemon=True
            )
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
    """The Dragon context patches the Dragon Native API into
    Python Multiprocessing using the dragon.mpbridge modules,
    when the startmethod is set to 'dragon' and Dragon is imported.
    This is done in 2 ways:

    1. The Dragon context is selected and with it all of
    the replaced API (see dragon/__init__.py).

    2. The module is monkey patched during import, to ensure
    users that don't use the context to get parts of the API
    still get the dragon/mpbridge/*.py methods and classes.
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
        """Returns the number of CPUs in the system"""
        if multiprocessing.get_start_method() == self._name:
            from ..native.machine import cpu_count

            if self.USE_MPFUNCTIONS:
                return _original_cpu_count()
            else:
                return cpu_count()
        else:
            return multiprocessing.get_context().cpu_count()

    def freeze_support(self):
        """Check whether this is a fake forked process in a frozen executable.
        If so then run code specified by commandline and exit.
        """
        if multiprocessing.get_start_method() == self._name:
            raise NotImplementedError(
                f"Multiprocessing.freeze_support() is not implemented in Dragon, as it is Windows only."
            )
        else:
            return multiprocessing.get_context().freeze_support()

    def log_to_stderr(self, level=None):
        """Turn on logging and add a handler which prints to stderr"""

        if multiprocessing.get_start_method() == self._name:
            from .util import log_to_stderr

            if self.USE_MPFUNCTIONS:
                return log_to_stderr(level=level, original=_original_log_to_stderr, use_base_impl=True)
            else:
                return log_to_stderr(level=level, original=_original_log_to_stderr, use_base_impl=False)
        else:
            return multiprocessing.get_context().log_to_stderr(level=level)

    def get_logger(self):
        """Return package logger -- if it does not already exist then
        it is created.
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
        """Implement a wait on a list of various Dragon and Multiprocessing objects.

        We aim to support the following types:
            * dragon.mpbridge.process.PUID,
            * dragon.infrastructure.connection.Connection,
            * dragon.infrastructure.connection.CUID, (broken)
            * dragon.mpbridge.queue.DragonQueue,
            * dragon.mpbridge.queue.Faker, (broken)
            * standard multiprocessing objects: sockets, mp.connection, mp.sentinel

        :param object_list: list of objects to wait on
        :type object_list: supported types
        :param timeout: time to wait before return, defaults to None
        :type timeout: float, optional
        :return: returns a list of objects that are "ready"
        :rtype: list of supported types
        """

        my_tid = threading.get_native_id()

        start = time.monotonic()

        if timeout == None:
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
            _wait_service.wait_timeout = _wait_service.THREAD_WAIT # we need a value for the polling timeout
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
        if multiprocessing.get_start_method() == self._name:
            from .managers import Manager

            m = Manager(ctx=self.get_context(), use_base_impl=self.USE_MPMANAGER)
        else:
            m = multiprocessing.get_context().Manager()

        m.start()

        return m

    def Pipe(self, duplex=True):
        if multiprocessing.get_start_method() == self._name:
            from .connection import Pipe

            return Pipe(duplex, original=_original_Pipe, use_base_impl=self.USE_MPPIPE)
        else:
            return _original_Pipe(duplex)

    def Lock(self):
        """Returns a non-recursive lock object"""
        if multiprocessing.get_start_method() == self._name:
            from .synchronize import Lock

            return Lock(ctx=self.get_context(), use_base_impl=self.USE_MPLOCK)
        else:
            return multiprocessing.get_context().Lock(ctx=self.get_context())

    def RLock(self):
        """Returns a recursive lock object"""
        if multiprocessing.get_start_method() == self._name:
            from .synchronize import RLock

            return RLock(ctx=self.get_context(), use_base_impl=self.USE_MPRLOCK)
        else:
            return multiprocessing.get_context().RLock(ctx=self.get_context())

    def Condition(self, lock=None):
        """Returns a condition object"""
        if multiprocessing.get_start_method() == self._name:
            from .synchronize import Condition

            return Condition(lock, ctx=self.get_context(), use_base_impl=self.USE_MPCONDITION)
        else:
            return multiprocessing.get_context().Condition(lock, ctx=self.get_context())

    def Semaphore(self, value=1):
        """Returns a semaphore object"""
        if multiprocessing.get_start_method() == self._name:
            from .synchronize import Semaphore

            return Semaphore(value, ctx=self.get_context(), use_base_impl=self.USE_MPSEMAPHORE)
        else:
            return multiprocessing.get_context().Semaphore(value, ctx=self.get_context())

    def BoundedSemaphore(self, value=1):
        """Returns a semaphore object"""
        if multiprocessing.get_start_method() == self._name:
            from .synchronize import BoundedSemaphore

            return BoundedSemaphore(value, ctx=self.get_context(), use_base_impl=self.USE_MPBOUNDSEMAPHORE)
        else:
            return multiprocessing.get_context().BoundedSemaphore(value, ctx=self.get_context())

    def Event(self):
        """Returns an event object"""
        if multiprocessing.get_start_method() == self._name:
            from .synchronize import Event

            return Event(ctx=self.get_context(), use_base_impl=self.USE_MPEVENT)
        else:
            return multiprocessing.get_context().Event(ctx=self.get_context())

    def Barrier(self, parties, action=None, timeout=None):
        """Returns a barrier object"""
        if multiprocessing.get_start_method() == self._name:
            from .synchronize import Barrier

            return Barrier(parties, action, timeout, ctx=self.get_context(), use_base_impl=self.USE_MPBARRIER)
        else:
            return multiprocessing.get_context().Barrier(
                parties, action=action, timeout=timeout, ctx=self.get_context()
            )

    def Queue(self, maxsize=0):
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

    def JoinableQueue(self, maxsize=0):
        if multiprocessing.get_start_method() == self._name:
            from .queues import JoinableQueue

            if self.USE_MPJQUEUE:
                q = JoinableQueue(maxsize=maxsize, ctx=self.get_context(), use_base_impl=True)
                # patches internal dragon.infrastructure.connection.Connection
                # to not send EOT when it is closed.
                # q._writer.ghost = True
                return q
            else:
                return JoinableQueue(maxsize=maxsize, ctx=self.get_context(), use_base_impl=False)
        else:
            return multiprocessing.get_context().JoinableQueue(maxsize)

    def SimpleQueue(self):
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
                return SimpleQueue(ctx=self.get_context(), use_base_impl=False)
        else:
            return multiprocessing.get_context().SimpleQueue()

    def Pool(self, processes=None, initializer=None, initargs=(), maxtasksperchild=None):
        """Returns a process pool object"""
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
                processes=processes,
                initializer=initializer,
                initargs=initargs,
                maxtasksperchild=maxtasksperchild,
            )

    def RawValue(self, typecode_or_type, *args):
        """Returns a shared object"""
        if multiprocessing.get_start_method() == self._name:
            from .sharedctypes import RawValue

            return RawValue(
                typecode_or_type, *args, original=_original_RawValue, use_base_impl=self.USE_MPVALUE
            )
        else:
            return _original_RawValue(typecode_or_type, *args)

    def RawArray(self, typecode_or_type, size_or_initializer):
        """Returns a shared array"""
        if multiprocessing.get_start_method() == self._name:
            from .sharedctypes import RawArray

            return RawArray(
                typecode_or_type,
                size_or_initializer,
                original=_original_RawArray,
                use_base_impl=self.USE_MPARRAY,
            )
        else:
            return _original_RawArray(typecode_or_type, size_or_initializer)

    def Value(self, typecode_or_type, *args, lock=True, ctx=None):
        """Returns a synchronized shared object"""
        if multiprocessing.get_start_method() == self._name:
            from .sharedctypes import Value

            return Value(
                typecode_or_type,
                *args,
                lock=lock,
                ctx=ctx,
                original=_original_Value,
                use_base_impl=self.USE_MPRAWVALUE,
            )
        else:
            return _original_Value(typecode_or_type, *args, lock=lock, ctx=ctx)

    def Array(self, typecode_or_type, size_or_initializer, *, lock=True, ctx=None):
        """Returns a synchronized shared array"""
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
        if multiprocessing.get_start_method() == self._name:
            from .connection import Listener

            return Listener(
                address=address,
                family=family,
                backlog=backlog,
                authkey=authkey,
                use_base_impl=self.USE_MPLISTENER,
            )

        else:
            return multiprocessing.get_context().Listener(
                address=address, family=family, backlog=backlog, authkey=authkey
            )

    def Client(self, address, family=None, authkey=None):
        if multiprocessing.get_start_method() == self._name:
            from .connection import Client

            return Client(
                address,
                family=family,
                authkey=authkey,
                original=_original_Client,
                use_base_impl=self.USE_MPCLIENT,
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
