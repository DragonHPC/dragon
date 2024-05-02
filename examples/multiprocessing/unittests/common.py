"""Common imports, functions and constants shared among all tests.
"""

import unittest
import os
import sys
import time
import operator
import gc

import test.support
try: 
    from test.support.import_helper import import_module
    from test.support.threading_helper import join_thread 
except ImportError:
    #location prior to Python 3.10
    from test.support import import_module
    from test.support import join_thread 


import threading

# Dragon must be imported before multiprocessing
import dragon

import multiprocessing

from multiprocessing import util
from multiprocessing.connection import wait

# Skip tests if _multiprocessing wasn't built.
_multiprocessing = import_module("_multiprocessing")

# Skip tests if sem_open implementation is broken.
test.support.skip_if_broken_multiprocessing_synchronize()


def latin(s):
    return s.encode("latin")


def close_queue(queue):
    if isinstance(queue, multiprocessing.queues.Queue):
        queue.close()
        queue.join_thread()


def join_process(process):
    """Since multiprocessing.Process has the same API than threading.Thread
    (join() and is_alive(), the support function can be reused
    """
    join_thread(process)


#
# Constants
#

LOG_LEVEL = util.SUBWARNING
# LOG_LEVEL = logging.DEBUG

DELTA = 0.1
CHECK_TIMINGS = False  # making true makes tests take a lot longer
# and can sometimes cause some non-serious
# failures because some calls block a bit
# longer than expected
if CHECK_TIMINGS:
    TIMEOUT1, TIMEOUT2, TIMEOUT3 = 0.82, 0.35, 1.4
else:
    TIMEOUT1, TIMEOUT2, TIMEOUT3 = 0.1, 0.1, 0.1

HAVE_GETVALUE = not getattr(_multiprocessing, "HAVE_BROKEN_SEM_GETVALUE", False)

WIN32 = sys.platform == "win32"

# To speed up tests when using the forkserver, we can preload these:
PRELOAD = ["__main__", "test.test_multiprocessing_forkserver"]


def wait_for_handle(handle, timeout):
    if timeout is not None and timeout < 0.0:
        timeout = None
    return wait([handle], timeout)


try:
    MAXFD = os.sysconf("SC_OPEN_MAX")
except:
    MAXFD = 256


def check_enough_semaphores():
    """Check that the system supports enough semaphores to run the test."""
    # minimum number of semaphores available according to POSIX
    nsems_min = 256
    try:
        nsems = os.sysconf("SC_SEM_NSEMS_MAX")
    except (AttributeError, ValueError):
        # sysconf not available or setting not available
        return
    if nsems == -1 or nsems >= nsems_min:
        return
    raise unittest.SkipTest(
        "The OS doesn't support enough semaphores " "to run the test (required: %d)." % nsems_min
    )


#
# Creates a wrapper for a function which records the time it takes to finish
#


class TimingWrapper(object):
    def __init__(self, func):
        self.func = func
        self.elapsed = None

    def __call__(self, *args, **kwds):
        t = time.monotonic()
        try:
            return self.func(*args, **kwds)
        finally:
            self.elapsed = time.monotonic() - t


#
# Base class for test cases
#


class BaseTestCase(object):
    """Base class for test cases"""

    ALLOWED_TYPES = ("processes", "manager", "threads")

    def assertTimingAlmostEqual(self, a, b):
        if CHECK_TIMINGS:
            self.assertAlmostEqual(a, b, 1)

    def assertReturnsIfImplemented(self, value, func, *args):
        try:
            res = func(*args)
        except NotImplementedError:
            pass
        else:
            return self.assertEqual(value, res)


# DRAGON I am commenting this out, as we are clearly not on Windows

# For the sanity of Windows users, rather than crashing or freezing in
# multiple ways.
#    def __reduce__(self, *args):
#        raise NotImplementedError("shouldn't try to pickle a test case")

#    __reduce_ex__ = __reduce__


#
# Return the value of a semaphore
#
def get_value(self):
    try:
        return self.get_value()
    except AttributeError:
        try:
            return self._Semaphore__value
        except AttributeError:
            try:
                return self._value
            except AttributeError:
                raise NotImplementedError


#
# Mixins
#


class BaseMixin(object):
    """Base class to inherit the correct part of the multiprocessing API"""

    @classmethod
    def setUpClass(cls):
        cls.dangling = (multiprocessing.process._dangling.copy(), threading._dangling.copy())

    @classmethod
    def tearDownClass(cls):
        # bpo-26762: Some multiprocessing objects like Pool create reference
        # cycles. Trigger a garbage collection to break these cycles.
        test.support.gc_collect()

        processes = set(multiprocessing.process._dangling) - set(cls.dangling[0])
        if processes:
            test.support.environment_altered = True
            test.support.print_warning(f"Dangling processes: {processes}")
        processes = None

        threads = set(threading._dangling) - set(cls.dangling[1])
        if threads:
            test.support.environment_altered = True
            test.support.print_warning(f"Dangling threads: {threads}")
        threads = None


class ProcessesMixin(BaseMixin):
    """Process based tests"""

    TYPE = "processes"
    Process = multiprocessing.Process
    connection = multiprocessing.connection
    current_process = staticmethod(multiprocessing.current_process)
    parent_process = staticmethod(multiprocessing.parent_process)
    active_children = staticmethod(multiprocessing.active_children)
    Pool = staticmethod(multiprocessing.Pool)
    Pipe = staticmethod(multiprocessing.Pipe)
    Queue = staticmethod(multiprocessing.Queue)
    JoinableQueue = staticmethod(multiprocessing.JoinableQueue)
    Lock = staticmethod(multiprocessing.Lock)
    RLock = staticmethod(multiprocessing.RLock)
    Semaphore = staticmethod(multiprocessing.Semaphore)
    BoundedSemaphore = staticmethod(multiprocessing.BoundedSemaphore)
    Condition = staticmethod(multiprocessing.Condition)
    Event = staticmethod(multiprocessing.Event)
    Barrier = staticmethod(multiprocessing.Barrier)
    Value = staticmethod(multiprocessing.Value)
    Array = staticmethod(multiprocessing.Array)
    RawValue = staticmethod(multiprocessing.RawValue)
    RawArray = staticmethod(multiprocessing.RawArray)


class ManagerMixin(BaseMixin):
    TYPE = "manager"
    Process = multiprocessing.Process
    Queue = property(operator.attrgetter("manager.Queue"))
    JoinableQueue = property(operator.attrgetter("manager.JoinableQueue"))
    Lock = property(operator.attrgetter("manager.Lock"))
    RLock = property(operator.attrgetter("manager.RLock"))
    Semaphore = property(operator.attrgetter("manager.Semaphore"))
    BoundedSemaphore = property(operator.attrgetter("manager.BoundedSemaphore"))
    Condition = property(operator.attrgetter("manager.Condition"))
    Event = property(operator.attrgetter("manager.Event"))
    Barrier = property(operator.attrgetter("manager.Barrier"))
    Value = property(operator.attrgetter("manager.Value"))
    Array = property(operator.attrgetter("manager.Array"))
    list = property(operator.attrgetter("manager.list"))
    dict = property(operator.attrgetter("manager.dict"))
    Namespace = property(operator.attrgetter("manager.Namespace"))

    @classmethod
    def Pool(cls, *args, **kwds):
        return cls.manager.Pool(*args, **kwds)

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.manager = multiprocessing.Manager()

    @classmethod
    def tearDownClass(cls):
        # only the manager process should be returned by active_children()
        # but this can take a bit on slow machines, so wait a few seconds
        # if there are other children too (see #17395)
        start_time = time.monotonic()
        t = 0.01
        while len(multiprocessing.active_children()) > 1:
            time.sleep(t)
            t *= 2
            dt = time.monotonic() - start_time
            if dt >= 5.0:
                test.support.environment_altered = True
                test.support.print_warning(
                    f"multiprocessing.Manager still has "
                    f"{multiprocessing.active_children()} "
                    f"active children after {dt} seconds"
                )
                break

        gc.collect()  # do garbage collection
        if cls.manager._number_of_objects() != 0:
            # This is not really an error since some tests do not
            # ensure that all processes which hold a reference to a
            # managed object have been joined.
            test.support.environment_altered = True
            test.support.print_warning("Shared objects which still exist " "at manager shutdown:")
            test.support.print_warning(cls.manager._debug_info())
        cls.manager.shutdown()
        cls.manager.join()
        cls.manager = None

        super().tearDownClass()


class ThreadsMixin(BaseMixin):
    TYPE = "threads"


# DRAGON need to comment these out, as the dummy class is not present in our current multiprocessing implementation
#     Process = multiprocessing.dummy.Process
#     connection = multiprocessing.dummy.connection
#     current_process = staticmethod(multiprocessing.dummy.current_process)
#     active_children = staticmethod(multiprocessing.dummy.active_children)
#     Pool = staticmethod(multiprocessing.dummy.Pool)
#     Pipe = staticmethod(multiprocessing.dummy.Pipe)
#     Queue = staticmethod(multiprocessing.dummy.Queue)
#     JoinableQueue = staticmethod(multiprocessing.dummy.JoinableQueue)
#     Lock = staticmethod(multiprocessing.dummy.Lock)
#     RLock = staticmethod(multiprocessing.dummy.RLock)
#     Semaphore = staticmethod(multiprocessing.dummy.Semaphore)
#     BoundedSemaphore = staticmethod(multiprocessing.dummy.BoundedSemaphore)
#     Condition = staticmethod(multiprocessing.dummy.Condition)
#     Event = staticmethod(multiprocessing.dummy.Event)
#     Barrier = staticmethod(multiprocessing.dummy.Barrier)
#     Value = staticmethod(multiprocessing.dummy.Value)
#     Array = staticmethod(multiprocessing.dummy.Array)

# DRAGON We set start_method, dangling and old_start_method explicitely,
# to avoid programmatic module creation as done in the standard multiprocessing tests
start_method = "dragon"

dangling = [None, None]
old_start_method = [None]

# DRAGON this was the init code in setUpModule()
def setUpModule():
    """Prepare env for test"""

    multiprocessing.set_forkserver_preload(PRELOAD)
    multiprocessing.process._cleanup()
    dangling[0] = multiprocessing.process._dangling.copy()
    dangling[1] = threading._dangling.copy()
    old_start_method[0] = multiprocessing.get_start_method(allow_none=True)
    try:
        multiprocessing.set_start_method(start_method, force=True)
    except ValueError:
        raise unittest.SkipTest(start_method + " start method not supported")

    if sys.platform.startswith("linux"):
        try:
            lock = multiprocessing.RLock()
        except OSError:
            raise unittest.SkipTest("OSError raises on RLock creation, " "see issue 3111!")
    check_enough_semaphores()
    util.get_temp_dir()  # creates temp directory
    multiprocessing.get_logger().setLevel(LOG_LEVEL)


# DRAGON this was the destroy code in tearDownModule()
def tearDownModule():
    """Clean up the environmen"""

    need_sleep = False

    # bpo-26762: Some multiprocessing objects like Pool create reference
    # cycles. Trigger a garbage collection to break these cycles.
    test.support.gc_collect()

    multiprocessing.set_start_method(old_start_method[0], force=True)
    # pause a bit so we don't get warning about dangling threads/processes
    processes = set(multiprocessing.process._dangling) - set(dangling[0])
    if processes:
        need_sleep = True
        test.support.environment_altered = True
        test.support.print_warning(f"Dangling processes: {processes}")
    processes = None

    threads = set(threading._dangling) - set(dangling[1])
    if threads:
        need_sleep = True
        test.support.environment_altered = True
        test.support.print_warning(f"Dangling threads: {threads}")
    threads = None

    # Sleep 500 ms to give time to child processes to complete.
    if need_sleep:
        time.sleep(0.5)

    multiprocessing.util._cleanup_tests()
