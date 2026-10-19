"""Test Finalize, Import * and Logging
"""
import sys
import time
import os
import gc
import random

import unittest
import test.support

import threading

import dragon  # DRAGON import before multiprocessing

import multiprocessing
from multiprocessing import util

import logging

try:
    from ctypes import c_int
except ImportError:
    Structure = object
    c_int = c_double = c_longlong = None

try:
    from multiprocessing import reduction

    HAS_REDUCTION = reduction.HAVE_SEND_HANDLE
except ImportError:
    HAS_REDUCTION = False

from common import (
    BaseTestCase,
    ProcessesMixin,
    LOG_LEVEL,
    setUpModule,
    tearDownModule,
)

#
#
#


@unittest.skip("DRAGON: not implemented")
class WithProcessesTestFinalize(BaseTestCase, ProcessesMixin, unittest.TestCase):

    # DRAGON ALLOWED_TYPES = ('processes',)

    def setUp(self):
        self.registry_backup = util._finalizer_registry.copy()
        util._finalizer_registry.clear()

    def tearDown(self):
        self.assertFalse(util._finalizer_registry)
        util._finalizer_registry.update(self.registry_backup)

    @classmethod
    def _test_finalize(cls, conn):
        class Foo(object):
            pass

        a = Foo()
        util.Finalize(a, conn.send, args=("a",))
        del a  # triggers callback for a

        b = Foo()
        close_b = util.Finalize(b, conn.send, args=("b",))
        close_b()  # triggers callback for b
        close_b()  # does nothing because callback has already been called
        del b  # does nothing because callback has already been called

        c = Foo()
        util.Finalize(c, conn.send, args=("c",))

        d10 = Foo()
        util.Finalize(d10, conn.send, args=("d10",), exitpriority=1)

        d01 = Foo()
        util.Finalize(d01, conn.send, args=("d01",), exitpriority=0)
        d02 = Foo()
        util.Finalize(d02, conn.send, args=("d02",), exitpriority=0)
        d03 = Foo()
        util.Finalize(d03, conn.send, args=("d03",), exitpriority=0)

        util.Finalize(None, conn.send, args=("e",), exitpriority=-10)

        util.Finalize(None, conn.send, args=("STOP",), exitpriority=-100)

        # call multiprocessing's cleanup function then exit process without
        # garbage collecting locals
        util._exit_function()
        conn.close()
        os._exit(0)

    def test_finalize(self):
        conn, child_conn = self.Pipe()

        p = self.Process(target=self._test_finalize, args=(child_conn,))
        p.daemon = True
        p.start()
        p.join()

        result = [obj for obj in iter(conn.recv, "STOP")]
        self.assertEqual(result, ["a", "b", "d10", "d03", "d02", "d01", "e"])

    def test_thread_safety(self):
        # bpo-24484: _run_finalizers() should be thread-safe
        def cb():
            pass

        class Foo(object):
            def __init__(self):
                self.ref = self  # create reference cycle
                # insert finalizer at random key
                util.Finalize(self, cb, exitpriority=random.randint(1, 100))

        finish = False
        exc = None

        def run_finalizers():
            nonlocal exc
            while not finish:
                time.sleep(random.random() * 1e-1)
                try:
                    # A GC run will eventually happen during this,
                    # collecting stale Foo's and mutating the registry
                    util._run_finalizers()
                except Exception as e:
                    exc = e

        def make_finalizers():
            nonlocal exc
            d = {}
            while not finish:
                try:
                    # Old Foo's get gradually replaced and later
                    # collected by the GC (because of the cyclic ref)
                    d[random.getrandbits(5)] = {Foo() for i in range(10)}
                except Exception as e:
                    exc = e
                    d.clear()

        old_interval = sys.getswitchinterval()
        old_threshold = gc.get_threshold()
        try:
            sys.setswitchinterval(1e-6)
            gc.set_threshold(5, 5, 5)
            threads = [threading.Thread(target=run_finalizers), threading.Thread(target=make_finalizers)]
            with test.support.start_threads(threads):
                time.sleep(4.0)  # Wait a bit to trigger race condition
                finish = True
            if exc is not None:
                raise exc
        finally:
            sys.setswitchinterval(old_interval)
            gc.set_threshold(*old_threshold)
            gc.collect()  # Collect remaining Foo's


#
# Test that from ... import * works for each module
#


class WithProcessesTestImportStar(unittest.TestCase):
    def get_module_names(self):
        import glob

        folder = os.path.dirname(multiprocessing.__file__)
        pattern = os.path.join(glob.escape(folder), "*.py")
        files = glob.glob(pattern)
        modules = [os.path.splitext(os.path.split(f)[1])[0] for f in files]
        modules = ["multiprocessing." + m for m in modules]
        modules.remove("multiprocessing.__init__")
        modules.append("multiprocessing")
        return modules

    def test_import(self):
        modules = self.get_module_names()
        if sys.platform == "win32":
            modules.remove("multiprocessing.popen_fork")
            modules.remove("multiprocessing.popen_forkserver")
            modules.remove("multiprocessing.popen_spawn_posix")
        else:
            modules.remove("multiprocessing.popen_spawn_win32")
            if not HAS_REDUCTION:
                modules.remove("multiprocessing.popen_forkserver")

        if c_int is None:
            # This module requires _ctypes
            modules.remove("multiprocessing.sharedctypes")

        for name in modules:
            __import__(name)
            mod = sys.modules[name]
            self.assertTrue(hasattr(mod, "__all__"), name)

            for attr in mod.__all__:
                self.assertTrue(hasattr(mod, attr), "%r does not have attribute %r" % (mod, attr))


#
# Quick test that logging works -- does not test logging output
#


class WithProcessesTestLogging(BaseTestCase, ProcessesMixin, unittest.TestCase):

    # DRAGON ALLOWED_TYPES = ('processes',)

    def test_enable_logging(self):
        logger = multiprocessing.get_logger()
        logger.setLevel(util.SUBWARNING)
        self.assertTrue(logger is not None)
        logger.debug("this will not be printed")
        logger.info("nor will this")
        logger.setLevel(LOG_LEVEL)

    @classmethod
    def _test_level(cls, conn):
        logger = multiprocessing.get_logger()
        conn.send(logger.getEffectiveLevel())

    @unittest.skip(f"DRAGON: Logging currently incomplete PE-41692")
    def test_level(self):
        LEVEL1 = 32
        LEVEL2 = 37

        logger = multiprocessing.get_logger()
        root_logger = logging.getLogger()
        root_level = root_logger.level

        reader, writer = multiprocessing.Pipe(duplex=False)

        logger.setLevel(LEVEL1)
        p = self.Process(target=self._test_level, args=(writer,))
        p.start()
        self.assertEqual(LEVEL1, reader.recv())
        p.join()
        p.close()

        logger.setLevel(logging.NOTSET)
        root_logger.setLevel(LEVEL2)
        p = self.Process(target=self._test_level, args=(writer,))
        p.start()
        self.assertEqual(LEVEL2, reader.recv())
        p.join()
        p.close()

        root_logger.setLevel(root_level)
        logger.setLevel(level=LOG_LEVEL)


# class _TestLoggingProcessName(BaseTestCase):
#
#     def handle(self, record):
#         assert record.processName == multiprocessing.current_process().name
#         self.__handled = True
#
#     def test_logging(self):
#         handler = logging.Handler()
#         handler.handle = self.handle
#         self.__handled = False
#         # Bypass getLogger() and side-effects
#         logger = logging.getLoggerClass()(
#                 'multiprocessing.test.TestLoggingProcessName')
#         logger.addHandler(handler)
#         logger.propagate = False
#
#         logger.warn('foo')
#         assert self.__handled


# DRAGON
if __name__ == "__main__":
    setUpModule()
    unittest.main()
    tearDownModule()
