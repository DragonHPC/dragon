"""Test multiprocessing manager restart
"""
import unittest
import time
import os
import errno
import signal
import queue as pyqueue

from test.support import hashlib_helper
from test.support import socket_helper

import dragon  # DRAGON import before multiprocessing

import multiprocessing
from multiprocessing.managers import BaseManager

from common import (
    BaseTestCase,
    ProcessesMixin,
    ManagerMixin,
    ThreadsMixin,
    setUpModule,
    tearDownModule,
)

_queue = pyqueue.Queue()


def get_queue():
    return _queue


class QueueManager(BaseManager):
    """manager class used by server process"""


QueueManager.register("get_queue", callable=get_queue)


class QueueManager2(BaseManager):
    """manager class which specifies the same interface as QueueManager"""


QueueManager2.register("get_queue")

SERIALIZER = "xmlrpclib"


@hashlib_helper.requires_hashdigest("md5")
@unittest.skip("DRAGON: not implemented")
class WithProcessesTestManagerRestart(BaseTestCase, ProcessesMixin, unittest.TestCase):
    @classmethod
    def _putter(cls, address, authkey):
        manager = QueueManager(address=address, authkey=authkey, serializer=SERIALIZER)
        manager.connect()
        queue = manager.get_queue()
        queue.put("hello world")

    def test_rapid_restart(self):
        authkey = os.urandom(32)
        manager = QueueManager(address=(socket_helper.HOST, 0), authkey=authkey, serializer=SERIALIZER)
        try:
            srvr = manager.get_server()
            addr = srvr.address
            # Close the connection.Listener socket which gets opened as a part
            # of manager.get_server(). It's not needed for the test.
            srvr.listener.close()
            manager.start()

            p = self.Process(target=self._putter, args=(manager.address, authkey))
            p.start()
            p.join()
            queue = manager.get_queue()
            self.assertEqual(queue.get(), "hello world")
            del queue
        finally:
            if hasattr(manager, "shutdown"):
                manager.shutdown()

        manager = QueueManager(address=addr, authkey=authkey, serializer=SERIALIZER)
        try:
            manager.start()
            self.addCleanup(manager.shutdown)
        except OSError as e:
            if e.errno != errno.EADDRINUSE:
                raise
            # Retry after some time, in case the old socket was lingering
            # (sporadic failure on buildbots)
            time.sleep(1.0)
            manager = QueueManager(address=addr, authkey=authkey, serializer=SERIALIZER)
            if hasattr(manager, "shutdown"):
                self.addCleanup(manager.shutdown)


#
# Test of creating a customized manager class
#

from multiprocessing.managers import BaseManager, BaseProxy, RemoteError


class FooBar(object):
    def f(self):
        return "f()"

    def g(self):
        raise ValueError

    def _h(self):
        return "_h()"


def baz():
    for i in range(10):
        yield i * i


class IteratorProxy(BaseProxy):
    _exposed_ = ("__next__",)

    def __iter__(self):
        return self

    def __next__(self):
        return self._callmethod("__next__")


class MyManager(BaseManager):
    pass


MyManager.register("Foo", callable=FooBar)
MyManager.register("Bar", callable=FooBar, exposed=("f", "_h"))
MyManager.register("baz", callable=baz, proxytype=IteratorProxy)


@unittest.skip("DRAGON: Manager not implemented")
@hashlib_helper.requires_hashdigest("md5")
class WithManagerTestMyManager(BaseTestCase, ManagerMixin, unittest.TestCase):

    # DRAGON ALLOWED_TYPES = ('manager',)

    def test_mymanager(self):
        manager = MyManager()
        manager.start()
        self.common(manager)
        manager.shutdown()

        # bpo-30356: BaseManager._finalize_manager() sends SIGTERM
        # to the manager process if it takes longer than 1 second to stop,
        # which happens on slow buildbots.
        self.assertIn(manager._process.exitcode, (0, -signal.SIGTERM))

    def test_mymanager_context(self):
        with MyManager() as manager:
            self.common(manager)
        # bpo-30356: BaseManager._finalize_manager() sends SIGTERM
        # to the manager process if it takes longer than 1 second to stop,
        # which happens on slow buildbots.
        self.assertIn(manager._process.exitcode, (0, -signal.SIGTERM))

    def test_mymanager_context_prestarted(self):
        manager = MyManager()
        manager.start()
        with manager:
            self.common(manager)
        self.assertEqual(manager._process.exitcode, 0)

    def common(self, manager):
        foo = manager.Foo()
        bar = manager.Bar()
        baz = manager.baz()

        foo_methods = [name for name in ("f", "g", "_h") if hasattr(foo, name)]
        bar_methods = [name for name in ("f", "g", "_h") if hasattr(bar, name)]

        self.assertEqual(foo_methods, ["f", "g"])
        self.assertEqual(bar_methods, ["f", "_h"])

        self.assertEqual(foo.f(), "f()")
        self.assertRaises(ValueError, foo.g)
        self.assertEqual(foo._callmethod("f"), "f()")
        self.assertRaises(RemoteError, foo._callmethod, "_h")

        self.assertEqual(bar.f(), "f()")
        self.assertEqual(bar._h(), "_h()")
        self.assertEqual(bar._callmethod("f"), "f()")
        self.assertEqual(bar._callmethod("_h"), "_h()")

        self.assertEqual(list(baz), [i * i for i in range(10)])


@unittest.skip("DRAGON: Manager not implemented")
@hashlib_helper.requires_hashdigest("md5")
class WithManagerTestRemoteManager(BaseTestCase, ManagerMixin, unittest.TestCase):

    ALLOWED_TYPES = ("manager",)
    values = [
        "hello world",
        None,
        True,
        2.25,
        "hall\xe5 v\xe4rlden",
        "\u043f\u0440\u0438\u0432\u0456\u0442 \u0441\u0432\u0456\u0442",
        b"hall\xe5 v\xe4rlden",
    ]
    result = values[:]

    @classmethod
    def _putter(cls, address, authkey):
        manager = QueueManager2(address=address, authkey=authkey, serializer=SERIALIZER)
        manager.connect()
        queue = manager.get_queue()
        # Note that xmlrpclib will deserialize object as a list not a tuple
        queue.put(tuple(cls.values))

    def test_remote(self):
        authkey = os.urandom(32)

        manager = QueueManager(address=(socket_helper.HOST, 0), authkey=authkey, serializer=SERIALIZER)
        manager.start()
        self.addCleanup(manager.shutdown)

        p = self.Process(target=self._putter, args=(manager.address, authkey))
        p.daemon = True
        p.start()

        manager2 = QueueManager2(address=manager.address, authkey=authkey, serializer=SERIALIZER)
        manager2.connect()
        queue = manager2.get_queue()

        self.assertEqual(queue.get(), self.result)

        # Because we are using xmlrpclib for serialization instead of
        # pickle this will cause a serialization error.
        self.assertRaises(Exception, queue.put, time.sleep)

        # Make queue finalizer run before the server is stopped
        del queue


@unittest.skip("DRAGON: Manager not implemented")
@hashlib_helper.requires_hashdigest("md5")
class WithManagerTestManagerRestart(BaseTestCase, ManagerMixin, unittest.TestCase):
    @classmethod
    def _putter(cls, address, authkey):
        manager = QueueManager(address=address, authkey=authkey, serializer=SERIALIZER)
        manager.connect()
        queue = manager.get_queue()
        queue.put("hello world")

    def test_rapid_restart(self):
        authkey = os.urandom(32)
        manager = QueueManager(address=(socket_helper.HOST, 0), authkey=authkey, serializer=SERIALIZER)
        try:
            srvr = manager.get_server()
            addr = srvr.address
            # Close the connection.Listener socket which gets opened as a part
            # of manager.get_server(). It's not needed for the test.
            srvr.listener.close()
            manager.start()

            p = self.Process(target=self._putter, args=(manager.address, authkey))
            p.start()
            p.join()
            queue = manager.get_queue()
            self.assertEqual(queue.get(), "hello world")
            del queue
        finally:
            if hasattr(manager, "shutdown"):
                manager.shutdown()

        manager = QueueManager(address=addr, authkey=authkey, serializer=SERIALIZER)
        try:
            manager.start()
            self.addCleanup(manager.shutdown)
        except OSError as e:
            if e.errno != errno.EADDRINUSE:
                raise
            # Retry after some time, in case the old socket was lingering
            # (sporadic failure on buildbots)
            time.sleep(1.0)
            manager = QueueManager(address=addr, authkey=authkey, serializer=SERIALIZER)
            if hasattr(manager, "shutdown"):
                self.addCleanup(manager.shutdown)


@unittest.skip("DRAGON: Threads not implemented")
@hashlib_helper.requires_hashdigest("md5")
class WithThreadsTestManagerRestart(BaseTestCase, ThreadsMixin, unittest.TestCase):
    @classmethod
    def _putter(cls, address, authkey):
        manager = QueueManager(address=address, authkey=authkey, serializer=SERIALIZER)
        manager.connect()
        queue = manager.get_queue()
        queue.put("hello world")

    def test_rapid_restart(self):
        authkey = os.urandom(32)
        manager = QueueManager(address=(socket_helper.HOST, 0), authkey=authkey, serializer=SERIALIZER)
        try:
            srvr = manager.get_server()
            addr = srvr.address
            # Close the connection.Listener socket which gets opened as a part
            # of manager.get_server(). It's not needed for the test.
            srvr.listener.close()
            manager.start()

            p = self.Process(target=self._putter, args=(manager.address, authkey))
            p.start()
            p.join()
            queue = manager.get_queue()
            self.assertEqual(queue.get(), "hello world")
            del queue
        finally:
            if hasattr(manager, "shutdown"):
                manager.shutdown()

        manager = QueueManager(address=addr, authkey=authkey, serializer=SERIALIZER)
        try:
            manager.start()
            self.addCleanup(manager.shutdown)
        except OSError as e:
            if e.errno != errno.EADDRINUSE:
                raise
            # Retry after some time, in case the old socket was lingering
            # (sporadic failure on buildbots)
            time.sleep(1.0)
            manager = QueueManager(address=addr, authkey=authkey, serializer=SERIALIZER)
            if hasattr(manager, "shutdown"):
                self.addCleanup(manager.shutdown)


# DRAGON
if __name__ == "__main__":
    setUpModule()
    unittest.main()
    tearDownModule()
