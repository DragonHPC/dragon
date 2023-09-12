""" This file contains tests of the mpbridge interfaces, i.e. classes, methods, attributes and how they
are hooked into multiprocessing context.

This is driven by the requirement that Python multiprocessing should **just work** with Dragon.
"""

import unittest
import copy

import dragon
import multiprocessing as mp

# these parts of the "public" API are apparently not reachable through the context or top level import
from multiprocessing.managers import BaseManager, SyncManager, BaseProxy, Token, SharedMemoryManager
from multiprocessing.shared_memory import SharedMemory, ShareableList
from multiprocessing.util import get_logger, log_to_stderr
from multiprocessing.heap import Arena, BufferWrapper


def setUpModule():
    mp.set_start_method("dragon", force=True)


class TestMultiprocessingAPI(unittest.TestCase):
    """An attempt at testing that we are monkeypatching correctly.
    In particular, that that the multiprocessing API is correctly
    connected to the Dragon Native API.
    """

    def setUp(self):
        self.assertEqual(mp.get_start_method(), "dragon")

    def test_monkeypatching(self):

        ctx = mp.get_context()
        self.assertTrue(ctx._name == "dragon")

        # enable all Dragon API
        ctx_backup = copy.copy(ctx)  # save orig for copy back later
        ctx.USE_MPFUNCTIONS = ctx.USE_MPQUEUE = ctx.USE_MPJQUEUE = ctx.USE_MPSQUEUE = False
        ctx.USE_MPPOOL = ctx.USE_MPPIPE = ctx.USE_MPMANAGER = ctx.USE_MPLOCK = False
        ctx.USE_MPRLOCK = ctx.USE_MPCONDITION = ctx.USE_MPSEMAPHORE = ctx.USE_MPBOUNDSEMAPHORE = False
        ctx.USE_MPEVENT = ctx.USE_MPBARRIER = ctx.USE_MPVALUE = ctx.USE_MPRAWVALUE = ctx.USE_MPARRAY = False
        ctx.USE_MPRAWARRAY = ctx.USE_MPSHAREDMEMORY = ctx.USE_MPSHAREABLELIST = ctx.USE_MPLISTENER = False
        ctx.USE_MPCLIENT = False

        # Functions
        self.assertRaises(NotImplementedError, mp.freeze_support)  # win32 only
        self.assertTrue(callable(getattr(mp, "cpu_count")))
        self.assertTrue(callable(getattr(mp, "log_to_stderr")))
        self.assertTrue(callable(getattr(mp, "get_logger")))
        self.assertTrue(callable(log_to_stderr))
        self.assertTrue(callable(get_logger))

        self.assertTrue(mp.connection.wait == mp.get_context().wait)
        self.assertTrue(mp.pool.wait == mp.get_context().wait)

        self.assertTrue(mp.reduction.DupFd == dragon.mpbridge.reduction.dragon_DupFd)
        self.assertTrue(mp.reduction.recvfds == dragon.mpbridge.reduction.dragon_recvfds)
        self.assertTrue(mp.reduction.sendfds == dragon.mpbridge.reduction.dragon_sendfds)
        self.assertTrue(mp.get_context().reducer.DupFd == dragon.mpbridge.reduction.dragon_DupFd)
        self.assertTrue(mp.get_context().reducer.recvfds == dragon.mpbridge.reduction.dragon_recvfds)
        self.assertTrue(mp.get_context().reducer.sendfds == dragon.mpbridge.reduction.dragon_sendfds)

        # Classes
        self.assertIsInstance(mp.Queue(), dragon.mpbridge.queues.DragonQueue)
        self.assertIsInstance(mp.SimpleQueue(), dragon.mpbridge.queues.DragonSimpleQueue)
        self.assertIsInstance(mp.JoinableQueue(), dragon.mpbridge.queues.DragonJoinableQueue)

        self.assertIsInstance(mp.Semaphore(), dragon.mpbridge.synchronize.DragonSemaphore)
        self.assertIsInstance(mp.BoundedSemaphore(), dragon.mpbridge.synchronize.DragonBoundedSemaphore)
        self.assertIsInstance(mp.Lock(), dragon.mpbridge.synchronize.DragonLock)
        self.assertIsInstance(mp.RLock(), dragon.mpbridge.synchronize.DragonRLock)
        self.assertIsInstance(mp.Event(), dragon.mpbridge.synchronize.DragonEvent)
        self.assertIsInstance(mp.Value(typecode_or_type="i"), dragon.mpbridge.sharedctypes.DragonValue)
        self.assertIsInstance(mp.RawValue(typecode_or_type="i"), dragon.mpbridge.sharedctypes.DragonRawValue)
        self.assertIsInstance(mp.Barrier(5), dragon.mpbridge.synchronize.DragonBarrier)
        self.assertIsInstance(mp.Condition(), dragon.mpbridge.synchronize.DragonCondition)
        self.assertIsInstance(mp.Array('i', []), dragon.mpbridge.sharedctypes.DragonArray)
        self.assertIsInstance(mp.RawArray('i', []), dragon.mpbridge.sharedctypes.DragonRawArray)

        self.assertIsInstance(Arena(1024), dragon.mpbridge.heap.DragonArena)
        self.assertIsInstance(BufferWrapper(1024)._heap._arenas[0], dragon.mpbridge.heap.DragonArena)

        with mp.Pool(1) as dut:  # it is not correct to rely on the garbage collector to destroy the pool
            self.assertIsInstance(dut, dragon.mpbridge.pool.DragonPool)

        self.assertTrue(
            callable(getattr(mp, "Pipe"))
        )  # this should be dragon.mpbridge.connection.DragonConnection
        r, w = mp.Pipe()
        self.assertIsInstance(r, dragon.infrastructure.connection.Connection)  # TODO: PE-41642
        self.assertIsInstance(w, dragon.infrastructure.connection.Connection)

        # Missing features
        self.assertRaises(NotImplementedError, mp.Manager)
        self.assertRaises(NotImplementedError, BaseManager)
        self.assertRaises(NotImplementedError, SyncManager)
        self.assertRaises(NotImplementedError, BaseProxy)
        self.assertRaises(NotImplementedError, Token)
        self.assertRaises(NotImplementedError, SharedMemoryManager)

        self.assertRaises(NotImplementedError, SharedMemory)
        self.assertRaises(NotImplementedError, ShareableList)

        self.assertRaises(NotImplementedError, mp.connection.Listener)
        self.assertRaises(NotImplementedError, mp.connection.Client, "")

        # copy back original values
        ctx.USE_MPFUNCTIONS = ctx_backup.USE_MPFUNCTIONS
        ctx.USE_MPJQUEUE = ctx_backup.USE_MPJQUEUE
        ctx.USE_MPMANAGER = ctx_backup.USE_MPMANAGER
        ctx.USE_MPLOCK = ctx_backup.USE_MPLOCK
        ctx.USE_MPRLOCK = ctx_backup.USE_MPRLOCK
        ctx.USE_MPCONDITION = ctx_backup.USE_MPCONDITION
        ctx.USE_MPSEMAPHORE = ctx_backup.USE_MPSEMAPHORE
        ctx.USE_MPBOUNDSEMAPHORE = ctx_backup.USE_MPBOUNDSEMAPHORE
        ctx.USE_MPEVENT = ctx_backup.USE_MPEVENT
        ctx.USE_MPBARRIER = ctx_backup.USE_MPBARRIER
        ctx.USE_MPVALUE = ctx_backup.USE_MPVALUE
        ctx.USE_MPRAWVALUE = ctx_backup.USE_MPRAWVALUE
        ctx.USE_MPARRAY = ctx_backup.USE_MPARRAY
        ctx.USE_MPRAWARRAY = ctx_backup.USE_MPRAWARRAY
        ctx.USE_MPSHAREDMEMORY = ctx_backup.USE_MPSHAREDMEMORY
        ctx.USE_MPSHAREABLELIST = ctx_backup.USE_MPSHAREABLELIST
        ctx.USE_MPLISTENER = ctx_backup.USE_MPLISTENER
        ctx.USE_MPCLIENT = ctx_backup.USE_MPLISTENER

    def test_queue_public_interfaces(self):
        """Test that our metaclasses create the correct public interfaces, i.e.
        clean the dragon.native objects correctly of any non-standard attributes
        """

        import dragon.mpbridge.queues

        sq = mp.SimpleQueue()
        sq_attr = ["close", "empty", "get", "put"]
        self.assertListEqual([x for x in dir(sq) if not x.startswith("_")], sq_attr)

        q = mp.Queue()
        q_attr = [
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
        self.assertListEqual([x for x in dir(q) if not x.startswith("_")], q_attr)

        jq = mp.JoinableQueue()
        jq_attr = [
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

        self.assertListEqual([x for x in dir(jq) if not x.startswith("_")], jq_attr)


class TestMultiprocessingInternalPatching(unittest.TestCase):
    """Test that internal instances of Multiprocessing classes correctly reference
    Dragon components, not Multiprocessing components.
    """

    def test_condition(self):
        cond = mp.Condition()
        self.assertIsInstance(cond._lock, dragon.mpbridge.synchronize.DragonRLock)
        self.assertIsInstance(cond._sleeping_count, dragon.mpbridge.synchronize.DragonSemaphore)
        self.assertIsInstance(cond._woken_count, dragon.mpbridge.synchronize.DragonSemaphore)
        self.assertIsInstance(cond._wait_semaphore, dragon.mpbridge.synchronize.DragonSemaphore)


if __name__ == "__main__":
    unittest.main()
