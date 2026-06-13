import unittest
import dragon
import multiprocessing as mp


class TestDragonContext(unittest.TestCase):
    """
    Unit tests to verify that DragonContext returns Dragon-native multiprocessing objects.
    These tests ensure that calling `multiprocessing.get_context('dragon')` on Dragon objects produces Dragon-specific classes, not
    CPython's standard multiprocessing types. This helps catch misconfigurations in Dragon's context and monkeypatching system.
    """

    @classmethod
    def setUpClass(cls):
        cls.ctx = mp.get_context()

    def test_semaphore(self):
        sem = self.ctx.Semaphore()
        from dragon.mpbridge.synchronize import DragonSemaphore

        self.assertIsInstance(sem, DragonSemaphore)

    def test_lock(self):
        lock = self.ctx.Lock()
        from dragon.mpbridge.synchronize import DragonLock

        self.assertIsInstance(lock, DragonLock)

    def test_rlock(self):
        rlock = self.ctx.RLock()
        from dragon.mpbridge.synchronize import DragonRLock

        self.assertIsInstance(rlock, DragonRLock)

    def test_condition(self):
        cond = self.ctx.Condition()
        from dragon.mpbridge.synchronize import DragonCondition

        self.assertIsInstance(cond, DragonCondition)

    def test_event(self):
        ev = self.ctx.Event()
        from dragon.mpbridge.synchronize import DragonEvent

        self.assertIsInstance(ev, DragonEvent)

    def test_barrier(self):
        barrier = self.ctx.Barrier(2)
        from dragon.mpbridge.synchronize import DragonBarrier

        self.assertIsInstance(barrier, DragonBarrier)

    def test_queue(self):
        q = self.ctx.Queue()
        from dragon.mpbridge.queues import DragonQueue

        self.assertIsInstance(q, DragonQueue)

    def test_joinable_queue(self):
        jq = self.ctx.JoinableQueue()
        from dragon.mpbridge.queues import DragonJoinableQueue

        self.assertIsInstance(jq, DragonJoinableQueue)

    def test_simple_queue(self):
        sq = self.ctx.SimpleQueue()
        from dragon.mpbridge.queues import DragonSimpleQueue

        self.assertIsInstance(sq, DragonSimpleQueue)

    def test_pipe(self):
        conn1, conn2 = self.ctx.Pipe()
        from dragon.infrastructure.connection import Connection

        self.assertIsInstance(conn1, Connection)
        self.assertIsInstance(conn2, Connection)

    def test_value(self):
        val = self.ctx.Value("i", 5)
        from dragon.mpbridge.sharedctypes import DragonValue

        self.assertIsInstance(val, DragonValue)

    def test_array(self):
        arr = self.ctx.Array("i", 5)
        from dragon.mpbridge.sharedctypes import DragonArray

        self.assertIsInstance(arr, DragonArray)


if __name__ == "__main__":
    mp.set_start_method("dragon", force=True)
    unittest.main()
