import dragon
from dragon import mpbridge
import multiprocessing as mp
import os
import unittest


mpbridge.inspections = True  # Inspect and report origin of any direct invocations of os.pipe
# logger = mp.log_to_stderr()
# logger.setLevel(mp.SUBWARNING)
# logger.setLevel(mp.SUBDEBUG)  # Verbosity to aid detailed debugging.


def sleepy_getpid(delay):
    """Convenient function to use as target when creating new processes."""
    import time, os

    time.sleep(delay)
    mp.util.info(f"sleepy_getpid done on {os.getpid()}")
    return os.getpid()


class TestStartMethod(unittest.TestCase):
    def test_AAA_available_start_method(self):
        start_methods = mp.get_all_start_methods()
        self.assertTrue(mpbridge.DragonPopen.method in start_methods)
        self.assertTrue("spawn" in start_methods)
        self.assertTrue("fork" in start_methods)
        self.assertTrue("forkserver" in start_methods)

    def test_AAB_set_dragon_start_method(self):
        mp.set_start_method("spawn", force=True)
        self.assertFalse(mp.get_start_method(allow_none=True) == "dragon")
        mp.set_start_method(mpbridge.DragonPopen.method, force=True)
        self.assertEqual(mp.get_start_method(allow_none=True), "dragon")

    def test_dragon_is_available_via_default_context(self):
        from dragon.mpbridge.monkeypatching import AugmentedDefaultContext

        self.assertTrue(isinstance(mp.context._default_context, AugmentedDefaultContext))

    # check we're no longer using file descriptors
    @unittest.skipIf("DRAGON_MY_PUID" not in os.environ, "Not launched via dragon")
    @unittest.skip("filed as part of CIRRUS-1473")
    def test_DragonProcess_pool_map(self):
        mp.set_start_method(mpbridge.DragonPopen.method, force=True)
        with mp.Pool(2) as pool:
            results = pool.map(sleepy_getpid, [0.1, 0.3, 0.2])
            self.assertEqual(len(mp.process._children), 2)
            for live_process in mp.process._children:
                mp.util.info(f"{live_process}:\n\t{live_process._popen._fds}")
                self.assertEqual(len(live_process._popen._fds), 0)
        del pool
        self.assertEqual(len(results), 3)
        self.assertEqual(len(set(results)), 2)
        mp.set_start_method(mpbridge.DragonPopen.method, force=True)

    @unittest.skip(f"Currently fails, mp.Process contains _start_method, PE-37293")
    def test_basic_process_start_join(self):
        mp.set_start_method(mpbridge.DragonPopen.method, force=True)
        p1 = mpbridge.DragonProcess(target=sleepy_getpid, args=(0.1,))
        p2 = mp.Process(target=sleepy_getpid, args=(0.1,))
        p1.start()
        p2.start()
        p1.join()
        self.assertEqual(p1.exitcode, 0)
        p2.join()
        self.assertEqual(p2.exitcode, 0)
        self.assertEqual(p1._start_method, mpbridge.DragonPopen.method)
        self.assertEqual(p2._start_method, None)
        self.assertTrue(isinstance(p1, mpbridge.DragonProcess))
        self.assertTrue(isinstance(p2, mp.context.Process))

    @classmethod
    def _check_context(cls, conn):
        conn.send(mp.get_start_method())

    def check_context(self, ctx):
        r, w = ctx.Pipe(duplex=False)
        p = ctx.Process(target=self._check_context, args=(w,))
        p.start()
        w.close()
        child_method = r.recv()
        r.close()
        p.join()
        self.assertEqual(child_method, ctx.get_start_method())

    def test_set_get(self):
        # To speed up tests when using the forkserver, we can preload these:
        PRELOAD = ["__main__", "test.test_multiprocessing_forkserver"]
        mp.set_forkserver_preload(PRELOAD)
        count = 0
        # mp.set_start_method('fork', force=True)
        old_method = mp.get_start_method()
        try:
            # for method in ('fork', 'spawn', 'forkserver'):  # Modified to avoid issue when testing on WSL.
            for method in ("fork", "spawn"):
                try:
                    mp.set_start_method(method, force=True)
                except ValueError:
                    raise
                self.assertEqual(mp.get_start_method(), method)
                ctx = mp.get_context()
                self.assertEqual(ctx.get_start_method(), method)
                self.assertTrue(type(ctx).__name__.lower().startswith(method))
                self.assertTrue(ctx.Process.__name__.lower().startswith(method))
                self.check_context(mp)
                count += 1
        finally:
            mp.set_start_method(old_method, force=True)
        self.assertGreaterEqual(count, 1)

    def test_get_all(self):
        import sys

        methods = mp.get_all_start_methods()
        if sys.platform == "win32":
            self.assertEqual(methods, ["spawn"])
        else:
            self.assertTrue("dragon" in methods)
            methods.remove("dragon")
            self.assertTrue(
                methods == ["fork", "spawn"]
                or methods == ["spawn", "fork"]
                or methods == ["fork", "spawn", "forkserver"]
                or methods == ["spawn", "fork", "forkserver"]
            )


if __name__ == "__main__":
    unittest.main()
