import unittest
import time
import os
import random
import signal

from dragon.globalservices.process import query, kill, ProcessError
from dragon.infrastructure.process_desc import ProcessOptions

from dragon.native.process import Process, ProcessTemplate
from dragon.native.event import Event
from dragon.native.queue import Queue
from dragon.native.process_group import ProcessGroup, DragonProcessGroupError

# we have to test every transition in the state diagram here.
# plus a few robustness tests


class TestDragonNativeProcessGroup(unittest.TestCase):
    """Unit tests for the Dragon Native ProcessGroup Manager.
    This class has to be very robust against processes disappearing.
    """

    @classmethod
    def setUpClass(cls):
        """Set parameters"""

        # a few standard values
        cls.nproc = 3
        cls.cmd = "sleep"
        cls.args = ("128",)
        cls.cwd = os.getcwd()
        cls.options = ProcessOptions(make_inf_channels=True)
        cls.template = ProcessTemplate(cls.cmd, args=cls.args, cwd=cls.cwd)

    def test_init(self):

        pg = ProcessGroup()
        pg.add_process(self.nproc, self.template)
        pg.init()

        self.assertTrue(pg.status == "Idle")

        man_proc = pg._manager._proc

        self.assertTrue(man_proc.is_alive)

        pg.stop()

        man_proc.join(timeout=None)
        self.assertFalse(man_proc.is_alive)

    def test_start_stop(self):

        pg = ProcessGroup()
        pg.add_process(self.nproc, self.template)
        pg.init()

        manager = pg._manager
        self.assertTrue(manager.is_alive)
        self.assertTrue(pg.status == "Idle")

        pg.start()
        self.assertTrue(pg.status == "Maintain")

        puids = pg.puids
        processes = [Process(None, ident=puid) for puid in puids]
        for p in processes:
            self.assertTrue(p.is_alive)

        pg.stop()
        manager.join(timeout=None)  # will the manager exit after stop ?
        self.assertFalse(manager.is_alive)

    def test_alive_puids(self):

        pg = ProcessGroup()
        pg.add_process(self.nproc, self.template)
        pg.init()

        man_proc = pg._manager._proc
        self.assertTrue(man_proc.is_alive)
        self.assertTrue(pg.status == "Idle")

        pg.start()
        self.assertTrue(pg.status == "Maintain")

        puids = pg.puids.copy()
        processes = [Process(None, ident=puid) for puid in puids]
        for p in processes:
            self.assertTrue(p.is_alive)

        pg.stop(save_puids=True)
        # Confirm the puids have been removed from the active and moved to the inactive
        puid_statuses = pg.inactive_puids
        for puid, ecode in puid_statuses:
            self.assertTrue(puid in puids)
            self.assertEqual(ecode, -1*signal.SIGKILL.value)

        man_proc.join()
        self.assertFalse(man_proc.is_alive)

    @classmethod
    def event_quitter(cls, ev):
        ev.wait()

    def test_join_from_idle(self):

        ev = Event()

        template = ProcessTemplate(self.event_quitter, args=(ev,), cwd=".")
        pg = ProcessGroup(restart=False)
        pg.add_process(self.nproc, template)
        pg.init()

        self.assertTrue(pg.status == "Idle")

        pg.start()
        self.assertTrue(pg.status == "Running")

        puids = pg.puids
        processes = [Process(None, ident=puid) for puid in puids]

        for p in processes:
            self.assertTrue(p.is_alive)

        ev.set()

        for p in processes:
            p.join()

        for p in processes:
            self.assertFalse(p.is_alive)

        while not pg.status == "Idle":
            self.assertTrue(pg.status == "Running")
            time.sleep(0.1)  # avoid race condition

        # Make sure all the puids have exited with 0 exit codes
        exit_states = pg.inactive_puids
        for puid, exit_code in exit_states:
            self.assertTrue(puid in puids)
            self.assertEqual(exit_code, 0)
        pg.stop()

    def test_join_from_maintain(self):
        # test that join indeed moves to idle when the processes exit

        ev = Event()

        template = ProcessTemplate(self.event_quitter, args=(ev,), cwd=".")
        pg = ProcessGroup(restart=True)
        pg.add_process(self.nproc, template)
        pg.init()

        self.assertTrue(pg.status == "Idle")

        pg.start()

        self.assertTrue(pg.status == "Maintain")

        puids = pg.puids
        processes = [Process(None, ident=puid) for puid in puids]
        self.assertRaises(TimeoutError, pg.join, 0, True)
        self.assertTrue(pg.status == "Running")

        for p in processes:
            self.assertTrue(p.is_alive)

        ev.set()

        for p in processes:
            p.join()

        # test autotransition
        state_transitioned = False
        while not state_transitioned:
            # have to call pg.status once so it doesn't change between checks
            status = pg.status
            if status == "Idle":
                state_transitioned = True
            else:
                self.assertTrue(status == "Running", f"status supposed to be running, it is {status}")
            time.sleep(0.1)  # keeps the loop from being too hot

        # Make sure all the puids have exited with 0 exit codes
        exit_states = pg.exit_status
        active_puids = pg.puids
        self.assertTrue(all(puid == 0 for puid in active_puids))
        for puid, exit_code in exit_states:
            self.assertTrue(puid in puids)
            self.assertEqual(exit_code, 0)

        pg.stop()

    @unittest.skip("This one is very slow for reasons I don't understand.")
    def test_join_with_timeout(self):

        ev = Event()

        pg = ProcessGroup(restart=True)
        pg.add_process(3, self.template)
        pg.init()

        self.assertTrue(pg.status == "Idle")

        pg.start()

        self.assertTrue(pg.status == "Maintain")

        puids = pg.puids
        processes = [Process(None, ident=puid) for puid in puids]

        self.assertRaises(TimeoutError, pg.join, 0)

        self.assertTrue(pg.status == "Running")

        start = time.monotonic()
        self.assertRaises(TimeoutError, pg.join, 1)
        stop = time.monotonic()

        self.assertAlmostEqual(stop - start, 1, 0)

        ev.set()

        for p in processes:
            p.join()  # processes won't exit. Scheduling/spin wait issue ?

        start = time.monotonic()
        pg.join(timeout=None)
        stop = time.monotonic()
        self.assertAlmostEqual(stop - start, 0, 0)

        self.assertTrue(pg.status == "Idle")

        pg.stop()

    def test_stop_from_idle(self):

        pg = ProcessGroup()
        pg.add_process(self.nproc, self.template)
        pg.init()

        self.assertTrue(pg.status == "Idle")

        man_proc = pg._manager._proc

        self.assertTrue(man_proc.is_alive)

        pg.stop()

        man_proc.join(timeout=None)  # will the manager exit after stop ?

        self.assertFalse(man_proc.is_alive)

    @classmethod
    def _putters(cls, q):

        termflag = False

        def handler(signum, frame):
            nonlocal termflag
            termflag = True

        signal.signal(signal.SIGTERM, handler)

        q.put(True)

        while not termflag:
            time.sleep(1)

    def test_shutdown_from_maintain(self):

        q = Queue()

        template = ProcessTemplate(self._putters, args=(q,), cwd=".")
        pg = ProcessGroup()
        pg.add_process(self.nproc, template)
        pg.init()

        self.assertTrue(pg.status == "Idle")

        pg.start()

        self.assertTrue(pg.status == "Maintain")

        puids = pg.puids

        for _ in puids:
            __ = q.get()  # make sure they've all executed code

        pg.kill(signal.SIGTERM, save_puids=True)

        while not pg.status == "Idle":
            self.assertTrue(pg.status == "Running")
            time.sleep(0.1)

        exit_states = pg.exit_status
        active_puids = pg.puids
        self.assertTrue(all(puid == 0 for puid in active_puids))
        for puid, exit_code in exit_states:
            self.assertTrue(puid in puids)
            self.assertEqual(exit_code, 0)

        for puid in puids:  # check if really dead
            gs_info = query(puid)
            self.assertTrue(gs_info.state == gs_info.State.DEAD)

        pg.stop()

    def test_shutdown_from_join(self):

        q = Queue()

        template = ProcessTemplate(self._putters, args=(q,), cwd=".")
        pg = ProcessGroup(restart=False)
        pg.add_process(self.nproc, template)
        pg.init()

        self.assertTrue(pg.status == "Idle")

        pg.start()

        self.assertTrue(pg.status == "Running")

        puids = pg.puids

        pg.kill(signal.SIGTERM, save_puids=True)

        while not pg.status == "Idle":
            self.assertTrue(pg.status == "Running")
            time.sleep(0.1)

        # Make sure all the puids have exited with SIGTERM exit codes
        exit_states = pg.exit_status
        active_puids = pg.puids
        self.assertTrue(all(puid == 0 for puid in active_puids))
        for puid, exit_code in exit_states:
            self.assertTrue(puid in puids)
            self.assertEqual(exit_code, -1 * signal.SIGTERM.value)

        for puid in puids:  # check if really dead
            gs_info = query(puid)
            self.assertTrue(gs_info.state == gs_info.State.DEAD)

        pg.stop()

    def test_stop_from_maintain(self):

        pg = ProcessGroup()
        pg.add_process(self.nproc, self.template)
        pg.init()

        self.assertTrue(pg.status == "Idle")
        man_proc = pg._manager._proc
        self.assertTrue(man_proc.is_alive)

        pg.start()

        self.assertTrue(pg.status == "Maintain")

        puids = pg.puids

        pg.stop(save_puids=True)
        man_proc.join(timeout=None)  # will the manager exit after stop ?
        self.assertFalse(man_proc.is_alive)

        self.assertTrue(pg.status == "Idle")

        # Make sure all the puids have exited with SIGKILL exit codes
        exit_states = pg.exit_status
        active_puids = pg.puids
        self.assertTrue(all(puid == 0 for puid in active_puids))
        for puid, exit_code in exit_states:
            self.assertTrue(puid in puids)
            self.assertEqual(exit_code, -1 * signal.SIGKILL.value)

        for puid in puids:
            gs_info = query(puid)
            self.assertTrue(gs_info.state == gs_info.State.DEAD)

        pg.stop()


    def test_kill_from_maintain(self):

        pg = ProcessGroup()
        pg.add_process(self.nproc, self.template)
        pg.init()

        self.assertTrue(pg.status == "Idle")
        man_proc = pg._manager._proc
        self.assertTrue(man_proc.is_alive)

        pg.start()

        self.assertTrue(pg.status == "Maintain")

        puids = pg.puids

        pg.kill()

        self.assertTrue(pg.status == "Idle")

        # Make sure all the puids have exited with SIGKILL exit codes
        exit_states = pg.exit_status
        active_puids = pg.puids
        self.assertTrue(all(puid == 0 for puid in active_puids))
        for puid, exit_code in exit_states:
            self.assertTrue(puid in puids)
            self.assertEqual(exit_code, -1 * signal.SIGKILL.value)

        for puid in puids:
            gs_info = query(puid)
            self.assertTrue(gs_info.state == gs_info.State.DEAD)

        pg.stop()

    def test_kill_from_join(self):

        pg = ProcessGroup(restart=False, ignore_error_on_exit=True)
        pg.add_process(self.nproc, self.template)
        pg.init()

        self.assertTrue(pg.status == "Idle")

        pg.start()

        self.assertTrue(pg.status == "Running")

        puids = pg.puids

        for puid in puids:
            gs_info = query(puid)
            self.assertTrue(gs_info.state == gs_info.State.ACTIVE)

        pg.kill()

        self.assertTrue(pg.status == "Idle")

        for puid in puids:
            gs_info = query(puid)
            self.assertTrue(gs_info.state == gs_info.State.DEAD)

        pg.stop()

    @classmethod
    def _failer(cls, ev):

        ev.wait()

        raise Exception("42 - ignore")

    def test_no_error_from_maintain(self):

        ev = Event()

        template = ProcessTemplate(self._failer, args=(ev,), cwd=".")
        pg = ProcessGroup(restart=True)
        pg.add_process(self.nproc, template)
        pg.init()
        pg.start()
        self.assertTrue(pg.status == "Maintain")

        puids = pg.puids

        ev.set()

        cnt = 0
        while not pg.status == "Error":  # maintain should catch failing processes
            self.assertTrue(pg.status == "Maintain")
            time.sleep(0.1)
            cnt += 1
            if cnt == 32:  # good enough
                break
        else:
            cnt = -1

        self.assertTrue(cnt > -1)
        self.assertTrue(pg.status == "Maintain")

        pg.kill()

        while not pg.status == "Idle":
            self.assertTrue(pg.status == "Error")
            time.sleep(0.1)

        for puid in puids:
            gs_info = query(puid)
            self.assertTrue(gs_info.state == gs_info.State.DEAD)

        pg.stop()

    def test_error_and_kill_from_join(self):

        ev = Event()

        template = ProcessTemplate(self._failer, args=(ev,), cwd=".")
        pg = ProcessGroup(restart=False)
        pg.add_process(self.nproc, template)
        pg.init()
        pg.start()
        self.assertTrue(pg.status == "Running")

        puids = pg.puids

        ev.set()

        while not pg.status == "Error":
            self.assertTrue(pg.status == "Running")
            time.sleep(0.1)

        self.assertTrue(pg.status == "Error")

        pg.kill()

        while not pg.status == "Idle":
            self.assertTrue(pg.status == "Error")
            time.sleep(0.05)

        for puid in puids:
            gs_info = query(puid)
            self.assertTrue(gs_info.state == gs_info.State.DEAD)

        pg.stop()

    def test_error_and_stop_from_join(self):

        ev = Event()

        template = ProcessTemplate(self._failer, args=(ev,), cwd=".")
        pg = ProcessGroup(restart=False)
        pg.add_process(self.nproc, template)
        pg.init()

        man_proc = pg._manager._proc
        self.assertTrue(man_proc.is_alive)

        pg.start()
        self.assertTrue(pg.status == "Running")

        puids = pg.puids

        ev.set()

        while not pg.status == "Error":
            self.assertTrue(pg.status == "Running")
            time.sleep(0.1)

        self.assertTrue(pg.status == "Error")

        pg.stop()

        while not pg.status == "Stop":
            self.assertTrue(pg.status == "Error")
            time.sleep(0.05)

        for puid in puids:
            gs_info = query(puid)
            self.assertTrue(gs_info.state == gs_info.State.DEAD)

        man_proc.join()
        self.assertFalse(man_proc.is_alive)

    def test_ignore_error_during_shutdown(self):
        pg = ProcessGroup(ignore_error_on_exit=True)
        pg.add_process(self.nproc, self.template)
        pg.init()

        pg.start()
        self.assertTrue(pg.status == "Maintain")

        puids = pg.puids
        puids.reverse()

        pg.kill(signal=signal.SIGTERM)  # return immediately

        for puid in puids:  # force kill all processes
            try:
                kill(puid, sig=signal.SIGKILL)
            except ProcessError:
                pass

        while not pg.status == "Idle":
            self.assertFalse(pg.status == "Error")

        pg.stop()

    def test_bad_transitions(self):

        pg = ProcessGroup()
        pg.add_process(self.nproc, self.template)

        pg.init()
        self.assertTrue(pg.status == "Idle")
        self.assertRaises(DragonProcessGroupError, pg.kill)

        pg.start()
        self.assertTrue(pg.status == "Maintain")
        self.assertRaises(DragonProcessGroupError, pg.start)
        self.assertRaises(TimeoutError, pg.join, 0)
        self.assertTrue(pg.status == "Running")
        self.assertRaises(DragonProcessGroupError, pg.start)
        self.assertRaises(DragonProcessGroupError, pg.stop)

        pg.kill()
        pg.stop()
        self.assertTrue(pg.status == "Stop")
        self.assertRaises(DragonProcessGroupError, pg.start)
        self.assertRaises(DragonProcessGroupError, pg.stop)
        self.assertRaises(DragonProcessGroupError, pg.kill)
        self.assertRaises(DragonProcessGroupError, pg.kill, signal.SIGTERM)

    def test_inactive_puid_max_size(self):
        # Kill enough processes that the initial inactive_puid array size limit is hit and recover
        # from it gracefully

        # set a sleep time that is long enough for the kill loop to finish
        # before all the procs exit
        template = ProcessTemplate("sleep", args=(128,), cwd=".")

        nworkers = 4
        pg = ProcessGroup(ignore_error_on_exit=True)  # We're going to SIGKILL or SIGTERM everything. Don't raise an exception on it.
        pg.add_process(nworkers, template)
        pg.init()
        man_proc = pg._manager._proc
        self._update_interval_sec = 0.1

        pg.start()

        killed_puids = []

        while len(killed_puids) < 2 * nworkers:
            puids = pg.puids
            try:
                puid = puids[random.randint(0, len(puids) - 1)]
                if puid != 0:
                    kill(puid, sig=signal.SIGKILL)
                    killed_puids.append(puid)
                else:
                    continue
            except ProcessError:  # maybe it disappeared already
                pass

            self.assertTrue(man_proc.is_alive)
            self.assertTrue(pg.status != "Error")

            time.sleep(0.2)

        # Check that all the killed puids appear in inactive puids with correct exit codes
        inactive_puids = pg.inactive_puids
        for puid, _ in inactive_puids:
            self.assertTrue(puid in killed_puids)
            idx = [idx for idx, kpuid in enumerate(killed_puids) if puid == kpuid]
            self.assertTrue(len(idx) == 1)
            killed_puids.pop(idx[0])

        self.assertTrue(len(killed_puids) == 0)

        puids = pg.puids

        pg.kill(signal=signal.SIGTERM)

        while not pg.status == "Idle":
            self.assertTrue(pg.status != "Error")

        self.assertTrue(man_proc.is_alive)

        for puid in puids:
            gs_info = query(puid)
            self.assertTrue(gs_info.state == gs_info.State.DEAD)
            self.assertTrue(puid not in killed_puids)
        pg.stop()

        man_proc.join()

    def test_maintain_stress(self):

        # we will keep killing processes that keep exiting.
        # the class has to maintain them and GS has to handle all of that.
        testtime = 3  # sec

        # set a sleep time that is long enough for the kill loop to finish
        # before all the procs exit
        template = ProcessTemplate("sleep", args=(f"{round(testtime)}",), cwd=".")

        pg = ProcessGroup(ignore_error_on_exit=True)  # We're going to SIGKILL or SIGTERM everything. Don't raise an exception on it.
        pg.add_process(64, template)
        pg.init()
        man_proc = pg._manager._proc
        self._update_interval_sec = 0.1

        pg.start()

        beg = time.monotonic()

        while (time.monotonic() - beg) < testtime:
            puids = pg.puids

            try:
                puid = puids[random.randint(0, len(puids) - 1)]
                if puid != 0:
                    kill(puid, sig=signal.SIGKILL)
                else:
                    continue
            except ProcessError:  # maybe it disappeared already
                pass

            self.assertTrue(man_proc.is_alive)
            self.assertTrue(pg.status != "Error")

            time.sleep(0.2)

        puids = pg.puids

        pg.kill(signal=signal.SIGTERM)

        while not pg.status == "Idle":
            self.assertTrue(pg.status != "Error")

        self.assertTrue(man_proc.is_alive)

        for puid in puids:
            if puid != 0:
                gs_info = query(puid)
                self.assertTrue(gs_info.state == gs_info.State.DEAD)

        pg.stop()

        man_proc.join()

    def test_maintain_clean_exit_with_restarts(self):

        # we will keep killing processes that keep exiting.
        # the class has to maintain them and GS has to handle all of that.
        testtime = 3  # sec

        # set a sleep time that is long enough for the kill loop to finish
        # before all the procs exit
        template = ProcessTemplate("sleep", args=(f"{round(testtime)}",), cwd=".")

        pg = ProcessGroup(ignore_error_on_exit=True)  # We're going to SIGKILL or SIGTERM everything. Don't raise an exception on it.
        pg.add_process(64, template)
        pg.init()
        man_proc = pg._manager._proc
        self._update_interval_sec = 0.1

        pg.start()

        beg = time.monotonic()
        while (time.monotonic() - beg) < testtime:
            puids = pg.puids

            try:
                puid = puids[random.randint(0, len(puids) - 1)]
                kill(puid, sig=signal.SIGKILL)
            except ProcessError:  # maybe it disappeared already
                pass

            self.assertTrue(man_proc.is_alive)
            self.assertTrue(pg.status != "Error")

            time.sleep(0.2)

        puids = pg.puids
        pg.join()

        while not pg.status == "Idle":
            self.assertTrue(pg.status != "Error")

        self.assertTrue(man_proc.is_alive)
        for puid in puids:
            gs_info = query(puid)
            self.assertTrue(gs_info.state == gs_info.State.DEAD)

        pg.stop()
        man_proc.join()

    def test_walltime(self):

        wtime = 3

        pg = ProcessGroup(walltime=wtime)
        pg.add_process(3, self.template)
        pg.init()

        start = time.monotonic()
        pg.start()

        while not pg.status == "Idle":
            self.assertFalse(pg.status == "Error")

        stop = time.monotonic()

        self.assertAlmostEqual(stop - start, wtime, 0)

    @unittest.skip("Waiting for group implementation")
    def test_pmi_enabled(self):
        pass

    @unittest.skip("Waiting for group implementation")
    def test_multiple_groups(self):
        pass

    @unittest.skip("Waiting for group implementation")
    def test_multiple_clients(self):
        pass

    @unittest.skip("Waiting for group implementation")
    def test_multiple_groups_and_multiple_clients(self):
        pass


if __name__ == "__main__":
    unittest.main()
