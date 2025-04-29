import unittest
import time
import sys
import os
import random
import signal
import math
import tempfile
from pathlib import Path
import threading
from functools import wraps

from dragon.globalservices.process import query, kill, ProcessError
from dragon.infrastructure.process_desc import ProcessOptions, ProcessDescriptor

from dragon.native.barrier import Barrier
from dragon.native.process import Process, ProcessTemplate
from dragon.native.event import Event
from dragon.native.queue import Queue
from dragon.native.process_group import (
    ProcessGroup,
    DragonProcessGroupError,
    DragonProcessGroupException,
    DragonUserCodeError,
)


TIMEOUT_DELTA_TOL = 1.0

# we have to test every transition in the state diagram here.
# plus a few robustness tests
FAIL_IDX = 2


def catch_thread_exceptions(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        exceptions_caught_in_threads = {}

        def custom_excepthook(args):
            thread_name = args.thread.name
            exceptions_caught_in_threads[thread_name] = {
                "thread": args.thread,
                "exception": {"type": args.exc_type, "value": args.exc_value, "traceback": args.exc_traceback},
            }

        # Registering our custom excepthook to catch the exception in the threads
        old_excepthook = threading.excepthook
        threading.excepthook = custom_excepthook

        result = func(*args + (exceptions_caught_in_threads,), **kwargs)

        threading.excepthook = old_excepthook
        return result

    return wrapper


def call_exit_function(error_code=True, exception=False):
    if error_code:
        sys.exit(21)

    elif exception:
        raise RuntimeError("testing traceback")


def run_exceptions_worker(idx, exception, error_code):
    """Worker who will raise exception under given conditions"""

    if error_code or exception:
        fail = True

    if fail and idx == FAIL_IDX:
        call_exit_function(error_code=error_code, exception=exception)

    # Now just sleep
    time.sleep(10)


def run_exception_worker_stderr(hit_exception, hit_exit_code):
    """Set up stderr for one worker for exception testing"""

    call_exit_function(hit_exception, hit_exit_code)


def run_exception_worker_stderr(hit_exception, hit_exit_code):
    """Set up stderr for one worker for exception testing"""

    call_exit_function(hit_exception, hit_exit_code)


def config_and_run_exceptions_worker(target_func, idx, hit_exception, hit_exit_code, stderr_file):
    """Set up stderr for one worker for exception testing"""

    try:
        if idx == FAIL_IDX:
            sys.stderr = stderr_file.open("w")
        run_exceptions_worker(idx, hit_exception, hit_exit_code)
    except Exception:
        if idx == FAIL_IDX:
            sys.stderr.flush()
            stderr_file.close()
        raise


def run_exceptions_demo_stderr(num_workers, hit_exception, hit_exit_code, raise_on_exception):
    """Simple test that starts workers and have one of them croak as we specify"""
    caught_ex = None

    if raise_on_exception:
        pg = ProcessGroup(restart=False, ignore_error_on_exit=False)
    else:
        pg = ProcessGroup(restart=False, ignore_error_on_exit=True, walltime=2)

    # create & start processes
    for _ in range(num_workers):
        template = ProcessTemplate(target=run_exception_worker_stderr, args=(hit_exit_code, hit_exception))
        pg.add_process(1, template)

    # Init group
    pg.init()

    try:
        pg.start()
        pg.join()
        pg.close()
    except Exception as ex:
        if raise_on_exception:
            caught_ex = ex
    finally:
        if raise_on_exception and caught_ex is not None:
            raise caught_ex


def run_exceptions_demo(num_workers, hit_exception, hit_exit_code, raise_on_exception):
    """Simple test that starts workers and have one of them croak as we specify"""

    with tempfile.TemporaryDirectory() as d:
        stderr_file = Path(f"{d}/stderr.txt")

        if raise_on_exception:
            pg = ProcessGroup(restart=False, ignore_error_on_exit=False)
        else:
            pg = ProcessGroup(restart=False, ignore_error_on_exit=True, walltime=2)

        # create & start processes
        for idx in range(num_workers):
            template = ProcessTemplate(
                target=config_and_run_exceptions_worker,
                args=(config_and_run_exceptions_worker, idx, hit_exception, hit_exit_code, stderr_file),
            )
            pg.add_process(1, template)

        # Init group
        pg.init()

        try:
            pg.start()
            pg.join()
            pg.close()
        except Exception as ex:
            if raise_on_exception:
                caught_ex = ex
        finally:
            err_string = stderr_file.read_text()
            if raise_on_exception and caught_ex is not None:
                raise caught_ex
            else:
                return err_string


def pg_join_client(pg, bar, tq):
    bar.wait()
    start = time.monotonic()
    with pg:
        pg.join()
    tq.put((time.monotonic() - start))


def pg_join2_client(pg1, pg2, bar, tq):
    bar.wait()
    start = time.monotonic()
    with pg1:
        pg1.join()
    with pg2:
        pg2.join()
    tq.put((time.monotonic() - start))


def wait_for_ev(ev):
    ev.wait()


def file_writer(file_name, string_to_write):
    with open(file_name, "a") as f:
        f.write(string_to_write)


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
        cls.args = ["128"]
        cls.cwd = os.getcwd()
        cls.options = ProcessOptions(make_inf_channels=True)
        cls.template = ProcessTemplate(cls.cmd, args=cls.args, cwd=cls.cwd, options=cls.options)

    def test_init(self):
        pg = ProcessGroup(ignore_error_on_exit=True)
        pg.add_process(self.nproc, self.template)
        pg.init()

        mgr_puid = pg._mgr_p_uid
        self.assertTrue(pg._state == "Idle")

        mgr_puid = pg._mgr_p_uid
        mgr_pdesc = query(mgr_puid)
        self.assertTrue(mgr_pdesc.state == ProcessDescriptor.State.ACTIVE)

        pg.stop()
        pg.close()

        mgr_pdesc = query(mgr_puid)
        self.assertTrue(mgr_pdesc.state == ProcessDescriptor.State.DEAD)

    def test_start_stop(self):
        pg = ProcessGroup(restart=True, ignore_error_on_exit=True)
        pg.add_process(self.nproc, self.template)
        pg.init()

        mgr_puid = pg._mgr_p_uid
        mgr_pdesc = query(mgr_puid)
        self.assertTrue(mgr_pdesc.state == ProcessDescriptor.State.ACTIVE)

        self.assertTrue(pg._state == "Idle")

        pg.start()
        self.assertTrue(pg._state == "Running")

        puids = pg.puids
        processes = [Process(None, ident=puid) for puid in puids]
        for p in processes:
            self.assertTrue(p.is_alive)

        pg.stop()
        pg.join()

        pg.close()
        mgr_pdesc = query(mgr_puid)
        self.assertTrue(mgr_pdesc.state == ProcessDescriptor.State.DEAD)

    def test_alive_puids(self):
        pg = ProcessGroup(restart=True, ignore_error_on_exit=True)
        pg.add_process(self.nproc, self.template)
        pg.init()

        # Make sure the manager is alive
        mgr_pdesc = query(pg._mgr_p_uid)
        self.assertTrue(mgr_pdesc.state == ProcessDescriptor.State.ACTIVE)

        self.assertTrue(pg._state == "Idle")

        pg.start()
        self.assertTrue(pg._state == "Running")

        puids = pg.puids.copy()
        processes = [Process(None, ident=puid) for puid in puids]
        for p in processes:
            self.assertTrue(p.is_alive)

        pg.stop()

        # Confirm the puids have been removed from the active and moved to the inactive
        puid_statuses = pg.inactive_puids
        for puid, ecode in puid_statuses:
            self.assertTrue(puid in puids)

            # Value could be SIGINT, SIGTERN, or SIGKILL. In this test, it should be SIGINT
            self.assertEqual(ecode, -1 * signal.SIGINT.value)

        pg.close()

    @classmethod
    def event_quitter(cls, ev):
        ev.wait()

    def test_join_from_idle(self):
        ev = Event()

        template = ProcessTemplate(self.event_quitter, args=(ev,), cwd=".")
        pg = ProcessGroup(restart=False, ignore_error_on_exit=True)
        pg.add_process(self.nproc, template)
        pg.init()

        self.assertTrue(pg._state == "Idle")

        pg.start()
        self.assertTrue(pg._state == "Running")

        puids = pg.puids
        processes = [Process(None, ident=puid) for puid in puids]

        for p in processes:
            self.assertTrue(p.is_alive)

        ev.set()

        for p in processes:
            p.join()

        for p in processes:
            self.assertFalse(p.is_alive)

        while not pg._state == "Idle":
            time.sleep(0.1)  # loop backoff
            self.assertTrue(pg._state in {"Running", "Idle"})

        # Make sure all the puids have exited with 0 exit codes
        exit_states = pg.inactive_puids
        for puid, exit_code in exit_states:
            self.assertTrue(puid in puids)
            self.assertEqual(exit_code, 0)
        pg.stop()
        pg.close()

    def test_join_from_maintain(self):
        # test that join indeed moves to idle when the processes exit

        ev = Event()

        template = ProcessTemplate(self.event_quitter, args=(ev,), cwd=".")
        pg = ProcessGroup(restart=True, ignore_error_on_exit=True)
        pg.add_process(self.nproc, template)
        pg.init()

        self.assertTrue(pg._state == "Idle")

        pg.start()

        self.assertTrue(pg._state == "Running")

        puids = pg.puids
        processes = [Process(None, ident=puid) for puid in puids]
        self.assertRaises(TimeoutError, pg.join, 0)
        self.assertTrue(pg._state == "Running")

        for p in processes:
            self.assertTrue(p.is_alive)

        ev.set()

        for p in processes:
            p.join()

        # Make sure all the puids have exited with 0 exit codes
        exit_states = pg.exit_status
        self.assertTrue(len(exit_states) > 0)
        for puid, exit_code in exit_states:
            self.assertTrue(puid in puids)
            self.assertEqual(exit_code, 0)

        pg.stop()
        pg.close()

    def test_join_with_timeout(self):
        ev = Event()

        pg = ProcessGroup(restart=False, ignore_error_on_exit=True)
        templ = ProcessTemplate(wait_for_ev, args=[ev])
        pg.add_process(3, templ)
        pg.init()

        self.assertTrue(pg._state == "Idle")

        pg.start()

        self.assertTrue(pg._state == "Running")

        puids = pg.puids
        processes = [Process(None, ident=puid) for puid in puids]

        self.assertRaises(TimeoutError, pg.join, 0)

        self.assertTrue(pg._state == "Running")

        start = time.monotonic()
        self.assertRaises(TimeoutError, pg.join, 0)
        elap = time.monotonic() - start

        self.assertGreaterEqual(elap, 0)
        self.assertLess(elap, (1 + TIMEOUT_DELTA_TOL))

        ev.set()

        for p in processes:
            p.join()  # processes won't exit. Scheduling/spin wait issue ?

        start = time.monotonic()
        pg.join(timeout=None)
        elap = time.monotonic() - start
        self.assertGreaterEqual(elap, 0)
        self.assertLess(elap, TIMEOUT_DELTA_TOL)

        self.assertTrue(pg._state == "Idle")

        pg.stop()
        pg.close()

    def test_stop_from_idle(self):
        pg = ProcessGroup(restart=False, ignore_error_on_exit=True)
        pg.add_process(self.nproc, self.template)
        pg.init()

        mgr_puid = pg._mgr_p_uid
        mgr_pdesc = query(mgr_puid)
        self.assertTrue(mgr_pdesc.state == ProcessDescriptor.State.ACTIVE)

        self.assertTrue(pg._state == "Idle")

        pg.stop()
        pg.close()

        mgr_pdesc = query(mgr_puid)
        self.assertTrue(mgr_pdesc.state == ProcessDescriptor.State.DEAD)

    @classmethod
    def _putters(cls, q):
        q.put(True)

        while True:
            time.sleep(1)

    def test_shutdown_from_maintain(self):
        q = Queue()

        template = ProcessTemplate(self._putters, args=(q,), cwd=".")
        pg = ProcessGroup(restart=True, ignore_error_on_exit=True)
        pg.add_process(self.nproc, template)
        pg.init()

        self.assertTrue(pg._state == "Idle")

        pg.start()

        self.assertTrue(pg._state == "Running")

        puids = pg.puids

        for _ in puids:
            _ = q.get()  # make sure they've all executed code

        pg.terminate()

        while not pg._state == "Idle":
            time.sleep(0.1)
            self.assertTrue(pg._state in {"Running", "Idle"})

        exit_states = pg.exit_status

        for puid, exit_code in exit_states:
            self.assertTrue(puid in puids)
            self.assertEqual(exit_code, -1 * signal.SIGTERM)

        for puid in puids:  # check if really dead
            gs_info = query(puid)
            self.assertTrue(gs_info.state == gs_info.State.DEAD)

        pg.stop()
        pg.close()

    def test_shutdown_from_join(self):
        q = Queue()

        template = ProcessTemplate(self._putters, args=(q,), cwd=".")
        pg = ProcessGroup(restart=False, ignore_error_on_exit=True)
        pg.add_process(self.nproc, template)

        pg.init()

        self.assertTrue(pg._state == "Idle")
        pg.start()

        puids = pg.puids

        self.assertTrue(pg._state == "Running")

        pg.terminate()

        while not pg._state == "Idle":
            time.sleep(0.1)

        pg.join()

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
        pg.close()

    def test_stop_from_maintain(self):
        pg = ProcessGroup(restart=True, ignore_error_on_exit=True)
        pg.add_process(self.nproc, self.template)
        pg.init()

        self.assertTrue(pg._state == "Idle")
        mgr_puid = pg._mgr_p_uid
        mgr_pdesc = query(mgr_puid)
        self.assertTrue(mgr_pdesc.state == ProcessDescriptor.State.ACTIVE)

        pg.start()

        self.assertTrue(pg._state == "Running")

        puids = pg.puids

        pg.stop()
        # May briefly be in state as we finish the joining
        self.assertTrue(pg._state in {"Running", "Idle"})

        # Make sure all the puids have exited with SIGKILL exit codes
        exit_states = pg.exit_status
        active_puids = pg.puids
        self.assertTrue(all(puid == 0 for puid in active_puids))
        for puid, exit_code in exit_states:
            self.assertTrue(puid in puids)
            self.assertEqual(exit_code, -1 * signal.SIGINT.value)

        for puid in puids:
            gs_info = query(puid)
            self.assertTrue(gs_info.state == gs_info.State.DEAD)

        pg.close()
        mgr_pdesc = query(mgr_puid)
        self.assertTrue(mgr_pdesc.state == ProcessDescriptor.State.DEAD)

    def test_kill_from_maintain(self):
        pg = ProcessGroup(restart=True, ignore_error_on_exit=True)
        pg.add_process(self.nproc, self.template)
        pg.init()

        self.assertTrue(pg._state == "Idle")
        mgr_puid = pg._mgr_p_uid
        mgr_pdesc = query(mgr_puid)
        self.assertTrue(mgr_pdesc.state == ProcessDescriptor.State.ACTIVE)

        pg.start()

        self.assertTrue(pg._state == "Running")

        puids = pg.puids

        pg.kill()
        pg.join()
        self.assertTrue(pg._state == "Idle")

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
        pg.close()

    def test_kill_from_join(self):
        pg = ProcessGroup(restart=False, ignore_error_on_exit=True)
        pg.add_process(self.nproc, self.template)
        pg.init()

        self.assertTrue(pg._state == "Idle")

        pg.start()

        self.assertTrue(pg._state == "Running")

        puids = pg.puids

        for puid in puids:
            gs_info = query(puid)
            self.assertTrue(gs_info.state == gs_info.State.ACTIVE)

        pg.kill()

        pg.join()

        self.assertTrue(pg._state == "Idle")

        for puid in puids:
            gs_info = query(puid)
            self.assertTrue(gs_info.state == gs_info.State.DEAD)

        pg.close()

    @classmethod
    def _failer(cls, ev):
        ev.wait()

        raise Exception("42 - ignore")

    def test_no_error_from_maintain(self):
        ev = Event()

        template = ProcessTemplate(self._failer, args=(ev,), cwd=".")
        pg = ProcessGroup(restart=True, ignore_error_on_exit=True)
        pg.add_process(self.nproc, template)
        pg.init()
        pg.start()
        self.assertTrue(pg._state == "Running")

        puids = pg.puids

        ev.set()

        cnt = 0
        while not pg._state == "Error":  # maintain should catch failing processes
            self.assertTrue(pg._state == "Running")
            time.sleep(0.1)
            cnt += 1
            if cnt == 32:  # good enough
                break
        else:
            cnt = -1

        self.assertTrue(cnt > -1)
        self.assertTrue(pg._state == "Running")

        pg.kill()

        while not pg._state == "Idle":
            self.assertTrue(pg._state != "Error")
            time.sleep(0.1)

        for puid in puids:
            gs_info = query(puid)
            self.assertTrue(gs_info.state == gs_info.State.DEAD)

        pg.stop()
        pg.close()

    def test_error_and_kill_from_join(self):
        ev = Event()

        template = ProcessTemplate(self._failer, args=(ev,), cwd=".")
        pg = ProcessGroup(restart=False, ignore_error_on_exit=True)
        pg.add_process(self.nproc, template)
        pg.init()
        pg.start()
        self.assertTrue(pg._state == "Running")

        puids = pg.puids

        ev.set()

        self.assertTrue(pg._state == "Running")
        time.sleep(0.1)

        pg.kill()
        pg.join()

        while not pg._state == "Idle":
            time.sleep(0.05)

        for puid in puids:
            gs_info = query(puid)
            self.assertTrue(gs_info.state == gs_info.State.DEAD)

        pg.close()

    def test_error_and_stop_from_join(self):
        ev = Event()

        template = ProcessTemplate(self._failer, args=(ev,), cwd=".")
        pg = ProcessGroup(restart=False, ignore_error_on_exit=True)
        pg.add_process(self.nproc, template)
        pg.init()

        mgr_puid = pg._mgr_p_uid
        mgr_pdesc = query(mgr_puid)

        self.assertTrue(mgr_pdesc.state == ProcessDescriptor.State.ACTIVE)

        pg.start()
        self.assertTrue(pg._state == "Running")

        puids = pg.puids

        ev.set()

        self.assertTrue(pg._state == "Running")

        pg.stop()
        pg.join()

        for puid in puids:
            gs_info = query(puid)
            self.assertTrue(gs_info.state == gs_info.State.DEAD)

        pg.close()
        mgr_pdesc = query(mgr_puid)
        self.assertTrue(mgr_pdesc.state == ProcessDescriptor.State.DEAD)

    def test_ignore_error_during_shutdown(self):
        pg = ProcessGroup(restart=True, ignore_error_on_exit=True)
        pg.add_process(self.nproc, self.template)
        pg.init()

        pg.start()
        self.assertTrue(pg._state == "Running")

        puids = pg.puids
        puids.reverse()

        pg.terminate()  # return immediately

        for puid in puids:  # force kill all processes
            try:
                kill(puid, sig=signal.SIGKILL)
            except ProcessError:
                pass

        while not pg._state == "Idle":
            time.sleep(0.1)
            self.assertFalse(pg._state == "Error")

        pg.stop()
        pg.close()

    def test_bad_transitions(self):
        pg = ProcessGroup(restart=True, ignore_error_on_exit=True)
        pg.add_process(self.nproc, self.template)

        pg.init()
        self.assertTrue(pg._state == "Idle")
        self.assertRaises(DragonProcessGroupError, pg.kill)

        pg.start()
        self.assertTrue(pg._state == "Running")
        self.assertRaises(DragonProcessGroupError, pg.start)
        self.assertRaises(TimeoutError, pg.join, 0)
        self.assertTrue(pg._state == "Running")
        self.assertRaises(DragonProcessGroupError, pg.start)

        pg.kill()
        pg.stop()
        self.assertTrue(pg._state == "Idle")

        pg.close()
        self.assertRaises(DragonProcessGroupError, pg.stop)
        self.assertRaises(DragonProcessGroupError, pg.kill)
        self.assertRaises(DragonProcessGroupError, pg.terminate)

    def test_inactive_puid_max_size(self):
        # Kill enough processes that the initial inactive_puid array size limit is hit and recover
        # from it gracefully

        # set a sleep time that is long enough for the kill loop to finish
        # before all the procs exit
        template = ProcessTemplate("sleep", args=(128,), cwd=".")

        nworkers = 4
        pg = ProcessGroup(
            restart=True, ignore_error_on_exit=True
        )  # We're going to SIGKILL or SIGTERM everything. Don't raise an exception on it.
        pg.add_process(nworkers, template)
        pg.init()

        mgr_puid = pg._mgr_p_uid
        mgr_pdesc = query(mgr_puid)
        self.assertTrue(mgr_pdesc.state == ProcessDescriptor.State.ACTIVE)

        self._update_interval_sec = 0.1

        pg.start()

        killed_puids = []
        while len(killed_puids) < 2 * nworkers:
            puids = pg.puids
            try:
                puid = puids[random.randint(0, len(puids) - 1)]
                if puid != 0:
                    kill(puid, sig=signal.SIGKILL)

                    pdesc = query(puid)
                    while pdesc.state is ProcessDescriptor.State.ACTIVE:
                        time.sleep(0.1)
                        pdesc = query(puid)
                    killed_puids.append(puid)
                else:
                    continue
            except ProcessError:  # maybe it disappeared already
                pass

            # Wait until the puids no longer have the killed puid in there and
            # that the length matches workers
            while puid in puids and len(puids) != nworkers:
                time.sleep(0.1)
                puids = pg.puids

        # Check that all the killed puids appear in inactive puids with correct exit codes
        inactive_puids = pg.inactive_puids
        for puid, _ in inactive_puids:
            self.assertTrue(puid in killed_puids)
            idx = [idx for idx, kpuid in enumerate(killed_puids) if puid == kpuid]
            self.assertTrue(len(idx) == 1)
            killed_puids.pop(idx[0])

        self.assertTrue(len(killed_puids) == 0)

        puids = pg.puids

        pg.terminate()

        while not pg._state == "Idle":
            time.sleep(0.1)
            self.assertTrue(pg._state != "Error")

        gs_info = query(mgr_puid)
        self.assertTrue(gs_info.state == gs_info.State.ACTIVE)

        for puid in puids:
            gs_info = query(puid)
            self.assertTrue(gs_info.state == gs_info.State.DEAD)
            self.assertTrue(puid not in killed_puids)
        pg.stop()
        pg.close()

        gs_info = query(mgr_puid)
        self.assertTrue(gs_info.state == gs_info.State.DEAD)

    def test_maintain_stress(self):
        # we will keep killing processes that keep exiting.
        # the class has to maintain them and GS has to handle all of that.
        testtime = 3  # sec

        # set a sleep time that is long enough for the kill loop to finish
        # before all the procs exit
        template = ProcessTemplate("sleep", args=(f"{round(testtime)}",), cwd=".")

        pg = ProcessGroup(
            ignore_error_on_exit=True
        )  # We're going to SIGKILL or SIGTERM everything. Don't raise an exception on it.
        pg.add_process(64, template)
        pg.init()
        mgr_puid = pg._mgr_p_uid
        self._update_interval_sec = 0.1

        pg.start()

        beg = time.monotonic()

        while (time.monotonic() - beg) < testtime:
            puids = pg.puids

            try:
                if len(puids) > 1:
                    puid = puids[random.randint(0, len(puids) - 1)]
                    if puid != 0:
                        kill(puid, sig=signal.SIGKILL)
                    else:
                        continue
            except ProcessError:  # maybe it disappeared already
                pass

            mgr_pdesc = query(mgr_puid)
            self.assertTrue(mgr_pdesc.state == ProcessDescriptor.State.ACTIVE)
            self.assertTrue(pg._state != "Error")
            time.sleep(0.2)

        puids = pg.puids

        pg.terminate()

        while not pg._state == "Idle":
            time.sleep(0.1)
            self.assertTrue(pg._state != "Error")

        mgr_pdesc = query(mgr_puid)
        self.assertTrue(mgr_pdesc.state == ProcessDescriptor.State.ACTIVE)

        for puid in puids:
            if puid != 0:
                gs_info = query(puid)
                self.assertTrue(gs_info.state == gs_info.State.DEAD)

        pg.stop()
        pg.close()
        mgr_pdesc = query(mgr_puid)
        self.assertTrue(mgr_pdesc.state == ProcessDescriptor.State.DEAD)

    def test_backtrace_reporting_with_exception(self):
        """Checks that we report backtraces correctly when an exception is raised in worker proc"""

        nworkers = 4
        hit_exception = True
        hit_ec = False
        raise_on_exception = False

        err_string = run_exceptions_demo(nworkers, hit_exception, hit_ec, raise_on_exception)

        self.assertTrue("run_exceptions_worker" in err_string)
        self.assertTrue("testing traceback" in err_string)
        self.assertTrue("call_exit_function" in err_string)

    def test_backtrace_reporting_with_exception_stderr(self):
        """Checks that we report backtraces correctly when an exception is raised in worker proc"""

        nworkers = 4
        hit_exception = True
        hit_ec = False
        raise_on_exception = False

        run_exceptions_demo_stderr(nworkers, hit_exception, hit_ec, raise_on_exception)

    def test_backtrace_reporting_with_error_code_stderr(self):
        """Checks that we report backtraces correctly when an exception is raised in worker proc"""

        nworkers = 4
        hit_exception = False
        hit_ec = True
        raise_on_exception = False

        run_exceptions_demo_stderr(nworkers, hit_exception, hit_ec, raise_on_exception)

    def test_backtrace_reporting_with_raise_stderr(self):
        """Checks that we report backtraces correctly when an exception is raised in worker proc"""

        nworkers = 4
        hit_exception = True
        hit_ec = False
        raise_on_exception = True

        try:
            run_exceptions_demo_stderr(nworkers, hit_exception, hit_ec, raise_on_exception)
            self.assertTrue(False, "Did not raise exception.")
        except DragonUserCodeError as ex:
            pass
        except Exception:
            self.assertTrue(False, "Did not raise DragonUserCodeError exception.")

    def test_worker_exception_raise_with_exception(self):
        """Checks that we correctly raise exception in head process when worker proc raises an exception"""

        nworkers = 4
        hit_exception = True
        hit_ec = False
        raise_on_exception = True

        with self.assertRaises(DragonUserCodeError):
            run_exceptions_demo(nworkers, hit_exception, hit_ec, raise_on_exception)

    def test_backtrace_reporting_with_sys_exit(self):
        """Checks that we report backtraces correctly when worker proc exits with a non-zero code"""

        nworkers = 4
        hit_exception = False
        hit_ec = True
        raise_on_exception = False

        err_string = run_exceptions_demo(nworkers, hit_exception, hit_ec, raise_on_exception)

        self.assertTrue("run_exceptions_worker" in err_string)
        self.assertTrue("SystemExit: 21" in err_string)
        self.assertTrue("call_exit_function" in err_string)

    def test_worker_exception_raise_with_sys_exit(self):
        """Checks that we correctly raise exception in head process when worker proc exits with non-zero code"""

        nworkers = 4
        hit_exception = False
        hit_ec = True
        raise_on_exception = True

        with self.assertRaises(DragonUserCodeError):
            run_exceptions_demo(nworkers, hit_exception, hit_ec, raise_on_exception)

    def test_maintain_clean_exit_with_restarts(self):
        # we will keep killing processes that keep exiting.
        # the class has to maintain them and GS has to handle all of that.
        testtime = 3  # sec

        # set a sleep time that is long enough for the kill loop to finish
        # before all the procs exit
        template = ProcessTemplate("sleep", args=(f"{round(testtime)}",), cwd=".")

        pg = ProcessGroup(
            restart=True, ignore_error_on_exit=True
        )  # We're going to SIGKILL or SIGTERM everything. Don't raise an exception on it.
        pg.add_process(64, template)
        pg.init()

        mgr_puid = pg._mgr_p_uid
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

            mgr_pdesc = query(mgr_puid)
            self.assertTrue(mgr_pdesc.state == ProcessDescriptor.State.ACTIVE)
            self.assertTrue(pg._state != "Error")

            time.sleep(0.2)

        puids = pg.puids
        pg.stop()
        pg.join()

        while not pg._state == "Idle":
            time.sleep(0.1)
            self.assertTrue(pg._state != "Error")

        mgr_pdesc = query(mgr_puid)
        self.assertTrue(mgr_pdesc.state == ProcessDescriptor.State.ACTIVE)
        for puid in puids:
            gs_info = query(puid)
            self.assertTrue(gs_info.state == gs_info.State.DEAD)

        pg.close()

        mgr_pdesc = query(mgr_puid)
        self.assertTrue(mgr_pdesc.state == ProcessDescriptor.State.DEAD)

    def test_walltime(self):
        wtime = 3

        pg = ProcessGroup(walltime=wtime, ignore_error_on_exit=True)
        pg.add_process(3, self.template)
        pg.init()

        start = time.monotonic()
        pg.start()

        while not pg._state == "Idle":
            time.sleep(0.1)
            self.assertFalse(pg._state == "Error")

        elap = time.monotonic() - start

        pg.stop()
        self.assertGreaterEqual(elap, wtime)
        self.assertLess(elap, (wtime + TIMEOUT_DELTA_TOL))
        pg.close()

    def test_python_exe_with_infra(self):
        exe = sys.executable
        args = ["-c", "import dragon; import multiprocessing as mp; mp.set_start_method('dragon'); q = mp.Queue()"]

        pg = ProcessGroup(ignore_error_on_exit=True)

        templ = ProcessTemplate(exe, args, options=ProcessOptions(make_inf_channels=True))
        pg.add_process(1, templ)
        pg.init()
        pg.start()

        puids = pg.puids
        pg.join()

        for puid in puids:
            gs_info = query(puid)
            self.assertTrue(gs_info.ecode == 0)

        pg.stop()
        pg.close()

    @unittest.skip("Waiting for group implementation")
    def test_pmi_enabled(self):
        pass

    def test_multiple_groups(self):
        pg1 = ProcessGroup()
        pg2 = ProcessGroup()

        exe = "true"
        run_dir = "."

        pg1.add_process(nproc=1, template=ProcessTemplate(target=exe, args=[], cwd=run_dir))

        pg2.add_process(nproc=1, template=ProcessTemplate(target=exe, args=[], cwd=run_dir))

        pg1.init()
        pg1.start()
        pg2.init()
        pg2.start()

        pg1.join()
        pg2.join()
        pg1.close()
        pg2.close()

    @unittest.skip("Bug report filed in AICI-1565. Can be re-enabled pending fix.")
    def test_multiple_clients(self):
        wtime = 1.0
        tq = Queue()
        bar = Barrier(2)

        pg = ProcessGroup(ignore_error_on_exit=True)

        exe = "sleep"
        args = ["20"]
        run_dir = "."

        pg.add_process(nproc=1, template=ProcessTemplate(target=exe, args=args, cwd=run_dir))

        pg.init()
        pg.start()

        p = Process(target=pg_join_client, args=(pg, bar, tq))
        p.start()

        bar.wait()

        try:
            pg.join(timeout=wtime)
        except TimeoutError:
            pass
        pg.send_signal(sig=signal.SIGTERM, hide_stderr=True)
        pg.join()
        pg.close()

        elap = tq.get()
        p.join()

        self.assertGreaterEqual(elap, wtime)
        self.assertLess(elap, (wtime + TIMEOUT_DELTA_TOL))

    def test_multiple_groups_and_multiple_clients(self):
        wtime = 1.0
        tq = Queue()
        bar = Barrier(2)

        pg1 = ProcessGroup(ignore_error_on_exit=True)
        pg2 = ProcessGroup(ignore_error_on_exit=True)

        exe = "sleep"
        args = ["20"]
        run_dir = "."

        pg1.add_process(nproc=1, template=ProcessTemplate(target=exe, args=args, cwd=run_dir))
        pg2.add_process(nproc=1, template=ProcessTemplate(target=exe, args=args, cwd=run_dir))

        pg1.init()
        pg2.init()
        pg1.start()
        pg2.start()

        p = Process(target=pg_join2_client, args=(pg1, pg2, bar, tq))
        p.start()

        bar.wait()

        try:
            pg1.join(timeout=wtime)
        except TimeoutError:
            pass

        pg1.send_signal(sig=signal.SIGTERM, hide_stderr=True)
        pg2.send_signal(sig=signal.SIGTERM, hide_stderr=True)
        pg1.join()
        pg2.join()
        pg1.close()
        pg2.close()

        elap = tq.get()
        p.join()

        self.assertGreaterEqual(elap, wtime)
        self.assertLess(elap, (wtime + TIMEOUT_DELTA_TOL))

    def test_templating_cwd(self):
        # this is necessary for running test from test_native.py
        my_env = None
        if os.path.basename(os.getcwd()) == "test":
            my_env = {"PYTHONPATH": f"{os.getcwd()}:{os.environ.get('PYTHONPATH', '')}"}

        with tempfile.TemporaryDirectory(dir=os.getcwd()) as temp_dir:
            path = temp_dir

            file_name = "test_cwd_file.txt"
            string_to_write = f"wrote to {path} \n"

            templ = ProcessTemplate(file_writer, args=(file_name, string_to_write), cwd=path, env=my_env)
            pg = ProcessGroup(restart=False, ignore_error_on_exit=True)
            pg.add_process(1, templ)

            func, args, kwargs = templ.get_original_python_parameters()
            self.assertTrue(callable(func))
            self.assertIsInstance(args[0], str)
            self.assertIsInstance(args[1], str)
            self.assertIsInstance(args, tuple)
            self.assertTrue(kwargs == {})

            self.assertTrue(templ.is_python)

            pg.init()
            pg.start()
            pg.join()
            pg.close()

            file_path = os.path.join(path, file_name)

            self.assertTrue(os.path.exists(file_path))

            with open(file_path, "r") as f:
                self.assertEqual(f.readline(), string_to_write)


if __name__ == "__main__":
    unittest.main()
