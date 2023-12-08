"""This file contains multi-node tests for the process group classes.
"""
import os
import unittest
import time
import random
import signal

import dragon

from dragon.native.process_group import ProcessGroup
from dragon.native.process import TemplateProcess, Process
from dragon.native.machine import cpu_count, current

from dragon.globalservices.process import query as process_query, kill, signal as dragon_signal
from dragon.globalservices.node import query as node_query, get_list as node_get_list
from dragon.infrastructure.process_desc import ProcessOptions
from dragon.infrastructure.policy import Policy


class TestProcessGroupMultiNode(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set parameters"""

        # a few standard values
        cls.nproc = cpu_count() * 2
        cls.cmd = "sleep"
        cls.args = ("1000",)
        cls.cwd = os.getcwd()
        cls.options = ProcessOptions(make_inf_channels=True)
        cls.template = TemplateProcess(cls.cmd, args=cls.args, cwd=cls.cwd)

    @unittest.skip(f"CIRRUS-1163: Will fail until process descriptor contains a h_uid/host_id.")
    def test_placement_roundrobin(self):

        pg = ProcessGroup(restart=False)
        pg.add_process(self.nproc, self.template)
        pg.init()

        pg.start()

        self.assertTrue(pg.status == "Running")

        puids = pg.puids  # list is ordered by placement policy !

        huids = node_get_list()

        for i, puid in enumerate(puids):

            idx = i % len(huids)

            p = Process(None, ident=puid)
            self.assertTrue(
                p.node == huids[i], f"{p.node=} {huids[idx]=} {idx=} "
            )  # will fail until placement is fixed to h_uid

        pg.kill()

        self.assertTrue(pg.status == "Idle")

        pg.stop()

        self.assertTrue(pg.status == "Stop")

    def test_maintain_stress(self):
        """This test may take a minute."""

        # we will keep killing processes, 10% will keep exiting on their own.

        testtime = 10  # sec

        pg = ProcessGroup(ignore_error_on_exit=True, policy=Policy())

        count = 0
        for i in range(self.nproc):
            if i % 10 == 0:
                args = (f"{testtime/self.nproc * (i+1)}",)
                t = TemplateProcess("sleep", args=args, cwd=self.cwd)
                pg.add_process(1, t)
            else:
                count += 1
        args = ("10000000",)
        t = TemplateProcess("sleep", args=args, cwd=self.cwd)
        pg.add_process(count, t)

        pg.init()

        man_proc = pg._manager._proc
        self._update_interval_sec = 0.1

        pg.start()

        beg = time.monotonic()

        while time.monotonic() - beg < testtime:

            puids = pg.puids

            try:
                puid = puids[random.randint(0, len(puids))]
                kill(puid, sig=dragon_signal.SIGKILL)
            except Exception:  # maybe it disappeared already
                pass

            self.assertTrue(man_proc.is_alive)
            self.assertTrue(pg.status != "Error")

            time.sleep(testtime // 10)

        puids = pg.puids

        pg.kill(signal.SIGTERM)

        while not pg.status == "Idle":
            self.assertTrue(pg.status != "Error")

        self.assertTrue(man_proc.is_alive)

        for puid in puids:
            gs_info = process_query(puid)
            self.assertTrue(gs_info.state == gs_info.State.DEAD)

        pg.stop()

        self.assertTrue(pg.status == "Stop")

        man_proc.join()

    def test_walltime(self):

        wtime = 3

        # small number of procs, so that the startup overhead is minimal
        nproc = len(node_get_list())

        pg = ProcessGroup(walltime=wtime)
        pg.add_process(nproc, self.template)
        pg.init()

        pg.start()

        start = time.monotonic()
        while not pg.status == "Idle":
            self.assertFalse(pg.status == "Error")
            time.sleep(0.5)

        stop = time.monotonic()

        self.assertAlmostEqual(stop - start, wtime, 0)

        pg.stop()

    @unittest.skip(f"CIRRUS-1831: Will fail until PG api is fixed. The TODO comment should also be addressed.")
    def test_node_id(self):
        pg = ProcessGroup(self.template, 4)
        pg.start()

        nodes = node_get_list()
        puids = pg.puids
        for puid in puids:
            gs_info = process_query(puid)
            print(gs_info)
            # TODO: Increment counter for each node, assert we've got equal distribution of procs

        pg.stop()



if __name__ == "__main__":
    unittest.main()
