#!/usr/bin/env python3
"""Single node gs startup/teardown smoke test"""

import logging
import multiprocessing
import os
import threading
import time
import unittest

from dragon.infrastructure.node_desc import NodeDescriptor
from dragon.utils import B64

import dragon.channels as dch
import dragon.globalservices.server as dserver
import dragon.dlogging.util as dlog
import dragon.infrastructure.facts as dfacts
import dragon.infrastructure.messages as dmsg
import dragon.infrastructure.parameters as dparm
import dragon.managed_memory as dmm
import support.util as tsu
from dragon.infrastructure.queue import InfraQueue



def startup(gs_input, shep_input, bela_input, gs_stdout, logname=""):
    dlog.setup_logging(basename="gs_" + logname, level=logging.DEBUG, force=True)
    log = logging.getLogger("single startup entry")
    log.info("starting")
    the_ctx = dserver.GlobalContext()

    try:
        the_ctx.run_startup(
            mode=the_ctx.LaunchModes.TEST_STANDALONE_SINGLE,
            test_gs_input=gs_input,
            test_ls_inputs=[shep_input],
            test_bela_input=bela_input,
            test_gs_stdout=gs_stdout,
        )

        the_ctx.run_global_server(
            mode=the_ctx.LaunchModes.TEST_STANDALONE_SINGLE,
            test_gs_input=gs_input,
            test_ls_inputs=[shep_input],
            test_bela_input=bela_input,
            test_gs_stdout=gs_stdout,
        )

        log.info("normal exit")
    except:
        log.exception("fatal startup error")
        gs_stdout.send(dmsg.AbnormalTermination(tag=0).serialize())

    logging.shutdown()


def main_loop_exit(gs_input, shep_input, bela_input, gs_stdout, logname=""):
    dlog.setup_logging(basename="gs_" + logname, level=logging.DEBUG, force=True)
    log = logging.getLogger("main loop exit test")
    log.info("starting")

    the_ctx = dserver.GlobalContext()
    the_ctx._launch_mode = the_ctx.LaunchModes.TEST_STANDALONE_SINGLE

    try:
        the_ctx.run_global_server(
            mode=the_ctx.LaunchModes.TEST_STANDALONE_SINGLE,
            test_gs_input=gs_input,
            test_ls_inputs=[shep_input],
            test_bela_input=bela_input,
            test_gs_stdout=gs_stdout,
        )
        log.info("normal exit")
    except:
        log.exception("fatal main loop error")
        gs_stdout.send(dmsg.AbnormalTermination(tag=0).serialize())

    logging.shutdown()


def updown(gs_input, shep_input, bela_input, gs_stdout, logname=""):
    dlog.setup_logging(basename="gs_" + logname, level=logging.DEBUG, force=True)
    log = logging.getLogger("up down test")
    log.info("starting")

    the_ctx = dserver.GlobalContext()

    try:
        the_ctx.run_startup(
            mode=the_ctx.LaunchModes.TEST_STANDALONE_SINGLE,
            test_gs_input=gs_input,
            test_ls_inputs=[shep_input],
            test_bela_input=bela_input,
            test_gs_stdout=gs_stdout,
        )

        the_ctx.run_global_server(
            mode=the_ctx.LaunchModes.TEST_STANDALONE_SINGLE,
            test_gs_input=gs_input,
            test_ls_inputs=[shep_input],
            test_bela_input=bela_input,
            test_gs_stdout=gs_stdout,
        )

        the_ctx.run_teardown(
            test_gs_input=gs_input,
            test_ls_inputs=[shep_input],
            test_bela_input=bela_input,
            test_gs_stdout=gs_stdout,
        )
    except:
        log.exception("fatal error")
        gs_stdout.send(dmsg.AbnormalTermination(tag=0).serialize())

    log.info("normal exit")
    logging.shutdown()


class SingleInternal(unittest.TestCase):
    def setUp(self) -> None:
        self.gs_stdout_rh, self.gs_stdout_wh = multiprocessing.Pipe(duplex=False)

        username = os.environ.get("USER", str(os.getuid()))
        self.pool_name = "si_" + username
        self.pool_size = 2**30
        self.pool_uid = 18
        self.mpool = dmm.MemoryPool(self.pool_size, self.pool_name, self.pool_uid, None)

        chan = dch.Channel(self.mpool, dfacts.GS_INPUT_CUID)
        self.gs_input_chan = chan
        self.gs_input = InfraQueue(pool=self.mpool, main_channel=chan)
        self.gs_input_rh = self.gs_input
        self.gs_input_wh = self.gs_input

        chan = dch.Channel(self.mpool, dfacts.BASE_LS_CUID)
        self.shep_input_chan = chan
        self.shep_input = InfraQueue(pool=self.mpool, main_channel=chan)
        self.shep_input_rh = self.shep_input
        self.shep_input_wh = self.shep_input

        chan = dch.Channel(self.mpool, dfacts.BASE_BE_CUID)
        self.bela_input_chan = chan
        self.bela_input = InfraQueue(pool=self.mpool, main_channel=chan)
        self.bela_input_rh = self.bela_input
        self.bela_input_wh = self.bela_input

        self.node_sdesc = NodeDescriptor.get_localservices_node_conf(is_primary=True).sdesc
        self.dut = None
        self.tag = 0

    def tearDown(self) -> None:
        self.gs_stdout_rh.close()
        self.gs_stdout_wh.close()
        self.gs_input.close()
        self.shep_input.close()
        self.bela_input.close()
        self.gs_input_chan.destroy()
        self.shep_input_chan.destroy()
        self.bela_input_chan.destroy()
        self.mpool.destroy()
        self.dut.join(1)

    def next_tag(self):
        tmp = self.tag
        self.tag += 1
        return tmp

    def test_startup_lifecycle_err(self):
        self.dut = threading.Thread(
            target=startup,
            args=(self.gs_input, self.shep_input, self.bela_input, self.gs_stdout_wh),
            kwargs={"logname": self.id()},
            daemon=True,
            name="globalservices",
        )

        self.dut.start()
        tsu.get_and_check_type(self.shep_input_rh, dmsg.GSPingLS)
        self.gs_input_wh.put(tsu._BadMsg(tag=self.next_tag()))  # triggers error
        tsu.get_and_check_type(self.gs_stdout_rh, dmsg.AbnormalTermination)

    def test_main_loop_exit(self):
        self.dut = threading.Thread(
            target=main_loop_exit,
            args=(self.gs_input, self.shep_input, self.bela_input, self.gs_stdout_wh),
            kwargs={"logname": self.id()},
            daemon=True,
            name="globalservices",
        )
        self.dut.start()
        self.gs_input_wh.put(dmsg.GSTeardown(tag=0))
        time.sleep(1)
        self.assertFalse(self.dut.is_alive())

    def test_main_loop_bad_exit(self):
        self.dut = threading.Thread(
            target=main_loop_exit,
            args=(self.gs_input, self.shep_input, self.bela_input, self.gs_stdout_wh),
            kwargs={"logname": self.id()},
            daemon=True,
            name="globalservices",
        )
        self.dut.start()
        self.gs_input_wh.put(tsu._BadMsg(tag=self.next_tag()))
        tsu.get_and_check_type(self.gs_stdout_rh, dmsg.AbnormalTermination)

    def test_updown(self):
        self.dut = threading.Thread(
            target=updown,
            args=(self.gs_input, self.shep_input, self.bela_input, self.gs_stdout_wh),
            kwargs={"logname": self.id()},
            daemon=True,
            name="globalservices",
        )
        self.dut.start()
        tsu.get_and_check_type(self.shep_input_rh, dmsg.GSPingLS)
        self.gs_input_wh.put(dmsg.LSPingGS(tag=self.next_tag(), node_sdesc=self.node_sdesc))
        tsu.get_and_check_type(self.bela_input_rh, dmsg.GSIsUp)
        self.gs_input_wh.put(dmsg.GSTeardown(tag=self.next_tag()))
        tsu.get_and_check_type(self.gs_stdout_rh, dmsg.GSHalted)


if __name__ == "__main__":
    unittest.main()
