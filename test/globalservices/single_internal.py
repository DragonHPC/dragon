#!/usr/bin/env python3
"""Single node gs startup/teardown smoke test"""

import logging
import multiprocessing
import threading
import time
import unittest

from dragon.infrastructure.node_desc import NodeDescriptor

import dragon.globalservices.server as dserver
import dragon.dlogging.util as dlog
import dragon.infrastructure.messages as dmsg
import support.util as tsu


def startup(gs_input, shep_input, bela_input, gs_stdout, logname=""):
    dlog.setup_logging(basename="gs_" + logname, level=logging.DEBUG, force=True)
    log = logging.getLogger("single startup entry")
    log.info("starting")
    the_ctx = dserver.GlobalContext()

    try:
        the_ctx.run_startup(
            mode=the_ctx.LaunchModes.TEST_STANDALONE_SINGLE,
            test_gs_input=gs_input,
            test_shep_inputs=[shep_input],
            test_bela_input=bela_input,
            test_gs_stdout=gs_stdout
        )

        the_ctx.run_global_server(
            mode=the_ctx.LaunchModes.TEST_STANDALONE_SINGLE,
            test_gs_input=gs_input,
            test_shep_inputs=[shep_input],
            test_bela_input=bela_input,
            test_gs_stdout=gs_stdout
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
            test_shep_inputs=[shep_input],
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
            test_shep_inputs=[shep_input],
            test_bela_input=bela_input,
            test_gs_stdout=gs_stdout,
        )

        the_ctx.run_global_server(
            mode=the_ctx.LaunchModes.TEST_STANDALONE_SINGLE,
            test_gs_input=gs_input,
            test_shep_inputs=[shep_input],
            test_bela_input=bela_input,
            test_gs_stdout=gs_stdout,
        )

        the_ctx.run_teardown(
            test_gs_input=gs_input,
            test_shep_inputs=[shep_input],
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
        self.gs_input_rh, self.gs_input_wh = multiprocessing.Pipe(duplex=False)
        self.gs_stdout_rh, self.gs_stdout_wh = multiprocessing.Pipe(duplex=False)
        self.shep_input_rh, self.shep_input_wh = multiprocessing.Pipe(duplex=False)
        self.bela_input_rh, self.bela_input_wh = multiprocessing.Pipe(duplex=False)
        self.node_sdesc = NodeDescriptor.get_localservices_node_conf(is_primary=True).sdesc
        self.dut = None
        self.tag = 0

    def tearDown(self) -> None:
        self.gs_input_rh.close()
        self.gs_input_wh.close()
        self.gs_stdout_rh.close()
        self.gs_stdout_wh.close()
        self.bela_input_rh.close()
        self.bela_input_wh.close()
        self.shep_input_rh.close()
        self.shep_input_wh.close()
        self.dut.join(1)

    def next_tag(self):
        tmp = self.tag
        self.tag += 1
        return tmp


    def test_startup_lifecycle_err(self):
        self.dut = threading.Thread(
            target=startup,
            args=(self.gs_input_rh, self.shep_input_wh, self.bela_input_wh, self.gs_stdout_wh),
            kwargs={"logname": self.id()},
            daemon=True,
            name="globalservices",
        )

        self.dut.start()
        tsu.get_and_check_type(self.shep_input_rh, dmsg.GSPingSH)
        self.gs_input_wh.send("crap")  # triggers error
        tsu.get_and_check_type(self.gs_stdout_rh, dmsg.AbnormalTermination)


    def test_main_loop_exit(self):
        self.dut = threading.Thread(
            target=main_loop_exit,
            args=(self.gs_input_rh, self.shep_input_wh, self.bela_input_wh, self.gs_stdout_wh),
            kwargs={"logname": self.id()},
            daemon=True,
            name="globalservices",
        )
        self.dut.start()
        self.gs_input_wh.send(dmsg.GSTeardown(tag=0).serialize())
        time.sleep(1)
        self.assertFalse(self.dut.is_alive())

    def test_main_loop_bad_exit(self):
        self.dut = threading.Thread(
            target=main_loop_exit,
            args=(self.gs_input_rh, self.shep_input_wh, self.bela_input_wh, self.gs_stdout_wh),
            kwargs={"logname": self.id()},
            daemon=True,
            name="globalservices",
        )
        self.dut.start()
        self.gs_input_wh.send("crap")
        tsu.get_and_check_type(self.gs_stdout_rh, dmsg.AbnormalTermination)

    def test_updown(self):
        self.dut = threading.Thread(
            target=updown,
            args=(self.gs_input_rh, self.shep_input_wh, self.bela_input_wh, self.gs_stdout_wh),
            kwargs={"logname": self.id()},
            daemon=True,
            name="globalservices",
        )
        self.dut.start()
        tsu.get_and_check_type(self.shep_input_rh, dmsg.GSPingSH)
        self.gs_input_wh.send(dmsg.SHPingGS(tag=self.next_tag(), node_sdesc=self.node_sdesc).serialize())
        tsu.get_and_check_type(self.bela_input_rh, dmsg.GSIsUp)
        self.gs_input_wh.send(dmsg.GSTeardown(tag=self.next_tag()).serialize())
        tsu.get_and_check_type(self.gs_stdout_rh, dmsg.GSHalted)


if __name__ == "__main__":
    unittest.main()
