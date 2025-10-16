#!/usr/bin/env python3
"""Test script for single node shepherd message handling"""

import shim_dragon_paths
import inspect
import multiprocessing as mp
import logging
import os
import signal
import sys
import time
import unittest

import dragon.channels as dch
import dragon.managed_memory as dmm

import dragon.infrastructure.messages as dmsg
import dragon.infrastructure.connection as dconn
import dragon.infrastructure.facts as dfacts
import dragon.infrastructure.parameters as parms
import dragon.localservices.local_svc as dsls
import dragon.dlogging.util as dlog
import dragon.utils as du

import support.util as tsu

get_msg = tsu.get_and_parse


def run_ls(shep_stdin_queue, shep_stdout_queue, name_addition=""):
    dlog.setup_logging(basename="ls_" + name_addition, level=logging.DEBUG)
    log = logging.getLogger("outer runner")
    parms.this_process.mode = dfacts.SINGLE_MODE
    os.environ.update(parms.this_process.env())

    log.info("starting ls from runner")
    try:
        dsls.single(ls_stdin=shep_stdin_queue, ls_stdout=shep_stdout_queue)
    except RuntimeError as rte:
        log.exception("runner")
        shep_stdout_queue.send(dmsg.AbnormalTermination(tag=dsls.get_new_tag(), err_info=f"{rte!s}").serialize())


# TODO: pending pool attach lifecycle solution
ATTACH_POOLS = True


class SingleLS(unittest.TestCase):
    DEFAULT_TEST_POOL_SIZE = 2**20

    def setUp(self) -> None:
        self.shep_stdin_rh, self.shep_stdin_wh = mp.Pipe(duplex=False)
        self.shep_stdout_rh, self.shep_stdout_wh = mp.Pipe(duplex=False)
        self.mode = dfacts.SINGLE_MODE
        self.tag = 0

    def tearDown(self) -> None:
        self.shep_stdout_rh.close()
        self.shep_stdin_wh.close()

    def next_tag(self):
        tmp = self.tag
        self.tag += 1
        return tmp

    def do_bringup(self):
        test_name = self.__class__.__name__ + "_" + inspect.stack()[1][0].f_code.co_name
        self.proc = mp.Process(
            target=run_ls,
            args=(self.shep_stdin_rh, self.shep_stdout_wh),
            kwargs={"name_addition": test_name},
            daemon=True,
            name="local_svc",
        )
        self.proc.start()

        self.shep_stdin_wh.send(dmsg.BENodeIdxSH(tag=self.next_tag(), node_idx=0, net_conf_key="0").serialize())

        msg = tsu.get_and_check_type(self.shep_stdout_rh, dmsg.SHPingBE)

        if ATTACH_POOLS:
            default_pd = du.B64.str_to_bytes(msg.default_pd)
            self.def_pool = dmm.MemoryPool.attach(default_pd)

            inf_pd = du.B64.str_to_bytes(msg.inf_pd)
            self.inf_pool = dmm.MemoryPool.attach(inf_pd)

        shep_cd = du.B64.str_to_bytes(msg.shep_cd)
        self.shep_ch = dch.Channel.attach(shep_cd)
        self.shep_main_wh = dconn.Connection(outbound_initializer=self.shep_ch)

        be_cd = du.B64.str_to_bytes(msg.be_cd)
        self.be_ch = dch.Channel.attach(be_cd)
        self.be_main_rh = dconn.Connection(inbound_initializer=self.be_ch)

        gs_cd = du.B64.str_to_bytes(msg.gs_cd)
        self.gs_ch = dch.Channel.attach(gs_cd)
        self.gs_main_rh = dconn.Connection(inbound_initializer=self.gs_ch)

        self.shep_main_wh.send(dmsg.BEPingSH(tag=0).serialize())

        tsu.get_and_check_type(self.be_main_rh, dmsg.SHChannelsUp)

        self.shep_main_wh.send(dmsg.GSPingSH(tag=0).serialize())

        tsu.get_and_check_type(self.gs_main_rh, dmsg.SHPingGS)

    def do_teardown(self):
        self.shep_main_wh.send(dmsg.GSHalted(tag=self.next_tag()).serialize())
        count = 0
        msg = get_msg(self.be_main_rh)

        # keep ignoring everything received other than GSHalted
        while not isinstance(msg, dmsg.GSHalted) and count < 100:
            msg = get_msg(self.be_main_rh)
            count += 1

        self.shep_main_wh.send(dmsg.SHTeardown(tag=self.next_tag()).serialize())

        count = 0
        msg = get_msg(self.be_main_rh)

        # keep ignoring everything received other than SHHaltBE
        while not isinstance(msg, dmsg.SHHaltBE) and count < 100:
            msg = get_msg(self.be_main_rh)
            count += 1

        self.shep_stdin_wh.send(dmsg.BEHalted(tag=self.next_tag()).serialize())
        tsu.get_and_check_type(self.shep_stdout_rh, dmsg.SHHalted)

        self.proc.join()

        # In place of the next three destroy calls we could have called
        # detach, but this demonstrates that we can call destroy twice
        # on a channel and it will work. Local services destroyed first,
        # and then after the join above we know we can destroy here as well.
        # The advantage of calling destroy here is that since we know we are
        # done with the channel, destroy will ignore any ref counting and clean
        # it up.
        try:
            self.gs_ch.destroy()
        except:
            pass

        try:
            self.be_ch.destroy()
        except:
            pass

        try:
            self.shep_ch.destroy()
        except:
            pass

        if ATTACH_POOLS:
            # Calling destroy here is OK because we have joined on exit of local services
            # which already destoyed the pools. Calling destroy here forcefully detaches
            # from the pool (regardless of ref counting).
            self.def_pool.destroy()
            self.inf_pool.destroy()

    def do_abnormal_teardown(self):
        # when the BE receives AbnormalTermination, then the SHTeardown proceeds as usual
        # and LS expects SHTeardown in order to shut down
        tsu.get_and_check_type(self.be_main_rh, dmsg.AbnormalTermination)
        self.shep_main_wh.send(dmsg.SHTeardown(tag=self.next_tag()).serialize())
        tsu.get_and_check_type(self.be_main_rh, dmsg.SHHaltBE)
        self.shep_stdin_wh.send(dmsg.BEHalted(tag=self.next_tag()).serialize())
        tsu.get_and_check_type(self.shep_stdout_rh, dmsg.SHHalted)

        self.proc.join()

        try:
            self.gs_ch.detach()
        except:
            pass

        try:
            self.be_ch.detach()
        except:
            pass

        try:
            self.shep_ch.detach()
        except:
            pass

        if ATTACH_POOLS:
            # Calling destroy here is OK because we have joined on exit of local services
            # which already destoyed the pools. Calling destroy here forcefully detaches
            # from the pool (regardless of ref counting).
            self.def_pool.destroy()
            self.inf_pool.destroy()

    def _make_pool(self, tag, p_uid, r_c_uid, size, m_uid, name):
        self.shep_main_wh.send(
            dmsg.SHPoolCreate(tag=tag, p_uid=p_uid, r_c_uid=r_c_uid, size=size, m_uid=m_uid, name=name).serialize()
        )
        res = tsu.get_and_check_type(self.gs_main_rh, dmsg.SHPoolCreateResponse)
        self.assertEqual(dmsg.SHPoolCreateResponse.Errors.SUCCESS, res.err)

    def _destroy_pool(self, tag, p_uid, r_c_uid, m_uid):
        # Destroy the pool
        self.shep_main_wh.send(dmsg.SHPoolDestroy(tag=tag, p_uid=p_uid, r_c_uid=r_c_uid, m_uid=m_uid).serialize())
        res = tsu.get_and_check_type(self.gs_main_rh, dmsg.SHPoolDestroyResponse)
        self.assertEqual(dmsg.SHPoolDestroyResponse.Errors.SUCCESS, res.err)

    def test_bringup_teardown(self):
        # Tests normal bringup followed by teardown.
        self.do_bringup()
        self.do_teardown()

    def test_process_fwdoutput(self):
        self.do_bringup()
        target_puid = 17777

        the_tag = self.next_tag()
        self.shep_main_wh.send(
            dmsg.SHProcessCreate(
                tag=the_tag,
                exe=sys.executable,
                args=["shepherd/proc2.py"],
                p_uid=dfacts.GS_PUID,
                r_c_uid=dfacts.GS_INPUT_CUID,
                t_p_uid=target_puid,
            ).serialize()
        )

        process_create_resp = tsu.get_and_check_type(self.gs_main_rh, dmsg.SHProcessCreateResponse)

        self.assertEqual(process_create_resp.ref, the_tag)

        process_output = tsu.get_and_check_type(self.be_main_rh, dmsg.SHFwdOutput, timeout=5)

        if process_output == "Hello World\n":
            process_output = tsu.get_and_check_type(self.be_main_rh, dmsg.SHFwdOutput, timeout=5)
            self.assertEqual(process_output.data, "Doing some more\n")
        else:
            self.assertEqual(process_output.data, "Hello World\nDoing some more\n")

        tsu.get_and_check_type(self.gs_main_rh, dmsg.SHProcessExit)
        self.do_teardown()

    def test_process_fwdinput(self):
        self.do_bringup()

        target_puid = 17777
        the_tag = self.next_tag()
        self.shep_main_wh.send(
            dmsg.SHProcessCreate(
                tag=the_tag,
                exe=sys.executable,
                args=["shepherd/proc4.py"],
                p_uid=dfacts.GS_PUID,
                r_c_uid=dfacts.GS_INPUT_CUID,
                t_p_uid=target_puid,
            ).serialize()
        )

        process_create_resp = tsu.get_and_check_type(self.gs_main_rh, dmsg.SHProcessCreateResponse)

        self.assertEqual(process_create_resp.ref, the_tag)

        self.shep_main_wh.send(
            dmsg.SHFwdInput(
                tag=self.next_tag(),
                p_uid=dfacts.GS_PUID,
                t_p_uid=target_puid,
                r_c_uid=dfacts.GS_INPUT_CUID,
                input="Hi There\n",
                confirm=True,
            ).serialize()
        )

        tsu.get_and_check_type(self.gs_main_rh, dmsg.SHFwdInputErr)

        output = ""
        try:
            msg = tsu.get_and_check_type(self.be_main_rh, dmsg.SHFwdOutput)
            output += msg.data
        except TimeoutError:
            pass

        self.assertTrue((output in "Enter some text: Here is the response text.\nHi There\n") and (len(output) != 0))

        self.do_teardown()

    def test_sh_pool_new_destroy(self):
        self.do_bringup()

        test_muid = 123456
        self.shep_main_wh.send(
            dmsg.SHPoolCreate(
                tag=self.next_tag(),
                p_uid=dfacts.GS_PUID,
                r_c_uid=dfacts.GS_INPUT_CUID,
                size=self.DEFAULT_TEST_POOL_SIZE,
                m_uid=test_muid,
                name="single_shep_test",
            ).serialize()
        )

        res = tsu.get_and_check_type(self.gs_main_rh, dmsg.SHPoolCreateResponse)
        self.assertEqual(dmsg.SHPoolCreateResponse.Errors.SUCCESS, res.err)

        self.shep_main_wh.send(
            dmsg.SHPoolDestroy(
                tag=self.next_tag(), p_uid=dfacts.GS_PUID, r_c_uid=dfacts.GS_INPUT_CUID, m_uid=test_muid
            ).serialize()
        )

        res = tsu.get_and_check_type(self.gs_main_rh, dmsg.SHPoolDestroyResponse)
        self.assertEqual(dmsg.SHPoolDestroyResponse.Errors.SUCCESS, res.err)

        self.do_teardown()

    def test_sh_pool_invalid_destroy(self):
        self.do_bringup()
        test_muid = 123456
        self.shep_main_wh.send(
            dmsg.SHPoolDestroy(
                tag=self.next_tag(), p_uid=dfacts.GS_PUID, r_c_uid=dfacts.GS_INPUT_CUID, m_uid=test_muid
            ).serialize()
        )
        res = tsu.get_and_check_type(self.gs_main_rh, dmsg.SHPoolDestroyResponse)
        self.assertEqual(dmsg.SHPoolDestroyResponse.Errors.FAIL, res.err)

        self.do_teardown()

    def test_sh_pool_duplicate_id(self):
        self.do_bringup()
        test_muid = 123456

        self.shep_main_wh.send(
            dmsg.SHPoolCreate(
                tag=self.next_tag(),
                p_uid=dfacts.GS_PUID,
                r_c_uid=dfacts.GS_INPUT_CUID,
                size=self.DEFAULT_TEST_POOL_SIZE,
                m_uid=test_muid,
                name="single_shep_test",
            ).serialize()
        )
        res = tsu.get_and_check_type(self.gs_main_rh, dmsg.SHPoolCreateResponse)
        self.assertEqual(dmsg.SHPoolCreateResponse.Errors.SUCCESS, res.err)

        self.shep_main_wh.send(
            dmsg.SHPoolCreate(
                tag=self.next_tag(),
                p_uid=dfacts.GS_PUID,
                r_c_uid=dfacts.GS_INPUT_CUID,
                size=self.DEFAULT_TEST_POOL_SIZE,
                m_uid=test_muid,
                name="single_shep_test",
            ).serialize()
        )
        res = tsu.get_and_check_type(self.gs_main_rh, dmsg.SHPoolCreateResponse)
        self.assertEqual(dmsg.SHPoolCreateResponse.Errors.FAIL, res.err)

        # Destroy the pool
        self.shep_main_wh.send(
            dmsg.SHPoolDestroy(
                tag=self.next_tag(), p_uid=dfacts.GS_PUID, r_c_uid=dfacts.GS_INPUT_CUID, m_uid=test_muid
            ).serialize()
        )

        # Check we succeeded so we know if other tests fail after this
        res = tsu.get_and_check_type(self.gs_main_rh, dmsg.SHPoolDestroyResponse)
        self.assertEqual(dmsg.SHPoolDestroyResponse.Errors.SUCCESS, res.err)

        self.do_teardown()

    def test_sh_channel_create_destroy(self):
        self.do_bringup()
        test_muid = 1234
        test_cuid = 4567

        self._make_pool(
            self.next_tag(),
            dfacts.GS_PUID,
            dfacts.GS_INPUT_CUID,
            self.DEFAULT_TEST_POOL_SIZE,
            test_muid,
            "single_shep_test",
        )

        self.shep_main_wh.send(
            dmsg.SHChannelCreate(
                tag=self.next_tag(),
                p_uid=dfacts.GS_PUID,
                r_c_uid=dfacts.GS_INPUT_CUID,
                m_uid=test_muid,
                c_uid=test_cuid,
            ).serialize()
        )
        res = tsu.get_and_check_type(self.gs_main_rh, dmsg.SHChannelCreateResponse)
        self.assertEqual(dmsg.SHChannelCreateResponse.Errors.SUCCESS, res.err)

        self.shep_main_wh.send(
            dmsg.SHChannelDestroy(
                tag=self.next_tag(), p_uid=dfacts.GS_PUID, r_c_uid=dfacts.GS_INPUT_CUID, c_uid=test_cuid
            ).serialize()
        )
        res = tsu.get_and_check_type(self.gs_main_rh, dmsg.SHChannelDestroyResponse)
        self.assertEqual(dmsg.SHChannelDestroyResponse.Errors.SUCCESS, res.err)

        self._destroy_pool(self.next_tag(), dfacts.GS_PUID, dfacts.GS_INPUT_CUID, test_muid)

        self.do_teardown()

    def test_sh_channel_create_duplicate_uid(self):
        self.do_bringup()
        test_muid = 1234
        test_cuid = 4567

        self._make_pool(
            self.next_tag(),
            dfacts.GS_PUID,
            dfacts.GS_INPUT_CUID,
            self.DEFAULT_TEST_POOL_SIZE,
            test_muid,
            "single_shep_test",
        )

        # Make a channel
        self.shep_main_wh.send(
            dmsg.SHChannelCreate(
                tag=self.next_tag(),
                p_uid=dfacts.GS_PUID,
                r_c_uid=dfacts.GS_INPUT_CUID,
                m_uid=test_muid,
                c_uid=test_cuid,
            ).serialize()
        )
        res = tsu.get_and_check_type(self.gs_main_rh, dmsg.SHChannelCreateResponse)
        self.assertEqual(dmsg.SHChannelCreateResponse.Errors.SUCCESS, res.err)

        self.shep_main_wh.send(
            dmsg.SHChannelCreate(
                tag=self.next_tag(),
                p_uid=dfacts.GS_PUID,
                r_c_uid=dfacts.GS_INPUT_CUID,
                m_uid=test_muid,
                c_uid=test_cuid,
            ).serialize()
        )
        res = tsu.get_and_check_type(self.gs_main_rh, dmsg.SHChannelCreateResponse)
        self.assertEqual(dmsg.SHChannelCreateResponse.Errors.FAIL, res.err)

        self.shep_main_wh.send(
            dmsg.SHChannelDestroy(
                tag=self.next_tag(), p_uid=dfacts.GS_PUID, r_c_uid=dfacts.GS_INPUT_CUID, c_uid=test_cuid
            ).serialize()
        )
        res = tsu.get_and_check_type(self.gs_main_rh, dmsg.SHChannelDestroyResponse)
        self.assertEqual(dmsg.SHChannelDestroyResponse.Errors.SUCCESS, res.err)

        self._destroy_pool(self.next_tag(), dfacts.GS_PUID, dfacts.GS_INPUT_CUID, test_muid)

        self.do_teardown()

    def test_noprocess_fwdinput(self):
        self.do_bringup()
        target_puid = 177

        self.shep_main_wh.send(
            dmsg.SHFwdInput(
                tag=self.next_tag(),
                p_uid=dfacts.GS_PUID,
                confirm=True,
                t_p_uid=target_puid,
                r_c_uid=dfacts.GS_INPUT_CUID,
                input="Hi There\n",
            ).serialize()
        )

        tsu.get_and_check_type(self.gs_main_rh, dmsg.SHFwdInputErr)

        self.do_teardown()

    def test_process_errexit(self):
        self.do_bringup()
        the_tag = self.next_tag()
        target_puid = 177
        self.shep_main_wh.send(
            dmsg.SHProcessCreate(
                tag=the_tag,
                exe=sys.executable,
                args=["missing_prog.py"],
                p_uid=dfacts.GS_PUID,
                r_c_uid=dfacts.GS_INPUT_CUID,
                t_p_uid=target_puid,
            ).serialize()
        )

        process_create_resp = tsu.get_and_check_type(self.gs_main_rh, dmsg.SHProcessCreateResponse)

        self.assertEqual(process_create_resp.ref, the_tag)

        process_exit = tsu.get_and_check_type(self.gs_main_rh, dmsg.SHProcessExit)

        self.assertEqual(process_exit.exit_code, 2)

        tsu.get_and_check_type(self.be_main_rh, dmsg.SHFwdOutput)

        self.do_teardown()

    def test_dump_state(self):
        self.do_bringup()
        self.shep_main_wh.send(dmsg.SHDumpState(tag=self.next_tag()).serialize())
        self.do_teardown()

    def test_dump_state_to_file(self):
        self.do_bringup()
        self.shep_main_wh.send(dmsg.SHDumpState(tag=self.next_tag(), filename="shep_dump.log").serialize())
        self.do_teardown()

    def test_process_kill(self):
        self.do_bringup()
        target_puid = 177
        the_tag = self.next_tag()
        self.shep_main_wh.send(
            dmsg.SHProcessCreate(
                tag=the_tag,
                exe=sys.executable,
                args=["shepherd/proc2.py"],
                p_uid=dfacts.GS_PUID,
                r_c_uid=dfacts.GS_INPUT_CUID,
                t_p_uid=target_puid,
            ).serialize()
        )

        process_create_resp = tsu.get_and_check_type(self.gs_main_rh, dmsg.SHProcessCreateResponse)

        self.assertEqual(process_create_resp.ref, the_tag)

        process_output = tsu.get_and_check_type(self.be_main_rh, dmsg.SHFwdOutput, timeout=5)

        self.shep_main_wh.send(
            dmsg.SHProcessKill(
                tag=self.next_tag(),
                p_uid=dfacts.GS_PUID,
                t_p_uid=target_puid,
                r_c_uid=dfacts.GS_INPUT_CUID,
                sig=signal.SIGKILL,
            ).serialize()
        )

        if process_output.data == "Hello World\n":
            process_output = tsu.get_and_check_type(self.be_main_rh, dmsg.SHFwdOutput, timeout=5)
            self.assertEqual(process_output.data, "Doing some more\n")
        else:
            self.assertEqual(process_output.data, "Hello World\nDoing some more\n")

        msg = tsu.get_and_parse(self.gs_main_rh)
        self.assertTrue(isinstance(msg, dmsg.SHProcessExit) or isinstance(msg, dmsg.SHProcessKillResponse))
        self.do_teardown()

    def test_process_kill_wait(self):
        self.do_bringup()
        target_puid = 177
        the_tag = self.next_tag()
        self.shep_main_wh.send(
            dmsg.SHProcessCreate(
                tag=the_tag,
                exe=sys.executable,
                args=["shepherd/proc3.py"],
                p_uid=dfacts.GS_PUID,
                r_c_uid=dfacts.GS_INPUT_CUID,
                t_p_uid=target_puid,
            ).serialize()
        )

        process_create_resp = tsu.get_and_check_type(self.gs_main_rh, dmsg.SHProcessCreateResponse)

        self.assertEqual(process_create_resp.ref, the_tag)

        self.shep_main_wh.send(
            dmsg.SHProcessKill(
                tag=self.next_tag(),
                p_uid=dfacts.GS_PUID,
                t_p_uid=target_puid,
                r_c_uid=dfacts.GS_INPUT_CUID,
                sig=signal.SIGKILL,
            ).serialize()
        )

        msg = tsu.get_and_parse(self.gs_main_rh)
        self.assertTrue(isinstance(msg, dmsg.SHProcessExit) or isinstance(msg, dmsg.SHProcessKillResponse))
        self.do_teardown()

    def test_bringup_abnormal_termination(self):
        self.do_bringup()
        self.shep_main_wh.send(dmsg.AbnormalTermination(tag=self.next_tag()).serialize())
        self.do_abnormal_teardown()

    def test_bringup_bad_message(self):
        self.do_bringup()
        self.shep_main_wh.send("crap")
        self.do_abnormal_teardown()

    def test_process_create(self):
        self.do_bringup()
        the_tag = self.next_tag()
        target_puid = 177
        self.shep_main_wh.send(
            dmsg.SHProcessCreate(
                tag=the_tag,
                exe=sys.executable,
                args=["shepherd/proc1.py"],
                p_uid=dfacts.GS_PUID,
                r_c_uid=dfacts.GS_INPUT_CUID,
                t_p_uid=target_puid,
            ).serialize()
        )

        process_create_resp = tsu.get_and_check_type(self.gs_main_rh, dmsg.SHProcessCreateResponse)

        self.assertEqual(process_create_resp.ref, the_tag)
        self.shep_main_wh.send(dmsg.SHDumpState(tag=self.next_tag()).serialize())
        process_output = tsu.get_and_check_type(self.be_main_rh, dmsg.SHFwdOutput)
        self.assertEqual(process_output.data, "Hello World\n")
        tsu.get_and_check_type(self.gs_main_rh, dmsg.SHProcessExit, 5)

        self.do_teardown()

    def test_process_create_with_env(self):
        self.do_bringup()
        target_puid = 177
        the_tag = self.next_tag()
        self.shep_main_wh.send(
            dmsg.SHProcessCreate(
                tag=the_tag,
                exe=sys.executable,
                args=["shepherd/procenv1.py"],
                env={"new_env_var": "env_value"},
                p_uid=dfacts.GS_PUID,
                r_c_uid=dfacts.GS_INPUT_CUID,
                t_p_uid=target_puid,
            ).serialize()
        )

        process_create_resp = tsu.get_and_check_type(self.gs_main_rh, dmsg.SHProcessCreateResponse)
        self.assertEqual(process_create_resp.ref, the_tag)

        self.shep_main_wh.send(dmsg.SHDumpState(tag=0).serialize())

        process_output = tsu.get_and_check_type(self.be_main_rh, dmsg.SHFwdOutput)
        self.assertEqual(process_output.data, "env_value\n")

        process_done = tsu.get_and_check_type(self.gs_main_rh, dmsg.SHProcessExit, 5)
        self.assertEqual(process_done.exit_code, 0)

        self.do_teardown()

    def test_process_create_with_env2(self):
        self.do_bringup()
        target_puid = 177
        the_tag = self.next_tag()
        self.shep_main_wh.send(
            dmsg.SHProcessCreate(
                tag=the_tag,
                exe=sys.executable,
                args=["shepherd/procenv2.py"],
                env={"SHELL": "shell_value"},
                p_uid=dfacts.GS_PUID,
                r_c_uid=dfacts.GS_INPUT_CUID,
                t_p_uid=target_puid,
            ).serialize()
        )

        process_create_resp = tsu.get_and_check_type(self.gs_main_rh, dmsg.SHProcessCreateResponse)
        self.assertEqual(process_create_resp.ref, the_tag)

        self.shep_main_wh.send(dmsg.SHDumpState(tag=self.next_tag()).serialize())

        process_output = tsu.get_and_check_type(self.be_main_rh, dmsg.SHFwdOutput)
        self.assertEqual(process_output.data, "shell_value\n")

        process_done = tsu.get_and_check_type(self.gs_main_rh, dmsg.SHProcessExit, 5)
        self.assertEqual(process_done.exit_code, 0)

        self.do_teardown()

    def test_process_create_dup(self):
        self.do_bringup()

        tag1 = self.next_tag()
        test_puid = 177

        self.shep_main_wh.send(
            dmsg.SHProcessCreate(
                tag=tag1,
                exe=sys.executable,
                args=["shepherd/proc1.py"],
                p_uid=dfacts.GS_PUID,
                r_c_uid=dfacts.GS_INPUT_CUID,
                t_p_uid=test_puid,
            ).serialize()
        )

        tag2 = self.next_tag()
        self.shep_main_wh.send(
            dmsg.SHProcessCreate(
                tag=tag2,
                exe=sys.executable,
                args=["shepherd/proc1.py"],
                p_uid=dfacts.GS_PUID,
                r_c_uid=dfacts.GS_INPUT_CUID,
                t_p_uid=test_puid,
            ).serialize()
        )

        process_create_resp1 = tsu.get_and_check_type(self.gs_main_rh, dmsg.SHProcessCreateResponse)

        if process_create_resp1.ref == tag1:
            process_create_resp2 = tsu.get_and_check_type(self.gs_main_rh, dmsg.SHProcessCreateResponse)
            self.assertEqual(process_create_resp2.ref, tag2)
            self.assertEqual(process_create_resp2.err, dmsg.SHProcessCreateResponse.Errors.FAIL)
        else:
            self.assertEqual(process_create_resp1.ref, tag2)
            self.assertEqual(process_create_resp1.err, dmsg.SHProcessCreateResponse.Errors.FAIL)
            process_create_resp2 = tsu.get_and_check_type(self.gs_main_rh, dmsg.SHProcessCreateResponse)
            self.assertEqual(process_create_resp2.ref, tag1)

        self.do_teardown()


if __name__ == "__main__":
    unittest.main()
