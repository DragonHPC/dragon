#!/usr/bin/env python3

"""Test script for single node gs process messages"""
import signal

import copy
import inspect
import logging
import multiprocessing
import os
import unittest

from dragon import channels as dch
from dragon import managed_memory as dmm

from dragon.globalservices import server as dserver

from dragon.infrastructure import process_desc
from dragon.infrastructure import facts as dfacts
from dragon.dlogging import util as dlog
from dragon.infrastructure import messages as dmsg
from dragon.infrastructure import connection as dconn
from dragon.infrastructure import parameters as dparm
import support.util as tsu
from dragon.utils import B64

from dragon.infrastructure.node_desc import NodeDescriptor


def bringup_channels(gs_stdout, env_updates, logname=""):
    dlog.setup_logging(basename="gs_" + logname, level=logging.DEBUG, force=True)
    log = logging.getLogger("channels bringup test")
    log.info("start")

    # patch up the environment
    dparm.this_process = dparm.LaunchParameters.from_env(env_updates)

    the_ctx = dserver.GlobalContext()

    try:
        the_ctx.run_startup(mode=the_ctx.LaunchModes.TEST_STANDALONE_SINGLE, test_gs_stdout=gs_stdout)

        the_ctx.run_global_server(mode=the_ctx.LaunchModes.TEST_STANDALONE_SINGLE, test_gs_stdout=gs_stdout)

        the_ctx.run_teardown(test_gs_stdout=gs_stdout)

    except:
        log.exception("fatal")
        gs_stdout.send(dmsg.AbnormalTermination(tag=0).serialize())

    log.info("normal exit")
    logging.shutdown()


class SingleProcMsgChannels(unittest.TestCase):
    def setUp(self) -> None:
        self.gs_stdout_rh, self.gs_stdout_wh = multiprocessing.Pipe(duplex=False)
        self.some_parms = copy.copy(dparm.this_process)

        username = os.environ.get("USER", str(os.getuid()))
        self.pool_name = "mctest_" + username
        self.pool_size = 2**30
        self.pool_prealloc_blocks = None
        self.pool_uid = 17
        self.mpool = dmm.MemoryPool(self.pool_size, self.pool_name, self.pool_uid, self.pool_prealloc_blocks)
        self.mpool_ser = self.mpool.serialize()
        self.some_parms.inf_pd = B64.bytes_to_str(self.mpool_ser)

        self.gs_input_chan = dch.Channel(self.mpool, dfacts.GS_INPUT_CUID)
        self.gs_input_rh = dconn.Connection(inbound_initializer=self.gs_input_chan)
        self.gs_input_wh = dconn.Connection(outbound_initializer=self.gs_input_chan)
        self.some_parms.gs_cd = B64.bytes_to_str(self.gs_input_chan.serialize())

        self.bela_input_chan = dch.Channel(self.mpool, dfacts.BASE_BE_CUID)
        self.bela_input_rh = dconn.Connection(inbound_initializer=self.bela_input_chan)
        self.bela_input_wh = dconn.Connection(outbound_initializer=self.bela_input_chan)
        self.some_parms.local_be_cd = B64.bytes_to_str(self.bela_input_chan.serialize())

        self.shep_input_chan = dch.Channel(self.mpool, dfacts.BASE_SHEP_CUID)
        self.shep_input_rh = dconn.Connection(inbound_initializer=self.shep_input_chan)
        self.shep_input_wh = dconn.Connection(outbound_initializer=self.shep_input_chan)
        self.some_parms.local_shep_cd = B64.bytes_to_str(self.shep_input_chan.serialize())

        self.node_sdesc = NodeDescriptor.get_localservices_node_conf(is_primary=True).sdesc

        self.dut = None
        self.head_uid = None
        self.tag_cnt = 4

    def tearDown(self) -> None:
        # wrapped queue ends
        self.gs_stdout_rh.close()
        self.gs_stdout_wh.close()

        # connections
        self.gs_input_rh.close()
        self.gs_input_wh.close()

        self.bela_input_rh.close()
        self.bela_input_wh.close()

        self.shep_input_rh.close()
        self.shep_input_wh.close()

        # underlying channels
        self.gs_input_chan.destroy()
        self.bela_input_chan.destroy()
        self.shep_input_chan.destroy()
        self.mpool.destroy()

        if self.dut is not None:
            self.dut.join(timeout=1)

    def _start_dut(self, test_name=None):
        if test_name is None:
            test_name = self.__class__.__name__ + "_" + inspect.stack()[1][0].f_code.co_name

        the_env_wanted = self.some_parms.env()

        dut = multiprocessing.Process(
            target=bringup_channels,
            args=(self.gs_stdout_wh, the_env_wanted),
            kwargs={"logname": test_name},
            name="globalservices",
        )
        dut.start()

        tsu.get_and_check_type(self.shep_input_rh, dmsg.GSPingSH)

        self.gs_input_wh.send(dmsg.SHPingGS(tag=self._tag_inc(), node_sdesc=self.node_sdesc).serialize())

        tsu.get_and_check_type(self.bela_input_rh, dmsg.GSIsUp)

        return dut

    def _tag_inc(self):
        self.tag_cnt += 1
        return self.tag_cnt

    def _start_a_process(
        self, exe_name, the_args, the_name, the_tag, the_puid, the_rcuid, pmi=None, pmi_info=None, head_proc=False
    ):
        if pmi is not None:
            self.assertIsNotNone(pmi_info)

        create_msg = dmsg.GSProcessCreate(
            tag=the_tag,
            p_uid=the_puid,
            r_c_uid=the_rcuid,
            exe=exe_name,
            args=the_args,
            env={},
            user_name=the_name,
            pmi=pmi,
            _pmi_info=pmi_info,
            head_proc=head_proc,
        )

        self.gs_input_wh.send(create_msg.serialize())

        shep_msg = tsu.get_and_check_type(self.shep_input_rh, dmsg.SHProcessCreate)
        self.assertEqual(shep_msg.exe, create_msg.exe)
        self.assertEqual(shep_msg.args, create_msg.args)

        if pmi is not None:
            self.assertIsNotNone(shep_msg.pmi_info)
            self.assertEqual(shep_msg.pmi_info.lrank, pmi_info.lrank)
            self.assertEqual(shep_msg.pmi_info.ppn, pmi_info.ppn)
            self.assertEqual(shep_msg.pmi_info.nid, pmi_info.nid)

        shep_reply_msg = dmsg.SHProcessCreateResponse(
            tag=self._tag_inc(), ref=shep_msg.tag, err=dmsg.SHProcessCreateResponse.Errors.SUCCESS
        )

        self.gs_input_wh.send(shep_reply_msg.serialize())

        create_reply_msg = tsu.get_and_check_type(self.bela_input_rh, dmsg.GSProcessCreateResponse)
        self.assertEqual(create_reply_msg.ref, create_msg.tag, "tag-ref mismatch")
        self.assertEqual(create_reply_msg.err, dmsg.GSProcessCreateResponse.Errors.SUCCESS)
        self.assertEqual(shep_msg.t_p_uid, create_reply_msg.desc.p_uid)

        return create_msg, create_reply_msg

    def _teardown_dut(self):
        self.gs_input_wh.send(dmsg.GSTeardown(tag=self._tag_inc()).serialize())

        tsu.get_and_check_type(self.gs_stdout_rh, dmsg.GSHalted)

    def _bringup_head_and_dut(self):
        test_name = self.__class__.__name__ + "_" + inspect.stack()[1][0].f_code.co_name

        self._start_dut(test_name)
        test_exe = "head"
        test_args = ["foo"]
        test_name = "first one"
        create_msg, create_reply_msg = self._start_a_process(
            exe_name=test_exe,
            the_args=test_args,
            the_name=test_name,
            the_tag=self._tag_inc(),
            the_puid=dfacts.LAUNCHER_PUID,
            the_rcuid=dfacts.BASE_BE_CUID,
            head_proc=True,
        )

        self.assertEqual(create_reply_msg.desc.state, process_desc.ProcessDescriptor.State.ACTIVE)
        self.assertEqual(create_msg.user_name, create_reply_msg.desc.name)
        self.head_uid = create_reply_msg.desc.p_uid
        self.head_name = test_name

    def _teardown_head_and_dut(self):
        death_msg = dmsg.SHProcessExit(tag=self._tag_inc(), p_uid=self.head_uid)

        self.gs_input_wh.send(death_msg.serialize())

        tsu.get_and_check_type(self.bela_input_rh, dmsg.GSHeadExit)
        self._teardown_dut()

    def _make_head_fail(self):
        test_exe = "dummy"
        test_args = ["foo", "bar"]
        test_name = "first one"

        create_msg = dmsg.GSProcessCreate(
            tag=self._tag_inc(),
            p_uid=dfacts.LAUNCHER_PUID,
            r_c_uid=dfacts.BASE_BE_CUID,
            exe=test_exe,
            args=test_args,
            env={},
            user_name=test_name,
            head_proc=True,
        )

        self.gs_input_wh.send(create_msg.serialize())

        shep_msg = tsu.get_and_check_type(self.shep_input_rh, dmsg.SHProcessCreate)
        self.assertEqual(shep_msg.exe, create_msg.exe)
        self.assertEqual(shep_msg.args, create_msg.args)

        shep_reply_msg = dmsg.SHProcessCreateResponse(
            tag=self._tag_inc(), ref=shep_msg.tag, err=dmsg.SHProcessCreateResponse.Errors.FAIL, err_info="whoa"
        )

        self.gs_input_wh.send(shep_reply_msg.serialize())

        create_reply_msg = tsu.get_and_check_type(self.bela_input_rh, dmsg.GSProcessCreateResponse)
        self.assertEqual(create_reply_msg.ref, create_msg.tag, "tag-ref mismatch")
        self.assertEqual(create_reply_msg.err, dmsg.GSProcessCreateResponse.Errors.FAIL)

    def _make_a_pool(self):
        the_tag = self._tag_inc()

        pcm = dmsg.GSPoolCreate(tag=the_tag, p_uid=dfacts.LAUNCHER_PUID, r_c_uid=dfacts.BASE_BE_CUID, size=2**30)
        self.gs_input_wh.send(pcm.serialize())

        shep_msg = tsu.get_and_check_type(self.shep_input_rh, dmsg.SHPoolCreate)

        dummy_sdesc = B64.bytes_to_str("xxxTESTDUMMYxxx".encode())
        shep_response = dmsg.SHPoolCreateResponse(
            tag=self._tag_inc(), ref=shep_msg.tag, err=dmsg.SHPoolCreateResponse.Errors.SUCCESS, desc=dummy_sdesc
        )
        self.gs_input_wh.send(shep_response.serialize())

        gs_response = tsu.get_and_check_type(self.bela_input_rh, dmsg.GSPoolCreateResponse)

        self.assertEqual(gs_response.err, dmsg.GSPoolCreateResponse.Errors.SUCCESS)
        self.assertEqual(gs_response.ref, the_tag)
        return gs_response.desc.m_uid

    def _destroy_a_pool(self, the_muid):
        the_tag = self._tag_inc()
        pdm = dmsg.GSPoolDestroy(tag=the_tag, p_uid=dfacts.LAUNCHER_PUID, r_c_uid=dfacts.BASE_BE_CUID, m_uid=the_muid)
        self.gs_input_wh.send(pdm.serialize())

        shep_msg = tsu.get_and_check_type(self.shep_input_rh, dmsg.SHPoolDestroy)

        shep_response = dmsg.SHPoolDestroyResponse(
            tag=self._tag_inc(), ref=shep_msg.tag, err=dmsg.SHPoolDestroyResponse.Errors.SUCCESS
        )

        self.gs_input_wh.send(shep_response.serialize())

        gs_response = tsu.get_and_check_type(self.bela_input_rh, dmsg.GSPoolDestroyResponse)
        self.assertEqual(gs_response.err, dmsg.GSPoolDestroyResponse.Errors.SUCCESS)
        self.assertEqual(gs_response.ref, the_tag)

    def _make_a_channel(self, the_muid):
        the_tag = self._tag_inc()

        pcm = dmsg.GSChannelCreate(tag=the_tag, p_uid=dfacts.LAUNCHER_PUID, m_uid=the_muid, r_c_uid=dfacts.BASE_BE_CUID)

        self.gs_input_wh.send(pcm.serialize())

        shep_msg = tsu.get_and_check_type(self.shep_input_rh, dmsg.SHChannelCreate)

        dummy_sdesc = B64.bytes_to_str("xxxTESTDUMMYxxx".encode())
        shep_response = dmsg.SHChannelCreateResponse(
            tag=self._tag_inc(), ref=shep_msg.tag, err=dmsg.SHChannelCreateResponse.Errors.SUCCESS, desc=dummy_sdesc
        )
        self.gs_input_wh.send(shep_response.serialize())

        gs_response = tsu.get_and_check_type(self.bela_input_rh, dmsg.GSChannelCreateResponse)

        self.assertEqual(gs_response.err, dmsg.GSChannelCreateResponse.Errors.SUCCESS)
        self.assertEqual(gs_response.ref, the_tag)

        return gs_response.desc.c_uid

    def _destroy_a_channel(self, the_cuid):
        the_tag = self._tag_inc()

        cdm = dmsg.GSChannelDestroy(
            tag=the_tag, p_uid=dfacts.LAUNCHER_PUID, r_c_uid=dfacts.BASE_BE_CUID, c_uid=the_cuid
        )
        self.gs_input_wh.send(cdm.serialize())

        shep_msg = tsu.get_and_check_type(self.shep_input_rh, dmsg.SHChannelDestroy)

        shep_response = dmsg.SHChannelDestroyResponse(
            tag=self._tag_inc(), ref=shep_msg.tag, err=dmsg.SHChannelDestroyResponse.Errors.SUCCESS
        )
        self.gs_input_wh.send(shep_response.serialize())

        gs_response = tsu.get_and_check_type(self.bela_input_rh, dmsg.GSChannelDestroyResponse)
        self.assertEqual(gs_response.err, dmsg.GSChannelDestroyResponse.Errors.SUCCESS)
        self.assertEqual(gs_response.ref, the_tag)

    def test_proc_updown(self):
        self._bringup_head_and_dut()
        self._teardown_head_and_dut()

    def test_null_bringup(self):
        self._start_dut()
        self._teardown_dut()

    # regression for PE-33554
    def test_make_head_proc_fail_to_teardown(self):
        self._start_dut()
        self._make_head_fail()
        self._teardown_dut()

    def test_make_head_proc_list(self):
        self._bringup_head_and_dut()
        list_msg = dmsg.GSProcessList(tag=self._tag_inc(), p_uid=dfacts.LAUNCHER_PUID, r_c_uid=dfacts.BASE_BE_CUID)
        self.gs_input_wh.send(list_msg.serialize())

        list_reply_msg = tsu.get_and_check_type(self.bela_input_rh, dmsg.GSProcessListResponse)
        self.assertEqual(self.head_uid, list_reply_msg.plist[0])

        self._teardown_head_and_dut()

    def test_make_head_proc_query(self):
        self._bringup_head_and_dut()

        query_msg = dmsg.GSProcessQuery(
            tag=self._tag_inc(), p_uid=dfacts.LAUNCHER_PUID, t_p_uid=self.head_uid, r_c_uid=dfacts.BASE_BE_CUID
        )
        query_msg_byname = dmsg.GSProcessQuery(
            tag=self._tag_inc(), user_name=self.head_name, p_uid=dfacts.LAUNCHER_PUID, r_c_uid=dfacts.BASE_BE_CUID
        )
        query_msg_unknown = dmsg.GSProcessQuery(
            tag=self._tag_inc(), user_name="absent", p_uid=dfacts.LAUNCHER_PUID, r_c_uid=dfacts.BASE_BE_CUID
        )

        self.gs_input_wh.send(query_msg.serialize())
        query_reply_msg = tsu.get_and_check_type(self.bela_input_rh, dmsg.GSProcessQueryResponse)
        self.assertEqual(query_reply_msg.ref, query_msg.tag)
        self.assertEqual(query_reply_msg.err, dmsg.GSProcessQueryResponse.Errors.SUCCESS)
        self.assertEqual(query_reply_msg.desc.name, self.head_name)

        self.gs_input_wh.send(query_msg_byname.serialize())
        query_byname_reply_message = tsu.get_and_check_type(self.bela_input_rh, dmsg.GSProcessQueryResponse)
        self.assertEqual(query_byname_reply_message.ref, query_msg_byname.tag)
        self.assertEqual(query_byname_reply_message.err, dmsg.GSProcessQueryResponse.Errors.SUCCESS)
        self.assertEqual(query_byname_reply_message.desc.p_uid, self.head_uid)

        self.gs_input_wh.send(query_msg_unknown.serialize())
        query_unknown_reply = tsu.get_and_check_type(self.bela_input_rh, dmsg.GSProcessQueryResponse)
        self.assertEqual(query_unknown_reply.ref, query_msg_unknown.tag)
        self.assertEqual(query_unknown_reply.err, dmsg.GSProcessQueryResponse.Errors.UNKNOWN)

        self._teardown_head_and_dut()

    def test_up_down_with_proc(self):
        self._start_dut()

        test_exe = "dummy"
        test_args = ["foo", "bar"]
        test_name = "first one"

        create_msg, create_reply_msg = self._start_a_process(
            exe_name=test_exe,
            the_args=test_args,
            the_name=test_name,
            the_tag=self._tag_inc(),
            the_puid=dfacts.LAUNCHER_PUID,
            the_rcuid=dfacts.BASE_BE_CUID,
            head_proc=True,
        )

        death_msg = dmsg.SHProcessExit(tag=self._tag_inc(), p_uid=create_reply_msg.desc.p_uid)

        self.gs_input_wh.send(death_msg.serialize())

        tsu.get_and_check_type(self.bela_input_rh, dmsg.GSHeadExit)

        self._teardown_dut()

    def test_up_down_with_pmi_proc(self):
        self._start_dut()

        test_exe = "dummy"
        test_args = ["foo", "bar"]
        test_name = "first one"

        create_msg, create_reply_msg = self._start_a_process(
            exe_name=test_exe,
            the_args=test_args,
            the_name=test_name,
            the_tag=self._tag_inc(),
            the_puid=dfacts.LAUNCHER_PUID,
            the_rcuid=dfacts.BASE_BE_CUID,
            pmi=dfacts.PMIBackend.CRAY,
            pmi_info=dmsg.PMIProcessInfo(lrank=0, ppn=1, nid=1, pid_base=1),
            head_proc=True,
        )

        death_msg = dmsg.SHProcessExit(tag=self._tag_inc(), p_uid=create_reply_msg.desc.p_uid)

        self.gs_input_wh.send(death_msg.serialize())

        tsu.get_and_check_type(self.bela_input_rh, dmsg.GSHeadExit)

        self._teardown_dut()

    def test_controlled_kill(self):
        self._start_dut()

        test_exe = "dummy"
        test_args = ["foo", "bar"]
        test_name = "first one"

        create_msg, create_reply_msg = self._start_a_process(
            exe_name=test_exe,
            the_args=test_args,
            the_name=test_name,
            the_tag=self._tag_inc(),
            the_puid=dfacts.LAUNCHER_PUID,
            the_rcuid=dfacts.BASE_BE_CUID,
            head_proc=True,
        )

        kill_msg = dmsg.GSProcessKill(
            tag=self._tag_inc(),
            p_uid=dfacts.LAUNCHER_PUID,
            r_c_uid=dfacts.BASE_BE_CUID,
            t_p_uid=create_reply_msg.desc.p_uid,
            sig=signal.SIGKILL,
        )

        self.gs_input_wh.send(kill_msg.serialize())

        sh_kill_msg = tsu.get_and_check_type(self.shep_input_rh, dmsg.SHProcessKill)
        self.assertEqual(sh_kill_msg.t_p_uid, kill_msg.t_p_uid)

        sh_kill_reply = dmsg.SHProcessKillResponse(
            tag=17, ref=sh_kill_msg.tag, err=dmsg.SHProcessKillResponse.Errors.SUCCESS
        )

        self.gs_input_wh.send(sh_kill_reply.serialize())

        kill_response = tsu.get_and_check_type(self.bela_input_rh, dmsg.GSProcessKillResponse)
        self.assertEqual(kill_response.err, dmsg.GSProcessKillResponse.Errors.SUCCESS)

        exit_msg = dmsg.SHProcessExit(tag=self._tag_inc(), p_uid=sh_kill_msg.t_p_uid)

        self.gs_input_wh.send(exit_msg.serialize())

        tsu.get_and_check_type(self.bela_input_rh, dmsg.GSHeadExit)

        self._teardown_dut()

    def test_up_down_twice_with_proc(self):
        self._start_dut()

        test_exe = "dummy"
        test_args = ["foo", "bar"]
        test_name = "first one"

        create_msg, create_reply_msg = self._start_a_process(
            exe_name=test_exe,
            the_args=test_args,
            the_name=test_name,
            the_tag=self._tag_inc(),
            the_puid=dfacts.LAUNCHER_PUID,
            the_rcuid=dfacts.BASE_BE_CUID,
            head_proc=True,
        )

        death_msg = dmsg.SHProcessExit(tag=self._tag_inc(), p_uid=create_reply_msg.desc.p_uid)

        self.gs_input_wh.send(death_msg.serialize())

        tsu.get_and_check_type(self.bela_input_rh, dmsg.GSHeadExit)

        test_args = ["foo2", "bar2"]
        test_name = "second one"

        create_msg2, create_reply_msg2 = self._start_a_process(
            exe_name=test_exe,
            the_args=test_args,
            the_name=test_name,
            the_tag=self._tag_inc(),
            the_puid=dfacts.LAUNCHER_PUID,
            the_rcuid=dfacts.BASE_BE_CUID,
            head_proc=True,
        )

        death_msg2 = dmsg.SHProcessExit(tag=self._tag_inc(), p_uid=create_reply_msg2.desc.p_uid)

        self.gs_input_wh.send(death_msg2.serialize())

        tsu.get_and_check_type(self.bela_input_rh, dmsg.GSHeadExit)

        self._teardown_dut()

    def test_pool_create(self):
        self._bringup_head_and_dut()
        self._make_a_pool()
        self._teardown_head_and_dut()

    def test_pool_create_destroy(self):
        self._bringup_head_and_dut()

        the_muid = self._make_a_pool()
        self._destroy_a_pool(the_muid)

        self._teardown_head_and_dut()

    def test_channel_create(self):
        self._bringup_head_and_dut()

        the_muid = self._make_a_pool()
        self._make_a_channel(the_muid)
        self._teardown_head_and_dut()

    def test_channel_destroy(self):
        self._bringup_head_and_dut()

        the_muid = self._make_a_pool()
        the_cuid = self._make_a_channel(the_muid)
        self._destroy_a_channel(the_cuid)
        self._teardown_head_and_dut()

    def test_channel_pool_list(self):
        self._bringup_head_and_dut()

        the_muid = self._make_a_pool()
        the_cuid = self._make_a_channel(the_muid)

        the_tag = self._tag_inc()
        pool_list_msg = dmsg.GSPoolList(tag=the_tag, p_uid=dfacts.LAUNCHER_PUID, r_c_uid=dfacts.BASE_BE_CUID)
        self.gs_input_wh.send(pool_list_msg.serialize())
        gs_response = tsu.get_and_check_type(self.bela_input_rh, dmsg.GSPoolListResponse)
        self.assertEqual(gs_response.err, dmsg.GSPoolListResponse.Errors.SUCCESS)
        self.assertEqual(gs_response.mlist, [the_muid])

        the_tag = self._tag_inc()
        channel_list_msg = dmsg.GSChannelList(tag=the_tag, p_uid=dfacts.LAUNCHER_PUID, r_c_uid=dfacts.BASE_BE_CUID)
        self.gs_input_wh.send(channel_list_msg.serialize())
        gs_response = tsu.get_and_check_type(self.bela_input_rh, dmsg.GSChannelListResponse)
        self.assertEqual(gs_response.err, dmsg.GSChannelListResponse.Errors.SUCCESS)
        self.assertEqual(gs_response.clist, [the_cuid])

        self._teardown_head_and_dut()

    def test_pool_query(self):
        self._bringup_head_and_dut()

        the_muid = self._make_a_pool()

        the_tag = self._tag_inc()
        pool_query_msg = dmsg.GSPoolQuery(
            m_uid=the_muid, tag=the_tag, p_uid=dfacts.LAUNCHER_PUID, r_c_uid=dfacts.BASE_BE_CUID
        )
        self.gs_input_wh.send(pool_query_msg.serialize())
        gs_response = tsu.get_and_check_type(self.bela_input_rh, dmsg.GSPoolQueryResponse)

        self.assertEqual(the_muid, gs_response.desc.m_uid)

        self._teardown_head_and_dut()

    def test_channel_query(self):
        self._bringup_head_and_dut()

        the_muid = self._make_a_pool()
        the_cuid = self._make_a_channel(the_muid)

        the_tag = self._tag_inc()
        channel_query_msg = dmsg.GSChannelQuery(
            c_uid=the_cuid, tag=the_tag, p_uid=dfacts.LAUNCHER_PUID, r_c_uid=dfacts.BASE_BE_CUID
        )
        self.gs_input_wh.send(channel_query_msg.serialize())
        gs_response = tsu.get_and_check_type(self.bela_input_rh, dmsg.GSChannelQueryResponse)

        self.assertEqual(the_cuid, gs_response.desc.c_uid)
        self._teardown_head_and_dut()


if __name__ == "__main__":
    unittest.main()
