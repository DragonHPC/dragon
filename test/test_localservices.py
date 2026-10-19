#!/usr/bin/env python3
"""Test script for single node shepherd message handling"""

import inspect
import logging
import multiprocessing as mp
import os
import queue
import signal
import sys
import unittest
from unittest import mock
from types import SimpleNamespace
from collections import deque

import dragon.channels as dch
import dragon.managed_memory as dmm
import dragon.utils as du
import shim_dragon_paths
import support.util as tsu

import dragon.dlogging.util as dlog
import dragon.infrastructure.connection as dconn
import dragon.infrastructure.facts as dfacts
import dragon.infrastructure.messages as dmsg
import dragon.infrastructure.parameters as parms
import dragon.localservices.local_svc as dsls
from dragon.infrastructure.node_desc import NodeDescriptor
from dragon.infrastructure.queue import InfraQueue

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


class MultiNodeStartupOrder(unittest.TestCase):
    TA_PING_TIMEOUT_SECONDS = dfacts.TA_PING_TIMEOUT
    MOCK_CLOCK_START_SECONDS = 100
    EARLY_GS_PING_DELAY_SECONDS = 3
    REMAINING_TA_PING_TIMEOUT_SECONDS = TA_PING_TIMEOUT_SECONDS - EARLY_GS_PING_DELAY_SECONDS

    def test_ta_ping_timeout_reports_abnormal_termination(self):
        self._run_multinode_startup(timeout_case="initial")

    def test_ta_ping_timeout_after_early_gs_ping_reports_abnormal_termination(self):
        self._run_multinode_startup(timeout_case="after_gs_ping")

    def test_primary_gs_ping_timeout_reports_abnormal_termination(self):
        self._run_multinode_startup(timeout_case="gs", is_primary=True)

    def test_remote_gs_ping_timeout_reports_abnormal_termination(self):
        self._run_multinode_startup(timeout_case="gs")

    def test_transport_agent_launch_exception_reports_abnormal_termination(self):
        self._run_multinode_startup(exception_case="ta_launch")

    def test_unexpected_ta_ping_reports_abnormal_termination(self):
        self._run_multinode_startup(message_case="unexpected_ta_ping", is_primary=True)

    def test_primary_multinode_startup_succeeds_without_timeouts(self):
        self._run_multinode_startup(ta_ping_first=True, is_primary=True)

    def test_multinode_non_primary_gs_ping_before_ta_ping(self):
        self._run_multinode_startup(ta_ping_first=False)

    def test_multinode_non_primary_ta_ping_before_gs_ping(self):
        self._run_multinode_startup(ta_ping_first=True)

    def _run_multinode_startup(
        self, ta_ping_first=False, timeout_case=None, is_primary=False, exception_case=None, message_case=None
    ):
        class FakeTA:
            def __init__(self):
                self.communicate = mock.Mock(return_value=(None, b"TA stderr"))

        class FakeGS:
            def __init__(self):
                self.stdin = mock.Mock()
                self.communicate = mock.Mock(return_value=(None, b"GS stderr"))

        node_idx_msg = dmsg.BENodeIdxLS(
            tag=1,
            node_idx=0 if is_primary else 1,
            host_name="node-1",
            ip_addrs=["127.0.0.1"],
            primary=is_primary,
            net_conf_key="0" if is_primary else "1",
        )
        la_channels_info = dmsg.LAChannelsInfo(
            tag=2,
            nodes_desc={},
            gs_qd="gs-desc",
            num_gw_channels=0,
            fe_ext_ip_addr="127.0.0.1",
        )
        gs_ping = dmsg.GSPingLS(tag=3)
        ta_ping = dmsg.TAPingLS(tag=4)
        if exception_case == "ta_launch":
            startup_pings = []
        elif message_case == "unexpected_ta_ping":
            startup_pings = [gs_ping]
        elif timeout_case == "initial":
            startup_pings = [queue.Empty]
        elif timeout_case == "after_gs_ping":
            startup_pings = [gs_ping, queue.Empty]
        elif timeout_case == "gs":
            startup_pings = [ta_ping, queue.Empty]
        else:
            startup_pings = [ta_ping, gs_ping] if ta_ping_first else [gs_ping, ta_ping]
        behalted = dmsg.BEHalted(tag=5)

        ls_stdin = mock.Mock()
        ls_stdin.recv.side_effect = [node_idx_msg.serialize(), behalted.serialize()]
        ls_stdout = mock.Mock()
        ls_input = mock.Mock()
        ls_input.get.side_effect = [dmsg.BEPingLS(tag=6), la_channels_info, *startup_pings]
        la_input = mock.Mock()
        ta_input = mock.Mock()
        ta = FakeTA()
        gs = FakeGS()
        gs_input = mock.Mock()
        server = mock.Mock()
        process_state = mock.Mock()
        process_state.env.return_value = {}
        process_state.local_ls_qd = "ls-desc"
        process_state.local_be_qd = "be-desc"
        process_state.default_pd = "default-pool-desc"
        process_state.inf_pd = "infrastructure-pool-desc"
        process_state.gs_qd = "primary-gs-desc"

        local_server = mock.Mock(return_value=server)
        has_early_gs_ping = timeout_case == "after_gs_ping" or (timeout_case is None and not ta_ping_first)
        monotonic_values = [self.MOCK_CLOCK_START_SECONDS]
        if has_early_gs_ping:
            monotonic_values.append(self.MOCK_CLOCK_START_SECONDS + self.EARLY_GS_PING_DELAY_SECONDS)
        gs_stdin_send = mock.Mock()
        start_ta = mock.Mock(return_value=ta)
        if exception_case == "ta_launch":
            start_ta.side_effect = RuntimeError("mock TA launch failure")
        with mock.patch.object(dsls, "setup_BE_logging"), mock.patch.object(dsls, "set_procname"), mock.patch.object(
            dsls.os, "setpgid"
        ), mock.patch.object(dsls, "get_ls_msg_queue", return_value=(ls_stdin, ls_stdout)), mock.patch.object(
            dsls,
            "mk_inf_resources",
            return_value=({}, {}, ls_input, la_input, "ta-desc", ta_input, None),
        ), mock.patch.object(
            dsls.NodeDescriptor,
            "get_localservices_node_conf",
            return_value=SimpleNamespace(sdesc="node-desc"),
        ), mock.patch.object(
            dsls.dparms, "this_process", process_state
        ), mock.patch.object(
            dsls.dch, "register_gateways_from_env"
        ), mock.patch.object(
            dsls, "start_transport_agent", start_ta
        ), mock.patch.object(
            dsls, "maybe_start_gs", return_value=(gs if is_primary else None, gs_input)
        ), mock.patch.object(
            dsls.dutil, "NewlineStreamWrapper", return_value=gs_stdin_send
        ), mock.patch.object(
            dsls, "PopenProps", FakeTA
        ), mock.patch.object(
            dsls.InfraQueue, "attach", return_value=gs_input
        ), mock.patch.object(
            dsls, "LocalServer", local_server
        ), mock.patch.object(
            dsls.time,
            "monotonic",
            side_effect=monotonic_values,
        ), mock.patch.dict(
            os.environ, {}
        ):
            if timeout_case or exception_case or message_case:
                with self.assertRaisesRegex(RuntimeError, "startup fatal error"):
                    dsls.multinode(gs_env={})
            else:
                dsls.multinode(gs_env={})

        if exception_case == "ta_launch":
            expected_ping_calls = [mock.call(), mock.call()]
        elif message_case == "unexpected_ta_ping":
            expected_ping_calls = [
                mock.call(),
                mock.call(),
                mock.call(timeout=self.TA_PING_TIMEOUT_SECONDS),
            ]
        elif timeout_case == "initial":
            expected_ping_calls = [
                mock.call(),
                mock.call(),
                mock.call(timeout=self.TA_PING_TIMEOUT_SECONDS),
            ]
        elif timeout_case == "after_gs_ping":
            expected_ping_calls = [
                mock.call(),
                mock.call(),
                mock.call(timeout=self.TA_PING_TIMEOUT_SECONDS),
                mock.call(timeout=self.REMAINING_TA_PING_TIMEOUT_SECONDS),
            ]
        elif timeout_case == "gs":
            gs_timeout = dfacts.LOCAL_LAUNCH_TIMEOUT if is_primary else dfacts.REMOTE_LAUNCH_TIMEOUT
            expected_ping_calls = [
                mock.call(),
                mock.call(),
                mock.call(timeout=self.TA_PING_TIMEOUT_SECONDS),
                mock.call(timeout=gs_timeout),
            ]
        else:
            expected_final_ping_timeout = (
                (dfacts.LOCAL_LAUNCH_TIMEOUT if is_primary else dfacts.REMOTE_LAUNCH_TIMEOUT)
                if ta_ping_first
                else self.REMAINING_TA_PING_TIMEOUT_SECONDS
            )
            expected_ping_calls = [
                mock.call(),
                mock.call(),
                mock.call(timeout=self.TA_PING_TIMEOUT_SECONDS),
                mock.call(timeout=expected_final_ping_timeout),
            ]
        self.assertEqual(
            ls_input.get.call_args_list,
            expected_ping_calls,
        )
        if exception_case == "ta_launch":
            sent_to_backend = [call.args[0] for call in la_input.put.call_args_list]
            self.assertIsInstance(sent_to_backend[0], dmsg.LSChannelsUp)
            self.assertIsInstance(sent_to_backend[1], dmsg.AbnormalTermination)
            self.assertEqual(sent_to_backend[1].err_info, "transport agent launch failed")
            start_ta.assert_called_once()
            local_server.clean_pools.assert_called_once_with({}, mock.ANY)
            server.run.assert_not_called()
            return

        if message_case == "unexpected_ta_ping":
            sent_to_backend = [call.args[0] for call in la_input.put.call_args_list]
            self.assertIsInstance(sent_to_backend[0], dmsg.LSChannelsUp)
            self.assertIsInstance(sent_to_backend[1], dmsg.AbnormalTermination)
            self.assertEqual(
                sent_to_backend[1].err_info,
                f"ls expected TAPingLS, received {type(gs_ping)}",
            )
            local_server.clean_pools.assert_called_once_with({}, mock.ANY)
            server.run.assert_not_called()
            return

        if timeout_case:
            sent_to_backend = [call.args[0] for call in la_input.put.call_args_list]
            self.assertIsInstance(sent_to_backend[0], dmsg.LSChannelsUp)
            abnormal_termination_idx = 2 if timeout_case == "gs" else 1
            self.assertIsInstance(sent_to_backend[abnormal_termination_idx], dmsg.AbnormalTermination)
            expected_error = "Unable to bring up Dragon transport agent: TA stderr"
            if timeout_case == "gs":
                expected_error = "Unable to bring up Dragon global services: GS stderr"
                if not is_primary:
                    expected_error = (
                        "Unable to bring up Dragon global services: timed out waiting for LS bringup message "
                        f"after {dfacts.REMOTE_LAUNCH_TIMEOUT} seconds"
                    )
            self.assertEqual(
                sent_to_backend[abnormal_termination_idx].err_info,
                expected_error,
            )
            if timeout_case == "gs":
                if is_primary:
                    gs.communicate.assert_called_once_with()
                else:
                    gs.communicate.assert_not_called()
            else:
                ta.communicate.assert_called_once_with()
            local_server.clean_pools.assert_called_once_with({}, mock.ANY)
            server.run.assert_not_called()
            return

        sent_to_backend = [call.args[0] for call in la_input.put.call_args_list]
        self.assertIsInstance(sent_to_backend[0], dmsg.LSChannelsUp)
        self.assertIsInstance(sent_to_backend[1], dmsg.TAUp)
        self.assertEqual(sent_to_backend[1].idx, 0 if is_primary else 1)
        self.assertEqual(ta_input.put.call_args.args[0], la_channels_info)
        self.assertIsInstance(gs_input.put.call_args.args[0], dmsg.LSPingGS)
        self.assertEqual(gs_input.put.call_args.args[0].idx, 0 if is_primary else 1)
        server.run.assert_called_once_with(
            ls_in=ls_input,
            gs_in=gs_input,
            be_in=la_input,
            is_primary=is_primary,
            ta_in=ta_input,
            gw_channels=[],
        )
        gs.communicate.assert_not_called()
        server.cleanup.assert_called_once_with()
        ls_stdin.close.assert_called_once_with()
        ls_stdout.close.assert_called_once_with()


class SingleLS(unittest.TestCase):
    DEFAULT_TEST_POOL_SIZE = 2**20

    def setUp(self) -> None:
        self.test_pool = dmm.MemoryPool(1024**3, "sheptest" + str(os.getpid()), 42)
        self.shep_stdin = dch.Channel(self.test_pool, 142)
        self.shep_stdin_rh = dconn.Connection(inbound_initializer=self.shep_stdin)
        self.shep_stdin_wh = dconn.Connection(outbound_initializer=self.shep_stdin)
        self.shep_stdout = dch.Channel(self.test_pool, 143)
        self.shep_stdout_rh = dconn.Connection(inbound_initializer=self.shep_stdout)
        self.shep_stdout_wh = dconn.Connection(outbound_initializer=self.shep_stdout)
        self.mode = dfacts.SINGLE_MODE
        self.tag = 0

    def tearDown(self) -> None:
        self.shep_stdout_rh.close()
        self.shep_stdin_wh.close()
        self.shep_stdin.destroy()
        self.shep_stdout.destroy()
        self.test_pool.destroy()

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

        self.shep_stdin_wh.send(dmsg.BENodeIdxLS(tag=self.next_tag(), node_idx=0, net_conf_key="0").serialize())

        msg = tsu.get_and_check_type(self.shep_stdout_rh, dmsg.LSPingBE)

        if ATTACH_POOLS:
            default_pd = du.B64.str_to_bytes(msg.default_pd)
            self.def_pool = dmm.MemoryPool.attach(default_pd)

            inf_pd = du.B64.str_to_bytes(msg.inf_pd)
            self.inf_pool = dmm.MemoryPool.attach(inf_pd)

        # ls_qd, be_qd, gs_qd are InfraQueue descriptors
        self.ls_main_wh = InfraQueue.attach(msg.ls_qd)
        self.be_main_rh = InfraQueue.attach(msg.be_qd)
        self.gs_main_rh = InfraQueue.attach(msg.gs_qd)

        self.ls_main_wh.put(dmsg.BEPingLS(tag=0))

        tsu.get_and_check_type(self.be_main_rh, dmsg.LSChannelsUp)

        self.ls_main_wh.put(dmsg.GSPingLS(tag=0))

        tsu.get_and_check_type(self.gs_main_rh, dmsg.LSPingGS)

    def do_teardown(self):
        self.ls_main_wh.put(dmsg.GSHalted(tag=self.next_tag()))
        count = 0
        msg = get_msg(self.be_main_rh)

        # keep ignoring everything received other than GSHalted
        while not isinstance(msg, dmsg.GSHalted) and count < 100:
            msg = get_msg(self.be_main_rh)
            count += 1

        self.ls_main_wh.put(dmsg.LSTeardown(tag=self.next_tag()))

        count = 0
        msg = get_msg(self.be_main_rh)

        # keep ignoring everything received other than LSHaltBE
        while not isinstance(msg, dmsg.LSHaltBE) and count < 100:
            msg = get_msg(self.be_main_rh)
            count += 1

        self.shep_stdin_wh.send(dmsg.BEHalted(tag=self.next_tag()).serialize())
        tsu.get_and_check_type(self.shep_stdout_rh, dmsg.LSHalted)

        self.proc.join()

        # TODO CPW: Ask Kent about this. We could change the closes to destroys but these Queue's are not owned by this process so I think close is the right thing. I checked and there isn't any files left around so I think it's okay.
        # In place of the next three destroy calls we could have called
        # detach, but this demonstrates that we can call destroy twice
        # on a channel and it will work. Local services destroyed first,
        # and then after the join above we know we can destroy here as well.
        # The advantage of calling destroy here is that since we know we are
        # done with the channel, destroy will ignore any ref counting and clean
        # it up.
        try:
            self.gs_main_rh.close()
        except:
            pass

        try:
            self.be_main_rh.close()
        except:
            pass

        try:
            self.ls_main_wh.close()
        except:
            pass

        if ATTACH_POOLS:
            # Calling destroy here is OK because we have joined on exit of local services
            # which already destoyed the pools. Calling destroy here forcefully detaches
            # from the pool (regardless of ref counting).
            self.def_pool.destroy()
            self.inf_pool.destroy()

    def do_abnormal_teardown(self):
        # when the BE receives AbnormalTermination, then the LSTeardown proceeds as usual
        # and LS expects LSTeardown in order to shut down
        tsu.get_and_check_type(self.be_main_rh, dmsg.AbnormalTermination)
        self.ls_main_wh.put(dmsg.LSTeardown(tag=self.next_tag()))
        tsu.get_and_check_type(self.be_main_rh, dmsg.LSHaltBE)
        self.shep_stdin_wh.send(dmsg.BEHalted(tag=self.next_tag()).serialize())
        tsu.get_and_check_type(self.shep_stdout_rh, dmsg.LSHalted)

        self.proc.join()

        try:
            self.gs_main_rh.close()
        except:
            pass

        try:
            self.be_main_rh.close()
        except:
            pass

        try:
            self.ls_main_wh.close()
        except:
            pass

        if ATTACH_POOLS:
            # Calling destroy here is OK because we have joined on exit of local services
            # which already destoyed the pools. Calling destroy here forcefully detaches
            # from the pool (regardless of ref counting).
            self.def_pool.destroy()
            self.inf_pool.destroy()

    def _make_pool(self, tag, p_uid, r_c_uid, size, m_uid, name):
        self.ls_main_wh.put(dmsg.LSPoolCreate(tag=tag, p_uid=p_uid, r_c_uid=r_c_uid, size=size, m_uid=m_uid, name=name))
        res = tsu.get_and_check_type(self.gs_main_rh, dmsg.LSPoolCreateResponse)
        self.assertEqual(dmsg.LSPoolCreateResponse.Errors.SUCCESS, res.err)

    def _destroy_pool(self, tag, p_uid, r_c_uid, m_uid):
        # Destroy the pool
        self.ls_main_wh.put(dmsg.LSPoolDestroy(tag=tag, p_uid=p_uid, r_c_uid=r_c_uid, m_uid=m_uid))
        res = tsu.get_and_check_type(self.gs_main_rh, dmsg.LSPoolDestroyResponse)
        self.assertEqual(dmsg.LSPoolDestroyResponse.Errors.SUCCESS, res.err)

    def test_bringup_teardown(self):
        # Tests normal bringup followed by teardown.
        self.do_bringup()
        self.do_teardown()

    def test_process_fwdoutput(self):
        self.do_bringup()
        target_puid = 17777

        the_tag = self.next_tag()
        self.ls_main_wh.put(
            dmsg.LSProcessCreate(
                tag=the_tag,
                exe=sys.executable,
                args=["shepherd/proc2.py"],
                p_uid=dfacts.GS_PUID,
                r_c_uid=dfacts.GS_INPUT_CUID,
                t_p_uid=target_puid,
            )
        )

        process_create_resp = tsu.get_and_check_type(self.gs_main_rh, dmsg.LSProcessCreateResponse)

        self.assertEqual(process_create_resp.ref, the_tag)

        process_output = tsu.get_and_check_type(self.be_main_rh, dmsg.LSFwdOutput, timeout=5)

        if process_output == "Hello World\n":
            process_output = tsu.get_and_check_type(self.be_main_rh, dmsg.LSFwdOutput, timeout=5)
            self.assertEqual(process_output.data, "Doing some more\n")
        else:
            self.assertEqual(process_output.data, "Hello World\nDoing some more\n")

        tsu.get_and_check_type(self.gs_main_rh, dmsg.LSProcessExit)
        self.do_teardown()

    def test_process_fwdinput(self):
        self.do_bringup()

        target_puid = 17777
        the_tag = self.next_tag()
        self.ls_main_wh.put(
            dmsg.LSProcessCreate(
                tag=the_tag,
                exe=sys.executable,
                args=["shepherd/proc4.py"],
                p_uid=dfacts.GS_PUID,
                r_c_uid=dfacts.GS_INPUT_CUID,
                t_p_uid=target_puid,
            )
        )

        process_create_resp = tsu.get_and_check_type(self.gs_main_rh, dmsg.LSProcessCreateResponse)

        self.assertEqual(process_create_resp.ref, the_tag)

        self.ls_main_wh.put(
            dmsg.LSFwdInput(
                tag=self.next_tag(),
                p_uid=dfacts.GS_PUID,
                t_p_uid=target_puid,
                r_c_uid=dfacts.GS_INPUT_CUID,
                input="Hi There\n",
                confirm=True,
            )
        )

        tsu.get_and_check_type(self.gs_main_rh, dmsg.LSFwdInputErr)

        output = ""
        try:
            msg = tsu.get_and_check_type(self.be_main_rh, dmsg.LSFwdOutput)
            output += msg.data
        except TimeoutError:
            pass

        self.assertTrue((output in "Enter some text: Here is the response text.\nHi There\n") and (len(output) != 0))

        self.do_teardown()

    def test_sh_pool_new_destroy(self):
        self.do_bringup()

        test_muid = 123456
        self.ls_main_wh.put(
            dmsg.LSPoolCreate(
                tag=self.next_tag(),
                p_uid=dfacts.GS_PUID,
                r_c_uid=dfacts.GS_INPUT_CUID,
                size=self.DEFAULT_TEST_POOL_SIZE,
                m_uid=test_muid,
                name="single_shep_test",
            )
        )

        res = tsu.get_and_check_type(self.gs_main_rh, dmsg.LSPoolCreateResponse)
        self.assertEqual(dmsg.LSPoolCreateResponse.Errors.SUCCESS, res.err)

        self.ls_main_wh.put(
            dmsg.LSPoolDestroy(tag=self.next_tag(), p_uid=dfacts.GS_PUID, r_c_uid=dfacts.GS_INPUT_CUID, m_uid=test_muid)
        )

        res = tsu.get_and_check_type(self.gs_main_rh, dmsg.LSPoolDestroyResponse)
        self.assertEqual(dmsg.LSPoolDestroyResponse.Errors.SUCCESS, res.err)

        self.do_teardown()

    def test_sh_pool_invalid_destroy(self):
        self.do_bringup()
        test_muid = 123456
        self.ls_main_wh.put(
            dmsg.LSPoolDestroy(tag=self.next_tag(), p_uid=dfacts.GS_PUID, r_c_uid=dfacts.GS_INPUT_CUID, m_uid=test_muid)
        )
        res = tsu.get_and_check_type(self.gs_main_rh, dmsg.LSPoolDestroyResponse)
        self.assertEqual(dmsg.LSPoolDestroyResponse.Errors.FAIL, res.err)

        self.do_teardown()

    def test_sh_pool_duplicate_id(self):
        self.do_bringup()
        test_muid = 123456

        self.ls_main_wh.put(
            dmsg.LSPoolCreate(
                tag=self.next_tag(),
                p_uid=dfacts.GS_PUID,
                r_c_uid=dfacts.GS_INPUT_CUID,
                size=self.DEFAULT_TEST_POOL_SIZE,
                m_uid=test_muid,
                name="single_shep_test",
            )
        )
        res = tsu.get_and_check_type(self.gs_main_rh, dmsg.LSPoolCreateResponse)
        self.assertEqual(dmsg.LSPoolCreateResponse.Errors.SUCCESS, res.err)

        self.ls_main_wh.put(
            dmsg.LSPoolCreate(
                tag=self.next_tag(),
                p_uid=dfacts.GS_PUID,
                r_c_uid=dfacts.GS_INPUT_CUID,
                size=self.DEFAULT_TEST_POOL_SIZE,
                m_uid=test_muid,
                name="single_shep_test",
            )
        )
        res = tsu.get_and_check_type(self.gs_main_rh, dmsg.LSPoolCreateResponse)
        self.assertEqual(dmsg.LSPoolCreateResponse.Errors.FAIL, res.err)

        # Destroy the pool
        self.ls_main_wh.put(
            dmsg.LSPoolDestroy(tag=self.next_tag(), p_uid=dfacts.GS_PUID, r_c_uid=dfacts.GS_INPUT_CUID, m_uid=test_muid)
        )

        # Check we succeeded so we know if other tests fail after this
        res = tsu.get_and_check_type(self.gs_main_rh, dmsg.LSPoolDestroyResponse)
        self.assertEqual(dmsg.LSPoolDestroyResponse.Errors.SUCCESS, res.err)

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

        self.ls_main_wh.put(
            dmsg.LSChannelCreate(
                tag=self.next_tag(),
                p_uid=dfacts.GS_PUID,
                r_c_uid=dfacts.GS_INPUT_CUID,
                m_uid=test_muid,
                c_uid=test_cuid,
            )
        )
        res = tsu.get_and_check_type(self.gs_main_rh, dmsg.LSChannelCreateResponse)
        self.assertEqual(dmsg.LSChannelCreateResponse.Errors.SUCCESS, res.err)

        self.ls_main_wh.put(
            dmsg.LSChannelDestroy(
                tag=self.next_tag(), p_uid=dfacts.GS_PUID, r_c_uid=dfacts.GS_INPUT_CUID, c_uid=test_cuid
            )
        )
        res = tsu.get_and_check_type(self.gs_main_rh, dmsg.LSChannelDestroyResponse)
        self.assertEqual(dmsg.LSChannelDestroyResponse.Errors.SUCCESS, res.err)

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
        self.ls_main_wh.put(
            dmsg.LSChannelCreate(
                tag=self.next_tag(),
                p_uid=dfacts.GS_PUID,
                r_c_uid=dfacts.GS_INPUT_CUID,
                m_uid=test_muid,
                c_uid=test_cuid,
            )
        )
        res = tsu.get_and_check_type(self.gs_main_rh, dmsg.LSChannelCreateResponse)
        self.assertEqual(dmsg.LSChannelCreateResponse.Errors.SUCCESS, res.err)

        self.ls_main_wh.put(
            dmsg.LSChannelCreate(
                tag=self.next_tag(),
                p_uid=dfacts.GS_PUID,
                r_c_uid=dfacts.GS_INPUT_CUID,
                m_uid=test_muid,
                c_uid=test_cuid,
            )
        )
        res = tsu.get_and_check_type(self.gs_main_rh, dmsg.LSChannelCreateResponse)
        self.assertEqual(dmsg.LSChannelCreateResponse.Errors.FAIL, res.err)

        self.ls_main_wh.put(
            dmsg.LSChannelDestroy(
                tag=self.next_tag(), p_uid=dfacts.GS_PUID, r_c_uid=dfacts.GS_INPUT_CUID, c_uid=test_cuid
            )
        )
        res = tsu.get_and_check_type(self.gs_main_rh, dmsg.LSChannelDestroyResponse)
        self.assertEqual(dmsg.LSChannelDestroyResponse.Errors.SUCCESS, res.err)

        self._destroy_pool(self.next_tag(), dfacts.GS_PUID, dfacts.GS_INPUT_CUID, test_muid)

        self.do_teardown()

    def test_noprocess_fwdinput(self):
        self.do_bringup()
        target_puid = 177

        self.ls_main_wh.put(
            dmsg.LSFwdInput(
                tag=self.next_tag(),
                p_uid=dfacts.GS_PUID,
                confirm=True,
                t_p_uid=target_puid,
                r_c_uid=dfacts.GS_INPUT_CUID,
                input="Hi There\n",
            )
        )

        tsu.get_and_check_type(self.gs_main_rh, dmsg.LSFwdInputErr)

        self.do_teardown()

    def test_process_errexit(self):
        self.do_bringup()
        the_tag = self.next_tag()
        target_puid = 177
        self.ls_main_wh.put(
            dmsg.LSProcessCreate(
                tag=the_tag,
                exe=sys.executable,
                args=["missing_prog.py"],
                p_uid=dfacts.GS_PUID,
                r_c_uid=dfacts.GS_INPUT_CUID,
                t_p_uid=target_puid,
            )
        )

        process_create_resp = tsu.get_and_check_type(self.gs_main_rh, dmsg.LSProcessCreateResponse)

        self.assertEqual(process_create_resp.ref, the_tag)

        process_exit = tsu.get_and_check_type(self.gs_main_rh, dmsg.LSProcessExit)

        self.assertEqual(process_exit.exit_code, 2)

        tsu.get_and_check_type(self.be_main_rh, dmsg.LSFwdOutput)

        self.do_teardown()

    def test_dump_state(self):
        self.do_bringup()
        self.ls_main_wh.put(dmsg.LSDumpState(tag=self.next_tag()))
        self.do_teardown()

    def test_dump_state_to_file(self):
        self.do_bringup()
        self.ls_main_wh.put(dmsg.LSDumpState(tag=self.next_tag(), filename="shep_dump.log"))
        self.do_teardown()

    def test_process_kill(self):
        self.do_bringup()
        target_puid = 177
        the_tag = self.next_tag()
        self.ls_main_wh.put(
            dmsg.LSProcessCreate(
                tag=the_tag,
                exe=sys.executable,
                args=["shepherd/proc2.py"],
                p_uid=dfacts.GS_PUID,
                r_c_uid=dfacts.GS_INPUT_CUID,
                t_p_uid=target_puid,
            )
        )

        process_create_resp = tsu.get_and_check_type(self.gs_main_rh, dmsg.LSProcessCreateResponse)

        self.assertEqual(process_create_resp.ref, the_tag)

        process_output = tsu.get_and_check_type(self.be_main_rh, dmsg.LSFwdOutput, timeout=5)

        self.ls_main_wh.put(
            dmsg.LSProcessKill(
                tag=self.next_tag(),
                p_uid=dfacts.GS_PUID,
                t_p_uid=target_puid,
                r_c_uid=dfacts.GS_INPUT_CUID,
                sig=signal.SIGKILL,
            )
        )

        if process_output.data == "Hello World\n":
            process_output = tsu.get_and_check_type(self.be_main_rh, dmsg.LSFwdOutput, timeout=5)
            self.assertEqual(process_output.data, "Doing some more\n")
        else:
            self.assertEqual(process_output.data, "Hello World\nDoing some more\n")

        msg = tsu.get_and_parse(self.gs_main_rh)
        self.assertTrue(isinstance(msg, dmsg.LSProcessExit) or isinstance(msg, dmsg.LSProcessKillResponse))
        self.do_teardown()

    def test_process_kill_wait(self):
        self.do_bringup()
        target_puid = 177
        the_tag = self.next_tag()
        self.ls_main_wh.put(
            dmsg.LSProcessCreate(
                tag=the_tag,
                exe=sys.executable,
                args=["shepherd/proc3.py"],
                p_uid=dfacts.GS_PUID,
                r_c_uid=dfacts.GS_INPUT_CUID,
                t_p_uid=target_puid,
            )
        )

        process_create_resp = tsu.get_and_check_type(self.gs_main_rh, dmsg.LSProcessCreateResponse)

        self.assertEqual(process_create_resp.ref, the_tag)

        self.ls_main_wh.put(
            dmsg.LSProcessKill(
                tag=self.next_tag(),
                p_uid=dfacts.GS_PUID,
                t_p_uid=target_puid,
                r_c_uid=dfacts.GS_INPUT_CUID,
                sig=signal.SIGKILL,
            )
        )

        msg = tsu.get_and_parse(self.gs_main_rh)
        self.assertTrue(isinstance(msg, dmsg.LSProcessExit) or isinstance(msg, dmsg.LSProcessKillResponse))
        self.do_teardown()

    def test_bringup_abnormal_termination(self):
        self.do_bringup()
        self.ls_main_wh.put(dmsg.AbnormalTermination(tag=self.next_tag()))
        self.do_abnormal_teardown()

    def test_bringup_bad_message(self):
        self.do_bringup()
        self.ls_main_wh.put(tsu._BadMsg(tag=self.next_tag()))
        self.do_abnormal_teardown()

    def test_process_create(self):
        self.do_bringup()
        the_tag = self.next_tag()
        target_puid = 177
        self.ls_main_wh.put(
            dmsg.LSProcessCreate(
                tag=the_tag,
                exe=sys.executable,
                args=["shepherd/proc1.py"],
                p_uid=dfacts.GS_PUID,
                r_c_uid=dfacts.GS_INPUT_CUID,
                t_p_uid=target_puid,
            )
        )

        process_create_resp = tsu.get_and_check_type(self.gs_main_rh, dmsg.LSProcessCreateResponse)

        self.assertEqual(process_create_resp.ref, the_tag)
        self.ls_main_wh.put(dmsg.LSDumpState(tag=self.next_tag()))
        process_output = tsu.get_and_check_type(self.be_main_rh, dmsg.LSFwdOutput)
        self.assertEqual(process_output.data, "Hello World\n")
        tsu.get_and_check_type(self.gs_main_rh, dmsg.LSProcessExit, 5)

        self.do_teardown()

    def test_process_create_with_env(self):
        self.do_bringup()
        target_puid = 177
        the_tag = self.next_tag()
        self.ls_main_wh.put(
            dmsg.LSProcessCreate(
                tag=the_tag,
                exe=sys.executable,
                args=["shepherd/procenv1.py"],
                env={"new_env_var": "env_value"},
                p_uid=dfacts.GS_PUID,
                r_c_uid=dfacts.GS_INPUT_CUID,
                t_p_uid=target_puid,
            )
        )

        process_create_resp = tsu.get_and_check_type(self.gs_main_rh, dmsg.LSProcessCreateResponse)
        self.assertEqual(process_create_resp.ref, the_tag)

        self.ls_main_wh.put(dmsg.LSDumpState(tag=0))

        process_output = tsu.get_and_check_type(self.be_main_rh, dmsg.LSFwdOutput)
        self.assertEqual(process_output.data, "env_value\n")

        process_done = tsu.get_and_check_type(self.gs_main_rh, dmsg.LSProcessExit, 5)
        self.assertEqual(process_done.exit_code, 0)

        self.do_teardown()

    def test_process_create_with_env2(self):
        self.do_bringup()
        target_puid = 177
        the_tag = self.next_tag()
        self.ls_main_wh.put(
            dmsg.LSProcessCreate(
                tag=the_tag,
                exe=sys.executable,
                args=["shepherd/procenv2.py"],
                env={"SHELL": "shell_value"},
                p_uid=dfacts.GS_PUID,
                r_c_uid=dfacts.GS_INPUT_CUID,
                t_p_uid=target_puid,
            )
        )

        process_create_resp = tsu.get_and_check_type(self.gs_main_rh, dmsg.LSProcessCreateResponse)
        self.assertEqual(process_create_resp.ref, the_tag)

        self.ls_main_wh.put(dmsg.LSDumpState(tag=self.next_tag()))

        process_output = tsu.get_and_check_type(self.be_main_rh, dmsg.LSFwdOutput)
        self.assertEqual(process_output.data, "shell_value\n")

        process_done = tsu.get_and_check_type(self.gs_main_rh, dmsg.LSProcessExit, 5)
        self.assertEqual(process_done.exit_code, 0)

        self.do_teardown()

    def test_process_create_dup(self):
        self.do_bringup()

        tag1 = self.next_tag()
        test_puid = 177

        self.ls_main_wh.put(
            dmsg.LSProcessCreate(
                tag=tag1,
                exe=sys.executable,
                args=["shepherd/proc1.py"],
                p_uid=dfacts.GS_PUID,
                r_c_uid=dfacts.GS_INPUT_CUID,
                t_p_uid=test_puid,
            )
        )

        tag2 = self.next_tag()
        self.ls_main_wh.put(
            dmsg.LSProcessCreate(
                tag=tag2,
                exe=sys.executable,
                args=["shepherd/proc1.py"],
                p_uid=dfacts.GS_PUID,
                r_c_uid=dfacts.GS_INPUT_CUID,
                t_p_uid=test_puid,
            )
        )

        process_create_resp1 = tsu.get_and_check_type(self.gs_main_rh, dmsg.LSProcessCreateResponse)

        if process_create_resp1.ref == tag1:
            process_create_resp2 = tsu.get_and_check_type(self.gs_main_rh, dmsg.LSProcessCreateResponse)
            self.assertEqual(process_create_resp2.ref, tag2)
            self.assertEqual(process_create_resp2.err, dmsg.LSProcessCreateResponse.Errors.FAIL)
        else:
            self.assertEqual(process_create_resp1.ref, tag2)
            self.assertEqual(process_create_resp1.err, dmsg.LSProcessCreateResponse.Errors.FAIL)
            process_create_resp2 = tsu.get_and_check_type(self.gs_main_rh, dmsg.LSProcessCreateResponse)
            self.assertEqual(process_create_resp2.ref, tag1)

        self.do_teardown()


if __name__ == "__main__":
    mp.set_start_method("spawn")
    unittest.main()
