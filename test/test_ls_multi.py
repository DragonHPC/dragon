#!/usr/bin/env python3
"""Test script for single node shepherd message handling"""

import logging
import os
import socket
import subprocess
import sys
import unittest

from dragon.cli import console_script_args
import dragon.channels as dch
import dragon.infrastructure.parameters as dparm
import dragon.infrastructure.messages as dmsg
import dragon.infrastructure.connection as dconn
import dragon.infrastructure.facts as dfacts
import dragon.launcher.util as dlutil
import dragon.launcher.pmsgqueue as dpq
import dragon.dlogging.util as dlog
from dragon.utils import B64

import support.util as tsu

get_msg = tsu.get_and_parse


def run_ls(test_env=None):
    dlog.setup_logging(basename="ls_multi_", level=logging.DEBUG, force=True)
    log = logging.getLogger('run ls')
    log.info('starting ls from run')

    if test_env is None:
        the_env = os.environ
    else:
        the_env = dict(os.environ)
        the_env.update(test_env)

    try:
        ls = subprocess.Popen(console_script_args(dfacts.PROCNAME_LS), env=the_env)
    except RuntimeError as rte:
        # shep_stdout_queue.send(dmsg.AbnormalTermination(tag=dsls.get_new_tag(),
        #                                                err_info=f'{rte!s}').serialize())
        print("something here")
        raise rte
    return ls


class MultiLS(unittest.TestCase):
    DEFAULT_TEST_POOL_SIZE = 2 ** 20

    def setUp(self) -> None:
        # Create posix msg queue for launcher comm
        _user = os.environ.get('USER', str(os.getuid()))
        self.hostname = socket.gethostname()
        self.ip_addr = socket.gethostbyname(self.hostname)
        self.pq_stdin = dpq.PMsgQueue(f'/{_user}_{self.hostname}_la_stdout', write_intent=True)
        self.pq_stdout = dpq.PMsgQueue(f'/{_user}_{self.hostname}_la_stdin', read_intent=True)

        self.dragon_logger = dlog.setup_dragon_logging(node_index=0)
        self.logger_sdesc = self.dragon_logger.serialize()

        self.channels_connected = False
        self.ls = None
        self.tag = 0

    def tearDown(self) -> None:
        self.pq_stdin.close(destroy=True)
        self.pq_stdout.close(destroy=True)

        if (self.channels_connected):
            self.ls_channel.detach()
            self.la_channel.detach()

        if self.ls is not None:
            self.ls.wait()

        self.dragon_logger.destroy()

    def next_tag(self):
        tmp = self.tag
        self.tag += 1
        return tmp

    def do_bringup(self):
        sh_ping_be_msg = self.do_BENodeIdxSH_SHPingBE(test_env={dfacts.TRANSPORT_TEST_ENV: '1'},
                                                      node_idx=0)
        self.connect_to_ls_channels(sh_ping_be_msg)
        sh_channels_up = self.do_BEPingSH_SHChannelsUp()
        self.do_TAUp(sh_channels_up)

    def do_teardown(self):
        self.do_SHHaltTA()
        self.do_SHTeardown()

    def do_BENodeIdxSH_SHPingBE(self, test_env=None, node_idx=0):

        self.ls = run_ls(test_env=test_env)

        # Send BENodeIdxSH as if I were launcher backend
        self.pq_stdin.send(dmsg.BENodeIdxSH(tag=self.next_tag(), node_idx=node_idx, ip_addrs=[self.ip_addr],
                                            host_name=self.hostname, primary=(node_idx == 0),
                                            logger_sdesc=B64(self.logger_sdesc),
                                            net_conf_key = str(node_idx)).serialize())
        return tsu.get_and_check_type(self.pq_stdout, dmsg.SHPingBE)

    def connect_to_ls_channels(self, sh_ping_be):

        self.ls_channel = dch.Channel.attach(B64.from_str(sh_ping_be.shep_cd).decode())
        self.la_channel = dch.Channel.attach(B64.from_str(sh_ping_be.be_cd).decode())

        # IN test transport mode this is null
        try:
            self.gs_cd = sh_ping_be.gs_cd
        except TypeError:
            self.gs_cd = None

        self.ls_queue = dconn.Connection(outbound_initializer=self.ls_channel, policy=dparm.POLICY_INFRASTRUCTURE)
        self.la_queue = dconn.Connection(inbound_initializer=self.la_channel, policy=dparm.POLICY_INFRASTRUCTURE)

        self.channels_connected = True

    def do_BEPingSH_SHChannelsUp(self):

        self.ls_queue.send(dmsg.BEPingSH(tag=self.next_tag()).serialize())
        return tsu.get_and_check_type(self.la_queue, dmsg.SHChannelsUp)

    def do_TAUp(self, sh_channels_msg):

        num_gw_channels = dfacts.DRAGON_DEFAULT_NUM_GW_CHANNELS_PER_NODE

        # Send LAChannelsInfo in a test environment or to all
        la_ch_info = dmsg.LAChannelsInfo(tag=self.next_tag(), nodes_desc=[sh_channels_msg],
                                         gs_cd=self.gs_cd, num_gw_channels=num_gw_channels)
        self.ls_queue.send(la_ch_info.serialize())
        tsu.get_and_check_type(self.la_queue, dmsg.TAUp)

    def do_SHHaltTA(self):

        self.ls_queue.send(dmsg.SHHaltTA(tag=self.next_tag()).serialize())
        tsu.get_and_check_type(self.la_queue, dmsg.TAHalted)

    def do_SHTeardown(self):

        self.ls_queue.send(dmsg.SHTeardown(tag=self.next_tag()).serialize())
        tsu.get_and_check_type(self.la_queue, dmsg.SHHaltBE)
        self.pq_stdin.send(dmsg.BEHalted(tag=self.next_tag()).serialize())


    def test_bringup_teardown_no_gs_primary(self):
        '''Transport test mode with no global services and is primary node

        Teardown of LS will begin with SHProcessExit receipt and
        transmission of TAHalted
        '''
        log = logging.getLogger("test_bringup_teardown_no_gs_primary")

        self.do_bringup()

        proc_create_msg = dlutil.mk_shproc_echo_msg()
        self.ls_queue.send(proc_create_msg.serialize())
        tsu.get_and_check_type(self.la_queue, dmsg.SHProcessCreateResponse)

        # Should get a forwarded output
        tsu.get_and_check_type(self.la_queue, dmsg.SHFwdOutput)
        tsu.get_and_check_type(self.la_queue, dmsg.SHProcessExit)

        self.do_teardown()


    def test_bringup_abnormal_termination(self):
        log = logging.getLogger("test_bringup_abnormal_termination")

        self.do_bringup()

        self.ls_queue.send(dmsg.AbnormalTermination(tag=self.next_tag()).serialize())
        tsu.get_and_check_type(self.la_queue, dmsg.AbnormalTermination)

        self.do_teardown()


    def test_bringup_bad_message(self):
        log = logging.getLogger("test_bringup_bad_message")

        self.do_bringup()

        self.ls_queue.send("crap")
        log.info("sent bad message to ls")
        tsu.get_and_check_type(self.la_queue, dmsg.AbnormalTermination)
        log.info("received AbnormalTermination from ls")

        self.do_teardown()


    def test_bringup_unexpected_message(self):
        log = logging.getLogger("test_bringup_unexpected_message")

        self.do_bringup()

        # send a message that we're not supposed to
        # LS should send AbnormalTermination msg to launcher be
        self.ls_queue.send(dmsg.GSProcessCreate(tag=self.next_tag(), exe='/bin/echo',
                                                args=['Hello World'], p_uid=dfacts.LAUNCHER_PUID,
                                                r_c_uid=dfacts.BASE_BE_CUID).serialize())
        log.info("sent unexpected message to ls")
        tsu.get_and_check_type(self.la_queue, dmsg.AbnormalTermination)
        log.info("received AbnormalTermination from ls")

        self.do_teardown()


if __name__ == "__main__":
    unittest.main()
