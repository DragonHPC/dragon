#!/usr/bin/env python3

"""Simple gs-shep integration tests

These tests are a 'first light' connecting global services and the shepherd and
the global services API.
"""
import shim_dragon_paths

import inspect
import logging
import multiprocessing
import threading
import os
import pickle
import signal
import sys
import time
import unittest
import time
import uuid

import dragon.channels as dch
import dragon.managed_memory as dmm
import dragon.utils as dutils

import dragon.globalservices.api_setup as dapi
import dragon.globalservices.process as dproc
import dragon.globalservices.channel as dgchan
import dragon.globalservices.pool as dgpool

import dragon.infrastructure.process_desc as pdesc
import dragon.infrastructure.channel_desc as dgchan_desc
import dragon.infrastructure.facts as dfacts
import dragon.infrastructure.messages as dmsg
import dragon.infrastructure.parameters as dparm
import dragon.infrastructure.connection as dconn
import dragon.infrastructure.util as dutil

import dragon.localservices.local_svc as dsls
import dragon.dlogging.util as dlog

import support.util as tsu

# Dummy executable for testing:
# `slow_echo.sh time msg` sleeps for time seconds then prints msg.
# this should be the path from where the test driver (this script) is run.
import pathlib


# make sure we use a safe pool uid for creating a pool
# that will never overlap with infra/default/user pool uids
CUSTOM_POOL_MUID = 2 ** 60

path_to_slow_echo = pathlib.Path(__file__).parent / 'slow_echo.sh'
if not path_to_slow_echo.exists():
    path_to_slow_echo = pathlib.Path(__file__).parent / 'integration' / 'slow_echo.sh'
SLOW_ECHO = str(path_to_slow_echo)


def mk_sleep_msg(seconds, tag, p_uid, r_c_uid):
    return dmsg.GSProcessCreate(tag=tag, exe='/bin/sleep', args=[str(seconds)],
                                p_uid=p_uid, r_c_uid=r_c_uid)


def mk_slow_echo_msg(seconds, msg, tag, p_uid, r_c_uid):
    return dmsg.GSProcessCreate(tag=tag, exe=SLOW_ECHO, args=[str(seconds), msg],
                                p_uid=p_uid, r_c_uid=r_c_uid)


def start_ls(shep_stdin_queue, shep_stdout_queue, env_update, name_addition=''):
    dlog.setup_logging(basename='ls_' + name_addition, level=logging.DEBUG)
    log = logging.getLogger('ls-gs integration test channels, ls starts gs')
    log.info('--------------------ls started-----------------------')

    dparm.this_process.mode = dfacts.SINGLE_MODE
    os.environ.update(dparm.this_process.env())
    log.info('starting')
    try:
        dsls.single(ls_stdin=shep_stdin_queue, ls_stdout=shep_stdout_queue,
                    gs_args=[sys.executable, '-c', dfacts.GS_SINGLE_LAUNCH_CMD],
                    gs_env=env_update)
        log.info('exited')
    except RuntimeError as rte:
        log.exception('runtime failure\n')
        shep_stdout_queue.send(dmsg.AbnormalTermination(tag=dsls.get_new_tag(),
                                                        err_info=f'{rte!s}').serialize())
    finally:
        shep_stdin_queue.close()
        shep_stdout_queue.close()

    logging.shutdown()


class GSLS(unittest.TestCase):
    """ Global Services / Local Services (Shepherd) Integration tests
    """

    SLEEPY_HEAD_TIME = 60  # seconds, 60 seconds

    def setUp(self) -> None:
        self.tag = 0
        self.mode = dfacts.SINGLE_MODE

        self.shep_stdin_rh, self.shep_stdin_wh = multiprocessing.Pipe(duplex=False)
        self.shep_stdout_rh, self.shep_stdout_wh = multiprocessing.Pipe(duplex=False)

        self.pool_name = 'gslsinttest_' + os.environ.get('USER', str(os.getuid())) + str(uuid.uuid4())
        self.pool_size = 2 ** 30
        self.pool_uid = CUSTOM_POOL_MUID
        self.test_pool = dmm.MemoryPool(self.pool_size, self.pool_name, self.pool_uid)

        # need a test pool and test channels to hold the return channel
        # from GS to the test bench.  Picking cuid for these high enough
        # that they won't be used in the tests

        self.gs_return_cuid = 2 ** 64 - CUSTOM_POOL_MUID
        self.proc_gs_return_chan = dch.Channel(self.test_pool, self.gs_return_cuid)
        self.proc_gs_return_rh = dconn.Connection(inbound_initializer=self.proc_gs_return_chan)

        self.chatty_teardown = False

    def next_tag(self):
        tmp = self.tag
        self.tag += 1
        return tmp

    def start_duts(self):
        test_name = self.__class__.__name__ + '_' + inspect.stack()[1][0].f_code.co_name

        # Make a list of channels to pass thru the environment to GS for testing purposes.
        test_chan_list = [(self.gs_return_cuid, self.test_pool.serialize(),
                           self.proc_gs_return_chan.serialize(), False)]

        val = pickle.dumps(test_chan_list)
        env_update = {dfacts.GS_TEST_CH_EV: dutils.B64.bytes_to_str(val),
                      dfacts.GS_LOG_BASE: 'gs_' + test_name}

        start_args = (self.shep_stdin_rh, self.shep_stdout_wh, env_update)

        self.ls_dut = multiprocessing.Process(target=start_ls, args=start_args,
                                              kwargs={'name_addition': test_name},
                                              name='local svc')

        self.ls_dut.start()

        self.shep_stdin_wh.send(dmsg.BENodeIdxSH(tag=self.next_tag(), node_idx=0).serialize())

        ping_be_msg = tsu.get_and_check_type(self.shep_stdout_rh, dmsg.SHPingBE)

        env2b = dutils.B64.str_to_bytes

        self.inf_pool = dmm.MemoryPool.attach(env2b(ping_be_msg.inf_pd))

        self.shep_input_chan = dch.Channel.attach(env2b(ping_be_msg.shep_cd))
        self.shep_input_wh = dconn.Connection(outbound_initializer=self.shep_input_chan)

        self.bela_input_chan = dch.Channel.attach(env2b(ping_be_msg.be_cd))
        self.bela_input_rh = dconn.Connection(inbound_initializer=self.bela_input_chan)

        self.gs_input_chan = dch.Channel.attach(env2b(ping_be_msg.gs_cd))
        self.gs_input_wh = dconn.Connection(outbound_initializer=self.gs_input_chan)

        dapi.test_connection_override(test_gs_input=self.gs_input_wh,
                                      test_gs_return=self.proc_gs_return_rh,
                                      test_gs_return_cuid=self.gs_return_cuid,
                                      test_shep_input=self.shep_input_wh)

        self.shep_input_wh.send(dmsg.BEPingSH(tag=self.next_tag()).serialize())

        sh_msg = tsu.get_and_check_type(self.bela_input_rh, dmsg.SHChannelsUp)
        self.assertIsInstance(sh_msg, dmsg.SHChannelsUp)
        self.assertEqual(dutils.host_id(), sh_msg.node_desc.host_id)
        if self.chatty_teardown:
            print(f"info from SHChannelsUp: host_id={sh_msg.node_desc.host_id}, hostname={sh_msg.node_desc.host_name}")
            sys.stdout.flush()

        tsu.get_and_check_type(self.bela_input_rh, dmsg.GSIsUp)

        self.starting_shm = dutil.survey_dev_shm()

    def tearDown(self) -> None:
        self.gs_input_wh.send(dmsg.GSTeardown(tag=self.next_tag()).serialize())
        if self.chatty_teardown:
            print('sent GSTeardown')
            sys.stdout.flush()

        tsu.get_and_check_several_ignore_SHFwdOutput(self, self.bela_input_rh, {dmsg.GSHalted: 1})
        if self.chatty_teardown:
            print('got GSHalted')
            sys.stdout.flush()

        self.shep_input_wh.send(dmsg.SHTeardown(tag=self.next_tag()).serialize())
        if self.chatty_teardown:
            print('sent SHTeardown')
            sys.stdout.flush()

        tsu.get_and_check_several_ignore_SHFwdOutput(self, self.bela_input_rh, {dmsg.SHHaltBE: 1})
        if self.chatty_teardown:
            print('got SHHaltBE')
            sys.stdout.flush()

        self.shep_input_chan.detach()
        self.bela_input_chan.detach()
        self.gs_input_chan.detach()

        self.proc_gs_return_chan.destroy()

        self.inf_pool.detach()

        self.shep_stdin_wh.send(dmsg.BEHalted(tag=self.next_tag()).serialize())

        tsu.get_and_check_several_ignore_SHFwdOutput(self, self.shep_stdout_rh, {dmsg.SHHalted: 1})
        self.shep_stdin_rh.close()
        self.shep_stdin_wh.close()
        self.shep_stdout_rh.close()
        self.shep_stdout_wh.close()

        self.ls_dut.join()
        self.test_pool.destroy()
        dutil.compare_dev_shm(self.starting_shm)

    def kill_sleepy_head(self):
        msg = dmsg.GSProcessKill(tag=self.next_tag(), p_uid=dfacts.LAUNCHER_PUID,
                                 r_c_uid=dfacts.BASE_BE_CUID,
                                 sig=signal.SIGKILL, t_p_uid=self.head_puid)
        self.gs_input_wh.send(msg.serialize())

        # Since the backend can still be receiving messages from LS,
        # discard its SHFwdOutput messages that still may be in the channel
        # queue
        msg = tsu.get_and_parse(self.bela_input_rh)
        while (isinstance(msg, dmsg.SHFwdOutput)):
            msg = tsu.get_and_parse(self.bela_input_rh)

        if isinstance(msg, dmsg.GSProcessKillResponse):
            tsu.get_and_check_type(self.bela_input_rh, dmsg.GSHeadExit)
        else:
            self.assertIsInstance(msg, dmsg.GSHeadExit)
            tsu.get_and_check_type(self.bela_input_rh, dmsg.GSProcessKillResponse)

    def start_sleepy_head(self, timeout):
        msg = mk_sleep_msg(timeout, self.next_tag(), p_uid=dfacts.LAUNCHER_PUID,
                           r_c_uid=dfacts.BASE_BE_CUID)

        self.gs_input_wh.send(msg.serialize())

        resp = tsu.get_and_check_type(self.bela_input_rh, dmsg.GSProcessCreateResponse)

        self.assertEqual(resp.err, dmsg.GSProcessCreateResponse.Errors.SUCCESS)

        self.head_puid = resp.desc.p_uid
        dparm.this_process.my_puid = self.head_puid

    def test_proc_direct(self):

        self.start_duts()
        echotxt = 'Hello World'

        msg = dmsg.GSProcessCreate(tag=self.next_tag(), exe='/bin/echo',
                                args=[echotxt], p_uid=dfacts.LAUNCHER_PUID,
                                r_c_uid=dfacts.BASE_BE_CUID)

        self.gs_input_wh.send(msg.serialize())

        # Expect a GSPCR, a SHFwdOutput, and a GSHeadExit message where
        # the output could be in any order but the PCR will precede
        # the HeadExit.

        msgs = tsu.get_and_check_several_ignore_SHFwdOutput(self, self.bela_input_rh,
                                        {dmsg.GSProcessCreateResponse: 1,
                                        dmsg.GSHeadExit: 1})

        pcr_msg, _ = msgs[dmsg.GSProcessCreateResponse][0]
        self.assertEqual(pcr_msg.err, dmsg.GSProcessCreateResponse.Errors.SUCCESS)

        if len(msgs[dmsg.SHFwdOutput]) > 0:
            output_msg, _ = msgs[dmsg.SHFwdOutput][0]
            self.assertEqual(echotxt + '\n', output_msg.data)

    def test_sleepy_exit(self):
        self.start_duts()

        self.start_sleepy_head(1)

        tsu.get_and_check_type(self.bela_input_rh, dmsg.GSHeadExit, timeout=5)

    def test_sleepy_echo_head(self):
        self.start_duts()

        output = 'hyenas are awesome'
        msg = mk_slow_echo_msg(1, output, self.next_tag(), p_uid=dfacts.LAUNCHER_PUID,
                               r_c_uid=dfacts.BASE_BE_CUID)
        self.gs_input_wh.send(msg.serialize())

        msgs = tsu.get_and_check_several_ignore_SHFwdOutput(self, self.bela_input_rh, {dmsg.GSProcessCreateResponse: 1,
                                                              dmsg.GSHeadExit: 1})

        create_msg, create_order = msgs[dmsg.GSProcessCreateResponse][0]
        self.assertEqual(create_msg.err, dmsg.GSProcessCreateResponse.Errors.SUCCESS)

        if len(msgs[dmsg.SHFwdOutput]) > 0:
            output_msg, _ = msgs[dmsg.SHFwdOutput][0]
            self.assertEqual(output + '\n', output_msg.data)


    def test_head_kill(self):
        self.start_duts()
        self.start_sleepy_head(self.SLEEPY_HEAD_TIME)
        self.kill_sleepy_head()

    def test_api_create(self):
        self.start_duts()
        self.start_sleepy_head(self.SLEEPY_HEAD_TIME)
        res = dproc.create(exe='/bin/sleep', run_dir='', args=['1'], env={}, user_name='hproc')
        self.assertEqual(res.name, 'hproc')
        self.kill_sleepy_head()

    @unittest.skip('derp')
    def test_api_create_output(self):
        self.start_duts()
        self.start_sleepy_head(self.SLEEPY_HEAD_TIME)

        res = dproc.create(exe=SLOW_ECHO, run_dir='', args=['10', 'hyena'],
                           env={}, user_name='hproc')
        self.assertEqual(res.name, 'hproc')

        resp = tsu.get_and_check_type(self.bela_input_rh, dmsg.SHFwdOutput, 3)
        self.assertEqual('hyena\n', resp.data)
        self.kill_sleepy_head()

    def test_api_join(self):
        self.start_duts()
        self.start_sleepy_head(self.SLEEPY_HEAD_TIME)

        res = dproc.create(exe='/bin/sleep', run_dir='', args=['1'], env={})

        dproc.join(res.p_uid)

        self.kill_sleepy_head()

    # will print warnings if the pool isn't cleaned up in
    # teardown.  Would like a better way to do this but
    # TODO: refactor the test so it fails properly if this does not happen
    def test_pool_and_cleanup(self):
        self.start_duts()
        self.start_sleepy_head(self.SLEEPY_HEAD_TIME)

        _ = dgpool.create(2 ** 20)

        self.kill_sleepy_head()

    def test_list(self):
        self.start_duts()

        self.start_sleepy_head(self.SLEEPY_HEAD_TIME)

        res = dproc.create(exe='/bin/sleep', run_dir='', args=['1'], env={})
        procs = dproc.get_list()
        self.assertEqual([res.p_uid-1, res.p_uid], procs)

        self.kill_sleepy_head()

    def test_inf_channel_basic(self):
        self.start_duts()
        self.start_sleepy_head(self.SLEEPY_HEAD_TIME)

        res = dproc.create(exe='python3', run_dir='', env={},
                           args=['globalservices/managed_proc_test_target.py'])
        dproc.join(res.p_uid)
        dead_pd = dproc.query(res.p_uid)
        self.kill_sleepy_head()

        self.assertEqual(dead_pd.state, pdesc.ProcessDescriptor.State.DEAD)
        self.assertEqual(dead_pd.ecode, 0)

    def test_inf_channel_gsr(self):
        self.start_duts()

        self.start_sleepy_head(self.SLEEPY_HEAD_TIME)

        res = dproc.create(exe='python3', run_dir='',
                           args=['globalservices/managed_proc_test_target.py', '--gs-ret'],
                           env={},
                           options=pdesc.ProcessOptions(make_inf_channels=True))

        dproc.join(res.p_uid)

        dead_pd = dproc.query(res.p_uid)
        gsr_desc = dgchan.wait(dead_pd.gs_ret_cuid, timeout=3)

        self.kill_sleepy_head()

        self.assertEqual(dead_pd.state, pdesc.ProcessDescriptor.State.DEAD)
        self.assertEqual(dead_pd.ecode, 0)
        self.assertEqual(gsr_desc.state, gsr_desc.State.DEAD)

    def test_inf_channels_nullarg(self):
        self.start_duts()
        self.start_sleepy_head(self.SLEEPY_HEAD_TIME)

        res = dproc.create(exe='python3', run_dir='',
                           args=['globalservices/managed_proc_test_target.py', '--args'],
                           env={},
                           options=pdesc.ProcessOptions(make_inf_channels=True))

        dproc.join(res.p_uid)
        dead_pd = dproc.query(res.p_uid)
        self.kill_sleepy_head()

        self.assertEqual(dead_pd.state, pdesc.ProcessDescriptor.State.DEAD)
        self.assertEqual(dead_pd.ecode, 0)

    def test_inf_channels_nullarg_twice(self):
        self.start_duts()

        self.start_sleepy_head(self.SLEEPY_HEAD_TIME)

        res = dproc.create(exe=sys.executable, run_dir='',
                           args=['globalservices/managed_proc_test_target.py', '--args'],
                           env={},
                           options=pdesc.ProcessOptions(make_inf_channels=True))

        res2 = dproc.create(exe=sys.executable, run_dir='',
                            args=['globalservices/managed_proc_test_target.py', '--args'],
                            env={},
                            options=pdesc.ProcessOptions(make_inf_channels=True))

        dproc.join(res.p_uid)
        dproc.join(res2.p_uid)

        dead_pd = dproc.query(res.p_uid)
        dead_pd2 = dproc.query(res2.p_uid)

        self.kill_sleepy_head()

        self.assertEqual(dead_pd.state, pdesc.ProcessDescriptor.State.DEAD)
        self.assertEqual(dead_pd.ecode, 0)
        self.assertEqual(dead_pd2.state, pdesc.ProcessDescriptor.State.DEAD)
        self.assertEqual(dead_pd2.ecode, 0)

    def test_inf_channels_immediate_arg(self):
        self.start_duts()

        # Checks that the args got there.
        self.start_sleepy_head(self.SLEEPY_HEAD_TIME)

        test_script_args = ['globalservices/managed_proc_test_target.py',
                            '--args',
                            '23',
                            '45',
                            '17']

        the_argdata = pickle.dumps((23, 45, 17))
        res = dproc.create_with_argdata(exe=sys.executable, run_dir='',
                                        args=test_script_args,
                                        env={}, argdata=the_argdata)

        dproc.join(res.p_uid)

        dead_pd = dproc.query(res.p_uid)

        self.kill_sleepy_head()

        self.assertEqual(dead_pd.state, pdesc.ProcessDescriptor.State.DEAD)
        self.assertEqual(dead_pd.ecode, 0)

    def test_inf_channels_jumbo_arg(self):
        self.start_duts()

        self.start_sleepy_head(self.SLEEPY_HEAD_TIME)
        arg_size = 2 ** 25

        test_script_args = ['globalservices/jumbo_arg_test_target.py',
                            str(arg_size)]

        res = dproc.create_with_argdata(exe=sys.executable, run_dir='',
                                        args=test_script_args,
                                        env={}, argdata=bytearray(arg_size))

        dproc.join(res.p_uid)

        dead_pd = dproc.query(res.p_uid)

        self.kill_sleepy_head()

        self.assertEqual(dead_pd.state, pdesc.ProcessDescriptor.State.DEAD)
        self.assertEqual(dead_pd.ecode, 0)

    def test_connection_and_api(self):
        self.start_duts()
        self.start_sleepy_head(self.SLEEPY_HEAD_TIME)

        first_cn = 'bob'
        second_cn = 'fred'

        my_muid = dfacts.default_pool_muid_from_index(0)

        dgchan.create(my_muid, first_cn)
        dgchan.create(my_muid, second_cn)

        first_args = ['connection/conn_arg_test_target.py', 'first']
        second_args = ['connection/conn_arg_test_target.py', 'second']

        delivered_args = (first_cn, second_cn)
        the_argdata = pickle.dumps(delivered_args)
        first_pd = dproc.create_with_argdata(exe=sys.executable, run_dir='',
                                             args=first_args,
                                             env={}, argdata=the_argdata)

        delivered_args = (second_cn, first_cn)
        the_argdata = pickle.dumps(delivered_args)
        second_pd = dproc.create_with_argdata(exe=sys.executable, run_dir='',
                                              args=second_args,
                                              env={}, argdata=the_argdata)

        dproc.join(first_pd.p_uid)
        dproc.join(second_pd.p_uid)

        # refresh the descriptors
        first_pd = dproc.query(first_pd.p_uid)
        second_pd = dproc.query(second_pd.p_uid)

        dgchan.destroy(first_cn)
        dgchan.destroy(second_cn)

        self.kill_sleepy_head()

        self.assertEqual(first_pd.state, pdesc.ProcessDescriptor.State.DEAD)
        self.assertEqual(first_pd.ecode, 0)

        self.assertEqual(second_pd.state, pdesc.ProcessDescriptor.State.DEAD)
        self.assertEqual(second_pd.ecode, 0)

    def test_channel_refcount(self):
        """PE-38654 Test ref counted channels and channel parameters
        """

        self.start_duts()
        self.start_sleepy_head(self.SLEEPY_HEAD_TIME)

        # Create a refcounted user channel
        m_uid = dfacts.default_pool_muid_from_index(0)

        the_options = dgchan_desc.ChannelOptions(ref_count=True)
        the_name = "test_channel_refcount-"+os.environ.get('USER', str(os.getuid()))

        descr = dgchan.create(m_uid, the_name)

        # get identifiers
        c_uid = descr.c_uid
        c_name= descr.name

        # Functions for the threads
        def call_get_refcnt(name, c_uid):
            dgchan.get_refcnt(name)
            dgchan.get_refcnt(c_uid)

        def call_release_refcnt(name, c_uid):
            dgchan.release_refcnt(name)
            dgchan.release_refcnt(c_uid)

        # Petal to the metal
        maxproc = 16

        # Create a few threads and have them run get_refcnt()
        threads = [ threading.Thread(target=call_get_refcnt, args=(c_name, c_uid)) for i in range(0,maxproc)]

        for th in threads:
            th.start()

        for th in threads:
            th.join()

        # See what's going on with the channel
        new_descr = dgchan.query(c_uid, inc_refcnt=False)

        self.assertEqual(descr.sdesc, new_descr.sdesc) # at the least ...

        # Create a bunch of threads have them run release_refcnt()
        threads = [ threading.Thread(target=call_release_refcnt, args=(c_name, c_uid)) for i in range(0,maxproc)]

        for th in threads:
            th.start()

        for th in threads:
            th.join()

        # check that channel status is DEAD, but give Dragon some time.
        final_descr = dgchan.wait(c_uid, timeout=3)

        self.assertEqual(final_descr.state, final_descr.State.DEAD)

        # cleanup
        self.kill_sleepy_head()


def start_multi_ls(shep_stdin_queue, shep_stdout_queue, env_update, name_addition=''):
    dparm.this_process.mode = dfacts.CRAYHPE_MULTI_MODE
    dparm.this_process.debug = '1'
    os.environ.update(dparm.this_process.env())
    try:
        dsls.multinode(ls_stdin=shep_stdin_queue, ls_stdout=shep_stdout_queue,
                       gs_args=[sys.executable, '-c', dfacts.GS_MULTI_LAUNCH_CMD],
                       gs_env=env_update, fname='LS_' + name_addition,
                       ta_args=[sys.executable, '-c', dfacts.TA_MULTI_LAUNCH_CMD])
    except RuntimeError as rte:
        shep_stdout_queue.send(dmsg.AbnormalTermination(tag=dsls.get_new_tag(),
                                                        err_info=f'{rte!s}').serialize())
    finally:
        shep_stdin_queue.close()
        shep_stdout_queue.close()


class GSLSMulti(unittest.TestCase):
    """ Global Services / Local Services (Shepherd) Integration tests
    where GS process talks to two or more LS processes on the same node
    """

    SLEEPY_HEAD_TIME = 60  # seconds, 60 seconds

    def setUp(self) -> None:
        self.nls = 3 # number of LS processes
        self.tag = 0
        self.mode = dfacts.CRAYHPE_MULTI_MODE
        self.names = {} # helper dict that holds all the necessary names for connections for the different LS processes

        for i in range(self.nls):
            self.names[f'shep_stdin_rh_{i}'], self.names[f'shep_stdin_wh_{i}'] = multiprocessing.Pipe(duplex=False)
            self.names[f'shep_stdout_rh_{i}'], self.names[f'shep_stdout_wh_{i}'] = multiprocessing.Pipe(duplex=False)

        # need a test pool and test channels to hold the return channel
        # from GS to the test bench.  Picking cuid for these high enough
        # that they won't be used in the tests
        self.pool_name = 'gslsinttest_' + os.environ.get('USER', str(os.getuid())) + str(uuid.uuid4())
        self.pool_size = 2 ** 30
        self.pool_uid = CUSTOM_POOL_MUID
        self.test_pool = dmm.MemoryPool(self.pool_size, self.pool_name, self.pool_uid)

        self.gs_return_cuid = 2 ** 64 - CUSTOM_POOL_MUID
        self.proc_gs_return_chan = dch.Channel(self.test_pool, self.gs_return_cuid)
        self.proc_gs_return_rh = dconn.Connection(inbound_initializer=self.proc_gs_return_chan)

        self.chatty_teardown = False

        self.gs_cd = None

    def next_tag(self):
        tmp = self.tag
        self.tag += 1
        return tmp

    def start_multi_duts(self):
        for i in range(self.nls):
            test_name = f'{i}_{self.__class__.__name__}_{inspect.stack()[1][0].f_code.co_name}'
            if i == 0: # primary LS
                # Make a list of channels to pass thru the environment to GS for testing purposes.
                test_chan_list = [(self.gs_return_cuid, self.test_pool.serialize(),
                                   self.proc_gs_return_chan.serialize(), False)]
                val = pickle.dumps(test_chan_list)
                env_update = {dfacts.GS_TEST_CH_EV: dutils.B64.bytes_to_str(val),
                              dfacts.GS_LOG_BASE: 'gs_' + test_name}
                start_args = (self.names[f'shep_stdin_rh_{i}'], self.names[f'shep_stdout_wh_{i}'], env_update)
            else:
                start_args = (self.names[f'shep_stdin_rh_{i}'], self.names[f'shep_stdout_wh_{i}'], None)

            self.names[f'ls_dut_{i}'] = multiprocessing.Process(target=start_multi_ls, args=start_args,
                                                                kwargs={'name_addition': test_name},
                                                                name=f'local_svc_{i}')
            self.names[f'ls_dut_{i}'].start()

        if self.chatty_teardown:
            print(f'The {self.nls} local services have started', flush=True)

        chs_up = []
        for i in range(self.nls):
            if i == 0:
                self.names[f'shep_stdin_wh_{i}'].send(dmsg.BENodeIdxSH(tag=self.next_tag(), node_idx=i, primary=True).serialize())
            else:
                self.names[f'shep_stdin_wh_{i}'].send(dmsg.BENodeIdxSH(tag=self.next_tag(), node_idx=i, primary=False).serialize())

            ping_be_msg = tsu.get_and_check_type(self.names[f'shep_stdout_rh_{i}'], dmsg.SHPingBE)
            if self.chatty_teardown:
                print(f'got SHPingBE for LS {i}', flush=True)

            env2b = dutils.B64.str_to_bytes

            self.names[f'shep_input_chan_{i}'] = dch.Channel.attach(env2b(ping_be_msg.shep_cd))
            self.names[f'shep_input_wh_{i}'] = dconn.Connection(outbound_initializer=self.names[f'shep_input_chan_{i}'])


            self.names[f'bela_input_chan_{i}'] = dch.Channel.attach(env2b(ping_be_msg.be_cd))
            self.names[f'bela_input_rh_{i}'] = dconn.Connection(inbound_initializer=self.names[f'bela_input_chan_{i}'])

            if i == 0:
                self.gs_cd = ping_be_msg.gs_cd
                self.gs_input_chan = dch.Channel.attach(env2b(ping_be_msg.gs_cd))
                self.gs_input_wh = dconn.Connection(outbound_initializer=self.gs_input_chan)

                dapi.test_connection_override(test_gs_input=self.gs_input_wh,
                                              test_gs_return=self.proc_gs_return_rh,
                                              test_gs_return_cuid=self.gs_return_cuid,
                                              test_shep_input=self.names[f'shep_input_wh_{i}'])

            self.names[f'shep_input_wh_{i}'].send(dmsg.BEPingSH(tag=self.next_tag()).serialize())

            msg = tsu.get_and_check_type(self.names[f'bela_input_rh_{i}'], dmsg.SHChannelsUp)
            assert isinstance(msg, dmsg.SHChannelsUp)
            self.assertEqual(dutils.host_id(), msg.node_desc.host_id)
            if self.chatty_teardown:
                print(f'got SHChannelsUp from LS {i}', flush=True)
                print(f'Info from SHChannelsUp: host_id={msg.node_desc.host_id}, hostname={msg.node_desc.host_name}', flush=True)

            chs_up.append(msg)

        nodes_desc = {ch_up.idx: ch_up.node_desc for ch_up in chs_up}
        la_ch_info = dmsg.LAChannelsInfo(tag=self.next_tag(), nodes_desc=nodes_desc,
                                         gs_cd=self.gs_cd, num_gw_channels=0)
        for i in range(self.nls):
            # communicate gs_cd to all local services process so that they
            # can communicate with gs
            self.names[f'shep_input_wh_{i}'].send(la_ch_info.serialize())
            if self.chatty_teardown:
                print(f'sent LACHannelsInfo to LS {i}', flush=True)

            # we need to receive TAUp
            tsu.get_and_check_type(self.names[f'bela_input_rh_{i}'], dmsg.TAUp)
            if self.chatty_teardown:
                print(f'received TAUp from LS {i}')
                sys.stdout.flush()

        tsu.get_and_check_type(self.names['bela_input_rh_0'], dmsg.GSIsUp)
        if self.chatty_teardown:
            print('received GSIsUp')
            sys.stdout.flush()

        self.starting_shm = dutil.survey_dev_shm()


    def tearDown(self) -> None:
        self.gs_input_wh.send(dmsg.GSTeardown(tag=self.next_tag()).serialize())
        if self.chatty_teardown:
            print('sent GSTeardown')
            sys.stdout.flush()

        tsu.get_and_check_type(self.names['bela_input_rh_0'], dmsg.GSHalted)
        if self.chatty_teardown:
            print('got GSHalted')
            sys.stdout.flush()

        self.names['shep_input_wh_0'].send(dmsg.SHHaltTA(tag=self.next_tag()).serialize())
        if self.chatty_teardown:
            print('sent SHHaltTA')
            sys.stdout.flush()

        tsu.get_and_check_type(self.names['bela_input_rh_0'], dmsg.TAHalted)
        if self.chatty_teardown:
            print('got TAHalted')
            sys.stdout.flush()

        for i in range(self.nls):
            self.names[f'shep_input_wh_{i}'].send(dmsg.SHTeardown(tag=self.next_tag()).serialize())
            if self.chatty_teardown:
                print(f'sent SHTeardown to LS {i}')
                sys.stdout.flush()

            tsu.get_and_check_type(self.names[f'bela_input_rh_{i}'], dmsg.SHHaltBE)
            if self.chatty_teardown:
                print(f'got SHHaltBE from LS {i}')
                sys.stdout.flush()

        for i in range(self.nls):
            self.names[f'shep_input_chan_{i}'].detach()
            self.names[f'bela_input_chan_{i}'].detach()

            self.names[f'shep_stdin_wh_{i}'].send(dmsg.BEHalted(tag=self.next_tag()).serialize())
            if self.chatty_teardown:
                print(f'sent BEHalted to LS {i}')
                sys.stdout.flush()

        self.gs_input_chan.detach()
        self.proc_gs_return_chan.destroy()

        for i in range(self.nls):
            self.names[f'shep_stdin_rh_{i}'].close()
            self.names[f'shep_stdin_wh_{i}'].close()
            self.names[f'shep_stdout_rh_{i}'].close()
            self.names[f'shep_stdout_wh_{i}'].close()

        for i in range(self.nls):
            self.names[f'ls_dut_{i}'].join()

        self.test_pool.destroy()
        dutil.compare_dev_shm(self.starting_shm)

    def kill_sleepy_head(self):
        msg = dmsg.GSProcessKill(tag=self.next_tag(), p_uid=dfacts.LAUNCHER_PUID,
                                 r_c_uid=dfacts.BASE_BE_CUID,
                                 sig=signal.SIGKILL, t_p_uid=self.head_puid)
        self.gs_input_wh.send(msg.serialize())

        # Since the backend can still be receiving messages from LS,
        # discard its SHFwdOutput messages that still may be in the channel
        # queue
        msg = tsu.get_and_parse(self.names['bela_input_rh_0'])
        while (isinstance(msg, dmsg.SHFwdOutput)):
            msg = tsu.get_and_parse(self.names['bela_input_rh_0'])

        if isinstance(msg, dmsg.GSProcessKillResponse):
            tsu.get_and_check_type(self.names['bela_input_rh_0'], dmsg.GSHeadExit)
        else:
            self.assertIsInstance(msg, dmsg.GSHeadExit)
            tsu.get_and_check_type(self.names['bela_input_rh_0'], dmsg.GSProcessKillResponse)

    def start_sleepy_head(self, timeout):
        msg = mk_sleep_msg(timeout, self.next_tag(), p_uid=dfacts.LAUNCHER_PUID,
                           r_c_uid=dfacts.BASE_BE_CUID)

        self.gs_input_wh.send(msg.serialize())

        resp = tsu.get_and_check_type(self.names['bela_input_rh_0'], dmsg.GSProcessCreateResponse)

        self.assertEqual(resp.err, dmsg.GSProcessCreateResponse.Errors.SUCCESS)

        self.head_puid = resp.desc.p_uid
        dparm.this_process.my_puid = self.head_puid

    @unittest.skip("The teardown behavior is incorrect since SHHaltTA is sent to each node, not just primary.")
    def test_gs_ls_comm(self):
        self.start_multi_duts()
        if self.chatty_teardown:
            print('Everything is set up. Ready to fire up a few processes.', flush=True)
        self.start_sleepy_head(self.SLEEPY_HEAD_TIME)

        # create processes
        # GS will assign the creation of 2 processes to each LS
        # in a cyclic order
        procs = []
        for i in range(2 * self.nls):
            res = dproc.create(exe='/bin/sleep', run_dir='', args=['1'], env={})
            procs.append(res.p_uid)

        dproc.multi_join(procs, join_all=True)

        self.kill_sleepy_head()


if __name__ == "__main__":
    multiprocessing.set_start_method('spawn')
    unittest.main()
