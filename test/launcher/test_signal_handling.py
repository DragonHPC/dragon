#!/usr/bin/env python3
import os
import logging
import unittest
from unittest.mock import patch
import threading
from time import time

from dragon.launcher.launch_selector import determine_environment
from dragon.launcher.frontend import LauncherFrontEnd
from dragon.launcher.util import next_tag, SRQueue, get_with_timeout
from dragon.launcher.launchargs import get_args

from dragon.infrastructure.process_desc import ProcessDescriptor
from dragon.infrastructure.node_desc import NodeDescriptor
from dragon.infrastructure import facts as dfacts
from dragon.infrastructure import messages as dmsg
import dragon.utils as du

from dragon.managed_memory import DragonPoolError, DragonMemoryError
from dragon.channels import Channel, ChannelError
from dragon.utils import set_host_id, B64
from dragon.dlogging.util import setup_FE_logging

from .frontend_testing_mocks import open_overlay_comms, open_backend_comms


def send_beisup(nodes):
    '''Send valid beisup messages'''

    for host_id, node in nodes.items():
        be_up_msg = dmsg.BEIsUp(tag=next_tag(),
                                be_ch_desc=B64.bytes_to_str(node['ch_in'].serialize()),
                                host_id=host_id)
        node['conn'].send(be_up_msg.serialize())


def recv_fenodeidx(nodes):
    '''recv FENoe4deIdxBE and finish filling out node dictionary'''
    log = logging.getLogger('recv_fe_nodeidx')
    for node in nodes.values():
        fe_node_idx_msg = dmsg.parse(node['conn'].recv())
        assert isinstance(fe_node_idx_msg, dmsg.FENodeIdxBE), 'la_be node_index from fe expected'

        log.info(f'got FENodeIdxBE for index {fe_node_idx_msg.node_index}')
        node['node_index'] = fe_node_idx_msg.node_index
        if node['node_index'] < 0:
            raise RuntimeError("frontend giving bad node indices")
        node['is_primary'] = node['node_index'] == 0
        if node['is_primary']:
            primary_conn = node['conn']
        log.info(f'constructed be node: {node}')

    return primary_conn


def send_shchannelsup(nodes, mpool):

    log = logging.getLogger('send_shchannelsup')
    for host_id, node in nodes.items():
        ls_cuid = dfacts.shepherd_cuid_from_index(node['node_index'])
        ls_ch = Channel(mpool, ls_cuid)
        node['ls_ch'] = ls_ch

        if node['is_primary']:
            node['gs_ch'] = Channel(mpool, dfacts.GS_INPUT_CUID)
            gs_cd = du.B64.bytes_to_str(node['gs_ch'].serialize())
        else:
            node['gs_ch'] = None
            gs_cd = None

        node_desc = NodeDescriptor(host_name=node['hostname'],
                                   host_id=host_id,
                                   ip_addrs=node['ip_addrs'],
                                   shep_cd=B64.bytes_to_str(node['ls_ch'].serialize()))
        ch_up_msg = dmsg.SHChannelsUp(tag=next_tag(),
                                      node_desc=node_desc,
                                      gs_cd=gs_cd,
                                      idx=node['node_index'])
        log.info(f'construct SHChannelsUp: {ch_up_msg}')
        node['conn'].send(ch_up_msg.serialize())
        log.info(f'sent SHChannelsUp for {node["node_index"]}')

    log.info('sent all SHChannelsUp')


def recv_lachannelsinfo(nodes):
    '''Loop to recv all LAChannelsInfo messages in frontend bcast'''
    for host_id, node in nodes.items():
        la_channels_info_msg = dmsg.parse(node['conn'].recv())
        assert isinstance(la_channels_info_msg, dmsg.LAChannelsInfo), 'la_be expected all ls channels info from la_fe'


def send_taup(nodes):
    for host_id, node in nodes.items():
        ta_up = dmsg.TAUp(tag=next_tag(), idx=node['node_index'])
        node['conn'].send(ta_up.serialize())


def handle_gsprocesscreate(primary_conn, proc_create):
    '''Manage a valid response to GSProcessCreate'''

    # Send response
    gs_desc = ProcessDescriptor(p_uid=5000,  # Just a dummy value
                                name=proc_create.user_name,
                                node=0,
                                p_p_uid=proc_create.p_uid)
    response = dmsg.GSProcessCreateResponse(tag=next_tag(),
                                            ref=proc_create.tag,
                                            err=dmsg.GSProcessCreateResponse.Errors.SUCCESS,
                                            desc=gs_desc)
    primary_conn.send(response.serialize())

    # And go ahead and immediately exit because we're not actually doing anything
    primary_conn.send(dmsg.GSHeadExit(exit_code=0, tag=next_tag()).serialize())


def get_args_map(network_config, from_wlm=False):

    if from_wlm:
        arg_list = ['-t', 'tcp',
                    'launcher/helloworld.py']
    else:
        arg_list = ['--wlm', 'slurm',
                    '--network-config', f'{network_config}',
                    '--network-prefix', "",
                    'helloworld.py']
    args_map = get_args(arg_list)
    return args_map


def cleanup_mocks(fe_ta_conn,
                  be_ch_out, be_ch_in, be_nodes,
                  ls_ch,
                  ta_ch_out, ta_ch_in,
                  be_mpool):

    try:
        fe_ta_conn.close()
    except (AttributeError, ChannelError, ConnectionError):
        pass

    try:
        be_ch_out.detach()
    except (AttributeError, ChannelError):
        pass

    try:
        for node in be_nodes.values():
            try:
                node['conn'].close()
            except (KeyError, AttributeError, ConnectionError):
                pass

            try:
                node['ch_in'].destroy()
            except (KeyError, AttributeError, ChannelError):
                pass
    except AttributeError:
        pass

    try:
        for node in be_nodes.values():
            try:
                node['ls_ch'].destroy()
                if node['is_primary']:
                    node['gs_ch'].destroy()
            except (KeyError, AttributeError, ChannelError):
                pass
    except AttributeError:
        pass

    try:
        ta_ch_out.detach()
    except (AttributeError, ChannelError):
        pass

    try:
        ta_ch_in.detach()
    except (AttributeError, ChannelError):
        pass

    try:
        be_mpool.destroy()
    except (AttributeError, DragonMemoryError, DragonPoolError):
        pass


def run_frontend_supporting_mocks(mock_overlay=None,
                                  mock_launch=None,
                                  args_map=None,
                                  overlay_only=False,
                                  no_proc_create=False,
                                  hang_backend=False,
                                  hang_overlay=False,
                                  exit_at_fenodeidx=False,
                                  exit_queue=None):

    log = logging.getLogger('frontend mocks')

    be_mpool = None
    be_ch_out = None
    be_ch_in = None
    overlay_inout = None

    ls_ch = None

    ta_ch_in = None
    ta_ch_out = None
    fe_ta_conn = None
    be_nodes = None

    log.info("inside frontend mocks")
    # Get the mock's input args to the
    try:

        while mock_overlay.call_args is None:
            pass
        log.info("getting mock overlay args")
        overlay_args = mock_overlay.call_args.kwargs

        # Connect to overlay comms to talk to fronteend
        log.info("opening overlay comms overlay args")
        ta_ch_in, ta_ch_out, fe_ta_conn = open_overlay_comms(overlay_args['ch_in_sdesc'],
                                                             overlay_args['ch_out_sdesc'])

        # Let frontend know the overlay is "up"
        log.info("sending tapingsh")
        fe_ta_conn.send(dmsg.OverlayPingLA(next_tag()).serialize())

        if overlay_only:
            halt_on = dmsg.parse(fe_ta_conn.recv())
            assert isinstance(halt_on, dmsg.LAHaltOverlay)
            fe_ta_conn.send(dmsg.OverlayHalted(tag=next_tag()).serialize())

            raise Exception('exiting due to return_after_overlay_up being True')

        # Grab the frontend channel descriptor for the launched backend and
        # send it mine
        while mock_launch.call_args is None:
            pass
        launch_be_args = mock_launch.call_args.kwargs
        log.info(f'got be args: {launch_be_args}')

        mock_launch.wait.return_value = None
        log.info("set srun launch wait to None")

        # Connect to backend comms for frontend-to-backend and back comms
        be_mpool, be_ch_out, be_ch_in, be_nodes, overlay_inout = open_backend_comms(launch_be_args['frontend_sdesc'],
                                                                                  args_map['network_config'])
        log.info('got backend up')

        # Send BEIsUp
        send_beisup(be_nodes)
        log.info('send BEIsUp messages')

        # Recv FENodeIdxBE
        if not exit_at_fenodeidx:
            primary_conn = recv_fenodeidx(be_nodes)
            log.info('got all the FENodeIdxBE messages')
        else:
            log.info('told to return at FENodeIdx recv')
            cleanup_mocks(fe_ta_conn, be_ch_out, be_ch_in, be_nodes,
                          ls_ch, ta_ch_out, ta_ch_in, be_mpool)
            return

        # Fudge some SHChannelsUp messages
        send_shchannelsup(be_nodes, be_mpool)
        log.info(f'sent shchannelsup: {[node["gs_ch"].serialize() for node in be_nodes.values() if node["gs_ch"] is not None]}')

        # Receive LAChannelsInfo
        recv_lachannelsinfo(be_nodes)
        log.info('la_be received LAChannelsInfo')

        # Send TAUp
        send_taup(be_nodes)
        log.info('sent TAUp messages')

        # Send gs is up from primary
        primary_conn.send(dmsg.GSIsUp(tag=next_tag()).serialize())
        log.info('send GSIsUp')

        while True:
            gs_msg = None
            try:
                log.debug('doing gs get')
                gs_msg = get_with_timeout(primary_conn, timeout=0.01)
            except TimeoutError:
                log.debug('gs timeout')
                pass

            try:
                log.debug('doing exit get')
                _ = get_with_timeout(exit_queue, timeout=0.01)
                log.debug('cleaning up mocks')
                cleanup_mocks(fe_ta_conn, be_ch_out, be_ch_in, be_nodes,
                              ls_ch, ta_ch_out, ta_ch_in, be_mpool)
                return
            except TimeoutError:
                log.debug('exit timeout')
                pass

            if isinstance(gs_msg, dmsg.GSProcessCreate):
                log.info("handling GSProcessCreate on la_be")
                handle_gsprocesscreate(primary_conn, gs_msg)
            elif isinstance(gs_msg, dmsg.GSTeardown):
                log.info('la_be received GSTeardown')
                primary_conn.send(dmsg.GSHalted(tag=next_tag()).serialize())
                break

        nhalted = 0
        goal_halted = len(be_nodes)
        while True:

            for node in be_nodes.values():
                lmsg = None
                log.debug('posting mock recv')
                try:
                    lmsg = get_with_timeout(node['conn'], timeout=0.01)
                except TimeoutError:
                    pass

                try:
                    _ = get_with_timeout(exit_queue, timeout=0.01)
                    cleanup_mocks(fe_ta_conn, be_ch_out, be_ch_in, be_nodes,
                                  ls_ch, ta_ch_out, ta_ch_in, be_mpool)
                    return
                except TimeoutError:
                    pass

                if isinstance(lmsg, dmsg.SHHaltTA):
                    log.info('recvd SHHaltTA')
                    node['conn'].send(dmsg.TAHalted(tag=next_tag()).serialize())
                elif isinstance(lmsg, dmsg.SHTeardown):
                    log.info('recvd SHTeardown')
                    if hang_backend:
                        # Wait to tear everything down until I know I won't make everything go
                        # crazy
                        log.debug('waiting on exit queue signal')
                        while True:
                            log.debug('posting exit queue recv')
                            la_exit = exit_queue.recv(timeout=1)
                            if la_exit is not None:
                                break
                        log.debug('exit queue signal received')
                        cleanup_mocks(fe_ta_conn, be_ch_out, be_ch_in, be_nodes,
                                      ls_ch, ta_ch_out, ta_ch_in, be_mpool)
                        mock_launch.wait.return_value = 2
                        log.debug('returning early from mocks')
                        return
                    else:
                        node['conn'].send(dmsg.SHHaltBE(tag=next_tag()).serialize())

                elif isinstance(lmsg, dmsg.BEHalted):
                    log.info("recvd BEHalted")
                    nhalted += 1
                    log.info(f'inner nhalted ({nhalted}) v. goal_halted ({goal_halted})')
                    if nhalted == goal_halted:
                        log.info('breaking out of first BEHalted')
                        break

            log.info(f'nhalted ({nhalted}) v. goal_halted ({goal_halted})')
            if nhalted == goal_halted:
                log.info('breaking out of second BEHalted')
                break

        # Handle the final TAHalted for overlay
        log.info('posting recv for FE overlay')
        halt_on = dmsg.parse(fe_ta_conn.recv())
        assert isinstance(halt_on, dmsg.LAHaltOverlay), 'was excpecting an SHHaltTA'
        if hang_overlay:
            # Wait to tear everything down until I know I won't make everything go
            # crazy
            log.debug('waiting on exit queue signal')
            _ = exit_queue.recv()
            log.debug('exit queue signal received')
            cleanup_mocks(fe_ta_conn, be_ch_out, be_ch_in, be_nodes,
                          ls_ch, ta_ch_out, ta_ch_in, be_mpool)
            mock_launch.wait.return_value = 2
            log.debug('breaking out of mocks before halting overlay')
            return
        else:
            fe_ta_conn.send(dmsg.OverlayHalted(tag=next_tag()).serialize())
    except Exception as e:
        log.info(f'hit frontend mocks exception: {e}')
        pass

        if mock_launch is not None:
            mock_launch.wait.return_value = None

    cleanup_mocks(fe_ta_conn, be_ch_out, be_ch_in, be_nodes,
                  ls_ch, ta_ch_out, ta_ch_in, be_mpool)

    # Let the test know we're done:
    if mock_launch is not None:
        mock_launch.wait.return_value = 0

    log.info('returning from frontends mock')


class SigIntTest(unittest.TestCase):

    def setUp(self):

        self.test_dir = os.path.dirname(os.path.realpath(__file__))
        self.network_config = os.path.join(self.test_dir, 'slurm_primary.yaml')

        self.be_mpool = None
        self.be_ch_out = None
        self.be_ch_in = None
        self.overlay_inout = None

        self.ls_ch = None

        self.ta_ch_in = None
        self.ta_ch_out = None
        self.fe_ta_conn = None

        self.args_map = get_args_map(self.network_config, from_wlm=False)
        self.wlm_args_map = get_args_map(self.network_config, from_wlm=True)

        self.exit_queue = SRQueue()

        # Some tests will only work in a multinode environment with
        # an activate allocation
        try:
            self.multi_mode = determine_environment(self.wlm_args_map)
        except RuntimeError:
            self.multi_mode = False
            pass

        # Before doing anything set my host ID
        set_host_id(dfacts.FRONTEND_HOSTID)
        setup_FE_logging(log_device_level_map=self.args_map['log_device_level_map'],
                         basename='dragon', basedir=os.getcwd())
        log = logging.getLogger('sigint setUp')
        for key, value in self.args_map.items():
            if value is not None:
                log.info(f'args_map: {key}: {value}')

    def tearDown(self):

        if self.fe_ta_conn is not None:
            self.fe_ta_conn.close()

        if self.be_ch_out is not None:
            self.be_ch_out.detach()

        if self.be_ch_in is not None:
            for node in self.be_nodes.values():
                node['conn'].close()
                node['ch_in'].destroy()

        if self.ls_ch is not None:
            for node in self.be_nodes.values():
                node['ls_ch'].destroy()

                if node['is_primary']:
                    node['gs_ch'].destroy()

        if self.ta_ch_out is not None:
            self.ta_ch_out.detach()

        if self.ta_ch_in is not None:
            self.ta_ch_in.detach()

        if self.be_mpool is not None:
            self.be_mpool.destroy()

    @patch('dragon.launcher.frontend.LauncherFrontEnd._launch_backend')
    @patch('dragon.launcher.frontend.start_overlay_network')
    def test_clean_exit(self, mock_overlay, mock_be_launch):
        '''Test a clean bring-up and teardown'''

        # Start my mocks support the frontend in a background thread()
        mock_args = {'mock_overlay': mock_overlay,
                     'mock_launch': mock_be_launch,
                     'args_map': self.args_map,
                     'overlay_only': False,
                     'no_proc_create': False,
                     'exit_queue': self.exit_queue}

        mock_procs = threading.Thread(name='Frontend Supporting Mocks',
                                      target=run_frontend_supporting_mocks,
                                      kwargs=mock_args,
                                      daemon=False)

        mock_procs.start()

        fe_server = LauncherFrontEnd(args_map=self.args_map)
        fe_server.run_startup()
        fe_server.run_app()
        fe_server.run_msg_server()

        mock_procs.join()

    @patch('dragon.launcher.frontend.LauncherFrontEnd._launch_backend')
    @patch('dragon.launcher.frontend.start_overlay_network')
    def test_sig_0(self, mock_overlay, mock_be_launch):
        '''Test SIGINT being raised at case 0 in launcher'''

        sigint_trigger = 0
        fe_server = LauncherFrontEnd(args_map=self.args_map,
                                     sigint_trigger=sigint_trigger)

        with self.assertRaises(KeyboardInterrupt):
            fe_server.run_startup()
            fe_server.run_app()
            fe_server.run_msg_server()

    @patch('dragon.launcher.frontend.LauncherFrontEnd._launch_backend')
    @patch('dragon.launcher.frontend.start_overlay_network')
    def test_sig_1(self, mock_overlay, mock_be_launch):
        '''Test SIGINT being raised at case 1 in launcher'''

        sigint_trigger = 1
        fe_server = LauncherFrontEnd(args_map=self.args_map,
                                     sigint_trigger=sigint_trigger)

        with self.assertRaises(KeyboardInterrupt):
            fe_server.run_startup()
            fe_server.run_app()
            fe_server.run_msg_server()

    def test_sig_1_no_mock(self):
        '''Test SIGINT with full runtime being raised at case 1 in launcher'''

        if self.multi_mode:
            sigint_trigger = 1
            fe_server = LauncherFrontEnd(args_map=self.wlm_args_map,
                                         sigint_trigger=sigint_trigger)

            with self.assertRaises(KeyboardInterrupt):
                fe_server.run_startup()
                fe_server.run_app()
                fe_server.run_msg_server()
        else:
            print("Unable to run. Requires WLM job allocation")

    def test_sig_minus_1(self):
        '''Test SIGINT with full runtime being raised at case -1 in launcher'''

        if self.multi_mode:

            sigint_trigger = -1
            fe_server = LauncherFrontEnd(args_map=self.wlm_args_map,
                                         sigint_trigger=sigint_trigger)
            with self.assertRaises(KeyboardInterrupt):
                fe_server.run_startup()
                fe_server.run_app()
                fe_server.run_msg_server()

        else:
            print("Unable to run. Requires WLM job allocation")

    def test_sig_minus_2(self):
        '''Test SIGINT with full runtime being raised at case -2 in launcher'''
        if self.multi_mode:
            sigint_trigger = -2
            fe_server = LauncherFrontEnd(args_map=self.wlm_args_map,
                                         sigint_trigger=sigint_trigger)

            with self.assertRaises(KeyboardInterrupt):
                fe_server.run_startup()
                fe_server.run_app()
                fe_server.run_msg_server()

        else:
            print("Unable to run. Requires WLM job allocation")

    @patch('dragon.launcher.frontend.LauncherFrontEnd._launch_backend')
    @patch('dragon.launcher.frontend.start_overlay_network')
    def test_sig_2(self, mock_overlay, mock_be_launch):
        '''Test SIGINT being raised at case 2 in launcher'''

        sigint_trigger = 2
        fe_server = LauncherFrontEnd(args_map=self.args_map,
                                     sigint_trigger=sigint_trigger)

        with self.assertRaises(KeyboardInterrupt):
            fe_server.run_startup()

    def test_sig_2_no_mock(self):
        '''Test SIGINT with full runtime being raised at case 2 in launcher'''
        if self.multi_mode:
            sigint_trigger = 2
            fe_server = LauncherFrontEnd(args_map=self.wlm_args_map,
                                         sigint_trigger=sigint_trigger)

            with self.assertRaises(KeyboardInterrupt):
                fe_server.run_startup()
                fe_server.run_app()
                fe_server.run_msg_server()
        else:
            print("Unable to run. Requires WLM job allocation")

    @patch('dragon.launcher.frontend.LauncherFrontEnd._launch_backend')
    @patch('dragon.launcher.frontend.start_overlay_network')
    def test_sig_3(self, mock_overlay, mock_be_launch):
        '''Test SIGINT being raised at case 3 in launcher'''

        # Start my mocks support the frontend in a background thread()
        mock_args = {'mock_overlay': mock_overlay,
                     'mock_launch': mock_be_launch,
                     'args_map': self.args_map,
                     'overlay_only': True,
                     'no_proc_create': False,
                     'exit_queue': self.exit_queue}

        mock_procs = threading.Thread(name='Frontend Supporting Mocks',
                                      target=run_frontend_supporting_mocks,
                                      kwargs=mock_args,
                                      daemon=False)

        mock_procs.start()

        sigint_trigger = 3
        fe_server = LauncherFrontEnd(args_map=self.args_map,
                                     sigint_trigger=sigint_trigger)

        with self.assertRaises(KeyboardInterrupt):
            fe_server.run_startup()

        mock_procs.join()

    def test_sig_3_no_mock(self):
        '''Test SIGINT with full runtime being raised at case 3 in launcher'''
        if self.multi_mode:
            sigint_trigger = 3
            fe_server = LauncherFrontEnd(args_map=self.wlm_args_map,
                                         sigint_trigger=sigint_trigger)

            with self.assertRaises(KeyboardInterrupt):
                fe_server.run_startup()
                fe_server.run_app()
                fe_server.run_msg_server()
        else:
            print("Unable to run. Requires WLM job allocation")

    @patch('dragon.launcher.frontend.LauncherFrontEnd._launch_backend')
    @patch('dragon.launcher.frontend.start_overlay_network')
    def test_sig_4(self, mock_overlay, mock_be_launch):
        '''Test SIGINT being raised at case 4 in launcher'''

        # Start my mocks support the frontend in a background thread()
        mock_args = {'mock_overlay': mock_overlay,
                     'mock_launch': mock_be_launch,
                     'args_map': self.args_map,
                     'overlay_only': True,
                     'no_proc_create': False,
                     'exit_queue': self.exit_queue}

        mock_procs = threading.Thread(name='Frontend Supporting Mocks',
                                      target=run_frontend_supporting_mocks,
                                      kwargs=mock_args,
                                      daemon=False)

        mock_procs.start()

        sigint_trigger = 4
        fe_server = LauncherFrontEnd(args_map=self.args_map,
                                     sigint_trigger=sigint_trigger)

        with self.assertRaises(KeyboardInterrupt):
            fe_server.run_startup()
            fe_server.run_app()
            fe_server.run_msg_server()

        mock_procs.join()

    def test_sig_4_no_mock(self):
        '''Test SIGINT with full runtime being raised at case 4 in launcher'''
        if self.multi_mode:
            sigint_trigger = 4
            fe_server = LauncherFrontEnd(args_map=self.wlm_args_map,
                                         sigint_trigger=sigint_trigger)

            with self.assertRaises(KeyboardInterrupt):
                fe_server.run_startup()
                fe_server.run_app()
                fe_server.run_msg_server()
        else:
            print("Unable to run. Requires WLM job allocation")

    @patch('dragon.launcher.frontend.LauncherFrontEnd._launch_backend')
    @patch('dragon.launcher.frontend.start_overlay_network')
    def test_sig_5(self, mock_overlay, mock_be_launch):
        '''Test SIGINT being raised at case 5 in launcher'''

        # Start my mocks support the frontend in a background thread()
        mock_args = {'mock_overlay': mock_overlay,
                     'mock_launch': mock_be_launch,
                     'args_map': self.args_map,
                     'overlay_only': False,
                     'no_proc_create': True,
                     'exit_at_fenodeidx': True,
                     'exit_queue': self.exit_queue}

        mock_procs = threading.Thread(name='Frontend Supporting Mocks',
                                      target=run_frontend_supporting_mocks,
                                      kwargs=mock_args,
                                      daemon=False)

        mock_procs.start()

        sigint_trigger = 5
        fe_server = LauncherFrontEnd(args_map=self.args_map,
                                     sigint_trigger=sigint_trigger)

        with self.assertRaises(KeyboardInterrupt):
            fe_server.run_startup()
            fe_server.run_app()
            fe_server.run_msg_server()

        self.exit_queue.send(dmsg.LAExit(tag=next_tag()).serialize())
        mock_procs.join()

    def test_sig_5_no_mock(self):
        '''Test SIGINT with full runtime being raised at case 5 in launcher'''
        if self.multi_mode:
            sigint_trigger = 5
            fe_server = LauncherFrontEnd(args_map=self.wlm_args_map,
                                         sigint_trigger=sigint_trigger)

            with self.assertRaises(KeyboardInterrupt):
                fe_server.run_startup()
                fe_server.run_app()
                fe_server.run_msg_server()
        else:
            print("Unable to run. Requires WLM job allocation")

    @patch('dragon.launcher.frontend.LauncherFrontEnd._launch_backend')
    @patch('dragon.launcher.frontend.start_overlay_network')
    def test_sig_6(self, mock_overlay, mock_be_launch):
        '''Test SIGINT being raised at case 6 in launcher'''

        # Start my mocks support the frontend in a background thread()
        mock_args = {'mock_overlay': mock_overlay,
                     'mock_launch': mock_be_launch,
                     'args_map': self.args_map,
                     'overlay_only': False,
                     'no_proc_create': False,
                     'exit_queue': self.exit_queue}

        mock_procs = threading.Thread(name='Frontend Supporting Mocks',
                                      target=run_frontend_supporting_mocks,
                                      kwargs=mock_args,
                                      daemon=False)

        mock_procs.start()

        sigint_trigger = 6
        fe_server = LauncherFrontEnd(args_map=self.args_map,
                                     sigint_trigger=sigint_trigger)

        with self.assertRaises(KeyboardInterrupt):
            fe_server.run_startup()
            fe_server.run_app()
            fe_server.run_msg_server()
        log = logging.getLogger('test_sig_5')
        self.exit_queue.send(dmsg.LAExit(tag=next_tag()).serialize())
        mock_procs.join()

    def test_sig_6_no_mock(self):
        '''Test SIGINT with full runtime being raised at case 6 in launcher'''
        if self.multi_mode:
            sigint_trigger = 6
            fe_server = LauncherFrontEnd(args_map=self.wlm_args_map,
                                         sigint_trigger=sigint_trigger)

            with self.assertRaises(KeyboardInterrupt):
                fe_server.run_startup()
                fe_server.run_app()
                fe_server.run_msg_server()
        else:
            print("Unable to run. Requires WLM job allocation")

    @patch('dragon.launcher.frontend.LauncherFrontEnd._launch_backend')
    @patch('dragon.launcher.frontend.start_overlay_network')
    def test_sig_7(self, mock_overlay, mock_be_launch):
        '''Test SIGINT being raised at case 7 in launcher'''

        # Start my mocks support the frontend in a background thread()
        mock_args = {'mock_overlay': mock_overlay,
                     'mock_launch': mock_be_launch,
                     'args_map': self.args_map,
                     'overlay_only': False,
                     'no_proc_create': False,
                     'exit_queue': self.exit_queue}

        mock_procs = threading.Thread(name='Frontend Supporting Mocks',
                                      target=run_frontend_supporting_mocks,
                                      kwargs=mock_args,
                                      daemon=False)

        mock_procs.start()

        sigint_trigger = 7
        fe_server = LauncherFrontEnd(args_map=self.args_map,
                                     sigint_trigger=sigint_trigger)

        log = logging.getLogger('test_sig_7')
        log.debug('entering main test')
        with self.assertRaises(KeyboardInterrupt):
            fe_server.run_startup()
            fe_server.run_app()
            fe_server.run_msg_server()

        self.exit_queue.send(dmsg.LAExit(tag=next_tag()).serialize())
        mock_procs.join()

    def test_sig_7_no_mock(self):
        '''Test SIGINT with full runtime being raised at case 7 in launcher'''
        if self.multi_mode:
            sigint_trigger = 7
            fe_server = LauncherFrontEnd(args_map=self.wlm_args_map,
                                         sigint_trigger=sigint_trigger)

            with self.assertRaises(KeyboardInterrupt):
                fe_server.run_startup()
                fe_server.run_app()
                fe_server.run_msg_server()
        else:
            print("Unable to run. Requires WLM job allocation")

    @patch('dragon.launcher.frontend.LauncherFrontEnd._launch_backend')
    @patch('dragon.launcher.frontend.start_overlay_network')
    def test_sigint_hung_backend(self, mock_overlay, mock_be_launch):
        '''Test SIGINT being raised at case 7 in launcher with a hanging backend'''

        # Start my mocks supporting the frontend in a background thread()
        mock_args = {'mock_overlay': mock_overlay,
                     'mock_launch': mock_be_launch,
                     'args_map': self.args_map,
                     'overlay_only': False,
                     'no_proc_create': False,
                     'hang_backend': True,
                     'hang_overlay': False,
                     'exit_queue': self.exit_queue}

        mock_procs = threading.Thread(name='Frontend Supporting Mocks',
                                      target=run_frontend_supporting_mocks,
                                      kwargs=mock_args,
                                      daemon=False)

        mock_procs.start()

        sigint_trigger = 7
        fe_server = LauncherFrontEnd(args_map=self.args_map,
                                     sigint_trigger=sigint_trigger)

        with self.assertRaises(KeyboardInterrupt):
            fe_server.run_startup()
            fe_server.run_app()
            fe_server.run_msg_server()

        # Let the mock thread know to cleanup:
        log = logging.getLogger('test_sigint_hung_backend')
        log.debug('sending exit signal')
        self.exit_queue.send(dmsg.LAExit(tag=next_tag()).serialize())

        mock_procs.join()

    @patch('dragon.launcher.frontend.LauncherFrontEnd._launch_backend')
    @patch('dragon.launcher.frontend.start_overlay_network')
    def test_sigint_hung_overlay(self, mock_overlay, mock_be_launch):
        '''Test SIGINT being raised at case 7 in launcher with a hanging overlay'''

        # Start my mocks supporting the frontend in a background thread
        mock_args = {'mock_overlay': mock_overlay,
                     'mock_launch': mock_be_launch,
                     'args_map': self.args_map,
                     'overlay_only': False,
                     'no_proc_create': False,
                     'hang_backend': False,
                     'hang_overlay': True,
                     'exit_queue': self.exit_queue}

        mock_procs = threading.Thread(name='Frontend Supporting Mocks',
                                      target=run_frontend_supporting_mocks,
                                      kwargs=mock_args,
                                      daemon=False)

        mock_procs.start()

        sigint_trigger = 7
        fe_server = LauncherFrontEnd(args_map=self.args_map,
                                     sigint_trigger=sigint_trigger)

        with self.assertRaises(KeyboardInterrupt):
            fe_server.run_startup()
            fe_server.run_app()
            fe_server.run_msg_server()

        # Let the mock thread know to cleanup:
        self.exit_queue.send(dmsg.LAExit(tag=next_tag()).serialize())

        mock_procs.join()

    @unittest.skip("Skipped pending fix of problem outlined in CIRRUS-1922. This test terminates too much and sometimes kills Docker container.")
    @patch('dragon.launcher.frontend.LauncherFrontEnd._launch_backend')
    @patch('dragon.launcher.frontend.start_overlay_network')
    def test_rapid_sigint(self, mock_overlay, mock_be_launch):
        '''Test SIGINT being raised at case 8 -- handles 2 SIGINT signals transmitted'''

        # Start my mocks support the frontend in a background thread()
        mock_args = {'mock_overlay': mock_overlay,
                     'mock_launch': mock_be_launch,
                     'args_map': self.args_map,
                     'overlay_only': False,
                     'no_proc_create': True,
                     'exit_at_fenodeidx': True,
                     'exit_queue': self.exit_queue}

        mock_procs = threading.Thread(name='Frontend Supporting Mocks',
                                      target=run_frontend_supporting_mocks,
                                      kwargs=mock_args,
                                      daemon=False)

        mock_procs.start()

        sigint_trigger = 8
        fe_server = LauncherFrontEnd(args_map=self.args_map,
                                     sigint_trigger=sigint_trigger)

        start = time()
        with self.assertRaises(KeyboardInterrupt):
            fe_server.run_startup()
            fe_server.run_app()
            fe_server.run_msg_server()

        # this test should exit much more quickly since we're triggering the
        # quick exit via 2 SIGINTs and should be faster than the 5 second
        # default timeout
        self.assertLessEqual(time() - start, 5.0)

        self.exit_queue.send(dmsg.LAExit(tag=next_tag()).serialize())
        mock_procs.join()

    def test_rapid_sigint_no_mock(self):
        '''Test SIGINT being raised at case 8 -- handles 2 SIGINT signals transmitted. No mocks'''

        if self.multi_mode:
            sigint_trigger = 8
            fe_server = LauncherFrontEnd(args_map=self.wlm_args_map,
                                         sigint_trigger=sigint_trigger)

            with self.assertRaises(KeyboardInterrupt):
                fe_server.run_startup()
                fe_server.run_app()
                fe_server.run_msg_server()
        else:
            print("Unable to run. Requires WLM job allocation")

    @unittest.skip("Skipped pending fix of problem outlined in CIRRUS-1922. This test terminates too much and sometimes kills Docker container.")
    @patch('dragon.launcher.frontend.LauncherFrontEnd._launch_backend')
    @patch('dragon.launcher.frontend.start_overlay_network')
    def test_teardown_with_hung_backend_sigint(self, mock_overlay, mock_be_launch):
        '''Test SIGINT during a clean teardown to trigger a quick exit'''

        mock_args = {'mock_overlay': mock_overlay,
                     'mock_launch': mock_be_launch,
                     'args_map': self.args_map,
                     'overlay_only': False,
                     'hang_backend': True,
                     'exit_queue': self.exit_queue}

        mock_procs = threading.Thread(name='Frontend Supporting Mocks',
                                      target=run_frontend_supporting_mocks,
                                      kwargs=mock_args,
                                      daemon=False)

        mock_procs.start()

        sigint_trigger = 9
        fe_server = LauncherFrontEnd(args_map=self.args_map,
                                     sigint_trigger=sigint_trigger)

        start = time()
        with self.assertRaises(KeyboardInterrupt):
            fe_server.run_startup()
            fe_server.run_app()
            fe_server.run_msg_server()

        self.assertLessEqual(time() - start, 5.0)

        # Let the mock thread know to cleanup:
        self.exit_queue.send(dmsg.LAExit(tag=next_tag()).serialize())

        mock_procs.join()


if __name__ == "__main__":
    unittest.main()
