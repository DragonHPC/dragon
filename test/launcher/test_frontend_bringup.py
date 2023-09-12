#!/usr/bin/env python3
import os
import logging
import random
import string
import unittest
import threading
import json
from subprocess import TimeoutExpired
from unittest.mock import patch

from dragon.launcher.util import next_tag
from dragon.launcher.launchargs import get_parser
from dragon.launcher.network_config import NetworkConfig

from dragon.infrastructure import messages as dmsg
from dragon.launcher.wlm import WLM
from dragon.channels import ChannelError
from dragon.managed_memory import DragonMemoryError
from dragon.utils import B64
from dragon.infrastructure.facts import DEFAULT_TRANSPORT_NETIF, DEFAULT_OVERLAY_NETWORK_PORT, TransportAgentOptions

from .launcher_testing_utils import catch_thread_exceptions

from .frontend_testing_mocks import run_frontend, open_backend_comms, open_overlay_comms
from .frontend_testing_mocks import send_beisup, handle_teardown, recv_fenodeidx, send_shchannelsup, recv_lachannelsinfo
from .frontend_testing_mocks import handle_gsprocesscreate, handle_bringup, stand_up_backend, handle_overlay_teardown
from .frontend_testing_mocks import send_abnormal_term


def get_args_map(network_config, **kwargs):

    parser = get_parser()
    arg_list = ['--wlm', 'slurm',
                '--network-config', f'{network_config}',
                '--network-prefix', '^(eth|hsn)']
    for val in kwargs.values():
        arg_list = arg_list + val

    arg_list.append('hello_world.py')

    args = parser.parse_args(args=arg_list)
    if args.basic_label or args.verbose_label:
        args.no_label = False
    args_map = {key: value for key, value in vars(args).items() if value is not None}

    return args_map


class FrontendBringUpTeardownTest(unittest.TestCase):

    def setUp(self):
        self.test_dir = os.path.dirname(os.path.realpath(__file__))
        self.network_config = os.path.join(self.test_dir, 'slurm_primary.yaml')
        self.bad_network_config = os.path.join(self.test_dir, 'slurm_bad.yaml')

        self.be_mpool = None
        self.be_ch_out = None
        self.be_ch_in = None
        self.overlay_inout = None

        self.ls_ch = None

        self.ta_ch_in = None
        self.ta_ch_out = None
        self.fe_ta_conn = None

    def tearDown(self):

        try:
            self.fe_ta_conn.close()
        except (ConnectionError, AttributeError):
            pass

        try:
            self.be_ch_out.detach()
        except (ChannelError, AttributeError):
            pass

        try:
            for node in self.be_nodes.values():
                node['conn'].close()
                node['ch_in'].destroy()
        except (AttributeError, ChannelError):
            pass

        try:
            for node in self.be_nodes.values():
                node['ls_ch'].destroy()
                if node['is_primary']:
                    node['gs_ch'].destroy()
        except (AttributeError, ChannelError, KeyError):
            pass

        try:
            self.ta_ch_out.detach()
        except (AttributeError, ChannelError):
            pass

        try:
            self.ta_ch_in.detach()
        except (AttributeError, ChannelError):
            pass

        try:
            self.be_mpool.destroy()
            del self.be_mpool
        except (AttributeError, DragonMemoryError):
            pass

    def do_bringup(self, mock_overlay, mock_launch):

        overlay, la_info = handle_bringup(mock_overlay, mock_launch, self.network_config)
        self.ta_ch_in = overlay['ta_ch_in']
        self.ta_ch_out = overlay['ta_ch_out']
        self.fe_ta_conn = overlay['fe_ta_conn']
        self.be_mpool = overlay['be_mpool']
        self.be_ch_out = overlay['be_ch_out']
        self.be_ch_in = overlay['be_ch_in']
        self.be_nodes = overlay['be_nodes']
        self.overlay_inout = overlay['overlay_inout']
        self.primary_conn = overlay['primary_conn']

        return la_info

    def get_backend_up(self, mock_overlay, mock_launch):

        overlay = stand_up_backend(mock_overlay, mock_launch, self.network_config)
        self.ta_ch_in = overlay['ta_ch_in']
        self.ta_ch_out = overlay['ta_ch_out']
        self.fe_ta_conn = overlay['fe_ta_conn']
        self.be_mpool = overlay['be_mpool']
        self.be_ch_out = overlay['be_ch_out']
        self.be_ch_in = overlay['be_ch_in']
        self.be_nodes = overlay['be_nodes']
        self.overlay_inout = overlay['overlay_inout']

    @patch('dragon.launcher.frontend.LauncherFrontEnd._launch_backend')
    @patch('dragon.launcher.frontend.start_overlay_network')
    def test_clean_exit(self, mock_overlay, mock_launch):
        '''Test a clean bring-up and teardown'''

        args_map = get_args_map(self.network_config)

        # get startup going in another thread. Note: need to do threads in order to use
        # all our mocks
        fe_proc = threading.Thread(name='Frontend Server',
                                   target=run_frontend,
                                   args=(args_map,),
                                   daemon=False)
        fe_proc.start()

        # Get backend up
        la_info = self.do_bringup(mock_overlay, mock_launch)

        # Check we launched the backend with default transport
        self.assertEqual(la_info.transport, TransportAgentOptions.TCP)

        # Receive GSProcessCreate
        handle_gsprocesscreate(self.primary_conn)

        # Send GSHalted
        handle_teardown(self.be_nodes, self.primary_conn, self.fe_ta_conn)

        # Join on the frontend thread
        fe_proc.join()

    @patch('dragon.launcher.frontend.LauncherFrontEnd._launch_backend')
    @patch('dragon.launcher.frontend.start_overlay_network')
    def test_clean_exit_with_tcp_launch(self, mock_overlay, mock_launch):
        '''Test a clean bring-up and teardown using TCP agent'''

        args_map = get_args_map(self.network_config,
                                arg1=['-t', 'tcp'])

        # get startup going in another thread. Note: need to do threads in order to use
        # all our mocks
        fe_proc = threading.Thread(name='Frontend Server',
                                   target=run_frontend,
                                   args=(args_map,),
                                   daemon=False)
        fe_proc.start()

        # Get backend up
        la_info = self.do_bringup(mock_overlay, mock_launch)

        # Check we launched the backend with default transport
        self.assertEqual(la_info.transport, TransportAgentOptions.TCP)

        # Receive GSProcessCreate
        handle_gsprocesscreate(self.primary_conn)

        # Send GSHalted
        handle_teardown(self.be_nodes, self.primary_conn, self.fe_ta_conn)

        # Join on the frontend thread
        fe_proc.join()

    @patch('dragon.launcher.frontend.LauncherFrontEnd._launch_backend')
    @patch('dragon.launcher.frontend.start_overlay_network')
    def test_default_transport(self, mock_overlay, mock_launch):
        '''Test a clean bring-up and teardown using default transport (TCP)'''

        args_map = get_args_map(self.network_config)

        # get startup going in another thread. Note: need to do threads in order to use
        # all our mocks
        fe_proc = threading.Thread(name='Frontend Server',
                                   target=run_frontend,
                                   args=(args_map,),
                                   daemon=False)
        fe_proc.start()

        # Get backend up
        self.do_bringup(mock_overlay, mock_launch)

        # Receive GSProcessCreate
        handle_gsprocesscreate(self.primary_conn)

        # Send GSHalted
        handle_teardown(self.be_nodes, self.primary_conn, self.fe_ta_conn)

        # Join on the frontend thread
        fe_proc.join()

    @catch_thread_exceptions
    @patch('dragon.launcher.frontend.LauncherFrontEnd._launch_backend')
    @patch('dragon.launcher.frontend.start_overlay_network')
    def test_bad_network_config(self, exceptions_caught_in_threads, mock_overlay, mock_launch):
        '''Test with an invalid network config'''

        args_map = get_args_map(self.bad_network_config)

        # get startup going in another thread. Note: need to do threads in order to use
        # all our mocks
        fe_proc = threading.Thread(name='Frontend Server',
                                   target=run_frontend,
                                   args=(args_map,),
                                   daemon=False)

        fe_proc.start()
        fe_proc.join()

        logging.info(f"exception: {exceptions_caught_in_threads}")
        assert 'Frontend Server' in exceptions_caught_in_threads  # there was an exception in thread  1
        assert exceptions_caught_in_threads['Frontend Server']['exception']['type'] == RuntimeError
        assert str(exceptions_caught_in_threads['Frontend Server']['exception']['value']) == 'Unable to acquire backend network configuration from input file.'

    @catch_thread_exceptions
    @patch('dragon.launcher.frontend.LauncherFrontEnd._launch_backend')
    @patch('dragon.launcher.frontend.start_overlay_network')
    def test_launch_backend_exception(self, exceptions_caught_in_threads, mock_overlay, mock_launch):
        '''Test unable to Popen backend launcher'''

        all_chars = string.ascii_letters + string.digits + string.punctuation
        exception_text = ''.join(random.choices(all_chars, k=64))
        mock_launch.side_effect = RuntimeError(exception_text)

        args_map = get_args_map(self.network_config)

        # get startup going in another thread. Note: need to do threads in order to use
        # all our mocks
        fe_proc = threading.Thread(name='Frontend Server',
                                   target=run_frontend,
                                   args=(args_map,),
                                   daemon=False)

        fe_proc.start()

        # Get the mock's input args to the
        while mock_overlay.call_args is None:
            pass
        overlay_args = mock_overlay.call_args.kwargs

        # Connect to overlay comms to talk to fronteend
        self.ta_ch_in, self.ta_ch_out, self.fe_ta_conn = open_overlay_comms(overlay_args['ch_in_sdesc'],
                                                                            overlay_args['ch_out_sdesc'])
        # Let frontend know the overlay is "up"
        self.fe_ta_conn.send(dmsg.OverlayPingLA(next_tag()).serialize())

        handle_overlay_teardown(self.fe_ta_conn)

        fe_proc.join()

        assert mock_launch.call_count == 1
        assert 'Frontend Server' in exceptions_caught_in_threads  # there was an exception in thread  1
        assert exceptions_caught_in_threads['Frontend Server']['exception']['type'] == RuntimeError
        assert str(exceptions_caught_in_threads['Frontend Server']['exception']['value']) == "Backend launch failed from launcher frontend"

    @catch_thread_exceptions
    @patch('dragon.launcher.frontend.LauncherFrontEnd._launch_backend')
    @patch('dragon.launcher.frontend.start_overlay_network')
    def test_launch_overlay_exception(self, exceptions_caught_in_threads, mock_overlay, mock_launch):
        '''Test exception during launch of overlay network'''

        all_chars = string.ascii_letters + string.digits + string.punctuation
        exception_text = ''.join(random.choices(all_chars, k=64))
        mock_overlay.side_effect = RuntimeError(exception_text)

        args_map = get_args_map(self.network_config)

        # get startup going in another thread. Note: need to do threads in order to use
        # all our mocks
        fe_proc = threading.Thread(name='Frontend Server',
                                   target=run_frontend,
                                   args=(args_map,),
                                   daemon=False)

        fe_proc.start()
        fe_proc.join()

        assert mock_overlay.call_count == 1
        assert 'Frontend Server' in exceptions_caught_in_threads  # there was an exception in thread  1
        assert exceptions_caught_in_threads['Frontend Server']['exception']['type'] == RuntimeError
        assert str(exceptions_caught_in_threads['Frontend Server']['exception']['value']) \
            == 'Overlay transport agent launch failed on launcher frontend'

    @catch_thread_exceptions
    @patch('dragon.launcher.frontend.LauncherFrontEnd._launch_backend')
    @patch('dragon.launcher.frontend.start_overlay_network')
    def test_message_out_of_order(self, exceptions_caught_in_threads, mock_overlay, mock_launch):
        '''Test message out of order'''

        log = logging.getLogger('msg_out_of_order')
        args_map = get_args_map(self.network_config)

        # get startup going in another thread. Note: need to do threads in order to use
        # all our mocks
        fe_proc = threading.Thread(name='Frontend Server',
                                   target=run_frontend,
                                   args=(args_map,),
                                   daemon=False)
        fe_proc.start()

        # Get the mock's input args to the
        while mock_overlay.call_args is None:
            pass
        overlay_args = mock_overlay.call_args.kwargs

        # Connect to overlay comms to talk to fronteend
        self.ta_ch_in, self.ta_ch_out, self.fe_ta_conn = open_overlay_comms(overlay_args['ch_in_sdesc'],
                                                                            overlay_args['ch_out_sdesc'])

        # Let frontend know the overlay is "up"
        self.fe_ta_conn.send(dmsg.OverlayPingLA(next_tag()).serialize())

        # Grab the frontend channel descriptor for the launched backend and
        # send it mine
        while mock_launch.call_args is None:
            pass
        launch_be_args = mock_launch.call_args.kwargs
        log.info(f'got be args: {launch_be_args}')

        # Connect to backend comms for frontend-to-backend and back comms
        self.be_mpool, self.be_ch_out, self.be_ch_in, self.be_nodes, self.overlay_inout = open_backend_comms(launch_be_args['frontend_sdesc'],
                                                                                                           self.network_config)
        log.info('got backend up')

        # Send BEIsUp
        send_beisup(self.be_nodes)
        log.info('send BEIsUp messages')

        # Recv FENodeIdxBE
        recv_fenodeidx(self.be_nodes)
        log.info('got all the FENodeIdxBE messages')

        # The Dragon FrontEnd is expecting us to some SHChannelsUp messages.
        # Instead, we're going to send a message that the FE is not expecting.

        # Send BEIsUp
        send_beisup(self.be_nodes)
        log.info('send BEIsUp messages')

        handle_overlay_teardown(self.fe_ta_conn)

        fe_proc.join()

        assert 'Frontend Server' in exceptions_caught_in_threads  # there was an exception in thread  1
        assert exceptions_caught_in_threads['Frontend Server']['exception']['type'] == RuntimeError
        assert str(exceptions_caught_in_threads['Frontend Server']['exception']['value']) == 'Abnormal exit detected'

    @catch_thread_exceptions
    @patch('dragon.launcher.frontend.LauncherFrontEnd._launch_backend')
    @patch('dragon.launcher.frontend.start_overlay_network')
    def test_garbled_beisup(self, exceptions_caught_in_threads, mock_overlay, mock_launch):
        '''Test receiving a garbage message in middle of loop'''

        log = logging.getLogger('garbled_beisup')
        args_map = get_args_map(self.network_config)

        # get startup going in another thread. Note: need to do threads in order to use
        # all our mocks
        fe_proc = threading.Thread(name='Frontend Server',
                                   target=run_frontend,
                                   args=(args_map,),
                                   daemon=False)
        fe_proc.start()

        self.get_backend_up(mock_overlay, mock_launch)
        log.info('got backend up')

        # Send BEIsUp
        for i, (host_id, node) in enumerate(self.be_nodes.items()):
            be_up_msg = dmsg.BEIsUp(tag=next_tag(),
                                    be_ch_desc=str(B64(node['ch_in'].serialize())),
                                    host_id=host_id)
            if i == 1:
                garbage_dict = {'junk': 'abdecdf', '52': 20}
                node['conn'].send(json.dumps(garbage_dict))
            else:
                node['conn'].send(be_up_msg.serialize())

        handle_overlay_teardown(self.fe_ta_conn)

        log.debug('sitting on frontend join')
        fe_proc.join()

        assert 'Frontend Server' in exceptions_caught_in_threads  # there was an exception in thread  1
        assert exceptions_caught_in_threads['Frontend Server']['exception']['type'] == RuntimeError
        assert str(exceptions_caught_in_threads['Frontend Server']['exception']['value']) == 'Abnormal exit detected'

    @catch_thread_exceptions
    @patch('dragon.launcher.frontend.LauncherFrontEnd._kill_backend')
    @patch('dragon.launcher.frontend.LauncherFrontEnd._wait_on_wlm_proc_exit')
    @patch('dragon.launcher.frontend.LauncherFrontEnd._launch_backend')
    @patch('dragon.launcher.frontend.start_overlay_network')
    def test_sigkill_to_wlm(self, exceptions_caught_in_threads, mock_overlay, mock_launch,
                            mock_wait, mock_kill):
        '''Test that we transmit a SIGKILL for a non-responsive backend'''
        mock_wait.side_effect = TimeoutExpired(None, 'raising a fake timeout')

        log = logging.getLogger('sigkill_to_wlm')
        args_map = get_args_map(self.network_config)

        # get startup going in another thread. Note: need to do threads in order to use
        # all our mocks
        fe_proc = threading.Thread(name='Frontend Server',
                                   target=run_frontend,
                                   args=(args_map,),
                                   daemon=False)
        fe_proc.start()

        self.get_backend_up(mock_overlay, mock_launch)
        log.info('got backend up')

        # Send BEIsUp
        for i, (host_id, node) in enumerate(self.be_nodes.items()):
            be_up_msg = dmsg.BEIsUp(tag=next_tag(),
                                    be_ch_desc=str(B64(node['ch_in'].serialize())),
                                    host_id=host_id)
            if i == 1:
                garbage_dict = {'junk': 'abdecdf', '52': 20}
                node['conn'].send(json.dumps(garbage_dict))
            else:
                node['conn'].send(be_up_msg.serialize())

        handle_overlay_teardown(self.fe_ta_conn)
        log.debug('sitting on frontend join')
        fe_proc.join()

        log.info(f'exception: {exceptions_caught_in_threads}')
        assert 'Frontend Server' in exceptions_caught_in_threads  # there was an exception in thread  1
        assert exceptions_caught_in_threads['Frontend Server']['exception']['type'] == RuntimeError
        assert str(exceptions_caught_in_threads['Frontend Server']['exception']['value']) == 'Abnormal exit detected'
        mock_wait.assert_called_once()
        mock_kill.assert_called_once()

    @catch_thread_exceptions
    @patch('dragon.launcher.frontend.LauncherFrontEnd._launch_backend')
    @patch('dragon.launcher.frontend.start_overlay_network')
    def test_garbled_taup(self, exceptions_caught_in_threads, mock_overlay, mock_launch):
        '''Test receiving garbage TAUp in middle of loop'''

        log = logging.getLogger('garbled_taup')
        args_map = get_args_map(self.network_config)

        # get startup going in another thread. Note: need to do threads in order to use
        # all our mocks
        fe_proc = threading.Thread(name='Frontend Server',
                                   target=run_frontend,
                                   args=(args_map,),
                                   daemon=False)
        fe_proc.start()

        self.get_backend_up(mock_overlay, mock_launch)
        log.info('got backend up')

        # Send BEIsUp
        send_beisup(self.be_nodes)
        log.info('send BEIsUp messages')

        # Recv FENodeIdxBE
        self.primary_conn = recv_fenodeidx(self.be_nodes)
        log.info('got all the FENodeIdxBE messages')

        # Fudge some SHChannelsUp messages
        send_shchannelsup(self.be_nodes, self.be_mpool)
        log.info(f'sent shchannelsup: {[node["gs_ch"].serialize() for node in self.be_nodes.values() if node["gs_ch"] is not None]}')

        # Receive LAChannelsInfo
        recv_lachannelsinfo(self.be_nodes)
        log.info('la_be received LAChannelsInfo')

        # Send TAUp
        for i, (host_id, node) in enumerate(self.be_nodes.items()):
            if i == 2:
                garbage_msg = {'fizz': 'abdecdf', 'buzz': 20}
                node['conn'].send(json.dumps(garbage_msg))
            else:
                ta_up = dmsg.TAUp(tag=next_tag(), idx=node['node_index'])
                node['conn'].send(ta_up.serialize())

        handle_overlay_teardown(self.fe_ta_conn)

        log.debug('sitting on frontend join')
        fe_proc.join()

        assert 'Frontend Server' in exceptions_caught_in_threads  # there was an exception in thread  1
        assert exceptions_caught_in_threads['Frontend Server']['exception']['type'] == RuntimeError
        assert str(exceptions_caught_in_threads['Frontend Server']['exception']['value']) == 'Abnormal exit detected'

    @catch_thread_exceptions
    @patch('dragon.launcher.frontend.LauncherFrontEnd._launch_backend')
    @patch('dragon.launcher.frontend.start_overlay_network')
    def test_abnormal_term_in_app_exec(self, exceptions_caught_in_threads, mock_overlay, mock_launch):
        '''Test the ability of frontend to exit from an Abnormal Termination during app execution'''

        args_map = get_args_map(self.network_config)

        # get startup going in another thread. Note: need to do threads in order to use
        # all our mocks
        fe_proc = threading.Thread(name='Frontend Server',
                                   target=run_frontend,
                                   args=(args_map,),
                                   daemon=False)
        fe_proc.start()

        # Get backend up
        self.do_bringup(mock_overlay, mock_launch)

        # Receive GSProcessCreate
        handle_gsprocesscreate(self.primary_conn)

        # Send an abormal termination rather than proceeding with teardown
        for i, (host_id, node) in enumerate(self.be_nodes.items()):
            if i == 2:
                send_abnormal_term(node['conn'])

        # Proceed with teardown
        handle_teardown(self.be_nodes, self.primary_conn, self.fe_ta_conn, gs_head_exit=False)

        # Join on the frontend thread
        fe_proc.join()

        assert 'Frontend Server' in exceptions_caught_in_threads  # there was an exception in thread  1
        assert exceptions_caught_in_threads['Frontend Server']['exception']['type'] == RuntimeError
        assert str(exceptions_caught_in_threads['Frontend Server']['exception']['value']) == 'Abnormal exit detected'

    @catch_thread_exceptions
    @patch('dragon.launcher.frontend.LauncherFrontEnd._launch_backend')
    @patch('dragon.launcher.frontend.start_overlay_network')
    def test_backend_timeout_in_abnormal_teardown(self, exceptions_caught_in_threads, mock_overlay, mock_launch):
        '''Test the frontend is able to recover from a hung backend'''

        args_map = get_args_map(self.network_config)

        # get startup going in another thread. Note: need to do threads in order to use
        # all our mocks
        fe_proc = threading.Thread(name='Frontend Server',
                                   target=run_frontend,
                                   args=(args_map,),
                                   daemon=False)
        fe_proc.start()

        # Get backend up
        self.do_bringup(mock_overlay, mock_launch)

        # Receive GSProcessCreate
        handle_gsprocesscreate(self.primary_conn)

        # Send an abormal termination rather than proceeding with teardown
        for i, (host_id, node) in enumerate(self.be_nodes.items()):
            if i == 2:
                send_abnormal_term(node['conn'])

        # Proceed with teardown
        handle_teardown(self.be_nodes,
                        self.primary_conn,
                        self.fe_ta_conn,
                        gs_head_exit=False,
                        timeout_backend=True)

        # Join on the frontend thread
        fe_proc.join()

        assert 'Frontend Server' in exceptions_caught_in_threads  # there was an exception in thread  1
        assert exceptions_caught_in_threads['Frontend Server']['exception']['type'] == RuntimeError
        assert str(exceptions_caught_in_threads['Frontend Server']['exception']['value']) == 'Abnormal exit detected'

    @catch_thread_exceptions
    @patch('dragon.launcher.frontend.LauncherFrontEnd._launch_backend')
    @patch('dragon.launcher.frontend.start_overlay_network')
    def test_overlay_timeout_in_abnormal_teardown(self, exceptions_caught_in_threads, mock_overlay, mock_launch):
        '''Test the frontend is able to recover from a hung overlay transport agent'''

        args_map = get_args_map(self.network_config)

        # get startup going in another thread. Note: need to do threads in order to use
        # all our mocks
        fe_proc = threading.Thread(name='Frontend Server',
                                   target=run_frontend,
                                   args=(args_map,),
                                   daemon=False)
        fe_proc.start()

        # Get backend up
        self.do_bringup(mock_overlay, mock_launch)

        # Receive GSProcessCreate
        handle_gsprocesscreate(self.primary_conn)

        # Send an abormal termination rather than proceeding with teardown
        for i, (host_id, node) in enumerate(self.be_nodes.items()):
            if i == 2:
                send_abnormal_term(node['conn'])

        # Proceed with teardown
        handle_teardown(self.be_nodes,
                        self.primary_conn,
                        self.fe_ta_conn,
                        gs_head_exit=False,
                        timeout_overlay=True)

        # Join on the frontend thread
        fe_proc.join()

        assert 'Frontend Server' in exceptions_caught_in_threads  # there was an exception in thread  1
        assert exceptions_caught_in_threads['Frontend Server']['exception']['type'] == RuntimeError
        assert str(exceptions_caught_in_threads['Frontend Server']['exception']['value']) == 'Abnormal exit detected'

    @catch_thread_exceptions
    @patch('dragon.launcher.frontend.LauncherFrontEnd._launch_backend')
    @patch('dragon.launcher.frontend.start_overlay_network')
    def test_abnormal_term_in_teardown(self, exceptions_caught_in_threads, mock_overlay, mock_launch):
        '''Test the ability of frontend to exit from an Abnormal Termination during teardown'''

        args_map = get_args_map(self.network_config)

        # get startup going in another thread. Note: need to do threads in order to use
        # all our mocks
        fe_proc = threading.Thread(name='Frontend Server',
                                   target=run_frontend,
                                   args=(args_map,),
                                   daemon=False)
        fe_proc.start()

        # Get backend up
        self.do_bringup(mock_overlay, mock_launch)

        # Receive GSProcessCreate
        handle_gsprocesscreate(self.primary_conn)

        handle_teardown(self.be_nodes, self.primary_conn, self.fe_ta_conn,
                        gs_head_exit=True,
                        abort_shteardown=3)

        fe_proc.join()

        log = logging.getLogger('test_abnormal_term_in_teardown')
        log.info(f'exception = {exceptions_caught_in_threads}')
        assert 'Frontend Server' in exceptions_caught_in_threads  # there was an exception in thread  1
        assert exceptions_caught_in_threads['Frontend Server']['exception']['type'] == RuntimeError
        assert str(exceptions_caught_in_threads['Frontend Server']['exception']['value']) == 'Abnormal exit detected'

    @patch('dragon.launcher.network_config.NetworkConfig.from_wlm')
    @patch('dragon.launcher.frontend.LauncherFrontEnd._launch_backend')
    @patch('dragon.launcher.frontend.start_overlay_network')
    def test_wlm_slurm_launch_selection(self, mock_overlay, mock_launch, mock_config):
        """Test that we honor user's slurm WLM selection rather than auto detect"""
        parser = get_parser()
        arg_list = ['--wlm', 'slurm',
                    '--network-prefix', '^(eth|hsn)',
                    'hello_world.py']

        args = parser.parse_args(args=arg_list)
        if args.basic_label or args.verbose_label:
            args.no_label = False
        args_map = {key: value for key, value in vars(args).items() if value is not None}

        # Give the net conf a correct object so that rest of this test can complete
        net_config_obj = NetworkConfig.from_file(self.network_config)
        mock_config.side_effect = [net_config_obj]

        # get startup going in another thread. Note: need to do threads in order to use
        # all our mocks
        fe_proc = threading.Thread(name='Frontend Server',
                                   target=run_frontend,
                                   args=(args_map,),
                                   daemon=False)
        fe_proc.start()

        # Get backend up
        self.do_bringup(mock_overlay, mock_launch)

        # Receive GSProcessCreate
        handle_gsprocesscreate(self.primary_conn)

        # Send GSHalted
        handle_teardown(self.be_nodes, self.primary_conn, self.fe_ta_conn)

        # Join on the frontend thread
        fe_proc.join()

        # Check the config mock actually use slurm
        assert mock_config.called_with(workload_manager=WLM.SLURM,
                                       port=DEFAULT_OVERLAY_NETWORK_PORT,
                                       network_prefix=DEFAULT_TRANSPORT_NETIF,
                                       sigint_trigger=None)

    @catch_thread_exceptions
    @patch('dragon.launcher.network_config.NetworkConfig.from_wlm')
    def test_wlm_ssh_auto_detect(self, exceptions_caught_in_threads, mock_config):
        """Test we throw an error if a user doesn't specify a WLM, and we only detect ssh"""
        parser = get_parser()
        arg_list = ['-l', 'dragon_file=DEBUG',
                    '--network-prefix', '^(eth|hsn)',
                    'hello_world.py']
        args = parser.parse_args(args=arg_list)
        if args.basic_label or args.verbose_label:
            args.no_label = False
        args_map = {key: value for key, value in vars(args).items() if value is not None}

        # Give the net conf a correct object so that rest of this test can complete
        net_config_obj = NetworkConfig.from_file(self.network_config)
        mock_config.side_effect = [net_config_obj]

        # get startup going in another thread. Note: need to do threads in order to use
        # all our mocks
        fe_proc = threading.Thread(name='Frontend Server',
                                   target=run_frontend,
                                   args=(args_map,),
                                   daemon=False)
        fe_proc.start()
        fe_proc.join()

        log = logging.getLogger('auto_ssh')
        log.error(f'exception: {exceptions_caught_in_threads}')
        assert 'Frontend Server' in exceptions_caught_in_threads  # there was an exception in thread  1
        assert exceptions_caught_in_threads['Frontend Server']['exception']['type'] == RuntimeError
        assert 'SSH was only supported launcher found' in str(exceptions_caught_in_threads['Frontend Server']['exception']['value'])

    @patch('dragon.launcher.network_config.NetworkConfig.from_wlm')
    @patch('dragon.launcher.frontend.LauncherFrontEnd._launch_backend')
    @patch('dragon.launcher.frontend.start_overlay_network')
    def test_pbs_slurm_launch_selection(self, mock_overlay, mock_launch, mock_config):
        """Test that we honor user's pbs+pals WLM selection rather than auto detect"""
        args_map = get_args_map(self.network_config,
                                arg1=['--wlm', 'slurm'])

        parser = get_parser()
        arg_list = ['--wlm', 'pbs+pals',
                    '--network-prefix', '^(eth|hsn)',
                    'hello_world.py']

        args = parser.parse_args(args=arg_list)
        if args.basic_label or args.verbose_label:
            args.no_label = False
        args_map = {key: value for key, value in vars(args).items() if value is not None}

        # Give the net conf a correct object so that rest of this test can complete
        net_config_obj = NetworkConfig.from_file(self.network_config)
        mock_config.side_effect = [net_config_obj]

        # get startup going in another thread. Note: need to do threads in order to use
        # all our mocks
        fe_proc = threading.Thread(name='Frontend Server',
                                   target=run_frontend,
                                   args=(args_map,),
                                   daemon=False)
        fe_proc.start()

        # Get backend up
        self.do_bringup(mock_overlay, mock_launch)

        # Receive GSProcessCreate
        handle_gsprocesscreate(self.primary_conn)

        # Send GSHalted
        handle_teardown(self.be_nodes, self.primary_conn, self.fe_ta_conn)

        # Join on the frontend thread
        fe_proc.join()

        # Check the config mock actually use slurm
        assert mock_config.called_with(workload_manager=WLM.SLURM,
                                       port=DEFAULT_OVERLAY_NETWORK_PORT,
                                       network_prefix=DEFAULT_TRANSPORT_NETIF,
                                       sigint_trigger=None)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    unittest.main()
