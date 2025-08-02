#!/usr/bin/env python3
import os
import sys
import logging
import unittest
import threading
from unittest.mock import patch
from io import StringIO

from dragon.launcher.launchargs import get_parser

from dragon.infrastructure.node_desc import NodeDescriptor
from dragon.launcher.network_config import NetworkConfig
from dragon.channels import ChannelError
from dragon.managed_memory import DragonMemoryError

from .launcher_testing_utils import catch_thread_exceptions

from .frontend_testing_mocks import run_resilient_frontend
from .frontend_testing_mocks import handle_teardown
from .frontend_testing_mocks import handle_gsprocesscreate, handle_bringup, stand_up_backend
from .frontend_testing_mocks import send_abnormal_term


def get_args_map(network_config, **kwargs):
    parser = get_parser()
    arg_list = ["--wlm", "slurm", "--network-config", f"{network_config}", "--network-prefix", "^(eth|hsn)"]
    for val in kwargs.values():
        arg_list = arg_list + val

    arg_list.append("hello_world.py")

    args = parser.parse_args(args=arg_list)
    if args.basic_label or args.verbose_label:
        args.no_label = False
    args_map = {key: value for key, value in vars(args).items() if value is not None}

    return args_map


class FrontendRestartTest(unittest.TestCase):
    def setUp(self):
        self.test_dir = os.path.dirname(os.path.realpath(__file__))
        self.network_config = os.path.join(self.test_dir, "slurm_primary.yaml")
        self.bad_network_config = os.path.join(self.test_dir, "slurm_bad.yaml")
        self.big_network_config = os.path.join(self.test_dir, "slurm_big.yaml")

        self.be_mpool = None
        self.be_ch_out = None
        self.be_ch_in = None
        self.overlay_inout = None

        self.ls_ch = None

        self.ta_ch_in = None
        self.ta_ch_out = None
        self.fe_ta_conn = None

    def tearDown(self):
        self.cleanup()

    def cleanup(self):
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
                node["conn"].close()
                node["ch_in"].destroy()
        except (AttributeError, ChannelError):
            pass

        try:
            for node in self.be_nodes.values():
                node["ls_ch"].destroy()
                if node["is_primary"]:
                    node["gs_ch"].destroy()
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

    def do_bringup(self, mock_overlay, mock_launch, net_conf=None):
        overlay, la_info = handle_bringup(mock_overlay, mock_launch, self.network_config, net_conf=net_conf)
        self.ta_ch_in = overlay["ta_ch_in"]
        self.ta_ch_out = overlay["ta_ch_out"]
        self.fe_ta_conn = overlay["fe_ta_conn"]
        self.be_mpool = overlay["be_mpool"]
        self.be_ch_out = overlay["be_ch_out"]
        self.be_ch_in = overlay["be_ch_in"]
        self.be_nodes = overlay["be_nodes"]
        self.overlay_inout = overlay["overlay_inout"]
        self.primary_conn = overlay["primary_conn"]

        return la_info

    def get_backend_up(self, mock_overlay, mock_launch):
        overlay = stand_up_backend(mock_overlay, mock_launch, self.network_config)
        self.ta_ch_in = overlay["ta_ch_in"]
        self.ta_ch_out = overlay["ta_ch_out"]
        self.fe_ta_conn = overlay["fe_ta_conn"]
        self.be_mpool = overlay["be_mpool"]
        self.be_ch_out = overlay["be_ch_out"]
        self.be_ch_in = overlay["be_ch_in"]
        self.be_nodes = overlay["be_nodes"]
        self.overlay_inout = overlay["overlay_inout"]

    @catch_thread_exceptions
    @patch("dragon.launcher.frontend.LauncherFrontEnd._launch_backend")
    @patch("dragon.launcher.frontend.start_overlay_network")
    def test_abnormal_restart_no_promotion(self, exceptions_caught_in_threads, mock_overlay, mock_launch):
        """Test the ability of frontend to restart from an Abnormal Term, excluding the node that sent the signal with no replacement"""

        args_map = get_args_map(self.network_config, arg1=["--resilient", "--exhaust-resources"], arg2=["--nodes", "4"])

        # get startup going in another thread. Note: need to do threads in order to use
        # all our mocks
        fe_proc = threading.Thread(
            name="Frontend Server", target=run_resilient_frontend, args=(args_map,), daemon=False
        )
        fe_proc.start()

        # Get backend up
        self.do_bringup(mock_overlay, mock_launch)

        # Receive GSProcessCreate
        handle_gsprocesscreate(self.primary_conn)

        # Send an abormal termination rather than proceeding with teardown
        dropped_host_id = 0
        dropped_index = 2
        for i, (host_id, node) in enumerate(self.be_nodes.items()):
            if i == dropped_index:
                dropped_host_id = host_id
                send_abnormal_term(node["conn"], host_id=host_id)

        # Necessarily clean up all the backend stuff:
        self.cleanup()

        # Construct my own network configuration and set the State manually so
        # the backend mocks know how to behave
        net = NetworkConfig.from_file(self.network_config)
        net_conf = net.get_network_config()

        for node in net_conf.values():
            if node.host_id == dropped_host_id:
                node.state = NodeDescriptor.State.DOWN

        # Get backend back up for the resilient launch
        self.do_bringup(mock_overlay, mock_launch, net_conf=net_conf)

        # Check that the frontend gave us the expected config
        self.assertEqual(len(self.be_nodes), len(net_conf) - 1)
        for host_id, node in self.be_nodes.items():
            self.assertNotEqual(host_id, dropped_host_id)

        handle_gsprocesscreate(self.primary_conn)
        handle_teardown(self.be_nodes, self.primary_conn, self.fe_ta_conn)

        # Join on the frontend thread
        fe_proc.join()

    @catch_thread_exceptions
    @patch("dragon.launcher.frontend.LauncherFrontEnd._launch_backend")
    @patch("dragon.launcher.frontend.start_overlay_network")
    def test_abnormal_restart_with_promotion(self, exceptions_caught_in_threads, mock_overlay, mock_launch):
        """Test the ability of frontend to restart from an Abnormal Term, excluding the node that sent the signal with a replacement"""

        nnodes = 4
        self.network_config = self.big_network_config
        args_map = get_args_map(self.network_config, arg1=["--resilient"], arg2=["--nodes", f"{nnodes}"])

        # Construct our node list:
        net = NetworkConfig.from_file(self.network_config)
        net_conf = net.get_network_config()

        active_nodes = 0
        for node in net_conf.values():
            if active_nodes != nnodes:
                node.state = NodeDescriptor.State.ACTIVE
                active_nodes = active_nodes + 1
            else:
                node.state = NodeDescriptor.State.IDLE

        # get startup going in another thread. Note: need to do threads in order to use
        # all our mocks
        fe_proc = threading.Thread(
            name="Frontend Server", target=run_resilient_frontend, args=(args_map,), daemon=False
        )
        fe_proc.start()

        # Get backend up
        self.do_bringup(mock_overlay, mock_launch, net_conf=net_conf)

        # Receive GSProcessCreate
        handle_gsprocesscreate(self.primary_conn)

        # Send an abormal termination rather than proceeding with teardown
        dropped_host_id = 0
        dropped_index = 2
        for i, (host_id, node) in enumerate(self.be_nodes.items()):
            if i == dropped_index:
                dropped_host_id = host_id
                send_abnormal_term(node["conn"], host_id=host_id)

        # Necessarily clean up all the backend stuff:
        self.cleanup()

        active_nodes = 0
        for node in net_conf.values():
            if node.host_id == dropped_host_id:
                node.state = NodeDescriptor.State.DOWN
            elif active_nodes != nnodes:
                node.state = NodeDescriptor.State.ACTIVE
                active_nodes = active_nodes + 1
            else:
                node.state = NodeDescriptor.State.IDLE

        # Get backend back up for the resilient launch
        self.do_bringup(mock_overlay, mock_launch, net_conf=net_conf)

        # Check that the frontend gave us the expected config
        self.assertEqual(len(self.be_nodes), nnodes)
        for host_id, node in self.be_nodes.items():
            self.assertNotEqual(host_id, dropped_host_id)

        handle_gsprocesscreate(self.primary_conn)
        handle_teardown(self.be_nodes, self.primary_conn, self.fe_ta_conn)

        # Join on the frontend thread
        fe_proc.join()

    @catch_thread_exceptions
    @patch("dragon.launcher.frontend.LauncherFrontEnd._launch_backend")
    @patch("dragon.launcher.frontend.start_overlay_network")
    def test_abnormal_restart_with_promotion_and_idle_nodes(
        self, exceptions_caught_in_threads, mock_overlay, mock_launch
    ):
        """Test the ability of frontend to restart from an Abnormal Term, excluding the node that sent the signal with a replacement"""

        nnodes = 4
        idle_nodes = 12
        self.network_config = self.big_network_config
        args_map = get_args_map(
            self.network_config, arg1=["--resilient"], arg2=["--nodes", f"{nnodes}"], arg3=["--idle", f"{idle_nodes}"]
        )
        # Construct our node list:
        net = NetworkConfig.from_file(self.network_config)
        net_conf = net.get_network_config()

        active_nodes = 0
        for node in net_conf.values():
            if active_nodes != nnodes:
                node.state = NodeDescriptor.State.ACTIVE
                active_nodes = active_nodes + 1
            else:
                node.state = NodeDescriptor.State.IDLE

        # get startup going in another thread. Note: need to do threads in order to use
        # all our mocks
        fe_proc = threading.Thread(
            name="Frontend Server", target=run_resilient_frontend, args=(args_map,), daemon=False
        )
        fe_proc.start()

        # Get backend up
        self.do_bringup(mock_overlay, mock_launch, net_conf=net_conf)

        # Receive GSProcessCreate
        handle_gsprocesscreate(self.primary_conn)

        # Send an abormal termination rather than proceeding with teardown
        dropped_host_id = 0
        dropped_index = 2
        for i, (host_id, node) in enumerate(self.be_nodes.items()):
            if i == dropped_index:
                dropped_host_id = host_id
                send_abnormal_term(node["conn"], host_id=host_id)

        # Necessarily clean up all the backend stuff:
        self.cleanup()

        active_nodes = 0
        for node in net_conf.values():
            if node.host_id == dropped_host_id:
                node.state = NodeDescriptor.State.DOWN
            elif active_nodes != nnodes:
                node.state = NodeDescriptor.State.ACTIVE
                active_nodes = active_nodes + 1
            else:
                node.state = NodeDescriptor.State.IDLE

        # Get backend back up for the resilient launch
        self.do_bringup(mock_overlay, mock_launch, net_conf=net_conf)

        # Check that the frontend gave us the expected config
        self.assertEqual(len(self.be_nodes), nnodes)
        for host_id, node in self.be_nodes.items():
            self.assertNotEqual(host_id, dropped_host_id)

        handle_gsprocesscreate(self.primary_conn)
        handle_teardown(self.be_nodes, self.primary_conn, self.fe_ta_conn)

        # Join on the frontend thread
        fe_proc.join()

    @catch_thread_exceptions
    @patch("dragon.launcher.frontend.LauncherFrontEnd._launch_backend")
    @patch("dragon.launcher.frontend.start_overlay_network")
    def test_rapid_abnormal_restart(self, exceptions_caught_in_threads, mock_overlay, mock_launch):
        """Test the ability to arrest ourselves out of a continuous boot loop"""
        nnodes = 4
        idle_nodes = 12
        self.network_config = self.big_network_config
        args_map = get_args_map(
            self.network_config, arg1=["--resilient"], arg2=["--nodes", f"{nnodes}"], arg3=["--idle", f"{idle_nodes}"]
        )

        # Construct our node list:
        net = NetworkConfig.from_file(self.network_config)
        net_conf = net.get_network_config()

        active_nodes = 0
        for node in net_conf.values():
            if active_nodes != nnodes:
                node.state = NodeDescriptor.State.ACTIVE
                active_nodes = active_nodes + 1
            else:
                node.state = NodeDescriptor.State.IDLE

        # get startup going in another thread. Note: need to do threads in order to use
        # all our mocks
        fe_proc = threading.Thread(
            name="Frontend Server", target=run_resilient_frontend, args=(args_map,), daemon=False
        )
        fe_proc.start()

        # Get backend up
        self.do_bringup(mock_overlay, mock_launch, net_conf=net_conf)

        # Receive GSProcessCreate
        handle_gsprocesscreate(self.primary_conn)

        # Send an abormal termination rather than proceeding with teardown
        dropped_host_id = 0
        dropped_index = 2
        for i, (host_id, node) in enumerate(self.be_nodes.items()):
            if i == dropped_index:
                dropped_host_id = host_id
                send_abnormal_term(node["conn"], host_id=host_id)

        # Necessarily clean up all the backend stuff:
        self.cleanup()

        mock_overlay.side_effect = RuntimeError("something to complain about")

        # Catch the stdout
        captured_stdout = StringIO()
        sys.stdout = captured_stdout

        active_nodes = 0
        for node in net_conf.values():
            if node.host_id == dropped_host_id:
                node.state = NodeDescriptor.State.DOWN
            elif active_nodes != nnodes:
                node.state = NodeDescriptor.State.ACTIVE
                active_nodes = active_nodes + 1
            else:
                node.state = NodeDescriptor.State.IDLE

        # Join on the frontend thread
        fe_proc.join()

        # Set stdout back and check output
        sys.stdout = sys.__stdout__
        self.assertTrue("Dragon runtime is in an unrecoverable state. Exiting." in captured_stdout.getvalue())

    @catch_thread_exceptions
    @patch("dragon.launcher.frontend.LauncherFrontEnd._launch_backend")
    @patch("dragon.launcher.frontend.start_overlay_network")
    def test_abnormal_restart_kill_global_services(self, exceptions_caught_in_threads, mock_overlay, mock_launch):
        """Test the ability of frontend to restart from an Abnormal Term when downed node is global services"""

        nnodes = 4
        self.network_config = self.big_network_config
        args_map = get_args_map(self.network_config, arg1=["--resilient"], arg2=["--nodes", f"{nnodes}"])

        # Construct our node list:
        net = NetworkConfig.from_file(self.network_config)
        net_conf = net.get_network_config()

        active_nodes = 0
        for node in net_conf.values():
            if active_nodes != nnodes:
                node.state = NodeDescriptor.State.ACTIVE
                active_nodes = active_nodes + 1
            else:
                node.state = NodeDescriptor.State.IDLE

        # get startup going in another thread. Note: need to do threads in order to use
        # all our mocks
        fe_proc = threading.Thread(
            name="Frontend Server", target=run_resilient_frontend, args=(args_map,), daemon=False
        )
        fe_proc.start()

        # Get backend up
        self.do_bringup(mock_overlay, mock_launch, net_conf=net_conf)

        # Receive GSProcessCreate
        handle_gsprocesscreate(self.primary_conn)

        # Send an abormal termination to global services
        dropped_host_id = 0
        dropped_index = [int(k) for k, v in net_conf.items() if v.is_primary][0]
        for i, (host_id, node) in enumerate(self.be_nodes.items()):
            if i == dropped_index:
                dropped_host_id = host_id
                send_abnormal_term(node["conn"], host_id=host_id)

        # Necessarily clean up all the backend stuff:
        self.cleanup()

        # Update our own internal tracking to match what should be in the launcher's
        # network configuration
        active_nodes = 0
        for node in net_conf.values():
            if node.host_id == dropped_host_id:
                node.state = NodeDescriptor.State.DOWN
            elif active_nodes != nnodes:
                node.state = NodeDescriptor.State.ACTIVE
                active_nodes = active_nodes + 1
            else:
                node.state = NodeDescriptor.State.IDLE

        # Get backend back up for the resilient launch
        self.do_bringup(mock_overlay, mock_launch, net_conf=net_conf)

        # Check that the frontend gave us the expected config
        self.assertEqual(len(self.be_nodes), nnodes)
        for host_id, node in self.be_nodes.items():
            self.assertNotEqual(host_id, dropped_host_id)

        # Check that some node is selected as primary
        self.assertTrue(any([node["is_primary"] for node in self.be_nodes.values()]))

        # Do the rest of bring-up and teardown
        handle_gsprocesscreate(self.primary_conn)
        handle_teardown(self.be_nodes, self.primary_conn, self.fe_ta_conn)

        # Join on the frontend thread
        fe_proc.join()

    @unittest.skip("Test Hangs. Waiting for fix. Jira AICI-1859.")
    @catch_thread_exceptions
    @patch("dragon.launcher.frontend.LauncherFrontEnd._launch_backend")
    @patch("dragon.launcher.frontend.start_overlay_network")
    def test_abnormal_restart_exhaust_resources(self, exceptions_caught_in_threads, mock_overlay, mock_launch):
        """Test the ability of frontend to restart until there are no nodes left to use"""

        nnodes = 3
        args_map = get_args_map(
            self.network_config, arg1=["--resilient", "--exhaust-resources"], arg2=["--nodes", f"{nnodes}"]
        )

        # Construct our node list:
        net = NetworkConfig.from_file(self.network_config)
        net_conf = net.get_network_config()
        all_nodes = len(net_conf)

        active_nodes = 0
        for node in net_conf.values():
            if active_nodes != nnodes:
                node.state = NodeDescriptor.State.ACTIVE
                active_nodes = active_nodes + 1
            else:
                node.state = NodeDescriptor.State.IDLE

        # get startup going in another thread. Note: need to do threads in order to use
        # all our mocks
        fe_proc = threading.Thread(
            name="Frontend Server", target=run_resilient_frontend, args=(args_map,), daemon=False
        )
        fe_proc.start()

        # Get backend
        dropped_index = 2
        dropped_host_id = 0

        log = logging.getLogger("test")
        for index in range(all_nodes):
            self.do_bringup(mock_overlay, mock_launch, net_conf=net_conf)

            # Receive GSProcessCreate
            log.info("Creating head proc")
            handle_gsprocesscreate(self.primary_conn)
            log.info("Head proc created")

            # Check that the frontend gave us the expected config
            self.assertEqual(len(self.be_nodes), nnodes)
            for host_id, node in self.be_nodes.items():
                self.assertNotEqual(host_id, dropped_host_id)

            # if the last execution, grab stdout to make sure the correct message is printed
            if index == all_nodes - 1:
                captured_stdout = StringIO()
                sys.stdout = captured_stdout

            # Send an abormal termination rather than proceeding with teardown
            if dropped_index > nnodes - 1:
                dropped_index = nnodes - 1
            for i, (host_id, node) in enumerate(self.be_nodes.items()):
                if i == dropped_index:
                    dropped_host_id = host_id
                    log.info(f"sending a abnormal signal to {host_id}: {node}")
                    send_abnormal_term(node["conn"], host_id=host_id)

            # Necessarily clean up all the backend stuff:
            log.info("doing cleanup of mocks")
            self.cleanup()

            # Update our internal ref on the node states
            active_nodes = 0
            log.info("updating net conf")
            for node in net_conf.values():
                if node.state != NodeDescriptor.State.DOWN:
                    if node.host_id == dropped_host_id:
                        log.info(f"marking {dropped_host_id} down in net_conf")
                        node.state = NodeDescriptor.State.DOWN
                    elif active_nodes != nnodes:
                        node.state = NodeDescriptor.State.ACTIVE
                        active_nodes = active_nodes + 1
                    else:
                        node.state = NodeDescriptor.State.IDLE

            # Update the node count if we have fewer than originally proposed
            if active_nodes < nnodes:
                nnodes = active_nodes

        # Join on the frontend thread
        fe_proc.join()

        # Reset stdout and check captured output
        sys.stdout = sys.__stdout__
        self.assertTrue(
            "There are no more hardware resources available for continued app execution." in captured_stdout.getvalue()
        )

    @catch_thread_exceptions
    @patch("dragon.launcher.frontend.LauncherFrontEnd._launch_backend")
    @patch("dragon.launcher.frontend.start_overlay_network")
    def test_abnormal_restart_min_nodes(self, exceptions_caught_in_threads, mock_overlay, mock_launch):
        """Test the ability of frontend to restart until there are not enough nodes left for user requested node count"""

        nnodes = 2
        args_map = get_args_map(self.network_config, arg1=["--resilient"], arg2=["--nodes", f"{nnodes}"])

        # Construct our node list:
        net = NetworkConfig.from_file(self.network_config)
        net_conf = net.get_network_config()
        all_nodes = len(net_conf)

        active_nodes = 0
        for node in net_conf.values():
            if active_nodes != nnodes:
                node.state = NodeDescriptor.State.ACTIVE
                active_nodes = active_nodes + 1
            else:
                node.state = NodeDescriptor.State.IDLE

        # get startup going in another thread. Note: need to do threads in order to use
        # all our mocks
        fe_proc = threading.Thread(
            name="Frontend Server", target=run_resilient_frontend, args=(args_map,), daemon=False
        )
        fe_proc.start()

        # Get backend
        dropped_index = 2
        dropped_host_id = 0

        log = logging.getLogger("test")
        for index in range(all_nodes - nnodes + 1):
            self.do_bringup(mock_overlay, mock_launch, net_conf=net_conf)

            # Receive GSProcessCreate
            log.info("Creating head proc")
            handle_gsprocesscreate(self.primary_conn)
            log.info("Head proc created")

            # Check that the frontend gave us the expected config
            self.assertEqual(len(self.be_nodes), nnodes)
            for host_id, node in self.be_nodes.items():
                self.assertNotEqual(host_id, dropped_host_id)

            # if the last execution, grab stdout to make sure the correct message is printed
            if index == all_nodes - nnodes:
                captured_stdout = StringIO()
                sys.stdout = captured_stdout

            # Send an abormal termination rather than proceeding with teardown
            if dropped_index > nnodes - 1:
                dropped_index = nnodes - 1
            for i, (host_id, node) in enumerate(self.be_nodes.items()):
                if i == dropped_index:
                    dropped_host_id = host_id
                    log.info(f"sending a abnormal signal to {host_id}: {node}")
                    send_abnormal_term(node["conn"], host_id=host_id)

            # Necessarily clean up all the backend stuff:
            log.info("doing cleanup of mocks")
            self.cleanup()

            # Update our internal ref on the node states
            active_nodes = 0
            log.info("updating net conf")
            for node in net_conf.values():
                if node.state != NodeDescriptor.State.DOWN:
                    if node.host_id == dropped_host_id:
                        log.info(f"marking {dropped_host_id} down in net_conf")
                        node.state = NodeDescriptor.State.DOWN
                    elif active_nodes != nnodes:
                        node.state = NodeDescriptor.State.ACTIVE
                        active_nodes = active_nodes + 1
                    else:
                        node.state = NodeDescriptor.State.IDLE

            # Update the node count if we have fewer than originally proposed
            if active_nodes < nnodes:
                nnodes = active_nodes

        # Join on the frontend thread
        fe_proc.join()

        # Reset stdout and check captured output
        sys.stdout = sys.__stdout__
        self.assertTrue(
            "There are not enough hardware resources available for continued app execution"
            in captured_stdout.getvalue()
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    unittest.main()
