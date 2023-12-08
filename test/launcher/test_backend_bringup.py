#!/usr/bin/env python3
import os
import logging
import threading
import string
import random

import unittest
from unittest.mock import patch

from dragon.infrastructure.facts import PROCNAME_LA_BE
from dragon.launcher.backend import LauncherBackEnd
from .launcher_testing_utils import catch_thread_exceptions
from .backend_testing_mocks import LauncherBackendHelper, mock_start_localservices_wrapper


def start_backend(args):

    # Before doing anything set my host ID
    from dragon.utils import set_procname
    from dragon.dlogging.util import DragonLoggingServices as dls
    from dragon.dlogging.util import setup_BE_logging, LOGGING_OUTPUT_DEVICE_ACTOR_FILE

    # Enable debug logging in BE service
    os.environ[
        f"DRAGON_LOG_DEVICE_{LOGGING_OUTPUT_DEVICE_ACTOR_FILE.upper()}"
    ] = "DEBUG"

    set_procname(PROCNAME_LA_BE)

    # Set up the logging level. If debug, start logging immediately
    level, fname = setup_BE_logging(dls.LA_BE)

    log = logging.getLogger("start_backend")
    for key, value in args.items():
        if value is not None:
            log.info(f"args: {key}: {value}")
    with LauncherBackEnd(args["transport_test"], args["network_prefix"]) as be_server:
        be_server.run_startup(args["ip_addrs"], args["host_ids"], args["frontend_sdesc"], level, fname)
        be_server.run_msg_server()

    del os.environ[
        f"DRAGON_LOG_DEVICE_{LOGGING_OUTPUT_DEVICE_ACTOR_FILE.upper()}"
    ]


class BackendBringUpTeardownTest(unittest.TestCase):
    def setUp(self):

        self.test_dir = os.path.dirname(os.path.realpath(__file__))
        self.network_config = os.path.join(self.test_dir, "slurm_primary.yaml")

        self.node_index = 0  # so localservices will create GS_CD
        self.ip_addrs = "127.0.0.1:6570"
        self.fe_host_id = "fe"
        self.be_host_id = "be"

        self.be_helper = LauncherBackendHelper(self.network_config,
                                               self.node_index,
                                               self.ip_addrs,
                                               self.fe_host_id,
                                               self.be_host_id)

    def tearDown(self):

        # Make sure we don't leave any logging handers up
        log = logging.getLogger()
        for handler in log.handlers:
            log.removeHandler(handler)
            handler.close()

    def start_backend_thread(self, args_map):

        # get startup going in another thread. Note: need to do threads
        # in order to use all our mocks
        self.be_thread = threading.Thread(
            name="Backend Server", target=start_backend, args=(args_map,), daemon=False
        )
        self.be_thread.start()

    @mock_start_localservices_wrapper
    @patch("dragon.launcher.backend.start_overlay_network")
    @patch("dragon.launcher.backend.NodeDescriptor.get_local_node_network_conf")
    def test_clean_exit(self, mock_localservices_tuple, mock_network_config, mock_overlay):
        """Test that we can come up and exit cleanly"""

        log = logging.getLogger("clean_be_startup_teardown")

        args_map = self.be_helper.handle_network_and_frontend_start(mock_network_config)
        self.start_backend_thread(args_map)

        self.be_helper.clean_startup(log, mock_overlay, mock_network_config, mock_localservices_tuple)
        self.be_helper.launch_user_process(log)
        self.be_helper.clean_shutdown(log)
        self.be_helper.cleanup()

        # join on the be thread
        self.be_thread.join()

    @mock_start_localservices_wrapper
    @patch("dragon.launcher.backend.start_overlay_network")
    @patch("dragon.launcher.backend.NodeDescriptor.get_local_node_network_conf")
    def test_accelerators_present(self, mock_localservices_tuple, mock_network_config, mock_overlay):
        """Test that we can come up and exit cleanly"""

        log = logging.getLogger("clean_be_startup_teardown")

        args_map = self.be_helper.handle_network_and_frontend_start(mock_network_config)
        self.start_backend_thread(args_map)

        self.be_helper.clean_startup(log,
                                     mock_overlay,
                                     mock_network_config,
                                     mock_localservices_tuple,
                                     accelerator_present=True)
        self.be_helper.launch_user_process(log)
        self.be_helper.clean_shutdown(log)
        self.be_helper.cleanup()

        # join on the be thread
        self.be_thread.join()

    @catch_thread_exceptions
    @mock_start_localservices_wrapper
    @patch("dragon.launcher.backend.start_overlay_network")
    @patch("dragon.launcher.backend.NodeDescriptor.get_local_node_network_conf")
    def test_local_services_launch_exception(self, exceptions_caught_in_threads,
                                             mock_localservices_tuple, mock_network_config, mock_overlay):
        '''Test unable to Popen backend launcher'''

        # Set up our breaking of local services start
        all_chars = string.ascii_letters + string.digits + string.punctuation
        exception_text = ''.join(random.choices(all_chars, k=64))
        mock_start_localservices, _, _ = mock_localservices_tuple
        mock_start_localservices.side_effect = RuntimeError(exception_text)

        # Get our frontend crap going so we can get the overlay up
        args_map = self.be_helper.handle_network_and_frontend_start(mock_network_config)
        self.start_backend_thread(args_map)

        # Start up the overlay
        self.be_helper.handle_overlay_start(mock_overlay, mock_localservices_tuple)

        # At this point, our LS mock should throw its side-effect:
        self.be_thread.join()

        # Clean up local services
        self.be_helper.cleanup()

        log = logging.getLogger('test_local_services_launch_exception')
        log.info(f'exceptions: {exceptions_caught_in_threads}')
        assert mock_start_localservices.call_count == 1
        assert 'Backend Server' in exceptions_caught_in_threads
        assert exceptions_caught_in_threads['Backend Server']['exception']['type'] == RuntimeError
        assert str(exceptions_caught_in_threads['Backend Server']['exception']['value']) == exception_text

    @catch_thread_exceptions
    @mock_start_localservices_wrapper
    @patch("dragon.launcher.backend.start_overlay_network")
    @patch("dragon.launcher.backend.NodeDescriptor.get_local_node_network_conf")
    def test_garbled_lachannelsinfo(self, exceptions_caught_in_threads,
                                    mock_localservices_tuple, mock_network_config, mock_overlay):
        '''Test that we exit correctly if the LAChannelsInfo we receive is garbled'''

        log = logging.getLogger('test_garbled_shchannelsup')

        # Get our frontend crap going so we can get the overlay up
        args_map = self.be_helper.handle_network_and_frontend_start(mock_network_config)
        self.start_backend_thread(args_map)

        # Cleanly start everything to LS returning its SHChannelsUp
        self.be_helper.start_ls(mock_overlay, mock_localservices_tuple, garble_lachannelsinfo=True)

        # Confirm we send out an abnormal term to the frontend so it can
        # issue a GSTeardown to the other nodes
        self.be_helper.start_abnormal_termination_exit(log, gs_up=False)

        # Clean up local services
        self.be_helper.cleanup()

        # At this point, our LS mock should throw its side-effect:
        self.be_thread.join()
        log.debug(f'exceptions: {exceptions_caught_in_threads}')
        assert 'Backend Server' in exceptions_caught_in_threads
        assert exceptions_caught_in_threads['Backend Server']['exception']['type'] == RuntimeError
        assert str(exceptions_caught_in_threads['Backend Server']['exception']['value']) == 'Abnormal exit detected'

    @catch_thread_exceptions
    @mock_start_localservices_wrapper
    @patch("dragon.launcher.backend.start_overlay_network")
    @patch("dragon.launcher.backend.NodeDescriptor.get_local_node_network_conf")
    def test_garbled_shchannelsup(self, exceptions_caught_in_threads,
                                  mock_localservices_tuple, mock_network_config, mock_overlay):
        '''Test that we exit correctly if the SHChannelsUp from LS is garbled'''

        log = logging.getLogger('test_garbled_shchannelsup')

        # Get our frontend crap going so we can get the overlay up
        args_map = self.be_helper.handle_network_and_frontend_start(mock_network_config)
        self.start_backend_thread(args_map)

        # Cleanly start everything up to SHChannelsUp
        self.be_helper.start_ls(mock_overlay, mock_localservices_tuple, garble_shchannelsup=True)

        # Confirm we send out an abnormal term to the frontend so it can
        # issue a GSTeardown to the other nodes
        self.be_helper.start_abnormal_termination_exit(log, gs_up=False, ls_up=False)

        # Clean up local services
        self.be_helper.cleanup()

        # At this point, our LS mock should throw its side-effect:
        self.be_thread.join()

        log.info(f'exceptions: {exceptions_caught_in_threads}')
        assert 'Backend Server' in exceptions_caught_in_threads
        assert exceptions_caught_in_threads['Backend Server']['exception']['type'] == RuntimeError
        assert str(exceptions_caught_in_threads['Backend Server']['exception']['value']) == 'Abnormal exit detected'

    @catch_thread_exceptions
    @mock_start_localservices_wrapper
    @patch("dragon.launcher.backend.start_overlay_network")
    @patch("dragon.launcher.backend.NodeDescriptor.get_local_node_network_conf")
    def test_app_exec_abnormal_term_from_frontend(self, exceptions_caught_in_threads,
                                                  mock_localservices_tuple, mock_network_config, mock_overlay):
        '''Test that we exit if we get an AbnormalTermination from the frontend during app execution'''

        log = logging.getLogger('test_app_exec_abnormal_term_from_frontend')

        # Get our frontend crap going so we can get the overlay up
        args_map = self.be_helper.handle_network_and_frontend_start(mock_network_config)
        self.start_backend_thread(args_map)

        # Cleanly start everything up to SHChannelsUp
        self.be_helper.clean_startup(log, mock_overlay, mock_network_config, mock_localservices_tuple)
        self.be_helper.launch_user_process(log)

        self.be_helper.localservices.send_AbnormalTermination()

        self.be_helper.start_abnormal_termination_exit(log)

        # Clean up local services
        self.be_helper.cleanup()
        log.debug('cleaned up backend helpers')

        # Should be able to join here
        self.be_thread.join()
        log.debug('cleaned up backend')

        log.info(f'exceptions: {exceptions_caught_in_threads}')
        assert 'Backend Server' in exceptions_caught_in_threads
        assert exceptions_caught_in_threads['Backend Server']['exception']['type'] == RuntimeError
        assert str(exceptions_caught_in_threads['Backend Server']['exception']['value']) == 'Abnormal exit detected'

    @catch_thread_exceptions
    @mock_start_localservices_wrapper
    @patch("dragon.launcher.backend.start_overlay_network")
    @patch("dragon.launcher.backend.NodeDescriptor.get_local_node_network_conf")
    def test_early_abnormal_term_from_frontend(self, exceptions_caught_in_threads,
                                               mock_localservices_tuple, mock_network_config, mock_overlay):
        '''Test that we exit if we get an AbnormalTermination from the frontend during app execution'''

        log = logging.getLogger('test_early_abnormal_term_from_frontend')

        # Get our frontend crap going so we can get the overlay up
        args_map = self.be_helper.handle_network_and_frontend_start(mock_network_config)
        self.start_backend_thread(args_map)

        # Cleanly start everything to LS returning its SHChannelsUp
        self.be_helper.start_ls(mock_overlay, mock_localservices_tuple, abort_lachannelsinfo=True)

        # Confirm we send out an abnormal term to the frontend so it can
        # issue a GSTeardown to the other nodes
        self.be_helper.start_abnormal_termination_exit(log, gs_up=False)

        # Clean up local services
        self.be_helper.cleanup()

        # At this point, our LS mock should throw its side-effect:
        self.be_thread.join()

        log.debug(f'exceptions: {exceptions_caught_in_threads}')
        assert 'Backend Server' in exceptions_caught_in_threads
        assert exceptions_caught_in_threads['Backend Server']['exception']['type'] == RuntimeError
        assert str(exceptions_caught_in_threads['Backend Server']['exception']['value']) == 'Abnormal exit detected'

    @catch_thread_exceptions
    @mock_start_localservices_wrapper
    @patch("dragon.launcher.backend.start_overlay_network")
    @patch("dragon.launcher.backend.NodeDescriptor.get_local_node_network_conf")
    def test_late_abnormal_term_from_frontend(self, exceptions_caught_in_threads,
                                              mock_localservices_tuple, mock_network_config, mock_overlay):
        '''Test that we exit if we get an AbnormalTermination from the frontend during app execution'''

        log = logging.getLogger('test_late_abnormal_term_from_frontend')

        # Get our frontend crap going so we can get the overlay up
        args_map = self.be_helper.handle_network_and_frontend_start(mock_network_config)
        self.start_backend_thread(args_map)

        # Cleanly start everything up to SHChannelsUp
        self.be_helper.clean_startup(log, mock_overlay, mock_network_config, mock_localservices_tuple)
        self.be_helper.launch_user_process(log)
        self.be_helper.clean_shutdown(log, shteardown_abort=True)

        # Clean up local services
        self.be_helper.cleanup()

        # Should be able to join here
        self.be_thread.join()

        log.info(f'exceptions: {exceptions_caught_in_threads}')
        assert 'Backend Server' in exceptions_caught_in_threads
        assert exceptions_caught_in_threads['Backend Server']['exception']['type'] == RuntimeError
        assert str(exceptions_caught_in_threads['Backend Server']['exception']['value']) == 'Abnormal exit detected'


if __name__ == "__main__":
    # Use the fork method for these so I can do mocking inside the mp process.
    unittest.main()
