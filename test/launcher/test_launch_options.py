#!/usr/bin/env python3
import unittest
import logging
import threading
from os import environ, path
from io import StringIO
from unittest.mock import patch
from dragon.launcher.launchargs import get_parser
from dragon.launcher import launch_selector as dls

from .launcher_testing_utils import catch_thread_exceptions
from .frontend_testing_mocks import run_frontend


def get_args_map(network_config, network_prefix='^(eth|hsn)', **kwargs):

    parser = get_parser()
    arg_list = ['--wlm', 'slurm',
                '--network-config', f'{network_config}',
                '--network-prefix', network_prefix]
    for val in kwargs.values():
        arg_list = arg_list + val

    arg_list.append('hello_world.py')
    args = parser.parse_args(args=arg_list)
    if args.basic_label or args.verbose_label:
        args.no_label = False
    args_map = {key: value for key, value in vars(args).items() if value is not None}

    return args_map


class LaunchSelectionTest(unittest.TestCase):

    @patch('sys.argv', ['dragon', 'dummy.py'])
    @patch('shutil.which', return_value=None)
    def test_single_automatic_selection(self, mock_which):
        """Route to the single node launcher"""
        multi_mode = dls.determine_environment()
        self.assertFalse(multi_mode)

    @patch('sys.argv', ['dragon', 'dummy.py'])
    @patch('shutil.which')
    @patch.dict(environ, {"SLURM_JOB_ID": "3195234.0"})
    def test_multinode_automatic_selection_slurm(self, mock_which):
        """Route to the multinode launcher"""
        mock_which.side_effect = ['/opt/slurm/bin/qstat', '/usr/bin/srun', None]
        multi_mode = dls.determine_environment()
        self.assertTrue(multi_mode)

    @patch('sys.argv', ['dragon', 'dummy.py'])
    @patch('shutil.which')
    @patch.dict(environ, {"SLURM_JOB_ID": ""})
    def test_multinode_auto_without_allocation_slurm(self, mock_which):
        """Raise exception due to slurm being present with no job allocation"""
        mock_which.side_effect = ['/opt/slurm/bin/qstat', '/usr/bin/srun', None]
        with self.assertRaises(RuntimeError) as e:
            dls.determine_environment()
        self.assertTrue('Executing in a Slurm environment, but with no job allocation' in str(e.exception))

    @patch('sys.argv', ['dragon', 'dummy.py'])
    @patch('shutil.which')
    @patch.dict(environ, {"PBS_NODEFILE": ""})
    def test_multinode_auto_without_allocation_pbs_bad_mpiexec(self, mock_which):
        """Raise exception due to PBS  being present with no job allocation and a bad mpiexec in path"""
        mock_which.side_effect = ['/opt/pbs/bin/qstat', None, '/usr/bin/mpiexec']
        with self.assertRaises(RuntimeError) as e:
            dls.determine_environment()
        print(f'{str(e.exception)}')
        self.assertTrue('PBS has been detected on the system. However, Dragon is only' in str(e.exception))

    @patch('sys.argv', ['dragon', 'dummy.py'])
    @patch('shutil.which')
    @patch.dict(environ, {"PBS_NODEFILE": "/a/presumably/valid/path"})
    def test_multinode_auto_with_allocation_pbs_bad_mpiexec(self, mock_which):
        """Raise exception due to PBS being present with job allocation but a bad mpiexec in path"""
        mock_which.side_effect = ['/opt/pbs/bin/qstat', None, '/usr/bin/mpiexec']
        with self.assertRaises(RuntimeError) as e:
            dls.determine_environment()
        print(f'{str(e.exception)}')
        self.assertTrue('PBS has been detected on the system. However, Dragon is only' in str(e.exception))

    @patch('sys.argv', ['dragon', 'dummy.py'])
    @patch('shutil.which')
    @patch.dict(environ, {"PBS_NODEFILE": "/a/presumably/valid/path"})
    def test_multinode_auto_with_allocation_pbs_good_mpiexec(self, mock_which):
        """Correctly identify we're doing PBS and PALS"""
        mock_which.side_effect = ['/opt/pbs/bin/qstat', None, '/opt/cray/pe/pals/default/bin/mpiexec']
        multi_mode = dls.determine_environment()
        self.assertTrue(multi_mode)

    @patch('sys.argv', ['dragon', 'dummy.py'])
    @patch('shutil.which')
    @patch.dict(environ, {"PBS_NODEFILE": ""})
    def test_multinode_auto_without_allocation_pbs_good_mpiexec(self, mock_which):
        """Correctly identify we're doing PBS and PALS"""
        mock_which.side_effect = ['/opt/pbs/bin/qstat', None, '/opt/cray/pe/pals/default/bin/mpiexec']
        with self.assertRaises(RuntimeError) as e:
            dls.determine_environment()
        self.assertTrue('Using a supported PALS with PBS config. However, no active jobs allocation' in str(e.exception))

    @patch('sys.argv', ['dragon', '--single-node-override', 'dummy.py'])
    def test_single_node_override(self):
        """Test go to single node if requested"""
        self.assertFalse(dls.determine_environment())

    @patch('sys.argv', ['dragon', '--multi-node-override', 'dummy.py'])
    def test_multi_node_override(self):
        """Test go to multi node if requested"""
        self.assertTrue(dls.determine_environment())

    @patch('sys.stderr', new_callable=StringIO)
    @patch('sys.argv', ['dragon', '--single-node-override', '--multi-node-override', 'dummy.py'])
    def test_bad_override(self, mock_stderr):
        """Test erroneous request of multi and single node"""
        with self.assertRaises(SystemExit):
            dls.determine_environment()
        self.assertRegexpMatches(mock_stderr.getvalue(), r" not allowed with argument")


class LaunchOptionsTest(unittest.TestCase):

    def setUp(self):
        self.test_dir = path.dirname(path.realpath(__file__))
        self.network_config = path.join(self.test_dir, 'slurm_primary.yaml')

    @catch_thread_exceptions
    @patch('dragon.launcher.frontend.LauncherFrontEnd._launch_backend')
    @patch('dragon.launcher.frontend.start_overlay_network')
    def test_too_many_nodes_requested(self, exceptions_caught_in_threads, mock_overlay, mock_launch):
        """What happens when a user requests more nodes than they have in an allocation"""

        args_map = get_args_map(self.network_config,
                                arg1=['--nodes', '10010'])

        # get startup going in another thread. Note: need to do threads in order to use
        # all our mocks
        fe_proc = threading.Thread(name='Frontend Server',
                                   target=run_frontend,
                                   args=(args_map,),
                                   daemon=False)
        fe_proc.start()
        fe_proc.join()

        assert 'Frontend Server' in exceptions_caught_in_threads  # there was an exception in thread  1
        assert exceptions_caught_in_threads['Frontend Server']['exception']['type'] == ValueError
        assert str(exceptions_caught_in_threads['Frontend Server']['exception']['value']) \
            == 'Not enough backend nodes allocated to match requested'

    @patch('sys.stderr', new_callable=StringIO)
    def test_nonint_nodes_requested(self, mock_stderr):
        """what happens when users passes a non-int for # nodes"""

        with self.assertRaises(SystemExit):
            get_args_map(self.network_config,
                         arg1=['--nodes', 'abcdef'])
        self.assertTrue("invalid non_negative_int value: 'abcdef'" in mock_stderr.getvalue())

    @patch('sys.stderr', new_callable=StringIO)
    def test_bad_port_requested(self, mock_stderr):
        """what happens when user passes a bad value to port"""

        with self.assertRaises(SystemExit):
            get_args_map(self.network_config,
                         arg1=['--port', '23'])
        self.assertTrue("must be in port range 1024-65535" in mock_stderr.getvalue())

        with self.assertRaises(SystemExit):
            get_args_map(self.network_config,
                         arg1=['--port', '799999'])
        self.assertTrue("must be in port range 1024-65535" in mock_stderr.getvalue())

        with self.assertRaises(SystemExit):
            get_args_map(self.network_config,
                         arg1=['--port', 'abcdef'])
        self.assertTrue("invalid valid_port_int value" in mock_stderr.getvalue())

    @catch_thread_exceptions
    @patch('dragon.launcher.frontend.LauncherFrontEnd._launch_backend')
    @patch('dragon.launcher.frontend.start_overlay_network')
    def test_bad_network_prefix(self, exceptions_caught_in_threads, mock_overlay, mock_launch):
        """What happens when user requests a network prefix that doesn't exist"""
        args_map = get_args_map(self.network_config,
                                network_prefix='garbage-prefix')

        # get startup going in another thread. Note: need to do threads in order to use
        # all our mocks
        fe_proc = threading.Thread(name='Frontend Server',
                                   target=run_frontend,
                                   args=(args_map,),
                                   daemon=False)
        fe_proc.start()
        fe_proc.join()

        assert 'Frontend Server' in exceptions_caught_in_threads  # there was an exception in thread  1
        assert exceptions_caught_in_threads['Frontend Server']['exception']['type'] == ValueError
        self.assertTrue('No IP addresses found for' in
                        str(exceptions_caught_in_threads['Frontend Server']['exception']['value']))
        self.assertTrue('matching regex pattern: garbage-prefix' in
                        str(exceptions_caught_in_threads['Frontend Server']['exception']['value']))


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    unittest.main()
