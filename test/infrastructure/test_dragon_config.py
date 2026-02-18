import unittest
import tempfile
import os
import json
import subprocess

from unittest.mock import patch
from contextlib import redirect_stdout
from io import StringIO

import dragon.infrastructure.config as dconf
import dragon.launcher.wlm as dwlm
from dragon.transport.util import get_fabric_backend
from dragon.launcher.frontend import LauncherFrontEnd
from dragon.launcher.launchargs import get_args as get_cli_args
from dragon.infrastructure.facts import (
    HighSpeedTransportBackends,
    DEFAULT_OVERLAY_NETWORK_PORT,
    DEFAULT_TRANSPORT_NETIF,
)

# Create a PBS Nodefile we'll delete with the completion of that test
PBS_NODEFILE = tempfile.NamedTemporaryFile(delete=False)


class DragonConfigTest(unittest.TestCase):
    def setUp(self):
        # Define the parser
        self.parser = dconf._configure_parser()
        self.add_keys = dconf.ADD_KEYS
        self.test_keys = dconf.TEST_KEYS

    def _get_valid_add_args(self):
        # Make some paths and fill in args for non-path options
        non_path_options = {"tcp_runtime", "netconfig_mpiexec_override", "backend_mpiexec_override"}

        add_options = {
            option: tempfile.TemporaryDirectory() for option in self.add_keys if option not in non_path_options
        }

        # Make sure the expected files exist for each of these options
        for option, temp_dir in add_options.items():
            files = dconf._concat_search_files(temp_dir.name, dconf.ROUTING_ENUM[option])
            for file in files:
                os.makedirs(os.path.dirname(file), exist_ok=True)
                with open(file, "w") as f:
                    f.write("fasdfdasgha")

        args_list = ["add"] + [f"--{key.replace('_', '-')}={val.name}" for key, val in add_options.items()]

        add_options["tcp_runtime"] = True
        add_options[
            "netconfig_mpiexec_override"
        ] = "mpiexec --np {nnodes} --map-by ppr:1:node --stream-buffering=1 --tag-output"
        add_options[
            "backend_mpiexec_override"
        ] = "mpiexec --np {nnodes} --map-by ppr:1:node --stream-buffering=1 --tag-output --host {nodelist}"

        args_list.append("--tcp-runtime")
        args_list.append("--netconfig-mpiexec-override")
        args_list.append(f"{add_options['netconfig_mpiexec_override']}")
        args_list.append("--backend-mpiexec-override")
        args_list.append(str(add_options["backend_mpiexec_override"]))

        return add_options, args_list

    def _get_valid_test_args(self):
        # test that MPI libraries are correctly managed
        test_options = {option: tempfile.TemporaryDirectory() for option in self.test_keys}

        # Make sure the expected files exist for each of these options
        for option, temp_dir in test_options.items():
            files = dconf._concat_search_files(temp_dir.name, dconf.ROUTING_ENUM[option])
            for file in files:
                os.makedirs(os.path.dirname(file), exist_ok=True)
                with open(file, "w") as f:
                    f.write("fasdfdasgha")

        args_list = ["test"] + [f"--{key.replace('_', '-')}={val.name}" for key, val in test_options.items()]

        return test_options, args_list

    def test_valid_add_subparser(self):
        """Test that all valid add subparser options work as expected"""
        add_options, args_list = self._get_valid_add_args()

        # Run the parser and check the results
        with tempfile.NamedTemporaryFile() as config_file, tempfile.NamedTemporaryFile() as makefile:
            config_args = self.parser.parse_args(args_list)
            base_dir = os.path.dirname(config_file.name)

            dconf._handle_subparsers(config_args, base_dir, config_file.name, makefile.name)

            # Clean up all the temp dirs
            for option, temp_dir in add_options.items():
                if isinstance(temp_dir, tempfile.TemporaryDirectory):
                    temp_dir.cleanup()

            # Make sure the results match
            for key, val in add_options.items():
                x = subprocess.run(
                    ["dragon-config", f"--config-file={config_file.name}", "-g", key], capture_output=True
                )
                output = x.stdout.decode().strip()
                try:
                    expected_output = str(val.name)
                    self.assertEqual(expected_output, output)
                except AttributeError:
                    self.assertIn(val, [output, True])

    def test_invalid_add_subparser(self):
        """Make sure invalid add subparser options raise errors"""

        # Make some paths and fill in args for non-path options
        non_path_options = {"tcp_runtime", "netconfig_mpiexec_override", "backend_mpiexec_override"}
        add_options = {
            option: tempfile.TemporaryDirectory() for option in self.add_keys if option not in non_path_options
        }

        args_options = []
        # Make sure the expected files exist for each of these options
        for option, temp_dir in add_options.items():
            files = dconf._concat_search_files(temp_dir.name, dconf.ROUTING_ENUM[option])

            # If the config tool is expecting files to exist, don't create them
            if len(files) != 0:
                args_options.append(f"--{option.replace('_', '-')}={temp_dir.name}")
            else:
                temp_dir.cleanup()

        # Run the parser and check the results
        with tempfile.NamedTemporaryFile() as config_file, tempfile.NamedTemporaryFile() as makefile:
            base_dir = os.path.dirname(config_file.name)
            for option in args_options:
                config_args = self.parser.parse_args(["add", option])

                captured_output = StringIO()
                with redirect_stdout(captured_output):
                    with self.assertRaises(SystemExit):
                        dconf._handle_subparsers(config_args, base_dir, config_file.name, makefile.name)
                    x = captured_output.getvalue()
                self.assertIn("Make sure file paths have been set correctly", x)

            # Clean up all the temp dirs
            for option, temp_dir in add_options.items():
                if isinstance(temp_dir, tempfile.TemporaryDirectory):
                    temp_dir.cleanup()

    def test_valid_test_subparser(self):
        """Make sure we correctly handle valid paths for the test subparser"""

        test_options, args_list = self._get_valid_test_args()

        # Run the parser and check the results
        with tempfile.NamedTemporaryFile() as config_file, tempfile.NamedTemporaryFile() as makefile:
            config_args = self.parser.parse_args(args_list)
            base_dir = os.path.dirname(config_file.name)

            dconf._handle_subparsers(config_args, base_dir, config_file.name, makefile.name)

            # Clean up all the temp dirs
            for option, temp_dir in test_options.items():
                if isinstance(temp_dir, tempfile.TemporaryDirectory):
                    temp_dir.cleanup()

            # Make sure the results match
            for key, val in test_options.items():
                x = subprocess.run(
                    ["dragon-config", f"--config-file={config_file.name}", "-g", key], capture_output=True
                )
                output = x.stdout.decode().strip()
                try:
                    expected_output = str(val.name)
                    self.assertEqual(expected_output, output)
                except AttributeError:
                    self.assertIn(val, [output, True])

    def test_fully_loaded_config_file(self):
        """Make sure we don't choke on a maximally defined config file"""

        # Run the parser and check the results
        with tempfile.NamedTemporaryFile() as config_file, tempfile.NamedTemporaryFile() as makefile:
            add_options, add_args_list = self._get_valid_add_args()
            test_options, test_args_list = self._get_valid_test_args()

            add_config_args = self.parser.parse_args(add_args_list)
            test_config_args = self.parser.parse_args(test_args_list)
            base_dir = os.path.dirname(config_file.name)

            dconf._handle_subparsers(add_config_args, base_dir, config_file.name, makefile.name)
            dconf._handle_subparsers(test_config_args, base_dir, config_file.name, makefile.name)

            # Clean up all the temp dirs
            all_options = test_options | add_options
            for option, temp_dir in all_options.items():
                if isinstance(temp_dir, tempfile.TemporaryDirectory):
                    temp_dir.cleanup()

            # Make sure the results match
            for key, val in all_options.items():
                x = subprocess.run(
                    ["dragon-config", f"--config-file={config_file.name}", "-g", key], capture_output=True
                )
                output = x.stdout.decode().strip()
                try:
                    expected_output = str(val.name)
                    self.assertEqual(expected_output, output)
                except AttributeError:
                    self.assertIn(val, [output, True])

            # And make sure we return a sane fabric backend
            backend_name, backend_lib = get_fabric_backend(config_file=config_file.name)
            self.assertTrue(backend_name in set(HighSpeedTransportBackends))

    def test_invalid_test_subparser(self):
        """Make sure we correctly handle invalid paths for the test subparser"""

        # test that MPI libraries are correctly managed
        test_options = {option: tempfile.TemporaryDirectory() for option in self.test_keys}

        args_options = []
        # Make sure the expected files exist for each of these options
        for option, temp_dir in test_options.items():
            files = dconf._concat_search_files(temp_dir.name, dconf.ROUTING_ENUM[option])

            # If the config tool is expecting files to exist, don't create them
            if len(files) != 0:
                args_options.append(f"--{option.replace('_', '-')}={temp_dir.name}")
            else:
                temp_dir.cleanup()

        # Run the parser and check the results
        with tempfile.NamedTemporaryFile() as config_file, tempfile.NamedTemporaryFile() as makefile:
            base_dir = os.path.dirname(config_file.name)
            for option in args_options:
                config_args = self.parser.parse_args(["test", option])

                captured_output = StringIO()
                with redirect_stdout(captured_output):
                    with self.assertRaises(SystemExit):
                        dconf._handle_subparsers(config_args, base_dir, config_file.name, makefile.name)
                    x = captured_output.getvalue()
                self.assertIn("Make sure file paths have been set correctly", x)

            # Clean up all the temp dirs
            for option, temp_dir in test_options.items():
                if isinstance(temp_dir, tempfile.TemporaryDirectory):
                    temp_dir.cleanup()

    def test_lists_of_paths(self):
        # Make sure we behave sensibly when we're giving a list of paths

        # Make sure the expected files exist for each of these options
        option = "ofi_runtime_lib"
        dirs = [tempfile.TemporaryDirectory() for _ in range(3)]

        for temp_dir in dirs:
            files = dconf._concat_search_files(temp_dir.name, option)
            for file in files:
                os.makedirs(os.path.dirname(file), exist_ok=True)
                with open(file, "w") as f:
                    f.write("fasdfdasgha")

        args_list = ["add", f"--{option.replace('_', '-') }=" + ":".join([d.name for d in dirs])]

        # Run the parser and check the results
        with tempfile.NamedTemporaryFile() as config_file, tempfile.NamedTemporaryFile() as makefile:
            config_args = self.parser.parse_args(args_list)
            base_dir = os.path.dirname(config_file.name)

            dconf._handle_subparsers(config_args, base_dir, config_file.name, makefile.name)

            # Clean up all the temp dirs
            for d in dirs:
                if isinstance(d, tempfile.TemporaryDirectory):
                    d.cleanup()

            backend_name, backend_lib = get_fabric_backend(config_file=config_file.name)

            self.assertEqual(backend_name, "ofi")
            self.assertEqual(":".join([d.name for d in dirs]), backend_lib)

    def test_tcp_error_msg(self):
        """Make sure the frontend raises the TCP is being used error, or ignores it if told to"""

        args_map = get_cli_args(["hello_world.py"])
        error_string = "To eliminate this message and continue to use the TCP transport agent"

        with tempfile.NamedTemporaryFile() as config_file, tempfile.NamedTemporaryFile() as makefile:
            captured_output = StringIO()

            with redirect_stdout(captured_output):
                LauncherFrontEnd._determine_transport_backend(args_map.get("transport"), config_file=config_file.name)

            # Replace newlines with spaces:
            out = captured_output.getvalue().replace("\n", " ")
            self.assertIn(error_string, out)

            # Now write out a config file that uses TCP intentionally and make sure it works
            config_args = self.parser.parse_args(["add", "--tcp-runtime"])
            base_dir = os.path.dirname(config_file.name)

            dconf._handle_subparsers(config_args, base_dir, config_file.name, makefile.name)

            captured_output = StringIO()
            with redirect_stdout(captured_output):
                LauncherFrontEnd._determine_transport_backend(args_map.get("transport"), config_file=config_file.name)

            out = captured_output.getvalue().replace("\n", " ")
            self.assertNotIn(error_string, out)

    def test_location_placement(self):
        """Make sure the config file goes to the expected placed"""

        # We should always be putting the files in the root python installation path.
        # Anywhere else will make behavior between release builds and develop builds i
        # inconsistent.

        makefile, config_file, _, __ = dconf._get_config_filenames()

        # Function to check if 'dragon' is in the parent directories
        def is_in_dragon_subdirectory(file_path):
            parts = os.path.normpath(file_path).split(os.sep)
            return "dragon" in parts[-3:-1]  # Check 1 or 2 directories above

        # Assertions
        assert is_in_dragon_subdirectory(makefile), "makefile is not in a 'dragon' subdirectory"
        assert is_in_dragon_subdirectory(config_file), "config_file is not in a 'dragon' subdirectory"

    def test_using_old_config(self):
        """Throw an error if using older hyphen-formatting and need to update config"""

        ddict_args = {"ofi-runtime-lib": "/some/path/to/nowhere/libfabric.so"}

        # Create a temporary file
        with tempfile.NamedTemporaryFile() as temp_file:
            # Dump the dictionary to a JSON file, as we do
            with open(temp_file.name, "w") as json_file:
                json.dump(ddict_args, json_file)

            # Run the file through the backend fabric detector and see what we come out with
            with self.assertRaises(ValueError) as e:
                backend_name, backend_lib = get_fabric_backend(config_file=temp_file.name)
                self.assertIn("dragon-config -c", str(e.exception))

    @patch.dict(os.environ, {"PBS_NODEFILE": PBS_NODEFILE.name})
    def test_mpiexec_override(self):
        """Make sure we correctly use the mpiexec override"""

        netconfig_override_str = "mpiexec --np {nnodes} --map-by ppr:1:node --stream-buffering=1 --tag-output"
        backend_override_str = (
            "mpiexec --np {nnodes} --map-by ppr:1:node --stream-buffering=1 --tag-output --host {nodelist}"
        )

        args_list = [
            "add",
            "--netconfig-mpiexec-override",
            netconfig_override_str,
            "--backend-mpiexec-override",
            f"{backend_override_str}",
        ]

        # Run the PBS WLM stuff and make sure it picks up the changes
        hostlist = []
        port = DEFAULT_OVERLAY_NETWORK_PORT
        network_prefix = DEFAULT_TRANSPORT_NETIF

        # Put some made up nodes in the PBS nodefile
        nnodes = 5
        nodelist = []
        with open(PBS_NODEFILE.name, "w") as f:
            for i in range(nnodes):
                node_name = f"node{i+1}\n"
                nodelist.append(node_name)
                f.write(node_name)
                f.flush()

        with tempfile.NamedTemporaryFile() as config_file, tempfile.NamedTemporaryFile() as makefile:
            config_args = self.parser.parse_args(args_list)
            base_dir = os.path.dirname(config_file.name)

            dconf._handle_subparsers(config_args, base_dir, config_file.name, makefile.name)

            pbs_wlm = dwlm.PBSWLM(network_prefix, port, hostlist, config_file=config_file.name)

            # Check the net config was correct:
            self.assertEqual(netconfig_override_str.format(nnodes=nnodes).split(), pbs_wlm.MPIEXEC_ARGS)

            # Check the backend config was correct:
            args_map = {"nnodes": nnodes, "nodelist": ",".join(nodelist).strip()}
            backend = ["backend"]
            launchargs = pbs_wlm._get_wlm_launch_be_args(args_map, backend, config_file=config_file.name)
            self.assertEqual(backend_override_str.format(**args_map).split() + backend, launchargs)

        # Remove the nodefile
        os.remove(PBS_NODEFILE.name)
