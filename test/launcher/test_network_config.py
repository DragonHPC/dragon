#!/usr/bin/env python3
import unittest
import logging
import subprocess
import json
from os import environ, path
import random
import string


from dragon.launcher.network_config import WLM, ConfigOutputType, NetworkConfig
from dragon.infrastructure.node_desc import NodeDescriptor
from dragon.infrastructure.facts import DEFAULT_TRANSPORT_NETIF, DEFAULT_OVERLAY_NETWORK_PORT


class FileNetworkConfigTest(unittest.TestCase):

    def setUp(self):
        """Read a network config from file"""
        base_abs_path = path.dirname(__file__)
        self.bad_yaml = path.join(base_abs_path, "slurm_bad.yaml")
        self.good_yaml = path.join(base_abs_path, "slurm.yaml")
        self.good_primary_yaml = path.join(base_abs_path, "slurm_primary.yaml")
        self.bad_json = path.join(base_abs_path, "slurm_bad.json")
        self.good_json = path.join(base_abs_path, "slurm.json")

    def test_good_input_yaml_parser(self):
        net_conf = NetworkConfig.from_file(self.good_yaml, ConfigOutputType.YAML)
        self.assertIsInstance(net_conf, NetworkConfig)

        config = net_conf.get_network_config()
        for key, value in config.items():
            self.assertIsInstance(key, str)
            self.assertIsInstance(value, NodeDescriptor)

    def test_good_input_json_parser(self):
        net_conf = NetworkConfig.from_file(self.good_json, ConfigOutputType.JSON)
        self.assertIsInstance(net_conf, NetworkConfig)

        config = net_conf.get_network_config()
        for key, value in config.items():
            self.assertIsInstance(key, str)
            self.assertIsInstance(value, NodeDescriptor)

    def test_bad_input_yaml_parser(self):
        with self.assertRaises(TypeError) as e:
            NetworkConfig.from_file(self.bad_yaml, ConfigOutputType.YAML)
        self.assertTrue("got an unexpected keyword argument 'junk-key'" in str(e.exception))

    def test_bad_input_json_parser(self):
        with self.assertRaises(TypeError) as e:
            NetworkConfig.from_file(self.bad_json, ConfigOutputType.JSON)
        self.assertTrue("got an unexpected keyword argument 'junk-key'" in str(e.exception))

    def test_primary_from_file_selection(self):
        """Confirm we behave if primary is given in file"""
        net = NetworkConfig.from_file(self.good_primary_yaml, ConfigOutputType.YAML)
        self.assertIsInstance(net, NetworkConfig)

        conf = net.get_network_config()
        primary_keys = [k for k, v in conf.items() if conf[k].is_primary]

        # Should only be 1
        self.assertEqual(len(primary_keys), 1)

        # YAML has node index 1 selected
        self.assertEqual(primary_keys[0], "1")

    def test_no_primary_from_file_selection(self):
        """Confirm selection by host id works correctly"""
        net = NetworkConfig.from_file(self.good_yaml, ConfigOutputType.YAML)
        self.assertIsInstance(net, NetworkConfig)

        conf = net.get_network_config()
        primary_keys = [k for k, v in conf.items() if conf[k].is_primary]

        # Should only be 1
        self.assertEqual(len(primary_keys), 1)

        # '0' has the smallest host ID and should be selected as primary
        self.assertEqual(primary_keys[0], "0")

    def test_no_primary_with_input_from_file_selection(self):
        """Confirm we can override default behavior with specific hostname"""
        prime_node = "nid00006"
        net = NetworkConfig.from_file(self.good_yaml, ConfigOutputType.YAML, primary_hostname=prime_node)
        self.assertIsInstance(net, NetworkConfig)

        conf = net.get_network_config()
        primary_keys = [k for k, v in conf.items() if conf[k].is_primary]

        # Should only be 1
        self.assertEqual(len(primary_keys), 1)

        # Every primary node should be at '0', but we can
        # confirm the correct name
        self.assertEqual(conf["0"].name, prime_node)

    def test_primary_with_input_from_file_selection(self):
        """Confirm we can override always ignore primary hostname if on in config file specific hostname"""
        with self.assertRaises(RuntimeError) as e:
            NetworkConfig.from_file(self.good_primary_yaml, ConfigOutputType.YAML, primary_hostname="nid00006")
        self.assertTrue("Primary hostname input by user but is already set in network config file" in str(e.exception))

        with self.assertRaises(RuntimeError) as e:
            NetworkConfig.from_file(self.good_primary_yaml, ConfigOutputType.YAML, primary_hostname="nid0000654")
        self.assertTrue("Primary hostname input by user but is already set in network config file" in str(e.exception))

    def test_nonexistent_primary_node(self):
        """Confirm we can raise an error if requested primary doesn't exist and one isn't already selected"""
        with self.assertRaises(RuntimeError) as e:
            NetworkConfig.from_file(self.good_yaml, ConfigOutputType.YAML, primary_hostname="nid0000654")
        self.assertTrue("Input hostname does not match any available" in str(e.exception))


class SlurmNetworkConfigTest(unittest.TestCase):

    def setUp(self):
        """Set values specific to slurm"""
        self.wlm = WLM.SLURM
        self.config_files = {}

    def test_slurm(self):

        # Only run this test if we have a slurm allocation
        if not environ.get("SLURM_JOB_ID"):
            logging.info("Slurm test is checking for error")
            with self.assertRaises(RuntimeError):
                net = NetworkConfig.from_wlm(
                    workload_manager=WLM.SLURM,
                    port=DEFAULT_OVERLAY_NETWORK_PORT,
                    network_prefix=DEFAULT_TRANSPORT_NETIF,
                )
        else:
            logging.info("slurm test launched backend config jobs")
            net = NetworkConfig.from_wlm(
                workload_manager=WLM.SLURM, port=DEFAULT_OVERLAY_NETWORK_PORT, network_prefix=DEFAULT_TRANSPORT_NETIF
            )
            self.assertIsInstance(net, NetworkConfig)

            config = net.get_network_config()
            for key, value in config.items():
                self.assertIsInstance(key, str)
                self.assertIsInstance(value, NodeDescriptor)

    def test_slurm_stdout(self):
        args = ["python3", "-m", "dragon.launcher.network_config", "--wlm", "slurm"]
        if not environ.get("SLURM_JOB_ID"):
            logging.info("Slurm test is checking for error")
            try:
                proc = subprocess.run(args=args, capture_output=True)
            except Exception as e:
                isinstance(e, RuntimeError)
        else:
            logging.info("slurm test launched backend config jobs")
            proc = subprocess.run(args=args, timeout=30, capture_output=True, text=True)
            net = NetworkConfig.from_sdict(json.loads(proc.stdout))
            self.assertIsInstance(net, NetworkConfig)

            config = net.get_network_config()
            for key, value in config.items():
                self.assertIsInstance(key, str)
                self.assertIsInstance(value, NodeDescriptor)

    def test_slurm_bad_network_prefix(self):
        args = ["python3", "-m", "dragon.launcher.network_config", "--wlm", "slurm", "--network-prefix", "garbage"]

        if environ.get("SLURM_JOB_ID"):
            proc = subprocess.run(args=args, capture_output=True, text=True)
            self.assertNotEqual(proc.returncode, 0)
            self.assertTrue("ValueError: No IP addresses found for" in proc.stderr)
            self.assertTrue(" matching regex pattern: garbage" in proc.stderr)


class PbsPalsNetworkConfigTest(unittest.TestCase):

    def setUp(self):
        """Set values specific to PBS+PALS"""
        self.wlm = WLM.PBS_PALS
        self.config_files = {}

    def test_pbs_pals(self):

        # Only run this test if we have a slurm allocation
        if not environ.get("PBS_NODEFILE"):
            logging.info("PBS+PALS test is checking for error")
            with self.assertRaises(RuntimeError):
                net = NetworkConfig.from_wlm(
                    workload_manager=WLM.PBS_PALS,
                    port=DEFAULT_OVERLAY_NETWORK_PORT,
                    network_prefix=DEFAULT_TRANSPORT_NETIF,
                )
        else:
            logging.info("PBS+PALS test launched backend config jobs")
            net = NetworkConfig.from_wlm(
                workload_manager=WLM.PBS_PALS, port=DEFAULT_OVERLAY_NETWORK_PORT, network_prefix=DEFAULT_TRANSPORT_NETIF
            )
            self.assertIsInstance(net, NetworkConfig)

            config = net.get_network_config()
            for key, value in config.items():
                self.assertIsInstance(key, str)
                self.assertIsInstance(value, NodeDescriptor)

    def test_pbs_pals_stdout(self):
        args = ["python3", "-m", "dragon.launcher.network_config", "--wlm", "pbs+pals"]
        if not environ.get("PBS_NODEFILE"):
            logging.info("PBS+PALS test is checking for error")
            try:
                proc = subprocess.run(args=args, capture_output=True)
            except Exception as e:
                isinstance(e, RuntimeError)
        else:
            logging.info("PBS+PALS test launched backend config jobs")
            proc = subprocess.run(args=args, timeout=30, capture_output=True, text=True)
            net = NetworkConfig.from_sdict(json.loads(proc.stdout))
            self.assertIsInstance(net, NetworkConfig)

            config = net.get_network_config()
            for key, value in config.items():
                self.assertIsInstance(key, str)
                self.assertIsInstance(value, NodeDescriptor)

    def test_pbs_pals_bad_network_prefix(self):
        args = ["python3", "-m", "dragon.launcher.network_config", "--wlm", "pbs+pals", "--network-prefix", "garbage"]

        if environ.get("PBS_NODEFILE"):
            proc = subprocess.run(args=args, capture_output=True, text=True)
            self.assertNotEqual(proc.returncode, 0)
            self.assertTrue("ValueError: No IP addresses found for" in proc.stderr)
            self.assertTrue(" matching regex pattern: garbag" in proc.stderr)


class SSHNetworkConfigTest(unittest.TestCase):

    def setUp(self):
        """Set values specific to slurm"""

        base_abs_path = path.dirname(__file__)
        self.bad_hostfile = path.join(base_abs_path, "bad_hostfile.txt")
        self.good_hostfile = path.join(base_abs_path, "good_hostfile.txt")

        self.wlm = WLM.SSH
        self.config_files = {}

    def test_host_parsing(self):
        """test the config tool parses input arguments correctly"""

        from dragon.launcher.network_config import get_args

        # Test that we error out if specify hostfile and hostlist
        input_args = ["--wlm", "ssh", "--hostfile", self.good_hostfile, "--hostlist", "host1,host2"]
        with self.assertRaises(SystemExit):
            get_args(inputs=input_args)

        # Test we error when set with a different type of wlm
        input_args = ["--wlm", "slurm", "--hostfile", self.good_hostfile]
        with self.assertRaises(NotImplementedError) as e:
            get_args(inputs=input_args)
        self.assertTrue("hostlist and hostfile arguments are only supported in the SSH case" in str(e.exception))

        # test that we throw an error when hostfile is not formatted correctly
        input_args = ["--wlm", "ssh", "--hostfile", self.bad_hostfile]
        with self.assertRaises(ValueError) as e:
            get_args(inputs=input_args)
        self.assertTrue("Hostname is invalid:" in str(e.exception))

        # test that we like a good hostfile
        input_args = ["--wlm", "ssh", "--hostfile", self.good_hostfile]

        wlm, args = get_args(inputs=input_args)
        self.assertEqual(args.hostlist[0], "host-1")
        self.assertEqual(args.hostlist[1], "host-2")

        # test that we handle good hostlists
        input_args = ["--wlm", "ssh", "--hostlist", "host-1,host-2"]
        wlm, args = get_args(inputs=input_args)
        self.assertEqual(args.hostlist[0], "host-1")
        self.assertEqual(args.hostlist[1], "host-2")

        # Test that white space after comma is okay
        input_args = ["--wlm", "ssh", "--hostlist", "host-1,  host-2"]
        wlm, args = get_args(inputs=input_args)
        self.assertEqual(args.hostlist[0], "host-1")
        self.assertEqual(args.hostlist[1], "host-2")

        # Test that a psychotic tab is okay after comma is okay
        input_args = ["--wlm", "ssh", "--hostlist", "host-1,\thost-2"]
        wlm, args = get_args(inputs=input_args)
        self.assertEqual(args.hostlist[0], "host-1")
        self.assertEqual(args.hostlist[1], "host-2")

        # Test that we invalidate too long hostname
        input_args = ["--wlm", "ssh", "--hostlist", "".join(random.choices(string.ascii_letters, k=300))]
        with self.assertRaises(ValueError) as e:
            wlm, args = get_args(inputs=input_args)
        self.assertTrue("Hostname is invalid:" in str(e.exception))

        too_long_part = (
            "".join(random.choices(string.ascii_letters, k=64))
            + "."
            + "".join(random.choices(string.ascii_letters, k=20))
        )
        input_args = ["--wlm", "ssh", "--hostlist", too_long_part]
        with self.assertRaises(ValueError) as e:
            wlm, args = get_args(inputs=input_args)
        self.assertTrue("Hostname is invalid:" in str(e.exception))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    unittest.main()
