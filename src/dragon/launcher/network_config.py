import os
import argparse
import logging
import json
import enum
import zlib
import socket
from typing import Optional
from base64 import b64encode, b64decode

from ..infrastructure.node_desc import NodeDescriptor
from ..infrastructure.facts import DEFAULT_TRANSPORT_NETIF, DEFAULT_OVERLAY_NETWORK_PORT
from ..dlogging import util as dlog


from .launchargs import HOSTFILE_HELP, HOSTLIST_HELP, WLM_HELP
from .launchargs import SplitArgsAtComma, parse_hosts
from .wlm import WLM, wlm_cls_dict
from .wlm import SlurmNetworkConfig, PBSPalsNetworkConfig, SSHNetworkConfig, KubernetesNetworkConfig


@enum.unique
class ConfigOutputType(enum.Enum):
    """Enumerated list of supported network config I/O formats"""

    JSON = "json"
    YAML = "yaml"

    def __str__(self):
        return self.value


class NetworkConfig:
    """Class for constructing network configurations for backend compute"""

    def __init__(self):
        """Initializer for class. Assumes a Slurm workload manager presently"""

        self.network_config = {}
        self.allocation_nnodes = 0

    def get_network_config(self):
        """Get the object's stored network configuration

        Returns:
            dict: network configuration with keys of node indices and values
                  dragon.infrastructure.NodeDescriptor
        """
        return self.network_config

    def output_to_file(
        self,
        basename: str = "network-config",
        basedir: str = None,
        output_type: ConfigOutputType = ConfigOutputType.YAML,
    ):
        """Output network config to formatted file

        Args:
            basename (str, optional): Basename of output file. Defaults to "network-config".
            basedir (str, optional): Directory to output file. Defaults to None and uses
                                     current working directory
            output_type (ConfigOutputType, optional): Format to output to. Defaults to ConfigOutputType.YAML.
        """

        if basedir is None:
            basedir = os.getcwd()

        filename = os.path.join(basedir, ".".join([basename, str(output_type)]))

        if output_type == ConfigOutputType.JSON:
            with open(filename, "w+") as f:
                from json import dumps

                f.write(dumps(self.serialize(), indent=4))

        elif output_type == ConfigOutputType.YAML:
            from yaml import dump

            with open(filename, "w+") as f:
                f.write(dump(self.serialize(), indent=4))

    def serialize(self):
        """Serialize network config

        Returns:
            dict: network config with keys of node indices and values of serialized dicts
                  of dragon.infrastructure.NodeDescriptor
        """
        sdesc = {key: val.get_sdict() for key, val in self.network_config.items()}
        return sdesc

    def compress(self):
        """Serialize and compress the network configuration"""

        sdesc = self.serialize()
        compressed = b64encode(zlib.compress(json.dumps(sdesc).encode("utf-8"))).decode("ascii")
        return compressed

    def decompress(self, compressed_config):
        """Decompress a compressed serialized dictionary network config

        :param compressed_config: Compressed seralized NetworkConfig dictionary. Returned by
                                  NetworkConfig.compress() given an already existing object
        :type compressed_config: str
        """
        sdict = json.loads(zlib.decompress(b64decode(compressed_config)))
        return sdict

    def _initialize_from_sdict(self, serialized_dict):
        """Create a network config from seralized dictionary of one

        Args:
            serialized_dict (dict): network config with keys of node indices
                                    and values of serialized dicts
                                    of dragon.infrastructure.NodeDescriptor
        """
        self.network_config = {}
        for key, val in serialized_dict.items():
            if isinstance(val, dict):
                self.network_config[key] = NodeDescriptor.from_sdict(val)
            elif isinstance(val, NodeDescriptor):
                self.network_config[key] = val
            else:
                raise ValueError(f"Unable to initialize NetworkConfig with {type(val)}")

    def _set_primary_index_to_zero(self, ref_key):
        """Make sure the primary node is indexed to 0"""
        if int(ref_key) != 0:
            old_zero = self.network_config["0"]
            self.network_config["0"] = self.network_config[ref_key]
            self.network_config["0"].is_primary = True
            self.network_config[ref_key] = old_zero
        else:
            self.network_config["0"].is_primary = True

    def _select_primary(self, primary_hostname: Optional[str] = None):
        """Select a backend node to be primary

        Args:
            primary_hostname (str, optional): Hostname of desired node to be primary.
                Defaults to None in which case node with smallest host ID is selected

        Raises:
            RuntimeError: If for any reason unable to select a node for primary host
        """
        if primary_hostname is None:
            # Find the index with minimum host ID
            try:
                primary_key = min(self.network_config, key=lambda k: self.network_config[k].host_id)
                self._set_primary_index_to_zero(primary_key)

                # If key is not 0, make it going forward
            except Exception:
                raise RuntimeError("Unable to select primary host via host ID")
        else:
            # Match given input to those in our network config
            try:
                key = [k for k, v in self.network_config.items() if v.name == primary_hostname]
                if len(key) == 0:
                    raise RuntimeError("Input hostname does not match any available")
                elif len(key) > 1:
                    raise RuntimeError("Invalid network configuration. hostname's are not all unique")
                else:
                    self._set_primary_index_to_zero(key[0])

            except Exception:
                raise

    @classmethod
    def from_wlm(
        cls,
        workload_manager: WLM = WLM.SLURM,
        network_prefix: str = DEFAULT_TRANSPORT_NETIF,
        port: str = DEFAULT_OVERLAY_NETWORK_PORT,
        primary_hostname: Optional[str] = None,
        hostlist: Optional[list[str]] = None,
        sigint_trigger=None,
    ):
        """Obtain a network configuration for a given worklaod manager

        :param workload_manager: Workload manager to be used to obtain info, defaults to WLM.SLURM
        :type workload_manager: WLM, optional
        :param network_prefix: Network prefix to select for backend to frontend communication,
                               defaults to DEFAULT_TRANSPORT_NETIF
        :type network_prefix: str, optional
        :param port: Port to use for listening, defaults to DEFAULT_OVERLAY_NETWORK_PORT
        :type port: str, optional
        :param primary_hostname: Hostname desired to be selected as primary. Defaults
                                 to None, in which case host with smallest host ID is selected.
        :type primary_hostname: str, optional
        :param hostlist: list of hosts to use, defaults to None
        :type hostlist: list[str], optional
        :param sigint_trigger: Conditional location to raise a SIGINT for testing purposes, defaults to None
        :type sigint_trigger: [type], optional

        """

        obj = cls()
        wlm_generator = wlm_cls_dict.get(workload_manager)(network_prefix, port, hostlist)
        if workload_manager is WLM.K8S:
            obj.network_config = wlm_generator._launch_network_config_helper()
        else:
            obj.network_config = wlm_generator.get_network_config(sigint_trigger=sigint_trigger)
        obj.allocation_nnodes = wlm_generator.get_allocation_node_count()
        # If coming from the workload manager, we need to use the host ID to select the primary
        # node (unless overruled by user input) and assign a node index
        obj._select_primary(primary_hostname=primary_hostname)

        return obj

    @classmethod
    def from_file(
        cls, filename: str, format: ConfigOutputType = ConfigOutputType.YAML, primary_hostname: Optional[str] = None
    ):
        """Read input from a YAML or JSON network configuration file

        Args:
            filename (str): name of file containing configuration
            format (ConfigOutputType, optional): YAML or JSON structure of file. Defaults to ConfigOutputType.YAML.
            primary_hostname (str, optional): Node to be selected as primary if configuration
                doesn't have one already selected. Defaults to None.

        Raises:
            RuntimeError: if primary host is requested but already set in file or if
                file is not parsible

        Returns:
            NetworkConfig: object with complete network configuration
        """

        obj = cls()
        try:
            # Don't assume we've been told the correct format
            clean_read = False

            if not clean_read:
                with open(filename, "r") as f:
                    try:
                        from json import load

                        sdict = load(f)
                        clean_read = True
                    except Exception:
                        pass

            if not clean_read:
                with open(filename, "r") as f:
                    try:
                        from yaml import safe_load

                        sdict = safe_load(f)
                        clean_read = True
                    except Exception:
                        pass

            if not clean_read:
                raise RuntimeError("Unable to parse config file in JSON or YAML format")

            for key, value in sdict.items():
                obj.network_config[str(key)] = NodeDescriptor.from_sdict(value)

            # Confirm a primary host is selected. If not, set.
            has_primary = any(p.is_primary is True for p in obj.network_config.values())
            if not has_primary:
                obj._select_primary(primary_hostname=primary_hostname)
            elif has_primary and primary_hostname is not None:
                raise RuntimeError("Primary hostname input by user but is already set in network config file")

        except Exception:
            raise

        return obj

    @classmethod
    def from_sdict(cls, serialized_dict):
        """Initialize configuration from serialized dictionary

        Args:
            serialized_dict (dict): network config with keys of node indices
                                    and values of serialized dicts
                                    of dragon.infrastructure.NodeDescriptor

        Returns:
            NetworkConfig: object with complete network configuration
        """
        obj = cls()
        obj._initialize_from_sdict(serialized_dict)

        return obj


def deliver_backend_node_descriptor(network_prefix=DEFAULT_TRANSPORT_NETIF, port=DEFAULT_OVERLAY_NETWORK_PORT):
    """Print to stdout the node descriptor of node being executed on

    Args:
        network_prefix (str, optional): Network prefix to use for IP address selection.
            Defaults to dragon.infrastructure.facts.DEFAULT_TRANSPORT_NETIF.
        port (int, optional): Listening port. Defaults to DEFAULT_OVERLAY_NETWORK_PORT.
    """

    from dragon.launcher.launchargs import valid_port_int

    parser = argparse.ArgumentParser(description="Starts the Dragon launch helper for generating network topology")

    parser.add_argument("--network-prefix", required=True, type=str, help="Network prefix to look for.")
    parser.add_argument("--port", required=True, type=valid_port_int, help="Port to connect to.")

    parser.set_defaults(
        port=DEFAULT_OVERLAY_NETWORK_PORT,
        network_prefix=DEFAULT_TRANSPORT_NETIF,
    )

    args = parser.parse_args()

    # Generate my node descriptor
    try:
        node_info = NodeDescriptor.get_local_node_network_conf(args.network_prefix, args.port)
    except RuntimeError:
        raise RuntimeError("Unable to acquire backend network configuration")

    # Dump to stdout
    print(json.dumps(node_info.get_sdict()), flush=True)


def get_args(inputs=None):
    """Parse command line inputs for use in network config generator

    Returns:
        Namespace: namespace of expressing input arguments
    """
    from dragon.launcher.launchargs import NETWORK_HELP, valid_port_int

    parser = argparse.ArgumentParser(description="Runs Dragon internal tool for generating network topology")

    parser.add_argument(
        "-p", "--port", type=valid_port_int, help="Infrastructure listening port (default: %(default)s)"
    )
    parser.add_argument("--network-prefix", metavar="NETWORK_PREFIX", type=str, help=NETWORK_HELP)
    parser.add_argument("--wlm", "-w", metavar="WORKLOAD_MANAGER", type=WLM.from_str, choices=list(WLM), help=WLM_HELP)
    parser.add_argument("--log", "-l", help="Enable debug logging", action="store_true")
    parser.add_argument("--output-to-yaml", "-y", action="store_true", help="Output configuration to YAML file")
    parser.add_argument("--output-to-json", "-j", action="store_true", help="Output configuration to JSON file")
    parser.add_argument("--no-stdout", action="store_true", help="Do not print the configuration to stdout")
    parser.add_argument("--primary", type=str, help="Specify the hostname to be used for the primary compute node")

    host_group = parser.add_mutually_exclusive_group()
    host_group.add_argument("--hostlist", action=SplitArgsAtComma, metavar="HOSTLIST", type=str, help=HOSTLIST_HELP)
    host_group.add_argument("--hostfile", type=str, metavar="HOSTFILE", help=HOSTFILE_HELP)

    parser.set_defaults(
        port=DEFAULT_OVERLAY_NETWORK_PORT,
        network_prefix=DEFAULT_TRANSPORT_NETIF,
        log=False,
        output_to_yaml=False,
        output_to_json=False,
        no_stdout=False,
        primary=None,
        hostlist=None,
        hostfile=None,
    )

    if inputs is None:
        args = parser.parse_args()
    else:
        args = parser.parse_args(inputs)

    if args.log:
        dlog.setup_logging(basename="wlm_config", level=logging.DEBUG)
        LOGGER = logging.getLogger("wlm_config")
        LOGGER.debug(f"Command line arguments: {args}")

    wlm = vars(args).get("wlm", None)
    if not wlm:
        for wlm, cls in wlm_cls_dict.items():
            if cls.check_for_wlm_support():
                print(f"Detected a {str(wlm)} environment.")
                break

        if not wlm:
            raise RuntimeError("Could not detect a supported WLM environment.")

    # Handle the ssh case
    if wlm is WLM.SSH:
        args.hostlist = parse_hosts(args.hostlist, args.hostfile)
    else:
        if args.hostlist is not None or args.hostfile is not None:
            raise NotImplementedError("hostlist and hostfile arguments are only supported in the SSH case")

    return wlm, args


def main():
    """
    .. code-block::
        :caption: **Dragon Network Config (dragon-network-config) tool's help and basic use**

        usage: dragon-network-config [-h] [-p PORT] [--network-prefix NETWORK_PREFIX] [--wlm WORKLOAD_MANAGER] [--log] [--output-to-yaml] [--output-to-json]
                                     [--no-stdout] [--primary PRIMARY] [--hostlist HOSTLIST | --hostfile HOSTFILE]

        Runs Dragon internal tool for generating network topology

        optional arguments:
          -h, --help            show this help message and exit
          -p PORT, --port PORT  Infrastructure listening port (default: 6565)
          --network-prefix NETWORK_PREFIX
                                NETWORK_PREFIX specifies the network prefix the dragon runtime will use to determine which IP addresses it should use to build
                                multinode connections from. By default the regular expression r'^(hsn|ipogif|ib)\\d+$' is used -- the prefix for known HPE-Cray XC
                                and EX high speed networks. If uncertain which networks are available, the following will return them in pretty formatting: `dragon-
                                network-ifaddrs --ip --no-loopback --up --running | jq`. Prepending with `srun` may be necessary to get networks available on
                                backend compute nodes
          --wlm WORKLOAD_MANAGER, -w WORKLOAD_MANAGER
                                Specify what workload manager is used. Currently supported WLMs are: slurm, pbs+pals, ssh
          --log, -l             Enable debug logging
          --output-to-yaml, -y  Output configuration to YAML file
          --output-to-json, -j  Output configuration to JSON file
          --no-stdout           Do not print the configuration to stdout
          --primary PRIMARY     Specify the hostname to be used for the primary compute node
          --hostlist HOSTLIST   Specify backend hostnames as a comma-separated list, eg: `--hostlist host_1,host_2,host_3`. `--hostfile` or `--hostlist` is a
                                required argument for WLM SSH and is only used for SSH
          --hostfile HOSTFILE   Specify a list of hostnames to connect to via SSH launch. The file should be a newline character separated list of hostnames.
                                `--hostfile` or `--hostlist` is a required argument for WLM SSH and is only used for SSH

        # To create YAML and JSON files with a slurm WLM:
        $ dragon-network-config --wlm slurm --output-to-yaml --output-to-json
    """

    try:
        wlm, args = get_args()

        net = NetworkConfig.from_wlm(
            workload_manager=wlm,
            port=args.port,
            network_prefix=args.network_prefix,
            primary_hostname=args.primary,
            hostlist=args.hostlist,
        )
        if not args.no_stdout:
            config = net.get_network_config()
            local_sdict = {}
            for key, item in config.items():
                local_sdict[key] = item.get_sdict()
            print(json.dumps(local_sdict, indent=4))

        if args.output_to_yaml:
            net.output_to_file(basename=str(wlm), basedir=os.getcwd(), output_type=ConfigOutputType.YAML)

        if args.output_to_json:
            net.output_to_file(basename=str(wlm), basedir=os.getcwd(), output_type=ConfigOutputType.JSON)

    except RuntimeError:
        raise


if __name__ == "__main__":
    main()
