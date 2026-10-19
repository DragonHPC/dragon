import argparse
from .wlm import WLM
from typing import cast


WLM_HELP = (
    f"Specify what workload manager is used. Currently supported WLMs are: {', '.join([wlm.value for wlm in WLM])}"
)

HOSTFILE_HELP = """
    Specify a list of hostnames to connect to via SSH launch. The file
    should be a newline character separated list of hostnames.
    `--hostfile` or `--hostlist` is a required argument for WLM SSH
    and is only used for SSH
    """

HOSTLIST_HELP = """
    Specify backend hostnames as a comma-separated list, eg:
    `--hostlist host_1,host_2,host_3`.
    `--hostfile` or `--hostlist` is a required argument for WLM SSH
    and is only used for SSH
    """

SINGLE_MODE_HELP = "Override automatic launcher selection to force use of the single node launcher"
MULTI_MODE_HELP = "Override automatic launcher selection to force use of the multi-node launcher"


class SplitArgsAtComma(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        hostlist = cast(str, values)
        setattr(namespace, self.dest, [v.strip() for v in hostlist.split(",")])


class LoadHostFile(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        try:
            hostfilename = cast(str, values)
            with open(hostfilename) as hostfile:
                setattr(namespace, self.dest, hostfile.read().splitlines())
        except FileNotFoundError:
            raise argparse.ArgumentError(self, f"Could not find hostfile {values}")


def add_common_args(parser, add_mode_group=True, add_ssh_args=True):
    parser.add_argument(
        "--wlm",
        metavar="WORKLOAD_MANAGER",
        type=WLM.from_str,
        choices=list(WLM),
        help=WLM_HELP,
        dest="force_wlm",
    )

    host_group = parser.add_mutually_exclusive_group()
    host_group.add_argument(
        "--hostlist",
        dest="host_list",
        action=SplitArgsAtComma,
        type=str,
        help=HOSTLIST_HELP,
    )
    host_group.add_argument(
        "--hostfile",
        dest="host_list",
        action=LoadHostFile,
        type=str,
        help=HOSTFILE_HELP,
    )

    if add_mode_group:
        mode_group = parser.add_mutually_exclusive_group()
        mode_group.add_argument(
            "-s",
            "--single-node-override",
            action="store_true",
            dest="force_single_node",
            help=SINGLE_MODE_HELP,
            default=False,
        )
        mode_group.add_argument(
            "-m",
            "--multi-node-override",
            action="store_true",
            dest="force_multi_node",
            help=MULTI_MODE_HELP,
            default=False,
        )

    if add_ssh_args:
        parser.add_argument(
            "--ssh-config-path",
            type=str,
            help="Path to SSH config file. If not provided, the default SSH config path ~/.ssh/config will be used.",
        )

        parser.add_argument(
            "--private-key",
            type=str,
            help="Path to private key file for SSH authentication. If not provided, default SSH keys will be used.",
        )

        parser.add_argument(
            "--passphrase",
            type=str,
            help="Passphrase for the private key file, if it is encrypted. If not provided, it is assumed that the private key is not encrypted or that an SSH agent is being used.",
        )

    parser.set_defaults(
        host_list=[],
        ssh_config_path="~/.ssh/config",
    )