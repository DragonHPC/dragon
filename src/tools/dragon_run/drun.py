#! /usr/bin/env python3

import argparse
import socket
import shtab
import sys
import os

from .src import run_wrapper
from .src.common_args import add_common_args
from .src.exceptions import DragonRunMissingAllocation, DragonRunNoSupportedWLM, DragonRunSingleNodeUnsupported

from argparse import RawDescriptionHelpFormatter

import logging

logger = logging.getLogger(__name__)


EXPORT_HELP = (
    "Identify which environment variables from the submission environment are propagated to the launched application."
)
ENV_HELP = "Environment variables to set in the remote environment. Example: --env DEBUG=True"
INCLUDE_FE_HELP = (
    "In addition to running the given command on the dragon backend node, also run the command on the dragon frontend."
)
USER_CMD_HELP = "The executable, including any command line options, to execute on the remote nodes."
SINGLE_MODE_HELP = "Override automatic launcher selection to force use of the single node launcher"
MULTI_MODE_HELP = "Override automatic launcher selection to force use of the multi-node launcher"
FANOUT_HELP = "DragonRun uses a fanout tree to effeciently communicate with its backend nodes. This value sets the number of children each node in this fanout tree talks to."

LOGGING_HELP = """Enables the output of diagnostic log messages. By default, the
DragonRun runtime disables all diagnostic log messaging. Passing one of NOTSET, DEBUG,
INFO, WARNING, ERROR, or CRITICAL to this option, the Dragon runtime will enable
the specified log verbosity."""

EXPORT_ALL = "ALL"
EXPORT_NONE = "NONE"


class kwargs_append_action(argparse.Action):
    def __call__(self, parser, args, values, option_string=None):
        try:
            k, v = values.split("=")
            ret_value = {k: v}
        except ValueError as ex:
            raise argparse.ArgumentError(self, f'Could not parse argument "{values}" as k1=v1 format')
        setattr(args, self.dest, ret_value)

def getLoggingLevels():
    if hasattr(logging, "getLevelNamesMapping"):
        return list(logging.getLevelNamesMapping())
    # getLevelNamesMapping was added in Python 3.11, so for older versions we need to do this manually
    else:
        return list(logging._nameToLevel.keys())


DRUN_DESCRIPTION = """
The DragonRun (drun) utility is used to launch applications on a set of hosts.

The tool automatically detects if a Workload Manager (WLM)—such as Slurm or PBS—was
used for node allocation. If a WLM is present, drun targets those
active nodes. Nodes can also be manually specified via the --hostlist or
--hostfile arguments, or leverage the dhosts utility to set the
DRAGON_RUN_NODEFILE environment variable.

To ensure efficiency across large multi-node environments, drun utilizes
an SSH-tree to launch processes on each node. This requires that all nodes
are configured for password-less SSH and maintain mutual routability.

Example usage:

drun --hostlist host1,host2,host3 my_executable --option1 --option2
    Manually specify a list of hosts, in this case, host1, host2 and host3,
    on which to run my_executable with options --option1 and --option2.

drun --hostfile my_hostfile.txt my_executable
    Specify a file containing a list of hosts, in this case, my_hostfile.txt,
    on which to run my_executable.

drun --wlm slurm my_executable
    Force drun to look for an active Slurm allocation and use the nodes
    from that allocation to run my_executable.
"""

def get_parser():
    parser = argparse.ArgumentParser(
        prog="drun",
        description=DRUN_DESCRIPTION,
        fromfile_prefix_chars="@",
        formatter_class=RawDescriptionHelpFormatter,
    )

    add_common_args(parser)

    parser.add_argument(
        "--export",
        default=EXPORT_NONE,
        choices=[EXPORT_ALL, EXPORT_NONE],
        help=EXPORT_HELP,
    )
    parser.add_argument(
        "--env",
        dest="env",
        required=False,
        default=dict(),
        action=kwargs_append_action,
        metavar="KEY=VALUE",
        help=ENV_HELP,
    )
    parser.add_argument(
        "--include-fe",
        required=False,
        default=False,
        action="store_true",
        help=INCLUDE_FE_HELP,
    )
    parser.add_argument(
        "--fanout",
        required=False,
        default=16,
        type=int,
        help=FANOUT_HELP,
    )
    parser.add_argument(
        "-l",
        "--log-level",
        choices=getLoggingLevels(),
        metavar="LOG_LEVEL",
        help=LOGGING_HELP,
        default=logging.getLevelName(logging.NOTSET),
    )

    parser.add_argument(
        "user_cmd",
        type=str,
        metavar="USER_CMD",
        nargs=argparse.REMAINDER,
        default=[],
        help=USER_CMD_HELP,
    ).complete = shtab.FILE

    return parser


def get_args(args_input=None):
    parser = get_parser()
    args = parser.parse_args(args_input)
    return {key: value for key, value in vars(args).items() if value is not None}


def main():
    args = get_args(sys.argv[1:])

    log_level = args.get("log_level")
    if not isinstance(log_level, int):
        numeric_log_level = getattr(logging, str(log_level), None)
        if not isinstance(numeric_log_level, int):
            raise ValueError(f"Invalid log level: {log_level}")
        log_level = numeric_log_level

    if log_level != logging.NOTSET:
        logging.basicConfig(
            filename=f"drun_{socket.gethostname()}.log",
            encoding="utf-8",
            level=log_level,
            format="%(relativeCreated)6d %(threadName)s %(thread)d %(levelname)s:%(name)s:%(message)s",
        )

    if not args["user_cmd"]:
        print("You must specify a command to run")
        return 1

    env = {}
    if args["export"] == EXPORT_ALL:
        env = os.environ.copy()
    if args["env"]:
        env.update(args["env"])

    hosts = args.get("host_list", [])

    user_command = args["user_cmd"]
    logger.debug("++main(user_command=%s)", user_command)
    try:
        run_wrapper(
            user_command=user_command,
            env=env,
            host_list=hosts,
            force_wlm=args.get("force_wlm"),  # type: ignore
            force_single_node=args["force_single_node"],
            force_multi_node=args["force_multi_node"],
            exec_on_fe=args["include_fe"],
            fanout=args["fanout"],
            ssh_config_path=args.get("ssh_config_path"),
            private_key=args.get("private_key"),
            passphrase=args.get("passphrase"),
            log_level=args.get("log_level"),  # type: ignore
        )
    except (DragonRunMissingAllocation, DragonRunNoSupportedWLM, DragonRunSingleNodeUnsupported) as exc:
        print(exc, flush=True)

    logger.debug("--main")


if __name__ == "__main__":
    sys.exit(main())
