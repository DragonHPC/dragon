#! /usr/bin/env python3

import argparse
import socket
import sys
import os

from subprocess import check_output
from .src import run_wrapper
from .src.common_args import add_common_args
from .src.exceptions import DragonRunMissingAllocation, DragonRunNoSupportedWLM, DragonRunSingleNodeUnsupported

import logging

logger = logging.getLogger(__name__)
logging.basicConfig(
    filename=f"drun_{socket.gethostname()}.log",
    encoding="utf-8",
    level=logging.DEBUG,
    format="%(relativeCreated)6d %(threadName)s %(thread)d %(levelname)s:%(name)s:%(message)s",
)


EXPORT_HELP = "Identify which environment variables from the submission environment are propagated to the launched application."
ENV_HELP = "Environment variables to set in the remote environment. Example: --env DEBUG=True"
INCLUDE_FE_HELP = "In addition to running the given command on the dragon backend node, also run the command on the dragon frontend."
USER_CMD_HELP = "The executable, including any command line options, to execute on the remote nodes."
SINGLE_MODE_HELP = "Override automatic launcher selection to force use of the single node launcher"
MULTI_MODE_HELP = "Override automatic launcher selection to force use of the multi-node launcher"
FANOUT_HELP = "DragonRun uses a fanout tree to effeciently communicate with its backend nodes. This value sets the number of children each node in this fanout tree talks to."

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


def get_parser():
    parser = argparse.ArgumentParser(
        prog="drun",
        description="Dragon Run Launcher Arguments and Options",
        fromfile_prefix_chars="@",
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
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-s", "--single-node-override", action="store_true", help=SINGLE_MODE_HELP)
    group.add_argument("-m", "--multi-node-override", action="store_true", help=MULTI_MODE_HELP)

    # TODO Add command line options for log level

    parser.add_argument(
        "user_cmd",
        type=str,
        metavar="USER_CMD",
        nargs=argparse.REMAINDER,
        default=[],
        help=USER_CMD_HELP,
    )

    return parser


def get_args(args_input=None):
    parser = get_parser()
    args = parser.parse_args(args_input)
    return {key: value for key, value in vars(args).items() if value is not None}


def main():
    args = get_args(sys.argv[1:])

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
            force_wlm=args.get("wlm"),  # type: ignore
            force_single_node=args["single_node_override"],
            force_multi_node=args["multi_node_override"],
            exec_on_fe=args["include_fe"],
            fanout=args["fanout"]
        )
    except (DragonRunMissingAllocation, DragonRunNoSupportedWLM, DragonRunSingleNodeUnsupported) as exc:
        print(exc, flush=True)

    logger.debug("--main")


if __name__ == "__main__":
    sys.exit(main())
