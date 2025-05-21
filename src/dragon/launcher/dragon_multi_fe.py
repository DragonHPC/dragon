#!/usr/bin/env python3

"""Simple multi-node dragon infrastructure startup"""
import os
import logging

from .frontend import LauncherFrontEnd, LAUNCHER_FAIL_EXIT, LAUNCHER_SUCCESS_EXIT
from .launchargs import get_args as get_cli_args

from ..utils import set_procname, set_host_id, host_id
from ..dlogging.util import setup_FE_logging, DragonLoggingServices as dls

from ..infrastructure.facts import PROCNAME_LA_FE, FRONTEND_HOSTID
from ..infrastructure.node_desc import NodeDescriptor

from ..telemetry import progress_bar


def main(args_map=None):

    if args_map is None:
        args_map = get_cli_args()

    setup_FE_logging(log_device_level_map=args_map["log_device_level_map"], basename="dragon", basedir=os.getcwd())
    log = logging.getLogger(dls.LA_FE).getChild("main")
    log.info(f"start in pid {os.getpid()}, pgid {os.getpgid(0)}")

    # Before doing anything set my host ID
    set_host_id(FRONTEND_HOSTID)
    log.info(f"set host id to {FRONTEND_HOSTID}, and return {host_id()}")

    for key, value in args_map.items():
        if value is not None:
            log.info(f"args_map: {key}: {value}")

    execution_complete = False
    net_conf = None
    telemetry_level = args_map["telemetry_level"]
    restart = False
    last_avail_nodes = None
    use_progress_bar = args_map["progress_bar"]

    while not execution_complete:
        # Try to run the launcher
        try:
            with LauncherFrontEnd(args_map=args_map) as fe_server:
                if use_progress_bar:
                    handle = progress_bar.start(fe_server)
                net_conf = fe_server.run_startup(net_conf=net_conf)
                net_conf = fe_server.run_app(restart=restart)
                if telemetry_level > 0:
                    net_conf = fe_server.run_telem(telemetry_level)
                net_conf = fe_server.run_msg_server()

        # Handle an obvious exception as well as what to do if we're trying a resilient runtime
        except Exception as err:
            log.exception(f"Error in launcher frontend: {err}")
            if not fe_server.resilient:
                return LAUNCHER_FAIL_EXIT

            # pass testing mode along to the backend if set
            if os.getenv("_DRAGON_HSTA_TESTING_MODE", None):
                os.environ["_DRAGON_HSTA_TESTING_MODE"] = str(int(os.environ["_DRAGON_HSTA_TESTING_MODE"]) + 1)

            # Check if the sum of active and idle nodes is > 0:
            if last_avail_nodes is None:
                last_avail_nodes_list = [idx for idx, node in net_conf.items()]
                last_avail_nodes = len(last_avail_nodes_list)
            avail_nodes_list = [
                idx
                for idx, node in net_conf.items()
                if node.state in [NodeDescriptor.State.ACTIVE, NodeDescriptor.State.IDLE] and idx != "f"
            ]
            avail_nodes = len(avail_nodes_list)

            # Make sure we didn't exit for reasons other than a downed node
            if last_avail_nodes == avail_nodes:
                print("Dragon runtime is in an unrecoverable state. Exiting.", flush=True)
                return LAUNCHER_FAIL_EXIT

            last_avail_nodes = avail_nodes
            log.info(f"avail nodes found to be {avail_nodes}")

            # Proceed
            log.debug(f"{args_map['exhaust_resources']=}, {args_map['node_count']=}")
            if args_map["exhaust_resources"]:
                if avail_nodes == 0:
                    print(
                        "There are no more hardware resources available for continued app execution.",
                        flush=True,
                    )
                    return LAUNCHER_FAIL_EXIT
            elif avail_nodes < args_map["node_count"]:
                print(
                    "There are not enough hardware resources available for continued app execution.",
                    flush=True,
                )
                return LAUNCHER_FAIL_EXIT

            # Make sure the user app has some semblance of understanding this is a resiliency restart
            restart = True

        # If everything exited wtihout exception, break out of the loop and exit
        else:
            execution_complete = True

    if use_progress_bar:
        progress_bar.stop(handle)

    log.info("exiting frontend")
    return LAUNCHER_SUCCESS_EXIT


if __name__ == "__main__":
    set_procname(PROCNAME_LA_FE)
    ecode = main()
    exit(ecode)
