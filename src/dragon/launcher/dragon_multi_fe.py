#!/usr/bin/env python3

"""Simple multi-node dragon infrastructure startup"""
import os
import logging

from .frontend import LauncherFrontEnd, LAUNCHER_FAIL_EXIT
from .launchargs import get_args as get_cli_args

from ..utils import set_procname, set_host_id, host_id
from ..dlogging.util import setup_FE_logging, DragonLoggingServices as dls

from ..infrastructure.facts import PROCNAME_LA_FE, FRONTEND_HOSTID

def main():

    args_map = get_cli_args()

    setup_FE_logging(log_device_level_map=args_map['log_device_level_map'],
                     basename='dragon', basedir=os.getcwd())
    log = logging.getLogger(dls.LA_FE).getChild('main')
    log.info(f'start in pid {os.getpid()}, pgid {os.getpgid(0)}')

    # Before doing anything set my host ID
    set_host_id(FRONTEND_HOSTID)
    log.info(f"set host id to {FRONTEND_HOSTID}, and return {host_id()}")

    for key, value in args_map.items():
        if value is not None:
            log.info(f'args_map: {key}: {value}')

    with LauncherFrontEnd(args_map=args_map) as fe_server:
        try:
            fe_server.run_startup()
            fe_server.run_app()
            fe_server.run_msg_server()
        except Exception as err:
            log.exception(f'Error in launcher frontend: {err}')
            return LAUNCHER_FAIL_EXIT

    log.info("exiting frontend")


if __name__ == "__main__":
    set_procname(PROCNAME_LA_FE)
    ecode = main()
    exit(ecode)
