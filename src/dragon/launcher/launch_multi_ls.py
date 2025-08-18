#!/usr/bin/env python3
import os
import logging
import socket

from ..localservices.local_svc import multinode
from ..infrastructure.facts import GS_LOG_BASE, TRANSPORT_TEST_ENV, PROCNAME_GS
from ..dlogging.util import DragonLoggingServices, setup_BE_logging
from ..cli import console_script_args


def main():

    _, fname = setup_BE_logging(service=DragonLoggingServices.LS)
    log = logging.getLogger(DragonLoggingServices.LS).getChild("launch_multi_ls")
    log.info("--------------------ls started-----------------------")
    log.debug(f"starting debug mode on pid={os.getpid()}")

    for key, val in os.environ.items():
        log.debug(f"{key}: {val}")

    env_update = {GS_LOG_BASE: f"gs-{socket.gethostname()}"}

    # Discover whether the transport service test mode is being requested
    # or not.
    transport_test_flag = os.environ.get(TRANSPORT_TEST_ENV) is not None

    gs_args = console_script_args(PROCNAME_GS)

    try:
        _ = multinode(gs_args=gs_args, gs_env=env_update, fname=fname, transport_test_env=transport_test_flag)

    except Exception as ex:
        log.exception("Final launch exception catch")
        raise ex


if __name__ == "__main__":
    main()
