#!/usr/bin/env python3
import os
import sys
import logging
import socket

from .backend import LauncherBackEnd
from .launchargs import NETWORK_HELP

from ..utils import B64
from ..infrastructure.facts import PROCNAME_LA_BE, DEFAULT_TRANSPORT_NETIF, TRANSPORT_TEST_ENV
from ..dlogging.util import setup_BE_logging
from ..dlogging.util import DragonLoggingServices as dls
from . import util as dlutil

LOGBASE = 'launcher_multi_be'


def main(transport_test_env: bool = False):

    from argparse import ArgumentParser
    from distutils.util import strtobool
    from dragon.utils import set_procname
    from dragon import _patch_multiprocessing

    _patch_multiprocessing()

    set_procname(PROCNAME_LA_BE)

    # Set up the logging level. If debug, start logging immediately
    level, fname = setup_BE_logging(dls.LA_BE)
    log = logging.getLogger(dls.LA_BE).getChild('main')
    log.debug("in multi_be")
    log.debug(f"sys.argv = {sys.argv}")

    parser = ArgumentParser(description='Run Dragon backend')

    parser.add_argument('--ip-addr', metavar='FRONTEND_IP', dest='ip_addrs',
                        type=str, help="IP address to connect to frontend")
    parser.add_argument('--host-id', dest='host_ids', type=str, help="Host ID of frontend")
    parser.add_argument('--frontend-sdesc', dest='frontend_sdesc', type=B64.from_str,
                        help="File descriptor for communication to frontend")
    parser.add_argument('--transport-test', action='store_true', help="Run in transport test mode")
    parser.add_argument('--network-prefix', dest='network_prefix', type=str, help=NETWORK_HELP)

    parser.set_defaults(
        transport_test=bool(strtobool(os.environ.get(TRANSPORT_TEST_ENV, str(transport_test_env)))),
        network_prefix=DEFAULT_TRANSPORT_NETIF
    )
    args = parser.parse_args()

    with LauncherBackEnd(args.transport_test, args.network_prefix) as be_server:
        try:
            be_server.run_startup(args.ip_addrs, args.host_ids, args.frontend_sdesc, level, fname)
            be_server.run_msg_server()

        except Exception as err:  # pylint: disable=broad-except
            log.exception(f'la_be {socket.gethostname} main exception: {err}')
            raise


if __name__ == "__main__":
    main()
