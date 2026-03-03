#!/usr/bin/env python3

# This is a target program for a managed process test
# of large argument delivery.
# it returns with 0 if everything is good
# and with 1 if not, and creates its own logfile too.

import sys

import dragon.globalservices.api_setup as dapi
import dragon.infrastructure.parameters as dparms
import dragon.dlogging.util as dlog
import logging


def show_desc(desc):
    return f"len {len(desc)}: {desc[:4]} ... {desc[-4:]}"


dlog.setup_logging(basename=f"jatt_id{dparms.this_process.my_puid}".format(), level=logging.DEBUG)
log = logging.getLogger("")
log.info("hello from jumbo arg test")

expected_size = int(sys.argv[1])
log.info(f"expecting {expected_size} bytes")

dapi.connect_to_infrastructure()

if len(dapi._ARG_PAYLOAD) == expected_size:
    log.info("payload arrived ok")
else:
    log.error(f"payload was {len(dapi._ARG_PAYLOAD)} bytes")
    exit(1)

log.info("test finished successfully")
exit(0)
