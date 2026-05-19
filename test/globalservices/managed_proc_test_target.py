#!/usr/bin/env python3

# This is a target program for a managed process test.
# It attempts to verify that certain channels are
# constructed and that their descriptors
# appear in the environment in the expected way.

# it returns with 0 if everything is good
# and with 1 if not, and creates its own logfile too.

# by default this checks for the GS channel descriptor
# and whether one can attach to it
# options. pick at most one
# --gs-ret  : check for the gs return channel too
# --args : connect to infrastructure and receive ping message thru api
# If --args is chosen, can add 3 more integer arguments to check
# against the bytes like objects in which immediate args are
# deposited.

import binascii
import logging
import sys

import dragon.channels as dch
import dragon.managed_memory as dmm
import dragon.infrastructure.parameters as dparms
import dragon.dlogging.util as dlog
import dragon.utils as du

import pickle


def show_desc(desc):
    return "len {}: {} ... {}".format(len(desc), desc[:4], desc[-4:])


dlog.setup_logging(basename="mptt_id{}".format(dparms.this_process.my_puid), level=logging.DEBUG)
log = logging.getLogger("")
log.info("hello from test util")

do_args = len(sys.argv) > 1 and sys.argv[1] == "--args"
check_gs_ret = do_args or (len(sys.argv) > 1 and sys.argv[1] == "--gs-ret")

if check_gs_ret:
    log.info("checking gs return channel")

if do_args:
    log.info("checking full infrastructure conn and arg delivery")

gs_desc = None
gs_return_desc = None

log.info(f"assigned uid is {dparms.this_process.my_puid}")

if 0 == len(dparms.this_process.inf_pd):
    log.info("inf pd absent")
    exit(1)

try:
    infpool_desc = du.B64.str_to_bytes(dparms.this_process.inf_pd)
except binascii.Error as err:
    log.info("failed to decode inf pool desc")
    log.exception(f"error is {err}", exc_info=True)
    exit(1)

try:
    infpool = dmm.MemoryPool.attach(infpool_desc)
except Exception as err:
    log.info("Failed to attach to inf pool")
    log.exception(f"error is {err}", exc_info=True)
    exit(1)

if 0 == len(dparms.this_process.default_pd):
    log.info("default pd absent")
    exit(1)

try:
    defp_desc = du.B64.str_to_bytes(dparms.this_process.default_pd)
except binascii.Error as err:
    log.info("failed to decode default pool desc")
    log.exception(f"error is {err}", exc_info=True)
    exit(1)

try:
    defpool = dmm.MemoryPool.attach(defp_desc)
except Exception as err:
    log.info("Failed to attach to default pool")
    log.exception(f"error is {err}", exc_info=True)
    exit(1)


if 0 == len(dparms.this_process.gs_cd):
    log.info("GS cd absent")
    exit(1)

log.info("gs desc is: {}".format(show_desc(dparms.this_process.gs_cd)))

try:
    gs_desc = du.B64.str_to_bytes(dparms.this_process.gs_cd)
except binascii.Error as err:
    log.info("Failed to decode GS cd")
    log.exception(f"error is {err}", exc_info=True)
    exit(1)

try:
    gs_chan = dch.Channel.attach(gs_desc)
    log.info("attached to gs channel")
except Exception as err:
    log.info("Failed to attach to gs channel")
    log.exception(f"error is {err}", exc_info=True)
    exit(1)

if check_gs_ret:
    log.info("checking gs ret channel")

    if 0 == len(dparms.this_process.gs_ret_cd):
        log.info("GS ret cd absent")
        exit(1)

    log.info("gs return desc is: {}".format(show_desc(dparms.this_process.gs_ret_cd)))

    try:
        gs_return_desc = du.B64.str_to_bytes(dparms.this_process.gs_ret_cd)
    except binascii.Error as err:
        log.info("Failed to decode GS return cd")
        log.exception(f"error is {err}", exc_info=True)
        exit(1)

    try:
        gs_ret_chan = dch.Channel.attach(gs_return_desc)
        log.info("attached to gs return channel")
    except Exception as err:
        log.info("Failed to attach to gs return channel")
        log.exception(f"error is {err}", exc_info=True)
        exit(1)

if do_args:
    log.info("trying to attach to infrastructure and args")
    import dragon.globalservices.api_setup as dapi

    try:
        dapi.connect_to_infrastructure()
    except Exception as err:
        log.info("failed to attach to infrastructure")
        log.exception(f"error is {err}", exc_info=True)
        exit(1)

    if 5 == len(sys.argv):
        log.info("checking argument delivery")
        ex_item1 = int(sys.argv[2])
        ex_item2 = int(sys.argv[3])
        ex_item3 = int(sys.argv[4])

        if dapi._ARG_PAYLOAD is None:
            log.info("arg payload not updated")

        item1, item2, item3 = pickle.loads(dapi._ARG_PAYLOAD)

        if item1 != ex_item1:
            log.info("item1 expected: {} got: {}".format(ex_item1, item1))
            exit(1)

        if item2 != ex_item2:
            log.info("args expected: {} got: {}".format(ex_item2, item2))
            exit(1)

        if item3 != ex_item3:
            log.info("kwargs expected: {} got: {}".format(ex_item3, item3))
            exit(1)

log.info("test finished successfully")
exit(0)
