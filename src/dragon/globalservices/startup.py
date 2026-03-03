"""Startup processing for Global Services server.

These functions implement the infrastructure startup sequence.
"""

import logging
import os
import sys

import dragon.channels as dch

import dragon.infrastructure.messages as dmsg
import dragon.infrastructure.connection as dconn
import dragon.infrastructure.parameters as dparm
import dragon.infrastructure.util as dutil
from dragon.dlogging.util import DragonLoggingServices as dls
from dragon.utils import B64


class StartupError(Exception):
    pass


def single_connect_to_default_channels(gs_input, shep_input, bela_input):
    """Connects to default channels.

    Any argument that is not None is returned un
    Purpose of doing it like this is to make it easy to substitute
    different handles in test.

    :return: Connection objects or overrides from parameters.
    :rtype: tuple, of gs_input, shep_input, bela_input
    """

    # if shep_input or gs_input or bela_input isn't a message send/recv obj, do
    # the work, and if it is an object just echo it because
    # we are in some test bench scenario.

    if gs_input is None:
        gs_input_cd = B64.str_to_bytes(dparm.this_process.gs_cd)
        gs_input_channel = dch.Channel.attach(gs_input_cd)
        gs_input = dconn.Connection(inbound_initializer=gs_input_channel, policy=dparm.POLICY_INFRASTRUCTURE)

    if shep_input is None:
        shep_input_cd = B64.str_to_bytes(dparm.this_process.local_shep_cd)
        shep_input_channel = dch.Channel.attach(shep_input_cd)
        shep_input = dconn.Connection(outbound_initializer=shep_input_channel, policy=dparm.POLICY_INFRASTRUCTURE)

    if bela_input is None:
        bela_input_cd = B64.str_to_bytes(dparm.this_process.local_be_cd)
        bela_input_channel = dch.Channel.attach(bela_input_cd)
        bela_input = dconn.Connection(outbound_initializer=bela_input_channel, policy=dparm.POLICY_INFRASTRUCTURE)

    return gs_input, shep_input, bela_input


def startup_single(the_ctx, gs_input=None, shep_input=None, bela_input=None):
    """Single node Global Services startup.

    This goes through the protocol steps needed for global services
    to come up on a single node.

    The gs_input, shep_input, bela_input parameters are here for test.  Normally
    they are gotten from interacting with the Channels library.

    Todo: add a timeout to this

    :param the_ctx: global services runtime context object
    :param shep_input: testbench override handle to Shepherd input
    :param gs_input: testbench override handle to Global Services Server (this process) input
    :param bela_input: testbench override handle to back end/launcher input channel.
    :raises StartupError: Reports errors in starting up.

    :return: shepherd input channel and gs input channel
    :rtype: tuple: ([shepherd input channel], gs input channel)
    """

    gait = the_ctx.tag_inc  # 'get and inc tag'

    log = logging.getLogger("startup_single")

    log.info("single node startup procedure entered")
    inputs = single_connect_to_default_channels(gs_input, shep_input, bela_input)
    gs_input, shep_input, bela_input = inputs
    log.info("connected to channels")

    shep_input.send(dmsg.GSPingSH(tag=gait()).serialize())
    log.debug("handshake message sent to Shepherd")

    return [shep_input], gs_input, bela_input, 1  # == number of nodes in single mode


def startup_multi(the_ctx, gs_input=None, shep_inputs=None, bela_input=None):
    """Multi-node Global services startup

    :param the_ctx: global services runtime context object
    :param shep_inputs: List of handles to Shepherd input for debug setup
    :param gs_input: handle to Global Services Server (this process) input
    :param bela_input: handle to back end/launcher input channel.

    """
    gait = the_ctx.tag_inc  # 'get and inc tag'

    log = logging.getLogger(dls.GS).getChild("startup.startup_multi")
    log.info("beginning gs startup...")

    # Get the LAChannelsInfo from ls via stdin
    ls_stdin = os.fdopen(sys.stdin.fileno(), "rb")
    ls_stdin_recv = dutil.NewlineStreamWrapper(ls_stdin, write_intent=False)
    la_chs_info = dmsg.parse(ls_stdin_recv.recv())
    log.info("received all channels info, LAChannelsInfo - m9")
    log.debug(f"la_channels.nodes_desc: {la_chs_info.nodes_desc}")
    log.debug(f"la_channels.gs_cd: {la_chs_info.gs_cd}")

    # Attach to the gs channel for recv'ing and the node_idx==0 ls for this test bringup
    gs_ch = la_chs_info.gs_cd
    gs_in_ch = dch.Channel.attach(B64.str_to_bytes(gs_ch))
    gs_in_rh = dconn.Connection(inbound_initializer=gs_in_ch, policy=dparm.POLICY_INFRASTRUCTURE)
    log.info("attached to its recv channel - a17")

    ls_in_whs = []
    n_ls = len(la_chs_info.nodes_desc)
    for val in range(n_ls):
        ls_ch = dch.Channel.attach(B64.str_to_bytes(la_chs_info.nodes_desc[str(val)].shep_cd))
        ls_in_whs.append(dconn.Connection(outbound_initializer=ls_ch, policy=dparm.POLICY_INFRASTRUCTURE))
    log.info("GS now attached to all ls channels a17")

    # Ping all the shepherds and wait for responses.
    for ls_in_wh in ls_in_whs:
        ls_in_wh.send(dmsg.GSPingSH(tag=gait()).serialize())
    log.info("pinged all LS (GSPingSH) - m10")

    # Attach to the launcher backend on this node.
    be_ch = dparm.this_process.local_be_cd
    be_in_ch = dch.Channel.attach(B64.from_str(be_ch).decode())
    be_in_wh = dconn.Connection(outbound_initializer=be_in_ch, policy=dparm.POLICY_INFRASTRUCTURE)
    log.info("GS attached to its launcher BE channel - a17")

    return ls_in_whs, gs_in_rh, be_in_wh, n_ls
