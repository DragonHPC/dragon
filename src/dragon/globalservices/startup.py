"""Startup processing for Global Services server.

These functions implement the infrastructure startup sequence.
"""

import io
import logging
import sys

import dragon.infrastructure.messages as dmsg
import dragon.infrastructure.parameters as dparm
import dragon.infrastructure.util as dutil
from dragon.infrastructure.queue import InfraQueue
from dragon.dlogging.util import DragonLoggingServices as dls


class StartupError(Exception):
    pass


def single_connect_to_default_channels(gs_input, ls_input, bela_input):
    """Connects to default channels.

    Any argument that is not None is returned un
    Purpose of doing it like this is to make it easy to substitute
    different handles in test.

    :return: Connection objects or overrides from parameters.
    :rtype: tuple, of gs_input, ls_input, bela_input
    """

    # if ls_input or gs_input or bela_input isn't a message send/recv obj, do
    # the work, and if it is an object just echo it because
    # we are in some test bench scenario.

    if gs_input is None:
        gs_input = InfraQueue.attach(dparm.this_process.gs_qd)

    if ls_input is None:
        ls_input = InfraQueue.attach(dparm.this_process.local_ls_qd)

    if bela_input is None:
        bela_input = InfraQueue.attach(dparm.this_process.local_be_qd)

    return gs_input, ls_input, bela_input


def startup_single(the_ctx, gs_input=None, ls_input=None, bela_input=None):
    """Single node Global Services startup.

    This goes through the protocol steps needed for global services
    to come up on a single node.

    The gs_input, ls_input, bela_input parameters are here for test.  Normally
    they are gotten from interacting with the Channels library.

    Todo: add a timeout to this

    :param the_ctx: global services runtime context object
    :param ls_input: testbench override handle to Local Service input
    :param gs_input: testbench override handle to Global Services Server (this process) input
    :param bela_input: testbench override handle to back end/launcher input channel.
    :raises StartupError: Reports errors in starting up.

    :return: local service input channel and gs input channel
    :rtype: tuple: ([local service input channel], gs input channel)
    """

    gait = the_ctx.tag_inc  # 'get and inc tag'

    log = logging.getLogger("startup_single")

    log.info("single node startup procedure entered")
    inputs = single_connect_to_default_channels(gs_input, ls_input, bela_input)
    gs_input, ls_input, bela_input = inputs
    log.info("connected to channels")

    ls_input.put(dmsg.GSPingLS(tag=gait()))
    log.debug("handshake message sent to Local Service")

    return [ls_input], gs_input, bela_input, 1  # == number of nodes in single mode


def startup_multi(the_ctx, gs_input=None, ls_inputs=None, bela_input=None):
    """Multi-node Global services startup

    :param the_ctx: global services runtime context object
    :param ls_inputs: List of handles to local services input for debug setup
    :param gs_input: handle to Global Services Server (this process) input
    :param bela_input: handle to back end/launcher input channel.

    """
    gait = the_ctx.tag_inc  # 'get and inc tag'

    log = logging.getLogger(dls.GS).getChild("startup.startup_multi")
    log.info("beginning gs startup...")

    # Get the LAChannelsInfo from ls via stdin.
    # detach() transfers the BufferedReader out of TextIOWrapper so only one
    # object owns FD 0 (avoids EBADF double-close in Python 3.13). Replace
    # sys.stdin with a dummy so the shutdown finalizer doesn't complain.
    ls_stdin = sys.stdin.detach()
    sys.stdin = io.StringIO()
    ls_stdin_recv = dutil.NewlineStreamWrapper(ls_stdin, write_intent=False, b64_encode_decode=True)
    la_chs_info = dmsg.parse(ls_stdin_recv.recv())
    log.info("received all channels info, LAChannelsInfo - m9")
    log.debug(f"la_channels.nodes_desc: {la_chs_info.nodes_desc}")
    log.debug(f"la_channels.gs_qd: {la_chs_info.gs_qd}")

    # Attach to the gs channel for recv'ing and the node_idx==0 ls for this test bringup
    gs_ch = la_chs_info.gs_qd
    gs_in_rh = InfraQueue.attach(gs_ch)
    log.info("attached to its recv channel - a17")

    ls_in_whs = []
    n_ls = len(la_chs_info.nodes_desc)
    for val in range(n_ls):
        ls_in_whs.append(InfraQueue.attach(la_chs_info.nodes_desc[str(val)].ls_cd))
    log.info("GS now attached to all ls channels a17")

    # Ping all the local services and wait for responses.
    for ls_in_wh in ls_in_whs:
        ls_in_wh.put(dmsg.GSPingLS(tag=gait()))
    log.info("pinged all LS (GSPingLS) - m10")

    # Attach to the launcher backend on this node.
    be_in_wh = InfraQueue.attach(dparm.this_process.local_be_qd)

    log.info("GS attached to its launcher BE channel - a17")

    return ls_in_whs, gs_in_rh, be_in_wh, n_ls
