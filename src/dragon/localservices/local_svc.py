"""Dragon local services"""

import json
import logging
import os
import subprocess
import sys

from typing import Optional

import dragon.channels as dch
import dragon.managed_memory as dmm
import dragon.utils as dutils

from ..infrastructure.node_desc import NodeDescriptor
from ..transport import start_transport_agent

from ..infrastructure import connection as dconn
from ..infrastructure import facts as dfacts
from ..infrastructure import messages as dmsg
from ..infrastructure import parameters as dparms
from ..infrastructure import util as dutil
from ..dlogging.util import setup_BE_logging, DragonLoggingServices as dls
from ..utils import B64, set_procname

from .server import LocalServer, PopenProps, get_new_tag, ProcessProps, OutputConnector


def maybe_start_gs(
    gs_args: Optional[list], gs_env: Optional[dict], hostname: str, be_in: object, logger_sdesc: B64 = None
):
    """Starts gs if there are any args to start it with.

    Args:
        gs_args (list): args to start global services

        gs_env (dict): environment variable to set prior to gs launcher

        logger_sdesc (B64, optional): logging channel descriptor

    Raises:
        RuntimeError: Launch of global services was not successful.

    Returns:
        gs_proc (PopenProps) of gs launch, or None if gs_args is None
    """
    log = logging.getLogger(dls.LS).getChild("maybe_start_gs")
    if gs_args is not None:
        if gs_env is None:
            the_env = os.environ
        else:
            the_env = dict(os.environ)
            the_env.update(gs_env)

        if logger_sdesc is not None:
            the_env[dfacts.DRAGON_LOGGER_SDESC] = str(logger_sdesc)

        try:
            log.debug("launcher gs: %s" % gs_args)
            stdout_connector = OutputConnector(
                be_in=be_in,
                puid=dfacts.GS_PUID,
                hostname=hostname,
                out_err=dmsg.SHFwdOutput.FDNum.STDOUT.value,
                conn=None,
                root_proc=True,
                critical_proc=True,
            )
            stderr_connector = OutputConnector(
                be_in=be_in,
                puid=dfacts.GS_PUID,
                hostname=hostname,
                out_err=dmsg.SHFwdOutput.FDNum.STDERR.value,
                conn=None,
                root_proc=True,
                critical_proc=True,
            )

            gs_proc = PopenProps(
                ProcessProps(
                    p_uid=dfacts.GS_PUID,
                    critical=True,
                    r_c_uid=None,
                    stdin_req=None,
                    stdout_req=None,
                    stderr_req=None,
                    stdin_connector=None,
                    stdout_connector=stdout_connector,
                    stderr_connector=stderr_connector,
                    layout=None,
                    local_cuids=set(),
                    local_muids=set(),
                    creation_msg_tag=None,
                ),
                gs_args,
                bufsize=0,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=the_env,
            )

            gs_proc.props.stdout_connector.add_proc_info(gs_proc)
            gs_proc.props.stderr_connector.add_proc_info(gs_proc)

            log.info("started gs")
        except (OSError, ValueError) as gs_launch_err:
            log.fatal("gs launch failed")
            raise RuntimeError("gs launch failed") from gs_launch_err
    else:
        gs_proc = None

    return gs_proc


def mk_inf_resources(node_index):
    log = logging.getLogger(dls.LS).getChild("inf resource maker")
    log.info("creating  pools and channels")

    inf_pool = None
    def_pool = None

    _user = os.environ.get("USER", str(os.getuid()))

    try:
        inf_muid = dfacts.infrastructure_pool_muid_from_index(node_index)
        ips = dparms.this_process.inf_seg_sz
        ipn = "%s_%s_" % (_user, os.getpid()) + dfacts.INFRASTRUCTURE_POOL_SUFFIX
        log.info("inf pool: %s size %s" % (ipn, ips))
        inf_pool = dmm.MemoryPool(ips, ipn, inf_muid)

        def_muid = dfacts.default_pool_muid_from_index(node_index)
        dps = dparms.this_process.default_seg_sz
        dpn = "%s_%s_" % (_user, os.getpid()) + dfacts.DEFAULT_POOL_SUFFIX
        log.info("def pool: %s size %s" % (dpn, dps))

        # PJM TODO: how pre-allocated blocks are done here is temporary.  We also need a better way
        # to manage Pool attributes as there are many more we will expose.
        def_pool = dmm.MemoryPool(dps, dpn, def_muid, pre_alloc_blocks=[8, 8, 8, 8, 8, 8, 8, 8, 0, 0, 0, 0, 8])
        start_pools = {inf_muid: inf_pool, def_muid: def_pool}
        dparms.this_process.inf_pd = dutils.B64.bytes_to_str(inf_pool.serialize())
        dparms.this_process.default_pd = dutils.B64.bytes_to_str(def_pool.serialize())

        shep_input_cuid = dfacts.shepherd_cuid_from_index(node_index)
        shep_ch = dch.Channel(inf_pool, shep_input_cuid, None)
        dparms.this_process.local_shep_cd = dutils.B64.bytes_to_str(shep_ch.serialize())
        shep_input = dconn.Connection(inbound_initializer=shep_ch, policy=dparms.POLICY_INFRASTRUCTURE)

        la_input_cuid = dfacts.launcher_cuid_from_index(node_index)
        la_input_ch = dch.Channel(inf_pool, la_input_cuid, None)
        dparms.this_process.local_be_cd = dutils.B64.bytes_to_str(la_input_ch.serialize())
        dparms.this_process.be_cuid = dfacts.launcher_cuid_from_index(node_index)
        la_input = dconn.Connection(outbound_initializer=la_input_ch, policy=dparms.POLICY_INFRASTRUCTURE)

        ta_input_cuid = dfacts.transport_cuid_from_index(node_index)
        ta_input_ch = dch.Channel(inf_pool, ta_input_cuid, None)
        ta_input_descr = ta_input_ch.serialize()
        dparms.this_process.local_ta_cd = dutils.B64.bytes_to_str(ta_input_descr)
        ta_input = dconn.Connection(outbound_initializer=ta_input_ch, policy=dparms.POLICY_INFRASTRUCTURE)

        start_channels = {shep_input_cuid: shep_ch, la_input_cuid: la_input_ch, ta_input_cuid: ta_input_ch}

        if 0 == node_index:
            gs_chan = dch.Channel(inf_pool, dfacts.GS_INPUT_CUID, None)
            start_channels[dfacts.GS_INPUT_CUID] = gs_chan
            dparms.this_process.gs_cd = dutils.B64.bytes_to_str(gs_chan.serialize())
            gs_input = dconn.Connection(outbound_initializer=gs_chan, policy=dparms.POLICY_INFRASTRUCTURE)
        else:
            gs_input = None

    except (dch.ChannelError, dmm.DragonPoolError, dmm.DragonMemoryError) as init_err:
        log.fatal("could not make infrastructure resources")
        # last ditch attempt to clean up pools.
        if inf_pool is not None:
            inf_pool.destroy()
        if def_pool is not None:
            def_pool.destroy()
        raise RuntimeError("infrastructure resource creation failed") from init_err

    log.info("infrastructure resources constructed")
    return start_pools, start_channels, shep_input, la_input, ta_input_descr, ta_input, gs_input


def get_shepherd_msg_queue(stdin=None, stdout=None):

    if stdin is None:
        bin_stdin = os.fdopen(sys.stdin.fileno(), "rb", buffering=0)
        shep_stdin_msg = dutil.NewlineStreamWrapper(bin_stdin, write_intent=False)
    else:
        shep_stdin_msg = stdin

    if stdout is None:
        bin_stdout = os.fdopen(sys.stdout.fileno(), "wb", buffering=0)
        shep_stdout_msg = dutil.NewlineStreamWrapper(bin_stdout, write_intent=True)
    else:
        shep_stdout_msg = stdout

    return shep_stdin_msg, shep_stdout_msg


def single(
    make_infrastructure_resources: bool = True,
    gs_input=None,
    la_input=None,
    shep_input=None,
    gs_args=None,
    gs_env=None,
    ls_stdin=None,
    ls_stdout=None,
    ta_input=None,
):

    log = logging.getLogger("local_svc single node main")
    log.info("starting on pid=%s" % os.getpid())

    start_pools = {}
    start_channels = {}

    try:  # attempt to construct infrastructure objects and establish communications
        os.setpgid(0, 0)

        shep_stdin_msg, shep_stdout_msg = get_shepherd_msg_queue(ls_stdin, ls_stdout)

        msg = dmsg.parse(shep_stdin_msg.recv())
        assert isinstance(msg, dmsg.BENodeIdxSH), "startup msg expected on stdin"
        log.info("got BENodeIdxSH")
        assert msg.node_idx == 0, "single node"
        node_index = msg.node_idx
        net_conf_key = msg.net_conf_key

        if make_infrastructure_resources:
            assert not any((gs_input, la_input, shep_input, ta_input))
            start_pools, start_channels, shep_input, la_input, ta_input_descr, ta_input, gs_input = mk_inf_resources(
                node_index
            )

        dparms.this_process.index = node_index
        os.environ.update(dparms.this_process.env())
        be_ping = dmsg.SHPingBE(
            tag=get_new_tag(),
            shep_cd=dparms.this_process.local_shep_cd,
            be_cd=dparms.this_process.local_be_cd,
            gs_cd=dparms.this_process.gs_cd,
            default_pd=dparms.this_process.default_pd,
            inf_pd=dparms.this_process.inf_pd,
        )
        shep_stdout_msg.send(be_ping.serialize())
        log.info("wrote SHPingBE")

        msg = dmsg.parse(shep_input.recv())
        assert isinstance(msg, dmsg.BEPingSH), "startup expectation on shep input"
        log.info("got BEPingSH")

        ls_node_desc = NodeDescriptor.get_localservices_node_conf(
            host_name="localhost", name="localhost", ip_addrs=["127.0.0.1"], is_primary=True
        )
        ch_up_msg = dmsg.SHChannelsUp(
            tag=get_new_tag(), node_desc=ls_node_desc, gs_cd=dparms.this_process.gs_cd, net_conf_key=net_conf_key
        )

        la_input.send(ch_up_msg.serialize())
        log.info("sent SHChannelsUp")
        gs_proc = maybe_start_gs(gs_args, gs_env, hostname="localhost", be_in=la_input)
        msg = dmsg.parse(shep_input.recv())
        assert isinstance(msg, dmsg.GSPingSH), "startup expectation shep input"
        log.info("got GSPingSH")
    except (OSError, EOFError, json.JSONDecodeError, AssertionError, RuntimeError) as rte:
        log.fatal("startup failed")
        LocalServer.clean_pools(start_pools, log)
        raise RuntimeError("startup fatal error") from rte

    gs_input.send(dmsg.SHPingGS(tag=get_new_tag(), node_sdesc=ls_node_desc.sdesc).serialize())

    server = LocalServer(channels=start_channels, pools=start_pools, hostname="localhost")

    if gs_proc is not None:
        server.add_proc(gs_proc)
        server.gs_proc = gs_proc

    try:
        server.run(shep_in=shep_input, gs_in=gs_input, be_in=la_input, is_primary=True)
    except Exception:
        log.fatal("There was an exception and LS is going to clean up.")
        server.cleanup()
        raise

    try:
        msg = dmsg.parse(shep_stdin_msg.recv())
        assert isinstance(msg, dmsg.BEHalted)
        shep_stdout_msg.send(dmsg.SHHalted(tag=get_new_tag()).serialize())
    except (OSError, AssertionError, EOFError, json.JSONDecodeError) as tde:
        log.fatal("teardown sequence error")
        raise RuntimeError("teardown") from tde
    finally:
        server.cleanup()

    log.info("shutdown complete")


def multinode(
    make_infrastructure_resources: bool = True,
    gs_input=None,
    la_input=None,
    ls_input=None,
    gs_args=None,
    gs_env=None,
    ta_args=None,
    ta_env=None,
    ls_stdin=None,
    ls_stdout=None,
    fname=None,
    ta_input=None,
    transport_test_env: bool = False,
):
    """Local services for multi-node dragon"""

    setup_BE_logging(service=dls.LS, fname=fname)
    log = logging.getLogger(dls.LS).getChild("multinode")
    log.debug("starting debug modes on pid=%s" % os.getpid())

    set_procname(dfacts.PROCNAME_LS)

    start_pools = {}
    start_channels = {}

    try:  # attempt to construct infrastructure objects and establish communications
        os.setpgid(0, 0)

        ls_stdin_queue, ls_stdout_queue = get_shepherd_msg_queue(ls_stdin, ls_stdout)

        msg = dmsg.parse(ls_stdin_queue.recv())
        assert isinstance(msg, dmsg.BENodeIdxSH), "startup msg expected on stdin"
        node_index = msg.node_idx
        net_conf_key = msg.net_conf_key
        hostname = msg.host_name
        ip_addrs = msg.ip_addrs
        is_primary = msg.primary
        logger_sdesc = msg.logger_sdesc
        log.info(
            "got BENodeIdxSH (id=%s, ips=%s, host=%s, primary=%s) - m2.1" % (node_index, ip_addrs, hostname, is_primary)
        )

        # Add the dragon logging handler to our already existing log
        setup_BE_logging(service=dls.LS, logger_sdesc=logger_sdesc, fname=fname)
        log.debug("dragon logging initiated on pid=%s" % os.getpid())

        if make_infrastructure_resources:
            assert not any((gs_input, la_input, ls_input, ta_input))
            start_pools, start_channels, ls_input, la_input, ta_input_descr, ta_input, gs_input = mk_inf_resources(
                node_index
            )

        dparms.this_process.index = node_index
        os.environ.update(dparms.this_process.env())

        if is_primary:
            gs_cd = dparms.this_process.gs_cd
        else:
            gs_cd = None

        be_ping = dmsg.SHPingBE(
            tag=get_new_tag(),
            shep_cd=dparms.this_process.local_shep_cd,
            be_cd=dparms.this_process.local_be_cd,
            gs_cd=gs_cd,
            default_pd=dparms.this_process.default_pd,
            inf_pd=dparms.this_process.inf_pd,
        )
        ls_stdout_queue.send(be_ping.serialize())
        log.info("wrote SHPingBE")

        msg = dmsg.parse(ls_input.recv())
        assert isinstance(msg, dmsg.BEPingSH), "startup expectation on shep input"
        log.info("got BEPingSH")

        # Create a node descriptor for this node I'm running on
        ls_node_desc = NodeDescriptor.get_localservices_node_conf(
            host_name=hostname, name=hostname, ip_addrs=ip_addrs, is_primary=is_primary
        )
        ch_up_msg = dmsg.SHChannelsUp(
            tag=get_new_tag(), node_desc=ls_node_desc, gs_cd=gs_cd, idx=node_index, net_conf_key=net_conf_key
        )

        la_input.send(ch_up_msg.serialize())
        log.info("sent SHChannelsUp")

        # Recv LAChannelsInfo Broadcast
        la_channels_info = dmsg.parse(ls_input.recv())
        assert isinstance(la_channels_info, dmsg.LAChannelsInfo), "expected LAChannelsInfo"
        log.info("node index %s received all channels info" % node_index)
        log.debug("la_channels.nodes_desc: %s" % la_channels_info.nodes_desc)
        log.debug("la_channels.gs_cd: %s" % la_channels_info.gs_cd)

        # Work out transport agent launch arguments:
        ta_args = ["%s" % la_channels_info.transport]

        # Create gateway channels
        gw_channels = []
        num_ls_gw_channels = la_channels_info.num_gw_channels

        # We set the number of gateways in the environment for other processes
        # that will be created by Local Services and that need to call
        # register_gateways_from_env. C level programs that want to use multi-node
        # channels need to call this function.
        dparms.this_process.set_num_gateways_per_node(num_ls_gw_channels)
        node_count = len(la_channels_info.nodes_desc)
        # get the cuid of the first gateway channel on node index
        gw_cuid = dfacts.gw_cuid_from_index(node_index, num_ls_gw_channels)
        def_muid = dfacts.default_pool_muid_from_index(node_index)
        for id in range(num_ls_gw_channels):
            gw_ch = dch.Channel(start_pools[def_muid], gw_cuid + id, capacity=dparms.this_process.gw_capacity)
            encoded_ser_gw = B64(gw_ch.serialize())
            # add the serialized descriptor of the gw channel to the environment
            # for the transport agent to get it
            encoded_ser_gw_str = str(encoded_ser_gw)
            os.environ[dfacts.GW_ENV_PREFIX + str(id + 1)] = encoded_ser_gw_str
            gs_env[dfacts.GW_ENV_PREFIX + str(id + 1)] = encoded_ser_gw_str
            gw_channels.append(gw_ch)
        log.info("ls created %s gateway channels" % num_ls_gw_channels)

        # Here we register the gateway channels
        # created by this process as environment
        # variables. This is to enable the shepherd
        # (local services) once the transport service
        # is started, which happens next. Order is
        # not important here. The gateways can be
        # registered as soon as they are created and part
        # of the environment.
        dch.register_gateways_from_env()
        log.info("ls has registered the gateways in its environment.")

        # Start TA (telling it its node ID by appending to args) and send LAChannelsInfo
        log.info("standing up ta")
        try:
            ta = start_transport_agent(
                node_index,
                B64(ta_input_descr),
                logger_sdesc,
                args=ta_args,
                env=ta_env,
            )
            # Cast the Popen instance returned by start_transport_agent() to
            # PopenProps mainly for consistency, though it doesn't appear to
            # matter since the TA process isn't used elsewhere.
            ta.__class__ = PopenProps
            ta.props = ProcessProps(
                p_uid=dfacts.transport_puid_from_index(node_index),
                critical=True,
                r_c_uid=None,
                stdin_connector=None,
                stdout_connector=None,
                stderr_connector=None,
                stdin_req=None,
                stdout_req=None,
                stderr_req=None,
                layout=None,
                local_cuids=set(),
                local_muids=set(),
                creation_msg_tag=None,
            )
        except Exception as e:
            logging.getLogger(dls.LS).getChild("start_ta").fatal("transport agent launch failed on %s" % node_index)
            raise RuntimeError("transport agent launch failed on node %s" % node_index) from e

        # Send LAChannelsInfo to TA
        ta_input.send(la_channels_info.serialize())

        # Confirmation TA is up
        ta_ping = dmsg.parse(ls_input.recv())
        assert isinstance(ta_ping, dmsg.TAPingSH), "ls did not receive ping from TA)"
        log.info("ls received TAPingSH - m7")
        ch_list = []
        if transport_test_env:
            first_cuid = dfacts.FIRST_CUID + node_index * 2
            second_cuid = dfacts.FIRST_CUID + node_index * 2 + 1
            barrier_cuid = dfacts.FIRST_CUID + node_count * 2

            ch1 = dch.Channel(start_pools[def_muid], first_cuid)
            ch2 = dch.Channel(start_pools[def_muid], second_cuid)
            ch1_ser = B64(ch1.serialize())
            ch2_ser = B64(ch2.serialize())
            ch_list.append(str(ch1_ser))
            ch_list.append(str(ch2_ser))
            if node_index == 0:
                # Create one Barrier channel for use in testing.
                ch3 = dch.Channel(start_pools[def_muid], barrier_cuid, capacity=node_count)
                ch3_ser = B64(ch3.serialize())
                ch_list.append(str(ch3_ser))

        la_input.send(dmsg.TAUp(tag=get_new_tag(), idx=node_index, test_channels=ch_list).serialize())
        log.info("ls send TAUp to la_be - m8.1")

        # Init these here so if in transport test mode, they have a value even though GS will not
        # be started or used in transport test mode.
        gs = None
        gs_in_wh = None

        if not transport_test_env:
            # Start global services on primary node.
            if is_primary:
                log.info("Starting global services on primary")
                gs = maybe_start_gs(gs_args, gs_env, hostname=hostname, be_in=la_input, logger_sdesc=logger_sdesc)
                if gs is None:
                    log.info("did not start gs")
                else:
                    log.info("gs up")

                gs_stdin = os.fdopen(gs.stdin.fileno(), "wb")
                gs_stdin_send = dutil.NewlineStreamWrapper(gs_stdin, read_intent=False)
                gs_stdin_send.send(la_channels_info.serialize())
                log.info("transmitted la_channels_info to gs")

            gs_ch = la_channels_info.gs_cd
            gs_in_ch = dch.Channel.attach(dutils.B64.str_to_bytes(gs_ch))
            log.info("ls attached to gs channel")
            gs_in_wh = dconn.Connection(outbound_initializer=gs_in_ch, policy=dparms.POLICY_INFRASTRUCTURE)

            gs_ping_ls = dmsg.parse(ls_input.recv())
            assert isinstance(gs_ping_ls, dmsg.GSPingSH), "ls expected GSPingSH"
            log.info("ls received GSPingSH from gs - m10")

            # Send response to GS
            gs_in_wh.send(dmsg.SHPingGS(tag=get_new_tag(), idx=node_index, node_sdesc=ls_node_desc.sdesc).serialize())
            log.info("ls sent SHPingGS - m11")
    except (OSError, EOFError, json.JSONDecodeError, AssertionError, RuntimeError) as rte:
        log.fatal("startup failed")
        LocalServer.clean_pools(start_pools, log)
        raise RuntimeError("startup fatal error") from rte

    server = LocalServer(
        channels=start_channels, pools=start_pools, transport_test_mode=transport_test_env, hostname=hostname
    )

    if gs is not None:
        server.add_proc(gs)
        server.gs_proc = gs

    if ta is not None:
        server.add_proc(ta)
        server.ta_proc = ta

    try:
        server.run(
            shep_in=ls_input,
            gs_in=gs_in_wh,
            be_in=la_input,
            is_primary=is_primary,
            ta_in=ta_input,
            gw_channels=gw_channels,
        )
    except Exception:
        log.fatal("There was an exception in LS and clean up is called.")
        server.cleanup()
        raise

    try:
        # m14 Recv BEHalted from BE
        # Wait till I'm told the backend is detached from me -- listening on the original pmsgqueue
        be_halted = dmsg.parse(ls_stdin_queue.recv())
        assert isinstance(be_halted, dmsg.BEHalted), "m14 BEHalted msg expected. Received %s" % type(be_halted)
        log.debug("m14 Received final BEHalted. Cleaning up and exiting.")
        ls_stdin_queue.close()
        ls_stdout_queue.close()

    except (OSError, AssertionError, EOFError, json.JSONDecodeError) as tde:
        log.fatal("teardown sequence error")
        raise RuntimeError("teardown") from tde
    finally:
        server.cleanup()

    log.info("shutdown complete")
