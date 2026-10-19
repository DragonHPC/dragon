"""Dragon local services"""

import io
import json
import logging
import os
import queue
import subprocess
import sys
import time
import traceback

from typing import Optional

import dragon.channels as dch
import dragon.managed_memory as dmm
import dragon.utils as dutils

from ..infrastructure.node_desc import NodeDescriptor
from ..transport import start_transport_agent

from ..infrastructure import facts as dfacts
from ..infrastructure import messages as dmsg
from ..infrastructure import parameters as dparms
from ..infrastructure import util as dutil
from ..dlogging.util import setup_BE_logging, DragonLoggingServices as dls
from ..utils import B64, set_procname
from ..infrastructure.queue import InfraQueue

from .server import LocalServer, PopenProps, get_new_tag, ProcessProps, OutputConnector

_LS_INPUT_STARTUP_EXPECTATION = "startup expectation on ls input"


def _get_startup_msg(ls_input, timeout, la_input, service_name, process=None):
    try:
        return ls_input.get(timeout=timeout)
    except (queue.Empty, TimeoutError):
        error_details = None
        if process is not None:
            _, stderr = process.communicate()
            if stderr:
                error_details = stderr.decode()
        if error_details is None:
            error_details = f"timed out waiting for LS bringup message after {timeout} seconds"

        error_str = f"Unable to bring up Dragon {service_name}: {error_details}"
        la_input.put(dmsg.AbnormalTermination(tag=get_new_tag(), err_info=error_str))
        raise RuntimeError(error_str)


def _build_attach_info(node_index, gs_qd):
    attach_info = {
        "node_index": node_index,
        "gs_qd": gs_qd,
        "ls_qd": dparms.this_process.local_ls_qd,
        "default_pd": dparms.this_process.default_pd,
        "inf_pd": dparms.this_process.inf_pd,
        "rt_uid": os.environ.get("DRAGON_RT_UID", ""),
    }
    return attach_info


def maybe_start_gs(
    gs_args: Optional[list],
    gs_env: Optional[dict],
    hostname: str,
    be_in: object,
    logger_sdesc: B64 = None,
    *,
    is_primary=True,
    la_channels_info=None,
):
    """Optionally start GS and, for multinode startup, attach to its channel.

    Args:
        gs_args (list): Arguments for starting global services on the primary node.

        gs_env (dict): Environment to use when launching GS.

        hostname (str): Hostname recorded for GS output forwarding.

        be_in (object): Backend input queue used to forward GS stdout and stderr.

        logger_sdesc (B64, optional): Logging channel descriptor for GS.

        is_primary (bool): Whether this node is responsible for launching GS.

        la_channels_info (LAChannelsInfo, optional): Multinode channel information.
            When provided, it is sent to the primary GS and this node attaches to
            the shared GS channel. Non-primary nodes attach without launching GS.

    Raises:
        RuntimeError: GS launch was not successful.

    Returns:
        Without ``la_channels_info``, the GS process, or ``None`` if it was not
        launched. With ``la_channels_info``, a tuple containing the GS process
        (or ``None`` on non-primary nodes) and the attached GS queue.
    """
    log = logging.getLogger(dls.LS).getChild("maybe_start_gs")
    if is_primary and gs_args is not None:
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
                out_err=dmsg.LSFwdOutput.FDNum.STDOUT.value,
                conn=None,
                root_proc=True,
                critical_proc=True,
            )
            stderr_connector = OutputConnector(
                be_in=be_in,
                puid=dfacts.GS_PUID,
                hostname=hostname,
                out_err=dmsg.LSFwdOutput.FDNum.STDERR.value,
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

    if la_channels_info is None:
        return gs_proc

    if is_primary:
        log.info("Starting global services on primary")
        if gs_proc is None:
            log.info("did not start gs")
        else:
            log.info("gs up")

        gs_stdin = gs_proc.stdin
        gs_proc.stdin = None
        gs_stdin_send = dutil.NewlineStreamWrapper(gs_stdin, read_intent=False, b64_encode_decode=True)
        gs_stdin_send.send(la_channels_info.serialize())
        log.info("transmitted la_channels_info to gs")

    gs_in_wh = InfraQueue.attach(la_channels_info.gs_qd)
    log.info("ls attached to gs channel")
    return gs_proc, gs_in_wh


def mk_inf_resources(node_index):
    log = logging.getLogger(dls.LS).getChild("inf resource maker")
    log.info("creating  pools and channels")

    inf_pool = None
    def_pool = None

    _user = os.environ.get("USER", str(os.getuid()))

    try:
        inf_muid = dfacts.infrastructure_pool_muid_from_index(node_index)
        ips = dparms.this_process.inf_seg_sz
        ipn = dfacts.runtime_pool_name(dfacts.INFRASTRUCTURE_POOL_SUFFIX)
        inf_pool = dmm.MemoryPool.create_with_retry(ips, ipn, inf_muid)
        log.info("inf pool: %s size %s" % (ipn, ips))

        def_muid = dfacts.default_pool_muid_from_index(node_index)
        dps = dparms.this_process.default_seg_sz
        dpn = dfacts.runtime_pool_name(dfacts.DEFAULT_POOL_SUFFIX)
        def_pool = dmm.MemoryPool.create_with_retry(
            dps, dpn, def_muid, pre_alloc_blocks=[8, 8, 8, 8, 8, 8, 8, 8, 0, 0, 0, 0, 8]
        )
        log.info("def pool: %s size %s" % (dpn, dps))

        # PJM TODO: how pre-allocated blocks are done here is temporary.  We also need a better way
        # to manage Pool attributes as there are many more we will expose.

        start_pools = {inf_muid: inf_pool, def_muid: def_pool}
        dparms.this_process.inf_pd = dutils.B64.bytes_to_str(inf_pool.serialize())
        dparms.this_process.default_pd = dutils.B64.bytes_to_str(def_pool.serialize())

        ls_input_cuid = dfacts.localservices_cuid_from_index(node_index)
        ls_ch = dch.Channel(inf_pool, ls_input_cuid, None)
        ls_input = InfraQueue(pool=inf_pool, main_channel=ls_ch)
        dparms.this_process.local_ls_qd = ls_input.serialize()

        la_input_cuid = dfacts.launcher_cuid_from_index(node_index)
        la_input_ch = dch.Channel(inf_pool, la_input_cuid, None)
        dparms.this_process.be_cuid = dfacts.launcher_cuid_from_index(node_index)
        la_input = InfraQueue(pool=inf_pool, main_channel=la_input_ch)
        dparms.this_process.local_be_qd = la_input.serialize()

        ta_input_cuid = dfacts.transport_cuid_from_index(node_index)
        ta_input_ch = dch.Channel(inf_pool, ta_input_cuid, None)
        ta_input = InfraQueue(pool=inf_pool, main_channel=ta_input_ch)
        ta_input_descr = ta_input.serialize()
        dparms.this_process.local_ta_cd = ta_input_descr

        start_channels = {ls_input_cuid: ls_ch, la_input_cuid: la_input_ch, ta_input_cuid: ta_input_ch}

        if 0 == node_index:
            gs_chan = dch.Channel(inf_pool, dfacts.GS_INPUT_CUID, None)
            start_channels[dfacts.GS_INPUT_CUID] = gs_chan
            gs_input = InfraQueue(pool=inf_pool, main_channel=gs_chan)
            dparms.this_process.gs_qd = gs_input.serialize()
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
    return start_pools, start_channels, ls_input, la_input, ta_input_descr, ta_input, gs_input


def _write_attach_info_file(attach_descr):
    """Write this node's Base64-encoded AttachableRuntimeDescr to the attach-info directory.

    The auth token is intentionally omitted from the file; attaching processes
    supply it via ``dragon.attach(auth_token=...)`` or the ``DRAGON_ATTACH_TOKEN``
    environment variable.
    """
    log = logging.getLogger(dls.LS).getChild("write_attach_info")
    try:
        attach_dir = dfacts.attach_runtime_dir()
        os.makedirs(attach_dir, mode=0o700, exist_ok=True)
        # Leave the permissions of a user-chosen DRAGON_ATTACH_DIR alone.
        if attach_dir == dfacts.default_attach_runtime_dir():
            os.chmod(attach_dir, 0o700)
        attach_path = os.path.join(attach_dir, dfacts.ATTACH_INFO_FILENAME)
        fd = os.open(attach_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as fh:
            fh.write(dutils.b64encode(attach_descr.serialize()))
        log.info("wrote attach info to %s", attach_path)
    except Exception as ex:  # pylint: disable=broad-except
        log.warning("could not write attach info file: %s", ex)


def _remove_attach_info_file():
    """Best-effort removal of this node's attach-info file and directory."""
    log = logging.getLogger(dls.LS).getChild("remove_attach_info")
    try:
        attach_dir = dfacts.attach_runtime_dir()
        attach_path = os.path.join(attach_dir, dfacts.ATTACH_INFO_FILENAME)
        try:
            os.remove(attach_path)
        except OSError:
            pass
        try:
            os.rmdir(attach_dir)
        except OSError:
            pass
    except Exception as ex:  # pylint: disable=broad-except
        log.warning("could not remove attach info file: %s", ex)


def get_ls_msg_queue(stdin=None, stdout=None):
    if stdin is None:
        # detach() transfers the BufferedReader out of the TextIOWrapper so only
        # one object owns FD 0 (avoids EBADF double-close in Python 3.13).
        # Replace sys.stdin with a harmless dummy so Python's shutdown finalizer
        # doesn't raise "underlying buffer has been detached".
        bin_stdin = sys.stdin.detach()
        sys.stdin = io.StringIO()
        ls_stdin_msg = dutil.NewlineStreamWrapper(bin_stdin, write_intent=False, b64_encode_decode=True)
    else:
        ls_stdin_msg = stdin

    if stdout is None:
        bin_stdout = sys.stdout.detach()
        sys.stdout = io.StringIO()
        ls_stdout_msg = dutil.NewlineStreamWrapper(bin_stdout, write_intent=True, b64_encode_decode=True)
    else:
        ls_stdout_msg = stdout

    return ls_stdin_msg, ls_stdout_msg


def single(
    make_infrastructure_resources: bool = True,
    gs_input=None,
    la_input=None,
    ls_input=None,
    gs_args=None,
    gs_env=None,
    ls_stdin=None,
    ls_stdout=None,
    ta_input=None,
    attach_auth_token: str = None,
):
    log = logging.getLogger("local_svc single node main")
    log.info("starting on pid=%s" % os.getpid())

    start_pools = {}
    start_channels = {}

    try:  # attempt to construct infrastructure objects and establish communications
        os.setpgid(0, 0)

        ls_stdin_msg, ls_stdout_msg = get_ls_msg_queue(ls_stdin, ls_stdout)

        msg = dmsg.parse(ls_stdin_msg.recv())
        assert isinstance(msg, dmsg.BENodeIdxLS), "startup msg expected on stdin"
        log.info("got BENodeIdxLS")
        assert msg.node_idx == 0, "single node"
        node_index = msg.node_idx
        net_conf_key = msg.net_conf_key

        if make_infrastructure_resources:
            assert not any((gs_input, la_input, ls_input, ta_input))
            start_pools, start_channels, ls_input, la_input, ta_input_descr, ta_input, gs_input = mk_inf_resources(
                node_index
            )

        dparms.this_process.index = node_index
        os.environ.update(dparms.this_process.env())
        be_ping = dmsg.LSPingBE(
            tag=get_new_tag(),
            ls_qd=dparms.this_process.local_ls_qd,
            be_qd=dparms.this_process.local_be_qd,
            gs_qd=dparms.this_process.gs_qd,
            default_pd=dparms.this_process.default_pd,
            inf_pd=dparms.this_process.inf_pd,
        )
        ls_stdout_msg.send(be_ping.serialize())
        log.info("wrote LSPingBE")

        log.info("getting on ls_input")
        msg = ls_input.get()
        assert isinstance(msg, dmsg.BEPingLS), _LS_INPUT_STARTUP_EXPECTATION
        log.info("got BEPingLS")

        ls_node_desc = NodeDescriptor.get_localservices_node_conf(
            host_name="localhost", name="localhost", ip_addrs=["127.0.0.1"], is_primary=True
        )
        ch_up_msg = dmsg.LSChannelsUp(
            tag=get_new_tag(), node_desc=ls_node_desc, gs_qd=dparms.this_process.gs_qd, net_conf_key=net_conf_key
        )

        la_input.put(ch_up_msg)
        log.info("sent LSChannelsUp")
        gs_proc = maybe_start_gs(gs_args, gs_env, hostname="localhost", be_in=la_input, is_primary=True)
        msg = ls_input.get()
        assert isinstance(msg, dmsg.GSPingLS), _LS_INPUT_STARTUP_EXPECTATION
        log.info("got GSPingLS")
    except (OSError, EOFError, json.JSONDecodeError, AssertionError, RuntimeError) as rte:
        log.fatal("startup failed")
        LocalServer.clean_pools(start_pools, log)
        raise RuntimeError("startup fatal error") from rte

    gs_input.put(dmsg.LSPingGS(tag=get_new_tag(), node_sdesc=ls_node_desc.sdesc))

    server = LocalServer(
        channels=start_channels, pools=start_pools, hostname="localhost", attach_auth_token=attach_auth_token
    )

    if attach_auth_token is not None:
        attach_info = _build_attach_info(node_index, dparms.this_process.gs_qd)
        _write_attach_info_file(dmsg.AttachableRuntimeDescr(tag=get_new_tag(), **attach_info))
        # this ordering matters. we use this to write the auth token out to the frontend
        attach_info["auth_token"] = attach_auth_token
        server.kvs[dfacts.ATTACH_INFO_KVS_KEY] = json.dumps(attach_info)

        log.info("pre-populated LS KVS with attach-info (key=%s)", dfacts.ATTACH_INFO_KVS_KEY)

    if gs_proc is not None:
        server.add_proc(gs_proc)
        server.gs_proc = gs_proc

    try:
        server.run(ls_in=ls_input, gs_in=gs_input, be_in=la_input, is_primary=True)
    except Exception:
        log.fatal("There was an exception and LS is going to clean up.")
        server.cleanup()
        if attach_auth_token is not None:
            _remove_attach_info_file()
        raise

    try:
        msg = dmsg.parse(ls_stdin_msg.recv())
        assert isinstance(msg, dmsg.BEHalted)
        ls_stdout_msg.send(dmsg.LSHalted(tag=get_new_tag()).serialize())
    except (OSError, AssertionError, EOFError, json.JSONDecodeError) as tde:
        log.fatal("teardown sequence error")
        raise RuntimeError("teardown") from tde
    finally:
        server.cleanup()
        if attach_auth_token is not None:
            _remove_attach_info_file()

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
):
    """Local services for multi-node dragon"""

    setup_BE_logging(service=dls.LS, fname=fname)
    log = logging.getLogger(dls.LS).getChild("multinode")
    log.debug("starting debug modes on pid=%s" % os.getpid())

    set_procname(dfacts.PROCNAME_LS)

    start_pools = {}
    start_channels = {}
    attach_auth_token = None

    try:  # attempt to construct infrastructure objects and establish communications
        os.setpgid(0, 0)

        ls_stdin_queue, ls_stdout_queue = get_ls_msg_queue(ls_stdin, ls_stdout)

        msg = dmsg.parse(ls_stdin_queue.recv())
        assert isinstance(msg, dmsg.BENodeIdxLS), "startup msg expected on stdin"
        node_index = msg.node_idx
        net_conf_key = msg.net_conf_key
        hostname = msg.host_name
        ip_addrs = msg.ip_addrs
        is_primary = msg.primary
        logger_sdesc = msg.logger_sdesc
        log.info(
            "got BENodeIdxLS (id=%s, ips=%s, host=%s, primary=%s) - m2.1" % (node_index, ip_addrs, hostname, is_primary)
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
        os.environ[dfacts.DRAGON_LOGGER_SDESC] = str(logger_sdesc)

        if is_primary:
            gs_qd = dparms.this_process.gs_qd
        else:
            gs_qd = None

        be_ping = dmsg.LSPingBE(
            tag=get_new_tag(),
            ls_qd=dparms.this_process.local_ls_qd,
            be_qd=dparms.this_process.local_be_qd,
            gs_qd=gs_qd,
            default_pd=dparms.this_process.default_pd,
            inf_pd=dparms.this_process.inf_pd,
        )
        ls_stdout_queue.send(be_ping.serialize())
        log.info("wrote LSPingBE")

        msg = ls_input.get()
        assert isinstance(msg, dmsg.BEPingLS), _LS_INPUT_STARTUP_EXPECTATION
        log.info("got BEPingLS")

        # Create a node descriptor for this node I'm running on
        ls_node_desc = NodeDescriptor.get_localservices_node_conf(
            host_name=hostname, name=hostname, ip_addrs=ip_addrs, is_primary=is_primary
        )
        ch_up_msg = dmsg.LSChannelsUp(
            tag=get_new_tag(), node_desc=ls_node_desc, gs_qd=gs_qd, idx=node_index, net_conf_key=net_conf_key
        )

        la_input.put(ch_up_msg)
        log.info("sent LSChannelsUp")

        # Recv LAChannelsInfo Broadcast
        la_channels_info = ls_input.get()
        assert isinstance(la_channels_info, dmsg.LAChannelsInfo), "expected LAChannelsInfo"
        log.info("node index %s received all channels info" % node_index)
        log.debug("la_channels.nodes_desc: %s" % la_channels_info.nodes_desc)
        log.debug("la_channels.gs_qd: %s" % la_channels_info.gs_qd)
        # get auth token if it is in the message
        attach_auth_token = getattr(la_channels_info, "attach_auth_token", None)

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
        # variables. This is to enable the local services
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
            ta = start_transport_agent(node_index, ta_input_descr, logger_sdesc, args=ta_args, env=ta_env)
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
            la_input.put(dmsg.AbnormalTermination(tag=get_new_tag(), err_info="transport agent launch failed"))
            raise RuntimeError("transport agent launch failed on node %s" % node_index) from e

        # Send LAChannelsInfo to TA
        ta_input.put(la_channels_info)

        # Do a recv for TAPingLS from the transport agent letting us know it's ready for multi-node communication
        gs_ping_ls = None
        ta_deadline = time.monotonic() + dfacts.TA_PING_TIMEOUT
        ta_ping = _get_startup_msg(
            ls_input,
            dfacts.TA_PING_TIMEOUT,
            la_input,
            "transport agent",
            ta,
        )
        log.info("Received message %s", type(ta_ping))

        # If we are non-primary and got GSPingLS first, cache the GSPingLS and re-enter the recv for TAPingLS
        if not is_primary and isinstance(ta_ping, dmsg.GSPingLS):
            log.info("Received GSpingLS prior to TAPingLS, saving for GS handshake")
            gs_ping_ls = ta_ping
            remaining = max(ta_deadline - time.monotonic(), 0)
            ta_ping = _get_startup_msg(
                ls_input,
                remaining,
                la_input,
                "transport agent",
                ta,
            )

        # We should definitely have the TAPingLS now. If not, exit bring-up. Else, continue with the startup process.
        if not isinstance(ta_ping, dmsg.TAPingLS):
            error_str = f"ls expected TAPingLS, received {type(ta_ping)}"
            log.fatal(error_str)
            la_input.put(dmsg.AbnormalTermination(tag=get_new_tag(), err_info=error_str))
            raise RuntimeError(error_str)
        log.info("ls received TAPingLS - m7")
        la_input.put(dmsg.TAUp(tag=get_new_tag(), idx=node_index))
        log.info("ls send TAUp to la_be - m8.1")

        # Start the global services if primary and obtain the local comms channel
        gs, gs_in_wh = maybe_start_gs(
            gs_args,
            gs_env,
            hostname,
            la_input,
            logger_sdesc,
            is_primary=is_primary,
            la_channels_info=la_channels_info,
        )

        # Get the GSPingLS if we haven't received it already
        if gs_ping_ls is None:
            gs_timeout = dfacts.REMOTE_LAUNCH_TIMEOUT
            if is_primary:
                gs_timeout = dfacts.LOCAL_LAUNCH_TIMEOUT
            gs_ping_ls = _get_startup_msg(
                ls_input,
                gs_timeout,
                la_input,
                "global services",
                gs,
            )
        if not isinstance(gs_ping_ls, dmsg.GSPingLS):
            assert False, f"ls expected GSPingLS, received {type(gs_ping_ls)}"
        log.info("ls received GSPingLS from gs - m10")

        # Send LSPingGS response to GS
        gs_in_wh.put(dmsg.LSPingGS(tag=get_new_tag(), idx=node_index, node_sdesc=ls_node_desc.sdesc))
        log.info("ls sent LSPingGS - m11")
    except (OSError, EOFError, json.JSONDecodeError, AssertionError, RuntimeError) as rte:
        tb = traceback.format_exc()
        log.fatal(f"startup failed: Exception was {rte}\nwith traceback {tb}")
        LocalServer.clean_pools(start_pools, log)
        if attach_auth_token is not None:
            _remove_attach_info_file()
        raise RuntimeError("startup fatal error") from rte

    server = LocalServer(
        channels=start_channels,
        pools=start_pools,
        hostname=hostname,
        attach_auth_token=attach_auth_token,
    )

    if gs is not None:
        server.add_proc(gs)
        server.gs_proc = gs

    if ta is not None:
        server.add_proc(ta)
        server.ta_proc = ta

    # If the runtime was launched --attachable, publish this node's own
    # attach-info file. LS validates the auth token itself, so it is not
    # forwarded to GS.
    if attach_auth_token is not None:
        attach_info = _build_attach_info(node_index, la_channels_info.gs_qd)
        _write_attach_info_file(dmsg.AttachableRuntimeDescr(tag=get_new_tag(), **attach_info))
        # this ordering matters. we use this to write the auth token out to the frontend
        attach_info["auth_token"] = attach_auth_token
        server.kvs[dfacts.ATTACH_INFO_KVS_KEY] = json.dumps(attach_info)
        log.info("pre-populated LS KVS with attach-info (key=%s)", dfacts.ATTACH_INFO_KVS_KEY)

    try:
        server.run(
            ls_in=ls_input,
            gs_in=gs_in_wh,
            be_in=la_input,
            is_primary=is_primary,
            ta_in=ta_input,
            gw_channels=gw_channels,
        )
    except Exception:
        log.fatal("There was an exception in LS and clean up is called.")
        server.cleanup()
        if attach_auth_token is not None:
            _remove_attach_info_file()
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
        if attach_auth_token is not None:
            _remove_attach_info_file()

    log.info("shutdown complete")
