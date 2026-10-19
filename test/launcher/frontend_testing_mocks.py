import os
import logging
import time
from typing import Optional

from dragon.launcher.frontend import LauncherFrontEnd
from dragon.launcher.util import next_tag
from dragon.launcher.network_config import NetworkConfig
from dragon.launcher.dragon_multi_fe import main as frontend_main

from dragon.infrastructure.process_desc import ProcessDescriptor
from dragon.infrastructure.node_desc import NodeDescriptor
from dragon.infrastructure import facts as dfacts
from dragon.infrastructure import messages as dmsg
from dragon.infrastructure.messages import MessagePickler

from dragon.channels import Channel, ChannelError
from dragon.fli import DragonFLIError
from dragon.managed_memory import MemoryPool, DragonPoolError, DragonMemoryError
from dragon.native.queue import Queue as DQueue
from dragon.utils import set_host_id, host_id, B64
from dragon.dlogging.util import setup_FE_logging

from .launcher_testing_utils import _GarbageMsg

MOCK_CALL_TIMEOUT = 30.0


def ensure_frontend_host_id():
    """Claim the frontend host ID for this process.

    dragon_set_host_id() is write-once per process, so this only succeeds for the
    first frontend test in a process. Never read the host ID before setting it:
    dragon_host_id() latches the same flag and would make the set impossible.
    """
    try:
        set_host_id(dfacts.FRONTEND_HOSTID)
    except RuntimeError as e:
        current = host_id()
        if current != dfacts.FRONTEND_HOSTID:
            raise RuntimeError(
                f"host ID is already set to {current} rather than "
                f"FRONTEND_HOSTID ({dfacts.FRONTEND_HOSTID}); the frontend tests need a "
                "process where no other component has claimed the host ID first"
            ) from e


def run_frontend(args_map):

    # Before doing anything set my host ID
    ensure_frontend_host_id()
    setup_FE_logging(log_device_level_map=args_map["log_device_level_map"], basename="dragon", basedir=os.getcwd())
    log = logging.getLogger("run_frontend")
    for key, value in args_map.items():
        if value is not None:
            log.info(f"args_map: {key}: {value}")

    with LauncherFrontEnd(args_map=args_map) as fe_server:
        fe_server.run_startup()
        fe_server.run_app()
        fe_server.run_msg_server()


def run_resilient_frontend(args_map):

    frontend_main(args_map=args_map)


def open_overlay_comms(ch_in_sdesc: str, ch_out_sdesc: str, mpool=None):
    """Attach to Frontend's overlay DQueues.
    ch_in_sdesc = local_out_q.serialize() — overlay reads what FE writes (LAHaltOverlay)
    ch_out_sdesc = local_in_q.serialize() — overlay writes what FE reads (OverlayPingLA, OverlayHalted)

    overlay_in_q receives with mpool (allocates receive buffers from caller's pool).
    overlay_out_q sends with mpool=None so the FLI uses the channel's own pool, which
    must match the pool that the FE used when it created local_in_q.  Using a foreign
    pool here caused the FE's DQueue.get() to never be signalled, and potentially a
    use-after-free crash when be_mpool was destroyed while the FE was still mid-read.
    """
    try:
        overlay_in_q = DQueue.attach(ch_in_sdesc, mpool=mpool, pickler=MessagePickler())
        overlay_out_q = DQueue.attach(ch_out_sdesc, mpool=None, pickler=MessagePickler())
    except Exception as init_err:
        raise RuntimeError("Overlay transport resource creation failed") from init_err
    return overlay_in_q, overlay_out_q


def open_backend_comms(frontend_sdesc: str, network_config: str, net_conf: Optional[dict] = None, be_mpool=None):
    """Attach to frontend DQueue and create per-node backend DQueues."""
    log = logging.getLogger("open_backend_comms")
    _created_mpool = be_mpool is None
    try:
        if be_mpool is None:
            be_mpool = MemoryPool(
                int(dfacts.DEFAULT_BE_OVERLAY_TRANSPORT_SEG_SZ),
                f"{os.getuid()}_{os.getpid()}_{2}" + dfacts.DEFAULT_POOL_SUFFIX,
                dfacts.be_pool_muid_from_hostid(2),
            )

        fe_in_q = DQueue.attach(frontend_sdesc, mpool=be_mpool, pickler=MessagePickler())

        if net_conf is None:
            net = NetworkConfig.from_file(network_config)
            net_conf = net.get_network_config()

        be_nodes = {}
        log.info(f"net_conf in open_backend_comms: {net_conf}")
        net_conf_key_mapping = list(net_conf.keys())
        for idx, node in net_conf.items():
            if node.state == NodeDescriptor.State.ACTIVE:
                be_cuid = dfacts.be_fe_cuid_from_hostid(node.host_id)
                be_ch_in = Channel(be_mpool, be_cuid)
                be_in_q = DQueue(main_channel=be_ch_in, pool=be_mpool, pickler=MessagePickler())

                be_nodes[node.host_id] = {
                    "fe_in_q": fe_in_q,
                    "be_in_q": be_in_q,
                    "be_ch_in": be_ch_in,
                    "hostname": node.name,
                    "ip_addrs": node.ip_addrs,
                    "state": node.state,
                    "node_index": idx,
                    "net_conf_key_mapping": net_conf_key_mapping,
                    "net_conf_key": str(idx),
                    "is_primary": node.is_primary,
                }
                log.debug(f"constructed backend node: {be_nodes[node.host_id]}")

    except (ChannelError, DragonPoolError, DragonMemoryError) as init_err:
        if _created_mpool and be_mpool is not None:
            be_mpool.destroy()
        raise RuntimeError("Overlay transport resource creation failed") from init_err

    return be_mpool, fe_in_q, be_nodes


def send_beisup(nodes):
    """Send valid BEIsUp messages to FE using DQueue."""

    for host_id, node in nodes.items():
        be_up_msg = dmsg.BEIsUp(
            tag=next_tag(),
            be_ch_desc=node["be_in_q"].serialize(),
            host_id=host_id,
        )
        node["fe_in_q"].put(be_up_msg)


def recv_fenodeidx(nodes):
    """Recv FENodeIdxBE and finish filling out node dictionary. Returns primary_be_in_q."""
    log = logging.getLogger("recv_fe_nodeidx")
    host_ids = [key for key in nodes.keys()]
    primary_be_in_q = None
    for idx, _ in enumerate(host_ids):
        if nodes[host_ids[idx]]["state"] == NodeDescriptor.State.ACTIVE:
            fe_node_idx_msg = nodes[host_ids[idx]]["be_in_q"].get()
            assert isinstance(fe_node_idx_msg, dmsg.FENodeIdxBE), "la_be node_index from fe expected"

            log.info(f"got FENodeIdxBE for index {fe_node_idx_msg.node_index}")
            nodes[host_ids[idx]]["node_index"] = fe_node_idx_msg.node_index
            if nodes[host_ids[idx]]["node_index"] < 0:
                raise RuntimeError("frontend giving bad node indices")
            nodes[host_ids[idx]]["is_primary"] = nodes[host_ids[idx]]["node_index"] == 0
            if nodes[host_ids[idx]]["is_primary"]:
                primary_be_in_q = nodes[host_ids[idx]]["be_in_q"]
            log.info(f"constructed be node: {nodes[host_ids[idx]]}")

    return primary_be_in_q


def send_shchannelsup(nodes, mpool):
    log = logging.getLogger("send_shchannelsup")
    for host_id, node in nodes.items():
        ls_cuid = dfacts.localservices_cuid_from_index(node["node_index"])
        ls_ch = Channel(mpool, ls_cuid)
        node["ls_ch"] = ls_ch

        if node["is_primary"]:
            node["gs_ch"] = Channel(mpool, dfacts.GS_INPUT_CUID)
            gs_cd = B64.bytes_to_str(node["gs_ch"].serialize())
        else:
            node["gs_ch"] = None
            gs_cd = None
        node_desc = NodeDescriptor(
            host_name=node["hostname"],
            host_id=host_id,
            ip_addrs=node["ip_addrs"],
            ls_cd=B64.bytes_to_str(node["ls_ch"].serialize()),
        )
        ch_up_msg = dmsg.LSChannelsUp(
            tag=next_tag(), node_desc=node_desc, gs_qd=gs_cd, idx=node["node_index"], net_conf_key=node["net_conf_key"]
        )
        log.info(f"construct LSChannelsUp: {ch_up_msg}")
        node["fe_in_q"].put(ch_up_msg)
        log.info(f'sent LSChannelsUp for {node["node_index"]}')

    log.info("sent all LSChannelsUp")


def recv_lachannelsinfo(nodes):
    """Recv all LAChannelsInfo messages in frontend bcast."""
    la_channels_info_msg = None
    for host_id, node in nodes.items():
        la_channels_info_msg = node["be_in_q"].get()
        assert isinstance(la_channels_info_msg, dmsg.LAChannelsInfo), "la_be expected all ls channels info from la_fe"
    return la_channels_info_msg


def send_taup(nodes):
    for host_id, node in nodes.items():
        ta_up = dmsg.TAUp(tag=next_tag(), idx=node["node_index"])
        node["fe_in_q"].put(ta_up)


def send_abnormal_term(fe_in_q, host_id=0):
    abnorm = dmsg.AbnormalTermination(tag=next_tag(), host_id=host_id)
    fe_in_q.put(abnorm)


def handle_gsprocesscreate(primary_be_in_q, fe_in_q):
    """Manage a valid response to GSProcessCreate."""

    log = logging.getLogger("handle_gsprocesscreate")
    proc_create = primary_be_in_q.get()
    log.info("presumably got GSProcessCreate")
    assert isinstance(proc_create, dmsg.GSProcessCreate)
    log.info("recvd GSProcessCreate")

    gs_desc = ProcessDescriptor(p_uid=5000, name=proc_create.user_name, node=0, p_p_uid=proc_create.p_uid)
    response = dmsg.GSProcessCreateResponse(
        tag=next_tag(), ref=proc_create.tag, err=dmsg.GSProcessCreateResponse.Errors.SUCCESS, desc=gs_desc
    )
    fe_in_q.put(response)


def handle_gsprocesscreate_error(primary_be_in_q, fe_in_q):
    """Indicate an error with GSProcessCreate."""

    log = logging.getLogger("handle_gsprocesscreate_error")
    proc_create = primary_be_in_q.get()
    log.info("presumably got GSProcessCreate")
    assert isinstance(proc_create, dmsg.GSProcessCreate)
    log.info("recvd GSProcessCreate")

    response = dmsg.GSProcessCreateResponse(
        tag=next_tag(),
        ref=proc_create.tag,
        err=dmsg.GSProcessCreateResponse.Errors.FAIL,
        err_info="Error starting Head Process",
    )
    fe_in_q.put(response)


def wait_for_mock_call(mock_obj, what, timeout=MOCK_CALL_TIMEOUT):
    """Wait for the frontend thread to invoke a patched entry point.

    The frontend runs in a worker thread whose exceptions are captured by
    catch_thread_exceptions, so if it dies early nothing here would ever be
    called. Time out rather than spinning forever on a thread that is gone.
    """
    deadline = time.monotonic() + timeout
    while len(mock_obj.call_args_list) == 0:
        if time.monotonic() > deadline:
            raise TimeoutError(
                f"frontend never called {what} within {timeout}s -- the frontend "
                "thread most likely raised before reaching it; check the captured "
                "thread exceptions"
            )
        time.sleep(0.01)


def stand_up_backend(mock_overlay, mock_launch, network_config, net_conf=None):

    log = logging.getLogger("mock_backend_standup")
    wait_for_mock_call(mock_overlay, "start_overlay_network")
    overlay_args = mock_overlay.call_args_list.pop().kwargs
    overlay = {}

    # Create be_mpool before attaching to any FE-owned queues so all FLI
    # allocations come from be_mpool rather than fe_mpool. This prevents a
    # use-after-free crash when the FE destroys fe_mpool concurrently with a
    # put() call in the mock (the FLI would otherwise allocate send buffers
    # from fe_mpool).
    be_mpool = MemoryPool(
        int(dfacts.DEFAULT_BE_OVERLAY_TRANSPORT_SEG_SZ),
        f"{os.getuid()}_{os.getpid()}_{2}" + dfacts.DEFAULT_POOL_SUFFIX,
        dfacts.be_pool_muid_from_hostid(2),
    )

    overlay["overlay_in_q"], overlay["overlay_out_q"] = open_overlay_comms(
        overlay_args["ch_in_sdesc"], overlay_args["ch_out_sdesc"], mpool=be_mpool
    )

    # Let frontend know the overlay is "up"
    overlay["overlay_out_q"].put(dmsg.OverlayPingLA(next_tag()))

    wait_for_mock_call(mock_launch, "_launch_backend")
    launch_be_args = mock_launch.call_args_list.pop().kwargs
    log.info(f"got be args: {launch_be_args}")

    # Pass the already-created be_mpool so open_backend_comms doesn't create a new one
    overlay["be_mpool"], overlay["fe_in_q"], overlay["be_nodes"] = open_backend_comms(
        launch_be_args["frontend_sdesc"], network_config, net_conf=net_conf, be_mpool=be_mpool
    )
    log.info("got backend up")

    return overlay


def handle_bringup(mock_overlay, mock_launch, network_config, net_conf=None):

    log = logging.getLogger("mock_full_bringup")

    overlay = stand_up_backend(mock_overlay, mock_launch, network_config, net_conf=net_conf)

    send_beisup(overlay["be_nodes"])
    log.info("send BEIsUp messages")

    overlay["primary_be_in_q"] = recv_fenodeidx(overlay["be_nodes"])
    log.info("got all the FENodeIdxBE messages")

    send_shchannelsup(overlay["be_nodes"], overlay["be_mpool"])
    log.info(
        f'sent shchannelsup: {[node["gs_ch"].serialize() for node in overlay["be_nodes"].values() if node.get("gs_ch") is not None]}'
    )

    la_info = recv_lachannelsinfo(overlay["be_nodes"])
    log.info("la_be received LAChannelsInfo")

    send_taup(overlay["be_nodes"])
    log.info("sent TAUp messages")

    overlay["fe_in_q"].put(dmsg.GSIsUp(tag=next_tag()))
    log.info("send GSIsUp")

    return overlay, la_info


def handle_overlay_teardown(overlay_in_q, overlay_out_q):
    """Complete teardown of frontend overlay process."""
    import queue as _queue

    log = logging.getLogger("handle_overlay_teardown")
    try:
        halt_on = overlay_in_q.get(timeout=10.0)
        assert isinstance(halt_on, dmsg.LAHaltOverlay)
    except _queue.Empty:
        log.warning("handle_overlay_teardown: LAHaltOverlay not received within timeout")
        return

    # This may fail depending on the testing infrastructure
    try:
        overlay_out_q.put(dmsg.OverlayHalted(tag=next_tag()))
    except (ChannelError, ValueError, DragonFLIError):
        pass


def handle_teardown(
    nodes,
    primary_be_in_q,
    fe_in_q,
    overlay_in_q,
    overlay_out_q,
    timeout_backend=False,
    timeout_overlay=False,
    gs_head_exit=True,
    abort_shteardown=None,
    abnormal_termination=False,
):
    """Do full teardown from perspective of backend.

    Uses timeouts on all queue operations so that if the FE exits early
    (e.g. during abnormal teardown race), the mock bails out gracefully
    instead of blocking on a closed DQueue and causing a segfault.
    """
    import queue as _queue

    log = logging.getLogger("handle_teardown")

    try:
        if gs_head_exit:
            fe_in_q.put(dmsg.GSHeadExit(exit_code=0, tag=next_tag()), timeout=2.0)

        gs_teardown = primary_be_in_q.get(timeout=10.0)
        assert isinstance(gs_teardown, dmsg.GSTeardown)
        log.debug("got gsteardown")

        fe_in_q.put(dmsg.GSHalted(tag=next_tag()), timeout=2.0)
        log.debug("sent gshalted")

        for node in nodes.values():
            ta_halt_msg = node["be_in_q"].get(timeout=10.0)
            assert isinstance(ta_halt_msg, dmsg.LSHaltTA), "LSHaltTA from fe expected"
            node["fe_in_q"].put(dmsg.TAHalted(tag=next_tag()), timeout=2.0)
        log.debug("received all LSHaltTA from frontend")

        if timeout_backend:
            log.info("returning early during teardown to test frontend teardown timeout")
            return

        for index, node in enumerate(nodes.values()):
            count = 2 if not abnormal_termination else 1
            for _ in range(count):
                sh_teardown_msg = node["be_in_q"].get(timeout=10.0)
                assert isinstance(sh_teardown_msg, dmsg.LSTeardown), "LSTeardown from fe expected"

            if abort_shteardown is not None and index == abort_shteardown:
                node["fe_in_q"].put(dmsg.AbnormalTermination(tag=next_tag()), timeout=2.0)
            node["fe_in_q"].put(dmsg.LSHaltBE(tag=next_tag()), timeout=2.0)

        for node in nodes.values():
            be_halted_msg = node["be_in_q"].get(timeout=10.0)
            assert isinstance(be_halted_msg, dmsg.BEHalted), f"BEHalted from fe expected got {type(be_halted_msg)}"

        if not timeout_overlay:
            handle_overlay_teardown(overlay_in_q, overlay_out_q)

    except _queue.Full:
        log.warning(
            "handle_teardown: fe_in_q.put() timed out — FE already closed its inbound queue (abnormal exit path)"
        )
    except _queue.Empty:
        log.warning("handle_teardown: be_in_q.get() timed out — FE did not send expected message (abnormal exit path)")
    except DragonFLIError:
        log.warning("handle_teardown: FLI error during put/get — FE may have destroyed channels (abnormal exit path)")
