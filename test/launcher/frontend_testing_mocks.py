import os
import logging

from dragon.launcher.frontend import LauncherFrontEnd
from dragon.launcher.util import next_tag
from dragon.launcher.network_config import NetworkConfig

from dragon.infrastructure.process_desc import ProcessDescriptor
from dragon.infrastructure.connection import Connection, ConnectionOptions
from dragon.infrastructure.node_desc import NodeDescriptor
from dragon.infrastructure.parameters import POLICY_INFRASTRUCTURE
from dragon.infrastructure import facts as dfacts
from dragon.infrastructure import messages as dmsg

from dragon.channels import Channel, ChannelError
from dragon.managed_memory import MemoryPool, DragonPoolError, DragonMemoryError
from dragon.utils import set_host_id, B64
from dragon.dlogging.util import setup_FE_logging


def run_frontend(args_map):

    # Before doing anything set my host ID
    set_host_id(dfacts.FRONTEND_HOSTID)
    setup_FE_logging(log_device_level_map=args_map['log_device_level_map'],
                     basename='dragon', basedir=os.getcwd())
    log = logging.getLogger('run_frontend')
    for key, value in args_map.items():
        if value is not None:
            log.info(f'args_map: {key}: {value}')

    with LauncherFrontEnd(args_map=args_map) as fe_server:
        fe_server.run_startup()
        fe_server.run_app()
        fe_server.run_msg_server()


def open_overlay_comms(ch_in_desc: B64,
                       ch_out_desc: B64):
    '''Attach to Frontend's overlay network channels and open a connection'''
    try:
        ta_ch_in = Channel.attach(ch_in_desc.decode())
        ta_ch_out = Channel.attach(ch_out_desc.decode())
        fe_ta_conn = Connection(inbound_initializer=ta_ch_in,
                                outbound_initializer=ta_ch_out,
                                options=ConnectionOptions(creation_policy=ConnectionOptions.CreationPolicy.EXTERNALLY_MANAGED))
        fe_ta_conn.ghost = True
        fe_ta_conn.open()

    except (ChannelError, DragonPoolError, DragonMemoryError) as init_err:
        raise RuntimeError('Overlay transport resource creation failed') from init_err

    return ta_ch_in, ta_ch_out, fe_ta_conn


def open_backend_comms(frontend_sdesc: str,
                       network_config: str):
    '''Attach to frontend channel and create channels for backend'''
    be_mpool = None
    try:
        conn_options = ConnectionOptions(min_block_size=2 ** 16)
        conn_policy = POLICY_INFRASTRUCTURE

        be_mpool = MemoryPool(int(dfacts.DEFAULT_BE_OVERLAY_TRANSPORT_SEG_SZ),
                              f'{os.getuid()}_{os.getpid()}_{2}' + dfacts.DEFAULT_POOL_SUFFIX,
                              dfacts.be_pool_muid_from_hostid(2))

        be_ch_out = Channel.attach(B64.from_str(frontend_sdesc).decode(),
                                   mem_pool=be_mpool)

        net = NetworkConfig.from_file(network_config)
        net_conf = net.get_network_config()

        be_nodes = {}
        for node in net_conf.values():
            be_cuid = dfacts.be_fe_cuid_from_hostid(node.host_id)

            # Add this to the object as it lets teardown know it needs to clean this up
            be_ch_in = Channel(be_mpool, be_cuid)

            overlay_inout = Connection(inbound_initializer=be_ch_in,
                                     outbound_initializer=be_ch_out,
                                     options=conn_options,
                                     policy=conn_policy)
            overlay_inout.ghost = True
            be_nodes[node.host_id] = {'conn': overlay_inout,
                                      'ch_in': be_ch_in,
                                      'hostname': node.name,
                                      'ip_addrs': node.ip_addrs}

    except (ChannelError, DragonPoolError, DragonMemoryError) as init_err:
        # Try to clean up the pool
        if be_mpool is not None:
            be_mpool.destroy()
        raise RuntimeError('Overlay transport resource creation failed') from init_err

    return be_mpool, be_ch_out, be_ch_in, be_nodes, overlay_inout


def send_beisup(nodes):
    '''Send valid beisup messages'''

    for host_id, node in nodes.items():
        be_up_msg = dmsg.BEIsUp(tag=next_tag(),
                                be_ch_desc=str(B64(node['ch_in'].serialize())),
                                host_id=host_id)
        node['conn'].send(be_up_msg.serialize())


def recv_fenodeidx(nodes):
    '''recv FENoe4deIdxBE and finish filling out node dictionary'''
    log = logging.getLogger('recv_fe_nodeidx')
    for node in nodes.values():
        fe_node_idx_msg = dmsg.parse(node['conn'].recv())
        assert isinstance(fe_node_idx_msg, dmsg.FENodeIdxBE), 'la_be node_index from fe expected'

        log.info(f'got FENodeIdxBE for index {fe_node_idx_msg.node_index}')
        node['node_index'] = fe_node_idx_msg.node_index
        if node['node_index'] < 0:
            raise RuntimeError("frontend giving bad node indices")
        node['is_primary'] = node['node_index'] == 0
        if node['is_primary']:
            primary_conn = node['conn']
        log.info(f'constructed be node: {node}')

    return primary_conn


def send_shchannelsup(nodes, mpool):

    log = logging.getLogger('send_shchannelsup')
    for host_id, node in nodes.items():
        ls_cuid = dfacts.shepherd_cuid_from_index(node['node_index'])
        ls_ch = Channel(mpool, ls_cuid)
        node['ls_ch'] = ls_ch

        if node['is_primary']:
            node['gs_ch'] = Channel(mpool, dfacts.GS_INPUT_CUID)
            gs_cd = B64.bytes_to_str(node['gs_ch'].serialize())
        else:
            node['gs_ch'] = None
            gs_cd = None
        node_desc = NodeDescriptor(host_name=node['hostname'],
                                   host_id=host_id,
                                   ip_addrs=node['ip_addrs'],
                                   shep_cd=B64.bytes_to_str(node['ls_ch'].serialize()))
        ch_up_msg = dmsg.SHChannelsUp(tag=next_tag(),
                                      node_desc=node_desc,
                                      gs_cd=gs_cd,
                                      idx=node['node_index'])
        log.info(f'construct SHChannelsUp: {ch_up_msg}')
        node['conn'].send(ch_up_msg.serialize())
        log.info(f'sent SHChannelsUp for {node["node_index"]}')

    log.info('sent all SHChannelsUp')


def recv_lachannelsinfo(nodes):
    '''Loop to recv all LAChannelsInfo messages in frontend bcast'''
    for host_id, node in nodes.items():
        la_channels_info_msg = dmsg.parse(node['conn'].recv())
        assert isinstance(la_channels_info_msg, dmsg.LAChannelsInfo), 'la_be expected all ls channels info from la_fe'
    return la_channels_info_msg


def send_taup(nodes):
    for host_id, node in nodes.items():
        ta_up = dmsg.TAUp(tag=next_tag(), idx=node['node_index'])
        node['conn'].send(ta_up.serialize())


def send_abnormal_term(conn):
    abnorm = dmsg.AbnormalTermination(tag=next_tag())
    conn.send(abnorm.serialize())


def handle_gsprocesscreate(primary_conn):
    '''Manage a valid response to GSProcessCreate'''

    log = logging.getLogger('handle_gsprocesscreate')
    proc_create = dmsg.parse(primary_conn.recv())
    log.info('presumably got GSProcessCreate')
    assert isinstance(proc_create, dmsg.GSProcessCreate)
    log.info('recvd GSProccessCreate')

    # Send response
    gs_desc = ProcessDescriptor(p_uid=5000,  # Just a dummy value
                                name=proc_create.user_name,
                                node=0,
                                p_p_uid=proc_create.p_uid)
    response = dmsg.GSProcessCreateResponse(tag=next_tag(),
                                            ref=proc_create.tag,
                                            err=dmsg.GSProcessCreateResponse.Errors.SUCCESS,
                                            desc=gs_desc)
    primary_conn.send(response.serialize())


def stand_up_backend(mock_overlay, mock_launch, network_config):

    log = logging.getLogger('mock_backend_standup')
    # Get the mock's input args to the
    while mock_overlay.call_args is None:
        pass
    overlay_args = mock_overlay.call_args.kwargs

    overlay = {}

    # Connect to overlay comms to talk to fronteend
    overlay['ta_ch_in'], overlay['ta_ch_out'], overlay['fe_ta_conn'] = open_overlay_comms(overlay_args['ch_in_sdesc'],
                                                                                    overlay_args['ch_out_sdesc'])

    # Let frontend know the overlay is "up"
    overlay['fe_ta_conn'].send(dmsg.OverlayPingLA(next_tag()).serialize())

    # Grab the frontend channel descriptor for the launched backend and
    # send it mine
    while mock_launch.call_args is None:
        pass
    launch_be_args = mock_launch.call_args.kwargs
    log.info(f'got be args: {launch_be_args}')

    # Connect to backend comms for frontend-to-backend and back comms
    overlay['be_mpool'], overlay['be_ch_out'], overlay['be_ch_in'], overlay['be_nodes'], overlay['overlay_inout'] = open_backend_comms(launch_be_args['frontend_sdesc'],
                                                                                                                           network_config)
    log.info('got backend up')

    return overlay


def handle_bringup(mock_overlay, mock_launch, network_config):

    log = logging.getLogger('mock_fulL_bringup')

    overlay = stand_up_backend(mock_overlay, mock_launch, network_config)

    # Send BEIsUp
    send_beisup(overlay['be_nodes'])
    log.info('send BEIsUp messages')

    # Recv FENodeIdxBE
    overlay['primary_conn'] = recv_fenodeidx(overlay['be_nodes'])
    log.info('got all the FENodeIdxBE messages')

    # Fudge some SHChannelsUp messages
    send_shchannelsup(overlay['be_nodes'], overlay['be_mpool'])
    log.info(f'sent shchannelsup: {[node["gs_ch"].serialize() for node in overlay["be_nodes"].values() if node["gs_ch"] is not None]}')

    # Receive LAChannelsInfo
    la_info = recv_lachannelsinfo(overlay['be_nodes'])
    log.info('la_be received LAChannelsInfo')

    # Send TAUp
    send_taup(overlay['be_nodes'])
    log.info('sent TAUp messages')

    # Send gs is up from primary
    overlay['primary_conn'].send(dmsg.GSIsUp(tag=next_tag()).serialize())
    log.info('send GSIsUp')

    return overlay, la_info


def handle_overlay_teardown(overlay_conn):
    '''complete teardown of frontend overlay process'''
    halt_on = dmsg.parse(overlay_conn.recv())
    assert isinstance(halt_on, dmsg.LAHaltOverlay)
    overlay_conn.send(dmsg.OverlayHalted(tag=next_tag()).serialize())


def handle_teardown(nodes, primary_conn, overlay_conn,
                    timeout_backend=False,
                    timeout_overlay=False,
                    gs_head_exit=True,
                    abort_shteardown=None):
    '''Do full teardown from perspective of backend'''

    if gs_head_exit:
        primary_conn.send(dmsg.GSHeadExit(exit_code=0, tag=next_tag()).serialize())

    # Recv GSTeardown
    gs_teardown = dmsg.parse(primary_conn.recv())
    assert isinstance(gs_teardown, dmsg.GSTeardown)
    log = logging.getLogger('handle_teardown')
    log.debug('got gstteardown')

    primary_conn.send(dmsg.GSHalted(tag=next_tag()).serialize())
    log.debug('sent gshalted')

    # Recv SHHaltTA for everyone and send back TAHalted
    for node in nodes.values():
        ta_halt_msg = dmsg.parse(node['conn'].recv())
        assert isinstance(ta_halt_msg, dmsg.SHHaltTA), 'SHHaltTA from fe expected'
        node['conn'].send(dmsg.TAHalted(tag=next_tag()).serialize())
    log.debug('received all SHHaltTA from frontend')

    if timeout_backend:
        log.info('returning early during teardown to test frontend teardown timeout')
        return

    # Recv SHTeardown and send shhaltbe
    for index, node in enumerate(nodes.values()):
        sh_teardown_msg = dmsg.parse(node['conn'].recv())
        assert isinstance(sh_teardown_msg, dmsg.SHTeardown), 'SHTeardown from fe expected'
        if abort_shteardown is not None and index == abort_shteardown:
            node['conn'].send(dmsg.AbnormalTermination(tag=next_tag()).serialize())
        node['conn'].send(dmsg.SHHaltBE(tag=next_tag()).serialize())

    # Recv BEHalted
    for node in nodes.values():
        be_halted_msg = dmsg.parse(node['conn'].recv())
        assert isinstance(be_halted_msg, dmsg.BEHalted), 'BEHalted from fe expected'

    # Recv FE's LAHaltOverlay for overlay and then tell it we've shut down our mocked up overlay tree
    if not timeout_overlay:
        handle_overlay_teardown(overlay_conn)
