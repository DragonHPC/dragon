#!/usr/bin/env python3
import os
import logging
import json

from unittest.mock import MagicMock, patch

from dragon.channels import Channel, ChannelError
from dragon.infrastructure import facts as dfacts
from dragon.infrastructure import messages as dmsg
from dragon.infrastructure import process_desc
from dragon.infrastructure.node_desc import NodeDescriptor
from dragon.infrastructure.connection import Connection, ConnectionOptions
from dragon.infrastructure.parameters import this_process, POLICY_INFRASTRUCTURE
from dragon.infrastructure.messages import AbnormalTerminationError
from dragon.launcher.util import next_tag, get_with_blocking
from dragon.launcher.network_config import NetworkConfig
from dragon.localservices.local_svc import mk_inf_resources
from dragon.managed_memory import MemoryPool, DragonPoolError, DragonMemoryError
from dragon.utils import B64


def get_args_map(network_config, frontend_sdesc, host_id, ip_addrs):
    network_prefix = "^(eth|hsn)"

    args_map = {
        "ip_addrs": ip_addrs,  # it includes the port
        "host_ids": host_id,
        "frontend_sdesc": frontend_sdesc,
        "network_prefix": f"{network_prefix}",
        "transport_test": False
    }
    return args_map


def mock_start_localservices_wrapper(func):
    def wrapper(*args, **kwargs):
        def mocked_start_localservices(*args, **kwargs):
            return MagicMock(), stdin_w_fd, stdout_r_fd

        from dragon.infrastructure.util import NewlineStreamWrapper

        stdin_r_fd, stdin_w_fd = os.pipe()
        stdout_r_fd, stdout_w_fd = os.pipe()

        ls_stdin_r = NewlineStreamWrapper(
            os.fdopen(stdin_r_fd, "rb", buffering=0), read_intent=True, write_intent=False
        )
        ls_stdout_w = NewlineStreamWrapper(
            os.fdopen(stdout_w_fd, "wb", buffering=0), read_intent=False, write_intent=True
        )

        with patch(
            "dragon.launcher.backend.LauncherBackEnd._start_localservices",
            side_effect=mocked_start_localservices
        ) as mock_localservices:
            localservices_arg_tuple = (mock_localservices, ls_stdin_r, ls_stdout_w)
            args += (localservices_arg_tuple,)
            result = func(*args, **kwargs)

        ls_stdout_w.close()
        del ls_stdout_w

        ls_stdin_r.close()
        del ls_stdin_r

        return result

    return wrapper


class LauncherBackendHelper:

    def __init__(self,
                 network_config_file,
                 node_index,
                 ip_addrs,
                 fe_host_id,
                 be_host_id):
        self.network_config_file = network_config_file
        self.network_config = \
            NetworkConfig.from_file(self.network_config_file).get_network_config()
        self.node_index = node_index
        self.ip_addrs = ip_addrs
        self.fe_host_id = fe_host_id
        self.be_host_id = be_host_id

        self.garbage_dict = {'junk': 'abdecdf', '52': 20}

    def cleanup(self):

        try:
            del self.localservices
        except Exception:
            pass

        try:
            del self.be_overlay_tsta
        except Exception:
            pass

        try:
            self.cleanup_ls()
        except Exception:
            pass

        try:
            del self.launcher_fe
        except Exception:
            pass


    def handle_overlay_start(self, mock_overlay, mock_start_localservices):
        '''Start of mocked up overlay network'''

        log = logging.getLogger('handle_overlay_start')
        log.info("Waiting for overlay call...")
        while mock_overlay.call_args is None:
            pass

        log.info("Got overlay call...")
        overlay_args = mock_overlay.call_args.kwargs

        # Create our backend overlay TCP agent and connect to the
        # frontend's channels
        self.be_overlay_tsta = BackendOverlay(
            overlay_args["ch_in_sdesc"], overlay_args["ch_out_sdesc"]
        )

        # Let backend know its overlay agent is "up"
        self.be_overlay_tsta.send_OverlayPingBE()  # M7

        # Establish 2-way communication with backend launcher
        self.launcher_fe.recv_BEIsUp()  # M8
        self.launcher_fe.connect_to_be()
        self.launcher_fe.send_FENodeIdxBE()  # M9

        self.be_overlay_tsta.recv_TAUpdateNodes()

        # Get generated LS stdin/stdout file descriptors. Turn them into newline stream wrappers
        _, self.ls_stdin_queue, self.ls_stdout_queue = mock_start_localservices

    def handle_network_and_frontend_start(self, mock_network_config):
        '''Define the mocked up network and frontend'''
        mock_network_config.return_value = self.network_config[str(self.node_index)]

        self.launcher_fe = LauncherFrontEnd(self.node_index, self.fe_host_id, self.network_config)

        args_map = get_args_map(
            self.network_config,
            self.launcher_fe.encoded_inbound,
            self.ip_addrs,
            self.be_host_id,
        )

        return args_map

    def start_ls(self,
                 mock_overlay,
                 mock_start_localservices,
                 garble_shchannelsup=False,
                 garble_lachannelsinfo=False,
                 abort_lachannelsinfo=False,
                 accelerator_present=False):

        self.handle_overlay_start(mock_overlay, mock_start_localservices)

        # create our localservices standin class and connect to stdin/stdout queues
        self.localservices = LocalServices(
            self.ls_stdin_queue, self.ls_stdout_queue, self.node_index, self.be_host_id
        )
        self.localservices.recv_BENodeIdxSH()  # M10
        self.localservices.send_SHPingBE()  # M11

        self.localservices.recv_BEPingSH()  # M12

        if garble_shchannelsup:
            self.localservices.send_SHChannelsUP(custom_msg=json.dumps(self.garbage_dict))
            return
        elif accelerator_present:
            self.localservices.send_SHChannelsUP(accelerator_present=accelerator_present)  # M13
        else:
            self.localservices.send_SHChannelsUP()  # M13

        # Launcher Backend spawns overlay TCP agent
        # Wait for BE Launcher to initialize its Overlay network
        # We need to grab the parameters passed to the overlay mock
        # FE -> BE Send LAChannelsInfo
        self.launcher_fe.recv_SHChannelsUp()  # M14

        if garble_lachannelsinfo:
            self.launcher_fe.send_LAChannelsInfo(custom_msg=json.dumps(self.garbage_dict))
            return
        elif abort_lachannelsinfo:
            self.launcher_fe.send_LAChannelsInfo(custom_msg=dmsg.AbnormalTermination(tag=next_tag()).serialize())
            return
        else:
            self.launcher_fe.send_LAChannelsInfo()  # M15

        self.localservices.recv_LAChannelsInfo()  # M16

    def clean_startup(self,
                      log,
                      mock_overlay,
                      mock_network_config,
                      mock_start_localservices,
                      accelerator_present=False):
        '''Execute a clean bringup and exit of all backend services'''

        self.start_ls(mock_overlay, mock_start_localservices, accelerator_present=accelerator_present)

        # A11 Local Services spawns TA

        self.localservices.send_TAUP()  # M20
        self.launcher_fe.recv_TAUP()  # M21

        # Local Services spawns Global Services
        self.globalservices = self.localservices.launch_globalservices()  # A13
        self.globalservices.send_GSIsUp()  # M25
        self.launcher_fe.recv_GSIsUp()  # M26

    def launch_user_process(self, log):
        p_uid = 23941
        self.launcher_fe.send_GSProcessCreate()
        create_process_tag = self.globalservices.recv_GSProcessCreate()

        self.globalservices.send_GSProcessCreateResponse(
            p_uid=p_uid, create_process_tag=create_process_tag
        )
        self.launcher_fe.recv_GSProcessCreateResponse()

        self.localservices.send_SHFwdOutput_StdOut(p_uid=p_uid)
        self.localservices.send_SHFwdOutput_StdErr(p_uid=p_uid)

        self.launcher_fe.recv_SHFwdOutput()
        self.launcher_fe.recv_SHFwdOutput()

    def start_gsteardown_exit(self,
                              shteardown_abort=False):

        self.launcher_fe.send_GSTeardown()  # M5
        self.globalservices.recv_GSTeardown()  # M6

        del self.globalservices
        self.globalservices = None

        self.localservices.send_GSHalted()  # M8
        self.launcher_fe.recv_GSHalted()  # M9

        self.launcher_fe.send_SHHaltTA()  # M10
        self.localservices.recv_SHHaltTA()  # M11

        self.localservices.send_TAHalted()  # M14
        self.launcher_fe.recv_TAHalted()  # M15

        if shteardown_abort:
            self.localservices.send_AbnormalTermination()

        self.launcher_fe.send_SHTeardown()  # M16
        self.localservices.recv_SHTeardown()  # M17

        self.localservices.send_SHHaltBE()  # M18
        self.launcher_fe.recv_SHHaltBE()  # M19

        self.launcher_fe.send_BEHalted()  # M20
        self.localservices.recv_BEHalted()  # M21

        self.be_overlay_tsta.recv_BEHaltOverlay()  # M22
        self.be_overlay_tsta.send_OverlayHalted()  # M23

    def start_gsteardown_exit_no_ls_no_gs(self,
                                          log,
                                          shteardown_abort=False):

        self.launcher_fe.send_GSTeardown()  # M5
        log.debug('sent gsteardown')

        self.launcher_fe.recv_GSHalted()  # M9
        log.debug('recved gshalted')

        self.launcher_fe.send_SHHaltTA()  # M10
        log.debug('send shhaltta')
        self.launcher_fe.recv_TAHalted()  # M15

        if shteardown_abort:
            self.launcher_fe.send_AbnormalTermination()
            return
        else:
            self.launcher_fe.send_SHTeardown()  # M16
        self.localservices.recv_SHTeardown()  # M17

        self.localservices.send_SHHaltBE()  # M18
        self.launcher_fe.recv_SHHaltBE()  # M19

        self.launcher_fe.send_BEHalted()  # M20
        self.localservices.recv_BEHalted()  # M21

        self.be_overlay_tsta.recv_BEHaltOverlay()  # M22
        self.be_overlay_tsta.send_OverlayHalted()  # M23

    def start_gsteardown_exit_no_gs(self,
                                    log,
                                    shteardown_abort=False):

        self.launcher_fe.send_GSTeardown()  # M5
        log.debug('sent gsteardown')

        self.launcher_fe.recv_GSHalted()  # M9
        log.debug('recved gshalted')

        self.launcher_fe.send_SHHaltTA()  # M10
        log.debug('send shhaltta')
        self.localservices.recv_SHHaltTA()  # M11

        self.localservices.send_TAHalted()  # M14
        self.launcher_fe.recv_TAHalted()  # M15

        if shteardown_abort:
            self.localservices.send_AbnormalTermination()
            return
        else:
            self.launcher_fe.send_SHTeardown()  # M16
        self.localservices.recv_SHTeardown()  # M17

        self.localservices.send_SHHaltBE()  # M18
        self.launcher_fe.recv_SHHaltBE()  # M19

        self.launcher_fe.send_BEHalted()  # M20
        self.localservices.recv_BEHalted()  # M21

        self.be_overlay_tsta.recv_BEHaltOverlay()  # M22
        self.be_overlay_tsta.send_OverlayHalted()  # M23

    def start_abnormal_termination_exit(self,
                                        log,
                                        gs_up=True,
                                        ls_up=True,
                                        shteardown_abort=False):

        try:
            self.launcher_fe.recv_AbnormalTermination()
        except AbnormalTerminationError:
            pass

        if not gs_up and not ls_up:
            self.start_gsteardown_exit_no_ls_no_gs(log,
                                                   shteardown_abort=shteardown_abort)
        elif not gs_up:
            self.start_gsteardown_exit_no_gs(log,
                                             shteardown_abort=shteardown_abort)
        else:
            self.start_gsteardown_exit(shteardown_abort=shteardown_abort)

    def clean_shutdown(self,
                       log,
                       shteardown_abort=False):

        self.globalservices.send_GSHeadExit()  # M3
        self.launcher_fe.recv_GSHeadExit()  # M4
        self.start_gsteardown_exit(shteardown_abort=shteardown_abort)

    def cleanup_ls(self):

        self.ls_stdin_queue.stream.close()
        del self.ls_stdin_queue

        self.ls_stdout_queue.stream.close()
        del self.ls_stdout_queue


class LauncherFrontEnd:
    def __init__(self, node_index, host_id, network_config):
        self.log = logging.getLogger("_launcher_fe")
        self.node_index = node_index
        self.host_id = host_id
        self.network_config = network_config

        self.port = dfacts.DEFAULT_TRANSPORT_PORT
        self.num_gw_channels = dfacts.DRAGON_OVERLAY_DEFAULT_NUM_GW_CHANNELS_PER_NODE

        self.create_frontend_comms()

    def __del__(self):
        self.log.debug("__del__")

        try:
            self.conn_out.close()
            del self.conn_out
        except Exception:
            pass

        try:
            self.be_ch.detach()
            del self.be_ch
        except Exception:
            pass

        try:
            self.conn_in.close()
            del self.conn_in
        except Exception:
            pass

        try:
            self.fe_inbound.destroy()
            del self.fe_inbound
        except Exception:
            pass

        try:
            self.fe_mpool.destroy()
            del self.fe_mpool
        except Exception:
            pass

    def create_frontend_comms(self):
        self.log.debug("creating frontend memory pool and channels to talk to backend")
        conn_options = ConnectionOptions(min_block_size=2**16)

        self.fe_mpool = None
        self.fe_inbound = None
        self.conn_in = None

        try:
            # Create my memory pool
            self.fe_mpool = MemoryPool(
                                int(dfacts.DEFAULT_SINGLE_DEF_SEG_SZ),
                                f"{os.getuid()}_{os.getpid()}_{self.host_id}_{dfacts.DEFAULT_POOL_SUFFIX}",
                                dfacts.FE_OVERLAY_TRANSPORT_AGENT_MUID,
                                )

            # Create my receiving channel
            self.fe_cuid = dfacts.FE_CUID

            # Channel for launcher backend to talk to me
            self.fe_inbound = Channel(self.fe_mpool, self.fe_cuid)
            self.encoded_inbound = B64(self.fe_inbound.serialize())

            self.conn_in = Connection(
                inbound_initializer=self.fe_inbound,
                options=conn_options,
                policy=POLICY_INFRASTRUCTURE,
            )
        except (
            ChannelError,
            DragonPoolError,
            DragonMemoryError,
        ) as init_err:
            if self.conn_in:
                del self.conn_in
                self.conn_in = None

            if self.fe_inbound:
                del self.fe_inbound
                self.fe_inbound = None

            if self.fe_mpool:
                self.fe_mpool.destroy()

            raise RuntimeError(
                "Overlay transport resource creation failed"
            ) from init_err

    def recv_BEIsUp(self):
        self.log.debug("waiting for BEIsUp from Launcher BE")
        self.be_is_up = get_with_blocking(self.conn_in)
        assert isinstance(self.be_is_up, dmsg.BEIsUp), "expected BSIsUp"
        self.log.debug(f"recv_BEIsUp got {self.be_is_up=}")

    def connect_to_be(self):
        self.log.debug("connecting to be")
        self.be_sdesc = B64.from_str(self.be_is_up.be_ch_desc)
        self.be_ch = Channel.attach(self.be_sdesc.decode(), mem_pool=self.fe_mpool)
        be_conn_options = ConnectionOptions(
            default_pool=self.fe_mpool, min_block_size=2**16
        )
        self.conn_out = Connection(
            outbound_initializer=self.be_ch,
            options=be_conn_options,
            policy=POLICY_INFRASTRUCTURE,
        )
        self.log.debug("connected to be")

    def send_FENodeIdxBE(self):
        # Pack up all of our node descriptors for the backend:
        forwarding = {}
        for be_up in [self.be_is_up]:
            assert isinstance(be_up, dmsg.BEIsUp), 'la_fe received invalid be up'
            for key, node_desc in self.network_config.items():
                if str(be_up.host_id) == str(node_desc.host_id):
                    forwarding[key] = NodeDescriptor(host_id=int(node_desc.host_id),
                                                     ip_addrs=node_desc.ip_addrs,
                                                     overlay_cd=be_up.be_ch_desc)
                    break
        fe_node_idx = dmsg.FENodeIdxBE(
            tag=next_tag(),
            node_index=self.node_index,
            forward=forwarding,
            send_desc=self.encoded_inbound)
        self.conn_out.send(fe_node_idx.serialize())
        self.log.debug(f"send_FENodeIdxBE sent {fe_node_idx=}")

    def recv_SHChannelsUp(self):
        nnodes = 1
        self.log.debug("receiving SHChannelsUp messages")
        self.chs_up = [get_with_blocking(self.conn_in) for _ in range(nnodes)]
        self.log.debug(f"{self.chs_up=}")
        for ch_up in self.chs_up:
            assert isinstance(
                ch_up, dmsg.SHChannelsUp
            ), "la_fe received invalid channel up"

        self.log.debug("Looking for gs_cd")
        gs_cds = [ch_up.gs_cd for ch_up in self.chs_up if ch_up.gs_cd is not None]
        assert len(gs_cds) > 0, "could not find a gs_cd response"
        assert len(gs_cds) == 1, "more than one gs_cd response found"
        if len(gs_cds) == 0:
            msg = "The Global Services CD was not returned by any of the SHChannelsUp messages. Launcher Exiting."
            raise RuntimeError(msg)
        self.gs_cd = gs_cds[0]

        self.log.debug(f"recv_SHChannelsUp got {self.chs_up} with {self.gs_cd=}")

    def send_LAChannelsInfo(self, custom_msg: str = None):

        if custom_msg is None:
            nodes_desc = {ch_up.idx: ch_up.node_desc for ch_up in self.chs_up}
            la_ch_info = dmsg.LAChannelsInfo(tag=next_tag(),
                                             nodes_desc=nodes_desc,
                                             gs_cd=self.gs_cd,
                                             num_gw_channels=self.num_gw_channels,
                                             port=self.port,
                                             )
            # self.la_fe_stdout.send("A", la_ch_info.serialize())
            self.conn_out.send(la_ch_info.serialize())

            self.log.debug(f"send_LAChannelsInfo sent {la_ch_info=}")
            self.log.debug(f"{la_ch_info.nodes_desc=}")
            self.log.debug(f"{la_ch_info.gs_cd=}")
        else:
            self.conn_out.send(custom_msg)

    def recv_TAUP(self):
        ta_up = get_with_blocking(self.conn_in)
        assert isinstance(
            ta_up, dmsg.TAUp
        ), "la_fe received invalid channel up"
        self.log.debug(f"recv_TAUP got {ta_up=}")

    def recv_GSIsUp(self):
        gs_is_up = get_with_blocking(self.conn_in)
        assert isinstance(gs_is_up, dmsg.GSIsUp), "expected GSIsUP"
        self.log.debug(f"recv_GSIsUp got {gs_is_up=}")

    def send_GSProcessCreate(self):
        exe = "hello_world.py"
        args = ["--language", "espanol"]
        options = process_desc.ProcessOptions(make_inf_channels=True)
        gs_process_create = dmsg.GSProcessCreate(
            tag=next_tag(),
            p_uid=dfacts.LAUNCHER_PUID,
            r_c_uid=dfacts.BASE_BE_CUID,
            exe=exe,
            args=args,
            options=options,
        )
        self.conn_out.send(gs_process_create.serialize())
        self.log.debug(f"send_GSProcessCreate sent {gs_process_create=}")

    def recv_GSProcessCreateResponse(self):
        gs_process_create_response = get_with_blocking(self.conn_in)
        assert isinstance(
            gs_process_create_response, dmsg.GSProcessCreateResponse
        ), "expected GSIsUP"
        self.log.debug(f"recv_GSProcessCreateResponse got {gs_process_create_response=}")

    def recv_SHFwdOutput(self):
        sh_fwd_output = get_with_blocking(self.conn_in)
        assert isinstance(sh_fwd_output, dmsg.SHFwdOutput), "expected SHFwdOutput"
        self.log.debug(f"recv_SHFwdOutput got {sh_fwd_output=}")

    def recv_GSHeadExit(self):  # M4
        gs_head_exit = get_with_blocking(self.conn_in)
        assert isinstance(gs_head_exit, dmsg.GSHeadExit), "expected GSHeadExit"
        self.log.debug(f"recv_GSHeadExit got {gs_head_exit=}")

    def send_GSTeardown(self):  # M5
        gs_teardown = dmsg.GSTeardown(tag=next_tag())
        self.conn_out.send(gs_teardown.serialize())
        self.log.debug(f"send_GSTeardown sent {gs_teardown=}")

    def recv_GSHalted(self):  # M9
        gs_halted = get_with_blocking(self.conn_in)
        assert isinstance(gs_halted, dmsg.GSHalted), "expected GSHalted"
        self.log.debug(f"recv_GSHalted got {gs_halted=}")

    def send_SHHaltTA(self):  # M10
        sh_halt_ta = dmsg.SHHaltTA(tag=next_tag())
        self.conn_out.send(sh_halt_ta.serialize())
        self.log.debug(f"send_SHHaltTA sent {sh_halt_ta=}")

    def recv_TAHalted(self):  # M15
        ta_halted = get_with_blocking(self.conn_in)
        assert isinstance(ta_halted, dmsg.TAHalted), "expected TAHalted"
        self.log.debug(f"recv_TAHalted got {ta_halted=}")

    def send_SHTeardown(self):  # M16
        sh_teardown = dmsg.SHTeardown(tag=next_tag())
        self.conn_out.send(sh_teardown.serialize())
        self.log.debug(f"send_SHTeardown sent {sh_teardown=}")

    def recv_SHHaltBE(self):  # M19
        sh_halt_be = get_with_blocking(self.conn_in)
        assert isinstance(sh_halt_be, dmsg.SHHaltBE), "expected sh_halt_be"
        self.log.debug(f"recv_SHHaltBE got {sh_halt_be=}")

    def send_BEHalted(self):  # M20
        be_halted = dmsg.BEHalted(tag=next_tag())
        self.conn_out.send(be_halted.serialize())
        self.log.debug(f"send_BEHalted sent {be_halted=}")

    def recv_AbnormalTermination(self):  # M5
        abnormal_term = get_with_blocking(self.conn_in)
        self.log.debug(f"recv_AbnormalTermination got {abnormal_term=}")


class BackendOverlay:
    def __init__(self, ch_in_desc: B64, ch_out_desc: B64):
        self.log = logging.getLogger("_be_overlay_tsta")

        try:
            self.ta_ch_in = Channel.attach(ch_in_desc.decode())
            self.ta_ch_out = Channel.attach(ch_out_desc.decode())
            self.be_ta_conn = Connection(
                inbound_initializer=self.ta_ch_in,
                outbound_initializer=self.ta_ch_out,
                options=ConnectionOptions(
                    creation_policy=ConnectionOptions.CreationPolicy.EXTERNALLY_MANAGED
                ),
            )
            self.be_ta_conn.ghost = True
            self.be_ta_conn.open()

        except (ChannelError, DragonPoolError, DragonMemoryError) as init_err:
            raise RuntimeError(
                "Overlay transport resource creation failed"
            ) from init_err

    def __del__(self):
        self.log.debug("__del__")

        try:
            self.be_ta_conn.close()
            del self.be_ta_conn
        except Exception:
            pass

        try:
            self.ta_ch_out.detach()
            del self.ta_ch_out
        except Exception:
            pass

        try:
            self.ta_ch_in.detach()
            del self.ta_ch_in
        except Exception:
            pass

    def send_OverlayPingBE(self):
        overlay_ping_be = dmsg.OverlayPingBE(next_tag())
        self.be_ta_conn.send(overlay_ping_be.serialize())
        self.log.debug(f"send_OverlayPingBE sent {overlay_ping_be=}")

    def recv_TAUpdateNodes(self):
        be_hsta_update_nodes = get_with_blocking(self.be_ta_conn)
        assert isinstance(be_hsta_update_nodes, dmsg.TAUpdateNodes), "expected TAUpdateNodes"
        self.log.debug(f"recv_TAUpdateNodes got {be_hsta_update_nodes=}")

    def recv_BEHaltOverlay(self):  # M22
        be_halt_overlay = get_with_blocking(self.be_ta_conn)
        assert isinstance(be_halt_overlay, dmsg.BEHaltOverlay), "expected BEHaltOverlay"
        self.log.debug(f"recv_SHHaltTA got {be_halt_overlay=}")

    def send_OverlayHalted(self):  # M23
        overlay_halted = dmsg.OverlayHalted(next_tag())
        self.be_ta_conn.send(overlay_halted.serialize())
        self.log.debug(f"send_OverlayHalted sent {overlay_halted}")


class LocalServices:
    def __init__(self, ls_stdin_queue, ls_stdout_queue, node_index, host_id):
        self.log = logging.getLogger("_localservices")
        self.ls_stdin_queue = ls_stdin_queue
        self.ls_stdout_queue = ls_stdout_queue
        self.node_index = node_index
        self.host_id = host_id

        # Make LS Infrastructure channels
        (
            self.start_pools,
            self.start_channels,
            self.ls_input,
            self.la_input,
            self.ta_input_descr,
            self.ta_input,
            self.gs_input,
        ) = mk_inf_resources(node_index)

        self.gs_cd = this_process.gs_cd

    def __del__(self):
        self.log.debug("__del__")

        try:
            self.log.debug("gs_input destroy")
            self.gs_input.destroy()
            del self.gs_input
        except Exception:
            pass

        try:
            self.log.debug("ta_input close")
            self.ta_input.close()
            del self.ta_input
        except Exception:
            pass

        try:
            self.log.debug("la_input close")
            self.la_input.close()
            del self.la_input
        except Exception:
            pass

        try:
            self.log.debug("ls_input close")
            self.ls_input.close()
            del self.ls_input
        except Exception:
            pass

        try:
            self.log.debug("start_channels destroy")
            for key, chan in self.start_channels.items():
                try:
                    self.log.debug(f'destroy channel {key}')
                    chan.destroy()
                except Exception:
                    pass
            del self.start_channels
        except Exception:
            pass

        try:
            self.log.debug("start_pools destroy")
            for pool in self.start_pools.values():
                try:
                    pool.destroy()
                except Exception:
                    pass
            del self.start_pools
        except AttributeError:
            pass

    def send_SHPingBE(self):
        this_process.index = self.node_index
        be_ping = dmsg.SHPingBE(
            tag=next_tag(),
            shep_cd=this_process.local_shep_cd,
            be_cd=this_process.local_be_cd,
            gs_cd=this_process.gs_cd,
            default_pd=this_process.default_pd,
            inf_pd=this_process.inf_pd,
        )
        self.ls_stdout_queue.send(be_ping.serialize())
        self.log.debug(f"send_SHPingBE sent {be_ping=}")

    def recv_BENodeIdxSH(self):
        self.be_node_idx_sh = dmsg.parse(self.ls_stdin_queue.recv())
        assert isinstance(self.be_node_idx_sh, dmsg.BENodeIdxSH), "expected BENodeIdxSH"
        self.log.debug(f"recv_BENodeIdxSH got {self.be_node_idx_sh=}")

    @patch("dragon.infrastructure.gpu_desc.find_nvidia")
    def send_SHChannelsUP(self,
                          mock_find_nvidia,
                          custom_msg=None,
                          accelerator_present=False):

        if custom_msg:
            self.la_input.send(custom_msg)
        else:
            mock_find_nvidia.return_value = None
            if accelerator_present:
                mock_find_nvidia.return_value = (0, 1, 2, 3)

            node_desc = NodeDescriptor.get_localservices_node_conf(host_name=self.be_node_idx_sh.host_name,
                                                                   name=self.be_node_idx_sh.host_name,
                                                                   host_id=self.host_id,
                                                                   ip_addrs=self.be_node_idx_sh.ip_addrs,
                                                                   shep_cd=this_process.local_shep_cd)
            ch_up_msg = dmsg.SHChannelsUp(tag=next_tag(),
                                          node_desc=node_desc,
                                          gs_cd=self.gs_cd,
                                          idx=self.be_node_idx_sh.node_idx)

            self.la_input.send(ch_up_msg.serialize())
            self.log.debug(f"send_SHChannelsUP sent {ch_up_msg=}")

    def recv_BEPingSH(self):
        be_ping_sh = dmsg.parse(self.ls_input.recv())
        assert isinstance(be_ping_sh, dmsg.BEPingSH), "expected BSPingSH"
        self.log.debug(f"recv_BEPingSH got {be_ping_sh=}")

    def recv_LAChannelsInfo(self):
        # Recv LAChannelsInfo Broadcast
        la_channels_info = dmsg.parse(self.ls_input.recv())
        assert isinstance(
            la_channels_info, dmsg.LAChannelsInfo
        ), "expected LAChannelsInfo"
        self.gs_cd = la_channels_info.gs_cd
        self.log.debug(f"recv_LAChannelsInfo got {la_channels_info=}")
        self.log.debug(f"{la_channels_info.nodes_desc=}")
        self.log.debug(f"{la_channels_info.gs_cd=}")

    def send_TAUP(self):
        ch_list = []
        ta_up = dmsg.TAUp(
            tag=next_tag(), idx=self.node_index, test_channels=ch_list
        )
        self.la_input.send(ta_up.serialize())
        self.log.debug(f"send_TAUP sent {ta_up=}")

    def launch_globalservices(self):
        return GlobalServices(self.gs_cd, self.la_input)

    def send_SHFwdOutput_StdOut(self, p_uid):
        sh_fwd_output = dmsg.SHFwdOutput(
            tag=next_tag(),
            p_uid=p_uid,
            idx=self.node_index,
            fd_num=dmsg.SHFwdOutput.FDNum.STDOUT.value,
            data="Hola! This is STDOUT",
        )
        self.la_input.send(sh_fwd_output.serialize())
        self.log.debug(f"send_SHFwdOutput_StdErr sent via STDOUT {sh_fwd_output=}")

    def send_SHFwdOutput_StdErr(self, p_uid):
        sh_fwd_output = dmsg.SHFwdOutput(
            tag=next_tag(),
            p_uid=p_uid,
            idx=self.node_index,
            fd_num=dmsg.SHFwdOutput.FDNum.STDERR.value,
            data="Hola! This is STDERR",
        )
        self.la_input.send(sh_fwd_output.serialize())
        self.log.debug(f"send_SHFwdOutput_StdErr sent via STDERR {sh_fwd_output=}")

    def send_GSHalted(self):  # M8
        gs_halted = dmsg.GSHalted(tag=next_tag())
        self.la_input.send(gs_halted.serialize())
        self.log.debug(f"send_GSHalted sent {gs_halted=}")

    def send_AbnormalTermination(self):
        abnormal_term = dmsg.AbnormalTermination(tag=next_tag())
        self.la_input.send(abnormal_term.serialize())
        self.log.debug(f"send_AbnormalTermination sent {abnormal_term=}")

    def recv_SHHaltTA(self):  # M11
        sh_halt_ta = dmsg.parse(self.ls_input.recv())
        assert isinstance(sh_halt_ta, dmsg.SHHaltTA), "expected SHHaltTA"
        self.log.debug(f"recv_SHHaltTA got {sh_halt_ta=}")

    def send_TAHalted(self):  # M14
        ta_halted = dmsg.TAHalted(tag=next_tag())
        self.la_input.send(ta_halted.serialize())
        self.log.debug(f"send_TAHalted sent {ta_halted=}")

    def recv_SHTeardown(self):  # M17
        sh_teardown = dmsg.parse(self.ls_input.recv())
        assert isinstance(sh_teardown, dmsg.SHTeardown), "expected SHTeardown"
        self.log.debug(f"recv_SHTeardown got {sh_teardown=}")

    def send_SHHaltBE(self):  # M18
        sh_halt_be = dmsg.SHHaltBE(tag=next_tag())
        self.la_input.send(sh_halt_be.serialize())
        self.log.debug(f"send_SHHaltBE sent {sh_halt_be=}")

    def recv_BEHalted(self):
        be_halted = dmsg.parse(self.ls_stdin_queue.recv())
        assert isinstance(be_halted, dmsg.BEHalted), "expected BEHalted"
        self.log.debug(f"recv_BEHalted got {be_halted=}")


class GlobalServices:
    def __init__(self, gs_cd, la_input):
        self.log = logging.getLogger("_globalservices")
        self.gs_cd = gs_cd
        self.la_input = la_input
        self.la_input.ghost = True

        self.gs_ch_in = Channel.attach(B64.from_str(gs_cd).decode())
        self.gs_queue = Connection(
            inbound_initializer=self.gs_ch_in, policy=POLICY_INFRASTRUCTURE
        )

    def __del__(self):
        self.log.debug("__del__")

        try:
            self.gs_queue.close()
            del self.gs_queue
        except Exception:
            pass

        try:
            self.gs_ch_in.detach()
            del self.gs_ch_in
        except Exception:
            pass

        del self.la_input

    def send_GSIsUp(self):
        gs_is_up = dmsg.GSIsUp(tag=next_tag())
        self.la_input.send(gs_is_up.serialize())
        self.log.debug(f"send_GSIsUp sent {gs_is_up=}")

    def recv_GSProcessCreate(self):
        gs_process_create = get_with_blocking(self.gs_queue)
        assert isinstance(
            gs_process_create, dmsg.GSProcessCreate
        ), "expected GSProcessCreate"
        self.log.debug(f"recv_GSProcessCreate got {gs_process_create=}")
        return gs_process_create.tag

    def send_GSProcessCreateResponse(self, p_uid, create_process_tag):
        gs_process_create_response = dmsg.GSProcessCreateResponse(
            tag=next_tag(),
            ref=create_process_tag,
            err=dmsg.GSProcessCreateResponse.Errors.SUCCESS,
            desc=process_desc.ProcessDescriptor(
                p_uid=p_uid, p_p_uid=47622, name="hello_world", node=0
            ),
        )
        self.la_input.send(gs_process_create_response.serialize())
        self.log.debug(f"send_GSProcessCreateResponse sent {gs_process_create_response=}")

    def send_GSHeadExit(self):
        gs_head_exit = dmsg.GSHeadExit(tag=next_tag(), exit_code=0)
        self.la_input.send(gs_head_exit.serialize())
        self.log.debug(f"send_GSHeadExit sent {gs_head_exit=}")

    def recv_GSTeardown(self):
        gs_teardown = get_with_blocking(self.gs_queue)
        assert isinstance(gs_teardown, dmsg.GSTeardown), "expected GSTeardown"
        self.log.debug(f"recv_GSTeardown got {gs_teardown=}")
