#!/usr/bin/env python3
import os
import logging
import unittest
from unittest.mock import patch
import threading
from time import time

from dragon.launcher.launch_selector import determine_environment
from dragon.launcher.frontend import LauncherFrontEnd
from dragon.launcher.util import next_tag, SRQueue, get_with_timeout
from dragon.launcher.launchargs import get_args
from dragon.launcher.wlm import SlurmWLM

from dragon.infrastructure.process_desc import ProcessDescriptor
from dragon.infrastructure.node_desc import NodeDescriptor
from dragon.infrastructure import facts as dfacts
from dragon.infrastructure import messages as dmsg
import dragon.utils as du

from dragon.managed_memory import MemoryPool, DragonPoolError, DragonMemoryError
from dragon.channels import Channel, ChannelError
from dragon.utils import B64
from dragon.dlogging.util import setup_FE_logging

from .frontend_testing_mocks import (
    ensure_frontend_host_id,
    open_overlay_comms,
    open_backend_comms,
    send_beisup,
    recv_fenodeidx,
    send_shchannelsup,
    recv_lachannelsinfo,
    send_taup,
    handle_overlay_teardown,
)


def handle_gsprocesscreate(fe_in_q, proc_create):
    """Send GSProcessCreate response and head exit."""
    gs_desc = ProcessDescriptor(
        p_uid=5000, name=proc_create.user_name, node=0, p_p_uid=proc_create.p_uid  # Just a dummy value
    )
    response = dmsg.GSProcessCreateResponse(
        tag=next_tag(), ref=proc_create.tag, err=dmsg.GSProcessCreateResponse.Errors.SUCCESS, desc=gs_desc
    )
    fe_in_q.put(response)
    fe_in_q.put(dmsg.GSHeadExit(exit_code=0, tag=next_tag()))


def get_args_map(network_config, from_wlm=False):

    if from_wlm:
        arg_list = ["-t", "tcp", "launcher/helloworld.py"]
    else:
        arg_list = ["--wlm", "slurm", "--network-config", f"{network_config}", "--network-prefix", "", "helloworld.py"]
    args_map = get_args(arg_list)
    return args_map


def cleanup_mocks(overlay_in_q, overlay_out_q, fe_in_q, be_nodes, be_mpool):
    for q in [overlay_in_q, overlay_out_q, fe_in_q]:
        try:
            if q is not None:
                q.close()
        except Exception:
            pass

    try:
        for node in (be_nodes or {}).values():
            try:
                node["be_in_q"].close()
            except Exception:
                pass
            try:
                node["be_ch_in"].destroy()
            except Exception:
                pass
            try:
                node["ls_ch"].destroy()
            except Exception:
                pass
            try:
                if node.get("gs_ch") is not None:
                    node["gs_ch"].destroy()
            except Exception:
                pass
    except Exception:
        pass

    try:
        if be_mpool is not None:
            be_mpool.destroy()
    except Exception:
        pass


def run_frontend_supporting_mocks(
    mock_overlay=None,
    mock_launch=None,
    args_map=None,
    overlay_only=False,
    no_proc_create=False,
    hang_backend=False,
    hang_overlay=False,
    exit_at_fenodeidx=False,
    exit_queue=None,
):
    log = logging.getLogger("frontend mocks")

    be_mpool = None
    fe_in_q = None
    be_nodes = None
    overlay_in_q = None
    overlay_out_q = None
    primary_be_in_q = None

    log.info("inside frontend mocks")
    try:
        while mock_overlay.call_args is None:
            pass
        log.info("getting mock overlay args")
        overlay_args = mock_overlay.call_args.kwargs

        # Create be_mpool first so FLI uses be_mpool, not fe_mpool
        be_mpool = MemoryPool(
            int(dfacts.DEFAULT_BE_OVERLAY_TRANSPORT_SEG_SZ),
            f"{os.getuid()}_{os.getpid()}_{2}" + dfacts.DEFAULT_POOL_SUFFIX,
            dfacts.be_pool_muid_from_hostid(2),
        )

        # Connect to overlay comms to talk to frontend
        log.info("opening overlay comms")
        overlay_in_q, overlay_out_q = open_overlay_comms(
            overlay_args["ch_in_sdesc"], overlay_args["ch_out_sdesc"], mpool=be_mpool
        )

        # Let frontend know the overlay is "up"
        log.info("sending OverlayPingLA")
        overlay_out_q.put(dmsg.OverlayPingLA(next_tag()))

        if overlay_only:
            halt_on = overlay_in_q.get(timeout=10.0)
            assert isinstance(halt_on, dmsg.LAHaltOverlay)
            overlay_out_q.put(dmsg.OverlayHalted(tag=next_tag()))
            raise RuntimeError("exiting due to overlay_only being True")

        # Grab the frontend channel descriptor for the launched backend and
        # send it mine
        while mock_launch.call_args is None:
            pass
        launch_be_args = mock_launch.call_args.kwargs
        log.info(f"got be args: {launch_be_args}")

        mock_launch.wait.return_value = None
        log.info("set srun launch wait to None")

        # Connect to backend comms; pass already-created be_mpool
        be_mpool, fe_in_q, be_nodes = open_backend_comms(
            launch_be_args["frontend_sdesc"], args_map["network_config"], be_mpool=be_mpool
        )
        log.info("got backend up")

        # Send BEIsUp
        send_beisup(be_nodes)
        log.info("send BEIsUp messages")

        # Recv FENodeIdxBE
        if not exit_at_fenodeidx:
            primary_be_in_q = recv_fenodeidx(be_nodes)
            log.info("got all the FENodeIdxBE messages")
        else:
            log.info("told to return at FENodeIdx recv")
            cleanup_mocks(overlay_in_q, overlay_out_q, fe_in_q, be_nodes, be_mpool)
            return

        # Send LSChannelsUp messages
        send_shchannelsup(be_nodes, be_mpool)
        log.info("sent shchannelsup")

        # Receive LAChannelsInfo
        recv_lachannelsinfo(be_nodes)
        log.info("la_be received LAChannelsInfo")

        # Send TAUp
        send_taup(be_nodes)
        log.info("sent TAUp messages")

        # Send gs is up from primary
        fe_in_q.put(dmsg.GSIsUp(tag=next_tag()))
        log.info("send GSIsUp")

        while True:
            gs_msg = None
            try:
                log.debug("doing gs get")
                gs_msg = primary_be_in_q.get(timeout=0.01)
            except Exception:
                log.debug("gs timeout")
                pass

            try:
                log.debug("doing exit get")
                _ = get_with_timeout(exit_queue, timeout=0.01)
                log.debug("cleaning up mocks")
                cleanup_mocks(overlay_in_q, overlay_out_q, fe_in_q, be_nodes, be_mpool)
                return
            except TimeoutError:
                log.debug("exit timeout")
                pass

            if isinstance(gs_msg, dmsg.GSProcessCreate):
                log.info("handling GSProcessCreate on la_be")
                handle_gsprocesscreate(fe_in_q, gs_msg)
            elif isinstance(gs_msg, dmsg.GSTeardown):
                log.info("la_be received GSTeardown")
                fe_in_q.put(dmsg.GSHalted(tag=next_tag()))
                break

        nhalted = 0
        goal_halted = len(be_nodes)
        while True:
            for node in be_nodes.values():
                lmsg = None
                log.debug("posting mock recv")
                try:
                    lmsg = node["be_in_q"].get(timeout=0.01)
                except Exception:
                    pass

                try:
                    _ = get_with_timeout(exit_queue, timeout=0.01)
                    cleanup_mocks(overlay_in_q, overlay_out_q, fe_in_q, be_nodes, be_mpool)
                    return
                except TimeoutError:
                    pass

                if isinstance(lmsg, dmsg.LSHaltTA):
                    log.info("recvd LSHaltTA")
                    node["fe_in_q"].put(dmsg.TAHalted(tag=next_tag()))
                elif isinstance(lmsg, dmsg.LSTeardown):
                    log.info("recvd LSTeardown")
                    if hang_backend:
                        log.debug("waiting on exit queue signal")
                        while True:
                            la_exit = exit_queue.recv(timeout=1)
                            if la_exit is not None:
                                break
                        log.debug("exit queue signal received")
                        cleanup_mocks(overlay_in_q, overlay_out_q, fe_in_q, be_nodes, be_mpool)
                        mock_launch.wait.return_value = 2
                        log.debug("returning early from mocks")
                        return
                    else:
                        node["fe_in_q"].put(dmsg.LSHaltBE(tag=next_tag()))

                elif isinstance(lmsg, dmsg.BEHalted):
                    log.info("recvd BEHalted")
                    nhalted += 1
                    log.info(f"inner nhalted ({nhalted}) v. goal_halted ({goal_halted})")
                    if nhalted == goal_halted:
                        log.info("breaking out of first BEHalted")
                        break

            log.info(f"nhalted ({nhalted}) v. goal_halted ({goal_halted})")
            if nhalted == goal_halted:
                log.info("breaking out of second BEHalted")
                break

        # Handle the final TAHalted for overlay
        log.info("posting recv for FE overlay")
        halt_on = overlay_in_q.get(timeout=10.0)
        assert isinstance(halt_on, dmsg.LAHaltOverlay), "was expecting an LAHaltOverlay"
        if hang_overlay:
            log.debug("waiting on exit queue signal")
            _ = exit_queue.recv()
            log.debug("exit queue signal received")
            cleanup_mocks(overlay_in_q, overlay_out_q, fe_in_q, be_nodes, be_mpool)
            mock_launch.wait.return_value = 2
            log.debug("breaking out of mocks before halting overlay")
            return
        else:
            overlay_out_q.put(dmsg.OverlayHalted(tag=next_tag()))
    except Exception as e:
        log.info(f"hit frontend mocks exception: {e}")
        pass

        if mock_launch is not None:
            mock_launch.wait.return_value = None

    cleanup_mocks(overlay_in_q, overlay_out_q, fe_in_q, be_nodes, be_mpool)

    # Let the test know we're done:
    if mock_launch is not None:
        mock_launch.wait.return_value = 0

    log.info("returning from frontends mock")


class SigIntTest(unittest.TestCase):

    def setUp(self):

        self.test_dir = os.path.dirname(os.path.realpath(__file__))
        self.network_config = os.path.join(self.test_dir, "slurm_primary.yaml")

        self.be_mpool = None
        self.fe_in_q = None
        self.be_nodes = {}
        self.overlay_in_q = None
        self.overlay_out_q = None
        self.primary_be_in_q = None

        self.args_map = get_args_map(self.network_config, from_wlm=False)
        self.wlm_args_map = get_args_map(self.network_config, from_wlm=True)

        self.exit_queue = SRQueue()

        # Some tests will only work in a multinode environment with
        # an activate allocation
        try:
            self.multi_mode = determine_environment(self.wlm_args_map)
        except RuntimeError:
            self.multi_mode = False
            pass

        # Before doing anything set my host ID
        ensure_frontend_host_id()
        setup_FE_logging(
            log_device_level_map=self.args_map["log_device_level_map"], basename="dragon", basedir=os.getcwd()
        )
        log = logging.getLogger("sigint setUp")
        for key, value in self.args_map.items():
            if value is not None:
                log.info(f"args_map: {key}: {value}")

    def tearDown(self):

        for q_attr in ["overlay_in_q", "overlay_out_q", "fe_in_q"]:
            try:
                q = getattr(self, q_attr, None)
                if q is not None:
                    q.close()
                    setattr(self, q_attr, None)
            except Exception:
                pass

        try:
            for node in (self.be_nodes or {}).values():
                try:
                    node["be_in_q"].close()
                except Exception:
                    pass
                try:
                    node["be_ch_in"].destroy()
                except Exception:
                    pass
                try:
                    node["ls_ch"].destroy()
                except Exception:
                    pass
                try:
                    if node.get("gs_ch") is not None:
                        node["gs_ch"].destroy()
                except Exception:
                    pass
        except Exception:
            pass

        try:
            if self.be_mpool is not None:
                self.be_mpool.destroy()
                del self.be_mpool
                self.be_mpool = None
        except Exception:
            pass

    @patch.dict(os.environ, {SlurmWLM.ENV_SLURM_JOB_ID: "1234", SlurmWLM.ENV_SLURM_NUM_NODES: "4"})
    @patch("dragon.launcher.frontend.LauncherFrontEnd._launch_backend")
    @patch("dragon.launcher.frontend.start_overlay_network")
    def test_clean_exit(self, mock_overlay, mock_be_launch):
        """Test a clean bring-up and teardown"""

        # Start my mocks support the frontend in a background thread()
        mock_args = {
            "mock_overlay": mock_overlay,
            "mock_launch": mock_be_launch,
            "args_map": self.args_map,
            "overlay_only": False,
            "no_proc_create": False,
            "exit_queue": self.exit_queue,
        }

        mock_procs = threading.Thread(
            name="Frontend Supporting Mocks", target=run_frontend_supporting_mocks, kwargs=mock_args, daemon=False
        )

        mock_procs.start()

        with LauncherFrontEnd(args_map=self.args_map) as fe_server:
            fe_server.run_startup()
            fe_server.run_app()
            fe_server.run_msg_server()

        mock_procs.join()

    @patch.dict(os.environ, {SlurmWLM.ENV_SLURM_JOB_ID: "1234", SlurmWLM.ENV_SLURM_NUM_NODES: "4"})
    @patch("dragon.launcher.frontend.LauncherFrontEnd._launch_backend")
    @patch("dragon.launcher.frontend.start_overlay_network")
    def test_sig_0(self, mock_overlay, mock_be_launch):
        """Test SIGINT being raised at case 0 in launcher"""

        sigint_trigger = 0
        with LauncherFrontEnd(args_map=self.args_map, sigint_trigger=sigint_trigger) as fe_server, \
             self.assertRaises(KeyboardInterrupt):
            fe_server.run_startup()
            fe_server.run_app()
            fe_server.run_msg_server()

    @patch.dict(os.environ, {SlurmWLM.ENV_SLURM_JOB_ID: "1234", SlurmWLM.ENV_SLURM_NUM_NODES: "4"})
    @patch("dragon.launcher.frontend.LauncherFrontEnd._launch_backend")
    @patch("dragon.launcher.frontend.start_overlay_network")
    def test_sig_1(self, mock_overlay, mock_be_launch):
        """Test SIGINT being raised at case 1 in launcher"""

        sigint_trigger = 1
        with LauncherFrontEnd(args_map=self.args_map, sigint_trigger=sigint_trigger) as fe_server, \
             self.assertRaises(KeyboardInterrupt):
            fe_server.run_startup()
            fe_server.run_app()
            fe_server.run_msg_server()

    @patch.dict(os.environ, {SlurmWLM.ENV_SLURM_JOB_ID: "1234", SlurmWLM.ENV_SLURM_NUM_NODES: "4"})
    def test_sig_1_no_mock(self):
        """Test SIGINT with full runtime being raised at case 1 in launcher"""

        if self.multi_mode:
            sigint_trigger = 1
            with LauncherFrontEnd(args_map=self.wlm_args_map, sigint_trigger=sigint_trigger) as fe_server, \
                 self.assertRaises(KeyboardInterrupt):
                fe_server.run_startup()
                fe_server.run_app()
                fe_server.run_msg_server()
        else:
            print("Unable to run. Requires WLM job allocation")

    @patch.dict(os.environ, {SlurmWLM.ENV_SLURM_JOB_ID: "1234", SlurmWLM.ENV_SLURM_NUM_NODES: "4"})
    def test_sig_minus_1(self):
        """Test SIGINT with full runtime being raised at case -1 in launcher"""

        if self.multi_mode:

            sigint_trigger = -1
            with LauncherFrontEnd(args_map=self.wlm_args_map, sigint_trigger=sigint_trigger) as fe_server, \
                 self.assertRaises(KeyboardInterrupt):
                fe_server.run_startup()
                fe_server.run_app()
                fe_server.run_msg_server()

        else:
            print("Unable to run. Requires WLM job allocation")

    @patch.dict(os.environ, {SlurmWLM.ENV_SLURM_JOB_ID: "1234", SlurmWLM.ENV_SLURM_NUM_NODES: "4"})
    def test_sig_minus_2(self):
        """Test SIGINT with full runtime being raised at case -2 in launcher"""
        if self.multi_mode:
            sigint_trigger = -2
            with LauncherFrontEnd(args_map=self.wlm_args_map, sigint_trigger=sigint_trigger) as fe_server, \
                self.assertRaises(KeyboardInterrupt):
                fe_server.run_startup()
                fe_server.run_app()
                fe_server.run_msg_server()

        else:
            print("Unable to run. Requires WLM job allocation")

    @patch.dict(os.environ, {SlurmWLM.ENV_SLURM_JOB_ID: "1234", SlurmWLM.ENV_SLURM_NUM_NODES: "4"})
    @patch("dragon.launcher.frontend.LauncherFrontEnd._launch_backend")
    @patch("dragon.launcher.frontend.start_overlay_network")
    def test_sig_2(self, mock_overlay, mock_be_launch):
        """Test SIGINT being raised at case 2 in launcher"""

        sigint_trigger = 2
        with LauncherFrontEnd(args_map=self.args_map, sigint_trigger=sigint_trigger) as fe_server, \
             self.assertRaises(KeyboardInterrupt):
            fe_server.run_startup()

    @patch.dict(os.environ, {SlurmWLM.ENV_SLURM_JOB_ID: "1234", SlurmWLM.ENV_SLURM_NUM_NODES: "4"})
    def test_sig_2_no_mock(self):
        """Test SIGINT with full runtime being raised at case 2 in launcher"""
        if self.multi_mode:
            sigint_trigger = 2
            with LauncherFrontEnd(args_map=self.wlm_args_map, sigint_trigger=sigint_trigger) as fe_server, \
                 self.assertRaises(KeyboardInterrupt):
                fe_server.run_startup()
                fe_server.run_app()
                fe_server.run_msg_server()
        else:
            print("Unable to run. Requires WLM job allocation")

    @patch.dict(os.environ, {SlurmWLM.ENV_SLURM_JOB_ID: "1234", SlurmWLM.ENV_SLURM_NUM_NODES: "4"})
    @patch("dragon.launcher.frontend.LauncherFrontEnd._launch_backend")
    @patch("dragon.launcher.frontend.start_overlay_network")
    def test_sig_3(self, mock_overlay, mock_be_launch):
        """Test SIGINT being raised at case 3 in launcher"""

        # Start my mocks support the frontend in a background thread()
        mock_args = {
            "mock_overlay": mock_overlay,
            "mock_launch": mock_be_launch,
            "args_map": self.args_map,
            "overlay_only": True,
            "no_proc_create": False,
            "exit_queue": self.exit_queue,
        }

        mock_procs = threading.Thread(
            name="Frontend Supporting Mocks", target=run_frontend_supporting_mocks, kwargs=mock_args, daemon=False
        )

        mock_procs.start()

        sigint_trigger = 3
        with LauncherFrontEnd(args_map=self.args_map, sigint_trigger=sigint_trigger) as fe_server, \
             self.assertRaises(KeyboardInterrupt):
            fe_server.run_startup()

        mock_procs.join()

    @patch.dict(os.environ, {SlurmWLM.ENV_SLURM_JOB_ID: "1234", SlurmWLM.ENV_SLURM_NUM_NODES: "4"})
    def test_sig_3_no_mock(self):
        """Test SIGINT with full runtime being raised at case 3 in launcher"""
        if self.multi_mode:
            sigint_trigger = 3
            with LauncherFrontEnd(args_map=self.wlm_args_map, sigint_trigger=sigint_trigger) as fe_server, \
                 self.assertRaises(KeyboardInterrupt):
                fe_server.run_startup()
                fe_server.run_app()
                fe_server.run_msg_server()
        else:
            print("Unable to run. Requires WLM job allocation")

    @patch.dict(os.environ, {SlurmWLM.ENV_SLURM_JOB_ID: "1234", SlurmWLM.ENV_SLURM_NUM_NODES: "4"})
    @patch("dragon.launcher.frontend.LauncherFrontEnd._launch_backend")
    @patch("dragon.launcher.frontend.start_overlay_network")
    def test_sig_4(self, mock_overlay, mock_be_launch):
        """Test SIGINT being raised at case 4 in launcher"""

        # Start my mocks support the frontend in a background thread()
        mock_args = {
            "mock_overlay": mock_overlay,
            "mock_launch": mock_be_launch,
            "args_map": self.args_map,
            "overlay_only": True,
            "no_proc_create": False,
            "exit_queue": self.exit_queue,
        }

        mock_procs = threading.Thread(
            name="Frontend Supporting Mocks", target=run_frontend_supporting_mocks, kwargs=mock_args, daemon=False
        )

        mock_procs.start()

        sigint_trigger = 4
        with LauncherFrontEnd(args_map=self.args_map, sigint_trigger=sigint_trigger) as fe_server, \
             self.assertRaises(KeyboardInterrupt):
            fe_server.run_startup()
            fe_server.run_app()
            fe_server.run_msg_server()

        mock_procs.join()

    @patch.dict(os.environ, {SlurmWLM.ENV_SLURM_JOB_ID: "1234", SlurmWLM.ENV_SLURM_NUM_NODES: "4"})
    def test_sig_4_no_mock(self):
        """Test SIGINT with full runtime being raised at case 4 in launcher"""
        if self.multi_mode:
            sigint_trigger = 4
            with LauncherFrontEnd(args_map=self.wlm_args_map, sigint_trigger=sigint_trigger) as fe_server, \
                 self.assertRaises(KeyboardInterrupt):
                fe_server.run_startup()
                fe_server.run_app()
                fe_server.run_msg_server()
        else:
            print("Unable to run. Requires WLM job allocation")

    @patch.dict(os.environ, {SlurmWLM.ENV_SLURM_JOB_ID: "1234", SlurmWLM.ENV_SLURM_NUM_NODES: "4"})
    @patch("dragon.launcher.frontend.LauncherFrontEnd._launch_backend")
    @patch("dragon.launcher.frontend.start_overlay_network")
    def test_sig_5(self, mock_overlay, mock_be_launch):
        """Test SIGINT being raised at case 5 in launcher"""

        # Start my mocks support the frontend in a background thread()
        mock_args = {
            "mock_overlay": mock_overlay,
            "mock_launch": mock_be_launch,
            "args_map": self.args_map,
            "overlay_only": False,
            "no_proc_create": True,
            "exit_at_fenodeidx": True,
            "exit_queue": self.exit_queue,
        }

        mock_procs = threading.Thread(
            name="Frontend Supporting Mocks", target=run_frontend_supporting_mocks, kwargs=mock_args, daemon=False
        )

        mock_procs.start()

        sigint_trigger = 5
        with LauncherFrontEnd(args_map=self.args_map, sigint_trigger=sigint_trigger) as fe_server, \
             self.assertRaises(KeyboardInterrupt):
            fe_server.run_startup()
            fe_server.run_app()
            fe_server.run_msg_server()

        self.exit_queue.send(dmsg.LAExit(tag=next_tag()).serialize())
        mock_procs.join()

    @patch.dict(os.environ, {SlurmWLM.ENV_SLURM_JOB_ID: "1234", SlurmWLM.ENV_SLURM_NUM_NODES: "4"})
    def test_sig_5_no_mock(self):
        """Test SIGINT with full runtime being raised at case 5 in launcher"""
        if self.multi_mode:
            sigint_trigger = 5
            with LauncherFrontEnd(args_map=self.wlm_args_map, sigint_trigger=sigint_trigger) as fe_server, \
                 self.assertRaises(KeyboardInterrupt):
                fe_server.run_startup()
                fe_server.run_app()
                fe_server.run_msg_server()
        else:
            print("Unable to run. Requires WLM job allocation")

    @patch.dict(os.environ, {SlurmWLM.ENV_SLURM_JOB_ID: "1234", SlurmWLM.ENV_SLURM_NUM_NODES: "4"})
    @patch("dragon.launcher.frontend.LauncherFrontEnd._launch_backend")
    @patch("dragon.launcher.frontend.start_overlay_network")
    def test_sig_6(self, mock_overlay, mock_be_launch):
        """Test SIGINT being raised at case 6 in launcher"""

        # Start my mocks support the frontend in a background thread()
        mock_args = {
            "mock_overlay": mock_overlay,
            "mock_launch": mock_be_launch,
            "args_map": self.args_map,
            "overlay_only": False,
            "no_proc_create": False,
            "exit_queue": self.exit_queue,
        }

        mock_procs = threading.Thread(
            name="Frontend Supporting Mocks", target=run_frontend_supporting_mocks, kwargs=mock_args, daemon=False
        )

        mock_procs.start()

        sigint_trigger = 6
        with LauncherFrontEnd(args_map=self.args_map, sigint_trigger=sigint_trigger) as fe_server, \
             self.assertRaises(KeyboardInterrupt):
            fe_server.run_startup()
            fe_server.run_app()
            fe_server.run_msg_server()
        log = logging.getLogger("test_sig_5")
        self.exit_queue.send(dmsg.LAExit(tag=next_tag()).serialize())
        mock_procs.join()

    @patch.dict(os.environ, {SlurmWLM.ENV_SLURM_JOB_ID: "1234", SlurmWLM.ENV_SLURM_NUM_NODES: "4"})
    def test_sig_6_no_mock(self):
        """Test SIGINT with full runtime being raised at case 6 in launcher"""
        if self.multi_mode:
            sigint_trigger = 6
            with LauncherFrontEnd(args_map=self.wlm_args_map, sigint_trigger=sigint_trigger) as fe_server, \
                 self.assertRaises(KeyboardInterrupt):
                fe_server.run_startup()
                fe_server.run_app()
                fe_server.run_msg_server()
        else:
            print("Unable to run. Requires WLM job allocation")

    @patch.dict(os.environ, {SlurmWLM.ENV_SLURM_JOB_ID: "1234", SlurmWLM.ENV_SLURM_NUM_NODES: "4"})
    @patch("dragon.launcher.frontend.LauncherFrontEnd._launch_backend")
    @patch("dragon.launcher.frontend.start_overlay_network")
    def test_sig_7(self, mock_overlay, mock_be_launch):
        """Test SIGINT being raised at case 7 in launcher"""

        # Start my mocks support the frontend in a background thread()
        mock_args = {
            "mock_overlay": mock_overlay,
            "mock_launch": mock_be_launch,
            "args_map": self.args_map,
            "overlay_only": False,
            "no_proc_create": False,
            "exit_queue": self.exit_queue,
        }

        mock_procs = threading.Thread(
            name="Frontend Supporting Mocks", target=run_frontend_supporting_mocks, kwargs=mock_args, daemon=False
        )

        mock_procs.start()

        sigint_trigger = 7
        log = logging.getLogger("test_sig_7")
        log.debug("entering main test")
        with LauncherFrontEnd(args_map=self.args_map, sigint_trigger=sigint_trigger) as fe_server, \
             self.assertRaises(KeyboardInterrupt):
            fe_server.run_startup()
            fe_server.run_app()
            fe_server.run_msg_server()

        self.exit_queue.send(dmsg.LAExit(tag=next_tag()).serialize())
        mock_procs.join()

    def test_sig_7_no_mock(self):
        """Test SIGINT with full runtime being raised at case 7 in launcher"""
        if self.multi_mode:
            sigint_trigger = 7
            with LauncherFrontEnd(args_map=self.wlm_args_map, sigint_trigger=sigint_trigger) as fe_server, \
                self.assertRaises(KeyboardInterrupt):
                fe_server.run_startup()
                fe_server.run_app()
                fe_server.run_msg_server()
        else:
            print("Unable to run. Requires WLM job allocation")

    @patch.dict(os.environ, {SlurmWLM.ENV_SLURM_JOB_ID: "1234", SlurmWLM.ENV_SLURM_NUM_NODES: "4"})
    @patch("dragon.launcher.frontend.LauncherFrontEnd._launch_backend")
    @patch("dragon.launcher.frontend.start_overlay_network")
    def test_sigint_hung_backend(self, mock_overlay, mock_be_launch):
        """Test SIGINT being raised at case 7 in launcher with a hanging backend"""

        # Start my mocks supporting the frontend in a background thread()
        mock_args = {
            "mock_overlay": mock_overlay,
            "mock_launch": mock_be_launch,
            "args_map": self.args_map,
            "overlay_only": False,
            "no_proc_create": False,
            "hang_backend": True,
            "hang_overlay": False,
            "exit_queue": self.exit_queue,
        }

        mock_procs = threading.Thread(
            name="Frontend Supporting Mocks", target=run_frontend_supporting_mocks, kwargs=mock_args, daemon=False
        )

        mock_procs.start()

        sigint_trigger = 7
        with LauncherFrontEnd(args_map=self.args_map, sigint_trigger=sigint_trigger) as fe_server, \
             self.assertRaises(KeyboardInterrupt):
            fe_server.run_startup()
            fe_server.run_app()
            fe_server.run_msg_server()

        # Let the mock thread know to cleanup:
        log = logging.getLogger("test_sigint_hung_backend")
        log.debug("sending exit signal")
        self.exit_queue.send(dmsg.LAExit(tag=next_tag()).serialize())

        mock_procs.join()

    @patch.dict(os.environ, {SlurmWLM.ENV_SLURM_JOB_ID: "1234", SlurmWLM.ENV_SLURM_NUM_NODES: "4"})
    @patch("dragon.launcher.frontend.LauncherFrontEnd._launch_backend")
    @patch("dragon.launcher.frontend.start_overlay_network")
    def test_sigint_hung_overlay(self, mock_overlay, mock_be_launch):
        """Test SIGINT being raised at case 7 in launcher with a hanging overlay"""

        # Start my mocks supporting the frontend in a background thread
        mock_args = {
            "mock_overlay": mock_overlay,
            "mock_launch": mock_be_launch,
            "args_map": self.args_map,
            "overlay_only": False,
            "no_proc_create": False,
            "hang_backend": False,
            "hang_overlay": True,
            "exit_queue": self.exit_queue,
        }

        mock_procs = threading.Thread(
            name="Frontend Supporting Mocks", target=run_frontend_supporting_mocks, kwargs=mock_args, daemon=False
        )

        mock_procs.start()

        sigint_trigger = 7
        with LauncherFrontEnd(args_map=self.args_map, sigint_trigger=sigint_trigger) as fe_server, \
             self.assertRaises(KeyboardInterrupt):
            fe_server.run_startup()
            fe_server.run_app()
            fe_server.run_msg_server()

        # Let the mock thread know to cleanup:
        self.exit_queue.send(dmsg.LAExit(tag=next_tag()).serialize())

        mock_procs.join()

    @unittest.skip(
        "Skipped pending fix of problem outlined in CIRRUS-1922. This test terminates too much and sometimes kills Docker container."
    )
    @patch.dict(os.environ, {SlurmWLM.ENV_SLURM_JOB_ID: "1234", SlurmWLM.ENV_SLURM_NUM_NODES: "4"})
    @patch("dragon.launcher.frontend.LauncherFrontEnd._launch_backend")
    @patch("dragon.launcher.frontend.start_overlay_network")
    def test_rapid_sigint(self, mock_overlay, mock_be_launch):
        """Test SIGINT being raised at case 8 -- handles 2 SIGINT signals transmitted"""

        # Start my mocks support the frontend in a background thread()
        mock_args = {
            "mock_overlay": mock_overlay,
            "mock_launch": mock_be_launch,
            "args_map": self.args_map,
            "overlay_only": False,
            "no_proc_create": True,
            "exit_at_fenodeidx": True,
            "exit_queue": self.exit_queue,
        }

        mock_procs = threading.Thread(
            name="Frontend Supporting Mocks", target=run_frontend_supporting_mocks, kwargs=mock_args, daemon=False
        )

        mock_procs.start()

        sigint_trigger = 8
        start = time()
        with LauncherFrontEnd(args_map=self.args_map, sigint_trigger=sigint_trigger) as fe_server, \
             self.assertRaises(KeyboardInterrupt):
            fe_server.run_startup()
            fe_server.run_app()
            fe_server.run_msg_server()

        # this test should exit much more quickly since we're triggering the
        # quick exit via 2 SIGINTs and should be faster than the 5 second
        # default timeout
        self.assertLessEqual(time() - start, 5.0)

        self.exit_queue.send(dmsg.LAExit(tag=next_tag()).serialize())
        mock_procs.join()

    def test_rapid_sigint_no_mock(self):
        """Test SIGINT being raised at case 8 -- handles 2 SIGINT signals transmitted. No mocks"""

        if self.multi_mode:
            sigint_trigger = 8
            with LauncherFrontEnd(args_map=self.wlm_args_map, sigint_trigger=sigint_trigger) as fe_server, \
                 self.assertRaises(KeyboardInterrupt):
                fe_server.run_startup()
                fe_server.run_app()
                fe_server.run_msg_server()
        else:
            print("Unable to run. Requires WLM job allocation")

    @unittest.skip(
        "Skipped pending fix of problem outlined in CIRRUS-1922. This test terminates too much and sometimes kills Docker container."
    )
    @patch.dict(os.environ, {SlurmWLM.ENV_SLURM_JOB_ID: "1234", SlurmWLM.ENV_SLURM_NUM_NODES: "4"})
    @patch("dragon.launcher.frontend.LauncherFrontEnd._launch_backend")
    @patch("dragon.launcher.frontend.start_overlay_network")
    def test_teardown_with_hung_backend_sigint(self, mock_overlay, mock_be_launch):
        """Test SIGINT during a clean teardown to trigger a quick exit"""

        mock_args = {
            "mock_overlay": mock_overlay,
            "mock_launch": mock_be_launch,
            "args_map": self.args_map,
            "overlay_only": False,
            "hang_backend": True,
            "exit_queue": self.exit_queue,
        }

        mock_procs = threading.Thread(
            name="Frontend Supporting Mocks", target=run_frontend_supporting_mocks, kwargs=mock_args, daemon=False
        )

        mock_procs.start()

        sigint_trigger = 9
        start = time()
        with LauncherFrontEnd(args_map=self.args_map, sigint_trigger=sigint_trigger) as fe_server, \
             self.assertRaises(KeyboardInterrupt):
            fe_server.run_startup()
            fe_server.run_app()
            fe_server.run_msg_server()

        self.assertLessEqual(time() - start, 5.0)

        # Let the mock thread know to cleanup:
        self.exit_queue.send(dmsg.LAExit(tag=next_tag()).serialize())

        mock_procs.join()


if __name__ == "__main__":
    unittest.main()
