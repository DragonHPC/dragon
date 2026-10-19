import ctypes
import copy
import os
import sys
import logging
import secrets
import threading
import signal
import subprocess
from enum import Enum
from functools import total_ordering
from pathlib import Path
from typing import Optional, List

from ..utils import B64, host_id
from ..channels import Channel, ChannelError, ChannelEmpty, register_gateways_from_env, discard_gateways
from ..managed_memory import MemoryPool, DragonPoolError, DragonMemoryError, DragonPoolCreateFail
from ..transport.overlay import start_overlay_network
from ..transport.util import get_fabric_backend

from ..dlogging.util import DragonLoggingServices as dls
from ..dlogging.util import setup_dragon_logging
from ..dlogging.util import _get_dragon_log_device_level, LOGGING_OUTPUT_DEVICE_DRAGON_FILE
from ..dlogging.logger import DragonLoggingError


from ..infrastructure.util import route, rt_uid_from_ip_addrs, get_external_ip_addr
from ..infrastructure.parameters import POLICY_INFRASTRUCTURE, this_process
from ..infrastructure.node_desc import NodeDescriptor
from ..infrastructure import facts as dfacts
from ..infrastructure import messages as dmsg

from ..infrastructure.queue import InfraQueue

from . import util as dlutil
from .network_config import NetworkConfig
from .launchargs import parse_hosts
from .wlm import WLM, wlm_cls_dict
from .wlm.base import BaseWLM
from .wlm.k8s import KubernetesNetworkConfig

LAUNCHER_FAIL_EXIT = 1
LAUNCHER_SUCCESS_EXIT = 0

# Grace period used when draining already-queued messages during teardown. poll()
# can report a message that another thread consumes first, so the drain loop needs
# a short timeout rather than the full sigint timeout.
OVERLAY_DRAIN_TIMEOUT = 0.1


class LauncherImmediateExit(Exception):
    def __init__(self, msg=""):
        self._msg = msg

    def __str__(self):
        return f"{self._msg}"

    def __repr__(self):
        return f"{str(__class__)}({repr(self._msg)})"


class SigIntImmediateExit(Exception):
    def __init__(self, msg=""):
        self._msg = msg

    def __str__(self):
        return f"{self._msg}"

    def __repr__(self):
        return f"{str(__class__)}({repr(self._msg)})"


@total_ordering
class FrontendState(Enum):
    """Enumerated states of Dragon LauncherFrontEnd"""

    NET_CONFIG = 0
    OVERLAY_STARTING = 1
    OVERLAY_UP = 2
    STARTUP = 3
    BACKEND_LAUNCHED = 4
    STOOD_UP = 5
    APP_EXECUTION = 6
    TEARDOWN = 7
    ABNORMAL_TEARDOWN = 8
    LAUNCHER_DOWN = 9

    def __lt__(self, other):
        if self.__class__ is other.__class__:
            return self.value < other.value
        else:
            return False

    def __gt__(self, other):
        if self.__class__ is other.__class__:
            return self.value > other.value
        else:
            return False

    def __eq__(self, other):
        if self.__class__ is other.__class__:
            return self.value == other.value
        else:
            return False

    def __ne__(self, other):
        if self.__class__ is other.__class__:
            return not self.value == other.value
        else:
            return True


class LauncherFrontEnd:
    """State manager for Dragon Frontend

    Returns:
        LauncherFrontEnd: instance of class
    """

    _DTBL = {}  # dispatch router for msgs, keyed by type of message
    _STBL = {}  # dispatch router for SIGINT handling, keyed by current state
    _TRDOWN = {}  # dispatch router teardown errors, keyed by FrontendState

    _STATE = None

    def __init__(self, args_map, sigint_trigger=None):
        log = logging.getLogger(dls.LA_FE).getChild("LauncherFrontEnd init")
        log.info("start in pid %s, pgid %s", os.getpid(), os.getpgid(0))

        try:
            pals_lib = ctypes.cdll.LoadLibrary("libpals.so")
            self.pals_lib_present = pals_lib is not None
            del pals_lib
        except Exception:
            self.pals_lib_present = False

        # This running value is used to control when to bring down the OverlayNet service threads
        self._shutdown = threading.Event()
        self._abnormal_termination = threading.Event()

        # First error observed anywhere in the runtime. Later failures are usually
        # fallout from this one, so this is what gets reported to the user.
        self._first_error = None
        self._first_error_lock = threading.Lock()

        self.args_map = args_map
        self.nnodes = args_map.get("node_count", 0)
        self.n_idle = args_map.get("idle_count", 0)
        self.telemetry_level = args_map.get("telemetry_level", 0)
        self.network_prefix = args_map.get("network_prefix", dfacts.DEFAULT_TRANSPORT_NETIF)
        self.overlay_port = args_map.get("overlay_port", dfacts.DEFAULT_OVERLAY_NETWORK_PORT)
        self.frontend_port = args_map.get("frontend_port", dfacts.DEFAULT_FRONTEND_PORT)
        self.port = args_map.get("port", dfacts.DEFAULT_TRANSPORT_PORT)
        self._config_from_file = args_map.get("network_config", None)
        self.transport, self.overlay_transport = self._determine_transport_backend(args_map)
        os.environ["DRAGON_TRANSPORT_PORT"] = str(self.port)

        # The oob network should use the same transport agent as the overlay network,
        # but it's started by an arbitrary user process without that knowledge. So,
        # we add the overlay network transport string to the environment, which gets
        # propagated to all user processes.
        os.environ[dfacts.OVERLAY_TRANSPORT_VAR] = str(self.overlay_transport)
        # Proxy gateway selection also needs the user-channel TA: it may differ
        # from the overlay TA, and only the Python TCP user TA lacks mesh headers.
        os.environ[dfacts.env_name("TRANSPORT_AGENT")] = str(self.transport)

        # If using SSH, confirm we have enough info do that:
        self._wlm = args_map.get("wlm", None)
        self.hostlist = args_map.get("hostlist", None)
        self.hostfile = args_map.get("hostfile", None)

        self.wlm_cls: Optional[type[BaseWLM]] = wlm_cls_dict[self._wlm] if self._wlm else None

        # Don't default wlm in argparse. We can better do its error/case handling outside of
        # argparse/make it easier to test
        if self._wlm is None:
            self.wlm_cls = dlutil.detect_wlm()
            self._wlm = WLM.from_str(self.wlm_cls.name) if self.wlm_cls else None

        # If using SSH, confirm we have enough info do that:
        if self._wlm in (WLM.SSH, WLM.DRUN) and self._config_from_file is None:
            self.hostlist = parse_hosts(self.hostlist, self.hostfile)
            if self._wlm is WLM.SSH and not self.hostlist:
                raise RuntimeError("SSH workload manager requires a valid hostlist or hostfile")

        if not self.wlm_cls:
            raise RuntimeError(f"Unsupported workload manager specified: {self._wlm}")
        self.wlm_obj = self.wlm_cls(self.network_prefix, self.port, self.hostlist)

        # Handle some sanity checks on the resilient mode

        # If using resilient mode, confirm --nodes or --idle is set.
        # Checks on a sane node count occur in the frontend code
        self.resilient = args_map.get("resilient", False)
        if self.resilient:
            if self.nnodes == 0 and self.n_idle == 0:
                raise RuntimeError("resilient flag requires setting of '--nodes' or '--idle'")

        # When attachable, generate (or honor a pre-set) auth token that is
        # distributed to Local Services so co-located processes can attach.
        self.attachable = args_map.get("attachable", False)
        if self.attachable:
            self.attach_auth_token = os.environ.get(dfacts.DRAGON_ATTACH_TOKEN_ENV) or secrets.token_hex(32)
        else:
            self.attach_auth_token = None

        # Variety of other state trackers:
        self._sigint_count = 0
        self._sigint_timeout = 5.0
        self._bumpy_exit = threading.Event()

        self._proc_create_resps = 0
        self._proc_exits = 0
        self._tas_halted = 0
        self._sh_halt_be = 0
        self._gs_head_exit_received = 0
        self._gs_process_create_resp_received = 0

        # Objects that need tracked for abnormal termincation
        self._orig_sigint = None
        self.fe_mpool = None
        self.fe_inbound = None
        self.conn_in = None
        self.conn_in_bd = None
        self.conn_outs = None
        self.local_ch_in = None
        self.local_ch_out = None
        self.local_inout = None
        self.gw_ch = None
        self.logging_queue = None
        self.over_proc = None
        self.recv_overlaynet_thread = None
        self.send_overlaynet_thread = None
        self.recv_logs_from_overlaynet_thread = None

        # Int trigger for raising SIGINT at various points of
        # bringup for unit testing purposes
        self._sigint_trigger = sigint_trigger

    def __enter__(self):
        return self

    def _record_error(self, err_info: str, host_id=None) -> None:
        """Record the first error seen so it can be reported instead of its fallout."""
        if not err_info:
            return
        log = logging.getLogger(dls.LA_FE).getChild("_record_error")
        with self._first_error_lock:
            if self._first_error is not None:
                log.debug("ignoring subsequent error (already have a root cause): %s", err_info)
                return
            if host_id is not None:
                node_name = self._host_name_from_id(host_id)
                self._first_error = f"{err_info}" if node_name is None else f"[{node_name}] {err_info}"
            else:
                self._first_error = err_info
        log.error("first error recorded: %s", self._first_error)

    def _host_name_from_id(self, host_id):
        """Best-effort lookup of a hostname for error reporting."""
        try:
            host_ids = host_id if isinstance(host_id, (list, tuple, set)) else [host_id]
            names = [
                node.host_name for node in self.net_conf.values() if str(node.host_id) in {str(h) for h in host_ids}
            ]
            return ", ".join(n for n in names if n) or None
        except Exception:
            return None

    def _recv_startup_msgs(self, expected_type, count: int = 1, *, what: str = None) -> list:
        """Receive `count` startup messages of `expected_type` from the backend.

        Raises the recorded root-cause error if the runtime aborted rather than
        an assertion about whichever message failed to show up.
        """
        what = what or expected_type.__name__
        msgs = []
        for _ in range(count):
            msg = dlutil.get_with_blocking(self.la_fe_stdin)
            if not isinstance(msg, expected_type):
                self._raise_startup_failure(msg, what)
            msgs.append(msg)
        return msgs

    def _raise_startup_failure(self, msg, what: str):
        log = logging.getLogger(dls.LA_FE).getChild("_raise_startup_failure")
        with self._first_error_lock:
            first_error = self._first_error
        log.error("startup failed waiting for %s, received %s", what, type(msg).__name__)
        if first_error:
            raise RuntimeError(f"Runtime startup failed while waiting for {what}. First error was: {first_error}")
        if self._abnormal_termination.is_set() or isinstance(msg, dmsg.ExceptionlessAbort):
            raise RuntimeError(f"Runtime startup aborted while waiting for {what}")
        raise RuntimeError(f"Expected {what} during startup but received {type(msg).__name__}")

    def __exit__(self, exc_type, exc_value, exc_tb):
        log = logging.getLogger(dls.LA_FE).getChild("_cleanup")
        log.debug("doing __exit__ cleanup")
        if exc_type is not None:
            from traceback import print_tb

            log.debug("Exception type: %s, value: %s", exc_type, exc_value)
            print_tb(exc_tb)
        self._cleanup()
        log.debug("exiting frontend via __exit__")

    @staticmethod
    def _determine_transport_backend(args_map):
        transport = args_map.get("transport")
        overlay_transport = args_map.get("overlay_transport")

        if not dfacts.HSTA_BINARY.is_file():
            # Check that there is an HSTA binary
            transport = dfacts.TransportAgentOptions.TCP
            overlay_transport = dfacts.TransportAgentOptions.TCP
            print(f"HSTA binary ({dfacts.HSTA_BINARY}) not available, falling back on TCP transport agent", flush=True)
        else:
            # Default to HSTA if transport was not set by user
            if transport is dfacts.TransportAgentOptions.DRAGON_CONFIG:
                transport = dfacts.TransportAgentOptions.HSTA

            if overlay_transport is dfacts.TransportAgentOptions.DRAGON_CONFIG:
                overlay_transport = dfacts.TransportAgentOptions.TCP

        return transport, overlay_transport

    def _kill_backend(self):
        """Simple function for transmitting SIGKILL to backend with helpful message"""

        if self._wlm is not WLM.K8S:
            if self.wlm_proc.returncode is None:
                self._bumpy_exit.set()
                self.wlm_proc.kill()

    def _takedown_abnormal_backend(self, sigint=False):
        """Function to iteratively take down the backend with one thread to enable easy abort"""
        log = logging.getLogger(dls.LA_FE).getChild("_takedown_abnormal_backend")

        if not sigint:
            print("Abnormal Exit detected. Attempting to clean up over next minute....", flush=True)

        # This is a chance we try to use the route decorated functions which all assume self.msg_log
        # exits. It doesn't necessarily:
        self.msg_log = logging.getLogger(dls.LA_FE).getChild("_takedown_abnormal_backend")

        # Get the threads down first
        self._close_threads(abnormal=True)

        # Do a quick "should I proceed with a teardown loop logic"
        # node index 0 is the primary node
        if FrontendState.STOOD_UP <= self._STATE < FrontendState.TEARDOWN:
            self._STATE = FrontendState.ABNORMAL_TEARDOWN
            log.debug("Updated state to %s", self._STATE)
            try:
                assert self.conn_outs is not None
                self.conn_outs[0].put(dmsg.GSTeardown(tag=dlutil.next_tag()))
            except Exception:
                raise TimeoutError
            log.info("got GSTeardown out")

            try:
                while True:
                    try:
                        msg = dlutil.q_get_with_timeout(self.conn_in, timeout=self._sigint_timeout)

                        if isinstance(msg, dmsg.BEHalted):
                            log.debug("Breaking due to BEHalted")
                            break
                        elif type(msg) in self._DTBL:
                            log.debug("routing message of type %s", type(msg))
                            self._DTBL[type(msg)][0](self, msg=msg)
                        else:
                            log.debug("raising runtime error due to unknown message")
                            raise RuntimeError
                    except Exception as e:
                        if str(e) == "Abnormal exit detected":
                            log.debug("continuing after receiving abnormal exit detected")
                            continue
                        log.debug("exception during abnormal takedown: %s", e)
                        raise e
            finally:
                log.debug("Exiting abnormal backend takedown loop")

    def _wait_on_wlm_proc_exit(self, timeout=30):
        """Short function for doing a wait on wlm subprocess. Allows for easier mocking"""

        return self.wlm_proc.wait(timeout=timeout)

    def _cleanup_abnormal_state(self, sigint=False):
        """Attempt to clean up backend in case it hasn't"""

        log = logging.getLogger(dls.LA_FE).getChild("_cleanup_abnormal_state")
        log.debug("Entered cleanup routine with a still up backend. Trying to teardown...")

        # If we got here, make sure we raise an exception on exit
        if not sigint:
            self._abnormal_termination.set()
        else:
            log.debug("executing abnormal cleanup due to SIGINT rather than AbnormalTermination: %s", self._STATE)

        # Set an alarm just in case
        try:
            self._takedown_abnormal_backend(sigint=sigint)
        except (subprocess.TimeoutExpired, RuntimeError, TimeoutError) as e:
            log.debug("teardown failed with exception %s, proceeding to kill backend", e)
            pass

        try:
            if self.resilient:
                # We need to make sure there's nothing hanging around on the backend if we
                # do a restart. So, kill all our work on the backend first
                log.debug("dragon bumpy cleanup")
                self._dragon_cleanup_bumpy_exit()
                if self._wlm is not WLM.K8S:
                    log.debug("waiting on wlm_proc exit")
                    self._wait_on_wlm_proc_exit(timeout=dfacts.DEFAULT_WLM_TIMEOUT)
            else:
                if self._wlm is not WLM.K8S:
                    self._wait_on_wlm_proc_exit(timeout=self._sigint_timeout)
        except (subprocess.TimeoutExpired, RuntimeError, TimeoutError) as e:
            log.debug("forcibly kill wlm proc due to exception %s", e)
            self._kill_backend()

        self._close_comm_overlaynet(abnormal=True)

        log.debug("Finished abnormal state cleanup")

        log.debug("Sending a message to break msg server out of its loop, just in case")
        self.la_fe_stdin.send(dmsg.LAExit(sigint=sigint, tag=dlutil.next_tag()).serialize())
        log.debug("LAExit sent")
        self._STATE = FrontendState.LAUNCHER_DOWN

        # if during teardown we hit an error, clean ourselves up
        if self._bumpy_exit.is_set():
            self._dragon_cleanup_bumpy_exit()

    def _cleanup(self, sigint=False):
        """clean up any memory resources"""
        log = logging.getLogger(dls.LA_FE).getChild("_cleanup")

        log.debug("beginning cleanup")

        # Close threads best as possible, assume abnormal
        # because we want to make sure they're down at this point
        self._close_threads(abnormal=True)

        # If we're here due to an abnormal termination, we need
        # to make a good faith effort to teardown the backend
        log.debug("current state: %s", self._STATE)
        if FrontendState.LAUNCHER_DOWN > self._STATE > FrontendState.STARTUP:
            log.debug("Attemping to take down backend before killing it all off")
            self._cleanup_abnormal_state(sigint=sigint)

        try:
            if self._wlm is not WLM.K8S:
                if self.wlm_proc.returncode is None:
                    log.debug("killing workload manager backend proc")
                    self.wlm_proc.kill()
        except Exception:
            pass

        self._close_comm_overlaynet()

        # if during teardown we hit an error, clean ourselves up
        if self._bumpy_exit.is_set():
            self._dragon_cleanup_bumpy_exit()

        log.debug("Exiting _cleanup")

        if self.logging_queue:
            try:
                self.logging_queue.destroy()
                self.logging_queue = None
            except Exception:
                pass

        # And raise error
        if self._abnormal_termination.is_set():
            log.debug("Raising Abnormal RuntimeError")
            with self._first_error_lock:
                first_error = self._first_error
            if first_error:
                raise RuntimeError(f"Abnormal exit detected. First error was: {first_error}")
            raise RuntimeError("Abnormal exit detected")

    def _wait_on_overlay_init(self):
        """Wait until overlay network returns OverlayPingLA signaling it is up"""
        log = logging.getLogger("overlay_startup")
        log.info("Channel tree initializing...")
        ping_back = dlutil.q_get_with_blocking(self.local_in_q)
        assert isinstance(ping_back, dmsg.OverlayPingLA)
        log.debug("recvd msg type %s", type(ping_back))
        self._STATE = FrontendState.OVERLAY_UP

    def _close_overlay(self, abnormal=False):
        """Close the overlay process"""
        log = logging.getLogger("close_overlay")
        if self._STATE >= FrontendState.OVERLAY_UP:
            try:
                self.local_out_q.put(dmsg.LAHaltOverlay(tag=dlutil.next_tag()))
                log.debug("sent halt overlay signal ")

                # If we're in an abnormal exit, make sure to not block
                try:
                    if abnormal:
                        overlay_halted = dlutil.q_get_with_timeout(self.local_in_q, timeout=self._sigint_timeout)
                    else:
                        overlay_halted = dlutil.q_get_with_blocking(self.local_in_q)
                except TimeoutError:
                    log.debug("timeout on recv dmsg.OverlayHalted")
                else:
                    log.debug("received %s from local TCP agent", overlay_halted)
                    assert isinstance(overlay_halted, dmsg.OverlayHalted)

            except (AttributeError, ValueError):
                pass

        # Make sure the overlay is down before continuing
        try:
            self.over_proc.wait(timeout=self._sigint_timeout)
        except subprocess.TimeoutExpired:
            log.debug("timeout on overlay exit. Killing")
            self.over_proc.kill()
        except AttributeError:
            pass

    def _close_threads(self, abnormal=False):
        log = logging.getLogger(dls.LA_FE).getChild("close_threads")
        log.debug("entering _close_threads abnormal=%s", abnormal)

        halt_overlay_msg = dmsg.HaltOverlay(tag=dlutil.next_tag())
        halt_logging_msg = dmsg.HaltLoggingInfra(tag=dlutil.next_tag())

        if self.recv_overlaynet_thread is not None:
            if self.recv_overlaynet_thread.is_alive():
                if abnormal:
                    log.info("abnormal closing of recv overlay thread")
                    self._shutdown.set()
                    try:
                        self.conn_in_bd.put(halt_overlay_msg)
                    except Exception:
                        log.debug("could not send halt to recv overlay thread", exc_info=True)
                dlutil.join_thread(self.recv_overlaynet_thread, log=log, abnormal=abnormal)

        # Test a rapid sigint trigger
        try:
            if self._sigint_trigger == 8:
                log.debug("raising signal 2nd time")
                self._sigint_trigger = None
                signal.raise_signal(signal.SIGINT)
        except TypeError:
            pass

        if self.send_overlaynet_thread is not None:
            if self.send_overlaynet_thread.is_alive():
                if abnormal:
                    self._shutdown.set()
                    try:
                        self.send_msg_to_overlaynet("A", halt_overlay_msg)
                    except Exception:
                        log.debug("could not send halt to send overlay thread", exc_info=True)
                dlutil.join_thread(self.send_overlaynet_thread, log=log, abnormal=abnormal)

        if self.recv_logs_from_overlaynet_thread is not None:
            if self.recv_logs_from_overlaynet_thread.is_alive():
                # Send it a message to make sure it can get out
                self._shutdown.set()
                try:
                    self.logging_queue.put(halt_logging_msg)
                except Exception:
                    log.debug("could not send halt to logging thread", exc_info=True)
            dlutil.join_thread(self.recv_logs_from_overlaynet_thread, log=log, abnormal=abnormal)

    def _close_comm_overlaynet(self, abnormal=False):
        log = logging.getLogger(dls.LA_FE).getChild("_close_comm_overlaynet")

        try:
            while self.conn_in.poll():
                # Empty the connection first. There may be log messages
                # in it still after the head process exits. Emptying it
                # allows the stream transport to exit below if it should
                # be stuck in a send because the channel was full. Otherwise
                # if the channel is full, the transport will not cleanly exit.
                try:
                    msg = dlutil.q_get_with_timeout(self.conn_in, timeout=OVERLAY_DRAIN_TIMEOUT)
                except TimeoutError:
                    log.debug("nothing left to drain from conn_in during cleanup")
                    break
                if type(msg) in LauncherFrontEnd._DTBL:
                    log.debug("routing message of type %s during cleanup", type(msg))
                    self._DTBL[type(msg)][0](self, msg=msg)
                else:
                    log.info("Message discarded after head process exit: %s", type(msg))

        except Exception:
            pass
        finally:
            if self.conn_in is not None:
                self.conn_in.close()
                self.conn_in = None

        # Close the overlay tree agent
        try:
            log.info("shutting down frontend overlay tree agent")
            self._close_overlay(abnormal=abnormal)
        except Exception:
            if abnormal:
                log.info("killing overlay on frontend")
                self.over_proc.kill()
            else:
                raise

        try:
            self.conn_in_bd.close()
        except Exception:
            pass

        log.debug("closing overlaynet channels and gateway")
        try:
            for conn_out in self.conn_outs.values():
                try:
                    conn_out.close()
                except Exception:
                    pass
        except Exception:
            pass

        try:
            if self.gw_ch is not None:
                log.debug("closing comms to overlay TCP agent and mpool")
                self.gw_ch.destroy()
                discard_gateways()
                try:
                    del os.environ[dfacts.GW_ENV_PREFIX + str(dfacts.DRAGON_OVERLAY_DEFAULT_NUM_GW_CHANNELS_PER_NODE)]
                except KeyError:
                    pass
                log.debug("gw closed")
        except ChannelError:
            pass

        try:
            self.local_in_q.close()
        except Exception:
            pass
        log.debug("local_in_q closed")
        try:
            self.local_out_q.close()
        except Exception:
            pass
        log.debug("local_out_q closed")

        try:
            if self.local_ch_in is not None:
                self.local_ch_in.destroy()
                log.debug("local_ch_in closed")
        except ChannelError:
            pass

        try:
            if self.local_ch_out is not None:
                self.local_ch_out.destroy()
                log.debug("local_ch_out closed")
        except ChannelError:
            pass

        try:
            if self.fe_inbound is not None:
                self.fe_inbound.destroy()
                log.debug("fe_inbound closed")
        except ChannelError:
            pass

        try:
            if self.fe_mpool is not None:
                self.fe_mpool.destroy()
                log.debug("fe_mp closed")
        except (DragonMemoryError, DragonPoolError):
            pass
        log.info("comms overlay network closed")

    def _set_quick_teardown(self):
        """Set up conditions to tear down runtime as quickly as reasonable"""
        print("Quickly tearing down Dragon runtime due to KeyboardInterrupt.", flush=True)
        self._bumpy_exit.set()
        self._sigint_timeout = 0.01

    def _dragon_cleanup_bumpy_exit(self):
        """Helper function to launch `dragon-cleanup` at end of teardown to clean up all procs and mem"""
        print("Detected an abnormal exit. Will attempt to clean up Dragon resources...", flush=True)

        env = os.environ.copy()
        if "DRAGON_PATCH_MP" in env:
            del env["DRAGON_PATCH_MP"]

        if self.resilient:
            subprocess.run(args=["dragon-cleanup", "--only-be", "--resilient"], env=env)
        else:
            subprocess.run(args=["dragon-cleanup", "--only-be"], env=env)

        # Make sure we don't manage to call this more than once
        self._bumpy_exit.clear()

    def _sigint_handler(self, *args):
        """Handler for SIGINT signals for graceful teardown"""
        log = logging.getLogger("_sigint_handler")
        log.debug("Entered sigint handler")
        self.sigint_log = logging.getLogger("sigint_route")
        self._sigint_count = self._sigint_count + 1

        print("KeyboardInterrupt detected. Attempting to clean up...", flush=True)

        # If the user has triggered SIGINT more than once just get out of here
        # very quickly.
        if self._sigint_count > 1:
            self._set_quick_teardown()

        if self._STATE.value in LauncherFrontEnd._STBL:
            self._STBL[self._STATE.value][0](self)
        else:
            log.warning("SIGINT detected in %s and no routing exists for it", self._STATE)

    def _sighup_handler(self, *args):
        log = logging.getLogger("_sighup_handler")
        # needed for logging of set_quick_teardown path
        self.sigint_log = logging.getLogger("sighup_route")
        self._sigint_count = 2

        print("Frontend detected SIGHUP or SIGTERM from WLM. Attempting to clean up...", flush=True)
        log.debug("caught sighup on frontend")

        # Commented out the below call to self._set_quick_teardown() as part of AICI-1963. Calling
        # _set_quick_teardown reduces the _sigint_timeout from a full 5 seconds to .01 seconds.
        # Effectively, this means that the FE launcher will try to 'gracefully' shutdown for 0.01
        # seconds, and then start shooting threads in the head. By allowing the full 5 seconds, the
        # backends are allowed to communicate with the frontend and proceed through their shutdown
        # sequence. If things are not gracefully shutdown after 5 seconds, the FE launcher will again
        # start shooting threads. Or, at least that's the theory. Leaving this commented out for now
        # to see if it resolves issues seen in AICI-1963 or if further changes are needed.

        # self._set_quick_teardown()

        if self._STATE.value in LauncherFrontEnd._STBL:
            self._STBL[self._STATE.value][0](self)
        else:
            log.warning("SIGHUP or SIGTERM detected in %s and no routing exists for it", self._STATE)

    def _sigterm_handler(self, *args):
        log = logging.getLogger("_sigterm_handler")
        log.debug("caught sigterm on frontend")
        self._dragon_cleanup_bumpy_exit()

    @route(FrontendState.NET_CONFIG.value, _STBL)
    def _sigint_net_config(self):
        # if we're here, we should be able to just exit
        self.sigint_log.debug("SIGINT caught during NET_CONFIG. Executing default SIGINT behavior")
        self._close_comm_overlaynet()
        raise KeyboardInterrupt

    @route(FrontendState.OVERLAY_STARTING.value, _STBL)
    def _sigint_overlay_starting(self):
        # Wait on receipt that the Overlay is up so we can
        # calmly tear it down
        self.sigint_log.debug("SIGINT caught during OVERYLAY_STARTING. Waiting for Overlay to come up and then exit")
        self._wait_on_overlay_init()

        # Close everything else:
        self._close_comm_overlaynet()
        raise KeyboardInterrupt

    @route(FrontendState.OVERLAY_UP.value, _STBL)
    def _sigint_overlay_up(self):
        # Close overlay and then exit
        self.sigint_log.debug("SIGINT caught during OVERYLAY_UP. Closing overlay and exiting")

        # Close everything else:
        self._close_threads(abnormal=True)
        self.sigint_log.debug("closed threads")

        self._close_comm_overlaynet()
        self.sigint_log.debug("cleaned comms")
        raise KeyboardInterrupt

    @route(FrontendState.STARTUP.value, _STBL)
    @route(FrontendState.BACKEND_LAUNCHED.value, _STBL)
    def _sigint_startup(self):
        # If in startup, get through it first
        self.sigint_log.debug("Raising SIGINT caught in bringup.")
        self._cleanup(sigint=True)
        self.sigint_log.debug("cleaned up. Raising KeyboardInterrupt")
        raise KeyboardInterrupt

    @route(FrontendState.STOOD_UP.value, _STBL)
    @route(FrontendState.APP_EXECUTION.value, _STBL)
    def _sigint_stood_up(self):
        self.sigint_log.debug("Doing teardown due to SIGINT in signal handler")
        self.start_sigint_teardown()

    @route(FrontendState.TEARDOWN.value, _STBL)
    @route(FrontendState.ABNORMAL_TEARDOWN.value, _STBL)
    @route(FrontendState.LAUNCHER_DOWN.value, _STBL)
    def _sigint_teardown(self):
        # Assume if we've caught a signal during teardown it's because
        # the user wants us to get out of here quickly.
        self.sigint_log.debug("Caught sigint during teardown. Will begin quick teardown")
        self._set_quick_teardown()
        self.start_sigint_teardown()

    def start_sigint_teardown(self):
        """SIGINT teardown init

        Sends GSTeardown message if SIGINT is registered by user
        """
        # Set an alarm just in case
        log = logging.getLogger(dls.LA_FE).getChild("sigint teardown")
        log.debug("SIGINT received. Tearing down runtime....")
        self._cleanup_abnormal_state(sigint=True)

    def _launch_backend(
        self,
        nnodes: int,
        nodelist: list[str],
        fe_ip_addr: str,
        fe_host_id: str,
        frontend_sdesc: str,
        network_prefix: str,
        *,
        node_ip_addrs: Optional[list[str]] = None,
    ):
        """Launch backend with selected wlm"""
        log = logging.getLogger(dls.LA_FE).getChild("_launch_backend")

        assert self.wlm_obj
        log.debug("Launching backend with wlm %s", self._wlm)
        wlm_proc = self.wlm_obj.launch_backend(
            nnodes=nnodes,
            node_ip_addrs=node_ip_addrs,
            nodelist=nodelist,
            args_map=self.args_map,
            fe_ip_addr=fe_ip_addr,
            fe_host_id=fe_host_id,
            frontend_sdesc=frontend_sdesc,
            network_prefix=network_prefix,
            overlay_transport=self.overlay_transport,
            overlay_port=self.overlay_port,
        )

        return wlm_proc

    def construct_bcast_tree(self, net_conf, be_ups, frontend_sdesc):
        log = logging.getLogger(dls.LA_FE).getChild("construct_bcast_tree")

        # Pack up all of our node descriptors for the backend:
        forwarding = {}
        for be_up in be_ups:
            assert isinstance(be_up, dmsg.BEIsUp), "la_fe received invalid be up"
            # TODO: VERIFY WE GET A UNIQUE CUID: keep a set of seen cuids.
            # After attaching, get the cuid and compare it against the set
            # of already seen cuids. Throw an exception if already seen.
            # Delete the set after this loop.
            log.debug("received descriptor: %s and host_id: %s", be_up.be_ch_desc, be_up.host_id)
            for key, node_desc in net_conf.items():
                if str(be_up.host_id) == str(node_desc.host_id):
                    forwarding[key] = NodeDescriptor(
                        host_id=int(node_desc.host_id),
                        name=node_desc.host_name,
                        ip_addrs=node_desc.ip_addrs,
                        overlay_cd=be_up.be_ch_desc,
                    )
                    break
            else:
                raise RuntimeError(
                    f"Received unmatched/unknown host_id=%s. Expected one of hosts=%s.",
                    str(be_up.host_id),
                    ", ".join([str(node_desc.host_id) for node_desc in net_conf.values()]),
                )

        # Send out the FENodeIdx to the child nodes I own
        conn_outs = {}  # key is the node_index and value is the Queue object

        # Create a mapping of node indices to their key in the network config
        net_conf_key_mapping = [k for k, v in net_conf.items() if v.state is NodeDescriptor.State.ACTIVE]
        fe_node_idx_msg = dmsg.FENodeIdxBE(
            tag=dlutil.next_tag(),
            node_index=0,
            forward=forwarding,
            send_desc=frontend_sdesc,
            net_conf_key_mapping=net_conf_key_mapping,
        )

        # Create a counter that increments to ensure there's an increment of 1 for each
        # active node index. Otherwise, there may be problems in the bcast algorithm
        fe_node_index = 0
        # iterate over the keys in the same order as the net_conf_key_mapping
        for idx in net_conf.keys():
            if idx == "f":
                continue
            # If we haven't grabbed enough nodes, do so and make sure they're active
            if fe_node_index < this_process.overlay_fanout and net_conf[str(idx)].state is NodeDescriptor.State.ACTIVE:
                log.debug(
                    "constructing FENodeIdxBE for %s (idx = %s | fe_node_index = %s)",
                    net_conf[str(idx)],
                    idx,
                    fe_node_index,
                )
                try:
                    conn_out = InfraQueue.attach(forwarding[str(idx)].overlay_cd, mpool=self.fe_mpool)

                    # Update the node index to the one we're talking to
                    fe_node_idx_msg.node_index = fe_node_index
                    log.debug("sending %s", fe_node_idx_msg.uncompressed_serialize())
                    conn_out.put(fe_node_idx_msg)

                    conn_outs[fe_node_index] = conn_out
                    fe_node_index = fe_node_index + 1

                except ChannelError as ex:
                    log.fatal("could not connect to BE channel with host_id %s", be_up.host_id)
                    raise RuntimeError("Connection with BE failed") from ex

        log.info("sent all FENodeIdxBE msgs")

        return conn_outs

    def _set_node_to_down_state(self, host_ids: list):
        """Given a host ID, update the Net conf, setting the node to a down state

        :param host_id: host ID of down node
        :type host_id: int
        """

        log = logging.getLogger(dls.LA_FE).getChild("_set_node_to_down_state")

        for index, node in self.net_conf.items():
            for host_id in host_ids:
                if node.host_id == host_id:
                    log.debug("setting node index %s, host_id %s hostname %s to down", index, host_id, node.host_name)
                    node.state = NodeDescriptor.State.DOWN

    def _define_node_pools(self, net_conf: dict):
        """Make net config match what we know about our node pools

        :param net_conf: backend network configuration made of NodeDescriptor objects index by string integer
        :type net_conf: dict
        """

        log = logging.getLogger(dls.LA_FE).getChild("_define_node_pools")

        # Work out logic for our node counts:
        nnodes = len(net_conf) - 1  # Remove the frontend from consideration
        all_avail_nodes = nnodes
        log.debug("requested %s and got %s", self.nnodes, nnodes)
        if self.nnodes > 0:
            if self.nnodes > nnodes:
                log.exception("too many nodes requested")
                print(
                    f"Requested {self.nnodes} nodes but only {nnodes} viable nodes were found on the high speed network in the allocation.\nSpecify -N {nnodes} to run with your viable nodes.",
                    file=sys.stderr,
                    flush=True,
                )
                raise ValueError("Not enough backend nodes allocated to match requested")
            nnodes = self.nnodes
        else:
            self.nnodes = nnodes

        # If doing resilient training, do some sanity checks on requested # nodes, idle nodes
        if self.resilient:
            # Confirm the number of idle nodes requested agrees with nnodes
            if self.n_idle != 0:
                if all_avail_nodes - (self.nnodes + self.n_idle) < 0:
                    msg = f"Sum of requested active ({self.nnodes}) and idle ({self.n_idle}) nodes is greater than available ({all_avail_nodes})"
                    raise RuntimeError(msg)
            else:
                self.n_idle = all_avail_nodes - self.nnodes
            log.debug("Executing resilient mode with %s idle nodes", self.n_idle)

        # Make sure the number of active/idle nodes matches what's been requested
        n_active = len(
            [node for index, node in net_conf.items() if index != "f" and node.state is NodeDescriptor.State.ACTIVE]
        )

        n_down = len(
            [node for index, node in net_conf.items() if index != "f" and node.state is NodeDescriptor.State.DOWN]
        )
        log.debug("currently have %s active nodes against %s down and %s idle", n_active, n_down, self.n_idle)
        if n_active != self.nnodes or n_down != 0:
            current_active = 0
            for index, node in net_conf.items():
                if index == "f":
                    continue
                elif current_active != self.nnodes and node.state is not NodeDescriptor.State.DOWN:
                    if node.state in [NodeDescriptor.State.IDLE, NodeDescriptor.State.ACTIVE]:
                        node.state = NodeDescriptor.State.ACTIVE
                        current_active = current_active + 1
                elif node.state != NodeDescriptor.State.DOWN:
                    node.state = NodeDescriptor.State.IDLE

            log.debug("Updated active list has %s active nodes", current_active)
            # Make sure the number of active nodes is not 0
            if current_active == 0:
                raise RuntimeError("No available backend hardware resources to use")

            # Log if there are fewer than requested nodes available because too many have been
            # marked as down
            if current_active != self.nnodes:
                self.nnodes = current_active
                msg = """There are fewer available backend nodes than requested.
Will continue using the available nodes until all resources are exhausted.
Performance may be suboptimal."""
                log.warning(msg)

        # Make sure there is a designated primary node in the configuration and select one if there isn't
        primary_election = [
            index for index, node in net_conf.items() if node.state is NodeDescriptor.State.ACTIVE and node.is_primary
        ]

        # Make sure there is just 1 primary node in case something screwy happened
        if len(primary_election) != 1:
            prim_set = False
            for node in net_conf.values():
                if node.state is NodeDescriptor.State.ACTIVE and not prim_set:
                    log.debug("node %s is now primary", node)
                    node.is_primary = True
                    prim_set = True
                elif node.is_primary and prim_set:
                    node.is_primary = False

        return net_conf

    def _define_overlay_network(self, net_conf: dict, fe_host_id: int, fe_ip_addr: str):
        """Extract backend IP addresses, hostnames, and host IDs for overlay network comms

        :param net_conf: Network configuration dict of NodeDescriptors keyed by string node index
        :type net_conf: dict
        :param fe_host_id: Host ID of frontend node
        :type fe_host_id: int
        :param fe_ip_addr: IP address of frontend node
        :type fe_ip_addr: str
        """
        log = logging.getLogger(dls.LA_FE).getChild("_define_overlay_network")

        # Add as many as needed to meet the requested node count.
        # We also find the minimum number of network interface cards
        # per node.
        min_nics_per_node = 99999
        host_ids = [str(fe_host_id)]
        ip_addrs = [fe_ip_addr]
        hostnames = []

        # Host IDs are the routing keys for the overlay network and for matching
        # BEIsUp messages back to nodes, so duplicates silently collapse distinct
        # nodes onto a single route.
        hosts_by_id = {str(fe_host_id): ["frontend"]}

        for index, node in net_conf.items():
            if index != "f" and node.state is NodeDescriptor.State.ACTIVE:
                min_nics_per_node = min(min_nics_per_node, len(net_conf[index].ip_addrs))
                host_ids.append(str(net_conf[index].host_id))
                ip_addrs.append(net_conf[index].ip_addrs[0])
                hostnames.append(net_conf[index].host_name)
                hosts_by_id.setdefault(str(net_conf[index].host_id), []).append(net_conf[index].host_name)

        if len(hosts_by_id) != len(host_ids):
            collisions = "; ".join(
                f"host_id={hid} shared by {', '.join(names)}" for hid, names in hosts_by_id.items() if len(names) > 1
            )
            log.fatal("duplicate host IDs in network configuration: %s", collisions)
            raise RuntimeError(
                f"Expected {len(host_ids)} unique host IDs but found {len(hosts_by_id)}. "
                f"Duplicate host IDs: {collisions}"
            )

        return host_ids, ip_addrs, hostnames, min_nics_per_node

    def _get_healthy_nodelist(self, net_conf: dict) -> List[str]:
        nodelist = [
            node.host_name
            for idx, node in net_conf.items()
            if node.state in [NodeDescriptor.State.ACTIVE, NodeDescriptor.State.IDLE] and idx != "f"
        ]

        return nodelist

    def _populate_net_config(self, net_conf: Optional[dict] = None):
        """Build an object for the network configuration on the backend

        [description]
        :param net_conf: [description], defaults to None
        :type net_conf: dict, optional
        """

        log = logging.getLogger(dls.LA_FE).getChild("run_startup")
        if net_conf is None:
            # Get the node config for the backend
            log.debug("Getting the node config for the backend.")

            if self._config_from_file is not None:
                try:
                    log.debug("Acquiring network config from file %s", self._config_from_file)
                    self.net = NetworkConfig.from_file(self._config_from_file)
                except Exception:
                    raise RuntimeError("Unable to acquire backend network configuration from input file.")
            else:
                try:
                    log.info("Acquiring network config via %s WLM queries", self._wlm)
                    # This sigint trigger is -2 and -1 cases
                    self.net = NetworkConfig.from_wlm(
                        workload_manager=self._wlm,
                        port=self.overlay_port,
                        network_prefix=self.network_prefix,
                        hostlist=self.hostlist,
                        args_map=self.args_map,
                        sigint_trigger=self._sigint_trigger,
                    )
                except Exception as ex:
                    log.debug("Exception while acquiring network config via WLM: %s", ex)
                    raise RuntimeError("Unable to acquire backend network configuration via workload manager")

        # If we're here, we're doing a restart. We need to assume the Overlay ports we were using are being
        # blocked and that we need to maintain the active and inactive node list inside the current network
        # config provided by the dict
        else:
            try:
                self.net = NetworkConfig.from_sdict(net_conf)
            except Exception:
                raise RuntimeError("Unable to acquire backend network configuration via workload manager")

        return self.net

    def run_startup(self, net_conf: Optional[dict] = None):
        """Complete bring up of runtime services


        :param net_conf: Net config for overlay network. Used if resiliency has been requested at initializationq, defaults to None
        :type net_conf: dict, optional
        """
        log = logging.getLogger(dls.LA_FE).getChild("run_startup")

        # This is set here for the overlay network.
        this_process.set_num_gateways_per_node(dfacts.DRAGON_OVERLAY_DEFAULT_NUM_GW_CHANNELS_PER_NODE)

        # setup sigint handling to proceed in a manner that works for us
        self._STATE = FrontendState.NET_CONFIG

        try:
            # Catch ctrl+c events
            self._orig_sigint = signal.signal(signal.SIGINT, self._sigint_handler)

            # Catch WLM killing off the backend
            self._orig_sighup = signal.signal(signal.SIGHUP, self._sighup_handler)

            self._orig_sigterm = signal.signal(signal.SIGTERM, self._sigterm_handler)

            log.debug("got signal handling in place")

        except ValueError:
            # this error is thrown if we are running inside a child thread
            # which we do for unit tests. So pass on this
            log.debug("Unable to do signal handling outside of main thread")

        self.la_fe_stdin = dlutil.OverlayNetLaFEQueue()
        self.la_fe_stdout = dlutil.LaOverlayNetFEQueue()

        if this_process.net_conf_cache_salt:
            log.debug("checking if net_conf_cache_salt is in the environment")
            if "DRAGON_NET_CONF_CACHE_SALT" not in os.environ:
                log.debug("Setting DRAGON_NET_CONF_CACHE_SALT in the environment")
                os.environ["DRAGON_NET_CONF_CACHE_SALT"] = this_process.net_conf_cache_salt
            else:
                log.debug("DRAGON_NET_CONF_CACHE_SALT already set in the environment")
        else:
            log.debug("No net_conf_cache_salt set in this_process, not setting in the environment")

        if self._wlm is not WLM.K8S:
            # If we have the config via an earlier frontend, don't do it all over again
            self.net = self._populate_net_config(net_conf)
            self.net_conf = self.net.get_network_config()
            if self.nnodes == 0:
                self.nnodes = self.net.allocation_nnodes

            if self._sigint_trigger == 0:
                signal.raise_signal(signal.SIGINT)

            # Add the frontend config
            self.net_conf["f"] = NodeDescriptor.get_local_node_network_conf(
                network_prefix=self.network_prefix, port_range=self.frontend_port
            )
            fe_host_id = os.getenv("DRAGON_FE_HOST_ID", str(self.net_conf["f"].host_id))

            # This annoying if block is to keep Cython from throwing a seg fault for some reason
            if len(self.net_conf["f"].ip_addrs) > 0:
                conf_ip = self.net_conf["f"].ip_addrs[0]
                fe_ip_addr = os.getenv("DRAGON_FE_IP_ADDR", conf_ip)  # it includes the port
            else:
                raise RuntimeError("No NIC matching the requested network prefix found on the frontend. Exiting.")

            if fe_ip_addr != conf_ip:
                self.net_conf["f"].ip_addrs.insert(0, fe_ip_addr)  # Ensure it's first given logic used below.
            log.debug("network config: %s", self.net_conf)
            # this will raise an OSError when the frontend is run on a compute node w/o external access
            try:
                fe_ext_ip_addr = os.getenv("DRAGON_FE_EXTERNAL_IP_ADDR", get_external_ip_addr())
            except OSError:
                fe_ext_ip_addr = None

            # this will exist even w/o external access
            head_node_ip_addr = (
                self.net_conf["0"].ip_addrs[0].split(":")[0]
            )  # TODO: again grabbing 0th ip addr is fragile needs fix!
            os.environ["DRAGON_HEAD_NODE_IP_ADDR"] = head_node_ip_addr

            if fe_ext_ip_addr is not None:
                os.environ["DRAGON_FE_EXTERNAL_IP_ADDR"] = fe_ext_ip_addr
                os.environ["DRAGON_RT_UID"] = str(rt_uid_from_ip_addrs(fe_ext_ip_addr, head_node_ip_addr, self.port))

        # Create my memory pool
        try:
            # Create my memory pool
            if self._wlm is WLM.K8S:
                fe_kubernetes = KubernetesNetworkConfig()

                # Query the API to get my own info
                fe_hostname = os.getenv("HOSTNAME")
                fe_ip_addr = os.getenv("POD_IP")
                fe_host_id = str(host_id())
                fe_port = int(os.getenv("FRONTEND_PORT"))

            try:
                self.fe_mpool = MemoryPool.create_with_retry(
                    int(dfacts.DEFAULT_SINGLE_DEF_SEG_SZ),
                    dfacts.runtime_pool_name(dfacts.DEFAULT_POOL_SUFFIX, host_id=fe_host_id),
                    dfacts.FE_OVERLAY_TRANSPORT_AGENT_MUID,
                )
            except DragonPoolCreateFail:
                print(
                    "\nPossible Configuration Issue:\n"
                    "Pool creation failed. There is not enough shared memory reserved\n"
                    "for Dragon. If you are running in a container, you must increase the\n"
                    "amount of shared memory reserved for your container by specifying\n"
                    "--shm-size=6g on your Docker run command with an appropriate size for \n"
                    "your intended application. At a minimum that might be 6GB for both\n"
                    "backend and frontend nodes, but you may want to reserve more depending\n"
                    "on your managed memory pool needs.\n",
                    file=sys.stderr,
                )
                raise
            pool_ser_bytes = self.fe_mpool.serialize()
            puid, mpool_fname = MemoryPool.serialized_uid_fname(pool_ser_bytes)
            log.debug("fe_mpool has uid %s and file %s", puid, mpool_fname)
            pool_ser_str = B64.bytes_to_str(pool_ser_bytes)
            os.environ["DRAGON_DEFAULT_PD"] = pool_ser_str

            # Create my receiving channel
            fe_cuid = dfacts.FE_CUID
            local_in_cuid = dfacts.FE_LOCAL_IN_CUID
            local_out_cuid = dfacts.FE_LOCAL_OUT_CUID
            gw_cuid = dfacts.FE_GW_CUID

            if self._sigint_trigger == 1:
                signal.raise_signal(signal.SIGINT)

            # Channel for tcp to tell me it's up
            self.local_ch_in = Channel(self.fe_mpool, local_in_cuid)
            self.local_ch_out = Channel(self.fe_mpool, local_out_cuid)

            # Create a gateway and logging channel for my tcp agent
            self.gw_ch = Channel(self.fe_mpool, gw_cuid)
            self.logging_queue = setup_dragon_logging(0)
        except (ChannelError, DragonPoolError, DragonLoggingError, DragonMemoryError) as init_err:
            log.fatal("could not create resources: %s", init_err)
            raise RuntimeError("overlay transport resource creation failed") from init_err

        log.info("Memory pools and channels created")

        if self._sigint_trigger == 2:
            signal.raise_signal(signal.SIGINT)
        # Set gateway in my environment and register them
        encoded_ser_gw = B64(self.gw_ch.serialize())
        encoded_ser_gw_str = str(encoded_ser_gw)
        gw_chan_env = dfacts.GW_ENV_PREFIX + str(dfacts.DRAGON_OVERLAY_DEFAULT_NUM_GW_CHANNELS_PER_NODE)
        os.environ[gw_chan_env] = encoded_ser_gw_str
        register_gateways_from_env()

        try:
            self.fe_inbound = Channel(self.fe_mpool, fe_cuid)
            # Channel for backend to come to
            self.conn_in = InfraQueue(main_channel=self.fe_inbound, pool=self.fe_mpool)
            encoded_inbound_str = self.conn_in.serialize()
            # Backdoor connection for breaking recv_msgs thread out of
            # its blocking receive at teardown
            self.conn_in_bd = self.conn_in
            self.local_in_q = InfraQueue(main_channel=self.local_ch_in, pool=self.fe_mpool)
            self.local_out_q = InfraQueue(main_channel=self.local_ch_out, pool=self.fe_mpool)
        except (ChannelError, DragonPoolError, DragonLoggingError, DragonMemoryError) as init_err:
            log.fatal("could not create resources: %s", init_err)
            raise RuntimeError("overlay transport resource creation failed") from init_err

        # For K8s launch: create the BEs
        if self._wlm is WLM.K8S:
            import yaml

            log.debug("standing up backend")

            self._STATE = FrontendState.STARTUP

            # we need to put the telemetry level in the env before the backend is launched so that it is in both telemetry and user space
            os.environ["DRAGON_TELEMETRY_LEVEL"] = str(self.telemetry_level)

            try:
                temp_fe_label_selector = os.getenv("FRONTEND_JOB_LABEL")
                be_config_map_name = f"/config/backend_pod_{temp_fe_label_selector}.yml"

                with open(be_config_map_name, "r") as f:
                    be_job_config = yaml.safe_load(f)

                be_job_config_str = yaml.dump(be_job_config)

                # Replace the placeholder for the FE label with the actual value
                be_job_config_str = be_job_config_str.replace("{{temp_fe_label}}", temp_fe_label_selector)

                # Replace the placeholder for the telemetry level with the actual value
                be_job_config_str = be_job_config_str.replace(
                    "{{temp_telemetry_level}}", os.environ["DRAGON_TELEMETRY_LEVEL"]
                )

                # Replace the placeholder for the FE channel with the actual value
                be_job_config_str = be_job_config_str.replace("{{temp_fe_sdesc}}", encoded_inbound_str)

                # Replace the placeholder for the dragon log with the actual value
                be_job_config_str = be_job_config_str.replace(
                    "{{temp_log_value}}", os.environ["DRAGON_LOG_DEVICE_ACTOR_FILE"]
                )

                # Replace the placeholder for the overlay transport with the actual value
                be_job_config_str = be_job_config_str.replace("{{temp_overlay_transport}}", str(self.overlay_transport))

                # Do the same for the gateway channel env variable
                be_job_config_str = be_job_config_str.replace(
                    "{{DRAGON_FE_GW}}",
                    dfacts.GW_ENV_PREFIX + str(dfacts.DRAGON_OVERLAY_DEFAULT_NUM_GW_CHANNELS_PER_NODE),
                )
                be_job_config_str = be_job_config_str.replace(
                    "{{temp_fe_gw}}",
                    os.environ[dfacts.GW_ENV_PREFIX + str(dfacts.DRAGON_OVERLAY_DEFAULT_NUM_GW_CHANNELS_PER_NODE)],
                )

                # Get the frontend Job uid and use it in the BE config map for ownerReferences needed by the helm chart
                # to be able to delete the backend pods started by this frontend
                fe_job = fe_kubernetes.k8s_batch_v1.read_namespaced_job(
                    name=temp_fe_label_selector, namespace=fe_kubernetes.namespace
                )
                fe_job_uid = fe_job.metadata.uid
                be_job_config_str = be_job_config_str.replace("{{parentDeployment.metadata.uid}}", fe_job_uid)

                be_job_config = yaml.safe_load(be_job_config_str)
                fe_kubernetes.k8s_batch_v1.create_namespaced_job(namespace=fe_kubernetes.namespace, body=be_job_config)

                self.fe_label_selector = f"app={temp_fe_label_selector}"

                # Get info from the config yaml file
                parallelism = be_job_config.get("spec", {}).get("parallelism", None)
                self.nnodes = int(parallelism)
                # namespace = be_job_config.get('metadata', {}).get('namespace', None)
                be_label_selector = (
                    be_job_config.get("spec", {})
                    .get("template", {})
                    .get("metadata", {})
                    .get("labels", {})
                    .get("app", None)
                )
                be_label_selector = f"app={be_label_selector}"
                del be_job_config

            except Exception as e:
                log.fatal("FE failed to stand up BE")
                log.debug("error: %s", e)
                raise RuntimeError("Backend launch failed from launcher frontend") from e

            # Wait for all the BE to stand up
            import time

            while True:
                pods = fe_kubernetes.k8s_api_v1.list_namespaced_pod(
                    namespace=fe_kubernetes.namespace, label_selector=be_label_selector
                )
                # We need to make sure that k8s has assigned ip addresses to the pods
                # TODO: this is poor check; find a better one
                count = 0
                if len(pods.items) == self.nnodes:
                    for pod in pods.items:
                        if pod.status.pod_ip:
                            count += 1
                        else:
                            break
                    if count == self.nnodes:
                        break
                log.debug("Waiting for all backend pods to be ready...")
                time.sleep(2)

            log.debug("LA BE started on each compute node")
            self._STATE = FrontendState.BACKEND_LAUNCHED

            # Now, build the net config object

            # If we have the config via an earlier frontend, don't do it all over again
            self.net = self._populate_net_config(net_conf)
            self.net_conf = self.net.get_network_config()

            if self._sigint_trigger == 0:
                signal.raise_signal(signal.SIGINT)

            # Add the frontend config
            self.net_conf["f"] = NodeDescriptor(
                state=NodeDescriptor.State.ACTIVE,
                name=fe_hostname,
                host_name=fe_hostname,
                ip_addrs=[fe_ip_addr],
                host_id=fe_host_id,
                port=fe_port,
            )
            log.debug("network config: %s", self.net_conf)
            fe_ip_addr = f"{fe_ip_addr}:{fe_port}"  # it includes the port now, similar to other types of launch

            try:
                fe_ext_ip_addr = os.getenv("DRAGON_FE_EXTERNAL_IP_ADDR", fe_ip_addr.split(":")[0])
            except OSError:
                fe_ext_ip_addr = None

            # this will exist even w/o external access
            head_node_ip_addr = (
                self.net_conf["0"].ip_addrs[0].split(":")[0]
            )  # TODO: again grabbing 0th ip addr is fragile needs fix!
            os.environ["DRAGON_HEAD_NODE_IP_ADDR"] = head_node_ip_addr

            if fe_ext_ip_addr is not None:
                os.environ["DRAGON_FE_EXTERNAL_IP_ADDR"] = fe_ext_ip_addr
                if self._wlm is not WLM.K8S:
                    # DRAGON_RT_UID is not working as is for k8s; more changes need to be made
                    os.environ["DRAGON_RT_UID"] = str(
                        rt_uid_from_ip_addrs(fe_ext_ip_addr, head_node_ip_addr, self.port)
                    )

        # If we have any mods we need to make to the net_conf, do it now
        self.net_conf = self._define_node_pools(self.net_conf)

        # Acquire the primary node and add
        # the frontend info to host_ids and ip_addrs, so the TA functions
        host_ids, ip_addrs, hostnames, min_nics_per_node = self._define_overlay_network(
            self.net_conf, fe_host_id, fe_ip_addr
        )

        log.debug("ip_addrs=%s, host_ids=%s, hostnames=%s", ip_addrs, host_ids, hostnames)
        log.debug("Found %s NICs per node.", min_nics_per_node)
        log.debug("standing up tcp agent with gw: %s", encoded_ser_gw_str)

        self._STATE = FrontendState.OVERLAY_STARTING

        try:
            self.over_proc = start_overlay_network(
                overlay_transport=self.overlay_transport,
                ch_in_sdesc=self.local_out_q.serialize(),
                ch_out_sdesc=self.local_in_q.serialize(),
                log_sdesc=self.logging_queue.serialize(),
                host_ids=host_ids,
                ip_addrs=ip_addrs,
                overlay_port=self.overlay_port,
                frontend=True,
                env=os.environ,
            )
        except Exception as e:
            log.fatal("transport agent launch failed on FE")
            raise RuntimeError("Overlay transport agent launch failed on launcher frontend") from e

        # Wait on a started message
        self._wait_on_overlay_init()

        # Start a thread for monitoring messages into the logging channel
        # so we can see what comes out of the overlay network
        self.recv_logs_from_overlaynet_thread = dlutil.ServiceThread(
            name="Logging Monitor", target=self.recv_log_msgs_from_overlaynet, daemon=False
        )
        self.recv_logs_from_overlaynet_thread.start()

        # Start the backend
        log.debug("standing up backend")
        # we need to send to backend only the ip_addr and host_id of the frontend
        log.debug("fe_ip_addr=%s, fe_host_id=%s", fe_ip_addr, fe_host_id)

        # Send/recv data with my OverlayNet FE server
        send_overlaynet_args = (self.la_fe_stdout,)
        self.send_overlaynet_thread = dlutil.ServiceThread(
            name="OverlayNet Sender", target=self._send_msgs_to_overlaynet, args=send_overlaynet_args, daemon=False
        )
        self.send_overlaynet_thread.start()
        log.info("started overlaynet sender thread")
        if self._sigint_trigger == 3:
            signal.raise_signal(signal.SIGINT)

        recv_overlaynet_args = (self.la_fe_stdin,)
        self.recv_overlaynet_thread = dlutil.ServiceThread(
            name="OverlayNet Receiver", target=self.recv_msgs_from_overlaynet, args=recv_overlaynet_args, daemon=False
        )
        self.recv_overlaynet_thread.start()

        if self._sigint_trigger == 4:
            signal.raise_signal(signal.SIGINT)

        if self._wlm is not WLM.K8S:
            # About to actually launch backend
            log.debug("about to launch backend")
            # we need to send to backend only the ip_addr and host_id of the frontend
            log.debug("fe_ip_addr=%s, fe_host_id=%s", fe_ip_addr, fe_host_id)

            self._STATE = FrontendState.STARTUP

            # we need to put the telemetry level in the env before the backend is launched so that it is in both telemetry and user space
            os.environ["DRAGON_TELEMETRY_LEVEL"] = str(self.telemetry_level)

            try:
                self.wlm_proc = self._launch_backend(
                    nnodes=self.nnodes,
                    nodelist=hostnames,
                    fe_ip_addr=fe_ip_addr,
                    fe_host_id=fe_host_id,
                    frontend_sdesc=encoded_inbound_str,
                    network_prefix=self.network_prefix,
                    node_ip_addrs=ip_addrs[1:],
                )  # TODO: again skipping 0th ip addr is fragile needs fix!
            except Exception as e:
                log.fatal("FE failed to stand up BE")
                log.debug("error: %s", repr(e))
                raise RuntimeError("Backend launch failed from launcher frontend") from e

            log.debug("LA BE started on each compute node")
            self._STATE = FrontendState.BACKEND_LAUNCHED

            if self._sigint_trigger == 5 or self._sigint_trigger == 8:
                signal.raise_signal(signal.SIGINT)

        # Receive BEIsUp msg - Try getting a backend channel descriptor
        be_ups = self._recv_startup_msgs(dmsg.BEIsUp, self.nnodes, what="BEIsUp")
        assert len(be_ups) == self.nnodes

        # Construct the number of backend connections based on
        # the hierarchical bcast info and send FENodeIdxBE to those
        # nodes
        log.info("received %s BEIsUp msgs", self.nnodes)
        self.conn_outs = self.construct_bcast_tree(self.net_conf, be_ups, encoded_inbound_str)
        del be_ups

        chs_up = self._recv_startup_msgs(dmsg.LSChannelsUp, self.nnodes, what="LSChannelsUp")
        log.info("received %s SHChannelsUP msgs", self.nnodes)

        # Replace dynamically discovered IP addresses with those specified in network config.
        for ch_up in chs_up:
            try:
                conveyed_ip_addrs = self.net_conf[ch_up.net_conf_key].ip_addrs
                if conveyed_ip_addrs:
                    ch_up.node_desc.ip_addrs[:] = [ipaddr.split(":", 1)[0] for ipaddr in conveyed_ip_addrs]
            except Exception:
                pass

        nodes_desc = {ch_up.idx: ch_up.node_desc for ch_up in chs_up}
        gs_qds = [ch_up.gs_qd for ch_up in chs_up if ch_up.gs_qd is not None]
        if len(gs_qds) == 0:
            print(
                "The Global Services CD was not returned by any of the LSChannelsUp messages. Launcher Exiting.",
                flush=True,
            )
            sys.exit(LAUNCHER_FAIL_EXIT)
        gs_qd = gs_qds[0]

        # Set the number of gateway channels per node. When HSTA is used and multiple
        # NICs per node are available, we can use multi-nic support. In this case, the
        # number of gateway channels will control the number of HSTA Agents, with each
        # agent binding to a separate NIC. All other configurations will have one NIC
        # per node and one gateway per node (for now anyway - eventually the number of
        # gateways may not be tied to multiple NIC support).
        if self.transport is dfacts.TransportAgentOptions.HSTA:
            try:
                num_nics = int(os.environ["DRAGON_HSTA_NUM_NICS"])
                num_gw_channels = min(num_nics, min_nics_per_node)
            except KeyError:
                num_gw_channels = min_nics_per_node
        else:
            num_gw_channels = 1

        # HSTA uses NUM_GW_TYPES gateways per agent
        if self.transport is dfacts.TransportAgentOptions.HSTA:
            num_gw_channels *= dfacts.NUM_GW_TYPES

        # Send LAChannelsInfo in a test environment or to all
        la_ch_info = dmsg.LAChannelsInfo(
            tag=dlutil.next_tag(),
            nodes_desc=nodes_desc,
            gs_qd=gs_qd,
            num_gw_channels=num_gw_channels,
            port=self.port,
            transport=str(self.transport),
            fe_ext_ip_addr=fe_ext_ip_addr,
            attach_auth_token=self.attach_auth_token,
        )
        log.debug("la_fe la_channels.nodes_desc: %s", la_ch_info.nodes_desc)
        log.debug("la_fe la_channels.gs_qd: %s", la_ch_info.gs_qd)
        log.debug("la_fe la_channels.transport: %s", la_ch_info.transport)
        self.la_fe_stdout.send("A", la_ch_info.serialize())
        log.info("sent LACHannelsInfo to overlaynet fe")

        self.tas_up = self._recv_startup_msgs(dmsg.TAUp, self.nnodes, what="TAUp")
        log.info("received %s TAUp messages", self.nnodes)

        if self._sigint_trigger == 6:
            signal.raise_signal(signal.SIGINT)

        log.info("Now waiting on getting GSIsUp....")
        (gs_up,) = self._recv_startup_msgs(dmsg.GSIsUp, what="GSIsUp")
        log.info("la_fe received GSIsUp. Prepping launch of user application")

        # Infrastructure is up
        self._STATE = FrontendState.STOOD_UP

        return self.net_conf

    def _start_head_process(self, args_map, *, logger_name, process_name, restart=False, resilient=False):
        self._STATE = FrontendState.APP_EXECUTION
        log = logging.getLogger(dls.LA_FE).getChild(logger_name)
        log.info("Prepping launch of %s", process_name)
        start_msg = dlutil.mk_head_proc_start_msg(
            logbase=dls.LA_FE,
            make_inf_channels=True,
            args_map=args_map,
            restart=restart,
            resilient=resilient,
        )
        self.la_fe_stdout.send("P", start_msg.serialize())
        if self._sigint_trigger == 7:
            signal.raise_signal(signal.SIGINT)
        log.info("transmitted GSProcessCreate")

        return self.net_conf

    def run_telem(self, level: int = 0):
        """Start telem app execution via GSProcessCreate or LSProcessCreate"""
        telem_map = copy.deepcopy(self.args_map)
        telem_map["prog"] = sys.executable
        telem_map["args"] = [
            "-c",
            "from dragon.telemetry.telemetry_head import start_telemetry; start_telemetry()",
        ]
        return self._start_head_process(
            telem_map,
            logger_name="run_telem`",
            process_name="telemetry application",
        )

    def run_app(self, restart=False):
        """Start user app execution via GSProcessCreate or LSProcessCreate"""
        return self._start_head_process(
            self.args_map,
            logger_name="run_app",
            process_name="user application",
            restart=restart,
            resilient=self.resilient,
        )

    def run_msg_server(self):
        """Process messages from backend after user app starts"""
        self.msg_log = logging.getLogger(dls.LA_FE).getChild("run_msg_server")
        running = True
        execute_teardown = True

        while running:
            self.msg_log.debug(f"frontend msg server waiting for message")
            msg = dlutil.get_with_blocking(self.la_fe_stdin)

            if hasattr(msg, "r_c_uid"):
                msg.r_c_uid = dfacts.launcher_cuid_from_index(self.node_idx)
            self.msg_log.info("run_msg_server received %s", type(msg))

            try:
                if type(msg) in LauncherFrontEnd._DTBL:
                    self._DTBL[type(msg)][0](self, msg=msg)
                else:
                    # TODO: Make sure handling abnormal termniation works this way
                    self.msg_log.warning("unexpected msg type in the Frontend Launcher: %s", repr(msg))
            except LauncherImmediateExit:
                self.msg_log.debug("completing a sys.exit(%s)", LAUNCHER_FAIL_EXIT)
                sys.exit(LAUNCHER_FAIL_EXIT)
            except SigIntImmediateExit:
                self.msg_log.debug("completing an exit due to SIGINT")
                execute_teardown = False
                running = False
                break

            # Break and wait on threads to exit
            if self._sh_halt_be == self.nnodes:
                running = False

        self.msg_log.debug("out of msg_server loop")
        if execute_teardown:
            self.msg_log.info("joining on message threads")
            self._close_threads(abnormal=self._abnormal_termination.is_set())
            self.msg_log.info("la_fe has shutdown overlaynet send/recv threads")

            if self._wlm is not WLM.K8S:
                # Waiting on teardown of wlm launched backend
                self.wlm_proc.wait()

            # TEARDOWN
            self.msg_log.info("WLM launched backend down. Tearing down comm infra")
            self._close_comm_overlaynet()
            self.msg_log.info("Leaving run_msg_server")

        if self._sigint_count > 0:
            self.msg_log.debug("raising keyboard interrupt")
            raise KeyboardInterrupt

        # Set state so our exit method correctly executes.
        self._STATE = FrontendState.LAUNCHER_DOWN

        return self.net_conf

    def probe_teardown(self):
        """Check on whether to begin teardown based on received backend messages"""
        # Global services is up and we're using it
        if self._gs_head_exit_received == self._gs_process_create_resp_received and self._gs_head_exit_received > 0:
            # m4.1 Send GSTeardown to Primary BE

            # Only update this state if we're not in an abnormal teardown. Too many
            # subsequent state checks depend on that differentiation
            if self._STATE != FrontendState.ABNORMAL_TEARDOWN:
                self._STATE = FrontendState.TEARDOWN

            gs_teardown = dmsg.GSTeardown(tag=dlutil.next_tag())
            self.la_fe_stdout.send("P", gs_teardown.serialize())
            self.msg_log.info("m4.1 la_fe transmitted teardown msg to BE")
        else:
            self.msg_log.debug(
                "gs_head_exit: %s | gs_proc_create_resp: %s",
                self._gs_head_exit_received,
                self._gs_process_create_resp_received,
            )

    @route(dmsg.LSFwdOutput, _DTBL)
    def handle_sh_fwd_output(self, msg: dmsg.LSFwdOutput):
        msg_out = self.build_stdmsg(msg, self.args_map, msg.fd_num == dmsg.LSFwdOutput.FDNum.STDOUT.value)
        self.msg_log.debug("%s", msg)
        print(msg_out, end="", flush=True)

    @route(dmsg.LAExit, _DTBL)
    def handle_la_exit(self, msg: dmsg.LAExit):
        self.msg_log.debug("Received LAExit message. Will break out of message loop")
        if msg.sigint:
            self.msg_log.debug("Exiting due to receipt of SIGINT")
            raise SigIntImmediateExit("Exiting due to receipt of SIGINT")
        else:
            self.msg_log.debug("Exiting due to reception of %s", type(msg))
            raise LauncherImmediateExit(f"Exiting due to reception of {type(msg)}")

    @route(dmsg.ExceptionlessAbort, _DTBL)
    def handle_exceptionless_abort(self, msg: dmsg.ExceptionlessAbort):
        self.msg_log.debug("exceptionless abort routing")
        self._cleanup_abnormal_state()

    @route(dmsg.GSHeadExit, _DTBL)
    def handle_gs_head_exit(self, msg: dmsg.GSHeadExit):
        self.msg_log.info("The head process has exited")
        self._gs_head_exit_received += 1
        self.probe_teardown()

    @route(dmsg.GSProcessCreateResponse, _DTBL)
    def handle_gs_proc_create_response(self, msg: dmsg.GSProcessCreateResponse):
        if dmsg.GSProcessCreateResponse.Errors.SUCCESS == msg.err:
            self._gs_process_create_resp_received += 1
            self.msg_log.info("The GSProcessCreateResponse was received in the launcher front end")
            self.probe_teardown()
        elif dmsg.GSProcessCreateResponse.Errors.FAIL == msg.err:
            self.msg_log.warning("Unable to start the head process %s", msg.err_info)
            self._cleanup_abnormal_state(sigint=False)

    @route(dmsg.LSProcessCreateResponse, _DTBL)
    def handle_sh_proc_create_response(self, msg: dmsg.LSProcessCreateResponse):
        self._proc_create_resps += 1
        self.msg_log.info("Got %s LSProcessCreateResponse messages", self._proc_create_resps)
        self.probe_teardown()

    @route(dmsg.LSProcessExit, _DTBL)
    def handle_sh_proc_exit(self, msg: dmsg.LSProcessExit):
        self._proc_exits += 1
        self.msg_log.info("Got %s LSProcessExit messages", self._proc_exits)
        self.probe_teardown()

    @route(dmsg.GSHalted, _DTBL)
    def handle_gs_halted(self, msg: dmsg.GSHalted):
        # m6.2 recv GSHalted from Primary BE
        self.msg_log.info("m6.2 la_fe received GSHalted")

        # m7.1 Send LSHaltTA to All BEs
        sh_halt_ta = dmsg.LSHaltTA(tag=dlutil.next_tag())

        try:
            if self._STATE != FrontendState.ABNORMAL_TEARDOWN:
                self.la_fe_stdout.send("A", sh_halt_ta.serialize())
                self.msg_log.info("m7.1 transmitted LSHaltTA msg to BE via threads: %s", self._STATE)
            else:
                self._overlay_bcast(sh_halt_ta)
                self.msg_log.info("m7.1 transmitted LSHaltTA msg to BE directly")
        except Exception as e:
            self.msg_log.warning("Exception sending LSHaltTA %s", e)

    @route(dmsg.TAHalted, _DTBL)
    def handle_ta_halted(self, msg: dmsg.TAHalted):
        self._tas_halted += 1
        if self._tas_halted == self.nnodes:
            self.msg_log.info("m10.2 la_fe received All TAHalted messages")
            sh_teardown = dmsg.LSTeardown(tag=dlutil.next_tag())

            if self._STATE != FrontendState.ABNORMAL_TEARDOWN:
                # m11.1 Send LSTeardown to all BE's
                self.la_fe_stdout.send("A", sh_teardown.serialize())
                self.msg_log.info("m11.1 la_fe transmitted LSTeardown msg to BE via threads")

            try:
                self._overlay_bcast(sh_teardown)
                self.msg_log.info("m11.1 la_fe transmitted LSTeardown msg to BE directly")
            except Exception:
                pass

            # Handle one of my sigint test cases
            try:
                if self._sigint_trigger == 9:
                    self.msg_log.debug("raising signal during teardown")
                    self._sigint_trigger = None
                    signal.raise_signal(signal.SIGINT)
                    self.msg_log.debug("SIGINT raised teardown")
            except TypeError:
                pass

    @route(dmsg.LSHaltBE, _DTBL)
    def handle_sh_halt_be(self, msg: dmsg.LSHaltBE):
        self._sh_halt_be += 1
        if self._sh_halt_be == self.nnodes:
            self.msg_log.info("Received all LSHaltBE messages")

            be_halted = dmsg.BEHalted(tag=dlutil.next_tag())

            if self._STATE != FrontendState.ABNORMAL_TEARDOWN:
                self.la_fe_stdout.send("A", be_halted.serialize())
                self.msg_log.info(
                    "m14 transmitted BEHalted msg to BE to be forwarded to local services on all nodes via threads"
                )
                self._shutdown.set()

                # signal the end of the logging thread by sending its way
                # an halt message
                halt_logging_msg = dmsg.HaltLoggingInfra(tag=dlutil.next_tag())
                self.logging_queue.put(halt_logging_msg)

                self.conn_in_bd.put(be_halted)
                self.msg_log.info("sent BEHalted to break overlay recv out of loop")

            else:
                self._overlay_bcast(be_halted)
                self.msg_log.info(
                    f"m14 transmitted BEHalted msg to BE to be forwarded to local services on all nodes directly"
                )

    def recv_log_msgs_from_overlaynet(self):
        """This service grabs messages from overlay logger and logs them in this

        Args:
            level (int): minimum log priority of messages to forward
        """
        _, level = _get_dragon_log_device_level(os.environ, LOGGING_OUTPUT_DEVICE_DRAGON_FILE)
        log = logging.getLogger(dls.LA_FE).getChild("recv_overlay_logs")
        try:
            # Set timeout to None which allows better interaction with the GIL
            while not self._shutdown.is_set():
                try:
                    msg = self.logging_queue.get(level, timeout=None)
                    if isinstance(msg, dmsg.HaltLoggingInfra):
                        break
                    elif isinstance(msg, dmsg.CpLoggingMessage):
                        _log = logging.getLogger(msg.name)
                        _log.log(msg.level, msg.msg, extra=msg.get_logging_dict())
                    else:
                        log.warning("Unexpected message type: %s", type(msg))

                except ChannelEmpty:
                    pass

        except Exception as ex:  # pylint: disable=broad-except
            log.warning("Caught exception %s in logging thread", ex)
        log.info("exiting recv log msg thread")

    def send_msg_to_overlaynet(self, target, msg):
        """Send single message to backend

        Args:
            target (str): "A" for all backend nodes. "P" for primary node only
            msg (dmsg.InfraMsg): non-serialized message to send
        """
        try:
            self.la_fe_stdout.send(target, msg.serialize())
        except Exception:
            raise RuntimeError("Unable to send message to overlaynet send thread")

    def _overlay_bcast(self, msg: dmsg.InfraMsg):
        """Send bcast of message to all backend nodes via overlay network

        :param msg: Message to send
        :type msg: dmsg.InfraMsg
        """

        for conn_out in self.conn_outs.values():
            conn_out.put(msg)

    def _send_msgs_to_overlaynet(self, la_fe_stdout: dlutil.LaOverlayNetFEQueue):
        """Thread that send Messages from frontend to backend service

        Args:
            la_fe_stdout (dlutil.LaOverlayNetFEQueue): SimpleQueue sending messages from launcher
                FE main thread to this OverlayNet server thread
        """
        log = logging.getLogger(dls.LA_FE).getChild("send_msgs_to_overlaynet")

        try:
            # recv msg from parent thread to send to backend
            while not self._shutdown.is_set():
                target, fe_msg = dlutil.get_with_blocking(la_fe_stdout)
                if isinstance(fe_msg, dmsg.HaltOverlay):
                    self._shutdown.set()
                    break

                if target == "P":
                    # node index 0 is the primary node
                    self.conn_outs[0].put(fe_msg)
                    log.info("target = %s -- msg = %s", target, fe_msg.__class__)
                else:
                    self._overlay_bcast(fe_msg)
                    log.info("sent %s down OverlayNet tree", fe_msg.__class__)

                if isinstance(fe_msg, dmsg.BEHalted):
                    log.info("overlaynet send thread got halt msg")
                    self._shutdown.set()
                    break
        except Exception as err:
            log.exception("overlaynet sending thread failure: %s", err)

        # Let the OverlayNet receiving thread know we're gone, and it can exit
        self._shutdown.set()

        # signal the end of the logging thread by sending its way
        # a HaltLoggingInfra message
        msg = dmsg.HaltLoggingInfra(tag=dlutil.next_tag())
        self.logging_queue.put(msg)

        log.info("overlaynet sending thread exiting ...")

    def recv_msgs_from_overlaynet(self, la_fe_stdin: dlutil.OverlayNetLaFEQueue):
        """ "Start and run OverlayNet FrontEnd service

        Args:
            la_fe_stdin (dlutil.OverlayNetLaFEQueue): SimpleQueue sending messages from
                OverlayNetServer thread to launcher FE main thread
        """

        # Get the logger I will use to write my own log messages
        log = logging.getLogger(dls.LA_FE).getChild("recv_msgs_from_overlaynet")

        try:
            # Just recv messages and send them to my parent thread
            # until my parent thread says stop
            while not self._shutdown.is_set():
                # send msg from callback to parent

                # the get_with_timeout is a decorated function (i.e. wrapped function
                # in a function) that filters out all the log messages coming up through
                # overlaynet. So we don't worry about them here. See dlutil for details.
                be_msg = dlutil.get_with_blocking_frontend_server(self.conn_in)

                if isinstance(be_msg, dmsg.AbnormalTermination):
                    # I need to get this to the main thread without throwing an exception
                    log.debug("Got AbnormalTermination")
                    self._abnormal_termination.set()

                    # The backend's err_info is the root cause; everything the
                    # frontend trips over afterwards is fallout from it.
                    self._record_error(getattr(be_msg, "err_info", ""), host_id=be_msg.host_id)

                    # Update the network configuration to reflect the down state of whatever node
                    # we got this error from
                    log.debug("Abort found from backend node: %s", be_msg.host_id)
                    if isinstance(be_msg.host_id, int):
                        set_down = [be_msg.host_id]
                    else:
                        set_down = be_msg.host_id

                    self._set_node_to_down_state(set_down)

                    if self._STATE < FrontendState.TEARDOWN:
                        log.debug("Frontend sending an ExceptionlessAbort to backend")
                        be_msg = dmsg.ExceptionlessAbort(tag=dlutil.next_tag())
                    else:
                        log.debug(
                            "Caught AbnormalTermination during teardown. Skipping it in hopes of completing teardown"
                        )
                        continue

                if isinstance(be_msg, dmsg.HaltOverlay):
                    self._shutdown.set()
                    break

                # We could also use decorator here, but not yet...
                if isinstance(be_msg, dmsg.BEHalted):
                    log.info("overlaynet recv thread got halt msg")
                    break
                else:
                    log.info("Got %s in the front end monitor and forwarding to launcher front end.", be_msg.__class__)
                    la_fe_stdin.send(be_msg.serialize())

            # Let the OverlayNet sending thread know we're gone
            self._shutdown.set()

            # signal the end of the logging thread by sending its way
            # a Halt message
            msg = dmsg.HaltLoggingInfra(tag=dlutil.next_tag())
            self.logging_queue.put(msg)

            log.info("Overlaynet recv thread exiting.")
        except Exception as err:
            log.exception("Overlaynet recving thread failure. Exiting: %s", err)
            la_fe_stdin.send(dmsg.AbnormalTermination(tag=dlutil.next_tag()).serialize())
            log.debug("send abnormal term from except block")
            raise

    def build_stdmsg(self, msg: dmsg.LSFwdOutput, arg_map, is_stdout=True):
        if arg_map["no_label"]:
            return f"{msg.data}"

        msg_str = ""
        if is_stdout:
            msg_str += "[stdout: "
        else:
            msg_str += "[stderr: "

        if arg_map["verbose_label"]:
            msg_str += f"PID {msg.pid} @ {msg.hostname}]"
        elif arg_map["basic_label"]:
            msg_str += f"Dragon PID {msg.p_uid}]"

        msg_str += f" {msg.data}"
        return msg_str
