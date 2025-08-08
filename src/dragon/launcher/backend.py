import os
import logging
import threading
import re
import subprocess
import signal
from enum import Enum
from functools import total_ordering
from time import sleep


from ..utils import B64, host_id as get_host_id, host_id_from_k8s, set_host_id
from ..managed_memory import MemoryPool, DragonPoolError, DragonMemoryError
from ..channels import Channel, register_gateways_from_env, ChannelError, discard_gateways

from . import util as dlutil
from ..transport.overlay import start_overlay_network

from .wlm.k8s import KubernetesNetworkConfig

from ..dlogging.util import setup_dragon_logging, setup_BE_logging, detach_from_dragon_handler
from ..dlogging.util import DragonLoggingServices as dls
from ..dlogging.logger import DragonLogger, DragonLoggingError

from ..infrastructure import facts as dfacts
from ..infrastructure import messages as dmsg
from ..infrastructure.node_desc import NodeDescriptor
from ..infrastructure.connection import Connection, ConnectionOptions
from ..infrastructure.parameters import POLICY_INFRASTRUCTURE, this_process
from ..infrastructure.util import NewlineStreamWrapper, route

from ..infrastructure.watchers import CriticalPopen


def _get_host_info(network_prefix) -> tuple[str, str, list[str]]:
    """Return username, hostname, and list of IP addresses."""
    from dragon.transport.ifaddrs import getifaddrs, InterfaceAddressFilter

    log = logging.getLogger(dls.LA_BE).getChild("get_host_info")
    _user = os.environ.get("USER", str(os.getuid()))

    ifaddr_filter = InterfaceAddressFilter()
    ifaddr_filter.af_inet(inet6=False)  # Disable IPv6 for now
    ifaddr_filter.up_and_running()
    try:
        ifaddrs = list(filter(ifaddr_filter, getifaddrs()))
    except OSError:
        log.exception("Failed to get network interface addresses")
        raise
    if not ifaddrs:
        _msg = "No network interface with an AF_INET address was found"
        log.error(_msg)
        raise RuntimeError(_msg)

    try:
        re_prefix = re.compile(network_prefix)
    except (TypeError, NameError):
        _msg = "expected a string regular expression for network interface network_prefix"
        log.error(_msg)
        raise RuntimeError(_msg)

    ifaddr_filter.clear()
    ifaddr_filter.name_re(re_prefix)
    ip_addrs = [ifa["addr"]["addr"] for ifa in filter(ifaddr_filter, ifaddrs)]
    if not ip_addrs:
        _msg = f"No IP addresses found matching regex pattern: {network_prefix}"
        log.error(_msg)
        raise RuntimeError(_msg)
    log.info(f"Found IP addresses: {','.join(ip_addrs)}")

    return _user, ip_addrs


@total_ordering
class BackendState(Enum):
    """Enumerated states of Dragon LauncherBackEnd"""

    INITIALIZING = 0
    OVERLAY_STARTING = 1
    OVERLAY_UP = 2
    OVERLAY_THREADS_UP = 3
    LS_STARTING = 4
    LS_UP = 5
    TA_UP = 6
    BACKEND_UP = 7
    TEARDOWN = 8
    BACKEND_DOWN = 9

    def __lt__(self, other):
        if self.__class__ is other.__class__:
            return self.value < other.value

    def __gt__(self, other):
        if self.__class__ is other.__class__:
            return self.value > other.value

    def __eq__(self, other):
        if self.__class__ is other.__class__:
            return self.value == other.value


class LauncherBackEnd:
    """State manager for Dragon BackEnd

    Returns:
        LauncherBackEnd: instance of class
    """

    _DTBL = {}  # dispatch router for msgs, keyed by type of message

    def __init__(self, transport_test_env, network_prefix, overlay_port):

        self._state = BackendState.INITIALIZING
        self._abnormally_terminating = False

        self.transport_test_env = bool(transport_test_env)
        self.network_prefix = network_prefix
        self.overlay_port = overlay_port

        # The la_be_stdin queue is the connection between this main thread
        # (i.e. the launcher backend) and the OverlayNet down the tree thread.
        # The down the tree thread writes to this queue when communicating
        # a message sent from the front end to this queue.
        self.la_be_stdin = dlutil.SRQueue()

        # The la_be_stdout queue is the connection between this main thread
        # (i.e. the launcher backend) and the OverlayNet up the tree thread.
        # This thread writes to this queue when it wants to send something
        # to the launcher front end.
        self.la_be_stdout = dlutil.SRQueue()

        # This shutdown value is used to control when to bring down the OverlayNet
        # and its threads for routing values up and down the tree.
        self._shutdown = threading.Event()
        self._logging_shutdown = threading.Event()

        # Only the primary launcher backend communicates with GlobalServices.
        # Initialize gs_channel and gs_queue to None to enable us to verify that
        # they are properly initialized before use.
        self.gs_channel = None
        self.gs_queue = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):

        log = logging.getLogger("__exit__")
        self._cleanup()

        # If we were exiting under pretense of an abnormal termination, raise that error
        log.debug("cleaned up backend")
        if self._abnormally_terminating:
            # logging here creates a race condition that
            # occasionally causes an exception. Don't log it.
            raise RuntimeError("Abnormal exit detected")

    def _cleanup(self):
        """General purpose clean-up all the crap in this object method"""

        log = logging.getLogger(dls.LA_BE).getChild("_cleanup")
        # handle server threads. Assume it's abnormal because if they're not closed
        # before here, they should be
        log.debug("cleaning up threads")
        self._close_threads(abnormal=True)

        # Make sure our LS is down
        try:
            log.debug("waiting on LS")
            self.ls_proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            log.debug("killing LS")
            self.ls_proc.kill()
        except AttributeError:
            pass

        log.debug("closing connections")
        self._close_conns_chs_pools()

        log.debug("backend down")
        self._state = BackendState.BACKEND_DOWN

    def _close_threads(self, abnormal=False):
        """Join on the launcher backend threads"""

        if self._state == BackendState.BACKEND_DOWN:
            return

        log = logging.getLogger(dls.LA_BE).getChild("_close_threads")
        halt_overlay_msg = dmsg.HaltOverlay(tag=dlutil.next_tag())
        halt_logging_msg = dmsg.HaltLoggingInfra(tag=dlutil.next_tag())

        try:
            if self.recv_msgs_from_overlaynet_thread.is_alive():
                if abnormal:
                    log.info("abnormal closing of recv overlay thread")
                    self._shutdown.set()
                    self.infra_in_bd.send(halt_overlay_msg.serialize())
                self.recv_overlaynet_thread.join()
        except AttributeError:
            pass
        log.info("recv_overlaynet_thread joined")

        try:
            if self.send_msgs_to_overlaynet_thread.is_alive():
                if abnormal:
                    self.la_be_stdout.send(halt_overlay_msg.serialize())
                self.recv_overlaynet_thread.join()
        except AttributeError:
            pass
        log.info("send_overlaynet_thread joined")

        try:
            if self.send_log_to_overlaynet_thread.is_alive():
                # Send it a message to make sure it can get out
                self._logging_shutdown.set()
                self.dragon_logger.put(halt_logging_msg.serialize())
            self.send_log_to_overlaynet_thread.join()
        except AttributeError:
            pass
        log.info("send_logs_to_overlaynet_thread joined")

        try:
            if self.channel_monitor_thread.is_alive():
                if abnormal:
                    self.la_queue_bd.send(halt_overlay_msg.serialize())
                self.recv_overlaynet_thread.join()
        except AttributeError:
            pass
        log.info("channel_monitor_thread joined")

    def _close_conns_chs_pools(self):
        """Clean up all communication-based data structures:"""

        if self._state == BackendState.BACKEND_DOWN:
            return

        log = logging.getLogger(dls.LA_BE).getChild("_close_conns")
        try:
            log.debug("local_inout close")
            self.local_inout.close()
        except Exception:
            pass

        try:
            log.debug("local_in close")
            self.local_in.close()
        except Exception:
            pass

        try:
            log.debug("local_in_be close")
            self.local_in_be.close()
        except Exception:
            pass

        try:
            log.debug("local_out close")
            self.local_out.close()
        except Exception:
            pass

        try:
            log.debug("ls_queue close")
            self.ls_queue.close()
        except Exception:
            pass

        try:
            log.debug("la_queue close")
            self.la_queue.close()
        except Exception:
            pass

        try:
            log.debug("la_queue_bd close")
            self.la_queue_bd.close()
        except Exception:
            pass

        try:
            log.debug("frontend_fwd_conns close")
            for conn in self.frontend_fwd_conns.values():
                try:
                    conn.close()
                except Exception:
                    pass
        except Exception:
            pass

        try:
            log.debug("gs_queue close")
            self.gs_queue.close()
        except Exception:
            pass

        try:
            log.debug("be_inbound destroy")
            self.be_inbound.destroy()
        except Exception:
            pass

        try:
            log.debug("local_ch_in destroy")
            self.local_ch_in.destroy()
        except Exception:
            pass

        try:
            log.debug("local_ch_out destroy")
            self.local_ch_out.destroy()
        except Exception:
            pass

        try:
            log.debug("gw_ch destroy")
            self.gw_ch.destroy()
            discard_gateways()
            try:
                del os.environ[dfacts.GW_ENV_PREFIX + str(dfacts.DRAGON_OVERLAY_DEFAULT_NUM_GW_CHANNELS_PER_NODE)]
            except KeyError:
                pass
            log.debug("gw closed")
        except Exception:
            pass

        try:
            log.debug("ls_stdin close")
            self.ls_stdin.close()
        except AttributeError:
            pass

        try:
            log.debug("ls_stdout close")
            self.ls_stdout.close()
        except Exception:
            pass

        try:
            log.debug("dragon_logger destroy")
            self.dragon_logger.destroy()
        except (DragonLoggingError, AttributeError):
            pass

        try:
            log.debug("be_mpool destroy")
            self.be_mpool.destroy()
            del self.be_mpool
        except Exception:
            pass

    def _handle_abnormal_gsteardown(self, msg):
        """Send teardown messages through appropriate queues based on backend state"""
        log = logging.getLogger(dls.LA_BE).getChild("_handle_abnormal_gsteardown")

        if self._state < BackendState.OVERLAY_THREADS_UP:
            log.debug(f"sending {type(msg)} via frontend channel rather than send_overlay thread")
            self.infra_out.send(msg)
        else:
            log.debug(f"sending {type(msg)} message to send_overlay thread")
            self.la_be_stdout.send(msg)

    def _abnormal_termination(self, msg):
        """Clean up the backend when we've found ourselves in an indeterminate state"""
        log = logging.getLogger("Abnormal termination")

        self._abnormally_terminating = True

        log.debug("Begin handling of abnormal termination")

        # Handle the situation where our runtime services didn't get all the way up, but the Frontend
        # signaled a GSTeardown for whatever reason
        if self._state >= BackendState.OVERLAY_UP:

            # in this case we weren't able to route a GSTeardown to GS. So, we need to act on its
            # behalf in the teardown and just let LS kill it later.
            if isinstance(msg, dmsg.GSTeardown):
                self._handle_abnormal_gsteardown(dmsg.GSHalted(tag=dlutil.next_tag()).serialize())

            elif isinstance(msg, dmsg.SHHaltTA):
                self._handle_abnormal_gsteardown(dmsg.TAHalted(tag=dlutil.next_tag()).serialize())

            elif isinstance(msg, dmsg.SHTeardown):
                self._handle_abnormal_gsteardown(dmsg.SHHaltBE(tag=dlutil.next_tag()).serialize())

            elif isinstance(msg, dmsg.BEHalted):
                pass

    def _close_overlay_comms(self):

        log = logging.getLogger(dls.LA_BE).getChild("_close_overlay_comms")

        log.info("shutting down overlay tree agent")
        self.local_inout.send(dmsg.BEHaltOverlay(tag=dlutil.next_tag()).serialize())
        ta_halted = dlutil.get_with_blocking(self.local_inout)
        assert isinstance(ta_halted, dmsg.OverlayHalted)
        self.msg_log.info(f"received {ta_halted} from local TCP agent")
        try:
            self.tree_proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            self.tree_proc.kill()
        log.info("overlay tree down")

        # Clean up the resources needed for the
        # infrastructure transport agent
        log.info("destroying tcp channels")
        self.infra_out.close()
        log.debug("infra out closed")
        self.infra_in.close()
        log.debug("infra in closed")

        self.dragon_logger.destroy()
        log.debug("dragon_logger closed")

        # Close rest of infrastructure
        self.gw_ch.destroy()
        discard_gateways()
        log.debug("gateway closed")
        try:
            del os.environ[dfacts.GW_ENV_PREFIX + str(dfacts.DRAGON_OVERLAY_DEFAULT_NUM_GW_CHANNELS_PER_NODE)]
        except KeyError:
            pass
        self.local_inout.close()
        log.debug("local_inout closed")

        for conn in self.frontend_fwd_conns.values():
            conn.close()
        log.debug("closed all backend forwarding connections")

        try:
            self.local_ch_in.destroy()
            log.debug("local_ch_in destroyed")
        except Exception:
            pass

        try:
            self.local_ch_out.destroy()
            log.debug("local_ch_out destroyed")
        except Exception:
            pass

        try:
            self.be_inbound.destroy()
            log.debug("be_inbound destroyed")
        except Exception:
            pass

        log.info("overlay infra down")

    def _start_localservices(self):
        log = logging.getLogger(dls.LA_BE).getChild("start_localservices")

        log.info("standing up local services")
        try:
            ls_args = [dfacts.PROCNAME_LS]
            ls_proc = CriticalPopen(
                args=ls_args,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                name="local services",
                notify_channel=self.infra_out,
                notify_msg=dmsg.AbnormalTermination,
            )
        except Exception as ex:
            log.fatal("BE failed to stand up LS")
            raise RuntimeError("Local Services launch failed from launcher backend") from ex
        log.debug("LA backend started LS")

        # Return the file descriptors to make mocking easier
        return ls_proc, ls_proc.stdin.fileno(), ls_proc.stdout.fileno()

    def _get_my_child_nodes(self, index, nnodes, fanout):
        """Get the node IDs I need to forward bcast messages to

        :param index: my node index, assume 0-indexed counting (ie: start counting at 0)
        :type index: int
        :param nnodes: number of nodes on backend
        :type nnodes: int
        :param fanout: number of nodes a given node will send to
        :type fanout: int
        """

        return list(range(fanout * (index + 1), min(nnodes, fanout * (index + 1) + fanout)))

    def _construct_child_forwarding(self, node_info: dict[NodeDescriptor], net_conf_key_mapping: list[str]):
        """Set up any infrastructure messages I must forward to other backend nodes

        :param node_info: node indices and channel descriptors
        :type node_info: dict[NodeDescriptor]
        """

        log = logging.getLogger(dls.LA_BE).getChild("_construct_child_forwarding")

        # Figure out what nodes I need to forward to
        nnodes = len(node_info)
        fanout = this_process.overlay_fanout
        fwd_conns = {}

        # I need to work out what nodes I'm responsible for
        log.info(f"getting children for fan {fanout} and {nnodes} nodes. My idx = {self.node_idx}")
        ids_to_forward = self._get_my_child_nodes(self.node_idx, nnodes, fanout)
        if 0 == len(ids_to_forward):
            # my index is in the last tree layer, I don't need to forward
            log.info(f"{self.node_idx} is a leaf node")
            return fwd_conns

        log.info(f"{self.node_idx} forward to nodes {ids_to_forward}")

        conn_options = ConnectionOptions(default_pool=self.be_mpool, min_block_size=2**16)
        conn_policy = POLICY_INFRASTRUCTURE
        for idx in ids_to_forward:
            outbound = Channel.attach(
                B64.from_str(node_info[str(net_conf_key_mapping[idx])].overlay_cd).decode(), mem_pool=self.be_mpool
            )
            fwd_conns[idx] = Connection(outbound_initializer=outbound, options=conn_options, policy=conn_policy)
            fwd_conns[idx].ghost = True

        return fwd_conns

    def _sigterm_handler(self, *args):
        """Handle transmitting AbnormalTermination to frontend if SIGTERM comes from WLM"""

        log = logging.getLogger(dls.LA_BE).getChild("_sigterm_handler")

        # If overlay threads aren't up, wait till they are
        while self._state < BackendState.OVERLAY_THREADS_UP:
            sleep(0.1)

        # Now send an exceptionless abort which will get an AbnormalTerm to the frontend,
        # and then we're done
        log.debug("sending exceptionless abort due to SIGTERM signal")
        self.la_be_stdout.send(dmsg.ExceptionlessAbort(tag=dlutil.next_tag()).serialize())

    def run_startup(
        self,
        arg_ip_addr: str,
        arg_host_id: str,
        frontend_sdesc: B64,
        level=logging.INFO,
        fname=None,
        *,
        backend_ip_addr=None,
        backend_hostname=None,
    ):
        """Begin bringup of backend services

        :param arg_ip_addr: IP address of frontened with port appended
        :type arg_ip_addr: str
        :param arg_host_id: host ID of frontend
        :type arg_host_id: str
        :param frontend_sdesc: serialized channel descriptor for comms to frontend
        :type frontend_sdesc: B64
        :param level: logging level, defaults to logging.INFO
        :type level: int, optional
        :param fname: filename to log to, defaults to None
        :type fname: str, optional
        """
        log = logging.getLogger(dls.LA_BE).getChild("run_startup")

        # Take control of SIGTERM signal from WLM
        try:
            self._orig_sigterm = signal.signal(signal.SIGTERM, self._sigterm_handler)
            log.debug("got sigterm signal handling in place")
        except ValueError:
            # this error is thrown if we are running inside a child thread
            # which we do for unit tests. So pass on this
            log.debug("Unable to do signal handling outside of main thread")

        is_k8s = (os.getenv("KUBERNETES_SERVICE_HOST") and os.getenv("KUBERNETES_SERVICE_PORT")) != None

        if is_k8s:
            self.be_label_selector = f"app={os.getenv('BACKEND_JOB_LABEL')}"
            self.kubernetes = KubernetesNetworkConfig()

            frontend_sdesc = B64.from_str(os.getenv("DRAGON_FE_SDESC"))
            fe_label_selector = f"app={os.getenv('FE_LABEL_SELECTOR')}"
            self.hostname = os.getenv("HOSTNAME")
            pod_uid = os.getenv("POD_UID")
            self.host_id = get_host_id()
            self.port = int(os.getenv("BACKEND_OVERLAY_PORT"))
            self.ip_addr = f"{os.getenv('POD_IP')}:{self.port}"

            log.debug(
                f"be ipaddr: {self.ip_addr} --- be host_id: {self.host_id} --- host_id_from_k8s(pod_uid): {host_id_from_k8s(pod_uid)}"
            )
            self.transport_agent_ipaddr = backend_ip_addr or self.ip_addr
        else:
            # Propagate information passed on command-line or discover if needed.
            if not (backend_hostname and backend_ip_addr):
                log.debug(f"Before calling NodeDescriptor.get_cached_local_network_conf")
                result, net_conf = NodeDescriptor.get_cached_local_network_conf()
                log.debug("After calling NodeDescriptor.get_cached_local_network_conf (%s, %s)", result, net_conf)
                if not result:
                    log.debug("Before calling NodeDescriptor.get_local_node_network_conf()")
                    net_conf = NodeDescriptor.get_local_node_network_conf(
                        network_prefix=self.network_prefix, port_range=self.overlay_port
                    )
                self.host_id = int(net_conf.host_id)
                log.debug("Before calling NodeDescriptor.remove_cached_local_network_conf()")
                NodeDescriptor.remove_cached_local_network_conf()
                log.debug("After calling NodeDescriptor.remove_cached_local_network_conf()")
            else:
                self.host_id = get_host_id()
            self.hostname = backend_hostname or net_conf.name
            self.transport_agent_ipaddr = backend_ip_addr or net_conf.ip_addrs[0]

        conn_options = ConnectionOptions(min_block_size=2**16)
        conn_policy = POLICY_INFRASTRUCTURE
        try:
            local_cuid_in = dfacts.be_local_cuid_in_from_hostid(self.host_id)
            local_cuid_out = dfacts.be_local_cuid_out_from_hostid(self.host_id)
            gw_cuid = dfacts.be_gw_cuid_from_hostid(self.host_id)
            be_cuid = dfacts.be_fe_cuid_from_hostid(self.host_id)

            # Create my memory pool
            self.be_mpool = MemoryPool(
                int(dfacts.DEFAULT_BE_OVERLAY_TRANSPORT_SEG_SZ),
                f"{os.getuid()}_{os.getpid()}_{self.host_id}" + dfacts.DEFAULT_POOL_SUFFIX,
                dfacts.be_pool_muid_from_hostid(self.host_id),
            )
            puid, mpool_fname = MemoryPool.serialized_uid_fname(self.be_mpool.serialize())
            log.info(f"be_mpool has uid {puid} and file {mpool_fname}")

            # Create my receiving channels:
            self.be_inbound = Channel(self.be_mpool, be_cuid)
            self.local_ch_in = Channel(self.be_mpool, local_cuid_in)
            self.local_ch_out = Channel(self.be_mpool, local_cuid_out)
            self.local_inout = Connection(
                inbound_initializer=self.local_ch_in,
                outbound_initializer=self.local_ch_out,
                options=conn_options,
                policy=conn_policy,
            )
            self.infra_in = Connection(inbound_initializer=self.be_inbound, options=conn_options, policy=conn_policy)

            # Create a backdoor into sending messages to infra_in in case I need
            # to tell the thread using it to shutdown:
            self.infra_in_bd = Connection(
                outbound_initializer=self.be_inbound, options=conn_options, policy=conn_policy
            )

            # Create a gateway channel for myself
            self.gw_ch = Channel(self.be_mpool, gw_cuid)

            # Connect to frontend
            be_outbound = Channel.attach(frontend_sdesc.decode(), mem_pool=self.be_mpool)
            conn_options = ConnectionOptions(default_pool=self.be_mpool, min_block_size=2**16)
            self.infra_out = Connection(outbound_initializer=be_outbound, options=conn_options, policy=conn_policy)
            self.infra_out.ghost = True

        except (ChannelError, DragonPoolError, DragonMemoryError) as init_err:
            log.fatal("could not create resources")
            raise RuntimeError("infrastructure transport resource creation failed") from init_err

        self._state = BackendState.OVERLAY_STARTING
        log.info("Channels created from backend memory pool")

        # Set gateway in my environment and register it
        encoded_ser_gw = B64(self.gw_ch.serialize())
        encoded_ser_gw_str = str(encoded_ser_gw)
        os.environ[dfacts.GW_ENV_PREFIX + str(dfacts.DRAGON_OVERLAY_DEFAULT_NUM_GW_CHANNELS_PER_NODE)] = (
            encoded_ser_gw_str
        )
        register_gateways_from_env()

        # start my transport agent
        # Add my own host_id and ip_addr to frontend's
        if is_k8s:
            fe_pod = self.kubernetes.k8s_api_v1.list_namespaced_pod(
                namespace=self.kubernetes.namespace, label_selector=fe_label_selector
            )
            if len(fe_pod.items) == 1:
                arg_host_id = str(dfacts.FRONTEND_HOSTID)
                port = fe_pod.items[0].spec.containers[0].ports[0].container_port
                arg_ip_addr = f"{fe_pod.items[0].status.pod_ip}:{port}"
            else:
                raise RuntimeError("More than one frontend identified.")

        host_ids = [arg_host_id, str(self.host_id)]
        ip_addrs = [arg_ip_addr, self.transport_agent_ipaddr]  # it includes the port
        log.debug(f"standing up tcp agent with gw: {encoded_ser_gw_str}, host_ids={host_ids}, and ip_addrs={ip_addrs}")
        self.dragon_logger = setup_dragon_logging(node_index=0)

        try:
            self.tree_proc = start_overlay_network(
                ch_in_sdesc=B64(self.local_ch_out.serialize()),
                ch_out_sdesc=B64(self.local_ch_in.serialize()),
                log_sdesc=B64(self.dragon_logger.serialize()),
                host_ids=host_ids,
                ip_addrs=ip_addrs,
                frontend=False,
                env=os.environ,
            )
        except Exception as e:
            log.fatal(f"transport agent launch failed on node with host_id {self.host_id}")
            raise RuntimeError(f"transport agent launch failed on node with host_id {self.host_id}") from e

        # Wait on a message from the transport agent telling me it's good to go.
        log.info("Channel tree initializing....")
        ping_back = dlutil.get_with_blocking(self.local_inout)
        assert isinstance(ping_back, dmsg.OverlayPingBE)
        log.debug(f"comm tree initialized with {type(ping_back)} with pid {self.tree_proc.pid}")

        # Send BEIsUP msg to FE
        # Send my serialized descriptor and host_id to the frontend
        be_ch_desc = str(B64(self.be_inbound.serialize()))
        be_up_msg = dmsg.BEIsUp(tag=dlutil.next_tag(), be_ch_desc=be_ch_desc, host_id=self.host_id)
        self.infra_out.send(be_up_msg.serialize())
        log.info(f"sent BEIsUp to the frontend, with host_id = {self.host_id}")

        # Receive my node_index from the frontend - FENodeIdxBE msg
        fe_node_idx_msg = dlutil.get_with_blocking(self.infra_in)
        assert isinstance(fe_node_idx_msg, dmsg.FENodeIdxBE), "la_be node_index from fe expected"
        self._state = BackendState.OVERLAY_UP

        log.info("la_be recv FENodeIdxBE")
        self.node_idx = fe_node_idx_msg.node_index

        # Update the k8s labels to use with the k8s Service for Jupyter
        if is_k8s and self.node_idx == 0:
            pod_name = os.getenv("POD_NAME")
            patch_labels = {"metadata": {"labels": {"node_index": f"{os.getenv('BACKEND_JOB_LABEL')}_pod_0"}}}
            self.kubernetes.k8s_api_v1.patch_namespaced_pod(
                pod_name, namespace=self.kubernetes.namespace, body=patch_labels
            )

        self.net_conf_key = fe_node_idx_msg.net_conf_key_mapping[self.node_idx]
        if self.node_idx < 0:
            log.error(f"node_idx = {self.node_idx} | invalid node id, error starting LA BE")
        log.debug(f"my node index is {self.node_idx}")

        # Update my overlay network with the extra IP and host ID info:
        log.debug(f"Before sending TAUpdateNodes")
        update_msg = dmsg.TAUpdateNodes(tag=dlutil.next_tag(), nodes=list(fe_node_idx_msg.forward.values()))
        log.debug(f"sending TAUpdateNodes to overlay: {update_msg.uncompressed_serialize()}")
        self.local_inout.send(update_msg.serialize())

        # create a contiguous mapping of keys that came from net_config
        # If I'm a tree child of the frontend, I need to forward infrastructure messages to
        # specific children of my own
        self.frontend_fwd_conns = self._construct_child_forwarding(
            fe_node_idx_msg.forward, fe_node_idx_msg.net_conf_key_mapping
        )
        log.debug(f"connections to forward to: {self.frontend_fwd_conns}")

        # If I have any FENodeIdxBE messages to propogate, do it now,
        # filling in that node's index as well as my channel descriptor, for when
        # we eventually have a hierarchical reduce implemented
        # We make node_index contiguous with range [0...n-1]. The net_conf_key_mapping provides a way to refernece back to the index in the network config.
        for idx, conn in self.frontend_fwd_conns.items():
            fe_node_idx = dmsg.FENodeIdxBE(
                tag=dlutil.next_tag(),
                node_index=int(idx),
                forward=fe_node_idx_msg.forward,
                send_desc=self.be_inbound,
                net_conf_key_mapping=fe_node_idx_msg.net_conf_key_mapping,
            )
            conn.send(fe_node_idx.serialize())

        # This starts the "down the tree" router.
        log.debug("starting frontend monitor")
        recv_msgs_from_overlaynet_thread_args = (self.la_be_stdin,)
        self.recv_msgs_from_overlaynet_thread = threading.Thread(
            name="Frontend Monitor",
            target=self.receive_messages_from_overlaynet,
            args=recv_msgs_from_overlaynet_thread_args,
            daemon=False,
        )
        self.recv_msgs_from_overlaynet_thread.start()

        # Start the "up the tree" router
        send_msgs_to_overlaynet_thread_args = (self.la_be_stdout,)
        self.send_msgs_to_overlaynet_thread = threading.Thread(
            name="Backend Monitor",
            target=self.send_messages_to_overlaynet,
            args=send_msgs_to_overlaynet_thread_args,
            daemon=False,
        )
        self.send_msgs_to_overlaynet_thread.start()
        self._state = BackendState.OVERLAY_THREADS_UP

        logger_sdesc = self.dragon_logger.serialize()
        log.debug("setting up logging again")
        level, fname = setup_BE_logging(dls.LA_BE, logger_sdesc=B64(logger_sdesc), fname=fname)
        log = logging.getLogger(dls.LA_BE).getChild("run_startup")

        self.is_primary = self.node_idx == 0
        log.debug(f"primary node status = {self.is_primary}")

        # Add the dragon channel as a handler to our internal logging calls
        log = logging.getLogger(dls.LA_BE).getChild("run_startup")
        log.info(f"start in pid {os.getpid()}, pgid {os.getpgid(0)}")
        log.info(f"OverlayNet be start complete. La_BE running on node {self.node_idx}")
        log.debug("testing debug level")

        # This starts the "logging" router.
        send_log_to_overlaynet_thread_args = (self.dragon_logger, level)
        self.send_log_to_overlaynet_thread = threading.Thread(
            name="Logging Monitor",
            target=self.send_log_msgs_to_overlaynet,
            args=send_log_to_overlaynet_thread_args,
            daemon=False,
        )

        self.send_log_to_overlaynet_thread.start()

        # Now start local services....
        self._state = BackendState.LS_STARTING
        self.ls_proc, stdin_fd, stdout_fd = self._start_localservices()
        self.ls_stdin = NewlineStreamWrapper(
            os.fdopen(stdin_fd, "wb", buffering=0), read_intent=False, write_intent=True
        )
        self.ls_stdout = NewlineStreamWrapper(
            os.fdopen(stdout_fd, "rb", buffering=0), read_intent=True, write_intent=False
        )

        # Let local services know we're up and give it everything it needs to get going
        if is_k8s:
            _user = os.environ.get("USER", str(os.getuid()))
            ip_addrs = [str(self.ip_addr.split(":")[0])]
        else:
            _user, ip_addrs = _get_host_info(self.network_prefix)  # get ip_addrs without the port

        be_node_idx_msg = dmsg.BENodeIdxSH(
            tag=dlutil.next_tag(),
            node_idx=self.node_idx,
            host_name=self.hostname,
            ip_addrs=ip_addrs,
            primary=self.is_primary,
            logger_sdesc=B64(logger_sdesc),
            net_conf_key=self.net_conf_key,
        )

        self.ls_stdin.send(be_node_idx_msg.serialize())
        log.info(
            f"sent BENodeIdxSH(node_idx={self.node_idx}, net_conf_key={self.net_conf_key}) - m2.1 -- ip_addrs = {ip_addrs}"
        )

        # Wait for SHPingBE on the incoming Posix Message Queue. This tells us the
        # infrastructure channels have been created. Then we know we can attach to them.
        sh_ping_be_msg = dlutil.get_with_blocking(self.ls_stdout)
        assert isinstance(sh_ping_be_msg, dmsg.SHPingBE), "la_be ping from ls expected"
        log.info("la_be recv SHPingBE - m3")

        # switch to comms with local services over channels and proceed with bring up
        self.ls_channel = Channel.attach(B64.from_str(sh_ping_be_msg.shep_cd).decode())
        self.la_channel = Channel.attach(B64.from_str(sh_ping_be_msg.be_cd).decode())
        self.ls_queue = Connection(outbound_initializer=self.ls_channel, policy=POLICY_INFRASTRUCTURE)
        self.la_queue = Connection(inbound_initializer=self.la_channel, policy=POLICY_INFRASTRUCTURE)
        self.la_queue_bd = Connection(outbound_initializer=self.la_channel, policy=POLICY_INFRASTRUCTURE)
        log.info("la_be attached to ls created channels - a7")

        # Start the channel monitor thread
        channel_monitor_args = (self.la_queue, self.la_be_stdout)
        self.channel_monitor_thread = threading.Thread(
            name="BE Channel Monitor", target=self.be_channel_monitor, args=channel_monitor_args, daemon=False
        )
        self.channel_monitor_thread.start()

        # Now send the BEPingSH message to Local Services on its channel to confirm
        # that this code has switched over to using the infra channels.
        self.ls_queue.send(dmsg.BEPingSH(tag=dlutil.next_tag()).serialize())
        log.info("la_be sent BEPingSH - m4")

        log.debug("Exiting backend startup...")

    def receive_messages_from_overlaynet(self, la_be_stdin: dlutil.SRQueue):
        """This service forwards anything sent down from the frontend launcher to the backend main
           thread for processing.

        Args:
           la_be_stdin (dlutil.SRQueue): The connection to the main thread to route messages to.
        """
        log = logging.getLogger(dls.LA_BE).getChild("recv_msgs_from_overlaynet")
        running = True
        # send msg from callback to parent
        while running:
            try:
                fe_msg = dlutil.get_with_blocking(self.infra_in)
                if isinstance(fe_msg, dmsg.HaltOverlay):
                    running = False
                    break
                la_be_stdin.send(fe_msg.serialize())
                log.info(f"received {type(fe_msg)} from Launcher FE and sent to Launcher BE")

                # Forwad the message to any
                if isinstance(fe_msg, dmsg.BEHalted):
                    log.debug("Exiting receive_messages_from_overlaynet")
                    running = False
            except Exception as ex:  # pylint: disable=broad-except
                log.critical(f"Caught exception in router: {ex}")
                # Send an ExceptionlessAbort in order to get an AbnormalTermiation
                # to the frontend
                self.la_be_stdout.send(dmsg.ExceptionlessAbort(tag=dlutil.next_tag()).serialize())

        log.info("exiting receive_messages_from_overlaynet thread")

    def send_messages_to_overlaynet(self, la_be_stdout: dlutil.SRQueue):
        """This service forwards any messages writen to la_be_stdout to the front end launcher.

        Args:
            la_be_stdout (dlutil.SRQueue): SimpleQueue used for sending one information
            message from main thread to the frontend via the overlay network
        """
        self.to_overlaynet_log = logging.getLogger(dls.LA_BE).getChild("send_msgs_to_overlaynet")

        try:
            # Just recv messages and send them where they need to
            # go until told to stop
            while not self._shutdown.is_set():
                # recv msg from parent to send to frontend
                self.to_overlaynet_log.info("overlay net waiting for message")
                msg = dlutil.get_with_blocking(la_be_stdout)

                if isinstance(msg, dmsg.HaltOverlay):
                    self._shutdown.set()
                    self.to_overlaynet_log.info(f"Being told by BE server to exit overlay send loop: {type(msg)}")

                # if it's be halted, get out of here. we got it just to break out of our blocking
                # recv
                if isinstance(msg, dmsg.BEHalted):
                    self.to_overlaynet_log.info(f"Leaving msgs to overlaynet loop due to {type(msg)}")
                    break

                # Handle the case that we're able to catch an abort and need to send
                # that to the frontend but then assume the frontend will no longer talk to us.
                if isinstance(msg, dmsg.ExceptionlessAbort):
                    at_msg = dmsg.AbnormalTermination(tag=dlutil.next_tag(), host_id=self.host_id)
                    self.infra_out.send(at_msg.serialize())
                    self._abnormally_terminating = True
                    self.to_overlaynet_log.info("forwarded AbnormalTermination to frontend via ExceptionlessAbort")

                if isinstance(msg, dmsg.RebootRuntime):
                    at_msg = dmsg.AbnormalTermination(tag=dlutil.next_tag(), host_id=msg.h_uid)
                    self.infra_out.send(at_msg.serialize())
                    self._abnormally_terminating = True
                    self.to_overlaynet_log.info(
                        f"forwarded AbnormalTermination with h_uids to ignore {at_msg.tag} to frontend via RebootRuntime"
                    )

                # Do any specific to the message handling then forward it along
                self.to_overlaynet_log.info(f"received {type(msg)}")
                if type(msg) in LauncherBackEnd._DTBL:
                    if isinstance(msg, dmsg.SHFwdOutput):
                        self.to_overlaynet_log.debug(f"{msg}")
                    self.infra_out.send(msg.serialize())
                    self.to_overlaynet_log.info(f"forwarded {type(msg)} to frontend")

            self.to_overlaynet_log.info("exiting thread routine")

        except Exception as err:  # pylint: disable=broad-except
            self.to_overlaynet_log.exception(f"OverlayNet BE thread failure: {err}")

        self.to_overlaynet_log.info("setting shutdown flag")
        self._shutdown.set()
        self.to_overlaynet_log.info("exiting send_messages_to_overlaynet")

    def send_log_msgs_to_overlaynet(self, my_dragon_logger: DragonLogger, level: int):
        """This service forwards any log messages received on the Dragon logging channel to
        the OverlayNet queue to be sent to the front end launcher.

        Args:
            logger_sdesc (B64): Dragon handle used to attach to the logging channel
            level (int): minimum log priority of messages to forward
        """
        log = logging.getLogger(dls.LA_BE).getChild("forward_logging_to_FE")
        try:
            # Set timeout to None which allows better interaction with the GIL
            while not self._logging_shutdown.is_set():
                try:
                    serialized_msg = my_dragon_logger.get(level, timeout=None)
                    if isinstance(serialized_msg, dmsg.HaltLoggingInfra):
                        break
                    elif isinstance(serialized_msg, dmsg.LoggingMsg):
                        self.infra_out.send(serialized_msg)
                except Exception:
                    pass
            log.debug("exiting send logs loop")
        except Exception as ex:  # pylint: disable=broad-except
            log.warning(f"Caught exception {ex} in logging thread")

    def be_channel_monitor(self, la_in, la_be_stdout):
        """This function (running in a thread) monitors the backend channel
        and any communication coming into it from the rest of the node
        (i.e. local services or global services)

        Args:
            la_in (Connection): communication channel receiving messages
            la_be_stdout (SRQueue): main queue of messages to process
        """
        log = logging.getLogger(dls.LA_BE).getChild("be_channel_monitor")
        running = True
        while running:
            try:
                try:
                    msg = dlutil.get_with_blocking(la_in)
                except Exception:
                    # Don't deliver abort if we're already in the middle of teardown.
                    if self._state < BackendState.TEARDOWN:
                        msg = dmsg.ExceptionlessAbort(tag=dlutil.next_tag())
                    else:
                        # Don't throw anything here as we're in the middle of teardown and want that
                        # to continue and ideally complete. But do set the flag to raise an exception at exit
                        log.debug(
                            "Caught an abnormal termination during teardown. Setting to flag to raise Exception at exit."
                        )
                        self._abnormally_terminating = True
                        continue

                if isinstance(msg, dmsg.HaltOverlay):
                    break

                if isinstance(msg, dmsg.SHChannelsUp):
                    self._state = BackendState.LS_UP

                la_be_stdout.send(msg.serialize())
                log.debug(f"be_channel_monitor - forwarded {type(msg)}")
                if isinstance(msg, dmsg.SHHaltBE):
                    log.info("setting logging for shutdown")
                    running = False
                    self._logging_shutdown.set()

                    log.info("detaching from dragon logging")
                    detach_from_dragon_handler(dls.LA_BE)
                    self.dragon_logger.put(dmsg.HaltLoggingInfra(tag=dlutil.next_tag()).serialize())

            except Exception as ex:
                log.critical(f"Caught exception in be_channel_monitor thread: {ex}")

        log.debug("exiting channel monitor loop")

    def run_msg_server(self):
        """Handle messages from dragon services"""
        self.msg_log = logging.getLogger(dls.LA_BE).getChild("msg_server")

        while not self._shutdown.is_set():
            self.msg_log.debug(f"waiting for another message")
            msg = dlutil.get_with_blocking(self.la_be_stdin)
            self.msg_log.info(f"JUST received {type(msg)}")
            if hasattr(msg, "r_c_uid"):
                msg.r_c_uid = dfacts.launcher_cuid_from_index(self.node_idx)
            self.msg_log.info(f"received {type(msg)}")
            if type(msg) in LauncherBackEnd._DTBL:
                self._DTBL[type(msg)][0](self, msg=msg)
            else:
                self.msg_log.warning(f"unexpected msg type: {repr(msg)}")

        # Join on all service threads
        self.msg_log.debug("joining on threads...")
        self._close_threads()

        self.msg_log.debug("la_be joining on LS child proc")
        self.ls_proc.wait(timeout=None)
        self.msg_log.info("localservices closed from LA_BE perspective")
        self.msg_log.debug("la_be server exiting cleanly")

        # Close infrasructure communication
        self._close_overlay_comms()

    def forward_to_leaves(self, msg):
        """Forward an infrastructure messages to leaves I'm responsible for

        :param msg: Infrastructure message
        :type msg: dragon.infrastructure.messages.InfraMsg
        """
        for conn in self.frontend_fwd_conns.values():
            conn.send(msg.serialize())

    @route(dmsg.GSProcessCreate, _DTBL)
    def handle_gs_process_create(self, msg: dmsg.GSProcessCreate):

        # The GSProcessCreate message should only be received by the Primary Launcher Backend process.
        # Verify that we're the primary and that our connection to Global Services is established.
        if (not self.is_primary) or self.transport_test_env:
            self.msg_log.error(
                "Non-primary LA_BE received GSProcessCreate Request. Cannot forward request to GlobalServices."
            )
            raise RuntimeError("Non-primary LA_BE received GSProcessCreate Request.")

        if not self.gs_queue:
            self.msg_log.error(
                "Primary LA_BE received GSProcessCreate Request but gs_queue not established. Cannot forward request to GlobalServices."
            )
            raise RuntimeError("Primary LA_BE received GSProcessCreate Request but gs_queue not established.")

        self.msg_log.info("Primary la_be received a GSProcess Create in the Launcher Backend and forwarded to GS.")
        self.gs_queue.send(msg.serialize())

    @route(dmsg.GSProcessCreateResponse, _DTBL)
    def handle_gs_process_create_resp(self, msg: dmsg.GSProcessCreateResponse):
        pass

    @route(dmsg.SHFwdOutput, _DTBL)
    def handle_sh_fwd_output(self, msg: dmsg.SHFwdOutput):
        pass

    @route(dmsg.GSHeadExit, _DTBL)
    def handle_gs_head_exit(self, msg: dmsg.GSHeadExit):
        self.msg_log.info("m3.1 la_be transmitted GSHeadExit to la_fe")

    @route(dmsg.GSTeardown, _DTBL)
    def handle_gs_teardown(self, msg: dmsg.GSTeardown):

        # The GSTeardown message should only be received by the Primary Launcher Backend process.
        # Verify that we're the primary and that our connection to Global Services is established.
        if (not self.is_primary) or self.transport_test_env:
            self.msg_log.error(
                "Non-primary LA_BE received GSTeardown Request. Cannot forward request to GlobalServices."
            )
            raise RuntimeError("Non-primary LA_BE received GSTeardown Request.")

        # If we've been told to teardown before infrastructure is fully up,
        # we may be in trouble, so guard against that
        self.msg_log.info("m4.1 primary la_be received GSTeardown")
        try:
            self.gs_queue.send(msg.serialize())
            self._state = BackendState.TEARDOWN
        except (AttributeError, ConnectionError):
            self.msg_log.info(
                "Received a GSTeardown before backend fully up. Will work towards cleaning backend and exit"
            )
            self._abnormal_termination(msg)

    @route(dmsg.SHHaltTA, _DTBL)
    def handle_sh_halt_ta(self, msg: dmsg.SHHaltTA):
        self.msg_log.info("m7.1 la_be received SHHaltTA")
        try:
            # Forward to leaves
            self.forward_to_leaves(msg)

            # Try forwarding ot local services
            if self._state >= BackendState.LS_UP:
                self.ls_queue.send(msg.serialize())
                self.msg_log.info("m7.2 la_be forwarded SHHaltTA to LS")
                if not self.is_primary:
                    self._state = BackendState.TEARDOWN
            else:
                raise ConnectionError("Cannot communicate over LS channel reliably")
        except (AttributeError, ConnectionError):
            self.msg_log.info(
                "Received a SHHaltTA before backend fully up. Will work towards cleaning backend and exit"
            )
            self._abnormal_termination(msg)

    @route(dmsg.SHTeardown, _DTBL)
    def handle_sh_teardown(self, msg: dmsg.SHTeardown):

        self.msg_log.info("m11.1 la_be received SHTeardown")

        self.forward_to_leaves(msg)
        self.msg_log.info("forward shteardown to backend leaves")

        self.ls_queue.send(msg.serialize())
        self.msg_log.info("m11.2 la_be forwarded SHTeardown to LS")

    @route(dmsg.BEHalted, _DTBL)
    def handle_be_halted(self, msg: dmsg.BEHalted):
        self.msg_log.info("m14 la_be received dmsg.BEHalted")
        self._shutdown.set()
        self.la_be_stdout.send(msg.serialize())

        self.forward_to_leaves(msg)
        self.msg_log.info("forward BEHalted to backend leaves")

        # This is the dmsg.BEHalted
        self.ls_stdin.send(msg.serialize())
        self.msg_log.debug("la_be forwarded m14 BEHalted")

    @route(dmsg.AbnormalTermination, _DTBL)
    def handle_abnormal_term(self, msg: dmsg.AbnormalTermination):
        self.msg_log.info("la_be received dmsg.AbnormalTermination")

    @route(dmsg.RebootRuntime, _DTBL)
    def handle_reboot_runtime(self, msg: dmsg.RebootRuntime):
        self.msg_log.info("la_be received dmsg.RebootRuntime")

    @route(dmsg.SHProcessCreate, _DTBL)
    def handle_sh_proc_crate(self, msg: dmsg.SHProcessCreate):
        self.msg_log.info("In Transport Test Mode: Sending SHProcessCreate directly to local services.")
        self.ls_queue.send(msg.serialize())

    @route(dmsg.SHChannelsUp, _DTBL)
    def handle_sh_channels_up(self, msg: dmsg.SHChannelsUp):
        self.to_overlaynet_log.info("la_be recv SHChannelsUp - m5.1")
        self.to_overlaynet_log.info("la_be sent SHChannelsUp to OverlayNet be - m5.2")

    @route(dmsg.LAChannelsInfo, _DTBL)
    def handle_la_channels_info(self, msg: dmsg.LAChannelsInfo):
        # Wait for the LAChannelsInfo Message to be sent
        # The LAChannelsInfo gave us the serialized descriptor of
        # global services channel, so now we can attach to that channel.
        if self.is_primary and (not self.transport_test_env):
            self.gs_channel = Channel.attach(B64.from_str(msg.gs_cd).decode())
            self.gs_queue = Connection(outbound_initializer=self.gs_channel, policy=POLICY_INFRASTRUCTURE)
            self.msg_log.debug(f"Primary la_be created gs_queue: {self.gs_queue}")

        # Forward the message to my leaves
        self.forward_to_leaves(msg)

        # Forward the LAChannelsInfo Message to Local Services
        self.ls_queue.send(msg.serialize())
        self.msg_log.info("la_be sent LAChannelsInfo to ls - m5.3")

    @route(dmsg.TAUp, _DTBL)
    def handle_ta_up(self, msg: dmsg.TAUp):
        self._state = BackendState.TA_UP
        self.to_overlaynet_log.info("la_be recvd Ping response from TA - m8.1")
        if not self.is_primary:
            self._state = BackendState.BACKEND_UP
            self.to_overlaynet_log.info("non-primary backend node is up")

    @route(dmsg.GSIsUp, _DTBL)
    def handle_gs_is_up(self, msg: dmsg.GSIsUp):
        self.to_overlaynet_log.info("la_be received GSIsUp ping - m12.1")
        self._state = BackendState.BACKEND_UP

    @route(dmsg.GSHalted, _DTBL)
    def handle_gs_halted(self, msg: dmsg.GSHalted):
        self.to_overlaynet_log.info("m6.1 primary la_be received GSHalted")

    @route(dmsg.TAHalted, _DTBL)
    def handle_ta_halted(self, msg: dmsg.TAHalted):
        self.to_overlaynet_log.info("m10.1 la_be received TAHalted")
        self.to_overlaynet_log.info("m10.2 la_be forwarded TAHalted to FE")

    @route(dmsg.SHHaltBE, _DTBL)
    def handle_sh_halt_be(self, msg: dmsg.SHHaltBE):
        self.to_overlaynet_log.info("m12 la_be received SHHaltBE")

    @route(dmsg.SHProcessExit, _DTBL)
    def handle_sh_process_exit(self, msg: dmsg.SHProcessExit):
        pass

    @route(dmsg.SHProcessCreateResponse, _DTBL)
    def handle_sh_proc_create_response(self, msg: dmsg.SHProcessCreateResponse):
        pass
