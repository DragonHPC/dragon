import ctypes
import os
import sys
import logging
import threading
import signal
import subprocess
from enum import Enum
from shlex import quote
from functools import total_ordering

from ..utils import B64
from ..channels import Channel, ChannelError, ChannelEmpty, register_gateways_from_env, discard_gateways
from ..managed_memory import MemoryPool, DragonPoolError, DragonMemoryError
from ..transport.overlay import start_overlay_network

from ..dlogging.util import DragonLoggingServices as dls
from ..dlogging.util import _get_dragon_log_device_level, LOGGING_OUTPUT_DEVICE_DRAGON_FILE
from ..dlogging.logger import DragonLogger, DragonLoggingError

from ..infrastructure.util import route
from ..infrastructure.parameters import POLICY_INFRASTRUCTURE, this_process
from ..infrastructure.connection import Connection, ConnectionOptions
from ..infrastructure.node_desc import NodeDescriptor
from ..infrastructure import facts as dfacts
from ..infrastructure import messages as dmsg
from ..infrastructure.messages import AbnormalTerminationError

from . import util as dlutil
from .network_config import NetworkConfig
from .launchargs import parse_hosts
from .wlm import WLM
from .wlm.ssh import SSHSubprocessPopen

LAUNCHER_FAIL_EXIT = 1


class LauncherImmediateExit(Exception):

    def __init__(self, msg=''):
        self._msg = msg

    def __str__(self):
        return f'{self._msg}'

    def __repr__(self):
        return f"{str(__class__)}({repr(self._msg)})"


class SigIntImmediateExit(Exception):

    def __init__(self, msg=''):
        self._msg = msg

    def __str__(self):
        return f'{self._msg}'

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


class LauncherFrontEnd():
    """State manager for Dragon Frontend

    Returns:
        LauncherFrontEnd: instance of class
    """
    _DTBL = {}  # dispatch router for msgs, keyed by type of message
    _STBL = {}  # dispatch router for SIGINT handlign, keyed by current state
    _TRDOWN = {}  # dispatch router teardown errors, keyed by FrontendState

    _BE_LAUNCHARGS = f'{dfacts.PROCNAME_LA_BE}' \
                     ' --ip-addr {ip_addr}' \
                     ' --host-id {host_id}' \
                     ' --frontend-sdesc {frontend_sdesc}' \
                     ' --network-prefix {network_prefix}'

    _STATE = None

    def __init__(self, args_map, sigint_trigger=None):

        log = logging.getLogger(dls.LA_FE).getChild('LauncherFrontEnd init')
        log.info(f'start in pid {os.getpid()}, pgid {os.getpgid(0)}')

        # Discover whether the transport service test mode is being requested
        # or not.
        self.transport_test_env = os.environ.get(dfacts.TRANSPORT_TEST_ENV) is not None

        try:
            pals_lib = ctypes.cdll.LoadLibrary('libpals.so')
            self.pals_lib_present = pals_lib != None
            del pals_lib
        except:
            self.pals_lib_present = False

        # This running value is used to control when to bring down the OverlayNet service threads
        self._shutdown = threading.Event()
        self._abnormal_termination = threading.Event()

        self.args_map = args_map
        self.nnodes = args_map.get('node_count', 0)
        self.ntree_nodes = this_process.overlay_fanout
        self.network_prefix = args_map.get('network_prefix', dfacts.DEFAULT_TRANSPORT_NETIF)
        self.port = args_map.get('port', dfacts.DEFAULT_TRANSPORT_PORT)
        self._config_from_file = args_map.get('network_config', None)
        self.transport = args_map.get('transport')

        # If using SSH, confirm we have enough info do that:
        self._wlm = args_map.get('wlm', None)
        self.hostlist = args_map.get('hostlist', None)
        self.hostfile = args_map.get('hostfile', None)

        # Don't default wlm in argparse. We can better do its error/case handling outside of
        # argparse/make it easier to test
        if self._wlm is None:
            self._wlm = dlutil.detect_wlm()

        # If using SSH, confirm we have enough info do that:
        if self._wlm == WLM.SSH and self._config_from_file is None:
            self.hostlist = parse_hosts(self.hostlist, self.hostfile)

        if self._wlm is WLM.SSH and self.transport is not dfacts.TransportAgentOptions.TCP:
            msg = '''
When using SSH to execute dragon jobs, the TCP transport agent is the only allowed agent.
Please resubmit your dragon launch command with the `--transport tcp` option set.
'''
            print(msg)
            sys.exit(LAUNCHER_FAIL_EXIT)

        # Variety of other state trackers:
        self._sigint_count = 0
        self._sigint_timeout = 5.0
        self._bumpy_exit = threading.Event()

        self._proc_create_resps = 0
        self._proc_exits = 0
        self._tas_halted = 0
        self._sh_halt_be = 0
        self._gs_head_exit_received = False
        self._gs_process_create_resp_received = False

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
        self.dragon_logger = None
        self.over_proc = None
        self.recv_overlaynet_thread = None
        self.send_overlaynet_thread = None
        self.recv_logs_from_overlaynet_thread = None

        # Int trigger for raising SIGINT at various points of
        # bringup for unit testing purposes
        self._sigint_trigger = sigint_trigger

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):

        self._cleanup()

    def _kill_backend(self):
        '''Simple function for transmitting SIGKILL to backend with helpful message'''

        if self.wlm_proc.returncode is None:
            self._bumpy_exit.set()
            self.wlm_proc.kill()

    def _takedown_abnormal_backend(self, sigint=False):
        '''Function to iteratively take down the backend with one thread to enable easy abort'''
        log = logging.getLogger(dls.LA_FE).getChild('_takedown_abnormal_backend')

        if not sigint:
            print("Abnormal Exit detected. Attempting to clean up over next minute....", flush=True)

        # This is a chance we try to use the route decorated functions which all assume self.msg_log
        # exits. It doesn't necessarily:
        self.msg_log = logging.getLogger(dls.LA_FE).getChild('_takedown_abnormal_backend')

        # Get the threads down first
        self._close_threads(abnormal=True)

        # Do a quick "should I proceed with a teardown loop logic"
        # node index 0 is the primary node
        if FrontendState.STOOD_UP <= self._STATE < FrontendState.TEARDOWN:
            self._STATE = FrontendState.ABNORMAL_TEARDOWN
            log.debug(f'Updated state to {self._STATE}')
            try:
                self.conn_outs[0].send(dmsg.GSTeardown(tag=dlutil.next_tag()).serialize())
            except Exception:
                raise TimeoutError
            log.info('got GSTeardown out')

            while True:
                msg = dlutil.get_with_timeout(self.conn_in, timeout=self._sigint_timeout)

                if isinstance(msg, dmsg.BEHalted):
                    log.debug('Breaking due to BEHalted')
                    break
                elif type(msg) in LauncherFrontEnd._DTBL:
                    self._DTBL[type(msg)][0](self, msg=msg)
                else:
                    log.debug('raising runtime error due to unknown message')
                    raise RuntimeError

    def _wait_on_wlm_proc_exit(self, timeout=30):
        '''Short function for doing a wait on wlm subprocess. Allows for easier mocking'''

        return self.wlm_proc.wait(timeout)

    def _cleanup_abnormal_state(self, sigint=False):
        """Attempt to clean up backend in case it hasn't"""

        log = logging.getLogger(dls.LA_FE).getChild('_cleanup_abnormal_state')
        log.debug('Entered cleanup routine with a still up backend. Trying to teardown...')

        # If we got here, make sure we raise an exception on exit
        if not sigint:
            self._abnormal_termination.set()
        else:
            log.debug(f'executing abnormal cleanup due to SIGINT rather than AbnormalTermination: {self._STATE}')

        # Set an alarm just in case
        try:
            self._takedown_abnormal_backend(sigint=sigint)
        except (subprocess.TimeoutExpired, RuntimeError, TimeoutError):
            pass

        try:
            log.debug('waiting on wlm_proc exit')
            self._wait_on_wlm_proc_exit(timeout=self._sigint_timeout)
        except (subprocess.TimeoutExpired, RuntimeError, TimeoutError):
            log.debug('forcibly kill wlm proc')
            self._kill_backend()

        self._close_comm_overlaynet(abnormal=True)

        log.debug('Finished abnormal state cleanup')

        log.debug('Sending a message to break msg server out of its loop, just in case')
        self.la_fe_stdin.send(dmsg.LAExit(sigint=sigint, tag=dlutil.next_tag()).serialize())
        log.debug('LAExit sent')
        self._STATE = FrontendState.LAUNCHER_DOWN

        # if during teardown we hit an error, clean ourselves up
        if self._bumpy_exit.is_set():
            self._dragon_cleanup_bumpy_exit()

    def _cleanup(self, sigint=False):
        '''clean up any memory resources'''
        log = logging.getLogger(dls.LA_FE).getChild('_cleanup')

        log.debug('beginning cleanup')

        # Close threads best as possible, assume abnormal
        # because we want to make sure they're down at this point
        self._close_threads(abnormal=True)

        # If we're here due to an abnormal termination, we need
        # to make a good faith effort to teardown the backend
        log.debug(f"current state: {self._STATE}")
        if FrontendState.LAUNCHER_DOWN > self._STATE > FrontendState.STARTUP:
            log.debug("Attemping to take down backend before killing it all off")
            self._cleanup_abnormal_state(sigint=sigint)

        try:
            if self.wlm_proc.returncode is None:
                log.debug('killing workload manager backend proc')
                self.wlm_proc.kill()
        except Exception:
            pass

        self._close_comm_overlaynet()

        # if during teardown we hit an error, clean ourselves up
        if self._bumpy_exit.is_set():
            self._dragon_cleanup_bumpy_exit()

        log.debug('Exiting _cleanup')
        # And raise error
        if self._abnormal_termination.is_set():
            log.debug("Raising Abnormal RuntimeError")
            raise RuntimeError('Abnormal exit detected')

    def _wait_on_overlay_init(self):
        """Wait until overlay network returns OverlayPingLA signaling it is up"""
        log = logging.getLogger('overlay_startup')
        log.info('Channel tree initializing...')
        ping_back = dlutil.get_with_blocking(self.local_inout)
        assert isinstance(ping_back, dmsg.OverlayPingLA)
        log.debug(f'recvd msg type {type(ping_back)}')
        self._STATE = FrontendState.OVERLAY_UP

    def _close_overlay(self, abnormal=False):
        """Close the overlay process"""
        log = logging.getLogger('close_overlay')
        if self._STATE >= FrontendState.OVERLAY_UP:
            try:
                self.local_inout.send(dmsg.LAHaltOverlay(tag=dlutil.next_tag()).serialize())
                log.debug('sent halt overlay signal ')

                # If we're in an abnormal exit, make sure to not block
                try:
                    if abnormal:
                        overlay_halted = dlutil.get_with_timeout(self.local_inout, timeout=self._sigint_timeout)
                    else:
                        overlay_halted = dlutil.get_with_blocking(self.local_inout)
                except TimeoutError:
                    log.debug('timeout on recv dmsg.OverlayHalted')
                else:
                    log.debug(f'received {overlay_halted} from local TCP agent')
                    assert isinstance(overlay_halted, dmsg.OverlayHalted)

            except (AttributeError, ConnectionError):
                pass

        # Make sure the overlay is down before continuing
        try:
            self.over_proc.wait(timeout=self._sigint_timeout)
        except subprocess.TimeoutExpired:
            log.debug('timeout on overlay exit. Killing')
            self.over_proc.kill()
        except AttributeError:
            pass

    def _close_threads(self, abnormal=False):
        log = logging.getLogger(dls.LA_FE).getChild('close_threads')
        halt_overlay_msg = dmsg.HaltOverlay(tag=dlutil.next_tag())
        halt_logging_msg = dmsg.HaltLoggingInfra(tag=dlutil.next_tag())
        if self.recv_overlaynet_thread is not None:
            if self.recv_overlaynet_thread.is_alive():
                if abnormal:
                    log.info('abnormal closing of recv overlay thread')
                    self._shutdown.set()
                    self.conn_in_bd.send(halt_overlay_msg.serialize())
                self.recv_overlaynet_thread.join()
        log.info('recv_overlaynet_thread joined')

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
                    self.send_msg_to_overlaynet("A", halt_overlay_msg)
                self.send_overlaynet_thread.join()
        log.info('send_overlaynet_thread joined')
        if self.recv_logs_from_overlaynet_thread is not None:
            if self.recv_logs_from_overlaynet_thread.is_alive():
                # Send it a message to make sure it can get out
                self._shutdown.set()
                self.dragon_logger.put(halt_logging_msg.serialize())
            self.recv_logs_from_overlaynet_thread.join()
        log.info('recv_logs_from_overlaynet_thread joined')

    def _close_comm_overlaynet(self, abnormal=False):

        log = logging.getLogger(dls.LA_FE).getChild('_close_comm_overlaynet')

        # Close the overlay tree agent
        try:
            log.info('shutting down frontend overlay tree agent')
            self._close_overlay(abnormal=abnormal)

        except Exception:
            if abnormal:
                log.info('killing overlay on frontend')
                self.over_proc.kill()
            else:
                raise

        try:
            self.conn_in.close()
        except Exception:
            pass

        try:
            self.conn_in_bd.close()
        except Exception:
            pass

        log.debug('closing overlaynet channels and gateway')
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
                log.debug('closing comms to overlay TCP agent and mpool')
                self.gw_ch.destroy()
                discard_gateways()
                try:
                    del os.environ[dfacts.GW_ENV_PREFIX+str(dfacts.DRAGON_OVERLAY_DEFAULT_NUM_GW_CHANNELS_PER_NODE)]
                except KeyError:
                    pass
                log.debug('gw closed')
        except ChannelError:
            pass

        try:
            self.local_inout.close()
        except Exception:
            pass
        log.debug('local_input closed')

        try:
            if self.local_ch_in is not None:
                self.local_ch_in.destroy()
                log.debug('local_ch_in closed')
        except ChannelError:
            pass

        try:
            if self.local_ch_out is not None:
                self.local_ch_out.destroy()
                log.debug('local_ch_out closed')
        except ChannelError:
            pass

        try:
            if self.fe_inbound is not None:
                self.fe_inbound.destroy()
                log.debug('fe_inbound closed')
        except ChannelError:
            pass

        try:
            if self.fe_mpool is not None:
                self.fe_mpool.destroy()
                log.debug('fe_mp closed')
        except (DragonMemoryError, DragonPoolError):
            pass
        log.info('comms overlay network closed')

    def _set_quick_teardown(self):
        '''Set up conditions to tear down runtime as quickly as reasonable'''
        print("Quickly tearing down Dragon runtime due to KeyboardInterrupt.", flush=True)
        self._bumpy_exit.set()
        self._sigint_timeout = 0.01

    def _dragon_cleanup_bumpy_exit(self):
        '''Helper function to launch `dragon-cleanup` at end of teardown to clean up all procs and mem'''
        print("Detected an abnormal exit. Will attempt to clean up Dragon resources...", flush=True)
        subprocess.run(args=['dragon-cleanup', '0'])

        # Make sure we don't manage to call this more than once
        self._bumpy_exit.clear()

    def sigint_handler(self, *args):
        """Handler for SIGINT signals for graceful teardown
        """
        log = logging.getLogger('sigint_handler')
        log.debug('Entered sigint handler')
        self.sigint_log = logging.getLogger('sigint_route')
        self._sigint_count = self._sigint_count + 1

        print("KeyboardInterrupt detected. Attempting to clean up...", flush=True)

        # If the user has triggered SIGINT more than once just get out of here
        # very quickly.
        if self._sigint_count > 1:
            self._set_quick_teardown()

        if self._STATE.value in LauncherFrontEnd._STBL:
            self._STBL[self._STATE.value][0](self)
        else:
            log.warning(f'SIGINT detected in {self._STATE} and no routing exists for it')

    @route(FrontendState.NET_CONFIG.value, _STBL)
    def _sigint_net_config(self):
        # if we're here, we should be able to just exit
        self.sigint_log.debug('SIGINT caught during NET_CONFIG. Executing default SIGINT behavior')
        self._close_comm_overlaynet()
        raise KeyboardInterrupt

    @route(FrontendState.OVERLAY_STARTING.value, _STBL)
    def _sigint_overlay_starting(self):
        # Wait on receipt that the Overlay is up so we can
        # calmly tear it down
        self.sigint_log.debug('SIGINT caught during OVERYLAY_STARTING. Waiting for Overlay to come up and then exit')
        self._wait_on_overlay_init()

        # Close everything else:
        self._close_comm_overlaynet()
        raise KeyboardInterrupt

    @route(FrontendState.OVERLAY_UP.value, _STBL)
    def _sigint_overlay_up(self):
        # Close overlay and then exit
        self.sigint_log.debug('SIGINT caught during OVERYLAY_UP. Closing overlay and exiting')

        # Close everything else:
        self._close_threads(abnormal=True)
        self.sigint_log.debug('closed threads')

        self._close_comm_overlaynet()
        self.sigint_log.debug('cleaned comms')
        raise KeyboardInterrupt

    @route(FrontendState.STARTUP.value, _STBL)
    @route(FrontendState.BACKEND_LAUNCHED.value, _STBL)
    def _sigint_startup(self):
        # If in startup, get through it first
        self.sigint_log.debug("Raising SIGINT caught in bringup.")
        self._cleanup(sigint=True)
        self.sigint_log.debug('cleaned up. Raising KeyboardInterrupt')
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
        log = logging.getLogger(dls.LA_FE).getChild('sigint teardown')
        log.debug('SIGINT received. Tearing down runtime....')
        self._cleanup_abnormal_state(sigint=True)

    def _launch_backend(self,
                        nnodes: int,
                        nodelist: list[str],
                        fe_ip_addr: str,
                        fe_host_id: str,
                        frontend_sdesc: str,
                        network_prefix: str):
        """Launch backend with selected wlm"""
        log = logging.getLogger(dls.LA_FE).getChild('_launch_backend')
        try:
            if self._wlm is WLM.SSH:
                be_args = self._BE_LAUNCHARGS.format(ip_addr=fe_ip_addr,
                                                     host_id=fe_host_id,
                                                     frontend_sdesc=frontend_sdesc,
                                                     network_prefix=quote(network_prefix)).split()
            else:
                be_args = self._BE_LAUNCHARGS.format(ip_addr=fe_ip_addr,
                                                     host_id=fe_host_id,
                                                     frontend_sdesc=frontend_sdesc,
                                                     network_prefix=network_prefix).split()
            if self.transport_test_env:
                be_args.append('--transport-test')

        except Exception:
            raise RuntimeError("Unable to construct backend launcher arg list")

        the_env = dict(os.environ)
        the_env['DRAGON_NETWORK_CONFIG'] = self.net.compress()

        # TODO: The differentiation between the SSH path vs. other paths
        #       is clunky. Ideally, this could be abstracted to make the
        #       if/else disappear

        # Syntax is largely same for anything launched with a true WLM
        if self._wlm is not WLM.SSH:

            try:
                wlm_args = dlutil.get_wlm_launch_args(args_map=self.args_map,
                                                      nodes=(nnodes, nodelist),
                                                      wlm=self._wlm)
                args = wlm_args + be_args
                log.info(f'launch be with {args}')


                wlm_proc = subprocess.Popen(args,
                                            stdin=subprocess.DEVNULL,
                                            stdout=subprocess.DEVNULL,
                                            env=the_env,
                                            start_new_session=True)
            except Exception:
                raise RuntimeError("Unable to launch backend using workload manager launch")

        # in case of SSH, we need to loop over each host to launch our backend and
        # the bundle up all the processes into a monitoring object that allows
        # the frontend server to query only one object for its monitoring needs
        else:
            try:
                popen_dict = {}

                for host in nodelist:
                    args = dlutil.get_wlm_launch_args(args_map=self.args_map,
                                                      hostname=host,
                                                      wlm=self._wlm)
                    args = args + be_args
                    log.info(f"SSH launch config: {args}")

                    popen_dict[host] = subprocess.Popen(
                                           args=args,
                                           stdin=subprocess.DEVNULL,
                                           stdout=subprocess.DEVNULL,
                                           env=the_env,
                                           start_new_session=True
                                       )
                wlm_proc = SSHSubprocessPopen(popen_dict)

            except Exception:
                raise RuntimeError("Unable to launch backend via SSH loop")

        return wlm_proc

    def construct_bcast_tree(self, net_conf, conn_policy, be_ups, frontend_sdesc):

        log = logging.getLogger(dls.LA_FE).getChild('construct_bcast_tree')

        # Pack up all of our node descriptors for the backend:
        forwarding = {}
        for be_up in be_ups:
            assert isinstance(be_up, dmsg.BEIsUp), 'la_fe received invalid be up'
            # TODO: VERIFY WE GET A UNIQUE CUID: keep a set of seen cuids.
            # After attaching, get the cuid and compare it against the set
            # of already seen cuids. Throw an exception if already seen.
            # Delete the set after this loop.
            log.debug(f'received descriptor: {be_up.be_ch_desc} and host_id: {be_up.host_id}')
            for key, node_desc in net_conf.items():
                if str(be_up.host_id) == str(node_desc.host_id):
                    forwarding[key] = NodeDescriptor(host_id=int(node_desc.host_id),
                                                     ip_addrs=node_desc.ip_addrs,
                                                     overlay_cd=be_up.be_ch_desc)
                    break

        # Send out the FENodeIdx to the child nodes I own
        conn_outs = {}  # key is the node_index and value is the Connection object
        fe_node_idx = dmsg.FENodeIdxBE(tag=dlutil.next_tag(),
                                       node_index=0,
                                       forward=forwarding,
                                       send_desc=frontend_sdesc)
        log.debug(f'fanout = {this_process.overlay_fanout}')
        for idx in range(this_process.overlay_fanout):
            if idx < self.nnodes:
                try:
                    be_sdesc = B64.from_str(forwarding[str(idx)].overlay_cd)
                    be_ch = Channel.attach(be_sdesc.decode(), mem_pool=self.fe_mpool)
                    conn_options = ConnectionOptions(default_pool=self.fe_mpool, min_block_size=2 ** 16)
                    conn_out = Connection(outbound_initializer=be_ch,
                                          options=conn_options,
                                          policy=conn_policy)
                    conn_out.ghost = True

                    # Update the node index to the one we're talking to
                    fe_node_idx.node_index = idx
                    log.debug(f'sending {fe_node_idx.uncompressed_serialize()}')
                    conn_out.send(fe_node_idx.serialize())

                    conn_outs[idx] = conn_out

                except ChannelError as ex:
                    log.fatal(f'could not connect to BE channel with host_id {be_up.host_id}')
                    raise RuntimeError('Connection with BE failed') from ex
            else:
                break

        log.info('sent all FENodeIdxBE msgs')

        return conn_outs

    def run_startup(self):
        """Complete bring up of runtime services
        """
        log = logging.getLogger(dls.LA_FE).getChild('run_startup')


        # This is set here for the overlay network.
        this_process.set_num_gateways_per_node(dfacts.DRAGON_OVERLAY_DEFAULT_NUM_GW_CHANNELS_PER_NODE)

        # setup sigint handling to proceed in a manner that works for us
        self._STATE = FrontendState.NET_CONFIG

        try:
            self._orig_sigint = signal.signal(signal.SIGINT, self.sigint_handler)
            log.debug("got signal handling in place")
        except ValueError:
            # this error is thrown if we are running inside a child thread
            # which we do for unit tests. So pass on this
            log.debug("Unable to do signal handling outside of main thread")

        self.la_fe_stdin = dlutil.OverlayNetLaFEQueue()
        self.la_fe_stdout = dlutil.LaOverlayNetFEQueue()

        # Get the node config for the backend
        log.debug('Getting the node config for the backend.')
        if self._config_from_file is not None:
            try:
                log.info(f"Acquiring network config from file {self._config_from_file}")
                self.net = NetworkConfig.from_file(self._config_from_file)
            except Exception:
                raise RuntimeError("Unable to acquire backend network configuration from input file.")
        else:
            try:
                log.info("Acquiring network config via WLM queries")
                # This sigint trigger is -2 and -1 cases
                self.net = NetworkConfig.from_wlm(workload_manager=self._wlm,
                                                  port=dfacts.DEFAULT_OVERLAY_NETWORK_PORT,
                                                  network_prefix=dfacts.DEFAULT_TRANSPORT_NETIF,
                                                  hostlist=self.hostlist,
                                                  sigint_trigger=self._sigint_trigger)
            except Exception:
                raise RuntimeError("Unable to acquire backend network configuration via workload manager")
        net_conf = self.net.get_network_config()
        log.debug(f"net_conf = {net_conf}")

        if self._sigint_trigger == 0:
            signal.raise_signal(signal.SIGINT)

        # Add the frontend config
        net_conf['f'] = NodeDescriptor.get_local_node_network_conf(network_prefix=self.network_prefix,
                                                                   port_range=dfacts.DEFAULT_FRONTEND_PORT)
        fe_host_id = str(net_conf['f'].host_id)
        fe_ip_addr = net_conf['f'].ip_addrs[0]  # it includes the port
        log.debug(f'node config: {net_conf}')

        # Create my memory pool
        conn_options = ConnectionOptions(min_block_size=2 ** 16)
        conn_policy = POLICY_INFRASTRUCTURE

        try:
            # Create my memory pool
            self.fe_mpool = MemoryPool(int(dfacts.DEFAULT_SINGLE_DEF_SEG_SZ),
                                       f'{os.getuid()}_{os.getpid()}_{fe_host_id}' + dfacts.DEFAULT_POOL_SUFFIX,
                                       dfacts.FE_OVERLAY_TRANSPORT_AGENT_MUID)
            puid, mpool_fname = MemoryPool.serialized_uid_fname(self.fe_mpool.serialize())
            log.debug(f'fe_mpool has uid {puid} and file {mpool_fname}')
            # Create my receiving channel
            fe_cuid = dfacts.FE_CUID
            local_in_cuid = dfacts.FE_LOCAL_IN_CUID
            local_out_cuid = dfacts.FE_LOCAL_OUT_CUID
            gw_cuid = dfacts.FE_GW_CUID

            # Channel for backend to come to
            self.fe_inbound = Channel(self.fe_mpool, fe_cuid)
            encoded_inbound = B64(self.fe_inbound.serialize())
            encoded_inbound_str = str(encoded_inbound)
            self.conn_in = Connection(inbound_initializer=self.fe_inbound,
                                      options=conn_options,
                                      policy=conn_policy)

            if self._sigint_trigger == 1:
                signal.raise_signal(signal.SIGINT)

            # Backdoor connection for breaking recv_msgs thread out of
            # its blocking receive at teardown
            self.conn_in_bd = Connection(outbound_initializer=self.fe_inbound)

            # Channel for tcp to tell me it's up
            self.local_ch_in = Channel(self.fe_mpool, local_in_cuid)
            self.local_ch_out = Channel(self.fe_mpool, local_out_cuid)
            self.local_inout = Connection(inbound_initializer=self.local_ch_in,
                                          outbound_initializer=self.local_ch_out)

            # Create a gateway and logging channel for my tcp agent
            self.gw_ch = Channel(self.fe_mpool, gw_cuid)
            self.dragon_logger = DragonLogger(self.fe_mpool)
        except (ChannelError, DragonPoolError, DragonLoggingError,
                DragonMemoryError) as init_err:
            log.fatal(f'could not create resources: {init_err}')
            raise RuntimeError('overlay transport resource creation failed') from init_err

        log.info('Memory pools and channels created')

        if self._sigint_trigger == 2:
            signal.raise_signal(signal.SIGINT)
        # Set gateway in my environment and register them
        encoded_ser_gw = B64(self.gw_ch.serialize())
        encoded_ser_gw_str = str(encoded_ser_gw)
        os.environ[dfacts.GW_ENV_PREFIX + str(dfacts.DRAGON_OVERLAY_DEFAULT_NUM_GW_CHANNELS_PER_NODE)] = encoded_ser_gw_str
        register_gateways_from_env()

        # start my transport agent
        nnodes = len(net_conf) - 1  # Exclude the frontend node from this
        log.debug(f'requested {self.nnodes} and got {nnodes}')
        if self.nnodes > 0:
            if self.nnodes > nnodes:
                log.exception('too many nodes requested')
                raise ValueError('Not enough backend nodes allocated to match requested')
            nnodes = self.nnodes
        else:
            self.nnodes = nnodes

        log.debug(f'main has {nnodes} nodes')

        # Acquire the primary node and add
        # the frontend info to host_ids and ip_addrs, so the TA functions
        host_ids = [str(net_conf['0'].host_id)] + [fe_host_id]
        ip_addrs = [net_conf['0'].ip_addrs[0]] + [fe_ip_addr]
        hostnames = [net_conf['0'].name]

        # Add as many as needed to meet the requested node count.
        # We also find the minimum number of network interface cards
        # per node.
        min_nics_per_node = 99999
        for node_index in range(nnodes):
            sindex = str(node_index)
            min_nics_per_node = min(min_nics_per_node, len(net_conf[sindex].ip_addrs))
            if not net_conf[sindex].is_primary:
                host_ids.append(str(net_conf[sindex].host_id))
                ip_addrs.append(net_conf[sindex].ip_addrs[0])
                hostnames.append(net_conf[sindex].host_name)

        log.debug(f"ip_addrs={ip_addrs}, host_ids={host_ids}")
        log.debug(f'Found {min_nics_per_node} NICs per node.')

        log.debug(f'standing up tcp agent with gw: {encoded_ser_gw_str}')

        self._STATE = FrontendState.OVERLAY_STARTING

        try:
            self.over_proc = start_overlay_network(ch_in_sdesc=B64(self.local_ch_out.serialize()),
                                                   ch_out_sdesc=B64(self.local_ch_in.serialize()),
                                                   log_sdesc=B64(self.dragon_logger.serialize()),
                                                   host_ids=host_ids,
                                                   ip_addrs=ip_addrs,
                                                   frontend=True,
                                                   env=os.environ)
        except Exception as e:
            log.fatal('transport agent launch failed on FE')
            raise RuntimeError('Overlay transport agent launch failed on launcher frontend') from e

        # Wait on a started message
        self._wait_on_overlay_init()

        # Start a thread for monitoring messages into the logging channel
        # so we can see what comes out of the overlay network
        self.recv_logs_from_overlaynet_thread = threading.Thread(name='Logging Monitor',
                                                                 target=self.recv_log_msgs_from_overlaynet,
                                                                 daemon=False)
        self.recv_logs_from_overlaynet_thread.start()

        # Start the backend
        log.debug("standing up backend")
        # we need to send to backend only the ip_addr and host_id of the frontend
        log.debug(f"fe_ip_addr={fe_ip_addr}, fe_host_id={fe_host_id}")

        # Send/recv data with my OverlayNet FE server
        send_overlaynet_args = (self.la_fe_stdout,)
        self.send_overlaynet_thread = threading.Thread(name='OverlayNet Sender',
                                                       target=self._send_msgs_to_overlaynet,
                                                       args=send_overlaynet_args,
                                                       daemon=False)
        self.send_overlaynet_thread.start()
        log.info('started overlaynet sender thread')
        if self._sigint_trigger == 3:
            signal.raise_signal(signal.SIGINT)

        recv_overlaynet_args = (self.la_fe_stdin,)
        self.recv_overlaynet_thread = threading.Thread(name='OverlayNet Receiver',
                                                       target=self.recv_msgs_from_overlaynet,
                                                       args=recv_overlaynet_args,
                                                       daemon=False)
        self.recv_overlaynet_thread.start()

        if self._sigint_trigger == 4:
            signal.raise_signal(signal.SIGINT)

        # Start the backend
        log.debug("standing up backend")
        # we need to send to backend only the ip_addr and host_id of the frontend
        log.debug(f"fe_ip_addr={fe_ip_addr}, fe_host_id={fe_host_id}")

        self._STATE = FrontendState.STARTUP

        try:
            self.wlm_proc = self._launch_backend(nnodes=nnodes,
                                                 nodelist=hostnames,
                                                 fe_ip_addr=fe_ip_addr,
                                                 fe_host_id=fe_host_id,
                                                 frontend_sdesc=encoded_inbound_str,
                                                 network_prefix=self.network_prefix)
        except Exception as e:
            log.fatal('FE failed to stand up BE')
            log.debug(f'error: {e}')
            raise RuntimeError('Backend launch failed from launcher frontend') from e

        log.debug("LA BE started on each compute node")
        self._STATE = FrontendState.BACKEND_LAUNCHED

        if self._sigint_trigger == 5 or self._sigint_trigger == 8:
            signal.raise_signal(signal.SIGINT)

        # Receive BEIsUp msg - Try getting a backend channel descriptor
        be_ups = [dlutil.get_with_blocking(self.la_fe_stdin) for _ in range(nnodes)]
        assert len(be_ups) == self.nnodes

        # Construct the number of backend connections based on
        # the hierarchical bcast info and send FENodeIdxBE to those
        # nodes
        log.info(f'received {nnodes} BEIsUp msgs')
        self.conn_outs = self.construct_bcast_tree(net_conf,
                                                   conn_policy,
                                                   be_ups,
                                                   encoded_inbound_str)
        del be_ups

        chs_up = [dlutil.get_with_blocking(self.la_fe_stdin) for _ in range(self.nnodes)]
        for ch_up in chs_up:
            assert isinstance(ch_up, dmsg.SHChannelsUp), 'la_fe received invalid channel up'
        log.info(f'received {nnodes} SHChannelsUP msgs')

        nodes_desc = {ch_up.idx: ch_up.node_desc for ch_up in chs_up}
        gs_cds = [ch_up.gs_cd for ch_up in chs_up if ch_up.gs_cd is not None]
        if len(gs_cds) == 0:
            print('The Global Services CD was not returned by any of the SHChannelsUp messages. Launcher Exiting.')
            sys.exit(LAUNCHER_FAIL_EXIT)
        gs_cd = gs_cds[0]

        # Set the number of gateway channels per node. When
        # HSTA is used, and the pals library is present, then
        # we are on a Shasta (EX) machine and we can then use
        # multi-nic support under HSTA when multiple NICs per
        # node are available. In this circumstance, the number of
        # gateway channels will control the number of HSTA Agent
        # procs created and HSTA uses the default MPI binding to
        # bind each agent to a separate NIC. All other configurations
        # will have one NIC per node and one gateway per node used
        # (for now anyway - eventually the number of gateways may not
        # be tied to multiple NIC support).
        #FIXME-MULTI-NIC: force one nic per node for now
        if (min_nics_per_node > 1) and self.pals_lib_present and (self.transport is dfacts.TransportAgentOptions.HSTA) and False:
            num_gw_channels = min_nics_per_node
        else:
            num_gw_channels = 1

        # HSTA uses NUM_GW_TYPES gateways per agent
        if self.transport is dfacts.TransportAgentOptions.HSTA:
            num_gw_channels *= dfacts.NUM_GW_TYPES

        # Send LAChannelsInfo in a test environment or to all
        la_ch_info = dmsg.LAChannelsInfo(tag=dlutil.next_tag(), nodes_desc=nodes_desc,
                                         gs_cd=gs_cd, num_gw_channels=num_gw_channels,
                                         port=self.port,
                                         transport=str(self.transport))
        log.debug(f'la_fe la_channels.nodes_desc: {la_ch_info.nodes_desc}')
        log.debug(f'la_fe la_channels.gs_cd: {la_ch_info.gs_cd}')
        log.debug(f'la_fe la_channels.transport: {la_ch_info.transport}')
        self.la_fe_stdout.send("A", la_ch_info.serialize())
        log.info('sent LACHannelsInfo to overlaynet fe')

        self.tas_up = [dlutil.get_with_blocking(self.la_fe_stdin) for _ in range(nnodes)]
        for ta_up in self.tas_up:
            assert isinstance(ta_up, dmsg.TAUp), 'la_fe received invalid channel up'
        log.info(f'received {nnodes} TAUp messages')

        if self._sigint_trigger == 6:
            signal.raise_signal(signal.SIGINT)

        if not self.transport_test_env:
            log.info('Now waiting on getting GSIsUp....')
            gs_up = dlutil.get_with_blocking(self.la_fe_stdin)
            assert isinstance(gs_up, dmsg.GSIsUp), 'la_fe expected GSIsUp msg'
            log.info('la_fe received GSIsUp. Prepping launch of user application')

        # Infrastructure is up
        self._STATE = FrontendState.STOOD_UP

    def run_app(self):
        """Start user app execution via GSProcessCreate or SHProcessCreate
        """
        self._STATE = FrontendState.APP_EXECUTION
        log = logging.getLogger(dls.LA_FE).getChild('run_app')

        make_inf_channels = True

        # Send message to start user application
        if self.transport_test_env:
            # start a process on each shepherd with an SHProcessCreate broadcast to all shepherds
            # build a dictionary mapping each node_index to the test channels created on that node.
            node_idx_to_channels_map = dict((msg.idx, msg.test_channels) for msg in self.tas_up)
            start_msg = dlutil.mk_shproc_start_msg(logbase=dls.LA_FE, stdin_str=str(node_idx_to_channels_map))
            self.la_fe_stdout.send('A', start_msg.serialize())
            log.info(f'broadcast the program to start to all shepherds with message={start_msg}')
        else:
            # Send GS user proc start msg
            log.info('Prepping launch of user application')
            start_msg = dlutil.mk_head_proc_start_msg(logbase=dls.LA_FE,
                                                      make_inf_channels=make_inf_channels,
                                                      args_map=self.args_map)
            self.la_fe_stdout.send("P", start_msg.serialize())
            if self._sigint_trigger == 7:
                signal.raise_signal(signal.SIGINT)
            log.info('transmitted GSProcessCreate')

    def run_msg_server(self):
        """Process messages from backend after user app starts
        """
        self.msg_log = logging.getLogger(dls.LA_FE).getChild('run_msg_server')
        running = True
        execute_teardown = True

        while running:

            msg = dlutil.get_with_blocking(self.la_fe_stdin)

            if hasattr(msg, 'r_c_uid'):
                msg.r_c_uid = dfacts.launcher_cuid_from_index(self.node_idx)
            self.msg_log.info(f"received {type(msg)}")

            try:
                if type(msg) in LauncherFrontEnd._DTBL:
                    self._DTBL[type(msg)][0](self, msg=msg)
                else:
                    # TODO: Make sure handling abnormal termniation works this way
                    self.msg_log.warning(f'unexpected msg type: {repr(msg)}')
            except LauncherImmediateExit:
                self.msg_log.debug(f'completing a sys.exit({LAUNCHER_FAIL_EXIT})')
                sys.exit(LAUNCHER_FAIL_EXIT)
            except SigIntImmediateExit:
                self.msg_log.debug('completing an exit due to SIGINT')
                execute_teardown = False
                running = False
                break

            # Break and wait on threads to exit
            if self._sh_halt_be == self.nnodes:
                running = False

        self.msg_log.debug('out of msg_server loop')
        if execute_teardown:
            self.msg_log.info('joining on message threads')
            self._close_threads(abnormal=self._abnormal_termination.is_set())
            self.msg_log.info('la_fe has shutdown overlaynet send/recv threads')

            # Waiting on teardown of wlm launched backend
            self.wlm_proc.wait()

            # TEARDOWN
            self.msg_log.info('WLM launched backend down. Tearing down comm infra')
            self._close_comm_overlaynet()
            self.msg_log.info("Leaving run_msg_server")

        if self._sigint_count > 0:
            self.msg_log.debug('raising keyboard interrupt')
            raise KeyboardInterrupt

        # Set state so our exit method correctly executes.
        self._STATE = FrontendState.LAUNCHER_DOWN

    def probe_teardown(self):
        """Check on whether to begin teardown based on received backend messages"""
        # Global services is up and we're using it
        if self._gs_head_exit_received and self._gs_process_create_resp_received:
            # m4.1 Send GSTeardown to Primary BE

            # Only update this state if we're not in an abnormal teardown. Too many
            # subsequent state checks depend on that differentiation
            if self._STATE != FrontendState.ABNORMAL_TEARDOWN:
                self._STATE = FrontendState.TEARDOWN

            gs_teardown = dmsg.GSTeardown(tag=dlutil.next_tag())
            self.la_fe_stdout.send("P", gs_teardown.serialize())
            self.msg_log.info('m4.1 la_fe transmitted teardown msg to BE')
        # No global services so it's being overlooked
        elif self.transport_test_env:
            if self._proc_create_resps == self.nnodes and self._proc_exits == self.nnodes:
                # m7.1 Send SHHaltTA to All BEs
                self._STATE = FrontendState.TEARDOWN
                sh_halt_ta = dmsg.SHHaltTA(tag=dlutil.next_tag())
                self.la_fe_stdout.send("A", sh_halt_ta.serialize())
                self.msg_log.info('m7.1 transmitted SHHaltTA msg to BE')
        else:
            self.msg_log.debug(f'gs_head_exit: {self._gs_head_exit_received} | gs_proc_create_resp: {self._gs_process_create_resp_received}')

    @route(dmsg.SHFwdOutput, _DTBL)
    def handle_sh_fwd_output(self, msg: dmsg.SHFwdOutput):
        msg_out = self.build_stdmsg(msg, self.args_map,
                                    msg.fd_num == dmsg.SHFwdOutput.FDNum.STDOUT.value)
        print(msg_out, end="")

    @route(dmsg.LAExit, _DTBL)
    def handle_la_exit(self, msg: dmsg.LAExit):
        self.msg_log.debug('Received LAExit message. Will break out of message loop')
        if msg.sigint:
            raise SigIntImmediateExit('Exiting due to receipt of SIGINT')
        else:
            raise LauncherImmediateExit(f'Exiting due to reception of {type(msg)}')

    @route(dmsg.ExceptionlessAbort, _DTBL)
    def handle_exceptionless_abort(self, msg: dmsg.ExceptionlessAbort):
        self.msg_log.debug('exceptionless abort routing')
        self._cleanup_abnormal_state()

    @route(dmsg.GSHeadExit, _DTBL)
    def handle_gs_head_exit(self, msg: dmsg.GSHeadExit):
        self.msg_log.info('The head process has exited')
        self._gs_head_exit_received = True
        self.probe_teardown()

    @route(dmsg.GSProcessCreateResponse, _DTBL)
    def handle_gs_proc_create_response(self, msg: dmsg.GSProcessCreateResponse):
        self.msg_log.info('The GSProcessCreateResponse was received in the launcher front end')
        self._gs_process_create_resp_received = True
        self.probe_teardown()

    @route(dmsg.SHProcessCreateResponse, _DTBL)
    def handle_sh_proc_create_response(self, msg: dmsg.SHProcessCreateResponse):
        self._proc_create_resps += 1
        self.msg_log.info(f'Got {self._proc_create_resps} SHProcessCreateResponse messages')
        self.probe_teardown()

    @route(dmsg.SHProcessExit, _DTBL)
    def handle_sh_proc_exit(self, msg: dmsg.SHProcessExit):
        self._proc_exits += 1
        self.msg_log.info(f'Got {self._proc_exits} SHProcessExit messages')
        self.probe_teardown()

    @route(dmsg.GSHalted, _DTBL)
    def handle_gs_halted(self, msg: dmsg.GSHalted):
        # m6.2 recv GSHalted from Primary BE
        self.msg_log.info('m6.2 la_fe received GSHalted')

        # m7.1 Send SHHaltTA to All BEs
        sh_halt_ta = dmsg.SHHaltTA(tag=dlutil.next_tag())

        if self._STATE != FrontendState.ABNORMAL_TEARDOWN:
            self.la_fe_stdout.send("A", sh_halt_ta.serialize())
            self.msg_log.info(f'm7.1 transmitted SHHaltTA msg to BE via threads: {self._STATE}')
        else:
            self._overlay_bcast(sh_halt_ta)
            self.msg_log.info('m7.1 transmitted SHHaltTA msg to BE directly')

    @route(dmsg.TAHalted, _DTBL)
    def handle_ta_halted(self, msg: dmsg.TAHalted):
        self._tas_halted += 1
        if self._tas_halted == self.nnodes:
            self.msg_log.info('m10.2 la_fe received All TAHalted messages')
            sh_teardown = dmsg.SHTeardown(tag=dlutil.next_tag())

            if self._STATE != FrontendState.ABNORMAL_TEARDOWN:
                # m11.1 Send SHTeardown to all BE's
                self.la_fe_stdout.send("A", sh_teardown.serialize())
                self.msg_log.info('m11.1 la_fe transmitted SHTeardown msg to BE via threads')
            else:
                self._overlay_bcast(sh_teardown)
                self.msg_log.info('m11.1 la_fe transmitted SHTeardown msg to BE directly')

            # Handle one of my sigint test cases
            try:
                if self._sigint_trigger == 9:
                    self.msg_log.debug("raising signal during teardown")
                    self._sigint_trigger = None
                    signal.raise_signal(signal.SIGINT)
                    self.msg_log.debug("SIGINT raised teardown")
            except TypeError:
                pass

    @route(dmsg.SHHaltBE, _DTBL)
    def handle_sh_halt_be(self, msg: dmsg.SHHaltBE):
        self._sh_halt_be += 1
        if self._sh_halt_be == self.nnodes:
            self.msg_log.info('Received all SHHaltBE messages')

            be_halted = dmsg.BEHalted(tag=dlutil.next_tag())

            if self._STATE != FrontendState.ABNORMAL_TEARDOWN:
                self.la_fe_stdout.send("A", be_halted.serialize())
                self.msg_log.info('m14 transmitted BEHalted msg to BE to be forwarded to local services on all nodes via threads')
                self._shutdown.set()

                # signal the end of the logging thread by sending its way
                # an halt message
                msg = dmsg.HaltLoggingInfra(tag=dlutil.next_tag())
                self.dragon_logger.put(msg.serialize(), logging.INFO)

                self.conn_in_bd.send(be_halted.serialize())
                self.msg_log.info('sent BEHalted to break overlay recv out of loop')

            else:
                self._overlay_bcast(be_halted)
                self.msg_log.info(f'm14 transmitted BEHalted msg to BE to be forwarded to local services on all nodes directly')

    def recv_log_msgs_from_overlaynet(self):
        """This service grabs messages from overlay logger and logs them in this

        Args:
            level (int): minimum log priority of messages to forward
        """
        _, level = _get_dragon_log_device_level(os.environ, LOGGING_OUTPUT_DEVICE_DRAGON_FILE)
        log = logging.getLogger(dls.LA_FE).getChild('recv_overlay_logs')
        try:
            # Set timeout to None which allows better interaction with the GIL
            while not self._shutdown.is_set():
                try:
                    msg = dmsg.parse(self.dragon_logger.get(level, timeout=None))
                    if isinstance(msg, dmsg.HaltLoggingInfra):
                        break
                    log = logging.getLogger(msg.name)
                    log.log(msg.level, msg.msg, extra=msg.get_logging_dict())
                except ChannelEmpty:
                    pass

        except Exception as ex:  # pylint: disable=broad-except
            log.warning(f'Caught exception {ex} in logging thread')
        log.info("exiting recv log msg thread")

    def send_msg_to_overlaynet(self, target, msg):
        """Send single message to backend

        Args:
            target (str): "A" for all backend nodes. "P" for primary node only
            msg (dmsg._MsgBase): non-serialized message to send
        """
        try:
            self.la_fe_stdout.send(target, msg.serialize())
        except Exception:
            raise RuntimeError("Unable to send message to overlaynet send thread")

    def _overlay_bcast(self, msg: dmsg._MsgBase):
        '''Send bcast of message to all backend nodes via overlay network

        :param msg: Message to send
        :type msg: dmsg._MsgBase
        '''

        for conn_out in self.conn_outs.values():
            conn_out.send(msg.serialize())

    def _send_msgs_to_overlaynet(self, la_fe_stdout: dlutil.LaOverlayNetFEQueue):
        """Thread that send Messages from frontend to backend service

        Args:
            la_fe_stdout (dlutil.LaOverlayNetFEQueue): SimpleQueue sending messages from launcher
                FE main thread to this OverlayNet server thread
        """
        log = logging.getLogger(dls.LA_FE).getChild('send_msgs_to_overlaynet')

        try:
            # recv msg from parent thread to send to backend
            while not self._shutdown.is_set():
                target, fe_msg = dlutil.get_with_blocking(la_fe_stdout)
                if isinstance(fe_msg, dmsg.HaltOverlay):
                    self._shutdown.set()
                    break

                if target == "P":
                    # node index 0 is the primary node
                    self.conn_outs[0].send(fe_msg.serialize())
                    log.info(f'target = {target} -- msg = {fe_msg.__class__}')
                else:
                    self._overlay_bcast(fe_msg)
                    log.info(f'sent {fe_msg.__class__} down OverlayNet tree')

                if isinstance(fe_msg, dmsg.BEHalted):
                    log.info('overlaynet send thread got halt msg')
                    self._shutdown.set()
                    break
        except Exception as err:
            log.exception(f'overlaynet sending thread failure: {err}')

        # Let the OverlayNet receiving thread know we're gone, and it can exit
        self._shutdown.set()

        # signal the end of the logging thread by sending its way
        # a HaltLoggingInfra message
        msg = dmsg.HaltLoggingInfra(tag=dlutil.next_tag())
        self.dragon_logger.put(msg.serialize(), logging.INFO)

        log.info('overlaynet sending thread exiting ...')

    def recv_msgs_from_overlaynet(self,
                                  la_fe_stdin: dlutil.OverlayNetLaFEQueue):
        """"Start and run OverlayNet FrontEnd service

        Args:
            la_fe_stdin (dlutil.OverlayNetLaFEQueue): SimpleQueue sending messages from
                OverlayNetServer thread to launcher FE main thread
        """

        # Get the logger I will use to write my own log messages
        log = logging.getLogger(dls.LA_FE).getChild('recv_msgs_from_overlaynet')

        try:
            # Just recv messages and send them to my parent thread
            # until my parent thread says stop
            while not self._shutdown.is_set():
                # send msg from callback to parent

                # the get_with_timeout is a decorated function (i.e. wrapped function
                # in a function) that filters out all the log messages coming up through
                # overlaynet. So we don't worry about them here. See dlutil for details.
                try:
                    be_msg = dlutil.get_with_blocking(self.conn_in)
                except AbnormalTerminationError:
                    # I need to get this to the main thread without throwing an exception
                    self._abnormal_termination.set()
                    if self._STATE < FrontendState.TEARDOWN:
                        be_msg = dmsg.ExceptionlessAbort(tag=dlutil.next_tag())
                    else:
                        log.debug("Caught AbnormalTermination during teardown. Skipping it in hopes of completing teardown")
                        continue

                if isinstance(be_msg, dmsg.HaltOverlay):
                    self._shutdown.set()
                    break

                # We could also use decorator here, but not yet...
                if isinstance(be_msg, dmsg.BEHalted):
                    log.info('overlaynet recv thread got halt msg')
                    break
                else:
                    log.info(f'Got {be_msg.__class__} in the front end monitor and forwarding to launcher front end.')
                    la_fe_stdin.send(be_msg.serialize())

            # Let the OverlayNet sending thread know we're gone
            self._shutdown.set()

            # signal the end of the logging thread by sending its way
            # a Halt message
            msg = dmsg.HaltLoggingInfra(tag=dlutil.next_tag())
            self.dragon_logger.put(msg.serialize(), logging.INFO)

            log.info('Overlaynet recv thread exiting.')
        except Exception as err:
            log.exception(f'Overlaynet recving thread failure. Exiting: {err}')
            la_fe_stdin.send(dmsg.AbnormalTermination(tag=dlutil.next_tag()).serialize())
            log.debug('send abnormal term from except block')
            raise

    def build_stdmsg(self, msg: dmsg.SHFwdOutput, arg_map, is_stdout=True):
        if arg_map['no_label']:
            return f'{msg.data}'

        msg_str = ""
        if is_stdout:
            msg_str += "[stdout: "
        else:
            msg_str += "[stderr: "

        if arg_map['verbose_label']:
            msg_str += f"PID {msg.pid} @ {msg.hostname}]"
        elif arg_map['basic_label']:
            msg_str += f"Dragon PID {msg.p_uid}]"

        msg_str += f" {msg.data}"
        return msg_str
