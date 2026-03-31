import cmd
import ctypes
import os
import shlex
import shutil
import signal
import socket
import subprocess
import time

from ... import channels as dch
from ... import managed_memory as dmm
from ...globalservices.channel import create, release_refcnt
from ...localservices.options import ChannelOptions as ShepherdChannelOptions
from ...infrastructure.channel_desc import ChannelOptions
from ...infrastructure import connection as dconn
from ...infrastructure import facts as dfacts
from ...infrastructure import messages as dmsg
from ...infrastructure import parameters as dparms
from ...infrastructure import util as dutil
from ...launcher import util as dlutil
from ...dlogging import util as dlogutil
from ...utils import B64
from ...infrastructure.parameters import this_process


def set_pdeathsig():
    """
    Kills the ssh tunnel on shutdown/failure
    """
    PR_SET_PDEATHSIG = 1
    libc = ctypes.CDLL("libc.so")
    libc.prctl(PR_SET_PDEATHSIG, signal.SIGTERM)


class OutOfBand:

    def __init__(self, log_sdesc=None, timeout=5, oob_ssh_tunnel_override=None):
        self.connecting_ta = False
        self.accepting_ta = True
        self.ta_started = False
        self.ta_input = None
        self.tunnel_proc = None
        self.port = None
        self.channels = []
        self.log_sdesc = log_sdesc
        self.timeout = timeout
        self.oob_ssh_tunnel_override = oob_ssh_tunnel_override

    # TODO: need to guarantee that this is channel local
    def new_local_channel(self, capacity=128):
        sh_channel_options = ShepherdChannelOptions(capacity)
        gs_channel_options = ChannelOptions(ref_count=True, local_opts=sh_channel_options)

        m_uid = dfacts.default_pool_muid_from_index(dparms.this_process.index)
        descriptor = create(m_uid, options=gs_channel_options)
        ch = dch.Channel.attach(descriptor.sdesc)
        self.channels.append(ch)

        return ch

    def setup_gateways(self, env, fe_ext_ip_addr, head_node_ip_addr):
        # create a gateway channel and associate it with the remote runtime
        gw_ch = self.new_local_channel()
        gw_str = B64.bytes_to_str(gw_ch.serialize())
        remote_rt_uid = dutil.rt_uid_from_ip_addrs(fe_ext_ip_addr, head_node_ip_addr)

        env["DRAGON_REMOTE_RT_UID"] = str(remote_rt_uid)
        env[f"DRAGON_RT_UID__{remote_rt_uid}"] = gw_str
        os.environ[f"DRAGON_RT_UID__{remote_rt_uid}"] = gw_str

        env[f"{dfacts.OOB_GW_ENV_PREFIX}1"] = gw_str

    def start_oob_transport(self, fe_ext_ip_addr=None, head_node_ip_addr=None, port=None):
        # save port
        self.port = port

        print(f"Starting OOB transport agents on port {port}", flush=True)
        # create tcp agents input and output channels
        output_ch = self.new_local_channel()
        input_ch = self.new_local_channel()
        self.ta_input = dconn.Connection(outbound_initializer=input_ch, policy=dparms.POLICY_INFRASTRUCTURE)

        env = dict(os.environ)

        # TODO: Allow the user to set the transport used for oob agents
        overlay_transport = os.environ[dfacts.OVERLAY_TRANSPORT_VAR]
        args = shlex.split(overlay_transport)
        alias = dfacts.TRANSPORT_AGENT_ALIASES.get(args[0].lower())
        if alias:
            args = alias + args[1:]
        # Resolve executable
        cmd = shutil.which(args[0])
        if cmd:  # Only replace if resolved
            args[0] = cmd

        args = args + [
            f"--oob-port={port}",
            f"--ch-in-sdesc={B64.bytes_to_str(input_ch.serialize())}",
            f"--ch-out-sdesc={B64.bytes_to_str(output_ch.serialize())}",
        ]

        self.log_sdesc = os.environ.get(dfacts.DRAGON_LOGGER_SDESC, None)

        if self.log_sdesc != None:
            args.append(f"--log-sdesc={self.log_sdesc}")
            args.append(f"--dragon-logging=true")
            args.append(f"--log-level=DEBUG")
        else:
            args.append(f"--no-dragon-logging")

        # this should only be true on the connecting side
        if head_node_ip_addr != None:
            self.setup_gateways(env, fe_ext_ip_addr, head_node_ip_addr)
            oob_ip_addr = "127.0.0.1"
            args.append(f"--oob-ip-addr={oob_ip_addr}")

        # this sets an arbitrary value for node_index
        args.append(str(0))

        subprocess.Popen(args, env=env)

    def connect(self, fe_ext_ip_addr, head_node_ip_addr, port):
        print(
            f"Connecting to OOB transport agents on port {port} with {fe_ext_ip_addr} and {head_node_ip_addr}",
            flush=True,
        )
        self.connecting_ta = True
        if not self.ta_started:
            if self.oob_ssh_tunnel_override is not None:
                tunnel_args = self.oob_ssh_tunnel_override.format(
                    fe_ext_ip_addr=fe_ext_ip_addr, head_node_ip_addr=head_node_ip_addr, port=port
                ).split()
            else:
                tunnel_args = [
                    "ssh",
                    "-J",
                    f"{fe_ext_ip_addr}",
                    "-o",
                    "ExitOnForwardFailure=yes",
                    "-N",
                    "-L",
                    f"{port}:localhost:{port}",
                    f"{head_node_ip_addr}",
                ]

            # TODO: this should help clean up the tunnel if the parent process dies, but
            # we're currently seeing an error in the pre-exec function
            # self.tunnel_proc = subprocess.Popen(
            #     tunnel_args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, preexec_fn=set_pdeathsig
            # )
            self.tunnel_proc = subprocess.Popen(tunnel_args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

            start = time.time()
            while time.time() - start < self.timeout:
                try:
                    s = socket.create_connection(("localhost", int(port)), timeout=1)
                    s.close()
                    break
                except Exception:
                    time.sleep(0.2)
            else:
                raise RuntimeError("ssh tunnel failed to establish")

            self.start_oob_transport(fe_ext_ip_addr=fe_ext_ip_addr, head_node_ip_addr=head_node_ip_addr, port=port)
            self.ta_started = True

    def accept(self, port):
        self.accepting_ta = True
        if not self.ta_started:
            self.start_oob_transport(port=port)
            self.ta_started = True

    def __del__(self):
        # kill the (local) ssh tunnel
        if self.tunnel_proc is not None:
            self.tunnel_proc.terminate()

        # halt the OOB transport agents
        if self.ta_started:
            halt_msg = dmsg.UserHaltOOB(tag=dlutil.next_tag())
            self.ta_input.send(halt_msg.serialize())

        # clean up input/output channels used for OOB transport agents
        for ch in self.channels:
            release_refcnt(ch.cuid)
            ch.detach()
