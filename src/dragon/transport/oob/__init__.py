import os
import subprocess
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
from ...utils import B64


class OutOfBand:

    def __init__(self, log_sdesc=None):
        self.connecting_ta = False
        self.accepting_ta = True
        self.ta_started = False
        self.ta_input = None
        self.tunnel_proc = None
        self.port = None
        self.channels = []
        self.log_sdesc = log_sdesc

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

    def start_oob_transport(self, fe_ext_ip_addr=None, head_node_ip_addr=None, port=None):
        # save port
        self.port = port

        # create tcp agents input and output channels
        output_ch = self.new_local_channel()
        input_ch = self.new_local_channel()
        self.ta_input = dconn.Connection(outbound_initializer=input_ch, policy=dparms.POLICY_INFRASTRUCTURE)

        env = dict(os.environ)

        args = [
            "python3",
            "-m",
            "dragon.cli",
            "dragon-tcp",
            f"--oob-port={port}",
            "--no-tls",
            "--no-tls-verify",
            f"--ch-in-sdesc={B64.bytes_to_str(input_ch.serialize())}",
            f"--ch-out-sdesc={B64.bytes_to_str(output_ch.serialize())}",
        ]

        if self.log_sdesc != None:
            args.append(f"--log-sdesc={self.log_sdesc}")
            args.append(f"--dragon-logging=true")
            args.append(f"--log-level=DEBUG")
        else:
            args.append(f"--no-dragon-logging")

        # this should only be true on the connecting side
        if head_node_ip_addr != None:
            self.setup_gateways(env, fe_ext_ip_addr, head_node_ip_addr)
            oob_ip_addr = head_node_ip_addr
            args.append(f"--oob-ip-addr={oob_ip_addr}")

        # this sets an arbitrary value for node_index
        args.append(str(0))

        subprocess.Popen(args, env=env)

    def connect(self, fe_ext_ip_addr, head_node_ip_addr, port):
        self.connecting_ta = True
        if not self.ta_started:
            tunnel_args = [
                "ssh",
                "-J",
                f"{fe_ext_ip_addr}",
                "-N",
                "-L",
                f"{port}:localhost:{port}",
                "-f",
                f"{head_node_ip_addr}",
                ">",
                "/dev/null",
                "2>&1",
            ]
            self.tunnel_proc = subprocess.Popen(tunnel_args)
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

        # kill process that accepts incoming tcp connections
        # TODO: Improve this. We should know which process is being killed
        # and kill it explicitly.
        if self.accepting_ta:
            os.system(f"kill $(lsof -t -i:{self.port}) > /dev/null 2>&1")
