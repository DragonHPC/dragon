import os
import shlex
import socket
import subprocess
import time

from .. import resolve_args
import dragon.channels as dch
from ...globalservices.channel import create, release_refcnt
from ...localservices.options import ChannelOptions as ShepherdChannelOptions
from ...infrastructure import channel_desc as ich_desc
from ...infrastructure import facts as dfacts
from ...infrastructure import messages as dmsg
from ...infrastructure import parameters as dparms
from ...infrastructure import runtime_env as druntime_env
from ...infrastructure import util as dutil
from ...launcher import util as dlutil
from ...utils import B64
from ...infrastructure.queue import InfraQueue


class OutOfBand:
    """Manage proxy bootstrap channels, tunnels, and their route metadata.

    Launcher-managed runtimes inject requests through registered local gateway
    channels. A single-node proxy instead creates a local gateway and starts a
    dedicated OOB transport agent. The legacy TCP OOB listener remains
    available for published runtimes that use that transport.
    """

    def __init__(self, log_sdesc=None, timeout=5, oob_ssh_tunnel_override=None):
        self.tunnel_proc = None
        self.transport_proc = None
        self.ta_input = None
        self.ta_output = None
        self.channels = []
        self.timeout = timeout
        self.log_sdesc = log_sdesc or os.environ.get(dfacts.DRAGON_LOGGER_SDESC)
        self.oob_ssh_tunnel_override = oob_ssh_tunnel_override
        self._saved_env = {}
        self._installed_env = {}
        self._closed = False

    def _stop_subprocess(self, proc):
        if proc is None:
            return None

        try:
            proc.wait(timeout=self.timeout)
        except Exception:
            try:
                proc.terminate()
                proc.wait(timeout=self.timeout)
            except Exception:
                try:
                    proc.kill()
                    proc.wait(timeout=self.timeout)
                except Exception:
                    pass

        return None

    def _transport_selector(self, overlay_transport=None):
        if overlay_transport is None:
            overlay_transport = os.environ.get(
                dfacts.OVERLAY_TRANSPORT_VAR,
                str(dfacts.TransportAgentOptions.TCP),
            )
        args = shlex.split(overlay_transport)
        return os.path.basename(args[0]).lower()

    def _local_ip_addrs(self):
        local_ips = {"127.0.0.1"}

        for host in {socket.gethostname(), socket.getfqdn(), "localhost"}:
            try:
                _name, _aliases, ip_addrs = socket.gethostbyname_ex(host)
            except OSError:
                continue

            local_ips.update(ip_addrs)

        return local_ips

    def _is_local_bootstrap_target(self, fe_ext_ip_addr, head_node_ip_addr):
        local_ips = self._local_ip_addrs()
        return fe_ext_ip_addr in local_ips and head_node_ip_addr in local_ips

    def uses_tcp_oob_transport(self, overlay_transport=None):
        """Return whether the selected overlay transport is legacy TCP OOB."""
        selector = self._transport_selector(overlay_transport)
        return selector in {
            str(dfacts.TransportAgentOptions.TCP),
            getattr(dfacts, "PROCNAME_TCP_TA", "dragon-tcp"),
        }

    def uses_python_tcp_user_transport(self, user_transport=None):
        """Return whether local channel gateways use the legacy Python TCP TA."""
        if user_transport is None:
            user_transport = os.environ.get(dfacts.env_name("TRANSPORT_AGENT"))
        if user_transport is None:
            user_transport = str(
                dfacts.TransportAgentOptions.HSTA if dfacts.HSTA_BINARY.is_file() else dfacts.TransportAgentOptions.TCP
            )

        selector = self._transport_selector(user_transport)
        return selector in {
            str(dfacts.TransportAgentOptions.TCP),
            getattr(dfacts, "PROCNAME_TCP_TA", "dragon-tcp"),
        }

    def _has_existing_gateway(self, env):
        for key, value in env.items():
            if key.startswith(dfacts.GW_ENV_PREFIX) and value:
                suffix = key[len(dfacts.GW_ENV_PREFIX) :]
                if suffix.isdigit():
                    return True

        return False

    def _select_existing_gateway(self, env, remote_rt_uid):
        gateway_items = self._existing_gateway_items(env)

        if not gateway_items:
            raise RuntimeError("no local user gateway descriptors are available for remote runtime bootstrap")

        _, gw_str = gateway_items[remote_rt_uid % len(gateway_items)]
        return gw_str

    def _existing_gateway_items(self, env):
        gateway_items = []

        for key, value in env.items():
            if not key.startswith(dfacts.GW_ENV_PREFIX):
                continue

            suffix = key[len(dfacts.GW_ENV_PREFIX) :]
            if not suffix.isdigit() or not value:
                continue

            gateway_items.append((int(suffix), value))

        gateway_items.sort(key=lambda item: item[0])
        return gateway_items

    def _existing_gateway_port_count(self, env):
        gateway_items = self._existing_gateway_items(env)
        if not gateway_items:
            return 1

        num_gateway_types = max(1, int(getattr(dfacts, "NUM_GW_TYPES", 1)))
        return max(1, len(gateway_items) // num_gateway_types)

    def _local_ports_are_free(self, port, port_count):
        sockets = []
        try:
            for offset in range(port_count):
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                sock.bind(("127.0.0.1", int(port) + offset))
                sockets.append(sock)
        except OSError:
            return False
        finally:
            for sock in sockets:
                sock.close()

        return True

    def _allocate_local_tunnel_port(self, remote_port, port_count):
        start_port = max(int(remote_port) + port_count, int(remote_port) + 100)
        end_port = start_port + 1000

        for port in range(start_port, end_port + 1):
            if self._local_ports_are_free(port, port_count):
                return port

        raise RuntimeError("unable to allocate a local port range for remote runtime bootstrap tunnel")

    def _install_env_update(self, env, key, value):
        if key not in self._saved_env:
            self._saved_env[key] = os.environ.get(key)

        self._installed_env[key] = value
        env[key] = value
        os.environ[key] = value

    def _local_runtime_bootstrap_addr(self, env):
        host = env.get("DRAGON_HEAD_NODE_IP_ADDR") or env.get("DRAGON_FE_EXTERNAL_IP_ADDR") or "127.0.0.1"
        port = env.get("DRAGON_TRANSPORT_PORT", str(dfacts.DEFAULT_TRANSPORT_PORT))
        return f"{host}:{port}"

    def _install_runtime_route(self, env, rt_uid, gw_str, bootstrap_addr=None):
        self._install_env_update(env, druntime_env.runtime_route_env_key(rt_uid), gw_str)
        if bootstrap_addr is not None:
            self._install_env_update(env, druntime_env.runtime_bootstrap_env_key(rt_uid), bootstrap_addr)

    def _install_self_runtime_route(self, env, gw_str):
        local_rt_uid = env.get("DRAGON_RT_UID")
        if local_rt_uid is None:
            return

        self._install_runtime_route(env, local_rt_uid, gw_str, self._local_runtime_bootstrap_addr(env))

    def test_tunnel(self, fe_ext_ip_addr, head_node_ip_addr, port, connect_timeout=10):
        """Run a non-interactive SSH preflight check for the OOB tunnel route.

        The check disables password prompts so host-key and auth issues fail fast
        rather than blocking on an interactive prompt.
        """
        tunnel_args = self._tunnel_args(
            fe_ext_ip_addr,
            head_node_ip_addr,
            local_port=port,
            remote_port=port,
        )

        if len(tunnel_args) == 0 or tunnel_args[0] != "ssh":
            return {
                "ok": False,
                "reason": "unsupported-command",
                "message": "Only ssh-based tunnel commands are supported by test_tunnel.",
                "command": tunnel_args,
                "returncode": None,
                "stdout": "",
                "stderr": "",
            }

        filtered_args = [tunnel_args[0]]
        i = 1
        while i < len(tunnel_args):
            arg = tunnel_args[i]
            # Remove tunnel-open flags so we can execute a simple remote command.
            if arg == "-N":
                i += 1
                continue
            if arg == "-L":
                i += 2
                continue
            filtered_args.append(arg)
            i += 1

        preflight_args = [
            filtered_args[0],
            "-o",
            "BatchMode=yes",
            "-o",
            "NumberOfPasswordPrompts=0",
            "-o",
            f"ConnectTimeout={int(connect_timeout)}",
        ] + filtered_args[1:]

        # Run a harmless command on the remote side.
        preflight_args.append("true")

        cp = subprocess.run(preflight_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        stderr = cp.stderr.strip()
        stdout = cp.stdout.strip()
        stderr_lower = stderr.lower()

        if cp.returncode == 0:
            return {
                "ok": True,
                "reason": "ok",
                "message": "SSH preflight succeeded.",
                "command": preflight_args,
                "returncode": cp.returncode,
                "stdout": stdout,
                "stderr": stderr,
            }

        if (
            "are you sure you want to continue connecting" in stderr_lower
            or "host key verification failed" in stderr_lower
        ):
            reason = "host-key"
            message = "SSH host key trust is not established. Run SSH manually once to accept host keys."
        elif "permission denied" in stderr_lower or "passphrase" in stderr_lower or "password" in stderr_lower:
            reason = "auth"
            message = "SSH authentication failed or requires interactive input."
        elif "connection timed out" in stderr_lower or "operation timed out" in stderr_lower:
            reason = "timeout"
            message = "SSH connection timed out. Verify routing and host reachability."
        else:
            reason = "ssh-error"
            message = "SSH preflight failed. See stderr for details."

        return {
            "ok": False,
            "reason": reason,
            "message": message,
            "command": preflight_args,
            "returncode": cp.returncode,
            "stdout": stdout,
            "stderr": stderr,
        }

    def new_local_channel(self, capacity=128):
        shepherd_options = ShepherdChannelOptions(capacity=capacity)
        channel_options = ich_desc.ChannelOptions(ref_count=True, local_opts=shepherd_options)

        process_index = getattr(dparms.this_process, "index", 0)
        m_uid = dfacts.default_pool_muid_from_index(process_index)
        descriptor = create(m_uid, options=channel_options)
        channel = dch.Channel.attach(descriptor.sdesc)
        self.channels.append(channel)
        return channel

    def _resolve_transport_command(self, overlay_transport=None):
        if overlay_transport is None:
            overlay_transport = os.environ.get(
                dfacts.OVERLAY_TRANSPORT_VAR,
                str(dfacts.TransportAgentOptions.TCP),
            )
        return resolve_args(shlex.split(overlay_transport))

    def _start_transport(self, bootstrap_port, env=None, oob_ip_addr=None, overlay_transport=None):
        output_ch = self.new_local_channel()
        input_ch = self.new_local_channel()
        self.ta_input = InfraQueue(main_channel=input_ch)
        self.ta_output = InfraQueue(main_channel=output_ch)

        if env is None:
            env = dict(os.environ)

        if overlay_transport is not None:
            env[dfacts.OVERLAY_TRANSPORT_VAR] = overlay_transport

        args = self._resolve_transport_command(overlay_transport)
        args.extend(
            [
                f"--oob-port={bootstrap_port}",
                f"--ch-in-sdesc={self.ta_input.serialize()}",
                f"--ch-out-sdesc={self.ta_output.serialize()}",
            ]
        )

        if oob_ip_addr is not None:
            args.append(f"--oob-ip-addr={oob_ip_addr}")

        if self.log_sdesc is not None:
            args.extend([f"--log-sdesc={self.log_sdesc}", "--dragon-logging=true", "--log-level=DEBUG"])
        else:
            args.append("--no-dragon-logging")

        # node_index is unused for OOB agent startup but remains required by both TCP and HSTA CLIs.
        args.append("0")

        self.transport_proc = subprocess.Popen(args, env=env)

    def _start_connect_transport(
        self,
        fe_ext_ip_addr,
        head_node_ip_addr,
        bootstrap_port,
        overlay_transport=None,
        remote_rt_uid=None,
        local_bootstrap_port=None,
    ):
        env = dict(os.environ)
        if remote_rt_uid is None:
            remote_rt_uid = dutil.rt_uid_from_ip_addrs(fe_ext_ip_addr, head_node_ip_addr, bootstrap_port)
        if local_bootstrap_port is None:
            local_bootstrap_port = bootstrap_port
        gw_ch = self.new_local_channel()
        gw_str = B64.bytes_to_str(gw_ch.serialize())

        self._install_self_runtime_route(env, gw_str)
        env["DRAGON_REMOTE_RT_UID"] = str(remote_rt_uid)
        self._install_runtime_route(env, remote_rt_uid, gw_str, f"127.0.0.1:{local_bootstrap_port}")
        env[f"{dfacts.OOB_GW_ENV_PREFIX}1"] = gw_str

        self._install_runtime_route(os.environ, remote_rt_uid, gw_str, f"127.0.0.1:{local_bootstrap_port}")
        self._start_transport(
            local_bootstrap_port,
            env=env,
            oob_ip_addr="127.0.0.1",
            overlay_transport=overlay_transport,
        )

    def _tunnel_args(self, fe_ext_ip_addr, head_node_ip_addr, local_port, remote_port=None, port_count=1):
        if remote_port is None:
            remote_port = local_port

        if self.oob_ssh_tunnel_override is not None:
            return self.oob_ssh_tunnel_override.format(
                fe_ext_ip_addr=fe_ext_ip_addr,
                head_node_ip_addr=head_node_ip_addr,
                port=local_port,
            ).split()

        args = [
            "ssh",
            "-J",
            f"{fe_ext_ip_addr}",
            "-o",
            "ExitOnForwardFailure=yes",
            "-N",
        ]

        for offset in range(port_count):
            args.extend(
                [
                    "-L",
                    f"{int(local_port) + offset}:localhost:{int(remote_port) + offset}",
                ]
            )

        args.append(f"{head_node_ip_addr}")
        return args

    def _wait_for_tunnel(self, port, port_count=1):
        start = time.time()
        while time.time() - start < self.timeout:
            try:
                sockets = []
                for offset in range(port_count):
                    sock = socket.create_connection(("localhost", int(port) + offset), timeout=1)
                    sockets.append(sock)

                for sock in sockets:
                    sock.close()
                return
            except Exception:
                time.sleep(0.2)

        raise RuntimeError("ssh tunnel failed to establish")

    def setup_gateways(
        self,
        env,
        fe_ext_ip_addr,
        head_node_ip_addr,
        bootstrap_port,
        remote_overlay_transport=None,
        remote_rt_uid=None,
        local_bootstrap_port=None,
    ):
        """Install a local gateway and portable route metadata for a runtime.

        Existing launcher gateways use the remote head-node address because a
        localhost tunnel exists only on the node that created it. Without an
        existing gateway, this starts the single-node OOB transport path and
        publishes a runtime-local gateway descriptor for channel attachment.
        """
        if remote_rt_uid is None:
            remote_rt_uid = dutil.rt_uid_from_ip_addrs(fe_ext_ip_addr, head_node_ip_addr, bootstrap_port)

        if local_bootstrap_port is None:
            local_bootstrap_port = bootstrap_port

        local_user_transport = env.get(dfacts.env_name("TRANSPORT_AGENT"))
        use_python_tcp_oob_route = self.uses_python_tcp_user_transport(local_user_transport)

        if self._has_existing_gateway(env) and not use_python_tcp_oob_route:
            gw_str = self._select_existing_gateway(env, remote_rt_uid)
            # In the multi-node (launcher-managed gateway) path the cross-runtime
            # bootstrap ingress is carried in every gateway message and used by
            # the HSTA transport agent on whichever node services that message.
            # A 127.0.0.1 tunnel endpoint only exists on the node that started
            # the SSH tunnel (the head node), so compute-node agents could never
            # reach it and would hang waiting for host-id translation. Advertise
            # the remote runtime's real head-node address instead, which is
            # reachable from every node over the high-speed network.
            bootstrap_addr = f"{head_node_ip_addr}:{bootstrap_port}"
            self._install_self_runtime_route(env, gw_str)

            # Bootstrap state is keyed by remote runtime UID now. Do not publish a
            # singleton DRAGON_REMOTE_RT_UID in the combined-TA path.
            env.pop("DRAGON_REMOTE_RT_UID", None)
            os.environ.pop("DRAGON_REMOTE_RT_UID", None)

            self._install_runtime_route(env, remote_rt_uid, gw_str, bootstrap_addr)
            return

        if use_python_tcp_oob_route:
            # The Python TCP TA routes by host ID and cannot consume the
            # runtime-aware fields used by the launcher-managed mesh gateways.
            # Mark this remote so C uses the per-runtime OOB connector below.
            route_mode_key = f"DRAGON_RT_UID__{int(remote_rt_uid)}_PYTHON_TCP_OOB_ROUTE"
            self._install_env_update(env, route_mode_key, "1")
            if env is not os.environ:
                self._install_env_update(os.environ, route_mode_key, "1")

        # Single-node Dragon does not stand up launcher-managed gateway channels.
        # Fall back to the connect-side bootstrap path by creating a local
        # gateway and an OOB transport agent that matches the remote runtime's
        # transport backend when it is known.
        self._start_connect_transport(
            fe_ext_ip_addr,
            head_node_ip_addr,
            bootstrap_port,
            overlay_transport=remote_overlay_transport,
            remote_rt_uid=remote_rt_uid,
            local_bootstrap_port=local_bootstrap_port,
        )

    def connect(self, fe_ext_ip_addr, head_node_ip_addr, port, remote_overlay_transport=None, remote_rt_uid=None):
        """Connect to a published runtime, tunneling its bootstrap port if needed."""
        if self._is_local_bootstrap_target(fe_ext_ip_addr, head_node_ip_addr):
            self.setup_gateways(
                os.environ,
                fe_ext_ip_addr,
                head_node_ip_addr,
                port,
                remote_overlay_transport=remote_overlay_transport,
                remote_rt_uid=remote_rt_uid,
                local_bootstrap_port=port,
            )
            return

        tunnel_port_count = 1
        local_tunnel_port = int(port)

        if not self.uses_tcp_oob_transport(remote_overlay_transport) and self._has_existing_gateway(os.environ):
            tunnel_port_count = self._existing_gateway_port_count(os.environ)
            local_tunnel_port = self._allocate_local_tunnel_port(port, tunnel_port_count)

        self.tunnel_proc = subprocess.Popen(
            self._tunnel_args(
                fe_ext_ip_addr,
                head_node_ip_addr,
                local_tunnel_port,
                remote_port=port,
                port_count=tunnel_port_count,
            ),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        self._wait_for_tunnel(local_tunnel_port, tunnel_port_count)

        self.setup_gateways(
            os.environ,
            fe_ext_ip_addr,
            head_node_ip_addr,
            port,
            remote_overlay_transport=remote_overlay_transport,
            remote_rt_uid=remote_rt_uid,
            local_bootstrap_port=local_tunnel_port,
        )

    def accept(self, port):
        """Start the legacy TCP OOB listener; HSTA uses the user transport agent."""
        # Only the legacy TCP OOB bootstrap still needs a dedicated accept-side
        # helper. HSTA routes remote-runtime control traffic through the user TA.
        if not self.uses_python_tcp_user_transport():
            return False

        if self.transport_proc is None:
            self._start_transport(port)
        return True

    def close(self):
        """Stop tunnels and transport helpers, restore route environment, and detach channels."""
        if self._closed:
            return

        self._closed = True

        if self.tunnel_proc is not None:
            self.tunnel_proc = self._stop_subprocess(self.tunnel_proc)

        if self.ta_input is not None:
            try:
                self.ta_input.put(dmsg.UserHaltOOB(tag=dlutil.next_tag()))
            except Exception:
                pass
            finally:
                try:
                    self.ta_input.close()
                except Exception:
                    pass
                self.ta_input = None

        if self.ta_output is not None:
            try:
                self.ta_output.close()
            except Exception:
                pass
            self.ta_output = None

        if self.transport_proc is not None:
            self.transport_proc = self._stop_subprocess(self.transport_proc)

        for key, value in self._installed_env.items():
            if os.environ.get(key) != value:
                continue

            previous_value = self._saved_env.get(key)
            if previous_value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = previous_value

        self._installed_env.clear()
        self._saved_env.clear()

        for channel in self.channels:
            try:
                release_refcnt(channel.cuid)
            except Exception:
                pass
            try:
                channel.detach()
            except Exception:
                pass
        self.channels.clear()

    def __del__(self):
        self.close()
