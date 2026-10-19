"""Unit tests for dragon.transport.oob.OutOfBand.

Tests cover _tunnel_args and test_tunnel (SSH preflight) using
unittest.mock to avoid any real subprocess/network calls.

Note: test/proxy/ is a separate, untouched functional multi-node suite —
these tests here are pure standard-Python-mode unit tests with no live Dragon
runtime.
"""

import os
import subprocess
import sys
import unittest
from unittest.mock import MagicMock, call, patch

from dragon.transport.oob import OutOfBand


class TestTunnelArgs(unittest.TestCase):
    """Tests for OutOfBand._tunnel_args."""

    def setUp(self):
        self.oob = OutOfBand()

    def test_default_tunnel_args(self):
        """Default (no override) produces a standard ssh -J ... -N -L ... command."""
        args = self.oob._tunnel_args("fe.example.com", "compute.example.com", 7575)
        self.assertEqual(args[0], "ssh")
        self.assertIn("-J", args)
        self.assertIn("fe.example.com", args)
        self.assertIn("-N", args)
        self.assertIn("-L", args)
        self.assertIn("7575:localhost:7575", args)
        self.assertIn("compute.example.com", args)

    def test_override_placeholder_substitution(self):
        """Override template placeholders are substituted before splitting."""
        template = "ssh -J {fe_ext_ip_addr} -N -L {port}:localhost:{port} {head_node_ip_addr}"
        oob = OutOfBand(oob_ssh_tunnel_override=template)
        args = oob._tunnel_args("jump.host", "node.host", 8888)
        self.assertIn("jump.host", args)
        self.assertIn("node.host", args)
        self.assertIn("8888:localhost:8888", args)

    def test_override_missing_placeholder(self):
        """Override template with an unknown placeholder raises KeyError from .format()."""
        oob = OutOfBand(oob_ssh_tunnel_override="ssh -J {unknown_placeholder} {head_node_ip_addr}")
        with self.assertRaises(KeyError):
            oob._tunnel_args("fe.host", "node.host", 7575)

    def test_override_empty_string(self):
        """Empty oob_ssh_tunnel_override string produces an empty args list."""
        oob = OutOfBand(oob_ssh_tunnel_override="")
        args = oob._tunnel_args("fe.host", "node.host", 7575)
        self.assertEqual(args, [])

    def test_port_is_stringified(self):
        """Integer port is correctly interpolated into the -L flag string."""
        oob = OutOfBand()
        args = oob._tunnel_args("fe.host", "node.host", 7575)
        self.assertIn("7575:localhost:7575", args)


class TestTestTunnel(unittest.TestCase):
    """Tests for OutOfBand.test_tunnel (SSH preflight check)."""

    def _make_completed_process(self, returncode, stdout="", stderr=""):
        cp = MagicMock()
        cp.returncode = returncode
        cp.stdout = stdout
        cp.stderr = stderr
        return cp

    def test_success_returns_ok(self):
        """returncode==0 → ok=True, reason='ok'."""
        oob = OutOfBand()
        cp = self._make_completed_process(0, stdout="", stderr="")
        with patch("dragon.transport.oob.subprocess.run", return_value=cp) as mock_run:
            result = oob.test_tunnel("fe.host", "compute.host", 7575)
        self.assertTrue(result["ok"])
        self.assertEqual(result["reason"], "ok")
        self.assertEqual(result["returncode"], 0)
        mock_run.assert_called_once()

    def test_host_key_failure(self):
        """'host key verification failed' in stderr → reason='host-key'."""
        oob = OutOfBand()
        cp = self._make_completed_process(255, stderr="Host key verification failed.")
        with patch("dragon.transport.oob.subprocess.run", return_value=cp):
            result = oob.test_tunnel("fe.host", "compute.host", 7575)
        self.assertFalse(result["ok"])
        self.assertEqual(result["reason"], "host-key")

    def test_auth_failure(self):
        """'permission denied' in stderr → reason='auth'."""
        oob = OutOfBand()
        cp = self._make_completed_process(255, stderr="Permission denied (publickey).")
        with patch("dragon.transport.oob.subprocess.run", return_value=cp):
            result = oob.test_tunnel("fe.host", "compute.host", 7575)
        self.assertFalse(result["ok"])
        self.assertEqual(result["reason"], "auth")

    def test_timeout_failure(self):
        """'connection timed out' in stderr → reason='timeout'."""
        oob = OutOfBand()
        cp = self._make_completed_process(255, stderr="ssh: connect to host compute.host port 22: Connection timed out")
        with patch("dragon.transport.oob.subprocess.run", return_value=cp):
            result = oob.test_tunnel("fe.host", "compute.host", 7575)
        self.assertFalse(result["ok"])
        self.assertEqual(result["reason"], "timeout")

    def test_generic_ssh_error(self):
        """Unrecognised stderr → reason='ssh-error'."""
        oob = OutOfBand()
        cp = self._make_completed_process(255, stderr="Some unexpected ssh error occurred")
        with patch("dragon.transport.oob.subprocess.run", return_value=cp):
            result = oob.test_tunnel("fe.host", "compute.host", 7575)
        self.assertFalse(result["ok"])
        self.assertEqual(result["reason"], "ssh-error")

    def test_non_ssh_override_rejected(self):
        """Non-ssh override command returns unsupported-command without calling subprocess.run."""
        oob = OutOfBand(oob_ssh_tunnel_override="nc -z {head_node_ip_addr} {port}")
        with patch("dragon.transport.oob.subprocess.run") as mock_run:
            result = oob.test_tunnel("fe.host", "compute.host", 7575)
        self.assertFalse(result["ok"])
        self.assertEqual(result["reason"], "unsupported-command")
        mock_run.assert_not_called()

    def test_connect_timeout_passed(self):
        """connect_timeout value is forwarded as ConnectTimeout= in preflight args."""
        oob = OutOfBand()
        cp = self._make_completed_process(0)
        with patch("dragon.transport.oob.subprocess.run", return_value=cp) as mock_run:
            oob.test_tunnel("fe.host", "compute.host", 7575, connect_timeout=30)
        call_args = mock_run.call_args[0][0]  # first positional arg = preflight_args list
        self.assertIn("ConnectTimeout=30", call_args)

    def test_subprocess_run_timeout_exception(self):
        """subprocess.run raising TimeoutExpired propagates out of test_tunnel uncaught."""
        oob = OutOfBand()
        with patch(
            "dragon.transport.oob.subprocess.run",
            side_effect=subprocess.TimeoutExpired(cmd="ssh", timeout=10),
        ):
            with self.assertRaises(subprocess.TimeoutExpired):
                oob.test_tunnel("fe.host", "compute.host", 7575)


# ---------------------------------------------------------------------------
# TestInit
# ---------------------------------------------------------------------------


class TestInit(unittest.TestCase):
    """Tests for OutOfBand.__init__."""

    def test_default_attributes(self):
        """Default constructor sets all attributes to documented defaults."""
        oob = OutOfBand()
        self.assertIsNone(oob.ta_input)
        self.assertIsNone(oob.ta_output)
        self.assertIsNone(oob.tunnel_proc)
        self.assertIsNone(oob.transport_proc)
        self.assertEqual(oob.channels, [])
        self.assertEqual(oob.log_sdesc, os.environ.get("DRAGON_LOGGER_SDESC"))
        self.assertEqual(oob.timeout, 5)
        self.assertIsNone(oob.oob_ssh_tunnel_override)

    def test_custom_arguments(self):
        """Constructor stores custom log_sdesc, timeout, and override correctly."""
        oob = OutOfBand(log_sdesc="some-sdesc", timeout=42, oob_ssh_tunnel_override="cmd {port}")
        self.assertEqual(oob.log_sdesc, "some-sdesc")
        self.assertEqual(oob.timeout, 42)
        self.assertEqual(oob.oob_ssh_tunnel_override, "cmd {port}")


# ---------------------------------------------------------------------------
# TestNewLocalChannel
# ---------------------------------------------------------------------------


class TestNewLocalChannel(unittest.TestCase):
    """Tests for OutOfBand.new_local_channel."""

    def setUp(self):
        self.oob = OutOfBand()

        # dch.Channel is a Cython extension type (immutable), so patch the whole dch namespace
        self.mock_dch = MagicMock()
        self.mock_attach_ch = MagicMock()
        self.mock_dch.Channel.attach.return_value = self.mock_attach_ch

        self.p_dch = patch("dragon.transport.oob.dch", self.mock_dch)
        self.p_this_process = patch("dragon.transport.oob.dparms.this_process")
        self.p_muid = patch("dragon.transport.oob.dfacts.default_pool_muid_from_index", return_value=42)
        self.p_create = patch("dragon.transport.oob.create")

        self.mock_dch_mod = self.p_dch.start()
        self.mock_this_process = self.p_this_process.start()
        self.mock_muid = self.p_muid.start()
        self.mock_create = self.p_create.start()

        self.mock_this_process.index = 0
        self.mock_create.return_value = MagicMock()

    def tearDown(self):
        _neutralize_for_gc(self.oob)
        self.p_dch.stop()
        self.p_this_process.stop()
        self.p_muid.stop()
        self.p_create.stop()

    def test_returns_attached_channel(self):
        """new_local_channel returns the channel returned by dch.Channel.attach."""
        ch = self.oob.new_local_channel()
        self.assertIs(ch, self.mock_attach_ch)

    def test_passes_capacity_and_options(self):
        """new_local_channel passes the m_uid from default_pool_muid_from_index to create."""
        self.oob.new_local_channel()
        self.mock_create.assert_called_once()
        call_args = self.mock_create.call_args
        self.assertEqual(call_args[0][0], 42)

    def test_custom_capacity(self):
        """new_local_channel forwards a non-default capacity to ShepherdChannelOptions."""
        with patch("dragon.transport.oob.ShepherdChannelOptions") as mock_sh_opts:
            self.oob.new_local_channel(capacity=512)
        mock_sh_opts.assert_called_once_with(capacity=512)

    def test_multiple_calls_accumulate(self):
        """Each call to new_local_channel appends to self.channels."""
        ch1 = MagicMock()
        ch2 = MagicMock()
        self.mock_dch.Channel.attach.side_effect = [ch1, ch2]
        self.oob.new_local_channel()
        self.oob.new_local_channel()
        self.assertEqual(len(self.oob.channels), 2)
        self.assertIs(self.oob.channels[0], ch1)
        self.assertIs(self.oob.channels[1], ch2)


# ---------------------------------------------------------------------------
# TestSetupGateways
# ---------------------------------------------------------------------------


class TestSetupGateways(unittest.TestCase):
    """Tests for OutOfBand.setup_gateways."""

    def setUp(self):
        self.oob = OutOfBand()
        self.mock_gw_ch = MagicMock()
        self.mock_gw_ch.serialize.return_value = b"gw-bytes"
        self.mock_new_local_channel = MagicMock(return_value=self.mock_gw_ch)
        self.oob.new_local_channel = self.mock_new_local_channel
        self.oob._start_transport = MagicMock()

        self.p_rt_uid = patch("dragon.transport.oob.dutil.rt_uid_from_ip_addrs", return_value=9999)
        # B64 is a class with static/class methods; patch the whole name in the oob module
        self.mock_b64_cls = MagicMock()
        self.mock_b64_cls.bytes_to_str.return_value = "gw-b64-str"
        self.p_b64 = patch("dragon.transport.oob.B64", self.mock_b64_cls)
        self.mock_rt_uid = self.p_rt_uid.start()
        self.p_b64.start()

    def tearDown(self):
        self.p_rt_uid.stop()
        self.p_b64.stop()

    def test_single_node_delegates_to_connect_transport(self):
        """setup_gateways delegates single-node bootstrap to the connect path."""
        mock_start_connect_transport = MagicMock()
        self.oob._start_connect_transport = mock_start_connect_transport
        env = {}
        self.oob.setup_gateways(env, "fe.host", "node.host", 7575)
        mock_start_connect_transport.assert_called_once_with(
            "fe.host",
            "node.host",
            7575,
            overlay_transport=None,
            remote_rt_uid=9999,
            local_bootstrap_port=7575,
        )

    def test_os_environ_side_effect(self):
        """setup_gateways mutates os.environ as a documented side-effect (design smell)."""
        env = {}
        with patch.dict(os.environ, {}, clear=False):
            self.oob.setup_gateways(env, "fe.host", "node.host", 7575)
            self.assertIn("DRAGON_RT_UID__9999", os.environ)

    def test_new_local_channel_called(self):
        """setup_gateways calls new_local_channel exactly once to obtain the gateway channel."""
        env = {}
        self.oob.setup_gateways(env, "fe.host", "node.host", 7575)
        self.mock_new_local_channel.assert_called_once()


# ---------------------------------------------------------------------------
# TestStartTransport
# ---------------------------------------------------------------------------

from dragon.infrastructure import facts as _dfacts  # needed for constant access


class TestStartTransport(unittest.TestCase):
    """Tests for OutOfBand._start_transport."""

    _BASE_ENV = {_dfacts.OVERLAY_TRANSPORT_VAR: "mytransport arg1"}

    def setUp(self):
        self.env_patcher = patch.dict(os.environ, self._BASE_ENV, clear=False)
        # Remove DRAGON_LOGGER_SDESC if present so tests start clean
        if _dfacts.DRAGON_LOGGER_SDESC in os.environ:
            os.environ.pop(_dfacts.DRAGON_LOGGER_SDESC)
        self.env_patcher.start()

        self.oob = OutOfBand()

        self.output_mock = MagicMock(name="output_ch")
        self.input_mock = MagicMock(name="input_ch")
        self.oob.new_local_channel = MagicMock(side_effect=[self.output_mock, self.input_mock])

        self.p_infraqueue = patch("dragon.transport.oob.InfraQueue")
        self.p_popen = patch("dragon.transport.oob.subprocess.Popen")

        self.mock_infraqueue = self.p_infraqueue.start()
        self.mock_popen = self.p_popen.start()

    def tearDown(self):
        self.p_infraqueue.stop()
        self.p_popen.stop()
        self.env_patcher.stop()

    def _all_popen_args(self):
        """Return the args list passed to subprocess.Popen."""
        return self.mock_popen.call_args[0][0]

    def test_forwards_bootstrap_port(self):
        """_start_transport forwards the bootstrap port to the transport agent."""
        self.oob._start_transport(7575)
        self.assertIn("--oob-port=7575", self._all_popen_args())

    def test_creates_two_channels_correct_assignment(self):
        """output_ch is new_local_channel()[0]; input_ch is [1]; ta_input gets input_ch, ta_output gets output_ch."""
        self.oob._start_transport(7575)
        iq_calls = self.mock_infraqueue.call_args_list
        self.assertEqual(len(iq_calls), 2)
        # Source: ta_input = InfraQueue(main_channel=input_ch) first,
        #         ta_output = InfraQueue(main_channel=output_ch) second.
        # new_local_channel side_effect: first call → output_mock, second → input_mock.
        self.assertEqual(iq_calls[0], call(main_channel=self.input_mock))
        self.assertEqual(iq_calls[1], call(main_channel=self.output_mock))

    def test_popen_called_with_base_args(self):
        """Popen args include --oob-port=, --ch-in-sdesc=, --ch-out-sdesc=, and trailing '0'."""
        self.oob._start_transport(7575)
        args = self._all_popen_args()
        joined = " ".join(str(a) for a in args)
        self.assertIn("--oob-port=7575", joined)
        self.assertIn("--ch-in-sdesc=", joined)
        self.assertIn("--ch-out-sdesc=", joined)
        self.assertEqual(args[-1], "0")

    def test_transport_alias_resolved(self):
        """When args[0].lower() matches an alias, args is replaced with alias + rest."""
        with patch.dict(os.environ, {_dfacts.OVERLAY_TRANSPORT_VAR: "myalias extraarg"}):
            with patch.dict(
                "dragon.transport.oob.dfacts.TRANSPORT_AGENT_ALIASES",
                {"myalias": ["resolved-bin", "--flag"]},
            ):
                self.oob.new_local_channel = MagicMock(side_effect=[MagicMock(), MagicMock()])
                self.oob._start_transport(8000)
        args = self._all_popen_args()
        # alias replaces first token; original extra args follow
        self.assertEqual(args[0], "resolved-bin")
        self.assertEqual(args[1], "--flag")
        self.assertEqual(args[2], "extraarg")

    def test_transport_alias_miss(self):
        """When args[0].lower() has no alias match, original args[0] is kept."""
        with patch.dict(
            "dragon.transport.oob.dfacts.TRANSPORT_AGENT_ALIASES",
            {},
            clear=True,
        ):
            self.oob._start_transport(8000)
        args = self._all_popen_args()
        self.assertEqual(args[0], "mytransport")

    def test_log_sdesc_present(self):
        """When DRAGON_LOGGER_SDESC is in os.environ, log args are appended."""
        self.oob.log_sdesc = "/path/to/log.sdesc"
        self.oob._start_transport(8000)
        args = self._all_popen_args()
        joined = " ".join(str(a) for a in args)
        self.assertIn("--log-sdesc=", joined)
        self.assertIn("--dragon-logging=true", joined)
        self.assertIn("--log-level=DEBUG", joined)

    def test_log_sdesc_absent(self):
        """When DRAGON_LOGGER_SDESC is absent, --no-dragon-logging is appended."""
        self.oob._start_transport(8000)
        args = self._all_popen_args()
        self.assertIn("--no-dragon-logging", args)

    def test_oob_ip_addr_is_forwarded(self):
        """An explicit OOB address is included in the transport-agent command."""
        self.oob._start_transport(8000, oob_ip_addr="127.0.0.1")
        args = self._all_popen_args()
        self.assertIn("--oob-ip-addr=127.0.0.1", args)

    def test_no_oob_ip_addr_omits_argument(self):
        """Without an explicit OOB address, the transport-agent command omits it."""
        self.oob._start_transport(8000)
        args = self._all_popen_args()
        self.assertNotIn("--oob-ip-addr=127.0.0.1", args)


def _neutralize_for_gc(oob):
    """Reset an OutOfBand instance to an inert state before it goes out of scope.

    Several tests intentionally put instances into states where __del__ has
    real work to do (terminating a tunnel proc, sending a halt message,
    releasing channels) and may even call __del__ explicitly. Once the test's
    mocks are torn down, a *second*, implicit __del__ call triggered later by
    the garbage collector would run against the real (unmocked) production
    code and print a noisy "Exception ignored in __del__" traceback. Calling
    this at the end of such tests keeps the test output clean without
    affecting the assertions already made.
    """
    oob.tunnel_proc = None
    oob.transport_proc = None
    oob.channels = []
    oob.ta_input = None
    oob.ta_output = None
    oob._closed = True


# ---------------------------------------------------------------------------
# TestConnect
# ---------------------------------------------------------------------------


class TestConnect(unittest.TestCase):
    """Tests for OutOfBand.connect."""

    def setUp(self):
        self.oob = OutOfBand(timeout=5)
        self.oob._tunnel_args = MagicMock(return_value=["ssh", "-N", "node.host"])
        self.mock_setup_gateways = MagicMock()
        self.oob.setup_gateways = self.mock_setup_gateways

        self.p_popen = patch("dragon.transport.oob.subprocess.Popen")
        self.p_socket = patch("dragon.transport.oob.socket.create_connection")
        self.p_sleep = patch("dragon.transport.oob.time.sleep")
        self.p_time = patch("dragon.transport.oob.time.time")

        self.mock_popen = self.p_popen.start()
        self.mock_socket = self.p_socket.start()
        self.mock_sleep = self.p_sleep.start()
        self.mock_time = self.p_time.start()

    def tearDown(self):
        _neutralize_for_gc(self.oob)
        self.p_popen.stop()
        self.p_socket.stop()
        self.p_sleep.stop()
        self.p_time.stop()

    def test_happy_path(self):
        """Socket connects immediately and the gateway setup receives the tunnel endpoint."""
        # time.time returns 0 then 1 — still within timeout=5, socket succeeds first try
        self.mock_time.side_effect = [0, 1]
        mock_sock = MagicMock()
        self.mock_socket.return_value = mock_sock

        self.oob.connect("fe.host", "node.host", 7575)

        self.mock_setup_gateways.assert_called_once_with(
            os.environ,
            "fe.host",
            "node.host",
            7575,
            remote_overlay_transport=None,
            remote_rt_uid=None,
            local_bootstrap_port=7575,
        )
        mock_sock.close.assert_called_once()

    def test_socket_retry_then_success(self):
        """create_connection fails once then succeeds before gateway setup."""
        # time values: start=0, first check=1, second check=2 — both within timeout=5
        self.mock_time.side_effect = [0, 1, 2]
        mock_sock = MagicMock()
        self.mock_socket.side_effect = [OSError("refused"), mock_sock]

        self.oob.connect("fe.host", "node.host", 7575)

        self.assertEqual(self.mock_socket.call_count, 2)
        self.mock_setup_gateways.assert_called_once()

    def test_socket_timeout_raises(self):
        """When the while-loop times out, RuntimeError('ssh tunnel failed to establish') is raised."""
        # time.time: start=0, check=100 — immediately exceeds timeout=5
        self.mock_time.side_effect = [0, 100]

        with self.assertRaises(RuntimeError) as ctx:
            self.oob.connect("fe.host", "node.host", 7575)
        self.assertIn("ssh tunnel failed to establish", str(ctx.exception))

    def test_tunnel_proc_stored(self):
        """self.tunnel_proc is set to the return value of subprocess.Popen."""
        self.mock_time.side_effect = [0, 1]
        self.mock_socket.return_value = MagicMock()
        mock_proc = MagicMock()
        self.mock_popen.return_value = mock_proc

        self.oob.connect("fe.host", "node.host", 7575)

        self.assertIs(self.oob.tunnel_proc, mock_proc)


# ---------------------------------------------------------------------------
# TestAccept
# ---------------------------------------------------------------------------


class TestAccept(unittest.TestCase):
    """Tests for OutOfBand.accept."""

    def test_happy_path(self):
        """accept starts the listener for a Python TCP user transport."""
        oob = OutOfBand()
        start_transport = MagicMock(side_effect=lambda port: setattr(oob, "transport_proc", MagicMock()))
        oob._start_transport = start_transport
        self.addCleanup(_neutralize_for_gc, oob)

        with patch.dict(os.environ, {_dfacts.env_name("TRANSPORT_AGENT"): "tcp"}, clear=False):
            self.assertTrue(oob.accept(port=7575))

        start_transport.assert_called_once_with(7575)

    def test_idempotent_when_transport_is_running(self):
        """A second accept() is a successful no-op when the agent is already running."""
        oob = OutOfBand()
        oob._start_transport = MagicMock()
        oob.transport_proc = MagicMock()
        self.addCleanup(_neutralize_for_gc, oob)

        with patch.dict(os.environ, {_dfacts.env_name("TRANSPORT_AGENT"): "tcp"}, clear=False):
            self.assertTrue(oob.accept(port=7575))
            self.assertTrue(oob.accept(port=7575))

        oob._start_transport.assert_not_called()


# ---------------------------------------------------------------------------
# TestDel
# ---------------------------------------------------------------------------


class TestDel(unittest.TestCase):
    """Tests for OutOfBand.__del__."""

    def test_tunnel_proc_waited_for(self):
        """__del__ waits for an existing tunnel process during close."""
        oob = OutOfBand()
        self.addCleanup(_neutralize_for_gc, oob)
        mock_proc = MagicMock()
        oob.tunnel_proc = mock_proc

        oob.__del__()

        mock_proc.wait.assert_called_once_with(timeout=5)

    def test_no_tunnel_proc(self):
        """__del__ does not raise when tunnel_proc is None."""
        oob = OutOfBand()
        self.addCleanup(_neutralize_for_gc, oob)
        oob.__del__()  # should not raise

    def test_ta_input_sends_halt(self):
        """__del__ puts a UserHaltOOB instance onto an active transport queue."""
        from dragon.infrastructure import messages as dmsg

        oob = OutOfBand()
        self.addCleanup(_neutralize_for_gc, oob)
        mock_ta_input = MagicMock()
        oob.ta_input = mock_ta_input

        with patch("dragon.transport.oob.dlutil.next_tag", return_value=1):
            oob.__del__()

        mock_ta_input.put.assert_called_once()
        put_arg = mock_ta_input.put.call_args[0][0]
        self.assertIsInstance(put_arg, dmsg.UserHaltOOB)

    def test_no_ta_input_no_halt(self):
        """__del__ completes without a transport queue."""
        oob = OutOfBand()
        self.addCleanup(_neutralize_for_gc, oob)

        oob.__del__()

    def test_channels_released_and_detached_zero(self):
        """__del__ completes without error when channels list is empty."""
        oob = OutOfBand()
        self.addCleanup(_neutralize_for_gc, oob)
        with patch("dragon.transport.oob.release_refcnt") as mock_release:
            oob.__del__()
        mock_release.assert_not_called()

    def test_channels_released_and_detached_multiple(self):
        """release_refcnt and ch.detach() are called for each channel in order."""
        oob = OutOfBand()
        self.addCleanup(_neutralize_for_gc, oob)

        ch1 = MagicMock()
        ch2 = MagicMock()
        ch1.cuid = 101
        ch2.cuid = 102
        oob.channels = [ch1, ch2]

        with patch("dragon.transport.oob.release_refcnt") as mock_release:
            oob.__del__()

        mock_release.assert_any_call(101)
        mock_release.assert_any_call(102)
        self.assertEqual(mock_release.call_count, 2)
        ch1.detach.assert_called_once()
        ch2.detach.assert_called_once()


if __name__ == "__main__":
    unittest.main()
