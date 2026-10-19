"""Unit tests for dragon.workflows.runtime module-level functions and the Proxy class.

These are pure standard-Python-mode unit tests (no live Dragon runtime, no real SSH,
no multi-node allocation). All subprocess/socket/filesystem/network interactions are
mocked via unittest.mock.

NOTE: test/proxy/ is a *separate*, untouched functional multi-node suite — do not
confuse it with these unit tests.  The two suites are fully independent.
"""

import atexit
import json
import os
import socket
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, call, patch

import dragon.workflows.runtime as runtime

# ---------------------------------------------------------------------------
# Helpers: compute the env-var key names the same way the source does so that
# we don't duplicate magic strings in the tests.
# ---------------------------------------------------------------------------
import dragon.infrastructure.facts as _dfacts

_GS_QD_KEY = _dfacts.env_name("GS_QD")  # "DRAGON_GS_QD"
_GS_RET_QD_KEY = _dfacts.env_name("GS_RET_QD")  # "DRAGON_GS_RET_QD"
_LS_QD_KEY = _dfacts.env_name("LOCAL_LS_QD")  # "DRAGON_LOCAL_LS_QD"
_LS_RET_QD_KEY = _dfacts.env_name("LS_RET_QD")  # "DRAGON_LS_RET_QD"
_DEFAULT_PD_KEY = _dfacts.env_name("DEFAULT_PD")  # "DRAGON_DEFAULT_PD"
_INF_PD_KEY = _dfacts.env_name("INF_PD")  # "DRAGON_INF_PD"
_RT_UID_KEY = "DRAGON_RT_UID"

# A fake snapshot containing every descriptor returned by get_current_inf_env.
_FAKE_INF_ENV = {
    _GS_QD_KEY: "gs_qd_val",
    _GS_RET_QD_KEY: "gs_ret_qd_val",
    _LS_QD_KEY: "ls_qd_val",
    _LS_RET_QD_KEY: "ls_ret_qd_val",
    _DEFAULT_PD_KEY: "default_pd_val",
    _INF_PD_KEY: "inf_pd_val",
    _RT_UID_KEY: "42",
}


# ---------------------------------------------------------------------------
# Shared base: snapshot/restore mutable module-level globals so that every
# test class starts from a clean slate without relying on test execution order.
# ---------------------------------------------------------------------------


class RuntimeGlobalsTestCase(unittest.TestCase):
    """Base TestCase that snapshots and restores dragon.workflows.runtime globals.

    All runtime.py TestCase classes should subclass this.  The base class
    itself has no test_ methods.
    """

    def setUp(self):
        # Snapshot the mutable module-level globals by *value*, not reference.
        self._saved_already_published = dict(runtime.already_published)
        self._saved_runtime_table = dict(runtime.runtime_table)
        self._saved_oob_net_list = list(runtime.oob_net_list)
        self._saved_runtime_stack = list(runtime._get_runtime_stack())
        self._saved_current_rt_uid = runtime._current_rt_uid

    def tearDown(self):
        # Restore in-place (do NOT rebind with =) to avoid aliasing bugs.
        runtime.already_published.clear()
        runtime.already_published.update(self._saved_already_published)

        runtime.runtime_table.clear()
        runtime.runtime_table.update(self._saved_runtime_table)

        runtime.oob_net_list.clear()
        runtime.oob_net_list.extend(self._saved_oob_net_list)

        runtime._get_runtime_stack().clear()
        runtime._get_runtime_stack().extend(self._saved_runtime_stack)

        runtime._current_rt_uid = self._saved_current_rt_uid


# ---------------------------------------------------------------------------
# TestFreePort
# ---------------------------------------------------------------------------


class TestFreePort(RuntimeGlobalsTestCase):
    def test_free_port_returns_true_on_refused(self):
        """ConnectionRefusedError → port is free → returns True."""
        with patch("dragon.workflows.runtime.socket.socket") as mock_sock_cls:
            mock_sock = MagicMock()
            mock_sock_cls.return_value = mock_sock
            mock_sock.connect.side_effect = ConnectionRefusedError
            result = runtime.free_port("localhost", 9999)
        self.assertTrue(result)
        mock_sock.close.assert_called_once()

    def test_free_port_returns_false_on_success(self):
        """Successful connect → port is in use → returns False."""
        with patch("dragon.workflows.runtime.socket.socket") as mock_sock_cls:
            mock_sock = MagicMock()
            mock_sock_cls.return_value = mock_sock
            # connect succeeds (no exception)
            result = runtime.free_port("localhost", 9999)
        self.assertFalse(result)
        mock_sock.close.assert_called_once()


# ---------------------------------------------------------------------------
# TestGetPort
# ---------------------------------------------------------------------------


class TestGetPort(RuntimeGlobalsTestCase):
    def test_returns_first_free_port(self):
        """Returns the first port for which free_port() is True."""
        # ports 7000 and 7001 are occupied; 7002 is free
        free_answers = {7000: False, 7001: False, 7002: True}
        with patch("dragon.workflows.runtime.free_port", side_effect=lambda h, p: free_answers.get(p, True)):
            result = runtime.get_port("localhost", 7000, 7010)
        self.assertEqual(result, 7002)

    def test_raises_when_no_port_available(self):
        """Raises RuntimeError when all ports in range are occupied."""
        with patch("dragon.workflows.runtime.free_port", return_value=False):
            with self.assertRaises(RuntimeError):
                runtime.get_port("localhost", 8000, 8005)


# ---------------------------------------------------------------------------
# TestGetIp
# ---------------------------------------------------------------------------


class TestGetIp(RuntimeGlobalsTestCase):
    """Tests for runtime.get_ip().

    NOTE: an earlier version of get_ip() had a latent bug (an undefined
    ``host`` name referenced in a LOGGER.debug() call, causing a NameError on
    every invocation). That bug has since been fixed upstream (the offending
    debug line was removed), so these tests now cover the intended
    success/fallback behavior instead of the old regression.
    """

    def test_returns_ip_from_udp_trick(self):
        """get_ip() returns the local IP address reported by the UDP-connect trick."""
        mock_sock = MagicMock()
        mock_sock.getsockname.return_value = ("192.168.1.5", 0)
        with patch("dragon.workflows.runtime.socket.socket", return_value=mock_sock):
            result = runtime.get_ip()
        self.assertEqual(result, "192.168.1.5")
        mock_sock.close.assert_called_once()

    def test_fallback_to_localhost_on_exception(self):
        """get_ip() falls back to 127.0.0.1 if the UDP-connect trick raises."""
        mock_sock = MagicMock()
        mock_sock.connect.side_effect = OSError("network unreachable")
        with patch("dragon.workflows.runtime.socket.socket", return_value=mock_sock):
            result = runtime.get_ip()
        self.assertEqual(result, "127.0.0.1")
        mock_sock.close.assert_called_once()


# ---------------------------------------------------------------------------
# TestInfEnvRoundTrip
# ---------------------------------------------------------------------------


class TestInfEnvRoundTrip(RuntimeGlobalsTestCase):
    def test_get_current_inf_env_reads_expected_vars(self):
        """get_current_inf_env returns all infrastructure descriptor keys."""
        with patch.dict(os.environ, _FAKE_INF_ENV, clear=False):
            env = runtime.get_current_inf_env()
        self.assertEqual(env, _FAKE_INF_ENV)

    def test_set_inf_env_writes_expected_vars_and_calls_connect_to_infrastructure(self):
        """set_inf_env pushes env vars into os.environ and calls connect_to_infrastructure(force=True)."""
        with patch("dragon.globalservices.api_setup.connect_to_infrastructure") as mock_connect:
            with patch.dict(os.environ, {}, clear=True):
                runtime.set_inf_env(_FAKE_INF_ENV)
                mock_connect.assert_called_once_with(force=True)
                # Only the control-plane descriptors and runtime ID are changed;
                # node-local descriptors remain owned by the local runtime.
                for key in (_GS_QD_KEY, _GS_RET_QD_KEY, _RT_UID_KEY):
                    val = _FAKE_INF_ENV[key]
                    self.assertEqual(os.environ.get(key), val)
                for key in (_LS_QD_KEY, _LS_RET_QD_KEY, _DEFAULT_PD_KEY, _INF_PD_KEY):
                    self.assertNotIn(key, os.environ)


# ---------------------------------------------------------------------------
# TestGetSdesc
# ---------------------------------------------------------------------------


class TestGetSdesc(RuntimeGlobalsTestCase):
    """Tests for runtime._get_sdesc()."""

    _ENV_VARS = {
        _GS_QD_KEY: "gs_qd",
        _GS_RET_QD_KEY: "gs_ret_qd",
        _LS_QD_KEY: "ls_qd",
        _LS_RET_QD_KEY: "ls_ret_qd",
        "DRAGON_FE_EXTERNAL_IP_ADDR": "1.2.3.4",
        "DRAGON_HEAD_NODE_IP_ADDR": "5.6.7.8",
    }

    def test_builds_runtime_desc_from_env(self):
        """_get_sdesc() returns Base64 text when all required vars are present."""
        with patch.dict(os.environ, self._ENV_VARS, clear=False):
            with patch("dragon.workflows.runtime.dmsg.RuntimeDesc") as mock_cls:
                mock_sdesc = MagicMock()
                mock_sdesc.serialize.return_value = b"serialized"
                mock_cls.return_value = mock_sdesc
                with patch("dragon.workflows.runtime.du.b64encode", return_value="encoded") as mock_encode:
                    result = runtime._get_sdesc()
        self.assertEqual(result, "encoded")
        mock_cls.assert_called_once()
        mock_sdesc.serialize.assert_called_once_with()
        mock_encode.assert_called_once_with(b"serialized")

    def test_raises_on_missing_env_var(self):
        """_get_sdesc() raises KeyError when a required env var is absent."""
        # Ensure the env does NOT contain the GS_QD key
        env_without_key = {k: v for k, v in self._ENV_VARS.items() if k != _GS_QD_KEY}
        # Remove from real env too in case it happens to be set
        with patch.dict(os.environ, env_without_key, clear=False):
            saved = os.environ.pop(_GS_QD_KEY, None)
            try:
                with self.assertRaises(KeyError):
                    runtime._get_sdesc()
            finally:
                if saved is not None:
                    os.environ[_GS_QD_KEY] = saved


# ---------------------------------------------------------------------------
# TestPublish
# ---------------------------------------------------------------------------


class TestPublish(RuntimeGlobalsTestCase):
    """Tests for runtime.publish()."""

    def setUp(self):
        super().setUp()
        runtime.already_published.clear()

    def _make_sdesc(self):
        return "FAKE_SDESC"

    def test_writes_sdesc_to_file(self):
        """publish() writes the sdesc to the expected file path."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch("dragon.workflows.runtime._get_sdesc", return_value=self._make_sdesc()):
                with patch("dragon.workflows.runtime.doob.OutOfBand") as mock_oob_cls:
                    mock_oob_cls.return_value = MagicMock()
                    runtime.publish("myruntime", publish_dir=tmpdir)

            expected_path = os.path.join(tmpdir, "myruntime")
            self.assertTrue(os.path.exists(expected_path))
            with open(expected_path, "r") as f:
                self.assertEqual(f.read(), self._make_sdesc())

    def test_republish_returns_cached_descriptor(self):
        """Publishing the same path twice reuses the cached descriptor and agent."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch("dragon.workflows.runtime._get_sdesc", return_value=self._make_sdesc()):
                with patch("dragon.workflows.runtime.doob.OutOfBand") as mock_oob_cls:
                    mock_instance = MagicMock()
                    mock_oob_cls.return_value = mock_instance
                    first = runtime.publish("myruntime", publish_dir=tmpdir)
                    second = runtime.publish("myruntime", publish_dir=tmpdir)

            self.assertEqual(first, self._make_sdesc())
            self.assertEqual(second, first)
            self.assertEqual(mock_oob_cls.call_count, 1)
            mock_instance.accept.assert_called_once()

    def test_publish_does_not_register_atexit_handler(self):
        """publish() does not register an atexit teardown handler in current runtime API."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch("dragon.workflows.runtime._get_sdesc", return_value=self._make_sdesc()):
                with patch("dragon.workflows.runtime.doob.OutOfBand"):
                    with patch.object(atexit, "register") as mock_register:
                        runtime.publish("rt1", publish_dir=tmpdir)
                        runtime.publish("rt2", publish_dir=tmpdir)

            mock_register.assert_not_called()


# ---------------------------------------------------------------------------
# TestLookup
# ---------------------------------------------------------------------------


class TestLookup(RuntimeGlobalsTestCase):
    """Tests for runtime.lookup()."""

    def setUp(self):
        super().setUp()
        # Change to a temp directory so any files written by scp-mock land there.
        self._orig_cwd = os.getcwd()
        self._tmpdir = tempfile.mkdtemp()
        os.chdir(self._tmpdir)

    def tearDown(self):
        os.chdir(self._orig_cwd)
        import shutil

        shutil.rmtree(self._tmpdir, ignore_errors=True)
        super().tearDown()

    def test_scp_retries_until_success(self):
        """lookup() retries scp and returns descriptor text once scp succeeds."""
        cwd = self._tmpdir

        def scp_side_effect(cmd):
            if "scp" in cmd:
                # Write a fake file named "myrt" in cwd to simulate scp success
                try:
                    with open(os.path.join(cwd, "myrt"), "w") as f:
                        f.write("REMOTE_SDESC")
                    return 0
                except Exception:
                    return 1
            return 0  # ssh ls

        with patch("dragon.workflows.runtime.os.system", side_effect=scp_side_effect):
            with patch("dragon.workflows.runtime.time.sleep"):  # don't actually sleep
                result = runtime.lookup("mysys", "myrt", timeout_in=5, publish_dir="/remote/dir")

        self.assertEqual(result, "REMOTE_SDESC")

    def test_timeout_raises_runtime_error(self):
        """lookup() raises RuntimeError when scp never succeeds within timeout."""
        with patch("dragon.workflows.runtime.os.system", return_value=1):
            with patch("dragon.workflows.runtime.time.sleep"):
                with self.assertRaises(RuntimeError):
                    runtime.lookup("mysys", "myrt", timeout_in=2, publish_dir="/remote/dir")


# ---------------------------------------------------------------------------
# TestAttach
# ---------------------------------------------------------------------------


class TestAttach(RuntimeGlobalsTestCase):
    def setUp(self):
        super().setUp()
        runtime.runtime_table.clear()
        self.started_local_runtime = MagicMock(spec=runtime.LocalRuntime)
        self.attached_local_runtime = MagicMock(spec=runtime.LocalRuntime)
        self.mock_in_dragon = self.enterContext(patch("dragon.workflows.runtime.dragon.in_dragon", return_value=False))
        self.mock_start_local_runtime = self.enterContext(
            patch("dragon.workflows.runtime.start_local_runtime", return_value=self.started_local_runtime)
        )
        self.mock_local_attach = self.enterContext(
            patch("dragon.workflows.runtime._local_attach", return_value=self.attached_local_runtime)
        )

    def _build_sdesc_mock(self):
        """Return a mock sdesc object with all required attributes."""
        m = MagicMock()
        m.fe_ext_ip_addr = "1.2.3.4"
        m.head_node_ip_addr = "5.6.7.8"
        m.oob_port = 9575
        m.python_path = "/usr/bin/python3"
        m.env = json.dumps({})
        m.dragon_version = runtime.dragon.__version__
        return m

    def test_creates_proxy_and_registers(self):
        """attach() returns a Proxy and registers it in runtime_table."""
        fake_sdesc = self._build_sdesc_mock()
        with patch("dragon.workflows.runtime.du.b64decode", return_value=b"raw"):
            with patch("dragon.workflows.runtime.dmsg.parse", return_value=fake_sdesc):
                with patch("dragon.workflows.runtime.doob.OutOfBand") as mock_oob_cls:
                    mock_oob = MagicMock()
                    mock_oob_cls.return_value = mock_oob
                    with patch("dragon.workflows.runtime.dutil.rt_uid_from_ip_addrs", return_value=99):
                        proxy = runtime.attach(sdesc_str="FAKE_SDESC_STR")

        self.assertIsInstance(proxy, runtime.Proxy)
        self.assertIn(99, runtime.runtime_table)
        self.assertIs(proxy._local_runtime, self.attached_local_runtime)
        self.mock_in_dragon.assert_called_once_with()
        self.mock_start_local_runtime.assert_called_once_with()
        self.mock_local_attach.assert_called_once_with(timeout=30, runtime=self.started_local_runtime)

    def test_passes_ssh_tunnel_override_to_oob(self):
        """attach() passes oob_ssh_tunnel_override to OutOfBand constructor."""
        fake_sdesc = self._build_sdesc_mock()
        override = "mytunnel {fe_ext_ip_addr} {head_node_ip_addr} {port}"
        with patch("dragon.workflows.runtime.du.b64decode", return_value=b"raw"):
            with patch("dragon.workflows.runtime.dmsg.parse", return_value=fake_sdesc):
                with patch("dragon.workflows.runtime.doob.OutOfBand") as mock_oob_cls:
                    mock_oob_cls.return_value = MagicMock()
                    with patch("dragon.workflows.runtime.dutil.rt_uid_from_ip_addrs", return_value=1):
                        runtime.attach(sdesc_str="FAKE_SDESC_STR", oob_ssh_tunnel_override=override)

        mock_oob_cls.assert_called_once_with(oob_ssh_tunnel_override=override)


# ---------------------------------------------------------------------------
# TestRuntimeTestTunnel
# ---------------------------------------------------------------------------


class TestRuntimeTestTunnel(RuntimeGlobalsTestCase):
    def test_delegates_to_oob_test_tunnel(self):
        """runtime.test_tunnel() delegates to doob.OutOfBand(...).test_tunnel() with correct args."""
        fake_sdesc = MagicMock()
        fake_sdesc.fe_ext_ip_addr = "1.2.3.4"
        fake_sdesc.head_node_ip_addr = "5.6.7.8"
        fake_sdesc.oob_port = 9575

        expected_result = {"ok": True, "reason": "ok"}

        with patch("dragon.workflows.runtime.du.b64decode", return_value=b"raw"):
            with patch("dragon.workflows.runtime.dmsg.parse", return_value=fake_sdesc):
                with patch("dragon.workflows.runtime.doob.OutOfBand") as mock_oob_cls:
                    mock_oob = MagicMock()
                    mock_oob.test_tunnel.return_value = expected_result
                    mock_oob_cls.return_value = mock_oob
                    result = runtime.test_tunnel("FAKE_SDESC_STR", connect_timeout=20)

        mock_oob.test_tunnel.assert_called_once_with("1.2.3.4", "5.6.7.8", 9575, connect_timeout=20)
        self.assertEqual(result, expected_result)


# ---------------------------------------------------------------------------
# TestProxy
# ---------------------------------------------------------------------------


def _make_proxy(env=None, cwd=None, python_exe=None):
    """Helper: build a Proxy without live Dragon infrastructure."""
    if env is None:
        env = dict(_FAKE_INF_ENV)
    sdesc = MagicMock()
    sdesc.env = json.dumps(env)
    oob_net = MagicMock()
    return runtime.Proxy(sdesc, oob_net, cwd=cwd, remote_python_executable=python_exe)


class TestProxy(RuntimeGlobalsTestCase):
    def setUp(self):
        super().setUp()
        self.enterContext(patch("dragon.workflows.runtime.dch.register_gateways_from_env"))
        # Remove proxy env vars that might have been left by previous tests.
        for key in (
            "DRAGON_PROXY_ENABLED",
            "DRAGON_PROXY_REMOTE_ENV",
            "DRAGON_PROXY_REMOTE_CWD",
            "DRAGON_PROXY_PYTHON_EXECUTABLE",
            "DRAGON_PROXY_LOCAL_INF_ENV",
        ):
            os.environ.pop(key, None)

    def tearDown(self):
        for key in (
            "DRAGON_PROXY_ENABLED",
            "DRAGON_PROXY_REMOTE_ENV",
            "DRAGON_PROXY_REMOTE_CWD",
            "DRAGON_PROXY_PYTHON_EXECUTABLE",
            "DRAGON_PROXY_LOCAL_INF_ENV",
        ):
            os.environ.pop(key, None)
        super().tearDown()

    def test_enable_sets_env_vars(self):
        """enable() sets DRAGON_PROXY_ENABLED=1 and related env vars."""
        proxy = _make_proxy(cwd="/remote/cwd", python_exe="/usr/bin/python3")
        with patch("dragon.workflows.runtime.get_current_inf_env", return_value=dict(_FAKE_INF_ENV)):
            with patch("dragon.workflows.runtime.set_inf_env"):
                proxy.enable()

        self.assertEqual(os.environ.get("DRAGON_PROXY_ENABLED"), "1")
        self.assertIn("DRAGON_PROXY_REMOTE_ENV", os.environ)
        self.assertEqual(os.environ.get("DRAGON_PROXY_REMOTE_CWD"), "/remote/cwd")
        self.assertEqual(os.environ.get("DRAGON_PROXY_PYTHON_EXECUTABLE"), "/usr/bin/python3")

    def test_disable_restores_env_vars(self):
        """disable() clears proxy env vars and restores the displaced infra env."""
        proxy = _make_proxy()

        # Simulate entering proxy mode from a known local infra environment.
        with patch("dragon.workflows.runtime.get_current_inf_env", return_value=dict(_FAKE_INF_ENV)):
            with patch("dragon.workflows.runtime.set_inf_env") as mock_set:
                proxy.enable()
                proxy.disable()

        # First call is from enable() activation; second is disable() restore.
        self.assertEqual(mock_set.call_count, 2)
        self.assertEqual(mock_set.call_args_list[-1], call(dict(_FAKE_INF_ENV)))
        for key in (
            "DRAGON_PROXY_ENABLED",
            "DRAGON_PROXY_REMOTE_ENV",
            "DRAGON_PROXY_REMOTE_CWD",
            "DRAGON_PROXY_PYTHON_EXECUTABLE",
        ):
            self.assertNotIn(key, os.environ)

    def test_context_manager_calls_enable_disable(self):
        """__enter__ calls enable(); __exit__ calls disable()."""
        proxy = _make_proxy()
        proxy.enable = MagicMock()
        proxy.disable = MagicMock()

        with proxy:
            proxy.enable.assert_called_once()
            proxy.disable.assert_not_called()

        proxy.disable.assert_called_once()

    def test_set_remote_cwd(self):
        """set_remote_cwd sets _cwd and DRAGON_PROXY_REMOTE_CWD."""
        proxy = _make_proxy()
        proxy.set_remote_cwd("/new/cwd")
        self.assertEqual(proxy._cwd, "/new/cwd")
        self.assertEqual(os.environ.get("DRAGON_PROXY_REMOTE_CWD"), "/new/cwd")

    def test_set_remote_env(self):
        """set_remote_env sets _env and DRAGON_PROXY_REMOTE_ENV as JSON."""
        proxy = _make_proxy()
        new_env = {"FOO": "bar"}
        proxy.set_remote_env(new_env)
        self.assertEqual(proxy._env, new_env)
        self.assertEqual(json.loads(os.environ["DRAGON_PROXY_REMOTE_ENV"]), new_env)

    def test_set_remote_python_executable(self):
        """set_remote_python_executable sets the attribute and env var."""
        proxy = _make_proxy()
        proxy.set_remote_python_executable("/opt/python/bin/python3")
        self.assertEqual(proxy._remote_python_executable, "/opt/python/bin/python3")
        self.assertEqual(os.environ.get("DRAGON_PROXY_PYTHON_EXECUTABLE"), "/opt/python/bin/python3")

    def test_unset_remote_user_env_tolerates_missing(self):
        """_unset_remote_user_env does not raise even if none of the four vars are set."""
        proxy = _make_proxy()
        # Ensure none of the vars are set
        for key in (
            "DRAGON_PROXY_ENABLED",
            "DRAGON_PROXY_REMOTE_ENV",
            "DRAGON_PROXY_REMOTE_CWD",
            "DRAGON_PROXY_PYTHON_EXECUTABLE",
        ):
            os.environ.pop(key, None)

        # Should not raise
        proxy._unset_remote_user_env()
        # Calling again is also safe
        proxy._unset_remote_user_env()

    def test_disable_without_prior_enable(self):
        """disable() without a matching enable() raises RuntimeError."""
        proxy = _make_proxy()
        with self.assertRaises(RuntimeError):
            proxy.disable()


# ---------------------------------------------------------------------------
# TestRemovedLegacyAPI
# ---------------------------------------------------------------------------


class TestRemovedLegacyAPI(RuntimeGlobalsTestCase):
    def test_proxy_teardown_removed(self):
        """Legacy proxy_teardown helper is no longer part of runtime API."""
        self.assertFalse(hasattr(runtime, "proxy_teardown"))


if __name__ == "__main__":
    unittest.main()
