"""Driver tests for dragon_ext_attach()/dragon_ext_detach() behavior.

Starts a single-node, --attachable Dragon runtime via
dragon.workflows.runtime.start_local_runtime(), then runs an external client
as a subprocess for each of five attach/detach modes, asserting exit codes.

Two client implementations are exercised:
- C client: test/ext_attach/test_ext_attach_client.c
- Python client: test/ext_attach/test_ext_attach_client.py

This is a standard-Python-mode test: the test process itself is not part of
a Dragon runtime; it only starts one as a subprocess and exercises the
external-attach path against it.
"""

import os
import pathlib
import shutil
import subprocess
import sys
import tempfile
import unittest
import unittest.mock

import dragon.workflows.runtime as runtime
import dragon.globalservices.api_setup as api_setup
import dragon.infrastructure.facts as dfacts
import dragon.infrastructure.parameters as dparms
from dragon.infrastructure.facts import DRAGON_LIB_DIR

test_dir = pathlib.Path(__file__).resolve().parent
os.system(f"cd {test_dir}; make --silent")

THIS_DIR = str(test_dir)
TEST_DIR = os.path.dirname(THIS_DIR)
C_CLIENT_BIN = os.path.join(THIS_DIR, "test_ext_attach_client")
PY_CLIENT_SCRIPT = os.path.join(THIS_DIR, "test_ext_attach_client.py")

_INITIAL_DRAGON_ENV = {key: value for key, value in os.environ.items() if key.startswith("DRAGON_")}
_INITIAL_THIS_PROCESS_ENV = dparms.this_process.env()
_INITIAL_CURRENT_RT_UID = runtime._current_rt_uid


def _find_stale_process_state():
    issues = []

    current_dragon_env = {key: value for key, value in os.environ.items() if key.startswith("DRAGON_")}
    added_env = sorted(current_dragon_env.keys() - _INITIAL_DRAGON_ENV.keys())
    removed_env = sorted(_INITIAL_DRAGON_ENV.keys() - current_dragon_env.keys())
    changed_env = sorted(
        key
        for key in current_dragon_env.keys() & _INITIAL_DRAGON_ENV.keys()
        if current_dragon_env[key] != _INITIAL_DRAGON_ENV[key]
    )
    if added_env:
        issues.append(f"added DRAGON_* environment variables: {added_env}")
    if removed_env:
        issues.append(f"removed DRAGON_* environment variables: {removed_env}")
    if changed_env:
        issues.append(f"changed DRAGON_* environment variables: {changed_env}")

    expected_none = (
        "_GS_INPUT",
        "_GS_RETURN",
        "_LS_INPUT",
        "_LS_RETURN",
        "_GS_RETURN_CUID",
        "_LS_RETURN_CUID",
        "_EXTERNAL_DEFAULT_POOL",
    )
    stale_api_globals = [name for name in expected_none if getattr(api_setup, name) is not None]
    if stale_api_globals:
        issues.append(f"api_setup globals not cleared: {stale_api_globals}")
    if api_setup._INFRASTRUCTURE_CONNECTED:
        issues.append("api_setup._INFRASTRUCTURE_CONNECTED is still true")
    if api_setup._WAKEUPS:
        issues.append(f"api_setup._WAKEUPS still has {len(api_setup._WAKEUPS)} entries")
    if api_setup._RESULTS:
        issues.append(f"api_setup._RESULTS still has {len(api_setup._RESULTS)} entries")

    if runtime._get_runtime_stack():
        issues.append(f"runtime activation stack still has {len(runtime._get_runtime_stack())} entries")
    if runtime._current_rt_uid != _INITIAL_CURRENT_RT_UID:
        issues.append("runtime._current_rt_uid differs from its initial value")
    if dparms.this_process.env() != _INITIAL_THIS_PROCESS_ENV:
        issues.append("dragon.infrastructure.parameters.this_process differs from its initial value")

    return issues


class TestExtAttach:
    @classmethod
    def setUpClass(cls):
        # launch runtime but don't make this process part of the runtime
        cls.rt = runtime.start_local_runtime()

    @classmethod
    def tearDownClass(cls):
        # Prefer graceful shutdown via attach+stop. If the runtime has already
        # exited (or attach bootstrap fails), fall back to direct stop cleanup.
        if cls.rt._proc is not None and cls.rt._proc.poll() is not None:
            cls.rt.stop(timeout=0)
            return

        try:
            attached_runtime = runtime.attach(runtime=cls.rt)
        except Exception:
            cls.rt.stop(timeout=0)
            return

        attached_runtime.stop()

    def _run_client(self, mode, env=None):
        raise NotImplementedError

    def _assert_client_success(self, proc):
        # tests return 1 if behavior is unexpected otherwise returns 0
        self.assertEqual(proc.returncode, 0, msg=f"stdout={proc.stdout!r} stderr={proc.stderr!r}")

    def test_attach_detach(self):
        proc = self._run_client("attach_detach")
        self._assert_client_success(proc)
        self.assertIn("p_uid=", proc.stdout)

    def test_bad_token(self):
        proc = self._run_client("bad_token")
        self._assert_client_success(proc)

    def test_double_detach(self):
        proc = self._run_client("double_detach")
        self._assert_client_success(proc)

    def test_reattach(self):
        proc = self._run_client("reattach")
        self._assert_client_success(proc)

    def test_stale_runtime(self):
        empty_dir = tempfile.mkdtemp(prefix="dragon_ext_attach_stale_")
        try:
            proc = self._run_client("stale_runtime", env={"DRAGON_ATTACH_DIR": empty_dir})
            self._assert_client_success(proc)
        finally:
            shutil.rmtree(empty_dir, ignore_errors=True)


@unittest.skipUnless(
    os.path.exists(C_CLIENT_BIN),
    f"C test client not built at {C_CLIENT_BIN}; run 'make' in test/ext_attach first",
)
class TestExtAttachCClient(TestExtAttach, unittest.TestCase):
    def _run_client(self, mode, env=None):
        run_env = os.environ.copy()
        run_env["LD_LIBRARY_PATH"] = str(DRAGON_LIB_DIR) + ":" + run_env.get("LD_LIBRARY_PATH", "")
        run_env["DYLD_FALLBACK_LIBRARY_PATH"] = str(DRAGON_LIB_DIR) + ":" + run_env.get(
            "DYLD_FALLBACK_LIBRARY_PATH", ""
        )
        run_env["DRAGON_ATTACH_TOKEN"] = self.rt.auth_token
        if env:
            run_env.update(env)
        proc = subprocess.run([C_CLIENT_BIN, mode], env=run_env, capture_output=True, text=True, timeout=60, cwd=TEST_DIR)
        return proc


class TestExtAttachPythonClient(TestExtAttach, unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        stale_state = _find_stale_process_state()
        if stale_state:
            raise AssertionError("Stale Dragon process state before Python client tests:\n- " + "\n- ".join(stale_state))
        super().setUpClass()

    def _run_client(self, mode, env=None):
        run_env = os.environ.copy()
        run_env["DRAGON_ATTACH_TOKEN"] = self.rt.auth_token
        if env:
            run_env.update(env)
        proc = subprocess.run(
            [sys.executable, PY_CLIENT_SCRIPT, mode],
            env=run_env,
            capture_output=True,
            text=True,
            timeout=60,
            cwd=TEST_DIR,
        )
        return proc


_TEMPDIR_ENV_VARS = ("TMPDIR", "TEMP", "TMP")


class TestAttachRuntimeDir(unittest.TestCase):
    def test_env_override(self):
        with unittest.mock.patch.dict(os.environ, {dfacts.ATTACH_DIR_ENV: "/some/attach/dir"}):
            self.assertEqual(dfacts.attach_runtime_dir(), "/some/attach/dir")

    def test_default_under_gettempdir(self):
        with unittest.mock.patch.dict(os.environ):
            os.environ.pop(dfacts.ATTACH_DIR_ENV, None)
            expected = os.path.join(tempfile.gettempdir(), f"dragon_{os.getuid()}")
            self.assertEqual(dfacts.attach_runtime_dir(), expected)
            self.assertEqual(dfacts.default_attach_runtime_dir(), expected)


@unittest.skipUnless(
    os.path.exists(C_CLIENT_BIN),
    f"C test client not built at {C_CLIENT_BIN}; run 'make' in test/ext_attach first",
)
class TestGetTempDirParity(unittest.TestCase):
    """dragon_gettempdir() must pick the same directory as Python's tempfile.gettempdir()."""

    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="dragon_gettempdir_")
        self.addCleanup(shutil.rmtree, self.root, ignore_errors=True)

    def _mkdir(self, name):
        path = os.path.join(self.root, name)
        os.makedirs(path, exist_ok=True)
        return path

    def _run(self, cmd, env, cwd):
        run_env = {key: value for key, value in os.environ.items() if key not in _TEMPDIR_ENV_VARS}
        run_env["LD_LIBRARY_PATH"] = str(DRAGON_LIB_DIR) + ":" + run_env.get("LD_LIBRARY_PATH", "")
        run_env["DYLD_FALLBACK_LIBRARY_PATH"] = str(DRAGON_LIB_DIR) + ":" + run_env.get(
            "DYLD_FALLBACK_LIBRARY_PATH", ""
        )
        run_env.update(env)
        proc = subprocess.run(cmd, env=run_env, cwd=cwd, capture_output=True, text=True, timeout=60)
        self.assertEqual(proc.returncode, 0, msg=f"{cmd}: stdout={proc.stdout!r} stderr={proc.stderr!r}")
        return proc.stdout.strip()

    def assert_parity(self, env, cwd=None, expected=None):
        cwd = cwd or self.root
        py_dir = self._run(
            [sys.executable, "-c", "from tempfile import gettempdir; print(gettempdir())"], env, cwd
        )
        c_dir = self._run([C_CLIENT_BIN, "gettempdir"], env, cwd)
        self.assertEqual(c_dir, py_dir, msg=f"env={env}")
        if expected is not None:
            self.assertEqual(py_dir, expected, msg=f"env={env}")

    def test_no_env_vars(self):
        self.assert_parity({})

    def test_tmpdir(self):
        self.assert_parity({"TMPDIR": self._mkdir("a")}, expected=os.path.join(self.root, "a"))

    def test_temp(self):
        self.assert_parity({"TEMP": self._mkdir("b")}, expected=os.path.join(self.root, "b"))

    def test_tmp(self):
        self.assert_parity({"TMP": self._mkdir("c")}, expected=os.path.join(self.root, "c"))

    def test_precedence(self):
        env = {"TMPDIR": self._mkdir("a"), "TEMP": self._mkdir("b"), "TMP": self._mkdir("c")}
        self.assert_parity(env, expected=env["TMPDIR"])

    def test_temp_before_tmp(self):
        env = {"TEMP": self._mkdir("b"), "TMP": self._mkdir("c")}
        self.assert_parity(env, expected=env["TEMP"])

    def test_empty_env_var_skipped(self):
        env = {"TMPDIR": "", "TEMP": self._mkdir("b")}
        self.assert_parity(env, expected=env["TEMP"])

    def test_missing_dir_falls_through(self):
        env = {"TMPDIR": os.path.join(self.root, "missing"), "TMP": self._mkdir("c")}
        self.assert_parity(env, expected=env["TMP"])

    def test_file_instead_of_dir_falls_through(self):
        not_a_dir = os.path.join(self.root, "file")
        with open(not_a_dir, "w") as fh:
            fh.write("x")
        env = {"TMPDIR": not_a_dir, "TEMP": self._mkdir("b")}
        self.assert_parity(env, expected=env["TEMP"])

    @unittest.skipIf(os.geteuid() == 0, "root can write to read-only directories")
    def test_unwritable_dir_falls_through(self):
        read_only = self._mkdir("ro")
        os.chmod(read_only, 0o500)
        self.addCleanup(os.chmod, read_only, 0o700)
        env = {"TMPDIR": read_only, "TEMP": self._mkdir("b")}
        self.assert_parity(env, expected=env["TEMP"])

    def test_all_env_vars_invalid(self):
        missing = os.path.join(self.root, "missing")
        self.assert_parity({"TMPDIR": missing, "TEMP": missing, "TMP": missing})

    def test_path_is_normalized(self):
        target = self._mkdir(os.path.join("x", "y"))
        messy = self.root + "//x/./z/../y/"
        os.makedirs(os.path.join(self.root, "x", "z"), exist_ok=True)
        self.assert_parity({"TMPDIR": messy}, expected=target)

    def test_relative_path(self):
        target = self._mkdir("rel")
        self.assert_parity({"TMPDIR": "rel"}, cwd=self.root, expected=target)

    def test_double_leading_slash_preserved(self):
        self.assert_parity({"TMPDIR": "/" + self._mkdir("dbl")})
