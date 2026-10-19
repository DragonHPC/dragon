from __future__ import annotations

import json
import os
import pathlib
import subprocess
import time
import logging
import sys
import socket
import select
import shutil
import threading
import base64
import uuid
import gc
from typing import Optional

import dragon
import dragon.channels as dch
import dragon.managed_memory as dmm
import dragon.utils as du
import dragon.infrastructure.messages as dmsg
import dragon.infrastructure.facts as dfacts
import dragon.infrastructure.runtime_env as druntime_env
import dragon.infrastructure.util as dutil
from dragon.infrastructure.queue import InfraQueue
from dragon.infrastructure.parameters import LaunchParameters
import dragon.infrastructure.parameters as dparms
from dragon.native.process import Process
import dragon.globalservices.api_setup as api_setup
import dragon.transport.oob as doob

LOGGER = logging.getLogger("dragon.workflows.runtime")


def get_logs(name):
    global LOGGER
    log = LOGGER.getChild(name)
    return log.debug, log.info


def _runtime_uid_from_env(runtime_env, fe_ext_ip_addr, head_node_ip_addr, oob_port=None):
    rt_uid = runtime_env.get("DRAGON_RT_UID") if runtime_env else None
    if rt_uid is not None:
        return int(rt_uid)

    return dutil.rt_uid_from_ip_addrs(fe_ext_ip_addr, head_node_ip_addr, oob_port)


def _known_dragon_release(release):
    return isinstance(release, str) and release.strip().lower() not in {"", "unknown", "(unknown)"}


def _validate_runtime_release(local_release, remote_release, remote_rt_uid):
    if not _known_dragon_release(local_release) or not _known_dragon_release(remote_release):
        raise RuntimeError(
            f"Cannot attach to Dragon runtime {remote_rt_uid}: both runtimes must advertise a known Dragon release "
            f"(local={local_release!r}, remote={remote_release!r})"
        )

    if local_release != remote_release:
        raise RuntimeError(
            f"Cannot attach to Dragon runtime {remote_rt_uid}: Dragon release mismatch "
            f"(local={local_release!r}, remote={remote_release!r})"
        )


_current_rt_uid = None
already_published = {}
runtime_table = {}
oob_net_list = []


__all__ = [
    "attach",
    "start_local_runtime",
    "publish",
    "lookup",
    "test_tunnel",
    "current_runtime",
    "DragonRuntime",
    "LocalRuntime",
    "Proxy",
    "RUNTIME_INFO_PREFIX",
]

#: Sentinel prefix printed by the head process to stdout so the parent can
#: locate the control-info JSON line in the stream.
RUNTIME_INFO_PREFIX = "DRAGON_RUNTIME_INFO:"

#: Absolute path to the internal head process script.
_HEAD_SCRIPT = str(pathlib.Path(__file__).resolve().parent / "_runtime_head.py")

#: Environment variables inherited from a parent Dragon runtime that must be
#: stripped before spawning a new runtime subprocess.
_STALE_ENV_VARS = (
    "DRAGON_PATCH_MP",
    "DRAGON_PATCH_TORCH",
    "DRAGON_RT_UID",
    "DRAGON_FE_EXTERNAL_IP_ADDR",
    "DRAGON_HEAD_NODE_IP_ADDR",
)

# ---------------------------------------------------------------------------
# Module-level state — all mutations must hold _lock.
# ---------------------------------------------------------------------------

_lock = threading.Lock()

# ---------------------------------------------------------------------------
# Attach Method that is responsible for attaching the current process to a local or remote Dragon runtime. If attempting to attach to a remote runtime from a process that is not within a dragon runtime we bootstrap a local runtime first and then proxy into the remote runtime. The local runtime is managed as part of the Proxy and will shutdown when either detach or stop is called on the Proxy.
# ---------------------------------------------------------------------------


def attach(
    timeout: float = 30,
    runtime=None,
    auth_token: str = None,
    sdesc_str=None,
    oob_ssh_tunnel_override=None,
    remote_cwd=None,
) -> DragonRuntime:

    if sdesc_str is None:
        _local_runtime = _local_attach(timeout=timeout, runtime=runtime, auth_token=auth_token)
        return _local_runtime
    else:
        _local_runtime = None
        try:
            if not dragon.in_dragon():
                _local_runtime = start_local_runtime()
                _local_runtime = _local_attach(timeout=timeout, runtime=_local_runtime)
            return _remote_attach(
                sdesc_str,
                oob_ssh_tunnel_override=oob_ssh_tunnel_override,
                remote_cwd=remote_cwd,
                local_runtime=_local_runtime,
            )
        except Exception:
            if _local_runtime is not None:
                _local_runtime.stop()
            raise


# ---------------------------------------------------------------------------
# LocalRuntime helpers
# ---------------------------------------------------------------------------


def _resolve_attach_info_path() -> str:
    """Return the filesystem path to the attach-info file.

    See :py:func:`dragon.infrastructure.facts.attach_runtime_dir`, which honours
    the ``DRAGON_ATTACH_DIR`` override.
    """
    return os.path.join(dfacts.attach_runtime_dir(), dfacts.ATTACH_INFO_FILENAME)


def _local_attach(timeout: float = 30, runtime: LocalRuntime = None, auth_token: str = None) -> LocalRuntime:
    """Register this process through LS and wait for direct GS completion.

    Reads the attach-info file written by the Dragon launcher, creates a
    temporary reply queue in the default managed-memory pool, sends a
    :py:class:`~dragon.infrastructure.messages.LSExternalRegister` message to
    the node-local Local Services (``ls_qd``), and — on success — installs
    any gateway environment variables delivered by LS and calls
    :py:func:`~dragon.globalservices.api_setup.connect_as_external` to set up
    the four infrastructure queues so that subsequent Dragon API calls work
    normally.

    The two-stage design means the external process never performs an off-node
    send before gateway environment is installed: LS provisions both return queues and
    submits the prepared registration to Global Services on the process's behalf.

    .. note::
       A process that is already part of a Dragon runtime (``dragon.in_dragon()``
       is ``True``) cannot attach to a new LocalRuntime. This is a hard
       requirement for now, though it may be eased in the future.

    :param timeout: Total seconds allowed for LS provisioning and direct GS
        registration completion. Defaults to 30.
    :type timeout: float
    :param runtime: If provided, use this :py:class:`~dragon.workflows.runtime.LocalRuntime`
        instance's :py:attr:`~dragon.workflows.runtime.LocalRuntime.attach_descr`
        instead of reading the attach-info file from disk.  This bypasses
        filesystem access entirely.
    :type runtime: dragon.workflows.runtime.LocalRuntime, optional
    :param auth_token: Attach auth token to present during external registration. If not
        provided, the ``DRAGON_ATTACH_TOKEN`` environment variable is used.  The
        token is printed by the launcher when the runtime is started with
        ``--attachable`` (or by ``start_local_runtime()`` / ``LocalRuntime.start()``);
        it is no longer stored in
        the attach-info file.
    :type auth_token: str, optional
    :returns: The attached :py:class:`LocalRuntime` handle.
    :rtype: LocalRuntime
    :raises RuntimeError: If no running Dragon runtime is found, the
        attach-info file does not hold a valid AttachableRuntimeDescr, no auth
        token was supplied, registration fails for any non-auth reason, or this
        process is already attached.
    :raises PermissionError: If the supplied auth token does not match the token
        held by Global Services (wrong user / stale token).
    """
    with _lock:
        if runtime is not None and runtime._attached:
            raise RuntimeError("already attached to a Dragon runtime; call detach() first")

        if dragon.in_dragon():
            raise RuntimeError(
                "cannot attach to a LocalRuntime from within an existing Dragon runtime: "
                "this process is already part of a Dragon runtime. "
                "(This restriction is required for now but may be eased in the future.)"
            )

        if runtime is not None:
            token = runtime.auth_token
        else:
            token = auth_token or os.environ.get(dfacts.DRAGON_ATTACH_TOKEN_ENV)

        if not token:
            raise RuntimeError(
                "No attach auth token provided. Pass auth_token=... to attach() or set the "
                f"{dfacts.DRAGON_ATTACH_TOKEN_ENV} environment variable to the token printed by the "
                "launcher (runtime must be started with --attachable)."
            )

        # ------------------------------------------------------------------
        # Step 1 — Obtain the attachable runtime descriptor
        # ------------------------------------------------------------------
        if runtime is not None:
            attach_descr = runtime.attach_descr
        else:
            info_path = _resolve_attach_info_path()
            if not os.path.exists(info_path):
                raise RuntimeError(
                    f"No running Dragon runtime found: attach-info file does not exist at"
                    f" '{info_path}'.  Start a Dragon runtime with 'dragon -s' or"
                    " use LocalRuntime.start() to start a runtime and attach directly to it."
                )
            with open(info_path, "r") as fh:
                encoded_descr = fh.read().strip()
            try:
                attach_descr = dmsg.parse(du.b64decode(encoded_descr))
            except Exception as ex:
                raise RuntimeError(f"attach-info file '{info_path}' could not be parsed: {ex}") from ex

        if not isinstance(attach_descr, dmsg.AttachableRuntimeDescr):
            raise RuntimeError(f"attach-info holds {type(attach_descr).__name__}, expected AttachableRuntimeDescr")

        if not all(
            (
                attach_descr.gs_qd,
                attach_descr.ls_qd,
                attach_descr.default_pd,
                attach_descr.inf_pd,
                attach_descr.rt_uid,
            )
        ):
            raise RuntimeError("attach-info descriptor is missing one or more required fields")

        # If no handle was supplied, build one from the descriptor we just read.
        if runtime is None:
            runtime = LocalRuntime(proc=None, attach_descr=attach_descr, auth_token=token)

        runtime_uid = attach_descr.rt_uid

        previous_dragon_env = {key: value for key, value in os.environ.items() if key.startswith(dfacts.PREFIX)}
        previous_this_process = dparms.this_process
        previous_native_rt_uid = du.get_local_rt_uid()
        previous_runtime_pool = runtime._pool
        infrastructure_mutated = False
        bootstrap_restored = False

        def rollback_attach():
            nonlocal bootstrap_restored
            if bootstrap_restored:
                return
            bootstrap_restored = True

            if infrastructure_mutated:
                api_setup.disconnect_infrastructure()

            if runtime._pool is not None and runtime._pool is not previous_runtime_pool:
                try:
                    runtime._pool.detach()
                except Exception:
                    pass
            runtime._pool = previous_runtime_pool

            for key in list(os.environ):
                if key.startswith(dfacts.PREFIX):
                    del os.environ[key]
            os.environ.update(previous_dragon_env)
            du.set_local_rt_uid(previous_native_rt_uid)
            LaunchParameters.init_class_vars()
            dparms.this_process = previous_this_process

        deadline = None if timeout is None else time.monotonic() + timeout

        def remaining_timeout():
            if deadline is None:
                return None
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("external attach deadline expired")
            return remaining

        # ------------------------------------------------------------------
        # Step 2 — Attach default memory pool and create temp reply queue
        # ------------------------------------------------------------------
        # The C memory-pool layer decides pool locality by comparing DRAGON_RT_UID
        # against the pool's embedded rt_uid. Set it before attaching the pool so the
        # default pool is recognized as local (required for channel creation).
        os.environ["DRAGON_RT_UID"] = runtime_uid
        du.set_local_rt_uid(int(runtime_uid))

        reply_ch = None
        reply_q = None
        ls_in_q = None

        def release_provisioning_resources():
            nonlocal reply_ch, reply_q, ls_in_q
            for queue in (reply_q, ls_in_q):
                if queue is not None:
                    try:
                        queue.close()
                    except Exception:
                        pass
            reply_q = None
            ls_in_q = None
            if reply_ch is not None:
                try:
                    reply_ch.destroy()
                except Exception:
                    pass
                reply_ch = None

        try:
            pool = dmm.MemoryPool.attach(du.B64.str_to_bytes(attach_descr.default_pd))
            runtime._pool = pool

            temp_cuid = dfacts.attach_temp_cuid_from_pid(os.getpid())
            reply_ch = dch.Channel(pool, temp_cuid)
            # Wrap the temp channel in an InfraQueue; its serialized (queue) descriptor
            # is what LS attaches to in order to deliver the response.  The channel is
            # externally provided so the queue will not destroy it on cleanup.
            reply_q = InfraQueue(pool=pool, main_channel=reply_ch)
            proc_resp_sdesc = reply_q.serialize()

            # ------------------------------------------------------------------
            # Step 3 — Attach node-local LS input queue
            # ------------------------------------------------------------------
            ls_in_q = InfraQueue.attach(attach_descr.ls_qd)

            # ------------------------------------------------------------------
            # Step 4 — Send LSExternalRegister to Local Services
            # ------------------------------------------------------------------
            req = dmsg.LSExternalRegister(
                tag=api_setup.next_tag(),
                auth_token=token,
                pid=os.getpid(),
                proc_resp_sdesc=proc_resp_sdesc,
                runtime_incarnation=runtime_uid,
                attempt_id=uuid.uuid4().hex,
            )
        except Exception:
            release_provisioning_resources()
            rollback_attach()
            raise

        def cancel_registration(p_uid=0):
            cancel_queue = None
            try:
                cancel_queue = InfraQueue.attach(attach_descr.ls_qd)
                cancel_queue.put(
                    dmsg.LSExternalRegisterCancel(
                        tag=api_setup.next_tag(),
                        runtime_incarnation=runtime_uid,
                        attempt_id=req.attempt_id,
                        p_uid=p_uid,
                    ),
                    timeout=0,
                )
            except Exception:
                pass
            finally:
                if cancel_queue is not None:
                    cancel_queue.close()

        def release_local_pool():
            rollback_attach()

        try:
            ls_in_q.put(req)
        except Exception:
            cancel_registration()
            release_provisioning_resources()
            release_local_pool()
            raise

        # ------------------------------------------------------------------
        # Step 5 — Receive LS provisioning on the temporary queue
        # ------------------------------------------------------------------
        try:
            resources = reply_q.get(timeout=remaining_timeout())
        except Exception as e:
            cancel_registration()
            release_provisioning_resources()
            release_local_pool()
            raise RuntimeError(f"Failed waiting for LSExternalResourcesReady: {e}") from e

        release_provisioning_resources()

        if not isinstance(resources, dmsg.LSExternalResourcesReady):
            cancel_registration()
            release_local_pool()
            raise RuntimeError(f"Dragon attach() received unexpected provisioning message {type(resources).__name__}")
        if resources.err == dmsg.LSExternalResourcesReady.Errors.AUTH_FAILURE:
            release_local_pool()
            raise PermissionError(
                "Dragon attach() rejected: authentication token mismatch."
                "  Ensure you are running as the same OS user as the Dragon runtime."
            )
        if resources.err != dmsg.LSExternalResourcesReady.Errors.SUCCESS:
            release_local_pool()
            err_info = getattr(resources, "errInfo", "") or ""
            raise RuntimeError(
                "Dragon attach() provisioning failed: " + resources.err.name + (f": {err_info}" if err_info else "")
            )
        if (
            resources.runtime_incarnation != runtime_uid
            or resources.attempt_id != req.attempt_id
            or resources.node_index != attach_descr.node_index
        ):
            cancel_registration(resources.p_uid)
            release_local_pool()
            raise RuntimeError("Dragon attach() provisioning identity mismatch")

        # ------------------------------------------------------------------
        # Step 6 — Install the authoritative gateway environment
        # ------------------------------------------------------------------
        num_gw = resources.num_gw_channels_per_node
        gw_env = resources.gw_env or {}
        expected_gateway_keys = {f"{dfacts.GW_ENV_PREFIX}{index}" for index in range(1, num_gw + 1)}
        if set(gw_env) != expected_gateway_keys:
            cancel_registration(resources.p_uid)
            release_local_pool()
            raise RuntimeError("Dragon attach() received an incomplete or unexpected gateway environment")

        for key in list(os.environ):
            suffix = key.removeprefix(dfacts.GW_ENV_PREFIX)
            if key.startswith(dfacts.GW_ENV_PREFIX) and suffix.isdigit():
                del os.environ[key]
        os.environ[dfacts.PREFIX + dfacts.NUM_GW_CHANNELS_PER_NODE_VAR] = str(num_gw)
        os.environ.update(gw_env)

        # ------------------------------------------------------------------
        # Step 7 — Wait for final GS commit on the provisioned return queue
        # ------------------------------------------------------------------
        gs_return_q = InfraQueue.attach(resources.gs_return_qd)
        try:
            completion = gs_return_q.get(timeout=remaining_timeout())
        except Exception as e:
            gs_return_q.close()
            cancel_registration(resources.p_uid)
            release_local_pool()
            raise RuntimeError(f"Failed waiting for GSExternalRegisterComplete: {e}") from e
        gs_return_q.close()

        if not isinstance(completion, dmsg.GSExternalRegisterComplete):
            cancel_registration(resources.p_uid)
            release_local_pool()
            raise RuntimeError(f"Dragon attach() received unexpected completion {type(completion).__name__}")
        if completion.err != dmsg.GSExternalRegisterComplete.Errors.SUCCESS:
            cancel_registration(resources.p_uid)
            release_local_pool()
            err_info = getattr(completion, "errInfo", "") or ""
            raise RuntimeError(
                "Dragon attach() registration failed: " + completion.err.name + (f": {err_info}" if err_info else "")
            )
        if completion.p_uid != resources.p_uid:
            cancel_registration(resources.p_uid)
            release_local_pool()
            raise RuntimeError("Dragon attach() completion PUID does not match provisioning")

        # ------------------------------------------------------------------
        # Step 8 — Publish bootstrap state and connect infrastructure queues
        # ------------------------------------------------------------------
        try:
            params = LaunchParameters.from_attach_info(attach_descr, resources)
            infrastructure_mutated = True
            api_setup.connect_as_external(params)
        except Exception:
            cancel_registration(resources.p_uid)
            release_local_pool()
            raise

        # ------------------------------------------------------------------
        # Step 9 — Record attached state on the runtime handle
        # ------------------------------------------------------------------
        runtime._pre_attach_dragon_env = previous_dragon_env
        runtime._pre_attach_this_process = previous_this_process
        runtime._pre_attach_native_rt_uid = previous_native_rt_uid
        runtime._attached = True
        runtime._puid = resources.p_uid
        runtime._pending_detach_ref = None

    return runtime


def start_local_runtime(
    timeout: float = 60,
    dragon_cmd: str = None,
) -> LocalRuntime:
    """Start a single-node Dragon runtime and return a handle to it.

    Spawns ``dragon -s -a <internal_head_script>`` as a subprocess, waits for
    the runtime to become ready by reading a sentinel line from its stdout,
    and returns a :py:class:`LocalRuntime` instance.

    :param timeout: Seconds to wait for the runtime to become ready.
    :type timeout: float
    :param dragon_cmd: Path to the ``dragon`` command. If ``None``, the command
        is looked up on PATH and invoked with ``-s`` and ``-a`` to start the
        internal dragon-waiting-runtime-head script.
    :type dragon_cmd: str, optional
    :returns: A handle to the running Dragon runtime.
    :rtype: LocalRuntime
    :raises FileNotFoundError: If the ``dragon`` command cannot be found.
    :raises RuntimeError: If the runtime does not become ready within
        *timeout* seconds or exits prematurely.
    """
    if dragon_cmd is None:
        dragon_cmd = shutil.which("dragon")
        # proc_args = [dragon_cmd, "-s", "-a", "-l", "DEBUG", _HEAD_SCRIPT]
        proc_args = [dragon_cmd, "-s", "-a", _HEAD_SCRIPT]

    # Strip env vars inherited from a parent Dragon session that would
    # confuse the new runtime subprocess.
    # This is likely unnecessary for the use case that I imagine for this but maybe there is a good reason to leave it.
    env = os.environ.copy()
    for var in _STALE_ENV_VARS:
        env.pop(var, None)

    proc = subprocess.Popen(
        proc_args,
        stdout=subprocess.PIPE,
        stderr=None,  # inherit caller's stderr so startup errors are visible
        env=env,
    )

    # -------------------------------------------------------------------
    # Read stdout bytes until we find the complete sentinel+JSON line.
    # We accumulate raw bytes so we are immune to any LS message-framing
    # granularity (SHFwdOutput messages may not align with newlines).
    # -------------------------------------------------------------------
    import time

    deadline = time.monotonic() + timeout
    buf = b""
    prefix_bytes = RUNTIME_INFO_PREFIX.encode("utf-8")
    info = None

    try:
        while time.monotonic() < deadline:
            # Check for premature subprocess exit.
            if proc.poll() is not None:
                raise RuntimeError(
                    f"Dragon runtime exited prematurely with code " f"{proc.returncode} before becoming ready."
                )
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            rlist, _, _ = select.select([proc.stdout], [], [], min(remaining, 1.0))
            if not rlist:
                continue
            # read1() returns whatever bytes are available without blocking.
            chunk = proc.stdout.read1(4096)
            if not chunk:
                raise RuntimeError("Dragon runtime stdout closed before sentinel was received.")
            buf += chunk
            # Search for a complete sentinel line in the accumulated buffer.
            idx = buf.find(prefix_bytes)
            if idx >= 0:
                rest = buf[idx + len(prefix_bytes) :]
                nl = rest.find(b"\n")
                if nl >= 0:
                    info = json.loads(rest[:nl].decode("utf-8"))
                    break
    except Exception:
        proc.terminate()
        proc.wait(timeout=5)
        raise

    if info is None:
        proc.terminate()
        proc.wait(timeout=5)
        raise RuntimeError(f"Timed out waiting for Dragon runtime to become ready " f"after {timeout}s.")

    print(f"{info=}", flush=True)

    auth_token = info.get("auth_token")

    # Drain remaining subprocess stdout in a daemon thread so the OS pipe
    # buffer never fills and blocks the dragon subprocess during teardown.
    def _drain(pipe):
        try:
            while True:
                data = pipe.read1(4096)
                if not data:
                    break
        except (OSError, ValueError):
            pass

    threading.Thread(target=_drain, args=(proc.stdout,), daemon=True).start()

    return LocalRuntime(
        proc=proc,
        attach_descr=dmsg.AttachableRuntimeDescr.from_sdict({**info, "tag": 0}),
        auth_token=auth_token,
    )


# ---------------------------------------------------------------------------
# Proxy helpers
# ---------------------------------------------------------------------------


def free_port(host, port):
    LOGGER.debug("Checking if port %d is free on host %s", port, host)
    connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        connection.connect((host, int(port)))
        connection.shutdown(socket.SHUT_WR)
        return False
    except ConnectionRefusedError:
        return True
    finally:
        connection.close()


def get_port(host, start_port, end_port):
    for port in range(start_port, end_port + 1):
        if free_port(host, port):
            return port
    raise RuntimeError("No available ports")


def get_ip():
    connection = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    connection.settimeout(0)
    try:
        connection.connect(("10.254.254.254", 1))
        return connection.getsockname()[0]
    except Exception:
        return "127.0.0.1"
    finally:
        connection.close()


def _get_sdesc(oob_port=None):
    """Return the current runtime descriptor as Base64-encoded text.

    :return: Base64-encoded serialized runtime descriptor
    :rtype: str
    """

    LOGGER.debug("Getting serialized runtime descriptor")
    gs_qd = os.environ[dfacts.env_name("GS_QD")]
    gs_ret_qd = os.environ[dfacts.env_name("GS_RET_QD")]
    local_ls_qd = os.environ[dfacts.env_name("LOCAL_LS_QD")]
    ls_ret_qd = os.environ[dfacts.env_name("LS_RET_QD")]

    fe_ext_ip_addr = os.environ["DRAGON_FE_EXTERNAL_IP_ADDR"]
    head_node_ip_addr = os.environ["DRAGON_HEAD_NODE_IP_ADDR"]
    if oob_port is None:
        oob_port = int(os.environ.get("DRAGON_TRANSPORT_PORT", dfacts.DEFAULT_TRANSPORT_PORT))
    python_path = sys.executable
    sdesc = dmsg.RuntimeDesc(
        0,
        gs_qd,
        gs_ret_qd,
        local_ls_qd,
        ls_ret_qd,
        fe_ext_ip_addr,
        head_node_ip_addr,
        oob_port,
        python_path,
        os.environ,
        dragon_version=dragon.__version__,
    )
    return du.b64encode(sdesc.serialize())


def get_sdesc(oob_port=None):
    """Return the current runtime descriptor as Base64 text."""
    return _get_sdesc(oob_port=oob_port)


def publish(name, publish_dir=None):
    """Publish the current runtime descriptor to make it discoverable by other systems.

    This function creates and publishes a serialized runtime descriptor that contains all necessary information for remote systems to connect to the current Dragon runtime. The descriptor is written to a file in the specified directory.

    The published descriptor can later be retrieved by other systems using the lookup() function to establish remote execution capabilities.

    :param name: Identifier name for the published runtime. This will be used as the filename
    :type name: str
    :param publish_dir: Directory where the descriptor file will be written. If None, uses ~/.dragon, defaults to None
    :type publish_dir: str, optional
    :return: Base64-encoded runtime descriptor string
    :rtype: str
    :raises RuntimeError: If the runtime has already been published with the given name
    """
    LOGGER.debug("Publishing runtime descriptor with name: %s", name)
    global already_published

    if publish_dir is not None:
        dragon_dir = publish_dir
    else:
        home_dir = pathlib.Path.home()
        dragon_dir = home_dir / ".dragon"

    publish_path = f"{dragon_dir}/{name}"
    pathlib.Path(dragon_dir).mkdir(parents=True, exist_ok=True)

    published_sdesc = already_published.get(publish_path)
    if published_sdesc is not None:
        return published_sdesc

    transport_port = int(os.environ.get("DRAGON_TRANSPORT_PORT", dfacts.DEFAULT_TRANSPORT_PORT))
    oob_net = doob.OutOfBand()
    publish_oob_port = transport_port

    if oob_net.uses_python_tcp_user_transport():
        # The legacy TCP OOB helper must listen on its own port because the
        # runtime's normal transport agent already owns DRAGON_TRANSPORT_PORT.
        publish_oob_port = get_port("127.0.0.1", transport_port + 1, transport_port + 100)

    published_sdesc = _get_sdesc(oob_port=publish_oob_port)

    try:
        with open(publish_path, "w") as publish_file:
            publish_file.write(published_sdesc)
    except OSError as exc:
        raise RuntimeError(f"publish failed: unable to write runtime descriptor to {publish_path}: {exc}") from exc

    try:
        if oob_net.accept(publish_oob_port):
            oob_net_list.append(oob_net)
    except Exception:
        try:
            os.remove(publish_path)
        except OSError:
            pass
        raise

    already_published[publish_path] = published_sdesc

    return published_sdesc


def lookup(system, name, timeout_in=None, publish_dir=None):
    """Look up and retrieve a published runtime descriptor from a remote system.

    This function attempts to retrieve a serialized runtime descriptor that was previously published on a remote system using the publish() function. It uses SCP to copy the descriptor file from the remote system to the local machine.

    Each call fetches the current descriptor. The returned string is a snapshot
    and does not track a runtime that later replaces the published instance.

    The function will retry the lookup operation until the specified timeout is reached, sleeping 1 second between attempts. This allows time for the descriptor file to become visible on distributed/parallel file systems.

    :param system: Hostname or IP address of the remote system where the runtime was published
    :type system: str
    :param name: Name identifier used when the runtime descriptor was published
    :type name: str
    :param timeout_in: Maximum time in seconds to wait for the descriptor file, defaults to 1
    :type timeout_in: int, optional
    :param publish_dir: Directory path where the descriptor was published. If None, uses ~/.dragon, defaults to None
    :type publish_dir: str, optional
    :return: Base64-encoded runtime descriptor string
    :rtype: str
    :raises RuntimeError: If the descriptor could not be retrieved within the timeout period
    """
    LOGGER.debug("Looking up runtime descriptor for system: %s, name: %s", system, name)

    # loop until timeout trying to scp the sdesc file from the remote system,
    # sleeping 1 seconds between each attempt
    time_so_far = 0
    if timeout_in is None:
        timeout = 1
    else:
        timeout = timeout_in

    if publish_dir is not None:
        dragon_dir = publish_dir
    else:
        home_dir = pathlib.Path.home()
        dragon_dir = home_dir / ".dragon"
    publish_path = f"{dragon_dir}/{name}"

    while time_so_far < timeout:
        rc = os.system(f"scp {system}:{publish_path} . > /dev/null 2>&1")
        if rc == 0:
            with open(name, "r") as publish_file:
                sdesc = publish_file.read()
            os.remove(name)
            return sdesc
        else:
            # do ls on remote .dragon directory to force visibility of
            # newly created files on the parallel file system
            os.system(f'ssh {system} "ls {publish_path}" > /dev/null 2>&1')
            time.sleep(1)
            time_so_far += 1

    raise RuntimeError(f"lookup failed: could not obtain serialized descriptor for {system=} and {name=}")


def test_tunnel(sdesc_str, oob_ssh_tunnel_override=None, connect_timeout=10):
    """Run a non-interactive SSH preflight check for a remote runtime tunnel.

    This helper parses a serialized runtime descriptor and tests the SSH route
    used for OOB connectivity without starting transport agents.

    :param sdesc_str: Serialized runtime descriptor string obtained from lookup()
    :type sdesc_str: str
    :param oob_ssh_tunnel_override: Optional SSH tunnel command template override
    :type oob_ssh_tunnel_override: str, optional
    :param connect_timeout: SSH connect timeout in seconds for the preflight check
    :type connect_timeout: int, optional
    :return: Diagnostic result from OutOfBand.test_tunnel
    """
    LOGGER.debug("Running SSH tunnel preflight for runtime descriptor")
    sdesc = dmsg.parse(du.b64decode(sdesc_str))

    jump_host = sdesc.fe_ext_ip_addr
    compute_node = sdesc.head_node_ip_addr
    tunnel_port = sdesc.oob_port

    oob_net = doob.OutOfBand(oob_ssh_tunnel_override=oob_ssh_tunnel_override)
    return oob_net.test_tunnel(jump_host, compute_node, int(tunnel_port), connect_timeout=connect_timeout)


def _remote_attach(sdesc_str, oob_ssh_tunnel_override=None, remote_cwd=None, local_runtime=None):
    """Attach to a remote Dragon runtime using a serialized descriptor.

    This function establishes a connection to a remote Dragon runtime by parsing the provided serialized descriptor and setting up an out-of-band network tunnel. It creates and returns a Proxy object that can be used to switch between local and remote runtime environments.

    The returned Proxy object allows processes and Dragon resources created locally to interact with the remote runtime, enabling distributed execution across systems.

    Descriptor strings are snapshots. Call lookup() again before a new attach if
    the published runtime may have been replaced since the descriptor was read.

    :param sdesc_str: Base64-encoded runtime descriptor string from lookup()
    :type sdesc_str: str
    :param oob_ssh_tunnel_override: Optional override for SSH tunnel configuration, defaults to None
    :type oob_ssh_tunnel_override: str, optional
    :param remote_cwd: Current working directory to use on the remote runtime, defaults to None
    :type remote_cwd: str, optional
    :return: Proxy object for managing the remote runtime connection
    :rtype: Proxy
    """
    LOGGER.debug("Attaching to remote runtime with sdesc: %s", sdesc_str)
    sdesc = dmsg.parse(du.b64decode(sdesc_str))

    remote_env = json.loads(sdesc.env) if sdesc.env else {}
    remote_overlay_transport = remote_env.get(dfacts.OVERLAY_TRANSPORT_VAR)

    jump_host = sdesc.fe_ext_ip_addr
    compute_node = sdesc.head_node_ip_addr
    tunnel_port = sdesc.oob_port
    remote_python_executable = sdesc.python_path
    remote_rt_uid = _runtime_uid_from_env(remote_env, jump_host, compute_node, tunnel_port)
    _validate_runtime_release(dragon.__version__, getattr(sdesc, "dragon_version", None), remote_rt_uid)

    oob_net = doob.OutOfBand(oob_ssh_tunnel_override=oob_ssh_tunnel_override)
    try:
        oob_net.connect(
            jump_host,
            compute_node,
            int(tunnel_port),
            remote_overlay_transport=remote_overlay_transport,
            remote_rt_uid=remote_rt_uid,
        )
    except Exception:
        oob_net.close()
        raise

    proxy = Proxy(sdesc, oob_net, remote_cwd, remote_python_executable, local_runtime=local_runtime)
    if remote_rt_uid in runtime_table:
        runtime_table[remote_rt_uid].close()
    runtime_table[remote_rt_uid] = proxy

    return proxy


# ---------------------------------------------------------------------------
# LocalRuntime API
# ---------------------------------------------------------------------------


class DragonRuntime:
    """Base class for Dragon runtime handles."""

    def detach(self):
        """Detach from the runtime, if attached."""
        pass

    def stop(self):
        """Stop the runtime, if running."""
        pass


class LocalRuntime(DragonRuntime):
    """Handle to a running Dragon runtime subprocess.

    Returned by :py:func:`start_local_runtime` and
    :py:meth:`LocalRuntime.start`. Supports the context-manager
    protocol: exiting the ``with`` block calls :py:meth:`stop`.

    Do not construct directly — use :py:func:`start_local_runtime` or
    :py:meth:`LocalRuntime.start`.

    :param proc: The subprocess running the Dragon launcher, or ``None`` if
        this handle is attach-only (does not own a subprocess).
    :type proc: subprocess.Popen or None
    :param attach_descr: Descriptor of the attachable runtime (may be ``None``).
    :type attach_descr: dragon.infrastructure.messages.AttachableRuntimeDescr or None
    :param auth_token: Attach auth token for the runtime, or ``None`` if the
        runtime was not started attachable.  Not stored in ``attach_descr``.
    :type auth_token: str or None
    """

    def __init__(
        self,
        proc: subprocess.Popen = None,
        attach_descr: dmsg.AttachableRuntimeDescr = None,
        auth_token: str = None,
    ):
        self._proc = proc
        self._attach_descr = attach_descr
        self._auth_token = auth_token
        self._stopped = False
        self._attached = False
        self._puid = None
        self._pending_detach_ref = None
        self._pool = None  # MemoryPool ref kept alive while attached
        self._pre_attach_dragon_env = None
        self._pre_attach_this_process = None
        self._pre_attach_native_rt_uid = None
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Class methods
    # ------------------------------------------------------------------

    @classmethod
    def start(cls, timeout: float = 60) -> LocalRuntime:
        """Start a single-node Dragon runtime and return a handle to it.

        Alternative constructor that delegates to :py:func:`start_local_runtime`.

        :param timeout: Seconds to wait for the runtime to become ready.
        :type timeout: float
        :returns: A handle to the running Dragon runtime.
        :rtype: LocalRuntime
        """
        rt = start_local_runtime(timeout=timeout)
        return attach(runtime=rt)

    @property
    def attach_descr(self) -> dmsg.AttachableRuntimeDescr:
        """Descriptor of the attachable runtime this handle refers to.

        :rtype: dragon.infrastructure.messages.AttachableRuntimeDescr
        :raises RuntimeError: If this handle has no descriptor.
        """
        if self._attach_descr is None:
            raise RuntimeError("This LocalRuntime has no attachable runtime descriptor.")
        return self._attach_descr

    @property
    def auth_token(self) -> Optional[str]:
        """Attach auth token for this runtime, or ``None`` if not attachable.

        Pass this to :py:func:`~dragon.workflows.runtime.attach` via
        ``auth_token=`` (or set the ``DRAGON_ATTACH_TOKEN`` environment
        variable) to attach a co-located process. The token is intentionally
        not stored in the attach-info dict.

        :rtype: str or None
        """
        return self._auth_token

    def _restore_pre_attach_process_state(self) -> None:
        """Restore the environment, local runtime identity, and launch parameters."""
        if self._pre_attach_dragon_env is None and self._pre_attach_this_process is None:
            return

        for key in list(os.environ):
            if key.startswith(dfacts.PREFIX):
                del os.environ[key]

        if self._pre_attach_dragon_env is not None:
            os.environ.update(self._pre_attach_dragon_env)

        if self._pre_attach_native_rt_uid is not None:
            du.set_local_rt_uid(self._pre_attach_native_rt_uid)

        LaunchParameters.init_class_vars()

        if self._pre_attach_this_process is not None:
            dparms.this_process = self._pre_attach_this_process
        else:
            dparms.reload_this_process()

        self._pre_attach_dragon_env = None
        self._pre_attach_this_process = None
        self._pre_attach_native_rt_uid = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def stop(self, timeout: float = 30) -> None:
        """Signal the runtime to shut down and (if owned) wait for it to exit.

        Retrieves the ctrl_event from the Local Services KVS (stored there by
        every LS node when the head process sent
        :py:class:`~dragon.infrastructure.messages.GSRegisterCtrlEvent` to GS
        at startup) and calls :py:meth:`~dragon.native.event.Event.set` on it.
        This unblocks the head process's ``ctrl_event.wait()``, which causes
        it to return and triggers Dragon teardown.  Idempotent — calling twice
        has no effect.

        **Owned subprocess** (handle returned by
        :py:func:`start_local_runtime`):
        after setting the event this method waits up to *timeout* seconds for
        the subprocess to exit; if the deadline is exceeded, SIGTERM is sent.

        **Attach-only handle** (``proc is None``):
        the event is set but the method returns immediately afterwards — it
        does not wait for process exit because the caller does not own the
        subprocess.

        :param timeout: Seconds to wait for an owned subprocess to exit after
            sending the shutdown signal.  If exceeded, SIGTERM is sent.
            Ignored when this is an attach-only handle.
        :type timeout: float
        """

        def cleanup_local_attach_state():
            # stop() tears down the runtime itself; any local attachment state
            # should be cleared regardless of whether shutdown was graceful.
            if self._attached:
                try:
                    api_setup.disconnect_infrastructure()
                except Exception:
                    pass

            if self._pool is not None:
                try:
                    self._pool.detach()
                except Exception:
                    pass
                self._pool = None

            self._attached = False
            self._puid = None
            self._pending_detach_ref = None
            self._restore_pre_attach_process_state()
            gc.collect()

        with self._lock:
            if self._stopped:
                return
            if self._proc is None:
                # Attach-only handle: set the event but do not wait for exit.
                try:
                    self._set_ctrl_event()
                except Exception:
                    pass
                cleanup_local_attach_state()
                self._stopped = True
                return
            try:
                if self._proc.poll() is not None:
                    return
                try:
                    self._set_ctrl_event()
                except Exception:
                    # If the event cannot be set (e.g., runtime already gone),
                    # still wait for the subprocess to exit.
                    pass
                cleanup_local_attach_state()
                try:
                    self._proc.wait(timeout=timeout)
                except subprocess.TimeoutExpired:
                    self._proc.terminate()
                    try:
                        self._proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        pass
            finally:
                cleanup_local_attach_state()
                self._stopped = True

    # ------------------------------------------------------------------
    # Context manager / repr
    # ------------------------------------------------------------------

    def __enter__(self) -> LocalRuntime:
        return self

    def __exit__(self, *args) -> None:
        self.stop()

    def __repr__(self) -> str:
        pid = self._proc.pid if self._proc is not None else None
        return f"LocalRuntime(pid={pid}, stopped={self._stopped})"

    def detach(self) -> None:
        """Gracefully detach this process from the Dragon runtime.

        Sends :py:class:`~dragon.infrastructure.messages.GSProcessDetach` to
        Global Services, waits for
        :py:class:`~dragon.infrastructure.messages.GSProcessDetachResponse` on
        the GS-return channel, then calls
        :py:func:`~dragon.globalservices.api_setup.disconnect_infrastructure` to
        close the four infrastructure connections and release the default pool.

        :raises RuntimeError: If this process is not currently attached.
        """
        with _lock:
            if not self._attached:
                raise RuntimeError("not attached to a Dragon runtime; call attach() first")

            if self._pending_detach_ref is None:
                req = dmsg.GSProcessDetach(
                    tag=api_setup.next_tag(),
                    p_uid=self._puid,
                    r_c_uid=api_setup._GS_RETURN_CUID,
                )
                # gs_request() sends to GS-INPUT and blocks until the response
                # arrives on the gs_ret channel keyed by req.tag.
                response = api_setup.gs_request(req)
                if not isinstance(response, dmsg.GSProcessDetachResponse):
                    raise RuntimeError(f"Dragon detach received unexpected response {type(response).__name__}")
                if response.err != dmsg.GSProcessDetachResponse.Errors.SUCCESS:
                    err_info = getattr(response, "errInfo", "") or ""
                    raise RuntimeError(
                        "Dragon detach failed: " + response.err.name + (f": {err_info}" if err_info else "")
                    )
                self._pending_detach_ref = response.ref

            api_setup.gs_request(
                dmsg.GSProcessDetachAck(
                    tag=api_setup.next_tag(),
                    p_uid=self._puid,
                    detach_ref=self._pending_detach_ref,
                ),
                expecting_response=False,
            )

            # disconnect_infrastructure() closes the four infra connections and
            # detaches the default pool that connect_as_external() attached.
            api_setup.disconnect_infrastructure()

            # runtime._pool holds a *separate* attach() to the same pool.
            # disconnect_infrastructure() already detached connect_as_external()'s
            # handle; we try to detach ours too, ignoring errors (e.g. if the
            # underlying pool was already torn down by the runtime).
            if self._pool is not None:
                try:
                    self._pool.detach()
                except Exception:
                    pass
                self._pool = None

            self._attached = False
            self._puid = None
            self._pending_detach_ref = None
            self._restore_pre_attach_process_state()
            gc.collect()

    def _set_ctrl_event(self) -> None:
        """Signal the attached Dragon runtime to shut down.

        Retrieves the serialized ctrl_event that the runtime head process
        registered with Global Services (broadcast to every Local Services node's
        key-value store under
        :py:data:`~dragon.infrastructure.facts.CTRL_EVENT_KVS_KEY`), reconstructs
        the :py:class:`~dragon.native.event.Event`, and sets it.  Setting the event
        unblocks the head process's ``ctrl_event.wait()`` call in
        ``_runtime_head.py``, which returns and triggers Dragon runtime teardown.

        This process must be attached (via :py:func:`attach`) before calling
        ``stop`` because reconstructing the ctrl_event requires the default managed
        memory pool and infrastructure channels set up during attach.

        :raises RuntimeError: If this process is not attached, or the ctrl_event
            key has not yet been written to the KVS.
        """
        from dragon.utils import get_local_kv
        from dragon.native.event import Event

        with _lock:
            if not self._attached:
                raise RuntimeError("not attached to a Dragon runtime; call attach() first")

        try:
            raw = get_local_kv(dfacts.CTRL_EVENT_KVS_KEY)
        except KeyError as e:
            raise RuntimeError(
                f"ctrl_event not found in Local Services KVS (key={dfacts.CTRL_EVENT_KVS_KEY!r}). "
                "The runtime head process may not have finished starting."
            ) from e

        state = json.loads(raw)
        sdesc = base64.b64decode(state["sdesc"])
        cuid = state["cuid"]

        ctrl_event = Event.__new__(Event)
        ctrl_event.__setstate__((sdesc, cuid))
        ctrl_event.set()


# ---------------------------------------------------------------------------
# Proxy API
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Runtime activation stack.
#
# .. warning::
#    NOT thread-safe. enable()/disable() mutate process-global state
#    (os.environ and the dragon.globalservices.api_setup connection globals),
#    so the "active" runtime is shared by every thread in the process. This
#    stack is deliberately a plain module-global rather than a thread-local:
#    a thread-local would falsely imply per-thread isolation that does not
#    exist. Only enable/disable runtimes from a single thread, or serialize
#    access to them externally.
# ---------------------------------------------------------------------------

_runtime_stack = []


def _get_runtime_stack():
    """Return the process-global stack of active runtimes.

    Each entry is a ``(runtime, saved_env)`` tuple: the runtime that was
    activated and the infrastructure environment it displaced.

    .. warning::
       Not thread-safe; see the module note above.
    """
    return _runtime_stack


def current_runtime():
    """Return the runtime currently active in this process, or ``None``.

    .. warning::
       The active runtime is process-global and not thread-safe.

    :rtype: DragonRuntime or None
    """
    return _runtime_stack[-1][0] if _runtime_stack else None


def get_current_inf_env():
    LOGGER.debug("Getting current infrastructure environment")
    return druntime_env.get_current_inf_env()


def set_inf_env(env, *, update_rt_uid=True):
    LOGGER.debug("Setting infrastructure environment")
    druntime_env.set_inf_env(env, update_rt_uid=update_rt_uid)
    global _current_rt_uid
    _current_rt_uid = int(env["DRAGON_RT_UID"]) if update_rt_uid else du.get_local_rt_uid()


def _signal_exit(exit_path):
    with open(exit_path, "w", encoding="utf-8"):
        pass


class Proxy(DragonRuntime):

    def __init__(self, sdesc, oob_net, cwd=None, remote_python_executable=None, local_runtime=None):
        """Initialize a Proxy object for managing remote runtime connections.

        :param sdesc: Serialized descriptor of the remote runtime containing connection information
        :type sdesc: dragon.infrastructure.messages.RuntimeDesc
        :param oob_net: Out-of-band network connection to remote runtime for communication
        :type oob_net: dragon.transport.oob.OutOfBand
        :param cwd: Current working directory to use on remote runtime, defaults to None
        :type cwd: str, optional
        :param remote_python_executable: Path to Python executable on remote runtime, defaults to None
        :type remote_python_executable: str, optional
        """

        self._sdesc = sdesc
        self._env = json.loads(sdesc.env)
        self._oob_net = oob_net
        self._debug, self._info = get_logs("Proxy")
        self._cwd = cwd
        self._remote_python_executable = remote_python_executable
        self._local_runtime = local_runtime
        self._gateways_registered = False

    def close(self):
        """Release transport resources without stopping a runtime."""
        self._oob_net.close()

    def detach(self):
        """Detach from the proxy runtime.

        This currently tears down the local helper runtime (if one was started
        for the proxy) and does not send a detach message to the remote runtime.
        """
        stack = _get_runtime_stack()
        if stack and stack[-1][0] is self:
            self.disable()
        self.close()
        if self._local_runtime is not None:
            self._local_runtime.stop()

    def stop(self, exit_path, remote_working_dir):
        exit_proc = Process(target=_signal_exit, args=(exit_path,), cwd=remote_working_dir)
        exit_proc.start()
        exit_proc.join()

        # Restore the displaced infra env before stopping the local helper
        # runtime; LocalRuntime.stop() must run against local infrastructure.
        stack = _get_runtime_stack()
        if stack and stack[-1][0] is self:
            self.disable()

        self.close()
        if self._local_runtime is not None:
            self._local_runtime.stop()

    def __enter__(self):
        """Enter the runtime context manager, activating this runtime.

        :return: This runtime instance
        :rtype: DragonRuntime
        """
        self.enable()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Exit the runtime context manager, restoring the previous runtime."""
        self.disable()

    def enable(self):
        """Activate this runtime, making it the current one for the process.

        Pushes this runtime onto the process-global runtime stack together with
        a snapshot of the infrastructure environment it displaces, then calls
        the subclass :py:meth:`_activate` hook. The saved env lets
        :py:meth:`disable` restore whatever runtime (or base/local env) was
        active before, which is what makes nested ``with`` blocks work.

        .. warning::
           Not thread-safe. This mutates process-global state (``os.environ``
           and the ``api_setup`` connection globals). Only enable/disable
           runtimes from a single thread, or serialize access externally.
        """
        LOGGER.debug("Enabling runtime %r", self)
        stack = _get_runtime_stack()
        # Snapshot the env this runtime displaces (the parent runtime's env, or
        # the base/local env if the stack is empty) so disable() can restore it.
        saved_env = get_current_inf_env()
        saved_proxy_env = {key: os.environ[key] for key in druntime_env.PROXY_ENV_KEYS if key in os.environ}
        stack.append((self, saved_env))
        try:
            if not druntime_env.proxy_enabled():
                druntime_env.preserve_local_proxy_pool_env(saved_env)
            self._activate()
        except Exception:
            stack.pop()
            try:
                set_inf_env(saved_env)
            finally:
                for key in druntime_env.PROXY_ENV_KEYS:
                    os.environ.pop(key, None)
                os.environ.update(saved_proxy_env)
            raise

    def disable(self):
        """Deactivate this runtime, restoring the previously active one.

        Pops this runtime off the process-global stack (enforcing LIFO order),
        runs the subclass :py:meth:`_deactivate` hook, restores the infra env
        this runtime displaced, and — if a parent runtime remains on the
        stack — re-applies its non-infra (user) environment via
        :py:meth:`_reactivate`.

        .. warning::
           Not thread-safe. See :py:meth:`enable`.
        """
        LOGGER.debug("Disabling runtime %r", self)
        stack = _get_runtime_stack()
        if not stack or stack[-1][0] is not self:
            raise RuntimeError(
                "disable() called out of order: this runtime is not the " "currently active runtime on this thread."
            )
        _, saved_env = stack[-1]
        active_env = get_current_inf_env()
        active_proxy_env = {key: os.environ[key] for key in druntime_env.PROXY_ENV_KEYS if key in os.environ}
        try:
            self._deactivate()
            set_inf_env(saved_env)
        except Exception:
            try:
                set_inf_env(active_env)
            finally:
                for key in druntime_env.PROXY_ENV_KEYS:
                    os.environ.pop(key, None)
                os.environ.update(active_proxy_env)
            raise
        stack.pop()
        if stack:
            # Re-apply the parent runtime's user env that _deactivate cleared.
            stack[-1][0]._reactivate()
        else:
            druntime_env.clear_local_proxy_pool_env()

    def get_remote_cwd(self):
        """Get the current working directory configured for the remote runtime.

        :return: Path to the remote working directory, or None if not set
        :rtype: str or None
        """
        return self._cwd

    def get_remote_python_executable(self):
        """Get the Python executable path configured for the remote runtime.

        :return: Path to the remote Python executable, or None if not set
        :rtype: str or None
        """
        return self._remote_python_executable

    def set_remote_cwd(self, cwd):
        """Set the current working directory to use on the remote runtime.

        Configures the working directory that will be used when launching processes
        on the remote runtime.

        :param cwd: Path to the desired working directory on the remote system
        :type cwd: str
        """
        self._debug("Setting Proxy cwd to %s", cwd)
        self._cwd = cwd
        druntime_env.set_proxy_remote_cwd(cwd)

    def set_remote_python_executable(self, remote_python_executable):
        """Set the Python executable to use on the remote runtime.

        Configures which Python interpreter will be used when launching Python
        processes on the remote runtime.

        :param remote_python_executable: Path to the Python executable on the remote system
        :type remote_python_executable: str
        """
        self._debug("Setting Proxy remote Python executable to %s", remote_python_executable)
        self._remote_python_executable = remote_python_executable
        druntime_env.set_proxy_remote_python_executable(remote_python_executable)

    def set_remote_env(self, remote_env):
        """Set the environment variables to use on the remote runtime.

        Configures the environment that will be used when launching processes
        on the remote runtime.

        :param remote_env: Dictionary of environment variables for the remote runtime
        :type remote_env: dict
        """
        self._debug("Setting Proxy remote environment to %s", remote_env)
        self._env = remote_env
        druntime_env.set_proxy_remote_env(remote_env)

    def get_env(self):
        """Get the remote runtime environment variables.

        :return: Dictionary of environment variables from the remote runtime
        :rtype: dict
        """
        self._debug("Getting Proxy environment")
        return self._env

    # ------------------------------------------------------------------
    # Activation hooks (see Proxy.enable/disable).
    # ------------------------------------------------------------------

    def _activate(self):
        """Switch infra env to this remote runtime and set proxy user env."""
        if not self._gateways_registered:
            dch.register_gateways_from_env()
            self._gateways_registered = True
        set_inf_env(self._env)
        self._set_remote_user_env()

    def _deactivate(self):
        """Clear this remote runtime's proxy user env."""
        self._unset_remote_user_env()

    def _reactivate(self):
        """Re-apply this remote runtime's proxy user env (nested restore)."""
        self._set_remote_user_env()

    def _set_remote_user_env(self):
        """Internal method to configure environment variables for remote execution.

        Sets the necessary environment variables that signal to Dragon infrastructure that proxy mode is enabled and provides configuration for remote execution.
        """
        druntime_env.apply_proxy_user_env(
            self._env,
            cwd=self._cwd,
            remote_python_executable=self._remote_python_executable,
        )

    def _unset_remote_user_env(self):
        """Internal method to remove proxy-related environment variables.

        Cleans up environment variables used for proxy configuration when switching back to local runtime.
        """
        druntime_env.clear_proxy_user_env()
