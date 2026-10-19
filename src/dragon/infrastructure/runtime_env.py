"""Helpers for proxy-aware runtime environment and local pool resolution."""

from __future__ import annotations
from contextlib import contextmanager
import json
import os
import threading

import dragon.infrastructure.facts as dfacts
import dragon.infrastructure.parameters as dparm
import dragon.utils as dutils
from dragon.managed_memory import MemoryPool
from dragon.utils import B64

PROXY_ENABLED_ENV = "DRAGON_PROXY_ENABLED"
PROXY_REMOTE_ENV_ENV = "DRAGON_PROXY_REMOTE_ENV"
PROXY_REMOTE_CWD_ENV = "DRAGON_PROXY_REMOTE_CWD"
PROXY_PYTHON_EXECUTABLE_ENV = "DRAGON_PROXY_PYTHON_EXECUTABLE"
# Full snapshot of the local infrastructure environment captured at proxy enable.
# Used to swap the Global Services control plane back to the local runtime for a
# control-plane operation on a node-local object (e.g. a proxy Queue registering
# its local pool), and to recover the original local runtime's bootstrap route
# keys when building a child environment. Node-local resource descriptors are
# never swapped, so they need no per-descriptor preservation. See CHANNELS_MESH.md.
PROXY_LOCAL_INF_ENV_ENV = "DRAGON_PROXY_LOCAL_INF_ENV"

# Proxy switches the Global Services control plane and DRAGON_RT_UID environment
# for active operations and child environments. The native cached UID continues
# to identify this process's local runtime, which owns node-local resources.
# See CHANNELS_MESH.md, "Future Simplification: Separate The Control Plane From
# Node-Local Resources".
_CONTROL_PLANE_ENV_NAMES = (
    "GS_QD",
    "GS_RET_QD",
)
_NODE_LOCAL_ENV_NAMES = (
    "LOCAL_LS_QD",
    "LS_RET_QD",
    "DEFAULT_PD",
    "INF_PD",
)
_INF_ENV_NAMES = _CONTROL_PLANE_ENV_NAMES + _NODE_LOCAL_ENV_NAMES
_RT_UID_ENV = "DRAGON_RT_UID"
_RT_ROUTE_ENV_PREFIX = "DRAGON_RT_UID__"
_RT_BOOTSTRAP_ENV_SUFFIX = "_BOOTSTRAP_ADDR"
_LOCAL_INF_ENV_LOCK = threading.RLock()

PROXY_USER_ENV_KEYS = frozenset(
    {
        PROXY_ENABLED_ENV,
        PROXY_REMOTE_ENV_ENV,
        PROXY_REMOTE_CWD_ENV,
        PROXY_PYTHON_EXECUTABLE_ENV,
    }
)
PROXY_ENV_KEYS = frozenset(
    {
        PROXY_ENABLED_ENV,
        PROXY_REMOTE_ENV_ENV,
        PROXY_REMOTE_CWD_ENV,
        PROXY_PYTHON_EXECUTABLE_ENV,
        PROXY_LOCAL_INF_ENV_ENV,
    }
)


def _env(environ=None):
    return os.environ if environ is None else environ


def _optional_env(name, environ=None):
    value = _env(environ).get(name)
    if value in (None, "", "None"):
        return None

    return value


def _json_env(name, environ=None):
    value = _optional_env(name, environ)
    if value is None:
        return None

    return json.loads(value)


def _set_optional_env(name, value, environ=None):
    env = _env(environ)
    if value is None:
        env.pop(name, None)
    else:
        env[name] = str(value)


def get_current_inf_env():
    """Return a snapshot of the current runtime's infrastructure environment.

    The snapshot includes the control-plane and node-local descriptors plus the
    active runtime UID environment value so it can be restored after a proxy
    context. Callers that need to operate on a local pool use the snapshot with
    ``using_local_infrastructure``.
    """
    env = _env()
    current_env = {}

    for name in _INF_ENV_NAMES:
        key = dfacts.env_name(name)
        current_env[key] = env[key]

    current_env[_RT_UID_ENV] = env[_RT_UID_ENV]
    return current_env


def set_inf_env(env, *, update_rt_uid=True):
    """Switch the active infrastructure control plane and reconnect GS clients.

    The Global Services descriptors and DRAGON_RT_UID environment value follow
    the active runtime. The native cached UID remains the runtime this process
    belongs to, so node-local resources keep their local owner identity.
    """
    for name in _CONTROL_PLANE_ENV_NAMES:
        key = dfacts.env_name(name)
        os.environ[key] = env[key]

    if update_rt_uid:
        dutils.get_local_rt_uid()
        os.environ[_RT_UID_ENV] = env[_RT_UID_ENV]

    # Imported lazily to avoid a cycle with api_setup -> process -> runtime_env.
    import dragon.globalservices.api_setup as api_setup

    api_setup.connect_to_infrastructure(force=True)


def proxy_enabled(environ=None):
    """Return whether proxy execution is enabled in ``environ``."""
    value = str(_env(environ).get(PROXY_ENABLED_ENV, "")).strip().lower()
    return value not in {"", "0", "false", "no", "off"}


def proxy_remote_env(environ=None):
    """Return the serialized remote runtime environment, if configured."""
    return _json_env(PROXY_REMOTE_ENV_ENV, environ)


def proxy_remote_cwd(environ=None):
    """Return the remote working directory configured for proxy children."""
    return _optional_env(PROXY_REMOTE_CWD_ENV, environ)


def proxy_remote_python_executable(environ=None):
    """Return the Python executable configured for proxy children."""
    return _optional_env(PROXY_PYTHON_EXECUTABLE_ENV, environ)


def proxy_local_infrastructure_env(environ=None):
    """Return the preserved local control-plane environment, if present."""
    return _json_env(PROXY_LOCAL_INF_ENV_ENV, environ)


def set_proxy_remote_env(remote_env, environ=None):
    env = _env(environ)
    if remote_env is None:
        env.pop(PROXY_REMOTE_ENV_ENV, None)
    else:
        env[PROXY_REMOTE_ENV_ENV] = json.dumps(remote_env)


def set_proxy_remote_cwd(cwd, environ=None):
    _set_optional_env(PROXY_REMOTE_CWD_ENV, cwd, environ)


def set_proxy_remote_python_executable(remote_python_executable, environ=None):
    _set_optional_env(PROXY_PYTHON_EXECUTABLE_ENV, remote_python_executable, environ)


def apply_proxy_user_env(remote_env, cwd=None, remote_python_executable=None, environ=None):
    env = _env(environ)
    env[PROXY_ENABLED_ENV] = "1"
    set_proxy_remote_env(remote_env, env)
    set_proxy_remote_cwd(cwd, env)
    set_proxy_remote_python_executable(remote_python_executable, env)


def clear_proxy_user_env(environ=None):
    env = _env(environ)
    for key in PROXY_USER_ENV_KEYS:
        env.pop(key, None)


def strip_proxy_env(env):
    for key in PROXY_ENV_KEYS:
        env.pop(key, None)

    return env


def runtime_route_env_key(rt_uid):
    """Return the environment key for a runtime-local OOB gateway descriptor.

    The descriptor is valid only in the runtime that created the gateway. It
    is therefore used for local OOB bootstrap, not copied into proxy-launched
    children that belong to another runtime.
    """
    # Cross-language contract: this key format (and the _BOOTSTRAP_ADDR suffix
    # below) must stay identical to the C helpers in src/lib/_channels.h
    # (dragon_channel_runtime_route_env_key / _bootstrap_env_key), which parse
    # these same variables from the environment.
    return f"{_RT_ROUTE_ENV_PREFIX}{rt_uid}"


def runtime_bootstrap_env_key(rt_uid):
    """Return the environment key for a runtime's portable ingress address."""
    return f"{runtime_route_env_key(rt_uid)}{_RT_BOOTSTRAP_ENV_SUFFIX}"


def is_runtime_bootstrap_env_key(key):
    """Return whether ``key`` names a portable runtime bootstrap address."""
    return key.startswith(_RT_ROUTE_ENV_PREFIX) and key.endswith(_RT_BOOTSTRAP_ENV_SUFFIX)


def route_bootstrap_env_keys(environ=None):
    """Return non-empty portable bootstrap-address entries from ``environ``."""
    env = _env(environ)
    return {key: value for key, value in env.items() if is_runtime_bootstrap_env_key(key) and value}


def launch_process_env(user_env=None, environ=None):
    """Build the child-process environment for the active runtime context."""
    env = _env(environ)
    if proxy_enabled(env):
        remote_env = proxy_remote_env(env)
        launch_env = dict(remote_env) if remote_env is not None else dict(env)

        # A DRAGON_RT_UID__<rt> gateway descriptor is a runtime-local object and
        # is only valid to attach to in the runtime that created it. So we never
        # copy those descriptors across the runtime boundary into a proxy-launched
        # child. The only cross-runtime routing state a child needs in order to
        # reply to a channel/FLI in another runtime is that runtime's bootstrap
        # address (DRAGON_RT_UID__<rt>_BOOTSTRAP_ADDR); the C channels layer pairs
        # that with one of the child's own local gateways. Preserve bootstrap
        # addresses from both the active env and the original local runtime.
        route_keys = route_bootstrap_env_keys(env)
        local_env = proxy_local_infrastructure_env(env)
        if local_env is not None:
            route_keys.update(route_bootstrap_env_keys(local_env))

        for key, value in route_keys.items():
            launch_env.setdefault(key, value)
    else:
        launch_env = dict(env)

    if user_env is not None:
        launch_env.update(user_env)

    return strip_proxy_env(launch_env)


def is_infrastructure_policy(policy):
    if policy is dparm.POLICY_INFRASTRUCTURE:
        return True

    if policy is None:
        return False

    return (
        getattr(policy, "wait_mode", None) == dparm.POLICY_INFRASTRUCTURE.wait_mode
        and getattr(policy, "return_when", None) == dparm.POLICY_INFRASTRUCTURE.return_when
    )


def local_default_pool_desc(environ=None, local_env=None):
    """Return the node-local default-pool descriptor for the active process."""
    if local_env is not None:
        return local_env[dfacts.env_name("DEFAULT_PD")]

    # The default pool is node-local and never swapped under proxy, so the
    # process parameter already names the local runtime's default pool.
    return getattr(dparm.this_process, "default_pd")


def local_infrastructure_pool_desc(environ=None, local_env=None):
    """Return the node-local infrastructure-pool descriptor for the process."""
    if local_env is not None:
        return local_env[dfacts.env_name("INF_PD")]

    return getattr(dparm.this_process, "inf_pd")


def local_buffer_pool_desc(policy, environ=None, local_env=None):
    """Select the local pool descriptor implied by an infrastructure policy."""
    if is_infrastructure_policy(policy):
        return local_infrastructure_pool_desc(environ=environ, local_env=local_env)

    return local_default_pool_desc(environ=environ, local_env=local_env)


def attach_pool_from_desc(pool_desc):
    """Attach to a managed-memory pool represented by a base64 descriptor."""
    return MemoryPool.attach(B64.str_to_bytes(pool_desc))


def local_infrastructure_for_pool(pool):
    """Return ``(use_local_infrastructure, local_infrastructure_env)`` for a pool.

    A node-local pool means the object lives in the current (local) runtime, so
    under proxy mode its infrastructure operations must be performed against the
    preserved local infrastructure environment rather than the active remote
    one. Non-local pools need no local-infrastructure handling.
    """
    use_local = bool(pool is not None and getattr(pool, "is_local", False))
    local_env = proxy_local_infrastructure_env() if use_local else None
    return use_local, local_env


def attach_local_buffer_pool(policy=None, environ=None, local_env=None):
    return attach_pool_from_desc(local_buffer_pool_desc(policy, environ=environ, local_env=local_env))


def preserve_local_proxy_pool_env(source_env, environ=None):
    # Snapshot the local infrastructure environment so a control-plane operation
    # on a node-local object (e.g. a proxy Queue registering its local pool) can
    # briefly swap the GS control plane back to the local runtime. Node-local
    # descriptors themselves are never swapped, so they do not need per-descriptor
    # preservation.
    env = _env(environ)
    env[PROXY_LOCAL_INF_ENV_ENV] = json.dumps(source_env)


def clear_local_proxy_pool_env(environ=None):
    env = _env(environ)
    env.pop(PROXY_LOCAL_INF_ENV_ENV, None)


@contextmanager
def using_local_infrastructure(environ=None, local_env=None):
    """Temporarily switch GS control-plane access to the preserved local runtime.

    The yielded value is true only when this context changed the active
    control plane. Node-local descriptors are never replaced; the switch is
    needed only for operations on a local object while proxy mode is active.
    """
    env = _env(environ)
    resolved_local_env = local_env

    if resolved_local_env is None:
        resolved_local_env = proxy_local_infrastructure_env(env)
        if not proxy_enabled(env) or resolved_local_env is None:
            yield False
            return

    from dragon.workflows import runtime as druntime

    # Serialize the process-wide switch through the body; do not wait there
    # for another thread that needs this same infrastructure-switch lock.
    with _LOCAL_INF_ENV_LOCK:
        current_env = druntime.get_current_inf_env()
        if current_env == resolved_local_env:
            yield False
            return

        try:
            druntime.set_inf_env(resolved_local_env)
            yield True
        finally:
            druntime.set_inf_env(current_env)


@contextmanager
def using_infrastructure(*, local=False, environ=None, local_env=None):
    """Select local infrastructure switching when ``local`` is requested.

    This small context-manager facade keeps callers independent of whether an
    operation needs the active remote control plane or the preserved local one.
    It yields the same switch-status boolean as ``using_local_infrastructure``.
    """
    if local:
        with using_local_infrastructure(environ=environ, local_env=local_env) as switched:
            yield switched
        return

    yield False
