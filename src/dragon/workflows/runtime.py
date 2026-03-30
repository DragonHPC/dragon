import atexit
import json
import os
import pathlib
import socket
import subprocess
import time
import logging
import sys
import dragon
import multiprocessing as mp
from dragon.native.process import Process

import dragon.infrastructure.messages as dmsg
import dragon.infrastructure.facts as dfacts
import dragon.infrastructure.util as dutil
import dragon.globalservices.api_setup as api_setup
import dragon.transport.oob as doob
import dragon.utils as dutils

LOGGER = logging.getLogger("dragon.workflows.runtime")

def get_logs(name):
    global LOGGER
    log = LOGGER.getChild(name)
    return log.debug, log.info

_current_rt_uid = None
already_published = {}
sdesc_by_system_and_name = {}
runtime_table = {}
oob_net_list = []
must_register_teardown = True


def proxy_teardown():
    LOGGER.debug("Tearing down runtime proxies")
    global already_published
    for publish_path in already_published:
        os.remove(publish_path)

    runtime_table.clear()
    oob_net_list.clear()


def free_port(host, port):
    LOGGER.debug("Checking if port %d is free on host %s", port, host)

    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((host, int(port)))
        s.shutdown(socket.SHUT_WR)
        s.close()
        return False
    except ConnectionRefusedError:
        return True


def get_port(host, start_port, end_port):
    LOGGER.debug("Getting free port on host %s in range %d-%d", host, start_port, end_port)

    for port in range(start_port, end_port + 1):
        if free_port(host, port):
            return port

    raise RuntimeError("No available ports")


def get_ip():
    LOGGER.debug("Getting IP address for host %s", host)

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.settimeout(0)
    try:
        # doesn't even have to be reachable
        s.connect(("10.254.254.254", 1))
        IP = s.getsockname()[0]
    except Exception:
        IP = "127.0.0.1"
    finally:
        s.close()
    return IP


def get_current_inf_env():
    LOGGER.debug("Getting current infrastructure environment")
    rv = {}
    gs_cd_key = dfacts.env_name("GS_CD")
    gs_ret_cd_key = dfacts.env_name("GS_RET_CD")
    ls_cd_key = dfacts.env_name("LOCAL_SHEP_CD")
    ls_ret_cd_key = dfacts.env_name("SHEP_RET_CD")
    rv[gs_cd_key] = os.environ[gs_cd_key]
    rv[gs_ret_cd_key] = os.environ[gs_ret_cd_key]
    rv[ls_cd_key] = os.environ[ls_cd_key]
    rv[ls_ret_cd_key] = os.environ[ls_ret_cd_key]
    # get runtime ip addrs
    rt_uid_key = "DRAGON_RT_UID"
    rv[rt_uid_key] = os.environ[rt_uid_key]
    return rv


def set_inf_env(env):
    LOGGER.debug("Setting infrastructure environment")
    gs_cd_key = dfacts.env_name("GS_CD")
    gs_ret_cd_key = dfacts.env_name("GS_RET_CD")
    ls_cd_key = dfacts.env_name("LOCAL_SHEP_CD")
    ls_ret_cd_key = dfacts.env_name("SHEP_RET_CD")
    os.environ[gs_cd_key] = env[gs_cd_key]
    os.environ[gs_ret_cd_key] = env[gs_ret_cd_key]
    os.environ[ls_cd_key] = env[ls_cd_key]
    os.environ[ls_ret_cd_key] = env[ls_ret_cd_key]
    # set runtime ip addrs
    rt_uid_key = "DRAGON_RT_UID"
    os.environ[rt_uid_key] = env[rt_uid_key]

    global _current_rt_uid
    _current_rt_uid = env[rt_uid_key]

    api_setup.connect_to_infrastructure(force=True)


@property
def current_rt_uid():
    LOGGER.debug("Getting current runtime UID")

    global _current_rt_uid
    if _current_rt_uid is None:
        return dutils.get_local_rt_uid()
    else:
        return _current_rt_uid


class Proxy:
    """Object providing API to manage Dragon runtime connections for remote execution.

    The Proxy class enables seamless switching between local and remote Dragon runtime environments. It manages infrastructure connections and environment variables to allow processes and resources created in one runtime to be accessed from another.

    A typical workflow using Proxy may look like:

    .. highlight:: python
    .. code-block:: python

        import dragon.workflows.runtime as runtime

        def howdy(q):
            q.put(
                f"howdy from {socket.gethostname()} - local num cores is {os.cpu_count()}, "
                f"runtime available cores is {mp.cpu_count()}"
            )

        def signal_exit(exit_path):
            file = open(exit_path, "w")
            file.close()

        def shutdown_remote_runtime(exit_path, remote_working_dir):
            exit_proc = Process(target=signal_exit, args=(exit_path,), cwd=remote_working_dir)
            exit_proc.start()
            exit_proc.join()

        if __name__ == '__main__':
            # Set up paths for remote runtime discovery

            system = "my.remote.system"
            runtime_name = "proxy_runtime"
            publish_dir = "/my/remote/publish/dir"
            exit_path = "/my/remote/publish/dir/exit_client"

            # Configure Dragon to use multiprocessing API
            mp.set_start_method("dragon")

            # Look up and attach to remote runtime
            runtime_sdesc = runtime.lookup(system, runtime_name, 30, publish_dir=publish_dir)
            proxy = runtime.attach(runtime_sdesc, remote_cwd=remote_working_dir)

            # Enable proxy to create queue in remote runtime
            proxy.enable()
            q = mp.Queue()

            # Launch processes in remote runtime
            print("Launching remote runtime processes...", flush=True)
            procs = []
            for _ in range(2):
                p = Process(target=howdy, args=(q,))
                p.start()
                procs.append(p)


            # Wait for all processes to complete.
            # The queue must have a maxsize greater than the number of processes for this ordering to work.
            for p in procs:
                p.join()

            proxy.disable()

            # Collect results from remote processes
            # The remote q can still be accessed outside the proxy environment
            for p in procs:
                msg = q.get()
                print(f"Message from remote runtime: {msg}", flush=True)

            # Clean up resources
            for p in procs:
                del p
            del q

            # signal client's exit
            proxy.enable()
            shutdown_remote_runtime(exit_path, remote_working_dir)
            proxy.disable()
    """

    def __init__(self, sdesc, oob_net, cwd=None, remote_python_executable=None):
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
        self._original_env = None
        self._oob_net = oob_net
        self._debug, self._info = get_logs("Proxy")
        self._cwd = cwd
        self._remote_python_executable = remote_python_executable

    def __enter__(self):
        """Enter the Proxy context manager, enabling remote runtime connections.

        Reconfigures infrastructure connections to point to the remote runtime
        when entering a 'with' statement context.

        :return: The Proxy instance
        :rtype: Proxy
        """
        # Reconfigure infrastructure connections
        self._debug("Entering Proxy context manager")
        self.enable()
        # I think we do want to return the object here since there are useful methods on the object
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Exit the Proxy context manager, restoring original runtime connections.

        Restores the original infrastructure connections when exiting a 'with'
        statement context.

        :param exc_type: Exception type if an exception occurred
        :param exc_value: Exception value if an exception occurred
        :param traceback: Exception traceback if an exception occurred
        """
        self._debug("Exiting Proxy context manager")
        # Restore the original infrastructure connections
        self.disable()

    def enable(self):
        """Enable the proxy by switching to remote runtime environment.

        Saves the current local environment and reconfigures the Dragon infrastructure
        to use the remote runtime's channels and environment variables. This allows subsequent Dragon operations to target the remote runtime.
        """
        self._debug("Enabling Proxy")
        self._original_env = get_current_inf_env()
        set_inf_env(self._env)
        self._set_remote_user_env()

    def disable(self):
        """Disable the proxy by restoring local runtime environment.

        Restores the original local infrastructure connections and environment
        variables, switching back from remote to local runtime.
        """
        self._debug("Disabling Proxy")
        set_inf_env(self._original_env)
        self._unset_remote_user_env()

    def get_env(self):
        """Get the remote runtime environment variables.

        :return: Dictionary of environment variables from the remote runtime
        :rtype: dict
        """
        self._debug("Getting Proxy environment")
        return self._env

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
        os.environ["DRAGON_PROXY_REMOTE_CWD"] = str(cwd)

    def set_remote_python_executable(self, remote_python_executable):
        """Set the Python executable to use on the remote runtime.

        Configures which Python interpreter will be used when launching Python
        processes on the remote runtime.

        :param remote_python_executable: Path to the Python executable on the remote system
        :type remote_python_executable: str
        """
        self._debug("Setting Proxy remote Python executable to %s", remote_python_executable)
        self._remote_python_executable = remote_python_executable
        os.environ["DRAGON_PROXY_PYTHON_EXECUTABLE"] = str(remote_python_executable)

    def set_remote_env(self, remote_env):
        """Set the environment variables to use on the remote runtime.

        Configures the environment that will be used when launching processes
        on the remote runtime.

        :param remote_env: Dictionary of environment variables for the remote runtime
        :type remote_env: dict
        """
        self._debug("Setting Proxy remote environment to %s", remote_env)
        self._env = remote_env
        os.environ["DRAGON_PROXY_REMOTE_ENV"] = json.dumps(remote_env)

    def _set_remote_user_env(self):
        """Internal method to configure environment variables for remote execution.

        Sets the necessary environment variables that signal to Dragon infrastructure that proxy mode is enabled and provides configuration for remote execution.
        """
        os.environ["DRAGON_PROXY_ENABLED"] = "1"
        self.set_remote_env(self._env)
        self.set_remote_cwd(self._cwd)
        self.set_remote_python_executable(self._remote_python_executable)

    def _unset_remote_user_env(self):
        """Internal method to remove proxy-related environment variables.

        Cleans up environment variables used for proxy configuration when switching back to local runtime.
        """
        try:
            del os.environ["DRAGON_PROXY_ENABLED"]
        except KeyError:
            pass
        try:
            del os.environ["DRAGON_PROXY_REMOTE_ENV"]
        except KeyError:
            pass
        try:
            del os.environ["DRAGON_PROXY_REMOTE_CWD"]
        except KeyError:
            pass
        try:
            del os.environ["DRAGON_PROXY_PYTHON_EXECUTABLE"]
        except KeyError:
            pass


def get_sdesc():

    LOGGER.debug("Getting serialized runtime descriptor")
    gs_cd = os.environ[dfacts.env_name(dfacts.GS_CD)]
    gs_ret_cd = os.environ[dfacts.env_name(dfacts.GS_RET_CD)]
    ls_cd = os.environ[dfacts.env_name(dfacts.LOCAL_SHEP_CD)]
    ls_ret_cd = os.environ[dfacts.env_name(dfacts.SHEP_RET_CD)]

    fe_ext_ip_addr = os.environ["DRAGON_FE_EXTERNAL_IP_ADDR"]
    head_node_ip_addr = os.environ["DRAGON_HEAD_NODE_IP_ADDR"]
    oob_port = dfacts.OOB_PORT
    python_path = sys.executable
    sdesc = dmsg.RuntimeDesc(
        0, gs_cd, gs_ret_cd, ls_cd, ls_ret_cd, fe_ext_ip_addr, head_node_ip_addr, oob_port, python_path, os.environ
    )
    return sdesc.serialize()


def publish(name, publish_dir=None):
    """Publish the current runtime descriptor to make it discoverable by other systems.

    This function creates and publishes a serialized runtime descriptor that contains all necessary information for remote systems to connect to the current Dragon runtime. The descriptor is written to a file in the specified directory and an out-of-band network listener is started to accept incoming connections.

    The published descriptor can later be retrieved by other systems using the lookup() function to establish remote execution capabilities.

    :param name: Identifier name for the published runtime. This will be used as the filename
    :type name: str
    :param publish_dir: Directory where the descriptor file will be written. If None, uses ~/.dragon, defaults to None
    :type publish_dir: str, optional
    :return: Serialized runtime descriptor string
    :rtype: str
    :raises RuntimeError: If the runtime has already been published with the given name
    """
    LOGGER.debug("Publishing runtime descriptor with name: %s", name)
    sdesc_str = get_sdesc()

    global already_published
    if name in already_published:
        return get_sdesc()

    if publish_dir is not None:
        dragon_dir = publish_dir
    else:
        home_dir = pathlib.Path.home()
        dragon_dir = home_dir / ".dragon"

    publish_path = f"{dragon_dir}/{name}"
    pathlib.Path(dragon_dir).mkdir(parents=True, exist_ok=True)

    try:
        with open(publish_path, "w") as publish_file:
            publish_file.write(sdesc_str)

        already_published[publish_path] = True

        global oob_net_list
        oob_net = doob.OutOfBand()
        oob_net.accept(dfacts.OOB_PORT)
        oob_net_list.append(oob_net)

        global must_register_teardown
        if must_register_teardown:
            atexit.register(proxy_teardown)
            must_register_teardown = False
    except:
        raise RuntimeError(f"publish failed: {name} has already been published")

    return sdesc_str


def lookup(system, name, timeout_in=None, publish_dir=None):
    """Look up and retrieve a published runtime descriptor from a remote system.

    This function attempts to retrieve a serialized runtime descriptor that was previously published on a remote system using the publish() function. It uses SCP to copy the descriptor file from the remote system to the local machine.

    The function will retry the lookup operation until the specified timeout is reached, sleeping 1 second between attempts. This allows time for the descriptor file to become visible on distributed/parallel file systems.

    :param system: Hostname or IP address of the remote system where the runtime was published
    :type system: str
    :param name: Name identifier used when the runtime descriptor was published
    :type name: str
    :param timeout_in: Maximum time in seconds to wait for the descriptor file, defaults to 1
    :type timeout_in: int, optional
    :param publish_dir: Directory path where the descriptor was published. If None, uses ~/.dragon, defaults to None
    :type publish_dir: str, optional
    :return: Serialized runtime descriptor string
    :rtype: str
    :raises RuntimeError: If the descriptor could not be retrieved within the timeout period
    """
    LOGGER.debug("Looking up runtime descriptor for system: %s, name: %s", system, name)
    global sdesc_by_system_and_name

    # if we've already looked up this system/name combination, just grab
    # the sdesc from our local dict
    if (system, name) in sdesc_by_system_and_name:
        return sdesc_by_system_and_name[(system, name)]

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
            sdesc_by_system_and_name[(system, name)] = sdesc
            os.remove(name)
            return sdesc
        else:
            # do ls on remote .dragon directory to force visibility of
            # newly created files on the parallel file system
            os.system(f'ssh {system} "ls {publish_path}" > /dev/null 2>&1')
            time.sleep(1)
            time_so_far += 1

    raise RuntimeError(f"lookup failed: could not obtain serialized descriptor for {system=} and {name=}")


def attach(sdesc_str, oob_ssh_tunnel_override=None, remote_cwd=None):
    """Attach to a remote Dragon runtime using a serialized descriptor.

    This function establishes a connection to a remote Dragon runtime by parsing the provided serialized descriptor and setting up an out-of-band network tunnel. It creates and returns a Proxy object that can be used to switch between local and remote runtime environments.

    The returned Proxy object allows processes and Dragon resources created locally to interact with the remote runtime, enabling distributed execution across systems.

    :param sdesc_str: Serialized runtime descriptor string obtained from lookup()
    :type sdesc_str: str
    :param oob_ssh_tunnel_override: Optional override for SSH tunnel configuration, defaults to None
    :type oob_ssh_tunnel_override: str, optional
    :param remote_cwd: Current working directory to use on the remote runtime, defaults to None
    :type remote_cwd: str, optional
    :return: Proxy object for managing the remote runtime connection
    :rtype: Proxy
    """
    LOGGER.debug("Attaching to remote runtime with sdesc: %s", sdesc_str)
    sdesc = dmsg.parse(sdesc_str)

    jump_host = sdesc.fe_ext_ip_addr
    compute_node = sdesc.head_node_ip_addr
    tunnel_port = sdesc.oob_port
    remote_python_executable = sdesc.python_path

    oob_net = doob.OutOfBand(oob_ssh_tunnel_override=oob_ssh_tunnel_override)
    oob_net.connect(jump_host, compute_node, int(tunnel_port))

    global must_register_teardown

    if must_register_teardown:
        atexit.register(proxy_teardown)
        must_register_teardown = False

    proxy = Proxy(sdesc, oob_net, remote_cwd, remote_python_executable)
    remote_rt_uid = dutil.rt_uid_from_ip_addrs(sdesc.fe_ext_ip_addr, sdesc.head_node_ip_addr)
    runtime_table[remote_rt_uid] = proxy

    return proxy
