import shutil
import subprocess
import selectors
import json
from itertools import groupby
from io import BufferedIOBase, TextIOBase
from os import environ, getcwd
from shlex import quote
from typing import Dict, List

from ...infrastructure.node_desc import NodeDescriptor
from ...infrastructure.parameters import this_process
from ...infrastructure.config import dragon_config
from .base import BaseNetworkConfig

SSH_OPTIONS = 'ssh-options'

# TODO: we're propagating environment variables to make sure
#       the SSH envionment has the same envrionment as the
#       frontend. This works if there's a shared filesystem
#       or a homogeneous (duplicated) filesystem.  It can
#       also work through thoughtful manipulation of env vars
#       such as PATH on the head node, but this requires
#       documentation/explanation for users.  Ultimately we
#       wish to propagate Dragon out absent a shared filesystem.
ENV_VARS = None
BASE_ENV_VARNAMES = (
    "DRAGON_BASE_DIR",
    "DRAGON_VERSION",
    "DRAGON_HSTA_DEBUG",
    "PATH",
    "PYTHONPATH",
    "LD_LIBRARY_PATH",
    "PYTHONSTARTUP",
    "VIRTUAL_ENV",
    "DRAGON_FE_EXTERNAL_IP_ADDR",
    "DRAGON_HEAD_NODE_IP_ADDR",
    "DRAGON_RT_UID",
)


def get_ssh_env_vars(args_map=None, propagate_base=BASE_ENV_VARNAMES) -> None:

    global ENV_VARS
    ENV_VARS = [f'{varname}={environ.get(varname, "")}' for varname in propagate_base]

    # Make sure we propogate the device logging
    try:
        local_env = [f"{log_device}={log_level}" for log_device, log_level in args_map["log_device_level_map"].items()]
    except (KeyError, TypeError):
        local_env = []
    ENV_VARS.extend(local_env)

    # Construct a list of all the environment variables
    this_process_envs = [f"{var}={quote(val)}" for var, val in this_process.env().items()]
    ENV_VARS.extend(this_process_envs)


def get_ssh_launch_be_args(args_map: Dict, launch_args: Dict) -> List:

    global ENV_VARS
    if ENV_VARS is None or args_map is not None:
        get_ssh_env_vars(args_map=args_map)

    bash_cmd = f"cd {getcwd()} && {' '.join(ENV_VARS + launch_args)}"

    ssh_cmd = ["ssh", "-oBatchMode=yes"]
    cfg_dict = dragon_config()
    if SSH_OPTIONS in cfg_dict:
        ssh_cmd.append(cfg_dict[SSH_OPTIONS])

    ssh_cmd.extend([args_map["hostname"], bash_cmd])

    return ssh_cmd


class SSHIOStream(BufferedIOBase):

    def __init__(self, streams: list[(int, TextIOBase)]):

        self._inputs = streams
        self.selectors = {}

    def _iterative_poll(self, idx, stream, timeout=0):
        """Test if there's a read event in the input stream"""

        try:
            sel = self.selectors[idx]
        except (AttributeError, KeyError):
            sel = self.selectors[idx] = selectors.DefaultSelector()
            sel.register(stream, selectors.EVENT_READ)
        events = sel.select(timeout)
        for key, mask in events:
            if key.fileobj is not stream:
                continue
            if mask & selectors.EVENT_READ:
                return True
        return False

    def poll(self, timeout=0):
        """Loop through our streams testing for a read event

        Return as soon as an event is found with True. False otherwise
        """
        for idx, stream in self._inputs:
            if self._iterative_poll(idx, stream, timeout=timeout):
                return True
        return False

    def readline(self, **kwargs):
        """Find a line to read and return it, prefixed with an index"""
        for idx, stream in self._inputs:
            if self._iterative_poll(idx, stream):
                msg = stream.readline()

                # See if this gets parsed into a Node Descriptor. If so,
                # remove it from the stream from our monitoring list:
                try:
                    NodeDescriptor.from_sdict(json.loads(msg))
                    self._inputs.remove((idx, stream))
                except (TypeError, json.decoder.JSONDecodeError):
                    pass

                if isinstance(msg, str):
                    msg = f"{idx}: {msg}"
                else:
                    msg = f"{idx}: {msg.decode()}"

                return msg

    def close(self):
        """Close the streams"""
        for _, stream in self._inputs:
            try:
                stream.close()
            except Exception:
                pass

    def write(self, b):
        for _, stream in self._inputs:
            stream.write(b)

    def flush(self):
        for _, stream in self._inputs:
            stream.flush()


class SSHSubprocessPopen:
    """A convenience class providing Popen methods and attributes on a list of Popen objects"""

    def __init__(self, popen_dict: dict[str, subprocess.Popen]):

        self.procs = popen_dict
        self.nprocs = len(self.procs)
        self.args = [proc.args for proc in self.procs.values()]
        self.pid = [proc.pid for proc in self.procs.values()]

        self._returncode = None
        self._ret_codes = []

        # Set up newlinestreamwrappers for each of the subprocess stdout/stderr
        self._stdouts = [(i, proc.stdout) for i, proc in enumerate(self.procs.values()) if proc.stdout is not None]
        self._stderrs = [(i, proc.stderr) for i, proc in enumerate(self.procs.values()) if proc.stderr is not None]
        self._stdins = [(i, proc.stdin) for i, proc in enumerate(self.procs.values()) if proc.stdin is not None]

        self.stdout = None
        self.stderr = None
        self.stdin = None
        if len(self._stdouts) > 0:
            self.stdout = SSHIOStream(self._stdouts)

        if len(self._stderrs) > 0:
            self.stderr = SSHIOStream(self._stderrs)

        if len(self._stdins) > 0:
            self.stdin = SSHIOStream(self._stdins)

    def poll(self):
        """Check exit status on all Popen objects

        If not all procs have exited, return None. Otherwise returncode
        """
        for proc in self.procs.values():
            if proc.poll() is not None:
                self._ret_codes.append(proc.returncode)

        if len(self._ret_codes) == self.nprocs:
            return self.returncode
        else:
            return None

    def wait(self, timeout=None):
        """Wait on exit for all Popen objects"""

        for proc in self.procs.values():
            # TODO: We don't want to necessarily wait the full timeout
            #       for each Popen object. Ideally, we'd aggregate this
            #       timeout over multiple calls to wait
            if proc.wait(timeout=timeout) is not None:
                self._ret_codes.append(proc.returncode)

        if len(self._ret_codes) == self.nprocs:
            return self.returncode
        else:
            return None

    def terminate(self):
        """Send SIGTERM to all Popen objects"""
        for proc in self.procs.values():
            proc.terminate()

    def kill(self):
        """Send SIGKILL to all Popen objects"""
        for proc in self.procs.values():
            proc.kill()

    def send_signal(self, signal):
        """Send signal to all Popen objects"""
        for proc in self.procs.values():
            proc.send_signal(signal)

    def communicate(self, input=None, timeout=None):
        """Send data to stdin or recv data from stdout,stderr of all Popen objects"""
        if input is not None:
            self.stdin.write(input)

        out_msg = None
        err_msg = None
        if self.stderr is not None:
            err_msg = self.stderr.readline()

        if self.stdout is not None:
            out_msg = self.stdout.readline()

        return out_msg, err_msg

    @property
    def returncode(self):
        """Return return codes for Popen objects"""

        if isinstance(self._ret_codes, list) and len(self._ret_codes):
            code_group = groupby(self._ret_codes)
            # If they're all the same, return any one of them
            if next(code_group, True) and not next(code_group, False):
                self._returncode = self._ret_codes[0]
            # if not, return the first non-zero:
            else:
                self._returncode = next((code for code in self._ret_codes if code != 0), 0)

        return self._returncode


class SSHNetworkConfig(BaseNetworkConfig):

    def __init__(self, network_prefix, port, hostlist):

        super().__init__("ssh", network_prefix, port, len(hostlist))
        self.hostlist = hostlist

    @classmethod
    def check_for_wlm_support(cls) -> bool:
        return shutil.which("ssh") is not None

    @classmethod
    def check_for_allocation(cls) -> bool:
        return True

    def _get_wlm_job_id(self) -> str:
        raise RuntimeError("SSHNetworkConfig does not implement _get_wlm_job_id")

    def _supports_net_conf_cache(self) -> bool:
        return False

    def _launch_network_config_helper(self) -> subprocess.Popen:
        popen_dict = {}

        for host in self.hostlist:
            args_map = {"hostname": host}
            ssh_args = get_ssh_launch_be_args(args_map=args_map, launch_args=self.NETWORK_CFG_HELPER_LAUNCH_SHELL_CMD)
            self.LOGGER.debug(f"Launching config with: {ssh_args}")

            popen_dict[host] = subprocess.Popen(
                args=ssh_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=0, start_new_session=True
            )

        return SSHSubprocessPopen(popen_dict)
