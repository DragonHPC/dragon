import os
import logging
from typing import List, Dict, Optional

import threading

from .dragon_run import DragonRunFE
from .exceptions import DragonRunMissingAllocation
from .wlm import WLM, wlm_cls_dict
from .wlm.base import WLMBase
from .facts import DEFAULT_FANOUT

logger = logging.getLogger(__name__)


def determine_wlm(
    force_single_node: bool = False,
    force_multi_node: bool = False,
    force_wlm: Optional[WLM] = None,
) -> Optional[WLMBase]:
    """
    Determine if dragon_run should be started in multi-node or single-node mode.

    Returns a tuple (multi_node, wlm_cls).

    multi_node:
        False if dragon_run should be started in single-node mode
        True if dragon_run should be started in multi-node mode

    wlm_cls:
        If multi_node is True, a reference to the WLM class object that
        can be used to get the list of nodes in the current allocation.

    Raises:
        ValueError, RuntimeError
    """

    # Verify that, if the user  passed a WLM, its a valid option.
    if force_wlm and force_wlm not in wlm_cls_dict:
        raise RuntimeError(f"The requsted wlm {force_wlm} is not supported. Please specify a valid wlm.")

    if force_single_node and force_multi_node:
        msg = "Cannot request both single node and multi-node simultaneously."
        raise ValueError(msg)

    if force_single_node and force_wlm:
        msg = "Cannot request single node deployment of Dragon and specify a workload manager."
        raise ValueError(msg)

    if force_single_node:
        return None

    if force_multi_node and force_wlm:
        return wlm_cls_dict[force_wlm]

    # Try to determine if we're on a supported multinode system
    if force_wlm != None:
        # Hopefully only one of these will be true
        is_slurm = force_wlm == WLM.SLURM
        is_pbs = force_wlm == WLM.PBS
        is_dragon_ssh = force_wlm == WLM.DRAGON_SSH
    else:
        # Likewise, only one of these will be true
        is_slurm = wlm_cls_dict[WLM.SLURM].check_for_wlm_support()
        is_pbs = wlm_cls_dict[WLM.PBS].check_for_wlm_support()
        is_dragon_ssh = wlm_cls_dict[WLM.DRAGON_SSH].check_for_wlm_support()

    # We cannot find a supported WLM
    if is_dragon_ssh + is_slurm + is_pbs == 0:

        # If the user requested multi-node mode, then we have a problem.
        if force_multi_node:
            raise RuntimeError("Cannot determine WLM to use for multi-node mode. Please specify a WLM to use.")

        # Assume we're running single node mode
        return None

    # There is a possibility that we may detect more than one WLM. In this case, we should
    # prefer the dragon_ssh configuration.
    if is_dragon_ssh:
        wlm_cls = wlm_cls_dict[WLM.DRAGON_SSH]
    elif is_slurm:
        wlm_cls = wlm_cls_dict[WLM.SLURM]
    elif is_pbs:
        wlm_cls = wlm_cls_dict[WLM.PBS]
    else:
        raise RuntimeError("Error: Unable to get WLM class object.")

    if not wlm_cls.check_for_allocation():
        msg = f"Executing in a {wlm_cls.NAME} environment, but cannot detect any active jobs or allocated nodes."
        raise DragonRunMissingAllocation(msg)

    return wlm_cls


def get_host_list(
    host_list: Optional[List[str]] = None,
    force_single_node: bool = False,
    force_multi_node: bool = False,
    force_wlm: Optional[WLM] = None,
    *args,
    **kwargs,
) -> Optional[List[str]]:
    # If we're being provided with a host_list, then just use those, otherwise figure out our host_list
    if not host_list:
        # Try to determine the wlm and gather our host-list that way.
        if cls := determine_wlm(
            force_single_node=force_single_node,
            force_multi_node=force_multi_node,
            force_wlm=force_wlm,
        ):
            host_list = cls.get_host_list()
    return host_list


PIPE = -1
STDOUT = -2
DEVNULL = -3


class DragonRunPopen:
    """
    A Popen-like object to wrap DragonRun.
    """

    def __init__(
        self,
        user_command,
        cwd: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
        host_list: Optional[List[str]] = None,
        force_single_node: bool = False,
        force_multi_node: bool = False,
        force_wlm: Optional[WLM] = None,
        exec_on_fe: bool = False,
        fanout: int = DEFAULT_FANOUT,
        log_level: int = logging.NOTSET,
        stdout: Optional[int] = None,
        stderr: Optional[int] = None,
        stdin: Optional[int] = None,
    ):
        self.user_command = user_command
        self.cwd = cwd
        self.env = env
        self.host_list = host_list
        self.force_single_node = force_single_node
        self.force_multi_node = force_multi_node
        self.force_wlm = force_wlm
        self.fanout = fanout
        self.log_level = log_level
        self._stdout: Optional[int] = stdout
        self._stderr: Optional[int] = stderr
        self._stdin: Optional[int] = stdin

        self.exec_on_fe = exec_on_fe
        if not self.host_list:
            self.exec_on_fe = True

        if self._stdout == None:
            self._stdout_rd = None
            self._stdout_wd = None
            self._stdout_rh = None
            self._stdout_wh = None
        elif self._stdout == PIPE:
            self._stdout_rd, self._stdout_wd = os.pipe()
            self._stdout_rh = os.fdopen(self._stdout_rd, "r")
            self._stdout_wh = os.fdopen(self._stdout_wd, "w")
        elif self._stdout == DEVNULL:
            self._stdout_rd = None
            self._stdout_wd = None
            self._stdout_rh = open(os.devnull, "r")
            self._stdout_wh = open(os.devnull, "w")
        else:
            raise NotImplementedError("DragonRunPopen only supports stdout=PIPE, DEVNULL, or None.")

        if self._stderr == None:
            self._sterr_rd = None
            self._stderr_wd = None
            self._stderr_rh = None
            self._stderr_wh = None
        elif self._stderr == PIPE:
            self._stderr_rd, self._stderr_wd = os.pipe()
            self._stderr_rh = os.fdopen(self._stderr_rd, "r")
            self._stderr_wh = os.fdopen(self._stderr_wd, "w")
        elif self._stderr == DEVNULL:
            self._stderr_rd = None
            self._stderr_wd = None
            self._stderr_rh = open(os.devnull, "r")
            self._stderr_wh = open(os.devnull, "w")
        else:
            raise NotImplementedError("DragonRunPopen only supports stderr=PIPE, DEVNULL, or None.")

        if self._stdin != None:
            raise NotImplementedError("DragonRunPopen does not support stdin redirection.")

        self._drun_thread: threading.Thread = threading.Thread(target=self._drun_proc, name="DragonRunMainThread")
        self._drun_thread.start()

    @property
    def stdout(self):
        """
        Mimics Popen.stdout to provide access to the subprocess's standard output.
        """
        return self._stdout_rh

    @property
    def stderr(self):
        """
        Mimics Popen.stderr to provide access to the subprocess's standard error.
        """
        return self._stderr_rh

    @property
    def stdin(self):
        """
        Mimics Popen.stdin to provide access to the subprocess's standard input.
        """
        raise NotImplementedError("DragonRunPopen does not support stdin property.")

    def communicate(self, input=None):
        """
        Mimics Popen.communicate() to interact with the subprocess.
        """
        raise NotImplementedError("DragonRunPopen does not support communicate() method.")

    def poll(self):
        """
        Mimics Popen.poll() to check if the process has terminated.
        """
        if self._drun_thread and self._drun_thread.is_alive():
            return None
        return self.returncode

    def wait(self, timeout=None):
        """
        Mimics Popen.wait() to wait for the process to terminate.
        """
        if self._drun_thread:
            self._drun_thread.join(timeout=timeout)

    @property
    def returncode(self):
        """
        Returns the return code of the subprocess.
        """
        return 0

    def send_signal(self, signal):
        """
        Mimics Popen.send_signal() to send a signal to the subprocess.
        """
        raise NotImplementedError("DragonRunPopen does not support send_signal() method.")

    def terminate(self):
        """
        Mimics Popen.terminate() to terminate the subprocess.
        """
        raise NotImplementedError("DragonRunPopen does not support terminate() method.")

    def kill(self):
        """
        Mimics Popen.kill() to kill the subprocess.
        """
        raise NotImplementedError("DragonRunPopen does not support kill() method.")

    @property
    def args(self):
        """
        Mimics Popen.args to provide access to the subprocess's arguments.
        """
        return self.user_command

    @property
    def pid(self):
        """
        Mimics Popen.pid to provide access to the subprocess's process ID.
        """
        raise NotImplementedError("DragonRunPopen does not support pid property.")

    def _drun_proc(self):
        """
        The main thread that runs the DragonRunFE and captures output.
        """
        logger.debug("++_drun_proc")
        try:
            with DragonRunFE(
                children=self.host_list,
                fanout=self.fanout,
                log_level=self.log_level,
                stdout_wh=self._stdout_wh,
                stderr_wh=self._stderr_wh,
            ) as drun:
                drun.run_user_app(
                    self.user_command,
                    self.env,
                    self.cwd,
                    exec_on_fe=self.exec_on_fe,
                )
        finally:
            logger.debug("--_drun_proc")


def run_wrapper(
    user_command,
    cwd: Optional[str] = None,
    env: Optional[Dict[str, str]] = None,
    host_list: Optional[List[str]] = None,
    force_single_node: bool = False,
    force_multi_node: bool = False,
    force_wlm: Optional[WLM] = None,
    exec_on_fe: bool = False,
    fanout: int = DEFAULT_FANOUT,
    log_level: int = logging.NOTSET,
):
    host_list = get_host_list(
        host_list=host_list,
        force_single_node=force_single_node,
        force_multi_node=force_multi_node,
        force_wlm=force_wlm,
    )

    if not host_list:
        exec_on_fe = True

    drun_h = DragonRunPopen(
        user_command=user_command,
        cwd=cwd,
        env=env,
        host_list=host_list,
        force_single_node=force_single_node,
        force_multi_node=force_multi_node,
        force_wlm=force_wlm,
        exec_on_fe=exec_on_fe,
        fanout=fanout,
        log_level=log_level,
        stdout=None,
        stderr=None,
        stdin=None,
    )

    drun_h.wait()
