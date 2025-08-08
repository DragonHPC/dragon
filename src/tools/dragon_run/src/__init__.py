from threading import Thread

from queue import Queue, Empty
from multiprocessing import Event
from typing import List, Dict, Optional

from .remote_executor import FERemoteExecutor
from .dragon_run import DragonRunFE
from .exceptions import DragonRunMissingAllocation, DragonRunSingleNodeUnsupported
from .wlm import WLM, wlm_cls_dict
from .wlm.base import WLMBase
from .facts import DEFAULT_FANOUT


def determine_wlm(
        force_single_node: bool = False,
        force_multi_node: bool = False,
        force_wlm: Optional[WLM] = None
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
        is_slurm = force_wlm == str(WLM.SLURM)
        is_dragon_ssh = force_wlm == str(WLM.DRAGON_SSH)
    else:
        # Likewise, only one of these will be true
        is_slurm = wlm_cls_dict[WLM.SLURM].check_for_wlm_support()
        is_dragon_ssh = wlm_cls_dict[WLM.DRAGON_SSH].check_for_wlm_support()

    # We cannot find a supported WLM
    if is_dragon_ssh + is_slurm == 0:

        # If the user requested multi-node mode, then we have a problem.
        if force_multi_node:
            raise RuntimeError("Cannot determine WLM to use for multi-node mode. Please specify a WLM to use.")

        # Assume we're running single node mode
        return None

    # There is a possibility that we may detect both dragon_ssh and slurm. In this case, we should
    # prefer the dragon_ssh configuration.
    if is_dragon_ssh:
        wlm_cls = wlm_cls_dict[WLM.DRAGON_SSH]
    elif is_slurm:
        wlm_cls = wlm_cls_dict[WLM.SLURM]
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
    *args, **kwargs
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
):
    host_list = get_host_list(
        host_list=host_list,
        force_single_node=force_single_node,
        force_multi_node=force_multi_node,
        force_wlm=force_wlm,
    )

    if not host_list:
        exec_on_fe = True

    with DragonRunFE(host_list, fanout=fanout) as drun:
        drun.run_user_app(
            user_command,
            env,
            cwd,
            exec_on_fe=exec_on_fe
        )
