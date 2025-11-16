import os
import subprocess

from .base import BaseWLM
from dragon.tools.dragon_run.src import DragonRunPopen, PIPE
from typing import Optional


class DRunWLM(BaseWLM):

    def __init__(self, network_prefix, port, hostlist):
        nhosts = len(hostlist) if hostlist is not None else 0
        super().__init__("drun", network_prefix, port, nhosts)
        self.hostlist = hostlist

    @classmethod
    def check_for_wlm_support(cls) -> bool:
        return True

    @classmethod
    def check_for_allocation(cls) -> bool:
        return True

    def _get_wlm_job_id(self) -> str:
        raise RuntimeError("DRunNetworkConfig does not implement _get_wlm_job_id")

    def _supports_net_conf_cache(self) -> bool:
        return False

    def _get_wlm_launch_be_args(self, args_map: dict, launch_args: list):
        """
        Abstract method to return WLM specific command line arguments
        to use to launch the backend process.
        """
        raise RuntimeError("DRunNetworkConfig does not implement _get_wlm_launch_be_args")

    def _launch_network_config_helper(self) -> subprocess.Popen:
        if not self.hostlist:
            raise RuntimeError("DRunWLM requires a non-empty hostlist.")

        network_config_helper_cmd = self.NETWORK_CFG_HELPER_LAUNCH_CMD
        self.LOGGER.debug(f"Launching config with: {network_config_helper_cmd=}")
        return DragonRunPopen(
            user_command=network_config_helper_cmd,
            host_list=self.hostlist,
            stdout=PIPE,
            stderr=PIPE,
            env=os.environ.copy(),
        )  # type: ignore

    def launch_backend(  # type: ignore
        self,
        nnodes: int,
        node_ip_addrs: Optional[list[str]],
        nodelist: list[str],
        args_map: dict,
        fe_ip_addr: str,
        fe_host_id: str,
        frontend_sdesc: str,
        network_prefix: str,
        overlay_port: int,
        transport_test_env: bool,
    ):
        be_args = self._get_dragon_launch_be_args(
            fe_ip_addr=fe_ip_addr,
            fe_host_id=fe_host_id,
            frontend_sdesc=frontend_sdesc,
            network_prefix=network_prefix,
            overlay_port=overlay_port,
            transport_test_env=transport_test_env,
        )

        wlm_proc = DragonRunPopen(
            user_command=be_args,
            host_list=nodelist,
            stdout=PIPE,
            stderr=PIPE,
            env=os.environ.copy(),
            # log_level=logging.DEBUG,
        )

        return wlm_proc
