import os
import subprocess
import logging

from .base import WLM, BaseWLM
from dragon.infrastructure.facts import TransportAgentOptions
from dragon.tools.dragon_run.src import DragonRunPopen, PIPE
from dragon.tools.dragon_run.src.wlm import WLM as DrunWLM
from dragon.tools.dragon_run.src.facts import ENV_DRAGON_RUN_NODE_FILE

from typing import Optional

logger = logging.getLogger(__name__)


class DRunWLM(BaseWLM):

    name = WLM.DRUN.value

    def __init__(self, network_prefix, port, hostlist):
        nhosts = len(hostlist) if hostlist is not None else 0
        super().__init__(WLM.DRUN.value, network_prefix, port)
        self.hostlist = hostlist

    @classmethod
    def check_for_wlm_support(cls, *args, **kwargs) -> int:
        if cls.has_allocation():
            logger.info("Detected Dragon Run %s in the environment.", ENV_DRAGON_RUN_NODE_FILE)
            return 3
        logger.info("Dragon Run was not detected")
        return 0

    @classmethod
    def requires_allocation(cls) -> bool:
        return False

    @classmethod
    def has_allocation(cls) -> bool:
        return ENV_DRAGON_RUN_NODE_FILE in os.environ

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

    def _launch_network_config_helper(self, args_map: dict) -> subprocess.Popen:
        network_config_helper_cmd = self.NETWORK_CFG_HELPER_LAUNCH_CMD
        self.LOGGER.debug(f"Launching config with: {network_config_helper_cmd=}")
        return DragonRunPopen(
            user_command=network_config_helper_cmd,
            host_list=self.hostlist,
            stdout=PIPE,
            stderr=PIPE,
            env=os.environ.copy(),
            force_wlm=DrunWLM.DRAGON_SSH,
            ssh_config_path=args_map.get("ssh_config_path"),
            private_key=args_map.get("private_key"),
            passphrase=args_map.get("passphrase"),
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
        overlay_transport: TransportAgentOptions,
        overlay_port: int,
        log_level: int = logging.NOTSET,
    ):
        be_args = self._get_dragon_launch_be_args(
            fe_ip_addr=fe_ip_addr,
            fe_host_id=fe_host_id,
            frontend_sdesc=frontend_sdesc,
            network_prefix=network_prefix,
            overlay_transport=overlay_transport,
            overlay_port=overlay_port,
        )

        wlm_proc = DragonRunPopen(
            user_command=be_args,
            host_list=nodelist,
            stdout=PIPE,
            stderr=PIPE,
            env=os.environ.copy(),
            force_wlm=DrunWLM.DRAGON_SSH,
            ssh_config_path=args_map.get("ssh_config_path"),
            private_key=args_map.get("private_key"),
            passphrase=args_map.get("passphrase"),
            log_level=log_level,
        )

        return wlm_proc
