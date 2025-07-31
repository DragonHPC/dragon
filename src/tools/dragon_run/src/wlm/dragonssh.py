import os

from subprocess import check_output

from .base import WLMBase
from ..facts import ENV_DRAGON_RUN_NODE_FILE


class WLMDragonSSH(WLMBase):

    NAME = "SSH"

    @classmethod
    def check_for_wlm_support(cls):
        return ENV_DRAGON_RUN_NODE_FILE in os.environ

    @classmethod
    def check_for_allocation(cls) -> bool:
        return ENV_DRAGON_RUN_NODE_FILE in os.environ

    @classmethod
    def get_host_list(cls):
        dragon_run_node_fn = os.environ[ENV_DRAGON_RUN_NODE_FILE]
        with open(dragon_run_node_fn) as inf:
            return inf.read().splitlines()
