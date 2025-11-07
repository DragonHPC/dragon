import os
import shutil

from subprocess import check_output

from .base import WLMBase


class WLMSlurm(WLMBase):

    ENV_SLURM_JOB_ID = "SLURM_JOB_ID"
    NAME = "slurm"

    @classmethod
    def check_for_wlm_support(cls):
        return all(
            [
                shutil.which("srun") is not None,
                shutil.which("scontrol") is not None,
            ]
        )

    @classmethod
    def check_for_allocation(cls) -> bool:
        return os.environ.get(cls.ENV_SLURM_JOB_ID) is not None

    @classmethod
    def get_host_list(cls):
        output = check_output(["scontrol", "show", "hostnames"])
        return [host.decode("utf-8") for host in output.split()]
