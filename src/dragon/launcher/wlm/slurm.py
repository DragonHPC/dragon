import os
import shutil
import subprocess

from .base import BaseNetworkConfig


def get_slurm_launch_be_args(args_map, launch_args):
    slurm_launch_be_args = [
        "srun",
        f"--nodes={args_map['nnodes']}",
        f"--ntasks={args_map['nnodes']}",
        "--cpu_bind=none",
        f"--nodelist={args_map['nodelist']}",
    ]
    return slurm_launch_be_args + launch_args


class SlurmNetworkConfig(BaseNetworkConfig):

    SRUN_COMMAND_LINE = "srun --nodes={nnodes} --ntasks={nnodes} --cpu_bind=none -u -l -W 0"
    ENV_SLURM_JOB_ID = "SLURM_JOB_ID"

    def __init__(self, network_prefix, port, hostlist):

        self.job_id = os.environ.get(self.ENV_SLURM_JOB_ID)
        if not self.job_id:
            msg = """Requesting a slurm network config outside of slurm job allocation.
Resubmit as part of a 'salloc' or 'sbatch' execution."""
            raise RuntimeError(msg)

        super().__init__("slurm", network_prefix, port, int(os.environ.get("SLURM_JOB_NUM_NODES")))

        self.SRUN_ARGS = self.SRUN_COMMAND_LINE.format(nnodes=self.NNODES).split()

    @classmethod
    def check_for_wlm_support(cls) -> bool:
        return shutil.which("srun") is not None

    @classmethod
    def check_for_allocation(cls) -> bool:
        return os.environ.get(cls.ENV_SLURM_JOB_ID) is not None

    def _get_wlm_job_id(self) -> str:
        return self.job_id

    def _supports_net_conf_cache(self) -> bool:
        return False

    def _launch_network_config_helper(self) -> subprocess.Popen:
        srun_launch_args = self.SRUN_ARGS[:]
        srun_launch_args.extend(self.NETWORK_CFG_HELPER_LAUNCH_CMD)

        self.LOGGER.debug(f"Launching config with: {srun_launch_args=}")

        return subprocess.Popen(
            args=srun_launch_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=0, start_new_session=True
        )
