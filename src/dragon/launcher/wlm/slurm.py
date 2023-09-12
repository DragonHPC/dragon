import os
import shutil
import subprocess

from .base import BaseNetworkConfig

SLURM_LAUNCH_BE_ARGS = 'srun --nodes={nnodes} --ntasks={nnodes} --cpu_bind=none --nodelist={nodelist}'


def get_slurm_launch_be_args(args_map=None):
    return SLURM_LAUNCH_BE_ARGS


class SlurmNetworkConfig(BaseNetworkConfig):

    SRUN_COMMAND_LINE = "srun --nodes={nnodes} --ntasks={nnodes} --cpu_bind=none -u -l"

    def __init__(self, network_prefix, port, hostlist):

        if not os.environ.get("SLURM_JOB_ID"):
            msg = """Requesting a slurm network config outside of slurm job allocation.
Resubmit as part of a 'salloc' or 'sbatch' execution."""
            raise RuntimeError(msg)

        super().__init__(
            network_prefix, port, int(os.environ.get("SLURM_JOB_NUM_NODES"))
        )

        self.SRUN_ARGS = self.SRUN_COMMAND_LINE.format(nnodes=self.NNODES).split()

    @classmethod
    def check_for_wlm_support(cls) -> bool:
        return shutil.which("srun")

    def _launch_network_config_helper(self) -> subprocess.Popen:
        srun_launch_args = self.SRUN_ARGS[:]
        srun_launch_args.extend(self.NETWORK_CFG_HELPER_LAUNCH_CMD)

        self.LOGGER.debug(f"Launching config with: {srun_launch_args=}")

        return subprocess.Popen(
            args=srun_launch_args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=0,
            start_new_session=True
        )
