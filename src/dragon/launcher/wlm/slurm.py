import os
import shutil
import logging
import subprocess

from .base import WLM, BaseWLM


logger = logging.getLogger(__name__)

class SlurmWLM(BaseWLM):

    name = WLM.SLURM.value
    SRUN_COMMAND_LINE = "srun --nodes={nnodes} --ntasks={nnodes} --cpu_bind=none -u -l -W 0"
    ENV_SLURM_JOB_ID = "SLURM_JOB_ID"
    ENV_SLURM_NUM_NODES = "SLURM_JOB_NUM_NODES"

    def __init__(self, network_prefix, port, _hostlist):

        self.job_id = os.environ.get(self.ENV_SLURM_JOB_ID)
        if not self.job_id:
            msg = """Requesting a slurm network config outside of slurm job allocation.
Resubmit as part of a 'salloc' or 'sbatch' execution."""
            raise RuntimeError(msg)

        nnodes = os.environ.get(self.ENV_SLURM_NUM_NODES)
        if not nnodes:
            msg = f"{self.ENV_SLURM_NUM_NODES} environment variable is not set"
            raise RuntimeError(msg)

        super().__init__(WLM.SLURM.value, network_prefix, port)

        self.SRUN_ARGS = self.SRUN_COMMAND_LINE.format(nnodes=nnodes).split()

    @classmethod
    def check_for_wlm_support(cls) -> int:
        if shutil.which("srun") is not None:
            logger.info("Slurm was detected on the system.")
            if cls.has_allocation():
                logger.info("Slurm job allocation detected.")
                return 2
            logger.info("No Slurm job allocation detected.")
            return 1
        logger.info("Slurm was not detected on the system.")
        return 0

    @classmethod
    def requires_allocation(cls) -> bool:
        return True

    @classmethod
    def has_allocation(cls) -> bool:
        return os.environ.get(cls.ENV_SLURM_JOB_ID) is not None

    def _get_wlm_job_id(self) -> str:
        assert self.job_id
        return self.job_id

    def _supports_net_conf_cache(self) -> bool:
        return False

    def _launch_network_config_helper(self, args_map: dict) -> subprocess.Popen:
        srun_launch_args = self.SRUN_ARGS[:]
        srun_launch_args.extend(list(self.NETWORK_CFG_HELPER_LAUNCH_CMD))  # type: ignore

        self.LOGGER.debug(f"Launching config with: {srun_launch_args=}")

        return subprocess.Popen(
            args=srun_launch_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=0, start_new_session=True
        )

    def _get_wlm_launch_be_args(self, args_map, launch_args):
        slurm_launch_be_args = [
            "srun",
            f"--nodes={args_map['nnodes']}",
            f"--ntasks={args_map['nnodes']}",
            "--cpu_bind=none",
            f"--nodelist={args_map['nodelist']}",
        ]
        return slurm_launch_be_args + launch_args
