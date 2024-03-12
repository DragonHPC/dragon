import os
import re
import shutil
import subprocess

from .base import BaseNetworkConfig

PBS_PALS_LAUNCH_BE_ARGS = 'mpiexec --np {nnodes} --ppn 1 --cpu-bind none --hosts {nodelist} --line-buffer'


def get_pbs_pals_launch_be_args(args_map=None):
    return PBS_PALS_LAUNCH_BE_ARGS


def get_nodefile_node_count(filename) -> int:
    nnodes = 0
    with open(filename) as f:
        for nnodes, _ in enumerate(f, start=1):
            pass
    return nnodes


class PBSPalsNetworkConfig(BaseNetworkConfig):

    MPIEXEC_COMMAND_LINE = "mpiexec --np {nnodes} -ppn 1 -l"

    def __init__(self, network_prefix, port, hostlist):

        if not os.environ.get("PBS_NODEFILE"):
            msg = """Requesting a PBS network config outside of PBS job allocation.
Resubmit as part of a 'qsub' execution"""
            raise RuntimeError(msg)

        super().__init__(
            'pbs+pals',
            network_prefix,
            port,
            get_nodefile_node_count(os.environ.get("PBS_NODEFILE")),
        )

        self.job_id = os.environ.get("PBS_JOBID")
        self.MPIEXEC_ARGS = self.MPIEXEC_COMMAND_LINE.format(nnodes=self.NNODES).split()

    @classmethod
    def check_for_wlm_support(cls) -> bool:
        if (mpiexec := shutil.which("mpiexec")):
            return re.match('.*/pals/.*', mpiexec)
        return False

    def _get_wlm_job_id(self) -> str:
        return self.job_id

    def _supports_net_conf_cache(self) -> bool:
        return False

    def _launch_network_config_helper(self) -> subprocess.Popen:
        mpiexec_launch_args = self.MPIEXEC_ARGS[:]
        mpiexec_launch_args.append("--line-buffer")
        mpiexec_launch_args.extend(self.NETWORK_CFG_HELPER_LAUNCH_CMD)

        self.LOGGER.debug(f"Launching config with: {mpiexec_launch_args=}")

        return subprocess.Popen(
            args=mpiexec_launch_args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=0,
            start_new_session=True
        )
