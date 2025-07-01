import os
import re
import shutil
import subprocess
import json


from .base import BaseNetworkConfig
from ...infrastructure import facts as dfacts


def get_pbs_pals_launch_be_args(args_map, launch_args):
    config_file_path = dfacts.CONFIG_FILE_PATH

    mpiexec_override = None
    if config_file_path.exists():
        with open(config_file_path) as config_file:
            config_dict = json.load(config_file)

        # Get all the runtimes
        try:
            mpiexec_override = config_dict["launcher-mpiexec-override-be"]
        except KeyError:
            pass

    if mpiexec_override is not None:
        pbs_pals_launch_be_args = mpiexec_override.format(
            nnodes=str(args_map["nnodes"]), nodelist=args_map["nodelist"]
        ).split()
    else:
        pbs_pals_launch_be_args = [
            "mpiexec",
            "--np",
            str(args_map["nnodes"]),
            "--ppn",
            "1",
            "--cpu-bind",
            "none",
            "--hosts",
            args_map["nodelist"],
            "--line-buffer",
        ]
    return pbs_pals_launch_be_args + launch_args


def get_nodefile_node_count(filename) -> int:
    nnodes = 0
    with open(filename) as f:
        for nnodes, _ in enumerate(f, start=1):
            pass
    return nnodes


class PBSPalsNetworkConfig(BaseNetworkConfig):

    MPIEXEC_COMMAND_LINE = "mpiexec --np {nnodes} -ppn 1 -l --line-buffer"
    ENV_PBS_JOB_ID = "PBS_JOBID"

    def __init__(self, network_prefix, port, hostlist):

        if not os.environ.get("PBS_NODEFILE"):
            msg = """Requesting a PBS network config outside of PBS job allocation.
Resubmit as part of a 'qsub' execution"""
            raise RuntimeError(msg)

        super().__init__(
            "pbs+pals",
            network_prefix,
            port,
            get_nodefile_node_count(os.environ.get("PBS_NODEFILE")),
        )

        self.job_id = os.environ.get(self.ENV_PBS_JOB_ID)
        config_file_path = dfacts.CONFIG_FILE_PATH

        mpiexec_override = None
        if config_file_path.exists():
            with open(config_file_path) as config_file:
                config_dict = json.load(config_file)

            # Get all the runtimes
            try:
                mpiexec_override = config_dict["launcher-mpiexec-override-netconfig"]
            except KeyError:
                pass

        if mpiexec_override is not None:
            self.MPIEXEC_ARGS = mpiexec_override.format(nnodes=self.NNODES).split()
        else:
            self.MPIEXEC_ARGS = self.MPIEXEC_COMMAND_LINE.format(nnodes=self.NNODES).split()

    @classmethod
    def check_for_wlm_support(cls) -> bool:
        # Look for qstat which is part of PBS
        qstat = shutil.which("qstat")
        if not qstat or re.match(".*/pbs/.*", qstat) is None:
            return False

        # Now to see if we have a supported version of mpiexec
        if mpiexec := shutil.which("mpiexec"):
            if re.match(".*/pals/.*", mpiexec) is None:
                raise RuntimeError(
                    "PBS has been detected on the system. However, Dragon is only compatible with a PALS mpiexec and it was not found."
                )
            return True

        raise RuntimeError("PBS was detected on the system, but Dragon cannot find the mpiexec command.")

    @classmethod
    def check_for_allocation(cls) -> bool:
        return os.environ.get(cls.ENV_PBS_JOB_ID) is not None

    def _get_wlm_job_id(self) -> str:
        return self.job_id

    def _supports_net_conf_cache(self) -> bool:
        return False

    def _launch_network_config_helper(self) -> subprocess.Popen:
        mpiexec_launch_args = self.MPIEXEC_ARGS[:]
        mpiexec_launch_args.extend(self.NETWORK_CFG_HELPER_LAUNCH_CMD)

        self.LOGGER.debug(f"Launching config with: {mpiexec_launch_args=}")

        return subprocess.Popen(
            args=mpiexec_launch_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=0, start_new_session=True
        )
