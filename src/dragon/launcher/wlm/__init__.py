import enum

from .slurm import SlurmNetworkConfig, get_slurm_launch_be_args
from .pbs_pals import PBSPalsNetworkConfig, get_pbs_pals_launch_be_args
from .ssh import SSHNetworkConfig, get_ssh_launch_be_args
from .k8s import KubernetesNetworkConfig


@enum.unique
class WLM(enum.Enum):
    """Enumerated list of supported workload manager"""

    SLURM = "slurm"
    PBS_PALS = "pbs+pals"
    SSH = "ssh"
    K8S = "k8s"

    def __str__(self):
        return self.value

    @staticmethod
    def from_str(s):
        """Obtain enum value from WLM string

        Args:
            s (str): name of WLM (eg: 'slurm', 'pbs_pals', etc.)

        Raises:
            ValueError: invalid string input

        Returns:
            WLM: enumerated value associated with string WLM
        """
        try:
            return WLM(s)
        except KeyError:
            raise ValueError()


wlm_cls_dict = {
    WLM.SLURM: SlurmNetworkConfig,
    WLM.PBS_PALS: PBSPalsNetworkConfig,
    WLM.K8S: KubernetesNetworkConfig,
    WLM.SSH: SSHNetworkConfig,
}

wlm_launch_dict = {
    WLM.SLURM: get_slurm_launch_be_args,
    WLM.PBS_PALS: get_pbs_pals_launch_be_args,
    WLM.SSH: get_ssh_launch_be_args,
}
