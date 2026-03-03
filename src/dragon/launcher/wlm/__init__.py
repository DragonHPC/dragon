import enum

from .slurm import SlurmWLM
from .pbs import PBSWLM
from .ssh import SSHWLM
from .k8s import KubernetesNetworkConfig
from .drun import DRunWLM


@enum.unique
class WLM(enum.Enum):
    """Enumerated list of supported workload manager"""

    SLURM = "slurm"
    PBS = "pbs+pals"
    SSH = "ssh"
    K8S = "k8s"
    DRUN = "drun"

    def __str__(self):
        return self.value

    @staticmethod
    def from_str(s):
        """Obtain enum value from WLM string

        Args:
            s (str): name of WLM (eg: 'slurm', 'pbs', etc.)

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
    WLM.SLURM: SlurmWLM,
    WLM.PBS: PBSWLM,
    WLM.K8S: KubernetesNetworkConfig,
    WLM.SSH: SSHWLM,
    WLM.DRUN: DRunWLM,
}
