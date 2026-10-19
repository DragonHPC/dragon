from .slurm import SlurmWLM
from .pbs import PBSWLM
from .ssh import SSHWLM
from .k8s import KubernetesNetworkConfig
from .drun import DRunWLM
from .base import WLM

__all__ = ['WLM', 'wlm_cls_dict']

wlm_cls_dict = {
    WLM.SLURM: SlurmWLM,
    WLM.PBS: PBSWLM,
    WLM.K8S: KubernetesNetworkConfig,
    WLM.SSH: SSHWLM,
    WLM.DRUN: DRunWLM,
}
