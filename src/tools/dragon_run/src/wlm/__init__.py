import enum

from .slurm import WLMSlurm
from .dragonssh import WLMDragonSSH
from typing import Optional
from ..exceptions import DragonRunNoSupportedWLM


@enum.unique
class WLM(enum.Enum):
    """Enumerated list of supported workload manager"""

    SLURM = "slurm"
    DRAGON_SSH = "ssh"

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
    WLM.SLURM: WLMSlurm,
    WLM.DRAGON_SSH: WLMDragonSSH,
}


def detect_wlm():
    """Detect a supported WLM"""

    wlm = None
    cls = None

    try:
        for wlm, cls in wlm_cls_dict.items():
            if cls.check_for_wlm_support():
                break

    except Exception:
        raise RuntimeError("Error searching for supported WLM")

    if wlm is None:
        raise DragonRunNoSupportedWLM("No supported WLM found")

    return wlm, cls
