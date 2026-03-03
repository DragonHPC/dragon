import enum

from . import launch_selector, network_config, launchargs


@enum.unique
class ConfigOutputType(enum.Enum):
    JSON = "json"
    YAML = "yaml"

    def __str__(self):
        return self.value


@enum.unique
class WLM(enum.Enum):
    SLURM = "slurm"
    PBS_PALS = "pbs+pals"

    def __str__(self):
        return self.value

    @staticmethod
    def from_str(s):
        try:
            return WLM(s)
        except KeyError:
            raise ValueError()
