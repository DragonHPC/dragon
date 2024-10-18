import re
import enum
from dataclasses import dataclass, field
from subprocess import check_output

GENERIC_REGEX = "((3d|display|vga) (controller|connector)?): (NVIDIA|AMD|Advanced Micro Devices|Intel Corporation Device)"


class AccVendor(enum.IntEnum):
    NVIDIA = enum.auto()
    AMD = enum.auto()
    INTEL = enum.auto()
    UNKNOWN = enum.auto()


class AccEnvStr():
    NVIDIA = "CUDA_VISIBLE_DEVICES"
    AMD = "ROCR_VISIBLE_DEVICES"
    HIP = "HIP_VISIBLE_DEVICES"
    INTEL = "ZE_AFFINITY_MASK"


@dataclass
class AcceleratorDescriptor:
    vendor: AccVendor = AccVendor.UNKNOWN
    device_list: list[int] = field(default_factory=list)  # Currently just returns a list of ints that correspond to device ID (0, 1, 2, etc)
    env_str: str = ""

    def get_sdict(self):
        rv = {
            "vendor": self.vendor,
            "device_list": self.device_list,
            "env_str": self.env_str
        }
        return rv

    @classmethod
    def from_sdict(cls, sdict):
        return AcceleratorDescriptor(**sdict)


def find_nvidia():
    try:
        output = check_output(["nvidia-smi", "-L"]).decode('utf-8').splitlines()
        return output
    except:
        return None


def find_amd():
    # rocm-smi works similar to nvidia-smi but currently there is no clean "list devices" param
    # lspci is easier to parse
    return None


def find_accelerators() -> AcceleratorDescriptor:
    devices = find_nvidia()
    if devices is not None:
        acc = AcceleratorDescriptor(vendor=AccVendor.NVIDIA,
                                    device_list=list(range(len(devices))),
                                    env_str=AccEnvStr.NVIDIA
                                    )
        return acc

    devices = find_amd()
    if devices is not None:
        return None  # Not implemented, see find_amd()

    try:
        output = check_output(["lspci"]).decode('utf-8').splitlines()
    except FileNotFoundError as e:
        # print("LSPCI not installed") # TODO: This needs to be sent to a logger somewhere
        return None

    # NOTE: Will not work as expected with heterogenous setups (e.g. mixed Nvidia/AMD cards on one node)
    devices = AcceleratorDescriptor()
    n_devices = 0
    for line in output:
        m = re.search(GENERIC_REGEX, line, re.IGNORECASE)
        if m:
            if m[4] == "NVIDIA":
                devices.vendor = AccVendor.NVIDIA
                devices.env_str = AccEnvStr.NVIDIA
            elif m[4] == "AMD" or m[4] == "Advanced Micro Devices":
                devices.vendor = AccVendor.AMD
                devices.env_str = AccEnvStr.AMD
            elif m[4] == "Intel Corporation Device":
                devices.vendor = AccVendor.INTEL
                devices.env_str = AccEnvStr.INTEL
            n_devices += 1

    if n_devices > 0:
        if devices.vendor == AccVendor.INTEL:
            devices.device_list = []
            for i in range(n_devices):
                devices.device_list.append(i + 0.0)
                devices.device_list.append(i + 0.1)
        else:
            devices.device_list = list(range(n_devices))
    else:
        return None

    return devices
