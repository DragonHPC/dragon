import re
import enum
from dataclasses import dataclass, field
from subprocess import check_output, DEVNULL

GENERIC_REGEX = (
    "((3d|display|vga) (controller|connector)?): (NVIDIA|AMD|Advanced Micro Devices|Intel Corporation Device)"
)


class AccVendor(enum.IntEnum):
    NVIDIA = enum.auto()
    AMD = enum.auto()
    INTEL = enum.auto()
    UNKNOWN = enum.auto()


class AccEnvStr:
    NVIDIA = "CUDA_VISIBLE_DEVICES"
    AMD = "ROCR_VISIBLE_DEVICES"
    HIP = "HIP_VISIBLE_DEVICES"
    INTEL = "ZE_AFFINITY_MASK"


@dataclass
class AcceleratorDescriptor:
    vendor: AccVendor = AccVendor.UNKNOWN
    device_list: list[int] = field(
        default_factory=list
    )  # Currently just returns a list of ints that correspond to device ID (0, 1, 2, etc)
    env_str: str = ""

    def get_sdict(self):
        rv = {"vendor": self.vendor, "device_list": self.device_list, "env_str": self.env_str}
        return rv

    @classmethod
    def from_sdict(cls, sdict):
        return AcceleratorDescriptor(**sdict)


def find_nvidia() -> list:
    """Return list of Nvidia GPUs returned by nvidia-smi. Expected output from smi:
    .
    .
    .
    GPU 1: NVIDIA A100-SXM4-40GB (UUID: GPU-ccdb6af5-102b-3fb4-4e06-8b7aaeba0578)
    GPU 2: NVIDIA A100-SXM4-40GB (UUID: GPU-43539da6-e86a-d93e-8db2-f8814ef47c41)
    .
    .
    .


    :return: list of GPUs with IDs.
    :rtype: list
    """
    try:
        output = check_output(["nvidia-smi", "-L"], stderr=DEVNULL).decode("utf-8").splitlines()
        return output
    except:
        return None


def find_amd() -> list:
    """Return list of AMD GPUs returned by rocm-smi. Expected output from smi:
    .
    .
    .
    card2,0x4eda2591da9a0592
    card3,0x96ca52b5699c2baf
    .
    .
    .

    :return: a list of cards that can be iterated over
    :rtype: list
    """
    try:
        output = (
            check_output(["rocm-smi", "--showuniqueid", "--csv"], stderr=DEVNULL)
            .decode("utf-8")
            .strip("\n")
            .splitlines()
        )
        return output[1:]
    except:
        return None


def find_intel() -> list:
    """Return list of Intel GPUs returned by xpu-smi. Expected output from smi:
        .
        .
        .
        +-----------+--------------------------------------------------------------------------------------+
    | 2         | Device Name: Intel(R) Data Center GPU Max 1550                                       |
    |           | Vendor Name: Intel(R) Corporation                                                    |
    |           | SOC UUID: 00000000-0000-0000-0a2a-ca25127eb373                                       |
    |           | PCI BDF Address: 0000:6c:00.0                                                        |
    |           | DRM Device: /dev/dri/card2                                                           |
    |           | Function Type: physical                                                              |
    +-----------+--------------------------------------------------------------------------------------+
    | 3         | Device Name: Intel(R) Data Center GPU Max 1550                                       |
    |           | Vendor Name: Intel(R) Corporation                                                    |
    |           | SOC UUID: 00000000-0000-0000-e986-d69bb5dc50cb                                       |
    |           | PCI BDF Address: 0001:18:00.0                                                        |
    |           | DRM Device: /dev/dri/card3                                                           |
    |           | Function Type: physical                                                              |
        .
        .
        .

        :return: list of tuples with gpu device number and ID
        :rtype: list
    """
    try:
        output = check_output(["xpu-smi", "discovery"], stderr=DEVNULL).decode("utf-8").splitlines()
        gpu_device = 0
        devices = []
        for line in output:
            if "SOC UUID:" in line:
                devices.append((gpu_device, line))
                gpu_device += 1
        return devices
    except:
        return None


def find_accelerators() -> AcceleratorDescriptor:
    devices = find_nvidia()
    if devices is not None:
        acc = AcceleratorDescriptor(
            vendor=AccVendor.NVIDIA, device_list=list(range(len(devices))), env_str=AccEnvStr.NVIDIA
        )
        return acc

    devices = find_amd()
    if devices is not None:
        acc = AcceleratorDescriptor(vendor=AccVendor.AMD, device_list=list(range(len(devices))), env_str=AccEnvStr.AMD)
        return acc

    devices = find_intel()
    if devices is not None:
        intel_device_list = []
        for i in range(len(devices)):
            intel_device_list.append(i + 0.0)
            intel_device_list.append(i + 0.1)
        acc = AcceleratorDescriptor(vendor=AccVendor.INTEL, device_list=intel_device_list, env_str=AccEnvStr.INTEL)
        return acc

    try:
        output = check_output(["lspci"]).decode("utf-8").splitlines()
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
