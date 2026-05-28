"""Module for detecting GPU devices across differet vendors"""

import re
import enum
import os
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


def str_to_num(s: str) -> int | float:
    """Return a number from of the correct type from a string

    :param s: GPU ID
    :type s: str
    :return: number corresponding to input string
    :rtype: int or float
    """
    try:
        return int(s)
    except ValueError:
        try:
            return float(s)
        except ValueError:
            raise ValueError("GPU mask needs to take ints or floats as the GPU IDs.") from None


def find_nvidia() -> list:
    """Return list of Nvidia GPUs returned by nvidia-smi. Expected output from smi:

    .. code-block:: text

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
    # If a mask is set, return GPUs in mask
    mask = os.environ.get("CUDA_VISIBLE_DEVICES", "")
    if mask:
        devices = []
        for device in mask.split(","):
            devices.append(str_to_num(device))
        return devices

    try:
        output = check_output(["nvidia-smi", "-L"], stderr=DEVNULL).decode("utf-8").splitlines()
        return list(range(len(output)))
    except:
        return None


def find_amd() -> list:
    """Return list of AMD GPUs returned by rocm-smi. Expected output from smi:

    .. code-block:: text

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
    # If a mask is set, return GPUs in mask
    mask = os.environ.get("ROCR_VISIBLE_DEVICES", "")
    if mask:
        devices = []
        for device in mask.split(","):
            devices.append(str_to_num(device))
        return devices

    try:
        output = (
            check_output(["rocm-smi", "--showuniqueid", "--csv"], stderr=DEVNULL)
            .decode("utf-8")
            .strip("\n")
            .splitlines()
        )
        return list(range(len(output[1:])))
    except:
        return None


def find_intel() -> list:
    """Return list of Intel GPUs returned by xpu-smi. Expected output from smi:

    .. code-block:: text

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
    # If a mask is set, return GPUs in mask
    mask = os.environ.get("ZE_AFFINITY_MASK", "")
    if mask:
        devices = []
        for device in mask.split(","):
            devices.append(str_to_num(device))
        return devices

    # Read the output of xpu-smi (recommended)
    try:
        output = check_output(["xpu-smi", "discovery"], stderr=DEVNULL).decode("utf-8").splitlines()
        gpu_card = 0
        cards = []
        for line in output:
            if "SOC UUID:" in line:
                cards.append((gpu_card, line))
                gpu_card += 1

        devices = []
        hierarchy_mode = os.environ.get("ZE_FLAT_DEVICE_HIERARCHY", "COMPOSITE")
        for i,_ in cards:
            if hierarchy_mode == "FLAT":
                devices.append(i * 2)
                devices.append(i * 2 + 1)
            elif hierarchy_mode == "COMPOSITE":
                devices.append(i + 0.0)
                devices.append(i + 0.1)
        return devices
    except:
        return None


def find_accelerators() -> AcceleratorDescriptor:
    """Scan for accelerators across all supported vendors"""
    devices = find_nvidia()
    if devices:
        acc = AcceleratorDescriptor(
            vendor=AccVendor.NVIDIA, device_list=devices, env_str=AccEnvStr.NVIDIA
        )
        return acc

    devices = find_amd()
    if devices is not None:
        acc = AcceleratorDescriptor(vendor=AccVendor.AMD, device_list=devices, env_str=AccEnvStr.AMD)
        return acc

    devices = find_intel()
    if devices is not None:
        acc = AcceleratorDescriptor(vendor=AccVendor.INTEL, device_list=devices, env_str=AccEnvStr.INTEL)
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
            hierarchy_mode = os.environ.get("ZE_FLAT_DEVICE_HIERARCHY", "COMPOSITE")
            for i in range(n_devices):
                if hierarchy_mode == "FLAT":
                    devices.device_list.append(i)
                    devices.device_list.append(i + n_devices)
                elif hierarchy_mode == "COMPOSITE":
                    devices.device_list.append(i + 0.0)
                    devices.device_list.append(i + 0.1)
            # sorting for readability
            devices.device_list.sort()
        else:
            devices.device_list = list(range(n_devices))
    else:
        return None

    return devices
