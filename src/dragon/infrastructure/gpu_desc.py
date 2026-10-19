"""Module for detecting GPU devices across differet vendors"""

import re
import enum
import os
import sys
from dataclasses import dataclass, field
from subprocess import check_output, DEVNULL

from ..accelerators import Accelerator, AccVendor, AccEnvStr

GENERIC_REGEX = (
    "((3d|display|vga) (controller|connector)?): (NVIDIA|AMD|Advanced Micro Devices|Intel Corporation Device)"
)


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


def apply_gpu_affinity(env, device_list, env_str, inherited_env=None):
    inherited_env = os.environ if inherited_env is None else inherited_env
    mask = inherited_env.get(env_str)
    visible_devices = [token.strip() for token in mask.split(",")] if mask else None

    if env_str == AccEnvStr.AMD:
        hip_mask = inherited_env.get(AccEnvStr.HIP)
        if hip_mask:
            hip_devices = [token.strip() for token in hip_mask.split(",")]
            visible_devices = (
                [visible_devices[int(device)] for device in hip_devices] if visible_devices else hip_devices
            )
        env.pop(AccEnvStr.HIP, None)

    if visible_devices is None:
        devices = [str(device) for device in device_list]
    elif env_str == AccEnvStr.INTEL and inherited_env.get("ZE_FLAT_DEVICE_HIERARCHY", "COMPOSITE") == "COMPOSITE":
        roots = sorted({int(device.split(".")[0]) for device in visible_devices})
        devices = []
        for device in device_list:
            root, separator, tile = str(device).partition(".")
            physical_root = roots[int(root)]
            devices.append(f"{physical_root}{separator}{tile}")
    else:
        devices = [visible_devices[int(device)] for device in device_list]

    env[env_str] = ",".join(devices)


def find_accelerators() -> AcceleratorDescriptor:
    """Scan for accelerators across all supported vendors"""
    acc = Accelerator()
    if acc.get_vendor() == AccVendor.UNKNOWN:
        return None

    else:
        return AcceleratorDescriptor(
            vendor=acc.get_vendor(), device_list=acc.get_device_list(), env_str=acc.get_vendor_env_str()
        )
