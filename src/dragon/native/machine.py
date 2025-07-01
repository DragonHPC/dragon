"""The Dragon native machine interface provides hardware related functionality.

These calls return various hardware properties of the system(s), like the number of processors
or a list of all nodes.
"""

import logging

from ..globalservices.node import query, query_total_cpus, get_list
from ..infrastructure.gpu_desc import AccVendor
from ..utils import host_id

LOG = logging.getLogger(__name__)

# TODO: Decide on a model for system architecture that generalizes
# well to HPC and cloud environments alike


def cpu_count() -> int:
    """Return the number of CPUs in this system.

    :return: integer representing the number of cpus
    """

    return query_total_cpus()


class Node:
    def __init__(self, ident: str or int):
        """A class abstracting the current hardware node.

        :param ident: name or h_uid of the node to request.
        :type ident: str or int
        """

        self._descr = None
        self._update_descriptor(ident)

    def __str__(self) -> str:
        return f"name={self._descr.name}, h_uid={self._descr.h_uid}, hostname={self._descr.host_name}, ip={self._descr.ip_addrs}"

    @property
    def name(self) -> str:
        """Return the Dragon name of this node.
        Use Node.hostname for the hostname.

        :return: The name of the node
        :rtype: str
        """
        return self._descr.name

    @property
    def h_uid(self) -> int:
        """The unique global identifier of this node.

        :return: the h_uid
        :rtype: int
        """

        return self._descr.h_uid

    @property
    def ip(self) -> str:
        """Return the IPv4 address of this node

        :return: The addres
        :rtype: str
        """
        return self._descr.ip_addrs

    @property
    def num_cpus(self) -> int:
        """Return the number of CPUs of this node.

        :return: The number of CPUs
        :rtype: int
        """
        return self._descr.num_cpus

    @property
    def num_gpus(self) -> int:
        """Return a the number of GPUs on this node

        :return: The number of GPUs
        :rtype: list[int]
        """
        if self._descr.accelerators is None:
            return 0
        return len(self._descr.accelerators.device_list)

    @property
    def physical_mem(self) -> int:
        """Return the physical memory of this node.

        :return: Physical memory in bytes
        :rtype: int
        """
        return self._descr.physical_mem

    @property
    def gpus(self) -> list[int]:
        """Return a list of GPU visible devices on this node

        :return: list of GPU visible devices
        :rtype: list[int]
        """
        if self._descr.accelerators is None:
            return None
        return self._descr.accelerators.device_list

    @property
    def gpu_vendor(self) -> str:
        """Return the name of the GPU Vendor on this node

        :return: GPU vendor name
        :rtype: str
        """
        if self._descr.accelerators is None:
            return None
        vendor_int = self._descr.accelerators.vendor
        if vendor_int == AccVendor.NVIDIA:
            return "Nvidia"
        elif vendor_int == AccVendor.AMD:
            return "AMD"
        elif vendor_int == AccVendor.INTEL:
            return "Intel"
        else:
            return "Unknown Vendor"

    @property
    def cpus(self) -> list[int]:
        """Return the CPUs available on this node

        :return: list of CPUs
        :rtype: list[int]
        """
        return self._descr.cpu_devices

    @property
    def hostname(self) -> str:
        """Host name of the node in the network.

        This is not the Dragon internal name (Node.name) for this node.

        :return: The hostname
        :rtype: str
        """
        return self._descr.host_name

    @property
    def is_primary(self) -> bool:
        """Returns true if this is the primary node for the runtime. The primary node is important in that it is the node with global services.
        :return: is the primary node
        :rtype: bool
        """
        return self._descr.is_primary

    def _update_descriptor(self, ident=None):
        if ident is not None:
            self._descr = query(ident)
        else:
            self._descr = query(self._descr.h_uid)


def current() -> Node:
    """Return a node object for this process.

    :return: Node object
    :rtype: Node
    """
    h_uid = host_id()
    return Node(h_uid)


class System:
    def __init__(self):
        """A stub of a system abstraction"""
        self._nodes = get_list()

    @property
    def nodes(self):
        return self._nodes

    @property
    def nnodes(self) -> int:
        return len(self._nodes)
