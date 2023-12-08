""" A class that uniquely defines properties of a managed hardware node for infrastructure communication.

This is a stub at the moment, and should be extended for dynamic hardware
allocations/elasticity aka The Cloud (TM) and more complex node properties
(accelerators, networks, encryption).
"""

import enum
import re
import os
from socket import gethostname, socket, AF_INET, SOCK_STREAM
from typing import Optional

from .facts import DEFAULT_TRANSPORT_NETIF, DEFAULT_OVERLAY_NETWORK_PORT, DEFAULT_PORT_RANGE
from ..utils import host_id as get_host_id
from .util import port_check
from .gpu_desc import AcceleratorDescriptor, find_accelerators


class NodeDescriptor:
    """Globally available description of a 'managed node'."""

    @enum.unique
    class State(enum.IntEnum):
        NULL = enum.auto()
        DISCOVERABLE = enum.auto()
        PENDING = enum.auto()
        ACTIVE = enum.auto()
        ERROR = enum.auto()

    # TODO: this is a stub (PE-42397). How do we deal with:
    #   * networks and their encryption ?
    #   * hetergeneous federated systems -> "cloud"
    #   * heterogenous networks betweens these systems -> "cloud"

    # TODO: Look for opportunities to integrate this further with LS and LA
    # during startup. See dragon.launcher.node_desc.py

    def __init__(
        self,
        state: State = State.NULL,
        h_uid: Optional[int] = None,
        name: str = "",
        ip_addrs: Optional[list[str]] = None,
        port: int = None,
        num_cpus: int = 0,
        physical_mem: int = 0,
        is_primary: bool = False,
        host_id: int = None,
        shep_cd: str = '',
        overlay_cd: str = '',
        host_name: str = '',
        cpu_devices: Optional[list[int]] = None,
        accelerators: Optional[AcceleratorDescriptor] = None
    ):
        self.h_uid = h_uid
        self.name = name
        self.state = state
        self.num_cpus = num_cpus
        self.physical_mem = physical_mem
        self.is_primary = is_primary

        self.host_name = host_name
        self.shep_cd = shep_cd
        self.overlay_cd = overlay_cd
        self.host_id = host_id
        self.cpu_devices = cpu_devices
        self.accelerators = accelerators

        # Not a very accurate measure since we don't know when a policy group is done,
        #  but it gives some manner of tracking for block application
        # TODO: This might be useful later when we can GC finished policy jobs
        self.num_policies = 0

        if port is not None:
            self.ip_addrs = [f'{ip_addr}:{port}' for ip_addr in ip_addrs]
        else:
            self.ip_addrs = ip_addrs

    def __repr__(self) -> str:
        return f"name:{self.name}, host_id:{self.host_id} at {self.ip_addrs}, state:{self.state.name}"

    def __str__(self):
        return f"name:{self.name}, host_id:{self.host_id} at {self.ip_addrs}, state:{self.state.name}"

    @classmethod
    def make_for_current_node(cls, name: Optional[str] = None, ip_addrs: Optional[list[str]] = None, is_primary: bool = False):
        """Create a serialized node descriptor for the node this Shepherd is running on.
        Can only be used to send to GS, as h_uid will be None in the descriptor.

        :param name: hostname of this node, defaults to None
        :type name: str, optional
        :param ip_addrs: List of the IP addresses for this node, defaults to ["127.0.0.1"]
        :type ip_addrs: list[str], optional
        :param is_primary: denote if this is the primary node running GS, defaults to False
        :type is_primary: bool, optional
        :return: serialized Node descruptor
        :rtype: dict
        """

        state = cls.State.ACTIVE

        huid = get_host_id() # will become h_uid in GS

        if name is None:
            name = f"Node-{huid}"

        if ip_addrs is None:
            ip_addrs = ["127.0.0.1"]

        num_cpus = os.cpu_count()
        physical_mem = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
        host_name = gethostname()

        desc = cls(
            state=state, name=name, ip_addrs=ip_addrs, num_cpus=num_cpus,
            physical_mem=physical_mem, is_primary=is_primary, host_id=huid, host_name=host_name
        )

        return desc

    @classmethod
    def get_local_node_network_conf(cls,
                                    network_prefix: str = DEFAULT_TRANSPORT_NETIF,
                                    port_range: tuple[int, int] = (DEFAULT_OVERLAY_NETWORK_PORT, DEFAULT_OVERLAY_NETWORK_PORT+DEFAULT_PORT_RANGE)):
        """Return NodeDescriptor with IP for given network prefix, hostname, and host ID

        :param network_prefix: network prefix used to find IP address of this node. Defaults to DEFAULT_TRANSPORT_NETIF
        :type network_prefix: str, optional

        :param port_range: Port range to use for communication. Defaults to (DEFAULT_OVERLAY_NETWORK_PORT, DEFAULT_OVERLAY_NETWORK_PORT+DEFAULT_PORT_RANGE)
        :type port_range: tuple[int, int], optional

        :raises RuntimeError:  RuntimeError: Unable to find network prefix matching requested

        :return: Filled with local network info for node of execution
        :rtype: NodeDescriptor
        """
        from dragon.transport.ifaddrs import getifaddrs, InterfaceAddressFilter

        if type(port_range) is int:
            port_range = (port_range, port_range+DEFAULT_PORT_RANGE)
        hostname = gethostname()
        ifaddr_filter = InterfaceAddressFilter()
        ifaddr_filter.af_inet(inet6=False)  # Disable IPv6 for now
        ifaddr_filter.up_and_running()
        try:
            ifaddrs = list(filter(ifaddr_filter, getifaddrs()))
        except OSError:
            raise
        if not ifaddrs:
            _msg = 'No network interface with an AF_INET address was found'
            raise RuntimeError(_msg)

        try:
            re_prefix = re.compile(network_prefix)
        except (TypeError, NameError):
            _msg = "expected a string regular expression for network interface network_prefix"
            raise RuntimeError(_msg)

        ifaddr_filter.clear()
        ifaddr_filter.name_re(re.compile(re_prefix))
        ip_addrs = [ifa['addr']['addr'] for ifa in filter(ifaddr_filter, ifaddrs)]
        if not ip_addrs:
            _msg = f'No IP addresses found for {hostname} matching regex pattern: {network_prefix}'
            raise ValueError(_msg)

        ip_addr = ip_addrs[0]

        # There is a small chance (very small) that the port could appear to be available
        # and then in the next second or so, something else could grab it. In that case
        # binding to the port could fail. But this would be no different than other port
        # binding applications and on the next try it should be fine since it will then find
        # a new port. Also, predictably getting the same port on each run could be beneficial
        # for debugging. However, if randomness of port was really desired, then rand_range
        # could be used below in place of range.

        for port in range(port_range[0], port_range[1]):
            if port_check((ip_addr, port)):
                node_info = cls(state=NodeDescriptor.State.ACTIVE,
                                name=hostname,
                                host_name=hostname,
                                ip_addrs=ip_addrs,
                                host_id=get_host_id(),
                                port=port)

                return node_info

        raise RuntimeError(f'Could not find available port for IP address={ip_addr} in port range {port_range}')

    @classmethod
    def get_localservices_node_conf(cls,
                                    name: str = "",
                                    host_name: str = '',
                                    host_id: int = None,
                                    ip_addrs: Optional[list[str]] = None,
                                    shep_cd: str = '',
                                    cpu_devices: Optional[list[int]] = None,
                                    accelerators: Optional[AcceleratorDescriptor] = None):
        """Return a NodeDescriptor object for Local Services to pass into its SHChannelsUp message

        Populates the values in a NodeDescriptor object that Local Services needs to provide to the
        launcher frontend as part of infrastructure bring-up

        :param name: Name for node. Often resorts to hostname, defaults to ""
        :type name: str, optional
        :param host_name: Hostname for the node, defaults to ''
        :type host_name: str, optional
        :param host_id: unique host ID of this node, defaults to None
        :type host_id: int, optional
        :param ip_addrs: IP addresses used for backend messaging by transport agents, defaults to None
        :type ip_addrs: list[str], optional
        :param shep_cd: Channel descriptor for this node's Local Services, defaults to ''
        :type shep_cd: str, optional
        :param cpu_devices: List of CPUs and IDs on this node, defaults to None
        :type cpu_devices: list[int], optional
        :param accelerators: List of any accelerators available on this node, defaults to None
        :type accelerators: AcceleratorDescriptor, optional
        """

        from dragon.infrastructure import parameters as dparms

        return cls(state=NodeDescriptor.State.ACTIVE,
                   name=name,
                   host_name=host_name,
                   ip_addrs=ip_addrs,
                   host_id=get_host_id(),
                   shep_cd=dparms.this_process.local_shep_cd,
                   cpu_devices=list(os.sched_getaffinity(0)),
                   accelerators=find_accelerators())

    @property
    def sdesc(self):
        return self.get_sdict()

    def get_sdict(self):

        rv = {
            "state": int(self.state),
            "h_uid": self.h_uid,
            "name": self.name,
            "host_name": self.host_name,
            "is_primary": self.is_primary,
            "ip_addrs": self.ip_addrs,
            "host_id": self.host_id,
            "num_cpus": self.num_cpus,
            "physical_mem": self.physical_mem,
            "shep_cd": self.shep_cd,
            "overlay_cd": self.overlay_cd,
            "cpu_devices": self.cpu_devices
        }

        # Account for a NULL accelerator giving us a None for now
        try:
            rv["accelerators"] = self.accelerators.get_sdict()
        except AttributeError:
            rv["accelerators"] = None

        return rv

    @classmethod
    def from_sdict(cls, sdict):
        sdict["state"] = NodeDescriptor.State(sdict["state"])
        return NodeDescriptor(**sdict)
