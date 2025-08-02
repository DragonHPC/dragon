"""A class that uniquely defines properties of a managed hardware node for infrastructure communication.

This is a stub at the moment, and should be extended for dynamic hardware
allocations/elasticity aka The Cloud (TM) and more complex node properties
(accelerators, networks, encryption).
"""

import enum
import re
import os
import sys
import json
import tempfile
from socket import gethostname
from subprocess import CalledProcessError
from typing import Optional
from datetime import datetime

from . import parameters as dparms

from .facts import DEFAULT_TRANSPORT_NETIF, DEFAULT_OVERLAY_NETWORK_PORT, DEFAULT_PORT_RANGE
from ..utils import host_id as get_host_id
from .util import port_check
from ..transport.util import get_fabric_ep_addrs
from .gpu_desc import AcceleratorDescriptor, find_accelerators
from typing import Union, Tuple


class NodeDescriptor:
    """Globally available description of a 'managed node'."""

    @enum.unique
    class State(enum.IntEnum):
        NULL = enum.auto()
        DISCOVERABLE = enum.auto()
        PENDING = enum.auto()
        ACTIVE = enum.auto()
        IDLE = enum.auto()
        ERROR = enum.auto()
        DOWN = enum.auto()

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
        fabric_ep_addrs_available: bool = False,
        fabric_ep_addrs: Optional[list[str]] = None,
        fabric_ep_addr_lens: Optional[list[int]] = None,
        port: int = None,
        num_cpus: int = 0,
        physical_mem: int = 0,
        is_primary: bool = False,
        host_id: int = None,
        shep_cd: str = "",
        overlay_cd: str = "",
        host_name: str = "",
        cpu_devices: Optional[list[int]] = None,
        accelerators: Optional[AcceleratorDescriptor] = None,
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
        # but it gives some manner of tracking for block application
        # TODO: This might be useful later when we can GC finished policy jobs
        self.num_policies = 0

        if port is not None:
            self.ip_addrs = [f"{ip_addr}:{port}" for ip_addr in ip_addrs]
        else:
            self.ip_addrs = ip_addrs

        if fabric_ep_addrs is None and not os.getenv("DRAGON_HSTA_NO_NET_CONFIG", True):
            num_nics = len(ip_addrs)
            self.fabric_ep_addrs_available, self.fabric_ep_addrs, self.fabric_ep_addr_lens = get_fabric_ep_addrs(
                num_nics, False
            )
        else:
            self.fabric_ep_addrs_available = fabric_ep_addrs_available
            self.fabric_ep_addrs = fabric_ep_addrs
            self.fabric_ep_addr_lens = fabric_ep_addr_lens

    def __repr__(self) -> str:
        return f"name:{self.name}, host_id:{self.host_id} at {self.ip_addrs}, state:{self.state.name}"

    def __str__(self):
        return f"name:{self.name}, host_id:{self.host_id} at {self.ip_addrs}, state:{self.state.name}"

    @classmethod
    def get_localservices_node_conf(
        cls,
        name: Optional[str] = None,
        host_name: Optional[str] = None,
        host_id: Optional[int] = None,
        is_primary: bool = False,
        ip_addrs: Optional[list[str]] = None,
        shep_cd: Optional[str] = None,
        cpu_devices: Optional[list[int]] = None,
        accelerators: Optional[AcceleratorDescriptor] = None,
    ):
        """Return a NodeDescriptor object for Local Services to pass into its SHChannelsUp message

        Populates the values in a NodeDescriptor object that Local Services needs to provide to the
        launcher frontend as part of infrastructure bring-up


        :param name: Name for node. Often resorts to hostname, defaults to None
        :type name: Optional[str], optional
        :param host_name: Hostname for the node, defaults to gethostname()
        :type host_name: Optional[str], optional
        :param host_id: unique host ID of this node, defaults to get_host_id()
        :type host_id: Optional[int], optional
        :param is_primary: denote if this is the primary node running GS, defaults to False
        :type is_primary: bool, optional
        :param ip_addrs: IP addresses used for backend messaging by transport agents, defaults to ["127.0.0.1"]
        :type ip_addrs: Optional[list[str]], optional
        :param shep_cd: Channel descriptor for this node's Local Services, defaults to None
        :type shep_cd: Optional[str], optional
        :param cpu_devices: List of CPUs and IDs on this node, defaults to list(os.sched_getaffinity(0))
        :type cpu_devices: Optional[list[int]], optional
        :param accelerators: List of any accelerators available on this node, defaults to find_accelerators()
        :type accelerators: Optional[AcceleratorDescriptor], optional
        """

        from dragon.infrastructure import parameters as dparms

        if host_name is None:
            host_name = gethostname()

        if host_id is None:
            host_id = get_host_id()

        if cpu_devices is None:
            cpu_devices = list(os.sched_getaffinity(0))

        if accelerators is None:
            accelerators = find_accelerators()

        if ip_addrs is None:
            ip_addrs = ["127.0.0.1"]

        state = cls.State.ACTIVE

        if name is None:
            name = f"Node-{host_id}"

        if shep_cd is None:
            shep_cd = dparms.this_process.local_shep_cd

        num_cpus = os.cpu_count()
        physical_mem = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")

        num_nics = len(ip_addrs)
        if not os.getenv("DRAGON_HSTA_NO_NET_CONFIG", True):
            get_fabric_ep_addrs(num_nics, os.environ.get("DRAGON_HSTA_GET_FABRIC", "True").lower() == "true")

        return cls(
            state=state,
            name=name,
            host_name=host_name,
            ip_addrs=ip_addrs,
            host_id=host_id,
            shep_cd=shep_cd,
            is_primary=is_primary,
            num_cpus=num_cpus,
            physical_mem=physical_mem,
            cpu_devices=cpu_devices,
            accelerators=accelerators,
        )

    @classmethod
    def get_local_network_conf_cache_file_name(cls):
        tmp_dir = tempfile.gettempdir()
        host_id = str(get_host_id())

        dragon_tmp_dir = os.path.join(tmp_dir, ".dragon")
        os.makedirs(dragon_tmp_dir, exist_ok=True)

        return os.path.join(dragon_tmp_dir, host_id)

    @classmethod
    def remove_cached_local_network_conf(cls):
        try:
            os.remove(cls.get_local_network_conf_cache_file_name())
        except (FileNotFoundError, IOError):
            pass

    @classmethod
    def get_cached_local_network_conf(cls):
        try:
            with open(cls.get_local_network_conf_cache_file_name(), "r") as inf:
                json_str = inf.read()
                json_data = json.loads(json_str)

                cache_key = next(iter(json_data))
                cache_time = datetime.fromisoformat(cache_key)

                now = datetime.now()
                time_diff = now - cache_time
                if time_diff.total_seconds() < dparms.this_process.net_conf_cache_timeout:
                    return True, cls.from_sdict(json_data[cache_key])

                return True, None
        except (FileNotFoundError, IOError):
            return False, None

    @classmethod
    def set_cached_local_network_conf(cls, node_info):
        try:
            with open(cls.get_local_network_conf_cache_file_name(), "w") as outf:
                now = datetime.now()
                json_str = json.dumps({now.isoformat(): node_info.sdesc})
                outf.write(json_str)
        except IOError:
            pass

    @classmethod
    def get_local_node_network_conf(
        cls,
        network_prefix: str = DEFAULT_TRANSPORT_NETIF,
        port_range: Union[Tuple[int, int], int] = (
            DEFAULT_OVERLAY_NETWORK_PORT,
            DEFAULT_OVERLAY_NETWORK_PORT + DEFAULT_PORT_RANGE,
        ),
    ):
        """Return NodeDescriptor with IP for given network prefix, hostname, and host ID

        :param network_prefix: network prefix used to find IP address of this node. Defaults to DEFAULT_TRANSPORT_NETIF
        :type network_prefix: str, optional

        :param port_range: Port and port range to use for communication. Defaults to (DEFAULT_OVERLAY_NETWORK_PORT, DEFAULT_OVERLAY_NETWORK_PORT+DEFAULT_PORT_RANGE)
            If just the port is passed as an int, the range will be assumed to be DEFAULT_PORT_RANGE
        :type port_range: tuple[int, int] | int, optional

        :return: Filled with local network info for node of execution
        :rtype: NodeDescriptor
        """
        from dragon.transport.ifaddrs import getifaddrs, InterfaceAddressFilter

        if type(port_range) is int:
            port_range = (port_range, port_range + DEFAULT_PORT_RANGE)
        hostname = gethostname()
        ifaddr_filter = InterfaceAddressFilter()
        ifaddr_filter.af_inet(inet6=False)  # Disable IPv6 for now
        ifaddr_filter.up_and_running()
        try:
            ifaddrs = list(filter(ifaddr_filter, getifaddrs()))
        except OSError:
            raise
        if not ifaddrs:
            _msg = "No network interface with an AF_INET address was found"
            raise RuntimeError(_msg)

        try:
            re_prefix = re.compile(network_prefix)
        except (TypeError, NameError):
            _msg = "expected a string regular expression for network interface network_prefix"
            raise RuntimeError(_msg)

        ifaddr_filter.clear()
        ifaddr_filter.name_re(re.compile(re_prefix))
        all_ifs = list(filter(ifaddr_filter, ifaddrs))
        non_eth_ifs = [ifa for ifa in all_ifs if not ifa["name"].startswith("eth")]

        # We check for high-speed interfaces, other than eth, here and if we have them
        # then eliminate the eth interfaces since they would be slower. However, if no
        # other interfaces are available, then use the eth interface.
        if len(non_eth_ifs) > 0:
            filtered_ifs = non_eth_ifs
        else:
            filtered_ifs = all_ifs

        ip_addrs = [ifa["addr"]["addr"] for ifa in filtered_ifs]
        if not ip_addrs:
            _msg = f"No NICs found for {hostname} with regex pattern {network_prefix}"
            print(_msg, file=sys.stderr, flush=True)
            node_info = cls(
                state=NodeDescriptor.State.ACTIVE,
                name=hostname,
                host_name=hostname,
                ip_addrs=ip_addrs,
                host_id=get_host_id(),
                port=port_range[0],
            )
            return node_info
        else:
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
                node_info = cls(
                    state=NodeDescriptor.State.ACTIVE,
                    name=hostname,
                    host_name=hostname,
                    ip_addrs=ip_addrs,
                    host_id=get_host_id(),
                    port=port,
                )

                return node_info

        raise RuntimeError(f"Could not find available port for IP address={ip_addr} in port range {port_range}")

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
            "fabric_ep_addrs_available": self.fabric_ep_addrs_available,
            "fabric_ep_addrs": self.fabric_ep_addrs,
            "fabric_ep_addr_lens": self.fabric_ep_addr_lens,
            "host_id": self.host_id,
            "num_cpus": self.num_cpus,
            "physical_mem": self.physical_mem,
            "shep_cd": self.shep_cd,
            "overlay_cd": self.overlay_cd,
            "cpu_devices": self.cpu_devices,
            "state": self.state.value,
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
        try:
            if sdict["accelerators"] is not None:
                sdict["accelerators"] = AcceleratorDescriptor.from_sdict(sdict["accelerators"])
        except KeyError:
            sdict["accelerators"] = None

        return NodeDescriptor(**sdict)
