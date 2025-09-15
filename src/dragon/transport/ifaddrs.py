from __future__ import annotations

import ctypes
import ctypes.util
import enum
import os
import re
import socket
from typing import Any, Union


libc = ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True)


class SIOCGIFFLAGS(enum.IntFlag):
    """Linux interface flags used by the SIOCGIFFLAGS ioctl(2) operation;
    see <linux/if.h> and <net/if.h>.
    """

    IFF_UP = 1 << 0  # Interface is up
    IFF_BROADCAST = 1 << 1  # Broadcast address valid
    IFF_DEBUG = 1 << 2  # Turn on debugging
    IFF_LOOPBACK = 1 << 3  # Is a loopback net
    IFF_POINTOPOINT = 1 << 4  # Interface is point-to-point link
    IFF_NOTRAILERS = 1 << 5  # Avoid use of trailers
    IFF_RUNNING = 1 << 6  # Resources allocated
    IFF_NOARP = 1 << 7  # No address resolution protocol
    IFF_PROMISC = 1 << 8  # Receive all packets
    IFF_ALLMULTI = 1 << 9  # Receive all multicast packets
    IFF_MASTER = 1 << 10  # Master of a load balancer
    IFF_SLAVE = 1 << 11  # Slave of a load balancer
    IFF_MULTICAST = 1 << 12  # Supports multicast
    IFF_PORTSEL = 1 << 13  # Can set media type
    IFF_AUTOMEDIA = 1 << 14  # Auto media select active
    IFF_DYNAMIC = 1 << 15  # Dialup device with changing addresses
    IFF_LOWER_UP = 1 << 16
    IFF_DORMANT = 1 << 17
    IFF_ECHO = 1 << 18


class EthernetProtocol(enum.IntEnum):
    """Linux ethernet protocols; see <linux/if_ether.h>.

    The definitive list of ethernet protocols is maintained by the IEEE at
    https://standards-oui.ieee.org/ethertype/eth.txt.
    """

    ETH_P_LOOP = 0x0060  # Ethernet Loopback packet
    ETH_P_PUP = 0x0200  # Xerox PUP packet
    ETH_P_PUPAT = 0x0201  # Xerox PUP Addr Trans packet
    ETH_P_TSN = 0x22F0  # TSN (IEEE 1722) packet
    ETH_P_ERSPAN2 = 0x22EB  # ERSPAN version 2 (type III)
    ETH_P_IP = 0x0800  # Internet Protocol packet
    ETH_P_X25 = 0x0805  # CCITT X.25
    ETH_P_ARP = 0x0806  # Address Resolution packet
    ETH_P_BPQ = 0x08FF  # G8BPQ AX.25 Ethernet Packet    [ NOT AN OFFICIALLY REGISTERED ID ]
    ETH_P_IEEEPUP = 0x0A00  # Xerox IEEE802.3 PUP packet
    ETH_P_IEEEPUPAT = 0x0A01  # Xerox IEEE802.3 PUP Addr Trans packet
    ETH_P_BATMAN = 0x4305  # B.A.T.M.A.N.-Advanced packet [ NOT AN OFFICIALLY REGISTERED ID ]
    ETH_P_DEC = 0x6000  # DEC Assigned proto
    ETH_P_DNA_DL = 0x6001  # DEC DNA Dump/Load
    ETH_P_DNA_RC = 0x6002  # DEC DNA Remote Console
    ETH_P_DNA_RT = 0x6003  # DEC DNA Routing
    ETH_P_LAT = 0x6004  # DEC LAT
    ETH_P_DIAG = 0x6005  # DEC Diagnostics
    ETH_P_CUST = 0x6006  # DEC Customer use
    ETH_P_SCA = 0x6007  # DEC Systems Comms Arch
    ETH_P_TEB = 0x6558  # Trans Ether Bridging
    ETH_P_RARP = 0x8035  # Reverse Addr Res packet
    ETH_P_ATALK = 0x809B  # Appletalk DDP
    ETH_P_AARP = 0x80F3  # Appletalk AARP
    ETH_P_8021Q = 0x8100  # 802.1Q VLAN Extended Header
    ETH_P_ERSPAN = 0x88BE  # ERSPAN type II
    ETH_P_IPX = 0x8137  # IPX over DIX
    ETH_P_IPV6 = 0x86DD  # IPv6 over bluebook
    ETH_P_PAUSE = 0x8808  # IEEE Pause frames. See 802.3 31B
    ETH_P_SLOW = 0x8809  # Slow Protocol. See 802.3ad 43B
    ETH_P_WCCP = 0x883E  # Web-cache coordination protocol defined in draft-wilson-wrec-wccp-v2-00.txt
    ETH_P_MPLS_UC = 0x8847  # MPLS Unicast traffic
    ETH_P_MPLS_MC = 0x8848  # MPLS Multicast traffic
    ETH_P_ATMMPOA = 0x884C  # MultiProtocol Over ATM
    ETH_P_PPP_DISC = 0x8863  # PPPoE discovery messages
    ETH_P_PPP_SES = 0x8864  # PPPoE session messages
    ETH_P_LINK_CTL = 0x886C  # HPNA, wlan link local tunnel
    ETH_P_ATMFATE = 0x8884  # Frame-based ATM Transport over Ethernet
    ETH_P_PAE = 0x888E  # Port Access Entity (IEEE 802.1X)
    ETH_P_PROFINET = 0x8892  # PROFINET
    ETH_P_REALTEK = 0x8899  # Multiple proprietary protocols
    ETH_P_AOE = 0x88A2  # ATA over Ethernet
    ETH_P_ETHERCAT = 0x88A4  # EtherCAT
    ETH_P_8021AD = 0x88A8  # 802.1ad Service VLAN
    ETH_P_802_EX1 = 0x88B5  # 802.1 Local Experimental 1.
    ETH_P_PREAUTH = 0x88C7  # 802.11 Preauthentication
    ETH_P_TIPC = 0x88CA  # TIPC
    ETH_P_LLDP = 0x88CC  # Link Layer Discovery Protocol
    ETH_P_MRP = 0x88E3  # Media Redundancy Protocol
    ETH_P_MACSEC = 0x88E5  # 802.1ae MACsec
    ETH_P_8021AH = 0x88E7  # 802.1ah Backbone Service Tag
    ETH_P_MVRP = 0x88F5  # 802.1Q MVRP
    ETH_P_1588 = 0x88F7  # IEEE 1588 Timesync
    ETH_P_NCSI = 0x88F8  # NCSI protocol
    ETH_P_PRP = 0x88FB  # IEC 62439-3 PRP/HSRv0
    ETH_P_CFM = 0x8902  # Connectivity Fault Management
    ETH_P_FCOE = 0x8906  # Fibre Channel over Ethernet
    ETH_P_IBOE = 0x8915  # Infiniband over Ethernet
    ETH_P_TDLS = 0x890D  # TDLS
    ETH_P_FIP = 0x8914  # FCoE Initialization Protocol
    ETH_P_80221 = 0x8917  # IEEE 802.21 Media Independent Handover Protocol
    ETH_P_HSR = 0x892F  # IEC 62439-3 HSRv1
    ETH_P_NSH = 0x894F  # Network Service Header
    ETH_P_LOOPBACK = 0x9000  # Ethernet loopback packet, per IEEE 802.3
    ETH_P_QINQ1 = 0x9100  # deprecated QinQ VLAN [ NOT AN OFFICIALLY REGISTERED ID ]
    ETH_P_QINQ2 = 0x9200  # deprecated QinQ VLAN [ NOT AN OFFICIALLY REGISTERED ID ]
    ETH_P_QINQ3 = 0x9300  # deprecated QinQ VLAN [ NOT AN OFFICIALLY REGISTERED ID ]
    ETH_P_EDSA = 0xDADA  # Ethertype DSA [ NOT AN OFFICIALLY REGISTERED ID ]
    ETH_P_DSA_8021Q = 0xDADB  # Fake VLAN Header for DSA [ NOT AN OFFICIALLY REGISTERED ID ]
    ETH_P_DSA_A5PSW = 0xE001  # A5PSW Tag Value [ NOT AN OFFICIALLY REGISTERED ID ]
    ETH_P_IFE = 0xED3E  # ForCES inter-FE LFB type
    ETH_P_AF_IUCV = 0xFBFB  # IBM af_iucv [ NOT AN OFFICIALLY REGISTERED ID ]

    # If the value in the ethernet type is more than this value hen the frame is
    # Ethernet II. Else it is 802.3
    ETH_P_802_3_MIN = 0x0600

    # Non DIX types. Won't clash for 1500 types.

    ETH_P_802_3 = 0x0001  # Dummy type for 802.3 frames
    ETH_P_AX25 = 0x0002  # Dummy protocol id for AX.25
    ETH_P_ALL = 0x0003  # Every packet (be careful!!!)
    ETH_P_802_2 = 0x0004  # 802.2 frames
    ETH_P_SNAP = 0x0005  # Internal only
    ETH_P_DDCMP = 0x0006  # DEC DDCMP: Internal only
    ETH_P_WAN_PPP = 0x0007  # Dummy type for WAN PPP frames
    ETH_P_PPP_MP = 0x0008  # Dummy type for PPP MP frames
    ETH_P_LOCALTALK = 0x0009  # Localtalk pseudo type
    ETH_P_CAN = 0x000C  # CAN: Controller Area Network
    ETH_P_CANFD = 0x000D  # CANFD: CAN flexible data rate
    ETH_P_CANXL = 0x000E  # CANXL: eXtended frame Length
    ETH_P_PPPTALK = 0x0010  # Dummy type for Atalk over PPP
    ETH_P_TR_802_2 = 0x0011  # 802.2 frames
    ETH_P_MOBITEX = 0x0015  # Mobitex (kaz@cafe.net)
    ETH_P_CONTROL = 0x0016  # Card specific control frames
    ETH_P_IRDA = 0x0017  # Linux-IrDA
    ETH_P_ECONET = 0x0018  # Acorn Econet
    ETH_P_HDLC = 0x0019  # HDLC frames
    ETH_P_ARCNET = 0x001A  # 1A for ArcNet :-)
    ETH_P_DSA = 0x001B  # Distributed Switch Arch.
    ETH_P_TRAILER = 0x001C  # Trailer switch tagging
    ETH_P_PHONET = 0x00F5  # Nokia Phonet frames
    ETH_P_IEEE802154 = 0x00F6  # IEEE802.15.4 frame
    ETH_P_CAIF = 0x00F7  # ST-Ericsson CAIF protocol
    ETH_P_XDSA = 0x00F8  # Multiplexed DSA protocol
    ETH_P_MAP = 0x00F9  # Qualcomm multiplexing and aggregation protocol
    ETH_P_MCTP = 0x00FA  # Management component transport protocol packets


class ARPHardware(enum.IntEnum):
    ARPHRD_NETROM = 0  # from KA9Q: NET/ROM pseudo
    ARPHRD_ETHER = 1  # Ethernet 10Mbps
    ARPHRD_EETHER = 2  # Experimental Ethernet
    ARPHRD_AX25 = 3  # AX.25 Level 2
    ARPHRD_PRONET = 4  # PROnet token ring
    ARPHRD_CHAOS = 5  # Chaosnet
    ARPHRD_IEEE802 = 6  # IEEE 802.2 Ethernet/TR/TB
    ARPHRD_ARCNET = 7  # ARCnet
    ARPHRD_APPLETLK = 8  # APPLEtalk
    ARPHRD_DLCI = 15  # Frame Relay DLCI
    ARPHRD_ATM = 19  # ATM
    ARPHRD_METRICOM = 23  # Metricom STRIP (new IANA id)
    ARPHRD_IEEE1394 = 24  # IEEE 1394 IPv4 - RFC 2734
    ARPHRD_EUI64 = 27  # EUI-64
    ARPHRD_INFINIBAND = 32  # InfiniBand

    # Dummy types for non ARP hardware
    ARPHRD_SLIP = 256
    ARPHRD_CSLIP = 257
    ARPHRD_SLIP6 = 258
    ARPHRD_CSLIP6 = 259
    ARPHRD_RSRVD = 260  # Notional KISS type
    ARPHRD_ADAPT = 264
    ARPHRD_ROSE = 270
    ARPHRD_X25 = 271  # CCITT X.25
    ARPHRD_HWX25 = 272  # Boards with X.25 in firmware
    ARPHRD_CAN = 280  # Controller Area Network
    ARPHRD_MCTP = 290
    ARPHRD_PPP = 512
    ARPHRD_CISCO = 513  # Cisco HDLC
    ARPHRD_HDLC = ARPHRD_CISCO
    ARPHRD_LAPB = 516  # LAPB
    ARPHRD_DDCMP = 517  # Digital's DDCMP protocol
    ARPHRD_RAWHDLC = 518  # Raw HDLC
    ARPHRD_RAWIP = 519  # Raw IP

    ARPHRD_TUNNEL = 768  # IPIP tunnel
    ARPHRD_TUNNEL6 = 769  # IP6IP6 tunnel
    ARPHRD_FRAD = 770  # Frame Relay Access Device
    ARPHRD_SKIP = 771  # SKIP vif
    ARPHRD_LOOPBACK = 772  # Loopback device
    ARPHRD_LOCALTLK = 773  # Localtalk device
    ARPHRD_FDDI = 774  # Fiber Distributed Data Interface
    ARPHRD_BIF = 775  # AP1000 BIF
    ARPHRD_SIT = 776  # sit0 device - IPv6-in-IPv4
    ARPHRD_IPDDP = 777  # IP over DDP tunneller
    ARPHRD_IPGRE = 778  # GRE over IP
    ARPHRD_PIMREG = 779  # PIMSM register interface
    ARPHRD_HIPPI = 780  # High Performance Parallel Interface
    ARPHRD_ASH = 781  # Nexus 64Mbps Ash
    ARPHRD_ECONET = 782  # Acorn Econet
    ARPHRD_IRDA = 783  # Linux-IrDA
    # ARP works differently on different FC media .. so
    ARPHRD_FCPP = 784  # Point to point fibrechannel
    ARPHRD_FCAL = 785  # Fibrechannel arbitrated loop
    ARPHRD_FCPL = 786  # Fibrechannel public loop
    ARPHRD_FCFABRIC = 787  # Fibrechannel fabric
    # 787->799 reserved for fibrechannel media types
    ARPHRD_IEEE802_TR = 800  # Magic type ident for TR
    ARPHRD_IEEE80211 = 801  # IEEE 802.11
    ARPHRD_IEEE80211_PRISM = 802  # IEEE 802.11 + Prism2 header
    ARPHRD_IEEE80211_RADIOTAP = 803  # IEEE 802.11 + radiotap header
    ARPHRD_IEEE802154 = 804
    ARPHRD_IEEE802154_MONITOR = 805  # IEEE 802.15.4 network monitor

    ARPHRD_PHONET = 820  # PhoNet media type
    ARPHRD_PHONET_PIPE = 821  # PhoNet pipe header
    ARPHRD_CAIF = 822  # CAIF media type
    ARPHRD_IP6GRE = 823  # GRE over IPv6
    ARPHRD_NETLINK = 824  # Netlink header
    ARPHRD_6LOWPAN = 825  # IPv6 over LoWPAN
    ARPHRD_VSOCKMON = 826  # Vsock monitor header

    ARPHRD_VOID = 0xFFFF  # Void type, nothing is known
    ARPHRD_NONE = 0xFFFE  # zero header length


class PacketType(enum.IntEnum):
    PACKET_HOST = 0  # To us
    PACKET_BROADCAST = 1  # To all
    PACKET_MULTICAST = 2  # To group
    PACKET_OTHERHOST = 3  # To someone else
    PACKET_OUTGOING = 4  # Outgoing of any type
    PACKET_LOOPBACK = 5  # MC/BRD frame looped back
    PACKET_USER = 6  # To user space
    PACKET_KERNEL = 7  # To kernel space
    # Unused, PACKET_FASTROUTE and PACKET_LOOPBACK are invisible to user space
    PACKET_FASTROUTE = 6  # Fastrouted frame


class sockaddr_in(ctypes.Structure):
    _fields_ = [
        ("sin_family", ctypes.c_ushort),  # Always AF_INET
        ("sin_port", ctypes.c_uint16),
        ("sin_addr", ctypes.c_byte * 4),
        ("sin_zero", ctypes.c_byte * 8),  # Ignored
    ]

    def asdict(self) -> dict[str, Any]:
        return {
            "family": socket._intenum_converter(self.sin_family, socket.AddressFamily),
            "port": int(self.sin_port),
            "addr": socket.inet_ntop(self.sin_family, self.sin_addr),
        }


class sockaddr_in6(ctypes.Structure):
    _fields_ = [
        ("sin6_family", ctypes.c_ushort),  # Always AF_INET6
        ("sin6_port", ctypes.c_uint16),
        ("sin6_flowinfo", ctypes.c_uint32),
        ("sin6_addr", ctypes.c_byte * 16),
        ("sin6_scope_id", ctypes.c_uint32),
    ]

    def asdict(self) -> dict[str, Any]:
        return {
            "family": socket._intenum_converter(self.sin6_family, socket.AddressFamily),
            "port": int(self.sin6_port),
            "flowinfo": int(self.sin6_flowinfo),
            "addr": socket.inet_ntop(self.sin6_family, self.sin6_addr),
            "scope_id": int(self.sin6_scope_id),
        }


class sockaddr_ll(ctypes.Structure):
    _fields_ = [
        ("sll_family", ctypes.c_ushort),  # Always AF_PACKET
        ("sll_protocol", ctypes.c_ushort),  # Physical-layer protocol
        ("sll_ifindex", ctypes.c_int),  # Interface number
        ("sll_hatype", ctypes.c_ushort),  # ARP hardware type
        ("sll_pkttype", ctypes.c_ubyte),  # Packet type
        ("sll_halen", ctypes.c_ubyte),  # Length of address
        ("sll_addr", ctypes.c_ubyte * 8),  # Physical-layer address
    ]

    def asdict(self) -> dict[str, Any]:
        return {
            "family": socket._intenum_converter(self.sll_family, socket.AddressFamily),
            "protocol": socket._intenum_converter(socket.ntohs(self.sll_protocol), EthernetProtocol),
            "ifindex": int(self.sll_ifindex),
            "hatype": socket._intenum_converter(self.sll_hatype, ARPHardware),
            "pkttype": socket._intenum_converter(self.sll_pkttype, PacketType),
            "halen": int(self.sll_halen),
            "addr": bytes(self.sll_addr)[: self.sll_halen],
        }


class sockaddr(ctypes.Structure):
    _fields_ = [
        ("sa_family", ctypes.c_ushort),
        ("sa_data", ctypes.c_byte * 14),
    ]

    af_type = {
        socket.AF_INET: ctypes.POINTER(sockaddr_in),
        socket.AF_INET6: ctypes.POINTER(sockaddr_in6),
        socket.AF_PACKET: ctypes.POINTER(sockaddr_ll),
    }

    def cast(self) -> Union[sockaddr_in, sockaddr_in6, sockaddr_ll]:
        af_type = self.af_type.get(self.sa_family)
        if af_type is None:
            return self
        return ctypes.cast(ctypes.addressof(self), af_type).contents

    def asdict(self) -> dict[str, Any]:
        return {
            "family": socket.AddressFamily(self.sa_family),
            "data": bytes(self.sa_data),
        }


class ifa_ifu(ctypes.Union):
    _fields_ = [
        ("ifu_broadaddr", ctypes.POINTER(sockaddr)),  # broadcast address
        ("ifu_dstaddr", ctypes.POINTER(sockaddr)),  # other end of link
    ]


class ifaddrs(ctypes.Structure):
    _anonymous_ = ("ifa_ifu",)

    def __iter__(self):
        yield self
        p = self
        while p.ifa_next:
            p = p.ifa_next.contents
            yield p

    def asdict(self) -> dict[str, Any]:
        ifa = {
            "name": self.ifa_name.decode("utf-8"),
            "flags": SIOCGIFFLAGS(int(self.ifa_flags)),
        }

        addr = self.ifa_addr.contents if self.ifa_addr else None
        if addr is not None:
            ifa["addr"] = addr.cast().asdict()

        if self.ifa_netmask:
            netmask = self.ifa_netmask.contents
            if not netmask.sa_family and addr is not None:
                netmask.sa_family = addr.sa_family
            ifa["netmask"] = netmask.cast().asdict()

        if self.ifa_flags & SIOCGIFFLAGS.IFF_BROADCAST:
            ifa["broadaddr"] = None
            if self.ifu_broadaddr:
                broadaddr = self.ifu_broadaddr.contents
                if not broadaddr.sa_family and addr is not None:
                    broadaddr.sa_family = addr.sa_family
                ifa["broadaddr"] = broadaddr.cast().asdict()

        elif self.ifa_flags & SIOCGIFFLAGS.IFF_POINTOPOINT:
            ifa["dstaddr"] = None
            if self.ifu_dstaddr:
                dstaddr = self.ifu_dstaddr.contents
                if not dstaddr.sa_family and addr is not None:
                    dstaddr.sa_family = addr.sa_family
                ifa["dstaddr"] = dstaddr.cast().asdict()

        # TODO Support data field?
        return ifa


ifaddrs._fields_ = [
    ("ifa_next", ctypes.POINTER(ifaddrs)),  # Pointer to next struct
    ("ifa_name", ctypes.c_char_p),  # Interface name
    ("ifa_flags", ctypes.c_uint),  # Interface flags
    ("ifa_addr", ctypes.POINTER(sockaddr)),  # Interface address
    ("ifa_netmask", ctypes.POINTER(sockaddr)),  # Intrfacee netmask
    ("ifa_ifu", ifa_ifu),
    ("ifa_data", ctypes.c_void_p),  # Address specific data
]


def getifaddrs() -> list[dict]:
    """Returns a list of dicts that mirror an ifaddrs structure.

    NOTE: Dict items corresponding to sockaddr pointer types in the ifaddrs
    structure (i.e., addr, netmask, broadaddr, dstaddr) will only be present if
    they were set by getifaddrs(3). Do not assume they exist! Nested sockaddr
    mappings contain items specific family, though values have been coerced
    into more appropriate types (e.g., a packed IPv4 address is returned as a
    string in dotted notation). Also, address specific data (i.e., ifa_data) is
    not currently exposed.
    """
    ifap = ctypes.POINTER(ifaddrs)()
    if libc.getifaddrs(ctypes.byref(ifap)) < 0:
        errno = ctypes.get_errno()
        raise OSError(errno, os.strerror(errno))
    try:
        # TODO Convert to ipaddress.IPv4Interface or ipaddress.IPv6Interface
        return [ifa.asdict() for ifa in iter(ifap.contents)]
    finally:
        libc.freeifaddrs(ifap)


class InterfaceAddressFilter:

    def __init__(self, *filters):
        self.filters = list(filters)

    def clear(self):
        self.filters.clear()

    def __call__(self, ifa: dict[str, Any]) -> bool:
        return all(f(ifa) for f in self.filters)

    def require_flags(self, flags: SIOCGIFFLAGS, default: SIOCGIFFLAGS = SIOCGIFFLAGS(0)) -> InterfaceAddressFilter:
        """Include interfaces with all specified flags set."""

        def require_flags_filter(ifa):
            return ifa.get("flags", default) & flags == flags

        self.filters.append(require_flags_filter)
        return self

    def any_flags(self, flags: SIOCGIFFLAGS, default: SIOCGIFFLAGS = SIOCGIFFLAGS(0)) -> InterfaceAddressFilter:
        """Include interfaces with any specified flags set."""

        def any_flags_filter(ifa):
            return ifa.get("flags", default) & flags != 0

        self.filters.append(any_flags_filter)
        return self

    def exclude_flags(self, flags: SIOCGIFFLAGS, default: SIOCGIFFLAGS = SIOCGIFFLAGS(0)) -> InterfaceAddressFilter:
        """Exclude interfaces with any specified flags set."""

        def exclude_flags_filter(ifa):
            return ifa.get("flags", default) & flags == 0

        self.filters.append(exclude_flags_filter)
        return self

    def family(self, *args: socket.AddressFamily) -> InterfaceAddressFilter:
        """Include interfaces with any of the specified address families."""
        if not args:
            return self
        family = set(args)

        def family_filter(ifa):
            addr = ifa.get("addr")
            if addr is None:
                return False
            return addr.get("family") in family

        self.filters.append(family_filter)
        return self

    def name_re(self, *patterns: re.Pattern) -> InterfaceAddressFilter:
        """Include interfaces with a name that matches any of the specified
        regex patterns.
        """
        if not patterns:
            return self

        def name_re_filter(ifa):
            name = ifa.get("name", "")
            return any(p.match(name) is not None for p in patterns)

        self.filters.append(name_re_filter)
        return self

    def name_prefix(self, *prefixes: str) -> InterfaceAddressFilter:
        """Include interfaces with a names that starts with any of the
        specified prefixes.
        """
        if not prefixes:
            return self

        def name_prefix_filter(ifa):
            name = ifa.get("name", "")
            return any(name.startswith(p) for p in prefixes)

        self.filters.append(name_prefix_filter)
        return self

    def up_and_running(self) -> InterfaceAddressFilter:
        return self.require_flags(SIOCGIFFLAGS.IFF_UP | SIOCGIFFLAGS.IFF_RUNNING)

    def no_loopback(self) -> InterfaceAddressFilter:
        return self.exclude_flags(SIOCGIFFLAGS.IFF_LOOPBACK)

    def af_inet(self, inet6: bool = True, loopback: bool = False) -> InterfaceAddressFilter:
        family = {socket.AddressFamily.AF_INET}
        if inet6:
            family.add(socket.AddressFamily.AF_INET6)
        f = self.family(*family)
        if not loopback:
            f = f.no_loopback()
        return f

    def high_speed_if_names(self) -> InterfaceAddressFilter:
        return self.name_prefix("hsn", "ipogif", "ib")


def _json_preprocess(obj):
    if isinstance(obj, (list, tuple)):
        return [_json_preprocess(item) for item in obj]
    if isinstance(obj, dict):
        return {_json_preprocess(k): _json_preprocess(v) for k, v in obj.items()}
    if isinstance(obj, (bytes, bytearray)):
        return ":".join(f"{b:02x}" for b in obj)
    if isinstance(obj, enum.IntFlag):
        return str(obj).removeprefix(obj.__class__.__name__).lstrip(".").split("|")
    if isinstance(obj, enum.IntEnum):
        return obj.name
    return obj


def main(args=None):
    import argparse
    from functools import reduce
    import json
    import operator

    parser = argparse.ArgumentParser(
        prog="ifaddrs",
        description="Get network interface addresses (Linux only).",
        epilog="""
            Output is a JSON encoded list of objects corresponding to an
            ifaddrs structure; see getifaddr(3) for more information.
        """,
    )
    parser.add_argument(
        "prefix",
        metavar="PREFIX",
        nargs="*",
        help="show interfaces with names matching prefix",
    )
    parser.add_argument(
        "-n",
        "--name",
        metavar="REGEX",
        action="append",
        default=[],
        help="show interfaces with names matching regex pattern",
    )

    addr_group = parser.add_argument_group("filter on address")
    addr_group.add_argument(
        "-f",
        "--family",
        metavar="FAMILY",
        choices=set(socket.AddressFamily.__members__),
        action="append",
        default=[],
        help="socket address family: %(choices)s",
    )
    addr_group.add_argument(
        "--ip",
        action="store_true",
        default=False,
        help="show interfaces with IPv4 addresses, same as -f AF_INET",
    )
    addr_group.add_argument(
        "--ip6",
        action="store_true",
        default=False,
        help="show interfaces with IPv6 addresses, same as -f AF_INET6",
    )

    _flags = set(SIOCGIFFLAGS.__members__)
    flags_group = parser.add_argument_group(
        "filter on flags",
        description=f"Available SIOCGIFFLAGS flags: {', '.join(_flags)}",
    )
    flags_group.add_argument(
        "-r",
        "--require-flags",
        metavar="FLAG",
        choices=_flags,
        action="append",
        default=[],
        help="all flags required to be set",
    )
    flags_group.add_argument(
        "-a",
        "--any-flags",
        metavar="FLAG",
        choices=_flags,
        action="append",
        default=[],
        help="any flags which may be set",
    )
    flags_group.add_argument(
        "-x",
        "--exclude-flags",
        metavar="FLAG",
        choices=_flags,
        action="append",
        default=[],
        help="all flags that must not be set",
    )
    flags_group.add_argument(
        "--up",
        action="store_true",
        default=False,
        help="show up interfaces, same as -r IFF_UP",
    )
    flags_group.add_argument(
        "--running",
        action="store_true",
        default=False,
        help="show running interfaces, same as -r IFF_RUNNING",
    )
    flags_group.add_argument(
        "--no-loopback",
        dest="loopback",
        action="store_false",
        default=True,
        help="exclude loopback interfaces, same as -x IFF_LOOPBACK",
    )

    args = parser.parse_args(args)

    args.family = {getattr(socket.AddressFamily, af) for af in args.family}
    if args.ip:
        args.family.add(socket.AddressFamily.AF_INET)
    if args.ip6:
        args.family.add(socket.AddressFamily.AF_INET6)

    args.require_flags = reduce(
        operator.or_,
        {getattr(SIOCGIFFLAGS, f) for f in args.require_flags},
        SIOCGIFFLAGS(0),
    )
    args.any_flags = reduce(
        operator.or_,
        {getattr(SIOCGIFFLAGS, f) for f in args.any_flags},
        SIOCGIFFLAGS(0),
    )
    args.exclude_flags = reduce(
        operator.or_,
        {getattr(SIOCGIFFLAGS, f) for f in args.exclude_flags},
        SIOCGIFFLAGS(0),
    )
    if args.up:
        args.require_flags |= SIOCGIFFLAGS.IFF_UP
    if args.running:
        args.require_flags |= SIOCGIFFLAGS.IFF_RUNNING
    if not args.loopback:
        args.exclude_flags |= SIOCGIFFLAGS.IFF_LOOPBACK

    ifaddr_filter = InterfaceAddressFilter()
    if args.family:
        ifaddr_filter.family(*args.family)
    if args.require_flags:
        ifaddr_filter.require_flags(args.require_flags)
    if args.any_flags:
        ifaddr_filter.any_flags(args.any_flags)
    if args.exclude_flags:
        ifaddr_filter.exclude_flags(args.exclude_flags)
    if args.name:
        ifaddr_filter.name_re(*args.name)
    if args.prefix:
        ifaddr_filter.name_prefix(*args.prefix)

    ifaddrs = list(filter(ifaddr_filter, getifaddrs()))
    print(json.dumps(_json_preprocess(ifaddrs)))


if __name__ == "__main__":
    main()
