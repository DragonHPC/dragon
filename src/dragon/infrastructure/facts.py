"""Specified constants and names used in the Dragon runtime."""

import os
import shlex
import enum
import socket
import sys
from .. import dtypes
from pathlib import Path

PREFIX = "DRAGON_"

# Number of gateway channels per node
DRAGON_DEFAULT_NUM_GW_CHANNELS_PER_NODE = 0
DRAGON_OVERLAY_DEFAULT_NUM_GW_CHANNELS_PER_NODE = 1

# These three constants are repeated within C code. Any change
# here requires a change in globals.h as well.
NUM_GW_CHANNELS_PER_NODE_VAR = "NUM_GW_CHANNELS_PER_NODE"
DEFAULT_PD_VAR = "DEFAULT_PD"
INF_PD_VAR = "INF_PD"

NUM_GW_TYPES = 3
# needed for naming convention for the gateway channels
# should be unique on a given node, not globally
# Note: there is also a dependency on dragon_channel_register_gateways_from_env()
GW_ENV_PREFIX = PREFIX + "GW"

DEFAULT_HSTA_TRANSPORT_TYPE = "p2p"

OOM_WARNING_PCT_VAR = "MEMORY_WARNING_PCT"
OOM_WARNING_BYTES_VAR = "MEMORY_WARNING_BYTES"
OOM_CRITICAL_PCT_VAR = "MEMORY_CRITICAL_PCT"
OOM_CRITICAL_BYTES_VAR = "MEMORY_CRITICAL_BYTES"
QUIET_VAR = "WARNINGS_OFF"

OOM_DEFAULT_WARN_PCT = 10
OOM_DEFAULT_WARN_BYTES = 6 * 1024 * 1024 * 1024 # 6GB
OOM_DEFAULT_CRITICAL_PCT = 2
OOM_DEFAULT_CRITICAL_BYTES = 600 * 1024 * 1024 # 600MB

# For environment variable passing, this set is the list of dragon parameters
# in capital letters.
env_vars = frozenset(
    {
        "MODE",
        "INDEX",
        DEFAULT_PD_VAR,
        INF_PD_VAR,
        "LOCAL_SHEP_CD",
        "LOCAL_BE_CD",
        "GS_RET_CD",
        "SHEP_RET_CD",
        "GS_CD",
        "DEFAULT_SEG_SZ",
        "INF_SEG_SZ",
        "TEST",
        "DEBUG",
        "MY_PUID",
        "BE_CUID",
        "INF_WAIT_MODE",
        "USER_WAIT_MODE",
        "USER_RETURN_WHEN_MODE",
        "INF_RETURN_WHEN_MODE",
        "GW_CAPACITY",
        NUM_GW_CHANNELS_PER_NODE_VAR,
        "TRANSPORT_AGENT",
        "HSTA_MAX_EJECTION_MB",
        "HSTA_MAX_GETMSG_MB",
        "HSTA_FABRIC_BACKEND",
        "HSTA_TRANSPORT_TYPE",
        "PMOD_COMMUNICATION_TIMEOUT",
        "BASEPOOL",
        "OVERLAY_FANOUT",
        "NET_CONF_CACHE",
        OOM_WARNING_PCT_VAR,
        OOM_WARNING_BYTES_VAR,
        OOM_CRITICAL_PCT_VAR,
        OOM_CRITICAL_BYTES_VAR,
        QUIET_VAR,
    }
)


# TODO:PE-37770  This list of names should NOT need to appear
# both here and in the parameters module.


# Get environment variable name from parameter name. Conversion routine used by the parameters module.
def env_name(parameter_name: str) -> str:
    if parameter_name not in env_vars:
        raise ValueError(f"{parameter_name} is not a known parameter")

    return PREFIX + parameter_name


# This sets the environment variable names as attributes of this module. So for instance, ALLOC_SZ is a
# variable name of this module with value 'DRAGON_ALLOC_SZ'. These are the environment variable names only
# not their values, which can be accessed through the parameters module.
for var_name in env_vars:
    setattr(sys.modules[__name__], var_name, var_name)
    setattr(sys.modules[__name__], "ENV_" + var_name, env_name(var_name))

# Mode Values
CRAYHPE_MULTI_MODE = "crayhpe-multi"
SINGLE_MODE = "single"
TEST_MODE = "test"

# Pool Values
PATCHED = "PATCHED"
NATIVE = "NATIVE"
DEFAULT_POOL = NATIVE

# This number is set so that cuids can be divided between local
# services instances for the purpose of doling out process local
# channels. This allows a little more than 16 million nodes in the
# dragon run-time instance.
MAX_NODES_POW = 24
MAX_NODES = 2**MAX_NODES_POW

#: Input channel unique ID for Global Services head
GS_INPUT_CUID = 2

#: Unique process ID for head user process started by the launcher
LAUNCHER_PUID = 0
#: Unique process ID given for the global services process
GS_PUID = 1

#: The first PUID for user applications launched by local services
FIRST_PUID = 2**32

#: Base index used by the transport services for the unique process ID (puid)
FIRST_TRANSPORT_PUID = 2**8
#: Range of the unique process ID's for the transport services
RANGE_TRANSPORT_PUID = FIRST_PUID - FIRST_TRANSPORT_PUID

#: Starting value for PMOD launch Channel ID (cuid) that
#: communicates with the client pmod at process launch
BASE_PMOD_LAUNCH_CUID = 2**54

#: Range for the PMOD launch channel ID's (cuid) that
#: communicates with the client pmod at process launch
RANGE_PMOD_LAUNCH_CUID = 2**54

# needed by the frontend for its infrastructure channels
FE_CUID = 2**55 - 1
FE_GW_CUID = 2**55 - 2
FE_LOCAL_IN_CUID = 2**55 - 3
FE_LOCAL_OUT_CUID = 2**55 - 4

#: Starting value of a reserved range of cuids that can be managed by a user
BASE_USER_MANAGED_CUID = 2**54
#: Range for the user managed cuids
RANGE_USER_MANAGED_CUID = 2**54

#: Starting value of the local transport agent channel unique ID (cuid)
BASE_TA_CUID = 2**55
#: Range for the local transport agent channel ID's (cuid)
RANGE_TA_CUID = 2**55

#: Starting value for the backend channel ID (cuid) that
#: communicates with the frontend
BASE_BE_FE_CUID = 2**56
#: Range for the backend channel ID's (cuid) that
#: communicate with the frontend
RANGE_BE_FE_CUID = 2**56

#: Starting value for the local launcher backend channel unique ID (cuid)
BASE_BE_CUID = 2**57
#: Range for the local launcher backend channel ID's (cuid)
RANGE_BE_CUID = 2**57

#: Starting value for the gateway channel unique ID (cuid)
BASE_GW_CUID = 2**58
#: Range for the gateway channel ID's (cuid)
RANGE_GW_CUID = 2**58

#: Base value for the logging channel ID (cuid)
BASE_LOG_CUID = 2**59
#: Range for the logging channel ID's (cuid)
RANGE_LOG_CUID = 2**59

#: Starting value for the backend channel ID (cuid)
#: of the infrastructure transport gateway channel
BASE_BE_GW_CUID = 2**60
#: Range for the backend channel ID's (cuid)
#: of the infrastructure transport gateway channel
RANGE_BE_GW_CUID = 2**60

#: Starting value for the backend channel ID (cuid) that
#: communicates with its transport agent
BASE_BE_LOCAL_CUID = 2**61
#: Range for the backend channel ID's (cuid) that
#: communicate with its transport agent
RANGE_BE_LOCAL_CUID = 2**61

#: Starting value of the local services shepherd channel unique ID (cuid)
SHEP_CUID_POW = 62
BASE_SHEP_CUID = 2**SHEP_CUID_POW
#: Range for the local services channel ID's (cuid)
RANGE_SHEP_CUID = 2**SHEP_CUID_POW

#: Starting value of the User created channel ID (cuid)
FIRST_CUID = 2**63

#: First MUID managed by Local Services.
FIRST_LS_MANAGED_MUID = 2**59
#: Number of MUIDs possible allowable for each Local Services
#: The difference between FIRST_LS_MANAGED_MUID and FIRST_BE_MUID is
#: 2**59 (i.e. FIRST_LS_MANAGED_MUID). So we can take FIRST_LS_MANAGED_MUID
#: and divide by max nodes to get the range provided by each node.
RANGE_LS_MUIDS_PER_NODE = FIRST_LS_MANAGED_MUID // MAX_NODES

#: First backend pool ID (MUID) needed for the overlay network transport
FIRST_BE_MUID = 2**60
#: Range of the backend pool ID's (MUID) needed for the overlay network transport
RANGE_BE_MUID = 2**60

#: Pool for the frontend overlay network transport agent
FE_OVERLAY_TRANSPORT_AGENT_MUID = 2**60 - 1

#: First Logging MUID
FIRST_LOGGING_MUID = 2**61

#: First MUID of Default Pools.
FIRST_DEFAULT_MUID = 2**62

#: User Pool MUID starting value.
FIRST_MUID = 2**63

#: Starting value for the Group of resources (guid)
FIRST_GUID = 2**63

#: Frontend's magic host ID
FRONTEND_HOSTID = 1

# Names
DEFAULT_PROCESS_NAME_BASE = "dragon_process_"
DEFAULT_POOL_NAME_BASE = "dragon_pool_"
DEFAULT_DICT_POOL_NAME_BASE = "dragon_dict_pool_"
DEFAULT_CHANNEL_NAME_BASE = "dragon_channel_"
DEFAULT_GROUP_NAME_BASE = "dragon_group_"

GS_ERROR_EXIT = -1

# Reserved Shared Memory Segments
INFRASTRUCTURE_POOL_SUFFIX = "_inf"
DEFAULT_POOL_SUFFIX = "_def"
LOGGING_POOL_SUFFIX = "_log"


# Pre-defined process PUIDs:
def is_transport_puid(puid) -> bool:
    """Check whether a puid is associated with a transport agent

    :param puid: puid of a process
    :type puid: int
    :return: Whether the puid is associated with pre-defined puid value
    :type: bool
    """

    if puid >= FIRST_TRANSPORT_PUID and puid <= (FIRST_TRANSPORT_PUID + RANGE_TRANSPORT_PUID):
        return True
    else:
        return False


def transport_puid_from_index(index) -> int:
    """Get the unique process ID from the node index.
    Otherwise, throw an `AssertionError`

    :param index: node index
    :type index: int
    :return: p_uid of the local transport
    :rtype: int
    """
    assert index >= 0
    assert index < RANGE_TRANSPORT_PUID
    return FIRST_TRANSPORT_PUID + index


# Pre-defined Channel CUIDs
def launcher_cuid_from_index(index: int) -> int:
    """Get the unique Channel ID of the local Launcher backend from the node
    index.

    :param index: local node index.
    :type index: int
    :return: c_uid of the Launcher backend
    :rtype: int
    """
    assert index >= 0
    assert index < RANGE_BE_CUID
    return BASE_BE_CUID + index


def transport_cuid_from_index(index: int) -> int:
    """Get the unique Channel ID of the local transport agent from the nodex index.

    :param index: local node index
    :type index: int
    :return: c_uid of the local transport agent
    :rtype: int
    """

    assert index >= 0
    assert index < RANGE_TA_CUID
    return BASE_TA_CUID + index


def shepherd_cuid_from_index(index: int) -> int:
    """Get the unique Channel ID of local services from the node index.

    :param index: Local node index
    :type index: int
    :return: c_uid of the Local Services
    :rtype: int
    """

    assert index >= 0
    assert index < RANGE_SHEP_CUID
    return BASE_SHEP_CUID + index


def gw_cuid_from_index(index: int, num_gw_channels: int) -> int:
    """Return the unique Channel ID of the first gateway channel from the local
    node index

    :param index: local node index
    :type index: int
    :param num_gw_channels: number of gateway channels
    :type num_gw_channels: int
    :return: c_uid of first gateway channel
    :rtype: int
    """
    assert index >= 0
    assert index * num_gw_channels < RANGE_GW_CUID
    return BASE_GW_CUID + index * num_gw_channels


def be_gw_cuid_from_hostid(host_id):
    """Return the unique backend channel ID of the overlay network
    transport gateway channel, based on the host id

    :param host_id: host id
    :type host_id: int
    :return: c_uid of backend transport gateway channel
    :rtype: int
    """
    assert host_id >= 0
    cand_cuid = BASE_BE_GW_CUID + host_id
    cand_cuid = cand_cuid % (BASE_BE_GW_CUID + RANGE_BE_GW_CUID)
    assert cand_cuid < BASE_BE_GW_CUID + RANGE_BE_GW_CUID
    return cand_cuid


def be_fe_cuid_from_hostid(host_id):
    """Return the unique backend Channel ID that communicates
    with the fronted, based on the host id

    :param host_id: host id
    :type host_id: int
    :return: c_uid of backend channel that communicates with fronted
    :rtype: int
    """
    assert host_id >= 0
    cand_cuid = BASE_BE_FE_CUID + host_id
    cand_cuid = cand_cuid % (BASE_BE_FE_CUID + RANGE_BE_FE_CUID)
    assert cand_cuid < BASE_BE_FE_CUID + RANGE_BE_FE_CUID
    return cand_cuid


def be_local_cuid_in_from_hostid(host_id):
    """Return the unique channel ID based on host id, for a backend channel that acts
    as an inbound initializer for its communication with transport agent

    :param host_id: host id
    :type host_id: int
    :return: c_uid of backend inbound channel that communicates with transport agent
    :rtype: int
    """
    assert host_id >= 0
    cand_cuid = BASE_BE_LOCAL_CUID + host_id
    cand_cuid = cand_cuid % (BASE_BE_LOCAL_CUID + RANGE_BE_LOCAL_CUID)
    assert cand_cuid < BASE_BE_LOCAL_CUID + RANGE_BE_LOCAL_CUID
    return cand_cuid


def be_local_cuid_out_from_hostid(host_id):
    """Return the unique channel ID based on host id, for a backend channel that acts
    as an outbound initializer for its communication with transport agent

    :param host_id: host id
    :type host_id: int
    :return: c_uid of backend outbound channel that communicates with transport agent
    :rtype: int
    """
    assert host_id >= 0
    cand_cuid = BASE_BE_LOCAL_CUID + host_id + 1
    cand_cuid = cand_cuid % (BASE_BE_LOCAL_CUID + RANGE_BE_LOCAL_CUID)
    assert cand_cuid < BASE_BE_LOCAL_CUID + RANGE_BE_LOCAL_CUID
    return cand_cuid


def pmod_launch_cuid_from_jobinfo(host_id, job_id, lrank):
    """Return the unique channel ID based on host id, for a channel that initializes
    a client PMI process with its required pmod data.

    :param host_id: host id
    :type host_id: int
    :return: c_uid of backend outbound channel that communicates with transport agent
    :rtype: int
    """
    assert host_id >= 0
    job_spacing = 2**20
    host_spacing = 2**14
    pmod_launch_cuid = BASE_PMOD_LAUNCH_CUID + (job_id * job_spacing) + (host_id * host_spacing) + lrank + 1
    pmod_launch_cuid = pmod_launch_cuid % (BASE_PMOD_LAUNCH_CUID + RANGE_PMOD_LAUNCH_CUID)
    assert pmod_launch_cuid < BASE_PMOD_LAUNCH_CUID + RANGE_PMOD_LAUNCH_CUID
    return pmod_launch_cuid


# Pre-defined Pool MUIDs
def default_pool_muid_from_index(index: int) -> int:
    """Return the unique managed memory pool ID of the default user pool from
    the node index.

    :param index: index of this node
    :type index: int
    :return: m_uid of user pool
    :rtype: int
    """
    assert index >= 0
    return FIRST_DEFAULT_MUID + index


def logging_pool_muid_from_index(index: int) -> int:
    """Return the unique managed memory pool ID of the default logging pool from
    the node index.

    :param index: index of this node
    :type index: int
    :return: m_uid of logging pool
    :rtype: int
    """
    assert index >= 0
    return FIRST_LOGGING_MUID + index


def infrastructure_pool_muid_from_index(index: int) -> int:
    """Return the unique managed memory pool ID of the default infrastructure pool from
    the node index.

    :param index: index of this node
    :type index: int
    :return: m_uid of the infrastructure pool
    :rtype: int
    """
    assert index >= 0
    return index


def local_pool_first_muid(index: int) -> int:
    """Return the first muid for local services managed pools given the node index
       provided. Each local services uses this to begin allocating muids starting
       at a specfic integer.

    :param index: index of this node
    :type index: int
    :return: m_uid of the infrastructure pool
    :rtype: int
    """
    assert index >= 0
    return FIRST_LS_MANAGED_MUID + index * RANGE_LS_MUIDS_PER_NODE


def index_from_infrastructure_pool_muid(m_uid: int) -> int:
    """Return the node index from the unique managed memory pool ID of the
    default infrastructure pool.

    :param m_uid: m_uid of the infrastructure pool
    :type index: int
    :return: unique node index of this node
    :rtype: int
    """
    assert m_uid < FIRST_DEFAULT_MUID
    return m_uid


def be_pool_muid_from_hostid(host_id):
    """Return the backend memory pool unique ID based on the host index

    :param host_id: host id
    :type index: int
    :return: unique ID of the backend memory pool
    :rtype: int
    """
    assert host_id >= 0
    cand_muid = FIRST_BE_MUID + host_id
    cand_muid = cand_muid % (FIRST_BE_MUID + RANGE_BE_MUID)
    assert cand_muid < FIRST_BE_MUID + RANGE_BE_MUID
    return cand_muid


def is_pre_defined_pool(m_uid: int) -> bool:
    """Return if the pool corresponding to the given `m_uid` is pre-defined by
    the runtime.

    :param m_uid: uid to check
    :type m_uid: int
    :return: True if the m_uid is predefined, otherwise throw an `AssertionError`.
    :rtype: bool
    """

    assert m_uid >= 0
    return m_uid < FIRST_MUID


def index_from_shepherd_cuid(cuid: int) -> int:
    """Retun the local node index from the `c_uid` of local services.

    :param cuid: unique channel id of the local Shepherd
    :type cuid: int
    :return: Unique local node index.
    :rtype: int
    """
    assert cuid >= BASE_SHEP_CUID
    assert cuid < BASE_SHEP_CUID + RANGE_SHEP_CUID
    return cuid - BASE_SHEP_CUID


def is_default_pool(m_uid: int) -> bool:
    """Check if the given `m_uid` is a default `m_uid`. Otherwise throw an `AssertionError`.

    :param m_uid: uid to check
    :type m_uid: int
    :return: True
    :rtype: bool
    """
    assert m_uid >= FIRST_DEFAULT_MUID
    assert m_uid < FIRST_MUID
    return True


def index_from_default_pool_muid(m_uid: int) -> int:
    """Return the unique node index from the default pool id.

    :param m_uid: A default pool id.
    :type m_uid: int
    :return: node index
    :rtype: int
    """
    assert m_uid >= FIRST_DEFAULT_MUID
    assert is_default_pool(m_uid)
    return m_uid - FIRST_DEFAULT_MUID


# Size constants
ONE_MB = 2**20
ONE_GB = 2**30
TWO_GB = 2**31
FOUR_GB = 2**32
SIXTEEN_GB = 2**34

# decision constant for argument delivery
# TODO: tune this - 64K is probably too big. check size of e.g. pool worker Process call.
ARG_IMMEDIATE_LIMIT = 2**16  # 64K

# environment variable name to use to pass in
# some pickled pre-made test channels to gs
# for testing purposes.
GS_TEST_CH_EV = "_XDRAGON_GS_TEST_CHANS"
GS_LOG_BASE = "_XDRAGON_GS_LOG_BASE"

GS_SINGLE_LAUNCH_CMD = "import dragon.globalservices.server as dgs; dgs.single()"
GS_MULTI_LAUNCH_CMD = "import dragon.globalservices.server as dgs; dgs.multi()"

# add names for more entry points for e.g. multinode
DRAGON_LOGGER_SDESC = "DRAGON_LOGGER_SDESC"
DEFAULT_GS_SINGLE_MAX_MSGS = str(1000)
DEFAULT_SH_SINGLE_MAX_MSGS = str(1000)
DEFAULT_SINGLE_DEF_SEG_SZ = str(FOUR_GB)
DEFAULT_SINGLE_INF_SEG_SZ = str(ONE_GB)
DEFAULT_BE_OVERLAY_TRANSPORT_SEG_SZ = str(2**8 * ONE_MB)  # 256MB

DRAGON_CFG_FILENAME = ".dragon_launch_config"

BREAKPOINT_FILENAME = ".dragon_breakpoints"
BASE_DEBUG_CUID = 2**33  # range of pid is 32 bits, need 2 per, so < 2**34

# time to wait for messages in ls teardown before declaring an error
TEARDOWN_PATIENCE = 1.0  # seconds, 1 second

# This may become configurable and part of the managed process creation in the future.
MANAGED_PROCESS_MAX_OUTPUT_SIZE_PER_MESSAGE = 5000

# needed for naming the environment variable keys for the
# three channels when piping stdout/stderr/stdin for a process
# created by the Dragon run-time services.
STDOUT_DESC = "dragon_stdout_desc"
STDERR_DESC = "dragon_stderr_desc"
STDIN_DESC = "dragon_stdin_desc"

# default capacity of gateway channels. This might need to be raised
# in circumstances where we're flooded in ops in a sequence that lead to a hang
GW_DEFAULT_CAPACITY = 2048

# This defines an enivironment variable name for an isolated transport test
# environment where global services is not started.
TRANSPORT_TEST_ENV = "DRAGON_TRANSPORT_TEST"

# This provides env. var. to perform a simple bring-up and teardown of all
# multi-node services in order to measure bounce time of our services.
BOUNCE_TEST_ENV = "DRAGON_BOUNCE_RUNTIME_TEST"

# This defines the preferred waiting mode of connections, either idle waiting
# or spin waiting for both user connections (those made directly by user code)
# and infrastructure connections (made by dragon infrastructure code).
INFRASTRUCTURE_DEFAULT_WAIT_MODE = dtypes.IDLE_WAIT
USER_DEFAULT_WAIT_MODE = dtypes.ADAPTIVE_WAIT

# This defines the preferred send return when mode for connections.
# WHEN_DEPOSITED means to return control to the sender when the message
# is deposited into the destination channel. WHEN_IMMEDIATE means to
# return control to the sender earlier, allowing more asynchronous behavior
# but not guaranteeing order between other operations on the channel like
# receives and polling operations.
INFRASTRUCTURE_DEFAULT_RETURN_WHEN_MODE = dtypes.WHEN_BUFFERED
USER_DEFAULT_RETURN_WHEN_MODE = dtypes.WHEN_DEPOSITED

# Service process names
from ..cli import (
    PROCNAME_GS,
    PROCNAME_LA_FE,
    PROCNAME_LA_BE,
    PROCNAME_LS,
    PROCNAME_TCP_TA,
    PROCNAME_OVERLAY_TA,
    PROCNAME_OOB_TA,
    PROCNAME_RDMA_TA,
    PROCNAME_GLOBAL_CLEANUP,
    PROCNAME_LOCAL_CLEANUP,
    console_script_args,
)


# Aliases for transport agent commands. Transport agent commands are always
# called with two positional arguments:
#   1. Node index
#   2. Input channel serialized descriptor
@enum.unique
class TransportAgentOptions(enum.Enum):
    """Enumerated list of supported transport agents"""

    HSTA = "hsta"
    TCP = "tcp"
    DRAGON_CONFIG = "configured"

    def __str__(self):
        return self.value

    @staticmethod
    def from_str(s):
        """Obtain enum value from TransportAgentOptions string

        :param s: string representation of enumerated TransportAgentOptions
        :type s: str
        :return: name of an available transport agent
        :rtype: TransportAgentOptions
        """
        try:
            return TransportAgentOptions(s)
        except KeyError:
            raise ValueError()


TRANSPORT_AGENT_ALIASES = {
    # NOTE Cannot use dragon.cli.console_script_args() as it would cause a
    # NOTE circular import.
    str(TransportAgentOptions.HSTA): console_script_args(f"{PROCNAME_RDMA_TA}-if"),
    str(TransportAgentOptions.TCP): console_script_args(PROCNAME_TCP_TA, "--no-tls"),
    # TODO Launcher does not yet support dynamic cert/key generation
    # TODO See dragon.transport.x509 to generate keys and certs. Possible
    # TODO approaches for integrating X509 creds:
    # TODO
    # TODO   1. Add new messages to support CSRs and Certs and instrument
    # TODO      agents, local services, launcher BE and FE to support X509
    # TODO      lifecycle.
    # TODO
    # TODO   2. Launcher/Local services detects TLS-enabled agent and
    # TODO      generates keys and certs and passes them as command line
    # TODO      arguments (as shown below).
    # TODO
    # TODO Either way we should probably consider the launcher to generate
    # TODO a root CA which is used to issue a sub-CA to each local
    # TODO services. That would enable local services to more easily manage
    # TODO the X509 lifecycle with (option 1) or for (option 2) transport
    # TODO agents.
    #'tcp+tls': console_script_args(PROCNAME_TCP_TA, '--cafile', CA, '--certfile', CERT, '--keyfile', KEY)),
}

DEFAULT_TRANSPORT_AGENT = shlex.split(str(TransportAgentOptions.HSTA))

# Prefer to use high-speed network interfaces. Be cautious about matching any
# interface (e.g., r'^.*$') as it may expose the transport agent on an
# unintended network (e.g., the internet).
DEFAULT_TRANSPORT_NETIF = r"^(hsn|ipogif|ib)\d+$"
DEFAULT_TRANSPORT_PORT = 7575
DEFAULT_OVERLAY_NETWORK_PORT = 6565
DEFAULT_FRONTEND_PORT = 6566
DEFAULT_PMI_CONTROL_PORT = 8575
DEFAULT_OOB_PORT = 9575
DEFAULT_PORT_RANGE = 1000

DRAGON_BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
DRAGON_LIB_DIR = os.path.join(DRAGON_BASE_DIR, "lib")
DRAGON_INCLUDE_DIR = os.path.join(DRAGON_BASE_DIR, "include")
DRAGON_BIN_DIR = os.path.join(DRAGON_BASE_DIR, "bin")

HSTA_BINARY = Path(os.path.join(DRAGON_BIN_DIR, PROCNAME_RDMA_TA))
DRAGON_LIB_SO = os.path.join(DRAGON_LIB_DIR, "libdragon.so")


def _set_base_dir():
    os.environ["DRAGON_BASE_DIR"] = DRAGON_BASE_DIR


if os.environ.get("DRAGON_BASE_DIR", False):
    _set_base_dir()


def port_check(ip_port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind(ip_port)
    except Exception:
        if s is not None:
            s.close()
        return False
    else:
        s.close()
        return True


def get_port(min_port, port_range):
    host = socket.gethostname()
    max_port = min_port + port_range

    for port in range(min_port, max_port):
        if port_check((host, port)):
            return port


# Port used for out-of-band communication
OOB_PORT = get_port(DEFAULT_OOB_PORT, DEFAULT_PORT_RANGE)

# Policy.global_policy() -- To prevent circular imports, this lives in policy.py
DRAGON_POLICY_CONTEXT_ENV = "DRAGON_POLICY_CONTEXT_ENV"

DEFAULT_NET_CONF_CACHE = os.path.join(os.getcwd(), ".dragon-net-conf")

# Default dragon config directory and file
DRAGON_CONFIG_DIR = Path(DRAGON_BASE_DIR) / ".hsta-config"
CONFIG_FILE_PATH = DRAGON_CONFIG_DIR / "dragon-config.json"
