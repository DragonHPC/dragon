"""Launch parameters for Dragon infrastructure and managed processes

Dragon processes that get launched need a number of parameters explaining
the size of the allocation, which node they are on, and other information
needed to bootstrap communication to the infrastructure itself.

It isn't reasonable to provide these on the command line, so they are instead
passed through environment variables managed by this module.
"""

import base64
import binascii
import os
import shlex

import dragon.infrastructure.facts as dfacts
import dragon.dtypes as dtypes
import dragon.utils as du


class ParmError(Exception):
    pass


class TypedParm:
    """Defines a parameter and how it is to be checked.

    Attributes:
        name: string name of parameter, as env variable comes out as DRAGON_name
        cast: callable to cast the string parameter to what it should be
        check: had better be True, call on output of cast
        default: default value for parameter; default/default is None
    """

    def __init__(self, name, cast, check, default=None):
        self.check = check
        self.cast = cast
        self.name = name
        self.default = default


def nocast(x):
    return x


# noinspection PyUnusedLocal
def nocheck(dummy):
    return True


def nonnegative(x):
    return x >= 0


def positive(x):
    return x > 0


def check_pct(x):
    if isinstance(x, int) or isinstance(x, float):
        return x >= 0 and x <= 100


def check_mode(mode):
    return mode in {dfacts.CRAYHPE_MULTI_MODE, dfacts.SINGLE_MODE, dfacts.TEST_MODE}


def check_pool(pool):
    return pool in {dfacts.PATCHED, dfacts.NATIVE}


def cast_wait_mode(wait_mode=dfacts.INFRASTRUCTURE_DEFAULT_WAIT_MODE):
    if isinstance(wait_mode, dtypes.WaitMode):
        return wait_mode
    if wait_mode == "IDLE_WAIT":
        return dtypes.IDLE_WAIT
    if wait_mode == "SPIN_WAIT":
        return dtypes.SPIN_WAIT
    if wait_mode == "ADAPTIVE_WAIT":
        return dtypes.ADAPTIVE_WAIT
    if wait_mode == dtypes.IDLE_WAIT.value:
        return dtypes.IDLE_WAIT
    if wait_mode == dtypes.SPIN_WAIT.value:
        return dtypes.SPIN_WAIT
    if wait_mode == dtypes.ADAPTIVE_WAIT.value:
        return dtypes.ADAPTIVE_WAIT
    if wait_mode == str(dtypes.IDLE_WAIT.value):
        return dtypes.IDLE_WAIT
    if wait_mode == str(dtypes.SPIN_WAIT.value):
        return dtypes.SPIN_WAIT
    if wait_mode == str(dtypes.ADAPTIVE_WAIT.value):
        return dtypes.ADAPTIVE_WAIT
    if wait_mode == str(dtypes.IDLE_WAIT):
        return dtypes.IDLE_WAIT
    if wait_mode == str(dtypes.SPIN_WAIT):
        return dtypes.SPIN_WAIT
    if wait_mode == str(dtypes.ADAPTIVE_WAIT):
        return dtypes.ADAPTIVE_WAIT
    raise ValueError(
        f"The provided wait mode must be one of IDLE_WAIT, SPIN_WAIT, or ADAPTIVE_WAIT. The value was {wait_mode}"
    )


def cast_return_when_mode(return_when=dfacts.INFRASTRUCTURE_DEFAULT_RETURN_WHEN_MODE):
    if isinstance(return_when, dtypes.ReturnWhen):
        return return_when
    if return_when == "WHEN_IMMEDIATE":
        return dtypes.WHEN_IMMEDIATE
    if return_when == "WHEN_BUFFERED":
        return dtypes.WHEN_BUFFERED
    if return_when == "WHEN_DEPOSITED":
        return dtypes.WHEN_DEPOSITED
    if return_when == "WHEN_RECEIVED":
        return dtypes.WHEN_RECEIVED
    if return_when == str(dtypes.WHEN_IMMEDIATE.value):
        return dtypes.WHEN_IMMEDIATE
    if return_when == str(dtypes.WHEN_BUFFERED.value):
        return dtypes.WHEN_BUFFERED
    if return_when == str(dtypes.WHEN_DEPOSITED.value):
        return dtypes.WHEN_DEPOSITED
    if return_when == str(dtypes.WHEN_RECEIVED.value):
        return dtypes.WHEN_RECEIVED
    if return_when == dtypes.WHEN_IMMEDIATE.value:
        return dtypes.WHEN_IMMEDIATE
    if return_when == dtypes.WHEN_BUFFERED.value:
        return dtypes.WHEN_BUFFERED
    if return_when == dtypes.WHEN_DEPOSITED.value:
        return dtypes.WHEN_DEPOSITED
    if return_when == dtypes.WHEN_RECEIVED.value:
        return dtypes.WHEN_RECEIVED
    if return_when == str(dtypes.WHEN_IMMEDIATE):
        return dtypes.WHEN_IMMEDIATE
    if return_when == str(dtypes.WHEN_BUFFERED):
        return dtypes.WHEN_BUFFERED
    if return_when == str(dtypes.WHEN_DEPOSITED):
        return dtypes.WHEN_DEPOSITED
    if return_when == str(dtypes.WHEN_RECEIVED):
        return dtypes.WHEN_RECEIVED

    raise ValueError(
        f"The provided return mode must be one of WHEN_IMMEDIATE, WHEN_BUFFERED, WHEN_DEPOSITED, or WHEN_RECEIVED. The value was {return_when}"
    )


def check_base64(strdata):
    decoded_ok = True
    try:
        base64.b64decode(strdata, validate=True)
    except binascii.Error:
        decoded_ok = False

    return decoded_ok


typecast = lambda ty: lambda val: ty() if val == "" else ty(val)

# This needs to be written up in docs once available. This
# contains information about how to configure the OOM warming and critical
# limits.

# By default, one such warning, the approaching out of memory (i.e. OOM)
# warning, will be printed if the amount of remaining free memory on a node is
# getting low. Remaining free memory is measured in either a percent of remaining
# memory or in a number of free bytes, whichever results in warnings for the
# lowest number of bytes still available.

# This means by default, nodes with fewer than 64GB of memory will use the
# percentage value to decide if a warning should be generated and for bigger nodes
# less than 6GB of remaining free memory will be used as the threshold.

# The OOM percentage value can be controlled through the use of the environment
# variable {OOM_WARNING_PCT_VAR} and should be set to the allowable percentage of
# remaining memory that can be tolerated on any node. By default it is set to 10
# percent. If you don't wish to use percentage, setting it to 100 percent will
# result in using the bytes threshold instead.

# The OOM bytes threshold is set to 6GB by default which is the allowable number of
# bytes of remaining free memory before warning. It can be adjusted by setting the
# {OOM_WARNING_BYTES_VAR} env var. If there is less than 64GB memory on a node,
# then by default the percentage will be less and the percentage will be used
# instead. Setting the allowable remaining bytes threshold to 0 indicates that
# the percent threshold should be used instead.

# Two more environment variables are used to detect, notify, and take down the
# Dragon run-time when it is critically out of memory and (hopefully) before a
# deadlock/hang would occur. There are two similar environment variables and the
# same use of the fewer number of bytes to determine which threshold should be
# used. The {OOM_CRITICAL_PCT_VAR} is the percentage of free memory threshold that
# should be used when deciding to take down the Dragon runtime. The
# {OOM_CRITICAL_BYTES_VAR} is the number of free bytes threshold that should be
# used in making the same decision. Again, Dragon will use the smaller of these two
# values (in bytes) to decide when it would be necessary to take down the run-time.
# Setting the percent to 100 means that bytes would be used to make the decision to
# take down the run-time. Setting the bytes to 0 will result in using the
# percentage to make the decision of program termination. Setting both the percent
# to 100 and the number of bytes to 0 for nodes in your cluster/allocation will
# result in no critical OOM handling which may/will lead to a deadlock condition
# and is up to the programmer to detect and deal with in that case. By default the
# critical percent is set at 2% and the OOM bytes is set to 600MB.

# In addition, setting {OOM_QUIET_VAR} to any value other than "False" will
# result in no warnings being printed to stderr. The default is to print
# warnings. If critical OOM is detected, the entire run-time will be taken down
# with a critical OOM message but without further warnings.


class LaunchParameters:
    """Launch Parameters for Dragon processes.

    This class contains the basic runtime parameters for Dragon infrastructure
    and managed processes.

    The parameter names are available in the environment of every Dragon managed
    process with the prefix `DRAGON_`. E.g. `DRAGON_MY_PUID` for `MY_PUID`. Setting
    some of these parameters will alter runtime behaviour. E.g. setting
    `DRAGON_DEBUG=1` in the environment before starting the runtime, will setup the
    runtime in debug mode and result in verbose log files.

    The parameter names are available as attributes with lower cased names in
    Python.

    The following launch parameters are defined

    :param MODE: Startup mode of the infrastructure, one of `'crayhpe-multi'`, `'single'`, `'test'`, defaults to `'test'`.
    :type MODE: str
    :param INDEX: Internal non-negative index of the current node, defaults to 0.
    :type INDEX: int
    :param DEFAULT_PD: Base64 encoded serialized descriptor of the default user memory pool, defaults to ''.
    :type DEFAULT_PD: str
    :param INF_PD: Base64 encoded serialized descriptor of the default infrastructure memory pool, defaults to ''.
    :type INF_PD: str
    :param LOCAL_SHEP_CD: Base64 encoded serialized descriptor of the channel to send to the local services on the same node, defaults to ''.
    :type LOCAL_SHEP_CD: str
    :param LOCAL_BE_CD: Base64 encoded serialized descriptor of the channel to send to the launcher backend on the same node, defaults to ''.
    :type LOCAL_BE_CD:  str
    :param GS_CD: Base64 encoded serialized descriptor of the channel to send to global services, defaults to ''.
    :type GS_CD: str
    :param GS_RET_CD: Base64 encoded serialized descriptor of the channel to receive from global services, defaults to ''.
    :type GS_RET_CD: str
    :param SHEP_RET_CD: Base64 encoded serialized descriptor of the channel to receive from the local services on the same node, defaults to ''.
    :type SHEP_RET_CD: str
    :param DEFAULT_SEG_SZ: Size of the default user managed memory pool in bytes, defaults to 2**32.
    :type DEFAULT_SEG_SZ: int
    :param INF_SEG_SZ: Size of the default infrastructure managed memory pool in bytes, defaults to 2**30.
    :type INF_SEG_SZ: int
    :param MY_PUID: Unique process ID (`puid`) given to this process by global services, defaults to 1.
    :type MY_PUID: int
    :param TEST: if in test mode, defaults to 0.
    :type TEST: int
    :param DEBUG: if in debug mode, defaults to 0.
    :type DEBUG: int
    :param BE_CUID: Unique channel ID (`cuid`) of the launcher backend on this node, defaults to 2**60.
    :type BE_CUID: int
    :param INF_WAIT_MODE: Default wait mode of infrastructure objects. One of `'IDLE_WAIT'`, `'SPIN_WAIT'`, or `'ADAPTIVE_WAIT'`. Defaults to `'IDLE_WAIT'`.
    :type INF_WAIT_MODE: str
    :param USER_WAIT_MODE: Default wait mode of user objects. One of `'IDLE_WAIT'`, `'SPIN_WAIT'`, , or `'ADAPTIVE_WAIT'`. defaults to `'ADAPTIVE_WAIT'`.
    :type USER_WAIT_MODE: str
    :param INF_RETURN_WHEN_MODE: Default return mode for infrastructure objects. One of `'WHEN_IMMEDIATE'`, `'WHEN_BUFFERED'`, `'WHEN_DEPOSITED'`, `'WHEN_RECEIVED'`, defaults to `'WHEN_BUFFERED'`.
    :type INF_RETURN_WHEN_MODE: str
    :param USER_RETURN_WHEN_MODE: Default return mode for user objects. One of `'WHEN_IMMEDIATE'`, `'WHEN_BUFFERED'`, `'WHEN_DEPOSITED'`, `'WHEN_RECEIVED'`, defaults to `'WHEN_DEPOSITED'`.
    :type USER_RETURN_WHEN_MODE: str
    :param GW_CAPACITY: Positive capacity of gateway channels, defaults to 2048.
    :type GW_CAPACITY: int
    :param HSTA_MAX_EJECTION_MB: Size in MB of buffers used for network receive operations. This controls network ejection rate, defaults to 8.
    :type HSTA_MAX_EJECTION_MB: int
    :param HSTA_MAX_GETMSG_MB: Size in MB of buffers used for local `'getmsg`' operations. This controls memory consumption rate for messages with `'GETMSG`' protocol, defaults to 8.
    :type HSTA_MAX_GETMSG_MB: int
    :param HSTA_FABRIC_BACKEND: Select the fabric backend to use. Possible options are `'ofi`', `'ucx`', and `'mpi`', defaults to `'ofi`'.
    :type HSTA_FABRIC_BACKEND: str
    :param HSTA_TRANSPORT_TYPE: Select if HSTA uses point-to-point operations or RDMA Get to send large messages. Possible options are `'p2p`' and `'rma`', defaults to `'p2p`'.
    :type HSTA_TRANSPORT_TYPE: str
    :param PMOD_COMMUNICATION_TIMEOUT: Timeout in seconds for PMOD communication with child MPI processes, defaults to 30.
    :type PMOD_COMMUNICATION_TIMEOUT: int
    :param BASEPOOL: Default implementation of multiprocessing pool. One of `'NATIVE'` or `'PATCHED'`, defaults to `'NATIVE'`.
    :type BASEPOOL: str
    :param OOM_WARNING_PCT: The percentage of free memory between 0 and 100 when a warning is printed to stderr to warn of low memory conditions. Setting this value to 100 will result in no warning messages.
    :type OOM_WARNING_PCT: float
    :param OOM_CRITICAL_PCT: The percentage of free memory between 0 and 100 when a critical error is printed to stderr and the run-time is taken down due to extremely low memory conditions. Setting this value to 100 will result in no critical takedown and likely lead to a hang of the Dragon run-time.
    :type OOM_CRITICAL_PCT: float
    :param OOM_WARNING_BYTES: The bytes of free memory when a warning error is printed to stderr. Setting this value to 0 will result in no warning message. When bytes is non-zero the smaller value of the percent and bytes (in bytes) will be used to decide about warnings.
    :type OOM_WARNING_BYTES: int
    :param OOM_CRITICAL_BYTES: The bytes of free memory when a critical error is printed to stderr. Setting this value to 0 will result in no critical error message. When bytes is non-zero the smaller value of the percent and bytes (in bytes) will be used to decide about critical takedown of the Dragon run-time.
    :type OOM_CRITICAL_BYTES: int
    """

    @classmethod
    def init_class_vars(cls):

        typecast = lambda ty: lambda val: ty() if val == "" else ty(val)

        PARMS = [
            TypedParm(name=dfacts.MODE, cast=typecast(str), check=check_mode, default=dfacts.TEST_MODE),
            TypedParm(name=dfacts.INDEX, cast=typecast(int), check=nonnegative, default=0),
            TypedParm(name=dfacts.DEFAULT_PD, cast=typecast(str), check=check_base64, default=""),
            TypedParm(name=dfacts.INF_PD, cast=typecast(str), check=check_base64, default=""),
            TypedParm(name=dfacts.LOCAL_SHEP_CD, cast=typecast(str), check=check_base64, default=""),
            TypedParm(name=dfacts.LOCAL_BE_CD, cast=typecast(str), check=check_base64, default=""),
            TypedParm(name=dfacts.GS_RET_CD, cast=typecast(str), check=check_base64, default=""),
            TypedParm(name=dfacts.SHEP_RET_CD, cast=typecast(str), check=check_base64, default=""),
            TypedParm(name=dfacts.GS_CD, cast=typecast(str), check=check_base64, default=""),
            TypedParm(
                name=dfacts.DEFAULT_SEG_SZ,
                cast=typecast(int),
                check=positive,
                default=int(dfacts.DEFAULT_SINGLE_DEF_SEG_SZ),
            ),
            TypedParm(
                name=dfacts.INF_SEG_SZ,
                cast=typecast(int),
                check=positive,
                default=int(dfacts.DEFAULT_SINGLE_INF_SEG_SZ),
            ),
            TypedParm(name=dfacts.MY_PUID, cast=typecast(int), check=positive, default=1),
            TypedParm(name=dfacts.TEST, cast=typecast(int), check=nonnegative, default=0),
            TypedParm(name=dfacts.DEBUG, cast=typecast(int), check=nonnegative, default=0),
            TypedParm(name=dfacts.BE_CUID, cast=typecast(int), check=positive, default=dfacts.BASE_BE_CUID),
            TypedParm(
                name=dfacts.INF_WAIT_MODE,
                cast=typecast(cast_wait_mode),
                check=nocheck,
                default=dfacts.INFRASTRUCTURE_DEFAULT_WAIT_MODE,
            ),
            TypedParm(
                name=dfacts.USER_WAIT_MODE,
                cast=typecast(cast_wait_mode),
                check=nocheck,
                default=dfacts.USER_DEFAULT_WAIT_MODE,
            ),
            TypedParm(
                name=dfacts.INF_RETURN_WHEN_MODE,
                cast=typecast(cast_return_when_mode),
                check=nocheck,
                default=dfacts.INFRASTRUCTURE_DEFAULT_RETURN_WHEN_MODE,
            ),
            TypedParm(
                name=dfacts.USER_RETURN_WHEN_MODE,
                cast=typecast(cast_return_when_mode),
                check=nocheck,
                default=dfacts.USER_DEFAULT_RETURN_WHEN_MODE,
            ),
            TypedParm(name=dfacts.GW_CAPACITY, cast=typecast(int), check=positive, default=dfacts.GW_DEFAULT_CAPACITY),
            TypedParm(
                name=dfacts.NUM_GW_CHANNELS_PER_NODE,
                cast=typecast(int),
                check=nonnegative,
                default=dfacts.DRAGON_DEFAULT_NUM_GW_CHANNELS_PER_NODE,
            ),
            TypedParm(name=dfacts.HSTA_MAX_EJECTION_MB, cast=typecast(int), check=positive, default=8),
            TypedParm(name=dfacts.HSTA_MAX_GETMSG_MB, cast=typecast(int), check=positive, default=8),
            TypedParm(name=dfacts.HSTA_FABRIC_BACKEND, cast=typecast(str), check=nocheck),
            TypedParm(
                name=dfacts.HSTA_TRANSPORT_TYPE,
                cast=typecast(str),
                check=nocheck,
                default=dfacts.DEFAULT_HSTA_TRANSPORT_TYPE,
            ),
            TypedParm(name=dfacts.PMOD_COMMUNICATION_TIMEOUT, cast=typecast(int), check=positive, default=30),
            TypedParm(name=dfacts.BASEPOOL, cast=typecast(str), check=check_pool, default=dfacts.DEFAULT_POOL),
            TypedParm(name=dfacts.OVERLAY_FANOUT, cast=typecast(int), check=positive, default=32),
            TypedParm(
                name=dfacts.NET_CONF_CACHE, cast=typecast(str), check=nocheck, default=dfacts.DEFAULT_NET_CONF_CACHE
            ),
            TypedParm(
                name=dfacts.NET_CONF_CACHE_TIMEOUT,
                cast=typecast(int),
                check=nonnegative,
                default=dfacts.DEFAULT_NET_CONF_CACHE_TIMEOUT,
            ),
            TypedParm(
                name=dfacts.OOM_WARNING_PCT_VAR,
                cast=typecast(float),
                check=check_pct,
                default=dfacts.OOM_DEFAULT_WARN_PCT,
            ),
            TypedParm(
                name=dfacts.OOM_CRITICAL_PCT_VAR,
                cast=typecast(float),
                check=check_pct,
                default=dfacts.OOM_DEFAULT_CRITICAL_PCT,
            ),
            TypedParm(
                name=dfacts.OOM_WARNING_BYTES_VAR,
                cast=typecast(int),
                check=nonnegative,
                default=dfacts.OOM_DEFAULT_WARN_BYTES,
            ),
            TypedParm(
                name=dfacts.OOM_CRITICAL_BYTES_VAR,
                cast=typecast(int),
                check=nonnegative,
                default=dfacts.OOM_DEFAULT_CRITICAL_BYTES,
            ),
            TypedParm(name=dfacts.QUIET_VAR, cast=typecast(str), check=nocheck, default="False"),
        ]

        env = os.environ

        try:
            num_gateways_env_var = dfacts.PREFIX + dfacts.NUM_GW_CHANNELS_PER_NODE_VAR
            num_gateways = (
                int(env[num_gateways_env_var])
                if num_gateways_env_var in env
                else dfacts.DRAGON_DEFAULT_NUM_GW_CHANNELS_PER_NODE
            )
            assert num_gateways >= 0
        except:
            raise ValueError(
                f"The value of the environment variable {dfacts.NUM_GW_CHANNELS_PER_NODE} must be valid integer >= 1. It was {env[dfacts.NUM_GW_CHANNELS_PER_NODE]}"
            )

        cls.gw_env_vars = frozenset([f"GW{x+1}" for x in range(num_gateways)])

        for gw_env_var in cls.gw_env_vars:
            PARMS.append(TypedParm(gw_env_var, cast=typecast(str), check=nocheck, default=""))

        cls.PARMS = PARMS

        cls.NODE_LOCAL_PARAMS = (
            frozenset([dfacts.DEFAULT_PD, dfacts.INF_PD, dfacts.LOCAL_SHEP_CD, dfacts.LOCAL_BE_CD, dfacts.BE_CUID])
            | cls.gw_env_vars
        )

    def __init__(self, **kwargs):
        for pt in LaunchParameters.PARMS:
            attr_name = pt.name.lower()
            if attr_name in kwargs:
                setattr(self, attr_name, kwargs[attr_name])
            else:
                setattr(self, attr_name, None)

    @classmethod
    def _ev_name_from_pt(cls, ptname: str):
        if ptname in cls.gw_env_vars:
            return dfacts.PREFIX + ptname

        return dfacts.env_name(ptname)

    @staticmethod
    def _attr_name_from_pt(ptname):
        return ptname.lower()

    @classmethod
    def all_parm_names(cls):
        return {cls._attr_name_from_pt(pt.name) for pt in cls.PARMS}

    @classmethod
    def remove_node_local_evars(cls, env):
        for e in cls.NODE_LOCAL_PARAMS:
            ev_name = cls._ev_name_from_pt(e)
            if ev_name in env:
                del env[ev_name]

    @staticmethod
    def from_env(init_env=None):
        if init_env is None:
            init_env = os.environ

        evd = {}

        for pt in LaunchParameters.PARMS:
            ev_name = LaunchParameters._ev_name_from_pt(pt.name)
            attr_name = LaunchParameters._attr_name_from_pt(pt.name)
            if ev_name in init_env:
                try:
                    val = pt.cast(init_env[ev_name])
                except Exception as ex:
                    raise ValueError(
                        f"The value of environment variable {ev_name} was not the correct type. It was {init_env[ev_name]}. The error was: {repr(ex)}"
                    )

                if not pt.check(val):
                    msg = f"{ev_name} = {val} failed check {pt.check.__name__}"
                    raise ParmError(msg)

                evd[attr_name] = val
            else:
                evd[attr_name] = pt.default

        return LaunchParameters(**evd)

    def env(self):
        """Returns dictionary of environment variables for this object.

        An entry is created for parameters that are not None and are listed in
        LaunchParameters.Parms, with the correctly prefixed name of the
        environment variable and so on.

        The reason for this option is to let a process or actor construct
        the launch parameters it wants to have for another process
        it is getting ready to start.
        """
        rv = {}
        for pt in LaunchParameters.PARMS:
            attr_name = LaunchParameters._attr_name_from_pt(pt.name)
            val = getattr(self, attr_name, None)

            if val is not None:
                ev_name = LaunchParameters._ev_name_from_pt(pt.name)
                rv[ev_name] = str(val)

        return rv

    def set_num_gateways_per_node(self, num_gateways=dfacts.DRAGON_DEFAULT_NUM_GW_CHANNELS_PER_NODE):
        # Configure the number of gateways per node as an
        # environment variable.
        num_gateways_env_var = dfacts.PREFIX + dfacts.NUM_GW_CHANNELS_PER_NODE_VAR
        assert num_gateways >= 0, f"The number of gateways must be greater or equal to 0. It was {num_gateways}"
        os.environ[num_gateways_env_var] = str(num_gateways)
        this_process.num_gw_channels_per_node = num_gateways
        self.init_class_vars()


LaunchParameters.init_class_vars()
this_process = LaunchParameters.from_env()


def reload_this_process():
    global this_process
    this_process = LaunchParameters.from_env()


class Policy:
    """Used to encapsulate policy decisions.

    An instance of this class can be used to configure the policy decisions
    made within code as shown. Right now it includes the wait mode. But it
    is intended to include other configurable policy decisions in the future
    like affinity.

    There are two constant instances of Policy that are created as
    POLICY_INFRASTRUCTURE and POLICY_USER as defaults for both
    infrastructure and user program resources.

    """

    def __init__(self, wait_mode, return_from_send_when):
        self._wait_mode = wait_mode
        self._return_from_send_when = return_from_send_when

    def __getstate__(self):
        return (self._wait_mode, self._return_from_send_when)

    def __setstate__(self, state):
        (wait_mode, return_from_send_when) = state
        self._wait_mode = wait_mode
        self._return_from_send_when = return_from_send_when

    def __repr__(self):
        return f"{self.__class__.__name__}({self._wait_mode, self._return_from_send_when})"

    def __str__(self):
        return repr(self)

    @property
    def wait_mode(self):
        return self._wait_mode

    @property
    def return_when(self):
        return self._return_from_send_when


POLICY_INFRASTRUCTURE = Policy(this_process.inf_wait_mode, this_process.inf_return_when_mode)
POLICY_USER = Policy(this_process.user_wait_mode, this_process.user_return_when_mode)
