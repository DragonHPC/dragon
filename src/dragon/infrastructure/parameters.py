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
    raise ValueError(f"The provided wait mode must be one of IDLE_WAIT, SPIN_WAIT, or ADAPTIVE_WAIT. The value was {wait_mode}")



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

typecast = lambda ty: lambda val: ty() if val == '' else ty(val)

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
    :param PMOD_COMMUNICATION_TIMEOUT: Timeout in seconds for PMOD communication with child MPI processes, defaults to 30.
    :type PMOD_COMMUNICATION_TIMEOUT: int
    :param BASEPOOL: Default implementation of multiprocessing pool. One of `'NATIVE'` or `'PATCHED'`, defaults to `'NATIVE'`.
    :type BASEPOOL: str
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
                name=dfacts.INF_SEG_SZ, cast=typecast(int), check=positive, default=int(dfacts.DEFAULT_SINGLE_INF_SEG_SZ)
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
            TypedParm(name=dfacts.NUM_GW_CHANNELS_PER_NODE, cast=typecast(int), check=nonnegative, default=dfacts.DRAGON_DEFAULT_NUM_GW_CHANNELS_PER_NODE),
            TypedParm(name=dfacts.HSTA_MAX_EJECTION_MB, cast=typecast(int), check=positive, default=8),
            TypedParm(name=dfacts.HSTA_MAX_GETMSG_MB, cast=typecast(int), check=positive, default=8),
            TypedParm(name=dfacts.PMOD_COMMUNICATION_TIMEOUT, cast=typecast(int), check=positive, default=30),
            TypedParm(name=dfacts.BASEPOOL, cast=typecast(str), check=check_pool, default=dfacts.DEFAULT_POOL),
            TypedParm(name=dfacts.OVERLAY_FANOUT, cast=typecast(int), check=positive, default=32),
            TypedParm(name=dfacts.NET_CONF_CACHE, cast=typecast(str), check=nocheck, default=dfacts.DEFAULT_NET_CONF_CACHE),
        ]

        env = os.environ

        try:
            num_gateways_env_var = dfacts.PREFIX + dfacts.NUM_GW_CHANNELS_PER_NODE_VAR
            num_gateways = int(env[num_gateways_env_var]) if num_gateways_env_var in env else dfacts.DRAGON_DEFAULT_NUM_GW_CHANNELS_PER_NODE
            assert(num_gateways >= 0)
        except:
            raise ValueError(f'The value of the environment variable {dfacts.NUM_GW_CHANNELS_PER_NODE} must be valid integer >= 1. It was {env[dfacts.NUM_GW_CHANNELS_PER_NODE]}')

        cls.gw_env_vars = frozenset([f'GW{x+1}' for x in range(num_gateways)])

        for gw_env_var in cls.gw_env_vars:
            PARMS.append(TypedParm(gw_env_var, cast=typecast(str), check=nocheck, default=""))

        cls.PARMS = PARMS

        cls.NODE_LOCAL_PARAMS = \
            frozenset([dfacts.DEFAULT_PD, dfacts.INF_PD, dfacts.LOCAL_SHEP_CD, dfacts.LOCAL_BE_CD, dfacts.BE_CUID]) | \
            cls.gw_env_vars


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
        num_gateways_env_var = dfacts.PREFIX+dfacts.NUM_GW_CHANNELS_PER_NODE_VAR
        assert(num_gateways >= 0), f'The number of gateways must be greater or equal to 0. It was {num_gateways}'
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
