"""Dragon infrastructure messages are the internal API used for service communication.
"""

import enum
import json
import zlib
import base64
import subprocess
from typing import Optional, Union
from dataclasses import dataclass, asdict

from ..infrastructure import channel_desc
from ..infrastructure import pool_desc
from ..infrastructure.node_desc import NodeDescriptor
from ..infrastructure import process_desc
from ..infrastructure import parameters as parms
from ..infrastructure import facts as dfacts
from ..localservices import options as dso
from ..infrastructure.util import to_str_iter
from ..utils import B64

from ..globalservices.policy_eval import ResourceLayout, Policy

from ..infrastructure import group_desc
# from ..infrastructure.policy import DefaultPolicy


# This enum class lists the type codes in infrastructure
# messages.  The values are significant for interoperability.


@enum.unique
class MessageTypes(enum.Enum):
    """
    These are the enumerated values of message type identifiers within
    the Dragon infrastructure messages.
    """
    INVALID = 0  #:
    GS_PROCESS_CREATE = 1  #:
    GS_PROCESS_CREATE_RESPONSE = 2  #:
    GS_PROCESS_LIST = 3  #:
    GS_PROCESS_LIST_RESPONSE = 4  #:
    GS_PROCESS_QUERY = 5  #:
    GS_PROCESS_QUERY_RESPONSE = 6  #:
    GS_PROCESS_KILL = 7  #:
    GS_PROCESS_KILL_RESPONSE = 8  #:
    GS_PROCESS_JOIN = 9  #:
    GS_PROCESS_JOIN_RESPONSE = 10  #:
    GS_CHANNEL_CREATE = 11  #:
    GS_CHANNEL_CREATE_RESPONSE = 12  #:
    GS_CHANNEL_LIST = 13  #:
    GS_CHANNEL_LIST_RESPONSE = 14  #:
    GS_CHANNEL_QUERY = 15  #:
    GS_CHANNEL_QUERY_RESPONSE = 16  #:
    GS_CHANNEL_DESTROY = 17  #:
    GS_CHANNEL_DESTROY_RESPONSE = 18  #:
    GS_CHANNEL_JOIN = 19  #:
    GS_CHANNEL_JOIN_RESPONSE = 20  #:
    GS_CHANNEL_DETACH = 21  #:
    GS_CHANNEL_DETACH_RESPONSE = 22  #:
    GS_CHANNEL_GET_SENDH = 23  #:
    GS_CHANNEL_GET_SENDH_RESPONSE = 24  #:
    GS_CHANNEL_GET_RECVH = 25  #:
    GS_CHANNEL_GET_RECVH_RESPONSE = 26  #:
    ABNORMAL_TERMINATION = 27  #:
    GS_STARTED = 28  #:
    GS_PING_SH = 29  #:
    GS_IS_UP = 30  #:
    GS_HEAD_EXIT = 31  #:
    GS_CHANNEL_RELEASE = 32  #:
    GS_HALTED = 33  #:
    SH_PROCESS_CREATE = 34  #:
    SH_PROCESS_CREATE_RESPONSE = 35  #:
    SH_PROCESS_KILL = 36  #:
    SH_PROCESS_EXIT = 37  #:
    SH_CHANNEL_CREATE = 38  #:
    SH_CHANNEL_CREATE_RESPONSE = 39  #:
    SH_CHANNEL_DESTROY = 40  #:
    SH_CHANNEL_DESTROY_RESPONSE = 41  #:
    SH_LOCK_CHANNEL = 42  #:
    SH_LOCK_CHANNEL_RESPONSE = 43  #:
    SH_ALLOC_MSG = 44  #:
    SH_ALLOC_MSG_RESPONSE = 45  #:
    SH_ALLOC_BLOCK = 46  #:
    SH_ALLOC_BLOCK_RESPONSE = 47  #:
    SH_IS_UP = 48  #:
    SH_CHANNELS_UP = 49  #:
    SH_PING_GS = 50  #:
    SH_HALTED = 51  #:
    SH_FWD_INPUT = 52  #:
    SH_FWD_INPUT_ERR = 53  #:
    SH_FWD_OUTPUT = 54  #:
    GS_TEARDOWN = 55  #:
    SH_TEARDOWN = 56  #:
    SH_PING_BE = 57  #:
    BE_PING_SH = 58  #:
    TA_PING_SH = 59  #:
    SH_HALT_TA = 60  #:
    TA_HALTED = 61  #:
    SH_HALT_BE = 62  #:
    BE_HALTED = 63  #:
    TA_UP = 64  #:
    GS_PING_PROC = 65  #:
    GS_DUMP = 66  #:
    SH_DUMP = 67  #:
    LA_BROADCAST = 68  #:
    LA_PASSTHRU_FB = 69  #:
    LA_PASSTHRU_BF = 70  #:
    GS_POOL_CREATE = 71  #:
    GS_POOL_CREATE_RESPONSE = 72  #:
    GS_POOL_DESTROY = 73  #:
    GS_POOL_DESTROY_RESPONSE = 74  #:
    GS_POOL_LIST = 75  #:
    GS_POOL_LIST_RESPONSE = 76  #:
    GS_POOL_QUERY = 77  #:
    GS_POOL_QUERY_RESPONSE = 78  #:
    SH_POOL_CREATE = 79  #:
    SH_POOL_CREATE_RESPONSE = 80  #:
    SH_POOL_DESTROY = 81  #:
    SH_POOL_DESTROY_RESPONSE = 82  #:
    SH_EXEC_MEM_REQUEST = 83  #:
    SH_EXEC_MEM_RESPONSE = 84  #:
    GS_UNEXPECTED = 85  #:
    LA_SERVER_MODE = 86  #:
    LA_SERVER_MODE_EXIT = 87  #:
    LA_PROCESS_DICT = 88  #:
    LA_PROCESS_DICT_RESPONSE = 89  #:
    LA_DUMP = 90  #:
    BE_NODEINDEX_SH = 91  #:
    LA_CHANNELS_INFO = 92  #:
    SH_PROCESS_KILL_RESPONSE = 93  #:
    BREAKPOINT = 94  #:
    GS_PROCESS_JOIN_LIST = 95  #:
    GS_PROCESS_JOIN_LIST_RESPONSE = 96  #:
    GS_NODE_QUERY = 97  #:
    GS_NODE_QUERY_RESPONSE = 98  #:
    LOGGING_MSG = 99  #:
    LOGGING_MSG_LIST = 100  #:
    LOG_FLUSHED = 101  #:
    GS_NODE_LIST = 102  #:
    GS_NODE_LIST_RESPONSE = 103  #:
    GS_NODE_QUERY_TOTAL_CPU_COUNT = 104  #:
    GS_NODE_QUERY_TOTAL_CPU_COUNT_RESPONSE = 105  #:
    BE_IS_UP = 106  #:
    FE_NODE_IDX_BE = 107  #:
    HALT_OVERLAY = 108  #:
    HALT_LOGGING_INFRA = 109
    OVERLAY_PING_BE = 110  #:
    OVERLAY_PING_LA = 111  #:
    LA_HALT_OVERLAY = 112  #:
    BE_HALT_OVERLAY = 113  #:
    OVERLAY_HALTED = 114  #:
    EXCEPTIONLESS_ABORT = 115  #: Communicate abnormal termination without raising exception
    LA_EXIT = 116  #:
    GS_GROUP_LIST = 117 #:
    GS_GROUP_LIST_RESPONSE = 118 #:
    GS_GROUP_QUERY = 119 #:
    GS_GROUP_QUERY_RESPONSE = 120 #:
    GS_GROUP_DESTROY = 121 #:
    GS_GROUP_DESTROY_RESPONSE = 122 #:
    GS_GROUP_ADD_TO = 123 #:
    GS_GROUP_ADD_TO_RESPONSE = 124 #:
    GS_GROUP_REMOVE_FROM = 125 #:
    GS_GROUP_REMOVE_FROM_RESPONSE = 126 #:
    GS_GROUP_CREATE = 127 #:
    GS_GROUP_CREATE_RESPONSE = 128 #:
    GS_GROUP_KILL = 129 #:
    GS_GROUP_KILL_RESPONSE = 130 #:
    GS_GROUP_CREATE_ADD_TO = 131 #:
    GS_GROUP_CREATE_ADD_TO_RESPONSE = 132 #:
    GS_GROUP_DESTROY_REMOVE_FROM = 133 #:
    GS_GROUP_DESTROY_REMOVE_FROM_RESPONSE = 134 #:
    HSTA_UPDATE_NODES = 135  #:

@enum.unique
class FileDescriptor(enum.Enum):
    stdin = 0
    stdout = 1
    stderr = 2


PIPE = subprocess.PIPE
STDOUT = subprocess.STDOUT
DEVNULL = subprocess.DEVNULL


class AbnormalTerminationError(Exception):

    def __init__(self, msg=''):
        self._msg = msg

    def __str__(self):
        return f'{self._msg}'

    def __repr__(self):
        return f"{str(__class__)}({repr(self._msg)})"

@dataclass
class PMIInfo():
    """
    Required information to enable the launching of pmi based applications.
    """

    job_id: int
    lrank: int
    ppn: int
    nid: int
    nnodes: int
    nranks: int
    nidlist: list[int]
    hostlist: list[str]
    control_port: int
    pid_base: int

    @classmethod
    def fromdict(cls, d):
        try:
            return cls(**d)
        except Exception as exc:
            raise ValueError(f'Error deserializing {cls.__name__} {d=}') from exc


class _MsgBase(object):
    """Common base for all messages.

        This common base type for all messages sets up the
        default fields and the serialization strategy for
        now.
    """

    _tc = MessageTypes.INVALID  # deliberately invalid value, overridden

    @enum.unique
    class Errors(enum.Enum):
        INVALID = -1  # deliberately invalid, overridden

    def __init__(self, tag, ref=None, err=None):
        assert isinstance(tag, int)

        self._tag = int(tag)

        if ref is None:
            self._ref = None
        else:
            self._ref = int(ref)

        if err is not None:
            if isinstance(err, self.Errors):
                self._err = err
            elif isinstance(err, int):
                self._err = self.Errors(err)
            else:
                raise NotImplementedError('invalid error parameter')
        else:
            self._err = err

    def get_sdict(self):

        rv = {'_tc': self._tc.value,
              'tag': self.tag}

        if self.err is not None:
            rv['err'] = self.err.value

        if self.ref is not None:
            assert isinstance(self.ref, int)
            rv['ref'] = self.ref

        return rv

    @property
    def tc(self):
        return self._tc

    @classmethod
    def tcv(cls):
        return cls._tc.value

    @property
    def tag(self):
        return self._tag

    @tag.setter
    def tag(self, value):
        self._tag = value

    @property
    def ref(self):
        return self._ref

    @property
    def err(self):
        return self._err

    # the keys in the serialization dictionary must match up
    # with the arguments in the __init__ constructor
    # for all the subclasses
    @classmethod
    def from_sdict(cls, sdict):
        return cls(**sdict)

    def uncompressed_serialize(self):
        return json.dumps(self.get_sdict())

    def serialize(self):
        j = {'COMPRESSED': base64.b64encode(zlib.compress(json.dumps(self.get_sdict()).encode('utf-8'))).decode('ascii')}
        return json.dumps(j)

    def __str__(self):
        cn = self.__class__.__name__
        msg = f'{cn}: {self.tag}'
        if hasattr(self, 'p_uid'):
            msg += f' {self.p_uid}'

        if hasattr(self, 'r_c_uid'):
            msg += f'->{self.r_c_uid}'
        return msg

    def __repr__(self):
        fields_to_set = self.get_sdict()
        del fields_to_set['_tc']
        fs = ', '.join([f'{k!s}={v!r}' for k, v in fields_to_set.items()])
        return f'{self.__class__.__name__}({fs})'


# class setup methodology:
# 1) the _tc class variable has the value of the typecode
# for this class.
#
# 2) To allow keyword-initialization of the object
# from a dictionary sent over the wire - which will have the
# _tc member in it - we need to repeat the _tc parameter
# simply to allow __init__ to absorb that parameter without
# an error. We could take the _tc out manually
# in parse, but this would mutate the dictionary which
# is worth avoiding.
#
# 3) This setup lets one initialize the object normally.  In
#    the object constructor the _tc parameter is ignored, and
#    in the serialization the typecode is gotten from the
#    class attribute _tc.

class GSProcessCreate(_MsgBase):
    """
        Refer to :ref:`definition<gsprocesscreate>`
        and :ref:`Common Fields<cfs>` for a description of
        the message structure.
    """

    _tc = MessageTypes.GS_PROCESS_CREATE

    def __init__(self, tag, p_uid, r_c_uid, exe, args, env=None, rundir='',
                 user_name='', options=None, stdin=None, stdout=None, stderr=None,
                 group=None, user=None, umask=- 1, pipesize=None, pmi_required=False,
                 _pmi_info=None, layout=None, policy=None, _tc=None):

        # Coerce args to a list of strings
        args = list(to_str_iter(args))

        super().__init__(tag)

        if options is None:
            options = {}

        if env is None:
            env = {}

        self.p_uid = p_uid
        self.r_c_uid = int(r_c_uid)
        self.exe = exe
        self.args = args
        self.env = env
        self.rundir = rundir
        self.user_name = user_name
        self.options = options
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        self.group = group
        self.user = user
        self.umask = umask
        self.pipesize = pipesize

        self.pmi_required = pmi_required

        if _pmi_info is None:
            self._pmi_info = None
        elif isinstance(_pmi_info, dict):
            self._pmi_info = PMIInfo.fromdict(_pmi_info)
        elif isinstance(_pmi_info, PMIInfo):
            self._pmi_info = _pmi_info
        else:
            raise ValueError(f'GS unsupported _pmi_info value {_pmi_info=}')

        if layout is None:
            self.layout = None
        elif isinstance(layout, dict):
            self.layout = ResourceLayout(**layout)
        elif isinstance(layout, ResourceLayout):
            self.layout = layout
        else:
            raise ValueError(f'GS unsupported layout value {layout=}')

        if policy is None:
            self.policy = None
        elif isinstance(policy, dict):
            self.policy = Policy(**policy)
        elif isinstance(policy, Policy):
            self.policy = policy
        else:
            raise ValueError(f'GS unsupported policy value {policy=}')

    @property
    def options(self):
        return self._options

    @options.setter
    def options(self, value):

        if isinstance(value, process_desc.ProcessOptions):
            self._options = value
        else:
            self._options = process_desc.ProcessOptions.from_sdict(value)

    def get_sdict(self):
        rv = super().get_sdict()

        rv['p_uid'] = self.p_uid
        rv['r_c_uid'] = self.r_c_uid
        rv['exe'] = self.exe
        rv['args'] = self.args
        rv['env'] = self.env
        rv['rundir'] = self.rundir
        rv['user_name'] = self.user_name
        rv['options'] = self.options.get_sdict()
        rv['stdin'] = self.stdin
        rv['stdout'] = self.stdout
        rv['stderr'] = self.stderr
        rv['group'] = self.group
        rv['user'] = self.user
        rv['umask'] = self.umask
        rv['pipesize'] = self.pipesize
        rv['pmi_required'] = self.pmi_required
        rv['_pmi_info'] = None if self._pmi_info is None else asdict(self._pmi_info)
        rv['layout'] = None if self.layout is None else asdict(self.layout)
        rv['policy'] = None if self.policy is None else asdict(self.policy)

        return rv

    def __str__(self):
        return super().__str__() + f'{self.exe} {self.args}'


class GSProcessCreateResponse(_MsgBase):
    """
        Refer to :ref:`definition<gsprocesscreateresponse>`
        and :ref:`Common Fields<cfs>` for a
        description of the message structure.
    """

    _tc = MessageTypes.GS_PROCESS_CREATE_RESPONSE

    @enum.unique
    class Errors(enum.Enum):
        SUCCESS = 0  #: Process was created
        FAIL = 1  #: Process was not created
        ALREADY = 2  #: Process exists already

    def __init__(self, tag, ref, err, desc=None, err_info='', _tc=None):
        super().__init__(tag, ref, err)

        if self.Errors.SUCCESS == self.err or self.Errors.ALREADY == self.err:
            self.desc = desc
        elif self.err == self.Errors.FAIL:
            self.err_info = err_info
        else:
            raise NotImplementedError('missing case')

    @property
    def desc(self):
        return self._desc

    @desc.setter
    def desc(self, value):
        if isinstance(value, process_desc.ProcessDescriptor):
            self._desc = value
        else:
            self._desc = process_desc.ProcessDescriptor.from_sdict(value)

    def get_sdict(self):
        rv = super().get_sdict()

        if self.err == self.Errors.SUCCESS or self.err == self.Errors.ALREADY:
            rv['desc'] = self.desc.get_sdict()
        elif self.err == self.Errors.FAIL:
            rv['err_info'] = self.err_info

        return rv


class GSProcessList(_MsgBase):
    """
        Refer to :ref:`definition<gsprocesslist>` and :ref:`Common Fields<cfs>` for a description of
        the message structure.
    """

    _tc = MessageTypes.GS_PROCESS_LIST

    def __init__(self, tag, p_uid, r_c_uid, _tc=None):
        super().__init__(tag)
        self.p_uid = p_uid
        self.r_c_uid = int(r_c_uid)

    def get_sdict(self):
        rv = super().get_sdict()
        rv['p_uid'] = self.p_uid
        rv['r_c_uid'] = self.r_c_uid
        return rv


class GSProcessListResponse(_MsgBase):
    """
        Refer to :ref:`definition<gsprocesslistresponse>` and :ref:`Common Fields<cfs>` for a
        description of the message structure.

    """

    _tc = MessageTypes.GS_PROCESS_LIST_RESPONSE

    @enum.unique
    class Errors(enum.Enum):
        SUCCESS = 0  #: Always succeeds

    def __init__(self, tag, ref, err, plist=None, _tc=None):
        super().__init__(tag, ref, err)

        if plist is None:
            self.plist = []
        else:
            self.plist = list(plist)

    def get_sdict(self):
        rv = super().get_sdict()
        rv['plist'] = self.plist
        return rv


class GSProcessQuery(_MsgBase):
    """
        Refer to :ref:`definition<gsprocessquery>` and :ref:`Common Fields<cfs>`
        for a description of the message structure.
    """

    _tc = MessageTypes.GS_PROCESS_QUERY

    def __init__(self, tag, p_uid, r_c_uid,
                 t_p_uid=None, user_name='', _tc=None):
        super().__init__(tag)
        self.p_uid = int(p_uid)
        self.r_c_uid = int(r_c_uid)

        if t_p_uid is not None:
            self.t_p_uid = int(t_p_uid)
        else:
            self.t_p_uid = t_p_uid

        self.user_name = user_name

    def get_sdict(self):
        rv = super().get_sdict()
        rv['p_uid'] = self.p_uid
        rv['r_c_uid'] = self.r_c_uid

        if self.t_p_uid is not None:
            rv['t_p_uid'] = self.t_p_uid
        else:
            rv['user_name'] = self.user_name

        return rv


class GSProcessQueryResponse(_MsgBase):
    """
            Refer to :ref:`definition<gsprocessqueryresponse>` and :ref:`Common Fields<cfs>` for a
            description of the message structure.

    """

    _tc = MessageTypes.GS_PROCESS_QUERY_RESPONSE

    @enum.unique
    class Errors(enum.Enum):
        SUCCESS = 0  #:
        UNKNOWN = 1  #:

    def __init__(self, tag, ref, err, desc=None, err_info='', _tc=None):
        super().__init__(tag, ref, err)

        if desc is None:
            desc = {}
            self._desc = None

        if self.Errors.SUCCESS == self.err:
            self.desc = desc
        elif self.Errors.UNKNOWN == self.err:
            self.err_info = err_info
        else:
            raise NotImplementedError('close case')

    @property
    def desc(self):
        return self._desc

    @desc.setter
    def desc(self, value):
        if isinstance(value, process_desc.ProcessDescriptor):
            self._desc = value
        else:
            self._desc = process_desc.ProcessDescriptor.from_sdict(value)

    def get_sdict(self):
        rv = super().get_sdict()
        if self.Errors.SUCCESS == self.err:
            rv['desc'] = self.desc.get_sdict()
        else:
            rv['err_info'] = self.err_info

        return rv


class GSProcessKill(_MsgBase):
    """
            Refer to :ref:`definition<gsprocesskill>` and :ref:`Common Fields<cfs>` for a description of
            the message structure.

    """

    _tc = MessageTypes.GS_PROCESS_KILL

    def __init__(self, tag, p_uid, r_c_uid, sig, t_p_uid=None, user_name='', _tc=None):
        super().__init__(tag)
        self.p_uid = int(p_uid)
        self.r_c_uid = int(r_c_uid)
        self.t_p_uid = t_p_uid
        self.sig = int(sig)
        self.user_name = user_name

    def get_sdict(self):
        rv = super().get_sdict()
        rv['p_uid'] = self.p_uid
        rv['r_c_uid'] = self.r_c_uid
        rv['sig'] = self.sig

        if self.t_p_uid is not None:
            rv['t_p_uid'] = self.t_p_uid
        else:
            rv['user_name'] = self.user_name

        return rv


class GSProcessKillResponse(_MsgBase):
    """
            Refer to :ref:`definition<gsprocesskillresponse>` and :ref:`Common Fields<cfs>` for a
            description of the message structure.

    """

    _tc = MessageTypes.GS_PROCESS_KILL_RESPONSE

    @enum.unique
    class Errors(enum.Enum):
        SUCCESS = 0  #:
        UNKNOWN = 1  #:
        FAIL_KILL = 2  #:
        DEAD = 3  #:
        PENDING = 4  #:

    def __init__(self, tag, ref, err, exit_code=0, err_info='', _tc=None):
        super().__init__(tag, ref, err)
        self.err_info = err_info
        self.exit_code = exit_code

    def get_sdict(self):
        rv = super().get_sdict()
        if (self.Errors.UNKNOWN == self.err or
                self.Errors.FAIL_KILL == self.err):
            rv['err_info'] = self.err_info
        else:
            rv['exit_code'] = self.exit_code

        return rv


class GSProcessJoin(_MsgBase):
    """
            Refer to :ref:`definition<gsprocessjoin>` and :ref:`Common Fields<cfs>` for a description of
            the message structure.

    """

    _tc = MessageTypes.GS_PROCESS_JOIN

    def __init__(self, tag, p_uid, r_c_uid, timeout=-1, t_p_uid=None, user_name='', _tc=None):
        super().__init__(tag)
        self.p_uid = int(p_uid)
        self.r_c_uid = int(r_c_uid)
        self.t_p_uid = t_p_uid
        self.timeout = timeout
        self.user_name = user_name

    def get_sdict(self):
        rv = super().get_sdict()
        rv['p_uid'] = self.p_uid
        rv['r_c_uid'] = self.r_c_uid

        rv['timeout'] = self.timeout

        if self.t_p_uid is not None:
            rv['t_p_uid'] = self.t_p_uid
        else:
            rv['user_name'] = self.user_name

        return rv

    def __str__(self):
        first = super().__str__()
        return first + f' {self.t_p_uid}:{self.user_name}'


class GSProcessJoinResponse(_MsgBase):
    """
            Refer to :ref:`definition<gsprocessjoinresponse>` and :ref:`Common Fields<cfs>` for a
            description of the message structure.

    """

    _tc = MessageTypes.GS_PROCESS_JOIN_RESPONSE

    @enum.unique
    class Errors(enum.Enum):
        SUCCESS = 0  #:
        UNKNOWN = 1  #:
        TIMEOUT = 2  #:
        SELF = 3  #:

    def __init__(self, tag, ref, err, exit_code=0, err_info='', _tc=None):
        super().__init__(tag, ref, err)
        self.err_info = err_info
        self.exit_code = exit_code

    def get_sdict(self):
        rv = super().get_sdict()

        if self.Errors.SUCCESS == self.err:
            rv['exit_code'] = self.exit_code
        elif self.Errors.UNKNOWN == self.err:
            rv['err_info'] = self.err_info
        elif self.Errors.SELF == self.err:
            rv['err_info'] = self.err_info

        return rv

    def __str__(self):
        msg = super().__str__()

        if self.Errors.SUCCESS == self.err:
            msg += f' exit_code: {self.exit_code}'
        elif self.Errors.UNKNOWN == self.err:
            msg += f' unknown: {self.err_info}'
        else:
            msg += f' timeout'

        return msg


class GSProcessJoinList(_MsgBase):
    """
            Refer to :ref:`definition<gsprocessjoinlist>` and :ref:`Common Fields<cfs>` for a description of
            the message structure.

    """

    _tc = MessageTypes.GS_PROCESS_JOIN_LIST

    def __init__(self, tag, p_uid, r_c_uid, timeout=-1, t_p_uid_list=None, user_name_list=None, join_all=False, _tc=None):
        super().__init__(tag)
        self.p_uid = int(p_uid)
        self.r_c_uid = int(r_c_uid)
        if t_p_uid_list is None:
            t_p_uid_list = []
        self.t_p_uid_list = t_p_uid_list
        if user_name_list is None:
            user_name_list = []
        self.user_name_list = user_name_list
        self.timeout = timeout
        self.join_all = join_all

    def get_sdict(self):
        rv = super().get_sdict()
        rv['p_uid'] = self.p_uid
        rv['r_c_uid'] = self.r_c_uid
        rv['timeout'] = self.timeout
        rv['join_all'] = self.join_all
        if self.t_p_uid_list:
            rv['t_p_uid_list'] = self.t_p_uid_list
        if self.user_name_list:
            rv['user_name_list'] = self.user_name_list

        return rv

    def __str__(self):
        first = super().__str__()
        return first + f' {self.t_p_uid_list}:{self.user_name_list}'


class GSProcessJoinListResponse(_MsgBase):
    """
            Refer to :ref:`definition<gsprocessjoinlistresponse>` and :ref:`Common Fields<cfs>` for a
            description of the message structure.

    """

    _tc = MessageTypes.GS_PROCESS_JOIN_LIST_RESPONSE

    @enum.unique
    class Errors(enum.Enum):
        SUCCESS = 0  #:
        UNKNOWN = 1  #:
        TIMEOUT = 2  #:
        SELF = 3  #:
        PENDING = 4  #:

    def __init__(self, tag, ref, puid_status, _tc=None):
        super().__init__(tag, ref)
        self.puid_status = puid_status

    def get_sdict(self):
        rv = super().get_sdict()
        rv['puid_status'] = self.puid_status

        return rv

    def __str__(self):
        msg = super().__str__()
        msg += f' ref: {self.ref}, puid_status: {self.puid_status}'

        return msg


class GSPoolCreate(_MsgBase):
    """
            Refer to :ref:`definition<gspoolcreate>` and :ref:`Common Fields<cfs>` for a description of
            the message structure.

    """

    _tc = MessageTypes.GS_POOL_CREATE

    def __init__(self, tag, p_uid, r_c_uid, size, user_name='', options=None, _tc=None):
        super().__init__(tag)

        if options is None:
            options = {}

        self.p_uid = p_uid
        self.r_c_uid = int(r_c_uid)
        self.size = int(size)
        self.user_name = user_name
        self.options = options

    @property
    def options(self):
        return self._options

    @options.setter
    def options(self, value):

        if isinstance(value, pool_desc.PoolOptions):
            self._options = value
        else:
            self._options = pool_desc.PoolOptions.from_sdict(value)

    def get_sdict(self):
        rv = super().get_sdict()

        rv['p_uid'] = self.p_uid
        rv['r_c_uid'] = self.r_c_uid
        rv['size'] = self.size
        rv['user_name'] = self.user_name
        rv['options'] = self.options.get_sdict()
        return rv


class GSPoolCreateResponse(_MsgBase):
    """
            Refer to :ref:`definition<gspoolcreateresponse>` and :ref:`Common Fields<cfs>` for a
            description of the message structure.

    """

    _tc = MessageTypes.GS_POOL_CREATE_RESPONSE

    @enum.unique
    class Errors(enum.Enum):
        SUCCESS = 0  #: Pool was created
        FAIL = 1  #: Pool was not created
        ALREADY = 2  #: Pool exists already

    def __init__(self, tag, ref, err, err_code=0, desc=None, err_info='', _tc=None):
        super().__init__(tag, ref, err)

        if self.Errors.SUCCESS == self.err or self.Errors.ALREADY == self.err:
            self.desc = desc
        elif self.err == self.Errors.FAIL:
            self.err_info = err_info
            self.err_code = err_code
        else:
            raise NotImplementedError('missing case')

    @property
    def desc(self):
        return self._desc

    @desc.setter
    def desc(self, value):
        if isinstance(value, pool_desc.PoolDescriptor):
            self._desc = value
        else:
            self._desc = pool_desc.PoolDescriptor.from_sdict(value)

    def get_sdict(self):
        rv = super().get_sdict()

        if self.err == self.Errors.SUCCESS or self.err == self.Errors.ALREADY:
            rv['desc'] = self.desc.get_sdict()
        elif self.err == self.Errors.FAIL:
            rv['err_info'] = self.err_info
            rv['err_code'] = self.err_code

        return rv


class GSPoolList(_MsgBase):
    """
            Refer to :ref:`definition<gspoollist>` and :ref:`Common Fields<cfs>` for a description of the
            message structure.

    """

    _tc = MessageTypes.GS_POOL_LIST

    def __init__(self, tag, p_uid, r_c_uid,
                 _tc=None):
        super().__init__(tag)
        self.p_uid = p_uid
        self.r_c_uid = int(r_c_uid)

    def get_sdict(self):
        rv = super().get_sdict()
        rv['p_uid'] = self.p_uid
        rv['r_c_uid'] = self.r_c_uid
        return rv


class GSPoolListResponse(_MsgBase):
    """
            Refer to :ref:`definition<gspoollistresponse>` and :ref:`Common Fields<cfs>` for a description
            of the message structure.

    """

    _tc = MessageTypes.GS_POOL_LIST_RESPONSE

    @enum.unique
    class Errors(enum.Enum):
        SUCCESS = 0  #: Always succeeds

    def __init__(self, tag, ref, err, mlist=None, _tc=None):
        super().__init__(tag, ref, err)

        if mlist is None:
            self.mlist = []
        else:
            self.mlist = list(mlist)

    def get_sdict(self):
        rv = super().get_sdict()
        rv['mlist'] = self.mlist
        return rv


class GSPoolQuery(_MsgBase):
    """
            Refer to :ref:`definition<gspoolquery>` and :ref:`Common Fields<cfs>` for a description of the
            message structure.

    """

    _tc = MessageTypes.GS_POOL_QUERY

    def __init__(self, tag, p_uid, r_c_uid,
                 m_uid=None, user_name='', _tc=None):
        super().__init__(tag)
        self.p_uid = p_uid
        self.r_c_uid = int(r_c_uid)

        if m_uid is not None:
            self.m_uid = int(m_uid)
        else:
            self.m_uid = m_uid

        self.user_name = user_name

    def get_sdict(self):
        rv = super().get_sdict()
        rv['p_uid'] = self.p_uid
        rv['r_c_uid'] = self.r_c_uid

        if self.m_uid is not None:
            rv['m_uid'] = self.m_uid
        else:
            rv['user_name'] = self.user_name

        return rv


class GSPoolQueryResponse(_MsgBase):
    """
            Refer to :ref:`definition<gspoolqueryresponse>` and :ref:`Common Fields<cfs>` for a
            description of the message structure.

    """

    _tc = MessageTypes.GS_POOL_QUERY_RESPONSE

    @enum.unique
    class Errors(enum.Enum):
        SUCCESS = 0  #:
        UNKNOWN = 1  #:

    def __init__(self, tag, ref, err, desc=None, err_info='', _tc=None):
        super().__init__(tag, ref, err)

        if desc is None:
            desc = {}
            self._desc = None

        if self.Errors.SUCCESS == self.err:
            self.desc = desc
        elif self.Errors.UNKNOWN == self.err:
            self.err_info = err_info
        else:
            raise NotImplementedError('close case')

    @property
    def desc(self):
        return self._desc

    @desc.setter
    def desc(self, value):
        if isinstance(value, pool_desc.PoolDescriptor):
            self._desc = value
        else:
            self._desc = pool_desc.PoolDescriptor.from_sdict(value)

    def get_sdict(self):
        rv = super().get_sdict()
        if self.Errors.SUCCESS == self.err:
            rv['desc'] = self.desc.get_sdict()
        else:
            rv['err_info'] = self.err_info

        return rv


class GSPoolDestroy(_MsgBase):
    """
            Refer to :ref:`definition<gspooldestroy>` and :ref:`Common Fields<cfs>` for a description of
            the message structure.

    """

    _tc = MessageTypes.GS_POOL_DESTROY

    def __init__(self, tag, p_uid, r_c_uid, m_uid=0, user_name='', _tc=None):
        super().__init__(tag)
        self.p_uid = p_uid
        self.r_c_uid = int(r_c_uid)
        self.m_uid = m_uid
        self.user_name = user_name

    def get_sdict(self):
        rv = super().get_sdict()
        rv['p_uid'] = self.p_uid
        rv['r_c_uid'] = self.r_c_uid

        if self.m_uid is not None:
            rv['m_uid'] = self.m_uid
        else:
            rv['user_name'] = self.user_name

        return rv


class GSPoolDestroyResponse(_MsgBase):
    """
            Refer to :ref:`definition<gspooldestroyresponse>` and :ref:`Common Fields<cfs>` for a
            description of the message structure.

    """

    _tc = MessageTypes.GS_POOL_DESTROY_RESPONSE

    @enum.unique
    class Errors(enum.Enum):
        SUCCESS = 0  #:
        UNKNOWN = 1  #:
        FAIL = 2  #:
        GONE = 3  #:
        PENDING = 4  #:

    def __init__(self, tag, ref, err, err_code=0, err_info='', _tc=None):
        super().__init__(tag, ref, err)
        self.err_info = err_info
        self.err_code = err_code

    def get_sdict(self):
        rv = super().get_sdict()

        if (self.Errors.UNKNOWN == self.err or
                self.Errors.FAIL == self.err):
            rv['err_info'] = self.err_info
            rv['err_code'] = self.err_code

        return rv


class GSGroupCreate(_MsgBase):
    """
            Refer to :ref:`definition<gsgroupcreate>` and :ref:`Common Fields<cfs>` for a description of
            the message structure.

    """

    _tc = MessageTypes.GS_GROUP_CREATE

    def __init__(self, tag, p_uid, r_c_uid, items=None, policy=None, user_name='', _tc=None):
        super().__init__(tag)

        if items is None:
            items = []

        self.p_uid = p_uid
        self.r_c_uid = int(r_c_uid)
        self.items = list(items)
        self.user_name = user_name

        if policy is None:
            self.policy = None
        elif isinstance(policy, dict):
            self.policy = Policy(**policy)
        elif isinstance(policy, Policy):
            self.policy = policy
        else:
            raise ValueError(f'GS Groups unsupported policy value {policy=}')

    def get_sdict(self):
        rv = super().get_sdict()

        rv['p_uid'] = self.p_uid
        rv['r_c_uid'] = self.r_c_uid
        rv['items'] = self.items
        rv['policy'] = self.policy.get_sdict()
        rv['user_name'] = self.user_name

        return rv


class GSGroupCreateResponse(_MsgBase):
    """
            Refer to :ref:`definition<gsgroupcreateresponse>` and :ref:`Common Fields<cfs>` for a
            description of the message structure.

    """

    _tc = MessageTypes.GS_GROUP_CREATE_RESPONSE

    def __init__(self, tag, ref, desc=None, _tc=None):
        super().__init__(tag, ref)

        if desc is None:
            desc = {}
            self._desc = None
        self.desc = desc

    @property
    def desc(self):
        return self._desc

    @desc.setter
    def desc(self, value):
        if isinstance(value, group_desc.GroupDescriptor):
            self._desc = value
        else:
            self._desc = group_desc.GroupDescriptor.from_sdict(value)

    def get_sdict(self):
        rv = super().get_sdict()
        rv['desc'] = None if self.desc is None else self.desc.get_sdict()
        return rv

class GSGroupList(_MsgBase):
    """
            Refer to :ref:`definition<gsgrouplist>` and :ref:`Common Fields<cfs>` for a description of the
            message structure.

    """

    _tc = MessageTypes.GS_GROUP_LIST

    def __init__(self, tag, p_uid, r_c_uid, _tc=None):
        super().__init__(tag)
        self.p_uid = p_uid
        self.r_c_uid = int(r_c_uid)

    def get_sdict(self):
        rv = super().get_sdict()
        rv['p_uid'] = self.p_uid
        rv['r_c_uid'] = self.r_c_uid
        return rv


class GSGroupListResponse(_MsgBase):
    """
            Refer to :ref:`definition<gsgrouplistresponse>` and :ref:`Common Fields<cfs>` for a description
            of the message structure.

    """

    _tc = MessageTypes.GS_GROUP_LIST_RESPONSE

    @enum.unique
    class Errors(enum.Enum):
        SUCCESS = 0  #: Always succeeds

    def __init__(self, tag, ref, err, glist=None, _tc=None):
        super().__init__(tag, ref, err)

        if glist is None:
            self.glist = []
        else:
            self.glist = list(glist)

    def get_sdict(self):
        rv = super().get_sdict()
        rv['glist'] = self.glist
        return rv


class GSGroupQuery(_MsgBase):
    """
            Refer to :ref:`definition<gsgroupquery>` and :ref:`Common Fields<cfs>` for a description of the
            message structure.

    """

    _tc = MessageTypes.GS_GROUP_QUERY

    def __init__(self, tag, p_uid, r_c_uid,
                 g_uid=None, user_name='', _tc=None):
        super().__init__(tag)
        self.p_uid = p_uid
        self.r_c_uid = int(r_c_uid)

        if g_uid is not None:
            self.g_uid = int(g_uid)
        else:
            self.g_uid = g_uid

        self.user_name = user_name

    def get_sdict(self):
        rv = super().get_sdict()
        rv['p_uid'] = self.p_uid
        rv['r_c_uid'] = self.r_c_uid

        if self.g_uid is not None:
            rv['g_uid'] = self.g_uid
        else:
            rv['user_name'] = self.user_name

        return rv


class GSGroupQueryResponse(_MsgBase):
    """
            Refer to :ref:`definition<gsgroupqueryresponse>` and :ref:`Common Fields<cfs>` for a
            description of the message structure.

    """

    _tc = MessageTypes.GS_GROUP_QUERY_RESPONSE

    @enum.unique
    class Errors(enum.Enum):
        SUCCESS = 0  #:
        UNKNOWN = 1  #:

    def __init__(self, tag, ref, err, desc=None, err_info='', _tc=None):
        super().__init__(tag, ref, err)

        if desc is None:
            desc = {}
            self._desc = None

        if self.Errors.SUCCESS == self.err:
            self.desc = desc
        elif self.Errors.UNKNOWN == self.err:
            self.err_info = err_info
        else:
            raise NotImplementedError('close case')

    @property
    def desc(self):
        return self._desc

    @desc.setter
    def desc(self, value):
        if isinstance(value, group_desc.GroupDescriptor):
            self._desc = value
        else:
            self._desc = group_desc.GroupDescriptor.from_sdict(value)

    def get_sdict(self):
        rv = super().get_sdict()
        if self.Errors.SUCCESS == self.err:
            rv['desc'] = self.desc.get_sdict()
        else:
            rv['err_info'] = self.err_info

        return rv


class GSGroupKill(_MsgBase):
    """
            Refer to :ref:`definition<gsgroupkill>` and :ref:`Common Fields<cfs>` for a description of
            the message structure.

    """

    _tc = MessageTypes.GS_GROUP_KILL

    def __init__(self, tag, p_uid, r_c_uid, sig, g_uid=None, user_name='', _tc=None):
        super().__init__(tag)
        self.p_uid = p_uid
        self.r_c_uid = int(r_c_uid)
        self.sig = int(sig)
        self.g_uid = g_uid
        self.user_name = user_name

    def get_sdict(self):
        rv = super().get_sdict()
        rv['p_uid'] = self.p_uid
        rv['r_c_uid'] = self.r_c_uid
        rv['sig'] = self.sig

        if self.g_uid is not None:
            rv['g_uid'] = self.g_uid
        else:
            rv['user_name'] = self.user_name

        return rv


class GSGroupKillResponse(_MsgBase):
    """
            Refer to :ref:`definition<gsgroupkillresponse>` and :ref:`Common Fields<cfs>` for a
            description of the message structure.

    """

    _tc = MessageTypes.GS_GROUP_KILL_RESPONSE

    @enum.unique
    class Errors(enum.IntEnum):
        SUCCESS = 0  #:
        UNKNOWN = 1  #:
        DEAD = 2  #:
        PENDING = 3  #:
        ALREADY = 4  #:

    def __init__(self, tag, ref, err, err_info='', desc=None, _tc=None):
        super().__init__(tag, ref, err)
        self.err_info = err_info

        if desc is None:
            desc = {}
            self._desc = None
        self.desc = desc

    @property
    def desc(self):
        return self._desc

    @desc.setter
    def desc(self, value):
        if isinstance(value, group_desc.GroupDescriptor):
            self._desc = value
        else:
            self._desc = group_desc.GroupDescriptor.from_sdict(value)

    def get_sdict(self):
        rv = super().get_sdict()

        if self.Errors.UNKNOWN == self.err:
            rv['err_info'] = self.err_info
        if self.Errors.SUCCESS == self.err or self.Errors.ALREADY == self.err:
            rv['desc'] = None if self.desc is None else self.desc.get_sdict()

        return rv


class GSGroupDestroy(_MsgBase):
    """
            Refer to :ref:`definition<gsgroupdestroy>` and :ref:`Common Fields<cfs>` for a description of
            the message structure.

    """

    _tc = MessageTypes.GS_GROUP_DESTROY

    def __init__(self, tag, p_uid, r_c_uid, g_uid=None, user_name='', _tc=None):
        super().__init__(tag)
        self.p_uid = p_uid
        self.r_c_uid = int(r_c_uid)
        self.g_uid = g_uid
        self.user_name = user_name

    def get_sdict(self):
        rv = super().get_sdict()
        rv['p_uid'] = self.p_uid
        rv['r_c_uid'] = self.r_c_uid

        if self.g_uid is not None:
            rv['g_uid'] = self.g_uid
        else:
            rv['user_name'] = self.user_name

        return rv


class GSGroupDestroyResponse(_MsgBase):
    """
            Refer to :ref:`definition<gsgroupdestroyresponse>` and :ref:`Common Fields<cfs>` for a
            description of the message structure.

    """

    _tc = MessageTypes.GS_GROUP_DESTROY_RESPONSE

    @enum.unique
    class Errors(enum.IntEnum):
        SUCCESS = 0  #:
        UNKNOWN = 1  #:
        DEAD = 3  #:
        PENDING = 4  #:

    def __init__(self, tag, ref, err, err_info='', desc=None, _tc=None):
        super().__init__(tag, ref, err)
        self.err_info = err_info

        if desc is None:
            desc = {}
            self._desc = None
        self.desc = desc

    @property
    def desc(self):
        return self._desc

    @desc.setter
    def desc(self, value):
        if isinstance(value, group_desc.GroupDescriptor):
            self._desc = value
        else:
            self._desc = group_desc.GroupDescriptor.from_sdict(value)

    def get_sdict(self):
        rv = super().get_sdict()

        if self.Errors.UNKNOWN == self.err :
            rv['err_info'] = self.err_info
        if self.Errors.SUCCESS == self.err:
            rv['desc'] = None if self.desc is None else self.desc.get_sdict()

        return rv


class GSGroupAddTo(_MsgBase):
    """
            Refer to :ref:`definition<gsgroupaddto>` and :ref:`Common Fields<cfs>` for a description of
            the message structure.

    """

    _tc = MessageTypes.GS_GROUP_ADD_TO

    def __init__(self, tag, p_uid, r_c_uid, g_uid=None, user_name='', items=None, _tc=None):
        super().__init__(tag)

        if items is None:
            items = []

        self.p_uid = p_uid
        self.r_c_uid = int(r_c_uid)
        self.g_uid = g_uid
        self.user_name = user_name
        self.items = list(items)

    def get_sdict(self):
        rv = super().get_sdict()
        rv['p_uid'] = self.p_uid
        rv['r_c_uid'] = self.r_c_uid

        if self.g_uid is not None:
            rv['g_uid'] = self.g_uid
        else:
            rv['user_name'] = self.user_name

        rv['items'] = self.items

        return rv


class GSGroupAddToResponse(_MsgBase):
    """
            Refer to :ref:`definition<gsgroupaddtoresponse>` and :ref:`Common Fields<cfs>` for a
            description of the message structure.

    """

    _tc = MessageTypes.GS_GROUP_ADD_TO_RESPONSE

    @enum.unique
    class Errors(enum.Enum):
        SUCCESS = 0  #:
        UNKNOWN = 1  #:
        FAIL = 2  #:
        DEAD = 3  #:
        PENDING = 4  #:

    def __init__(self, tag, ref, err, err_info='', desc=None, _tc=None):
        super().__init__(tag, ref, err)

        self.err_info = err_info

        if desc is None:
            desc = {}
            self._desc = None
        self.desc = desc

    @property
    def desc(self):
        return self._desc

    @desc.setter
    def desc(self, value):
        if isinstance(value, group_desc.GroupDescriptor):
            self._desc = value
        else:
            self._desc = group_desc.GroupDescriptor.from_sdict(value)

    def get_sdict(self):
        rv = super().get_sdict()

        if (self.Errors.UNKNOWN == self.err or
                self.Errors.FAIL == self.err):
            rv['err_info'] = self.err_info

        rv['desc'] = None if self.desc is None else self.desc.get_sdict()

        return rv

class GSGroupCreateAddTo(_MsgBase):
    """
            Refer to :ref:`definition<gsgroupcreateaddto>` and :ref:`Common Fields<cfs>` for a description of
            the message structure.

    """

    _tc = MessageTypes.GS_GROUP_CREATE_ADD_TO

    def __init__(self, tag, p_uid, r_c_uid, g_uid=None, user_name='', items=None, policy=None, _tc=None):
        super().__init__(tag)

        if items is None:
            items = []

        self.p_uid = p_uid
        self.r_c_uid = int(r_c_uid)
        self.g_uid = g_uid
        self.user_name = user_name
        self.items = list(items)

        if policy is None:
            self.policy = None
        elif isinstance(policy, dict):
            self.policy = Policy(**policy)
        elif isinstance(policy, Policy):
            self.policy = policy
        else:
            raise ValueError(f'GS Groups unsupported policy value {policy=}')

    def get_sdict(self):
        rv = super().get_sdict()
        rv['p_uid'] = self.p_uid
        rv['r_c_uid'] = self.r_c_uid

        if self.g_uid is not None:
            rv['g_uid'] = self.g_uid
        else:
            rv['user_name'] = self.user_name

        rv['items'] = self.items
        rv['policy'] = self.policy.get_sdict()

        return rv


class GSGroupCreateAddToResponse(_MsgBase):
    """
            Refer to :ref:`definition<gsgroupcreateaddtoresponse>` and :ref:`Common Fields<cfs>` for a
            description of the message structure.

    """

    _tc = MessageTypes.GS_GROUP_CREATE_ADD_TO_RESPONSE

    @enum.unique
    class Errors(enum.Enum):
        SUCCESS = 0  #:
        UNKNOWN = 1  #:
        DEAD = 2  #:
        PENDING = 3  #:

    def __init__(self, tag, ref, err, err_info='', desc=None, _tc=None):
        super().__init__(tag, ref, err)

        self.err_info = err_info

        if desc is None:
            desc = {}
            self._desc = None
        self.desc = desc

    @property
    def desc(self):
        return self._desc

    @desc.setter
    def desc(self, value):
        if isinstance(value, group_desc.GroupDescriptor):
            self._desc = value
        else:
            self._desc = group_desc.GroupDescriptor.from_sdict(value)

    def get_sdict(self):
        rv = super().get_sdict()

        if (self.Errors.UNKNOWN == self.err or
                self.Errors.PENDING == self.err):
            rv['err_info'] = self.err_info

        rv['desc'] = None if self.desc is None else self.desc.get_sdict()

        return rv


class GSGroupRemoveFrom(_MsgBase):
    """
            Refer to :ref:`definition<gsgroupremovefrom>` and :ref:`Common Fields<cfs>` for a description of
            the message structure.

    """

    _tc = MessageTypes.GS_GROUP_REMOVE_FROM

    def __init__(self, tag, p_uid, r_c_uid, g_uid=None, user_name='', items=None, _tc=None):
        super().__init__(tag)

        if items is None:
            items = []

        self.p_uid = p_uid
        self.r_c_uid = int(r_c_uid)
        self.g_uid = g_uid
        self.user_name = user_name
        self.items = list(items)

    def get_sdict(self):
        rv = super().get_sdict()
        rv['p_uid'] = self.p_uid
        rv['r_c_uid'] = self.r_c_uid

        if self.g_uid is not None:
            rv['g_uid'] = self.g_uid
        else:
            rv['user_name'] = self.user_name

        rv['items'] = self.items

        return rv


class GSGroupRemoveFromResponse(_MsgBase):
    """
            Refer to :ref:`definition<gsgroupremovefromresponse>` and :ref:`Common Fields<cfs>` for a
            description of the message structure.

    """

    _tc = MessageTypes.GS_GROUP_REMOVE_FROM_RESPONSE

    @enum.unique
    class Errors(enum.Enum):
        SUCCESS = 0  #:
        UNKNOWN = 1  #:
        FAIL = 2  #:
        DEAD = 3  #:
        PENDING = 4  #:

    def __init__(self, tag, ref, err, err_info='', desc=None, _tc=None):
        super().__init__(tag, ref, err)
        self.err_info = err_info

        if desc is None:
            desc = {}
            self._desc = None
        self.desc = desc

    @property
    def desc(self):
        return self._desc

    @desc.setter
    def desc(self, value):
        if isinstance(value, group_desc.GroupDescriptor):
            self._desc = value
        else:
            self._desc = group_desc.GroupDescriptor.from_sdict(value)

    def get_sdict(self):
        rv = super().get_sdict()

        if (self.Errors.UNKNOWN == self.err or
                self.Errors.FAIL == self.err):
            rv['err_info'] = self.err_info

        rv['desc'] = None if self.desc is None else self.desc.get_sdict()

        return rv


class GSGroupDestroyRemoveFrom(_MsgBase):
    """
            Refer to :ref:`definition<gsgroupdestroyremovefrom>` and :ref:`Common Fields<cfs>` for a description of
            the message structure.

    """

    _tc = MessageTypes.GS_GROUP_DESTROY_REMOVE_FROM

    def __init__(self, tag, p_uid, r_c_uid, g_uid=None, user_name='', items=None, _tc=None):
        super().__init__(tag)

        if items is None:
            items = []

        self.p_uid = p_uid
        self.r_c_uid = int(r_c_uid)
        self.g_uid = g_uid
        self.user_name = user_name
        self.items = list(items)

    def get_sdict(self):
        rv = super().get_sdict()
        rv['p_uid'] = self.p_uid
        rv['r_c_uid'] = self.r_c_uid

        if self.g_uid is not None:
            rv['g_uid'] = self.g_uid
        else:
            rv['user_name'] = self.user_name

        rv['items'] = self.items

        return rv


class GSGroupDestroyRemoveFromResponse(_MsgBase):
    """
            Refer to :ref:`definition<gsgroupdestroyremovefromresponse>` and :ref:`Common Fields<cfs>` for a
            description of the message structure.

    """

    _tc = MessageTypes.GS_GROUP_DESTROY_REMOVE_FROM_RESPONSE

    @enum.unique
    class Errors(enum.Enum):
        SUCCESS = 0  #:
        UNKNOWN = 1  #:
        FAIL = 2  #:
        DEAD = 3  #:
        PENDING = 4  #:

    def __init__(self, tag, ref, err, err_info='', desc=None, _tc=None):
        super().__init__(tag, ref, err)
        self.err_info = err_info

        if desc is None:
            desc = {}
            self._desc = None
        self.desc = desc

    @property
    def desc(self):
        return self._desc

    @desc.setter
    def desc(self, value):
        if isinstance(value, group_desc.GroupDescriptor):
            self._desc = value
        else:
            self._desc = group_desc.GroupDescriptor.from_sdict(value)

    def get_sdict(self):
        rv = super().get_sdict()

        if (self.Errors.UNKNOWN == self.err or
                self.Errors.FAIL == self.err):
            rv['err_info'] = self.err_info

        rv['desc'] = None if self.desc is None else self.desc.get_sdict()

        return rv


class GSChannelCreate(_MsgBase):
    """
            Refer to :ref:`definition<gschannelcreate>` and :ref:`Common Fields<cfs>` for a description of
            the message structure.

    """

    _tc = MessageTypes.GS_CHANNEL_CREATE

    def __init__(self, tag, p_uid, r_c_uid, m_uid, options=None, user_name='', _tc=None):
        super().__init__(tag)

        if options is None:
            options = {}

        self.p_uid = p_uid
        self.r_c_uid = int(r_c_uid)
        self.m_uid = int(m_uid)
        self.user_name = user_name
        self.options = options

    def get_sdict(self):
        rv = super().get_sdict()
        rv['p_uid'] = self.p_uid
        rv['r_c_uid'] = self.r_c_uid
        rv['user_name'] = self.user_name
        rv['m_uid'] = self.m_uid
        rv['options'] = self.options.get_sdict()
        return rv

    @property
    def options(self):
        return self._options

    @options.setter
    def options(self, value):
        if isinstance(value, channel_desc.ChannelOptions):
            self._options = value
        else:
            self._options = channel_desc.ChannelOptions.from_sdict(value)


class GSChannelCreateResponse(_MsgBase):
    """
        Refer to :ref:`definition<gschannelcreateresponse>` and :ref:`Common Fields<cfs>` for a
        description of the message structure.
    """

    _tc = MessageTypes.GS_CHANNEL_CREATE_RESPONSE

    @enum.unique
    class Errors(enum.Enum):
        SUCCESS = 0  #:
        FAIL = 1  #:
        ALREADY = 2  #:

    def __init__(self, tag, ref, err, desc=None, err_info='', _tc=None):
        super().__init__(tag, ref, err)
        if desc is None:
            desc = {}

        if self.Errors.SUCCESS == self.err or self.Errors.ALREADY == self.err:
            self.desc = desc
        else:
            self.err_info = err_info

    @property
    def desc(self):
        return self._desc

    @desc.setter
    def desc(self, value):
        if isinstance(value, channel_desc.ChannelDescriptor):
            self._desc = value
        else:
            self._desc = channel_desc.ChannelDescriptor.from_sdict(value)

    def get_sdict(self):
        rv = super().get_sdict()
        if self.Errors.SUCCESS == self.err or self.Errors.ALREADY == self.err:
            rv['desc'] = self.desc.get_sdict()
        else:
            rv['err_info'] = self.err_info

        return rv


class GSChannelList(_MsgBase):
    """
        Refer to :ref:`definition<gschannellist>` and to
        :ref:`Common Fields<cfs>` for a description of the message structure.
    """

    _tc = MessageTypes.GS_CHANNEL_LIST

    def __init__(self, tag, p_uid, r_c_uid, _tc=None):
        super().__init__(tag)
        self.p_uid = p_uid
        self.r_c_uid = int(r_c_uid)

    def get_sdict(self):
        rv = super().get_sdict()
        rv['p_uid'] = self.p_uid
        rv['r_c_uid'] = self.r_c_uid
        return rv


class GSChannelListResponse(_MsgBase):
    """
        Refer to :ref:`definition<gschannellistresponse>` and :ref:`Common Fields<cfs>` for a
        description of the message structure.
    """

    _tc = MessageTypes.GS_CHANNEL_LIST_RESPONSE

    @enum.unique
    class Errors(enum.Enum):
        SUCCESS = 0  #: Always succeeds

    def __init__(self, tag, ref, err, clist=None, _tc=None):
        super().__init__(tag, ref, err)

        if clist is None:
            self.clist = []
        else:
            self.clist = list(clist)

    def get_sdict(self):
        rv = super().get_sdict()
        rv['clist'] = self.clist
        return rv


class GSChannelQuery(_MsgBase):
    """
            Refer to :ref:`definition<gschannelquery>` and :ref:`Common Fields<cfs>` for a description of
            the message structure.

    """

    _tc = MessageTypes.GS_CHANNEL_QUERY

    def __init__(self, tag, p_uid, r_c_uid, c_uid=None, user_name='',
                 inc_refcnt=False, _tc=None):
        super().__init__(tag)
        self.p_uid = int(p_uid)
        self.r_c_uid = int(r_c_uid)
        self.inc_refcnt = inc_refcnt

        if c_uid is not None:
            self.c_uid = int(c_uid)
        else:
            self.c_uid = c_uid

        self.user_name = user_name

    def get_sdict(self):
        rv = super().get_sdict()
        rv['p_uid'] = self.p_uid
        rv['r_c_uid'] = self.r_c_uid
        rv['inc_refcnt'] = self.inc_refcnt

        if self.c_uid is not None:
            rv['c_uid'] = self.c_uid
        else:
            rv['user_name'] = self.user_name

        return rv


class GSChannelQueryResponse(_MsgBase):
    """
            Refer to :ref:`definition<gschannelqueryresponse>` and :ref:`Common Fields<cfs>` for a
            description of the message structure.

    """

    _tc = MessageTypes.GS_CHANNEL_QUERY_RESPONSE

    @enum.unique
    class Errors(enum.Enum):
        SUCCESS = 0  #:
        UNKNOWN = 1  #:

    def __init__(self, tag, ref, err, desc=None, err_info='', _tc=None):
        super().__init__(tag, ref, err)

        self.err_info = err_info
        if desc is None:
            desc = {}

        if self.Errors.SUCCESS == self.err:
            self.desc = desc
        elif self.Errors.UNKNOWN == self.err:
            self.err_info = err_info
        else:
            raise NotImplementedError('open enum')

    @property
    def desc(self):
        return self._desc

    @desc.setter
    def desc(self, value):
        if isinstance(value, channel_desc.ChannelDescriptor):
            self._desc = value
        else:
            self._desc = channel_desc.ChannelDescriptor.from_sdict(value)

    def get_sdict(self):
        rv = super().get_sdict()
        if self.Errors.SUCCESS == self.err:
            rv['desc'] = self.desc.get_sdict()
        elif self.Errors.UNKNOWN == self.err:
            rv['err_info'] = self.err_info
        else:
            raise NotImplementedError('open enum')

        return rv


class GSChannelDestroy(_MsgBase):
    """
            Refer to :ref:`definition<gschanneldestroy>` and :ref:`Common Fields<cfs>` for a description
            of the message structure.

    """

    _tc = MessageTypes.GS_CHANNEL_DESTROY

    def __init__(self, tag, p_uid, r_c_uid, c_uid=None, user_name='',
                 reply_req=True, dec_ref=False, _tc=None):
        super().__init__(tag)
        self.p_uid = p_uid
        self.r_c_uid = int(r_c_uid)
        self.c_uid = c_uid
        self.user_name = user_name
        self.reply_req = reply_req
        self.dec_ref = dec_ref

    def get_sdict(self):
        rv = super().get_sdict()
        rv['p_uid'] = self.p_uid
        rv['r_c_uid'] = self.r_c_uid
        rv['reply_req'] = self.reply_req
        rv['dec_ref'] = self.dec_ref

        if self.c_uid is not None:
            rv['c_uid'] = self.c_uid
        else:
            rv['user_name'] = self.user_name

        return rv


class GSChannelDestroyResponse(_MsgBase):
    """
            Refer to :ref:`definition<gschanneldestroyresponse>` and :ref:`Common Fields<cfs>` for a
            description of the message structure.

    """

    _tc = MessageTypes.GS_CHANNEL_DESTROY_RESPONSE

    @enum.unique
    class Errors(enum.Enum):
        SUCCESS = 0  #:
        UNKNOWN = 1  #:
        UNKNOWN_CHANNEL = 2  #:
        BUSY = 3  #:

    def __init__(self, tag, ref, err, err_info='', _tc=None):
        super().__init__(tag, ref, err)

        self.err_info = err_info

    def get_sdict(self):
        rv = super().get_sdict()
        if self.Errors.SUCCESS != self.err:
            rv['err_info'] = self.err_info

        return rv


class GSChannelJoin(_MsgBase):
    """
            Refer to :ref:`definition<gschanneljoin>` and :ref:`Common Fields<cfs>` for a description of
            the message structure.

    """

    _tc = MessageTypes.GS_CHANNEL_JOIN

    def __init__(self, tag, p_uid, r_c_uid, name, timeout=-1, _tc=None):
        super().__init__(tag)
        self.timeout = timeout
        self.p_uid = p_uid
        self.r_c_uid = int(r_c_uid)
        self.name = name

    def get_sdict(self):
        rv = super().get_sdict()
        rv['p_uid'] = self.p_uid
        rv['r_c_uid'] = self.r_c_uid
        rv['name'] = self.name
        rv['timeout'] = self.timeout
        return rv


class GSChannelJoinResponse(_MsgBase):
    """
            Refer to :ref:`definition<gschanneljoinresponse>` and :ref:`Common Fields<cfs>` for a
            description of the message structure.

    """

    _tc = MessageTypes.GS_CHANNEL_JOIN_RESPONSE

    @enum.unique
    class Errors(enum.Enum):
        SUCCESS = 0  #:
        TIMEOUT = 1  #:
        DEAD = 2  #:

    def __init__(self, tag, ref, err, desc=None, _tc=None):
        super().__init__(tag, ref, err)

        if desc is None:
            desc = {}

        if self.Errors.SUCCESS == self.err:
            self.desc = desc

    @property
    def desc(self):
        return self._desc

    @desc.setter
    def desc(self, value):
        if isinstance(value, channel_desc.ChannelDescriptor):
            self._desc = value
        else:
            self._desc = channel_desc.ChannelDescriptor.from_sdict(value)

    def get_sdict(self):
        rv = super().get_sdict()
        if self.Errors.SUCCESS == self.err:
            rv['desc'] = self.desc.get_sdict()

        return rv


class GSChannelDetach(_MsgBase):
    """
            Refer to :ref:`definition<gschanneldetach>` and :ref:`Common Fields<cfs>` for a description of
            the message structure.

    """

    _tc = MessageTypes.GS_CHANNEL_DETACH

    def __init__(self, tag, p_uid, r_c_uid, c_uid, _tc=None):
        super().__init__(tag)
        self.p_uid = int(p_uid)
        self.r_c_uid = int(r_c_uid)
        self.c_uid = int(c_uid)

    def get_sdict(self):
        rv = super().get_sdict()
        rv['p_uid'] = self.p_uid
        rv['r_c_uid'] = self.r_c_uid
        rv['c_uid'] = self.c_uid
        return rv


class GSChannelDetachResponse(_MsgBase):
    """
            Refer to :ref:`definition<gschanneldetachresponse>` and :ref:`Common Fields<cfs>` for a
            description of the message structure.

    """

    _tc = MessageTypes.GS_CHANNEL_DETACH_RESPONSE

    @enum.unique
    class Errors(enum.Enum):
        SUCCESS = 0  #:
        UNKNOWN = 1  #:
        UNKNOWN_CHANNEL = 2  #:
        NOT_ATTACHED = 3  #:

    def __init__(self, tag, ref, err, _tc=None):
        super().__init__(tag, ref, err)

    def get_sdict(self):
        rv = super().get_sdict()
        return rv


class GSChannelGetSendH(_MsgBase):
    """
            Refer to :ref:`definition<gschannelgetsendh>` and :ref:`Common Fields<cfs>` for a description
            of the message structure.

    """

    # TODO: REMOVE, DEPRECATED
    _tc = MessageTypes.GS_CHANNEL_GET_SENDH

    def __init__(self, tag, p_uid, r_c_uid, c_uid, _tc=None):
        raise NotImplementedError('OBE')
        super().__init__(tag)
        self.p_uid = p_uid
        self.r_c_uid = int(r_c_uid)
        self.c_uid = int(c_uid)

    def get_sdict(self):
        rv = super().get_sdict()
        rv['p_uid'] = self.p_uid
        rv['r_c_uid'] = self.r_c_uid
        rv['c_uid'] = self.c_uid
        return rv


class GSChannelGetSendHResponse(_MsgBase):
    """
            Refer to :ref:`definition<gschannelgetsendhresponse>` and :ref:`Common Fields<cfs>` for a
            description of the message structure.

    """

    # TODO: REMOVE, DEPRECATED
    _tc = MessageTypes.GS_CHANNEL_GET_SENDH_RESPONSE

    @enum.unique
    class Errors(enum.Enum):
        SUCCESS = 0  #:
        UNKNOWN = 1  #:
        UNKNOWN_CHANNEL = 2  #:
        NOT_ATTACHED = 3  #:
        CANT = 4  #:

    def __init__(self, tag, ref, err, sendh=None, err_info='', _tc=None):
        raise NotImplementedError('OBE')
        super().__init__(tag, ref, err)
        if sendh is None:
            sendh = {}

        self.sendh = sendh
        self.err_info = err_info

    @property
    def sendh(self):
        return self._sendh

    @sendh.setter
    def sendh(self, value):
        # FIXME, once we have ChannelSendHandle
        # if isinstance(value, ChannelSendHandle):
        #    self._sendh = value
        # else:
        #    self._sendh = ChannelSendHandle.from_sdict(value)

        self._sendh = value

    def get_sdict(self):
        rv = super().get_sdict()
        if self.Errors.CANT == self.err:
            rv['err_info'] = self.err_info
        elif self.Errors.SUCCESS == self.err:
            rv['sendh'] = self.sendh.get_sdict()

        return rv


class GSChannelGetRecvH(_MsgBase):
    """
            Refer to :ref:`definition<gschannelgetrecvh>` and :ref:`Common Fields<cfs>` for a description
            of the message structure.

    """

    # TODO: REMOVE, DEPRECATED
    _tc = MessageTypes.GS_CHANNEL_GET_RECVH

    def __init__(self, tag, p_uid, r_c_uid, c_uid, _tc=None):
        raise NotImplementedError('OBE')
        super().__init__(tag)
        self.p_uid = p_uid
        self.r_c_uid = int(r_c_uid)
        self.c_uid = int(c_uid)

    def get_sdict(self):
        rv = super().get_sdict()
        rv['p_uid'] = self.p_uid
        rv['r_c_uid'] = self.r_c_uid
        rv['c_uid'] = self.c_uid
        return rv


class GSChannelGetRecvHResponse(_MsgBase):
    """
            Refer to :ref:`definition<gschannelgetrecvhresponse>` and :ref:`Common Fields<cfs>` for a
            description of the message structure.

    """

    # TODO: REMOVE, DEPRECATED
    _tc = MessageTypes.GS_CHANNEL_GET_RECVH_RESPONSE

    @enum.unique
    class Errors(enum.Enum):
        SUCCESS = 0  #:
        UNKNOWN = 1  #:
        UNKNOWN_CHANNEL = 2  #:
        NOT_ATTACHED = 3  #:
        CANT = 4  #:

    def __init__(self, tag, ref, err, recvh=None, err_info='', _tc=None):
        raise NotImplementedError('OBE')
        super().__init__(tag, ref, err)
        if recvh is None:
            recvh = {}
        self.recvh = recvh
        self.err_info = err_info

    @property
    def recvh(self):
        return self._recvh

    @recvh.setter
    def recvh(self, value):
        # FIXME, once we have ChannelRecvHandle
        # if isinstance(value, ChannelRecvHandle):
        #    self._recvh = value
        # else:
        #    self._recvh = ChannelRecvHandle.from_sdict(value)

        self._recvh = value

    def get_sdict(self):
        rv = super().get_sdict()
        if self.Errors.CANT == self.err:
            rv['err_info'] = self.err_info
        elif self.Errors.SUCCESS == self.err:
            rv['recvh'] = self.recvh.get_sdict()

        return rv


class GSNodeList(_MsgBase):
    """
    *type enum*
        GS_NODE_LIST (= 102)

    *purpose*
        Return a list of tuples of ``h_uid`` for all nodes currently registered.

    *fields*
        None additional

    *response*
        GSNodeListResponse

    *see also*
        GSNodeQuery

    *see also*
        refer to the :ref:`cfs` section for additional request message fields
    """

    _tc = MessageTypes.GS_NODE_LIST

    def __init__(self, tag, p_uid, r_c_uid,
                 _tc=None):
        super().__init__(tag)
        self.p_uid = p_uid
        self.r_c_uid = int(r_c_uid)

    def get_sdict(self):
        rv = super().get_sdict()
        rv['p_uid'] = self.p_uid
        rv['r_c_uid'] = self.r_c_uid
        return rv


class GSNodeListResponse(_MsgBase):
    """
    *type enum*
        GS_NODE_LIST_RESPONSE (= 103)

    *purpose*
        Responds with a list of ``h_uid`` for all the
        nodes currently registered.

    *fields*
        **hlist**
            - list of nonnegative integers

    *request*
        GSNodeList

    *see also*
        GSNodeQuery
    """

    _tc = MessageTypes.GS_NODE_LIST_RESPONSE

    @enum.unique
    class Errors(enum.Enum):
        SUCCESS = 0  #: Always succeeds

    def __init__(self, tag, ref, err, hlist=None, _tc=None):
        super().__init__(tag, ref, err)

        if hlist is None:
            self.hlist = []
        else:
            self.hlist = list(hlist)

    def get_sdict(self):
        rv = super().get_sdict()
        rv['hlist'] = self.hlist
        return rv


class GSNodeQuery(_MsgBase):
    """
            Refer to :ref:`definition<gsnodequery>` and :ref:`Common Fields<cfs>` for a description of
            the message structure.

    """

    _tc = MessageTypes.GS_NODE_QUERY

    def __init__(self, tag, p_uid:int, r_c_uid:int, name:str='', h_uid=None, _tc=None):

        super().__init__(tag)
        self.p_uid = int(p_uid)
        self.r_c_uid = int(r_c_uid)
        self.name = name
        self.h_uid = h_uid

    def get_sdict(self):
        rv = super().get_sdict()
        rv['p_uid'] = self.p_uid
        rv['r_c_uid'] = self.r_c_uid
        rv['name'] = self.name
        rv['h_uid'] = self.h_uid

        return rv


class GSNodeQueryResponse(_MsgBase):
    """
            Refer to :ref:`definition<gsnodequeryresponse>` and :ref:`Common Fields<cfs>` for a
            description of the message structure.

    """

    _tc = MessageTypes.GS_NODE_QUERY_RESPONSE

    @enum.unique
    class Errors(enum.Enum):
        SUCCESS = 0  #:
        UNKNOWN = 1  #:

    def __init__(self, tag, ref, err, desc=None, err_info='', _tc=None):
        super().__init__(tag, ref, err)

        self.err_info = err_info
        if desc is None:
            desc = {}

        if self.Errors.SUCCESS == self.err:
            self.desc = desc
        elif self.Errors.UNKNOWN == self.err:
            self.err_info = err_info
        else:
            raise NotImplementedError('open enum')

    @property
    def desc(self):
        return self._desc

    @desc.setter
    def desc(self, value):
        if isinstance(value, NodeDescriptor):
            self._desc = value
        else:
            self._desc = NodeDescriptor.from_sdict(value)

    def get_sdict(self):
        rv = super().get_sdict()
        if self.Errors.SUCCESS == self.err:
            rv['desc'] = self.desc.get_sdict()
        elif self.Errors.UNKNOWN == self.err:
            rv['err_info'] = self.err_info
        else:
            raise NotImplementedError('open enum')

        return rv


class GSNodeQueryTotalCPUCount(_MsgBase):
    """
    *type enum*
        GS_NODE_QUERY_TOTAL_CPU_COUNT (= 104)

    *purpose*
        Asks GS to return the total number of CPUS beloging to all of the registered nodes.

    *see also*
        refer to the :ref:`cfs` section for additional request message fields

    *response*
        GSNodeQueryTotalCPUCountResponse
    """

    _tc = MessageTypes.GS_NODE_QUERY_TOTAL_CPU_COUNT

    def __init__(self, tag, p_uid:int, r_c_uid:int, _tc=None):
        super().__init__(tag)
        self.p_uid = int(p_uid)
        self.r_c_uid = int(r_c_uid)

    def get_sdict(self):
        rv = super().get_sdict()
        rv['p_uid'] = self.p_uid
        rv['r_c_uid'] = self.r_c_uid

        return rv


class GSNodeQueryTotalCPUCountResponse(_MsgBase):
    """
    *type enum*
        GS_NODE_QUERY_TOTAL_CPU_COUNT_RESPONSE (= 105)

    *purpose*
         Return the total number of CPUS beloging to all of the registered nodes.

    *fields*
        Alternatives on ``err``:

        SUCCESS (= 0)

            The machine descriptor was successfully constructed

            **total_cpus**
                - total number of CPUS beloging to all of the registered nodes.

        UNKNOWN ( = 1)
            An unknown error has occured.

    *request*
        GSNodeQueryTotalCPUCount

    *see also*
        GSNodeQuery
    """

    _tc = MessageTypes.GS_NODE_QUERY_TOTAL_CPU_COUNT_RESPONSE

    @enum.unique
    class Errors(enum.Enum):
        SUCCESS = 0  #:
        UNKNOWN = 1  #:

    def __init__(self, tag, ref, err, total_cpus=0, err_info='', _tc=None):
        super().__init__(tag, ref, err)

        self.err_info = err_info

        if self.Errors.SUCCESS == self.err:
            self.total_cpus = total_cpus
        elif self.Errors.UNKNOWN == self.err:
            self.err_info = err_info
        else:
            raise NotImplementedError('open enum')

    @property
    def total_cpus(self):
        return self._total_cpus

    @total_cpus.setter
    def total_cpus(self, value):
        self._total_cpus = int(value)

    def get_sdict(self):
        rv = super().get_sdict()
        if self.Errors.SUCCESS == self.err:
            rv['total_cpus'] = self.total_cpus
        elif self.Errors.UNKNOWN == self.err:
            rv['err_info'] = self.err_info
        else:
            raise NotImplementedError('open enum')

        return rv


class AbnormalTermination(_MsgBase):
    """
            Refer to :ref:`definition<abnormaltermination>` and :ref:`Common Fields<cfs>` for a
            description of the message structure.

    """

    _tc = MessageTypes.ABNORMAL_TERMINATION

    def __init__(self, tag, err_info='', _tc=None):
        super().__init__(tag)
        self.err_info = err_info

    def get_sdict(self):
        rv = super().get_sdict()
        rv['err_info'] = self.err_info
        return rv

    def __str__(self):
        traceback = str(self.err_info)
        if len(traceback) > 0:
            return str(super()) + f'\n***** Dragon Traceback: *****\n{traceback}***** End of Dragon Traceback: *****\n'

        return str(super())


class ExceptionlessAbort(_MsgBase):
    """
            Refer to :ref:`definition<exceptionlessabort>` and :ref:`Common Fields<cfs>` for a
            description of the message structure.

    """

    _tc = MessageTypes.EXCEPTIONLESS_ABORT

    def __init__(self, tag, _tc=None):
        super().__init__(tag)

    def get_sdict(self):
        rv = super().get_sdict()
        return rv


class GSStarted(_MsgBase):
    """
            Refer to :ref:`definition<gsstarted>` and :ref:`Common Fields<cfs>` for a description of the
            message structure.

    """

    """Indicates Global Services head process has started."""

    _tc = MessageTypes.GS_STARTED

    def __init__(self, tag, _tc=None):
        super().__init__(tag)

    def get_sdict(self):
        rv = super().get_sdict()
        return rv


class GSPingSH(_MsgBase):
    """
            Refer to :ref:`definition<gspingsh>` and :ref:`Common Fields<cfs>` for a description of the
            message structure.

    """

    _tc = MessageTypes.GS_PING_SH

    def __init__(self, tag, _tc=None):
        super().__init__(tag)

    def get_sdict(self):
        rv = super().get_sdict()
        return rv


class GSIsUp(_MsgBase):
    """
            Refer to :ref:`definition<gsisup>` and :ref:`Common Fields<cfs>` for a description of the
            message structure.

    """

    _tc = MessageTypes.GS_IS_UP

    def __init__(self, tag, _tc=None):
        super().__init__(tag)

    def get_sdict(self):
        rv = super().get_sdict()
        return rv


class GSPingProc(_MsgBase):
    """
            Refer to :ref:`definition<gspingproc>` and :ref:`Common Fields<cfs>` for a description of the
            message structure.

    """

    _tc = MessageTypes.GS_PING_PROC

    def __init__(self, tag, mode=None, argdata=None, _tc=None):
        super().__init__(tag)

        self.mode = process_desc.mk_argmode_from_default(mode)

        self.argdata = argdata

    def get_sdict(self):
        rv = super().get_sdict()

        rv['mode'] = self.mode.value
        rv['argdata'] = self.argdata

        return rv


class GSDump(_MsgBase):
    """
            Refer to :ref:`definition<gsdump>` and :ref:`Common Fields<cfs>` for a description of the
            message structure.

    """

    _tc = MessageTypes.GS_DUMP

    def __init__(self, tag, filename, _tc=None):
        super().__init__(tag)
        self.filename = filename

    def get_sdict(self):
        rv = super().get_sdict()
        rv['filename'] = self.filename
        return rv


class GSHeadExit(_MsgBase):
    """
            Refer to :ref:`definition<gsheadexit>` and :ref:`Common Fields<cfs>` for a description of the
            message structure.

    """

    _tc = MessageTypes.GS_HEAD_EXIT

    def __init__(self, tag, exit_code=0, _tc=None):
        super().__init__(tag)
        self.exit_code = exit_code

    def get_sdict(self):
        rv = super().get_sdict()
        rv['exit_code'] = self.exit_code
        return rv


class GSTeardown(_MsgBase):
    """
            Refer to :ref:`definition<gsteardown>` and :ref:`Common Fields<cfs>` for a description of the
            message structure.

    """

    _tc = MessageTypes.GS_TEARDOWN

    def __init__(self, tag, _tc=None):
        super().__init__(tag)

    def get_sdict(self):
        rv = super().get_sdict()
        return rv


class GSUnexpected(_MsgBase):
    """
            Refer to :ref:`definition<gsunexpected>` and :ref:`Common Fields<cfs>` for a description of
            the message structure.

    """

    _tc = MessageTypes.GS_UNEXPECTED

    def __init__(self, tag, ref, _tc=None):
        super().__init__(tag, ref)

    def get_sdict(self):
        rv = super().get_sdict()
        return rv


class GSChannelRelease(_MsgBase):
    """
            Refer to :ref:`definition<gschannelrelease>` and :ref:`Common Fields<cfs>` for a description
            of the message structure.

    """

    _tc = MessageTypes.GS_CHANNEL_RELEASE

    def __init__(self, tag, _tc=None):
        super().__init__(tag)

    def get_sdict(self):
        rv = super().get_sdict()
        return rv


class GSHalted(_MsgBase):
    """
            Refer to :ref:`definition<gshalted>` and :ref:`Common Fields<cfs>` for a description of the
            message structure.

    """

    _tc = MessageTypes.GS_HALTED

    def __init__(self, tag, _tc=None):
        super().__init__(tag)

    def get_sdict(self):
        rv = super().get_sdict()
        return rv


class SHProcessCreate(_MsgBase):
    """
            Refer to :ref:`definition<shprocesscreate>` and :ref:`Common Fields<cfs>` for a description of
            the message structure.

            The initial_stdin is a string which if non-empty is written along with a terminating newline character
            to the stdin of the newly created process.

            The stdin, stdout, and stderr are all either None or an instance of SHChannelCreate to be processed
            by the local services component.

    """

    _tc = MessageTypes.SH_PROCESS_CREATE

    def __init__(self, tag, p_uid, r_c_uid, t_p_uid, exe, args, env=None, rundir='', options=None, initial_stdin='',
                 stdin=None, stdout=None, stderr=None, group=None, user=None, umask=- 1, pipesize=None,
                 stdin_msg=None, stdout_msg=None, stderr_msg=None, pmi_info=None, layout=None, _tc=None):
        super().__init__(tag)

        if options is None:
            options = {}

        if env is None:
            env = {}

        self.p_uid = p_uid
        self.r_c_uid = int(r_c_uid)
        self.t_p_uid = t_p_uid
        self.exe = exe
        self.args = args
        self.env = env
        self.rundir = rundir
        self.options = options
        self.initial_stdin = initial_stdin
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        self.group = group
        self.user = user
        self.umask = umask
        self.pipesize = pipesize
        if stdin_msg is None or isinstance(stdin_msg, SHChannelCreate):
            self.stdin_msg = stdin_msg
        else:
            self.stdin_msg = SHChannelCreate.from_sdict(stdin_msg)

        if stdout_msg is None or isinstance(stdout_msg, SHChannelCreate):
            self.stdout_msg = stdout_msg
        else:
            self.stdout_msg = SHChannelCreate.from_sdict(stdout_msg)

        if stderr_msg is None or isinstance(stderr_msg, SHChannelCreate):
            self.stderr_msg = stderr_msg
        else:
            self.stderr_msg = SHChannelCreate.from_sdict(stderr_msg)

        if pmi_info is None:
            self.pmi_info = None
        elif isinstance(pmi_info, dict):
            self.pmi_info = PMIInfo.fromdict(pmi_info)
        elif isinstance(pmi_info, PMIInfo):
            self.pmi_info = pmi_info
        else:
            raise ValueError(f'LS unsupported pmi_info value {pmi_info=}')

        if layout is None:
            self.layout = None
        elif isinstance(layout, dict):
            self.layout = ResourceLayout(**layout)
        elif isinstance(layout, ResourceLayout):
            self.layout = layout
        else:
            raise ValueError(f'LS unsupported layout value {layout=}')

    @property
    def options(self):
        return self._options

    @options.setter
    def options(self, value):

        if isinstance(value, dso.ProcessOptions):
            self._options = value
        else:
            self._options = dso.ProcessOptions.from_sdict(value)

    def get_sdict(self):
        rv = super().get_sdict()

        rv['p_uid'] = self.p_uid
        rv['r_c_uid'] = self.r_c_uid
        rv['t_p_uid'] = self.t_p_uid
        rv['exe'] = self.exe
        rv['args'] = self.args
        rv['env'] = self.env
        rv['rundir'] = self.rundir
        rv['options'] = self.options.get_sdict()
        rv['initial_stdin'] = self.initial_stdin
        rv['stdin'] = self.stdin
        rv['stdout'] = self.stdout
        rv['stderr'] = self.stderr
        rv['group'] = self.group
        rv['user'] = self.user
        rv['umask'] = self.umask
        rv['pipesize'] = self.pipesize
        rv['stdin_msg'] = (None if self.stdin_msg is None else self.stdin_msg.get_sdict())
        rv['stdout_msg'] = (None if self.stdout_msg is None else self.stdout_msg.get_sdict())
        rv['stderr_msg'] = (None if self.stderr_msg is None else self.stderr_msg.get_sdict())
        rv['pmi_info'] = None if self.pmi_info is None else asdict(self.pmi_info)
        rv['layout'] = None if self.layout is None else asdict(self.layout)
        return rv


class SHProcessCreateResponse(_MsgBase):
    """
            Refer to :ref:`definition<shprocesscreateresponse>` and :ref:`Common Fields<cfs>` for a
            description of the message structure.

    """

    _tc = MessageTypes.SH_PROCESS_CREATE_RESPONSE

    @enum.unique
    class Errors(enum.Enum):
        SUCCESS = 0  #:
        FAIL = 1  #:

    def __init__(self, tag, ref, err, err_info='', stdin_resp=None, stdout_resp=None, stderr_resp=None, _tc=None):
        super().__init__(tag, ref, err)
        self.err_info = err_info


        if stdin_resp is None or isinstance(stdin_resp, SHChannelCreateResponse):
            self.stdin_resp = stdin_resp
        else:
            self.stdin_resp = SHChannelCreateResponse.from_sdict(stdin_resp)

        if stdout_resp is None or isinstance(stdout_resp, SHChannelCreateResponse):
            self.stdout_resp = stdout_resp
        else:
            self.stdout_resp = SHChannelCreateResponse.from_sdict(stdout_resp)

        if stderr_resp is None or isinstance(stderr_resp, SHChannelCreateResponse):
            self.stderr_resp = stderr_resp
        else:
            self.stderr_resp = SHChannelCreateResponse.from_sdict(stderr_resp)

    def get_sdict(self):
        rv = super().get_sdict()
        if self.Errors.FAIL == self.err:
            rv['err_info'] = self.err_info
        rv['stdin_resp'] = (None if self.stdin_resp is None else self.stdin_resp.get_sdict())
        rv['stdout_resp'] = (None if self.stdout_resp is None else self.stdout_resp.get_sdict())
        rv['stderr_resp'] = (None if self.stderr_resp is None else self.stderr_resp.get_sdict())
        return rv


class SHProcessKill(_MsgBase):
    """
            Refer to :ref:`definition<shprocesskill>` and :ref:`Common Fields<cfs>` for a description of
            the message structure.

    """

    _tc = MessageTypes.SH_PROCESS_KILL

    def __init__(self, tag, p_uid, r_c_uid, t_p_uid, sig, _tc=None):
        super().__init__(tag)
        self.p_uid = int(p_uid)
        self.r_c_uid = int(r_c_uid)
        self.t_p_uid = t_p_uid
        self.sig = sig

    def get_sdict(self):
        rv = super().get_sdict()
        rv['p_uid'] = self.p_uid
        rv['r_c_uid'] = self.r_c_uid
        rv['t_p_uid'] = self.t_p_uid
        rv['sig'] = self.sig
        return rv


class SHProcessKillResponse(_MsgBase):
    _tc = MessageTypes.SH_PROCESS_KILL_RESPONSE

    @enum.unique
    class Errors(enum.Enum):
        SUCCESS = 0
        FAIL = 1

    def __init__(self, tag, ref, err, err_info='', _tc=None):
        super().__init__(tag, ref, err)
        self.err_info = err_info

    def get_sdict(self):
        rv = super().get_sdict()
        if self.Errors.FAIL == self.err:
            rv['err_info'] = self.err_info

        return rv


class SHProcessExit(_MsgBase):
    """
        Refer to :ref:`definition<shprocessexit>` and to
        :ref:`Common Fields<cfs>` for a description of
        the message structure.
    """

    _tc = MessageTypes.SH_PROCESS_EXIT

    @enum.unique
    class Errors(enum.Enum):
        SUCCESS = 0  #:
        UNKNOWN = 1  #:

    def __init__(self, tag, p_uid, exit_code=0, _tc=None):
        super().__init__(tag)
        self.p_uid = int(p_uid)
        self.exit_code = exit_code

    def get_sdict(self):
        rv = super().get_sdict()
        rv['exit_code'] = self.exit_code
        rv['p_uid'] = self.p_uid
        return rv


class SHPoolCreate(_MsgBase):
    """
        Refer to :ref:`definition<shpoolcreate>` and
        :ref:`Common Fields<cfs>` for a description of the message structure.
    """

    _tc = MessageTypes.SH_POOL_CREATE

    def __init__(self, tag, p_uid, r_c_uid, size, m_uid, name, attr='', _tc=None):
        super().__init__(tag)

        self.m_uid = int(m_uid)
        self.p_uid = p_uid
        self.r_c_uid = int(r_c_uid)
        self.size = int(size)
        self.name = name
        self.attr = attr

    def get_sdict(self):
        rv = super().get_sdict()

        rv['p_uid'] = self.p_uid
        rv['r_c_uid'] = self.r_c_uid
        rv['size'] = self.size
        rv['m_uid'] = self.m_uid
        rv['name'] = self.name
        rv['attr'] = self.attr
        return rv


class SHPoolCreateResponse(_MsgBase):
    """
        Refer to :ref:`definition<shpoolcreateresponse>` and
        :ref:`Common Fields<cfs>` for a description of the message structure.
    """

    _tc = MessageTypes.SH_POOL_CREATE_RESPONSE

    @enum.unique
    class Errors(enum.Enum):
        SUCCESS = 0  #: Pool was created
        FAIL = 1  #: Pool was not created

    def __init__(self, tag, ref, err, desc=None, err_info='', _tc=None):
        super().__init__(tag, ref, err)

        if self.Errors.SUCCESS == self.err:
            self.desc = desc
        elif self.err == self.Errors.FAIL:
            self.err_info = err_info
        else:
            raise NotImplementedError('close case')

    def get_sdict(self):
        rv = super().get_sdict()

        if self.err == self.Errors.SUCCESS:
            rv['desc'] = self.desc
        elif self.err == self.Errors.FAIL:
            rv['err_info'] = self.err_info
        else:
            raise NotImplementedError('close case')

        return rv


class SHPoolDestroy(_MsgBase):
    """
        Refer to :ref:`definition<shpooldestroy>` and to
        the :ref:`Common Fields<cfs>` for a description of
        the message structure.
    """

    _tc = MessageTypes.SH_POOL_DESTROY

    def __init__(self, tag, p_uid, r_c_uid, m_uid, _tc=None):
        super().__init__(tag)
        self.p_uid = p_uid
        self.r_c_uid = int(r_c_uid)
        self.m_uid = int(m_uid)

    def get_sdict(self):
        rv = super().get_sdict()
        rv['p_uid'] = self.p_uid
        rv['r_c_uid'] = self.r_c_uid
        rv['m_uid'] = self.m_uid

        return rv


class SHPoolDestroyResponse(_MsgBase):
    """
        Refer to :ref:`definition<shpooldestroyresponse>` and :ref:`Common Fields<cfs>` for a
        description of the message structure.
    """

    _tc = MessageTypes.SH_POOL_DESTROY_RESPONSE

    @enum.unique
    class Errors(enum.Enum):
        SUCCESS = 0  #:
        FAIL = 1  #:

    def __init__(self, tag, ref, err, err_info='', _tc=None):
        super().__init__(tag, ref, err)
        self.err_info = err_info

    def get_sdict(self):
        rv = super().get_sdict()

        if self.Errors.FAIL == self.err:
            rv['err_info'] = self.err_info

        return rv


class SHExecMemRequest(_MsgBase):
    """
            Refer to :ref:`definition<shexecmemrequest>` and :ref:`Common Fields<cfs>` for a description
            of the message structure.

    """

    _tc = MessageTypes.SH_EXEC_MEM_REQUEST

    @enum.unique
    class KINDS(enum.Enum):
        MEMORY_POOL_REQUEST = 0
        MEMORY_ALLOC_REQUEST = 1

    def __init__(self, tag, p_uid, r_c_uid, kind, request, _tc=None):
        super().__init__(tag)
        self.p_uid = p_uid
        self.r_c_uid = int(r_c_uid)
        self.kind = kind
        self.request = request

    def get_sdict(self):
        rv = super().get_sdict()
        rv['p_uid'] = self.p_uid
        rv['r_c_uid'] = self.r_c_uid
        rv['kind'] = self.kind
        rv['request'] = self.request

        return rv


class SHExecMemResponse(_MsgBase):
    """
            Refer to :ref:`definition<shexecmemresponse>` and :ref:`Common Fields<cfs>` for a description
            of the message structure.

    """

    _tc = MessageTypes.SH_EXEC_MEM_RESPONSE

    @enum.unique
    class Errors(enum.Enum):
        SUCCESS = 0  #:
        FAIL = 1  #:

    def __init__(self, tag, ref, err, err_code=0, err_info='', response=None, _tc=None):
        super().__init__(tag, ref, err)
        self.response = response
        self.err_info = err_info
        self.err_code = err_code

    def get_sdict(self):
        rv = super().get_sdict()

        if self.Errors.FAIL == self.err:
            rv['err_info'] = self.err_info
            rv['err_code'] = self.err_code
        elif self.Errors.SUCCESS == self.err:
            rv['response'] = self.response

        return rv


class SHChannelCreate(_MsgBase):
    """
        Refer to :ref:`definition<shchannelcreate>` and :ref:`Common Fields<cfs>`
        for a description of the message structure.
    """

    _tc = MessageTypes.SH_CHANNEL_CREATE

    def __init__(self, tag, p_uid, r_c_uid, m_uid, c_uid, options=None, _tc=None):
        super().__init__(tag)
        self.p_uid = p_uid
        self.r_c_uid = int(r_c_uid)
        self.m_uid = int(m_uid)
        self.c_uid = int(c_uid)

        if options is None:
            options = {}

        self.options = options

    def get_sdict(self):
        rv = super().get_sdict()
        rv['p_uid'] = self.p_uid
        rv['r_c_uid'] = self.r_c_uid
        rv['m_uid'] = self.m_uid
        rv['c_uid'] = self.c_uid
        rv['options'] = self.options.get_sdict()

        return rv

    @property
    def options(self):
        return self._options

    @options.setter
    def options(self, value):
        if isinstance(value, dso.ChannelOptions):
            self._options = value
        else:
            self._options = dso.ChannelOptions.from_sdict(value)


class SHChannelCreateResponse(_MsgBase):
    """
        Refer to :ref:`definition<shchannelcreateresponse>` and :ref:`Common Fields<cfs>` for a
        description of the message structure.
    """

    _tc = MessageTypes.SH_CHANNEL_CREATE_RESPONSE

    @enum.unique
    class Errors(enum.Enum):
        SUCCESS = 0  #:
        FAIL = 1  #:

    def __init__(self, tag, ref, err, desc=None, err_info='', _tc=None):
        super().__init__(tag, ref, err)

        self.err_info = err_info
        self.desc = desc

    def get_sdict(self):
        rv = super().get_sdict()

        if self.Errors.SUCCESS == self.err:
            rv['desc'] = self.desc
        else:
            rv['err_info'] = self.err_info

        return rv


class SHChannelDestroy(_MsgBase):
    """
            Refer to :ref:`definition<shchanneldestroy>` and :ref:`Common Fields<cfs>` for a description
            of the message structure.

    """

    _tc = MessageTypes.SH_CHANNEL_DESTROY

    def __init__(self, tag, p_uid, r_c_uid, c_uid, _tc=None):
        super().__init__(tag)
        self.p_uid = p_uid
        self.r_c_uid = int(r_c_uid)
        self.c_uid = int(c_uid)

    def get_sdict(self):
        rv = super().get_sdict()
        rv['p_uid'] = self.p_uid
        rv['r_c_uid'] = self.r_c_uid
        rv['c_uid'] = self.c_uid

        return rv


class SHChannelDestroyResponse(_MsgBase):
    """
            Refer to :ref:`definition<shchanneldestroyresponse>` and :ref:`Common Fields<cfs>` for a
            description of the message structure.

    """

    _tc = MessageTypes.SH_CHANNEL_DESTROY_RESPONSE

    @enum.unique
    class Errors(enum.Enum):
        SUCCESS = 0  #:
        FAIL = 1  #:

    def __init__(self, tag, ref, err, err_info='', _tc=None):
        super().__init__(tag, ref, err)
        self.err_info = err_info

    def get_sdict(self):
        rv = super().get_sdict()

        if self.Errors.FAIL == self.err:
            rv['err_info'] = self.err_info
        return rv


class SHLockChannel(_MsgBase):
    """
            Refer to :ref:`definition<shlockchannel>` and :ref:`Common Fields<cfs>` for a description of
            the message structure.

    """

    _tc = MessageTypes.SH_LOCK_CHANNEL

    def __init__(self, tag, p_uid, r_c_uid, _tc=None):
        super().__init__(tag)
        self.p_uid = int(p_uid)
        self.r_c_uid = int(r_c_uid)

    def get_sdict(self):
        rv = super().get_sdict()
        rv['p_uid'] = self.p_uid
        rv['r_c_uid'] = self.r_c_uid
        return rv


class SHLockChannelResponse(_MsgBase):
    """
            Refer to :ref:`definition<shlockchannelresponse>` and :ref:`Common Fields<cfs>` for a
            description of the message structure.

    """

    _tc = MessageTypes.SH_LOCK_CHANNEL_RESPONSE

    @enum.unique
    class Errors(enum.Enum):
        SUCCESS = 0  #:
        ALREADY = 1  #:

    def __init__(self, tag, ref, err, _tc=None):
        super().__init__(tag, ref, err)

    def get_sdict(self):
        rv = super().get_sdict()

        return rv


class SHAllocMsg(_MsgBase):
    """
            Refer to :ref:`definition<shallocmsg>` and :ref:`Common Fields<cfs>` for a description of the
            message structure.

    """

    _tc = MessageTypes.SH_ALLOC_MSG

    def __init__(self, tag, p_uid, r_c_uid, _tc=None):
        super().__init__(tag)
        self.r_c_uid = int(r_c_uid)
        self.p_uid = int(p_uid)

    def get_sdict(self):
        rv = super().get_sdict()
        rv['p_uid'] = self.p_uid
        rv['r_c_uid'] = self.r_c_uid
        return rv


class SHAllocMsgResponse(_MsgBase):
    """
            Refer to :ref:`definition<shallocmsgresponse>` and :ref:`Common Fields<cfs>` for a description
            of the message structure.

    """

    _tc = MessageTypes.SH_ALLOC_MSG_RESPONSE

    @enum.unique
    class Errors(enum.Enum):
        SUCCESS = 0  #:

    def __init__(self, tag, ref, err, _tc=None):
        super().__init__(tag, ref, err)

    def get_sdict(self):
        rv = super().get_sdict()
        return rv


class SHAllocBlock(_MsgBase):
    """
            Refer to :ref:`definition<shallocblock>` and :ref:`Common Fields<cfs>` for a description of
            the message structure.

    """

    _tc = MessageTypes.SH_ALLOC_BLOCK

    def __init__(self, tag, p_uid, r_c_uid, _tc=None):
        super().__init__(tag)
        self.p_uid = int(p_uid)
        self.r_c_uid = int(r_c_uid)

    def get_sdict(self):
        rv = super().get_sdict()
        rv['p_uid'] = self.p_uid
        rv['r_c_uid'] = self.r_c_uid
        return rv


class SHAllocBlockResponse(_MsgBase):
    """
            Refer to :ref:`definition<shallocblockresponse>` and :ref:`Common Fields<cfs>` for a
            description of the message structure.

    """

    _tc = MessageTypes.SH_ALLOC_BLOCK_RESPONSE

    @enum.unique
    class Errors(enum.Enum):
        SUCCESS = 0  #:
        UNKNOWN = 1  #:
        FAIL = 2  #:

    def __init__(self, tag, ref, err, seg_name='',
                 offset=0, err_info='', _tc=None):
        super().__init__(tag, ref, err)

        self.seg_name = seg_name
        self.offset = offset
        self.err_info = err_info

    def get_sdict(self):
        rv = super().get_sdict()

        if self.Errors.SUCCESS == self.err:
            rv['seg_name'] = self.seg_name
            rv['offset'] = self.offset
        elif self.Errors.FAIL == self.err:
            rv['err_info'] = self.err_info

        return rv


class SHChannelsUp(_MsgBase):
    """
            Refer to :ref:`definition<shchannelsup>` and :ref:`Common Fields<cfs>` for a description of
            the message structure.

    """

    _tc = MessageTypes.SH_CHANNELS_UP

    def __init__(self, tag, node_desc, gs_cd, idx=0, _tc=None):
        super().__init__(tag)

        self.idx = idx
        if isinstance(node_desc, dict):
            self.node_desc = NodeDescriptor.from_sdict(node_desc)
        elif isinstance(node_desc, NodeDescriptor):
            self.node_desc = node_desc

        # On the primary node the gs_cd is set to the base64 encoded gs channel descriptor.
        # Otherwise, it is ignored and presumably the empty string.
        self.gs_cd = gs_cd

    def get_sdict(self):
        rv = super().get_sdict()
        rv['node_desc'] = self.node_desc.get_sdict()
        rv['gs_cd'] = self.gs_cd
        rv['idx'] = self.idx
        return rv


class SHPingGS(_MsgBase):
    """
            Refer to :ref:`definition<shpinggs>` and :ref:`Common Fields<cfs>` for a description of the
            message structure.

    """

    _tc = MessageTypes.SH_PING_GS

    def __init__(self, tag, idx=0, node_sdesc=None, _tc=None):
        super().__init__(tag)
        self.idx = idx
        self.node_sdesc = node_sdesc

    def get_sdict(self):
        rv = super().get_sdict()
        rv['idx'] = self.idx
        rv['node_sdesc'] = self.node_sdesc

        return rv

class SHTeardown(_MsgBase):
    """
            Refer to :ref:`definition<shteardown>` and :ref:`Common Fields<cfs>` for a description of the
            message structure.

    """

    _tc = MessageTypes.SH_TEARDOWN

    def __init__(self, tag, _tc=None):
        super().__init__(tag)

    def get_sdict(self):
        rv = super().get_sdict()
        return rv


class SHPingBE(_MsgBase):
    """
            Refer to :ref:`definition<shpingbe>` and :ref:`Common Fields<cfs>` for a description of the
            message structure.

    """

    _tc = MessageTypes.SH_PING_BE
    EMPTY = B64.bytes_to_str(b'')

    def __init__(self, tag, shep_cd=EMPTY, be_cd=EMPTY, gs_cd=EMPTY,
                 default_pd=EMPTY, inf_pd=EMPTY, _tc=None):
        super().__init__(tag)
        self.shep_cd = shep_cd
        self.be_cd = be_cd
        self.gs_cd = gs_cd
        self.default_pd = default_pd
        self.inf_pd = inf_pd

    def get_sdict(self):
        rv = super().get_sdict()
        rv['shep_cd'] = self.shep_cd
        rv['be_cd'] = self.be_cd
        rv['gs_cd'] = self.gs_cd
        rv['default_pd'] = self.default_pd
        rv['inf_pd'] = self.inf_pd
        return rv


class SHHaltTA(_MsgBase):
    """
            Refer to :ref:`definition<shhaltta>` and :ref:`Common Fields<cfs>` for a description of the
            message structure.

    """

    _tc = MessageTypes.SH_HALT_TA

    def __init__(self, tag, _tc=None):
        super().__init__(tag)

    def get_sdict(self):
        rv = super().get_sdict()
        return rv


class SHHaltBE(_MsgBase):
    """
            Refer to :ref:`definition<shhaltbe>` and :ref:`Common Fields<cfs>` for a description of the
            message structure.

    """

    _tc = MessageTypes.SH_HALT_BE

    def __init__(self, tag, _tc=None):
        super().__init__(tag)

    def get_sdict(self):
        rv = super().get_sdict()
        return rv


class SHHalted(_MsgBase):
    """
            Refer to :ref:`definition<shhalted>` and :ref:`Common Fields<cfs>` for a description of the
            message structure.

    """

    _tc = MessageTypes.SH_HALTED

    def __init__(self, tag, idx=0, _tc=None):
        super().__init__(tag)
        self.idx = idx

    def get_sdict(self):
        rv = super().get_sdict()
        rv['idx'] = self.idx
        return rv


class SHFwdInput(_MsgBase):
    """
            Refer to :ref:`definition<shfwdinput>` and :ref:`Common Fields<cfs>` for a description of the
            message structure.

    """

    _tc = MessageTypes.SH_FWD_INPUT

    MAX = 1024

    def __init__(self, tag, p_uid, r_c_uid, t_p_uid=None, input='', confirm=False, _tc=None):
        super().__init__(tag)
        self.p_uid = int(p_uid)
        self.r_c_uid = int(r_c_uid)
        self.t_p_uid = t_p_uid
        if len(input) > self.MAX:
            raise ValueError(f'input limited to {self.MAX} bytes')

        self.input = input
        self.confirm = confirm

    def get_sdict(self):
        rv = super().get_sdict()
        rv['p_uid'] = self.p_uid
        rv['r_c_uid'] = self.r_c_uid
        rv['t_p_uid'] = self.t_p_uid
        rv['input'] = self.input
        rv['confirm'] = self.confirm
        return rv


class SHFwdInputErr(_MsgBase):
    """
            Refer to :ref:`definition<shfwdinputerr>` and :ref:`Common Fields<cfs>` for a description of
            the message structure.

    """

    _tc = MessageTypes.SH_FWD_INPUT_ERR

    @enum.unique
    class Errors(enum.Enum):
        SUCCESS = 0  #:
        FAIL = 1  #:

    def __init__(self, tag, ref, err, idx=0, err_info='', _tc=None):
        super().__init__(tag, ref, err)
        self.err_info = err_info
        self.idx = idx

    def get_sdict(self):
        rv = super().get_sdict()
        rv['idx'] = self.idx
        if self.Errors.FAIL == self.err:
            rv['err_info'] = self.err_info

        return rv


class SHFwdOutput(_MsgBase):
    """
            Refer to :ref:`definition<shfwdoutput>` and :ref:`Common Fields<cfs>` for a description of the
            message structure.

    """

    _tc = MessageTypes.SH_FWD_OUTPUT

    MAX = 1024

    # TODO: contemplate interface where this is used
    @enum.unique
    class FDNum(enum.Enum):
        STDOUT = 1
        STDERR = 2

    def __init__(self, tag, p_uid, idx, fd_num, data, _tc=None, pid=-1, hostname='NONE'):
        super().__init__(tag)
        self.idx = int(idx)
        if len(data) > self.MAX:
            raise ValueError(f'output data limited to {self.MAX} bytes')

        self.data = data
        self.p_uid = int(p_uid)
        assert fd_num in {self.FDNum.STDOUT.value, self.FDNum.STDERR.value}
        self.fd_num = fd_num
        self.hostname = hostname
        self.pid = pid

    def get_sdict(self):
        rv = super().get_sdict()
        rv['idx'] = self.idx
        rv['data'] = self.data
        rv['p_uid'] = self.p_uid
        rv['fd_num'] = self.fd_num
        rv['hostname'] = self.hostname
        rv['pid'] = self.pid
        return rv

    def __str__(self):
        return f'{super().__str__()}, self.data={self.data!r}, self.p_uid={self.p_uid!r}, self.fd_num={self.fd_num!r}'


class SHDumpState(_MsgBase):
    """
            Refer to :ref:`definition<shdumpstate>` and :ref:`Common Fields<cfs>` for a description of the
            message structure.

    """

    _tc = MessageTypes.SH_DUMP

    def __init__(self, tag, filename=None, _tc=None):
        super().__init__(tag)
        self.filename = filename

    def get_sdict(self):
        rv = super().get_sdict()
        rv['filename'] = self.filename
        return rv


class BENodeIdxSH(_MsgBase):
    """
            Refer to :ref:`definition<benodeidxsh>` and :ref:`Common Fields<cfs>` for a description of the
            message structure.

    """

    _tc = MessageTypes.BE_NODEINDEX_SH

    def __init__(self, tag, node_idx, host_name=None, ip_addrs=None,
                 primary=None, logger_sdesc=None, _tc=None):
        super().__init__(tag)
        self.node_idx = node_idx
        self.host_name = host_name
        self.ip_addrs = ip_addrs
        self.primary = primary
        self.logger_sdesc = logger_sdesc

    @property
    def logger_sdesc(self):
        return self._logger_sdesc

    @logger_sdesc.setter
    def logger_sdesc(self, value):
        if isinstance(value, str):
            self._logger_sdesc = B64.from_str(value)
        else:
            self._logger_sdesc = value

    def get_sdict(self):
        rv = super().get_sdict()
        rv['node_idx'] = self.node_idx
        rv['host_name'] = self.host_name
        rv['ip_addrs'] = self.ip_addrs
        rv['primary'] = self.primary
        if isinstance(self._logger_sdesc, B64):
            rv['logger_sdesc'] = str(self._logger_sdesc)
        else:
            rv['logger_sdesc'] = self._logger_sdesc
        return rv


class BEPingSH(_MsgBase):
    """
            Refer to :ref:`definition<bepingsh>` and :ref:`Common Fields<cfs>` for a description of the
            message structure.

    """

    _tc = MessageTypes.BE_PING_SH

    def __init__(self, tag, _tc=None):
        super().__init__(tag)

    def get_sdict(self):
        rv = super().get_sdict()
        return rv


class BEHalted(_MsgBase):
    """
            Refer to :ref:`definition<behalted>` and :ref:`Common Fields<cfs>` for a description of the
            message structure.

    """

    _tc = MessageTypes.BE_HALTED

    def __init__(self, tag, _tc=None):
        super().__init__(tag)

    def get_sdict(self):
        rv = super().get_sdict()
        return rv


class LABroadcast(_MsgBase):
    """
            Refer to :ref:`definition<labroadcast>` and :ref:`Common Fields<cfs>` for a description of the
            message structure.
            The *_tc* value of this message must be set to 68. The :ref:`Launcher Network Front End <launchernet>`
            has this as
            an external dependency.

    """

    _tc = MessageTypes.LA_BROADCAST

    def __init__(self, tag, data, _tc=None):
        super().__init__(tag)
        self.data = data

    def get_sdict(self):
        rv = super().get_sdict()
        rv['data'] = self.data
        return rv


class LAPassThruFB(_MsgBase):
    """
            Refer to :ref:`definition<lapassthrufb>` and :ref:`Common Fields<cfs>` for a description of
            the message structure.

    """

    _tc = MessageTypes.LA_PASSTHRU_FB

    def __init__(self, tag, c_uid, data, _tc=None):
        super().__init__(tag)
        self.c_uid = int(c_uid)
        self.data = data

    def get_sdict(self):
        rv = super().get_sdict()
        rv['c_uid'] = self.c_uid
        rv['data'] = self.data
        return rv


class LAPassThruBF(_MsgBase):
    """
            Refer to :ref:`definition<lapassthrubf>` and :ref:`Common Fields<cfs>` for a description of
            the message structure.

    """

    _tc = MessageTypes.LA_PASSTHRU_BF

    def __init__(self, tag, data, _tc=None):
        super().__init__(tag)
        self.data = data

    def get_sdict(self):
        rv = super().get_sdict()
        rv['data'] = self.data
        return rv


class LAServerMode(_MsgBase):
    """
            Refer to :ref:`definition<laservermode>` and :ref:`Common Fields<cfs>` for a description of
            the message structure.

    """

    _tc = MessageTypes.LA_SERVER_MODE

    def __init__(self, tag, frontend, backend, frontend_args=None, backend_args=None, backend_env=None,
                 backend_run_dir='', backend_user_name='', backend_options=None, _tc=None):
        super().__init__(tag)

        if backend_args is None:
            self.backend_args = []
        else:
            self.backend_args = list(backend_args)

        if frontend_args is None:
            self.frontend_args = []
        else:
            self.frontend_args = list(frontend_args)

        self.frontend = frontend
        self.backend = backend
        self.frontend_args = frontend_args
        self.backend_args = backend_args
        self.backend_env = backend_env
        self.backend_run_dir = backend_run_dir
        self.backend_user_name = backend_user_name
        self.backend_options = backend_options

    def get_sdict(self):
        rv = super().get_sdict()
        rv['frontend'] = self.frontend
        rv['backend'] = self.backend
        rv['frontend_args'] = self.frontend_args
        rv['backend_args'] = self.backend_args
        rv['backend_env'] = self.backend_env
        rv['backend_run_dir'] = self.backend_run_dir
        rv['backend_user_name'] = self.backend_user_name
        rv['backend_options'] = self.backend_options
        return rv


# TODO FIXME: if messages are in this hierarchy they must follow the rules.
#   This one does not; the spec needs fixing too.
class LAServerModeExit(_MsgBase):
    """
            Refer to :ref:`definition<laservermodeexit>` and :ref:`Common Fields<cfs>` for a description
            of the message structure.

    """

    _tc = MessageTypes.LA_SERVER_MODE_EXIT

    @enum.unique
    class Errors(enum.Enum):
        SUCCESS = 0  #:
        FAIL = 1  #:

    def __init__(self, tag, ref, err, _tc=None):
        # The err field is the exit code of the
        # process exit.
        super().__init__(tag, ref, err)

    def get_sdict(self):
        rv = super().get_sdict()
        return rv


class LAProcessDict(_MsgBase):
    """
            Refer to :ref:`definition<laprocessdict>` and :ref:`Common Fields<cfs>` for a description of
            the message structure.

    """

    _tc = MessageTypes.LA_PROCESS_DICT

    def __init__(self, tag, _tc=None):
        super().__init__(tag)

    def get_sdict(self):
        rv = super().get_sdict()
        return rv


class LAProcessDictResponse(_MsgBase):
    """
            Refer to :ref:`definition<laprocessdictresponse>` and :ref:`Common Fields<cfs>` for a
            description of the message structure.

    """

    _tc = MessageTypes.LA_PROCESS_DICT_RESPONSE

    @enum.unique
    class Errors(enum.Enum):
        SUCCESS = 0  #: Always succeeds

    def __init__(self, tag, ref, err, pdict=None, _tc=None):
        super().__init__(tag, ref, err)

        if pdict is None:
            pdict = {}

        self.pdict = pdict

    def get_sdict(self):
        rv = super().get_sdict()
        rv['pdict'] = self.pdict
        return rv


class LADumpState(_MsgBase):
    """
            Refer to :ref:`definition<ladumpstate>` and :ref:`Common Fields<cfs>` for a description of the
            message structure.

    """

    _tc = MessageTypes.LA_DUMP

    def __init__(self, tag, filename=None, _tc=None):
        super().__init__(tag)
        self.filename = filename

    def get_sdict(self):
        rv = super().get_sdict()
        rv['filename'] = self.filename
        return rv


class LAChannelsInfo(_MsgBase):
    """
            Refer to :ref:`definition<lachannelsinfo>` and :ref:`Common Fields<cfs>` for a description of
            the message structure.

    """

    _tc = MessageTypes.LA_CHANNELS_INFO

    def __init__(self, tag, nodes_desc, gs_cd, num_gw_channels, port=dfacts.DEFAULT_TRANSPORT_PORT,
                 transport=str(dfacts.TransportAgentOptions.TCP), _tc=None):
        super().__init__(tag)

        self.gs_cd = gs_cd
        self.transport = dfacts.TransportAgentOptions.from_str(transport)
        self.num_gw_channels = num_gw_channels

        self.nodes_desc = {}
        for key in nodes_desc.keys():
            if isinstance(nodes_desc[key], dict):
                self.nodes_desc[key] = NodeDescriptor.from_sdict(nodes_desc[key])
            elif isinstance(nodes_desc[key], NodeDescriptor):
                self.nodes_desc[key] = nodes_desc[key]

            if port is not None:
                self.nodes_desc[key].port = port

    def get_sdict(self):
        rv = super().get_sdict()

        rv['nodes_desc'] = self.nodes_desc.copy()
        for key in self.nodes_desc.keys():
            rv['nodes_desc'][key] = self.nodes_desc[key].get_sdict()
        rv['gs_cd'] = self.gs_cd
        rv['num_gw_channels'] = self.num_gw_channels
        rv['transport'] = str(self.transport)
        return rv


class LoggingMsg(_MsgBase):
    """
            Refer to :ref:`definition<loggingmsg>` and :ref:`Common Fields<cfs>` for a description of the
            message structure.

    """

    _tc = MessageTypes.LOGGING_MSG

    def __init__(self, tag, name, msg, time, func, hostname, ip_address, port, service, level, _tc=None):
        super().__init__(tag)
        self.name = name
        self.msg = msg
        self.time = time
        self.func = func
        self.hostname = hostname
        self.ip_address = ip_address
        self.port = port
        self.service = service
        self.level = level

    def get_sdict(self):
        rv = super().get_sdict()
        rv['name'] = self.name
        rv['msg'] = self.msg
        rv['time'] = self.time
        rv['func'] = self.func
        rv['hostname'] = self.hostname
        rv['ip_address'] = self.ip_address
        rv['port'] = self.port
        rv['service'] = self.service
        rv['level'] = self.level

        return rv

    # Extra method to cleanly generate the dictionary needed for logging
    # which requires omitting names that match attributes in LogRecord
    def get_logging_dict(self):
        rv = super().get_sdict()
        rv['time'] = self.time
        rv['hostname'] = self.hostname
        rv['ip_address'] = self.ip_address
        rv['port'] = self.port
        rv['service'] = self.service
        rv['level'] = self.level

        return rv


class LoggingMsgList(_MsgBase):
    """
            Refer to :ref:`definition<loggingmsglist>` and :ref:`Common Fields<cfs>` for a description of the
            message structure.

    """

    _tc = MessageTypes.LOGGING_MSG_LIST

    def __init__(self, tag, records, _tc=None):
        super().__init__(tag)
        self.records = records

    @property
    def records(self):
        return self._records

    @records.setter
    def records(self, value):
        if isinstance(value, list):
            self._records = value
        elif (value, dict):
            self._records = [LoggingMsg(**v) for v in value.values()]
        else:
            msg = "LoggingMsgList records attributes requires a list or dict of LoggingMsg"
            raise AttributeError(msg)

    def get_sdict(self):
        rv = super().get_sdict()
        rv['records'] = {i: v.get_sdict() for i, v in enumerate(self._records)}
        return rv


class LogFlushed(_MsgBase):
    """
            Refer to :ref:`definition<logflushed>` and :ref:`Common Fields<cfs>` for a description of the
            message structure.

    """

    _tc = MessageTypes.LOG_FLUSHED

    def __init__(self, tag, _tc=None):
        super().__init__(tag)

    def get_sdict(self):
        rv = super().get_sdict()
        return rv


class TAPingSH(_MsgBase):
    """
            Refer to :ref:`definition<tapingsh>` and :ref:`Common Fields<cfs>` for a description of the
            message structure.

    """

    _tc = MessageTypes.TA_PING_SH

    def __init__(self, tag, _tc=None):
        super().__init__(tag)

    def get_sdict(self):
        rv = super().get_sdict()
        return rv


class TAHalted(_MsgBase):
    """
            Refer to :ref:`definition<tahalted>` and :ref:`Common Fields<cfs>` for a description of the
            message structure.

    """

    _tc = MessageTypes.TA_HALTED

    def __init__(self, tag, _tc=None):
        super().__init__(tag)

    def get_sdict(self):
        rv = super().get_sdict()
        return rv


class TAUp(_MsgBase):
    """
            Refer to :ref:`definition<taup>` and :ref:`Common Fields<cfs>` for a description of the
            message structure.

            The test_channels are empty unless the DRAGON_TRANSPORT_TEST environment variable is
            set to some value. When set in the environment, local services will create
            two channels and provide the base64 encoded serialized channel descriptors in this
            test_channels field to the launcher front end which then disseminates them to
            the test program which is started on each node.
    """

    _tc = MessageTypes.TA_UP

    def __init__(self, tag, idx=0, test_channels=[], _tc=None):
        super().__init__(tag)
        self.idx = int(idx)
        self.test_channels = list(test_channels)

    def get_sdict(self):
        rv = super().get_sdict()
        rv['idx'] = self.idx
        rv['test_channels'] = self.test_channels
        return rv


class Breakpoint(_MsgBase):
    _tc = MessageTypes.BREAKPOINT

    def __init__(self, tag, p_uid, index, out_desc, in_desc, _tc=None):
        super().__init__(tag)
        self.in_desc = in_desc
        self.out_desc = out_desc
        self.index = index
        self.p_uid = p_uid

    def get_sdict(self):
        rv = super().get_sdict()
        rv.update({'index': self.index,
                   'p_uid': self.p_uid,
                   'in_desc': self.in_desc,
                   'out_desc': self.out_desc})
        return rv


class BEIsUp(_MsgBase):
    """
            Refer to :ref:`definition<beisup>` and :ref:`Common Fields<cfs>` for a description of the
            message structure.
    """

    _tc = MessageTypes.BE_IS_UP

    def __init__(self, tag, be_ch_desc, host_id, _tc=None):
        super().__init__(tag)
        self.be_ch_desc = be_ch_desc
        self.host_id = host_id

    def get_sdict(self):
        rv = super().get_sdict()
        rv['be_ch_desc'] = self.be_ch_desc
        rv['host_id'] = self.host_id
        return rv


class FENodeIdxBE(_MsgBase):
    """
            Refer to :ref:`definition<fenodeidxbe>` and :ref:`Common Fields<cfs>` for a description of the
            message structure.
    """

    _tc = MessageTypes.FE_NODE_IDX_BE

    def __init__(self,
                 tag,
                 node_index,
                 forward: Optional[dict['str', Union[NodeDescriptor, dict]]] = None,
                 send_desc: Optional[Union[B64, str]] = None,
                 _tc=None):

        super().__init__(tag)
        self.node_index = int(node_index)
        self.forward = forward
        self.send_desc = send_desc

    @property
    def forward(self):
        return self._forward

    @forward.setter
    def forward(self, value):
        try:
            self._forward = {}
            for idx, node in value.items():
                if isinstance(node, NodeDescriptor):
                    self._forward[idx] = node
                else:
                    self._forward[idx] = NodeDescriptor.from_sdict(node)
        except (TypeError, AttributeError):
            self._forward = value

    @property
    def send_desc(self):
        return self._send_desc

    @send_desc.setter
    def send_desc(self, value):
        if isinstance(value, str):
            self._send_desc = B64.from_str(value)
        else:
            self._send_desc = value

    def get_sdict(self):
        rv = super().get_sdict()
        rv['node_index'] = self.node_index
        try:
            rv['forward'] = self.forward.copy()
            for idx in self.forward.keys():
                rv['forward'][idx] = self.forward[idx].get_sdict()
        except AttributeError:
            rv['forward'] = self.forward
        rv['send_desc'] = str(self.send_desc)
        return rv


class HaltLoggingInfra(_MsgBase):
    """
            Refer to :ref:`definition<haltlogginginfra>` and :ref:`Common Fields<cfs>` for a description of the
            message structure.

    """

    _tc = MessageTypes.HALT_LOGGING_INFRA

    def __init__(self, tag, _tc=None):
        super().__init__(tag)

    def get_sdict(self):
        rv = super().get_sdict()
        return rv


class HaltOverlay(_MsgBase):
    """
            Refer to :ref:`definition<haltoverlay>` and :ref:`Common Fields<cfs>` for a description of the
            message structure.

    """

    _tc = MessageTypes.HALT_OVERLAY

    def __init__(self, tag, _tc=None):
        super().__init__(tag)

    def get_sdict(self):
        rv = super().get_sdict()
        return rv


class OverlayHalted(_MsgBase):
    """
            Refer to :ref:`definition<overlayhalted>` and :ref:`Common Fields<cfs>` for a description of the
            message structure.

    """
    _tc = MessageTypes.OVERLAY_HALTED

    def __init__(self, tag, _tc=None):
        super().__init__(tag)

    def get_sdict(self):
        rv = super().get_sdict()
        return rv


class BEHaltOverlay(_MsgBase):
    """
            Refer to :ref:`definition<behaltoverlay>` and :ref:`Common Fields<cfs>` for a description of the
            message structure.

    """
    _tc = MessageTypes.BE_HALT_OVERLAY

    def __init__(self, tag, _tc=None):
        super().__init__(tag)

    def get_sdict(self):
        rv = super().get_sdict()
        return rv


class LAHaltOverlay(_MsgBase):
    """
            Refer to :ref:`definition<lahaltoverlay>` and :ref:`Common Fields<cfs>` for a description of the
            message structure.

    """
    _tc = MessageTypes.LA_HALT_OVERLAY

    def __init__(self, tag, _tc=None):
        super().__init__(tag)

    def get_sdict(self):
        rv = super().get_sdict()
        return rv


class OverlayPingBE(_MsgBase):
    """
            Refer to :ref:`definition<overlaypingbe>` and :ref:`Common Fields<cfs>` for a description of the
            message structure.

    """
    _tc = MessageTypes.OVERLAY_PING_BE

    def __init__(self, tag, _tc=None):
        super().__init__(tag)

    def get_sdict(self):
        rv = super().get_sdict()
        return rv


class OverlayPingLA(_MsgBase):
    """
            Refer to :ref:`definition<overlaypingla>` and :ref:`Common Fields<cfs>` for a description of the
            message structure.

    """
    _tc = MessageTypes.OVERLAY_PING_LA

    def __init__(self, tag, _tc=None):
        super().__init__(tag)

    def get_sdict(self):
        rv = super().get_sdict()
        return rv


class LAExit(_MsgBase):
    """
            Refer to :ref:`definition<laexit>` and :ref:`Common Fields<cfs>` for a description of the
            message structure.

    """
    _tc = MessageTypes.LA_EXIT

    def __init__(self, tag, sigint=False, _tc=None):
        super().__init__(tag)
        self.sigint = sigint

    def get_sdict(self):
        rv = super().get_sdict()
        rv['sigint'] = self.sigint
        return rv


class TAUpdateNodes(_MsgBase):
    """
            Refer to :ref:`definition<taupdatenodes>` and :ref:`Common Fields<cfs>` for a description of the
            message structure.

    """
    _tc = MessageTypes.HSTA_UPDATE_NODES

    def __init__(self, tag,
                 nodes: list[Union[NodeDescriptor, dict]],
                 _tc=None):
        super().__init__(tag)
        self.nodes = nodes

    @property
    def nodes(self):
        return self._nodes

    @nodes.setter
    def nodes(self, value):
        self._nodes = []
        for node in value:
            if isinstance(node, NodeDescriptor):
                self._nodes.append(node)
            else:
                self._nodes.append(NodeDescriptor.from_sdict(node))

    def get_sdict(self):
        rv = super().get_sdict()
        rv['nodes'] = [node.get_sdict() for node in self.nodes]

        return rv


all_message_classes = [GSProcessCreate,
                       GSProcessCreateResponse,
                       GSProcessList,
                       GSProcessListResponse,
                       GSProcessQuery,
                       GSProcessQueryResponse,
                       GSProcessKill,
                       GSProcessKillResponse,
                       GSProcessJoin,
                       GSProcessJoinResponse,
                       GSChannelCreate,
                       GSChannelCreateResponse,
                       GSChannelList,
                       GSChannelListResponse,
                       GSChannelQuery,
                       GSChannelQueryResponse,
                       GSChannelDestroy,
                       GSChannelDestroyResponse,
                       GSChannelJoin,
                       GSChannelJoinResponse,
                       GSChannelDetach,
                       GSChannelDetachResponse,
                       GSChannelGetSendH,
                       GSChannelGetSendHResponse,
                       GSChannelGetRecvH,
                       GSChannelGetRecvHResponse,
                       AbnormalTermination,
                       GSStarted,
                       GSPingSH,
                       GSIsUp,
                       GSHeadExit,
                       GSChannelRelease,
                       GSHalted,
                       SHProcessCreate,
                       SHProcessCreateResponse,
                       SHProcessKill,
                       SHProcessExit,
                       SHChannelCreate,
                       SHChannelCreateResponse,
                       SHChannelDestroy,
                       SHChannelDestroyResponse,
                       SHLockChannel,
                       SHLockChannelResponse,
                       SHAllocMsg,
                       SHAllocMsgResponse,
                       SHAllocBlock,
                       SHAllocBlockResponse,
                       SHChannelsUp,
                       SHPingGS,
                       SHHalted,
                       SHFwdInput,
                       SHFwdInputErr,
                       SHFwdOutput,
                       SHDumpState,
                       GSTeardown,
                       SHTeardown,
                       SHPingBE,
                       BEPingSH,
                       BENodeIdxSH,
                       TAPingSH,
                       SHHaltTA,
                       TAHalted,
                       SHHaltBE,
                       BEHalted,
                       TAUp,
                       GSPingProc,
                       GSDump,
                       LABroadcast,
                       LAChannelsInfo,
                       LAPassThruBF,
                       LAPassThruFB,
                       LAServerMode,
                       LAServerModeExit,
                       LAProcessDict,
                       LAProcessDictResponse,
                       LADumpState,
                       GSPoolList,
                       GSPoolListResponse,
                       GSPoolCreate,
                       GSPoolCreateResponse,
                       GSPoolDestroy,
                       GSPoolDestroyResponse,
                       GSPoolQuery,
                       GSPoolQueryResponse,
                       SHPoolCreate,
                       SHPoolCreateResponse,
                       SHPoolDestroy,
                       SHPoolDestroyResponse,
                       SHExecMemRequest,
                       SHExecMemResponse,
                       GSUnexpected,
                       SHProcessKillResponse,
                       Breakpoint,
                       GSProcessJoinList,
                       GSProcessJoinListResponse,
                       GSNodeList,
                       GSNodeListResponse,
                       GSNodeQuery,
                       GSNodeQueryResponse,
                       GSNodeQueryTotalCPUCount,
                       GSNodeQueryTotalCPUCountResponse,
                       LoggingMsg,
                       LoggingMsgList,
                       LogFlushed,
                       BEIsUp,
                       FENodeIdxBE,
                       HaltOverlay,
                       HaltLoggingInfra,
                       OverlayPingBE,
                       OverlayPingLA,
                       LAHaltOverlay,
                       BEHaltOverlay,
                       OverlayHalted,
                       ExceptionlessAbort,
                       LAExit,
                       GSGroupCreate,
                       GSGroupCreateResponse,
                       GSGroupList,
                       GSGroupQuery,
                       GSGroupListResponse,
                       GSGroupCreateResponse,
                       GSGroupQueryResponse,
                       GSGroupKill,
                       GSGroupKillResponse,
                       GSGroupDestroy,
                       GSGroupDestroyResponse,
                       GSGroupAddTo,
                       GSGroupAddToResponse,
                       GSGroupRemoveFrom,
                       GSGroupRemoveFromResponse,
                       GSGroupCreateAddTo,
                       GSGroupCreateAddToResponse,
                       GSGroupDestroyRemoveFrom,
                       GSGroupDestroyRemoveFromResponse,
                       TAUpdateNodes]

mt_dispatch = {cls._tc.value: cls for cls in all_message_classes}


def parse(jstring, restrict=None):

    try:
        sdict = json.loads(jstring)
    except TypeError as e:
        raise TypeError(f'The message "{jstring}" could not be parsed.') from e

    # if a compressed message, decompress to get the service message
    if 'COMPRESSED' in sdict:
        jstring = zlib.decompress(base64.b64decode(sdict['COMPRESSED']))
        sdict = json.loads(jstring)

    typecode = sdict['_tc']

    if restrict:
        assert typecode in restrict

    return mt_dispatch[typecode].from_sdict(sdict)


# types of all the messages that global services can receive
all_gs_messages = {GSProcessCreate, GSProcessList, GSProcessQuery,
                   GSProcessKill, GSProcessJoin, GSChannelCreate,
                   GSChannelList, GSChannelQuery, GSChannelDestroy,
                   GSChannelJoin, GSChannelDetach, GSChannelGetSendH,
                   GSChannelGetRecvH, GSChannelRelease, SHProcessCreateResponse,
                   SHProcessExit, SHChannelCreateResponse,
                   SHChannelDestroyResponse, SHLockChannelResponse,
                   SHAllocMsgResponse, SHAllocBlockResponse,
                   SHPingGS, GSTeardown, GSDump, GSPoolList, GSPoolCreate, GSPoolQuery,
                   GSPoolDestroy, SHProcessKillResponse,
                   GSPoolListResponse, GSPoolDestroyResponse, GSPoolCreateResponse,
                   GSPoolQueryResponse, GSTeardown,
                   SHPoolCreateResponse, SHPoolDestroyResponse, GSProcessJoinList,
                   GSNodeList, GSNodeQuery, GSNodeQueryTotalCPUCount,
                   GSGroupList, GSGroupCreate, GSGroupQuery,
                   GSGroupListResponse, GSGroupDestroy, GSGroupAddTo,
                   GSGroupRemoveFrom, GSGroupCreateAddTo,
                   GSGroupKill, GSGroupDestroyRemoveFrom}
