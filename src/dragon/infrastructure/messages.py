"""Dragon infrastructure messages are the internal API used for service communication.
"""

import sys
import enum
import json
import zlib
import subprocess
import traceback
from typing import Dict, List, Optional, Union
from dataclasses import dataclass, asdict

import ctypes
from contextlib import contextmanager,redirect_stderr,redirect_stdout
from os import devnull

from ..infrastructure import channel_desc
from ..infrastructure import pool_desc
from ..infrastructure.node_desc import NodeDescriptor
from ..infrastructure import process_desc
from ..infrastructure import parameters as parms
from ..infrastructure import facts as dfacts
from ..localservices import options as dso
from ..infrastructure.util import get_external_ip_addr, to_str_iter
from ..utils import B64, b64encode, b64decode
from ..rc import DragonError
import capnp
from ..infrastructure import message_defs_capnp as capnp_schema

from ..globalservices.policy_eval import ResourceLayout, Policy

from ..infrastructure import group_desc
# from ..infrastructure.policy import DefaultPolicy



INT_NONE = 0 - 0X80000000

# This enum class lists the type codes in infrastructure
# messages.  The values are significant for interoperability.

@enum.unique
class MessageTypes(enum.Enum):
    """
    These are the enumerated values of message type identifiers within
    the Dragon infrastructure messages.
    """
    DRAGON_MSG = 0 #: Deliberately invalid
    GS_PROCESS_CREATE = enum.auto() #:
    GS_PROCESS_CREATE_RESPONSE = enum.auto() #:
    GS_PROCESS_LIST = enum.auto() #:
    GS_PROCESS_LIST_RESPONSE = enum.auto() #:
    GS_PROCESS_QUERY = enum.auto() #:
    GS_PROCESS_QUERY_RESPONSE = enum.auto() #:
    GS_PROCESS_KILL = enum.auto() #:
    GS_PROCESS_KILL_RESPONSE = enum.auto() #:
    GS_PROCESS_JOIN = enum.auto() #:
    GS_PROCESS_JOIN_RESPONSE = enum.auto() #:
    GS_CHANNEL_CREATE = enum.auto() #:
    GS_CHANNEL_CREATE_RESPONSE = enum.auto() #:
    GS_CHANNEL_LIST = enum.auto() #:
    GS_CHANNEL_LIST_RESPONSE = enum.auto() #:
    GS_CHANNEL_QUERY = enum.auto() #:
    GS_CHANNEL_QUERY_RESPONSE = enum.auto() #:
    GS_CHANNEL_DESTROY = enum.auto() #:
    GS_CHANNEL_DESTROY_RESPONSE = enum.auto() #:
    GS_CHANNEL_JOIN = enum.auto() #:
    GS_CHANNEL_JOIN_RESPONSE = enum.auto() #:
    GS_CHANNEL_DETACH = enum.auto() #:
    GS_CHANNEL_DETACH_RESPONSE = enum.auto() #:
    GS_CHANNEL_GET_SENDH = enum.auto() #:
    GS_CHANNEL_GET_SENDH_RESPONSE = enum.auto() #:
    GS_CHANNEL_GET_RECVH = enum.auto() #:
    GS_CHANNEL_GET_RECVH_RESPONSE = enum.auto() #:
    ABNORMAL_TERMINATION = enum.auto() #:
    GS_STARTED = enum.auto() #:
    GS_PING_SH = enum.auto() #:
    GS_IS_UP = enum.auto() #:
    GS_HEAD_EXIT = enum.auto() #:
    GS_CHANNEL_RELEASE = enum.auto() #:
    GS_HALTED = enum.auto() #:
    SH_PROCESS_CREATE = enum.auto() #:
    SH_PROCESS_CREATE_RESPONSE = enum.auto() #:
    SH_MULTI_PROCESS_CREATE = enum.auto() #:
    SH_MULTI_PROCESS_CREATE_RESPONSE = enum.auto() #:
    SH_PROCESS_KILL = enum.auto() #:
    SH_PROCESS_EXIT = enum.auto() #:
    SH_CHANNEL_CREATE = enum.auto() #:
    SH_CHANNEL_CREATE_RESPONSE = enum.auto() #:
    SH_CHANNEL_DESTROY = enum.auto() #:
    SH_CHANNEL_DESTROY_RESPONSE = enum.auto() #:
    SH_LOCK_CHANNEL = enum.auto() #:
    SH_LOCK_CHANNEL_RESPONSE = enum.auto() #:
    SH_ALLOC_MSG = enum.auto() #:
    SH_ALLOC_MSG_RESPONSE = enum.auto() #:
    SH_ALLOC_BLOCK = enum.auto() #:
    SH_ALLOC_BLOCK_RESPONSE = enum.auto() #:
    SH_CHANNELS_UP = enum.auto() #:
    SH_PING_GS = enum.auto() #:
    SH_HALTED = enum.auto() #:
    SH_FWD_INPUT = enum.auto() #:
    SH_FWD_INPUT_ERR = enum.auto() #:
    SH_FWD_OUTPUT = enum.auto() #:
    GS_TEARDOWN = enum.auto() #:
    SH_TEARDOWN = enum.auto() #:
    SH_PING_BE = enum.auto() #:
    BE_PING_SH = enum.auto() #:
    TA_PING_SH = enum.auto() #:
    SH_HALT_TA = enum.auto() #:
    TA_HALTED = enum.auto() #:
    SH_HALT_BE = enum.auto() #:
    BE_HALTED = enum.auto() #:
    TA_UP = enum.auto() #:
    GS_PING_PROC = enum.auto() #:
    GS_DUMP_STATE = enum.auto() #:
    SH_DUMP_STATE = enum.auto() #:
    LA_BROADCAST = enum.auto() #:
    LA_PASS_THRU_FB = enum.auto() #:
    LA_PASS_THRU_BF = enum.auto() #:
    GS_POOL_CREATE = enum.auto() #:
    GS_POOL_CREATE_RESPONSE = enum.auto() #:
    GS_POOL_DESTROY = enum.auto() #:
    GS_POOL_DESTROY_RESPONSE = enum.auto() #:
    GS_POOL_LIST = enum.auto() #:
    GS_POOL_LIST_RESPONSE = enum.auto() #:
    GS_POOL_QUERY = enum.auto() #:
    GS_POOL_QUERY_RESPONSE = enum.auto() #:
    SH_POOL_CREATE = enum.auto() #:
    SH_POOL_CREATE_RESPONSE = enum.auto() #:
    SH_POOL_DESTROY = enum.auto() #:
    SH_POOL_DESTROY_RESPONSE = enum.auto() #:
    SH_CREATE_PROCESS_LOCAL_CHANNEL = enum.auto() #:
    SH_CREATE_PROCESS_LOCAL_CHANNEL_RESPONSE = enum.auto() #:
    SH_PUSH_KVL = enum.auto() #:
    SH_PUSH_KVL_RESPONSE = enum.auto() #:
    SH_POP_KVL = enum.auto() #:
    SH_POP_KVL_RESPONSE = enum.auto() #:
    SH_GET_KVL = enum.auto() #:
    SH_GET_KVL_RESPONSE = enum.auto() #:
    SH_SET_KV = enum.auto() #:
    SH_SET_KV_RESPONSE = enum.auto()
    SH_GET_KV = enum.auto()
    SH_GET_KV_RESPONSE = enum.auto()
    SH_EXEC_MEM_REQUEST = enum.auto() #:
    SH_EXEC_MEM_RESPONSE = enum.auto() #:
    GS_UNEXPECTED = enum.auto() #:
    LA_SERVER_MODE = enum.auto() #:
    LA_SERVER_MODE_EXIT = enum.auto() #:
    LA_PROCESS_DICT = enum.auto() #:
    LA_PROCESS_DICT_RESPONSE = enum.auto() #:
    LA_DUMP_STATE = enum.auto() #:
    BE_NODE_IDX_SH = enum.auto() #:
    LA_CHANNELS_INFO = enum.auto() #:
    SH_PROCESS_KILL_RESPONSE = enum.auto() #:
    BREAKPOINT = enum.auto() #:
    GS_PROCESS_JOIN_LIST = enum.auto() #:
    GS_PROCESS_JOIN_LIST_RESPONSE = enum.auto() #:
    GS_NODE_QUERY = enum.auto() #:
    GS_NODE_QUERY_RESPONSE = enum.auto() #:
    LOGGING_MSG = enum.auto() #:
    LOGGING_MSG_LIST = enum.auto() #:
    LOG_FLUSHED = enum.auto() #:
    GS_NODE_LIST = enum.auto() #:
    GS_NODE_LIST_RESPONSE = enum.auto() #:
    GS_NODE_QUERY_TOTAL_CPU_COUNT = enum.auto() #:
    GS_NODE_QUERY_TOTAL_CPU_COUNT_RESPONSE = enum.auto() #:
    BE_IS_UP = enum.auto() #:
    FE_NODE_IDX_BE = enum.auto() #:
    HALT_OVERLAY = enum.auto() #:
    HALT_LOGGING_INFRA = enum.auto() #:
    OVERLAY_PING_BE = enum.auto() #:
    OVERLAY_PING_LA = enum.auto() #:
    LA_HALT_OVERLAY = enum.auto() #:
    BE_HALT_OVERLAY = enum.auto() #:
    OVERLAY_HALTED = enum.auto() #:
    EXCEPTIONLESS_ABORT = enum.auto() #: Communicate abnormal termination without raising exception
    LA_EXIT = enum.auto() #:
    GS_GROUP_LIST = enum.auto() #:
    GS_GROUP_LIST_RESPONSE = enum.auto() #:
    GS_GROUP_QUERY = enum.auto() #:
    GS_GROUP_QUERY_RESPONSE = enum.auto() #:
    GS_GROUP_DESTROY = enum.auto() #:
    GS_GROUP_DESTROY_RESPONSE = enum.auto() #:
    GS_GROUP_ADD_TO = enum.auto() #:
    GS_GROUP_ADD_TO_RESPONSE = enum.auto() #:
    GS_GROUP_REMOVE_FROM = enum.auto() #:
    GS_GROUP_REMOVE_FROM_RESPONSE = enum.auto() #:
    GS_GROUP_CREATE = enum.auto() #:
    GS_GROUP_CREATE_RESPONSE = enum.auto() #:
    GS_GROUP_KILL = enum.auto() #:
    GS_GROUP_KILL_RESPONSE = enum.auto() #:
    GS_GROUP_CREATE_ADD_TO = enum.auto() #:
    GS_GROUP_CREATE_ADD_TO_RESPONSE = enum.auto() #:
    GS_GROUP_DESTROY_REMOVE_FROM = enum.auto() #:
    GS_GROUP_DESTROY_REMOVE_FROM_RESPONSE = enum.auto() #:
    TA_UPDATE_NODES = enum.auto() #:
    RUNTIME_DESC = enum.auto() #:
    USER_HALT_OOB = enum.auto() #:
    DD_REGISTER_CLIENT = enum.auto() #:
    DD_REGISTER_CLIENT_RESPONSE = enum.auto() #:
    DD_DESTROY = enum.auto() #:
    DD_DESTROY_RESPONSE = enum.auto() #:
    DD_REGISTER_MANAGER = enum.auto() #:
    DD_REGISTER_MANAGER_RESPONSE = enum.auto() #:
    DD_REGISTER_CLIENT_ID = enum.auto() #:
    DD_REGISTER_CLIENT_ID_RESPONSE = enum.auto() #:
    DD_DESTROY_MANAGER = enum.auto() #:
    DD_DESTROY_MANAGER_RESPONSE = enum.auto() #:
    DD_PUT = enum.auto() #:
    DD_PUT_RESPONSE = enum.auto() #:
    DD_GET = enum.auto() #:
    DD_GET_RESPONSE = enum.auto() #:
    DD_POP = enum.auto() #:
    DD_POP_RESPONSE = enum.auto() #:
    DD_CONTAINS = enum.auto() #:
    DD_CONTAINS_RESPONSE = enum.auto() #:
    DD_GET_LENGTH = enum.auto() #:
    DD_GET_LENGTH_RESPONSE = enum.auto() #:
    DD_CLEAR = enum.auto() #:
    DD_CLEAR_RESPONSE = enum.auto() #:
    DD_GET_ITERATOR = enum.auto() #:
    DD_GET_ITERATOR_RESPONSE = enum.auto() #:
    DD_ITERATOR_NEXT = enum.auto() #:
    DD_ITERATOR_NEXT_RESPONSE = enum.auto() #:
    DD_KEYS = enum.auto() #:
    DD_KEYS_RESPONSE = enum.auto() #:
    DD_DEREGISTER_CLIENT = enum.auto() #:
    DD_DEREGISTER_CLIENT_RESPONSE = enum.auto() #:
    DD_CREATE = enum.auto() #:
    DD_CREATE_RESPONSE = enum.auto() #:
    DD_CONNECT_TO_MANAGER = enum.auto() #:
    DD_CONNECT_TO_MANAGER_RESPONSE = enum.auto() #:
    DD_GET_RANDOM_MANAGER = enum.auto() #:
    DD_GET_RANDOM_MANAGER_RESPONSE = enum.auto() #:


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
class PMIGroupInfo():
    """
    Required information to enable the launching of pmi based applications.
    """

    job_id: int
    nnodes: int
    nranks: int
    nidlist: list[int]
    hostlist: list[str]
    control_port: int

    @classmethod
    def fromdict(cls, d):
        try:
            return cls(**d)
        except Exception as exc:
            raise ValueError(f'Error deserializing {cls.__name__} {d=}') from exc


@dataclass
class PMIProcessInfo():
    """
    Required information to enable the launching of pmi based applications.
    """

    lrank: int
    ppn: int
    nid: int
    pid_base: int

    @classmethod
    def fromdict(cls, d):
        try:
            return cls(**d)
        except Exception as exc:
            raise ValueError(f'Error deserializing {cls.__name__} {d=}') from exc


class InfraMsg(object):
    """Common base for all messages.

        This common base type for all messages sets up the
        default fields and the serialization strategy for
        now.
    """

    _tc = MessageTypes.DRAGON_MSG  # deliberately invalid value, overridden

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

    @classmethod
    def deserialize(cls, msg):
        raise ValueError('Called deserialize on InfraMsg base class which should not happen.')

    def uncompressed_serialize(self):
        return json.dumps(self.get_sdict())

    def serialize(self):
        return b64encode(zlib.compress(json.dumps(self.get_sdict()).encode('utf-8')))

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


class CapNProtoMsg:
    """Common base for all capnproto messages.

        This common base type for all messages sets up the
        default fields and the serialization strategy for
        messages to be exchanged between C and Python.
    """
    Errors = DragonError

    _tc = MessageTypes.DRAGON_MSG  # deliberately invalid value, overridden

    def __init__(self, tag):
        self._tag = tag

    @classmethod
    def from_sdict(cls, sdict):
        return cls(**sdict)

    @classmethod
    def deserialize(cls, msg_str):
        msg = capnp_schema.MessageDef.from_bytes_packed(msg_str)
        sdict = msg.to_dict()
        flattened_dict = {}
        typecode = sdict['tc']
        del sdict['tc']
        tag = sdict['tag']
        del sdict['tag']
        if 'value' in sdict['responseOption']:
            flattened_dict.update(sdict['responseOption']['value'])
        del sdict['responseOption']
        for msg_type in sdict:
            for field in sdict[msg_type]:
                flattened_dict[field] = sdict[msg_type][field]
        flattened_dict['tag'] = tag
        if 'none' in flattened_dict:
            del flattened_dict['none']

        return mt_dispatch[typecode].from_sdict(flattened_dict)

    def serialize(self):
        cap_msg = self.builder()
        return cap_msg.to_bytes_packed()

    def get_sdict(self):
        rv = {'_tc': self._tc.value,
              'tag': self.tag}
        return rv

    def builder(self):
        cap_msg = capnp_schema.MessageDef.new_message()
        cap_msg.tc = self._tc.value
        cap_msg.tag = self._tag
        return cap_msg

    def __repr__(self):
        fields_to_set = self.get_sdict()
        del fields_to_set['_tc']
        fs = ', '.join([f'{k!s}={v!r}' for k, v in fields_to_set.items()])
        return f'{self.__class__.__name__}({fs})'

    @property
    def capnp_name(self):
        name = self.__class__.__name__
        return name[:2].lower()+name[2:]

    @property
    def tc(self):
        return self._tc

    @property
    def tag(self):
        return self._tag

class CapNProtoResponseMsg(CapNProtoMsg):
    """Common base for all capnproto response messages.

        This provides some support for code common
        to all response messages.
    """
    def __init__(self, tag, ref, err, errInfo):
        super().__init__(tag)
        self._ref = ref
        self._err = err
        self._errInfo = errInfo

    def get_sdict(self):
        rv = super().get_sdict()
        rv['ref'] = self._ref
        rv['err'] = self._err
        rv['errInfo'] = self._errInfo
        return rv

    def builder(self):
        cap_msg = super().builder()
        resp_msg = cap_msg.init('responseOption').init('value')
        resp_msg.ref = self._ref
        resp_msg.err = DragonError(self._err).value
        resp_msg.errInfo = self._errInfo
        return cap_msg

    @property
    def ref(self):
        return self._ref

    @property
    def err(self):
        return self._err

    @property
    def errInfo(self):
        return self._errInfo


class SHCreateProcessLocalChannel(CapNProtoMsg):

    _tc = MessageTypes.SH_CREATE_PROCESS_LOCAL_CHANNEL

    def __init__(self, tag, puid, respFLI):
        super().__init__(tag)
        self._puid = puid
        self._respFLI = respFLI

    def get_sdict(self):
        rv = super().get_sdict()
        rv['puid'] = self._puid
        rv['respFLI'] = self._respFLI
        return rv

    def builder(self):
        cap_msg = super().builder()
        client_msg = cap_msg.init(self.capnp_name)
        client_msg.puid = self._puid
        client_msg.respFLI = self._respFLI
        return cap_msg

    @property
    def respFLI(self):
        return self._respFLI

    @property
    def puid(self):
        return self._puid

class SHCreateProcessLocalChannelResponse(CapNProtoResponseMsg):

    _tc = MessageTypes.SH_CREATE_PROCESS_LOCAL_CHANNEL_RESPONSE

    def __init__(self, tag, ref, err, errInfo='', serChannel=''):
        super().__init__(tag, ref, err, errInfo)
        self._serChannel = serChannel

    def get_sdict(self):
        rv = super().get_sdict()
        rv['serChannel'] = self._serChannel
        return rv

    def builder(self):
        cap_msg = super().builder()
        client_msg = cap_msg.init(self.capnp_name)
        client_msg.serChannel = self._serChannel
        return cap_msg

    @property
    def serialized_channel(self):
        return self._serChannel

class SHPushKVL(CapNProtoMsg):
    _tc = MessageTypes.SH_PUSH_KVL

    def __init__(self, tag, key, value, respFLI):
        super().__init__(tag)
        self._key = key
        self._value = value
        self._respFLI = respFLI

    def get_sdict(self):
        rv = super().get_sdict()
        rv['key'] = self._key
        rv['value'] = self._value
        rv['respFLI'] = self._respFLI
        return rv

    def builder(self):
        cap_msg = super().builder()
        client_msg = cap_msg.init(self.capnp_name)
        client_msg.key = self._key
        client_msg.value = self._value
        client_msg.respFLI = self._respFLI
        return cap_msg

    @property
    def key(self):
        return self._key

    @property
    def value(self):
        return self._value

    @property
    def respFLI(self):
        return self._respFLI

class SHPushKVLResponse(CapNProtoResponseMsg):

    _tc = MessageTypes.SH_PUSH_KVL_RESPONSE

    def __init__(self, tag, ref, err, errInfo=''):
        super().__init__(tag, ref, err, errInfo)

class SHPopKVL(CapNProtoMsg):
    _tc = MessageTypes.SH_POP_KVL

    def __init__(self, tag, key, value, respFLI):
        super().__init__(tag)
        self._key = key
        self._value = value
        self._respFLI = respFLI

    def get_sdict(self):
        rv = super().get_sdict()
        rv['key'] = self._key
        rv['value'] = self._value
        rv['respFLI'] = self._respFLI
        return rv

    def builder(self):
        cap_msg = super().builder()
        client_msg = cap_msg.init(self.capnp_name)
        client_msg.key = self._key
        client_msg.value = self._value
        client_msg.respFLI = self._respFLI
        return cap_msg

    @property
    def key(self):
        return self._key

    @property
    def value(self):
        return self._value

    @property
    def respFLI(self):
        return self._respFLI


class SHPopKVLResponse(CapNProtoResponseMsg):

    _tc = MessageTypes.SH_POP_KVL_RESPONSE

    def __init__(self, tag, ref, err, errInfo=''):
        super().__init__(tag, ref, err, errInfo)

class SHGetKVL(CapNProtoMsg):
    _tc = MessageTypes.SH_GET_KVL

    def __init__(self, tag, key, respFLI):
        super().__init__(tag)
        self._key = key
        self._respFLI = respFLI

    def get_sdict(self):
        rv = super().get_sdict()
        rv['key'] = self._key
        rv['respFLI'] = self._respFLI
        return rv

    def builder(self):
        cap_msg = super().builder()
        client_msg = cap_msg.init(self.capnp_name)
        client_msg.key = self._key
        client_msg.respFLI = self._respFLI
        return cap_msg

    @property
    def key(self):
        return self._key

    @property
    def respFLI(self):
        return self._respFLI


class SHGetKVLResponse(CapNProtoResponseMsg):

    _tc = MessageTypes.SH_GET_KVL_RESPONSE

    def __init__(self, tag, ref, err, errInfo='', values=[]):
        super().__init__(tag, ref, err, errInfo)
        self._values = values

    def get_sdict(self):
        rv = super().get_sdict()
        rv['values'] = self._values
        return rv

    def builder(self):
        cap_msg = super().builder()
        client_msg = cap_msg.init(self.capnp_name)
        values = client_msg.init('values', len(self._values))
        for i in range(len(self._values)):
            values[i] = self._values[i]
        return cap_msg

    @property
    def values(self):
        return self._values

class SHSetKV(CapNProtoMsg):
    _tc = MessageTypes.SH_SET_KV

    def __init__(self, tag, key, value, respFLI):
        super().__init__(tag)
        self._key = key
        self._value = value
        self._respFLI = respFLI

    def get_sdict(self):
        rv = super().get_sdict()
        rv['key'] = self._key
        rv['value'] = self._value
        rv['respFLI'] = self._respFLI
        return rv

    def builder(self):
        cap_msg = super().builder()
        client_msg = cap_msg.init(self.capnp_name)
        client_msg.key = self._key
        client_msg.value = self._value
        client_msg.respFLI = self._respFLI
        return cap_msg

    @property
    def key(self):
        return self._key

    @property
    def value(self):
        return self._value

    @property
    def respFLI(self):
        return self._respFLI

class SHSetKVResponse(CapNProtoResponseMsg):

    _tc = MessageTypes.SH_SET_KV_RESPONSE

    def __init__(self, tag, ref, err, errInfo=''):
        super().__init__(tag, ref, err, errInfo)


class SHGetKV(CapNProtoMsg):
    _tc = MessageTypes.SH_GET_KV

    def __init__(self, tag, key, respFLI):
        super().__init__(tag)
        self._key = key
        self._respFLI = respFLI

    def get_sdict(self):
        rv = super().get_sdict()
        rv['key'] = self._key
        rv['respFLI'] = self._respFLI
        return rv

    def builder(self):
        cap_msg = super().builder()
        client_msg = cap_msg.init(self.capnp_name)
        client_msg.key = self._key
        client_msg.respFLI = self._respFLI
        return cap_msg

    @property
    def key(self):
        return self._key

    @property
    def respFLI(self):
        return self._respFLI


class SHGetKVResponse(CapNProtoResponseMsg):

    _tc = MessageTypes.SH_GET_KV_RESPONSE

    def __init__(self, tag, ref, err, errInfo='', value=None):
        super().__init__(tag, ref, err, errInfo)
        self._value = value

    def get_sdict(self):
        rv = super().get_sdict()
        rv['value'] = self._value
        return rv

    def builder(self):
        cap_msg = super().builder()
        client_msg = cap_msg.init(self.capnp_name)
        client_msg.value = self._value
        return cap_msg

    @property
    def value(self):
        return self._value

class DDCreate(CapNProtoMsg):

    _tc = MessageTypes.DD_CREATE

    def __init__(self, tag, respFLI, args):
        super().__init__(tag)
        self._respFLI = respFLI
        self._args = args

    def get_sdict(self):
        rv = super().get_sdict()
        rv['respFLI'] = self._respFLI
        rv['args'] = self._args
        return rv

    def builder(self):
        cap_msg = super().builder()
        client_msg = cap_msg.init(self.capnp_name)
        client_msg.respFLI = self._respFLI
        client_msg.args = self._args
        return cap_msg

    @property
    def respFLI(self):
        return self._respFLI

    @property
    def args(self):
        return self._args

class DDCreateResponse(CapNProtoResponseMsg):

    _tc = MessageTypes.DD_CREATE_RESPONSE

    def __init__(self, tag, ref, err, errInfo=''):
        super().__init__(tag, ref, err, errInfo)

class DDGetRandomManager(CapNProtoMsg):

    _tc = MessageTypes.DD_GET_RANDOM_MANAGER

    def __init__(self, tag, respFLI):
        super().__init__(tag)
        self._respFLI = respFLI

    def get_sdict(self):
        rv = super().get_sdict()
        rv['respFLI'] = self._respFLI
        return rv

    def builder(self):
        cap_msg = super().builder()
        client_msg = cap_msg.init(self.capnp_name)
        client_msg.respFLI = self._respFLI
        return cap_msg

    @property
    def respFLI(self):
        return self._respFLI

class DDGetRandomManagerResponse(CapNProtoResponseMsg):

    _tc = MessageTypes.DD_GET_RANDOM_MANAGER_RESPONSE

    def __init__(self, tag, ref, err, manager, errInfo=''):
        super().__init__(tag, ref, err, errInfo)
        self._manager = manager

    def get_sdict(self):
        rv = super().get_sdict()
        rv['manager'] = self._manager
        return rv

    def builder(self):
        cap_msg = super().builder()
        client_msg = cap_msg.init(self.capnp_name)
        client_msg.manager = self._manager
        return cap_msg

    @property
    def manager(self):
        return self._manager

class DDRegisterClient(CapNProtoMsg):

    _tc = MessageTypes.DD_REGISTER_CLIENT

    def __init__(self, tag, respFLI, bufferedRespFLI):
        super().__init__(tag)
        self._respFLI = respFLI
        self._bufferedRespFLI = bufferedRespFLI

    def get_sdict(self):
        rv = super().get_sdict()
        rv['respFLI'] = self._respFLI
        rv['bufferedRespFLI'] = self._bufferedRespFLI
        return rv

    def builder(self):
        cap_msg = super().builder()
        client_msg = cap_msg.init(self.capnp_name)
        client_msg.respFLI = self._respFLI
        client_msg.bufferedRespFLI = self._bufferedRespFLI
        return cap_msg

    @property
    def respFLI(self):
        return self._respFLI

    @property
    def bufferedRespFLI(self):
        return self._bufferedRespFLI

class DDRegisterClientResponse(CapNProtoResponseMsg):

    _tc = MessageTypes.DD_REGISTER_CLIENT_RESPONSE

    def __init__(self, tag, ref, err, clientID, numManagers, errInfo=''):
        super().__init__(tag, ref, err, errInfo)
        self._clientID = clientID
        self._num_managers = numManagers

    def get_sdict(self):
        rv = super().get_sdict()
        rv['clientID'] = self._clientID
        rv['numManagers'] = self._num_managers
        return rv

    def builder(self):
        cap_msg = super().builder()
        client_msg = cap_msg.init(self.capnp_name)
        client_msg.clientID = self._clientID
        client_msg.numManagers = self._num_managers
        return cap_msg

    @property
    def clientID(self):
        return self._clientID

    @property
    def numManagers(self):
        return self._num_managers

class DDConnectToManager(CapNProtoMsg):

    _tc = MessageTypes.DD_CONNECT_TO_MANAGER

    def __init__(self, tag, clientID, managerID):
        super().__init__(tag)
        self._clientID = clientID
        self._managerID = managerID

    def get_sdict(self):
        rv = super().get_sdict()
        rv['clientID'] = self._clientID
        rv['managerID'] = self._managerID
        return rv

    def builder(self):
        cap_msg = super().builder()
        client_msg = cap_msg.init(self.capnp_name)
        client_msg.clientID = self._clientID
        client_msg.managerID = self._managerID
        return cap_msg

    @property
    def clientID(self):
        return self._clientID

    @property
    def managerID(self):
        return self._managerID

class DDConnectToManagerResponse(CapNProtoResponseMsg):

    _tc = MessageTypes.DD_CONNECT_TO_MANAGER_RESPONSE

    def __init__(self, tag, ref, err, manager, errInfo=''):
        super().__init__(tag, ref, err, errInfo)
        self._manager = manager

    def get_sdict(self):
        rv = super().get_sdict()
        rv['manager'] = self._manager
        return rv

    def builder(self):
        cap_msg = super().builder()
        client_msg = cap_msg.init(self.capnp_name)
        client_msg.manager = self._manager
        return cap_msg

    @property
    def manager(self):
        return self._manager

class DDDestroy(CapNProtoMsg):

    _tc = MessageTypes.DD_DESTROY

    def __init__(self, tag, clientID, respFLI):
        super().__init__(tag)
        self._clientID = clientID
        self._respFLI = respFLI

    def get_sdict(self):
        rv = super().get_sdict()
        rv['clientID'] = self._clientID
        rv['respFLI'] = self._respFLI
        return rv

    def builder(self):
        cap_msg = super().builder()
        client_msg = cap_msg.init(self.capnp_name)
        client_msg.clientID = self._clientID
        client_msg.respFLI = self.respFLI
        return cap_msg

    @property
    def respFLI(self):
        return self._respFLI

    @property
    def clientID(self):
        return self._clientID

class DDDestroyResponse(CapNProtoResponseMsg):

    _tc = MessageTypes.DD_DESTROY_RESPONSE

    def __init__(self, tag, ref, err, errInfo=''):
        super().__init__(tag, ref, err, errInfo)

class DDRegisterManager(CapNProtoMsg):

    _tc = MessageTypes.DD_REGISTER_MANAGER

    def __init__(self, tag, mainFLI, respFLI):
        super().__init__(tag)
        self._mainFLI = mainFLI
        self._respFLI = respFLI

    def get_sdict(self):
        rv = super().get_sdict()
        rv['mainFLI'] = self._mainFLI
        rv['respFLI'] = self._respFLI
        return rv

    def builder(self):
        cap_msg = super().builder()
        client_msg = cap_msg.init(self.capnp_name)
        client_msg.mainFLI = self._mainFLI
        client_msg.respFLI = self._respFLI
        return cap_msg

    @property
    def mainFLI(self):
        return self._mainFLI

    @property
    def respFLI(self):
        return self._respFLI

class DDRegisterManagerResponse(CapNProtoResponseMsg):

    _tc = MessageTypes.DD_REGISTER_MANAGER_RESPONSE

    def __init__(self, tag, ref, err, managerID, errInfo='', managers=[]):
        super().__init__(tag, ref, err, errInfo)
        self._managers = managers
        self._managerID = managerID

    def get_sdict(self):
        rv = super().get_sdict()
        rv['managers'] = self._managers
        rv['managerID'] = self._managerID
        return rv

    def builder(self):
        cap_msg = super().builder()
        client_msg = cap_msg.init(self.capnp_name)
        client_msg.managerID = self._managerID
        msg_mgrs = client_msg.init('managers', len(self._managers))
        for i in range(len(self._managers)):
            msg_mgrs[i] = self._managers[i]
        return cap_msg

    @property
    def managerID(self):
        return self._managerID

    @property
    def managers(self):
        return self._managers

class DDRegisterClientID(CapNProtoMsg):

    _tc = MessageTypes.DD_REGISTER_CLIENT_ID

    def __init__(self, tag, clientID, respFLI, bufferedRespFLI):
        super().__init__(tag)
        self._clientID = clientID
        self._respFLI = respFLI
        self._bufferedRespFLI = bufferedRespFLI

    def get_sdict(self):
        rv = super().get_sdict()
        rv['clientID'] = self._clientID
        rv['respFLI'] = self._respFLI
        rv['bufferedRespFLI'] = self._bufferedRespFLI
        return rv

    def builder(self):
        cap_msg = super().builder()
        client_msg = cap_msg.init(self.capnp_name)
        client_msg.clientID = self._clientID
        client_msg.respFLI = self._respFLI
        client_msg.bufferedRespFLI = self._bufferedRespFLI
        return cap_msg

    @property
    def clientID(self):
        return self._clientID

    @property
    def respFLI(self):
        return self._respFLI

    @property
    def bufferedRespFLI(self):
        return self._bufferedRespFLI

class DDRegisterClientIDResponse(CapNProtoResponseMsg):

    _tc = MessageTypes.DD_REGISTER_CLIENT_ID_RESPONSE

    def __init__(self, tag, ref, err, errInfo=''):
        super().__init__(tag, ref, err, errInfo)

class DDDestroyManager(CapNProtoMsg):

    _tc = MessageTypes.DD_DESTROY_MANAGER

    def __init__(self, tag, respFLI):
        super().__init__(tag)
        self._respFLI = respFLI

    def get_sdict(self):
        rv = super().get_sdict()
        rv['respFLI'] = self._respFLI
        return rv

    def builder(self):
        cap_msg = super().builder()
        client_msg = cap_msg.init(self.capnp_name)
        client_msg.respFLI = self._respFLI
        return cap_msg

    @property
    def respFLI(self):
        return self._respFLI

class DDDestroyManagerResponse(CapNProtoResponseMsg):

    _tc = MessageTypes.DD_DESTROY_MANAGER_RESPONSE

    def __init__(self, tag, ref, err, errInfo=''):
        super().__init__(tag, ref, err, errInfo)

class DDPut(CapNProtoMsg):

    _tc = MessageTypes.DD_PUT

    def __init__(self, tag, clientID):
        super().__init__(tag)
        self._clientID = clientID

    def get_sdict(self):
        rv = super().get_sdict()
        rv['clientID'] = self._clientID
        return rv

    def builder(self):
        cap_msg = super().builder()
        client_msg = cap_msg.init(self.capnp_name)
        client_msg.clientID = self._clientID
        return cap_msg

    @property
    def clientID(self):
        return self._clientID

class DDPutResponse(CapNProtoResponseMsg):

    _tc = MessageTypes.DD_PUT_RESPONSE

    def __init__(self, tag, ref, err, errInfo=''):
        super().__init__(tag, ref, err, errInfo)

class DDGet(CapNProtoMsg):

    _tc = MessageTypes.DD_GET

    def __init__(self, tag, clientID):
        super().__init__(tag)
        self._clientID = clientID

    def get_sdict(self):
        rv = super().get_sdict()
        rv['clientID'] = self._clientID
        return rv

    def builder(self):
        cap_msg = super().builder()
        client_msg = cap_msg.init(self.capnp_name)
        client_msg.clientID = self._clientID
        return cap_msg

    @property
    def clientID(self):
        return self._clientID

class DDGetResponse(CapNProtoResponseMsg):

    _tc = MessageTypes.DD_GET_RESPONSE

    def __init__(self, tag, ref, err, errInfo=''):
        super().__init__(tag, ref, err, errInfo)

class DDPop(CapNProtoMsg):

    _tc = MessageTypes.DD_POP

    def __init__(self, tag, clientID):
        super().__init__(tag)
        self._clientID = clientID

    def get_sdict(self):
        rv = super().get_sdict()
        rv['clientID'] = self._clientID
        return rv

    def builder(self):
        cap_msg = super().builder()
        client_msg = cap_msg.init(self.capnp_name)
        client_msg.clientID = self._clientID
        return cap_msg

    @property
    def clientID(self):
        return self._clientID

class DDPopResponse(CapNProtoResponseMsg):

    _tc = MessageTypes.DD_POP_RESPONSE

    def __init__(self, tag, ref, err, errInfo=''):
        super().__init__(tag, ref, err, errInfo)

class DDContains(CapNProtoMsg):

    _tc = MessageTypes.DD_CONTAINS

    def __init__(self, tag, clientID):
        super().__init__(tag)
        self._clientID = clientID

    def get_sdict(self):
        rv = super().get_sdict()
        rv['clientID'] = self._clientID
        return rv

    def builder(self):
        cap_msg = super().builder()
        client_msg = cap_msg.init(self.capnp_name)
        client_msg.clientID = self._clientID
        return cap_msg

    @property
    def clientID(self):
        return self._clientID

class DDContainsResponse(CapNProtoResponseMsg):

    _tc = MessageTypes.DD_CONTAINS_RESPONSE

    def __init__(self, tag, ref, err, errInfo=''):
        super().__init__(tag, ref, err, errInfo)

class DDGetLength(CapNProtoMsg):

    _tc = MessageTypes.DD_GET_LENGTH

    def __init__(self, tag, clientID):
        super().__init__(tag)
        self._clientID = clientID

    def get_sdict(self):
        rv = super().get_sdict()
        rv['clientID'] = self._clientID
        return rv

    def builder(self):
        cap_msg = super().builder()
        client_msg = cap_msg.init(self.capnp_name)
        client_msg.clientID = self._clientID
        return cap_msg

    @property
    def clientID(self):
        return self._clientID

class DDGetLengthResponse(CapNProtoResponseMsg):

    _tc = MessageTypes.DD_GET_LENGTH_RESPONSE

    def __init__(self, tag, ref, err, errInfo='', length=0):
        super().__init__(tag, ref, err, errInfo)
        self._length = length

    def get_sdict(self):
        rv = super().get_sdict()
        rv['length'] = self._length
        return rv

    def builder(self):
        cap_msg = super().builder()
        client_msg = cap_msg.init(self.capnp_name)
        client_msg.length = self._length
        return cap_msg

    @property
    def length(self):
        return self._length

class DDClear(CapNProtoMsg):

    _tc = MessageTypes.DD_CLEAR

    def __init__(self, tag, clientID):
        super().__init__(tag)
        self._clientID = clientID

    def get_sdict(self):
        rv = super().get_sdict()
        rv['clientID'] = self._clientID
        return rv

    def builder(self):
        cap_msg = super().builder()
        client_msg = cap_msg.init(self.capnp_name)
        client_msg.clientID = self._clientID
        return cap_msg

    @property
    def clientID(self):
        return self._clientID

class DDClearResponse(CapNProtoResponseMsg):

    _tc = MessageTypes.DD_CLEAR_RESPONSE

    def __init__(self, tag, ref, err, errInfo=''):
        super().__init__(tag, ref, err, errInfo)

class DDGetIterator(CapNProtoMsg):

    _tc = MessageTypes.DD_GET_ITERATOR

    def __init__(self, tag, clientID):
        super().__init__(tag)
        self._clientID = clientID

    def get_sdict(self):
        rv = super().get_sdict()
        rv['clientID'] = self._clientID
        return rv

    def builder(self):
        cap_msg = super().builder()
        client_msg = cap_msg.init(self.capnp_name)
        client_msg.clientID = self._clientID
        return cap_msg

    @property
    def clientID(self):
        return self._clientID

class DDGetIteratorResponse(CapNProtoResponseMsg):

    _tc = MessageTypes.DD_GET_ITERATOR_RESPONSE

    def __init__(self, tag, ref, err, errInfo='', iterID=0):
        super().__init__(tag, ref, err, errInfo)
        self._iter_id = iterID

    def get_sdict(self):
        rv = super().get_sdict()
        rv['iterID'] = self._iter_id
        return rv

    def builder(self):
        cap_msg = super().builder()
        client_msg = cap_msg.init(self.capnp_name)
        client_msg.iterID = self._iter_id
        return cap_msg

    @property
    def iterID(self):
        return self._iter_id

class DDIteratorNext(CapNProtoMsg):

    _tc = MessageTypes.DD_ITERATOR_NEXT

    def __init__(self, tag, clientID, iterID):
        super().__init__(tag)
        self._clientID = clientID
        self._iterID = iterID

    def get_sdict(self):
        rv = super().get_sdict()
        rv['clientID'] = self._clientID
        rv['iterID'] = self._iterID
        return rv

    def builder(self):
        cap_msg = super().builder()
        client_msg = cap_msg.init(self.capnp_name)
        client_msg.clientID = self._clientID
        client_msg.iterID = self._iterID
        return cap_msg

    @property
    def clientID(self):
        return self._clientID

    @property
    def iterID(self):
        return self._iterID

class DDIteratorNextResponse(CapNProtoResponseMsg):

    _tc = MessageTypes.DD_ITERATOR_NEXT_RESPONSE

    def __init__(self, tag, ref, err, errInfo=''):
        super().__init__(tag, ref, err, errInfo)

class DDKeys(CapNProtoMsg):

    _tc = MessageTypes.DD_KEYS

    def __init__(self, tag, clientID):
        super().__init__(tag)
        self._clientID = clientID

    def get_sdict(self):
        rv = super().get_sdict()
        rv['clientID'] = self._clientID
        return rv

    def builder(self):
        cap_msg = super().builder()
        client_msg = cap_msg.init(self.capnp_name)
        client_msg.clientID = self._clientID
        return cap_msg

    @property
    def clientID(self):
        return self._clientID

class DDKeysResponse(CapNProtoResponseMsg):

    _tc = MessageTypes.DD_KEYS_RESPONSE

    def __init__(self, tag, ref, err, errInfo=''):
        super().__init__(tag, ref, err, errInfo)

class DDDeregisterClient(CapNProtoMsg):

    _tc = MessageTypes.DD_DEREGISTER_CLIENT

    def __init__(self, tag, clientID, respFLI):
        super().__init__(tag)
        self._clientID = clientID
        self._respFLI = respFLI

    def get_sdict(self):
        rv = super().get_sdict()
        rv['clientID'] = self._clientID
        rv['respFLI'] = self._respFLI
        return rv

    def builder(self):
        cap_msg = super().builder()
        client_msg = cap_msg.init(self.capnp_name)
        client_msg.clientID = self._clientID
        client_msg.respFLI = self._respFLI
        return cap_msg

    @property
    def clientID(self):
        return self._clientID

    @property
    def respFLI(self):
        return self._respFLI

    @respFLI.setter
    def respFLI(self, respFLI):
        self._respFLI = respFLI

class DDDeregisterClientResponse(CapNProtoResponseMsg):

    _tc = MessageTypes.DD_DEREGISTER_CLIENT_RESPONSE

    def __init__(self, tag, ref, err, errInfo=''):
        super().__init__(tag, ref, err, errInfo)

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

class GSProcessCreate(InfraMsg):
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
            self._pmi_info = PMIProcessInfo.fromdict(_pmi_info)
        elif isinstance(_pmi_info, PMIProcessInfo):
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


class GSProcessCreateResponse(InfraMsg):
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


class GSProcessList(InfraMsg):
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


class GSProcessListResponse(InfraMsg):
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


class GSProcessQuery(InfraMsg):
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


class GSProcessQueryResponse(InfraMsg):
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


class GSProcessKill(InfraMsg):
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


class GSProcessKillResponse(InfraMsg):
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


class GSProcessJoin(InfraMsg):
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


class GSProcessJoinResponse(InfraMsg):
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


class GSProcessJoinList(InfraMsg):
    """
            Refer to :ref:`definition<gsprocessjoinlist>` and :ref:`Common Fields<cfs>` for a description of
            the message structure.

    """

    _tc = MessageTypes.GS_PROCESS_JOIN_LIST

    def __init__(self, tag, p_uid, r_c_uid, timeout=-1,
                 t_p_uid_list=None, user_name_list=None,
                 join_all=False, return_on_bad_exit=False,
                 _tc=None):

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
        self.return_on_bad_exit = return_on_bad_exit

    def get_sdict(self):
        rv = super().get_sdict()
        rv['p_uid'] = self.p_uid
        rv['r_c_uid'] = self.r_c_uid
        rv['timeout'] = self.timeout
        rv['join_all'] = self.join_all
        rv['return_on_bad_exit'] = self.return_on_bad_exit
        if self.t_p_uid_list:
            rv['t_p_uid_list'] = self.t_p_uid_list
        if self.user_name_list:
            rv['user_name_list'] = self.user_name_list

        return rv

    # TODO AICI-1422 Implement verbose logging options
    # def __str__(self):
    #     first = super().__str__()
    #     return first + f' {self.t_p_uid_list}:{self.user_name_list}'


class GSProcessJoinListResponse(InfraMsg):
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


class GSPoolCreate(InfraMsg):
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


class GSPoolCreateResponse(InfraMsg):
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


class GSPoolList(InfraMsg):
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


class GSPoolListResponse(InfraMsg):
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


class GSPoolQuery(InfraMsg):
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


class GSPoolQueryResponse(InfraMsg):
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


class GSPoolDestroy(InfraMsg):
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


class GSPoolDestroyResponse(InfraMsg):
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


class GSGroupCreate(InfraMsg):
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
        elif isinstance(policy, list):
            temp_policies = []
            for p in policy:
                if isinstance(p, Policy):
                    temp_policies.append(p)
                elif isinstance(p, dict):
                    temp_policies.append(Policy(**p))
                else:
                    raise ValueError(f'GS Groups unsupported policy value {p=}')
            self.policy = temp_policies
        else:
            raise ValueError(f'GS Groups unsupported policy value {policy=}')

    def get_sdict(self):
        rv = super().get_sdict()

        rv['p_uid'] = self.p_uid
        rv['r_c_uid'] = self.r_c_uid
        rv['items'] = self.items
        if isinstance(self.policy, list):
            rv['policy'] = [policy.get_sdict() if isinstance(policy, Policy) else policy for policy in self.policy]
        else:
            rv['policy'] = self.policy.get_sdict()
        rv['user_name'] = self.user_name

        return rv


class GSGroupCreateResponse(InfraMsg):
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

class GSGroupList(InfraMsg):
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


class GSGroupListResponse(InfraMsg):
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


class GSGroupQuery(InfraMsg):
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


class GSGroupQueryResponse(InfraMsg):
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


class GSGroupKill(InfraMsg):
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


class GSGroupKillResponse(InfraMsg):
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


class GSGroupDestroy(InfraMsg):
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


class GSGroupDestroyResponse(InfraMsg):
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


class GSGroupAddTo(InfraMsg):
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


class GSGroupAddToResponse(InfraMsg):
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

class GSGroupCreateAddTo(InfraMsg):
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


class GSGroupCreateAddToResponse(InfraMsg):
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


class GSGroupRemoveFrom(InfraMsg):
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


class GSGroupRemoveFromResponse(InfraMsg):
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


class GSGroupDestroyRemoveFrom(InfraMsg):
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


class GSGroupDestroyRemoveFromResponse(InfraMsg):
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


class GSChannelCreate(InfraMsg):
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


class GSChannelCreateResponse(InfraMsg):
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


class GSChannelList(InfraMsg):
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


class GSChannelListResponse(InfraMsg):
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


class GSChannelQuery(InfraMsg):
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


class GSChannelQueryResponse(InfraMsg):
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


class GSChannelDestroy(InfraMsg):
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


class GSChannelDestroyResponse(InfraMsg):
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


class GSChannelJoin(InfraMsg):
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


class GSChannelJoinResponse(InfraMsg):
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


class GSChannelDetach(InfraMsg):
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


class GSChannelDetachResponse(InfraMsg):
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


class GSChannelGetSendH(InfraMsg):
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


class GSChannelGetSendHResponse(InfraMsg):
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


class GSChannelGetRecvH(InfraMsg):
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


class GSChannelGetRecvHResponse(InfraMsg):
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


class GSNodeList(InfraMsg):
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


class GSNodeListResponse(InfraMsg):
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


class GSNodeQuery(InfraMsg):
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


class GSNodeQueryResponse(InfraMsg):
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


class GSNodeQueryTotalCPUCount(InfraMsg):
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


class GSNodeQueryTotalCPUCountResponse(InfraMsg):
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


class AbnormalTermination(InfraMsg):
    """
            Refer to :ref:`definition<abnormaltermination>` and :ref:`Common Fields<cfs>` for a
            description of the message structure.

    """

    _tc = MessageTypes.ABNORMAL_TERMINATION

    def __init__(self, tag, err_info='', host_id=0,  _tc=None):
        super().__init__(tag)
        self.err_info = err_info
        self.host_id = host_id

    def get_sdict(self):
        rv = super().get_sdict()
        rv['err_info'] = self.err_info
        rv['host_id'] = self.host_id
        return rv

    def __str__(self):
        traceback = str(self.err_info)
        if len(traceback) > 0:
            return str(super()) + f'\n***** Dragon Traceback: *****\n{traceback}***** End of Dragon Traceback: *****\n'

        return str(super())


class ExceptionlessAbort(InfraMsg):
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


class GSStarted(InfraMsg):
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


class GSPingSH(InfraMsg):
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


class GSIsUp(InfraMsg):
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


class GSPingProc(InfraMsg):
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


class GSDumpState(InfraMsg):
    """
            Refer to :ref:`definition<gsdumpstate>` and :ref:`Common Fields<cfs>` for a description of the
            message structure.

    """

    _tc = MessageTypes.GS_DUMP_STATE

    def __init__(self, tag, filename, _tc=None):
        super().__init__(tag)
        self.filename = filename

    def get_sdict(self):
        rv = super().get_sdict()
        rv['filename'] = self.filename
        return rv


class GSHeadExit(InfraMsg):
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


class GSTeardown(InfraMsg):
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


class GSUnexpected(InfraMsg):
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


class GSChannelRelease(InfraMsg):
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


class GSHalted(InfraMsg):
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


class SHProcessCreate(InfraMsg):
    """
            Refer to :ref:`definition<shprocesscreate>` and :ref:`Common Fields<cfs>` for a description of
            the message structure.

            The initial_stdin is a string which if non-empty is written along with a terminating newline character
            to the stdin of the newly created process.

            The stdin, stdout, and stderr are all either None or an instance of SHChannelCreate to be processed
            by the local services component.

    """

    _tc = MessageTypes.SH_PROCESS_CREATE

    def __init__(self, tag, p_uid, r_c_uid, t_p_uid, exe, args, env=None, rundir='', options=None,
                 initial_stdin='', stdin=None, stdout=None, stderr=None, group=None, user=None,
                 umask=- 1, pipesize=None, stdin_msg=None, stdout_msg=None, stderr_msg=None,
                 pmi_info=None, layout=None, gs_ret_chan_msg=None, _tc=None):
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
            self.pmi_info = PMIProcessInfo.fromdict(pmi_info)
        elif isinstance(pmi_info, PMIProcessInfo):
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

        if gs_ret_chan_msg is None or isinstance(gs_ret_chan_msg, SHChannelCreate):
            self.gs_ret_chan_msg = gs_ret_chan_msg
        else:
            self.gs_ret_chan_msg = SHChannelCreate.from_sdict(gs_ret_chan_msg)

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
        rv['gs_ret_chan_msg'] = (None if self.gs_ret_chan_msg is None else self.gs_ret_chan_msg.get_sdict())
        return rv


class SHProcessCreateResponse(InfraMsg):
    """
            Refer to :ref:`definition<shprocesscreateresponse>` and :ref:`Common Fields<cfs>` for a
            description of the message structure.

    """

    _tc = MessageTypes.SH_PROCESS_CREATE_RESPONSE

    @enum.unique
    class Errors(enum.Enum):
        SUCCESS = 0  #:
        FAIL = 1  #:

    def __init__(self, tag, ref, err, err_info='', stdin_resp=None, stdout_resp=None,
                 stderr_resp=None, gs_ret_chan_resp = None, _tc=None):
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

        if gs_ret_chan_resp is None or isinstance(gs_ret_chan_resp, SHChannelCreateResponse):
            self.gs_ret_chan_resp = gs_ret_chan_resp
        else:
            self.gs_ret_chan_resp = SHChannelCreateResponse.from_sdict(gs_ret_chan_resp)

    def get_sdict(self):
        rv = super().get_sdict()
        if self.Errors.FAIL == self.err:
            rv['err_info'] = self.err_info
        rv['stdin_resp'] = (None if self.stdin_resp is None else self.stdin_resp.get_sdict())
        rv['stdout_resp'] = (None if self.stdout_resp is None else self.stdout_resp.get_sdict())
        rv['stderr_resp'] = (None if self.stderr_resp is None else self.stderr_resp.get_sdict())
        rv['gs_ret_chan_resp'] = (None if self.gs_ret_chan_resp is None else self.gs_ret_chan_resp.get_sdict())
        return rv


class SHProcessKill(InfraMsg):
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


class SHProcessKillResponse(InfraMsg):
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


class SHProcessExit(InfraMsg):
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


class SHMultiProcessCreate(InfraMsg):

    _tc = MessageTypes.SH_MULTI_PROCESS_CREATE

    def __init__(self, tag, r_c_uid, procs : List[Union[Dict, SHProcessCreate]],
                 pmi_group_info : Optional[PMIGroupInfo] = None, _tc=None):
        super().__init__(tag)
        self.r_c_uid = int(r_c_uid)

        if pmi_group_info is None:
            self.pmi_group_info = None
        elif isinstance(pmi_group_info, dict):
            self.pmi_group_info = PMIGroupInfo.fromdict(pmi_group_info)
        elif isinstance(pmi_group_info, PMIGroupInfo):
            self.pmi_group_info = pmi_group_info
        else:
            raise ValueError(f'GS unsupported pmi_group_info value {pmi_group_info=}')

        self.procs = []
        for proc in procs:
            if isinstance(proc, SHProcessCreate):
                self.procs.append(proc)
            elif isinstance(proc, dict):
                self.procs.append(SHProcessCreate.from_sdict(proc))
            else:
                raise ValueError(f'proc is not a supported type %s', type(proc))

    def get_sdict(self):
        rv = super().get_sdict()
        rv['r_c_uid'] = self.r_c_uid
        rv['pmi_group_info'] = None if self.pmi_group_info is None else asdict(self.pmi_group_info)
        rv["procs"] = [proc.get_sdict() for proc in self.procs]
        return rv


class SHMultiProcessCreateResponse(InfraMsg):

    _tc = MessageTypes.SH_MULTI_PROCESS_CREATE_RESPONSE

    @enum.unique
    class Errors(enum.Enum):
        SUCCESS = 0
        FAIL = 1

    def __init__(
        self,
        tag,
        ref,
        err,
        err_info="",
        exit_code=0,
        responses: List[Union[Dict, SHProcessCreateResponse]] = None,
        failed: bool = False,
        _tc=None,
    ):
        super().__init__(tag, ref, err)
        self.err_info = err_info
        self.exit_code = exit_code

        self.failed = failed
        self.responses = []
        for response in responses:
            if isinstance(response, SHProcessCreateResponse):
                self.responses.append(response)
            elif isinstance(response, dict):
                self.responses.append(SHProcessCreateResponse.from_sdict(response))
            else:
                raise ValueError(f'response is not a supported type %s', type(response))

    def get_sdict(self):
        rv = super().get_sdict()
        rv['exit_code'] = self.exit_code

        if self.err == self.Errors.SUCCESS:
            rv['failed'] = self.failed
            rv["responses"] = [response.get_sdict() for response in self.responses]
        elif self.err == self.Errors.FAIL:
            rv['err_info'] = self.err_info
        else:
            raise NotImplementedError('close case')

        return rv


class SHPoolCreate(InfraMsg):
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


class SHPoolCreateResponse(InfraMsg):
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


class SHPoolDestroy(InfraMsg):
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


class SHPoolDestroyResponse(InfraMsg):
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


class SHExecMemRequest(InfraMsg):
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


class SHExecMemResponse(InfraMsg):
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


class SHChannelCreate(InfraMsg):
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


class SHChannelCreateResponse(InfraMsg):
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


class SHChannelDestroy(InfraMsg):
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


class SHChannelDestroyResponse(InfraMsg):
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


class SHLockChannel(InfraMsg):
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


class SHLockChannelResponse(InfraMsg):
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


class SHAllocMsg(InfraMsg):
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


class SHAllocMsgResponse(InfraMsg):
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


class SHAllocBlock(InfraMsg):
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


class SHAllocBlockResponse(InfraMsg):
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


class SHChannelsUp(InfraMsg):
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


class SHPingGS(InfraMsg):
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

class SHTeardown(InfraMsg):
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


class SHPingBE(InfraMsg):
    """
            Refer to :ref:`definition<shpingbe>` and :ref:`Common Fields<cfs>` for a description of the
            message structure.

    """

    _tc = MessageTypes.SH_PING_BE
    EMPTY = b64encode(b'')

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


class SHHaltTA(InfraMsg):
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


class SHHaltBE(InfraMsg):
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


class SHHalted(InfraMsg):
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


class SHFwdInput(InfraMsg):
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


class SHFwdInputErr(InfraMsg):
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


class SHFwdOutput(InfraMsg):
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
        return f'{super().__str__()}, self.data={self.data!r}, self.p_uid={self.p_uid!r}, self.pid={self.pid!r}, self.fd_num={self.fd_num!r}'


class SHDumpState(InfraMsg):
    """
            Refer to :ref:`definition<shdumpstate>` and :ref:`Common Fields<cfs>` for a description of the
            message structure.

    """

    _tc = MessageTypes.SH_DUMP_STATE

    def __init__(self, tag, filename=None, _tc=None):
        super().__init__(tag)
        self.filename = filename

    def get_sdict(self):
        rv = super().get_sdict()
        rv['filename'] = self.filename
        return rv


class BENodeIdxSH(InfraMsg):
    """
            Refer to :ref:`definition<benodeidxsh>` and :ref:`Common Fields<cfs>` for a description of the
            message structure.

    """

    _tc = MessageTypes.BE_NODE_IDX_SH

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


class BEPingSH(InfraMsg):
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


class BEHalted(InfraMsg):
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


class LABroadcast(InfraMsg):
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


class LAPassThruFB(InfraMsg):
    """
            Refer to :ref:`definition<lapassthrufb>` and :ref:`Common Fields<cfs>` for a description of
            the message structure.

    """

    _tc = MessageTypes.LA_PASS_THRU_FB

    def __init__(self, tag, c_uid, data, _tc=None):
        super().__init__(tag)
        self.c_uid = int(c_uid)
        self.data = data

    def get_sdict(self):
        rv = super().get_sdict()
        rv['c_uid'] = self.c_uid
        rv['data'] = self.data
        return rv


class LAPassThruBF(InfraMsg):
    """
            Refer to :ref:`definition<lapassthrubf>` and :ref:`Common Fields<cfs>` for a description of
            the message structure.

    """

    _tc = MessageTypes.LA_PASS_THRU_BF

    def __init__(self, tag, data, _tc=None):
        super().__init__(tag)
        self.data = data

    def get_sdict(self):
        rv = super().get_sdict()
        rv['data'] = self.data
        return rv


class LAServerMode(InfraMsg):
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
class LAServerModeExit(InfraMsg):
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


class LAProcessDict(InfraMsg):
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


class LAProcessDictResponse(InfraMsg):
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


class LADumpState(InfraMsg):
    """
            Refer to :ref:`definition<ladumpstate>` and :ref:`Common Fields<cfs>` for a description of the
            message structure.

    """

    _tc = MessageTypes.LA_DUMP_STATE

    def __init__(self, tag, filename=None, _tc=None):
        super().__init__(tag)
        self.filename = filename

    def get_sdict(self):
        rv = super().get_sdict()
        rv['filename'] = self.filename
        return rv


class LAChannelsInfo(InfraMsg):
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
        try:
            self.fe_ext_ip_addr = get_external_ip_addr()
        except OSError:
            self.fe_ext_ip_addr = None

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


class LoggingMsg(InfraMsg):
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


class LoggingMsgList(InfraMsg):
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


class LogFlushed(InfraMsg):
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


class TAPingSH(InfraMsg):
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


class TAHalted(InfraMsg):
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


class TAUp(InfraMsg):
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


class Breakpoint(InfraMsg):
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


class BEIsUp(InfraMsg):
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


class FENodeIdxBE(InfraMsg):
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


class HaltLoggingInfra(InfraMsg):
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


class HaltOverlay(InfraMsg):
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


class OverlayHalted(InfraMsg):
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


class BEHaltOverlay(InfraMsg):
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


class LAHaltOverlay(InfraMsg):
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


class OverlayPingBE(InfraMsg):
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


class OverlayPingLA(InfraMsg):
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


class LAExit(InfraMsg):
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

class RuntimeDesc(InfraMsg):
    """
            Refer to :ref:`definition<bsinfo>` and :ref:`Common Fields<cfs>` for a description of the
            message structure.
    """
    _tc = MessageTypes.RUNTIME_DESC

    def __init__(self, tag, gs_cd, gs_ret_cd, ls_cd, ls_ret_cd, fe_ext_ip_addr, head_node_ip_addr, oob_port, env, _tc=None):
        super().__init__(tag)
        self.gs_cd = gs_cd
        self.gs_ret_cd = gs_ret_cd
        self.ls_cd = ls_cd
        self.ls_ret_cd = ls_ret_cd
        # should we add "username" to the sdesc?
        self.fe_ext_ip_addr = fe_ext_ip_addr
        self.head_node_ip_addr = head_node_ip_addr
        self.oob_port = oob_port
        self.env = json.dumps(dict(env))
        # add something to help deal with differences in dir structure?

    def get_sdict(self):
        rv = super().get_sdict()
        rv['gs_cd'] = self.gs_cd
        rv['gs_ret_cd'] = self.gs_ret_cd
        rv['ls_cd'] = self.ls_cd
        rv['ls_ret_cd'] = self.ls_ret_cd
        rv['fe_ext_ip_addr'] = self.fe_ext_ip_addr
        rv['head_node_ip_addr'] = self.head_node_ip_addr
        rv['oob_port'] = self.oob_port
        rv['env'] = json.loads(self.env)
        return rv

class UserHaltOOB(InfraMsg):
    """
            Refer to :ref:`definition<shhaltta>` and :ref:`Common Fields<cfs>` for a description of the
            message structure.

    """

    _tc = MessageTypes.USER_HALT_OOB

    def __init__(self, tag, _tc=None):
        super().__init__(tag)

    def get_sdict(self):
        rv = super().get_sdict()
        return rv

class TAUpdateNodes(InfraMsg):
    """
            Refer to :ref:`definition<taupdatenodes>` and :ref:`Common Fields<cfs>` for a description of the
            message structure.

    """
    _tc = MessageTypes.TA_UPDATE_NODES

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


PREDETERMINED_CAPS = {'GS':'GS', 'SH':'SH', 'TA':'TA', 'BE':'BE', 'FE':'FE', 'LA':'LA', 'BF':'BF',
                      'FB':'FB', 'DD':'DD', 'SENDH':'SendH', 'RECVH':'RecvH', 'CPU': 'CPU', 'ID':'ID',
                      'OOB':'OOB', 'KVL':'KVL', 'KV':'KV'}

MSG_TYPES_WITHOUT_CLASSES = {MessageTypes.DRAGON_MSG}

def type_filter(the_msg_types):
    msg_types = set(the_msg_types) - MSG_TYPES_WITHOUT_CLASSES
    return msg_types

def camel_case_msg_name(msg_id):

    lst = msg_id.split('.')[1].split('_')
    cased = []

    for word in lst:
        if word in PREDETERMINED_CAPS:
            cased.append(PREDETERMINED_CAPS[word])
        else:
            cased.append(word[0].upper()+word[1:].lower())

    converted = "".join(cased)
    return converted

def mk_all_message_classes_set():
    result = set()
    for msg_id in type_filter(MessageTypes):
        try:
            class_name = camel_case_msg_name(str(msg_id))
            class_def = getattr(sys.modules[__name__],class_name)
            result.add(class_def)
        except:
            raise TypeError(f'Unable to find corresponding class {class_name} for message id {msg_id}.')

    return result

all_message_classes = mk_all_message_classes_set()

mt_dispatch = {cls._tc.value: cls for cls in all_message_classes}

def parse(serialized, restrict=None):
    try:
        # if a compressed message, decompress to get the service message
        try:
            decoded = b64decode(serialized)
        except:
            decoded = serialized

        try:
            jstring = zlib.decompress(decoded)
        except zlib.error:
            jstring = decoded


        sdict = json.loads(jstring)
        typecode = sdict['_tc']
        if restrict:
            assert typecode in restrict

        return mt_dispatch[typecode].from_sdict(sdict)


    except Exception as json_exception:
        try:
            # A DecodeError probaby indicates this is a CapnProto message so we'll
            # try parsing it that way before returning

            msg = CapNProtoMsg.deserialize(serialized)

            if restrict:
                assert msg.tc in restrict

            return msg
        except Exception as ex:
            tb = traceback.format_exc()
            raise TypeError(f'The message "{serialized}" could not be parsed.\nJSON Parsing Error Message:{json_exception}\nCapnProto Parsing Error Message:{ex}\n Traceback {tb}')
