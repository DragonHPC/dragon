import sys
import enum
import json
import zlib
import traceback

from base64 import b64encode, b64decode

import logging

logger = logging.getLogger(__name__)


@enum.unique
class MessageTypes(enum.Enum):
    """
    These are the enumerated values of message type identifiers within
    the Dragon infrastructure messages.
    """

    INVALID = 0  #: Deliberately invalid
    HELLO_SYN = enum.auto()
    HELLO_SYN_ACK = enum.auto()
    HELLO_ACK = enum.auto()
    CREATE_TREE = enum.auto()
    ABNORMAL_RUNTIME_EXIT = enum.auto()
    BACKEND_UP = enum.auto()
    RUN_USER_APP = enum.auto()
    FWD_OUTPUT = enum.auto()
    PING = enum.auto()
    PING_RESPONSE = enum.auto()
    USER_APP_EXIT = enum.auto()
    DESTROY_TREE = enum.auto()
    TREE_DESTROYED = enum.auto()


class InfraMsg(object):
    """Common base for all messages.

    This common base type for all messages sets up the
    default fields and the serialization strategy for
    now.
    """

    _tc = MessageTypes.INVALID  # deliberately invalid value, overridden

    @enum.unique
    class Errors(enum.Enum):
        INVALID = -1  # deliberately invalid, overridden

    def __init__(self, tag, ref=None, err=None, _tc=None):
        assert isinstance(tag, int)

        # logger.debug(f'InfraMsg __init__ {tag=} {ref=} {_tc=}')

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
                raise NotImplementedError("invalid error parameter")
        else:
            self._err = err

    def get_sdict(self):

        rv = {"_tc": self._tc.value, "tag": self.tag}

        if self.err is not None:
            rv["err"] = self.err.value

        if self.ref is not None:
            assert isinstance(self.ref, int)
            rv["ref"] = self.ref

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
        return json.dumps(self.get_sdict()).encode("utf-8")

    def serialize(self):
        return b64encode(zlib.compress(json.dumps(self.get_sdict()).encode("utf-8")))

    def __str__(self):
        cn = self.__class__.__name__
        msg = f"{cn}: {self.tag}"
        if self._err:
            msg += f" {self._err=}"
        if self._ref:
            msg += f" {self._ref=}"
        return msg

    def __repr__(self):
        fields_to_set = self.get_sdict()
        del fields_to_set["_tc"]
        fs = ", ".join([f"{k!s}={v!r}" for k, v in fields_to_set.items()])
        return f"{self.__class__.__name__}({fs})"


class HelloSyn(InfraMsg):
    _tc = MessageTypes.HELLO_SYN


class HelloSynAck(InfraMsg):
    _tc = MessageTypes.HELLO_SYN_ACK


class HelloAck(InfraMsg):
    _tc = MessageTypes.HELLO_ACK


class CreateTree(InfraMsg):
    _tc = MessageTypes.CREATE_TREE

    def __init__(self, tag, children, fanout, _tc=None):
        super().__init__(tag, _tc)
        self.children = children
        self.fanout = fanout

    def get_sdict(self):
        rv = super().get_sdict()
        rv["children"] = self.children
        rv["fanout"] = self.fanout
        return rv

    def __str__(self):
        return f"{super().__str__()}, self.children={self.children!r}"


class AbnormalRuntimeExit(InfraMsg):
    _tc = MessageTypes.ABNORMAL_RUNTIME_EXIT

    def __init__(self, tag, hostname, data, _tc=None):
        super().__init__(tag, _tc)

        self.hostname = hostname
        self.data = data

    def get_sdict(self):
        rv = super().get_sdict()
        rv["hostname"] = self.hostname
        rv["data"] = self.data
        return rv

    def __str__(self):
        return f"{super().__str__()}, self.hostname={self.hostname!r} self.data={self.data!r}"

class BackendUp(InfraMsg):
    _tc = MessageTypes.BACKEND_UP

    def __init__(self, tag, hostname, _tc=None):
        super().__init__(tag, _tc)

        self.hostname = hostname

    def get_sdict(self):
        rv = super().get_sdict()
        rv["hostname"] = self.hostname
        return rv

    def __str__(self):
        return f"{super().__str__()}, self.hostname={self.hostname!r}"

class RunUserApp(InfraMsg):
    _tc = MessageTypes.RUN_USER_APP

    def __init__(self, tag, command, num_ranks, env=None, cwd=None, _tc=None):
        super().__init__(tag, _tc)
        self.command = command
        self.num_ranks = num_ranks
        self.env = env
        self.cwd = cwd

    def get_sdict(self):
        rv = super().get_sdict()
        rv["command"] = self.command
        rv["num_ranks"] = self.num_ranks

        if self.env:
            rv["env"] = self.env

        if self.cwd:
            rv["cwd"] = self.cwd

        return rv

    def __str__(self):
        return f"{super().__str__()}, self.command={self.command!r} {self.env=} {self.cwd=}"


class FwdOutput(InfraMsg):
    _tc = MessageTypes.FWD_OUTPUT

    MAX = 1024

    @enum.unique
    class FDNum(enum.Enum):
        STDOUT = 1
        STDERR = 2

    def __init__(self, tag, fd_num, data, hostname="NONE", _tc=None):
        super().__init__(tag, _tc)
        if len(data) > self.MAX:
            raise ValueError(f"output data limited to {self.MAX} bytes")

        self.data = data
        assert fd_num in {self.FDNum.STDOUT.value, self.FDNum.STDERR.value}
        self.fd_num = fd_num
        self.hostname = hostname

    def get_sdict(self):
        rv = super().get_sdict()
        rv["data"] = self.data
        rv["fd_num"] = self.fd_num
        rv["hostname"] = self.hostname
        return rv

    def __str__(self):
        return f"{super().__str__()}, self.hostname={self.hostname!r}, self.data={self.data!r}, self.fd_num={self.fd_num!r}"


class Ping(InfraMsg):
    _tc = MessageTypes.PING


class PingResponse(InfraMsg):
    _tc = MessageTypes.PING_RESPONSE

    @enum.unique
    class Errors(enum.Enum): # type: ignore
        OK = 0
        ERROR = 1

    def __init__(self, tag, ref, err, hostname, _tc=None):
        super().__init__(tag, ref, err, _tc)
        self.hostname = hostname

    def get_sdict(self):
        rv = super().get_sdict()
        rv["hostname"] = self.hostname
        return rv

    def __str__(self):
        return f"{super().__str__()}, self.hostname={self.hostname!r}"


class UserAppExit(InfraMsg):
    _tc = MessageTypes.USER_APP_EXIT

    def __init__(self, tag, ref, hostname, exit_code, _tc=None):
        super().__init__(tag, ref, _tc)

        self.hostname = hostname
        self.exit_code = exit_code

    def get_sdict(self):
        rv = super().get_sdict()
        rv["hostname"] = self.hostname
        rv["exit_code"] = self.exit_code
        return rv

    def __str__(self):
        return f"{super().__str__()}, self.hostname={self.hostname!r}, self.exit_code={self.exit_code}"


class DestroyTree(InfraMsg):
    _tc = MessageTypes.DESTROY_TREE


class TreeDestroyed(InfraMsg):
    _tc = MessageTypes.TREE_DESTROYED


PREDETERMINED_CAPS = {}
MSG_TYPES_WITHOUT_CLASSES = {MessageTypes.INVALID}


def camel_case_msg_name(msg_id):

    lst = msg_id.split(".")[1].split("_")
    cased = []

    for word in lst:
        if word in PREDETERMINED_CAPS:
            cased.append(PREDETERMINED_CAPS[word])
        else:
            cased.append(word[0].upper() + word[1:].lower())

    converted = "".join(cased)
    return converted


def mk_all_message_classes_set():
    result = set()
    for msg_id in type_filter(MessageTypes):
        class_name = camel_case_msg_name(str(msg_id))
        try:
            class_def = getattr(sys.modules[__name__], class_name)
            result.add(class_def)
        except Exception:
            raise TypeError(f"Unable to find corresponding class {class_name} for message id {msg_id}.")

    return result


def type_filter(the_msg_types):
    msg_types = set(the_msg_types) - MSG_TYPES_WITHOUT_CLASSES
    return msg_types


all_message_classes = mk_all_message_classes_set()
mt_dispatch = {cls._tc.value: cls for cls in all_message_classes}


def parse(serialized, restrict=None):
    try:
        # if a compressed message, decompress to get the service message
        try:
            decoded = b64decode(serialized, validate=True)
        except Exception:
            decoded = serialized.encode(encoding="utf-8")

        try:
            jstring = zlib.decompress(decoded)
        except zlib.error:
            jstring = decoded

        sdict = json.loads(jstring)
        typecode = sdict["_tc"]
        if restrict:
            assert typecode in restrict

        del sdict["_tc"]

        return mt_dispatch[typecode].from_sdict(sdict)

    except Exception as json_exception:
        tb = traceback.format_exc()
        print(f"{tb=}", flush=True)
        raise TypeError(
            f'The message "{serialized}" could not be parsed.\nJSON Parsing Error Message:{json_exception}\n'
        ) from json_exception
