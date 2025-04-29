"""A class that uniquely defines properties of a managed Process for infrastructure communication."""

import enum
from dragon.infrastructure.policy import Policy


@enum.unique
class ArgMode(enum.Enum):
    """
    Enum for mode of delivering the arguments, either process will be using the channel (or)
    to deliver the argdata directly
    """

    NONE = 0  # no args
    PYTHON_IMMEDIATE = 1  # args are found in GSPingProc argdata members in pickled form
    PYTHON_CHANNEL = 2  # argdata in GSPingProc is a recipe for a channel descriptor, attach and read from it


def mk_argmode_from_default(mode):
    """Populate and return the mode of arguments as per the ArgMode

    :return: Mode of delivering the arguments for the launching process
    :rtype: ArgMode Enum
    """
    if isinstance(mode, ArgMode):
        mode = mode
    elif mode is None:
        mode = ArgMode.NONE
    elif isinstance(mode, int):
        mode = ArgMode(mode)
    else:
        raise NotImplementedError("unknown mode initializer")

    return mode


class ProcessDescriptor:
    """Globally available description of a managed process.

    Attributes:

        ecode : int or None
            exit code of the process, or None if still running

        node : nonnegative int
            which node of the allocation this process is/was on

        name : str
            user assigned unique name of the process

        policy : Policy
            Dragon policy object

        p_uid : nonnegative int
            unique process id

        p_p_uid : nonnegative int
            parent process unique id

        live_children : set of nonnegative int

        gs_ret_cuid : nonnegative int
            c_uid of the global services return channel

        shep_ret_cuid : nonnegative int
            c_uid of the local shepherd return channel (currently not used)

        state : ProcessDescriptor.State enum member
            current run state of this process

        stdin_sdesc : a serialized channel where the stdin of the process can be provided.

        stdout_sdesc : a serialized channel where the stdout of the process is written.

        stderr_sdesc : a serialized channel where the stderr of the process is written.

    """

    @enum.unique
    class State(enum.Enum):
        """Current state of the process."""

        PENDING = enum.auto()
        ACTIVE = enum.auto()
        DEAD = enum.auto()

    def __init__(
        self,
        *,
        p_uid,
        p_p_uid,
        name,
        node,
        policy=None,
        h_uid=None,
        live_children=None,
        gs_ret_cuid=None,
        shep_ret_cuid=None,
        state=None,
        ecode=None,
        stdin_sdesc=None,
        stdout_sdesc=None,
        stderr_sdesc=None,
    ):
        self.p_p_uid = p_p_uid
        self.ecode = ecode
        self.node = int(node)
        self.h_uid = h_uid
        self.name = name
        self.policy = None
        self.p_uid = int(p_uid)
        self.gs_ret_cuid = gs_ret_cuid
        self.shep_ret_cuid = shep_ret_cuid
        self.stdin_sdesc = stdin_sdesc
        self.stdout_sdesc = stdout_sdesc
        self.stderr_sdesc = stderr_sdesc
        self.stdin_conn = None
        self.stdout_conn = None
        self.stderr_conn = None

        if isinstance(policy, Policy):
            self.policy = policy
        elif isinstance(policy, dict):
            self.policy = Policy.from_sdict(policy)

        if live_children is None:
            self.live_children = set()
        else:
            self.live_children = set(live_children)

        if state is None:
            self.state = self.State.PENDING
        else:
            if isinstance(state, self.State):
                self.state = state
            elif isinstance(state, int):
                self.state = self.State(state)
            else:
                raise NotImplementedError("unknown state init")

    def __str__(self):
        rv = f"{self.p_uid}:{self.name} on {self.node}: {self.state.name}"

        if self.state is self.State.DEAD:
            rv += f" exit {self.ecode}"

        return rv

    def get_sdict(self):
        """Collect the entire information of the process.

        :return: A dictionary with all key-value pairs of the available information of the process.
        :rtype: Dictionary
        """
        rv = {
            "node": self.node,
            "name": self.name,
            "p_uid": self.p_uid,
            "p_p_uid": self.p_p_uid,
            "live_children": list(self.live_children),
            "state": self.state.value,
        }

        if self.policy:
            rv["policy"] = self.policy.get_sdict()
        else:
            rv["policy"] = None

        if self.state == self.State.DEAD:
            rv["ecode"] = self.ecode

        if self.gs_ret_cuid is not None:
            rv["gs_ret_cuid"] = self.gs_ret_cuid

        if self.h_uid is not None:
            rv["h_uid"] = self.h_uid

        if self.shep_ret_cuid is not None:
            rv["shep_ret_cuid"] = self.shep_ret_cuid

        rv["stdin_sdesc"] = self.stdin_sdesc
        rv["stdout_sdesc"] = self.stdout_sdesc
        rv["stderr_sdesc"] = self.stderr_sdesc

        return rv

    @classmethod
    def from_sdict(cls, sdict):
        """Returns the descriptor of the launched process."""
        return ProcessDescriptor(**sdict)


# TODO: document what these options actually mean !
class ProcessOptions:
    """Open type, placeholder, for options on how to launch a process

    Attributes:

    argdata
        argument data, bytes to deliver to the launching process

    mode
        mode of delivering the arguments, either process will be using the channel (or)
        to deliver the argdata directly

    make_inf_channels
        bool, default False, whether process should start making the infrastructure channels
        when they are needed, or just start the process directly.
    """

    def __init__(self, make_inf_channels=False, mode=None, argdata=None):
        self.argdata = argdata
        self.mode = mk_argmode_from_default(mode)
        self.make_inf_channels = make_inf_channels

    def get_sdict(self):
        """Collect the different attributes/options of launching the process.

        :return: A dictionary with key-value pairs of the process options.
        :rtype: Dictionary
        """
        return {"mode": self.mode.value, "argdata": self.argdata, "make_inf_channels": self.make_inf_channels}

    @staticmethod
    def from_sdict(sdict):
        """Returns the options of the launched process."""
        return ProcessOptions(**sdict)
