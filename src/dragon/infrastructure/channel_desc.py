"""A class that uniquely defines  properties of a managed channel for infrastructure communication."""

import enum

import dragon.infrastructure.parameters as dparm
import dragon.localservices.options as dso
import dragon.utils as du


class ChannelDescriptor:
    """Globally available description of a managed channel.

    Attributes:

        node : nonnegative int
            which node this channel

        name : str
            user assigned unique name of the process

        c_uid : nonnegative int
            unique channel id

        m_uid : nonnegative int
            memory pool containing this channel

        sdesc : bytes-like object
            serialized descriptor from the Channels library

        state : cls.State enum member
            current run state of this process
    """

    @enum.unique
    class State(enum.Enum):
        """Current state of the channel."""

        PENDING = enum.auto()
        ACTIVE = enum.auto()
        DEAD = enum.auto()

    def __init__(self, m_uid, c_uid, name, node, state=None, sdesc=""):
        """
        sdesc - serialized library descriptor
        """
        self.m_uid = m_uid
        self.c_uid = c_uid
        self.name = name
        self.sdesc = sdesc
        self.node = node

        if state is None:
            self.state = self.State.PENDING
        else:
            if isinstance(state, self.State):
                self.state = state
            elif isinstance(state, int):
                self.state = self.State(state)
            else:
                raise NotImplementedError("unknown state init")

    @property
    def sdesc(self):
        """Returns the serialized library descriptor of the channel."""
        return self._sdesc

    @sdesc.setter
    def sdesc(self, value):
        """Populate the serialized library descriptor of the channel.

        :param value: value to be populated as the serialzied descriptor
        :type value: either bytes (or) string passed through the environment
        """
        if isinstance(value, str):
            self._sdesc = du.B64.str_to_bytes(value)
        else:
            self._sdesc = value

    def __str__(self):
        return f"{self.name}:{self.c_uid} {self.state.name} on node {self.node}"

    def get_sdict(self):
        """Collect the entire information of the channel.

        :return: A dictionary with all key-value pairs of the available description of the channel.
        :rtype: Dictionary
        """
        rv = {
            "sdesc": du.B64.bytes_to_str(self.sdesc),
            "name": self.name,
            "m_uid": self.m_uid,
            "c_uid": self.c_uid,
            "node": self.node,
            "state": self.state.value,
        }

        return rv

    @classmethod
    def from_sdict(cls, sdict):
        """Returns the descriptor of the managed channel."""
        return ChannelDescriptor(**sdict)


class ChannelOptions:
    """Open type, placeholder, for options on how to make a channel

    Attributes:

    local_opts
        local channel creation options, such as block size and capacity.
        see dragon.localservices.options.ChannelOptions

    ref_count
        bool, default False, whether GS should count references on this
        channel and destroy if count reaches zero

        NOTE: these options will be growing and the scheme needs
        thought for organization.
    """

    def __init__(self, *, local_opts=None, ref_count=False):
        self.ref_count = ref_count
        if local_opts is None:
            self.local_opts = dso.ChannelOptions()
        elif isinstance(local_opts, dso.ChannelOptions):
            self.local_opts = local_opts
        else:
            self.local_opts = dso.ChannelOptions(**local_opts)

    def get_sdict(self):
        """Collect the different attributes/options of the channel.

        :return: A dictionary with key-value pairs of the channel options.
        :rtype: Dictionary
        """
        return {"local_opts": self.local_opts.get_sdict(), "ref_count": self.ref_count}

    @staticmethod
    def from_sdict(sdict):
        """Returns the options of the managed channel."""
        return ChannelOptions(**sdict)
