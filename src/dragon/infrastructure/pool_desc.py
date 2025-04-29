"""A class that uniquely defines properties of a managed memory pool for infrastructure communication."""

import enum
from ..infrastructure import parameters as dparm
from .. import utils as du


class PoolDescriptor:
    """Globally available descriptior of a pool.

    Attributes:

        m_uid : nonnegative int
            unique memory pool id

        name : str
            string indicating pool name that servers as an identifier
            to query for the pool

        sdesc : bytes-like object
            serialized library descriptor

        node : nonnegative int
            which node this memory pool is associated with

        state : cls.State enum member
            current state of this pool
    """

    @enum.unique
    class State(enum.Enum):
        """Current state of the pool."""

        PENDING = enum.auto()
        ACTIVE = enum.auto()
        DEAD = enum.auto()

    def __init__(self, m_uid=0, name="", node=0, state=None, sdesc=""):
        """
        sdesc - serialized library descriptor
        """
        self.m_uid = int(m_uid)
        self.name = name
        self.sdesc = sdesc
        self.node = int(node)

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
        """Returns the serialized library descriptor of the pool."""
        return self._sdesc

    @sdesc.setter
    def sdesc(self, value):
        """Populate the serialized library descriptor of the pool.

        :param value: value to be populated as the serialzied descriptor
        :type value: either bytes (or) string passed through the environment
        """
        if isinstance(value, str):
            self._sdesc = du.B64.str_to_bytes(value)
        else:
            self._sdesc = value

    def get_sdict(self):
        """Collect the entire information of the pool.

        :return: A dictionary with all key-value pairs of the available description of the pool.
        :rtype: Dictionary
        """
        rv = {
            "sdesc": du.B64.bytes_to_str(self.sdesc),
            "name": self.name,
            "m_uid": self.m_uid,
            "node": self.node,
            "state": self.state.value,
        }
        return rv

    def __str__(self):
        return f"{self.m_uid}:{self.name} on {self.node}: {self.state.name}"

    @classmethod
    def from_sdict(cls, sdict):
        """Returns the descriptor of the memory pool."""
        return PoolDescriptor(**sdict)


class PoolOptions:
    """Options object for how a Pool is supposed to be created."""

    class Placement(enum.Enum):
        """Enum for placement strategy of the pool for its creation."""

        LOCAL = 0
        DEFAULT = 1
        SPECIFIC = 2

    def __init__(self, placement=0, target_node=0, sattr=""):
        """
        sattr - serialized library attributes

        placement - enum for placement strategy

        target_node - ignored if placement request isn't for a specific node
        """
        self.placement = self.Placement(placement)
        self.target_node = int(target_node)
        self.sattr = sattr

    def get_sdict(self):
        """Collect the different attributes/options of the memory pool.

        :return: A dictionary with key-value pairs of the pool creation options.
        :rtype: Dictionary
        """
        rv = {"sattr": self.sattr, "target_node": self.target_node, "placement": self.placement.value}

        return rv

    @staticmethod
    def from_sdict(sdict):
        """Returns the options of the memory pool."""
        return PoolOptions(**sdict)
