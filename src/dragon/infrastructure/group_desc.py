"""A class that uniquely defines properties of a managed Group of resources for infrastructure communication."""

import enum
from dataclasses import dataclass, field
from ..infrastructure import parameters as dparm
from ..infrastructure.process_desc import ProcessDescriptor as process_desc
from ..infrastructure.channel_desc import ChannelDescriptor as channel_desc
from ..infrastructure.pool_desc import PoolDescriptor as pool_desc
from ..infrastructure.node_desc import NodeDescriptor as node_desc
from ..globalservices.policy_eval import Policy


@dataclass
class GroupDescriptor:
    """The group descriptor holds a list of tuples of puids, muids, cuids or huids."""

    @enum.unique
    class State(enum.IntEnum):
        """Current state of a group's member."""

        PENDING = 0
        ACTIVE = 1
        DEAD = 2

    @dataclass
    class GroupMember:
        """The GroupMember class holds information about each object that is a member of the group."""

        @enum.unique
        class Errors(enum.Enum):
            SUCCESS = 0  #: Resource was created
            FAIL = 1  #: Resource was not created
            ALREADY = 2  #: Resource exists already

        state: object = None
        uid: int = None
        placement: int = None
        desc: object = None
        error_code: int = None
        error_info: str = None

        @classmethod
        def from_sdict(cls, d):
            """Returns the launched group member object.

            :param d: dictionary with all key-value pairs of the available information of the group's member
            :type d: Dictionary
            :raises ValueError: when there is an error during deserialization
            :return: the GroupMember object
            :rtype: GroupMember
            """
            try:
                return cls(**d)
            except Exception as exc:
                raise ValueError(f"Error deserializing {cls.__name__} {d=}") from exc

        def get_sdict(self):
            """Collects the entire information of a member of the group.

            :return: A dictionary with all key-value pairs of the available information of the group's member.
            :rtype: Dictionary
            """
            rv = {"uid": self.uid, "placement": self.placement}

            if isinstance(self.state, int):
                rv["state"] = self.state
            else:
                rv["state"] = self.state.value

            rv["desc"] = self.desc.get_sdict()

            if isinstance(self.error_code, int):
                rv["error_code"] = self.error_code
            else:
                rv["error_code"] = self.error_code.value

            rv["error_info"] = self.error_info

            return rv

    # GroupDescriptor attributes
    state: State = State.PENDING
    g_uid: int = None
    name: str = None
    sets: list(list()) = field(default_factory=list)
    policy: Policy = None
    resilient: bool = False

    def __post_init__(self):
        if type(self.policy) is dict:
            self.policy = Policy.from_sdict(self.policy)

        if type(self.policy) is list:
            modded_args = [(i, Policy.from_sdict(p)) for i, p in enumerate(self.policy) if isinstance(p, dict)]
            for i, p in modded_args:
                self.policy[i] = p

        if self.sets:
            old_sets, self.sets = self.sets, []
            for i, lst in enumerate(old_sets):
                self.sets.append([])
                for j, member in enumerate(lst):
                    self.sets[i] += [
                        (
                            member
                            if type(member) is GroupDescriptor.GroupMember
                            else GroupDescriptor.GroupMember.from_sdict(member)
                        )
                    ]
                    if type(self.sets[i][j].desc) is dict:
                        # self.sets[i][j].desc is a dictionary and we need to call from_sdict() on it
                        # to convert it to the appropriate Descriptor object
                        # figure out what type of resource object this member is
                        if "c_uid" in self.sets[i][j].desc.keys():
                            self.sets[i][j].desc = channel_desc.from_sdict(self.sets[i][j].desc)
                        elif "m_uid" in self.sets[i][j].desc.keys():
                            self.sets[i][j].desc = pool_desc.from_sdict(self.sets[i][j].desc)
                        elif "host_name" in self.sets[i][j].desc.keys():
                            self.sets[i][j].desc = node_desc.from_sdict(self.sets[i][j].desc)
                        else:
                            self.sets[i][j].desc = process_desc.from_sdict(self.sets[i][j].desc)

    @classmethod
    def from_sdict(cls, d):
        """Returns the descriptor of the launched group of resources.

        :param d: dictionary with all key-value pairs of the available information of the group
        :type d: Dictionary
        :raises ValueError: when there is an error during deserialization
        :return: the GroupDescriptor object
        :rtype: GroupDescriptor
        """

        try:
            return cls(**d)
        except Exception as exc:
            raise ValueError(f"Error deserializing {cls.__name__} {d=}") from exc

    def get_sdict(self):
        """Collects the entire information of the group.

        :return: A dictionary with all key-value pairs of the available information of the group.
        :rtype: Dictionary
        """
        rv = {"name": self.name, "g_uid": self.g_uid, "state": self.state, "resilient": self.resilient}

        if isinstance(self.policy, Policy):
            rv["policy"] = self.policy.get_sdict()
        elif isinstance(self.policy, list):
            rv["policy"] = [policy.get_sdict() for policy in self.policy]
        else:
            rv["policy"] = self.policy

        if self.sets is not None:
            rv["sets"] = []
            for i, tup in enumerate(self.sets):
                rv["sets"].append(())
                for member in tup:
                    rv["sets"][i] += (member.get_sdict(),)

        return rv
