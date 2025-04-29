"""Node related objects internal to Global Services."""

from ..infrastructure.node_desc import NodeDescriptor

import logging

LOG = logging.getLogger(__name__)


# NOTE: This is a stub really and will need to be extended once we are
# targetting cloud hardware.
class NodeContext:
    """Everything to do with a single node in global services.

    In contrast to other resources handled by GS, the NodeContext
    gets its descriptors from the Shepherd started on the node via
    SHPingGS.

    This class includes the code that does the lifecycle of the
    node to the launcher controlling that channel.
    """

    def __init__(self, desc, ls_index, h_uid, name):

        self.h_uid = h_uid
        self.name = name
        self.ls_index = ls_index
        self._descriptor = desc
        self._descriptor.h_uid = h_uid

    def __str__(self):
        return f"[[{self.__class__.__name__}]] desc:{self.descriptor!r} idx:{self.ls_index!r}"

    @property
    def descriptor(self):
        return self._descriptor

    @classmethod
    def construct(cls, msg):
        """Registers a new node with Global Services, by constructing the appropriate
        context from the message.

        :param msg: Message send to GS to create the node
        :type msg: class SHPingGS
        :return: The new node context
        :rtype: class NodeContext
        """

        ls_index = msg.idx  # index of the node according to Local Services
        desc = NodeDescriptor.from_sdict(msg.node_sdesc)

        h_uid = desc.host_id

        name = desc.name

        context = cls(desc, ls_index, h_uid, name)

        return context
