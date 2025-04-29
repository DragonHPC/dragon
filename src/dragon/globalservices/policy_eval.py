import enum
from dataclasses import dataclass, field, fields
from ..infrastructure.policy import Policy
from ..infrastructure.node_desc import NodeDescriptor
import logging
import warnings

LOG = logging.getLogger("policy_eval:")


@dataclass
class ResourceLayout:
    h_uid: int
    host_name: str
    numa_node: int  # TODO
    cpu_core: [int]  # List of acceptable CPU cores the layout can be applied to
    gpu_core: [int]
    accelerator_env: str


# Possible TODO: Affinity offset or placement offset, to "start counting from here", just make sure it starts at a certain place
class PolicyEvaluator:
    """
    Based on a list of NodeDescriptors, evaluate policies and apply them
    """

    def __init__(self, nodes: list[NodeDescriptor], default_policy=None):
        self.nodes = nodes
        self.num_nodes = len(nodes)
        self.default_policy = (
            default_policy  # This will be used to fill in any Policy values that are DEFAULT when evaluated
        )
        self.overprovision = False  # Should make this into a parameter flag to allow/disallow it?
        self.cur_node = 0

    def _find_next_open(self, cur_idx):
        """
        Find next node with an open policy slot
        If no nodes are available, return None to indicate overprovisioning mode

        :param cur_idx: Current index in the list of nodes
        """

        idx = (cur_idx + 1) % self.num_nodes
        node = None
        if self.num_nodes == 1:
            return node  # Indicate we are now overprovisioning mode, we only have one node

        # Loop through nodes until we hit our starting point
        while idx != cur_idx:
            next_node = self.nodes[idx]
            if next_node.num_policies < next_node.num_cpus:
                node = next_node
                break

            idx = (idx + 1) % self.num_nodes

        return node

    def _add_layout(self, node, cpu_affinity, gpu_affinity, env_str, layouts):
        """
        Short helper function to auto-increment num_policies value on nodes

        :param node: Node we're generating the layout for
        :param affinity: List of CPU devices local services can apply to
        :param layouts: List of layouts to append to

        """
        # NOTE: Numa node and accelerator are placeholders
        numa_node = 0
        layouts.append(ResourceLayout(node.h_uid, node.host_name, numa_node, cpu_affinity, gpu_affinity, env_str))
        node.num_policies += 1

    def _node_by_id(self, host_id) -> NodeDescriptor:
        """
        Try to find a node with a given host ID
        Raise exception if specified host is not in the list
        """
        node = None
        for n in self.nodes:
            if n.h_uid == host_id:
                node = n
                break

        if node is None:
            raise ValueError(f"Could not find host with specified ID {host_id} in policy")
        return node

    def _node_by_name(self, host_name) -> NodeDescriptor:
        """
        Try to find a node with a given host name
        Raise exception if specified host is not in the list
        """
        node = None
        for n in self.nodes:
            if n.host_name == host_name:
                node = n
                break

        if node is None:
            raise ValueError(f"Could not find host with specified name {host_name} in policy")

        return node

    def _get_node(self, p: Policy) -> NodeDescriptor:
        """
        Find the next available node based on the provided policy
        """

        # Reset to the first node if we've cycled all the way through
        if self.cur_node >= self.num_nodes:
            self.cur_node = 0

        # If placement is specified to a host ID or name, apply to that node no questions asked
        if p.placement == Policy.Placement.HOST_ID:
            return self._node_by_id(p.host_id)
        elif p.placement == Policy.Placement.HOST_NAME:
            return self._node_by_name(p.host_name)

        else:
            # TODO: Other placements not used in any meaningful way yet
            # Default to whatever the next node based on the distribution policy is
            # if p.placement == default, local, anywhere ...
            node = self.nodes[self.cur_node]

        # Default to roundrobin for now
        distribution = p.distribution  # Use a temp value instead of modifying the policy
        if distribution == Policy.Distribution.DEFAULT:
            distribution = Policy.Distribution.ROUNDROBIN

        # RoundRobin defaults to always overprovisioning, it will always just assign a policy to "next node"
        # TODO: Mimic some overprovisioning logic like in BLOCK for roundrobin to find the next empty slot?
        if distribution == Policy.Distribution.ROUNDROBIN:
            self.cur_node += 1
            return node

        elif distribution == Policy.Distribution.BLOCK:
            if self.overprovision:
                self.cur_node += 1  # Basically means we're in round-robin

            elif node.num_policies >= node.num_cpus:
                next_node = self._find_next_open(self.cur_node)  # Find next open node
                if next_node is None:  # All nodes full
                    self.overprovision = True  # Start overprovisioning
                    next_node = node  # Reassign next_node from None to current node
                    self.cur_node += 1  # Move to next node in cycle (round robin, essentially)
                node = next_node  # Assign current node to "next node" (either open node, or same node to begin with)

            return node

    def _get_cpu_affinity(self, p: Policy, node: NodeDescriptor) -> list[int]:
        """
        Generate a list of available devices the policy can be applied to for the given Node
        """

        if p.cpu_affinity:  # List not empty, assume SPECIFIC affinity
            assert isinstance(p.cpu_affinity, list)
            affinity = [x for x in node.cpu_devices if x in p.cpu_affinity]
            return affinity  # This covers both "ANY" and "SPECIFIC" if a specific list is given
        elif node.cpu_devices is not None:
            return node.cpu_devices

        return []

    def _get_gpu_affinity(self, p: Policy, node: NodeDescriptor) -> list[int]:

        if p.gpu_affinity:
            assert isinstance(p.gpu_affinity, list)
            if node.accelerators is not None:
                affinity = [x for x in p.gpu_affinity if x in node.accelerators.device_list]
                return affinity
            else:
                LOG.warning("GPU affinity provided when no GPUs are available on the node")
                return []
        elif node.accelerators is not None:
            return node.accelerators.device_list

        return []

    def evaluate(self, policies: list[Policy] = None) -> list[ResourceLayout]:
        """
        Evaluate a list of policies and return layouts of what nodes to apply them to
        """

        layouts = []

        # Iterate over policies and apply them through node descriptors
        for p in policies:
            # Merge incoming policies against the self.default_policy so any DEFAULT enums get replaced with the default policy option
            p = Policy.merge(self.default_policy, p)
            node = self._get_node(p)  # Get a node based on policy (if requesting specific nodes, may raise exception)
            cpu_affinity = self._get_cpu_affinity(p, node)  # Get affinity based on policy
            gpu_affinity = self._get_gpu_affinity(p, node)
            env_str = ""  # Environment string for setting accelerator affinity
            if gpu_affinity:
                env_str = node.accelerators.env_str
            self._add_layout(node, cpu_affinity, gpu_affinity, env_str, layouts)

        return layouts
