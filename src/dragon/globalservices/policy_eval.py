import enum
from dataclasses import dataclass, field, fields
from ..infrastructure.policy import Policy
from ..infrastructure.node_desc import NodeDescriptor

@dataclass
class ResourceLayout:
    h_uid : int
    host_name : str
    numa_node : int # TODO
    core : [int] # List of acceptable CPU cores the layout can be applied to
    accelerator : int # TODO


# Possible TODO: Affinity offset or placement offset, to "start counting from here", just make sure it starts at a certain place
class PolicyEvaluator:
    """
    Based on a list of NodeDescriptors, evaluate policies and apply them
    """

    def __init__(self, nodes : list[NodeDescriptor], default_policy=None):
        self.nodes = nodes
        self.num_nodes = len(nodes)
        self.default_policy = default_policy # This will be used to fill in any Policy values that are DEFAULT when evaluated
        self.overprovision = False # Should make this into a parameter flag to allow/disallow it?
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
            return node # Indicate we are now overprovisioning mode, we only have one node

        # Loop through nodes until we hit our starting point
        while idx != cur_idx:
            next_node = self.nodes[idx]
            if next_node.num_policies < next_node.num_cpus:
                node = next_node
                break

            idx = (idx + 1) % self.num_nodes

        return node

    def _add_layout(self, node, affinity, layouts):
        """
        Short helper function to auto-increment num_policies value on nodes

        :param node: Node we're generating the layout for
        :param affinity: List of CPU devices local services can apply to
        :param layouts: List of layouts to append to

        """
        # NOTE: Numa node and accelerator are placeholders
        layouts.append( ResourceLayout(node.h_uid, node.host_name, 1, affinity, 0) )
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

    def _get_node(self, p : Policy) -> NodeDescriptor:
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
        distribution = p.distribution # Use a temp value instead of modifying the policy
        if distribution == Policy.Distribution.DEFAULT:
            distribution = Policy.Distribution.ROUNDROBIN

        # RoundRobin defaults to always overprovisioning, it will always just assign a policy to "next node"
        # TODO: Mimic some overprovisioning logic like in BLOCK for roundrobin to find the next empty slot?
        if distribution == Policy.Distribution.ROUNDROBIN:
            self.cur_node += 1
            return node

        elif distribution == Policy.Distribution.BLOCK:
            if self.overprovision:
                self.cur_node += 1 # Basically means we're in round-robin

            elif node.num_policies >= node.num_cpus:
                next_node = self._find_next_open(self.cur_node) # Find next open node
                if next_node is None: # All nodes full
                    self.overprovision = True # Start overprovisioning
                    next_node = node # Reassign next_node from None to current node
                    self.cur_node += 1 # Move to next node in cycle (round robin, essentially)
                node = next_node # Assign current node to "next node" (either open node, or same node to begin with)

            return node

    def _get_affinity(self, p : Policy, node : NodeDescriptor) -> list[int]:
        """
        Generate a list of available devices the policy can be applied to for the given Node
        """

        affinity = p.affinity
        if affinity == Policy.Affinity.DEFAULT:
            affinity = Policy.Affinity.ANY

        device = p.device
        if device == Policy.Device.DEFAULT:
            device = Policy.Device.CPU

        if device == Policy.Device.CPU:
            if p.specific_affinity: # List not empty, assume SPECIFIC affinity
                affinity = [x for x in node.cpu_devices if x in p.specific_affinity]
                return affinity # This covers both "ANY" and "SPECIFIC" if a specific list is given

            if affinity == Policy.Affinity.ANY:
                return node.cpu_devices

        if device == Policy.Device.GPU:
            raise RuntimeError("GPU Affinity Not Implemented")
        # TODO: Affinity ROUNDROBIN and BLOCK.  These are more load balancing options than not, so tracking usage needs to be figured out
        # TODO: GPU Affinity.  NodeDescriptor needs to pull a list of available accelerators (through an os.exec grep command maybe?)

        return []

    def evaluate(self, policies : list[Policy]=None) -> list[ResourceLayout]:
        """
        Evaluate a list of policies and return layouts of what nodes to apply them to
        """

        layouts = []

        # Iterate over policies and apply them through node descriptors
        for p in policies:
            # Merge incoming policies against the self.default_policy so any DEFAULT enums get replaced with the default policy option
            p = self.merge(self.default_policy, p)
            node = self._get_node(p) # Get a node based on policy (if requesting specific nodes, may raise exception)
            affinity = self._get_affinity(p, node) # Get affinity based on policy
            self._add_layout(node, affinity, layouts)

        return layouts

    @staticmethod
    def merge(high_policy: Policy, low_policy: Policy) -> Policy:
        """
        Merge two policies, using values from high_policy for values not assigned on init
        Returns a new policy

        :param high_policy: Non-default values take precidence
        :param low_policy: Default values will be replaced by `high_policy` values
        """

        retain = {}

        for f in fields(low_policy):
            v = getattr(low_policy, f.name)
            if v == f.default:
                continue # Default value, do not retain
            retain[f.name] = v # Value was assigned specifically, retain

        kwargs = {**high_policy.get_sdict(), **retain} # Merge retained policy values with high priority values
        return type(high_policy)(**kwargs)
