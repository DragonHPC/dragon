import unittest
import os
from unittest.mock import patch
from dragon.ai.collective_group import CollectiveGroup, RankInfo
from dragon.infrastructure.policy import Policy
from dragon.native.process import Popen


class FakeNode:
    def __init__(self, node_name):
        self.hostname = node_name


class TestCollectiveGroup(unittest.TestCase):
    def setUp(self):
        self.dummy_train = lambda: None

        # Patch System and Node
        self.system_patch = patch("dragon.native.process_group.System")
        self.node_patch = patch("dragon.native.process_group.Node")

        self.mock_system = self.system_patch.start()
        self.mock_node = self.node_patch.start()

        # Simulate 2 nodes
        nodes = ["node1", "node2"]
        self.mock_system.nodes.return_value = nodes
        self.mock_system.hostname_policies.return_value = [
            Policy(placement=Policy.Placement.HOST_NAME, host_name=h) for h in nodes
        ]
        self.mock_node.side_effect = lambda node: FakeNode(node)

    def tearDown(self):
        self.system_patch.stop()
        self.node_patch.stop()

    def test_basic(self):
        ppn = 2
        pols = [p for p in self.mock_system.hostname_policies() for _ in range(ppn)]
        nprocs = len(pols)

        pg = CollectiveGroup(
            training_fn=self.dummy_train,
            training_args=None,
            training_kwargs=None,
            policies=pols,
            hide_stderr=True,
            port=29500,
        )

        self.assertEqual(len(pg._local_templates), nprocs)

        for rank, (nproc, template) in enumerate(pg._local_templates):
            env = template.env
            node_rank = rank // ppn
            local_rank = rank % ppn

            with patch.dict(os.environ, env, clear=False):
                rank_info = RankInfo()
                self.assertEqual(rank_info.master_addr, "node1")
                self.assertEqual(rank_info.master_port, "29500")
                self.assertEqual(rank_info.my_rank, rank)
                self.assertEqual(rank_info.my_local_rank, local_rank)
                self.assertEqual(rank_info.world_size, nprocs)
                self.assertEqual(rank_info.my_local_world_size, ppn)
                self.assertEqual(rank_info.my_node_rank, node_rank)

            self.assertIsInstance(template.policy, Policy)
            self.assertEqual(template.policy.host_name, f"node{node_rank + 1}")


if __name__ == "__main__":
    unittest.main()
