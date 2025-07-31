"""
This test suite validates the behavior of the ProcessGroup.configure_training_group() API
in the Dragon HPC framework. It ensures correct process template creation, environment
variable setup for NCCL distributed training, process placement policies, error handling,
and basic ProcessGroup lifecycle calls (init, start, close).
"""

import unittest
import os
from unittest.mock import patch
from dragon.native.process_group import ProcessGroup, RankInfo
from dragon.infrastructure.policy import Policy
from dragon.native.process import Popen


class FakeNode:
    def __init__(self, node_name):
        self.hostname = node_name


class TestProcessGroupConfigureTrainingGroup(unittest.TestCase):
    def setUp(self):
        self.dummy_train = lambda: None

        # Patch System and Node
        self.system_patch = patch("dragon.native.process_group.System")
        self.node_patch = patch("dragon.native.process_group.Node")

        self.mock_system = self.system_patch.start()
        self.mock_node = self.node_patch.start()

        # Simulate 2 nodes
        self.mock_system.return_value.nodes = ["node1", "node2"]
        self.mock_node.side_effect = lambda node: FakeNode(node)

    def tearDown(self):
        self.system_patch.stop()
        self.node_patch.stop()

    def test_configure_nccl_training_group(self):
        """
        Correct Environment Variable Setup
        """
        ppn = 2
        nprocs = 4

        pg = ProcessGroup.configure_training_group(
            training_fn=self.dummy_train,
            training_args=None,
            training_kwargs=None,
            ppn=ppn,
            nprocs=nprocs,
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
                self.assertEqual(rank_info.my_world_size, nprocs)
                self.assertEqual(rank_info.my_local_world_size, ppn)
                self.assertEqual(rank_info.my_node_rank, node_rank)

            self.assertIsInstance(template.policy, Policy)
            self.assertEqual(template.policy.host_name, f"node{node_rank + 1}")

    # The following tests look for error handling for invalid inputs.

    def test_invalid_ppn_zero(self):
        with self.assertRaises(ValueError):
            ProcessGroup.configure_training_group(training_fn=self.dummy_train, ppn=0, nprocs=4)

    def test_invalid_nprocs_zero(self):
        with self.assertRaises(ValueError):
            ProcessGroup.configure_training_group(training_fn=self.dummy_train, ppn=2, nprocs=0)

    def test_oversubscription(self):
        with self.assertRaises(RuntimeError):
            ProcessGroup.configure_training_group(training_fn=self.dummy_train, ppn=1, nprocs=10)

    def test_empty_node_list(self):
        self.mock_system.return_value.nodes = []
        with self.assertRaises(RuntimeError):
            ProcessGroup.configure_training_group(training_fn=self.dummy_train, ppn=1, nprocs=1)

    def test_node_hostname_failure(self):
        def bad_node(node):
            raise Exception("Node hostname resolution failed")

        self.mock_node.side_effect = bad_node

        with self.assertRaises(Exception):
            ProcessGroup.configure_training_group(training_fn=self.dummy_train, ppn=1, nprocs=2)

    def test_hide_stderr_flag(self):
        """
        hide_stderr Behavior
        """
        pg = ProcessGroup.configure_training_group(training_fn=self.dummy_train, ppn=1, nprocs=2, hide_stderr=True)

        for _, template in pg._local_templates:
            self.assertEqual(template.stderr, template.DEVNULL)

    def test_network_prefix_and_port_defaults(self):
        """
        Defaults for network_prefix and port
        """
        pg = ProcessGroup.configure_training_group(training_fn=self.dummy_train, ppn=1, nprocs=2)

        for rank, (_, template) in enumerate(pg._local_templates):
            env = template.env
            self.assertEqual(env["MASTER_PORT"], "29500")
            self.assertEqual(env["DRAGON_PG_RANK"], str(rank))

    def test_correct_process_count_and_env(self):
        """
        Correct Process Count and Policy Assignment
        """
        ppn = 2
        nprocs = 4

        pg = ProcessGroup.configure_training_group(
            training_fn=self.dummy_train,
            ppn=ppn,
            nprocs=nprocs,
            hide_stderr=False,
            port=12345,
        )

        self.assertEqual(len(pg._local_templates), nprocs)

        for rank, (nproc, template) in enumerate(pg._local_templates):
            env = template.env
            node_rank = rank // ppn
            local_rank = rank % ppn

            with patch.dict(os.environ, env, clear=False):
                rank_info = RankInfo()
                self.assertEqual(rank_info.master_port, "12345")
                self.assertEqual(rank_info.my_rank, rank)
                self.assertEqual(rank_info.my_local_rank, local_rank)
                self.assertEqual(rank_info.my_node_rank, node_rank)

            self.assertEqual(nproc, 1)
            self.assertIsInstance(template.policy, Policy)
            self.assertEqual(template.policy.host_name, f"node{node_rank + 1}")

    def test_smoke_pg_init_start_close(self):
        """
        Smoke test
        """
        with patch.object(ProcessGroup, "init", return_value=None) as mock_init, patch.object(
            ProcessGroup, "start", return_value=None
        ) as mock_start, patch.object(ProcessGroup, "close", return_value=None) as mock_close:

            pg = ProcessGroup.configure_training_group(training_fn=self.dummy_train, ppn=1, nprocs=2)

            pg.init()
            pg.start()
            pg.close()

            mock_init.assert_called_once()
            mock_start.assert_called_once()
            mock_close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
