import os
import unittest

from dragon.data.ddict import DDict
from dragon.native.machine import System, Node
from dragon.infrastructure.policy import Policy
from dragon.telemetry.analysis import AnalysisClient


class TestRuntimeReboot(unittest.TestCase):

    def test_reboot(self):

        is_a_restart = bool(os.environ.get("DRAGON_RESILIENT_RESTART", False))

        managers_per_node = 1
        total_mem_size = 1024 * 1024 * 1024
        my_alloc = System()
        node_list = my_alloc.nodes
        self.assertTrue(len(node_list) > 1)
        node_hostnames = []
        for node_id in node_list:
            node = Node(node_id)
            node_hostnames.append(node.hostname)

        excluded_node = node_hostnames[-1]
        included_nodes = node_hostnames[:-1]
        n_nodes = len(included_nodes)

        if not is_a_restart:
            print(f"Excluding node = {excluded_node}", flush=True)
            print(f"Will restart with = {included_nodes}", flush=True)
        else:
            print(f"Restarted on = {node_hostnames}", flush=True)

        policy = []
        for node_hostname in included_nodes:
            policy.append(Policy(placement=Policy.Placement.HOST_NAME, host_name=node_hostname))

        if not is_a_restart:
            ddict_policy = policy
            managers_per_policy = managers_per_node
            managers_per_node = None
            n_nodes = None
        else:
            managers_per_policy = None
            ddict_policy = None

        state_dict = DDict(
            managers_per_node=managers_per_node,
            n_nodes=n_nodes,
            managers_per_policy=managers_per_policy,
            policy=ddict_policy,
            total_mem=total_mem_size,
            name="test_name",
            timeout=200,
            restart=is_a_restart,
        )

        tac = AnalysisClient()

        if not is_a_restart:
            state_dict["excluded_node"] = excluded_node
            state_dict["gnar"] = 15000
            state_dict.destroy(allow_restart=True)
            print("Passed non-restart path", flush=True)
            tac.reboot(exclude_hostnames=[excluded_node])
        else:
            self.assertEqual(state_dict["gnar"], 15000)
            self.assertFalse(state_dict["excluded_node"] in node_hostnames)
            state_dict.destroy()
            print("Passed restart path", flush=True)


if __name__ == "__main__":
    unittest.main()
