#!/usr/bin/env python3

import unittest
import random
import string
import json

from dragon.globalservices.policy_eval import Policy, PolicyEvaluator, ResourceLayout
from dragon.infrastructure.node_desc import NodeDescriptor

class TestPolicy(unittest.TestCase):

    def test_sdict(self):
        policy = Policy()
        sdict = policy.sdesc
        policy2 = Policy.from_sdict(sdict)
        sdict2 = policy2.sdesc
        for k in sdict:
            self.assertEqual(sdict[k], sdict2[k])

    def test_json_sdict(self):
        # Policy can be json-ified, then un-json'd, and values match
        policy = Policy()
        orig_sdict = policy.sdesc
        ser_sdict = json.dumps(Policy.get_sdict(policy))
        unser_sdict = json.loads(ser_sdict)
        for k in orig_sdict:
            self.assertEqual(orig_sdict[k], unser_sdict[k])


    def test_default(self):
        policy = Policy()
        self.assertEqual(policy.placement, Policy.Placement.DEFAULT)
        self.assertEqual(policy.host_name, '')
        self.assertEqual(policy.host_id, -1)
        self.assertEqual(policy.distribution, Policy.Distribution.DEFAULT)

    def test_host_name(self):
        # Test a random hostname and make sure the policy holds it
        name = ''.join(random.choices(string.ascii_letters + string.digits,
                                      k=random.randint(1, 236)))

        policy = Policy(host_name=name)
        self.assertEqual(policy.placement, Policy.Placement.DEFAULT)
        self.assertEqual(policy.host_name, name)
        self.assertEqual(policy.host_id, -1)
        self.assertEqual(policy.distribution, Policy.Distribution.DEFAULT)

    def test_host_id(self):
        policy = Policy(host_id=100)
        self.assertEqual(policy.placement, Policy.Placement.DEFAULT)
        self.assertEqual(policy.host_name, '')
        self.assertEqual(policy.host_id, 100)
        self.assertEqual(policy.distribution, Policy.Distribution.DEFAULT)

    def test_merge(self):
        high = Policy(placement=Policy.Placement.HOST_NAME, host_name='high_policy')
        low = Policy(affinity=Policy.Affinity.SPECIFIC, specific_affinity=[1,3,5])
        out = PolicyEvaluator.merge(high, low)
        self.assertEqual(high.placement, out.placement)
        self.assertEqual(high.host_name, out.host_name)
        self.assertEqual(low.affinity, out.affinity)
        self.assertEqual(low.specific_affinity, out.specific_affinity)


class TestEvaluator(unittest.TestCase):

    def setUp(self):
        self.node_cpus = 4
        self.nodes = []
        self.num_nodes = 4
        for i in range(1, self.num_nodes+1):
            node = NodeDescriptor()
            node.host_name = str(i)
            node.host_id = i
            node.h_uid = i
            node.num_cpus = self.node_cpus
            self.nodes.append(node)

        self.RR = Policy.Distribution.ROUNDROBIN
        self.BLOCK = Policy.Distribution.BLOCK
        self.policy = Policy(
            placement=Policy.Placement.ANYWHERE,
            host_name="",
            host_id=-1,
            distribution=Policy.Distribution.ROUNDROBIN,
            device=Policy.Device.CPU,
            affinity=Policy.Affinity.ANY,
            specific_affinity=[],
            wait_mode=Policy.WaitMode.IDLE,
            refcounted=True
        )

    def test_roundrobin(self):
        policies = []
        for i in range(len(self.nodes)*2):
            p = Policy(distribution=self.RR)
            policies.append(p)

        eval = PolicyEvaluator(self.nodes, self.policy)
        layouts = eval.evaluate(policies=policies)
        self.assertEqual(len(layouts), len(policies))
        for node in self.nodes:
            self.assertEqual(node.num_policies, 2)

    def test_block(self):
        policies = []
        for i in range(self.node_cpus * 2):
            p = Policy(distribution=self.BLOCK)
            policies.append(p)

        eval = PolicyEvaluator(self.nodes, self.policy)
        layouts = eval.evaluate(policies=policies)
        self.assertEqual(len(layouts), len(policies))

        # First half of nodes should be full
        for i in range(self.num_nodes // 2):
            self.assertEqual(self.nodes[i].num_policies, 4)

        # Last half of nodes should be empty
        for i in range(self.num_nodes // 2, self.num_nodes):
            self.assertEqual(self.nodes[i].num_policies, 0)

    def test_mix(self):
        policies = []
        policies.append(Policy(distribution=self.RR)) # Node 0, 1 policy, move to next node
        policies.append(Policy(distribution=self.BLOCK)) # Node 1, 1 policy, stay on node
        policies.append(Policy(distribution=self.BLOCK)) # Node 1, 2 policies, stay on node
        policies.append(Policy(distribution=self.BLOCK)) # Node 1, 3 policies, stay on node
        policies.append(Policy(distribution=self.RR)) # Node 1, 4 policies, move to next node
        policies.append(Policy(distribution=self.BLOCK)) # Node 2, 1 policy, stay on node
        policies.append(Policy(distribution=self.RR)) # Node 2, 2 policies, move to next node
        policies.append(Policy(distribution=self.BLOCK)) # Node 3, 1 policy, stay on node

        eval = PolicyEvaluator(self.nodes, self.policy)
        layouts = eval.evaluate(policies=policies)
        self.assertEqual(self.nodes[0].num_policies, 1)
        self.assertEqual(self.nodes[1].num_policies, 4)
        self.assertEqual(self.nodes[2].num_policies, 2)
        self.assertEqual(self.nodes[3].num_policies, 1)

    def test_host_id(self):
        p = Policy(placement=Policy.Placement.HOST_ID, host_id=2)
        eval = PolicyEvaluator(self.nodes, self.policy)
        layouts = eval.evaluate(policies=[p])
        self.assertEqual(layouts[0].h_uid, 2)

    def test_host_name(self):
        p = Policy(placement=Policy.Placement.HOST_NAME, host_name='2')
        eval = PolicyEvaluator(self.nodes, self.policy)
        layouts = eval.evaluate(policies=[p])
        self.assertEqual(layouts[0].host_name, '2')

    def test_overprovision(self):
        # Generate N policies for N total CPUs across nodes
        # Apply N policies to N/2 nodes
        policies =[]
        for i in range(self.num_nodes * self.node_cpus):
            p = Policy(distribution=self.BLOCK)
            policies.append(p)

        eval = PolicyEvaluator(self.nodes[:len(self.nodes)//2], self.policy)
        layouts = eval.evaluate(policies=policies)

        self.assertEqual(self.nodes[0].num_policies, self.num_nodes * 2)
        self.assertEqual(self.nodes[1].num_policies, self.num_nodes * 2)

    def test_affinity_any(self):
        node = NodeDescriptor()
        p = Policy()
        eval = PolicyEvaluator([node], self.policy)
        layout = eval.evaluate([p])
        self.assertEqual(layout[0].core, node.cpu_devices)

    def test_affinity_specific(self):
        node = NodeDescriptor()
        expected_cores = [1, 3, 5]
        p = Policy(affinity=Policy.Affinity.SPECIFIC, specific_affinity=expected_cores)
        eval = PolicyEvaluator([node], self.policy)
        layout = eval.evaluate([p])
        self.assertEqual(layout[0].core, expected_cores)

if __name__ == '__main__':
    unittest.main()
