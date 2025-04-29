#!/usr/bin/env python3

import unittest
import random
import string
import json
import threading

from .group_api import GSGroupBaseClass
from .process_api import GSProcessBaseClass

import support.util as tsu
import dragon.infrastructure.messages as dmsg

from dragon.globalservices.policy_eval import Policy, PolicyEvaluator
from dragon.infrastructure.node_desc import NodeDescriptor
from dragon.infrastructure.process_desc import ProcessDescriptor
from dragon.globalservices.process import create as process_create
from dragon.globalservices.group import create as group_create
from dragon.globalservices.process import get_create_message


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
        self.assertEqual(policy.host_name, "")
        self.assertEqual(policy.host_id, -1)
        self.assertEqual(policy.distribution, Policy.Distribution.DEFAULT)

    def test_host_name(self):
        # Test a random hostname and make sure the policy holds it
        name = "".join(random.choices(string.ascii_letters + string.digits, k=random.randint(1, 236)))

        policy = Policy(host_name=name)
        self.assertEqual(policy.placement, Policy.Placement.DEFAULT)
        self.assertEqual(policy.host_name, name)
        self.assertEqual(policy.host_id, -1)
        self.assertEqual(policy.distribution, Policy.Distribution.DEFAULT)

    def test_host_id(self):
        policy = Policy(host_id=100)
        self.assertEqual(policy.placement, Policy.Placement.DEFAULT)
        self.assertEqual(policy.host_name, "")
        self.assertEqual(policy.host_id, 100)
        self.assertEqual(policy.distribution, Policy.Distribution.DEFAULT)

    def test_merge(self):
        low = Policy(placement=Policy.Placement.HOST_NAME, host_name="low_policy", gpu_affinity=[1, 2])
        high = Policy(cpu_affinity=[1, 3, 5])
        out = Policy.merge(low, high)
        self.assertEqual(low.placement, out.placement)
        self.assertEqual(low.host_name, out.host_name)
        self.assertEqual(high.cpu_affinity, out.cpu_affinity)
        self.assertEqual(low.gpu_affinity, out.gpu_affinity)


class TestEvaluator(unittest.TestCase):
    def setUp(self):
        self.node_cpus = 4
        self.nodes = []
        self.num_nodes = 4
        for i in range(1, self.num_nodes + 1):
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
            cpu_affinity=[],
            wait_mode=Policy.WaitMode.IDLE,
            refcounted=True,
        )

    def test_roundrobin(self):
        policies = []
        for i in range(len(self.nodes) * 2):
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
        policies.append(Policy(distribution=self.RR))  # Node 0, 1 policy, move to next node
        policies.append(Policy(distribution=self.BLOCK))  # Node 1, 1 policy, stay on node
        policies.append(Policy(distribution=self.BLOCK))  # Node 1, 2 policies, stay on node
        policies.append(Policy(distribution=self.BLOCK))  # Node 1, 3 policies, stay on node
        policies.append(Policy(distribution=self.RR))  # Node 1, 4 policies, move to next node
        policies.append(Policy(distribution=self.BLOCK))  # Node 2, 1 policy, stay on node
        policies.append(Policy(distribution=self.RR))  # Node 2, 2 policies, move to next node
        policies.append(Policy(distribution=self.BLOCK))  # Node 3, 1 policy, stay on node

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
        p = Policy(placement=Policy.Placement.HOST_NAME, host_name="2")
        eval = PolicyEvaluator(self.nodes, self.policy)
        layouts = eval.evaluate(policies=[p])
        self.assertEqual(layouts[0].host_name, "2")

    def test_overprovision(self):
        # Generate N policies for N total CPUs across nodes
        # Apply N policies to N/2 nodes
        policies = []
        for i in range(self.num_nodes * self.node_cpus):
            p = Policy(distribution=self.BLOCK)
            policies.append(p)

        eval = PolicyEvaluator(self.nodes[: len(self.nodes) // 2], self.policy)
        layouts = eval.evaluate(policies=policies)

        self.assertEqual(self.nodes[0].num_policies, self.num_nodes * 2)
        self.assertEqual(self.nodes[1].num_policies, self.num_nodes * 2)

    def test_affinity_any(self):
        node = NodeDescriptor.get_localservices_node_conf()
        p = Policy()
        eval = PolicyEvaluator([node], self.policy)
        layout = eval.evaluate([p])
        self.assertEqual(layout[0].cpu_core, node.cpu_devices)

    def test_affinity_specific(self):
        node = NodeDescriptor.get_localservices_node_conf()
        expected_cores = [1, 3, 5]
        p = Policy(cpu_affinity=expected_cores)
        eval = PolicyEvaluator([node], self.policy)
        layout = eval.evaluate([p])
        self.assertEqual(layout[0].cpu_core, expected_cores)


class TestProcessPolicy(GSProcessBaseClass):
    def _create_proc_with_context(self, proc_name, context_policy, process_policy=None):
        def create_wrap(the_exe, the_run_dir, the_args, the_env, result_list):
            with context_policy:
                res = process_create(
                    exe=the_exe,
                    run_dir=the_run_dir,
                    args=the_args,
                    env=the_env,
                    user_name=proc_name,
                    policy=process_policy,
                )
            result_list.append(res)

        create_result = []
        create_thread = threading.Thread(target=create_wrap, args=("test", "/tmp", ["foo", "bar"], {}, create_result))
        create_thread.start()

        shep_msg = tsu.get_and_check_type(self.shep_input_rh, dmsg.SHProcessCreate)
        shep_reply_msg = dmsg.SHProcessCreateResponse(
            tag=self.next_tag(), ref=shep_msg.tag, err=dmsg.SHProcessCreateResponse.Errors.SUCCESS
        )

        self.gs_input_wh.send(shep_reply_msg.serialize())

        create_thread.join()
        desc = create_result[0]
        return desc

    def test_policy_merge(self):
        my_policy = Policy(distribution=Policy.Distribution.BLOCK)

        # Manually combine my_policy with Policy.global_policy()
        expected_policy = Policy(
            placement=Policy.Placement.ANYWHERE,
            distribution=Policy.Distribution.BLOCK,
            wait_mode=Policy.WaitMode.IDLE,
        )

        policy_after_merge = Policy.merge(Policy.global_policy(), my_policy)
        self.assertEqual(policy_after_merge, expected_policy)

    def test_process_with_no_policy(self):
        pdescr: ProcessDescriptor = self._create_proc(f"alice", policy=None)
        self.assertIsNotNone(pdescr.policy)
        self.assertEqual(pdescr.policy, Policy.global_policy())

    def test_process_with_policy(self):
        my_context_distribution = Policy.Distribution.BLOCK
        my_policy = Policy(distribution=Policy.Distribution.BLOCK)
        expected_policy = Policy.merge(Policy.global_policy(), my_policy)

        pdescr: ProcessDescriptor = self._create_proc(f"alice", policy=my_policy)

        self.assertIsNotNone(pdescr.policy)
        self.assertEqual(pdescr.policy, expected_policy)
        self.assertEqual(pdescr.policy.placement, Policy.global_policy().placement)
        self.assertEqual(pdescr.policy.host_name, Policy.global_policy().host_name)
        self.assertEqual(pdescr.policy.host_id, Policy.global_policy().host_id)
        self.assertEqual(pdescr.policy.distribution, my_context_distribution)
        self.assertEqual(pdescr.policy.cpu_affinity, Policy.global_policy().cpu_affinity)
        self.assertEqual(pdescr.policy.gpu_env_str, Policy.global_policy().gpu_env_str)
        self.assertEqual(pdescr.policy.gpu_affinity, Policy.global_policy().gpu_affinity)
        self.assertEqual(pdescr.policy.wait_mode, Policy.global_policy().wait_mode)
        self.assertEqual(pdescr.policy.refcounted, Policy.global_policy().refcounted)

    def test_process_with_policy_context(self):
        my_context_distribution = Policy.Distribution.BLOCK
        my_context_policy = Policy(distribution=my_context_distribution)
        expected_policy = Policy.merge(Policy.global_policy(), my_context_policy)

        pdescr: ProcessDescriptor = self._create_proc_with_context(f"alice", context_policy=my_context_policy)

        self.assertIsNotNone(pdescr.policy)
        self.assertEqual(pdescr.policy, expected_policy)
        self.assertEqual(pdescr.policy.placement, Policy.global_policy().placement)
        self.assertEqual(pdescr.policy.host_name, Policy.global_policy().host_name)
        self.assertEqual(pdescr.policy.host_id, Policy.global_policy().host_id)
        self.assertEqual(pdescr.policy.distribution, my_context_distribution)
        self.assertEqual(pdescr.policy.cpu_affinity, Policy.global_policy().cpu_affinity)
        self.assertEqual(pdescr.policy.gpu_env_str, Policy.global_policy().gpu_env_str)
        self.assertEqual(pdescr.policy.gpu_affinity, Policy.global_policy().gpu_affinity)
        self.assertEqual(pdescr.policy.wait_mode, Policy.global_policy().wait_mode)
        self.assertEqual(pdescr.policy.refcounted, Policy.global_policy().refcounted)

    def test_process_with_policy_context_and_process_policy(self):
        my_process_placement = Policy.Placement.LOCAL

        my_process_policy = Policy(placement=my_process_placement)

        my_context_placement = Policy.Placement.HOST_NAME
        my_context_placement_host_name = "node2"
        my_context_distribution = Policy.Distribution.BLOCK

        my_context_policy = Policy(
            placement=my_context_placement,
            host_name=my_context_placement_host_name,
            distribution=my_context_distribution,
        )

        expected_policy = Policy.merge(Policy.global_policy(), Policy.merge(my_context_policy, my_process_policy))

        pdescr: ProcessDescriptor = self._create_proc_with_context(
            f"alice", context_policy=my_context_policy, process_policy=my_process_policy
        )

        self.assertIsNotNone(pdescr.policy)
        self.assertEqual(pdescr.policy, expected_policy)
        self.assertEqual(pdescr.policy.placement, my_process_placement)
        self.assertEqual(pdescr.policy.host_name, my_context_placement_host_name)
        self.assertEqual(pdescr.policy.host_id, Policy.global_policy().host_id)
        self.assertEqual(pdescr.policy.distribution, my_context_distribution)
        self.assertEqual(pdescr.policy.cpu_affinity, Policy.global_policy().cpu_affinity)
        self.assertEqual(pdescr.policy.gpu_env_str, Policy.global_policy().gpu_env_str)
        self.assertEqual(pdescr.policy.gpu_affinity, Policy.global_policy().gpu_affinity)
        self.assertEqual(pdescr.policy.wait_mode, Policy.global_policy().wait_mode)
        self.assertEqual(pdescr.policy.refcounted, Policy.global_policy().refcounted)


class TestGroupPolicy(GSGroupBaseClass):
    def _create_group_with_policy_context(
        self, group_items, policy_context, group_name, existing=False, group_policy=None
    ):
        def create_wrap(items, result_list):
            with policy_context:
                res = group_create(items, policy=group_policy, user_name=group_name)
                result_list.append(res)

        create_result = []
        create_thread = threading.Thread(target=create_wrap, args=(group_items, create_result))
        create_thread.start()

        # get the number of members inside this group in total
        nitems = 0
        for lst in group_items:
            nitems += lst[0]

        # in the case that we're trying to create a group that already exists,
        # we don't need the following code
        if not existing:  # not already existing group
            self._send_get_responses(nitems, "success")

        create_thread.join()

        desc = create_result[0]
        return desc

    def test_group_create_no_policy(self):
        n = 10
        process_msg = get_create_message(exe="test", run_dir="/tmp", args=["foo", "bar"], env={}, user_name="solver")
        items = [(n, process_msg.serialize())]
        policy = Policy()
        descr = self._create_group(items, policy, "bob")

        self.assertEqual(descr.policy, Policy.global_policy())
        self.assertEqual(len(descr.sets), 1)  # 1 list in the sets
        self.assertEqual(len(descr.sets[0]), n)  # n members inside the list

        for set in descr.sets:
            for group_member in set:
                self.assertEqual(group_member.desc.policy, Policy.global_policy())

    def test_group_with_group_policy(self):
        n = 10

        my_group_placement = Policy.Placement.LOCAL
        my_group_policy = Policy(placement=my_group_placement)

        expected_policy = Policy.merge(Policy.global_policy(), my_group_policy)

        process_msg = get_create_message(exe="test", run_dir="/tmp", args=["foo", "bar"], env={}, user_name="solver")
        items = [(n, process_msg.serialize())]

        descr = self._create_group(items, my_group_policy, "bob")

        self.assertEqual(descr.policy, expected_policy)
        self.assertEqual(descr.policy.placement, my_group_placement)
        self.assertEqual(descr.policy.host_name, Policy.global_policy().host_name)
        self.assertEqual(descr.policy.host_id, Policy.global_policy().host_id)
        self.assertEqual(descr.policy.distribution, Policy.global_policy().distribution)
        self.assertEqual(descr.policy.cpu_affinity, Policy.global_policy().cpu_affinity)
        self.assertEqual(descr.policy.gpu_env_str, Policy.global_policy().gpu_env_str)
        self.assertEqual(descr.policy.gpu_affinity, Policy.global_policy().gpu_affinity)
        self.assertEqual(descr.policy.wait_mode, Policy.global_policy().wait_mode)
        self.assertEqual(descr.policy.refcounted, Policy.global_policy().refcounted)

        self.assertEqual(len(descr.sets), 1)  # 1 list in the sets
        self.assertEqual(len(descr.sets[0]), n)  # n members inside the list

        for set in descr.sets:
            for group_member in set:
                self.assertEqual(group_member.desc.policy, expected_policy)
                self.assertEqual(group_member.desc.policy.placement, my_group_placement)
                self.assertEqual(group_member.desc.policy.host_name, Policy.global_policy().host_name)
                self.assertEqual(group_member.desc.policy.host_id, Policy.global_policy().host_id)
                self.assertEqual(group_member.desc.policy.distribution, Policy.global_policy().distribution)
                self.assertEqual(group_member.desc.policy.cpu_affinity, Policy.global_policy().cpu_affinity)
                self.assertEqual(group_member.desc.policy.gpu_env_str, Policy.global_policy().gpu_env_str)
                self.assertEqual(group_member.desc.policy.gpu_affinity, Policy.global_policy().gpu_affinity)
                self.assertEqual(group_member.desc.policy.wait_mode, Policy.global_policy().wait_mode)
                self.assertEqual(group_member.desc.policy.refcounted, Policy.global_policy().refcounted)

    def test_group_with_policy_context(self):
        n = 10

        my_context_placement = Policy.Placement.LOCAL
        my_context_policy = Policy(placement=my_context_placement)

        expected_policy = Policy.merge(Policy.global_policy(), my_context_policy)

        process_msg = get_create_message(exe="test", run_dir="/tmp", args=["foo", "bar"], env={}, user_name="solver")
        items = [(n, process_msg.serialize())]

        descr = self._create_group_with_policy_context(items, my_context_policy, "bob")

        self.assertEqual(descr.policy, expected_policy)
        self.assertEqual(descr.policy.placement, my_context_placement)
        self.assertEqual(descr.policy.host_name, Policy.global_policy().host_name)
        self.assertEqual(descr.policy.host_id, Policy.global_policy().host_id)
        self.assertEqual(descr.policy.distribution, Policy.global_policy().distribution)
        self.assertEqual(descr.policy.cpu_affinity, Policy.global_policy().cpu_affinity)
        self.assertEqual(descr.policy.gpu_env_str, Policy.global_policy().gpu_env_str)
        self.assertEqual(descr.policy.gpu_affinity, Policy.global_policy().gpu_affinity)
        self.assertEqual(descr.policy.wait_mode, Policy.global_policy().wait_mode)
        self.assertEqual(descr.policy.refcounted, Policy.global_policy().refcounted)

        self.assertEqual(len(descr.sets), 1)  # 1 list in the sets
        self.assertEqual(len(descr.sets[0]), n)  # n members inside the list

        for set in descr.sets:
            for group_member in set:
                self.assertEqual(group_member.desc.policy, expected_policy)
                self.assertEqual(group_member.desc.policy.placement, my_context_placement)
                self.assertEqual(group_member.desc.policy.host_name, Policy.global_policy().host_name)
                self.assertEqual(group_member.desc.policy.host_id, Policy.global_policy().host_id)
                self.assertEqual(group_member.desc.policy.distribution, Policy.global_policy().distribution)
                self.assertEqual(group_member.desc.policy.cpu_affinity, Policy.global_policy().cpu_affinity)
                self.assertEqual(group_member.desc.policy.gpu_env_str, Policy.global_policy().gpu_env_str)
                self.assertEqual(group_member.desc.policy.gpu_affinity, Policy.global_policy().gpu_affinity)
                self.assertEqual(group_member.desc.policy.wait_mode, Policy.global_policy().wait_mode)
                self.assertEqual(group_member.desc.policy.refcounted, Policy.global_policy().refcounted)

    def test_group_with_policy_context_and_policy(self):
        n = 10

        my_group_placement = Policy.Placement.LOCAL
        my_group_policy = Policy(placement=my_group_placement)

        my_context_placement = Policy.Placement.HOST_ID
        my_context_host_id = 1234
        my_context_policy = Policy(placement=my_context_placement, host_id=my_context_host_id)

        expected_policy = Policy.merge(Policy.global_policy(), Policy.merge(my_context_policy, my_group_policy))

        process_msg = get_create_message(exe="test", run_dir="/tmp", args=["foo", "bar"], env={}, user_name="solver")
        items = [(n, process_msg.serialize())]

        descr = self._create_group_with_policy_context(items, my_context_policy, "bob", group_policy=my_group_policy)

        self.assertEqual(descr.policy, expected_policy)
        self.assertEqual(descr.policy.placement, my_group_placement)
        self.assertEqual(descr.policy.host_name, Policy.global_policy().host_name)
        self.assertEqual(descr.policy.host_id, my_context_host_id)
        self.assertEqual(descr.policy.distribution, Policy.global_policy().distribution)
        self.assertEqual(descr.policy.cpu_affinity, Policy.global_policy().cpu_affinity)
        self.assertEqual(descr.policy.gpu_env_str, Policy.global_policy().gpu_env_str)
        self.assertEqual(descr.policy.gpu_affinity, Policy.global_policy().gpu_affinity)
        self.assertEqual(descr.policy.wait_mode, Policy.global_policy().wait_mode)
        self.assertEqual(descr.policy.refcounted, Policy.global_policy().refcounted)

        self.assertEqual(len(descr.sets), 1)  # 1 list in the sets
        self.assertEqual(len(descr.sets[0]), n)  # n members inside the list

        for set in descr.sets:
            for group_member in set:
                self.assertEqual(group_member.desc.policy, expected_policy)
                self.assertEqual(group_member.desc.policy.placement, my_group_placement)
                self.assertEqual(group_member.desc.policy.host_name, Policy.global_policy().host_name)
                self.assertEqual(group_member.desc.policy.host_id, my_context_host_id)
                self.assertEqual(group_member.desc.policy.distribution, Policy.global_policy().distribution)
                self.assertEqual(group_member.desc.policy.cpu_affinity, Policy.global_policy().cpu_affinity)
                self.assertEqual(group_member.desc.policy.gpu_env_str, Policy.global_policy().gpu_env_str)
                self.assertEqual(group_member.desc.policy.gpu_affinity, Policy.global_policy().gpu_affinity)
                self.assertEqual(group_member.desc.policy.wait_mode, Policy.global_policy().wait_mode)
                self.assertEqual(group_member.desc.policy.refcounted, Policy.global_policy().refcounted)

    def test_group_with_policy_context_group_policy_and_process_policy(self):
        n = 10

        my_process_1_placement = Policy.Placement.LOCAL
        my_process_1_policy = Policy(placement=my_process_1_placement)

        my_process_2_placement = Policy.Placement.LOCAL
        my_process_2_cpu_affinity = [1, 2]
        my_process_2_policy = Policy(placement=my_process_2_placement, cpu_affinity=my_process_2_cpu_affinity)

        my_group_placement = Policy.Placement.HOST_NAME
        my_group_host_name = "node2"
        my_group_policy = Policy(placement=my_group_placement, host_name=my_group_host_name)

        my_context_placement = Policy.Placement.HOST_ID
        my_context_host_id = 1234
        my_context_policy = Policy(placement=my_context_placement, host_id=my_context_host_id)

        expected_group_policy = Policy.merge(
            Policy.global_policy(),
            Policy.merge(
                my_context_policy,
                my_group_policy,
            ),
        )

        expected_process_1_policy = Policy.merge(
            Policy.global_policy(), Policy.merge(my_context_policy, Policy.merge(my_group_policy, my_process_1_policy))
        )

        expected_process_2_policy = Policy.merge(
            Policy.global_policy(), Policy.merge(my_context_policy, Policy.merge(my_group_policy, my_process_2_policy))
        )

        process_msg_1 = get_create_message(
            exe="test", run_dir="/tmp", args=["foo", "bar"], env={}, user_name="solver", policy=my_process_1_policy
        )
        process_msg_2 = get_create_message(
            exe="test", run_dir="/tmp", args=["foo", "bar"], env={}, user_name="solver", policy=my_process_2_policy
        )
        items = [
            (n // 2, process_msg_1.serialize()),
            (n // 2, process_msg_2.serialize()),
        ]

        descr = self._create_group_with_policy_context(items, my_context_policy, "bob", group_policy=my_group_policy)

        self.assertEqual(descr.policy, expected_group_policy)
        self.assertEqual(descr.policy.placement, my_group_placement)
        self.assertEqual(descr.policy.host_name, my_group_host_name)
        self.assertEqual(descr.policy.host_id, my_context_host_id)
        self.assertEqual(descr.policy.distribution, Policy.global_policy().distribution)
        self.assertEqual(descr.policy.cpu_affinity, Policy.global_policy().cpu_affinity)
        self.assertEqual(descr.policy.gpu_env_str, Policy.global_policy().gpu_env_str)
        self.assertEqual(descr.policy.gpu_affinity, Policy.global_policy().gpu_affinity)
        self.assertEqual(descr.policy.wait_mode, Policy.global_policy().wait_mode)
        self.assertEqual(descr.policy.refcounted, Policy.global_policy().refcounted)

        self.assertEqual(len(descr.sets), 2)  # 1 list in the sets
        self.assertEqual(len(descr.sets[0]), n // 2)  # n members inside the list
        self.assertEqual(len(descr.sets[1]), n // 2)  # n members inside the list

        first_set = descr.sets[0]
        for group_member in first_set:
            self.assertEqual(group_member.desc.policy, expected_process_1_policy)
            self.assertEqual(group_member.desc.policy.placement, my_process_1_placement)
            self.assertEqual(group_member.desc.policy.host_name, my_group_host_name)
            self.assertEqual(group_member.desc.policy.host_id, my_context_host_id)
            self.assertEqual(group_member.desc.policy.distribution, Policy.global_policy().distribution)
            self.assertEqual(group_member.desc.policy.cpu_affinity, Policy.global_policy().cpu_affinity)
            self.assertEqual(group_member.desc.policy.gpu_env_str, Policy.global_policy().gpu_env_str)
            self.assertEqual(group_member.desc.policy.gpu_affinity, Policy.global_policy().gpu_affinity)
            self.assertEqual(group_member.desc.policy.wait_mode, Policy.global_policy().wait_mode)
            self.assertEqual(group_member.desc.policy.refcounted, Policy.global_policy().refcounted)

        second_set = descr.sets[1]
        for group_member in second_set:
            self.assertEqual(group_member.desc.policy, expected_process_2_policy)
            self.assertEqual(group_member.desc.policy.placement, my_process_2_placement)
            self.assertEqual(group_member.desc.policy.host_name, my_group_host_name)
            self.assertEqual(group_member.desc.policy.host_id, my_context_host_id)
            self.assertEqual(group_member.desc.policy.distribution, Policy.global_policy().distribution)
            self.assertEqual(group_member.desc.policy.cpu_affinity, my_process_2_cpu_affinity)
            self.assertEqual(group_member.desc.policy.gpu_env_str, Policy.global_policy().gpu_env_str)
            self.assertEqual(group_member.desc.policy.gpu_affinity, Policy.global_policy().gpu_affinity)
            self.assertEqual(group_member.desc.policy.wait_mode, Policy.global_policy().wait_mode)
            self.assertEqual(group_member.desc.policy.refcounted, Policy.global_policy().refcounted)


if __name__ == "__main__":
    unittest.main()
