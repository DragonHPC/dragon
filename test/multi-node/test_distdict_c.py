import os
import sys
import unittest
import dragon
from dragon.native.process import Popen
from dragon.data.ddict.ddict import DDict
from dragon.native.machine import System, Node
from dragon.infrastructure.policy import Policy
from dragon.infrastructure.facts import DRAGON_LIB_DIR
import multiprocessing as mp

ENV = dict(os.environ)
ENV["LD_LIBRARY_PATH"] = str(DRAGON_LIB_DIR) + ":" + str(ENV.get("LD_LIBRARY_PATH", ""))

class TestDDictC(unittest.TestCase):
    def test_attach_detach(self):
        my_alloc = System()
        nnodes = my_alloc.nnodes
        exe = os.path.abspath("c_ddict")
        ddict = DDict(1, nnodes, 3000000, trace=True)
        ser_ddict = ddict.serialize()

        proc = Popen(executable=exe, args=[ser_ddict, "test_attach_detach"], env=ENV)
        proc.wait()
        ddict.destroy()
        self.assertEqual(proc.returncode, 0, "C client exited with non-zero exit code")

    def test_manager_placement(self):
        # create policy
        my_alloc = System()
        node_list = my_alloc.nodes

        policies = []

        # have only one manager on the last node
        node_id = node_list[-1]
        node = Node(node_id)
        policy = Policy(placement=Policy.Placement.HOST_NAME, host_name=node.hostname)
        policies.append(policy)

        # note that we're going to ignore what it says for number of nodes.
        exe = os.path.abspath("c_ddict")
        # one manager on sencond node, we want to test when c client is on different node.
        ddict = DDict(None, None, 1 * 1024 * 1024 * 1024, managers_per_policy=1, policy=policies, trace=True)
        ser_ddict = ddict.serialize()
        proc = Popen(executable=exe, args=[ser_ddict, "test_manager_placement"], env=ENV)
        proc.wait()
        ddict.destroy()
        self.assertEqual(proc.returncode, 0, "C client exited with non-zero exit code")

    def test_local_managers(self):
        exe = os.path.abspath("c_ddict")
        my_alloc = System()
        nnodes = my_alloc.nnodes
        ddict = DDict(4, nnodes, 3000000, trace=True)
        local_managers = ddict.local_managers
        ser_ddict = ddict.serialize()

        arg_list = [ser_ddict, "test_local_managers"]
        arg_list.append(len(local_managers))

        proc = Popen(executable=exe, args=arg_list, env=ENV)
        proc.wait()
        ddict.destroy()
        self.assertEqual(proc.returncode, 0, "C client exited with non-zero exit code")


class TestDDictCPP(unittest.TestCase):
    def test_local_keys(self):
        exe = os.path.abspath("cpp_ddict")
        my_alloc = System()
        nnodes = my_alloc.nnodes
        num_managers = 2 * nnodes
        ddict = DDict(2, nnodes, 3000000*num_managers, trace=True)
        ser_ddict = ddict.serialize()

        proc = Popen(executable=exe, args=[ser_ddict, "test_local_keys", num_managers], env=ENV)
        proc.wait()
        ddict.destroy()
        self.assertEqual(proc.returncode, 0, "CPP client exited with non-zero exit code")


if __name__ == "__main__":
    mp.set_start_method("dragon")
    unittest.main()
