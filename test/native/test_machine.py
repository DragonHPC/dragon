import unittest
import os
import dragon

from dragon.utils import host_id


class TestMachineSingle(unittest.TestCase):
    def test_multinode_cpu_count(self):
        """Test the number of logical CPUs in the single node case"""

        the_cpu_count = os.cpu_count()

        dragons_cpu_count = dragon.native.machine.cpu_count()

        # Since we don't have multiple nodes, the count should equal our CPU count
        self.assertTrue(dragons_cpu_count == the_cpu_count)

    def test_node_list(self):
        """Test that we can get a list of the registered nodes"""

        hlist = dragon.globalservices.node.get_list()
        self.assertEqual(1, len(hlist))

    def test_node_class(self):

        mynode = dragon.native.machine.current()
        self.assertTrue(mynode.h_uid == host_id())


if __name__ == "__main__":
    unittest.main()
