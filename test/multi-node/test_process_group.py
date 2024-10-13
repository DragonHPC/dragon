"""This file contains multi-node tests for the process group classes.
"""
import os
import unittest
import time
import random
import signal
import socket

import dragon


from dragon.native.process_group import ProcessGroup
from dragon.native.process import ProcessTemplate, Process
from dragon.native.queue import Queue
from dragon.native.machine import cpu_count, System, Node

from dragon.globalservices.process import query as process_query, kill, signal as dragon_signal
from dragon.globalservices.node import get_list as node_get_list
from dragon.infrastructure.process_desc import ProcessOptions
from dragon.infrastructure.policy import Policy


def placement_info(q, vendor):
    hostname = socket.gethostname()
    pid = os.getpid()
    cpus_allowed_list = -1
    with open(f'/proc/{pid}/status') as f:
        for _, line in enumerate(f):
            split_line = line.split(':')
            if split_line[0] == "Cpus_allowed_list":
                cpus_allowed_list = split_line[1].strip('\n').strip('\t')
                break
    visible_devices = None
    if vendor == 'Nvidia':
        visible_devices = os.getenv("CUDA_VISIBLE_DEVICES")
    elif vendor == 'AMD':
        visible_devices = os.getenv("ROCR_VISIBLE_DEVICES")
    elif vendor == 'Intel':
        visible_devices = os.getenv("ZE_AFFINITY_MASK")

    q.put((hostname, cpus_allowed_list, visible_devices,))


class TestProcessGroupMultiNode(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set parameters"""

        # a few standard values
        cls.nproc = cpu_count() * 2
        cls.cmd = "sleep"
        cls.args = ("1000",)
        cls.cwd = os.getcwd()
        cls.options = ProcessOptions(make_inf_channels=True)
        cls.template = ProcessTemplate(cls.cmd, args=cls.args, cwd=cls.cwd)

    @unittest.skip("CIRRUS-1163: Will fail until process descriptor contains a h_uid/host_id.")
    def test_placement_roundrobin(self):

        pg = ProcessGroup(restart=False)
        pg.add_process(self.nproc, self.template)
        pg.init()

        pg.start()

        self.assertTrue(pg.status == "Running")

        puids = pg.puids  # list is ordered by placement policy !

        huids = node_get_list()

        for i, puid in enumerate(puids):

            idx = i % len(huids)

            p = Process(None, ident=puid)
            self.assertTrue(
                p.node == huids[i], f"{p.node=} {huids[idx]=} {idx=} "
            )  # will fail until placement is fixed to h_uid

        pg.kill()

        self.assertTrue(pg.status == "Idle")

        pg.stop()

        self.assertTrue(pg.status == "Stop")

    def test_maintain_stress(self):
        """This test may take a minute."""

        # we will keep killing processes, 10% will keep exiting on their own.

        testtime = 10  # sec

        pg = ProcessGroup(restart=True, ignore_error_on_exit=True, policy=Policy())

        count = 0
        for i in range(self.nproc):
            if i % 10 == 0:
                args = (f"{testtime/self.nproc * (i+1)}",)
                t = ProcessTemplate("sleep", args=args, cwd=self.cwd)
                pg.add_process(1, t)
            else:
                count += 1
        args = ("10000000",)
        t = ProcessTemplate("sleep", args=args, cwd=self.cwd)
        pg.add_process(count, t)

        pg.init()

        self._update_interval_sec = 0.1

        pg.start()

        beg = time.monotonic()

        while time.monotonic() - beg < testtime:

            puids = pg.puids

            try:
                puid = puids[random.randint(0, len(puids))]
                kill(puid, sig=dragon_signal.SIGKILL)
            except Exception:  # maybe it disappeared already
                pass

            self.assertTrue(pg._state != "Error")

            time.sleep(testtime // 10)

        puids = pg.puids

        pg.stop()

        while not pg._state == "Idle":
            self.assertTrue(pg._state != "Error")

        for puid in puids:
            gs_info = process_query(puid)
            self.assertTrue(gs_info.state == gs_info.State.DEAD)

        pg.close()

    def test_walltime(self):

        wtime = 3

        # small number of procs, so that the startup overhead is minimal
        nproc = len(node_get_list())

        pg = ProcessGroup(walltime=wtime, ignore_error_on_exit=True)
        pg.add_process(nproc, self.template)
        pg.init()

        pg.start()
        start = time.monotonic()

        while not pg._state == "Idle":
            self.assertFalse(pg._state == "Error")

        stop = time.monotonic()

        self.assertAlmostEqual(stop - start, wtime, None, "Not within 1s tolerance for walltime test", 1)

        pg.stop()
        pg.close()

    def test_hostname_node_restriction(self):
        my_alloc = System()
        num_procs_per_node = 2
        num_nodes_to_use = int(my_alloc.nnodes/2)
        node_list = my_alloc.nodes
        num_procs = num_nodes_to_use*num_procs_per_node
        q = Queue()
        gpu_vendor = None
        args = (q, gpu_vendor)
        cwd = os.getcwd()
        grp = ProcessGroup(restart=False)
        acceptable_hostnames = []

        # create a process group that runs on a subset of nodes
        for node_num in range(num_nodes_to_use):
            node_name = Node(node_list[node_num]).hostname
            local_policy = Policy(placement=Policy.Placement.HOST_NAME,host_name=node_name)
            grp.add_process(nproc=num_procs_per_node, template=ProcessTemplate(target=placement_info, args=args, cwd=cwd, policy=local_policy))
            acceptable_hostnames.append(node_name)

        # init and start my process group
        grp.init()
        grp.start()

        count = 0
        while count < num_procs:
            hostname, _, _ = q.get()
            # check that proc is on a node it was meant to land on
            self.assertIn(hostname, acceptable_hostnames, msg=f'Got hostname {hostname} which is not in {acceptable_hostnames}')
            count += 1

        # wait for workers to finish and shutdown process group
        grp.join()
        grp.stop()
        grp.close()

    def test_huid_node_restriction(self):
        my_alloc = System()
        num_procs_per_node = 2
        num_nodes_to_use = int(my_alloc.nnodes/2)
        node_list = my_alloc.nodes
        num_procs = num_nodes_to_use*num_procs_per_node
        q = Queue()
        gpu_vendor = None
        args = (q, gpu_vendor)
        cwd = os.getcwd()
        grp = ProcessGroup(restart=False)
        acceptable_huids = []
        # create a process group that runs on a subset of nodes
        for index, huid in enumerate(node_list[:num_nodes_to_use]):
            node_huid = Node(node_list[index]).h_uid
            self.assertEqual(huid, node_huid, f'{huid} is not equal to {node_huid} from Node.h_uid')
            local_policy = Policy(placement=Policy.Placement.HOST_ID,host_id=huid)
            grp.add_process(nproc=num_procs_per_node, template=ProcessTemplate(target=placement_info, args=args, cwd=cwd, policy=local_policy))
            acceptable_huids.append(huid)
        #init and start my process group
        grp.init()
        grp.start()

        count = 0
        while count < num_procs:
            hostname, _, _ = q.get()
            host_id = Node(hostname).h_uid
            # check that proc is on a node it was meant to land on
            self.assertIn(host_id, acceptable_huids, msg=f'Got hostname {host_id} which is not in {acceptable_huids}')
            count += 1
        grp.join()
        grp.stop()
        grp.close()


    def test_policy_hierarchy(self):
        my_alloc = System()
        num_procs_per_node = 2
        num_nodes_to_use = int(my_alloc.nnodes/2)
        node_list = my_alloc.nodes
        num_procs = num_nodes_to_use*num_procs_per_node
        q = Queue()
        gpu_vendor = None
        args = (q, gpu_vendor)
        cwd = os.getcwd()
        global_policy = Policy(placement=Policy.Placement.HOST_NAME,host_name=Node(node_list[num_nodes_to_use]).hostname)
        grp = ProcessGroup(restart=False, policy=global_policy)
        acceptable_hostnames = []

        # create a process group that runs on a subset of nodes
        for node_num in range(num_nodes_to_use):
            node_name = Node(node_list[node_num]).hostname
            local_policy = Policy(placement=Policy.Placement.HOST_NAME,host_name=node_name)
            grp.add_process(nproc=num_procs_per_node, template=ProcessTemplate(target=placement_info, args=args, cwd=cwd, policy=local_policy))
            acceptable_hostnames.append(node_name)

        # init and start my process group
        grp.init()
        grp.start()

        count = 0
        while count < num_procs:
            hostname, _, _ = q.get()
            # check that proc is on a node it was meant to land on
            self.assertIn(hostname, acceptable_hostnames, msg=f'Got hostname {hostname} which is not in {acceptable_hostnames}')
            count += 1

        # wait for workers to finish and shutdown process group
        grp.join()
        grp.stop()
        grp.close()

    def test_block_distribution(self):
        my_alloc = System()
        num_procs = cpu_count() // my_alloc.nnodes - 1  # We subtract one because the Manager is already on the head node
        q = Queue()
        gpu_vendor = None
        args = (q, gpu_vendor)
        cwd = os.getcwd()
        global_policy = Policy(distribution=Policy.Distribution.BLOCK)
        grp = ProcessGroup(restart=False, policy=global_policy)
        grp.add_process(nproc=num_procs, template=ProcessTemplate(target=placement_info, args=args, cwd=cwd))

        # init and start my process group
        grp.init()
        grp.start()

        count = 0
        acceptable_hostname = None
        while count < num_procs:
            hostname, _, _ = q.get()
            if count > 0:
                # check that proc is on a node it was meant to land on
                self.assertEqual(hostname, acceptable_hostname, msg=f'Got hostname {hostname} on proc {count} which is not in {acceptable_hostname}')
            else:
                acceptable_hostname = hostname
            count += 1
        # wait for workers to finish and shutdown process group
        grp.join()
        grp.stop()
        grp.close()

    def test_roundrobin_distribution(self):
        my_alloc = System()
        num_procs_per_node = 10
        num_procs = int(num_procs_per_node*my_alloc.nnodes)
        q = Queue()
        gpu_vendor = None
        args = (q, gpu_vendor)
        cwd = os.getcwd()
        global_policy = Policy(distribution=Policy.Distribution.ROUNDROBIN)
        grp = ProcessGroup(restart=False, policy=global_policy)
        grp.add_process(nproc=num_procs, template=ProcessTemplate(target=placement_info, args=args, cwd=cwd))

        # init and start my process group
        grp.init()
        grp.start()

        host_num_procs = {}
        count = 0
        while count < num_procs:
            hostname, _, _ = q.get()
            try:
                host_num_procs[hostname] += 1
            except KeyError:
                host_num_procs[hostname] = 1
            count += 1
        for val in host_num_procs.values():
            self.assertEqual(val, num_procs_per_node)

        # wait for workers to finish and shutdown process group
        grp.join()
        grp.stop()
        grp.close()

    def test_gpu_affinity(self):
        # there are a couple spots where we assume if a node has gpus that all do
        my_alloc = System()
        node_list = my_alloc.nodes
        nodes = {}
        has_gpus = False
        for node_id in node_list:
            node = Node(node_id)
            nodes[node.hostname] = node

        a_node = list(nodes.values())[0]
        num_use_devices = int(a_node.num_gpus/2)
        if num_use_devices > 0:
            devices_to_use = a_node.gpus[num_use_devices:]
            if a_node.gpu_vendor == 'Intel':
                devices_to_use[0] = int(devices_to_use[0])
                del devices_to_use[1]
            correct_env_var =""
            for i in devices_to_use:
                correct_env_var +=str(i)+','
            correct_env_var = correct_env_var.strip(',')
        else:
            correct_env_var = None
            # check that if we use define GPUs to use with non-available that the env string is set to None
            devices_to_use = ['5', '6']

        if num_use_devices == 0:
            self.assertEqual(a_node.gpu_vendor, None)
        else:
            self.assertIn(a_node.gpu_vendor, ['Nvidia', 'AMD', 'Intel'])
        q = Queue()
        args = (q, a_node.gpu_vendor)
        cwd = os.getcwd()
        global_policy = Policy(gpu_affinity=devices_to_use)
        grp = ProcessGroup(restart=False, policy=global_policy)
        grp.add_process(nproc=my_alloc.nnodes, template=ProcessTemplate(target=placement_info, args=args, cwd=cwd))

        # init and start my process group
        grp.init()
        grp.start()

        count = 0
        while count < my_alloc.nnodes:
            _, _, visible_devices = q.get()
            self.assertEqual(visible_devices, correct_env_var)
            count += 1

        # wait for workers to finish and shutdown process group
        grp.join()
        grp.stop()
        grp.close()

    def test_cpu_affinity(self):
        my_alloc = System()
        q = Queue()
        gpu_vendor = None
        args = (q, gpu_vendor)
        cwd = os.getcwd()
        allowed_cpus = [3, 8, 11, 13]
        group_policy = Policy(cpu_affinity=allowed_cpus)
        grp = ProcessGroup(restart=False, policy=group_policy)
        grp.add_process(nproc=my_alloc.nnodes, template=ProcessTemplate(target=placement_info, args=args, cwd=cwd))

        #init and start my process group
        grp.init()
        grp.start()

        count = 0
        while count < my_alloc.nnodes:
            _, cpus_list_allowed, _ = q.get()
            self.assertEqual(allowed_cpus, [int(x) for x in cpus_list_allowed.split(',')])
            count += 1

        # wait for workers to finish and shutdown process group
        grp.join()
        grp.stop()
        grp.close()


if __name__ == "__main__":
    unittest.main()
