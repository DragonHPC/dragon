"""This file contains multi-node tests for the process group classes."""

import os, sys
import unittest
import time
import random
import signal
import socket
from contextlib import ExitStack
import dragon


from dragon import ProcessGroup, ProcessTemplate, System, Policy
from dragon.native.process_group import State
from dragon.native.process import Process
from dragon.native.queue import Queue
from dragon.native.machine import Node

from dragon.globalservices.process import query as process_query, kill, signal as dragon_signal
from dragon.globalservices.node import get_list as node_get_list
from dragon.infrastructure.process_desc import ProcessOptions
from dragon.accelerators import Accelerator
from dragon.managed_memory import MemoryPool


def placement_info(q, vendor):
    hostname = socket.gethostname()
    pid = os.getpid()
    cpus_allowed_list = -1
    with open(f"/proc/{pid}/status") as f:
        for _, line in enumerate(f):
            split_line = line.split(":")
            if split_line[0] == "Cpus_allowed_list":
                cpus_allowed_list = split_line[1].strip("\n").strip("\t")
                break
    visible_devices = Accelerator().get_device_list()
    q.put(
        (
            hostname,
            cpus_allowed_list,
            visible_devices,
        )
    )


def gpu_uuid_info(q):
    hostname = socket.gethostname()
    try:
        accelerator = Accelerator()
        devices = accelerator.get_device_list()
        roots = sorted({int(device) for device in devices})
        uuids = [accelerator.get_device_uuid(device_id) for device_id in range(accelerator.get_device_count())]
        if len(roots) != len(uuids):
            raise RuntimeError("GPU device list does not match the physical GPU count")
        q.put((hostname, dict(zip(roots, uuids)), None))
    except Exception as ex:
        q.put((hostname, {}, f"{type(ex).__name__}: {ex}"))


def gpu_memory_copy_info(q):
    hostname = socket.gethostname()
    try:
        with ExitStack() as resources:
            pid = os.getpid()
            host_pool = MemoryPool(1024 * 1024, f"gpu_copy_host_{pid}", pid)
            resources.callback(host_pool.destroy)
            device_pool = MemoryPool(1024 * 1024, f"gpu_copy_device_{pid}", pid + 1, gpu_memory=True)
            resources.callback(device_pool.destroy)
            payload = bytes(range(256)) * 4
            source = host_pool.alloc(len(payload))
            resources.callback(source.free)
            source.get_memview()[:] = payload
            copied_data = []

            for iteration in range(2):
                if iteration == 1:
                    temporary_pool = MemoryPool(1024 * 1024, f"gpu_copy_temporary_{pid}", pid + 2, gpu_memory=True)
                    temporary_pool.destroy()
                with ExitStack() as allocations:
                    device = source.copy(device_pool)
                    allocations.callback(device.free)
                    destination = device.copy(host_pool)
                    allocations.callback(destination.free)
                    copied_data.append(bytes(destination.get_memview()))

            device_count = Accelerator().get_device_count()
        q.put((hostname, copied_data, device_count, None))
    except Exception as ex:
        q.put((hostname, [], 0, f"{type(ex).__name__}: {ex}"))


class TestProcessGroupMultiNode(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set parameters"""

        # a few standard values
        cls.nproc = System.cpu_count() * 2
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

    @unittest.skipIf(bool(os.getenv("DRAGON_PROXY_ENABLED", False)), "Ill-defined behavior in proxy mode")
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

            self.assertTrue(pg._state != State.ERROR)

            time.sleep(testtime // 10)

        puids = pg.puids

        pg.stop()

        while not pg._state == State.IDLE:
            self.assertTrue(pg._state != State.ERROR)

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

        while not pg._state == State.IDLE:
            self.assertFalse(pg._state == State.ERROR)

        stop = time.monotonic()

        self.assertAlmostEqual(stop - start, wtime, None, "Not within 1s tolerance for walltime test", 1)

        pg.stop()
        pg.close()

    def test_hostname_node_restriction(self):
        my_alloc = System()
        num_procs_per_node = 2
        num_nodes_to_use = int(my_alloc.nnodes / 2)
        node_list = my_alloc.nodes
        num_procs = num_nodes_to_use * num_procs_per_node
        q = Queue()
        gpu_vendor = None
        args = (q, gpu_vendor)
        cwd = os.getcwd()
        grp = ProcessGroup(restart=False)
        acceptable_hostnames = []

        # create a process group that runs on a subset of nodes
        for node_num in range(num_nodes_to_use):
            node_name = Node(node_list[node_num]).hostname
            local_policy = Policy(placement=Policy.Placement.HOST_NAME, host_name=node_name)
            grp.add_process(
                nproc=num_procs_per_node,
                template=ProcessTemplate(target=placement_info, args=args, cwd=cwd, policy=local_policy),
            )
            acceptable_hostnames.append(node_name)

        # init and start my process group
        grp.init()
        grp.start()

        count = 0
        while count < num_procs:
            hostname, _, _ = q.get()
            # check that proc is on a node it was meant to land on
            self.assertIn(
                hostname, acceptable_hostnames, msg=f"Got hostname {hostname} which is not in {acceptable_hostnames}"
            )
            count += 1

        # wait for workers to finish and shutdown process group
        grp.join()
        grp.stop()
        grp.close()

    def test_huid_node_restriction(self):
        my_alloc = System()
        num_procs_per_node = 2
        num_nodes_to_use = int(my_alloc.nnodes / 2)
        node_list = my_alloc.nodes
        num_procs = num_nodes_to_use * num_procs_per_node
        q = Queue()
        gpu_vendor = None
        args = (q, gpu_vendor)
        cwd = os.getcwd()
        grp = ProcessGroup(restart=False)
        acceptable_huids = []
        # create a process group that runs on a subset of nodes
        for index, huid in enumerate(node_list[:num_nodes_to_use]):
            node_huid = Node(node_list[index]).h_uid
            self.assertEqual(huid, node_huid, f"{huid} is not equal to {node_huid} from Node.h_uid")
            local_policy = Policy(placement=Policy.Placement.HOST_ID, host_id=huid)
            grp.add_process(
                nproc=num_procs_per_node,
                template=ProcessTemplate(target=placement_info, args=args, cwd=cwd, policy=local_policy),
            )
            acceptable_huids.append(huid)
        # init and start my process group
        grp.init()
        grp.start()

        count = 0
        while count < num_procs:
            hostname, _, _ = q.get()
            host_id = Node(hostname).h_uid
            # check that proc is on a node it was meant to land on
            self.assertIn(host_id, acceptable_huids, msg=f"Got hostname {host_id} which is not in {acceptable_huids}")
            count += 1
        grp.join()
        grp.stop()
        grp.close()

    def test_policy_hierarchy(self):
        my_alloc = System()
        num_procs_per_node = 2
        num_nodes_to_use = int(my_alloc.nnodes / 2)
        node_list = my_alloc.nodes
        num_procs = num_nodes_to_use * num_procs_per_node
        q = Queue()
        gpu_vendor = None
        args = (q, gpu_vendor)
        cwd = os.getcwd()
        global_policy = Policy(
            placement=Policy.Placement.HOST_NAME, host_name=Node(node_list[num_nodes_to_use]).hostname
        )
        grp = ProcessGroup(restart=False, policy=global_policy)
        acceptable_hostnames = []

        # create a process group that runs on a subset of nodes
        for node_num in range(num_nodes_to_use):
            node_name = Node(node_list[node_num]).hostname
            local_policy = Policy(placement=Policy.Placement.HOST_NAME, host_name=node_name)
            grp.add_process(
                nproc=num_procs_per_node,
                template=ProcessTemplate(target=placement_info, args=args, cwd=cwd, policy=local_policy),
            )
            acceptable_hostnames.append(node_name)

        # init and start my process group
        grp.init()
        grp.start()

        count = 0
        while count < num_procs:
            hostname, _, _ = q.get()
            # check that proc is on a node it was meant to land on
            self.assertIn(
                hostname, acceptable_hostnames, msg=f"Got hostname {hostname} which is not in {acceptable_hostnames}"
            )
            count += 1

        # wait for workers to finish and shutdown process group
        grp.join()
        grp.stop()
        grp.close()

    @unittest.skipIf(bool(os.getenv("DRAGON_PROXY_ENABLED", False)), "Fails in proxy mode")
    def test_block_distribution(self):
        my_alloc = System()
        num_procs = (
            System.cpu_count() // (2 * my_alloc.nnodes) - 1
        )  # We subtract one because the Manager is already on the head node
        q = Queue(maxsize=num_procs * 100)
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
                self.assertEqual(
                    hostname,
                    acceptable_hostname,
                    msg=f"Got hostname {hostname} on proc {count} which is not in {acceptable_hostname}",
                )
            else:
                acceptable_hostname = hostname
            count += 1
        # wait for workers to finish and shutdown process group
        grp.join()
        grp.close()

    def test_roundrobin_distribution(self):
        my_alloc = System()
        num_procs_per_node = 10
        num_procs = int(num_procs_per_node * my_alloc.nnodes)
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

        grp = ProcessGroup(restart=False)
        qs = {}
        correct_env_vars = {}
        for node in nodes.values():
            num_use_devices = int(node.num_gpus / 2)
            if num_use_devices > 0:
                devices_to_use = node.gpus[num_use_devices:]
                if node.gpu_vendor == "Intel":
                    devices_to_use[0] = int(devices_to_use[0])
                    del devices_to_use[1]
                correct_env_var = devices_to_use
            else:
                correct_env_var = None
                devices_to_use = ["5", "6"]

            correct_env_vars[node.hostname] = correct_env_var

            if num_use_devices == 0:
                self.assertEqual(node.gpu_vendor, None)
            else:
                self.assertIn(node.gpu_vendor, ["Nvidia", "AMD", "Intel"])

            q = Queue()
            qs[node.hostname] = q
            args = (q, node.gpu_vendor)
            cwd = os.getcwd()
            local_policy = Policy(
                placement=Policy.Placement.HOST_NAME, host_name=node.hostname, gpu_affinity=devices_to_use
            )
            grp.add_process(
                nproc=1, template=ProcessTemplate(target=placement_info, args=args, cwd=cwd, policy=local_policy)
            )

        # init and start my process group
        grp.init()
        grp.start()

        for hostname, q in qs.items():
            _, _, visible_devices = q.get()
            selected_devices = correct_env_vars[hostname]
            expected_devices = list(range(len(selected_devices))) if selected_devices is not None else []
            self.assertEqual(
                visible_devices,
                expected_devices,
                msg=f"Got visible devices {visible_devices} which is not equal to {expected_devices} for node {hostname}",
            )

        # wait for workers to finish and shutdown process group
        grp.join()
        grp.stop()
        grp.close()

    def test_gpu_affinity_uuid(self):
        nodes = [Node(node_id) for node_id in System().nodes]
        gpu_nodes = [node for node in nodes if node.num_gpus > 1]
        if not gpu_nodes:
            self.skipTest("No nodes with multiple GPUs available in this allocation")

        baseline_uuids = {}
        for phase in ("baseline", "affinity"):
            if phase == "affinity":
                gpu_nodes = [node for node in gpu_nodes if len(baseline_uuids[node.hostname]) > 1]
                if not gpu_nodes:
                    self.skipTest("At least two distinct physical GPUs are needed to check UUID affinity")

            q = Queue()
            grp = ProcessGroup(restart=False)
            for node in gpu_nodes:
                policy = Policy(
                    placement=Policy.Placement.HOST_NAME,
                    host_name=node.hostname,
                    gpu_affinity=[node.gpus[-1]] if phase == "affinity" else [],
                )
                grp.add_process(
                    nproc=1,
                    template=ProcessTemplate(target=gpu_uuid_info, args=(q,), cwd=self.cwd, policy=policy),
                )

            grp.init()
            try:
                grp.start()
                expected_nodes = {node.hostname: node for node in gpu_nodes}
                for _ in gpu_nodes:
                    hostname, uuids, error = q.get(timeout=60)
                    self.assertIn(hostname, expected_nodes)
                    node = expected_nodes.pop(hostname)
                    self.assertIsNone(error, f"GPU UUID query failed on {hostname}: {error}")
                    if phase == "baseline":
                        self.assertTrue(uuids, f"No GPU UUIDs reported on {hostname}")
                        self.assertEqual(len(set(uuids.values())), len(uuids), f"Duplicate GPU UUIDs on {hostname}")
                        baseline_uuids[hostname] = uuids
                    else:
                        selected_root = int(node.gpus[-1])
                        expected_uuid = baseline_uuids[hostname][selected_root]
                        self.assertEqual(
                            list(uuids.values()), [expected_uuid], f"GPU policy selected the wrong GPU on {hostname}"
                        )
                grp.join()
            finally:
                grp.stop()
                grp.close()
                q.destroy()

    def test_gpu_memory_copy(self):
        nodes = [Node(node_id) for node_id in System().nodes]
        gpu_nodes = [node for node in nodes if node.num_gpus > 0]
        if not gpu_nodes:
            self.skipTest("No GPUs available in this allocation")

        q = Queue()
        grp = ProcessGroup(restart=False)
        for node in gpu_nodes:
            policy = Policy(
                placement=Policy.Placement.HOST_NAME,
                host_name=node.hostname,
                gpu_affinity=[node.gpus[-1]],
            )
            grp.add_process(
                nproc=1,
                template=ProcessTemplate(target=gpu_memory_copy_info, args=(q,), cwd=self.cwd, policy=policy),
            )

        grp.init()
        try:
            grp.start()
            expected_hostnames = {node.hostname for node in gpu_nodes}
            payload = bytes(range(256)) * 4
            for _ in gpu_nodes:
                hostname, copied_data, device_count, error = q.get(timeout=60)
                self.assertIn(hostname, expected_hostnames)
                expected_hostnames.remove(hostname)
                self.assertIsNone(error, f"GPU copy failed on {hostname}: {error}")
                self.assertEqual(copied_data, [payload, payload], f"GPU copy corrupted data on {hostname}")
                self.assertEqual(device_count, 1, f"GPU affinity was not applied on {hostname}")
            grp.join()
        finally:
            grp.stop()
            grp.close()
            q.destroy()

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

        # init and start my process group
        grp.init()
        grp.start()

        count = 0
        while count < my_alloc.nnodes:
            _, cpus_list_allowed, _ = q.get()
            self.assertEqual(allowed_cpus, [int(x) for x in cpus_list_allowed.split(",")])
            count += 1

        # wait for workers to finish and shutdown process group
        grp.join()
        grp.stop()
        grp.close()


if __name__ == "__main__":
    unittest.main()
