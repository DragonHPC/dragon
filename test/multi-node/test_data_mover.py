import numpy as np
import cupy as cp
import os
import unittest

from dragon.workflows.data_mover import DataMovers, CuPyDataMover, NumPyDataMover
from dragon.managed_memory import MemoryAlloc
from dragon.native.process_group import ProcessGroup
from dragon.native.process import ProcessTemplate, Process
from dragon.infrastructure.policy import Policy
from dragon.native.machine import System, Node


def data_mover_simple_lifecycle(manager_policy=None, policies=None):

    device = int(os.getenv("CUDA_VISIBLE_DEVICES"))  # should be set by the policy or defaults to the entire list
    cp.cuda.runtime.setDevice(0)

    if policies is None:
        num_workers = 1
    else:
        num_workers = None
    data = np.random.rand(1000, 1000)
    movers_on = DataMovers(
        data_mover=NumPyDataMover,
        data_mover_args={"pool_size": 1024**3},
        num_workers=num_workers,
        manager_policy=manager_policy,
        policies=policies,
        num_workers_per_policy=1,
    )
    movers_off = DataMovers(
        data_mover=CuPyDataMover,
        data_mover_args={"pool_size": 1024**3},
        num_workers=num_workers,
        manager_policy=manager_policy,
        policies=policies,
        num_workers_per_policy=1,
    )
    movers_on.start()
    movers_off.start()
    input_queue, moved_descriptor_queue = movers_on.get_queues()
    input_descriptor_queue, output_queue = movers_off.get_queues()
    input_queue.put(data)
    cp_array, moved_descriptor = moved_descriptor_queue.get()
    plus_one = cp_array + 1
    cp.cuda.runtime.deviceSynchronize()
    NumPyDataMover.free_alloc(moved_descriptor)
    input_descriptor_queue.put(plus_one)
    cp.cuda.runtime.deviceSynchronize()

    output = output_queue.get()

    # checking diff on GPU
    data2 = cp.asarray(data)
    diff_gpu = plus_one - data2
    ones_gpu = cp.ones(diff_gpu.shape, dtype=diff_gpu.dtype)
    if cp.any(cp.absolute(diff_gpu - ones_gpu) > 1e-8):
        raise ValueError(f"Expected all elements to be 1")

    # checking diff on CPU
    diff_cpu = output - data
    ones_cpu = np.ones(diff_cpu.shape, dtype=diff_cpu.dtype)
    if np.any(np.absolute(diff_cpu - ones_cpu) > 1e-8):
        raise ValueError(f"Expected all elements to be 1")

    movers_on.stop()
    movers_off.stop()


class TestDataMovers(unittest.TestCase):

    def test_data_mover_multi_node_multi_gpu(self):
        my_alloc = System()
        node_list = my_alloc.nodes
        nodes = {}
        for node_id in node_list:
            node = Node(node_id)
            nodes[node.hostname] = node

        a_node = list(nodes.values())[0]
        self.assertIn(a_node.gpu_vendor, ["Nvidia"], "This test requires NVIDIA GPUs")
        test_procs = []
        for hostname, node in nodes.items():
            for device in node.gpus:
                policy = Policy(placement=Policy.Placement.HOST_NAME, host_name=hostname, gpu_affinity=[device])
                proc = Process(target=data_mover_simple_lifecycle, args=(policy, [policy]), policy=policy)
                proc.start()
                test_procs.append(proc)

        for proc in test_procs:
            proc.join()
            self.assertEqual(proc.returncode, 0, f"Process {proc.puid} failed with exit code {proc.returncode}")


if __name__ == "__main__":
    unittest.main()
