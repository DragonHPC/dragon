import random
import time
import socket
import numpy as np
import os
import itertools
import argparse
import torch
import pandas as pd

import dragon
import multiprocessing as mp
from dragon.telemetry import Telemetry, SlowGPUDetector
from dragon.native.pool import Pool
from dragon.native.process import Process
from dragon.native.event import Event
from dragon.native.machine import System, Node
from dragon.infrastructure.policy import Policy
from dragon.data.ddict import DDict
from dragon.native.process import ProcessTemplate
from dragon.native.process_group import ProcessGroup


def gpu_thrasher(stop_event, gpu_vendor) -> float:
    dt = Telemetry()
    import torch

    if gpu_vendor == "Nvidia":
        matrix_size = 4000
    elif gpu_vendor == "AMD":
        matrix_size = 10000

    # Move tensors to GPU
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    C = torch.zeros(matrix_size, matrix_size, device=device)
    # Initialize the output tensor
    while not stop_event.is_set():
        # Perform the tensor contraction
        torch.cuda.synchronize()
        start = time.perf_counter()
        A = torch.randn((matrix_size, matrix_size), device=device)
        B = torch.randn((matrix_size, matrix_size), device=device)
        C = torch.matmul(A, B)
        torch.cuda.synchronize()
        end = time.perf_counter()
        dt.add_data("gpu-thrasher-time", end - start)


def launch_nvidia_mps_daemon(nodes: list) -> None:
    """A helper function to launch the nvidia-mps daemon on every node in the allocation.

    :param nodes: a list of native.machine.Node objects
    :type nodes: list
    """

    exe = "nvidia-cuda-mps-control"
    args = "-d"

    grp = ProcessGroup(ignore_error_on_exit=True)
    for node in nodes:
        temp_policy = Policy(placement=Policy.Placement.HOST_NAME, host_name=node.hostname)
        grp.add_process(nproc=1, template=ProcessTemplate(target=exe, args=args, policy=temp_policy))

    grp.init()
    grp.start()
    grp.join()
    grp.close()


def wigner_rand_matrix_eigs(args: tuple) -> int:
    """Compute a the eigenvalues of num_iters wigner random matrices.

    :param args: input args. including rank, num_iters, distributed dict,the gpu vendor device str, and other input args
    :type args: tuple
    :return: number of eigenvalues computed
    :rtype: int
    """
    # parse args
    rank, num_iters, dd, vendor_device_str, input_args = args

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    if vendor_device_str is not None:
        device_number = int(os.getenv(vendor_device_str, None))
    else:
        device_number = rank

    # map ranks to devices. most useful for debugging.
    dd[f"{socket.gethostname()}_{device_number}"] = rank

    # get matrix size.
    min_matrix_size, max_matrix_size = dd["matrix_sizes"]
    try:
        # if a restart, this key will exist
        matrix_size = dd[rank]
    except KeyError:
        # if it's the first time, pick a matrix size and save it for restart
        matrix_size = random.randint(min_matrix_size, max_matrix_size)
        dd[rank] = matrix_size

    # check if the array of eigenvalues exists, and if not, initialize it
    try:
        _ = dd[matrix_size]
    except KeyError:
        dd[matrix_size] = np.empty((0, 0), dtype=float)

    # initialize the number of completed iterations
    try:
        count = dd[f"{rank}_completed_iters"]
    except KeyError:
        count = 0

    # do main work
    while count < num_iters:
        # create wigner matrix
        m = torch.triu(torch.randn((matrix_size, matrix_size), device=device))
        mT = torch.transpose(torch.triu(m, diagonal=1), 0, 1)
        # compute eigenvalues
        eigvals, _ = torch.linalg.eigh(1 / (matrix_size ** (1 / 2)) * (m + mT))
        # increment iter
        count += 1
        dd[f"{rank}_completed_iters"] = count
        # append eigenvalues to list of my matrix sizes eigenvalues
        dd[matrix_size] = np.append(dd[matrix_size], eigvals.to("cpu").numpy())

    return matrix_size * count


def get_args():
    parser = argparse.ArgumentParser(description="Compute and plot eigenvalues of Wigner Matrices")

    parser.add_argument("--plot", action="store_true", help="plot eigenvalue histogram")

    my_args = parser.parse_args()

    return my_args


if __name__ == "__main__":

    args = get_args()

    dt = Telemetry()
    mp.set_start_method("dragon")

    # get list of nodes
    alloc = System()
    nodes = [Node(id) for id in alloc.nodes]

    # find the primary node and define it as sacred.
    # a list of nodes can also be used as sacred nodes, but for simplicity we use the primary node in this example
    sacred_node = alloc.primary_node.hostname
    print(f"Sacred node: {sacred_node}", flush=True)

    # determine if we're using cpus or gpus
    a_node = nodes[0]
    num_gpus = a_node.num_gpus
    gpu_vendor = a_node.gpu_vendor

    if a_node.num_gpus > 0:
        policies = alloc.gpu_policies()
        num_workers = len(policies)
    else:
        num_workers = a_node.num_cpus // 2
        policies = None

    # if we're using nvidia gpus and the mode is E. Process, start the mps daemon
    vendor_device_str = None
    if gpu_vendor == "Nvidia":
        launch_nvidia_mps_daemon(nodes)
        vendor_device_str = "CUDA_VISIBLE_DEVICES"
    elif gpu_vendor == "AMD":
        vendor_device_str = "ROCR_VISIBLE_DEVICES"

    # define ddict placement policy so that we can restart if sacred node is in set of nodes we restart on
    ddict_policy = Policy(placement=Policy.Placement.HOST_NAME, host_name=sacred_node)
    orc_ddict_policy = Policy(placement=Policy.Placement.HOST_NAME, host_name=sacred_node)

    # check if this is a restart and set args to allow restart if it is
    restarted = alloc.restarted
    n_nodes = 1
    managers_per_node = 1
    if restarted:
        ddict_policy = None

    # define ddict
    dd = DDict(
        n_nodes=n_nodes,
        managers_per_node=managers_per_node,
        total_mem=50 * 1024 * 1024 * 1024,
        managers_per_policy=1,
        policy=ddict_policy,
        orc_policy=orc_ddict_policy,
        name="coordination_dict",
        restart=restarted,
    )

    thrasher_policy = None
    for node in nodes:
        if node.hostname != sacred_node:
            thrasher_policy = Policy(
                placement=Policy.Placement.HOST_NAME, host_name=node.hostname, gpu_affinity=list(str(0))
            )
            break

    # if the thrasher is enabled it will slow down GPU 0 on a non-primary node
    if thrasher_policy is not None:
        stop_thrasher = Event()
        thrasher_proc = Process(
            target=gpu_thrasher,
            args=(
                stop_thrasher,
                gpu_vendor,
            ),
            policy=thrasher_policy,
        )
        thrasher_proc.start()

    heimdallr = SlowGPUDetector(restartable_ddicts=[dd])
    heimdallr.start()

    # define amount of computational work
    num_iters = 50
    if gpu_vendor == "Nvidia":
        dd["matrix_sizes"] = (8000, 10000)
    elif gpu_vendor == "AMD":
        dd["matrix_sizes"] = (1600, 2000)
        # dd["matrix_sizes"] = (4000, 5000)

    try:
        dd["max_num_workers"] = max(num_workers, dd["max_num_workers"])
    except KeyError:
        dd["max_num_workers"] = num_workers

    # start pool and submit main computational work
    pool = Pool(policy=policies, processes_per_policy=1)
    results = pool.map_async(
        wigner_rand_matrix_eigs,
        zip(
            [rank for rank in range(num_workers)],
            itertools.repeat(num_iters),
            itertools.repeat(dd),
            itertools.repeat(vendor_device_str),
            itertools.repeat(args),
        ),
    )

    # wait for all work to be done
    results.wait()

    heimdallr.stop()

    # compute the total number of eigenvalues
    num_eigs = 0
    for res in results.get():
        num_eigs += res
    print(f"Computed {num_eigs} eigenvalues", flush=True)

    # shutdown main workers and watcher
    pool.close()
    pool.join()

    if thrasher_policy is not None:
        stop_thrasher.set()
        thrasher_proc.join()

    # plot the eigenvalues
    if args.plot:
        keys = dd.keys()
        all_eigs = np.empty((0, 0), dtype=float)
        max_num_workers = dd["max_num_workers"]
        for k in keys:
            if not isinstance(k, str):
                if k > max_num_workers:
                    all_eigs = np.append(all_eigs, dd[k])

        from matplotlib import pyplot as plt

        plt.hist(all_eigs, bins=100)
        plt.title("Eigenvalues of Wigner Matrices")
        plt.xlabel("Values")
        plt.ylabel("Frequency")
        plt.show()
        plt.savefig("eigvals.png")

    # destroy the ddict without restart
    dd.destroy()
    # shutdown telemetry
    dt.finalize()
