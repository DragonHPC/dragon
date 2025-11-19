"""
Run an MPI application with either a Cray PALS or PMIx backend
"""

import os
import argparse
from dragon.native.process import ProcessTemplate
from dragon.native.process_group import ProcessGroup
from dragon.infrastructure.facts import PMIBackend
from dragon.native.machine import System, Node
from dragon.infrastructure.policy import Policy


def iterative_execution(exe, pmi, ppn):
    my_alloc = System()
    node_list = my_alloc.nodes
    nnodes = len(node_list)

    args = []
    cwd = os.getcwd()

    pg1 = ProcessGroup(restart=False, pmi=pmi)

    pg1.add_process(nproc=nnodes * ppn + 1, template=ProcessTemplate(target=exe, args=args, cwd=cwd, env=None))

    pg1.init()

    for idx in range(4):
        print(f"  Replay {idx} of PMIx ProcessGroup", flush=True)
        pg1.start()
        pg1.join()

    pg1.close()

    return 0


def concurrent_execution(exe, pmi, ppn):
    my_alloc = System()
    node_list = my_alloc.nodes
    nnodes = len(node_list)

    args = []
    cwd = os.getcwd()

    pg1 = ProcessGroup(restart=False, pmi=pmi)
    pg2 = ProcessGroup(restart=False, pmi=pmi)

    pg1.add_process(nproc=nnodes * ppn + 1, template=ProcessTemplate(target=exe, args=args, cwd=cwd, env=None))
    pg2.add_process(nproc=nnodes * ppn, template=ProcessTemplate(target=exe, args=args, cwd=cwd, env=None))

    pg1.init()
    pg2.init()

    pg1.start()
    pg2.start()

    pg1.join()
    pg2.join()

    pg1.close()
    pg2.close()

    return 0


def swap_node(exe, pmi, ppn, i):
    my_alloc = System()
    node_list = my_alloc.nodes
    nnodes = len(node_list)

    if nnodes < 2:
        raise RuntimeError("This example requires at least 2 nodes to run")
    if i % 2 == 0:
        policies = [
            Policy(placement=Policy.Placement.HOST_NAME, host_name=Node(node_list[nid]).hostname)
            for nid in range(nnodes - 1)
        ]
    else:
        policies = [
            Policy(placement=Policy.Placement.HOST_NAME, host_name=Node(node_list[nnodes - nid - 1]).hostname)
            for nid in range(nnodes - 1)
        ]

    args = []
    cwd = os.getcwd()

    pg1 = ProcessGroup(restart=False, pmi=pmi)
    for policy in policies:
        pg1.add_process(nproc=ppn, template=ProcessTemplate(target=exe, args=args, cwd=cwd, env=None, policy=policy))

    pg1.init()
    pg1.start()
    pg1.join()
    pg1.close()

    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Execute an MPI Hello world for a given PMI backend")

    parser.add_argument("exe", type=str, help="Name of MPI executable to execute")
    parser.add_argument(
        "--pmi",
        type=PMIBackend.from_str,
        default=PMIBackend.CRAY,
        help="PMI backend for launch MPI app. Choices are 'cray' or 'pmix'",
    )
    parser.add_argument("--ppn", type=int, default=4, help="Number of processes to launch per node")
    args = parser.parse_args()

    for i in range(4):
        print(f"iteration {i}: node swap", flush=True)
        swap_node(args.exe, args.pmi, args.ppn + i, i)

    for i in range(4):
        print(f"iteration {i}: iterative_execution", flush=True)
        iterative_execution(args.exe, args.pmi, args.ppn + i)

    for i in range(4):
        print(f"iteration {i}: concurrent_execution", flush=True)
        concurrent_execution(args.exe, args.pmi, args.ppn + i)
