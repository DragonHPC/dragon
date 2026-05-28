"""pytorch_matmul_comm.py - Dragon telemetry example

Launches a Dragon CollectorGroup of PyTorch worker processes.  Each rank independently runs two phases per iteration:

  Phase 1 – matmul:    C = A @ B   where A, B ∈ R^{matrix_size × matrix_size}
  Phase 2 – allreduce: torch.distributed all_reduce on a 1-D tensor of
                        ``comm_size`` float32 elements

Both phases are accumulated separately in a ``TimeKeeper``.  When the Dragon
runtime is started with ``--telemetry-level ≥ 1``, the TimeKeeper background
thread automatically streams the per-phase accumulated durations to the
telemetry TSDB on the local node every ``collection_window`` seconds.

Usage
-----
    dragon --telemetry-level=3 pytorch_matmul_comm.py --matrix_size 4096
"""

import dragon
import multiprocessing as mp
import torch
import torch.distributed as dist
import argparse
import socket


from dragon.ai.collective_group import CollectiveGroup, RankInfo
from dragon.utils import TimeKeeper
from dragon.infrastructure.util import get_port
from dragon.native.machine import System, Node
from dragon.infrastructure.policy import Policy
from dragon.telemetry import Telemetry


def get_args():
    parser = argparse.ArgumentParser(
        description="Matrix multiply vs. allreduce timing with Dragon telemetry",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--matrix_size",
        type=int,
        default=1000,
        help="N for the N×N square matrix multiply",
    )
    parser.add_argument(
        "--comm_size",
        type=int,
        default=None,
        help="number of elements of C (flattened) to allreduce per round; "
        "defaults to matrix_size**2 (the full matrix). "
        "Must be <= matrix_size**2.",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=20,
        help="number of timed iterations per worker",
    )
    parser.add_argument(
        "--burns",
        type=int,
        default=5,
        help="warm-up iterations discarded before timing begins",
    )
    parser.add_argument(
        "--num_workers",
        type=int,
        default=None,
        help="total number of worker processes; defaults to one per allocated node",
    )
    parser.add_argument(
        "--collection_window",
        type=float,
        default=1.0,
        help="seconds between TimeKeeper telemetry flushes",
    )
    return parser.parse_args()


def worker(
    matrix_size: int,
    comm_size: int,
    iterations: int,
    burns: int,
    collection_window: float,
) -> None:
    """Run one distributed rank: time matmul and allreduce separately.

    :param matrix_size: N for the N×N matmul
    :param comm_size: length of the 1-D tensor passed to all_reduce
    :param iterations: number of timed rounds after warm-up
    :param burns: number of warm-up rounds whose timings are discarded
    :param collection_window: seconds between TimeKeeper telemetry flushes
    """
    rank_info = RankInfo()
    rank = rank_info.my_rank
    world_size = rank_info.world_size
    master_addr = rank_info.master_addr
    master_port = rank_info.master_port
    hostname = socket.gethostname()

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    backend = "nccl" if device.type == "cuda" else "gloo"

    dist.init_process_group(
        backend=backend,
        init_method=f"tcp://{master_addr}:{master_port}",
        world_size=world_size,
        rank=rank,
    )

    # Telemetry is discovered automatically from DRAGON_TELEMETRY_LEVEL.
    # If telemetry is not active (level == 0) no background thread is
    # started and no lock overhead is incurred. The timekeeper still collects
    # data locally and can be queried for analysis after the fact, but it won't
    # be streamed to telemetry.
    tk = TimeKeeper(
        timekeeper_name="matmul_vs_allreduce",
        recording=True,
        collection_window=collection_window,
    )

    # allocate tensors once so as to not create on every iteration
    A = torch.randn(matrix_size, matrix_size, device=device)
    B = torch.randn(matrix_size, matrix_size, device=device)

    def sync():
        """Ensure GPU kernels finish before stopping the clock."""
        if device.type == "cuda":
            torch.cuda.synchronize()

    for i in range(burns + iterations):

        sync()
        tic = tk.now()
        C = torch.matmul(A, B)
        sync()
        tk.add("matmul", tic)

        sync()
        tic = tk.now()
        dist.all_reduce(C.view(-1)[:comm_size], op=dist.ReduceOp.SUM)
        sync()
        tk.add("allreduce", tic)

        # Discard accumulated warm-up data after the last burn iteration
        if i == burns - 1:
            tk.reset_all()

    print(
        f"rank={rank:>3d}  host={hostname:<20s}  backend={backend}"
        f"  matrix={matrix_size}x{matrix_size}  comm_size={comm_size}",
        flush=True,
    )

    # Stop the telemetry background thread cleanly before process exits
    tk.stop()
    dist.destroy_process_group()


if __name__ == "__main__":
    args = get_args()
    comm_size = args.comm_size if args.comm_size is not None else args.matrix_size**2
    if comm_size > args.matrix_size**2:
        raise ValueError(
            f"--comm_size {comm_size} exceeds the number of elements in C "
            f"({args.matrix_size}**2 = {args.matrix_size ** 2})"
        )

    mp.set_start_method("dragon")
    dt = Telemetry()

    alloc = System()
    nodes = [Node(node_id) for node_id in alloc.nodes]
    num_nodes = len(nodes)

    a_node = nodes[0]
    if a_node.num_gpus > 0:
        policies = alloc.gpu_policies()
    else:
        num_workers = args.num_workers if args.num_workers is not None else num_nodes
        policies = [
            Policy(placement=Policy.Placement.HOST_NAME, host_name=nodes[r % num_nodes].hostname)
            for r in range(num_workers)
        ]

    world_size = len(policies)
    port = get_port()

    print(
        f"\nStarting pytorch_matmul_comm"
        f"  world_size={world_size}"
        f"  nodes={num_nodes}"
        f"  matrix={args.matrix_size}x{args.matrix_size}"
        f"  comm_size={comm_size}"
        f"  iterations={args.iterations}"
        f"  burns={args.burns}"
        f"  port={port}\n",
        flush=True,
    )

    # All ranks receive the same application args; distributed metadata
    # (rank, world_size, master_addr/port) is injected by CollectiveGroup
    # and read inside the worker via RankInfo().
    worker_args = (
        args.matrix_size,
        comm_size,
        args.iterations,
        args.burns,
        args.collection_window,
    )

    grp = CollectiveGroup(
        training_fn=worker,
        training_args=worker_args,
        policies=policies,
        port=port,
    )

    grp.init()
    grp.start()
    grp.join()
    grp.close()

    print("\nAll workers done.", flush=True)
    dt.shutdown()
