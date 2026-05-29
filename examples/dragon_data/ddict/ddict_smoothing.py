"""
Distributed smoothing algorithm using DragonHPC DDict with checkpointing.

This example demonstrates a distributed 1D smoothing algorithm where an array is divided
into chunks and each chunk is processed by a separate worker process. The DDict is used
to store the chunk data and checkpointing allows for fault tolerance and resumability.

To run this example, you must use the 'dragon' command instead of 'python3' because
DragonHPC requires the runtime to be initialized:

    # Validate mode (no persistence):
    dragon ddict_smoothing.py --mode validate --size 128 --chunks 4 --iterations 10

    # Persist checkpoints:
    dragon ddict_smoothing.py --mode persist --size 128 --chunks 4 --iterations 10 --persist_path ./ddict_checkpoints

    # Restore and continue from persisted checkpoint:
    dragon ddict_smoothing.py --mode restore --resume_iterations 10 --persist_path ./ddict_checkpoints
"""
import dragon
import argparse
import multiprocessing as mp
from typing import Optional


import numpy as np

from dragon.data.ddict import DDict, PosixCheckpointPersister, DAOSCheckpointPersister
from dragon.native.machine import System


def build_ddict(args, persister_class=None):
    nnodes = System().nnodes
    kwargs = {
        "wait_for_keys": True,
        "working_set_size": args.working_set_size,
        "timeout": 200,
    }

    # [smooth-kwargs-start]
    if persister_class is not None:
        kwargs.update(
            {
                "persister_class": persister_class,
                "persist_freq": args.persist_frequency,
                "persist_count": args.persist_count,
                "persist_path": args.persist_path,
                "name": args.name,
            }
        )
    # [smooth-kwargs-end]

    return DDict(args.managers_per_node, nnodes, nnodes * int(4 * 1024 * 1024 * 1024), **kwargs)


def smooth_segment(segment: np.ndarray, left_val: Optional[float], right_val: Optional[float]) -> np.ndarray:
    if segment.size == 0:
        return segment

    left_pad = segment[0] if left_val is None else left_val
    right_pad = segment[-1] if right_val is None else right_val

    if segment.size == 1:
        return np.array([(left_pad + segment[0] + right_pad) / 3.0], dtype=segment.dtype)

    smoothed = segment.copy()
    smoothed[0] = (left_pad + segment[0] + segment[1]) / 3.0
    smoothed[-1] = (segment[-2] + segment[-1] + right_pad) / 3.0
    if segment.size > 2:
        smoothed[1:-1] = (segment[:-2] + segment[1:-1] + segment[2:]) / 3.0

    return smoothed


def worker_smooth(ddict: DDict, chunk_id: int, iterations: int, checkpoint_id: int, trace: bool) -> None:
    ddict.checkpoint_id = checkpoint_id
    num_chunks = ddict["num_chunks"]
    # Load the initial chunk data for this worker
    current = np.array(ddict[f"chunk_{chunk_id}"])

    for step in range(iterations):
        left_val = None
        right_val = None

        # [ start-data-sharing-ref ]
        if chunk_id > 0:
            left_segment = np.array(ddict[f"chunk_{chunk_id - 1}"])
            left_val = left_segment[-1]
        if chunk_id < num_chunks - 1:
            right_segment = np.array(ddict[f"chunk_{chunk_id + 1}"])
            right_val = right_segment[0]
        # [ end-data-sharing-ref ]

        # [start-compute-avg]
        current = smooth_segment(current, left_val, right_val)

        ddict.checkpoint()

        ddict[f"chunk_{chunk_id}"] = current
        # [end-compute-avg]

        if trace:
            print(
                f"chunk={chunk_id} iter={step} checkpoint={ddict.checkpoint_id} min={current.min():.5f} max={current.max():.5f}",
                flush=True,
            )

    ddict.detach()


def populate_initial_chunks(ddict: DDict, size: int, num_chunks: int) -> None:
    chunk_size = size // num_chunks
    if chunk_size * num_chunks != size:
        raise ValueError("size must be divisible by num_chunks")

    # [start-pput]
    # These two kv pairs do not change on each iteration. They are the same
    # across all iterations and as such are put into the DDict as persistent
    # kv pairs. This means they are not affected by checkpointing.
    ddict.pput("num_chunks", num_chunks)
    ddict.pput("chunk_size", chunk_size)
    # [end-pput]

    values = np.linspace(0.0, 1.0, size, dtype=np.float64)
    for chunk_id in range(num_chunks):
        start = chunk_id * chunk_size
        end = start + chunk_size
        ddict[f"chunk_{chunk_id}"] = values[start:end]


def collect_result(ddict: DDict) -> np.ndarray:
    num_chunks = ddict["num_chunks"]
    chunk_size = ddict["chunk_size"]
    result = np.zeros(num_chunks * chunk_size, dtype=np.float64)

    for chunk_id in range(num_chunks):
        result[chunk_id * chunk_size : (chunk_id + 1) * chunk_size] = np.array(ddict[f"chunk_{chunk_id}"])

    return result


def parse_args():
    parser = argparse.ArgumentParser(
        description="Distributed smoothing algorithm using DDict checkpointing",
        epilog="Note: Launch with 'dragon' command, not 'python3'"
    )
    parser.add_argument("--mode", choices=["validate", "persist", "restore"], default="validate")
    parser.add_argument("--size", type=int, default=128, help="Total length of the array to smooth")
    parser.add_argument("--chunks", type=int, default=4, help="Number of distributed chunks")
    parser.add_argument("--iterations", type=int, default=10, help="Number of smoothing iterations")
    parser.add_argument("--resume_iterations", type=int, default=10, help="Iterations to run after restore")
    parser.add_argument("--managers_per_node", type=int, default=1)
    parser.add_argument("--working_set_size", type=int, default=3)
    parser.add_argument("--persist_path", type=str, default="./ddict_checkpoints")
    parser.add_argument("--persist_count", type=int, default=5)
    parser.add_argument("--persist_frequency", type=int, default=1)
    parser.add_argument("--persister", choices=["POSIX", "DAOS"], default="POSIX")
    parser.add_argument("--trace", action="store_true")
    parser.add_argument("--name", type=str, default="ddict_smoothing_example")
    return parser.parse_args()


def main():
    args = parse_args()
    mp.set_start_method("dragon")

    if args.persister == "POSIX":
        persister_class = PosixCheckpointPersister
    else:
        persister_class = DAOSCheckpointPersister

    if args.mode == "validate":
        ddict = build_ddict(args)
        print("DDict built in validate mode, populating initial chunks...", flush=True)
        populate_initial_chunks(ddict, args.size, args.chunks)
        print("Initial chunks populated, starting smoothing iterations...", flush=True)
        num_iterations = args.iterations
        checkpoint_id = 0
    elif args.mode == "persist":
        ddict = build_ddict(args, persister_class=persister_class)
        populate_initial_chunks(ddict, args.size, args.chunks)
        num_iterations = args.iterations
        checkpoint_id = 0
    else:
        ddict = build_ddict(args, persister_class=persister_class)
        available = ddict.persisted_ids()
        if not available:
            raise RuntimeError("No persisted checkpoints found to restore.")
        checkpoint_id = available[-1]
        print(f"Restoring persisted checkpoint {checkpoint_id}", flush=True)
        ddict.restore(checkpoint_id)
        num_iterations = args.resume_iterations

    print(
        f"Starting DDict smoothing: mode={args.mode} chunks={args.chunks} iterations={num_iterations} checkpoint={ddict.checkpoint_id}",
        flush=True,
    )

    processes = []
    for chunk_id in range(args.chunks):
        proc = mp.Process(target=worker_smooth, args=(ddict, chunk_id, num_iterations, checkpoint_id, args.trace))
        proc.start()
        processes.append(proc)

    for proc in processes:
        proc.join()

    ddict.sync_to_newest_checkpoint()
    result = collect_result(ddict)

    print(
        f"Finished smoothing mode={args.mode} checkpoint={ddict.checkpoint_id} result min={result.min():.6f} max={result.max():.6f}",
        flush=True,
    )

    if args.mode == "persist":
        print(f"Persisted checkpoint ids: {ddict.persisted_ids()}", flush=True)
    elif args.mode == "restore":
        print(f"Restored to checkpoint {checkpoint_id} and continued smoothing.", flush=True)

    ddict.destroy()


if __name__ == "__main__":
    main()
