"""
Simple PyTorch DDP Smoke Example with Dragon ProcessGroup and NCCL Backend

This script demonstrates how to use the Dragon ProcessGroup API to launch a multiprocess distributed PyTorch training job with NCCL backend.

Key Features:
NCCL Backend Initialization
Per-Process Computation and All-Reduce
Dragon ProcessGroup Launch

How to run:
salloc 1 node with allgriz
Please make sure Numpy 1.26 is installed.
pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu118
"""

import os
import torch
import torch.distributed as dist
from dragon.native.process_group import ProcessGroup, RankInfo


def train(local_rank, world_size):
    rank_info = RankInfo()
    rank = rank_info.my_rank
    master_addr = rank_info.master_addr
    master_port = rank_info.master_port

    dist.init_process_group(
        backend="nccl",
        init_method=f"tcp://{master_addr}:{master_port}",
        world_size=world_size,
        rank=rank,
    )

    device = torch.device(f"cuda:{local_rank}")
    tensor = torch.ones(1, device=device) * rank
    dist.all_reduce(tensor, op=dist.ReduceOp.SUM)

    print(f"Rank {rank}: Tensor after all_reduce = {tensor.item()}")

    dist.destroy_process_group()


def dragon_training_fn():
    rank_info = RankInfo()
    train(rank_info.my_local_rank, rank_info.my_world_size)


if __name__ == "__main__":
    pg = ProcessGroup.configure_training_group(
        training_fn=dragon_training_fn,
        training_args=None,
        training_kwargs=None,
        ppn=1,
        nprocs=1,
        hide_stderr=False,
        port=29500,
    )

    pg.init()
    pg.start()
    pg.join()
    pg.close()
