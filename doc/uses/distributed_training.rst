.. _distributed_training:

Distributed PyTorch
+++++++++++++++++++


Using ProcessGroup for PyTorch Distributed Training
===================================================

Any time you want to do distributed training on GPUs with PyTorch, there is necessary configuration
to the PyTorch backend. Doing that with ProcessGroup is straightforward albeit unslightly as is always
the case for distributed training. Future work will provide helper classes to complete most standard
configurations. In the meantime, given some PyTorch function designed for distributed training `training_fn`,
these code snippets will aid in using :py:class:`~dragon.native.process_group.ProcessGroup` with a CUDA backend.

.. code-block:: python
    :linenos:
    :caption: **Setting up NCCL backend for PyTorch Distributed Training with ProcessGroup**

    from dragon.native.machine import System
    from dragon.ai.collective_group import CollectiveGroup, RankInfo

    import torch
    import torch.distributed as dist


    def train():
        rank_info = RankInfo()
        rank = rank_info.my_rank
        master_addr = rank_info.master_addr
        master_port = rank_info.master_port
        world_size = rank_info.world_size

        dist.init_process_group(
            backend="nccl",
            init_method=f"tcp://{master_addr}:{master_port}",
            world_size=world_size,
            rank=rank,
        )

        device = torch.device("cuda")  # the provided Policy already sets which GPU id to use
        tensor = torch.ones(1, device=device) * rank
        dist.all_reduce(tensor, op=dist.ReduceOp.SUM)

        print(f"Rank {rank}: Tensor after all_reduce = {tensor.item()}")

        dist.destroy_process_group()

    if __name__ == "main":

        gpu_policies = System().gpu_policies()
        pg = CollectiveGroup(
                training_fn=train,
                training_args=None,
                training_kwargs=None,
                policies=gpu_policies,
                hide_stderr=False,
                port=29500,
            )
        pg.init()
        pg.start()
        pg.join()
        pg.close()


Loading Training Data with PyTorch
==================================

Coming soon...