.. _distributed_training:

Distributed PyTorch
+++++++++++++++++++


Launching PyTorch Distributed Training with Dragon
==================================================

PyTorch distributed training needs each worker to agree on three things before
the first collective call happens: how many ranks are participating, which rank
the current worker owns, and how all ranks will find the same rendezvous
address. Dragon helps with that setup, but two different layers are involved.

:py:class:`~dragon.native.process_group.ProcessGroup` is Dragon's general native
process orchestration API. It gives you explicit control over placement and
lifecycle, and it is the right fit when you are launching mixed workloads or
building a lower-level orchestration layer yourself.

:py:func:`~dragon.ai.collective_group.CollectiveGroup` is the higher-level
helper used in the examples below. It builds on Dragon's runtime services to
launch one training worker per policy, provides a
:py:class:`~dragon.ai.collective_group.RankInfo` object inside each worker, and
supplies the rank, world size, and rendezvous information that PyTorch needs
for ``torch.distributed.init_process_group``.

If your goal is "launch one distributed training worker per GPU and initialize
PyTorch correctly," start with ``CollectiveGroup``. Reach for ``ProcessGroup``
when you need more manual control over how those workers are created or placed.

.. code-block:: python
    :linenos:
    :caption: **Setting up NCCL backend for PyTorch Distributed Training with CollectiveGroup**

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

    if __name__ == "__main__":

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

Distributed training requires each rank to consume a different, non-overlapping shard of the
training dataset. PyTorch's :py:class:`~torch.utils.data.distributed.DistributedSampler` handles
this automatically: it partitions the dataset indices across ``world_size`` ranks so that every
GPU receives a unique slice of each epoch's data.

When using Dragon's :py:func:`~dragon.ai.collective_group.CollectiveGroup`, the
:py:class:`~dragon.ai.collective_group.RankInfo` helper provides the rank, world size, and master
address that :py:class:`~torch.utils.data.distributed.DistributedSampler` needs. No manual
rank-file management is required.

.. code-block:: python
    :linenos:
    :caption: **Distributed DataLoader with DistributedSampler inside a CollectiveGroup worker**

    from dragon.ai.collective_group import CollectiveGroup, RankInfo
    from dragon.native.machine import System

    import torch
    import torch.distributed as dist
    from torch.utils.data import DataLoader, TensorDataset
    from torch.utils.data.distributed import DistributedSampler


    def training_fn():
        rank_info = RankInfo()
        rank        = rank_info.my_rank
        world_size  = rank_info.world_size
        master_addr = rank_info.master_addr
        master_port = rank_info.master_port

        dist.init_process_group(
            backend="nccl",
            init_method=f"tcp://{master_addr}:{master_port}",
            world_size=world_size,
            rank=rank,
        )

        # Build a synthetic dataset — replace with your real dataset
        num_samples = 10_000
        input_size  = 128
        X = torch.randn(num_samples, input_size)
        y = torch.randint(0, 10, (num_samples,))
        dataset = TensorDataset(X, y)

        # DistributedSampler ensures each rank sees a unique shard of the dataset
        sampler = DistributedSampler(
            dataset,
            num_replicas=world_size,
            rank=rank,
            shuffle=True,
            drop_last=True,
        )
        loader = DataLoader(dataset, batch_size=64, sampler=sampler, num_workers=0)

        # Call sampler.set_epoch(epoch) at the start of each epoch so that shuffling
        # differs between epochs across all ranks.
        for epoch in range(5):
            sampler.set_epoch(epoch)
            for batch_X, batch_y in loader:
                # Move data to this rank's GPU (Policy already sets GPU affinity)
                batch_X = batch_X.to("cuda")
                batch_y = batch_y.to("cuda")
                # ... forward pass, loss, backward, optimizer step ...

            if rank == 0:
                print(f"Epoch {epoch + 1} complete", flush=True)

        dist.destroy_process_group()


    if __name__ == "__main__":
        gpu_policies = System().gpu_policies()
        pg = CollectiveGroup(
            training_fn=training_fn,
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

Key points:

* **One DataLoader per rank** — each rank constructs its own DataLoader backed by the same
  full dataset. The :py:class:`~torch.utils.data.distributed.DistributedSampler` partitions
  the indices so that ranks never load the same sample within an epoch.
* **``sampler.set_epoch(epoch)``** — call this at the start of every epoch to ensure that the
  random shuffle seeds differ between epochs while remaining consistent across ranks.
* **GPU affinity** — Dragon's :py:class:`~dragon.infrastructure.policy.Policy` already pins
  each worker to its own GPU, so ``torch.device("cuda")`` always refers to the correct device.
