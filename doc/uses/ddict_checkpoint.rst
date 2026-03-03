.. _ddict_checkpoint:


Resiliency with DDict Checkpointing
+++++++++++++++++++++++++++++++++++

The Dragon distribute dictionary, refered to as the :py:class:`~dragon.data.DDict`, provides APIs for users to add resiliency to their applications. The :py:class:`DDict.checkpoint() <dragon.data.ddict.DDict.checkpoint>` is the foundation that enables this capability. This feature allows users to create consistent snapshots of the DDict's state at specific points in time. This is particularly useful for applications that require fault tolerance, iterative computations, or long-running processes where intermediate results need to be saved and potentially restored later.

Some features of this tutorial are still experimental and under development. These features may not be ready for production use and could change in future releases. Please refer to the Dragon documentation and release notes for the most up-to-date information on the status of these features.

Checkpointing and Rolling Back
==============================

Checkpointing allows applications to save their state at specific points in time. :py:class:`DDict.rollback() <dragon.data.ddict.DDict.rollback>` allows users to roll back to a previous checkpoint if needed, enabling recovery from errors or failures. The example below demonstrates how to use the DDict to create checkpoints and roll back to previous states if corruption of the current state is detected.

.. code-block:: python
    :linenos:
    :caption: **Use DDict with checkpointing to compute average of samples with potential rollback if value is out of allowed range**

    import random
    import socket
    from dragon.native.machine import System
    from dragon.data.ddict import DDict
    from dragon.native.process_group import ProcessGroup
    from dragon.native.process import ProcessTemplate
    from dragon.native.barrier import Barrier
    import dragon.infrastructure.parameters as di_param


    def biased_sampler(ddict, barrier):

        my_puid = di_param.this_process.my_puid
        local_sample_agg = 0.0
        num_samples = 0
        rollback = 0

        while num_samples < 1000:
            # some work
            sample = random.normalvariate()
            local_sample_agg += sample
            num_samples += 1

            # condition suggesting state has been corrupted and we need to roll back
            if abs(sample) >= 4 and num_samples > 100:
                # checkpoint current state before rolling back
                ddict.checkpoint()
                rollback_chkpt = ddict.checkpoint_id - 1
                print(f"Process {my_puid} rolling back to checkpoint {rollback_chkpt}", flush=True)
                ddict.rollback()
                rollback += 1
                local_sample_agg = ddict[f"puid_{my_puid}"]
                num_samples = rollback_chkpt * 100
                continue

            # periodically checkpoint state of application
            if num_samples % 100 == 0:
                ddict[f"puid_{my_puid}"] = local_sample_agg
                ddict.checkpoint()  # proceed to the next checkpoint
                barrier.wait()


    if __name__ == "__main__":

        my_alloc = System()
        nnodes = my_alloc.nnodes
        procs_per_node = 30
        ddict = DDict(1, nnodes, nnodes * int(4 * 1024 * 1024), working_set_size=4)
        barrier = Barrier(nnodes * procs_per_node)
        pg = ProcessGroup()
        temp_proc = ProcessTemplate(target=biased_sampler, args=(ddict, barrier))
        pg.add_process(template=temp_proc, nproc=nnodes * procs_per_node)

        pg.init()
        pg.start()
        pg.join()

        avg = 0.0
        # update current process to the latest checkpoint
        ddict.sync_to_newest_checkpoint()
        for puid, _ in pg.inactive_puids:
            avg += ddict[f"puid_{puid}"]
        avg /= nnodes * procs_per_node * 1000
        print(f"Final average from all processes is {avg}", flush=True)
        pg.close()
        ddict.destroy()

Using Checkpointing to Persist Training State
=============================================

Checkpointing combined with persistence, allows users to save the state of their application to a more permanent storage solution, such as disk. This is particularly useful for long-running training jobs where hardware and software failures can occur. The example below demonstrates how to checkpoint and persist the state of distributed data parallel training process using a DDict.

.. code-block:: python
    :linenos:
    :caption: **Checkpoint model and optimzier state to DDict and persist state to disk**

    import dragon
    import getpass
    import os
    import argparse

    from dragon.ai.collective_group import CollectiveGroup, RankInfo
    from dragon.data.ddict import DDict, PosixCheckpointPersister, DAOSCheckpointPersister
    from dragon.native.machine import System

    import torch
    import torch.nn as nn
    import torch.optim as optim
    from torch.utils.data import DataLoader, TensorDataset
    from torch.utils.data.distributed import DistributedSampler
    import torch.distributed as dist
    from torch.nn.parallel import DistributedDataParallel as DDP


    class SimpleNN(nn.Module):
        def __init__(self, input_size, hidden_size, output_size):
            super(SimpleNN, self).__init__()
            self.fc1 = nn.Linear(input_size, hidden_size)
            self.relu = nn.ReLU()
            self.fc2 = nn.Linear(hidden_size, output_size)

        def forward(self, x):
            out = self.fc1(x)
            out = self.relu(out)
            out = self.fc2(out)
            return out


    def training_fn(ddict, restart):
        rank_info = RankInfo()
        rank = rank_info.my_rank
        local_rank = rank_info.my_local_rank
        master_addr = rank_info.master_addr
        master_port = rank_info.master_port
        world_size = rank_info.world_size
        print(
            f"Rank Info: rank {rank}, local_rank {local_rank}, master_addr {master_addr}, master_port {master_port}, world_size {world_size}"
        )

        dist.init_process_group(
            backend="nccl",
            init_method=f"tcp://{master_addr}:{master_port}",
            world_size=world_size,
            rank=rank,
        )

        input_size = 10
        hidden_size = 20
        output_size = 1
        num_samples = 100 * nnodes
        batch_size = 10
        learning_rate = 0.01
        num_epochs = 20
        device = torch.device(f"cuda:0")
        criterion = nn.MSELoss().to(device)

        if restart:
            # get globally checkpointed variables
            train_loader = ddict["train_loader"]
            # get my local ddict shard to recover local checkpoints
            my_manager_id = ddict.local_managers[local_rank]
            ddict = ddict.manager(my_manager_id)
            # get loader, model, optimizer state from ddict
            model = SimpleNN(input_size, hidden_size, output_size)
            model.load_state_dict(ddict[f"model_state_dict_{rank}"])
            optimizer = optim.Adam(model.parameters(), lr=learning_rate)
            optimizer.load_state_dict(ddict[f"optimizer_state_dict_{rank}"])

            # Move optimizer state to device
            for state in optimizer.state.values():
                for k, v in state.items():
                    if isinstance(v, torch.Tensor):
                        state[k] = v.to(device)

            ddict.checkpoint()  # proceed to the next checkpoint

            first_chkpt = ddict.checkpoint_id
            last_chkpt = first_chkpt + num_epochs
        else:
            # generate random data
            X_train = torch.randn(num_samples, input_size)
            y_train = torch.randn(num_samples, output_size)
            train_dataset = TensorDataset(X_train, y_train)
            train_loader = DataLoader(
                train_dataset, batch_size=batch_size, shuffle=False, sampler=DistributedSampler(train_dataset)
            )
            model = SimpleNN(input_size, hidden_size, output_size)
            optimizer = optim.Adam(model.parameters(), lr=learning_rate)

            # store loader to ddict. only needs to be done once with a persistent put
            if rank == 0:
                ddict.pput("train_loader", train_loader)
            optimizer = optim.Adam(model.parameters(), lr=learning_rate)

            first_chkpt = 0
            last_chkpt = num_epochs
            # get my local ddict so checkpoints are to node-local memory
            my_manager_id = ddict.local_managers[local_rank]
            ddict = ddict.manager(my_manager_id)

        # device ids is an index. we control gpu affinity using policy so each process only sees it's own GPU.
        model.to(device)
        model = DDP(model, device_ids=[0])

        print(f"Rank {rank} starting training", flush=True)
        # training
        for epoch in range(num_epochs):
            for i, (inputs, labels) in enumerate(train_loader):
                inputs = inputs.to(device)
                labels = labels.to(device)
                outputs = model(inputs)
                loss = criterion(outputs, labels)
                optimizer.zero_grad()
                loss.backward()
                optimizer.step()

            # checkpoint model and optimizer state
            ddict[f"model_state_dict_{rank}"] = model.module.state_dict()
            ddict[f"optimizer_state_dict_{rank}"] = optimizer.state_dict()
            # if every ddict manager may not get a key then using a small bput
            # can keep managers in sync for checkpointing. In this case we have as many managers as GPUs so each manager gets a key.
            # ddict.bput("epoch", epoch)
            ddict.checkpoint()
            if rank == 0:
                print(f"Epoch [{first_chkpt + epoch + 1}/{last_chkpt}], Loss: {loss.item():.4f}", flush=True)

        print(f"Rank {rank} finished training!", flush=True)
        dist.destroy_process_group()


    if __name__ == "__main__":

        parser = argparse.ArgumentParser(description="Training with DDict Checkpoint Persistence")
        parser.add_argument(
            "--restart",
            action="store_true",
            help="Whether to restart from the last persisted checkpoint",
        )
        args = parser.parse_args()

        my_alloc = System()
        nnodes = my_alloc.nnodes

        # Checkpointing parameters
        managers_per_node = my_alloc.primary_node.num_gpus
        working_set_size = 2
        persist_frequency = 2
        persist_count = 2
        persist_path = ""
        restart = True if args.restart or my_alloc.restarted else False
        persister = PosixCheckpointPersister  # switch to DAOSCheckpointPersister if using DAOS
        name = f"chkpt_persistence_example_{getpass.getuser()}"

        ddict = DDict(
            managers_per_node,
            nnodes,
            nnodes * int(4 * 1024 * 1024 * 1024),
            wait_for_keys=True,
            working_set_size=working_set_size,
            persister_class=persister,
            persist_freq=persist_frequency,
            persist_count=persist_count,
            persist_path=persist_path,
            name=name,
        )

        if restart:
            # if it's a restart, we recover the ddict and find the last persisted checkpoint
            available_persisted_chkpt = ddict.persisted_ids()
            print(f"available persisted checkpoints: {available_persisted_chkpt}", flush=True)

            # restore from the last complete checkpoint
            latest_chkpt = available_persisted_chkpt[-1]
            ddict.restore(latest_chkpt)

        # launch one training process per GPU
        num_gpus_per_node = my_alloc.primary_node.num_gpus
        policies = my_alloc.gpu_policies()
        training_group = CollectiveGroup(
            training_fn,
            training_args=(
                ddict,
                restart,
            ),
            policies=policies,
        )

        training_group.init()
        training_group.start()
        training_group.join()
        training_group.close()

        ddict.destroy()

In this example, all checkpoints are first written to node-local memory for performance. At a specified frequency, checkpoints are asynchronously persisted to disk using a chosen persister class. Upon restart, the application can restore from the last persisted checkpoint. Although in this example duplicate state was saved, this covers the more general case where each rank may have a shard of the state, like what occurs in fully sharded data parallel training or when running an ensemble with different hyperparameters. This replicated state can also provide an insurance that if a node fails, the system can recover from another node's in-memory checkpoint. In the next section we will cover how to automatically recover from failures using only in-memory checkpoints rather than persistent storage.

Recovering from Failures with In-Memory Checkpoints
===================================================

In progress...