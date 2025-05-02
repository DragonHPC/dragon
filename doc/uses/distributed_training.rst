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

    from dragon.native.process_group import ProcessGroup
    import dragon.ai.torch  # needs to be imported before torch to avoid multiprocessing conflicts

    import torch
    import torch.distributed as dist

    def training_fn():

        device = 'cuda:' + os.getenv("LOCAL_RANK")
        dist.init_process_group('nccl')
        torch.cuda.set_device(device)

        #### Do your training

        dist.destroy_process_group()


    def configure_training_group(training_fn, training_args: tuple = None, training_kwargs: dict = None):

        # Get the list of nodes available to the Dragon runtime
        my_alloc = System()
        node_list = my_alloc.nodes

        tasks_per_node = 4  # Set to the number of GPUs on a given node

        num_nodes = len(node_list)
        world_size = num_nodes * tasks_per_node  #

        master_node = node_list[0].host_name
        master_port = str(29500)

        pg = ProcessGroup()
        for node_rank, policy in range(num_nodes):
            for local_rank in range(tasks_per_node):
                rank = node_rank * self.tasks_per_node + local_rank

                env = dict(os.environ).copy()
                env["MASTER_ADDR"] = master_node
                env["MASTER_PORT"] = master_port
                env["RANK"] = str(rank)
                env["LOCAL_RANK"] = str(local_rank)
                env["WORLD_SIZE"] = str(self.world_size)
                env["LOCAL_WORLD_SIZE"] = str(self.tasks_per_node)
                env["GROUP_RANK"] = str(node_rank)

                template = ProcessTemplate(target=training_fn,
                                           args=training_args,
                                           kwargs=training_kwargs,
                                           env=env,
                                           policy=policy,
                                           stderr=stderr)

                pg.add_process(nproc=1, template=template)

        pg.init()
        pg.start()

        pg.join()
        pg.close()


Loading Training Data with PyTorch
==================================