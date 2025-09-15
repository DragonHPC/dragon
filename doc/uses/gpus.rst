.. _gpu_affinity:

Controlling GPU Affinity
++++++++++++++++++++++++

This tutorial will draw from the :py:class:`~dragon.infrastructure.policy.Policy` documentation to walk through how
to use :py:class:`~dragon.native.process_group.ProcessGroup` to set what GPUs to use for a given function or process as
well as how to do it with :py:class:`~dragon.native.Pool`.



Policies via the System API
===========================

The simplest way to create a list of :py:class:`Policies <dragon.infrastructure.policy.Policy>` for each GPU on every
node Dragon is running on is with the :py:meth:`~dragon.native.machine.System.gpu_policies` method. In the
example below, we'll apply each :py:class:`~dragon.infrastructure.policy.Policy` to 4 processes in a
:py:class:`~dragon.native.pool.Pool`.

.. code-block:: python
    :linenos:
    :caption: **Create a list of Policies specifying GPU affinity**

    from dragon.native.machine import System
    from dragon.native.pool import Pool


    def gpu_work(item):
        # GPU processing code, such as PyTorch or CuPy

    gpu_policies = System().gpu_policies()
    nworkers = 4 * len(gpu_policies)
    p = Pool(policy=policies, processes_per_policy=4)

    results = p.map_async(gpu_work, range(100)).get()
    print(f"{nworkers} workers say: {results}", flush=True)

    p.close()
    p.join()
    return results


Manually Derived Policies
=========================

To leverage full control over generating policies and applying them to processes, the first place to start is to ask
Dragon about the infrastructure it is running on and what GPUs are available. The
most direct way to access this information is through :py:class:`~dragon.native.machine.System` and
:py:class:`~dragon.native.machine.Node`. The example below shows how to use :py:class:`~dragon.native.machine.System`
and :py:class:`~dragon.native.machine.Node` to manually inspect each node that
Dragon is running on and create a list of hostnames and GPU IDs. You can of course add any other logic you like
to focus on just a subset of the nodes or filter by other criteria about a node.

.. code-block:: python
    :linenos:
    :caption: **Scan all nodes for GPUs and create a list of tuples containing hostname and GPU ID**

    from dragon.native.machine import System, Node


    def find_gpus():

        all_gpus = []
        # loop through all nodes Dragon is running on
        for huid in System().nodes:
            node = Node(huid)
            # loop through however many GPUs it may have
            for gpu_id in node.gpus:
                all_gpus.append((node.hostname, gpu_id))
        return all_gpus


Next we'll use that list of tuples to create a list of :py:class:`Policies <dragon.infrastructure.policy.Policy>`
where each :py:class:`~dragon.infrastructure.policy.Policy` specifies a host and a GPU on that host.

.. code-block:: python
    :linenos:
    :caption: **Given a list of (hostname, gpu_id), create a list of Policies specifying GPU affinity**

    from dragon.infrastructure.policy import Policy


    # pass in the output from find_gpus() above
    def make_policies(all_gpus=None, nprocs=32):

        # loop over each desired Policy
        # the number of which will be the number of processes we'll launch with ProcessGroup
        policies = []
        i = 0
        for worker in range(nprocs):
            # assign them in a round robin fashion
            policies.append(Policy(placement=Policy.Placement.HOST_NAME,
                                   host_name=all_gpus[i][0],
                                   gpu_affinity=[all_gpus[i][1]]))
            i += 1
            if i == len(all_gpus):
                i = 0
        return policies


Test It Out with Native Pool
----------------------------

Now that we can build a list of :py:class:`Policies <dragon.infrastructure.policy.Policy>` for our processes, let's
try it out using :py:class:`~dragon.native.pool.Pool`. In the example below, each worker will first say what host and
GPU it will use to verify its :py:class:`dragon.infrastructure.policy.Policy` is working as intended.
Then we'll use PyTorch to do some computation on the specified GPU.

.. code-block:: python
    :linenos:
    :caption: **Run a native Pool where workers are assigned a GPU to use**

    import os
    import torch
    import numpy as np

    from dragon.native.machine import current
    from dragon.native import Pool


    # reuse find_gpus() and make_policies() from above

    # GPU affinity is specified to the process by Dragon using the relevant method/environment variable,
    # such as CUDA_VISIBLE_DEVICES for NVIDIA devices (AMD and Intel also supported, see dragon.infrastructure.gpu_desc)
    # we'll assume NVIDIA GPUs for this example and verify CUDA_VISIBLE_DEVICES
    def my_gpu():
        mynode = current()
        print(f"Hello!, I have GPU={os.getenv('CUDA_VISIBLE_DEVICES')} on host={mynode.hostname}", flush=True)


    # do some matrix multiplication
    def gpu_work(x):
        v = np.array(512*[x*1.0])
        nx = 16
        ny = 512 // 16
        a = v.reshape(ny, nx)
        b = v.reshape(nx, ny)
        tensor_a = torch.from_numpy(a).cuda()
        tensor_b = torch.from_numpy(b).cuda()
        output = torch.sum(torch.matmul(tensor_a, tensor_b)).cpu().item()

        del tensor_a, tensor_b
        torch.cuda.empty_cache()
        return output


    # run a native Pool with the given number of workers, each assinged a single GPU
    def gpu_pool(nprocs=32):
        all_gpus = find_gpus()
        policies = make_policies(all_gpus=all_gpus, nprocs=nprocs)

        # light up as many as nprocs worth of GPUs!
        p = Pool(policy=policies, processes_per_policy=1, initializer=my_gpu)
        results = p.map_async(gpu_work, range(32)).get()
        p.close()
        p.join()
        return results


    if __name__ == '__main__':
        gpu_pool()

Running this example on a 4 nodes, each equipped with 4 NVIDIA A100 GPUs, gives us:

.. code-block:: console

    $ pip install torch numpy
    $ dragon gpu_pool.py
    Hello!, I have GPU=0 on host=pinoak0039
    Hello!, I have GPU=1 on host=pinoak0039
    Hello!, I have GPU=1 on host=pinoak0035
    Hello!, I have GPU=0 on host=pinoak0034
    Hello!, I have GPU=0 on host=pinoak0036
    Hello!, I have GPU=2 on host=pinoak0039
    Hello!, I have GPU=2 on host=pinoak0035
    Hello!, I have GPU=1 on host=pinoak0034
    Hello!, I have GPU=3 on host=pinoak0039
    Hello!, I have GPU=3 on host=pinoak0036
    Hello!, I have GPU=3 on host=pinoak0035
    Hello!, I have GPU=2 on host=pinoak0039
    Hello!, I have GPU=1 on host=pinoak0034
    Hello!, I have GPU=2 on host=pinoak0036
    Hello!, I have GPU=0 on host=pinoak0035
    Hello!, I have GPU=1 on host=pinoak0036
    Hello!, I have GPU=2 on host=pinoak0035
    Hello!, I have GPU=0 on host=pinoak0039
    Hello!, I have GPU=2 on host=pinoak0034
    Hello!, I have GPU=1 on host=pinoak0035
    Hello!, I have GPU=1 on host=pinoak0036
    Hello!, I have GPU=2 on host=pinoak0034
    Hello!, I have GPU=3 on host=pinoak0039
    Hello!, I have GPU=0 on host=pinoak0036
    Hello!, I have GPU=0 on host=pinoak0035
    Hello!, I have GPU=1 on host=pinoak0039
    Hello!, I have GPU=0 on host=pinoak0034
    Hello!, I have GPU=2 on host=pinoak0036
    Hello!, I have GPU=3 on host=pinoak0034
    Hello!, I have GPU=3 on host=pinoak0035
    Hello!, I have GPU=3 on host=pinoak0034
    Hello!, I have GPU=3 on host=pinoak0036



Test It Out with ProcessGroup
-----------------------------

Next we'll adapt some of the code above to run with :py:class:`~dragon.native.process_group.ProcessGroup`, where we'll
have a little more control over what the processes do. We'll still run a Python function in this example, but you could
instead run serial executables or even MPI processes this way (see :ref:`orchestrate_procs` and :ref:`orchestrate_mpi`).

.. code-block:: python
    :linenos:
    :caption: **Run a ProcessGroup where each process is assigned a single GPU**

    import os
    import torch
    import numpy as np

    from dragon.native.machine import current
    from dragon.native.process_group import ProcessGroup, ProcessTemplate


    # reuse find_gpus() and make_policies() from above

    # GPU affinity is specified to the process by Dragon using the relevant method/environment variable,
    # such as CUDA_VISIBLE_DEVICES for NVIDIA devices (AMD and Intel also supported, see dragon.infrastructure.gpu_desc)
    # we'll assume NVIDIA GPUs for this example and verify CUDA_VISIBLE_DEVICES
    def my_gpu(id, x=512):
        mynode = current()
        print(f"ID {id} has GPU={os.getenv('CUDA_VISIBLE_DEVICES')} on host={mynode.hostname}", flush=True)

        # reuse the definition of gpu_work() from above
        gpu_work(x)


    def gpu_pg(nprocs=32):
        all_gpus = find_gpus()
        policies = make_policies(all_gpus, nprocs=nprocs)

        # light up as many as nprocs worth of GPUs!
        pg = ProcessGroup()
        for i in range(nprocs):
            pg.add_process(nproc=1, template=ProcessTemplate(target=my_gpu, args=(i, i,), policy=policies[i]))

        pg.init()
        pg.start()
        pg.join()
        pg.close()


    if __name__ == '__main__':
        gpu_pg()


Running this example on a 4 nodes, each equipped with 4 NVIDIA A100 GPUs, gives us:

.. code-block:: console

    $ pip install torch numpy
    $ dragon gpu_process_group.py
    ID 18 has GPU=2 on host=pinoak0039
    ID 2 has GPU=2 on host=pinoak0039
    ID 0 has GPU=0 on host=pinoak0039
    ID 19 has GPU=3 on host=pinoak0039
    ID 17 has GPU=1 on host=pinoak0039
    ID 16 has GPU=0 on host=pinoak0039
    ID 3 has GPU=3 on host=pinoak0039
    ID 1 has GPU=1 on host=pinoak0039
    ID 30 has GPU=2 on host=pinoak0036
    ID 29 has GPU=1 on host=pinoak0036
    ID 28 has GPU=0 on host=pinoak0036
    ID 12 has GPU=0 on host=pinoak0036
    ID 13 has GPU=1 on host=pinoak0036
    ID 25 has GPU=1 on host=pinoak0034
    ID 15 has GPU=3 on host=pinoak0036
    ID 6 has GPU=2 on host=pinoak0035
    ID 22 has GPU=2 on host=pinoak0035
    ID 4 has GPU=0 on host=pinoak0035
    ID 14 has GPU=2 on host=pinoak0036
    ID 24 has GPU=0 on host=pinoak0034
    ID 20 has GPU=0 on host=pinoak0035
    ID 5 has GPU=1 on host=pinoak0035
    ID 9 has GPU=1 on host=pinoak0034
    ID 27 has GPU=3 on host=pinoak0034
    ID 31 has GPU=3 on host=pinoak0036
    ID 8 has GPU=0 on host=pinoak0034
    ID 21 has GPU=1 on host=pinoak0035
    ID 23 has GPU=3 on host=pinoak0035
    ID 7 has GPU=3 on host=pinoak0035
    ID 11 has GPU=3 on host=pinoak0034
    ID 10 has GPU=2 on host=pinoak0034
    ID 26 has GPU=2 on host=pinoak0034

