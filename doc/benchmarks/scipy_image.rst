SciPy Image Convolution Benchmark
+++++++++++++++++++++++++++++++++

This is an image processing benchmark. It performs image convolution using the SciPy package.
The computation is done in parallel by using a pool of workers.
For a given number of iterations, the benchmark reports the average time and standard
deviation of the computation.

We can succeed scaling with respect to
* the number of workers,
* the workload, by controlling the size of the NumPy array (image) that each worker is working on
and the number of work items, i.e., the number of images that the workers will work on.


.. code-block:: python
    :linenos:
    :caption: **scipy_scale_work.py: A SciPy image convolution benchmark**

    import dragon
    import argparse
    import multiprocessing
    import numpy as np
    import scipy.signal
    import time

    def get_args():
        parser = argparse.ArgumentParser(description="Basic SciPy test")
        parser.add_argument("--num_workers", type=int, default=4, help="number of workers")
        parser.add_argument("--iterations", type=int, default=10, help="number of iterations to do")
        parser.add_argument(
            "--burns", type=int, default=2, help="number of iterations to burn/ignore in order to warm up"
        )
        parser.add_argument("--dragon", action="store_true", help="run with dragon objs")
        parser.add_argument("--size", type=int, default=1000, help="size of the array")
        parser.add_argument(
            "--mem", type=int, default=(32 * 1024 * 1024), help="overall footprint of image dataset to process"
        )
        my_args = parser.parse_args()

        return my_args

    def f(args):
        image, random_filter = args
        # Do some image processing.
        return scipy.signal.convolve2d(image, random_filter)[::5, ::5]

    if __name__ == "__main__":
        args = get_args()

        if args.dragon:
            print("using dragon runtime")
            multiprocessing.set_start_method("dragon")
        else:
            print("using regular mp libs/calls")
            multiprocessing.set_start_method("spawn")

        num_cpus = args.num_workers

        print(f"Number of workers: {num_cpus}")
        pool = multiprocessing.Pool(num_cpus)
        image = np.zeros((args.size, args.size))
        nimages = int(float(args.mem) / float(image.size))
        print(f"Number of images: {nimages}")
        images = []
        for j in range(nimages):
            images.append(np.zeros((args.size, args.size)))
        filters = [np.random.normal(size=(4, 4)) for _ in range(nimages)]

        all_iter = args.iterations + args.burns
        results = np.zeros(all_iter)
        for i in range(all_iter):
            start = time.perf_counter()
            res = pool.map(f, zip(images, filters))
            del res
            finish = time.perf_counter()
            results[i] = finish - start

        print(f"Average time: {round(np.mean(results[args.burns:]), 2)} second(s)")
        print(f"Standard deviation: {round(np.std(results[args.burns:]), 2)} second(s)")


Arguments list
==============

.. code-block:: console
    :linenos:

    dragon scipy_scale_work.py [-h] [--num_workers NUM_WORKERS] [--iterations ITERATIONS]
                               [--burns BURN_ITERATIONS] [--dragon] [--size IMAGE_SIZE]
                               [--mem MEMORY_FOOTPRINT]

    --num_workers       number of the workers, int, default=4
    --iterations        number of iterations, int, default=10
    --burns             number of iterations to burn, int, default=2
    --dragon            use the Dragon calls, action="store_true"
    --size              size of the numpy array used to create an image, int, default=1000
    --mem               overall memory footprint of image dataset to process, int, default=(32 * 1024 * 1024)


How to run
==========

System Information
------------------

We ran the experiments in a Cray XC-50 system,
with Intel Xeon(R) Platinum 8176 CPU @ 2.10GHz (28 cores per socket, single socket).

The Dragon version used is 0.3.


Single node
-----------

**Base Multiprocessing**

In order to use the standard multiprocessing library, this code can be run with
`dragon scipy_scale_work.py`:

.. code-block:: console
    :linenos:

    > salloc --nodes=1 --exclusive
    > dragon scipy_scale_work.py --num_workers 4
    [stdout: p_uid=4294967296] using regular mp libs/calls
    Number of workers: 4
    [stdout: p_uid=4294967296] Number of images: 33
    Average time: 0.5 second(s)
    Standard deviation: 0.01 second(s)

**Dragon runtime**

In order to use Dragon runtime calls, run with `dragon scipy_scale_work.py --dragon`:

.. code-block:: console
    :linenos:

    > salloc --nodes=1 --exclusive
    > dragon scipy_scale_work.py --dragon --num_workers 4
    [stdout: p_uid=4294967296] using dragon runtime
    Number of workers: 4
    [stdout: p_uid=4294967296] Number of images: 33
    Average time: 0.39 second(s)
    Standard deviation: 0.0 second(s)

From the above results, we can see that Dragon outperforms Base Multiprocessing.


Multi-node
----------

**Dragon runtime**

In multinode case, the placement of processes follows a round robin scheme. More information
can be found in :ref:`Processes <pguide/owner:Processes>`

For running on 2 nodes with 4 workers:

.. code-block:: console
    :linenos:

    > salloc --nodes=2 --exclusive
    > dragon scipy_scale_work.py --dragon --num_workers 4
    [stdout: p_uid=4294967296] using dragon runtime
    Number of workers: 4
    [stdout: p_uid=4294967296] Number of images: 33
    Average time: 1.46 second(s)
    Standard deviation: 0.02 second(s)

For running on 2 nodes with 32 workers:

.. code-block:: console
    :linenos:

    > salloc --nodes=2 --exclusive
    > dragon ./scipy_scale_work.py --dragon --num_workers 32
    [stdout: p_uid=4294967296] using dragon runtime
    Number of workers: 32
    [stdout: p_uid=4294967296] Number of images: 33
    Average time: 1.34 second(s)
    Standard deviation: 0.61 second(s)

For running on 4 nodes with 32 workers:

.. code-block:: console
    :linenos:

    > salloc --nodes=4 --exclusive
    > dragon ./scipy_scale_work.py --dragon --num_workers 32
    [stdout: p_uid=4294967296] using dragon runtime
    Number of workers: 32
    [stdout: p_uid=4294967296] Number of images: 33
    Average time: 1.11 second(s)
    Standard deviation: 0.02 second(s)

In the above case, we keep the amount of work fixed, `Number of images: 33`, and we scale
the number of nodes or number of workers. We see that the performance keeps improving as expected.


Finally, we run on 64 nodes with 896 workers:

.. code-block:: console
    :linenos:

    > salloc --nodes=64 --exclusive
    > dragon ./scipy_scale_work.py --dragon --mem 536870912 --size 256 --num_workers 896
    [stdout: p_uid=4294967296] using dragon runtime
    Number of workers: 896
    [stdout: p_uid=4294967296] Number of images: 8192
    Average time: 1.46 second(s)
    Standard deviation: 0.03 second(s)

In this last experiment, we scale the amount of nodes by a factor of 16, the number of workers by a factor of 28,
and the amount of work by a factor of 16. This is a substantially heavier experiment compared to the previous ones.
Dragon was able to scale and the performance almost remained the same as before.