SciPy Image Convolution Benchmark
+++++++++++++++++++++++++++++++++

This little benchmark performs an image convolution in parallel using SciPy's
`signal.convolve2d` call. It can be run with `dragon` or standard `python3` to
compare performance on your machine. 

The code demonstrates the following key concepts working with Dragon:

* How to write programs that can run with either: Dragon or standard Python3
* How to use `multiprocessing.Pool` to parallelize scientific workloads

.. code-block:: python
    :linenos:
    :caption: **scipy_image_demo.py: A small parallel image convolution benchmark**

    import os
    import time
    import numpy as np

    import dragon
    import multiprocessing as mp
    from scipy import signal

    NUM_WORKERS = 4
    IMAGE_SIZE = 1024
    MEMSIZE = 32 * 1024**2  # 32 MB
    NUM_ITER = 10
    NUM_BURN = 2


    def convolve_image_filter(args: tuple) -> np.ndarray:
        """Use scipy to convolve an image with a filter

        :param args: tuple containing image and filter
        :type args: tuple
        :return: convolved image
        :rtype: np.array
        """
        image, random_filter = args
        return signal.convolve2d(image, random_filter)[::5, ::5]


    if __name__ == "__main__":

        # check if we should use the Dragon
        if "DRAGON_PATCH_MP" in os.environ:
            mp.set_start_method("dragon")
        else:
            mp.set_start_method("spawn")

        # create the worker pool
        pool = mp.Pool(NUM_WORKERS)

        # create images, stay within MEMSIZE
        image = np.zeros((IMAGE_SIZE, IMAGE_SIZE))
        num_images = int(float(MEMSIZE) / float(image.size))

        rng = np.random.default_rng(42)
        filters = [rng.standard_normal(size=(4, 4)) for _ in range(num_images)]
        images = [np.zeros((IMAGE_SIZE, IMAGE_SIZE)) for _ in range(num_images)]

        print(f"# of workers = {NUM_WORKERS}", flush=True)
        print(f"memory footprint = {MEMSIZE}", flush=True)
        print(f"# of images = {num_images}", flush=True)

        # run a small benchmark

        results = []
        for _ in range(NUM_BURN + NUM_ITER):
            beg = time.perf_counter()
            pool.map(convolve_image_filter, zip(images, filters))
            end = time.perf_counter()
            results.append(end - beg)

        results = results[NUM_BURN:]  # throw away burn in results
        
        pool.close()
        pool.join()
        print(f"Average execution time {round(np.mean(results), 2)} second(s)", flush=True)
        print(f"Standard deviation: {round(np.std(results), 2)} second(s)")

This code can be run either with standard Python3 with `python3 scipy_image_demo.py`:

.. code-block:: console
    :linenos:

    >$python3 scipy_image_demo.py 
    # of workers = 4
    memory footprint = 33554432
    # of images = 32
    Average execution time 0.73 second(s)
    Standard deviation: 0.03 second(s)

or using the Dragon runtime, potentially also on multiple nodes with `dragon scipy_image_demo.py`:

.. code-block:: console
    :linenos:

    >$dragon scipy_image_demo.py 
    # of workers = 4
    memory footprint = 33554432
    # of images = 32
    Average execution time 1.02 second(s)
    Standard deviation: 0.2 second(s)
    +++ head proc exited, code 0