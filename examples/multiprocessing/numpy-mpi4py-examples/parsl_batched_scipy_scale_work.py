import dragon
import multiprocessing as mp
import parsl
from parsl.config import Config
from parsl import python_app
from dragon.workflows.parsl_batch_executor import DragonBatchPoolExecutor

import os
import math
import argparse
import numpy as np
import time
import itertools



def get_args():
    parser = argparse.ArgumentParser(description="Basic SciPy test")

    parser.add_argument("--num_workers", type=int, default=4,
                        help="number of workers")

    parser.add_argument("--iterations", type=int, default=10,
                        help="number of iterations to do")

    parser.add_argument("--burns", type=int, default=2,
                        help="number of iterations to burn/ignore in order to warm up")

    parser.add_argument("--size", type=int, default=1000,
                        help="size of the array")

    parser.add_argument("--mem", type=int, default=(512 * 1024 * 1024),
                        help="overall footprint of image dataset to process")

    parser.add_argument('--work_time', type=float, default=0.0,
                        help='how many seconds of compute per image')

    my_args = parser.parse_args()

    return my_args

@python_app
def f(args):
    import scipy.signal
    import time
    # Do some image processing
    image, random_filter, work_time = args
    elapsed = 0.
    start = time.perf_counter()

    # Explicitly control compute time per image
    while elapsed < work_time:
        scipy.signal.convolve2d(image, random_filter)[::5, ::5]
        elapsed = time.perf_counter() - start
    return scipy.signal.convolve2d(image, random_filter)[::5, ::5]



def main():
    mp.set_start_method("dragon")
    args = get_args()

    # Image and filter generation
    image = np.zeros((args.size, args.size))
    nimages = int(float(args.mem) / float(image.size))
    print(f"Number of images: {nimages}", flush=True)
    images = []
    for j in range(nimages):
        images.append(np.zeros((args.size, args.size)))
    filters = [np.random.normal(size=(4, 4)) for _ in range(nimages)]

    num_cpus = args.num_workers
    nnodes=int(os.environ['SLURM_NNODES'])
    num_cpus_per_node= math.ceil(num_cpus/nnodes)
    print(f"Number of workers: {num_cpus}", flush=True)
    print(f"Number of workers per node: {num_cpus_per_node}", flush=True)
    optimal_batch_size=int(nimages/num_cpus)
    print(f"Batch size: {optimal_batch_size}", flush=True)

    config = Config(
        executors=[
            DragonBatchPoolExecutor(
                max_processes=num_cpus,
                batch_size=optimal_batch_size,
            ),
        ],
        strategy=None,
    )

    parsl.load(config)

    # Main body of the computation
    times = []
    for i in range(args.iterations + args.burns):
        start = time.perf_counter()
        results=[]
        for input_args in zip(images, filters, itertools.repeat(float(args.work_time))):
            # launch task and get back a future
            res_future = f(input_args)
            results.append(res_future) 
        for res_future in results:
            # this blocks till each result is available
            res_future.result()
        times.append(time.perf_counter() - start)
        print(f"Time for iteration {i} is {times[i]} seconds", flush=True)

    # Print out the mean and standard deviation, excluding the burns iterations
    print(f"Average time: {round(np.mean(times[args.burns:]), 2)} second(s)")
    print(f"Standard deviation: {round(np.std(times[args.burns:]), 2)} second(s)")
    config.executors[0].shutdown()

if __name__ == "__main__":
    main()
