import argparse
import numpy as np
import psutil
import ray
import scipy.signal
import time


def get_args():
    parser = argparse.ArgumentParser(description='Basic SciPy test with Ray')

    parser.add_argument('--num_workers', type=int, default=4,
                        help='number of workers')

    parser.add_argument('--iterations', type=int, default=10,
                        help='number of iterations to do')

    parser.add_argument('--burns', type=int, default=2,
                        help='number of iterations to burn/ignore in order to warm up')

    parser.add_argument('--size', type=int, default=1000,
                        help='size of the array')

    parser.add_argument('--mem', type=int, default=(512 * 1024 * 1024),
                        help='overall footprint of image dataset to process')

    parser.add_argument('--work_time', type=float, default=1.0,
                        help='how many seconds of compute per image')

    parser.add_argument('--multinode', action='store_true',
                        help='run on multiple nodes')

    parser.add_argument('--ipaddress', type=str,
                        help='for multinode only - ip address of the head node of the Ray cluster')

    my_args = parser.parse_args()

    return my_args


@ray.remote
def f(image, random_filter, work_time):
    # Do some image processing
    elapsed = 0.
    start = time.perf_counter()

    # Explicitly control compute time per image
    while elapsed < work_time:
        scipy.signal.convolve2d(image, random_filter)[::5, ::5]
        elapsed = time.perf_counter() - start
    return scipy.signal.convolve2d(image, random_filter)[::5, ::5]


if __name__ == '__main__':
    args = get_args()

    # Image and filter generation
    image = np.zeros((args.size, args.size))
    nimages = int(float(args.mem) / float(image.size))
    print(f'Number of images: {nimages}')
    images = []
    for j in range(nimages):
        images.append(np.zeros((args.size, args.size)))
    filters = [np.random.normal(size=(4, 4)) for _ in range(nimages)]

    # Ray initialization
    # when using multiple nodes, the number of workers
    # is defined during the setup of the Ray cluster
    if args.multinode is False: # single node case
        num_cpus = args.num_workers
        print(f'Number of workers: {num_cpus}')
        ray.init(num_cpus=num_cpus)
    else:
        ray.init(address="auto", _node_ip_address=args.ipaddress)

    # Main body of the computation
    times = []
    for i in range(args.iterations + args.burns):
        start = time.perf_counter()
        remotes = []
        for j in range(nimages):
            remotes.append(f.remote(ray.put(images[j]), filters[j], float(args.work_time)))
        res = ray.get(remotes)
        del(res)
        del(remotes)
        times.append(time.perf_counter() - start)
        print(f"Time for iteration {i} is {times[i]} seconds", flush=True)

    # Print out the mean and standard deviation, excluding the burns iterations
    print(f"Average time: {round(np.mean(times[args.burns:]), 2)} second(s)")
    print(f"Standard deviation: {round(np.std(times[args.burns:]), 2)} second(s)")
