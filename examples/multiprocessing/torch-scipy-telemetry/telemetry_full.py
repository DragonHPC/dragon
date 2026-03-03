"""This is the main file. It imports the other files and orchestrates the telemetry work. It contains telem_work, which
is the function launched on every node that gathers telemetry data and pushes it to a shared queue, and post_process, which is launched only
on one node and reads the telemetry data from the queue and then prints that information."""

import dragon
from dragon.globalservices.node import get_list, query_total_cpus
import multiprocessing as mp
import argparse
import time
import os
import queue

import functools
import mnist
import telem as tm
import conv


def get_args():
    """Get the user provided arguments
    :return args: input args from command line
    :rtype args: ArgumentParser object
    """
    parser = argparse.ArgumentParser(description="SciPy and MNIST test with telemetry")
    parser.add_argument("--scipy_workers", type=int, default=2, help="number of scipy workers (default: 2)")
    parser.add_argument("--mnist_workers", type=int, default=2, help="number of mnist workers (default: 2)")
    parser.add_argument(
        "--bars", action="store_true", default=False, help="uses tqdm bars to print telemetry info"
    )
    parser.add_argument("--no-cuda", action="store_true", default=False, help="disables CUDA training")
    parser.add_argument("--size", type=int, default=1024, help="size of the array (default: 1024)")
    parser.add_argument(
        "--mem",
        type=int,
        default=(1024 * 1024 * 1024),
        help="overall footprint of image dataset to process (default: 1024^3)",
    )
    parser.add_argument(
        "--batch-size", type=int, default=64, metavar="N", help="input batch size for training (default: 64)"
    )
    parser.add_argument(
        "--test-batch-size",
        type=int,
        default=1000,
        metavar="N",
        help="input batch size for testing (default: 1000)",
    )
    parser.add_argument(
        "--epochs", type=int, default=14, metavar="N", help="number of epochs to train (default: 14)"
    )
    parser.add_argument(
        "--gamma", type=float, default=0.7, metavar="M", help="Learning rate step gamma (default: 0.7)"
    )
    parser.add_argument("--seed", type=int, default=1, metavar="S", help="random seed (default: 1)")
    args = parser.parse_args()

    my_args = parser.parse_args()
    return my_args


def telem_work(q, start_ev, end_ev):
    """This is used by every monitoring process. It gathers telemetry data
    for CPU and GPU utilization and pushed it into the shared queue.

    :param q: shared queue that stores the telemetry data for each node
    :type q: Dragon Multiprocessing Queue
    :param start_ev: event that signals the beginning of monitoring
    :type start_ev: Event
    :param end_ev: event that signals the end of monitoring
    :type end_ev: Event
    """
    print(f"This is a telemetry process on node {os.uname().nodename}.", flush=True)
    start_ev.wait()  # wait until the starting event is set
    while True:
        gpu_info_list = tm.call_nvml()
        # one process on each node adds to a shared queue
        q.put(gpu_info_list)
        time.sleep(1)

        # check if the end event is set. If yes, exit.
        if end_ev.is_set():
            print(f"Telemetry process on node {os.uname().nodename} exiting ...", flush=True)
            break


def post_process(q, start_ev, end_ev, tqdm_bars):
    """This is used by the single post-processing process
    that gets the telemetry data from the shared queue and
    prints it.

    :param q: shared queue that stores the telemetry data for each node
    :type q: Dragon Multiprocessing Queue
    :param start_ev: event that signals the beginning of monitoring
    :type start_ev: Event
    :param end_ev: event that signals the end of monitoring
    :type end_ev: Event
    :param tqdm_bars: flag that signals whether to use bars or not for the presentation of the telemetry data
    :type tqdm_bars: Boolean
    """
    print(f"This is the postprocessing process, {os.uname().nodename}.", flush=True)
    start_ev.wait()  # wait until the starting event is set
    tqdm_dict = {}  # used when bars are used for the presentation of the telemetry data
    while True:
        # single process reads from the shared queue and prints results
        try:
            results_telem = q.get(timeout=10)
            if tqdm_bars:
                tm.updateTelemDict(results_telem, tqdm_dict, deviceID=None)
            else:
                tm.printTelem(results_telem)
        # when the queue is empty, exit
        except queue.Empty:
            print("Post process is exiting", flush=True)
            break
        except Exception as e:
            print(f"Exception caught: {e}", flush=True)


if __name__ == "__main__":
    args = get_args()
    print(f"Hello from main process {os.uname().nodename}.", flush=True)
    print("using dragon runtime", flush=True)
    mp.set_start_method("dragon")

    # get the list of nodes from Global Services
    nodeslist = get_list()
    nnodes = len(nodeslist)

    num_mnist_workers = args.mnist_workers
    assert num_mnist_workers > 1
    num_cpus = args.scipy_workers
    print(f"Number of nodes: {nnodes}", flush=True)
    print(f"Number of scipy workers: {num_cpus}", flush=True)
    print(f"Number of MNIST workers: {num_mnist_workers}", flush=True)

    # variable used to signal whether to use bars for the presentation of data or not
    use_bars = args.bars

    # Initialize the shared queue among the nodes
    # that is used for the communication of the telemetry data
    q = mp.Queue()

    # event used to signal the beginning of monitoring processes
    start_ev = mp.Event()
    # event used to signal the end of monitoring processes
    end_ev = mp.Event()

    # Create a process that gets and processes the telemetry data
    post_proc = mp.Process(target=post_process, args=(q, start_ev, end_ev, use_bars))
    post_proc.start()

    # Create a process on each node for monitoring
    procs = []
    for _ in range(nnodes):
        proc = mp.Process(target=telem_work, args=(q, start_ev, end_ev))
        proc.start()
        procs.append(proc)

    # Create a pool of workers for the scipy work
    scipy_data = conv.init_data(args)
    scipy_pool = mp.Pool(num_cpus)

    # Create a pool of workers for the mnist work
    deviceQueue = mnist.buildDeviceQueue()
    lr_list = [1 / (num_mnist_workers - 1) * i + 0.5 for i in range(num_mnist_workers)]
    mnist_lr_sweep_partial = functools.partial(mnist.mnist_lr_sweep, args, deviceQueue)
    mnist_pool = mp.Pool(num_mnist_workers)

    # start telemetry
    start_ev.set()

    # launch scipy and mnist jobs
    print(f"Launching scipy and mnist jobs", flush=True)
    workers_mnist = mnist_pool.map_async(mnist_lr_sweep_partial, lr_list, 1)
    workers_scipy = scipy_pool.map_async(conv.f, scipy_data)

    # wait on async processes
    mnist_pool.close()
    mnist_pool.join()
    scipy_pool.close()
    scipy_pool.join()

    # set the event to signal the end of computation
    time.sleep(10)
    print(f"Shutting down procs", flush=True)
    end_ev.set()

    # wait on the monitoring processes and the post-processing process
    for proc in procs:
        proc.join()
    post_proc.join()
    q.close()

    for result in workers_mnist.get():
        print(
            f"Final test for learning rate {result[0]}: loss: {result[1]} accuracy: {result[2]}", flush=True
        )
