import cupy as cp
import cupyx
import time
import multiprocessing as mp
import argparse
import numpy as np
import queue
import os

from dragon import (
    ProcessGroup,
    ProcessTemplate,
    Policy,
    System,
    current,
    DDict
)
from dragon.workflows.data_mover import (
    DataMovers,
    CuPyDataMover,
    NumPyDataMover,
)
from dragon.native.machine import Node
from dragon.managed_memory import (
    MemoryAlloc,
)

num_decimal_places = 8


def cupy_user_kernel(data):
    """A user-defined GPU kernel that performs a forward and inverse FFT on the input data. This kernel could be replaced with arbitrary user code."""
    y = cupyx.scipy.fftpack.fft(data)
    x_recovered = cupyx.scipy.fftpack.ifft(y)
    return x_recovered


def numpy_user_kernel(data):
    """A user-defined CPU kernel that performs a forward and inverse FFT on the input data. This kernel could be replaced with arbitrary user code. This kernel gets called as part of the validation. This kernel should produce the same results as cupy_user_kernel to a user specified precision."""
    y = np.fft.fft(data)
    x_recovered = np.fft.ifft(y)
    return x_recovered


def launch_nvidia_mps_daemon(alloc: System) -> None:
    """A helper function to launch the nvidia-mps daemon on every node in the allocation.

    :param alloc: a native.machine.System object representing the allocation
    :type alloc: System
    """

    nodes = [Node(id) for id in alloc.nodes]
    exe = "nvidia-cuda-mps-control"
    args = "-d"

    grp = ProcessGroup(ignore_error_on_exit=True)
    for node in nodes:
        temp_policy = Policy(placement=Policy.Placement.HOST_NAME, host_name=node.hostname)
        grp.add_process(nproc=1, template=ProcessTemplate(target=exe, args=args, policy=temp_policy))

    grp.init()
    grp.start()
    grp.join()
    grp.close()


def producer_init(producer_queue, validation_dict=None):
    me = mp.current_process()
    me.stash = {}
    me.stash["pq"] = producer_queue
    if validation_dict is not None:
        me.stash["validation_dict"] = validation_dict


def producer(args):
    me = mp.current_process()
    producer_queue = me.stash["pq"]

    data_size, data_type, data_production_delay, validate = args
    data = np.random.rand(data_size).astype(data_type)
    time.sleep(data_production_delay)

    if validate:
        validation_dict = me.stash["validation_dict"]
        # keying this way assumes no two data arrays will be identical
        # this assumption is probably ok for this benchmark
        # also need to keep track of any missed keys at the end
        kp0 = f"{np.real(data[0]):.{num_decimal_places}f}"
        kp1 = f"{np.real(data[1]):.{num_decimal_places}f}"
        key = kp0 + "_" + kp1
        validation_dict[key] = data
    producer_queue.put(data)


def cupy_general_kernel_proc(data_queue, stop_event, output_data_queue, devID):
    num_kernel_calls = 0
    start = time.perf_counter()
    with cp.cuda.Device(devID):
        kernel_stream = cp.cuda.Stream(non_blocking=True, ptds=True)
        with kernel_stream:
            while not stop_event.is_set():
                try:
                    x, serialized_descriptor = data_queue.get(timeout=1)
                except (TimeoutError, queue.Empty):
                    continue
                xp = cupy_user_kernel(x)
                kernel_stream.synchronize()
                NumPyDataMover.free_alloc(serialized_descriptor)
                num_kernel_calls += 1
                # not sure if this is necessary but it might be with the memcpy that happens in the put
                kernel_stream.synchronize()
                output_data_queue.put(xp)
                # not sure if this is necessary but it might be with the memcpy that happens in the put
                kernel_stream.synchronize()
    # might be unnecessary
    kernel_stream.synchronize()
    end = time.perf_counter()
    print(
        f"{num_kernel_calls} kernel calls on device {devID} in {end - start} seconds equating to a rate of {num_kernel_calls * 2 / (end - start)} FFT calls per second"
    )


def processed_consumer(stop_event, processed_data_queue, num_items, validate, devID=None, validation_dict=None):
    item_counter = 0
    num_validated = 0
    while item_counter < num_items:
        try:
            data = processed_data_queue.get(timeout=1)
            item_counter += 1
        except (TimeoutError, queue.Empty):
            continue
        if validate:
            kp0 = f"{np.real(data[0]):.{num_decimal_places}f}"
            kp1 = f"{np.real(data[1]):.{num_decimal_places}f}"
            key = kp0 + "_" + kp1
            try:
                og_data = validation_dict.pop(key)
            except Exception as e:
                raise KeyError(
                    f"Got Exception: {e}\n Could not find original data for validation, potentially corrupted data. {key=}\n Available keys: {list(validation_dict.keys())}"
                )
            np.testing.assert_allclose(numpy_user_kernel(og_data), data, atol=1e-5)
            num_validated += 1
            del data
            del og_data
        else:
            del data
        if item_counter % 50 == 0:
            if devID is None:
                print(f"Processed {item_counter} out of {num_items} items", flush=True)
            else:
                print(f"Processed {item_counter} out of {num_items} items on GPU {devID}", flush=True)
    if validate:
        if devID is None:
            print(f"Validated {num_validated} items out of {num_items} items", flush=True)
        else:
            print(f"Validated {num_validated} items out of {num_items} items on GPU {devID}", flush=True)

    stop_event.set()


def single_gpu_pipeline(
    args, num_producers, data_size, data_type, devID, stop_event, control_dict, validation_dict=None
):
    control_dict[devID]["movers_on"] = DataMovers(
        data_mover=NumPyDataMover,
        data_mover_args={"pool_size": 10 * 1024**3},
        device_pool_size=10 * 1024**3,
        num_workers=args.num_movers,
        device_id=devID,
    )
    control_dict[devID]["movers_off"] = DataMovers(
        data_mover=CuPyDataMover,
        data_mover_args={"pool_size": 10 * 1024**3},
        device_pool_size=10 * 1024**3,
        num_workers=args.num_movers,
        device_id=devID,
    )

    control_dict[devID]["movers_on"].start()
    control_dict[devID]["movers_off"].start()
    input_queue, output_descriptor_queue = control_dict[devID]["movers_on"].get_queues()
    input_descriptor_queue, output_queue = control_dict[devID]["movers_off"].get_queues()

    control_dict[devID]["producer_pool"] = mp.Pool(
        num_producers,
        initializer=producer_init,
        initargs=(
            input_queue,
            validation_dict,
        ),
    )

    if args.num_consumers == 1:
        control_dict[devID]["kernel_proc"] = mp.Process(
            target=cupy_general_kernel_proc,
            args=(
                output_descriptor_queue,
                stop_event,
                input_descriptor_queue,
                devID,
            ),
        )
    else:
        kernel_proc_temp = ProcessTemplate(
            target=cupy_general_kernel_proc,
            args=(
                output_descriptor_queue,
                stop_event,
                input_descriptor_queue,
                devID,
            ),
            policy=Policy(placement=Policy.Placement.HOST_NAME, host_name=current().hostname),
        )
        control_dict[devID]["kernel_proc"] = ProcessGroup()
        control_dict[devID]["kernel_proc"].add_process(
            nproc=args.num_consumers,
            template=kernel_proc_temp,
        )
        control_dict[devID]["kernel_proc"].init()

    control_dict[devID]["processed_consumer_proc"] = mp.Process(
        target=processed_consumer,
        args=(
            stop_event,
            output_queue,
            args.num_items,
            args.validate,
            devID,
            validation_dict,
        ),
    )


def main_gpu(args, data_type):
    dtype_nbytes = data_type().nbytes
    data_size = args.data_size_mb * 1024 * 1024 // dtype_nbytes

    alloc = System()
    if alloc.nnodes > 1:
        raise RuntimeError("This benchmark only supports single node runs.")

    my_node = Node(alloc.nodes[0])

    launch_nvidia_mps_daemon(alloc)

    # Create a data pipeline for each GPU on the node
    if args.ngpus > 0:
        if args.ngpus > my_node.num_gpus:
            raise RuntimeError(f"Requested {args.ngpus} GPUs but only {my_node.num_gpus} GPUs are available on node.")
        n_gpus = args.ngpus
    else:
        n_gpus = my_node.num_gpus

    num_producers = args.num_producers // n_gpus

    if args.validate:
        ddict_size = max(int(args.data_size_mb * 1024**2 * (args.num_items * n_gpus) * 2), 1024**3)
        print(f"Allocating DDict of size {ddict_size / (1024**3)} GB for validation", flush=True)
        validation_dict = DDict(1, 1, ddict_size)
    else:
        validation_dict = None

    print("Benchmark will be run with the following parameters:")
    print("====================================================")
    print(f"Data size: {args.data_size_mb} MB per FFT")
    print(f"Number of iterations per GPU: {args.num_items}")
    print(f"Number of GPUs: {n_gpus}")
    print(f"Number of producers per GPU: {num_producers} (total: {num_producers * n_gpus})")
    print(f"Number of movers per GPU: {args.num_movers}")
    print(f"Number of consumers per GPU: {args.num_consumers}")
    print(f"Data type: {args.data_type}")
    print(f"Producer delay: {args.producer_delay} seconds")
    print(f"Update frequency: {args.update} steps")
    print(f"Validation: {args.validate}")
    print("====================================================")
    print("Starting benchmark...\n", flush=True)

    # Start a dataflow on each GPU
    control_dict = {}
    for devID in range(n_gpus):
        control_dict[devID] = {}
        control_dict[devID]["stop_event"] = mp.Event()
        single_gpu_pipeline(
            args,
            num_producers,
            data_size,
            data_type,
            devID,
            control_dict[devID]["stop_event"],
            control_dict,
            validation_dict,
        )

    producer_args = (data_size, data_type, args.producer_delay, args.validate)
    for devID in range(n_gpus):
        control_dict[devID]["kernel_proc"].start()
        control_dict[devID]["processed_consumer_proc"].start()

        if devID == 0:
            start_time = time.perf_counter()
        result = control_dict[devID]["producer_pool"].map_async(producer, [producer_args] * args.num_items)

    for devID in range(n_gpus):
        control_dict[devID]["processed_consumer_proc"].join()

        if devID == n_gpus - 1:
            end_time = time.perf_counter()
            total_time = end_time - start_time

        control_dict[devID]["producer_pool"].close()
        control_dict[devID]["producer_pool"].join()
        control_dict[devID]["movers_on"].stop()
        control_dict[devID]["movers_off"].stop()
        control_dict[devID]["kernel_proc"].join()
        try:
            control_dict[devID]["kernel_proc"].close()
        except AttributeError:
            # not a process group
            pass

    print(f"Processed {args.num_items * n_gpus} items in {total_time} seconds")
    print(
        f"Pipeline throughput (includes forwards and backwards FFT): {n_gpus * data_size * data_type().nbytes * (args.num_items * n_gpus) / total_time / 1e9} GB/s"
    )

    # Run a data mover on each GPU
    if validation_dict is not None:
        validation_dict.destroy()


def producer_cpu(args):
    data_size, data_type, data_production_delay = args
    data = np.random.rand(data_size).astype(data_type)
    time.sleep(data_production_delay)
    return data


def cpu_proc(args):
    data = producer_cpu(args)
    result = numpy_user_kernel(data)
    return result


def cpu_main(args, data_type):
    num_total_workers = args.num_producers + args.num_movers + args.num_consumers
    dtype_nbytes = data_type().nbytes
    data_size = args.data_size_mb * 1024 * 1024 // dtype_nbytes

    alloc = System()
    if alloc.nnodes > 1:
        raise RuntimeError("This benchmark only supports single node runs.")

    print("Benchmark will be run CPU-only with the following parameters:")
    print("=============================================================")
    print(f"Data size: {args.data_size_mb} MB per FFT")
    print(f"Number of iterations: {args.num_items}")
    print(f"Data type: {args.data_type}")
    print("=============================================================")
    print("Starting benchmark...\n", flush=True)

    pool = mp.Pool(num_total_workers)

    producer_args = (data_size, data_type, args.producer_delay)
    start_time = time.perf_counter()
    result = pool.map_async(cpu_proc, [producer_args] * args.num_items).get()
    end_time = time.perf_counter()
    total_time = end_time - start_time
    print(f"Processed {args.num_items} items in {total_time} seconds")
    print(
        f"FFT Kernel Bandwidth (includes forwards and backwards): {data_size * dtype_nbytes * args.num_items / total_time / 1e9} GB/s"
    )
    pool.close()
    pool.join()


def parse_args():
    parser = argparse.ArgumentParser(description="Run GPU kernel and copy processes with Cupy.")
    parser.add_argument(
        "--data_size_mb",
        type=int,
        default=100,
        help="Size of data in MB to process. This will be scaled down based on how many GPUs are used.",
    )
    parser.add_argument(
        "--num-items",
        type=int,
        default=100,
        help="Number of work items to process.",
    )
    parser.add_argument(
        "--num-producers", type=int, default=64, help="Number of producer processes. These will be sharded across GPUs."
    )
    parser.add_argument("--num-movers", type=int, default=1, help="Number of mover processes per GPU")
    parser.add_argument("--num-consumers", type=int, default=1, help="Number of consumer processes per GPU.")
    parser.add_argument(
        "--producer-delay",
        type=float,
        default=1,
        help="Time delay in seconds for producer to produce data.",
    )
    parser.add_argument(
        "--cpu",
        action="store_true",
        help="Use CPU to execute kernel. Number of processses will be the sum of all consumers, movers, and producers.",
    )
    parser.add_argument(
        "--ngpus",
        type=int,
        default=-1,
        help="Number of GPUs to shard consumers across. If not set, all GPUs on node will be used.",
    )
    parser.add_argument(
        "--data-type",
        type=str,
        default="complex128",
        help="Data type to use (e.g. int8, float64, complex128) for GPU, at the moment we only take complex data types. Default is complex128.",
    )
    parser.add_argument(
        "--update", type=int, default=50, help="The number of FFT steps completed before a new status update."
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Validate the results.",
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    mp.set_start_method("dragon")

    if args.cpu:
        main = cpu_main
    else:
        main = main_gpu

    main(args, data_type=getattr(np, args.data_type))
