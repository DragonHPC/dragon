import argparse
import time

from dragon.workflows.batch import Batch


width_num_tasks = 10
width_throughput = 20


def no_op(array=None):
    pass


def run_tasks(batch: Batch, num_tasks: int, warmup: bool = False) -> None:
    tasks = []
    # array = numpy.zeros((1024, 1024))

    for _ in range(num_tasks):
        task = batch.function(no_op)
        tasks.append(task)

    # run list of tasks one at a time and time it

    start_time = time.time()

    for task in tasks:
        task.start()

    for task in tasks:
        task.wait()

    end_time = time.time()

    if not warmup:
        runtime = end_time - start_time
        throughput = num_tasks / runtime
        print(
            f"{num_tasks:<{width_num_tasks}} {throughput:<{width_throughput}}",
            flush=True,
        )


def run_compiled_task(batch: Batch, num_tasks: int, warmup: bool = False) -> None:
    tasks = []
    # array = numpy.zeros((1024, 1024))

    # compile list of tasks into a single task and time it

    for _ in range(num_tasks):
        task = batch.function(no_op)
        tasks.append(task)

    start_time = time.time()
    compiled_task = batch.compile(tasks)
    compiled_task.start()
    compiled_task.wait()
    end_time = time.time()

    if not warmup:
        runtime = end_time - start_time
        throughput = num_tasks / runtime
        print(
            f"{num_tasks:<{width_num_tasks}} {throughput:<{width_throughput}}",
            flush=True,
        )


def run_bench(batch: Batch, min_tasks: int, max_tasks: int, compiled: bool):
    if compiled:
        print("running a single compiled task", flush=True)
        print("------------------------------", flush=True)
    else:
        print("running tasks one at a time", flush=True)
        print("---------------------------", flush=True)

    if compiled:
        run_compiled_task(batch, min_tasks, warmup=True)
    else:
        run_tasks(batch, min_tasks, warmup=True)

    # TODO: also print: (1) scheduling overhead per task, and (2) maximum task parallelism
    # for tasks that run 10ms, 1s, 100s (~1.7 minutes), 10,000s (~2.8 hours)
    num_tasks_str = "num tasks"
    throughput_str = "throughput [tasks/s]"
    print(
        f"{num_tasks_str.ljust(width_num_tasks)} {throughput_str.ljust(width_throughput)}",
        flush=True,
    )

    num_tasks = min_tasks

    while num_tasks <= max_tasks:
        if compiled:
            run_compiled_task(batch, num_tasks)
        else:
            run_tasks(batch, num_tasks)

        num_tasks *= 2

    print("", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Task throughput benchmark")
    parser.add_argument(
        "--min_tasks", type=int, default=4, help="minimum number of tasks to run"
    )
    parser.add_argument(
        "--max_tasks", type=int, default=128, help="maximum number of tasks to run"
    )
    args = parser.parse_args()

    if args.max_tasks < args.min_tasks:
        args.max_tasks = args.min_tasks

    batch = Batch()

    # TODO: the non-compiled benchmark hangs when it reaches larger numbers of tasks
    # (around 1024), possibly due to the need for progress threads that process queues
    # run_bench(batch, args.min_tasks, args.max_tasks, compiled=False)
    run_bench(batch, args.min_tasks, args.max_tasks, compiled=True)

    batch.close()
    batch.join()
