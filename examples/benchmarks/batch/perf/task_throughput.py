import argparse
import time

from dragon.workflows.batch import Batch

width_num_tasks = 10
width_throughput = 20


def no_op(array=None):
    pass


def run_tasks(batch: Batch, num_tasks: int, warmup: bool = False) -> None:
    start_time = time.perf_counter()

    for _ in range(num_tasks):
        batch.function(no_op)

    batch.fence()
    end_time = time.perf_counter()

    # clear_results() fences (waits for all tasks to complete) then clears the
    # results DDict, preventing accumulated stale entries from triggering hash
    # table resizes at power-of-2 boundaries in subsequent benchmark iterations.
    batch.clear_results()

    if not warmup:
        runtime = end_time - start_time
        throughput = num_tasks / runtime if runtime > 0 else 0.0
        print(
            f"{num_tasks:<{width_num_tasks}} {throughput:<{width_throughput}.6f}",
            flush=True,
        )


def run_bench(batch: Batch, min_tasks: int, max_tasks: int):
    print("task throughput benchmark", flush=True)
    print("-------------------------", flush=True)

    run_tasks(batch, min_tasks, warmup=True)

    print(
        f"{'num tasks'.ljust(width_num_tasks)} {'throughput [tasks/s]'.ljust(width_throughput)}",
        flush=True,
    )

    num_tasks = min_tasks
    while num_tasks <= max_tasks:
        run_tasks(batch, num_tasks)
        num_tasks *= 2

    print("", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Task throughput benchmark")
    parser.add_argument("--min_tasks", type=int, default=4, help="minimum number of tasks to run")
    parser.add_argument("--max_tasks", type=int, default=128, help="maximum number of tasks to run")
    parser.add_argument(
        "--results_ddict_managers_per_pool",
        type=int,
        default=None,
        help=(
            "number of results-DDict manager shards per worker pool; "
            "when omitted, uses one shard per worker (passes results_ddict_managers_per_pool=None to Batch)"
        ),
    )
    args = parser.parse_args()

    if args.max_tasks < args.min_tasks:
        args.max_tasks = args.min_tasks

    batch = Batch(results_ddict_managers_per_pool=args.results_ddict_managers_per_pool)

    try:
        run_bench(batch, args.min_tasks, args.max_tasks)
    finally:
        batch.join()
