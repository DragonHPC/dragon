from time import perf_counter

from dragon.infrastructure.facts import PMIBackend
from dragon.infrastructure.policy import Policy
from dragon.native.machine import Node, System
from dragon.native.process import ProcessTemplate
from dragon.workflows.batch import Batch


def run_job(batch: Batch, num_ranks: int):
    job = batch.job(
        process_templates=[
            (
                num_ranks,
                ProcessTemplate(
                    target="./mpi_job",
                    args=(),
                    policy=Policy(placement=Policy.Placement.ANYWHERE),
                ),
            ),
        ],
        pmi=PMIBackend.CRAY,
    )

    return job


def main() -> int:
    system = System()
    num_ranks = sum(max(1, Node(node).num_cpus // 2) for node in system.nodes)
    print(f"Num ranks: {num_ranks}", flush=True)

    batch = Batch()
    print(f"Batch topology: {batch.topology()}", flush=True)
    timed_runs = 4
    elapsed = None
    jobs = []
    ret_codes = []

    try:
        warmup_job = run_job(batch, num_ranks)
        warmup_job.get()

        start = perf_counter()
        for _ in range(timed_runs):
            job = run_job(batch, num_ranks)
            jobs.append(job)
        batch.fence()
        elapsed = perf_counter() - start

        for job in jobs:
            ret_codes.extend(job.get())
    except Exception:
        print("Error running job", flush=True)
        ret_codes = [-1]
    finally:
        batch.join()

    if elapsed is not None:
        print(
            f"Average time per job run over {timed_runs} timed runs: " f"{elapsed / timed_runs:.6f} seconds", flush=True
        )

    return 1 if any(ret_codes) else 0


if __name__ == "__main__":
    raise SystemExit(main())
