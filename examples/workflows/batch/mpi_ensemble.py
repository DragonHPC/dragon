from dragon.workflows.batch import Batch
from dragon.native.process import ProcessTemplate
from dragon.native.queue import Queue
from dragon.native.machine import cpu_count

import time
import random


def mpi_f(i, secs):
    import mpi4py

    mpi4py.rc.initialize = False

    from mpi4py import MPI

    MPI.Init()

    time.sleep(secs)

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    if rank == (size - 1):
        print(f"Job {i}: Rank {rank} of {size} says: Hello from MPI!", flush=True)

    MPI.Finalize()

    return True


def main():

    njobs = 32
    sleepsecs = 2
    maxranks = 32

    batch = Batch()

    alljobs = []
    for i in range(njobs):
        nranks = random.randint(2, maxranks)
        template = ProcessTemplate(
            target=mpi_f,
            args=(
                i,
                sleepsecs,
            ),
        )
        alljobs.append(batch.job(process_templates=[(nranks, template)]))

    for job in alljobs:
        try:
            print(f"Job result={job.get()}", flush=True)
        except Exception as e:
            print(f"Job failed with exception {e}", flush=True)

    batch.close()
    batch.join()


if __name__ == "__main__":
    main()
