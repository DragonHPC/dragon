""" 
Run a sample MPI Hello World application with 1 rank per allocated node.
"""

import sys
import os

from dragon.native.pool_workers import PoolWorkers, Worker, DragonPoolWorkersError
from dragon.infrastructure.process_desc import ProcessOptions
from dragon.globalservices.node import get_list


def main():
    nnodes = len(get_list())
    mpi_hello_cmd = os.path.join(os.getcwd(), "mpi_hello")
    args = []
    cwd = os.getcwd()
    options = ProcessOptions(make_inf_channels=True)

    if not nnodes:
        print("No slurm allocation detected.")
        os.exit(-1)

    mpi_workers = [
        Worker(
            cmd=mpi_hello_cmd,
            args=args,
            cwd=cwd,
            processOptions=options,
            env=None,
            restartPolicy=Worker.RestartPolicy.NONE,
        )
        for _ in range(nnodes)
    ]

    pool = PoolWorkers(mpi_workers)
    pool.start()
    pool.join()
    pool.stop()

    return 0


if __name__ == "__main__":
    main()
