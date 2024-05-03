""" 
Run a sample MPI Hello World application with 1 rank per allocated node.
"""

import os

from dragon.native.process import ProcessTemplate
from dragon.native.process_group import ProcessGroup
from dragon.globalservices.node import get_list


def main():
    nnodes = len(get_list())
    mpi_hello_cmd = os.path.join(os.getcwd(), "mpi_hello")
    args = []
    cwd = os.getcwd()

    if not nnodes:
        print("No slurm allocation detected.")
        os.exit(-1)

    pool = ProcessGroup(restart=False, pmi_enabled=True)
    pool.add_process(
        nproc=nnodes,
        template=ProcessTemplate(
            target=mpi_hello_cmd, args=args, cwd=cwd, env=None
        ),
    )
    pool.init()
    pool.start()
    pool.join()

    return 0


if __name__ == "__main__":
    main()
