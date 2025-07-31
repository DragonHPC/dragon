"""
Run an MPI application with either a Cray PALS or PMIx backend
"""

import os
import argparse
from dragon.native.process import ProcessTemplate
from dragon.native.process_group import ProcessGroup
from dragon.globalservices.node import get_list
from dragon.infrastructure.facts import PMIBackend


def main(exe, pmi, ppn):
    nnodes = len(get_list())
    args = []
    cwd = os.getcwd()

    if not nnodes:
        print("No slurm allocation detected.")
        os.exit(-1)

    pg = ProcessGroup(restart=False, pmi=pmi)
    pg.add_process(nproc=nnodes * ppn, template=ProcessTemplate(target=exe, args=args, cwd=cwd, env=None))
    pg.init()
    pg.start()
    pg.join()
    pg.close()

    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Execute an MPI Hello world for a given PMI backend")

    parser.add_argument("exe", type=str, help="Name of MPI executable to execute")
    parser.add_argument(
        "--pmi",
        type=PMIBackend.from_str,
        default=PMIBackend.CRAY,
        help="PMI backend for launch MPI app. Choices are 'cray' or 'pmix'",
    )
    parser.add_argument("--ppn", type=int, default=4, help="Number of processes to launch per node")
    args = parser.parse_args()

    main(args.exe, args.pmi, args.ppn)
