import os
import time
import argparse
import pathlib
import subprocess

import dragon
from dragon.data.ddict.ddict import DDict, PosixCheckpointPersister, DAOSCheckpointPersister
from multiprocessing import set_start_method

class TestTimer:

    def __init__(self):

        self._t0 = time.clock_gettime_ns(time.CLOCK_MONOTONIC)

    def elapsed(self) -> float:

        return 1.0e-9 * (time.clock_gettime_ns(time.CLOCK_MONOTONIC) - self._t0)

def get_args():
    parser = argparse.ArgumentParser(description="DDict daos restore test")
    parser.add_argument(
        "--num_nodes",
        type=int,
        default=1,
        help="number of nodes the dictionary distributed across",
    )
    parser.add_argument(
        "--managers_per_node",
        type=int,
        default=1,
        help="number of managers per node for the dragon dict",
    )
    parser.add_argument(
        "--total_mem_size",
        type=float,
        default=40,
        help="total managed memory size for dictionary in GB",
    )
    parser.add_argument(
        "--persist_path",
        type=str,
        help="path/pool to the persisted checkpoint file",
    )
    parser.add_argument(
        "--name",
        type=str,
        help="name of the DDict",
    )
    parser.add_argument(
        "--persister",
        type=str,
        choices=["DAOS", "POSIX"],
        default="POSIX",
        help="type of persister used to restore the checkpoint",
    )
    my_args = parser.parse_args()
    return my_args

class TestDDictChkptRestore:
    def __init__(self, persister, parser):
        if persister == DAOSCheckpointPersister:
            self.dd = DDict(
                parser.managers_per_node,
                parser.num_nodes,
                total_mem=parser.total_mem_size,
                persist_path=parser.persist_path,
                persist_freq=1,
                name=parser.name,
                trace=True,
                read_only=True,
                restore_from=0,
                persister_class=persister,
            )
        else:
            self.dd = DDict(
                parser.managers_per_node,
                parser.num_nodes,
                total_mem=parser.total_mem_size,
                persist_path=parser.persist_path,
                persist_freq=1,
                name=parser.name,
                trace=True,
                persister_class=persister,
            )
        self.persister = persister

    def stats(self):
        print(f"{len(self.dd)=}", flush=True)

    def restore(self):
        print(f"{self.dd.persisted_ids()=}")
        t = TestTimer()
        self.dd.restore(0)
        total_time = t.elapsed()
        print(f"{self.persister} - time spent on restoring a checkpoint: {total_time}")

    def destroy(self):
        self.dd.destroy()

def main():

    set_start_method("dragon")

    parser = get_args()
    num_nodes = parser.num_nodes
    managers_per_node = parser.managers_per_node
    parser.total_mem_size = int(parser.total_mem_size * (1024**3))
    trace = parser.trace
    name = parser.name

    if parser.persister == "DAOS":
        persister = DAOSCheckpointPersister
    else:
        persister = PosixCheckpointPersister

    test = TestDDictChkptRestore(persister, parser)
    test.restore()
    test.stats()
    test.destroy()

if __name__ == "__main__":
    main()