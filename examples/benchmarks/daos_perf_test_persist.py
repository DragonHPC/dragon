import os
import time
import argparse
import pathlib
import subprocess

import dragon
from dragon.data.ddict.ddict import DDict, PosixCheckpointPersister, DAOSCheckpointPersister
from multiprocessing import set_start_method

daos_total_time = []
posix_total_time = []

class TestTimer:

    def __init__(self):

        self._t0 = time.clock_gettime_ns(time.CLOCK_MONOTONIC)

    def elapsed(self) -> float:

        return 1.0e-9 * (time.clock_gettime_ns(time.CLOCK_MONOTONIC) - self._t0)

class TestDDictChkptDump:

    def __init__(self, persister, parser):
        self._value_size = parser.value_size
        self._chkpt_size = parser.chkpt_size
        self._num_nodes = parser.num_nodes
        self._nclients = parser.nclients
        self._managers_per_node = parser.managers_per_node
        self._total_mem_size = int(parser.total_mem_size * (1024**3))
        self._persister = persister
        self._trace = parser.trace

        self._persist_path_created = False
        if persister == PosixCheckpointPersister:
            self._persist_path = "./posix_persister"
            if not os.path.exists(self._persist_path):
                os.makedirs(self._persist_path, exist_ok=True)
                self._persist_path_created = True
        else:
            self._persist_path = "testpool"
        self._working_set_size = 2
        self._dd = DDict(
            self._managers_per_node,
            self._num_nodes,
            total_mem=self._total_mem_size,
            working_set_size=self._working_set_size,
            wait_for_keys=True,
            persist_path=self._persist_path,
            persist_count=-1,
            persist_freq=1,
            persister_class=persister,
            trace=True,
        )

        self._num_key_per_chkpt = parser.chkpt_size // self._value_size

        self._kvs = {}
        self._random_kvs()

    def stats(self):
        print(f"{self._num_key_per_chkpt=}\n{self._chkpt_size=}", flush=True)

    def _random_kvs(self):
        key_size = 1024 # bytes
        for i in range(self._num_key_per_chkpt):
            key = os.urandom(key_size)
            val = os.urandom(self._value_size)
            self._kvs[key] = val

    def _fill_chkpt(self):
        for k in self._kvs:
            self._dd[k] = self._kvs[k]

    def write_and_persist_current_chkpt(self):
        self._fill_chkpt()
        t = TestTimer()
        self._dd.persist()
        total_time = t.elapsed()
        if self._trace:
            print(f"{self._persister} persisted one chkpt, time: {total_time}s", flush=True)
        if self._persister == PosixCheckpointPersister:
            posix_total_time.append(total_time)
        elif self._persister == DAOSCheckpointPersister:
            daos_total_time.append(total_time)


    def destroy(self):
        # cleanup files
        name = self._dd.get_name()
        self._dd.destroy()
        if self._persister == PosixCheckpointPersister:
            for p in pathlib.Path(self._persist_path).glob(f"{name}_*.ddict"):
                os.remove(p)
            if self._persist_path_created:
                os.rmdir(self._persist_path)
                self._persist_path_created = False
        else:
            subprocess.run(f"daos cont destroy {self._persist_path} {name}", shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="DDict persister performance test")
    parser.add_argument(
        "--value_size",
        type=int,
        default=1024,
        help="size of the value (bytes) that are stored in the dict",
    )
    parser.add_argument(
        "--chkpt_size",
        type=int,
        default=1048576, # 1 MB
        help="size of the persisting checkpoint (bytes)",
    )
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
        "--trace",
        action="store_true",
        help="trace time performance for each iteration",
    )
    parser.add_argument(
        "--benchmarks",
        action="store_true",
        help="run with a list of different sizes of checkpoint and get the performance comparison",
    )

    my_args = parser.parse_args()
    set_start_method("dragon")

    if my_args.benchmarks:
        for chkpt_sz in range(100, 10001, 100):
            posix_total_time = []
            daos_total_time = []

            for i in range(10):
                print(f"========== size {chkpt_sz}, round {i} ==========", flush=True)
                my_args.chkpt_size = chkpt_sz * (1024**2)
                test_daos_dump = TestDDictChkptDump(DAOSCheckpointPersister, my_args)
                test_daos_dump.write_and_persist_current_chkpt()
                test_daos_dump.destroy()
                assert (i+1) == len(daos_total_time)
                res = f"DAOS persister: {sum(daos_total_time)/len(daos_total_time)}"
                print(res, flush=True)

                test_posix_dump = TestDDictChkptDump(PosixCheckpointPersister, my_args)
                test_posix_dump.write_and_persist_current_chkpt()
                # test_posix_dump.stats()
                test_posix_dump.destroy()
                assert (i+1) == len(posix_total_time)
                res = f"POSIX persister: {sum(posix_total_time)/len(posix_total_time)}"
                print(res, flush=True)
    else:
        posix_total_time = []
        daos_total_time = []
        test_daos_dump = TestDDictChkptDump(DAOSCheckpointPersister, my_args)
        test_daos_dump.write_and_persist_current_chkpt()
        test_daos_dump.stats()
        test_daos_dump.destroy()
        res = f"DAOS persister: {daos_total_time[0]}"
        print(res, flush=True)

        test_posix_dump = TestDDictChkptDump(PosixCheckpointPersister, my_args)
        test_posix_dump.write_and_persist_current_chkpt()
        test_posix_dump.destroy()
        res = f"POSIX persister: {posix_total_time[0]}"
        print(res, flush=True)

