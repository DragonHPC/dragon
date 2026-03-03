import os
import time
import argparse
import pathlib
import subprocess
import random
import string

import dragon
from dragon.data.ddict.ddict import DDict, PosixCheckpointPersister, DAOSCheckpointPersister, NULLCheckpointPersister
from dragon.native.queue import Queue
from dragon.native.barrier import Barrier
from dragon.native.process_group import ProcessGroup
from dragon.native.process import ProcessTemplate

try:
    import pydaos
    DAOS_EXISTS = True
except:
    DAOS_EXISTS = False

from multiprocessing import set_start_method

daos_total_time = []
posix_total_time = []

DAOS_POOL_NAME = "dragontestpool"
NUM_KEYS_PER_CLIENT = 100

class TestTimer:

    def __init__(self):

        self._t0 = time.clock_gettime_ns(time.CLOCK_MONOTONIC)

    def stop(self):
        self._t1 = time.clock_gettime_ns(time.CLOCK_MONOTONIC)
        self._t_elapsed = 1.0e-9 * (self._t1 - self._t0)

    @property
    def start_timestamp(self):
        return self._t0

    @property
    def stop_timestamp(self):
        return self._t1

    @property
    def elapsed_time(self):
        return self._t_elapsed

class Tester:
    def __init__(self, dd, iterations, num_keys, value, key_size):

        self._keys = [''.join(random.choices(string.ascii_letters, k=key_size)) for _ in range(num_keys)]
        self._num_keys = num_keys
        self._value = value
        self._dd = dd
        self._iterations = iterations

    def run_chkpt_persistence(self, bar, res_queue):
        for _ in range(self._iterations):
            timer = TestTimer()
            for k in self._keys:
                self._dd[k] = self._value
            self._dd.checkpoint()
            timer.stop()
            bar.wait()
            res_queue.put((self._dd._client_id, timer.elapsed_time))
    def run(self):
        for k in self._keys:
            self._dd[k] = self._value


def proc_func(dd, bar, daos_ddict_io_test, id, name, num_nodes, iterations, res_queue, bar_start, bar_end, num_keys, value_size, key_size):
    # assign a local manager to each client process
    original_dd = dd
    if dd is None: # use daos dict
        dcont = pydaos.DCont(DAOS_POOL_NAME, name)
        dd = dcont.get(name)
    else:
        local_managers = original_dd.local_managers
        select_manager = local_managers[id // num_nodes]
        dd = original_dd.manager(select_manager)

    value = ''.join(random.choices(string.ascii_letters, k=value_size))
    tester = Tester(dd, iterations, num_keys, value, key_size)
    bar_start.wait()

    if isinstance(dd, DDict) and not daos_ddict_io_test:
        tester.run_chkpt_persistence(bar, res_queue)
    else:
        tester.run()

    bar_end.wait()

    if isinstance(dd, DDict) and not daos_ddict_io_test:
        dd.sync_to_newest_checkpoint()
        assert len(dd) == num_keys



class TestDDictChkptDump:

    def __init__(self, persister, parser, value_size, dict_type=None):
        self._key_size = parser.key_size
        self._value_size = value_size
        self._chkpt_size = parser.size_per_client * parser.nclients_per_node * parser.num_nodes * (1024**2)
        self._num_nodes = parser.num_nodes
        self._iterations = parser.iterations
        self._nclients = parser.nclients_per_node * self._num_nodes
        self._managers_per_node = parser.managers_per_node
        self._working_set_size = my_args.working_set_size
        self._total_mem_size = int(self._chkpt_size * 4)
        self._persister = persister
        self._trace = parser.trace
        self._daos_ddict_io_test = parser.daos_ddict_io_test

        self._data_size_per_client_mb = self._chkpt_size / self._nclients / (1024**2)
        print(f"data size (MB)/client: {self._data_size_per_client_mb}", flush=True)

        self._persist_path_created = False
        self._persist_path = ""
        persist_freq = 0
        if not self._daos_ddict_io_test:
            if persister == PosixCheckpointPersister:
                persist_freq = 1
                self._persist_path = f"./posix_persister_N{self._num_nodes}_size{self._data_size_per_client_mb}"
                if not os.path.exists(self._persist_path):
                    os.makedirs(self._persist_path, exist_ok=True)
                    self._persist_path_created = True
            elif persister == DAOSCheckpointPersister:
                persist_freq = 1
                self._persist_path = DAOS_POOL_NAME

        self._working_set_size = 2

        self._dd = None
        print(f"{dict_type=}, {self._daos_ddict_io_test=}", flush=True)

        if dict_type is not None or not self._daos_ddict_io_test:
            self._dd = DDict(
                self._managers_per_node,
                self._num_nodes,
                total_mem=self._total_mem_size,
                working_set_size=self._working_set_size,
                wait_for_keys=True,
                persist_path=self._persist_path,
                persist_count=1,
                persist_freq=persist_freq,
                persister_class=persister if persister is not None else NULLCheckpointPersister,
                trace=True,
            )
            self._name = self._dd.get_name()
            print(f"DDict name: {self._name}", flush=True)
        else:
            # destroy existing daos pool/container if any
            self._name = ''.join(random.choices(string.ascii_lowercase, k=8))
            print(f"DAOS dict name: {self._name}", flush=True)
            result = subprocess.run(f"daos cont create {DAOS_POOL_NAME} {self._name} --type PYTHON", shell=True, capture_output=True, text=True)
            print("Stdout:", result.stdout)
            print("Stderr:", result.stderr)
            print("Return code:", result.returncode)
            if result.returncode != 0:
                raise RuntimeError(f"Failed to create DAOS container {self._name} in pool {DAOS_POOL_NAME}")
            dcont = pydaos.DCont(DAOS_POOL_NAME, self._name)
            dd = dcont.dict(f"{self._name}", {}) # create a daos dict


        self._num_key_per_chkpt = self._chkpt_size // (self._value_size+self._key_size)
        self._num_keys_per_client = self._num_key_per_chkpt // self._nclients
        assert self._num_keys_per_client == NUM_KEYS_PER_CLIENT


        self._start_bar = Barrier(parties=self._nclients)
        self._end_bar = Barrier(parties=self._nclients)
        self._bar = Barrier(parties=self._nclients)
        self._res_queue = Queue()


        self._pg = ProcessGroup(restart=False)
        for i in range(self._nclients):
            self._pg.add_process(
                nproc=1,
                template=ProcessTemplate(
                    target=proc_func,
                    args=(
                        self._dd,
                        self._bar,
                        self._daos_ddict_io_test,
                        i,
                        self._name,
                        self._num_nodes,
                        self._iterations,
                        self._res_queue,
                        self._start_bar,
                        self._end_bar,
                        self._num_keys_per_client,
                        self._value_size,
                        1024,
                    ),
                )
            )

        self._pg.init()

    def start(self):
        self._pg.start()
        self._timer = TestTimer()

        res_dict = {}

        for _ in range(self._iterations * self._nclients):
            clientID, elapsed = self._res_queue.get()
            if clientID not in res_dict:
                res_dict[clientID] = []
            res_dict[clientID].append(elapsed)

        self._res_dict = res_dict

    def stats(self):
        print(f"{self._num_key_per_chkpt=}\n{self._chkpt_size=}", flush=True)

    def stop(self):
        self._pg.join()
        self._pg.close()
        self._timer.stop()

        self._num_persisted_chkpts = self._iterations - self._working_set_size
        if self._dd is not None and not self._daos_ddict_io_test:
            self._dd.sync_to_newest_checkpoint()
            self._dd.sync()

        if self._dd is None:
            dcont = pydaos.DCont(DAOS_POOL_NAME, self._name)
            dd = dcont.get(self._name)
            print(f"{len(dd)=}", flush=True)

        max_client_time = 0
        min_client_time = float('inf')
        for clientID in self._res_dict:
            client_time = sum(self._res_dict[clientID])
            max_client_time = max(max_client_time, client_time)
            min_client_time = min(min_client_time, client_time)

        self._total_elapsed = max_client_time

        self._min_elapsed = min_client_time
        self._max_elapsed = max_client_time
        self._total_time_pg = self._timer.elapsed_time

    def destroy(self):
        # cleanup files
        if self._dd is not None:
            self._dd.destroy()

        # cleanup persisted chkpt data
        if not self._daos_ddict_io_test:
            if self._persister == PosixCheckpointPersister:
                for p in pathlib.Path(self._persist_path).glob(f"{self._name}_*.ddict"):
                    os.remove(p)
                if self._persist_path_created:
                    os.rmdir(self._persist_path)
                    self._persist_path_created = False
            elif self._persister == DAOSCheckpointPersister:
                result = subprocess.run(f"daos cont destroy {DAOS_POOL_NAME} {self._name}", shell=True, capture_output=True, text=True)
                print("Stdout:", result.stdout)
                print("Stderr:", result.stderr)
                print("Return code:", result.returncode)
        elif self._dd is None:
            result = subprocess.run(f"daos cont destroy {DAOS_POOL_NAME} {self._name}", shell=True, capture_output=True, text=True)
            print("Stdout:", result.stdout)
            print("Stderr:", result.stderr)
            print("Return code:", result.returncode)

    def get_results(self):
        return self._num_persisted_chkpts, self._data_size_per_client_mb, self._total_elapsed, self._total_time_pg, self._min_elapsed, self._max_elapsed


def get_args():

    parser = argparse.ArgumentParser(description="DDict persister performance test")
    parser.add_argument(
        "--key_size",
        type=int,
        default=1024,
        help="size of the key (bytes) that are stored in the dict",
    )
    parser.add_argument(
        "--size_per_client",
        type=int,
        default=128,
        help="size of the data (MB) that are written by each client",
    )
    parser.add_argument(
        "--num_nodes",
        type=int,
        default=1,
        help="number of nodes the dictionary distributed across",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=16,
        help="number of iterations"
    )
    parser.add_argument(
        "--managers_per_node",
        type=int,
        default=8,
        help="number of managers per fnode for the dragon dict",
    )
    parser.add_argument(
        "--nclients_per_node",
        type=int,
        default=8,
        help="number of client processes for each node",
    )
    parser.add_argument(
        "--trace",
        action="store_true",
        help="trace time performance for each iteration",
    )
    parser.add_argument(
        "--working_set_size",
        type=int,
        default=2,
        help="working set size for the dragon dict",
    )
    parser.add_argument(
        "--persister",
        type=str,
        default="NULLCheckpointPersister",
        help="checkpoint persister to use",
    )
    parser.add_argument(
        "--benchmarks",
        action="store_true",
        help="run with all supported persisted backend and compare performance",
    )
    parser.add_argument(
        "--daos_ddict_io_test",
        action="store_true",
        help="write key value pairs to DAOS and DDict and compare performance",
    )

    my_args = parser.parse_args()

    return my_args


if __name__ == "__main__":

    my_args = get_args()

    set_start_method("dragon")

    if not my_args.daos_ddict_io_test:
        ## Use
        persisters = [NULLCheckpointPersister, PosixCheckpointPersister]

        if my_args.benchmarks:
            if DAOS_EXISTS:
                persisters = [NULLCheckpointPersister, DAOSCheckpointPersister, PosixCheckpointPersister]
        else:
            if my_args.persister == "NULLCheckpointPersister":
                persisters = [NULLCheckpointPersister]
            elif my_args.persister == "DAOSCheckpointPersister":
                if not DAOS_EXISTS:
                    raise RuntimeError("Cannot import DAOS module, please make sure the PYTHONPATH is set correctly.")
                persisters = [DAOSCheckpointPersister]
            elif my_args.persister == "PosixCheckpointPersister":
                persisters = [PosixCheckpointPersister]

        chkpt_size = my_args.size_per_client * my_args.nclients_per_node * my_args.num_nodes * (1024**2)
        for persister in persisters:
            values_size = chkpt_size - (NUM_KEYS_PER_CLIENT * my_args.nclients_per_node*my_args.num_nodes * my_args.key_size) # default key size is 1024 bytes
            value_size = values_size //(NUM_KEYS_PER_CLIENT * my_args.nclients_per_node * my_args.num_nodes)

            test_ddict_obj = TestDDictChkptDump(persister, my_args, value_size)
            test_ddict_obj.start()
            test_ddict_obj.stats()
            test_ddict_obj.stop()
            test_ddict_obj.destroy()
            num_chkpts_persisted, data_size_per_client_mb, total_elapsed, total_time_pg, min_elapsed, max_elapsed = test_ddict_obj.get_results()
            print(f"\n\n\nNumber of nodes:{my_args.num_nodes}	\nNumber of Persisted Chkpts	data size(MB)/client  {persister.__name__} time(s)   total time(s) PG  min time(s)   max time(s)", flush=True)
            print(f"{num_chkpts_persisted:25}	{data_size_per_client_mb:18.5f}	{total_elapsed:15.5f}	{total_time_pg:15.5f}	{min_elapsed:15.5f}	{max_elapsed:15.5f}\n\n", flush=True)

            print(test_ddict_obj._res_dict, flush=True)

    else:

        chkpt_size = my_args.size_per_client * my_args.nclients_per_node * my_args.num_nodes * (1024**2)
        values_size = chkpt_size - (NUM_KEYS_PER_CLIENT * my_args.nclients_per_node*my_args.num_nodes * my_args.key_size) # default key size is 1024 bytes
        value_size = values_size //(NUM_KEYS_PER_CLIENT * my_args.nclients_per_node * my_args.num_nodes)

        print(f"Run DDict vs DAOS IO Performance Test\n\n", flush=True)
        results = []
        for i in range(11):
            test_ddict_obj = TestDDictChkptDump(None, my_args, value_size, dict_type=DDict)
            test_ddict_obj.start()
            test_ddict_obj.stats()
            test_ddict_obj.stop()
            test_ddict_obj.destroy()
            num_chkpts_persisted, data_size_per_client_mb, total_elapsed = test_ddict_obj.get_results()
            print(f"data size(MB)/client  DDict IO time(s)", flush=True)
            print(f"{data_size_per_client_mb:18.5f}	{total_elapsed:15.5f}", flush=True)
            print("----------------------------------------------------\n", flush=True)
            results.append(total_elapsed)
        avg_time = sum(results[1:]) / (len(results)-1)
        min_time = min(results[1:])
        max_time = max(results[1:])
        print(f"Average DDict IO time (excluding first run): {avg_time:15.5f} seconds", flush=True)
        print(f"Min DDict IO time (excluding first run): {min_time:15.5f} seconds", flush=True)
        print(f"Max DDict IO time (excluding first run): {max_time:15.5f} seconds", flush=True)