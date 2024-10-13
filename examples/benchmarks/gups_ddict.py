import time
import dragon
from multiprocessing import Barrier, Pool, Queue, Event, set_start_method
from enum import IntEnum, auto
from dataclasses import dataclass
from math import ceil, floor

import random
import argparse
import queue

import numpy as np

from dragon.data.ddict.ddict import DDict
import dragon.infrastructure.parameters as dp
from dragon.native.machine import System

DEFAULT_BLK_SIZE = 4096

class TestTimer:

    def __init__(self):

        self._t0 = time.clock_gettime_ns(time.CLOCK_MONOTONIC)

    def elapsed(self) -> float:

        return 1.0e-9 * (time.clock_gettime_ns(time.CLOCK_MONOTONIC) - self._t0)


class DDictTestOp(IntEnum):
    PUT = auto()
    GET = auto()


@dataclass
class DDictTest:
    op: DDictTestOp = DDictTestOp.PUT
    iterations: int = 100
    skips: int = 1
    value_bytes: int = 1024
    nkeys: int = 1
    key_bytes: int = 128


class DDictTester:

    def __init__(self, the_dict: DDict, the_test: DDictTest):

        self._dict = the_dict
        self._test = the_test

        rng = np.random.default_rng(seed=dp.this_process.my_puid)

        nwords = self._test.value_bytes // np.dtype(float).itemsize
        self._value = rng.random(nwords)

        nwords = self._test.key_bytes // np.dtype(float).itemsize
        self._keys = [rng.random(nwords) for _ in range(self._test.nkeys)]

    def run_test(self) -> float:

        if self._test.op == DDictTestOp.PUT:
            return self._run_put_test()

    def _run_put_test(self) -> float:

        i = 0
        tot_iters = self._test.iterations + self._test.skips
        t = None

        while i < tot_iters:
            i += 1
            if i == self._test.skips:
                t = TestTimer()

            for k in self._keys:
                try:
                    self._dict[k] = self._value
                except Exception as e:
                    print(f"Client raised exception on DDict op: {e}", flush=True)
                    time.sleep(30)

        t_end = t.elapsed()

        for k in self._keys:
            del self._dict[k]

        return ((self._test.nkeys * self._test.iterations) / t_end)


class DDictGUPS:

    def __init__(self, num_ddict_nodes: int = 1, num_ddict_managers_per_node: int = 1, num_clients: int = 1,
                 ddict_gbytes: int = 1, start_value_bytes: int = 1024, end_value_bytes: int = (1024**3),
                 start_iterations: int = 100, mem_frac: float = 0.01, trace: bool = False, print_stats = False):

        self._ddict_nodes = num_ddict_nodes
        self._ddict_nmgrs = num_ddict_managers_per_node
        self._nclients    = num_clients
        self._ddict_gbs   = ddict_gbytes
        self._start_bytes = start_value_bytes
        self._end_bytes   = end_value_bytes
        self._start_iters = start_iterations
        self._mem_frac    = mem_frac
        self._print_stats = print_stats

        self._tot_dict_bytes = int(self._ddict_gbs * (1024**3))
        self._ddict = DDict(self._ddict_nmgrs, self._ddict_nodes, self._tot_dict_bytes, timeout=None, trace=trace)

        # create a Barrier used to sync clients with
        self._barr = Barrier((self._nclients + 1))

        # create Queues for communicating work and results
        self._inq  = Queue()
        self._outq = Queue()

        # create a stopping Event
        self._stop_ev = Event()

        # create a Pool of all the testers
        self._pool = Pool(self._nclients, initializer=self._tester_loop, initargs=(self._ddict, self._barr, self._inq,
                                                                                   self._outq, self._stop_ev))

    @classmethod
    def _tester_loop(cls, _ddict: DDict, _barr: Barrier, _inq: Queue, _outq: Queue, _stop_ev: Event) -> None:

        while True:

            # get the next test work item
            keep_checking = True
            next_test = None
            while keep_checking:
                try:
                    next_test = _inq.get(timeout=0.5)
                    keep_checking = False
                except queue.Empty:
                    if _stop_ev.is_set():
                        keep_checking = False

            if next_test is None:
                break

            ddict_test = DDictTester(_ddict, next_test)
            _barr.wait()
            result_time = ddict_test.run_test()

            _outq.put(result_time)

    def driver(self):

        print(f" Value [B]  Iters  keys/client  min(ops/s)  max(ops/s)  sum(ops/s)   sum(GB/s)", flush=True)

        def_test = DDictTest()
        val_bytes = self._start_bytes
        iters = self._start_iters
        isize = 1
        while val_bytes <= self._end_bytes:

            blks_per_key = ceil(float(def_test.key_bytes) / float(DEFAULT_BLK_SIZE)) + 4  # extra is for pickle header
            blks_per_val = ceil(float(val_bytes) / float(DEFAULT_BLK_SIZE)) + 4  # extra is for pickle header
            op_blks      = self._nclients * (blks_per_key + blks_per_val)
            avail_blks   = floor(float(self._mem_frac * self._tot_dict_bytes) / DEFAULT_BLK_SIZE)

            nkeys = avail_blks // op_blks

            if nkeys < 1:
                print(f"Calculated number of keys per client is zero. No test for {val_bytes} B to run.", flush=True)
                break

            the_test = DDictTest(op=DDictTestOp.PUT, iterations=iters, value_bytes=val_bytes, nkeys=nkeys)
            self._ddict.clear()
            if self._print_stats:
                print(f'{self._ddict.stats=}', flush=True)
            for _ in range(self._nclients):
                self._inq.put(the_test)

            self._barr.wait()

            minv = 1.0e6
            maxv = -1.0e6
            avgv = 0.0
            for _ in range(self._nclients):
                r = self._outq.get()
                minv = min(minv, r)
                maxv = max(maxv, r)
                avgv += r

            agg_bw = avgv * val_bytes / (1024**3)
            print(f"{val_bytes:10.0f} {iters:6.0f} {nkeys:12.0f} {minv:11.3E} {maxv:11.3E} {avgv:11.3E} {agg_bw:11.3E}", flush=True)

            if val_bytes <= DEFAULT_BLK_SIZE:
                iters = max(1, int(float(iters) * 0.5)) # to account for number of keys not changing until we hit the block size
            else:
                iters += int(isize * 1.2) # more iterations because fewer ops needed to fill ddict
                isize += 1
            val_bytes *= 2

    def stop(self):

        self._stop_ev.set()
        self._pool.close()
        self._pool.join()
        if self._print_stats:
            print("Final Stats for DDict")
            print("+++++++++++++++++++++")
            print(self._ddict.stats)
        self._ddict.destroy()



if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='GUPS-like DDict Benchmark')
    parser.add_argument('--value_size_min', type=int, default=1024,
                        help='minimum size of the value (bytes) that are stored in the dict')
    parser.add_argument('--value_size_max', type=int, default=(1024**2),
                        help='maximum size of the value (bytes) that are stored in the dict')
    parser.add_argument('--num_nodes', type=int, default=1,
                        help='number of nodes the dictionary distributed across')
    parser.add_argument('--nclients', type=int, default=16,
                        help='number of client processes performing operations on the dict')
    parser.add_argument('--managers_per_node', type=int, default=1,
                        help='number of managers per node for the dragon dict')
    parser.add_argument('--total_mem_size', type=float, default=0.25,
                        help='total managed memory size for dictionary in GB')
    parser.add_argument('--mem_frac', type=float, default=0.01,
                        help='fraction of total_mem_size to use for keys+values')
    parser.add_argument('--iterations', type=int, default=10,
                        help='number of iterations at the minimum value size')
    parser.add_argument('--trace_ddict', action='store_true',
                        help='enable DDict tracing - reduces performance')
    parser.add_argument('--stats', type=bool, default=False,
                        help='print ddict stats for each iteration')

    my_args = parser.parse_args()
    set_start_method("dragon")
    my_system = System()

    if my_args.num_nodes > my_system.nnodes:
        raise ValueError("More nodes requested for the DDict than available in the runtime")

    print("DDict GUPS Benchmark", flush=True)
    print(f"  Running on {my_system.nnodes} nodes (nclients={my_args.nclients})", flush=True)
    print(f"  {(my_args.managers_per_node * my_args.num_nodes)} DDict managers", flush=True)
    print(f"  {my_args.num_nodes} DDict nodes", flush=True)
    print(f"  {my_args.total_mem_size} GB total DDict memory ({my_args.mem_frac * my_args.total_mem_size} GB for keys+values)", flush=True)
    if my_args.trace_ddict:
        print("  DDict tracing ON", flush=True)
    print("", flush=True)

    the_test = DDictGUPS(num_ddict_nodes=my_args.num_nodes, num_ddict_managers_per_node=my_args.managers_per_node,
                         num_clients=my_args.nclients, ddict_gbytes=my_args.total_mem_size,
                         start_value_bytes=my_args.value_size_min, end_value_bytes=my_args.value_size_max,
                         start_iterations=my_args.iterations, mem_frac=my_args.mem_frac, trace=my_args.trace_ddict, print_stats=my_args.stats)

    the_test.driver()
    the_test.stop()
