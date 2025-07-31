import time
import dragon
from multiprocessing import Barrier, Queue, Event, set_start_method, cpu_count
from enum import IntEnum, auto
from dataclasses import dataclass
from typing import List

import random
import argparse
import queue

import numpy as np

from dragon.data import DDict
import dragon.infrastructure.parameters as dp
from dragon.native.machine import System, Node
from dragon.native import Pool
from dragon.infrastructure.policy import Policy


class TestTimer:

    def __init__(self):

        self._t0 = time.clock_gettime_ns(time.CLOCK_MONOTONIC)

    def elapsed(self) -> float:

        return 1.0e-9 * (time.clock_gettime_ns(time.CLOCK_MONOTONIC) - self._t0)


class DDictTestOp(IntEnum):
    PUT = auto()
    BPUT = auto()
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

    def run_test(self, barrier) -> float:
        if self._test.op == DDictTestOp.PUT:
            return self._run_put_test()
        elif self._test.op == DDictTestOp.BPUT:
            return self._run_put_test(bput=True)
        else:
            return self._run_get_test(barrier)

    def _run_put_test(self, bput=False) -> float:
        i = 0
        tot_iters = self._test.iterations + self._test.skips
        t = None

        while i < tot_iters:
            i += 1
            if i == self._test.skips:
                t = TestTimer()

            if bput:
                self._dict.start_batch_put()
            for k in self._keys:
                try:
                    self._dict[k] = self._value
                except Exception as e:
                    raise RuntimeError(f"Client raised exception on DDict op: {e}")
            if bput:
                self._dict.end_batch_put()

        t_end = t.elapsed()

        return (self._test.nkeys * self._test.iterations) / t_end

    def _run_get_test(self, freeze_barrier) -> float:
        i = 0
        tot_iters = self._test.iterations + self._test.skips
        t = None

        for k in self._keys:
            try:
                self._dict[k] = self._value
            except Exception as e:
                raise RuntimeError(f"Client raised exception on DDict op: {e}")

        freeze_barrier.wait() # wait for others to all finish storing into DDict
        freeze_barrier.wait() # Now DDict is frozen.

        while i < tot_iters:
            i += 1
            if i == self._test.skips:
                t = TestTimer()

            for k in self._keys:
                try:
                    v = self._dict[k]
                except Exception as e:
                    raise RuntimeError(f"Client raised exception on DDict op: {e}")

        t_end = t.elapsed()

        freeze_barrier.wait() # Now wait for all clients to finish doing gets.

        return (self._test.nkeys * self._test.iterations) / t_end


class DDictGUPS:

    OPS = {"put": DDictTestOp.PUT, "bput": DDictTestOp.BPUT, "get": DDictTestOp.GET}

    def __init__(
        self,
        num_ddict_nodes: int = 1,
        num_ddict_managers_per_node: int = 1,
        num_clients: int = 1,
        ddict_gbytes: int = 1,
        start_value_bytes: int = 1024,
        end_value_bytes: int = (1024**3),
        start_iterations: int = 100,
        mem_frac: float = 0.01,
        operation: str = "put",
        max_keys: int = 0,
        trace: bool = False,
        print_stats: bool = False,
        nodes: List[Node] = None,
    ):

        self._ddict_nodes = num_ddict_nodes
        self._ddict_nmgrs = num_ddict_managers_per_node
        self._nclients = num_clients
        self._ddict_gbs = ddict_gbytes
        self._start_bytes = start_value_bytes
        self._end_bytes = end_value_bytes
        self._start_iters = start_iterations
        self._mem_frac = mem_frac
        self._max_keys = max_keys
        self._print_stats = print_stats
        self._tot_dict_bytes = int(self._ddict_gbs * (1024**3))
        try:
            self._op = self.OPS[operation]
        except:
            raise ValueError("invalid test operation given")

        policies = []
        if nodes is not None:
            for node in nodes:
                policies.append(Policy(placement=Policy.Placement.HOST_NAME, host_name=node.hostname))

            self._ddict = DDict(
                n_nodes=None,
                managers_per_node=None,
                policy=policies,
                managers_per_policy=self._ddict_nmgrs,
                total_mem=self._tot_dict_bytes,
                trace=trace,
            )

        else:
            self._ddict = DDict(
                managers_per_node=self._ddict_nmgrs,
                n_nodes=self._ddict_nodes,
                total_mem=self._tot_dict_bytes,
                trace=trace,
            )

        # create a Barrier used to sync clients with
        self._barr = Barrier((self._nclients + 1))
        self._freeze_barr = Barrier((self._nclients + 1))

        # create Queues for communicating work and results
        self._inq = Queue()
        self._outq = Queue()

        # create a stopping Event
        self._stop_ev = Event()

        # create a Pool of all the testers
        if nodes is not None:
            self._pool = Pool(
                processes_per_policy=(self._nclients // len(policies)),
                policy=policies,
                initializer=self._tester_loop,
                initargs=(self._ddict, self._barr, self._freeze_barr, self._inq, self._outq, self._stop_ev),
            )
        else:
            self._pool = Pool(
                self._nclients,
                initializer=self._tester_loop,
                initargs=(self._ddict, self._barr, self._freeze_barr, self._inq, self._outq, self._stop_ev),
            )

    @classmethod
    def _tester_loop(cls, _ddict: DDict, _barr: Barrier, _freeze_barrier: Barrier, _inq: Queue, _outq: Queue, _stop_ev: Event) -> None:

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
            result_time = ddict_test.run_test(_freeze_barrier)

            _outq.put(result_time)

    def driver(self):

        print(
            f" Value [B]  Iters  keys/client  min(ops/s)  max(ops/s)  sum(ops/s)   sum(GB/s)",
            flush=True,
        )

        def_test = DDictTest()
        val_bytes = self._start_bytes
        iters = self._start_iters
        isize = 1
        while val_bytes <= self._end_bytes:

            # lets assume we want to align on 32 bytes, but note this is imprecise because we're not accounting for
            # any pickle overhead
            kvbytes = self._nclients * 32 * ((def_test.key_bytes + val_bytes) // 32)
            nkeys = int((self._mem_frac * self._tot_dict_bytes) / kvbytes)
            if self._max_keys > 0:
                nkeys = min(self._max_keys, nkeys)

            if nkeys < 1:
                print(
                    f"Calculated number of keys per client is zero. No test for {val_bytes} B to run.",
                    flush=True,
                )
                break

            the_test = DDictTest(op=self._op, iterations=iters, value_bytes=val_bytes, nkeys=nkeys)
            self._ddict.clear()

            if self._print_stats:
                print(f"{self._ddict.stats=}", flush=True)
            for _ in range(self._nclients):
                self._inq.put(the_test)

            self._barr.wait()

            if self._op == DDictTestOp.GET:
                self._freeze_barr.wait() # Wait for all puts to be done.
                self._ddict.freeze()
                self._freeze_barr.wait() # Now tell clients to continue with get ops.
                self._freeze_barr.wait() # Now wait for clients to finish gets.
                self._ddict.unfreeze()

            minv = 1.0e6
            maxv = -1.0e6
            avgv = 0.0
            for _ in range(self._nclients):
                r = self._outq.get()
                minv = min(minv, r)
                maxv = max(maxv, r)
                avgv += r

            agg_bw = avgv * val_bytes / (1024**3)
            print(
                f"{val_bytes:10.0f} {iters:6.0f} {nkeys:12.0f} {minv:11.3E} {maxv:11.3E} {avgv:11.3E} {agg_bw:11.3E}",
                flush=True,
            )

            val_bytes *= 2
            if val_bytes < (128 * 1024) or val_bytes > (32 * 1024**2):
                iters = max(
                    1, int(float(iters) * 0.7)
                )  # to account for number of keys not changing until we hit the block size
            else:
                iters += int(isize * 1.2)  # more iterations because fewer ops needed to fill ddict
                isize += 1

    def stop(self):

        self._stop_ev.set()
        self._pool.close()
        self._pool.join()
        if self._print_stats:
            print("Final Stats for DDict")
            print("+++++++++++++++++++++")
            print(self._ddict.stats)
        self._ddict.destroy()


def get_args(arg_dict=None):
    parser = argparse.ArgumentParser(description="GUPS-like DDict Benchmark")
    parser.add_argument(
        "--max_keys",
        type=int,
        default=500,
        help="maximum number of keys per client or 0 to fill available DDict memory",
    )
    parser.add_argument(
        "--value_size_min",
        type=int,
        default=1024,
        help="minimum size of the value (bytes) that are stored in the dict",
    )
    parser.add_argument(
        "--value_size_max",
        type=int,
        default=(512 * 1024),
        help="maximum size of the value (bytes) that are stored in the dict",
    )
    parser.add_argument(
        "--num_nodes",
        type=int,
        default=1,
        help="number of nodes the dictionary is distributed across",
    )
    parser.add_argument(
        "--nclients",
        type=int,
        default=16,
        help="number of client processes performing operations on the dict",
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
        default=0.1,
        help="total managed memory size for dictionary in GB",
    )
    parser.add_argument(
        "--mem_frac",
        type=float,
        default=0.1,
        help="fraction of total_mem_size to use for keys+values",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=2,
        help="number of iterations at the minimum value size",
    )
    parser.add_argument(
        "--operation", choices={"put", "bput", "get"}, default="put", help="operation to to benchmark (put, bput, get)"
    )
    parser.add_argument(
        "--trace_ddict",
        action="store_true",
        help="enable DDict tracing - reduces performance",
    )
    parser.add_argument("--stats", type=bool, default=False, help="print ddict stats for each iteration")

    if arg_dict is None:
        return parser.parse_args()
    else:
        args = vars(parser.parse_args())
        merged = args.copy()
        merged.update(arg_dict)
        return argparse.Namespace(**merged)


def main(bargs=None, nodelist=None):
    my_args = get_args(bargs)

    my_system = System()
    nnodes = my_system.nnodes
    if nodelist is not None:
        nnodes = len(nodelist)

    if my_args.num_nodes > my_system.nnodes:
        raise ValueError("More nodes requested for the DDict than available in the runtime")

    print("DDict GUPS Benchmark", flush=True)
    print(
        f"  Running on {nnodes} nodes (nclients={my_args.nclients})",
        flush=True,
    )
    print(
        f"  {(my_args.managers_per_node * my_args.num_nodes)} DDict managers",
        flush=True,
    )
    print(f"  {my_args.num_nodes} DDict nodes", flush=True)
    print(
        f"  {my_args.total_mem_size} GB total DDict memory ({(my_args.mem_frac * my_args.total_mem_size):5.3E} GB for keys+values)",
        flush=True,
    )
    print(f"  Operation: {my_args.operation}", flush=True)
    if my_args.trace_ddict:
        print("  DDict tracing ON", flush=True)
    print("", flush=True)

    the_test = DDictGUPS(
        num_ddict_nodes=my_args.num_nodes,
        num_ddict_managers_per_node=my_args.managers_per_node,
        num_clients=my_args.nclients,
        ddict_gbytes=my_args.total_mem_size,
        start_value_bytes=my_args.value_size_min,
        end_value_bytes=my_args.value_size_max,
        start_iterations=my_args.iterations,
        mem_frac=my_args.mem_frac,
        operation=my_args.operation,
        max_keys=my_args.max_keys,
        trace=my_args.trace_ddict,
        print_stats=my_args.stats,
        nodes=nodelist,
    )

    the_test.driver()
    the_test.stop()


def benchit():
    my_system = System()
    mem_per_node = 18

    nnodes = 1
    while nnodes <= my_system.nnodes:
        nclients = nnodes * (cpu_count() // my_system.nnodes) // 4
        my_args = {
            "num_nodes": nnodes,
            "managers_per_node": 2,
            "nclients": nclients,
            "total_mem_size": (nnodes * mem_per_node),
            "value_size_min": 1024,
            "value_size_max": (64 * 1024**2),
            "iterations": 1,
            "mem_frac": 0.6,
            "max_keys": 256,
        }

        allnodes = my_system.nodes
        nodelist = [Node(h_uid) for h_uid in allnodes[0:nnodes]]

        my_args["operation"] = "put"
        main(my_args, nodelist=nodelist)

        my_args["operation"] = "bput"
        main(my_args, nodelist=nodelist)

        my_args["operation"] = "get"
        main(my_args, nodelist=nodelist)

        nnodes *= 2


if __name__ == "__main__":
    set_start_method("dragon")
    # main()
    benchit()
