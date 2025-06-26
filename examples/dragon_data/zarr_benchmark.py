import dragon
from multiprocessing import set_start_method, Pool, cpu_count, Event, Barrier

import zarr

from dragon.native.machine import System, Node, current
from dragon.data.zarr import Store

import time
import psutil
import numpy as np
import collections.abc
import argparse


class NullMapping(collections.abc.MutableMapping):
    "MutableMapping to nowhere."

    def __init__(self, skip=-1):
        self.untested_for_zgroup = skip

    def __getitem__(self, key):
        if key.endswith(".zgroup") and self.untested_for_zgroup:
            self.untested_for_zgroup -= 1
            return b'{\n    "zarr_format": 2\n}'
        raise KeyError(key)

    def __setitem__(self, key, value):
        pass

    def __delitem__(self, key):
        pass

    def __iter__(self):
        return (i for i in ())

    def __len__(self):
        return 0


def show_node_mem():
    mynode = current().name
    mem = int(psutil.virtual_memory().available / 1024**3)
    print(f"{mem} GB of available memory on {mynode}", flush=True)


def show_mem():
    p = Pool(System().nnodes, initializer=show_node_mem)
    p.close()
    p.join()


def hold_node_mem(reserve_mem_gb, bar, shutdown_ev):
    """Allocate a NumPy array to fill up the requested memory and otherwise sit here until the Event is set"""
    if reserve_mem_gb > 0:
        value_bytes = int(reserve_mem_gb * 1024**3)
        nwords = value_bytes // np.dtype(float).itemsize
        values = np.random.rand(nwords)
    bar.wait()
    shutdown_ev.wait()


def get_np_mem(mem_to_leave, ddict_gb):
    """Determine the size of a NumPy array to create to park some memory. The idea behind parking some memory is
    to emulate the fact that the dataset is bigger than system memory, which means OS caching of files is not
    possible.
    """
    my_system = System()
    num_nodes = my_system.nnodes
    total_mem = 0
    for huid in my_system.nodes:
        anode = Node(huid)
        total_mem += anode.physical_mem
    total_mem /= 1024**3
    tot_mem_to_leave = mem_to_leave * num_nodes
    return int(total_mem - ddict_gb - tot_mem_to_leave) // num_nodes


def get_args():
    parser = argparse.ArgumentParser(description="Benchmark for Zarr Store over the Dragon DDict")

    parser.add_argument(
        "--park_mem",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Hold on to some memory on each node",
    )
    parser.add_argument(
        "--mem_to_leave",
        type=int,
        default=200,
        help="GB of memory to leave after any held memory and DDict",
    )
    parser.add_argument(
        "--validate",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Check that DragonStore has valid values",
    )
    parser.add_argument(
        "--show_keys",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Have the DragonStore show what keys are being requested during slices",
    )
    parser.add_argument(
        "--test_copy_store",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Measure copy_store performance (may be very slow from disk)",
    )
    parser.add_argument(
        "--test_slices",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Measure copy_store performance of a set of time slices",
    )
    parser.add_argument(
        "--path",
        type=str,
        default="/scratch/ZSNS001.ome.zarr",
        help="Path to Zarr data (e.g., /lus/ZSNS001.ome.zarr)",
    )
    parser.add_argument(
        "--data_gb",
        type=int,
        default=201,
        help="Approximate size of dataset in GB (ZSNS001.ome.zarr is 201GB, ZSNS001_tail.ome.zarr is 61GB)",
    )

    return parser.parse_args()


if __name__ == "__main__":

    set_start_method("dragon", force=True)

    num_loaders = (
        cpu_count() // 2
    )  # have the DragonStore use a lot of workers to load (even if they won't all be busy)
    num_mem_parkers_per_node = (
        16  # more just speed up filling the space. They otherwise sleep
    )
    args = get_args()

    print(
        f"Running on {System().nnodes} nodes and will use {num_loaders} workers to load data",
        flush=True,
    )
    print(f"Working with data: {args.path}", flush=True)

    ddict_gb = (
        int(args.data_gb * 1.5)
    )
    np_gb = get_np_mem(args.mem_to_leave, ddict_gb)
    print(f"Will create a DragonStore with {ddict_gb} GB of total storage", flush=True)

    if args.park_mem:
        print(f"Will park {np_gb} GB of memory per node", flush=True)
        stop_ev = Event()
        sync_bar = Barrier(num_mem_parkers_per_node * System().nnodes + 1)
        mem_p_p = np_gb // num_mem_parkers_per_node
        res_p = Pool(
            num_mem_parkers_per_node * System().nnodes,
            initializer=hold_node_mem,
            initargs=(
                mem_p_p,
                sync_bar,
                stop_ev,
            ),
        )
        sync_bar.wait()

    show_mem()
    cold_zg = zarr.open(args.path, mode="r")
    print(f"\nOn-disk Store:\n{cold_zg.tree()}", flush=True)

    dstore = Store(
        nloaders=(cpu_count() // 2),
        managers_per_node=2,
        n_nodes=System().nnodes,
        total_mem=(ddict_gb * (1024**3)),
        path=args.path
    )

    t0 = time.time()
    loopmore = True
    while loopmore:
        try:
            tot_bytes = dstore.wait_on_load(30)
            loopmore = False
        except TimeoutError:
            tot_bytes = dstore.loaded_bytes
            print(f"Loaded bytes = {tot_bytes}", flush=True)
    tlap = time.time() - t0
    bw = tot_bytes / (tlap * 1024**3)
    print(
        f"Walltime to zarr.copy_store from disk to DragonStore: {tlap} ({tot_bytes} bytes loaded, {bw} GB/s)",
        flush=True,
    )

    show_mem()
    warm_zg = zarr.group(store=dstore)
    print(f"\nDragonStore:\n{warm_zg.tree()}", flush=True)

    if args.show_keys:
        # grab a time slice to show the sequence of keys that are grabbed when accessed this way
        dstore.show_gets(enabled=True)
        try:
            retval = warm_zg["/0"][100][:]
            retval = warm_zg["/1"][100][:]
            retval = warm_zg["/2"][100][:]
        except Exception as e:
            print("Failed trying to fetch data: {e}", flush=True)
        dstore.show_gets(enabled=False)

    if args.test_slices:
        skips = 0  # warmup
        iters = [3, 5, 11]
        for l in range(3):
            for i in range(iters[l]):
                tot_bytes = 0
                if i == skips:
                    t0 = time.time()
                    tot_bytes = 0
                for ts in range(100 + i, 701 + i, 100):
                    k = f"/{l}/{ts}"
                    b = zarr.copy_store(
                        cold_zg.store, NullMapping(False), source_path=k, dest_path=k
                    )
                    tot_bytes += b[2]
            tlap = time.time() - t0
            bw = tot_bytes / (tlap * 1024**3)
            savg = tlap / ((iters[l] - skips) * 7)
            print(
                f"Average walltime to pull a time slice [{l}] from disk: {savg} ({bw} GB/s, tot_bytes={tot_bytes})",
                flush=True,
            )

            for i in range(iters[l]):
                tot_bytes = 0
                if i == skips:
                    t0 = time.time()
                    dstore.reset_get_timer()
                    tot_bytes = 0
                for ts in range(100 + i, 701 + i, 100):
                    k = f"/{l}/{ts}"
                    b = zarr.copy_store(
                        warm_zg.store, NullMapping(False), source_path=k, dest_path=k
                    )
                    tot_bytes += b[2]
            tlap = time.time() - t0
            bw = tot_bytes / (tlap * 1024**3)
            savg = tlap / ((iters[l] - skips) * 7)
            dsavg = dstore.get_timer / ((iters[l] - skips) * 7)
            print(
                f"Average walltime to pull a time slice [{l}] from DragonStore: {savg} ({bw} GB/s, tot_bytes={tot_bytes}) (DDict time = {dsavg})",
                flush=True,
            )

    show_mem()

    # pick some slice and check all the values match between the groups
    if args.validate:
        valid = True
        for ts in range(100, 701, 100):
            wa = warm_zg["/2"]
            ca = cold_zg["/2"]
            if not np.array_equal(wa[ts, ...], ca[ts, ...]):
                valid = False
        if not valid:
            print(f"Zarr groups' data does NOT match!", flush=True)
        else:
            print(f"Zarr groups' data matches", flush=True)

    if args.test_copy_store:
        t0 = time.time()
        b = zarr.copy_store(cold_zg.store, NullMapping(False))
        tlap = time.time() - t0
        bw = b[2] / (tlap * 1024**3)
        print(
            f"Walltime to zarr.copy_store from disk (but not storing copy): {tlap} ({bw} GB/s)",
            flush=True,
        )

        t0 = time.time()
        b = zarr.copy_store(warm_zg.store, NullMapping(False))
        tlap = time.time() - t0
        bw = b[2] / (tlap * 1024**3)
        print(
            f"Walltime to zarr.copy_store from DragonStore (not storing copy): {tlap} ({bw} GB/s)",
            flush=True,
        )

    # shutdown the memory holders and the DragonStore
    dstore.destroy()
    if args.park_mem:
        stop_ev.set()
        res_p.close()
        res_p.join()
