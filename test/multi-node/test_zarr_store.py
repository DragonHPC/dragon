"""This file contains Dragon multi-node acceptance tests for the
`dragon.data.zarr.Store` object.  The test scales with the total number of CPUs
reported by the allocation, i.e. it becomes tougher on larger allocations.
The test is run with `dragon test_zarr_store.py -f -v`
"""

import unittest
import random
import shutil

import dragon
import multiprocessing as mp
import zarr
import numpy as np
from dragon.data.zarr import Store
from dragon.native.machine import System

DD_MB = 64
FV = np.float32(1.1)
DT = np.float32
PATH = "./zarr_data"


def init_worker(dstore):
    me = mp.current_process()
    me.stash = {}
    me.stash["zg"] = zarr.group(store=dstore)


def validate(itm):
    me = mp.current_process()
    return FV == me.stash["zg"]["/0"][20][20][20]


class TestZarrStoreMultiNode(unittest.TestCase):
    @classmethod
    def _create_dir_store(cls, path):
        zg = zarr.open_group(path, mode="w")
        a0 = np.random.rand(50, 100, 200)
        a1 = np.random.rand(100, 50, 200)
        a2 = np.random.rand(200, 100, 50)
        zg["0"] = a0
        zg["1"] = a1
        zg["2"] = a2
        zg.store.close()
        return zarr.open(path, mode="r")

    @classmethod
    def tearDownClass(cls):
        try:
            shutil.rmtree(PATH)
        except:
            pass

    def test_empty_store(self):
        dstore = Store(
            managers_per_node=1,
            n_nodes=System().nnodes,
            total_mem=(DD_MB * (1024**2)),
        )

        zg = zarr.open(store=dstore)
        zg.create(name="0", shape=(50, 100, 200), dtype=DT, fill_value=FV, overwrite=True)

        self.assertEqual(FV, zg["0"][20][20][20])
        dstore.destroy()

    def _base_mirror(self, use_copy_store):
        path = PATH
        zg_cold = self._create_dir_store(path)

        dstore = Store(
            nloaders=(max(1, mp.cpu_count() // 16)),
            managers_per_node=1,
            n_nodes=System().nnodes,
            total_mem=(DD_MB * (1024**2)),
            path=path,
            use_copy_store=use_copy_store,
        )

        loaded = dstore.wait_on_load()
        self.assertAlmostEqual(loaded, 21026802, delta=2000)
        zg = zarr.open(store=dstore)
        self.assertTrue(np.array_equal(zg_cold["0"], zg["0"]))
        dstore.destroy()

    def test_basic_mirror(self):
        self._base_mirror(True)

    def test_key_mirror(self):
        self._base_mirror(False)

    def test_multiple_clients(self):
        dstore = Store(
            managers_per_node=1,
            n_nodes=System().nnodes,
            total_mem=(DD_MB * (1024**2)),
        )

        zg = zarr.open(store=dstore)
        zg.create(name="0", shape=(50, 100, 200), dtype=DT, fill_value=FV, overwrite=True)

        pool = mp.Pool(2, initializer=init_worker, initargs=(dstore,))
        res = pool.map(validate, [0, 1])
        pool.close()
        pool.join()

        for v in res:
            self.assertTrue(v)
        dstore.destroy()

    def test_async_load(self):
        path = PATH
        zg_cold = self._create_dir_store(path)

        dstore = Store(
            nloaders=(max(1, mp.cpu_count() // 16)),
            managers_per_node=1,
            n_nodes=System().nnodes,
            total_mem=(DD_MB * (1024**2)),
            path=path,
        )

        # do not wait for a load to complete before grabbing data

        zg = zarr.open(store=dstore)
        self.assertTrue(np.array_equal(zg_cold["0"], zg["0"]))

        # now wait and check we get right answers still

        loaded = dstore.wait_on_load()
        self.assertTrue(np.array_equal(zg_cold["0"], zg["0"]))

        dstore.destroy()


if __name__ == "__main__":
    mp.set_start_method("dragon", force=True)
    unittest.main()
