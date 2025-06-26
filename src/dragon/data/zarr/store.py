"""A class that implements a Zarr store based on the DDict that can also be a cache for a file-based store."""
import os
import sys
import zarr
import time

from ...native import Pool, Process, Value, cpu_count, Queue
from ...data import DDict
from zarr._storage.store import Store as ZStore

from ctypes import c_int64


class Store(DDict, ZStore):
    """
    This class implements a Zarr store using the Dragon distributed dictionary (:py:class:`~dragon.data.DDict`). This
    allows users to load a dataset fully into the memory of a set of nodes and keep it very close to Zarr clients. It
    can also be used as an initially empty store without data loaded from a file-based store.

    When cloning from a file-based store, this class uses a process Pool to load data in parallel asychnronously
    from object construction. This allows the caller to interact with the store while it is still being loaded.
    Any missing key will be loaded directly by the client if not already loaded by the Pool. This object can
    be serialized (e.g., via Pickle) and shared with another process, who can then also access the store.

    This version of the DDict-based Zarr store supports Zarr versions 2.8.X.

    Example usage:

        .. highlight:: python
        .. code-block:: python

            from dragon.data.zarr import Store
            from dragon.native.machine import System
            import zarr

            dstore = Store(
                        nloaders=32,
                        managers_per_node=1,
                        n_nodes=System().nnodes,
                        total_mem=(2 * (1024**3)),
                        path="/path/to/zarr/data",
                    )

            warm_zg = zarr.group(store=dstore)
            print(f"Dragon-based Store: {warm_zg.tree()}", flush=True)

            # access data while loading proceeds in the background
            vals = warm_zg["/0"][100][:]

            # wait for the load to complete and inspect the exact volume of data loaded
            tot_bytes = dstore.wait_on_load()
            print(f"Total bytes loaded={tot_bytes}", flush=True)

    """

    _erasable = False

    def __init__(self, *args, path: str = None, nloaders: int = 2, dimension_separator: str = None, **kwargs):
        """
        Construct a new DDict-backed Zarr store that is either empty or a cached form of a Zarr file-based store.
        This class is a subclass of `DDict` and accepts all arguments it takes.

        :param path: Either None for an empty store, or a path to a file-based store to load into this store.
        :param nloaders: The number of processes used to load data in parallel from the file-based store.
        :param dimension_separator: (optional) Separator placed between the dimensions of a chunk. Used only when
                                    determining how to parallelize loading of a store from a path.
        """
        vpath = None
        if path is not None:
            vpath = os.path.abspath(path)
            if os.path.exists(vpath) and not os.path.isdir(vpath):
                raise OSError(f"{path} is not a valid path on disk")

        nl = int(nloaders)
        if nl < 1 or nl > cpu_count():
            raise RuntimeError("number of parallel loaders must be > 0 and < total CPUs")

        super().__init__(*args, **kwargs)
        self._init_zprops()

        self._path = vpath
        self._loaded_bytes = Value(c_int64, 0)

        if self._path is not None:
            self._init_store()
            self._start_parallel_loader(nloaders)

        if dimension_separator is not None:
            self._sep = dimension_separator

    def _init_zprops(self):
        self._path = None
        self._src_zg = None
        self._loadp = None
        self._loaded_bytes = None
        self._show_gets = False
        self._timer = 0
        self._zkeys = None
        self._sep = "/"

    def __getstate__(self):
        dstate = super().__getstate__()
        lp_puid = None
        if self._loadp is not None:
            lp_puid = self._loadp.puid

        return (self._sep, self._path, self._loaded_bytes, lp_puid) + dstate

    def __setstate__(self, state):
        self._init_zprops()
        self._sep, self._path, self._loaded_bytes, lp_puid, *dstate = state
        super().__setstate__(dstate)

        self._init_store()
        if lp_puid is not None:
            self._loadp = Process(None, ident=lp_puid)

    def keys(self) -> list[object]:
        """
        Return a list of the keys in the store. If the store is acting as a cache of an existing store,
        this will be cached once loading is complete to speed up access.
        """
        if self._zkeys is None:
            if self._loadp is not None and self._loadp.is_alive:
                # when loading is still going, we need to return the keys from the base store
                if self._src_zg is not None:
                    return self._src_zg.store.keys()
                else:
                    return super().keys()
            else:
                self._zkeys = super().keys()
        return self._zkeys

    def __iter__(self):
        return iter(self.keys())

    def __contains__(self, key: object) -> bool:
        if self._zkeys is None:
            if self._loadp is not None and self._loadp.is_alive:
                # when loading is still going, we need to return the keys from the base store
                if self._src_zg is not None:
                    return key in self._src_zg.store.keys()

        return super().__contains__(key)

    def load_cmplt(self) -> bool:
        """
        Check if loading is complete from another store is complete
        """
        if self._loadp is None:
            raise RuntimeError("no data has been loaded from a path")

        if self._loadp.is_alive:
            return False
        else:
            return True

    def wait_on_load(self, timeout: float = None) -> int:
        """
        Wait until the data is loaded into the DDict and then return the number of bytes loaded. If timeout
        is None (default) this call blocks until the load is done. It timeout is a postive numbers, it blocks
        at most timeout seconds before raising TimeoutError.

        :param timeout: Either None to block or a postive number of seconds to wait for
        """
        if self._loadp is None:
            raise RuntimeError("no data has been loaded")

        if self._loadp.is_alive:
            ec = self._loadp.join(timeout)
            if ec is None:
                raise TimeoutError
            elif ec != 0:
                raise RuntimeError("parallel loading has failed")

        return self.loaded_bytes

    @property
    def loaded_bytes(self) -> int:
        """
        Return the number of bytes currently loaded
        """
        return self._loaded_bytes.value

    def _init_store(self):
        if self._path is not None:
            self._src_zg = zarr.open(self._path, mode="r")

    @classmethod
    def _init_worker(cls, path, dstore, prog_q):
        cold_zg = zarr.open(path, mode="r")
        warm_zg = zarr.open_group(store=dstore)

        Process.stash = {}
        Process.stash["cold_zg"] = cold_zg
        Process.stash["warm_zg"] = warm_zg
        Process.stash["pq"] = prog_q

    @classmethod
    def _load_key(cls, key):
        try:
            b = zarr.copy_store(
                Process.stash["cold_zg"].store,
                Process.stash["warm_zg"].store,
                source_path=key,
                dest_path=key,
                if_exists="replace",
            )
            try:
                Process.stash["pq"].put(b[2])
            except:
                pass
            return b
        except Exception as e:
            return (key, e, None)

    @classmethod
    def _find_gkeys(cls, sep, path, zg):
        thekeys = set()
        for k in zg.store.keys():
            lk = k.split(sep)

            # try to parallelize on the second index
            try:
                sk = f"{lk[0]}{sep}{lk[1]}"
            except:
                sk = lk[0]
            thekeys.add(sk)

        return list(thekeys)

    @classmethod
    def _parallel_loader(cls, sep, path, nloaders, dstore, loadv):
        # get the top level array keys we'll use to break up the loading
        src_zg = zarr.open(path, mode="r")

        # TODO change to a real iterable for better performance so we can overlap key scans with loading
        keys_to_proc = iter(cls._find_gkeys(sep, "", src_zg))

        # do the initial touch of the DDict-based store
        warm_zg = zarr.group(store=dstore)

        prog_q = Queue()

        p = Pool(nloaders, initializer=cls._init_worker, initargs=(path, dstore, prog_q))

        try:
            tot_bytes = 0
            loading = p.map_async(cls._load_key, keys_to_proc, 1)
            while True:
                if loading.ready():
                    break
                else:
                    try:
                        b = prog_q.get(timeout=1.0)
                        tot_bytes += b
                        loadv.value = tot_bytes
                    except:
                        pass

            tot_bytes = 0
            loads = loading.get()
            for i in loads:
                n, s, b = i
                if b is None:
                    raise RuntimeError(f"failed to load key {n} with exception {s}")
                tot_bytes += b
                loadv.value = tot_bytes
        except Exception as e:
            raise RuntimeError("parallel loading has failed") from e
        finally:
            p.close()
            p.join()

        # Grab top-level metadata not copied by iterating over top-level keys
        for top_level_meta in (".zattrs", ".zarray"):
            if top_level_meta in src_zg.store.keys():
                dstore[top_level_meta] = src_zg.store[top_level_meta]

    def _start_parallel_loader(self, nloaders):
        self._loadp = Process(
            target=self._parallel_loader, args=(self._sep, self._path, nloaders, self, self._loaded_bytes)
        )
        self._loadp.start()

    @property
    def show_gets(self):
        return self._show_gets

    @show_gets.setter
    def show_gets(self, enabled):
        """
        If true, print out the name of the key each time a key is requested
        """
        self._show_gets = bool(enabled)

    def reset_get_timer(self):
        """
        Reset the timer that accumulates with each getitem call. This is diagnostic
        """
        self._timer = 0

    @property
    def get_timer(self):
        """
        Get the value of the timer that accumulates on getitem calls
        """
        return self._timer

    def __getitem__(self, key):
        if self._show_gets:
            print(f"Requesting {key}", flush=True)

        # is loading is still in progress, try to get the key but grab it ourself if it's not there
        if self._loadp is not None and self._loadp.is_alive:
            try:
                return super().__getitem__(key)

            except KeyError:
                # load it from the cold store for now
                return self._src_zg.store[key]
        else:
            t0 = time.time()
            v = super().__getitem__(key)
            self._timer += time.time() - t0
            return v

    def __iter__(self):
        if self._loadp is not None and self._loadp.is_alive:
            self._loadp.join()
        return iter(self.keys())
