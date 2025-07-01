#!/usr/bin/env python3

import unittest
import traceback
import multiprocessing as mp
import cloudpickle
import os
import socket
import sys
import gc
import getpass
import psutil
import random

import pathlib

import numpy as np
import time
import dragon

import dragon.infrastructure.messages as dmsg
import dragon.channels as dch
from dragon.fli import FLInterface, DragonFLIError, FLIEOT
import dragon.fli as fli
import dragon.managed_memory as dmem
from dragon.managed_memory import MemoryPool, MemoryAlloc
from dragon.native.process import Popen
from dragon.native.process_group import ProcessGroup
from dragon.native.process import ProcessTemplate
from dragon.globalservices import api_setup
from dragon.infrastructure.policy import Policy
from dragon.utils import b64encode, b64decode, hash as dhash, host_id
from dragon.data.ddict import (
    DDict,
    DDictManagerFull,
    DDictUnableToCreateDDict,
    DDictError,
    DDictCheckpointSync,
    DDictFutureCheckpoint,
    DDictPersistCheckpointError,
    strip_pickled_bytes,
    PosixCheckpointPersister,
)
from dragon.rc import DragonError
from dragon.native.machine import System, Node
import multiprocessing as mp
from multiprocessing import Barrier, Pool, Queue, Event, set_start_method

NUM_BUCKETS = 4
POOL_MUID = 897


class numPy2dValuePickler:
    def __init__(self, shape: tuple, data_type: np.dtype, chunk_size=0):
        self._shape = shape
        self._data_type = data_type
        self._chunk_size = chunk_size

    def dump(self, nparr, file) -> None:
        mv = memoryview(nparr)
        bobj = mv.tobytes()
        # print(f"Dumping {bobj=}", file=sys.stderr, flush=True)
        if self._chunk_size == 0:
            chunk_size = len(bobj)
        else:
            chunk_size = self._chunk_size

        for i in range(0, len(bobj), chunk_size):
            file.write(bobj[i : i + chunk_size])

    def load(self, file):
        obj = None
        try:
            while True:
                data = file.read(self._chunk_size)
                if obj is None:
                    # convert bytes to bytearray
                    view = memoryview(data)
                    obj = bytearray(view)
                else:
                    obj.extend(data)
        except EOFError:
            pass

        ret_arr = np.frombuffer(obj, dtype=self._data_type).reshape(self._shape)

        return ret_arr


class intKeyPickler:
    def __init__(self, num_bytes=4):
        self._num_bytes = num_bytes

    def dumps(self, val: int) -> bytearray:
        return val.to_bytes(self._num_bytes, byteorder=sys.byteorder)

    def loads(self, val: bytearray) -> int:
        return int.from_bytes(val, byteorder=sys.byteorder)


def batch_put(d, key, val):
    d.start_batch_put(persist=False)
    for i in range(len(key)):
        d[key[i]] = val[i]
    d.end_batch_put()
    d.detach()


def fillit(d):
    i = 0
    key = "abc"
    while True:
        d[key] = key
        i += 1
        key += "abc" * i


def client_func_1_get_newest_chkpt_id(d):
    ## 0
    d["abc"] = "def"
    d.checkpoint()  ## 1
    d["abc"] = "def2"
    d["hello"] = "world"
    d.checkpoint()  ## 2
    d["abc"] = "def3"
    d["hello"] = "world2"
    d["dragon"] = "runtime"
    d.checkpoint()  ## 3
    d["abc"] = "def4"
    d["hello"] = "world3"
    d["dragon"] = "runtime2"
    d["Miska"] = "dog"
    d.checkpoint()  ## 4
    d["abc"] = "def5"
    d["hello"] = "world4"
    d["dragon"] = "runtime3"
    d["Miska"] = "dog2"
    d[123] = 456
    d.detach()


def client_func_1_wait_keys_clear(d, ev):
    ## 0
    d.pput("hello0", "world0")
    d.checkpoint()  ## 1
    d.pput("hello0_1", "world1")
    d.checkpoint()  ## 2
    d.pput("hello0_2", "world2")
    d.checkpoint()  ## 3
    d.pput("hello0_3", "world3")
    d["Miska"] = "dog"
    d.detach()
    ev.set()


def client_func_2_wait_keys_clear(d, ev):
    ## 0
    d.checkpoint()  ## 1
    d.checkpoint()  ## 2
    del d["hello0_1"]
    # the deleted set is not cleared - should report key not found for the deleted persistent key
    try:
        x = d["hello0_1"]
        raise Exception(f"Expected DDictError DRAGON_KEY_NOT_FOUND is not raised")
    except DDictError as ex:
        assert ex.lib_err == "DRAGON_KEY_NOT_FOUND"
    d.clear()
    d.checkpoint()  ## 3
    ev.set()
    assert (
        d["hello0_1"] == "world1_client3"
    )  # hang - hello1 was clear in chkpt 2, so it should hang until the key is written into dict
    assert "hello0" not in d
    assert "hello0_2" not in d
    assert "Miska" in d
    d.detach()


def client_func_3_wait_keys_clear(d):
    ## 0
    d.checkpoint()  ## 1
    d.checkpoint()  ## 2
    d.checkpoint()  ## 3
    d.pput("hello0_1", "world1_client3")  # flush the get request in client2
    d.detach()


def client_func_1_nonpersist(d, ev):
    d["non_persist_key0"] = "non_persist_val0_ckp1"
    ev.set()
    d.detach()


def client_func_2_nonpersist(d, ev):
    ev.set()
    assert d["non_persist_key0"] == "non_persist_val0_ckp1"
    d.detach()


def client_func_1_persist(d, ev):
    d.pput("persist_key0", "persist_val0")
    ev.set()
    d.detach()


def client_func_2_persist(d, ev):
    ev.set()
    assert d["persist_key0"] == "persist_val0"
    d.detach()


def client_func_1_wait_keys_write_retired(d, ev):
    ## 0
    d.pput("persist_key0", "persist_val0")
    d.checkpoint()  ## 1
    d.pput("persist_key1", "persist_val1")
    d.checkpoint()  ## 2
    d.pput("persist_key2", "persist_val2")
    d.detach()
    ev.set()


def client_func_2_wait_keys_write_retired(d):
    ## 0
    try:
        d.pput("persist_key0", "persist_val0")
        raise Exception(f"Expected DDictError DRAGON_DDICT_CHECKPOINT_RETIRED is not raised")
    except DDictError as ex:
        assert ex.lib_err == "DRAGON_DDICT_CHECKPOINT_RETIRED"

    d.detach()


def client_func_1_wait_keys_read_retired(d, ev):
    ## 0
    d.pput("persist_key0", "persist_val0")
    d.checkpoint()  ## 1
    d.pput("persist_key1", "persist_val1")
    d.checkpoint()  ## 2
    d.pput("persist_key2", "persist_val2")
    d.detach()
    ev.set()


def client_func_2_wait_keys_read_retired(d):
    ## 0
    try:
        x = d["persist_key0"]
        raise Exception(f"Expected DDictError DRAGON_DDICT_CHECKPOINT_RETIRED is not raised")
    except DDictError as ex:
        assert ex.lib_err == "DRAGON_DDICT_CHECKPOINT_RETIRED"

    d.detach()


def client_func_1_wait_keys_defer_put_get_same_key(d, ev):
    ## 0
    d.pput("persist_key0", "persist_val0")
    d["nonpersist_key0"] = "nonpersist_val0"
    d.checkpoint()  ## 1
    d.pput("persist_key0", "persist_val0_ckpt1")
    ev.set()
    d.checkpoint()  ## 2
    d["nonpersist_key0"] = "nonpersist_val0_ckpt2"  # deferred 1
    d.detach()


def client_func_2_wait_keys_defer_put_get_same_key(d, ev):
    ## 0
    d.checkpoint()  ## 1
    ev.set()
    d.checkpoint()  ## 2
    x = d["nonpersist_key0"]  # deferred 2
    assert x == "nonpersist_val0_ckpt2"
    assert d["nonpersist_key0"] == "nonpersist_val0_ckpt2_client3"
    d.detach()


def client_func_3_wait_keys_defer_put_get_same_key(d):
    ## 0
    d.checkpoint()  ## 1
    d["nonpersist_key0"] = "nonpersist_val0_ckpt1"
    d.checkpoint()  ## 2
    d["nonpersist_key0"] = "nonpersist_val0_ckpt2_client3"  # flush
    d.detach()


def client_func_1_wait_keys_contains(d, ev):
    ## 0
    d["nonpersist_key0"] = "nonpersist_val0"
    d.pput("persist_key0", "persist_val0")
    d.checkpoint()  ## 1
    d["nonpersist_key0"] = "nonpersist_val0_chkpt1"
    d.detach()
    ev.set()


def client_func_2_wait_keys_contains(d):
    ## 0
    assert "nonpersist_key0" in d
    d.pput("persist_key0_1", "persist_val1")
    d.checkpoint()  ## 1
    assert "persist_key0" in d
    assert "nonpersist_key0" in d
    del d["persist_key0_1"]
    assert "persist_key0_1" not in d
    d.checkpoint()  ## 2
    assert "persist_key0" in d
    assert "nonpersist_key0" not in d
    d["nonpersist_key0"] = "nonpersist_val0_chkpt2a"
    d.checkpoint()  ## 3
    assert "persist_key0_1" not in d
    assert "nonpersist_key0" not in d
    d.detach()


def client_func_1_wait_keys_pop(d):
    ## 0
    d.checkpoint()  ## 1
    d["key0"] = "nonpersist_val0"
    assert d["key0"] == "nonpersist_val0"
    d.checkpoint()  ## 2
    d["key0"] = "nonpersist_val0_chkpt2"
    assert d["key0"] == "nonpersist_val0_chkpt2"
    d.detach()


def client_func_2_wait_keys_pop(d, ev):
    ## 0
    d.pput("key0", "persist_val0")
    d.pop("key0")
    d.detach()
    ev.set()


def client_func_1_wait_keys_pop_nonpersist(d):
    ## 0
    d["nonpersist_key0"] = "nonpersist_val0"
    d.checkpoint()  ## 1
    assert d.pop("nonpersist_key0") == "nonpersist_val0_chkpt1"
    d.checkpoint()  ## 2
    d["nonpersist_key0"] = "nonpersist_val0_chkpt2"
    d.detach()


def client_func_2_wait_keys_pop_nonpersist(d):
    ## 0
    assert d.pop("nonpersist_key0") == "nonpersist_val0"
    d.checkpoint()  ## 1
    d["nonpersist_key0"] = "nonpersist_val0_chkpt1"
    d.checkpoint()  ## 2
    assert d.pop("nonpersist_key0") == "nonpersist_val0_chkpt2"
    d.detach()


def client_func_1_wait_keys_pop_persist(d):
    # ## 0
    d.checkpoint()  ## 1
    d.checkpoint()  ## 2
    assert "persist_key0" not in d
    d.detach()


def client_func_2_wait_keys_pop_persist(d):
    # ## 0
    d.pput("persist_key0", "persist_val0")
    assert d.pop("persist_key0") == "persist_val0"
    assert "persist_key0" not in d
    d.checkpoint()  ## 1
    d.checkpoint()  ## 2
    # Write a value into checkpoint 2 of the
    # same manager.
    d[2] = "hello"
    d.detach()


def client_func_1_wait_keys_length(d):
    d_0 = d.manager(0)
    d_1 = d.manager(1)
    ## 0
    assert len(d) == 1
    d_0.checkpoint()  ## 1
    d.checkpoint()  ## 1
    d_1.checkpoint()  ## 1
    d_0.pop("key0")
    assert len(d) == 2
    d_0.checkpoint()  ## 2
    d.checkpoint()  ## 2
    d_1.checkpoint()  ## 2
    assert len(d) == 1
    d_0["nonpersist_key1"] = "nonpersist_val1_chkpt2"
    assert len(d) == 2
    d_0.checkpoint()  ## 3
    d.checkpoint()  ## 3
    d_1.checkpoint()  ## 3
    assert len(d) == 1
    d_0["nonpersist_key3"] = "nonpersist_val3"
    assert len(d) == 2
    d_0["key0"] = "val0_chkpt3"  # add persistent key back as a non-persist key
    assert len(d) == 3
    d_1["key3"] = "val3"  # other manager
    assert len(d) == 4
    d.detach()


def client_func_2_wait_keys_length(d, ev):
    d_0 = d.manager(0)
    ## 0
    d_0.pput("key0", "persist_val0")
    d_0.checkpoint()  ## 1
    d_0.pput("persist_key1", "persist_val1")
    d_0["nonpersist_key1"] = "nonpersist_val1"
    d.detach()
    ev.set()


def client_func_1_wait_keys_keys(d, ev):
    ## 0
    d.pput("key0", "val0")
    d.detach()
    ev.set()


def client_func_2_wait_keys_keys(d):
    ## 0
    d["key1_0"] = "val1_0_nonpersist"

    d.checkpoint()  ## 1
    key_list_chkpt1 = list(d.keys())
    assert len(key_list_chkpt1) == 1
    assert key_list_chkpt1[0] == "key0"
    d["key1_nonpersist"] = "val1_nonpersist"
    key_list_chkpt1 = d.keys()
    assert len(key_list_chkpt1) == 2
    assert "key1_nonpersist" in key_list_chkpt1
    assert "key0" in key_list_chkpt1
    del d["key0"]
    d["key1_0"] = "val1_0_nonpersist_chkpt1"

    d.checkpoint()  ## 2
    key_list_chkpt2 = d.keys()  # keys request to a checkpoint that hasn't exist
    assert len(key_list_chkpt2) == 0
    d.pput("key0", "val1")  # retire chkpt 0
    d["key1_0"] = "val1_0_nonpersist_chkpt2"
    d["key1_nonpersist"] = "val1_nonpersist_chkpt2"

    d.checkpoint()  ## 3
    key_list_chkpt3 = (
        d.keys()
    )  # check that the future checkpoint can get read the existing checkpoint and only return persistent keys
    assert len(key_list_chkpt3) == 1
    assert "key0" in key_list_chkpt3

    d.pput("key2", "val2")  # retire chkpt 1
    key_list_chkpt3 = d.keys()  # check that the deleted set is copied correctly from retired checkpoint
    assert len(key_list_chkpt3) == 2
    assert "key0" in key_list_chkpt3
    assert "key2" in key_list_chkpt3
    d.detach()


def client_func_1_wait_writers_persist(d, ev):
    ## 0
    d["persist_key0"] = "persist_val0"
    d.checkpoint()  ## 1
    d["persist_key0"] = "persist_val1"
    d.detach()
    ev.set()


def client_func_2_wait_writers_persist(d):
    ## 0
    assert d["persist_key0"] == "persist_val0"
    d.checkpoint()  ## 1
    assert d["persist_key0"] == "persist_val1"
    d.detach()


def client_func_2_wait_writers_persist_err(d, ev):
    try:
        assert d["persist_key0"] == "persist_val0"
        raise Exception(f"Expected DDictError DRAGON_KEY_NOT_FOUND is not raised")
    except DDictError as ex:
        assert ex.lib_err == "DRAGON_KEY_NOT_FOUND"
    d.detach()
    ev.set()


def client_func_1_wait_writers_read_future_chkpt(d, ev):
    ## 0
    d.checkpoint()  ## 1
    d["key1"] = "val1"
    d.detach()
    ev.set()


def client_func_2_wait_writers_read_future_chkpt(d):
    ## 0
    d.checkpoint()  ## 1
    d.checkpoint()  ## 2
    assert d["key1"] == "val1"
    d.checkpoint()  ## 3
    assert d["key1"] == "val1"  # should be able to get the key even the working set is not advanced
    d.detach()


def client_func_1_wait_writers_write_retired(d, ev):
    ## 0
    d["key0"] = "val0"
    d.checkpoint()  ## 1
    d["key0"] = "val0_chkpt1"
    d.checkpoint()  ## 2
    d["key0"] = "val0_chkpt2"
    d.detach()
    ev.set()


def client_func_2_wait_writers_write_retired(d):
    ## 0
    try:
        d["key0"] = "val0_client2"
        raise Exception(f"Expected DDictError DRAGON_DDICT_CHECKPOINT_RETIRED is not raised")
    except DDictError as ex:
        assert ex.lib_err == "DRAGON_DDICT_CHECKPOINT_RETIRED"
    d.detach()


def client_func_2_wait_writers_read_retired(d):
    ## 0
    try:
        x = d["key0"]
        raise Exception(f"Expected DDictError DRAGON_DDICT_CHECKPOINT_RETIRED is not raised")
    except DDictError as ex:
        assert ex.lib_err == "DRAGON_DDICT_CHECKPOINT_RETIRED"
    d.detach()


def client_func_1_wait_writers_contains(d, ev):
    ## 0
    d["key0"] = "val0"
    d.detach()
    ev.set()


def barrier_waiter(b):
    b.wait()


def client_func_2_wait_writers_contains(d):
    ## 0
    assert "key0" in d
    d.checkpoint()  ## 1
    assert "key0" in d
    assert "key1" not in d
    d.detach()


def client_func_1_wait_writers_pop(d, ev):
    ## 0
    d["key0"] = "val0"
    d["key1"] = "val1"
    d.checkpoint()  ## 1
    d["key2"] = "val2"
    d.detach()
    ev.set()


def client_func_2_wait_writers_pop(d):
    ## 0
    assert "key0" in d
    del d["key0"]
    assert "key0" not in d
    try:
        d.pop("key0")
        raise Exception(f"Expected DDictError DRAGON_KEY_NOT_FOUND is not raised")
    except DDictError as ex:
        assert ex.lib_err == "DRAGON_KEY_NOT_FOUND"
    d.checkpoint()  ## 1
    assert "key1" in d
    assert d.pop("key1") == "val1"
    assert "key1" not in d
    d.detach()


def client_func_1_wait_writers_len(d, ev):
    ## 0
    d_0 = d.manager(0)
    d_0["key0"] = "val0"
    d_0.checkpoint()  ## 1
    d_0["key0_1"] = "val0_1"
    d.detach()
    ev.set()


def client_func_2_wait_writers_len(d):
    ## 0
    d_0 = d.manager(0)
    d_1 = d.manager(1)
    assert len(d) == 1
    d.checkpoint()  ## 1
    d_0.checkpoint()  ## 1
    d_1.checkpoint()  ## 1
    assert len(d) == 2
    d.checkpoint()  ## 2
    d_0.checkpoint()  ## 2
    d_1.checkpoint()  ## 2
    assert len(d) == 2
    d_0["key0_1_1"] = "val0_1_1"
    assert len(d) == 3
    d_1["key0_2"] = "val0_2"  # on different manager
    assert len(d) == 4
    del d_0["key0"]
    assert len(d) == 3
    del d_0["key0_1"]
    assert len(d) == 2

    d.detach()


def client_func_1_wait_writers_keys(d, ev):
    ## 0
    d["key1_0"] = "persistent_val1"
    d.checkpoint()  ## 1
    d["key2"] = "persistent_val2"
    d.detach()
    ev.set()


def client_func_2_wait_writers_keys(d):
    ## 0
    key_list_chkpt0 = d.keys()
    assert len(key_list_chkpt0) == 1
    assert "key1_0" in key_list_chkpt0
    del d["key1_0"]
    d["key1_1"] = "persistent_val1_1"

    d.checkpoint()  ## 1
    key_list_chkpt1 = d.keys()
    assert len(key_list_chkpt1) == 2
    assert "key1_1" in key_list_chkpt1
    assert "key2" in key_list_chkpt1
    del d["key1_1"]

    d.checkpoint()  ## 2
    key_list_chkpt2 = d.keys()
    assert len(key_list_chkpt2) == 1
    assert "key2" in key_list_chkpt2

    d.detach()


def remote_proc(resp_fli_ser, queue):
    try:
        resp_fli = fli.FLInterface.attach(b64decode(resp_fli_ser))

        main_channel = dch.Channel.make_process_local()
        strm_channel = dch.Channel.make_process_local()

        the_fli = fli.FLInterface(main_ch=main_channel)

        queue.put(b64encode(the_fli.serialize()))

        with the_fli.sendh(stream_channel=strm_channel) as sendh:
            b = socket.gethostname().encode("utf-8")
            sendh.send_bytes(b)

        with resp_fli.recvh(use_main_as_stream_channel=True) as recvh:
            b = recvh.recv_bytes()

        del the_fli

        return 0
    except Exception as ex:
        print(f"There was an exception on the remote_proc: {ex}", file=sys.stder, flush=True)


def client_func_1_wait_writers_rollback(d, ev):
    ## 0
    d["key0"] = "val0"
    d.checkpoint()  ## 1
    assert d.checkpoint_id == 1
    d["key0"] = "val1"
    d.rollback()  ## 0
    assert d.checkpoint_id == 0
    assert d["key0"] == "val0"
    d.checkpoint()  ## 1
    d.checkpoint()  ## 2
    d["key0"] = "val2"
    d.detach()
    ev.set()


def client_func_2_wait_writers_rollback(d):
    ## 0
    d.checkpoint()  ## 1
    assert d.checkpoint_id == 1
    assert d["key0"] == "val1"
    d.checkpoint()  ## 2
    assert d.checkpoint_id == 2
    assert d["key0"] == "val2"
    d.rollback()  ## 1
    d.rollback()  ## 0
    try:
        assert d["key0"] == "val0"
        raise Exception(f"Expected DDictError DRAGON_DDICT_CHECKPOINT_RETIRED is not raised")
    except DDictError as ex:
        assert ex.lib_err == "DRAGON_DDICT_CHECKPOINT_RETIRED"
    d.detach()


def client_func_1_restart_wait_writers(d, arr):
    ## 0
    for i in arr:
        d[i] = "client1_" + str(i)
    d.checkpoint()  ## 1
    d.detach()


def client_func_2_restart_wait_writers(d, arr):
    ## 0
    d.checkpoint()  ## 1
    for i in arr:
        d[i] = "client2_" + str(i)
    d.detach()


def client_func_1_d2_restart_wait_writers(d, ev1):
    ## 0
    d.checkpoint()  ## 1
    d.checkpoint()  ## 2
    ev1.set()
    d["test_key"] = "value1"
    d.detach()


def client_func_2_d2_restart_wait_writers(d, ev2):
    ## 0
    d["hello"] = "world"
    d.checkpoint()  ## 1
    d.checkpoint()  ## 2
    ev2.wait()
    d["test_key"] = "value2"
    d.detach()


def client_func_3_d2_restart_wait_writers(d, ev1, ev2):
    ## 0
    d.checkpoint()  ## 1
    d.checkpoint()  ## 2
    ev1.wait()
    ev2.set()
    assert d["test_key"] == "value1"
    d.detach()


def client_func_1_restart_with_defer_ops_discarded(d, ev):
    ## 0
    d["hello"] = "world"
    d.checkpoint()  ## 1
    d.checkpoint()  ## 2
    ev.set()
    d["hello"] = "world2"  # defer op
    d.detach()


def client_func_1_d2_restart_with_defer_ops_discarded(d2):
    ## 0
    assert d2["hello"] == "world"
    d2.checkpoint()  ## 1
    d2["hello"] = "world1_d2"
    d2.checkpoint()  ## 2
    d2.pput("hello0", "world0")
    assert "hello" not in d2
    d2.detach()


def client_func_1_batch_put_defer(d, ev):
    ev.set()
    d0 = d.manager(0)
    ## 0
    d0.checkpoint()  ## 1
    d0["dragon"] = "runtime_chkpt1"
    d0.checkpoint()  ## 2
    d0["hello"] = "world_chkpt2"  ## checkpoint 0 at manager 0 is retired, flush batch put
    d.detach()


def client_func_2_batch_put_defer(d, ev, random_key, random_val):
    d0 = d.manager(0)

    ## 0
    d0["dragon"] = "runtime"
    d0.checkpoint()  ## 1
    d0.checkpoint()  ## 2

    d0.start_batch_put(persist=False)
    # defer batch put as checkpoint 0 is not ready to retire
    for i in range(len(random_key)):
        d0[random_key[i]] = random_val[i]
    ev.set()
    d0.end_batch_put()
    d.detach()


def client_func_1_defer_get_with_exception(dd, ev):
    dd[0] = 1
    dd.checkpoint()  ## 1
    dd.checkpoint()  ## 2
    ev.set()
    val = dd["x"]  # defered
    dd.detach()


def client_func_2_defer_get_with_exception(dd):
    dd.checkpoint()  ## 1
    dd[0] = 2
    dd.checkpoint()  ## 2
    arr = np.ones(4096, dtype=np.int32)
    dd["x"] = arr  # flush client 1 deferred get op with exception
    dd.detach()


def client_func_1_defer_bput(dd, ev):
    dd.bput("hello", "world")  ## 0
    dd.checkpoint()  ## 1
    dd.checkpoint()  ## 2
    ev.set()
    dd.bput("dragon", "runtime")  ## deferred
    dd.detach()


def client_func_2_defer_bput(dd, ev1):
    dd.checkpoint()  ## 1
    dd.checkpoint()  ## 2
    ev1.set()
    assert dd.bget("dragon") == "runtime"  ## deferred
    dd.detach()


def client_func_3_defer_bput(dd):
    dd.checkpoint()  ## 1
    dd.bput("hello", "world1")
    dd.checkpoint()  ## 2
    dd.bput("dragon", "runtime1")  # flush the deferred bput op in client 1 and deferred bget op in client 2
    dd.detach()


def client_too_big3(d):
    try:
        print(f"Client on host {socket.gethostname()}", flush=True)
        x = np.ones(4 * 1024 * 1024, dtype=np.int32)
        d["np array"] = x
        return 1
    except DDictManagerFull:
        return 0
    except Exception:
        return 1


def sort_keys(dd, out_queue):
    try:
        keys = list(dd.keys())
        keys.sort()

        # If EOFError is raised, the receiving side closed the queue
        try:
            for key in keys:
                out_queue.put(key)
        except EOFError:
            pass

    except Exception as ex:
        tb = traceback.format_exc()
        print(
            "There was an exception in sort_keys: %s\n Traceback: %s" % (ex, tb),
            flush=True,
        )


def sort_keys_comparator(x, y):
    return x < y


class TestDDict(unittest.TestCase):
    def setUp(self):
        pool_name = f"pydragon_ddict_test_{getpass.getuser()}_{os.getpid()}"
        pool_size = 1073741824  # 1GB
        pool_uid = POOL_MUID
        try:
            self.mpool = MemoryPool(pool_size, pool_name, pool_uid)
        except Exception as ex:
            print(ex)
            raise ex

    def tearDown(self):
        try:
            self.mpool.destroy()
        except Exception:
            raise

    def test_local_channel(self):
        ch = dch.Channel.make_process_local()
        ch.detach()

    @unittest.skip(
        "This test was used to measure the distribution of the new hash function. It does not need to run every time."
    )
    def test_distribution(self):
        d = DDict(32, 1, 16 * 1024 * 1024 * 1024)

        file = open("files.txt")
        for line in file:
            line = line.strip()
            d[line] = 0

        print(d.stats, flush=True)

        d.destroy()

    def test_local_pool(self):
        try:
            pool = MemoryPool.make_process_local("kdlpool", 4096)
            pool.detach()
        except Exception as ex:
            tb = traceback.format_exc()
            print(f"Exception caught {ex}\n Traceback: {tb}")
            raise Exception(f"Exception caught {ex}\n Traceback: {tb}")

    def test_two_local_pools(self):
        """
        Test we can make two pools with the interface
        """
        try:
            pool = MemoryPool.make_process_local("kdlpool8", 8096, min_block_size=64)
            pool2 = MemoryPool.make_process_local("kdlpool9", 8096, min_block_size=64)
            self.assertEqual(pool.muid + 1, pool2.muid)
            pool.detach()
            pool2.detach()
        except Exception as ex:
            tb = traceback.format_exc()
            print(f"Exception caught {ex}\n Traceback: {tb}")
            raise Exception(f"Exception caught {ex}\n Traceback: {tb}")

    def test_deregister(self):
        try:
            pool = MemoryPool.make_process_local("kdlpool3", 8096, min_block_size=64)
            pool.deregister()
            pool.destroy()
        except Exception as ex:
            tb = traceback.format_exc()
            print(f"Exception caught {ex}\n Traceback: {tb}")
            raise Exception(f"Exception caught {ex}\n Traceback: {tb}")

    def test_reregister(self):
        try:
            pool = MemoryPool.make_process_local("kdlpool4", 8096, min_block_size=64)
            pool.deregister()
            pool.register()
        except Exception as ex:
            tb = traceback.format_exc()
            print(f"Exception caught {ex}\n Traceback: {tb}")
            raise Exception(f"Exception caught {ex}\n Traceback: {tb}")

    def test_hash(self):
        try:
            here = pathlib.Path(__file__).parent.resolve()
        except:
            here = pathlib.Path(__file__).resolve()

        buckets = list(0 for _ in range(NUM_BUCKETS))
        file = open(os.path.join(here, "filenames.txt"), "r")
        for filename in file:
            lst = filename.split(".")
            if len(lst) > 2:
                key = lst[0] + lst[2][2:]
                pickled_key = strip_pickled_bytes(cloudpickle.dumps(key))
                # print(f'{key=} and {pickled_key=}')
                idx = dhash(pickled_key) % NUM_BUCKETS
                buckets[idx] += 1

        file.close()

        # print(f'\n{buckets=}')
        total = sum(buckets)
        avg_per_bucket = total / NUM_BUCKETS
        tolerance = avg_per_bucket * 0.10  # 10 percent tolerance

        for amt in buckets:
            # print(f'{amt=} and {avg_per_bucket=}')
            self.assertTrue(amt >= avg_per_bucket - tolerance)

    def test_dict_hash(self):
        # this test does not check for anything, but the
        # manager logs record the split of the keys and this
        # test can be used to see how it handles 2000 keys from
        # the filename data set.
        try:
            here = pathlib.Path(__file__).parent.resolve()
        except:
            here = pathlib.Path(__file__).resolve()

        buckets = list(0 for _ in range(NUM_BUCKETS))
        file = open(os.path.join(here, "filenames.txt"), "r")
        count = 0
        d = DDict(4, 1, 30000000)
        for filename in file:
            root = filename.split(".")[0]
            d[root] = 0
            count += 1
            if count == 2000:
                break
        file.close()
        d.destroy()

    def test_infra_message(self):
        msg = dmsg.GSHalted(42)
        ser = msg.serialize()
        newmsg = dmsg.parse(ser)
        self.assertIsInstance(newmsg, dmsg.GSHalted)
        newser = "eJyrVoovSVayUjA21lFQKklMBzItawE+xQWS"
        from_str = dmsg.parse(newser)
        self.assertIsInstance(from_str, dmsg.GSHalted)
        newser = "eJyrVoovSVayUjA21lFQKklMBzItawE+xQWS\n"
        from_str = dmsg.parse(newser)
        self.assertIsInstance(from_str, dmsg.GSHalted)
        newline = b"\n\n\n\n"
        encoded = b64encode(newline)
        decoded = b64decode(encoded)
        self.assertEqual(newline, decoded)
        newline = "\n\n\n\n"
        encoded = b64encode(newline.encode("utf-8"))
        decoded = b64decode(encoded)
        self.assertEqual(newline, decoded.decode("utf-8"))

    def test_capnp_message(self):
        msg = dmsg.DDRegisterClient(42, "HelloWorld", "MiskaIsAdorable")
        ser = msg.serialize()

        newmsg = dmsg.parse(ser)
        self.assertIsInstance(newmsg, dmsg.DDRegisterClient)

    def test_ddict_client_response_message(self):
        manager_nodes = b64encode(cloudpickle.dumps([Node(ident=host_id()) for _ in range(2)]))
        msg = dmsg.DDRegisterClientResponse(
            42, 43, DragonError.SUCCESS, 0, 2, 3, manager_nodes, "this is name", 10, "this is dragon error info"
        )
        ser = msg.serialize()
        newmsg = dmsg.parse(ser)
        self.assertIsInstance(newmsg, dmsg.DDRegisterClientResponse)

    def test_bringup_teardown(self):
        d = DDict(2, 1, 3000000, trace=True)
        d.destroy()

    def test_manager_pool_leak(self):
        d = DDict(2, 1, 3000000, trace=True)
        at_start = d.stats
        at_end = d.stats
        self.assertEqual(at_start[0].pool_free_space, at_end[0].pool_free_space)
        self.assertEqual(at_start[1].pool_free_space, at_end[1].pool_free_space)
        d.destroy()

    def test_add_delete(self):
        d = DDict(2, 1, 3000000, trace=True)

        at_start = d.dstats

        i = 0
        key = "abc"
        while i < 10:
            d[key] = key
            i += 1
            key += "abc" * i

        i = 0
        key = "abc"
        while i < 10:
            del d[key]
            i += 1
            key += "abc" * i

        at_end = d.dstats

        self.assertEqual(at_start[0].pool_free_space, at_end[0].pool_free_space)
        self.assertEqual(at_start[1].pool_free_space, at_end[1].pool_free_space)

        d.destroy()

    def test_custom_manager(self):
        d = DDict(2, 1, 3000000, trace=True)

        d0 = d.manager(0)
        d1 = d.manager(1)

        d0["hello"] = "goodbye"
        d1["berry"] = "good"
        d1["hello"] = "two"
        d1["today"] = "friday"

        self.assertEqual(len(d.keys()), 4)
        self.assertEqual(len(d1.keys()), 3)
        self.assertEqual(len(d0.keys()), 1)

        self.assertEqual(len(d), 4)
        self.assertEqual(len(d1), 3)
        self.assertEqual(len(d0), 1)

        self.assertEqual(len(d1.stats), 1)

        d1.clear()

        self.assertEqual(len(d.keys()), 1)
        self.assertEqual(len(d1.keys()), 0)
        self.assertEqual(len(d0.keys()), 1)

        d.destroy()

    def test_detach_client(self):
        d = DDict(2, 1, 3000000)
        d.detach()
        d.destroy()

    def test_for_leak(self):
        def_pool = dmem.MemoryPool.attach_default()
        before_free_space = def_pool.free_space
        allocs = def_pool.get_allocations()
        d = DDict(1, 1, 3000000, trace=True)
        d.destroy()
        after_free_space = def_pool.free_space
        self.assertEqual(before_free_space - after_free_space, 0)

    def test_put_get(self):
        d = DDict(2, 1, 3000000, trace=True)
        def_pool = dmem.MemoryPool.attach_default()
        d["abc"] = "def"
        x = d["abc"]
        x1 = d["abc"]
        self.assertEqual(x1, "def")

    @unittest.skip("The memory assertion passes when run manually. Fails in pipeline.")
    def test_put_and_get(self):
        d = DDict(2, 1, 3000000, trace=True)
        def_pool = dmem.MemoryPool.attach_default()
        q = Queue()
        b = Barrier(parties=2)
        before_free_space = def_pool.free_space
        d["abc"] = "def"
        x = d["abc"]
        self.assertEqual(d["abc"], "def")

        d[123] = "456"
        x = d[123]
        self.assertEqual(d[123], "456")
        d[(12, 34, 56)] = [1, 2, 3, 4, 5, 6]
        y = d[(12, 34, 56)]
        y1 = d[(12, 34, 56)]  # test if the key-value can be requested twice or more
        y2 = d[(12, 34, 56)]
        self.assertEqual(y, [1, 2, 3, 4, 5, 6])
        self.assertEqual(y1, [1, 2, 3, 4, 5, 6])
        self.assertEqual(y2, [1, 2, 3, 4, 5, 6])
        self.assertEqual(d[(12, 34, 56)], [1, 2, 3, 4, 5, 6])
        self.assertRaises(KeyError, lambda x: d[x], "hello")
        del d[123]
        d.clear()
        q.put(123)
        x = q.get()
        self.assertEqual(x, 123)
        bwaiter = mp.Process(target=barrier_waiter, args=(b,))
        bwaiter.start()
        b.wait()
        bwaiter.join()
        bwaiter = mp.Process(target=barrier_waiter, args=(b,))
        bwaiter.start()
        b.wait()
        bwaiter.join()
        gc.collect()
        self.assertEqual(def_pool.free_space, before_free_space)
        d.destroy()

    @unittest.skip("this success metric needs updating")
    def test_mem_leak(self):
        d = DDict(2, 1, 3000000)
        def_pool = dmem.MemoryPool.attach_default()
        before_space = def_pool.free_space
        try:
            del d[123]
        except Exception:
            pass
        self.assertEqual(before_space, def_pool.free_space)
        d.destroy()

    @unittest.skip("this success metric needs updating")
    def test_mem_leak_multi_procs(self):
        def_pool = dmem.MemoryPool.attach_default()
        before_space = def_pool.free_space
        p1 = mp.Process()
        p2 = mp.Process()
        p1.start()
        p2.start()
        p1.join()
        p2.join()
        gc.collect()
        self.assertEqual(before_space, def_pool.free_space)

    def test_numpy_put_and_get(self):
        d = DDict(2, 1, 1024 * 1024 * 1024)
        arr_np = np.random.rand(2048, 2048)
        d["arr_np"] = arr_np
        self.assertTrue((arr_np == d["arr_np"]).all())
        d.destroy()

    def test_pop(self):
        d = DDict(2, 1, 3000000)
        d["abc"] = "def"
        x = d.pop("abc")
        self.assertEqual(x, "def")
        with self.assertRaises(DDictError) as ex:
            d.pop("abc")
            self.assertIn(
                "KEY_NOT_FOUND", str(ex), "Expected DDictError message KEY_NOT_FOUND not in the raised exception."
            )

        d[123] = 456
        del d[123]
        with self.assertRaises(DDictError) as ex:
            d.pop(123)
            self.assertIn(
                "KEY_NOT_FOUND", str(ex), "Expected DDictError message KEY_NOT_FOUND not in the raised exception."
            )

        d[(12, 34, 56)] = [1, 2, 3, 4, 5, 6]
        x = d.pop((12, 34, 56))
        with self.assertRaises(DDictError) as ex:
            d.pop((12, 34, 56))
            self.assertIn(
                "KEY_NOT_FOUND", str(ex), "Expected DDictError message KEY_NOT_FOUND not in the raised exception."
            )

        d.destroy()

    def test_contains_key(self):
        d = DDict(1, 1, 3000000)
        d["abc"] = "def"
        self.assertTrue("abc" in d)  # test existence of the added key
        self.assertFalse(123 in d)  # test existence if the key is never added
        d[123] = 456
        self.assertTrue(123 in d)
        d.pop(123)
        self.assertFalse(123 in d)  # test existence of a poped key
        d.pop("abc")
        self.assertFalse("abc" in d)  # test existence of a poped key

        # test tuple key and value
        d[(1, 2, 3, 4, 5)] = [6, 7, 8, 9, 10]
        self.assertTrue((1, 2, 3, 4, 5) in d)
        del d[(1, 2, 3, 4, 5)]
        self.assertFalse((1, 2, 3, 4, 5) in d)

        d.destroy()

    def test_len(self):
        d = DDict(2, 1, 3000000, trace=True)
        self.assertEqual(len(d), 0)
        d["abc"] = "def"
        self.assertEqual(len(d), 1)
        d[123] = 456
        self.assertEqual(len(d), 2)
        d[(1, 2, 3, 4, 5)] = [6, 7, 8, 9, 10]
        self.assertEqual(len(d), 3)
        d.pop("abc")
        self.assertEqual(len(d), 2)
        d.pop(123)
        self.assertEqual(len(d), 1)
        d.pop((1, 2, 3, 4, 5))
        self.assertEqual(len(d), 0)
        d.destroy()

    def test_clear(self):
        d = DDict(2, 1, 3000000, trace=True)
        d["abc"] = "def"
        d[123] = 456
        d[(1, 2, 3, 4, 5)] = [6, 7, 8, 9, 10]
        self.assertEqual(len(d), 3)
        d.clear()
        self.assertEqual(len(d), 0)
        d.clear()  # test clearing an empty dictionary
        self.assertEqual(len(d), 0)
        d["hello"] = "world"
        d.clear()
        self.assertEqual(len(d), 0)
        d.destroy()

    def test_simple_iter(self):
        d = DDict(2, 1, 3000000, trace=True)
        keys = set(["one", "three"])
        values = set(["two", "four"])
        d["one"] = "two"
        d["three"] = "four"

        keys_set = set()
        for key in d:
            keys_set.add(key)
            self.assertIn(key, keys)

        self.assertEqual(keys, keys_set)

        d.destroy()

    def test_iter_break(self):
        d = DDict(2, 1, 3000000, trace=True)
        # two key/value pairs end up in each manager
        k = ["abc", 98765, "hello", (1, 2, 3, 4, 5)]
        v = ["def", 200, "world", ["a", 1, 3, 5, "b"]]
        for i, key in enumerate(k):
            d[key] = v[i]

        i = 0

        key_set = set()
        for key in d:
            key_set.add(key)
            print("Added", key, flush=True)
            # 1 key read from first manager so this
            # tests discarding data and ignoring rest
            # of the managers being iterated over.
            if i == 0:
                break
            i += 1

        self.assertEqual(len(k) - 3, len(key_set))
        d.destroy()

    def test_iter(self):
        try:
            d = DDict(2, 1, 3000000, trace=True)
            k = ["abc", 98765, "hello", (1, 2, 3, 4, 5)]
            v = ["def", 200, "world", ["a", 1, 3, 5, "b"]]
            for i, key in enumerate(k):
                d[key] = v[i]

            for i in d:
                if i == "abc":
                    self.assertEqual(d[i], "def")
                elif i == 98765:
                    self.assertEqual(d[i], 200)
                elif i == "hello":
                    self.assertEqual(d[i], "world")
                elif i == (1, 2, 3, 4, 5):
                    self.assertEqual(d[i], ["a", 1, 3, 5, "b"])
                else:
                    self.assertTrue(False, f"Get the key which is not added by client: key={i}")

            iter_d = iter(d)
            ddict_keys = []
            while True:
                try:
                    ddict_keys.append(next(iter_d))
                except StopIteration:
                    del iter_d
                    break
            for key in k:
                self.assertTrue(key in ddict_keys)

            d.destroy()
        except Exception as e:
            tb = traceback.format_exc()
            raise Exception(f"Exception caught {e}\n Traceback: {tb}")

    def test_keys_view(self):
        d = DDict(2, 1, 3000000, trace=True)
        k = ["abc", 98765, "hello", (1, 2, 3, 4, 5)]
        v = ["def", 200, "world", ["a", 1, 3, 5, "b"]]
        for i, key in enumerate(k):
            d[key] = v[i]

        self.assertIn("abc", d.keys())
        self.assertEqual(len(d.keys()), len(d))
        for key in d.keys():
            self.assertIn(key, k)

        d.destroy()

    def test_keys_mapping(self):
        d = DDict(2, 1, 3000000, trace=True)
        k = ["abc", 98765, "hello", (1, 2, 3, 4, 5)]
        v = ["def", 200, "world", ["a", 1, 3, 5, "b"]]
        for i, key in enumerate(k):
            d[key] = v[i]

        mapping = d.keys().mapping

        self.assertIn("abc", mapping)
        self.assertEqual(len(mapping.keys()), len(mapping))
        for key in mapping:
            self.assertIn(key, k)
            self.assertEqual(d[key], mapping.get(key))
            self.assertEqual(mapping[key], mapping.get(key))

        self.assertEqual(mapping.get("notthere", "hello"), "hello")

        d.destroy()

    def test_keys(self):
        d = DDict(2, 1, 3000000)
        k = ["abc", 98765, "hello", (1, 2, 3, 4, 5)]
        v = ["def", 200, "world", ["a", 1, 3, 5, "b"]]
        for i, key in enumerate(k):
            d[key] = v[i]
        ddict_keys = d.keys()
        for key in k:
            self.assertTrue(key in ddict_keys)
        d.destroy()

    def test_values_mapping(self):
        d = DDict(2, 1, 3000000, trace=True)
        k = ["abc", 98765, "hello", (1, 2, 3, 4, 5)]
        v = ["def", 200, "world", ["a", 1, 3, 5, "b"]]
        for i, key in enumerate(k):
            d[key] = v[i]

        mapping = d.values().mapping

        values = mapping.values()
        self.assertEqual(len(v), len(values))
        received_val = []
        for val in values:
            received_val.append(val)

        for val in received_val:
            self.assertIn(val, v)
            v.remove(val)
        self.assertEqual(len(v), 0)

        d.destroy()

    def test_values(self):
        d = DDict(2, 1, 3000000, trace=True)
        k = ["abc", 98765, "hello", (1, 2, 3, 4, 5)]
        v = ["def", 200, "world", ["a", 1, 3, 5, "b"]]
        for i, key in enumerate(k):
            d[key] = v[i]

        for val in d.values():
            self.assertIn(val, v)
            v.remove(val)
        self.assertEqual(0, len(v))
        d.destroy()

    def test_items_mapping(self):
        d = DDict(2, 1, 3000000, trace=True)
        k = ["abc", 98765, "hello", (1, 2, 3, 4, 5)]
        v = ["def", 200, "world", ["a", 1, 3, 5, "b"]]
        for i, key in enumerate(k):
            d[key] = v[i]

        mapping = d.items().mapping

        items = mapping.items()
        self.assertEqual(len(v), len(items))
        for key, val in items:
            self.assertIn(key, mapping)
            self.assertEqual(val, mapping.get(key))

        d.destroy()

    def test_items(self):
        d = DDict(2, 1, 3000000, trace=True)
        k = ["abc", 98765, "hello", (1, 2, 3, 4, 5)]
        v = ["def", 200, "world", ["a", 1, 3, 5, "b"]]
        for i, key in enumerate(k):
            d[key] = v[i]

        for key, val in d.items():
            self.assertIn(key, k)
            self.assertIn(val, v)
            k.remove(key)
            v.remove(val)
            self.assertEqual(d[key], val)
        self.assertEqual(0, len(k))
        self.assertEqual(0, len(v))
        d.destroy()

    def test_fill(self):
        d = DDict(1, 1, 900000)
        self.assertRaises(DDictManagerFull, fillit, d)
        d.destroy()

    def test_attach_ddict(self):
        d = DDict(2, 1, 3000000, name="kdl")
        d["hello"] = "world"
        d_serialized = d.serialize()
        new_d = DDict.attach(d_serialized)
        self.assertEqual(new_d["hello"], "world")
        d.detach()
        new_d.destroy()

    def test_np_array(self):
        """Make sure we pickle numpy arrays correctly"""

        d = DDict(1, 1, int(1.5 * 1024 * 1024 * 1024))
        x = np.ones((1024, 1024), dtype=np.int32)
        d["np array"] = x
        x_ref = d["np array"]

        self.assertTrue((x & x_ref).all())
        d.destroy()

    def test_get_missing_key(self):
        d = DDict(1, 1, 4 * 1024 * 1024)

        with self.assertRaises(KeyError):
            print(d["np array"])

        d.destroy()

    def test_get_newest_chkpt_id(self):
        d = DDict(4, 1, 5000000, wait_for_writers=True, working_set_size=3, trace=True)
        proc1 = mp.Process(target=client_func_1_get_newest_chkpt_id, args=(d,))
        proc1.start()
        proc1.join()
        self.assertEqual(0, proc1.exitcode)
        self.assertEqual(d.checkpoint_id, 0)
        d.sync_to_newest_checkpoint()
        self.assertEqual(d.checkpoint_id, 4)
        d.destroy()

    def test_wait_keys_put_get(self):
        """Make sure manager behaves when put request is received prior to get request"""
        d = DDict(2, 1, 3000000, wait_for_keys=True, working_set_size=2)
        ev = mp.Event()
        # persistent key
        proc1 = mp.Process(target=client_func_1_persist, args=(d, ev))
        proc2 = mp.Process(target=client_func_2_persist, args=(d, ev))
        proc1.start()
        ev.wait()
        proc2.start()
        proc1.join()
        proc2.join()
        self.assertEqual(0, proc1.exitcode)
        self.assertEqual(0, proc2.exitcode)

        # non-persistent key
        ev2 = mp.Event()
        proc1 = mp.Process(target=client_func_1_nonpersist, args=(d, ev2))
        proc2 = mp.Process(target=client_func_2_nonpersist, args=(d, ev2))
        proc1.start()
        ev2.wait()
        proc2.start()
        proc1.join()
        proc2.join()
        self.assertEqual(0, proc1.exitcode)
        self.assertEqual(0, proc2.exitcode)

        d.destroy()

    def test_wait_keys_get_put(self):
        """Make sure manager behaves when get request is received prior to put request"""
        d = DDict(2, 1, 3000000, wait_for_keys=True, working_set_size=2)
        ev = mp.Event()
        # persistent key
        proc1 = mp.Process(target=client_func_1_persist, args=(d, ev))
        proc2 = mp.Process(target=client_func_2_persist, args=(d, ev))
        proc2.start()
        ev.wait()
        proc1.start()
        proc1.join()
        proc2.join()
        self.assertEqual(0, proc1.exitcode)
        self.assertEqual(0, proc2.exitcode)

        # non-persistent key
        ev2 = mp.Event()
        proc1 = mp.Process(target=client_func_1_nonpersist, args=(d, ev2))
        proc2 = mp.Process(target=client_func_2_nonpersist, args=(d, ev2))
        proc2.start()
        ev2.wait()
        proc1.start()
        proc1.join()
        proc2.join()
        self.assertEqual(0, proc1.exitcode)
        self.assertEqual(0, proc2.exitcode)

        d.destroy()

    def test_wait_keys_write_retired(self):
        """Make sure we raise exception right if a client tries to write keys to a checkpoint that no longer exists"""
        d = DDict(1, 1, 3000000, wait_for_keys=True, working_set_size=2)
        ev = mp.Event()
        proc1 = mp.Process(target=client_func_1_wait_keys_write_retired, args=(d, ev))
        proc2 = mp.Process(target=client_func_2_wait_keys_write_retired, args=(d,))
        proc1.start()
        ev.wait()
        proc2.start()
        proc1.join()
        proc2.join()
        self.assertEqual(0, proc1.exitcode)
        self.assertEqual(0, proc2.exitcode)
        d.destroy()

    def test_wait_keys_read_retired(self):
        """Make sure we raise exception right if a client tries to read keys from a checkpoint that no longer exists"""
        d = DDict(1, 1, 3000000, wait_for_keys=True, working_set_size=2)
        ev = mp.Event()
        proc1 = mp.Process(target=client_func_1_wait_keys_read_retired, args=(d, ev))
        proc2 = mp.Process(target=client_func_2_wait_keys_read_retired, args=(d,))
        proc1.start()
        ev.wait()
        proc2.start()
        proc1.join()
        proc2.join()
        self.assertEqual(0, proc1.exitcode)
        self.assertEqual(0, proc2.exitcode)
        d.destroy()

    def test_wait_keys_defer_put_get_same_key(self):
        """make sure the dict behaves when the current key to write and the future key to read are in deferred put and get requests"""
        d = DDict(1, 1, 3000000, wait_for_keys=True, working_set_size=2, timeout=30)
        ev = mp.Event()
        proc1 = mp.Process(target=client_func_1_wait_keys_defer_put_get_same_key, args=(d, ev))
        proc2 = mp.Process(target=client_func_2_wait_keys_defer_put_get_same_key, args=(d, ev))
        proc3 = mp.Process(target=client_func_3_wait_keys_defer_put_get_same_key, args=(d,))
        proc1.start()
        ev.wait()
        ev.clear()
        proc2.start()
        ev.wait()
        proc3.start()
        proc1.join()
        proc2.join()
        proc3.join()
        self.assertEqual(0, proc1.exitcode)
        self.assertEqual(0, proc2.exitcode)
        self.assertEqual(0, proc3.exitcode)
        d.destroy()

    def test_wait_keys_contains(self):
        d = DDict(1, 1, 3000000, wait_for_keys=True, working_set_size=2)
        ev = mp.Event()
        proc1 = mp.Process(target=client_func_1_wait_keys_contains, args=(d, ev))
        proc2 = mp.Process(target=client_func_2_wait_keys_contains, args=(d,))
        proc1.start()
        ev.wait()
        proc2.start()
        proc1.join()
        proc2.join()
        self.assertEqual(0, proc1.exitcode)
        self.assertEqual(0, proc2.exitcode)
        d.destroy()

    def test_wait_keys_pop(self):
        """add and delete a persistent key in the same checkpoint and add it back as a non-persistent key later"""
        d = DDict(1, 1, 3000000, wait_for_keys=True, working_set_size=2)
        ev = mp.Event()
        proc1 = mp.Process(target=client_func_1_wait_keys_pop, args=(d,))
        proc2 = mp.Process(target=client_func_2_wait_keys_pop, args=(d, ev))
        proc2.start()
        ev.wait()
        proc1.start()
        proc1.join()
        proc2.join()
        self.assertEqual(0, proc1.exitcode)
        self.assertEqual(0, proc2.exitcode)
        d.destroy()

    def test_wait_keys_pop_nonpersist(self):
        d = DDict(1, 1, 3000000, wait_for_keys=True, working_set_size=2)
        proc1 = mp.Process(target=client_func_1_wait_keys_pop_nonpersist, args=(d,))
        proc2 = mp.Process(target=client_func_2_wait_keys_pop_nonpersist, args=(d,))
        proc1.start()
        proc2.start()
        proc1.join()
        proc2.join()
        self.assertEqual(0, proc1.exitcode)
        self.assertEqual(0, proc2.exitcode)
        d.destroy()

    def test_wait_keys_pop_persist(self):
        d = DDict(1, 1, 3000000, wait_for_keys=True, working_set_size=2)
        proc1 = mp.Process(target=client_func_1_wait_keys_pop_persist, args=(d,))
        proc2 = mp.Process(target=client_func_2_wait_keys_pop_persist, args=(d,))
        proc2.start()
        proc2.join()
        proc1.start()
        proc1.join()
        self.assertEqual(0, proc1.exitcode)
        self.assertEqual(0, proc2.exitcode)
        d.destroy()

    def test_wait_keys_len(self):
        d = DDict(2, 1, 3000000, wait_for_keys=True, working_set_size=2, timeout=30)
        ev = mp.Event()
        proc1 = mp.Process(target=client_func_1_wait_keys_length, args=(d,))
        proc2 = mp.Process(target=client_func_2_wait_keys_length, args=(d, ev))
        proc2.start()
        ev.wait()
        proc1.start()
        proc1.join()
        proc2.join()
        self.assertEqual(0, proc1.exitcode)
        self.assertEqual(0, proc2.exitcode)
        d.destroy()

    def test_wait_keys_clear(self):
        d = DDict(1, 1, 3000000, wait_for_keys=True, working_set_size=2)
        ev = mp.Event()
        proc1 = mp.Process(target=client_func_1_wait_keys_clear, args=(d, ev))
        proc2 = mp.Process(target=client_func_2_wait_keys_clear, args=(d, ev))
        proc3 = mp.Process(target=client_func_3_wait_keys_clear, args=(d,))
        proc1.start()
        ev.wait()
        proc2.start()
        ev.wait()
        proc3.start()
        proc1.join()
        proc2.join()
        proc3.join()
        self.assertEqual(0, proc1.exitcode)
        self.assertEqual(0, proc2.exitcode)
        self.assertEqual(0, proc3.exitcode)
        d.destroy()

    def test_clear_and_stuff(self):
        d = DDict(2, 1, 3000000, wait_for_keys=True, working_set_size=2, trace=True)
        d.clear()
        d.sync_to_newest_checkpoint()
        d.destroy()

    def test_wait_keys_clear_retired(self):
        d = DDict(1, 1, 3000000, wait_for_keys=True, working_set_size=2)
        d.checkpoint()  ## 1
        d.checkpoint()  ## 2
        d["hello"] = "world"
        d.rollback()  ## 1
        d.rollback()  ## 0
        with self.assertRaises(DDictCheckpointSync):
            d.clear()
        d.destroy()

    def test_wait_keys_keys(self):
        d = DDict(1, 1, 3000000, wait_for_keys=True, working_set_size=2, timeout=30)
        ev = mp.Event()
        proc1 = mp.Process(target=client_func_1_wait_keys_keys, args=(d, ev))
        proc2 = mp.Process(target=client_func_2_wait_keys_keys, args=(d,))
        proc1.start()
        ev.wait()
        proc2.start()
        proc1.join()
        proc2.join()
        self.assertEqual(0, proc1.exitcode)
        self.assertEqual(0, proc2.exitcode)
        d.destroy()

    def test_retire_working_set_size_1(self):
        d = DDict(1, 1, 3000000, working_set_size=1, timeout=30)
        d["hello"] = "there"
        d.pput("persisted", "value")
        d.checkpoint()
        d["hello"] = "my name is bob"
        self.assertEqual(len(d), 2)
        d.destroy()

    def test_wait_writers_put_get(self):
        """Make sure manager behaves when put request is received prior get request"""
        d = DDict(2, 1, 3000000, wait_for_writers=True, working_set_size=2)
        ev = mp.Event()
        proc1 = mp.Process(target=client_func_1_wait_writers_persist, args=(d, ev))
        proc2 = mp.Process(target=client_func_2_wait_writers_persist, args=(d,))
        proc1.start()
        ev.wait()
        proc2.start()
        proc1.join()
        proc2.join()
        self.assertEqual(0, proc1.exitcode)
        self.assertEqual(0, proc2.exitcode)
        d.destroy()

    def test_wait_writers_get_put(self):
        """Make sure manager behaves when get request is received prior put request"""
        d = DDict(2, 1, 3000000, wait_for_writers=True, working_set_size=2)
        ev = mp.Event()
        proc1 = mp.Process(target=client_func_1_wait_writers_persist, args=(d, ev))
        proc2 = mp.Process(target=client_func_2_wait_writers_persist_err, args=(d, ev))
        proc2.start()
        ev.wait()
        proc1.start()
        proc1.join()
        proc2.join()
        self.assertEqual(0, proc1.exitcode)
        self.assertEqual(0, proc2.exitcode)
        d.destroy()

    def test_wait_writers_read_future_chkpt(self):
        d = DDict(1, 1, 3000000, wait_for_writers=True, working_set_size=2)
        ev = mp.Event()
        proc1 = mp.Process(target=client_func_1_wait_writers_read_future_chkpt, args=(d, ev))
        proc2 = mp.Process(target=client_func_2_wait_writers_read_future_chkpt, args=(d,))
        proc1.start()
        ev.wait()
        proc2.start()
        proc1.join()
        proc2.join()
        self.assertEqual(0, proc1.exitcode)
        self.assertEqual(0, proc2.exitcode)
        d.destroy()

    def test_wait_writers_write_retired(self):
        d = DDict(1, 1, 3000000, wait_for_writers=True, working_set_size=2)
        ev = mp.Event()
        proc1 = mp.Process(target=client_func_1_wait_writers_write_retired, args=(d, ev))
        proc2 = mp.Process(target=client_func_2_wait_writers_write_retired, args=(d,))
        proc1.start()
        ev.wait()
        proc2.start()
        proc1.join()
        proc2.join()
        self.assertEqual(0, proc1.exitcode)
        self.assertEqual(0, proc2.exitcode)
        d.destroy()

    def test_wait_writers_read_retired(self):
        d = DDict(1, 1, 3000000, wait_for_writers=True, working_set_size=2)
        ev = mp.Event()
        proc1 = mp.Process(target=client_func_1_wait_writers_write_retired, args=(d, ev))
        proc2 = mp.Process(target=client_func_2_wait_writers_read_retired, args=(d,))
        proc1.start()
        ev.wait()
        proc2.start()
        proc1.join()
        proc2.join()
        self.assertEqual(0, proc1.exitcode)
        self.assertEqual(0, proc2.exitcode)
        d.destroy()

    def test_wait_writers_contains(self):
        d = DDict(2, 1, 3000000, wait_for_writers=True, working_set_size=2)
        ev = mp.Event()
        proc1 = mp.Process(target=client_func_1_wait_writers_contains, args=(d, ev))
        proc2 = mp.Process(target=client_func_2_wait_writers_contains, args=(d,))
        proc1.start()
        ev.wait()
        proc2.start()
        proc1.join()
        proc2.join()
        self.assertEqual(0, proc1.exitcode)
        self.assertEqual(0, proc2.exitcode)
        d.destroy()

    def test_wait_writers_pop(self):
        d = DDict(2, 1, 3000000, wait_for_writers=True, working_set_size=2)
        ev = mp.Event()
        proc1 = mp.Process(target=client_func_1_wait_writers_pop, args=(d, ev))
        proc2 = mp.Process(target=client_func_2_wait_writers_pop, args=(d,))
        proc1.start()
        ev.wait()
        proc2.start()
        proc1.join()
        proc2.join()
        self.assertEqual(0, proc1.exitcode)
        self.assertEqual(0, proc2.exitcode)
        d.destroy()

    def test_wait_writers_len(self):
        d = DDict(2, 1, 3000000, wait_for_writers=True, working_set_size=2)
        ev = mp.Event()
        proc1 = mp.Process(target=client_func_1_wait_writers_len, args=(d, ev))
        proc2 = mp.Process(target=client_func_2_wait_writers_len, args=(d,))
        proc1.start()
        ev.wait()
        proc2.start()
        proc1.join()
        proc2.join()
        self.assertEqual(0, proc1.exitcode)
        self.assertEqual(0, proc2.exitcode)
        d.destroy()

    def test_wait_writers_keys(self):
        d = DDict(2, 1, 3000000, wait_for_writers=True, working_set_size=2, trace=True)
        ev = mp.Event()
        proc1 = mp.Process(target=client_func_1_wait_writers_keys, args=(d, ev))
        proc2 = mp.Process(target=client_func_2_wait_writers_keys, args=(d,))
        proc1.start()
        ev.wait()
        proc2.start()
        proc1.join()
        proc2.join()
        self.assertEqual(0, proc1.exitcode)
        self.assertEqual(0, proc2.exitcode)
        d.destroy()

    def test_zero_byte_recv(self):
        main_channel = dch.Channel.make_process_local()
        resp_fli = fli.FLInterface(main_ch=main_channel)
        resp_fli_ser = b64encode(resp_fli.serialize())

        host = socket.gethostname()

        my_alloc = System()
        node_list = my_alloc.nodes
        num_nodes = my_alloc.nnodes
        node = Node(node_list[-1])
        queue = mp.Queue()

        with Policy(placement=Policy.Placement.HOST_NAME, host_name=node.hostname):
            remote = mp.Process(target=remote_proc, args=(resp_fli_ser, queue))
            remote.start()

        # Read the serialized FLI of the orchestrator.
        remote_fli_ser = queue.get()

        remote_fli = fli.FLInterface.attach(b64decode(remote_fli_ser))

        with remote_fli.recvh(destination_pool=self.mpool) as recvh:
            (x, _) = recvh.recv_mem()  # recv_bytes returns a tuple, first the bytes then the message attribute
            remote_host = x.get_memview().tobytes().decode("utf-8")
            if num_nodes == 1:
                self.assertEqual(remote_host, host)
            else:
                self.assertNotEqual(remote_host, host)

            global POOL_MUID
            self.assertEqual(x.pool.muid, POOL_MUID)

            with self.assertRaises(FLIEOT):
                (x, _) = recvh.recv_bytes()  # We should get back an EOT here

        with resp_fli.sendh(use_main_as_stream_channel=True) as sendh:
            sendh.send_bytes("Done!".encode("utf-8"))

        remote.join()

    def test_wait_writers_rollback(self):
        d = DDict(2, 1, 3000000, wait_for_writers=True, working_set_size=2)
        ev = mp.Event()
        proc1 = mp.Process(target=client_func_1_wait_writers_rollback, args=(d, ev))
        proc2 = mp.Process(target=client_func_2_wait_writers_rollback, args=(d,))
        proc1.start()
        ev.wait()
        proc2.start()
        proc1.join()
        proc2.join()
        self.assertEqual(proc1.exitcode, 0)
        self.assertEqual(proc2.exitcode, 0)
        d.destroy()

    def test_get_name(self):
        name = f"ddict_test_get_name_{getpass.getuser()}"
        d = DDict(2, 1, 3000000, trace=True, name=name)
        self.assertIn(name, d.get_name())
        d.destroy()

    def test_restart(self):
        name = f"ddict_test_restart_{getpass.getuser()}"
        d = DDict(2, 1, 3000000, trace=True, name=name)
        manager_nodes = d.manager_nodes
        for i in range(len(manager_nodes)):
            manager_nodes[i] = b64encode(cloudpickle.dumps(manager_nodes[i]))
        d.destroy(allow_restart=True)

        d2 = DDict(2, 1, 3000000, trace=True, name=name, restart=True)
        manager_nodes_d2 = d2.manager_nodes
        for i in range(len(manager_nodes)):
            manager_nodes_d2[i] = b64encode(cloudpickle.dumps(manager_nodes_d2[i]))
        self.assertEqual(manager_nodes, manager_nodes_d2)
        d2.destroy()

    def test_restart_with_keys(self):
        name = f"ddict_test_restart_with_keys_{getpass.getuser()}"
        d = DDict(2, 1, 3000000, trace=True, name=name)
        d["dragon"] = "runtime"
        d["hello"] = "world"
        d.destroy(allow_restart=True)

        d2 = DDict(2, 1, 3000000, trace=True, name=name, restart=True)
        self.assertEqual(d2["dragon"], "runtime")
        self.assertEqual(d2["hello"], "world")
        d2.destroy()

    def test_restart_without_name(self):
        # should raise exception properly
        try:
            d2 = DDict(2, 1, 3000000, trace=True, restart=True)
        except Exception as ex:
            self.assertIn("Name was not specified", str(ex))

    def test_restart_name_not_found(self):
        # should raise expection properly
        try:
            d2 = DDict(2, 1, 3000000, trace=True, name=f"test_name_not_exist_{getpass.getuser()}", restart=True)
        except Exception as ex:
            self.assertIn("No such file or directory", str(ex))

    def test_restart_num_managers_mismatch(self):
        # should raise expection properly
        name = f"ddict_test_restart_num_manager_mismatch_{getpass.getuser()}"
        d = DDict(2, 1, 3000000, trace=True, name=name)
        d.destroy(allow_restart=True)
        try:
            d2 = DDict(1, 1, 3000000, trace=True, name=name, restart=True)
        except Exception as ex:
            self.assertIn("DDict number of managers mismatch", str(ex))

        # For cleanup of the test.
        d2 = DDict(2, 1, 3000000, trace=True, name=name, restart=True)
        d2.destroy()

    @unittest.skip("the test cannot join orchestrator process")
    def test_restart_meta_data_mismatch(self):
        # should raise expection properly
        name = f"ddict_test_restart_meta_data_mismatch_{getpass.getuser()}"
        d = DDict(2, 1, 3000000, working_set_size=2, wait_for_keys=True, trace=True, name=name)
        d.destroy(allow_restart=True)
        try:
            d2 = DDict(2, 1, 3000000, working_set_size=2, wait_for_writers=True, trace=True, name=name, restart=True)
        except Exception as ex:
            self.assertIn("wait_for_keys mismatch", str(ex))
            # print(f'{ex}', flush=True)
            # TODO: kill orchestrator proc
            # d2.destroy()

    def test_restart_with_wait_keys(self):
        name = f"ddict_test_restart_with_wait_keys_{getpass.getuser()}"
        d = DDict(2, 1, 3000000, working_set_size=2, wait_for_keys=True, trace=True, name=name)
        ## 0
        d.pput("hello", "world")
        d["Miska"] = "dog"  # non-persist key
        d.checkpoint()  ## 1
        d.pput("dragon", "runtime")
        del d["hello"]
        d.destroy(allow_restart=True)

        d2 = DDict(2, 1, 3000000, working_set_size=2, wait_for_keys=True, trace=True, name=name, restart=True)
        ## 0
        self.assertEqual(d2["hello"], "world")
        self.assertEqual(d2["Miska"], "dog")  # non-persist key
        d2.checkpoint()  ## 1
        self.assertEqual(d2["dragon"], "runtime")
        self.assertTrue("hello" not in d2)
        d2.destroy()

    @unittest.skip("Hanging")
    def test_restart_with_deferred_ops_discarded(self):
        name = f"ddict_test_restart_with_deferred_ops_discarded_{getpass.getuser()}"
        d = DDict(2, 1, 3000000, working_set_size=2, wait_for_keys=True, trace=True, name=name)
        ev = mp.Event()
        proc1 = mp.Process(target=client_func_1_restart_with_defer_ops_discarded, args=(d, ev))
        proc1.start()
        ev.wait()
        proc1.terminate()
        if not proc1.join(3):
            proc1.kill()
            proc1.join()
        d.destroy(allow_restart=True)

        d2 = DDict(2, 1, 3000000, working_set_size=2, wait_for_keys=True, trace=True, name=name, restart=True)
        proc1 = mp.Process(target=client_func_1_d2_restart_with_defer_ops_discarded, args=(d2,))
        proc1.start()
        proc1.join()
        self.assertEqual(0, proc1.exitcode)
        d2.destroy()

    def test_multiple_restart(self):
        name = f"ddict_test_multiple_restart_{getpass.getuser()}"
        d = DDict(2, 1, 3000000, working_set_size=2, wait_for_keys=True, trace=True, name=name)
        ## 0
        d.pput("hello", "world")
        d.checkpoint()  ## 1
        d.pput("dragon", "runtime")
        d.destroy(allow_restart=True)

        # restart and destory dictionary 3 times
        for i in range(3):
            d_restart = DDict(
                2, 1, 3000000, working_set_size=2, wait_for_keys=True, trace=True, name=name, restart=True
            )
            ## 0
            self.assertTrue("hello" in d_restart)
            self.assertFalse("dragon" in d_restart)
            d_restart.checkpoint()  ## 1
            self.assertTrue("dragon" in d_restart)
            if i == 2:
                d_restart.destroy()
            else:
                d_restart.destroy(allow_restart=True)

    def test_get_empty_managers(self):
        name = f"test_get_empty_managers_{getpass.getuser()}"
        d = DDict(2, 1, 3000000, trace=True, name=name)
        self.assertEqual(d.empty_managers, [])
        d.destroy(allow_restart=True)

        d2 = DDict(2, 1, 3000000, trace=True, restart=True, name=name)
        self.assertEqual(d2.empty_managers, [])
        d2.destroy()

    def test_cross_ddict_sync(self):
        d = DDict(2, 1, 3000000, trace=True)
        d_ser = d.serialize()
        d_m1 = d.manager(1)

        d1 = DDict(2, 1, 3000000, trace=True)
        d1_ser = d1.serialize()
        d1_m1 = d1.manager(1)

        d_m1["test"] = "ddict"
        d_m1["hello"] = "world"
        d_m1["dragon"] = "runtime"

        d1_m1["test"] = "ddict"
        d1_m1["hello"] = "world"
        d1_m1["dragon"] = "runtime"

        d1_m1.clear()
        d1_m1._mark_as_drained(1)

        DDict.synchronize_ddicts([d_ser, d1_ser])

        self.assertEqual(d1_m1["test"], "ddict")
        self.assertEqual(d1_m1["hello"], "world")
        self.assertEqual(d1_m1["dragon"], "runtime")

        DDict.synchronize_ddicts([d_ser, d1_ser])

        self.assertEqual(d1_m1["test"], "ddict")
        self.assertEqual(d1_m1["hello"], "world")
        self.assertEqual(d1_m1["dragon"], "runtime")

        d.destroy()
        d1.destroy()

    def test_cross_ddict_sync_with_checkpoint(self):

        d = DDict(2, 1, 3000000, wait_for_keys=True, working_set_size=4, trace=True)
        d_1 = DDict(2, 1, 3000000, wait_for_keys=True, working_set_size=4, trace=True)

        d_m0 = d.manager(0)
        d_1_m0 = d_1.manager(0)

        d_m0.checkpoint()  # 1
        d_m0["dragon"] = "runtime"
        d_m0["hello"] = "world"

        d_1_m0.checkpoint()  # 1
        d_1_m0[100] = 1000
        d_1_m0["hello"] = "world"

        d_m0._mark_as_drained(0)
        DDict.synchronize_ddicts([d_m0.serialize(), d_1_m0.serialize()])

        self.assertFalse("dragon" in d_m0)
        self.assertTrue(100 in d_m0)
        self.assertTrue("hello" in d_m0)

        self.assertFalse("dragon" in d_1_m0)
        self.assertTrue(100 in d_1_m0)
        self.assertTrue("hello" in d_1_m0)

        d.destroy()
        d_1.destroy()

    def test_batch_put(self):
        d = DDict(4, 1, 12000000, trace=True, working_set_size=2, wait_for_keys=True)
        NUM_KEYS = 100
        random_key = np.random.rand(NUM_KEYS)
        random_val = np.random.rand(NUM_KEYS)

        d.start_batch_put(persist=False)

        for i in range(NUM_KEYS):
            d[random_key[i]] = random_val[i]

        d.end_batch_put()

        # Check each pair of kv after batch put ends
        for i in range(NUM_KEYS):
            self.assertEqual(d[random_key[i]], random_val[i])

        d.destroy()

    def test_batch_put_multi_clients(self):
        d = DDict(4, 1, 12000000, trace=True, working_set_size=2, wait_for_keys=True)

        NUM_KEYS = 100
        random_key = np.random.rand(NUM_KEYS)
        random_val = np.random.rand(NUM_KEYS)

        NUM_PROCS = 10
        NUM_KEYS_PER_PROC = int(NUM_KEYS / NUM_PROCS)
        grp = ProcessGroup(restart=False)
        for i in range(NUM_PROCS):
            grp.add_process(
                nproc=1,
                template=ProcessTemplate(
                    target=batch_put,
                    args=(
                        d,
                        random_key[i * NUM_KEYS_PER_PROC : (i + 1) * NUM_KEYS_PER_PROC],
                        random_val[i * NUM_KEYS_PER_PROC : (i + 1) * NUM_KEYS_PER_PROC],
                    ),
                ),
            )

        grp.init()
        grp.start()
        grp.join()
        grp.close()

        # Check each pair of kv after batch put ends
        for i in range(NUM_KEYS):
            self.assertEqual(d[random_key[i]], random_val[i])
        d.destroy()

    def test_batch_put_invalid_ops(self):
        d = DDict(4, 1, 12000000, trace=True, working_set_size=2, wait_for_keys=True)

        d.start_batch_put(persist=False)
        # raise exception if persist does't match
        with self.assertRaises(DDictError) as ex:
            d.pput("dragon", "runtime")
            self.assertEqual(ex.lib_err, "DRAGON_INVALID_OPERATION")
        d.end_batch_put()

        d.start_batch_put(persist=True)
        # raise exception if persist does't match
        with self.assertRaises(DDictError) as ex:
            d["dragon"] = "runtime"
            self.assertEqual(ex.lib_err, "DRAGON_INVALID_OPERATION")

        # Proceeding checkpoint during batch put is invalid
        with self.assertRaises(DDictError) as ex:
            d.checkpoint()
            self.assertEqual(ex.lib_err, "DRAGON_INVALID_OPERATION")

        # Roll back checkoint during batch put is invalid
        with self.assertRaises(DDictError) as ex:
            d.rollback()
            self.assertEqual(ex.lib_err, "DRAGON_INVALID_OPERATION")

        d.end_batch_put()

        d.destroy()

    def test_multiple_batch_put(self):
        """Run multiple rounds of batch put in sequence."""
        d = DDict(4, 1, 120000000, trace=True, working_set_size=2, wait_for_keys=True)

        NUM_KEYS = 100
        random_key = np.random.rand(NUM_KEYS)
        random_val = np.random.rand(NUM_KEYS)

        # first batch
        d.start_batch_put(persist=False)
        for i in range(NUM_KEYS):
            d[random_key[i]] = random_val[i]
        d.end_batch_put()

        # second batch
        d.start_batch_put(persist=True)
        for i in range(NUM_KEYS):
            d.pput(random_key[i], random_val[i])
        d.end_batch_put()

        # Check each pair of kv after batch put ends
        for i in range(NUM_KEYS):
            self.assertEqual(d[random_key[i]], random_val[i])
        d.destroy()

    def test_batch_put_future_checkpoint(self):
        ddict = DDict(1, 1, 3000000, trace=True, working_set_size=2, wait_for_keys=True)

        ## 0
        ddict["dragon"] = "runtime"
        ddict.checkpoint()  ## 1
        ddict.checkpoint()  ## 2

        ddict.start_batch_put(persist=False)
        ddict["hello"] = "world"
        ddict[1] = 2
        with self.assertRaises(DDictFutureCheckpoint):
            ddict.end_batch_put()

        ddict.destroy()

    def test_batch_put_checkpoint_retired(self):
        ddict = DDict(1, 1, 3000000, trace=True, working_set_size=2, wait_for_keys=True)

        ## 0
        ddict.checkpoint()  ## 1
        ddict.checkpoint()  ## 2
        ddict["dragon"] = "runtime"  # checkpoint 0 retired

        ddict.rollback()  ## 1
        ddict.rollback()  ## 0
        ddict.start_batch_put(persist=False)
        ddict["hello"] = "world"
        ddict[1] = 2
        with self.assertRaises(DDictCheckpointSync):
            ddict.end_batch_put()

        ddict.destroy()

    def test_filter(self):
        d = DDict(2, 1, 3000000)

        d["test"] = "ddict"
        d["hello"] = "world"
        d["dragon"] = "runtime"

        d["test2"] = "ddict"
        d["hello2"] = "world"
        d["dragon2"] = "runtime"

        candidate_list = []
        with d.filter(sort_keys, (), sort_keys_comparator) as candidates:
            for candidate in candidates:
                candidate_list.append(candidate)

        keys = list(d.keys())
        keys.sort()
        self.assertEqual(candidate_list, keys)
        d.destroy()

    def test_custom_pickler(self):
        ddict = DDict(2, 1, 3000000)

        INT_NUM_BYTES = 4
        key = 2048
        arr = [[0.12, 0.31, 3.4], [4.579, 5.98, 6.54]]
        value = np.array(arr)
        with ddict.pickler(intKeyPickler(INT_NUM_BYTES), numPy2dValuePickler((2, 3), np.double)) as type_dd:
            type_dd[key] = value
            get_val = type_dd[key]
            self.assertTrue(np.array_equal(value, get_val))

        ddict.destroy()

    def test_local_managers(self):
        d = DDict(2, 1, 3000000)
        local_managers = d.local_managers
        self.assertIn(0, local_managers)
        self.assertIn(1, local_managers)
        d.destroy()

    def test_local_keys(self):
        d = DDict(2, 1, 3000000)
        d["hello"] = "world"
        d[1] = 2
        local_keys = d.local_keys()
        self.assertIn("hello", local_keys)
        self.assertIn(1, local_keys)
        d.destroy()

    def test_local_values(self):
        d = DDict(2, 1, 3000000)
        d["hello"] = "world"
        d[1] = 2
        expected_vals = ["world", 2]
        local_values = d.local_values()
        for val in local_values:
            self.assertIn(val, expected_vals)
            expected_vals.remove(val)
        self.assertEqual(0, len(expected_vals))
        d.destroy()

    def test_local_items(self):
        d = DDict(2, 1, 3000000)
        d["hello"] = "world"
        d[1] = 2
        py_d = {}
        py_d["hello"] = "world"
        py_d[1] = 2
        local_items = d.local_items()
        self.assertEqual(len(py_d), len(local_items))
        for item in local_items:
            k, v = item
            self.assertIn(k, py_d)
            self.assertEqual(v, py_d[k])
            py_d.pop(k)
        self.assertEqual(0, len(py_d))
        d.destroy()

    def test_ddict_clone(self):

        d = DDict(2, 1, 3000000, trace=True)
        d_clone = DDict(2, 1, 3000000, trace=True)
        d_clone_1 = DDict(2, 1, 3000000, trace=True)

        d["dragon"] = "runtime"
        d["hello"] = "world"
        d[100] = 1000
        d["ddict"] = "test"
        d_clone["test_key"] = "test_val"
        d_clone_1["test_key_1"] = "test_val_1"

        d.clone([d_clone.serialize(), d_clone_1.serialize()])

        self.assertEqual(d_clone["dragon"], "runtime")
        self.assertEqual(d_clone["hello"], "world")
        self.assertEqual(d_clone[100], 1000)
        self.assertEqual(d_clone["ddict"], "test")
        self.assertFalse("test_key" in d_clone)

        self.assertEqual(d_clone_1["dragon"], "runtime")
        self.assertEqual(d_clone_1["hello"], "world")
        self.assertEqual(d_clone_1[100], 1000)
        self.assertEqual(d_clone_1["ddict"], "test")
        self.assertFalse("test_key_1" in d_clone_1)

        d.destroy()
        d_clone.destroy()
        d_clone_1.destroy()

    def test_ddict_clone_with_chkpt(self):

        d = DDict(2, 1, 3000000, wait_for_keys=True, working_set_size=4, trace=True)
        d_clone = DDict(2, 1, 3000000, wait_for_keys=True, working_set_size=4, trace=True)
        d_clone_1 = DDict(2, 1, 3000000, wait_for_keys=True, working_set_size=4, trace=True)

        d.checkpoint()  # 1
        d["dragon"] = "runtime"
        d["hello"] = "world"
        d[100] = 1000
        d["ddict"] = "test"

        d_clone.checkpoint()  # 1
        d_clone["test_key"] = "test_val"

        d_clone_1.checkpoint()  # 1
        d_clone_1.checkpoint()  # 2
        d_clone_1["test_key_1"] = "test_val_1"

        d.clone([d_clone.serialize(), d_clone_1.serialize()])

        self.assertEqual(d_clone["dragon"], "runtime")
        self.assertEqual(d_clone["hello"], "world")
        self.assertEqual(d_clone[100], 1000)
        self.assertEqual(d_clone["ddict"], "test")
        self.assertFalse("test_key" in d_clone)

        self.assertFalse("test_key_1" in d_clone_1)
        d_clone_1.rollback()  # 1
        self.assertEqual(d_clone_1["dragon"], "runtime")
        self.assertEqual(d_clone_1["hello"], "world")
        self.assertEqual(d_clone_1[100], 1000)
        self.assertEqual(d_clone_1["ddict"], "test")

        d_clone_1.rollback()  # 0
        self.assertFalse("dragon" in d_clone_1)
        self.assertFalse("hello" in d_clone_1)
        self.assertFalse(100 in d_clone_1)
        self.assertFalse("ddict" in d_clone_1)

        d.destroy()
        d_clone.destroy()
        d_clone_1.destroy()

    def test_copy(self):
        d = DDict(2, 1, 3000000, wait_for_keys=True, working_set_size=4, trace=True)
        d["hello"] = "world"
        name = f"test_copy_name_{getpass.getuser()}"
        d_copy = d.copy(name=name)
        self.assertEqual(d_copy["hello"], "world")
        self.assertEqual(name, d_copy.get_name())
        d.destroy()
        d_copy.destroy()

    def test_copy_from_handle(self):
        d = DDict(2, 1, 3000000, wait_for_keys=True, working_set_size=4, trace=True)
        d_attach = d.attach(d.serialize())
        with self.assertRaises(DDictError):
            d_copy = d_attach.copy()
        d.destroy()

    def test_bput_bget(self):
        NUM_MANAGERS = 5
        d = DDict(NUM_MANAGERS, 1, 1500000 * NUM_MANAGERS, wait_for_keys=True, working_set_size=4, trace=True)
        d.bput("hello", "world")

        # bget from every manager (In the dictionary handling with manager selection, the main manager is the selected manager)
        for i in range(NUM_MANAGERS):
            dm = d.manager(i)
            self.assertEqual(dm.bget("hello"), "world")
        d.destroy()

    def test_bput_batch(self):
        NUM_MANAGERS = 12
        d = DDict(NUM_MANAGERS, 1, 1500000 * NUM_MANAGERS, wait_for_keys=True, working_set_size=4, trace=True)

        NUM_KEYS = 10
        random_key = np.random.rand(NUM_KEYS)
        random_val = np.random.rand(NUM_KEYS)

        d.start_batch_put(persist=False)

        for i in range(NUM_KEYS):
            d.bput(random_key[i], random_val[i])

        d.end_batch_put()

        for m in range(NUM_MANAGERS):
            d_select = d.manager(m)
            for i in range(NUM_KEYS):
                self.assertEqual(d_select.bget(random_key[i]), random_val[i])
        d.destroy()

    def test_bput_multiple_batch(self):
        NUM_MANAGERS = 12
        d = DDict(NUM_MANAGERS, 1, 1500000 * NUM_MANAGERS, wait_for_keys=True, working_set_size=4, trace=True)

        # first batch
        NUM_KEYS = 10
        random_key = np.random.rand(NUM_KEYS)
        random_val = np.random.rand(NUM_KEYS)

        d.start_batch_put(persist=False)

        for i in range(NUM_KEYS):
            d.bput(random_key[i], random_val[i])

        d.end_batch_put()

        for m in range(NUM_MANAGERS):
            d_select = d.manager(m)
            for i in range(NUM_KEYS):
                self.assertEqual(d_select.bget(random_key[i]), random_val[i])

        # second batch
        NUM_KEYS = 10
        random_key = np.random.rand(NUM_KEYS)
        random_val = np.random.rand(NUM_KEYS)

        d.start_batch_put(persist=False)

        for i in range(NUM_KEYS):
            d.bput(random_key[i], random_val[i])

        d.end_batch_put()

        for m in range(NUM_MANAGERS):
            d_select = d.manager(m)
            for i in range(NUM_KEYS):
                self.assertEqual(d_select.bget(random_key[i]), random_val[i])

        d.destroy()

    def test_bput_invalid_op(self):
        NUM_MANAGERS = 12
        d = DDict(NUM_MANAGERS, 1, 1500000 * NUM_MANAGERS, trace=True, working_set_size=2, wait_for_keys=True)

        d.start_batch_put(persist=True)

        try:
            d.bput("hello", "world")
            raise Exception(f"Expected DDictError DRAGON_INVALID_OPERATION is not raised")
        except DDictError as ex:
            self.assertEqual(ex.lib_err, "DRAGON_INVALID_OPERATION")

        d.end_batch_put()
        d.destroy()

    def test_bput_batch_future_chkpt(self):
        NUM_MANAGERS = 12
        ddict = DDict(NUM_MANAGERS, 1, 1500000 * NUM_MANAGERS, trace=True, working_set_size=2, wait_for_keys=True)

        ## 0
        ddict.bput("dragon", "runtime")
        ddict.checkpoint()  ## 1
        ddict.checkpoint()  ## 2

        ddict.start_batch_put(persist=False)
        ddict.bput("hello", "world")
        ddict.bput(1, 2)
        with self.assertRaises(DDictFutureCheckpoint) as ex:
            ddict.end_batch_put()
        self.assertIn("DDICT_FUTURE_CHECKPOINT", str(ex.exception), "Failed to raise exception with expected details.")

        ddict.destroy()

    def test_bput_batch_retired_chkpt(self):
        NUM_MANAGERS = 12
        ddict = DDict(NUM_MANAGERS, 1, 1500000 * NUM_MANAGERS, trace=True, working_set_size=2, wait_for_keys=True)

        ## 0
        ddict.checkpoint()  ## 1
        ddict.checkpoint()  ## 2
        ddict.bput("dragon", "runtime")  # checkpoint 0 retired

        ddict.rollback()  ## 1
        ddict.rollback()  ## 0
        ddict.start_batch_put(persist=False)
        ddict.bput("hello", "world")
        ddict.bput(1, 2)
        with self.assertRaises(DDictCheckpointSync):
            ddict.end_batch_put()

        ddict.destroy()

    def test_bput_manager_full(self):
        NUM_MANAGERS = 12
        ddict = DDict(NUM_MANAGERS, 1, 4096 * NUM_MANAGERS, trace=True, working_set_size=2, wait_for_keys=True)
        x = np.ones(4096, dtype=np.int32)
        with self.assertRaises(DDictManagerFull):
            ddict.bput("np array ", x)
        ddict.destroy()

    def test_defer_bput(self):
        NUM_MANAGERS = 12
        ddict = DDict(NUM_MANAGERS, 1, 1500000 * NUM_MANAGERS, trace=True, working_set_size=2, wait_for_keys=True)

        ev = mp.Event()
        ev1 = mp.Event()

        proc1 = mp.Process(target=client_func_1_defer_bput, args=(ddict, ev))
        proc2 = mp.Process(target=client_func_2_defer_bput, args=(ddict, ev1))
        proc3 = mp.Process(target=client_func_3_defer_bput, args=(ddict,))

        proc1.start()
        ev.wait()
        proc2.start()
        ev1.wait()
        proc3.start()

        proc1.join()
        proc2.join()
        proc3.join()
        self.assertEqual(0, proc1.exitcode)
        self.assertEqual(0, proc2.exitcode)
        self.assertEqual(0, proc2.exitcode)

        ddict.destroy()

    @unittest.skip("should ran manually only")
    def test_defer_get_with_exception(self):
        NUM_MANAGERS = 1
        ddict = DDict(NUM_MANAGERS, 1, 1500000 * NUM_MANAGERS, trace=True, working_set_size=2, wait_for_keys=True)
        ev = mp.Event()
        proc1 = mp.Process(target=client_func_1_defer_get_with_exception, args=(ddict, ev))
        proc2 = mp.Process(target=client_func_2_defer_get_with_exception, args=(ddict,))

        proc1.start()
        ev.wait()
        proc2.start()

        proc1.join()
        proc2.join()
        self.assertEqual(0, proc1.exitcode)
        self.assertEqual(0, proc2.exitcode)

        ddict.destroy()

    def test_freeze(self):
        ddict = DDict(2, 1, 1500000 * 2, trace=True, working_set_size=2, wait_for_keys=True)
        ddict.freeze()
        self.assertTrue(ddict.is_frozen)
        ddict.unfreeze()
        self.assertFalse(ddict.is_frozen)
        ddict.destroy()

    def test_freeze_invalid_ops(self):
        ddict = DDict(2, 1, 1500000 * 2, trace=True, working_set_size=2, wait_for_keys=True)

        ddict.freeze()
        self.assertTrue(ddict.is_frozen)

        with self.assertRaises(DDictError) as ex:
            ddict["hello"] = "world"
        self.assertIn("DRAGON_INVALID_OPERATION", str(ex.exception), "Failed to raise exception with expected details.")

        with self.assertRaises(DDictError) as ex:
            ddict.start_batch_put()
            ddict["hello"] = "world"
            ddict.end_batch_put()
        self.assertIn("DRAGON_INVALID_OPERATION", str(ex.exception), "Failed to raise exception with expected details.")

        with self.assertRaises(DDictError) as ex:
            ddict.pop("hello")
        self.assertIn("DRAGON_INVALID_OPERATION", str(ex.exception), "Failed to raise exception with expected details.")

        with self.assertRaises(DDictError) as ex:
            ddict.clear()
        self.assertIn("DRAGON_INVALID_OPERATION", str(ex.exception), "Failed to raise exception with expected details.")

        with self.assertRaises(DDictError) as ex:
            ddict.bput("hello", "world")
        self.assertIn("DRAGON_INVALID_OPERATION", str(ex.exception), "Failed to raise exception with expected details.")

        ddict.destroy()

    def test_get_from_frozen_dict(self):
        ddict = DDict(2, 1, 1500000 * 2, trace=True, working_set_size=2, wait_for_keys=True)
        ddict["hello"] = "world"
        ddict["Miska"] = "dog"
        ddict.freeze()
        self.assertTrue(ddict.is_frozen)
        x = ddict["hello"]
        x1 = ddict["hello"]  # Exception raised! The key might be deleted in the first read.
        self.assertEqual(x1, "world")
        self.assertEqual(ddict["Miska"], "dog")
        ddict.destroy()


class TestDDictPersist(unittest.TestCase):

    ################################################
    # test_persist: read only mode
    ################################################

    def tearDown(cls):
        for p in pathlib.Path(".").glob("*.dat"):
            os.remove(p)

    def test_persist_1_checkpoint_freq_1_cnt_1_restore_from_0(self):
        """Persist one checkpoint and restore."""
        # This test persists chkpt 0 to disk. Persist count is 1 so the chkpt is kept.
        # A new dictionary then restores from chkpt 0 later.
        NUM_MANAGERS = 1
        d = DDict(
            NUM_MANAGERS,
            1,
            1500000 * NUM_MANAGERS,
            trace=True,
            working_set_size=2,
            wait_for_keys=True,
            persist_path="",
            persist_freq=1,
            persist_count=1,
            persister_class=PosixCheckpointPersister,
        )
        d["key"] = "v0"
        d.checkpoint()  ## 1
        d["key"] = "v1"
        d.checkpoint()  ## 2
        d["key"] = "v2"  ## retire and write checkpoint 0 to disk

        restore_name = d.get_name()
        d.destroy()

        chkpt_restore = 0
        dd_restore = DDict(
            NUM_MANAGERS,
            1,
            1500000 * NUM_MANAGERS,
            trace=True,
            persist_path=".",
            name=restore_name,
            restore_from=chkpt_restore,
            read_only=True,
            persister_class=PosixCheckpointPersister,
        )
        # chkpt ID should be set to the one ddict restored from
        self.assertEqual(dd_restore.checkpoint_id, chkpt_restore)
        self.assertEqual(dd_restore["key"], "v0")
        dd_restore.destroy()

    def test_persist_2_checkpoint_freq_1_cnt_1_restore_from_1(self):
        """
        Persist more than one checkpoints and restore from the second checkpoint. Also make sure the
        older persist file is removed when the number of persistedcheckpoints exceeds persist count.
        """
        # This test persists chkpt 0 and 1 to disk. Persist count is 1 so the oldest persist chkpt, which
        # is chkpt 0 is removed from disk before persisting chkpt 1. A new dicitonary than restores from
        # chkpt 1 later.
        NUM_MANAGERS = 1
        d = DDict(
            NUM_MANAGERS,
            1,
            1500000 * NUM_MANAGERS,
            trace=True,
            working_set_size=2,
            wait_for_keys=True,
            persist_path="",
            persist_freq=1,
            persist_count=1,
            name="ddict_restore_test",
            persister_class=PosixCheckpointPersister,
        )
        d["key"] = "v0"
        d.checkpoint()  ## 1
        d["key"] = "v1"
        d.checkpoint()  ## 2
        d["key"] = "v2"  ## retire and write checkpoint 0 to disk
        d.checkpoint()  ## 3
        # Retire and write checkpoint 1 to disk, persist checkpoint 0
        # is removed as the persist count is 1, so only 1 persisted checkpint
        # file is allowed in the persist path at any time.
        d["key"] = "v3"

        restore_name = d.get_name()
        d.destroy()

        chkpt_restore = 1
        dd_restore = DDict(
            NUM_MANAGERS,
            1,
            1500000 * NUM_MANAGERS,
            trace=True,
            persist_path="",
            name=restore_name,
            restore_from=chkpt_restore,
            read_only=True,
            persister_class=PosixCheckpointPersister,
        )
        # checkpoint ID should be set to the one ddict restored from
        self.assertEqual(dd_restore.checkpoint_id, chkpt_restore)
        self.assertEqual(dd_restore["key"], "v1")
        dd_restore.destroy()

    def test_persist_2_checkpoint_freq_1_cnt_2_restore_from_1(self):
        """
        Persist more than one checkpoint and restore from the second checkpoint. Also make sure the
        older persisted checkpoint is not removed when the number of persisted checkpoints has not
        exceeded the persist count.
        """
        # This test persists chkpt 0 and 1 to disk. Persist count is 2 so both persisted files
        # are kept. A new dictionary restores from chkpt 1 later.
        NUM_MANAGERS = 1
        persist_freq = 1
        persist_count = 2
        d = DDict(
            NUM_MANAGERS,
            1,
            1500000 * NUM_MANAGERS,
            trace=True,
            working_set_size=2,
            wait_for_keys=True,
            persist_path="",
            persist_freq=persist_freq,
            persist_count=persist_count,
            persister_class=PosixCheckpointPersister,
        )
        d["key"] = "v0"
        d.checkpoint()  ## 1
        d["key"] = "v1"
        d.checkpoint()  ## 2
        d["key"] = "v2"  ## retire and write checkpoint 0 to disk
        d.checkpoint()  ## 3
        # Retire and write checkpoint 1 to disk, persist checkpoint 0
        # is kept as the persist count is 2, so at most 2 persisted chkpt
        # files can exist in the persist path at any time.
        d["key"] = "v3"

        restore_name = d.get_name()
        d.destroy()

        # Check if the persist chkpts are written to disk with the correct filenames.
        path = pathlib.Path(".")
        self.assertEqual(len(list(path.glob(f"{restore_name}*.dat"))), persist_count)
        MIN_PERSISTED_CHKPT_ID = 0
        MAX_PERSISTED_CHKPT_ID = 1
        for managerID in range(NUM_MANAGERS):
            for chkptID in range(MIN_PERSISTED_CHKPT_ID, MAX_PERSISTED_CHKPT_ID + 1, persist_freq):
                pfile = pathlib.Path(f"./{restore_name}_{managerID}_{chkptID}.dat")
                self.assertTrue(pfile.exists())

        chkpt_restore = 1
        dd_restore = DDict(
            NUM_MANAGERS,
            1,
            1500000 * NUM_MANAGERS,
            trace=True,
            persist_path=".",
            name=restore_name,
            restore_from=chkpt_restore,
            read_only=True,
            persister_class=PosixCheckpointPersister,
        )
        # checkpoint ID should be set to the one ddict restored from
        self.assertEqual(dd_restore.checkpoint_id, chkpt_restore)
        self.assertEqual(dd_restore["key"], "v1")
        dd_restore.destroy()

    def test_persist_2_checkpoint_freq_2_cnt_2_restore_from_2(self):
        """
        Make sure checkopints persist with persist frequency > 1 and the persist checkpoints
        are saved.
        """
        # This test persists chkpt 0 and 2 to disk. Persist count is 2 so both persist files
        # are kept. A new dictionary restore from chkpt 2 later.

        NUM_MANAGERS = 1
        working_set_size = 2
        persist_freq = 2
        persist_count = 2
        d = DDict(
            NUM_MANAGERS,
            1,
            1500000 * NUM_MANAGERS,
            trace=True,
            working_set_size=working_set_size,
            wait_for_keys=True,
            persist_path="",
            persist_freq=persist_freq,
            persist_count=persist_count,
            persister_class=PosixCheckpointPersister,
        )
        d["key"] = "v0"
        d.checkpoint()  ## 1
        d["key"] = "v1"
        d.checkpoint()  ## 2
        d["key"] = "v2"  ## retire and write checkpoint 0 to disk
        d.checkpoint()  ## 3
        # retire chkpt 1 but don't write it to disk as we only write every 2
        # retiring chkpts to disk(persist_freq=2), persisted checkpoint 0
        # is still kept in the disk.
        d["key"] = "v3"
        d.checkpoint()  ## 4
        # Retire and write chkpt 2 to disk as persist_freq=2, the last persisted
        # chkpt is 0. Because 0 + 2 = 2, we should persist chkpt 2.
        d["key"] = "v4"

        restore_name = d.get_name()
        d.destroy()

        # Check if the persist chkpts are written to disk with the correct filenames.
        path = pathlib.Path(".")
        self.assertEqual(len(list(path.glob(f"{restore_name}*.dat"))), persist_count)
        MIN_PERSISTED_CHKPT_ID = 0
        MAX_PERSISTED_CHKPT_ID = 2
        for managerID in range(NUM_MANAGERS):
            for chkptID in range(MIN_PERSISTED_CHKPT_ID, MAX_PERSISTED_CHKPT_ID + 1, persist_freq):
                pfile = pathlib.Path(f"./{restore_name}_{managerID}_{chkptID}.dat")
                self.assertTrue(pfile.exists())

        chkpt_restore = 2
        dd_restore = DDict(
            NUM_MANAGERS,
            1,
            1500000 * NUM_MANAGERS,
            trace=True,
            persist_path=".",
            name=restore_name,
            restore_from=chkpt_restore,
            read_only=True,
            persister_class=PosixCheckpointPersister,
        )
        self.assertEqual(dd_restore.persisted_ids(), [0, 2])
        # checkpoint ID should be set to the one ddict restored from
        self.assertEqual(dd_restore.checkpoint_id, chkpt_restore)
        self.assertEqual(dd_restore["key"], "v2")
        dd_restore.destroy()

    def test_persist_3_checkpoint_freq_2_cnt_2_restore_from_4(self):
        """
        Make sure we can persist more than one checkpoint and restore from the latest persisted
        checkpoint with persist frequency > 1. Also make sure the older persisted checkpoint is removed
        when the number of persisted checkpoints exceeds persist count.
        """
        # This test persists chkpt 0, 2, and 4 to disk. Persisted chkpt 0 is removed from disk eventually as
        # the persist count is 2 so we only keep chkpt 2 and 4 and removed the oldest persist chkpt. A new
        # dictionary than restore from chkpt 4 later.
        NUM_MANAGERS = 1
        working_set_size = 2
        persist_freq = 2
        persist_count = 2
        d = DDict(
            NUM_MANAGERS,
            1,
            1500000 * NUM_MANAGERS,
            trace=True,
            working_set_size=working_set_size,
            wait_for_keys=True,
            persist_path="",
            persist_freq=persist_freq,
            persist_count=persist_count,
            persister_class=PosixCheckpointPersister,
        )
        d["key"] = "v0"
        d.checkpoint()  ## 1
        d["key"] = "v1"
        d.checkpoint()  ## 2
        d["key"] = "v2"  ## retire and write checkpoint 0 to disk
        d.checkpoint()  ## 3
        # Retire chkpt 1 but don't write it to disk as we only write every 2
        # retiring chkpt to disk(persist_freq=2), persisted checkpoint 0
        # is still kept in the disk.
        d["key"] = "v3"
        d.checkpoint()  ## 4
        # Retire and write chkpt 2 to disk as persist_freq=2, the last persisted
        # chkpt is 0. Because 0 + 2 = 2, we should persist chkpt 2.
        d["key"] = "v4"
        d.checkpoint()  ## 5
        # Retire chkpt 3 but don't write it to disk as we only write every 2
        # retiring chkpt to disk(persist_freq=2), persisted chkpt 2
        # is still kept in the disk.
        d["key"] = "v5"
        d.checkpoint()  ## 6
        # Retire and write chkpt 4 to disk as persist_freq=2(persisted chkpt 0, 2, 4),
        # persisted chkpt 2 is still kept in the disk but persisted chkpt 0 is removed
        # as the max number of persist files allows at any time is 2 (persist_count=2)
        d["key"] = "v6"

        restore_name = d.get_name()
        d.destroy()

        # Check if the persisted chkpts are written to disk with the correct filenames
        path = pathlib.Path(".")
        self.assertEqual(len(list(path.glob(f"{restore_name}*.dat"))), persist_count)
        MIN_PERSISTED_CHKPT_ID = 2
        MAX_PERSISTED_CHKPT_ID = 4
        for managerID in range(NUM_MANAGERS):
            for chkptID in range(MIN_PERSISTED_CHKPT_ID, MAX_PERSISTED_CHKPT_ID + 1, persist_freq):
                pfile = pathlib.Path(f"./{restore_name}_{managerID}_{chkptID}.dat")
                self.assertTrue(pfile.exists())

        chkpt_restore = 4
        dd_restore = DDict(
            NUM_MANAGERS,
            1,
            1500000 * NUM_MANAGERS,
            trace=True,
            persist_path=".",
            name=restore_name,
            restore_from=chkpt_restore,
            read_only=True,
            persister_class=PosixCheckpointPersister,
        )
        self.assertEqual(dd_restore.persisted_ids(), [2, 4])
        # checkpoint ID should be set to the one ddict restored from
        self.assertEqual(dd_restore.checkpoint_id, chkpt_restore)
        self.assertEqual(dd_restore["key"], "v4")
        dd_restore.destroy()

    def test_persist_restore_from_non_existing_persist_chkpt(self):
        """
        Make sure that the dictionary raise exception when it is brought up with a non-existing
        persisted chkpt to restore from.
        """
        # This test persists chkpt 0 and restores from chkpt 1 later while bringing up dictionary.
        # It should raise exception saying that the persisted chkpt 1 is not available.
        NUM_MANAGERS = 1
        d = DDict(
            NUM_MANAGERS,
            1,
            1500000 * NUM_MANAGERS,
            trace=True,
            working_set_size=2,
            wait_for_keys=True,
            persist_path="",
            persist_freq=1,
            persist_count=1,
            persister_class=PosixCheckpointPersister,
        )
        d["key"] = "v0"
        d.checkpoint()  ## 1
        d["key"] = "v1"
        d.checkpoint()  ## 2
        d["key"] = "v2"  ## retire and write checkpoint 0 to disk

        restore_name = d.get_name()
        d.destroy()

        chkpt_restore = 1
        with self.assertRaises(RuntimeError) as ex:
            dd_restore = DDict(
                NUM_MANAGERS,
                1,
                1500000 * NUM_MANAGERS,
                trace=True,
                persist_path=".",
                name=restore_name,
                restore_from=chkpt_restore,
                read_only=True,
                persister_class=PosixCheckpointPersister,
            )
        self.assertIn(
            "DRAGON_DDICT_PERSIST_CHECKPOINT_UNAVAILABLE",
            str(ex.exception),
            "The expected dragon error code DRAGON_DDICT_PERSIST_CHECKPOINT_UNAVAILABLE is not presented in the exception.",
        )

    def test_persist_read_only_advance(self):
        """
        Restore dictionary and advance to next available persisted chkpt in ready-only mode.
        """
        # This test persists chkpt 0 and 2. Both are kept in disk. A dictionary
        # then restores from chkpt 0 and advances to chkpt 2.
        NUM_MANAGERS = 1
        working_set_size = 2
        persist_freq = 2
        persist_count = 2
        d = DDict(
            NUM_MANAGERS,
            1,
            1500000 * NUM_MANAGERS,
            trace=True,
            working_set_size=working_set_size,
            wait_for_keys=True,
            persist_path="",
            persist_freq=persist_freq,
            persist_count=persist_count,
            persister_class=PosixCheckpointPersister,
        )
        d["key"] = "v0"
        d.checkpoint()  ## 1
        d["key"] = "v1"
        d.checkpoint()  ## 2
        d["key"] = "v2"  ## retire and write checkpoint 0 to disk
        d.checkpoint()  ## 3
        # Retire chkpt 1 but don't write it to disk as we only write every 2
        # retiring chkpts to disk(persist_freq=2), persisted checkpoint 0
        # is still kept in the disk.
        d["key"] = "v3"
        d.checkpoint()  ## 4
        # Retire and write chkpt 2 to disk as persist_freq=2, the last persisted
        # chkpt is 0. Because 0 + 2 = 2, we should persist chkpt 2.
        d["key"] = "v4"

        restore_name = d.get_name()
        d.destroy()

        # Check if the persisted chkpts are written to disk with the correct filenames.
        path = pathlib.Path(".")
        self.assertEqual(len(list(path.glob(f"{restore_name}*.dat"))), persist_count)
        MIN_PERSISTED_CHKPT_ID = 0
        MAX_PERSISTED_CHKPT_ID = 2
        for managerID in range(NUM_MANAGERS):
            for chkptID in range(MIN_PERSISTED_CHKPT_ID, MAX_PERSISTED_CHKPT_ID + 1, persist_freq):
                pfile = pathlib.Path(f"./{restore_name}_{managerID}_{chkptID}.dat")
                self.assertTrue(pfile.exists())

        chkpt_restore = 0
        dd_restore = DDict(
            NUM_MANAGERS,
            1,
            1500000 * NUM_MANAGERS,
            trace=True,
            persist_path=".",
            persist_freq=persist_freq,
            name=restore_name,
            restore_from=chkpt_restore,
            read_only=True,
            persister_class=PosixCheckpointPersister,
        )
        # checkpoint ID should be set to the one ddict restored from
        self.assertEqual(dd_restore.checkpoint_id, chkpt_restore)
        self.assertEqual(dd_restore["key"], "v0")

        # advancing to chkpt 2
        dd_restore.advance()
        self.assertEqual(dd_restore.checkpoint_id, chkpt_restore + persist_freq)
        dd_restore.restore(dd_restore.checkpoint_id)
        self.assertEqual(dd_restore["key"], "v2")
        dd_restore.destroy()

    def test_persist_read_only_advance_to_non_existing_persist_chkpt(self):
        """
        Make sure ddict reports invalid ops err when advancing to a non-existing persisted chkpt.
        """
        # This test persists chkpt 0. A new dictionary than restores from chkpt 0 and tries to advance
        # to chkpt 2, which is not persisted earlier. It should raise persistent chkpt error.
        NUM_MANAGERS = 1
        working_set_size = 2
        persist_freq = 2
        persist_count = 2
        d = DDict(
            NUM_MANAGERS,
            1,
            1500000 * NUM_MANAGERS,
            trace=True,
            working_set_size=working_set_size,
            wait_for_keys=True,
            persist_path="",
            persist_freq=persist_freq,
            persist_count=persist_count,
            persister_class=PosixCheckpointPersister,
        )
        d["key"] = "v0"
        d.checkpoint()  ## 1
        d["key"] = "v1"
        d.checkpoint()  ## 2
        d["key"] = "v2"  ## retire and write checkpoint 0 to disk
        d.checkpoint()  ## 3
        # Retire chkpt 1 but don't write it to disk as we only write every 2
        # retiring chkpt to disk(persist_freq=2), persisted checkpoint 0
        # is still kept in the disk.
        d["key"] = "v3"

        restore_name = d.get_name()
        d.destroy()

        # Check if the persisted chkpts are written to disk with the correct filenames.
        path = pathlib.Path(".")
        self.assertEqual(len(list(path.glob(f"{restore_name}*.dat"))), 1)
        MIN_PERSISTED_CHKPT_ID = 0
        MAX_PERSISTED_CHKPT_ID = 0
        for managerID in range(NUM_MANAGERS):
            for chkptID in range(MIN_PERSISTED_CHKPT_ID, MAX_PERSISTED_CHKPT_ID + 1, persist_freq):
                pfile = pathlib.Path(f"./{restore_name}_{managerID}_{chkptID}.dat")
                self.assertTrue(pfile.exists())

        chkpt_restore = 0
        dd_restore = DDict(
            NUM_MANAGERS,
            1,
            1500000 * NUM_MANAGERS,
            trace=True,
            persist_path=".",
            persist_freq=persist_freq,
            name=restore_name,
            restore_from=chkpt_restore,
            read_only=True,
            persister_class=PosixCheckpointPersister,
        )
        # checkpoint ID should be set to the one ddict restored from
        self.assertEqual(dd_restore.checkpoint_id, chkpt_restore)
        self.assertEqual(dd_restore["key"], "v0")
        with self.assertRaises(DDictPersistCheckpointError) as ex:
            dd_restore.advance()
            self.assertEqual(ex.lib_err, "DRAGON_DDICT_PERSIST_CHECKPOINT_UNAVAILABLE")
        dd_restore.destroy()

    def test_persist_read_only_persist_no_cnt_limit(self):
        """
        Test that when persist_cnt is -1, persist_freq != 0, every chkpt is persisted.
        """
        d = DDict(
            1,
            1,
            1500000,
            trace=True,
            working_set_size=2,
            wait_for_keys=True,
            persist_path="",
            persist_freq=1,
            persist_count=-1,
            persister_class=PosixCheckpointPersister,
        )

        # persist chkpt 0 - 14
        for i in range(17):
            d["key"] = "val" + str(i)
            d.checkpoint()

        name = d.get_name()
        d.destroy()

        # chkpt 0 - 14 are persisted
        # check persisted chkpt files
        managerID = 0
        MIN_PERSISTED_CHKPT_ID = 0
        MAX_PERSISTED_CHKPT_ID = 14
        persist_freq = 1
        for chkptID in range(MIN_PERSISTED_CHKPT_ID, MAX_PERSISTED_CHKPT_ID + 1, persist_freq):
            pfile = pathlib.Path(f"./{name}_{managerID}_{chkptID}.dat")
            self.assertTrue(pfile.exists())

        # restore persisted chkpt
        dd_restore = DDict(
            1,
            1,
            1500000,
            persist_path="",
            persist_freq=1,
            read_only=True,
            name=name,
            restore_from=0,
            persister_class=PosixCheckpointPersister,
        )
        self.assertEqual(dd_restore.persisted_ids(), [i for i in range(MAX_PERSISTED_CHKPT_ID + 1)])
        for i in range(MAX_PERSISTED_CHKPT_ID + 1):
            dd_restore.restore(i)
            self.assertEqual(dd_restore["key"], "val" + str(i))
            if i != MAX_PERSISTED_CHKPT_ID:
                dd_restore.advance()
        dd_restore.destroy()

    def test_persist_no_chkpt_persist_freq_0(self):
        """
        Test that when persist_freq=0, no chkpt is persisted.
        """
        d = DDict(
            1,
            1,
            1500000,
            trace=True,
            working_set_size=2,
            wait_for_keys=True,
            persist_path="",
            persist_freq=0,
            persist_count=-1,
            persister_class=PosixCheckpointPersister,
        )

        for i in range(10):
            d["key"] = "val" + str(i)
            d.checkpoint()

        name = d.get_name()
        d.destroy()

        dat_files = list(pathlib.Path(".").glob(f"{name}_0_*.dat"))
        self.assertEqual(len(dat_files), 0)

    def test_persist_no_chkpt_persist_cnt_0(self):
        """
        Test that when persist_cnt=0, no chkpt is persisted.
        """
        d = DDict(
            1,
            1,
            1500000,
            trace=True,
            working_set_size=2,
            wait_for_keys=True,
            persist_path="",
            persist_freq=1,
            persist_count=0,
            persister_class=PosixCheckpointPersister,
        )

        for i in range(10):
            d["key"] = "val" + str(i)
            d.checkpoint()

        name = d.get_name()
        d.destroy()

        dat_files = list(pathlib.Path(".").glob(f"{name}_0_*.dat"))
        self.assertEqual(len(dat_files), 0)

    ################################################
    # test_persist: non read-only mode
    ################################################

    def test_persist_restore_write_retire(self):
        """Restore from a persisted chkpt and writes, and persist it again."""
        d = DDict(
            1,
            1,
            1500000,
            trace=True,
            working_set_size=2,
            wait_for_keys=True,
            persist_path="",
            persist_freq=1,
            persist_count=-1,
            persister_class=PosixCheckpointPersister,
        )

        # chkpt 0 is persisted
        for i in range(3):
            d["key"] = "val" + str(i)
            d.checkpoint()
        name = d.get_name()
        d.destroy()

        # restore and write chkpt 0, and persist it to disk again.
        d_restore = DDict(
            1,
            1,
            1500000,
            working_set_size=2,
            wait_for_keys=True,
            persist_path="",
            persist_freq=1,
            persist_count=-1,
            restore_from=0,
            name=name,
            trace=True,
            persister_class=PosixCheckpointPersister,
        )
        self.assertEqual(d_restore.persisted_ids(), [0])
        self.assertEqual(d_restore["key"], "val0")
        d_restore["key"] = "valrestore0"
        d_restore["hello"] = "world0"
        d_restore.checkpoint()  ## 1
        d_restore["key"] = "valrestore1"
        d_restore["hello"] = "world1"
        d_restore.checkpoint()  ## 2
        d_restore["key"] = "valrestore2"  # persist chkpt 0 again with updated key value
        d_restore.destroy()

        # verify that updated data are persisted
        d_second_restore = DDict(
            1,
            1,
            1500000,
            persist_path="",
            persist_freq=1,
            restore_from=0,
            name=name,
            persister_class=PosixCheckpointPersister,
        )
        self.assertEqual(d_second_restore["key"], "valrestore0")
        self.assertEqual(d_second_restore["hello"], "world0")

        d_second_restore.destroy()

    def test_persist_restore_write_restore(self):
        # persist chkpt 0, 1, 2
        # restore from chkpt 0
        # checkpoint and write chkpt 1 (new, not persist)
        # restore from chkpt 1
        # the value in chkpt 1 should still be the one written in persisted chkpt
        # as restore doesn't write the new chkpt 1 to disk
        d = DDict(
            1,
            1,
            1500000,
            trace=True,
            working_set_size=2,
            wait_for_keys=True,
            persist_path="",
            persist_freq=1,
            persist_count=-1,
            persister_class=PosixCheckpointPersister,
        )

        # chkpt 0, 1 is persisted
        for i in range(4):
            d["key"] = "val" + str(i)
            d.checkpoint()
        name = d.get_name()
        d.destroy()

        # restore and write chkpt 0, and persist it to disk again.
        d_restore = DDict(
            1,
            1,
            1500000,
            working_set_size=2,
            wait_for_keys=True,
            persist_path="",
            persist_freq=1,
            persist_count=-1,
            restore_from=0,
            name=name,
            trace=True,
            persister_class=PosixCheckpointPersister,
        )
        self.assertEqual(d_restore["key"], "val0")
        d_restore["key"] = "valrestore0"
        d_restore["hello"] = "world0"
        d_restore.checkpoint()  ## 1
        d_restore["key"] = "valrestore1"
        d_restore["hello"] = "world1"
        # restore persisted chkpt 1, the new key written in current working
        # set that hasn't been persisted is discarded
        d_restore.restore(1)
        self.assertEqual(d_restore.checkpoint_id, 1)
        self.assertEqual(d_restore["key"], "val1")
        self.assertTrue("hello" not in d_restore)
        d_restore.restore(0)
        self.assertEqual(d_restore.checkpoint_id, 0)
        self.assertEqual(d_restore["key"], "val0")
        self.assertTrue("hello" not in d_restore)
        d_restore.destroy()

    def test_persist_path(self):
        # test persist with a different persist path
        d = DDict(
            1,
            1,
            1500000,
            trace=True,
            working_set_size=2,
            wait_for_keys=True,
            persist_path="../",
            persist_freq=1,
            persist_count=-1,
            persister_class=PosixCheckpointPersister,
        )

        # chkpt 0, 1, 2 is persisted
        for i in range(5):
            d["key"] = "val" + str(i)
            d.checkpoint()
        restore_name = d.get_name()
        d.destroy()

        # Check if the persist chkpts are written to disk with the correct filenames
        path = pathlib.Path("../")
        self.assertEqual(len(list(path.glob(f"{restore_name}*.dat"))), 3)
        MIN_PERSISTED_CHKPT_ID = 0
        MAX_PERSISTED_CHKPT_ID = 2
        managerID = 0
        for chkptID in range(MIN_PERSISTED_CHKPT_ID, MAX_PERSISTED_CHKPT_ID + 1):
            pfile = pathlib.Path(f"../{restore_name}_{managerID}_{chkptID}.dat")
            self.assertTrue(pfile.exists())
            os.remove(pfile)

    def test_persist_ids(self):
        """
        Query the available persisted chkpt.
        """
        d = DDict(
            1,
            1,
            1500000,
            trace=True,
            working_set_size=2,
            wait_for_keys=True,
            persist_path="",
            persist_freq=1,
            persist_count=-1,
            persister_class=PosixCheckpointPersister,
        )

        # chkpt 0, 1, 2 is persisted
        for i in range(5):
            d["key"] = "val" + str(i)
            d.checkpoint()
        restore_name = d.get_name()
        d.destroy()

        d_restore = DDict(
            1,
            1,
            1500000,
            read_only=True,
            persist_path="",
            persist_freq=1,
            name=restore_name,
            restore_from=0,
            persister_class=PosixCheckpointPersister,
        )
        self.assertEqual(d_restore.persisted_ids(), [i for i in range(3)])
        d_restore.destroy()

    def test_persist_intersection_persisted_ids_between_managers(self):
        """
        Make sure client return the intersection of the persisted chkpt ID among managers.
        """
        d = DDict(
            2,
            1,
            1500000 * 2,
            trace=True,
            working_set_size=2,
            wait_for_keys=True,
            persist_path="",
            persist_freq=1,
            persist_count=3,
            persister_class=PosixCheckpointPersister,
        )

        # Goal:
        # manager 0 persists chkpt 0, 1, 2
        # manager 1 persists chkpt 1, 2, 3

        dm0 = d.manager(0)
        for i in range(5):
            dm0["m0"] = "m0val" + str(i)
            dm0.checkpoint()

        dm1 = d.manager(1)
        for i in range(6):
            dm1["m1"] = "m1val" + str(i)
            dm1.checkpoint()

        restore_name = d.get_name()
        d.destroy()

        # Check if the persisted chkpts are written to disk with the correct filenames
        path = pathlib.Path(".")
        self.assertEqual(len(list(path.glob(f"{restore_name}*.dat"))), 6)
        # Check manager 0 persisted chkpts
        managerID = 0
        MIN_PERSISTED_CHKPT_ID = 0
        MAX_PERSISTED_CHKPT_ID = 2
        for chkptID in range(MIN_PERSISTED_CHKPT_ID, MAX_PERSISTED_CHKPT_ID + 1):
            pfile = pathlib.Path(f"./{restore_name}_{managerID}_{chkptID}.dat")
            self.assertTrue(pfile.exists())
        # Check manager 1 persisted chkpts
        managerID = 1
        MIN_PERSISTED_CHKPT_ID = 1
        MAX_PERSISTED_CHKPT_ID = 3
        for chkptID in range(MIN_PERSISTED_CHKPT_ID, MAX_PERSISTED_CHKPT_ID + 1):
            pfile = pathlib.Path(f"./{restore_name}_{managerID}_{chkptID}.dat")
            self.assertTrue(pfile.exists())

        d_restore = DDict(
            2,
            1,
            1500000 * 2,
            trace=True,
            persist_path="",
            persist_freq=1,
            read_only=True,
            name=restore_name,
            restore_from=1,
            persister_class=PosixCheckpointPersister,
        )
        self.assertEqual(d_restore.persisted_ids(), [1, 2])
        d_restore.destroy()

    def test_persist_current_checkpoint(self):
        d = DDict(
            2,
            1,
            1500000 * 2,
            trace=True,
            working_set_size=4,
            wait_for_keys=True,
            persist_path="",
            persist_freq=1,
            persist_count=-1,
            persister_class=PosixCheckpointPersister,
        )

        d["hello"] = "world0"
        d.checkpoint()  ## 1
        d["hello"] = "world1"
        d.persist()  ## persist chkpt 1

        restore_name = d.get_name()

        d.destroy()

        d_restore = DDict(
            2,
            1,
            1500000 * 2,
            trace=True,
            persist_path="",
            persist_freq=1,
            read_only=True,
            name=restore_name,
            restore_from=1,
            persister_class=PosixCheckpointPersister,
        )
        self.assertEqual(d_restore.persisted_ids(), [1])
        self.assertEqual(d_restore["hello"], "world1")
        d_restore.destroy()

    ################################################
    # test_persist: client input args check
    ################################################

    def test_persist_invalid_arg_read_only_name(self):
        """
        Test that ddict raise attr error when ddict name is not provided in read-only mode.
        """
        with self.assertRaises(AttributeError) as ex:
            d = DDict(
                1,
                1,
                1500000,
                trace=True,
                persist_path="",
                read_only=True,
                restore_from=1,
                persister_class=PosixCheckpointPersister,
            )
        self.assertEqual(
            "When creating a read-only Dragon Distributed Dict you must provide the name to restore the dictionary.",
            str(ex.exception),
            "Failed to raise attribute error with expected details.",
        )

    def test_persist_invalid_arg_read_only_restore_from(self):
        """
        Test that ddict raise attr error when restore_from is not provided in read-only mode.
        """
        # ready-only is True only when restore from is specified
        with self.assertRaises(AttributeError) as ex:
            d = DDict(
                1,
                1,
                1500000,
                trace=True,
                persist_path="",
                read_only=True,
                name="hello",
                persister_class=PosixCheckpointPersister,
            )
        self.assertEqual(
            "When creating a read-only Dragon Distributed Dict you must provide the checkpoint ID to restore from.",
            str(ex.exception),
            "Failed to raise attribute error with expected details.",
        )

    def test_persist_invalid_arg_read_only_wait_for_keys_writers(self):
        """
        Test that ddict raise attr error when user specifies wait for keys or wait for writers in read-only mode.
        """
        with self.assertRaises(AttributeError) as ex:
            d = DDict(
                1,
                1,
                1500000,
                trace=True,
                working_set_size=2,
                wait_for_keys=True,
                persist_path="",
                name="hello",
                read_only=True,
                restore_from=0,
                persister_class=PosixCheckpointPersister,
            )
        self.assertEqual(
            "When creating a read-only Dragon Distributed Dict, specifying wait_for_keys or wait_for_writers is invalid.",
            str(ex.exception),
            "Failed to raise attribute error with expected details.",
        )

        with self.assertRaises(AttributeError) as ex:
            d = DDict(
                1,
                1,
                1500000,
                trace=True,
                working_set_size=2,
                wait_for_writers=True,
                persist_path="",
                name="hello",
                read_only=True,
                restore_from=0,
                persister_class=PosixCheckpointPersister,
            )
        self.assertEqual(
            "When creating a read-only Dragon Distributed Dict, specifying wait_for_keys or wait_for_writers is invalid.",
            str(ex.exception),
            "Failed to raise attribute error with expected details.",
        )

    def test_persist_invalid_arg_read_only_working_set_size(self):
        """
        Test that ddict raise attr error when user specifies invalid working set size in read-only mode.
        """
        # In read-only mode, working set size should be 1
        with self.assertRaises(AttributeError) as ex:
            d = DDict(
                1,
                1,
                1500000,
                trace=True,
                working_set_size=2,
                persist_path="",
                name="hello",
                read_only=True,
                restore_from=0,
                persister_class=PosixCheckpointPersister,
            )
        self.assertEqual(
            "When creating a read-only Dragon Distributed Dict, working set size should be 1.",
            str(ex.exception),
            "Failed to raise attribute error with expected details.",
        )

    def test_persist_invalid_arg_read_only_persist_cnt(self):
        """
        Test that ddict raise attr error when user specifies persist count in read-only mode.
        """
        with self.assertRaises(AttributeError) as ex:
            d = DDict(
                1,
                1,
                1500000,
                trace=True,
                name="hello",
                restore_from=0,
                read_only=True,
                persist_count=2,
                persister_class=PosixCheckpointPersister,
            )
        self.assertEqual(
            "When creating a read-only Dragon Distributed Dict, specifying a non-zero persist count is invalid.",
            str(ex.exception),
            "Failed to raise attribute error with expected details.",
        )

    def test_persist_invalid_persist_freq(self):
        """
        Test that ddict raise attr error when persist frequency is negative.
        """
        # persist_freq should be >= 0
        with self.assertRaises(ValueError) as ex:
            d = DDict(
                1,
                1,
                1500000,
                trace=True,
                working_set_size=2,
                wait_for_keys=True,
                persist_freq=-1,
                name="hello",
                persister_class=PosixCheckpointPersister,
            )
        self.assertEqual(
            "Persist frequency should be non-negative.",
            str(ex.exception),
            "Failed to raise attribute error with expected details.",
        )

    def test_persist_invalid_persist_cnt(self):
        """
        Test that ddict raise attr error when persist frequency is <= -1.
        """
        # persist_cnt should be >= -1
        with self.assertRaises(ValueError) as ex:
            d = DDict(
                1,
                1,
                1500000,
                trace=True,
                working_set_size=2,
                wait_for_keys=True,
                persist_count=-2,
                name="hello",
                persister_class=PosixCheckpointPersister,
            )
        self.assertEqual(
            "Persist count should be greater or equal to -1.",
            str(ex.exception),
            "Failed to raise attribute error with expected details.",
        )

    def test_persist_invalid_arg_restore_from_without_name(self):
        """
        Test that DDict raise attr error when restore from is specified but the name is not provided.
        """
        with self.assertRaises(AttributeError) as ex:
            d = DDict(
                1,
                1,
                1500000,
                trace=True,
                restore_from=0,
                persister_class=PosixCheckpointPersister,
            )
        self.assertEqual(
            "When restoring from a checkpoint in Dragon Distributed Dict you must provide the name to restore the dictionary.",
            str(ex.exception),
            "Failed to raise attribute error with expected details.",
        )

    def test_persist_invalid_arg_null_persister_non_zero_persist_freq(self):
        with self.assertRaises(AttributeError) as ex:
            d = DDict(
                1,
                1,
                1500000,
                trace=True,
                restore_from=0,
                persist_freq=1,
            )
        self.assertEqual(
            "When using NULLCheckpointPersister, specifying a non-zero persist_freq is invalid.",
            str(ex.exception),
            "Failed to raise attribute error with expected details.",
        )

    ################################################
    # test_persist: invalid operation with read only
    ################################################

    def test_persist_invalid_op_restore_during_batch(self):
        """
        Restoring checkpoint during batch put is invalid and DDict should raise exception.
        """
        d = DDict(1, 1, 1500000, working_set_size=2, wait_for_keys=True)
        d.start_batch_put()
        with self.assertRaises(DDictError) as ex:
            d.restore(0)
        self.assertIn(
            "DRAGON_INVALID_OPERATION", str(ex.exception), "Failed to raise DDictError with expected details."
        )
        d.destroy()

    def test_persist_invalid_op_write_when_read_only(self):
        """
        Any writes, deletes, clears op is not valid in read-only mode.
        """
        # In read-only mode, calling put, batch put, pop, clear is invalid
        d = DDict(
            1,
            1,
            1500000,
            trace=True,
            working_set_size=2,
            wait_for_keys=True,
            persist_path="",
            persist_freq=1,
            persist_count=1,
            persister_class=PosixCheckpointPersister,
        )

        d["hello"] = "world0"
        d.checkpoint()  ## 1
        d["hello"] = "world1"
        d.checkpoint()  ## 2
        d["hello"] = "world2"  # persist chkpt 0 to disk
        name = d.get_name()
        d.destroy()

        d_restore = DDict(
            1,
            1,
            1500000,
            trace=True,
            read_only=True,
            name=name,
            restore_from=0,
            persister_class=PosixCheckpointPersister,
        )
        # put
        with self.assertRaises(DDictError) as ex:
            d_restore["hello"] = "world"
        self.assertIn("DRAGON_INVALID_OPERATION", str(ex.exception), "Failed to raise exception with expected details.")
        # pput
        with self.assertRaises(DDictError) as ex:
            d_restore.pput("hello", "world")
        self.assertIn("DRAGON_INVALID_OPERATION", str(ex.exception), "Failed to raise exception with expected details.")
        # bcast put
        with self.assertRaises(DDictError) as ex:
            d_restore.bput("hello", "world")
        self.assertIn("DRAGON_INVALID_OPERATION", str(ex.exception), "Failed to raise exception with expected details.")
        # pop
        with self.assertRaises(DDictError) as ex:
            d_restore.pop("hello")
        self.assertIn("DRAGON_INVALID_OPERATION", str(ex.exception), "Failed to raise exception with expected details.")
        # clear
        with self.assertRaises(DDictError) as ex:
            d_restore.clear()
        self.assertIn("DRAGON_INVALID_OPERATION", str(ex.exception), "Failed to raise exception with expected details.")

        d_restore.destroy()

    def test_persist_invalid_op_advance_without_readonly(self):
        # Calling advance under non read-only mode is invalid.
        d = DDict(
            1,
            1,
            1500000,
            trace=True,
            working_set_size=2,
            wait_for_keys=True,
            persist_path="",
            persist_freq=1,
            persist_count=1,
            persister_class=PosixCheckpointPersister,
        )
        with self.assertRaises(DDictError) as ex:
            d.advance()
        self.assertIn("DRAGON_INVALID_OPERATION", str(ex.exception), "Failed to raise exception with expected details.")
        d.destroy()

class TestTooBig(unittest.TestCase):
    @unittest.skip("Don't need to test every time.")
    def test_oom(self):
        """Make sure we see OOM warnings and handle taking down run-time when critical"""
        # You can set up to run this with a script like this.
        # This will terminate after memory reaches 30% full.
        # Bytes are not considered since they are set to 0.
        #!/bin/bash
        # export DRAGON_MEMORY_WARNING_PCT="80"
        # export DRAGON_MEMORY_WARNING_BYTES="0"
        # export DRAGON_MEMORY_CRITICAL_PCT="70"
        # export DRAGON_MEMORY_CRITICAL_BYTES="0"
        # and optionally
        # export DRAGON_QUIET="True"

        # dragon test_ddict.py -v -f TestTooBig.test_oom

        dlist = []
        print(f"Client on host {socket.gethostname()}")
        for i in range(100):
            dd = DDict(20, 2, 8 * 1024 * 1024 * 1024)
            dlist.append(dd)

            print("Mem Percent", psutil.virtual_memory().percent, flush=True)
            print("Mem Free", psutil.virtual_memory().free, flush=True)

        time.sleep(10)

        for dd in dlist:
            dd.destroy()

    def test_too_big(self):
        """Make sure we handle and respond to too big an allocation and raise an exception"""

        print(f"Client on host {socket.gethostname()}")
        d = DDict(1, 1, 4096)
        x = np.ones(4096, dtype=np.int32)

        with self.assertRaises(DDictManagerFull):
            d["np array"] = x

        d.destroy()

    def test_too_big2(self):
        """Make sure we handle and respond to too big an allocation and raise an exception"""

        print(f"Client on host {socket.gethostname()}", flush=True)
        d = DDict(1, 1, 4 * 1024 * 1024)
        x = np.ones(4 * 1024 * 1024, dtype=np.int32)

        with self.assertRaises(DDictManagerFull):
            d["np array"] = x

        d.destroy()

    def test_too_big3(self):
        """Make sure we handle and respond to too big an allocation and raise an exception"""

        d = DDict(1, 1, 4 * 1024 * 1024, trace=True)

        proc = mp.Process(target=client_too_big3, args=(d,))
        proc.start()
        proc.join()

        self.assertEqual(proc.exitcode, 0, f"The client function returned a return code of {proc.exitcode}")

        x = np.ones(4 * 1024 * 1024, dtype=np.int32)

        with self.assertRaises(DDictManagerFull):
            d["np array"] = x

        d.destroy()


if __name__ == "__main__":
    mp.set_start_method("dragon")
    unittest.main()
