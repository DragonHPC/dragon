#!/usr/bin/env python3

import unittest
import traceback
import multiprocessing as mp
import cloudpickle
import os
import socket
import sys

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
from dragon.globalservices import api_setup
from dragon.infrastructure.policy import Policy
from dragon.utils import b64encode, b64decode, hash as dhash, host_id
from dragon.data.ddict import DDict, DDictManagerFull, DDictError, strip_pickled_bytes
from dragon.rc import DragonError
from dragon.native.machine import System, Node
import multiprocessing as mp
from multiprocessing import Barrier, Pool, Queue, Event, set_start_method

NUM_BUCKETS = 4
POOL_MUID = 897

def fillit(d):
    i = 0
    key = "abc"
    while True:
        d[key] = key
        i += 1
        key += "abc"*i

def client_func_1_get_newest_chkpt_id(d):
    ## 0
    d['abc'] = 'def'
    d.checkpoint() ## 1
    d['hello'] = 'world'
    d.checkpoint() ## 2
    d['dragon'] = 'runtime'
    d.checkpoint() ## 3
    d['Miska'] = 'dog'
    d.checkpoint() ## 4
    d[123] = 456
    d.detach()

def client_func_1_wait_keys_clear(d, ev):
    ## 0
    d.pput('hello0', 'world0')
    d.checkpoint() ## 1
    d.pput('hello0_1', 'world1')
    d.checkpoint() ## 2
    d.pput('hello0_2', 'world2')
    d.checkpoint() ## 3
    d.pput('hello0_3', 'world3')
    d['Miska'] = 'dog'
    d.detach()
    ev.set()

def client_func_2_wait_keys_clear(d, ev):
    ## 0
    d.checkpoint() ## 1
    d.checkpoint() ## 2
    del d['hello0_1']
    # the deleted set is not cleared - should report key not found for the deleted persistent key
    try:
        x = d['hello0_1']
        raise Exception(f'Expected DDictError DRAGON_KEY_NOT_FOUND is not raised')
    except DDictError as ex:
        assert ex.lib_err == 'DRAGON_KEY_NOT_FOUND'
    d.clear()
    d.checkpoint() ## 3
    ev.set()
    assert d['hello0_1'] == 'world1_client3' # hang - hello1 was clear in chkpt 2, so it should hang until the key is written into dict
    assert 'hello0' not in d
    assert 'hello0_2' not in d
    assert 'Miska' in d
    d.detach()

def client_func_3_wait_keys_clear(d):
    ## 0
    d.checkpoint() ## 1
    d.checkpoint() ## 2
    d.checkpoint() ## 3
    d.pput('hello0_1', 'world1_client3') # flush the get request in client2
    d.detach()

def client_func_1_nonpersist(d, ev):
    d['non_persist_key0'] = 'non_persist_val0_ckp1'
    ev.set()
    d.detach()

def client_func_2_nonpersist(d, ev):
    ev.set()
    assert d['non_persist_key0'] == 'non_persist_val0_ckp1'
    d.detach()

def client_func_1_persist(d, ev):
    d.pput('persist_key0', 'persist_val0')
    ev.set()
    d.detach()

def client_func_2_persist(d, ev):
    ev.set()
    assert d['persist_key0'] == 'persist_val0'
    d.detach()

def client_func_1_wait_keys_write_retired(d, ev):
    ## 0
    d.pput('persist_key0', 'persist_val0')
    d.checkpoint() ## 1
    d.pput('persist_key1', 'persist_val1')
    d.checkpoint() ## 2
    d.pput('persist_key2', 'persist_val2')
    d.detach()
    ev.set()

def client_func_2_wait_keys_write_retired(d):
    ## 0
    try:
        d.pput('persist_key0', 'persist_val0')
        raise Exception(f'Expected DDictError DRAGON_DDICT_CHECKPOINT_RETIRED is not raised')
    except DDictError as ex:
        assert ex.lib_err == 'DRAGON_DDICT_CHECKPOINT_RETIRED'

    d.detach()

def client_func_1_wait_keys_read_retired(d, ev):
    ## 0
    d.pput('persist_key0', 'persist_val0')
    d.checkpoint() ## 1
    d.pput('persist_key1', 'persist_val1')
    d.checkpoint() ## 2
    d.pput('persist_key2', 'persist_val2')
    d.detach()
    ev.set()

def client_func_2_wait_keys_read_retired(d):
    ## 0
    try:
        x = d['persist_key0']
        raise Exception(f'Expected DDictError DRAGON_DDICT_CHECKPOINT_RETIRED is not raised')
    except DDictError as ex:
        assert ex.lib_err == 'DRAGON_DDICT_CHECKPOINT_RETIRED'

    d.detach()

def client_func_1_wait_keys_defer_put_get_same_key(d, ev):
    ## 0
    d.pput('persist_key0', 'persist_val0')
    d['nonpersist_key0'] = 'nonpersist_val0'
    d.checkpoint() ## 1
    d.pput('persist_key0', 'persist_val0_ckpt1')
    ev.set()
    d.checkpoint() ## 2
    d['nonpersist_key0'] = 'nonpersist_val0_ckpt2' # deferred 1
    d.detach()

def client_func_2_wait_keys_defer_put_get_same_key(d, ev):
    ## 0
    d.checkpoint() ## 1
    ev.set()
    d.checkpoint() ## 2
    x = d['nonpersist_key0'] # deferred 2
    assert x == 'nonpersist_val0_ckpt2'
    assert d['nonpersist_key0'] == 'nonpersist_val0_ckpt2_client3'
    d.detach()

def client_func_3_wait_keys_defer_put_get_same_key(d):
    ## 0
    d.checkpoint() ## 1
    d['nonpersist_key0'] = 'nonpersist_val0_ckpt1'
    d.checkpoint() ## 2
    d['nonpersist_key0'] = 'nonpersist_val0_ckpt2_client3' # flush
    d.detach()

def client_func_1_wait_keys_contains(d, ev):
    ## 0
    d['nonpersist_key0'] = 'nonpersist_val0'
    d.pput('persist_key0', 'persist_val0')
    d.checkpoint() ## 1
    d['nonpersist_key0'] = 'nonpersist_val0_chkpt1'
    d.detach()
    ev.set()

def client_func_2_wait_keys_contains(d):
    ## 0
    assert 'nonpersist_key0' in d
    d.pput('persist_key0_1', 'persist_val1')
    d.checkpoint() ## 1
    assert 'persist_key0' in d
    assert 'nonpersist_key0' in d
    del d['persist_key0_1']
    assert 'persist_key0_1' not in d
    d.checkpoint() ## 2
    assert 'persist_key0' in d
    assert 'nonpersist_key0' not in d
    d['nonpersist_key0'] = 'nonpersist_val0_chkpt2a'
    d.checkpoint() ## 3
    assert 'persist_key0_1' not in d
    assert 'nonpersist_key0' not in d
    d.detach()

def client_func_1_wait_keys_pop(d):
    ## 0
    d.checkpoint() ## 1
    d['key0'] = 'nonpersist_val0'
    assert d['key0'] == 'nonpersist_val0'
    d.checkpoint() ## 2
    d['key0'] = 'nonpersist_val0_chkpt2'
    assert d['key0'] == 'nonpersist_val0_chkpt2'
    d.detach()

def client_func_2_wait_keys_pop(d, ev):
    ## 0
    d.pput('key0', 'persist_val0')
    d.pop('key0')
    d.detach()
    ev.set()

def client_func_1_wait_keys_pop_nonpersist(d):
    ## 0
    d['nonpersist_key0'] = 'nonpersist_val0'
    d.checkpoint() ## 1
    assert d.pop('nonpersist_key0') == 'nonpersist_val0_chkpt1'
    d.checkpoint() ## 2
    d['nonpersist_key0'] = 'nonpersist_val0_chkpt2'
    d.detach()

def client_func_2_wait_keys_pop_nonpersist(d):
    ## 0
    assert d.pop('nonpersist_key0') == 'nonpersist_val0'
    d.checkpoint() ## 1
    d['nonpersist_key0'] = 'nonpersist_val0_chkpt1'
    d.checkpoint() ## 2
    assert d.pop('nonpersist_key0') == 'nonpersist_val0_chkpt2'
    d.detach()

def client_func_1_wait_keys_pop_persist(d):
    # ## 0
    d.checkpoint() ## 1
    d.checkpoint() ## 2
    assert 'persist_key0' not in d
    d.detach()

def client_func_2_wait_keys_pop_persist(d):
    # ## 0
    d.pput('persist_key0', 'persist_val0')
    assert d.pop('persist_key0') == 'persist_val0'
    assert 'persist_key0' not in d
    d.checkpoint() ## 1
    d.checkpoint() ## 2
    # Write a value into checkpoint 2 of the
    # same manager.
    # TBD: VERIFY IT IS THE SAME MANAGER.
    d[2] = 'hello'
    d.detach()

def client_func_1_wait_keys_length(d):
    ## 0
    assert len(d) == 1
    d.checkpoint() ## 1
    d.pop('key0')
    assert len(d) == 2
    d.checkpoint() ## 2
    assert len(d) == 1
    d['nonpersist_key1'] = 'nonpersist_val1_chkpt2'
    assert len(d) == 2
    d.checkpoint() ## 3
    assert len(d) == 1
    d['nonpersist_key3'] = 'nonpersist_val3'
    assert len(d) == 2
    d['key0'] = 'val0_chkpt3' # add persistent key back as a non-persist key
    assert len(d) == 3
    d['key3'] = 'val3' # other manager
    assert len(d) == 4
    d.detach()

def client_func_2_wait_keys_length(d, ev):
    ## 0
    d.pput('key0', 'persist_val0')
    d.checkpoint() ## 1
    d.pput('persist_key1', 'persist_val1')
    d['nonpersist_key1'] = 'nonpersist_val1'
    d.detach()
    ev.set()

def client_func_1_wait_keys_keys(d, ev):
    ## 0
    d.pput('key0', 'val0')
    d.detach()
    ev.set()

def client_func_2_wait_keys_keys(d):
    ## 0
    d['key1_0'] = 'val1_0_nonpersist'

    d.checkpoint() ## 1
    key_list_chkpt1 = d.keys()
    assert len(key_list_chkpt1) == 1
    assert key_list_chkpt1[0] == 'key0'
    d['key1_nonpersist'] = 'val1_nonpersist'
    key_list_chkpt1 = d.keys()
    assert len(key_list_chkpt1) == 2
    assert 'key1_nonpersist' in key_list_chkpt1
    assert 'key0' in key_list_chkpt1
    del d['key0']
    d['key1_0'] = 'val1_0_nonpersist_chkpt1'

    d.checkpoint() ## 2
    key_list_chkpt2 = d.keys() # keys request to a checkpoint that hasn't exist
    assert len(key_list_chkpt2) == 0
    d.pput('key0', 'val1') # retire chkpt 0
    d['key1_0'] = 'val1_0_nonpersist_chkpt2'
    d['key1_nonpersist'] = 'val1_nonpersist_chkpt2'

    d.checkpoint() ## 3
    key_list_chkpt3 = d.keys() # check that the future checkpoint can get read the existing checkpoint and only return persistent keys
    assert len(key_list_chkpt3) == 1
    assert 'key0' in key_list_chkpt3

    d.pput('key2', 'val2') # retire chkpt 1
    key_list_chkpt3 = d.keys() # check that the deleted set is copied correctly from retired checkpoint
    assert len(key_list_chkpt3) == 2
    assert 'key0' in key_list_chkpt3
    assert 'key2' in key_list_chkpt3
    d.detach()

def client_func_1_wait_writers_persist(d, ev):
    ## 0
    d['persist_key0'] = 'persist_val0'
    d.checkpoint() ## 1
    d['persist_key0'] = 'persist_val1'
    d.detach()
    ev.set()

def client_func_2_wait_writers_persist(d):
    ## 0
    assert d['persist_key0'] == 'persist_val0'
    d.checkpoint() ## 1
    assert d['persist_key0'] == 'persist_val1'
    d.detach()

def client_func_2_wait_writers_persist_err(d, ev):
    try:
        assert d['persist_key0'] == 'persist_val0'
        raise Exception(f'Expected DDictError DRAGON_KEY_NOT_FOUND is not raised')
    except DDictError as ex:
        assert ex.lib_err == 'DRAGON_KEY_NOT_FOUND'
    d.detach()
    ev.set()

def client_func_1_wait_writers_read_future_chkpt(d, ev):
    ## 0
    d.checkpoint() ## 1
    d['key1'] = 'val1'
    d.detach()
    ev.set()

def client_func_2_wait_writers_read_future_chkpt(d):
    ## 0
    d.checkpoint() ## 1
    d.checkpoint() ## 2
    assert d['key1'] == 'val1'
    d.checkpoint() ## 3
    assert d['key1'] == 'val1' # should be able to get the key even the working set is not advanced
    d.detach()

def client_func_1_wait_writers_write_retired(d, ev):
    ## 0
    d['key0'] = 'val0'
    d.checkpoint() ## 1
    d['key0'] = 'val0_chkpt1'
    d.checkpoint() ## 2
    d['key0'] = 'val0_chkpt2'
    d.detach()
    ev.set()

def client_func_2_wait_writers_write_retired(d):
    ## 0
    try:
        d['key0'] = 'val0_client2'
        raise Exception(f'Expected DDictError DRAGON_DDICT_CHECKPOINT_RETIRED is not raised')
    except DDictError as ex:
        assert ex.lib_err == 'DRAGON_DDICT_CHECKPOINT_RETIRED'
    d.detach()

def client_func_2_wait_writers_read_retired(d):
    ## 0
    try:
        x = d['key0']
        raise Exception(f'Expected DDictError DRAGON_DDICT_CHECKPOINT_RETIRED is not raised')
    except DDictError as ex:
        assert ex.lib_err == 'DRAGON_DDICT_CHECKPOINT_RETIRED'
    d.detach()

def client_func_1_wait_writers_contains(d, ev):
    ## 0
    d['key0'] = 'val0'
    d.detach()
    ev.set()

def barrier_waiter(b,):
    b.wait()

def client_func_2_wait_writers_contains(d):
    ## 0
    assert 'key0' in d
    d.checkpoint() ## 1
    assert 'key0' in d
    assert 'key1' not in d
    d.detach()

def client_func_1_wait_writers_pop(d, ev):
    ## 0
    d['key0'] = 'val0'
    d['key1'] = 'val1'
    d.checkpoint() ## 1
    d['key2'] = 'val2'
    d.detach()
    ev.set()

def client_func_2_wait_writers_pop(d):
    ## 0
    assert 'key0' in d
    del d['key0']
    assert 'key0' not in d
    try:
        d.pop('key0')
        raise Exception(f'Expected DDictError DRAGON_KEY_NOT_FOUND is not raised')
    except DDictError as ex:
        assert ex.lib_err == 'DRAGON_KEY_NOT_FOUND'
    d.checkpoint() ## 1
    assert 'key1' in d
    assert d.pop('key1') == 'val1'
    assert 'key1' not in d
    d.detach()

def client_func_1_wait_writers_len(d, ev):
    ## 0
    d['key0'] = 'val0'
    d.checkpoint() ## 1
    d['key0_1'] = 'val0_1'
    d.detach()
    ev.set()

def client_func_2_wait_writers_len(d):
    ## 0
    assert len(d) == 1
    d.checkpoint() ## 1
    assert len(d) == 2
    d.checkpoint() ## 2
    assert len(d) == 2
    d['key0_1_1'] = 'val0_1_1'
    assert len(d) == 3
    d['key0_2'] = 'val0_2' # on different manager
    assert len(d) == 4
    del d['key0']
    assert len(d) == 3
    del d['key0_1']
    assert len(d) == 2

    d.detach()

def client_func_1_wait_writers_keys(d, ev):
    ## 0
    d['persistent_key1_0'] = 'persistent_val1'
    d.checkpoint() ## 1
    d['persistent_key2'] = 'persistent_val2'
    d.detach()
    ev.set()

def client_func_2_wait_writers_keys(d):
    ## 0
    key_list_chkpt0 = d.keys()
    assert len(key_list_chkpt0) == 1
    assert 'persistent_key1_0' in key_list_chkpt0
    del d['persistent_key1_0']
    d['persistent_key1_1'] = 'persistent_val1_1'

    d.checkpoint() ## 1
    key_list_chkpt1 = d.keys()
    assert len(key_list_chkpt1) == 2
    assert 'persistent_key1_1' in key_list_chkpt1
    assert 'persistent_key2' in key_list_chkpt1
    del d['persistent_key1_1']

    d.checkpoint() ## 2
    key_list_chkpt1 = d.keys()
    assert len(key_list_chkpt1) == 1
    assert 'persistent_key2' in key_list_chkpt1

    d.detach()

def remote_proc(resp_fli_ser, queue):
    try:
        resp_fli = fli.FLInterface.attach(b64decode(resp_fli_ser))

        main_channel = dch.Channel.make_process_local()
        strm_channel = dch.Channel.make_process_local()

        the_fli = fli.FLInterface(main_ch=main_channel)

        queue.put(b64encode(the_fli.serialize()))

        with the_fli.sendh(stream_channel=strm_channel) as sendh:
            b = socket.gethostname().encode('utf-8')
            sendh.send_bytes(b)

        with resp_fli.recvh(use_main_as_stream_channel=True) as recvh:
            b = recvh.recv_bytes()

        del(the_fli)

        return 0
    except Exception as ex:
        print(f'There was an exception on the remote_proc: {ex}', file=sys.stder, flush=True)


def client_func_1_wait_writers_rollback(d, ev):
    ## 0
    d['key0'] = 'val0'
    d.checkpoint() ## 1
    assert d.current_checkpoint_id == 1
    d['key0'] = 'val1'
    d.rollback() ## 0
    assert d.current_checkpoint_id == 0
    assert d['key0'] == 'val0'
    d.checkpoint() ## 1
    d.checkpoint() ## 2
    d['key0'] = 'val2'
    d.detach()
    ev.set()

def client_func_2_wait_writers_rollback(d):
    ## 0
    d.checkpoint() ## 1
    assert d.current_checkpoint_id == 1
    assert d['key0'] == 'val1'
    d.checkpoint() ## 2
    assert d.current_checkpoint_id == 2
    assert d['key0'] == 'val2'
    d.rollback() ## 1
    d.rollback() ## 0
    try:
        assert d['key0'] == 'val0'
        raise Exception(f'Expected DDictError DRAGON_DDICT_CHECKPOINT_RETIRED is not raised')
    except DDictError as ex:
        assert ex.lib_err == 'DRAGON_DDICT_CHECKPOINT_RETIRED'
    d.detach()

def client_too_big3(d):
    try:
        print(f'Client on host {socket.gethostname()}')
        x = np.ones(4*1024*1024, dtype=np.int32)
        d['np array'] = x
        return 1
    except DDictManagerFull:
        return 0
    except Exception:
        return 1



class TestDDict(unittest.TestCase):
    def setUp(self):
        pool_name = f"pydragon_ddict_test_{os.getpid()}"
        pool_size = 1073741824  # 1GB
        pool_uid = POOL_MUID
        self.mpool = MemoryPool(pool_size, pool_name, pool_uid)

    def tearDown(self):
        try:
            self.mpool.destroy()
        except:
            pass

    def test_local_channel(self):
        ch = dch.Channel.make_process_local()
        ch.detach()

    def test_hash(self):
        try:
            here = pathlib.Path(__file__).parent.resolve()
        except:
            here = pathlib.Path(__file__).resolve()

        buckets = list(0 for _ in range(NUM_BUCKETS))
        file = open(os.path.join(here, 'filenames.txt'), 'r')
        for filename in file:
            lst = filename.split('.')
            if len(lst) > 2:
                key = lst[0] + lst[2][2:]
                pickled_key = strip_pickled_bytes(cloudpickle.dumps(key))
                #print(f'{key=} and {pickled_key=}')
                idx = dhash(pickled_key) % NUM_BUCKETS
                buckets[idx] += 1

        file.close()

        # print(f'\n{buckets=}')
        total = sum(buckets)
        avg_per_bucket = total / NUM_BUCKETS
        tolerance = avg_per_bucket * 0.10 # 10 percent tolerance

        for amt in buckets:
            #print(f'{amt=} and {avg_per_bucket=}')
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
        file = open(os.path.join(here, 'filenames.txt'), 'r')
        count = 0
        d = DDict(4, 1, 30000000)
        for filename in file:
            root = filename.split('.')[0]
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
        newser = 'eJyrVoovSVayUjA21lFQKklMBzItawE+xQWS'
        from_str = dmsg.parse(newser)
        self.assertIsInstance(from_str, dmsg.GSHalted)
        newser = 'eJyrVoovSVayUjA21lFQKklMBzItawE+xQWS\n'
        from_str = dmsg.parse(newser)
        self.assertIsInstance(from_str, dmsg.GSHalted)
        newline = b'\n\n\n\n'
        encoded = b64encode(newline)
        decoded = b64decode(encoded)
        self.assertEqual(newline, decoded)
        newline = '\n\n\n\n'
        encoded = b64encode(newline.encode('utf-8'))
        decoded = b64decode(encoded)
        self.assertEqual(newline, decoded.decode('utf-8'))

    def test_capnp_message (self):
        msg = dmsg.DDRegisterClient(42, "HelloWorld", "MiskaIsAdorable")
        ser = msg.serialize()

        newmsg = dmsg.parse(ser)
        self.assertIsInstance(newmsg, dmsg.DDRegisterClient)

    def test_ddict_client_response_message(self):
        manager_nodes = b64encode(cloudpickle.dumps([Node(ident=host_id()) for _ in range(2)]))
        msg = dmsg.DDRegisterClientResponse(42, 43, DragonError.SUCCESS, 0, 2, 3, manager_nodes, 10, 'this is dragon error info')
        ser = msg.serialize()
        newmsg = dmsg.parse(ser)
        self.assertIsInstance(newmsg, dmsg.DDRegisterClientResponse)

    def test_bringup_teardown(self):
        d = DDict(2, 1, 3000000)
        d.destroy()

    def test_add_delete(self):
        d = DDict(2, 1, 3000000, trace=True)

        at_start = d.stats

        i = 0
        key = "abc"
        while i < 10:
            d[key] = key
            i += 1
            key += "abc"*i


        i = 0
        key = "abc"
        while i < 10:
            del d[key]
            i += 1
            key += "abc"*i

        at_end =  d.stats

        self.assertEqual(at_start[0], at_end[0])
        self.assertEqual(at_start[1], at_end[1])

        d.destroy()

    def test_detach_client(self):
        d = DDict(2, 1, 3000000)
        d.detach()
        d.destroy()

    def test_put_and_get(self):
        d = DDict(2, 1, 3000000, trace=True)
        def_pool = dmem.MemoryPool.attach_default()
        q = Queue()
        b = Barrier(parties=2)
        before_free_space = def_pool.free_space
        d['abc'] = 'def'
        x = d['abc']
        self.assertEqual(d['abc'], 'def')

        d[123] = '456'
        x = d[123]
        self.assertEqual(d[123], '456')
        d[(12, 34, 56)] = [1,2,3,4,5,6]
        y = d[(12, 34, 56)]
        y1 = d[(12, 34, 56)] # test if the key-value can be requested twice or more
        y2 = d[(12, 34, 56)]
        self.assertEqual(y, [1,2,3,4,5,6])
        self.assertEqual(y1, [1,2,3,4,5,6])
        self.assertEqual(y2, [1,2,3,4,5,6])
        self.assertEqual(d[(12,34,56)], [1,2,3,4,5,6])
        self.assertRaises(KeyError, lambda x: d[x], 'hello')
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
        self.assertEqual(def_pool.free_space, before_free_space)
        d.destroy()

    def test_numpy_put_and_get(self):
        d = DDict(2, 1, 1024*1024*1024)
        arr_np = np.random.rand(2048, 2048)
        d['arr_np'] = arr_np
        self.assertTrue((arr_np == d['arr_np']).all())
        d.destroy()

    def test_pop(self):
        d = DDict(2,1,3000000)
        d['abc'] = 'def'
        x = d.pop('abc')
        self.assertEqual(x, 'def')
        with self.assertRaises(DDictError) as ex:
            d.pop('abc')
            self.assertIn('KEY_NOT_FOUND', str(ex), 'Expected DDictError message KEY_NOT_FOUND not in the raised exception.')

        d[123] = 456
        del d[123]
        with self.assertRaises(DDictError) as ex:
            d.pop(123)
            self.assertIn('KEY_NOT_FOUND', str(ex), 'Expected DDictError message KEY_NOT_FOUND not in the raised exception.')

        d[(12,34,56)] = [1,2,3,4,5,6]
        x = d.pop((12,34,56))
        with self.assertRaises(DDictError) as ex:
            d.pop((12,34,56))
            self.assertIn('KEY_NOT_FOUND', str(ex), 'Expected DDictError message KEY_NOT_FOUND not in the raised exception.')

        d.destroy()

    def test_contains_key(self):
        d = DDict(1,1,3000000)
        d['abc'] = 'def'
        self.assertTrue('abc' in d) # test existence of the added key
        self.assertFalse(123 in d) # test existence if the key is never added
        d[123] = 456
        self.assertTrue(123 in d)
        d.pop(123)
        self.assertFalse(123 in d) # test existence of a poped key
        d.pop('abc')
        self.assertFalse('abc' in d) # test existence of a poped key

        # test tuple key and value
        d[(1,2,3,4,5)] = [6,7,8,9,10]
        self.assertTrue((1,2,3,4,5) in d)
        del d[(1,2,3,4,5)]
        self.assertFalse((1,2,3,4,5) in d)

        d.destroy()

    def test_len(self):
        d = DDict(2,1,3000000, trace=True)
        self.assertEqual(len(d), 0)
        d['abc'] = 'def'
        self.assertEqual(len(d), 1)
        d[123] = 456
        self.assertEqual(len(d), 2)
        d[(1,2,3,4,5)] = [6,7,8,9,10]
        self.assertEqual(len(d), 3)
        d.pop('abc')
        self.assertEqual(len(d), 2)
        d.pop(123)
        self.assertEqual(len(d), 1)
        d.pop((1,2,3,4,5))
        self.assertEqual(len(d), 0)
        d.destroy()

    def test_clear(self):
        d = DDict(2,1,3000000, trace=True)
        d['abc'] = 'def'
        d[123] = 456
        d[(1,2,3,4,5)] = [6,7,8,9,10]
        self.assertEqual(len(d), 3)
        d.clear()
        self.assertEqual(len(d), 0)
        d.clear() # test clearing an empty dictionary
        self.assertEqual(len(d), 0)
        d['hello'] = 'world'
        d.clear()
        self.assertEqual(len(d), 0)
        d.destroy()

    @unittest.skip("Not yet implemented.")
    def test_iter(self):
        try:
            d = DDict(2,1,3000000)
            k = ['abc', 98765, 'hello', (1,2,3,4,5)]
            v = ['def', 200,   'world', ['a',1,3,5,'b']]
            for i, key in enumerate(k):
                d[key] = v[i]

            for i in d:
                if i == "abc":
                    self.assertEqual(d[i], 'def')
                elif i == 98765:
                    self.assertEqual(d[i], 200)
                elif i == 'hello':
                    self.assertEqual(d[i], 'world')
                elif i == (1,2,3,4,5):
                    self.assertEqual(d[i], ['a',1,3,5,'b'])
                else:
                    raise RuntimeError(f'Get the key which is not added by client: key={i}')

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
            raise Exception(f'Exception caught {e}\n Traceback: {tb}')

    def test_keys(self):
        d = DDict(2, 1, 3000000)
        k = ['abc', 98765, 'hello', (1,2,3,4,5)]
        v = ['def', 200,   'world', ['a',1,3,5,'b']]
        for i, key in enumerate(k):
            d[key] = v[i]
        ddict_keys = d.keys()
        for key in k:
            self.assertTrue(key in ddict_keys)
        d.destroy()

    def test_fill(self):
        d = DDict(1, 1, 900000)
        self.assertRaises(DDictManagerFull, fillit, d)
        d.destroy()

    def test_attach_ddict(self):
        d = DDict(2, 1, 3000000)
        d['hello'] = 'world'
        d_serialized = d.serialize()
        new_d = DDict.attach(d_serialized)
        self.assertEqual(new_d['hello'], 'world')
        d.detach()
        new_d.destroy()

    def test_np_array(self):
        """Make sure we pickle numpy arrays correctly"""

        d = DDict(1, 1, int(1.5 * 1024 * 1024 * 1024))
        x = np.ones((1024, 1024), dtype=np.int32)
        d['np array'] = x
        x_ref = d['np array']

        self.assertTrue((x & x_ref).all())
        d.destroy()

    def test_too_big(self):

        """Make sure we handle and respond to too big an allocation and raise an exception"""

        print(f'Client on host {socket.gethostname()}')
        d = DDict(1, 1, 4096)
        x = np.ones(4096, dtype=np.int32)

        with self.assertRaises(DDictManagerFull):
            d['np array'] = x

        print(d.stats)

        d.destroy()

    def test_too_big2(self):

        """Make sure we handle and respond to too big an allocation and raise an exception"""

        print(f'Client on host {socket.gethostname()}')
        d = DDict(1, 1, 4*1024*1024)
        x = np.ones(4*1024*1024, dtype=np.int32)

        with self.assertRaises(DDictManagerFull):
            d['np array'] = x

        print(d.stats)

        d.destroy()

    def test_too_big3(self):

        """Make sure we handle and respond to too big an allocation and raise an exception"""

        d = DDict(1, 1, 4*1024*1024, trace=True)

        proc = mp.Process(target=client_too_big3, args=(d,))
        proc.start()
        proc.join()

        self.assertEqual(proc.exitcode, 0, f'The client function returned a return code of {proc.exitcode}')

        print(d.stats)

        d.destroy()

    def test_get_missing_key(self):
        d = DDict(1, 1, 4*1024*1024)

        with self.assertRaises(KeyError):
            print(d['np array'])

        d.destroy()

    def test_get_newest_chkpt_id(self):
        d = DDict(4, 1, 5000000, wait_for_writers=True, working_set_size=3, trace=True)
        proc1 = mp.Process(target=client_func_1_get_newest_chkpt_id, args=(d,))
        proc1.start()
        proc1.join()
        self.assertEqual(0, proc1.exitcode)
        self.assertEqual(d.current_checkpoint_id, 0)
        d.sync_to_newest_checkpoint()
        self.assertEqual(d.current_checkpoint_id, 4)
        d.destroy()

    def test_wait_keys_put_get(self):
        """Make sure manager behaves when put request is received prior to get request"""
        d = DDict(2, 1, 3000000, wait_for_keys=True, working_set_size=2)
        ev = mp.Event()
        # persistent key
        proc1 = mp.Process(target=client_func_1_persist, args=(d,ev))
        proc2 = mp.Process(target=client_func_2_persist, args=(d,ev))
        proc1.start()
        ev.wait()
        proc2.start()
        proc1.join()
        proc2.join()
        self.assertEqual(0, proc1.exitcode)
        self.assertEqual(0, proc2.exitcode)

        # non-persistent key
        ev2 = mp.Event()
        proc1 = mp.Process(target=client_func_1_nonpersist, args=(d,ev2))
        proc2 = mp.Process(target=client_func_2_nonpersist, args=(d,ev2))
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
        proc1 = mp.Process(target=client_func_1_persist, args=(d,ev))
        proc2 = mp.Process(target=client_func_2_persist, args=(d,ev))
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
        d = DDict(2, 1, 3000000, wait_for_keys=True, working_set_size=2)
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
        d = DDict(2, 1, 3000000, wait_for_keys=True, working_set_size=2)
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
        d = DDict(2, 1, 3000000, wait_for_keys=True, working_set_size=2, timeout=30)
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
        d = DDict(2, 1, 3000000, wait_for_keys=True, working_set_size=2, trace=True)
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
        d = DDict(2, 1, 3000000, wait_for_keys=True, working_set_size=2)
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
        d = DDict(2, 1, 3000000, wait_for_keys=True, working_set_size=2)
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
        d = DDict(2, 1, 3000000, wait_for_keys=True, working_set_size=2)
        proc1 = mp.Process(target=client_func_1_wait_keys_pop_persist, args=(d, ))
        proc2 = mp.Process(target=client_func_2_wait_keys_pop_persist, args=(d, ))
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
        d = DDict(2, 1, 3000000, wait_for_keys=True, working_set_size=2, trace=True)
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
        print(d.stats)
        d.destroy()

    def test_wait_keys_keys(self):
        d = DDict(2, 1, 3000000, wait_for_keys=True, working_set_size=2, timeout=30)
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

    def test_wait_writers_put_get(self):
        """Make sure manager behave when put request is received prior get request"""
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
        """Make sure manager behave when get request is received prior put request"""
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
        d = DDict(2, 1, 3000000, wait_for_writers=True, working_set_size=2)
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
        d = DDict(2, 1, 3000000, wait_for_writers=True, working_set_size=2)
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
        d = DDict(2, 1, 3000000, wait_for_writers=True, working_set_size=2)
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
        d = DDict(2, 1, 3000000, wait_for_writers=True, working_set_size=2)
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
            (x, _) = recvh.recv_mem() # recv_bytes returns a tuple, first the bytes then the message attribute
            remote_host = x.get_memview().tobytes().decode('utf-8')
            if num_nodes == 1:
                self.assertEqual(remote_host, host)
            else:
                self.assertNotEqual(remote_host, host)

            self.assertEqual(x.pool.muid, POOL_MUID)

            with self.assertRaises(FLIEOT):
                (x, _) = recvh.recv_bytes() # We should get back an EOT here

        with resp_fli.sendh(use_main_as_stream_channel=True) as sendh:
            sendh.send_bytes('Done!'.encode('utf-8'))

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
        self.assertEqual(0, proc1.exitcode)
        self.assertEqual(0, proc2.exitcode)
        d.destroy()

if __name__ == "__main__":
    mp.set_start_method("dragon")
    unittest.main()
