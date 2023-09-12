#!/usr/bin/env python3

import unittest
import random
import string
import pickle
import math
import multiprocessing as mp
from dragon.managed_memory import MemoryPool, MemoryAlloc, MemoryAllocations, MemoryPoolAttr, \
    DragonPoolError, DragonPoolAttachFail, DragonPoolCreateFail, DragonMemoryError, AllocType

from dragon.dtypes import DragonError

# limits are for tests
MIN_POOL_SIZE = 32768
MAX_POOL_SIZE = 2 ** 31
DEFAULT_UID = 123456

# NOTE: The following function is dependent on the serialization of memory descriptors.
# It relies on the following format:
#    bytes offset     description
#    0-7              length of pool serialized descriptor
#    8-15             m_uid of pool
#    16-23            hostid of pool
#    ...              No dependencies beginning at byte offset 24
def mk_remote_mem_ser(local_mem_ser):
    mem_ser_array = bytearray(local_mem_ser)
    mem_ser_array[8] = 99 # change m_uid
    mem_ser_array[16] = 99 # change hostid
    mem_ser_remote = bytes(mem_ser_array)
    return mem_ser_remote

# NOTE: The following function is dependent on the serialization of memory pool descriptors.
# It relies on the following format:
#    bytes offset     description
#    0-7              m_uid of pool
#    8-15             hostid of pool
#    ...              No dependencies beginning at byte offset 16
def mk_remote_pool_ser(local_pool_ser):
    pool_ser_array = bytearray(local_pool_ser)
    pool_ser_array[0] = 99 # change m_uid
    pool_ser_array[8] = 99 # change hostid
    pool_ser_remote = bytes(pool_ser_array)
    return pool_ser_remote


class MemPoolCreateTest(unittest.TestCase):
    seed = 0

    @classmethod
    def setUpClass(cls):
        cls.seed = random.getrandbits(100)
        random.seed(a=cls.seed)
        # print(f'Our random seed is {cls.seed}')

    def test_create(self):
        size = random.randint(MIN_POOL_SIZE, MAX_POOL_SIZE)
        mpool = MemoryPool(size, "mpool_test", DEFAULT_UID, None)
        self.assertIsInstance(mpool, MemoryPool,
                              msg=f'Create failed to produce a memory pool of size {size}')
        mpool.destroy()

    def test_destroy(self):
        mpool = MemoryPool(MIN_POOL_SIZE, "mpool_test", DEFAULT_UID, None)
        pool_ser = mpool.serialize()
        mpool.destroy()
        with self.assertRaises(DragonPoolAttachFail):
            _ = MemoryPool.attach(pool_ser)

    @unittest.skip("Skipping for now until bug in ref counting is found")
    def test_attach(self):
        mpool = MemoryPool(MIN_POOL_SIZE, "mpool_test", DEFAULT_UID, None)
        pool_ser = mpool.serialize()
        mpool2 = MemoryPool.attach(pool_ser)
        mpool2.detach()
        mpool.destroy()

    @unittest.skip("Skipping for now until bug in ref counting is found")
    def test_attach_ref_count(self):
        mpool = MemoryPool(MIN_POOL_SIZE, "mpool_test", DEFAULT_UID, None)
        pool_ser = mpool.serialize()
        mpool.detach()

        mpools = []
        for i in range(10):
            mpools.append(MemoryPool.attach(pool_ser))

        for i in range(9):
            mpools[i].detach()

        # If any of the other detaches had removed our knowledge of the pool, this should fail
        # Success means we have not detached
        mpools[9].destroy()

    def test_serialized_info_no_attach(self):
        mpool = MemoryPool(MIN_POOL_SIZE, "mpool_test", DEFAULT_UID, None)
        pool_ser = mpool.serialize()

        (uid, fname) = MemoryPool.serialized_uid_fname(pool_ser)
        try:
            self.assertEqual(uid, DEFAULT_UID)
            self.assertEqual(fname, "/_dragon_mpool_test_manifest")
        finally:
            mpool.destroy()

    def test_destroy_not_created(self):
        # This doesn't really make sense to do, but it does validate destroy errors correctly
        mpool = MemoryPool.__new__(MemoryPool)
        with self.assertRaises(DragonPoolError,
                               msg=f'Destroying an uncreated pool did not raise error'):
            mpool.destroy()

    def test_negative_size(self):
        with self.assertRaises(OverflowError,
                               msg=f'Creating a negative size pool did not raise an OverflowError'):
            _ = MemoryPool(-12, "mpool_test", DEFAULT_UID, None)

    def test_negative_uid(self):
        with self.assertRaises(OverflowError):
            _ = MemoryPool(MIN_POOL_SIZE, "mpool_test", -1, None)

    def test_zero_uid(self):
        mpool = MemoryPool(MIN_POOL_SIZE, "mpool_test", 0, None)
        self.assertIsInstance(mpool, MemoryPool)
        mpool.destroy()

    def test_create_prealloc_blocks(self):
        prealloc_blocks = [8,16,11]
        mpool = MemoryPool(MAX_POOL_SIZE, "mpool_test", DEFAULT_UID, prealloc_blocks)
        self.assertIsInstance(mpool, MemoryPool,
                              msg=f'Failed to create memory pool of size f{MAX_POOL_SIZE} with pre-allocated blocks')
        mpool.destroy()

    def test_create_large_mpool(self):
        mpool = MemoryPool(MAX_POOL_SIZE, "mpool_test", DEFAULT_UID, None)
        self.assertIsInstance(mpool, MemoryPool,
                              msg=f'Failed to create memory pool of size f{MAX_POOL_SIZE}')
        mpool.destroy()

    def test_create_small_mpool(self):
        with self.assertRaises(DragonPoolCreateFail, msg=f'Creating an undersized pool did not raise a RuntimeError'):
            self.mpool = MemoryPool(100, "mpool_test", DEFAULT_UID, None)

    def test_create_zero_mpool(self):
        with self.assertRaises(DragonPoolCreateFail,
                               msg=f'Failed to raise RuntimeError on creation of 0 size memory pool'):
            _ = MemoryPool(0, "mpool_test", DEFAULT_UID, None)

    def test_create_empty_name_mpool(self):
        with self.assertRaises(DragonPoolCreateFail):
            _ = MemoryPool(MIN_POOL_SIZE, "", DEFAULT_UID, None)

    def test_create_large_uid_mpool(self):
        mpool = MemoryPool(MIN_POOL_SIZE, "mpool_test", 2 ** 63 + 1234, None)
        self.assertIsInstance(mpool, MemoryPool)
        mpool.destroy()

    def test_create_numerical_name_mpool(self):
        mpool = MemoryPool(MIN_POOL_SIZE, "12345", DEFAULT_UID, None)
        self.assertIsInstance(mpool, MemoryPool)
        mpool.destroy()

    def test_create_int_name_mpool(self):
        with self.assertRaises(TypeError):
            _ = MemoryPool(MIN_POOL_SIZE, 12345, DEFAULT_UID, None)

    def test_create_string_bytes(self):
        with self.assertRaises(TypeError):
            _ = MemoryPool("32768", "mpool_test", DEFAULT_UID, None)

    def test_create_string_uid(self):
        with self.assertRaises(TypeError):
            _ = MemoryPool(MIN_POOL_SIZE, "mpool_test", "1", None)

    def test_create_two(self):
        size1 = random.randint(MIN_POOL_SIZE, MAX_POOL_SIZE)
        size2 = random.randint(MIN_POOL_SIZE, MAX_POOL_SIZE)
        mpool1 = MemoryPool(size1, "mpool_test1", DEFAULT_UID, None)
        mpool2 = MemoryPool(size2, "mpool_test2", DEFAULT_UID + 1, None)
        self.assertIsInstance(mpool1, MemoryPool)
        self.assertIsInstance(mpool2, MemoryPool)
        mpool1.destroy()
        mpool2.destroy()

    def test_create_two_mpool_same_name(self):
        mpool1 = MemoryPool(34567, "mpool_test", DEFAULT_UID, None)
        with self.assertRaises(DragonPoolCreateFail):
            _ = MemoryPool(45678, "mpool_test", DEFAULT_UID + 1, None)
        mpool1.destroy()

    def test_create_two_mpool_same_uid(self):
        mpool1 = MemoryPool(34567, "mpool_test", DEFAULT_UID, None)
        with self.assertRaises(DragonPoolCreateFail):
            _ = MemoryPool(45678, "mpool_test", DEFAULT_UID, None)
        mpool1.destroy()

    def test_create_two_mpool_same_name_uid(self):
        mpool1 = MemoryPool(34567, "mpool_test", DEFAULT_UID, None)
        with self.assertRaises(DragonPoolCreateFail):
            _ = MemoryPool(34567, "mpool_test", DEFAULT_UID, None)
        mpool1.destroy()

    def test_create_destroy_many(self):
        for i in range(random.randint(25, 150)):
            size = random.randint(MIN_POOL_SIZE, MAX_POOL_SIZE)
            mpool = MemoryPool(size, "mpool_test", DEFAULT_UID, None)
            self.assertIsInstance(mpool, MemoryPool)
            mpool.destroy()

    def test_create_max_name(self):
        size = random.randint(MIN_POOL_SIZE, MAX_POOL_SIZE)
        # Expect shm_open() to limit names to _POSIX_PATH_MAX, which is
        # typically 256 bytes. Recall that names are prefixed with `"/" +
        # DRAGON_MEMORY_DEFAULT_FILE_PREFIX` (e.g., `"_dragon_"`) and suffixed
        # with `"_manifest."`, see _alloc_pool_shm(). This implies the longest
        # possible name given to `MemoryPool.create()` is 236 bytes, after being
        # UTF-8 encoded and accounting for the terminating `\0` byte.
        name = "a" * 236
        mpool = MemoryPool(size, name, 1, None)
        self.assertIsInstance(mpool, MemoryPool)
        mpool.destroy()

    #@unittest.skip("Limit is 236, bugfix PE-38813 incoming")
    def test_create_too_long_name(self):
        size = random.randint(MIN_POOL_SIZE, MAX_POOL_SIZE)
        # Based on the comment above, you might think any name over 238 bytes
        # will be too long; however, maximum paths can be finicky. Instead,
        # this function creates a name that is guaranteed to fill the maximum
        # size of the buffer passed to shm_open(), which is
        # DRAGON_MEMORY_DEFAULT_MAX_FILE_NAM_LENGTH bytes.
        name = "a" * 4096
        with self.assertRaises(DragonPoolCreateFail):
            _ = MemoryPool(size, name, 1, None)

    def test_create_random_name(self):
        name = ''.join(random.choices(string.ascii_letters + string.digits,
                                      k=random.randint(1, 236)))
        size = random.randint(MIN_POOL_SIZE, MAX_POOL_SIZE)
        mpool = MemoryPool(size, name, 1, None)
        self.assertIsInstance(mpool, MemoryPool)
        mpool.destroy()

    def test_create_random(self):
        name = ''.join(random.choices(string.ascii_letters + string.digits,
                                      k=random.randint(1, 236)))
        size = random.randint(MIN_POOL_SIZE, MAX_POOL_SIZE)
        uid = random.randint(0, 2 ** 64)
        mpool = MemoryPool(size, name, uid, None)
        self.assertIsInstance(mpool, MemoryPool)
        mpool.destroy()

    def test_create_size_float(self):
        with self.assertRaises(TypeError):
            _ = MemoryPool(34567.987, "mpool_test", DEFAULT_UID, None)

    def test_create_uid_float(self):
        with self.assertRaises(TypeError):
            _ = MemoryPool(MIN_POOL_SIZE, "mpool_test", 1.8, None)

    def test_create_too_few_args(self):
        with self.assertRaises(TypeError):
            _ = MemoryPool(MIN_POOL_SIZE)

    def test_create_too_many_args(self):
        with self.assertRaises(TypeError):
            _ = MemoryPool(MIN_POOL_SIZE, "mpool_test", DEFAULT_UID, None, None)


class MemoryPoolAllocTest(unittest.TestCase):

    def setUp(self):
        self.size = random.randint(MIN_POOL_SIZE, MAX_POOL_SIZE)
        self.mpool = MemoryPool(self.size, "mpool_test", DEFAULT_UID, None)

    def tearDown(self):
        self.mpool.destroy()

    def test_alloc_free(self):
        mem = self.mpool.alloc(512)
        self.assertEqual(1, self.mpool.get_allocations().num_allocs)
        mem.free()
        self.assertEqual(0, self.mpool.get_allocations().num_allocs)

    def test_clone(self):
        mem = self.mpool.alloc(512)
        mem.get_memview()[100] = 75
        num_allocs = self.mpool.get_allocations().num_allocs
        self.assertEqual(1, num_allocs)
        mem2 = mem.clone(100)
        self.assertEqual(mem2.get_memview()[0], mem.get_memview()[100])
        self.assertEqual(mem.size, mem2.size + 100)
        mem.free()

    def test_alloc_random(self):
        num_allocations = random.randint(25, 100)
        max_size = max(MIN_POOL_SIZE, math.floor(self.size / num_allocations))
        num_curr_allocs = 0
        curr_allocs = []
        for i in range(num_allocations):
            alloc_size = random.randint(256, max_size)
            mem = self.mpool.alloc(alloc_size)
            curr_allocs.append(mem)
            num_curr_allocs += 1
            self.assertEqual(num_curr_allocs, self.mpool.get_allocations().num_allocs)
            num_frees = math.floor(random.random() * num_curr_allocs)
            for j in range(num_frees):
                index = random.randint(0, num_curr_allocs - 1)
                curr_allocs[index].free()
                del curr_allocs[index]
                num_curr_allocs -= 1
                self.assertEqual(num_curr_allocs, self.mpool.get_allocations().num_allocs)
        for alloc in curr_allocs:
            alloc.free()
        self.assertEqual(self.mpool.get_allocations().num_allocs, 0)

    def test_get_id(self):
        mem = self.mpool.alloc(512)
        allocs = self.mpool.get_allocations()
        self.assertIsInstance(allocs, MemoryAllocations)
        self.assertEqual(allocs.alloc_id(0), 1)
        mem.free()

    def test_alloc_string(self):
        with self.assertRaises(TypeError):
            _ = self.mpool.alloc("512")

    def test_alloc_negative(self):
        with self.assertRaises(Exception):
            _ = self.mpool.alloc(-1)

    def test_alloc_float(self):
        with self.assertRaises(TypeError):
            _ = self.mpool.alloc(512.789)

    def test_alloc_zero(self):
        with self.assertRaises(RuntimeError):
            _ = self.mpool.alloc(0)

    def test_alloc_all(self):
        mem = self.mpool.alloc(self.size)
        self.assertEqual(1, self.mpool.get_allocations().num_allocs)
        mem.free()

    def test_alloc_small(self):
        mem = self.mpool.alloc(1)
        self.assertEqual(1, self.mpool.get_allocations().num_allocs)
        mem.free()

    def test_alloc_all_incremental(self):
        # Loop over 1000 times to stress test
        for i in range(0, 1000):
            size = self.size
            allocs = []

            # Random size out of remaining pool
            alloc_size = random.randint(1, size)
            # Set to >= power of 2 to prevent overflow
            alloc_size = 1 << (alloc_size - 1).bit_length()

            # While we have a valid block left, allocate and repeat
            while size >= alloc_size:
                # Reduce remaining size
                size -= alloc_size
                # Allocate
                mem = self.mpool.alloc(alloc_size)
                allocs.append(mem)

                if size == 0:
                    break

                alloc_size = random.randint(1, size)
                alloc_size = 1 << (alloc_size - 1).bit_length()

            # Free allocations to repeat
            for alloc in allocs:
                alloc.free()

    def test_return_codes(self):
        x = DragonError.SUCCESS
        self.assertEqual(x.name, 'SUCCESS')
        self.assertEqual(x.value, 0)

        y = DragonError.INVALID_ARGUMENT
        self.assertEqual(y.name, 'INVALID_ARGUMENT')



class MemoryPoolAllocNoSetupTests(unittest.TestCase):

    def test_alloc_after_destroy(self):
        with self.assertRaises(DragonPoolError):
            self.mpool = MemoryPool(random.randint(MIN_POOL_SIZE, MAX_POOL_SIZE),
                                           "mpool_test", 1, None)
            self.mpool.destroy()
            _ = self.mpool.alloc(512)

    def test_alloc_more_than_size(self):
        mpool = MemoryPool(MIN_POOL_SIZE, "mpool_test", 1, None)
        mem1 = mpool.alloc(10000)  # Powers of 2, allocates 16k
        mem2 = mpool.alloc(10000)  # Powers of 2, allocates 16k
        # Out of memory, expect failure
        with self.assertRaises(DragonMemoryError):
            _ = mpool.alloc(5000)

        mem1.free()
        mem2.free()
        mpool.destroy()


class MemoryPoolAttachTests(unittest.TestCase):

    def setUp(self):
        self.mpool_size = random.randint(MIN_POOL_SIZE, MAX_POOL_SIZE)
        self.mpool = MemoryPool(self.mpool_size, "mpool_test", 1, None)

    def tearDown(self):
        self.mpool.destroy()

    def test_attach(self):
        mem1 = self.mpool.alloc(512)
        mem_ser = mem1.serialize()
        mem2 = MemoryAlloc.attach(mem_ser)
        memview1 = mem1.get_memview()
        memview2 = mem2.get_memview()
        memview2[0:5] = b'Hello'
        self.assertEqual(memview1[0:5], memview2[0:5])
        self.assertEqual(memview1[0:5], b'Hello')
        memview1[0:11] = b'Hello world'
        self.assertEqual(memview1[0:11], memview2[0:11])
        self.assertEqual(memview1[0:11], b'Hello world')
        mem1.free()

    def test_attach_to_number(self):
        with self.assertRaises(TypeError):
            _ = MemoryAlloc.attach(1)

    def test_attach_to_string(self):
        with self.assertRaises(TypeError):
            _ = MemoryAlloc.attach("mpool_test")

    def test_attach_to_mpool(self):
        with self.assertRaises(TypeError):
            pool = MemoryPool(MIN_POOL_SIZE, "temp", 2, None)
            _ = MemoryAlloc.attach(pool)
        pool.destroy()

    def test_attach_allocation(self):
        with self.assertRaises(TypeError):
            mem1 = self.mpool.alloc(512)
            _ = MemoryAlloc.attach(mem1)
        mem1.free()

    def test_attach_bytes(self):
        with self.assertRaises(TypeError):
            mem1 = self.mpool.alloc(512)
            bytes_obj = pickle.dumps(mem1)
            _ = MemoryAlloc.attach(bytes_obj)
        mem1.free()

    def test_attach_many(self):
        mem1 = self.mpool.alloc(512)
        mem_ser = mem1.serialize()
        mem2 = MemoryAlloc.attach(mem_ser)
        mem_ser2 = mem2.serialize()
        mem3 = MemoryAlloc.attach(mem_ser2)
        memview1 = mem1.get_memview()
        memview2 = mem2.get_memview()
        memview3 = mem3.get_memview()
        memview2[0:5] = b'Hello'
        self.assertEqual(memview1[0:5], memview2[0:5])
        self.assertEqual(memview1[0:5], memview3[0:5])
        self.assertEqual(memview1[0:5], b'Hello')
        memview1[0:11] = b'Hello world'
        self.assertEqual(memview1[0:11], memview2[0:11])
        self.assertEqual(memview1[0:11], memview3[0:11])
        self.assertEqual(memview1[0:11], b'Hello world')
        mem1.free()

    def test_attach_detach(self):
        mem1 = self.mpool.alloc(512)
        mem_ser = mem1.serialize()
        mem2 = MemoryAlloc.attach(mem_ser)
        memview1 = mem1.get_memview()
        memview2 = mem2.get_memview()
        memview2[0:5] = b'Hello'
        self.assertEqual(memview1[0:5], memview2[0:5])
        self.assertEqual(memview1[0:5], b'Hello')

        # Assert that after detaching, we cannot get a reference to the underlying memory
        with self.assertRaises(DragonMemoryError):
            mem2.detach()
            _ = mem2.get_memview()

        mem1.free()

    def test_remote(self):
        mem1 = self.mpool.alloc(512)
        mem_ser = mem1.serialize()
        mem_ser_remote = mk_remote_mem_ser(mem_ser)
        mem2 = MemoryAlloc.attach(mem_ser_remote)
        mem_ser2 = mem2.serialize()
        mem3 = MemoryAlloc.attach(mem_ser2)
        mem4 =  mem3.clone(100)
        mem4_ser = mem4.serialize()
        mem5 = MemoryAlloc.attach(mem4_ser)

        rmt_pool_ser = mk_remote_pool_ser(self.mpool.serialize())
        rmt_pool = MemoryPool.attach(rmt_pool_ser)
        rmt_pool.detach()

        rmt_pool = MemoryPool.attach(rmt_pool_ser)
        with self.assertRaises(DragonPoolError):
            rmt_pool.destroy()

        with self.assertRaises(DragonPoolError):
            mem6 = rmt_pool.alloc(100)

        with self.assertRaises(DragonPoolError):
            mem6 = rmt_pool.alloc_blocking(100)

        with self.assertRaises(DragonPoolError):
            allocs = rmt_pool.get_allocations()

        with self.assertRaises(DragonPoolError):
            rmt_pool.allocation_exists(AllocType.DATA,20)

        rmt_pool.detach()

        with self.assertRaises(DragonMemoryError):
            mem5.get_memview()

        with self.assertRaises(DragonMemoryError):
            mem5.free()

        mem1.free()

if __name__ == '__main__':
    mp.set_start_method('spawn', force=True)
    unittest.main(verbosity=2)
