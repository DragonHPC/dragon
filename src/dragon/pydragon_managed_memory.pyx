from dragon.dtypes_inc cimport *
import dragon.dtypes as dtypes
import enum
import sys
import math


################################
# Begin Cython definitions
################################
class DragonMemoryError(dtypes.DragonException):
    def __init__(self, lib_err, msg):
        super().__init__(msg, lib_err)

class DragonMemoryTypeError(DragonMemoryError, TypeError):
    def __init__(self, lib_err, msg):
        super().__init__(lib_err, msg)

class DragonPoolError(DragonMemoryError):
    def __init__(self, lib_err, msg):
        super().__init__(lib_err, msg)

    @enum.unique
    class Errors(enum.Enum):
        SUCCESS = 0
        FAIL = 1
        CREATE_FAIL = 2
        ATTACH_FAIL = 3

class DragonPoolCreateFail(DragonPoolError):
    def __init__(self, lib_err, msg):
        super().__init__(lib_err, msg)

class DragonPoolAttachFail(DragonPoolError):
    def __init__(self, lib_err, msg):
        super().__init__(lib_err, msg)

class DragonPoolDetachFail(DragonPoolError):
    def __init__(self, lib_err, msg):
        super().__init__(lib_err, msg)

class DragonPoolAllocationNotAvailable(DragonPoolError):
    def __init__(self, lib_err, msg):
        super().__init__(lib_err, msg)

# @MCB TODO: How do we expose ctypedef enums directly to python instead of having to maintain this?
# PJM: there is an easy way to do this (I have it below for the ConnMsgHeader stuff).  We'level
#  do this as part of cleanup.
class PoolType(enum.Enum):
    """TBD """
    SHM = DRAGON_MEMORY_TYPE_SHM
    FILE = DRAGON_MEMORY_TYPE_FILE
    PRIVATE = DRAGON_MEMORY_TYPE_PRIVATE


class AllocType(enum.Enum):
    """TBD """
    DATA = DRAGON_MEMORY_ALLOC_DATA
    CHANNEL = DRAGON_MEMORY_ALLOC_CHANNEL
    CHANNEL_BUFFER = DRAGON_MEMORY_ALLOC_CHANNEL_BUFFER
    BOOTSTRAP = DRAGON_MEMORY_ALLOC_BOOTSTRAP

DRAGON_MEMORY_DEFAULT_TIMEOUT = 300

# PJM: PE-38098 is a place holder for a lot of cleanup in this Filename
# we need to:
# * get rid of the attr class here and absorb it as optional args into create
# * mimic the way the constructors for Channels ard done here
# * LOTS of cleanup on typing of args, having things sit on C calls (not other Pythin classes) for performance
# * fix the exception classes following what we did in Channels
# * carry through all of the available Pool attributes
# * tidy up the enum above and anywhere else

cdef class MemoryPoolAttr:
    """
    Cython wrapper for managed memory attributes
    Currently unused
    """

    cdef dragonMemoryPoolAttr_t _mattr

    def __init__(self, pre_alloc_blocks=None):
        cdef dragonError_t derr

        if pre_alloc_blocks is not None:
            if not isinstance(pre_alloc_blocks, list):
                raise RuntimeError(f"MemoryAttr Error: pre_alloc_blocks must be a list of ints")
            if not all(isinstance(item, int) for item in pre_alloc_blocks):
                raise RuntimeError(f"MemoryAttr Error: pre_alloc_blocks must be a list of ints")

        derr = dragon_memory_attr_init(&self._mattr)
        if derr != DRAGON_SUCCESS:
            raise RuntimeError(f"MemoryAttr Error: Unable to initialize memory attribute. Dragon Error Code: ({dragon_get_rc_string(derr)})")

        if pre_alloc_blocks is not None:
            self._mattr.npre_allocs = len(pre_alloc_blocks)
            self._mattr.pre_allocs = <size_t *>malloc(sizeof(size_t) * self._mattr.npre_allocs)
            for i in range(self._mattr.npre_allocs):
                self._mattr.pre_allocs[i] = pre_alloc_blocks[i]

    def __del__(self):
        cdef dragonError_t derr

        derr = dragon_memory_attr_destroy(&self._mattr)


cdef class MemoryAlloc:
    """
    Cython wrapper object for memory pool allocations.
    """

    def __getstate__(self):
        return self.serialize()

    def __setstate__(self, state):
        serialized_bytes = state
        self._attach(serialized_bytes)

    def __del__(self):
        if self._is_serial == 1:
            dragon_memory_serial_free(&self._mem_ser)

    def __hash__(self):
        """
        Get a hash value for the underlying memory

        :return: an integer hash value.
        """
        cdef:
            dragonError_t derr
            dragonULInt hash_val

        derr = dragon_memory_hash(&self._mem_descr, &hash_val)
        if derr != DRAGON_SUCCESS:
            raise DragonMemoryError(derr, "Could not get memory hash")

        return hash_val

    def __eq__(self, other):
        cdef:
            dragonError_t derr
            bool result
            MemoryAlloc other_mem

        # Try it the other way around first.
        if (type(other) is not MemoryAlloc):
            return other.__eq__(self)

        other_mem = <MemoryAlloc>other

        derr = dragon_memory_equal(&self._mem_descr, &other_mem._mem_descr, &result)
        if derr != DRAGON_SUCCESS:
            raise DragonMemoryError(derr, "Could not compare memory allocations")

        return result

    # debug use only by developers.
    def _manifest_info(self):
        cdef:
            dragonError_t derr
            dragonULInt type_val
            dragonULInt type_id_val

        derr = dragon_memory_manifest_info(&self._mem_descr, &type_val, &type_id_val)
        if derr != DRAGON_SUCCESS:
            raise DragonMemoryError(derr, "Could not compare memory allocations")

        return (type_val, type_id_val)

    def is_the_same_as(self, MemoryAlloc other):
        cdef:
            dragonError_t derr
            bool result

        # not sure this is needed, but doesn't hurt.
        if (type(other) is not MemoryAlloc):
                raise DragonMemoryError(DRAGON_INVALID_OPERATION, "Cannot compare MemoryAlloc with value of different type.")

        derr = dragon_memory_is(&self._mem_descr, &other._mem_descr, &result)
        if derr != DRAGON_SUCCESS:
            raise DragonMemoryError(derr, "Could not check memory allocations")

        return result

    def get_memview(self):
        """
        Get a memoryview of the underlying memory

        :return: Memoryview object
        """
        cdef:
            dragonError_t derr
            void * ptr

        derr = dragon_memory_get_pointer(&self._mem_descr, &ptr)
        if derr != DRAGON_SUCCESS:
            raise DragonMemoryError(derr, "Could not get memory pointer")

        # 256 for Read, 512 for Write?
        return PyMemoryView_FromMemory(<char*>ptr, self._mem_size, 512)

    def serialize(self):
        """
        Serialize the memory allocation for storage or communication

        :return: Memoryview of the serialized data
        """
        cdef:
            dragonError_t derr

        derr = dragon_memory_serialize(&self._mem_ser, &self._mem_descr)
        if derr != DRAGON_SUCCESS:
            raise DragonMemoryError(derr, "Could not serialize memory")

        self._is_serial = 1
        return self._mem_ser.data[:self._mem_ser.len]

    def clone(self, offset=0, length=None):
        """
        Clone this memory allocation with an offset into it.

        :param size: offset in bytes into this allocation
        :return: New MemoryAlloc object
        :raises: RuntimeError
        """
        cdef:
            dragonError_t derr
            dragonMemoryDescr_t mem
            size_t custom_length

        if not isinstance(offset, int):
            raise TypeError(f"Allocation offset must be int, got type {type(offset)}")

        if offset < 0:
            raise RuntimeError("Offset cannot be less than 0 for memory allocations")

        if length is not None:
            if not isinstance(length, int):
                raise TypeError(f"Allocation custom length must be int, got type {type(length)}")
            if length < 0:
                raise RuntimeError("Length cannot be less than 0 for memory allocations")

            custom_length = <size_t>length
            derr = dragon_memory_descr_clone(&mem, &self._mem_descr, offset, &custom_length)
        else:
            derr = dragon_memory_descr_clone(&mem, &self._mem_descr, offset, NULL)

        if derr != DRAGON_SUCCESS:
            raise DragonPoolError(derr, "Could not clone allocation")

        mem_alloc_obj = MemoryAlloc.cinit(mem)
        # We had to move the error handling here, in the caller
        if not isinstance(mem_alloc_obj, MemoryAlloc):
            # if there was an error, the returned value is a tuple of the form: (derr, err_str)
            raise DragonMemoryError(mem_alloc_obj[0], mem_alloc_obj[1])
        return mem_alloc_obj

    def _attach(self, ser_bytes):
        cdef:
            dragonError_t derr
            dragonMemorySerial_t _ser
            const unsigned char[:] cdata = ser_bytes

        _ser.len = len(ser_bytes)
        _ser.data = <uint8_t*>&cdata[0]

        derr = dragon_memory_attach(&self._mem_descr, &_ser)
        if derr != DRAGON_SUCCESS:
            raise DragonMemoryTypeError(derr, "Could not attach to memory")

        derr = dragon_memory_get_size(&self._mem_descr, &self._mem_size)
        if derr != DRAGON_SUCCESS:
            raise DragonMemoryError(derr, "Could not retrieve memory size")

        self._is_attach = 1

    @classmethod
    def attach(cls, ser_bytes):
        """
        Attach to a serialized memory allocation

        :param ser_bytes: Bytes-like object (memoryview, bytearray, bytes) of a serialized memory descriptor
        :return: MemoryAlloc object
        """
        memobj = MemoryAlloc()
        memobj._attach(ser_bytes)
        return memobj

    def detach(self):
        """
        Detach from memory previously attached to.
        """
        cdef:
            dragonError_t derr


        # @MCB TODO: Does this still make sense?
        if self._is_attach == 0:
            raise RuntimeError("cannot detach from memory not attached to")

        derr = dragon_memory_detach(&self._mem_descr)
        if derr != DRAGON_SUCCESS:
            raise DragonMemoryError(derr, "could not detach from memory")

        self._is_attach = 0

    def free(self):
        """
        Free the managed memory allocation.

        :return: None
        :raises: DragonMemoryError
        """
        cdef dragonError_t derr

        with nogil:
            derr = dragon_memory_free(&self._mem_descr)
        if derr != DRAGON_SUCCESS:
            raise DragonMemoryError(derr, "could not free allocation")

    def copy(self, pool:MemoryPool, timeout=None):
        """
        Copy an allocation into a pool

        :param pool: The pool in which to copy it.
        :return: A new memory allocation
        :raises: DragonMemoryError
        """

        cdef:
            dragonError_t err
            dragonMemoryDescr_t to_mem
            dragonMemorySerial_t ser_mem
            timespec_t timer
            timespec_t* time_ptr

        if timeout is None:
            time_ptr = NULL
        elif isinstance(timeout, int) or isinstance(timeout, float):
            if timeout < 0:
                raise ValueError('Cannot provide timeout < 0')
            # Anything > 0 means use that as seconds for timeout.
            time_ptr = &timer
            timer.tv_sec =  int(timeout)
            timer.tv_nsec = int((timeout - timer.tv_sec)*1000000000)
        else:
            raise ValueError('The timeout must be a float or int')

        err = dragon_memory_copy(&self._mem_descr, &to_mem, &pool._pool_hdl, time_ptr)
        if err != DRAGON_SUCCESS:
            raise DragonMemoryError(err, "Could not copy memory.")

        mem_alloc_obj = MemoryAlloc.cinit(to_mem)
        return mem_alloc_obj

    def clear(self, start=0, stop=None):
        cdef:
            dragonError_t derr
            size_t stop_idx


        if stop is None:
            # The API will self adjust down to allowable stop.
            stop_idx = 0xffffffffffffffff
        else:
            stop_idx = stop

        derr = dragon_memory_clear(&self._mem_descr, start, stop_idx)
        if derr != DRAGON_SUCCESS:
            raise DragonMemoryError(derr, "could not clear memory allocation.")

    @property
    def size(self):
        """
        Return the size of the allocation

        return: the size in bytes of the memory allocation
        """
        return self._mem_size

    @property
    def id(self):
        """
        Returns the unique identifier of this memory allocation.

        :return: the unique allocation identifier
        :raises: DragonMemoryError
        """
        cdef:
            dragonError_t err
            uint64_t id_val

        err = dragon_memory_id(&self._mem_descr, &id_val)
        if err != DRAGON_SUCCESS:
            raise DragonMemoryError(err, "Could not retrieve the pool for the memory allocation.")

        return id_val

    @property
    def pool(self):
        """
        Return the pool of this memory allocation.

        :return: pool where this resides.
        :raises: DragonMemoryError
        """
        cdef:
            dragonError_t err
            dragonMemoryPoolDescr_t pool_descr
            dragonMemoryPoolSerial_t ser_pool
            dragonULInt muid

        err = dragon_memory_get_pool(&self._mem_descr, &pool_descr)
        if err != DRAGON_SUCCESS:
            raise DragonMemoryError(err, "Could not retrieve the pool for the memory allocation.")

        err = dragon_memory_pool_serialize(&ser_pool, &pool_descr)
        if err != DRAGON_SUCCESS:
            raise DragonPoolError(err, "Could not serialize pool")

        # Returns a python copy of the serializer
        return MemoryPool.attach(ser_pool.data[:ser_pool.len])


cdef class MemoryAllocations:
    """
    Cython wrapper object to provide access to lists of existing memory allocations in a pool
    """

    cdef dragonMemoryPoolAllocations_t allocs

    def __del__(self):
        with nogil:
            dragon_memory_pool_allocations_destroy(&self.allocs)

    # @MCB Note: Cython gets really mad if we try to pass in C structs to __cinit__, so this will
    #  do for now
    @staticmethod
    cdef cinit(dragonMemoryPoolAllocations_t allocs):
        """
        Create a MemoryAllocations object and populate its inner C struct.

        :return: MemoryAllocations object
        """
        pyobj = MemoryAllocations()
        pyobj.allocs.nallocs = allocs.nallocs
        pyobj.allocs.types = allocs.types
        pyobj.allocs.ids = allocs.ids
        return pyobj

    @property
    def num_allocs(self):
        """ Number of allocations in pool. """
        return self.allocs.nallocs

    def alloc_type(self, idx):
        """
        Get the type of a particular allocation id.

        :return: Enum of the allocation type (if it exists)
        :raises: RuntimeError if allocation not found
        """
        if idx < 0 or idx >= self.allocs.nallocs:
            raise RuntimeError("Index out of bounds")

        return AllocType(self.allocs.types[idx])

    def alloc_id(self, idx):
        if idx < 0 or idx >= self.allocs.nallocs:
            raise RuntimeError("Index out of bounds")

        return self.allocs.ids[idx]


cdef class MemoryPool:
    """
    Cython wrapper for managed memory pools and related structures
    """

    # @MCB: C attributes and methods are defined in managed_memory.pxd to be shared with other Cython objects
    # This is probably worth revisiting, it feels very clunky.
    #cdef dragonMemoryPoolDescr_t _pool_hdl # Lives in managed_memory.pxd
    #cdef dragonMemoryPoolSerial_t _pool_ser
    #cdef bint _serialized

    def __cinit__(self):
        self._serialized = 0

    def __getstate__(self):
        return (self.serialize(), self._muid)

    def __setstate__(self, state):
        (serialized_bytes, self._muid) = state
        self.attach(serialized_bytes, existing_memory_pool=self)

    def __del__(self):
        # TODO: Proper error handling for this?
        if self._serialized == 1:
            dragon_memory_pool_serial_free(&self._pool_ser)


    def __init__(self, size, str fname, uid, pre_alloc_blocks=None, min_block_size=None, max_allocations=None):
        """
        Create a new memory pool and return a MemoryPool object.

        :param size: Minimum size (in bytes) of the pool. The actual size will be at least
            this size.
        :param fname: Filename of the pool to use.
        :param uid: Unique pool identifier to use.
        :param pre_alloc_blocks: A list of integers indicating the number of pre-allocated
            blocks to allocate of each power of two starting with the minimum block size.
        :param min_block_size: The minimum block size of the pool. The maximum block size
            is determined by the pool size. The default minimum block size is 4KB.
        :param max_allocations: The maximum number of concurrent allocations that can exist
            among all processes at a given time. The default is 1048576 (1024^2) but can be
            set higher if needed. Space is reserved for a manifest of all allocations in
            shared memory and the default results in approximately 128MB of reserved space.
            It can be adjusted, but adjustments will increase or decrease the reserved
            space proportionally.
        :return: MemoryPool object
        :raises: DragonPoolCreateFail
        """
        cdef:
            dragonError_t derr

        # These are specifically not set with type hints because Cython will automatically
        #   truncate float objects to ints, which allows for things like
        #   mpool = MemoryPool.create(1000.5764, 'foo', 1.5)
        #   which should be considered invalid
        if not isinstance(size, int):
            raise TypeError(f"Pool size must be int, got type {type(size)}")

        if not isinstance(uid, int):
            raise TypeError(f"Pool uid must be int, got type {type(uid)}")

        self._muid = uid

        derr = dragon_memory_attr_init(&self._mattr)
        if derr != DRAGON_SUCCESS:
            raise RuntimeError(f"MemoryAttr Error: Unable to initialized memory attribute. Dragon Error Code: ({dragon_get_rc_string(derr)})")

        if min_block_size is not None:
            if not isinstance(min_block_size, int):
                raise RuntimeError('MemoryAttr Error: min_block_size must be an int')

            self._mattr.data_min_block_size = min_block_size

        if max_allocations is not None:
            if not isinstance(max_allocations, int):
                raise RuntimeError('MemoryAttr Error: max_allocations must be an int')

            self._mattr.max_allocations = max_allocations

        # if pre_alloc_blocks is used, build mattr struct
        if pre_alloc_blocks is not None:
            if not isinstance(pre_alloc_blocks, list):
                raise RuntimeError(f"MemoryAttr Error: pre_alloc_blocks must be a list of ints")
            if not all(isinstance(item, int) for item in pre_alloc_blocks):
                raise RuntimeError(f"MemoryAttr Error: pre_alloc_blocks must be a list of ints")

            alloc_arr_size = int(math.log(size//self._mattr.data_min_block_size, 2))+1
            alloc_arr_diff = alloc_arr_size - len(pre_alloc_blocks)
            pre_alloc_blocks += ([0]*alloc_arr_diff)

            self._mattr.npre_allocs = len(pre_alloc_blocks)
            self._mattr.pre_allocs = <size_t *>malloc(sizeof(size_t) * self._mattr.npre_allocs)
            for i in range(self._mattr.npre_allocs):
                self._mattr.pre_allocs[i] = pre_alloc_blocks[i]

        derr = dragon_memory_pool_create(&self._pool_hdl, size,
                                         fname.encode('utf-8'), uid, &self._mattr)
        # This is purely temporary and gets copied internally on the pool_create call, free it here
        if pre_alloc_blocks is not None:
            free(self._mattr.pre_allocs)
        if derr != DRAGON_SUCCESS:
            raise DragonPoolCreateFail(derr, "Could not create pool")

    @classmethod
    def serialized_uid_fname(cls, pool_ser):
        cdef:
            dragonError_t derr
            dragonULInt uid
            char * fname
            const unsigned char[:] cdata = pool_ser
            dragonMemoryPoolSerial_t _ser

        _ser.len = len(pool_ser)
        _ser.data = <uint8_t*>&cdata[0]
        derr = dragon_memory_pool_get_uid_fname(&_ser, &uid, &fname)
        if derr != DRAGON_SUCCESS:
            raise DragonPoolError(derr, "Error retrieving data from serialized pool data")

        pystring = fname[:].decode('utf-8')
        free(fname)

        return (uid, pystring)


    @classmethod
    def empty_pool(cls):
        return cls.__new__(cls)


    @classmethod
    def attach(cls, pool_ser, *, existing_memory_pool=None):
        """
        Attach to an existing pool through a serialized descriptor.

        :param pool_ser: Bytes-like object of a serialized pool descriptor.
        :return: MemoryPool object
        :raises: DragonPoolAttachFail
        """
        cdef:
            dragonError_t derr
            dragonMemoryPoolSerial_t _ser
            const unsigned char[:] cdata = pool_ser
            MemoryPool mpool
            dragonULInt the_muid

        if existing_memory_pool is None:
            mpool = cls.__new__(cls) # Create an empty instance of MemoryPool
        elif isinstance(existing_memory_pool, MemoryPool):
            mpool = existing_memory_pool
        else:
            raise TypeError(f"Unsupported {type(existing_memory_pool)} != MemoryPool")

        if len(pool_ser) == 0:
            raise ValueError(f'Zero length serialized pool descriptor cannot be attached.')

        _ser.len = len(pool_ser)
        _ser.data = <uint8_t*>&cdata[0]

        derr = dragon_memory_pool_attach(&mpool._pool_hdl, &_ser)
        if derr != DRAGON_SUCCESS:
            raise DragonPoolAttachFail(derr, "Could not attach to serialized pool")

        derr = dragon_memory_pool_muid(&mpool._pool_hdl, &the_muid)
        if derr != DRAGON_SUCCESS:
            raise DragonPoolError(derr, "Could not retrieve muid of pool.")

        mpool._muid = the_muid

        return mpool

    @classmethod
    def attach_default(cls):
        """
        Attach to the default pool.
        :return: default memory pool object
        :raises: DragonPoolAttachFail
        """
        cdef:
            dragonError_t derr
            MemoryPool mpool
            dragonULInt the_muid

        mpool = cls.__new__(cls)

        derr = dragon_memory_pool_attach_default(&mpool._pool_hdl)
        if derr != DRAGON_SUCCESS:
            raise DragonPoolAttachFail(derr, "Could not attach to serialized pool")

        derr = dragon_memory_pool_muid(&mpool._pool_hdl, &the_muid)
        if derr != DRAGON_SUCCESS:
            raise DragonPoolError(derr, "Could not retrieve muid of pool.")

        mpool._muid = the_muid

        return mpool

    @classmethod
    def make_process_local(cls, str name, size, min_block_size=None, pre_allocs=None, timeout=None):
        """
        Create a process local pool which is a pool that exists for the
        lifetime of the current process. The pool may be shared with other
        processes, but must be managed in such a way that all shared allocations
        in the pool are freed prior to this process terminating since the
        pool will be destroyed when the process exits. The pool may live beyond
        the process by deregistering the pool from the process (see deregister)
        and a pool may be registered as a local pool if desired. When registering
        a pool you should make sure that its lifetime is not being managed by
        Global Services. Locally created pools are not managed by Global Services.

        This is especially useful for processes that need a pool for their own
        application's use and don't need the pool anymore once the process exits.

        :param name: The name to be used as part of the name of the mmap that is used
            for the shared memory.

        :param size: The size of the managed memory pool. It will be at least that large.

        :param min_block_size: The minimum allocatable block size. The min block size will be
            at least this big, possibly bigger. The current implementation will make it
            the next smallest power of 2. If no min_block_size is provided a default will
            be used.

        :param pre_allocs: A list of integers corresponding to pre-allocated blocks within the
            pool where each index within the list corresponds to a power of 2 starting with the
            min_block_size. The value at each location in the list is the number of pre-allocated
            blocks of that size.

        :param timeout: Default is None which means to block without timeout until the
            pool is made. This should not timeout and should be processed quickly. If a
            timeout value is specified, it is the number of seconds to wait which may be a
            float.

        :return: A new pool object.

        :raises: DragonMemoryError if there was an error. Note that the Dragon run-time
            must be running to use this function as it interacts with Local Services on the
            node on which it is called.
        """
        cdef:
            dragonError_t err
            timespec_t * time_ptr
            timespec_t val_timeout
            dragonMemoryPoolDescr_t pool
            dragonMemoryPoolSerial_t ser
            dragonMemoryPoolAttr_t attrs
            dragonMemoryPoolAttr_t* attr_ptr
            size_t* pre
            size_t num_pres
            size_t num_bytes
            const unsigned char [:] ucname = name.encode('utf-8')
            const char * cname = <const char*> &ucname[0]

        attr_ptr = NULL
        pre = NULL

        if timeout is None:
            time_ptr = NULL
        elif isinstance(timeout, int) or isinstance(timeout, float):
            if timeout < 0:
                raise ValueError('Cannot provide timeout < 0 to make_process_local operation')
            # Anything > 0 means use that as seconds for timeout.
            time_ptr = & val_timeout
            val_timeout.tv_sec =  int(timeout)
            val_timeout.tv_nsec = int((timeout - val_timeout.tv_sec)*1000000000)
        else:
            raise ValueError('make_process_local timeout must be a float or int')

        if min_block_size is not None or pre_allocs is not None:
            attr_ptr = &attrs
            err = dragon_memory_attr_init(attr_ptr)
            if err != DRAGON_SUCCESS:
                raise DragonMemoryError(err, "Could not init attributes")

            if min_block_size is not None:
                attr_ptr.data_min_block_size = min_block_size

            if pre_allocs is not None:
                num_pres = len(pre_allocs)
                if num_pres > 0:
                    pre = <size_t*>malloc(sizeof(size_t)*num_pres)
                    for i in range(num_pres):
                        pre[i] = pre_allocs[i]

                    attr_ptr.pre_allocs = pre

        num_bytes = size

        with nogil:
            err = dragon_create_process_local_pool(&pool, num_bytes, cname, attr_ptr, time_ptr)

        if pre_allocs is not None:
            free(pre)

        if err != DRAGON_SUCCESS:
            raise DragonMemoryError(err, "Could not create process local pool")

        err = dragon_memory_pool_serialize(&ser, &pool)
        if err != DRAGON_SUCCESS:
            raise DragonMemoryError(err, "Could not serialize pool")

        py_obj = ser.data[:ser.len]

        dragon_memory_pool_serial_free(&ser)

        # This inits the rest of the object given the pool descriptor above.
        return MemoryPool.attach(py_obj)

    def deregister(self, timeout=None):
        """
        Deregister a process local pool which is a pool that exists for the
        lifetime of the current process. By deregistering it the current process is
        claiming responsibility for the management of the lifetime of this pool. This
        only applies to pools created with the make_process_local path. Those pools
        created through Global Services are managed exclusively by Global Services.

        :parm timeout: The amount of time the process is willing to wait for a response.
            A timeout of None means to wait without timing out.

        :raises: DragonMemoryError if there was an error. Note that the Dragon run-time
            must be running to use this function as it interacts with Local Services on the
            node on which it is called.
        """
        cdef:
            dragonError_t err
            timespec_t * time_ptr
            timespec_t val_timeout

        if timeout is None:
            time_ptr = NULL
        elif isinstance(timeout, int) or isinstance(timeout, float):
            if timeout < 0:
                raise ValueError('Cannot provide timeout < 0 to deregister operation')
            # Anything > 0 means use that as seconds for timeout.
            time_ptr = & val_timeout
            val_timeout.tv_sec =  int(timeout)
            val_timeout.tv_nsec = int((timeout - val_timeout.tv_sec)*1000000000)
        else:
            raise ValueError('deregister timeout must be a float or int')

        err = dragon_deregister_process_local_pool(&self._pool_hdl, time_ptr)
        if err != DRAGON_SUCCESS:
            DragonPoolAttachFail(err, "Could not deregister pool")

    def register(self, timeout=None):
        """
        Register a process local pool which is a pool that exists for the
        lifetime of the current process. By registering it the current
        process is turning over responsibility for the management of the
        lifetime of this pool to local services. This only applies to
        pools created with the make_process_local path or those pools
        created entirely by a process through the MemoryPool constructor
        (be careful if doing this - the muid must be unique). Local
        Services will guarantee unique muids for pools created through
        the create_process_local method of MemoryPool as will Global
        Services through its API. Those pools created through Global
        Services are managed exclusively by Global Services and should
        not be registered with Local Services.

        :parm timeout: The amount of time the process is willing to wait for a response.
            A timeout of None means to wait without timing out.

        :raises: DragonMemoryError if there was an error. Note that the Dragon run-time
            must be running to use this function as it interacts with Local Services on the
            node on which it is called.
        """
        cdef:
            dragonError_t err
            timespec_t * time_ptr
            timespec_t val_timeout

        if timeout is None:
            time_ptr = NULL
        elif isinstance(timeout, int) or isinstance(timeout, float):
            if timeout < 0:
                raise ValueError('Cannot provide timeout < 0 to register operation')
            # Anything > 0 means use that as seconds for timeout.
            time_ptr = & val_timeout
            val_timeout.tv_sec =  int(timeout)
            val_timeout.tv_nsec = int((timeout - val_timeout.tv_sec)*1000000000)
        else:
            raise ValueError('register timeout must be a float or int')

        err = dragon_register_process_local_pool(&self._pool_hdl, time_ptr)
        if err != DRAGON_SUCCESS:
            DragonPoolAttachFail(err, "Could not register pool")

    def destroy(self):
        """
        Destroy the pool created by this object.
        """
        cdef dragonError_t derr

        with nogil:
            derr = dragon_memory_pool_destroy(&self._pool_hdl)

        if derr != DRAGON_SUCCESS:
            raise DragonPoolError(derr, "Could not destroy pool")

    def detach(self, serialize=False):
        """
        Detach from a previously attached to pool by this object.

        :param serialize: Boolean to optionally store a serializer before detaching
        """
        cdef dragonError_t derr

        if serialize:
            self.serialize()

        derr = dragon_memory_pool_detach(&self._pool_hdl)
        if derr != DRAGON_SUCCESS:
            DragonPoolAttachFail(derr, "Could not detach pool")

    def serialize(self):
        """
        Serialize the pool held by this object.
        Will store a copy of the serialized data as part of the object after first call.

        :return: Memoryview of serialized pool descriptor.
        """
        cdef:
            dragonError_t derr

        if self._serialized != 1:
            derr = dragon_memory_pool_serialize(&self._pool_ser, &self._pool_hdl)
            if derr != DRAGON_SUCCESS:
                raise DragonPoolError(derr, "Could not serialize pool")

            self._serialized = 1

        # Returns a python copy of the serializer
        return self._pool_ser.data[:self._pool_ser.len]

    def alloc(self, size, alloc_type=None):
        """
        Allocate a memory block within this pool.
        Please note that the internal memory manager allocates to nearest powers of 2.

        :param size: Size (in bytes) to allocate
        :return: New MemoryAlloc object
        :raises: RuntimeError
        """
        cdef:
            dragonError_t derr
            dragonMemoryDescr_t mem
            size_t sz
            int flag
            dragonMemoryAllocationType_t tyval

        if not isinstance(size, int):
            raise TypeError(f"Allocation size must be int, got type {type(size)}")

        if size < 0:
            raise RuntimeError("Size cannot be less than 0 for memory allocations")

        sz = size

        # allocate with type
        if alloc_type is not None:

            if type(alloc_type) is AllocType:
                tyval = alloc_type.value
            else:
                raise DragonMemoryError(DRAGON_INVALID_ARGUMENT, 'alloc_type must be None or of type AllocType.')

            with nogil:
                derr = dragon_memory_alloc_type(&mem, &self._pool_hdl, sz, tyval)
        else:
            # allocate without type
            with nogil:
                derr = dragon_memory_alloc(&mem, &self._pool_hdl, sz)

        if derr == DRAGON_DYNHEAP_REQUESTED_SIZE_NOT_AVAILABLE:
            raise DragonPoolAllocationNotAvailable(derr, f"An allocation of size={size} is not available.")

        if derr != DRAGON_SUCCESS:
            raise DragonPoolError(derr, "Could not perform allocation")

        mem_alloc_obj = MemoryAlloc.cinit(mem)

        if not isinstance(mem_alloc_obj, MemoryAlloc):
            # if there was an error, the returned value is a tuple of the form: (derr, err_str)
            raise DragonMemoryError(mem_alloc_obj[0], mem_alloc_obj[1])

        return mem_alloc_obj

    def alloc_blocking(self, size, timeout=None, alloc_type=None):
        """
        Allocate a memory block within this pool.
        Please note that the internal memory manager allocates to nearest powers of 2.

        :param size: Size (in bytes) to allocate
        :return: New MemoryAlloc object
        :raises: RuntimeError
        """
        cdef:
            dragonError_t derr
            dragonMemoryDescr_t mem
            timespec_t timer
            timespec_t* time_ptr
            size_t sz
            int flag
            dragonMemoryAllocationType_t tyval
            dragonULInt alid

        if timeout is None:
            time_ptr = NULL
        elif isinstance(timeout, int) or isinstance(timeout, float):
            if timeout < 0:
                raise ValueError('Cannot provide timeout < 0 to alloc_blocking operation')

            # Anything > 0 means use that as seconds for timeout.
            time_ptr = &timer
            timer.tv_sec =  int(timeout)
            timer.tv_nsec = int((timeout - timer.tv_sec)*1000000000)
        else:
            raise ValueError('receive timeout must be a float or int')

        if not isinstance(size, int):
            raise TypeError(f"Allocation size must be int, got type {type(size)}")

        # @MCB TODO: What do we want to make the minimum size?
        if size < 1:
            raise RuntimeError("Size cannot be less than 1 for memory allocations")

        # Assignment causes coercion, not allowed without gil
        sz = size

        # allocate with type
        if alloc_type is not None:

            if type(alloc_type) is AllocType:
                tyval = alloc_type.value
            else:
                raise DragonMemoryError(DRAGON_INVALID_ARGUMENT, 'alloc_type must be None or of type AllocType.')

            with nogil:
                derr = dragon_memory_alloc_type_blocking(&mem, &self._pool_hdl, sz, tyval, time_ptr)
        else:
            # allocate without type
            with nogil:
                derr = dragon_memory_alloc_blocking(&mem, &self._pool_hdl, sz, time_ptr)

        if derr == DRAGON_TIMEOUT:
            raise TimeoutError("The blocking allocation timed out")

        if derr == DRAGON_DYNHEAP_REQUESTED_SIZE_NOT_AVAILABLE:
            raise DragonPoolAllocationNotAvailable(derr, "A pool allocation of size={size} is not available.")

        if derr != DRAGON_SUCCESS:
            raise DragonPoolError(derr, "Could not perform allocation")

        mem_alloc_obj = MemoryAlloc.cinit(mem)
        # We had to move the error handling here, in the caller
        if not isinstance(mem_alloc_obj, MemoryAlloc):
            # if there was an error, the returned value is a tuple of the form: (derr, err_str)
            raise DragonMemoryError(mem_alloc_obj[0], mem_alloc_obj[1])
        return mem_alloc_obj

    def alloc_from_id(self, id):
        """
        Attach to a memory allocation given its unique identifier

        :return: New MemoryAlloc object
        """
        cdef:
            dragonError_t derr
            dragonMemoryDescr_t mem
            uint64_t id_val

        id_val = id
        with nogil:
            derr = dragon_memory_from_id(&self._pool_hdl, id_val, &mem)

        if derr != DRAGON_SUCCESS:
            raise DragonPoolError(derr, "Could not get memory allocation from id.")

        mem_alloc_obj = MemoryAlloc.cinit(mem)
        # We had to move the error handling here, in the caller
        if not isinstance(mem_alloc_obj, MemoryAlloc):
            # if there was an error, the returned value is a tuple of the form: (derr, err_str)
            raise DragonMemoryError(mem_alloc_obj[0], mem_alloc_obj[1])

        return mem_alloc_obj

    def get_allocations(self, alloc_type=None):
        """
        Get a list of allocations in this pool

        :return: New MemoryAllocations object
        """
        cdef:
            dragonError_t derr
            dragonMemoryPoolAllocations_t allocs
            dragonMemoryAllocationType_t tyval

        if alloc_type is None:
            with nogil:
                derr = dragon_memory_pool_get_allocations(&self._pool_hdl, &allocs)
            if derr != DRAGON_SUCCESS:
                raise DragonPoolError(derr, "Could not retrieve allocation list from pool")
        else:
            if type(alloc_type) is AllocType:
                tyval = alloc_type.value
            else:
                raise DragonMemoryError(DRAGON_INVALID_ARGUMENT, 'alloc_type must be None or of type AllocType.')

            with nogil:
                derr = dragon_memory_pool_get_type_allocations(&self._pool_hdl, tyval, &allocs)

        return MemoryAllocations.cinit(allocs)

    def allocation_exists(self, id):
        """
        Scan the pool to determine if a given allocation exists

        :param id: Integer ID of the allocation
        :return: True if allocation exists, False otherwise
        """
        cdef:
            dragonError_t derr
            dragonMemoryDescr_t mem_descr
            int flag
            uint64_t id_val

        id_val = id

        with nogil:
            derr = dragon_memory_get_alloc_memdescr(&mem_descr, &self._pool_hdl, id_val, 0, NULL)

        if derr == DRAGON_KEY_NOT_FOUND:
            return False

        if derr != DRAGON_SUCCESS:
            raise DragonPoolError(derr, "Error checking allocation existence")

        return True

    @property
    def is_local(self):
        return dragon_memory_pool_is_local(&self._pool_hdl)

    @property
    def muid(self):
        return self._muid

    @property
    def free_space(self):
        cdef:
            dragonError_t derr
            uint64_t sz

        with nogil:
            derr = dragon_memory_pool_get_free_size(&self._pool_hdl, &sz)
        if derr != DRAGON_SUCCESS:
            raise DragonPoolError(derr, "Could not retrieve the pool's free space.")

        return sz

    @property
    def size(self):
        cdef:
            dragonError_t derr
            uint64_t sz

        with nogil:
            derr = dragon_memory_pool_get_total_size(&self._pool_hdl, &sz)
        if derr != DRAGON_SUCCESS:
            raise DragonPoolError(derr, "Could not retrieve the pool's total size.")

        return sz

    @property
    def utilization(self):
        cdef:
            dragonError_t derr
            double pct

        with nogil:
            derr = dragon_memory_pool_get_utilization_pct(&self._pool_hdl, &pct)
        if derr != DRAGON_SUCCESS:
            raise DragonPoolError(derr, "Could not retrieve the pool's percent utilized space.")

        return pct

    @property
    def max_allocations(self):
        cdef:
            dragonError_t derr
            dragonMemoryPoolAttr_t attrs

        with nogil:
            derr = dragon_memory_get_attr(&self._pool_hdl, &attrs)
        if derr != DRAGON_SUCCESS:
            raise DragonPoolError(derr, "Could not retrieve the pool's attributes.")

        return attrs.max_allocations

    @property
    def max_used_allocations(self):
        cdef:
            dragonError_t derr
            dragonMemoryPoolAttr_t attrs

        with nogil:
            derr = dragon_memory_get_attr(&self._pool_hdl, &attrs)
        if derr != DRAGON_SUCCESS:
            raise DragonPoolError(derr, "Could not retrieve the pool's attributes.")

        return attrs.max_manifest_entries

    @property
    def current_allocations(self):
        cdef:
            dragonError_t derr
            dragonMemoryPoolAttr_t attrs

        with nogil:
            derr = dragon_memory_get_attr(&self._pool_hdl, &attrs)
        if derr != DRAGON_SUCCESS:
            raise DragonPoolError(derr, "Could not retrieve the pool's attributes.")

        return attrs.manifest_entries

    @property
    def num_block_sizes(self):
        """
        Return the total number of block sizes.

        :return: number of block sizes of the pool.
        """
        cdef:
            dragonError_t derr
            size_t num_block_sizes

        derr = dragon_memory_pool_get_num_block_sizes(&self._pool_hdl, &num_block_sizes)
        if derr != DRAGON_SUCCESS:
            raise DragonPoolError(derr, "Could not retrieve the pool's number of block sizes.")

        return num_block_sizes

    @property
    def free_blocks(self):
        """
        Return the number of free blocks with different block sizes.

        :return: A dictionary with keys as block sizes and values as the number of blocks with the size.
        """
        cdef:
            dragonError_t derr
            dragonHeapStatsAllocationItem_t *free_blocks

        num_block_sizes = self.num_block_sizes

        free_blocks = <dragonHeapStatsAllocationItem_t*>malloc(sizeof(dragonHeapStatsAllocationItem_t) * num_block_sizes)
        if free_blocks == NULL:
            raise DragonPoolError(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate space for stats.")

        with nogil:
            derr = dragon_memory_pool_get_free_blocks(&self._pool_hdl, free_blocks)
        if derr != DRAGON_SUCCESS:
            raise DragonPoolError(derr, "Could not retrieve the pool's free blocks.")

        free_blocks_map = {}
        for i in range(num_block_sizes):
            free_blocks_map[ free_blocks[i].block_size ] = free_blocks[i].num_blocks

        free(free_blocks)
        free_blocks = NULL

        return free_blocks_map

    @property
    def rt_uid(self):
        cdef:
            dragonError_t derr
            dragonULInt rtuid

        with nogil:
            derr = dragon_memory_pool_get_rt_uid(&self._pool_hdl, &rtuid)
        if derr != DRAGON_SUCCESS:
            raise DragonPoolError(derr, "Error getting pool's rt_uid")

        return rtuid

    # Debug use only by developers.
    @classmethod
    def _set_debug_flag(cls, flag):
        dragon_memory_set_debug_flag(flag)
