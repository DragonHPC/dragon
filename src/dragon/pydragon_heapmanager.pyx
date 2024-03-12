from dragon.dtypes_inc cimport *
import tempfile

cdef timespec_t* _compute_timeout(timeout, timespec_t* default_val_ptr, timespec_t* time_value):

    cdef:
        timespec_t* time_ptr

    if timeout is None:
        time_ptr = default_val_ptr
    elif isinstance(timeout, int) or isinstance(timeout, float):
        if timeout < 0:
            raise ValueError('Cannot provide timeout < 0.')

        # Anything > 0 means use that as seconds for timeout.
        time_ptr = time_value
        time_value.tv_sec = int(timeout)
        time_value.tv_nsec =  int((timeout - time_value.tv_sec)*1000000000)
    else:
        raise ValueError('The timeout value must be a float or int')

    return time_ptr

cdef class BitSet:
    """
    A Cython class interface for the malloc Bitset library
    """

    cdef dragonBitSet_t _set
    cdef void * _mem_ptr
    cdef _attach

    @staticmethod
    def init(const size_t num_bits, unsigned char [:] space):
        """
        Initialize the space with a BitSet of size num_bits.

        :param num_bits: The size of the BitSet.
        :param space: The space big enough to hold the BitSet.
        :return: The BitSet object
        """
        cdef dragonError_t derr
        cdef size_t set_size

        cdef BitSet bitset = BitSet()
        bitset._attach = False
        bitset._mem_ptr = <void*>&space[0]

        derr = dragon_bitset_init(bitset._mem_ptr, &bitset._set, num_bits)
        if derr != DRAGON_SUCCESS:
            bitset._handle_error(derr, "Could not initialize BitSet")

        return bitset

    @staticmethod
    def attach(unsigned char [:] space):
        cdef dragonError_t derr

        cdef BitSet bitset = BitSet()
        bitset._attach = True
        bitset._mem_ptr = <void*>&space[0]

        derr = dragon_bitset_attach(bitset._mem_ptr, &bitset._set)
        if derr != DRAGON_SUCCESS:
            bitset._handle_error(derr, "Could not attach BitSet")

        return bitset

    @staticmethod
    def size(const size_t num_bits):
        cdef dragonError_t derr
        cdef size_t size

        size = dragon_bitset_size(num_bits)

        return size


    def __dealloc__(self):
        if not self._attach:
            self.destroy()
        else:
            self.detach()


    def detach(self):
        cdef dragonError_t derr

        derr = dragon_bitset_detach(&self._set)
        if derr != DRAGON_SUCCESS:
            self._handle_error(derr, "Error detaching BitSet")

    def destroy(self):
        cdef dragonError_t derr

        derr = dragon_bitset_destroy(&self._set)
        if derr != DRAGON_SUCCESS:
            self._handle_error(derr, "Error destroying BitSet")

    def _handle_error(self, dragonError_t derr, err_msg):
        raise RuntimeError(err_msg + f" (Dragon Bitset error code={dragon_get_rc_string(derr)})")


    def get_num_bits(self):
        cdef dragonError_t derr
        cdef size_t num_bits

        derr = dragon_bitset_get_num_bits(&self._set, &num_bits)
        if derr != DRAGON_SUCCESS:
            self._handle_error(derr, "Could not get number of bits in set")

        return num_bits


    def get_bit(self, const size_t idx):
        cdef dragonError_t derr
        cdef unsigned char val

        derr = dragon_bitset_get(&self._set, idx, &val)
        if derr != DRAGON_SUCCESS:
            self._handle_error(derr, "Could not retrieve bitset")

        return val


    def set_bit(self, const size_t idx):
        cdef dragonError_t derr

        derr = dragon_bitset_set(&self._set, idx)
        if derr != DRAGON_SUCCESS:
            self._handle_error(derr, "Could not set bitset")


    def reset_bit(self, const size_t idx):
        cdef dragonError_t derr

        derr = dragon_bitset_reset(&self._set, idx)
        if derr != DRAGON_SUCCESS:
            self._handle_error(derr, "Could not reset bitset")


    def right_zeroes(self, const size_t idx):
        cdef dragonError_t derr
        cdef size_t val

        derr = dragon_bitset_zeroes_to_right(&self._set, idx, &val)
        if derr != DRAGON_SUCCESS:
            self._handle_error(derr, "Could not check rightmost zero count")

        return val

    def dump(self, title, indent):
        cdef dragonError_t derr

        # @MCB: This doesn't seem like the right way to pass strings around in cython
        derr = dragon_bitset_dump(<bytes>title, &self._set, <bytes>indent)
        if derr != DRAGON_SUCCESS:
            self._handle_error(derr, "Could not dump bitset")


    def dump_to_str(self, title, indent):
        cdef dragonError_t derr
        cdef FILE * fp

        tmp = tempfile.TemporaryFile()
        fp = fdopen(tmp.fileno(), 'w+b')

        derr = dragon_bitset_dump_to_fd(fp, title.encode('utf-8'), &self._set, indent.encode('utf-8'))
        if derr != DRAGON_SUCCESS:
            self._handle_error(derr, "Could not dump bitset to string")

        rewind(fp)
        output = tmp.read()
        tmp.close()

        return output.decode('utf-8')



cdef class MemStats:
    """Stats wrapper to provide python-accessible object"""

    cdef dragonHeapStats_t _stats

    # Update stats struct, only call internally from other Cython classes
    def update(self, Heap hdl):
        return dragon_heap_get_stats(&hdl._hdl, &self._stats)

    def free_block_count(self, free_block_idx):
        return self._stats.free_blocks[free_block_idx].num_blocks

    @property
    def num_segments(self):
        return self._stats.num_segments

    @property
    def segment_size(self):
        return self._stats.segment_size

    @property
    def total_size(self):
        return self._stats.total_size

    @property
    def num_block_sizes(self):
        return self._stats.num_block_sizes

    @property
    def utilization_pct(self):
        return self._stats.utilization_pct

cdef class Heap:
    """
    A cython class interface for the DynMem DynHeap Manager
    """

    # Pointer to memory heap
    cdef void * _mem_ptr
    # Size of the memory heap (Currently unused)
    cdef size_t _mem_size
    # Did we successfully initialize or attach the handle?  Used for __dealloc__ to prevent invalid detach/destroy calls
    cdef _valid
    # @MCB: Do we need max_list_sz here?
    cdef unsigned int max_list_sz
    # Handle struct
    cdef dragonDynHeap_t _hdl
    # Stats struct object so we can access it from Python space
    cdef MemStats _stats

    def __cinit__(self):
        self.max_list_sz = BLOCK_SIZE_MAX_POWER - BLOCK_SIZE_MIN_POWER + 1
        self._mem_size = 0
        self._stats = MemStats()
        self._valid = False

    def _handle_error(self, dragonError_t derr, err_msg):
        raise RuntimeError(err_msg + f" (Dragon DynHeap error code={dragon_get_rc_string(derr)})")

    @staticmethod
    def size(const size_t max_sz_pwr, const size_t min_sz_pwr, const size_t alignment, lock_kind) -> size_t:
        cdef dragonError_t derr
        cdef size_t mem_size

        derr = dragon_heap_size(max_sz_pwr, min_sz_pwr, alignment, lock_kind.value, &mem_size)
        if derr != DRAGON_SUCCESS:
            raise RuntimeError(f"Failed to get DynHeap size.  Dragon DynHeap error code=({dragon_get_rc_string(derr)})")

        return mem_size

    @staticmethod
    def init(const size_t max_sz_pwr, const size_t min_sz_pwr, const size_t alignment, lock_kind, unsigned char[:] memview):
        cdef Heap hdl
        cdef void * ptr = &memview[0]

        hdl = Heap()

        derr = dragon_heap_init(ptr, &hdl._hdl,
                                max_sz_pwr, min_sz_pwr, alignment, lock_kind.value, NULL)
        if derr != DRAGON_SUCCESS:
            hdl._handle_error(derr, "Could not initialize DynHeap!")

        hdl._valid = True
        hdl._mem_ptr = ptr
        return hdl


    # @MCB TODO: Do we want to also pass in the size of the memory heap?
    @staticmethod
    def attach(unsigned char [:] memview):
        cdef dragonError_t derr
        cdef Heap hdl
        cdef void * ptr = &memview[0]

        hdl = Heap()
        hdl._mem_ptr = ptr

        derr = dragon_heap_attach(hdl._mem_ptr, &hdl._hdl)
        if derr != DRAGON_SUCCESS:
            hdl._handle_error(derr, "Could not attach heap handle to provided memory space!")

        hdl._valid = True
        return hdl

    def free_lists(self, idx):
        """Returns whether a free_list pointer is NULL or not"""
        if idx < 0 or idx > self._hdl.num_freelists-1:
            raise RuntimeError("Out of bounds index request in free_lists")

        return not self._hdl.free_lists[idx]


    def detach(self):
        cdef dragonError_t derr

        derr = dragon_heap_detach(&self._hdl)
        if derr != DRAGON_SUCCESS:
            self._handle_error(derr, "Error detaching heap handle")


    def destroy(self):
        cdef dragonError_t derr

        derr = dragon_heap_destroy(&self._hdl)
        if derr != DRAGON_SUCCESS:
            self._handle_error(derr, "Error destroying heap handle")


    def malloc(self, size):
        cdef dragonError_t derr
        cdef void * ptr = NULL

        derr = dragon_heap_malloc(&self._hdl, size, &ptr)
        if derr != DRAGON_SUCCESS:
            self._handle_error(derr, "Error mallocing memory for memview")

        # NOTE: Read: 256, Write: 512
        return PyMemoryView_FromMemory(<char*>ptr, size, 512)

    def malloc_blocking(self, size, timeout):
        cdef:
            dragonError_t derr
            void * ptr = NULL
            timespec_t timer
            timespec_t * time_ptr

        time_ptr = _compute_timeout(timeout, NULL, &timer)

        derr = dragon_heap_malloc_blocking(&self._hdl, size, &ptr, time_ptr)
        if derr != DRAGON_SUCCESS:
            self._handle_error(derr, "Error mallocing memory for memview")

        # NOTE: Read: 256, Write: 512
        return PyMemoryView_FromMemory(<char*>ptr, size, 512)

    def free(self, unsigned char [:] memobj):
        cdef dragonError_t derr
        cdef void * ptr = &memobj[0]

        derr = dragon_heap_free(&self._hdl, ptr)
        if derr == DRAGON_DYNHEAP_RECOVERY_REQUIRED:
            raise RuntimeError("Recovery needed on heap handle")

        elif derr != DRAGON_SUCCESS:
            self._handle_error(derr, "Error freeing memview")

        del memobj

    def recover(self):
        cdef dragonError_t derr

        if self._hdl.recovery_needed_ptr[0]:
            derr = dragon_heap_recover(&self._hdl)
            if derr != DRAGON_SUCCESS:
                self._handle_error(derr, "Error making dragon recovery call")

        else:
            raise RuntimeError("Attempting to perform recovery on non-corrupt heap")

    def get_stats(self):
        cdef dragonError_t derr

        derr = self._stats.update(self)
        if derr != DRAGON_SUCCESS:
            self._handle_error(derr, "Error retrieving stats on heap")

        return self._stats

    def dump(self, title):
        cdef dragonError_t derr

        derr = dragon_heap_dump(title.encode('utf-8'), &self._hdl)
        if derr != DRAGON_SUCCESS:
            self._handle_error(derr, "Error when dumping heap info")


    def dump_to_str(self, title):
        cdef dragonError_t derr
        cdef FILE * fp

        # Generate a temporary file to get the fileno() handle
        tmp = tempfile.TemporaryFile()

        # Open the file
        fp = fdopen(tmp.fileno(), 'w+b')

        # Make the call
        derr = dragon_heap_dump_to_fd(fp, title.encode('utf-8'), &self._hdl)
        if derr != DRAGON_SUCCESS:
            self._handle_error(derr, "Error trying to dump heap info into temporary file")

        # Call rewind on FILE handle
        # For some reason this works, but tmp.seek(0) doesn't?
        rewind(fp)

        # Read back the heap dump from the temp file
        output = tmp.read()
        tmp.close()

        # Decode bytes to utf-8 string
        return output.decode('utf-8')

    def dump_to_file(self, title, fobj):
        raise RuntimeError("dump_to_file not yet implemented")

    @property
    def num_segments(self):
        return self._hdl.num_segments

    @property
    def segment_size(self):
        return self._hdl.segment_size

    @property
    def num_freelists(self):
        return self._hdl.num_freelists

    @property
    def recovery_needed(self):
        return self._hdl.recovery_needed_ptr[0]

    @property
    def exclusive_access(self):
        return <uintptr_t>self._hdl.base_pointer
