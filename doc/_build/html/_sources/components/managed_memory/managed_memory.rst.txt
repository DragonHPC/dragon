.. _ManagedMemory:

Managed Memory
++++++++++++++

Memory managed by the Dragon runtime/services consists of memory pools and allocations within those pools.
Pools are managed by :ref:`LocalServices` and can be backed by shared memory or a filesystem.  Pools can also
be entirely private to a process, but that type of memory cannot be used for inter-process communication.
Memory pools have many attributes that describe the type, how the memory is managed, and various other tuning
options.  Memory allocations from that pool then have the properties of the pool.

Memory pools also have the ability to grow based on demand. If they grow and how they grow is all defined by
the attributes of the pool.  All of the state for a memory pool and allocations within the pool is directly
encoded into the memory (as opposed to being tracked in a given process's memory).  This design allows for
further enhancements for fault tolerance, relocation of pools and allocations, and potential for concurrency
in operations.  All operations on the state are guarded by Dragon Locks.  Because of this, the API is
completely thread safe.

Another attribute for Dragon managed memory is that handles to allocations and pools come through descriptors.
These descriptors can be serialized and communicated to another process.  The receiving process can
immediately used the reconstructed objects.  Interactions with remote memory must involve another process that
can directly access the memory.  In the Dragon context this will often be the Shepherd.  Other services and
libraries directly interact with managed memory and pools.  Channels, for example, requires Dragon managed
memory.

Finally, there are many ways to misuse the memory API.  Using a pointer after the associated memory has been
released back to the pool, for example, is permitted with undefined results.  It is on the user to ensure the
safety of operating on memory that can be directly addressed and potentially released by other processes.

Architecture
============

.. figure:: images/managed_memory.svg

    **Figure 1: Architecture of the Managed Memory component**

**MISSING Description of Request Handler and Pools**

Allocations from managed memory pools is implemented using a custom buddy allocator, or :ref:`HeapManager`.
This custom allocator partitions a segment of memory and hands out portions of the memory through an
"allocation".  Dragon managed memory uses a hierarchy of heap managers to implement allocations.  Bitset and
hexdump are support utilities for the heap manager.

.. toctree::
    :glob:
    :maxdepth: 1

    heapmanager.rst
    bitset.rst
    hexdump.rst

Cython Adapters
===============

We provide Cython adapters to the :ref:`HeapManager` and :ref:`ManagedMemory`:

.. toctree::
    :glob:
    :maxdepth: 1

    cy_heapmanager.rst

.. TBD - The information from here to the end of this file needs to be merged into the
.. managed memory header file and C source file in the restructuring of documentation.
.. It will be documented in the public facing Core API reference unless there is
.. specs here that are not implemented.

.. Managed Memory API
.. ==================

.. Structures
.. ----------

.. Public
.. """"""

.. .. c:enum:: dragonMemoryPoolType_t

..     .. c:enumerator:: DRAGON_MEMORY_TYPE_SHM

..         Memory allocated through shared memory.

..     .. c:enumerator:: DRAGON_MEMORY_TYPE_FILE

..         Memory allocated through a file.  The file does not need to reside in a globally accessible
..         filesystem.

..     .. c:enumerator:: DRAGON_MEMORY_TYPE_PRIVATE

..         Memory is allocated privately in the virtual address space of the requesting process.  A use case for
..         this is threads using Channels for communication in a single process.

.. .. c:enum:: dragonMemoryPoolGrowthType_t

..     .. c:enumerator:: DRAGON_MEMORY_GROWTH_NONE

..         The size of the memory is frozen.

..     .. c:enumerator:: DRAGON_MEMORY_GROWTH_UNLIMITED

..         The size of the memory can grow beyond the initial request without any upper bound.

..     .. c:enumerator:: DRAGON_MEMORY_GROWTH_CAPPED

..         The size of the memory can grow beyond the initial request but is capped at a specified value.

.. .. c:enum:: dragonMemoryPoolExecutable_t

..     .. c:enumerator:: DRAGON_MEMORY_EXECUTABLE

..         Memory will be mapped in as executable as well as read/write.

..     .. c:enumerator:: DRAGON_MEMORY_NOT_EXECUTABLE

..         Memory will be mapped in read/write only.

.. .. doxygenenum:: dragonMemoryAllocationType_t

.. .. c:struct:: dragonMemoryPoolAttr_t

..     .. c:var:: size_t allocatable_data_size

..         Ignored if set by user. The size of the allocated data memory pool.

..     .. c:var:: size_t total_data_size

..         Ignored if set by user. The total size of the pool's original data segment.

..     .. c:var:: size_t data_min_block_size

..         The minimum block size that can be allocated from the data memory pool.

..     .. c:var:: size_t manifest_allocated_size

..         The size of the allocated manifest memory pool.

..     .. c:var:: size_t seg_size

..         The size of each additional segment to allocate for memory that is allowed to grow.  Defaults
..         to 128 MB.

..     .. c:var:: size_t max_size

..         The maximum size memory can grow to if it is capped. This is the maximum number of bytes across all the segments.

..     .. c:var:: size_t n_segs

..         Ignored if set by the user.  The :c:struct:`dragonMemoryAttr_t` struct returned by
..         *dragon_memory_pool_getattr()* will have this set to the number of memory segments used to provide the
..         memory pool.

..     .. c:var:: size_t data_alignment

..         The alignment to use, in number of bytes, for all allocations within the heap.

..     .. c:var:: dragonLockKind_t lock_type

..         The type of lock to use for management (e.g., sub-allocations) from the memory.

..     .. c:var:: dragonMemoryPoolType_t mem_type

..         The type of memory for the allocation.  The default is ``DRAGON_MEMORY_TYPE_SHM``.

..     .. c:var:: dragonMemoryPoolGrowthType_t growth_type

..         The growth type for the memory.  Defaults to ``DRAGON_MEMORY_GROWTH_NONE``.

..     .. c:var:: mode_t mode

..         The access mode for the allocated memory.  Defaults to 0600.

.. ..    .. c:var:: dragonMemoryPoolExecutable_t prot_exe
.. ..
.. ..        Set to ``DRAGON_MEMORY_EXECUTABLE`` to make the memory executable.  Defaults to ``DRAGON_MEMORY_NOT_EXECUTABLE``.
.. ..
.. ..    .. c:var:: dragonShepID_t shep_id
.. ..
.. ..        The identifier of the shepherd process that memory should be local and allocated through.  Defaults to the local
.. ..        shepherd.  This value is ignored if *mem_type* is set to ``DRAGON_MEMORY_TYPE_PRIVATE``.
.. ..
.. ..    .. c:var:: bitmask * nodemask
.. ..
.. ..        If set to a valid NUMA node mask, as allocated by *numa_allocate_nodemask()* and with bits set with a function
.. ..        such as *numa_bitmask_setbit()*, memory will be interleaved page-by-page across set NUMA nodes.  This is done with
.. ..        *numa_interleave_memory()* and then faulting in the memory pool.  If *nodemask* is instead set to ``NULL`` then no
.. ..        policy is set and the memory is **not** faulted in.  NUMA affinity can then instead be set elsewhere after the
.. ..        memory pool is created but before any allocations happen within it.
.. ..
..     .. c:var:: char * mname

..         Name of the file to store the manifest data in.

..     .. c:var:: char ** names

..         Ignored if set by the user.  The :c:struct:`dragonMemoryAttr_t` struct returned by *dragon_memory_pool_getattr()*
..         will have this set to an array, of size *n_seg*, of null-terminated strings with the names of each file or shared
..         memory segment used to provide the pool.

.. .. c:struct:: dragonMemoryPoolRequest_t

..     .. c:var:: size_t len

..         The number of bytes in the *data* member.

..     .. c:var:: uint8_t * data

..         The request payload.

.. .. c:struct:: dragonMemoryPoolResponse_t

..     .. c:var:: size_t len

..         The number of bytes in the *data* member.

..     .. c:var:: uint8_t * data

..         The response payload.


.. .. c:struct:: dragonMemoryRequest_t

..     .. c:var:: size_t len

..         The number of bytes in the *data* member.

..     .. c:var:: uint8_t * data

..         The request payload.

.. .. c:struct:: dragonMemoryResponse_t

..     .. c:var:: size_t len

..         The number of bytes in the *data* member.

..     .. c:var:: uint8_t * data

..         The response payload.

.. .. c:struct:: dragonMemoryPoolSerial_t

..         .. c:var:: size_t len

..         The number of bytes in the *data* member.

..     .. c:var:: uint8_t * data

..         The payload of the serialized memory pool descriptor.

.. .. c:struct:: dragonMemorySerial_t

..     .. c:var:: size_t len

..         The number of bytes in the *data* member.

..     .. c:var:: uint8_t * data

..         The payload of the serialized memory descriptor.

.. .. c:struct:: dragonMemoryPoolAllocations_t

..     .. c:var:: dragonULInt nallocs

..         The number of allocations currently active in the memory pool

..     .. c:var:: dragonMemoryAllocationType_t * types

..         Array of the type of each active allocation

..     .. c:var:: dragonULInt * ids

..         Array of the ID number of each active allocation


.. Internal
.. """"""""

.. .. c:struct:: dragonMemoryPoolHeader_t

.. .. c:struct:: dragonMemoryPoolHeap_t

..    .. c:var:: uint32_t nmgrs

..       Number of heap managers in the pool

..    .. c:var:: size_t * cutoffs

..    .. c:var:: void * mfst_dptr

..       Hashtable used for the manifest

..    .. c:var:: void ** mgrs_dpts

..       Array of memory pointers used by the heap managers

..    .. c:var:: dragonDynHeap_t * mgrs

..       Array of heap manager handles used for data pools

..    .. c:var:: dragonDynHeap_t mfstmgr

..       Manifest hashtable handle


.. .. c:struct:: dragonMemoryPool_t

..    .. c:var:: int dfd

..       Data file descriptor

..    .. c:var:: int mfd

..       Manifest file descriptor

..    .. c:var:: size_t data_requested_size

..    .. c:var:: size_t manifest_requested_size

..       The maximum number of manifest records

..    .. c:var:: void * dptr

..       Points to the memory used by this pool for data allocations

..    .. c:var:: void * mptr

..       Points to the memory used by this pool for manifest operations

..    .. c:var:: dragonMemoryPoolHeap_t heap

..    .. c:var:: dragonMemoryPoolHeader_t header

..    .. c:var:: dragonLock_t mlock

..       Dragon lock for accessing/reading the pool manifest.

..    .. c:var:: char * mname

..       Filename for the pool manifest

..    .. c:var:: atomic_int_fast64_t ref_cnt

..       Reference counter for attach/detaches

.. .. c:struct:: dragonMemoryManifestRec_t

..    Structure used to track allocations, stored in the manifest.

..    .. c:var:: dragonULInt offset

..       Memory offset to the pointer in the :c:struct:`dragonMemoryPool_t` *dptr* for the given allocation.

..    .. c:var:: dragonULInt size

..       Size of the allocation.

..    .. c:var:: dragonULInt alloc_type

..       :c:enumerator:`dragonMemoryAllocationType_t` type of the allocation.

..    .. c:var:: dragonULInt alloc_type_id

..       Unique ID number associated with the allocation for lookups.

.. .. c:struct:: dragonMemory_t

..    Structure used for allocations

..    .. c:var:: size_t bytes

..       Size of the allocation in bytes

..    .. c:var:: void * dptr

..       Pointer to the allocation in the `dragonMemoryPool_t` *dptr*.

..    .. c:var:: void * mptr

..       Pointer to manifest record information held in `dragonMemoryPool_t` *mptr*

..    .. c:var:: dragonMemoryPoolDescr_t pool_descr

..       Locally-held copy of the pool descriptor this allocation belongs to

..    .. c:var:: dragonMemoryManifestRec_t mfst_record

..       Locally-held copy of the manifest record describing this allocation

.. Functions
.. ---------

.. Memory Pools
.. """"""""""""

.. .. doxygenfunction:: dragon_memory_pool_create


.. .. c:function:: dragonError_t dragon_memory_pool_create_gen_req(dragonMemoryPoolRequest_t * pool_req, size_t bytes, const char * base_name, const dragonMemoryPoolAttr_t * attr)

..     Update the :c:struct:`dragonMemoryPoolRequest_t` request structure *pool_req* for creating a new memory
..     pool.  The *data* member of *pool_req* is updated to point to an allocated :c:type:`uint8_t` blob with the
..     request *message encoded and the *len* member set to the number of bytes in the *data* blob upon return.
..     The caller can communicate the *data* member of *pool_req* with another process that can service the
..     request.  The managed memory will be allocated from memory directly accessible by that process.

..     See :c:func:`dragon_memory_pool_create()` for details on the other arguments.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonError_t dragon_memory_pool_create_cmplt(dragonMemoryPoolDescr_t * pool_descr, const dragonMemoryPoolResponse_t * pool_resp)

..     Complete creation of a memory pool processed through a request.  *pool_resp* is the result from a call to
..     :c:func:`dragon_memory_pool_exec_req()` (possibly on a remote process) with the
..     :c:struct:`dragonMemoryPoolRequest_t` *pool_req* argument coming from a call to
..     :c:func:`dragon_memory_pool_create_gen_req()`.  *pool_descr* is *updated for use in all other actions with
..     the memory pool upon success.  If the call to :c:func:`dragon_memory_pool_exec_req()` failed this call
..     will return an error.  An error will occur if *pool_resp* originated from a request other than creating a
..     memory pool.

..     The caller is responsible for freeing memory associated with any :c:struct:`dragonMemoryPoolRequest_t` or
..     :c:struct:`dragonMemoryPoolResponse_t` variables with :c:func:`dragon_memory_pool_free_req()` or
..     :c:func:`dragon_memory_pool_free_resp()`.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. doxygenfunction:: dragon_memory_pool_is_local

.. .. doxygenfunction:: dragon_memory_pool_destroy

.. .. c:function:: dragonError_t dragon_memory_pool_destroy_gen_req(dragonMemoryPoolRequest_t * pool_req, dragonMemoryPoolDescr_t * pool_descr)

..     Update the :c:struct:`dragonMemoryPoolRequest_t` request structure *pool_req* for destroying an existing
..     memory pool.  The *data* member of *pool_req* is updated to point to an allocated :c:type:`uint8_t` blob
..     with the request message encoded and the *len* member set to the number of bytes in the *data* blob upon
..     return.  The caller can communicate the *data* member of *pool_req* with another process that can service
..     the request.  That process must be able to directly access the memory described in the Pool descriptor
..     argument, *pool_descr*.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonError_t dragon_memory_pool_destroy_cmplt(const dragonMemoryPoolResponse_t * pool_resp)

..     Complete destruction of a memory pool processed through a request.  *pool_resp* is the result from a call
..     to :c:func:`dragon_memory_pool_exec_req()` (possibly on a remote process) with the
..     :c:struct:`dragonMemoryPoolRequest_t` *pool_req* argument coming from a call to
..     :c:func:`dragon_memory_pool_destroy_req()`.  If the call to :c:func:`dragon_memory_pool_exec_req()` failed
..     this call will return an error.  An error will occur if *pool_resp* originated from a request other than
..     destroying a memory pool.

..     The caller is responsible for freeing memory associated with any :c:struct:`dragonMemoryPoolRequest_t` or
..     :c:struct:`dragonMemoryPoolResponse_t` variables with :c:func:`dragon_memory_pool_free_req()` or
..     :c:func:`dragon_memory_pool_free_resp()`.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: size_t dragon_memory_pool_max_serialized_len()

..     Return the space required to store a serialized memory pool descriptor. This will return
..     a size big enough for any serialized memory pool descriptor.

.. .. doxygenfunction:: dragon_memory_pool_serialize

.. .. c:function:: dragonError_t dragon_memory_pool_serial_free(dragonMemoryPoolSerial_t * pool_ser)

..     Clean up the components of *pool_ser*.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonError_t dragon_memory_pool_attach(dragonMemoryPoolDescr_t * pool_descr, const dragonMemoryPoolSerial_t * pool_ser)

..     Takes a serialized pool descriptor and attempts to attach to it, populating the *pool_descr* struct.
..     :c:func:`dragon_memory_pool_detach()` must be used to clean up the resulting descriptor.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonError_t dragon_memory_pool_detach(dragonMemoryPoolDescr_t * pool_descr)

..     Cleans up the components of *pool_descr*

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. doxygenfunction:: dragon_memory_pool_descr_clone

.. .. c:function:: dragonError_t dragon_memory_pool_get_allocations(dragonMemoryPoolDescr_t * pool_descr, dragonMemoryPoolAllocations_t * allocs)

..     Populates the *allocs* structure with a list of all active memory allocations in the current pool.
..     :c:func:`dragon_memory_pool_allocations_destroy` must be called to free allocated resources.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonError_t dragon_memory_pool_get_type_allocations(dragonMemoryPoolDescr_t * pool_descr, dragonMemoryAllocationType_t type, dragonMemoryPoolAllocations_t * allocs)

..     Populates the *allocs* structure with a list of all active memory allocations of specified *type* in the
..     current pool.  :c:func:`dragon_memory_pool_allocations_destroy` must be called to free allocated resources.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonError_t dragon_memory_pool_allocation_exists(dragonMemoryPoolDescr_t * pool_descr, dragonMemoryAllocationType_t type, dragonULInt type_id, int * flag)

..     Searches the manifest of the provided *pool_descr* for an allocation matching both *type* and *type_id*.
..     Sets *flag* to 1 on success, 0 on failure.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonError_t dragon_memory_pool_allocations_destroy(dragonMemoryPoolAllocations_t * allocs)

..     Releases resources used by the :c:struct:`dragonMemoryPoolAllocations_t` struct.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. Memory Pool Attributes
.. """"""""""""""""""""""

.. .. c:function:: dragonError_t dragon_memory_attr_init(dragonMemoryPoolAttr_t * attr)

..     Initialize *attr* with default values.  Users should call this on any :c:struct:`dragonMemoryPoolAttr_t`
..     structure intended to be passed for memory allocation functions.  Otherwise, the user must completely fill
..     out the structure.

.. .. c:function:: dragonError_t dragon_memory_pool_setattr(dragonMemoryPoolDescr_t * pool_descr, const dragonMemoryPoolAttr_t * newattr, dragonMemoryPoolAttr_t * oldattr)

..     Modify the attributes of the memory pool *pool_descr* as specified in *newattr* and return the old
..     attributes are returned in *oldattr*.  The values for *shep_id*, *mem_type*, and *lock_type* cannot be
..     modified.  Those values in *newattr* will be ignored.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonError_t dragon_memory_pool_setattr_gen_req(dragonMemoryPoolRequest_t * pool_req, dragonMemoryPoolDescr_t * pool_descr, const dragonMemoryPoolAttr_t * newattr)

..     Update the :c:struct:`dragonMemoryPoolRequest_t` request structure *pool_req* for setting attributes on a
..     memory pool.  The *data* member of *pool_req* is updated to point to an allocated :c:type:`uint8_t` blob
..     with the request *message    encoded and the *len* member set to the number of bytes in the *data* blob
..     upon return.  The caller can communicate the *data* member of *pool_req* with another process that can
..     service the request.  The managed memory will be allocated from memory directly accessible by that
..     process.

..     See :c:func:`dragon_memory_setattr()` for details on the other arguments.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonError_t dragon_memory_pool_setattr_cmplt(const dragonMemoryPoolResponse_t * pool_resp, dragonMemoryPoolAttr_t * oldattr)

..     Complete setting attributes on a memory pool processed through a request.  *pool_resp* is the result from
..     a call to :c:func:`dragon_memory_pool_exec_req()` (possibly on a remote process) with the
..     :c:struct:`dragonMemoryPoolRequest_t` *pool_req* argument coming from a call to
..     :c:func:`dragon_memory_pool_setattr_gen_req()`.  If the call to *:c:func:`dragon_memory_pool_exec_req()`
..     failed this call will return an error.  An error will occur if *pool_resp* originated from a request other
..     than setting attributes for a memory pool.

..     The caller is responsible for freeing memory associated with any :c:struct:`dragonMemoryPoolRequest_t` or
..     :c:struct:`dragonMemoryPoolResponse_t` variables with :c:func:`dragon_memory_pool_free_req()` or
..     :c:func:`dragon_memory_pool_free_resp()`.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonError_t dragon_memory_pool_getattr(dragonMemoryPoolDescr_t * pool_descr, dragonMemoryAttr_t * attr)

..     Retrieve the attributes from the memory pool *pool_descr* and return them in *attr*.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonError_t dragon_memory_pool_getattr_gen_req(dragonMemoryPoolRequest_t * pool_req, dragonMemoryPoolDescr_t * pool_descr)

..     Update the :c:struct:`dragonMemoryPoolRequest_t` request structure *pool_req* for getting attributes of a
..     memory pool.  The *data* member of *pool_req* is updated to point to an allocated :c:type:`uint8_t` blob
..     with the request message encoded and the *len* member set to the number of bytes in the *data* blob upon
..     return.  The caller can communicate the *data* member of *pool_req* with another process that can service
..     the request.  The managed memory will be allocated from memory directly accessible by that process.

..     See :c:func:`dragon_memory_pool_getattr()` for details on the other arguments.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonError_t dragon_memory_pool_getattr_cmplt(dragonMemoryPoolRequest_t * pool_req, dragonMemoryAttr_t * attr)

..     Complete getting attributes of a memory pool processed through a request.  *pool_resp* is the result from a call to
..     :c:func:`dragon_memory_pool_exec_req()` (possibly on a remote process) with the :c:struct:`dragonMemoryPoolRequest_t`
..     *pool_req* argument coming from a call to :c:func:`dragon_memory_pool_getattr_gen_req()`.  If the call to :c:func:`dragon_memory_pool_exec_req()`
..     failed this call will return an error.  An error will occur if *pool_resp* originated from a request other than
..     getting attributes from a memory pool.

..     The caller is responsible for freeing memory associated with any :c:struct:`dragonMemoryPoolRequest_t` or
..     :c:struct:`dragonMemoryPoolResponse_t` variables with :c:func:`dragon_memory_pool_free_req()` or
..     :c:func:`dragon_memory_pool_free_resp()`.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. Memory Allocations
.. """"""""""""""""""

.. .. c:function:: dragonError_t dragon_memory_alloc(dragonMemoryDescr_t * mem_descr, dragonMemoryPoolDescr_t * pool_descr, size_t bytes)

..     Returns an allocation of size *bytes* described in *mem_descr* from the memory pool provided in
..     *pool_descr*. *mem_descr* can be serialized and shared with other processes they may want to place Channels or other
..     objects on top of it.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonError_t dragon_memory_alloc_blocking(dragonMemoryDescr_t * mem_descr, dragonMemoryPoolDescr_t * pool_descr, size_t bytes, timespec_t* timeout)

..     Returns an allocation of size *bytes* described in *mem_descr* from the memory pool provided in
..     *pool_descr*.
..     *mem_descr* can be serialized and shared with other processes they may want to place Channels or other
..     objects on top of it. If timeout is NULL, then it blocks until a block satisfying bytes size is available. Otherwise
..     it blocks until available or until timeout seconds expire, whichever comes first.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonError_t dragon_memory_alloc_type(dragonMemoryDescr_t * mem_descr, dragonMemoryPoolDescr_t * pool_descr, size_t bytes, dragonMemoryAllocationType_t type, dragonULInt type_id)

..     Returns an allocation of size *bytes* with memory type *type* and ID number *type_id* *described in
..     *mem_descr* from the memory pool provided in *pool_descr*. *mem_descr* can be serialized and shared with
..     other processes they may want to place Channels or other objects on top of it.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonError_t dragon_memory_alloc_type_blocking(dragonMemoryDescr_t * mem_descr, dragonMemoryPoolDescr_t * pool_descr, size_t bytes, dragonMemoryAllocationType_t type, dragonULInt type_id, timespec_t* timeout)

..     Returns an allocation of size *bytes* with memory type *type* and ID number *type_id* *described in
..     *mem_descr* from the memory pool provided in *pool_descr*. *mem_descr* can be serialized and shared with
..     other processes they may want to place Channels or other objects on top of it. If timeout is NULL, then it
..     blocks until a block satisfying bytes size is available. Otherwise it blocks until available or until
..     timeout seconds expire, whichever comes first.

..     Returns ``DRAGON_SUCCESS`` or an error code.

..     .. c:function:: dragonError_t dragon_memory_alloc_gen_req(dragonMemoryRequest_t * mem_req, dragonMemoryPoolDescr_t * pool_descr, size_t bytes)

..     Update the :c:struct:`dragonMemoryRequest_t` request structure *mem_req* for allocating memory from a
..     memory pool.  The *data* member of *mem_req* is updated to point to an allocated :c:type:`uint8_t` blob
..     with the request message encoded and the *len* member set to the number of bytes in the *data* blob upon
..     return.  The caller can communicate the *data* member of *mem_req* with another process that can service
..     the request.

..     See :c:func:`dragon_memory_alloc()` for details on the other arguments.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonError_t dragon_memory_alloc_cmplt(const dragonMemoryResponse_t * mem_resp)

..     Complete allocating memory back from a memory pool as processed through a request.  *mem_resp* is the
..     result from a call to :c:func:`dragon_memory_exec_req()` (possibly on a remote process) with the
..     :c:struct:`dragonMemoryRequest_t` *mem_req* argument coming from a call to
..     :c:func:`dragon_memory_alloc_gen_req()`.  If the call to :c:func:`dragon_memory_exec_req()` failed this
..     call will return an error.  An error will occur if *mem_resp* originated from a request other than
..     allocating memory.

..     The caller is responsible for freeing memory associated with any :c:struct:`dragonMemoryRequest_t` or
..     :c:struct:`dragonMemoryResponse_t` variables with :c:func:`dragon_memory_free_req()` or
..     :c:func:`dragon_memory_free_resp()`.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonError_t dragon_memory_get_pointer(dragonMemoryDescr_t * mem_descr, void ** ptr)

..     Attempt to resolve the memory descriptor *mem_descr* to a valid pointer.  If successful *ptr* is updated
..     with the valid address.  On failure *ptr* is set to NULL and an error code is returned.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonError_t dragon_memory_descr_clone(dragonMemoryDescr_t * newmem_descr, const dragonMemoryDescr_t * oldmem_descr, ptrdiff_t head_diff)

..     Make a clone of *oldmem_descr* with head of the memory adjusted by *head_diff* and return the result in
..     *newmem_descr*. Cloned memory descriptors cannot be used to release memory back to the pool.  Only the
..     original descriptor can be used for that.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonError_t dragon_memory_descr_isoriginal(const dragonMemoryDescr_t * mem_descr, int * original)

..     If the provided memory descriptor *mem_descr* is the original memory descriptor obtained with
..     *dragon_memory_alloc* then *original* is set to 1.  If it is a clone or not a valid memory descriptor,
..     *original* is set to 0.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonError_t dragon_memory_free(dragonMemoryDescr_t * mem_descr)

..     Releases the memory described by *mem_descr* back to the pool it was allocated from.  Any pointers to the
..     memory obtained with *dragon_memory_get_pointer()* must be cleaned up by the user.  Otherwise further use
..     of that pointer may result in silent memory corruption for later allocations that overlap with that
..     address.  Any copies of *mem_descr* still in existence are no longer valid.  Attempts to use those
..     descriptors will fail with an *error.  Returns ``DRAGON_SUCCESS`` or an error code.

..     .. c:function:: dragonError_t dragon_memory_free_gen_req(dragonMemoryRequest_t * mem_req, dragonMemoryDescr_t * mem_descr)

..     Update the :c:struct:`dragonMemoryRequest_t` request structure *mem_req* for releasing memory back to a
..     memory pool.  The *data* member of *mem_req* is updated to point to an allocated :c:type:`uint8_t` blob
..     with the request message encoded and the *len* member set to the number of bytes in the *data* blob upon
..     return.  The caller can communicate the *data* member of *mem_req* with another process that can service
..     the request.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonError_t dragon_memory_free_cmplt(const dragonMemoryResponse_t * mem_resp)

..     Complete releasing memory back to a memory pool as processed through a request.  *mem_resp* is the result
..     from a call to :c:func:`dragon_memory_exec_req()` (possibly on a remote process) with the
..     :c:struct:`dragonMemoryRequest_t` *mem_req* argument coming from a call to
..     :c:func:`dragon_memory_free_gen_req()`.  If the call to :c:func:`dragon_memory_exec_req()` failed this
..     call will return an error.  An error will occur if *mem_resp* originated from a request other than freeing
..     memory.

..     The caller is responsible for freeing memory associated with any :c:struct:`dragonMemoryRequest_t` or
..     :c:struct:`dragonMemoryResponse_t` variables with :c:func:`dragon_memory_free_req()` or
..     :c:func:`dragon_memory_free_resp()`.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: size_t dragon_memory_max_serialized_len()

..     Return the space required to store a serialized memory descriptor. This will return
..     a size big enough for any serialized memory descriptor.

.. .. c:function:: dragonError_t dragon_memory_serialize(dragonMemorySerial_t * mem_ser, const dragonMemoryDescr_t * mem_descr)

..     Serialize the memory descriptor *mem_descr* into a :c:struct:`dragonMemorySerial_t`, *mem_ser*, that can be
..     communicated with another process.  :c:func:`dragon_memory_serial_free()` must be used to clean up the components
..     of *mem_ser* once it is no longer needed.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonError_t dragon_memory_serial_free(dragonMemorySerial_t * mem_ser)

..     Clean up the components of *mem_ser*.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonError_t dragon_memory_attach(dragonMemoryDescr_t * mem_descr, dragonMemorySerial_t * mem_ser)

..     Takes a serialized memory descriptor *mem_ser* and attempts to attach to the allocation, storing its
..     description is *mem_descr*.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonError_t dragon_memory_get_size(dragonMemoryDescr_t * mem_descr, size_t * bytes)

..     Looks up the memory allocation described by *mem_descr* and stores the allocation size in *bytes*.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. Managed Memory Request Handling Functions
.. """""""""""""""""""""""""""""""""""""""""

.. .. c:function:: dragonError_t dragon_memory_pool_exec_req(dragonMemoryPoolResponse * pool_resp, const dragonMemoryPoolRequest * pool_req)

..     Execute the operations in the :c:struct:`dragonMemoryPoolRequest` request object *pool_req*.  The
..     operations will be performed using local resources (e.g., memory).  Return a
..     :c:struct:`dragonMemoryPoolResponse` response object that can be communicated back to the process that
..     generated *pool_req*.  Any errors that occur executing the request will be encoded into *pool_resp* and
..     therefore detectable by the requesting process.

..     The caller is responsible for freeing memory associated with any :c:struct:`dragonMemoryPoolRequest_t` or
..     :c:struct:`dragonMemoryPoolResponse_t` variables with :c:func:`dragon_memory_pool_free_req()` or
..     :c:func:`dragon_memory_pool_free_resp()`.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonError_t dragon_memory_pool_free_req(dragonMemoryPoolRequest * pool_req)

..     Free the memory associated with *pool_req*.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonError_t dragon_memory_pool_free_resp(dragonMemoryPoolResponse * pool_resp)

..     Free the memory associated with *pool_resp*.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonError_t dragon_memory_exec_req(dragonMemoryResponse * mem_resp, const dragonMemoryRequest * mem_req)

..     Execute the operations in the :c:struct:`dragonMemoryRequest` request object *mem_req*.  The operations
..     will be performed using local resources (e.g., memory).  Return a :c:struct:`dragonMemoryResponse`
..     response object that can be communicated back to the process that generated *mem_req*.  Any errors that
..     occur executing the request will be encoded into *mem_resp* and therefore detectable by the requesting
..     process.

..     The caller is responsible for freeing memory associated with any :c:struct:`dragonMemoryRequest_t` or
..     :c:struct:`dragonMemoryResponse_t` variables with :c:func:`dragon_memory_free_req()` or
..     :c:func:`dragon_memory_free_resp()`.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonError_t dragon_memory_free_req(dragonMemoryRequest * mem_req)

..     Free the memory associated with *mem_req*.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonError_t dragon_memory_free_resp(dragonMemoryResponse * mem_resp)

..     Free the memory associated with *mem_resp*.

..     Returns ``DRAGON_SUCCESS`` or an error code.