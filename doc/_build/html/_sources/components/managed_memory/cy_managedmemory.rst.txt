.. _ManagedMemoryCython:

`dragon.managed_memory` -- Managed Memory Cython Adapter
++++++++++++++++++++++++++++++++++++++++++++++++++++++++

This page documents the Cython interface for the Dragon Managed Memory library.  For a description of Managed
Memory, Pools, and general functionality please refer to :ref:`this document<ManagedMemory>`.

All functions and variables listed as C language are implemented with the *cdef* keyword and are not directly
accessible from python-space.

Factory init methods are used in place of pythonic constructors as they require Python object arguments, while
some instances require the use of C structs.

Objects
=======

MemoryAlloc
-----------

.. py:class:: MemoryAlloc

   Python object for managing allocations made in the pool.

   .. c:var:: dragonMemoryDescr_t mem_descr

      Memory descriptor, used internally.

   .. c:var:: size_t mem_size

      Size of the allocated memory.  Used for returning memoryviews.

   .. c:method:: object cinit(dragonMemoryDescr_t mem_descr, size_t mem_size)

      NOTE: Static function.
      Creates a new `MemoryAlloc` object and populates struct values.
      Returns said object.

   .. c:method:: dragonMemoryDescr_t get_descr()

      Returns the memory descriptor held by this object.

   .. py:staticmethod:: attach(serialized_bytes)

      Attaches a new MemoryAlloc object to an existing serialized memory descriptor.
      Must be explicitly cleaned up using the *detach* method.

      :param serialized_bytes: Bytes object containing a serialized memory descriptor
      :return: MemoryAlloc object
      :rtype: MemoryAlloc
      :raises: RuntimeError

   .. py:method:: get_memview()

      Returns a memoryview instance of the underlying memory allocation.

      :return: memoryview
      :rtype: memoryview
      :raises: RuntimeError

   .. py:method:: serialize()

      Serializes the underlying memory descriptor and returns it as a bytes object.

      :return: bytes object of the serialized descriptor
      :rtype: bytes
      :raises: RuntimeError

   .. py:method:: detach()

      Detaches from the underlying memory.

      :raises: RuntimeError

MemoryAllocations
-----------------

.. py:class:: MemoryAllocations

   Python object for retrieving and searching through a current list of allocations in the pool manifest.

   .. c:var:: dragonMemoryPoolAllocations_t allocs

      Underlying C struct

   .. c:method:: object cinit(dragonMemoryPoolAllocations_t allocs)

      NOTE: Static function.
      Populates the underlying C struct.
      Returns a new MemoryAllocations object.

   .. py:method:: alloc_type(idx)

      Returns the type of the given allocation.

      :return: AllocType of the specified allocation index
      :rtype: AllocType
      :raises: RuntimeError (index out of bounds)

   .. py:method:: alloc_id(idx)

      Return the unique ID of the given allocation

      :return: ID of the specified allocation
      :rtype: int
      :raises: RuntimeError (index out of bounds)

MemoryPool
----------

.. py:class:: MemoryPool

   Python object for creating, attaching to, etc. pools.  Memory allocations and frees are performed through
   this object, as well as retrieving a list of allocations through a MemoryAllocations object.

   .. c:var:: dragonMemoryPoolDescr_t _pool_hdl

      Internal C struct pool descriptor handle.

   .. c:method:: object cinit(size, str fname, uid, mattr=None)

      Create a new pool of *size* with filename handle *fname*.  Returns a new MemoryPool object on success.
      Cleaning requires an explicit call to `destroy`.

      :param size: Size in bytes to allocate for the pool
      :param fname: String name to use for the pool filename
      :param uid: Unique identifier for the pool
      :param mattr: Memory pool attributes object.  Currently unused.
      :return: MemoryPool object with a new allocated pool
      :rtype: MemoryPool
      :raises: DragonPoolCreateFail, TypeError

   .. py:staticmethod:: attach(pool_serialized)

      Attaches to an existing memory pool.
      Cleaning requires an explicit call to `detach`.

      :param pool_serialized: bytes object of a serialized pool descriptor.
      :return: MemoryPool object
      :rtype: MemoryPool
      :raises: DragonPoolAttachFail

   .. py:method:: destroy()

      Destroys the pool created by this object.

      :raises: DragonPoolError

   .. py:method:: detach(serialize=False)

      Detaches the pool handle owned by this object.

      :param serialize: Boolean to optionally store a serializer before detaching
      :raises: DragonPoolError

   .. py:method:: serialize()

      Serialized the pool descriptor held by this object.  Used for passing to other processes for attaching.
      Stores a copy in the object when called the first time.

      :return: Bytes object of the pool serializer struct.
      :rtype: bytes
      :raises: DragonPoolError

   .. py:method:: alloc(size_t size)

      Allocate a new chunk of memory of size *size* in the memory pool.
      Returns a new MemoryAlloc object on success.

      :param size: Size in bytes to allocate
      :return: MemoryAlloc object with new allocation
      :rtype: object
      :raises: DragonPoolError

   .. py:method:: alloc_blocking(size_t size, timeout=None)

      Allocate a new chunk of memory of size *size* in the memory pool.  Returns a new MemoryAlloc object on
      success. If *size* block is unavailable, it blocks *timeout* seconds waiting for an allocation.  If
      *timeout* is None, it blocks without timeout.

      :param size: Size in bytes to allocate
      :return: MemoryAlloc object with new allocation
      :rtype: object
      :raises: DragonPoolException

   .. py:method:: free(MemoryAlloc mem_obj)

      Frees the allocation held by *mem_obj* in the pool.

      :param mem_obj: The MemoryAlloc object holding the descriptor for the given allocation to free
      :raises: RuntimeError

   .. py:method:: get_allocations()

      Retrieves a list of all active allocations in the memory pool.

      :return: MemoryAllocations object
      :rtype: object
      :raises: DragonPoolError

   .. py:method:: allocation_exists(alloc_type, alloc_id)

      Given a type and ID, check whether a given allocation exists in the pool manifest.

      :param alloc_type: AllocType enum of the allocation
      :param alloc_id: Unique ID of the allocation
      :return: True or False
      :rtype: Boolean
      :raises: DragonPoolError

   .. py:method:: get_alloc_by_id(alloc_type, alloc_id)

      Retrieve a specific allocation from the pool by its type and unique ID.

      :param alloc_type: AllocType enum of the allocation
      :param alloc_id: Unique ID of the allocation
      :return: MemoryAlloc object containing a description of the allocation
      :rtype: object
      :raises: DragonPoolError
