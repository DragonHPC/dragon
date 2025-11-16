.. _DragonCoreManagedMemory:

Managed Memory
+++++++++++++++++++++++++++++++

.. contents:: Table of Contents
    :local:

Description
''''''''''''

Dragon memory pools and memory allocations are shared memory
allocations that can be attached to both on-node and off-node.
Attaching on-node to a pool means that a process can create allocations
within the pool. Attaching to a memory allocation on-node means a
process can get a pointer to the allocation.

Structures and Constants
''''''''''''''''''''''''''''

Here are structures and constants for creating and sharing memory pools and memory
pool allocations. These structures and constants are found in ``managed_memory.h``.

.. doxygenenum:: dragonMemoryPoolType_t

.. doxygenenum:: dragonMemoryPoolGrowthType_t

.. doxygenstruct:: dragonMemoryPoolAttr_t
    :members:

.. doxygenstruct:: dragonMemoryPoolDescr_t

.. doxygenenum:: dragonMemoryAllocationType_t

.. doxygenstruct:: dragonMemoryDescr_t

.. doxygenstruct:: dragonMemoryPoolSerial_t
    :members:

.. doxygenstruct:: dragonMemorySerial_t
    :members:

.. doxygenstruct:: dragonMemoryPoolAllocations_t
    :members:


Pool Lifecycle Management
''''''''''''''''''''''''''

.. doxygenfunction:: dragon_memory_attr_init

.. doxygenfunction:: dragon_memory_attr_destroy

.. doxygenfunction:: dragon_memory_pool_create

.. doxygenfunction:: dragon_memory_pool_destroy

.. doxygenfunction:: dragon_memory_pool_max_serialized_len

.. doxygenfunction:: dragon_memory_pool_serialize

.. doxygenfunction:: dragon_memory_pool_serial_free

.. doxygenfunction:: dragon_memory_pool_attach

.. doxygenfunction:: dragon_memory_pool_attach_from_env

.. doxygenfunction:: dragon_memory_pool_detach

.. doxygenfunction:: dragon_memory_pool_descr_clone

Pool Information & Functionality
''''''''''''''''''''''''''''''''''

.. doxygenfunction:: dragon_memory_pool_get_hostid

.. doxygenfunction:: dragon_memory_pool_get_uid_fname

.. doxygenfunction:: dragon_memory_pool_is_local

.. doxygenfunction:: dragon_memory_pool_allocations_free

.. doxygenfunction:: dragon_memory_pool_allocation_exists

.. doxygenfunction:: dragon_memory_pool_get_allocations

.. doxygenfunction:: dragon_memory_pool_get_type_allocations

.. doxygenfunction:: dragon_memory_pool_allocations_destroy


Memory Lifecycle Management
''''''''''''''''''''''''''''''

.. doxygenfunction:: dragon_memory_alloc

.. doxygenfunction:: dragon_memory_alloc_blocking

.. doxygenfunction:: dragon_memory_alloc_type

.. doxygenfunction:: dragon_memory_alloc_type_blocking

.. doxygenfunction:: dragon_memory_max_serialized_len

.. doxygenfunction:: dragon_memory_serialize

.. doxygenfunction:: dragon_memory_attach

.. doxygenfunction:: dragon_memory_detach

.. doxygenfunction:: dragon_memory_serial_free

.. doxygenfunction:: dragon_memory_get_alloc_memdescr

.. doxygenfunction:: dragon_memory_id

.. doxygenfunction:: dragon_memory_from_id

Memory Information & Functionality
''''''''''''''''''''''''''''''''''''''

.. doxygenfunction:: dragon_memory_get_size

.. doxygenfunction:: dragon_memory_get_pool

.. doxygenfunction:: dragon_memory_get_pointer

.. doxygenfunction:: dragon_memory_free

.. doxygenfunction:: dragon_memory_descr_clone

.. doxygenfunction:: dragon_memory_modify_size

