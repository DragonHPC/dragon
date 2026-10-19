.. _DragonCoreManagedMemory:

Managed Memory
+++++++++++++++++++++++++++++++

.. contents:: Table of Contents
    :local:

Description
''''''''''''

Dragon Managed Memory Pools are created to, as the name indicates, manage
shared memory allocations between processes. When a pool is created, the
user decides on the size of the pool. Then the user may make allocations from the
pool and serialize and share those allocations with other on-node processes. Typically
this is done using other higher-level objects like channels or even higher level
objects built on channels like queues, or the Distributed Dictionary, both of which
also support off-node communication and synchronization via a Dragon transport agent.

Constants
''''''''''''''''''''''''''''

.. doxygengroup:: managed_memory_constants
   :content-only:
   :members:

Structures
''''''''''''''''''''''''''''

.. doxygengroup:: managed_memory_structs
   :content-only:
   :members:

Pool Lifecycle Management
''''''''''''''''''''''''''

.. doxygengroup:: managed_memory_pool_lifecycle
   :content-only:
   :members:

Pool Information & Functionality
''''''''''''''''''''''''''''''''''

.. doxygengroup:: managed_memory_pool_functionality
   :content-only:
   :members:

Memory Lifecycle Management
''''''''''''''''''''''''''''''

.. doxygengroup:: managed_memory_alloc_lifetime
   :content-only:
   :members:

Memory Information & Functionality
''''''''''''''''''''''''''''''''''''''

.. doxygengroup:: managed_memory_alloc_functionality
   :content-only:
   :members: