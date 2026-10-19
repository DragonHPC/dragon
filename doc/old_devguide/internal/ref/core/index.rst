.. _DragonCore:

Dragon Core
+++++++++++

<Picture of memory pools and channels within memory pools with
 transport service providing transparent communication services.>

The Dragon Core API supports much of the rest of the Dragon run-time services. It
can also be programmed to directly by knowledgable programmers. The API has at
its core two types of objects, shared memory allocations and channels. Written in
C, the API is interoperable with other languages, including C, C++, Python, and
Fortran (C++ and Fortran bindings are currently not available as of this
writing).

Shared memory allocations come from memory pools. Both memory pools and memory
pool allocations are part of the shared memory API.

The channels API provides an organized means of synchronizing and communicating
between processes. Channels provides a queue-like interface between these
processes both on-node and off-node (when used in conjunction with a transport
service).


C Reference
===========

Internal Objects
----------------

.. toctree::
    :maxdepth: 1

    c/bcast.rst
    c/bitset.rst
    c/hashtable.rst
    c/heap_manager.rst
    c/priority_heap.rst
    c/shared_lock.rst
    c/ulist.rst
    c/umap.rst
    c/utils.rst

Python Reference
================

Internal Objects
----------------

.. toctree::
    :maxdepth: 1

    Cython/dtypes.rst
    Cython/heap.rst
    Cython/lock.rst
    Cython/utils.rst
