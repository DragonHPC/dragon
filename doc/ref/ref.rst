.. _DragonAPI:

API Reference
+++++++++++++

.. toctree::
    :maxdepth: 2

    native/index.rst
    client/index.rst
    inf/index.rst
    core/index.rst
    mpbridge/index.rst
    workflows/index.rst
    data/index.rst

<Picture of Dragon services. The picture should show unmanaged running on top of
the run-time services and also beside it. The segment on top of dragon run-time
services should have managed on top of it and then multiprocssing on top of that.
We should show Dragon Transport Service at bottom of diagram.>

The Dragon API is broken up into a few services with different usability
characteristics. The *core* API includes several lower-level synchronization
and communication objects. The :term:`managed <Managed Object>` API, built on top of the *core* API
provides several services in addition to the *core* API, including discoverability
and reference counting. The multiprocessing API, part of the Python distribution, is
re-implemented using *managed* core objects from the Dragon run-time services.

The core services, provide bindings for several different languages
including Python, C++, C, and Fortran. Synchronization and communication objects
include shared memory pools, shared memory allocations, channels, queues,
barriers. These services can be used on-node with or without running the Dragon
run-time services. When using them while running the Dragon run-time services the
objects transparently work in a distributed fashion across all nodes in the
:term:`Dragon run-time instance <Dragon Run-time Instance>`.

The managed services provide additional discoverability of pools, memory
allocations, and channels. In addition, the programmer may choose to have memory
allocations and channels be [reference counted](term:`refcounted <Refcounted Object>`) by the Dragon run-time services
when they are created. These services are available from Python and rely on the
Dragon run-time services.

The Dragon implementation of multiprocessing provides a distributed run-time environment
for multiprocessing, where the original implementation of multiprocessing was confined to
one node. These objects are always managed and refcounted. The Dragon implementation runs across all nodes in the Dragon run-time services
instance.

The following table provides an overview over the Dragon API stack. For a
description of the Dragon APIs, see the :ref:`Programming Guide <pguide/stack:The API Stack>`

.. list-table:: API capability overview
   :widths: 20 20 30 20
   :header-rows: 1

   * - API Name
     - Languages
     - Requires Run-time Services
     - Use Case
   * - Multiprocessing
     - Python
     - Yes
     - Distributed multiprocessing
   * - Managed Dragon
     - Python
     - Yes
     - Extend the Core API
   * - Core Dragon
     - C and Python
     - No, but multinode with it.
     - Custom implementations



