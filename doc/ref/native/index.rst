.. _NativeAPI:

Native
++++++

The following reference provides information about the Dragon Native API. Much of the API is inspired by Python
multiprocessing but includes extended functionality, such as support for explicit placement of resources through
the :py:class:`dragon.infrastructure.policy.Policy` class.

Use the Native API when you need more control than Dragon's multiprocessing
layer provides. It is the right entry point for explicit process placement,
manual lifecycle control, direct queue and synchronization usage, cross-language support, and programs
that need to combine Python with lower-level Dragon runtime behavior.

For the architectural view behind these APIs, start with :ref:`runtime_design`
in the developer guide. For placement decisions specifically, see
:ref:`ref/policy:Resource Placement and Affinity`.

Python Reference
================

Infrastructure Information
--------------------------

.. currentmodule:: dragon.native

.. autosummary::
    :toctree:
    :recursive:

    machine


.. _NativeProcess:

Process Management
------------------

.. currentmodule:: dragon.native

.. autosummary::
    :toctree:
    :recursive:

    process

.. currentmodule:: dragon.native.process_group

.. autosummary::
    :toctree:
    :recursive:

    ProcessGroup


Synchronization
---------------

.. currentmodule:: dragon.native

.. autosummary::
    :toctree:
    :recursive:

    barrier
    event
    lock
    semaphore


Communication and Shared Data
-----------------------------

.. currentmodule:: dragon.native

.. autosummary::
    :toctree:
    :recursive:

    queue
    value
    array


Process Pools
-------------

.. currentmodule:: dragon.native.pool

.. autosummary::
    :toctree:
    :recursive:

    ApplyResult
    AsyncResult
    MapResult
    Pool

.. _DragonNativeC++:

C++ Reference
================

All C functions used in the C++ implementations are also accessible when using
C++. The C++ API provides higher-level abstractions using class definitions
that allow for the same level of cross-language compatability with greater ease
of use.

.. toctree::
    :maxdepth: 1

    C++/index.rst