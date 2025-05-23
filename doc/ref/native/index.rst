.. _NativeAPI:

Native
++++++

The following reference provides information about the Dragon Native API. Much of the API is inspired by Python
multiprocessing but includes extended functionality, such as support for explicit placement of resources through
the :py:class:`dragon.infrastructure.policy.Policy` class.

Python Reference
================

Infrastructure Information
--------------------------

.. currentmodule:: dragon.native

.. autosummary::
    :toctree:
    :recursive:

    machine


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
