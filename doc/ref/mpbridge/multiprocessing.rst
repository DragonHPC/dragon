.. _MultiprocessingAPI:

Python multiprocessing
++++++++++++++++++++++

Dragon implements most of the :external+python:doc:`Python multiprocessing <library/multiprocessing>` API though a
new context and start method. The primary difference with this new start method is that objects can exist and be
accessed across multiple nodes. This can impact the meaning of some of the
:external+python:doc:`multiprocessing <library/multiprocessing>` API, and these differenes are noted in the
documentation below.

.. autoclass:: dragon.mpbridge.context.DragonContext
    :members:
    :no-undoc-members:
    :no-index:


.. _MPBridgeAPI:

Dragon multiprocessing Classes
==============================

These classes are not instantiated directly. Instead users will obtain instances of these classes through
the :external+python:doc:`multiprocessing <library/multiprocessing>` interfaces discussed above.

Process
-------
.. currentmodule:: dragon.mpbridge.process
.. autosummary::
    :toctree:
    :recursive:

    DragonProcess


Pipes and Queues
----------------
.. currentmodule:: dragon.infrastructure.connection
.. autosummary::
    :toctree:
    :recursive:

    Connection
    Pipe
    ConnectionOptions
    PipeOptions

.. currentmodule:: dragon.mpbridge.queues
.. autosummary::
    :toctree:
    :recursive:

    DragonQueue
    DragonJoinableQueue
    DragonSimpleQueue


Synchronization
---------------
.. currentmodule:: dragon.mpbridge.synchronize
.. autosummary::
    :toctree:
    :recursive:

    DragonBarrier
    DragonBoundedSemaphore
    DragonCondition
    DragonEvent
    DragonLock
    DragonRLock
    DragonSemaphore


Shared `ctypes`
---------------
.. currentmodule:: dragon.mpbridge.sharedctypes
.. autosummary::
    :toctree:
    :recursive:

    DragonArray
    DragonValue
    DragonRawArray
    DragonRawValue


Process Pools
-------------
.. currentmodule:: dragon.mpbridge.pool
.. autosummary::
    :toctree:
    :recursive:

    DragonPool
    WrappedResult