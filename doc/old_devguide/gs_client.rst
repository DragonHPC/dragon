Dragon GS Client
++++++++++++++++


Architecture
============

.. figure:: images/client_architecture.svg
    :scale: 75%
    :name: GS-client-architecture

    **GS Client architecture**

:numref:`GS-client-architecture` shows the architecture of GS Client API. It exposes four  base components to the user:

1. **Process**: An interface to a managed process.
2. **Pool/Shared Memory**: An interface to a managed memory pool or allocation.
3. **Channel**: An interface to a managed channel.
4. **Node**: An interface to a hardware node the run-time is currently running on.
5. **Group**: An interface to a managed group of resources, such as processes, channels and pools.

The objects are owned and managed by the Dragon runtime system through Global Services - user
processes interact with them through handle objects called descriptors. These base components
are always managed and can always be queried by name, uid or serialized descriptor. All higher
level abstractions (e.g. `dragon.native.queue`) are derived from the four base components. The
API provides convience functions to setup connections to the infrastructure and send message to
the runtime services.


API Reference
==============

Here is the :ref:`DragonGSClient` API.