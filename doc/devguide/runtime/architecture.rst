.. _InfrastructureArchitecture:

Infrastructure Architecture
===========================

.. figure:: images/infrastructure.svg
    :name: infra-schematic

    **Dragon Runtime Architecture in a multi-node deployment**

There are various actors involved in the Dragon runtime, they are shown in :numref:`infra-schematic`.  Although they
will be specified in more detail in other documents, we list them here and summarize their function. Whenever
necessary, the user program is called ``my.py`` and is started from the command line by invoking the
:ref:`Launcher` with ``dragon my.py``.  In particular, no preliminary interaction with the system's workload
manager (if present) is required.

**Launcher**

The :ref:`Launcher` service is the ``dragon`` executable that brings up the other pieces of the runtime from
nothing and arranges for the user program (e.g. ``my.py``) to start executing. It consists of a frontend and
backend component.  The backend communicates with the frontend to provide all the functionality of the
:ref:`Launcher` on compute-nodes. In particular, it routes traffic the node-local :ref:`LocalServices` process may
have to the frontend and connects traffic that the launcher frontend may have to :ref:`Channels`. In the
single-node case, the launcher frontend and backend talk directly to each other. In the multi-node case, they
communicate over a TCP overlay network organized as a broadcast tree. See :ref:`MultiNodeDeployment` and
:ref:`SingleNodeDeployment` for more details on the deployment process.

**Local Services**

The :ref:`LocalServices` process is the direct parent of all managed processes as well as some of the
infrastructure processes.

Local Services' responsibilities include:

   * creating a named shared memory pools (default and infrastructure pool) for interprocess communication during bringup
   * instantiating infrastructure channels in that segment.
   * starting managed and unmanaged processes on its node
   * aggregating, filtering, and forwarding stdout and stderr from the processes it has started ultimately back to the launcher
   * propagating messages to a process's stdin from a channel

In the single node case, there is exactly one shepherd. In a multi node case, there is exactly one shepherd
process per node. See :ref:`ProcessCreationAndInteraction` for more details on process creation.

**Global Services**

:ref:`GlobalServices` maintains a global namespace and tracks the state of global objects in a Dragon program,
which include managed processes and channels.
Global Services currently runs as a single centralized process on the primary node. In multi-node
deployments it communicates with each node's Local Services through infrastructure channels routed over the
transport agent.

**Transport Agent**

The :ref:`TransportAgent` is a process that is present, one per node, on all
multi-node Dragon runtimes.  It attaches to the shared memory segment created by the :ref:`LocalServices` and
routes messages destined to other nodes using a lower-level communication
mechanism. The default transport is HSTA (a high-speed RDMA-based agent); when
HSTA is not available the runtime falls back to TCP. There is no transport agent
in single-node deployments.

**Communication Pathways**

There are various :ref:`CommunicationComponents` that need to be set up to get the Dragon runtime running.
Generally speaking, once the runtime is up, it uses :ref:`Channels` (implemented in shared memory and the
Transport Agent where applicable) for as many operations as possible, whether those operations originate from
the runtime itself or from user programs.

:ref:`Channels` are the main mechanism to unify on-node and off-node communication of Dragon processes in the
runtime. Dragon services communicate with each other using the :ref:`Messages` API through special
infrastructure :ref:`Channels`. There is always at least one infrastructure channel per service present and
except for bringup and teardown of the runtime, all communication between services runs through
channels.

During :ref:`SingleNodeBringup` or :ref:`MultiNodeBringup`, the :ref:`LocalServices` allocates a segment of
_POSIXSharedMemory to hold :ref:`ManagedMemory`. It then allocates a dedicated infrastructure managed memory
pool and creates all infrastructure :ref:`Channels` into it. Every channel is then represented by a serialised
descriptor that contains enough information about the channel, the managed memory allocation for the channel,
and the managed memory pool. Every process can use the serialized descriptor to attach to and use the channel.

This effectively implements shared on-node shared memory for Dragon managed and un-managed processes.

The launcher frontend and backend communicate over a TCP overlay network. During
:ref:`MultiNodeBringup`, the frontend starts backend processes on every compute node (via the workload manager
or SSH). Each backend opens a TCP connection back to the frontend, which then constructs a broadcast tree
based on a configurable fanout. This tree is used to distribute bringup and teardown messages across all
nodes without requiring a third-party overlay library.

The stdin, stdout and stderr pipes of managed processes are captured by the :ref:`LocalServices`. Some
:ref:`InfrastructureBootstrapping` may in some cases involve information passed through the process's stdin
and stdout - this can remove some restrictions on the size of command lines and give a conventional way to
handshake startup processing.

**Conventional IDs**

The Dragon infrastructure uses :ref:`p_uid` (``p_uid``), :ref:`c_uid` (``c_uid``), and :ref:`m_uid`
(``m_uid``) to uniquely identify processes, channels, and memory pools in the runtime system. See
:ref:`ConventionalIDs` for more details.

**Dragon Process Creation and Interaction**

Dragon infrastructure :ref:`Services` are so-called **unmanaged** processes - namely, runtime support
processes that are not managed by the :ref:`GlobalServices` process. The category of **managed** processes
covers those that are created as a result of the code the user runs. This could be because the user creates a
process explicitly (such as instantiating a ``multiprocessing.Process``), implicitly (such as instantiating a
``multiprocessing.Pool``), or as a result of creating a managed data structure for higher level communication.
Managed processes are always started by the Shepherd (see :ref:`ProcessCreationAndInteraction`) and are handed
a set of :ref:`LaunchParameters` as environment variables to define the Dragon environment.

**Low-level Components**

All Dragon processes (managed and unmanaged) are `POSIX`_ live processes using Dragon's
:ref:`ManagedMemory` API to share thread-safe memory allocations. During an allocation of managed memory from
a memory pool, an *opaque memory handle* (descriptor) is created by the runtime and handed to the calling
process. It can then be shared with any other Dragon process to *attach* to the memory pool and use the
underlying object (e.g. channel). The runtime takes care of proper address translation between processes by
storing only the offset from shared memory base pointer. Thread-safety of underlying the memory object is
ensured by using Dragons :ref:`Locks`.

The Dragon infrastructure uses the following :ref:`Components`:

1. :ref:`Locks`: High performance locks to protect :ref:`ManagedMemory` pools.
2. :ref:`ManagedMemory`: Thread-safe memory pools holding their own state so they can be shared among processes using opaque handles.
3. :ref:`UnorderedMap`: A hash table implementation in :ref:`ManagedMemory`.
4. :ref:`Broadcast`: Any to many broadcaster to trigger events for a collection of processes flexibly.

.. ------------------------------------------------------------------------
.. External Links
.. _Python Multiprocessing module: https://docs.python.org/3/library/multiprocessing.html
.. _POSIX: https://pubs.opengroup.org/onlinepubs/9699919799.2018edition/
.. _POSIXSharedMemory: https://man7.org/linux/man-pages/man7/shm_overview.7.html