.. _DragonCoreChannels:

Channels
++++++++

.. contents:: Table of Contents
    :local:

Description
===========

Dragon Channels is the low-level transport layer for communicating messages
between POSIX processes in the Dragon runtime. Channels are a thread and
interprocess-safe queue where messages can be sent and received. The Dragon
run-time services itself uses channels for communication between processes. User
programs, utilizing the Dragon run-time services also use channels either
directly, or indirectly. This API would be used directly when a program wants to
communicate in the most efficient way possible while being willing to give up the
services provided by higher level communication and synchronization abstractions.
Choosing to communicate at the channels level will mean giving up features like
pickling and unpickling, automatic serialization of data, and the automatic
streaming of data over a channel.

Channels provide flexible on-node and communication that processes use by
attaching and detaching to the underlying
:ref:`Managed Memory<DragonCoreManagedMemory>`. When Channels are used in conjunction with a
transport agent, transparent off-node communication is also provided when sending
and receiving messages using the Channels API. In this respect, Channels resemble
POSIX sockets as they are always available as a service and not built upon the
static distributed model that MPI or SHMEM is. :term:`Transparency` is provided because
the exact same Channels API calls work for both on-node and off-node
communication. The user program does not change when communicating off-node or
on-node.

A major advantage of Channels is that they retain the flexibility of using
sockets or a filesystem while enabling zero-copy on-node transfers, single-copy
RDMA-enabled transfers off-node, and choices for blocking semantics. There is a
rich set of buffer management options that enable use cases such as:

    - static target-side managed payload landing pads much like SHMEM or
      one-sided MPI

    - dynamic target-side managed payload landing pads much like two-sided MPI

    - static or dynamic origin-side managed payload landing pad, which nothing
      else has

Dragon Channels can reside in any valid :c:struct:`dragonMemoryDescr_t` as
provided by Dragon Memory Pools. This includes shared memory, a filesystem, or
private virtual memory. Payload buffers for messages can reside within a
channel, a memory pool :c:struct:`dragonMemoryPoolDescr_t` the channel was
allocated from, or any valid :c:struct:`dragonMemoryDescr_t` passed with a
message. This design allows Channels to provide multiple usage scenarios with
different performance and persistence characteristics.

Channels provide both blocking an non-blocking semantics for sending and getting
messages. When sending, blocking can be chosen to wait when memory from a pool is
needed and not available. When getting messages, blocking receives will wait
until a message is available. Blocking operations can either idle wait, consuming
fewer resources and energy, or spin wait, with relatively no wakeup cycles.
Channels are highly configurable and customizable for whatever situation they are
used in at the expense of being a low-level primitive synchronization and
communication construct.

Example
==========

:ref:`An example can be found here <channels_example>`. The example illustrates how
to use the C Channels API.

Channels API
==============

Constants
''''''''''''''''''''''''''''

.. doxygengroup:: channels_constants
   :members:

Structures
''''''''''''''''

.. doxygengroup:: channels_structs
   :members:

Message Lifecycle Management
'''''''''''''''''''''''''''''

.. doxygengroup:: messages_lifecycle
   :members:

Message Functionality
''''''''''''''''''''''''''''''''''''

.. doxygengroup:: messages_functionality
   :members:

Channel Lifecycle Management
'''''''''''''''''''''''''''''

.. doxygengroup:: channels_lifecycle
   :members:

Channels Functionality
''''''''''''''''''''''''''''''''''''''

.. doxygengroup:: channels_functionality
   :members:

Gateway Message Lifecycle Management
'''''''''''''''''''''''''''''''''''''''

.. doxygengroup:: gateway_messages_lifecycle
   :members:

Gateway Message Functionality
''''''''''''''''''''''''''''''''

.. doxygengroup:: gateway_messages_functionality
   :members:
