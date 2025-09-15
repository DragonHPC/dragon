.. _HSTA:

High Speed Transport Agent (HSTA)
+++++++++++++++++++++++++++++++++

Dragon’s High Speed Transport Agent, or HSTA, is an RDMA-based solution for off-node communication
of distributed Python multiprocessing applications. HSTA provides a high bandwidth, high message
rate and low latency communication framework that aims to make it easy to obtain high network
performance via a high-level programming interface. Other popular HPC communication frameworks,
such as MPI or SHMEM, generally require low level programming and expert knowledge of both the
communication API and the network hardware to obtain optimal performance. Network communication
is implicit in applications and workflows using the Dragon ecosystem, allowing HSTA to optimize
network communication behind the scenes.

Dragon takes an innovative approach to HPC networking. Rather than providing a networking library
such as MPI or SHMEM, it uses a communication offloading approach where the user application sends
communication requests to a separate executable, called the transport agent. The transport agent
receives communication requests and handles the actual network communication, all facilitated
through Dragon Channels, the core communication layer for Dragon (see :numref:`channels-hsta`).

The use of a transport agent rather than a library provides a number of benefits. Maybe foremost
is the ability for processes to come and go efficiently, without any need to initialize a communication
library for each new process, since transport agents persist throughout the duration of a job. Another
advantage is the ability to aggregate small message across an entire node, rather than only across a
single process. This allows for much greater aggregation of small messages, especially as node sizes grow.

.. figure:: ../images/channels_hsta.png
   :scale: 25%
   :name: channels-hsta

   **Dragon Channels off-node communication via the HSTA transport agent**

.. _HSTAArchitecture:

HSTA Architecture
=================

This section covers communication protocols, ordering semantics, transmit and receive queues,
flow control and memory allocation for the HSTA transport agent. Future additions to the
architecture are discussed in a later section.

Communication Protocols
-----------------------

HSTA supports three communication protocols: ``SENDMSG``, ``GETMSG`` and ``POLL``. ``SENDMSG`` transfers a
payload to a remote channel; ``GETMSG`` retrieves a payload from a remote channel; and ``POLL`` polls
a remote channel for the current state (i.e., if the channel is full). Protocols that require a
response (``SENDMSG`` with ``DRAGON_CHANNEL_SEND_RETURN_WHEN_DEPOSITED`` or
``DRAGON_CHANNEL_SEND_RETURN_WHEN_RECEIVED``, ``GETMSG`` and ``POLL``) generate a new work request at the
target for the response. The work request is generated once the local channel operation has
completed.

Local channel operations are queued in the order that their corresponding headers are received
and processed--the queues of local channel operations are processed asynchronously. For each
protocol type, the corresponding channel operation is tried with a timeout of 0 sec. If the first
operation in a queue does not complete, the HSTA progress engine moves on to other work (possibly
processing another queue). The user-specified timeout is honored by only retrying a given channel
operation until an HSTA-maintained timer reaches the timeout value.

Ordering
--------

Messages using the ``SENDMSG`` protocol are ordered according to the send handle used to send the
message. A globally unique 64-bit integer, called a *port*, is allocated for each send handle,
i.e., all messages are ordered by their corresponding port. Ports are used to look up information
related to the target channel, including the channel descriptor.

Messages using the ``GETMSG`` or ``POLL`` protocol are ordered according to their target channel.
Consequently, ports for messages using the ``GETMSG`` or ``POLL`` protocol are allocated per target
channel. For a given process, messages using the ``GETMSG`` or ``POLL`` protocol will complete in
the order in which they were started. There are no ordering guarantees for ``GETMSG`` and ``POLL``
messages originating from different processes. ``GETMSG`` and ``POLL`` requests queue up at the
target channel in the order in which they were received at the target HSTA agent.

Transmit and Receive Queues
---------------------------

For each message, regardless of protocol, a header is appended to a *TX* (*transmit*) *queue*. The
header contains a header type (3 bits), protocol type (2 bits), send return mode (3 bits),
eager data size (2 bytes), timeout (1 byte) and port (8 bytes), for a total of 12 bytes per
header. Messages with any protocol type and any port (for ordering) can be placed on the same TX
queue, as long as they have the same target node. If the message is small enough, eager data for
the message is placed after its header.

TX queues are broken down into 2KB chunks called *eager buffers*, limiting the amount of message
aggregation that is possible to less than 2KB. Heuristics are used to determine when to stop
packing headers and eager data into an eager buffer. An eager buffer, or some prefix of the
buffer, is sent to the target node for processing. At the target node, the eager buffer is
received into an *RX* (*receive*) *queue*, which is essentially identical to a TX queue but
dedicated to incoming traffic. Newly received eager buffers are processed and a handler function
is called for each received header.

For a given agent, there is one TX queue per target node, and a single RX queue (although this
may change in the future). The number of eager buffers per TX queue is 4, and the number of
eager buffers per RX queue is the minimum of (1) 4 times the number of nodes, and (2) 128.

Flow Control
------------

The *ejection rate* is the rate, measured in GB/s, at which data is written by the NIC into the
user's memory. HSTA controls the ejection rate by limiting the number of pending receive
operations based on the amount of memory required for the receive buffers. The default is to
allow 8MB worth of pending receive buffers, but can be modified using the environment variable
``DRAGON_HSTA_MAX_EJECTION_MB``. Currently, the injection rate is not controlled, but it will be
in future releases (using ``DRAGON_HSTA_MAX_INJECTION_MB``).

A buffer is allocated for each local ``getmsg`` operation executed at the target channel
for a message with ``GETMSG`` protocol. The rate of memory consumption for these buffers can be
controlled by the environment variable ``DRAGON_HSTA_MAX_GETMSG_MB``.

There are a finite number of eager buffers for each TX queue, providing further backpressure.
This backpressure is not propagated back to the user, however, since HSTA will queue up new
requests that cannot be immediately handled. This may change in the future, depending on the need
for stiffer backpressure to improve performance.

Back pressure is also applied at gateway channels that can fill if off-node transport requests
are made at rate faster than HSTA is able to process them. This backpressure will be propagated
back to the user by way of an error code.

Memory Allocation
-----------------

Eager buffers, receive buffers and buffers for local ``getmsg`` operations are all allocated from
managed memory. Managed memory is necessary for channels operations, but there are advantages
for eager buffers and receive buffers as well; specifically, using managed memory will make it
possible to back memory with hugepages, and also simplifiy memory registration by registering
the entire managed memory pool with the NIC. This will improve performance, as memory registration
is generally quite expensive.

Environment Variables
=====================

* ``DRAGON_HSTA_FABRIC_BACKEND``

  * Select the fabric backend to use. Possible options are ``ofi``, ``ucx``, and ``mpi``.

* ``DRAGON_HSTA_TRANSPORT_TYPE``

  * Select if HSTA uses point-to-point operations or RDMA Get to send large messages. Possible
  options are ``p2p`` and ``rma``.

* ``DRAGON_HSTA_ENABLE_VALGRIND``

  * Enable running with Valgrind to help debug memory issues in HSTA. Possible options are ``0`` and ``1``.

* ``DRAGON_HSTA_ENABLE_PERF``

  * Enable running with the Linux ``perf`` utility to help measure the local performance of HSTA.
  Possible options are ``0`` and ``1``. ``dragon-flame-graph`` is a helper script for creating flame graphs
  after running with ``DRAGON_HSTA_ENABLE_PERF`` enabled.

* ``DRAGON_HSTA_MAX_EJECTION_MB``

  * Size in MB of buffers used for network receive operations. This controls network ejection rate. Defaults to 8.

* ``DRAGON_HSTA_MAX_GETMSG_MB``

  * Size in MB of buffers used for local ``getmsg`` operations. This controls memory consumption rate for messages with ``GETMSG`` protocol. Defaults to 8.

* ``DRAGON_HSTA_DEBUG``

  * Enable debugging mode for HSTA. Defaults to 0.

Near-term Architectural Additions
=================================

Multiple agents per node
------------------------

Currently, there is only a single HSTA agent per node. In the near future, this will be updated
to default to one HSTA agent per NIC. Each HSTA agent will be a separate process and have its
own MPI rank. Any two agents on the same node will not be able to communicate; the cluster of
agents on a node will best be seen as implementing a single "super agent" for the node.

As mentioned above, injection and ejection rates for large messages will be controlled by
queueing up send and receive operations and only posting limited batches at a time. This means
that there will often be a backlog of work for each agent (i.e., send and receive operations for
large messages that an agent has not yet been able to post). Load balancing will be achieved by
allowing agents to steal work from other agents on the same node. Work stealing will be the only
interaction allowed between different agents on the same node. Eager messages will always be
handled by a single agent, guaranteeing correct ordering of messages.

Many-to-Many Communication Patterns
-----------------------------------

Target randomization will be the primary method used to improve performance for many-to-many
communication patterns. Target randomization reduces the pressure on NICs and routers by
distributing traffic more evenly. The effectiveness of target randomization can be improved
by *strip mining*, or breaking up larger messages into smaller chunks. Strip mining is also
useful for more fine-grained control of the injection and ejection rates for a NIC, since it
avoids having to transfer a very large amount of data in a single operation.

Many-to-One Communication Patterns
----------------------------------

Many-to-one communication patterns are only relevant for eager messages, since HSTA is able to
control the ejection rate for large messages. On-node aggregation of headers and eager data into
eager buffers reduces the number of separate small messages going between two nodes, and target
randomization can also, in some cases, reduce the maximum in-degree of any many-to-one traffic
patterns experienced by an application. But the best way to eliminate many-to-one patterns is by
using something like a fan-in tree to reduce the maximum degree of any many-to-one pattern.
Rather than create a new fan-in tree for each observed pattern, it makes more sense to use a
single dragonfly graph, which provides a consistent way to provide a guaranteed minimum in-degree
for arbitrary communication patterns.
