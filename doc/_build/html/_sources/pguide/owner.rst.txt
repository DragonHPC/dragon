Ownership, Placement and Locality
+++++++++++++++++++++++++++++++++

In this section we discuss the relationship of :term:`Dragon objects <Dragon Object>` with
:term:`system resources <System Resource>`. Due to Dragon's hierarchical
:ref:`resource model <uguide/resource_model:The Dragon HPC Resource Model>`, it is sufficient
to consider only the :term:`primary objects <Primary Object>` in the Client
API that form the basis of all other objects created in the
:ref:`Dragon native API <ref/native/index:Dragon Native>` and Multiprocessing with Dragon API.

Fundamentally, Dragon does not have the concept of ownership of an object, i.e. the lifetime of
an object is not bound to the process that created it. For example a process can
create an Dragon native :any:`queue <dragon.native.queue.Queue>`, hand its
:term:`serialized descriptor <Serialized Descriptor>` to another process
and terminate. The Dragon queue will remain usable as long as one process holds
the descriptor and the queue destructor has not been called. However, as
:term:`primary objects <Primary Object>` abstract a :term:`resource <System Resource>`,
the Dragon channel that underpins the Dragon native queue has to be placed in memory
*somewhere* on the physical system. To that end, the channel is *local* to one specific
:term:`hardware node <Node>`. The Dragon run-time uses its transport service to make access
:term:`transparent <Transparency>` to other nodes.

In what follows, we discuss placement and locality properties of processes, managed memory and
channels. We leave out the node object as it solely describes the hardware the run-time is running
on. Some aspects of placement can be controlled using :ref:`policies <uguide/resource_model:Policies>`
in the Dragon Native API.


Processes
=========

With version 0.5 Dragon only supports round robin placement of processes. Each new process will be placed on the next
node that Dragon is running on. Future releases of Dragon will allow for two levels of control that improve upon the
current behavior. The first will be the ability to control the default policy, and the second will be explicit
placement control for each process.

Round Robin Placement
---------------------

.. figure:: images/roundrobin.svg
    :scale: 75%

    **Figure 1: UML deployment diagram of round robin placement with 4 processes on a distributed system with 3 compute nodes and one login node**

Figure 1 shows how processes will be placed across nodes in your allocation with the current round-robin placement
policy. Any two processes started consecutively will be placed on unique nodes unless there is only a single node
within your Slurm allocation.


Managed Memory
==============

Managed Memory is the underlying shared memory abstraction from which all :term:`Dragon Objects <Dragon Object>` are
allocated. They consist of `Pools` and `allocations` from those Pools. Every node Dragon is executing on has
at least two Pools, one for infrastructure usage and another for user allocations. Pools can be created, destroyed,
and shared with other processes. Management of Pools is handled by Dragon Global Services (GS), and interactions
required with an operating system to actually manage them on a node is done by Dragon Local Services (LS).

When a request is made to create a Dragon Object, the underlying Pool that it will be allocated from can be included
with the request. This in turn specifies the physical location of that Dragon Object. If the request does not included
the Pool to allocate the object from, one will be selected by GS through a default policy. With version 0.5, users
programming to Python multiprocessing do not have control over the exact placement of the Dragon-managed resources
used to implement multiprocessing abstractions. Such controls will be available in future releases.

Advanced users programming to the :ref:`Dragon Native API <ref/native/index:Dragon Native>` can explicitly manage the
life-cycle of Pools outside of the control of GS.

Allocations made from a Pool can be serialized and shared with other processes. These processes can then attach to the
allocation. If the Pool the allocation is from is on the same node as that process, it can directly obtain a pointer
to the memory. The process can modify the memory from that allocation, but Dragon does not provide any support
for maintaining consistency of that memory between processes. Dragon makes use of a custom lock, which will be
part of the public API in future releases, that can be used to support consistency of memory allocated from a Pool.
If the allocation is not from a Pool on the same node as the process, the process can get limited information about
the allocation, such as its size and physical location. Future Dragon releases will include more detail on using
its low levels APIs like this.


Channels
========

Dragon Channels is the primary abstraction upon which all :term:`Dragon Objects
<Dragon Object>` are implemented. A Channel is similar to a basic FIFO queue,
but with a rich set of capabilities for monitoring events, controlling
buffering behavior, and completion semantics. Python multiprocessing
abstractions, including Connection, Queue, Lock, and many more are implemented
from one or more Channels. Channels provides :term:`transparent <Transparency>`
access no matter which Managed Memory Pool the Channel is allocated from. If
the Channel is on the same node as a process, the Channels library will
directly access the Channel through shared memory. If the Channel is on a
remote node, a `Transport Agent` will act on behalf of the process to directly
access the remote Channel. The access transparency of Channels is how Dragon is
able to provide access transparency for all objects available through the
runtime.

Version 0.5 of Dragon uses an RDMA-based transport agent by default (note:
:ref:`a TCP-based agent is optionally available <Transport FAQ>`). A transport agent
process exists on every node that Dragon is running on. The transport agent
process monitors an infrastructure Channel on its node, called a Gateway
Channel, that user processes write requests into when they need to communicate
with a remote Channel. The transport agent services that request and
communicates back with the user process through a synchronization mechanism.
All user processes on a node perform off-node communication through their local
transport agent process. Transport agents can use any network protocol to
perform their work. Later Dragon releases will include enhancements to the
TCP-based transport agent to use encrypted communication.

The physical location of a Channel can be specified in the request to GS by
indicating which Managed Memory Pool it should be allocated from. If the
location is not specified, a default policy is used to select the location. The
default policy for version 0.5 is that the Channel be co-located with the
process requesting its creation. For most Python multiprocessing objects that
are constructed from Channels, the Channels will be on the same node as the
process that is creating the object.
