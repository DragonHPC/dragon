.. _HeapManager:

Heap Manager
++++++++++++

The Heap Manager provides low-level services to components within the dragon
run-time. The heap manager dynamically manages memory through a malloc/free
interface. The heap manager uses a buddy allocator algorithm to manage the heap.
The heap manager itself does not provide the actual heap space. That is provided
by other parts of the memory pool code. The heap manager manages the space
through the malloc/free style interface.

.. figure:: images/blocks.png
    :name: heap-view

    **A logical view of the Heap**

:numref:`heap-view` provides a logical view of the organization of the heap. A
heap is made up of one or more segments. Segments correspond to the smallest
allocatable blocks of the heap. The actual segment size is left up to the user of
the heap. The heap manager will return offsets to pointers based on the segment
size.

Logically, one or more segments that are grouped together are called a block. Two
blocks that are next to each other, shown with the same color in the
:numref:`heap-view`, are buddies. If two blocks that are buddies of each other
are both are free, then they can be merged and thought of as one block that is twice
the size of its two component buddies. In this way there are hierachically many
block sizes possible within the heap.

When a user calls the heap manager to allocate a block (i.e. a malloc call) the
heap manager will look for a free block of the right size and return it while
recording that the block is no longer free.

While the picture in :numref:`heap-view` shows a pyramid shape, that is only to
show the organization. The segments at the bottom represent the only space that
can be allocated.

The heap manager keeps track of which blocks are free in the heap manager. Each
block (not segments, but the logical grouping of segments) can be free or
allocated. Initially, the largest block is marked as free and all other blocks
are not free (because they are included in the largest block). Recording of free
(or not) is done in a series of bitsets. So there is one bit assigned to record
whether each block is free or not.

.. figure:: images/bitsets.png
    :name: heap-bitsets

    **The Heap Manager Data Structures**

There is one bitset created for each possible size of blocks. Looking at
:numref:`heap-bitsets` each level of the figure corresponds to one bitset in the
*free* bitsets. As you can see from the picture, the number of bits in each
bitset is divided in half as block sizes double. :numref:`heap-bitsets` is drawn
to match the logical view, but the bitsets are all half the size of the bitset
below them in the free list of bitsets. This pattern means that we need
:math:`2*num\_segments` bits in total to keep track of all the free blocks.

The series of bitsets is recorded in an array called *free* in the heap manager
with index 0 corresponding to the smallest blocks (segment sized). Index 1
corresponds to blocks made up of two segments, index 2 blocks with four segments,
and so on. The number of segments in a block is :math:`2^{index}` where *index* is
the index into the series of free bitsets.

When a request is made to malloc memory the heap manager computes the smallest block
size that will satisfy that request. It looks into the series of free bitsets to see
if there is a block free of the desired size. If it finds one, great. If it does not, then
the heap manager looks for a block in the next sizes of blocks and splits the block, if
it finds one, into two equal size blocks. If no next size block is available, then the
heap manager repeats the process and looks for an even bigger block that is free.

By repeating the splitting process, the heap manager will split bigger blocks up as
necessary to satisfy smaller requests.

Similarly, when a block is freed, the heap manager will examine its buddy
to see if it is free as well. If it is, then the buddy and block being freed are
marked as not free and the parent block, that includes both the block being freed
and its buddy, is marked as free.

There is one caveat in freeing and merging blocks. Preallocation of blocks is
also possible. Preallocation is handled by one more bitset of *num_segments*
bits. The preallocated bit set has the bit correponding to where a block starts
set to true if it is a preallocated block. This preallocated bit is consulted
before deciding to merge a freed block with its buddy.

The heap manager allocates memory in sizes of powers of two. In doing so, there
is no external fragmentation resulting in unused space between allocations. A
user may request any size allocation, but the heap manager will allocate space
equal to or greater than the requested space to bring it to the next power of
two. This results in what is called internal fragmentation within allocations.

By supporting segments as small as one byte, the heap manager is designed to
minimize internal fragmentation. However, there is still a trade-off in segment
size vs. performance. A segment size of 32 bytes has been tested and results in
acceptable sizes as described below.

What is missing in this discussion is the tracking of allocations that will later
be freed. Managed Memory provides a manifest that tracks all allocations in
shared memory. When an allocation is freed, it is removed from the manifest so
that it can only be freed by one and only one process. A lock on the manifest
itself guarantees that multiple processes cannot free an allocation more than
once. The existence of the manifest means that allocations do not need to be
tracked by the heap manager. The heap manager only needs to track free blocks
within the heap.

Blocking Allocations
======================

The heap manager provides blocking behavior for client malloc requests that cannot
be immediately satisfied. If a client attempts to get an allocation
and no block is available, then the client code may choose to wait on an allocation
becoming avialable. Waiting for an allocation provides back-pressure on processes
to keep them running within the finite resources of the distributed application.

:numref:`heap-bitsets` shows a BCast object for each possible block size. If a block
size is not available, even after splitting, then a process may choose to wait by doing
a blocking allocation. Such processes end up waiting on a BCast object corresponding to the
block size they are waiting for. Care is taken when a process becomes a waiter to make
sure that a process which is freeing a block cannot have a race condition with a process
needing a block. The lock of the heap insures that waiters wait before another process can
free a block.

Space Needed For The Heap Manager
=================================

Required space for the heap manager is significant, but not overwhelming. As an
example, consider a 4GB heap with a segment size of 32 bytes. :math:`4GB =
2^{32}` and :math:`32 = 2^5`. Each entry in the free array consists of a bitset
for a block size of the given power of 2. There are :math:`32 - 5 + 1 = 28`
powers of two (i.e. 28 different block sizes). There are :math:`2^{27}` segments
in this size of heap.

This means that the bitset *free[0]* contains :math:`2^{27}` bits. *free[1]* has
half the bits in its bitset (i.e. :math:`2^{26}`). This pattern repeats and it is
provable that this results in :math:`2 * 2^{27} = 2^{28}` bits total for the
*free* array of bitsets. The preallocated bitset array is one more bitset of
:math:`2^{27}` bits. This results in 48MB of space being required for the *free*
and *preallocated* bitsets. Then there are 28 bcast objects and space for the
lock. Most of the space is for the *free* and *preallocated* bitsets with the
actual space needed at 48.01324462890625 MB.

An Example of Malloc and Free
=============================

.. figure:: images/heapallocations.png
    :name: heap-allocations

    **Initial Heap Allocations**

Consider a 1K heap with a minimum block size of 32 bytes. The smallest
allocatable block size is called a segment. The 1K heap is made up of 32
segments, each of 32 bytes each. :numref:`heap-allocations` shows a heap with
allocations in colors. The first allocation was for 16 bytes, which resulted in a
32 byte allocation (the smallest possible size for this heap) and was allocated
to segment 0. The second allocation was for 500 bytes (actually 512 bytes) which
resulted in the allocation of segments 16-31. Then came an allocation of 64 bytes
which went into segments 2 and 3. The allocation of segments 8-15 was for a
request of 222 bytes but allocated 256 bytes since that is the nearest power of
2. Finally, the purple allocation from segments 4-7 resulted from a request of
112 bytes but again resulted in an allocation of 4 segments and a size of 128
bytes. So the mallocs that lead to the allocations in :numref:`heap-allocations`
might be as follows.

    * 32 bytes
    * 512 bytes
    * 64 bytes
    * 256 bytes
    * 128 bytes

Worst case, the malloc operations are O(log #segments). In other words, there are
potentially repeated split operations that must occur, but the maximum number of
splits is based on the maximum and minimum block size powers. Each split
operation is O(1). In this example, the maximum number of splits is 5. In the
example above there were 5 splits required on the first allocation. The second
allocation required 0 splits. The third required two splits. The fourth 0 splits.
The fifth and final allocation required 0 splits.

A heap with this maximum block size and minimum block size is initialized as
shown in :numref:`heap-init`. Since the heap manager always manages blocks sizes
of powers of 2, a heap is initialized by providing the maximum and minimum block
size powers. In :numref:`heap-init` the 10 is the 1024 byte maximum block size
and 5 is the 32 byte minimum block size.

.. code-block:: C
    :linenos:
    :caption: **Heap Initialization**
    :name: heap-init

    // make a heap of size 1K with 32 byte segments as minimum block size. How much space
    // is required? This call determines how much space is required for a heap with
    // maximum block size of 2^10 and minimimum block size of 2^5. In other words,
    // a 1K to 32 byte block size heap.

    dragonError_t rc;
    size_t heap_size;

    dragon_heap_size(10, 5, DRAGON_LOCK_FIFO, &heap_size);

    space = (dragonDynHeap_t*)malloc(heap_size);

    clock_gettime(CLOCK_MONOTONIC, &t1);

    rc = dragon_heap_init(space, &heap,32,5,DRAGON_LOCK_FIFO, NULL);


As blocks are freed, they are joined together into larger free blocks if the
block and its buddy are free. The buddy of a block is its odd or even neighbor in
the bitset for the block size. Given the design of the heap manager, the bitset
at a given block size is used to find the neighboring buddy of a block. If the
block index is even, the neighbor is at :math:`index + 1`. If the block index is
odd, the neighbor index is at :math:`index - 1`.

For instance, segment 0 in the allocation of :numref:`heap-allocations` has
segment 1 as its buddy because they are at index 0 and index 1 of the list of
segments. The block starting at segment 2 has its buddy starting at index 0, but
since index 0 is currently split, the buddy of the green block is not available
for joining to it once it is freed. To illustrate this joining of blocks,
consider the following sequence of free requests.

Freeing the Green Block Starting at Segment 2
---------------------------------------------

When the green block is freed we examine its buddy which starts at segment 0 and
has a segment span of 1 segment. Since the block starting at segment 0 is not
free, the green block cannot be joined with its buddy. The algorithm doesn't
consider anything further, but because segment 0 is in a block of 32 bytes and
the green block is part of a block of 64 bytes, they could not be joined either
(at this point anyway).

.. figure:: images/heapfree1.png
    :name: heap-free-green

    **After Freeing the Green Block here**

Freeing the Purple Block Starting at Segment 4
----------------------------------------------

Freeing the purple allocation starting at segment 4 examines segment 0 as a
potential buddy to join with. However, the segment at 0 is not free and again
does not result in any joining of blocks. At this point, there are three free
blocks that are available in the heap. The segment 1 is a 32 byte block. The
segments 2 and 3 make up a 64 byte free block. Finally, the segments 4-6 make up
a 128 byte free block.

.. figure:: images/heapfree2.png
    :name: heap-free-purple

    **After Freeing the Purple Block**

Freeing the Yellow Block Starting at Segment 16
-----------------------------------------------

The 512 byte block starting at segment 16 is freed next and results in once again
examining its buddy at segment 0. Again, segment 0 is not free and no further
joining of blocks is possible.

.. figure:: images/heapfree3.png
    :name: heap-free-yellow

    **After Freeing the Yellow Block**

Freeing the Orange Block Starting at Segment 0
----------------------------------------------

Finally, freeing segment 0 results in examining the buddy block. Since segment 0
is in a block of 32 bytes, the buddy is at segment 1 which is free. These two one
segment blocks are joined together forming a 64 byte free block. But, joining
cascades, resulting in now looking at the 64 byte block starting at segment 2.
Again, this block is free so the two blocks are joined, forming a 128 byte block.
But again, the 128 byte block starting at segment 4 is free and is joined with
the 128 byte block starting at 0 to form a 256 byte block. The buddy of this 256
byte block starts at segment 8 which is not free. So the joining of blocks stops
at this point.

At this point there are two free blocks: a 256 byte block starting at segment 0
and a 512 byte block starting at segment 16.

.. figure:: images/heapfree4.png
    :name: heap-free-orange

    **After Freeing the Orange Block**

Freeing the Maroon Block Starting at Segment 8
----------------------------------------------

Freeing the maroon block starting at segment 8 results in examining the buddy
starting at segment 0. Since it is free and the same size as the block being
freed, the blocks are joined together into a block of 512 bytes, but since its
buddy is also free and the same size, the two 512 byte blocks are joined together
into one 1K block.

.. figure:: images/heapfree5.png
    :name: heap-free-maroon

    **After Freeing the Maroon Block**

As mentioned above, Managed Memory provides a manifest that tracks all
allocations in shared memory. When an allocation is freed, it is removed from the
manifest so that it can only be freed by one and only one process. A lock on the
manifest itself guarantees that multiple processes cannot free an allocation more
than once. The existence of the manifest means that allocations do not need to be
tracked by the heap manager. The heap manager only needs to track free blocks
within the heap.

Data Structures and Handles
============================

As shown in :numref:`heap-bitsets` the heap manager is implemented with an array
of bitsets called *free*, a *preallocated* bitset, an array of BCast *waiters*,
and a lock. The heap has a handle that is either *inited* when creating the heap or
*attached* when attaching to the heap. The handle definition is provided in :numref:`heap-handle.`


.. code-block:: C
    :name: heap-handle
    :linenos:
    :caption: **C Handle Definition**

    typedef struct dragonDynHeap_st {
        uint64_t * base_pointer;
        dragonLock_t dlock;
        volatile atomic_uint * num_waiting;
        uint64_t segment_size;
        uint64_t num_segments;
        uint64_t total_size; // Not stored in shared mem, but computed.
        uint64_t num_block_sizes;
        uint64_t biggest_block;
        dragonBitSet_t* free; // array of sets - one for each block size.
        dragonBitSet_t preallocated; // one set. bit is set of corresponding block was pre-allocated.
        dragonBCastDescr_t* waiters; // array of bcasts for waiters.
    } dragonDynHeap_t;

The handle structure copies fields that can safely be accessed from either the
meta-data or the handle (i.e. constants like segment_size, num_segments,
total_size, num_block_sizes, and biggest_block). The pointers within the handle
point to shared data structures in the meta-data. The free BitSet pointer is a
pointer to an array of BitSets. The *waiters* is an array of BCasts.

While a description of fields in the handle is provided here, no data in the
handle should be accessed directly. The handle is to be used on calls to the API
and should be treated as an opaque type. However, the heap is intended to be used
internally in managed memory pools and is not written for external use.

Performance of Malloc and Free
==============================

Each free block of the heap corresponds to a bit within one of the free bitsets.
When a block is freed it may be joined together into a larger free block if its
buddy is free. If its buddy is not free, then the free bit for the block that
corresponds to its size and block location is set to a 1. There is no searching
that needs to be done to find a free block of the right size. Finding a block is
a O(1) BitSet lookup operation.

The entire structure and current state of the heap can be determined from the
segment size, number of segments, and free set. The free lists are kept to make
it possible to have O(1) malloc operations. The worst case complexity of the
malloc operation is O(log (max block size - min block size)). In other words, the
worst case is O(n) where n is the number of powers of 2 between the maximum block
size and the minimum block size. This is a result of potential splits of blocks
that occur when a malloc is called. This results in an amortized complexity of
O(1) for malloc.

The free operation needs to know the block size of the block being freed which is
provided by the manifest entry which keeps track of the allocation's size. Again
the lookup of the buddy free bit can be done in O(1) time. Worst case blocks are
joined together as a result of a free all the way up to the maximum sized block.
This too has an amortized complexity of O(1) assuming random frees and mallocs.

In a 4GB heap with minimum block size of 32 bytes, running on a reasonable test
node, a test was executed with 60 random calls to malloc. Fifty-two of them were
able to be satisfied. Eight required blocks that were not available and had to be
rejected. The average malloc execution time was 0.000007 seconds or 7
microseconds. The average free time for the 52 blocks was negligible at 0.000000
seconds to the granularity of time in the test. The longest free time observed
was 0.000001 seconds or 1 microsecond.

Heap Client API
===============

The client API is meant for components that want to allocate and manage a heap.
The actual allocation of the space for the heap is outside the scope of this API.
The user of this API must allocate the space and where and how that allocation is
done is irrelevant to this API. The heap can be managed in any memory
adderessable address space. The performance of the API primitives is dependent on
the address space being random accessible memory.

Structures
----------

The enumeration of error codes and the handle definition are the two structures
that are defined for the heap.

The Handle
^^^^^^^^^^

.. doxygenstruct:: dragonDynHeap_t
    :members:

Statistics
^^^^^^^^^^

The dragonHeapStatsAllocationItem_t below is located in the managed_memory.h
include to make it easily sharable with other code that may want to pass along
these free block stats.

.. doxygenstruct:: dragonHeapStatsAllocationItem_t
    :members:

.. doxygenstruct:: dragonHeapStats_t
    :members:

API
---

These are the user-facing API calls for heap management.

Life-Cycle
^^^^^^^^^^

Functions for creating or attaching to a heap.

.. doxygengroup:: heapmanager_lifecycle
   :content-only:
   :members:

Operation
^^^^^^^^^^
 Functions for interacting with a heap.

.. doxygengroup:: heapmanager_operation
   :content-only:
   :members:

Query
^^^^^^

Functions useful for debugging and understanding performance.

.. doxygengroup:: heapmanager_query
   :content-only:
   :members:
