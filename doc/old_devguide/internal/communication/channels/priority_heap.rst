.. _PriorityHeap:

Heap Priority Queue
+++++++++++++++++++

This heap implementation supports trees of arbitrary fan-out (base).  If the heap is going to be very deep it
is likely better to use a larger value for the base.  The caller can embed an arbitrary number of values with
any item.  These values are moved in the heap with the item similar to a kev/value with multiple values per
key.  For example, the OT tracks at least the index of the message block for the message in each item.

The heap is designed to be fully mapped into memory.  All state information lies there such that any process
can attach to the heap and begin operations on it.  This heap implementation is **not** thread safe, however.
The reason for this is that :ref:`Channels` manages thread safety carefully itself for better performance.  To
use this heap in isolation with multiple threads or processes the caller must protect each call with a lock.

.. _PriorityHeapAPI:

Heap API
========

TBD. Write this to pull documentation from the C source code. Use channels.c and it's associated
documentation as a guide. Don't forget to include doxygen groups.

.. This documentation is incorrect. It has the wrong names of functions.
.. Structures and Types
.. --------------------

.. .. c:type:: uint64_t dragonHeapLongUint_t

.. .. c:type:: uint32_t dragonHeapUint_t

.. Heap Functions
.. --------------

.. .. c:function:: size_t dragon_heap_size(dragonHeapLongUint_t capacity, dragonHeapUint_t nvals_per_key)

..     Returns the number of bytes to allocate to support a heap with the given properties.  *capacity* is the
..     maximum number of items the heap can manage.  *nvals_per_key* is the number of values of type
..     :c:type:`dragonHeapLongInt_t` values that are carried along with an item.

..     Returns number of bytes.

.. .. c:function:: dragonError_t dragon_heap_init(dragonHeap_t * heap, dragonHeapUint_t base, dragonHeapLongUint_t capacity, dragonHeapUint_t nvals_per_key, void * ptr)

..     Map a new heap with the provided properties onto the memory pointed to by *ptr*.  The caller should have
..     used *dragon_heap_size()* to allocate sufficient memory to support the heap.  *base* is the base of the
..     tree to use for the heap (e.g., 2 or 3).  *capacity* is the number of items the heap can support.
..     *nvals_per_key* is the number of :c:type:`dragonHeapLongInt_t` values that are carried along with an item.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonError_t dragon_heap_attach(dragonHeap_t * heap, void * ptr)

..     Attach to the heap mapped to memory pointed to by *ptr*.  To be usable another process should have called
..     :c:func:`dragon_heap_init()` with the same memory.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonError_t dragon_heap_detach(dragonHeap_t * heap)

..     Detach from the heap  The heap remains usable and can be re-attached.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonError_t dragon_heap_destroy(dragonHeap_t * heap)

..     Remove the mapping of *heap* onto the memory originally given in :c:func:`dragon_heap_init()`.  The heap is no
..     longer usable upon return of this call.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonError_t dragon_insert_item(dragonHeap_t * heap, dragonHeapLongUint_t * vals)

..     Insert an item with lowest priority into the heap *heap*.  The values for the item are given in the
..     :c:type:`dragonHeapLongUint_t` array *vals*.  *vals* must be of size *nvals_per_key* as given when *heap*
..     was initialized with :c:func:`dragon_heap_init()`.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonError_t dragon_insert_urgent_item(dragonHeap_t * heap, dragonHeapLongUint_t * vals)

..     Insert an item into the heap *heap* with the highest priority.  The values for the item are given in *vals*.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonError_t dragon_extract_highest_priority(dragonHeap_t * heap, dragonHeapLongUint_t * vals, dragonHeapLongUint_t * priority)

..     Return the highest priority item in the heap *heap*.  If the heap is empty the error code
..     ``DRAGON_HEAP_EMPTY`` is returned.  If an item exists, the values for the item are placed in the provided
..     :c:type:`dragonHeapLongUint_t` array *vals*. *vals* must be of size *nvals_per_key* as given when *heap*
..     was initialized with :c:func:`dragon_heap_init()`. The priority of the item is returned in *priority*.

..     Returns ``DRAGON_SUCCESS`` or an error code.