.. _HeapManagerCython:

`dragon.heapmanager` -- Heap allocations through the Heap Manager library
=========================================================================

This page documents the Cython interface for the Dragon Heap Manager library.  For a description of the Heap
Manager and its functionality please refer to [link to heap manager doc goes here].  Example usage of this API
can be found at the bottom of this document.

All functions and variables listed as C language are implemented with the *cdef* keyword and are not directly
accessible from python-space.

Objects
++++++++

BitSet
###############

.. py:class:: BitSet

   BitSet object.  For internal testing use of the bitset library used by the heapmanager library.

	.. c:var:: dragonBitSet_t _set

	   BitSet struct handle

	.. c:var:: void * _mem_ptr

	   Pointer to the heap this bitset is part of

	.. py:attribute:: _attach

	   Whether this bitset was initialzed to a new heap or attached to an existing one

	.. py:classmethod:: init(num_bits, space)

	   Initializes a new BitSet object handle to the provided memory space

	   :param size_t num_bits: Number of bits the handle should track
	   :param space: A bytearray or memoryview object the bitset should attach to
	   :return: BitSet object
	   :rtype: BitSet
	   :raises: RuntimeError

	.. py:classmethod:: attach(space)

	   Attaches a new BitSet object handle to an existing memory space with an initialized BitSet

	   :param space: A bytearray or memoryview object that already has an initialized BitSet
	   :return: BitSet object
	   :rtype: BitSet
	   :raises: RuntimeError

	.. py:classmethod:: size(num_bits)

	   Get the needed size for the BitSet based on the number of bits to track

	   :param size_t num_bits: Number of bits to be tracked by the BitSet handle
	   :return: Required memory size for metadata and bits to track
	   :rtype: size_t


	.. py:method:: detach()

	   Detach the BitSet from its heap.  Call after using attach.

	   :raises: RuntimeError if not attached

	.. py:method:: destroy()

	   Destroy the BitSet from its heap.  Call after using init.

	   :raises: RuntimeError if not initialized

	.. py:method:: get_num_bits()

	   :return: The number of bits in the set
	   :rtype: size_t

	.. py:method:: get_bit(idx)

	   :return: The state of the bit at the specified index
	   :rtype: unsigned char
	   :raises: RuntimeError if out of index range

	.. py:method:: set_bit(idx)

	   Set the specified bit index to 1 unconditionally.

	   :param int idx: Index to set to 1
	   :raises: Runtime error if out of index range

	.. py:method:: reset_bit(idx)

	   Set the specified bit index to 0 unconditionally.

	   :param int idx: Index to set to 0
	   :raises: Runtime error if out of index range

	.. py:method:: right_zeroes(idx)

	   :param int idx: Index to check right of
	   :return: The number of 0 bits to the right of the specified index
	   :rtype: int
	   :raises: Runtime error

	.. py:method:: dump(title, indent)

	   :param str title: Title to place in header of dump string
	   :param str indent: TODO: Descriptor
	   :raises: RuntimeError

	.. py:method:: dump_to_str(title, indent)

	   Same behavior as dump but returns to string instead of stdout

	   :param str title: Title to place in header of dump string
	   :param str indent: TODO: Descriptor
	   :return: Dump
	   :rtype: str
	   :raises: RuntimeError

Heap
###############

.. py:class:: Heap

   Heap object.  Used to manage allocated memory.  Can be explicitly detached from a heap or destroyed,
   otherwise GC should call the correct function when the objects lifetime ends.

   .. py:attribute:: num_segments

   .. py:attribute:: segment_size

   .. py:attribute:: num_freelists

   .. py:attribute:: recovery_needed

   .. py:attribute:: init_handle

   .. py:attribute:: exclusive_access

	  Return a pointer to the exlusive access mutex

	  .. py:classmethod:: size(max_sz_pwr, min_sz_pwr, alignment)

		 Calculate the necessary memory size for the given powers of two and segment alignment.

		 :param size_t max_sz_pwr: Power of 2 to allocate for the heap
		 :param size_t min_sz_pwr: Power of 2 for minimum segment size
		 :param size_t alignment: Memory alignment size (Multiples of 8)
		 :return: Size of heap to be allocated
		 :rtype: size_t
		 :raises: RuntimeError

	  .. py:classmethod:: init(max_sz_pwr, min_sz_pwr, alignment, mem_addr)

		 Not to be confused with the default constructor.  Takes the same parameters as the size class method
		 as well as a pointer to an allocated heap.

		 :param size_t max_sz_pwr: Power of 2 to allocate for the heap
		 :param size_t min_sz_pwr: Power of 2 for minimum segment size
		 :param size_t alignment: Memory alignment size (Multiples of 8)
		 :param bytearray mem_addr: Bytearray or memoryview object to initialize heap with
		 :return: New heap manager
		 :rtype: Heap
		 :raises: RuntimeError

	  .. py:classmethod:: attach(mem_addr)

		 Creates and attaches a new handle to an existing heap.

		 :param bytearray mem_addr: Bytearray or memoryview object to attach heap to
		 :return: New heap manager handle
		 :rtype: Heap
		 :raises: RuntimeError

	  .. py:method:: detach()

		 Detaches the handle from its heap.

		 :raises: RuntimeError

	  .. py:method:: destroy()

		 Destroys the handle.  To be used when the object was created using *init*.

		 :raises: RuntimeError


	  .. py:method:: malloc(size)

		 Allocates the requested size of memory in the heap manager.  Returns a new DragonMem object to hold the pointer.

		 :param size_t size: Size of memory to request from the heap manager
		 :return: New memory object
		 :rtype: MemoryView
		 :raises: RuntimeError

	  .. py:method:: free(mem_obj)

		 Free the memory held by the memoryview object that was allocated using the *malloc* method.

		 :param memoryview mem_obj: Memory object
		 :return: None
		 :raises: RuntimeError if recovery is needed first

	  .. py:method:: recover()

		 Perform heap recovery when corrupted.

		 :return: None
		 :raises: RuntimeError if recovery needed flag is not set

	  .. py:method:: get_stats()

		 Get an object that contains statistics about the heap.

		 :return: Statistics object
		 :rtype: MemStat
		 :raises: RuntimeError

	  .. py:method:: dump(title)

		 Print out a dump of heap information to stdout

		 :param str title: Title to include at the top of the output
		 :return: None
		 :raises: RuntimeError

	  .. py:method:: dump_to_str(title)

		 Return a dump of heap information to a string

		 :param str title: Title to include at the top of the output
		 :return: Heap info
		 :rtype: String
		 :raises: RuntimeError

	  .. py:method:: dump_to_file(title, fobj)

		 Not implemented

		 :param str title: Title to include at the top of the output
		 :param fobj: Python file object
		 :raises RuntimeError:


Example usage
------------------

.. code-block::

   from dragon.malloc.heapmanager import Heap

   # Get a 4GB Heap with 4k minimum segment sizes
   heap_size = Heap.size(32, 12, 4096)
   # Allocate a memory object
   memobj = bytearray(heap_size)
   # Initialize the handle
   heap_handle = Heap.init(32, 12, 4096, bytearray)
   # Attach another handle
   heap_handle_2 = HeapHandle.attach(bytearray)

   # Get some memory from the heap
   tmp_mem = heap_handle.malloc(32)
   # Do some stuff with the memory...
   tmp[0:10] = b'Hello world'

   # Free the memory
   # When recovery is needed, runtime exception will be thrown
   try:
       heap_handle.free(tmp_mem)
   except:
       heap_handle.recover()
	   heap_handle.free(tmp_mem)

   # Detach the other handle
   heap_handle_2.detach()

   # Dump heap info
   heap_handle.dump("Heap info")

   # Destroy the handle
   heap_handle.destroy()
