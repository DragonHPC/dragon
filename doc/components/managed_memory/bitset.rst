BitSet
==============

The BitSet datatype provides a low-level bit set. It provides the usual insert, delete, and membership
functions. It has one special purpose function required for heap management that counts the zeros to the right
starting at a position within the bit set.

This low-level implementation builds the set inside a pre-defined space that is passed into the initialization
function for the datatype. The implementation does not dynamically allocate any data. All space for the
implementation is determined before the datatype is instantiated.

An Example of Using the BitSet
-------------------------------

Consider a BitSet of 1024 bits. The following code creates a BitSet, initializes it, and exercises a few of
the functions.

.. code-block:: C
    :linenos:
    :caption: **Figure 1: A BitSet Example**

    size_t bitsetsize;
    dragonBitSetErr_t brc;
    bitsetsize = dragon_bitset_size(1024);
    void* bsptr = malloc(bitsetsize);
    dragonBitSet_t bset;
    brc = dragon_bitset_init(bsptr,&bset,1024);
    if (brc != DRAGON_SUCCESS)
        printf("Error Code was %u\n",brc);
    brc = dragon_bitset_set(&bset, 0);
    if (brc != DRAGON_SUCCESS)
        printf("Error Code was %u\n",brc);
    brc = dragon_bitset_set(&bset, 400);
    if (brc != DRAGON_SUCCESS)
        printf("Error Code was %u\n",brc);
    brc = dragon_bitset_set(&bset, 916);
    if (brc != DRAGON_SUCCESS)
        printf("Error Code was %u\n",brc);
    brc = dragon_bitset_set(&bset, 42);
    if (brc != DRAGON_SUCCESS)
        printf("Error Code was %u\n",brc);
    unsigned char bit;
    brc = dragon_bitset_get(&bset, 42, &bit);
    if (brc != DRAGON_SUCCESS)
        printf("Error Code was %u\n",brc);
    if (bit == 1)
        printf("That was a one\n");
    brc = dragon_bitset_reset(&bset, 42);
    if (brc != DRAGON_SUCCESS)
        printf("Error Code was %u\n",brc);
    brc = dragon_bitset_dump("A Bit Dump", &bset, "");
    if (brc != DRAGON_SUCCESS)
        printf("Error Code was %u\n",brc);
    size_t val = 0;
    dragon_bitset_zeroes_to_right(&bset,0,&val);
    printf("The number is %lu\n",val);

The output from the example program in figure 1 is given in figure 2. The bits in the bitset display bit 0 on
the left, not the right. In this way, the bits display lexicographically in a dump from left to right for
easier reading. Bit 0 is the left-most bit in the dump while the last bit is lexicographically the last bit to
be displayed.

.. code-block:: text
    :caption: **Figure 2: BitSet Example Output**

    That was a one
    A Bit Dump
    Size of bitset is 125
    BITS:
      0000000001123268 80 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00  ................
      0000000001123278 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00  ................
      ...
      0000000001123298 00 00 80 00 00 00 00 00 00 00 00 00 00 00 00 00  ................
      00000000011232A8 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00  ................
      ...
      00000000011232D8 00 00 08 00 00 00 00 00 00 00 00 00 00           .............
    The number is 399


Meta-Data and Handles
--------------------------------

The meta-data/data structures required to implement the BitSet includes the number of bits in the BitSet as
the first 8 bytes, followed by enough bytes to hold the rest of the bits. Note that the BitSet is designed to
always use a multiple of 8 bytes for easy alignment with other data.

When a BitSet is initialized, a handle to the BitSet is also initialized. The handle is now the user of this
API accesses the BitSet. The handle structure is given in figure 3.

.. code-block:: C
    :caption: **Figure 3: BitSet Handle Definition**

    typedef struct dragonBitSet_st {
        size_t size;
        char* data;
    } dragonBitSet_t;

Initialization copies the *size* field from the pre-defined memory into the handle. The *data* pointer points
into the shared data.

**NOTE**: This implementation of BitSet is not locked and relies on some other locking mechanism for shared
access between processes or threads.

Performance of Operations
--------------------------------------------

The performance of all operations is O(1) in complexity except for the :c:func:`dragon_bitset_zeroes_to_right`
function, which is O(n). However, this function is optimized so that is O(n) where n is the number of words to
the right of the given bit. Whenever possible, entire words are examined rather than examining a bit at a time
in the :c:func:`dragon_bitset_zeroes_to_right` function, resulting in a very efficient O(n) operation.

BitSet Client API
-------------------

The API for using BitSets defines a shared datatype and a similar per-process handle definition.

Structures
++++++++++++

The enumeration of error codes and the handle definition are the two structures that are defined for the
BitSet datatype.

Error Codes
########################

.. c:enum:: dragonBitSetErr_t

    This is the BitSet error code enumeration. Most API calls return an error code as a result of calling
    them. The possible error codes for specific API calls are given with their function definitions, given
    below.

    .. c:enumerator:: DRAGON_SUCCESS

        Operation completed successfully.

    .. c:enumerator:: DRAGON_BOUNDS_ERROR

        The requested bit was out of the bounds of the set.

    .. c:enumerator:: DRAGON_BITSET_NULL_POINTER

        A null pointer was provided for the BitSet handle.

The Handle
####################

.. doxygenstruct:: dragonBitSet_t
    :members:


API
++++++++++++++++++++++++++

These are the user-facing API calls for bit sets.

Life-Cycle
############################

.. doxygenfunction:: dragon_bitset_size

**Example Usage**

.. code-block:: C

    size_t bitsetsize = dragon_bitset_size(1024);

----

.. doxygenfunction:: dragon_bitset_get_num_bits

**Example Usage**

.. code-block:: C

    dragonError_t brc;
    dragonBitSet_t bset;
    size_t num_bits;
    brc = dragon_bitset_init(bsptr,&bset,1024);
    if (brc != DRAGON_SUCCESS) {
        printf("Error Code was %u\n",brc);
    }
    brc = dragon_bitset_get_num_bits(bset, &num_bits);
    if (brc != DRAGON_SUCCESS) {
        printf("Error Code was %u\n",brc);
    }
    printf("Number of bits was %u\n", num_bits);

----

.. doxygenfunction:: dragon_bitset_init

**Example Usage**

.. code-block:: C

    dragonBitSet_t bset;
    brc = dragon_bitset_init(bsptr,&bset,1024);
    if (brc != DRAGON_SUCCESS) {
        printf("Error Code was %u\n",brc);
    }


----

.. doxygenfunction:: dragon_bitset_destroy

**Example Usage**

.. code-block:: C

    dragonBitSet_t set;
    // initialize and use the bitset. Then finally destroy it.
    dragon_bitset_destroy(&set);


----

.. doxygenfunction:: dragon_bitset_attach

**Example Usage**

.. code-block:: C

    dragonBitSet_t bset;
    //bsptr points at the previously initialized BitSet space.
    brc = dragon_bitset_attach(bsptr,&bset);
    if (brc != DRAGON_SUCCESS) {
        // handle it
    }

----

.. doxygenfunction:: dragon_bitset_detach

**Example Usage**

.. code-block:: C

    dragonBitSet_t set2; // attach this space to a BitSet.
    // Then later...
    brc = dragon_bitset_detach(&set2);

    if (brc != DRAGON_SUCCESS) {
        // handle it
    }

Services
####################################

.. doxygenfunction:: dragon_bitset_set

**Example Usage**

.. code-block:: C

    brc = dragon_bitset_set(&bset, 0);
    if (brc != DRAGON_SUCCESS)
        printf("Error Code was %u\n",brc);

-----

.. doxygenfunction:: dragon_bitset_reset

**Example Usage**

.. code-block:: C

    brc = dragon_bitset_reset(&bset, 0);
    if (brc != DRAGON_SUCCESS)
        printf("Error Code was %u\n",brc);


----

.. doxygenfunction:: dragon_bitset_get

**Example Usage**

.. code-block:: C

    unsigned char bit;
    brc = dragon_bitset_get(&bset, 42, &bit);
    if (brc != DRAGON_SUCCESS)
        printf("Error Code was %u\n",brc);

----

.. doxygenfunction:: dragon_bitset_zeroes_to_right

**Example Usage**

.. code-block:: C

    size_t val = 0;
    brc = dragon_bitset_zeroes_to_right(&bset,0,&val);
    if (brc != DRAGON_SUCCESS)
        printf("Error Code was %u\n",brc);
    else
        printf("The number is %lu\n",val);

Status/Debug
#############################

.. doxygenfunction:: dragon_bitset_dump

**Example Usage**

.. code-block:: C

    dragon_bitset_dump("Block Set", &set, "  ");

----

.. doxygenfunction:: dragon_bitset_dump_to_fd

**Example Usage**

Print a dump of the BitSet structure with the given *title* to standard output. The output looks similar to
this.

    .. code-block:: text

        *   Block Set
        *    Size of bitset is 131072
        *    BITS:
        *    00007F5218663068 80 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00  ................
        *    00007F5218663078 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00  ................
        *    ...
        *    00007F5218683058 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00  ................

    .. code-block:: C

        dragon_bitset_dump_to_fd(logfile, "Block Set", &set, "  ");

Python Interface
------------------------

This provides the Python interface to BitSet.

.. py:currentmodule:: dragon.heapmanager.BitSet

init
++++++++

.. autofunction:: init(const size_t num_bits, unsigned char [:] space)

**Example Usage**

.. code-block:: python

    # example using BitSet.init