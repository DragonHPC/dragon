Hex Dump
==============

The Hex Dump service provides a convenient interface for dumping untyped memory to a file or to standard
output. It is useful in debugging problems that rely on low-level memory state. Hex Dump is given a pointer
and a length to dump to the screen. Hex Dumps are printed in groups of 16 bytes in hexadecimal format.

The Hex Dump utility will suppress 0's by displaying the first and last sixteen 0's of a region of memory and
suppressing also 0's in between. This allows large regions of memory to be dumped without overwhelming the
output. Suppressed 0's are indicated by an ellipsis (i.e. ...).

To the right of the hexadecimal interpretation of the bytes, the same 16 bytes are interpreted as ASCII
characters as a convenience of the data contains ASCII data. Non-printable ASCII data is printed as a dot or
period (i.e. a .).

Example
-------------------------------

Hex Dump has an easy to use interface. You provide a pointer and a length to it to dump. You can also provide
an indentation string.

.. code-block:: C
    :caption: **Hex Dump Example Code**
    :name: hex-dump-example-code

    hex_dump_to_fd(fd, "BITS",(void*)set->data,num_bytes,indent);

When invoked, The output looks something like that found in :numref:`hex-dump-example-output`. Note how lines with 0's are suppressed.
The *fd* is a file, which includes the possibility of using *stdout* or *stderr*. A title comes second
followed by a pointer to the data and the number of bytes to dump. Finally, the *indent* is a null-terminated
string to print before each line of the dump.

.. code-block:: text
    :caption: **Hex Dump Sample Output**
    :name: hex-dump-example-output

    *  BITS:
    *    00007FCF60C97070 80 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00  ................
    *    00007FCF60C97080 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00  ................
    *    ...
    *    00007FCF60CB7060 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00  ................

API Services
++++++++++++++++++++++++++

.. c:function:: void hex_dump(const char *title, const void *addr, const size_t len, const char* indent)

    Prints the memory pointed to by *addr* for length *len* printing *indent* before each line. Dumping memory
    prints 16 bytes per line and suppresses all but the first and last 16 bytes of regions of all zeroes.

    **Returns**

    No return value.

    **Example Usage**

    .. code-block:: C

        hex_dump("BITS",(void*)set->data,num_bytes,indent);



.. c:function:: void hex_dump_to_fd(FILE* fd, const char *title, const void *addr, const size_t len, const char* indent)

    Works the same as :c:func:`hex_dump` but prints to the file *fd* instead.

    **Returns**

    No return value.

    **Example Usage**

    .. code-block:: C

        FILE* fd = ?;
        hex_dump_to_fd(fd, "BITS",(void*)set->data,num_bytes,indent);
