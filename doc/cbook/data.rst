Dragon Data
+++++++++++

Data and computation, the two go hand in hand. This section explores how to distribute data to your
computation. The Dragon Distributed Dictionary (`DDict``) is one means to provide distributed data access.
The DDict also provides synchronization in between processes based on data availability. Check out the
DDict example here for an example of distributing data, synchronization between processes using the DDict's
working set and checkpointing, and resilience by persisting checkpoints to disk.

.. toctree::
    :maxdepth: 1

    dragon_dict.rst