.. _cbook_policy:

Using Dragon Policies to Control Placement and Resources for Processes
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

This example shows how policies can be passed and applied to processes that are started from a process group.
Policies can be applied to the whole group as well as to individual processes.
In this example, we apply a group policy that restricts the cpu affinity of all processes that are part of the group.
A policy is then applied in batches to processes that are part of the group that restrict the placement of the processes to specific nodes.
To demonstrate this restricted placement, we launch an MPI program, `mpi_hello`, that returns the hostname that it is running on along with its local process ID and its rank within the group.

Note, if the group policy and process policy conflict, an error is not raised.
Instead, we resolve conflicts based on the following hierarchy: process policies > group policies > global policy.

This example consists of the following files:

* `policy_demo.py` - This is the main file. It defines the policies and process group, launches the group, and then parses the output from the ranks before printing the output.

* `mpi_hello.c` - This file contains a simple MPI program that prints the hostname, pid, and rank within the MPI group.

Below, we present the main python code (`policy_demo.py`) which acts as the coordinator of the workflow.
The code of the other files can be found in the release package, inside `examples/dragon_native/mpi` directory.


.. literalinclude:: ../../examples/dragon_native/mpi/policy_demo.py
    :language: python

How to run
==========

Example Output when run on 4 nodes with 8 AMD GPUs per node
-------------------------------------------------------------------------------------

.. code-block:: console
    :linenos:

    > make
    gcc -g  -pedantic -Wall -I /opt/cray/pe/mpich/8.1.27/ofi/gnu/9.1/include -L /opt/cray/pe/mpich/8.1.27/ofi/gnu/9.1/lib  -c mpi_hello.c -o mpi_hello.c.o
    gcc -lm -L /opt/cray/pe/mpich/8.1.27/ofi/gnu/9.1/lib -lmpich  mpi_hello.c.o -o mpi_hello
    > salloc --nodes=4 --exclusive
    > dragon policy_demo.py
    Using 2 of 4
    pinoak0015 has AMD GPUs with visible devices: [0, 1, 2, 3, 4, 5, 6, 7]
    pinoak0016 has AMD GPUs with visible devices: [0, 1, 2, 3, 4, 5, 6, 7]
    pinoak0014 has AMD GPUs with visible devices: [0, 1, 2, 3, 4, 5, 6, 7]
    pinoak0013 has AMD GPUs with visible devices: [0, 1, 2, 3, 4, 5, 6, 7]
    4294967298 returned output: Hello world from pid 57645, processor pinoak0015, rank 0 out of 16 processors

    4294967299 returned output: Hello world from pid 57646, processor pinoak0015, rank 1 out of 16 processors

    4294967300 returned output: Hello world from pid 57647, processor pinoak0015, rank 2 out of 16 processors

    4294967301 returned output: Hello world from pid 57648, processor pinoak0015, rank 3 out of 16 processors

    4294967302 returned output: Hello world from pid 57649, processor pinoak0015, rank 4 out of 16 processors

    4294967303 returned output: Hello world from pid 57650, processor pinoak0015, rank 5 out of 16 processors

    4294967304 returned output: Hello world from pid 57651, processor pinoak0015, rank 6 out of 16 processors

    4294967305 returned output: Hello world from pid 57652, processor pinoak0015, rank 7 out of 16 processors

    4294967306 returned output: Hello world from pid 56247, processor pinoak0016, rank 8 out of 16 processors

    4294967307 returned output: Hello world from pid 56248, processor pinoak0016, rank 9 out of 16 processors

    4294967308 returned output: Hello world from pid 56249, processor pinoak0016, rank 10 out of 16 processors

    4294967309 returned output: Hello world from pid 56250, processor pinoak0016, rank 11 out of 16 processors

    4294967310 returned output: Hello world from pid 56251, processor pinoak0016, rank 12 out of 16 processors

    4294967311 returned output: Hello world from pid 56252, processor pinoak0016, rank 13 out of 16 processors

    4294967312 returned output: Hello world from pid 56253, processor pinoak0016, rank 14 out of 16 processors

    4294967313 returned output: Hello world from pid 56254, processor pinoak0016, rank 15 out of 16 processors
