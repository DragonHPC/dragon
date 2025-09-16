.. _RunningDragon:

Running Dragon
++++++++++++++

Launching a Dragon multiprocessing program is done in a similar fashion to starting a program with the workload manager.

.. code-block:: bash
    :caption: **Invoking the Launcher**

    usage: dragon [-h] [-N NODE_COUNT] [-s | -m] [PROG] [ARG [ARG ...]]

Available options for the *dragon* launch command:

* *-N, --nodes* : specifies the number of nodes to use if running using dragon multi-node support.
NODE_COUNT must be less or equal to the number of available nodes within the WLM allocation. A value of
zero (0) indicates that all available nodes should be used (the default).

* *-s, --single-node-override* : Override automatic launcher selection to force use of the single node launcher.
It is considered an error to pass both --single-node-override and --multi-node-override options.

* *-m, --multi-node-override* : Override automatic launcher selection to force use of the multi-node launcher.
It is considered an error to pass both --single-node-override and --multi-node-override options.

* *-h, --help* : Print this help message and exit

*PROG* specifies an executable program. to be run on the primary compute node. In this case, the file
may be either executable or not. If *PROG* is not executable, then Python version 3 will be used to
interpret it. All *ARG* command-line arguments after *PROG* are passed to the program as its arguments. The
*PROG* and *ARG* parameters are optional.

.. External links

.. _Slurm srun options: https://slurm.schedmd.com/srun.html 
