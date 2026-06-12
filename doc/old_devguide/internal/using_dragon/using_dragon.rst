.. _UsingDragon:

Using Dragon
++++++++++++

Here we describe starting, using and programming for the Dragon runtime.

**FIXME: It might be useful in the future to show how Dragon can be used in a variety of scenarios, including various examples. This immediately translates to Acceptance Tests, i.e. the examples shown here could be part of the 
test suite at the highest level.**

.. toctree::

    running_dragon.rst
    python_multiprocessing.rst
    dragon_native.rst

**FIXME: From Launcher**

The user program interacts with the Python package *multiprocessing* when the user program is a Python
program. If it is not a Python program, the user program may use lower-level APIs for the Dragon services. The
Dragon version of multiprocessing is written to use the Dragon services in it's implementation.

Dependencies
============

The Dragon run-time :ref:`Services` and the :ref:`Launcher` in particular currently require the following setup:

* The login node has access to the same identical file system as the compute nodes, i.e. a *shared
  filesystem*. In this way, the login node can check for existence of files on the login node and the same
  files will be found in the same locations on the compute nodes.
* The only supported operating systems are Linux (and possibly other Un\*x) variants that support *`POSIX`*.
* The compute nodes must have access to *shared memory*.
* *Python version 3* is installed and accessible on all nodes.
* Standard *Python multiprocessing* for the :ref:`DragonWithPythonMultiprocessing` use case.
* The *SLURM* workload manager is present for the multi-node case.
* *MRNet* can be run on the system.
* The Cray common tools interface (*CTI*) is present and can be used for starting programs on compute nodes.

