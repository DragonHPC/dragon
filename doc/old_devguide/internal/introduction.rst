.. _Introduction:

Introduction 
++++++++++++

.. this is for motivation 

Dragon is a runtime to manage concurrent POSIX processes, shared memory and inter-process communication on a
wide variety of distributed hardware. 

In particular, the Dragon :ref:`Infrastructure` provides an abstraction of the distributed system so that
processes running both on-node and off-node (on another compute node) interact in precisely the same  way.
This is the primary benefit of having a multi-node Dragon implementation. Programs using the Dragon
:ref:`Services` can scale without any rewriting of code from one compute node, to thousands of nodes.

With its infrastructure services and APIs, Dragon provides a flexible way for developers to write code that
can:

.. these are already the top level software requirements for our components

* start the runtime on multiple nodes and collect process output on the login node
* spawn, monitor and destroy processes on a single compute node
* manage a large number of processes on multiple compute nodes 
* safely share memory among processes on a single node with a zero copy approach
* safely share memory among processes on multiple nodes asynchronously.
* asynchronously and bidirectionally communicate process data across multiple nodes
* run efficiently on a laptop as well as on a supercomputer without additional dependencies or changes.
* requires only minimal dependence on wide-spread standard libraries: Python 3 and POSIX

The Dragon runtime currently supports the following use cases for developers:

1. :ref:`Python multiprocessing <DragonWithPythonMultiprocessing>`: Using Dragon with the `Python multiprocessing API`_.

See :ref:`UsingDragon` for more information on how to run and program for Dragon.


.. Requirements Analysis
.. =====================

.. This is implementation _independent_ functionality and non-functional (e.g. performance, security, reliability)
.. constraints to the solution. 
.. These should not reference any software components directly, but have to reference user roles and use cases.

.. .. _UserRoles:
.. User Roles
.. ==========
.. * Dragon User
.. * Dragon developer
.. * Dragon security admin (?)
..
.. .. _StakeholderRequirements:

.. Stakeholder Requirements
.. ========================
.. 1. As a Dragon User, I want to start my program using the Dragon runtime on my laptop as well as on a supercomputer, so that I get the optimal performance on both systems.
.. 2. As a Dragon Developer, I want to be able to write code that runs on a laptop as well as on a supercomputer , so that I dont have to adapt my program to the underlying hardware and run into more dependencies.
.. 3. As a Dragon Developer, I want to use Dragon with Python Multiprocessing, so that I don't have to adapt my existing programs to Dragon.
.. 4. As a Dragon Developer, I want start processes on any number of compute nodes, to write parallel programs.
.. 5. ... TBD ...

.. .. _SoftwareRequirements:

.. Software Requirements
.. =====================
..
.. 1. The Dragon Launcher has to start the runtime on multiple nodes and collect process output from every Shepherd and transfer it on the login node
.. 2. The Dragon Shepherd has to spawn, monitor and destroy processes on a single compute node
.. 3. The Dragon Global Services has to manage a large number of processes on multiple compute nodes to provide control for user code.
.. 4. The Dragon Channels have to allow POSIX processes to safely share memory among processes on a single node with a zero copy approach 
.. 5. The Dragon Channels have to allow POSIX processes safely share memory among processes on multiple nodes without blocking 
.. 6. The Dragon Transport Agent has to use Dragon Channels to asynchronously and bidirectionally communicate process data across multiple nodes
.. 7. The Dragon Infrastructure has to enable user to code to show competitive weak and strong scaling on single and multiple nodes
.. 8. The Dragon Infrastructure has to require only minimal wide-spread dependencies to maximize portability.



.. ------------------------------------------------------------------------
.. External Links 
.. _Python multiprocessing API: https://docs.python.org/3/library/multiprocessing.html
