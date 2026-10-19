.. _InfrastructureBootstrapping:

Infrastructure Bootstrapping
++++++++++++++++++++++++++++

This section needs updating.

The information provided in this section is subject to change at any time and should not be used by any *user*
code.

While the specific steps for bootstrapping the environment are provided here, this is an implementation detail
and no *infrastructure* code should access these variables directly or depend on the specific details provided
here. Instead, all infrastructure code should use the method described in :ref:`accessing parameters
<accessing-parms>` to refer to environment variables as needed by components of the Dragon run-time services.

Some of the parameters listed :ref:`in the parameters <parameters>` must be set by the :ref:`LocalServices` during
bringup of the Dragon runtime services. Others are needed by the Shepherd to complete startup of the services.

The *MODE*, *ALLOC_SZ*, *PRIMARY_INDEX*, *DEFAULT_SEG_SZ*, *INF_SEG_SZ*, *GS_MAX_MSGS*, *SH_MAX_MSGS*, *TEST*,
and *DEBUG* environment variables are set by the :ref:`Launcher` and provided to the Shepherd when it is
started. The Shepherd then makes sure that the following list of variables is set for all processes started by
the Dragon Runtime Services and also for :ref:`GlobalServices` running on the primary node. The launcher
will then pass those values along during infrastructure startup if certain values are ovverridden.

**FIXME: These need to be looked at again. **

* MODE
* ALLOC_SZ
* INDEX
* PRIMARY_INDEX
    * DEFAULT_PD
    * INF_PD
    * LOCAL_SHEP_CD
    * LOCAL_BE_CD
    * GS_CD
    * TEST
    * DEBUG
    * MY_PUID

In addition to these environment variables that are present on back end compute nodes, the Launcher Front End
and the Network Front End communicate through two environment variables, the DRAGON_CTI_EXEC and the
DRAGON_MRNET_EXEC variables. These values are set by the Launcher Front End to tell the Network Front End
code which programs to start when CTI launches its application (the Shepherd) and MRNet Launches its back end.
These two environment variables are only necessary between the Launcher Front End and the Network Front End.

The next sections contain specific details of how bootstrapping the value of these environment variables
differs between single and multinode startup. Each section below provides the details for setting the
environment variables in both modes.

Naming Conventions Critical to Bootstrapping
============================================

There are a few resources that are allocated early on during startup, a phase of bringup informally referred
to as the bootstrapping phase. These naming conventions are documented here.

* POSIX message queues used during multinode startup are named dragon_<username>_shep_in and
  dragon_<username>_shep_out. The two names must be available. Otherwise startup fails. These POSIX message
  queues are closed and deleted at teardown of the services.

* The Shepherd names the infrastructure shared memory pool segment as dragon_<username>_inf_shm during
  startup. This shared memory segment is deleted at teardown of the services.

* The Shepherd names the default shared memory pool segment used by user processes during interaction with the
  runtime services as dragon_<username>_proc_shm during startup. This shared memory segment is deleted at
  teardown of the services.

The <username> is included in the name of these resources to allow multiple users to start the Dragon Runtime
Services simultaneously on a node. This may be advantageous in some circumstances. For instance, a Superdome
processor may allow multiple simultaneous users. A conscious decision was made not to include a time stamp in
naming to prevent an error in programming from consuming all available shared memory segments and/or POSIX
message queues. If there is a bug that causes these resources to remain allocated after Dragon Runtime
Services exit, then we want to know about it as soon as possible.

Environment Variables Provided to Shepherd at Startup
=====================================================

The following environment variables are provided to the Shepherd during startup. For each value, the exact
mechanism of how they are provided is described as well as their purpose.

.. _mode_env:

MODE
----

In single-node startup the Shepherd is started by the Launcher back end. When the Launcher back end is started
in single-node mode it will be started by the Launcher with the *MODE* set to 'single'. When this is detected,
the Launcher Back End will start the Shepherd with the its *MODE* environment variable also set to 'single'.

In multinode startup the Shepherd is started by CTI. The CTI API allows for environment variables to be
set in the daemon process which is the Shepherd for the Dragon runtime services. So, the Launcher, through
CTI, will set the *MODE* for the Shepherd to 'mutinode'. In addition, the Launcher Back End is also
started and the mode is passed in as a commandline parameter to the Launcher Back End.

.. _alloc_sz:

ALLOC_SZ
--------

In single-node startup the Shepherd is started by the Launcher back end. When the Launcher back end is started
in single-node mode it will be started by the Launcher with the *ALLOC_SZ* set to 1. When this is detected,
the Launcher Back End will start the Shepherd with the its *ALLOC_SZ* environment variable also set to 1.

In multinode startup the Shepherd is started by CTI. The CTI API allows for environment variables to be
set in the daemon process which is the Shepherd for the Dragon runtime services. So, the Launcher, through
CTI, will set the ALLOC_SZ for the Shepherd in the mutinode case.

INDEX
-----

The *INDEX* is the node index indicating which of the nodes in the allocation this one is.  The node index
values range from 0 to n-1 in our current implementation (but this could change).

In the mutinode startup the MRNet Server Back End has access to the rank of its node. The MRNet Server
Back End communicates this to the Launcher Back End during startup as the first communication coming through
on its standard output stream. The Launcher Backend reads this value as its first value from the MRNet Server
Back End. It then sends the *INDEX* value along to the Shepherd in the *BENodeIdxSH* message on its POSIX
message queue. The Shepherd reads this message as the first step in its preparation for multinode
execution.

In single node startup the Launcher Back End is started by the Launcher. In this case, the Launcher Back End
sends the *BENodeIdxSH* message to the Shepherd with the value of **INDEX** in it (which will be 0 in this
case).  In the case of single node startup, that message is written to the standard input of the Shepherd
which matches where it expects to find this bootstrap message.

PRIMARY_INDEX
-------------

Setting the *PRIMARY_INDEX* will be done in the same manner as :ref:`setting the allocation size <alloc_sz>`.
In single-node it will be set by the launcher and passed along to the Launcher Back End and the Shepherd. In
mutinode mode it will be set by CTI for the Shepherd.

The *PRIMARY_INDEX* is hard-coded to 0 by the launcher in our current implementation of the Dragon runtime services.

GS_MAX_MSGS and SH_MAX_MSGS
---------------------------

These two environment variables are provided by the launcher but at this time are fixed in size. They are provided
to the Shepherd in the same manner as *ALLOC_SZ*. In single mode the environment variables are set by the Launcher
Back End which has its two copies of the environment variable set by the Launcher. In the mutinode mode, the
two variables are set via CTI.

These two variables are configurable because it is likely that we will want to adjust these values at some
future point depending on characteristics of the system the services are running on.

DEFAULT_SEG_SZ
--------------

The *DEFAULT_SEG_SZ* is provided by the launcher but is fixed at this time to 1GB.  The *DEFAULT_SEG_SZ* is
provided to the Shepherd in the same manner the *ALLOC_SZ* is provided. In single mode the environment
variable is set by the Launcher Back End when the Shepherd is started. In mutinode execution, this
environment variable is set via CTI for the Shepherd. This default segment size is used to determine how big
the pool is for user-level processes when they communicate with the Dragon Runtime Services on the current
node. Each Shepherd instance, and therefore each node, creates its own default segment for shared memory.

INF_SEG_SZ
----------

Like *DEFAULT_SEG_SZ*, the *INF_SEG_SZ* is set the same way in single and mutinode modes. The segment
size is used in determining the size of the infrastructure only shared memory pool for the current node to be
used for communication and synchronization of the Dragon Runtime Services on the node. This is defaulted to
1GB presently.

TEST
----

The *TEST* environment variable is set at launch for the Dragon Runtime Services. It defaults to false, but
may be set to true to bypass certain initializations. The *TEST* environment variable is set via the same
mechanism that *ALLOC_SZ* and *DEFAULT_SEG_SIZE* are set through CTI and the Launcher Backend for
mutinode and single mode bootstrapping, respectively.

DEBUG
-----

This is a placeholder for future functionality.

The *DEBUG* environment variable is set at launch for the Dragon Runtime Services. It defaults to 0, but may
be set to a higher number to increase the verbosity and/or frequency of log entries. The *DEBUG* environment
variable is set via the same mechanism that *ALLOC_SZ* and *DEFAULT_SEG_SIZE* are set through CTI and the
Launcher Backend for mutinode and single mode bootstrapping, respectively.


Environment Variables Set by Shepherd
=====================================

The Shepherd sets the following environment variables for Global Services and all user-level processes.  Some
of these values were given to the Shepherd. Other values are created during startup of the Shepherd.  Two of
the values are set on a per process basis.  The values that the Shepherd creates are as follows.

    * LOCAL_SHEP_CD - Channel Descriptor for the Shepherd
    * LOCAL_BE_CD - The Launcher Back End Channel Descriptor
    * GS_CD  - The Global Services Channel Descriptor
    * DEFAULT_PD - The Default Pool Descriptor
    * INF_PD - The Infrastructure Pool Descriptor
    * GS_RET_CD - This Channel Descriptor is created on a per process basis for responses
      to API calls that are processed by Global Services.
    * SHEP_RET_CD - This Channel Descriptor is created on a per process basis for responses
      to API calls that are processed by the Shepherd (unused).

Since the Launcher Back End does not have these values given to it through environment variables, they are
passed to the Launcher Back End via the SHPingBE message during startup (excluding the last two which are set
on a per process basis). For all other processes, these are set as environment variables when the process is
started.

In addition, the Shepherd passes along the following environment variables to all processes it creates
including Global Services.

    * MODE
    * ALLOC_SZ
    * INDEX
    * PRIMARY_INDEX
    * TEST
    * DEBUG
    * MY_PUID

Points of Consideration
=======================

On nodes other than the primary node, we must be able to create a Channel Descriptor for Global Services on
the primary node. This means that the node index must be a part of the creation of a Channel Descriptor and we
should be able to create this descriptor remotely.  This has been written up in JIRA PE-34573.