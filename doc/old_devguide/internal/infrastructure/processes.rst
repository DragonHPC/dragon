.. _ProcessCreationAndInteraction:

Process Creation and Interaction
+++++++++++++++++++++++++++++++++

Here we describe the creation and interaction of managed POSIX processes by the Dragon runtime
:ref:`Infrastructure` :ref:`Services`.

.. _LaunchParameters:

Launch Parameters
=================

The Dragon runtime has a number of parameters that may be a constant or determined at job launch time.  These
need to be accessed in a mutually coherent way by a number of different runtime components:

    - :ref:`Launcher`
        - on login node
        - Source of some parameters, such as number of back end nodes

    - :ref:`MRNet` Backend, one per node

    - :ref:`LocalServices`, one per node

    - :ref:`TransportAgent`, one per node

    - :ref:`GlobalServices` head process (plus any more it may launch)

    - managed user processes

Here we specify how these parameters are to be supplied, and what their names and types are.  For now, these
parameters are viewed as potentially changing between separate user launches but are immutable from the point
of view of the lifetime of any one component process.  They may not necessarily all be identical across
processes.

Providing parameters
--------------------

For the time being, parameters will be provided to launched components as *POSIX environment variables*. This
may change to another scheme in the future.

Each parameter listed below, when provided as an environment variable, will have the string ``DRAGON_``
prepended to its name.

.. _accessing-parms:

Accessing parameters
--------------------

All code that uses parameters will do this through a defined interface and not through hard-coding them or
directly accessing wherever they happen to be sourced from.  This lets us change policy if need be as to where
these things ultimately come from.

Parameters are accessed in Python through the ``dragon.infrastructure.parameters`` module which defines the
LaunchParameters object.

This module contains ``dragon.infrastructure.parameters.this_process`` singleton instance which should be the
only way other Python code interacts with the launch parameters.

This object is initialized, from the environment, when the parameters module is imported into a process and is
meant to contain immutable data. Changes to the this_process object's attributes do not change the process's
actual environment.  Instead, the this_process object has a method that returns the environment variable name
and value for each parameter in a dictionary as strings.  In this way one can collect the environment
variables that need to be set when starting a new process.

The ``LaunchParameters`` object can be instantiated separately from the this_process singleton. One may do a
deep copy on the this_process variable, adjust what needs adjusting in the copy by changing the members
directly, then ask the object to produce a dictionary of environment variables.  If these variables are set
for a new process, the corresponding ``this_process`` singleton object will match with what has been prepared.

**FIXME**: proper API documentation and cross links **FIXME**: define C interface for this stuff

    - the keys will be (at some level) strings, but they should not
      be mentioned all over the code, but only as const char* defined
      in one place.
    - could be as simple as initializing a struct.

Parameter organization
----------------------

The general structure of parameters is as key-value pairs, where the key is name of the parameter.

Types
^^^^^

Parameter values can have integer or string type.

    - Everything will be in utf-8 encoding.
    - parameter names will be legal C99 identifiers
    - Integer parameters will be provided in base 10
        - with no leading, trailing, or interstitial spaces
        - e.g. as the "%d" C99 printf format specifier

Parameters that for some reason need richer typing than integer and string will be provided as JSON encoded
strings.

We will avoid having floating point parameters by defining units as part of the parameter - that is, instead
of a floating point number of seconds, an integer number of Planck time units.

Individual parameters may have their own range restrictions beyond the basic type and these should be checked
up front if possible when building the interface object.

For instance, an integer may be expected to be a nonnegative integer, or less than another integer.

.. _Parameters:

List of parameters
------------------

This list may be added to in the future. These values include values needed for bootstrapping the Dragon
services during startup and these environment variables also contain values needed by user processes that are
executing under the supervision of the Dragon runtime services.

**FIXME Add source and sink of these variables to improve clarity**

+----------------+-------------+---------------------------------------+
| Name           | Type        | Description                           |
+================+=============+=======================================+
| MODE           | string      | set to 'hsta' if it is a              |
|                |             | multi-node run.                       |
|                |             | Otherwise it is                       |
|                |             | set to 'single'. Used during          |
|                |             | bootstrapping.                        |
+----------------+-------------+---------------------------------------+
| ALLOC_SZ       | integer >0  | number back end nodes in allocation.  |
+----------------+-------------+---------------------------------------+
| INDEX          | integer >=0 | which node in the allocation this     |
|                |             | process is on                         |
+----------------+-------------+---------------------------------------+
| PRIMARY_INDEX  | integer >=0 | which node in the allocation is       |
|                |             | designated as the primary node.       |
|                |             | Global services runs on the primary   |
|                |             | node.                                 |
+----------------+-------------+---------------------------------------+
| DEFAULT_PD     | string      | default local pool descriptor for     |
|                |             | process allocations on behalf of the  |
|                |             | Dragon runtime services (base64).     |
+----------------+-------------+---------------------------------------+
| INF_PD         | string      | infrastructure local pool desc. Used  |
|                |             | strictly by Dragon services (base64). |
+----------------+-------------+---------------------------------------+
| LOCAL_SHEP_CD  | string      | channel descriptor (base64) for the   |
|                |             | local Shepherd's main queue.          |
+----------------+-------------+---------------------------------------+
| LOCAL_BE_CD    | string      | channel descriptor (base64) for the   |
|                |             | launcher's backend main queue.        |
+----------------+-------------+---------------------------------------+
| GS_RET_CD      | string      | channel descriptor (base64) for GS    |
|                |             | responses to this process from GS API |
|                |             | calls.                                |
+----------------+-------------+---------------------------------------+
| SHEP_RET_CD    | string      | channel descriptor (base64) for Shep  |
|                |             | responses to this process from        |
|                |             | Shepherd API calls.                   |
+----------------+-------------+---------------------------------------+
| GS_CD          | string      | channel descriptor (base64) for the   |
|                |             | Global Services' main recv queue.     |
+----------------+-------------+---------------------------------------+
| GS_MAX_MSGS    | integer     | The maximum number of allowed msgs    |
|                |             | in the main Global Services channel.  |
+----------------+-------------+---------------------------------------+
| SH_MAX_MSGS    | integer     | The maximum number of allowed msgs    |
|                |             | in the main Shepherd channel.         |
+----------------+-------------+---------------------------------------+
| DEFAULT_SEG_SZ | integer >0  | size of the default shared memory     |
|                |             | segment. Used during bootstrapping.   |
+----------------+-------------+---------------------------------------+
| INF_SEG_SZ     | integer >0  | size of the infrastructure shared     |
|                |             | memory segment. For bootstrapping.    |
+----------------+-------------+---------------------------------------+
| TEST           | integer >=0 | as bool; indicates 'test mode' to     |
|                |             | bypass some initializations           |
+----------------+-------------+---------------------------------------+
| DEBUG          | integer >=0 | integers greater than 0 indicate      |
|                |             | increased levels of logging for debug |
|                |             | purposes. 0 indicates no logging.     |
|                |             | This is a placeholder for now.        |
+----------------+-------------+---------------------------------------+
| MY_PUID        | integer >0  | process uid for this process          |
+----------------+-------------+---------------------------------------+


Activity Diagram
================

Following is a flow diagram showing the interaction between components during process launch, output and input
handling, signaling, and process termination. The text below the figure gives additional details on the
activities during this interaction. The *a1* through *a6* are denoted in :numref:`launchproc` and the numbered list below
further describes those activities.

.. figure:: images/launchproc.srms1.png
    :scale: 75%
    :name: launchproc 

    **Launcher Component Interaction during Process Interaction**


Activity Description
--------------------

#. Starting with *a1* a process is launched by the user. During process launch a
   :ref:`GSProcessCreate <gsprocesscreate>` message is created by the Launcher and
   forwarded to Global Services through its pipe connection either to MRNet, or in
   the case of single-node launch, directly to the Backend. The p_uid field of the
   :ref:`GSProcessCreate <gsprocesscreate>` message is set to 0 to indicate it is
   the head process. The r_c_uid field is set to the channel id of the Backend.

#. When output is created by a process, as shown at *a2*, the output is detected by
   the Shepherd which creates an SHFwdOutput message and sends it directly to the
   Backend component. The Backend forwards these messages to the Launcher through
   the MRNet (if multi-node) or directly to the launcher (if single-node).

#. At *a3* the user sends input on standard in to the Launcher. The Launcher
   waits for available input on stdin and reacts by creating an :ref:`SHFwdInput
   <shfwdinput>` message and sends it to the Backend on the primary node which in
   turn forwards it on to the Shepherd. The Shepherd then sends that same input
   into stdin of the head process.

#. Activity *a4* demonstrates what happens when a signal is detected. The
   Launcher monitors signals that occur while it is running. During process
   execution, the Launcher continues to run, accepting input and signals. When a
   signal occurs, the signal is forwarded in the same manner, this time through
   Global Services, then the Shepherd, to signal the head process. Note that the
   Shepherd sends the :ref:`SHProcessExit <shprocessexit>` in response to the kill
   request. It is not acknowledging that it exited in this case. Simply that the
   signal was sent to the process. Then Global Services acknowledges the signal
   was sent through the :ref:`GSProcessKillResponse <gsprocesskillresponse>`
   message. Again, this does not mean that the process exited or otherwise
   terminated. It simply indicates the signal was received or provides an error
   code indicating what went wrong. For instance, if the process had already
   terminated before the signal message was received by Global Services or the
   Shepherd then an appropriate response code will be set. See the
   :ref:`GSProcessKillResponse <gsprocesskillresponse>` message for possible
   return codes to the Launcher.

#. At *a5*, after the process is created, the launcher may request notification
   of exit by
   sending the :ref:`GSProcessJoin <gsprocessjoin>` message.

#. When the process exits, as indicated starting at *a6*, the Shepherd detects
   the process is exiting and sends the :ref:`SHProcessExit <shprocessexit>` to
   Global Services which recognizes this was the head process and then
   sends the :ref:`GSProcessJoinResponse <gsprocessjoinresponse>` message. This message is forwarded on
   to the Launcher through the Backend. If the user then exits the launcher command processor
   using the *exit* command and no
   other messages needed processing, it initiates teardown of the infrastructure
   as shown in :ref:`SingleNodeTeardown` or :ref:`MultiNodeTeardown`.

.. **Requirements**
..     * Starting processes.
..     * Routing standard input to processes.
..     * Collecting and forwarding output from these processes (both stdout and stderr).
..     * Terminating processes.
..     * Propagate the Environment
..     * Debugger interaction (future development)
..     * Slurm and PBS Pro documentation for baseline for what
..       multi-node will do and make sure we have the same functionality
..       single-node. srun is the command. CTI will do this for us.
..       srun --export by default is set to all to get the environment.
..     * Configuring of Dragon run-time services tunable resources
..     * How big should shared memory segment for channels be?
..       - Use this dragon.ini file. Look at Python std lib for config files and use that format if available.
..     * Run Jupyter notebook as managed process? Run launcher inside Jupyter notebook shell.
..     * Nersc runs Jupyter Hub which starts notebooks on Large memory compute nodes. Look at Dask for example. How does it interact with Jupyter notebooks.
