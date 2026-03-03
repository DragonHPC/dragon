.. _GlobalServices:

Global Services
+++++++++++++++

The Global Services subsystem in the Dragon runtime is the coordination point for all objects that have global
scope - principally managed processes and channels.

The initial implementation of Global Services is a single Python process that is involved in the creation of
every managed process and most of the infrastructure.


Architecture
============

.. figure:: images/global_services.svg

    **Image 1: Global Services Architecture**

**FIXME: GS needs a textual description of the architecture, i.e. software requirements.**

Code structure
==============

Global Services server code is primarily in ``./dragon/globalservices/``,
and makes use of utilities in ``./dragon/infrastructure``.

The main server loop itself is found in ``./server.py`` as well
as functions needed to start the server from the command line
using the '-c' option to python.  Some files in this directory are
for the global services API

===============     ==============
file                contents
---------------     --------------
api_setup.py        globals and setup for managed process infrastructure channels
channel.py          the Channel api for managed processes
channel_int.py      the ChannelContext object and other
group.py            the Group api for managed groups of resources
group_int.py        the GroupContext object and PMIJobHelper class for PMI information
node.py             the Node api for information related to the hardware on a node level
node_int.py         the NodeContext object for a single node
policy_eval.py      the PolicyEvaluator object which evaluates policies and applies them
pool.py             the Managed Memory Pools and Allocations api
pool_int.py         the PoolContext object
process.py          the Process api for managed processes
process_int.py      the ProcessContext object and other
server.py           main server object, processing loop entry
startup.py          functions for server startup sequences
===============     ==============

Startup
=======

In normal operation Global Services will be started as a separate process.

All of the parameters it needs will be obtained from :ref:`LaunchParameters` which are normally derived from
environment variables via the ``infrastructure/parameters.py`` package, so no command line arguments are
needed.

To start up in single node mode, use:

``python3 -c "import dragon.globalservices.server; server.single()``

To start up in multi node mode, use:
``python3 -c "import dragon.globalservices.server; server.multi()``

Note that Global Services startup itself is part of a sequence involving
many other actors in the runtime.  See :ref:`SingleNodeBringup` and :ref:`MultiNodeBringup`.

Logging
=======

Logging routines are found in ``infrastructure/log_setup.py``.  Generally speaking,
logging will be configured once with the ``setup_logging()`` call
and new logging objects are gotten with ``get_logger()``.  The
name passed to get_logger indicates what section of the code
logging is coming from.

Global services will log every message at the DEBUG
logging level, while the INFO logging level will have at least one
log message for every major state change and every global object
creation or deletion.

Currently, Global Services catches all exceptions at the top level
and converts them when possible into an ``AbnormalTermination`` message,
which it serializes and sends to stdout before exiting.  These
exceptions are logged including the backtrace.

Test harness
============

Global Services needs to be tested in isolation and also in combination
with other parts of the runtime, but without the whole runtime being
available. This is achieved via making sure that the server only communicates
messages through object handles with a common interface, and providing means
to run the code with 'test' handles provided by the test bench instead
of the normal ones.

The server object has ``run_startup(...)``, ``run_global_server(...)`` and ``run_teardown`` methods,
which hve overridable arguments for the objects through which the server sends and
receives messages.

This lets tests enter the server with one or more of these
arguments overridden with ones coming from the test harness.

Typically tests are set up to launch the global server code in either
a thread using the Python threading library or a separate process
using the existing multiprocessing library.  Then they can be run in a unittest
test case object in the usual way.

Structure
=========

The global services server is structured as a serial program that communicates through messages.
The explicitly serial nature of its processing is deliberate because global services serves
as the unique serialization point for all transactions involving global objects, which are
channels and managed processes.  All of its input comes through a single channel.

The server state is held in the member variables of a singleton server object of type
``globalservices.server.GlobalContext``.

The main loop of the server is the ``run_global_server()`` method.  Currently it is structured
to process messages until there aren't any more, then sleep for a short time, which is crude but
adequate initially, under the assumption that the global server is allowed to take up one core.
Enough message interpretation to orchestrate the server through its basic state lifecycle happens
here - specifically tracking the creation and exit of the head process from the Launcher and
receiving the message that sends the global server into the teardown sequence.   Messages
are passed to a single ``handle()`` method from this method, where they are processed.

In ``handle()``, messages are dispatched though a table having a member function to handle each type of message
the global services server will receive; typically these functions are named ``handle_MESSAGENAME``.  Many of
the messages global services receives require messages to be sent to a Shepherd and a response received.  In these
cases the subsidiary message to the selected Shepherd is sent immediately and a completion is stored in
a member variable.  When the response from the Shepherd is received then the original message can get responded to;
the point is that outstanding requests to Shepherds do not block Global Services from continuing to process
messages.  Of course, whenever a response can be generated immediately inside Global Services its response is
sent immediately.

Parallelization strategy
========================

First of all, the concept behind global services is that modern HPC networks are capable of
achieving decent point to point latency and throughput on a small volume of small sized messages even in
the presence of a heavily loaded network.  Messages to and from global services are inherently randomly
scattered over the application's node allocation.

Reasons to parallelize are to get better latency and throughput on global services transactions, and
to spread the processing impact of these operations over more of the user hardware allocations.  What any
user application will require of global services depends on how dynamically the application needs to reconfigure
its resources.  It's reasonable to expect that a single threaded implementation of Global Services
in a compiled language could process at least 100K messages per second.  Approximately 10 messages are needed to launch
a managed process and establish its connections to infrastructure, while getting a handle to a new
channel is about 5, meaning a single instance of global services could turn over all the processes in a 10,000 core
system in about a second - this amounts to about 100kW worth of compute hardware - which is less time than getting
processes running on such an allocation normally takes anyway on an unloaded system.

These estimates are based on only the lowest
level messaging interface; implementing bulk operations could increase the basic messaging efficiency of common
cases like starting a large number of worker processes or an array of channels by a factor of 2 or 3.
They also assume that message serialization is more lightweight and byte-efficient than JSON.

Most of the activity that global services provides is to allocate and manage the namespace of channels
and the namespace of processes.  Interactions to do with a particular channel or process don't involve
another channel or process on a basic level, so as long as every client can find the global services agent
that is responsible for matters related to a particular p_uid or c_uid or user name only given the name
or the index.

Process message handling
========================

Processes are tracked in global services in the ProcessContext object; this object contains among
other things the ProcessDescriptor what state the process is in.  Process states are PENDING, ACTIVE, and DEAD.
Global Services maintains a record for every process that existed during the program's runtime.

Messages to Global Services take the form of a directive or query together with a channel ID to send the response
to. Typically managed processes will be set up with a response channel dedicated to such responses from Global Services.
Below we discuss at a high level how some of the process related messages are handled.

GSProcessCreate
^^^^^^^^^^^^^^^

The server assigns a p_uid, chooses which Shepherd to place the process on, creates a ProcessContext
record, and sends a SHProcessCreate message to the chosen Shepherd.  At this stage reasons the process can't be created
include if the user requested name or process itself already exists; in these cases the response message carrying
the error gets sent back to the entity requesting process creation immediately.

Otherwise, the process is put in a PENDING state; when the SHProcessCreateResponse arrives it is matched to the process
and the original GSProcessCreate message, and a response gets sent to the requester confirming that the process
has started.

TODO: do we want a different state for processes that have proved they are alive by talking to Global Services?
Fully managed processes will get some channels set up for them, and there will be a 'release' message sent to them
as well.  But not all processes are in this category, on this level.

GSProcessList
^^^^^^^^^^^^^

Returns a list of the p_uid of all processes.

Todo: options to restrict this only to active processes and so on.

GSProcessQuery
^^^^^^^^^^^^^^

This returns the ProcessDescriptor for a specified process. The process may not currently be running.
The only error response is if the process specified (by name or by p_uid) can't be found.

GSProcessJoin
^^^^^^^^^^^^^

This message requests notification later of a process's exit.  Error returns, or if the process is already
DEAD, are sent immediately, otherwise the request gets saved so that a response can be issued whenever
the corresponding process exits.  The request can also carry a timeout, where a timed-out response will get sent after
the timeout if the process hasn't exited by then.

GSProcessJoinList
^^^^^^^^^^^^^^^^^

This message requests notification when at least one process or all processes from a target list of processes exit.
In the case where the value of 'join_all' is 'False', if there is at least one process in the list that has exited (already DEAD),
then a response message is sent including the status of every process in the list.  Otherwise the request gets saved so that a
response can be issued whenever at least one of the corresponding processes in the list exits.  The request can also carry
a timeout, where a timed-out response will get sent after the timeout if none of the processes in the list has exited by then.
Similar is the case when 'join_all' is 'True'. The difference now is that a response message is sent if all the processes exit or
there is a timeout.

GSProcessKill
^^^^^^^^^^^^^

This message is in the spirit of Unix 'kill', which is a method of sending a signal to a process.  Some of these
signals have the intended effect of terminating the process.

If the process is PENDING or DEAD, or the process specified can't be found, an error response is returned
immediately. Otherwise a SHProcessKill message is sent to the Shepherd.

SHProcessExit
^^^^^^^^^^^^^

The SHProcessExit is the response expected from a SHProcessKill message; when it is received it is matched to
the corresponding previous GSProcessKill message if there is one.

This message is also sent by a Shepherd, asynchronously, if a process it is managing exits.  In this case
the did_exit field is expected to be True and the ref field is expected to be 0; that the ref field is 0 denotes
that this is the process exiting of its own accord and not via being killed.

In either case, if a process has exited and it has any pending Join requests, responses from these get sent to
the requester.