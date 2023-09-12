.. _LocalServices:
.. _shepherd-design:

Local Services
++++++++++++++

Local Services is one component of the Dragon run-time :ref:`Services`. Together with :ref:`GlobalServices`, the
Shepherd provides services for creating and running processes.

It also provides services for the creation and deletion of channels for interprocess communication and
synchronization. Local Services is primarily responsible for run-time services that are on-node while Global
Services is responsible for services that must span across nodes on a distributed system.

Local Services manages shared memory that is used on-node for communication through :ref:`channels`.  Each of
the Dragon services uses a channel for communication to and from other services.


Local Services runs on every node and has responsibility for managing resources on that particular node.  This
keeps other parts of the runtime from needing to interact directly with operating system resources, and
provides a single point of contact. The set of services provided by Local Services is identical in the
ulti-node and single-node cases. The details of these services are provided below.

Local Services has responsibility for:

- orchestrating the Dragon run-time startup on a node

- creating a shared memory segment for use in interprocess
  communication

- allocating memory in that segment for channel structures and
  communication between processes

- launching new processes

- forwarding output from user processes on stdout/stderr file descriptors to the
  launcher

- forwarding input from the launcher to the stdin of user processes.

- creating Channels from the shared memory segment for other parts of the Dragon run-time
  and potentially for other user level processes.


Architecture
============

.. figure:: images/shepherd.svg

    **Figure 1: Internal Shepherd Structure**

.. figure:: images/shepherdstructure.png

    **Figure 2: Internal Shepherd Structure**

Local Services runs as a separate process that receives its work through a channel. After bringup of the Dragon
run-time services, Local Services receives messages through the :ref:`Messages` from this channel, processes
them, and responds to requesters. The handling of these requests is done asynchronously, meaning that
fullfilling a request may require other interactions with other parts of the Dragon run-time while handling
other requests. Local Services maintains state information about the progress of each request as necessary.

**FIXME: SH needs a better description and the diagram needs to be fixed.**

AsyncIO
-------

Local Services is written as an AsyncIO application using Python. AsyncIO is a
*newer* API within Python where *tasks* are executed *concurrently*. With
Python, processes are single-threaded, meaning they cannot run more than one thread of execution concurrently
due to the Global Interpreter Lock or *GIL*.  However, with AsyncIO, multi-tasking is accomplished in a
cooperative multi-tasking environment. With cooperative multi-tasking, one task must give up control to allow
another to run.

AsyncIO accomplishes cooperative multi-tasking through what are called
*awaitables*. Awaitables are typically I/O operations. However, other
operations, including *sleep*, can also be awaited. The design of AsyncIO is well thought out given that the
programmer is really making use of time on the CPU that would otherwise be wasted waiting for some
asynchronous I/O operation to complete synchronously.

AsyncIO Python applications have a loop that the user does not program that is the scheduler of AsyncIO tasks.
Local Services then is a collection of *tasks* that are submitted to the AsyncIO loop for scheduling.

.. _tasktypes:

Task Types
----------
Local Services categorizes these *tasks* into the following categories based on the *TaskType* enumeration found
in the Process Manager *manager.py* source code.

    * *sys* - System tasks that are created in support of Local Services and include the task that receives
    * messages from Local Services's main queue.
    * *process* - A process task monitors for the exit of a managed process by executing a *wait* on the
      process.
    * *stdin* - Each managed process gets its own *stdin* task when there is available standard input to write
      to the process. These tasks come and go as input becomes available.
    * *stdout* - Each managed process gets a *stdout* task that monitors for standard output
      from the managed process.
    * *stderr* - Each managed process gets a *stderr* task that monitors for standard error output
      from the managed process.

Local Services primarily consists of two classes, a *Shepherd* class, in
*server.py* and a *ProcessManager* class, as previously stated in *manager.py* .
The *Shepherd* class relies on the *ProcessManager* to manage user processes.  This involves creating *tasks*
to capture the standard output and standard error from those processes. The actual user process, running its
own code, runs in a separate process. While Python does not support multi-threading, it does support creating
separate processes.

In addition to the user process tasks, the *Shepherd* creates a *sys* task (i.e.  system task) for receiving
messages from its channel. Other system tasks are also possible, but currently the only system task is the one
for receiving messages from Local Services's channel.

Local Services processes incoming messages on the main receive channel and routes them to an appropriate handler
in the Local Services class. The Process Manager is called on to handle anything related to user processes. The
Shepherd's run method is called during startup and the AsyncIO scheduler is executed by calling the Process
Manager's *run_tasks* method, which in turn calls the AsyncIO loop's
*run_until_complete* method. This method completes when one of the tasks
completes its execution.

Process Management
==================

.. figure:: images/processstates.png

    **Figure 8: Process State Transition Diagram**

Managed processes are created by Local Services in response to the
:ref:`SHProcessCreate <shprocesscreate>` message. The following fields are part
of the managed process creation message.

    * *exe* - The executable of the process
    * *args* - a list of argument strings to be provided to the process
    * *env* - a dictionary of strings mapped to strings representing the environment variables that should be
      appended to the current environment for a process. Variables in *env* will override anything in the
      previously defined environment.
    * *rundir* - The current working directory for the process. If an empty string is provided, then the
      default *cwd* (current working directory) is used.
    * *t_p_uid* - the target process identifier used to identify the process to the Dragon run-time services.
      This must be unique for all executing managed processes.

Additionally, there are a few common fields within the message.

    * *tag* - a unique identifier that is provided as the *ref* on a creation confirmation response.
    * *p_uid* - the requesting process id
    * *r_c_uid* - the return channel id for sending confirmation of this process creation.

Internally to Local Services, when a :ref:`SHProcessCreate <shprocesscreate>` message is received, it creates a
Process object to hold state information about the managed process including its state of init, running,
complete. Internally, when a managed process is created, three separate channels may be specified to receive
notifications about output on both standard output and standard error and about the termination of the
process. As implemented, when a user-defined managed process is created, the Launcher/Backend channel receives
all notifications about output on standard output and error, while the Global Services channel is used for
notification of the termination of the process.

.. figure:: images/managedservices.png

    **Figure 9: Managed Process services provided by Local Services**

Initially the managed process is in the *init* state and an AsyncIO *process* task (see :ref:`Task Types
<tasktypes>`) is created that will run to create the process and move it to the *run* state. Once the task is
confirmed to have started, the *_handle_started_procs* internal function in the Process Manager (i.e.
*manager.py*) is called. This function creates three AsyncIO tasks to manage the process termination and its
standard output and error streams.

A stdin AsyncIO task, for writing standard input, is created when there is standard input available as
supplied by the :ref:`SHFwdInput <shfwdinput>` message. When the standard input has been written, the task
terminates.  Additionally, if more input comes in on a subsequent :ref:`SHFwdInput <shfwdinput>` message
before the first input was written, the input task will combine the input from the first message with the
second and write it all at once. If no process exists, Local Services responds with the :ref:`SHFwdInputErr
<shfwdinputerr>` message. Otherwise no response is sent. When the input has been written to the managed
process, the stdin AsyncIO task exits. If more input is written later, a new AsyncIO stdin task is created.

Two AsyncIO tasks manage the output created by the process and forward it on as needed, one for standard
output and one for standard error. These tasks continue to run as long as the process runs. All output coming
from a managed program is forwarded on to the Launcher/Backend through the Backend/Launcher channel in an
:ref:`SHFwdOutput <shfwdoutput>` message. Output from a managed process is forwarded in chunks up to 5000
characters long. If more than 5000 characters are printed to the stream, they will be packaged in separate
messages. It might be that at a future point we'll decide on a different size for tuning and/or we may make
this size configurable on a process by process basis when the process is created.

At completion of a managed process the ProcessManager is notified of the process exit by executing a *wait* on
the process. This results in a :ref:`SHProcessExit <shprocessexit>` message being sent to the Global Services
to confirm the exit of process. At this point the process is moved into the *complete* state. Local Services
then runs to clean up the process by cancelling any of the outstanding tasks for monitoring input and output
on the task. Once cleanup has occurred, the process is deleted from Local Services.

The Local Services/Global Services Integration
========================================

.. figure:: images/gsmonitor.png

    **Figure 10: The Global Services Monitor**

During startup, Local Services creates :ref:`GlobalServices` like a managed process on the node designated as
the *PRIMARY_INDEX* in the Dragon Runtime launch parameters (see :ref:`LaunchParameters`) from the perspective
of the :ref:`LocalServices`. All managed processes have their two output streams, stdout and stderr, monitored for
any output by Local Services. This includes Global Services. In addition, managed processes are also monitored
for process exit, as described in the last section. When any of these conditions occur, Local Services notifies
other entities by sending one of the messages :ref:`SHFwdOutput <shfwdoutput>` or :ref:`SHProcessExit
<shprocessexit>` to a queue on the system. Usually this queue is simply a wrap of a channel as presented in
the last section. In this case, however, the queue is not a wrap of a channel, but simply an internal
structure for sending and receiving messages.

At the other end of this internal queue sits the GSMonitor which acts as the receiving entity for any
:ref:`SHFwdOutput <shfwdoutput>` or :ref:`SHProcessExit <shprocessexit>` messages related to Global Services.
The GSMonitor object is run as an AsyncIO task and monitors the internal queue for any messages coming from
the managed Global Services. As an AsyncIO task, it sits quietly, waiting for available input on this internal
queue.

Since Global Services is run as a managed process, any output from Global Services is wrapped up in a
:ref:`SHFwdOutput <shfwdoutput>` message by Local Services and forwarded on to the receiving entity, in this
case the GSMonitor's queue. Normally, the output from Global Services is a serialized :ref:`GSHalted
<gshalted>` message. Local Services wraps this serialized :ref:`GSHalted <gshalted>` message into the data field
of a :ref:`SHFwdOutput <shfwdoutput>` message and forwards it to the GSMonitor's queue. The GSMonitor unwraps
that :ref:`SHFwdOutput <shfwdoutput>` message by taking the data field of the forwarded output and forwarding
that data as a message to Local Services which in turn takes the appropriate action for that message. The
GSMonitor sees messages from Global Services as a message inside a message. Again, the :ref:`SHFwdOutput
<shfwdoutput>` wrap of the Global Services message is created by Local Services when it detects output from a
managed process. The role of the GSMonitor is to unwrap that message and forward it to Local Services's main
queue.

Global Services is expected to send one of two messages through its standard output. It should either send the
:ref:`GSHalted <gshalted>` message or it should send the :ref:`Abnormal Termination <abnormaltermination>`
message. When the GSMonitor receives any message from Global Services, it is forwarded on to Local Services's
main queue for processing. If the GSMonitor receives text on stdout or stderr from Global Services that is not
a valid message the GSMonitor still forwards that to Local Services's main queue and Local Services in turn
recognizes that this is a bad message format and begins abnormal end processing.  Abnormally ending creates
log entries to document the problem and brings down the Dragon run-time system quickly.

Anything written by Global Services to standard output or standard error that is not a valid message would
likely be a traceback or some other text indicating a failure in Global Services. By treating this like a
message (a bad format message), Local Services will log the message and abnormally end. In that way, the failure
gets logged before terminating. If a traceback is present in the text written to one of these two streams, it
will be logged for further identification of the problem.

Finally, if the GSMonitor is notified that Global Services exited, then it will initiate an
AbnormalTermination message to Local Services to bring it down with appropriate logging as to the reason.

In all of these cases, once the GSMonitor has detected either a message, text, or just termination of Global
Services, the GSMonitor task exits. Once a normal termination or abnormal termination of the Global Services
has been detected, the lifetime of the GSMonitor is at its end.

Channel Allocation
---------------------

Upon startup Local Services creates a MemoryPool for using in creating Channels.  Local Services creates two
channels, one for its own receive queue and one for the Global Services. Other services and/or user-level
programs may also the request creation of Channels. In particular, the Dragon version of multiprocesing
creates and uses many channels in its implementation. TBD

