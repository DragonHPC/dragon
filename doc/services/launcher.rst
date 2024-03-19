.. _Launcher:

Launcher
++++++++

**DISCLAIMER: Much of this is out of date following removal of CTI and MRNet. Refer to** :ref:`MultiNodeDeployment`
**for most accurate info**

The Launcher is responsible for communicating from the user to :ref:`GlobalServices` and :ref:`LocalServices`.
The Launcher supports :ref:`SingleNodeDeployment` as well as :ref:`MultiNodeDeployment`.

:ref:`SingleNodeDeployment` and :ref:`MultiNodeDeployment`, have different requirements for launching
applications. In both cases there is a Launcher frontend  and a Launcher backend`. In the multi-node case
additional components come into play to set up the communication between the frontend and backend. The
Launcher can be run in :ref:`LauncherServerMode` to support user-defined interaction between the login node
and compute nodes.

**FIXME: Some more detail on the requirements for multi-node and single-node would be good here**

.. _LauncherSingleNodeArchitecture:

Launcher Single Node Architecture
=================================

.. figure:: images/launcher_single_node.svg
    :name: launcher-single-node

    **Single node architecture of the Launcher component**

.. figure:: images/singlenodelauncher.png
    :name: singlenode-launcher

    **Single-node Launcher/Backend Components**

In a single node deployment of Dragon, the launcher consists of the following components:

* *Launcher Frontend*: User input and output.
* *Launcher Backend*:
    * Startup of and communication with :ref:`LocalServices` using standard POSIX pipes.
    * Communication with all Dragon :ref:`Services` via :ref:`Messages` through :ref:`Channels`.

In the single-node case, the Launcher frontend starts the Launcher Backend which then in turn starts the
:ref:`LocalServices`. The Launcher Front End communicates with the Launcher Back End via its stdin and stdout
streams. The Launcher Back End communicates with the Shepherd via its stdin and stout during startup and
teardown. After startup and before teardown all communication between the Launcher Back End, the Shepherd, and
:ref:`GlobalServices` occurs over :ref:`Channels`. See :ref:`SingleNodeBringup` and :ref:`SingleNodeTeardown`
for details on the process.


.. _LauncherMultiNodeArchitecture:

Launcher Multi Node Architecture
================================

.. figure:: images/launcher_multi_node.svg
    :name: launcher-multi-node

    **Multi node architecture of the Launcher component**

.. figure:: images/launchercomponents.png
    :name: launcher-comps

    **Multi-node Launcher/Backend Components**

In the multi-node case, shown in :numref:`launcher-multi-node` and :numref:`launcher-comps`, the Launcher Front End uses the workload manager to start
the  Shepherd on every node through :ref:`CTI`. It then starts the Network Front End which creates an MRNet
Server Front End which creates a scalable communication tree that eventually connects to a Network Back End.
The Launcher Back End  component is started by MRNet and communication between the Launcher Back End and the
Shepherd is accomplished via a pair of Posix message queues during startup and teardown. After startup and
before teardown all communication between the Backend, the Shepherd, and Global Services occurs over channels.

The Launcher Back End starts the Network Back End during startup. The Network Back End the creates a
MRNetServer Back end which attaches to the MRNet network. Both the Launcher Front End and the Launcher Back
End communicate with their corresponding Network Front End and Back End components via stdin and stdout of
their respective processes.

Internally, the *Launcher Front End* is composed of a server that routes messages to and from the user. The
user interacts with the *Command Processor* which is a *Read Evaluate Print Loop* for Python. The command
processor is a full-fledged Python interpreter with several predefined functions for invoking the various
*Launcher* commands. The command definitions are given in the :ref:`LauncherCommands` section.

Not depicted in :numref:`launcher-comps`, the :ref:`GlobalServices` and its :ref:`Channels` are only present on the primary
node. All other components on the compute node are present on every compute node.


.. _LauncherComponents:

Launcher Components
===================

During initialization the *Launcher Front End* creates the *Network Front End* as a process. The *Network
Front End* creates an instance of the MRNetServerFE object and provides it a *callback* that is invoked when
data comes from the *MRNet Server* to the  *Network Front End*. The callback handler writes the message to
stdout, which the *Launcher Front End* can then read. The *Launch Front End* writes to stdin of the *Network
Front End* to send data across the MRNet Network to compute nodes. If the *Network Front End* receives an
:ref:`LABroadcast <labroadcast>` message, it calls the broadcast method of the *MRNet Server Front End*.

The *Launcher Backend* is an AsyncIO process and monitors its stdout of the *Network Back End* (via an AsyncIO
task) and reads from the *Network Back End* pipe and to receive data coming from the front end. The *Network
Back End* provides a *callback handler* the the MRNetServerBE object to be called when data flows from the
front end. This *callback handler* writes any data to the stdout of the *Network Backend* which then gets read
by the *Launcher Back End's* AsyncIO monitor task.

As mentioned, the *Network Front End* is a process and is started by the *Launcher Front End*. The *Network
Backend* is also a process and is started by *MRNet* as part of the bringup of an HPC job allocation under the
control of *slurm*. The Shepherd is brought up by CTI during startup.

In the case of single-node Dragon run-time services, the *Launcher Front End* is started by the user and the
*Launcher Front End* starts the *Launcher Back End* which in turn starts the Shepherd. All startup/teardown
communication between the components occurs on these stdin and stdout streams resulting from these process
creations.

In both multi-node and single-node mode, the *Launcher Back End* does not run as a managed process to be
consistent between the multi-node and single-node cases.

Any :ref:`LABroadcast <labroadcast>` message ends up in the *Launcher Back End* which then unwraps the
broadcasted message and forwards it to the appropriate component, which as of this writing is always the
*Shepherd*. Currently there are two broadcasted messages, the :ref:`SHHaltTA <shhaltta>` message and the
:ref:`SHTearDown <shteardown>` message.

**FIXME: We could introduce separate frontend and backend descriptions here. They are references at a lot of places.**


The Launcher's Network Components
---------------------------------

The Network front and back end program components of the launcher are responible for communicating with their
respctive Launcher front end and back end components. The code for both the network front end and back end
components of the are relatively simple applications employing the two classes MRNetServerFE and
MRNetServerBE. The network front end and back end programs create an instance of their respective class and
then read from standard input and send any standard input on to the other side. Both components write any
received messages from the other side (via their callback handler) to standard output. The two programs are
provided below for reference.

NOTE: The Launcher's Network Front End has an external dependency on the *_tc* field of the :ref:`LABroadcast
<labroadcast>` message being set to 68.

.. _LauncherNetworkFrontend:

Network Front End
^^^^^^^^^^^^^^^^^

.. code-block:: cpp

    #include <dragon/mrnetserverfe.hpp>
    #include <cstdlib>
    #include <unistd.h>
    #include <fstream>

    void mrnet_callback(MRNetServerFE* server, const std::string& msg) {
        std::cout << msg << std::endl << std::flush;
    }

    bool file_exists(const char *fileName)
    {
        std::ifstream infile(fileName);
        return infile.good();
    }

    // The argv arguments are passed to the MRNet backend program as
    // command-line arguments.

    int main(int argc, char *argv[])
    {
        try {
            std::vector<const char*> cti_args;
            char* cti_ptr = std::getenv("DRAGON_CTI_EXEC");

            if (cti_ptr == NULL) {
                std::cerr << "DRAGON_CTI_EXEC value not found in environment." << std::endl;
                return -1;
            }

            std::string cti_exec = cti_ptr;
            cti_args.push_back(cti_exec.c_str());

            std::vector<const char*> mrnetbe_args;
            char* mrnet_ptr = std::getenv("DRAGON_MRNETBE_EXEC");

            if (mrnet_ptr == NULL) {
                std::cerr << "DRAGON_MRNETBE_EXEC value not found in environment." << std::endl;
                return -1;
            }

            std::string mrnetbe_exec = mrnet_ptr;
            mrnetbe_args.push_back(mrnetbe_exec.c_str());

            // argv[0] is this executable which is not needed by the backend.
            // argv[1] is the dragon_mode set to 'hsta'.
            // argv[2] is the extra manifest file required for the backend executable. This is the
            // path to the launchernetbe executable which is started via a Popen by the
            // DRAGON_MRNETBE_EXEC program.
            // Starting at argv[2] are any arguments needed by the backend executable
            // specified by the DRAGON_MRNETBE_EXEC environment variable.
            std::vector<const char*> additionalManifestFiles;

            for (int k=1;k<argc;k++) {
                mrnetbe_args.push_back(argv[k]);

                if (file_exists(argv[k])) {
                    additionalManifestFiles.push_back(argv[k]);
                }
            }

            MRNetServerFE server(cti_args, mrnetbe_args, &mrnet_callback, environ, additionalManifestFiles);

            // The first thing written to stdout is the number of nodes in the allocation.
            std::cout << server.get_num_nodes() << std::endl << std::flush;

            std::string msg;

            // A Broadcast message will contain "_tc": 68 in it since this is the typecode
            // for a LABroadcast message.
            std::string bcast = "\"_tc\": 68";

            while (std::getline(std::cin, msg)) {
                if (msg.find(bcast) != std::string::npos)
                    server.send_all(msg);
                else
                    server.send_primary(msg);
            }

            server.shutdown();

        } catch (const std::exception &exc) {
            // catch anything thrown within try block that derives from std::exception
            std::cerr << exc.what();
        }
    }

.. _LauncherNetworkBackend:

Network Back End
^^^^^^^^^^^^^^^^

.. code-block:: cpp

    #include <stdio.h>
    #include <stdlib.h>
    #include <unistd.h>
    #include <dragon/mrnetserverbe.hpp>

    void mrnet_callback(MRNetServerBE* server, const std::string& msg) {
        // Anything coming down the MRNet tree is written
        // to standard output for the piped parent process to read.
        std::cout << msg << std::endl << std::flush;
    }

    int main(int argc, char *argv[])
    {
        MRNetServerBE server(argc, argv, &mrnet_callback);

        // After attaching to the MRNet the first thing is to
        // provide the node index to the backend launcher.

        std::cout << server.get_node_id() << std::endl << std::flush;

        std::string msg;

        // Anything coming from the parent process through
        // stdin is sent up to through the MRNet tree.
        while (std::getline(std::cin, msg)) {
            server.send(msg);
        }

        server.shutdown();
    }

MRNet
-----

The MRNet is an open source API for constructing a tree communication structure between nodes in a distributed
system. The MRNet API comes out of the University of Wisconsin, Madison. The MRNet is used to start the
shepherd on each  node which in turn brings up other parts of the service.

See the :ref:`MRNet` page for further details.

Starting the Launcher
---------------------
.. this could also go into running_dragon.rst

In the multi-node version of Dragon, the Launcher is started by a wrapper program that
manages the allocation of a number of nodes via an salloc command. The SLURM workload manager
provides this salloc command for starting the Launcher. When a different workload manager
is used, then a different wrapper may be necessary. This wrapper accepts any parameters as specified
in the section on `Invoking the Launcher <invoking-the-launcher>`_.

The Launcher wrapper requires one extra parameter, the argument -cores specifies how many
cores that Dragon is to be allocated on. The launcher then determines from the current partition
the minimum number of nodes that will be required to satisfy that request.
Then this value is passed on to the *salloc* command to acquire and allocation that satisfies the
user's request and runs one instance
of the Shepherd per node so each is included in the set of Dragon run-time service nodes.

This Launcher wrapper sets required environment variables including the number of nodes for the allocation
and the *DRAGON_MODE* environment variable that indicates that dragon is running in *muitinode* mode.
The wrapper then executes the salloc command with the actual start of the launcher within it and any
launcher specific arguments passed into it.

Launcher Messages
==================

Launcher specific message definitions can be found within the :ref:`LauncherAPI`. Definitions for other
messages can be found within the :ref:`Messages` section. Links to specific messages are provided within this
documentation as they appear.

Starting the Launcher
=====================

In the multi-node version of Dragon, the Launcher is started by a wrapper program that
manages the allocation of a number of nodes via an salloc command. The SLURM workload manager
provides this salloc command for starting the Launcher. When a different workload manager
is used, then a different wrapper may be necessary. This wrapper accepts any parameters as specified
in the section on `Invoking the Launcher <invoking-the-launcher>`_.

The Launcher wrapper requires one extra parameter, the argument -cores specifies how many
cores that Dragon is to be allocated on. The launcher then determines from the current partition
the minimum number of nodes that will be required to satisfy that request.
Then this value is passed on to the *salloc* command to acquire and allocation that satisfies the
user's request and runs one instance
of the Shepherd per node so each is included in the set of Dragon run-time service nodes.

This Launcher wrapper sets required environment variables including the number of nodes for the allocation
and the *DRAGON_MODE* environment variable that indicates that dragon is running in *multinode* mode.
The wrapper then executes the salloc command with the actual start of the launcher within it and any
launcher specific arguments passed into it.

.. _LauncherServerMode:

Launcher Server Mode
=================================

This section provides details of running the *Dragon Launcher* in *Server Mode*.
This mode can be used to support any user-defined interaction between the login
node and compute nodes running under the *Dragon* run-time services. Server mode
may be necessary for some multi-node applications but can be used in single-node
as well allowing a server application to run in either environment.

.. figure:: images/servermode.png
    :name: servermode

    **Dragon Server Mode**

In server mode there are two programs that are started by the launcher. The
*Server Front End* and the *Server Back End*. The front end runs on the login
node. The back end runs on the primary compute node. When the server front end
is started, it is started so that standard input and output are pipes.
On the back end the program is started and has access to the complete
Dragon run-time services.

.. figure:: images/server.srms1.png
    :scale: 75%
    :name: passthru-message-ex

    **PassThru Message Exchange**

The *Launcher* starts the
front end specifying that standard input and output are to be piped from/to
the launcher.

The *Server Back End* initiates contact with the *Server Front End* by sending a
:ref:`LAPassThruBF <lapassthrubf>` message. Initiating the conversation by first
sending this message guarantees that the backend will be ready to accept
messages on its channel. The *Server Back End* creates a *channel* for receiving
messages from the  *Server Front End* and provides the *channel id* in this
initial :ref:`LAPassThruBF <lapassthrubf>` message as the *r_c_uid* field. After
receiving this initial message, *Server Front End* can then send
:ref:`LAPassThruFB <lapassthrufb>`  messages to the *Server Back End* using this
*channel id*.

From the perspective of the implementer of both the *Server Front End* and the
*Server Back End* the exact mechanics of sending and receiving these *passthru*
messages can be managed by a few of API calls.  From the *front end* the
*send_to_backend* function sends a :ref:`LAPassThruFB <lapassthrufb>` message
containing  a user-defined string to a specified *channel id*. The
*send_to_backend* API call packages up the user-defined string into a
:ref:`LAPassThruFB <lapassthrufb>` message and writes it to the output pipe of the
*Server Front End*. This is a convenience function only. A programmer can write
their own code to carry out this functionality.

From the *back end* the programmer may use a *send_to_frontend*  call to build
and send a :ref:`LAPassThruBF <lapassthrubf>` message to the *front end*. The
*send_to_frontend* API call includes the *return channel id* as an argument. The
*send_to_frontend* packages up the data into a :ref:`:APassThruBF <lapassthrubf>`
message and sends it to the *Dragon Back End* which then routes it to the
*Launcher* (through *MRNet* in the multi-node case) and through the *Launcher*
to the *Server Front End*. This is a convenience function only. A programmer can
write their own code to carry out this functionality.

The only messages passed from/to the *Front End Server* and to/from the *Back
End Server* are the two *PassThru* messages and optionally a *LASeverModeExit*
message to indicate that the backend server has exited.

Any output from the
back end that is to be shared with the front end must be wrapped up in
a :ref:`LAPassThruBF <lapassthrubf>` message.

It is likely that the designer of
the front and back end services will design their own message structure to be
passed within the two *PassThru* messages. Any standard output or standard error
output generated by the *Back End Server* will automatically be written to the
console where the Launcher was invoked. If stdout or stderr is supposed to go to the
*Front End Server* then it must be captured by the *Back End Server* and routed
to the *Front End Server* in a :ref:`LAPassThruBF <lapassthrubf>` message.

Likewise, two *receive* API calls are also available. The *receive_from_backend*
and  *receive_from_frontend* functions can be called to receive messages. The
two *receive* API calls are implemented as awaitables in Python to support the
AsyncIO framework.

The backend of the server can initiate shutdown of *Server Mode* by sending the
*LAServerModeExit* message to the launcher. When the launcher receives this message
it forwards it to the frontend of the server and also responds to the command processor,
allowing the *serve* command to complete.

[TBD: How is API exposed/imported by the programmer. Exact packaging/use of API
should be described here. If we were to decide to not expose infrastructure
messages, then appropriate bindings of these API calls would need to be provided
for C/C++ and Python (and others?). In Python the interface should support the
AsyncIO framework.]

There are many possible uses for *Server Mode*. The next section provides
details of using *Server Mode* for implementing a *Jupyter Notebook Kernel*.
Another possible use is in providing a Python debugger interface to the *Dragon
Run-time Services*. Finally, it would be possible to use this mode to provide
HPC management services on a system. In each of these cases the dynamic nature
of Python would allow the applications to be developed incrementally and tested
incrementally, potentially saving a lot of costly development and testing time.

State Transitions
-------------------

.. figure:: images/launcherstates.png
    :name: launcherstates

    **State Diagram**

The four states of the Launcher define four states the launcher could be in. In
addition, there are a few more states during initialization that are not described
here. The transitions
shown in  the state diagram document how the Launcher moves from one state to
another. The state diagram  does not show all commands possible in command mode.
Specifically, commands that don't cause a transition to a state are not shown in
the state diagram.

The *Initialization* State takes care of bringing up the Dragon run-time services and then
transitioning to the *Command* state. The *Exit* state takes care of bringing down the
Dragon run-time services and terminating the launcher.

During initialization, if a program file, *PROG*, was provided on the
command-line (not for server mode), then the following commands are issued in
*Command Mode* once initialization is complete.

.. code-block:: python

    it = launch(exe="PROG")
    join(it[0])

If *PROG* is not executable then the *exe* is *python3* and *PROG* is passed as an
argument to the launch command.
During initialization, if *Server Mode* is specified, then the following commands are issued to
the *Command Mode* once intialization is complete.

.. code-block:: python

    serve(frontendprog, backendprog, frontendargs, backendargs, ...)

And, if *-r* is specified, then the following command is issued to the command processor where
*PROG* is the program given on the command-line.

.. code-block:: python

    run("PROG")

In this case, the *PROG* is a launch program and is run on the login node to control launching
of programs within the Dragon environment.

If *-i* is NOT specified on the command-line and the program exits, then the following
command is fed to the command processor when the program exits (i.e. after the join completes).

.. code-block:: python

    exit()

As a general rule, while initially in *Command* mode, commands will be
issued automatically for the simple cases of running a single program or
starting server mode. Command mode becomes visible to the user when the user
uses the *-i* option from the command line.

Supporting Jupyter Notebooks
--------------------------------

*Server Mode* was designed to support any distributed implementation of a server
between a login node and the primary compute node. One use case of this
functionality is in the implementation of a Jupyter Notebook kernel that runs
within the Dragon run-time services.

.. figure:: images/jupytermode.png
    :name: jupytermode

    **Dragon Server Mode for Jupyter Notebooks**


There are two supported methods to run a Jupyter notebook in conjunction with
the Dragon run-time services. The two methods have differing characteristics.

* Fat Login Mode
* Server Mode

Running the Jupyter notebook on a fat login node means that the notebook can be
long running. In this case, the IPython Kernel runs on the fat login node. From
within that IPython kernel a user can start a Dragon job by using the REPL
command mode of the launcher to launch a Dragon program. A program is launched by
using the Dragon launch command.

The benefit of fat login mode is that notebooks can be long-running. The
disadvantage is that while computations can be launched on the compute nodes,
the result is not available directly within the notebook. (Should we design a
serializable result to be sent back from a process?). There is no additional
support that is required of the Dragon run-time services required to run in this
mode.

When running Dragon in Server mode, a *Specialized Jupyter Kernel* is run on the login node
that interacts with the *Kernel Back End* running on the primary compute node
to provide the notebook kernel functionality on the compute node. The
disadvantage is that notebooks started in this mode only run as long as the
allocation runs. The advantage is that the Jupyter notebook is run within the
context of the Dragon run-time services and has full access to all of the
compute nodes in the allocation. In addition, intermediate results are available
to the notebook.

In Server Mode, the launcher starts two programs and distributes the
responsibilities between these two programs. In the case of Jupyter notebooks
the Specialized Jupyter Kernel provides the interface to the browser because it
is from the login node that socket connections can be made to remote browsers.
The Jupyter Kernel has several socket connections to maintain. The Kernel
Back End provides the REPL environment where Python code is executed and provides
the rest of the services of a Jupyter Python kernel.

The login node *Specialized Jupyter Kernel* must be written according to the
documentation on making `kernels in Jupyter`_. The
*Specialized Jupyter Kernel* conforms to the requirements of a Jupyter kernel.
The front end functions as a passthru to the *Specialized Jupyter Kernel Back
End* and passes all incoming messages from the Jupyter front end browser to the
Jupyter back end. The Jupyter messaging requirements are detailed in a document
titled `Messaging in Jupyter`_. A Jupyter
kernel has 5 sockets that each serve a different purpose. Messages between the
front end and the back end are defined for requests on these sockets and
responses to the front end (as yet to be determined). The Launcher is not
impacted by the design of the Jupyter support because all messaging between the
Jupyter front end and back end occurs within :ref:`LAPassThruFB <lapassthrufb>` and :ref:`LAPassThruBF <lapassthrubf>`
messages as defined in the :ref:`LauncherServerMode` section.



.. External links.
.. _kernels in Jupyter: https://jupyter-client.readthedocs.io/en/stable/kernels.html
.. _Messaging in Jupyter: https://jupyter-client.readthedocs.io/en/stable/messaging.html