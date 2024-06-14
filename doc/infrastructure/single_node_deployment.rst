
.. _SingleNodeDeployment:

Single Node Deployment
++++++++++++++++++++++

Single node mode runs everything on the host the initial ``dragon my.py`` command gets run on. In this respect
it is meant to operate exactly like the existing ``multiprocessing`` runtime, with the "spawn" launch method.

Below the steps of single node bringup and teardown are outlined.  Note that once the user application
(``my.py``) is running, managed processes and applications can be started using :ref:`GlobalServices`.  If the
user application decides to directly spawn processes itself, it retains the responsibilty for cleaning up any
resources they use.

.. figure:: images/deployment_single_node.svg
    :name: deploy-single-node 

    **Deployment diagram a single node**

.. figure:: images/singlenodeoverview.png
    :name: singlenode-overview 

    **Single-Node Overview of Dragon Services**

.. FIXME: NOTE: In the single-node case the :ref:`Launcher` serves as both frontned and backend component. So the launcher sends and receives several backend messages during bringup and teardown.

**FIXME**: Adapt UML Diagram to be correct

In the single-node case, as depicted in :numref:`deploy-single-node` + :numref:`singlenode-overview`, there is no :ref:`TransportAgent`, :ref:`MRNet` tree,
or :ref:`Launcher` backend service.  :ref:`Channels` in :numref:`singlenode-overview` are represented by the colored arrows. The
:ref:`Launcher` steps into the place of the Backend and the Shepherd communicates directly with the Launcher
instead of going  through the Backend and the :ref:`MRNet` tree. The :ref:`TransportAgent` is not started
since there is no off-node communication in this case. However, :ref:`LocalServices` and :ref:`GlobalServices`
still are present to provide the same level of service that is present in the multi-node case. While the
bringup and teardown of the Dragon :ref:`Services` is significantly different in the single-node and
multi-node cases, from :numref:`deploy-single-node` and :numref:`singlenode-overview` the overall structure is similar.

.. _SingleNodeBringup:

Single Node Bringup
===================

.. figure:: images/startup_seq_single_node.svg
    :name: startup-seq-single-node 

    **Startup Sequence on a single node**

The bringup of the Dragon run-time services is detailed in :numref:`startup-seq-single-node` and below, where also message descriptions
are given.

During single node bringup the Shepherd is started by the Launcher and a pipe is used to provide the initial
startup messages on the Shepherds stdin file descriptor. The message structure itself is identical to messages
that are later passed on channels. However, since the Shepherd brings up Channels, they are not available when
the Shepherd is started.

Initially the Shepherd process is started by the Launcher and run-time arguments are provided during the
process launch. The Shepherd accesses the object named *this* as outlined in the section
:ref:`LaunchParameters`. The channel Ids for both the Shepherd and Global Services are obtained from this
dictionary object under the names *GS_CUID* and *SHEP_CUID*.

The Shepherd process immediately sends a *SHIsUp* message as detailed in the section on the :ref:`Shepherd's
Other Messages<sh-other-messages>` to tell the launcher that it is up and running. It sends this message on
its standard output file descriptor which the launcher receives through it's pipe that was instantiated when
the Shepherd process was created.

The Shepherd then allocates a shared segment for the Dragon run-time services using a memory pool object and
then allocates a channel for itself with Channel Id 3. The Shepherd also allocates a channel for Global
Services with Channel Id 2.

Once the two channels are created the *SHChannelsUp* message is sent to the launcher. At this point the
Shepherd expects the first message on it's channel to be the *GSPingSH* message. Once that is received the
Shepherd responds with a *SHPingGS* message sent to the Global Services channel and it runs its AsyncIO loop
with a recv task ready to receive any other messages off its main queue. And that concludes the single-node
startup.

Discuss the startup function and how we enter into the main loop.

This section describes what has to happen between the different actors to bring up a runtime in a single node
case.


Transaction diagram
-------------------

This transaction diagram indicates the activities (denoted by **a** and a serial number) and messages (denoted
by **m** and a serial number).  Some activities between different actors can happen in parallel, or in
arbitrary order such as (example).  However, any inbound message to an entity must be received before any
subsequent activities can take place.

    .. container:: figboxright

        .. figure:: images/single_startup.srms1.png
            :scale: 75%


Activities
^^^^^^^^^^

1.  **Start shepherd process**

    *actor*
        Launcher

    *call*
        *note module or call that does this*

    *description*
        Launch the shepherd process locally with an OS spawn.
        See message 1.

2.  **Shepherd - launcher startup handshake**

    *actor*
        Shepherd

    *call*
        *tbd*

    *description*
        Creates default channel structures for the infrastructure.

            - Global Services's channel
            - Shepherd's channel
            - Launcher's channel

        Once these are created notify the launcher, see message 2.

3.  **Connect Launcher to Shepherd**

    *actor*
        Launcher

    *call*
        *tbd*

    *description*
        Attach to shepherd channel and ping the shepherd, see message 3.

4.  **Ping Launcher that Channels Are Up**

    *actor*
        Shepherd

    *call*
        *tbd*

    *description*


    *error*
        Notify launcher with error message on stdout and exit.

    *description*
        In the single node case this seems redundant, but since the launcher
        is a substitute for the backend in the multi-node case, the shepherd
        sends this extra message to function more like the multi-node case.


5. **All Channels are Up**

    *actor*
        Launcher

    *description*
        At this point all channels are up since there is only one
        node in the single-node bringup.


6.  **Pend on handshake message from Global Services through channel.**

    *actor*
        Launcher

    *call*
        *tbd*

    *description*
        Launcher blocks, waiting to hear from global services that it
        is started.  Probably nothing more than this, because if there
        is a problem with global services coming up the shepherd is
        the parent of global services and will be notified of a failure.

7.  **Start global services**

    *actor*
        Shepherd

    *preceded by*
        Default Channels are created.

    *call*
        X

    *description*
        Launches the global services process.  Will need to have some
        information passed in the command line or environment
        variables.  See message 5.

            - Default queue names
            - Logging level


8.  **Global services attach to default channels**

    *actor*
        Global Services

    *call*
        X

    *description*
        Connect to the default Global Services command
        channel as well as the input channels to the Shepherd and
        the Launcher.

9.  **Global Services ping to Shepherd**

    *actor*
        Global Services

    *call*
        X

    *description*
        This establishes that the Shepherd can communicate with global
        services.  See message 6

10. **Shepherd recv ping from Global Services**

    *actor*
        Shepherd

    *preceded by*
        Message 6, from Global Services

    *call*
        X

    *description*
        This is the second message that should be received on the
        Shepherd channel.  Return the ping to Global Services though
        Global Services command channel.  See message 7.

11. **Global Services complete handshake with Shepherd**

    *actor*
        Global Services

    *preceded by*
        Message 7, from the Shepherd

    *call*
        X

    *description*
        There may be some additional protocol here if there are facts
        about how things are set up that only the Shepherd knows.  But
        likely these sorts of things (like how big the shared segment
        is) are already provided to Global Services when it starts.

12. **Report to the launcher that the runtime is ready**

    *actor*
        Global Services

    *call*
        X

    *description*
        Sends a message to the launcher through the launcher's input channel
        that it is ok to connect to the Global Services command channel and issue
        the user program. See message 8.

13. **Initiate user program start**

    *actor*
        Launcher

    *call*
        Probably this should be wrapped in a special interface,
        *todo, tbd* but
        it will boil down to a ``dragon.globalservices.process.create``
        call and attendant protocol.

    *description*
        This issues the head user program to the runtime to execute.

        The user program may itself have command line parameters, so
        any special setup facts it needs must be passed through special
        environment variables.  This shouldn't need to be any
        different from any other launch.

        Note that this call will involve protocol with Global Services
        concerning *todo: add link* successful launch.
        See message 9.

14. **Register head process**

    *actor*
        Global Services

    *call*
        Might be wrapped, because the head process is special.  Or it
        could just be a special flag, and nothing more. *tbd*

    *description*
        The head process is special in that its exit (planned or not)
        means that the whole program is deemed to have ended,
        so Global Services needs a different code path for this
        situation.

15. **Issue Shepherd spawn command**

    *actor*
        Global Services

    *call*
        Should be ``dragon.globalservices.process.create``, just the
        normal code path.

    *description*
        This should be the normal proxy to the shepherd to spawn
        the user process.  See message 10.  It will need to carry not
        only the arguments and environment that the user specifies but
        also startup data.  See message 10

16. **Start my.py**

    *actor*
        Shepherd

    *preceded by*
        Message 10

    *call*
        Should be the low level os.spawn call, with parameters suitably
        filled in.

    *description*
        This is the final call that gets the user script running.  It
        should have in the environment enough information to start
        interacting with Global Services.  Since it is the first
        process and since it needs a special code path anyway, we may
        be able to skip the normal handshaking a new process does with
        Global Services, but probably should do this anyway.

        See Message 11.

17. **Confirm to Global Services that the process is launched.**

    *actor*
        Shepherd

    *call*
        This is part of the Shepherd side protocol between the Shepherd
        and Global Services involved in completing process creation.
        Should not be any different from normal local process startup.

        Note that in turn Global Services should confirm to the
        Launcher that the user process is started as part of the other
        side of that protocol but this should not be expected to
        precede output getting forwarded to the Launcher.

    *description*
        Confirms that the process is really started.  If it hasn't
        started, the error has to propagate back to Global Services,
        then back to the Launcher which should initiate teardown.

18. **Package output**

    *actor*
        Shepherd

    *call*
        Normal stdout processing path.

    *description*
        Example of Shepherd packaging up output.

19. **Recieve output at launcher.**

    *actor*
        Launcher

    *call*
        Normal processing on Launcher input channel.

    *description*
        Aggregated output comes in packaged form to the Launcher in the
        channel.  At a minimum the Launcher should be able to know
        which process (in terms of p_uid) the output is coming from,
        but also metadata.


Messages
^^^^^^^^

1.  **Shepherd start**

    *source*
        Launcher

    *target*
        OS (Shepherd)

    *transport*
        OS call

    *payload*
        Command line parameters + environment specific to Shepherd.

        Todo: make table of these.  Env vars?

            - name of shared memory segment
            - size of shared memory segment
            - ...?

    *class*
        None

2.  **Shepherd is started**

    *source*
        Shepherd

    *target*
        Launcher

    *transport*
        Shepherd stdout

    *payload*
        Nothing other than message.

    *class*
        SHPingBE

3.  **Launcher is started/ready**

    *source*
        Launcher

    *target*
        Shepherd

    *transport*
        Shepherd Channel

    *payload*
        Nothing other than message.

    *class*
        BEPingSH

4.  **Shepherd channels are up**

    *source*
        Shepherd

    *target*
        Launcher

    *transport*
        Launcher Channel

    *payload*
        Nothing other than message

    *class*
        SHChannelsUp


5.  **Global Services start**

    *source*
        Launcher

    *target*
        OS (Global Services)

    *transport*
        OS call

    *payload*
        Command line parameters + environment specific to Global Services.

        Todo: make table of these.  Env vars?

            - name of shared memory segment
            - size of shared memory segment
            - ...?

    *class*
        None


6.  **Global Services to Shepherd ping**

    *source*
        Global Services

    *target*
        Shepherd

    *transport*
        Shepherd input channel

    *payload*
        Nothing other than the message

    *class*
        GSPingSH


7.  **Shepherd to Global Service ping acknowledge**

    *source*
        Shepherd

    *target*
        Global Services

    *transport*
        Global Services input channel

    *payload*
        Contains the 'index' of the shepherd in the message, but in the
        single node case this is always 0.

    *class*
        SHPingGS


8.  **Global Services runtime up**

    *source*
        Global Services

    *target*
        Launcher

    *transport*
        Launcher input channel

    *payload*
        Nothing other than the message

    *class*
        GSIsUp


9.  **Create head user process message**

    *source*
        Launcher

    *target*
        Global Services

    *transport*
        Global Services input channel

    *payload*
        What is necessary to launch a process in
        ``dragon.globalservices.process.create`` but packaged
        indicating it is the head process.  This could be contextual however.


    *class*
        GSProcessCreate


10. **Shepherd directive to create head process**

    *source*
        Global Services

    *target*
        Shepherd

    *transport*
        Shepherd input channel

    *payload*
        Standard Shepherd process start command, tbd.

    *class*
        SHProcessCreate


11. **User process stdout forwarding**

    *source*
        User process

    *target*
        Shepherd

    *transport*
        stdout file descriptor of user process, owned by Shepherd

    *payload*
        whatever the user process prints

    *class*
        None


12. **Shepherd packaged stdout forwarding**

    *source*
        Shepherd

    *target*
        Launcher

    *transport*
        Launcher input channel

    *payload*
        Packaged and consolidated stdout message

        Includes:
            - consolidated stdout
            - process or processes that produced it
            - process metadata as the launcher won't know the p_uid

    *class*
        SHFwdOutput


.. _SingleNodeTeardown:

Single Node Teardown
====================

This section describes the (normal path) message flow to
bring down a single node runtime.  As is discussed under the first
activity below, this could be made better than described here, **FIXME**,
making what is described below what should happen when the main process
crashes unexpectedly.

In an abnormal situation, the *AbnormalTermination* message may be received
by the Launcher from either the Shepherd or Global Services. In that case,
the launcher will initiate a teardown of the
infrastructure starting with activity 5 and message 4 in the diagram below.

Transaction diagram
-------------------

:numref:`teardown-seq-single-node` depicts the normal single node teardown sequence and is also included
in :ref:`SingleNodeTeardown` where message defintions are given in more
detail. The tear down is initiated by Global Services. The Shepherd shuts down
as a result of the *SHTakedown* message sent from the launcher but the sequence
is initiated by Global Services in response to the exit of the head process.
Global Services is notified of a process exit via the *SHProcessExit* message.
Global Services then recognizes it is the head process exiting and it initiates
the teardown of the Dragon Services.

**FIXME**: Discuss the exit from the main loop and how tear down proceeds.

    .. figure:: images/single_teardown.srms1.png
        :scale: 75%
        :name: teardown-seq-single-node 

        **Single-Node Teardown Sequence**

Activities
^^^^^^^^^^

1.  **Main process exits**

    *actor*
        Main user process

    *call*
        X

    *description*
        This description of the teardown is assuming that the process
        simply quits unexpectedly.  This could be made more graceful
        by arranging for the main process to register an exit handler
        using the ``atexit`` package, which would handshake with
        Global Services before process exit.  See message 1.


2.  **Notify Global Services that main process has exited**

    *actor*
        Shepherd

    *preceded by*
        Message 2, from the Shepherd, indicating that the process is
        gone.  Note that if the process has the more graceful exit path
        this message should still be collected, giving a final
        confirmation that the process has gone away, but that following
        cleanup activities may be allowed to be proceed.

    *call*
        X

    *description*
        This is the normal notification path for process exit back to
        global services - the Shepherd always will send this message
        when a managed process it has started (and is servicing the
        stdin and stdout of) when the subsidiary process has exited.

3.  **Clean up existing globals**

    *actor*
        Global Services

    *call*
        X

    *description*
        This action serves to cover everything Global Services needs to
        do to clean up existing processes and Channels as best it can.

        Of course, if the managed processes have created a lot of their
        own resources it is up to them to clean up properly.  Note that
        workers in Pools may have their own graceful exit command, and
        it may be smart to have Global Services know about this and be
        able to send a cleanup command on that interface as well.

        *TBD*: should we try to get parallel interpreters
        started via ``multiprocessing.Process`` to exit in this way?
        It should be possible.

4.  **Notify launcher of exit**

    *actor*
        Global Services

    *call*
        X

    *description*
        Sends a message to the launcher that the head process has
        exited, and waits for a message back from the launcher to
        either start a new head process or have the runtime exit.

5.  **Issue runtime teardown**

    *actor*
        Launcher

    **preceded by**
        Message 3, that the head process has exited, from Global Services

    *call*
        X

    *description*
        Here is where the Launcher could start a new head process or
        decide to tear down the existing head process.  This activity
        commits the launcher to bringing everything down and exiting.
        See Message 4.

6.  **GS Release from Shepherd**

    *actor*
        Global Services

    *call*
        X

    *description*
        This is the last message that Global Services will send the
        Shepherd, indicating that it is no longer going to interact
        with any of the channels. See Message 5.

7.  **GS detach from channels.**

    *actor*
        Global Services

    *call*
        X

    *description*
        Note: this and activity 6 might really be merged into one
        thing, if the local allocation of memory in the shared segment
        is through the Shepherd.

8.  **Global Services exit**

    *actor*
        Global Services

    *call*
        X

    *description*
        The Global Services process exits here.

9. **Direct the Shepherd to halt**

    *actor*
        Launcher

    *call*
        X

    *description*
        The launcher sends a message to the Shepherd, indicating a
        clean exit.  Note that the Shepherd can assume that Global
        Services has exited when this message is received.
        See Message 7.

10. **Detach from Channels**

    *actor*
        Launcher

    *call*
        X

    *description*
        The launcher detaches from channels and prepares to exit
        gracefully once shepherd exits.

11. **Unmap shared segment**

    *actor*
        Shepherd

    *preceded by*
        Message 7.

    *call*
        X

    *description*
        The Shepherd gives the shared memory segment back to the OS.

12. **Shepherd Notifies Launcher of exit.**

    *actor*
        Shepherd

    *call*
        X

    *description*
        Shepherd declares to Launcher that everything is cleaned up and
        it is exiting.  See Message 8.

13. **Shepherd exit**

    *actor*
        Shepherd

    *call*
        X

    *description*
        Shepherd exits.

14. **Launcher exit**

    *actor*
        Launcher

    *call*
        X

    *description*
        Launcher exits


Messages
^^^^^^^^

1.  **User process exit**

    *source*
        Main user process

    *target*
        Shepherd

    *transport*
        OS exit (side effect of stdout monitoring)

    *payload*
        None

    *class*
        None

2.  **Notify GS process exited**

    *source*
        Shepherd

    *target*
        Global Services

    *transport*
        Global Services Channel

    *payload*
        p_uid of process, possibly exit code.  Other info?  Part
        of the normal Shepherd-GS messaging.

    *class*
        SHProcessKillResponse

3.  **Notify Launcher head process exits**

    *source*
        Global Services

    *target*
        Launcher

    *transport*
        Launcher Channel

    *payload*
        Exit code of head process.

    *class*
        GSHeadExit

4.  **Tell Global Services to halt the runtime**

    *source*
        Launcher

    *target*
        Global Services

    *transport*
        Global Services channel

    *payload*
        Just the message itself.

    *class*
        GSTeardown

5.  **Tell Shepherd Global Services is releasing channels**

    *source*
        Global Services

    *target*
        Shepherd

    *transport*
        Shepherd Channel

    *payload*
        Just the message itself.

    *class*
        GSChannelRelease

6.  **Tell Launcher Global Services is halted**

    *source*
        Global Services

    *target*
        Launcher

    *transport*
        stdout

    *payload*
        Just the message

    *class*
        GSHalted

7.  **Direct the Shepherd to quit**

    *source*
        Launcher

    *target*
        Shepherd

    *transport*
        Shepherd's stdin

    *payload*
        Just the message

    *class*
        SHTeardown

8.  **Shepherd final goodbye**

    *source*
        Shepherd

    *target*
        Launcher

    *transport*
        Shepherd's stdout

    *payload*
        Just the message, just before Shepherd exits.

    *class*
        SHHalted
