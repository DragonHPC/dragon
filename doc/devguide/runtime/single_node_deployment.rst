
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

