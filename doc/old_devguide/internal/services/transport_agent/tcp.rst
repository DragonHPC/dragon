.. _TSTA:

TSTA
++++


**FIXME: This looks like a TSTA design spec. What do we do with it ?**

.. figure:: images/transport_agent.svg
    :name: transport-agent 

    **Socket based transport agent TSTA **

.. container:: figboxright

    .. figure:: images/overview.png
        :scale: 75%
        :name: tsta-overview 

        **TSTA Design Overview**

The transport service of Dragon is designed as both a prototype and publicly available version of the cluster
version of Dragon. In the design of the transport service, the gateway channel and the message, both within a
managed memory pool, are the access point between channels and the transport service processes/threads. The
design separates the on-node Dragon services and the TSTA with any communication going through the gateway
channel and the message in the gateway channel. Any information in the channel handle or the message handle is
off-limits to the TSTA process since it will not have access to user process handles. Communication between
user processes and TSTA occurs through message and channel information as it is stored within shared memory
pools.

:numref:`transport-agent` provides the system view of the infrastructure. Each node has a designated port which is used to
connect up a socket between each pair of nodes.  All the threads shown in the overview exist on a node but in
this overview each socket monitor and its associated local channels exist on a separate host.

When work arrives from on-node user processes, The Gateway Monitor thread routes any outgoing communication to
the appropriate host monitor thread which then uses its associated socket connection to send to the designated
target host.

:numref:`tsta-overview` provides the view of one node within a Dragon infrastructure allocation. This view helps in
understanding how the design looks from a single node view.

Each node has a designated port that is used for setting up the sockets to the other n-1 nodes in the
allocation. When connections arrive the port monitor sets up the socket connection and socket monitor thread
to service that connection.  The socket monitor thread reads requests as they arrive on their associated port.
Each socket is associated with one other node in the allocation. When requests arrive from a remote host, they
are processed by the socket monitor thread to complete the requested work.

.. container:: figboxright

    .. figure:: images/overview2.png
        :scale: 75%
        :name: tsta-overview-onenode 

        **TSTA Design Overview of One Node**

The sections in this design document describe, in more detail, the handling
of sending and receiving from remote channels.


Initialization
==============

:ref:`MultiNodeBringup` already contains the startup sequence of TCP/HSTA.  These notes are provided here to
give some additional details.

    #. When the Shepherd creates the memory pools and default channels it
       also creates the gateway channels. There is a pre-configured (tunable) number of gateway channels
       (environment variable **DRAGON_NUM_GATEWAYS**) that are doled out to processes on a round robin schedule
       when the Shepherd creates a process. There is one internal host channel per other nodes in the allocation.
       So, there are *n-1* host channels where *n* is the number of nodes in the allocation.
    #. It's possible that the Shepherd could create the internally used routing channels.
       Alternatively, the TSTA may create these routing channels internally.
    #. The LAChannelsInfo message contains a list of hostids and ip addresses
       as they map to each node index within the allocation. This way ip addresses
       and host ids can be correlated within TCP/HSTA.
    #. The LAChannelsInfo message is provided to TCP/HSTA during startup as shown in
       :ref:`MultiNodeBringup` transaction diagram.
    #. TSTA uses that information to wire up its connection between nodes.
    #. When the Shepherd starts a process, it adds the serialized descriptor
       of a gateway channel (one of the *n* that are configured) in a round
       robin fashion to the **DRAGON_GATEWAY_CD** environment variable of the process.

There may be an opportunity to use these internal channels as an organized way of doing RDMA in an HSTA
implementation. If the internal channel is in a pool that is pinned and set up for RDMA, then we may be able
to quiesce and sync an internal channel from one node to another on a regular time interval.

Channel Library Changes
=======================

.. container:: figboxright

    .. figure:: images/channels.png
        :scale: 75%
        :name: channels 

        **Channel Changes**

The channels library requires some changes to support TSTA. However, these changes are all self-contained
within library calls to the channels library. No user code is affected or needs to know it is a remote
channel. They are as described in the following sections.

Channel Descriptor
-------------------
Addition to the channel descriptor API support for TSTA.

    * The hostid needs to be extractable from a serialized descriptor of a channel.

Channel
---------
Changes required in the channel structure to support TSTA.

    * A tag_num (tag number) is  added to the header of the channel. This
      value is incremented for each send on the channel.
    * A new ACK hashset
      is added to the Channel header (after Ordering Table). The max entries
      will be two times the number of message blocks. The key is a tag_num called a tx_id or transaction
      id when used in this ACK hashset.
    * An ACK hashset lock is added as well for interacting with the ACK hashset.

Channel Handle
----------------
Changes required in the channel handle structure to support TSTA.

    * The hostid is added to the channel handle.
    * A serialized channel descriptor pointer is added to the channel handle.
    * When you attach to a remote channel the remote hostid and the remote channel descriptor
      are initialized and the channel attaches to the gateway channel instead as shown in :numref:`channels`.
    * The channel attach library function must be modified to account for local and remote channels.
      The hostid function in the hostid.h header file is used to get the hostid.

Communication
---------------
Communication between the user process and TSTA occurs through the use of two shared memory locations. Within
the local send and receive one of these shared memory locations was used prior to the design of TSTA. The TSTA
design introduces one more shared memory location. :numref:`remote-sr-struct` depicts the two shared memory regions.  The *ACK
Request* shared memory contains information about where to route the message that must be sent through TSTA
(i.e. it was determined to be non-local).

The thick arrows in :numref:`remote-sr-struct` indicate serialized descriptors that are stored within the message block of the
channel and the *ACK Request*. The use of serialized descriptors means that any process (i.e. the user process
AND the TSTA process/threads) can access those shared memory regions by attaching to the serialized
descriptor.  Attaching to a serialized memory descriptor maps the shared memory region into the address space
of the caller.

The *ACK Request* format is given by the **ChACKReq** structure definition below. The *ACK Request* is created
by the Channel library, under the user process.  On a send call to the library, the message payload is copied
into shared memory as it is on a local send, but instead of having the message block in the channel refer
directly to the payload, it is referred to by the *ACK Request* structure instead.

On a receive request, the *ACK Request* either refers to a payload landing area in shared memory, or it
contains a serialized pool descriptor which can then be used by the library to allocate the *Message Payload*
shared memory when the message is received.

On a receive, the user process uses the *ACK Request* to access the serialized descriptor of the received
*Message Payload* or to place the serialized descriptor of the newly created *Message Payload* landing area if
the receive operation requested that the space be allocated.

.. container:: figboxright

    .. figure:: images/RemoteSendRecvStructure.png
        :scale: 75%
        :name: remote-sr-struct 

        **Shared Memory Layout of ACK Send or Recv**


A few new messages are required for the TSTA design. The details are provided
below.

**FIXME: This looks like it belongs to the Messages API page**

Note: Part of the message structure is within the Ordering Table (OT) of the channel. The ordering table entry
(see *pack_ot_item* and *unpack_ot_item*) is altered to allow for storing the ACK field. The ACK field is an
integer field containing one of three values for buffered, deposited, and received for the ackknowledgement of
sending messages. The default is buffered ACK. The ACK field goes in the Ordering Table of the header and is a
fourth value to be added.

1.  **ChACKReq**

    *purpose*

      Used to wrap both send and receive requests with ACK requested. This is
      placed in the channel by a send or receive operation requesting an
      acknowledgement. It has a C struct definition and is the definition of the
      *ACK Request Meta Information*.

    *fields*
      **dest_hostid**
          - integer
          - a 32-bit integer representing the destination/target hostid of the send or receive request.

      **ack_type**
          - integer
          - The type of ACK required. See `Channel Acknowledgements`_.

      **tx_id**
          - integer
          - The acknowledgement transaction id. This is used when TSTA completes the acknowledgement
            on the origin machine after carrying out the receive or send on the target machine.

      **desc_type**
          - integer
          - indication of type of descriptor in the payload_desc field. If this is a send request,
            then it must be a memory descriptor. If it is a receive request it may be a memory
            descriptor if the payload landing is being supplied by the user. Otherwise, it is a
            pool descriptor, indicating where the payload landing is to be allocated once we have
            the received message on the origin machine.

            - POOL_DESC=1, MEM_DESC=2

      **rc**
          - integer
          - A return code indicating the result of the send or receive. When an ACK was requested,
            the return code from the ACK response will be deposited here.

      **recvreq**
          - boolean
          - true indicates this is a receive request, false indicates it is a send request

      **err_str**
          - string
          - When a non-success return code is indicated, this field will contain a base64 encoded
            error string if one is available.

      **dest_cd**
          - string
          - the base64 encoded serialized destination/target channel descriptor

      **payload_desc**
          - string
          - A base64 encoded serialized descriptor. It might be a memory descriptor or a pool descriptor
            according to the value **desc_type**. On a receive request, if this is a memory desciptor it
            refers to the landing pad for the payload. On a send request, the payload to send is referred
            to by this memory descriptor. If it is a base64 serialized pool descriptor, it must be
            a receive request and refers to the pool to use when allocating the landing pad for the
            received message until the message is received, at which time it will be overwritten with
            the serialized memory descriptor to the received message.

2.  **TSRemoteSend**

    *type enum*
        TS_REMOTE_SEND

    *purpose*
        This message is created by the TSTA, when a ChACKReq is
        picked up from the gateway channel. This message is then serialized and
        forwarded to the target host to send data to it.

    *fields*
        **payload**
          - string
          - A string representing the binary data to be transmitted
            to the target machine.

        **payload_sz**
          - integer
          - the number of bytes in the payload string

        **origin_hostid**
          - integer
          - a 32-bit integer representing the origin hostid of the send request

        **dest_cd**
          - string
          - the serialized destination/target channel descriptor

        **dest_cd_sz**
          - integer
          - the number of bytes in the dest_cd string

        **ack_type**
          - integer
          - The type of ACK required. See `Channel Acknowledgements`_. If ACK_RECEIVED is
            specified, then TSTA will be responsible for acknowledging the receipt of this
            message on the target machine. The sending user process on the origin machine
            will either wait for the acknowledgement or have to call a method to determine
            that the acknowledgement has occurred.

        **gateway_cuid**
          - integer
          - The cuid of the gateway channel on the origin machine.

        **tx_id**
          - integer
          - The acknowledgement transaction id. This is used when TSTA completes the acknowledgement
            on the origin machine after carrying out the receive or send on the target machine.

        **req_desc**
          - string
          - This is a memory descriptor to the *ACK Request* data. This is not
            a serialized descriptor. It is passed back to the origin for quick access to the
            *ACK Request* on the origin at the completion of the request. Since this contains a
            pointer, it can only be accessed from TSTA on the origin node where the same address
            space is available.

        **req_desc_sz**
          - integer
          - The number of bytes in the request descriptor

    *response*
        See the TSRemoteSendACK message.

    *see also*
        TSRemoteRecv

3.  **TSRemoteRecv**

    *type enum*
        TS_REMOTE_RECEIVE

    *purpose*
        This message is created by the TSTA, when a ChACKReq is
        picked up from the gateway channel. This message is then forwarded
        to the target host to receive data from it.

    *fields*
        **payload_sz**
          - integer
          - an integer representing the size of the payload area for the received
            message on the origin machine. A size of 0 indicates there are no
            restrictions on the received message size.

        **origin_hostid**
          - integer
          - a 32-bit integer representing the origin hostid of the send request

        **dest_cd**
          - string
          - the base64 encoded serialized destination/target channel descriptor

        **dest_cd_sz**
          - integer
          - the number of bytes in the dest_cd string

        **ack_type**
          - integer
          - The type of ACK required. See `Channel Acknowledgements`_. ACK_DEPOSITED
            is the only supported ACK type for a receive operation.

        **gateway_cuid**
          - integer
          - The cuid of the gateway channel on the origin machine.

        **tx_id**
          - integer
          - The acknowledgement transaction id. This is used when TSTA completes the acknowledgement
            on the origin machine after carrying out the receive or send on the target machine.

        **req_desc**
          - string
          - This is a memory descriptor to the *ACK Request* data. This is not
            a serialized descriptor. It is passed back to the origin for quick access to the
            *ACK Request* on the origin at the completion of the request. Since this contains a
            pointer, it can only be accessed from TSTA on the origin node where the same address
            space is available.

        **req_desc_sz**
          - integer
          - The number of bytes in the request descriptor

    *response*
        See the TSRemoteRecvACK message.

    *see also*
        TSRemoteSend

4. **TSRemoteSendAck**

   *type enum*
        TS_REMOTE_SEND_ACK

   *purpose*
        This message is sent from the target node to the origin node internally
        to TSTA to indicate the acknowledgement of a send request when
        ACK_RECEIVED was specified.

   *fields*
        **ack_type**
          - integer
          - The type of ACK requested. See `Channel Acknowledgements`_. This
            is included in this message purely for debug purposes and could
            be omitted from an implementation.

        **gateway_cuid**
          - integer
          - The cuid of the gateway channel on the origin machine.

        **tx_id**
          - integer
          - The acknowledgement transaction id. This is used when TSTA completes
            the acknowledgement on the origin machine after carrying out the
            send and waiting for the acknowledgement on the target machine. When
            responding to this message (see the TSRemoteSendACK message) the
            req_desc is used to update the ChACKReq structure before the tx_id
            is deleted. The tx_id is then deleted from the gateway channels ACK
            hashset.

        **req_desc**
          - string
          - This is a memory descriptor to the *ACK Request*
            data. This is not a serialized descriptor. It is passed back to the
            origin for quick access to the *ACK Request* on the origin at the
            completion of the request. Since this contains a pointer, it can
            only be accessed from TSTA on the origin node where the same address
            space is available.

        **req_desc_sz**
          - integer
          - The number of bytes in the request descriptor

        **rc**
          - integer
          - The return code from the send operation with ACK.

        **err_str**
          - string

          - If the return code is non-success, then this is a string indicating
            the error that occured while executing the send request.

   *see also*
        TSRemoteSend

5. **TSRemoteRecvAck**

   *type enum*
        TS_REMOTE_SEND_ACK

   *purpose*
        This message is sent from the target node to the origin node internally
        to TSTA to indicate the acknowledgement of a recv request when
        ACK_DEPOSITED was specified.

   *fields*
        **ack_type**
          - integer
          - The type of ACK requested. See `Channel Acknowledgements`_. This
            is included in this message purely for debug purposes and could
            be omitted from an implementation.

        **payload**
          - string
          - The binary bytes of the received message.

        **payload_sz**
          - integer
          - The number of bytes in the received message.

        **gateway_cuid**
          - integer
          - The cuid of the gateway channel on the origin machine.

        **tx_id**
          - integer
          - The acknowledgement transaction id. This is used when TSTA completes the acknowledgement
            on the origin machine after carrying out the receive or send on the target machine. The
            tx_id in the channel ACK hashset gets deleted to indicate the ACK is complete. The
            req_desc is used to update the ChACKReq structure before the tx_id is deleted.

        **req_desc**
          - string
          - This is a memory descriptor to the *ACK Request*
            data. This is not a serialized descriptor. It is passed back to the
            origin for quick access to the *ACK Request* on the origin at the
            completion of the request. Since this contains a pointer, it can
            only be accessed from TSTA on the origin node where the same address
            space is available.

        **req_desc_sz**
          - integer
          - The number of bytes in the request descriptor

        **rc**
          - integer
          - The return code from the receive operation.

        **err_str**
          - string
          - If the return code is non-success, then this is a string indicating the error
            that occured while executing the receive request.

   *see also*
        TSRemoteRecv



Channel Attach/Detach
-----------------------

During channel attach, the host_id is extracted from the channel descriptor. If
it does not match the current host_id, then a remote channel descriptor handle
is initialized and the gateway channel is attached. Otherwise, the normal local
channel descriptor is initialized. Detach has similar detach logic.

    * ALTERNATIVE : We could have a remote channel attach do a channel attach
      on the remote node before returning from the attach function call.

.. _TSTAChannelAcknowledgements:

Channel Acknowledgements
---------------------------

On send operations, a new ACK parameter will be added to wait on
an acknowledgement type for the send.

There are four values for the ACK as indicated below.

    * ACK_DEFAULT = 0 : Use the default send/recv behavior.
    * ACK_BUFFERED = 1 : Placed in buffer to send (send default behavior)
    * ACK_DEPOSITED = 2 : In target channel (recv default behavior)
    * ACK_RECEIVED = 3 : Receiver picked up message.

The default value for ACK on a send operation is ACK_BUFFERED meaning the message
was placed in a channel. Internally, the remote receive operation will use the
ACK_DEPOSITED acknowledgement.

ACK is not valid on a receive operation except when used internally for remote receives.
ACK_DEPOSITED is not valid for a send operation but is used internally in the
library. The user-level API should check that the user has not used this option,
but an internal send operation should allow it (it is used to implement a remote
receive operation).

The ACK_DEPOSITED and ACK_RECEIVED both rely on another process invoking the new
dragon_channel_ack function to complete the acknowledgement. The channel send or
recv operation completes once the acknowledgement has been received.

ACK_RECEIVED means the message has been picked up from the channel.

The following two sections describe how the ACK is used on both send and recv operations
in more detail.


Channel Send
--------------

.. container:: figboxright

    .. figure:: images/origchannelops.png
        :scale: 75%
        :name: orig-sr-state 

        **Original Send/Recv State Diagram**

During a channel send operation, the type of channel descriptor is determined.
If local, follow the local path. The original local path of send and receive operations is
provided in :numref:`orig-sr-struct`. :numref:`new-sr-struct` provides the details of the new local send and receive that
includes remote channels. If remote, we send to the local gateway channel instead.

The default behavior on a local channel is ACK_BUFFERED. In this case, no waiting
is required and the ACK hashset is not used.

When the usage table of the local channel is modified (under a lock), the tag_num is
designated as the current tx_id and the tag_num is incremenented in the channel for the next
available tag number.

When the ordering table is accessed, the ACK value is written into the ordering table
for use when the message is received.

If ACK_DEPOSITED or ACK_RECEIVED is specified, then the message to be sent is a
new *ACK Request* message structure (as described above) and the wrapped
message is added to the channel. The *ACK Request* along with the *Payload*
is allocated in shared memory and the ACK hashset of the channel is modified
to add the tx_id as a key value.

For ACK_DEPOSITED (internal use only) and ACK_RECEIVED on a local channel, it
means that some other process will mark the ACK as complete at a later time. The
tx_id is used as the hashset key. Marking it complete means the hashset key
is removed from the channel hashset.

Until the ACK is marked complete on a local channel, the send operation
will do a sleepy poll waiting for the ACK to complete. When that occurs, the send
operation will return.

ACK_BUFFERED will return immediately with no waiting.

On a local channel the send operation does a sleepy poll until the tx_id is
deleted from the ACK hashset. If ACK_BUFFERED was specified, then it was
never added and this sleepy poll loop just falls through. Otherwise it waits for
the other process to mark the ACK complete. For remote channels the sleepy poll
is at the end of the send operation to the gateway channel.


Channel Receive
-----------------

.. container:: figboxright

    .. figure:: images/newchannelops.png
        :scale: 75%
        :name: new-sr-state 

        **New Send/Recv State Diagram**

We add one additional optional parameter to the Channel Receive operation (for both local and remote
receives), the Pool that should be used to allocate space to store a received message. This pool must be local
to the node on which the receive is done. If no pool is provided and the memory descriptor for receiving the
message is not provided, then the channel's pool will be used.

During a channel receive operation, the type of channel descriptor is determined. If the channel is a local
channel, follow current path with one addition. When a message is received, the ordering table (OT) contains
the ACK field. If ACK_RECEIVED or ACK_DEPOSITED is found in the ordering table the message in the channel is a
new *ACK Request* message structure. From the *ACK Request* we get the tag number and the destination host id.
If the destination hostid matches the current hostid then this is a local channel receive with ACK requested.
Since this is a local channel we want to acknowledge the receipt of the message by calling the
dragon_channel_ack function, states q27-29 in :numref:`new-sr-struct`, to acknowledge the receipt of the message. This will
result in releasing the process that was waiting on the ACK_RECEIVED or ACK_DEPOSITED confirmation. All this
logic is internal to the library.

If the channel is a local channel (think gateway channel - but it applies to any channel) and the *ACK
Request* message's target hostid does not match the current hostid, then we return the received message with
no call to the dragon_channel_ack function. This function is called later by the transport service.

If the channel handle is a remote handle, we create a ACK Request message and send that message to the gateway
channel instead. See the ChACKReq message structure for details of what's included in this message.

Internally it sends this request to the gateway channel with an ACK_DEPOSITED acknowledgement request. That
internal send request, waits for a process (the transport service) to indicate it was deposited by calling the
dragon_channel_ack function. This dragon_channel_ack call is done by the transport service when the response
has been received and the message deposited in the wrapped message. The internal send operation then returns.

Recalling this was initiated by a receive from the original user process, that internal send call returns and
the message descriptor from the wrapped message is copied to the memory descriptor for the received message.

The received message is now deposited into the memory descriptor that resides inside the receive request
message and the channel receive operation completes and returns.

TSTA Design
===========

The TSTA component is composed of a number of threads. Each instance of TSTA has one designated port, the same
port on all nodes. The **DRAGON_GATEWAY_PORT** environment variable provides this port number. Connections
from all the other nodes in the allocation arrive on this port during initialization of TSTA. The accept of
the connection then assigns a new port for the direct communication coming from that remote node. A socket
monitor thread is started to read requests coming from that socket connection.

The TSTA also has one thread per gateway channel. Each gateway monitor thread is responsible for reading a
message from its gateway. These threads do a sleepy/non-blocking receive on their channel waiting for a
message to receive.

These gateway monitor threads then route a message, through an internal channel, to the correct host monitor
thread based on the target host id. The host monitor thread is reponsible for forwarding the request on its
socket to the remote target node.

On the remote target node, a socket monitor thread waits for requests to come in on its socket and does those
operations on the local channels. Any response that would be required (think ACK), is sent back to the origin
host by sending it to the corresponding internal channel for that origin host.

The following sections provide details of how send and receive operations work within TSTA.

Channel Send via TSTA to Remote Host
---------------------------------------

    .. container:: figboxright

        .. figure:: images/send.srms1.png
            :scale: 75%
            :name: remote-send-op 

            **Remote Send Operation**

A gateway monitor thread is in charge of receiving requests from its gateway channel and sending the request
to the appropriate internal host channel which then gets read by the host monitor thread and the request gets
forwarded to the remote host. :numref:`remote-send-op` provides details of how TSTA accomplishes this.

Within TSTA, the request is packaged in serialized JSON format. The format conforms to the *TSRemoteSend*
message structure.

The socket monitor thread gets this request on its socket. It then interacts with the local channels on the
target host to carry out the required send or receive. If there is an ACK to send back, the socket monitor
thread creates another thread to wait for the ACK. This ACK thread then writes the acknowledgement message to
the internal channel to route the response back to the origin host.

When interacting with the target channel (on the target machine) the same ACK type is used. When the send
completes, assuming ACK_DEPOSITED or ACK_RECEIVED was specified, a message is sent to the internal channel of
the origin host to confirm the ACK and the socket monitor thread on the origin system that receives that
response then does the ACK on that gateway channel where the user process is currently suspended and waiting
for the ACK.

Channel Receive via TSTA from Remote Host
-------------------------------------------

    .. container:: figboxright

        .. figure:: images/recv.srms1.png
            :scale: 75%
            :name: remote-recv-op 

            **Remote Receive Operation**

A process similar to implementing send is done to do the receive. :numref:`remote-recv-op` provides details of the interaction
within channels and TSTA to accomplish a receive. The gateway monitor thread picks up the original receive
request. It sends over a similar message to the target host - the TSRemoteRecv msg. The socket monitor process
picks up the request on the target node and does the receive with the ACK_DEPOSITED confirmation. When it
finishes the receive it packages a TSRemoteRecvACK response message and sends it to the origin host through
the internal TSTA host channel.

The origin host gets this message and from the memory descriptor in the TSRemoteRecvACK message, it updates
the original ChACKReq message with the delivered payload. Then it calls the dragon_channel_ack function which
deletes the ACK key from the ACK hashset. This releases the original user receive request which completes by
copying the memory descriptor back out of the received message (from the original ChACKReq message) and then
returns to the user program.

Extended Channels API
=====================

This is the extensions to the Channels API in support of Acknowledgements. From the user's perspective,
sending on-node and off-node are handled identically. There are no differences in the API between on-node and
off-node. However, there is a dependency on the off-node send and receive to have an DRAGON_GATEWAY_CD
environment variable set to the serialized gateway channel descriptor used for this process to send off-node
requests.

Existing API calls are not affected. They remain the same. These are the new extended API calls in support of
acknowledgements.

Channel Functions
------------------

Channel Internal Function
^^^^^^^^^^^^^^^^^^^^^^^^^

.. c:function:: dragonChannelError_t dragon_channel_serial_desc_get_hostid(const dragonChannelSerial_t * ch_ser, dragonULInt* hostid)

    Get the dragonHostId from the channel descriptor. This is the hostid as determined by a call to the
    dragon_host_id function. This function is used internally only. It has no purpose for user-level code
    other than to retrieve debug

    **Returns**

    | :c:enumerator:`DRAGON_CHANNEL_SUCCESS`
    |     Success.
    | :c:enumerator:`DRAGON_CHANNEL_INVALID_ARGUMENT`
    |     *ch_ser* must not be ``NULL`` and be a valid descriptor.

    **Example Usage**

    .. code-block:: c

        dragonULInt hostid;

        dragonM_UID_t m_uid = 0UL;
        // create a memory pool (1 GB) we can allocate things off of
        size_t pool_bytes = 1 << 30;
        dragonMemoryError_t dmerr;
        dragonMemoryPoolDescr_t pool;

        /* create a new memory pool (existing one could have been used) */
        dragonMemoryError_t merr = dragon_memory_pool_create(&mempool, pool_bytes, "test",
                                                            m_uid, NULL);
        if (merr != DRAGON_MEMORY_SUCCESS)
            return merr;

        dragonChannelDescr_t ch;
        dragonC_UID_t c_uid = 0UL;

        /* create the channel in that memory pool */
        dragonChannelError_t derr = dragon_channel_create(&ch, c_uid, &mempool, NULL);
        if (derr != DRAGON_CHANNEL_SUCCESS)
            return derr;

        dragonChannelSerial_t ch_ser;
        derr = dragon_channel_serialize(&ch, &ch_ser);

        if (derr != DRAGON_CHANNEL_SUCCESS)
          return derr;

        derr = dragon_channel_serial_desc_get_hostid(ch_ser, &hostid);

        if (derr != DRAGON_CHANNEL_SUCCESS)
          return derr;

Channel Receive Function
^^^^^^^^^^^^^^^^^^^^^^^^

.. .. c:function:: dragonChannelRecvError_t dragon_chrecv_get_msg_with_ack(dragonChannelRecvh_t * ch_rh, dragonMessage_t * msg_recv, dragonChannelAckToken_t * token)

..     Perform a non-blocking read for the message at the front of the queue of the attached Channel.  If the memory
..     descriptor associated with *msg_recv* is ``NULL``, the library will update it with a memory descriptor where the
..     payload is.  If instead the memory descriptor associated with *msg_recv* is not ``NULL``, the library will copy
..     the payload from wherever it is into that memory (provided it is of at least the required size).

..     If a message is immediately available, msg_recv is initialized with the payload and ``DRAGON_CHANNEL_RECV_SUCCESS``
..     is returned. If no message is currently available one of two return codes is possible. If the channel is a local
..     channel then it will return ``DRAGON_CHANNEL_EMPTY`` without blocking. If the channel is a
..     remote channel it will return ``DRAGON_CHANNEL_RECV_DEFERRED`` and the acknowledgement *token* will be
..     initialized and must be used to get the message once it is available.

..     Returns ``DRAGON_CHANNEL_RECV_SUCCESS``, ``DRAGON_CHANNEL_RECV_DEFERRED``, or an error code.

..     Use the *dragon_chrecv_check_ack* function when the return code is deferrred to get access to the message.

.. c:function:: dragonChannelRecvError_t dragon_chrecv_get_msg_with_ack(dragonChannelRecvh_t * ch_rh, dragonMessage_t * msg_recv, dragonMemoryPoolDescr_t * mem_pool, dragonChannelAckToken_t * token)

    Perform a non-blocking receive for the message at the front of the queue of the attached Channel. If the
    memory descriptor associated with *msg_recv* is ``NULL``, the library will update it with a memory
    descriptor where the payload is. If instead the memory descriptor associated with *msg_recv* is not
    ``NULL``, the library will copy the payload from wherever it is into that memory (provided it is of at
    least the required size). If the *mem_pool* is not null and the *msg_recv* memory descriptor is null, then
    mem_pool* will be used to *allocate the received message.

    If a message is immediately available, msg_recv is initialized with the payload and
    ``DRAGON_CHANNEL_RECV_SUCCESS`` is returned. If no message is immediately available one of two return
    codes is possible. If the channel is a local channel then it will return
    ``DRAGON_CHANNEL_EMPTY`` without blocking. If the channel is a remote channel it will return
    ``DRAGON_CHANNEL_RECV_DEFERRED`` and the acknowledgement *token* will be initialized and must be used to
   get the message once it is available.

    Returns ``DRAGON_CHANNEL_RECV_SUCCESS``, ``DRAGON_CHANNEL_RECV_DEFERRED``,
    or an error code.

.. c:function:: dragonChannelRecvError_t dragon_chrecv_check_ack(dragonChannelAckToken_t * token, dragonMessage_t * msg_recv)

    Perform a check for the acknowledgement of the receipt of a message. It
    returns success if the message is now available. Once success is returned,
    the *msg_recv* structure will have been initialized. If the message is not
    available yet, it returns ``DRAGON_CHANNEL_RECV_DEFERRED``.

    Returns ``DRAGON_CHANNEL_RECV_SUCCESS``, ``DRAGON_CHANNEL_RECV_DEFERRED``,
    or an error code.

Channel Send Function
^^^^^^^^^^^^^^^^^^^^^

.. c:function:: dragonChannelSendError_t dragon_chsend_send_msg_with_ack(dragonChannelSendh_t * ch_sh, const dragonMessage_t * msg_send, dragonMemoryDescr_t * dest_mem_descr, dragonMemoryPoolDescr_t * mem_pool, dragonChannelAckType_t ack_type, dragonChannelAckToken_t * token)

    Send the message in *msg_send* to the attached Channel. *dest_mem_descr* can be used to specify the
    location to place the message payload. If it is ``NULL`` the payload area will be allocated from
    *mem_pool*. If *mem_pool* is null, the memory pool of the channel will be used to store the payload of the
    message. If ``DRAGON_ACK_BUFFERED`` or ``DRAGON_ACK_DEFAULT`` is specified for this call, then the
    function will return success or an error code and there will be no acknowledgement. For any other
    acknowledgement this function will return ``DRAGON_CHANNEL_SEND_IN_PROGRESS`` or an error code and the
    token will be initialized for using with the *dragon_channel_chsend_check_ack* function to determine when
    it is acknowledged.

    Valid *dragonChannelAckType_t* values are ``DRAGON_ACK_DEFAULT``,
    ``DRAGON_ACK_BUFFERED``, ``DRAGON_ACK_RECEIVED`` (Internally only
    ``DRAGON_ACK_DEPOSITED`` is also supported). The default is
    ``DRAGON_ACK_BUFFERED`` which requires no acknowledgement checking for both
    remote and local channels.

    Returns ``DRAGON_CHANNEL_SEND_SUCCESS``,
    ``DRAGON_CHANNEL_SEND_IN_PROGRESS``, or an error code.

.. c:function:: dragonChannelRecvError_t dragon_chsend_check_ack(dragonChannelAckToken_t * token)

    Perform a check for the acknowledgement requested by the user. It returns
    success if the acknowledgement has been received. If the send operation
    acknowledgement has not yet been received, it returns
    ``DRAGON_CHANNEL_SEND_IN_PROGRESS``.

    Returns ``DRAGON_CHANNEL_RECV_SUCCESS``, ``DRAGON_CHANNEL_SEND_IN_PROGRESS``,
    or an error code.


Implementation and Planning
===========================

The affected components and a short description of changes are provided here.

    * Shepherd (PE-35873) - Must create the Gateway Channels on each node. The DRAGON_NUM_GATEWAYS
       environment variable tells you how many. They must each be assigned a node specific c_uid.  The Shepherd
       must then assign one gateway c_uid/channel descriptor to each process via the DRAGON_GATEWAY_CD
       environment variable when it is started. The gateway channels must also be deallocated during teardown.
       The infrastructure pool will be used for gateway channel creation.

      The Shepherd must also start the TSTA services on a multi-node system. The bringup and teardown of the
      Shepherd must be written. Most of this work is done, but some handling of the messages in multi-node
      bringup and teardown is not finished yet and it must be finished.

      TSTA will be a monitored process much the way the Global Services is started so re-use of code for this.
      The TSTA is proposed to be a C++ program.

      This work can be done independently once any new messages sent to TSTA are defined and the sequence of
      bringup/teardown is defined.

    * Launcher/Backend (PE-35874) - Some messaging for bringup/teardown of multi-node systems needs to be
      defined and implemented. Most of this is in place, but is not complete yet.

      Concurrent to other work (including Shepherd) once new messages
      and bringup/teardown sequence is defined.

    * Cython interface for hostid (PE-35879) - Very small enhancement to provide the hostid function to Python
      code so it can be used by the Shepherd during startup.

    * Channels (PE-35877) - The channels API needs to be expanded to allow for recv_with_ACK and send_with_ACK.
      These two functions have an extra parameter for the ACK type. The recv_with_ACK should also have
      one additional parameter for the memory pool to use to allocate space for the response.

      Existing logic should be moved to these two new function. The original send and receive should call
      them with default values for ACK type and for recv the memory pool. In this way the API remains
      backwards compatible. The additional ACK logic in send and receive, as described in :numref:`remote-sr-struct` & :numref:`orig-sr-state`
      must be incorporated into the code.

      Attach needs to be enhanced for remote node attach as described above.

      The Cython interface to channels should change to call the send and receive with ACK with default
      ACK type and default pool descriptor. If None is specified for pool descriptor on receive, the
      DRAGON_DEFAULT_PD can be used.

      Testing of both C interface and Cython interface will require some work.

    * TSTA (PE-35878) - The TSTA component is proposed as a multi-threaded C++ application that interfaces between
      gateway channels and socket connections in both directions as described above. In support of this
      we need to create an internal memory pool for the internal channels to draw from.

      Internal messaging needs definition and scenarios need to be written to describe the architecture
      of the internal communication to other nodes.

      Devising a testing strategy/framework will take some time as well as the
      development of the code.