.. _GatewayChannels:

Transport Agents and Gateway Channels
+++++++++++++++++++++++++++++++++++++

Dragon Channels can only function across nodes when coupled with a :ref:`TransportAgent`, whose job it is to perform
actions on a remote Channel on behalf of a calling client process.  A :ref:`TransportAgent` interacts with the Channels
library through a Gateway Channel.  A Gateway Channel is a normal Dragon Channel that has been registered with the
Channels library for a given process for facilitating off-node communication.  When operations, such as send,
are called for a remote Channel, the Channels library will package up a :c:struct:`dragonGatewayMessageDescr_t` of the
request and write it with a message into a selected Gateway Channel.  From there it is the job of the
:ref:`TransportAgent` to complete the request on the remote node.  The operation is completed through a synchronization
operation with the requesting process.

Client and Transport Send Example
=================================

The following code shows how the Channels library and a transport agent can interact to perform a single send operation
on a remote Channel.  Handling of errors is left out in this example just to focus on the functional steps.

The first code block shows what the Channels library is internally doing during a call to
:c:func:`dragon_chsend_send_msg()`.

.. code-block:: c

    dragonULInt target_host;
    dragonError_t err = dragon_channel_get_hostid(this_ch, &target_host);

    if (target_host != dragon_host_id()) {

        // get a serialized descriptor of the target (current) Channel
        dragonChannelSerial_t this_ch_ser;
        dragonError_t err = dragon_channel_serialize(this_ch, &this_ch_ser);

        // in this example there is no landing pad specified
        dragonGatewayMessage_t gmsg;
        err = dragon_channel_gatewaymessage_send_create(send_msg, NULL, &this_ch_ser, &this_ch_sh->_attrs,
                                                        NULL, &gmsg);

        /* What this call does: a gmsg is really just another Dragon object available through a memory descriptor.
           This call just produces a serialized memory descriptor, but we'll keep it abstract in case we want
           to embed anything else.  Done this way, there is minimal copying going on between a client and the
           transport (just this descriptor).  We can then keep Gateway Channels deep with small message blocks
        */
        dragonGatewayMessageSerial_t gmsg_ser;
        err = dragon_channel_gatewaymessage_serialize(&gmsg, &gmsg_ser);

        dragonMemoryDescr_t payload;
        void * pptr;
        err = dragon_memory_alloc(&payload, &pool, gmsg_ser.len);
        err = dragon_memory_get_pointer(payload, &pptr);
        memcpy(pptr, gmsg_ser.data, gmsg_ser.len);

        err = dragon_channel_gatewaymessage_serial_free(gmsg_ser);

        dragonMessage_t msg_for_transport;
        err = dragon_channel_message_init(&msg_for_transport, &payload, NULL);

        gw_ch_sh = _internal_select_gateway();
        err = dragon_chsend_send_msg(gw_ch_sh, &msg_for_transport, DRAGON_CHANNEL_SEND_TRANSFER_OWNERSHIP, 0);
        err = dragon_channel_message_destroy(&msg_for_transport, false);

        /* What this call does: for return immediately and buffered sends, this operation detaches from everything and
           then increments an atomic indicating it is done
           For other send return modes this will wait on the BCast object, grab the error code in the payload,
           detach from everything and increment the atomic that it is done
        */
        err = dragon_channel_gatewaymessage_client_send_complete(&gmsg);

    } else { on-node transfer }


The code for the transport agent is as follows:

.. code-block:: c

    dragonMessage_t transport_msg;
    dragonError_t err = dragon_chrecv_get_msg_blocking(monitored_gw_ch_rh, &msg_for_transport,
                                                       DRAGON_CHANNEL_BLOCKING_NOTIMEOUT);

    dragonMemoryDescr_t payload;
    void * pptr;
    size_t ser_bytes;
    err = dragon_channel_message_get_mem(&msg_for_transport, &payload);
    err = dragon_memory_get_pointer(payload, &pptr);
    err = dragon_memory_get_size(payload, &ser_bytes);

    dragonGatewayMessageSerial_t gmsg_ser;
    gmsg_ser.len = ser_bytes;
    gmsg_ser.data = pptr;

    dragonGatewayMessage_t gmsg;
    dragon_channel_gatewaymessage_attach(&gmsg_ser, &gmsg);

    if (gmsg.msg_kind == DRAGON_GATEWAY_MESSAGE_SEND) {

        /* the target node is in: gmsg.target_hostid
           the deadline is in: gmsg.deadline
           return mode is found in: gmsg.send_return_mode
           the serialized target channel descriptor: gmsg.target_ch_ser
        */

        /* What this call does: for return immediately and buffered return modes, this call waits on an atomic
           indicating the client has detached and then destroys everything
           For other return modes, this call waits until the client is a waiter on the BCast, triggers the
           BCast with a payload of the error code, waits on an atomic until the client has detached, and the destroys
           everything
        */
        if (s_gmsg->send_attrs.return_mode == DRAGON_CHANNEL_SEND_RETURN_IMMEDIATELY ||
            s_gmsg->send_attrs.return_mode == DRAGON_CHANNEL_SEND_RETURN_WHEN_BUFFERED)
            err = dragon_channel_gatewaymessage_transport_send_complete(&gmsg, op_err);

        /* Perform work to complete send through to writing the message into the target channel
           client payload message is in: gmsg.send_payload_message
           if applicable, destination serialized memory descriptor is in: gmsg.send_dest_mem_descr_ser
               Note, gmsg.send_dest_mem_descr_ser is a pointer that may be NULL if N/A or it may be
               DRAGON_CHANNEL_SEND_TRANSFER_OWNERSHIP.  Otherwise it points to a valid serialized
               memory descriptor indicating on the remote end where to write the data into.
        */

        /* For transfer of ownership, transport should destroy the client message and release the backing memory
           Anything other than transfer of ownership transport just needs to destroy the message
        */
        if (gmsg.send_dest_mem_descr_ser == DRAGON_CHANNEL_SEND_TRANSFER_OWNERSHIP) {
            err = dragon_channel_message_destroy(&gmsg.send_payload_message, true);
        } else {
            err = dragon_channel_message_destroy(&gmsg.send_payload_message, false);
        }

        if (s_gmsg->send_attrs.return_mode == DRAGON_CHANNEL_SEND_RETURN_WHEN_DEPOSITED)
            err = dragon_channel_gatewaymessage_transport_send_complete(&gmsg, op_err);

        // a return mode of DRAGON_CHANNEL_SEND_RETURN_WHEN_RECEIVED is not yet implemented into Channels


    } else { get or event handling }

    // unconditionally destroy the message from the gateway channel and release the memory backing it
    err = dragon_channel_message_destroy(&msg_for_transport, true);

.. TBD - This moves to the external API reference eventually.
.. .. _GatewayChannelsAPI:

.. Client API
.. ==========
.. This section documents the user-level C API.

.. Structures
.. ----------

.. .. doxygenenum:: dragonGatewayMessageKind_t

.. .. doxygenstruct:: dragonGatewayMessage_t
..     :members:

.. Functions
.. ---------

.. .. c:function:: dragonError_t dragon_channel_register_gateway(dragonChannelDescr_t * ch)

..     **NOT YET IMPLEMENTED**

..     Register the Channel *ch* as a gateway channel. Channels that cannot be serviced locally by the calling
..     process, such as Channels that reside in off-node memory, will interact with any of the available gateway
..     Channels registered with this function. *ch* must be addressable by the calling process to be a valid
..     gateway.

..     Implementers need to provide a service process that manages messages placed into the gateway Channels for
..     off-node interactions.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. doxygenfunction:: dragon_channel_gatewaymessage_send_create

.. .. doxygenfunction:: dragon_channel_gatewaymessage_get_create

.. .. doxygenfunction:: dragon_channel_gatewaymessage_event_create

.. .. doxygenfunction:: dragon_channel_gatewaymessage_destroy

.. .. doxygenfunction:: dragon_channel_gatewaymessage_serialize

.. .. doxygenfunction:: dragon_channel_gatewaymessage_serial_free

.. .. doxygenfunction:: dragon_channel_gatewaymessage_attach

.. .. doxygenfunction:: dragon_channel_gatewaymessage_detach

.. .. doxygenfunction:: dragon_channel_gatewaymessage_transport_send_cmplt

.. .. doxygenfunction:: dragon_channel_gatewaymessage_client_send_cmplt

.. .. doxygenfunction:: dragon_channel_gatewaymessage_transport_get_cmplt

.. .. doxygenfunction:: dragon_channel_gatewaymessage_client_get_cmplt

.. .. doxygenfunction:: dragon_channel_gatewaymessage_transport_event_cmplt

.. .. doxygenfunction:: dragon_channel_gatewaymessage_client_event_cmplt
