.. _Channels:

Channels
++++++++

Dragon Channels is the low-level transport layer for communicating messages between POSIX
processes in the Dragon runtime. They are effectively a thread-safe priority queue for
message blocks of fixed size or, via side-loading of additional :ref:`ManagedMemory` blocks,
arbitrary sized data. Infrastructure :ref:`Services` use the :ref:`Messages` to communicate
with each other.

Channels combined with the :ref:`TransportAgent` provide flexible on-node and off-node
communication that processes use by attaching and detaching  to the underlying :ref:`ManagedMemory`.
In this respect they resemble POSIX sockets as they are always available as a service and not
built upon the static distributed model that MPI or SHMEM is.

A major advantage of Channels is that they retain the flexibility of using
sockets or a filesystem while enabling zero-copy on-node transfers, single-copy
RDMA-enabled transfers off-node, and choices for blocking semantics. There is a
rich set of buffer management options that enable use cases such as:

    - static target-side managed payload landing pads much like SHMEM or
      one-sided MPI

    - dynamic target-side managed payload landing pads much like two-sided MPI

    - static or dynamic origin-side managed payload landing pad, which nothing
      else has

Dragon Channels can be mapped into any valid :c:struct:`dragonMemoryDescr_t` as
provided by Dragon :ref:`ManagedMemory`. This includes shared memory, a filesystem, or
private virtual memory. Payload buffers for messages can reside within the
Channel, the memory pool :c:struct:`dragonMemoryPoolDescr_t` the Channel was
allocated from, or any valid :c:struct:`dragonMemoryDescr_t` passed with a
message. This design allows Channels to provide multiple usage scenarios with
different performance and persistence characteristics. Because Channels are
mapped into memory, including all state information and thread locks, they can
be serialized and relocated just like files. They are also inherently
multi-thread and multi-process safe to operate on.

Architecture
============

.. figure:: images/ChannelArchitecture.svg
    :name: channel-architecture 

    **Dragon channel architecture as component diagram.**

A channel uses the following :ref:`Components`:

* Ordering Table (OT): A :ref:`PriorityHeap` to guarantee ordering of messages from a process.
* Usage Table (UT): A :ref:`PriorityHeap` to lock and free  message blocks.
* :ref:`Locks`: To protect the ordering table and usage table for thread-safety.
* :ref:`ManagedMemory`: To allow shared access from POSIX processes.

The ordering table (OT) and usage table (UT), which are part of the internal structure of a :ref:`Channels`,
are implemented with a :ref:`PriorityHeap`.  The OT guarantees ordering of messages from a process.  Even though
messages might be received out of order, through a gateway process like the :ref:`TransportAgent`, ordering in reconstructed
through a process-generated sequence number and the heap used to implement the OT.  The OT can also
support out-of-order priority messaging through the priority insert operation for the heap.

The UT also uses the heap to track which message blocks are currently free.  Initially all blocks are
items within the heap used to implement the OT.  Blocks are fetched off of the head of the heap and only
added back once the block is free again.  The argument for this implementation is that Channels are most
likely used by more than a single process.  The natural rotation of message block usage one gets from using
a heap is less likely to cause significant cache thrashing if instead early blocks were always favored
across all processes.  This is less optimal in the case of one sending process and one receiving process
adding to and removing from the Channel at equal rates.  If that is indeed the behavior of the Channel,
the caller should request a Channel of depth 1 or only a few.

**FIXME: What about Gateway channels ?**

.. figure:: images/ChannelAnatomy.png
    :scale: 60 %
    :name: channel-anatomy 

    **Anatomy of a Channel data structure with a capacity of six in a contiguous blob of memory.**

Above figure shows the anatomy of a channels in :ref:`ManagedMemory`. The payload messages of arbitrary constant size are
contained in the "Message Blocks". For infrastructure channels, these blocks transport the :ref:`Messages` between
:ref:`Services`. User programs will define their own message api. Varying data sizes are supported via side-loading.

**FIXME: Here would be a good place to describe what is actually happening, when a message is sent or received, i.e. how the queues and locks are used.
This may well include as sequence or state diagram**

Message sending
---------------


Message receiving
-----------------

Notes and TODO
==============

Collective Channels that require operations, such as reduce, need knowledge of
type and op. These should be bound to the Channel, which requires API or attr
support.

Adapters
========

For the Python/Cython layer, refer to:

.. toctree::
    :glob:
    :maxdepth: 1

    cy_channels.rst

.. TBD This moves to the external API reference in the future.

.. .. _MessagesAPI:

.. Messages API
.. ============

.. A major difference between Channels and other message passing APIs is that Channels will not operate on bare
.. pointers.  All memory must be described through memory descriptors, c:struct:`dragonMemoryDescr_t`.  This
.. enables the library to make informed decisions about buffering and optimizations that otherwise have to be
.. conservatively assumed.  For example, it is known if the memory is from shared memory with specific access
.. rules.  We also have the freedom to add in other rules and behaviors not possible with bare pointers.

.. The :ref:`Messages` API is used to package up messages for sending or unrolling messages once received off of
.. a :ref:`Channels`.

.. Types and Structures
.. --------------------

.. .. doxygenstruct:: dragonMessageAttr_t
..     :members:

.. .. doxygenstruct:: dragonMessage_t

.. Functions
.. ---------

.. .. doxygenfunction:: dragon_channel_message_init

.. .. doxygenfunction:: dragon_channel_message_destroy

.. .. doxygenfunction:: dragon_channel_message_get_mem

.. .. doxygenfunction:: dragon_channel_message_getattr

.. Message Buffering Notes
.. -----------------------

.. On the sending side, there are two options for how buffering might take place.  The selection between them is
.. accessed through the :c:func:`dragon_chsend_send_msg()` function. The message can go through whatever
.. automatic buffering is required by the library, such as copy through a message block or intermediate side
.. buffer, or the sender can specify the exact landing pad for the message. For example,

..     .. code-block:: c

..         dragonChannelSendh_t ch_sh;
..         dragonMessage_t msg;

..         //...

..         /* allow the library to do automatic buffering */
..         dragon_chsend_send_msg(&ch_sh, &msg, NULL);

..     .. code-block:: c

..         dragonChannelSendh_t ch_sh;
..         dragonMessage_t msg;

..         /* instead tell the library to place the message in a specific (local or non-local) memory descriptor */
..         dragonMemoryDescr_t mem_descr;

..         //...

..         dragon_chsend_send_msg(&ch_sh, &msg, &mem_descr);

.. In an on-node scenario, it is permissible to have the destination memory descriptor be the same as the memory
.. descriptor in the message being sent. What this indicates to the library is a transfer-of-ownership from the
.. sender to whatever receiving process. No copies take place in this case.

.. The receiving side also has two options for where the payload ends up. An empty message container can be
.. provided, in which case the library will update the message container with whatever memory descriptor it deems
.. as optimal for the location of the payload. Alternatively, the receiver can specify a memory descriptor in the
.. message, which tells the library no matter where the payload is now, put it into the one in the provided
.. message.

..     .. code-block:: c

..         dragonChannelRecvh_t ch_rh;
..         dragonMessage_t msg_recv;
..         dragonMemoryDescr_t mem;

..         //... do not allocate memory descriptor ...

..         /* allow channels to update the message with whatever memory descriptor it thinks is optimal */
..         dragon_channel_message_init(&msg_recv, NULL, NULL);
..         dragon_chrecv_get_msg(&ch_rh, &msg_recv);
..         dragon_channel_message_get_mem(&msg_recv, &mem_descr);

..     .. code-block:: c

..         dragonChannelRecvh_t ch_rh;
..         dragonMessage_t msg_recv;
..         dragonMemoryDescr_t mem;

..         //... allocate memory descriptor with enough space for whatever message we might get ...

..         /* tell channels to place the payload into memory descriptor mem */
..         dragon_channel_message_init(&msg_recv, &mem, NULL);
..         dragon_chrecv_get_msg(&ch_rh, &msg_recv);

.. All combinations between the send and receive side are allowed. Small messages themselves may be buffered
.. through message blocks, but the caller does not need access or knowledge to that occurring.

.. Gateway Channels and Gateway Messages
.. -------------------------------------

.. .. toctree::
..     :glob:
..     :maxdepth: 1

..     gateway_channels.rst

.. Performance Testing
.. -------------------

.. .. toctree::
..     :glob:
..     :maxdepth: 1

..     channels_performance.rst


.. Channels API
.. ------------

.. Structures
.. ^^^^^^^^^^

.. .. c:enum:: dragonChannelOFlag_t

..     .. c:enumerator:: DRAGON_CHANNEL_EXCLUSIVE

..         The Channel is to be created exclusively. If the Channel already exists an error will be returned by
..         *dragon_channel_create()*. This is the default option.

..     .. c:enumerator:: DRAGON_CHANNEL_NONEXCLUSIVE

..         If the Channel to be created already exists, the call to
..         *dragon_channel_create()* will succeed.

.. .. c:enum:: dragonChannelFC_t

..     .. c:enumerator:: DRAGON_CHANNEL_FC_NONE

..         Disable all flow control on the Channel.

..     .. c:enumerator:: DRAGON_CHANNEL_FC_RESOURCES

..         Enable flow control based on fairness across all resource metrics. This
..         is the default for a Channel.

..     .. c:enumerator:: DRAGON_CHANNEL_FC_MEMORY

..         Enable flow control based on memory consumption (message blocks and side
..         buffers).

..     .. c:enumerator:: DRAGON_CHANNEL_FC_MSGS

..         Enable flow control based on the number of messages in the Channel.

.. .. c:struct:: dragonChannelAttr_t

..     .. c:var:: dragonLockKind_t lock_type

..         The type of lock to use for management (e.g., ordering table lock) from
..         the Channel.

..     .. c:var:: dragonChannelOFlag oflag

..         The exclusive mode to use for creating a Channel. The default value is
..         ``DRAGON_CHANNEL_EXCLUSIVE``.

..     .. c:var:: size_t bytes_per_msg_block

..         The number of bytes each message block should be able to hold in payload. The actual size will be
..         greater than this to account for header data. The default value is 1024 bytes.

..     .. c:var:: size_t capacity

..         The maximum number of messages that can be in the Channel before sending of messages will block or
..         error out due to lack of resources. The default value is 100.

..     .. c:var:: dragonChannelFC_t fc_type

..         The type of flow control to be used in the Channel.

..     .. c:var:: dragonMemoryPoolDescr_t * buffer_pool

..         Use the specified memory pool for allocating side payload buffer for messages if needed. If set to
..         ``NULL`` then message side payload buffers will be allocated from the same memory pool the Channel was
..         allocated from. Note that messages can optionally encode their own side payload buffer management and
..         not use the pool given here.

..         *More to come*

.. .. c:struct dragonChannelState_t

..     .. c:var:: size_t current_depth

..         The number of messages in the Channel at the moment of inspection.  This can become quickly out of
..         date once returned to the caller unless read and write locks are set on the Channel before checking
..         the Channel state.

..     .. c:var:: size_t memory_usage

..         The amount of memory the Channel consumes, including side buffers, at the moment of inspection.  This
..         can become quickly out of date once returned to the caller unless read and write locks are set on the
..         Channel before checking the Channel state.

..     .. c:var:: size_t n_processes

..         The number of processes with messages in the Channel.  This can become quickly out of date once
..         returned to the caller unless read and write locks are set on the Channel before checking the Channel
..         state.

..     .. c:var:: dragonP_UID * processes

..         A list of size *n_processes* of the :c:var:`dragonP_UID` values of the processes with messages
..         currently in the Channel.  This can become quickly out of date once returned to the caller unless read
..         and write locks are set on the Channel before checking the Channel state.

..         *More to come*

.. .. c:struct:: dragonChannelDescr_t

..     Opaque structure that describes a Channel.

.. .. c:struct:: dragonChannelRequest_t

..     .. c:var:: size_t len

..         The number of bytes in the *data* member.

..     .. c:var:: uint8_t * data

..         The request payload.

.. .. c:struct:: dragonChannelResponse_t

..     .. c:var:: size_t len

..         The number of bytes in the *data* member.

..     .. c:var:: uint8_t * data

..         The response payload.

.. .. c:struct:: dragonChannelSerial_t

..     .. c:var:: size_t len

..         The number of bytes in the *data* member.

..     .. c:var:: uint8_t * data

..         The payload of the serialized Channel descriptor.

.. .. c:enum:: dragonSendBlocking_t

..     .. c:enumerator:: DRAGON_CHANNEL_SEND_BLOCKING_NONE

..         Do not block on a send operation. The caller is not free to reuse the original message buffer upon
..         return from a send. Some other communication will be required for the caller to know the original
..         message buffer can now be reused, such as another send operation on the same ordered Channel with the
..         blocking set to ``DRAGON_CHANNEL_SEND_BLOCKING_DEPOSITED`` or ``DRAGON_CHANNEL_SEND_BLOCKING_READ``.

..     .. c:enumerator:: DRAGON_CHANNEL_SEND_BLOCKING_BUFFERED

..         Block on a send operation until the message has been buffered and the caller can reuse the original
..         message buffer.

..     .. c:enumerator:: DRAGON_CHANNEL_SEND_BLOCKING_DEPOSITED

..         Block on a send operation until the message has been deposited into the target Channel.  The caller
..         can reuse the original message buffer.

..     .. c:enumerator:: DRAGON_CHANNEL_SEND_BLOCKING_READ

..         Block on a send operation until some process has read the message from the Channel.  The caller can
..         reuse the original message buffer.

.. .. c:struct:: dragonChannelSendAttrs_t

..     .. c:var:: dragonSendBlocking_t block_mode

..         The blocking mode to use.  Defaults to ``DRAGON_CHANNEL_SEND_BLOCKING_BUFFERED``.

.. .. c:struct:: dragonChannelRecvAttrs_t

..     .. c:var:: dragonChannelRecvNotif_t notif_type

..         Placeholder here.  We'll want to use this so a Channel can be embedded with information to notify a
..         receiver that a message is in the Channel with a signal.

.. .. c:struct:: dragonChannelSendh_t

..     Opaque structure referring to a send handle for a Channel.

.. .. c:struct:: dragonChannelRecvh_t

..     Opaque structure referring to a receive handle for a Channel.

.. .. c:struct:: dragonMessageAttrs_t

..     Members to be defined yet.

.. .. c:struct:: dragonMessage_t

..     Opaque structure referring to a Channel message.

.. Channel Functions
.. ^^^^^^^^^^^^^^^^^

.. .. doxygenfunction:: dragon_chsend_send_msg

.. **Example Usage**

.. .. code-block:: c

..     dragonMemoryPoolDescr_t mempool;

..     dragonM_UID_t m_uid = 0UL;
..     size_t pool_bytes = 1<<30;

..     /* create a new memory pool (existing one could have been used) */
..     dragonError_t merr = dragon_memory_pool_create(&mempool, pool_bytes, "test", m_uid, NULL);
..     if (merr != DRAGON_SUCCESS)
..         return ERR;

..     dragonChannelDescr_t ch;
..     dragonC_UID_t c_uid = 0UL;

..     /* create the channel in that memory pool */
..     dragonError_t derr = dragon_channel_create(&ch, c_uid, &mempool, NULL);
..     if (derr != DRAGON_SUCCESS)
..         return ERR;

..     /* destroy the channel */
..     dragonError_t derr = dragon_channel_destroy(&ch);
..     if (derr != DRAGON_SUCCESS)
..         return ERR;


.. Local Channel Life-cycle Functions
.. """"""""""""""""""""""""""""""""""

.. .. c:function:: dragonError_t dragon_channel_create(dragonChannelDescr_t * ch, const dragonC_UID_t c_uid, dragonMemoryPoolDescr_t * pool_descr, const dragonChannelAttr_t * attr)

..     Creates a standard Channel.

..     The Channel structure handle *ch* will be updated for use in all other actions with the Channel. *c_uid*
..     is the unique key (e.g., provided by Global Services or generated locally by some means) for the Channel.
..     *pool_descr* is the descriptor of the memory pool the Channel will allocated
..     from and encoded into. *attr* can be passed as ``NULL`` and ignored, or it can be used to tune the
..     Channel.

..     Depending on the value *oflag* in *attr*, this call may fail or succeed if
..     the Channel already exists.

..     **Returns**

..     | :c:enumerator:`DRAGON_SUCCESS`
..     |     Success.
..     | :c:enumerator:`DRAGON_CHANNEL_INVALID_ARGUMENT`
..     |     *ch* and *pool_descr* must not be ``NULL``.
..     | :c:enumerator:`DRAGON_MEMORY_OPERATION_ATTEMPT_ON_NONLOCAL_POOL`
..     |     *pool_descr* is for a memory pool that cannot be locally accessed.
..     | :c:enumerator:`DRAGON_INTERNAL_MALLOC_FAIL`
..     |     An internal allocation failed.
..     | :c:enumerator:`DRAGON_CHANNEL_MEMORY_POOL_ERROR`
..     |     A managed memory pool error occurred.
..     | :c:enumerator:`DRAGON_CHANNEL_UMAP_ERROR`
..     |     A unordered map error occurred.
..     | :c:enumerator:`DRAGON_CHANNEL_PRIORITY_HEAP_ERROR`
..     |     A priority heap error occurred.
..     | :c:enumerator:`DRAGON_CHANNEL_LOCK_ERROR`
..     |     A scalable shared lock error occurred.

..     If the provided *pool_descr* describes memory on another node from the caller this function will fail. The
..     caller must instead use the request-based interface (ie, :c:func:`dragon_channel_create_genreq()`) to
..     create a request that can be sent to a process on the node *pool_descr* is from.

..     **Example Usage**

..     .. code-block:: c

..         dragonMemoryPoolDescr_t mempool;

..         dragonM_UID_t m_uid = 0UL;
..         size_t pool_bytes = 1<<30;

..         /* create a new memory pool (existing one could have been used) */
..         dragonError_t merr = dragon_memory_pool_create(&mempool, pool_bytes, "test",
..                                                             m_uid, NULL);
..         if (merr != DRAGON_SUCCESS)
..             return ERR;

..         dragonChannelDescr_t ch;
..         dragonC_UID_t c_uid = 0UL;

..         /* create the channel in that memory pool */
..         dragonError_t derr = dragon_channel_create(&ch, c_uid, &mempool, NULL);
..         if (derr != DRAGON_SUCCESS)
..             return ERR;

..         /* destroy the channel */
..         dragonError_t derr = dragon_channel_destroy(&ch);
..         if (derr != DRAGON_SUCCESS)
..             return ERR;

.. .. c:function:: dragonError_t dragon_channel_destroy(dragonChannelDescr_t * ch)

..     Destroy the Channel and release resources it consumes. This operation will lock the Channel for reads and
..     writes and then release all Channel resources. Messages may still be in the Channel and will be silently
..     lost.  Any other processes attached to this Channel will receive errors from any operation using the
..     associated descriptor.

..     **Returns**

..     | :c:enumerator:`DRAGON_SUCCESS`
..     |     Success.
..     | :c:enumerator:`DRAGON_CHANNEL_INVALID_ARGUMENT`
..     |     *ch* must not be ``NULL``.  *ch* must describe a valid channel.
..     | :c:enumerator:`DRAGON_CHANNEL_MEMORY_POOL_ERROR`
..     |     A managed memory pool error occurred.
..     | :c:enumerator:`DRAGON_CHANNEL_UMAP_ERROR`
..     |     A unordered map error occurred.

..     **Example Usage**

..     .. code-block:: c

..         dragonMemoryPoolDescr_t mempool;

..         dragonM_UID_t m_uid = 0UL;
..         size_t pool_bytes = 1<<30;

..         /* create a new memory pool (existing one could have been used) */
..         dragonError_t merr = dragon_memory_pool_create(&mempool, pool_bytes, "test", m_uid, NULL);
..         if (merr != DRAGON_SUCCESS)
..             return ERR;

..         dragonChannelDescr_t ch;
..         dragonC_UID_t c_uid = 0UL;

..         /* create the channel in that memory pool */
..         dragonError_t derr = dragon_channel_create(&ch, c_uid, &mempool, NULL);
..         if (derr != DRAGON_SUCCESS)
..             return ERR;

..         /* destroy the channel */
..         dragonError_t derr = dragon_channel_destroy(&ch);
..         if (derr != DRAGON_SUCCESS)
..             return ERR;

.. Non-Local Channel Life-cycle Functions
.. """"""""""""""""""""""""""""""""""""""

.. .. c:function:: dragonError_t dragon_channel_create_gen_req(dragonChannelRequest_t * chreq, dragonC_UID_t c_uid, const dragonMemoryPoolDescr_t * pool_descr, const dragonChannelAttr_t * attr)

..     **NOT YET IMPLEMENTED**

..     Update the :c:struct:`dragonChannelRequest_t` request structure *chreq* for creating a new Channel. The
..     *data* member of *chreq* is updated to point to an allocated :c:type:`uint8_t` blob with the request
..     message encoded and the
..     *len* member set to the number of bytes in the *data* blob upon return. The
..     caller can communicate the *data* member of *chreq* with another process that can service the request.
..     That process must be able to directly access the memory described in the Pool descriptor *pool_descr*.

..     See :c:func:`dragon_channel_create()` for details on the other arguments.

..     The request/response-based interface is intended for situations where Channel operation requests must be
..     fulfilled by another process because it has direct access to the memory described in *pool_descr* and the
..     requesting process does not. In the request/response-based interface, the process calling
..     :c:func:`dragon_channel_exec_req()` has direct access to the memory pool.

..     Returns ``DRAGON_SUCCESS`` or an error code.

..     A call to this function is exactly equivalent to the request/response-based
..     interface if all executed in the same process. For example:

..     .. code-block:: c

..         dragonChannelRequest_t chreq;
..         dragonMemoryPoolDescr_t mempool;
..         dragonError_t derr;

..         derr = dragon_channel_create_gen_req(&chreq, c_uid, &mempool, NULL);
..         if (derr != DRAGON_SUCCESS)
..             return ERR;

..         dragonChannelResponse chresp;
..         derr = dragon_channel_exec_req(&chresp, &chreq);
..         if (derr != DRAGON_SUCCESS)
..             return ERR;

..         dragonChannelDescr_t ch;
..         derr = dragon_channel_create_cmplt(&ch, &chresp);
..         if (derr != DRAGON_SUCCESS)
..             return ERR;

.. .. c:function:: dragonError_t dragon_channel_create_cmplt(dragonChannelDescr_t * ch, const dragonChannelResponse_t * chresp)

..     **NOT YET IMPLEMENTED**

..     Complete creation of a Channel processed through a request. *chresp* is the result from a call to
..     :c:func:`dragon_channel_exec_req()` (possibly on a remote process) with the
..     :c:struct:`dragonChannelRequest_t` *chreq* argument coming from a call to
..     :c:func:`dragon_channel_create_gen_req()`. *ch* is updated for use in all other actions with the Channel
..     upon success. If the call to :c:func:`dragon_channel_exec_req()` failed this call will return an error. An
..     error will occur if *chresp* originated from a request other than creating a Channel.

..     The caller is responsible for freeing memory associated with any
..     :c:struct:`dragonChannelRequest_t` or :c:struct:`dragonChannelResponse_t`
..     variables with :c:func:`dragon_channel_free_req()` or
..     :c:func:`dragon_channel_free_resp()`.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonError_t dragon_channel_destroy_gen_req(dragonChannelRequest_t * chreq, dragonChannelDescr_t * ch)

..     **NOT YET IMPLEMENTED**

..     Update the :c:struct:`dragonChannelRequest_t` request structure *chreq* for destroying an existing
..     Channel. The *data* member of *chreq* is updated to point to an allocated :c:type:`uint8_t` blob with the
..     request message encoded and the *len* member set to the number of bytes in the *data* blob upon return.
..     The caller can communicate the *data* member of *chreq* with another process that can service the request.
..     That process must be able to directly access the memory described in the Pool descriptor argument,
..     *pool_descr*, given to the Channel creation function for the Channel.

..     The request/response-based interface is intended for situations where Channel operation requests must be
..     fulfilled by another process because it has direct access to the memory described in *pool_descr* and the
..     requesting process does not. In the request/response-based interface, the process calling
..     :c:func:`dragon_channel_exec_req()` has direct access to the memory pool.

..     Returns ``DRAGON_SUCCESS`` or an error code.

..     A call to this function is exactly equivalent to the request/response-based
..     interface if all executed in the same process. For example:

..     .. code-block:: c

..         dragonChannelRequest_t chreq;
..         dragonError_t derr;

..         derr = dragon_channel_destroy_gen_req(&chreq, &ch);
..         if (derr != DRAGON_SUCCESS)
..             return ERR;

..         dragonChannelResponse chresp;
..         derr = dragon_channel_exec_req(&chresp, &chreq);
..         if (derr != DRAGON_SUCCESS)
..             return ERR;

..         dragonChannelDescr_t ch;
..         derr = dragon_channel_destroy_cmplt(&chresp);
..         if (derr != DRAGON_SUCCESS)
..             return ERR;

.. .. c:function:: dragonError_t dragon_channel_destroy_cmplt(const dragonChannelResponse_t * chresp)

..     **NOT YET IMPLEMENTED**

..     Complete destruction of a Channel processed through a request.  *chresp* is the result from a call to
..     :c:func:`dragon_channel_exec_req()` (possibly on a remote process) with the :c:struct:`dragonChannelRequest_t`
..     *chreq* argument coming from a call to :c:func:`dragon_channel_destroy_gen_req()`.  If the call to
..     :c:func:`dragon_channel_exec_req()` failed this call will return an error.  An error will occur if *chresp*
..     originated from a request other than destroying a Channel.

..     The caller is responsible for freeing memory associated with any :c:struct:`dragonChannelRequest_t` or
..     :c:struct:`dragonChannelResponse_t` variables with :c:func:`dragon_channel_free_req()` or
..     :c:func:`dragon_channel_free_resp()`.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. Functions Using an Existing Channel
.. """""""""""""""""""""""""""""""""""

.. .. c:function:: dragonError_t dragon_channel_get_pool(const dragonChannelDescr_t * ch, dragonMemoryPoolDescr_t * pool_descr)

..     Update the memory pool descriptor *pool_descr* to be a valid handle to the memory pool the channel
..     described by *ch* was allocated from. An example use case for this is a situation of attaching to a
..     channel and then needing to create messages to send to it.

..     **Returns**

..     | :c:enumerator:`DRAGON_SUCCESS`
..     |     Success.
..     | :c:enumerator:`DRAGON_CHANNEL_INVALID_ARGUMENT`
..     |     *ch* and *pool_descr* must not be ``NULL``.  *ch* must describe a valid channel.
..     | :c:enumerator:`DRAGON_CHANNEL_MEMORY_POOL_ERROR`
..     |     A managed memory pool error occurred.

..     **Example Usage**

..     .. code-block:: c

..         dragonChannelDescr_t ch;
..         dragonChannelSerial_t ch_ser;

..         /* read a serialized descriptor from a file that another process wrote.
..            Any other means of communicating this could be used (e.g., sockets,
..            pipes, other channel)*/
..         struct stat sb;
..         stat("ch_serial.dat", &sb);

..         dragonChannelSerial_t ch_ser;
..         ch_ser.len = sb.st_size;
..         ch_ser.data = malloc(ch_ser.len);

..         FILE * fp = fopen("ch_serial.dat", "r+");
..         size_t readb = fread(ch_ser.data, 1, ch_ser.len, fp);

..         dragonError_t derr = dragon_channel_attach(&ch_ser, &ch);
..         if (derr != DRAGON_SUCCESS)
..             return ERR;

..         dragonMemoryPoolDescr_t mempool;
..         derr = dragon_channel_get_pool(&ch, &mempool);

..         dragonMemoryDescr_t mem;
..         size_t mem_bytes = 1024;
..         dragonError_t merr = dragon_memory_alloc(&mem, &mempool, mem_bytes);
..         if (merr != DRAGON_SUCCESS)
..             return ERR;

.. .. c:function:: dragonError_t dragon_channel_serialize(const dragonChannelDescr_t * ch, dragonChannelSerial_t * ch_ser)

..     Serialize the Channel descriptor *ch* into *ch_ser*, which can then be communicated with another process.
..     :c:func:`dragon_channel_serial_free()` must be used to clean up the components of *ch_ser* once it is no
..     longer needed.

..     **Returns**

..     | :c:enumerator:`DRAGON_SUCCESS`
..     |     Success.
..     | :c:enumerator:`DRAGON_CHANNEL_INVALID_ARGUMENT`
..     |     *ch* and *ch_ser* must not be ``NULL``.  *ch* must describe a valid channel.
..     | :c:enumerator:`DRAGON_CHANNEL_MEMORY_POOL_ERROR`
..     |     A managed memory pool error occurred.
..     | :c:enumerator:`DRAGON_INTERNAL_MALLOC_FAIL`
..     |     An internal memory allocation failed.

..     **Example Usage**

..     .. code-block:: c

..         dragonMemoryPoolDescr_t mempool;

..         dragonM_UID_t m_uid = 0UL;
..         size_t pool_bytes = 1<<30;

..         /* create a new memory pool (existing one could have been used) */
..         dragonError_t merr = dragon_memory_pool_create(&mempool, pool_bytes, "test",
..                                                             m_uid, NULL);
..         if (merr != DRAGON_SUCCESS)
..             return ERR;

..         dragonChannelDescr_t ch;
..         dragonC_UID_t c_uid = 0UL;

..         /* create the channel in that memory pool */
..         dragonError_t derr = dragon_channel_create(&ch, c_uid, &mempool, NULL);
..         if (derr != DRAGON_SUCCESS)
..             return ERR;

..         dragonChannelSerial_t ch_ser;
..         derr = dragon_channel_serialize(&ch, &ch_ser);

..         /* write the serialized channel to a file */
..         FILE * fp = fopen("ch_serial.dat", "w");
..         fwrite(ch_ser.data, 1, ch_ser.len, fp);
..         fclose(fp);

.. .. c:function:: dragonError_t dragon_channel_serial_free(dragonChannelSerial_t * ch_ser)

..     Clean up the components of *ch_ser*.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonError_t dragon_channel_attach(const dragonChannelSerial_t * ch_ser, dragonChannelDescr_t * ch)

..     Update the Channel descriptor *ch* to be a valid descriptor for the channel described by *ch_ser*. If the
..     channel no longer exists an error will be returned. Repeated calls to :c:func:`dragon_channel_attach()`
..     for the same channel is valid, but the library treats all descriptors to the same channel as if they were
..     pointers to the same memory. If :c:func:`dragon_channel_destroy()` is called with one of the descriptors,
..     operations with all others for that same channel will return an error.

..     **Returns**

..     | :c:enumerator:`DRAGON_SUCCESS`
..     |     Success.
..     | :c:enumerator:`DRAGON_CHANNEL_INVALID_ARGUMENT`
..     |     *ch* and *ch_ser* must not be ``NULL``.  *ch_ser.data* must be valid serialized descriptor data.
..     | :c:enumerator:`DRAGON_CHANNEL_MEMORY_ERROR`
..     |     A managed memory error occurred.
..     | :c:enumerator:`DRAGON_INTERNAL_MALLOC_FAIL`
..     |     An internal memory allocation failed.
..     | :c:enumerator:`DRAGON_CHANNEL_UMAP_ERROR`
..     |     A unordered map error occurred.
..     | :c:enumerator:`DRAGON_CHANNEL_PRIORITY_HEAP_ERROR`
..     |     A priority heap error occurred.
..     | :c:enumerator:`DRAGON_CHANNEL_LOCK_ERROR`
..     |     A scalable shared lock error occurred.

..     **Example Usage**

..     .. code-block:: c

..         dragonChannelDescr_t ch;
..         dragonChannelSerial_t ch_ser;

..         /* read a serialized descriptor from a file that another process wrote.
..            Any other means of communicating this could be used (e.g., sockets,
..            pipes, other channel)*/
..         struct stat sb;
..         stat("ch_serial.dat", &sb);

..         dragonChannelSerial_t ch_ser;
..         ch_ser.len = sb.st_size;
..         ch_ser.data = malloc(ch_ser.len);

..         FILE * fp = fopen("ch_serial.dat", "r+");
..         size_t readb = fread(ch_ser.data, 1, ch_ser.len, fp);

..         dragonError_t derr = dragon_channel_attach(&ch_ser, &ch);
..         if (derr != DRAGON_SUCCESS)
..             return ERR;

.. .. c:function:: dragonError_t dragon_channel_detach(dragonChannelDescr_t * ch)

..     Detach from the channel described by *ch*.  By default this does not check for open send or read handles,
..     (:c:struct:`dragonChannelSendh_t` and/or :c:struct:`dragonChannelRecvh_t`) attempting to use said handles
..     after a detach will result in UB.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonError_t dragon_channel_recvh(dragonChannelDescr_t * ch, dragonChannelRecvh_t * ch_rh, const dragonChannelRecvAttrs_t * rattr)

..     Update *ch_rh* to be a valid receive handle for the channel described by *ch*.  If the attributes *rattr*
..     is ``NULL`` default options will be set on the receive handle.

..     **Returns**

..     | :c:enumerator:`DRAGON_SUCCESS`
..     |     Success.
..     | :c:enumerator:`DRAGON_CHANNEL_INVALID_ARGUMENT`
..     |     *ch* and *ch_rh* must not be ``NULL``.  *ch* must describe a valid channel.

..     **Example Usage**

..     .. code-block:: c

..         dragonMemoryPoolDescr_t mempool;

..         dragonM_UID_t m_uid = 0UL;
..         size_t pool_bytes = 1<<30;

..         /* create a new memory pool (existing one could have been used) */
..         dragonError_t merr = dragon_memory_pool_create(&mempool, pool_bytes, "test",
..                                                             m_uid, NULL);
..         if (merr != DRAGON_SUCCESS)
..             return ERR;

..         dragonChannelDescr_t ch;
..         dragonC_UID_t c_uid = 0UL;

..         /* create the channel in that memory pool */
..         dragonError_t derr = dragon_channel_create(&ch, c_uid, &mempool, NULL);
..         if (derr != DRAGON_SUCCESS)
..             return ERR;

..         dragonChannelRecvh_t rh;
..         derr = dragon_channel_recvh(&ch, &rh, NULL);

..         if (derr != DRAGON_SUCCESS)
..             return ERR;

.. .. c:function:: dragonError_t dragon_channel_sendh(dragonChannelDescr_t * ch, dragonChannelSendh_t * ch_sh, const dragonChannelSendAttrs_t * sattr)

..     Update *ch_sh* to be a valid send handle for the channel described by *ch*.  If the attributes *sattr* is
..     ``NULL`` default options will be set on the send handle.

..     **Returns**

..     | :c:enumerator:`DRAGON_SUCCESS`
..     |     Success.
..     | :c:enumerator:`DRAGON_CHANNEL_INVALID_ARGUMENT`
..     |     *ch* and *ch_sh* must not be ``NULL``.  *ch* must describe a valid channel.

..     **Example Usage**

..     .. code-block:: c

..         dragonMemoryPoolDescr_t mempool;

..         dragonM_UID_t m_uid = 0UL;
..         size_t pool_bytes = 1<<30;

..         /* create a new memory pool (existing one could have been used) */
..         dragonError_t merr = dragon_memory_pool_create(&mempool, pool_bytes, "test",
..                                                             m_uid, NULL);
..         if (merr != DRAGON_SUCCESS)
..             return ERR;

..         dragonChannelDescr_t ch;
..         dragonC_UID_t c_uid = 0UL;

..         /* create the channel in that memory pool */
..         dragonError_t derr = dragon_channel_create(&ch, c_uid, &mempool, NULL);
..         if (derr != DRAGON_SUCCESS)
..             return ERR;

..         dragonChannelSendh_t sh;
..         derr = dragon_channel_sendh(&ch, &sh, NULL);

..         if (derr != DRAGON_SUCCESS)
..             return ERR;

.. Operations on an Existing Local Channel
.. """""""""""""""""""""""""""""""""""""""

.. .. c:function:: dragonError_t dragon_channel_write_lock(dragonChannelDescr_t * ch)

..     **NOT YET IMPLEMENTED**

..     Lock the Channel *ch* from any process writing to it. If the Channel is already write-locked an error will
..     be returned. Any process that attempts to write messages to *ch* will block until the lock is released.
..     Any process can continue to read messages or access the state information from a write-locked Channel. A
..     write-locked Channel can be destroyed.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonError_t dragon_channel_read_lock(dragonChannelDescr_t * ch)

..     **NOT YET IMPLEMENTED**

..     Lock the Channel *ch* from any process reading from it. If the Channel is already read-locked an error
..     will be returned. Any process that attempts to read messages from the Channel will block until the lock is
..     released. Any process can continue to write messages or access the state information from a read-locked
..     Channel. A read-locked Channel can be destroyed.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonError_t dragon_channel_write_unlock(dragonChannelDescr_t * ch)

..     **NOT YET IMPLEMENTED**

..     Release the write lock on the Channel *ch*. If *ch* is not currently locked
..     or *ch* is in a quiesced state an error is returned.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonError_t dragon_channel_read_unlock(dragonChannelDescr_t * ch)

..     **NOT YET IMPLEMENTED**

..     Release the read lock on the Channel *ch*. If *ch* is not currently locked
..     or *ch* is in a quiesced state an error is returned.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonError_t dragon_channel_quiesce(dragonChannelDescr_t * ch)

..     **NOT YET IMPLEMENTED**

..     This function has the effect of locking the Channel *ch* for both reads and writes, halting any attempts
..     to modify the attributes of the Channel, and flushing the memory of the Channel back to disk if the
..     Channel was allocated from a Pool backed by a file. All locks and attribute freezing is left in place
..     after this call completes. To resume normal operation of the Channel use
..     :c:func:`dragon_channel_unquiesce()`. If the Channel already has read or write locks in place prior to
..     this call it will not result in an error.

..     Use cases for this is to prepare a Channel for physical relocation or to access the backing file with some
..     other interface (e.g., the filesystem) if applicable.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonError_t dragon_channel_unquiesce(dragonChannelDescr_t * ch)

..     **NOT YET IMPLEMENTED**

..     Resume normal operation of the Channel *ch* that is currently quiesced from
..     a call to :c:func:`dragon_channel_quiesce`. If *ch* is not currently
..     quiesced an error is returned.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonError_t dragon_channel_state(dragonChannelDescr_t * ch, dragonChannelState_t * state)

..     **NOT YET IMPLEMENTED**

..     Return the current state information :c:struct:`dragonChannelState_t` from
..     the Channel *ch*. The information can quickly be out of date unless read and
..     write locks are used on the Channel.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. Operations on an Existing Non-Local Channel
.. """""""""""""""""""""""""""""""""""""""""""

.. .. c:function:: dragonError_t dragon_channel_write_lock_gen_req(dragonChannelRequest_t * chreq, dragonChannelDescr_t * ch)

..     **NOT YET IMPLEMENTED**

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonError_t dragon_channel_write_lock_cmplt(const dragonChannelResponse_t * chresp)

..     **NOT YET IMPLEMENTED**

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonError_t dragon_channel_read_lock_gen_req(dragonChannelRequest_t * chreq, dragonChannelDescr_t * ch)

..     **NOT YET IMPLEMENTED**

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonError_t dragon_channel_read_lock_cmplt(const dragonChannelResponse_t * chresp)

..     **NOT YET IMPLEMENTED**

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonError_t dragon_channel_write_unlock_gen_req(dragonChannelRequest_t * chreq, dragonChannelDescr_t * ch)

..     **NOT YET IMPLEMENTED**

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonError_t dragon_channel_write_unlock_cmplt(const dragonChannelResponse_t * chresp)

..     **NOT YET IMPLEMENTED**

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonError_t dragon_channel_read_unlock_gen_req(dragonChannelRequest_t * chreq, dragonChannelDescr_t * ch)

..     **NOT YET IMPLEMENTED**

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonError_t dragon_channel_read_unlock_cmplt(const dragonChannelResponse_t * chresp)

..     **NOT YET IMPLEMENTED**

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonError_t dragon_channel_quiesce_gen_req(dragonChannelRequest_t * chreq, dragonChannelDescr_t * ch)

..     **NOT YET IMPLEMENTED**

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonError_t dragon_channel_quiesce_cmplt(const dragonChannelResponse_t * chresp)

..     **NOT YET IMPLEMENTED**

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonError_t dragon_channel_unquiesce_gen_req(dragonChannelRequest_t * chreq, dragonChannelDescr_t * ch)

..     **NOT YET IMPLEMENTED**

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonError_t dragon_channel_unquiesce_cmplt(const dragonChannelResponse_t * chresp)

..     **NOT YET IMPLEMENTED**

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. Channel Request Handling Functions
.. """"""""""""""""""""""""""""""""""

.. .. c:function:: dragonError_t dragon_channel_exec_req(dragonChannelResponse * chresp, const dragonChannelRequest * chreq)

..     **NOT YET IMPLEMENTED**

..     Execute the operations in the :c:struct:`dragonChannelRequest` request object *chreq*. The operations will
..     be performed using local resources (e.g., memory). Return a :c:struct:`dragonChannelResponse` response
..     object that can be communicated back to the process that generated *chreq*. Any errors that occur
..     executing the request will be encoded into *chresp* and therefore detectable by the requesting process.

..     The caller is responsible for freeing memory associated with any :c:struct:`dragonChannelRequest_t` or
..     :c:struct:`dragonChannelResponse_t` variables with :c:func:`dragon_channel_free_req()` or
..     :c:func:`dragon_channel_free_resp()`.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonError_t dragon_channel_free_req(dragonChannelRequest * chreq)

..     **NOT YET IMPLEMENTED**

..     Free the memory associated with *chreq*.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonError_t dragon_channel_free_resp(dragonChannelResponse * chresp)

..     **NOT YET IMPLEMENTED**

..     Free the memory associated with *chresp*.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. Channel Attributes Functions
.. """"""""""""""""""""""""""""

.. .. c:function:: dragonError_t dragon_channel_attr_init(dragonChannelAttr_t * attr)

..     Initialize *attr* with default values. Users should call this on any :c:struct:`dragonChannelAttr_t`
..     structure intended to be passed for Channel creation. Otherwise, the user must completely fill out the
..     structure.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonError_t dragon_channel_attr_destroy(dragonChannelAttr_t * attr)

..     Release any allocations in the :c:struct:`dragonChannelAttr_t` struct.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonError_t dragon_channel_setattr(dragonChannelDescr_t * ch, const dragonChannelAttr_t * newattr, dragonChannelAttr_t * oldattr)

..     **NOT YET IMPLEMENTED**

..     Modify the attributes of the Channel given by *ch* as specified in *newattr* and return the old attributes
..     are returned in *oldattr*. The values for
..     *oflag*, *lock_type*, and *creator_p_uid* cannot be modified. Those values
..     in *newattr* will be ignored.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonError_t dragon_channel_getattr(dragonChannelDescr_t * ch, dragonChannelAttr_t * attr)

..     **NOT YET IMPLEMENTED**

..     Retrieve the attributes from the Channel *ch* and return them in *attr*.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. Channel Message Functions
.. """""""""""""""""""""""""

.. .. c:function:: dragonError_t dragon_channel_message_init(dragonMessage_t * msg, dragonMemoryDescr_t * mem_descr, const dragonMessageAttr_t * mattrs)

..     Update the provided message structure to an initialized state. If
..     *mem_descr* is ``NULL``, the message is in an empty container state and can
..     be used in a call to dragon_chrecv_get_msg(), which will assign a memory descriptor to the message. If
..     *mem_descr* is not ``NULL``, the message can either be used with dragon_chsend_send_msg() or with
..     dragon_chrecv_get_msg().

.. .. c:function:: dragonError_t dragon_channel_message_destroy(dragonMessage_t * msg, _Bool free_mem_descr)

..     Cleanup the components of *msg*.  If *free_mem_descr* is ``true`` then any underlying memory descriptor
..     for the message will be released back to its base pool.

.. .. c:function:: dragonError_t dragon_channel_message_get_mem(const dragonMessage_t * msg, dragonMemoryDescr_t * mem_descr)

..     Obtain the underlying memory descriptor, if any, associated with the message.

.. Channel Receive Functions
.. """""""""""""""""""""""""

.. .. c:function:: dragonError_t dragon_chrecv_open(dragonChannelRecvh_t * ch_rh)

..     Open *ch_rh* for reading messages off of the attached Channel.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonError_t dragon_chrecv_get_msg(dragonChannelRecvh_t * ch_rh, dragonMessage_t * msg_recv)

..     Perform a non-blocking receive for the message at the front of the queue of the attached Channel. If the
..     memory descriptor associated with *msg_recv* is ``NULL``, the library will update it with a memory
..     descriptor where the payload is. If instead the memory descriptor associated with *msg_recv* is not
..     ``NULL``, the library will copy the payload from wherever it is into that memory (provided it is of at
..     least the required size).

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonChannelError_t dragon_chrecv_get_msg_blocking(dragonChannelRecvh_t * ch_rh, dragonMessage_t * msg_recv, timespec_t * timeout)

..     Perform a blocking or non-blocking receive for the message at the front of the queue of the attached
..     Channel. If the memory descriptor associated with
..     *msg_recv* is ``NULL``, the library will update it with a memory descriptor
..     where the payload is. If instead the memory descriptor associated with
..     *msg_recv* is not ``NULL``, the library will copy the payload from wherever
..     it is into that memory (provided it is of at least the required size).

..     The default blocking behavior and timeout is specified in the attributes when opening the receive handle
..     *ch_rh* with the *dragon_channel_recvh* call. The default is to block indefinitely. The default for the
..     handle can be changed in the attributes provided when the handle is created.

..     If *timeout* is *NULL* then the default timeout will be used. If either the default timeout or *timeout*
..     is *&DRAGON_CHANNEL_BLOCKING_NOTIMEOUT* then the blocking receive will not timeout, which is the default
..     behavior if not set in the receive handle. If either the default timeout or *timeout* is 0 seconds and 0
..     microseconds, then the receive will return immediately with a return code indicating the channel was
..     empty.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonError_t dragon_channel_poll(const dragonChannelDescr_t * ch, dragonWaitMode_t wait_mode, const uint8_t event_mask, timespec_t * timeout)

..     When POLLIN is specified in the event mask this call performs a blocking
..     poll on the channel until a message is available. If a message is available
..     when called, the poll will not block. All messages remain in the channel
..     after poll is called. Calling poll does not receive a message.

..     When POLLOUT is specified in the event mask this call performs a blocking
..     poll on the channel until there is space in the channel. If space is available
..     when called, the poll will not block.

..     It is possible to poll on the combination of POLLIN and POLLOUT by OR'ing the two
..     bit values together in the event mask. In that case the poll will return for either
..     of the two conditions above.

..     The blocking behavior and timeout are specified as arguments. You can have
..     idle or spin waiting and any timeout including no timeout (i.e. block
..     forever) and no blocking (i.e. a zero timeout).

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonError_t dragon_chrecv_close(dragonChannelRecvh_t * ch_rh)

..     Close *ch_rh*.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. Channel Send Functions
.. """"""""""""""""""""""

.. .. c:function:: dragonError_t dragon_chsend_open(dragonChannelSendh_t * ch_sh)

..     Open *ch_sh* for sending messages to the attached Channel.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonChannelSendError_t dragon_chsend_send_msg(dragonChannelSendh_t * ch_sh, const dragonMessage_t * msg_send, dragonMemoryDescr_t * dest_mem_descr, timespec_t * timeout)

..     Send the message in *msg* to the attached Channel. *dest_mem_descr* can be used to specify the location to
..     place the message payload. If you specify
..     *DRAGON_CHANNEL_SEND_TRANSFER_OWNERSHIP* as the value of *dest_mem_descr*
..     then this will result in a no-copy path where ownership of the shared memory payload of the message is
..     transferred, through the channel, to the receiving process. If the same memory descriptor of the payload
..     of the message is also provided as the *dest_mem_descr* this will also result in a no-copy path. If
..     *dest_mem_descr* is ``NULL`` and the message is too big to fit entirely
..     within the channel, space will be allocated for a copy of the message payload from the memory pool defined
..     in the *buffer_pool* member of the :c:struct:`dragonChannelAttr_t` given when the Channel was created. If
..     *buffer_pool* is NULL (*the default value*) then the memory pool the Channel
..     was allocated from will be used to make a copy of the message when it won't fit entirely within the
..     channel's message block size.

..     If no space is available in the channel, or if the message is to be copied and the copy can't be completed
..     due to a depleted pool, then this function blocks until either it can be sent, or the timeout expires.

..     The default timeout can be set in the channel send handle attributes. If
..     *timeout* is NULL, the default timeout is used. If *timeout* is set to &DRAGON_CHANNEL_BLOCKING_NOTIMEOUT*
..     *then the send will block without
..     timeout, which is the default. Otherwise, *timeout* will be used for the timeout value. The value is
..     interpreted as a relative timeout from the point in time at which it was called.

..     If the default timeout or *timeout* is specified as 0 seconds and 0 microseconds, then the send returns
..     immediately if it could not be sent with an error code indicating why it could not be sent immediately.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. .. c:function:: dragonChannelSendError_t dragon_chsend_close(dragonChannelSendh_t * ch_sh)

..     Close *ch_sh*.

..     Returns ``DRAGON_SUCCESS`` or an error code.

.. On-node Example
.. ===============

.. In this example, a process creates a new memory pool from which to allocate a new Channel. After creating the
.. Channel it creates a message to send and sends it off. Once done it will detach from the Channel, leaving it
.. in place.

.. .. code-block:: c

..     // create a memory pool (1 GB) we can allocate things off of
..     size_t pool_bytes = 1 << 30;
..     dragonError_t dmerr;
..     dragonMemoryPoolDescr_t pool;
..     dmerr = dragon_memory_pool_create(&pool, pool_bytes, "dragon_shm_example_filename", NULL);
..     if (dmerr != DRAGON_SUCCESS)
..         return ERR;

..     // create a Channel (c_uid = 0) allocated from that memory pool
..     dragonChannelDescr_t ch;
..     dragonError_t dcerr;
..     dragonC_UID_t c_uid = 0;
..     dcerr = dragon_channel_create(&ch, c_uid, &pool, NULL);
..     if (dcerr != DRAGON_SUCCESS)
..         return ERR;

..     // allocate some payload buffer space (1 KB) to send
..     size_t msg_bytes = 1 << 10;
..     dragonMemoryDescr_t payload;
..     dmerr = dragon_memory_alloc(&payload, &pool, msg_bytes);
..     if (dmerr != DRAGON_SUCCESS)
..         return ERR;

..     // fill the payload buffer up with something
..     int * ptr;
..     int i;
..     dmerr = dragon_memory_get_pointer(&payload, (void **)&ptr);
..     if (dmerr != DRAGON_SUCCESS)
..         return ERR;
..     for (i = 0; i < msg_bytes/sizeof(int); i++) {
..         ptr[i] = i;
..     }

..     // create a message to send
..     dragonError_t dmsgerr;
..     dragonMessageDescr_t msg;
..     dmsgerr = dragon_message_create(&msg, &payload);
..     if (dmsgerr != DRAGON_SUCCESS)
..         return ERR;

..     // get a send handle from the Channel
..     dragonChannelSendh_t ch_sh;
..     dcerr = dragon_channel_sendh(&ch, &ch_sh);
..     if (dcerr != DRAGON_SUCCESS)
..         return ERR;

..     // send the message letting the Channel library decide where the receive buffer
..     // should be allocated from.
..     dragonError_t dserr;
..     dserr = dragon_chsend_send_msg(&ch_sh, &msg, NULL, NULL);
..     if (dserr != DRAGON_SUCCESS)
..         return ERR;

..     // clean up the message descriptor
..     // note that the actual payload buffer is left in place when doing this
..     dmsgerr = dragon_message_destroy(&msg)
..     if (dmsgerr != DRAGON_SUCCESS)
..         return ERR;

..     // close the send handle
..     dserr = dragon_chsend_close(&ch_sh);
..     if (dserr != DRAGON_SUCCESS)
..         return ERR;

..     // detach from the Channel
..     dcerr = dragon_channel_detach(&ch);
..     if (dcerr != DRAGON_SUCCESS)
..         return ERR;


.. .. _PriorityHeap: https://en.wikipedia.org/wiki/Priority_queue