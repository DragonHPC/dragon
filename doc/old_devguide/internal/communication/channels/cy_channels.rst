.. _ChannelsCython:

`dragon.channels` -- Channel Objects and Operations
===================================================

This page documents the Python/Cython interface for the Dragon Channels library.

Cython Interface API
--------------------

All functions and variables listed as C languages are implemented with the *cdef* keyword and are not directly
accessible from python-space.

Factory init methods are used in place of pythonic constructors as they require Python object arguments, while
some instances require the use of C structs.


Managing Messages
-----------------

Messages can either be explicitly managed, or bytes objects can be sent and the API will manage creating,
populating, and releasing messages.

Explicitly managing messages allows the underlying *ManagedMemory* allocation to be reused for subsequent
message sends.  However, it is now up to the user to explicitly destroy that message object when they are done
with it.  Similarly, when a Message is received or used as a destination, it is up to the receiving process to
destroy the object when done with it.  Explicit management can be thought of as the "high performance" way of
sending and receiving messages, since reusing message buffers eliminates costly repeat creation, allocation,
and destroy calls.

.. code-block::

   send_h = ch.sendh()
   recv_h = ch.recvh()

   s_msg = Message.create_alloc(512)
   r_msg = Message.create_alloc(512)
   # Assume we've populated the s_msg with some bytes (See code example at bottom of page)

   # Send to a specific destination
   send_h.send(s_msg, r_msg)
   # Pull the payload out of the Channel queue (this is essentially a free operation)
   recv_h.recv(r_msg)

   # Optionally, no destination can be specified on send
   send_h.send(s_msg)
   # But we can then receive the message into a specific payload
   recv_h.recv(r_msg)



The `send_bytes` and `recv_bytes` calls are easier to use.  `send_bytes` takes regular python bytes objects,
and will handle message lifetime for the user.  `recv_bytes` similarly returns a regular python bytes object,
and handles message lifetime for the user.  This is potentially less performant, since the underlying Message
buffers cannot be reused on subsequent calls.

.. code-block::

   x = bytearray(512)
   x[0:5] = b"Hello"
   # Length determines how big of a message will be allocated and managed by the API
   send_h.send_bytes(x, 512)
   y = recv_h.recv_bytes()


Both forms of these calls for sending and receiving can be intermixed, allowing for the sending of python
bytes objects and then receiving into explicitly managed messages and vice versa.


Objects
+++++++

.. py:class:: Channel

   Python object representing a Channel.  Used to create/attach to channels and their associated send/receive
   handles.

   .. c:var:: dragonChannelDescr_t _channel

      Internal C handle for the channel

   .. py:staticmethod:: create(MemoryPool mem_pool, dragonC_UID_t c_uid, attr=None)

      Create a new channel from the specified memorypool using a unique identifier and optional attributes
      object (not yet implemented).

      :param mem_pool: MemoryPool object the channel should be allocated into
      :param c_uid: Unique identifying number fo the channel.  Type assertions require it be >= 0.
      :param attr: Attributes object for the Channel
      :return: New Channel object.
      :raises: ChannelError if the channel could not be created, such as if the UID is already in use or the memory pool is full

   .. py:staticmethod:: attach(ser_bytes)

      Attach to an existing channel through a serialized byte descriptor.

      :param ser_bytes: The serialized bytes object containing info to attach to a channel
      :return: New Channel object
      :raises: ChannelError if serializer is invalid or channel is non-local

   .. py:method:: destroy()

      Destroy the channel this object is attached to.

      :raises: ChannelError

   .. py:method:: detach(serialize=False)

      Detach from the channel. Allows for
      storing a serializer to this channel in the object for later retrieval.

      :param serialize: Optionally store a serializer of the channel before detaching
      :raises: ChannelError if the channel cannot be detached from

   .. py:method:: serialize()

      :return: Bytes object of serialized info
      :raises: ChannelError if the channel cannot be serialized

   .. py:method:: sendh()

      :return: New Send Handle object and return it unopened.
      :raises: ChannelError if the object cannot be created

   .. py:method:: recvh()

      :return: New Receive Handle object and return it unopened.
      :raises: ChannelError if the object cannot be created

.. py:class:: Message

   Python object to handle message instantiation, allocation, and payloads.

   .. c:var:: dragonMessage_t _msg

      Internal C message struct used in Cython calls.

   .. c:var:: dragonMemoryDescr_t * new_mem

      Internal C pointer to deal with necessary managed memory allocations for sending messages.

   .. py:staticmethod:: create_empty()

      :return: Message object with no memory backing

   .. py:staticmethod:: create_alloc(MemoryPool mpool, size_t n_bytes)

      Create a new message and allocate ``n_bytes`` from the provided ``MemoryPool`` object.

      :param mpool: MemoryPool to allocate from
      :param n_bytes: Number of bytes to allocate
      :return: Message object with memory backing of specified size
      :raises: ChannelError if allocation fails

   .. py:method:: destroy(free_mem=True)

      Destroy the message and its underlying managed memory allocation.

      :param free_mem: Whether or not to free the underlying memory.
      :raises: ChannelError if the message cannot be destroyed.

   .. py:method:: bytes_memview()

      :return: Memoryview object of the underlying message payload memory
      :raises: ChannelError if message is not retrievable, memory is inaccessible, or size cannot be retrieved

.. py:class:: ChannelSendH

   Python object to handle sending messages to a channel.

   .. c:var:: dragonChannelSendh_t _sendh

      Internal C struct for the send handle.

   .. c:var:: MemoryPool _pool

      Used in the `send_bytes` method

   .. py:method:: open()

      Open the send handle for use.

      :Raises: ChannelError If the handle cannot be opened, such as if the underlying channel has been
      destroyed or detached

   .. py:method:: close()

      Close the send handle

      :raises: ChannelError if the handle cannot be closed, such as if the underlying channel has been
      destroyed or detached

   .. py:method:: send(msg: Message, dest_msg: Message=None, ownership=copy_on_send, blocking=True, timeout=ChannelSendH.USE_CHANNEL_SENDH_DEFAULT)

      Send the payload containing within ``msg`` out onto the channel.  Takes an optional destination message
      to deliver the payload into.

      With ``timeout`` as the default ``USE_CHANNEL_SENDH_DEFAULT`` the defaults within the channel handle are
      used. Defaults are settable when the handle is created. If not set differently, the channel send handle
      defaults are to block indefinitely until the message can be sent.

      Overriding either ``blocking`` or ``timeout`` means to use the override value and ignore the handle
      defaults.

      Overriding ``blocking`` as False means to try once and if the channel is full, return the appropriate
      return code indicating it is full.

      Overriding ``timeout`` as None means to block indefinitely. A value of ``timeout`` less than 0 raises an
      exception. A ``timeout`` greater than 0 will block the given number of seconds or until the message can
      be sent. A timeout of 0 is the same as specifying ``blocking`` as False.

      :param msg: Message object containing the desired payload
      :param dest_msg: Optional message object to send payload into
      :param timeout: None or an integer as described above.
      :raises: ChannelFull, ChannelSendError, or ChannelError if the message could not be sent

   .. py:method:: send_bytes(msg_bytes, msg_len: int, dest_msg: Message=None)

      Take a Python bytes object and send it into the channel.  This is as opposed to the `send` method which
      takes an explicitly managed Message object.  `send_bytes` will take a bytes object, create a Message
      object, send it out, and then destroy the Message for the caller.  Similar to `send` a specified
      destination can be supplied.

      :param msg_bytes: The bytes object to be copied and sent to the channel
      :param msg_len: The length of bytes to be allocated (will be rounded up to power of 2)
      :param dest_msg: Optional Message object to send the payload into
      :raises: ChannelSendError, ChannelFull, or ChannelError similar to `send`

.. py:class:: ChannelRecvH

   Python object to handle receiving messages from a channel.

   .. c:var:: dragonChannelRecvh_t _recvh

      Internal C struct for the receving handle.

   .. py:method:: open()

      Open the receive handle for use.

      :raises: ChannelError if the handle cannot be opened, such as if the underlying channel has been destroyed or detached

   .. py:method:: close()

      Close the receive handle

      :raises: ChannelError if the handle cannot be closed, such as if the underlying channel has been destroyed or detached

   .. py:method:: recv(dest_msg: message=None, blocking=True, timeout=ChannelRecvH.USE_CHANNEL_RECVH_DEFAULT)

      Receive a message from the channel. Optional parameter to specify a Message to receive the object. If no
      `dest_msg` is specified, a new Message object will be created and populated with the payload and
      returned. If a `dest_msg` is specified, the underlying memory will be populated with the payload.
      Passing in a `dest_msg` can be thought of as passing in a reference to the underlying memory, and the
      return value can be ignored as no new objects are created.

      With ``timeout`` as the default ``USE_CHANNEL_RECVH_DEFAULT`` the defaults within the channel handle are
      used. Defaults are settable when the handle is created. If not set differently, the channel receive
      handle defaults are to block indefinitely until a message is received.

      Overriding either ``blocking`` or ``timeout`` means to use the override value and ignore the handle
      defaults.

      Overriding ``blocking`` as False means to try once and if the channel is empty, return the appropriate
      return code indicating it is empty.

      Overriding ``timeout`` as None means to block indefinitely. A value of ``timeout`` less than 0 raises an
      exception. A ``timeout`` greater than 0 will block the given number of seconds or until a message is
      received. A timeout of 0 is the same as specifying ``blocking`` as False.

      :param: message is a destination message to to copy it into.
      :param: timeout None or an integer as described above.
      :return: Message object containing the payload from the channel
      :raises: ChannelEmpty if the channel is empty, ChannelHandleNotOpenError if it is closed, or ChannelTimeout if there was a timeout.

   .. py:method:: recv_bytes()

      Mirror to `ChannelSendH` `send_bytes` method.  Receive a message and pass back a copy in the form of a bytes object, freeing the message in the process.

      :return: Bytes object of the retrieved message
      :raises: ChannelEmpty, ChannelHandleNotOpenError or ChannelTimeout similar to `recv`


Example Usage
--------------

.. code-block::

   from dragon.managed_memory import MemoryPool
   from dragon.channels import Channel, ChannelRecvH, ChannelSendH, Message

   # Create a pool
   mpool = MemoryPool(32678, "example_pool", 0, None)
   # Create a channel
   ch = Channel(mpool, 1)

   # Open a send handle
   sendh = ch.sendh()
   sendh.open()

   # Open a receive handle
   recvh = ch.recvh()
   recvh.open()

   # Explicitly allocate a message and populate its payload
   msg = Message.create_alloc(mpool, 512)
   msg_view = msg.bytes_memview()
   msg_view[0:5] = b"Hello"

   # Send the message
   sendh.send(msg)

   # Receive a message
   recv_msg = recvh.recv()
   recv_msgview = recv_msg.bytes_memview()

   # Prints "Hello -- Hello"
   print(f"{msg_view[0:5]} -- {recv_msgview[0:5]})
