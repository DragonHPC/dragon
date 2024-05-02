.. _DragonCoreChannelsCython:

Channels
+++++++++++++

This is the Dragon channels interface for Python

.. contents::
    :depth: 3
    :local:
    :backlinks: entry


.. automodule:: dragon.channels
    :members: Message, ChannelSendH, ChannelRecvH, Channel, Peer2PeerReadingChannelFile, Many2ManyReadingChannelFile, Many2ManyWritingChannelFile, GatewayMessage
=======
Classes
=======

.. automodule:: dragon.channels
    :members: Message, ChannelSendH, ChannelRecvH, Channel, Peer2PeerReadingChannelFile, Many2ManyReadingChannelFile, Many2ManyWritingChannelFile, GatewayMessage

Functions
=========

.. automodule:: dragon.channels
    :members: register_gateways_from_env, discard_gateways

Enums
=====

.. automodule:: dragon.channels
    :members: OwnershipOnSend, LockType, EventType, FlowControl, ChannelFlags


Exceptions
==========

.. automodule:: dragon.channels
    :members: ChannelError, ChannelTimeout, ChannelHandleNotOpenError, ChannelSendError, ChannelSendTimeout, ChannelFull, ChannelRecvError, ChannelRecvTimeout, ChannelEmpty, ChannelBarrierBroken, ChannelBarrierReady, ChannelRemoteOperationNotSupported


