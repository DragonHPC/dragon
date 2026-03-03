.. _ChannelSets:

ChannelSets
===============


.. _ChannelSetAPI:

.. contents:: Table of Contents
    :local:

Description
''''''''''''

Dragon channelsets allow for efficient polling of one or more on-node channels. They
are completely event driven so no polling need be done to wait or query channels
within a channelset. As an event-driven construct, they can scale well to many channels.

API
'''''''

.. doxygengroup:: channelset_api
   :content-only:
   :members: