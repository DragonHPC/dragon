.. _DragonCoreChannels:

Channels
==============

.. contents:: Table of Contents
    :local:

Description
''''''''''''

Dragon channels are a structured means of sharing data both on and
off-node. On-node communication is achieved via shared memory to
minimize copying of data. Off-node communication via channels is
provided by a Dragon transport agent like HSTA.

Other higher-level constructs are typically built over channels to
hide many of the details of channel lifetime management and communication.
This API provides the lowest level channels access to Dragon.

Constants
''''''''''''''''''''''''''''

.. doxygengroup:: channels_constants
   :content-only:
   :members:

Structures
''''''''''''''''

.. doxygengroup:: channels_structs
   :content-only:
   :members:

Message Lifecycle Management
'''''''''''''''''''''''''''''

.. doxygengroup:: messages_lifecycle
   :content-only:
   :members:

Message Functionality
''''''''''''''''''''''''''''''''''''

.. doxygengroup:: messages_functionality
   :content-only:
   :members:

Channel Lifecycle Management
'''''''''''''''''''''''''''''

.. doxygengroup:: channels_lifecycle
   :content-only:
   :members:

Channels Functionality
''''''''''''''''''''''''''''''''''''''

.. doxygengroup:: channels_functionality
   :content-only:
   :members:

Gateway Message Lifecycle Management
'''''''''''''''''''''''''''''''''''''''

.. doxygengroup:: gateway_messages_lifecycle
   :content-only:
   :members:

Gateway Message Functionality
''''''''''''''''''''''''''''''''

.. doxygengroup:: gateway_messages_functionality
   :content-only:
   :members: