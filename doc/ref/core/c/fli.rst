

File-Like Interface
====================

.. _DragonFileLikeInterface:

.. contents:: Table of Contents
    :local:

Description
''''''''''''

The File-Like Interface is a streaming API for channels that guarantees
that a sender and a receiver can communicate in a streaming fashion (i.e.
more than one send operation per conversation) in an environment where there
may be multiple senders and multiple receivers from a single FLI.

There are many options for configuration of an FLI to achieve best performance from
buffering, to auxiliary stream channels, to receiver or sender supplied stream channels.

Constants
''''''''''''''''''''''''''''

.. doxygengroup:: fli_consts
   :content-only:
   :members:

Structures
''''''''''''''''

.. doxygengroup:: fli_structs
   :content-only:
   :members:

FLI Lifecycle Management
'''''''''''''''''''''''''''''

.. doxygengroup:: fli_lifecycle
   :content-only:
   :members:

FLI Send/Recv Handle Management
''''''''''''''''''''''''''''''''''''

.. doxygengroup:: fli_handles
   :content-only:
   :members:

FLI Send/Recv Functions
'''''''''''''''''''''''''''''

.. doxygengroup:: fli_sendrecv
   :content-only:
   :members:

FLI Information Functions
''''''''''''''''''''''''''''''

.. doxygengroup:: fli_info
   :content-only:
   :members: