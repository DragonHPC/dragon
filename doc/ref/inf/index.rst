.. _InfrastructureAPI:

Dragon Infrastructure
+++++++++++++++++++++

:ref:`Services` in the Dragon runtime interact with each other using messages transported with a variety of
different means (mostly :ref:`Channels`). Although there is the Client API to construct and send these
messages, the messages themselves constitute the true internal interface. To that end, they are a convention.
Developers should use this API to add functionality to the Dragon Services through new messages.
It is not meant for users.

Reference
=========

Python Components
-----------------

.. currentmodule:: dragon.infrastructure

.. autosummary::
    :toctree:
    :recursive:

   channel_desc
   connection
   facts
   group_desc
   messages
   node_desc
   parameters
   policy
   pool_desc
   process_desc
   standalone_conn
   util

C Components
------------

.. toctree::
    :maxdepth: 1

    logging


Architecture
============


.. figure:: images/infrastructure_architecture.svg
   :scale: 75%
   :name: dragon-inf-api-architecture 

   **Architecture of the Dragon Infrastructure API**

:numref:`dragon-inf-api-architecture` shows a UML2 component diagram of the Dragon infrastructure API and its components.

The infrastructure API is consumed by Dragon Services: Local Services, Global
Services, Launcher Backend, and the Transport Agents. It consists mostly of conventions, like message types and common IDs.
The API also implements a basic connection object that abstract Channels for convenience and performance.