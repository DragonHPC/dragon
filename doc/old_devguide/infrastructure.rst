Dragon Infrastructure
+++++++++++++++++++++


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


API Reference
==============

Here is the :ref:`InfrastructureAPI` API.