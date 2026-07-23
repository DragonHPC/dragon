.. _Launcher:

Launcher
++++++++

The Launcher is the entry point for the Dragon runtime — it is the ``dragon`` executable. Its primary
responsibility is to bring up all other Dragon services from nothing and arrange for the user's program
to start executing. Once the user program completes (or terminates), the Launcher orchestrates an orderly
shutdown of all services.

Frontend and Backend
====================

The Launcher consists of two components:

**Launcher Frontend**
    Runs on the node from which the user invokes ``dragon my.py``. It is responsible for:

    * Obtaining network configuration (node list, IP addresses) from the workload manager (Slurm, PBS+Pals, etc.)
      or from a user-supplied configuration file via ``dragon-network-config``.
    * Initiating bringup of :ref:`LocalServices` on each compute node.
    * Coordinating :ref:`GlobalServices` startup on the primary node.
    * Routing stdout, stderr, and stdin between the user's terminal and the managed processes.
    * Initiating and coordinating teardown when the user program exits.

**Launcher Backend**
    One backend process runs per compute node. It acts as the on-node representative of the Launcher
    Frontend, connecting the frontend to the node-local :ref:`LocalServices`. Specifically it:

    * Routes messages from :ref:`LocalServices` up to the frontend.
    * Routes messages from the frontend down into node-local :ref:`Channels`.
    * Participates in the bringup and teardown message protocol.

In the **single-node** case, the frontend and backend are co-located and communicate directly with each
other. The backend effectively plays both roles. See :ref:`SingleNodeDeployment` for details.

In the **multi-node** case, the frontend and backend communicate via a TCP-based overlay network
organized as a broadcast tree. See :ref:`MultiNodeDeployment` for the full bringup
and teardown sequence.

Network Configuration
=====================

Before starting compute-node services, the Launcher Frontend must know what resources are available
and get node level information. The ``dragon-network-config`` tool generates a YAML or JSON network
configuration file that is then passed to ``dragon`` at launch:

.. code-block:: console

    dragon-network-config --wlm slurm -o nodes.yaml
    dragon --network-config nodes.yaml my_program.py

See :ref:`command-line-interface` for the full list of ``dragon`` and ``dragon-network-config`` options.

Bringup Sequence
================

The high-level bringup sequence is:

1. Frontend reads network configuration and identifies the primary node.
2. Frontend spawns Launcher Backend processes on each compute node (via SSH or workload manager).
3. Each Backend starts :ref:`LocalServices` on its node and waits for it to report channels up.
4. Frontend coordinates :ref:`GlobalServices` startup on the primary node.
5. Once all services are up, Frontend starts the user's program as a managed process via Global Services.
6. Frontend enters its main event loop: routing I/O, handling process lifecycle messages.

For the detailed per-message bringup protocol, see :ref:`SingleNodeBringup` and :ref:`MultiNodeBringup`.

Teardown Sequence
=================

When the user's program exits (or is interrupted), the Launcher initiates an orderly shutdown:

1. Frontend sends a ``GSTeardown`` message to Global Services.
2. Global Services completes teardown and signals the backend.
3. Frontend sends ``LAHaltOverlay`` to halt the overlay network on each node.
4. Each Backend halts its node-local services and transport agents, then confirms with ``BEHalted``.
5. Frontend exits.

For the per-message teardown protocol, see the teardown sequence diagrams in
:ref:`SingleNodeDeployment` and :ref:`MultiNodeDeployment`.
