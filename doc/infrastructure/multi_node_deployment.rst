.. _MultiNodeDeployment:

Multi Node Deployment
+++++++++++++++++++++

This section outlines multi-node bringup and teardown procedure for multi-node
systems.

Multi node launches start from a frontend node that the user is assumed to have a
command prompt on. This frontend can be co-located with backend nodes
or run from its own node. All off-node communication that is initiated by the
:ref:`LocalServices` goes through :ref:`GlobalServices`. Local
Services itself has a one-node view of the world while Global Services does the
work of communicating off node when necessary. :numref:`deploy-multi-node` and :numref:`multi-node-overview` depict a
multi-node version of the Dragon :ref:`Services`.

.. figure:: images/deployment_multi_node.svg
    :name: deploy-multi-node

    **Startup Overview**

.. figure:: images/multinodeoverview.png
    :scale: 30%
    :name: multi-node-overview

    **Multi-Node Overview of Dragon Services**

.. _MultiNodeBringup:

Multi Node Bringup
==================

.. _NetworkConfiguration:

Network Configuration
---------------------

The launcher frontend must know what resources are available for its use on the
compute backend. To obtain that information for a given set of workload
managers, there is a network config tool in the launcher module. This tool is
exposed for general use. However, if deploying dragon on a supported workload
manager with an active job allocation, the launcher frontend will handle
obtaining the network configuration. It exists in `dragon.launcher.network_config.main`,
but can be invoked directly via `dragon-network-config`. Its help is below:

.. autodocstringonly:: dragon.launcher.network_config.main

If output to file (YAML or JSON are supported), the file can be provided to the
launcher frontend at launch. Formatting of the files appears below:

.. code-block:: YAML
  :linenos:
  :caption: **Example of YAML formatted network configuration file**

  '0':
    h_uid: null
    host_id: 18446744071562724608
    ip_addrs:
    - 10.128.0.5:6565
    is_primary: true
    name: nid00004
    num_cpus: 0
    physical_mem: 0
    shep_cd: ''
    state: 4
  '1':
    h_uid: null
    host_id: 18446744071562724864
    ip_addrs:
    - 10.128.0.6:6565
    is_primary: false
    name: nid00005
    num_cpus: 0
    physical_mem: 0
    shep_cd: ''
    state: 4

.. code-block:: JSON
  :linenos:
  :caption: **Example of JSON formatted network configuration file**

  {
    "0": {
          "state": 4,
          "h_uid": null,
          "name": "nid00004",
          "is_primary": true,
          "ip_addrs": [
              "10.128.0.5:6565"
          ],
          "host_id": 18446744071562724608,
          "num_cpus": 0,
          "physical_mem": 0,
          "shep_cd": ""
      },
      "1": {
          "state": 4,
          "h_uid": null,
          "name": "nid00005",
          "is_primary": false,
          "ip_addrs": [
              "10.128.0.6:6565"
          ],
          "host_id": 18446744071562724864,
          "num_cpus": 0,
          "physical_mem": 0,
          "shep_cd": ""
      }
  }


Launching
---------

The :ref:`LocalServices` and :ref:`GlobalServices` are instantiated when a
program is launched by the Dragon :ref:`Launcher`. There is one Local Services
instance on each node and one Global Services instance in the entire job.
Since all :ref:`Services` run as user-level services (i.e. not with superuser
authority), the services described here are assumed to be one per launched user
program.

The multi-node bring-up sequence is given in :numref:`startup-seq-multinode` and in the section titled
:ref:`MultiNodeBringup` where the message descriptions are also provided. The
Launcher Frontend brings up an instance of the Launcher Backend on each node.
Each launcher (frontend and backend) then brings up an instance of the TCP
Transport Agent which serves to create an Overlay Tree for communicating
infrastructure-related messages.

The Launcher Backend then brings up Local Services. The Backend
forwards messages from the Launcher Frontend to Local Services. Local Services forwards
output from the user program to the Frontend through the Backend.


Sequence diagram
-------------------

The diagram below depicts the message flow in the multi-node startup sequence.

.. raw:: html
    :file: images/startup_seq_multi_node.svg

**Sequence diagram of Dragon multi-node bringup**

Notes on Bring-up Sequence
^^^^^^^^^^^^^^^^^^^^^^^^^^

.. _launch_net_config:

Get Network Configuration (A1-A3)
    Launch 1 instance of network config tool on each backend compute node.
    Each instance will use other dragon tools to determine a preferred IP address
    for frontend and its unique Host ID that will be used in routing
    messages to and from it.

    Each network config instance will line buffer the network information to stdout
    as a serialized JSON structure. The frontend will consume this information.

    The config tool will then exit.

.. _wlm-launch:

Workload Manager Launch (Bring-up M1, M5 | Teardown M24)
    Launch one instance of given script/executable on each backend. No CPU affinity
    binding should be used (eg: `srun -n <# nodes> -N <# nodes> --cpu_bind=none python3 backend.py)

    The exit code from the WLM should be monitored for errors.

.. _launch-frontend-overlay:

Launch frontend overlay agent (A3-A4)
    Launch TCP transport agent. Over command-line arguments, provide channels for communication to
    and from the frontend (appears collectively as `CTRL ch` in the sequence diagrams),
    IP addresses of all backend compute nodes, and host IDs of all backend compute nodes.

    **NOTE: The Frontend and Backend each manage their own memory pool independent of Local Services
    specifically for managing the Channels necessary for Overlay Network communication**

    In the environment, there will be environment variables set to tell the overlay network what
    gateway channels it should monitor for remote messaging.

    Once the agent is up, it sends a ping saying as much.

.. _overlay-init:

Overlay input arguments (M3, M6):
    .. automodule:: dragon.transport.overlay
        :members: start_overlay_network

.. _launch-backend:

Launch backend and its overlay network (A4-M8)
    Use workload manager to launch 1 instance of backend on each compute node. In command line
    arguments, provide a channel to communicate with frontend, the frontend's IP address, and its
    host ID.

    The Backend will provide the frontend channel, IP, and host ID to start its own TCP overlay
    agent as in :ref:`launch-frontend-overlay`. Using the provided information via the launch command and
    its own locally obtained IP address and host ID, each backend instance will be able to communicate
    directly with the single frontend instance and vice-versa.

    Once backend and its overlay agent is up, it will send a `BEIsUp` message to the frontend. The
    frontend waits till it receives this message from every backend before continuing.

.. _give-node-id:

Provide Node Index to each Backend instance (M9)
    The Frontend will use host ID information in the `BEIsUp` message to assign node indices.
    The primary node is given node index 0 and can be specified by hostname via the user when
    starting the Dragon runtime. The remaining values will be integers up to `N-1`, where is `N`
    is the number of backend compute nodes.

    The node index is given in `FENodeIdxBE`. It travels from the frontend over the channel whose descriptor
    was provided in the `BEIsUp` by the backend.

.. _start-local-services:

Start Local Services (A12-A15)
    This is the most critical part of bring-up. If it is successful, everything else will likely be
    fine.

    Launcher Backend popens Local Services. Over `stdin`, it provides `BENodeIdxSH`. That messages
    contains logging channel descriptor, hostname, primary node status (boolean), node index, and
    IP address for backend communication **This is a potentially different IP address than the one being
    used for the overlay network. If it's the same IP, it IS a different port, as it will be used by a
    different transport agent for communication among other backend nodes, NOT the frontend.**

    `SHPingBE` is returned to the backend over `stdout`. This message contains the channel descriptors that
    will be used for all future communication on the node.

    The backend sends `BEPingSH` to confirm channel comms are successful. `SHChannelsUp` is returned by
    Local Services. It contains channel descriptors for all service communication for its node,
    including Global Services (for the primary node), which every other node will
    use to communicate with the Global Services instance on the primary node. The Host ID is also
    contained in `SHChannelsUp`.

.. _many-to-one:

Many-to-One SHChannelsUP, TAUp, TAHalted, SHHaltBE (Bring-up M14, M21 | Teardown M15, M19):
    These messages are a gather operation from all the backend nodes to the frontend.
    It represents a potential hang situation that requires care to eliminate the likelihood
    of a hung bring-up sequence.

.. _one-to-many:

One-to-Many LAChannelsInfo, SHHaltTA,  (Bring-up M15 | Teardown M10, M16):
    These messages and their communication (one-to-many Bcast) represents one of the biggest bottlenecks in
    the bring-up sequence. For larger messages, the first step in reducing that cost is compressing. Next would
    be to implement a tree Bcast.

.. _transmit-lachannelsinfo:

Transmit LAChannelsInfo (A15-A16)
    This is the most critical message. It contains all information necessary to make execution of
    the Dragon runtime possible.

    The Frontend receives one `SHChannelsUp` from each backend. It aggregates these into `LAChannelsInfo`.
    It is potentially a very large message as it scales linearly with the number of nodes. It is beneficial
    to compress this message before transmitting.

.. _start-transport:

Start Transport Agent (A16-A18)
    Local Services starts the Transport Agent as a child process. Over the command line, it passes the
    node index of the node, a channel descriptor to receive messages from Local Services (TA ch),
    logging channel descriptor. In the environment are environment variables containing channel
    descriptors for gateway channels, to be used for sending and receiving messages off-node.

    Over TA ch, `LAChannelsInfo` is provided, so the transport agent can take IP and host ID info
    to manage communication to other nodes on the backend.

    The transport agent communicates a `TAPingSH` to Local Services over the Local Services
    channel provided for its node index in the `LAChannelsInfo` message. This is sent upstream
    as an `TAUp` message. The frontend waits for such a message from every backend.

.. _transport-cli:

CLI Inputs to Start Transport Agent (M17)
    .. automodule:: dragon.transport
        :members: start_transport_agent


.. _start-global-services:

Start Global Services (A18-A22)
    After sending `TAUp`, Local Services on the primary node starts Global Services. Over `stdin`
    Local Services provides `LAChannelsInfo`. With this information, Global Services is
    immediately able to ping all Local Services' instances on the backend via `GSPingSH` (note:
    the Transport Agent is necessary to route those correctly).

    After receiving a response from every Local Services (`SHPingGS`), it sends a `GSIsUp`
    that is ultimately routed to the Frontend. It is important to note the `GSIsUp` message
    is not guaranteed to arrive after all `TAUp` messages, ie: `GSIsUp` will necessarily
    come after `TAUp` on the primary node but may come before the `TAUp` message from any
    other backend node.

    Once all `TAup` and `GSIsUp` messages are received, the Dragon runtime is fully up.

.. _start-user-program:

Start User Program (A22-A25)
    The Frontend starts the user program. This program was provided with the launcher's invocation.
    It is sent to the primary node via a `GSProcessCreate` message. Upon receipt, Global Services sends
    a `GSProcessCreateResponse`.

    After sending the response, it selects a Local Services to deploy the program to. After selection
    it sends this request as a `SHProcessCreate` to the selected Local Services. This Local Services
    sends a `SHProcessCreateResposne` as receipt.

.. _route-stdout:

Route User Program stdout (A25-A26)
    Local Services ingests all the User Program `stdout` and `stderr`. This is propogated to the
    Frontend as a `SHFwdOutput` message that contains information about what process the strings
    originated from. With this information, the Frontend can provide the output via its `stdout` so
    the user can see it buffered cleanly and correctly with varying levels of verbosity.


.. _MultiNodeTeardown:

Multi Node Teardown
===================

In the multi-node case, there are a few more messages that are processed by
Local Services than in the single-node case. The `GSHalted` message is forwarded
by Local Services to the launcher backend. The `SHHaltTA` is sent to Local Services,
and it forwards the `TAHalted` message to the backend when received.
Other than these three additional messages, the tear down is identical to the
single node version of tear down. One difference is that this tear down process
is repeated on every one of the compute nodes in the case of the multi-node
tear down.

In an abnormal situation, the `AbnormalTermination` message may be received by
the Launcher from either Local Services or Global Services (via the Backend). In
that case, the launcher will initiate a teardown of the infrastructure starting
with sending of `GSTeardown` (message 5 in diagram below).

Sequence diagram
-------------------

.. raw:: html
    :file: images/teardown_seq_multi_node.svg

**Multi-Node Teardown Sequence**

Notes on Teardown Sequence
^^^^^^^^^^^^^^^^^^^^^^^^^^

.. _head-proc-exit:

Head Proc Exit (A1-A4)
    Local Services monitors its managed processes via `waitpid`. Once it registers
    an exit, it matches its pid to the internally tracked and globally unique puid.
    This puid is transmitted via `SHProcessExit` to Global Services.

    Global Services cleans up any resources tied to tracking that specific head process.
    Once that is complete, it alerts the Frontend via a `GSHeadExit`.

.. _death-watcher:

Local Services Death Watcher (M1):
    Local Services' main thread has a thread repeatedly calling `waitpid`. This thread
    will receive the exit code from the head process.

.. _start-teardown:

Begin Teardown (A5-A7)
    With exit of the head process, as of this writing (03/24/2023), the Launcher will begin
    teardown.

    Teardown is initiated via the `GSTeardown` message to Global Services. Once this message is
    sent, every thing is ignored from user input. Teardown is what is being done. `GSTeardown`
    is also the message sent via a SIGINT signal from the user (assuming the full runtime is up).

    Consequently with a full runtime, `GSTeardown` is always the first point of entry for exiting
    the Dragon runtime no matter the state of various individual runtime services.

.. _halt-global-services:

Halt Global Services (A7-A9)
    Once Global Services receives the `GSTeardown`, it detaches from any channels. It does not
    destroy any (aside from any it may have directly created) because Local Services has created the
    channels used for infrastructure and manages the memory created for them.

    Once it has shutdown its resources, it transmits `GSHalted` to Local Services over `stdout`
    and exits. This is forwarded all the way to the Frontend.

.. _halt-transport:

Halt Transport Agent (A9-A12)
    After the Frontend receives `GSHalted`, it initiates teardown of all Backend
    Transport Agents via `SHHaltTA` which is eventually routed to the transport
    agent from Local Services.

    Upon receipt, it cancels any outstanding work it has, sends `TAHalted` over its
    channel to Local Services (**NOTE: This should be over stdout**) and exits.

    Local Services forwards the exit to the Frontend via routing of a `TAHalted` messages.
    The Frontend waits for receipt of N messages before continuing (**NOTE: This can easily
    result in hung processes and should be addressed**).

.. _issue-shteardown:

Issue SHTeardown (A13-A16)
    This is the first step in the more carefully orchestrated destruction of the Backend.
    Once Local Services receives `SHTeardown`, it detaches from its logging channel and
    tranmits a `SHHaltBE` to the Backend.

    After destroying its logging infrastructure, the Backend forwards the `SHHaltBE` to the Frontend.
    The Frontend waits till it receives N `SHHaltBE` messages before exiting. (**NOTE: Another
    potential mess**).

.. _issue-behalted:

Issue BEHalted (A17-A19)
    Frontend sends `BEHalted` to all Backends. Once sent, there is no more direct communication
    between Backend and Frontend. The Frontend will simply confirm completion via exit codes from
    the workload manager.

    Once Local Services receives its `BEHalted`, it deallocates all its allocated memory and exits.

.. _shutdown-be-overlay:

Shutdown Backend and its overlay (A20-A25)
    After transmitting `BEHalted` to Local Services, the Backend issues `SHHaltTA` to its
    overlay transport agent. This triggers teardown identical to the transport referenced in
    :ref:`halt-transport`.

    Once the overlay is down and Local Services is down as measured by a simple `wait` on
    child process exits, the Backend deallocated its managed memory, including logging
    infrastructure and exits.

.. _shutdown-fe-overlay:

Shutdown Frontend (A26-A30)
    After the workload manager returns exit code for the Backend exit, the Frontend shuts down
    its overlay similar to the Backend in :ref:`shutdown-be-overlay`. After waiting on the successful
    overlay exit, it similarly deallocates managed memory and exits.

    After the Frontend exits, the Dragon runtime is down.


