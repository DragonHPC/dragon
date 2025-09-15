.. _TransportAgent:

Transport Agent
+++++++++++++++

Dragon takes an innovative approach to HPC
networking. Rather than providing a networking library such as MPI or SHMEM, it uses a communication offloading
approach where the user application sends
communication requests to a separate executable, called the transport agent. The transport agent
receives communication requests and handles the actual network communication, all facilitated
through Dragon :ref:`Channels`, the core communication layer for Dragon (see :numref:`channels-hsta`).

The use of a transport agent rather than a library provides a number of benefits. Maybe foremost
is the ability for processes to come and go efficiently, without any need to initialize a communication
library for each new process, since transport agents persist throughout the duration of a job. Another
advantage is the ability to aggregate small message across an entire node, rather than only across a
single process. This allows for much greater aggregation of small messages, especially as node sizes grow.

.. _TCPTransport:

TCP Transport
+++++++++++++


.. _HSTA:

High Speed Transport Agent (HSTA)
+++++++++++++++++++++++++++++++++

Dragon’s High Speed Transport Agent, or HSTA, is an RDMA-based solution for off-node communication
of distributed Python multiprocessing applications. HSTA provides a high bandwidth, high message
rate and low latency communication framework that aims to make it easy to obtain high network
performance via a high-level programming interface. Other popular HPC communication frameworks,
such as MPI or SHMEM, generally require low level programming and expert knowledge of both the
communication API and the network hardware to obtain optimal performance. Network communication
is implicit in applications and workflows using the Dragon ecosystem, allowing HSTA to optimize
network communication behind the scenes.



.. figure:: images/channels_hsta.png
   :scale: 25%
   :name: channels-hsta

   **Dragon Channels off-node communication via the HSTA transport agent**