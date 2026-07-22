.. _multinode:

Running Across Nodes
++++++++++++++++++++

.. *Cleanup needed yet*

To run in multinode mode, Dragon must know what resources are available for its use on the
compute backend. Dragon natively supports the Slurm and PBS Workload Managers (WLMs) and can, in most
cases, automatically detect the allocated resources when running within an active job
on a cluster or supercomputer.

There are cases, however, when no traditional WLM is present, Dragon can't automatically
detect the available resources, or perhaps a subset of the available resources should be
used. In these cases, the :ref:`dragon` Launcher supports both the DragonRun and SSH
Lightweight Workload Managers (WLM).

The following multinode configurations are supported:

1. :ref:`Running on a cluster or supercomputer that has been configured with a Traditional WLM, such as Slurm or PBS+Pals.<using_a_traditional_wlm>`
2. :ref:`Running on a cluster using either the DragonRun or SSH Lightweight WLM.<using_drun_or_ssh_wlm>`

.. _using_a_traditional_wlm:

Running Dragon with a Traditional Workload Manager
--------------------------------------------------
To launch a Dragon program on several compute nodes, a Work Load Manager job allocation
obtained via `salloc` or `sbatch` (Slurm) or `qsub` (PBS+Pals) is required, eg:

.. code-block:: bash

    $ salloc --nodes=2
    $ dragon myprog.py arg1 arg2 ...

In the event that Dragon is run outside of an active WLM allocation an exception is
raised, and the program will not execute:

.. code-block:: bash
    :caption: Dragon exception when no WLM allocation exists:

    $ dragon p2p_lat.py --iterations 100 --lg_max_message_size 12 --dragon

    RuntimeError: Executing in a Slurm environment, but with no job allocation.
    Resubmit as part of an 'salloc' or 'sbatch' execution

To override this default behavior and execute a Dragon program on the same node as your shell,
the `--single-node-override / -s` option is available.

The Dragon runtime assumes all nodes in an allocation are to be used unless the `--nodes` option is specified.
This limits the user program to executing on a smaller subset of nodes, potentially useful for execution of scaling
benchmarks. For example, if the user has a job allocation for 4 nodes, but only wants to use 2 for their Dragon program,
they may do the following:

.. code-block:: bash

    $ salloc --nodes=4
    $ dragon --nodes 2 p2p_lat.py --iterations 100 --lg_max_message_size 12 --dragon

.. _using_drun_or_ssh_wlm:

Running Dragon using either the DragonRun or SSH WLM
----------------------------------------------------

The DragonRun and SSH WLMs are Lightweight Workload Managers (WLM) built into Dragon. These can be used on a generic cluster
without a traditional WLM, or any time Dragon needs to be run on a set of backend resources that otherwise can't be
automatically detected. This includes cases where a traditional WLM is present but perhaps only a specific subset of the allocated
nodes should be used, or when Dragon is not able to accurately detect the allocated nodes from the traditional WLM.

The DragonRun WLM uses an ssh-based 'command and control tree' to efficiently fan out the launch of the Dragon Runtime on
the backend compute notes. By using this tree-based launch mechanism, the DragonRun WLM can successfully launch on large
numbers of nodes with minimal load on the frontend Dragon launcher.

The soon to be deprecated SSH WLM uses a more traditional one-to-many SSH launch mechanism. Here the frontend Dragon Launcher
SSH's to each backend compute node individually in order to launch the Dragon runtime. This one-to-many SSH launch mechanism
can cause significant load on the frontend Dragon launcher when launching on large numbers of nodes, and is therefore not
recommended for large scale runs. For small scale runs, the SSH WLM can be used as a simple alternative to the DragonRun WLM
if desired.

The following sections describe how to use the DragonRun and SSH WLM options:

1. :ref:`Using the DragonRun WLM<use_drun_wlm>`
2. :ref:`Using the SSH WLM<use_ssh_wlm>`

.. _use_drun_wlm:

Using the DragonRun WLM:
^^^^^^^^^^^^^^^^^^^^^^^^

To use the DragonRun WLM, the following  options must be provided on the :ref:`dragon` launcher command line:

1. Select the DragonRun (drun) SSH Workload Manager

  The `--wlm drun / -w drun` option tells the :ref:`dragon` launcher to use the DragonRun launch WLM.

1. Provide available backend compute resources

  The list of available backend compute resources can be provided to the :ref:`dragon` launcher in
  one of several ways

    * :ref:`by using the Dragon Hosts (dhosts) utility<use_dhosts>` or
    * :ref:`by providing a list of backend compute resources (either explicitly on the launcher command line or via a file)<hostlist_hostfile>` or
    * :ref:`by providing a Dragon network configuration file<network_config>`

Note: Dragon requires that all nodes are configured for password-less SSH and maintain mutual routability.

.. _use_ssh_wlm:

Using the SSH WLM:
^^^^^^^^^^^^^^^^^^

To use the SSH WLM, the following  options must be provided on the :ref:`dragon` launcher command line:

1. Select the SSH SSH Workload Manager

  The `--wlm ssh / -w ssh` option tells the `dragon` launcher to use the SSH launch WLM.

1. Provide available backend compute resources

  The list of available backend compute resources can be provided to the :ref:`dragon` launcher in
  one of several ways

    * :ref:`by providing a list of backend compute resources (either explicitly on the launcher command line or via a file)<hostlist_hostfile>` or
    * :ref:`by providing a Dragon network configuration file<network_config>`

Note: Dragon requires that all nodes are configured for password-less SSH and maintain mutual routability.

.. _use_dhosts:

Using the Dragon Hosts (dhosts) utility
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The :ref:`dhosts` utility defines the list of hosts that should be used by other Dragon
runtime tools. To do this, :ref:`dhosts` generates a temporary hostfile and exports the
DRAGON_RUN_NODEFILE environment variable within a subshell. To generate the host
list, :ref:`dhosts` first attempts to detect an active Workload Manager (WLM)
allocation, such as from Slurm or PBS. If no WLM is present, or if :ref:`dhosts` is unable
to detect the allocated WLM nodes, the list of hosts can be specified manually via the
`--hostlist` or `--hostfile` options.

This is useful for running other dragon tools on a specific set of hosts without having to
specify the list of hosts to each tool individually. Since :ref:`dhosts` exports the
DRAGON_RUN_NODEFILE environment variable, any tool that relies on this environment variable
can automatically use the generated hostlist. For example, dragon-cleanup will automatically
use the hostlist generated by :ref:`dhosts` if DRAGON_RUN_NODEFILE is set in the environment.

To provide the available nodes explicitly on the :ref:`dhosts` command line, specify the available
backend hostnames as a comma-separated list, eg: `--hostlist host_1,host_2,host_3`.

.. code-block:: shell
  :name: host_list
  :caption: **Providing a list of hosts via the dhosts utility**

  $ dhosts --hostlist host_1,host_2,host_3
  $ echo $DRAGON_RUN_NODEFILE
  /tmp/dragon_run_nodefile_12345
  $ cat $DRAGON_RUN_NODEFILE
  host_1
  host_2
  host_3

To provide the available nodes via a text file, create a newline separated text file with each
backend node's hostname on a separate line. Pass the name of the text file to the :ref:`dhosts`
command line, eg: `--hostfile hosts.txt`.

.. code-block:: shell
  :name: host_file
  :caption: **Providing a list of hosts via a text file**

  $ cat hosts.txt
  host_1
  host_2
  host_3
  $ dhosts --hostfile hosts.txt
  $ echo $DRAGON_RUN_NODEFILE
  /tmp/dragon_run_nodefile_12345
  $ cat $DRAGON_RUN_NODEFILE
  host_1
  host_2
  host_3

NOTE: You cannot use both `--hostfile` and `--hostlist` on the commandline at the same time.

.. _hostlist_hostfile:

Providing a Host List or Host File
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Providing a list of hosts to the :ref:`dragon` launcher can be done either by listing them explicitly
on the :ref:`dragon` command-line or by providing the :ref:`dragon` launcher the name of a newline
seperated text file containing the list of host names.

To provide the available nodes explicitly on the :ref:`dragon` command line, specify the available
backend hostnames as a comma-separated list, eg: `--hostlist host_1,host_2,host_3`.

.. code-block:: shell
  :name: host_list
  :caption: **Providing a list of hosts via the command line**

  $ dragon -w drun -t tcp --hostlist host_1,host_2,host_3 [PROG]

To provide the available nodes via a text file, create a newline separated text file with each
backend node's hostname on a separate line. Pass the name of the text file to the :ref:`dragon`
launcher, eg: `--hostfile hosts.txt`.

.. code-block:: shell
  :name: host_file
  :caption: **Providing a list of hosts via a text file**

  $ cat hosts.txt
  host_1
  host_2
  host_3
  $ dragon -w drun -t tcp --hostfile hosts.txt [PROG]

NOTE: You cannot use both `--hostfile` and `--hostlist` on the commandline at the same time.

When passing the list of available backend nodes in either of these ways, the :ref:`dragon` launcher
needs to determine basic network configuration settings for each listed node before it can launch
the Dragon user application. This is done by launching a utility application on each listed node
to report the node's IP and other relevant information. Running this utility application slightly
delays the startup of Dragon. To prevent this delay, you can instead generate a Dragon
network-config file as explained below.

.. _network_config:

Providing a Dragon Network-Config File
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Dragon provides a utility application to gather and persist relevant network information
from it's backend compute resorces. This utility can be used to generate a persistent YAML
or JSON configuration which, when passed to the :ref:`dragon` launcher, provides all
required information about a set of backend compute nodes.

To generate a network configuration file for a given set of backend compute nodes, run the
`dragon-network-config` tool as shown below:

.. code-block:: shell
  :name: ex_run_network_config
  :caption: **Example of how to run the dragon-network-config tool**

  $ dragon-network-config -w drun --hostlist host1,host2,host3,host4 -j
  $  ls ssh.json
  ssh.json

Once you have a network configuration file, the name of the configuration file can
be passed to the :ref:`dragon` launcher to identify the available backend compute resources:

.. code-block:: shell
  :name: host_list
  :caption: **Providing a list of hosts via the command line**

  $ dragon -w drun -t tcp --network-config ssh.json [PROG]

*NOTE*: Changes to the backend compute node's IP addresses or other relevant network
settings will invalidate the saved network config file. If this happens, please
re-run the `dragon-network-config` tool to collect updated information.

The `dragon-network-config` help is below:

.. autodocstringonly:: dragon.launcher.network_config.main

Formatting of the network-config file appears below for both JSON and YAML:

.. code-block:: YAML
  :name: yaml_network_config
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
  :name: json_network_config
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

When nodes have multiple available NICs, attention should be paid to the number and order of
IP addresses specified in the network configuration file.  Because the `dragon-network-config`
utility has no way of knowing which of the multiple NICs and IP addresses should be used
preferentially on a given node, the list of "ip_addrs" specified in the network config
YAML/JSON file may need to be manually adjusted to ensure the preferred IP address is first
in the list. This manual review and ordering adjustment is only necessary when some NICs can
and some NICs can not route to other nodes in the Dragon cluster.

Although not specified as part of the network configuration, if the frontend node also has
multiple NICs and only some have available routes to the compute nodes, it is possible to
specify the routable IP address (and thereby NIC) to use on the frontend node for all
communications with the compute nodes via the environment variable, `DRAGON_FE_IP_ADDR`.
A toy example showcasing how to specify which NIC to use of the frontend / head node
while simultaneously specifying which NICs to use on the compute nodes (via the network
config JSON file):

.. code-block:: bash

    # Note that the value "1.2.3.4" should be replaced with the appropriate local IP address.
    $ DRAGON_FE_IP_ADDR="1.2.3.4:6566" dragon --wlm drun --network-config my_cluster_config.json --network-prefix '' my_user_code.py

.. _transport_agents:

High Speed Transport Agent (HSTA)
---------------------------------

HSTA is a high-speed transport agent that provides MPI-like performance using
Dragon Channels. HSTA uses libfrabric or libucp for communication over Slingshot
or Infiniband high-speed interconnection networks. If you have one of these networks
you can configure HSTA to run on it using the appropriate `dragon-config` options. See
the :ref:`installation-guide` section for examples of how to configure Dragon to use HSTA.

The HSTA transport agent is currently not available in the opensource version of
Dragon. For inquiries about Dragon's high speed RDMA-based transport, please
contact HPE by emailing dragonhpc@hpe.com.

.. _tcp_transport_agent:

TCP-based Transport Agent
-------------------------

The TCP-based transport agent is the default transport agent
for the Dragon opensource package. The TCP transport agent utilizes standard TCP
for inter-node communication through Dragon Channels.

When using a version of Dragon that includes the HSTA transport agent and you prefer to
use the TCP transport agent, the `--transport tcp` option can be passed to the launcher (see:
:ref:`FAQ <Transport FAQ>` and :ref:`Launcher options <Dragon CLI Options>`). The dragon-config
command can also be used to specify that the TCP transport should be used. To do that you run
dragon-config as follows.

.. code-block:: console

    dragon-config -a 'tcp-runtime=True'

The TCP agent is configured to use port 7575 by default. If that port is blocked,
it can be changed with the `--port` argument to :ref:`dragon`. If not specific,
7575 is used:, eg:

.. code-block:: bash

    # Port 7575 used
    $ dragon --nodes 2 p2p_lat.py --iterations 100 --lg_max_message_size 12 --dragon

    # Port 7000 used
    $ dragon --port 7000 --nodes 2 p2p_lat.py --iterations 100 --lg_max_message_size 12 --dragon

The TCP transport agent also favors known Cray high-speed interconnect networks by default. This is accomplished via
regex specification of the network's named prefix matchin `ipogif` (Aries) or `hsn` (Slingshot): `r'^(hsn|ipogif)\d+$'`.
To change, for example, to match only `hsn` networks, the `--network-prefix` argument could be used:

.. code-block:: bash

    $ dragon --network-prefix hsn --nodes 2 p2p_lat.py --iterations 100 --lg_max_message_size 12 --dragon


*Known Issue*: If a `--network-prefix` argument is given that doesn't actually exist, the Dragon runtime will enter
a hung state. This will be fixed in future releases. For now, a `ctrl+z` and `kill` will be necessary to recover.