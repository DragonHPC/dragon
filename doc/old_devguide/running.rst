.. _running:

Running Dragon
++++++++++++++

Dragon can be run on either a single node (such as your laptop or other single compute resource) or on a
cluster of many compute resources (multi-node system). In either case, launching a Dragon application is
done in a similar way.

1. Ensure that the Dragon package has been installed into your Python environment. This is typically done
   within a virtual Python environment. See the Dragon :doc:`installation instructions </start/start>` on
   performing this step.

2. Ensure that the Dragon module has been loaded. This adds the `dragon` command to your environment.

  .. code-block:: console

      module use [/path to dragon]/modulefiles
      module load dragon

3. Start your dragon application using the `dragon` launcher command, passing any relevant command line
   options.

  .. code-block:: console

      dragon [dragon options] [program] [program options]

The `dragon` launcher's full command help and basic usage appears below:

.. _Dragon CLI Options:

.. autodocstringonly:: dragon.launcher.launch_selector.main

In the event that dragon exits abnormlly, use the helper script `dragon-cleanup` to clean up any
zombie processes and reserved memory. The `dragon-cleanup` script is located in the
`[dragon install dir]/bin` directory and added to the `$PATH` environment variable after
loading the Dragon module.

Running Dragon on a Multi-Node System
=====================================

To run in multinode mode, Dragon must know what resources are available for its use on the
compute backend. When using a workload manager (WLM) such as Slurm or PBS+Pals, Dragon normally
obtains the list of available backend compute resources automatically from the active WLM
allocation. However, when Dragon is used on a generic cluster without a traditional WLM,
Dragon has no way to automatically ascertain what backend compute resources are available.
In these cases Dragon can be run using a generic SSH launch.

Dragon supports the following multinode configurations:

1. :ref:`Running on a cluster or supercomputer that has been configured with a Work Load Manager (WLM), such has Slurm or PBS+Pals.<using_a_wlm>`
2. :ref:`Running on a cluster without any Work Load Manager (WLM) using generic SSH launch.<using_ssh_launch>`

.. _using_a_wlm:

Running Dragon with a Work Load Manager
---------------------------------------
To launch a Dragon program on several compute nodes, a Work Load Manager job allocation
obtained via `salloc`_ or `sbatch`_ (Slurm) or `qsub` (PBS+Pals) is required, eg:

.. code-block:: bash

    $ salloc --nodes=2
    $ dragon p2p_lat.py --iterations 100 --lg_max_message_size 12 --dragon

In the event that Dragon is run outside of an active WLM allocation an exception is
raised, and the program will not execute:

.. code-block:: bash
    :caption: Dragon exception when no WLM allocation exists:

    $ dragon p2p_lat.py --iterations 100 --lg_max_message_size 12 --dragon

    RuntimeError: Executing in a Slurm environment, but with no job allocation.
    Resubmit as part of an 'salloc' or 'sbatch' execution

To override this default behavior and execute a Dragon program on the same node as your shell, the `--single-node-override / -s` option is available.

The Dragon service runtime assumes all nodes in an allocation are to be used unless the `--node-count` option is used.
This limits the user program to executing on a smaller subset of nodes, potentially useful for execution of scaling
benchmarks. For example, if the user has a job allocation for 4 nodes, but only wants to use 2 for their Dragon program,
they may do the following:

.. code-block:: bash

    $ salloc --nodes=4
    $ dragon --nodes 2 p2p_lat.py --iterations 100 --lg_max_message_size 12 --dragon

.. _using_ssh_launch:

Running Dragon using generic SSH launch
---------------------------------------

To use SSH launch, the following configuration options must be provided on the `dragon`
launcher command line:

1. Select the SSH Workload Manager
  The `--wlm ssh / -w ssh` option tells the `dragon` launcher to use generic SSH launch
  semantics.

2. Provide available backend compute resources
  The list of available backend compute resources can be provided to the `dragon` launcher in
  one of several ways

    * :ref:`by providing a list of backend compute resources (either explicitly on the launcher command line or via a file)<hostlist_hostfile>` or
    * :ref:`by providing a Dragon network configuration file<network_config>`

Note: Dragon requires that passwordless SSH is enabled for all backend compute resources.

.. _hostlist_hostfile:

Providing a Host List or Host File
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Providing a list of hosts to the `dragon` launcher can be done either by listing them explicitly
on the `dragon` command-line or by providing the `dragon` launcher the name of a newline
seperated text file containing the list of host names.

To provide the available nodes explicitly on the `dragon` command line, specify the available
backend hostnames as a comma-separated list, eg: `--hostlist host_1,host_2,host_3`.

.. code-block:: shell
  :name: host_list
  :caption: **Providing a list of hosts via the command line**

  (_env) root $ dragon -w ssh -t tcp --hostlist host_1,host_2,host_3 [PROG]

To provide the available nodes via a text file, create a newline separated text file with each
backend node's hostname on a separate line. Pass the name of the text file to the `dragon`
launcher, eg: `--hostfile hosts.txt`.

.. code-block:: shell
  :name: host_file
  :caption: **Providing a list of hosts via a text file**

  (_env) root $ cat hosts.txt
  host_1
  host_2
  host_3
  (_env) root $ dragon -w ssh -t tcp --hostfile hosts.txt [PROG]

NOTE: You cannot use both `--hostfile` and `--hostlist` on the commandline at the same time.

When passing the list of available backend nodes in either of these ways, the `dragon` launcher
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
or JSON configuration which, when passed to the `dragon` launcher, provides all
required information about a set of backend compute nodes.

To generate a network configuration file for a given set of backend compute nodes, run the
`dragon-network-config` tool as shown below:

.. code-block:: shell
  :name: ex_run_network_config
  :caption: **Example of how to run the dragon-network-config tool**

  (_env) root $ dragon-network-config -w ssh --hostlist host1,host2,host3,host4 -j
  (_env) root $  ls ssh.json
  ssh.json

Once you have a network configuration file, the name of the configuration file can
be passed to the `dragon` launcher to identify the available backend compute resources:

.. code-block:: shell
  :name: host_list
  :caption: **Providing a list of hosts via the command line**

  (_env) root $ dragon -w ssh -t tcp --network-config ssh.json [PROG]

NOTE: Changes to the backend compute node's IP addresses or other relevant network
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
    $ DRAGON_FE_IP_ADDR="1.2.3.4:6566" dragon --wlm ssh --network-config my_cluster_config.json --network-prefix '' my_user_code.py

.. _transport_agents:

Dragon's Transport Agents
=========================

To facilitate cross node communications when running in a multi-node mode, Dragon provides a couple of
different Transport Agents.

.. _hsta_transport_agent:

High Speed Transport Agent (HSTA)
---------------------------------

The HSTA is new in Dragon 0.4. The HSTA transport is an RDMA based transport agent that
combines MPI-like performance using Dragon Channels. There are no network ports to configure
for HSTA, but it does depend on Cray-MPICH being installed on the system.

The HSTA transport agent is currently not available in the opensource version of Dragon. For
inquiries about Dragon's high speed RDMA-based transport, please contact HPE by emailing
dragonhpc@hpe.com.

.. _tcp_transport_agent:

TCP-based Transport Agent
-------------------------

As of Dragon 0.5, the TCP-based transport agent is the default transport agent
for the Dragon opensource package. The TCP transport agent utilizes standard TCP
for inter-node communication through Dragon Channels.

When using a version of Dragon that includes the HSTA transport agent and you prefer to
use the TCP transport agent, the `--transport tcp` option can be passed to the launcher (see:
:ref:`FAQ <Transport FAQ>` and :ref:`Launcher options <Dragon CLI Options>`).

The TCP agent is configured to use port 7575 by default. If that port is blocked,
it can be changed with the `--port` argument to `dragon`. If not specific,
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


**KNOWN ISSUE**: If a `--network-prefix` argument is given that doesn't actually exist, the Dragon runtime will enter
a hung state. This will be fixed in future releases. For now, a `ctrl+z` and `kill` will be necessary to recover.

Dragon Logging
==============
The Dragon runtime has extensive internal logging for its services. For performance reasons, this is disabled by
default. However for debugging, various levels of logging can be requested via `--log-level`. The specific levels match
those in `Python's logging module`_. As some examples:

.. code-block:: bash

    # No runtime logging:
    $ dragon p2p_lat.py --iterations 100 --lg_max_message_size 12 --dragon

    # log messages of ERROR and CRITICAL level will be output to both stderr and dragon log
    # file in pwd. No logging will be output to runtime actor-specific files
    $ dragon -l ERROR program.py : Only file. No logging will be output the the actor files.

    # INFO, WARNING, ERROR and CRITICAL level will be output to both stderr and dragon log
    # file. No logging will be output to runtime actor-specific files
    $ dragon -l INFO program.py

    # INFO, WARNING, ERROR and CRITICAL level will be output to both stderr and dragon log
    # file. The runtime actor log files will contain all log messages, up to and including
    # DEBUG level.
    $ dragon -l DEBUG program.py

    # INFO, WARNING, ERROR and CRITICAL level will only be output to stderr. No dragon log
    # file will be created
    $ dragon -l stderr=INFO program.py

    # ERROR and CRITICAL level will only be output to stderr. Log messages of INFO, WARNING,
    # ERROR and CRITICAL level will only be output to the dragon log file.
    $ dragon -l stderr=ERROR -l dragon_file=INFO program.py

.. External links

.. _srun: https://slurm.schedmd.com/srun.html
.. _salloc: https://slurm.schedmd.com/salloc.html
.. _sbatch: https://slurm.schedmd.com/sbatch.html
.. _Python's logging module: https://docs.python.org/3/library/logging.html#levels