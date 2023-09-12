Running Dragon
++++++++++++++

Launching a Dragon program is done in a similar fashion to starting a program
with a workload manager, eg: `srun`_. In Dragon's case, it is invoked via
`dragon`. Its help and basic usage appears below:

.. _Dragon CLI Options:

.. autodocstringonly:: dragon.launcher.launch_selector.main

In the event your experiment goes awry, we provide a helper script, `dragon-cleanup`, to clean up any zombie processes and memory.
The script `dragon-cleanup` is placed in the `[dragon install dir]/bin` and added to the `$PATH` environment variable after loading the Dragon module.

Running Dragon on a Multi-Node System
=====================================

To launch a Dragon program on several compute nodes, a Slurm job allocation obtained via `salloc`_ or `sbatch`_ is required, eg:

.. code-block:: bash

    $ salloc --nodes=2
    $ dragon p2p_lat.py --iterations 100 --lg_max_message_size 12 --dragon

If the user attempts to execute on a slurm-enabled system, but without an active allocation, an exception is raised, and the program will not execute:

.. code-block:: bash

    # No salloc allocation exists:
    $ dragon p2p_lat.py --iterations 100 --lg_max_message_size 12 --dragon

    RuntimeError: Executing in a Slurm environment, but with no job allocation.
    Resubmit as part of an 'salloc' or 'sbatch' execution

To override this default behavior and execute a Dragon program on the same node as your shell, the `--single-node-override` option is available.

The Dragon service runtime assumes all nodes in an allocation are to be used unless the `--node-count` option is used.
This limits the user program to executing on a smaller subset of nodes, potentially useful for execution of scaling
benchmarks. For example, if the user has a job allocation for 4 nodes, but only wants to use 2 for their Dragon program,
they may do the following:

.. code-block:: bash

    $ salloc --nodes=4
    $ dragon --nodes 2 p2p_lat.py --iterations 100 --lg_max_message_size 12 --dragon



RDMA-based Transport Agent (HSTA)
---------------------------------

Please see the :ref:`FAQ <Transport FAQ>` for more information.

TCP-based Transport Agent
-------------------------

The TCP-based transport agent is the default transport service used for
inter-node communication through Channels. The `--transport tcp` or `--transport
hsta` option can be passed to the launcher to explicitly set the desired
transport. In the open source implementation, the TCP transport is the only
choice (see: :ref:`FAQ <Transport FAQ>` and :ref:`Launcher options <Dragon CLI
Options>`). The TCP agent is configured to use port 7575 by default. If that port
is blocked, it can be changed with the `--port` argument to `dragon`. If not
specific, 7575 is used:, eg:

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

Network Configuration
---------------------

We provide a command line tool as part of our runtime to query the network features available on your system. It can be
invoked as follow, and the "name" keywords indicate network names that could potentially be supplied as arguments:

.. code-block:: bash

    # Executing on an Aries network with the output partially truncated. Notice use
    # of srun so the compute node network is queries rather than the login-node's
    $ srun -n1 python3 -m dragon-transport-ifaddrs --ip --no-loopback --up --running | jq

    [
      {
        "name": "ipogif0",
        "flags": [
          "IFF_LOWER_UP",
          "IFF_NOARP",
          "IFF_RUNNING",
          "IFF_UP"
        ],
        "addr": {
          "family": "AF_INET",
          "port": 0,
          "addr": "10.128.0.3"
        },
        "netmask": {
          "family": "AF_INET",
          "port": 0,
          "addr": "255.252.0.0"
        }
      },
      {
        "name": "rsip",
        "flags": [
          "IFF_LOWER_UP",
          "IFF_NOARP",
          "IFF_RUNNING",
          "IFF_POINTOPOINT",
          "IFF_UP"
        ],
        "addr": {
          "family": "AF_INET",
          "port": 0,
          "addr": "172.30.48.181"
        },
        "netmask": {
          "family": "AF_INET",
          "port": 0,
          "addr": "255.255.255.255"
        },
        "dstaddr": {
          "family": "AF_INET",
          "port": 0,
          "addr": "172.30.48.181"
        }
      }
    ]

The Dragon launcher needs to know what resources are available for its use . To obtain that information,
the Dragon launcher uses an internal network config tool that is deployed at the beginning of every launch of a Dragon job.

The launcher frontend must know what resources are available for its use on the
compute backend. To obtain that information for a given set of workload
managers, there is a network config tool in the launcher module. This tool is
exposed for general use. However, if deploying dragon on a supported workload
manager with an active job allocation, the launcher frontend will handle
obtaining the network configuration. It exists in `dragon.launcher.network_config.main`,
but can be invoked directory via `dragon-network-config`. Its help is below:

.. autodocstringonly:: dragon.launcher.network_config.main

If output to file (YAML or JSON are supported), the file can be provided to the launcher frontend at launch. Formatting
of the files appears below:

.. code-block:: bash

    # Launching Dragon with an input network config
    $ dragon --network-config pbs+pals.yaml p2p-lat.py --iterations 100 --lg_max_message_size 12 --dragon

.. code-block:: YAML

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

Dragon Logging
--------------
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