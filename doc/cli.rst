.. _command-line-interface:

Command Line Interface
++++++++++++++++++++++

Dragon provides several command line interfaces (CLIs) for starting the runtime,
configuring an allocation, and launching work across one or more nodes. If you
are new to Dragon, the usual entry point is the :ref:`dragon` command: use it
to run a Python program under the Dragon runtime.

Choose The Right CLI
====================

Use the command that matches the job you are trying to do:

* :ref:`dragon` - The ``dragon`` command is used to start the Dragon runtime services and user applications.
* :ref:`dragon-config` - The ``dragon-config`` is used to set configuration options for the Dragon runtime.
* :ref:`dragon-cleanup` - The ``dragon-cleanup`` command is used to clean up Dragon runtime services and user applications in either a single or multi-node environment.
* :ref:`drun` - The ``drun`` command launches applications on a set of hosts using an SSH fanout tree. It auto-detects Workload Manager allocations (Slurm, PBS) or accepts hosts via ``--hostlist`` / ``--hostfile``.
* :ref:`dhosts` - The ``dhosts`` command defines the host list for other Dragon tools. It generates a temporary hostfile, exports the ``DRAGON_RUN_NODEFILE`` environment variable, and spawns an interactive subshell so that downstream tools (e.g. ``dragon-cleanup``) automatically use that host list.

.. _dragon:

dragon
======

Use ``dragon`` when you want the Dragon runtime to bring up its services and run
your application for you. This is the best default for development
and for any script that already uses Dragon-aware multiprocessing.

.. code-block:: console

    dragon my_script.py

.. argparse::
    :ref: dragon.launcher.launchargs.get_parser
    :prog: dragon

.. _dragon-config:

dragon-config
=============

Use ``dragon-config`` to inspect runtime settings or prepare configuration
changes before launching a job.

.. code-block:: console

    dragon-config --help

.. argparse::
    :ref: dragon.infrastructure.config._configure_parser
    :prog: dragon-config

.. _dragon-cleanup:

dragon-cleanup
==============

Use ``dragon-cleanup`` after a failed or interrupted run when you need to remove
stale runtime services before starting again.

.. code-block:: console

    dragon-cleanup --help

.. argparse::
    :ref: dragon.tools.dragon_cleanup.get_drun_parser
    :prog: dragon-cleanup

.. _drun:

drun
====

Use ``drun`` to launch an application on multiple hosts via an SSH fanout tree.
It automatically detects active Workload Manager allocations (Slurm, PBS) or
accepts a manual host list via ``--hostlist`` or ``--hostfile``. Note that
``drun`` is not a replacement for ``dragon``: it does not start the Dragon runtime services,
so you must ensure that the runtime is already running on all hosts before using ``drun``.

.. code-block:: console

    drun --hostlist host1,host2,host3 my_executable --option1

.. argparse::
    :ref: dragon.tools.dragon_run.drun.get_parser
    :prog: drun

.. _dhosts:

dhosts
======

Use ``dhosts`` to define a host list for other Dragon runtime tools. It generates
a temporary hostfile, exports the ``DRAGON_RUN_NODEFILE`` environment variable,
and spawns an interactive subshell. Any Dragon tool launched from that shell
(e.g. ``dragon-cleanup``) will automatically target the defined hosts without
needing to re-specify them.

.. code-block:: console

    dhosts --hostlist host1,host2,host3

.. argparse::
    :ref: dragon.tools.dragon_run.dhosts.get_parser
    :prog: dhosts

What To Read Next
=================

* :ref:`installation-guide` - install Dragon and verify the launcher is available.
* :ref:`getting-started` - run a first Dragon program before moving to multi-node execution.
* :ref:`multinode` - learn how to run across more than one node.
* :ref:`debugging` - use logging and runtime inspection when a launch does not behave as expected.