.. _command-line-interface:

Command Line Interface
++++++++++++++++++++++

Dragon provides a couple of different command line interfaces (CLIs) that allows users to interact with
the Dragon runtime and its components.

* :ref:`dragon` - The ``dragon`` command is used to start the Dragon runtime services and user applications.
* :ref:`dragon-config` - The ``dragon-config`` is used to set configuration options for the Dragon runtime.
* :ref:`dragon-cleanup` - The ``dragon-cleanup`` command is used to clean up Dragon runtime services and user applications in either a single or multi-node environment.
* :ref:`drun` - The ``drun`` command uses an ssh-tree to run user applications on a set of hostnames.
* :ref:`dhosts` - The ``dhosts`` command opens an interactive shell configured to run applications on a specified list of hostnames.

.. _dragon:

dragon
======

.. argparse::
    :ref: dragon.launcher.launchargs.get_parser
    :prog: dragon

.. _dragon-config:

dragon-config
=============

.. argparse::
    :ref: dragon.infrastructure.config._configure_parser
    :prog: dragon-config

.. _dragon-cleanup:

dragon-cleanup
==============

.. argparse::
    :ref: dragon.tools.dragon_cleanup.get_drun_parser
    :prog: dragon-cleanup

.. _drun:

drun
====

.. argparse::
    :ref: dragon.tools.dragon_run.drun.get_parser
    :prog: drun

.. _dhosts:

dhosts
======

.. argparse::
    :ref: dragon.tools.dragon_run.dhosts.get_parser
    :prog: dhosts