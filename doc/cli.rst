.. _command-line-interface:

Command Line Interface
++++++++++++++++++++++

Dragon provides a couple of different command line interfaces (CLIs) that allows users to interact with
the Dragon runtime and its components.

* :ref:`dragon` - The ``dragon`` command is used to start the Dragon runtime services and user applications.
* :ref:`dragon-config` - The ``dragon-config`` is used to set configuration options for the Dragon runtime.

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
