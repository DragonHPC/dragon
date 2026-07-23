.. _installation-guide:

Installation
++++++++++++

Dragon currently supports Python 3.11, 3.12, and 3.13. The published package is
built for Linux environments with ``manylinux2014`` compatibility, so most users
will install Dragon into a Linux virtual environment on a workstation, server,
or HPC system. As of v0.14.1 whls for MacOS are also provided on PyPI.

System Requirements
===================

Before installing Dragon, make sure you have:

* a Linux environment,
* Python 3.11 or newer, and
* a Python environment where command-line tools installed by ``pip`` are on your ``PATH``.

Quick Install
=============

For a first local or single-node run, install the package with ``pip``. Using a
virtual environment is recommended so Dragon and its optional packages stay
isolated from the rest of your system.

.. code-block:: console

    pip install dragonhpc

After the installation completes, you have everything needed to run Dragon
multiprocessing programs on a single node.

Extra requirements
===================

The Dragon package has some optional dependencies that are not installed by default.
These dependencies are not required for basic Dragon functionality, but they are needed
for certain features or workflows, such as AI workloads or telemetry.
To install the optional dependencies for AI workloads, run:

.. code-block:: console

    pip install dragonhpc[ai]

And telemetry dependencies can be installed via

.. code-block:: console

    pip install dragonhpc[telemetry]

If you were interested in installing both sets of those dependencies, a comma
separated list will achieve the result:

.. code-block:: console

    pip install dragonhpc[ai,telemetry]

For a complete list of the currently supported extra requirement tags, refer to
`extra-requirements.txt`_ in the open source repository.

.. _extra-requirements.txt: https://github.com/DragonHPC/dragon/blob/main/src/extra-requirements.txt

Basic Run Check
===============

Once the package is installed, verify that the launcher is available:

.. code-block:: console

    dragon --help

If that command works, your installation is ready for the examples in
:ref:`getting-started`.

Shell Tab Completions
=====================

Dragon supports shell tab completion for its primary user-facing CLI tools in bash and
zsh shells. Tab completions allow you to quickly complete commands, options,
and arguments by pressing the tab key, improving your command-line efficiency and user
experience. To use this feature, you need to install the shell completions after
installing Dragon by running the `dragon-install-completions` command.

.. code-block:: console

   dragon-install-completions


This script will:

1. Generate completion scripts for each of the Dragon CLI tools.
2. Store them in ``~/.dragon/completions/``
3. Add sourcing directives to your shell's rc file (``~/.bashrc``, ``~/.zshrc``)

After installation, reload your shell or run ``source ~/.bashrc`` (or equivalent) to
enable completions.

Optional Multi-Node Transport Configuration
===========================================

If you are running on a single node, you can skip this section. Transport-agent
configuration matters when Dragon needs to communicate across compute nodes.

On multi-node systems it is necessary to configure Dragon to handle off-node
communication, which Dragon does automatically through a transport agent. If
running Dragon single-node, no transport agent configuration is required.

Dragon includes two separate transport agents for communication across compute
nodes. The "TCP transport agent" provides TCP communication between nodes over
the interconnection network. The TCP transport is the lower performing of the two
agents, but still useful for many applications.

Dragon also includes a "High Speed Transport Agent (HSTA)", which supports UCX for Infiniband networks and OpenFabrics
Interface (OFI) for HPE Slingshot. However, Dragon can only use these networks if its environment is properly configured.

To configure the transport agent run `dragon-config` with the appropriate arguments. While Dragon will fall
back to the TCP agent, if `dragon-config` is not run, an annoying message about configuring the transport agent will be
displayed.

To configure HSTA, use `dragon-config` to provide an "ofi-runtime-lib" or "ucx-runtime-lib". The input should be a
library path that contains a `libfabric.so` for OFI or a `libucp.so` for UCX. These libraries are dynamically opened
by HSTA at runtime.

Some example high-speed transport agent configuration commands are:

.. code-block:: console

    # For UCX communication, provide a library path that contains a libucp.so:
    dragon-config add --ucx-runtime-lib=/opt/nvidia/hpc_sdk/Linux_x86_64/23.11/comm_libs/12.3/hpcx/hpcx-2.16/ucx/prof/lib

    # For OFI communication, provide a library path that contains a libfabric.so:
    dragon-config add --ofi-runtime-lib=/opt/cray/libfabric/1.22.0/lib64

As mentioned, if `dragon-config` is not run to tell Dragon where the appropriate libraries exist, Dragon will
fall back to using the TCP transport agent. You'll know this because a message similar to the following will print:

.. code-block:: console

    By default Dragon looks for a configuration file to determine which transport
    agent implementation to use. However, no such configuration file was found.
    Please refer to `dragon-config --help`, DragonHPC documentation, and README.md
    to determine the best way to configure the transport agent for your
    compute environment. In the meantime, Dragon will use the TCP transport agent
    for network communication. To eliminate this message and continue to use
    the TCP transport agent, run:

        dragon-config add --tcp-runtime=True

If you get tired of seeing this message and plan to only use TCP communication over ethernet, follow the
directions above and run the `dragon-config` command to silence it with:

.. code-block:: console

    dragon-config add --tcp-runtime=True

For help without referring to this README.md, you can always use `dragon-config --help`.

Running
=======

Dragon employs a launcher to bring up the Dragon run-time environment and run
your program. To start your Dragon program you run `dragon <prog>` where `<prog>`
is your program. If the program ends in .py and is not executable, Dragon will
use Python to run the program, as depicted below. If the program is executable,
Dragon will run the program itself.

.. code-block:: console

    dragon prog.py

For in-depth directions on running Dragon multi-node, refer to :ref:`multinode`.

Troubleshooting
===============

**"ModuleNotFoundError: No module named 'dragon'"**
    Ensure you have run ``pip install dragonhpc`` and that you are using the correct Python
    environment. If using a virtual environment, make sure it is activated.

**"dragon: command not found"**
    The ``dragon`` launcher is installed with the package. Ensure your Python environment's ``bin/``
    directory is in your ``$PATH``. You can check with ``which dragon``.

**Transport agent configuration message appears at runtime**
    This is expected on multi-node systems without a configured transport agent. Run
    ``dragon-config add --tcp-runtime=True`` to silence the message and use TCP, or configure
    HSTA as described above for high-speed transport.

**Dragon processes not cleaning up after a crash**
    Use the ``dragon-cleanup`` helper script to remove zombie processes and stale shared memory:

    .. code-block:: console

        dragon-cleanup

What's Next?
============

After completing installation:

1. **Try a simple example** — Run the introductory code in :ref:`getting-started` to verify your setup.
2. **Learn core concepts** — Review Pool, Queue, DDict, and more in :ref:`getting-started`.
3. **Run on multiple nodes** — For HPC deployments, see :ref:`uses/multinode:Running Dragon on Multiple Nodes`.
4. **Explore your use case** — Visit :ref:`uses` to find tutorials matching your application type.
