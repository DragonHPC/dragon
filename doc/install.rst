.. _installation-guide:

Installation
++++++++++++

Dragon currently supports Python 3.11, 3.12, and 3.13. The published package is
built for Linux environments with ``manylinux_2_28`` compatibility, so most users
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
For single and multi-node GPU detection, ensure that the appropriate GPU drivers and libraries are installed on all nodes.
By default, Dragon will attempt to detect GPUs using the installed drivers and libraries.
To manually specify the GPU libraries used by Dragon, you can use the ``dragon-config`` command as follows:

.. code-block:: console

    dragon-config add --cuda-runtime-libname=/usr/lib64/libcudart.so.12
    dragon-config add --hip-runtime-libname=/usr/lib64/libamdhip64.so.6
    dragon-config add --ze-runtime-libname=/usr/lib64/libze_intel_gpu.so.1


For best performance with multi-node support,
you should configure the high speed transport to use UCX or OFI as follows (modifying
the path(s) as appropriate for your system):

.. code-block:: console

    # For UCX communication, provide a library path that contains a libucp.so:
    dragon-config add --ucx-runtime-lib=/opt/nvidia/hpc_sdk/Linux_x86_64/23.11/comm_libs/12.3/hpcx/hpcx-2.16/ucx/prof/lib

    # For OFI communication, provide a library path that contains a libfabric.so:
    dragon-config add --ofi-runtime-lib=/opt/cray/libfabric/1.22.0/lib64


For more info on configuring the transport agent, see :ref:`configuring-dragon`.

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

Here are some troubleshooting tips. It might also be useful to consult :ref:`configuring-dragon`
for additional help in setting up Dragon for maximum performance.

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

    This also discards the orchestrator files left behind by a distributed dictionary that
    was not destroyed. Add ``--keep-ddict-files`` to preserve them:

    .. code-block:: console

        dragon-cleanup --keep-ddict-files

What's Next?
============

After completing installation:

1. **Try a simple example** — Run the introductory code in :ref:`getting-started` to verify your setup.
2. **Learn core concepts** — Review Pool, Queue, DDict, and more in :ref:`getting-started`.
3. **Run on multiple nodes** — For HPC deployments, see :ref:`uses/multinode:Running Dragon on Multiple Nodes`.
4. **Explore your use case** — Visit :ref:`uses` to find tutorials matching your application type.
5. **Fine tune Dragon** - Consult :ref:`configuring-dragon` for further information about fine-tuning the performance of Dragon.
