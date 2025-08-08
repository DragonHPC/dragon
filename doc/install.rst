.. _installation-guide:

Installation
++++++++++++

Dragon currently requires a minimum python version of 3.10 with support for 3.11
and 3.12. To install it, do a pip install of the package. You may wish to
create a python virtual environment prior to doing the pip install so you can
install dragonhpc and other packages within that environment.

.. code-block:: console

    pip3 install dragonhpc

After doing the `pip3` install of the package, you have completed the
prerequisites for running Dragon multiprocessing programs.

Dragon is built with `manylinux2014` support and should function on most Linux
distros.

Configuring a Transport Agent
================================================

On multi-node systems it is necessary to configure Dragon to handle off-node
communication, which Dragon does automatically through a transport agent. If
running Dragon single-node, no transport agent configuration is required.

Dragon includes two separate transport agents for communication across compute
nodes. The "TCP transport agent" provides TCP communication between nodes over
the interconnection network. The TCP transport is the lower performing of the two
agents, but still useful for many applications.

Dragon also includes a "High Speed Transport Agent (HSTA)", which supports UCX for Infiniband networks and OpenFabrics
Interface (OFI) for HPE Slingshot. However, Dragon can only use these networks if its envionment is properly configured.

To configure the transport agent run `dragon-config` with the appropriate arguments. While Dragon will fall
back to the TCP agent, if `dragon-config` is not run, an annoying message about configuring the transport agent will be
displayed.

To configure HSTA, use `dragon-config` to provide an "ofi-runtime-lib" or "ucx-runtime-lib". The input should be a
library path that contains a `libfabric.so` for OFI or a `libucp.so for UCX`. These libraries are dynamically opened
by HSTA at runtime.

Some example high-speed transport agent configuration commands are:

.. code-block:: console

    # For UCX communication, provide a library path that contains a libucp.so:
    dragon-config -a "ucx-runtime-lib=/opt/nvidia/hpc_sdk/Linux_x86_64/23.11/comm_libs/12.3/hpcx/hpcx-2.16/ucx/prof/lib"

    # For OFI communication, provide a library path that contains a libfabric.so:
    dragon-config -a "ofi-runtime-lib=/opt/cray/libfabric/1.22.0/lib64"

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

        dragon-config -a 'tcp-runtime=True'

If you get tired of seeing this message and plan to only use TCP communication over ethernet, follow the
directions above and run the `dragon-config` command to silence it with:

.. code-block:: console

    dragon-config -a 'tcp-runtime=True'

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
