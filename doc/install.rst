.. _installation-guide:

Installation
++++++++++++

Dragon currently requires a minimum python version of 3.10 with support for 3.11 and 3.12. Otherwise, just do a pip
install:

.. code-block:: console

    pip3 install dragohpc

After doing the `pip3` install of the package, you have completed the prerequisites for running Dragon multiprocessing
programs.

Dragon is built with `manylinux2014` support and should function on most Linux distros.

Configuring the High Performance Network Backend
================================================

Dragon includes two separate network backend services for communication across compute nodes. The first is referred as
the "TCP transport agent". This backend uses common TCP to perform any communication over the compute network. However,
this backend is relatively low performing and can be a perforamnce bottleneck.

Dragon also includes the "High Speed Transport Agent (HSTA)", which supports UCX for Infiniband networks and OpenFabrics
Interface (OFI) for HPE Slingshot. However, Dragon can only use these networks if its envionment is properly configured.

To configure HSTA, use `dragon-config` to provide an "ofi-runtime-lib" or "ucx-runtime-lib". The input should be a
library path that contains a `libfabric.so` for OFI or a `libucp.so for UCX`. These are libraries are dynamically opened
by HSTA at runtime. Without them, dragon will fallback to using the lower performing TCP transport agent.

Example configuration commands appear below:

.. code-block:: console

    # For a UCX backend, provide a library path that contains a libucp.so:
    dragon-config -a "ucx-runtime-lib=/opt/nvidia/hpc_sdk/Linux_x86_64/23.11/comm_libs/12.3/hpcx/hpcx-2.16/ucx/prof/lib"

    # For an OFI backend, provide a library path that contains a libfabric.so:
    dragon-config -a "ofi-runtime-lib=/opt/cray/libfabric/1.22.0/lib64"

As mentioned, if `dragon-config`` is not run as above to tell Dragon where to appropriate libraries exist, Dragon will
fall back to using the TCP transport agent. You'll know this because a message similar to the following will print to
stdout:

.. code-block:: console

    Dragon was unable to find a high-speed network backend configuration.
    Please refer to `dragon-config --help`, DragonHPC documentation, and README.md
    to determine the best way to configure the high-speed network backend to your
    compute environment (e.g., ofi or ucx). In the meantime, we will use the
    lower performing TCP transport agent for backend network communication.

If you get tired of seeing this message and plan to only use TCP communication over ethernet, you can use the following
`dragon-config` command to silence it:

.. code-block:: console

    dragon-config -a 'tcp-runtime=True'

For help without referring to this README.md, you can always use `dragon-config --help`.

Running
=======

TODO: quick overview and then point to the longer deployment howto.

.. code-block:: console

    dragon myprog.py

:ref:`getting-started`