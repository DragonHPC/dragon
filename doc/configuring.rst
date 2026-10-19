.. _configuring-dragon:

How to Tame your Dragon
+++++++++++++++++++++++++++

.. image:: images/tamedragon.png
   :align: right
   :width: 400px
   :alt: A Tame Dragon


This page provides information about configuring various aspects of the Dragon
run-time services. Dragon makes every attempt to configure itself to run right
out of the box. However, it does need to have :ref:`shared-memory` configured,
especially when Dragon is run in a container or kubernetes cluster.

The following sections describe how various aspects of Dragon can be configured
and/or controlled via environment variables. Please read as appropriate for your
needs.

.. contents:: Table of Contents
    :local:

.. _shared-memory:

Shared Memory
============================

Dragon runs in either a single-node mode or a multi-node mode. On-node, in
single-node or multi-node mode, Dragon uses shared memory to communicate between
processes that are co-located with it on the same node. This means that the
environment it runs in must be configured to support shared memory. Linux and Mac
OS X support shared memory out of the box.

Running inside a container requires a little extra setup. Docker reserves 64K of
memory for shared memory by default. That is not enough to run Dragon. Dragon
requires around 6GB of shared memory be configured. Depending on its workload it
may use much less, but Dragon certainly uses more than 64K. Don't worry, you can
reserve more than you need. Unless Dragon actually uses all of what you reserve,
the memory of your node is still available for other things. Reservation of
shared memory simply means it is allowed to grow to a specific size, not that it
necessarily will.

When a Docker container is started, you specify the amount of shared memory it
should get with a flag for the amount of shared memory reserved. We have an
example of our single-node Dockerfile in our repo `which can be found here
<https://github.com/DragonHPC/dragon/blob/main/.devcontainer/single-node/Dockerfile>`_.
If you are running the docker command outside of VSCode or some other
application, if you run the docker command using the `--shm-size` argument you
can specify the amount of shared memory.

.. code-block:: console

    docker run -p 8888:8888 -it --shm-size=6g --ulimit core=-1 --rm quay.io/jupyter/datascience-notebook:lab-4.4.9 /bin/bash

If you should forget (or not know) to set the shared memory size for a container,
Dragon will notify you of that and will tell you to set the amount of shared
memory to something reasonable. Dragon looks at the amount of available shared
memory when creating a managed memory pool and if there is insufficient space
left when the pool is created, then it will notify you of that and refuse to
create it. This is a slightly odd check, but it means you can create pools that
together are bigger than the amount of reserved shared memory as long as you
don't fill them. This check exists to prevent Dragon from taking a core dump if
the amount of shared memory is way too small. It also is useful to know before a
long run that you may need more shared memory allocated to complete the run so
this check is a *best attempt* at sharing that information with you up front.

While Dragon is running, the amount of *available* shared memory on each node is
also monitored and if it gets to a warning level, text will appear on the console
to warn the user of what is happening and it it gets critical, then the run-time
may be taken down with an appropriate error message to say what is happening.

The up front check of available shared memory when a pool is created, in some
situations, might become a nuisance and not helpful. If that were the case, then
it might be best to disable the up front check. This can be done by setting an
environment variable to turn it off.

.. code-block:: console

    export DRAGON_CHECK_SHM_FREE_SPACE=false

Setting this environment variable will turn off the check of available free space
when a managed memory pool is created. It will not turn off the monitoring of
available free shared memory while the application is running. That can be
controlled as described in :ref:`memory-monitoring`.


.. _memory-pool-sizes:

Memory Pool Sizes
============================

When the Dragon run-time starts, it creates two managed memory pools on every
node. The *default pool* is where user data, like the contents of queues,
connections, and Dragon data structures, is allocated. The *infrastructure pool*
is reserved for messages exchanged by the Dragon services themselves.

The size of the default pool is controlled by ``DRAGON_DEFAULT_SEG_SZ`` and the
size of the infrastructure pool is controlled by ``DRAGON_INF_SEG_SZ``. Both are
set to a number of bytes and must be positive. By default the pool sizes are 4GB
(i.e. 2\ :sup:`32` bytes) and 1GB (i.e. 2\ :sup:`30` bytes) respectively.

.. code-block:: console

    # Give the default pool 16GB on each node.
    export DRAGON_DEFAULT_SEG_SZ=17179869184

If your application stores a lot of data in Dragon objects, for instance in a
distributed dictionary or in large messages sent between processes, and you see
out of memory errors coming from the default pool, then increasing
``DRAGON_DEFAULT_SEG_SZ`` is the first thing to try. Remember that these pools
live in shared memory, so the size you request must fit within the shared memory
that is configured on the node. See :ref:`shared-memory` for how to reserve more
shared memory when running in a container.

The infrastructure pool rarely needs to be changed. It only holds infrastructure
messages, so it does not grow with the size of your data.


.. _memory-monitoring:

Monitoring Node Memory
============================

While your application is running, Dragon monitors the amount of free memory on
each node. If free memory gets low, a warning is written to stderr. If it gets
critically low, Dragon will bring the run-time down with a message rather than
letting the application deadlock while waiting for memory that will never become
available.

Both the warning and the critical threshold can be expressed either as a
percentage of free memory or as a number of free bytes. Dragon computes both and
uses whichever threshold triggers at the lower number of available bytes. That
way a single default works reasonably well for both small and large nodes.

The four environment variables that control these thresholds are:

* ``DRAGON_MEMORY_WARNING_PCT`` - The percentage of free memory, between 0 and
  100, at which a warning is printed. It defaults to 10. Setting it to 100 means
  the percentage is never the deciding factor, so the byte threshold is used
  instead.
* ``DRAGON_MEMORY_WARNING_BYTES`` - The number of free bytes at which a warning
  is printed. It defaults to 6GB. Setting it to 0 means the byte count is never
  the deciding factor, so the percentage is used instead.
* ``DRAGON_MEMORY_CRITICAL_PCT`` - The percentage of free memory, between 0 and
  100, at which the run-time is taken down. It defaults to 2. Setting it to 100
  means the percentage is never the deciding factor.
* ``DRAGON_MEMORY_CRITICAL_BYTES`` - The number of free bytes at which the
  run-time is taken down. It defaults to 600MB. Setting it to 0 means the byte
  count is never the deciding factor.

For example, to warn once a node drops below 20 percent free memory and to rely
only on the percentage rather than a byte count, you could use:

.. code-block:: console

    export DRAGON_MEMORY_WARNING_PCT=20
    export DRAGON_MEMORY_WARNING_BYTES=0

Setting both the percent to 100 and the bytes to 0 for the critical threshold
disables critical out of memory handling entirely. Be aware that doing so means
an out of memory condition will likely show up as a hang that your program has to
detect and deal with on its own.

If you would rather not see the warnings at all, but still want the critical
takedown to protect you from a hang, set ``DRAGON_WARNINGS_OFF`` to any value
other than ``False``.

.. code-block:: console

    export DRAGON_WARNINGS_OFF=True

Warnings are printed by default. Even with warnings suppressed, a critical out of
memory condition still prints its message and brings the run-time down.


.. _wait-modes:

Wait Modes
============================

Whenever a process waits on a Dragon object, for instance receiving from a queue
or a connection, it does so using one of three wait modes.

* ``IDLE_WAIT`` - The process sleeps until it is notified. This uses almost no
  CPU while waiting, but it has a higher latency when the wait is satisfied.
* ``SPIN_WAIT`` - The process busy waits. This gives the lowest latency, but it
  consumes a core for the entire time it is waiting.
* ``ADAPTIVE_WAIT`` - The process spins for a short time and then falls back to
  idle waiting. This gets much of the low latency of spin waiting without
  burning a core when the wait turns out to be a long one.

There are two environment variables that select the wait mode. ``DRAGON_USER_WAIT_MODE``
applies to the objects your program creates and defaults to ``ADAPTIVE_WAIT``.
``DRAGON_INF_WAIT_MODE`` applies to objects created internally by the Dragon
services and defaults to ``IDLE_WAIT``.

.. code-block:: console

    export DRAGON_USER_WAIT_MODE=SPIN_WAIT

Choosing ``SPIN_WAIT`` makes sense when you are latency sensitive and have cores
to spare. Choosing ``IDLE_WAIT`` makes sense when you are oversubscribing cores
and would rather not have waiting processes competing with working ones. Unless
you are debugging the run-time itself, there is generally no reason to change
``DRAGON_INF_WAIT_MODE``.


.. _return-modes:

Send Return Modes
============================

When a process sends a message, Dragon can return control to the sender at
several different points in the delivery of that message. The choice trades
asynchrony against the guarantees the sender gets when the send completes.

* ``WHEN_IMMEDIATE`` - Return as soon as the send is initiated. This is the most
  asynchronous option, but it does not guarantee ordering with respect to other
  operations on the destination channel.
* ``WHEN_BUFFERED`` - Return once the message has been buffered and the sender's
  copy of the data may be reused.
* ``WHEN_DEPOSITED`` - Return once the message has been deposited into the
  destination channel.
* ``WHEN_RECEIVED`` - Return once the message has been received by the receiver.
  This is the strongest guarantee and the least asynchronous.

As with wait modes, there are two environment variables.
``DRAGON_USER_RETURN_WHEN_MODE`` applies to the objects your program creates and
defaults to ``WHEN_DEPOSITED``. ``DRAGON_INF_RETURN_WHEN_MODE`` applies to
objects created by the Dragon services and defaults to ``WHEN_BUFFERED``.

.. code-block:: console

    export DRAGON_USER_RETURN_WHEN_MODE=WHEN_BUFFERED

Relaxing the user return mode to ``WHEN_BUFFERED`` can improve throughput for
applications that send many messages, so long as your application does not depend
on a message having landed in the destination when the send call returns.


.. _gateway-capacity:

Gateway Channel Capacity
============================

On multi-node runs, off-node operations are routed through gateway channels that
the transport agent services. Each gateway channel holds a fixed number of
pending requests, and that number is set by ``DRAGON_GW_CAPACITY``. It must be a
positive integer and it defaults to 2048.

.. code-block:: console

    export DRAGON_GW_CAPACITY=8192

If your application floods the run-time with off-node operations, the gateway
channel may fill up and processes will block waiting for room in it. Raising the
capacity gives the transport agent more room to absorb bursts, at the cost of
some additional memory in the default pool on each node. This variable has no
effect on single-node runs.

.. _gpu-configuration:

Optional GPU Library Configuration
===========================================

If your nodes do not have GPUs, you can skip this section. GPU library
configuration only matters when Dragon needs to detect and use GPUs on single or
multi-node systems.

Dragon supports CUDA, HIP, and ZE (Intel Level Zero) GPU backends. To detect
GPUs, Dragon dynamically opens the vendor's runtime library at runtime. By
default, Dragon looks for the following libraries:

* ``libcudart.so`` for CUDA,
* ``libamdhip64.so`` for HIP, and
* ``libze_intel_gpu.so.1`` for ZE.

If the appropriate GPU drivers and libraries are installed on all nodes and can
be found through the ld cache or a library path environment variable such as
``LD_LIBRARY_PATH``, no configuration is required. Otherwise, use
``dragon-config`` to tell Dragon where to find the library. There are two
options for each vendor that can be used separately or together.

The ``--<vendor>-runtime-lib`` option provides a directory that contains the
runtime library:

.. code-block:: console

    dragon-config add --cuda-runtime-lib=/opt/cuda/lib64
    dragon-config add --hip-runtime-lib=/opt/rocm/lib
    dragon-config add --ze-runtime-lib=/opt/intel/oneapi/level-zero/latest/lib

The ``--<vendor>-runtime-libname`` option overrides the default library name.
It accepts either a bare library name or a full path to the library. This is
useful when the system only provides a versioned library, such as
``libcudart.so.12``, rather than the unversioned default:

.. code-block:: console

    dragon-config add --cuda-runtime-libname=libcudart.so.12
    dragon-config add --cuda-runtime-libname=/usr/lib64/libcudart.so.12
    dragon-config add --hip-runtime-libname=libamdhip64.so.6
    dragon-config add --ze-runtime-libname=libze_intel_gpu.so.1

Using these settings, Dragon locates the GPU runtime library by trying each of
the following, in order, and using the first one that is found:

1. The name or full path given by ``--<vendor>-runtime-libname``, as-is, if set.
2. If ``--<vendor>-runtime-libname`` is a full path, its bare library name via
   the ld cache or a library path environment variable (e.g.
   ``LD_LIBRARY_PATH``).
3. The bare library name from ``--<vendor>-runtime-libname`` joined with the
   ``--<vendor>-runtime-lib`` directory, if both are set.
4. The default library name joined with the ``--<vendor>-runtime-lib``
   directory, if set.
5. The default library name via the ld cache or a library path environment
   variable.

For example, with the following configuration:

.. code-block:: console

    dragon-config add --cuda-runtime-lib=/opt/cuda/lib64
    dragon-config add --cuda-runtime-libname=libcudart.so.12

Dragon tries ``libcudart.so.12`` through the normal library search path, then
``/opt/cuda/lib64/libcudart.so.12``, then ``/opt/cuda/lib64/libcudart.so``, and
finally ``libcudart.so`` through the normal library search path.

For help without referring to this documentation, you can always use
``dragon-config --help`` and ``dragon-config add --help``.


.. _transport-agent-configuration:

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
Interface (OFI) for HPE Slingshot and RoCE RDMA. However, Dragon can only use these networks if its environment is properly configured.

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


.. _hsta-tuning:

Tuning the High Speed Transport Agent
=====================================

Two environment variables control how much memory HSTA dedicates to buffering
data. Both are given in megabytes and both must be positive.

``DRAGON_HSTA_MAX_EJECTION_MB`` sets the size of the buffers used for network
receive operations, which is what limits the rate at which HSTA ejects data from
the network. It defaults to 64.

``DRAGON_HSTA_MAX_GETMSG_MB`` sets the size of the buffers used for local
``getmsg`` operations, which limits the rate at which memory is consumed for
messages using the ``GETMSG`` protocol. It defaults to 8.

.. code-block:: console

    export DRAGON_HSTA_MAX_EJECTION_MB=128
    export DRAGON_HSTA_MAX_GETMSG_MB=16

Raising these values can improve bandwidth for applications that move a lot of
large messages between nodes, but it also increases the memory HSTA holds on
each node. These variables only apply when HSTA is in use, so they have no effect
if you are running with the TCP transport agent.


.. _overlay-network:

Overlay Network
============================

In addition to the transport agent that carries your application's data, Dragon
brings up an *overlay network* that connects the launcher frontend to the backend
on each compute node. The overlay network is used for bring-up, teardown, logging
and other infrastructure traffic.

``DRAGON_OVERLAY_TRANSPORT`` selects the transport agent used for the overlay
network. It can be set to ``tcp`` or ``hsta`` and it defaults to ``tcp``. This is
normally set for you by the launcher, and the ``--overlay-transport`` command
line option to ``dragon`` takes precedence over it.

.. code-block:: console

    export DRAGON_OVERLAY_TRANSPORT=tcp

``DRAGON_OVERLAY_FANOUT`` controls the shape of the tree the frontend uses to
reach the backend nodes over the overlay network. It is the number of children
each node in that tree has, it must be a positive integer, and it defaults to 32.

.. code-block:: console

    export DRAGON_OVERLAY_FANOUT=64

A larger fanout makes the tree shallower, which reduces the number of hops
messages take, but it also means more direct connections per node. On very large
allocations, adjusting the fanout can affect how quickly the run-time starts up
and shuts down.


.. _net-conf-cache:

Network Configuration Caching
=============================

Before a multi-node run can start, Dragon must discover the network
configuration of each node in the allocation. To avoid repeating that work for
every run in the same allocation, each node caches its own network configuration
in a file in the system temporary directory.

``DRAGON_NET_CONF_CACHE_TIMEOUT`` is the number of seconds a cached network
configuration is considered valid. It defaults to 3600, that is, one hour. A
cached configuration older than this is discarded and the configuration is
gathered again.

.. code-block:: console

    # Only trust cached network configuration for five minutes.
    export DRAGON_NET_CONF_CACHE_TIMEOUT=300

Setting the timeout to 0 effectively disables the use of the cache, since any
cached entry will immediately be considered stale.

``DRAGON_NET_CONF_CACHE_SALT`` is a string appended to the name of the cache
file. The launcher generates a random salt for each run-time and places it in
the environment so that two Dragon run-times sharing a node do not read each
other's cached configuration. You generally do not need to set this yourself, but
setting it lets you deliberately share, or deliberately isolate, a cache across
runs.


.. _multiprocessing-pool:

The multiprocessing Pool Implementation
=======================================

Dragon provides two implementations of ``multiprocessing.Pool``, and
``DRAGON_BASEPOOL`` selects which one is used. It must be set to either
``NATIVE`` or ``PATCHED`` and it defaults to ``NATIVE``.

``NATIVE`` is the Dragon native implementation of the pool. ``PATCHED`` is the
standard Python multiprocessing pool with Dragon's objects underneath it and a
patched ``terminate_pool`` method.

.. code-block:: console

    export DRAGON_BASEPOOL=PATCHED

The native pool is the better performing of the two and is the right choice for
almost all applications. Selecting ``PATCHED`` is worth trying if you are porting
an existing multiprocessing application that depends on some behavior of the
standard pool that the native pool does not reproduce.