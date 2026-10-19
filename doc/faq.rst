.. _faq:

FAQ
+++

This page collects quick answers to common setup, runtime, and performance
questions. For longer tutorials and walkthroughs, continue to :ref:`uses` or
the :ref:`developer-guide`.

Getting Started
===============

How do I convert my Python Multiprocessing program to Dragon ?
==============================================================

See :ref:`devguide/multiprocessing:Multiprocessing with Dragon`.


How do I debug my Dragon program ?
==================================

Use the `-l` option when running Dragon, e.g., `dragon -l DEBUG my_app.py`.
Possible log levels are `NONE`, `DEBUG`, `INFO`, `WARNING`, `ERROR`, or `CRITICAL`.
The Dragon logs will show debug output from all services alongside your program.

You can also pair this with the debugging guidance in :ref:`debugging` when you
need a broader workflow for inspecting runtime behavior.

Configuration and Runtime
=========================


How can I change the size of the default memory pool ?
======================================================


The size of the default memory pool in bytes is controlled by the environment
variable `DEFAULT_SEG_SZ` during startup. The default is `2^32` or four GBytes
per :term:`node <Node>`. If we want to increase the memory to eight GBytes, we set the
variable as follows in bash: `export DRAGON_DEFAULT_SEG_SZ=8589934592`.

How do I find the p_uid of the current process ?
================================================

* Python Multiprocessing: `multiprocessing.current_process().pid`. Note that Dragon replaces the OS `pid` with the `p_uid`, as the `pid` is not unique on a :term:`Distributed System`.
* Dragon Native: `dragon.native.process.current()`
* Dragon Client & Infrastructure: `dragon.infrastructure.parameters.this_process.my_puid`

.. _Transport FAQ:

How do I enable the (TCP|RDMA)-based transport ?
================================================

Dragon uses a transport agent for off-node communication. On a single node, no
transport configuration is required.

On multiple nodes, Dragon uses the high speed transport agent (HSTA) when HSTA
is available. HSTA selects a backend at runtime.

To prefer a specific backend, use ``dragon-config``:

.. code-block:: bash

    # Prefer HSTA's TCP backend
    dragon-config add --tcp-runtime

    # Prefer a UCX backend for HSTA
    dragon-config add --ucx-runtime-lib=/path/to/libucp-containing-directory

    # Prefer an OFI backend for HSTA
    dragon-config add --ofi-runtime-lib=/path/to/libfabric-containing-directory

If a high-speed backend is configured and available, HSTA uses that backend.
If no high-speed backend is available, HSTA falls back to its TCP backend.

Setting ``--tcp-runtime`` expresses a preference for HSTA's TCP backend when no
RDMA backend has been configured. The separate asyncio-based TCP transport agent
is used only when HSTA is unavailable or when the TCP transport agent is selected
explicitly with ``dragon -t tcp``.

The installation guide includes the full transport setup discussion in
:ref:`installation-guide`.


Where can I find the Environment Variables that control the Dragon Run-time ?
=============================================================================
.. this will eventually point to ref/inf/dragon.infrastructure.parameters:LaunchParameters
.. which is far less easy to find than a section in the programming guide.
See :any:`dragon.infrastructure.parameters.LaunchParameters`.


What do I do if I need to clean up any Dragon processes or experiments that did not complete properly?
============================================================================================================

In the event your experiment goes awry, we provide a helper script,
`dragon-cleanup`, to clean up any zombie processes and memory. The script
`dragon-cleanup` is placed in the `[dragon install dir]/bin` and added to the
`$PATH` environment variable after loading the Dragon module.

Performance
===========


Can I use Dragon to manage MPI jobs?
====================================
This functionality is currently only available on systems where the HPE Cray MPITCH library
is installed.

How can I get the best performance from my Distributed Dictionary?
====================================================================
There are a number of :ref:`performance tips and tricks <ddictperformance>` and
in the enclosing section on the :ref:`Design of the Distributed Dictionary <ddictdesign>`,
where many examples are also provided.