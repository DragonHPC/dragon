.. _faq:

FAQ
+++

How do I convert my Python Multiprocessing program to Dragon ?
==============================================================

See :ref:`devguide/dragon_multiprocessing:Multiprocessing with Dragon`.


How do I debug my Dragon program ?
==================================

.. TODO: uguide/log.rst

Use the `-l` option when running Dragon, e.g., `dragon -l DEBUG my_app.py`.
Possible log levels are `NONE`, `DEBUG`, `INFO`, `WARNING`, `ERROR`, or `CRITICAL`.
The Dragon logs will show debug output from all services alongside your program.


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

As of v0.5, the RDMA-based transport is enabled by default. To specify use of the RDMA or TCP-based
transport explicitly, the Dragon launcher provides the `--transport` option:

.. code-block:: bash

    # Explicitly use the RDMA-based transport
    $ dragon --transport hsta my-dragon-script.py

    # Use the RDMA-based transport via the runtime's default setting
    $ dragon my-dragon-script.py

    # Use the TCP-based transport
    $ dragon --transport tcp my-dragon-script.py



Where can I find the Environment Variables that control the Dragon Run-time ?
=============================================================================
.. this will eventually point to ref/inf/dragon.infrastructure.parameters:LaunchParameters
.. which is far less easy to find than a section in the programming guide.
See :ref:`pguide/envvars:Environment Variables`.


What do I do if I need to clean up any Dragon processes or experiments that did not complete properly?
============================================================================================================

In the event your experiment goes awry, we provide a helper script,
`dragon-cleanup`, to clean up any zombie processes and memory. The script
`dragon-cleanup` is placed in the `[dragon install dir]/bin` and added to the
`$PATH` environment variable after loading the Dragon module.


Can I use Dragon to manage MPI jobs?
====================================
This functionality is currently only available on systems where the HPE Cray MPITCH library
is installed.

How can I get the best performance from my Distributed Dictionary?
====================================================================
There are a number of :ref:`performance tips and tricks <ddictperformance>` and
in the enclosing section on the :ref:`Design of the Distributed Dictionary <distdictdesign>`,
where many examples are also provided.