Dragon
======

Dragon is a distributed environment for developing high-performance tools,
libraries, and applications at scale. This distribution package provides the
necessary components to run the Python multiprocessing library using the Dragon
implementation which provides greater scaling and performance improvements over
the legacy multiprocessing that is currently distributed with Python.

A complete set of documentation is available in the Dragon HTML documentation, which
is delivered in a separate tar file.

Before Running a Program
------------------------

Before you can run programs using Dragon, you must set up the run-time for your
environment. You must have Python 3.9 installed and it must be in your path
somewhere.

The untarred distribution file contains several subdirectories directories
including the following. All provided commands are relative to the directory
that contains this README.md.


* The dragon-*.whl file must be pip3 installed once for your environment.

        pip3 install --force-reinstall dragon-0.7-cp39-cp39-linux_x86_64.whl

* Check and possibly update that `$PATH` has the location of pip installed
  console scripts, such as ~/.local/bin

        export PATH=~/.local/bin:${PATH}

* modulefiles - This contains module files that are needed when using Dragon.
  You must set up the environment by loading the dragon module as follows.

        module use [/path to dragon-0.7]/modulefiles
        module load dragon

  If you intend to use Dragon on your own Linux VM or an image that you
  personally installed, you may need to enable module commands by adding the
  following command to your ~/.bashrc or other login script.

        source /usr/share/modules/init/bash

  If you use a different shell, look in the `init` directory for a script for
  your shell.

* bin - The module file will set up access to the dragon command found in the
  bin directory.

* examples - This directory provides a few demonstration programs that provide
  some working examples of using multiprocessing with Dragon and of the Dragon API
  itself. Also under this directory are the standard Python multiprocessing unit
  tests packaged for easier use with Dragon.  There is a README.md in the `examples`
  directory with more information about these demonstration programs.

* dragon_unittests - This directory contains a selection of Dragon-specific unit
  tests used in the development process.  The tests are included as validation
  of a Dragon install.  Later updates will have a complete set of unit tests.

* lib - The lib directory contains the library files providing the underlying
  services needed by the Dragon implementation of multiprocessing in the pip
  installed wheel package. The module command given above sets up a
  `$DRAGON_BASE_DIR` environment variable so these shared libraries can can
  found by the run-time when running Dragon multiprocessing programs.

* test - This directory contains acceptance tests for verifying the installation
  of Dragon.

After doing the `pip3 install` and the
`module use [/path to dragon-0.7]/modulefiles && module load dragon` you have
completed the prerequisites for running Dragon multiprocessing programs.

Running a Program using Dragon
------------------------------

There are two steps that users must take to use Dragon multiprocessing.

1. You must import the dragon module in your source code and set dragon as the
   start method, much as you would set the start method for `spawn` or `fork`.

        import dragon
        import multiprocessing as mp
        ...
        if __name__ == "__main__":
            # set the start method prior to using any multiprocessing methods
            mp.set_start_method('dragon')
            ...

   This must be done for once for each application. Dragon is an API level
   replacement for multiprocessing. So, to learn more about Dragon and what it
   can do, read up on
   [multiprocessing](https://docs.python.org/3/library/multiprocessing.html).


2. You must start your program using the dragon command. This not only starts
   your program, but it also starts the Dragon run-time services that provide
   the necesssary infrastructure for running multiprocessing at scale.

        dragon myprog.py

    If you want to run across multiple nodes, simply obtain an allocation through
    Slurm (or PBS) and then run `dragon`.

        salloc --nodes=2 --exclusive
        dragon myprog.py


If you find that there are directions that would be helpful and are missing from
our documentation, please make note of them and provide us with feedback. This is
an early stab at documentation. We'd like to hear from you. Have fun with Dragon!

Dragon Open Source Release
------------------------------

Dragon is now open source and available for download. The open source version
comes with the full implementation of Dragon using the TCP transport agent.

Please read the :ref:`FAQ <Transport FAQ>` for notes about the open source release.

Environment Variables
---------------------

DRAGON_BASE_DIR - Set by `dragon` module file, the base directory of the package

DRAGON_DEBUG - Set to any non-empty string to enable more verbose logging

DRAGON_DEFAULT_SEG_SZ - Set to the number of bytes for the default Managed Memory Pool.
                        The default size is 4294967296 (4 GB).  This may need to be
                        increased for applications running with a lot of Queues or Pipes,
                        for example.

DRAGON_TRANSPORT_AGENT - Dragon will use the TCP transport agent unless this variable is set
                         to 'hsta'. When set to 'hsta' the RDMA-enabled transport will be used if available.
                         Please read the :ref:`FAQ <Transport FAQ>` for more information about available
                         transport agents.

Requirements
------------

- Python 3.9 (e.g., module load cray-python)
- GCC 9 or later
- Slurm or PBS+PALS (for multi-node Dragon)

Known Issues
------------

For any issues you encounter, it is recommended that you run with a higher level of debug
output. It is often possible to find the root cause of the problem in the output from the
runtime. We also ask for any issues you wish to report that this output be included in the
report. To learn more about how to enable higher levels of debug logging refer to
`dragon --help`.

If you want to use Dragon with an Anaconda distribution of Python, there will be
libstdc++ ABI incompatibilities.  A workaround is to move/rename
[anaconda]/lib/libstdc++.so* to a subdirectory or other name.  The desired version
of libstdc++.so should come from GCC 9 or later.

Dragon Managed Memory, a low level component of the Dragon runtime, uses shared memory.
It is possible with this pre-alpha release that things go wrong while the runtime
is coming down and files are left in /dev/shm.  It is good to check if files are left
there if the runtime is killed or forced down without a clean exit.  If multiprocessing
features are used that are not yet fully supported, such as Pool, it is possible for
Python processes to be left behind after the runtime exits.

It is possible for a user application or workflow to exhaust memory resources in Dragon
Managed Memory without the runtime detecting it. Many allocation paths in the runtime use
"blocking" allocations that include a timeout, but not all paths do this if the multiprocessing
API in question doesn't have timeout semantics on an operation. When this happens, you
may observe what appears to be a hang. If this happens, try increasing the value of the
DRAGON_DEFAULT_SEG_SZ environment variable to larger sizes (default is 4 GB, try increasing
to 16 or 32 GB). Note this variable takes the number of bytes.

Python multiprocessing applications that switch between start methods may fail with this
due to how Queue is being patched in.  The issue will be addressed in a later update.

If there is a firewall blocking port 7575 between compute nodes, `dragon` will hang. You
will need to specify a different port that is not blocked through the `--port` option to
`dragon`. Additionally, if you specify `--network-prefix` and Dragon fails to find a match
the runtime will hang during startup. Proper error handling of this case will come in a later
release.

In the event your experiment goes awry, we provide a helper script, `dragon-cleanup`, to clean
up any zombie processes and memory. The script `dragon-cleanup` is placed in the `[dragon install dir]/bin`
after running `make` or loading the `setup` script and running the `build` script.  `dragon-cleanup` is loaded
in the `$PATH` environment variable. So, all you need to do is run `dragon-cleanup`.
