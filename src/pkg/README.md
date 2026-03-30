Dragon
======

Dragon is a distributed environment for developing high-performance tools,
libraries, and applications at scale. This distribution package provides the
necessary components to run the Python multiprocessing library using the Dragon
implementation which provides greater scaling and performance improvements over
the legacy multiprocessing that is currently distributed with Python.

For examples and the actual source code for Dragon, please visit its github
repository: <https://github.com/DragonHPC/dragon>

Installing Dragon
------------------------

Dragon currently requires a minimum python version of 3.10 with support
for 3.11 and 3.12. Otherwise, just do a `pip install`:

    pip install dragonhpc

After doing the `pip install` of the package, you have
completed the prerequisites for running Dragon multiprocessing programs.

Dragon is built with `manylinux2014` support and should function on most Linux
distros.

Configuring Dragon's high performance network backend for HSTA
--------------------------------------------------------------

Dragon includes two separate network backend services for communication across compute nodes.
The first is referred as the "TCP transport agent". This backend uses common TCP to 
perform any communication over the compute network. However, this backend is relatively
low performing and can be a perforamnce bottleneck.

Dragon also includes the "High Speed Transport Agent (HSTA)", which supports UCX for 
Infiniband networks and OpenFabrics Interface (OFI) for HPE Slingshot. However,
Dragon can only use these networks if its envionment is properly configured. 

To configure HSTA, use `dragon-config` to provide an "ofi-runtime-lib" or "ucx-runtime-lib".
The input should be a library path that contains a `libfabric.so` for OFI or a 
`libucp.so` for UCX. These are libraries are dynamically opened by HSTA at runtime.
Without them, dragon will fallback to using the lower performing TCP transport agent

Example configuration commands appear below:

```
# For a UCX backend, provide a library path that contains a libucp.so:
dragon-config add --ucx-runtime-lib=/opt/nvidia/hpc_sdk/Linux_x86_64/23.11/comm_libs/12.3/hpcx/hpcx-2.16/ucx/prof/lib

# For an OFI backend, provide a library path that contains a libfabric.so:
dragon-config add --ofi-runtime-lib=/opt/cray/libfabric/1.22.0/lib64
```

As mentioned, if `dragon-config` is not run as above to tell Dragon where to
appropriate libraries exist, Dragon will fall back to using the TCP transport
agent. You'll know this because a message similar to the following will
print to stdout:

```
Dragon was unable to find a high-speed network backend configuration.
Please refer to `dragon-config --help`, DragonHPC documentation, and README.md
to determine the best way to configure the high-speed network backend to your
compute environment (e.g., ofi or ucx). In the meantime, we will use the
lower performing TCP transport agent for backend network communication.
```

If you get tired of seeing this message and plan to only use TCP communication
over ethernet, you can use the following `dragon-config` command to silence it:

```
dragon-config add --tcp-runtime=True
```

For help without referring to this README.md, you can always use `dragon-config --help`


Running a Program using Dragon and python multiprocessing
---------------------------------------------------------

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

Sanity check Dragon installation
--------------------------------

Grab the following from the DragonHPC github by cloning the repository or a quick wget: [p2p_lat.py](https://raw.githubusercontent.com/DragonHPC/dragon/refs/heads/main/examples/multiprocessing/p2p_lat.py)

```
wget https://raw.githubusercontent.com/DragonHPC/dragon/refs/heads/main/examples/multiprocessing/p2p_lat.py .
```

If testing on a single compute node/instance, you can just do:
```
dragon p2p_lat.py --dragon
using Dragon
Msglen [B]   Lat [usec]
2  28.75431440770626
4  39.88605458289385
8  37.25141752511263
16  43.31085830926895
+++ head proc exited, code 0
```

If you're trying to test the same across two nodes connected via a high speed network,
try to get an allocation via the workload manager first and then run the test, eg:
```
salloc --nodes=2 --exclusive
dragon p2p_lat.py --dragon
using Dragon
Msglen [B]   Lat [usec]
2  73.80113238468765
4  73.75898555619642
8  73.52533907396719
16  72.79851596103981
```

Environment Variables
---------------------

DRAGON_DEBUG - Set to any non-empty string to enable more verbose logging

DRAGON_DEFAULT_SEG_SZ - Set to the number of bytes for the default Managed Memory Pool.
                        The default size is 4294967296 (4 GB).  This may need to be
                        increased for applications running with a lot of Queues or Pipes,
                        for example.

Requirements
------------

- Python 3.10 
- GCC 9 or later
- Slurm or PBS+PALS (for multi-node Dragon)

Known Issues
------------

For any issues you encounter, it is recommended that you run with a higher level of debug
output. It is often possible to find the root cause of the problem in the output from the
runtime. We also ask for any issues you wish to report that this output be included in the
report. To learn more about how to enable higher levels of debug logging refer to
`dragon --help`.

Dragon Managed Memory, a low level component of the Dragon runtime, uses shared memory.
It is possible that things go wrong while the runtime is coming down and files are 
left in /dev/shm. Dragon does attempt to clean these up in the chance of a bad exit, 
but it may not succeed. In that case, running `dragon-cleanup` on your own will clean up 
any zombie processes or un-freed memory.

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
up any zombie processes and memory.
