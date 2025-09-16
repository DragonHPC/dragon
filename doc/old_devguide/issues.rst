Known Issues
++++++++++++

For any issues you encounter, it is recommended that you run with a higher level of debug
output. It is often possible to find the root cause of the problem in the output from the
runtime. We also ask for any issues you wish to report that this output be included in the
report. To learn more about how to enable higher levels of debug logging refer to
:ref:`uguide/running_dragon:Running Dragon` .

* If you want to use Dragon with an Anaconda distribution of Python, there will be
  libstdc++ ABI incompatibilities.  A workaround is to move/rename
  [anaconda]/lib/libstdc++.so* to a subdirectory or other name.  The desired version
  of libstdc++.so should come from GCC 9 or later.

* Dragon Managed Memory, a low level component of the Dragon runtime, uses shared memory.
  It is possible with this pre-alpha release that things go wrong while the runtime
  is coming down and files are left in /dev/shm.  It is good to check if files are left
  there if the runtime is killed or forced down without a clean exit.  If multiprocessing
  features are used that are not yet fully supported, such as Pool, it is possible for
  Python processes to be left behind after the runtime exits.

* It is possible for a user application or workflow to exhaust memory resources in Dragon
  Managed Memory without the runtime detecting it. Many allocation paths in the runtime use
  "blocking" allocations that include a timeout, but not all paths do this if the multiprocessing
  API in question doesn't have timeout semantics on an operation. When this happens, you
  may observe what appears to be a hang. If this happens, try increasing the value of the
  DRAGON_DEFAULT_SEG_SZ environment variable to larger sizes (default is 4 GB, try increasing
  to 16 or 32 GB). Note this variable takes the number of bytes.

* Python multiprocessing applications that switch between start methods may fail with this
  due to how Queue is being patched in.  The issue will be addressed in a later update.

* If there is a firewall blocking port 7575 between compute nodes, `dragon` will hang. You
  will need to specify a different port that is not blocked through the `--port` option to
  `dragon`. Additionally, if you specify `--network-prefix` and Dragon fails to find a match
  the runtime will hang during startup. Proper error handling of this case will come in a later
  release.

* In the event your experiment goes awry, we provide a helper script, `dragon-cleanup`, to clean up
  any zombie processes and memory. The script `dragon-cleanup` is placed in the `[dragon install dir]/bin` and added to the
  `$PATH` environment variable after loading the Dragon module.