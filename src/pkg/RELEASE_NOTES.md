# Dragon Open Source Release
Dragon is now open source and available for download. The open source version
comes with the full implementation of Dragon using the TCP transport agent.

Optional Dragon extensions that further enhance Dragon's performance in HPC
environments exist; please see the FAQ on the Dragon documentation website.

# Dragon 0.6 and 0.61 Release Summary

This package is the first to extend Dragon beyond support for Python
multiprocessing. The key new feature is support for running collections of
executables, including executables that require support for PMI (e.g., MPI). PMI
support is currently limited to executables using Cray PMI, such as those linked
with Cray MPICH. The process group feature is also utilized for scalable
multiprocessing Pool, which can now scale to thousands of workers. Highlighted
new features are:

- ProcessGroup API for scalable management of processes
- Initial support for managing process requiring PMI (e.g., MPI)
- Rewrite of mp.Pool utilizing ProcessGroup
- mp.Array
- HPC workflow cookbook entry orchestrating MPI and Python processes
- Processing pipeline cookbook example
- LICENSE is now MIT

# Dragon 0.5 and 0.52 Release Summary
This package introduces a number of key features to Dragon in addition to many bug fixes, improved robustness, and
addition/refinements to documentation. Highlighted new features are:

- Initial support for Jupyter notebooks within Dragon
- mp.Barrier
- mp.Value
- Significantly improved launcher stability and ctrl-c handling
- Preview release of a distributed dictionary
- Documentation cookbook entries for an LLM inference service, Jupyter notebooks, and distributed dictionary

Note: Dragon 0.52 was released to fix a significant cosmetic bug that when triggered made it appear Dragon did not exit
cleanly. It also corrects an install issue with the distributed dictionary preview component.

# Dragon 0.4 Release Summary
This package is a significant improvement for multi-node support over v0.3. Many bugs were found and corrected for several
multiprocessing objects when used across nodes. The new RDMA-enabled transport (HSTA) is also part of this 0.4
release. We have also introduced a set of multi-node component tests that can be used to validate the installation.
These tests go above and beyond the capabilities of base multiprocessing, and Dragon does not execute them all
without error in call cases. We have skipped tests that are not yet functioning properly and will re-enable them
in future versions.

# Dragon 0.3 Release Summary
This package is the initial, pre-alpha, release of Dragon supporting most of Python multiprocessing for single and multiple
nodes.  For this release and the purposes of the standard multiprocessing unit tests, any objects that are not
meaningfully implemented over Dragon are considered "not implemented" even if they function from the base
implementation.  This is a list of objects that are implemented over Dragon, but other objects may yet function
because they rely on the base implementation.

- Process
- Pipe
- Connection
- multiprocessing.connection.wait
- Queue
- SimpleQueue
- JoinableQueue
- Pool
- Lock
- RLock
- Semaphore
- BoundedSemaphore
- Event
- Condition

The major updates from the 0.2 release of Dragon are:

- Dragon now scales across nodes through a TCP-based transport (RDMA will come in a future release)
- Multi-node support is compatible with Slurm only right now
- A wider selection of the multiprocessing API is implemented and much more of the standard multiprocessing
  unit tests pass with Dragon
- Optimizations improved on-node performance for communication objects, such as Connection
- Control over logging and debug level is available through the Dragon launcher
