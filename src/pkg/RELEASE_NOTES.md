# Dragon 0.12rc1 Release Summary
Dragon 0.12rc1 coincides with our open source release of code.

- Many small bug fixes.
- Jupyter notebook server now can be started with dragon-jupyter and now allows
  kernel restart and fixes some printing issues.
- DDict broadcast put and get for enhanced performance when all clients need a value.
- Huge cleanup and reorganization of documentation.
- Enhanced semaphore performance.
- Out of memory watcher added for user feedback before a deadlock might occur.
- Enhancements to allow efficiently iterating over keys in the DDict.


# Dragon 0.11.1 Release Summary
Dragon 0.11.1 addresses a few bugs found in the original 0.11 release and loosens install
requirements

- Drop numpy from required packages
- Support minimum psutil and gunicorn versions hosted in public conda repos
- Print ProcessGroup errors to stderr rather than via logging
- Decrease chances of long teardown times during abnoral exits from runtime

# Dragon 0.11 Release Summary
Dragon 0.11 adds support for python 3.12 and manylinux package distribution via PyPI.
Support for python 3.9 is dropped. Other features:

- Add C and C++ interface to Distributed Dictionary
- Standardize PEP8 formatting of source code
- Add Datastore class for Zarr
- Implement new hashing for Distributed Dictionary for more even distribution among managers
- Overhaul managed memory
- Add ability to customize telemetry configuration
- Add support for hugepages to improve HSTA performance
- Allow synchronization and restart of Distribution Dictionary managers
- Drop modules requirement for installation

# Dragon 0.10 Release Summary
Dragon 0.10 adds support for infiniband networks, checkpointing to the distributed dictionary,
and provides initial support for telemetry monitoring via a Grafana frontend. Other features include:

- Complete overhaul of ProcessGroup to improve reliability and user debugging
- Performance improvements in HSTA
- Better exception handling and logging
- Better support for specifying placement of processes
- Addition of a `dragon-activate` script to make it easier to set-up runtime environment
- Improved stability for the Distributed Dictionary and runtime overall


# Dragon 0.9 and 0.91 Release Summary
This release augments scalability and performance for launching 10k or more processes and greatly improves distributed dictionary
performanace. Other highlighted features:

- Improvements to ProcessGroup to provide better user experience and performance
- Improve launch time for large numbers of processes by enabling batch launch
- New implementation for distributed dictionary that improves performance and scalability
- Support for placement of processes via Policy API
- Bug fix for launching a Pool of pools

# Dragon 0.8 Release Summary
This package introduces new features that enhance portability, further optimize performance at scale, and increase usability with packages that rely on Python multiprocessing derivatives. Highlighted new features are:

- Ability for high speed transport agent to use multiple NICs
- Use of libfabric for high speed transport RDMA operations
- Improved performance of launcher start up time for allocations of more than ~100 nodes.
- Enhanced testing pipeline for Python 3.10 and 3.11
- Added documentation for Overlay Network and a cookbook entry for using the PyTorch native Dataloader over a Distributed Dictionary
- Fixed PMI patching for PBS/Pals, Overlay Network port conflict and exit signaling, detach/destroy of memory pools.
- Fixed numpy scaling test to be able to efficiently scale to 64+ nodes

# Dragon 0.7 Release Summary
This package introduces a number of key features to Dragon in addition to many bug fixes, improved robustness, and
addition/refinements to documentation. Highlighted new features are:

- Ability to support running Dragon in multinode mode on an allocation of up to 1024 nodes.
- Ability to establish policies for CPU and GPU placement when starting a Dragon Process or Dragon ProcessGroup
- Enhanced support for Conda python environments
- The Dragon GlobalServices API for Dragon process Groups now supports List and Query operations
- Documentation updates explaining how to run Dragon multinode using generic SSH launch

# Dragon 0.6 and 0.61 Release Summary
This package is the first to extend Dragon beyond support for Python multiprocessing. The key new feature is support
for running collections of executables, including executables that require support for PMI (e.g., MPI). PMI
support is currently limited to executables using Cray PMI, such as those linked with Cray MPICH. The process group
feature is also utilized for scalable multiprocessing Pool, which can now scale to thousands of workers. Highlighted
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
- RDMA support through HSTA is enabled by default
- Preview release of a distributed dictionary
- documentation cookbook entries for an llm inference service, jupyter notebooks, and distributed dictionary

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
