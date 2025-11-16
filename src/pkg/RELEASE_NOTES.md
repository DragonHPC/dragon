# Dragon 0.13 Release Summary

We're excited to announce the release of Dragon v0.13, which includes several updates, additions and fixes. Here are the highlights:

### Updates

- Multiple pages in the documentation have been updated
- Reworked how Dragon manages and references PMIx namespaces
- Updated the run_gups_scale.sh benchmark test to use --benchit commnd line option
- Updated the release scaling tests
- Removed distutils from entire Dragon codebase
- Remove Futex from BCast for Mac OS Support

### New Features

- Implemented a TCP backend for the Dragon HSTA agent
- Integrated a Dragon Batch workflow allowing jobs to be run via a Directed Acyclic Graph
- Implemented new unittests for the dragon-config tool
- Added usage pages to docs for dragon and dragon-config
- Add a default memory pool tracker that emits warnings if it is getting full

### Resolved Issues

- Fixed issue with running the Dragon multi-node tests
- Fixed PMIx teardown in local services
- Fixed inconsistency in training group creation
- Fixed issue with executable launch without arguments
- Policy evaluator has asserts in it that cause silent GS failures

# Dragon 0.12.3 Release Summary

We announce the release of Dragon v0.12.3, which includes several updates, additions, and fixes. Here are the highlights:

### Updates

- The DDict filter now utilizes native interfaces instead of multiprocessing.

### New Features

- GPU support for managed memory support has been introduced.
- A benchmark for checkpoint persistence testing has been added.
- Documentation has been created to guide users in selecting the appropriate PMI backend for MPI applications.
- Tests for dragon-cleanup have been implemented.
- The dragon-cleanup process has been expanded to include telemetry processe in its clean-up.
- SSH launch testing has been integrated into our merge to develop and release checklist.


### Resolved Issues

- Users can now re-run a process group with a PMIx backend without needing to close and re-initialize a completely new ProcessGroup
- utf-8 decoding errors in local services have been fixed.
- The src/dragon/lib symbolic link is now created as part of the Makefile build process, simplifying development builds of the runtime
- Inconsistencies in the DDict documentation have been addressed.
- An erroneous hostlist error raise during WLM launch has been corrected.
- Policy evaluator asserts that caused silent GS failures have been removed.


# Dragon 0.12.2 Release Summary

We're excited to announce the release of version 0.12.2, which includes several improvements, new features, and bug fixes. Here are the highlights:

### New Features:

- We've implemented a policy on native queues, making it easier to specify where a queue should be created.
- Telemetry is now compatible with OpenTSDB 2.4
- The Zarr store now supports the len function, making it compatible with PyTorch datasets.
- We've optimized Multi-NIC HSTA for better scaling performance.

### Improvements:

- The SSH launch process has been updated to use drun, providing an easier way to launch applications via SSH.
- Documentation has been updated to reflect new dragon-config arguments.
- A new benchmark section for DDict has been added to the documentation.

### Bug Fixes:

- We've resolved an issue where the system would hang when a process died in a channels BCast.
- Concurrent MPI applications can now be run via ProcessGroup and a PMIx backend without issues.
- Multiple MPI applications can be launched within a single instance of the dragon runtime using the PMIx backend
- The GSPingSH timeout logic in local services has been fixed, helping prevent indefinite hangs should something go wrong launching the runtime
- Memory leaks in HSTA have been identified and mitigated.


# Dragon 0.12.1 Release Summary

This release addresses several bugs encountered in 0.12.0 as well as a few new features

### Changes and Updates:

- Updated the gups test to use batched put and get
- Updated the dragon-run sequence diagram
- Updated the mpbridge BaseProcess for futureproofing
- Made documentation updates
- Explicitly use 'bash -c' when executing the drun backend

### New Features:

- Added PBS WLM support to dragon-run for dragon-cleanup
- Introduced C++ support for semaphore barriers

### Bug Fixes:

- Fixed naming collisions with cached network config
- Ignored errors when conda is unable to remove libstdcxx packages during pipeline builds
- Fixed the barrier unit test by correctly storing the puid
- Added missing ProcessGroup states to enumerated class


# Dragon 0.12.0 Release Summary

In Dragon 0.12, we introduce PMIx support for launching MPI applications, port our native
Queue implementation to C for a measured 20% performance improvement, add a read-only mode ('freeze')
to our distributed dictionary for faster reads, and offer a new `dragon-cleanup` executable
based on a new runtime launching mechanism we intend to ultimately subsume the responsibilities
of the Dragon launcher frontend.

### New Features
 - Added support for native queue and passing Send/Receive handles via a new attribute
 - Implemented checkpoint persistence improvements
 - Introduced new queue support in C/C++
 - Added exception handling in dragon-cleanup
 - Added native queue tests
 - Added PMIx server to support general MPI library apps
 - Added CPP queue interface and tests
 - Added authentication to telemetry
 - Added classmethod to process group for generally configuring distributing training
 - Added buffered send in the fli
 - Added batch stats to telemetry
 - Added slow GPU detection support
 - Added ddict checkpoint persistence for DAOS
 - Added MPI vs Dragon pool benchmark
 - Added freeze ddict support for fast ddict reads
 - Added tests for HSTA during a runtime restart
 - Added ddict checkpoint persistence
 - Added script to update open source

### Improvements
 - Made PMIx optional during developer build
 - Moved mpiexec override to dragon-config and added subparser
 - Improved working directory updates for child processes
 - Updated Jupyter docs and addressed typos
 - Improved string states as an enumerated class in ProcessGroup
 - Improved telemetry cleanup
 - Added torch import to ai/__init__.py to support documentation generation

### Fixes
 - Fixed zarr unit test failure
 - Fixed several issues with mixed up mp contexts in tests
 - Fixed Serializable Design for DDict keys
 - Fixed DDict example code and an mpbridge Queue fix
 - Fixed glibc mismatch in DST Pipeline
 - Fixed DDict compile error
 - Fixed barrier try again issue for HSTA
 - Fixed FE Launcher for dragon-cleanup to check for DRAGON_MATCH_MP env var
 - Fixed DDict regression issue via new turbo mode
 - Fixed temporary drun dragon-cleanup issues
 - Fixed working set size of one correctly in DDict
 - Updated transport default regex for network address detection
 - Fixed documentation for JobLib benchmarks
 - Fixed DataLoader to work with zarrdataset and newer PyTorch releases
 - Fixed ddict duplicate file creation error
 - Fixed SSH launch to respect --hostfile option
 - Fixed timeout value for low resources in DST
 - Fixed mpbridge context passing
 - Fixed issue where ctx was being passed to base RLock erroneously

### Removals
 - Removed setting of DRAGON_NETWORK_CONFIG env var from backend launch


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
