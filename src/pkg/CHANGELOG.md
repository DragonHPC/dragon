# Changelog

## [0.12.2] - 2025-09-11

### Changed

AICI-1881 Switch SSH launch to drun Eric Cozzi

AICI-1898 Update docs with new dragon-config arguments  Pete Mendygral

### Added

AICI-1911 Implement Policy on native Queue  Kent Lee

AICI-1905 New DDict benchmark section in the docs Pete Mendygral

AICI-1880 Make Telemetry compatible with OpenTSDB 2.4 Indira Pimpalkhare

AICI-1929 Zarr store needs len implemented so it can be used for a PyTorch Dataset  Pete Mendygral

AICI-1385 Optimize Multi-NIC HSTA Nick Radcliffe

### Fixed

AICI-1922 Hang when Process Dies in BCast Kent Lee

AICI-1825 Enable/confirm ability to run concurrent MPI apps via ProcessGroup  Nick Hill

AICI-1879 For PMIx, launch of multiple MPI apps within one instance of the dragon runtime Nick Hill

AICI-1896 Fix GSPingSH timeout logic in local services  Nick Hill

AICI-1904 Identify and mitigating apparent memory leaks in HSTA Nick Radcliffe


## [0.12.1] - 2025-08-08

### Changed
AICI-1784 update gups test to use batched put and get and fix ProcessGroup states #1068 by mendygra was merged Aug 8, 2025

Updated dragonrun sequence diagram #1067 by eric-cozzi was merged Aug 5, 2025

Update mpbridge BaseProcess #1065 by kent-lee was merged Aug 6, 2025

Documentation updates #1063, #1062, #1061 by kent-lee were merged Jul 27-28 2025

Explicitly use 'bash -c' when executing drun backend #1058 by eric-cozzi was merged Jul 28, 2025

### Added

Add PBS WLM support to dragon-run for dragon-cleanup #1072 by eric-cozzi was merged Aug 7, 2025

AICI-1732 cpp support for semaphore barrier #1060 by yian-chen was merged Jul 26, 2025

### Removed

### Fixed

AICI-1875 Fix naming collisions with cached network config #1069 by eric-cozzi was merged Aug 5, 2025

Ignore error if conda is unable to remove libstdcxx pkgs that may or may not be installed by default in build pipeline #1066 by mohammad-hadi was merged Jul 31, 2025

fixed barrier test - puid was not stored correctly #1056 by kent-lee was merged Jul 25, 2025

## [0.12.0] - 2025-07-29

### Changed

Move mpiexec override to dragon-config add subparser #1055 by nicholas-hill was merged Jul 25, 2025

Make PMIx optional during developer build #1052 by nicholas-hill was merged Jul 24, 2025

Add flush to native queue and pass Send/Receive handles via a new attr #1038 by kent-lee was merged Jul 8, 2025

Chkpt persistence improvement #1035 by yian-chen was merged Jul 14, 2025

New Queue Support in C/C++ #1030 by kent-lee was merged Jul 2, 2025

Ddict cleanup #1023 by yian-chen was merged Jun 25, 2025

Expose h_uid of the host node in the Process API #1018 by maria-kalantzi was merged Jun 17, 2025

Make string states an enumerated class in Process Group #1011 by veena-venkata-ghorakavi was merged Jul 10, 2025

Telemetry Cleanup #997 by indira-pimpalkhare was merged May 21, 2025

Ensure child processes update their working directory #991 by maria-kalantzi was merged May 12, 2025

Pin Cython version in build steps #990 by nicholas-hill was closed May 2, 2025

Update Jupyter Docs and address typos #986, 987 by kent-lee on Apr 29, 2025

### Added

Add/improve exception handling in dragon-cleanup #1048 by eric-cozzi was merged Jul 15, 2025

Add native queue tests #1047 by yian-chen was merged Jul 16, 2025

Add PMIx server to support general MPI library apps #1043 by nicholas-hill was merged Jul 22, 2025

CPP queue interface and tests #1040 by yian-chen was merged Jul 18, 2025

Tag v0.12rc3 #1034 by nicholas-hill was merged Jul 1, 2025

Add Authentication to Telemetry #1025 by indira-pimpalkhare was merged Jul 15, 2025

Add classmethod to process group for generally configuring distributing training #1024 by veena-venkata-ghorakavi was merged Jul 2, 2025

buffered send in the fli #1021 by kent-lee was merged Jun 24, 2025

Set Checkpoint ID in DDict clients and change property name #1004 by kent-lee was merged May 19, 2025

Allow users to specify and override default mpiexec launch of runtime #1003 by wahlc was merged May 21, 2025

Add batch stats to telmetry #1001 by nick-radcliffe was merged May 19, 2025

Slow gpu detection support #999 by wahlc was merged Jul 1, 2025

ddict checkpoint persistent for daos #1016 by yian-chen was merged Jun 27, 2025

MPI vs Dragon pool benchmark #1020 by nick-radcliffe was merged Jun 24, 2025

Freeze ddict support for fast ddict reads #1019 by yian-chen was merged Jun 18, 2025

Tests for HSTA during a runtime restart #1015 by indira-pimpalkhare was merged Jun 12, 2025

Add ddict checkpoint persistence #988 by yian-chen was merged May 19, 2025

Added script to update open source #989, #993, #994, #995 by kent-lee was merged May 2, 2025

### Removed

Remove setting of DRAGON_NETWORK_CONFIG env. var from backend launch #1017 by nicholas-hill was merged Jun 27, 2025

### Fixed

Fix zarr unit test failure #1054 by mendygra was merged Jul 24, 2025

Several fixes to tests with mixed up mp contexts #1053 by kent-lee was merged Jul 24, 2025

Fix Serializable Design for DDict keys #1050 by kent-lee was merged Jul 20, 2025

Fix for DDict Example Code and an mpbridge Queue fix #1049 by kent-lee was merged Jul 17, 2025

Fixes the glibc mismatch in DST Pipeline #1046 by kent-lee was merged Jul 11, 2025

Fix DDict compile error #1045 by sanian-gaffar was merged Jul 16, 2025

Barrier Try Again Fix for HSTA #1044 by kent-lee was merged Jul 12, 2025

Add symlink to tools and fix FE Launcher for dragon-cleanup to check for DRAGON_MATCH_MP env var. #1042 by eric-cozzi was merged Jul 15, 2025

Fixed DDict Regression Issue via new turbo mode#1032 by kent-lee was merged Jul 1, 2025

Temporary drun dragon-cleanup fixes #1031 by eric-cozzi was merged Jul 7, 2025

handle working set size of one correctly in DDict #1027 by kent-lee was merged Jun 26, 2025

Update transport default regex for network address detection #1007 by yian-chen was merged May 20, 2025

Fix documentation for JobLib benchmarks utilizing base multiprocessing and DragonHPC #1006 by veena-venkata-ghorakavi was merged May 28, 2025

Fix DataLoader to work with zarrdataset and newer PyTorch releases #1002 by potts was merged May 20, 2025

ddict duplicate file creation error #1000 by yian-chen was merged May 16, 2025

Make SSH launch respect --hostfile option #998 by kent-lee was merged May 16, 2025

Fixed up timeout value for low resources in DST #996 by kent-lee was merged May 7, 2025

Fixing mpbridge context passing #1010 by veena-venkata-ghorakavi was merged Jul 3, 2025

remove ctx being passed to base RLock erroneously #1008 by mendygra was merged May 21, 2025

Some patching issues fixed #992 by kent-lee was merged May 2, 2025


## [0.12rc1] - 2025-04-29

- Many small bug fixes.

- Jupyter notebook server now can be started with dragon-jupyter and now allows
  kernel restart and fixes some printing issues.

- DDict broadcast put and get for enhanced performance when all clients need a value.

- Huge cleanup and reorganization of documentation.

- Enhanced semaphore performance.

- Out of memory watcher added for user feedback before a deadlock might occur.

- Enhancements to allow efficiently iterating over keys in the DDict.


## [0.11.1] - 2025-03-19

### Changed

Remove numpy requirement, allow older minimum versions of other required packages, and allow empty LD_LIBRARY_PATH #952 by nicholas-hill was merged Mar 19, 2025

Fix Process Group Stderr printing and improve teardown speed #955 by kent-lee was merged on Mar 17, 2025

## [0.11] - 2025-03-07

### Changed

Make explicit use of HSTA backend libs and introduce TCP fallback #930 by nicholas-hill was merged Feb 28, 2025

Update ddict documentation #904 by yian-chen was merged Feb 10, 2025

Remove tcp restriction when using ssh launch #881 by eric-cozzi was merged Jan 17, 2025

Updated DDict manager to save space in bootstrap #872 by kent-lee was merged Jan 10, 2025

Update blocks in managed memory #869 by kent-lee was merged Jan 8, 2025

Changed ddict to murmur_hash3_32 #864 by kent-lee was merged Jan 2, 2025

Overhaul release packaging and wheel install #861 by nicholas-hill was merged Jan 9, 2025

Improved p2p_lat Test Performance #860 by kent-lee was merged Dec 19, 2024

C DDict - handle response messages in better way #858 by yian-chen was merged Jan 7, 2025

FLI Buffered Connection Optimization #854 by kent-lee was merged Dec 11, 2024

Black Formatting Changes #853 by burkemi was merged Jan 13, 2025

New Heap Manager #847 by kent-lee was merged Dec 9, 2024

Improve documentation to give users better getting started experience #816 by mendygra was merged Nov 14, 2024

set c_uid for getmsg response #794 by nick-radcliffe was merged Oct 7, 2024

### Added

Added scalable filtering to ddict #933 by kent-lee was merged Mar 3, 2025

Add local managers and local keys to DDict API #932 by yian-chen was merged Mar 4, 2025

Add __missing__ to DDict #924 by potts was merged Feb 24, 2025

Implement batch put for DDict #918 by yian-chen was merged Mar 5, 2025

Add cloning to DDicts #909 by yian-chen was merged Mar 5, 2025

Add manylinux2014 support #907 by nicholas-hill was merged Feb 19, 2025

Add data loader for Zarr #903 by mendygra was merged Feb 18, 2025

Add telemetry Analysis #897 by wahlc was merged Feb 10, 2025

Use ddict as context manager to support custom pickler #902 by yian-chen was merged Feb 7, 2025

Black Linter Github Action #900 by burkemi was merged Feb 6, 2025

Add automated  py3.12 build #890 by sanian-gaffar was merged Jan 24, 2025

Add ddict cpp examples #888 by yian-chen was merged Jan 28, 2025

Add Manifest Stats #886 by kent-lee was merged Jan 21, 2025

Independent HSTA log level #883 by wahlc was merged Jan 21, 2025

Add Intel GPU metric support in telemetry #882 by wahlc was merged Jan 21, 2025

Add link-line-advisor #877 by nicholas-hill was merged Jan 16, 2025

DDict cpp functionalities #874 by yian-chen was merged Jan 9, 2025

YAML config for Telemetry #873 by indira-pimpalkhare was merged Feb 3, 2025

Cross ddict sync #868 by yian-chen was merged Jan 21, 2025

Add hugepage support #866 by nick-radcliffe was merged Jan 8, 2025

C DDict manager selection #857 by yian-chen was merged Dec 13, 2024

DDict cpp interface #851 by yian-chen was merged Jan 6, 2025

Telemetry Unit Tests #850 by indira-pimpalkhare was merged Dec 19, 2024

Ddict c interface #846 by yian-chen was merged Dec 4, 2024

Add github actions for standardizing processes #844-845 by nicholas-hill was merged Dec 3, 2024

Add PyTorch DL Use Case #826 by veena-venkata-ghorakavi was merged Dec 16, 2024

Local pools with manager selector #819 by kent-lee was merged Nov 5, 2024

adding progress bar for hsta #800 by nick-radcliffe was merged Oct 30, 2024

Added numa check #798 by kent-lee was merged Oct 14, 2024

### Removed

Removed extra line #936 by kent-lee was merged Mar 3, 2025

Removed managed memory type from serialized descriptor #891 by kent-lee was merged Jan 24, 2025

Drop cpython build and code #862 by nicholas-hill was merged Jan 3, 2025

### Fixed

Fix A Scaling Issue with DDict #940 by kent-lee was merged Mar 6, 2025

Fix missing top-level meta files when only loading top-level keys for Zarr #939 by potts was merged Mar 6, 2025

Fixed single node mode for filter to avoid node name problem #934 by kent-lee was merged Mar 3, 2025

Event Fix #931 by kent-lee was merged Feb 28, 2025

Fix policy merge of priorities#928 by eric-cozzi was merged Feb 28, 2025

Fix zarr bugs #927 by mendygra was merged Feb 27, 2025

Fix the timeout argument to queue get #925 by mendygra was merged Feb 24, 2025

Fix Unit Tests #921 by kent-lee was merged Feb 22, 2025

BCast Idle Wait Fix #920 by kent-lee was merged Feb 21, 2025

BitSet Enhancements and various fixes #917 by kent-lee was merged Feb 20, 2025

Fixed broken telemetry tests #913 by wahlc was merged Feb 22, 2025

Update metadata for PyPI publishing #910 by nicholas-hill was merged Feb 20, 2025

Fix /api/suggest endpoint in Telemetry #908 by indira-pimpalkhare was merged Feb 13, 2025

Update ddict documentation #904 by yian-chen was merged Feb 10, 2025

Fix /api/suggest endpoint in Telemetry #908 by indira-pimpalkhare was merged Feb 13, 2025

Update py311-devel package link in automated builds #901 by sanian-gaffar was merged Jan 30, 2025

Fix DDict tests for new loading of dragon libs #899 by nicholas-hill was merged Jan 30, 2025

Fix Zero byte alloc failures #898 by nick-radcliffe was merged Jan 31, 2025

Fixes for manifest leaks #895 by kent-lee was merged Jan 24, 2025

Fix DRAGON_BIN_DIR value #893 by nick-radcliffe was merged Jan 23, 2025

BCast Workaround and other Fixes for ANL Workflow #885 by kent-lee was merged Jan 21, 2025

Add check that the network-config contains a head node element '0' #880 by eric-cozzi was merged Jan 15, 2025

Modify LD_PRELOAD and LD_LIBRARY_PATH to successfully launch MPI apps #879 by nicholas-hill was merged Jan 17, 2025

Fix for cleanup on login node #876 by wahlc was merged Jan 15, 2025

Raise error if unmatched/unknown host_id is received #875 by eric-cozzi was merged Jan 10, 2025

Move hpages cleanup to be accessible w/o modules and with wheel install #871 by nicholas-hill was merged Jan 9, 2025

Turn off skip on lock-fairness test as it works now #870 by nicholas-hill was merged Jan 9, 2025

Fix AMD and Intel gpu detection #867 by wahlc was merged Jan 9, 2025

Fix Compiler Warnings #856 by kent-lee was merged Dec 12, 2024

Fix issue with already existing folder in ProcessGroup cwd test #855 by nicholas-hill was merged Dec 17, 2024

Fixed test cases #852 by kent-lee was merged Dec 9, 2024

Bcast Performance #843 by kent-lee was merged Dec 10, 2024

Fix for cwd from v0.10-rc branch with test #835 by wahlc was merged Nov 25, 2024

Complete MPI workflow example by adding OSU alltoall microbenchmark #834 by nicholas-hill was merged Nov 25, 2024

Telemetry scale testing and one fix for larger node counts #833 by wahlc was merged Nov 26, 2024

Fix Telemetry Shutdown #832 by wahlc was merged Dec 6, 2024

Fixed default memory pool leak with multi-node test. #831 by yian-chen was merged Dec 2, 2024

Bring develop up to date with master #829 by nicholas-hill was merged Nov 18, 2024

Fix Parsl demo code to make runnable #825 by nicholas-hill was merged Nov 6, 2024

Fix HSTA config timeout #824 by nicholas-hill was merged Nov 5, 2024

Add GPU Metrics to Collector #823 by indira-pimpalkhare was merged Nov 27, 2024

Fix for cwd in sdesc for SmartSim #820-821 by wahlc was merged Nov 14, 2024

ddict-logging fix for v0.10-rc branch #818 by wahlc was merged Nov 4, 2024

Delay send_rndvs until ep_addrs is ready #814 by wahlc was merged Nov 26, 2024

Fixed bad reference on Intro Page of Docs #810 by kent-lee was merged Oct 22, 2024

Ddict logging #807 by kent-lee was merged Oct 21, 2024

Fix multinode value test and add mp shared_ctypes test (and several other issues) #801 by nicholas-hill was merged Nov 18, 2024

Reduce number of posted recvs #797 by nick-radcliffe was merged Oct 24, 2024

Add Capnp messaging without need for fds #795 by kent-lee was merged Oct 22, 2024

Hashtable Fix for Invalid Pointer #793 by kent-lee was merged Oct 3, 2024

Update resiliency runtime #791 by nicholas-hill was merged Oct 9, 2024

Launcher ignores nodes without hsn addr if we have enough extra nodes #790 by wahlc was merged Oct 2, 2024


## [0.10] - 2024-10-07

### Changed

Update docs and package constraints for install #789 by kent-lee was merged Sep 28, 2024

Allow launching on subset of nodes with correctly configured networks #787 by wahlc was merged Sep 27, 2024

Force use of original network configuration and ignore just-in-time dynamic ones default #784 by nick-radcliffe was merged Sep 23, 2024

Improve scipy_scale_work.py startup time #783 by nick-radcliffe was merged Sep 23, 2024

Update Value and Array to better handle long strings #782, #788 by nicholas-hill was merged Sep 27, 2024

Update release and test scripts  #777 by veena-venkata-ghorakavi was merged Sep 27, 2024

Updates and Docs and DDict Documentation #774 by kent-lee was merged Sep 6, 2024

Changes to improve automated build success #767 by nicholas-hill was merged Sep 3, 2024

Update typing_extensions from 4.4.0 to 4.12.2 #765 by ashish-vinodkumar was merged Aug 28, 2024

Update ProcessGroup backtrace and exception handling and bring docs up to date [#742, #731] by nicholas-hill was merged Aug 1 & July 30, 2024

Propagate network config to support multi-NIC for SSH launch and add related documentation [#730, #739] by potts was merged Jul 30, 2024

Policy API Cleanup #725 by wahlc was merged Jul 26, 2024

Cleanup HSTA odds and ends #724 by nick-radcliffe was merged Jul 22, 2024

Minimize execution of HSTA network discovery #718 by nick-radcliffe was merged Jul 2, 2024

Clean up launch selection code #716 by eric-cozzi was merged Jul 30, 2024

Update distributed dictionary example #737 by kent-lee was merged Jul 29, 2024

Auto-Register Gateway channels #726 by kent-lee was merged Jul 24, 2024

Omit parent process from termination in dragon-cleanup #711 by chris-mcbride was merged Jul 3, 2024

Enable more control over infrastructure channel creation for processes and ProcessGroup #708 by mendygra was merged Jun 18, 2024

Additional Tracing and Timeout Handling for distributed dictionary #702 by kent-lee was merged Jun 5, 2024

Add checks on gateway message completion to improve debugging #701 by kent-lee was merged Jun 12, 2024

Update distributed dictionary hash function #694 by kent-lee was merged May 24, 2024 • Approved

### Added

Implement round-robin schedule to numa domain use in managed memory #785 by wahlc was merged Sep 25, 2024

Add Intel GPU detection and affinity support #779 by wahlc was merged Sep 24, 2024

Added the print_stats option to the ddict benchmark #769 by kent-lee was merged Aug 30, 2024

Implemented new process group #761 by mendygra was merged Aug 30, 2024

Use new ddict in torch dataset #755 by yian-chen was merged Aug 8, 2024

Add libfabric OFI backend for HSTA to the release package [#750, #745, #734] by nicholas-hill was merged Aug 2 & July 26, 2024

Add support for Zero Byte Message Allocation #741 by kent-lee was merged Aug 1, 2024

Add cythonized pickle write/read adapter for improved performance/remove python version #740 by yian-chen was merged Aug 1, 2024

Add dragon-activate script to set up user environment #733 by eric-cozzi was merged Jul 25, 2024

Add non-blocking gateway complete checks for improved performance  #729 by nick-radcliffe was merged Jul 25, 2024

Add checkpointing rollback to distributed dictionary and logging for memory block frees #728 by yian-chen was merged Jul 25, 2024

Add support for placement of distributed dictionary managers #727 by wahlc was merged Jul 26, 2024

Add PyYAML to release package installation for network config files #723 by veena-venkata-ghorakavi was merged Jul 17, 2024

Add client-side streams to distributed dictionary #722 by kent-lee was merged Jul 18, 2024

Add Zero Bytes message allocation support for HSTA remote get operations #717 by kent-lee was merged Jul 3, 2024

Add UCX backend to HSTA #715 by nick-radcliffe was merged Jul 18, 2024

### Removed

### Fixed

Address invalid pointer via a persistent buffer for rehashing dict keys instead of dynamically allocating memory #793 by kent-lee was merged Oct 3, 2024 by kent-lee

Fix FLI EOT Handling #781 by kent-lee was merged Sep 23, 2024

Fix Barrier Leak #780 by kent-lee was merged Sep 11, 2024

Further salt DDict pool name to avoid naming collisions #778 by kent-lee was merged Sep 25, 2024

Fix issue with gups test caused by channel attr initialization #776 by nick-radcliffe was merged Sep 10, 2024

Fix race in gateway message completion #773 by nick-radcliffe was merged Sep 5, 2024

Allow launcher to avoid nodes with misconfigured networks #790 opened Sep 30, 2024 by wahlc

Remove destruction of default mem pool in demo. #772 by nicholas-hill was merged Sep 4, 2024

Fix ProcessGroup examples #770 by wahlc was merged Sep 3, 2024

Fix missing Unlock of Heap on Error Path #766 by kent-lee was merged Aug 28, 2024

Fixed FLI Memory Leak and Improved DDict Stats #762 by kent-lee was merged Aug 28, 2024

Fix path directory change in dragon-install script #763 by mohammad-hadi was merged Aug 27, 2024

Revert bad managed memory merge from 0.91 release #760 by nicholas-hill was merged Aug 23, 2024

Remove bad kwarg flush=True from logging Exceptions #759 by garrett-goon was merged Aug 22, 2024

Fix mem leak in pickle adapter #757 by yian-chen was merged Aug 21, 2024

Add fix to keep trying to allocate buffer until timeout #747 by nick-radcliffe was merged Aug 20, 2024

Fix response /api/query #756 by indira-pimpalkhare was merged Aug 14, 2024

Fix nightly test failure [#751, #752] by yian-chen was merged Aug 7, 2024

Fix typo in error message #749 by chris-mcbride was merged Aug 7, 2024

Fix bug in dragon-cleanup #744 by wahlc was merged Aug 1, 2024

Removed bad memory free in FLI interface #736 by kent-lee was merged Jul 29, 2024

Fix/improve distributed dictionary scaling #735 by kent-lee was merged Jul 29, 2024

Fix release package support for telemetry and update docs #732 by wahlc was merged Jul 30, 2024

Fix poorly named variable in dragon.native.machine.State #720 by chris-mcbride was merged Jul 24, 2024

Make unit tests more reliable #712 by mendygra was merged Jun 27, 2024

Fix environment variable setting for default HSTA backend #705 by nick-radcliffe was merged Jun 7, 2024

Fix policy inheritance in ProcessGroup #698 by wahlc was merged May 24, 2024

Fix overflow for object counter in HSTA #697 by nick-radcliffe was merged May 24, 2024

Update dragon AI docs #696 by wahlc was merged May 24, 2024

Remove unnecessary Exception that broke pool joins #695 by nicholas-hill was merged May 21, 2024

Fix ordering in distributed dictionary destroy operation #693 by yian-chen was merged May 20, 2024


## [0.91] - 2024-05-20


### Changed

### Added

### Removed

### Fixed

Join on DDict orchestrator during DDict destroy and fix DDict demo for single node #691 was merged May 15, 2024

Added timeout in DDict attach and remove additinoal connection to DDict manager #687 was merged May 15, 2024 by yian-chen

DDict manager raises exception when value is too large to store via FLI and correctly pickles numpy arrays #688 by wahlc was merged May 15, 2024

Fix seg fault in launcher from NULL pointers in heap manager #684 by nicholas-hill was merged May 13, 2024

Fix linked list bug in FLI #682 by kent-lee was merged May 10, 2024



## [0.9] - 2024-04-22


### Changed

Added lock size into required size of Pool #651 by kent-lee was merged Apr 18, 2024

Update python 3.9 build to sle15sp3 #654 by nicholas-hill was merged Apr 18, 2024

Efficient GS->LS Group Creation of processes #641 by eric-cozzi was merged Apr 11, 2024

Overhaul of ProcessGroup for improved scalability and reliability [#653, #647 #625] by nicholas-hill was merged Apr 18, 2024; Apr 15, 2024; Mar 25, 2024


### Added

Add signal handling for clean up when workload manager experiences allocation timeout #650 by wahlc was merged Apr 18, 2024

New Distributed Dictionary and Checkpointing Design [#644, #629] by kent-lee was merged Apr 18, 2024, Apr 3, 2024

Update Release Tests #639 by veena-venkata-ghorakavi was merged Apr 5, 2024

On node gpu framework and init #633 by nick-radcliffe was merged Apr 9, 2024

Add hints to transport payloads #630 by nick-radcliffe was merged Mar 27, 2024

Expose policy for process placement #628 by wahlc was merged Mar 26, 2024

### Removed

Limit docs update to one build of master #623 by mohammad-hadi was merged Mar 15, 2024

### Fixed

Fix bad bcast tree init regression brought out by resiliency changes. #645 by nicholas-hill was merged Apr 15, 2024

Fix bad frontend host ID assignment #652 by nicholas-hill was merged Apr 17, 2024

Ensure clean build script does a thorough clean and build #643 by kent-lee was merged Apr 9, 2024

Fix regression and add test for launching pool of pools #638 by wahlc was merged Apr 12, 2024

Fix capnproto build and install #631 by kent-lee was closed Mar 21, 2024

Remove HSTA from bring-up if there's only one backend node #632 by nick-radcliffe was merged Mar 25, 2024



## [0.8] - 2024-02-26


### Changed

### Added

Libfabric support for hsta and multi-NIC support [#620, #614, #594] by nick-radcliffe was merged Mar 7, 2024, Feb 22, 2024, Feb 15, 2024

PyTorch Dataloader process placement patch and example [#610, #608, #601, #599] by veena-venkata-ghorakavi and wahlc was merged Feb 9, 2024, Feb 6, 2024, Jan 4, 2024

Streamline node updates [#607] by eric-cozzi was merged Feb 8, 2024

Build multiple python versions of dragon [#600] by mohammad-hadi was merged Jan 4, 2024

Add Overlay Network docs [#595] by eric-cozzi was merged Jan 2, 2024

Startup time improvement [#586] by eric-cozzi was closed on Nov 29, 2023

Channels file-like interface [#585] by kent-lee was merged 10 days ago

### Removed

Remove devcontainer code-server from the dragon-cleanup script [#606] by nicholas-hill was merged Feb 7, 2024

Remove compressed json wrapper [#596] by eric-cozzi was merged Jan 4, 2024

Remove signal handling and update Slurm srun command [#584] by eric-cozzi was merged on Nov 27, 2023

### Fixed

Fix PMI issues on PBSPro+PALS systems [#617] by nicholas-hill was merged Feb 23, 2024

Add typecast to parameter args if env. var. is empty string [#615] by nicholas-hill was merged Feb 16, 2024

Fix queue `__del__` error from pi demo [#605] by yian-chen was merged Jan 29, 2024

Add checks that only the Primary Launcher Backend talks to GS [#604] by eric-cozzi was merged Jan 30, 2024 • Approved

Fix Pool Detach/Destroy [#598] by kent-lee was merged Jan 4, 2024

Fix mp unittests test.support import compatibility with Python 3.10 and 3.11 [#597] by wahlc was merged Jan 4, 2024

Fix multinode sequence diagrams [#592] by eric-cozzi was merged on Dec 8, 2023

Catch executables with Path-like paths and ensure they are strings [#591] by wahlc was merged on Dec 8, 2023

Raise abnormal exit if head process start fails [#590] by eric-cozzi was merged on Dec 11, 2023

Make device_list in AcceleratorDescriptor serializable for messages [#589] by nicholas-hill was merged on Dec 4, 2023

Log text of SHFwdOutput messages on both backend and frontend [#588] by eric-cozzi was merged on Dec 11, 2023

Remove find_accelerators from `NodeDescriptor.__init__` [#587] by nicholas-hill was merged on Nov 30, 2023

Fix ImportError for mp unittests when tested with Python 3.10+ [#583] by wahlc was merged on Dec 8, 2023

Fix network-config-helper to wait for signal to exit [#582] by eric-cozzi was merged on Nov 22, 2023

Launcher ON should use a different port [#581] by eric-cozzi was merged on Nov 22, 2023

Fix attrs version constraint [#579] by eric-cozzi was merged on Nov 16, 2023

Gateway per msg type [#578] by nick-radcliffe was merged on Nov 21, 2023
