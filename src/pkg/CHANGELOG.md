# Changelog

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
