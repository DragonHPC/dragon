MPI with Dragon
+++++++++++++++

Using the new `ProcessGroup` API, Dragon can now be used to start and manage a collection of PMI/MPI based jobs.
`ProcessGroup` uses the Global Services Client API Group interface, `GSGroup`, to manage the state of such
processes/jobs.

This functionality is currently only available on systems where Cray PALS is available. Before starting the
PMI enabled group, Dragon will interact with its job initialization hooks within the PMI library
to establish a unique job_id, place the PMI applications, and establish appropriate job & rank
parameters. Note that due to the nature of how PMI applications work, you cannot restart a
failed PMI rank, or add/remove ranks from a group of PMI processes.


PMI
---

PMI is used by MPI to determine various parameters for a job, such as the number of nodes and ranks,
number of ranks per node, etc. PMI gets information to set these values via another library called
PALS. The basic idea behind the PMI "plumbing" is to either bypass or hook PALS functions to
set these values according to user-specified job parameters. This can be done in a few ways.

* Slingshot requires a concept called `PID` to create an endpoint. One PID is required per MPI rank. PID values range between 0-511, with the lowest values being reserved by Dragon for the transport agents on a node. Remaining PIDs are allocated in contiguous intervals for all processes of a `ProcessGroup` colocated on a single node. Cray MPI is made aware of the base PID of an interval by setting `MPICH_OFI_CXI_PID_BASE=<base PID>`, with the length of the interval being determined by the number of MPI ranks on the node.
* The channels library, `libdragon.so`, contains the hooks for PALS functions, and we use `LD_PRELOAD` to make sure it is linked before other libraries.
* The `_DRAGON_PALS_ENABLED` environment variable is used to enable or disable PMI support as needed. In general it is disabled, but must be enabled to launch MPI jobs via `ProcessGroup`.
* `FI_CXI_RX_MATCH_MODE=hybrid` is needed to prevent exhaustion of Slingshot NIC matching resources during periods of heavy network traffic, i.e., many-to-many communication patterns.
* PMI requires a unique value for its "control network", which is used to implement PMI distributed pseudo-KVS. Global services allocates unique ports for each MPI job, and sets the port via the `PMI_CONTROL_PORT` environment variable. Note that PMI control ports can’t be determined "locally" within each job, since there can be overlap between nodes in different jobs.