# MPI Examples

This directory contains examples showing how to use the Dragon native API to
start MPI applications.

## MPI Pool Workers Demo

The file `mpi_pool_workers_demo.py` contains a program that starts a single rank 
MPI hello world application on each of the allocated workload manager nodes. As
support for MPI applications is expanded, Dragon will support multiple ranks per
node, partitioning of nodes, etc.

### Running the example

1. Run `make` to build the mpi_hello example application.
   ```
   > make
   ```
2. Get an allocation of nodes
   ```
   > salloc --nodes=2 --exclusive
   ```
3. Run the dragon example!
   ```
   > dragon mpi_pool_workers_demo.py
   ```

### Example Output

```
login> dragon mpi_pool_workers_demo.py
[stdout: p_uid=4294967297] Hello world from processor pinoak0202, rank 1 out of 2 processors
[stdout: p_uid=4294967298] Hello world from processor pinoak0201, rank 0 out of 2 processors
```

## HPC Workflow Demos

The files `hpc_workflow_demo.py` and `hpc_workflow_demo_highlevel.py` contain applications
that start multiple concurrent MPI-based workflows. The `hpc_workflow_demo.py`
uses the lower level Dragon 'Global Services' API, whereas the `hpc_workflow_demo_highlevel.py`
utilizes the higher level Dragon Native API. While slightly different, the both implementations
effectively implement the same problem solution.

### Running the example

1. Obtain a copy of the `osu_alltoall` application and place it in the same directory as the
   `hpc_workflow_demo.py` or `hpc_workflow_demo_highlevel.py` application.

2. Get an allocation of nodes
   ```
   > salloc --nodes=2 --exclusive
   ```
3. Run the dragon example!
   ```
   > dragon hpc_workflow_demo.py
   ```
   or
   ```
   > dragon hpc_workflow_demo_highlevel.py
   ```

### Example Output

```
login>$dragon hpc_workflow_demo_highlevel.py
INFO:api_setup:We are registering gateways for this process. dp.this_process.num_gw_channels_per_node=1
INFO:api_setup:connecting to infrastructure from 117921
INFO:api_setup:debug entry hooked
INFO:main:Starting consumer process
INFO:main:Starting a new producer
INFO:main:Starting a new producer
INFO:producer 0:Starting producer (num_ranks=252)
INFO:consumer:reading from result_queue
INFO:producer 1:Starting producer (num_ranks=252)
INFO:producer 0:Starting parse_results process for puid=4294967302
INFO:producer 0:node 0 has 63 ranks, node 1 has 63 ranks, node 2 has 63 ranks, node 3 has 63 ranks
INFO:producer 0:Waiting for group to finish
INFO:parse_results 0:Parsing stdout from stdout connection
INFO:producer 1:Starting parse_results process for puid=4294967554
INFO:producer 1:node 0 has 63 ranks, node 1 has 63 ranks, node 2 has 63 ranks, node 3 has 63 ranks
INFO:producer 1:Waiting for group to finish
INFO:parse_results 1:Parsing stdout from stdout connection
INFO:consumer:{0: ('1', '65.12')}
INFO:consumer:{0: ('2', '62.65')}
INFO:consumer:{0: ('4', '62.30')}
INFO:consumer:{0: ('8', '68.07')}
...
```
