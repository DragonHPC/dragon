# MPI Examples

This directory contains examples showing how to use the Dragon native API to
start MPI applications.

## MPI Pool Workers Demo

The file `mpi_process_group_demo.py` contains a program that starts a single rank
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
   > dragon mpi_process_group_demo.py
   ```

### Example Output

```
login> dragon mpi_process_group_demo.py
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

## Policy Demo

The file `policy_demo.py` contains a program that shows how policies can be passed to process groups and processes that are a part of the process group. This example highlights how an MPI application can be launched on a subset of the allocated nodes and how a policy restricting the cpu affinity can be applied to the whole group.

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
   > dragon policy_demo.py
   ```

### Example Output

```
 dragon policy_demo.py
Using 2 of 4
pinoak0015 has AMD GPUs with visible devices: [0, 1, 2, 3, 4, 5, 6, 7]
pinoak0016 has AMD GPUs with visible devices: [0, 1, 2, 3, 4, 5, 6, 7]
pinoak0014 has AMD GPUs with visible devices: [0, 1, 2, 3, 4, 5, 6, 7]
pinoak0013 has AMD GPUs with visible devices: [0, 1, 2, 3, 4, 5, 6, 7]
4294967298 returned output: Hello world from pid 57645, processor pinoak0015, rank 0 out of 16 processors

4294967299 returned output: Hello world from pid 57646, processor pinoak0015, rank 1 out of 16 processors

4294967300 returned output: Hello world from pid 57647, processor pinoak0015, rank 2 out of 16 processors

4294967301 returned output: Hello world from pid 57648, processor pinoak0015, rank 3 out of 16 processors

4294967302 returned output: Hello world from pid 57649, processor pinoak0015, rank 4 out of 16 processors

4294967303 returned output: Hello world from pid 57650, processor pinoak0015, rank 5 out of 16 processors

4294967304 returned output: Hello world from pid 57651, processor pinoak0015, rank 6 out of 16 processors

4294967305 returned output: Hello world from pid 57652, processor pinoak0015, rank 7 out of 16 processors

4294967306 returned output: Hello world from pid 56247, processor pinoak0016, rank 8 out of 16 processors

4294967307 returned output: Hello world from pid 56248, processor pinoak0016, rank 9 out of 16 processors

4294967308 returned output: Hello world from pid 56249, processor pinoak0016, rank 10 out of 16 processors

4294967309 returned output: Hello world from pid 56250, processor pinoak0016, rank 11 out of 16 processors

4294967310 returned output: Hello world from pid 56251, processor pinoak0016, rank 12 out of 16 processors

4294967311 returned output: Hello world from pid 56252, processor pinoak0016, rank 13 out of 16 processors

4294967312 returned output: Hello world from pid 56253, processor pinoak0016, rank 14 out of 16 processors

4294967313 returned output: Hello world from pid 56254, processor pinoak0016, rank 15 out of 16 processors
```

