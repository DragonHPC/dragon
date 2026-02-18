# MPI Examples

This directory contains examples showing how to use the Dragon native API to
start MPI applications.

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
> dragon policy_demo.py
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

## mpi_hello_world Demo

The file `mpi_hello_world.py` contains a program that will run an MPI application with either a Cray PMI or PMIx backend. This example highlights that Dragon can start MPI applications using several different MPI backends.

### Running the example

1. Get an allocation of nodes
   ```
   > salloc --nodes=2 --exclusive
   ```
2. Run the dragon example!
   ```
   > dragon mpi_hello_world.py --pmi cray ./mpi_hello
   ```

   NOTE: The "./" in front of "./mpi_hello" is important. The example will fail in ugly ways without it.

### Example output

```
> dragon mpi_hello_world.py --pmi cray ./mpi_hello
iteration 0: node swap
Hello, world, I am 2 of 4 on host pinoak0012, (MPI VERSION    : CRAY MPICH version 9.0.1.498 (ANL base 4.1.2)
MPI BUILD INFO : Wed Jul 16  9:39 2025 (git hash 0848216)
, 121)
Hello, world, I am 1 of 4 on host pinoak0012, (MPI VERSION    : CRAY MPICH version 9.0.1.498 (ANL base 4.1.2)
MPI BUILD INFO : Wed Jul 16  9:39 2025 (git hash 0848216)
, 121)
Hello, world, I am 0 of 4 on host pinoak0012, (MPI VERSION    : CRAY MPICH version 9.0.1.498 (ANL base 4.1.2)
MPI BUILD INFO : Wed Jul 16  9:39 2025 (git hash 0848216)
, 121)
Hello, world, I am 3 of 4 on host pinoak0012, (MPI VERSION    : CRAY MPICH version 9.0.1.498 (ANL base 4.1.2)
MPI BUILD INFO : Wed Jul 16  9:39 2025 (git hash 0848216)
, 121)
...
iteration 0: iterative_execution
  Replay 0 of PMIx ProcessGroup
Hello, world, I am 1 of 9 on host pinoak0012, (MPI VERSION    : CRAY MPICH version 9.0.1.498 (ANL base 4.1.2)
MPI BUILD INFO : Wed Jul 16  9:39 2025 (git hash 0848216)
, 121)
Hello, world, I am 0 of 9 on host pinoak0011, (MPI VERSION    : CRAY MPICH version 9.0.1.498 (ANL base 4.1.2)
MPI BUILD INFO : Wed Jul 16  9:39 2025 (git hash 0848216)
, 121)
Hello, world, I am 7 of 9 on host pinoak0012, (MPI VERSION    : CRAY MPICH version 9.0.1.498 (ANL base 4.1.2)
MPI BUILD INFO : Wed Jul 16  9:39 2025 (git hash 0848216)
, 121)
Hello, world, I am 5 of 9 on host pinoak0012, (MPI VERSION    : CRAY MPICH version 9.0.1.498 (ANL base 4.1.2)
MPI BUILD INFO : Wed Jul 16  9:39 2025 (git hash 0848216)
, 121)
Hello, world, I am 3 of 9 on host pinoak0012, (MPI VERSION    : CRAY MPICH version 9.0.1.498 (ANL base 4.1.2)
MPI BUILD INFO : Wed Jul 16  9:39 2025 (git hash 0848216)
, 121)
Hello, world, I am 6 of 9 on host pinoak0011, (MPI VERSION    : CRAY MPICH version 9.0.1.498 (ANL base 4.1.2)
MPI BUILD INFO : Wed Jul 16  9:39 2025 (git hash 0848216)
, 121)
Hello, world, I am 8 of 9 on host pinoak0011, (MPI VERSION    : CRAY MPICH version 9.0.1.498 (ANL base 4.1.2)
MPI BUILD INFO : Wed Jul 16  9:39 2025 (git hash 0848216)
, 121)
Hello, world, I am 4 of 9 on host pinoak0011, (MPI VERSION    : CRAY MPICH version 9.0.1.498 (ANL base 4.1.2)
MPI BUILD INFO : Wed Jul 16  9:39 2025 (git hash 0848216)
, 121)
Hello, world, I am 2 of 9 on host pinoak0011, (MPI VERSION    : CRAY MPICH version 9.0.1.498 (ANL base 4.1.2)
MPI BUILD INFO : Wed Jul 16  9:39 2025 (git hash 0848216)
, 121)
...
iteration 0: concurrent_execution
Hello, world, I am 0 of 8 on host pinoak0011, (MPI VERSION    : CRAY MPICH version 9.0.1.498 (ANL base 4.1.2)
MPI BUILD INFO : Wed Jul 16  9:39 2025 (git hash 0848216)
, 121)
Hello, world, I am 1 of 8 on host pinoak0012, (MPI VERSION    : CRAY MPICH version 9.0.1.498 (ANL base 4.1.2)
MPI BUILD INFO : Wed Jul 16  9:39 2025 (git hash 0848216)
, 121)
Hello, world, I am 4 of 8 on host pinoak0011, (MPI VERSION    : CRAY MPICH version 9.0.1.498 (ANL base 4.1.2)
MPI BUILD INFO : Wed Jul 16  9:39 2025 (git hash 0848216)
, 121)
Hello, world, I am 3 of 8 on host pinoak0012, (MPI VERSION    : CRAY MPICH version 9.0.1.498 (ANL base 4.1.2)
MPI BUILD INFO : Wed Jul 16  9:39 2025 (git hash 0848216)
, 121)
Hello, world, I am 2 of 8 on host pinoak0011, (MPI VERSION    : CRAY MPICH version 9.0.1.498 (ANL base 4.1.2)
MPI BUILD INFO : Wed Jul 16  9:39 2025 (git hash 0848216)
, 121)
Hello, world, I am 7 of 8 on host pinoak0012, (MPI VERSION    : CRAY MPICH version 9.0.1.498 (ANL base 4.1.2)
MPI BUILD INFO : Wed Jul 16  9:39 2025 (git hash 0848216)
, 121)
Hello, world, I am 5 of 8 on host pinoak0012, (MPI VERSION    : CRAY MPICH version 9.0.1.498 (ANL base 4.1.2)
MPI BUILD INFO : Wed Jul 16  9:39 2025 (git hash 0848216)
, 121)
Hello, world, I am 6 of 8 on host pinoak0011, (MPI VERSION    : CRAY MPICH version 9.0.1.498 (ANL base 4.1.2)
MPI BUILD INFO : Wed Jul 16  9:39 2025 (git hash 0848216)
, 121)
Hello, world, I am 1 of 9 on host pinoak0011, (MPI VERSION    : CRAY MPICH version 9.0.1.498 (ANL base 4.1.2)
MPI BUILD INFO : Wed Jul 16  9:39 2025 (git hash 0848216)
, 121)
Hello, world, I am 0 of 9 on host pinoak0012, (MPI VERSION    : CRAY MPICH version 9.0.1.498 (ANL base 4.1.2)
MPI BUILD INFO : Wed Jul 16  9:39 2025 (git hash 0848216)
, 121)
Hello, world, I am 8 of 9 on host pinoak0012, (MPI VERSION    : CRAY MPICH version 9.0.1.498 (ANL base 4.1.2)
MPI BUILD INFO : Wed Jul 16  9:39 2025 (git hash 0848216)
, 121)
Hello, world, I am 5 of 9 on host pinoak0011, (MPI VERSION    : CRAY MPICH version 9.0.1.498 (ANL base 4.1.2)
MPI BUILD INFO : Wed Jul 16  9:39 2025 (git hash 0848216)
, 121)
Hello, world, I am 4 of 9 on host pinoak0012, (MPI VERSION    : CRAY MPICH version 9.0.1.498 (ANL base 4.1.2)
MPI BUILD INFO : Wed Jul 16  9:39 2025 (git hash 0848216)
, 121)
Hello, world, I am 7 of 9 on host pinoak0011, (MPI VERSION    : CRAY MPICH version 9.0.1.498 (ANL base 4.1.2)
MPI BUILD INFO : Wed Jul 16  9:39 2025 (git hash 0848216)
, 121)
Hello, world, I am 6 of 9 on host pinoak0012, (MPI VERSION    : CRAY MPICH version 9.0.1.498 (ANL base 4.1.2)
MPI BUILD INFO : Wed Jul 16  9:39 2025 (git hash 0848216)
, 121)
Hello, world, I am 3 of 9 on host pinoak0011, (MPI VERSION    : CRAY MPICH version 9.0.1.498 (ANL base 4.1.2)
MPI BUILD INFO : Wed Jul 16  9:39 2025 (git hash 0848216)
, 121)
Hello, world, I am 2 of 9 on host pinoak0012, (MPI VERSION    : CRAY MPICH version 9.0.1.498 (ANL base 4.1.2)
MPI BUILD INFO : Wed Jul 16  9:39 2025 (git hash 0848216)
, 121)
```

## hpc_workflow_demo

This is a consumer-producer example where the producer process will start
`num_ranks` copies of the osu_alltoall MPI application. The stdout output
from the head process will be piped via Dragon to a separate process
responsible for parsing the osu_alltoall results and putting the results
onto the results queue. This example directly uses the Dragon Global
Services (GS) API to instantiate the consumers and producer tasks.

### Running the example

1. Get an allocation of nodes
   ```
   > salloc --nodes=2 --exclusive
   ```
2. Run the dragon example!
   ```
   > dragon hpc_workflow_demo.py
   ```

### Example output

```
> dragon hpc_workflow_demo.py
INFO:api_setup:connecting to infrastructure from 1124096
INFO:api_setup:debug entry hooked
INFO:main:Starting consumer process
INFO:main:Starting a new producer
INFO:main:Starting a new producer
INFO:consumer:reading from result_queue
INFO:producer 1:Starting producer (num_ranks=62)
INFO:producer 0:Starting producer (num_ranks=62)
INFO:producer 1:Starting parse_results process for p_uid=4294967300
INFO:producer 1:node 0 has 31 ranks, node 1 has 31 ranks
INFO:producer 1:Waiting for group to finish
INFO:producer 0:Starting parse_results process for p_uid=4294967362
INFO:producer 0:node 0 has 31 ranks, node 1 has 31 ranks
INFO:producer 0:Waiting for group to finish
INFO:parse_results 1:Parsing stdout from stdout connection
INFO:parse_results 0:Parsing stdout from stdout connection
INFO:consumer:{0: ('1', '28.33')}
INFO:consumer:{0: ('2', '26.58')}
INFO:consumer:{0: ('4', '30.91')}
INFO:consumer:{0: ('8', '32.24')}
INFO:consumer:{0: ('16', '37.41')}
INFO:consumer:{0: ('32', '35.58')}
INFO:consumer:{1: ('1', '30.70')}
INFO:consumer:{0: ('64', '71.12')}
INFO:consumer:{1: ('2', '30.15')}
INFO:consumer:{1: ('4', '33.69')}
INFO:consumer:{0: ('128', '76.71')}
INFO:consumer:{1: ('8', '33.92')}
INFO:consumer:{1: ('16', '35.22')}
INFO:consumer:{1: ('32', '36.62')}
INFO:consumer:{0: ('256', '95.05')}
INFO:consumer:{1: ('64', '75.81')}
INFO:consumer:{0: ('512', '94.15')}
INFO:consumer:{1: ('128', '81.95')}
INFO:consumer:{1: ('256', '101.54')}
INFO:consumer:{0: ('1024', '219.38')}
INFO:consumer:{1: ('512', '102.38')}
INFO:consumer:{0: ('2048', '232.57')}
INFO:consumer:{1: ('1024', '228.43')}
INFO:consumer:{0: ('4096', '226.96')}
INFO:consumer:{1: ('2048', '237.66')}
INFO:parse_results 0:Done
INFO:producer 0:Done
INFO:main:at least one producer has exited
INFO:main:Starting a new producer
INFO:producer 2:Starting producer (num_ranks=62)
INFO:consumer:{1: ('4096', '215.88')}
INFO:parse_results 1:Done
INFO:producer 2:Starting parse_results process for p_uid=4294967427
INFO:producer 1:Done
INFO:producer 2:node 0 has 31 ranks, node 1 has 31 ranks
INFO:producer 2:Waiting for group to finish
INFO:main:at least one producer has exited
INFO:main:Starting a new producer
INFO:parse_results 2:Parsing stdout from stdout connection
INFO:producer 3:Starting producer (num_ranks=62)
INFO:producer 3:Starting parse_results process for p_uid=4294967491
INFO:producer 3:node 0 has 31 ranks, node 1 has 31 ranks
INFO:producer 3:Waiting for group to finish
INFO:parse_results 3:Parsing stdout from stdout connection
INFO:consumer:{2: ('1', '25.26')}
INFO:consumer:{2: ('2', '24.93')}
INFO:consumer:{2: ('4', '27.85')}
INFO:consumer:{2: ('8', '28.80')}
INFO:consumer:{2: ('16', '31.21')}
INFO:consumer:{2: ('32', '32.55')}
INFO:consumer:{2: ('64', '78.85')}
INFO:consumer:{3: ('1', '29.43')}
INFO:consumer:{3: ('2', '28.68')}
INFO:consumer:{2: ('128', '77.11')}
INFO:consumer:{3: ('4', '31.55')}
INFO:consumer:{3: ('8', '32.06')}
INFO:consumer:{3: ('16', '33.34')}
INFO:consumer:{2: ('256', '95.05')}
INFO:consumer:{3: ('32', '33.60')}
INFO:consumer:{3: ('64', '71.14')}
INFO:consumer:{2: ('512', '93.47')}
INFO:consumer:{3: ('128', '75.93')}
INFO:consumer:{3: ('256', '96.60')}
INFO:consumer:{2: ('1024', '215.37')}
INFO:consumer:{3: ('512', '93.47')}
INFO:consumer:{2: ('2048', '225.93')}
INFO:consumer:{3: ('1024', '219.17')}
INFO:consumer:{2: ('4096', '220.97')}
INFO:consumer:{3: ('2048', '226.03')}
INFO:parse_results 2:Done
INFO:producer 2:Done
INFO:main:at least one producer has exited
INFO:main:Starting a new producer
INFO:consumer:{3: ('4096', '184.44')}
INFO:producer 4:Starting producer (num_ranks=62)
INFO:parse_results 3:Done
INFO:producer 3:Done
INFO:main:at least one producer has exited
INFO:producer 4:Starting parse_results process for p_uid=4294967555
INFO:producer 4:node 0 has 31 ranks, node 1 has 31 ranks
INFO:producer 4:Waiting for group to finish
INFO:parse_results 4:Parsing stdout from stdout connection
INFO:consumer:{4: ('1', '19.63')}
INFO:consumer:{4: ('2', '18.67')}
INFO:consumer:{4: ('4', '21.37')}
INFO:consumer:{4: ('8', '20.88')}
INFO:consumer:{4: ('16', '21.78')}
INFO:consumer:{4: ('32', '23.10')}
INFO:consumer:{4: ('64', '48.64')}
INFO:consumer:{4: ('128', '53.32')}
INFO:consumer:{4: ('256', '71.30')}
INFO:consumer:{4: ('512', '78.04')}
INFO:consumer:{4: ('1024', '157.02')}
INFO:consumer:{4: ('2048', '170.86')}
INFO:consumer:{4: ('4096', '187.17')}
INFO:parse_results 4:Done
INFO:producer 4:Done
INFO:main:at least one producer has exited
INFO:main:Shutting down
INFO:consumer:Done
INFO:main:Done
```

## hpc_workflow_demo_highlevel.py

This is a consumer-producer example where the producer process will start
`num_ranks` copies of the osu_alltoall MPI application. The stdout output
from the head process will be piped via Dragon to a separate process
responsible for parsing the osu_alltoall results and putting the results
onto the results queue. This example uses the Dragon Native APIs to
instantiate the consumers and producer tasks.

### Running the example

1. Get an allocation of nodes
   ```
   > salloc --nodes=2 --exclusive
   ```
2. Run the dragon example!
   ```
   > dragon hpc_workflow_demo_highlevel.py
   ```

### Example output

```
> dragon hpc_workflow_demo_highlevel.py
INFO:api_setup:connecting to infrastructure from 1125562
INFO:api_setup:debug entry hooked
INFO:main:Starting consumer process
INFO:main:Starting a new producer
INFO:main:Starting a new producer
INFO:consumer:reading from result_queue
INFO:producer 0:Starting producer (num_ranks=62)
INFO:producer 1:Starting producer (num_ranks=62)
INFO:producer 1:Starting parse_results process for puid=4294967302
INFO:producer 1:node 0 has 31 ranks, node 1 has 31 ranks
INFO:producer 1:Waiting for group to finish
INFO:parse_results 1:Parsing stdout from stdout connection
INFO:producer 0:Starting parse_results process for puid=4294967364
INFO:producer 0:node 0 has 31 ranks, node 1 has 31 ranks
INFO:producer 0:Waiting for group to finish
INFO:parse_results 0:Parsing stdout from stdout connection
INFO:consumer:{1: ('1', '28.72')}
INFO:consumer:{1: ('2', '29.66')}
INFO:consumer:{1: ('4', '32.87')}
INFO:consumer:{1: ('8', '30.92')}
INFO:consumer:{0: ('1', '28.75')}
INFO:consumer:{1: ('16', '33.43')}
INFO:consumer:{0: ('2', '28.05')}
INFO:consumer:{1: ('32', '34.58')}
INFO:consumer:{0: ('4', '31.46')}
INFO:consumer:{0: ('8', '30.94')}
INFO:consumer:{1: ('64', '75.03')}
INFO:consumer:{0: ('16', '32.42')}
INFO:consumer:{0: ('32', '34.09')}
INFO:consumer:{1: ('128', '82.98')}
INFO:consumer:{0: ('64', '77.23')}
INFO:consumer:{1: ('256', '107.89')}
INFO:consumer:{0: ('128', '85.94')}
INFO:consumer:{0: ('256', '98.90')}
INFO:consumer:{1: ('512', '103.11')}
INFO:consumer:{0: ('512', '92.45')}
INFO:consumer:{1: ('1024', '225.29')}
INFO:consumer:{0: ('1024', '223.83')}
INFO:consumer:{1: ('2048', '232.63')}
INFO:consumer:{0: ('2048', '248.27')}
INFO:consumer:{1: ('4096', '244.83')}
INFO:parse_results 1:Done
INFO:consumer:{0: ('4096', '224.46')}
INFO:parse_results 0:Done
INFO:producer 0:Done
INFO:producer 1:Done
INFO:main:at least one producer has exited
INFO:main:Starting a new producer
INFO:main:at least one producer has exited
INFO:main:Starting a new producer
INFO:producer 2:Starting producer (num_ranks=62)
INFO:producer 3:Starting producer (num_ranks=62)
INFO:producer 2:Starting parse_results process for puid=4294967432
INFO:producer 3:Starting parse_results process for puid=4294967494
INFO:producer 2:node 0 has 31 ranks, node 1 has 31 ranks
INFO:producer 2:Waiting for group to finish
INFO:producer 3:node 0 has 31 ranks, node 1 has 31 ranks
INFO:producer 3:Waiting for group to finish
INFO:parse_results 3:Parsing stdout from stdout connection
INFO:parse_results 2:Parsing stdout from stdout connection
INFO:consumer:{2: ('1', '28.21')}
INFO:consumer:{2: ('2', '27.68')}
INFO:consumer:{2: ('4', '33.11')}
INFO:consumer:{2: ('8', '32.37')}
INFO:consumer:{2: ('16', '30.83')}
INFO:consumer:{3: ('1', '29.39')}
INFO:consumer:{2: ('32', '32.55')}
INFO:consumer:{3: ('2', '28.72')}
INFO:consumer:{3: ('4', '31.86')}
INFO:consumer:{2: ('64', '71.53')}
INFO:consumer:{3: ('8', '32.38')}
INFO:consumer:{3: ('16', '33.83')}
INFO:consumer:{2: ('128', '77.57')}
INFO:consumer:{3: ('32', '35.89')}
INFO:consumer:{2: ('256', '97.38')}
INFO:consumer:{3: ('64', '79.99')}
INFO:consumer:{3: ('128', '81.65')}
INFO:consumer:{2: ('512', '97.17')}
INFO:consumer:{3: ('256', '98.47')}
INFO:consumer:{3: ('512', '95.33')}
INFO:consumer:{2: ('1024', '215.02')}
INFO:consumer:{3: ('1024', '218.71')}
INFO:consumer:{2: ('2048', '234.71')}
INFO:consumer:{3: ('2048', '246.41')}
INFO:consumer:{2: ('4096', '237.92')}
INFO:parse_results 2:Done
INFO:consumer:{3: ('4096', '209.26')}
INFO:parse_results 3:Done
INFO:producer 2:Done
INFO:main:at least one producer has exited
INFO:main:Starting a new producer
INFO:producer 4:Starting producer (num_ranks=62)
INFO:producer 3:Done
INFO:main:at least one producer has exited
INFO:producer 4:Starting parse_results process for puid=4294967560
INFO:producer 4:node 0 has 31 ranks, node 1 has 31 ranks
INFO:producer 4:Waiting for group to finish
INFO:parse_results 4:Parsing stdout from stdout connection
INFO:consumer:{4: ('1', '21.28')}
INFO:consumer:{4: ('2', '20.44')}
INFO:consumer:{4: ('4', '21.74')}
INFO:consumer:{4: ('8', '22.88')}
INFO:consumer:{4: ('16', '21.84')}
INFO:consumer:{4: ('32', '23.96')}
INFO:consumer:{4: ('64', '48.34')}
INFO:consumer:{4: ('128', '56.06')}
INFO:consumer:{4: ('256', '72.34')}
INFO:consumer:{4: ('512', '77.96')}
INFO:consumer:{4: ('1024', '151.38')}
INFO:consumer:{4: ('2048', '181.56')}
INFO:consumer:{4: ('4096', '178.74')}
INFO:parse_results 4:Done
INFO:producer 4:Done
INFO:main:at least one producer has exited
INFO:main:Shutting down
INFO:consumer:Done
INFO:main:Done
```
