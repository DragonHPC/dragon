# Dragon Unit Tests

This directory contains a selection of unit and acceptance tests for Dragon components.

The unittests tests can be run to validate very low level components of the Dragon software stack:

- Shared locks
- Managed Memory
- Priority heap
- Channels

The acceptance tests validate multi-node functionality of high level Python multiprocessing objects over Dragon:

- Barrier
- Connection
- Lock
- (Dragon-specific) Machine information
- Pool
- Process
- Queue
- Value
- Array
- Dictionary

## Running the Tests

A C code using MPI will be compiled during the make process to run the basic MPI/PMI tests. On Cray systems, you may
need to load PrgEnv-gnu or modify the CC value in multi-node/Makefile to point at a MPI C compiler wrapper:

```
module swap PrgEnv-cray PrgEnv-gnu
```

for example.

To run the acceptance tests execute the following inside an allocation of 2 nodes (e.g., salloc --nodes=2):

```
make
```

To run the low level unit tests, execute the following on any workstation:

```
make unittest
```

In order for the dictionary examples to work properly, we need to set the PYTHONHASHSEED. For example:

```
export PYTHONHASHSEED=1234
```
