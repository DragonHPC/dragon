# Dragon Dictionary Examples

The purpose of examples here is to show the usage of Dragon Distributed Dictionary. These examples
provide an idea of the dictionary interfaces and how different operations can be performed across the
processes using the dictionary. In the examples, a Dragon Distributed Dictionary is always created
from the parent process and passed to other child processes.

## Basic Functionalities of Dragon Distributed Dictionary

The example demonstrates the functionalities of Dragon Distributed Dictionary with multiple client
processes using Dragon native process.

```
dragon demo_ddict.py
```

The example above is not designed to work multi-node. To run the multi-node examples, run:

```
dragon demo_ddict_pool.py [--num_nodes]
```

This example creates multiple client processes through Dragon native Pool across nodes.

### Policy

The example `demo_ddict_manager_placement.py` creates distributed dictionary following the given
policy. The policy is a Dragon infrastructure policy object that enable fine-tuning of process
distribution across node. In this example, we specify the distribution of the managers in
distributed dicitionary across node by utilizing the policy. By doing this, user can manage data
shard distribution across nodes effectively.

```
dragon demo_ddict_manager_placement.py
```

The example above is designed to work multi-node.

## Dragon Distributed Dictionary Benchmark

In this example, we perform multiple read, write, and pop operations with a given key/value size
and measure the latency and throughput of each operation type. The dictionary is initially populated
with batches of kv pairs by mp processes. Each dictionary operation, will be performed by client
processes, where Dragon Distributed Dictionary is passed across. The overall time of each operation
for all iterations, will be measured to evaluate the performance of the dicitonary.

We run as follows:
```
dragon ddict_bench.py [-h] [--dict_size] [--value_size] [--num_nodes]
                          [--clients] [--managers_per_node] [--total_mem_size]
                          [--batch_size] [--iterations] [--dict_ops]
```

The example above creates client processes through Dragon native Porcess. The example below creates
them through Dragon native ProcessGroup, an API to manage group of Dragon processes.

```
dragon ddict_bench_pg.py [-h] [--dict_size] [--value_size] [--num_nodes]
                          [--clients] [--managers_per_node] [--total_mem_size]
                          [--batch_size] [--iterations] [--dict_ops]
```

### Optional arguments:
```
  -h, --help            show this help message and exit
  --dict_size
                        number of total (key, value) pairs inside the dict
  --value_size
                        size of the value (bytes) that are stored in the dict
  --num_nodes
                        number of nodes the dictionary distributed across
  --clients
                        number of client processes performing operations on the dict
  --managers_per_node
                        number of managers per node for the dragon dict
  --total_mem_size
                        total managed memory size for dictionary in GB
  --batch_size
                        number of kv pairs added by each process before dict operations
  --iterations
                        number of iterations
  --dict_ops
                        choose the operations to be performed on the dict -- 0 to set the values,
                        1 to get the values, 2 for both, 3 for deletes (includes setting the values
                        for now to continue further deletes)
```

The Dragon Distributed Dictionary in this example is designed to work on multi-node setup.

## Checkpointing in Distributed Dictionary - compute the value of PI

The following examples computes the value of constant π = 3.1415 ... while making use of checkpointing
in distirbuted dictionary. Clients write the local simulation results from each iteration into the
corresponding checkpoint and computes the aggregated result for the next training step. Data in distributed dictionary is shared across processes, allowing clients to retrieve simulation data written by other
processes. The processes aggregating the results may implicitly wait until the portion of results they
are reading has been written into the dictionary for the current checkpoint. For more details, refer to
checkpoint in Distributed Dictionary under API Reference in Dragon documentation.

To run the example:
```
dragon ddict_checkpoint_pi.py [--digits] [--trace]
```

#### Optional arguments:
```
  -h, --help            show this help message and exit
  --digits
                        the precision to approximate the value to
  --trace
                        set to true to print the result of each training step for all clients
```

The above example is the Python version of PI simulation. The following example demonstrates how to
perform the same PI simulation using Distributed Dictionary C++ API. In this example, the dictionary
is created from the driver script `ddict_cpp_driver.py` through dictionary Python API, and the serialized
dictionary is passed as an argument to the C++ executable. Each processes represents a single C++
dictionary client. The C++ clients then attach to the dictionary while creating the Dictionary objects
in the C++ program by using the given serilized dictionary.

All data interacting with the Distributed Dictionary C++ API must be in the form of serializable objects.
Those serializable objects are created from the classes defined and implemented in `serializable.hpp`
and `serializable.cpp`. These classes extend the Serializable interface.

Workers are instances of processes running program `ddict_pi_sim_train.cpp`, they compute local average
results of PI simulation in parallel. There's a single process that runs program `ddict_pi_sim_aggregate.cpp`
to gather local average reuslts of each iteration written by workers and writes the global average result
back to the dictionary. The training data is shared across processes through the distributed dictionary,
allowing workers to retrieve global average result from the dictionary. The workers checkpoint itself to
proceed to the next iteration and continues until the difference between the results of current and last
iteration is less than a given delta.

## Usage

`ddict_cpp_driver.py` - The driver script to execute C++ program. The dictionary is created in the script,
and the seriaized dictionary is an argument of the C++ executable runs by the worker processes.

`serializable.hpp` - The header file that defined the classes extending from the Serializable interface.
All data interacting with dictionary C++ API must be the serializable objects created by the classes
defined in this header.

`serializable.cpp` - The script implements the class functions defined in `serializable.hpp`.

`ddict_pi_sim_training.cpp` - The program that multiple worker processes executes in parallel to compute
the value PI simultaneously.

`ddict_pi_sim_aggregate.cpp` - The program that runs by a single process to gather and compute the average
results of each iteration from all workers.

```
make

dragon ddict_cpp_driver.py
```

## Restart and cross dictionary synchronization

Under certain condition related to AI resiliency training, we might need to shut down the whole program
and restart from where the training has been left. This example demonstrates the capability of resiliency
in Dragon Distributed Dictionary to restart from previous checkpoint, retrieve all data and states of the
dictionary, cross dictionary synchronization to recover the data if necessary and continues the training
from preivous state. For more details, refer to Restart in Data/Distributed Dictionary under API Reference
in Dragon documentation.

```
dragon ddict_restart.py
```