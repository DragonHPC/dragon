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
distributed dictionary across node by utilizing the policy. By doing this, user can manage data
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
for all iterations, will be measured to evaluate the performance of the dictionary.

We run as follows:
```
dragon ddict_bench.py [-h] [--dict_size] [--value_size] [--num_nodes]
                          [--clients] [--managers_per_node] [--total_mem_size]
                          [--batch_size] [--iterations] [--dict_ops]
```

The example above creates client processes through Dragon native Process. The example below creates
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
in distributed dictionary. Clients write the local simulation results from each iteration into the
corresponding checkpoint and compute the aggregated result for the next training step. Data in the distributed
dictionary is shared across processes, allowing clients to retrieve simulation data written by other
processes. The processes aggregating the results may implicitly wait until the portion of results they
are reading has been written into the dictionary for the current checkpoint. For more details, refer to
checkpointing in the Distributed Dictionary under the API Reference in the Dragon documentation.

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

The above example is the Python version of the PI simulation. The following
example demonstrates how to perform the same PI simulation using the Distributed
Dictionary C++ API. In this example, the dictionary is created from the driver
script `ddict_cpp_driver.py` through the dictionary's Python API, and the
serialized dictionary is passed as an argument to the C++ executable. Each
process represents a single C++ dictionary client. C++ clients attach to the
dictionary by using the given serialized dictionary descriptor.

All data interacting with the Distributed Dictionary C++ API must be in the form
of serializable objects. Those serializable objects are created from the classes
defined and implemented in `serializable.hpp` and `serializable.cpp`. These
classes extend the Serializable interface.

Workers are instances of processes running the program `ddict_pi_sim_train.cpp`.
They compute local average results of the PI simulation in parallel. There's a
single process that runs the program `ddict_pi_sim_aggregate.cpp` to gather local
average results of each iteration written by workers and writes their computed
average result back to the dictionary. The training data is shared across
processes through the distributed dictionary, allowing workers to retrieve global
average results from the dictionary. The workers checkpoint themselves and proceed to
their next iteration continuing until the difference between the results of the
current and last iteration is less than a given delta.

## Usage

`ddict_cpp_driver.py` - The driver script to execute the C++ program. The dictionary is created in the script,
and the serialized dictionary is an argument of the C++ executable run by the worker processes.

`ddict_pi_sim_training.cpp` - The program that multiple worker processes executes
in parallel to contribute to finding the value of PI simultaneously.

`ddict_pi_sim_aggregate.cpp` - The program that is run by a single process to gather and compute the average
results of each iteration from all workers.

```
make

dragon ddict_cpp_driver.py
```

## Restart and cross dictionary synchronization

Under certain conditions related to AI resiliency training, we might need to shut
down the whole program and restart from where the training has been left. This
example demonstrates the capability of resiliency in the Dragon Distributed
Dictionary to restart from a previous checkpoint, retrieve all data and the state
of the dictionary, using redundant dictionary synchronization to recover the data
if necessary and continue the training from the previous state. For more details,
refer to Restart in the Distributed Dictionary under the API Reference in the
Dragon documentation.

```
dragon ddict_restart.py
```

## Checkpoint Persistence Distributed Dictionary

This example demonstrates how checkpoint persistence works in the Dragon Distributed Dictionary. It persists
training checkpoints to disk and restores them. This enables long-running training or simulation jobs to
resume after unexpected shutdowns.

In this example, a neural network model is trained, and its updated weights from each training are saved
to a checkpoint in the Dragon Distributed Dictionary. These checkpoints are stored either in a POSIX directory
or a DAOS pool, depending on the type of persister provided. For the **POSIX persister**, a directory must
be created beforehand and passed via the `persist_path` argument. For the **DAOS persister**, a DAOS pool must
be created in advance and provided as the `persist_path` argument.

One of the goals for this example is to demonstrate how checkpoint persistence works with model training.
The other goal is to validate that checkpoint persistence works correctly by comparing training losses
across runs. If persistence and checkpoint restoration are functioning correctly, the loss value at each iteration
will be identical between runs with and without checkpoint persistence.

This example consists of two parts, both training the same model using the same initial weights and dataset.

### Part 1: `validate` Mode
- Runs a complete training session **without** checkpoint persistence.
- Saves the **initial model weights** (`initial_mode.pth`) and **training data loader** (`train_loader.pth`)
to disk. This will eliminate random factors in the training to ensures the reproducibility of the result
in part 2.

To run in validate mode:
```
dragon ddict_checkpoint_persistence.py --mode validate
```

### Part 2: `persist` and `restore` Mode
- **`persist` mode**
  - Loads the initial weights and data saved during the `validate` run.
  - Performs the **first half of training**, and writes the updated model weights from each iteration to
  checkpoints and persists them with the given persist frequency.
- **`restore` mode**
  - Restores and resumes from the **last persisted checkpoint** saved during thr `persist` mode.
  - Continues the **second half of training**

If checkpoint persistence works correctly, the loss value at every iteration should match those from the
`validate` run.

To run in persist mode and restore mode:
```
dragon ddict_checkpoint_persistence.py --mode persist
dragon ddict_checkpoint_persistence.py --mode restore
```

To run the example with user provided checkpoint persistence constraints:
```
dragon ddict_checkpoint_persistence.py [-h] [--mode] [--managers_per_node] [--working_set_size]
                                [--persister] [--persist_path] [--persist_count]
                                [--persist_frequency]
```

#### Optional arguments:
```
  -h, --help            Show this help message and exit.
  --mode
                        The training mode: Specify one of the modes validate, persist, restore.
  --managers_per_node
                        Specifies the number of managers per node for the dragon distributed dictionary.
  --working_set_size
                        Specifies the number of checkpoints in the working set of the distributed dictionary.
  --persister
                        Specifies the type of the checkpoint persister for the distributed dictionary.
  --persist_path
                        The path where the persisted checkpoints should be saved. For the POSIX persister this
                        is a file path. For the DAOS persister, it is the name of the DAOS pool in which the
                        persisted checkpoints will be save.
  --persist_count
                        The maximum number of persisted checkpoints under the given persist path.
                        If -1 is provided then all persisted checkpoints are preserved.

  --persist_frequency
                        The frequency that checkpoints are persisted. Non-persisted checkpoints are not preserved
                        once they fall out of the working set.
```

