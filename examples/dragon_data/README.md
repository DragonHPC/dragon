# Dragon Dictionary Examples

The purpose of examples here is to show the usage of Dragon Dictionary. These examples provide an idea of the
dictionary interfaces, how different operations can be performed across the processes using the dictionary.

In order for these dictionary examples to work properly, we need to set the PYTHONHASHSEED. For example:

```
export PYTHONHASHSEED=1234
```

## Example to demonstrate distributed dictionary functionality

This example demonstrates the functionality of the dragon dictionary. Dictionary object will be passed
to the client processes, that store and retrieve data from the dictinoary. Internally dragon dictionary
makes use of the Dragon native components like Channels, Pools, Transport Agent etc.

We run as follows:
```
dragon dist_dict_client.py [--num_nodes] [--managers_per_node] [--total_mem_size]
```

#### Optional arguments:
```
  -h, --help            show this help message and exit

  --num_nodes
                        number of nodes the dictionary distributed across
  --managers_per_node
                        number of managers per node for the dragon dict
  --total_mem_size
                        total managed memory size for dictionary in GB
```

The dragon dictionary in this example works on a multi-node setup.


## Dict benchmark for comparing mp Manager Dict and dragon-native Distributed Dict performance

This example evaluates the performance comparison between dragon dictionary and multiprocessing dictionary.
The dictionary is initially populated with batches of kv pairs by mp processes. Each dictionary operation,
will be performed by client processes, where dragon dictionary is passed across. The overall time of each
operation for all iterations, will be measured to evaluate the performance of the dragon dicitonary.
Internally dragon dictionary makes use of the Dragon native components like Channels, Pools, Transport Agent etc.

We run as follows:
```
dragon dist_dict_bench.py [-h] [--dragon] [--dict_size] [--value_size] [--num_nodes]
                          [--clients] [--managers_per_node] [--total_mem_size]
                          [--batch_size] [--iterations] [--dict_ops]
```

#### Optional arguments:
```
  -h, --help            show this help message and exit

  --dragon
                        whether to use Dragon or a Managed dictionary with Python Multiprocessing
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

The dragon dictionary in this example is designed to work on multi-node setup.
