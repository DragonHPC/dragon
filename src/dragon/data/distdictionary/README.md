# Dict benchmark for comparing mp Managed Dict and Dragon-native distributed Dict performance

In order for this benchmark to work properly, we need to set the PYTHONHASHSEED. For example:

```
export PYTHONHASHSEED=1234
```


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
