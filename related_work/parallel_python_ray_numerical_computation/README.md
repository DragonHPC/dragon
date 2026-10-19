# Parallel Python Ray Numerical Computation

In the blog post [10x Faster Parallel Python Without Python Multiprocessing](https://towardsdatascience.com/10x-faster-parallel-python-without-python-multiprocessing-e5017c93cce1), a comparison is made between Python's multiprocessing library and the [Ray](https://ray.io/) library. The "blog post benchmarks three workloads that aren’t easily expressed with Python multiprocessing and compares Ray, Python multiprocessing, and serial Python code."

The code in this directory is a lightly modified version of the code given in the above article and is used to compare Ray to a similar implementation using Dragon. The Dragon version of this code can be found under `/examples/multiprocessing/numpy-mpi4py-examples/scipy_scale_work.py`.


## Usage
```
dragon parallel_python_ray_numerical_computation.py [-h]
                        [--num_workers NUM_WORKERS]
                        [--iterations ITERATIONS]
                        [--burns BURN_ITERATIONS]
                        [--size ARRAY_SIZE]
                        [--mem DATASET_MEMORY_FOOTPRINT]
                        [--runtime PER_IMAGE_COMPUTE_TIME]
                        [--multinode MULTIPLE_NODES]
                        [--ipaddress HEAD_NODE_IP_ADDRESS]
```

## Multi-node

In order to run multi-node, we need to set up a Ray cluster. For this purpose, we have created a script that manually sets up the cluster by using Dragon's network configurator `dragon-network-config` to get the ip addresses of all the nodes in our allocation. This means that we need Dragon loaded in the login node.

Note that we need to make the following changes in the scripts:
- We need a python environment (in our case `_env`) which has Ray and the other requirements installed (`requirements.txt` in this directory).
- The `manual_launch.sh` script needs to be updated with the correct path of this environment (`python_env_path` variable).
- Both `manual_launch.sh` and `exec_per_node.sh` scripts need to be updated with the correct environment activation command (replace `_env/bin/activate` command).
- Last, variable `ray_script_path` in `manual_launch.sh` needs to be updated with the path that `manual_launch.sh`, `exec_per_node.sh` and `get_ips_from_json.py` scripts are located.
- Change the permissions on `manual_launch.sh` to be able to execute.

Let us assume that we get a slurm allocation of 8 nodes and we want to use 128 cpus/workers per node. We do the following in the login node:

```
> salloc --nodes=8 --exclusive
> ./manual_launch.sh 128 > output.txt
> grep "Head IP address:" output.txt
```

We can verify that the Ray cluster is up and running if we run `ray status` from the head node or one of the worker nodes.

The `manual_launch.sh` script prints the ip address of the head node of the Ray cluster, which we need for the Ray initialization in our python code. By grepping the output file, we can copy the head ip address.
Then, if for example the ip address is 120.130.40.50, we do the following to run our python code using all nodes:

```
> ssh 120.130.40.50
> python3 parallel_python_ray_numerical_computation.py --multinode --ipaddress 120.130.40.50
```

As shown above, we want to run the python code from the head node. Don't forget to activate the python environment with Ray installed.