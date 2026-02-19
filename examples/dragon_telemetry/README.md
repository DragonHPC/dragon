## Using Dragon Telemetry to profile applications

### Usage Scipy Scale Work Example

Simple SciPy example identical to the one in `examples/multiprocessing/numpy-mpi4py-examples` except with telemetry metrics added to it.

```
dragon scipy_scale_work.py [-h] [--num_workers NUM_WORKERS] [--iterations ITERATIONS]
                          [--burns BURN_ITERATIONS] [--dragon] [--size ARRAY_SIZE]
                          [--mem DATASET_MEMORY_FOOTPRINT] [--runtime PER_IMAGE_COMPUTE_TIME]
```

In order to run with telemetry turned on, the `--telemetry-level` option is needed. For example, to collect all level 2 metrics, we need to add `--telemetry-level=2`. To run on two nodes with four workers (default) the commands would be:

```
> salloc --nodes=2 --exclusive
> dragon --telemetry-level=2 scipy_scale_work.py --dragon
```

Metrics will be collected for all telemetry levels that are less than or equal to the given telemetry level argument. User defined metrics are by default set to a telemetry level of 1 but can be changed, as demonstrated in the example.
Running the benchmark as

```
> dragon scipy_scale_work.py --dragon
```

will mean that no telemetry data is collected. All of user generated telemetry data can be left in the user program but will not be collected.

### Usage Merge Sort Example

This example is an introduction to the dragon telemetry analysis client. It is a simple sorting example identical to the one in `examples/multiprocessing/` except the telemetry analysis client is used to monitor the number of python processes on each node. When any node reaches a specified number of processes a multiprocessing event is set to stop the recursion. This example requires that `--telemetry-level` be set to 2 or greater to run. If it were set to less than 2, the number of python processes would not be collected by telemetry and thus the recursion would not terminate gracefully. To run this example on four nodes the commands would be:

```
> salloc --nodes=4 --exclusive
> dragon --telemetry-level=2 merge_sort.py
```

### Usage Slow GPU Example

This example is an introduction to the dragon telemetry analysis `SlowGPU` class, which is based upon the more general `CollectorGroup` class. If GPUs are available, one process per GPU is started. Each process computes the eigenvalues of Wigner random matrices and adds those eigenvalues to the list of eigenvalues for that matrix size. To simulate a slow GPU, a large matrix multiply is done on a node (exlcuding the primary node) on GPU 0. This leads to the collector on that GPU's test kernel taking significantly longer to run than it's peers. This is eventually flagged by the watcher. In that case, the watcher initiates a reboot and will exclude that node on the next run if it is not the primary node. The primary node cannot be excluded as it is the node where the distributed dictionary resides and is required to restart this example from the state prior to the reboot. This example is most interesting when the `--telemetry-level` is set to 3 or greater so that users can observe default GPU metrics. This example can be used as follows:

```
dragon slow_gpu.py [-h] [--plot]
```

To run this example on three nodes with one reserved to swap in, the commands would be:

```
> salloc --nodes=3 --exclusive
> dragon --telemetry-level=3 --nodes=2 --resilient --exhaust-resources slow_gpu.py --plot
```

This will begin with two nodes and remove the non-primary node once the `SlowGPUDetector` triggers a restart. Following the restart, the reserved node will be swapped in and the example will once again run on two nodes. When the `SlowGPUDetector` triggers a restart for the second time, the example will be restarted and continue to run on the single primary node until completion.

If the `--plot` argument is given to the example, a plot of the eigenvalues and the frequencies will be saved to `eigvals.png`.

### Usage Database Dump Example

Simple SciPy example identical to the one above with database dump functionality added to it.

```
dragon scipy_scale_work_db_dump.py [-h] [--num_workers NUM_WORKERS] [--iterations ITERATIONS]
                          [--burns BURN_ITERATIONS] [--dragon] [--size ARRAY_SIZE]
                          [--mem DATASET_MEMORY_FOOTPRINT] [--runtime PER_IMAGE_COMPUTE_TIME]
```

In order to run with telemetry turned on, the `--telemetry-level` option is needed. For example, to collect all level 2 metrics, we need to add `--telemetry-level=2`. To run on two nodes with four workers (default) the commands would be:

```
> salloc --nodes=2 --exclusive
> dragon --telemetry-level=2 scipy_scale_work_db_dump.py --dragon
```

Metrics will be collected for all telemetry levels that are less than or equal to the given telemetry level argument. This example uses the `AnalysisClient` to dump telemetry data to a path specified in the telemetry.yaml config file.

Note that by default, only the last 5 minutes of telemetry data is retained for database dumps. For longer runs, you may want to adjust the `default_tmdb_window` parameter in the telemetry configuration to capture the full runtime. The database collection window depends on this `default_tmdb_window` setting (default: 300 seconds).

To view these metrics after the runtime has exited run the following command:

```
> salloc --nodes=1 --exclusive
> dragon mini_telemetry.py
```

Metrics can be accessed using Grafana as they are for telemetry.

### Setting up Grafana

Please refer to the Installation section in the Dragon Telemetry cookbook.
