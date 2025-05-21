## Using Dragon Telemetry to profile applications 

### Usage

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

### Setting up Grafana 

Please refer to the Installation section in the Dragon Telemetry cookbook.
