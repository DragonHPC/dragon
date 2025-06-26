# Comparing Dragon's Native Pool and an MPI-based Pool

Dragon presents a notably different programming model for parallel applications than more
traditional HPC options like MPI. People coming from an HPC background might be curious
about performance advantages of using Dragon, and this benchmark shows one area where
Dragon outperforms MPI.

## The Dragon Pool vs MPI-based Pool Benchmark

The benchmark is run as follows:

```
dragon dragon_pool_vs_mpi.py [-h] [--num_iters NUM_ITERS]
                             [--items_per_worker ITEMS_PER_WORKER]
                             [--imbalance_factor IMBALANCE_FACTOR]
```

The Dragon work pool relies on a distributed queue to balance the load on each worker.
This leads to improved performance in situations where there's an imbalance in the load
for each worker, especially when each worker is given multiple work items. See the results
section below (or the png files in this directory) for data demonstrating this.

### Arguments

```
  -h, --help            show this help message and exit
  --num_iters NUM_ITERS
                        number of iterations
  --items_per_worker ITEMS_PER_WORKER
                        number of work items per worker per iteration
  --imbalance_factor IMBALANCE_FACTOR
                        tunes how imbalanced the workload is--specifically,
                        (max work item time) / (min work item time)
```

- num_iters:
  > The number of iterations to run for each test in the benchmark.
- items_per_worker:
  > The number of work items given to each worker at each iteration. Larger numbers of
  items per worker generally perform better in a load-balanced work pool like Dragon's.
- imbalance_factor:
  > The ratio of maximum to minimum runtime for any given work item. Similarly to the
  number of items per worker, larger imbalance factors are handled better by Dragon's
  load-balanced work pool.

### Some Results

Running on two nodes and holding the items-per-worker constant at 2, we get the following
speedup results by varying the load-imbalance factor from 1 to 64:

| Load-Imbalance Factor | Speedup      |
| --------------------- | ------------ |
| 1                     | 1.035322908  |
| 2                     | 1.158914635  |
| 4                     | 1.261883247  |
| 8                     | 1.380560469  |
| 16                    | 1.461129635  |
| 32                    | 1.513906656  |
| 64                    | 1.526842195  |

Running on two nodes and holding the imbalance factor constant at 8, we get the following
speedup results by varying the items-per-worker from 1 to 64:

| Items-per-Worker Factor | Speedup      |
| ----------------------- | ------------ |
| 1                       | 0.998713967  |
| 2                       | 1.138566996  |
| 4                       | 1.404856314  |
| 8                       | 1.55692963   |
| 16                      | 1.662223625  |
| 32                      | 1.730707825  |
| 64                      | 1.749542884  |

These results are plotted in the files `speedup_vs_load_imbalance` and `speedup_vs_items_per_worker.png` in this directory.
