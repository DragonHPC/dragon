## Examples for testing NumPy and SciPy packages with Dragon runtime, as well as standard Python Multiprocessing.

### Usage

Simple NumPy example. We can succeed scaling with respect to the number of workers, the workload by
controlling the size of the NumPy array that each worker is working on and the number of work items, i.e., the
number of NumPy arrays that the workers will work on.

```
dragon numpy_scale_work.py [-h] [--num_workers NUM_WORKERS] [--iterations ITERATIONS]
                          [--burns BURN_ITERATIONS] [--dragon] [--size ARRAY_SIZE] [--work_items WORK_ITEMS]
```

Simple SciPy example. We can succeed scaling with respect to the number of workers, the workload by controlling the size of the NumPy array that each worker is working on and the number of work items, i.e., the number of NumPy arrays that the workers will work on. We can also control the compute time spent per image with the `runtime` argument.
```
dragon scipy_scale_work.py [-h] [--num_workers NUM_WORKERS] [--iterations ITERATIONS]
                          [--burns BURN_ITERATIONS] [--dragon] [--size ARRAY_SIZE]
                          [--mem DATASET_MEMORY_FOOTPRINT] [--runtime PER_IMAGE_COMPUTE_TIME]
```

### Multi-node with Dragon

In order to run multi-node with Dragon, the `--dragon` option is needed. For example, to run `scipy_scale_work.py` on 2 nodes, with 4 workers (default), we do:
```
> salloc --nodes=2 --exclusive
> dragon scipy_scale_work.py --dragon
```

## Simple mpi4py benchmarks for comparison with Dragon.

In order to install mpi4py, an MPI implementation is needed.

```
# Set $PATH to include the path of mpicc compiler wrapper.
export PATH=$PATH:/path/to/mpicc

export MPICC=mpicc
pip3 install mpi4py --user

# Then run the mpi4py script as follows (with 2 workers):
mpirun -n 2 python3 [my_mpi4py.py]
```

More details can be found here: https://mpi4py.readthedocs.io/en/stable/install.html


### MPI4PY_AA_BENCH - An all-to-all benchmark using mpi4py

#### Example Output when using OpenMPI
```
> mpirun -n 16 python3 mpi4py_aa_bench.py --iterations 100 --burn_iterations 5 --lg_message_size 21 --with_bytes
workers=16 its=100 msg_size_in_k=2048.0 completion_time_in_ms=32.895 bw_in_mb_sec=972.81
```


#### Optional arguments:
```
  -h, --help            show this help message and exit

  --iterations ITERATIONS
                        number of iterations to do
  --burn_iterations BURN_ITERATIONS
                        number of iterations to burn first time
  --lg_message_size LG_MESSAGE_SIZE
                        log base 2 of msg size between each pair
  --with_bytes
                        creates a payload using bytearray; if absent, the payload is a list of python objects of the given message size
```



### MPI4PY_P2P_LAT - A point-to-point ping-pong latency test using mpi4py

#### Example Output when using OpenMPI
```
> mpirun -n 2 python3 mpi4py_p2p_lat.py --iterations 100 --lg_max_message_size 21 --with_bytes
Msglen [B]   Lat [usec]
2  4.331087693572044
4  4.354733973741531
8  4.145447164773941
16  4.1770003736019135
32  4.221387207508087
64  4.504118114709854
128  4.40133735537529
256  5.666725337505341
512  4.659406840801239
1024  4.950165748596191
2048  5.435477942228317
4096  7.172068580985069
8192  8.6896400898695
16384  11.302977800369263
32768  15.469072386622429
65536  27.288533747196198
131072  56.31423555314541
262144  124.77216310799122
524288  529.5558739453554
1048576  573.2435267418623
2097152  1184.68233384192
```

#### Optional arguments:
```
  -h, --help            show this help message and exit

  --iterations ITERATIONS
                        number of iterations to do
  --lg_message_size LG_MESSAGE_SIZE
                        log base 2 of msg size between each pair
  --with_bytes
                        creates a payload using bytearray; if absent, the payload is a list of python objects of the given message size
```
