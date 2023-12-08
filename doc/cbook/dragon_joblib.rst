Dragon JobLib Examples
++++++++++++++++++++++
The subdirectory `JobLib` contains the joblib examples and benchmarks that are compatible with Dragon.

JobLib is used for on demand computing, transparent parallelization, data tracking, and data flow inspection.
Dragon allows for further optimization of multiprocessing joblib workloads For the multi-node systems.

The following use cases compare the performance of joblib workloads using `dragon` and base multiprocessing.
It is important to note that the joblib backend must be set to `multiprocessing` for the Dragon package
to work without errors.

The most interesting use cases involve joblib's `Parallel` function. Most of the use cases build around `Parallel`. `Parallel` allows for readable code with proper argument construction,
informative tracebacks, the ability to turn on and off parallel computing with `n_jobs`, efficient memory usage, and flexible pickling control.

The code demonstrates the following key concepts working with Dragon:

* How to write joblib programs that can run with Dragon and base multiprocessing
* A comparison of joblib with `dragon`, base multiprocessing, and multi-node with larger Dragon processes.

The set up for Single-node run for both base multiprocessing and `dragon`: For the single-node run, both base multiprocessing and Dragon are compared. The runs utilized a Single-node with 2 AMD EPYC 7742 64-Core Processors with 128 cores.
Dragon employs a number of optimizations on base multiprocessing.

The set up for the multi node run for `dragon`: For the multi-node Dragon run, the run was on 2 Apollo nodes. Each Apollo node has 1x AMD Rome CPU with 4x AMD MI100 GPUs and 128 cores.
The multi-node use case scales with the total number of CPUs reported by the allocation. As there are more nodes, workers, and CPUs available For the multi-node, Dragon extends
multiprocessing's stock capabilities.
Base multiprocessing does not support multi-node workloads.

In alphabetical order, these are the following joblib use cases and their usefulness:

.. literalinclude:: ../../examples/multiprocessing/joblib/bench_auto_batching.py

The timing for the base multiprocessing runtime is:

.. list-table:: Base Multiprocessing Timings for Bench Auto Batching
   :widths: 25 25 25 25
   :header-rows: 1

   * - n_jobs
     - Workload Name
     - Number of Tasks
     - Time in seconds
   * - 2
     - high variance, no trend
     - 5000
     - 1.648
   * - 2
     - low variance, no trend
     - 5000
     - 1.692
   * - 2
     - cyclic trends
     - 300
     - 4.165
   * - 2
     - shuffling of the previous benchmark: same mean and variance
     - 300
     - 4.150
   * - 4
     - high variance, no trend
     - 5000
     - 1.64
   * - 4
     - low variance, no trend
     - 5000
     - 1.42
   * - 4
     - cyclic trends
     - 300
     - 2.196
   * - 4
     - shuffling of the previous benchmark: same mean and variance
     - 300
     - 2.215
   * - 8
     - high variance, no trend
     - 5000
     - 0.908
   * - 8
     - low variance, no trend
     - 5000
     - 0.829
   * - 8
     - cyclic trends
     - 300
     - 1.382
   * - 8
     - shuffling of the previous benchmark: same mean and variance
     - 300
     - 1.227
   * - 16
     - high variance, no trend
     - 5000
     - 1.178
   * - 16
     - low variance, no trend
     - 5000
     - 0.906
   * - 16
     - cyclic trends
     - 300
     - 0.993
   * - 16
     - shuffling of the previous benchmark: same mean and variance
     - 300
     - 0.941
   * - 32
     - high variance, no trend
     - 5000
     - 1.124
   * - 32
     - low variance, no trend
     - 5000
     - 1.122
   * - 32
     - cyclic trends
     - 300
     - 0.907
   * - 32
     - shuffling of the previous benchmark: same mean and variance
     - 300
     - 0.904

The timing for the single-node Dragon runtime is:

.. list-table:: Dragon Timings for Bench Auto Batching
   :widths: 25 25 25 25
   :header-rows: 1

   * - n_jobs
     - Workload Name
     - Number of Tasks
     - Time in seconds
   * - 2
     - high variance, no trend
     - 5000
     - 4.445
   * - 2
     - low variance, no trend
     - 5000
     - 5.667
   * - 2
     - cyclic trends
     - 300
     - 8.669
   * - 2
     - shuffling of the previous benchmark: same mean and variance
     - 300
     - 7.27
   * - 4
     - high variance, no trend
     - 5000
     - 4.318
   * - 4
     - low variance, no trend
     - 5000
     - 3.883
   * - 4
     - cyclic trends
     - 300
     - 4.993
   * - 4
     - shuffling of the previous benchmark: same mean and variance
     - 300
     - 5.367
   * - 8
     - high variance, no trend
     - 5000
     - 4.660
   * - 8
     - low variance, no trend
     - 5000
     - 3.926
   * - 8
     - cyclic trends
     - 300
     - 4.740
   * - 8
     - shuffling of the previous benchmark: same mean and variance
     - 300
     - 4.65
   * - 16
     - high variance, no trend
     - 5000
     - 5.451
   * - 16
     - low variance, no trend
     - 5000
     - 5.358
   * - 16
     - cyclic trends
     - 300
     - 4.446
   * - 16
     - shuffling of the previous benchmark: same mean and variance
     - 300
     - 4.361
   * - 32
     - high variance, no trend
     - 5000
     - 10.295
   * - 32
     - low variance, no trend
     - 5000
     - 18.751
   * - 32
     - cyclic trends
     - 300
     - 6.577
   * - 32
     - shuffling of the previous benchmark: same mean and variance
     - 300
     - 5.998

The timing for the multi-node Dragon runtime is:

.. list-table:: Multi-node Dragon Timings for Bench Auto Batching
   :widths: 25 25 25 25
   :header-rows: 1

   * - n_jobs
     - Workload Name
     - Number of Tasks
     - Time in seconds
   * - 2
     - high variance, no trend
     - 5000
     - 6.007959
   * - 2
     - low variance, no trend
     - 5000
     - 8.862581
   * - 2
     - cyclic trends
     - 300
     - 8.567808
   * - 2
     - shuffling of the previous benchmark: same mean and variance
     - 300
     - 8.607972
   * - 4
     - high variance, no trend
     - 5000
     - 6.007959
   * - 4
     - low variance, no trend
     - 5000
     - 8.862581
   * - 4
     - cyclic trends
     - 300
     - 8.567808
   * - 4
     - shuffling of the previous benchmark: same mean and variance
     - 300
     - 8.607972
   * - 8
     - high variance, no trend
     - 5000
     - 7.252201
   * - 8
     - low variance, no trend
     - 5000
     - 6.686624
   * - 8
     - cyclic trends
     - 300
     - 6.242919
   * - 8
     - shuffling of the previous benchmark: same mean and variance
     - 300
     - 6.843477
   * - 16
     - high variance, no trend
     - 5000
     - 7.252201
   * - 16
     - low variance, no trend
     - 5000
     - 6.686624
   * - 16
     - cyclic trends
     - 300
     - 6.242919
   * - 16
     - shuffling of the previous benchmark: same mean and variance
     - 300
     - 6.843477
   * - 32
     - high variance, no trend
     - 5000
     - 7.252201
   * - 32
     - low variance, no trend
     - 5000
     - 6.686624
   * - 32
     - cyclic trends
     - 300
     - 6.242919
   * - 32
     - shuffling of the previous benchmark: same mean and variance
     - 300
     - 6.843477

.. literalinclude:: ../../examples/multiprocessing/joblib/compressor_comparison.py

The timing for the base multiprocessing runtime is:

.. list-table:: Base Multiprocessing Timings for Compressor Comparison
   :widths: 25 25
   :header-rows: 1

   * - n_jobs
     - Statistics
   * - Raw dump duration
     - 1.458s
   * - Raw dump file size
     - 167.218MB
   * - Raw load duration
     - 0.061s
   * - Zlib dump duration
     - 0.624s
   * - Zlib file size
     - 3.943MB
   * - Zlib load duration
     - 0.210s
   * - LZMA dump duration
     - 1.640s
   * - LZMA file size
     - 2.118MB
   * - LZMA load duration
     - 0.349s
   * - LZ4 file size
     - 2.118MB
   * - LZMA load duration
     - 0.331s

The timing for the single-node Dragon runtime is:

.. list-table:: Dragon Timings for Compressor Comparison
   :widths: 25 25
   :header-rows: 1

   * - n_jobs
     - Statistics
   * - Raw dump duration
     - 1.454s
   * - Raw dump file size
     - 167.218MB
   * - Raw load duration
     - 0.062s
   * - Zlib dump duration
     - 0.640s
   * - Zlib file size
     - 3.943MB
   * - Zlib load duration
     - 0.218s
   * - LZMA dump duration
     - 1.639s
   * - LZMA file size
     - 2.118MB
   * - LZMA load duration
     - 0.348s
   * - LZ4 file size
     - 2.118MB
   * - LZMA load duration
     - 0.334s

The timing for the multi-node Dragon runtime is:

.. list-table:: Multi-node Dragon Timings for Compressor Comparison
   :widths: 25 25
   :header-rows: 1

   * - n_jobs
     - Statistics
   * - Raw dump duration
     - 1.577s
   * - Raw dump file size
     - 167.218MB
   * - Raw load duration
     - 1.483s
   * - Zlib dump duration
     - 0.883s
   * - Zlib file size
     - 3.943MB
   * - Zlib load duration
     - 0.275s
   * - LZMA dump duration
     - 2.098s
   * - LZMA file size
     - 2.118MB
   * - LZMA load duration
     - 0.420s
   * - LZ4 file size
     - 2.118MB
   * - LZMA load duration
     - 0.414s

.. literalinclude:: ../../examples/multiprocessing/joblib/delayed_comparison.py

The timing for the base multiprocessing runtime is:

.. list-table:: Base Multiprocessing Timings for Delayed Comparison
   :widths: 25 25
   :header-rows: 1

   * - Type of parallel run
     - Time in seconds
   * - Without delayed
     - 10.75817883014679
   * - With delayed
     - 0.010308943688869476

The timing for the single-node Dragon runtime is:

.. list-table:: Dragon Timings for Delayed Comparison
   :widths: 25 25
   :header-rows: 1

   * - Type of parallel run
     - Time in seconds
   * - Without delayed
     - 10.73451592773199
   * - With delayed
     - 0.010201960802078247

The timing for the multi-node Dragon runtime is:

.. list-table:: Multi-node Dragon Timings for Delayed Comparison
   :widths: 25 25
   :header-rows: 1

   * - Type of parallel run
     - Time in seconds
   * - Without delayed
     - 10.547747920732945
   * - With delayed
     - 0.015844576992094517

.. literalinclude:: ../../examples/multiprocessing/joblib/memory_basic_usage.py

The timing for the base multiprocessing runtime is:

.. list-table:: Base Multiprocessing Timings for Memory Basic Usage
   :widths: 25 25
   :header-rows: 1

   * - Type of parallel run
     - Time in seconds
   * - First transformation
     - 5.01
   * - Second transformation
     - 5.08
   * - Third transformation
     - 0.01
   * - Fourth transformation
     - 5.09
   * - Fifth transformation
     - 0.01

The timing for the single-node Dragon runtime is:

.. list-table:: Single-node Dragon Runtimes for Memory Basic Usage
   :widths: 25 25
   :header-rows: 1

   * - Type of parallel run
     - Time in seconds
   * - First transformation
     - 5.01
   * - Second transformation
     - 5.06
   * - Third transformation
     - 0.01
   * - Fourth transformation
     - 5.07
   * - Fifth transformation
     - 0.01

The timing for the multi-node Dragon runtime is:

.. list-table:: Multi-node Dragon Runtimes for Memory Basic Usage
   :widths: 25 25
   :header-rows: 1

   * - Type of parallel run
     - Time in seconds
   * - First transformation
     - 5.00
   * - Second transformation
     - 5.06
   * - Third transformation
     - 0.02
   * - Fourth transformation
     - 5.12
   * - Fifth transformation
     - 0.02

.. literalinclude:: ../../examples/multiprocessing/joblib/nested_parallel_memory.py

The timing for the base multiprocessing runtime is:

.. list-table:: Base Multiprocessing Timings for Nested Parallel Memory
   :widths: 25 25
   :header-rows: 1

   * - Process step
     - Time in seconds
   * - First sequential processing
     - 8.01
   * - First round - caching the data
     - 4.09
   * - Second round - reloading the cache
     - 0.05
   * - Reusing intermediate checkpoints
     - 0.04
   * - Second sequential processing
     - 8.01
   * - First round - caching the data
     - 4.12
   * - Second round - reloading the cache
     - 0.05
   * - Reusing intermediate checkpoints
     - 0.04

The timing for the single-node Dragon runtime is:

.. list-table:: Single-node Dragon Timings for Nested Parallel Memory
   :widths: 25 25
   :header-rows: 1

   * - Process step
     - Time in seconds
   * - First sequential processing
     - 8.01
   * - First round - caching the data
     - 6.96
   * - Second round - reloading the cache
     - 3.18
   * - Reusing intermediate checkpoints
     - 3.18
   * - Second sequential processing
     - 8.01
   * - First round - caching the data
     - 7.17
   * - Second round - reloading the cache
     - 3.16
   * - Reusing intermediate checkpoints
     - 2.66

The timing for the multi-node Dragon runtime is:

.. list-table:: Multi-node Dragon Timings for Nested Parallel Memory
   :widths: 25 25
   :header-rows: 1

   * - Process step
     - Time in seconds
   * - First sequential processing
     - 8.01
   * - First round - caching the data
     - 6.96
   * - Second round - reloading the cache
     - 3.18
   * - Reusing intermediate checkpoints
     - 3.18
   * - Second sequential processing
     - 8.01
   * - First round - caching the data
     - 7.17
   * - Second round - reloading the cache
     - 3.16
   * - Reusing intermediate checkpoints
     - 2.66

.. literalinclude:: ../../examples/multiprocessing/joblib/parallel_memmap.py

The timing for the base multiprocessing runtime is:

.. list-table:: Base Multiprocessing Timings for Parallel Memory Map
   :widths: 25 25
   :header-rows: 1

   * - Process step
     - Time in seconds
   * - First elapsed time computing average of slices
     - 0.98
   * - Second elapsed time computing average of slices
     - 3.93
   * - Third elapsed time computing average of slices
     - 6.82

The timing for the single-node Dragon runtime

.. list-table:: Single-node Dragon Timings for Parallel Memory Map
   :widths: 25 25
   :header-rows: 1

   * - Process step
     - Time in seconds
   * - First elapsed time computing average of slices
     - 0.99
   * - Second elapsed time computing average of slices
     - 4.15
   * - Third elapsed time computing average of slices
     - 5.28

The timing for the multi-node Dragon runtime

.. list-table:: Multi-node Dragon Timings for Parallel Memory Map
   :widths: 25 25
   :header-rows: 1

   * - Process step
     - Time in seconds
   * - First elapsed time computing average of slices
     - 0.97
   * - Second elapsed time computing average of slices
     - 4.89
   * - Third elapsed time computing average of slices
     - 6.87


.. literalinclude:: ../../examples/multiprocessing/joblib/parallel_random_state.py

  The timing for the base multiprocessing runtime is:

.. list-table:: Base Multiprocessing Timings for Parallel Random State
   :widths: 25 25
   :header-rows: 1

   * - Process step
     - Time in seconds
   * - First iteratation (generation of stochastic vector)
     - 0.02696242928504944
   * - Second iteratation (replacement of stochastic vector)
     - 0.0243108868598938
   * - Third iteratation (replacement of second iteration stochastic vector)
     - 0.031805530190467834

The timing for the single-node Dragon runtime is:

.. list-table:: Single-Node Dragon Timings for Parallel Random State
   :widths: 25 25
   :header-rows: 1

   * - Process step
     - Time in seconds
   * - First iteratation (generation of stochastic vector)
     - 2.8984111174941063
   * - Second iteratation (replacement of stochastic vector)
     - 3.1529479399323463
   * - Third iteratation (replacement of second iteration stochastic vector)
     - 3.170066222548485

The timing for the multi-node Dragon runtime is:

.. list-table:: Multi-node Dragon Timings for Parallel Random State
   :widths: 25 25
   :header-rows: 1

   * - Process step
     - Time in seconds
   * - First iteratation (generation of stochastic vector)
     - 3.2446429850533605
   * - Second iteratation (replacement of stochastic vector)
     -  3.3172717401757836
   * - Third iteratation (replacement of second iteration stochastic vector)
     - 3.0256078988313675


.. literalinclude:: ../../examples/multiprocessing/joblib/serialization_and_wrappers.py

  The timing for the base multiprocessing runtime is:

.. list-table:: Base Multiprocessing Timings for Serialization and Wrappers
   :widths: 25 25
   :header-rows: 1

   * - Serialization Type
     - Time in seconds
   * - With loky backend and cloudpickle serialization
     - 0.085
   * - With multiprocessing backend and pickle serialization
     - 0.093
   * - With pickle serialization
     - 0.080

The timing for the single-node Dragon runtime is:

.. list-table:: Single-node Dragon Timings for Serialization and Wrappers
   :widths: 25 25
   :header-rows: 1

   * - Serialization Type
     - Time in seconds
   * - With loky backend and cloudpickle serialization
     - 3.147
   * - With multiprocessing backend and pickle serialization
     - 3.127
   * - With pickle serialization
     - 2.653

The timing for the multi-node Dragon runtime is:

.. list-table:: Multi-node Dragon Timings for Serialization and Wrappers
   :widths: 25 25
   :header-rows: 1

   * - Serialization Type
     - Time in seconds
   * - With loky backend and cloudpickle serialization
     - 3.343
   * - With multiprocessing backend and pickle serialization
     - 2.976
   * - With pickle serialization
     - 3.581