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

Parallel Memmap Example and Benchmark
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. literalinclude:: ../../examples/multiprocessing/joblib/parallel_memmap.py
.. Code for parallel memmap

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
     - 0.98
   * - Second elapsed time computing average of slices
     - 2.20
   * - Third elapsed time computing average of slices
     - 1.87

The timing for the multi-node Dragon runtime

.. list-table:: Multi-node Dragon Timings for Parallel Memory Map
   :widths: 25 25
   :header-rows: 1

   * - Process step
     - Time in seconds
   * - First elapsed time computing average of slices
     - 0.98
   * - Second elapsed time computing average of slices
     - 2.20
   * - Third elapsed time computing average of slices
     - 2.68


Delayed Comparison Example and Benchmark
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. literalinclude:: ../../examples/multiprocessing/joblib/delayed_comparison.py
.. Code for delayed comparison

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
     - 10.675060355992173
   * - With delayed
     - 0.0031840159936109558

The timing for the multi-node Dragon runtime is:

.. list-table:: Multi-node Dragon Timings for Delayed Comparison
   :widths: 25 25
   :header-rows: 1

   * - Type of parallel run
     - Time in seconds
   * - Without delayed
     - 10.547747920732945
   * - With delayed
     - 0.0032101319957291707

Compressor Comparison Example and Benchmark
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. literalinclude:: ../../examples/multiprocessing/joblib/compressor_comparison.py
.. Code for compressor comparision

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
     - 0.194s
   * - Raw dump file size
     - 167.218MB
   * - Raw load duration
     - 0.046s
   * - LZMA dump duration
     - 1.649s
   * - LZMA file size
     - 2.118MB
   * - LZMA load duration
     - 0.259s
   * - LZ4 file size
     - 2.118MB
   * - LZMA load duration
     - 0.257s

The timing for the multi-node Dragon runtime is:

.. list-table:: Multi-node Dragon Timings for Compressor Comparison
   :widths: 25 25
   :header-rows: 1

   * - n_jobs
     - Statistics
   * - Raw dump duration
     - 0.191s
   * - Raw dump file size
     - 167.218MB
   * - Raw load duration
     - 0.046s
   * - LZMA dump duration
     - 1.682s
   * - LZMA file size
     - 2.118MB
   * - LZMA load duration
     - 0.254s
   * - LZ4 file size
     - 2.118MB
   * - LZMA load duration
     - 0.254s

Memory Basic Usage Example and Benchmark
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. literalinclude:: ../../examples/multiprocessing/joblib/memory_basic_usage.py
.. Code for memory basic usage

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
     - 5.00
   * - Second transformation
     - 5.02
   * - Third transformation
     - 0.01
   * - Fourth transformation
     - 5.02
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
     - 5.02
   * - Third transformation
     - 0.01
   * - Fourth transformation
     - 5.02
   * - Fifth transformation
     - 0.01

Bench Auto Batching Example and Benchmark 
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. literalinclude:: ../../examples/multiprocessing/joblib/bench_auto_batching.py
.. Code for autobatching

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
     - 2.1
   * - 2
     - low variance, no trend
     - 5000
     - 2.0
   * - 2
     - cyclic trends
     - 300
     - 4.7
   * - 2
     - shuffling of the previous benchmark: same mean and variance
     - 300
     - 4.3
   * - 4
     - high variance, no trend
     - 5000
     - 2.1
   * - 4
     - low variance, no trend
     - 5000
     - 2.0
   * - 4
     - cyclic trends
     - 300
     - 4.7
   * - 4
     - shuffling of the previous benchmark: same mean and variance
     - 300
     - 4.3
   * - 8
     - high variance, no trend
     - 5000
     - 0.9
   * - 8
     - low variance, no trend
     - 5000
     - 0.9
   * - 8
     - cyclic trends
     - 300
     - 1.4
   * - 8
     - shuffling of the previous benchmark: same mean and variance
     - 300
     - 1.2
   * - 16
     - high variance, no trend
     - 5000
     - 1.2
   * - 16
     - low variance, no trend
     - 5000
     - 0.9
   * - 16
     - cyclic trends
     - 300
     - 1.0
   * - 16
     - shuffling of the previous benchmark: same mean and variance
     - 300
     - 0.9
   * - 32
     - high variance, no trend
     - 5000
     - 1.1
   * - 32
     - low variance, no trend
     - 5000
     - 1.1
   * - 32
     - cyclic trends
     - 300
     - 0.9
   * - 32
     - shuffling of the previous benchmark: same mean and variance
     - 300
     - 0.9

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
     - 2.6
   * - 2
     - low variance, no trend
     - 5000
     - 2.6
   * - 2
     - cyclic trends
     - 300
     - 2.6
   * - 2
     - shuffling of the previous benchmark: same mean and variance
     - 300
     - 2.6
   * - 4
     - high variance, no trend
     - 5000
     - 2.6
   * - 4
     - low variance, no trend
     - 5000
     - 2.6
   * - 4
     - cyclic trends
     - 300
     - 2.6
   * - 4
     - shuffling of the previous benchmark: same mean and variance
     - 300
     - 2.6
   * - 8
     - high variance, no trend
     - 5000
     - 2.5
   * - 8
     - low variance, no trend
     - 5000
     - 2.6
   * - 8
     - cyclic trends
     - 300
     - 2.6
   * - 8
     - shuffling of the previous benchmark: same mean and variance
     - 300
     - 2.6
   * - 16
     - high variance, no trend
     - 5000
     - 2.5
   * - 16
     - low variance, no trend
     - 5000
     - 2.5
   * - 16
     - cyclic trends
     - 300
     - 2.5
   * - 16
     - shuffling of the previous benchmark: same mean and variance
     - 300
     - 2.5
   * - 32
     - high variance, no trend
     - 5000
     - 2.5
   * - 32
     - low variance, no trend
     - 5000
     - 2.5
   * - 32
     - cyclic trends
     - 300
     - 2.5
   * - 32
     - shuffling of the previous benchmark: same mean and variance
     - 300
     - 2.6