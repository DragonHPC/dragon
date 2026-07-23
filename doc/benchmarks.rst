.. _benchmarks:

Benchmarks
++++++++++

Below are benchmarks we use to validate the performance of Dragon. They are typically adapted from benchmarks commonly
used in high performance computing or data processing.

These pages are best read as comparative workload studies rather than as fixed
performance guarantees. The exact numbers depend on hardware, node count,
transport configuration, and the problem size used for each run.

Use this section to answer three questions:

* what Dragon subsystem a benchmark is exercising,
* how Dragon compares to another baseline for that workload, and
* which configuration details matter when interpreting the results.

If you are evaluating Dragon for your own environment, focus on the benchmark
shape and methodology first, then compare those conditions to your target
hardware and runtime settings.

What To Read Next
=================

* :ref:`installation-guide` - confirm your environment and runtime configuration before comparing numbers.
* :ref:`getting-started` - understand the baseline Dragon execution model behind the benchmarked features.
* :ref:`uses` - find task-oriented examples that match the subsystem a benchmark exercises.

.. toctree::
   :maxdepth: 1

   benchmarks/ddict.rst
   benchmarks/pool_v_mpi.rst