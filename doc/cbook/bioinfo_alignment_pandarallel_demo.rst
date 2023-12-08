Bioinformatics Alignment Pandarallel Nucleotides and Amino Acids Benchmark in Single and Multi-node Environments
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

This Jupyter benchmark performs both DNA and protein alignments in parallel using the
pandarallel `parallel_apply` call. It can be run with `dragon` to
compare performance on your machine. The DNA/nucleotide workload is run in a single-node environment.
The protein/amino acid workload is run in a multi-node environment.

The use case utilizes pairwise alignments from pyalign, a jaccard distance calculation for the E value, and a hamming distance calculation for the coverage percentage.
The timings are provided with Dragon and base multiprocessing for `parallel_apply`, the multiprocessing verison of pandas `apply`.
The application utilizes nucleotide and amino acid workloads for feature selection.
The time to run the workloads is calculated and displayed in the pandas. K-means clustering is used to group sequences by alignment and percentage coverage of alignments.

The code demonstrates the following key concepts working with Dragon:

* How to write programs that can run with Dragon and base multiprocessing
* How to use pandarallel and pandas with Dragon for feature selection
* How pandarallel handles different dtypes
* How to utilize pandarallel in a multi-node environment
* How to utilize k-means clustering on features such as alignment, E value, and percentage coverage

The following notebook was used for the single-node comparison:

.. literalinclude:: ../../examples/jupyter/doc_ref/bioinformatics_alignment_pandarallel_demo.py

For the single-node run, both base multiprocessing and Dragon are compared. The runs utilized a single node with 2 AMD EPYC 7742 64-Core Processors with 128 cores.
Dragon employs a number of optimizations on base multiprocessing; the Dragon start method outperforms the use of the base multiprocessing spawn start method on the same hardware.

The timing for the base multiprocessing runtime is:

.. list-table:: Base Multiprocessing Timings for Nucleotide Alignments with Different Number of Bars
   :widths: 25 25 50
   :header-rows: 1

   * - Pandarallel Function
     - Number of Bars
     - Time
   * - PyAlign Alignment Score
     - 128
     - 61.000052
   * - E Score
     - 10
     - 22.919291
   * - Percentage Coverage
     - 1
     - 18.000021
   * - Total Time
     -
     - 101.919364


The timing for the single-node Dragon runtime is:

.. list-table:: Dragon Timings for Nucleotide Alignments with Different Number of Bars
   :widths: 25 25 50
   :header-rows: 1

   * - Pandarallel Function
     - Number of Bars
     - Time
   * - PyAlign Alignment Score
     - 128
     - 11.601343
   * - E Score
     - 10
     - 7.882140
   * - Percentage Coverage
     - 1
     - 7.930996
   * - Total Time
     -
     - 27.174203

For multi-node Dragon run, the run was on 2 Apollo nodes. Each Apollo node has 1x AMD Rome CPU with 4x AMD MI100 GPUs and 128 cores.
The multi-node use case scales with the total number of CPUs reported by the allocation. As there are more nodes, workers, and CPUs available for multi-node, Dragon extends
multiprocessing's stock capabilities and demonstrates additional improvement to measured execution time.
Base multiprocessing does not support multi-node workloads.

The following notebook was used for the multi-node comparison:

.. literalinclude:: ../../examples/jupyter/doc_ref/bioinformatics_alignment_pandarallel_multinode_demo.py

The timing for the multi-node Dragon runtime is:

.. list-table:: Multi-node Timings for Amino Acids Alignments with Same Number of Bars
   :widths: 25 25 50
   :header-rows: 1

   * - Pandarallel Function
     - Number of Bars
     - Time
   * - PyAlign Alignment Score
     - 10
     - 7.031509
   * - E Score
     - 10
     - 6.835784
   * - Percentage Coverage
     - 10
     - 7.396781
   * - Total Time
     -
     - 21.264074
