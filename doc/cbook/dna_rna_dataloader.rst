A Custom Dataloader Benchmark for Biological Sequences Using DragonDataset and Dragon DDict
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

This benchmark was altered from a Medium article over a DataLoader benchmark. The benchmark has been altered to utilize Dragon objects. The SeqDataset class was modified to use DragonDataset. DragonDataset is a PyTorch dataset that uses Dragon distributed dictionary to store the training data and labels. 
Dragon Dataset can handle an iterable for the data or an existing dragon distributed dictionary with a list of its keys. 
The dataloader used is from PyTorch for this example and benchmark and was coded to be compatible with Dragon distributed dictionary (DDict), DragonDataset, and the Dragon runtime.

This article (https://medium.com/swlh/pytorch-dataset-dataloader-b50193dc9855) outlines a developer's journey in creating a custom data-loading framework for biological datasets, emphasizing flexibility, efficiency, and computational resource management. 
The author highlights how common NLP frameworks like AllenNLP and Fast.ai simplify handling text-based datasets, allowing for easy adaptation of pre-trained models for downstream tasks. 
However, working with biological sequences, which often lack the structure of natural language, requires building custom tokenizers and data loaders. 
This need for customization drives the developer to delve deeper into PyTorch's native Dataset and DataLoader classes, which provide the flexibility and control needed for complex data structures.

The Dataset class in PyTorch is essential for data handling and requires implementing two methods, `__len__`` and `__getitem__`, which define the dataset's length and retrieve individual samples, respectively. 
For biological data, this often means incorporating custom tokenization directly into `__getitem__`, allowing parallelized data processing when used with the DataLoader. 
PyTorch's DataLoader enables efficient data loading with features like batching, collating, memory pinning, and multiprocessing, which become crucial for managing large biological datasets. 
The article contrasts single-processing and multi-processing data loading, detailing how each process affects performance and memory usage.

To test the data loader's efficiency, the author benchmarks a Doc2Vec model on synthetic DNA sequences, evaluating parameters such as GPU usage, pin_memory, batch size, and num_workers. 
The results reveal that increasing workers and batch size generally improves data loading efficiency, especially on GPUs. 
The article concludes that while PyTorch's framework offers powerful tools for custom data loading, tuning these parameters for specific hardware setups is essential for achieving optimal performance. 
The author's repository provides code examples and further resources to explore efficient data handling in PyTorch for large, complex datasets.

In the context of using PyTorch's DataLoader for biological data, DNA and RNA sequences present unique challenges, especially when scaling data loading processes with multiple workers 
and varying data file sizes. DNA sequences, typically larger and more stable due to their double-stranded nature, often require substantial storage and processing capacity. 
RNA sequences, being shorter and single-stranded, are generally more transient but can appear in vast quantities within datasets due to cellular transcription processes. 
In a multi-worker setup, DNA sequences may benefit from larger batch sizes and higher worker counts to offset their size and reduce I/O latency, ensuring efficient data streaming for model 
training. Conversely, RNA datasets, while often smaller per sequence, can have higher file counts, benefiting from finer-grained parallelization (i.e., more workers with smaller batch sizes) 
to balance between loading speed and memory efficiency. This tailored approach for DNA versus RNA data maximizes DataLoader efficiency, enabling effective processing of diverse biological datasets across hardware configurations.

The synthetic data for this use case is generated using `generate_synthetic_data.py`. The time to generate the syntehtic data is not factored into the overall runtime for the benchmark.

The following code demonstrates the utilization of DragonDataset, DragonDict, and PyTorch Dataloader for the creation of a customized tokenizer:
.. literalinclude:: ../../examples/dragon_ai/dna_rna_dataloader/dataset.py

The following code demonstrates creating a Doc2Vec model utilizing the Dragon runtime and DragonDataset:
.. literalinclude:: ../../examples/dragon_ai/dna_rna_dataloader/test_doc2vec_DM.py


Installation
============

After installing dragon, the only other dependency is on PyTorch. The PyTorch version and corresponding pip command can be found here (https://pytorch.org/get-started/locally/).

.. code-block:: console

    > pip3 install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu118

Description of the system used
==============================

For this example, an HPE Cray EX was used. Each node has AMD EPYC 7763 64-core CPUs and 4x Nvidia A100 GPUs.

How to run
==========

Example of how to generate 0.125 GB of data
--------------------------------------------
.. code-block:: console
    :linenos:

    > salloc --nodes=1 --exclusive -t 1:00:00
    > python3 generate_synthetic_data.py --size 0.125



Example output when run on 2 nodes with 0.125 GB of synthetic data and dragon dict memory.
------------------------------------------------------------------------------------------
.. code-block:: console
    :linenos:

    > salloc --nodes=2 -p allgriz --exclusive -t 1:00:00
    > dragon test_doc2vec_DM.py --file-name seq0.125gb.txt
    Started
    Namespace(file_name='seq0.125gb.txt', num_epochs=1, batch_size=4, shuffle=False, pin_memory=False, num_workers=2, use_gpu=False, seq_len=40, input_size=1000, dragon_dict_managers=2, dragon_dict_mem=17179869184)
    DDict_Timer 133.90535412499594
    [DDictManagerStats(manager_id=1, hostname='pinoak0035', total_bytes=4294967296, total_used_bytes=268173312, num_keys=32736, free_blocks={4096: 0, 8192: 0, 16384: 0, 32768: 0, 65536: 0, 131072: 0, 262144: 1, 524288: 0, 1048576: 0, 2097152: 0, 4194304: 0, 8388608: 0, 16777216: 0, 33554432: 0, 67108864: 0, 134217728: 0, 268435456: 1, 536870912: 1, 1073741824: 1, 2147483648: 1, 4294967296: 0}), DDictManagerStats(manager_id=3, hostname='pinoak0035', total_bytes=4294967296, total_used_bytes=268181504, num_keys=32737, free_blocks={4096: 0, 8192: 1, 16384: 1, 32768: 1, 65536: 1, 131072: 1, 262144: 0, 524288: 0, 1048576: 0, 2097152: 0, 4194304: 0, 8388608: 0, 16777216: 0, 33554432: 0, 67108864: 0, 134217728: 0, 268435456: 1, 536870912: 1, 1073741824: 1, 2147483648: 1, 4294967296: 0}), DDictManagerStats(manager_id=0, hostname='pinoak0034', total_bytes=4294967296, total_used_bytes=268181504, num_keys=32737, free_blocks={4096: 0, 8192: 1, 16384: 1, 32768: 1, 65536: 1, 131072: 1, 262144: 0, 524288: 0, 1048576: 0, 2097152: 0, 4194304: 0, 8388608: 0, 16777216: 0, 33554432: 0, 67108864: 0, 134217728: 0, 268435456: 1, 536870912: 1, 1073741824: 1, 2147483648: 1, 4294967296: 0}), DDictManagerStats(manager_id=2, hostname='pinoak0034', total_bytes=4294967296, total_used_bytes=268173312, num_keys=32736, free_blocks={4096: 0, 8192: 0, 16384: 0, 32768: 0, 65536: 0, 131072: 0, 262144: 1, 524288: 0, 1048576: 0, 2097152: 0, 4194304: 0, 8388608: 0, 16777216: 0, 33554432: 0, 67108864: 0, 134217728: 0, 268435456: 1, 536870912: 1, 1073741824: 1, 2147483648: 1, 4294967296: 0})]
    Building Vocabulary
    GACTU
    2
    True
    0
    130944
    0
    Time taken for epoch:
    0.0379030704498291
    Real time: 2.862032890319824
    User time: 0.048634999999990214
    System time: 2.565563



Dragon Timings for Use Case
============================

.. list-table:: Dragon Timings for Dataloader Use Case Using 2 Workers and 2 Nodes
   :widths: 25 25 50
   :header-rows: 1

   * - Size of file
     - 1/8 GB
     - 1/4 GB
   * - DDict Size
     - 17179869184
     - 17179869184
   * - DDict Timer (s)
     - 138.33297302300343
     - 263.93296506398474
   * - Time per Epoch (1 Epoch)
     - 0.03711223602294922
     - 0.06808733940124512
   * - Test DM Model Execution Time (Real)
     - 2.82588529586792
     - 2.8808722496032715
   * - Test DM Model Execution Time (User)
     - 0.08575100000001612
     - 0.06457100000000082
   * - Test DM Model Execution Time (Sys)
     - 2.505724
     - 2.565531
   * - Overall Execution Time (Real)
     - 5m55.754s
     - 10m34.443s
   * - Overall Execution Time (User)
     - 0m5.147s
     - 0m5.598s
   * - Overall Execution Time (Sys)
     - 0m1.324s
     - 0m1.408s