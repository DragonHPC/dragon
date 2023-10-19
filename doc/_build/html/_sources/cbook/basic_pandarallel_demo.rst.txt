Basic Pandarallel Demonstration for Single Node Environment
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

This Jupyter benchmark is a simple use case for the pandarallel `parallel_apply` call. 
It can be run with `dragon` and base multiprocessing to compare performance on your machine. 

The program demonstrates how to use `parallel_apply`, the multiprocessing verison of pandas `apply`, on a pandas dataframe with random input.

The code demonstrates the following key concepts working with Dragon:

* How to write basic programs that can run with Dragon and base multiprocessing
* How to use pandarallel and pandas with Dragon and base multiprocessing
* How pandarallel handles various dtypes

.. code-block:: python
    :linenos:
    :caption: **basic_pandarallel_demo.ipynb: A bioinformatics benchmark for aligning nucleotide sequences and amino acid sequences**

    import dragon
    import multiprocessing

    import cloudpickle

    import numpy as np
    import pandas as pd

    import pandarallel; pandarallel.__version__

    multiprocessing.set_start_method("dragon")
    pandarallel.core.dill = cloudpickle
    pandarallel.core.CONTEXT = multiprocessing.get_context("dragon")
    pandarallel.pandarallel.initialize(progress_bar=True)

    num_rows = 10

    df = pd.DataFrame(
        {
            "seqnum": np.arange(42, (42 + num_rows), dtype=int),
            #"metric_A": np.random.rand(num_rows),
            #"metric_B": np.random.rand(num_rows),
            "metric_C": np.random.rand(num_rows),
            "alt_seq": np.random.randint(low=42, high=(42 + num_rows), size=(num_rows,)),
            "label": np.array(list("ATCG"))[np.random.randint(0, 4, num_rows)],
        },
    )

    df['highlow_C'] = df['metric_C'].parallel_apply(lambda x: x < cutoff)
