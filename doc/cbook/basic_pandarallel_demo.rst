Basic Pandarallel Demonstration for Single Node Environment
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

This Jupyter benchmark is a simple use case for the pandarallel `parallel_apply` call.
It can be run with `dragon` and base multiprocessing to compare performance on your machine.

The program demonstrates how to use `parallel_apply`, the multiprocessing verison of pandas `apply`, on a pandas dataframe with random input.

The code demonstrates the following key concepts working with Dragon:

* How to write basic programs that can run with Dragon and base multiprocessing
* How to use pandarallel and pandas with Dragon and base multiprocessing
* How pandarallel handles various dtypes

.. literalinclude:: ../../examples/jupyter/doc_ref/basic_pandarallel_demo.py
