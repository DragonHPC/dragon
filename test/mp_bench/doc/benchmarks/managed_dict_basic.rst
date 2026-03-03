***********************
Basic Managed Dict Test
***********************

Introduction
============

This test measures how fast multiple workers can write information trivially to
a single managed dictionary.

Running
=======

::

    usage: basic_managed_dict_test.py [-h] [--spins SPINS]
                                      [--num_workers NUM_WORKERS]
                                      [--num_iterations NUM_ITERATIONS]
                                      [--burn_iterations BURN_ITERATIONS]

    Does a basic speed test of managed dicts

    optional arguments:
      -h, --help            show this help message and exit
      --spins SPINS         number of times for each worker to touch the
                            dictionary
      --num_workers NUM_WORKERS
                            number of workers to spawn at a time
      --num_iterations NUM_ITERATIONS
                            number of spawn/gather iterations to do
      --burn_iterations BURN_ITERATIONS
                            number of iterations to burn first time

Results
=======

On the Rome box, writing integers to integer keys:

* 1 worker can post at about 35kHz on the Rome box
* 4 get about 30kHz collectively
* 100 get about 12kHz collectively.

This means that for basic functionality the interaction rate is definitely adequate to support
uses for infrastructure.