***********************
Chain Token Test
***********************

Introduction
============

This test measures how fast multiple workers connected in a ring each with a single
input and output ``multiprocessing.Queue`` can pass a single data item around the ring.

Running
=======

::

    usage: chain_token_test.py [-h] [--spins SPINS] [--num_workers NUM_WORKERS]
                               [--num_iterations NUM_ITERATIONS]
                               [--burn_iterations BURN_ITERATIONS]

    Basic token passing test, passing around a chain

    optional arguments:
      -h, --help            show this help message and exit
      --spins SPINS         number of times to send the token around a chain
      --num_workers NUM_WORKERS
                            number of workers to spawn at a time
      --num_iterations NUM_ITERATIONS
                            number of iterations to do
      --burn_iterations BURN_ITERATIONS
                            number of iterations to burn first time


Results
=======

On the rome box it looks like tokens can get passed at about 7-8kHz around the queue.  It is slower when
more workers are involved.