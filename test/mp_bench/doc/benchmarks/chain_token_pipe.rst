***********************
Chain Token Pipe Test
***********************

Introduction
============

This test measures how fast multiple workers connected in a ring each with a single
input and output ``multiprocessing.Connection`` created from a
``multiprocessing.Pipe`` call can pass a single data item around the ring

Running
=======




::

    usage: chain_token_pipe_test.py [-h] [--spins SPINS] [--num_workers NUM_WORKERS] [--num_iterations NUM_ITERATIONS]
                                    [--burn_iterations BURN_ITERATIONS]

    Basic token passing test, passing around a chain using Pipe

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

Seems to be, on the rome box, about 10kHz for 250 workers and 45kHz for 10 workers.
This is about 4x faster than what one gets with Queues, but the Queues are protected.