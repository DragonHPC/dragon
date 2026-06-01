***************
Basic Pool Test
***************


Introduction
============

This test measures how long it takes a number of worker python processes
to be created in a Pool.

The processes do no work - the pool with this many processes is created
then destroyed multiple times.


Running
=======

::

    usage: basic_pool_test.py [-h] [--num_workers NUM_WORKERS]
                              [--num_iterations NUM_ITERATIONS]
                              [--burn_iterations BURN_ITERATIONS]

    Does a basic speed test of spawning processes in a Pool

    optional arguments:
      -h, --help            show this help message and exit
      --num_workers NUM_WORKERS
                            number of workers to spawn at a time
      --num_iterations NUM_ITERATIONS
                            number of spawn/gather iterations to do
      --burn_iterations BURN_ITERATIONS
                            number of iterations to burn first time

Results
=======

Seemingly about 100ms minimum to get a Pool at all.
