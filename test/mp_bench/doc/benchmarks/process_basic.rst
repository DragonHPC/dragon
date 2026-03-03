******************
Basic Process Test
******************


Introduction
============

This test measures how long it takes a number of worker python processes
to be created directly using ``multiprocessing.Process``

The processes do no work other than returning the single argument
they were called with.

A batch of processes is created then joined in the order they were created
and this test is repeated multiple times.

Running
=======

::

    usage: basic_process_test.py [-h] [--num_workers NUM_WORKERS]
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

Pretty big difference - very slow on WSL, maybe for file reasons.  Less of a difference
on real unix.