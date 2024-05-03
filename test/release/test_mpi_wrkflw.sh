#!/bin/bash

# To change the version of Python, please use the following commands:
# To find the version of Python on your system: ls -ls /usr/bin/python*
# To load the version of Python you want: `alias python=python<version>` or `alias python='\usr\bin\python<version>'`

# Python 3.11

# NUM_WORKERS      | NUM_IMAGES     | NUM_BURNS     | NUM_ITER | SIZE_IMG  | MEM_SIZE  | WORK TIME | 0TH_ITER(s)       | 1ST_ITER(s)       | AVG_TIME (S) |  STD_DEV (s)    
# 51200            | 51200          | 0             | 2        | 256       | 33554432  | 4.00      | 260.7048197858967 | 163.23564628418535|211.970233035 | 68.9211135397082              

dragon ../../examples/multiprocessing/numpy-mpi4py-examples/scipy_scale_work.py --dragon --num_workers 51200 --mem 3355443200 --size 256 --iterations 2 --burns 0 --work_time 4

# To set up a multi-node environment for deployment of the script, it is recommended to pass the command `salloc --nodes=8 <--exclusive if needed> <-t hh:mm:ss>`. 
# The arguments encapuslated by <> are recommended but not needed.

# The following line of code can be run to allocate the number of nodes needed:
# salloc --nodes=400 --exclusive -t 04:00:00

# The following lines of code ensure swift clean up.
# dragon-cleanup
# scancel -u $USER
