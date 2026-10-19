#!/bin/bash

# To change the version of Python, please use the following commands:
# To find the version of Python on your system: ls -ls /usr/bin/python* or `module spider cray-python/`
# To load the version of Python you want: `alias python=python<version>` or `alias python='\usr\bin\python<version>'` or `module load cray-python/<version>`

# Python 3.11.7 for 8 nodes
# NUM_WORKERS      | NUM_IMAGES     | NUM_BURNS     | NUM_ITER | SIZE_IMG  | MEM_SIZE  | WORK TIME | 0TH_ITER(s)        | 1ST_ITER(s)        | AVG_TIME (S) |  STD_DEV (s)    
# 1024             | 51200          | 0             | 2        | 256       | 33554432  | 4.00      | 219.87429636899924 | 210.00686607199987 | 214.94       | 4.93  

# real    7m28.292s
# user    0m2.729s
# sys     0m0.949s   

# The timings are the same for the iterations for the other versions of Python: 3.11.5, 3.10.10, and 3.9.13.1. 

#8 nodes
dragon ../../examples/multiprocessing/numpy-mpi4py-examples/scipy_scale_work.py --dragon --num_workers 1024 --mem 3355443200 --size 256 --iterations 2 --burns 0 --work_time 4

# Python 3.11.7 for 400 nodes

# NUM_WORKERS      | NUM_IMAGES     | NUM_BURNS     | NUM_ITER | SIZE_IMG  | MEM_SIZE  | WORK TIME | 0TH_ITER(s)        | 1ST_ITER(s)       | AVG_TIME (S) |  STD_DEV (s)    
# 51200            | 51200          | 0             | 2        | 256       | 33554432  | 4.00      | 2124.9003370779974 | 1198.239171817986 | 1661.57      |  463.33  

# Overall Execution Time

# real    59m8.481s
# user    0m17.880s
# sys     0m10.621s

#400 nodes
#dragon ../../examples/multiprocessing/numpy-mpi4py-examples/scipy_scale_work.py --dragon --num_workers 51200 --mem 3355443200 --size 256 --iterations 2 --burns 0 --work_time 4

# To set up a multi-node environment for deployment of the script, it is recommended to pass the command `salloc --nodes=8 <--exclusive if needed> <-t hh:mm:ss>`. 
# The arguments encapuslated by <> are recommended but not needed.

# The following line of code can be run to allocate the number of nodes (400) needed:
# salloc --nodes=400 --exclusive -t 04:00:00

# The following lines of code ensure swift clean up.
# dragon-cleanup
# scancel -u $USER
