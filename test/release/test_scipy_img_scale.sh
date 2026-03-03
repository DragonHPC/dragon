#!/bin/bash

#To change the version of Python, please use the following commands:
# To find the version of Python on your system: ls -ls /usr/bin/python*  or `module spider cray-python/`
# To load the version of Python you want: `alias python=python<version>` or `alias python='\usr\bin\python<version>'` or `module load cray-python/<version>`
#The work time or compute time is set to 1 second. This determines the execution time of the program.

# The following timings for the scipy_scale_work.py file are completed using Python 3.11.5 with 8 nodes. The program inputs, the timings for the iterations, the average time and the standard of deviations for the iterations are recorded in the tables below. The tables are grouped by number of iterations. 

# 4 iteratons

# NUM_WORKERS      | NUM_IMAGES     | NUM_BURNS     | NUM_ITER | SIZE_IMG        | MEM_SIZE  | WORK TIME    | 0TH_ITER(s)             | 1ST_ITER(s)              | 2ND_ITER(s)              | 3RD_ITER(s)               | 4TH_ITER(s)             |  AVG_TIME (S)  | STD_DEV (s)
# 1024             | 32768          | 1             | 4        | 32              | 33554432  | 1.00         | 42.1163173239911        | 33.04787137196399        | 33.13321163598448        | 32.956871169037186        | 33.30180826189462       |  33.11         | 0.13

# real    3m14.953s
# user    0m2.252s
# sys     0m0.760s

# The timings are the same for the other versions of Python: 3.11.5, 3.10.10, and 3.9.13.1. 

# To set up a multi-node environment for deployment of the script, it is recommended to pass the command `salloc --nodes=8 <--exclusive if needed> <-t hh:mm:ss>`. 
# The arguments encapuslated by <> are recommended but not needed.

#The following line of code can be run to allocate the number of nodes needed:
# salloc --nodes=8 --exclusive -t 04:00:00

#This runs the script with the parameters of 1024 workers, 32768 images, 1 burn cycle, 4 iterations, images of size 32, memory size of 33554432, and a work time for each image of 1 second.
dragon ../../examples/multiprocessing/numpy-mpi4py-examples/scipy_scale_work.py --dragon --num_workers 1024 --iterations 4 --burns 1 --size 32 --mem 33554432 --work_time 1

# #The following lines of code ensure swift clean up.
# #dragon-cleanup
# #scancel -u $USER