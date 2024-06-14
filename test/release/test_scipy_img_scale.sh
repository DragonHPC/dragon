#!/bin/bash

#To change the version of Python, please use the following commands:
# To find the version of Python on your system: ls -ls /usr/bin/python*
# To load the version of Python you want: `alias python=python<version>` or `alias python='\usr\bin\python<version>'`
#The work time or compute time is set to 1 second. This determines the execution time of the program.

# The following timings for the scipy_scale_work.py file are completed using Python 3.11.5 with 8 nodes. The program inputs, the timings for the iterations, the average time and the standard of deviations for the iterations are recorded in the tables below. The tables are grouped by number of iterations. 

# 4 iteratons

# NUM_WORKERS      | NUM_IMAGES     | NUM_BURNS     | NUM_ITER | SIZE_IMG        | MEM_SIZE  | WORK TIME    | 0TH_ITER(s)  | 1ST_ITER(s)  | 2ND_ITER(s)  | 3RD_ITER(s)  | 4TH_ITER(s) |  AVG_TIME (S)  | STD_DEV (s)
# 1024             | 32768          | 1             | 4        | 32              | 33554432  | 1.00         | 36.53        | 33.71        | 33.75        | 34.13        | 33.77       |  33.84         | 00.17

# The following timings for the scipy_scale_work.py file are completed using Python 3.10.12 with 8 nodes. The program inputs, the timings for the iterations, the average time and the standard of deviations for the iterations are recorded in the tables below. The tables are grouped by number of iterations.

# 4 iteratons

# NUM_WORKERS      | NUM_IMAGES     | NUM_BURNS     | NUM_ITER | SIZE_IMG        | MEM_SIZE  | WORK TIME    | 0TH_ITER(s)  | 1ST_ITER(s)  | 2ND_ITER(s)  | 3RD_ITER(s)  | 4TH_ITER(s) | AVG_TIME (S)  | STD_DEV (s)
# 1024             | 32768          | 1             | 4        | 32              | 33554432  | 1.00         | 36.58        | 33.65        | 33.92        | 33.99        | 33.80       | 33.84         | 00.13


# To set up a multi-node environment for deployment of the script, it is recommended to pass the command `salloc --nodes=8 <--exclusive if needed> <-t hh:mm:ss>`. 
# The arguments encapuslated by <> are recommended but not needed.

#The following line of code can be run to allocate the number of nodes needed:
# salloc --nodes=8 --exclusive -t 04:00:00

#This runs the script with the parameters of 1024 workers, 32768 images, 1 burn cycle, 4 iterations, images of size 32, memory size of 33554432, and a work time for each image of 1 second.
dragon ../../examples/multiprocessing/numpy-mpi4py-examples/scipy_scale_work.py --dragon --num_workers 1024 --iterations 4 --burns 1 --size 32 --mem 33554432 --work_time 1

# #The following lines of code ensure swift clean up.
# #dragon-cleanup
# #scancel -u $USER