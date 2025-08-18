#!/bin/bash
#SBATCH --nodes=512
#SBATCH --exclusive
#SBATCH --time=2:00:00


# The following timings for the scipy_scale_work.py file are completed using Python 3.11.5 with 8 nodes. The program inputs, the timings for the iterations, the average time and the standard of deviations for the iterations are recorded in the tables below. The tables are grouped by number of iterations. 
# for 0.10 release
# 4 iteratons

# NUM_WORKERS      | NUM_IMAGES     | NUM_BURNS     | NUM_ITER | SIZE_IMG        | MEM_SIZE  | WORK TIME    | 0TH_ITER(s)             | 1ST_ITER(s)              | 2ND_ITER(s)              | 3RD_ITER(s)               | 4TH_ITER(s)             |  AVG_TIME (S)  | STD_DEV (s)
# 1024             | 32768          | 1             | 4        | 32              | 33554432  | 1.00         | 42.1163173239911        | 33.04787137196399        | 33.13321163598448        | 32.956871169037186        | 33.30180826189462       |  33.11         | 0.13

# real    3m14.953s
# user    0m2.252s
# sys     0m0.760s

source ~/.bashrc
conda activate _env312
srun --nodes=512 hostname

##This runs the script with the parameters of 1024 workers, 32768 images, 1 burn cycle, 4 iterations, images of size 32, memory size of 33554432, and a work time for each image of 1 second.
#dragon --nodes=8 ../../examples/multiprocessing/numpy-mpi4py-examples/scipy_scale_work.py --dragon \
#    --num_workers 1024 \
#    --iterations 2 \
#    --burns 0 \
#    --size 32 \
#    --mem 33554432 \
#    --work_time 1
#
### 0.11 release:
## Number of images: 32768
## Number of workers: 1024
## Time for iteration 0 is 36.690396110061556 seconds
## Time for iteration 1 is 32.568573649041355 seconds
#
#
## 32 nodes
##dragon --nodes=32 ../../examples/multiprocessing/numpy-mpi4py-examples/scipy_scale_work.py --dragon \
##    --num_workers 4096 \
##    --iterations 2 \
##    --burns 0 \
##    --size 32 \
##    --mem  134217728 \
##    --work_time 1
#
### 0.11 release:
## Number of images: 131072
## Number of workers: 4096
## Time for iteration 0 is 41.64335555501748 seconds
## Time for iteration 1 is 35.767127881990746 seconds
#

## 512 nodes, 2 img/worker
dragon ../../examples/multiprocessing/numpy-mpi4py-examples/scipy_scale_work.py --dragon \
                                                                                --num_workers 65536  \
                                                                                --iterations 2 \
                                                                                --burns 0 \
                                                                                --size 32 \
                                                                                --mem 1073741824 \
                                                                                --work_time 1
## dragon 0.11 release from 512 nodes, 2 imgs/worker
## Number of images: 1048576
## Number of workers: 65536
## Time for iteration 0 is 228.91780479403678 seconds
## Time for iteration 1 is 232.08353578299284 seconds
## Average time: 230.5 second(s)
## Standard deviation: 1.58 second(s)
