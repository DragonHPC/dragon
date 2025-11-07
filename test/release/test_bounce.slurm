#!/bin/bash
#SBATCH --nodes=550
#SBATCH --exclusive

source ~/.bashrc
conda activate _env312

#To change the version of Python, please use the following commands:
# To find the version of Python on your system: ls -ls /usr/bin/python*
# To load the version of Python you want: `alias python=python<version>` or `alias python='\usr\bin\python<version>'`
# The following bounce test involves running "hello world" from 512 nodes. Dragon is stressed to see how it behaves under high load conditions.
# The following bounce timings are completed with 512 nodes. 

# time ./test_bounce.sh -- 0.11 numbers on hotlum
# Cold start
# Hello world
#
# real    0m17.976s
# user    0m5.536s
# sys     0m1.704s
# Warm start
# Hello world
#
# real    0m11.531s
# user    0m4.156s
# sys     0m1.181s

# Overall Time for Execution
# real    0m41.322s
# user    0m17.327s
# sys     0m5.658s

# The following line of code can be run to allocate the number of nodes needed:
# salloc --nodes=512 --exclusive -t 01:00:00

which dragon

echo "Cold start"
time dragon -N 512 hello.py

echo "Warm start"
time dragon -N 512 hello.py

# #The following lines of code ensure swift clean up.
# #dragon-cleanup
# #scancel -u $USER
