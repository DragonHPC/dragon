#!/bin/bash

#To change the version of Python, please use the following commands:
# To find the version of Python on your system: ls -ls /usr/bin/python*
# To load the version of Python you want: `alias python=python<version>` or `alias python='\usr\bin\python<version>'`
# The following bounce test involves running "hello world" from 512 nodes. Dragon is stressed to see how it behaves under high load conditions.
# The following bounce timings are completed with 512 nodes. 

# time ./test_bounce.sh
# Cold start
# Hello world

# real    0m27.236s
# user    0m10.855s
# sys     0m3.460s
# Warm start
# Hello world

# real    0m14.061s
# user    0m6.471s
# sys     0m2.189s

# Overall Time for Execution
# real    0m41.322s
# user    0m17.327s
# sys     0m5.658s

# The following line of code can be run to allocate the number of nodes needed:
# salloc --nodes=512 --exclusive -t 01:00:00

echo "Cold start"
time dragon hello.py

echo "Warm start"
time dragon hello.py

# #The following lines of code ensure swift clean up.
# #dragon-cleanup
# #scancel -u $USER
