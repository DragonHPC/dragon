#!/bin/bash

#To change the version of Python, please use the following commands:
# To find the version of Python on your system: ls -ls /usr/bin/python*
# To load the version of Python you want: `alias python=python<version>` or `alias python='\usr\bin\python<version>'`
# The following bounce test involves running "hello world" from 512 nodes. Dragon is stressed to see how it behaves under high load conditions.
# The following bounce timings are completed with 512 nodes. 

#./test_bounce.sh
# Cold start
# Hello world

# real    0m26.281s
# user    0m10.906s
# sys     0m2.862s
# Warm start
# Hello world

# real    0m17.046s
# user    0m7.506s
# sys     0m2.127s

# The following line of code can be run to allocate the number of nodes needed:
# salloc --nodes=512 --exclusive -t 01:00:00

echo "Cold start"
time dragon hello.py

echo "Warm start"
time dragon hello.py

# #The following lines of code ensure swift clean up.
# #dragon-cleanup
# #scancel -u $USER
