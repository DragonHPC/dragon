#!/bin/bash

#dragon-cleanup > /dev/null 2>&1
#../../src/bin/dragon-cleanup > /dev/null 2>&1
dragon-cleanup > /dev/null 2>&1
rm ./*.log core > /dev/null 2>&1
rm ~/.dragon/my-runtime > /dev/null 2>&1
rm my-runtime > /dev/null 2>&1
# needed on some systems (e.g. pinoak) to launch MPI apps in dragon
module load PrgEnv-cray
cc -o mpi_hello mpi_hello.c
dragon server.py
rm mpi_hello
