#!/bin/bash

#dragon-cleanup > /dev/null 2>&1
../../src/bin/dragon-cleanup > /dev/null 2>&1
rm ./*.log core > /dev/null 2>&1
rm ~/.dragon/my-runtime > /dev/null 2>&1
cc -o mpi_hello mpi_hello.c
dragon server.py
rm mpi_hello
