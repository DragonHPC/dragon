#!/bin/bash

if [ -z "${1}" ]; then
    ta=tcp
else
    ta=${1}
fi

unset DRAGON_TRANSPORT_TEST
export DRAGON_TRANSPORT_AGENT=${ta}

module use $PWD/../../src/modulefiles
module load dragon-dev

# Remove any cores lying around from the CTI destructor that does
# a double free on an exit from main
rm core

# Can set up to 3 for increasing verbosity
export MRNET_DEBUG_LEVEL=1
export DRAGON_DEBUG=1
# Setting CTI_DEBUG will turn on logging
export CTI_DEBUG=1

# Tell CTI where to log to
mkdir -p log
export CTI_LOG_DIR=$PWD/log

rm -f log/*
rm -f *.log
# dragon_multi is a sym link to src/dragon/dragon_multi_fe.py
dragon_multi ./serial_compute.py

