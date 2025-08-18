#!/bin/bash

export DRAGON_TRANSPORT_TEST=1
export DRAGON_TRANSPORT_AGENT=tcp

cd ../../../ && . hack/multinode_config
cd -

# Remove any cores lying around from the CTI destructor that does
# a double free on an exit from main
rm core

# Can set up to 3 for increasing verbosity
export MRNET_DEBUG_LEVEL=0
export DRAGON_DEBUG=0
# Setting CTI_DEBUG will turn on logging
export CTI_DEBUG=0

# Tell CTI where to log to
mkdir -p log
export CTI_LOG_DIR=$PWD/log

rm -f log/*
rm -f *.log

# dragon_multi is a sym link to src/dragon/dragon_multi_fe.py
dragon_multi ./ch_p2p_benchmark.py

