#!/bin/bash

set -x

export DRAGON_TRANSPORT_TEST=1

cd ../../ && . hack/multinode_config
cd -

export LD_LIBRARY_PATH=${CRAY_LD_LIBRARY_PATH}:${LD_LIBRARY_PATH}

# Remove any cores lying around from the CTI destructor that does
# a double free on an exit from main
rm core > /dev/null 2>&1

ulimit -c unlimited

# Can set up to 3 for increasing verbosity
export MRNET_DEBUG_LEVEL=1
export DRAGON_DEBUG=1

# Setting CTI_DEBUG will turn on logging
export CTI_DEBUG=1

# Tell CTI where to log to
mkdir -p log
export CTI_LOG_DIR=$PWD/log
export HSTA_DEBUG=cqe,general,gw_ch,tx_stats,work_req

rm -f log/* > /dev/null 2>&1
rm -f *.log > /dev/null 2>&1

srun ./cleanup

dragon ./transport_test.py
