#!/bin/bash

#
# install barebones PE PrgEnv-gnu in a DST container for Dragon bld
#

set -x

pwd
ls -alt
lscpu
unset TARGET_ARCH

cat pipeline_env_vars.txt

./dst/runBuildPrep.general.sh

source ~/.bashrc
conda create -y  -n _dev python=3.11  # For the building of dragon
conda create -y  -n _env python=3.11  # For testing of release package
