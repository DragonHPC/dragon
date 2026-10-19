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
uv venv --python=3.10 _dev # For the building of dragon
uv venv --python=3.10 _env # For testing of release package