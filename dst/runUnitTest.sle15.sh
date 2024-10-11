#!/bin/bash

. /etc/profile
. /opt/cray/pe/modules/default/init/sh

export NCPU=4  # See Jenkinsfile in "dockerRunArguments" at --cpus

set -ex

pwd
ls -alt

# Log system info
lscpu
cat /proc/meminfo
cat /etc/os-release

# Setup env

# Load cray-python module to get setup Python
# TODO Verify this configures Python 3.9
module use /opt/cray/pe/modulefiles
module load cray-python
module show cray-python

# Load dragon-dev module
module load dragon-dev
module show dragon-dev

# Need to load dragon-dev module to get version
module use $(pwd)/src/modulefiles
module load dragon-dev
module show dragon-dev

version="${DRAGON_VERSION}"

# Ensure dragon-dev module is not used
module unload dragon-dev
module unuse $(pwd)/src/modulefiles

module avail
module list

# Create virtual env
python3 -m venv --clear _env
. _env/bin/activate

# Install dependencies needed to run dragon and acceptance tests
python3 -m pip install --timeout=120 -U pip
python3 -m pip install --timeout=120 -c src/constraints.txt \
    cloudpickle \
    cryptography \
    numpy \
    parameterized \
    scipy \
    wheel \
    pyyaml

# Extract release distribution
GITHASH="$(git rev-parse --short HEAD)"
mkdir -p release
tar -C release -xvzf /home/jenkins/rpmbuild/RPMS/dragon-${version}-py${CRAY_PYTHON_VERSION}-${GITHASH}.rpm

ROOTDIR="$(realpath release/dragon-${version})"

# Load dragon module
module use $ROOTDIR/modulefiles/
module load dragon
module show dragon

# Install dragon wheel
python3 -m pip install $ROOTDIR/dragon-${version}-*.whl
python3 -m pip install $ROOTDIR/pycapnp-*.whl

# Re-run unit tests to verify wheel
make -C test test

# Run release unit tests
rm -f /dev/shm/*
pushd $ROOTDIR/dragon_unittests
python3 test_utils.py -f -v
python3 test_channels.py -f -v
popd

# Run Dragon core API tests
# TODO: dragon_core was removed from the package for now. We will
# want these back once we plan to start shipping them.
#rm -f /dev/shm/*
#sudo sync; sudo echo 3 > /proc/sys/vm/drop_caches
#pushd $ROOTDIR/examples/dragon_core/
#echo "Nothing to do"
#popd

## Run Dragon GS client API tests
#rm -f /dev/shm/*
#sudo sync; sudo echo 3 > /proc/sys/vm/drop_caches
#pushd $ROOTDIR/examples/dragon_gs_client/
#dragon queue_demo.py
#dragon pi_demo.py 4
## Confirm we handle input arguments to dragon and user script
#dragon --single-node-override pi_demo.py 4
## Reduce size of default memory pool to 0.5GB
#DRAGON_DEFAULT_SEG_SZ=536870912 dragon connection_demo.py
#dragon dragon_run_api.py ls
#dragon dragon_popen_api.py ls
#popd
#
## Run Dragon native API tests
#rm -f /dev/shm/*
#sudo sync; sudo echo 3 > /proc/sys/vm/drop_caches
#pushd $ROOTDIR/examples/dragon_native/
#dragon pi_demo.py 4
#popd

# Run multiproocessing unit tests
rm -f /dev/shm/*
pushd $ROOTDIR/examples/multiprocessing/unittests
make
popd

# Run multiprocessing benchmarks
#rm -f /dev/shm/*
#pushd $ROOTDIR/examples/multiprocessing/
#dragon p2p_lat.py --iterations 100 --lg_max_message_size 12 --dragon
#dragon p2p_bw.py --iterations 10 --lg_max_message_size 12 --dragon
#dragon aa_bench.py --iterations 100 --num_workers $NCPU --lg_message_size 21 --dragon
## TODO: Comment this test out for now, since it seems to be hanging
## (see PE-43717 - lock_performance.py test is hanging)
##dragon lock_performance.py --dragon
#popd

# Run multiprocessing numpy teests
#rm -f /dev/shm/*
#pushd $ROOTDIR/examples/multiprocessing/numpy-mpi4py-examples
#dragon numpy_scale_work.py
#dragon numpy_scale_work.py --dragon
#dragon scipy_scale_work.py --iterations=2 --burns=0
#dragon scipy_scale_work.py --dragon --iterations=2 --burns=0
#popd
