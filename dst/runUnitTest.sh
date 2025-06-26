#!/bin/bash
export NCPU=4  # See Jenkinsfile in "dockerRunArguments" at --cpus

set -ex

pwd
ls -alt

# Log system info
lscpu
cat /proc/meminfo
cat /etc/os-release

# Setup a release environment
source ~/.bashrc
conda activate _env
python3 --version
which python3

# Install dependencies needed to run dragon and acceptance tests
# numpy<1.27 is just to confirm 1.x works with us as it's what TensorFlow
# requires
python3 -m pip install --timeout=120 -U pip
python3 -m pip install --timeout=120  \
    cloudpickle \
    cryptography \
    "numpy<1.27" \
    parameterized \
    scipy \
    wheel \
    pyyaml

# Extract release distribution
GITHASH="$(git rev-parse --short HEAD)"
PYTHON_VERSION=`python -c 'import sys; version=sys.version_info[:3]; print("{0}.{1}.{2}".format(*version))'`
PYTHON_MAJOR_VERSION=`python -c 'import sys; print(sys.version_info[0])'`
PYTHON_MINOR_VERSION=`python -c 'import sys; print(sys.version_info[1])'`
mkdir -p release
tar -C release -xvzf /workspace/RPMS/centos7/py${PYTHON_MAJOR_VERSION}.${PYTHON_MINOR_VERSION}/dragon-*-py${PYTHON_VERSION}-${GITHASH}.x86_64.rpm

python3 -m pip install release/dragon-*/dragonhpc-*.whl
version=$(python -c 'from importlib.metadata import version; print(version("dragonhpc"))')

ROOTDIR="$(realpath release/dragon-${version})"

# Install the config files into our site-packages:
dragon-config -a "ofi-include=$PWD/ofi/include/:ofi-build-lib=/usr/lib64/:ofi-runtime-lib=/usr/lib64/"
dragon-config -a "ucx-include=$PWD/ucx/include:ucx-build-lib=$PWD/ucx/lib:ucx-runtime-lib=$PWD/ucx/lib"

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
