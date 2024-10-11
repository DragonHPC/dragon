#!/bin/bash

. /etc/profile
. /opt/cray/pe/modules/default/init/sh

set -ex

pwd
ls -alt

# Log system info
lscpu
cat /proc/meminfo
cat /etc/os-release

# Setup env

module use /opt/cray/pe/modulefiles
module use /opt/cray/pe/cpe-prgenv/default/modules
module use /opt/cray/pe/craype/default/modulefiles
module use $(pwd)/src/modulefiles
module avail

module load PrgEnv-gnu
module load craype-x86-rome craype-network-ofi cray-cti
module unload gcc
module load gcc/11.2.0
module unload cray-libsci cray-mpich


# Load cray-python module to get setup Python
# TODO Verify this configures Python 3.9
module load cray-python
module show cray-python

module load dragon-dev
module show dragon-dev

# Set url rules to allow submodules to get checked out
git config --global url."https://$HPE_GITHUB_TOKEN@github.hpe.com/".insteadOf "git@github.hpe.com:"

module list
cc --version
gcc --version

# Create virtual env
python3 -m venv --clear _env
. _env/bin/activate

# Install dependencies
python3 -m pip install --timeout=240 -U pip
python3 -m pip install --timeout=240 -r src/requirements.txt -c src/constraints.txt

# Configure HSTA for libfabric
./src/tools/dragon-config -a "ofi-include=/usr/include/:ofi-build-lib=/usr/lib64/:ofi-runtime-lib=/usr/lib64/"
./src/tools/dragon-config -a "ucx-include=$PWD/ucx/include:ucx-build-lib=$PWD/ucx/lib:ucx-runtime-lib=$PWD/ucx/lib"

# Build (dev mode)
make -C src build

# Run unit tests
make -C test test

# Build docs (requires dev build)
make -C doc -j ${DRAGON_BUILD_NTHREADS} dist

# Build release
rm -fr src/dist && make -C src release

GITHASH="$(git rev-parse --short HEAD)"
GITBRANCH="$(git branch --show)"

# Make fake RPMs
mkdir -p /home/jenkins/rpmbuild/RPMS
cp src/release/dragon-${DRAGON_VERSION}-${GITHASH}.tar.gz \
    /home/jenkins/rpmbuild/RPMS/dragon-${DRAGON_VERSION}-py${CRAY_PYTHON_VERSION}-${GITHASH}.rpm
cp doc/dragondocs-$DRAGON_VERSION-$GITBRANCH-$GITHASH.tar.gz \
    /home/jenkins/rpmbuild/RPMS/dragondocs-$DRAGON_VERSION-$GITHASH.rpm

# Publish docs
if [[ "${GITBRANCH}" = "master" ]] && [[ "${CRAY_PYTHON_VERSION}" =~ "3.10" ]]; then
    # push docs update only for cray-python/3.10.x and a master branch build
    git checkout master-docs
    git rm -rf docs
    mv doc/_build/html docs
    touch docs/.nojekyll
    git add -f docs
    git config --global user.email dst@example.com
    git config --global user.name DST Build
    git commit -m "automated update docs to master ${GITHASH}"
    git push https://$HPE_GITHUB_TOKEN@github.hpe.com/hpe/hpc-pe-dragon-dragon.git master-docs
    git checkout ${GITBRANCH}
fi
