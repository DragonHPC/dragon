#!/bin/bash

set -ex

pwd
ls -alt

# Log system info
lscpu
cat /proc/meminfo
cat /etc/os-release

# Setup env
cc --version

# Create virtual env
source ~/.bashrc
conda activate _dev
conda remove -y libstdcxx || true
conda remove -y libstdcxx-ng || true
python3 --version
which python3


# Set up environment variables for building
source hack/VARIABLES

# Install dependencies
python3 -m pip install --timeout=240 -U pip
python3 -m pip install --timeout=240 -r src/requirements.txt
python3 -m pip install --upgrade setuptools

# Configure HSTA for libfabric
python3 -m dragon.cli dragon-config add --ofi-include=$PWD/ofi/include/ \
                                        --ofi-build-lib=/usr/lib64/ \
                                        --ofi-runtime-lib=/usr/lib64/ \
                                        --ucx-include=$PWD/ucx/include \
                                        --ucx-build-lib=$PWD/ucx/lib \
                                        --ucx-runtime-lib=$PWD/ucx/lib \
                                        --pmix-include=/usr/include,/usr/include/pmi
cat ${DRAGON_BASE_DIR}/dragon/.dragon-config.mk

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
PYTHON_VERSION=`python -c 'import sys; version=sys.version_info[:3]; print("{0}.{1}.{2}".format(*version))'`
PYTHON_MAJOR_VERSION=`python -c 'import sys; print(sys.version_info[0])'`
PYTHON_MINOR_VERSION=`python -c 'import sys; print(sys.version_info[1])'`

# Make fake RPMs
mkdir -p /workspace/RPMS/centos7/py${PYTHON_MAJOR_VERSION}.${PYTHON_MINOR_VERSION}
cp src/release/dragon-${DRAGON_VERSION}-${GITHASH}.tar.gz \
    /workspace/RPMS/centos7/py${PYTHON_MAJOR_VERSION}.${PYTHON_MINOR_VERSION}/dragon-${DRAGON_VERSION}-py${PYTHON_VERSION}-${GITHASH}.x86_64.rpm
cp doc/dragondocs-$DRAGON_VERSION-$GITBRANCH-$GITHASH.tar.gz \
    /workspace/RPMS/centos7/dragondocs-$DRAGON_VERSION-$GITHASH.x86_64.rpm

# Publish docs
if [[ "${GITBRANCH}" = "master" ]] && [[ "${PYTHON_VERSION}" =~ "3.10" ]]; then
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
