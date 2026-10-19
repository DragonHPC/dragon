#!/bin/bash

set -ex

pwd
ls -alt

# Log system info
lscpu
cat /proc/meminfo
cat /etc/os-release

ARCH="x86_64"
if [[ $(uname -m) == "aarch64" ]]
then
  ARCH="aarch64"
fi

# Setup env
cc --version

# Create virtual env
source ~/.bashrc
source _dev/bin/activate
python3 --version
which python3


# Set up environment variables for building
source devtools/VARIABLES

# Use uv to do the dependency install. It's a bit faster.
uv pip install -U pip
uv pip install --upgrade setuptools
uv pip install build

# Install the most minimal of large packages that are needed for docs build.
# Install torch and torchvision together from the PyTorch CPU index so their
# compiled ops share a matching ABI (otherwise torchvision resolved from PyPI
# is built against a different torch and fails with "operator torchvision::nms
# does not exist").  Pre-installing torchvision here also keeps the later
# editable install from pulling an ABI-mismatched build from PyPI.
uv pip install torch torchvision --index-url https://download.pytorch.org/whl/cpu
uv pip install -v cupy-cuda13x

# Configure HSTA for libfabric
CURRENT_DIR=$(pwd -P)
python3 -m dragon.cli dragon-config add --ofi-include=$CURRENT_DIR/ofi/include/  \
                                        --ofi-build-lib=/usr/lib64/ \
                                        --ofi-runtime-lib=/usr/lib64/ \
                                        --ucx-include=/usr/include \
                                        --ucx-build-lib=/usr/lib64 \
                                        --ucx-runtime-lib=/usr/lib64 \
                                        --pmix-include=/usr/include:/usr/include/pmix

if [[ ${ARCH} == "x86_64" ]]
then
  python3 -m dragon.cli dragon-config add --hip-include=/opt/rocm/include \
                                          --ze-include=/usr/include/level_zero \
                                          --cuda-include=/usr/local/cuda-13.3/targets/x86_64-linux/include
else
  python3 -m dragon.cli dragon-config add --cuda-include=/usr/local/cuda-13.3/targets/sbsa-linux/include
fi
cat ${DRAGON_BASE_DIR}/dragon/.dragon-config.mk

# Build (dev mode)
if [[ ${ARCH} == "aarch64" ]]
then
  echo "Building dragon without installing docs dependencies on aarch64"
  # Skipping doc deps for aarch64 due to GLIBC_2.28 error from pytorch import
  cd src && uv pip install -e .[test] --verbose && cd -
else
  cd src && uv pip install -e .[test,docs] --verbose && cd -
fi

# Run unit tests
export PMIX_MCA_mca_base_component_show_load_errors=none # This quiets an annoying PMIx/OpenMPI component missing message
make -C test test


# Build docs (requires dev build)
if [[ $(uname -m) == "aarch64" ]]
then
  echo "Skipping doc build for aarch64 due to GLIBC_2.28 error from pytorch import"
else
  make -C doc -j ${DRAGON_BUILD_NTHREADS} dist
fi

# Build release
rm -fr src/dist && cd src && python -m build --wheel --verbose && cd -
make -C src release

GITHASH="$(git rev-parse --short HEAD)"
GITBRANCH="$(git branch --show)"
PYTHON_VERSION=`python -c 'import sys; version=sys.version_info[:3]; print("{0}.{1}.{2}".format(*version))'`
PYTHON_MAJOR_VERSION=`python -c 'import sys; print(sys.version_info[0])'`
PYTHON_MINOR_VERSION=`python -c 'import sys; print(sys.version_info[1])'`

# Make fake RPMs
if [[ $(uname -m) == "aarch64" ]]
then
  MY_RPM_CPU="aarch64"
else
  MY_RPM_CPU="x86_64"
fi
mkdir -p /workspace/RPMS/centos7/py${PYTHON_MAJOR_VERSION}.${PYTHON_MINOR_VERSION}
cp src/release/dragon-${DRAGON_VERSION}-${GITHASH}.tar.gz \
    /workspace/RPMS/centos7/py${PYTHON_MAJOR_VERSION}.${PYTHON_MINOR_VERSION}/dragon-${DRAGON_VERSION}-py${PYTHON_VERSION}-${GITHASH}.${MY_RPM_CPU}.rpm
if [[ $(uname -m) == "aarch64" ]]
then
  echo "Skipping doc build copy for aarch64 due to GLIBC_2.28 error from pytorch import"
else
  cp doc/dragondocs-$DRAGON_VERSION-$GITBRANCH-$GITHASH.tar.gz \
    /workspace/RPMS/centos7/dragondocs-$DRAGON_VERSION-$GITHASH.${MY_RPM_CPU}.rpm
fi

# Publish docs
if [[ "${GITBRANCH}" = "master" ]] && [[ "${PYTHON_VERSION}" =~ "3.13" ]]; then
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

# Remove _dev in order to free up some space for running compiled binary tests
deactivate
rm -rf _dev
