#!/bin/bash

#
# install barebones PE PrgEnv-gnu in a DST container for Dragon bld
#

set -x

pwd
ls -alt
lscpu

cat pipeline_env_vars.txt

my_install='yum -y'
ARCH="x86_64"
if [[ $(uname -m) == "aarch64" ]]
then
  ARCH="aarch64"
fi

# Force ssl use for yum repos:
echo "sslverify=true" >> /etc/yum.conf

# Turn off the mirrors in the yum repos, since they can route around our https
sed -i "s:#\s*\(baseurl\):\1:g" /etc/yum.repos.d/*.repo
sed -i "s:\(mirrorlist=\):# \1:g" /etc/yum.repos.d/*.repo

# Add NVIDIA and AMD repostiories for CUDA and ROCm packages
if [[ ${ARCH} == "x86_64" ]]
then
    dnf install -y https://arti.hpc.amslabs.hpecorp.net:443/artifactory/repo.radeon.com-remote/amdgpu-install/7.2.4/rhel/8/amdgpu-install-7.2.4.70204-1.el8.noarch.rpm
    dnf config-manager --add-repo https://arti.hpc.amslabs.hpecorp.net:443/artifactory/nvidia.com-cuda-remote/rhel8/${ARCH}/cuda-rhel8.repo
else
  dnf config-manager --add-repo https://arti.hpc.amslabs.hpecorp.net:443/artifactory/nvidia.com-cuda-remote/rhel8/sbsa/cuda-rhel8.repo
  dnf config-manager --save --setopt=cuda-rhel8-sbsa.baseurl=https://arti.hpc.amslabs.hpecorp.net:443/artifactory/nvidia.com-cuda-remote/rhel8/sbsa
  dnf config-manager --save --setopt=cuda-rhel8-sbsa.gpgkey=https://arti.hpc.amslabs.hpecorp.net:443/artifactory/nvidia.com-cuda-remote/rhel8/sbsa/D42D0685.pub
fi
yum clean all
yum makecache

# Install from repositories as much as possible
${my_install} install \
    libevent \
    libevent-devel \
    libfabric-devel \
    hwloc-devel \
    doxygen \
    ucx-devel \
    graphviz \
    ImageMagick \
    numactl-devel \
    transfig \
    java-11-openjdk \
    flex \
    python3.11 \
    python3.11-devel \
    python3.11-pip \
    cuda-minimal-build-13-3


if [[ ${ARCH} == "x86_64" ]]
then
  ${my_install} install hip-devel
  # Level Zero files aren't in a legit repo I can find. Build from source for the time.
  curl --proto '=https' --proto-redir '=https' -L -o level-zero.zip https://arti.hpc.amslabs.hpecorp.net:443/artifactory/github.com-remote/oneapi-src/level-zero/archive/refs/tags/v1.34.0.zip
  unzip -q level-zero.zip
  cd level-zero-1.34.0 && \
    cmake -S . -B build -DCMAKE_INSTALL_PREFIX=/usr && \
    cmake --build build -j6 && \
    cmake --install build && \
    cd ..
fi

# We need a newer pmix than is provided via centos/rhel repositories
curl --proto '=https' --proto-redir '=https' -L -O https://arti.hpc.amslabs.hpecorp.net:443/artifactory/github.com-remote/openpmix/openpmix/releases/download/v5.0.8/pmix-5.0.8.tar.gz
mkdir pmix && \
  tar xvf pmix-5.0.8.tar.gz -C pmix && \
  mv pmix/pmix-*/* pmix/ && \
  cd pmix &&  \
  ./configure --prefix=/usr && \
  make -j6 && \
  make install && \
  cd ..

# We need newer libfabric headers as well
curl --proto '=https' --proto-redir '=https' -L -O https://arti.hpc.amslabs.hpecorp.net:443/artifactory/github.com-remote/ofiwg/libfabric/releases/download/v1.17.1/libfabric-1.17.1.tar.bz2
mkdir ofi && tar xvf libfabric*.bz2 -C ofi && mv ofi/libfabric-*/* ofi/ && rm -rf ofi/libfabric-* libfabric-*.bz2

# SRMStoFigs for documenation
cd /usr/local/src && curl --proto '=https' --proto-redir '=https' -L -O https://arti.hpc.amslabs.hpecorp.net:443/artifactory/github.com-remote/kentdlee/SRMStoFigs/archive/refs/heads/master.zip
cd /usr/local/src && unzip master.zip && cd SRMStoFigs-master \
  && make -B srmstofigs CC=gcc \
  && install -D -m 755 srmstofigs srms2pdf srms2png /usr/local/bin \
  && cd -

# Change the PyPI index and use that to install uv
mkdir -p ~/.pip
echo -e '[global]\nindex-url = https://arti.hpc.amslabs.hpecorp.net/artifactory/api/pypi/pypi-remote/simple' > ~/.pip/pip.conf

# Install uv via the mirrored repo
python3.11 -m pip install -v uv

# Configure uv to use our internal artifactory for package installs
echo -e '[[index]]\nurl = "https://arti.hpc.amslabs.hpecorp.net/artifactory/api/pypi/pypi-remote/simple"\ndefault = true' > uv.toml
