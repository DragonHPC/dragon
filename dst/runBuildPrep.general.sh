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

# Force ssl use for yum repos:
echo "sslverify=true" >> /etc/yum.conf

# Turn off the mirrors in the yum repos, since they can route around our https
# requirement
sed -i "s:#\s*\(baseurl\):\1:g" /etc/yum.repos.d/*.repo
sed -i "s:\(mirrorlist=\):# \1:g" /etc/yum.repos.d/*.repo
yum clean all
yum make cache

${my_install} install \
  libfabric \
  libfabric-devel \
  libevent-devel \
  hwloc \
  hwloc-devel

# We need NVIDIA bits for our UCX support:
if [[ $(uname -m) == "aarch64" ]]
then
  curl -L -O https://arti.hpc.amslabs.hpecorp.net/artifactory/dragon-misc-master-local/hpcx-v2.18.1-gcc-mlnx_ofed-redhat7-aarch64.tbz
else
  curl -L -O https://arti.hpc.amslabs.hpecorp.net/artifactory/dragon-misc-master-local/hpcx-v2.18.1-gcc-mlnx_ofed-suse15.3-cuda12-x86_64.tbz
fi
mkdir mlnx && tar -xvf hpcx*.tbz -C mlnx && mv mlnx/hpcx*/ucx . && rm -rf mlnx hpcx*.tbz

if [[ $(uname -m) == "aarch64" ]]
then
  curl -L -O https://developer.download.nvidia.com/compute/cuda/redist/cuda_cudart/linux-aarch64/cuda_cudart-linux-aarch64-12.9.79-archive.tar.xz
else
  curl -L -O https://developer.download.nvidia.com/compute/cuda/redist/cuda_cudart/linux-x86_64/cuda_cudart-linux-x86_64-12.9.79-archive.tar.xz
fi
mkdir cuda && tar -xf cuda_cudart*.tar.xz -C cuda && mv cuda/cuda_cudart*/include cuda/ && rm -rf cuda/cuda_cudart* cuda_cudart*.tar.xz

if [[ $(uname -m) == "aarch64" ]]
then
  curl -L -O https://developer.download.nvidia.com/compute/cuda/redist/cuda_crt/linux-sbsa/cuda_crt-linux-sbsa-13.0.48-archive.tar.xz
else
  curl -L -O https://developer.download.nvidia.com/compute/cuda/redist/cuda_crt/linux-x86_64/cuda_crt-linux-x86_64-13.0.48-archive.tar.xz
fi
tar -xf cuda_crt*.tar.xz -C cuda && cp -r cuda/cuda_crt*/include cuda/ && rm -rf cuda/cuda_crt* cuda_crt*.tar.xz
ls cuda/include

# We need a newer pmix than is provided via centos/rhel repositories
curl -L -O https://github.com/openpmix/openpmix/releases/download/v5.0.8/pmix-5.0.8.tar.gz
mkdir pmix && \
  tar xvf pmix-5.0.8.tar.gz -C pmix && \
  mv pmix/pmix-*/* pmix/ && \
  cd pmix &&  \
  ./configure --prefix=/usr && \
  make -j6 && \
  make install && \
  cd ..

# We also need newer Libfabric header files:
curl -L -O https://github.com/ofiwg/libfabric/releases/download/v1.17.1/libfabric-1.17.1.tar.bz2
mkdir ofi && tar xvf libfabric*.bz2 -C ofi && mv ofi/libfabric-*/* ofi/ && rm -rf ofi/libfabric-* libfabric-*.bz2

# Dragon misc dependencies
# (net-tools appears to be required for hostname etc for unit tsts to pass)
${my_install} install \
  graphviz \
  ImageMagick \
  numactl-devel \
  transfig \
  java-11-openjdk \
  flex

# Install a newer doxygen than is available in centos7 repos:
curl -L -O https://arti.hpc.amslabs.hpecorp.net:443/artifactory/dragon-misc-master-local/doxygen-1.13.2.src.tar.gz
tar -xvf doxygen-1.13.2.src.tar.gz
cd doxygen-1.13.2
mkdir build && cd build
cmake -G "Unix Makefiles" ..
make -j6
make install

# SRMStoFigs for documenations
cd /usr/local/src && git clone https://github.com/kentdlee/SRMStoFigs.git
cd /usr/local/src/SRMStoFigs \
  && make -B srmstofigs CC=gcc \
  && install -D -m 755 srmstofigs srms2pdf srms2png /usr/local/bin \
  && cd -

# Install uv for venv management
curl -LsSf https://astral.sh/uv/install.sh | sh
