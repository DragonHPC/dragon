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

${my_install} install \
  libfabric \
  libfabric-devel \
  libevent-devel \
  hwloc \
  hwloc-devel \
  wget

# We need NVIDIA bits for our UCX support:
wget https://arti.hpc.amslabs.hpecorp.net/artifactory/dragon-misc-master-local/hpcx-v2.18.1-gcc-mlnx_ofed-suse15.3-cuda12-x86_64.tbz
mkdir mlnx && tar -xvf hpcx*.tbz -C mlnx && mv mlnx/hpcx*/ucx . && rm -rf mlnx hpcx*.tbz

# We need a newer pmix than is provided via centos/rhel repositories
wget https://github.com/openpmix/openpmix/releases/download/v5.0.8/pmix-5.0.8.tar.gz
mkdir pmix && \
  tar xvf pmix-5.0.8.tar.gz -C pmix && \
  mv pmix/pmix-*/* pmix/ && \
  cd pmix &&  \
  ./configure --prefix=/usr && \
  make -j6 && \
  make install && \
  cd ..

# We also need newer Libfabric header files:
wget https://github.com/ofiwg/libfabric/releases/download/v1.17.1/libfabric-1.17.1.tar.bz2
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
wget https://arti.hpc.amslabs.hpecorp.net:443/artifactory/dragon-misc-master-local/doxygen-1.13.2.src.tar.gz
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

# Install the common miniconda enviroments
mkdir miniconda3
wget https://arti.hpc.amslabs.hpecorp.net/artifactory/dragon-misc-master-local/Miniconda3-latest-Linux-x86_64.sh -O $PWD/miniconda3/miniconda.sh
bash $PWD/miniconda3/miniconda.sh -b -u -p $PWD/miniconda3
rm $PWD/miniconda3/miniconda.sh
rm -rf $PWD/miniconda3/miniconda.sh
source $PWD/miniconda3/bin/activate
conda init --all
source ~/.bashrc

conda config --add channels conda-forge
conda config --remove channels https://repo.anaconda.com/pkgs/r
conda config --remove channels https://repo.anaconda.com/pkgs/main
conda config --file $PWD/miniconda3/.condarc --remove channels https://repo.anaconda.com/pkgs/r
conda config --file $PWD/miniconda3/.condarc --remove channels https://repo.anaconda.com/pkgs/main
