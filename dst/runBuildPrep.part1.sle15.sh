#!/bin/bash

#
# install barebones PE PrgEnv-gnu in a DST container for Dragon bld
#

set -x

pwd
ls -alt
lscpu

cat pipeline_env_vars.txt


my_install='zypper --non-interactive --no-gpg-checks'

${my_install} addrepo \
  http://arti.hpc.amslabs.hpecorp.net/artifactory/pe-rpm-stable-local/release/21.10/sle15_sp1_pe \
  pe-21.10-release
  # repo has cpe-gcc-11.2.0, cray-mpich-8.1.10, cray-cti-2.15.[5,6] prgenv-gnu-8.[1,2].0

  # ${my_install} addrepo \
  # http://arti.hpc.amslabs.hpecorp.net/artifactory/pe-internal-rpm-stable-local/cray-mpt-deps/sles15sp3 \
  # cray-mpt-deps
  # repo has libfabric etc
  # the sles15sp1 repo at cray.com was retired/nonexistent--updated to sles15sp3 at hpecorp
  # but have left this repo off for now since it was not in use

${my_install} install \
  cray-modules-3.2.11.5 \
  craype-2.7.11 \
  cpe-support-21.10 \
  cpe-prgenv-gnu-8.2.0 \
  cpe-gcc-11.2.0 \
  cray-cti-2.15.5 \
  cmake \
  libfabric \
  libfabric-devel

# We need NVIDIA bits for our UCX support:
wget https://arti.hpc.amslabs.hpecorp.net/artifactory/dragon-misc-master-local/hpcx-v2.18.1-gcc-mlnx_ofed-suse15.3-cuda12-x86_64.tbz
mkdir mlnx && tar -xvf hpcx*.tbz -C mlnx && mv mlnx/hpcx*/ucx . && rm -rf mlnx hpcx*.tbz
