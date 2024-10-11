#!/bin/bash

#
# install barebones PE PrgEnv-gnu in a DST container for Dragon bld
#

set -x

my_install='zypper --non-interactive --no-gpg-checks'

# Dragon misc dependencies
# (net-tools appears to be required for hostname etc for unit tsts to pass)

${my_install} install \
  tcl-devel \
  tk-devel \
  libffi-devel \
  net-tools \
  doxygen \
  graphviz \
  ImageMagick

# Apply a fix to ImageMagick that will allow us to convert PDFs to PNGs. For discussion, see:
# https://stackoverflow.com/questions/52861946/imagemagick-not-authorized-to-convert-pdf-to-an-image
sed -i 's:\(rights="\)write\(" pattern="PDF"\):\1read|write\2:g' /etc/ImageMagick-7/policy.xml

# Install transfig seprately in order to not install the entirety of texlive
${my_install} install --no-recommends transfig

# SRMStoFigs for documenations
cd /usr/local/src && git clone https://github.com/kentdlee/SRMStoFigs.git
cd /usr/local/src/SRMStoFigs \
  && make -B srmstofigs CC=gcc \
  && install -D -m 755 srmstofigs srms2pdf srms2png /usr/local/bin \
  && cd -

# skipping any cray-mpich, libfabric installs for building Dragon

# avoid adding rhel_8_3_pe repo otherwise you pull in other incompatible rhel rpms

rpm -ivh \
  https://arti.hpc.amslabs.hpecorp.net/artifactory/pe-rpm-stable-local/release/21.10/rhel_8_3_pe/x86_64/cray-pe-profile-ofi-rome-1.2-1.202105210342.2fd5694a802cb.el8.noarch.rpm

rpm -ivh \
  https://arti.hpc.amslabs.hpecorp.net/artifactory/mirror-SUSE-cache/Products/SLE-Module-Basesystem/15-SP4/x86_64/product/x86_64/libnuma-devel-2.0.14.20.g4ee5e0c-150400.1.24.x86_64.rpm