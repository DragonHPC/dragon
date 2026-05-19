FROM python:3.11-slim

RUN pip install --upgrade pip && pip install kubernetes

# Install all the package dependencies that are required
RUN apt upgrade -y && apt update && apt-get install -y \
    environment-modules \
    git-all \
    gcc \
    g++ \
    vim \
    pciutils \
    pip \
    cmake \
    build-essential \
    make \
    libtool \
    libnuma-dev \
    devscripts \
    dpkg-dev \
    fakeroot \
    wget \
    libibverbs-dev \
    librdmacm-dev \
    libibmad-dev \
    libibumad-dev \
    libfuse3-dev \
    libamdhip64-dev \
    doxygen \
    texlive-latex-base \
    lcov

# Install UCX
RUN wget http://deb.debian.org/debian/pool/main/u/ucx/ucx_1.17.0+ds-3.dsc \
    && wget http://deb.debian.org/debian/pool/main/u/ucx/ucx_1.17.0+ds.orig.tar.xz \
    && wget http://deb.debian.org/debian/pool/main/u/ucx/ucx_1.17.0+ds-3.debian.tar.xz \
    && dpkg-source -x ucx_1.17.0+ds-3.dsc \
    && cd ucx-1.17.0+ds \
    && dpkg-buildpackage -us -uc -b \
    && cd .. \
    && dpkg -i libucx*.deb ucx-utils*.deb \
    && apt-get install -f -y \
    && rm -rf ucx*
