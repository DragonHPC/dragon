FROM mcr.microsoft.com/vscode/devcontainers/base:ubuntu-22.04

ENV DEBIAN_FRONTEND=noninteractive

# Required packages for building Python via pyenv, see https://github.com/pyenv/pyenv/wiki#suggested-build-environment
RUN apt-get update \
    && export DEBIAN_FRONTEND=noninteractive \
    && apt-get install -y \
    ca-certificates git \
    make build-essential libssl-dev zlib1g-dev libbz2-dev libreadline-dev \
    libsqlite3-dev wget curl llvm libncursesw5-dev xz-utils tk-dev \
    libxml2-dev libxmlsec1-dev libffi-dev liblzma-dev \
    && rm -rf /var/lib/apt/lists/*

# Download pyenv to install Python
ENV PYENV_ROOT=/opt/pyenv
RUN git clone https://github.com/pyenv/pyenv.git $PYENV_ROOT

# Compile dynamic bash extension to speed up pyenv
RUN cd $PYENV_ROOT && src/configure && make -C src

# Build Python 3.9.4 by default.
ARG python_version=3.9.4
# Keep Python source files in /usr/local/src
ENV PYTHON_BUILD_BUILD_PATH=/usr/local/src/
# Install a Python with a shared object
ENV PYTHON_CONFIGURE_OPTS="--enable-shared"
# Build Python and clean-up downloaded tarball
RUN /opt/pyenv/plugins/python-build/bin/python-build --verbose --keep ${python_version} /usr/local \
    && rm -fr /usr/local/src/Python-${python_version}.tar.*

# Install development tools required by Dragon
RUN apt-get update \
    && export DEBIAN_FRONTEND=noninteractive \
    && apt-get install -y default-jdk doxygen git openssl sudo unzip util-linux clang-format \
    && rm -rf /var/lib/apt/lists/*

# Install SRMStoFigs dependencies
RUN apt-get update \
    && export DEBIAN_FRONTEND=noninteractive \
    && apt-get install -y imagemagick transfig \
    && rm -rf /var/lib/apt/lists/*
# See https://stackoverflow.com/questions/52998331/imagemagick-security-policy-pdf-blocking-conversion
RUN sed -i '/disable ghostscript format types/,+6d' /etc/ImageMagick-6/policy.xml

RUN curl https://raw.githubusercontent.com/git/git/master/contrib/completion/git-completion.bash -o ~/.git-completion.bash
COPY git_completion.bash /tmp/git_completion.bash
RUN cat /tmp/git_completion.bash >> ~/.bashrc
# Next line insures a newline before the cd && source line
RUN echo >> ~/.bashrc
RUN echo 'cd /workspaces/hpc-pe-dragon-dragon && source hack/setup' >> ~/.bashrc

# Install SRMStoFigs (required to build Dragon docs)
RUN cd /usr/local/src && git clone https://github.com/kentdlee/SRMStoFigs.git
RUN cd /usr/local/src/SRMStoFigs \
    && make -B srmstofigs CC=gcc \
    && install -D -m 755 srmstofigs srms2pdf srms2png /usr/local/bin

# Additional packages for convenience
RUN apt-get update \
    && export DEBIAN_FRONTEND=noninteractive \
    && apt-get install -y \
    debianutils environment-modules gdb git-lfs git-man graphviz man \
    manpages manpages-dev manpages-posix manpages-posix-dev ssh psmisc \
    rsync sshpass tree vim xfsprogs \
    && rm -rf /var/lib/apt/lists/*

# Python packages required by Dragon
COPY requirements.txt /usr/src/requirements.txt
COPY constraints.txt /usr/src/constraints.txt

RUN /usr/local/bin/python3 -m ensurepip \
    && /usr/local/bin/python3 -m pip install --no-cache-dir --upgrade pip \
    && /usr/local/bin/python3 -m pip install --no-cache-dir -r /usr/src/requirements.txt -c /usr/src/constraints.txt

# Packages for building dragon multinode dependencies
RUN apt-get update \
    && export DEBIAN_FRONTEND=noninteractive \
    && apt-get install -y \
    automake autopoint flex bison cmake libtool-bin elfutils \
    libboost-all-dev libelf-dev libssh2-1-dev libarchive-dev libdwarf-dev \
    libdw-dev libiberty-dev libacl1-dev \
    && rm -rf /var/lib/apt/lists/*


# Run script to allow non-root vscode user to map to your own uid
# by replacing uid with your own (result of `id -u <username>`)
# on host system
#  COPY library-scripts/common-debian.sh /tmp/library-scripts/
#  RUN apt-get update \
#       && bash /tmp/library-scripts/common-debian.sh false vscode \
#                uid automatic false false false

# We must expose a port to run the Jupyter Notebook example.
EXPOSE 8888
