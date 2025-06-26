#!/bin/bash

set -ex

export PRODUCT="dragon"
export MASTER_BRANCH="main"

setup_env() {

  if [[ -z ${VIRTUAL_ENV} ]] && [[ ! -d dstlibvenv ]]; then
    python3 -m venv dstlibvenv
    source dstlibvenv/bin/activate
    if [ -n "${PACKAGE_VERSION}" ]; then
      package="craydstlib==${version}"
    else
      package="craydstlib"
    fi
    pip install ${package} --trusted-host https://arti.hpc.amslabs.hpecorp.net \
      --extra-index-url https://arti.hpc.amslabs.hpecorp.net:443/artifactory/dst-pip-${quality}-local \
      --index-url https://arti.hpc.amslabs.hpecorp.net:443/artifactory/api/pypi/pypi-remote/simple
  else
    source dstlibvenv/bin/activate
  fi

}

build() {
    if ${NOT_LOCAL_BUILD} ; then
      dst-rpm build --not-local \
                    --product "${PRODUCT}" \
                    --yamlfile "./dst/manylinux2014_py${PYTHON_VERSION}.yaml" \
                    --main-branch "main" \
                    --container-software "podman"
    else
      dst-rpm build --local \
                    --product "${PRODUCT}" \
                    --yamlfile "./dst/manylinux2014_py${PYTHON_VERSION}.yaml" \
                    --main-branch "main" \
                    --container-software "podman"
    fi
}

usage() {
    cat <<EOF
Usage: $0 [options]
-h  display this message
-n  indicate that the build is not local
EOF
}

main() {
  setup_env
  build
}

export NOT_LOCAL_BUILD=false

if [ -n "${QUALITY_STREAM}" ]; then
  quality="${QUALITY_STREAM}"
else
  quality="master"
fi

ORIG_ARGS=("$@")
while [ -n "$1" ]; do
  case "$1" in
      -h | --help)
        usage
        shift
        ;;
      -n | --not-local)
        export NOT_LOCAL_BUILD=true
        shift
        ;;
      -p|--python-version)
        PYTHON_VERSION="$2"
        shift
        shift
        ;;
      -*|--*)
        echo "Unknown option $1"
        exit 1
        ;;
    *)
  esac
done

main
