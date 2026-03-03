# Building

## Requirements

Dragon requires:

  * Python 3.10, 3.11, or 3.12
  * GCC > 9.0

Other depedencies are installed during the environment setup or are optional.

## Environment Setup

The `hack/setup` script does most of the work. Follow these steps:

    export DRAGON_BASE_DIR=$PWD/src
    . hack/setup

If the HSTA source is in your tree, you can enable building it by adding in the appropriate communication backends with
`dragon-config`. For example,

    dragon-config add --ofi-include=/opt/cray/libfabric/2.2.0rc1/include \
                    --ofi-build-lib=/opt/cray/libfabric/2.2.0rc1/lib64 \
                    --ofi-runtime-lib=/opt/cray/libfabric/2.2.0rc1/lib64

Then to build:

    . hack/clean_build

To rebuild without clean:

    . hack/build

## Testing

Unit tests are run as:

    make -C test test