# Dragon

## Introduction

Dragon is a distributed runtime environment with the goal of being a foundation for many
libraries, applications, tools, and services.  Software written on top of the Dragon runtime
benefits from lower complexity, interoperability, scalability, and performance from "laptop
to exaflop".

The Dragon project also includes implementations of strategically important APIs built on
the Dragon runtime with the goal of making programming HPC systems fast, fun, and productive.
The primary example of this is the standard Python multiprocessing API, which has driven many
of the requirements on the Dragon runtime.  Other APIs and services will come will be coming
in the future.

The Dragon runtime builds from basic operating system (OS) features to maximize portability and
incorporates numerous HPC techniques for performance and scalability.  It provides a higher
level abstraction of OS-like features including namespace, process and memory resource
management, and communication across a distributed system or systems.

## Directory Structure

| Directory       | Description                                                           |
|:----------------|-----------------------------------------------------------------------|
| .devcontainer/  | Docker development environment (via [VS Code Remote Containers])      |
| doc/            | Internal and public documentation                                     |
| examples/       | Examples and benchmarks for the Dragon API and Python multiprocessing |
| src/            | Source code and working development directory for all components      |
| test/           | Unit tests for all components                                         |
| external/       | External packages Dragon relies on                                    |

## Building and Installing a Package

The Dragon core requires only basic POSIX features in the OS, such as shared memory.  Dragon
should build and run on any modern Linux distribution.  See src/requirements.txt for a breakdown
of requirements and versions.  Using the modulefiles included with it to set the enviroment
requires environment-modules to be installed.  Modules must also be initialized in your shell
with a command like this (see CONTRIBUTING.md for a few more details):

```
. /usr/share/Modules/init/bash
```

To build a distribution package of Dragon that includes a Python wheel, run the following
from the top-level repo directory:

```
cd src
module use $PWD/modulefiles
module load dragon-dev
make dist
```

This will produce a tarfile inside of src/dragon-dist.  An error that may occur at this stage is
because the `wheel` Python package is not installed.  You'll know if you see an error suggesting
`python setup.py bdist_wheel` fails.  If you see that error, install the Python dependencies with

```
make install-pythondeps
```

To install the package from a new terminal:

```
tar -zxvf dragon-[rel info].tar.gz
cd dragon-[rel info]
pip install dragon-[rel info].whl
module use $PWD/modulefiles
module load dragon
```

You can then verify the install by running:

```
cd examples/multiprocessing
dragon p2p_lat.py --dragon
```

This should show output similar to the following if everything is setup correctly (note the
actual latency numbers will not be the same):

```
$ dragon p2p_lat.py --dragon
dragon p2p_lat.py --dragon
using Dragon
Msglen [B]   Lat [usec]
2  50.19235017243773
4  47.29981999844313
8  60.32353558111936
16  39.78859516791999
+++ head proc exited, code 0
```

## Building and Testing for Development

Setting up a development environment can be done locally, similar to the package building
steps above, or through a VSCode dev container.  See CONTRIBUTING.md For details on using
the VSCode container.  The steps here are the minimal steps for getting going on a build
so that you can then run tests. If you run into issues there are more hints and details
in CONTRIBUTING.md.

### Environment Setup and Building Dragon

The following lines assume that you have module commands configured. If you do not have them
configured on your system you may need to source a line like the following to enable them before
executing the other commands below.

```
. /opt/cray/pe/modules/default/init/bash
```

Begin by sourcing the `setup` script, which will enable the use of other scripts, such as `dragon-config`,
`dragon-cleanup`, and others.

```
. hack/setup
```

#### Configuring the Build for the High Speed Transport Agent (HSTA)

In order to configure the build and runtime environments in regards to external libraries, use
the `dragon-config` script. The `--help` option will give more details on its use.

When building for multi-node environments, make sure to specify the necessary include and lib directories
for the fabric backends using the `dragon-config --add` command with `<backend>-include=<path to include dir>`
and `<backend>-build-lib=<path to build-time lib dir>`, where `<backend>` can be `ofi` (Libfabric with the
CXI provider) or `ucx` (for Infiniband systems).

For running in multi-node environments, again use `dragon-config --add`, but this time using
`<backend>-runtime-lib=<path to runtime lib dir>`.

These can be combined into a single colon-separated list, e.g.:

```
dragon-config --add="ofi-include=/opt/cray/libfabric/1.15.2.0/include:ofi-build-lib=/opt/cray/libfabric/1.15.2.0/lib64:ofi-runtime-lib=/opt/cray/libfabric/1.15.2.0/lib64"
```

The current config is stored in `${HOME}/.dragon/dragon-config.json`. You can serialize the current config into a colon-separated string using `dragon-config --serialize`, or clean it out using `dragon-config --clean`.

If the build or runtime environment is not configured for the use of HSTA, Dragon will fall back on the
TCP transport agent.

#### Building

To completely build and set up the environment from scratch for single node execution, run these commands.

```
. hack/clean_build
```

Once you have built the package from scratch, new changes typically don't require a complete build. During
development, smaller changes can be followed by a

```
. hack/build
```

### Test Dragon

Once Dragon is built, you can run the unit tests as follows:

```
cd ../test
make
```

This runs all Dragon unit tests (not including multiprocessing unit tests).  To run all standard
Python multiprocessing unit tests, follow these steps starting next to this README.md file:

```
cd examples/multiprocessing/unittests
make
```

In the event your experiment goes awry, we provide a helper script to clean up any zombie processes and memory:

```
dragon-cleanup
```

## Contributing

Refer to CONTRIBUTING.md on processes and requirements for contributing to Dragon.

## Credits

The Dragon team is:

* Michael Burke [burke@hpe.com]
* Yian Chen [yian.chen@hpe.com]
* Eric Cozzi [eric.cozzi@hpe.com]
* Zach Crisler [zachary.crisler@hpe.com]
* Julius Donnert [julius.donnert@hpe.com]
* Veena Ghorakavi [veena.venkata.ghorakavi@hpe.com]
* Faisal Hadi (manager) [mohammad.hadi@hpe.com]
* Nick Hill [nicholas.hill@hpe.com]
* Maria Kalantzi [maria.kalantzi@hpe.com]
* Kent Lee [kent.lee@hpe.com]
* Pete Mendygral [pete.mendygral@hpe.com]
* Indira Pimpalkhare [indira.pimpalkhare@hpe.com]
* Davin Potts [davin.potts@hpe.com]
* Nick Radcliffe [nick.radcliffe@hpe.com]
* Colin Wahl [colin.wahl@hpe.com]
