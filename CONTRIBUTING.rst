Where to start
==================

We welcome contributions of any type (e.g., bug fixes, new features,
reporting issues, documentation, etc).

Please be aware of `Dragon's Code of Conduct
<https://github.com/DragonHPC/dragon/blob/master/CODE_OF_CONDUCT.md>`_.

If you are not familiar with GitHub pull requests, the main mechanism to
contribute changes to our code, there is `documentation available
<https://opensource.com/article/19/7/create-pull-request-github>`_.

.. Pete will give us link.
If you have questions or would like to discuss ideas, please post in our Slack's
`#dragon-discussion channel <https://dragonhpc-project.slack.com/>`_.
You can `join our Slack here
<https://join.slack.com/t/parsl-project/shared_invite/zt-4xbquc5t-Ur65ZeVtUOX51Ts~GRN6_g>`_.


Coding conventions
=====================

Dragon code should adhere to Python PEP-8. Dragon also contains C code and that code
conforms to a hybrid of KNR programming style and the POSIX API style. If you find
that you want/need to write some C code, please take a look at other C code examples
and try to match that style.

Naming conventions
---------------------

In Python code PEP-8 guidelines follows the ClassName, ExceptionName, CONSTANT,
method_or_function, and self._instance_variables naming conventions. If you are
unsure, take a look at other Dragon Python code and follow the examples you find there.

Version increments
==================

Dragon follows the `calendar versioning scheme <https://calver.org/#scheme>`_
with ``YYYY.MM.DD`` numbering scheme for versions. This scheme was chosen
following a switch from semantic versioning and manual release processes to an
automated weekly process.

Documentation
==================

All new code should be documented. Functions and classes must have
documentation to describe their purpose and the types and use of all
parameters. Follow the examples found in other code of the same language.

Python code use pydoc and should include docstrings. C and C++ code should be
documented in the Doxygen style. C and C++ code documentation is pulled into
the Dragon docs using Breathe on top of Doxygen. Breathe is a package that
brings doxygen to Sphinx documentation, which is what we use to generate all
Dragon documentation.

Testing
=======

Dragon uses the Python ``unittest`` framework to run most tests. All tests should
be placed in the ``tests`` subdirectory.

Any new code must have accompanying tests to thoroughly test all paths through the code.
Tests are added under the ``tests`` subdirectory and tied into the Makefile so
running ``make`` in the tests directory will run the tests.

There are two broad groups of tests: single-node tests and multi-node tests. Multi-node tests are
contained within the ``tests/multinode`` sudirectory.

Directory Structure
======================

Dragon's repository is organized as follows. New contributions to the Dragon
project will typically include source code, documentation, tests, and possibly
example code, touching on most of the major subdirectories of the repository.

| Directory       | Description                                                           |
|:----------------|-----------------------------------------------------------------------|
| .devcontainer/  | Docker development environment (via [VS Code Remote Containers])      |
| doc/            | Internal and public documentation                                     |
| examples/       | Examples and benchmarks for the Dragon API and Python multiprocessing |
| src/            | Source code and working development directory for all components      |
| test/           | Unit tests for all components                                         |
| external/       | External packages Dragon relies on                                    |

Contribution Workflow
=======================

A typical workflow on a Linux box is as follows:

1. Clone the repository

```
git clone [repo url]
cd dragon
```

2. Create and checkout your working branch
```
git checkout branch
```

3. Setup your environment (requires the Modules package be installed) on a system with no container.

This first step is required only if your shell does not have the `module` command
already initialized. On some systems the path to module initialization is
/usr/share/modules/init/[shell] where you replace the end of the path with your
shell name. For instance,

```
. /usr/share/modules/init/bash
```

If your module initialization is not found there, you can look up where they are
for your Linux distribution by doing an internet search. With the `module`
command setup, you then execute the following to complete environment setup.

The very first time you want to build after cloning the repository, you'll want to do a
`clean_build` to set up the Python environment and get it all built.

```
. hack/clean_build
```

After doing this once, on subsequent builds you can use the `build` helper script to build dragon

```
. hack/build
```

If you have already built it and just want to run something, then you can setup the
environment with

```
. hack/setup
```

If things aren't working and you need a fresh build you can try a clean build again with

```
. hack/clean_build
```

4. If you are running a container, the setup is even simpler. You can do the following
steps to build in a container.

```
cd src && make distclean && make
```

and to rebuild in the container you can use

```
cd src && make
```


5. Make code, test, and documentation modification as-needed for your task

6. Run the unit tests and verify all tests pass

```
cd ../test
make test
```

7. Verify the documentation builds correctly if you made documentation changes

```
cd ../doc
make
```

8. Commit your changes locally

```
git commit -m "enter a useful message here about the commit" [changed files]
```

9. Push your changes to origin

```
git push
```

10. Start a pull request review on Github


Additional Tools
===================

There is a `hack` directory at the top-level where additional helper scripts can be placed.  Anyone
adding scripts there is responsible for maintaining them.  The scripts are:

| Script       | Purpose                                                                          |
|:-------------|----------------------------------------------------------------------------------|
| setup        | File to source that runs two module commands needed to setup a build environment |
| build        | Allows you to be in any directory and rebuild Dragon                             |
| clean_build  | Cleans up /dev/shm and logfiles from Dragon                                      |
| where4all    | Batch script that executes script.gbd on processes & threads to get their status |

Dev Container Development
===========================

If using VS Code, the [Remote Containers extension] supports opening your
working tree in a container. Run the **Remote-Containers: Open Folder in
Container...** command from the Command Palette (`F1`) or quick actions Status
bar item, and select your Dragon project folder. VS Code automatically builds
the container image and runs it based on the configuration in
.devcontainer/devcontainer.json.

The dev container image (see .devcontainer/Dockerfile) is based on Ubuntu. It
includes appropriate versions of required tools to build and test Dragon as well
as the documentation. In particular, Python is built from source using [pyenv],
and it's source is available in /usr/local/src.

> Note that VS Code is not required to build the image, but it is recommended.
> To build using Docker directly, e.g.:
>
> ```
> $ docker build -t dragon-dev .devcontainer
> ```
>
> As long as the cache is used when building the image, it will result in the
> same image ID as the one built by VS Code, e.g.:
>
> ```
> $ docker images
> REPOSITORY                                                  TAG          IMAGE ID       CREATED        SIZE
> dragon-dev                                                  latest       c37896698c3f   25 hours ago   2.23GB
> vsc-hpc-pe-dragon-dragon-ec2b3104eaef710f570ebd5fd48d2534   latest       c37896698c3f   25 hours ago   2.23GB
> ```

Once VS Code has opened your folder in the dev container, any terminal window
you open in VS Code (**Terminal > New Terminal**) will automatically run a Bash
interactive login shell in the container rather than locally. The dev container
configuration includes additional setup to load the `dragon-dev` module in login
shells and refresh the Git index since the host OS is most likely not Linux, see
https://stackoverflow.com/questions/62157406/how-to-share-a-git-index-between-2-oses-avoid-index-refresh
and
https://stackoverflow.com/questions/59061816/git-forces-refresh-index-after-switching-between-windows-and-linux
for more information.

> To build or test Dragon in a container not managed by VS Code, you will need
> to properly initialize the environment by loading the `dragon-dev` module:
>
> ```
> $ docker run --rm -ti -u "$(id -u):$(id -g)" -v "$(pwd):/dragon" dragon-dev
> / $ . /etc/profile.d/modules.sh
> / $ module use /dragon/src/modulefiles
> / $ module load dragon-dev
> ```

Note that for convenience VS Code remaps the root user in the
container (UID 0) to your local user e.g.:

```
root ➜ /workspaces/hpc-pe-dragon-dragon (ubuntu-dev-container ✗) $ whoami
root
root ➜ /workspaces/hpc-pe-dragon-dragon (ubuntu-dev-container ✗) $ id
uid=0(root) gid=0(root) groups=0(root)
root ➜ /workspaces/hpc-pe-dragon-dragon (ubuntu-dev-container ✗) $ ls -l
total 24
-rw-r--r--   1 root root 11846 Mar 29 21:03 CONTRIBUTING.md
drwxr-xr-x   6 root root   192 Mar 29 21:03 demo
drwxr-xr-x  11 root root   352 Mar 29 21:03 doc
drwxr-xr-x   5 root root   160 Mar 29 21:03 dst
drwxr-xr-x   5 root root   160 Mar 29 21:03 examples
drwxr-xr-x   6 root root   192 Mar 29 21:04 external
drwxr-xr-x   5 root root   160 Mar 29 21:04 hack
-rw-r--r--   1 root root   200 Mar 29 15:05 Jenkinsfile.sle15sp1
-rw-r--r--   1 root root  6213 Mar 29 21:03 README.md
drwxr-xr-x  22 root root   704 Mar 30 14:33 src
drwxr-xr-x 119 root root  3808 Mar 29 21:04 test
```

> When running a dev container using Docker directly, it's usually sufficient to
> specify the effective user via `-u "$(id -u):$(id -g)"` in order to ensure
> consistent file system permissions. E.g.:
>
> ```
> $ docker run --rm -ti -u "$(id -u):$(id -g)" -v "$(pwd):/dragon" dragon-dev
> / $ whoami
> whoami: cannot find name for user ID 503
> / $ id
> uid=503 gid=12790 groups=12790
> / $ ls -l dragon
> total 8
> -rw-r--r--  1 root root  885 Feb  2 16:06 README.md
> drwxr-xr-x 12 root root  384 Feb  2 16:06 demo
> drwxr-xr-x  4 root root  128 Feb  7 15:55 external
> -rwxr-xr-x  1 root root 1373 Feb  2 16:06 setup.sh
> drwxr-xr-x 30 root root  960 Feb  8 15:41 src
> drwxr-xr-x 54 root root 1728 Feb  8 22:34 test
> ```

Build in a Container
-----------------------

Once you have a dev container running, you can build as you normally
would using `make`:

```
root ➜ /workspaces/hpc-pe-dragon-dragon (ubuntu-dev-container ✗) $ cd src
root ➜ /workspaces/hpc-pe-dragon-dragon/src (ubuntu-dev-container ✗) $ make
```

You can also use the hack scripts inside a container. The ``clean_build``,
``build``, and ``setup`` scripts work to do a clean build, a rebuild, and
setup and activate the environment respectively.

Testing in a Container
-------------------------

By default, VS Code appropriately initializes the environment in order to run
tests, e.g.:

```
root ➜ /workspaces/hpc-pe-dragon-dragon (ubuntu-dev-container ✗) $ cd test
root ➜ /workspaces/hpc-pe-dragon-dragon/test (ubuntu-dev-container ✗) $ make
```


Building and Installing a Package
====================================

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

Building and Testing for Development
--------------------------------------

Setting up a development environment can be done locally, similar to the package
building steps above, or through a VSCode dev container. The steps here are the
minimal steps for getting going on a build.

Environment Setup and Building Dragon
+++++++++++++++++++++++++++++++++++++++

The following lines assume that you have module commands configured. If you do not have them
configured on your system you may need to source a line like the following to enable them before
executing the other commands below.

```
. /usr/share/Modules/init/bash
```

To completely build and set up the environment from scratch for single node execution, run these commands.

```
. hack/clean_build
```

Once you have built the package from scratch, new changes typically don't require a complete build. During
development, smaller changes can be followed by a

```
. hack/build
```

If you are starting a new terminal session and already have Dragon build, you can just source the `setup`:

```
. hack/setup
```

Testing the Build
+++++++++++++++++++

Once Dragon is built, you can run the unit tests as follows:

```
cd ../test
make
```

This runs all Dragon unit tests (not including multiprocessing unit tests). To
run all standard Python multiprocessing unit tests, follow these steps starting
in the root directory of the repository:

```
cd examples/multiprocessing/unittests
make
```

In the event your experiment goes awry, we provide a helper script to clean up
any zombie processes and memory:

```
dragon-cleanup
```


