Dragon - Scalable Distributed Computing Made Easy
===================================================

.. contents::


Introduction
-------------

Dragon is a composable distributed run-time for managing dynamic processes,
memory, and data at scale through high-performance communication objects. Some of
the key use cases for Dragon include distributed applications and
workflows for analytics, HPC, and converged HPC/AI.

Dragon brings scalable, distributed computing to a wide range of programmers. It
runs on your laptop, cluster, supercomputer, or any collection of networked
computers. Dragon provides an environment where you can write programs that
transparently use all the computers or nodes in your cluster. And the same
program will run on your laptop with no change, just at smaller scale.

While Dragon implements many APIs, the primary one is Python multiprocessing. If
you write your programs using Python's multiprocessing API, your program will run
now and in the future. Dragon does not re-define the multiprocessing API, but it
does extend it in some circumstances. For example, Dragon enables
multiprocessing's Process and Pool objects to execute across the nodes of a
distributed system, extending those objects which previously only supported
parallel execution on a single node.

Dive into Dragon in the next section to see how easy it is to begin Writing
and running distributed multiprocessing programs!

Quickstart
---------------

Dragon requires a Linux POSIX environment. It is not supported under Microsoft
Windows (r) or Apple Mac OS (r). However, it can run in a container on either
platform with Docker installed. Under Microsoft Windows you must install Docker
with the WSL (Windows Subsystem for Linux). The Dragon repository is configured
for use as a Docker container or it can run within a Linux OS either on bare
hardware or within a VM.

You can download or clone the repository from here and open it inside a container
to run it in single node mode within the container. The setup below allows you to
configure and run Dragon either inside a container or directly on a Linux
machine. If you have not used containers before, download Docker and Visual
Studio Code (VSCode) to your laptop, start Docker, and open the Dragon repository
in VSCode. VSCode will prompt you to reopen the directory inside a container. If
you run inside a container you will need to first build Dragon. Open a terminal
pane in VSCode and type the following from the root directory of the container.

Whether in a container or not, once you have a terminal window, navigate to the
root directory of the repository and type the following.

.. code-block:: console

    . hack/clean_build

After building has completed you will have Dragon built and be ready to write and
run Dragon programs.

Using a Binary Distribution
------------------------------

If you wish to run multi-node or don't want to run in a container, you must set
up your environment to run Dragon programs. Choose the version of Dragon to
download that goes with your installed version of Python. Python 3.9+ is required
to run Dragon. You must have Python installed and it must be in your path
somewhere. A common choice is to use a Python virtual environment, which can be
initialized from a base Python with:

.. code-block:: console

    python3 -m venv --clear _env
    . _env/bin/activate

The untarred distribution file contains several subdirectories. All provided commands
are relative to the directory that contains the README.rst.

* The `dragon-*.whl` file must be pip3 installed once for your environment.

.. code-block:: console

    pip3 install --force-reinstall dragon-0.8-cp39-cp39-linux_x86_64.whl

* Check and possibly update your `PATH` environment variable to include the location of
  pip installed console scripts, such as ~/.local/bin if you're not using a virtual environment.

.. code-block:: console

    export PATH=~/.local/bin:${PATH}

* You must set up the environment by loading the dragon module as follows.

.. code-block:: console

    module use [/path to dragon-0.8]/modulefiles
    module load dragon

If you intend to use Dragon on your own Linux VM or an image that you
personally installed, you may need to enable module commands by adding the
following command to your ~/.bashrc or other login script.

.. code-block:: console

    source /usr/share/modules/init/bash

If you use a different shell, look in the `init` directory for a script for
your shell.

You have completed the prerequisites for running Dragon with multiprocessing programs.

Running Dragon
==============

Single-node Dragon
--------------------

This set of steps show you how to run a parallel "Hello World" application using
Python multiprocessing with Dragon. Because Dragon's execution context in
multiprocessing is not dependent upon file descriptors in the same way as the
standard "fork", "forkserver", or "spawn" execution contexts, Dragon permits
multiprocessing to scale to orders of magnitude more processes on a single node
(think tens-of-thousands instead of hundreds). For single nodes with a larger
number of cores, starting as many processes as there are cores is not always
possible with multiprocessing but it is possible when using Dragon with
multiprocessing.

This demo program will print a string of the form `"Hello World from $PROCESSID
with payload=$RUNNING_INT"` using every cpu on your system. So beware if you're
on a supercomputer and in an allocation, your console will be flooded.

Create a file `hello_world.py` containing:

.. code-block:: python
   :linenos:
   :caption: **Hello World in Python multiprocessing with Dragon**

    import dragon
    import multiprocessing as mp
    import time


    def hello(payload):

        p = mp.current_process()

        print(f"Hello World from {p.pid} with payload={payload} ", flush=True)
        time.sleep(1) # force all cpus to show up


    if __name__ == "__main__":

        mp.set_start_method("dragon")

        cpu_count = mp.cpu_count()
        with mp.Pool(cpu_count) as pool:
            result = pool.map(hello, range(cpu_count))

and run it by executing `dragon hello_world.py`. This will result in an output like this:

.. code-block:: console

    dir >$dragon hello_world.py
    Hello World from 4294967302 with payload=0
    Hello World from 4294967301 with payload=1
    Hello World from 4294967303 with payload=2
    Hello World from 4294967300 with payload=3
    +++ head proc exited, code 0


Multi-node Dragon
-------------------

Dragon can run on a supercomputer with a workload manager or on your cluster. The
hello world example from the previous section can be run across multiple nodes
without any modification. The only requirement is that you have an allocation of
nodes (obtained with `salloc` or `qsub` on a system with the Slurm workload
manager) and then execute `dragon` within that allocation. Dragon will launch
across all nodes in the allocation by default, giving you access to all processor
cores on every node. If you don't have Slurm installed on your system or cluster,
there are other means of running Dragon multi-node as well. For more details see
`Running Dragon on a Multi-Node System <https://dragonhpc.org/>`_.


Code of Conduct
===============

Dragon seeks to foster an open and welcoming environment - Please see the `Dragon
Code of Conduct
<https://github.com/DragonHPC/dragon/blob/master/CODE_OF_CONDUCT.md>`_ for more
details.

Contributing
============

We welcome contributions from the community. Please see our `contributing guide
<https://github.com/DragonHPC/dragon/blob/master/CONTRIBUTING.rst>`_.

Credits
---------

The Dragon team is:

* Michael Burke [burke@hpe.com]
* Eric Cozzi [eric.cozzi@hpe.com]
* Zach Crisler [zachary.crisler@hpe.com]
* Julius Donnert [julius.donnert@hpe.com]
* Veena Ghorakavi [veena.venkata.ghorakavi@hpe.com]
* Faisal Hadi (manager) [mohammad.hadi@hpe.com]
* Nick Hill [nicholas.hill@hpe.com]
* Maria Kalantzi [maria.kalantzi@hpe.com]
* Ben Keen [ keen.benjamin.j@gmail.com]
* Kent Lee [kent.lee@hpe.com]
* Pete Mendygral [pete.mendygral@hpe.com]
* Davin Potts [davin.potts@hpe.com]
* Nick Radcliffe [nick.radcliffe@hpe.com]
* Rajesh Ratnakaram [rajesh.ratnakaram@hpe.com]
