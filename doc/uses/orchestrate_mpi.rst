.. _orchestrate_mpi:

Orchestrate MPI Applications
++++++++++++++++++++++++++++

Python makes it easy to run functions in a new process through
:external+python:doc:`multiprocessing <library/multiprocessing>` and run executables through libraries like
:external+python:doc:`subprocess <library/subprocess>`. Python is a convenient tool for writing workflows that
orchestrate many things, but often that is limited by these APIs to a single node. Dragon makes it possible to orchestrate
across many nodes and even run parallel applications, such as those written with the Message Passing Interface (MPI)
most often used for HPC applications.

Using Dragon to orchestrate MPI applications makes it simple to develop workflows like parameter studies, where the
setup of a simulation code using MPI is varied in pursuit of "what if?" questions (e.g., "what if my airplane wing
was shaped like this?"). Dragon does not rely on a traditional workload manager (e.g., Slurm) to do this. Instead
Dragon runs on some set of nodes and orchestrates potentially many MPI applications run concurrently within those nodes.
This enables much more sophisticated workflows that can have dependencies or logical branches. In this short tutorial,
we'll show some basic examples to get you started.

Launching of MPI Applications
=============================

Say we want data generation to come about via the results of an MPI
model simulation. Once this data is generated, we want to be processed
outside of the MPI application. We can do this!

Let's take our :ref:`consumer/generator example <consumer_generator_example>` and
replace the data generation with an MPI app. We'll keep this simple and define a simple
C MPI app where each rank generates a random number and rank 0 gathers and prints
to stdout. stdout is then consumed by the "data generator" function and fed to
the "data consumer"

First let's define our MPI program:

.. code-block:: c
    :linenos:
    :caption: **MPI app code to generate random numbers gathered to rank 0**

    #include <stdlib.h>
    #include <stdio.h>
    #include <time.h>
    #include <mpi.h>


    int main(int argc, char *argv[])
    {
        // Initialize MPI and get my rank
        int rank, size;

        MPI_Init(&argc, &argv);
        MPI_Comm_rank(MPI_COMM_WORLD, &rank);
        MPI_Comm_size(MPI_COMM_WORLD, &size);

        // Generate a random number and gather to rank 0
        srand(time(NULL) * rank);
        float x = (float) (rand()  % (100));
        float *all_vals = NULL;

        if (rank == 0) {
            all_vals = (float *) malloc(size * sizeof *all_vals);
        }

        MPI_Gather(&x, 1, MPI_FLOAT, all_vals, 1, MPI_FLOAT, 0, MPI_COMM_WORLD);

        // Print the values to stdout to be consumed by dragon
        if (rank == 0) {
            for (int i = 0; i < size; i++) {
                fprintf(stdout, "%f\n", all_vals[i]); fflush(stdout);
            }
            free(all_vals);
        }

        return 0;
    }

And now the Dragon python code is the same except the data generation function
launches its own :py:class:`~dragon.native.process_group.ProcessGroup`, requesting Dragon set up an MPI-friendly
environment for a system with Cray PMI via the `pmi=PMIBackend.CRAY` kwarg. If the system is instead setup to use
PMIX for intializing an MPI application, set `pmi=PMIBackend.PMIX`
(see also :py:class:`~dragon.infrastructure.facts.PMIBackend`).

.. code-block:: python
    :linenos:
    :caption: **Generating data through an MPI app that is consumed by a python processing function**

    import os

    from dragon.infrastructure.facts import PMIBackend
    from dragon.native.process_group import ProcessGroup
    from dragon.native.process import ProcessTemplate, Process, Popen
    from dragon.native.queue import Queue
    from dragon.infrastructure.connection import Connection


    def parse_results(stdout_conn: Connection) -> tuple:

        x = []
        output = ""
        try:
            # this is brute force
            while True:
                output += stdout_conn.recv()
        except EOFError:
            pass
        finally:
            stdout_conn.close()

        split_line = output.split("\n")
        for line in split_line[:-1]:
            try:
                x_val = float(line)
                x.append(x_val)
            except (IndexError, ValueError):
                pass

        return x


    def data_consumer(q_in):

        input_values = q_in.get()

        for input_val in input_values:
            result = input_val * 2

            print(f'consumer computed result {result} from input {input_val}', flush=True)


    def data_generator(q_out, num_ranks):

        """Launch process group and parse data"""
        exe = os.path.join(os.getcwd(), "gather_random_numbers")

        mpi_pg = ProcessGroup(pmi=PMIBackend.CRAY)  # or PMIBackend.PMIX

        # Pipe the stdout output from rank 0, since we're going to do a simple MPI_Gather
        # to rank 0 of the MPI app
        mpi_pg.add_process(nproc=1, template=ProcessTemplate(target=exe, args=(), stdout=Popen.PIPE))

        # All other ranks should have their output go to DEVNULL
        mpi_pg.add_process(
            nproc=num_ranks - 1,
            template=ProcessTemplate(target=exe, args=(), stdout=Popen.DEVNULL),
        )

        # start the MPI process group
        mpi_pg.init()
        mpi_pg.start()

        # Create references to processes via the PUID values inside of the group object
        # This will allow us to parse their stdout
        group_procs = [Process(None, ident=puid) for puid in mpi_pg.puids]
        for proc in group_procs:
            if proc.stdout_conn:
                # get info printed to stdout from rank 0
                x = parse_results(proc.stdout_conn)
                q_out.put(x)
        # wait for workers to finish and shutdown process group
        mpi_pg.join()
        mpi_pg.close()


    def run_group():

        q = Queue()
        pg = ProcessGroup()

        num_ranks = 4
        generator_template = ProcessTemplate(target=data_generator,
                                             args=(q, num_ranks),
                                             stdout=Popen.DEVNULL)

        consumer_template = ProcessTemplate(target=data_consumer,
                                            args=(q,))

        pg.add_process(nproc=1, template=generator_template)
        pg.add_process(nproc=1, template=consumer_template)

        pg.init()
        pg.start()

        pg.join()
        pg.close()


    if __name__ == '__main__':

        run_group()


Running `mpi4py` Functions
==========================

:py:class:`~dragon.native.process_group.ProcessGroup` can also be used to run `mpi4py`, which is most easily done
following this recipe. First, we recommend that you access your `mpi4py` application as a target function rather
than starting `python` explictly as an executable. Next, you will need to delay MPI initialization and do it
explictly via the `mpi4py` API as opposed to the default automatic initialization done on import of `mpi4py`. The
primary motivation for this is to make it easy to pass Dragon objects to the MPI ranks, such as a
:py:class:`~dragon.native.queue.Queue` or :py:class:`~dragon.data.DDict`. Here is an example `mpi4py` application with
these suggested changes that also can access a Dragon :py:class:`~dragon.native.queue.Queue`:

.. code-block:: python
    :linenos:
    :caption: **Basic mpi4py application with a target main() function and MPI initialization done explicitly**

    import mpi4py
    mpi4py.rc.initialize = False

    from mpi4py import MPI

    def main(output_q):

        MPI.Init()  # now we can initializatize MPI

        comm = MPI.COMM_WORLD
        rank = comm.Get_rank()
        size = comm.Get_size()
        print(f"Rank {rank} of {size} says: Hello from MPI!", flush=True)

        # do some parallel work

        # let's write something unique from this MPI rank into the provided shared output Queue
        output_q.put(f"Rank {rank} did some work")


Let's say this code is in the local file `my_mpi4py.py` and can be imported with `import my_mpi4py`. The associated
Dragon application that uses :py:class:`~dragon.native.process_group.ProcessGroup` to orchestrate a single execution
of `my_mpi4py.py` looks like this:

.. code-block:: python
    :linenos:
    :caption: **Simple orchestrator for my_mpi4py that also allows ranks to communicate data back through a Queue**

    import os
    import my_mpi4py

    from dragon.infrastructure.facts import PMIBackend
    from dragon.native.process_group import ProcessGroup
    from dragon.native.process import ProcessTemplate
    from dragon.native.queue import Queue

    if __name__ == '__main__':

        q = Queue()
        pg = ProcessGroup(pmi=PMIBackend.CRAY)  # or PMIBackend.PMIX

        num_ranks = 16
        mpi_template = ProcessTemplate(target=my_mpi4py.main,
                                       args=(q,))

        pg.add_process(nproc=num_ranks, template=mpi_template)

        pg.init()
        pg.start()

        for _ in range(num_ranks):
            print(f"I got back: {q.get()}", flush=True)

        pg.join()
        pg.close()


Related Cookbook Examples
=========================

* :ref:`cbook_ai_in_the_loop`
* :ref:`cbook_mpi_workflow`
