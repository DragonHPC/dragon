.. _orchestrate_mpi:

Orchestrate MPI Applications
++++++++++++++++++++++++++++

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
launches its own :py:class:`~dragon.native.process_group.ProcessGroup``, requesting Dragon set up an MPI-friendly
environment via the `pmi_enabled=True` kwarg. Note: launching an MPI application through
Dragon currently requires `cray-pmi`, though development is ongoing to extend support to
`PMIx`:

.. code-block:: python
    :linenos:
    :caption: **Generating data through an MPI app that is consumed by a python processing function**

    import os

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

        # the 'pmi_enabled' kwarg tells Dragon to manipulate the PMI
        # environment to allow execution of your MPI app.
        mpi_pg = ProcessGroup(pmi_enabled=True)

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

Add in how to delay initialization and connect into the infra.