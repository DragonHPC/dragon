Example Parsl `@mpi_app` Using Dragon Based Executor
++++++++++++++++++++++++++++++++++++++++++++++++++++

This shows an example workflow using Parsl with Dragon. In this example we use a Dragon implementation of the `@mpi_app` decorator and the `DragonMPIExecutor`. The executor expects five arguments to be returned from the decorated function: the executable, the directory containing the executable, the policy for process placement, the number of MPI processes to launch, and the arguments to pass to the executable. The arguments are expected to be returned in this order. The executor returns a future thats result is a dictionary containing a connection to stdin and stdout to rank 0.

In this example we compute the factorial of the largest MPI rank. We multiply this factorial by a scale factor that is sent using the stdin connection and add a bias to the scaled factorial that is passed to the MPI app via the args. The result is printed out by rank 0 and received by the head process from the stdout connection. This result is printed out and compared to the expected exact solution.

.. code-block:: python
    :linenos:
    :caption: parsl_mpi_app_demo.py: Example of running a Parsl MPI App with DragonMPIExecutor

    import dragon
    import multiprocessing as mp

    from dragon.workflows.parsl_mpi_app import mpi_app, DragonMPIExecutor
    from dragon.infrastructure.connection import Connection
    from dragon.infrastructure.policy import Policy
    import parsl
    from parsl.config import Config
    import math

    @mpi_app
    def mpi_factorial_app(num_ranks: int, bias: float, policy: Policy = None):
        """Example of what an mpi_app needs to return. The ordering of these arguments is important.

        :param num_ranks: number of mpi ranks
        :type num_ranks: int
        :param bias: bias for the computation
        :type bias: float
        :param policy: placement policy for ranks, defaults to None
        :type policy: Policy, optional
        :return: returns the executable string, the dir where the executable exists and will run, the placement policy, the number of mpi ranks, and a list of mpi args to pass to the mpi executable
        :rtype: tuple
        """
        import os

        # executable located in run_dir that we want to launch
        exe = "factorial"
        run_dir = os.getcwd()
        # list of the mpi args we want to pass to the app
        mpi_args = [str(bias)]
        # format that is expected by the DragonMPIExecutor
        return exe, run_dir, policy, num_ranks, mpi_args


    def send_scale_factor(stdin_conn: Connection, scale_factor: float) -> None:
        """
        Read stdout from the Dragon connection. Parse statistical data
        and put onto result queue.
        :param stdout_conn: Dragon serialized Channel descriptor used to read stdout data
        :type stdout_conn: str
        """

        string_to_send = str(scale_factor) + "\r"
        stdin_conn.send(string_to_send)
        stdin_conn.close()


    def get_results(stdout_conn: Connection) -> str:
        """
        Read stdout from the Dragon connection. Parse statistical data
        and put onto result queue.

        :param stdout_conn: Dragon serialized Channel descriptor used to read stdout data
        :type stdout_conn: Connection
        :return: output from mpi app
        :rtype: str
        """

        output = ""
        try:
            while True:
                output += stdout_conn.recv()
        except EOFError:
            # once we hit EOF we have captured all the
            pass
        finally:
            stdout_conn.close()

        return output


    def main():
        mp.set_start_method("dragon")

        config = Config(
            executors=[
                DragonMPIExecutor(),
            ],
            strategy=None,
        )

        parsl.load(config)

        bias = 10
        num_mpi_ranks = 10
        scale_factor = 1 / 10000
        connections = mpi_factorial_app(num_mpi_ranks, bias)
        send_scale_factor(connections.result()["in"], scale_factor)
        output_string = get_results(connections.result()["out"])
        print(
            f"mpi computation: {output_string}, exact = {scale_factor * math.factorial(num_mpi_ranks-1) + bias} ",
            flush=True,
        )


    if __name__ == "__main__":
        main()



The program output can be seen below:

.. code-block:: console
    :linenos:
    :caption: **Output when running parsl_mpi_app_demo.py**

    > salloc -N 2
    > make
    gcc -g  -pedantic -Wall -I /opt/cray/pe/mpich/8.1.27/ofi/gnu/9.1/include -L /opt/cray/pe/mpich/8.1.27/ofi/gnu/9.1/lib   -c -o factorial.o factorial.c
    gcc -g  -pedantic -Wall -I /opt/cray/pe/mpich/8.1.27/ofi/gnu/9.1/include -L /opt/cray/pe/mpich/8.1.27/ofi/gnu/9.1/lib  factorial.o -o factorial -lm -L /opt/cray/pe/mpich/8.1.27/ofi/gnu/9.1/lib -lmpich
    >$dragon dragon parsl_mpi_app_demo.py
    mpi computation: 0.000100 * 362880.000000 + 10.000000 = 46.288000 , exact = 46.288000000000004