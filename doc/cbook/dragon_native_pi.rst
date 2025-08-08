Calculating π using parallel Monte Carlo
++++++++++++++++++++++++++++++++++++++++

The code below contains a program that computes the value of constant π =
3.1415... to single precision using the standard Monte Carlo technique (see
e.g. Press et al. 1992, *Numerical Recipes in C*, section 7.6). 

The parent process starts a user defined number of managed Dragon processes as
workers executing the function `pi_helper` to parallelize the radius
computation. The parent process sends two random numbers to the workers using an
outgoing Dragon queue `q_send` and collects the radii from an incoming queue
`q_recv` to compute the result. It also computes a residual `delta` with respect
to the previous iteration. This is repeated
until convergence is reached. 

This example demonstrates two important paradigms of parallel programming with 
Dragon: How to execute Python processes using Dragon native and how to use
Dragon native Queues to communicate objects between processes.

.. code-block:: python
    :linenos:
    :caption: **pi_demo.py: Calculate the value of π in parallel using Dragon native**

    """ Calculate pi in parallel using the standard Monte Carlo technique 
        with the Dragon native interface. 

        NOTE: due to the missing Process object in Dragon native for version v0.3
        we are using the Multiprocessing process API here. This will change with
        v1.0
    """

    import sys
    import os
    import random
    import pickle
    import math

    import dragon
    from dragon.native.queue import Queue

    from multiprocessing import Process as Process  # only for Dragon v0.3


    def pi_helper(q_in: Queue, q_out: Queue) -> None:

        while True:  # keep working

            x, y = q_in.get(timeout=None)

            if x == -1 and y == -1:  # we're done here
                break

            r = x * x + y * y

            if r > 1.0:
                q_out.put(False)
            else:
                q_out.put(True)


    def main(num_workers):

        # create Dragon queues
        q_send = Queue()
        q_recv = Queue()

        # create & start processes
        processes = []
        for _ in range(num_workers):
            p = Process(target=pi_helper, args=(q_send, q_recv))
            p.start()
            processes.append(p)

        # start calculation
        random.seed(a=42)

        num_true = 0
        num_send = 0
        delta = 1
        count = 0

        while abs(delta) > 1e-6:

            # send new values to everyone
            for _ in range(num_workers):
                x = random.uniform(0.0, 1.0)
                y = random.uniform(0.0, 1.0)

                q_send.put((x, y), timeout=0)
                num_send += 1

            # receive results from everyone
            for _ in range(num_workers):

                is_inside = q_recv.get(timeout=None)

                if is_inside:
                    num_true += 1

            # calculate result of this iteration
            value = 4.0 * float(num_true) / float(num_send)
            delta = (value - math.pi) / math.pi

            if count % 512 == 0:
                print(f"{count:04}: pi={value:10.7f}, error={delta:8.1e}", flush=True)

            count += 1

        print(f"Final value after {count} iterations: pi={value}, error={delta}")

        # shut down all managed processes
        for _ in range(num_workers):
            q_send.put((-1.0, -1.0), timeout=0)  # termination message

        # wait for all processes to be finished
        for p in processes:
            p.join(timeout=None)


    if __name__ == "__main__":

        print(f"\npi-demo: Calculate π = 3.1415 ... in parallel using the Dragon native API.\n")

        try:
            num_workers = int(sys.argv[1])
        except:
            print(f"USAGE: dragon pi_demo.py $N")
            print(f"  N : number of worker puids to start")
            sys.exit(f"Wrong argument '{sys.argv[1]}'")

        print(f"Got num_workers = {num_workers}", flush=True)
        main(num_workers)

The program can be run using 2 workers with `dragon pi-demo.py 2` and results in
the following output:

.. code-block:: console
    :linenos:
    :caption: **Output when running pi_demo.py with 2 workers**

    >$dragon pi_demo.py 2

    pi-demo: Calculate π = 3.1415 ... in parallel using the Dragon native API.

    Got num_workers = 2
    0000: pi= 4.0000000, error= 2.7e-01
    0512: pi= 3.1189084, error=-7.2e-03
    1024: pi= 3.1531707, error= 3.7e-03
    1536: pi= 3.1659076, error= 7.7e-03
    2048: pi= 3.1449488, error= 1.1e-03
    2560: pi= 3.1409606, error=-2.0e-04
    3072: pi= 3.1389522, error=-8.4e-04
    3584: pi= 3.1391911, error=-7.6e-04
    4096: pi= 3.1354650, error=-2.0e-03
    4608: pi= 3.1277934, error=-4.4e-03
    5120: pi= 3.1267331, error=-4.7e-03
    5632: pi= 3.1319013, error=-3.1e-03
    6144: pi= 3.1446705, error= 9.8e-04
    Final value after 6342 iterations: pi=3.141595711132135, error=9.732459548770225e-07
    +++ head proc exited, code 0