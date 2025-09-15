import dragon
import multiprocessing as mp
import argparse
import traceback
import math
import numpy as np

from dragon.data.ddict import DDict
from dragon.native.machine import System

# The number of worker/clients of the distributed dictionary.
NUM_PROCS = 32


def in_circle(point):
    x = point[0]
    y = point[1]
    return math.sqrt(x**2 + y**2) <= 1


def proc_function(d: DDict, client_id: int, places: float, trace: bool) -> None:
    """Calculate an approximation of PI

    :param d: distributed dictionary
    :type d: DDict
    :param client_id: client ID of the process
    :type client_id: int
    :param places: Stops the simulation if the answer has converged to less than this amount of change.
    :type places: float
    :param trace: set to True to print out the result in each iteration
    :type trace: bool
    """
    try:
        d.pput(f"result{client_id}", 0)
        # Not necessary here to access num_procs via dictionary, but demonstrates persistent keys.
        num_procs = d["num_procs"]
        num_points = d["num_points"]
        step = 0
        avg = 0
        done = False
        while not done:
            # generate a set of random points in each iteration
            rand_points = np.random.uniform(-1, 1, (num_points, 2))
            num_points_in_circle = 0
            for point in rand_points:
                if in_circle(point):
                    num_points_in_circle += 1

            # calculate the ratio of points fall within the circle to total number of points generated
            new_sim = num_points_in_circle / num_points

            # calculate the weighted average of the ratio
            local_avg = ((num_procs * step * num_points) * avg + new_sim * num_points) / (
                (num_procs * step * num_points) + num_points
            )

            # write new simulation result to dictionary as a non-persistent key that will be updated in every future checkpoint
            d[client_id] = local_avg
            if trace:
                print(f"Checkpoint {step}, client {client_id}, local average is {local_avg*4}", flush=True)

            # calculate the average of the result from all other clients for this checkpoint
            sum_of_avgs = 0
            for i in range(num_procs):
                # This blocks until average is available from other clients for this checkpoint
                sum_of_avgs += d[i]
            prev_avg = avg
            avg = sum_of_avgs / num_procs

            # stop the loop if the it has converged to a value that changes less than places
            # on each iteration.
            if abs(avg - prev_avg) < places:
                done = True

            # moving to the next checkpoint/iteration
            d.checkpoint()
            step += 1

        print(f"PI simulation result = {4*avg} from client {client_id}.", flush=True)
        d.detach()
        print(f"Client {client_id} now detached from dictionary.", flush=True)

        return local_avg

    except Exception as ex:
        tb = traceback.format_exc()
        print(f"caught exception {ex}\n{tb=}", flush=True)


def main():
    mp.set_start_method("dragon")
    parser = argparse.ArgumentParser(description="Distributed Dictionary Training Example")
    parser.add_argument(
        "--digits", type=int, default=10, help="Number of places after the decimal point of PI accuracy"
    )
    parser.add_argument(
        "--trace",
        nargs="?",
        type=bool,
        default=False,
        const=True,
        help="Set to True to print simulation result for all clients in each iteration",
    )

    my_args = parser.parse_args()
    places = 5 ** -(my_args.digits + 1)

    my_alloc = System()
    nnodes = my_alloc.nnodes
    d = DDict(1, nnodes, nnodes * int(4 * 1024 * 1024 * 1024), wait_for_keys=True, working_set_size=4, timeout=200)
    procs = []

    # write meta data to dictionary as persistent keys
    d.pput("num_procs", NUM_PROCS)
    d.pput("num_points", 100000)  # each client generate 100000 points in each iteration

    fun_args = [(d, i, places, my_args.trace) for i in range(NUM_PROCS)]

    with mp.Pool(processes=NUM_PROCS) as pool:
        results = pool.starmap(proc_function, fun_args)

    # sync up head process to the newest checkpoint - not needed, but demonstrates
    d.sync_to_newest_checkpoint()

    print(f"Globally there were {d.checkpoint_id+1} checkpoints that were performed.", flush=True)

    # calculate the average result from final iteration
    # This would be one way to do it. In this case we have the results from the map function.
    # sum_of_avgs = 0
    # for i in range(NUM_PROCS):
    #     # This blocks until average is available from other clients for this checkpoint
    #     sum_of_avgs += d[i]
    avg = sum(results) / NUM_PROCS

    print(f"The result is {round(4*avg, my_args.digits)}", flush=True)
    print(f"Stats of ddict: {d.stats}", flush=True)

    d.destroy()


if __name__ == "__main__":
    main()
