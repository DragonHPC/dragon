import os
import dragon
from dragon.native.process import Popen
from dragon.data.ddict.ddict import DDict
from dragon.native.machine import System
import multiprocessing as mp
import pathlib
import argparse

test_dir = pathlib.Path(__file__).resolve().parent
os.system(f"cd {test_dir}; make --silent")

if __name__ == "__main__":
    mp.set_start_method("dragon")

    parser = argparse.ArgumentParser(description="Distributed Dictionary Training Example")
    parser.add_argument("--digits", type=int, default=5, help="Number of places after the decimal point of PI accuracy")
    parser.add_argument(
        "--trace",
        nargs="?",
        type=bool,
        default=False,
        const=True,
        help="Set to True to print simulation result for all clients in each iteration",
    )

    my_args = parser.parse_args()

    my_alloc = System()
    nnodes = my_alloc.nnodes
    ddict = DDict(1, nnodes, nnodes * int(4 * 1024 * 1024 * 1024), wait_for_keys=True, working_set_size=4, trace=True)
    ser_ddict = ddict.serialize()
    num_digits = my_args.digits
    trace = str(my_args.trace).lower()
    num_workers = 4
    procs = []

    # main processes (aggregate result)
    exe = "ddict_pi_sim_aggregate"
    aggregate_proc = Popen(
        executable=str(test_dir / exe),
        args=[ser_ddict, num_workers, num_digits, trace],
    )
    procs.append(aggregate_proc)

    # worker processes (compute result)
    exe = "ddict_pi_sim_train"
    for i in range(num_workers):
        proc = Popen(
            executable=str(test_dir / exe),
            args=[ser_ddict, i, num_workers, num_digits, trace],
        )
        procs.append(proc)

    for proc in procs:
        proc.wait()
        assert proc.returncode == 0, "CPP client exited with non-zero exit code"

    ddict.destroy()
