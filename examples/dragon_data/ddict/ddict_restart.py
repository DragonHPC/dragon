import dragon
import multiprocessing as mp
import argparse
import traceback
import math
import numpy as np

from dragon.data.ddict import DDict
from dragon.native.machine import System


def main():

    my_alloc = System()
    nnodes = my_alloc.nnodes

    d = DDict(1, nnodes, nnodes * int(4 * 1024 * 1024 * 1024), wait_for_keys=True, working_set_size=4, timeout=200)
    d_parallel = DDict(
        1, nnodes, nnodes * int(4 * 1024 * 1024 * 1024), wait_for_keys=True, working_set_size=4, timeout=200
    )

    # the name of dictionary for restart later
    name_d = d.get_name()
    name_d_parallel = d_parallel.get_name()

    # TODO: some training here

    # destroy with restart specified so we will be able to recover dictiionary
    d.destroy(allow_restart=True)
    d_parallel.destroy(allow_restart=True)

    # set restart flag and the name of dictionary to restart
    d_restarted = DDict(
        1,
        nnodes,
        nnodes * int(4 * 1024 * 1024 * 1024),
        wait_for_keys=True,
        working_set_size=4,
        timeout=200,
        name=name_d,
        restart=True,
    )
    d_parallel_restarted = DDict(
        1,
        nnodes,
        nnodes * int(4 * 1024 * 1024 * 1024),
        wait_for_keys=True,
        working_set_size=4,
        timeout=200,
        name=name_d_parallel,
        restart=True,
    )

    # Return a list of managers that losts all keys during restart (this will happen if the managers restarted on new nodes that
    # didn't present before)
    empty_managers = d_restarted.empty_managers
    if len(empty_managers) != 0:
        # Some managers lost all keys so we need to recover it from other parallel dictionary
        DDict.synchronize_ddicts([d_restarted.serialize(), d_parallel_restarted.serialize()])

    # TODO: training continues

    d_restarted.destroy()
    d_parallel_restarted.destroy()


if __name__ == "__main__":
    main()
