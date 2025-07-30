import os
import argparse

import dragon
import multiprocessing as mp
from dragon.data.ddict import DDict


def client_function(distdict, client_id):

    key1 = str(os.getpid()) + "hello" + str(client_id)
    distdict[key1] = "world" + str(client_id)
    print(f"added {key1} to dictionary")
    return key1


def main(num_nodes=1):
    mp.set_start_method("dragon")

    d = dict()

    d["Miska"] = "Dog"
    d["Tigger"] = "Cat"
    d[123] = 456
    d["function"] = client_function

    distdict = DDict(1, num_nodes, 10000000)
    distdict["Miska"] = "Dog"
    distdict["Tigger"] = "Cat"
    distdict[123] = 456
    distdict["function"] = client_function

    with mp.Pool(5) as p:
        keys = p.starmap(client_function, [(distdict, client_id) for client_id in range(64)])

    print(keys)

    for key in keys:
        print(f"distdict[{repr(key)}] is mapped to {repr(distdict[key])}")

    distdict.destroy()


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Distributed dictionary demo")
    parser.add_argument("--num_nodes", type=int, default=1, help="number of nodes the dictionary distributed across")
    args = parser.parse_args()

    main(num_nodes=args.num_nodes)
