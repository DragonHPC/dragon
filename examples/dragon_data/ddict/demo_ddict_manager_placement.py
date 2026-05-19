import dragon
from dragon.data.ddict.ddict import DDict
from dragon.infrastructure.policy import Policy
from dragon.native.machine import System, Node
import multiprocessing as mp
import socket


def client_function(d, client_id):

    key1 = "hello" + str(client_id)
    d[key1] = "world" + str(client_id)
    print(
        f"process on {socket.gethostname()} added {key1} to dictionary, main manager = {d.main_manager}, local manager = {d.local_manager} for this client",
        flush=True,
    )
    for manager_id, node in enumerate(d.manager_nodes):
        print(f"manager {manager_id} on {node.hostname}", flush=True)


def main():
    """
    Test put and get functions.
    """
    # bring up dictionary
    # number of manager = 2
    # number of nodes = 1
    # total size of dictionary = 2000000
    # clients and managers will be on different node in round-robin fashion
    my_alloc = System()
    node_list = my_alloc.nodes

    policies = []
    for i, node_id in enumerate(node_list[:-1]):
        node = Node(node_id)
        temp_policy = Policy(placement=Policy.Placement.HOST_NAME, host_name=node.hostname)
        policies.extend([temp_policy] * (i + 1))
    # note that we're going to ignore what it says for number of nodes.
    d = DDict(None, None, 1 * 1024 * 1024 * 1024, managers_per_policy=1, policy=policies)
    # create 10 clients, each of them implements client_function
    procs = []
    for i in range(10):
        proc = mp.Process(target=client_function, args=(d, i))
        procs.append(proc)
        proc.start()

    # waiting for all client process to finish
    for i in range(10):
        procs[i].join()
    # lookup all keys and vals
    try:
        for i in range(10):
            key = "hello" + str(i)
            val = d[key]
            print(f"d[{repr(key)}] = {repr(val)}")
            # print out all key and value given the key
            assert val == "world" + str(i)
    except Exception as e:
        print(f"Got exception {repr(e)}")
    # destroy dictionary
    d.destroy()


if __name__ == "__main__":
    main()
