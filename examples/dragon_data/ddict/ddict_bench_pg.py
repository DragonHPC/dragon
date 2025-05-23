import enum
import time
import dragon
import multiprocessing as mp
import argparse
import string
import random
import traceback
import sys

from dragon.infrastructure.parameters import this_process
from dragon.native.queue import Queue
from dragon.native.process_group import ProcessGroup
from dragon.native.process import ProcessTemplate
from dragon.data.ddict import DDict


@enum.unique
class DictOp(enum.Enum):
    """Action to be performed on the dictionary item"""

    SET_ITEM = enum.auto()
    GET_ITEM = enum.auto()
    DEL_ITEM = enum.auto()


def do_dict_ops(keys, ddict, client_id, iterations, msg_size, result_link, dict_op):
    """Function used to execute operations on the shared Dragon dictionary
    :param keys: List of all the keys of the dictionary
    :type keys: list
    :param ddict: A dragon dictionary object
    :type ddict: dragon dictionary
    :param client_id: Unique ID of the client
    :type client_id: int
    :param iterations: Number of iterations to perform a dictionary operation
    :type iterations: int
    :param msg_size: Number of characters used for the length of the value
    :type msg_size: int
    :param result_link: A pipe used for the communication of the results
    :type result_link: connection object
    :param dict_op: Enum that controls the operations of the dictionary
    :type dict_op: enum
    """
    try:
        if dict_op == DictOp.SET_ITEM or dict_op == DictOp.DEL_ITEM:
            letters = string.ascii_letters
            value = "".join(random.choice(letters) for i in range(msg_size))

        if dict_op == DictOp.SET_ITEM:
            start = time.monotonic()
            for i in range(iterations):
                key = random.choice(keys)
                try:
                    ddict[key] = value
                except Exception as ex:
                    print(ex, file=sys.stderr, flush=True)
                    print("This client process is exiting due to the exception.", file=sys.stderr, flush=True)
                    q.put((i, i))
                    ddict.detach()
                    return
            end = time.monotonic()
        elif dict_op == DictOp.GET_ITEM:
            start = time.monotonic()
            for i in range(iterations):
                key = random.choice(keys)
                val = ddict[key]
            end = time.monotonic()
        elif dict_op == DictOp.DEL_ITEM:
            start = time.monotonic()
            for i in range(iterations):
                # key = random.choice(keys)
                key = keys[i]
                del ddict[key]
                ddict[key] = value
            end = time.monotonic()

        result_link.put((start, end))

        ddict.detach()
    except Exception as e:
        tb = traceback.format_exc()
        print(f"There was an exception in do_dict_ops: {e} \n Traceback: \n {tb}", flush=True, file=sys.stderr)


def generate_keys(dict_size=100):
    """Generate a list including the keys that will be used for the dictionary.
    :param dict_size: Total number of keys to be populated in the dicitonary
    :type dict_size: int
    :return: List of keys to be stored along with values in the dictionary
    :rtype: list
    """
    my_keys = list()
    letters = string.ascii_letters

    for _ in range(dict_size):
        # each key is 30 characters long
        # key = ''.join(random.choice(letters) for i in range(30)) # characters can be repeated
        key = "".join(random.choice(letters) for i in range(8))  # characters can be repeated
        my_keys.append(key)

    assert len(my_keys) == dict_size
    return my_keys


def assign_keys(ddict, keys, value_size):
    """Initiate the dictionary. Assign the values to each key in the provided list.
    Each value is a string of msg_size characters long.
    :param ddict: A dragon dictionary object
    :type ddict: dragon dictionary
    :param keys: List of keys to assign values in the dictionary
    :type keys: list
    :param value_size: Number of characters used for the length of the value
    :type value_size: int
    """
    for key in keys:
        letters = string.ascii_letters
        value = "".join(random.choice(letters) for i in range(value_size))
        ddict[key] = value

    ddict.detach()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Distributed dictionary benchmark")
    parser.add_argument("--dict_size", type=int, default=500, help="Number of (key,value) pairs inside the dict")
    parser.add_argument(
        "--value_size", type=int, default=1024, help="size of the value (bytes) that are stored in the dict"
    )
    parser.add_argument("--num_nodes", type=int, default=1, help="number of nodes the dictionary distributed across")
    parser.add_argument(
        "--clients", type=int, default=10, help="number of client processes performing operations on the dict"
    )
    parser.add_argument(
        "--managers_per_node", type=int, default=1, help="number of managers per node for the dragon dict"
    )
    parser.add_argument(
        "--total_mem_size", type=float, default=1, help="total managed memory size for dictionary in GB"
    )
    parser.add_argument(
        "--batch_size", type=int, default=100, help="number of kv pairs added by each process before dict operations"
    )
    parser.add_argument("--iterations", type=int, default=1000, help="number of iterations")
    parser.add_argument(
        "--dict_ops",
        type=int,
        default=0,
        help="choose the operations to be performed on the dict -- "
        "0 to set the values, "
        "1 to get the values, "
        "2 for both, "
        "3 for deletes (includes setting the values for now to continue further deletes)",
    )

    my_args = parser.parse_args()

    mp.set_start_method("dragon")
    total_mem_size = int(my_args.total_mem_size * (1024 * 1024 * 1024))
    ddict = DDict(my_args.managers_per_node, my_args.num_nodes, total_mem_size, timeout=3000)

    num_clients = my_args.clients
    value_size = my_args.value_size

    if my_args.dict_ops == 3:
        dict_size = my_args.clients * my_args.iterations
    else:
        dict_size = my_args.dict_size
    all_keys = generate_keys(dict_size=dict_size)
    # Parallelize the initialization with batch size of key/value entries
    num_keys = len(all_keys)
    batch_size = my_args.batch_size
    num_batches = num_keys // batch_size + int(num_keys % batch_size != 0)
    print(f"{num_batches=}", file=sys.stderr, flush=True)
    jobs = []

    if num_batches > 0:
        grp = ProcessGroup(restart=False)
        for i in range(num_batches):
            if (i == num_batches - 1) and (num_keys % batch_size != 0):
                batch_keys = all_keys[i * batch_size :]
            else:
                batch_keys = all_keys[i * batch_size : (i + 1) * batch_size]

            grp.add_process(nproc=1, template=ProcessTemplate(target=assign_keys, args=(ddict, batch_keys, value_size)))

        # initialization of the dictionary
        grp.init()
        grp.start()
        grp.join()
        grp.close()

    length = len(ddict)
    print(f"Length of the dictionary is {length}", flush=True)

    dict_ops = []
    if my_args.dict_ops == 0:
        dict_ops.append(DictOp.SET_ITEM)
    elif my_args.dict_ops == 1:
        dict_ops.append(DictOp.GET_ITEM)
    elif my_args.dict_ops == 2:
        dict_ops.append(DictOp.SET_ITEM)
        dict_ops.append(DictOp.GET_ITEM)
    elif my_args.dict_ops == 3:
        dict_ops.append(DictOp.DEL_ITEM)

    for ii in range(len(dict_ops)):
        try:
            grp = ProcessGroup(restart=False)
            q = Queue()
            if dict_ops[ii] == DictOp.DEL_ITEM:
                for i in range(num_clients):
                    grp.add_process(
                        nproc=1,
                        template=ProcessTemplate(
                            target=do_dict_ops,
                            args=(
                                all_keys[i * my_args.iterations : (i + 1) * my_args.iterations],
                                ddict,
                                i,
                                my_args.iterations,
                                value_size,
                                q,
                                dict_ops[ii],
                            ),
                        ),
                    )
            else:
                grp.add_process(
                    nproc=num_clients,
                    template=ProcessTemplate(
                        target=do_dict_ops, args=(all_keys, ddict, 100, my_args.iterations, value_size, q, dict_ops[ii])
                    ),
                )

            grp.init()
            grp.start()

            min_start = time.monotonic()
            max_end = 0.0
            for i in range(num_clients):
                start, end = q.get()
                print(f"Iteration {i}: Received results from client with value {start=} and {end=}", flush=True)
                min_start = min(min_start, start)
                max_end = max(max_end, end)

            result = (max_end - min_start) / max(1, my_args.iterations)
            rate = (my_args.iterations * num_clients) / max(1, max_end - min_start)  # aggregated rate

            print(f"Msglen [B]   Lat [sec]\n{value_size}  {result}", flush=True)
            print(f"Msglen [B]   Rate\n{value_size}  {rate}\n ", flush=True)

            grp.join()
            grp.close()

        except Exception as ex:
            tb = traceback.format_exc()
            print(f"There was an exception in ddict_bench: {ex} \n Traceback: \n {tb}", flush=True)

    ddict.destroy()
