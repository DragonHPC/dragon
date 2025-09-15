"""Multiple producers multiple consumers communication scheme with
Multiprocessing and Dragon.

The code demonstrates the following key concepts working with Dragon:
* Serialization of data to pass it to another process. We use cloudpickle
  here, which is Python only. Multiprocessing with Dragon uses standard pickle,
  as default.
* Shared communication objects between processes, here a queue.
* Creating, starting and joining worker processes.
* Observe out-of-order execution and printing
"""

import random
import string
import cloudpickle

import dragon
import multiprocessing as mp


def producer(serialized_args: bytes) -> None:
    """Generate some string data, bundle it up with some random functions, add
    it to a queue.

    :param pickled_args: arguments to the function
    :type funcs: bytes
    """

    q, funcs = cloudpickle.loads(serialized_args)

    data = random.choices(string.ascii_lowercase, k=1)[0]
    for i in range(5):
        data = data + " " + "".join(random.choices(string.ascii_lowercase, k=i + 3))

    print(
        f'I am producer {mp.current_process().pid} and I\'m sending data: "{data}" and string ops:', end=" "
    )

    n = random.randint(1, len(funcs))
    chosen = random.sample(list(funcs.items()), n)  # random selection without replacement

    for item in chosen:
        print(item[0], end=" ")
    print(flush=True)

    work_pkg = cloudpickle.dumps((chosen, data))

    q.put(work_pkg)


def consumer(q: mp.queues.Queue) -> None:
    """Retrieve data from a queue, do some work on it and print the result.

    :param q: Queue to retrieve from
    :type q: mp.queues.Queue
    """

    # gives multi-node compatible Dragon puid, not OS pid.
    print(f"I am consumer {mp.current_process().pid} --", end=" ")

    serialized_data = q.get()  # implicit timeout=None here, blocking
    funcs, data = cloudpickle.loads(serialized_data)

    for identifier, f in funcs:
        print(f'{identifier}: "{f(data)}"', end=" ")
        data = f(data)
    print(flush=True)


if __name__ == "__main__":

    mp.set_start_method("dragon")

    # define some string transformations
    funcs = {
        "upper": lambda a: a.upper(),
        "lower": lambda a: a.lower(),
        "strip": lambda a: a.strip(),
        "capitalize": lambda a: a.capitalize(),
        'replace(" ", "")': lambda a: a.replace(" ", ""),
    }

    # use a queue for communication
    q = mp.Queue()

    # serialize producer arguments: Dragon uses pickle as default that doesn't
    # work with lambdas
    serialized_args = cloudpickle.dumps((q, funcs))

    # create & start processes
    processes = []
    for _ in range(8):
        p = mp.Process(target=producer, args=(serialized_args,))
        processes.append(p)
        p = mp.Process(target=consumer, args=(q,))
        processes.append(p)

    for p in processes:
        p.start()

    # wait for processes to finish
    for p in processes:
        p.join()
