import dragon
import multiprocessing as mp

import os
import math
import torch
from itertools import count
from model import Net, make_features, infer, train

from dragon.native.process import Process, ProcessTemplate, Popen
from dragon.native.process_group import ProcessGroup
from dragon.infrastructure.connection import Connection
from dragon.native.machine import System


def parse_results(stdout_conn: Connection) -> tuple:
    """Read stdout from the Dragon connection.

    :param stdout_conn: Dragon connection to rank 0's stdout
    :type stdout_conn: Connection
    :return: tuple with a list of x values and the corresponding sin(x) values.
    :rtype: tuple
    """
    x = []
    y = []
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
            x_val = float(line.split(",")[0])
            y_val = float(line.split(",")[1])
            x.append(x_val)
            y.append(y_val)
        except (IndexError, ValueError):
            pass

    return x, y


def generate_data(
    num_ranks: int, samples_per_rank: int, sample_range: list, number_of_times_trained: int
) -> tuple:
    """Launches mpi application that generates (x, sin(x)) pairs uniformly sampled from [sample_range[0], sample_range[1]).

    :param num_ranks: number of ranks to use to generate data
    :type num_ranks: int
    :param samples_per_rank: number of samples to generate per rank
    :type samples_per_rank: int
    :param sample_range: range from which to sample training data
    :type sample_range: list
    :param number_of_times_trained: number of times trained. can be used to set a seed for the mpi application.
    :type number_of_times_trained: int
    :return: tuple of PyTorch tensors containing data and targets respectively
    :rtype: tuple
    """
    """Launch process group and parse data"""
    exe = os.path.join(os.getcwd(), "sim-expensive")
    args = [str(samples_per_rank), str(sample_range[0]), str(sample_range[1]), str(number_of_times_trained)]
    run_dir = os.getcwd()

    grp = ProcessGroup(restart=False, pmi_enabled=True)

    # Pipe the stdout output from the head process to a Dragon connection
    grp.add_process(nproc=1, template=ProcessTemplate(target=exe, args=args, cwd=run_dir, stdout=Popen.PIPE))

    # All other ranks should have their output go to DEVNULL
    grp.add_process(
        nproc=num_ranks - 1,
        template=ProcessTemplate(target=exe, args=args, cwd=run_dir, stdout=Popen.DEVNULL),
    )
    # start the process group
    grp.init()
    grp.start()
    group_procs = [Process(None, ident=puid) for puid in grp.puids]
    for proc in group_procs:
        if proc.stdout_conn:
            # get info printed to stdout from rank 0
            x, y = parse_results(proc.stdout_conn)
    # wait for workers to finish and shutdown process group
    grp.join()
    grp.stop()
    grp.close()
    # transform data into tensors for training
    data = torch.tensor(x)
    target = torch.tensor(y)
    return data, target.unsqueeze(1)


def compute_cheap_approx(num_ranks: int, x: float) -> float:
    """Launch process group with cheap approximation and parse output to float as a string

    :param num_ranks: number of mpi ranks (and therefor terms) to use for the cheap approximation
    :type num_ranks: int
    :param x: point where you are trying to compute sin(x)
    :type x: float
    :return: taylor expansion of sin(x)
    :rtype: float
    """
    exe = os.path.join(os.getcwd(), "sim-cheap")
    args = [str(x)]
    run_dir = os.getcwd()

    grp = ProcessGroup(restart=False, pmi_enabled=True)

    # Pipe the stdout output from the head process to a Dragon connection
    grp.add_process(nproc=1, template=ProcessTemplate(target=exe, args=args, cwd=run_dir, stdout=Popen.PIPE))

    # All other ranks should have their output go to DEVNULL
    grp.add_process(
        nproc=num_ranks - 1,
        template=ProcessTemplate(target=exe, args=args, cwd=run_dir, stdout=Popen.DEVNULL),
    )
    # start the process group
    grp.init()
    grp.start()
    group_procs = [Process(None, ident=puid) for puid in grp.puids]
    for proc in group_procs:
        # get info printed to stdout from rank 0
        if proc.stdout_conn:
            _, y = parse_results(proc.stdout_conn)
    # wait for workers to finish and shutdown process group
    grp.join()
    grp.stop()
    grp.close()

    return y


def infer_and_compare(model: torch.nn, x: float) -> tuple:
    """Launch inference and cheap approximation and check the difference between them

    :param model: PyTorch model that approximates sin(x)
    :type model: torch.nn
    :param x: value where we want to evaluate sin(x)
    :type x: float
    :return: the model's output val and the difference between it and the cheap approximation value
    :rtype: tuple
    """
    with torch.no_grad():
        # queues to send data to and from inference process
        q_in = mp.Queue()
        q_out = mp.Queue()
        q_in.put((model, x))
        inf_proc = mp.Process(target=infer, args=(q_in, q_out))
        inf_proc.start()
        # launch mpi application to compute cheap approximation
        te_fx = compute_cheap_approx(4, x.numpy()[0])
        inf_proc.join()
        model_val = q_out.get()
        # compare cheap approximation and model value
        diff = abs(model_val.numpy() - te_fx[0])

    return model_val, diff


def main():

    ranks_per_node = 8
    data_interval = [-math.pi, math.pi]
    samples_per_rank = 32
    my_alloc = System()
    # Define model
    model = Net()
    # Define optimizer
    optimizer = torch.optim.Adam(model.parameters(), lr=0.01)
    # Load pretrained model
    PATH = "model_pretrained_poly.pt"
    checkpoint = torch.load(PATH)
    model.load_state_dict(checkpoint["model_state_dict"])
    optimizer.load_state_dict(checkpoint["optimizer_state_dict"])

    number_of_times_trained = 0
    successes = 0

    generate_new_x = True

    while successes < 5:

        if generate_new_x:
            # uniformly sample from [-pi, pi)
            x = torch.rand(1) * (2 * math.pi) - math.pi

        model_val, diff = infer_and_compare(model, x)
        if diff > 0.05:
            print(f"training", flush=True)
            # want to train and then retry same value
            generate_new_x = False
            number_of_times_trained += 1
            # interval we uniformly sample training data from
            # launch mpi job to generate data
            data, target = generate_data(
                my_alloc.nnodes * ranks_per_node, samples_per_rank, data_interval, number_of_times_trained
            )
            # train model
            loss = train(model, optimizer, data, target)
        else:
            successes += 1
            generate_new_x = True
            print(f" approx = {model_val}, exact = {math.sin(x)}", flush=True)


if __name__ == "__main__":
    mp.set_start_method("dragon")
    main()
