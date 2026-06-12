import dragon
import getpass
import os
import argparse
from typing import Tuple

from dragon.data.ddict import DDict, PosixCheckpointPersister, DAOSCheckpointPersister
from dragon.native.machine import System

import torch
import torch.nn as nn
import torch.optim as optim
from torch.optim import Optimizer
from torch.utils.data import DataLoader, TensorDataset
from example_model import SimpleNN


def prepare_validate(args, persister) -> Tuple[DDict, DataLoader, SimpleNN, Optimizer]:
    # generate random data
    X_train = torch.randn(num_samples, input_size)
    y_train = torch.randn(num_samples, output_size)
    train_dataset = TensorDataset(X_train, y_train)
    # save data loader for later use, do not shuffle data in this example as we want to compare the loss
    train_loader = DataLoader(train_dataset, batch_size=batch_size, shuffle=False)
    torch.save(train_loader, "train_loader.pth")
    # instantiate a model and save the initial states of both model and optimizer
    model = SimpleNN(input_size, hidden_size, output_size)
    torch.save(model.state_dict(), "initial_model.pth")
    optimizer = optim.Adam(model.parameters(), lr=learning_rate)

    ddict = DDict(
        args.managers_per_node,
        nnodes,
        nnodes * int(4 * 1024 * 1024 * 1024),
        wait_for_keys=True,
        working_set_size=args.working_set_size,
    )

    return ddict, train_loader, model, optimizer


def prepare_persist(args, persister) -> Tuple[DDict, DataLoader, SimpleNN, Optimizer]:
    ddict = DDict(
        args.managers_per_node,
        nnodes,
        nnodes * int(4 * 1024 * 1024 * 1024),
        wait_for_keys=True,
        working_set_size=args.working_set_size,
        persister_class=persister,
        persist_freq=args.persist_frequency,
        persist_count=args.persist_count,
        persist_path=args.persist_path,
        name=name,
    )

    train_loader = torch.load("train_loader.pth", weights_only=False)
    ddict.pput("train_loader", train_loader)
    # load initial model weights
    model = SimpleNN(input_size, hidden_size, output_size)
    model.load_state_dict(torch.load("initial_model.pth"))
    optimizer = optim.Adam(model.parameters(), lr=learning_rate)

    return ddict, train_loader, model, optimizer


def prepare_restore(args, persister) -> Tuple[DDict, DataLoader, SimpleNN, Optimizer]:

    ddict = DDict(
        args.managers_per_node,
        nnodes,
        nnodes * int(4 * 1024 * 1024 * 1024),
        wait_for_keys=True,
        working_set_size=args.working_set_size,
        persister_class=persister,
        persist_freq=args.persist_frequency,
        persist_count=args.persist_count,
        persist_path=args.persist_path,
        name=name,
    )

    available_persisted_chkpt = ddict.persisted_ids()
    print(f"available persisted checkpoints: {available_persisted_chkpt}")

    # restore from the last chkpt saved during persist mode
    latest_chkpt = available_persisted_chkpt[-1]
    ddict.restore(latest_chkpt)

    # retrieve the same data loader as the one that used during persist mode
    train_loader = ddict["train_loader"]

    model = SimpleNN(input_size, hidden_size, output_size)
    # load the model from the last training iteration
    model.load_state_dict(ddict["model_state_dict"])

    # load the optimizer from the last training iteration
    optimizer = optim.Adam(model.parameters(), lr=learning_rate)
    optimizer.load_state_dict(ddict["optimizer_state_dict"])

    ddict.checkpoint()  # proceed to the next checkpoint

    return ddict, train_loader, model, optimizer


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Neural Network Training Using DDict Checkpoint Persistence")
    parser.add_argument(
        "--mode",
        nargs="?",
        const="validate",
        default="validate",
        choices=["validate", "persist", "restore"],
        help="The training mode (default: %(default)s)",
    )

    parser.add_argument(
        "--managers_per_node",
        type=int,
        default=1,
        help="number of managers per node for the dragon dict",
    )
    parser.add_argument(
        "--working_set_size",
        type=int,
        default=2,
        help="number of checkpoints in the working set in the dragon dict",
    )
    parser.add_argument(
        "--persister",
        nargs="?",
        const="POSIX",
        default="POSIX",
        choices=["POSIX", "DAOS"],
        help="The type of the checkpoint persister for dragon dict (default: %(default)s)",
    )
    parser.add_argument(
        "--persist_path",
        type=str,
        default="",
        help="The directory to save the persisted checkpoints for POSIX persister. \
            For DAOS persister, it is the name of the DAOS pool in which the persisted checkpoints will be save.",
    )
    parser.add_argument(
        "--persist_count",
        type=int,
        default=-1,
        help="The maximum number of persisted checkpoints under the given persist path. Unlimited number of files if -1 is provided.",
    )
    parser.add_argument("--persist_frequency", type=int, default=1, help="The frequnecy of persisting a checkpoint.")

    args = parser.parse_args()

    my_alloc = System()
    nnodes = my_alloc.nnodes
    name = f"chkpt_persistence_example_{getpass.getuser()}"
    if args.persister == "POSIX":
        persister = PosixCheckpointPersister
    else:
        persister = DAOSCheckpointPersister

    # Define the training parameters
    input_size = 10
    hidden_size = 20
    output_size = 1
    num_samples = 100
    batch_size = 10
    learning_rate = 0.01
    num_epochs = 20

    criterion = nn.MSELoss()

    if args.mode == "validate":
        """
        Runs a complete training session without checkpoint persistence.
        """
        num_epochs = 38
        ddict, train_loader, model, optimizer = prepare_validate(args, persister)
        first_chkpt = 0
        last_chkpt = num_epochs

    elif args.mode == "persist":
        """
        Performs the first half of the training, and write the updated model weights at each iteration to
        checkpoints and persists them with the given persist frequency.
        """
        ddict, train_loader, model, optimizer = prepare_persist(args, persister)
        first_chkpt = 0
        last_chkpt = num_epochs

    else:
        """
        Restores and resumes from the last persisted checkpoint saved during the persist mode. It finishes
        the second half of the training.
        """
        ddict, train_loader, model, optimizer = prepare_restore(args, persister)
        first_chkpt = ddict.checkpoint_id
        last_chkpt = first_chkpt + num_epochs


    # training
    for epoch in range(num_epochs):
        for i, (inputs, labels) in enumerate(train_loader):
            outputs = model(inputs)
            loss = criterion(outputs, labels)
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()

        ddict["model_state_dict"] = model.state_dict()
        ddict["optimizer_state_dict"] = optimizer.state_dict()
        ddict.checkpoint()

        print(f"Epoch [{first_chkpt + epoch + 1}/{last_chkpt}], Loss: {loss.item():.4f}")

    print("Training finished!")
    ddict.destroy()
