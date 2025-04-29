"""This contains the functions used to run the mnist jobs and utilizes dragon queues to orchestrate GPU placement.
"""

import dragon
from dragon.globalservices.node import get_list, query_total_cpus, query
import argparse
import torch
import torch.multiprocessing as mp
import torch.nn as nn
import torch.nn.functional as F
import torch.optim as optim
from torchvision import datasets, transforms
from torch.optim.lr_scheduler import StepLR
import random
import os
import socket
import time
import telem as tm


class Net(nn.Module):
    """Convolutional neural network (two convolutional layers and two fully connected layers)
    based on the PyTorch neural network module. The RelU activation function adds nonlinearity
    and the max pool reduces the noise. The dropouts help reduce overfitting.
    """

    def __init__(self):
        super(Net, self).__init__()
        self.conv1 = nn.Conv2d(1, 32, 3, 1)
        self.conv2 = nn.Conv2d(32, 64, 3, 1)
        self.dropout1 = nn.Dropout(0.25)
        self.dropout2 = nn.Dropout(0.5)
        self.fc1 = nn.Linear(9216, 128)
        self.fc2 = nn.Linear(128, 10)

    def forward(self, x):
        """Defines the computation done for a forward pass

        :param x: Input grayscaled image passed to the network
        :type x: torch.Tensor
        :return: Prediction
        :rtype: torch.Tensor
        """
        x = self.conv1(x)
        x = F.relu(x)
        x = self.conv2(x)
        x = F.relu(x)
        x = F.max_pool2d(x, 2)
        x = self.dropout1(x)
        x = torch.flatten(x, 1)
        x = self.fc1(x)
        x = F.relu(x)
        x = self.dropout2(x)
        x = self.fc2(x)
        output = F.log_softmax(x, dim=1)
        return output


def train(model, device, train_loader, optimizer):
    """Trains the model on the specified device. It utilizes a
    PyTorch dataloader to iterate over the data

    :param model: Neural network model that defines layers and data flow
    :type model: mnist.Net
    :param device: PyTorch device to use for training
    :type device: torch.device
    :param train_loader: PyTorch data loader for training dataset
    :type train_loader: torch.utils.data.dataloader.DataLoader
    :param optimizer: PyTorch optimizer used to update model parameters
    :type optimizer: torch.optim
    """
    model.train()
    for batch_idx, (data, target) in enumerate(train_loader):
        data, target = data.to(device), target.to(device)
        optimizer.zero_grad()
        output = model(data)
        loss = F.nll_loss(output, target)
        loss.backward()
        optimizer.step()


def test(model, device, test_loader):
    """Computes the accuracy and loss on the test data set

    :param model: Neural network model that defines layers and data flow
    :type model: mnist.Net
    :param device: PyTorch device to use for training
    :type device: torch.Device
    :param test_loader: PyTorch data loader for test dataset
    :type test_loader: torch.utils.data.dataloader.DataLoader
    :return: Test loass and test accuracy for current model parameters
    :rtype: tuple
    """
    model.eval()
    test_loss = 0
    correct = 0
    with torch.no_grad():
        for data, target in test_loader:
            data, target = data.to(device), target.to(device)
            output = model(data)
            test_loss += F.nll_loss(output, target, reduction="sum").item()  # sum up batch loss
            pred = output.argmax(dim=1, keepdim=True)  # get the index of the max log-probability
            correct += pred.eq(target.view_as(pred)).sum().item()

    test_loss /= len(test_loader.dataset)
    test_accuracy = 100.0 * correct / len(test_loader.dataset)
    return test_loss, test_accuracy


def buildDeviceQueue():
    """This constructs the dictinonary of device queues. It relies
    upon the ability to get the number of devices on a node and that
    the nodelist has unique node IDs. It assumes that the nodes
    all have the same number of GPUs per node.

    :return deviceQueue: a dictionary of multiprocessing queues that hold device numbers
    :rtype deviceQueue: dict[mp.queues.Queue]
    """
    nodeDict = {}
    numDevices = tm.getDeviceCount()
    nodelist = get_list()
    for node in nodelist:
        nodeDict[node] = mp.Queue()
    for node in nodelist:
        for device in range(numDevices):
            nodeDict[node].put(device)
    return nodeDict


def mnist_lr_sweep(args, deviceQueue, lr_p):
    """This trains on the MNIST dataset with variable learning rate
    and returns the final loss and accuracy. Each process gets a gpu
    that is determined by first getting a unique node identifier and
    then using that to access the queue of available devices.

    :param args: input args from command line
    :type args: ArgumentParser object
    :param deviceQueue: a dictionary of multiprocessing queues that hold device numbers
    :type deviceQueue: dict[mp.queues.Queue]
    :param lr_p: learning rate
    :type lr_p: float
    :return: learning rate, final test lost, and final test accuracy
    :rtype: list of floats
    """
    use_cuda = not args.no_cuda and torch.cuda.is_available()
    name = os.uname().nodename
    desc = query(str(name))
    # grabs an unoccupied device from the nodes unique queue
    available_cuda_device = deviceQueue[desc.h_uid].get()

    torch.manual_seed(args.seed)

    if use_cuda:
        device = torch.device("cuda", available_cuda_device)
    else:
        device = torch.device("cpu")

    train_kwargs = {"batch_size": args.batch_size}
    test_kwargs = {"batch_size": args.test_batch_size}
    if use_cuda:
        cuda_kwargs = {"num_workers": 0, "pin_memory": True, "shuffle": True}
        train_kwargs.update(cuda_kwargs)
        test_kwargs.update(cuda_kwargs)

    transform = transforms.Compose([transforms.ToTensor(), transforms.Normalize((0.1307,), (0.3081,))])
    dataset1 = datasets.MNIST("../data", train=True, download=True, transform=transform)
    dataset2 = datasets.MNIST("../data", train=False, transform=transform)
    train_loader = torch.utils.data.DataLoader(dataset1, **train_kwargs)
    test_loader = torch.utils.data.DataLoader(dataset2, **test_kwargs)

    model = Net().to(device)
    optimizer = optim.Adadelta(model.parameters(), lr=lr_p)

    scheduler = StepLR(optimizer, step_size=1, gamma=args.gamma)

    for epoch in range(1, args.epochs + 1):
        train(model, device, train_loader, optimizer)
        test_loss, test_accuracy = test(model, device, test_loader)
        scheduler.step()

    # places device back in queue
    deviceQueue[desc.h_uid].put(available_cuda_device)
    return [lr_p, test_loss, test_accuracy]
