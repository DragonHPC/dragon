"""This example shows how a PyTorch dataset can use a Dragon distributed dictionary to store the data. In principle, the distributed dictionary could be shared among other processes that might interact with the training data between training iterations.    
"""
import dragon
import multiprocessing as mp
from dragon.globalservices.node import get_list, query
import argparse
import functools
import os
import math
import queue
import dragon.ai.torch
from dragon.ai.torch.dictdataset import DragonDataset
import torch
import torch.multiprocessing as torch_mp
from torchvision import datasets, transforms
import torch.nn as nn
import torch.nn.functional as F
import torch.optim as optim
from torchvision import datasets, transforms
from torch.optim.lr_scheduler import StepLR


def get_args():
    """Get the user provided arguments
    :return args: input args from command line
    :rtype args: ArgumentParser object
    """
    parser = argparse.ArgumentParser(description="Multi-client MNIST test with DragonDataset")
    parser.add_argument("--mnist-workers", type=int, default=2, help="number of mnist workers (default: 2)")
    parser.add_argument(
        "--devices-per-node", type=int, default=1, help="number of devices per node (default: 1)"
    )
    parser.add_argument("--no-cuda", action="store_true", default=False, help="disables CUDA training")
    parser.add_argument(
        "--epochs", type=int, default=5, metavar="N", help="number of epochs to train (default: 5)"
    )
    parser.add_argument(
        "--dragon-dict-managers", type=int, default=2, help="number of dragon dictionary managers per node"
    )
    parser.add_argument(
        "--dragon-dict-mem",
        type=int,
        default=1024 * 1024 * 1024,
        help="total memory allocated to dragon dictionary",
    )

    my_args = parser.parse_args()
    return my_args


class DragonDictArgs(object):
    """Class for managing dragon distributed dictionary arguments."""

    def __init__(self, managers_per_node: int, n_nodes: int, total_mem: int):
        self.managers_per_node = managers_per_node
        self.n_nodes = n_nodes
        self.total_mem = total_mem


def build_device_queues(num_devices_per_node: int):
    """Builds a dictionary of device queues.

    :param num_devices_per_node: A dictionary of multiprocessing queues that hold device numbers
    :type num_devices_per_node: int
    :return: A dictionary of multiprocessing queues that hold device numbers
    :rtype: dict[mp.queues.Queue]
    """
    node_dict = {}
    node_list = get_list()
    for node in node_list:
        node_dict[node] = mp.Queue()
    for node in node_list:
        for device in range(num_devices_per_node):
            node_dict[node].put(device)
    return node_dict


def get_huid():
    """Gets huid for a worker's node.

    :return: returns h_uid
    :rtype: int
    """
    name = os.uname().nodename
    desc = query(str(name))
    return desc.h_uid


def get_device(device_queue):
    """Grabs an unoccupied device from the nodes unique queue if devices are available. Otherwise it returns the cpu as the available device.


    :param device_queue: A dictionary of multiprocessing queues that hold device numbers
    :type device_queue: dict[mp.queues.Queue]
    :return: This processes device
    :rtype: PyTorch device
    """
    huid = get_huid()
    try:
        available_cuda_device = device_queue[huid].get(timeout=10)
        gpu_available = True
    except queue.Empty:
        gpu_available = False

    if torch.cuda.is_available() and gpu_available:
        device = torch.device("cuda", available_cuda_device)
    else:
        # if we don't have a device that is free, we use the cpu
        device = torch.device("cpu")
    return device


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


def train(model, device, train_loader, optimizer, rank, epoch):
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
    :param rank: Global rank of this process
    :type rank: int
    :param epoch: Current epoch
    :type epoch: int
    """

    model.train()
    for batch_idx, (data, target) in enumerate(train_loader):
        data, target = data.to(device), target.to(device)
        optimizer.zero_grad()
        output = model(data)
        loss = F.nll_loss(output, target)
        loss.backward()
        optimizer.step()
        if batch_idx % 100 == 0 or False:
            print(
                "Rank {} Train Epoch: {} [{}/{} ({:.0f}%)]\tLoss: {:.6f}".format(
                    rank,
                    epoch,
                    batch_idx * len(data),
                    len(train_loader.dataset),
                    100.0 * batch_idx / len(train_loader),
                    loss.item(),
                ),
                flush=True,
            )


def mnist_lr_sweep(args, device_queue, dataset_train, lr_p_list, global_rank):
    """This trains on the MNIST dataset with variable learning rate
    and returns the final loss and accuracy. Each process gets a gpu
    that is determined by first getting a unique node identifier and
    then using that to access the queue of available devices.

    :param args: input args from command line
    :type args: ArgumentParser object
    :param device_queue: a dictionary of multiprocessing queues that hold device numbers
    :type device_queue: dict[mp.queues.Queue]
    :param dataset_train: the training dataset
    :type dataset_train: PyTorch Dataset
    :param lr_p_list: list of learning rates
    :type lr_p_list: list of floats
    :param global_rank: Global rank of this process
    :type global_rank: int

    """
    torch_mp.set_start_method("dragon", force=True)
    use_cuda = not args.no_cuda and torch.cuda.is_available()
    # grabs an unoccupied device from the nodes unique queue
    lr_p = lr_p_list[global_rank]
    device = get_device(device_queue)
    seed = math.floor(4099 * lr_p)
    torch.manual_seed(seed)

    train_kwargs = {"batch_size": 64}
    if use_cuda:
        cuda_kwargs = {
            "num_workers": 4,
            "pin_memory": True,
            "shuffle": True,
            "multiprocessing_context": "dragon",
            "persistent_workers": True,
        }
        train_kwargs.update(cuda_kwargs)

    train_loader = torch.utils.data.DataLoader(dataset_train, **train_kwargs)

    model = Net().to(device)
    optimizer = optim.Adadelta(model.parameters(), lr=lr_p)
    scheduler = StepLR(optimizer, step_size=1, gamma=0.7)

    for epoch in range(1, args.epochs + 1):
        train(model, device, train_loader, optimizer, global_rank, epoch)
        scheduler.step()


if __name__ == "__main__":
    args = get_args()
    mp.set_start_method("dragon")

    # get the list of nodes from Global Services
    nodeslist = get_list()
    nnodes = len(nodeslist)

    num_mnist_workers = args.mnist_workers
    assert num_mnist_workers > 1
    print(f"Number of nodes: {nnodes}", flush=True)
    print(f"Number of MNIST workers: {num_mnist_workers}", flush=True)
    print(f"Number of dragon dict managers: {args.dragon_dict_managers*nnodes}", flush=True)

    transform = transforms.Compose([transforms.ToTensor(), transforms.Normalize((0.1307,), (0.3081,))])
    dataset_base_path = "./torch-data-dict/data"
    dataset = datasets.MNIST(dataset_base_path, train=True, download=True, transform=transform)

    dragon_dict_args = DragonDictArgs(args.dragon_dict_managers, nnodes, args.dragon_dict_mem)
    dragon_dataset = DragonDataset(dataset, dragon_dict_args=dragon_dict_args)

    device_queue = build_device_queues(args.devices_per_node)
    lr_list = [1 / (num_mnist_workers - 1) * i + 0.5 for i in range(num_mnist_workers)]
    mnist_lr_sweep_partial = functools.partial(mnist_lr_sweep, args, device_queue, dragon_dataset, lr_list)
    mnist_pool = mp.Pool(num_mnist_workers)

    # launch scipy and mnist jobs
    results = mnist_pool.map(mnist_lr_sweep_partial, [idx for idx in range(num_mnist_workers)], 1)

    mnist_pool.close()
    mnist_pool.join()
