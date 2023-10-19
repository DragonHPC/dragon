import dragon
import multiprocessing as mp
import torch
import torch.nn.functional as F
import torch.nn as nn

# basis functions
from scipy.special import eval_chebyt as cheby
from scipy.special import eval_legendre as legendre
from scipy.special import eval_hermite as hermite


def poly(n: int, x: float) -> float:
    """polynomial basis functions

    :param n: polynomial degree
    :type n: int
    :param x: input value
    :type x: float
    :return: x^n
    :rtype: float
    """
    return x**n


BASIS_DEGREE = 6


def make_features(x: float, basis: callable = poly) -> torch.tensor:
    """builds features for a given set of basis functions

    :param x: input values
    :type x: float
    :param basis: basis for interpolation, defaults to poly
    :type basis: callable, optional
    :return: basis functions evaluated at x
    :rtype: torch.tensor
    """
    x = x.unsqueeze(1)
    features = []
    for i in range(0, BASIS_DEGREE):
        output = torch.tensor(basis(i, x.numpy()))
        features.append(output)
    torch_features = torch.cat(features, 1).to(dtype=torch.float32)
    return torch_features


def f(x: float) -> float:
    """Approximated function.

    :param x: value to compute at
    :type x: float
    :return: sin(x)
    :rtype: float
    """
    return torch.sin(x)


class Net(nn.Module):
    """Single fully connected layer"""

    def __init__(self):
        super(Net, self).__init__()
        self.fc = torch.nn.Linear(BASIS_DEGREE, 1)

    def forward(self, x: torch.tensor) -> float:
        output = self.fc(x)
        return output


def infer(q_in: mp.Queue, q_out: mp.Queue):
    """does inference for the model and input data put into the q_in and returns results in the q_out

    :param q_in: queue that has the model and input data
    :type q_in: multiprocessing.Queue
    :param q_out: q where we put the infered value
    :type q_out: multiprocessing.Queue
    """
    model, data = q_in.get()
    model.eval()
    # Get data
    data = make_features(data)

    # Forward pass
    output = model(data)

    output = torch.squeeze(output)
    q_out.put(output)


def train(model: torch.nn, optimizer: torch.optim, data: torch.tensor, target: torch.tensor) -> float:
    """trains the model on generated data

    :param model: model to train
    :type model: torch.nn
    :param optimizer: optimizer to use for training
    :type optimizer: torch.optim
    :param data: input data
    :type data: torch.tensor
    :param target: target for input data
    :type target: torch.tensor
    :return: training computed loss
    :rtype: float
    """
    model.train()
    # get data into dataloader format
    data = make_features(data)
    dataset = torch.utils.data.TensorDataset(data, target)
    dataloader = torch.utils.data.DataLoader(dataset, batch_size=32, shuffle=True)

    for _, batch in enumerate(dataloader):
        batch_data, batch_target = batch
        # Reset gradients
        optimizer.zero_grad()

        # Forward pass
        output = model(batch_data)
        loss = F.smooth_l1_loss(output, batch_target)

        # Backward pass
        loss.backward()
        # Apply gradients
        optimizer.step()

    return loss
