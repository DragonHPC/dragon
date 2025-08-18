"""Example of how a Dragon DDict can be used as a backend to a PyTorch Dataset."""

from dragon.data import DDict
from torch.utils.data import Dataset
from collections.abc import Iterable
from typing import Any

label = Any
data = Any
DataPair = tuple[data, label]
import warnings


class DragonDataset(Dataset):
    """
    This is a PyTorch dataset that utilizes the dragon distributed dictionary to store the training data and labels.
    It takes either an iterable for the data or an existing dragon distributed dictionary with a list of its keys.
    The PyTorch Dataloader requires three functions to be supported: `__getitem__`, `__len__`, and `__init__`. For use
    with an arbitrary iterable, a `stop()` function is also provided that closes the dragon distributed dictionary. If
    the user provides a dictionary, the user is expected to manage the dictionary and close it directly.

    Example usage:

    .. highlight:: python
    .. code-block:: python

        import dragon
        import torch
        from dragon.data import DDict
        from dragon.native.process import Process

        def train(dataset):
            train_kwargs = {"batch_size": 64}
            if torch.cuda.is_available():
                cuda_kwargs = {
                    "num_workers": 0,
                    "pin_memory": True,
                    "shuffle": True,
                }
                train_kwargs.update(cuda_kwargs)

            train_loader = torch.utils.data.DataLoader(dataset, **train_kwargs)

            for epoch in range(1, 10):
                train(model, device, train_loader, optimizer, epoch)
                scheduler.step()

        if __name__ == "__main__":
            mp.set_start_method("dragon")

            d = DDict(2, 1, 1 * 1024 * 1024 * 1024)
            # fill ddict with data

            dragon_dataset = DragonDataset(d, dataset_keys=d.keys())

            # this process may be on any node in the allocation
            proc = Process(target=train, args=(dragon_dataset,))
            proc.start()
            proc.join()
            d.destroy()
    """

    def __init__(self, dataset: Iterable[DataPair] or DDict, *, dataset_keys=None, dragon_dict_args=None):
        """Construct a Dataset from a :py:class:`~dragon.data.DDict` usable by a PyTorch `DataLoader`.

        :param dataset: Base PyTorch Dataset
        :type dataset: PyTorch Dataset or :py:class:`~dragon.data.DDict`
        :param dataset_keys: All keys in the dataset (e.g., from :py:method:`~dragon.data.DDict.keys`)
        :type dataset_keys: list or iterable
        :param dragon_dict_args: Optional arguments to construct a new :py:class:`~dragon.data.DDict`
        :type dragon_dict_args: dict
        """
        if dragon_dict_args is None and dataset_keys is not None:
            # dictionary is managed elsewhere
            self._manage_dict = False
            self.dict = dataset
            self.keys = dataset_keys
        else:
            # dataset manages dictionary
            self._manage_dict = True
            self.dict, self.keys = self._build_dict(dataset, dragon_dict_args)

    def __len__(self):
        return len(self.dict)

    def __getitem__(self, idx):
        """Gets a data and label pair from the distributed dictionary based on an idx in [0, len(self.dict)). It retrieves the key self.keys[idx].

        :param idx: A randomly generated index to the list of keys
        :type idx: int
        :return: Tuple of the data and label with key self.keys[idx]
        :rtype: tuple
        """
        data = self.dict[self.keys[idx]]
        return data

    def _build_dict(self, dataset: Iterable[DataPair], dragon_dict_args):
        data_dict = DDict(dragon_dict_args.managers_per_node, dragon_dict_args.n_nodes, dragon_dict_args.total_mem)

        # iterate through the dataset and put each sample in the dictionary
        # this can be done with multiple workers if the dataset is large

        # build with or without labels
        for i, data in enumerate(dataset):
            data_dict[i] = data
        keys = list(range(i + 1))

        return data_dict, keys

    def stop(self):
        """Bring down the Dataset and clean up all resources"""
        if self._manage_dict:
            self.dict.destroy()
        else:
            warnings.warn("Dragon dataset dictionary is user-managed. DragonDataset.stop() was ignored.")
