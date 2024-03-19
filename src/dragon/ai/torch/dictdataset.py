from dragon.data.distdictionary.dragon_dict import DragonDict
from torch.utils.data import Dataset
from collections.abc import Iterable
from typing import Any
label=Any 
data=Any
DataPair = tuple[data, label]
import warnings

class DragonDataset(Dataset):
    """
    This is a PyTorch dataset that utilizes the dragon distributed dictionary to store the training data and labels. It takes either an iterable for the data or an existing dragon distributed dictionary with a list of its keys. The PyTorch Dataloader requires three functions to be supported: `__getitem__`, `__len__`, and `__init__`. For use with an arbitrary iterable, a `stop()` function is also provided that closes the dragon distributed dictionary. If the user provides a dictionary, the user is expected to manage the dictionary and close it directly.  

    :param Dataset: Base PyTorch Dataset 
    :type Dataset: PyTorch Dataset 
    """
    def __init__(self, dataset: Iterable[DataPair] or DragonDict, *, dataset_keys=None, dragon_dict_args=None):
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
        """ Gets a data and label pair from the distributed dictionary based on an idx in [0, len(self.dict)). It retrieves the key self.keys[idx]. 

        :param idx: A randomly generated index to the list of keys 
        :type idx: int 
        :return: Tuple of the data and label with key self.keys[idx] 
        :rtype: tuple 
        """
        data, label = self.dict[self.keys[idx]]
        return data, label

    def _build_dict(self, dataset: Iterable[DataPair], dragon_dict_args):
        data_dict = DragonDict(dragon_dict_args.managers_per_node, dragon_dict_args.n_nodes, dragon_dict_args.total_mem)

        # iterate through the dataset and put each sample in the dictionary
        # this can be done with multiple workers if the dataset is large
        keys= [0]*len(dataset)
        for i, (data, label) in enumerate(dataset):
            keys[i] = i
            data_dict[i] = (data, label) 
        
        return data_dict, keys
    
    def stop(self):
        if self._manage_dict:
            self.dict.stop()
        else:
            warnings.warn('Dragon dataset dictionary is user-managed. DragonDataset.stop() was ignored.') 
