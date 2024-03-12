# Dragon AI API Examples

The purpose of the example here is to show the usage of DragonTorch. This example provides an idea of how to use the PyTorch dataset stored with a Dragon distributed dictionary. Over multiple iterations, the processes interact with the same distributed dictionary. 

The correct version of PyTorch CUDA 11.8 is installed with this command.

```
pip3 install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu118
```

## Example to demonstrate PyTorch Dataset Usage with Dragon Distributed Dictionary

This example shows how a PyTorch dataset can use a Dragon distributed dictionary to store the data. In principle, the distributed dictionary could be shared among other processes that might interact with the training data between training iterations. The PyTorch Dataloader is used to iterate over the MNIST data. 

We run as follows:
```
dict_torch_dataset.py [-h] [--mnist-workers MNIST_WORKERS]
                             [--devices-per-node DEVICES_PER_NODE] [--no-cuda]
                             [--epochs N]
                             [--dragon-dict-managers DRAGON_DICT_MANAGERS]
                             [--dragon-dict-mem DRAGON_DICT_MEM]
```

#### Optional arguments:
```
  -h, --help            show this help message and exit
  --mnist-workers MNIST_WORKERS
                        number of mnist workers (default: 2)
  --devices-per-node DEVICES_PER_NODE
                        number of devices per node (default: 1)
  --no-cuda             disables CUDA training
  --epochs N            number of epochs to train (default: 5)
  --dragon-dict-managers DRAGON_DICT_MANAGERS
                        number of dragon dictionary managers per node
  --dragon-dict-mem DRAGON_DICT_MEM
```

The PyTorch dataset in the distributed Dragon dictionary in this example works on a multi-node setup.