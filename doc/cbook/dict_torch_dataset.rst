PyTorch Dataset Usage with Dragon Distributed Dictionary
++++++++++++++++++++++++++++++++++++++++++++++++++++++++

This example shows how a PyTorch dataset can use a Dragon distributed dictionary to store the data. 
In principle, the distributed dictionary could be shared among other processes that might interact with the training data between training iterations. 
The program must be run with GPUs. 

The code demonstrates how the following key concepts work with Dragon:

* How to utilize Dragon and the PyTorch dataloader and neural network model for training on GPUs
* How to use the distributed Dragon dictionary with multiprocessing queues

.. literalinclude:: ../../examples/dragon_ai/dict_torch_dataset.py

Installation
============

After installing dragon, the only other dependency is on PyTorch. The PyTorch version and corresponding pip command can be found here (https://pytorch.org/get-started/locally/). 

```
> pip3 install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu118
```

Description of the system used
==============================

For this example, an HPE Cray EX was used. Each node has AMD EPYC 7763 64-core CPUs and 4x Nvidia A100 GPUs.

How to run
==========

Example Output when run on 2 nodes with 2 MNIST workers, 1 device per node, 2 epochs, CUDA training, 4 dragon dict managers, and dragon dict memory.
-------------------------------------------------------------------------------------

.. code-block:: console
    :linenos:

    > salloc --nodes=2 -p allgriz --exclusive -t 1:00:00
    > dragon dict_torch_dataset.py --mnist-workers 4 --devices-per-node 1 --epochs 2
    Number of nodes: 2
    Number of MNIST workers: 2
    Number of dragon dict managers: 4
    100.0%
    100.0%
    100.0%
    100.0%
    Rank 0 Train Epoch: 1 [0/60000 (0%)]    Loss: 2.316082
    Rank 1 Train Epoch: 1 [0/60000 (0%)]    Loss: 2.313832
    Rank 0 Train Epoch: 1 [6400/60000 (11%)]        Loss: 0.268168
    Rank 1 Train Epoch: 1 [6400/60000 (11%)]        Loss: 0.436355
    Rank 0 Train Epoch: 1 [12800/60000 (21%)]       Loss: 0.190972
    Rank 1 Train Epoch: 1 [12800/60000 (21%)]       Loss: 0.205474
    Rank 0 Train Epoch: 1 [19200/60000 (32%)]       Loss: 0.187326
    Rank 1 Train Epoch: 1 [19200/60000 (32%)]       Loss: 0.568415
    Rank 0 Train Epoch: 1 [25600/60000 (43%)]       Loss: 0.093499
    Rank 1 Train Epoch: 1 [25600/60000 (43%)]       Loss: 0.058430
    Rank 0 Train Epoch: 1 [32000/60000 (53%)]       Loss: 0.060121
    Rank 1 Train Epoch: 1 [32000/60000 (53%)]       Loss: 0.149605
    Rank 0 Train Epoch: 1 [38400/60000 (64%)]       Loss: 0.156384
    Rank 1 Train Epoch: 1 [38400/60000 (64%)]       Loss: 0.119814
    Rank 0 Train Epoch: 1 [44800/60000 (75%)]       Loss: 0.082197
    Rank 1 Train Epoch: 1 [44800/60000 (75%)]       Loss: 0.096987
    Rank 0 Train Epoch: 1 [51200/60000 (85%)]       Loss: 0.053689
    Rank 1 Train Epoch: 1 [51200/60000 (85%)]       Loss: 0.101078
    Rank 0 Train Epoch: 1 [57600/60000 (96%)]       Loss: 0.031515
    Rank 1 Train Epoch: 1 [57600/60000 (96%)]       Loss: 0.090198
    Downloading http://yann.lecun.com/exdb/mnist/train-images-idx3-ubyte.gz
    Downloading http://yann.lecun.com/exdb/mnist/train-images-idx3-ubyte.gz to ./torch-data-dict/data/MNIST/raw/train-images-idx3-ubyte.gz
    Extracting ./torch-data-dict/data/MNIST/raw/train-images-idx3-ubyte.gz to ./torch-data-dict/data/MNIST/raw

    Downloading http://yann.lecun.com/exdb/mnist/train-labels-idx1-ubyte.gz
    Downloading http://yann.lecun.com/exdb/mnist/train-labels-idx1-ubyte.gz to ./torch-data-dict/data/MNIST/raw/train-labels-idx1-ubyte.gz
    Extracting ./torch-data-dict/data/MNIST/raw/train-labels-idx1-ubyte.gz to ./torch-data-dict/data/MNIST/raw

    Downloading http://yann.lecun.com/exdb/mnist/t10k-images-idx3-ubyte.gz
    Downloading http://yann.lecun.com/exdb/mnist/t10k-images-idx3-ubyte.gz to ./torch-data-dict/data/MNIST/raw/t10k-images-idx3-ubyte.gz
    Extracting ./torch-data-dict/data/MNIST/raw/t10k-images-idx3-ubyte.gz to ./torch-data-dict/data/MNIST/raw

    Downloading http://yann.lecun.com/exdb/mnist/t10k-labels-idx1-ubyte.gz
    Downloading http://yann.lecun.com/exdb/mnist/t10k-labels-idx1-ubyte.gz to ./torch-data-dict/data/MNIST/raw/t10k-labels-idx1-ubyte.gz
    Extracting ./torch-data-dict/data/MNIST/raw/t10k-labels-idx1-ubyte.gz to ./torch-data-dict/data/MNIST/raw


