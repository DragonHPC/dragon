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
dragon dict_torch_dataset.py [-h] [--mnist-workers MNIST_WORKERS]
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

### How to Run

For this example, an HPE Cray EX was used. Each node has AMD EPYC 7763 64-core CPUs and 4x Nvidia A100 GPUs.

```
  salloc --nodes=2 -p allgriz --exclusive -t 1:00:00
  dragon dict_torch_dataset.py --mnist-workers 4 --devices-per-node 1 --epochs 2
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
```

For the dna_rna_dataloader, the example is run as such:

```
salloc --nodes=1 --exclusive -t 1:00:00
python3 generate_synthetic_data.py --size 0.125
salloc --nodes=2 -p allgriz --exclusive -t 1:00:00
dragon test_doc2vec_DM.py --file-name seq0.125gb.txt
    Started
  Namespace(file_name='seq0.125gb.txt', num_epochs=1, batch_size=4, shuffle=False, pin_memory=False, num_workers=2, use_gpu=False, seq_len=40, input_size=1000, dragon_dict_managers=2, dragon_dict_mem=17179869184)
  DDict_Timer 133.90535412499594
  [DDictManagerStats(manager_id=1, hostname='pinoak0035', total_bytes=4294967296, total_used_bytes=268173312, num_keys=32736, free_blocks={4096: 0, 8192: 0, 16384: 0, 32768: 0, 65536: 0, 131072: 0, 262144: 1, 524288: 0, 1048576: 0, 2097152: 0, 4194304: 0, 8388608: 0, 16777216: 0, 33554432: 0, 67108864: 0, 134217728: 0, 268435456: 1, 536870912: 1, 1073741824: 1, 2147483648: 1, 4294967296: 0}), DDictManagerStats(manager_id=3, hostname='pinoak0035', total_bytes=4294967296, total_used_bytes=268181504, num_keys=32737, free_blocks={4096: 0, 8192: 1, 16384: 1, 32768: 1, 65536: 1, 131072: 1, 262144: 0, 524288: 0, 1048576: 0, 2097152: 0, 4194304: 0, 8388608: 0, 16777216: 0, 33554432: 0, 67108864: 0, 134217728: 0, 268435456: 1, 536870912: 1, 1073741824: 1, 2147483648: 1, 4294967296: 0}), DDictManagerStats(manager_id=0, hostname='pinoak0034', total_bytes=4294967296, total_used_bytes=268181504, num_keys=32737, free_blocks={4096: 0, 8192: 1, 16384: 1, 32768: 1, 65536: 1, 131072: 1, 262144: 0, 524288: 0, 1048576: 0, 2097152: 0, 4194304: 0, 8388608: 0, 16777216: 0, 33554432: 0, 67108864: 0, 134217728: 0, 268435456: 1, 536870912: 1, 1073741824: 1, 2147483648: 1, 4294967296: 0}), DDictManagerStats(manager_id=2, hostname='pinoak0034', total_bytes=4294967296, total_used_bytes=268173312, num_keys=32736, free_blocks={4096: 0, 8192: 0, 16384: 0, 32768: 0, 65536: 0, 131072: 0, 262144: 1, 524288: 0, 1048576: 0, 2097152: 0, 4194304: 0, 8388608: 0, 16777216: 0, 33554432: 0, 67108864: 0, 134217728: 0, 268435456: 1, 536870912: 1, 1073741824: 1, 2147483648: 1, 4294967296: 0})]
  Building Vocabulary
  GACTU
  2
  True
  0
  130944
  0
  Time taken for epoch:
  0.0379030704498291
  Real time: 2.862032890319824
  User time: 0.048634999999990214
  System time: 2.565563
```