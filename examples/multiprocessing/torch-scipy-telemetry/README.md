## Multi-node process orchestration and node telemetry

This is an example of running ensembles of PyTorch and SciPy jobs with the Dragon runtime as well as gathering telemetry data using queues and events. It demonstrates the life-cycle management of many different processes and node monitoring using Dragon Multiprocessing.

In this example, we gather gpu utilization and the average cpu load over 1 minute. The SciPy job is similar to the one described in the Dragon documentation's Soultion Cookbook. The PyTorch job is similar to the PyTorch MNIST example (https://github.com/pytorch/examples/tree/main/mnist) and only differs in that each worker trains with a different learning rate. Currently only Nvidia GPUs are supported since we utilize the py3nvml package to gather the GPU utilization. AMD GPUs can be utilized by gathering similar info via rocm-smi directly.

The example consists of four components:
- cpu computation: image processing using the SciPy library
- gpu computation: training on the MNIST dataset
- monitor processes: we start a single process on each node. Every such process gathers telemetry data and pushes the data into a single queue that is shared among the nodes
- a post-processing process: this process gets the data from the queue, processes the data and then prints it. This process can live on any of the nodes, depending on the allocation scheme of Dragon. For now, Dragon follows a round-robin allocation over the available nodes. In the future, Dragon will provide different allocation schemes for the user to choose.

We fire up a pool of workers for the mnist computation, a different pool of workers for the SciPy computation, as many monitor processes as the number of nodes that Dragon uses (it could be a subset of the node allocation) and a single post-processing process. All the workers are distributed across the available nodes.


More information can be found at the Dragon documentation page.

### Usage

`telemetry_full.py` - This is the main file. It imports the other files and orchestrates the telemetry work. It contains telem_work, which is the function launched on every node that gathers telemetry data and pushes it to a shared queue, and post_process, which is launched only on one node and reads the telemetry data from the queue and then prints that information.

`telem.py` - This file has all the functions used to gather telemetry data on each node. It relies heavily on py3nvml to gather this data.

`mnist.py` - This contains the functions used to run the mnist jobs and utilizes dragon queues to orchestrate GPU placement.

`conv.py` - This contains all of the functions used for the SciPy convolution jobs.

It is used as follows:


```
dragon telemetry_full.py [-h] [--scipy_workers NUM_SCIPY_WORKERS] [--mnist_workers NUM_MNIST_WORKERS] [--bars]
                         [--no-cuda] [--size ARRAY_SIZE] [--mem IMAGE_MEM_SIZE] [--batch-size BATCH_SIZE]
                         [--test-batch-size TEST_BATCH_SIZE] [--epochs NUM_EPOCHS] [--gamma GAMMA]
                         [--seed SEED]
```

#### Optional arguments:

```
  -h, --help            show this help message and exit

  --scipy_workers NUM_SCIPY_WORKERS
                       number of scipy workers (default: 2)
  --mnist_workers  NUM_MNIST_WORKERS
                       number of mnist workers (default: 2)
  --bars
                       uses tqdm bars to print telemetry data
  --no-cuda
                       disables CUDA training
  --size ARRAY_SIZE
                       size of the array (default: 1024)
  --mem IMAGE_MEM_SIZE
                       overall footprint of image dataset to process (default: 1024^3)
  --batch-size BATCH_SIZE
                       input batch size for training (default: 64)
  --test-batch-size TEST_BATCH_SIZE
                       input batch size for testing (default: 1000)
  --epochs NUM_EPOCHS
                       number of epochs to train (default: 14)
  --gamma
                       Learning rate step gamma (default: 0.7)
  --seed
                       random seed (default: 1)
```

### Installation

After installing dragon, the remaining packages needed to install are located in the requirements.txt file. The version of PyTorch and it's dependencies may need to be made to run on other systems.
```
> pip install -r requirements.txt
```

Alternatively, the packages and their dependencies can be installed individually. The PyTorch version and corresponding pip command can be found here (https://pytorch.org/get-started/locally/).

```
> pip install torch torchvision torchaudio
> pip install py3nvml
> pip install tqdm
> pip install scipy
```

### Example Output when run on 2 nodes with 2 MNIST workers and 2 SciPy workers on Pinoak

```
> salloc --exclusive -N 2 -p allgriz
> dragon telemetry_full.py
Hello from main process pinoak0033.
using dragon runtime
Number of nodes: 2
Number of scipy workers: 2
Number of MNIST workers: 2
This is a telemetry process on node pinoak0033.
Number of images: 1024
This is a telemetry process on node pinoak0034.
This is the postprocessing process, pinoak0034.
Launching scipy and mnist jobs
nodename: pinoak0033 cpu load average 1 minute: 0.17 device # 0 utilization: 0.00%
nodename: pinoak0034 cpu load average 1 minute: 0.34 device # 0 utilization: 0.00%
nodename: pinoak0033 cpu load average 1 minute: 0.17 device # 0 utilization: 0.00%
nodename: pinoak0034 cpu load average 1 minute: 0.34 device # 0 utilization: 0.00%
nodename: pinoak0033 cpu load average 1 minute: 0.17 device # 0 utilization: 0.00%
nodename: pinoak0034 cpu load average 1 minute: 0.72 device # 0 utilization: 0.00%
nodename: pinoak0033 cpu load average 1 minute: 0.31 device # 0 utilization: 0.00%
nodename: pinoak0033 cpu load average 1 minute: 0.31 device # 0 utilization: 0.00%
nodename: pinoak0033 cpu load average 1 minute: 0.31 device # 0 utilization: 1.00%
nodename: pinoak0033 cpu load average 1 minute: 0.31 device # 0 utilization: 0.00%
nodename: pinoak0033 cpu load average 1 minute: 0.31 device # 0 utilization: 1.00%
nodename: pinoak0033 cpu load average 1 minute: 0.69 device # 0 utilization: 0.00%
nodename: pinoak0033 cpu load average 1 minute: 0.69 device # 0 utilization: 2.00%
nodename: pinoak0033 cpu load average 1 minute: 0.69 device # 0 utilization: 10.00%
nodename: pinoak0033 cpu load average 1 minute: 0.69 device # 0 utilization: 10.00%
nodename: pinoak0033 cpu load average 1 minute: 0.96 device # 0 utilization: 10.00%
nodename: pinoak0033 cpu load average 1 minute: 0.96 device # 0 utilization: 10.00%
nodename: pinoak0034 cpu load average 1 minute: 0.91 device # 0 utilization: 0.00%
nodename: pinoak0033 cpu load average 1 minute: 0.96 device # 0 utilization: 10.00%
nodename: pinoak0034 cpu load average 1 minute: 0.91 device # 0 utilization: 2.00%
.
.
.
< More Telemetry Data >
.
.
.
nodename: pinoak0033 cpu load average 1 minute: 33.97 device # 0 utilization: 2.00%
nodename: pinoak0034 cpu load average 1 minute: 29.7 device # 0 utilization: 3.00%
nodename: pinoak0033 cpu load average 1 minute: 33.97 device # 0 utilization: 0.00%
nodename: pinoak0034 cpu load average 1 minute: 29.7 device # 0 utilization: 0.00%
nodename: pinoak0033 cpu load average 1 minute: 33.97 device # 0 utilization: 0.00%
nodename: pinoak0034 cpu load average 1 minute: 27.4 device # 0 utilization: 0.00%
.
.
.
< More Telemetry Data >
.
.
.
Shutting down procs
Telemetry process on node pinoak0033 exiting ...
Telemetry process on node pinoak0034 exiting ...
Post process is exiting
Final test for learning rate 0.5: loss: 0.02791020164489746 accuracy: 99.1
Final test for learning rate 1.5: loss: 0.027457854652404787 accuracy: 99.21
```

Running with --bars will print the information using tqdm bars that are updated. The utilization for all GPUs on each node will be printed along with the cpu load average. Mid-run the output should look like:
```
> dragon telemetry_full.py --bars
Hello from main process pinoak0033.
using dragon runtime
Number of nodes: 2
Number of scipy workers: 2
Number of MNIST workers: 2
This is the postprocessing process, pinoak0034.
This is a telemetry process on node pinoak0033.
This is a telemetry process on node pinoak0034.
Number of images: 1024
Launching scipy and mnist jobs
pinoak0034 cpu load avg.:  22%|██▏       | 22.07/100 [00:55<03:14,  2.50s/it]
pinoak0034 device 0 util:   9%|▉         | 9/100 [00:55<09:17,  6.13s/it]
pinoak0034 device 1 util:   0%|          | 0/100 [00:55<?, ?it/s]
pinoak0034 device 2 util:   0%|          | 0/100 [00:55<?, ?it/s]
pinoak0034 device 3 util:   0%|          | 0/100 [00:55<?, ?it/s]
pinoak0033 cpu load avg.:  15%|█▌        | 15.03/100 [00:54<05:09,  3.64s/it]
pinoak0033 device 0 util:   9%|▉         | 9/100 [00:54<09:13,  6.08s/it]
pinoak0033 device 1 util:   0%|          | 0/100 [00:54<?, ?it/s]
pinoak0033 device 2 util:   0%|          | 0/100 [00:54<?, ?it/s]
pinoak0033 device 3 util:   0%|          | 0/100 [00:54<?, ?it/s]

```
The same shut down message as above will be printed when the job is finished. Note, the first time this is run, the MNIST data set will be downloaded and will lead to additional output.
