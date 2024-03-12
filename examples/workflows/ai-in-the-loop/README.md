# AI-in-the-loop workflow with Dragon

## Introduction
This is an example of how Dragon can be used to execute an AI-in-the-loop workflow. Inspiration for this demo comes from the NERSC-10 Workflow Archetypes White Paper. This workflow most closely resembles the workflow scenario given as part of archetype four. In this example we use a small model implemented in PyTorch to compute an approximation to sin(x). In parallel to doing the inference with the model, we launch `sim-cheap` on four ranks. This MPI job computes the taylor approximation to sin(x) and compares this with the output of the model. If the difference is less than 0.05 we consider the model's approximation to be sufficiently accurate and print out the result with the exact result. If the difference is larger than 0.05 we consider this a failure and re-train the model on a new set of data. To generate this data we launch `sim-expensive`. This MPI job is launched on eight ranks-per-node and each rank generates 32 data points of the form (x, sin(x)) where x is sampled uniformly in [-pi, pi). This data is aggregated into a PyTorch tensor and then used to train the model. We then re-evaluate the re-trained model and decide if we need to re-train again or if the estimate is sufficiently accurate. We continue this loop until we've had five successes.

Below is a diagram of the main computational loop.
```
          ⬇
   Parallel Execution   ⬅   Re-train the AI Model
    ⬇         ⬇
   Infer    Calculate
value from  comparison
  AI Model  using four                ⬆
           rank MPI job
    ⬇         ⬇
   Parallel Execution
          ⬇
   Is the inferred     No    Launch expensive MPI process
     value within      ⮕       to generate new data
      tolerance?
          ⬇ Yes
```


## Usage

`ai-in-the-loop.py` - This is the main file. It contains functions for launching both MPI executables and parsing the results as well as imports functions defined in `model.py` and coordinates the model inference and training with the MPI jobs.

`model.py` - This file defines the model and provides some functions for model training and inference.

`sim-expensive.c` - This contains what we are considering the expensive MPI job. It computes (x, sin(x)) data points that are used to train the model.

`sim-cheap.c` - This is the cheap approximation. It computes the Taylor approximation of sin(x).

`Makefile` - Used to build the two MPI applications.

`model_pretrained_poly.pt` - A pre-trained model that we load and use until training is required.

```
usage: dragon ai-in-the-loop.py
```

## Installation

After installing dragon, the only other dependency is on PyTorch and SciPy. The PyTorch version and corresponding pip command can be found here (https://pytorch.org/get-started/locally/).

```
> pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cpu
> pip install scipy
```

### Description of the system used

For this example, HPE Cray Hotlum nodes were used. Each node has AMD EPYC 7763 64-core CPUs.


## Example Output

### Multi-node

The default parameters are for 16 nodes but this example has been run up to 64 nodes with 8 ranks-per-node.
```
> make
gcc -g  -pedantic -Wall -I /opt/cray/pe/mpich/8.1.26/ofi/gnu/9.1/include -L /opt/cray/pe/mpich/8.1.26/ofi/gnu/9.1/lib   -c -o sim-cheap.o sim-cheap.c
gcc -g  -pedantic -Wall -I /opt/cray/pe/mpich/8.1.26/ofi/gnu/9.1/include -L /opt/cray/pe/mpich/8.1.26/ofi/gnu/9.1/lib  sim-cheap.o -o sim-cheap -lm -L /opt/cray/pe/mpich/8.1.26/ofi/gnu/9.1/lib -lmpich
gcc -g  -pedantic -Wall -I /opt/cray/pe/mpich/8.1.26/ofi/gnu/9.1/include -L /opt/cray/pe/mpich/8.1.26/ofi/gnu/9.1/lib   -c -o sim-expensive.o
gcc -g  -pedantic -Wall -I /opt/cray/pe/mpich/8.1.26/ofi/gnu/9.1/include -L /opt/cray/pe/mpich/8.1.26/ofi/gnu/9.1/lib  sim-expensive.o -o sim-expensive -lm -L /opt/cray/pe/mpich/8.1.26/ofi/gnu/9.1/lib -lmpich
> salloc --nodes=16 --exclusive
> dragon ai-in-the-loop.py
training
approx = 0.1283823400735855, exact = 0.15357911534767393
training
approx = -0.41591891646385193, exact = -0.4533079140996079
approx = -0.9724616408348083, exact = -0.9808886564963794
approx = -0.38959139585494995, exact = -0.4315753703483373
approx = 0.8678910732269287, exact = 0.8812041533601648
```
