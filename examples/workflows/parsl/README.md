# AI-in-the-loop workflow with Dragon 

## Introduction
This shows an example workflow using Parsl with Dragon. In this example we use a Dragon implementation of the `@mpi_app` decorator and the `DragonMPIExecutor`. The executor expects five arguments to be returned from the decorated function: the executable, the directory containing the executable, the policy for process placement, the number of MPI processes to launch, and the arguments to pass to the executable. The arguments are expected to be returned in this order. The executor returns a future thats result is a dictionary containing a connection to stdin and stdout to rank 0.

In this example we compute the factorial of the largest MPI rank. We multiply this factorial by a scale factor that is sent using the stdin connection and add a bias to the scaled factorial that is passed to the MPI app via the args. The result is printed out by rank 0 and received by the head process from the stdout connection. This result is printed out and compared to the expected exact solution.  

## Usage

`parsl_mpi_app_demo.py` - This is the main file. It contains the `@mpi_app` decorated function with the required return arguments for that function. It also has the two functions used for sending data to and receiving data from stdin and stdout, respectively.  

`factorial.c` - This contains what the MPI application that computes the factorial, scales it by the scale factor received from the stdin connection, and then adds the bias from the args to it.

`Makefile` - Used to build the MPI application.

```
usage: dragon parsl_mpi_app_demo.py
```

## Installation 

After installing dragon, the only other dependency is on Parsl. The command to install Parsl is

```
> pip install parsl 
```

## Example Output

### Multi-node

```
> make
gcc -g  -pedantic -Wall -I /opt/cray/pe/mpich/8.1.27/ofi/gnu/9.1/include -L /opt/cray/pe/mpich/8.1.27/ofi/gnu/9.1/lib   -c -o factorial.o factorial.c
gcc -g  -pedantic -Wall -I /opt/cray/pe/mpich/8.1.27/ofi/gnu/9.1/include -L /opt/cray/pe/mpich/8.1.27/ofi/gnu/9.1/lib  factorial.o -o factorial -lm -L /opt/cray/pe/mpich/8.1.27/ofi/gnu/9.1/lib -lmpich
> salloc --nodes=2 --exclusive
>$dragon dragon parsl_mpi_app_demo.py
mpi computation: 0.000100 * 362880.000000 + 10.000000 = 46.288000 , exact = 46.288000000000004 
```
