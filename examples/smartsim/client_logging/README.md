## Using Dragon Infrastructure Logging for Distributed Client Logging


### Introduction
The HPE-Cray SmartSim team expressed interest in using the logging written for the Dragon
runtime for their own tracking of SmartSim experiments. This example provides a brief overview
of how to do that using a `DragonLogger` object and the python logging module.

This example takes a given executable (rand and rand_failure) and executes it as part of a 
multiprocessing pool. Each pool worker runs the executable and tracks job status and output
and logs it via the python logging module. 

The python logging module has been provided a handler that takes log messages and inserts 
them into a DragonLogger object and sends them to a central process that is consuming the 
log messages and logging them to file.

This example will create a log file in your current working directory prefixed with `dragon-shim`
if using dragon multiprocessing, or `regular-shim` if using base multiprocessing.

To run these executables, they will need to be compiled using the included Makefile via a `make`
call.

### Usage
```
usage: logging_shim.py [-h] [--retries RETRIES] [--timeout TIMEOUT] [--num_workers NUM_WORKERS] [--dragon] [prog] ...

positional arguments:
  prog                  Executable pool workers will launch
  args                  Option to the executable

optional arguments:
  -h, --help            show this help message and exit
  --retries RETRIES     Number of attempts to run an executable before exiting
  --timeout TIMEOUT     Time to wait on executable to complete
  --num_workers NUM_WORKERS
                        Number of pool workers for deploying executables
  --dragon              Run with dragon multiprocessing
```

### Example Logging Output

#### Single node with base multiprocessing

```
$ python3 logging_shim.py ${PWD}/rand.exe
$ cat regular-shim_05_11_15_26_35.log
2023-05-11 15:26:35,986 INFO     logger_hotlum-login :: hello from logging thread
2023-05-11 15:26:36,016 INFO     shim_hotlum-login_0.run_image :: Beginning execution, cmdline=/lus/scratch/nhill/dragon_redux/examples/smartsim/client_logging/rand.exe --retries 0
2023-05-11 15:26:36,016 INFO     shim_hotlum-login_1.run_image :: Beginning execution, cmdline=/lus/scratch/nhill/dragon_redux/examples/smartsim/client_logging/rand.exe --retries 0
2023-05-11 15:26:36,016 INFO     shim_hotlum-login_2.run_image :: Beginning execution, cmdline=/lus/scratch/nhill/dragon_redux/examples/smartsim/client_logging/rand.exe --retries 0
2023-05-11 15:26:36,023 INFO     shim_hotlum-login_2.run_image :: Execution complete
2023-05-11 15:26:36,024 INFO     shim_hotlum-login_2.run_image :: stdout: 7.44973

2023-05-11 15:26:36,024 INFO     shim_hotlum-login_1.run_image :: Execution complete
2023-05-11 15:26:36,024 INFO     shim_hotlum-login_0.run_image :: Execution complete
2023-05-11 15:26:36,024 INFO     shim_hotlum-login_1.run_image :: stdout: 7.44973

2023-05-11 15:26:36,024 INFO     shim_hotlum-login_0.run_image :: stdout: 7.44973

2023-05-11 15:26:36,024 INFO     shim_hotlum-login_3.run_image :: Beginning execution, cmdline=/lus/scratch/nhill/dragon_redux/examples/smartsim/client_logging/rand.exe --retries 0
2023-05-11 15:26:36,029 INFO     shim_hotlum-login_3.run_image :: Execution complete
2023-05-11 15:26:36,029 INFO     shim_hotlum-login_3.run_image :: stdout: 7.44973
```

#### Single node with dragon multiprocessing

Using single node dragon multiprocessing doesn't illustrate a meaningful
difference with regard to base multiprocessing logging. However to execute 
the example with the dragon runtime on 1 node:

```
$ dragon -s logging_shim.py --dragon ${PWD}/rand.exe
```

#### Multi-node with dragon multiprocessing
In order to run the example on two nodes, we do:

```
$ salloc --nodes=2 --exclusive
$ dragon logging_shim.py --dragon ${PWD}/rand.exe
$ cat dragon-shim
2023-05-11 15:26:42,687 INFO     logger_x1000c1s6b1n0 :: hello from logging thread
2023-05-11 15:26:42,691 INFO     shim_x1000c1s6b0n1_0.run_image :: Beginning execution, cmdline=/lus/scratch/nhill/dragon_redux/examples/smartsim/client_logging/rand.exe
2023-05-11 15:26:42,695 INFO     shim_x1000c1s6b0n1_0.run_image :: Execution complete
2023-05-11 15:26:42,696 INFO     shim_x1000c1s6b0n1_0.run_image :: stdout: 8.99304

2023-05-11 15:26:42,697 INFO     shim_x1000c1s6b0n1_1.run_image :: Beginning execution, cmdline=/lus/scratch/nhill/dragon_redux/examples/smartsim/client_logging/rand.exe
2023-05-11 15:26:42,699 INFO     shim_x1000c1s6b0n1_1.run_image :: Execution complete
2023-05-11 15:26:42,700 INFO     shim_x1000c1s6b0n1_1.run_image :: stdout: 8.99304

2023-05-11 15:26:42,701 INFO     shim_x1000c1s6b0n1_2.run_image :: Beginning execution, cmdline=/lus/scratch/nhill/dragon_redux/examples/smartsim/client_logging/rand.exe
2023-05-11 15:26:42,702 INFO     shim_x1000c1s6b0n1_3.run_image :: Beginning execution, cmdline=/lus/scratch/nhill/dragon_redux/examples/smartsim/client_logging/rand.exe
2023-05-11 15:26:42,703 INFO     shim_x1000c1s6b0n1_2.run_image :: Execution complete
2023-05-11 15:26:42,704 INFO     shim_x1000c1s6b0n1_2.run_image :: stdout: 8.99304

2023-05-11 15:26:42,705 INFO     shim_x1000c1s6b0n1_3.run_image :: Execution complete
2023-05-11 15:26:42,706 INFO     shim_x1000c1s6b0n1_3.run_image :: stdout: 8.99304
```

The main difference between this log file and the one above is the change in 
hostnames. In the single node case, every host is `hotlum-login`. In the
multinode case, we can see that the logging/pool work is done on all the
backend nodes via the multiple hostnames: `x1000c1s6b0n1, x1000c1s6b0n0`