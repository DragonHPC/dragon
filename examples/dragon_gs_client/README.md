# Dragon API Examples

The purpose of examples here is to show how the Dragon GS Client API is used directly to accomplish a variety of tasks
including shared memory access, process control, and communication.  These examples are in some cases motivated
by multiprocessing and give an idea of how its interfaces are implemented over Dragon.

## Simple Multiple Producer Multiple Consumer Communication Demo using Dragon API

This example shows how the GS Client API, can be used directly in a multi-reader/multi-writer scenario with
data that needs to be efficiently serialzied.  This example has producers generating random data and random
functions that are written into a Dragon Channel. The consumers then pull items out of the Dragon Channel and
execute the received function on the received data.

```
dragon queue_demo.py
```

This example is not designed to work multi-node.

## Example Using Dragon to Compute Pi

This example shows how to use the Dragon client API directly to launch and control a fleet of workers that are communicating with each other using Dragon channels.  This demonstrates the basic ingredients of Dragon used to implement dragon.native.pool.
To use on single or multi-node, run:

```
dragon pi_demo.py 2
```


## Simple Single Producer Single Consumer Communication Demo using Dragon API

This example demonstrates the optimal way to communicate data that will be serialized
with Pickle or other serializers which interact with a file-like interface. It shows
how large payloads (larger than the Dragon Managed Memory Pool) can be communicated.

To use, run `dragon connection_demo.py`.

This example is not designed to work multi-node.


## Dragon API Subprocess.Run and Subprocess.Popen Examples

The following Dragon examples handle Linux prompts with limited outputs via subprocess.run and larger outputs via subprocess.Popen. There are two processes: one started by main and the other by submain.
The programs respectively create a channel and start the process that starts the shell command. For this example, we used `ls`. The processes respectively run the command, capture the output, and pass the data through a channel. The original process from main outputs the message and joins on the other process from submain. The main difference between the two processes are that `subprocess.run` gets all the output after the process has exited, while the popen version gets the output as it becomes available while the program is running.

How to run the programs:

`dragon dragon_run_api.py ls`

`dragon dragon_popen_api.py ls`

This example is not designed to work multi-node.

## Dragon Client Server API example

The following Dragon API example creates a single multiprocessing process for the server. The Dragon channel is created on the default memory pool. The client handles user input from the requests.txt file. The client passes the request information on the Dragon channel to the server process. The server creates a server:client channel to pass the messages to the client. After all the requests are processed or the stop request is processed, the server stops listening for requests.

How to run the programs:

`dragon dragon_server_client_api.py`

This example is not designed to work multi-node.

## Managed Memory API Example

Dragon Python API example placing data into managed memory, starting a collection of processes, and have each modify that data with sequencing managed through channels.

How to run the program:

`dragon managed_mem_demo.py`

Expected output:

```
Completed message with all pids: [12844, 12845, 12846, 12847]
+++ head proc exited, code 0
```

This example is not designed to work multi-node.

## Group API Example

The Dragon Group API Example efficiently starts a group of Python processes.

How to run the example:

`dragon dragon_group_demo.py`

Expected output:

```
Hello from x1000c0s2b0n0
Hello from x1000c0s2b0n0
Hello from x1000c0s2b0n1
Hello from x1000c0s2b0n1
```

## Group API MPI Example

In this example, the Dragon Group API is used to start the mpi_hello MPI application.

How to run the example:

```
make
dragon dragon_group_mpi_demo.py
```

Expected Output:

```
> make
gcc -g  -pedantic -Wall -I /opt/cray/pe/mpich/8.1.27/ofi/gnu/9.1/include -L /opt/cray/pe/mpich/8.1.27/ofi/gnu/9.1/lib  -c mpi_hello.c -o mpi_hello.c.o
gcc -lm -L /opt/cray/pe/mpich/8.1.27/ofi/gnu/9.1/lib -lmpich  mpi_hello.c.o -o mpi_hello

> dragon dragon_group_mpi_demo.py
Hello world from pid 180513, processor x1000c0s2b0n0, rank 1 out of 4 processors
Hello world from pid 179427, processor x1000c0s2b0n1, rank 0 out of 4 processors
Hello world from pid 180514, processor x1000c0s2b0n0, rank 3 out of 4 processors
Hello world from pid 179428, processor x1000c0s2b0n1, rank 2 out of 4 processors
```

## Group Create Add To API Example

The Dragon Group API Example efficiently starts a group of Python processes.

How to run the example:

`dragon dragon_group_create_addto_demo.py`

Expected output:

```
Hello from x1000c0s2b0n0
Hello from x1000c0s2b0n0
Hello2 from x1002c0s1b1n0
Hello2 from x1002c0s3b0n0
```
