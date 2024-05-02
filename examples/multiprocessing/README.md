# Multiprocessing with Dragon Examples

## P2P_LAT - A point-to-point ping-pong latency test

### Introduction
This is a simple ping-pong latency microbenchmark useful for comparing the performance of the Pipe and Queue objects between Dragon and base multiprocessing.  In the case of Pipe, the benchmark can optionally use send() or send_bytes() as well to compare the overheads associated with packing/unpacking Python objects into/from serialized blobs of bytes.

### Usage
```
usage: dragon p2p_lat.py [-h] [--iterations ITERATIONS]
                         [--lg_max_message_size LG_MAX_MESSAGE_SIZE] [--dragon]
                         [--with_bytes] [--queues]

P2P latency test

optional arguments:
  -h, --help            show this help message and exit
  --iterations ITERATIONS
                        number of iterations to do
  --lg_max_message_size LG_MAX_MESSAGE_SIZE
                        log base 2 of size of message to pass in
  --dragon              run using dragon
  --with_bytes          use send_bytes/recv_bytes instead of send/recv
  --queues              use per-worker queues for the communication
```

### Example Output

#### Single node
```
> dragon p2p_lat.py --iterations 1000 --lg_max_message_size 12 --dragon
using Dragon
Msglen [B]   Lat [usec]
2  26.72357251867652
4  31.13584592938423
8  31.08951263129711
16  31.94776130840183
32  26.448716409504414
64  30.204192735254765
128  27.029050048440695
256  27.127526234835386
512  27.576626278460026
1024  30.415396671742197
2048  31.641206704080105
4096  34.05803442001343
```

#### Multi-node
In order to run the example on two nodes, we do:

```
> salloc --nodes=2 --exclusive

> dragon p2p_lat.py --iterations 1000 --lg_max_message_size 12 --dragon
[stdout: p_uid=4294967296] using Dragon
Msglen [B]   Lat [usec]
[stdout: p_uid=4294967296] 2  1244.2267920050654
[stdout: p_uid=4294967296] 4  1369.1070925051463
[stdout: p_uid=4294967296] 8  1383.1850550050146
[stdout: p_uid=4294967296] 16  1378.226093002013
[stdout: p_uid=4294967296] 32  1370.6759635024355
[stdout: p_uid=4294967296] 64  1347.0908230010536
[stdout: p_uid=4294967296] 128  1307.4521819944491
[stdout: p_uid=4294967296] 256  1356.2277884993819
[stdout: p_uid=4294967296] 512  1315.3960369963897
[stdout: p_uid=4294967296] 1024  1337.4174824930378
[stdout: p_uid=4294967296] 2048  1312.3137680013315
[stdout: p_uid=4294967296] 4096  1368.432818002475
```

### Communication Constructs
In each iteration of the test, the first Process sends a message through a Pipe to the second Process.  The second
Process blocks on a receive operation waiting for data.  Once received, the same size message is sent back to the
first process.  The --queues option uses one input queue for each worker; the first Process writes a message to the
Queue of the second, and the second Process, once received, writes the same size message to the Queue of the first Process.
The average *round trip* time is then reported (note OSU MPI latency is half round trip time).

When Pipe is used, small message latency with Dragon is typically lower than base multiprocessing using send()/recv() and roughly the
same as base multiprocessing for send_bytes()/recv_bytes().  We will continue to improve upon this.  For 1 MB message, Dragon approaches 7X lower latency than base multiprocessing.
When Queue is used, Dragon reaches 3X lower latency for smaller messages than base multiprocessing and 5X for 1 MB message.


## P2P_BW - A point-to-point bandwidth test

### Introduction
This is a simple bandwidth microbenchmark useful for comparing the performance of the Pipe and Queue objects between Dragon and base multiprocessing.  In the case of Pipe, the benchmark can optionally use send() or send_bytes() as well to compare the overheads associated with packing/unpacking Python objects into/from serialized blobs of bytes.

### Usage
```
usage: dragon p2p_bw.py [-h] [--iterations ITERATIONS]
                        [--lg_max_message_size LG_MAX_MESSAGE_SIZE] [--dragon]
                        [--with_bytes] [--queues]

P2P bandwidth test

optional arguments:
  -h, --help            show this help message and exit
  --iterations ITERATIONS
                        number of iterations to do
  --lg_max_message_size LG_MAX_MESSAGE_SIZE
                        log base 2 of size of message to pass in
  --dragon              run using dragon Connection
  --with_bytes          use send_bytes/recv_bytes instead of send/recv
  --queues              use queues for the communication
```

### Example Output

#### Single node

```
> dragon p2p_bw.py --iterations 10 --lg_max_message_size 12 --dragon
using Dragon
Msglen [B]   BW [MiB/s]
2  0.22227199395539773
4  0.44506294519019746
8  0.6250178367096635
16  1.7933765453555544
32  2.600928631169038
64  7.377148644960612
128  14.580965811690044
256  29.469662322678
512  37.86118024915544
1024  118.11021653589995
2048  189.86143989596152
4096  350.29070703866404
```

#### Multi-node
In order to run the example on two nodes, we do:

```
> salloc --nodes=2 --exclusive

> dragon p2p_bw.py --iterations 10 --lg_max_message_size 12 --dragon
[stdout: p_uid=4294967296] using Dragon
Msglen [B]   BW [MiB/s]
[stdout: p_uid=4294967296] 2  0.005786269384773654
[stdout: p_uid=4294967296] 4  0.011661270625917616
[stdout: p_uid=4294967296] 8  0.023239906189094772
[stdout: p_uid=4294967296] 16  0.046816081712733874
[stdout: p_uid=4294967296] 32  0.09345422002052811
[stdout: p_uid=4294967296] 64  0.18534289559215283
[stdout: p_uid=4294967296] 128  0.3687052490017522
[stdout: p_uid=4294967296] 256  0.7331597514276896
[stdout: p_uid=4294967296] 512  1.3139612269879999
[stdout: p_uid=4294967296] 1024  2.5836171471389586
[stdout: p_uid=4294967296] 2048  5.136942116564382
[stdout: p_uid=4294967296] 4096  10.229085884537279
```

### Communication Constructs
In each iteration of the test, the first Process sends a window of 32 messages through a Pipe/Queue to the second Process.
The second Process blocks on 32 receive operations waiting for data.  Once received, a single 8 B message is sent back
to the first process indicating all data has arrived.  The aggregate amount of data sent from the first Process
divided by the total time until the small 8 B message is received is used to compute bandwidth.

In the case of Pipe, Dragon currently has somewhat lower bandwidth than base multiprocessing for messages below 4 KB.  For 4 KB and above,
Dragon can significantly outperform base multiprocessing.  At 1 MB messages, Dragon achieves roughly 3X better
bandwidth than base multiprocessing.  We expect to improve on this.
In the case of Queue, Dragon reaches 2X better bandwidth than base multiprocessing for smaller messages, whereas, for messages larger than 8 MB approaches 4-5X better bandwidth than base multiprocessing.


## AA_BENCH - An all-to-all benchmark using Python multiprocessing

### Introduction

`aa_bench` is a simple all-to-all communication pattern comparison benchmark using basic multiprocessing API objects.
It’s meant to roughly compare performance and scaling between dragon and library multiprocessing code, as well as to stress test the infrastructure and underlying libraries as they approach the limits of system resources.

### Usage
```
usage: dragon aa_bench.py [-h] [--num_workers NUM_WORKERS] [--iterations ITERATIONS]
                          [--burn_iterations BURN_ITERATIONS]
                          [--lg_message_size LG_MESSAGE_SIZE] [--dragon] [--queues]

All-all connection/queue test

optional arguments:
  -h, --help            show this help message and exit
  --num_workers NUM_WORKERS
                        number of workers in all-all
  --iterations ITERATIONS
                        number of iterations to do
  --burn_iterations BURN_ITERATIONS
                        number of iterations to burn first time
  --lg_message_size LG_MESSAGE_SIZE
                        log base 2 of msg size between each pair
  --dragon              run with dragon objs
  --queues              measure aa with per-worker queues
```


Each iteration consists of each worker sending a message of the specified size to each other worker and receiving a message of the same size from each other worker.  Since this test is focused on general flow and exercising the library, there are no barriers between iterations.  The results presented are averaged over all the iterations.  --dragon runs with the ‘dragon’ start method, thus using dragon.infrastructure.Pipe and dragon.mpbridge.queue.DragonQueue for multiprocessing.Pipe and multiprocessing.Queue respectively.

### Example Output

#### Single node

```
> dragon aa_bench.py --iterations 10 --num_workers 16 --lg_message_size 21 --dragon
using dragon runtime
using 120 Pipes
nworkers=16 its=100 msg_size_in_k=2048.0 completion_time_in_ms=23.297 bw_in_mb_sec=1373.58
```

#### Multi-node
In order to run the example on two nodes, we do:

```
> salloc --nodes=2 --exclusive

> dragon aa_bench.py --iterations 10 --num_workers 16 --lg_message_size 21 --dragon
using dragon runtime
using 120 Pipes
nworkers=16 its=10 msg_size_in_k=2048.0 completion_time_in_ms=3200.456 bw_in_mb_sec=10.00

```

Resources in Dragon version 0.4 are generated on the node that the calling process resides on.  This means there are two ways for this benchmark to work, generating all resources in the main process and passing them to children workers ("native" multiprocessing style), or having each process generate its own communication resource and share it through a central queue ("dragon" multiprocessing style).

In particular, the latter cannot be done in native multiprocessing with queues due to underlying details, but can be done in Dragon.  This is especially important for multi-node scaling so that resources are generated on the node the worker process lives on.  In order to compare these two styles, you can run with `--dragon_compare`.  Compare the above multi-node result with the "native" style comparison, both running through dragon:

```
>salloc --nodes=2 --exclusive

> dragon aa_bench --iterations 10 --num_workers 16 --lg_message_size 21 --dragon_compare
using dragon runtime
using 120 Pipes
nworkers=16 its=10 msg_size_in_k=2048.0 completion_time_in_ms=6176.021 bw_in_mb_sec=5.18

```

### Communication Constructs

By default the worker processes are connected pairwise using bidirectional Pipes, so N workers will require N(N-1)/2 Pipes.

The dragon implementation can be observed to perform better.  It scales to larger numbers of nodes because it doesn’t reach the operating system open file descriptor limit the way the library code does.

The --queues option uses one input queue for each worker, that every other worker writes to.

The dragon implementation can be observed to perform better.  It scales to larger numbers of nodes because it doesn’t exhaust the number of threads the operating system will support.

## Merge Sort - A multiprocessing Example

The merge_sort.py program is a simple example illustrating the use of a multiprocessing Queue and Process. It
runs the merge sort algorithm in parallel by splitting a list in half, and merge sorting each half in a new
process. It uses a Queue to transmit the results back to the main process. The order the two halves are
received is irrelevant since the two halves are immediately merged in the parent process.

The cutoff second parameter specifies the list size where the built-in sort is then called to sort
the final sublists of the original list when they are equal to the cutoff size.

This example creates a large number of queues and exercises the Dragon infrastructure through the
Global Services API by using the Dragon implemented multiprocessing. A comparison is included here
from existing multiprocessing.

### Example Output from standard multiprocessing
```
> python3 merge_sort.py
    List Size    Time (seconds)    Proceses    Channels (or Queues) with cutoff=200000
      2000000          2.116517          31          15
      2800000          2.420228          31          15
      3600000          3.148048          63          31
      4400000          3.865382          63          31
      5200000          4.480543          63          31
      6000000          5.135389          63          31
      6800000          5.923115         127          63
      7600000          6.668537         127          63
      8400000          7.355825         127          63
      9200000          7.927604         127          63
     10000000          8.600533         127          63
     10800000          9.429227         127          63
     11600000          9.954516         127          63
     12400000         10.596600         127          63
     13200000         11.576345         255         127
     14000000         12.192575         255         127
     14800000         12.868313         255         127
     15600000         13.671241         255         127
     16400000         14.165466         255         127
     17200000         14.984548         255         127
     18000000         15.652224         255         127
     18800000         16.197072         255         127
     19600000         17.036290         255         127
     ...
     80000000         75.237872        1023         511
+++ head proc exited, code 0
```
### Example Output from Dragon Multiprocessing

#### Single node
```
> dragon merge_sort.py dragon
    List Size    Time (seconds)    Proceses    Channels (or Queues) with cutoff=200000
      2000000          2.347369          31          15
      2800000          2.973319          31          15
      3600000          3.951618          63          31
      4400000          4.624876          63          31
      5200000          5.475652          63          31
      6000000          5.957036          63          31
      6800000          7.324906         127          63
      7600000          7.913571         127          63
      8400000          8.563757         127          63
      9200000          9.305563         127          63
     10000000          9.959780         127          63
     10800000         10.551508         127          63
     11600000         11.361915         127          63
     12400000         11.986475         127          63
     13200000         14.220921         255         127
     14000000         14.906546         255         127
     14800000         15.606451         255         127
     15600000         16.572735         255         127
     16400000         17.688870         255         127
     17200000         23.341039         255         127
     18000000         19.065733         255         127
     18800000         20.078935         255         127
     19600000         20.935255         255         127
     ...
     80000000         79.719471        1023         511
+++ head proc exited, code 0
```

#### Multi-node

In order to run the example on two nodes, we do:

```
> salloc --nodes=2 --exclusive

> dragon merge_sort.py dragon
[stdout: p_uid=4294967296]     List Size    Time (seconds)    Processes    Channels (or Queues) with cutoff=20000
[stdout: p_uid=4294967296]        100000          0.921267          15           7
[stdout: p_uid=4294967296]        200000          1.341456          31          15
[stdout: p_uid=4294967296]        300000          1.425575          31          15
[stdout: p_uid=4294967296]        400000          2.297622          63          31
[stdout: p_uid=4294967296]        500000          2.382305          63          31
[stdout: p_uid=4294967296]        600000          2.476652          63          31
```

For this current version, this example cannot scale to larger list sizes when testing on multi-node. Future versions will take care of that.


## Parallel Producer - Consumer Communication with Queue

The `queue_demo.py` example shows a multiple producers multiple consumers communication scheme with Multiprocessing and Dragon.

The parent process creates and starts a number of processes. The first half acts as a producer, creating random strings,
packaging them up with a random selection of lambdas and putting this work package into a shared queue. The second half
of the processes acts as consumers. They get handed only the queue, execute a blocking get() on it to receive the work package.
Once the package is received, they execute the lambdas one by one on the string, printing the output. Finally, the parent process
joins on all process objects, to ensure they have completed successfully.

For data serialization we use `cloudpickle`, as standard `pickle` does not support lambdas. You will need to execute `pip3 install cloudpickle`.

### Example Output

#### Single node

```
> dragon queue_demo.py
I am producer 4294967297 and I'm sending data: "n jqc vneb itfqd eygjfc ljwzrfa" and string ops: capitalize upper lower strip
I am consumer 4294967298 -- capitalize: "N jqc vneb itfqd eygjfc ljwzrfa" upper: "N JQC VNEB ITFQD EYGJFC LJWZRFA" lower: "n jqc vneb itfqd eygjfc ljwzrfa" strip: "n jqc vneb itfqd eygjfc ljwzrfa"
I am producer 4294967301 and I'm sending data: "l xpp fvjh odgqi cmhxqa syxgnvl" and string ops: lower
I am consumer 4294967300 -- lower: "l xpp fvjh odgqi cmhxqa syxgnvl"
I am producer 4294967299 and I'm sending data: "w ebz uwjc ahpxw cmpfac uxyuoyd" and string ops: capitalize strip lower replace(" ", "") upper
I am consumer 4294967302 -- capitalize: "W ebz uwjc ahpxw cmpfac uxyuoyd" strip: "W ebz uwjc ahpxw cmpfac uxyuoyd" lower: "w ebz uwjc ahpxw cmpfac uxyuoyd" replace(" ", ""): "webzuwjcahpxwcmpfacuxyuoyd" upper: "WEBZUWJCAHPXWCMPFACUXYUOYD"
I am producer 4294967303 and I'm sending data: "x yga ysbv jqbvu eoryiv wemvydd" and string ops: upper lower replace(" ", "") capitalize strip
I am consumer 4294967306 -- upper: "X YGA YSBV JQBVU EORYIV WEMVYDD" lower: "x yga ysbv jqbvu eoryiv wemvydd" replace(" ", ""): "xygaysbvjqbvueoryivwemvydd" capitalize: "Xygaysbvjqbvueoryivwemvydd" strip: "Xygaysbvjqbvueoryivwemvydd"
I am producer 4294967305 and I'm sending data: "m evl kaaq bbamw yuxces mflukgc" and string ops: replace(" ", "")
I am consumer 4294967304 -- replace(" ", ""): "mevlkaaqbbamwyuxcesmflukgc"
I am producer 4294967311 and I'm sending data: "r zdv gqni phjop rxxnjv mwnoavn" and string ops: lower upper replace(" ", "") capitalize
I am consumer 4294967308 -- lower: "r zdv gqni phjop rxxnjv mwnoavn" upper: "R ZDV GQNI PHJOP RXXNJV MWNOAVN" replace(" ", ""): "RZDVGQNIPHJOPRXXNJVMWNOAVN" capitalize: "Rzdvgqniphjoprxxnjvmwnoavn"
I am producer 4294967307 and I'm sending data: "j njm pnpg spkvg bfsukk ihfmklm" and string ops: capitalize strip lower upper replace(" ", "")
I am consumer 4294967310 -- capitalize: "J njm pnpg spkvg bfsukk ihfmklm" strip: "J njm pnpg spkvg bfsukk ihfmklm" lower: "j njm pnpg spkvg bfsukk ihfmklm" upper: "J NJM PNPG SPKVG BFSUKK IHFMKLM" replace(" ", ""): "JNJMPNPGSPKVGBFSUKKIHFMKLM"
I am producer 4294967309 and I'm sending data: "a eij rzuz rlilc jkiaxr raqzvft" and string ops: replace(" ", "") capitalize upper strip lower
I am consumer 4294967312 -- replace(" ", ""): "aeijrzuzrlilcjkiaxrraqzvft" capitalize: "Aeijrzuzrlilcjkiaxrraqzvft" upper: "AEIJRZUZRLILCJKIAXRRAQZVFT" strip: "AEIJRZUZRLILCJKIAXRRAQZVFT" lower: "aeijrzuzrlilcjkiaxrraqzvft"
+++ head proc exited, code 0
```

#### Multi-node

In order to run the example on two nodes, we do:

```
> salloc --nodes=2 --exclusive

> dragon queue_demo.py
[stdout: p_uid=4294967297] I am producer 4294967297 and I'm sending data: "y wxo oink lnycd gjeigx ptwroky" and string ops: strip replace(" ", "") lower capitalize
[stdout: p_uid=4294967298] I am consumer 4294967298 -- strip: "y wxo oink lnycd gjeigx ptwroky" replace(" ", ""): "ywxooinklnycdgjeigxptwroky" lower: "ywxooinklnycdgjeigxptwroky" capitalize: "Ywxooinklnycdgjeigxptwroky"
[stdout: p_uid=4294967299] I am producer 4294967299 and I'm sending data: "r orc kytq uihyh axummw nohsved" and string ops: lower strip upper capitalize
[stdout: p_uid=4294967300] I am consumer 4294967300 -- lower: "r orc kytq uihyh axummw nohsved" strip: "r orc kytq uihyh axummw nohsved" upper: "R ORC KYTQ UIHYH AXUMMW NOHSVED" capitalize: "R orc kytq uihyh axummw nohsved"
[stdout: p_uid=4294967301] I am producer 4294967301 and I'm sending data: "q ydi iukq rkhhn hgphyi swsbqyj" and string ops: strip capitalize
[stdout: p_uid=4294967302] I am consumer 4294967302 -- strip: "q ydi iukq rkhhn hgphyi swsbqyj" capitalize: "Q ydi iukq rkhhn hgphyi swsbqyj"
[stdout: p_uid=4294967303] I am producer 4294967303 and I'm sending data: "y ccs wxmj aelrl nkivog ftwbhvp" and string ops: strip capitalize lower replace(" ", "")
[stdout: p_uid=4294967304] I am consumer 4294967304 -- strip: "y ccs wxmj aelrl nkivog ftwbhvp" capitalize: "Y ccs wxmj aelrl nkivog ftwbhvp" lower: "y ccs wxmj aelrl nkivog ftwbhvp" replace(" ", ""): "yccswxmjaelrlnkivogftwbhvp"
[stdout: p_uid=4294967305] I am producer 4294967305 and I'm sending data: "s csm ssqi tlibb wzytna yfmgdth" and string ops: lower upper
[stdout: p_uid=4294967306] I am consumer 4294967306 -- lower: "s csm ssqi tlibb wzytna yfmgdth" upper: "S CSM SSQI TLIBB WZYTNA YFMGDTH"
[stdout: p_uid=4294967307] I am producer 4294967307 and I'm sending data: "p tgb sqny hiaad fzcved rrthmzf" and string ops: lower strip replace(" ", "") capitalize upper
[stdout: p_uid=4294967308] I am consumer 4294967308 -- lower: "p tgb sqny hiaad fzcved rrthmzf" strip: "p tgb sqny hiaad fzcved rrthmzf" replace(" ", ""): "ptgbsqnyhiaadfzcvedrrthmzf" capitalize: "Ptgbsqnyhiaadfzcvedrrthmzf" upper: "PTGBSQNYHIAADFZCVEDRRTHMZF"
[stdout: p_uid=4294967309] I am producer 4294967309 and I'm sending data: "p apt dahe cxqgl cbfgii urriokd" and string ops: lower replace(" ", "") capitalize
[stdout: p_uid=4294967310] I am consumer 4294967310 -- lower: "p apt dahe cxqgl cbfgii urriokd" replace(" ", ""): "paptdahecxqglcbfgiiurriokd" capitalize: "Paptdahecxqglcbfgiiurriokd"
[stdout: p_uid=4294967311] I am producer 4294967311 and I'm sending data: "p ddo awwf qpyuj xcgwqf knoljyq" and string ops: capitalize lower replace(" ", "") strip
[stdout: p_uid=4294967312] I am consumer 4294967312 -- capitalize: "P ddo awwf qpyuj xcgwqf knoljyq" lower: "p ddo awwf qpyuj xcgwqf knoljyq" replace(" ", ""): "pddoawwfqpyujxcgwqfknoljyq" strip: "pddoawwfqpyujxcgwqfknoljyq"
```

## DISTMERGE - A Scalable, Ordered Sequence Merge Algorithm

### Introduction
The distmerge.py program is an example of a distributed, scalable algorithm for merging sorted
data from multiple sources into one ordered stream. It does this in a tree-like fashion where
the programmer can control the degree of any node in the process tree via a fanin_size argument.

The programmer provides a list of functions that are data produces. Each of these functions must
produce an ordered sequence of data. Data from all these functions is then merged into one
ordered stream via a hierarchy of MergePools. The MergePool class handles all the details of
creating this sorted stream. The programmer simply writes their data producing functions to accept
a queue and a optionally other arguments as supplied to the MergePool.

### Example Output
$ dragon distmerge.py
0
... (repeats with twenty 0's, 1's, 2's, etc. to twenty 9's)
9
9
+++ head proc exited, code 0
$

## Prime Numbers - Example of a Parallel Pipeline

In this example, we demonstrate how to set up a parallel pipeline. In this case it finds
prime numbers in parallel. Each stage of the pipeline can be configured to perform serial
work. In this case, the serial work is serially checking a set of numbers against a
candidate prime number to see if it is relatively prime or not. When numbers are found
to be relatively prime to the primes a in a stage of the pipeline, they are added to the
stage unless the stage size is maxxed out. If the stage is full, a new stage is dynamically
started to handle more of the workload in parallel and the stages are hooked together to
build a pipeline.

The demo code takes no command-line arguments, but you can play with the code to change
things like the stage size and the maximum number to find primes less than.

To run it on a multi-node system, get an allocation and then start the application with
dragon.

### Example Output

$ dragon prime_numbers.py
Prime numbers:
2 3 5 7 11 13 17 19 23 29 31 37 41 43 47 53 59 61 67 71 73 79 83 89 97
primes in pipeline stage are [97]

primes in pipeline stage are [79, 83, 89]

primes in pipeline stage are [67, 71, 73]

primes in pipeline stage are [53, 59, 61]

primes in pipeline stage are [41, 43, 47]

primes in pipeline stage are [29, 31, 37]

primes in pipeline stage are [17, 19, 23]

primes in pipeline stage are [7, 11, 13]

primes in pipeline stage are [2, 3, 5]
$

## Pascal Triangle: Shared State Context Demo

In this example, we demonstrate that the Dragon multiprocessing interface can be used to create a simple shared state example. The manager multiprocessing process and the client multiprocessing process communicate via a shared state spawned by the context multiprocessing process. The main multiprocessing process will start the manager and client multiprocessing processes. The manager process finds the sum of the Pascal triangle array calcualated by the client process. The third multiprocessing process spawned by the context class finds when the Pascal triangle has been completed. The shared state that contains the Pascal triangle array and the Pascal triangle sum is guarded by a lock; only the process that accesses the lock may alter the array and value.

### Example Output

```
> dragon shared_state_pascal_triangle.py --rows 5
Pascal Triangle Array Calculated for 5 rows from the Pascal row of 0 to the Pascal row of 5 , and the associated sum of the Pascal triangle array.
Pascal Triangle Array [1, 1, 1, 1, 1, 2, 1, 1, 3, 3, 1, 1, 4, 6, 4, 1]
Pascal Triangle Sum: 32
```
