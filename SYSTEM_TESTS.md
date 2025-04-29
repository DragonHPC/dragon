# Testing status


#### Systems to test on:

* pinoak (Slurm EX)
* horizon (Slurm XC w/ scaling capacity)
* cfa6k (PBS+PALS)

#### Tests to run:

* p2p latency and bandwidth, `examples/multiprocessing/p2p*.py`
```
dragon p2p_lat.py --dragon --lg_max_message_size 22 --iterations 20
dragon p2p_bw.lat --dragon --lg_max_message_size 22 --iterations 10
```

* SciPy image convolution - as close to this as possible (on enough nodes such that cores == num_workers), `examples/multiprocessing/numpy-mpi4py-examples/scipy_scale_work.py`:
```
dragon scipy_scale_work.py --dragon --num_workers=512 --size=256 --mem=8589934592 --iterations=1
```

* Multi-node tests, `test/multinode/*.py`.
    * 2 nodes with as many processes as possible. Manually adjust until the tests cease to function.



## pinoak - on 2 nodes with the release package 0.4 

bardpeak nodes

### test_connection.py

```
# sizes = [0.1, 1, 10, 50]
# num_processes = 10

> time dragon test_connection.py
.
----------------------------------------------------------------------
Ran 1 test in 20.138s
OK
real	0m35.988s
user	0m25.542s
sys	0m0.633s
```

```
# sizes = [0.1, 1, 10, 50, 100]
# num_processes = 10

> time dragon test_connection.py
.
----------------------------------------------------------------------
Ran 1 test in 50.221s
OK
real	0m56.292s
user	0m39.798s
sys	0m0.845s
```

At some point, after repeated iterations, I got a core dump.

```
# sizes = [0.1, 1, 10, 50, 100, 500]
# num_processes = 10

> time dragon test_connection.py
.
----------------------------------------------------------------------
Ran 1 test in 200.340s
OK
real	3m26.429s
user	2m26.311s
sys	0m1.780s
```

```
# sizes = [0.1, 1, 10, 50]
# num_processes = mp.cpu_count() * 2

> time dragon test_connection.py
Exception ignored in: <cyfunction Queue.__del__ at 0x7f22ce40dd40>
Traceback (most recent call last):
  File "dragon/native/queue.py", line 237, in dragon.native.queue.Queue.__del__
    
  File "dragon/native/queue.py", line 531, in dragon.native.queue.Queue._close
AttributeError: 'DragonQueue' object has no attribute '_buffer_pool'
E
======================================================================
ERROR: test_ring_multi_node (__main__.TestConnectionMultiNode)
This test creates 2 processes per cpu. They send increasingly large
----------------------------------------------------------------------
Traceback (most recent call last):
  File "dragon/native/queue.py", line 105, in dragon.native.queue.Queue.__init__
  File "dragon/globalservices/channel.py", line 54, in dragon.globalservices.channel.create
dragon.globalservices.channel.ChannelError: channel create GSChannelCreate: 512 4294967296->9223372036854775808 failed: GSChannelCreateResponse(tag=1033, err=1, ref=512, err_info="SHChannelCreate(tag=1032, p_uid=1, r_c_uid=2, m_uid=4611686018427387904, c_uid=9223372036854776320, options={'sattr': '', 'capacity': 100, 'block_size': 65536}) failed: Channel Library Error: Could not create Channel | Dragon Msg: Traceback (most recent call first):\n  hashtable.c: dragon_hashtable_get() (line 886) :: Hashtable key not found.\n  channels.c: dragon_channel_create() (line 1916) :: unable to allocate memory for channel from memory pool | Dragon Error Code: DRAGON_DYNHEAP_REQUESTED_SIZE_NOT_AVAILABLE")

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/cray/css/users/kalantzi/dragon/release0.4/dragon/multi-node/test_connection.py", line 98, in test_ring_multi_node
    qs = [mp.Queue() for i in range(num_processes)]
  File "/cray/css/users/kalantzi/dragon/release0.4/dragon/multi-node/test_connection.py", line 98, in <listcomp>
    qs = [mp.Queue() for i in range(num_processes)]
  File "dragon/mpbridge/context.py", line 722, in dragon.mpbridge.context.DragonContext.Queue
  File "dragon/mpbridge/queues.py", line 273, in dragon.mpbridge.queues.Queue
  File "dragon/mpbridge/queues.py", line 189, in dragon.mpbridge.queues.DragonQueue.__init__
  File "dragon/mpbridge/queues.py", line 84, in dragon.mpbridge.queues.PatchedDragonNativeQueue.__init__
  File "dragon/native/queue.py", line 109, in dragon.native.queue.Queue.__init__
dragon.native.queue.QueueError: Could not create queue
channel create GSChannelCreate: 512 4294967296->9223372036854775808 failed: GSChannelCreateResponse(tag=1033, err=1, ref=512, err_info="SHChannelCreate(tag=1032, p_uid=1, r_c_uid=2, m_uid=4611686018427387904, c_uid=9223372036854776320, options={'sattr': '', 'capacity': 100, 'block_size': 65536}) failed: Channel Library Error: Could not create Channel | Dragon Msg: Traceback (most recent call first):\n  hashtable.c: dragon_hashtable_get() (line 886) :: Hashtable key not found.\n  channels.c: dragon_channel_create() (line 1916) :: unable to allocate memory for channel from memory pool | Dragon Error Code: DRAGON_DYNHEAP_REQUESTED_SIZE_NOT_AVAILABLE")

----------------------------------------------------------------------
Ran 1 test in 0.197s

FAILED (errors=1)

real	0m6.347s
user	0m4.519s
sys	0m0.536s
```

#### With HSTA
```
# sizes = [0.1, 1, 10, 50]
# num_processes = 10

> time dragon test_connection.py
.
----------------------------------------------------------------------
Ran 1 test in 2.064s
OK
real	0m8.121s
user	0m6.109s
sys	0m0.614s
```

After running it multiple times, I eventually got a core dump and the test exited successfully.

The following also gives a core dump and the test ends successfully:

```
# sizes = [0.1, 1, 10, 50, 100, 500]

> time dragon test_connection.py
.
----------------------------------------------------------------------
Ran 1 test in 6.176s

OK

real	0m12.413s
user	0m8.794s
sys	0m0.611s
```

The same error as with tcp when:
```
# sizes = [0.1, 1, 10, 50]
# num_processes = mp.cpu_count() * 2

> dragon test_connection.py
Exception ignored in: <cyfunction Queue.__del__ at 0x7f22ce40dd40>
Traceback (most recent call last):
  File "dragon/native/queue.py", line 237, in dragon.native.queue.Queue.__del__
    
  File "dragon/native/queue.py", line 531, in dragon.native.queue.Queue._close
AttributeError: 'DragonQueue' object has no attribute '_buffer_pool'
E
======================================================================
ERROR: test_ring_multi_node (__main__.TestConnectionMultiNode)
This test creates 2 processes per cpu. They send increasingly large
----------------------------------------------------------------------
Traceback (most recent call last):
  File "dragon/native/queue.py", line 105, in dragon.native.queue.Queue.__init__
  File "dragon/globalservices/channel.py", line 54, in dragon.globalservices.channel.create
dragon.globalservices.channel.ChannelError: channel create GSChannelCreate: 512 4294967296->9223372036854775808 failed: GSChannelCreateResponse(tag=1033, err=1, ref=512, err_info="SHChannelCreate(tag=1032, p_uid=1, r_c_uid=2, m_uid=4611686018427387904, c_uid=9223372036854776320, options={'sattr': '', 'capacity': 100, 'block_size': 65536}) failed: Channel Library Error: Could not create Channel | Dragon Msg: Traceback (most recent call first):\n  hashtable.c: dragon_hashtable_get() (line 886) :: Hashtable key not found.\n  channels.c: dragon_channel_create() (line 1916) :: unable to allocate memory for channel from memory pool | Dragon Error Code: DRAGON_DYNHEAP_REQUESTED_SIZE_NOT_AVAILABLE")

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/cray/css/users/kalantzi/dragon/release0.4/dragon/multi-node/test_connection.py", line 98, in test_ring_multi_node
    qs = [mp.Queue() for i in range(num_processes)]
  File "/cray/css/users/kalantzi/dragon/release0.4/dragon/multi-node/test_connection.py", line 98, in <listcomp>
    qs = [mp.Queue() for i in range(num_processes)]
  File "dragon/mpbridge/context.py", line 722, in dragon.mpbridge.context.DragonContext.Queue
  File "dragon/mpbridge/queues.py", line 273, in dragon.mpbridge.queues.Queue
  File "dragon/mpbridge/queues.py", line 189, in dragon.mpbridge.queues.DragonQueue.__init__
  File "dragon/mpbridge/queues.py", line 84, in dragon.mpbridge.queues.PatchedDragonNativeQueue.__init__
  File "dragon/native/queue.py", line 109, in dragon.native.queue.Queue.__init__
dragon.native.queue.QueueError: Could not create queue
channel create GSChannelCreate: 512 4294967296->9223372036854775808 failed: GSChannelCreateResponse(tag=1033, err=1, ref=512, err_info="SHChannelCreate(tag=1032, p_uid=1, r_c_uid=2, m_uid=4611686018427387904, c_uid=9223372036854776320, options={'sattr': '', 'capacity': 100, 'block_size': 65536}) failed: Channel Library Error: Could not create Channel | Dragon Msg: Traceback (most recent call first):\n  hashtable.c: dragon_hashtable_get() (line 886) :: Hashtable key not found.\n  channels.c: dragon_channel_create() (line 1916) :: unable to allocate memory for channel from memory pool | Dragon Error Code: DRAGON_DYNHEAP_REQUESTED_SIZE_NOT_AVAILABLE")

----------------------------------------------------------------------
Ran 1 test in 0.197s

FAILED (errors=1)
```

The same error as above when
```
sizes = [0.1, 1, 10, 50]
num_processes = 100
```


```
# sizes = [0.1, 1, 10, 50]
# num_processes = 50

> time dragon test_connection.py
.
----------------------------------------------------------------------
Ran 1 test in 3.836s
OK
real	0m9.659s
user	0m6.855s
sys	0m0.534s
```

```
# sizes = [0.1, 1, 10, 50, 100]
# num_processes = 50

> time dragon test_connection.py
.
----------------------------------------------------------------------
Ran 1 test in 5.263s
OK
real	0m10.928s
user	0m7.919s
sys	0m0.633s
```

The following does not work with tcp:

```
# sizes = [0.1, 1, 10, 50, 100, 500]
# num_processes = 50

> time dragon test_connection.py
.
----------------------------------------------------------------------
Ran 1 test in 13.312s
OK
real	0m19.159s
user	0m13.659s
sys	0m0.537s
```



### test_pool.py

Runs as is, with default settings.
```
> time dragon test_pool.py -v -f
	test_apply (__main__.TestPoolMultiNode) ... ok
test_async (__main__.TestPoolMultiNode) ... ok
test_async_timeout (__main__.TestPoolMultiNode) ... ok
test_imap (__main__.TestPoolMultiNode) ... ok
test_imap_unordered (__main__.TestPoolMultiNode) ... ok
test_map (__main__.TestPoolMultiNode) ... ok
test_map_async (__main__.TestPoolMultiNode) ... ok
test_map_async_callbacks (__main__.TestPoolMultiNode) ... ok
test_starmap (__main__.TestPoolMultiNode) ... ok
test_starmap_async (__main__.TestPoolMultiNode) ... ok
test_strong_scalability (__main__.TestPoolScalingMultiNode)
Trivial process throughput/scalability benchmark: ... N=2560 int(cpus)=256 stop-start=33.294443725957535
N=1768 int(cpus)=176 stop-start=21.44994439004222
N=1222 int(cpus)=122 stop-start=16.849119380989578
N=844 int(cpus)=84 stop-start=14.822255352977663
N=583 int(cpus)=58 stop-start=13.374728536990006
N=403 int(cpus)=40 stop-start=12.88223663601093
N=278 int(cpus)=27 stop-start=12.900207425991539
N=192 int(cpus)=19 stop-start=12.795027130981907
N=132 int(cpus)=13 stop-start=12.7032122980454
N=91 int(cpus)=9 stop-start=12.61574960697908
N=63 int(cpus)=6 stop-start=12.692220458993688
N=43 int(cpus)=4 stop-start=12.575151488010306
N=30 int(cpus)=3 stop-start=12.53353498497745
N=20 int(cpus)=2 stop-start=11.499338100024033
N=14 int(cpus)=1 stop-start=14.523769963008817
N=10 int(cpus)=1 stop-start=10.517872483993415
ok
----------------------------------------------------------------------
Ran 11 tests in 260.866s
OK
real	4m26.646s
user	3m8.802s
sys	0m2.466s
```


### test_process.py

* Bard peak nodes

I got a core and a leftover dragon-infra-tc in the login node, in the first run of a new allocation.

```
# nchildren = mp.cpu_count()

> time dragon test_process.py -v -f
test_inception (__main__.TestProcessMultiNode)
Have processes spawn `nnew` processes, until `nchildren` have been ... ok
----------------------------------------------------------------------
Ran 1 test in 4.129s
OK
real	0m23.019s
user	0m16.964s
sys	0m0.815s


> file core
core: ELF 64-bit LSB core file x86-64, version 1 (SYSV), SVR4-style, from '/opt/cray/pe/python/3.9.13.1/bin/python -m dragon.cli dragon-tcp --no-tls 0 --i', real uid: 160151130, effective uid: 160151130, real gid: 12790, effective gid: 12790, execfn: '/opt/cray/pe/python/3.9.13.1/bin/python', platform: 'x86_64'
```

* Apollo nodes

```
# nchildren = mp.cpu_count() * 8

> time dragon test_process.py -v -f
test_inception (__main__.TestProcessMultiNode)
Have processes spawn `nnew` processes, until `nchildren` have been ... ok
----------------------------------------------------------------------
Ran 1 test in 21.430s
OK
real	0m27.343s
user	0m19.501s
sys	0m0.837s
```

The second run hung.


### test_queue.py

```
# nitems = 42

> time dragon test_queue.py -v -f
test_joinable (__main__.TestQueueMultiNode)
Test joinability of a multi-node queue ... ok
test_multi_producer_consumer (__main__.TestQueueMultiNode)
Start 2 processes per cpu and have each of them send 16 messages to ... ok
----------------------------------------------------------------------
Ran 2 tests in 21.354s
OK
real	0m30.460s
user	0m21.998s
sys	0m0.996s
```

After a couple of runs, it finishes and produces a core dump.


#### With HSTA
```
> time dragon test_queue.py -v -f
test_joinable (__main__.TestQueueMultiNode)
Test joinability of a multi-node queue ... Process DragonProcess-1:
Traceback (most recent call last):
  File "dragon/native/queue.py", line 442, in dragon.native.queue.Queue._call_read_adapter
  File "dragon/pydragon_channels.pyx", line 1951, in dragon.channels.Many2ManyReadingChannelFile.read
  File "dragon/pydragon_channels.pyx", line 1964, in dragon.channels.Many2ManyReadingChannelFile.readinto
  File "dragon/pydragon_channels.pyx", line 1869, in dragon.channels.Many2ManyReadingChannelFile._get_msg
dragon.channels.ChannelEmpty: Channel Library Error: Channel Empty | Dragon Msg: Traceback (most recent call first):
  channels_messages.c: dragon_channel_gatewaymessage_client_get_cmplt() (line 1734) :: There was an error returned from the remote side by the transport.
  channels.c: dragon_chrecv_get_msg_blocking() (line 3204) :: Unexpected error on waiting for the gateway send request message completion. | Dragon Error Code: DRAGON_CHANNEL_EMPTY

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/opt/cray/pe/python/3.9.13.1/lib/python3.9/multiprocessing/process.py", line 315, in _bootstrap
    self.run()
  File "/opt/cray/pe/python/3.9.13.1/lib/python3.9/multiprocessing/process.py", line 108, in run
    self._target(*self._args, **self._kwargs)
  File "/cray/css/users/kalantzi/dragon/release0.4/dragon/examples/multiprocessing/multi-node_tests/test_queue.py", line 16, in _joiner
    q.put(item)
  File "dragon/mpbridge/queues.py", line 111, in dragon.mpbridge.queues.PatchedDragonNativeQueue.put
  File "dragon/native/queue.py", line 498, in dragon.native.queue.Queue._put
  File "dragon/native/queue.py", line 564, in dragon.native.queue.Queue._reset_event_channel
  File "dragon/native/queue.py", line 444, in dragon.native.queue.Queue._call_read_adapter
_queue.Empty
FAIL

======================================================================
FAIL: test_joinable (__main__.TestQueueMultiNode)
Test joinability of a multi-node queue
----------------------------------------------------------------------
Traceback (most recent call last):
  File "/cray/css/users/kalantzi/dragon/release0.4/dragon/examples/multiprocessing/multi-node_tests/test_queue.py", line 115, in test_joinable
    self.assertTrue(p.exitcode == 0)
AssertionError: False is not true

----------------------------------------------------------------------
Ran 1 test in 2.918s

FAILED (failures=1)

real	0m10.664s
user	0m7.493s
sys	0m0.654s
```

Then, three successful runs and then a hang.

```
> time dragon test_queue.py -v -f
test_joinable (__main__.TestQueueMultiNode)
Test joinability of a multi-node queue ... ok
test_multi_producer_consumer (__main__.TestQueueMultiNode)
Start 2 processes per cpu and have each of them send 16 messages to ... ok
----------------------------------------------------------------------
Ran 2 tests in 5.618s
OK
real	0m11.242s
user	0m8.039s
sys	0m0.506s
```


## horizon

### Testing test/multi-node tests

3/14/2023

All tests run with TCP transport.

Running on Horizon with 2 nodes.

#### test_barrier.py

1 of 7 tests is not skipped. That test failed with too many open files.
Set ulimit with "ulimit -n 15000". CPU Count is 192. The standard test used 8 * CPU Count.
Did not finish in 3 minutes.

Changed ncpu to 8(2.3 s), then 16(4.134 s), 32(7.995 s), 64(15.708), 128(31.485s).
It ran successfully. Uncommenting other tests at 8 ncpu did not finish.

A core file was generated at some point in running the tests. I don't have full traceback,
but it was in _get_msg in dragon_fifolite_lock. Probably happened during teardown since no
error was signalled.

#### test_connection.py

Processes already set to 10 (in master). With 10 processes it ran in
20.699 seconds.

#### test_lock.py

8 processes doesn't complete in a reasonable amount of time. Ctl-C did not
take it down in a reasonable amount of time either.

#### test_machine.py

finished in 0.001 seconds.

#### test_pool.py

In two places, decreased the size of the pools. On line 45 set the ncpu
value to 8. On line 142 set the maxcpus value to

    maxcpus = np.flip(np.logspace(0, np.log10(20), num=16))

Missed changing this maxcpus on line 159 so it remained as originally specified. Ran
the test and it passed. Output from the test is provided at the bottom of this file/comment.
As modified it took 397.642s to run.

#### test_process.py

Ran with 8 processes, but fails assert that GS takes less than 2 seconds.
Ran in 3.965s. Removing that assert has the following results where all tests pass.

As written the mp.cpu_count() * 8 is equal to 1536 nchrildren on two nodes. That would make
this test run excessively long given present performance. The following values were tested.

nchildren =             Time of execution
************            *******************
8                       3.917s
16                      5.956s
32                      9.924s
64                      18.468s
128                     33.505s
256                     65.063s
512                     127.931s

### test_queue.py

This test passed but I had to modify the number of processes. Here are the
runs with processes. I modified the num_processes the same for both tests. It was
num_reader = num_writers on line 41 and num_processes on line 90. Otherwise with 2 nodes
on horizon there are 224 cpu's and on one of the tests 448 processes.

num_processes =             Time of execution
****************            *******************
8                           6.997s
16                          12.782s
32                          25.128s
64                          63.973s
128                         # The test_multi_producer_consumer did not complete in a
                            # reasonable amount of time.

#### Run of modified test_pool.py on horizon Passed.

(_env) klee@horizon:~/home/Repos/testing/hpc-pe-dragon-dragon/test/multi-node> dragon test_pool.py -v -f
test_apply (__main__.TestPoolMultiNode) ... ok
test_async (__main__.TestPoolMultiNode) ... ok
test_async_timeout (__main__.TestPoolMultiNode) ... ok
test_imap (__main__.TestPoolMultiNode) ... ok
test_imap_unordered (__main__.TestPoolMultiNode) ... ok
test_map (__main__.TestPoolMultiNode) ... ok
test_map_async (__main__.TestPoolMultiNode) ... ok
test_map_async_callbacks (__main__.TestPoolMultiNode) ... ok
test_starmap (__main__.TestPoolMultiNode) ... ok
test_starmap_async (__main__.TestPoolMultiNode) ... ok
test_strong_scalability (__main__.TestPoolScalingMultiNode)
Trivial process throughput/scalability benchmark: ... N=1920 int(cpus)=192 stop-start=68.22622263699304
N=1352 int(cpus)=135 stop-start=51.43630865000887
N=952 int(cpus)=95 stop-start=40.09825069300132
N=670 int(cpus)=67 stop-start=31.81652866798686
N=472 int(cpus)=47 stop-start=25.86190476399497
N=332 int(cpus)=33 stop-start=21.71195904299384
N=234 int(cpus)=23 stop-start=18.899837838020176
N=165 int(cpus)=16 stop-start=17.008500249008648
N=116 int(cpus)=11 stop-start=15.380329603998689
N=81 int(cpus)=8 stop-start=14.800603910989594
N=57 int(cpus)=5 stop-start=14.484850679000374
N=40 int(cpus)=4 stop-start=14.077756285987562
N=28 int(cpus)=2 stop-start=17.88346864300547
N=20 int(cpus)=2 stop-start=12.947332484996878
N=14 int(cpus)=1 stop-start=16.115036842005793
N=10 int(cpus)=1 stop-start=12.007811500981916
ok

----------------------------------------------------------------------
Ran 11 tests in 397.642s

## cfa6k

#### P2P
| Msglen [B] |  HSTA Lat [usec]     | HSTA BW [MiB/s]     | TCP Lat [Usec]     | TCP BW [MiB/s]        |
|:-----------|---------------------:|--------------------:|-------------------:|----------------------:|
| 2          | 192.02670082449913   | 0.08817286392858456 | 1559.8227764712647 | 0.0056089144470170445 |
| 4          | 164.0077476622537    | 0.05325227235509415 | 1647.6967750350013 | 0.011253250010941483  |
| 8          | 143.01575138233602   | 0.2337403453638114  | 1717.9816728457808 | 0.022434675891302467  |
| 16         | 143.94099998753518   | 0.47068635459414027 | 1706.9492518203333 | 0.043767088692781654  |
| 32         | 147.79345074202868   | 1.0319764568967138  | 1652.8093721717596 | 0.09035236022920166   |
| 64         | 174.25812839064747   | 2.1331015906917923  | 1717.5710498122498 | 0.18018104097202972   |
| 128        | 168.49682433530688   | 3.951731752406362   | 1818.0636776378378 | 0.35696530523579667   |
| 256        | 187.1902262791991    | 8.721753159155934   | 1700.2698732540011 | 0.7107901638875916    |
| 512        | 189.37162240035832   | 11.360904472916348  | 1460.699801100418  | 1.2935405400875744    |
| 1024       | 203.5374258412048    | 19.60850017533271   | 1653.045674902387  | 2.551396054337316     |
| 2048       | 192.84722511656582   | 41.62947221503056   | 1436.9202515808868 | 5.063157277061438     |
| 4096       | 196.78604730870575   | 78.26214474708416   | 1742.9054772946984 | 9.450996819546022     |
| 8192       | 202.81077595427632   | 144.24011247560546  | 1627.0992258796468 | 16.43300342098735     |
| 16384      | 212.68324635457247   | 282.8146698015057   | 2071.03242573794   | 28.330764784708602    |
| 32768      | 232.25799959618598   | 537.1044663587132   | 2707.4282494140793 | 39.89238138455791     |


#### SciPy Image Convolution
8 nodes. The largest job possible:
```shell
scipy_scale_work.py --dragon --num_workers=128 --size=256 --mem=268435456 --iterations=1

TCP w/ default GW capacity
--------------------------
Number of workers: 128
Number of images: 4096
Time for iteration 0 is 45.66205070800788 seconds
Time for iteration 1 is 87.06892795400927 seconds
Time for iteration 2 is 88.7391407739924 seconds
Average time: 88.74 second(s)
Standard deviation: 0.0 second(s)

HSTA w/ DRAGON_GW_CAPACITY=65536
--------------------------------
Number of workers: 128
Number of images: 4096
Time for iteration 0 is 55.37892966299842 seconds
Time for iteration 1 is 69.6687413329928 seconds
Time for iteration 2 is 72.2411004010064 seconds
Average time: 72.24 second(s)
```


### Multi-Node Tests
TCP data:
```
test_process.py -> nchildren==mp.cpu_count() completes but takes 256 s.
test_process.py -> nchildren==mp.cpu_count()/2 passes

test_connection.py -> up to 50MB okay. 30s

test_lock.py -> basic -> mp.cpu_count()/2
                contention ->mp.cpu_count()/4

test_pool.py -> okay as is:
Trivial process throughput/scalability benchmark: ...
N=1279 int(cpus)=127 stop-start=50.09385014500003
N=926 int(cpus)=92 stop-start=41.202914658002555
N=670 int(cpus)=67 stop-start=27.47458101600205
N=485 int(cpus)=48 stop-start=18.029168025997933
N=350 int(cpus)=35 stop-start=15.218694440001855
N=253 int(cpus)=25 stop-start=13.628033752000192
N=183 int(cpus)=18 stop-start=12.617709164002008
N=132 int(cpus)=13 stop-start=12.455216313002893
N=96 int(cpus)=9 stop-start=12.435480992000521
N=69 int(cpus)=6 stop-start=12.350599812001747
N=50 int(cpus)=5 stop-start=12.31817343900184
N=36 int(cpus)=3 stop-start=12.320295305002219
N=26 int(cpus)=2 stop-start=14.292485291996854
N=19 int(cpus)=1 stop-start=19.35346995799773
N=13 int(cpus)=1 stop-start=13.263360239001486
N=10 int(cpus)=1 stop-start=10.335744056999829

test_queue.py -> multi_producer_consumer -> cpu_count/4
                 joinable -> cpu_count / 2
```
