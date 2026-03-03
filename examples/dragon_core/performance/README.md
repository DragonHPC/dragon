# p2p_ch_benchmark - A suite of point to point benchmarks

## Introduction
This is a suite of benchmarks testing aspects of the Dragon channels infrastructure, including:
* Message Latency (seconds) - Latency is the time to send a single message from one process to another.
* Message Bandwidth (bytes/second) - Message bandwidth is the bytes per second rate at which messages can be sent from one process to another. In this case, whatever library is sending the messages is free to optimize for the fact that lots of things will be sent. This test is done by defining a “window” of a certain size (e.g., 64 messages), sending all of them without any blocking behavior or synchronization, and then once they are all sent synchronizing with the receiving process to know they have all been received.
* Message Rate (msgs/second) - Message rate is the rate at which individual messages can be sent from one process to another. In this case, whatever library is sending the messages is free to optimize for the fact that lots of things will be sent. This test is done by defining a “window” of a certain size (e.g., 64 messages), sending all of them without any blocking behavior or synchronization, and then once they are all sent synchronizing with the receiving process to know they have all been received.

**NOTE:** This application requires Dragon Multinode support and a minimum of 2 allocated nodes in order to run. If more than 2 nodes are allocated, the number of allocated nodes must be a multiple of 2.

## Usage
```
usage: run_ch_benchmark.sh
```

## Example Output
```
~/hpc-pe-dragon-dragon/examples/dragon_core/performance> ./run_ch_benchmark.sh
Loading LMOD module files....
+---------------------------------------------+
| Auto-loading PrgEnv-gnu and craype-x86-rome |

Lmod is automatically replacing "cce/14.0.4" with "gcc/10.3.0".

+---------------------------------------------+
+----------------------------+
| Auto-loading cray-python   |
+----------------------------+
+----------------------------------+
| Auto-loading cray-cdst-support   |
+----------------------------------+
+-------------------------------+
| Auto-loading cray-cti-devel   |
+-------------------------------+

Due to MODULEPATH changes, the following have been reloaded:
  1) cray-mpich/8.1.17

/home/users/user/hpc-pe-dragon-dragon/examples/dragon_core/performance
[stdout: p_uid=4294967296] 0: Starting ch_p2p_latency
[stdout: p_uid=4294967296] 1: Starting ch_p2p_latency
[stdout: p_uid=4294967296] 1-stdout: Iterations = 30 MsgSize = 8, Lat = 0.001012 seconds
1-stdout: Iterations = 30 MsgSize = 64, Lat = 0.001009 seconds
1-stdout: Iterations = 30 MsgSize = 1024, Lat = 0.001040 seconds
1-stdout: Iterations = 30 MsgSize = 16384, Lat = 0.001528 seconds
1-stdout: Iterations = 30 MsgSize = 65536, Lat = 0.003617 seconds
1-stdout: Iterations = 30 MsgSize = 1048576, Lat = 0.039882 seconds
1-stdout: Iterations = 30 MsgSize = 2097152, Lat = 0.100548 seconds
1: app exited with 0
1: Done
1: Starting ch_p2p_msg_rate
[stdout: p_uid=4294967296] 0-stdout: Iterations = 30 MsgSize = 8, Lat = 0.001012 seconds
0-stdout: Iterations = 30 MsgSize = 64, Lat = 0.001009 seconds
0-stdout: Iterations = 30 MsgSize = 1024, Lat = 0.001042 seconds
0-stdout: Iterations = 30 MsgSize = 16384, Lat = 0.001528 seconds
0-stdout: Iterations = 30 MsgSize = 65536, Lat = 0.003614 seconds
0-stdout: Iterations = 30 MsgSize = 1048576, Lat = 0.039895 seconds
0-stdout: Iterations = 30 MsgSize = 2097152, Lat = 0.100591 seconds
0: app exited with 0
0: Done
0: Starting ch_p2p_msg_rate
[stdout: p_uid=4294967296] 1-stdout: MsgSize = 8, WindowSize = 64, Samps = 100, Msg/Sec = 210.308
1-stdout: MsgSize = 64, WindowSize = 64, Samps = 80, Msg/Sec = 1549.71
1-stdout: MsgSize = 256, WindowSize = 64, Samps = 60, Msg/Sec = 1481.67
1-stdout: MsgSize = 1024, WindowSize = 64, Samps = 40, Msg/Sec = 1670.42
1-stdout: MsgSize = 2048, WindowSize = 64, Samps = 30, Msg/Sec = 1622.13
1: app exited with 0
1: Done
1: Starting ch_p2p_bandwidth
[stdout: p_uid=4294967296] 0-stdout: MsgSize = 8, WindowSize = 64, Samps = 100, Msg/Sec = 210.309
0-stdout: MsgSize = 64, WindowSize = 64, Samps = 80, Msg/Sec = 1549.62
0-stdout: MsgSize = 256, WindowSize = 64, Samps = 60, Msg/Sec = 1481.53
0-stdout: MsgSize = 1024, WindowSize = 64, Samps = 40, Msg/Sec = 1670.41
0-stdout: MsgSize = 2048, WindowSize = 64, Samps = 30, Msg/Sec = 1622.14
0: app exited with 0
0: Done
0: Starting ch_p2p_bandwidth
[stdout: p_uid=4294967296]1-stdout: MsgSize = 8, WindowSize = 64, Samps = 100, Bandwidth = 12804.4
1-stdout: MsgSize = 64, WindowSize = 48, Samps = 50, Bandwidth = 100952
1-stdout: MsgSize = 1024, WindowSize = 32, Samps = 40, Bandwidth = 1.26108e+06
1-stdout: MsgSize = 16384, WindowSize = 32, Samps = 30, Bandwidth = 1.97167e+07
1-stdout: MsgSize = 65536, WindowSize = 32, Samps = 30, Bandwidth = 2.44142e+07
1-stdout: MsgSize = 1048576, WindowSize = 16, Samps = 20, Bandwidth = 2.40735e+07
1: app exited with 0
1: Done
[stdout: p_uid=4294967296] data=0-stdout: MsgSize = 8, WindowSize = 64, Samps = 100, Bandwidth = 12803.9
0-stdout: MsgSize = 64, WindowSize = 48, Samps = 50, Bandwidth = 100963
0-stdout: MsgSize = 1024, WindowSize = 32, Samps = 40, Bandwidth = 1.26097e+06
0-stdout: MsgSize = 16384, WindowSize = 32, Samps = 30, Bandwidth = 1.97151e+07
0-stdout: MsgSize = 65536, WindowSize = 32, Samps = 30, Bandwidth = 2.4416e+07
0-stdout: MsgSize = 1048576, WindowSize = 16, Samps = 20, Bandwidth = 2.40753e+07
0: app exited with 0
0: Done
```