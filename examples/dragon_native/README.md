# Dragon Native API Examples

This directory contains examples showing how to use the Dragon native API in
practice.

## PI Demo

The file `pi_demo.py` contains a program that computes the value of constant π =
3.1415 ... to single precision using the standard Monte Carlo technique (see
Press et al. 1992, Numerical Recipes in C, section 7.6). It uses a number of
workers to parallelize the radius computation.

This example demonstrates two important paradigms of parallel programming with
Dragon:

1. How to execute Python processes using Dragon native
2. How to use Dragon native Queues to communicate objects between processes


### Example Output

#### Single node

The program can be run using 2 workers with `dragon pi-demo.py 2` and results in the following output:

```
> dragon pi_demo.py 2

pi-demo: Calculate π = 3.1415 ... in parallel using the Dragon native API.

Got num_workers = 2
0000: pi= 4.0000000, error= 2.7e-01
0512: pi= 3.1189084, error=-7.2e-03
1024: pi= 3.1531707, error= 3.7e-03
1536: pi= 3.1659076, error= 7.7e-03
2048: pi= 3.1449488, error= 1.1e-03
2560: pi= 3.1409606, error=-2.0e-04
3072: pi= 3.1389522, error=-8.4e-04
3584: pi= 3.1391911, error=-7.6e-04
4096: pi= 3.1354650, error=-2.0e-03
4608: pi= 3.1277934, error=-4.4e-03
5120: pi= 3.1267331, error=-4.7e-03
5632: pi= 3.1319013, error=-3.1e-03
6144: pi= 3.1446705, error= 9.8e-04
Final value after 6342 iterations: pi=3.141595711132135, error=9.732459548770225e-07
+++ head proc exited, code 0
```

#### Multi-node

In order to run the example on two nodes, we do:

```
> salloc --nodes=2 --exclusive

> dragon pi_demo.py 2
[stdout: p_uid=4294967296]
pi-demo: Calculate π = 3.1415 ... in parallel using the Dragon native API.

Got num_workers = 2
[stdout: p_uid=4294967296] 0000: pi= 4.0000000, error= 2.7e-01
[stdout: p_uid=4294967296] 0512: pi= 3.1189084, error=-7.2e-03
[stdout: p_uid=4294967296] 1024: pi= 3.1531707, error= 3.7e-03
[stdout: p_uid=4294967296] 1536: pi= 3.1659076, error= 7.7e-03
[stdout: p_uid=4294967296] 2048: pi= 3.1449488, error= 1.1e-03
[stdout: p_uid=4294967296] 2560: pi= 3.1409606, error=-2.0e-04
[stdout: p_uid=4294967296] 3072: pi= 3.1389522, error=-8.4e-04
[stdout: p_uid=4294967296] 3584: pi= 3.1391911, error=-7.6e-04
[stdout: p_uid=4294967296] 4096: pi= 3.1354650, error=-2.0e-03
[stdout: p_uid=4294967296] 4608: pi= 3.1277934, error=-4.4e-03
[stdout: p_uid=4294967296] 5120: pi= 3.1267331, error=-4.7e-03
[stdout: p_uid=4294967296] 5632: pi= 3.1319013, error=-3.1e-03
[stdout: p_uid=4294967296] 6144: pi= 3.1446705, error= 9.8e-04
[stdout: p_uid=4294967296] Final value after 6342 iterations: pi=3.141595711132135, error=9.732459548770225e-07
```