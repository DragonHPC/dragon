## SSH-based multi-node proxy suite

This set of tests is meant to run the multi-node tests in a proxy environment.
Passwordless ssh should be set up between the client and server. The server
should have a 4+ node allocation and should be launched before the client. The
server and client can be launched with their respective helper scripts. Note,
paths may need to be modified in those scripts for exit to work correctly.

From `test/proxy`, launch `./run_server.sh` on the server allocation first,
then `./run_client.sh` on the client. The client runs the existing multi-node
test list from `test/multi-node/Makefile` through `test_proxy_multinode.py`.

There are a subset of the multi-node tests that do fail in a proxy environment.
We are working to address these. Other tests are ill-defined because they try
to use local information or assume processes started as part of the test have
access to the same filesystem as the head process.

The currently failing list of tests are:
- test_connection.TestConnectionMultiNode.test_ring_multi_node
- test_fli.FLITest
- test_lock.TestLockMultiNode.test_lock_basic
- test_queue.TestQueueMultiNode.test_joinable

## Slurm channels-mesh proxy coverage

This is a separate harness for cross-runtime channels-mesh support. It starts
both runtimes within one Slurm allocation rather than SSH-launching the server.

With an active Slurm allocation, run from `test/multi-node`:

```console
make test_proxy_relay_matrix
make test_proxy_relay_matrix_two_multinode
```

The first command requires at least two allocated nodes and uses a multi-node
remote runtime with a single-node client to isolate proxy routing from
client-side multi-node startup. The second command requires at least four
allocated nodes, splits them into disjoint server and client runtimes, and
exercises the full multi-node-to-multi-node channels-mesh path. Both commands
run the six-cell relay matrix through the Make targets' client-driver and
success-marker overrides.

The test deliberately checks more than successful process creation. The relay
matrix covers remote-to-local, local-to-remote, and round-trip traffic at both
eager and rendezvous sizes. The companion object suite covers queue, DDict,
ProcessGroup, host placement, and cleanup behavior.

The server publishes its descriptor before waiting for the client to write the
exit sentinel. The runner bounds both client execution and server teardown so a
failed proxy operation cannot hold a Slurm allocation indefinitely. On a
parallel filesystem, the runner refreshes the publish directory while polling
because a newly-created descriptor can be hidden temporarily by a stale
directory entry.