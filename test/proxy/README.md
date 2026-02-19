This set of tests is meant to run the multi-node tests in a proxy environment. Passwordless ssh should be set up between the client and server. The server should have a 4+ node allocation and should be launched before the client. The server and client can be launched with their respective helper scripts. Note, paths may need to be modified in those scripts.

There are a subset of the mutli-node tests that do fail in a proxy environment. We are working to address these. Other tests are ill-defined because they try to use local information or assume processes started as part of the test have access to the same filesystem as the head process.

The currently failing list of tests are:
- test_connection.TestConnectionMultiNode.test_ring_multi_node
- test_fli.FLITest
- test_lock.TestLockMultiNode.test_lock_basic
- test_queue.TestQueueMultiNode.test_joinable