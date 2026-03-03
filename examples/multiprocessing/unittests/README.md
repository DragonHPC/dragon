# Python Multiprocessing Unit Tests using Dragon

This directory contains the Python Multiprocessing unit tests applicable to Dragon. These tests are among the
most complex in the whole CPython distribution. We include the Python 3.9.1 source in the ```orig/```
directory as a reference.

We refactored the existing unit tests into separate files to better expose the public API of multiprocessing.
The aim was to keep changes to an absolute minimum, but increase maintainability.  Modifications were
annotated with the keyword `DRAGON` to destinguish them from the existing comments.

The original tests are grouped by `TYPE` keyword: `processes, managers, threads`. We have instead split them
by API call, so that all types belonging to the same API call are located in the same file.

Some of the original tests are not associated with a `TYPE` keyword, i.e. do not inherit a Mixin. They can be
found in `test_others.py`

Some of the original tests do not pertain to Dragon, i.e. cannot succeed under any circumstance. We included
them here for completeness and added a `unittest.skip` to these tests, so they are not run.

## Running the tests

### Single node

All tests can be run by typing
```
make all
```
Whole API tests can be run with e.g.
```
make pool
```
Targets are given in the next section.

Test cases can be run simply by giving them to make:

```
make WithManagerTestPool
```

Specific tests can be run by giving their full reference:

```
make WithProcessesTestListener.test_context
```

### Multi-node

These unittests do not work multi-node, as this is out of scope (Python Multiprocessing does not work on multiple nodes).

## Test Cases Listing & Results

The following sections list all multiprocessing unittest test cases for the `spawn` start method under
consideration for `dragon`. The tables contain the original name without the type prefix, the file name, make
target and the number of tests in the file.  We also include the current results: The number of test cases not
implemented with Dragon as "Missing", the number of failed tests, the number of tests with an error and the
number skipped tests.


### Processes

Unit tests with the prefix `WithProcesses` and `TYPE=processes` in the original multiprocessing tests.

| Original Test Name without Prefix                       	| Makefile Target 	| File                  	| Total 	|  Missing  	| Failed 	| Error 	| Skipped 	|
|---------------------------------------------------------	|-----------------	|-----------------------	|:-----:	|:-----------:	|:------:	|:-----:	|:-------:	|
| TestArray                                               	| array           	| test_array.py         	|   4   	|      4      	|    0   	|   0   	|    4    	|
| TestBarrier                                             	| barrier         	| test_barrier.py       	|   11   	|      10      	|    0   	|   0   	|    10    	|
| TestCondition,                                          	| condition       	| test_condition.py     	|   7   	|      2      	|    0   	|   0   	|    2    	|
| TestConnection,  TestPicklingConnections                	| connection      	| test_connection.py    	|   8   	|      0      	|    1   	|   3   	|    4    	|
| TestListener, TestListenerClient                        	| listener       	| test_listener.py      	|   6   	|      0      	|    0   	|   0   	|    0    	|
| TestFinalize, TestImportStar,  TestLogging              	| finalize        	| test_finalize.py      	|   5   	|      1      	|    0   	|   0   	|    1    	|
| TestEvent                                               	| event           	| test_event.py         	|   1   	|      0      	|    0   	|   0   	|    0    	|
| TestLock, TestEvent                                     	| lock            	| test_lock.py          	|   3   	|      0      	|    0   	|   0   	|    0    	|
| TestManagerRestart                                      	| manager        	| test_manager.py       	|   1   	|      0      	|    0   	|   0   	|    0    	|
| TestPoll, TestPollEintr                                 	| poll            	| test_poll.py          	|   5   	|      0      	|    0   	|   1   	|    2    	|
| TestPool, TestPoolWorkerErrors,  TestPoolWorkerLifetime 	| pool            	| test_pool.py          	|   30   	|      0      	|    0   	|   1   	|    1    	|
| TestProcess, TestSubclassingProcess                     	| process         	| test_process.py       	|   21  	|      0     	|    1   	|   0   	|    2    	|
| TestQueue                                               	| queue           	| test_queue.py         	|   10   	|      1      	|    0   	|   0   	|    1    	|
| TestSemaphore                                           	| semaphore       	| test_semaphore.py     	|   3   	|      0      	|    0   	|   0   	|    0    	|
| TestSharedCTypes                                        	| shared_ctypes   	| test_shared_ctypes.py 	|   3   	|      0      	|    0   	|   2   	|    2    	|
| TestSharedMemory, TestHeap                              	| shared_memory   	| test_shared_memory.py 	|   2   	|      0      	|    0   	|   2   	|    2    	|
| TestValue                                               	| value           	| test_value.py         	|   3   	|      0     	|    0   	|   2   	|    2    	|

### Manager

Unit tests with `TYPE=manager` have the prefix `WithManager` in the original multiprocessing tests.

| Original Test Name without Prefix                    	| Makefile Target 	| File               	| Total 	| Missing    	| Failed 	| Error 	| Skipped 	|
|------------------------------------------------------	|-----------------	|--------------------	|:-----:	|:-----------:	|:------:	|:-----:	|:-------:	|
| TestBarrier                                          	| barrier         	| test_barrier.py    	|   11   	|      11      	|    0   	|   0   	|    0    	|
| TestCondition                                        	| condition       	| test_condition.py  	|   7   	|      7      	|    0   	|   0   	|    0    	|
| TestContainers                                       	| containers      	| test_containers.py 	|   7   	|      7       	|    0    	|   0    	|    0     	|
| TestEvent                                            	| event           	| test_event.py      	|   1   	|      1       	|    0   	|   0   	|    0     	|
| TestLock                                             	| lock            	| test_lock.py       	|   3   	|      3       	|    0    	|   0    	|    0     	|
| TestMyManager, TestManagerRestart, TestRemoteManager 	| manager         	| test_manager.py    	|   6   	|      6       	|    0    	|   0    	|    0     	|
| TestPool                                             	| pool            	| test_pool.py       	|   25   	|      25      	|    0    	|   0    	|    0     	|
| TestQueue                                            	| queue           	| test_queue.py      	|   10   	|      10      	|    0    	|   0    	|    0     	|
| TestSemaphore                                        	| semaphore       	| test_semaphore.py  	|   3   	|      3       	|    0    	|   0    	|    0     	|

### No Type

Unit tests  that are not associated with any `TYPE` and do not have a prefix in the
original multiprocessing tests. They share the Makefile target `others` and reside in `test_others.py`.

| Original Test Name         	| MakefileTarget 	| File           	| Total 	| Missing 	| Failed 	| Error 	| Skipped 	|
|----------------------------	|----------------	|----------------	|-------	|---------	|--------	|-------	|---------	|
| MiscTestCase               	| others         	| test_others.py 	| 1     	| 0       	| 0      	| 0     	| 0       	|
| OtherTest         			  	| others         	| test_others.py 	| 2     	| 0       	| 0      	| 0     	| 0       	|
| TestCloseFds               	| others         	| test_others.py 	| 1     	| 0       	| 0      	| 0     	| 0       	|
| TestFlags                  	| others         	| test_others.py 	| 1     	| 0       	| 0      	| 0     	| 0       	|
| TestForkAwareThreadLock    	| others         	| test_others.py 	| 1     	| 0       	| 0      	| 0     	| 0       	|
| TestIgnoreEINTR            	| others         	| test_others.py 	| 2     	| 1       	| 0      	| 1     	| 2       	|
| TestInitializers        		| others         	| test_others.py 	| 2     	| 0       	| 0      	| 0     	| 0       	|
| TestInvalidFamily          	| others         	| test_others.py 	| 2     	| 0       	| 0      	| 0     	| 0       	|
| TestInvalidHandle          	| others         	| test_others.py 	| 1     	| 0       	| 0      	| 0     	| 0       	|
| TestNoForkBomb             	| others         	| test_others.py 	| 1     	| 0       	| 1      	| 0     	| 1       	|
| TestPoolNotLeakOnFailure   	| others         	| test_others.py 	| 1     	| 0       	| 0      	| 0     	| 0       	|
| TestResourceTracker        	| others         	| test_others.py 	| 5     	| 0       	| 1      	| 0     	| 1       	|
| TestSimpleQueue            	| others         	| test_others.py 	| 3     	| 1       	| 0      	| 0     	| 1       	|
| TestStartMethod            	| others         	| test_others.py 	| 4     	| 0       	| 0      	| 1     	| 1       	|
| TestStdinBadfiledescriptor 	| others         	| test_others.py 	| 3     	| 0       	| 0      	| 0     	| 0       	|
| TestSyncManagerTypes       	| others         	| test_others.py 	| 15      | 15       	| 0      	| 0     	| 15       	|
| TestTimeouts               	| others         	| test_others.py 	| 1     	| 0       	| 0      	| 0     	| 0       	|
| TestWait                   	| others         	| test_others.py 	| 7     	| 0       	| 1      	| 0     	| 1       	|

### Threads

Unit tests with `TYPE=threads` have the prefix `WithThreads` in the original multiprocessing tests.


| Original Test Name without Prefix 	| Makefile Target 	| File               	| Total 	| Missing   	| Failed 	| Error 	| Skipped 	|
|-----------------------------------	|-----------------	|--------------------	|:-----:	|:-----------:	|:------:	|:-----:	|:-------:	|
| TestBarrier                       	| barrier         	| test_barrier.py    	|   11   	|      11      	|    0   	|   0   	|    0    	|
| TestCondition                     	| condition       	| test_condition.py  	|   7   	|      7      	|    0   	|   0   	|    0    	|
| TestConnection                    	| connection      	| test_connection.py 	|   8   	|      8      	|    0   	|   0   	|    0    	|
| TestEvent                         	| event           	| test_event.py      	|   1   	|      1      	|    0   	|   0   	|    0    	|
| TestListenerClient                	| listener        	| test_listener.py   	|   3   	|      3      	|    0   	|   0   	|    0    	|
| TestLock                          	| lock            	| test_lock.py       	|   3   	|      3      	|    0   	|   0   	|    0    	|
| TestManagerRestart                	| manager         	| test_manager.py    	|   1   	|      1      	|    0   	|   0   	|    0    	|
| TestPoll                          	| poll            	| test_poll.py       	|   4   	|      4      	|    0   	|   0   	|    0    	|
| TestPool                          	| pool            	| test_pool.py       	|   25   	|      25      	|    0   	|   0   	|    0    	|
| TestProcess                       	| process         	| test_process.py    	|   8   	|      8      	|    0   	|   0   	|    0    	|
| TestQueue                         	| queue           	| test_queue.py      	|   10   	|      10      	|    0   	|   0   	|    0    	|
| TestSemaphore                     	| semaphore       	| test_semaphore.py  	|   3   	|      3      	|    0   	|   0   	|    0    	|



