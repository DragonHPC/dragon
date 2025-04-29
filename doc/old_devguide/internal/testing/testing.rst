.. _Testing:

Testing
+++++++

General Guidelines
==================

Dragon infrastructure and internal APIs need unit testing and integration testing; this page collects notes on
methodology and how to run them.

For now, we will base the testing on the unittest module, which seems to have enough features to handle what
we need to do. This decision could be revisited in the future if a different framework brings a feature we
need.

Generally speaking, writing some basic 'smoke tests' of a new feature or component is the responsibility of
the author of the feature or component.  Such tests should include whatever is appropriate for hello world but
don't need to be comprehensive.  They should illustrate a 'happy path' case and prove that something is
working and should be easy to set up and run.

These author's tests should have some comments in them and be written to be as transparent as possible, so
they can serve as a simple usage example as much as a test case.  Author's tests for C APIs don't need to be
wrapped in Cython and can exist as separate runnable projects.

The bulk of other test cases exist more to be run than to be read, and are trying to cover more of the
functionality, so they might include more common and support setup routines.

Generally speaking, error paths need to be tested as much as successful cases.  Particularly when what is
being tested is a server or an API managing shared state, it's necessary to verify that the response to a bad
request or call indicates what has gone wrong and that the component being tested isn't in a bad state.

Where feasible tests should exercise the limits of the implementation.  For example, Global Services needs to
keep track of processes, so there should be at least one test trying to approach the number of processes that
might be alive in a system or the limit of a single Global Services process.  The test of an API managing
memory should include a case managing a large fraction of the memory available on a node.

Such tests can find scaling issues and performance issues early, and serve as an ongoing performance
regression.

Ideally, bug repeaters should be provided in the form of a failing test case.

Acceptance Tests
================
.. Test the desired properties of the runtime

Qualification Tests
===================
.. Test all components of the runtime at once

Integration Tests
=================
.. Test selected APIs and connected components of the runtime

Unit Tests
==========
.. Test part of a components, classes, methods, functions on their own




C Tests
========

C tests that should be automatically run (via the `test_c_files.py`) should be named `test_*.c`.  If the test is more
performance based or a benchmark of some kind, then it should be named `perf_*.c`.  Perf tests will not be run by
the script automatically.

Utilize the `_ctest_utils.h` header file for general util functions and macro headers.  The most useful
will likely be the `err_fail` and `main_err_fail` which will report back the last dragon error code
and message, with full traceback including calling function and failing line number.  If you find theres a
useful function to be added to this header, feel free to do so.

General patterns should make use of `goto` statements for shmem cleaup of memory pools, the `main_err_fail`
macro is mostly used for this purpose in the `main` function to do automatic cleanup on failed tests.

Ex:


.. code-block:: c
    :linenos:

    dragonError_t derr = dragon_memory_pool_create(&mpool, ... );
    if (derr != DRAGON_SUCCESS)
        err_fail(derr, "Failed to create memory pool"); // Exits test with FAILED

    // do some testing...
    derr = dragon_some_func_call();
    if (derr != DRAGON_SUCCESS)
        main_err_fail(derr, "Failed to do FOOBAR", jmp_destroy_pool); // Sets TEST_STATUS to FAILED, jumps to goto label

    // more testing...

    // pool cleanup
    jmp_destroy_pool:
    derr = dragon_memory_pool_destroy(&mpool);
    if (derr != DRAGON_SUCCESS)
        err_fail(derr, "Could not destroy pool");

    return TEST_STATUS;

Tests should adhere to either a "all or nothing" or "expected vs passed" style.  The former should exit and
jump to cleanup (if applicable) on any failure, and the latter should be a series of "mini-tests" that exists with a return code
based on whether or not the expected number of tests passed or not.

Multiprocessing tests can make use of simple `fork()` calls after pool or channel creation since the `umap` will spawn them with
all necessary information, there's no particular need to serialize data and call `attach` unless that's what is being tested.