.. _DragonCoreBCast:

Broadcast
+++++++++

.. contents:: Table of Contents
    :local:

The BCast object is an any to many payload broadcaster.

Description
===========

A BCast object is used for synchronization and communication from a process to one or more processes. Unlike
channels, it is not designed to be used in synchronization/communication between two processes, which could be
done using a channel and by sending a message from a sender to a receiver. Using a BCast object, many
processes can wait on the BCast object until a process has notified them that a payload is available via a
call to a trigger function. The payload is optional. The BCast object provides an efficient any to many
synchronization/communication structure.

.. figure:: image/bcast.png
    :scale: 75%
    :name: bcast 

    **An Any to Many Broadcast Synchronization Object**

A BCast object is meant to be shared by multiple threads/processes. The object is first created by a process.
Then a serialized descriptor to it can be shared with other processes. Via this serialized descriptor other
processes may attach to the same BCast Object. So there is one *create* call and perhaps many
*attach* calls using the API below. When the BCast object is no longer
needed, all processes should *detach* or *destroy* object. The *destroy* API call should only be called once
per object.

The object can be triggered to provide its payload to waiters. There are four options for waiting.

    * Idle Wait - A process sleeps via a futex until it is triggered via a syscall. This is relatively
      expensive, but has the advantage of completely suspending the process.
    * Spin Wait - A process sleeps by looping while taking advantage of any processor specific ability
      to relenquish cycles to other processes while it it spinning.
    * Asynchronously Wait for a callback - The process continues executing and a thread handles the callback.
    * Asynchronously Wait for a signal - A signal handler for the signal must be installed by the programmer.

When creating a BCast object, the programmer must decide on the maximum sized payload that could be provided to waiters.
The programmer must also decide on the maximum number of spin waiters that will be allowed to spin on its trigger event.

Triggering processes may trigger one or all processes that are waiting on a BCast object.

    * When triggering occurs, all new waiters must wait until triggering is complete.
    * Only current waiters will be triggered.
    * The triggering process provides the payload to be distributed when trigger is called.
    * The triggering process wakes up all waiters or one waiter.
    * Each triggered waiter process returns from its BCast wait primitive, but before it returns
      it copies the payload from the BCast object into the heap of the waiting process and provides
      a pointer to the payload to the triggered waiting process.


.. figure:: image/bcastflow.svg
    :scale: 75%
    :name: bcast-flow 

    **Operations on a BCast Object**

The flow diagram in :numref:`bcast-flow` shows an interaction with a BCast object and points out a few features/semantics
of these synchronization/communication objects. The flow of interaction proceeds as follows:

    #. The process T1 creates the BCast object and through some means, communicates its location to all the other
       processes in this interaction.
    #. In step 2 all the waiter processes begin to wait on the BCast object.
    #. In step 3 Trigger One is called before W4 can initiate its wait. W4 must wait until triggering is complete.
    #. At step 4 W1, W3, and W4 are waiters and Trigger All is called. The payload will be copied into the local heap of all
       the processes.
    #. While triggering is still happening, W2 wants to wait, but it will not be a waiter until triggering is complete.
    #. It is also possible to sign up for an asynchronous notification via either a callback or a signal. In this step, the
       process W3 signs up for a callback. The callback is called as a thread under the W3 process.
    #. When Trigger All is called the callback to cb2 is initiated in a thread of process W3. Process W2 is unblocked as well.
    #. When Trigger One is called by T4 there are no waiters on the object. Without a waiter, the Trigger call is invalid
       and is rejected.

User API
========

.. doxygenfile:: bcast.c


Example
=======

.. FIXME - This example is not complete. Placeholder only.

This creates a BCast object. 

.. code-block:: C
    :linenos:
    :caption: **A BCast Example**

    #include <dragon/bcast.h>
    #include <dragon/return_codes.h>
    #include <stdio.h>
    #include <stdlib.h>
    #include <time.h>

    #define TRUE 1
    #define FALSE 0

    #define SERFILE "bcast_serialized.dat"
    #define MFILE "bcast_test"
    #define M_UID 0

    #define FAILED 1
    #define SUCCESS 0

    int create_pool(dragonMemoryPoolDescr_t* mpool) {
        /* Create a memory pool to allocate messages and a Channel out of */
        size_t mem_size = 1UL<<31;
        printf("Allocating pool of size %lu bytes.\n", mem_size);

        dragonError_t derr = dragon_memory_pool_create(mpool, mem_size, MFILE, M_UID, NULL);
        if (derr != DRAGON_SUCCESS) {
            char * errstr = dragon_getlasterrstr();
            printf("Failed to create the memory pool.  Got EC=%i\nERRSTR = \n%s\n",derr, errstr);
            return FAILED;
        }

        return SUCCESS;
    }

    void check_result(dragonError_t err, dragonError_t expected_err, int* tests_passed, int* tests_attempted) {
        (*tests_attempted)++;

        if (err != expected_err) {
            printf("Test %d Failed with error code %s\n", *tests_attempted, dragon_get_rc_string(err));
            printf("%s\n", dragon_getlasterrstr());
        }
        else
            (*tests_passed)++;
    }

    int main(int argc, char* argv[]) {

        timespec_t t1, t2;
        int tests_passed = 0;
        int tests_attempted = 0;
        dragonMemoryPoolDescr_t pool;

        if (create_pool(&pool) != SUCCESS) {
            printf("Could not create memory pool for bcast tests.\n");
            return FAILED;
        }

        dragonBCastDescr_t bd, bd2;
        dragonBCastSerial_t bd_ser;

        dragonError_t err;

        // create in memory pool
        err = dragon_bcast_create(&pool, 128, 10, NULL, &bd);

        check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

        // destroy from memory pool
        err = dragon_bcast_destroy(&bd);

        check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

        // destroy already destroyed bcast object
        err = dragon_bcast_destroy(&bd);

        check_result(err, DRAGON_MAP_KEY_NOT_FOUND, &tests_passed, &tests_attempted);

        size_t sz;
        err = dragon_bcast_size(256, 10, NULL, &sz);

        check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

        tests_attempted++;

        if (sz < 256)
            printf("Test %d Failed. The required size was too small.\n", tests_attempted);
        else
            tests_passed++;

        void* ptr = malloc(sz);

        err = dragon_bcast_create_at(ptr, 256, 10, NULL, &bd);

        check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

        err = dragon_bcast_serialize(&bd, &bd_ser);

        check_result(err, DRAGON_BCAST_NOT_SERIALIZABLE, &tests_passed, &tests_attempted);

        err = dragon_bcast_destroy(&bd);

        check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

        // create in memory pool
        err = dragon_bcast_create(&pool, 128, 10, NULL, &bd);

        check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

        err = dragon_bcast_serialize(&bd, &bd_ser);

        check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

        err = dragon_bcast_attach(&bd_ser, &bd2);

        check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

        err = dragon_bcast_detach(&bd2);

        check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

        // This won't succeed because detaching the same BCast object just prior
        // to this call, removes it from the umap. You can't detach and destroy
        // the same BCast object.
        err = dragon_bcast_destroy(&bd);

        check_result(err, DRAGON_MAP_KEY_NOT_FOUND, &tests_passed, &tests_attempted);

        err = dragon_bcast_attach(&bd_ser, &bd2);

        check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

        err = dragon_bcast_serial_free(&bd_ser);

        check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

        err = dragon_bcast_destroy(&bd2);

        check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

        dragon_memory_pool_destroy(&pool);

        printf("Passed %d of %d tests.\n", tests_passed, tests_attempted);

        return 0;
    }