.. _ChannelsPerformance:


Channels Performance Testing
============================

**FIXME: This needs to be move to the Qualification Test section !**

Channels is not a drop-in replacement for other communication libraries, such as MPI.  Although it presents
different capabilities it should be competitve with traditional HPC communication libraries on key metrics.
Here we establish what those metrics are and how they are measured so they can be compared with MPI.

Key Metrics
-----------

The key metrics we need to measure are latency, message rate, and bandwidth across message sizes. Here is
pseudo code for each to show how they are to be measured.

Latency
^^^^^^^

Latency is the time to send a single message from one process to another.  Typically outside of MPI this is
measured as round-trip latency, but here we will measure the round trip latency and divide by 2 to get the
uni-directional latency.

.. code-block:: c

    /* On process 0
    ch0 = send channel
    ch1 = recv channel
    */
    for (j = 0; j < NSIZES; j++) {

        // prepare 2 messages (for send and recv) with the #bytes of the current j loop

        t0 = now();
        for (i = 0; i < NSAMPS; i++) {

            // this can be any form of non-blocking or blocking
            dragon_chsend_send_msg(&ch0,...);

            // this has to block
            dragon_chrecv_get_msg(&ch1, ...);
        }

        t1 = now();

        // yes, we divide by 2 because in MPI land we do 1/2 round trip latency for some reason
        avg_lat = (t1 - t0) / (2 * NSAMPS);
        printf("MsgSize = %i, Lat = %f\n", msg_lens[j], avg_lat);

    }

    /* On process 1
    ch0 = recv channel
    ch1 = send channel
    */
    for (j = 0; j < NSIZES; j++) {

        // prepare 2 messages (for send and recv) with the size of the current j loop

        t0 = now();
        for (i = 0; i < NSAMPS; i++) {

            // this has to block
            dragon_chrecv_get_msg(&ch0,...);

            // this can be any form of non-blocking or blocking
            dragon_chsend_send_msg(&ch1, ...);
        }

        t1 = now();

        // yes, we divide by 2 because in MPI land we do 1/2 round trip latency for some reason
        avg_lat = (t1 - t0) / (2 * NSAMPS);
        printf("MsgSize = %i, Lat = %f\n", msg_lens[j], avg_lat);

    }

Message Rate
^^^^^^^^^^^^

Message rate is the rate at which messages can be sent from one process to another.  In this case, whatever library is
sending the messages is free to optimize for the fact that lots of things will be sent.  This test is done by defining
a "window" of a certain size (e.g., 64 messages), sending all of them without any blocking behavior or synchronization,
and then once they are all sent synchronizing with the receiving process to know they have all been received.

.. code-block:: c

    /* On process 0
    ch0 = send channel
    ch1 = recv channel
    */
    for (j = 0; j < NSIZES; j++) {

        /* prepare WINDOWSIZE messages with the #bytes of the current j loop.  These can all be of the same memory
           prepare a single recv message that will come from the other process when it has received all WINDOWSIZE
           messages
        */

        t0 = now();
        for (i = 0; i < WINDOWSIZE; i++) {

            // this can be any form of non-blocking or blocking
            dragon_chsend_send_msg(&ch0,...);
        }

        // this has to block
        dragon_chrecv_get_msg(&ch1, ...);

        t1 = now();

        rate = WINDOWSIZE / (t1 - t0);
        printf("MsgSize = %i, Msgsps = %f\n", msg_lens[j], rate);

    }

    /* On process 1
    ch0 = recv channel
    ch1 = send channel
    */
    for (j = 0; j < NSIZES; j++) {

        // prepare 2 messages (for send and recv) with the size of the current j loop

        nrecvd = 0;
        while (nrecvd < WINDOWSIZE) {

            // this can be any form of non-blocking or blocking.  all are interesting
            ierr = dragon_chrecv_get_msg(&ch0,...);

            if (ierr == DRAGON_SUCCESS)
                nrecvd++;

        }

        // this can be any form of non-blocking or blocking
        dragon_chsend_send_msg(&ch1, ...);

        // message rate can only be measured by sending process(es), so nothing to report here

    }

Bandwidth
^^^^^^^^^

Bandwidth is measured in the same way as message rate but includes the message size in the performance calculation.

.. code-block:: c

    /* On process 0
    ch0 = send channel
    ch1 = recv channel
    */
    for (j = 0; j < NSIZES; j++) {

        /* prepare WINDOWSIZE messages with the #bytes of the current j loop.  These can all be of the same memory
           prepare a single recv message that will come from the other process when it has received all WINDOWSIZE
           messages
        */

        t0 = now();
        for (i = 0; i < WINDOWSIZE; i++) {

            // this can be any form of non-blocking or blocking
            dragon_chsend_send_msg(&ch0,...);
        }

        // this has to block
        dragon_chrecv_get_msg(&ch1, ...);

        t1 = now();

        bw = (msg_lens[j] * WINDOWSIZE) / (t1 - t0);
        printf("MsgSize = %i, Bps = %f\n", msg_lens[j], bw);

    }

    /* On process 1
    ch0 = recv channel
    ch1 = send channel
    */
    for (j = 0; j < NSIZES; j++) {

        // prepare 2 messages (for send and recv) with the size of the current j loop

        nrecvd = 0;
        while (nrecvd < WINDOWSIZE) {

            // this can be any form of non-blocking or blocking.  all are interesting
            ierr = dragon_chrecv_get_msg(&ch0,...);

            if (ierr == DRAGON_SUCCESS)
                nrecvd++;

        }

        // this can be any form of non-blocking or blocking
        dragon_chsend_send_msg(&ch1, ...);

        // BW can only be measured by sending process(es), so nothing to report here

    }
