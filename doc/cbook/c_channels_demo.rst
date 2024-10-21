.. _channels_example:

How To Use The C Channels API
++++++++++++++++++++++++++++++++++

The main purpose of this example is to demonstrate how you can program directly
to the channels API in the C language. However, there is some bootstrap code that is
written in Python to get everything started. The Python program bootstraps a C
example of using the Channels API. The Python code given here shows how the
program is started in the usage function in :numref:`ring_bootstrap`. A simple
run is given in :numref:`ring_sample_run`. The program starts up a specified
number of ring processes, potentially on different nodes of a multi-node
allocation. Each ring process runs a specified number of iterations of passing a
message around the ring. The final ring process receives a message from the
previous ring process and forwards it back to the beginning of the ring to be
sent around atain. The program reports the average time it takes to forward the
message from once ring process to the next in the ring.

.. code-block:: shell
    :name: ring_sample_run
    :caption: A Sample Run of the Ring Demo

    (_env) root ➜ .../examples/dragon_core $ dragon ring.py 2 100
    Ring proc exited...
    Ring proc exited...
    Test Passed.
    The average time per message transfer was 12.092739925719798 microseconds.
    Main proc exiting...
    +++ head proc exited, code 0
    (_env) root ➜ .../hpc-pe-dragon-dragon/examples/dragon_core $

The bootstrap code shown in :numref:`ring_bootstrap` demonstrates how a process
can be started on each node of an allocation or cluster. The default placement
strategy of round-robin means that each process is started on a different node.
The *start_ringproc* function then uses
`subprocess.Popen <https://docs.python.org/3/library/subprocess.html>`_
to start a second process on
the node. The standard output from *ringproc* is the serialized descriptor of a
channel that the *ringproc* instance will send messages to. That serialized
descriptor is fed back to the main program's process to be provided as the
receive channel for the next process in the ring of processes.

The design of this bootstrap program allows for the program to be started with as
many processes and iterations as desired. So all or some subset of nodes may be
used from a cluster or allocation. Or you can start more processes than nodes
that are available in the cluster/allocation and the ring will simply overlap
some nodes using the round-robin placement of processes.

The bootstrap application sends the message around the ring the number of
*iteration* times and it times that total time and computes the average time it
takes for a message transfer between channels. Note that the send channel for
each *ringproc* co-exists on the same node as the *ringproc* process instance. So
not only are the *ringprocs* distributed across nodes, but their send channels
for the ring have a similar distribution across nodes.

.. code-block:: python
    :name: ring_bootstrap
    :caption: Ring Demo Bootstrap Code
    :linenos:

    import dragon
    import multiprocessing as mp
    import subprocess
    import sys
    import dragon.channels as dch
    import dragon.managed_memory as dm
    import dragon.infrastructure.parameters as dp
    import dragon.infrastructure.facts as df
    import dragon.utils as du
    import time

    def start_ringproc(iterations, cuid, receive_from_channel_sdesc, ret_queue):
        proc = subprocess.Popen(['ringproc', str(iterations), str(cuid), receive_from_channel_sdesc], stdout=subprocess.PIPE)
        send_to_channel_sdesc = proc.stdout.readline()
        while len(send_to_channel_sdesc.strip()) == 0:
            send_to_channel_sdesc = proc.stdout.readline()
        ret_queue.put(send_to_channel_sdesc)
        proc.wait()
        if proc.returncode != 0:
            print('*******Proc exited with rc=', proc.returncode, flush=True)

    def usage():
        print('usage: dragon ring.py <num_procs> <iterations>')
        print('    <num_procs> is the number of processes to start, one per node.')
        print('    <iterations> is the number of times each process forwards a message')
        print('                to the next node.')
        print('    The program creates a ring across the user specified number of')
        print('    nodes and sends a message around a ring of nodes. The num_procs')
        print('    and iterations must be greater than 0.')
        sys.exit(1)

    def main():
        try:
            if len(sys.argv) != 3:
                raise ValueError()

            mp.set_start_method('dragon')
            ring_size = int(sys.argv[1])
            iterations = int(sys.argv[2])
            if iterations <= 0 or ring_size <= 0:
                raise ValueError()
        except:
            usage()

        pool = dm.MemoryPool.attach(du.B64.str_to_bytes(dp.this_process.default_pd))
        origin_channel = dch.Channel(pool, df.BASE_USER_MANAGED_CUID)
        receive_sdesc = du.B64.bytes_to_str(origin_channel.serialize())
        final_channel = dch.Channel(pool, df.BASE_USER_MANAGED_CUID+1)
        final_sdesc = du.B64.bytes_to_str(final_channel.serialize())
        origin_send_sdesc = receive_sdesc

        ret_queue = mp.Queue()
        mp_procs = []
        for i in range(1,ring_size):
            proc = mp.Process(target=start_ringproc, args=(str(iterations), str(i+df.BASE_USER_MANAGED_CUID+1), receive_sdesc, ret_queue))
            proc.start()
            mp_procs.append(proc)
            receive_sdesc = ret_queue.get().strip()

        # This final process starts on the current node and completes the ring. It
        # also provides the destination for the final message to be returned.
        proc = subprocess.Popen(['ringproc', str(iterations), str(df.BASE_USER_MANAGED_CUID), receive_sdesc, origin_send_sdesc, final_sdesc], stdout=subprocess.PIPE)

        reader = dch.ChannelRecvH(final_channel)
        writer = dch.ChannelSendH(origin_channel)
        reader.open()
        writer.open()
        start = time.perf_counter()
        writer.send_bytes(b'hello', timeout=None, blocking=True)
        last_msg = reader.recv_bytes(timeout=None, blocking=True)
        stop = time.perf_counter()

        avg_time = (stop - start) / (iterations*ring_size)
        proc.wait()
        print('Ring proc exited...', flush=True)
        for proc in mp_procs:
            proc.join()
            print('Ring proc exited...', flush=True)
        if last_msg == b'hello':
            print('Test Passed.', flush=True)
            print(f'The average time per message transfer was {avg_time*1e6} microseconds.')
        else:
            print('Test Failed.', flush=True)
        print('Main proc exiting...', flush=True)


    if __name__ == '__main__':
        main()

The code in :numref:`ring_proc` is the C program that uses the Channels API to receive and
send a message. There is one process running this code on each node of the ring. The code takes
three or five arguments. The three argument case is used for all but the last process in the ring.
The code is given a receive channel descriptor where it will receive a message from in the ring. It
then creates a new channel where it will send the message to. The send channel descriptor is written
to standard output which is monitored to read it and then provide that channel descriptor to the next
instance of the *ringproc* code from which it receives its message.

Comments in the code describe why each API call is made. The pattern used here checks return codes
from all calls and prints to standard error should there be any errors. Since standard error is
captured by Dragon, any error messages are displayed back to the user.

.. code-block:: C
    :name: ring_proc
    :caption: Ring Demo Process Code
    :linenos:

    #include <dragon/channels.h>
    #include <dragon/return_codes.h>
    #include <dragon/utils.h>
    #include <stdlib.h>
    #include <stdio.h>
    #include <sys/types.h>
    #include <sys/stat.h>
    #include <unistd.h>
    #include <string.h>
    #include <time.h>
    #include <stdlib.h>

    int main(int argc, char* argv[]) {

        if (argc < 4) {
            fprintf(stderr, "usage: ringproc <iterations> <cuid> <receive_from_channel_desc> [<send_to_channel_desc> <final_channel_desc>]\n");
            fflush(stderr);
            return -1;
        }

        int iterations = atoi(argv[1]);
        dragonC_UID_t cuid = strtoul(argv[2], NULL, 0);

        dragonChannelSerial_t recv_chser;
        dragonChannelDescr_t recv_ch;
        dragonChannelRecvh_t recv_h;
        dragonChannelSerial_t send_chser;
        dragonChannelSerial_t final_chser;
        dragonChannelDescr_t send_ch;
        dragonChannelSendh_t send_h;
        dragonChannelDescr_t final_ch;
        dragonChannelSendh_t finalsend_h;
        dragonMemoryPoolDescr_t pool_descr;
        dragonMessage_t msg;
        char* send_ser_encoded;
        char* final_ser_encoded;

        /*
        * When sending a message, the structure must be initialized first.
        */

        err = dragon_channel_message_init(&msg, NULL, NULL);
        if (err != DRAGON_SUCCESS) {
            fprintf(stderr, "Could not init message with err=%s\n", dragon_get_rc_string(err));
            fflush(stderr);
            return -1;
        }

        /* A serialized channel descriptor is binary data which must be base64
        * encoded so it is valid ascii data before being passed around.
        * Dragon provides both base64 encoding and decoding for
        * interoperability between languages. */

        recv_chser.data = dragon_base64_decode(argv[3], &recv_chser.len);

        /* With a valid serialized descriptor you can attach to a channel. This
        * attach here occurs on an off-node channel (except in the one node
        * case). Whether off-node or on-node, attach works exactly the same.
        * */

        err = dragon_channel_attach(&recv_chser, &recv_ch);
        if (err != DRAGON_SUCCESS) {
            fprintf(stderr, "Could not attach to receive channel with err=%s\n", dragon_get_rc_string(err));
            fprintf(stderr, "Converting '%s'\n", argv[3]);
            return -1;
        }

        /* The decode mallocs space. This frees any malloced code in the descriptor.
        * Be sure to only call this if there is malloced space stored in the
        * descriptor. */

        err = dragon_channel_serial_free(&recv_chser);
        if (err != DRAGON_SUCCESS) {
            fprintf(stderr, "Could not free serialized channel descriptor with err=%s\n", dragon_get_rc_string(err));
            return -1;
        }

        /* The receive handle has optional attributes that are not supplied here. To
        * supply non-default attributes to the receive handle, call
        * dragon_channel_recv_attr_init first, then modify the attributes to
        * desired values and pass them as the third argument here. NULL means
        * to use the default attrs. */

        err = dragon_channel_recvh(&recv_ch, &recv_h, NULL);
        if (err != DRAGON_SUCCESS) {
            fprintf(stderr, "Could not construct receive handle with err=%s\n", dragon_get_rc_string(err));
            fflush(stderr);
            return -1;
        }

        if (argc <= 4) {
            /* In most cases instance of this process, it creates a channel to send
            * the message to. To do this, the code must attach to a pool.
            * The default pool is already created, but users may also
            * create their own pools. The pool is an on-node resource
            * only, so it must exist where the channel is to be created.
            * There is a default pool on each node running under the
            * Dragon run-time services. */

            err = dragon_memory_pool_attach_from_env(&pool_descr, "DRAGON_DEFAULT_PD");
            if (err != DRAGON_SUCCESS) {
                fprintf(stderr, "Could not attach to memory pool with err=%s\n", dragon_get_rc_string(err));
                fflush(stderr);
                return -1;
            }

            /* We create our own send_to channel with the given cuid. Attributes
            * could be applied to the channel creation. NULL provides the
            * default attributes. To customize, call
            * dragon_channel_attr_init first, the customize and provide
            * them in place of NULL. */

            err = dragon_channel_create(&send_ch, cuid, &pool_descr, NULL);
            if (err != DRAGON_SUCCESS) {

                /* Notice the calls to dragon_get_rc_string which converts dragon
                * error codes into human readable strings. Also the
                * dragon_getlasterrstr provides useful traceback
                * information so you can see the origin of an error
                * should it occur. */

                fprintf(stderr, "Could not create send channel with err=%s\n", dragon_get_rc_string(err));
                fprintf(stderr, "Traceback: %s\n", dragon_getlasterrstr());
                fflush(stderr);
                return -1;
            }

            /*
            * Here we serialize the new channel and provide it on standard output.
            */

            err = dragon_channel_serialize(&send_ch, &send_chser);
            if (err != DRAGON_SUCCESS) {
                fprintf(stderr, "Could not serialize send channel with err=%s\n", dragon_get_rc_string(err));
                fflush(stderr);
                return -1;
            }

            send_ser_encoded = dragon_base64_encode(send_chser.data, send_chser.len);

            err = dragon_memory_pool_detach(&pool_descr);
            if (err != DRAGON_SUCCESS) {
                fprintf(stderr, "Could not detach to memory pool with err=%s\n", dragon_get_rc_string(err));
                fflush(stderr);
                return -1;
            }

            err = dragon_channel_serial_free(&send_chser);
            if (err != DRAGON_SUCCESS) {
                fprintf(stderr, "Could not free serialized channel descriptor with err=%s\n", dragon_get_rc_string(err));
                return -1;
            }

        } else {
            /*
            * We were given a channel descriptor for the send channel and the final
            * send channel.
            */
            send_ser_encoded = argv[4];
            final_ser_encoded = argv[5];

            send_chser.data = dragon_base64_decode(send_ser_encoded, &send_chser.len);

            err = dragon_channel_attach(&send_chser, &send_ch);
            if (err != DRAGON_SUCCESS) {
                fprintf(stderr, "Could not attach to send channel with err=%s\n", dragon_get_rc_string(err));
                fflush(stderr);
                return -1;
            }

            err = dragon_channel_serial_free(&send_chser);
            if (err != DRAGON_SUCCESS) {
                fprintf(stderr, "Could not free serialized channel descriptor with err=%s\n", dragon_get_rc_string(err));
                return -1;
            }

            final_chser.data = dragon_base64_decode(final_ser_encoded, &final_chser.len);

            err = dragon_channel_attach(&final_chser, &final_ch);
            if (err != DRAGON_SUCCESS) {
                fprintf(stderr, "Could not attach to final send channel with err=%s\n", dragon_get_rc_string(err));
                fflush(stderr);
                return -1;
            }

            /* The final channel is where to send the message when it has completed
            * its rounds on the ring. The final channel contents are read
            * by the Python bootstrap program to indicate that the test
            * has completed. */

            err = dragon_channel_serial_free(&final_chser);
            if (err != DRAGON_SUCCESS) {
                fprintf(stderr, "Could not free final serialized channel descriptor with err=%s\n", dragon_get_rc_string(err));
                return -1;
            }

            err = dragon_channel_sendh(&final_ch, &finalsend_h, NULL);
            if (err != DRAGON_SUCCESS) {
                fprintf(stderr, "Could not construct send handle for final channel with err=%s\n", dragon_get_rc_string(err));
                fflush(stderr);
                return -1;
            }

            err = dragon_chsend_open(&finalsend_h);
            if (err != DRAGON_SUCCESS) {
                fprintf(stderr, "Could not open final send handle with err=%s\n", dragon_get_rc_string(err));
                fflush(stderr);
                return -1;
            }
        }

        /*
        * This provides the newly created channel back to the caller of this code.
        */
        printf("%s\n", send_ser_encoded);
        fflush(stdout);

        /* The send handle is used to send message into a channel. Default attributes
        * are applied here. The send handle attributes can be customized by
        * calling dragon_channel_send_attr_init and providing in place of
        * NULL. */

        err = dragon_channel_sendh(&send_ch, &send_h, NULL);
        if (err != DRAGON_SUCCESS) {
            fprintf(stderr, "Could not construct send handle with err=%s\n", dragon_get_rc_string(err));
            fflush(stderr);
            return -1;
        }

        /*
        * You must open send and receive handles before sending or receiving.
        */
        err = dragon_chsend_open(&send_h);
        if (err != DRAGON_SUCCESS) {
            fprintf(stderr, "Could not open send handle with err=%s\n", dragon_get_rc_string(err));
            fflush(stderr);
            return -1;
        }

        err = dragon_chrecv_open(&recv_h);
        if (err != DRAGON_SUCCESS) {
            fprintf(stderr, "Could not open receive handle with err=%s\n", dragon_get_rc_string(err));
            fflush(stderr);
            return -1;
        }

        int k;
        dragonChannelSendh_t* sendto_h = &send_h;

        for (k=0; k<iterations; k++) {
            /* Blocking receives may be given a timeout. This code blocks using the
            * default receive handle timeout which is to wait indefinitely. */

            err = dragon_chrecv_get_msg_blocking(&recv_h, &msg, NULL);
            if (err != DRAGON_SUCCESS) {
                fprintf(stderr, "Could not receive message with err=%s\n", dragon_get_rc_string(err));
                fflush(stderr);
                return -1;
            }

            if ((argc > 4) && (k==iterations-1)) {
                /* On the last iteration for the origin process, write the message to
                * the final channel instead of back into the ring. */

                sendto_h = &finalsend_h;
            }

            /* Send the message on to its destination. Transfer of ownership means
            * that any pool allocation associated with the message will
            * be freed by the receiver. This works both on and off-node
            * since the transport agent will clean up the message in the
            * off-node case. */

            err = dragon_chsend_send_msg(sendto_h, &msg, DRAGON_CHANNEL_SEND_TRANSFER_OWNERSHIP, NULL);
            if (err != DRAGON_SUCCESS) {
                fprintf(stderr, "Could not send message with err=%s\n", dragon_get_rc_string(err));
                fflush(stderr);
                return -1;
            }
        }

        /*
        * Send and receive handles should be closed when no longer needed.
        */

        err = dragon_chsend_close(&send_h);
        if (err != DRAGON_SUCCESS) {
            fprintf(stderr, "Could not close send handle with err=%s\n", dragon_get_rc_string(err));
            fflush(stderr);
            return -1;
        }

        err = dragon_chrecv_close(&recv_h);
        if (err != DRAGON_SUCCESS) {
            fprintf(stderr, "Could not close receive handle with err=%s\n", dragon_get_rc_string(err));
            fflush(stderr);
            return -1;
        }
        if (argc <= 4) {

            /* Channels should be destroyed when no longer needed. Since the program
            * is ending, technically this would be cleaned up
            * automatically once the Dragon run-time services exit, but
            * better to be explicit about it in this example. */

            err = dragon_channel_destroy(&send_ch);
            if (err != DRAGON_SUCCESS) {
                fprintf(stderr, "Could not destroy send channel with err=%s\n", dragon_get_rc_string(err));
                fflush(stderr);
                return -1;
            }

            /* To be complete, we'll detach from the pool. But again, this is done
            * automatically during cleanup when Dragon run-time services
            * exit. */

            err = dragon_memory_pool_detach(&pool_descr);
            if (err != DRAGON_SUCCESS) {
                fprintf(stderr, "Could not detach from the default pool with err=%s\n", dragon_get_rc_string(err));
                fflush(stderr);
                return -1;
            }

        }

        return 0;
    }
