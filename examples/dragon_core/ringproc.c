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
    size_t send_ser_len;

    /* This function is necessary for off-node communication and relies on the
     * Dragon run-time services to supply gateway channels in the
     * environment. Gateway channels are automatically supplied by Dragon
     * on multi-node allocations and this function works on both single
     * and multi-node allocations, though on single-node allocations it
     * does nothing. */

    dragonError_t err = dragon_channel_register_gateways_from_env();
    if (err != DRAGON_SUCCESS) {
        fprintf(stderr, "Could not register gateway channels from environment with err=%s\n", dragon_get_rc_string(err));
        fflush(stderr);
        return -1;
    }

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

    recv_chser.data = dragon_base64_decode(argv[3], strlen(argv[3]), &recv_chser.len);

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

        send_ser_encoded = dragon_base64_encode(send_chser.data, send_chser.len, &send_ser_len);

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

        send_chser.data = dragon_base64_decode(send_ser_encoded, strlen(send_ser_encoded), &send_chser.len);

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

        final_chser.data = dragon_base64_decode(final_ser_encoded, strlen(final_ser_encoded), &final_chser.len);

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