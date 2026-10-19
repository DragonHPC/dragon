#include <dragon/channels.h>
#include <dragon/return_codes.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#include <string.h>

#include "../_ctest_utils.h"

#define SERFILE "channel_serialized.dat"
#define MFILE "channels_test"
#define M_UID 0
#define MSG_COUNT 250

#define NCHANNELS 3

int create_pool(dragonMemoryPoolDescr_t* mpool) {
    /* Create a memory pool to allocate messages and a Channel out of */
    size_t mem_size = 1UL<<31;
    printf("Allocate pool %lu B\n", mem_size);

    char * fname = util_salt_filename(MFILE);
    dragonError_t derr = dragon_memory_pool_create(mpool, mem_size, fname, M_UID, NULL);
    if (derr != DRAGON_SUCCESS)
        err_fail(derr, "Failed to create pool");
    free(fname);

    return SUCCESS;
}

int
serialize_channel(dragonChannelDescr_t * ch)
{
    dragonChannelSerial_t ch_ser;
    dragonError_t cerr = dragon_channel_serialize(ch, &ch_ser);
    if (cerr != DRAGON_SUCCESS)
        err_fail(cerr, "Failed to serialize Channel");

    FILE * fp = fopen(SERFILE, "w");
    fwrite(ch_ser.data, 1, ch_ser.len, fp);
    fclose(fp);

    return SUCCESS;
}

int
create_open_send_handle(dragonChannelDescr_t * ch, dragonChannelSendh_t * csend, dragonChannelSendAttr_t* sattrs)
{
    /* Create a send handle from the Channel */
    dragonError_t cerr = dragon_channel_sendh(ch, csend, sattrs);
    if (cerr != DRAGON_SUCCESS)
        err_fail(cerr, "Failed to create send handle");

    /* Open the send handle for writing */
    dragonError_t serr = dragon_chsend_open(csend);
    if (serr != DRAGON_SUCCESS)
        err_fail(serr, "Failed to open send handle");

    return SUCCESS;
}

int
create_open_recv_handle(dragonChannelDescr_t * ch, dragonChannelRecvh_t * crecv, dragonChannelRecvAttr_t* rattrs) {

    /* Create a receive handle from the Channel */

    dragonError_t cerr = dragon_channel_recvh(ch, crecv, rattrs);
    if (cerr != DRAGON_SUCCESS)
        err_fail(cerr, "Failed to create recv handle");

    /* Open the receive handle */
    dragonError_t rerr = dragon_chrecv_open(crecv);
    if (rerr != DRAGON_SUCCESS)
        err_fail(rerr, "Failed to open recv handle");

    return SUCCESS;
}

int
close_send_recv_handles(dragonChannelSendh_t * csend, dragonChannelRecvh_t * crecv)
{
    /* Close the send and receive handles */
    dragonError_t serr = dragon_chsend_close(csend);
    if (serr != DRAGON_SUCCESS)
        err_fail(serr, "Failed to close send handle");

    dragonError_t rerr = dragon_chrecv_close(crecv);
    if (rerr != DRAGON_SUCCESS)
        err_fail(rerr, "Failed to close recv handle");

    return SUCCESS;
}

int proc(int idx, dragonChannelDescr_t channels[]) {

    dragonChannelDescr_t* in;
    dragonChannelDescr_t* out;
    int err;

    in = &channels[idx];
    out = &channels[idx+1];

    dragonChannelRecvh_t recvh;
    dragonChannelSendh_t sendh;

    dragonChannelRecvAttr_t rattrs;
    dragon_channel_recv_attr_init(&rattrs);
    rattrs.default_notif_type = DRAGON_RECV_SYNC_MANUAL;
    rattrs.default_timeout = DRAGON_CHANNEL_BLOCKING_NOTIMEOUT;

    err = create_open_recv_handle(in, &recvh, &rattrs);
    if (err != SUCCESS) {
        printf("Error: Could not open receive handle in proc %d\n", idx);
        err_fail(err, "Could not open recv handle");
    }

    dragonChannelSendAttr_t sattrs;
    dragon_channel_send_attr_init(&sattrs);
    sattrs.return_mode = DRAGON_CHANNEL_SEND_RETURN_IMMEDIATELY;
    sattrs.default_timeout = DRAGON_CHANNEL_BLOCKING_NOTIMEOUT;

    err = create_open_send_handle(out, &sendh, &sattrs);
    if (err != SUCCESS) {
        printf("Could not open send handle in proc %d\n", idx);
        err_fail(err, "Could not open send handle");
    }

    int k;
    for (k=0;k<MSG_COUNT; k++) {
        /* Initialize the message structure */
        dragonMessage_t msg;
        err = dragon_channel_message_init(&msg, NULL, NULL);
        if (err != SUCCESS) {
            printf("Could not init message in proc %d\n", idx);
            err_fail(err, "Could not init message");
        }

        err = dragon_chrecv_get_msg_blocking(&recvh, &msg, NULL);
        if (err != SUCCESS) {
            printf("Error: Could not recv message in proc %d with error code %d\n", idx, err);
            err_fail(err, "Failed to create message");
        }

        printf("Got message %d in proc %d and sending it on\n", k, idx);


        err = dragon_chsend_send_msg(&sendh,&msg,DRAGON_CHANNEL_SEND_TRANSFER_OWNERSHIP,NULL);
        if (err != SUCCESS) {
            printf("Could not send message in proc %d\n", idx);
            err_fail(err, "Could not send message");
        }
    }

    printf("Proc %d Exiting\n", idx);

    return SUCCESS;
}

int main(int argc, char *argv[])
{
    dragonError_t derr;
    dragonMemoryPoolDescr_t mpool;
    int * msg_ptr;
    dragonMessage_t msg;
    dragonError_t cerr;
    dragonChannelDescr_t channels[NCHANNELS];

    derr = create_pool(&mpool);
    if (derr != SUCCESS)
        err_fail(derr, "Failed to create ring pool");

    for (int i = 0; i < NCHANNELS; i++) {
        derr = util_create_channel(&mpool, &channels[i], i, 0, 0);
        if (derr != SUCCESS)
            err_fail(DRAGON_FAILURE, "Failed to create ring channels");
    }


    int k;
    for (k=0;k<2;k++) {
        if (fork()==0) {
            proc(k, channels);
            return SUCCESS;
        }
    }

    dragonChannelSendh_t sendh;
    dragonChannelSendAttr_t sattrs;
    dragon_channel_send_attr_init(&sattrs);

    int err = create_open_send_handle(&channels[0], &sendh, &sattrs);
    if (err != SUCCESS)
        main_err_fail(err, "Failed to generate send handle", jmp_destroy_pool);


    for (k=0;k<MSG_COUNT;k++) {
        /* Allocate managed memory as buffer space for both send and receive */
        dragonMemoryDescr_t msg_buf;
        err = dragon_memory_alloc(&msg_buf, &mpool, sizeof(int)*MSGWORDS);
        if (err != DRAGON_SUCCESS)
            main_err_fail(err, "Failed to allocate message buffer from pool", jmp_destroy_pool);

        /* Get a pointer to that memory and fill up a message payload */
        int * msg_ptr;
        err = dragon_memory_get_pointer(&msg_buf, (void *)&msg_ptr);
        if (err != DRAGON_SUCCESS)
            main_err_fail(err, "Failed to get memory pointer", jmp_destroy_pool);

        for (int i = 0; i < MSGWORDS; i++) {
            msg_ptr[i] = i;
        }

        /* Create a message using that memory */
        dragonMessage_t msg;
        cerr = dragon_channel_message_init(&msg, &msg_buf, NULL);
        if (cerr != DRAGON_SUCCESS)
            main_err_fail(cerr, "Failed to create message", jmp_destroy_pool);

        err = dragon_chsend_send_msg(&sendh,&msg,DRAGON_CHANNEL_SEND_TRANSFER_OWNERSHIP,NULL);
        if (err != SUCCESS)
            main_err_fail(err, "Failed to send message", jmp_destroy_pool);

        printf("Sent message %d from main.\n", k);
    }

    int status;

    dragonChannelRecvh_t recvh;
    dragonChannelRecvAttr_t rattrs;
    dragon_channel_recv_attr_init(&rattrs);
    rattrs.default_notif_type = DRAGON_RECV_SYNC_MANUAL;
    rattrs.default_timeout = DRAGON_CHANNEL_BLOCKING_NOTIMEOUT;


    err = create_open_recv_handle(&channels[2], &recvh, &rattrs);
    if (err != SUCCESS)
        main_err_fail(err, "Failed to open recv handle", jmp_destroy_pool);

    for (k=0; k<MSG_COUNT; k++) {
        cerr = dragon_channel_message_init(&msg, NULL, NULL);
        if (cerr != DRAGON_SUCCESS)
            main_err_fail(cerr, "Failed to create message", jmp_destroy_pool);

        cerr = dragon_chrecv_get_msg_blocking(&recvh, &msg, NULL);
        if (cerr != DRAGON_SUCCESS)
            main_err_fail(cerr, "Failed to receive message", jmp_destroy_pool);

        derr = dragon_memory_get_pointer(msg._mem_descr, (void *)&msg_ptr);
        if (derr != DRAGON_SUCCESS)
            main_err_fail(derr, "Failed to get message buffer pointer", jmp_destroy_pool);

        /* Check that we got back what we should from the message */
        for (int i = 0; i < MSGWORDS; i++) {
            if (msg_ptr[i] != i) {
                printf("Error: element %i is wrong (%i != %i)\n", i, msg_ptr[i], i);
                TEST_STATUS = FAILED;
            }
        }

        printf("Received message %d in main.\n", k);
    }

    for (k=0;k<2;k++) {
        wait(&status);
    }

    close_send_recv_handles(&sendh, &recvh);

    for (k=0;k<3;k++) {
        dragon_channel_destroy(&channels[k]);
    }

jmp_destroy_pool:
    dragon_memory_pool_destroy(&mpool);

    printf("Send Test Complete\n");

    return TEST_STATUS;
}
