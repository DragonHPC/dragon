#include <dragon/channels.h>
#include <dragon/return_codes.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#include <string.h>
#include <time.h>
#include <sys/gmon.h>

#include "../_ctest_utils.h"

#undef NMSGS
#define NMSGS 10
#undef MSGWORDS
#define MSGWORDS 2
#define B_PER_MBLOCK 1024
#define SERFILE "channel_serialized.dat"
#define MFILE "channels_test"
#define M_UID 1
#define RING_SIZE 10
#define RING_ITERATIONS 10000

dragonError_t
create_pool(dragonMemoryPoolDescr_t* mpool)
{
    /* Create a memory pool to allocate messages and a Channel out of */
    size_t mem_size = 1UL<<31;

    char * fname = util_salt_filename(MFILE);
    dragonError_t derr = dragon_memory_pool_create(mpool, mem_size, fname, M_UID, NULL);
    if (derr != DRAGON_SUCCESS)
        err_fail(derr, "Failed to create memory pool");

    free(fname);

    return SUCCESS;
}


dragonError_t
create_ring_channels(dragonMemoryPoolDescr_t* mpool, dragonChannelDescr_t channel[], int arr_size)
{
    int k;

    for (k=0;k<arr_size;k++) {
        dragonError_t derr = util_create_channel(mpool, &channel[k], k, NMSGS, B_PER_MBLOCK);
        if (derr != SUCCESS) {
            printf("Failed creating channel %d\n", k);
            return FAILED;
        }
    }

    return SUCCESS;
}


dragonError_t
init_channels_serial_desc(dragonChannelDescr_t channel[], dragonChannelSerial_t serialized_channels[], int arr_size)
{
    int k;

    for (k=0; k<arr_size; k++) {
        dragonError_t cerr = dragon_channel_serialize(&channel[k], &serialized_channels[k]);
        if (cerr != DRAGON_SUCCESS)
            err_fail(cerr, "Failed to serialize channel");
    }

    return SUCCESS;
}

dragonError_t
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

dragonError_t
attach_channel(dragonMemoryPoolDescr_t * mpool, dragonChannelDescr_t * ch)
{
    struct stat sb;
    stat(SERFILE, &sb);

    dragonChannelSerial_t ch_ser;
    ch_ser.len = sb.st_size;
    ch_ser.data = malloc(ch_ser.len);

    FILE * fp = fopen(SERFILE, "r+");
    int count = fread(ch_ser.data, 1, ch_ser.len, fp);
    if (count != ch_ser.len) {
        printf("Failed to fread the serialized channel data\n");
        return FAILED;
    }

    dragonError_t cerr = dragon_channel_attach(&ch_ser, ch);
    if (cerr != DRAGON_SUCCESS)
        err_fail(cerr, "Failed to attach Channel");

    cerr = dragon_channel_get_pool(ch, mpool);
    if (cerr != DRAGON_SUCCESS)
        err_fail(cerr, "Failed to get pool from Channel");

    fclose(fp);
    return SUCCESS;
}

dragonError_t
create_open_send_handle(dragonChannelDescr_t * ch, dragonChannelSendh_t * csend)
{
    // dragonChannelSendAttr_t sattrs;
    // dragon_channel_send_attr_init(&sattrs);
    // sattrs.wait_mode = DRAGON_SPIN_WAIT;

    /* Create a send handle from the Channel */
    //dragonError_t cerr = dragon_channel_sendh(ch, csend, &sattrs);
    dragonError_t err = dragon_channel_sendh(ch, csend, NULL);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Failed to create send handle");

    /* Open the send handle for writing */
    err = dragon_chsend_open(csend);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Failed to open send handle");

    return SUCCESS;
}

dragonError_t
create_open_recv_handle(dragonChannelDescr_t * ch, dragonChannelRecvh_t * crecv, dragonChannelRecvAttr_t* rattrs) {

    /* Create a receive handle from the Channel */

    dragonError_t err = dragon_channel_recvh(ch, crecv, rattrs);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Failed to create recv handle");

    /* Open the receive handle */
    err = dragon_chrecv_open(crecv);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Failed to open recv handle");

    return SUCCESS;
}


dragonError_t
ring_proc(int idx, dragonChannelDescr_t channels[], dragonChannelSerial_t serialized_channels[], dragonMemoryPoolSerial_t* pool_ser) {

    // This was an experiment to see if channel descriptors can be shared between forked processes. They can. So
    // the serialization of channels was unnecessary here, but this commented out code works as well for sharing
    // channel descriptors.

    // dragonChannelDescr_t in;
    // dragonChannelDescr_t out;
    // dragonMemoryPoolDescr_t mpool;

    // dragonError_t merr = dragon_memory_pool_attach(&mpool, pool_ser);
    // if (merr != DRAGON_SUCCESS) {
    //     char * errstr = dragon_getlasterrstr();
    //     printf("Failed to attach Channel.  Got EC=%i\nERRSTR = \n%s\n", merr, errstr);
    //     return FAILED;
    // }

    // dragonError_t cerr = dragon_channel_attach(&serialized_channels[idx], &in);
    // if (cerr != DRAGON_SUCCESS) {
    //     char * errstr = dragon_getlasterrstr();
    //     printf("Failed to attach Channel.  Got EC=%i\nERRSTR = \n%s\n", cerr, errstr);
    //     return FAILED;
    // }

    // cerr = dragon_channel_attach(&serialized_channels[(idx+1)%RING_SIZE], &out);
    // if (cerr != DRAGON_SUCCESS) {
    //     char * errstr = dragon_getlasterrstr();
    //     printf("Failed to attach Channel.  Got EC=%i\nERRSTR = \n%s\n", cerr, errstr);
    //     return FAILED;
    // }

    dragonChannelDescr_t* in;
    dragonChannelDescr_t* out;
    dragonChannelDescr_t* final_out;
    int res;

    in = &channels[idx];
    out = &channels[(idx+1)%RING_SIZE];
    final_out = &channels[RING_SIZE];

    dragonChannelRecvh_t recvh;
    dragonChannelSendh_t sendh;
    dragonChannelSendh_t final_sendh;

    dragonChannelRecvAttr_t rattrs;

    dragon_channel_recv_attr_init(&rattrs);
    rattrs.default_timeout = DRAGON_CHANNEL_BLOCKING_NOTIMEOUT;

    res = create_open_recv_handle(in, &recvh, &rattrs);
    if (res != SUCCESS)
        return FAILED;

    res = create_open_send_handle(out, &sendh);
    if (res != SUCCESS)
        return FAILED;

    if (idx == 0) {
        res = create_open_send_handle(final_out, &final_sendh);
        if (res != SUCCESS) {
            printf("Error: Could not open final send handle\n");
            return FAILED;
        }
    }


    /* Create a message using that memory */
    dragonMessage_t msg;
    dragonError_t err = dragon_channel_message_init(&msg, NULL, NULL);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Error: Could not init message");

    int k;
    for (k=0;k<RING_ITERATIONS; k++) {
        // This code does a non-blocking receive with a sleepy poll. This
        // code performs worse on systems without enough cores and better
        // on systems where a core can run each process (but of course
        // consumes more of the CPU and is power hungry). The code below
        // on a Linux VM on a macbook pro took ~5.2 seconds to run. The
        // blocking version took ~2.3 seconds to run. On a Rome CPU the
        // version below took ~0.59 seconds to run while the blocking
        // version took ~1.4 seconds to run.
        // bool getting = true;
        // int sleep_amt = 0.000000000001;
        // while (getting) {
        //    err = dragon_chrecv_get_msg(&recvh, &msg);
        //    if (err != DRAGON_CHANNEL_EMPTY)
        //        getting = false;
        //     else {
        //         sleep(sleep_amt);
        //         sleep_amt += 0.000000000001;
        //     }
        // }

        err = dragon_chrecv_get_msg_blocking(&recvh, &msg, NULL);
        if (err != DRAGON_SUCCESS)
            err_fail(err, "Failed to get message");

        err = dragon_chsend_send_msg(&sendh,&msg,DRAGON_CHANNEL_SEND_TRANSFER_OWNERSHIP,NULL);
        if (err != DRAGON_SUCCESS)
            err_fail(err, "Error: Could not send message");
    }
    if (idx == 0) {
        // at the end get the final message and put it in the last channel for the main program
        // to receive.

        err = dragon_chrecv_get_msg_blocking(&recvh, &msg, NULL);
        if (err != DRAGON_SUCCESS)
            err_fail(err, "Failed to get final message");

        err = dragon_chsend_send_msg(&final_sendh,&msg,DRAGON_CHANNEL_SEND_TRANSFER_OWNERSHIP,NULL);
        if (err != SUCCESS)
            err_fail(err, "Error: Could not send final message");
    }

    return SUCCESS;
}

int main(int argc, char *argv[])
{
    dragonError_t derr;
    dragonMemoryPoolDescr_t mpool;
    timespec_t t1, t2;
    double etime;
    dragonChannelDescr_t channels[RING_SIZE+1];
    dragonChannelSerial_t serialized_channels[RING_SIZE+1];
    dragonMemoryPoolSerial_t serialized_pool;

    if (create_pool(&mpool) != SUCCESS)
        err_fail(DRAGON_FAILURE, "Failed to create ring pool");

    if (create_ring_channels(&mpool, channels, RING_SIZE+1) != SUCCESS)
        main_err_fail(DRAGON_FAILURE, "Failed to create ring channels", jmp_destroy_pool);

    if ((derr = dragon_memory_pool_serialize(&serialized_pool,&mpool)) != DRAGON_SUCCESS)
        main_err_fail(derr, "Failed to serialize memory pool", jmp_destroy_pool);

    if (init_channels_serial_desc(channels, serialized_channels, RING_SIZE+1) != SUCCESS)
        main_err_fail(DRAGON_FAILURE, "Could not serialize channel descriptors", jmp_destroy_pool);

    /* Allocate managed memory as buffer space for both send and receive */
    dragonMemoryDescr_t rmsg_buf;
    derr = dragon_memory_alloc(&rmsg_buf, &mpool, sizeof(int)*MSGWORDS);
    if (derr != DRAGON_SUCCESS)
        main_err_fail(derr, "Could not allocate memory from pool", jmp_destroy_pool);

    /* Get a pointer to that memory and fill up a message payload */
    int * rmsg_ptr;
    derr = dragon_memory_get_pointer(&rmsg_buf, (void *)&rmsg_ptr);
    if (derr != DRAGON_SUCCESS)
        main_err_fail(derr, "Could not retrieve allocation memory pointer", jmp_destroy_pool);

    for (int i = 0; i < MSGWORDS; i++) {
        rmsg_ptr[i] = i;
    }

    /* Create a message using that memory */
    dragonMessage_t ring_msg;
    derr = dragon_channel_message_init(&ring_msg, &rmsg_buf, NULL);
    if (derr != DRAGON_SUCCESS)
        main_err_fail(derr, "Failed to init message", jmp_destroy_pool);

    int k;
    for (k=0;k<RING_SIZE;k++) {
        if (fork()==0) {
            ring_proc(k, channels, serialized_channels, &serialized_pool);
            return SUCCESS; // Fork process, default to success? Was hardcoded to 0
        }
    }

    dragonChannelSendh_t sendh;

    // Test timeout on blocking receive.
    dragonMessage_t final_msg;
    derr = dragon_channel_message_init(&final_msg, NULL, NULL);
    if (derr != DRAGON_SUCCESS)
        main_err_fail(derr, "Failed to init message", jmp_destroy_pool);

    dragonChannelRecvh_t recvh;
    dragonChannelRecvAttr_t rattrs;

    dragon_channel_recv_attr_init(&rattrs);

    rattrs.default_notif_type = DRAGON_RECV_SYNC_MANUAL;
    rattrs.default_timeout.tv_sec = 0;
    rattrs.default_timeout.tv_nsec = 5000;

    int err = create_open_recv_handle(&channels[0], &recvh, &rattrs);
    if (err != SUCCESS)
        main_err_fail(DRAGON_FAILURE, "Failed to create and open receive handle", jmp_destroy_pool);

    derr = dragon_chrecv_get_msg_blocking(&recvh, &final_msg, NULL);
    if (derr != DRAGON_TIMEOUT)
        main_err_fail(derr, "Failed to timeout on blocking receive", jmp_destroy_pool);

    derr = dragon_chrecv_close(&recvh);
    if (derr != DRAGON_SUCCESS)
        main_err_fail(derr, "Failed to close receive handle", jmp_destroy_pool);

    rattrs.default_notif_type = DRAGON_RECV_SYNC_MANUAL;
    rattrs.default_timeout = DRAGON_CHANNEL_BLOCKING_NOTIMEOUT;

    err = create_open_recv_handle(&channels[RING_SIZE], &recvh, &rattrs);
    if (err != SUCCESS)
        main_err_fail(DRAGON_FAILURE, "Failed to create and open receive handle", jmp_destroy_pool);

    err = create_open_send_handle(&channels[0], &sendh);
    if (err != SUCCESS)
        main_err_fail(DRAGON_FAILURE, "Failed to create and open send handle", jmp_destroy_pool);

    // Now begin the Ring Test.
    clock_gettime(CLOCK_MONOTONIC, &t1);
    err = dragon_chsend_send_msg(&sendh,&ring_msg,DRAGON_CHANNEL_SEND_TRANSFER_OWNERSHIP,NULL);
    if (err != SUCCESS)
        main_err_fail(DRAGON_FAILURE, "Failed to send initial message", jmp_destroy_pool);

    derr = dragon_chrecv_get_msg_blocking(&recvh, &final_msg, NULL);
    if (derr != DRAGON_SUCCESS)
        main_err_fail(derr, "Failed to receive final message", jmp_destroy_pool);

    clock_gettime(CLOCK_MONOTONIC, &t2);

    derr = dragon_memory_get_pointer(final_msg._mem_descr, (void *)&rmsg_ptr);
    if (derr != DRAGON_SUCCESS)
        main_err_fail(derr, "Failed to get message buffer pointer", jmp_destroy_pool);

    /* Check that we got back what we should from the message */
    for (int i = 0; i < MSGWORDS; i++) {
        if (rmsg_ptr[i] != i)
            printf("Error: element %i is wrong (%i != %i)\n", i, rmsg_ptr[i], i);
    }

    etime   = 1e-9 * (double)(BILL * (t2.tv_sec - t1.tv_sec) + (t2.tv_nsec - t1.tv_nsec));
    printf("Perf: elapsed time is %5.3f seconds\n", etime);

    double lat = 1E6 * etime / (RING_ITERATIONS * RING_SIZE);
    double bw = (double)sizeof(int)*MSGWORDS * RING_ITERATIONS * RING_SIZE / (1024UL * 1024UL * etime);
    double mrate = RING_ITERATIONS * RING_SIZE / etime;

    printf("Perf:\tpt2pt message rate = %5.3f msgs/sec\n\tlatency = %5.3f usec\n\tBW = %5.3f MiB/s\n",
           mrate, lat, bw);

    util_close_send_recv_handles(&sendh, &recvh);

    for (k=0;k<RING_SIZE;k++) {
        dragon_channel_destroy(&channels[k]);
    }

jmp_destroy_pool:
    derr = dragon_memory_pool_destroy(&mpool);
    if (derr != DRAGON_SUCCESS)
        err_fail(derr, "Failed to destroy pool at end of test");

    printf("Ring Test Complete\n");

    return TEST_STATUS;
}
