#include <dragon/channels.h>
#include <dragon/return_codes.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <time.h>

#include "../_ctest_utils.h"

#define NREPEATS 10000
#define SERFILE "channel_serialized.dat"
#define MFILE "channels_test"
#define M_UID 0
#define C_UID 0

static dragonMemoryPoolDescr_t mpool;

static int
create_memory_channel(dragonMemoryPoolDescr_t * mpool, dragonChannelDescr_t * ch)
{
    /* Create a memory pool to allocate messages and a Channel out of */
    size_t mem_size = 1UL<<30;
    printf("Allocate pool %lu B\n", mem_size);

    size_t prealloc[] = {100,100,100,100};
    dragonMemoryPoolAttr_t attrs;
    dragonError_t err = dragon_memory_attr_init(&attrs);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Failed to initialize memory attributes");

    attrs.npre_allocs = 4;
    attrs.pre_allocs = prealloc;

    char * fname = util_salt_filename(MFILE);
    err = dragon_memory_pool_create(mpool, mem_size, fname, M_UID, &attrs);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Failed to create memory pool");

    free(fname);

    /* Create a Channel attributes structure so we can tune the Channel */
    dragonChannelAttr_t cattr;
    err = dragon_channel_attr_init(&cattr);

    /* Set the Channel capacity to our message limit for this test */
    cattr.capacity = NMSGS;
    cattr.bytes_per_msg_block = B_PER_MBLOCK;
    printf("Channel capacity set to %li\n", cattr.capacity);

    /* Create the Channel in the memory pool */
    err = dragon_channel_create(ch, C_UID, mpool, &cattr);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Failed to create channel");

    return SUCCESS;
}

static int
serialize_channel(dragonChannelDescr_t * ch)
{
    dragonChannelSerial_t ch_ser;
    dragonError_t err = dragon_channel_serialize(ch, &ch_ser);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Failed to serialize Channel");

    FILE * fp = fopen(SERFILE, "w");
    fwrite(ch_ser.data, 1, ch_ser.len, fp);
    fclose(fp);

    return SUCCESS;
}

static int
attach_channel(dragonMemoryPoolDescr_t * mpool, dragonChannelDescr_t * ch)
{
    struct stat sb;
    stat(SERFILE, &sb);

    dragonChannelSerial_t ch_ser;
    ch_ser.len = sb.st_size;
    ch_ser.data = malloc(ch_ser.len);

    FILE * fp = fopen(SERFILE, "r+");

    dragonError_t err = dragon_channel_attach(&ch_ser, ch);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Failed to attach to Channel");

    err = dragon_channel_get_pool(ch, mpool);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Failed to retrieve pool from Channel");

    fclose(fp);
    return SUCCESS;
}

static int
detach_mem_channel(dragonMemoryPoolDescr_t * mpool, dragonChannelDescr_t * ch)
{
    dragonError_t err = dragon_memory_pool_detach(mpool);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Failed to detach from pool");

    return SUCCESS;
}

static int
destroy_mem_channel(dragonMemoryPoolDescr_t * mpool, dragonChannelDescr_t * ch)
{
    /* Destroy the Channel and underlying memory pool */
    dragonError_t err = dragon_channel_destroy(ch);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Failed to destroy channel");

    err = dragon_memory_pool_destroy(mpool);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Failed to destroy memory pool");

    return SUCCESS;
}

static int
get_msg_for_send(dragonMessage_t *msg)
{
    dragonError_t err;
    dragonMemoryDescr_t msg_buf;

    err = dragon_memory_alloc(&msg_buf, &mpool, sizeof(int)*MSGWORDS);
    if (err != DRAGON_SUCCESS) {
        err_fail(err, "Failed to allocate message buffer from pool");
    } else {
        printf("Got memory allocation successfully\n");
    }

    /* Get a pointer to that memory and fill up a message payload */
    int * msg_ptr;
    err = dragon_memory_get_pointer(&msg_buf, (void *)&msg_ptr);
    for (int i = 0; i < MSGWORDS; i++) {
        msg_ptr[i] = i;
    }

    /* Create a message using that memory */
    err = dragon_channel_message_init(msg, &msg_buf, NULL);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Failed to create message");

    return SUCCESS;
}

int main(int argc, char *argv[])
{
    dragonError_t err;
    dragonChannelDescr_t ch;
    dragonMessage_t send_msg;
    dragonMessage_t peek_msg;

    if (argc == 1 || (argc == 2 && strcmp(argv[1], "create") == 0)) {

        int ctest = create_memory_channel(&mpool, &ch);
        if (ctest != SUCCESS) {
            printf("Create Memory and Channel: FAILED\n");
            return FAILED;
        } else {
            printf("Create Memory and Channel: SUCCESS\n");
        }

        int stest = serialize_channel(&ch);
        if (stest != SUCCESS) {
            printf("Serialize Channel: FAILED\n");
            return FAILED;
        } else {
            printf("Serialize Channel: SUCCESS\n");
        }

    } else if (argc == 2 && strcmp(argv[1], "attach") == 0) {

        int atest = attach_channel(&mpool, &ch);
        if (atest != SUCCESS) {
            printf("Channel Attach: FAILED\n");
            return FAILED;
        } else {
            printf("Channel Attach: SUCCESS\n");
        }

    } else {
        printf("Invalid test setup.  Valid args are: create | attach.  Got: %i (%s)\n", argc, argv[argc-1]);
        return FAILED;
    }

    dragonChannelSendh_t csend;
    dragonChannelRecvh_t cpeek;
    err = util_create_open_send_recv_handles(&ch, &csend, NULL, &cpeek, NULL);
    if (err != SUCCESS) {
        printf("Channel Create/Open Send/Recv Handles: FAILED\n");
        return FAILED;
    } else {
        printf("Channel Create/Open Send/Recv Handles: SUCCESS\n");
    }

    if (argc == 2 && strcmp(argv[1], "create") == 0) {
        printf("Press Enter to start communication test\n");
        // this is just pausing for some input
        char * buff = malloc(8);
        char * rv = fgets(buff, 8, stdin);
        if (rv != buff) {
            printf("Strange behavior with fgets\n");
            return FAILED;
        }
        free(buff);
    }

    /* Perform a timed block of sends and receives from the Channel */
    printf("Measuring performance for %li byte messages\n", sizeof(int)*MSGWORDS);

    timespec_t t1, t2, t3;
    clock_gettime(CLOCK_MONOTONIC, &t1);

    err = get_msg_for_send(&send_msg);
    if (err != SUCCESS)
        printf("Failed to get message for send operation: got EC=%i\n", err);

    for (int j = 0; j < NREPEATS; j ++) {

        for (int i = 0; i < NMSGS; i++) {

            //printf("Send message %i of %i\n", i+1, NMSGS);

            while (1) {
                err = dragon_chsend_send_msg(&csend, &send_msg, NULL, NULL);
                if (err != DRAGON_CHANNEL_FULL)
                    break;
            }
            if (err != DRAGON_SUCCESS) {
                char * errstr = dragon_getlasterrstr();
                printf("Failed to send message: got EC=%i\nERRSTR = \n%s\n", err, errstr);
                return FAILED;
            }
        }

        // Uncomment this block to check number of messages sent
        /*
        if (j == 0) {
            uint64_t msg_count;
            dragon_channel_message_count(&ch, &msg_count);
            printf("Messages: %lu\n", msg_count);
        }
        */
        clock_gettime(CLOCK_MONOTONIC, &t3);


        for (int i = 0; i < NMSGS; i++) {

            //printf("Peek at message %i of %i\n", i+1, NMSGS);

            err = dragon_channel_message_init(&peek_msg, NULL, NULL);
            if (err != DRAGON_SUCCESS)
                err_fail(err, "Failed to init peek message");

            while (1) {
                err = dragon_chrecv_peek_msg(&cpeek, &peek_msg);
                if (err != DRAGON_CHANNEL_EMPTY)
                    break;
            }
            if (err != DRAGON_SUCCESS)
                err_fail(err, "Failed to peek at message");

            err = dragon_chrecv_pop_msg(&cpeek);
            if (err != DRAGON_SUCCESS)
                err_fail(err, "Failed to pop message");

            if (i == NMSGS-1) {

                int * pmsg_ptr;
                err = dragon_memory_get_pointer(peek_msg._mem_descr, (void *)&pmsg_ptr);

                if (err != DRAGON_SUCCESS)
                    err_fail(err, "Failed to get message memory pointer");

                /* Check that we got back what we should from the message */
                for (int i = 0; i < MSGWORDS; i++) {
                    if (pmsg_ptr[i] != i)
                        printf("Error: element %i is wrong (%i != %i)\n", i, pmsg_ptr[i], i);
                }

            }

            //dbg comment explaining this
            err = dragon_channel_message_destroy(&peek_msg, 1);
            if (err != DRAGON_SUCCESS)
                err_fail(err, "Failed to destroy peek message");

        }

    }

    //dbg comment explaining this
    err = dragon_channel_message_destroy(&send_msg, 1);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Failed to destroy peek message");

    clock_gettime(CLOCK_MONOTONIC, &t2);
    double sendtime= 1e-9 * (double)(BILL * (t3.tv_sec - t1.tv_sec) + (t3.tv_nsec - t1.tv_nsec));
    double etime   = 1e-9 * (double)(BILL * (t2.tv_sec - t1.tv_sec) + (t2.tv_nsec - t1.tv_nsec));
    double mrate   = NREPEATS * NMSGS / etime;
    double uni_lat = 5e5 * etime / (NREPEATS * NMSGS);
    double uni_send_lat = 5e5 * sendtime / (NREPEATS * NMSGS);
    double bw      = (double)sizeof(int)*MSGWORDS * NREPEATS * NMSGS / (1024UL * 1024UL * etime);
    printf("Perf: pt2pt message rate = %5.3f msgs/sec, uni-directional latency = %5.3f usec, BW = %5.3f MiB/s\n",
           mrate, uni_lat, bw);

    printf("Send/Peek-Pop Split\n");
    printf("Send Time %5.3f usec\n", uni_send_lat);
    printf("Peek-Pop Time %5.3f usec\n", uni_lat - uni_send_lat);

    printf("****** Now measuring the no-copy path for sending messages *******\n");

    clock_gettime(CLOCK_MONOTONIC, &t1);

    for (int j = 0; j < NREPEATS; j ++) {

        for (int i = 0; i < NMSGS; i++) {

            err = get_msg_for_send(&send_msg);
            if (err != SUCCESS)
                printf("Failed to get message for send operation: got EC=%i\n", err);

            //printf("Send message %i of %i\n", i+1, NMSGS);

            while (1) {
                err = dragon_chsend_send_msg(&csend, &send_msg, DRAGON_CHANNEL_SEND_TRANSFER_OWNERSHIP, NULL);
                if (err != DRAGON_CHANNEL_FULL)
                    break;
            }
            if (err != DRAGON_SUCCESS)
                err_fail(err, "Failed to send message");
        }

        // Uncomment this block to check number of messages sent
        /*
        if (j == 0) {
            uint64_t msg_count;
            dragon_channel_message_count(&ch, &msg_count);
            printf("Messages: %lu\n", msg_count);
        }
        */
        clock_gettime(CLOCK_MONOTONIC, &t3);

        for (int i = 0; i < NMSGS; i++) {

            //printf("Peek at message %i of %i\n", i+1, NMSGS);

            err = dragon_channel_message_init(&peek_msg, NULL, NULL);
            if (err != DRAGON_SUCCESS)
                err_fail(err, "Failed to init peek message");

            while (1) {
                err = dragon_chrecv_peek_msg(&cpeek, &peek_msg);
                if (err != DRAGON_CHANNEL_EMPTY)
                    break;
            }
            if (err != DRAGON_SUCCESS)
                err_fail(err, "Failed to peek at message");

            err = dragon_chrecv_pop_msg(&cpeek);
            if (err != DRAGON_SUCCESS)
                err_fail(err, "Failed to pop message");

            if (i == NMSGS-1) {

                int * pmsg_ptr;
                err = dragon_memory_get_pointer(peek_msg._mem_descr, (void *)&pmsg_ptr);
                if (err != DRAGON_SUCCESS)
                    err_fail(err, "Failed to get message memory pointer");

                /* Check that we got back what we should from the message */
                for (int i = 0; i < MSGWORDS; i++) {
                    if (pmsg_ptr[i] != i)
                        printf("Error: element %i is wrong (%i != %i)\n", i, pmsg_ptr[i], i);
                }

            }

            //dbg comment explaining this
            err = dragon_channel_message_destroy(&peek_msg, 1);
            if (err != DRAGON_SUCCESS) {
                char * errstr = dragon_getlasterrstr();
                printf("Failed to destroy peek message: EC=%i, ERRSTR=%s\n", err, errstr);
                return FAILED;
            }
        }
    }

    clock_gettime(CLOCK_MONOTONIC, &t2);
    sendtime= 1e-9 * (double)(BILL * (t3.tv_sec - t1.tv_sec) + (t3.tv_nsec - t1.tv_nsec));
    etime   = 1e-9 * (double)(BILL * (t2.tv_sec - t1.tv_sec) + (t2.tv_nsec - t1.tv_nsec));
    mrate   = NREPEATS * NMSGS / etime;
    uni_lat = 5e5 * etime / (NREPEATS * NMSGS);
    uni_send_lat = 5e5 * sendtime / (NREPEATS * NMSGS);
    bw      = (double)sizeof(int)*MSGWORDS * NREPEATS * NMSGS / (1024UL * 1024UL * etime);
    printf("Perf: pt2pt message rate = %5.3f msgs/sec, uni-directional latency = %5.3f usec, BW = %5.3f MiB/s\n",
           mrate, uni_lat, bw);
    printf("Send/Peek-Pop Split\n");
    printf("Send Time %5.3f usec\n", uni_send_lat);
    printf("Peek-Pop Time %5.3f usec\n", uni_lat - uni_send_lat);

    int csrhtest = util_close_send_recv_handles(&csend, &cpeek);
    if (csrhtest != SUCCESS) {
        printf("Channel Close Send/Recv Handles: FAILED\n");
        return FAILED;
    } else {
        printf("Channel Close Send/Recv Handles: SUCCESS\n");
    }

    if (argc == 2 && strcmp(argv[1], "create") == 0) {

        int dmctest = detach_mem_channel(&mpool, &ch);
        if (dmctest != SUCCESS) {
            printf("Memory and Channel Detach: FAILED\n");
            return FAILED;
        } else {
            printf("Memory and Channel Detach: SUCCESS\n");
        }

    } else if (argc == 1 || (argc == 2 && strcmp(argv[1], "attach") == 0)) {

        int dmctest = destroy_mem_channel(&mpool, &ch);
        if (dmctest != SUCCESS) {
            printf("Memory and Channel Destroy: FAILED\n");
            return FAILED;
        } else {
            printf("Memory and Channel Destroy: SUCCESS\n");
        }

    }

    printf("Exiting\n");
    return TEST_STATUS;
}
