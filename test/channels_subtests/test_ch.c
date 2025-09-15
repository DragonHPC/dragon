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


    /* Here are the buffering scenarios on the send side:
        1) If the message will fit inside the message block AND the user did not specify and destination
        managed memory descriptor, we will buffer into the message block directly.

        2) If the message will NOT fit inside the message block AND the user did not specify and destination
        managed memory descriptor, we will allocate a new landing pad of managed memory.  It is by default allocated
        from the same managed memory pool as the Channel.  If the user specified a custom Pool to use in the Channel
        attributes, that will be used instead.  A serialized descriptor for that memory will be written into the
        message block, and the data is copied into the new blob of managed memory.

        3) If the user specifies the landing pad destination managed memory blob, we write a serialized descriptor for
        it into the message block and the data is copied into the managed memory blob.


       Here are the buffering scenarios on the receive side:
        1) The user does not specify a managed memory blob to copy the message into.  In that case, if the message
        was fully buffered into the message block, a new managed memory blob is allocated from the Pool and the data
        is copied into that.  A new Message is returned using that managed memory blob.  If the message was instead
        already copied into a managed memory blob (because the message was large, or we were told to use a specific
        one at send time) we simply wrap up a new message around that.

        2) If the user does specify a managed memory blob to copy the message into (via a prepared Message), we copy
        the data, no matter where it is right now, into managed memory blob for that Message.
    */

int
create_memory_channel(dragonMemoryPoolDescr_t * mpool, dragonChannelDescr_t * ch)
{
    /* Create a memory pool to allocate messages and a Channel out of */
    size_t mem_size = 1UL<<30;
    printf("Allocate pool %lu B\n", mem_size);

    size_t prealloc[] = {100,100,100,100};
    dragonMemoryPoolAttr_t attrs;
    dragonError_t derr = dragon_memory_attr_init(&attrs);
    if (derr != DRAGON_SUCCESS)
        err_fail(derr, "Failed to initialize memory attributes");

    attrs.npre_allocs = 4;
    attrs.pre_allocs = prealloc;

    char * fname = util_salt_filename(MFILE);
    derr = dragon_memory_pool_create(mpool, mem_size, fname, M_UID, &attrs);
    if (derr != DRAGON_SUCCESS)
        err_fail(derr, "Failed to create memory pool");

    free(fname);

    /* Create a Channel attributes structure so we can tune the Channel */
    dragonChannelAttr_t cattr;
    dragonError_t cerr = dragon_channel_attr_init(&cattr);

    /* Set the Channel capacity to our message limit for this test */
    cattr.capacity = NMSGS;
    cattr.bytes_per_msg_block = B_PER_MBLOCK;
    printf("Channel capacity set to %li\n", cattr.capacity);

    /* Create the Channel in the memory pool */
    cerr = dragon_channel_create(ch, C_UID, mpool, &cattr);
    if (cerr != DRAGON_SUCCESS)
        err_fail(cerr, "Failed to create channel");

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
attach_channel(dragonMemoryPoolDescr_t * mpool, dragonChannelDescr_t * ch)
{
    struct stat sb;
    stat(SERFILE, &sb);

    dragonChannelSerial_t ch_ser;
    ch_ser.len = sb.st_size;
    ch_ser.data = malloc(ch_ser.len);

    FILE * fp = fopen(SERFILE, "r+");

    dragonError_t cerr = dragon_channel_attach(&ch_ser, ch);
    if (cerr != DRAGON_SUCCESS)
        err_fail(cerr, "Failed to attach to Channel");

    cerr = dragon_channel_get_pool(ch, mpool);
    if (cerr != DRAGON_SUCCESS)
        err_fail(cerr, "Failed to retrieve pool from Channel");

    fclose(fp);
    return SUCCESS;
}



int
detach_mem_channel(dragonMemoryPoolDescr_t * mpool, dragonChannelDescr_t * ch)
{
    dragonError_t derr = dragon_memory_pool_detach(mpool);
    if (derr != DRAGON_SUCCESS)
        err_fail(derr, "Failed to detach from pool");

    return SUCCESS;
}

int
destroy_mem_channel(dragonMemoryPoolDescr_t * mpool, dragonChannelDescr_t * ch)
{
    /* Destroy the Channel and underlying memory pool */
    dragonError_t cerr = dragon_channel_destroy(ch);
    if (cerr != DRAGON_SUCCESS)
        err_fail(cerr, "Failed to destroy channel");

    dragonError_t derr = dragon_memory_pool_destroy(mpool);
    if (derr != DRAGON_SUCCESS)
        err_fail(derr, "Failed to destroy memory pool");

    return SUCCESS;
}

int main(int argc, char *argv[])
{
    dragonError_t derr;
    dragonMemoryPoolDescr_t mpool;
    dragonChannelDescr_t ch;
    dragonMessage_t recv_msg;

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
    dragonChannelRecvh_t crecv;
    derr = util_create_open_send_recv_handles(&ch, &csend, NULL, &crecv, NULL);
    if (derr != SUCCESS) {
        printf("Channel Create/Open Send/Recv Handles: FAILED\n");
        return FAILED;
    } else {
        printf("Channel Create/Open Send/Recv Handles: SUCCESS\n");
    }

    /* Allocate managed memory as buffer space for both send and receive */
    dragonMemoryDescr_t msg_buf;
    derr = dragon_memory_alloc(&msg_buf, &mpool, sizeof(int)*MSGWORDS);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to allocate message buffer from pool");
    } else {
        printf("Got memory allocation successfully\n");
    }

    /* Get a pointer to that memory and fill up a message payload */
    int * msg_ptr;
    derr = dragon_memory_get_pointer(&msg_buf, (void *)&msg_ptr);
    for (int i = 0; i < MSGWORDS; i++) {
        msg_ptr[i] = i;
    }

    /* Create a message using that memory */
    dragonMessage_t msg;
    dragonError_t cerr = dragon_channel_message_init(&msg, &msg_buf, NULL);
    if (cerr != DRAGON_SUCCESS)
        err_fail(cerr, "Failed to create message");

    dragonMemoryDescr_t rmsg_buf;
    derr = dragon_memory_alloc(&rmsg_buf, &mpool, sizeof(int)*MSGWORDS);
    if (derr != DRAGON_SUCCESS)
        err_fail(derr, "Failed to allocate message buffer from pool");

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

    for (int j = 0; j < NREPEATS; j ++) {

        for (int i = 0; i < NMSGS; i++) {

            //printf("Send message %i of %i\n", i+1, NMSGS);
            dragonError_t serr;
            while (1) {
                serr = dragon_chsend_send_msg(&csend, &msg, NULL, NULL);
                if (serr != DRAGON_CHANNEL_FULL)
                    break;
            }
            if (serr != DRAGON_SUCCESS) {
                char * errstr = dragon_getlasterrstr();
                printf("Failed to send message  Got EC=%i\nERRSTR = \n%s\n", serr, errstr);
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

            //printf("Recv message %i of %i\n", i+1, NMSGS);
            cerr = dragon_channel_message_init(&recv_msg, NULL, NULL);
            if (cerr != DRAGON_SUCCESS)
                err_fail(cerr, "Failed to init recv message");

            dragonError_t rerr;
            while (1) {
                 rerr = dragon_chrecv_get_msg(&crecv, &recv_msg);
                 if (rerr != DRAGON_CHANNEL_EMPTY)
                    break;
            }
            if (rerr != DRAGON_SUCCESS)
                err_fail(rerr, "Failed to recv message");

            if (i == NMSGS-1) {

                int * rmsg_ptr;
                dragonError_t derr = dragon_memory_get_pointer(recv_msg._mem_descr, (void *)&rmsg_ptr);

                if (derr != DRAGON_SUCCESS)
                    err_fail(derr, "Failed to recv message memory pointer");

                /* Check that we got back what we should from the message */
                for (int i = 0; i < MSGWORDS; i++) {
                    if (rmsg_ptr[i] != i)
                        printf("Error: element %i is wrong (%i != %i)\n", i, rmsg_ptr[i], i);
                }

            }

            dragonError_t cerr = dragon_channel_message_destroy(&recv_msg, 1);
            if (cerr != DRAGON_SUCCESS)
                err_fail(cerr, "Failed to destroy recv message");

        }

    }

    clock_gettime(CLOCK_MONOTONIC, &t2);
    double sendtime= 1e-9 * (double)(BILL * (t3.tv_sec - t1.tv_sec) + (t3.tv_nsec - t1.tv_nsec));
    double etime   = 1e-9 * (double)(BILL * (t2.tv_sec - t1.tv_sec) + (t2.tv_nsec - t1.tv_nsec));
    double mrate   = NREPEATS * NMSGS / etime;
    double uni_lat = 5e5 * etime / (NREPEATS * NMSGS);
    double uni_send_lat = 5e5 * sendtime / (NREPEATS * NMSGS);
    double bw      = (double)sizeof(int)*MSGWORDS * NREPEATS * NMSGS / (1024UL * 1024UL * etime);
    printf("Perf: pt2pt message rate = %5.3f msgs/sec, uni-directional latency = %5.3f usec, BW = %5.3f MiB/s\n",
           mrate, uni_lat, bw);

    printf("Send/Recv Split\n");
    printf("Send Time %5.3f usec\n", uni_send_lat);
    printf("Recv Time %5.3f usec\n", uni_lat - uni_send_lat);

    printf("****** Now measuring the no-copy path for sending messages *******\n");

    clock_gettime(CLOCK_MONOTONIC, &t1);

    for (int j = 0; j < NREPEATS; j ++) {

        for (int i = 0; i < NMSGS; i++) {

            //printf("Send message %i of %i\n", i+1, NMSGS);
            dragonError_t serr;
            while (1) {
                serr = dragon_chsend_send_msg(&csend, &msg, DRAGON_CHANNEL_SEND_TRANSFER_OWNERSHIP, NULL);
                if (serr != DRAGON_CHANNEL_FULL)
                    break;
            }
            if (serr != DRAGON_SUCCESS)
                err_fail(serr, "Failed to send message");
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

            //printf("Recv message %i of %i\n", i+1, NMSGS);

            cerr = dragon_channel_message_init(&recv_msg, NULL, NULL);
            if (cerr != DRAGON_SUCCESS)
                err_fail(cerr, "Failed to init recv message");

            dragonError_t rerr;
            while (1) {
                 rerr = dragon_chrecv_get_msg(&crecv, &recv_msg);
                 if (rerr != DRAGON_CHANNEL_EMPTY)
                    break;
            }
            if (rerr != DRAGON_SUCCESS)
                err_fail(rerr, "Failed to recv message");

            if (i == NMSGS-1) {

                int * rmsg_ptr;
                dragonError_t derr = dragon_memory_get_pointer(recv_msg._mem_descr, (void *)&rmsg_ptr);
                if (derr != DRAGON_SUCCESS)
                    err_fail(derr, "Failed to get message memory pointer");

                /* Check that we got back what we should from the message */
                for (int i = 0; i < MSGWORDS; i++) {
                    if (rmsg_ptr[i] != i)
                        printf("Error: element %i is wrong (%i != %i)\n", i, rmsg_ptr[i], i);
                }

            }

            //dragonError_t cerr = dragon_channel_message_destroy(&recv_msg, 1);
            //if (cerr != DRAGON_SUCCESS) {
            //    char * errstr = dragon_getlasterrstr();
            //    printf("Failed to destroy recv message  Got EC=%i\nERRSTR = \n%s\n", cerr, errstr);
            //    return FAILED;
            //}

        }

    }

    cerr = dragon_channel_message_destroy(&recv_msg, 1);
    if (cerr != DRAGON_SUCCESS) {
        char * errstr = dragon_getlasterrstr();
        printf("Failed to destroy recv message  Got EC=%i\nERRSTR = \n%s\n", cerr, errstr);
        return FAILED;
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
    printf("Send/Recv Split\n");
    printf("Send Time %5.3f usec\n", uni_send_lat);
    printf("Recv Time %5.3f usec\n", uni_lat - uni_send_lat);

    int csrhtest = util_close_send_recv_handles(&csend, &crecv);
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
