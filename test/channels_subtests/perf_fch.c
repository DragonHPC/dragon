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
#include <errno.h>

#define BILL 1000000000L
#define SERFILE "channel_serialized.dat"
#define M_UID 0
#define B_PER_MBLOCK 1024
#define FAN_OUT_IN 20
#define FAN_ITERS 1000
#define FAN_MSG_SIZE 512
#define FAN_CHANNEL_SIZE 100
#define FAN_POOL_M_UID 2
#define FPOOL "fan_test"

#define FAILED 1
#define SUCCESS 0


/* This test creates two channels, 100 messages, and 100 processes. Of the
   100 processes, 50 of them receive messages from the first channel and
   send to the second channel, while the other 50 of them receive messages
   from the second channel and send to the first. This repeats 1000 times.
   This measures the performance of the blocking receive on the channel and
   the blocking send if resources were exhausted.
*/


int create_fan_pool(dragonMemoryPoolDescr_t* mpool) {
    /* Create a memory pool to allocate messages and a Channel out of */
    size_t mem_size = 1UL<<31;
    //printf("Allocate pool %lu B\n", mem_size);

    dragonError_t derr = dragon_memory_pool_create(mpool, mem_size, FPOOL, FAN_POOL_M_UID, NULL);
    if (derr != DRAGON_SUCCESS) {
        char * errstr = dragon_getlasterrstr();
        printf("Failed to create the memory pool.  Got EC=%i\nERRSTR = \n%s\n",derr, errstr);
        return FAILED;
    }

    return SUCCESS;
}

int create_fan_channels(dragonMemoryPoolDescr_t* mpool, dragonChannelDescr_t channel[], int arr_size) {
    int k;

    for (k=0;k<arr_size;k++) {
        //printf("Creating Channel %d in fan\n",k);
        /* Create a Channel attributes structure so we can tune the Channel */
        dragonChannelAttr_t cattr;
        dragonError_t cerr = dragon_channel_attr_init(&cattr);

        /* Set the Channel capacity to our message limit for this test */
        cattr.capacity = FAN_CHANNEL_SIZE;
        cattr.bytes_per_msg_block = B_PER_MBLOCK;
        //printf("Channel capacity set to %li\n", cattr.capacity);

        /* Create the Channel in the memory pool */
        cerr = dragon_channel_create(&channel[k], k, mpool, &cattr);
        if (cerr != DRAGON_SUCCESS) {
            char * errstr = dragon_getlasterrstr();
            printf("Failed to create Channel.  Got EC=%i\nERRSTR = \n%s\n", cerr, errstr);
            return FAILED;
        }
    }

    return SUCCESS;
}


int init_channels_serial_desc(dragonChannelDescr_t channel[], dragonChannelSerial_t serialized_channels[], int arr_size) {
    int k;

    for (k=0; k<arr_size; k++) {
        dragonError_t cerr = dragon_channel_serialize(&channel[k], &serialized_channels[k]);
        if (cerr != DRAGON_SUCCESS) {
            char * errstr = dragon_getlasterrstr();
            printf("Failed to serialize Channel.  Got EC=%i\nERRSTR = \n%s\n", cerr, errstr);
            return FAILED;
        }
    }

    return SUCCESS;
}


int
create_open_send_handle(dragonChannelDescr_t * ch, dragonChannelSendh_t * csend)
{
    /* Create a send handle from the Channel */
    dragonError_t cerr = dragon_channel_sendh(ch, csend, NULL);
    if (cerr != DRAGON_SUCCESS) {
        char * errstr = dragon_getlasterrstr();
        printf("Failed to create send handle  Got EC=%i\nERRSTR = \n%s\n", cerr, errstr);
        return FAILED;
    }

    /* Open the send handle for writing */
    dragonError_t serr = dragon_chsend_open(csend);
    if (serr != DRAGON_SUCCESS) {
        char * errstr = dragon_getlasterrstr();
        printf("Failed to open send handle  Got EC=%i\nERRSTR = \n%s\n", cerr, errstr);
        return FAILED;
    }

    return SUCCESS;
}

int
create_open_recv_handle(dragonChannelDescr_t * ch, dragonChannelRecvh_t * crecv, dragonChannelRecvAttr_t* rattrs) {

    /* Create a receive handle from the Channel */

    dragonError_t cerr = dragon_channel_recvh(ch, crecv, rattrs);
    if (cerr != DRAGON_SUCCESS) {
        char * errstr = dragon_getlasterrstr();
        printf("Failed to create recv handle  Got EC=%i\nERRSTR = \n%s\n", cerr, errstr);
        return FAILED;
    }

    /* Open the receive handle */
    dragonError_t rerr = dragon_chrecv_open(crecv);
    if (rerr != DRAGON_SUCCESS) {
        char * errstr = dragon_getlasterrstr();
        printf("Failed to open recv handle  Got EC=%i\nERRSTR = \n%s\n", cerr, errstr);
        return FAILED;
    }

    return SUCCESS;
}


int fan_proc(int idx, dragonChannelSerial_t serialized_channels[], dragonMemoryPoolSerial_t* serialized_pool) {

    //printf("Fan Proc %d Starting\n", idx);

    dragonChannelDescr_t in;
    dragonChannelDescr_t out;

    dragonMemoryPoolDescr_t mpool;

    dragonError_t merr = dragon_memory_pool_attach(&mpool, serialized_pool);
    if (merr != DRAGON_SUCCESS) {
        char * errstr = dragon_getlasterrstr();
        printf("Failed to attach Channel.  Got EC=%i\nERRSTR = \n%s\n", merr, errstr);
        return FAILED;
    }

    dragonError_t cerr = dragon_channel_attach(&serialized_channels[idx%2], &in);
    if (cerr != DRAGON_SUCCESS) {
        char * errstr = dragon_getlasterrstr();
        printf("Failed to attach Channel.  Got EC=%i\nERRSTR = \n%s\n", cerr, errstr);
        return FAILED;
    }

    cerr = dragon_channel_attach(&serialized_channels[(idx+1)%2], &out);
    if (cerr != DRAGON_SUCCESS) {
        char * errstr = dragon_getlasterrstr();
        printf("Failed to attach Channel.  Got EC=%i\nERRSTR = \n%s\n", cerr, errstr);
        return FAILED;
    }

    dragonChannelRecvh_t recvh;
    dragonChannelSendh_t sendh;

    dragonChannelRecvAttr_t rattrs;
    dragon_channel_recv_attr_init(&rattrs);

    rattrs.default_notif_type = DRAGON_RECV_SYNC_MANUAL;
    rattrs.default_timeout = DRAGON_CHANNEL_BLOCKING_NOTIMEOUT;

    int err = create_open_recv_handle(&in, &recvh, &rattrs);

    if (err != SUCCESS) {
        printf("Error: Could not open receive handle with error code %d\n", err);
        return 0;
    }

    err = create_open_send_handle(&out, &sendh);

    if (err != SUCCESS) {
        printf("Error: Could not open send handle with error code %d\n", err);
        return 0;
    }

    /* Create a message using that memory */
    dragonMessage_t msg;

    //printf("Now ready to receive messages\n");

    int k;
    for (k=0;k<FAN_ITERS; k++) {
        err = dragon_channel_message_init(&msg, NULL, NULL);
        if (err != SUCCESS) {
            printf("Error: Could not init message with error code %d\n", err);
            fflush(stdout);
            return 0;
        }

        err = dragon_chrecv_get_msg_blocking(&recvh, &msg, NULL);
        if (err != SUCCESS) {
            printf("Error: Could not recv message in proc %d with pid=%d with error code %d\n", idx, getpid(), err);
            char * errstr = dragon_getlasterrstr();
            printf("Failed to get message  Got EC=%s\nERRSTR = \n%s\n", dragon_get_rc_string(err), errstr);
            fflush(stdout);
            return 0;
        }

        err = dragon_chsend_send_msg(&sendh,&msg,DRAGON_CHANNEL_SEND_TRANSFER_OWNERSHIP,NULL);
        if (err != SUCCESS) {
            printf("Error: Could not send message with error code %d\n", err);
            fflush(stdout);
            return 0;
        }
    }
    //printf("Fan Proc %d Exiting\n", idx);
    return SUCCESS;
}

int main(int argc, char *argv[])
{
    dragonError_t derr;
    dragonMemoryPoolDescr_t mpool;
    dragonError_t cerr;
    dragonChannelSendh_t sendh;
    dragonChannelRecvh_t recvh;
    dragonMemoryDescr_t* mem_desc_ptr;
    timespec_t t1, t2;
    dragonMessage_t final_msg;
    double etime;
    int status;
    int * rmsg_ptr;
    int err;
    int k;
    dragonChannelDescr_t fchannels[2];
    dragonChannelSerial_t fserialized_channels[2];
    dragonMemoryPoolSerial_t fserialized_pool;

    if (create_fan_pool(&mpool) != SUCCESS) {
        char * errstr = dragon_getlasterrstr();
        printf("Failed to create ring pool. Got ERRSTR = \n%s\n",  errstr);
        return FAILED;
    }

    if (dragon_memory_pool_serialize(&fserialized_pool,&mpool) != SUCCESS) {
        char * errstr = dragon_getlasterrstr();
        printf("Failed to serialize memory pool. Got ERRSTR = \n%s\n",  errstr);
        return FAILED;
    }

    if (create_fan_channels(&mpool, fchannels, 2) != SUCCESS) {
        char * errstr = dragon_getlasterrstr();
        printf("Failed to create ring channels. Got ERRSTR = \n%s\n",  errstr);
        return FAILED;
    }

    if (init_channels_serial_desc(fchannels, fserialized_channels, 2) != SUCCESS) {
        char * errstr = dragon_getlasterrstr();
        printf("Failed to serialize ring channels. Got ERRSTR = \n%s\n",  errstr);
        return FAILED;
    }

    err = create_open_send_handle(&fchannels[0], &sendh);
    if (err != SUCCESS) {
        char * errstr = dragon_getlasterrstr();
        printf("Failed to open send handle. Got EC=%i\nERRSTR = \n%s\n", err, errstr);
        return FAILED;
    }

    for (k=0;k<FAN_OUT_IN;k++) {
        //printf("Creating message\n");
        /* Create a message using that memory */
        mem_desc_ptr = malloc(sizeof(dragonMemoryDescr_t));

        derr = dragon_memory_alloc(mem_desc_ptr, &mpool, sizeof(int)*FAN_MSG_SIZE);
        if (derr != DRAGON_SUCCESS) {
            char * errstr = dragon_getlasterrstr();
            printf("Failed to allocate message buffer from pool.  Got EC=%i\nERRSTR = \n%s\n",derr, errstr);
            return FAILED;
        }

        /* Get a pointer to that memory and fill up a message payload */
        derr = dragon_memory_get_pointer(mem_desc_ptr, (void *)&rmsg_ptr);
        for (int i = 0; i < FAN_MSG_SIZE; i++) {
            rmsg_ptr[i] = i;
        }

        dragonMessage_t fan_msg;
        cerr = dragon_channel_message_init(&fan_msg, mem_desc_ptr, NULL);
        if (cerr != DRAGON_SUCCESS) {
            char * errstr = dragon_getlasterrstr();
            printf("Failed to create message  Got EC=%i\nERRSTR = \n%s\n", cerr, errstr);
            return FAILED;
        }

        err = dragon_chsend_send_msg(&sendh,&fan_msg,DRAGON_CHANNEL_SEND_TRANSFER_OWNERSHIP,NULL);
        if (err != SUCCESS) {
            char * errstr = dragon_getlasterrstr();
            printf("Failed to send initial message. Got EC=%i\nERRSTR = \n%s\n", err, errstr);
            return FAILED;
        }
    }

    clock_gettime(CLOCK_MONOTONIC, &t1);

    for (k=0;k<FAN_OUT_IN;k++) {
        if (fork()==0) {
            fan_proc(k, fserialized_channels, &fserialized_pool);
            return SUCCESS;
        }
    }


    /* Uncomment for additional information */
    // sleep(5);
    // printf("Channel 0 attrs\n");
    // dragonChannelAttr_t attrs0;
    // dragon_channel_get_attrs(&fchannels[0], &attrs0);

    // printf("   num messages %lu\n", attrs0.num_msgs);
    // printf("   num blocks available %lu\n", attrs0.num_avail_blocks);
    // printf("   receive waiters is %d\n", attrs0.blocked_receivers);
    // printf("   send waiters is %d\n", attrs0.blocked_senders);

    // printf("Channel 1 attrs\n");
    // dragonChannelAttr_t attrs1;
    // dragon_channel_get_attrs(&fchannels[1], &attrs1);

    // printf("   num messages %lu\n", attrs1.num_msgs);
    // printf("   num blocks available %lu\n", attrs1.num_avail_blocks);
    // printf("   receive waiters is %d\n", attrs1.blocked_receivers);
    // printf("   send waiters is %d\n", attrs1.blocked_senders);


    for (k=0;k<FAN_OUT_IN;k++) {
        int corpse = wait(&status);
        if (corpse < 0)
            printf("Failed to wait for process (errno = %d)\n", errno);
        else if (WIFEXITED(status))
            printf("Process %d exited with normal status 0x%.4X (status %d = 0x%.2X)\n",
                   corpse, status, WEXITSTATUS(status), WEXITSTATUS(status));
        else if (WIFSIGNALED(status))
            printf("Process %d exited because of a signal 0x%.4X (signal %d = 0x%.2X)\n",
                   corpse, status, WTERMSIG(status), WTERMSIG(status));
        else
            printf("Process %d exited with status 0x%.4X which is %s\n",
                   corpse, status, "neither a normal exit nor the result of a signal");
    }

    clock_gettime(CLOCK_MONOTONIC, &t2);

    dragonChannelRecvAttr_t rattrs;
    dragon_channel_recv_attr_init(&rattrs);
    rattrs.default_notif_type = DRAGON_RECV_SYNC_MANUAL;
    rattrs.default_timeout = DRAGON_CHANNEL_BLOCKING_NOTIMEOUT;

    err = create_open_recv_handle(&fchannels[0], &recvh, &rattrs);
    if (err != SUCCESS) {
        char * errstr = dragon_getlasterrstr();
        printf("Failed to open recv handle. Got EC=%i\nERRSTR = \n%s\n", err, errstr);
        return FAILED;
    }

    for (k=0;k<FAN_OUT_IN; k++) {

        cerr = dragon_channel_message_init(&final_msg, NULL, NULL);
        if (cerr != DRAGON_SUCCESS) {
            char * errstr = dragon_getlasterrstr();
            printf("Failed to create message  Got EC=%i\nERRSTR = \n%s\n", cerr, errstr);
            return FAILED;
        }
        cerr = dragon_chrecv_get_msg_blocking(&recvh, &final_msg, NULL);
        if (cerr != DRAGON_SUCCESS) {
            char * errstr = dragon_getlasterrstr();
            printf("Failed to receive message  Got EC=%i\nERRSTR = \n%s\n", cerr, errstr);
            return FAILED;
        }

        derr = dragon_memory_get_pointer(final_msg._mem_descr, (void *)&rmsg_ptr);
        if (derr != DRAGON_SUCCESS) {
            char * errstr = dragon_getlasterrstr();
            printf("Failed to get message buffer pointer.  Got EC=%i\nERRSTR = \n%s\n",derr, errstr);
            return FAILED;
        }

        /* Check that we got back what we should from the message */
        for (int i = 0; i < FAN_MSG_SIZE; i++) {
            if (rmsg_ptr[i] != i)
                printf("Error: element %i is wrong (%i != %i)\n", i, rmsg_ptr[i], i);
        }
    }

    etime   = 1e-9 * (double)(BILL * (t2.tv_sec - t1.tv_sec) + (t2.tv_nsec - t1.tv_nsec));
    printf("Perf: elapsed time is %5.3f seconds\n", etime);

    dragon_memory_pool_destroy(&mpool);

    printf("Fan Test Complete\n");

    return SUCCESS;
}