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

#include "../_ctest_utils.h"

#undef NMSGS
#define NMSGS 10
#define SERFILE "channel_serialized.dat"
#define MFILE "channels_test"
#define M_UID 0
#define POLL_SIZE 10


dragonError_t
create_pool(dragonMemoryPoolDescr_t* mpool)
{
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


dragonError_t
create_channels(dragonMemoryPoolDescr_t* mpool, dragonChannelDescr_t channel[], int arr_size)
{
    int k;

    for (k=0;k<arr_size;k++) {
        //printf("Creating Channel %d in ring\n",k);
        /* Create a Channel attributes structure so we can tune the Channel */
        dragonChannelAttr_t cattr;
        dragonError_t cerr = dragon_channel_attr_init(&cattr);

        /* Set the Channel capacity to our message limit for this test */
        cattr.capacity = NMSGS;
        cattr.bytes_per_msg_block = B_PER_MBLOCK;
        //printf("Channel capacity set to %li\n", cattr.capacity);

        /* Create the Channel in the memory pool */
        cerr = dragon_channel_create(&channel[k], k, mpool, &cattr);
        if (cerr != DRAGON_SUCCESS)
            err_fail(cerr, "Failed to create Channel");
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
            err_fail(cerr, "Failed to serialize Channel");
    }

    return SUCCESS;
}

dragonError_t
serialize_channel(dragonChannelDescr_t * ch)
{
    dragonChannelSerial_t ch_ser;
    dragonError_t cerr = dragon_channel_serialize(ch, &ch_ser);
    if (cerr != DRAGON_SUCCESS)
        err_fail(cerr, "Failed to serialize channel");

    FILE * fp = fopen(SERFILE, "w");
    fwrite(ch_ser.data, 1, ch_ser.len, fp);
    fclose(fp);

    return SUCCESS;
}


dragonError_t
poll_proc(int idx, dragonChannelDescr_t channels[])
{

    dragonChannelDescr_t* in;
    int err;

    in = &channels[0];

    err = dragon_channel_poll(in, DRAGON_IDLE_WAIT, DRAGON_CHANNEL_POLLIN, NULL, NULL);
    if (err != SUCCESS) {
        printf("Error: Could not poll in proc %d with error code %d\n", idx, err);
        err_fail(err, "Failed to create message");
    }

    return SUCCESS;
}

dragonError_t
poll_proc2(int idx, dragonChannelDescr_t channels[])
{

    dragonChannelDescr_t* in;
    int err;

    in = &channels[0];

    printf("Now waiting on poll in poll_proc2\n");
    err = dragon_channel_poll(in, DRAGON_IDLE_WAIT, DRAGON_CHANNEL_POLLOUT, NULL, NULL);
    if (err != SUCCESS) {
        printf("Error: Could not poll in proc %d with error code %d\n", idx, err);
        err_fail(err, "Failed to create message");
    }
    printf("Now completed poll operation when waiting for space.\n");

    return SUCCESS;
}

int main(int argc, char *argv[])
{
    dragonError_t err;
    dragonMemoryPoolDescr_t mpool;
    dragonMemoryDescr_t msg_buf;
    int * msg_ptr;
    dragonMessage_t msg;
    dragonChannelDescr_t channels[POLL_SIZE];
    int num_channels = 1;

    err = create_pool(&mpool);
    if (err != SUCCESS)
        err_fail(err, "Failed to create poll pool");

    for (int k = 0; k < num_channels; k++) {
        err = util_create_channel(&mpool, &channels[k], k, 0, 0);
        if (err != SUCCESS)
            err_fail(err, "Failed while creating channels");
    }

    /* Allocate managed memory as buffer space for both send and receive */
    err = dragon_memory_alloc(&msg_buf, &mpool, sizeof(int)*MSGWORDS);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Failed to allocate message buffer from pool");

    /* Get a pointer to that memory and fill up a message payload */
    err = dragon_memory_get_pointer(&msg_buf, (void *)&msg_ptr);
    for (int i = 0; i < MSGWORDS; i++) {
        msg_ptr[i] = i;
    }

    /* Create a message using that memory */
    err = dragon_channel_message_init(&msg, &msg_buf, NULL);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Failed to create message");

    int status;
    int k;

    for (k=0;k<POLL_SIZE;k++) {
        if (fork()==0) {
            status = poll_proc(k, channels);
            return status;
        }
    }

    dragonChannelSendh_t sendh;
    err = util_create_open_send_recv_handles(&channels[0], &sendh, NULL, NULL, NULL);
    if (err != SUCCESS)
        err_fail(err, "Failed to create/open send handle");

    printf("Sending message to channel\n");

    err = dragon_chsend_send_msg(&sendh,&msg,DRAGON_CHANNEL_SEND_TRANSFER_OWNERSHIP,NULL);
    if (err != SUCCESS)
        err_fail(err, "Failed to send initial message");

    for (k=0;k<POLL_SIZE;k++) {
        int pid = wait(&status);
        printf("Return status of child %d was %d\n", pid, status);
    }

    for (k=0;k<NMSGS-1;k++) {
        /* Allocate managed memory as buffer space for both send and receive */
        err = dragon_memory_alloc(&msg_buf, &mpool, sizeof(int)*MSGWORDS);
        if (err != DRAGON_SUCCESS)
            main_err_fail(err, "Failed to allocate message buffer from pool", jmp_destroy_pool);

        /* Get a pointer to that memory and fill up a message payload */
        err = dragon_memory_get_pointer(&msg_buf, (void *)&msg_ptr);
        for (int i = 0; i < MSGWORDS; i++) {
            msg_ptr[i] = i;
        }

        /* Create a message using that memory */
        err = dragon_channel_message_init(&msg, &msg_buf, NULL);
        if (err != DRAGON_SUCCESS)
            main_err_fail(err, "Failed to create message", jmp_destroy_pool);

        err = dragon_chsend_send_msg(&sendh,&msg,DRAGON_CHANNEL_SEND_TRANSFER_OWNERSHIP,NULL);
        if (err != SUCCESS)
            main_err_fail(err, "Failed to send message", jmp_destroy_pool);
    }

    /* Now the channel should be full. So start poll_proc2 */
    if (fork()==0) {
        status = poll_proc2(k, channels);
        return status;
    }

    printf("Created poll_proc2\n");

    /* poll_proc2 is polling on the channel having some space left. It is full
       so receive one message from it to make room */

    dragonChannelRecvh_t recvh;

    dragonChannelRecvAttr_t rattrs;
    dragon_channel_recv_attr_init(&rattrs);
    rattrs.default_notif_type = DRAGON_RECV_SYNC_MANUAL;
    rattrs.default_timeout = DRAGON_CHANNEL_BLOCKING_NOTIMEOUT;

    sleep(1);
    printf("Now receiving message from channel\n");

    err = util_create_open_send_recv_handles(&channels[0], NULL, NULL, &recvh, &rattrs);
    if (err != SUCCESS)
        main_err_fail(err, "Could not open recv handle in main", jmp_destroy_pool);

    err = dragon_chrecv_get_msg_blocking(&recvh, &msg, NULL);
    if (err != SUCCESS)
        main_err_fail(err, "Could not recv blocking message in main", jmp_destroy_pool);

    /* Now poll_proc2 has exited */
    wait(&status);
    printf("Return status of poll_proc2 was %d\n", status);

    /* Close the send and receive handles */
    err = dragon_chsend_close(&sendh);
    if (err != DRAGON_SUCCESS)
        main_err_fail(err, "Could not close send handle", jmp_destroy_pool);

    dragon_channel_destroy(&channels[0]);

jmp_destroy_pool:
    dragon_memory_pool_destroy(&mpool);

    printf("Poll Test Complete\n");

    return TEST_STATUS;
}
