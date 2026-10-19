#include <dragon/channels.h>
#include <dragon/channelsets.h>
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

#include "../_ctest_utils.h"

#define SERFILE "channel_serialized.dat"
#define M_UID 0
#define POOL_M_UID 2
#define POOL "channelset_test"
#define NUM_CHANNELS 5
#define CHANNEL_POLLIN_CAPACITY 10
#define CHANNEL_POLLOUT_CAPACITY 3
#define MSG_SIZE 200

static bool callback_finished = false;
static bool signal_finished = false;

/* This test creates five channels and some processes to test polling
   across channels.
*/

dragonChannelSetEventNotification_t* global_event;
dragonError_t global_err;
char* global_err_str = NULL;
dragonChannelDescr_t** global_channel_ptrs;

void
check_result(dragonError_t err, dragonError_t expected_err, int* tests_passed, int* tests_attempted)
{
    (*tests_attempted)++;

    if (err != expected_err) {
        printf("Test %d Failed with error code %s\n", *tests_attempted, dragon_get_rc_string(err));
        printf("%s\n", dragon_getlasterrstr());
        exit(0);
    }
    else
        (*tests_passed)++;
}

dragonError_t create_pool(dragonMemoryPoolDescr_t* mpool) {
    /* Create a memory pool to allocate messages and a Channel out of */
    size_t mem_size = 1UL<<31;
    //printf("Allocate pool %lu B\n", mem_size);

    dragonError_t err = dragon_memory_pool_create(mpool, mem_size, POOL, POOL_M_UID, NULL);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Failed to create memory pool");

    return DRAGON_SUCCESS;
}

dragonError_t create_channels(dragonMemoryPoolDescr_t* mpool, dragonChannelDescr_t channel[], int arr_size, int capacity, int start_cuid) {
    int k;

    /* Create a Channel attributes structure so we can tune the Channel */
    dragonChannelAttr_t cattr;
    dragonError_t err = dragon_channel_attr_init(&cattr);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Failed to init channel attributes");

    for (k=0;k<arr_size;k++) {
        /* Set the Channel capacity to our message limit for this test */
        cattr.capacity = capacity;
        cattr.bytes_per_msg_block = B_PER_MBLOCK;
        //printf("Channel capacity set to %li\n", cattr.capacity);

        /* Create the Channel in the memory pool */
        err = dragon_channel_create(&channel[k], k+start_cuid, mpool, &cattr);
        if (err != DRAGON_SUCCESS)
            err_fail(err, "Failed to create a channel");
    }

    return DRAGON_SUCCESS;
}


dragonError_t
create_open_send_handle(dragonChannelDescr_t * ch, dragonChannelSendh_t * csend)
{
    /* Create a send handle from the Channel */
    dragonError_t err = dragon_channel_sendh(ch, csend, NULL);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Failed to create send handle");

    /* Open the send handle for writing */
    err = dragon_chsend_open(csend);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Failed to open send handle");

    return DRAGON_SUCCESS;
}

dragonError_t
create_open_recv_handle(dragonChannelDescr_t * ch, dragonChannelRecvh_t * crecv, dragonChannelRecvAttr_t* rattrs) {

    /* Create a receive handle from the Channel */

    dragonError_t err = dragon_channel_recvh(ch, crecv, rattrs);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Failed to create recev handle");

    /* Open the receive handle */
    err = dragon_chrecv_open(crecv);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Failed to open recv handle");

    return DRAGON_SUCCESS;
}

int
proc_receiver(dragonChannelDescr_t* channel, dragonMemoryPoolDescr_t* pool)
{
    dragonChannelRecvh_t recvh;
    dragonError_t err;
    dragonMessage_t msg;


    err = dragon_channel_message_init(&msg, NULL, NULL);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Failed to init message");

    sleep(1);

    err = create_open_recv_handle(channel, &recvh, NULL);
    if (err != DRAGON_SUCCESS)
        return err;

    err = dragon_chrecv_get_msg(&recvh, &msg);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Failed to send message");

    err = dragon_channel_message_destroy(&msg, true);
    if (err != DRAGON_SUCCESS) {
        printf("Error in destroying message\n");
    }

    dragon_chrecv_close(&recvh);

    return DRAGON_SUCCESS;
}

int
proc_sender(dragonChannelDescr_t* channel, dragonMemoryPoolDescr_t* pool, int id)
{
    dragonChannelSendh_t sendh;
    dragonError_t err;
    dragonMessage_t msg;
    dragonMemoryDescr_t mem_desc;
    char* msg_data;

    err = dragon_memory_alloc(&mem_desc, pool, MSG_SIZE);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Failed to allocate message buffer from pool");

    /* Get a pointer to that memory and fill up a message payload */
    err = dragon_memory_get_pointer(&mem_desc, (void *)&msg_data);
    for (int i = 0; i < MSG_SIZE; i++) {
        msg_data[i] = i;
    }

    err = dragon_channel_message_init(&msg, &mem_desc, NULL);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Failed to create message");

    err = create_open_send_handle(channel, &sendh);
    if (err != DRAGON_SUCCESS)
        return err;

    sleep(1);

    err = dragon_chsend_send_msg(&sendh, &msg, DRAGON_CHANNEL_SEND_TRANSFER_OWNERSHIP, NULL);
    if (err != DRAGON_SUCCESS) {
        char err_msg[200];
        snprintf(err_msg, 199, "Failed to send message from client %d\n", id);
        err_fail(err, err_msg);
    }

    dragon_chsend_close(&sendh);

    return DRAGON_SUCCESS;
}

void
callback(void * user_def_ptr, dragonChannelSetEventNotification_t * event, dragonError_t err, char* err_str)
{
    dragonChannelDescr_t** channel_ptrs = (dragonChannelDescr_t**) user_def_ptr;
    dragonMessage_t msg;
    dragonChannelRecvh_t recvh;

    if (err != DRAGON_SUCCESS) {
        if (err_str == NULL)
            printf("Error string was NULL, err was %d in callback\n",err);
        else
            printf("Error string was %s and err=%d in callback\n", err_str, err);
    }

    /* Test passes if pollin and correct channel */
    if ((event->revent & DRAGON_CHANNEL_POLLIN) == 0)
        printf("The channel event mask was not correct in callback poll test.\n");

    if (event->channel_idx != 2)
        printf("Receiving message on channel %d in callback but expected channel 2.\n", event->channel_idx);

    err = create_open_recv_handle(channel_ptrs[event->channel_idx], &recvh, NULL);
    if (err != DRAGON_SUCCESS) {
        printf("Error in callback opening recv handle\n");
    }

    err = dragon_channel_message_init(&msg, NULL, NULL);
    if (err != DRAGON_SUCCESS) {
        printf("Error in callback initing message\n");
    }

    err = dragon_chrecv_get_msg(&recvh, &msg);
    if (err != DRAGON_SUCCESS) {
        printf("Error in callback receiving message\n");
    }

    err = dragon_channel_message_destroy(&msg, true);
    if (err != DRAGON_SUCCESS) {
        printf("Error in callback destroying message\n");
    }

    err = dragon_chrecv_close(&recvh);
    if (err != DRAGON_SUCCESS) {
        printf("Error in callback closing handle\n");
    }

    callback_finished = true;

    free(event);
}

void signal_handler(int sig) {
    signal(sig, SIG_IGN);
    signal(SIGUSR1, SIG_DFL);

    dragonMessage_t msg;
    dragonChannelRecvh_t recvh;
    dragonError_t err;

    if (global_err != DRAGON_SUCCESS) {
        if (global_err_str == NULL)
            printf("Error string was NULL, err was %d in callback\n",global_err);
        else
            printf("Error string was %s and err=%d in callback\n", global_err_str, global_err);
    }

    /* Test passes if pollin and correct channel */
    if ((global_event->revent & DRAGON_CHANNEL_POLLIN) == 0)
        printf("The channel event mask was not correct in callback poll test.\n");

    if (global_event->channel_idx != 1)
        printf("Receiving message on channel %d in callback but expected channel 2.\n", global_event->channel_idx);

    err = create_open_recv_handle(global_channel_ptrs[global_event->channel_idx], &recvh, NULL);
    if (err != DRAGON_SUCCESS) {
        printf("Error in callback opening recv handle\n");
    }

    err = dragon_channel_message_init(&msg, NULL, NULL);
    if (err != DRAGON_SUCCESS) {
        printf("Error in callback initing message\n");
    }

    err = dragon_chrecv_get_msg(&recvh, &msg);
    if (err != DRAGON_SUCCESS) {
        printf("Error in callback receiving message\n");
    }

    err = dragon_channel_message_destroy(&msg, true);
    if (err != DRAGON_SUCCESS) {
        printf("Error in callback destroying message\n");
    }

    err = dragon_chrecv_close(&recvh);
    if (err != DRAGON_SUCCESS) {
        printf("Error in callback closing handle\n");
    }

    signal_finished = true;

    free(global_event);
}

static bool found_channel(dragonChannelDescr_t* channel, dragonChannelDescr_t** channels_ptrs, int num_channels)
{
    for (int k=0; k<NUM_CHANNELS; k++)
        if (channel->_idx == channels_ptrs[k]->_idx)
            return true;

    return false;
}

int main() {
    dragonError_t err;
    int tests_passed = 0;
    int tests_attempted = 0;
    int status;

    dragonMemoryPoolDescr_t pool;
    dragonChannelDescr_t channels[NUM_CHANNELS];
    dragonChannelDescr_t* channel_ptrs[NUM_CHANNELS];
    dragonChannelSetDescr_t channel_set;
    dragonChannelSetEventNotification_t* event;
    dragonChannelRecvh_t recvh;
    dragonMessage_t msg;
    dragonChannelDescr_t* channels_reported;
    int num_channels_reported;

    /* Creating the channel set and the pool */
    for (int k=0;k<NUM_CHANNELS;k++)
        channel_ptrs[k] = &channels[k];

    err = create_pool(&pool);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = create_channels(&pool, channels, NUM_CHANNELS, CHANNEL_POLLIN_CAPACITY, 0);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = dragon_channelset_create(channel_ptrs, NUM_CHANNELS, DRAGON_CHANNEL_POLLIN, &pool, NULL, &channel_set);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = dragon_channelset_get_channels(&channel_set, &channels_reported, &num_channels_reported);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    if (num_channels_reported != NUM_CHANNELS) {
        printf("Failed: The dragon_channelset_get_channels reported %d channels, it should have been %d\n", num_channels_reported, NUM_CHANNELS);
        exit(0);
    }

    for (int k=0;k<NUM_CHANNELS;k++) {
        if (found_channel(&channels_reported[k],channel_ptrs, NUM_CHANNELS)) {
            tests_passed+=1;
        } else {
            printf("Failed: Did not find channel in list returned by dragon_channelset_get_channels\n");
        }
        tests_attempted+=1;
    }

    if (tests_attempted != tests_passed)
        exit(0);

    free(channels_reported);

    /* Testing mainline functionality */

    if (fork()==0) {
        return proc_sender(&channels[3], &pool, 3);
    }

    err = dragon_channelset_poll(&channel_set, DRAGON_IDLE_WAIT, NULL, NULL, NULL, &event);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    /* Test passes if pollin and correct channel */
    if ((event->revent & DRAGON_CHANNEL_POLLIN) != 0)
      check_result(DRAGON_SUCCESS, DRAGON_SUCCESS, &tests_passed, &tests_attempted);
    else {
      tests_attempted += 1;
      printf("The channel event mask was not correct in poll test.\n");
    }

    if (event->channel_idx == 3)
        check_result(DRAGON_SUCCESS, DRAGON_SUCCESS, &tests_passed, &tests_attempted);
    else {
        tests_attempted += 1;
        printf("The channel event channel index was %d and should have been 3 in poll test.\n", event->channel_idx);
    }

    while (wait(&status) < 0);
    check_result(status, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = create_open_recv_handle(channel_ptrs[3], &recvh, NULL);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = dragon_channel_message_init(&msg, NULL, NULL);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = dragon_chrecv_get_msg(&recvh, &msg);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = dragon_channel_message_destroy(&msg, true);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = dragon_chrecv_close(&recvh);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    free(event);

    if (fork()==0) {
        return proc_sender(&channels[0], &pool, 0);
    }

    err = dragon_channelset_poll(&channel_set, DRAGON_IDLE_WAIT, NULL, NULL, NULL, &event);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    /* Test passes if pollin and correct channel */
    if ((event->revent & DRAGON_CHANNEL_POLLIN) != 0)
      check_result(DRAGON_SUCCESS, DRAGON_SUCCESS, &tests_passed, &tests_attempted);
    else {
      tests_attempted += 1;
      printf("The channel event mask was not correct in poll test.\n");
    }

    if (event->channel_idx == 0)
        check_result(DRAGON_SUCCESS, DRAGON_SUCCESS, &tests_passed, &tests_attempted);
    else {
        tests_attempted += 1;
        printf("The channel event channel index was %d and should have been 0 in poll test.\n", event->channel_idx);
    }

    while (wait(&status) < 0);
    check_result(status, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    /* Calling reset is unnecessary, but done here to test it */

    err = dragon_channelset_reset(&channel_set);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = create_open_recv_handle(channel_ptrs[0], &recvh, NULL);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = dragon_channel_message_init(&msg, NULL, NULL);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = dragon_chrecv_get_msg(&recvh, &msg);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = dragon_channel_message_destroy(&msg, true);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = dragon_chrecv_close(&recvh);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    free(event);

    if (fork()==0) {
        return proc_sender(&channels[4], &pool, 4);
    }

    err = dragon_channelset_poll(&channel_set, DRAGON_SPIN_WAIT, NULL, NULL, NULL, &event);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    /* Test passes if pollin and correct channel */
    if ((event->revent & DRAGON_CHANNEL_POLLIN) != 0)
      check_result(DRAGON_SUCCESS, DRAGON_SUCCESS, &tests_passed, &tests_attempted);
    else {
      tests_attempted += 1;
      printf("The channel event mask was not correct in poll test.\n");
    }

    if (event->channel_idx == 4)
        check_result(DRAGON_SUCCESS, DRAGON_SUCCESS, &tests_passed, &tests_attempted);
    else {
        tests_attempted += 1;
        printf("The channel event channel index was %d and should have been 4 in poll test.\n", event->channel_idx);
    }

    while (wait(&status) < 0);
    check_result(status, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = create_open_recv_handle(channel_ptrs[4], &recvh, NULL);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = dragon_channel_message_init(&msg, NULL, NULL);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = dragon_chrecv_get_msg(&recvh, &msg);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = dragon_channel_message_destroy(&msg, true);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = dragon_chrecv_close(&recvh);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    free(event);

    /* Test the callback interface */

    err = dragon_channelset_notify_callback(&channel_set, &channel_ptrs, DRAGON_IDLE_WAIT, NULL, NULL, NULL, callback);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    if (fork()==0) {
        return proc_sender(&channels[2], &pool, 2);
    }

    int count = 0;
    while (callback_finished == false && count < 10) {
        sleep(1);
        count += 1;
    }

    if (callback_finished == false) {
        printf("Error: Callback did not finish\n");
        tests_attempted += 1;
    }

    while (wait(&status) < 0);

    /* Test the signal handler */

    signal(SIGUSR1, signal_handler);
    global_channel_ptrs = channel_ptrs;

    err = dragon_channelset_notify_signal(&channel_set, DRAGON_IDLE_WAIT, NULL, NULL, NULL, SIGUSR1, &global_event, &global_err, &global_err_str);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    if (fork()==0) {
        return proc_sender(&channels[1], &pool, 1);
    }

    count = 0;
    while (signal_finished == false && count < 10) {
        sleep(1);
        count += 1;
    }

    if (signal_finished == false) {
        printf("Error: Signal handler did not finish\n");
        tests_attempted += 1;
    }

    while (wait(&status) < 0);

    err = dragon_channelset_destroy(&channel_set);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    /* Test the synchronized version of channel sets where poll operations cannot be missed. */

    /* Creating a synchronized channel set makes sense for only POLLIN or POLLOUT,
       but not both at the same time. Requiring both would lead to a deadlock since
       you would need to both send and receive at the same time to allow any process
       to proceed */
    dragonChannelSetAttrs_t attrs;
    err = dragon_channelset_attr_init(&attrs);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    attrs.sync_type = DRAGON_SYNC;

    err = dragon_channelset_create(channel_ptrs, NUM_CHANNELS, DRAGON_CHANNEL_POLLIN, &pool, &attrs, &channel_set);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    for (int k=0;k<NUM_CHANNELS;k++) {
        // printf("synchronized polling channelset\n");
        if (fork()==0) {
            return proc_sender(&channels[k], &pool, k+10);
        }

        /* In practice, polling should be done in a hot loop to allow all channels in the channelset
           to continue to allow messages to be sent to them (in the case of POLLIN). The poll
           will block when no message is available on any of the channels. */

        err = dragon_channelset_poll(&channel_set, DRAGON_SPIN_WAIT, NULL, NULL, NULL, &event);
        check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

        /* Test passes if pollin and correct channel */
        if ((event->revent & DRAGON_CHANNEL_POLLIN) != 0)
            check_result(DRAGON_SUCCESS, DRAGON_SUCCESS, &tests_passed, &tests_attempted);
        else {
            tests_attempted += 1;
            printf("The channel event mask was not correct in sync poll test.\n");
        }

        if (event->channel_idx == k)
            check_result(DRAGON_SUCCESS, DRAGON_SUCCESS, &tests_passed, &tests_attempted);
        else {
            tests_attempted += 1;
            printf("The channel event channel index was %d and should have been %d in the sync poll test.\n", event->channel_idx, k);
        }

        err = create_open_recv_handle(channel_ptrs[k], &recvh, NULL);
        check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

        err = dragon_channel_message_init(&msg, NULL, NULL);
        check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

        // printf("Getting message from channel idx=%d\n", event->channel_idx);

        err = dragon_chrecv_get_msg(&recvh, &msg);
        check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

        err = dragon_channel_message_destroy(&msg, true);
        check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

        err = dragon_chrecv_close(&recvh);
        check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

        while (wait(&status) < 0);
    }

    check_result(status, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = dragon_channelset_destroy(&channel_set);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    for (int k=0;k<NUM_CHANNELS;k++) {
        err = dragon_channel_destroy(channel_ptrs[k]);
        check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);
    }

    /* Now test ChannelSet POLLOUT support */

    err = create_channels(&pool, channels, NUM_CHANNELS, CHANNEL_POLLOUT_CAPACITY, NUM_CHANNELS);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    for (int k=0;k<NUM_CHANNELS;k++)
        if (fork()==0) {
            return proc_sender(channel_ptrs[k], &pool, k+20);
        }

    for (int k=0;k<NUM_CHANNELS;k++)
        while (wait(&status) < 0);

    /* Below the event mask is set to POLLIN when it should be POLLOUT so we
       can test the set_event_mask below */
    err = dragon_channelset_create(channel_ptrs, NUM_CHANNELS, DRAGON_CHANNEL_POLLIN, &pool, NULL, &channel_set);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    short event_mask;

    err = dragon_channelset_get_event_mask(&channel_set, &event_mask);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = dragon_channelset_set_event_mask(&channel_set, DRAGON_CHANNEL_POLLOUT);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = dragon_channelset_poll(&channel_set, DRAGON_IDLE_WAIT, NULL, NULL, NULL, &event);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    /* Test passes if pollin and correct channel */
    if ((event->revent & DRAGON_CHANNEL_POLLOUT) != 0)
      check_result(DRAGON_SUCCESS, DRAGON_SUCCESS, &tests_passed, &tests_attempted);
    else {
      tests_attempted += 1;
      printf("The channel event mask was not correct in pollout test.\n");
    }

    if (event->channel_idx == 0)
        check_result(DRAGON_SUCCESS, DRAGON_SUCCESS, &tests_passed, &tests_attempted);
    else {
        tests_attempted += 1;
        printf("The channel event channel index was %d and should have been 0 in pollout test.\n", event->channel_idx);
    }

    err = dragon_channelset_destroy(&channel_set);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    for (int k=0;k<NUM_CHANNELS;k++) {
        err = dragon_channel_destroy(channel_ptrs[k]);
        check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);
    }

    err = dragon_memory_pool_destroy(&pool);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    printf("Passed %d of %d tests.\n", tests_passed, tests_attempted);

    TEST_STATUS = (tests_passed == tests_attempted ? SUCCESS : FAILED);

    return TEST_STATUS;
}
