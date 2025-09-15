#include <dragon/managed_memory.h>
#include <dragon/channels.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <math.h>
#include <stdbool.h>
#include "ch_p2p_common.h"

#define SUCCESS 0
#define FAILED 1

#define NUM_CHANNELS 2
#define MAX_SEND_MSG_SIZES 7
#define NSAMPS 30
#define WARM_UPS 10

#define NSEC_PER_SEC 1e9

int TEST_STATUS = SUCCESS;

int SEND_MSG_SIZE[] = {
    pow(2, 3),  /* 8     */
    pow(2, 6),  /* 64    */
    pow(2, 10), /* 1024  */
    pow(2, 14), /* 16384 */
    pow(2, 16), /* 65536 */
    pow(2, 20), /* 1MB   */
    pow(2, 21), /* 2MB   */
};

dragonChannelDescr_t dragon_channels[2];
dragonMemoryPoolDescr_t dragon_default_mpool;
dragonChannelSendh_t dragon_ch_send_handle;
dragonChannelRecvh_t dragon_ch_recv_handle;

dragonError_t
run_node_0()
{
    dragonError_t derr;
    timespec_t begin_time, end_time;

    DEBUG_PRINT(("I'm the sender first\n"));

    for (int cur_send_size_idx = 0; cur_send_size_idx < MAX_SEND_MSG_SIZES; cur_send_size_idx++) {
        // prepare 2 messages (for send and recv) with the #bytes of the current j loop
        int msg_size = SEND_MSG_SIZE[cur_send_size_idx];

        dragonMemoryDescr_t msg_buf;
        DEBUG_PRINT(("Allocating message buffer of size %i\n", msg_size));
        derr = dragon_memory_alloc(&msg_buf, &dragon_default_mpool, msg_size);
        if (derr != DRAGON_SUCCESS)
            err_fail(derr, "Failed to allocate from pool");

        int *msg_ptr;
        DEBUG_PRINT(("Getting message buffer pointer\n"));
        derr = dragon_memory_get_pointer(&msg_buf, (void *)&msg_ptr);
        if (derr != DRAGON_SUCCESS)
            err_fail(derr, "Failed to get memory pointer");

        DEBUG_PRINT(("Filling buffer with data\n"));
        for (int idx = 0; idx < msg_size % sizeof(int); idx++)
            msg_ptr[idx] = idx;

        dragonMessage_t the_msg;
        DEBUG_PRINT(("Initializing channel message\n"));
        derr = dragon_channel_message_init(&the_msg, &msg_buf, NULL);
        if (derr != DRAGON_SUCCESS)
            err_fail(derr, "Failed to create message");

        int total_iterations = NSAMPS + WARM_UPS;
        for (int cur_iteration_count = 0; cur_iteration_count < total_iterations; cur_iteration_count++) {
            if (cur_iteration_count == WARM_UPS)
                clock_gettime(CLOCK_MONOTONIC, &begin_time);

            DEBUG_PRINT(("Sending channel message\n"));
            derr = dragon_chsend_send_msg(&dragon_ch_send_handle, &the_msg, DRAGON_CHANNEL_SEND_TRANSFER_OWNERSHIP, NULL);
            if (derr != DRAGON_SUCCESS)
                err_fail(derr, "Failed to send message");

            DEBUG_PRINT(("Initializing channel message\n"));
            derr = dragon_channel_message_init(&the_msg, NULL, NULL);
            if (derr != DRAGON_SUCCESS)
                err_fail(derr, "Failed to init message");

            DEBUG_PRINT(("Receiving channel message\n"));
            derr = dragon_chrecv_get_msg_blocking(&dragon_ch_recv_handle, &the_msg, NULL);
            if (derr != DRAGON_SUCCESS)
                err_fail(derr, "Failed to recv message");
        }

        clock_gettime(CLOCK_MONOTONIC, &end_time);

        DEBUG_PRINT(("Destroying channel message\n"));
        derr = dragon_channel_message_destroy(&the_msg, true);
        if (derr != DRAGON_SUCCESS)
            err_fail(derr, "Failed to destroy channel message");

        // yes, we divide by 2 because in MPI land we do 1/2 round trip latency for some reason
        double begin_seconds = (double)begin_time.tv_sec + ((double)begin_time.tv_nsec / NSEC_PER_SEC);
        double end_seconds = (double)end_time.tv_sec + ((double)end_time.tv_nsec / NSEC_PER_SEC);
        double total_time = end_seconds - begin_seconds;
        float avg_lat_t = total_time / (2 * NSAMPS);

        printf("Iterations = %i MsgSize = %i, Lat = %f seconds\n", NSAMPS, msg_size, avg_lat_t);
    }

    return DRAGON_SUCCESS;
}

dragonError_t
run_node_1()
{
    dragonError_t derr;
    timespec_t begin_time, end_time;

    DEBUG_PRINT(("I'm the receiver first\n"));

    for (int cur_send_size_idx = 0; cur_send_size_idx < MAX_SEND_MSG_SIZES; cur_send_size_idx++) {
        // prepare 2 messages (for send and recv) with the size of the current j loop
        int msg_size = SEND_MSG_SIZE[cur_send_size_idx];

        dragonMessage_t the_msg;
        DEBUG_PRINT(("Initializing channel receive message\n"));
        derr = dragon_channel_message_init(&the_msg, NULL, NULL);
        if (derr != DRAGON_SUCCESS)
            err_fail(derr, "Failed to init message");

        int total_iterations = NSAMPS + WARM_UPS;
        for (int cur_iteration_count = 0; cur_iteration_count < total_iterations; cur_iteration_count++) {
            if (cur_iteration_count == WARM_UPS)
                clock_gettime(CLOCK_MONOTONIC, &begin_time);

            DEBUG_PRINT(("Receiving channel message\n"));
            derr = dragon_chrecv_get_msg_blocking(&dragon_ch_recv_handle, &the_msg, NULL);
            if (derr != DRAGON_SUCCESS)
                err_fail(derr, "Failed to recv message");

            DEBUG_PRINT(("Sending channel message\n"));
            derr = dragon_chsend_send_msg(&dragon_ch_send_handle, &the_msg, DRAGON_CHANNEL_SEND_TRANSFER_OWNERSHIP, NULL);
            if (derr != DRAGON_SUCCESS)
                err_fail(derr, "Failed to send message");

            DEBUG_PRINT(("Initializing channel receive message\n"));
            derr = dragon_channel_message_init(&the_msg, NULL, NULL);
            if (derr != DRAGON_SUCCESS)
                err_fail(derr, "Failed to init message");
        }

        clock_gettime(CLOCK_MONOTONIC, &end_time);

        // yes, we divide by 2 because in MPI land we do 1/2 round trip latency for some reason
        double begin_seconds = (double)begin_time.tv_sec + ((double)begin_time.tv_nsec / NSEC_PER_SEC);
        double end_seconds = (double)end_time.tv_sec + ((double)end_time.tv_nsec / NSEC_PER_SEC);
        double total_time = end_seconds - begin_seconds;
        float avg_lat_t = total_time / (2 * NSAMPS);

        printf("Iterations = %i MsgSize = %i, Lat = %f seconds\n", NSAMPS, msg_size, avg_lat_t);
    }

    return DRAGON_SUCCESS;
}

int main(int argc, char **argv)
{
    dragonError_t derr;
    char *channel_descriptors[NUM_CHANNELS];
    bool node_id = atoi(argv[1]);
    char *default_mpool_descr = argv[2];

    if (argc != 5) {
        printf("Expected 4 command line arguments. %i arguments were passed\n", argc-1);
        return FAILED;
    }

    for (int idx = 0; idx < NUM_CHANNELS; idx++)
        channel_descriptors[idx] = argv[idx + 3];

    derr = setup(NUM_CHANNELS, channel_descriptors, node_id, default_mpool_descr,
                 dragon_channels, &dragon_default_mpool,
                 &dragon_ch_send_handle, &dragon_ch_recv_handle);
    if (derr != DRAGON_SUCCESS)
        return derr;

    if (0 == node_id)
        derr = run_node_0();
    else
        derr = run_node_1();

    if (derr != DRAGON_SUCCESS)
        return derr;

    derr = cleanup(NUM_CHANNELS, node_id, dragon_channels, &dragon_default_mpool,
                   &dragon_ch_send_handle, &dragon_ch_recv_handle);
    if (derr != DRAGON_SUCCESS)
        return derr;

    return TEST_STATUS;
}