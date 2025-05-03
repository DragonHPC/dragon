#include <dragon/channels.h>
#include <dragon/fli.h>
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
#include <sys/param.h>

#include "../_ctest_utils.h"

#define M_UID 0
#define POOL_M_UID 2
#define POOL "fli_test"
#define NUM_CHANNELS 10
#define MAX_STREAM_SIZE 500

#define check(err, expected_err, tests_passed, tests_attempted)                                              \
    ({                                                                                                       \
        bool ok = check_result(err, expected_err, tests_passed, tests_attempted);                            \
        if (!ok)                                                                                             \
            err_fail(err, "Did not pass test.");                                                                                                                                                                 \
    })

const static char* text = "This is some text to compare to so we know that this thing works!";

bool
check_result(dragonError_t err, dragonError_t expected_err, int* tests_passed, int* tests_attempted)
{
    (*tests_attempted)++;

    if (err != expected_err) {
        printf("Test %d failed with error code %s\n", *tests_attempted, dragon_get_rc_string(err));
        printf("%s\n", dragon_getlasterrstr());
        return false;
    }
    else
        (*tests_passed)++;

    return true;
}

dragonError_t create_pool(dragonMemoryPoolDescr_t* mpool) {
    /* Create a memory pool to allocate messages and a Channel out of */
    size_t mem_size = 1UL<<31;

    dragonError_t err = dragon_memory_pool_create(mpool, mem_size, POOL, POOL_M_UID, NULL);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Failed to create memory pool");

    return DRAGON_SUCCESS;
}

dragonError_t create_channels(dragonMemoryPoolDescr_t* mpool, dragonChannelDescr_t channel[], int arr_size) {
    int k;
    dragonError_t err;

    for (k=0;k<arr_size;k++) {
        /* Create the Channel in the memory pool */
        err = dragon_channel_create(&channel[k], k, mpool, NULL);
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

int proc_receiver_provide_buf(dragonFLISerial_t* serial, dragonMemoryPoolDescr_t* pool, dragonChannelDescr_t* strm_ch) {
    dragonError_t err;
    dragonFLIDescr_t fli;
    dragonFLIRecvHandleDescr_t recvh;
    size_t num_bytes = 0;
    uint64_t arg;
    char buff[5];
    char str[MAX_STREAM_SIZE];
    char err_str[200];
    str[0] = '\0';



    err = dragon_fli_attach(serial, pool, &fli);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Failed to attach to adapter");

    err = dragon_fli_open_recv_handle(&fli, &recvh, strm_ch, NULL, NULL);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Failed to open receive handle");

    err = DRAGON_SUCCESS;
    while (err == DRAGON_SUCCESS) {
        num_bytes = 0;
        err = dragon_fli_recv_bytes_into(&recvh, 5, &num_bytes, (uint8_t*)buff, &arg, NULL);
        if (err == DRAGON_SUCCESS) {
            if (num_bytes > 5) {
                snprintf(err_str, 199, "%lu bytes were received while only 5 were requested in proc_receiver.", num_bytes);
                err_fail(DRAGON_FAILURE, err_str);
            }
            strncat(str, buff, num_bytes);
        }
    }

    if (err != DRAGON_EOT)
        err_fail(err, "There was an error reading the string");

    if (strcmp(text, str))
        err_fail(DRAGON_INVALID_ARGUMENT, "There was an error with the received string.");

    err = dragon_fli_close_recv_handle(&recvh, NULL);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "There was an error closing the receive handle");

    return DRAGON_SUCCESS;
}

int proc_receiver(dragonFLISerial_t* serial, dragonMemoryPoolDescr_t* pool, dragonChannelDescr_t* strm_ch, size_t requested_size) {
    dragonError_t err;
    dragonFLIDescr_t fli;
    dragonFLIRecvHandleDescr_t recvh;
    size_t num_bytes = 0;
    uint64_t arg;
    uint8_t* bytes;
    char str[MAX_STREAM_SIZE];
    char err_str[200];
    str[0] = '\0';


    err = dragon_fli_attach(serial, pool, &fli);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Failed to attach to adapter");

    err = dragon_fli_open_recv_handle(&fli, &recvh, strm_ch, NULL, NULL);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Failed to open receive handle");

    err = DRAGON_SUCCESS;
    while (err == DRAGON_SUCCESS) {
        err = dragon_fli_recv_bytes(&recvh, requested_size, &num_bytes, &bytes, &arg, NULL);
        if (err == DRAGON_SUCCESS) {
            if (requested_size > 0 && num_bytes > requested_size) {
                snprintf(err_str, 199, "%lu bytes were received while only %lu were requested in proc_receiver.", num_bytes, requested_size);
                err_fail(DRAGON_FAILURE, err_str);
            }
            strncat(str, (char*)bytes, num_bytes);
            free(bytes);
        }
    }

    if (strcmp(text, str))
        err_fail(DRAGON_INVALID_ARGUMENT, "There was an error with the received string.");

    err = dragon_fli_close_recv_handle(&recvh, NULL);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "There was an error closing the receive handle");

    return DRAGON_SUCCESS;
}

int proc_receiver_inc_chunk(dragonFLISerial_t* serial, dragonMemoryPoolDescr_t* pool, dragonChannelDescr_t* strm_ch, size_t requested_size) {
    dragonError_t err;
    dragonFLIDescr_t fli;
    dragonFLIRecvHandleDescr_t recvh;
    size_t num_bytes = 0;
    uint64_t arg;
    uint8_t* bytes;
    char str[MAX_STREAM_SIZE];
    char err_str[200];
    str[0] = '\0';

    err = dragon_fli_attach(serial, pool, &fli);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Failed to attach to adapter");

    err = dragon_fli_open_recv_handle(&fli, &recvh, strm_ch, NULL, NULL);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Failed to open receive handle");

    err = DRAGON_SUCCESS;
    while (err == DRAGON_SUCCESS) {
        err = dragon_fli_recv_bytes(&recvh, requested_size, &num_bytes, &bytes, &arg, NULL);
        if (err == DRAGON_SUCCESS) {
            if (requested_size > 0 && num_bytes > requested_size) {
                snprintf(err_str, 199, "%lu bytes were received while only %lu were requested in proc_receiver_inc_chunk.", num_bytes, requested_size);
                err_fail(DRAGON_FAILURE, err_str);
            }
            strncat(str, (char*)bytes, num_bytes);
            free(bytes);
        }
        requested_size+=1;
    }

    if (err != DRAGON_EOT)
        err_fail(err, "There was an error reading the string");

    if (strcmp(text, str))
        err_fail(DRAGON_INVALID_ARGUMENT, "There was an error with the received string.");

    err = dragon_fli_close_recv_handle(&recvh, NULL);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "There was an error closing the receive handle");

    return DRAGON_SUCCESS;
}

int proc_receiver_fd(dragonFLISerial_t* serial, dragonMemoryPoolDescr_t* pool) {
    dragonError_t err;
    dragonFLIDescr_t fli;
    dragonFLIRecvHandleDescr_t recvh;
    size_t num_bytes = 0;
    size_t read_bytes = 1;
    char str[MAX_STREAM_SIZE];
    int fd;

    err = dragon_fli_attach(serial, pool, &fli);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Failed to attach to adapter");

    err = dragon_fli_open_recv_handle(&fli, &recvh, NULL, NULL, NULL);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Failed to open receive handle");

    err = dragon_fli_create_readable_fd(&recvh, &fd, NULL);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Failed to open receive fd handle");

    err = DRAGON_SUCCESS;

    while (read_bytes > 0) {
        read_bytes = read(fd, &str[num_bytes], MAX_STREAM_SIZE - num_bytes);
        num_bytes += read_bytes;
    }

    if (strcmp(text, str))
        err_fail(DRAGON_INVALID_ARGUMENT, "There was an error with the received string from the file descriptor.");

    close(fd);

    err = dragon_fli_finalize_readable_fd(&recvh);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "There was an error finalizing the readable fd");

    err = dragon_fli_close_recv_handle(&recvh, NULL);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "There was an error closing the receive handle");

    return DRAGON_SUCCESS;
}

int main() {
    dragonError_t err;
    int tests_passed = 0;
    int tests_attempted = 0;

    dragonMemoryPoolDescr_t pool;
    dragonChannelDescr_t channels[NUM_CHANNELS];
    dragonChannelDescr_t* channel_ptrs[NUM_CHANNELS];
    dragonFLISerial_t ser;
    dragonFLIDescr_t fli;
    dragonFLISendHandleDescr_t sendh;
    dragonFLIRecvHandleDescr_t recvh;
    size_t num_bytes;
    uint8_t* bytes = NULL;
    uint64_t arg;
    int status;
    size_t chunk_size = 1;
    int fd;

    /* Creating the channel set and the pool */
    for (int k=0;k<NUM_CHANNELS;k++)
        channel_ptrs[k] = &channels[k];

    err = create_pool(&pool);
    check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = create_channels(&pool, channels, NUM_CHANNELS);
    check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    /* Open and Close Send/Recv handles with no data sent. General
     * lifecycle testing. */
    err = dragon_fli_create(&fli, channel_ptrs[0], channel_ptrs[1], &pool, NUM_CHANNELS-2, &channel_ptrs[2], false, NULL);
    check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = dragon_fli_open_send_handle(&fli, &sendh, NULL, NULL, false, NULL);
    check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = dragon_fli_close_send_handle(&sendh, NULL);
    check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = dragon_fli_open_recv_handle(&fli, &recvh, NULL, NULL, NULL);
    check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = DRAGON_SUCCESS;
    while (err == DRAGON_SUCCESS)
        err = dragon_fli_recv_bytes(&recvh, 0, &num_bytes, &bytes, &arg, NULL);

    check(err, DRAGON_EOT, &tests_passed, &tests_attempted);

    err = dragon_fli_close_recv_handle(&recvh, NULL);
    check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = dragon_fli_destroy(&fli);
    check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    for (int k=0;k<NUM_CHANNELS;k++) {
        err = dragon_channel_destroy(channel_ptrs[k]);
        check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);
    }

    err = create_channels(&pool, channels, NUM_CHANNELS);
    check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    /* Serialize and Attach Code Test */
    err = dragon_fli_create(&fli, channel_ptrs[0], channel_ptrs[1], &pool, NUM_CHANNELS-2, &channel_ptrs[2], false, NULL);
    check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = dragon_fli_serialize(&fli, &ser);
    check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    if (fork()==0)
        return proc_receiver(&ser, &pool, NULL, 0);

    err = dragon_fli_open_send_handle(&fli, &sendh, NULL, NULL, false, NULL);
    check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    int index = 0;
    size_t len = strlen(text) + 1;

    while (index < len) {
        size_t num_bytes = MIN(chunk_size, len-index);

        err = dragon_fli_send_bytes(&sendh, num_bytes, (uint8_t*)&text[index], 0, false, NULL);
        check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);
        index+=num_bytes;
        chunk_size+=1;
    }

    err = dragon_fli_close_send_handle(&sendh, NULL);
    check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    wait(&status);
    check(status, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = dragon_fli_destroy(&fli);
    check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    /* Try Buffered Mode with invalid arguments. */
    err = dragon_fli_create(&fli, channel_ptrs[0], channel_ptrs[1], &pool, NUM_CHANNELS-2, &channel_ptrs[2], true, NULL);
    check(err, DRAGON_INVALID_ARGUMENT, &tests_passed, &tests_attempted);

    /* Now using Buffered mode */
    err = dragon_fli_create(&fli, channel_ptrs[0], NULL, &pool, 0, NULL, true, NULL);
    check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = dragon_fli_serialize(&fli, &ser);
    check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    if (fork()==0)
        return proc_receiver(&ser, &pool, NULL, 0);

    err = dragon_fli_open_send_handle(&fli, &sendh, NULL, NULL, false, NULL);
    check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    index = 0;
    len = strlen(text) + 1;

    while (index < len) {
        size_t num_bytes = MIN(chunk_size, len-index);
        err = dragon_fli_send_bytes(&sendh, num_bytes, (uint8_t*)&text[index], 0, true, NULL);
        check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);
        index+=num_bytes;
        chunk_size+=1;
    }

    err = dragon_fli_close_send_handle(&sendh, NULL);
    check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    wait(&status);
    check(status, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = dragon_fli_destroy(&fli);
    check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    /* Now using unbuffered mode for 1:1 connection */
    err = dragon_fli_create(&fli, channel_ptrs[0], NULL, &pool, 0, NULL, false, NULL);
    check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = dragon_fli_serialize(&fli, &ser);
    check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    if (fork()==0)
        return proc_receiver(&ser, &pool, STREAM_CHANNEL_IS_MAIN_FOR_1_1_CONNECTION, 0);

    err = dragon_fli_open_send_handle(&fli, &sendh, STREAM_CHANNEL_IS_MAIN_FOR_1_1_CONNECTION, NULL, false, NULL);
    check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = dragon_fli_send_bytes(&sendh, strlen(text)+1, (uint8_t*)text, 0, false, NULL);
    check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = dragon_fli_close_send_handle(&sendh, NULL);
    check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    wait(&status);
    check(status, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = dragon_fli_destroy(&fli);
    check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    /* Now using Sender Supplied Stream Channel mode */
    err = dragon_fli_create(&fli, channel_ptrs[0], NULL, &pool, 0, NULL, false, NULL);
    check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = dragon_fli_serialize(&fli, &ser);
    check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    if (fork()==0)
        return proc_receiver(&ser, &pool, NULL, 0);

    /* This should be an error. You must provide a sender stream channel in this case. */
    err = dragon_fli_open_send_handle(&fli, &sendh, NULL, NULL, false, NULL);
    check(err, DRAGON_INVALID_ARGUMENT, &tests_passed, &tests_attempted);

    err = dragon_fli_open_send_handle(&fli, &sendh, channel_ptrs[1], NULL, false, NULL);
    check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    index = 0;
    len = strlen(text) + 1;

    while (index < len) {
        size_t num_bytes = MIN(chunk_size, len-index);

        err = dragon_fli_send_bytes(&sendh, num_bytes, (uint8_t*)&text[index], 0, false, NULL);
        check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);
        index+=num_bytes;
        chunk_size+=1;
    }

    err = dragon_fli_close_send_handle(&sendh, NULL);
    check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    wait(&status);
    check(status, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = dragon_fli_destroy(&fli);
    check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    /* Now using Receiver Supplied Stream Channel mode */
    err = dragon_fli_create(&fli, NULL, channel_ptrs[0], &pool, 0, NULL, false, NULL);
    check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = dragon_fli_serialize(&fli, &ser);
    check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    if (fork()==0)
        return proc_receiver(&ser, &pool, channel_ptrs[1], 0);

    /* This should be an error. You must provide a receive stream channel in this case. */
    err = dragon_fli_open_recv_handle(&fli, &recvh, NULL, NULL, NULL);
    check(err, DRAGON_INVALID_ARGUMENT, &tests_passed, &tests_attempted);

    err = dragon_fli_open_send_handle(&fli, &sendh, NULL, NULL, false, NULL);
    check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    index = 0;
    len = strlen(text) + 1;

    while (index < len) {
        size_t num_bytes = MIN(chunk_size, len-index);

        err = dragon_fli_send_bytes(&sendh, num_bytes, (uint8_t*)&text[index], 0, false, NULL);
        check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);
        index+=num_bytes;
        chunk_size+=1;
    }

    err = dragon_fli_close_send_handle(&sendh, NULL);
    check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    wait(&status);
    check(status, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = dragon_fli_destroy(&fli);
    check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    /* Now using Sender Supplied Stream Channel mode coupled with getting one
       buffered byte at a time. */
    err = dragon_fli_create(&fli, channel_ptrs[0], NULL, &pool, 0, NULL, false, NULL);
    check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = dragon_fli_serialize(&fli, &ser);
    check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    if (fork()==0)
        return proc_receiver(&ser, &pool, NULL, 1);

    err = dragon_fli_open_send_handle(&fli, &sendh, channel_ptrs[1], NULL, false, NULL);
    check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    index = 0;
    len = strlen(text) + 1;

    while (index < len) {
        size_t num_bytes = MIN(chunk_size, len-index);

        err = dragon_fli_send_bytes(&sendh, num_bytes, (uint8_t*)&text[index], 0, false, NULL);
        check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);
        index+=num_bytes;
        chunk_size+=1;
    }

    err = dragon_fli_close_send_handle(&sendh, NULL);
    check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    wait(&status);
    check(status, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = dragon_fli_destroy(&fli);
    check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);


    /* Now using Receiver Supplied Stream Channel mode coupled with getting one
       buffered byte at a time. */
    err = dragon_fli_create(&fli, NULL, channel_ptrs[0], &pool, 0, NULL, false, NULL);
    check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = dragon_fli_serialize(&fli, &ser);
    check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    if (fork()==0)
        return proc_receiver(&ser, &pool, channel_ptrs[1], 1);

    err = dragon_fli_open_send_handle(&fli, &sendh, NULL, NULL, false, NULL);
    check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    index = 0;
    len = strlen(text) + 1;
    chunk_size=1;

    while (index < len) {
        size_t num_bytes = MIN(chunk_size, len-index);

        err = dragon_fli_send_bytes(&sendh, num_bytes, (uint8_t*)&text[index], 0, false, NULL);
        check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);
        index+=num_bytes;
        chunk_size+=1;
    }

    err = dragon_fli_close_send_handle(&sendh, NULL);
    check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    wait(&status);
    check(status, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = dragon_fli_destroy(&fli);
    check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    /* Now using Receiver Supplied Stream Channel mode coupled with getting one
       buffered byte followed by 2 buffered bytes and so on. */
    err = dragon_fli_create(&fli, NULL, channel_ptrs[0], &pool, 0, NULL, false, NULL);
    check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = dragon_fli_serialize(&fli, &ser);
    check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    if (fork()==0)
        return proc_receiver_inc_chunk(&ser, &pool, channel_ptrs[1], 1);

    err = dragon_fli_open_send_handle(&fli, &sendh, NULL, NULL, false, NULL);
    check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    index = 0;
    len = strlen(text) + 1;
    chunk_size=3;

    while (index < len) {
        size_t num_bytes = MIN(chunk_size, len-index);

        /* test the buffering capability here as well that the data should be buffered. */
        err = dragon_fli_send_bytes(&sendh, num_bytes, (uint8_t*)&text[index], 0, true, NULL);
        check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);
        index+=num_bytes;
        chunk_size+=1;
    }

    err = dragon_fli_close_send_handle(&sendh, NULL);
    check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    wait(&status);
    check(status, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = dragon_fli_destroy(&fli);
    check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    /* Now using Receiver Supplied Stream Channel mode coupled reading into a buffer. */
    err = dragon_fli_create(&fli, NULL, channel_ptrs[0], &pool, 0, NULL, false, NULL);
    check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = dragon_fli_serialize(&fli, &ser);
    check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    if (fork()==0)
        return proc_receiver_provide_buf(&ser, &pool, channel_ptrs[1]);

    err = dragon_fli_open_send_handle(&fli, &sendh, NULL, NULL, false, NULL);
    check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    index = 0;
    len = strlen(text) + 1;
    chunk_size=3;

    while (index < len) {
        size_t num_bytes = MIN(chunk_size, len-index);

        err = dragon_fli_send_bytes(&sendh, num_bytes, (uint8_t*)&text[index], 0, NULL, NULL);
        check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);
        index+=num_bytes;
        chunk_size+=1;
    }

    err = dragon_fli_close_send_handle(&sendh, NULL);
    check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    wait(&status);
    check(status, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = dragon_fli_destroy(&fli);
    check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    /* Now test the file descriptor interface. */
    err = dragon_fli_create(&fli, channel_ptrs[0], channel_ptrs[1], &pool, NUM_CHANNELS-2, &channel_ptrs[2], false, NULL);
    check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = dragon_fli_serialize(&fli, &ser);
    check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    if (fork()==0)
        return proc_receiver_fd(&ser, &pool);

    err = dragon_fli_open_send_handle(&fli, &sendh, NULL, NULL, false, NULL);
    check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = dragon_fli_create_writable_fd(&sendh, &fd, true, 0, 0, NULL);
    check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    len = strlen(text) + 1;

    ssize_t written = write(fd, text, len);
    close(fd);

    err = dragon_fli_finalize_writable_fd(&sendh);
    check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = dragon_fli_close_send_handle(&sendh, NULL);
    check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    if (index != len)
        printf("There was an error. Written=%lu and len=%lu\n", written, len);

    wait(&status);
    check(status, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    /* Now do final cleanup */
    for (int k=0;k<NUM_CHANNELS;k++) {
        err = dragon_channel_destroy(channel_ptrs[k]);
        check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);
    }

    err = dragon_memory_pool_destroy(&pool);
    check(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    printf("Passed %d of %d tests.\n", tests_passed, tests_attempted);

    TEST_STATUS = (tests_passed == tests_attempted ? SUCCESS : FAILED);

    return TEST_STATUS;
}