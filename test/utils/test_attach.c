#include <dragon/managed_memory.h>
#include <dragon/channels.h>
#include <dragon/return_codes.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>
#include <time.h>

#include "../_ctest_utils.h"

#define N_ATTACH 5

dragonError_t derr;

int valid_pool(dragonMemoryPoolDescr_t * pool_descr)
{
    dragonMemoryDescr_t mem;
    derr = dragon_memory_alloc(&mem, pool_descr, 512);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Could not allocate memory from pool");
    }

    derr = dragon_memory_free(&mem);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Could not free memory from pool");
    }

    return SUCCESS;
}

int valid_channel(dragonMemoryPoolDescr_t * pool_descr, dragonChannelSendh_t * csend, dragonChannelRecvh_t * crecv)
{
    dragonMemoryDescr_t mem;
    dragonMessage_t s_msg, r_msg;

    derr = dragon_memory_alloc(&mem, pool_descr, 512);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Could not allocate memory from pool");
    }

    derr = dragon_memory_alloc(&mem, pool_descr, 512);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Could not allocate memory from pool");
    }

    derr = dragon_channel_message_init(&s_msg, &mem, NULL);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Could not initialize message");
    }

    derr = dragon_channel_message_init(&r_msg, NULL, NULL);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Could not initialize message");
    }

    derr = dragon_chsend_send_msg(csend, &s_msg, NULL, NULL);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Could not send message");
    }

    derr = dragon_chrecv_get_msg(crecv, &r_msg);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Could not receive message");
    }

    derr = dragon_channel_message_destroy(&r_msg, true);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Could not destroy message");
    }

    return SUCCESS;
}

int main(int argc, char **argv)
{
    size_t mem_size = 1UL<<30;
    dragonMemoryPoolDescr_t mpool;
    dragonMemoryPoolDescr_t mpools[N_ATTACH];
    dragonMemoryPoolSerial_t pool_ser;
    dragonM_UID_t m_uid = 1;
    char * fname = util_salt_filename("test_attach");

    srand(time(NULL));
    int rand_idx = rand() % N_ATTACH;

    printf("Pool Create\n");
    if ((derr = dragon_memory_pool_create(&mpool, mem_size, fname, m_uid, NULL)) != DRAGON_SUCCESS)
        err_fail(derr, "Failed to create the memory pool");

    printf("Pool Serialize\n");
    if ((derr = dragon_memory_pool_serialize(&pool_ser, &mpool)) != DRAGON_SUCCESS)
        main_err_fail(derr, "Failed to serialize pool", jmp_destroy_pool);

    printf("Pool Attach\n");
    for (int i = 0; i < N_ATTACH; i++) {
        if ((derr = dragon_memory_pool_attach(&mpools[i], &pool_ser)) != DRAGON_SUCCESS)
            err_fail(derr, "Failed to reattach to pool");
    }

    printf("Pool Detach\n");
    if ((derr = dragon_memory_pool_detach(&mpool) != DRAGON_SUCCESS))
        main_err_fail(derr, "Failed to detach from pool", jmp_destroy_pool);

    for (int i = 0; i < N_ATTACH; i++) {
        if (valid_pool(&mpools[i]) != SUCCESS)
            main_err_fail(derr, "Could not allocate on pool attach", jmp_destroy_pool);
    }

    for (int i = 0; i < N_ATTACH-1; i++) {
        if (i == rand_idx)
            continue;

        if ((derr = dragon_memory_pool_detach(&mpools[i]) != DRAGON_SUCCESS))
            main_err_fail(derr, "Failed to detach from pool", jmp_destroy_pool);
    }

    /* FIXME: This is temporary while PE-43816 is being worked on. */
    printf("This is a temporary fix to remove pool until PE-43816 is completed.\n");
    derr = dragon_memory_pool_destroy(&mpool);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to destroy the memory pool");
    }
    return SUCCESS;
/********
         Check our channel attach/detach before our final pool detach
********/

    printf("Validate channels\n");

    dragonChannelDescr_t ch;
    dragonChannelSerial_t ch_ser;
    int c_uid = 1;
    dragonChannelDescr_t chs[N_ATTACH];
    dragonChannelSendh_t csends[N_ATTACH];
    dragonChannelRecvh_t crecvs[N_ATTACH];

    derr = util_create_channel(&mpool, &ch, c_uid, 0, 0);
    if (derr != SUCCESS)
        main_err_fail(derr, "Failed to create channel on pool", jmp_destroy_pool);

    if ((derr = dragon_channel_serialize(&ch, &ch_ser)) != DRAGON_SUCCESS)
        main_err_fail(derr, "Failed to serialize channel descriptor", jmp_destroy_pool);

    for (int i = 0; i < N_ATTACH; i++) {
        if ((derr = dragon_channel_attach(&ch_ser, &chs[i])) != DRAGON_SUCCESS)
            main_err_fail(derr, "Failed to attach to channel", jmp_destroy_pool);

        if ((derr = util_create_open_send_recv_handles(&chs[i], &csends[i], NULL, &crecvs[i], NULL)) != SUCCESS)
            main_err_fail(derr, "Failed to open send/receive handle", jmp_destroy_pool);
    }

    if ((derr = dragon_channel_detach(&ch)) != DRAGON_SUCCESS)
        main_err_fail(derr, "Failed to detach from channel", jmp_destroy_pool);

    for (int i = 0; i < N_ATTACH; i++) {
        if (valid_channel(&mpools[rand_idx], &csends[i], &crecvs[i]) != SUCCESS)
            main_err_fail(derr, "Failed to validate channel", jmp_destroy_pool);
    }

    for (int i = 0; i < N_ATTACH; i++) {
        if (i == rand_idx)
            continue;

        if ((derr = dragon_channel_detach(&chs[i])) != DRAGON_SUCCESS)
            main_err_fail(derr, "Failed to detach channel", jmp_destroy_pool);
    }

    if (valid_channel(&mpools[rand_idx], &csends[rand_idx], &crecvs[rand_idx]) != SUCCESS)
        main_err_fail(derr, "Could not validate random channel after mass detach", jmp_destroy_pool);

    if ((derr = dragon_channel_detach(&chs[rand_idx])) != DRAGON_SUCCESS)
        main_err_fail(derr, "Failed to detach channel", jmp_destroy_pool);

    printf("Channels validated\n");

/********
         Check final pool detach
********/

    printf("Validate after mass detach\n");
    if (valid_pool(&mpools[rand_idx]) != 0)
        main_err_fail(derr, "Could not allocate on pool attach", jmp_destroy_pool);

    printf("Detach\n");
    if (dragon_memory_pool_detach(&mpools[rand_idx]) != DRAGON_SUCCESS)
        main_err_fail(derr, "Could not detach from pool", jmp_destroy_pool);

    /* Use this as a cleanup GOTO so we don't need to manually delete SHM files */
jmp_destroy_pool:
    printf("Goto Attach\n");
    derr = dragon_memory_pool_attach(&mpool, &pool_ser);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to reattach to pool");
    }

    printf("Goto Destroy\n");
    derr = dragon_memory_pool_destroy(&mpool);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to destroy the memory pool");
    }

    return TEST_STATUS;
}
