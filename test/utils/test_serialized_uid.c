#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <dragon/managed_memory.h>
#include <dragon/channels.h>

#include "../_ctest_utils.h"

int
main(int argc, char ** argv)
{
    size_t mem_size = 1UL<<30;
    dragonMemoryPoolDescr_t mpool;
    dragonM_UID_t m_uid = 90210;
    char * m_fname = "uid_test";

    dragonError_t derr = dragon_memory_pool_create(&mpool, mem_size, m_fname, m_uid, NULL);
    if (derr != DRAGON_SUCCESS)
        err_fail(derr, "Failed to create the memory pool");

    dragonMemoryPoolSerial_t pool_ser;
    derr = dragon_memory_pool_serialize(&pool_ser, &mpool);
    if (derr != DRAGON_SUCCESS)
        main_err_fail(derr, "Failed to serialized the memory pool.", jmp_destroy_pool);

    dragonULInt uid;
    char * fname;
    derr = dragon_memory_pool_get_uid_fname(&pool_ser, &uid, &fname);
    if (derr != DRAGON_SUCCESS)
        main_err_fail(derr, "Failed to retrieve uid or fname", jmp_destroy_pool);

    if (uid != m_uid) {
        printf("Retrieved ID does not match!\nGot %ld but expected %ld\n", uid, m_uid);
        TEST_STATUS = FAILED;
        goto jmp_destroy_pool;
    }


    size_t len = strlen(fname);
    char * buf = malloc(len+1);
    snprintf(buf, len, "/_dragon_%s_manifest", m_fname);

    if (!strcmp(buf, fname)) {
        printf("Filenames do not match!\nGot: %s\nExpected: %s\n", fname, buf);
        TEST_STATUS = FAILED;
        goto jmp_destroy_pool;
    }

    free(fname);
    free(buf);


    dragonChannelDescr_t ch;
    dragonChannelSerial_t ch_ser;
    dragonULInt c_uid = 83706;
    derr = dragon_channel_create(&ch, c_uid, &mpool, NULL);
    if (derr != DRAGON_SUCCESS)
        main_err_fail(derr, "Failed to create channel", jmp_destroy_pool);

    derr = dragon_channel_serialize(&ch, &ch_ser);
    if (derr != DRAGON_SUCCESS)
        main_err_fail(derr, "Failed to serialize channel", jmp_destroy_pool);

    dragonULInt c_uid_out;
    derr = dragon_channel_get_uid(&ch_ser, &c_uid_out);
    if (derr != DRAGON_SUCCESS)
        main_err_fail(derr, "Something broke", jmp_destroy_pool);

    if (c_uid_out != c_uid) {
        printf("Channel ID mismatch, expected %ld but got %ld\n", c_uid, c_uid_out);
        TEST_STATUS = FAILED;
        goto jmp_destroy_pool;
    }

    derr = dragon_channel_pool_get_uid_fname(&ch_ser, &uid, &fname);
    if (derr != DRAGON_SUCCESS)
        main_err_fail(derr, "Failed to retrieve pool data from channel", jmp_destroy_pool);

    if (uid != m_uid) {
        printf("Retrieved ID for channel pool does not match!\nGot %ld but expected %ld\n", uid, m_uid);
        TEST_STATUS = FAILED;
        goto jmp_destroy_pool;
    }

    len = strlen(fname);
    buf = malloc(len+1);
    snprintf(buf, len, "/_dragon_%s_manifest", m_fname);

    if (!strcmp(buf, fname)) {
        printf("Filenames do not match!\nGot: %s\nExpected: %s\n", fname, buf);
        TEST_STATUS = FAILED;
        goto jmp_destroy_pool;
    }

    free(fname);
    free(buf);

jmp_destroy_pool:
    derr = dragon_memory_pool_destroy(&mpool);
    if (derr != DRAGON_SUCCESS)
        err_fail(derr, "Failed to destroy the memory pool");

    if (TEST_STATUS == SUCCESS)
        printf("Test passed!\n");
    else
        printf("!!! Test failed !!!!\n");

    return TEST_STATUS;
}
