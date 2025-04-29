#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <dragon/managed_memory.h>
#include <dragon/channels.h>
#include <dragon/return_codes.h>

#include "../_ctest_utils.h"

// Special case since we're explicitly expecting errors, so any success should fail and jump to end
#undef err_fail
#define err_fail(msg, jmp) ({                   \
            printf("%s\n", msg);                \
            TEST_STATUS = FAILED;               \
            goto jmp;                           \
        })

int
main(int argc, char * argv[])
{
    dragonError_t derr;

    const size_t nBytes = 128 * 1024 * 1024;
    char * base_name = util_salt_filename("WisdomTestPool");
    const dragonM_UID_t m_uid = 8062013;

    printf("This test intentionally does not use dragon correctly and tests invalid operations.\n");
    printf("The goal is to not segfault.\n");

    printf("POOL ATTR INIT\n");
    fflush(stdout);
    dragonMemoryPoolAttr_t pool_attr;
    derr = dragon_memory_attr_init(&pool_attr);

    printf("POOL CREATE\n");
    fflush(stdout);
    dragonMemoryPoolDescr_t pool_descr;
    dragonMemoryPoolDescr_t good_descr;
    // This part should actually work
    derr = dragon_memory_pool_create(&good_descr, nBytes, base_name, m_uid, &pool_attr);
    if (derr != DRAGON_SUCCESS) {
        printf("Could not create pool. Error code was %s\n", dragon_get_rc_string(derr));
        return FAILED;
    }

    derr = dragon_memory_pool_create(&pool_descr, nBytes, base_name, m_uid, &pool_attr);
    if (derr == DRAGON_SUCCESS) {
        err_fail("Expected failure on duplicate pool creation", jmp_pool_destroy);
    }
    printf("Could not create pool second time. Error code was %s\n", dragon_get_rc_string(derr));

    printf("POOL SERIALIZE %lu\n",pool_descr._idx);
    fflush(stdout);
    dragonMemoryPoolSerial_t pool_ser;
    derr = dragon_memory_pool_serialize(&pool_ser, &pool_descr);
    if (derr == DRAGON_SUCCESS) {
        err_fail("Expected failure to serialize bad descriptor", jmp_pool_destroy);
    }
    printf("Could not serialize pool. Error code was %s\n", dragon_get_rc_string(derr));

    printf("POOL ATTACH\n");
    fflush(stdout);
    dragonMemoryPoolDescr_t pool_descr2;
    derr = dragon_memory_pool_attach(&pool_descr2, &pool_ser);
    if (derr == DRAGON_SUCCESS) {
        err_fail("Expected failure on attaching to bad serializer", jmp_pool_destroy);
    }
    printf("Could not attach to pool. Error code was %s\n", dragon_get_rc_string(derr));

    printf("MEM ALLOC\n");
    fflush(stdout);
    size_t nBytes_alloc = 1024 * 1024;
    dragonMemoryDescr_t mem_descr;
    derr = dragon_memory_alloc(&mem_descr, &pool_descr2, nBytes_alloc);
    if (derr == DRAGON_SUCCESS) {
        err_fail("Expected failure allocating on bad pool descriptor", jmp_pool_destroy);
    }
    printf("Could not allocate from pool. Error code was %s\n", dragon_get_rc_string(derr));

    printf("MEM POINTER\n");
    fflush(stdout);
    void * base_ptr = NULL;
    derr = dragon_memory_get_pointer(&mem_descr, &base_ptr);
    if (derr == DRAGON_SUCCESS) {
        err_fail("Expected failure getting memory pointer on invalid allocation", jmp_pool_destroy);
    }
    printf("Could not get pointer from allocation. Error code was %s\n", dragon_get_rc_string(derr));

    printf("MEM SERIALIZE\n");
    fflush(stdout);
    dragonMemorySerial_t sdescr;
    derr = dragon_memory_serialize(&sdescr, &mem_descr);
    if (derr == DRAGON_SUCCESS) {
        err_fail("Expected failure serializing invalid memory descriptor", jmp_pool_destroy);
    }
    printf("Could not serialize allocation. Error code was %s\n", dragon_get_rc_string(derr));

    printf("MEM ATTACH 2\n");
    fflush(stdout);
    dragonMemoryDescr_t mem_descr2;
    derr = dragon_memory_attach(&mem_descr2, &sdescr);
    if (derr == DRAGON_SUCCESS) {
        err_fail("Expected failure on attaching to invalid memory serialization", jmp_pool_destroy);
    }
    printf("Could not attach serialized allocation. Error code was %s\n", dragon_get_rc_string(derr));

    dragonChannelDescr_t ch_descr;
    dragonChannelDescr_t ch_descr2;

    printf("CHANNEL CREATE\n");
    derr = dragon_channel_create(&ch_descr, 4000, &pool_descr2, NULL);
    if (derr == DRAGON_SUCCESS) {
        err_fail("Expected failure creating channel on invalid pool", jmp_pool_destroy);
    }
    printf("Could not create channel. Error code was %s\n", dragon_get_rc_string(derr));

    dragonChannelSerial_t ch_ser;
    printf("CHANNEL SERIALIZE\n");
    derr = dragon_channel_serialize(&ch_descr, &ch_ser);
    if (derr == DRAGON_SUCCESS) {
        err_fail("Expected failure serializing invalid channel", jmp_pool_destroy);
    }
    printf("Could not serialize channel. Error code was %s\n", dragon_get_rc_string(derr));

    printf("CHANNEL ATTACH\n");
    derr = dragon_channel_attach(&ch_ser, &ch_descr2);
    if (derr == DRAGON_SUCCESS) {
        err_fail("Expected failure attaching to invalid channel serialization", jmp_pool_destroy);
    }
    printf("Could not attach to channel. Error code was %s\n", dragon_get_rc_string(derr));

jmp_pool_destroy:
    derr = dragon_memory_pool_destroy(&good_descr);
    if (derr != DRAGON_SUCCESS) {
        printf("Could not destroy original pool. Error code was %s\n", dragon_get_rc_string(derr));
        printf("FAILED - The test was NOT SUCCESSFUL!!!!!\n");
        return FAILED;
    }

    printf("SUCCESS - If you see this, the test was SUCCESSFUL!!!!!\n");
    fflush(stdout);

    return TEST_STATUS;
}
