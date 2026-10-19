#include <dragon/managed_memory.h>
#include <dragon/return_codes.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>
#include <ctype.h>
#include <dragon/utils.h>

#include "../_ctest_utils.h"

#ifdef DRAGON_DEBUG
#define debug_prntf(...) ({\
            fprintf(stderr, "%d ERROUT:\t", __LINE__);   \
            fprintf(__VA_ARGS__)  \
        })
#endif

int test_memdescr(dragonMemoryDescr_t * mem_descr)
{
    printf("Serializing memory descriptor\n");
    dragonMemorySerial_t mem_ser;
    mem_ser.len = 0;
    dragonError_t derr = dragon_memory_serialize(&mem_ser, mem_descr);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Could not serialize memory descriptor");
    }
    printf("Serialized memory descriptor\n");

    printf("Attaching new memory descriptor\n");
    dragonMemoryDescr_t mem_attach;
    derr = dragon_memory_attach(&mem_attach, &mem_ser);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Could not attach to serialized memory");
    }
    printf("Attached new memory descriptor\n");

    void * ptr;
    derr = dragon_memory_get_pointer(mem_descr, &ptr);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Could not retrieve pointer from original descriptor");
    }

    strcpy((char*)ptr, "Hello");

    void * ptr2;
    derr = dragon_memory_get_pointer(&mem_attach, &ptr2);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Could not retrieve pointer from attached descriptor");
    }

    if (strcmp((char*)ptr, (char*)ptr2) != 0) {
        err_fail(DRAGON_MEMORY_ERRNO, "Retrieved memory allocations do not match");
    }

    ((char*)ptr)[0] = 'Y';
    ((char*)ptr2)[1] = 'o';
    if (strcmp((char*)ptr, (char*)ptr2) != 0) {
        err_fail(DRAGON_MEMORY_ERRNO, "Modified memory allocations do not match");
    }

    derr = dragon_memory_serial_free(&mem_ser);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to free serialized descriptor");
    }

    derr = dragon_memory_detach(&mem_attach);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to detach from memory");
    }

    return SUCCESS;
}

int test_type_allocations(dragonMemoryPoolDescr_t * mpool, dragonMemoryAllocationType_t type)
{
    dragonMemoryPoolAllocations_t allocs;
    dragonMemoryDescr_t mem_descr;
    dragonError_t derr = dragon_memory_pool_get_type_allocations(mpool, type, &allocs);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Could not find type allocation, but should");
    }
    printf("Retrieved %ld allocations from pool:\n", allocs.nallocs);
    for (dragonULInt i = 0; i < allocs.nallocs; i++) {
        printf("%ld: %ld %ld\n", i, allocs.types[i], allocs.ids[i]);

        derr = dragon_memory_get_alloc_memdescr(&mem_descr, mpool, allocs.ids[i], 0, NULL);
        if (derr != DRAGON_SUCCESS)
            err_fail(derr, "Could not find allocation, but should");
    }

    derr = dragon_memory_pool_allocations_destroy(&allocs);
    if (derr != DRAGON_SUCCESS)
        err_fail(derr, "Could not destroy type allocations struct, but should");

    printf("Destroyed second allocations struct from get_type_allocations\n");

    return SUCCESS;
}

int test_allocations(dragonMemoryPoolDescr_t * mpool)
{
    dragonMemoryPoolAllocations_t allocs;
    dragonMemoryDescr_t mem_descr;
    dragonError_t derr = dragon_memory_pool_get_allocations(mpool, &allocs);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Could not retrieve allocations from pool");
    }
    printf("Retrieved %ld allocations from pool:\n", allocs.nallocs);

    for (dragonULInt i = 0; i < allocs.nallocs; i++) {
        printf("%ld: %ld %ld\n", i, allocs.types[i], allocs.ids[i]);

        derr = dragon_memory_get_alloc_memdescr(&mem_descr, mpool, allocs.ids[i], 0, NULL);
        if (derr != DRAGON_SUCCESS)
            err_fail(derr, "Could not find allocation, but should");
    }

    derr = dragon_memory_pool_allocations_destroy(&allocs);
    if (derr != DRAGON_SUCCESS)
        err_fail(derr, "Could not destroy allocations struct, but should");

    printf("Destroyed allocations struct from get_allocations\n\n");

    return SUCCESS;
}

int test_attach(dragonMemoryPoolDescr_t * mpool)
{
    dragonMemoryPoolSerial_t pool_ser;
    dragonError_t derr = dragon_memory_pool_serialize(&pool_ser, mpool);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to serialized the memory pool.");
    }
    printf("Serialized memory pool\n");

    dragonMemoryPoolDescr_t mpool2;
    derr = dragon_memory_pool_attach(&mpool2, &pool_ser);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to attach to the memory pool");
    }
    printf("Attached to memory pool with pool key %li\n", mpool2._idx);

    dragonMemoryDescr_t mem;
    derr = dragon_memory_alloc(&mem, &mpool2, 512);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to allocate from attached memory pool");
    }
    printf("Got memory key %lu on attached pool\n",mem._idx);

    derr = dragon_memory_free(&mem);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to free from attached memory pool");
    }
    printf("Freed memory on attached pool\n");

    derr = dragon_memory_pool_detach(&mpool2);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to detach from pool");
    }
    printf("Detached from memory pool\n");

    return SUCCESS;
}

int test_clone(dragonMemoryPoolDescr_t* mpool)
{
    dragonError_t derr;
    dragonMemoryDescr_t mem, mem2, mem3;
    dragonMemorySerial_t mem_ser;
    void* ptr;
    void* ptr2;
    void* ptr3;
    void* ptr4;
    void* ptr5;

    derr = dragon_memory_alloc(&mem, mpool, 512);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to allocate from attached memory pool");
    }

    derr = dragon_memory_get_pointer(&mem, &ptr);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to get pointer from mem");
    }

    printf("Got address %lu from mem\n", (uint64_t)ptr);
    printf("mem._idx is %lu\n", mem._idx);

    derr = dragon_memory_descr_clone(&mem2, &mem, 48, NULL);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to clone memory descriptor");
    }

    derr = dragon_memory_get_pointer(&mem2, &ptr2);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to get pointer from mem2");
    }

    printf("Got address %lu from mem2\n", (uint64_t)ptr2);
    printf("mem2._idx is %lu\n", mem2._idx);

    if (ptr + 48 != ptr2) {
        printf("Error: ptr2 was not the correct offset off of ptr\n");
    } else {
        printf("Pointer offset test passed\n");
    }

    derr = dragon_memory_serialize(&mem_ser, &mem2);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Could not serialize memory descriptor");
    }
    printf("Serialized memory descriptor\n");

    printf("Attaching new memory descriptor\n");
    dragonMemoryDescr_t mem_attach;
    derr = dragon_memory_attach(&mem_attach, &mem_ser);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Could not attach to serialized memory");
    }
    printf("Attached new memory descriptor\n");
    printf("mem_attach._idx is %lu\n", mem_attach._idx);

    derr = dragon_memory_get_pointer(&mem_attach, &ptr3);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to get pointer from mem_attach");
    }

    printf("Got address %lu from mem_attach\n", (uint64_t)ptr3);

    if (ptr3 != ptr2) {
        printf("Error: ptr2=%lu and ptr3=%lu, but should be equal\n",(uint64_t)ptr2, (uint64_t)ptr3);
    } else {
        printf("Cloned, serialized, attached pointer has correct offset\n");
    }


    derr = dragon_memory_get_pointer(&mem, &ptr4);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to get pointer from mem");
    }

    printf("Got address %lu from mem after clone\n", (uint64_t)ptr4);
    printf("mem._idx is %lu\n", mem._idx);

    if (ptr != ptr4) {
        printf("Error: ptr=%lu and ptr4=%lu, but should be equal\n",(uint64_t)ptr, (uint64_t)ptr4);
    } else {
        printf("After cloning, original memory pointer is still correct\n");
    }

    derr = dragon_memory_descr_clone(&mem3, &mem2, 52, NULL);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to clone memory descriptor");
    }

    derr = dragon_memory_get_pointer(&mem3, &ptr5);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to get pointer from mem3");
    }

    printf("Got address %lu from mem3\n", (uint64_t)ptr5);

    if (ptr + 100 != ptr5) {
        printf("Error: ptr=%lu and ptr4=%lu, but should be 100 apart\n",(uint64_t)ptr, (uint64_t)ptr4);
    } else {
        printf("After cloning, ptr5 has correct value\n");
    }

    derr = dragon_memory_free(&mem);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to free from memory pool");
    }
    printf("Freed memory on attached pool\n");

    return SUCCESS;
}

int proc_waiter(dragonMemoryPoolDescr_t* pool, size_t sz_alloc, int pid) {

    dragonMemoryDescr_t mem;
    dragonError_t err;

    err = dragon_memory_alloc_blocking(&mem, pool, sz_alloc, NULL);
    if (err != DRAGON_SUCCESS) {
        printf("Error %s on blocking memory alloc\n", dragon_get_rc_string(err));
        printf("%s\n", dragon_getlasterrstr());
    } else
        printf("Process %d got allocation\n", pid);

    err = dragon_memory_free(&mem);
    printf("Process %d freed it\n", pid);
    if (err != DRAGON_SUCCESS)
        printf("Error %u on free\n", err);

    return SUCCESS;
}

int proc_timeout_waiter(dragonMemoryPoolDescr_t* pool, size_t sz_alloc, int pid) {

    dragonMemoryDescr_t mem;
    dragonError_t err;
    timespec_t timeout = {3,0};

    err = dragon_memory_alloc_blocking(&mem, pool, sz_alloc, &timeout);
    if (err != DRAGON_SUCCESS) {
        printf("Process %d got %s on blocking memory alloc\n", pid, dragon_get_rc_string(err));
        return SUCCESS;
    } else
        printf("Process %d got allocation\n", pid);

    err = dragon_memory_free(&mem);
    printf("Process %d freed it\n", pid);
    if (err != DRAGON_SUCCESS)
        printf("Error %u on free\n", err);

    return SUCCESS;
}

void strtrim(char *str) {
    // Trim leading whitespace
    char *start = str;
    while (isspace(*start)) {
        start++;
    }
    if (start != str) {
        memmove(str, start, strlen(start) + 1);
    }

    // Trim trailing whitespace
    char *end = str + strlen(str) - 1;
    while (end >= str && isspace(*end)) {
        end--;
    }
    *(end + 1) = '\0';
}

/* This code can be used to gather information about the dragon_hash function. You
   need a file containing one key per line to be passed as a string of bytes to the
   hash function. In that case this code will show you how the dragon_hash function
   would distribute those keys across 32 buckets. */
void test_hash() {
    size_t bucket[32];
    FILE *file;
    char line[256];

    for (int k=0;k<32;k++)
        bucket[k] = 0;

    file = fopen("files.txt", "r");
    if (file == NULL) {
        perror("Error opening file");
        return;
    }

    while (fgets(line, sizeof(line), file) != NULL) {
        strtrim(line);
        dragonULInt hash_val = dragon_hash((void*)&line[0], strlen(line));
        bucket[hash_val % 32]++;
    }

    fclose(file);
    printf("BUCKET\tCONTAINS\n");

    for (int k=0;k<32;k++)
        printf("%d:\t%lu\n", k, bucket[k]);
}

int main(int argc, char *argv[])
{

    dragonUUID uuid1;
    dragonUUID uuid2;
    dragonUUID uuid3;

    dragon_generate_uuid(uuid1);
    dragon_generate_uuid(uuid2);

    if (!dragon_compare_uuid(uuid1, uuid2))
        printf("FAILED to find uuid1 and uuid2 as different values\n");

    if (dragon_compare_uuid(uuid1, uuid1))
        printf("FAILED to find uuid1 equals itself\n");

    dragon_copy_uuid(uuid3, uuid2);

    if (dragon_compare_uuid(uuid2, uuid3))
        printf("FAILED to find uuid2 equals uuid3\n");

    // Don't want this to print. If using the hex_str function
    // you should free the string after calling it.
    // printf("uuid1=%s\n", dragon_uuid_to_hex_str(uuid1));
    // printf("uuid2=%s\n", dragon_uuid_to_hex_str(uuid2));
    // printf("uuid3=%s\n", dragon_uuid_to_hex_str(uuid3));

    int k;
    long hid = gethostid();
    printf("My host ID = %lu\n", hid);
    int tests_attempted = 0;
    int tests_passed = 0;

    //not called unless interested in testing hash distribution.
    //test_hash();

    dragonError_t derr;
    dragonMemoryPoolDescr_t mpool;
    dragonM_UID_t m_uid = 1;
    char * fname = util_salt_filename("test_memory");

    derr = dragon_memory_pool_create(&mpool, 32768, fname, m_uid, NULL);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to create the memory pool");
    }
    printf("Got pool key %lu\n",mpool._idx);

    /* Assert that passing in a stack allocated mattr doesn't cause a core dump on a failed pool create */
    dragonMemoryPoolDescr_t mpool2;
    dragonM_UID_t m_uid2 = 2;
    dragonMemoryPoolAttr_t mattr;
    derr = dragon_memory_attr_init(&mattr);
    derr = dragon_memory_pool_create(&mpool2, 32768, fname, m_uid2, &mattr);
    if (derr == DRAGON_SUCCESS) {
        main_err_fail(derr, "Expected pool create with duplicate name to fail, did not", jmp_destroy_pool);
    }

    dragonMemoryDescr_t mem, mem2;

    derr = dragon_memory_alloc_blocking(&mem, &mpool, 32000, NULL);
    if (derr != DRAGON_SUCCESS) {
        main_err_fail(derr, "Error making a large allocation", jmp_destroy_pool);
    }

    for (k=0;k<10;k++) {
        if (fork()==0) {
            return proc_waiter(&mpool, 512, k);
        }
    }

    printf("Now freeing big block\n");
    derr = dragon_memory_free(&mem);

    int status;

    for (k=0;k<10;k++) {
        wait(&status);
        if (status!=0) {
            printf("There was an error on a blocking waiter.\n");
        } else {
            tests_passed += 1;
        }
        tests_attempted += 1;
    }

    printf("Attempted %d blocking waiters and %d completed\n", tests_attempted, tests_passed);
    if (tests_attempted == tests_passed)
        printf("Test Passed\n");
    else
        printf("Test Failed\n");


    for (k=0;k<10;k++) {
        if (fork()==0) {
            return proc_timeout_waiter(&mpool, 512, k);
        }
    }

    for (k=0;k<10;k++) {
        wait(&status);
        if (status!=0) {
            printf("There was an error on a blocking waiter.\n");
        } else {
            tests_passed += 1;
        }
        tests_attempted += 1;
    }

    printf("Attempted %d blocking waiters and %d completed\n", tests_attempted, tests_passed);
    if (tests_attempted == tests_passed)
        printf("Test Passed\n");
    else
        printf("Test Failed\n");

    test_clone(&mpool);

    derr = dragon_memory_alloc(&mem, &mpool, 512);
    if (derr != DRAGON_SUCCESS) {
        main_err_fail(derr, "Failed to allocate from memory pool", jmp_destroy_pool);
    }
    printf("Got memory key %lu on created pool\n",mem._idx);

    derr = dragon_memory_alloc(&mem2, &mpool, 512);
    if (derr != DRAGON_SUCCESS) {
        main_err_fail(derr, "Failed to allocate second from memory pool", jmp_destroy_pool);
    }
    printf("Got memory key %lu on created pool\n",mem2._idx);

    dragonMemoryPoolDescr_t empty_pool_descr;
    dragonMemoryDescr_t mem3;
    derr = dragon_memory_alloc(&mem3, &empty_pool_descr, 512);
    if (derr == DRAGON_SUCCESS) {
        main_err_fail(derr, "Successfully allocated with what should be an invalid pool descriptor!\n", jmp_destroy_pool);
    }
    printf("Did not allocate on invalid pool descriptor (correct behavior)\n");

    /* Test mem_descr functionality */
    printf("\nTesting memory descriptor serialize and attach\n");
    if (test_memdescr(&mem) != 0) {
        goto jmp_destroy_pool;
    }
    printf("\n");

    /* Test mem_pool attach functionality */
    printf("\nTesting pool attach\n");
    status = 0;
    pid_t pid = fork();
    if (pid == 0) {
        return test_attach(&mpool);
    } else {
        (void)wait(&status);
        if (status != 0)
            goto jmp_destroy_pool;
    }

    /* Test getting allocation list functionality */
    printf("\nTesting memory allocations\n");
    if (test_allocations(&mpool) != 0)
        goto jmp_destroy_pool;

    printf("Allocations Passed\n\n");

    /* Test getting allocation list _of a specific type_ functionality */
    printf("\nTesting type allocations\n");
    test_type_allocations(&mpool, DRAGON_MEMORY_ALLOC_DATA);
    printf("Type allocations passed\n\n");

    /* Try and get the first allocation */
    dragonMemoryDescr_t mem_get;
    derr = dragon_memory_descr_clone(&mem_get, &mem, 0, NULL);
    if (derr != DRAGON_SUCCESS) {
        main_err_fail(derr, "Failed to clone allocation structure", jmp_destroy_pool);
    }
    printf("Got cloned memory descriptor\n");

    derr = dragon_memory_detach(&mem_get);
    if (derr != DRAGON_SUCCESS) {
        main_err_fail(derr, "Failed to detach from attached allocation structure", jmp_destroy_pool);
    }

    dragonMemoryDescr_t type_mem;
    derr = dragon_memory_alloc_type(&type_mem, &mpool, 512, 9001);
    if (derr != DRAGON_SUCCESS) {
        main_err_fail(derr, "Failed to allocate specific type", jmp_destroy_pool);
    }
    printf("Allocated memory type with key %lu\n", type_mem._idx);

    /* Test that type_alloc updates allocation lists appropriately */
    if (test_type_allocations(&mpool, 0) != 0)
        goto jmp_destroy_pool;
    printf("\n");


    /* Assert our typed malloc freed correctly (third total malloc) */
    derr = dragon_memory_free(&type_mem);
    if (derr != DRAGON_SUCCESS) {
        main_err_fail(derr, "Failed to free from memory pool", jmp_destroy_pool);
    }
    printf("Freed type memory\n");


    /* Assert our second malloc freed correctly */
    derr = dragon_memory_free(&mem2);
    if (derr != DRAGON_SUCCESS) {
        main_err_fail(derr, "Failed to free from memory pool", jmp_destroy_pool);
    }
    printf("Freed one memory\n");

    /* Assert our first malloc freed correctly */
    derr = dragon_memory_free(&mem);
    if (derr != DRAGON_SUCCESS) {
        main_err_fail(derr, "Failed to free from memory pool", jmp_destroy_pool);
    }
    printf("Freed second memory\n");

jmp_destroy_pool:
    /* Use this as a cleanup GOTO so we don't need to manually delete SHM files */
    derr = dragon_memory_pool_destroy(&mpool);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to destroy the memory pool");
    }
    printf("After destroy, key = %lu\n",mpool._idx);

    return TEST_STATUS;
}
