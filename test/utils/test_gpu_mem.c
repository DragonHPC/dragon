#include <dragon/managed_memory.h>
#include <dragon/return_codes.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>
#include <ctype.h>
#include <dragon/utils.h>
#include <semaphore.h>
#include <sys/shm.h>
#include <sys/mman.h>
#include <errno.h>

#include "../_ctest_utils.h"

#ifdef DRAGON_DEBUG
#define debug_prntf(...) ({\
            fprintf(stderr, "%d ERROUT:\t", __LINE__);   \
            fprintf(__VA_ARGS__)  \
        })
#endif

int test_memdescr(dragonMemoryDescr_t * mem_descr)
{
    dragonMemorySerial_t mem_ser;
    mem_ser.len = 0;
    dragonError_t derr = dragon_memory_serialize(&mem_ser, mem_descr);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Could not serialize memory descriptor");
    }

    dragonMemoryDescr_t mem_attach;
    derr = dragon_memory_attach(&mem_attach, &mem_ser);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Could not attach to serialized memory");
    }

    void * ptr;
    derr = dragon_memory_get_pointer(mem_descr, &ptr);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Could not retrieve pointer from original descriptor");
    }

    // added to get around strcpy for GPU mem
    dragonMemoryPoolDescr_t mpool_host;
    dragonM_UID_t m_uid = 13;
    dragonMemoryDescr_t mem_descr_host, mem_descr_copy_off;
    char * fname = util_salt_filename("test_copy_pool");
    derr = dragon_memory_pool_create(&mpool_host, 32768, fname, m_uid, NULL);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to create the memory pool");
    }

    derr = dragon_memory_alloc_blocking(&mem_descr_host, &mpool_host, 512, NULL);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Error making a large allocation");
    }


    void * ptr_host;
    derr = dragon_memory_get_pointer(&mem_descr_host, &ptr_host);
    strcpy((char*)ptr_host, "Hello");
    derr = dragon_memory_copy(&mem_descr_host, mem_descr);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Could not copy H2D");
    }

    void * ptr2;
    //derr = dragon_memory_get_pointer(&mem_descr_host, &ptr2);
    derr = dragon_memory_get_pointer(&mem_attach, &ptr2);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Could not retrieve pointer from attached descriptor");
    }

    derr = dragon_memory_alloc_blocking(&mem_descr_copy_off, &mpool_host, 512, NULL);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Error making a large allocation");
    }
    derr = dragon_memory_copy(&mem_attach, &mem_descr_copy_off);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Could not copy D2H");
    }


    void * ptr3;
    derr = dragon_memory_get_pointer(&mem_descr_copy_off, &ptr3);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Could not retrieve pointer from attached descriptor");
    }
    printf("Host pointer says: %s\n", (char*)ptr_host);
    printf("Attached memory says: %s\n", (char*)ptr3);
    fflush(stdout);
    sleep(1);
    if (strcmp((char*)ptr_host, (char*)ptr3) != 0) {
        err_fail(DRAGON_MEMORY_ERRNO, "Retrieved memory allocations do not match");
    }

    // added to get around strcpy for GPU mem
    ((char*)ptr_host)[0] = 'Y';
    derr = dragon_memory_copy(&mem_descr_host, mem_descr);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Could not copy H2D");
    }
    derr = dragon_memory_copy(&mem_attach, &mem_descr_copy_off);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Could not copy D2H");
    }
    // now changes are refleced in both so can edit and do the reverse copy
    ((char*)ptr3)[1] = 'o';
    derr = dragon_memory_copy(&mem_descr_copy_off, &mem_attach);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Could not copy H2D");
    }
    derr = dragon_memory_copy(mem_descr, &mem_descr_host);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Could not copy D2H");
    }
    // now changes should be reflected in both host pointers
    printf("Host pointer says: %s\n", (char*)ptr_host);
    printf("Attached memory says: %s\n", (char*)ptr3);

    if (strcmp((char*)ptr_host, (char*)ptr3) != 0) {
        err_fail(DRAGON_MEMORY_ERRNO, "Modified memory allocations do not match");
    }

    derr = dragon_memory_serial_free(&mem_ser);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to free serialized descriptor");
    }

    //TODO CPW: This process created this memory pool. It can't detach but rather needs to free it.
    //derr = dragon_memory_detach(&mem_attach);
    //if (derr != DRAGON_SUCCESS) {
    //    err_fail(derr, "Failed to detach from memory");
    //}

    derr = dragon_memory_pool_destroy(&mpool_host);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to destory the copy pool");
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

    return SUCCESS;
}

int test_attach(dragonMemoryPoolDescr_t * mpool)
{
    dragonMemoryPoolSerial_t pool_ser;
    dragonError_t derr = dragon_memory_pool_serialize(&pool_ser, mpool);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to serialized the memory pool.");
    }

    dragonMemoryPoolDescr_t mpool2;
    derr = dragon_memory_pool_attach(&mpool2, &pool_ser);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to attach to the memory pool");
    }

    dragonMemoryDescr_t mem;
    derr = dragon_memory_alloc(&mem, &mpool2, 512);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to allocate from attached memory pool");
    }

    derr = dragon_memory_free(&mem);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to free from attached memory pool");
    }

    derr = dragon_memory_pool_detach(&mpool2);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to detach from pool");
    }

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


    derr = dragon_memory_descr_clone(&mem2, &mem, 48, NULL);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to clone memory descriptor");
    }

    derr = dragon_memory_get_pointer(&mem2, &ptr2);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to get pointer from mem2");
    }


    if (ptr + 48 != ptr2) {
        printf("Error: ptr2 was not the correct offset off of ptr\n");
    } else {
        printf("Pointer offset test passed\n");
    }

    derr = dragon_memory_serialize(&mem_ser, &mem2);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Could not serialize memory descriptor");
    }

    dragonMemoryDescr_t mem_attach;
    derr = dragon_memory_attach(&mem_attach, &mem_ser);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Could not attach to serialized memory");
    }

    derr = dragon_memory_get_pointer(&mem_attach, &ptr3);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to get pointer from mem_attach");
    }

    if (ptr3 != ptr2) {
        printf("Error: ptr2=%lu and ptr3=%lu, but should be equal\n",(uint64_t)ptr2, (uint64_t)ptr3);
    }

    derr = dragon_memory_get_pointer(&mem, &ptr4);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to get pointer from mem");
    }

    if (ptr != ptr4) {
        printf("Error: ptr=%lu and ptr4=%lu, but should be equal\n",(uint64_t)ptr, (uint64_t)ptr4);
    }

    derr = dragon_memory_descr_clone(&mem3, &mem2, 52, NULL);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to clone memory descriptor");
    }

    derr = dragon_memory_get_pointer(&mem3, &ptr5);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to get pointer from mem3");
    }

    if (ptr + 100 != ptr5) {
        printf("Error: ptr=%lu and ptr4=%lu, but should be 100 apart\n",(uint64_t)ptr, (uint64_t)ptr4);
    }

    derr = dragon_memory_free(&mem);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to free from memory pool");
    }

    return SUCCESS;
}

int proc_waiter(dragonMemoryPoolDescr_t* pool, size_t sz_alloc, int pid) {

    dragonMemoryDescr_t mem;
    dragonError_t err;

    err = dragon_memory_alloc_blocking(&mem, pool, sz_alloc, NULL);
    if (err != DRAGON_SUCCESS) {
        printf("Error %s on blocking memory alloc\n", dragon_get_rc_string(err));
        printf("%s\n", dragon_getlasterrstr());
    }

    err = dragon_memory_free(&mem);
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
    }

    err = dragon_memory_free(&mem);
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

int pool_create_destroy_lifecycle()
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

    //not called unless interested in testing hash distribution.
    //test_hash();

    dragonError_t derr;
    dragonMemoryPoolDescr_t mpool;
    dragonM_UID_t m_uid = 1;
    char * fname = util_salt_filename("test_gpu_memory");

    derr = dragon_memory_pool_create(&mpool, 32768, fname, m_uid, NULL);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to create the memory pool");
    }

    /* Use this as a cleanup GOTO so we don't need to manually delete SHM files */
    derr = dragon_memory_pool_destroy(&mpool);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to destroy the memory pool");
    }

    return TEST_STATUS;

}

int pool_create_destroy_gpu_lifecycle(){

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

    //not called unless interested in testing hash distribution.
    //test_hash();

    dragonError_t derr;
    dragonMemoryPoolDescr_t mpool;
    dragonM_UID_t m_uid = 1;
    char * fname = util_salt_filename("test_memory");
    char * fname_gpu = util_salt_filename("test_gpu_memory");

    derr = dragon_memory_pool_create(&mpool, 32768, fname, m_uid, NULL);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to create the memory pool");
    }

    /* Assert that passing in a stack allocated mattr doesn't cause a core dump on a failed pool create */
    dragonMemoryPoolDescr_t mpool2;
    dragonM_UID_t m_uid2 = 2;
    dragonMemoryPoolAttr_t mattr;
    derr = dragon_memory_attr_init(&mattr);
    mattr.mem_type = DRAGON_MEMORY_TYPE_GPU;
    // size_t pool_size = 32768;
    size_t pool_size = 1073741824;
    derr = dragon_memory_pool_create(&mpool2, pool_size, fname_gpu, m_uid2, &mattr);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to create the GPU memory pool");
    }

    /* Use this as a cleanup GOTO so we don't need to manually delete SHM files */
    /*CPW: This does indeed destroy both memory pools from above.*/
    derr = dragon_memory_pool_destroy(&mpool);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to destroy the memory pool");
    }
    derr = dragon_memory_pool_destroy(&mpool2);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to destroy the memory pool");
    }

    return TEST_STATUS;

}

int test_alloc_block_proc_waiters()
{
    int tests_attempted = 0;
    int tests_passed = 0;
    dragonError_t derr;
    dragonMemoryPoolDescr_t mpool;
    dragonM_UID_t m_uid = 1;
    char * fname = util_salt_filename("messaging_pool");
    derr = dragon_memory_pool_create(&mpool, 32768, fname, m_uid, NULL);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to create the memory pool");
    }

    dragonMemoryDescr_t mem_descr;

    size_t sz_alloc = 32000;
    derr = dragon_memory_alloc_blocking(&mem_descr, &mpool, sz_alloc, NULL);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Error making a large allocation");
    }

    int sem_rc = 0;
    // Allocate shared memory for the semaphore
    sem_t *sem = (sem_t *)mmap(NULL, sizeof(sem_t),
                            PROT_READ | PROT_WRITE,
                            MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    if (sem == MAP_FAILED) {
        perror("mmap");
        exit(EXIT_FAILURE);
    }

    sem_rc = sem_init(sem, 1, 0); // pshared=1 for inter-process
    if (sem_rc != 0) {
        perror("sem_init");
        exit(EXIT_FAILURE);
    }

    int num_children = 10;

    pid_t pid;
    for(int k=0;k<num_children;k++)
    {
        pid = fork();
        if (pid == 0)
        {
            sem_rc = sem_wait(sem);
            if (sem_rc != 0) {
                fprintf(stderr, "Failed to wait on semaphore: %s\n", strerror(errno));
                sem_destroy(sem);
                return EXIT_FAILURE;
            }

            void * ptr;
            derr = dragon_memory_get_pointer(&mem_descr, &ptr);
            if (derr != DRAGON_SUCCESS) {
                err_fail(derr, "Could not retrieve pointer from original descriptor");
            }

            dragonMemoryPoolSerial_t mem_ser_child;
            mem_ser_child.len = 0;
            memcpy(&mem_ser_child.len, ptr, sizeof(size_t));
            ptr = ptr + sizeof(size_t);
            mem_ser_child.data = (void *) malloc(mem_ser_child.len);
            memcpy(mem_ser_child.data, ptr, mem_ser_child.len);

            dragonMemoryPoolDescr_t mem_attach;
            derr = dragon_memory_pool_attach(&mem_attach, &mem_ser_child);
            if (derr != DRAGON_SUCCESS) {
                err_fail(derr, "Could not attach to serialized memory");
            }

            return proc_waiter(&mem_attach, 512, k);
        }
    }

    char * fname_gpu = util_salt_filename("test_gpu_memory");
    dragonMemoryPoolDescr_t mpool2;
    dragonM_UID_t m_uid2 = 2;
    dragonMemoryPoolAttr_t mattr;
    derr = dragon_memory_attr_init(&mattr);
    mattr.mem_type = DRAGON_MEMORY_TYPE_GPU;
    size_t pool_size = 32768;
    //size_t pool_size = 1073741824;
    derr = dragon_memory_pool_create(&mpool2, pool_size, fname_gpu, m_uid2, &mattr);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to create the GPU memory pool");
    }

    // need to get mem_descr for above pool
    dragonMemoryPoolSerial_t mem_ser;
    mem_ser.len = 0;
    derr = dragon_memory_pool_serialize(&mem_ser, &mpool2);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Could not serialize memory descriptor");
    }
    if (mem_ser.len > sz_alloc) {
        printf("Error: serialized memory descriptor length %ld is larger than allocated size %lu\n", mem_ser.len, sz_alloc);
        return EXIT_FAILURE;
    }

    dragonMemoryPoolDescr_t mem_attach;
    derr = dragon_memory_pool_attach(&mem_attach, &mem_ser);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Could not attach to serialized memory");
    }

    void * ptr;
    derr = dragon_memory_get_pointer(&mem_descr, &ptr);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Could not retrieve pointer from original descriptor");
    }
    memcpy(ptr, &mem_ser.len, sizeof(size_t));
    ptr = ptr + sizeof(size_t);
    memcpy(ptr, mem_ser.data, mem_ser.len);

    void * ptr2;
    derr = dragon_memory_get_pointer(&mem_descr, &ptr2);
    dragonMemoryPoolSerial_t mem_ser_ptr;
    mem_ser_ptr.len = 0;
    memcpy(&mem_ser_ptr.len, ptr2, sizeof(size_t));
    ptr2 = ptr2 + sizeof(size_t);
    mem_ser_ptr.data = (void *) malloc(mem_ser_ptr.len);
    memcpy(mem_ser_ptr.data, ptr2, mem_ser_ptr.len);

    dragonMemoryPoolDescr_t mem_attach_ptr;
    derr = dragon_memory_pool_attach(&mem_attach_ptr, &mem_ser_ptr);

    dragonMemoryDescr_t mem;

    derr = dragon_memory_alloc_blocking(&mem, &mpool2, 32000, NULL);

    // let child know it can get the ipc handle
    for (int k=0; k<num_children; k++)
        sem_rc = sem_post(sem);

    derr = dragon_memory_free(&mem);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to free the memory");
    }

    int status;
    for (int k=0; k<num_children; k++) {
        wait(&status);
        if (status != 0) {
            printf("There was an error on a blocking waiter that returned status %d.\n", status);
        } else {
            tests_passed += 1;
        }
        tests_attempted += 1;
    }

    if (tests_attempted == tests_passed)
        printf("Test Passed\n");
    else
        printf("Test Failed\n");

    derr = dragon_memory_pool_destroy(&mpool);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to destroy the memory pool");
    }
    derr = dragon_memory_pool_destroy(&mpool2);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to destroy the gpu memory pool");
    }

    return SUCCESS;
}

int test_alloc_block_proc_waiters_timeout()
{
    int tests_attempted = 0;
    int tests_passed = 0;
    dragonError_t derr;
    dragonMemoryPoolDescr_t mpool;
    dragonM_UID_t m_uid = 1;
    char * fname = util_salt_filename("messaging_pool");
    derr = dragon_memory_pool_create(&mpool, 32768, fname, m_uid, NULL);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to create the memory pool");
    }

    dragonMemoryDescr_t mem_descr;

    size_t sz_alloc = 32000;
    derr = dragon_memory_alloc_blocking(&mem_descr, &mpool, sz_alloc, NULL);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Error making a large allocation");
    }

    int sem_rc = 0;
    // Allocate shared memory for the semaphore
    sem_t *sem = (sem_t *)mmap(NULL, sizeof(sem_t),
                            PROT_READ | PROT_WRITE,
                            MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    if (sem == MAP_FAILED) {
        perror("mmap");
        exit(EXIT_FAILURE);
    }

    sem_rc = sem_init(sem, 1, 0); // pshared=1 for inter-process
    if (sem_rc != 0) {
        perror("sem_init");
        exit(EXIT_FAILURE);
    }

    int num_children = 10;

    pid_t pid;
    for(int k=0;k<num_children;k++)
    {
        pid = fork();
        if (pid == 0)
        {
            sem_rc = sem_wait(sem);
            if (sem_rc != 0) {
                fprintf(stderr, "Failed to wait on semaphore: %s\n", strerror(errno));
                sem_destroy(sem);
                return EXIT_FAILURE;
            }

            void * ptr;
            derr = dragon_memory_get_pointer(&mem_descr, &ptr);
            if (derr != DRAGON_SUCCESS) {
                err_fail(derr, "Could not retrieve pointer from original descriptor");
            }

            dragonMemoryPoolSerial_t mem_ser_child;
            mem_ser_child.len = 0;
            memcpy(&mem_ser_child.len, ptr, sizeof(size_t));
            ptr = ptr + sizeof(size_t);
            mem_ser_child.data = (void *) malloc(mem_ser_child.len);
            memcpy(mem_ser_child.data, ptr, mem_ser_child.len);

            dragonMemoryPoolDescr_t mem_attach;
            derr = dragon_memory_pool_attach(&mem_attach, &mem_ser_child);
            if (derr != DRAGON_SUCCESS) {
                err_fail(derr, "Could not attach to serialized memory");
            }
            sleep(10);
            return proc_timeout_waiter(&mem_attach, 512, k);
        }
    }

    char * fname_gpu = util_salt_filename("test_gpu_memory");
    dragonMemoryPoolDescr_t mpool2;
    dragonM_UID_t m_uid2 = 2;
    dragonMemoryPoolAttr_t mattr;
    derr = dragon_memory_attr_init(&mattr);
    mattr.mem_type = DRAGON_MEMORY_TYPE_GPU;
    size_t pool_size = 32768;
    //size_t pool_size = 1073741824;
    derr = dragon_memory_pool_create(&mpool2, pool_size, fname_gpu, m_uid2, &mattr);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to create the GPU memory pool");
    }

    dragonMemoryPoolSerial_t mem_ser;
    mem_ser.len = 0;
    derr = dragon_memory_pool_serialize(&mem_ser, &mpool2);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Could not serialize memory descriptor");
    }
    if (mem_ser.len > sz_alloc) {
        printf("Error: serialized memory descriptor length %ld is larger than allocated size %lu\n", mem_ser.len, sz_alloc);
        return EXIT_FAILURE;
    }

    dragonMemoryPoolDescr_t mem_attach;
    derr = dragon_memory_pool_attach(&mem_attach, &mem_ser);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Could not attach to serialized memory");
    }

    void * ptr;
    derr = dragon_memory_get_pointer(&mem_descr, &ptr);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Could not retrieve pointer from original descriptor");
    }
    memcpy(ptr, &mem_ser.len, sizeof(size_t));
    ptr = ptr + sizeof(size_t);
    memcpy(ptr, mem_ser.data, mem_ser.len);

    void * ptr2;
    derr = dragon_memory_get_pointer(&mem_descr, &ptr2);
    dragonMemoryPoolSerial_t mem_ser_ptr;
    mem_ser_ptr.len = 0;
    memcpy(&mem_ser_ptr.len, ptr2, sizeof(size_t));
    ptr2 = ptr2 + sizeof(size_t);
    mem_ser_ptr.data = (void *) malloc(mem_ser_ptr.len);
    memcpy(mem_ser_ptr.data, ptr2, mem_ser_ptr.len);

    dragonMemoryPoolDescr_t mem_attach_ptr;
    derr = dragon_memory_pool_attach(&mem_attach_ptr, &mem_ser_ptr);

    dragonMemoryDescr_t mem;

    derr = dragon_memory_alloc_blocking(&mem, &mpool2, 32000, NULL);

    // let child know it can get the ipc handle
    for (int k=0; k<num_children; k++)
        sem_rc = sem_post(sem);

    derr = dragon_memory_free(&mem);

    int status;
    for (int k=0; k<num_children; k++) {
        wait(&status);
        if (status != 0) {
            printf("There was an error on a blocking waiter that returned status %d.\n", status);
        }
        else
            tests_passed += 1;
        tests_attempted += 1;
    }

    if (tests_attempted == tests_passed)
        printf("Test Passed\n");
    else
        printf("Test Failed\n");

    test_clone(&mpool2);

    derr = dragon_memory_pool_destroy(&mpool);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to destroy the memory pool");
    }
    derr = dragon_memory_pool_destroy(&mpool2);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to destroy the GPU memory pool");
    }

    return SUCCESS;
}

int test_fork_attach()
{
    int tests_attempted = 0;
    int tests_passed = 0;
    dragonError_t derr;
    dragonMemoryPoolDescr_t mpool;
    dragonM_UID_t m_uid = 1;
    char * fname = util_salt_filename("messaging_pool");
    derr = dragon_memory_pool_create(&mpool, 32768, fname, m_uid, NULL);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to create the memory pool");
    }

    dragonMemoryDescr_t mem_descr;

    size_t sz_alloc = 32000;
    derr = dragon_memory_alloc_blocking(&mem_descr, &mpool, sz_alloc, NULL);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Error making a large allocation");
    }

    int sem_rc = 0;
    // Allocate shared memory for the semaphore
    sem_t *sem = (sem_t *)mmap(NULL, sizeof(sem_t),
                            PROT_READ | PROT_WRITE,
                            MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    if (sem == MAP_FAILED) {
        perror("mmap");
        exit(EXIT_FAILURE);
    }

    sem_rc = sem_init(sem, 1, 0); // pshared=1 for inter-process
    if (sem_rc != 0) {
        perror("sem_init");
        exit(EXIT_FAILURE);
    }

    // the original only has one child
    int num_children = 1;

    pid_t pid;
    for(int k=0;k<num_children;k++)
    {
        pid = fork();
        if (pid == 0)
        {
            sem_rc = sem_wait(sem);
            if (sem_rc != 0) {
                fprintf(stderr, "Failed to wait on semaphore: %s\n", strerror(errno));
                sem_destroy(sem);
                return EXIT_FAILURE;
            }

            void * ptr;
            derr = dragon_memory_get_pointer(&mem_descr, &ptr);
            if (derr != DRAGON_SUCCESS) {
                err_fail(derr, "Could not retrieve pointer from original descriptor");
            }

            dragonMemoryPoolSerial_t mem_ser_child;
            mem_ser_child.len = 0;
            memcpy(&mem_ser_child.len, ptr, sizeof(size_t));
            ptr = ptr + sizeof(size_t);
            mem_ser_child.data = (void *) malloc(mem_ser_child.len);
            memcpy(mem_ser_child.data, ptr, mem_ser_child.len);

            dragonMemoryPoolDescr_t mem_attach;
            derr = dragon_memory_pool_attach(&mem_attach, &mem_ser_child);
            if (derr != DRAGON_SUCCESS) {
                err_fail(derr, "Could not attach to serialized memory");
            }
            return test_attach(&mem_attach);
        }
    }

    char * fname_gpu = util_salt_filename("test_gpu_memory");
    dragonMemoryPoolDescr_t mpool2;
    dragonM_UID_t m_uid2 = 2;
    dragonMemoryPoolAttr_t mattr;
    derr = dragon_memory_attr_init(&mattr);
    mattr.mem_type = DRAGON_MEMORY_TYPE_GPU;
    size_t pool_size = 32768;
    //size_t pool_size = 1073741824;
    derr = dragon_memory_pool_create(&mpool2, pool_size, fname_gpu, m_uid2, &mattr);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to create the GPU memory pool");
    }

//    dragonMemoryDescr_t mem_descr2;
//    derr = dragon_memory_alloc(&mem_descr2, &mpool2, sz_alloc);
//    if (derr != DRAGON_SUCCESS) {
//        err_fail(derr, "Error making a large allocation");
//    }

    //dragonMemoryDescr_t * mem_descr
    // need to get mem_descr for above pool
    dragonMemoryPoolSerial_t mem_ser;
    mem_ser.len = 0;
    derr = dragon_memory_pool_serialize(&mem_ser, &mpool2);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Could not serialize memory descriptor");
    }
    if (mem_ser.len > sz_alloc) {
        printf("Error: serialized memory descriptor length %ld is larger than allocated size %lu\n", mem_ser.len, sz_alloc);
        return EXIT_FAILURE;
    }

    dragonMemoryPoolDescr_t mem_attach;
    derr = dragon_memory_pool_attach(&mem_attach, &mem_ser);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Could not attach to serialized memory");
    }

    void * ptr;
    derr = dragon_memory_get_pointer(&mem_descr, &ptr);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Could not retrieve pointer from original descriptor");
    }
    memcpy(ptr, &mem_ser.len, sizeof(size_t));
    ptr = ptr + sizeof(size_t);
    memcpy(ptr, mem_ser.data, mem_ser.len);

    void * ptr2;
    derr = dragon_memory_get_pointer(&mem_descr, &ptr2);
    dragonMemoryPoolSerial_t mem_ser_ptr;
    mem_ser_ptr.len = 0;
    memcpy(&mem_ser_ptr.len, ptr2, sizeof(size_t));
    ptr2 = ptr2 + sizeof(size_t);
    mem_ser_ptr.data = (void *) malloc(mem_ser_ptr.len);
    memcpy(mem_ser_ptr.data, ptr2, mem_ser_ptr.len);

    dragonMemoryPoolDescr_t mem_attach_ptr;
    derr = dragon_memory_pool_attach(&mem_attach_ptr, &mem_ser_ptr);

    dragonMemoryDescr_t mem;

    derr = dragon_memory_alloc_blocking(&mem, &mpool2, 32000, NULL);

    derr = dragon_memory_free(&mem);
    // let child know it can get the ipc handle
    for (int k=0; k<num_children; k++)
        sem_rc = sem_post(sem);

    int status;
    for (int k=0; k<num_children; k++) {
        wait(&status);
        if (status != 0) {
            printf("There was an error on attach test %d.\n", status);
        } else {
            tests_passed += 1;
        }
        tests_attempted += 1;
    }

    if (tests_attempted == tests_passed)
        printf("Test Passed\n");
    else
        printf("Test Failed\n");

    derr = dragon_memory_pool_destroy(&mpool);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to destroy the memory pool");
    }
    derr = dragon_memory_pool_destroy(&mpool2);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to destroy the GPU memory pool");
    }

    return SUCCESS;
}

int test_rest_of_mem_test()
{
    dragonError_t derr;
    dragonMemoryPoolDescr_t mpool;
    dragonM_UID_t m_uid = 1;
    char * fname = util_salt_filename("messaging_pool");
    derr = dragon_memory_pool_create(&mpool, 32768, fname, m_uid, NULL);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to create the memory pool");
    }

    char * fname_gpu = util_salt_filename("test_gpu_memory");
    dragonMemoryPoolDescr_t mpool2;
    dragonM_UID_t m_uid2 = 2;
    dragonMemoryPoolAttr_t mattr;
    derr = dragon_memory_attr_init(&mattr);
    mattr.mem_type = DRAGON_MEMORY_TYPE_GPU;
    size_t pool_size = 32768;
    //size_t pool_size = 1073741824;
    derr = dragon_memory_pool_create(&mpool2, pool_size, fname_gpu, m_uid2, &mattr);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to create the GPU memory pool");
    }

    dragonMemoryDescr_t mem_descr;
    size_t sz_alloc = 32000;
    derr = dragon_memory_alloc_blocking(&mem_descr, &mpool, sz_alloc, NULL);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Error making a large allocation");
    }
    dragonMemoryDescr_t mem, mem2;

    derr = dragon_memory_alloc(&mem, &mpool2, 512);
    if (derr != DRAGON_SUCCESS) {
        main_err_fail(derr, "Failed to allocate from memory pool", jmp_destroy_pool);
    }

    derr = dragon_memory_alloc(&mem2, &mpool2, 512);
    if (derr != DRAGON_SUCCESS) {
        main_err_fail(derr, "Failed to allocate second from memory pool", jmp_destroy_pool);
    }

    dragonMemoryPoolDescr_t empty_pool_descr;
    dragonMemoryDescr_t mem3;
    derr = dragon_memory_alloc(&mem3, &empty_pool_descr, 512);
    if (derr == DRAGON_SUCCESS) {
        main_err_fail(derr, "Successfully allocated with what should be an invalid pool descriptor!\n", jmp_destroy_pool);
    }

    /* Test mem_descr functionality */
    int err = test_memdescr(&mem);
    if (err != 0) {
        goto jmp_destroy_pool;
    }

    /* Test getting allocation list functionality */
    if (test_allocations(&mpool2) != 0)
        goto jmp_destroy_pool;


    /* Test getting allocation list _of a specific type_ functionality */
    test_type_allocations(&mpool2, DRAGON_MEMORY_ALLOC_DATA);

    /* Try and get the first allocation */
    dragonMemoryDescr_t mem_get;
    derr = dragon_memory_descr_clone(&mem_get, &mem, 0, NULL);
    if (derr != DRAGON_SUCCESS) {
        main_err_fail(derr, "Failed to clone allocation structure", jmp_destroy_pool);
    }

    //TODO CPW: Commenting out for now and will think about this some more.
    //This process created this memory pool. It can't detach but rather needs to free it.
//    derr = dragon_memory_detach(&mem_get);
//    if (derr != DRAGON_SUCCESS) {
//        main_err_fail(derr, "Failed to detach from attached allocation structure", jmp_destroy_pool);
//    }

    dragonMemoryDescr_t type_mem;
    derr = dragon_memory_alloc_type(&type_mem, &mpool2, 512, 9001);
    if (derr != DRAGON_SUCCESS) {
        main_err_fail(derr, "Failed to allocate specific type", jmp_destroy_pool);
    }

    /* Test that type_alloc updates allocation lists appropriately */
    if (test_type_allocations(&mpool2, 0) != 0)
        goto jmp_destroy_pool;


    /* Assert our typed malloc freed correctly (third total malloc) */
    derr = dragon_memory_free(&type_mem);
    if (derr != DRAGON_SUCCESS) {
        main_err_fail(derr, "Failed to free from memory pool", jmp_destroy_pool);
    }


    /* Assert our second malloc freed correctly */
    derr = dragon_memory_free(&mem2);
    if (derr != DRAGON_SUCCESS) {
        main_err_fail(derr, "Failed to free from memory pool", jmp_destroy_pool);
    }

    /* Assert our first malloc freed correctly */
    derr = dragon_memory_free(&mem);
    if (derr != DRAGON_SUCCESS) {
        main_err_fail(derr, "Failed to free from memory pool", jmp_destroy_pool);
    }

jmp_destroy_pool:
    /* Use this as a cleanup GOTO so we don't need to manually delete SHM files */
    derr = dragon_memory_pool_destroy(&mpool);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to destroy the memory pool");
    }
    derr = dragon_memory_pool_destroy(&mpool2);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to destroy the GPU memory pool");
    }

    return TEST_STATUS;

}

int forked_process_setup(sem_t * sync_sem, dragonMemoryDescr_t * messaging_mem_descr, dragonMemoryDescr_t * new_mem_descr)
{
    dragonError_t derr;
    int sem_rc = 0;
    sem_rc = sem_wait(sync_sem);
    if (sem_rc != 0) {
        fprintf(stderr, "Failed to wait on semaphore: %s\n", strerror(errno));
        sem_destroy(sync_sem);
        return EXIT_FAILURE;
    }

    void * ptr;
    derr = dragon_memory_get_pointer(messaging_mem_descr, &ptr);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Could not retrieve pointer from original descriptor");
    }
    dragonMemorySerial_t mem_ser_child;
    mem_ser_child.len = 0;
    memcpy(&mem_ser_child.len, ptr, sizeof(size_t));
    ptr = ptr + sizeof(size_t);
    mem_ser_child.data = (void *) malloc(mem_ser_child.len);
    memcpy(mem_ser_child.data, ptr, mem_ser_child.len);

    derr = dragon_memory_attach(new_mem_descr, &mem_ser_child);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Could not attach to serialized memory");
    }

    //response to above malloc being done and showing up as leak
    derr = dragon_memory_serial_free(&mem_ser_child);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Could not free serialized memory descriptor");
    }

    return DRAGON_SUCCESS;
}

int copy_str_to_gpu(char * str, dragonMemoryDescr_t * mem_descr)
{
    dragonError_t derr;
    dragonMemoryPoolDescr_t mpool_host;
    dragonM_UID_t m_uid = 10;
    dragonMemoryDescr_t mem_descr_host;
    char * fname = util_salt_filename("test_copy_pool");
    derr = dragon_memory_pool_create(&mpool_host, 32768, fname, m_uid, NULL);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to create the memory pool");
    }

    derr = dragon_memory_alloc_blocking(&mem_descr_host, &mpool_host, 512, NULL);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Error making a large allocation");
    }

    printf("got pointer and strcpy %s into it\n", str);
    void * ptr_host;
    derr = dragon_memory_get_pointer(&mem_descr_host, &ptr_host);
    strcpy((char*)ptr_host, str);
    printf("ptr after strcpy into it is %s\n", (char *)ptr_host);
    derr = dragon_memory_copy(&mem_descr_host, mem_descr);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Could not copy H2D");
    }

    derr = dragon_memory_free(&mem_descr_host);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Could not free host memory");
    }

    derr = dragon_memory_pool_destroy(&mpool_host);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Could not destroy host memory pool");
    }

    free(fname);

    return DRAGON_SUCCESS;
}

int copy_str_from_gpu(char * str, dragonMemoryDescr_t * mem_descr)
{
    dragonError_t derr;
    dragonMemoryPoolDescr_t mpool_host;
    dragonM_UID_t m_uid = 13;
    dragonMemoryDescr_t mem_descr_host;
    char * fname = util_salt_filename("test_copy_pool2");
    derr = dragon_memory_pool_create(&mpool_host, 32768, fname, m_uid, NULL);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to create the memory pool");
    }

    derr = dragon_memory_alloc_blocking(&mem_descr_host, &mpool_host, 512, NULL);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Error making a large allocation");
    }

    derr = dragon_memory_copy(mem_descr, &mem_descr_host);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Could not copy D2H");
    }

    void * ptr;
    derr = dragon_memory_get_pointer(&mem_descr_host, &ptr);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Could not retrieve pointer from attached descriptor");
    }
    strcpy(str, (char*)ptr);

    derr = dragon_memory_free(&mem_descr_host);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Could not free host memory");
    }

    derr = dragon_memory_pool_destroy(&mpool_host);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Could not destroy host memory pool");
    }
    free(fname);

    return DRAGON_SUCCESS;
}


int test_holy_grail()
{
    dragonError_t derr;
    dragonMemoryPoolDescr_t mpool;
    dragonM_UID_t m_uid = 1;
    char * fname = util_salt_filename("messaging_pool");
    derr = dragon_memory_pool_create(&mpool, 32768, fname, m_uid, NULL);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to create the memory pool");
    }

    dragonMemoryDescr_t mem_descr;
    size_t sz_alloc = 32000;
    derr = dragon_memory_alloc_blocking(&mem_descr, &mpool, sz_alloc, NULL);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Error making a large allocation");
    }

    int sem_rc = 0;
    // Allocate shared memory for the semaphore
    sem_t *sem = (sem_t *)mmap(NULL, sizeof(sem_t),
                            PROT_READ | PROT_WRITE,
                            MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    if (sem == MAP_FAILED) {
        perror("mmap");
        exit(EXIT_FAILURE);
    }

    sem_rc = sem_init(sem, 1, 0); // pshared=1 for inter-process
    if (sem_rc != 0) {
        perror("sem_init");
        exit(EXIT_FAILURE);
    }

    char * str = "Hello";

    pid_t pid;
    pid = fork();
    if (pid == 0)
    {
        dragonMemoryDescr_t mem_attach;
        derr = forked_process_setup(sem, &mem_descr, &mem_attach);
        if (derr != DRAGON_SUCCESS) {
            err_fail(derr, "Error setting up forked process");
        }

        derr = copy_str_to_gpu(str, &mem_attach);
        if (derr != DRAGON_SUCCESS) {
            err_fail(derr, "Error copying string to GPU memory");
        }

        derr = dragon_memory_detach(&mem_attach);
        if (derr != DRAGON_SUCCESS) {
            err_fail(derr, "Error detaching memory from GPU");
        }
    }
    else
    {
        char * fname_gpu = util_salt_filename("test_gpu_memory");
        dragonMemoryPoolDescr_t mpool_gpu;
        dragonM_UID_t m_uid2 = 2;
        dragonMemoryPoolAttr_t mattr;
        derr = dragon_memory_attr_init(&mattr);
        mattr.mem_type = DRAGON_MEMORY_TYPE_GPU;
        // setting gpu device to non-default. this will break if the node only has a single gpu
        mattr.gpu_device_id = 1UL;
        size_t pool_size = 1073741824;
        derr = dragon_memory_pool_create(&mpool_gpu, pool_size, fname_gpu, m_uid2, &mattr);
        if (derr != DRAGON_SUCCESS) {
            err_fail(derr, "Failed to create the GPU memory pool");
        }

        dragonMemoryDescr_t mem_descr_gpu;
        derr = dragon_memory_alloc_blocking(&mem_descr_gpu, &mpool_gpu, 512, NULL);
        if (derr != DRAGON_SUCCESS) {
            err_fail(derr, "Error making a large allocation");
        }

        dragonMemorySerial_t mem_ser;
        mem_ser.len = 0;
        dragonError_t derr = dragon_memory_serialize(&mem_ser, &mem_descr_gpu);
        if (derr != DRAGON_SUCCESS) {
            err_fail(derr, "Could not serialize memory descriptor");
        }
        if (mem_ser.len > sz_alloc) {
            printf("Error: serialized memory descriptor length %ld is larger than allocated size %lu\n", mem_ser.len, sz_alloc);
            return EXIT_FAILURE;
        }

        void * ptr;
        derr = dragon_memory_get_pointer(&mem_descr, &ptr);
        if (derr != DRAGON_SUCCESS) {
            err_fail(derr, "Could not retrieve pointer from original descriptor");
        }
        memcpy(ptr, &mem_ser.len, sizeof(size_t));
        ptr = ptr + sizeof(size_t);
        memcpy(ptr, mem_ser.data, mem_ser.len);

        // let child know it can get the ipc handle
        sem_rc = sem_post(sem);

        int status;
        wait(&status);
        if (status != 0) {
            printf("There was an error on attach test %d.\n", status);
            return EXIT_FAILURE;
        }

        char * new_str = (char *) malloc(512);
        derr = copy_str_from_gpu(new_str, &mem_descr_gpu);
        if (derr != DRAGON_SUCCESS)
        {
            err_fail(derr, "Error copying string from GPU memory");
        }

        printf("String copied from GPU: %s\n", new_str);
        if (strcmp(new_str, str) != 0) {
            printf("Error: Copied string does not match original string!\n");
            return EXIT_FAILURE;
        } else {
            printf("String copied successfully and matches original\n");
        }

        derr = dragon_memory_free(&mem_descr_gpu);
        if (derr != DRAGON_SUCCESS) {
            err_fail(derr, "Could not free gpu memory");
        }

        derr = dragon_memory_pool_destroy(&mpool_gpu);
        if (derr != DRAGON_SUCCESS) {
            err_fail(derr, "Could not destroy host memory pool");
        }

        derr = dragon_memory_free(&mem_descr);
        if (derr != DRAGON_SUCCESS) {
            err_fail(derr, "Could not free gpu memory");
        }

        derr = dragon_memory_pool_destroy(&mpool);
        if (derr != DRAGON_SUCCESS) {
            err_fail(derr, "Could not destroy host memory pool");
        }
        derr = dragon_memory_serial_free(&mem_ser);
        if (derr != DRAGON_SUCCESS) {
            err_fail(derr, "Could not destroy free serialized memory descriptor");
        }
        free(fname_gpu);
        free(new_str);
    }

    free(fname);
    return SUCCESS;

}

int test_cases(int test_case)
{
    printf("\n");
    switch (test_case) {
        case 0:
            printf("Running pool create/destroy GPU lifecycle test\n");
            return pool_create_destroy_gpu_lifecycle();
        case 1:
            printf("Running alloc block proc waiters\n");
            return test_alloc_block_proc_waiters();
        case 2:
            printf("Running alloc block proc waiters with timeout\n");
            return test_alloc_block_proc_waiters_timeout();
        case 3:
            printf("Running test attach with fork\n");
            return test_fork_attach();
        case 4:
            printf("Running test mem serialization and attachment\n");
            return test_rest_of_mem_test();
        case 5:
            printf("Running test passing string through GPU buffer\n");
            return test_holy_grail();
        default:
            printf("Invalid test case number: %d\n", test_case);
            return EXIT_FAILURE;
    }

}

int test_loop_forked()
{
    int num_tests = 6;
    pid_t pid;
    for(int k=0;k<num_tests;k++)
    {
        pid = fork();
        if (pid == 0)
        {
            return test_cases(k);
        }
        else
        {
            int status;
            wait(&status);
            if (status != 0) {
                printf("Child process returned with error status %d\n", status);
                return status;
            } else {
                printf("Child process completed test case %d successfully\n", k);
            }
        }
    }
    return DRAGON_SUCCESS;
}

int main(int argc, char *argv[])
{
    return test_loop_forked();
}
