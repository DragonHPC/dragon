#include <dragon/managed_memory.h>
#include <dragon/queue.h>
#include <stdlib.h>
#include <stdio.h>

#include "../_ctest_utils.h"

#define ELMS 1000

int main(void)
{
    size_t mem_size = 1UL<<30;
    dragonMemoryPoolDescr_t mpool;
    dragonM_UID_t m_uid = 1;
    char * fname = util_salt_filename("test_queue");

    printf("Pool Create\n");
    dragonError_t derr = dragon_memory_pool_create(&mpool, mem_size, fname, m_uid, NULL);
    if (derr != DRAGON_SUCCESS)
        err_fail(derr, "Failed to create the memory pool");

    dragonQueueDescr_t queue;
    dragonQ_UID_t q_uid = 90210;
    derr = dragon_queue_create(&mpool, DRAGON_QUEUE_DEFAULT_MAXSIZE, q_uid, false, NULL, NULL, &queue);
    if (derr != DRAGON_SUCCESS)
        main_err_fail(derr, "Failed to create queue", jmp_destroy_pool);

    /* ++++++++++++++++
       Test put and get of an array
       ++++++++++++++++ */
    int * arr = malloc(sizeof(int) * ELMS);
    for (int i = 0; i < ELMS; i++)
        arr[i] = i + 1;

    derr = dragon_queue_put(&queue, (void*)arr, sizeof(int)*ELMS, NULL);
    if (derr != DRAGON_SUCCESS)
        main_err_fail(derr, "Failed to put into queue", jmp_destroy_pool);

    int * arr2 = NULL;
    size_t ret_sz;
    derr = dragon_queue_get(&queue, (void**)&arr2, &ret_sz, NULL);
    if (derr != DRAGON_SUCCESS)
        main_err_fail(derr, "Failed to get from queue", jmp_destroy_pool);

    printf("Inserted int array size %ld\n", sizeof(int)*ELMS);
    printf("Retrieved int array size %ld\n", ret_sz);
    if (sizeof(int) * ELMS != ret_sz) {
        printf("Sizes do not match\n");
        goto jmp_destroy_pool;
    }
    printf("OK: Sizes match\n");

    for (int i = 1; i < ELMS; i++) {
        if (arr[i] != arr2[i]) {
            printf("Error: Expected value at idx %d to be %d, but was %d\n", i, arr[i], arr2[i]);
            TEST_STATUS = FAILED;
            goto jmp_destroy_pool;
        }
    }
    printf("OK: Retrieved array matches inserted array\n");

    free(arr);
    free(arr2);

    /* ++++++++++++++++
       Test filling up a queue
       ++++++++++++++++ */
    for (int i = 0; i < DRAGON_QUEUE_DEFAULT_MAXSIZE; i++) {
        derr = dragon_queue_put(&queue, (void*)&i, sizeof(int), NULL);
        if (derr != DRAGON_SUCCESS) {
            printf("Error: Failed to insert value %d in loop\n", i);
            main_err_fail(derr, " ", jmp_destroy_pool);
        }
    }
    printf("OK: Inserted max number of items\n");

    int x = 101;
    derr = dragon_queue_put(&queue, (void*)&x, sizeof(int), NULL);
    if (derr == DRAGON_SUCCESS)
        main_err_fail(derr, "Expected failure to insert additional item", jmp_destroy_pool);

    printf("OK: Could not insert item when queue capacity full\n");

    /* ++++++++++++++++
       Make sure a full queue retrieves all values correctly
       ++++++++++++++++ */
    for (int i = 0; i < DRAGON_QUEUE_DEFAULT_MAXSIZE; i++) {
        int * y;
        derr = dragon_queue_get(&queue, (void**)&y, &ret_sz, NULL);
        if (derr != DRAGON_SUCCESS) {
            printf("Failed to retrieve from queue (idx %d)\n", i);
            main_err_fail(derr, " ", jmp_destroy_pool);
        }

        if (*y != i) {
            printf("Retrieved %d, expected %d\n", *y, i);
            TEST_STATUS = FAILED;
            goto jmp_destroy_pool;
        }
        free(y);
    }
    printf("OK: All queue values match\n");

    /* ++++++++++++++++
       Serialize, attach, detach, re-attach
       ++++++++++++++++ */
    dragonQueueSerial_t q_ser;
    derr = dragon_queue_serialize(&queue, &q_ser);
    if (derr != DRAGON_SUCCESS)
        main_err_fail(derr, "Failed to serialize queue", jmp_destroy_pool);

    derr = dragon_queue_detach(&queue);
    if (derr != DRAGON_SUCCESS)
        main_err_fail(derr, "Failed to detach from queue", jmp_destroy_pool);

    printf("OK: Queue serialized, detached\n");

    dragonQueueDescr_t q2;
    derr = dragon_queue_attach(&q_ser, &q2);
    if (derr != DRAGON_SUCCESS)
        main_err_fail(derr, "Failed to attach to queue", jmp_destroy_pool);

    printf("OK: Queue re-attached\n");

    derr = dragon_queue_destroy(&q2);
    if (derr != DRAGON_SUCCESS)
        main_err_fail(derr, "Failed to destroy queue", jmp_destroy_pool);

    printf("OK: Queue destroyed\n");

 jmp_destroy_pool:
    derr = dragon_memory_pool_destroy(&mpool);
    if (derr != DRAGON_SUCCESS)
        err_fail(derr, "Failed to destroy memory pool");

    printf("End of test\n");
    return TEST_STATUS;
}
