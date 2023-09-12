#include "../../src/lib/priority_heap.h"
#include "../../src/include/dragon/global_types.h"
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "../_ctest_utils.h"

#define NREPEATS 1000
#define BILL 1000000000L

void perf_test()
{
    dragonError_t derr;
    dragonPriorityHeapLongUint_t capacity;
    dragonPriorityHeapUint_t base;

    capacity = 32768;
    base = 2;

    size_t req_bytes;
    req_bytes = dragon_priority_heap_size(capacity, 1);

    dragonPriorityHeapLongUint_t * myheap_mem = malloc(req_bytes);
    dragonPriorityHeapLongUint_t val;

    dragonPriorityHeap_t myheap;
    derr = dragon_priority_heap_init(&myheap, base, capacity, 1, myheap_mem);

    for (int i = 0; i < capacity; i++) {

        val = i;
        dragon_priority_heap_insert_item(&myheap, &val);

    }

    timespec_t t1, t2;
    clock_gettime(CLOCK_MONOTONIC, &t1);

    for (int j = 0; j < NREPEATS; j++) {

        for (int i = 0; i < capacity; i++) {

            dragonPriorityHeapLongUint_t priority;
            derr = dragon_priority_heap_extract_highest_priority(&myheap, &val, &priority);
            if (derr != DRAGON_SUCCESS)
                printf("A Crap: %i\n", derr);
            derr = dragon_priority_heap_insert_urgent_item(&myheap, &val);
            if (derr != DRAGON_SUCCESS)
                printf("B Crap: %i\n", derr);
        }
    }

    clock_gettime(CLOCK_MONOTONIC, &t2);

    double etime   = 1e-9 * (double)(BILL * (t2.tv_sec - t1.tv_sec) + (t2.tv_nsec - t1.tv_nsec));
    double mrate   = NREPEATS * capacity / etime;

    printf("Perf: pt2pt message rate = %5.3f msgs/sec\n", mrate);
}

int main(int argc, char *argv[])
{
    dragonError_t derr;
    dragonPriorityHeapLongUint_t capacity;
    dragonPriorityHeapUint_t base, nvals_per_key;

    capacity = 15;
    nvals_per_key = 2;
    base = 3;

    size_t req_bytes;
    req_bytes = dragon_priority_heap_size(capacity, nvals_per_key);

    printf("Require %li bytes for capacity=%li, vals_per_key=%i\n",
           req_bytes, capacity, nvals_per_key);

    dragonPriorityHeapLongUint_t * myheap_mem = malloc(req_bytes);
    dragonPriorityHeapLongUint_t * vals = malloc(sizeof(dragonPriorityHeapLongUint_t) * nvals_per_key);

    dragonPriorityHeap_t myheap;
    derr = dragon_priority_heap_init(&myheap, base, capacity, nvals_per_key, myheap_mem);
    if (derr != DRAGON_SUCCESS) {
        printf("INIT ERR = %i\n",derr);
        return FAILED;
    }

    int i, j, k;
    for (i = 0; i < capacity; i++) {
        for (j = 0; j < nvals_per_key; j++) {
            vals[j] = i*nvals_per_key + j;
        }
        if (i != 5) {
            derr = dragon_priority_heap_insert_item(&myheap, vals);
        } else {
            derr = dragon_priority_heap_insert_urgent_item(&myheap, vals);
        }
        if (derr != DRAGON_SUCCESS) {
            printf("INSERT ERR = %i\n",derr);
            return FAILED;
        }

        for (j = 0; j <= i; j++) {
            printf("H[%i]: ", j);
            printf("p = %li, ", myheap._harr[(nvals_per_key+1)*j]);
            for (k = 1; k <= nvals_per_key; k++) {
                printf("v[%i] = %li, ", k-1, myheap._harr[(nvals_per_key+1)*j+k]);
            }
            printf("\n");
        }
        printf("\n");
    }

    for (i = 0; i < capacity; i++) {

        for (j = 0; j < capacity; j++) {
            printf("H[%i]: ", j);
            printf("p = %li, ", myheap._harr[(nvals_per_key+1)*j]);
            for (k = 1; k <= nvals_per_key; k++) {
                printf("v[%i] = %li, ", k-1, myheap._harr[(nvals_per_key+1)*j+k]);
            }
            printf("\n");
        }

        dragonPriorityHeapLongUint_t priority;
        derr = dragon_priority_heap_extract_highest_priority(&myheap, vals, &priority);
        if (derr != DRAGON_SUCCESS) {
            printf("EXTRACT ERR = %i\n",derr);
            return FAILED;
        }
        printf("Extracted: pri = %li, vals: ", priority);
        for (j = 0; j < nvals_per_key; j++) {
            printf("v[%i] = %li, ", j, vals[j]);
        }
        printf("\n");
    }

    free(myheap_mem);
    free(vals);

    perf_test();

    return SUCCESS;
}
