#include "../../src/lib/_heap_manager.h"
#include "../../src/lib/shared_lock.h"
#include <dragon/utils.h>
#include <dragon/return_codes.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#define TRUE 1
#define FALSE 0

size_t values[] = {1382,250915223,1916,2121290834,449,177372358,2115,360690951,3524,286665256,
                   191,1241134,3778,29086950,1927,36346056,703,9029225,3507,84239849,3085,
                   4864365,81,2227456,2455,810373453,2991,40179276,3798,35426764,832,41696064,
                   3579,3584099,1329,2473419,2783,314163,1926,207293,1339,20486641,452,1546034581,
                   710,25659,3314,79469219,1828,65647013,812,2911824890,957,3223138597,3262,3072378300,807,
                   31477418,3862, 4096};

int main(int argc, char* argv[]) {

    timespec_t t1, t2;
    size_t tests_passed = 0;
    size_t sub_tests_failed = 0;

    size_t bitsetsize;
    dragonError_t brc;

    bitsetsize = dragon_bitset_size(1024);

    void* bsptr = malloc(bitsetsize);

    void* bsptr2 = malloc(bitsetsize);

    dragonBitSet_t bset, bset2;

    brc = dragon_bitset_init(bsptr,&bset,997);

    if (brc != DRAGON_SUCCESS) {
        printf("Error Code was %u\n",brc);
    }

    brc = dragon_bitset_init(bsptr2,&bset2,997);

    if (brc != DRAGON_SUCCESS) {
        printf("Error Code was %u\n",brc);
    }

    brc = dragon_bitset_set(&bset, 0);

    if (brc != DRAGON_SUCCESS) {
        printf("Error Code was %u\n",brc);
    }

    brc = dragon_bitset_set(&bset, 400);

    if (brc != DRAGON_SUCCESS) {
        printf("Error Code was %u\n",brc);
    }

    brc = dragon_bitset_set(&bset, 916);

    if (brc != DRAGON_SUCCESS) {
        printf("Error Code was %u\n",brc);
    }

    brc = dragon_bitset_set(&bset, 42);

    if (brc != DRAGON_SUCCESS) {
        printf("Error Code was %u\n",brc);
    }

    unsigned char bit;

    brc = dragon_bitset_get(&bset, 42, &bit);

    if (brc != DRAGON_SUCCESS) {
        printf("Error Code was %u\n",brc);
    }

    if (bit == 1) {
        printf("That was a one\n");
    }

    brc = dragon_bitset_reset(&bset, 42);

    if (brc != DRAGON_SUCCESS) {
        printf("Error Code was %u\n",brc);
    }

    brc = dragon_bitset_dump("A Bit Dump", &bset, "");

    if (brc != DRAGON_SUCCESS) {
        printf("Error Code was %u\n",brc);
    }

    brc = dragon_bitset_copy(&bset2, &bset);

    if (brc != DRAGON_SUCCESS) {
        printf("Error Code was %u\n",brc);
    }

    brc = dragon_bitset_dump("2nd Bit Dump", &bset2, "");

    if (brc != DRAGON_SUCCESS) {
        printf("Error Code was %u\n",brc);
    }

    size_t val = 0;

    dragon_bitset_zeroes_to_right(&bset,0,&val);

    printf("The number is %lu\n",val);

    // computes length of static array.
    size_t malloc_tests_size = sizeof(values) / sizeof(size_t);

    // reserve enough room for pointers to all mallocs.
    void** allocations = malloc(sizeof(void**)*malloc_tests_size);

    // make a heap of size 4GB with 4K segments as minimum block size. How much space
    // is required?
    size_t heap_size;

    dragon_heap_size(32,12,4096,DRAGON_LOCK_FIFO,&heap_size);

    // get the required space.
    void* space = (dragonDynHeap_t*)malloc(heap_size);

    dragonDynHeap_t heap;

    printf("Test Case 1 : Validate Inited Heap Handle\n");

    clock_gettime(CLOCK_MONOTONIC, &t1);
    // Initialize the heap.
    dragonError_t rc = dragon_heap_init(space, &heap, 32, 12, 4096, DRAGON_LOCK_FIFO, NULL);
    clock_gettime(CLOCK_MONOTONIC, &t2);
    double timer = 1e-9 * (double)(1000000000L * (t2.tv_sec - t1.tv_sec) +
                                   (t2.tv_nsec - t1.tv_nsec));

    printf("  dragon_heap_init duration=%lf seconds.\n", timer);

    if (rc != DRAGON_SUCCESS) {
        sub_tests_failed++;
        printf("  Init of heap did not succeed. Return code was %u\n",rc);
    }

    if (heap.num_segments != 1048576) {
        sub_tests_failed++;
        printf("  Incorrect number of segments %lu when 1048576 was expected.\n", heap.num_segments);
    }

    if (heap.segment_size != 4096) {
        sub_tests_failed++;
        printf("  Incorrect segment size of %lu when 4096 was expected.\n", heap.segment_size);
    }

    if (heap.num_freelists != 21) {
        sub_tests_failed++;
        printf("  Incorrect number of freelists %lu when 21 was expected.\n", heap.num_freelists);
    }

    if (*heap.recovery_needed_ptr) {
        sub_tests_failed++;
        printf("  Recovery_needed should be false after heap creation. It is not.\n");
    }

    for (int k=0;k<heap.num_freelists-1;k++) {
        if (heap.free_lists[k] != 0) {
            sub_tests_failed++;
            printf("  Free list at %d should be empty. It is not.\n",k);
        }
    }

    if (sub_tests_failed == 0)  {
        tests_passed++;
        printf("  Passed Test 1.\n");
    } else {
        printf("  Failed Test 1.\n");
    }

    printf("Test Case 2 : Validate Attached Heap Handle\n");

    sub_tests_failed = 0;

    dragonDynHeap_t heap2;

    clock_gettime(CLOCK_MONOTONIC, &t1);

    rc = dragon_heap_attach(space, &heap2);

    if (rc != DRAGON_SUCCESS) {
        sub_tests_failed++;
        printf("  The attach did not return success. It returned %u\n", rc);
    }

    clock_gettime(CLOCK_MONOTONIC, &t2);
    timer = 1e-9 * (double)(1000000000L * (t2.tv_sec - t1.tv_sec) +
                                   (t2.tv_nsec - t1.tv_nsec));

    printf("  dragon_heap_attach duration=%lf seconds.\n", timer);

    if (heap.base_pointer != heap2.base_pointer) {
        sub_tests_failed++;
        printf("  The mutex pointer was different in attached handle vs inited handle.\n");
    }

    if (heap.num_segments != heap2.num_segments) {
        sub_tests_failed++;
        printf("  The number of segments was incorrect in the attached handle.\n");
    }

    if (heap.segment_size != heap2.segment_size) {
        sub_tests_failed++;
        printf("  The segment size was incorrect in the attached handle.\n");
    }

    if (heap.num_freelists != heap2.num_freelists) {
        sub_tests_failed++;
        printf("  The number of free lists was incorrect in the attached handle.\n");
    }

    if (heap.recovery_needed_ptr != heap2.recovery_needed_ptr) {
        sub_tests_failed++;
        printf("  The recovery_needed pointer was different in attached handle vs inited handle.\n");
    }

    if (heap.free_lists != heap2.free_lists) {
        sub_tests_failed++;
        printf("  The address of free_lists is wrong in an attached handle to the heap.\n");
    }

    for (int k=0;k<heap2.num_freelists-1;k++) {
        if (heap2.free_lists[k] != 0) {
            sub_tests_failed++;
            printf("  Free list at %d should be empty. It is not.\n",k);
        }
    }

    if (heap2.free_lists[20] == 0) {
        sub_tests_failed++;
        printf("  Free list at 20 should not be empty. It is.\n");
    }

    if (sub_tests_failed == 0)  {
        tests_passed++;
        printf("  Passed Test 2.\n");
    } else {
        printf("  Failed Test 2.\n");
    }

    printf("Test Case 3 : Validate Stats Call\n");

    sub_tests_failed = 0;

    dragonHeapStats_t stats;

    clock_gettime(CLOCK_MONOTONIC, &t1);

    dragon_heap_get_stats(&heap, &stats);

    clock_gettime(CLOCK_MONOTONIC, &t2);
    timer = 1e-9 * (double)(1000000000L * (t2.tv_sec - t1.tv_sec) +
                                   (t2.tv_nsec - t1.tv_nsec));

    printf("  dragon_heap_get_stats duration=%lf seconds.\n", timer);

    if (stats.num_segments != 1048576) {
        sub_tests_failed++;
        printf("  Stats number of segments was wrong. It was %lu and it was expected that it would be 1048576\n", stats.num_segments);
    }

    if (stats.segment_size != 4096) {
        sub_tests_failed++;
        printf("  Incorrect segment size. It was supposed to be 4096. It was %lu instead.\n",stats.segment_size);
    }

    if (stats.total_size != 4294967296) {
        sub_tests_failed++;
        printf("  Total size should be 4294967296. It was %lu\n", stats.total_size);
    }

    if (stats.num_block_sizes != 21) {
        sub_tests_failed++;
        printf("  Number of block sizes should be 21. It was %lu\n", stats.num_block_sizes);
    }

    if (stats.utilization_pct != 0) {
        sub_tests_failed++;
        printf("  The total percent utilization should be 0. It was %lf.\n", stats.utilization_pct);
    }

    if (sub_tests_failed == 0)  {
        tests_passed++;
        printf("  Passed Test 3.\n");
    } else {
        printf("  Failed Test 3.\n");
    }

    sub_tests_failed = 0;

    printf("Test Case 4 : Malloc\n");

    void* tmp;


    clock_gettime(CLOCK_MONOTONIC, &t1);

    rc = dragon_heap_malloc(&heap, 16, &tmp);

    clock_gettime(CLOCK_MONOTONIC, &t2);
    timer = 1e-9 * (double)(1000000000L * (t2.tv_sec - t1.tv_sec) +
                                   (t2.tv_nsec - t1.tv_nsec));

    printf("  Initial worst case dragon_heap_malloc for 4GB to 4K heap duration=%lf seconds.\n", timer);


    dragon_heap_get_stats(&heap, &stats);

    if (stats.utilization_pct > 0.000096 || stats.utilization_pct < 0.000094) {
        sub_tests_failed++;
        printf("Utilization Percentage should be approximately 0.000095367431640. It was %lf\n", stats.utilization_pct);
    }

    if (stats.num_block_sizes != 21) {
        sub_tests_failed++;
        printf("  Number of block sizes should be 21. It was %lu\n", stats.num_block_sizes);
    }

    for (int k=0;k<stats.num_block_sizes-1;k++) {
        if (stats.free_blocks[k].num_blocks != 1) {
            sub_tests_failed++;
            printf("  Expected number of free blocks for blocks size %lu to be 1. It was %lu.\n",stats.free_blocks[k].block_size, stats.free_blocks[k].num_blocks);
        }
    }

    if (stats.free_blocks[20].num_blocks != 0) {
        sub_tests_failed++;
        printf("  Expected number of free blocks for blocks size %lu to be 0. It was %lu.\n",stats.free_blocks[20].block_size, stats.free_blocks[20].num_blocks);
    }

    if (sub_tests_failed == 0)  {
        tests_passed++;
        printf("  Passed Test 4.\n");
    } else {
        printf("  Failed Test 4.\n");
    }

    dragon_heap_dump("Heap after successful malloc", &heap);

    sub_tests_failed = 0;

    printf("Test Case 5 : Free\n");

    clock_gettime(CLOCK_MONOTONIC, &t1);
    dragon_heap_free(&heap, tmp);

    clock_gettime(CLOCK_MONOTONIC, &t2);
    timer = 1e-9 * (double)(1000000000L * (t2.tv_sec - t1.tv_sec) +
                                   (t2.tv_nsec - t1.tv_nsec));

    printf("  Worst case dragon_heap_free for joins of blocks in 4GB to 4K heap duration=%lf seconds.\n", timer);

    dragon_heap_get_stats(&heap, &stats);

    if (stats.utilization_pct != 0.0) {
        sub_tests_failed++;
        printf("  Utilization Percentage should be approximately 0.0. It was %lf\n", stats.utilization_pct);
    }

    for (int k=0;k<stats.num_block_sizes-1;k++) {
        if (stats.free_blocks[k].num_blocks != 0) {
            sub_tests_failed++;
            printf("  Expected number of free blocks for blocks size %lu to be 0. It was %lu.\n",stats.free_blocks[k].block_size, stats.free_blocks[k].num_blocks);
        }
    }

    if (stats.free_blocks[20].num_blocks != 1) {
        sub_tests_failed++;
        printf("  Expected number of free blocks for blocks size %lu to be 1. It was %lu.\n",stats.free_blocks[20].block_size, stats.free_blocks[20].num_blocks);
    }

    if (sub_tests_failed == 0)  {
        tests_passed++;
        printf("  Passed Test 5.\n");
    } else {
        printf("  Failed Test 5.\n");
    }

    dragon_heap_dump("Heap after successful free of only allocated block.", &heap);

    sub_tests_failed = 0;

    printf("Test Case 6: Corrupting Free List\n");

    dragon_heap_malloc(&heap, 32, &tmp);
    void* tmp2;
    dragon_heap_malloc(&heap,5000, &tmp2);

    //purposely overwrite the linked list to force recovery.
    size_t k;
    for (k=0;k<5100;k++) {
        char* c_ptr = (char*)(tmp + k);
        *c_ptr = 99;
    }

    // This free will result in the heap manager detecting that recovery is needed. But recovery won't be done
    // until the next call.
    clock_gettime(CLOCK_MONOTONIC, &t1);

    rc = dragon_heap_free(&heap, tmp);

    clock_gettime(CLOCK_MONOTONIC, &t2);
    timer = 1e-9 * (double)(1000000000L * (t2.tv_sec - t1.tv_sec) +
                                   (t2.tv_nsec - t1.tv_nsec));

    printf("  First free after corruption needing and detecting recovery needed duration=%lf seconds.\n", timer);


    if (rc != DRAGON_SUCCESS) {
        sub_tests_failed++;
        printf("  A free of a pointer that should have completed successfully did not. Return code was %u\n",rc);
    }

    clock_gettime(CLOCK_MONOTONIC, &t1);

    // This free is rejected.
    rc = dragon_heap_free(&heap, tmp2);

    clock_gettime(CLOCK_MONOTONIC, &t2);
    timer = 1e-9 * (double)(1000000000L * (t2.tv_sec - t1.tv_sec) +
                                   (t2.tv_nsec - t1.tv_nsec));

    printf("  Second free after corruption and insisting recovery needed duration=%lf seconds.\n", timer);

    if (rc != DRAGON_DYNHEAP_RECOVERY_REQUIRED) {
        sub_tests_failed++;
        printf("  A free of a pointer that should have caused a recover needed return code did not. Return code was %u\n",rc);
    }

    printf("  Recovery is needed and being attempted.\n");
    clock_gettime(CLOCK_MONOTONIC, &t1);

    dragon_heap_recover(&heap);

    clock_gettime(CLOCK_MONOTONIC, &t2);
    timer = 1e-9 * (double)(1000000000L * (t2.tv_sec - t1.tv_sec) +
                                   (t2.tv_nsec - t1.tv_nsec));

    printf("  Recovery duration=%lf seconds.\n", timer);

    // This free should now work.
    clock_gettime(CLOCK_MONOTONIC, &t1);

    rc = dragon_heap_free(&heap, tmp2);

    clock_gettime(CLOCK_MONOTONIC, &t2);
    timer = 1e-9 * (double)(1000000000L * (t2.tv_sec - t1.tv_sec) +
                                   (t2.tv_nsec - t1.tv_nsec));

    printf("  Second free after recovery duration=%lf seconds.\n", timer);

    if (rc != DRAGON_SUCCESS) {
        sub_tests_failed++;
        printf("  A free of a pointer that should have caused a recover needed return code did not. Return code was %u\n",rc);
    }


    dragon_heap_get_stats(&heap, &stats);

    if (stats.utilization_pct != 0.0) {
        sub_tests_failed++;
        printf("  Utilization Percentage should be approximately 0.0. It was %lf", stats.utilization_pct);
    }

    for (int k=0;k<stats.num_block_sizes-1;k++) {
        if (stats.free_blocks[k].num_blocks != 0) {
            sub_tests_failed++;
            printf("  Expected number of free blocks for blocks size %lu to be 0. It was %lu.\n",stats.free_blocks[k].block_size, stats.free_blocks[k].num_blocks);
        }
    }

    if (stats.free_blocks[20].num_blocks != 1) {
        sub_tests_failed++;
        printf("  Expected number of free blocks for blocks size %lu to be 1. It was %lu.\n",stats.free_blocks[20].block_size, stats.free_blocks[20].num_blocks);
    }

    if (sub_tests_failed == 0)  {
        tests_passed++;
        printf("  Passed Test 6.\n");
    } else {
        printf("  Failed Test 6.\n");
    }

    sub_tests_failed = 0;

    dragon_heap_dump("Heap just before random mallocs.", &heap);

    printf("Test Case 7: Random Mallocs in a 4GB to 4K Heap\n");

    int number_valid_allocations = 0;
    uint64_t total_size = 0;

    clock_gettime(CLOCK_MONOTONIC, &t1);

    for (int i=0;i<malloc_tests_size;i++) {
        void* ptr;
        dragon_heap_malloc(&heap, values[i],&ptr);
        if (ptr != NULL) {
            total_size += values[i];
            allocations[number_valid_allocations] = ptr;
            number_valid_allocations++;
        } else {
            printf("  Allocation of %lu bytes could not be satisfied\n",values[i]);
        }
    }

    clock_gettime(CLOCK_MONOTONIC, &t2);
    timer = (1e-9 * (double)(1000000000L * (t2.tv_sec - t1.tv_sec) +
                                   (t2.tv_nsec - t1.tv_nsec))) / malloc_tests_size;

    printf("  Average malloc time over %lu calls duration=%lf seconds.\n", malloc_tests_size, timer);


    if (number_valid_allocations != 52) {
        sub_tests_failed++;
        printf("  There were 52 allocations that should have succeeded. Instead there were %u\n",number_valid_allocations);
    }

    for (int i=0;i<number_valid_allocations;i++) {
        for (int j=i+1;j<number_valid_allocations;j++) {
            if (allocations[i] == allocations[j]) {
                sub_tests_failed++;
                printf("  ERROR: SAME ALLOCATION FOR %lu and %lu\n",(uint64_t)allocations[i],(uint64_t)allocations[j]);
            }
        }
    }

    dragon_heap_get_stats(&heap, &stats);

    if (stats.utilization_pct < 99.387263 || stats.utilization_pct > 99.387265) {
        sub_tests_failed++;
        printf("The utilization percentage should be 99.38692. It was %lf\n",stats.utilization_pct);
    }

    rc = dragon_heap_free(&heap, (void*)123456);

    if (rc != DRAGON_DYNHEAP_INVALID_POINTER) {
        sub_tests_failed++;
        printf("Attempted free of invalid pointer did not return proper return code. RC=%u\n",rc);
    }

    clock_gettime(CLOCK_MONOTONIC, &t1);

    for (int i=0;i<number_valid_allocations;i++) {
        rc = dragon_heap_free(&heap, allocations[i]);
        if (rc != DRAGON_SUCCESS) {
            sub_tests_failed++;
            printf("Freeing allocation resulted in non-zero rc=%u\n",rc);
        }
    }

    clock_gettime(CLOCK_MONOTONIC, &t2);
    timer = (1e-9 * (double)(1000000000L * (t2.tv_sec - t1.tv_sec) +
                                   (t2.tv_nsec - t1.tv_nsec))) / malloc_tests_size;

    printf("  Average free time over %u calls duration=%lf seconds.\n", number_valid_allocations, timer);

    dragon_heap_get_stats(&heap, &stats);

    if (stats.utilization_pct != 0.0) {
        sub_tests_failed++;
        printf("  Utilization Percentage should be approximately 0.0. It was %lf\n", stats.utilization_pct);
    }

    for (int k=0;k<stats.num_block_sizes-1;k++) {
        if (stats.free_blocks[k].num_blocks != 0) {
            sub_tests_failed++;
            printf("  Expected number of free blocks for blocks size %lu to be 0. It was %lu.\n",stats.free_blocks[k].block_size, stats.free_blocks[k].num_blocks);
        }
    }

    if (stats.free_blocks[20].num_blocks != 1) {
        sub_tests_failed++;
        printf("  Expected number of free blocks for blocks size %lu to be 1. It was %lu.\n",stats.free_blocks[20].block_size, stats.free_blocks[20].num_blocks);
    }

    if (sub_tests_failed == 0)  {
        tests_passed++;
        printf("  Passed Test 7.\n");
    } else {
        printf("  Failed Test 7.\n");
    }

    dragon_heap_destroy(&heap);

    free(space);

    sub_tests_failed = 0;

    printf("Test Case 8: A Second Heap with 4GB to 32 byte blocks\n");

    dragon_heap_size(32,5,0,DRAGON_LOCK_FIFO, &heap_size);

    space = (dragonDynHeap_t*)malloc(heap_size);

    clock_gettime(CLOCK_MONOTONIC, &t1);

    rc = dragon_heap_init(space, &heap,32,5,0,DRAGON_LOCK_FIFO, NULL);

    clock_gettime(CLOCK_MONOTONIC, &t2);
    timer = 1e-9 * (double)(1000000000L * (t2.tv_sec - t1.tv_sec) +
                                   (t2.tv_nsec - t1.tv_nsec));

    printf("  dragon_dymmem_init duration=%lf seconds.\n", timer);

    if (rc != DRAGON_SUCCESS) {
        sub_tests_failed++;
        printf("  Failed to initialize heap with rc=%u\n",rc);
    }

    clock_gettime(CLOCK_MONOTONIC, &t1);

    rc = dragon_heap_malloc(&heap, 32, &tmp);

    clock_gettime(CLOCK_MONOTONIC, &t2);
    timer = 1e-9 * (double)(1000000000L * (t2.tv_sec - t1.tv_sec) +
                                   (t2.tv_nsec - t1.tv_nsec));

    printf("  dragon_dynmem_malloc duration for initial malloc is %lf seconds.\n", timer);
    printf("  This causes splits of the largest 4GB block all the way down to 32 byte blocks in powers of 2. So 26 splits touching many pages.\n");

    if (rc != DRAGON_SUCCESS) {
        sub_tests_failed++;
        printf("  Failed to allocate initial allocation with 32 bytes receiving rc=%u\n",rc);
    }

    number_valid_allocations = 0;
    total_size = 0;

    clock_gettime(CLOCK_MONOTONIC, &t1);

    for (int i=0;i<malloc_tests_size;i++) {
        void* ptr;
        dragon_heap_malloc(&heap, values[i],&ptr);
        if (ptr != NULL) {
            total_size += values[i];
            allocations[number_valid_allocations] = ptr;
            number_valid_allocations++;
        } else {
            printf("  Allocation of %lu bytes could not be satisfied\n",values[i]);
        }
    }

    clock_gettime(CLOCK_MONOTONIC, &t2);
    timer = (1e-9 * (double)(1000000000L * (t2.tv_sec - t1.tv_sec) +
                                   (t2.tv_nsec - t1.tv_nsec))) / malloc_tests_size;

    printf("  Average malloc time over %lu calls duration=%lf seconds.\n", malloc_tests_size, timer);


    if (number_valid_allocations != 52) {
        sub_tests_failed++;
        printf("  There were 52 allocations that should have succeeded. Instead there were %u\n",number_valid_allocations);
    }

    for (int i=0;i<number_valid_allocations;i++) {
        for (int j=i+1;j<number_valid_allocations;j++) {
            if (allocations[i] == allocations[j]) {
                sub_tests_failed++;
                printf("  ERROR: SAME ALLOCATION FOR %lu and %lu\n",(uint64_t)allocations[i],(uint64_t)allocations[j]);
            }
        }
    }

    dragon_heap_get_stats(&heap, &stats);

    clock_gettime(CLOCK_MONOTONIC, &t1);

    for (int i=0;i<number_valid_allocations;i++) {
        rc = dragon_heap_free(&heap, allocations[i]);
        if (rc != DRAGON_SUCCESS) {
            sub_tests_failed++;
            printf("Freeing allocation resulted in non-zero rc=%u\n",rc);
        }
    }

    clock_gettime(CLOCK_MONOTONIC, &t2);

    timer = (1e-9 * (double)(1000000000L * (t2.tv_sec - t1.tv_sec) +
                                   (t2.tv_nsec - t1.tv_nsec))) / number_valid_allocations;

    printf("  Average free time over %u calls duration=%lf seconds.\n", number_valid_allocations, timer);

    clock_gettime(CLOCK_MONOTONIC, &t1);

    rc = dragon_heap_free(&heap, tmp);

    clock_gettime(CLOCK_MONOTONIC, &t2);
    timer = 1e-9 * (double)(1000000000L * (t2.tv_sec - t1.tv_sec) +
                                   (t2.tv_nsec - t1.tv_nsec));

    //dragon_dynmem_dump("Heap after successful free of last allocated block.", &heap);

    printf("  dragon_dynmem_free duration for last free is %lf seconds.\n", timer);

    if (rc != DRAGON_SUCCESS) {
        sub_tests_failed++;
        printf("Freeing allocation resulted in non-zero rc=%u\n",rc);
    }

    dragon_heap_destroy(&heap);


    if (sub_tests_failed == 0)  {
        tests_passed++;
        printf("  Passed Test 8.\n");
    } else {
        printf("  Failed Test 8.\n");
    }

    sub_tests_failed = 0;

    printf("Test Case 9: Preallocate blocks in a heap.\n");

    //At this point we'll do some pre-allocations to test that code.
    //31 allocation sizes in array for powers of 2 from 5 to 31.
    //The 2^32 entry is not included, because that would be non-sensical
    //for a heap up to 2^32 block size since that would be just one big
    //block.
    size_t preallocated[] = {2,2,2,10,0,0,0,0,
                          0,0,0,0,0,0,0,0,
                          0,0,0,0,0,0,0,0,
                          0,0,0};

    rc = dragon_heap_init(space, &heap, 32, 5, 0, DRAGON_LOCK_FIFO, preallocated);

    //dragon_heap_dump("Heap after preallocated blocks.", &heap);

    void* ptr = NULL;

    rc = dragon_heap_malloc(&heap,1024,&ptr);

    if (rc != DRAGON_SUCCESS) {
        sub_tests_failed++;
        printf("Failed with %s and %s\n",dragon_get_rc_string(rc),dragon_getlasterrstr());
    }

    rc = dragon_heap_free(&heap, ptr);

    if (rc != DRAGON_SUCCESS) {
        sub_tests_failed++;
        printf("Failed with %s and %s\n",dragon_get_rc_string(rc),dragon_getlasterrstr());
    }

    dragon_heap_dump("Heap after preallocated blocks and one dynamic allocation.", &heap);

    for (int i=0; i<1000; i++) {
        void* allocs[10];
        for (int k=0;k<10;k++) {
            allocs[k] = 0UL;
            rc = dragon_heap_malloc(&heap,250,&allocs[k]);
            if (rc != DRAGON_SUCCESS) {
                sub_tests_failed++;
                printf("Failed with %s and %s\n",dragon_get_rc_string(rc),dragon_getlasterrstr());
            }
        }
        for (int k=0;k<10;k++) {
            rc = dragon_heap_free(&heap,allocs[k]);
            if (rc != DRAGON_SUCCESS) {
                sub_tests_failed++;
                printf("Failed with %s and %s\n",dragon_get_rc_string(rc),dragon_getlasterrstr());
            }
        }
    }

    dragon_heap_dump("Heap after 10 preallocated blocks malloced and freed.", &heap);

    number_valid_allocations = 0;

    for (int i=0;i<malloc_tests_size;i++) {
        void* ptr;
        rc = dragon_heap_malloc(&heap, values[i],&ptr);
        if (rc == DRAGON_SUCCESS && ptr != NULL) {
            total_size += values[i];
            allocations[number_valid_allocations] = ptr;
            number_valid_allocations++;
        } else {
            printf("  Allocation of %lu bytes could not be satisfied with rc=%s\n",values[i], dragon_get_rc_string(rc));
        }
    }

    for (int i=0;i<number_valid_allocations;i++) {
        for (int j=i+1;j<number_valid_allocations;j++) {
            if (allocations[i] == allocations[j]) {
                sub_tests_failed++;
                printf("  ERROR: SAME ALLOCATION FOR %lu and %lu\n",(uint64_t)allocations[i],(uint64_t)allocations[j]);
            }
        }
    }

    for (int i=0;i<number_valid_allocations;i++) {
        rc = dragon_heap_free(&heap, allocations[i]);
        if (rc != DRAGON_SUCCESS) {
            sub_tests_failed++;
            printf("Freeing allocation resulted in non-zero rc=%u\n",rc);
        }
    }

    //dragon_heap_dump("Heap after random allocations and freeing.", &heap);

    dragon_heap_get_stats(&heap, &stats);

    if (sub_tests_failed == 0) {
        tests_passed++;
        printf("Test Case 9 Passed.\n");
    } else {
        printf("Test Case 9 Failed.\n");
    }

    dragon_heap_destroy(&heap);

    free(space);

    return 0;
}
