#include "../../src/lib/_blocks.h"
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <stdbool.h>
#include <inttypes.h>

//#include "../_ctest_utils.h"

#define TRUE 1
#define FALSE 0

int main(int argc, char* argv[]) {
    size_t size;
    dragonError_t err;
    size_t tests_passed = 0;
    size_t tests_attempted = 0;
    dragonBlocksStats_t stats;
    dragonBlocks_t blocks;
    dragonBlocks_t blocks2;
    uint64_t value;
    uint64_t id;
    uint64_t count;

    tests_attempted += 1;

    err = dragon_blocks_size(10, sizeof(uint64_t), &size);
    if (err != DRAGON_SUCCESS) {
        printf("Got error %s. Traceback:\n%s", dragon_get_rc_string(err), dragon_getlasterrstr());
        exit(0);
    }

    tests_passed += 1;
    tests_attempted += 1;

    if (size != 224) {
        printf("Size was %lu and should have been 224.\n", size);
        tests_passed -= 1;
    }

    tests_passed += 1;
    tests_attempted += 1;
    void* blocks_ptr = malloc(size);

    err = dragon_blocks_init(blocks_ptr, &blocks, 10, sizeof(uint64_t));
    if (err != DRAGON_SUCCESS) {
        printf("Got error %s. Traceback:\n%s", dragon_get_rc_string(err), dragon_getlasterrstr());
        exit(0);
    }

    tests_passed += 1;
    tests_attempted += 1;
    value = 42;

    err = dragon_blocks_alloc(&blocks, &value, &id);
    if (err != DRAGON_SUCCESS) {
        printf("Got error %s. Traceback:\n%s", dragon_get_rc_string(err), dragon_getlasterrstr());
        exit(0);
    }

    tests_passed += 1;
    tests_attempted += 1;
    value = 0;

    err = dragon_blocks_get(&blocks, id, &value);
    if (err != DRAGON_SUCCESS) {
        printf("Got error %s. Traceback:\n%s", dragon_get_rc_string(err), dragon_getlasterrstr());
        exit(0);
    }

    if (value != 42) {
        printf("Value was not 42. It was %" PRIu64 "\n", value);
        tests_passed -= 1;
    }

    tests_passed += 1;
    tests_attempted += 1;

    err = dragon_blocks_free(&blocks, id);
    if (err != DRAGON_SUCCESS) {
        printf("Got error %s. Traceback:\n%s", dragon_get_rc_string(err), dragon_getlasterrstr());
        exit(0);
    }

    tests_passed += 1;
    tests_attempted += 1;

    err = dragon_blocks_get(&blocks, id, &value);
    if (err != DRAGON_KEY_NOT_FOUND) {
        printf("Got error %s. Traceback:\n%s", dragon_get_rc_string(err), dragon_getlasterrstr());
        exit(0);
    }

    tests_passed += 1;
    tests_attempted += 1;
    uint64_t ids[10];

    for (int k=0; k<10; k++) {
        value = k;
        err = dragon_blocks_alloc(&blocks, &value, &ids[k]);
        if (err != DRAGON_SUCCESS) {
            printf("Got error %s. Traceback:\n%s", dragon_get_rc_string(err), dragon_getlasterrstr());
            exit(0);
        }
    }

    tests_passed += 1;
    tests_attempted += 1;

    err = dragon_blocks_dump("Blocks Data Structure\n==========================", &blocks, "");
    if (err != DRAGON_SUCCESS) {
        printf("Got error %s. Traceback:\n%s", dragon_get_rc_string(err), dragon_getlasterrstr());
        exit(0);
    }

    tests_passed += 1;
    tests_attempted += 1;

    for (int k=0; k<10; k++) {
        err = dragon_blocks_get(&blocks, ids[k], &value);
        if (err != DRAGON_SUCCESS) {
            printf("Got error %s. Traceback:\n%s", dragon_get_rc_string(err), dragon_getlasterrstr());
            exit(0);
        }

        if (value != k) {
            printf("The value was %" PRIu64 " and should have been %d\n", value, k);
            exit(0);
        }
    }

    tests_passed += 1;
    tests_attempted += 1;

    err = dragon_blocks_first(&blocks, NULL, 0, 0, &id);
    if (err != DRAGON_SUCCESS) {
        printf("Got error %s. Traceback:\n%s", dragon_get_rc_string(err), dragon_getlasterrstr());
        exit(0);
    }

    int iters = 0;

    while (err == DRAGON_SUCCESS) {
        iters += 1;
        err = dragon_blocks_next(&blocks, NULL, 0, 0, &id);
    }

    if (err != DRAGON_BLOCKS_ITERATION_COMPLETE) {
        printf("Expected DRAGON_BLOCKS_ITERATION_COMPLETE. Got error %s. Traceback:\n%s", dragon_get_rc_string(err), dragon_getlasterrstr());
        exit(0);
    }

    tests_passed += 1;
    tests_attempted += 1;

    if (iters != 10) {
        printf("There was an error. Iters should be 10. It was %d\n", iters);
        tests_passed -= 1;
    }

    tests_passed += 1;
    tests_attempted += 1;
    count = 0;

    err = dragon_blocks_count(&blocks, NULL, 0, 0, &count);
    if (err != DRAGON_SUCCESS) {
        printf("Got error %s. Traceback:\n%s", dragon_get_rc_string(err), dragon_getlasterrstr());
        exit(0);
    }

    if (count != 10) {
        printf("Expected a count of 10. Got %" PRIu64 ".\n", count);
        tests_passed -= 1;
    }

    tests_passed += 1;
    tests_attempted += 1;
    value = 0;

    err = dragon_blocks_alloc(&blocks, &value, &id);
    if (err != DRAGON_OUT_OF_SPACE) {
        printf("Got error %s. Traceback:\n%s", dragon_get_rc_string(err), dragon_getlasterrstr());
        exit(0);
    }

    tests_passed += 1;
    tests_attempted += 1;

    err = dragon_blocks_stats(&blocks, &stats);
    if (err != DRAGON_SUCCESS) {
        printf("Got error %s. Traceback:\n%s", dragon_get_rc_string(err), dragon_getlasterrstr());
        exit(0);
    }

    if (stats.current_count != 10) {
        printf("Stats count is not 10. It is %" PRIu64 ".\n", stats.current_count);
        tests_passed -= 1;
    }

    if (stats.num_blocks != 10) {
        printf("Stats num_blocks is not 10. It is %" PRIu64 ".\n", stats.num_blocks);
        tests_passed -= 1;
    }

    if (stats.current_count != 10) {
        printf("Stats current_count is not 10. It is %" PRIu64 ".\n", stats.current_count);
        tests_passed -= 1;
    }

    if (stats.max_count != 10) {
        printf("Stats max_count is not 10. It is %" PRIu64 ".\n", stats.max_count);
        tests_passed -= 1;
    }

    tests_passed += 1;
    tests_attempted += 1;

    for (int k=0; k<10; k++) {
        value = k;
        err = dragon_blocks_free(&blocks, ids[k]);
        if (err != DRAGON_SUCCESS) {
            printf("Got error %s. Traceback:\n%s", dragon_get_rc_string(err), dragon_getlasterrstr());
            exit(0);
        }
    }

    tests_passed += 1;
    tests_attempted += 1;

    for (int k=0; k<5; k++) {
        value = k;
        err = dragon_blocks_alloc(&blocks, &value, &ids[k]);
        if (err != DRAGON_SUCCESS) {
            printf("Got error %s. Traceback:\n%s", dragon_get_rc_string(err), dragon_getlasterrstr());
            exit(0);
        }

        value = 42;
        err = dragon_blocks_alloc(&blocks, &value, &ids[k]);
        if (err != DRAGON_SUCCESS) {
            printf("Got error %s. Traceback:\n%s", dragon_get_rc_string(err), dragon_getlasterrstr());
            exit(0);
        }
    }

    tests_passed += 1;
    tests_attempted += 1;

    err = dragon_blocks_first(&blocks, &value, 0, sizeof(uint64_t), &id);
    if (err != DRAGON_SUCCESS) {
        printf("Got error %s. Traceback:\n%s", dragon_get_rc_string(err), dragon_getlasterrstr());
        exit(0);
    }

    iters = 0;

    while (err == DRAGON_SUCCESS) {
        iters += 1;
        err = dragon_blocks_next(&blocks, &value, 0, sizeof(uint64_t), &id);
    }

    if (err != DRAGON_BLOCKS_ITERATION_COMPLETE) {
        printf("Expected DRAGON_BLOCKS_ITERATION_COMPLETE. Got error %s. Traceback:\n%s", dragon_get_rc_string(err), dragon_getlasterrstr());
        exit(0);
    }

    tests_passed += 1;
    tests_attempted += 1;

    if (iters != 5) {
        printf("There was an error. Iters should be 5. It was %d\n", iters);
        tests_passed -= 1;
    }

    tests_passed += 1;
    tests_attempted += 1;
    count = 0;

    err = dragon_blocks_count(&blocks, &value, 0, sizeof(uint64_t), &count);
    if (err != DRAGON_SUCCESS) {
        printf("Got error %s. Traceback:\n%s", dragon_get_rc_string(err), dragon_getlasterrstr());
        exit(0);
    }

    if (count != 5) {
        printf("Expected a count of 5. Got %" PRIu64 ".\n", count);
        tests_passed -= 1;
    }

    tests_passed += 1;
    tests_attempted += 1;

    err = dragon_blocks_attach(blocks_ptr, &blocks2);
    if (err != DRAGON_SUCCESS) {
        printf("Got error %s. Traceback:\n%s", dragon_get_rc_string(err), dragon_getlasterrstr());
        exit(0);
    }

    tests_passed += 1;
    tests_attempted += 1;
    count = 0;

    err = dragon_blocks_count(&blocks2, &value, 0, sizeof(uint64_t), &count);
    if (err != DRAGON_SUCCESS) {
        printf("Got error %s. Traceback:\n%s", dragon_get_rc_string(err), dragon_getlasterrstr());
        exit(0);
    }

    if (count != 5) {
        printf("Expected a count of 5. Got %" PRIu64 ".\n", count);
        tests_passed -= 1;
    }

    tests_passed += 1;
    tests_attempted += 1;

    err = dragon_blocks_detach(&blocks2);
    if (err != DRAGON_SUCCESS) {
        printf("Got error %s. Traceback:\n%s", dragon_get_rc_string(err), dragon_getlasterrstr());
        exit(0);
    }

    tests_passed += 1;
    tests_attempted += 1;

    err = dragon_blocks_dump("Blocks Data Structure At End\n============================", &blocks, "");
    if (err != DRAGON_SUCCESS) {
        printf("Got error %s. Traceback:\n%s", dragon_get_rc_string(err), dragon_getlasterrstr());
        exit(0);
    }

    tests_attempted += 1;
    tests_passed += 1;

    err = dragon_blocks_destroy(&blocks);
    if (err != DRAGON_SUCCESS) {
        printf("Got error %s. Traceback:\n%s", dragon_get_rc_string(err), dragon_getlasterrstr());
        exit(0);
    }

    tests_passed += 1;
    free(blocks_ptr);

    printf("Tests complete with %lu tests attempted and %lu tests passed!!!\n", tests_attempted, tests_passed);
    if (tests_attempted == tests_passed)
        printf("All Tests Passed!!!\n");

    return 0;
}



