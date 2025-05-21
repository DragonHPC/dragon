#include "../../src/dragon/launcher/include/_pmsgqueue.h"
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <stdbool.h>
#include <unistd.h>
#include <sys/wait.h>
#include <string.h>
#include <assert.h>

#include "../_ctest_utils.h"

#define TESTS_EXPECTED 5

int main(int argc, char* argv[]) {

    int tests_passed = 0;
    dragonError_t err;

    // create receive queue in parent first.
    dragonPMsgQueueHandle_t handle;
    dragon_pmsgqueue_init_handle(&handle);

    dragonError_t rc = dragon_pmsgqueue_create_recvq(&handle, "/klee_myq");
    assert(rc == DRAGON_SUCCESS);

    int pid = fork();

    if (pid==0) {
        // child
        dragonPMsgQueueHandle_t c_handle;
        err = dragon_pmsgqueue_init_handle(&c_handle);
        assert(err == DRAGON_SUCCESS);

        err = dragon_pmsgqueue_create_sendq(&c_handle, "/klee_myq");
        assert(err == DRAGON_SUCCESS);

        /* This message will be tossed by the reset below and
           the test program does not expect to see it then */
        err = dragon_pmsgqueue_send(&c_handle, "Bad Message");
        assert(err == DRAGON_SUCCESS);

        err = dragon_pmsgqueue_reset(&c_handle, 10);
        assert(err == DRAGON_SUCCESS);

        err = dragon_pmsgqueue_send(&c_handle, "Hello World");
        assert(err == DRAGON_SUCCESS);

        err = dragon_pmsgqueue_close(&c_handle, false);
        assert(err == DRAGON_SUCCESS);
        return SUCCESS;
    }

    char* msg;
    rc = dragon_pmsgqueue_reset(&handle, 10);
    assert(rc == DRAGON_SUCCESS);

    rc = dragon_pmsgqueue_recv(&handle, &msg, 0);
    assert(rc == DRAGON_SUCCESS);

    if (msg!=NULL && !strcmp(msg, "Hello World")) {
        printf("Test 1 Passed\n");
        tests_passed += 1;
    } else {
        printf("Test 1 Failed\n");
    }

    dragonPMsgQueueAttrs_t attrs;

    rc = dragon_pmsgqueue_stats(&handle, &attrs);

    if (rc == DRAGON_SUCCESS) {
        tests_passed += 1;
        printf("Test 2 Passed\n");
    } else {
        printf("Test 2 Failed with error code %d\n", rc);
        rc = dragon_pmsgqueue_get_last_err_msg(&handle, &msg);
        free(msg);

    }

    if (attrs.mq_curmsgs == 0) {
        tests_passed += 1;
        printf("Test 3 Passed\n");
    } else {
        printf("Test 3 Failed\n");
    }

    free(msg);
    waitpid(pid, NULL, 0);
    dragon_pmsgqueue_close(&handle, true);

    rc = dragon_pmsgqueue_create_recvq(&handle, "klee_myq");

    if (rc == DRAGON_PMSGQUEUE_INVALID_NAME) {
        tests_passed += 1;
        printf("Test 4 Passed\n");
    } else {
        printf("Test 4 Failed\n");
    }

    rc = dragon_pmsgqueue_get_last_err_msg(&handle, &msg);

    if (rc == DRAGON_SUCCESS) {
        tests_passed += 1;
        printf("Test 5 Passed with msg '%s'\n", msg);
        free(msg);
    } else {
        printf("Test 5 Failed\n");
    }

    printf("%d of %d tests passed.\n", tests_passed, TESTS_EXPECTED);

    dragon_pmsgqueue_close(&handle, true);

    TEST_STATUS = (tests_passed == TESTS_EXPECTED ? SUCCESS : FAILED);
    return TEST_STATUS;
}
