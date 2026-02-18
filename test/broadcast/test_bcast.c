#include <dragon/bcast.h>
#include <dragon/return_codes.h>
#include <dragon/utils.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <unistd.h>
#include <signal.h>
#include <pthread.h>

#define SERFILE "bcast_serialized.dat"
#define MFILE "bcast_test"
#define M_UID 0

#define idle_waiters_count 5
#define spin_waiters_count 5
#define adaptive_waiters_count 5
#define waiters_count (idle_waiters_count+spin_waiters_count)
#define exited_due_to_destroy 11

#include "../_ctest_utils.h"


/* for starting threads */
typedef struct thread_arg_st {
    dragonBCastDescr_t* bd;
    size_t expected_payload_sz;
    timespec_t* timeout;
} thread_arg_t;


void
fill_payload(char payload[], size_t payload_sz)
{
    for (size_t i = 0; i < payload_sz; i++)
        payload[i] = i % 128;
}

bool
payload_ok(char payload[], size_t payload_sz)
{
    for (size_t i = 0; i < payload_sz; i++) {
        unsigned int x = payload[i];
        if (i%128 != x) {
            printf("The value at %lu was %u\n",i,x);
            return false;
        }
    }

    return true;
}

int
proc_waiter(dragonBCastDescr_t* bd, dragonWaitMode_t wait_mode, size_t expected_payload_sz, const timespec_t* timeout)
{

    dragonError_t err;
    char* payload;
    size_t payload_sz;

    err = dragon_bcast_wait(bd, wait_mode, timeout, (void**)&payload, &payload_sz, NULL, NULL);

    if (err == DRAGON_TIMEOUT)
        exit(err);

    if (err == DRAGON_OBJECT_DESTROYED)
        exit(err);

    if (err != DRAGON_SUCCESS)
        err_fail(err, "Error on dragon_bcast_wait");

    if (payload_sz != expected_payload_sz) {
        printf("Payload size in waiter did not match expected payload size.\n");
        printf("expected=%lu and actual=%lu\n", expected_payload_sz, payload_sz);
        return FAILED;
    }

    if (!payload_ok(payload, payload_sz)) {
        printf("Error on payload verification\n");
        return FAILED;
    }

    if (err == DRAGON_SUCCESS && payload_sz > 0)
        free(payload);

    return SUCCESS;
}

int
proc_waiter_n(dragonBCastDescr_t* bd, dragonWaitMode_t wait_mode, size_t expected_payload_sz, const timespec_t* timeout, int repeats)
{

    dragonError_t err;
    char* payload;
    size_t payload_sz;

    for (int k=0;k<repeats;k++) {
        err = dragon_bcast_wait(bd, wait_mode, timeout, (void**)&payload, &payload_sz, NULL, NULL);

        if (err == DRAGON_TIMEOUT)
            exit(err);

        if (err != DRAGON_SUCCESS)
            err_fail(err, "Error on dragon_bcast_wait");

        if (payload_sz != expected_payload_sz) {
            printf("Payload size in waiter did not match expected payload size.\n");
            printf("expected=%lu and actual=%lu\n", expected_payload_sz, payload_sz);
            return FAILED;
        }

        if (!payload_ok(payload, payload_sz)) {
            printf("Error on payload verification\n");
            return FAILED;
        } else {
            // printf("Got payload %d in proc_waiter_n\n", k);
            // fflush(stdout);
        }

        if (err == DRAGON_SUCCESS && payload_sz > 0)
            free(payload);
    }

    return SUCCESS;
}

void * global_payload;
size_t global_payload_sz;
dragonError_t global_derr;
char* global_err_str;

void trigger_hdler(int sig) {
    signal(sig, SIG_IGN);
    signal(SIGUSR1, SIG_DFL);

    if (!payload_ok(global_payload, global_payload_sz))
        printf("Error on payload verification in callback\n");

    if (global_payload_sz > 0)
        free(global_payload);
}

void
callback(void* ptr, void* payload, size_t payload_sz, dragonError_t derr, char* err_str)
{
    if (!payload_ok(payload, payload_sz)) {
        printf("Error on payload verification in callback\n");
    }

    if (payload_sz > 0)
        free(payload);
}

int
create_pool(dragonMemoryPoolDescr_t* mpool)
{
    /* Create a memory pool to allocate messages and a Channel out of */
    size_t mem_size = 1UL<<31;
    //printf("Allocating pool of size %lu bytes.\n", mem_size);

    char * fname = util_salt_filename(MFILE);
    dragonError_t derr = dragon_memory_pool_create(mpool, mem_size, fname, M_UID, NULL);
    if (derr != DRAGON_SUCCESS)
        err_fail(derr, "Failed to create the memory pool");

    free(fname);

    return SUCCESS;
}

void
check_result(dragonError_t err, dragonError_t expected_err, int* tests_passed, int* tests_attempted)
{
    (*tests_attempted)++;

    if (err != expected_err) {
        printf("Test %d Failed with error code %s\n", *tests_attempted, dragon_get_rc_string(err));
        printf("%s\n", dragon_getlasterrstr());
    }
    else
        (*tests_passed)++;
}

static void*
thread_start(void * ptr)
{
    thread_arg_t * arg = (thread_arg_t*) ptr;

    proc_waiter(arg->bd, DRAGON_IDLE_WAIT, arg->expected_payload_sz, arg->timeout);

    return NULL;
}

int
main(int argc, char* argv[])
{
    int tests_passed = 0;
    int tests_attempted = 0;
    dragonMemoryPoolDescr_t pool;
    char payload[512];

    fill_payload(payload, 512);

    if (create_pool(&pool) != SUCCESS) {
        printf("Could not create memory pool for bcast tests.\n");
        return FAILED;
    }

    dragonBCastDescr_t bd, bd2;
    dragonBCastSerial_t bd_ser;
    dragonError_t err;
    int status;

    // create in memory pool
    err = dragon_bcast_create(&pool, 128, 10, NULL, &bd);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    // destroy the bcast object
    err = dragon_bcast_destroy(&bd);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    // destroy already destroyed bcast object
    err = dragon_bcast_destroy(&bd);
    check_result(err, DRAGON_MAP_KEY_NOT_FOUND, &tests_passed, &tests_attempted);

    size_t sz;
    err = dragon_bcast_size(256, 10, NULL, &sz);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    tests_attempted++;

    if (sz < 256)
        printf("Test %d Failed. The required size was too small.\n", tests_attempted);
    else
        tests_passed++;

    void* ptr = calloc(1, sz);

    err = dragon_bcast_create_at(ptr, sz, 256, 10, NULL, &bd);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = dragon_bcast_serialize(&bd, &bd_ser);
    check_result(err, DRAGON_BCAST_NOT_SERIALIZABLE, &tests_passed, &tests_attempted);

    err = dragon_bcast_destroy(&bd);
    free(ptr);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    // create in memory pool
    err = dragon_bcast_create(&pool, 1024, waiters_count, NULL, &bd);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    /***************** Idle Wait Test ********************/

    int k;

    for (k=0;k<idle_waiters_count;k++) {
        if (fork()==0) {
            return proc_waiter(&bd, DRAGON_IDLE_WAIT, 512, NULL);
        }
    }

    int num_waiters;

    int count;

    err = dragon_bcast_num_waiting(&bd, &count);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    while (count >= 0 && count < idle_waiters_count) {
        sleep(0.01);
        dragon_bcast_num_waiting(&bd, &count); // don't check error code here. Leads to non-deterministic test case count.
    }

    err = dragon_bcast_trigger_all(&bd, NULL, payload, 512);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    for (k=0;k<idle_waiters_count;k++) {
        wait(&status);
        if (status!=0) {
            printf("There was an error on an idle waiter being triggered.\n");
        } else {
            tests_passed += 1;
        }
        tests_attempted += 1;
    }

    /******************* End Idle Wait Test *****************/


    /***************** Spin Wait Test ********************/

    for (k=0;k<spin_waiters_count;k++) {
        if (fork()==0) {
            return proc_waiter(&bd, DRAGON_SPIN_WAIT, 512, NULL);
        }
    }

    err = dragon_bcast_num_waiting(&bd, &count);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    while (count >= 0 && count < spin_waiters_count) {
        sleep(0.01);
        dragon_bcast_num_waiting(&bd, &count); // don't check error code here. Leads to non-deterministic test case count.
    }

    err = dragon_bcast_trigger_all(&bd, NULL, payload, 512);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    for (k=0;k<spin_waiters_count;k++) {
        wait(&status);
        if (status!=0) {
            printf("There was an error on an idle waiter being triggered.\n");
        } else {
            tests_passed += 1;
        }
        tests_attempted += 1;
    }

    /* This test is to see that spin waiting reverts to idle waiting when
       the number of allowable spin waiters is exceeded. */

    for (k=0;k<waiters_count*2; k++) {
        if (fork()==0)
            return proc_waiter(&bd, DRAGON_SPIN_WAIT, 512, NULL);
    }

    err = dragon_bcast_num_waiting(&bd, &count);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    while (count >= 0 && count < waiters_count*2) {
        sleep(0.01);
        dragon_bcast_num_waiting(&bd, &count); // don't check error code here. Leads to non-deterministic test case count.
    }

    err = dragon_bcast_trigger_all(&bd, NULL, payload, 512);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    for (k=0;k<waiters_count*2;k++) {
        wait(&status);
        if (status!=0) {
            printf("There was an error on an idle waiter being triggered.\n");
        } else {
            tests_passed += 1;
        }
        tests_attempted += 1;
    }
    /******************* End Spin Wait Test *****************/

    /***************** Adaptive Wait Test ********************/

    for (k=0;k<adaptive_waiters_count;k++) {
        if (fork()==0) {
            return proc_waiter(&bd, DRAGON_ADAPTIVE_WAIT, 512, NULL);
        }
    }

    err = dragon_bcast_num_waiting(&bd, &count);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    while (count >= 0 && count < adaptive_waiters_count) {
        sleep(0.1);
        dragon_bcast_num_waiting(&bd, &count); // don't check error code here. Leads to non-deterministic test case count.
    }

    err = dragon_bcast_trigger_all(&bd, NULL, payload, 512);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    for (k=0;k<adaptive_waiters_count;k++) {
        wait(&status);
        if (status!=0) {
            printf("There was an error on an idle waiter being triggered.\n");
        } else {
            tests_passed += 1;
        }
        tests_attempted += 1;
    }

    /* This test is to see that adaptive waiting reverts to idle waiting and that
       the path through the code for adaptive waiting works correctly. */

    for (k=0;k<waiters_count*2; k++) {
        if (fork()==0)
            return proc_waiter(&bd, DRAGON_ADAPTIVE_WAIT, 512, NULL);
    }

    err = dragon_bcast_num_waiting(&bd, &count);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    while (count >= 0 && count < waiters_count*2) {
        sleep(0.1);
        dragon_bcast_num_waiting(&bd, &count); // don't check error code here. Leads to non-deterministic test case count.
    }

    err = dragon_bcast_trigger_all(&bd, NULL, payload, 512);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    for (k=0;k<waiters_count*2;k++) {
        wait(&status);
        if (status!=0) {
            printf("There was an error on an idle waiter being triggered.\n");
        } else {
            tests_passed += 1;
        }
        tests_attempted += 1;
    }

    /******************* End Adaptive Wait Test *****************/

    /***************** Spin+Idle Wait Test ********************/

    for (k=0;k<spin_waiters_count;k++) {
        if (fork()==0) {
            return proc_waiter(&bd, DRAGON_SPIN_WAIT, 512, NULL);
        }
    }

    for (k=0;k<idle_waiters_count;k++) {
        if (fork()==0) {
            return proc_waiter(&bd, DRAGON_IDLE_WAIT, 512, NULL);
        }
    }

    err = dragon_bcast_num_waiting(&bd, &count);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    while (count >= 0 && count < idle_waiters_count+spin_waiters_count) {
        sleep(0.01);
        dragon_bcast_num_waiting(&bd, &count); // don't check error code here. Leads to non-deterministic test case count.
    }

    err = dragon_bcast_trigger_all(&bd, NULL, payload, 512);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    for (k=0;k<idle_waiters_count+spin_waiters_count;k++) {
        wait(&status);
        if (status!=0) {
            printf("There was an error on an idle waiter being triggered.\n");
        } else {
            tests_passed += 1;
        }
        tests_attempted += 1;
    }

    /******************* End Spin+Idle Wait Test *****************/

    err = dragon_bcast_serialize(&bd, &bd_ser);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = dragon_bcast_attach(&bd_ser, &bd2);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    /***************** Spin+Idle Wait Test w/trigger_one on attached object ********************/

    for (k=0;k<spin_waiters_count;k++) {
        if (fork()==0) {
            return proc_waiter(&bd2, DRAGON_SPIN_WAIT, 512, NULL);
        }
    }

    for (k=0;k<idle_waiters_count;k++) {
        if (fork()==0) {
            return proc_waiter(&bd2, DRAGON_IDLE_WAIT, 512, NULL);
        }
    }

    err = dragon_bcast_num_waiting(&bd, &count);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    while (count >= 0 && count < idle_waiters_count+spin_waiters_count) {
        sleep(0.01);
        dragon_bcast_num_waiting(&bd, &count); // don't check error code here. Leads to non-deterministic test case count.
    }

    for (k=0;k<idle_waiters_count+spin_waiters_count;k++) {
        sleep(0.1);
        err = dragon_bcast_trigger_one(&bd, NULL, payload, 512);
        sleep(0.1);
        check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);
    }


    for (k=0;k<idle_waiters_count+spin_waiters_count;k++) {
        wait(&status);
        if (status!=0) {
            printf("There was an error on an idle waiter being triggered.\n");
        } else {
            tests_passed += 1;
        }
        tests_attempted += 1;
    }

    err = dragon_bcast_num_waiting(&bd, &count);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    if (count == 0) {
        tests_passed += 1;
    } else {
        printf("There should have been 0 waiters at this point, and there were %d\n", count);
    }
    tests_attempted += 1;

    /******************* End Spin+Idle Wait Test w/trigger_one on attached object *****************/

    /***************** Spin+Idle Wait Test w/trigger_one********************/

    for (k=0;k<spin_waiters_count;k++) {
        if (fork()==0) {
            return proc_waiter(&bd, DRAGON_SPIN_WAIT, 512, NULL);
        }
    }

    for (k=0;k<idle_waiters_count;k++) {
        if (fork()==0) {
            return proc_waiter(&bd, DRAGON_IDLE_WAIT, 512, NULL);
        }
    }

    err = dragon_bcast_num_waiting(&bd, &count);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    while (count >= 0 && count < idle_waiters_count+spin_waiters_count) {
        sleep(0.01);
        dragon_bcast_num_waiting(&bd, &count); // don't check error code here. Leads to non-deterministic test case count.
    }

    for (k=0;k<idle_waiters_count+spin_waiters_count;k++) {
        sleep(0.1);
        err = dragon_bcast_trigger_one(&bd, NULL, payload, 512);
        sleep(0.1);
        check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);
    }


    for (k=0;k<idle_waiters_count+spin_waiters_count;k++) {
        wait(&status);
        if (status!=0) {
            printf("There was an error on an idle waiter being triggered.\n");
        } else {
            tests_passed += 1;
        }
        tests_attempted += 1;
    }

    err = dragon_bcast_num_waiting(&bd, &count);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    if (count == 0) {
        tests_passed += 1;
    } else {
        printf("There should have been 0 waiters at this point, and there were %d\n", count);
    }
    tests_attempted += 1;

    /******************* End Spin+Idle Wait Test w/trigger_one *****************/

    /******************* Test Timeout of idle wait ******************/
    timespec_t start, end;
    clock_gettime(CLOCK_MONOTONIC, &start);
    timespec_t low = {0,400000000};

    if (fork() == 0) {
        timespec_t timeout;
        timeout.tv_sec = 0;
        timeout.tv_nsec = 400000000;
        return proc_waiter(&bd, DRAGON_IDLE_WAIT, 512, &timeout);
    }

    wait(&status);

    clock_gettime(CLOCK_MONOTONIC, &end);

    timespec_t diff;

    dragon_timespec_diff(&diff, &end, &start);

    if (dragon_timespec_le(&low, &diff)) {
        tests_passed += 1;
    } else {
        printf("timeout of idle wait was outside of bounds\n");
    }

    tests_attempted+=1;

    /***************** End of idle wait timeout test ************************/

    /******************* Test Timeout of spin wait ******************/

    err = dragon_bcast_num_waiting(&bd, &count);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);
    if (count != 0) {
        printf("The number of waiting should be 0, it was not\n");
        exit(1);
    }

    clock_gettime(CLOCK_MONOTONIC, &start);

    /* It is unnecessary to reset it. It is here just to test the reset API call */
    err = dragon_bcast_reset(&bd);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    if (fork() == 0) {
        timespec_t timeout;
        timeout.tv_sec = 0;
        timeout.tv_nsec = 400000000;
        return proc_waiter(&bd, DRAGON_SPIN_WAIT, 512, &timeout);
    }

    wait(&status);

    clock_gettime(CLOCK_MONOTONIC, &end);

    dragon_timespec_diff(&diff, &end, &start);

    if (dragon_timespec_le(&low, &diff)) {
        tests_passed += 1;
    } else {
        printf("timeout of spin wait was outside of bounds\n");
    }

    err = dragon_bcast_num_waiting(&bd, &count);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);
    if (count != 0) {
        printf("The number of waiting should be 0, it was not\n");
        exit(1);
    }

    tests_attempted+=1;

    /***************** End of spin wait timeout test ************************/

    /******************* Test Timeout of idle wait ******************/

    timespec_t low2 = {1,500000000};

    clock_gettime(CLOCK_MONOTONIC, &start);

    if (fork() == 0) {
        timespec_t timeout;
        timeout.tv_sec = 1;
        timeout.tv_nsec = 500000000;
        return proc_waiter(&bd, DRAGON_IDLE_WAIT, 512, &timeout);
    }

    wait(&status);

    clock_gettime(CLOCK_MONOTONIC, &end);

    dragon_timespec_diff(&diff, &end, &start);

    if (dragon_timespec_le(&low2, &diff)) {
        tests_passed += 1;
    } else {
        printf("timeout of idle wait was outside of bounds\n");
    }

    tests_attempted+=1;

    /***************** End of idle wait timeout test ************************/

    /******************* Test Timeout of spin wait ******************/

    clock_gettime(CLOCK_MONOTONIC, &start);

    if (fork() == 0) {
        timespec_t timeout;
        timeout.tv_sec = 1;
        timeout.tv_nsec = 500000000;
        return proc_waiter(&bd, DRAGON_SPIN_WAIT, 512, &timeout);
    }

    wait(&status);

    clock_gettime(CLOCK_MONOTONIC, &end);

    dragon_timespec_diff(&diff, &end, &start);

    if (dragon_timespec_le(&low2, &diff)) {
        tests_passed += 1;
    } else {
        printf("timeout of idle wait was outside of bounds\n");
    }

    tests_attempted+=1;

    /***************** End of spin wait timeout test ************************/

    /***************** Beginning of async callback test ************************/

    tests_attempted += 1;

    err = dragon_bcast_num_waiting(&bd, &num_waiters);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = dragon_bcast_notify_callback(&bd, &bd, DRAGON_IDLE_WAIT, NULL, NULL, NULL, callback);

    if (err != DRAGON_SUCCESS)
        printf("Error on async wait with EC=%s\n", dragon_get_rc_string(err));
    else
        tests_passed += 1;

    tests_attempted += 1;

    err = dragon_bcast_num_waiting(&bd, &num_waiters);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    while (num_waiters == 0) {
        dragon_bcast_num_waiting(&bd, &num_waiters); // don't check error code here. Leads to non-deterministic test case count.
    }

    err = dragon_bcast_trigger_all(&bd, NULL, payload, 512);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = dragon_bcast_num_waiting(&bd, &num_waiters);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    while (num_waiters == 1) {
        dragon_bcast_num_waiting(&bd, &num_waiters); // don't check error code here. Leads to non-deterministic test case count.
    }

    /* getting here means that we made it through an asynchronous
       callback and the num_waiters is again 0. */
    if (num_waiters == 0)
        tests_passed += 1;

    /***************** End of async callback test ************************/

    /***************** Beginning of async signal test ************************/

    tests_attempted += 1;

    signal(SIGUSR1, trigger_hdler);

    err = dragon_bcast_notify_signal(&bd, DRAGON_SPIN_WAIT, NULL, NULL, NULL, SIGUSR1, &global_payload, &global_payload_sz, &global_derr, &global_err_str);

    if (err != DRAGON_SUCCESS)
        printf("Error on async wait with EC=%s\n", dragon_get_rc_string(err));
    else
        tests_passed += 1;

    tests_attempted += 1;

    err = dragon_bcast_num_waiting(&bd, &num_waiters);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);
    while (num_waiters == 0) {
        dragon_bcast_num_waiting(&bd, &num_waiters); // don't check error code here. Leads to non-deterministic test case count.
    }

    err = dragon_bcast_trigger_all(&bd, NULL, payload, 512);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = dragon_bcast_num_waiting(&bd, &num_waiters);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    while (num_waiters == 1) {
        dragon_bcast_num_waiting(&bd, &num_waiters); // don't check error code here. Leads to non-deterministic test case count.

    }

    /* getting here means that we made it through an asynchronous
       callback and the num_waiters is again 0. */
    if (num_waiters == 0)
        tests_passed += 1;

    err = dragon_bcast_destroy(&bd);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    /***************** End of async signal test ************************/

    /******************* Test Synchronized BCast *******************/
    dragonBCastAttr_t attrs;
    err = dragon_bcast_attr_init(&attrs);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);
    attrs.sync_num = 3;
    attrs.sync_type = DRAGON_SYNC;
    int repeat_test = 5;

    err = dragon_bcast_create(&pool, 1024, 10, &attrs, &bd);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    for (k=0;k<attrs.sync_num;k++) {
        if (fork()==0) {
            return proc_waiter_n(&bd, DRAGON_IDLE_WAIT, 512, NULL, repeat_test);
        }
    }

    // printf("The following prints should alternate with a trigger print followed by %lu waiter messages.\n",attrs.sync_num);
    // printf("The pattern should repeat %d times\n", repeat_test);

    for (k=0;k<repeat_test;k++) {
        err = dragon_bcast_trigger_all(&bd, NULL, payload, 512);
        check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);
    }

    for (k=0;k<attrs.sync_num;k++) {
        wait(&status);
        if (status!=0) {
            printf("There was an error on a waiter being triggered.\n");
        } else {
            tests_passed += 1;
        }
        tests_attempted += 1;
    }

    /* It is unnecessary to reset it. It is here just to test the reset API call */
    err = dragon_bcast_reset(&bd);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    /* Now do it with spin waiters */
    for (k=0;k<attrs.sync_num;k++) {
        if (fork()==0) {
            return proc_waiter_n(&bd, DRAGON_SPIN_WAIT, 512, NULL, repeat_test);
        }
    }

    // printf("The same test is repeated with SPIN waiters instead of IDLE waiters.\n");
    // printf("The following prints should alternate with a trigger print followed by %lu waiter messages.\n",attrs.sync_num);
    // printf("The pattern should repeat %d times\n", repeat_test);

    for (k=0;k<repeat_test;k++) {
        err = dragon_bcast_trigger_all(&bd, NULL, payload, 512);
        check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);
    }

    for (k=0;k<attrs.sync_num;k++) {
        wait(&status);
        if (status!=0) {
            printf("There was an error on a waiter being triggered.\n");
        } else {
            tests_passed += 1;
        }
        tests_attempted += 1;
    }
    /******************* End of Test Synchronized BCast *******************/

    /******************* Test Destroy with Waiters ******************/

    err = dragon_bcast_create(&pool, 1024, 10, NULL, &bd);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    clock_gettime(CLOCK_MONOTONIC, &start);

    if (fork() == 0) {
        return proc_waiter(&bd, DRAGON_IDLE_WAIT, 512, NULL);
    }

    err = dragon_bcast_num_waiting(&bd, &num_waiters);
    while (num_waiters == 0) {
        err = dragon_bcast_num_waiting(&bd, &num_waiters);
    }

    /* testing a degenerative case */
    err = dragon_bcast_trigger_some(&bd, 0, NULL, NULL, 0);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = dragon_bcast_destroy(&bd);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    wait(&status);
    check_result(WEXITSTATUS(status), DRAGON_OBJECT_DESTROYED, &tests_passed, &tests_attempted);

    clock_gettime(CLOCK_MONOTONIC, &end);

    dragon_timespec_diff(&diff, &end, &start);

    if (dragon_timespec_le(&diff, &low2)) {
        tests_passed += 1;
    } else {
        printf("The destroy of the bcast seemingly took too long when there was a waiter.\n");
    }

    tests_attempted+=1;

    /***************** End of Test Destroy with Waiters test ************************/

    err = dragon_bcast_create(&pool, 1024, 10, NULL, &bd);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = dragon_bcast_serialize(&bd, &bd_ser);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = dragon_bcast_attach(&bd_ser, &bd2);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = dragon_bcast_detach(&bd2);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    // This won't succeed because detaching the same BCast object just prior
    // to this call, removes it from the umap. You can't detach and destroy
    // the same BCast object.

    err = dragon_bcast_destroy(&bd);
    check_result(err, DRAGON_MAP_KEY_NOT_FOUND, &tests_passed, &tests_attempted);

    err = dragon_bcast_attach(&bd_ser, &bd2);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = dragon_bcast_serial_free(&bd_ser);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = dragon_bcast_destroy(&bd2);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    err = dragon_memory_pool_destroy(&pool);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    /***************** Idle Wait Test with create_at ********************/

    err = dragon_bcast_size(256, 10, NULL, &sz);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    tests_attempted++;

    if (sz < 256)
        printf("Test %d Failed. The required size was too small.\n", tests_attempted);
    else
        tests_passed++;

    ptr = calloc(1, sz);

    pthread_attr_t attr;
    pthread_t threads[idle_waiters_count];

    pthread_attr_init(&attr);

    thread_arg_t* arg;

    err = dragon_bcast_create_at(ptr, sz, 256, 10, NULL, &bd);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    for (k=0;k<idle_waiters_count;k++) {
        arg = calloc(1, sizeof(thread_arg_t));
        arg->bd = &bd;
        arg->expected_payload_sz = 256;
        arg->timeout = NULL;
        err = pthread_create(&threads[k], &attr, thread_start, arg);
    }

    pthread_attr_destroy(&attr);

    err = dragon_bcast_num_waiting(&bd, &count);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    while (count < idle_waiters_count) {
        sleep(0.01);
        err = dragon_bcast_num_waiting(&bd, &count);
        if (err != DRAGON_SUCCESS)
            printf("ERROR: Could not get num waiting in main().\n");
    }

    err = dragon_bcast_trigger_all(&bd, NULL, payload, 256);
    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    while (count > 0) {
        sleep(0.01);
        dragon_bcast_num_waiting(&bd, &count); // don't check error code here. Leads to non-deterministic test case count.
    }

    tests_passed += idle_waiters_count;
    tests_attempted += idle_waiters_count;

    err = dragon_bcast_destroy(&bd);

    free(ptr);

    check_result(err, DRAGON_SUCCESS, &tests_passed, &tests_attempted);

    /******************* End Idle Wait Test *****************/

    printf("Passed %d of %d tests.\n", tests_passed, tests_attempted);

    if (tests_passed != tests_attempted)
        return FAILED;
    else
        return SUCCESS;
}
